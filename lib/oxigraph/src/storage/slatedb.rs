//! SlateDB backed storage primitives.
//!
//! SlateDB is an asynchronous LSM tree written to an object store, while Oxigraph's storage layer is
//! synchronous. [`RuntimeBridge`] owns a small dedicated Tokio runtime and every SlateDB call goes
//! through it, so no caller ever has to be inside a Tokio runtime.
//!
//! A SlateDB database holds every RDF index at once: the logical index is selected by a one-byte
//! namespace prefix (see [`crate::storage::slatedb_keyspace`]). The metadata of the database itself
//! (its magic and on-disk format version) lives in [`Ns::Meta`].

use crate::model::{GraphName, NamedOrBlankNode, OxString, Quad, Term};
use crate::storage::binary_encoder::{
    QuadEncoding, WRITTEN_TERM_MAX_SIZE, decode_term, encode_term, encode_term_pair,
    encode_term_quad, encode_term_triple, write_gosp_quad, write_gpos_quad, write_gspo_quad,
    write_osp_quad, write_ospg_quad, write_pos_quad, write_posg_quad, write_spo_quad,
    write_spog_quad, write_term,
};
use crate::storage::error::{CorruptionError, StorageError};
use crate::storage::numeric_encoder::{
    Decoder, EncodedQuad, EncodedTerm, StrHash, StrLookup, insert_term,
};
use crate::storage::bulk_encoder::BatchedKeys;
use crate::storage::slatedb_bridge::RuntimeBridge;
use crate::storage::slatedb_keyspace::{Ns, prefix_bounds, prefixed, strip_prefix};
use crate::storage::map_thread_result;
use slatedb::bytes::Bytes;
use slatedb::config::{CompressionCodec, ScanOptions, Settings};
use slatedb::object_store::ObjectStore;
use slatedb::object_store::aws::AmazonS3Builder;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::path::Path as ObjectStorePath;
use slatedb::{
    Db, DbIterator, DbReader, DbReaderMode, DbSnapshot, DbTransaction, ErrorKind, IsolationLevel,
    WriteBatch, WriteHandle,
};
use spareval::CancellationToken;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::str::Utf8Error;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tokio::sync::Mutex;

/// Marks a SlateDB database as created by Oxigraph.
const MAGIC: &[u8] = b"oxigraph-slatedb";
/// On-disk format version of the database layout, not of the RDF encoding.
const FORMAT_VERSION: u32 = 1;

const META_MAGIC: &[u8] = b"magic";
const META_FORMAT_VERSION: &[u8] = b"format_version";

/// Number of entries pulled from SlateDB per bridge round trip while scanning.
const SCAN_BATCH_SIZE: usize = 256;

/// Number of quads deleted per pass when a range delete is expanded into individual deletes.
const BATCH_SIZE: usize = 100_000;

/// Default bound on the number of keys a single range delete may expand to
/// (planning/slatedb/implementation_plan2.md, D7).
///
/// A pending delete costs about a kilobyte of memory until the commit (see
/// `planning/slatedb/performance.md`), because the deletes of one clear are staged and then written
/// as a single batch. This bound keeps that staging under roughly two gigabytes, i.e. the deletion
/// of a third of a million quads of a named graph (six keys each) or of two thirds of a million
/// quads of the default graph (three keys each).
const DEFAULT_MAX_CLEAR_DELETES: u64 = 2_000_000;
/// Default amount of staged data an atomic bulk load may hold in memory
/// (planning/slatedb/implementation_plan2.md, D8).
const DEFAULT_BULK_LOAD_MAX_STAGED_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Default read-ahead target of a scan, in bytes.
const DEFAULT_READ_AHEAD_BYTES: usize = 1024 * 1024;
/// Default number of concurrent block fetches of a scan.
const DEFAULT_MAX_FETCH_TASKS: usize = 4;
/// Number of keys copied per write while streaming a backup (D11).
const BACKUP_BATCH_SIZE: usize = 10_000;
/// Default number of times a commit retries a transaction conflict before giving up
/// (planning/slatedb/implementation_plan2.md, D6).
const DEFAULT_COMMIT_RETRY_ATTEMPTS: u32 = 8;
/// Delay before the first conflict retry; it doubles on each attempt.
const COMMIT_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(1);
/// Number of quads per write of a non atomic bulk load.
///
/// `Store` hands the loader batches of up to a million quads, i.e. of up to several million keys.
/// SlateDB write throughput collapses on such a single write, so a non atomic load splits its
/// batches: memory and WAL entries stay bounded whatever the size of the load.
const NON_ATOMIC_BULK_LOAD_CHUNK: usize = 50_000;

/// What a SlateDB commit waits for before answering (planning/slatedb/implementation_plan2.md, D5).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SlateDbDurability {
    /// Returns as soon as the write reached the WAL queue.
    ///
    /// The cheapest policy: a killed process loses up to the last [`Settings::flush_interval`].
    Process,
    /// Waits until the write is durable in the object store.
    ///
    /// This is the default, and matches the durability of the RocksDB backend, whose write-ahead
    /// log survives a killed process.
    #[default]
    Wal,
    /// Additionally moves the memtable to a level 0 table before answering.
    ///
    /// The slowest policy; it is what [`crate::store::Store::flush`] does.
    Flush,
}

/// Where a SlateDB database lives: a directory or an object store URL.
///
/// SlateDB never talks to the file system directly: even a local database is a set of objects in an
/// object store, here [`LocalFileSystem`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SlateDbObjectStore {
    /// A directory of the local file system.
    Directory(PathBuf),
    /// An object store URL: `s3://bucket/prefix` or `file:///absolute/path`.
    ///
    /// Credentials and endpoints of S3 do not go through this URL: they are read from the usual
    /// `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` (or `AWS_DEFAULT_REGION`),
    /// `AWS_ENDPOINT` and `AWS_ALLOW_HTTP` environment variables, exactly like the AWS CLI.
    Url(String),
}

impl SlateDbObjectStore {
    /// Builds the object store and the path of the database inside it.
    fn open(&self, prefix: &str) -> Result<(Arc<dyn ObjectStore>, ObjectStorePath), StorageError> {
        match self {
            Self::Directory(directory) => {
                // `LocalFileSystem` requires its root to exist, while `Store::open` must create a
                // missing directory like the RocksDB backend.
                std::fs::create_dir_all(directory)?;
                let store = LocalFileSystem::new_with_prefix(directory)
                    .map_err(|error| StorageError::Other(Box::new(error)))?;
                Ok((Arc::new(store), database_path(prefix)?))
            }
            Self::Url(url) => {
                let url = url::Url::parse(url).map_err(|error| {
                    StorageError::Other(format!("Invalid SlateDB object store URL: {error}").into())
                })?;
                match url.scheme() {
                    "file" => {
                        let directory = url.to_file_path().map_err(|()| {
                            StorageError::Other(
                                format!("The SlateDB object store URL {url} is not a valid file URL")
                                    .into(),
                            )
                        })?;
                        std::fs::create_dir_all(&directory)?;
                        let store = LocalFileSystem::new_with_prefix(&directory)
                            .map_err(|error| StorageError::Other(Box::new(error)))?;
                        Ok((Arc::new(store), database_path(prefix)?))
                    }
                    "s3" | "s3a" => {
                        let bucket = url.host_str().ok_or_else(|| {
                            StorageError::Other(
                                format!("The SlateDB object store URL {url} is missing a bucket")
                                    .into(),
                            )
                        })?;
                        let store = AmazonS3Builder::from_env()
                            .with_bucket_name(bucket)
                            .build()
                            .map_err(|error| StorageError::Other(Box::new(error)))?;
                        Ok((Arc::new(store), database_path(&join_path(url.path(), prefix))?))
                    }
                    scheme => Err(StorageError::Other(
                        format!(
                            "The `{scheme}` object store scheme is not supported: use `s3://bucket/prefix`, `file:///path` or `SlateDbOptions::at_directory`"
                        )
                        .into(),
                    )),
                }
            }
        }
    }
}

/// Appends `suffix` to the `prefix` path of a URL.
fn join_path(prefix: &str, suffix: &str) -> String {
    let prefix = prefix.trim_matches('/');
    let suffix = suffix.trim_matches('/');
    match (prefix.is_empty(), suffix.is_empty()) {
        (true, true) => String::new(),
        (true, false) => suffix.to_owned(),
        (false, true) => prefix.to_owned(),
        (false, false) => format!("{prefix}/{suffix}"),
    }
}

fn database_path(prefix: &str) -> Result<ObjectStorePath, StorageError> {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        // The database lives at the root of the object store.
        Ok(ObjectStorePath::from(""))
    } else {
        ObjectStorePath::parse(prefix).map_err(|error| StorageError::Other(Box::new(error)))
    }
}

/// Location, tuning and durability of a SlateDB database.
#[derive(Clone, Debug)]
pub struct SlateDbOptions {
    object_store: SlateDbObjectStore,
    prefix: String,
    durability: SlateDbDurability,
    settings: Settings,
    read_only: bool,
    commit_retry_attempts: u32,
    max_clear_deletes: u64,
    bulk_load_max_staged_bytes: u64,
    scan_options: ScanOptions,
}

impl Default for SlateDbOptions {
    /// A database of the current directory. Prefer [`SlateDbOptions::at_directory`] or
    /// [`SlateDbOptions::at_url`], which say where the database lives.
    fn default() -> Self {
        Self {
            object_store: SlateDbObjectStore::Directory(PathBuf::new()),
            prefix: String::new(),
            durability: SlateDbDurability::default(),
            settings: default_settings(),
            read_only: false,
            commit_retry_attempts: DEFAULT_COMMIT_RETRY_ATTEMPTS,
            max_clear_deletes: DEFAULT_MAX_CLEAR_DELETES,
            bulk_load_max_staged_bytes: DEFAULT_BULK_LOAD_MAX_STAGED_BYTES,
            scan_options: default_scan_options(),
        }
    }
}

impl SlateDbOptions {
    /// Stores the database in the `path` directory, creating it if it does not exist yet.
    #[must_use]
    pub fn at_directory(path: impl Into<PathBuf>) -> Self {
        Self {
            object_store: SlateDbObjectStore::Directory(path.into()),
            ..Self::default()
        }
    }

    /// Stores the database in an object store, described by an `s3://bucket/prefix` or
    /// `file:///absolute/path` URL.
    ///
    /// S3 credentials, region and endpoint come from the `AWS_*` environment variables.
    #[must_use]
    pub fn at_url(url: impl Into<String>) -> Self {
        Self {
            object_store: SlateDbObjectStore::Url(url.into()),
            ..Self::default()
        }
    }

    /// Adds a path inside the object store above the one the URL or directory describes.
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Opens the database for reading only, without taking the writer role.
    ///
    /// Reads go through a SlateDB [`DbReader`] that follows the latest manifest without writing
    /// anything, so a read-only store never fences the writer. Reading a database that another
    /// process is writing is still undefined behavior, like with [`crate::store::Store::open_read_only`].
    #[must_use]
    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Sets how many times a commit retries a transaction conflict before failing
    /// (planning/slatedb/implementation_plan2.md, D6).
    #[must_use]
    pub fn with_commit_retry_attempts(mut self, commit_retry_attempts: u32) -> Self {
        self.commit_retry_attempts = commit_retry_attempts;
        self
    }

    /// Sets what each commit waits for before answering.
    #[must_use]
    pub fn with_durability(mut self, durability: SlateDbDurability) -> Self {
        self.durability = durability;
        self
    }

    /// Replaces the SlateDB settings.
    #[must_use]
    pub fn with_settings(mut self, settings: Settings) -> Self {
        self.settings = settings;
        self
    }

    /// Sets how often the writer flushes its write-ahead log.
    #[must_use]
    pub fn with_flush_interval(mut self, flush_interval: Duration) -> Self {
        self.settings.flush_interval = Some(flush_interval);
        self
    }

    /// Sets the target size of a level 0 table.
    #[must_use]
    pub fn with_l0_sst_size_bytes(mut self, size: usize) -> Self {
        self.settings.l0_sst_size_bytes = size;
        self
    }

    /// Sets how many unflushed bytes the writer buffers before flushing on its own.
    #[must_use]
    pub fn with_max_unflushed_bytes(mut self, size: usize) -> Self {
        self.settings.max_unflushed_bytes = size;
        self
    }

    /// Sets the compression codec of the tables written by the compactor.
    #[must_use]
    pub fn with_compression_codec(mut self, codec: CompressionCodec) -> Self {
        self.settings.compression_codec = Some(codec);
        self
    }

    /// Sets how many keys a single range delete may expand to before failing instead of growing the
    /// transaction without bound.
    ///
    /// A named graph quad is six keys and a default graph quad three, and each pending delete costs
    /// about a kilobyte of memory until the commit.
    #[must_use]
    pub fn with_max_clear_deletes(mut self, max_clear_deletes: u64) -> Self {
        self.max_clear_deletes = max_clear_deletes;
        self
    }

    /// Sets how many bytes an atomic bulk load may stage in memory.
    #[must_use]
    pub fn with_bulk_load_max_staged_bytes(mut self, size: u64) -> Self {
        self.bulk_load_max_staged_bytes = size;
        self
    }

    /// Sets how much a scan reads ahead and how many block fetches it runs in parallel.
    #[must_use]
    pub fn with_scan_prefetch(mut self, read_ahead_bytes: usize, max_fetch_tasks: usize) -> Self {
        self.scan_options.read_ahead_bytes = read_ahead_bytes;
        self.scan_options.max_fetch_tasks = max_fetch_tasks;
        self
    }

    /// Splits the options into the location to open and the settings used at runtime.
    fn into_parts(self) -> (SlateDbObjectStore, String, SlateDbConfig) {
        let Self {
            object_store,
            prefix,
            durability,
            settings,
            read_only,
            commit_retry_attempts,
            max_clear_deletes,
            bulk_load_max_staged_bytes,
            scan_options,
        } = self;
        (
            object_store,
            prefix,
            SlateDbConfig {
                durability,
                settings,
                read_only,
                commit_retry_attempts,
                max_clear_deletes,
                bulk_load_max_staged_bytes,
                scan_options: Arc::new(scan_options),
                commit_lock: Arc::new(StdMutex::new(())),
            },
        )
    }
}

/// The starting point of the tuning knobs of planning/slatedb/implementation_plan2.md, §6.4.
fn default_settings() -> Settings {
    Settings {
        flush_interval: Some(Duration::from_millis(100)),
        l0_sst_size_bytes: 64 * 1024 * 1024,
        max_unflushed_bytes: 256 * 1024 * 1024,
        compression_codec: Some(CompressionCodec::Lz4),
        ..Settings::default()
    }
}

fn default_scan_options() -> ScanOptions {
    ScanOptions::default()
        .with_read_ahead_bytes(DEFAULT_READ_AHEAD_BYTES)
        .with_max_fetch_tasks(DEFAULT_MAX_FETCH_TASKS)
}

/// The parts of [`SlateDbOptions`] needed after the database is opened.
#[derive(Debug)]
pub(crate) struct SlateDbConfig {
    durability: SlateDbDurability,
    settings: Settings,
    read_only: bool,
    commit_retry_attempts: u32,
    max_clear_deletes: u64,
    bulk_load_max_staged_bytes: u64,
    scan_options: Arc<ScanOptions>,
    /// Serializes the commits of the process, so that two stores of the same process never conflict
    /// with each other (planning/slatedb/implementation_plan2.md, D6).
    commit_lock: Arc<StdMutex<()>>,
}

impl Default for SlateDbConfig {
    fn default() -> Self {
        Self {
            durability: SlateDbDurability::default(),
            settings: default_settings(),
            read_only: false,
            commit_retry_attempts: DEFAULT_COMMIT_RETRY_ATTEMPTS,
            max_clear_deletes: DEFAULT_MAX_CLEAR_DELETES,
            bulk_load_max_staged_bytes: DEFAULT_BULK_LOAD_MAX_STAGED_BYTES,
            scan_options: Arc::new(default_scan_options()),
            commit_lock: Arc::new(StdMutex::new(())),
        }
    }
}

/// The writer of a database, or the absence of one when it was opened read-only.
#[derive(Clone)]
pub(crate) enum SlateDbHandle {
    ReadWrite(Arc<Db>),
    ReadOnly(Arc<DbReader>),
}

impl SlateDbHandle {
    /// Returns the writer, or the error every mutation of a read-only database reports.
    fn writer(&self) -> Result<Arc<Db>, StorageError> {
        match self {
            Self::ReadWrite(db) => Ok(Arc::clone(db)),
            Self::ReadOnly(_) => Err(StorageError::Other(
                "this store was opened read-only, it cannot be modified".into(),
            )),
        }
    }
}

/// A single SlateDB database holding every Oxigraph keyspace.
#[derive(Clone)]
pub(crate) struct SlateDbStorage {
    handle: SlateDbHandle,
    // Declared after `handle` so that the runtime outlives the SlateDB tasks it drives.
    bridge: Arc<RuntimeBridge>,
    config: Arc<SlateDbConfig>,
}

impl SlateDbStorage {
    /// Opens, creating it if needed, the database stored in the `path` directory.
    #[cfg(any(not(feature = "rocksdb"), test))]
    pub(crate) fn open_at(path: &Path) -> Result<Self, StorageError> {
        Self::open_configured(
            &SlateDbObjectStore::Directory(path.to_path_buf()),
            "",
            SlateDbConfig::default(),
        )
    }

    /// Opens the database described by `options`.
    pub(crate) fn open_with_options(options: SlateDbOptions) -> Result<Self, StorageError> {
        let (object_store, prefix, config) = options.into_parts();
        Self::open_configured(&object_store, &prefix, config)
    }

    fn open_configured(
        location: &SlateDbObjectStore,
        prefix: &str,
        config: SlateDbConfig,
    ) -> Result<Self, StorageError> {
        let (object_store, path) = location.open(prefix)?;
        let bridge = Arc::new(RuntimeBridge::new()?);
        let handle = if config.read_only {
            // A reader never takes the writer role, so opening a database read-only does not fence
            // the process writing it (planning/slatedb/implementation_plan2.md, D9).
            let reader = run_slatedb(&bridge, async move {
                DbReader::builder(path, object_store)
                    .with_reader_mode(DbReaderMode::FollowLatest)
                    .build()
                    .await
            })?;
            SlateDbHandle::ReadOnly(Arc::new(reader))
        } else {
            let settings = config.settings.clone();
            let db = run_slatedb(&bridge, async move {
                Db::builder(path, object_store)
                    .with_settings(settings)
                    .build()
                    .await
            })?;
            SlateDbHandle::ReadWrite(Arc::new(db))
        };
        let storage = Self {
            handle,
            bridge,
            config: Arc::new(config),
        };
        storage.check_meta()?;
        Ok(storage)
    }

    /// Returns a handle reading the latest state: a snapshot of the writer, or the read-only reader.
    fn read_handle(&self) -> Result<SlateDbReadHandle, StorageError> {
        match &self.handle {
            SlateDbHandle::ReadWrite(db) => db
                .snapshot()
                .map(SlateDbReadHandle::Snapshot)
                .map_err(map_slatedb_error),
            SlateDbHandle::ReadOnly(reader) => Ok(SlateDbReadHandle::Reader(Arc::clone(reader))),
        }
    }

    /// Reads the database metadata, initializing it if the database is new.
    fn check_meta(&self) -> Result<(), StorageError> {
        match self.get(Ns::Meta, META_MAGIC)? {
            None => self.write_meta(),
            Some(magic) => {
                if magic.as_slice() != MAGIC {
                    return Err(CorruptionError::msg(
                        "This directory contains a SlateDB database that has not been created by Oxigraph",
                    )
                    .into());
                }
                let version = self.get(Ns::Meta, META_FORMAT_VERSION)?.ok_or_else(|| {
                    CorruptionError::msg("The SlateDB database is missing its format version")
                })?;
                let version: [u8; 4] = version.as_slice().try_into().map_err(|error| {
                    CorruptionError::new(format!(
                        "Error while parsing the SlateDB format version: {error}"
                    ))
                })?;
                let version = u32::from_be_bytes(version);
                match version.cmp(&FORMAT_VERSION) {
                    Ordering::Equal => Ok(()),
                    Ordering::Less => Err(CorruptionError::msg(format!(
                        "The SlateDB database is using the outdated format version {version}. Automated migration is not supported, please dump the store dataset using a compatible Oxigraph version and load it again using the current version"
                    ))
                    .into()),
                    Ordering::Greater => Err(CorruptionError::msg(format!(
                        "The SlateDB database is using the too recent format version {version}. Upgrade to the latest Oxigraph version to load this database"
                    ))
                    .into()),
                }
            }
        }
    }

    fn write_meta(&self) -> Result<(), StorageError> {
        // A read-only database cannot be initialized: fail with the read-only error.
        self.handle.writer()?;
        let mut batch = WriteBatch::new();
        batch.put_bytes(
            Bytes::from(prefixed(Ns::Meta, META_MAGIC)),
            Bytes::from_static(MAGIC),
        );
        batch.put_bytes(
            Bytes::from(prefixed(Ns::Meta, META_FORMAT_VERSION)),
            Bytes::from(FORMAT_VERSION.to_be_bytes().to_vec()),
        );
        self.write(&batch)
    }

    /// Reads the value of `key` in `ns`.
    pub(crate) fn get(&self, ns: Ns, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.read_handle()?.get(&self.bridge, prefixed(ns, key))
    }

    /// Writes `value` at `key` in `ns`.
    #[cfg(test)]
    pub(crate) fn put(&self, ns: Ns, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let mut batch = WriteBatch::new();
        batch.put_bytes(
            Bytes::from(prefixed(ns, key)),
            Bytes::copy_from_slice(value),
        );
        self.write(&batch)
    }

    /// Removes `key` from `ns`.
    #[cfg(test)]
    pub(crate) fn delete(&self, ns: Ns, key: &[u8]) -> Result<(), StorageError> {
        let mut batch = WriteBatch::new();
        batch.delete(prefixed(ns, key));
        self.write(&batch)
    }

    /// Commits `batch` according to the durability policy, like RocksDB does with its write-ahead
    /// log.
    fn write(&self, batch: &WriteBatch) -> Result<(), StorageError> {
        let db = self.handle.writer()?;
        write_batch(&self.bridge, &db, &self.config, batch)
    }

    /// Scans the keys of `ns` starting with `prefix`, in key order.
    #[cfg(test)]
    pub(crate) fn try_scan(&self, ns: Ns, prefix: &[u8]) -> Result<SlateDbScan, StorageError> {
        let options = Arc::clone(&self.config.scan_options);
        let (start, end) = prefix_bounds(ns, prefix);
        self.read_handle()?.scan_range(&self.bridge, start, end, &options)
    }

    /// Takes a consistent view of the whole database.
    ///
    /// Taking the snapshot itself cannot fail in practice, but the SlateDB API returns a `Result`.
    /// Rather than making every reader creation fallible, a failure is deferred to the reads.
    pub(crate) fn snapshot(&self) -> SlateDbStorageReader {
        let source = match self.read_handle() {
            Ok(handle) => SlateDbReadSource::Ready(handle),
            Err(error) => SlateDbReadSource::Failed(error.to_string()),
        };
        SlateDbStorageReader {
            source,
            bridge: Arc::clone(&self.bridge),
            scan_options: Arc::clone(&self.config.scan_options),
        }
    }

    /// Ensures that all the writes are durable on disk.
    pub(crate) fn flush(&self) -> Result<(), StorageError> {
        let db = self.handle.writer()?;
        run_slatedb(&self.bridge, async move { db.flush().await })
    }

    /// Reads a value of the metadata keyspace, where the format version and the migration marker
    /// live.
    #[cfg(feature = "rocksdb")]
    pub(crate) fn get_meta(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.get(Ns::Meta, key)
    }

    /// Writes entries whose keys already carry their namespace prefix, and removes `deletes`.
    ///
    /// Used by the RocksDB migration, which copies keys verbatim (planning/slatedb/
    /// implementation_plan2.md, §7.3).
    #[cfg(feature = "rocksdb")]
    pub(crate) fn write_prefixed(
        &self,
        entries: &[(Vec<u8>, Vec<u8>)],
        deletes: &[Vec<u8>],
    ) -> Result<(), StorageError> {
        let mut batch = WriteBatch::new();
        for (key, value) in entries {
            batch.put_bytes(Bytes::from(key.clone()), Bytes::copy_from_slice(value));
        }
        for key in deletes {
            batch.delete(key.clone());
        }
        self.write(&batch)
    }

    /// Flushes and closes the database. Later operations fail.
    #[cfg(any(test, feature = "rocksdb"))]
    pub(crate) fn close(&self) -> Result<(), StorageError> {
        let db = self.handle.writer()?;
        run_slatedb(&self.bridge, async move { db.close().await })
    }

    pub(crate) fn start_transaction(&self) -> Result<SlateDbStorageTransaction, StorageError> {
        Ok(SlateDbStorageTransaction {
            ops: Vec::new(),
            db: self.handle.writer()?,
            bridge: Arc::clone(&self.bridge),
            config: Arc::clone(&self.config),
        })
    }

    pub(crate) fn start_readable_transaction(
        &self,
    ) -> Result<SlateDbStorageReadableTransaction, StorageError> {
        let db = self.handle.writer()?;
        let transaction = run_slatedb(&self.bridge, {
            let db = Arc::clone(&db);
            async move { db.begin(IsolationLevel::Snapshot) }
        })?;
        Ok(SlateDbStorageReadableTransaction {
            transaction: Arc::new(transaction),
            deferred_error: None,
            db,
            bridge: Arc::clone(&self.bridge),
            config: Arc::clone(&self.config),
            clear_deleted: 0,
        })
    }

    pub(crate) fn bulk_loader(&self) -> SlateDbStorageBulkLoader {
        SlateDbStorageBulkLoader::new(
            self.handle.clone(),
            Arc::clone(&self.bridge),
            Arc::clone(&self.config),
        )
    }

    /// Copies every key of the database into a new SlateDB database stored in `target_directory`
    /// (planning/slatedb/implementation_plan2.md, D11).
    ///
    /// SlateDB checkpoints are object store metadata, not directories that [`Self::open_at`] could
    /// read back, so a backup is a logical copy streamed from a single snapshot in bounded batches.
    pub(crate) fn backup(&self, target_directory: &Path) -> Result<(), StorageError> {
        if target_directory.exists() {
            return Err(StorageError::Other(
                format!(
                    "The backup directory {} already exists",
                    target_directory.display()
                )
                .into(),
            ));
        }
        std::fs::create_dir_all(target_directory)?;
        let snapshot = self.read_handle()?;
        let bridge = Arc::clone(&self.bridge);
        let settings = self.config.settings.clone();
        let target = target_directory.to_path_buf();
        run_storage(&bridge, async move {
            let object_store: Arc<dyn ObjectStore> = Arc::new(
                LocalFileSystem::new_with_prefix(&target)
                    .map_err(|error| StorageError::Other(Box::new(error)))?,
            );
            let backup = Db::builder(ObjectStorePath::from(""), object_store)
                .with_settings(settings)
                .build()
                .await?;
            let mut batch = WriteBatch::new();
            let mut count = 0;
            let mut iter = snapshot.scan_all().await?;
            while let Some(entry) = iter.next().await? {
                batch.put_bytes(entry.key, entry.value);
                count += 1;
                if count >= BACKUP_BATCH_SIZE {
                    backup.write(std::mem::take(&mut batch)).await?;
                    count = 0;
                }
            }
            if !batch.is_empty() {
                backup.write(batch).await?;
            }
            backup.flush().await?;
            backup.close().await?;
            Ok::<(), StorageError>(())
        })
    }

}

/// A consistent, read-only view of a [`SlateDbStorage`], or of a readable transaction.
#[must_use]
pub(crate) struct SlateDbStorageReader {
    source: SlateDbReadSource,
    bridge: Arc<RuntimeBridge>,
    scan_options: Arc<ScanOptions>,
}

#[derive(Clone)]
enum SlateDbReadSource {
    Ready(SlateDbReadHandle),
    /// The snapshot could not be taken: every read reports this error.
    Failed(String),
}

#[derive(Clone)]
enum SlateDbReadHandle {
    Snapshot(Arc<DbSnapshot>),
    /// Reads a [`DbTransaction`], which sees its own uncommitted writes.
    Transaction(Arc<DbTransaction>),
    /// Reads a read-only database through its [`DbReader`].
    Reader(Arc<DbReader>),
}

impl SlateDbReadHandle {
    async fn get_inner(&self, stored_key: Vec<u8>) -> Result<Option<Bytes>, slatedb::Error> {
        match self {
            Self::Snapshot(snapshot) => snapshot.get(stored_key).await,
            Self::Transaction(transaction) => transaction.get(stored_key).await,
            Self::Reader(reader) => reader.get(stored_key).await,
        }
    }

    async fn scan_inner(
        &self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<DbIterator, slatedb::Error> {
        match self {
            Self::Snapshot(snapshot) => match end {
                Some(end) => snapshot.scan_with_options(start..end, options).await,
                None => snapshot.scan_with_options(start.., options).await,
            },
            Self::Transaction(transaction) => match end {
                Some(end) => transaction.scan_with_options(start..end, options).await,
                None => transaction.scan_with_options(start.., options).await,
            },
            Self::Reader(reader) => match end {
                Some(end) => reader.scan_with_options(start..end, options).await,
                None => reader.scan_with_options(start.., options).await,
            },
        }
    }

    /// Scans the whole database, for [`SlateDbStorage::backup`].
    async fn scan_all(&self) -> Result<DbIterator, slatedb::Error> {
        match self {
            Self::Snapshot(snapshot) => snapshot.scan(..).await,
            Self::Transaction(transaction) => transaction.scan(..).await,
            Self::Reader(reader) => reader.scan(..).await,
        }
    }

    /// Reads `stored_key`, a key that already carries its namespace prefix.
    fn get(
        &self,
        bridge: &Arc<RuntimeBridge>,
        stored_key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let handle = self.clone();
        run_slatedb(bridge, async move {
            handle
                .get_inner(stored_key)
                .await
                .map(|value| value.map(|value| value.to_vec()))
        })
    }
    fn scan_range(
        &self,
        bridge: &Arc<RuntimeBridge>,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        options: &Arc<ScanOptions>,
    ) -> Result<SlateDbScan, StorageError> {
        let handle = self.clone();
        let options = Arc::clone(options);
        let iter = run_slatedb(bridge, async move {
            handle.scan_inner(start, end, &options).await
        })?;
        Ok(SlateDbScan::new(Arc::clone(bridge), iter))
    }
}

impl SlateDbReadSource {
    fn get(
        &self,
        bridge: &Arc<RuntimeBridge>,
        stored_key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        match self {
            Self::Failed(message) => Err(StorageError::Other(message.clone().into())),
            Self::Ready(source) => source.get(bridge, stored_key),
        }
    }

    fn scan(
        &self,
        bridge: &Arc<RuntimeBridge>,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        options: &Arc<ScanOptions>,
    ) -> Result<SlateDbScan, StorageError> {
        match self {
            Self::Failed(message) => Err(StorageError::Other(message.clone().into())),
            Self::Ready(source) => source.scan_range(bridge, start, end, options),
        }
    }
}

impl SlateDbStorageReader {
    /// Reads the value of `key` in `ns`, as of the snapshot.
    pub(crate) fn get(&self, ns: Ns, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.source.get(&self.bridge, prefixed(ns, key))
    }

    /// Scans the keys of `ns` starting with `prefix`, in key order, as of the snapshot.
    pub(crate) fn try_scan(&self, ns: Ns, prefix: &[u8]) -> Result<SlateDbScan, StorageError> {
        let (start, end) = prefix_bounds(ns, prefix);
        self.source
            .scan(&self.bridge, start, end, &self.scan_options)
    }

    pub(crate) fn len(&self) -> Result<usize, StorageError> {
        Ok(count_quads(self.dspo_quads(&[]))? + count_quads(self.gspo_quads(&[]))?)
    }

    pub(crate) fn is_empty(&self) -> Result<bool, StorageError> {
        Ok(self.dspo_quads(&[]).next().transpose()?.is_none()
            && self.gspo_quads(&[]).next().transpose()?.is_none())
    }

    pub(crate) fn contains(&self, quad: &EncodedQuad) -> Result<bool, StorageError> {
        let mut buffer = Vec::with_capacity(4 * WRITTEN_TERM_MAX_SIZE);
        if quad.graph_name.is_default_graph() {
            write_spo_quad(&mut buffer, quad);
            Ok(self.get(Ns::Dspo, &buffer)?.is_some())
        } else {
            write_gspo_quad(&mut buffer, quad);
            Ok(self.get(Ns::Gspo, &buffer)?.is_some())
        }
    }

    pub(crate) fn quads_for_pattern(
        &self,
        subject: Option<&EncodedTerm>,
        predicate: Option<&EncodedTerm>,
        object: Option<&EncodedTerm>,
        graph_name: Option<&EncodedTerm>,
    ) -> SlateDbChainedDecodingQuadIterator {
        match subject {
            Some(subject) => match predicate {
                Some(predicate) => match object {
                    Some(object) => match graph_name {
                        Some(graph_name) => self.quads_for_subject_predicate_object_graph(
                            subject, predicate, object, graph_name,
                        ),
                        None => self.quads_for_subject_predicate_object(subject, predicate, object),
                    },
                    None => match graph_name {
                        Some(graph_name) => {
                            self.quads_for_subject_predicate_graph(subject, predicate, graph_name)
                        }
                        None => self.quads_for_subject_predicate(subject, predicate),
                    },
                },
                None => match object {
                    Some(object) => match graph_name {
                        Some(graph_name) => {
                            self.quads_for_subject_object_graph(subject, object, graph_name)
                        }
                        None => self.quads_for_subject_object(subject, object),
                    },
                    None => match graph_name {
                        Some(graph_name) => self.quads_for_subject_graph(subject, graph_name),
                        None => self.quads_for_subject(subject),
                    },
                },
            },
            None => match predicate {
                Some(predicate) => match object {
                    Some(object) => match graph_name {
                        Some(graph_name) => {
                            self.quads_for_predicate_object_graph(predicate, object, graph_name)
                        }
                        None => self.quads_for_predicate_object(predicate, object),
                    },
                    None => match graph_name {
                        Some(graph_name) => self.quads_for_predicate_graph(predicate, graph_name),
                        None => self.quads_for_predicate(predicate),
                    },
                },
                None => match object {
                    Some(object) => match graph_name {
                        Some(graph_name) => self.quads_for_object_graph(object, graph_name),
                        None => self.quads_for_object(object),
                    },
                    None => match graph_name {
                        Some(graph_name) => self.quads_for_graph(graph_name),
                        None => self.quads(),
                    },
                },
            },
        }
    }

    pub(crate) fn quads(&self) -> SlateDbChainedDecodingQuadIterator {
        SlateDbChainedDecodingQuadIterator::pair(self.dspo_quads(&[]), self.gspo_quads(&[]))
    }

    fn quads_for_subject(&self, subject: &EncodedTerm) -> SlateDbChainedDecodingQuadIterator {
        SlateDbChainedDecodingQuadIterator::pair(
            self.dspo_quads(&encode_term(subject)),
            self.spog_quads(&encode_term(subject)),
        )
    }

    fn quads_for_subject_predicate(
        &self,
        subject: &EncodedTerm,
        predicate: &EncodedTerm,
    ) -> SlateDbChainedDecodingQuadIterator {
        SlateDbChainedDecodingQuadIterator::pair(
            self.dspo_quads(&encode_term_pair(subject, predicate)),
            self.spog_quads(&encode_term_pair(subject, predicate)),
        )
    }

    fn quads_for_subject_predicate_object(
        &self,
        subject: &EncodedTerm,
        predicate: &EncodedTerm,
        object: &EncodedTerm,
    ) -> SlateDbChainedDecodingQuadIterator {
        SlateDbChainedDecodingQuadIterator::pair(
            self.dspo_quads(&encode_term_triple(subject, predicate, object)),
            self.spog_quads(&encode_term_triple(subject, predicate, object)),
        )
    }

    fn quads_for_subject_object(
        &self,
        subject: &EncodedTerm,
        object: &EncodedTerm,
    ) -> SlateDbChainedDecodingQuadIterator {
        SlateDbChainedDecodingQuadIterator::pair(
            self.dosp_quads(&encode_term_pair(object, subject)),
            self.ospg_quads(&encode_term_pair(object, subject)),
        )
    }

    fn quads_for_predicate(&self, predicate: &EncodedTerm) -> SlateDbChainedDecodingQuadIterator {
        SlateDbChainedDecodingQuadIterator::pair(
            self.dpos_quads(&encode_term(predicate)),
            self.posg_quads(&encode_term(predicate)),
        )
    }

    fn quads_for_predicate_object(
        &self,
        predicate: &EncodedTerm,
        object: &EncodedTerm,
    ) -> SlateDbChainedDecodingQuadIterator {
        SlateDbChainedDecodingQuadIterator::pair(
            self.dpos_quads(&encode_term_pair(predicate, object)),
            self.posg_quads(&encode_term_pair(predicate, object)),
        )
    }

    fn quads_for_object(&self, object: &EncodedTerm) -> SlateDbChainedDecodingQuadIterator {
        SlateDbChainedDecodingQuadIterator::pair(
            self.dosp_quads(&encode_term(object)),
            self.ospg_quads(&encode_term(object)),
        )
    }

    fn quads_for_graph(&self, graph_name: &EncodedTerm) -> SlateDbChainedDecodingQuadIterator {
        SlateDbChainedDecodingQuadIterator::new(if graph_name.is_default_graph() {
            self.dspo_quads(&Vec::default())
        } else {
            self.gspo_quads(&encode_term(graph_name))
        })
    }

    fn quads_for_subject_graph(
        &self,
        subject: &EncodedTerm,
        graph_name: &EncodedTerm,
    ) -> SlateDbChainedDecodingQuadIterator {
        SlateDbChainedDecodingQuadIterator::new(if graph_name.is_default_graph() {
            self.dspo_quads(&encode_term(subject))
        } else {
            self.gspo_quads(&encode_term_pair(graph_name, subject))
        })
    }

    fn quads_for_subject_predicate_graph(
        &self,
        subject: &EncodedTerm,
        predicate: &EncodedTerm,
        graph_name: &EncodedTerm,
    ) -> SlateDbChainedDecodingQuadIterator {
        SlateDbChainedDecodingQuadIterator::new(if graph_name.is_default_graph() {
            self.dspo_quads(&encode_term_pair(subject, predicate))
        } else {
            self.gspo_quads(&encode_term_triple(graph_name, subject, predicate))
        })
    }

    fn quads_for_subject_predicate_object_graph(
        &self,
        subject: &EncodedTerm,
        predicate: &EncodedTerm,
        object: &EncodedTerm,
        graph_name: &EncodedTerm,
    ) -> SlateDbChainedDecodingQuadIterator {
        SlateDbChainedDecodingQuadIterator::new(if graph_name.is_default_graph() {
            self.dspo_quads(&encode_term_triple(subject, predicate, object))
        } else {
            self.gspo_quads(&encode_term_quad(graph_name, subject, predicate, object))
        })
    }

    fn quads_for_subject_object_graph(
        &self,
        subject: &EncodedTerm,
        object: &EncodedTerm,
        graph_name: &EncodedTerm,
    ) -> SlateDbChainedDecodingQuadIterator {
        SlateDbChainedDecodingQuadIterator::new(if graph_name.is_default_graph() {
            self.dosp_quads(&encode_term_pair(object, subject))
        } else {
            self.gosp_quads(&encode_term_triple(graph_name, object, subject))
        })
    }

    fn quads_for_predicate_graph(
        &self,
        predicate: &EncodedTerm,
        graph_name: &EncodedTerm,
    ) -> SlateDbChainedDecodingQuadIterator {
        SlateDbChainedDecodingQuadIterator::new(if graph_name.is_default_graph() {
            self.dpos_quads(&encode_term(predicate))
        } else {
            self.gpos_quads(&encode_term_pair(graph_name, predicate))
        })
    }

    fn quads_for_predicate_object_graph(
        &self,
        predicate: &EncodedTerm,
        object: &EncodedTerm,
        graph_name: &EncodedTerm,
    ) -> SlateDbChainedDecodingQuadIterator {
        SlateDbChainedDecodingQuadIterator::new(if graph_name.is_default_graph() {
            self.dpos_quads(&encode_term_pair(predicate, object))
        } else {
            self.gpos_quads(&encode_term_triple(graph_name, predicate, object))
        })
    }

    fn quads_for_object_graph(
        &self,
        object: &EncodedTerm,
        graph_name: &EncodedTerm,
    ) -> SlateDbChainedDecodingQuadIterator {
        SlateDbChainedDecodingQuadIterator::new(if graph_name.is_default_graph() {
            self.dosp_quads(&encode_term(object))
        } else {
            self.gosp_quads(&encode_term_pair(graph_name, object))
        })
    }

    pub(crate) fn named_graphs(&self) -> SlateDbDecodingGraphIterator {
        SlateDbDecodingGraphIterator {
            scan: SlateDbScanState::new(self.try_scan(Ns::Graphs, &[])),
        }
    }

    pub(crate) fn contains_named_graph(
        &self,
        graph_name: &EncodedTerm,
    ) -> Result<bool, StorageError> {
        Ok(self.get(Ns::Graphs, &encode_term(graph_name))?.is_some())
    }

    fn spog_quads(&self, prefix: &[u8]) -> SlateDbDecodingQuadIterator {
        self.inner_quads(Ns::Spog, prefix, QuadEncoding::Spog)
    }

    fn posg_quads(&self, prefix: &[u8]) -> SlateDbDecodingQuadIterator {
        self.inner_quads(Ns::Posg, prefix, QuadEncoding::Posg)
    }

    fn ospg_quads(&self, prefix: &[u8]) -> SlateDbDecodingQuadIterator {
        self.inner_quads(Ns::Ospg, prefix, QuadEncoding::Ospg)
    }

    fn gspo_quads(&self, prefix: &[u8]) -> SlateDbDecodingQuadIterator {
        self.inner_quads(Ns::Gspo, prefix, QuadEncoding::Gspo)
    }

    fn gpos_quads(&self, prefix: &[u8]) -> SlateDbDecodingQuadIterator {
        self.inner_quads(Ns::Gpos, prefix, QuadEncoding::Gpos)
    }

    fn gosp_quads(&self, prefix: &[u8]) -> SlateDbDecodingQuadIterator {
        self.inner_quads(Ns::Gosp, prefix, QuadEncoding::Gosp)
    }

    fn dspo_quads(&self, prefix: &[u8]) -> SlateDbDecodingQuadIterator {
        self.inner_quads(Ns::Dspo, prefix, QuadEncoding::Dspo)
    }

    fn dpos_quads(&self, prefix: &[u8]) -> SlateDbDecodingQuadIterator {
        self.inner_quads(Ns::Dpos, prefix, QuadEncoding::Dpos)
    }

    fn dosp_quads(&self, prefix: &[u8]) -> SlateDbDecodingQuadIterator {
        self.inner_quads(Ns::Dosp, prefix, QuadEncoding::Dosp)
    }

    fn inner_quads(
        &self,
        ns: Ns,
        prefix: &[u8],
        encoding: QuadEncoding,
    ) -> SlateDbDecodingQuadIterator {
        SlateDbDecodingQuadIterator {
            scan: SlateDbScanState::new(self.try_scan(ns, prefix)),
            ns,
            encoding,
        }
    }

    pub(crate) fn contains_str(&self, key: &StrHash) -> Result<bool, StorageError> {
        Ok(self.get(Ns::Id2Str, &key.to_be_bytes())?.is_some())
    }

    /// Validate that all the storage invariants held in the data
    ///
    /// Unlike the RocksDB backend, which reads through the database handle to check the index
    /// entries of every quad, this reads through the snapshot so that the whole check sees one
    /// consistent view of the database.
    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        // triples
        let dspo_size = count_quads(self.dspo_quads(&[]))?;
        if dspo_size != count_quads(self.dpos_quads(&[]))?
            || dspo_size != count_quads(self.dosp_quads(&[]))?
        {
            return Err(
                CorruptionError::new("Not the same number of triples in dspo, dpos and dosp").into(),
            );
        }
        for spo in self.dspo_quads(&[]) {
            let spo = spo?;
            self.decode_quad(&spo)?; // We ensure that the quad is readable
            if self
                .get(
                    Ns::Dpos,
                    &encode_term_triple(&spo.predicate, &spo.object, &spo.subject),
                )?
                .is_none()
            {
                return Err(CorruptionError::new("Quad in dspo and not in dpos").into());
            }
            if self
                .get(
                    Ns::Dosp,
                    &encode_term_triple(&spo.object, &spo.subject, &spo.predicate),
                )?
                .is_none()
            {
                return Err(CorruptionError::new("Quad in dspo and not in dosp").into());
            }
        }

        // quads
        let gspo_size = count_quads(self.gspo_quads(&[]))?;
        if gspo_size != count_quads(self.gpos_quads(&[]))?
            || gspo_size != count_quads(self.gosp_quads(&[]))?
            || gspo_size != count_quads(self.spog_quads(&[]))?
            || gspo_size != count_quads(self.posg_quads(&[]))?
            || gspo_size != count_quads(self.ospg_quads(&[]))?
        {
            return Err(CorruptionError::new(
                "Not the same number of quads in gspo, gpos, gosp, spog, posg and ospg",
            )
            .into());
        }
        for gspo in self.gspo_quads(&[]) {
            let gspo = gspo?;
            self.decode_quad(&gspo)?; // We ensure that the quad is readable
            if self
                .get(
                    Ns::Gpos,
                    &encode_term_quad(
                        &gspo.graph_name,
                        &gspo.predicate,
                        &gspo.object,
                        &gspo.subject,
                    ),
                )?
                .is_none()
            {
                return Err(CorruptionError::new("Quad in gspo and not in gpos").into());
            }
            if self
                .get(
                    Ns::Gosp,
                    &encode_term_quad(
                        &gspo.graph_name,
                        &gspo.object,
                        &gspo.subject,
                        &gspo.predicate,
                    ),
                )?
                .is_none()
            {
                return Err(CorruptionError::new("Quad in gspo and not in gosp").into());
            }
            if self
                .get(
                    Ns::Spog,
                    &encode_term_quad(
                        &gspo.subject,
                        &gspo.predicate,
                        &gspo.object,
                        &gspo.graph_name,
                    ),
                )?
                .is_none()
            {
                return Err(CorruptionError::new("Quad in gspo and not in spog").into());
            }
            if self
                .get(
                    Ns::Posg,
                    &encode_term_quad(
                        &gspo.predicate,
                        &gspo.object,
                        &gspo.subject,
                        &gspo.graph_name,
                    ),
                )?
                .is_none()
            {
                return Err(CorruptionError::new("Quad in gspo and not in posg").into());
            }
            if self
                .get(
                    Ns::Ospg,
                    &encode_term_quad(
                        &gspo.object,
                        &gspo.subject,
                        &gspo.predicate,
                        &gspo.graph_name,
                    ),
                )?
                .is_none()
            {
                return Err(CorruptionError::new("Quad in gspo and not in ospg").into());
            }
            if !self.contains_named_graph(&gspo.graph_name)? {
                return Err(
                    CorruptionError::new("Quad graph name in gspo and not in graphs").into(),
                );
            }
        }
        Ok(())
    }
}

impl StrLookup for SlateDbStorageReader {
    fn get_str(&self, key: &StrHash) -> Result<Option<OxString>, StorageError> {
        Ok(self
            .get(Ns::Id2Str, &key.to_be_bytes())?
            .map(|v| Ok::<_, Utf8Error>(OxString::new_owned(str::from_utf8(&v)?)))
            .transpose()
            .map_err(CorruptionError::new)?)
    }
}

fn count_quads(
    quads: impl Iterator<Item = Result<EncodedQuad, StorageError>>,
) -> Result<usize, StorageError> {
    let mut count = 0;
    for quad in quads {
        quad?;
        count += 1;
    }
    Ok(count)
}

/// A lazy scan over a SlateDB range, pulled from the database in batches.
///
/// SlateDB iterators are async and must be polled from the bridge runtime, so entries are fetched
/// [`SCAN_BATCH_SIZE`] at a time and handed out synchronously from a buffer.
pub(crate) struct SlateDbScan {
    bridge: Arc<RuntimeBridge>,
    iter: Arc<Mutex<DbIterator>>,
    buffer: VecDeque<(Vec<u8>, Vec<u8>)>,
    exhausted: bool,
}

impl SlateDbScan {
    fn new(bridge: Arc<RuntimeBridge>, iter: DbIterator) -> Self {
        Self {
            bridge,
            iter: Arc::new(Mutex::new(iter)),
            buffer: VecDeque::with_capacity(SCAN_BATCH_SIZE),
            exhausted: false,
        }
    }

    /// Returns the next `(key, value)` pair, key still prefixed with its namespace.
    ///
    /// An error is yielded once, after which the scan ends, like a RocksDB iterator failing its
    /// status.
    pub(crate) fn next_key_value(&mut self) -> Option<Result<(Vec<u8>, Vec<u8>), StorageError>> {
        if self.buffer.is_empty() {
            if self.exhausted {
                return None;
            }
            if let Err(error) = self.refill() {
                self.exhausted = true;
                return Some(Err(error));
            }
            if self.buffer.is_empty() {
                return None;
            }
        }
        self.buffer.pop_front().map(Ok)
    }

    fn refill(&mut self) -> Result<(), StorageError> {
        let iter = Arc::clone(&self.iter);
        let entries = run_slatedb(&self.bridge, async move {
            let mut iter = iter.lock().await;
            let mut entries = Vec::with_capacity(SCAN_BATCH_SIZE);
            while entries.len() < SCAN_BATCH_SIZE {
                match iter.next().await? {
                    Some(entry) => entries.push((entry.key.to_vec(), entry.value.to_vec())),
                    None => break,
                }
            }
            Ok(entries)
        })?;
        self.exhausted = entries.len() < SCAN_BATCH_SIZE;
        self.buffer.extend(entries);
        Ok(())
    }
}

/// A scan that may have failed to be created, so that the iterators built on it stay infallible to
/// construct like the RocksDB ones.
enum SlateDbScanState {
    Ready(SlateDbScan),
    Failed(Option<StorageError>),
}

impl SlateDbScanState {
    fn new(scan: Result<SlateDbScan, StorageError>) -> Self {
        match scan {
            Ok(scan) => Self::Ready(scan),
            Err(error) => Self::Failed(Some(error)),
        }
    }

    fn next_key_value(&mut self) -> Option<Result<(Vec<u8>, Vec<u8>), StorageError>> {
        match self {
            Self::Ready(scan) => scan.next_key_value(),
            Self::Failed(error) => error.take().map(Err),
        }
    }
}

#[must_use]
pub(crate) struct SlateDbChainedDecodingQuadIterator {
    first: SlateDbDecodingQuadIterator,
    second: Option<SlateDbDecodingQuadIterator>,
}

impl SlateDbChainedDecodingQuadIterator {
    fn new(first: SlateDbDecodingQuadIterator) -> Self {
        Self {
            first,
            second: None,
        }
    }

    fn pair(first: SlateDbDecodingQuadIterator, second: SlateDbDecodingQuadIterator) -> Self {
        Self {
            first,
            second: Some(second),
        }
    }
}

impl Iterator for SlateDbChainedDecodingQuadIterator {
    type Item = Result<EncodedQuad, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result) = self.first.next() {
            Some(result)
        } else if let Some(second) = &mut self.second {
            second.next()
        } else {
            None
        }
    }
}

struct SlateDbDecodingQuadIterator {
    scan: SlateDbScanState,
    ns: Ns,
    encoding: QuadEncoding,
}

impl Iterator for SlateDbDecodingQuadIterator {
    type Item = Result<EncodedQuad, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        let (key, _) = match self.scan.next_key_value()? {
            Ok(entry) => entry,
            Err(error) => return Some(Err(error)),
        };
        match strip_prefix(self.ns, &key) {
            Some(key) => Some(self.encoding.decode(key)),
            None => Some(Err(CorruptionError::new(
                "A stored key is not in the keyspace it has been read from",
            )
            .into())),
        }
    }
}

#[must_use]
pub(crate) struct SlateDbDecodingGraphIterator {
    scan: SlateDbScanState,
}

impl Iterator for SlateDbDecodingGraphIterator {
    type Item = Result<EncodedTerm, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        let (key, _) = match self.scan.next_key_value()? {
            Ok(entry) => entry,
            Err(error) => return Some(Err(error)),
        };
        Some(match strip_prefix(Ns::Graphs, &key) {
            Some(key) => decode_term(key),
            None => Err(CorruptionError::new(
                "A graph key is not in the graph keyspace it has been read from",
            )
            .into()),
        })
    }
}

/// A buffered mutation, applied when a [`SlateDbStorageTransaction`] commits.
#[derive(Clone)]
enum SlateDbOp {
    Put {
        ns: Ns,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        ns: Ns,
        key: Vec<u8>,
    },
    /// Removes every entry of a keyspace.
    ///
    /// SlateDB write batches have no range delete, so it is recorded here and expanded into
    /// individual deletes at commit time (planning/slatedb/implementation_plan2.md, D7).
    Clear(Ns),
}

/// A read-write transaction whose mutations are buffered until [`Self::commit`].
///
/// The RDF layer never reads through this transaction, so buffering is indistinguishable from
/// applying the mutations as they come.
#[must_use]
pub(crate) struct SlateDbStorageTransaction {
    ops: Vec<SlateDbOp>,
    db: Arc<Db>,
    bridge: Arc<RuntimeBridge>,
    config: Arc<SlateDbConfig>,
}

impl SlateDbStorageTransaction {
    fn put_empty(&mut self, ns: Ns, key: &[u8]) {
        self.ops.push(SlateDbOp::Put {
            ns,
            key: key.to_vec(),
            value: Vec::new(),
        });
    }

    pub(crate) fn insert(&mut self, quad: Quad) {
        let encoded = EncodedQuad::from(&quad);
        let mut buffer = Vec::with_capacity(4 * WRITTEN_TERM_MAX_SIZE);
        if quad.graph_name.is_default_graph() {
            write_spo_quad(&mut buffer, &encoded);
            self.put_empty(Ns::Dspo, &buffer);

            buffer.clear();
            write_pos_quad(&mut buffer, &encoded);
            self.put_empty(Ns::Dpos, &buffer);

            buffer.clear();
            write_osp_quad(&mut buffer, &encoded);
            self.put_empty(Ns::Dosp, &buffer);

            self.insert_term(quad.subject.into(), &encoded.subject);
            self.insert_term(quad.predicate.into(), &encoded.predicate);
            self.insert_term(quad.object, &encoded.object);
        } else {
            write_spog_quad(&mut buffer, &encoded);
            self.put_empty(Ns::Spog, &buffer);

            buffer.clear();
            write_posg_quad(&mut buffer, &encoded);
            self.put_empty(Ns::Posg, &buffer);

            buffer.clear();
            write_ospg_quad(&mut buffer, &encoded);
            self.put_empty(Ns::Ospg, &buffer);

            buffer.clear();
            write_gspo_quad(&mut buffer, &encoded);
            self.put_empty(Ns::Gspo, &buffer);

            buffer.clear();
            write_gpos_quad(&mut buffer, &encoded);
            self.put_empty(Ns::Gpos, &buffer);

            buffer.clear();
            write_gosp_quad(&mut buffer, &encoded);
            self.put_empty(Ns::Gosp, &buffer);

            self.insert_term(quad.subject.into(), &encoded.subject);
            self.insert_term(quad.predicate.into(), &encoded.predicate);
            self.insert_term(quad.object, &encoded.object);

            buffer.clear();
            write_term(&mut buffer, &encoded.graph_name);
            self.put_empty(Ns::Graphs, &buffer);

            self.insert_graph_name(quad.graph_name, &encoded.graph_name);
        }
    }

    pub(crate) fn insert_named_graph(&mut self, graph_name: NamedOrBlankNode) {
        let encoded_graph_name = EncodedTerm::from(&graph_name);

        let mut buffer = Vec::with_capacity(WRITTEN_TERM_MAX_SIZE);
        write_term(&mut buffer, &encoded_graph_name);
        self.put_empty(Ns::Graphs, &buffer);

        self.insert_term(graph_name.into(), &encoded_graph_name);
    }

    fn insert_term(&mut self, term: Term, encoded: &EncodedTerm) {
        let ops = &mut self.ops;
        insert_term(term, encoded, &mut |key, value| {
            ops.push(SlateDbOp::Put {
                ns: Ns::Id2Str,
                key: key.to_be_bytes().to_vec(),
                value: value.as_bytes().to_vec(),
            });
        });
    }

    fn insert_graph_name(&mut self, graph_name: GraphName, encoded: &EncodedTerm) {
        match graph_name {
            GraphName::NamedNode(graph_name) => self.insert_term(graph_name.into(), encoded),
            GraphName::BlankNode(graph_name) => self.insert_term(graph_name.into(), encoded),
            GraphName::DefaultGraph => (),
        }
    }

    pub(crate) fn remove(&mut self, quad: &Quad) {
        self.remove_encoded(&quad.into());
    }

    fn remove_encoded(&mut self, quad: &EncodedQuad) {
        let mut buffer = Vec::with_capacity(4 * WRITTEN_TERM_MAX_SIZE);
        if quad.graph_name.is_default_graph() {
            write_spo_quad(&mut buffer, quad);
            self.push_delete(Ns::Dspo, &buffer);

            buffer.clear();
            write_pos_quad(&mut buffer, quad);
            self.push_delete(Ns::Dpos, &buffer);

            buffer.clear();
            write_osp_quad(&mut buffer, quad);
            self.push_delete(Ns::Dosp, &buffer);
        } else {
            write_spog_quad(&mut buffer, quad);
            self.push_delete(Ns::Spog, &buffer);

            buffer.clear();
            write_posg_quad(&mut buffer, quad);
            self.push_delete(Ns::Posg, &buffer);

            buffer.clear();
            write_ospg_quad(&mut buffer, quad);
            self.push_delete(Ns::Ospg, &buffer);

            buffer.clear();
            write_gspo_quad(&mut buffer, quad);
            self.push_delete(Ns::Gspo, &buffer);

            buffer.clear();
            write_gpos_quad(&mut buffer, quad);
            self.push_delete(Ns::Gpos, &buffer);

            buffer.clear();
            write_gosp_quad(&mut buffer, quad);
            self.push_delete(Ns::Gosp, &buffer);
        }
    }

    fn push_delete(&mut self, ns: Ns, key: &[u8]) {
        self.ops.push(SlateDbOp::Delete {
            ns,
            key: key.to_vec(),
        });
    }

    pub(crate) fn clear_default_graph(&mut self) {
        self.ops.push(SlateDbOp::Clear(Ns::Dspo));
        self.ops.push(SlateDbOp::Clear(Ns::Dpos));
        self.ops.push(SlateDbOp::Clear(Ns::Dosp));
    }

    pub(crate) fn clear_all_named_graphs(&mut self) {
        self.ops.push(SlateDbOp::Clear(Ns::Gspo));
        self.ops.push(SlateDbOp::Clear(Ns::Gpos));
        self.ops.push(SlateDbOp::Clear(Ns::Gosp));
        self.ops.push(SlateDbOp::Clear(Ns::Spog));
        self.ops.push(SlateDbOp::Clear(Ns::Posg));
        self.ops.push(SlateDbOp::Clear(Ns::Ospg));
    }

    pub(crate) fn clear_all_graphs(&mut self) {
        self.clear_default_graph();
        self.remove_all_named_graphs();
    }

    pub(crate) fn remove_all_named_graphs(&mut self) {
        self.clear_all_named_graphs();
        self.ops.push(SlateDbOp::Clear(Ns::Graphs));
    }

    pub(crate) fn clear(&mut self) {
        self.clear_all_graphs();
        // TODO: clear id2str?
    }

    pub(crate) fn commit(self) -> Result<(), StorageError> {
        let Self {
            ops,
            db,
            bridge,
            config,
        } = self;
        if ops.is_empty() {
            return Ok(());
        }
        if !ops.iter().any(|op| matches!(op, SlateDbOp::Clear(_))) {
            return write_ops(&bridge, &db, &config, &ops);
        }
        // Range deletes need a transaction to be expanded against a stable view of the database.
        // Nothing is written before the commit, so a conflicting commit is simply replayed from
        // `ops` (planning/slatedb/implementation_plan2.md, D6).
        retry_commit(&bridge, &config, || {
            let db = Arc::clone(&db);
            let config = Arc::clone(&config);
            let ops = ops.clone();
            async move {
                let options = Arc::clone(&config.scan_options);
                let max_clear_deletes = config.max_clear_deletes;
                let transaction = db.begin(IsolationLevel::Snapshot)?;
                let mut deleted = 0;
                for op in ops {
                    match op {
                        SlateDbOp::Put { ns, key, value } => transaction.put_bytes(
                            Bytes::from(prefixed(ns, &key)),
                            Bytes::from(value),
                        )?,
                        SlateDbOp::Delete { ns, key } => transaction.delete(prefixed(ns, &key))?,
                        SlateDbOp::Clear(ns) => {
                            delete_keyspace(
                                &transaction,
                                ns,
                                &options,
                                max_clear_deletes,
                                &mut deleted,
                            )
                            .await?;
                        }
                    }
                }
                let handle = transaction.commit().await?;
                make_durable(db.as_ref(), handle, config.durability)
                    .await
                    .map_err(StorageError::from)
            }
        })
    }
}

/// Writes `ops` as a single write batch and applies the durability policy.
fn write_ops(
    bridge: &Arc<RuntimeBridge>,
    db: &Arc<Db>,
    config: &Arc<SlateDbConfig>,
    ops: &[SlateDbOp],
) -> Result<(), StorageError> {
    let mut batch = WriteBatch::new();
    for op in ops {
        match op {
            SlateDbOp::Put { ns, key, value } => {
                batch.put_bytes(Bytes::from(prefixed(*ns, key)), Bytes::from(value.clone()));
            }
            SlateDbOp::Delete { ns, key } => batch.delete(prefixed(*ns, key)),
            SlateDbOp::Clear(_) => {
                return Err(CorruptionError::new(
                    "A range delete has not been expanded before committing",
                )
                .into());
            }
        }
    }
    write_batch(bridge, db, config, &batch)
}

/// Commits `batch` and applies the durability policy.
fn write_batch(
    bridge: &Arc<RuntimeBridge>,
    db: &Arc<Db>,
    config: &Arc<SlateDbConfig>,
    batch: &WriteBatch,
) -> Result<(), StorageError> {
    retry_commit(bridge, config, || {
        let db = Arc::clone(db);
        let batch = batch.clone();
        let durability = config.durability;
        async move {
            let handle = db.write(batch).await?;
            make_durable(db.as_ref(), Some(handle), durability)
                .await
                .map_err(StorageError::from)
        }
    })
}

/// Runs `commit` while holding the commit lock of this process, retrying the transaction conflicts
/// that a second process can still cause (planning/slatedb/implementation_plan2.md, D6).
///
/// The commit is built from scratch on each attempt, so retrying is safe as long as nothing has been
/// written yet: a write batch and a transaction are both rejected as a whole.
fn retry_commit<T, Fut>(
    bridge: &Arc<RuntimeBridge>,
    config: &Arc<SlateDbConfig>,
    mut commit: impl FnMut() -> Fut,
) -> Result<T, StorageError>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T, StorageError>> + Send + 'static,
{
    let _guard = config
        .commit_lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut attempt = 0;
    loop {
        match run_storage(bridge, commit()) {
            Err(error) if attempt < config.commit_retry_attempts && is_conflict(&error) => {
                attempt += 1;
                thread::sleep(commit_retry_delay(attempt));
            }
            result => return result,
        }
    }
}

/// Exponential backoff with a jitter, so that two processes do not retry in lockstep.
fn commit_retry_delay(attempt: u32) -> Duration {
    let base = u64::try_from(COMMIT_RETRY_INITIAL_DELAY.as_millis()).unwrap_or(u64::MAX)
        << attempt.min(u32::BITS - 1);
    let jittered = base / 2 + rand::random::<u64>() % (base / 2 + 1);
    Duration::from_millis(jittered)
}

/// Whether `error` is a transaction conflict, i.e. a commit that a retry can succeed.
fn is_conflict(error: &StorageError) -> bool {
    matches!(error, StorageError::Other(error) if error.downcast_ref::<slatedb::Error>().is_some_and(|error| error.kind() == ErrorKind::Transaction))
}

/// Applies the durability policy of planning/slatedb/implementation_plan2.md, D5 to a write that
/// just resolved.
async fn make_durable(
    db: &Db,
    handle: Option<WriteHandle>,
    durability: SlateDbDurability,
) -> Result<(), slatedb::Error> {
    if durability == SlateDbDurability::Process {
        return Ok(());
    }
    if let Some(handle) = handle {
        handle.await_durable().await?;
    }
    if durability == SlateDbDurability::Flush {
        db.flush().await?;
    }
    Ok(())
}

/// Deletes every entry of `ns` inside `transaction`, in [`BATCH_SIZE`] chunks.
///
/// `deleted` is shared by every range delete of the commit so that a `clear()` of a huge database
/// fails with a clear error instead of buffering an unbounded write set (D7).
async fn delete_keyspace(
    transaction: &DbTransaction,
    ns: Ns,
    options: &ScanOptions,
    max_clear_deletes: u64,
    deleted: &mut u64,
) -> Result<(), StorageError> {
    let (start, end) = prefix_bounds(ns, &[]);
    let mut iter = match end {
        Some(end) => transaction.scan_with_options(start..end, options).await?,
        None => transaction.scan_with_options(start.., options).await?,
    };
    // The transaction sees its own deletes, so a single pass over a stable snapshot is enough.
    while let Some(entry) = iter.next().await? {
        *deleted += 1;
        if *deleted > max_clear_deletes {
            return Err(clear_limit_error(max_clear_deletes));
        }
        transaction.delete(entry.key)?;
    }
    Ok(())
}

fn clear_limit_error(max_clear_deletes: u64) -> StorageError {
    StorageError::Other(
        format!(
            "This operation deletes more than {max_clear_deletes} keys. Raise SlateDbOptions::with_max_clear_deletes() to allow it"
        )
        .into(),
    )
}

/// A read-write transaction whose reads see its own uncommitted writes.
#[must_use]
pub(crate) struct SlateDbStorageReadableTransaction {
    transaction: Arc<DbTransaction>,
    /// The first error met while buffering a mutation, reported by [`Self::commit`].
    deferred_error: Option<StorageError>,
    db: Arc<Db>,
    bridge: Arc<RuntimeBridge>,
    config: Arc<SlateDbConfig>,
    /// Number of keys deleted by the range deletes of this transaction, checked against
    /// [`SlateDbOptions::with_max_clear_deletes`].
    clear_deleted: u64,
}

impl SlateDbStorageReadableTransaction {
    pub(crate) fn reader(&self) -> SlateDbStorageReader {
        SlateDbStorageReader {
            source: SlateDbReadSource::Ready(SlateDbReadHandle::Transaction(Arc::clone(
                &self.transaction,
            ))),
            bridge: Arc::clone(&self.bridge),
            scan_options: Arc::clone(&self.config.scan_options),
        }
    }

    fn put_empty(&mut self, ns: Ns, key: &[u8]) {
        self.put(ns, key, &[]);
    }

    fn put(&mut self, ns: Ns, key: &[u8], value: &[u8]) {
        if self.deferred_error.is_some() {
            return;
        }
        if let Err(error) = self.transaction.put_bytes(
            Bytes::from(prefixed(ns, key)),
            Bytes::copy_from_slice(value),
        ) {
            self.deferred_error = Some(map_slatedb_error(error));
        }
    }

    fn delete(&mut self, ns: Ns, key: &[u8]) {
        if self.deferred_error.is_some() {
            return;
        }
        if let Err(error) = self.transaction.delete(prefixed(ns, key)) {
            self.deferred_error = Some(map_slatedb_error(error));
        }
    }

    pub(crate) fn insert(&mut self, quad: Quad) {
        let encoded = EncodedQuad::from(&quad);
        let mut buffer = Vec::with_capacity(4 * WRITTEN_TERM_MAX_SIZE);
        if quad.graph_name.is_default_graph() {
            write_spo_quad(&mut buffer, &encoded);
            self.put_empty(Ns::Dspo, &buffer);

            buffer.clear();
            write_pos_quad(&mut buffer, &encoded);
            self.put_empty(Ns::Dpos, &buffer);

            buffer.clear();
            write_osp_quad(&mut buffer, &encoded);
            self.put_empty(Ns::Dosp, &buffer);

            self.insert_term(quad.subject.into(), &encoded.subject);
            self.insert_term(quad.predicate.into(), &encoded.predicate);
            self.insert_term(quad.object, &encoded.object);
        } else {
            write_spog_quad(&mut buffer, &encoded);
            self.put_empty(Ns::Spog, &buffer);

            buffer.clear();
            write_posg_quad(&mut buffer, &encoded);
            self.put_empty(Ns::Posg, &buffer);

            buffer.clear();
            write_ospg_quad(&mut buffer, &encoded);
            self.put_empty(Ns::Ospg, &buffer);

            buffer.clear();
            write_gspo_quad(&mut buffer, &encoded);
            self.put_empty(Ns::Gspo, &buffer);

            buffer.clear();
            write_gpos_quad(&mut buffer, &encoded);
            self.put_empty(Ns::Gpos, &buffer);

            buffer.clear();
            write_gosp_quad(&mut buffer, &encoded);
            self.put_empty(Ns::Gosp, &buffer);

            self.insert_term(quad.subject.into(), &encoded.subject);
            self.insert_term(quad.predicate.into(), &encoded.predicate);
            self.insert_term(quad.object, &encoded.object);

            buffer.clear();
            write_term(&mut buffer, &encoded.graph_name);
            self.put_empty(Ns::Graphs, &buffer);

            self.insert_graph_name(quad.graph_name, &encoded.graph_name);
        }
    }

    pub(crate) fn insert_named_graph(&mut self, graph_name: NamedOrBlankNode) {
        let encoded_graph_name = EncodedTerm::from(&graph_name);

        let mut buffer = Vec::with_capacity(WRITTEN_TERM_MAX_SIZE);
        write_term(&mut buffer, &encoded_graph_name);
        self.put_empty(Ns::Graphs, &buffer);

        self.insert_term(graph_name.into(), &encoded_graph_name);
    }

    fn insert_term(&mut self, term: Term, encoded: &EncodedTerm) {
        let mut ops = Vec::new();
        insert_term(term, encoded, &mut |key, value| {
            ops.push((key.to_be_bytes().to_vec(), value.as_bytes().to_vec()));
        });
        for (key, value) in ops {
            self.put(Ns::Id2Str, &key, &value);
        }
    }

    fn insert_graph_name(&mut self, graph_name: GraphName, encoded: &EncodedTerm) {
        match graph_name {
            GraphName::NamedNode(graph_name) => self.insert_term(graph_name.into(), encoded),
            GraphName::BlankNode(graph_name) => self.insert_term(graph_name.into(), encoded),
            GraphName::DefaultGraph => (),
        }
    }

    pub(crate) fn remove(&mut self, quad: &Quad) {
        self.remove_encoded(&quad.into());
    }

    fn remove_encoded(&mut self, quad: &EncodedQuad) {
        let mut buffer = Vec::with_capacity(4 * WRITTEN_TERM_MAX_SIZE);
        if quad.graph_name.is_default_graph() {
            write_spo_quad(&mut buffer, quad);
            self.delete(Ns::Dspo, &buffer);

            buffer.clear();
            write_pos_quad(&mut buffer, quad);
            self.delete(Ns::Dpos, &buffer);

            buffer.clear();
            write_osp_quad(&mut buffer, quad);
            self.delete(Ns::Dosp, &buffer);
        } else {
            write_spog_quad(&mut buffer, quad);
            self.delete(Ns::Spog, &buffer);

            buffer.clear();
            write_posg_quad(&mut buffer, quad);
            self.delete(Ns::Posg, &buffer);

            buffer.clear();
            write_ospg_quad(&mut buffer, quad);
            self.delete(Ns::Ospg, &buffer);

            buffer.clear();
            write_gspo_quad(&mut buffer, quad);
            self.delete(Ns::Gspo, &buffer);

            buffer.clear();
            write_gpos_quad(&mut buffer, quad);
            self.delete(Ns::Gpos, &buffer);

            buffer.clear();
            write_gosp_quad(&mut buffer, quad);
            self.delete(Ns::Gosp, &buffer);
        }
    }

    pub(crate) fn clear_graph(&mut self, graph_name: &GraphName) -> Result<(), StorageError> {
        self.clear_encoded_graph(&graph_name.into())
    }

    fn clear_encoded_graph(&mut self, graph_name: &EncodedTerm) -> Result<(), StorageError> {
        let keys_per_quad = if graph_name.is_default_graph() { 3 } else { 6 };
        loop {
            let quads = self
                .reader()
                .quads_for_graph(graph_name)
                .take(BATCH_SIZE)
                .collect::<Result<Vec<_>, _>>()?;
            for quad in &quads {
                self.remove_encoded(quad);
            }
            self.clear_deleted += quads.len() as u64 * keys_per_quad;
            if self.clear_deleted > self.config.max_clear_deletes {
                return Err(clear_limit_error(self.config.max_clear_deletes));
            }
            if quads.len() < BATCH_SIZE {
                return Ok(());
            }
        }
    }

    pub(crate) fn clear_all_named_graphs(&mut self) -> Result<(), StorageError> {
        loop {
            let graph_names = self
                .reader()
                .named_graphs()
                .take(BATCH_SIZE)
                .collect::<Result<Vec<_>, _>>()?;
            for graph_name in &graph_names {
                self.clear_encoded_graph(graph_name)?;
            }
            if graph_names.len() < BATCH_SIZE {
                return Ok(());
            }
        }
    }

    pub(crate) fn clear_all_graphs(&mut self) -> Result<(), StorageError> {
        self.clear_all_named_graphs()?;
        self.clear_graph(&GraphName::DefaultGraph)
    }

    pub(crate) fn remove_named_graph(
        &mut self,
        graph_name: &NamedOrBlankNode,
    ) -> Result<(), StorageError> {
        self.remove_encoded_named_graph(&graph_name.into())
    }

    fn remove_encoded_named_graph(
        &mut self,
        graph_name: &EncodedTerm,
    ) -> Result<(), StorageError> {
        self.clear_encoded_graph(graph_name)?;
        let mut buffer = Vec::with_capacity(WRITTEN_TERM_MAX_SIZE);
        write_term(&mut buffer, graph_name);
        self.delete(Ns::Graphs, &buffer);
        Ok(())
    }

    pub(crate) fn remove_all_named_graphs(&mut self) -> Result<(), StorageError> {
        loop {
            let graph_names = self
                .reader()
                .named_graphs()
                .take(BATCH_SIZE)
                .collect::<Result<Vec<_>, _>>()?;
            for graph_name in &graph_names {
                self.remove_encoded_named_graph(graph_name)?;
            }
            if graph_names.len() < BATCH_SIZE {
                return Ok(());
            }
        }
    }

    pub(crate) fn clear(&mut self) -> Result<(), StorageError> {
        self.remove_all_named_graphs()?;
        self.clear_graph(&GraphName::DefaultGraph)
    }

    pub(crate) fn commit(self) -> Result<(), StorageError> {
        if let Some(error) = self.deferred_error {
            return Err(error);
        }
        let Ok(transaction) = Arc::try_unwrap(self.transaction) else {
            return Err(StorageError::Other(
                "A readable transaction can only be committed once all the readers it created are dropped"
                    .into(),
            ));
        };
        let db = self.db;
        let durability = self.config.durability;
        // The transaction applies its writes as they arrive, so a conflict cannot be retried here:
        // taking the commit lock only keeps this process from conflicting with itself.
        let _guard = self
            .config
            .commit_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        run_storage(&self.bridge, async move {
            let handle = transaction.commit().await?;
            make_durable(db.as_ref(), handle, durability)
                .await
                .map_err(StorageError::from)
        })
    }
}

/// A bulk loader writing quads into a SlateDB database.
///
/// Encoding runs on the threads requested in [`Self::load_batch`], like the RocksDB loader. The
/// encoded keys are then either staged into a single transaction, which is what makes the default
/// load atomic, or written in batches as they land when [`Self::without_atomicity`] is called.
///
/// SlateDB has no external SST ingestion, so this loader is write- and compaction-bound: do not
/// expect the throughput of the RocksDB one (planning/slatedb/implementation_plan2.md, D8).
#[must_use]
pub(crate) struct SlateDbStorageBulkLoader {
    handle: SlateDbHandle,
    bridge: Arc<RuntimeBridge>,
    config: Arc<SlateDbConfig>,
    threads: VecDeque<JoinHandle<Result<BatchedKeys, StorageError>>>,
    done_counter: Arc<StdMutex<u64>>,
    done_and_displayed_counter: u64,
    hooks: Vec<Box<dyn Fn(u64) + Send + Sync>>,
    cancellation_token: CancellationToken,
    atomic: bool,
    /// The write batch an atomic load is staged into, committed as one batch at [`Self::commit`].
    staged: WriteBatch,
    staged_bytes: u64,
}

impl Drop for SlateDbStorageBulkLoader {
    fn drop(&mut self) {
        // Encoding threads stop at their next cancellation check. Nothing staged is ever written by
        // a dropped loader: the staging happens on the thread owning the loader.
        self.cancellation_token.cancel();
    }
}

impl SlateDbStorageBulkLoader {
    pub(crate) fn new(
        handle: SlateDbHandle,
        bridge: Arc<RuntimeBridge>,
        config: Arc<SlateDbConfig>,
    ) -> Self {
        Self {
            handle,
            bridge,
            config,
            threads: VecDeque::new(),
            done_counter: Arc::new(StdMutex::new(0)),
            done_and_displayed_counter: 0,
            hooks: Vec::new(),
            cancellation_token: CancellationToken::new(),
            atomic: true,
            staged: WriteBatch::new(),
            staged_bytes: 0,
        }
    }

    pub(crate) fn on_progress(mut self, callback: impl Fn(u64) + Send + Sync + 'static) -> Self {
        self.hooks.push(Box::new(callback));
        self
    }

    pub(crate) fn without_atomicity(mut self) -> Self {
        self.atomic = false;
        self
    }

    pub(crate) fn load_batch(
        &mut self,
        quads: Vec<Quad>,
        max_num_threads: usize,
    ) -> Result<(), StorageError> {
        self.on_possible_progress()?;
        if self.atomic || quads.len() <= NON_ATOMIC_BULK_LOAD_CHUNK {
            return self.encode_and_spawn(quads, max_num_threads);
        }
        let mut chunk = Vec::with_capacity(NON_ATOMIC_BULK_LOAD_CHUNK);
        for quad in quads {
            chunk.push(quad);
            if chunk.len() == NON_ATOMIC_BULK_LOAD_CHUNK {
                self.encode_and_spawn(std::mem::take(&mut chunk), max_num_threads)?;
            }
        }
        if chunk.is_empty() {
            Ok(())
        } else {
            self.encode_and_spawn(chunk, max_num_threads)
        }
    }

    fn encode_and_spawn(
        &mut self,
        quads: Vec<Quad>,
        max_num_threads: usize,
    ) -> Result<(), StorageError> {
        while self.threads.len() >= max_num_threads {
            if let Some(thread) = self.threads.pop_front() {
                self.apply_thread(thread)?;
                self.on_possible_progress()?;
            }
        }
        let counter = Arc::clone(&self.done_counter);
        let cancellation_token = self.cancellation_token.clone();
        self.threads.push_back(thread::spawn(move || {
            let keys = BatchedKeys::encode(quads, &cancellation_token)?;
            *counter
                .lock()
                .map_err(|_| io::Error::other("Mutex poisoned"))? += keys.quads;
            Ok(keys)
        }));
        Ok(())
    }

    fn apply_thread(
        &mut self,
        thread: JoinHandle<Result<BatchedKeys, StorageError>>,
    ) -> Result<(), StorageError> {
        let keys = map_thread_result(thread.join()).map_err(StorageError::Io)??;
        self.apply(&keys)
    }

    /// Stages `keys` in the atomic transaction, or writes them right away when the load is not
    /// atomic.
    fn apply(&mut self, keys: &BatchedKeys) -> Result<(), StorageError> {
        if keys.quads == 0 {
            return Ok(());
        }
        if !self.atomic {
            let db = self.handle.writer()?;
            let batch = batch_of(keys);
            return run_storage(&self.bridge, async move {
                // The handles are not awaited: the batches become visible as they land, and
                // `commit` flushes the WAL.
                db.write(batch).await?;
                Ok(())
            });
        }
        self.staged_bytes += keys.staged_bytes();
        if self.staged_bytes > self.config.bulk_load_max_staged_bytes {
            return Err(StorageError::Other(
                format!(
                    "atomic bulk load exceeds {} bytes; call BulkLoader::without_atomicity()",
                    self.config.bulk_load_max_staged_bytes
                )
                .into(),
            ));
        }
        put_entries(keys, |ns, key, value| {
            self.staged.put_bytes(
                Bytes::from(prefixed(ns, key)),
                Bytes::copy_from_slice(value),
            );
        });
        Ok(())
    }

    fn on_possible_progress(&mut self) -> Result<(), StorageError> {
        let new_counter = *self
            .done_counter
            .lock()
            .map_err(|_| io::Error::other("Mutex poisoned"))?;
        let display_step = crate::storage::DEFAULT_BULK_LOAD_BATCH_SIZE as u64;
        if new_counter / display_step > self.done_and_displayed_counter / display_step {
            for hook in &self.hooks {
                hook(new_counter);
            }
        }
        self.done_and_displayed_counter = new_counter;
        Ok(())
    }

    pub(crate) fn commit(mut self) -> Result<(), StorageError> {
        while let Some(thread) = self.threads.pop_front() {
            self.apply_thread(thread)?;
            self.on_possible_progress()?;
        }
        if self.atomic {
            let batch = std::mem::take(&mut self.staged);
            let db = self.handle.writer()?;
            if !batch.is_empty() {
                write_batch(&self.bridge, &db, &self.config, &batch)?;
            }
            run_slatedb(&self.bridge, async move { db.flush().await })?;
        }
        self.on_possible_progress()?;
        Ok(())
    }
}

/// A write batch holding every key of `keys`, without the `id2str` values deduplication SlateDB
/// does for us.
fn batch_of(keys: &BatchedKeys) -> WriteBatch {
    let mut batch = WriteBatch::new();
    put_entries(keys, |ns, key, value| {
        batch.put_bytes(
            Bytes::from(prefixed(ns, key)),
            Bytes::copy_from_slice(value),
        );
    });
    batch
}

/// Calls `put` with the namespace, key and value of every entry of `keys`.
fn put_entries(keys: &BatchedKeys, mut put: impl FnMut(Ns, &[u8], &[u8])) {
    for (ns, index) in [
        (Ns::Dspo, &keys.dspo),
        (Ns::Dpos, &keys.dpos),
        (Ns::Dosp, &keys.dosp),
        (Ns::Spog, &keys.spog),
        (Ns::Posg, &keys.posg),
        (Ns::Ospg, &keys.ospg),
        (Ns::Gspo, &keys.gspo),
        (Ns::Gpos, &keys.gpos),
        (Ns::Gosp, &keys.gosp),
        (Ns::Graphs, &keys.graphs),
    ] {
        for key in index {
            put(ns, key, &[]);
        }
    }
    for (key, value) in &keys.id2str {
        put(Ns::Id2Str, key, value.as_bytes());
    }
}

fn run_slatedb<T>(
    bridge: &RuntimeBridge,
    future: impl Future<Output = Result<T, slatedb::Error>> + Send + 'static,
) -> Result<T, StorageError>
where
    T: Send + 'static,
{
    bridge.run(future)?.map_err(map_slatedb_error)
}

/// Runs a future that already reports [`StorageError`]s, for the paths that mix SlateDB calls with
/// the durability policy and the result codec.
fn run_storage<T>(
    bridge: &RuntimeBridge,
    future: impl Future<Output = Result<T, StorageError>> + Send + 'static,
) -> Result<T, StorageError>
where
    T: Send + 'static,
{
    bridge.run(future)?
}

impl From<slatedb::Error> for StorageError {
    fn from(error: slatedb::Error) -> Self {
        map_slatedb_error(error)
    }
}

/// Maps a SlateDB error to the closest [`StorageError`] (planning/slatedb/implementation_plan2.md,
/// D6).
fn map_slatedb_error(error: slatedb::Error) -> StorageError {
    match error.kind() {
        ErrorKind::Data => CorruptionError::new(error).into(),
        ErrorKind::Unavailable => io::Error::other(error).into(),
        ErrorKind::Closed(..) => StorageError::Other(
            "this store was fenced or closed by another writer".into(),
        ),
        _ => StorageError::Other(Box::new(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::NamedNode;
    use tempfile::TempDir;

    fn open(directory: &TempDir) -> SlateDbStorage {
        SlateDbStorage::open_at(directory.path()).unwrap()
    }

    #[test]
    fn key_value_round_trip() {
        let directory = TempDir::new().unwrap();
        let storage = open(&directory);

        storage
            .put(Ns::Id2Str, &1_u64.to_be_bytes(), b"<http://example.com>")
            .unwrap();
        assert_eq!(
            storage.get(Ns::Id2Str, &1_u64.to_be_bytes()).unwrap(),
            Some(b"<http://example.com>".to_vec())
        );
        assert_eq!(storage.get(Ns::Id2Str, &2_u64.to_be_bytes()).unwrap(), None);
        // The namespace is part of the key: the same key in another keyspace is another value.
        assert_eq!(storage.get(Ns::Spog, &1_u64.to_be_bytes()).unwrap(), None);

        storage.flush().unwrap();
        storage.delete(Ns::Id2Str, &1_u64.to_be_bytes()).unwrap();
        assert_eq!(storage.get(Ns::Id2Str, &1_u64.to_be_bytes()).unwrap(), None);

        storage.close().unwrap();
    }

    #[test]
    fn storage_scans_read_the_latest_writes() {
        let directory = TempDir::new().unwrap();
        let storage = open(&directory);
        storage.put(Ns::Dspo, b"a", b"1").unwrap();
        storage.put(Ns::Dspo, b"b", b"2").unwrap();

        let mut scan = storage.try_scan(Ns::Dspo, b"a").unwrap();
        assert_eq!(scan.next_key_value().unwrap().unwrap().1, b"1".to_vec());
        assert!(
            scan.next_key_value().is_none(),
            "only one key starts with the scanned prefix"
        );

        storage.close().unwrap();
    }

    #[test]
    fn scans_are_bounded_to_the_namespace() {
        let directory = TempDir::new().unwrap();
        let storage = open(&directory);
        storage.put(Ns::Spog, b"a", b"spog-a").unwrap();
        storage.put(Ns::Spog, &[u8::MAX], b"spog-ff").unwrap();
        storage.put(Ns::Posg, b"a", b"posg-a").unwrap();
        storage.put(Ns::Posg, &[u8::MAX], b"posg-ff").unwrap();

        let reader = storage.snapshot();
        assert_eq!(
            reader.get(Ns::Spog, b"a").unwrap(),
            Some(b"spog-a".to_vec())
        );
        let mut scan = reader.try_scan(Ns::Spog, &[]).unwrap();
        let mut entries = Vec::new();
        while let Some(entry) = scan.next_key_value() {
            entries.push(entry.unwrap());
        }
        assert_eq!(
            entries,
            vec![
                (prefixed(Ns::Spog, b"a"), b"spog-a".to_vec()),
                (prefixed(Ns::Spog, &[u8::MAX]), b"spog-ff".to_vec()),
            ]
        );

        // A prefix ending with `0xFF` is the tail of its keyspace, not of the database.
        let mut scan = reader.try_scan(Ns::Spog, &[u8::MAX]).unwrap();
        assert_eq!(
            scan.next_key_value().unwrap().unwrap().1,
            b"spog-ff".to_vec()
        );
        assert!(
            scan.next_key_value().is_none(),
            "the scan must stop at the end of the keyspace"
        );

        // A snapshot does not see writes made after it was taken.
        storage.put(Ns::Spog, b"b", b"spog-b").unwrap();
        let mut scan = reader.try_scan(Ns::Spog, &[]).unwrap();
        let mut values = Vec::new();
        while let Some(entry) = scan.next_key_value() {
            values.push(entry.unwrap().1);
        }
        assert_eq!(values, vec![b"spog-a".to_vec(), b"spog-ff".to_vec()]);

        drop(reader);
        storage.close().unwrap();
    }

    #[test]
    fn data_survives_a_reopen() {
        let directory = TempDir::new().unwrap();
        {
            let storage = open(&directory);
            storage.put(Ns::Graphs, b"graph", b"<http://example.com>").unwrap();
            storage.close().unwrap();
        }
        let storage = open(&directory);
        assert_eq!(
            storage.get(Ns::Graphs, b"graph").unwrap(),
            Some(b"<http://example.com>".to_vec())
        );
        storage.close().unwrap();
    }

    #[test]
    fn rejects_a_database_created_by_another_engine() {
        let directory = TempDir::new().unwrap();
        {
            let storage = open(&directory);
            storage.put(Ns::Meta, META_MAGIC, b"not-oxigraph").unwrap();
            storage.close().unwrap();
        }
        let error = SlateDbStorage::open_at(directory.path()).err().unwrap();
        assert!(
            error.to_string().contains("not been created by Oxigraph"),
            "{error}"
        );
    }

    #[test]
    fn rejects_a_more_recent_format_version() {
        let directory = TempDir::new().unwrap();
        {
            let storage = open(&directory);
            storage
                .put(Ns::Meta, META_FORMAT_VERSION, &2_u32.to_be_bytes())
                .unwrap();
            storage.close().unwrap();
        }
        let error = SlateDbStorage::open_at(directory.path()).err().unwrap();
        assert!(error.to_string().contains("too recent"), "{error}");
    }

    #[test]
    fn handles_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SlateDbStorage>();
        assert_send_sync::<SlateDbStorageReader>();
        assert_send_sync::<SlateDbScan>();
        assert_send_sync::<SlateDbStorageReadableTransaction>();
        assert_send_sync::<SlateDbStorageBulkLoader>();
    }

    #[test]
    #[expect(clippy::panic_in_result_fn)]
    fn test_transaction() -> Result<(), StorageError> {
        let example = NamedNode::new_unchecked("http://example.com/1");
        let example2 = NamedNode::new_unchecked("http://example.com/2");
        let encoded_example = EncodedTerm::from(&example);
        let encoded_example2 = EncodedTerm::from(&example2);
        let default_quad = Quad::new(
            example.clone(),
            example.clone(),
            example.clone(),
            GraphName::DefaultGraph,
        );
        let encoded_default_quad = EncodedQuad::from(&default_quad);
        let named_graph_quad = Quad::new(
            example.clone(),
            example.clone(),
            example.clone(),
            example.clone(),
        );
        let encoded_named_graph_quad = EncodedQuad::from(&named_graph_quad);

        let path = TempDir::new()?;
        let storage = SlateDbStorage::open_at(path.as_ref())?;

        // We start with a graph
        let snapshot = storage.snapshot();
        let mut transaction = storage.start_transaction()?;
        transaction.insert_named_graph(example.clone().into());
        transaction.commit()?;
        assert!(!snapshot.contains_named_graph(&encoded_example)?);
        assert!(storage.snapshot().contains_named_graph(&encoded_example)?);
        storage.snapshot().validate()?;

        // We add two quads
        let snapshot = storage.snapshot();
        let mut transaction = storage.start_transaction()?;
        transaction.insert(default_quad.clone());
        transaction.insert(named_graph_quad.clone());
        transaction.commit()?;
        assert!(!snapshot.contains(&encoded_default_quad)?);
        assert!(!snapshot.contains(&encoded_named_graph_quad)?);
        assert!(storage.snapshot().contains(&encoded_default_quad)?);
        assert!(storage.snapshot().contains(&encoded_named_graph_quad)?);
        storage.snapshot().validate()?;

        // We remove the quads
        let snapshot = storage.snapshot();
        let mut transaction = storage.start_readable_transaction()?;
        transaction.remove(&default_quad);
        let example_graph: NamedOrBlankNode = example.into();
        transaction.remove_named_graph(&example_graph)?;
        transaction.commit()?;
        assert!(snapshot.contains(&encoded_default_quad)?);
        assert!(snapshot.contains(&encoded_named_graph_quad)?);
        assert!(snapshot.contains_named_graph(&encoded_example)?);
        assert!(!storage.snapshot().contains(&encoded_default_quad)?);
        assert!(!storage.snapshot().contains(&encoded_named_graph_quad)?);
        assert!(!storage.snapshot().contains_named_graph(&encoded_example)?);
        storage.snapshot().validate()?;

        // We add the quads again but rollback
        let snapshot = storage.snapshot();
        let mut transaction = storage.start_transaction()?;
        transaction.insert(default_quad.clone());
        transaction.insert(named_graph_quad.clone());
        transaction.insert_named_graph(example2.clone().into());
        drop(transaction);
        assert!(!snapshot.contains(&encoded_default_quad)?);
        assert!(!snapshot.contains(&encoded_named_graph_quad)?);
        assert!(!snapshot.contains_named_graph(&encoded_example)?);
        assert!(!snapshot.contains_named_graph(&encoded_example2)?);
        assert!(!storage.snapshot().contains(&encoded_default_quad)?);
        assert!(!storage.snapshot().contains(&encoded_named_graph_quad)?);
        assert!(!storage.snapshot().contains_named_graph(&encoded_example)?);
        assert!(!storage.snapshot().contains_named_graph(&encoded_example2)?);
        storage.snapshot().validate()?;

        // We add quads and graph, then clear
        let mut loader = storage.bulk_loader();
        loader.load_batch(vec![default_quad, named_graph_quad], 1)?;
        loader.commit()?;
        let mut transaction = storage.start_transaction()?;
        transaction.insert_named_graph(example2.into());
        transaction.commit()?;
        let mut transaction = storage.start_transaction()?;
        transaction.clear();
        transaction.commit()?;
        assert!(!storage.snapshot().contains(&encoded_default_quad)?);
        assert!(!storage.snapshot().contains(&encoded_named_graph_quad)?);
        assert!(!storage.snapshot().contains_named_graph(&encoded_example)?);
        assert!(!storage.snapshot().contains_named_graph(&encoded_example2)?);
        assert!(storage.snapshot().is_empty()?);
        storage.snapshot().validate()?;

        storage.close()?;
        Ok(())
    }

    #[test]
    fn every_quad_pattern_arm_selects_the_same_quads_as_the_memory_backend() {
        use crate::model::Literal;
        use crate::storage::memory::MemoryStorage;

        let s1 = NamedNode::new_unchecked("http://example.com/s1");
        let s2 = NamedNode::new_unchecked("http://example.com/s2");
        let p1 = NamedNode::new_unchecked("http://example.com/p1");
        let p2 = NamedNode::new_unchecked("http://example.com/p2");
        let o1 = Term::from(NamedNode::new_unchecked("http://example.com/o1"));
        let o2 = Term::from(Literal::new_simple_literal("o2"));
        let g1 = GraphName::from(NamedNode::new_unchecked("http://example.com/g1"));
        let g2 = GraphName::from(NamedNode::new_unchecked("http://example.com/g2"));
        let quads = [
            Quad::new(s1.clone(), p1.clone(), o1.clone(), GraphName::DefaultGraph),
            Quad::new(s1.clone(), p2.clone(), o2.clone(), GraphName::DefaultGraph),
            Quad::new(s2.clone(), p1.clone(), o1.clone(), GraphName::DefaultGraph),
            Quad::new(s1.clone(), p1.clone(), o1.clone(), g1.clone()),
            Quad::new(s2.clone(), p2.clone(), o2.clone(), g1.clone()),
            Quad::new(s1.clone(), p2.clone(), o1.clone(), g2.clone()),
        ];

        let directory = TempDir::new().unwrap();
        let storage = open(&directory);
        let mut transaction = storage.start_transaction().unwrap();
        for quad in &quads {
            transaction.insert(quad.clone());
        }
        transaction.commit().unwrap();

        let memory = MemoryStorage::new();
        let mut memory_transaction = memory.start_transaction();
        for quad in &quads {
            memory_transaction.insert(quad.clone());
        }
        memory_transaction.commit();

        let reader = storage.snapshot();
        let memory_reader = memory.snapshot();
        let bound_subject = EncodedTerm::from(&s1);
        let bound_predicate = EncodedTerm::from(&p1);
        let bound_object = EncodedTerm::from(&o1);
        let bound_graph_name = EncodedTerm::from(&g1);

        let mut arms = 0;
        for subject in [None, Some(&bound_subject)] {
            for predicate in [None, Some(&bound_predicate)] {
                for object in [None, Some(&bound_object)] {
                    for graph_name in [None, Some(&bound_graph_name)] {
                        let mut expected = memory_reader
                            .quads_for_pattern(subject, predicate, object, graph_name)
                            .map(|quad| format!("{quad:?}"))
                            .collect::<Vec<_>>();
                        let mut actual = reader
                            .quads_for_pattern(subject, predicate, object, graph_name)
                            .map(|quad| quad.map(|quad| format!("{quad:?}")))
                            .collect::<Result<Vec<_>, _>>()
                            .unwrap();
                        expected.sort_unstable();
                        actual.sort_unstable();
                        assert_eq!(
                            actual, expected,
                            "the pattern {subject:?} {predicate:?} {object:?} {graph_name:?} does not select the same quads as the memory backend"
                        );
                        arms += 1;
                    }
                }
            }
        }
        assert_eq!(
            arms, 16,
            "every arm of the quads_for_pattern decision tree is covered"
        );

        reader.validate().unwrap();
        drop(reader);
        storage.close().unwrap();
    }
}
