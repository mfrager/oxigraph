#![cfg(test)]
//! Fault injection tests of the SlateDB backend (planning/slatedb/implementation_plan2.md, §8.2).
//!
//! SlateDB reaches its storage only through an [`ObjectStore`], so these tests open a database on
//! top of [`ErroringObjectStore`], which fails the object store operations it is told to fail. The
//! assertions are the ones of the plan: a failed open leaves no database behind, a failed commit
//! writes nothing, a failed read yields one error and ends the scan, and the store works again as
//! soon as the object store does.

use super::error::StorageError;
use super::slatedb::{SlateDbOptions, SlateDbScanState, SlateDbStorage};
use super::slatedb_keyspace::Ns;
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use slatedb::config::Settings;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::TempDir;

/// How many of the next operations of each kind [`ErroringObjectStore`] must fail.
#[derive(Debug, Default)]
struct Faults {
    puts: AtomicUsize,
    gets: AtomicUsize,
    /// Number of failures actually returned, to tell an injected failure from a real one.
    injected: AtomicUsize,
}

impl Faults {
    fn fail_puts(&self, count: usize) {
        self.puts.store(count, Ordering::SeqCst);
    }

    fn fail_gets(&self, count: usize) {
        self.gets.store(count, Ordering::SeqCst);
    }

    /// Consumes one failure from `counter`, if any is left.
    fn take(&self, counter: &AtomicUsize) -> bool {
        let mut remaining = counter.load(Ordering::SeqCst);
        loop {
            if remaining == 0 {
                return false;
            }
            match counter.compare_exchange(
                remaining,
                remaining - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    self.injected.fetch_add(1, Ordering::SeqCst);
                    return true;
                }
                Err(actual) => remaining = actual,
            }
        }
    }
}

/// An object store that fails `puts` or `gets` on demand.
#[derive(Debug)]
struct ErroringObjectStore {
    inner: Arc<dyn ObjectStore>,
    faults: Arc<Faults>,
}

impl ErroringObjectStore {
    fn new(inner: Arc<dyn ObjectStore>, faults: Arc<Faults>) -> Self {
        Self { inner, faults }
    }

    fn failure() -> slatedb::object_store::Error {
        slatedb::object_store::Error::Generic {
            store: "fault injection",
            source: "the object store was told to fail this operation".into(),
        }
    }
}

impl Display for ErroringObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ErroringObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ErroringObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> slatedb::object_store::Result<PutResult> {
        if self.faults.take(&self.faults.puts) {
            return Err(Self::failure());
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
        if self.faults.take(&self.faults.puts) {
            return Err(Self::failure());
        }
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> slatedb::object_store::Result<GetResult> {
        if self.faults.take(&self.faults.gets) {
            return Err(Self::failure());
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, slatedb::object_store::Result<Path>>,
    ) -> BoxStream<'static, slatedb::object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> slatedb::object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> slatedb::object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Settings of a test database: no retry, and no background compaction or garbage collection, so
/// that the only object store failures are the injected ones.
fn settings() -> Settings {
    Settings {
        flush_interval: Some(std::time::Duration::from_millis(10)),
        object_store_max_retries: Some(0),
        compactor_options: None,
        garbage_collector_options: None,
        ..Settings::default()
    }
}

fn open(directory: &TempDir, faults: Arc<Faults>) -> Result<SlateDbStorage, StorageError> {
    open_with_settings(directory, faults, settings())
}

fn open_with_settings(
    directory: &TempDir,
    faults: Arc<Faults>,
    settings: Settings,
) -> Result<SlateDbStorage, StorageError> {
    let object_store = LocalFileSystem::new_with_prefix(directory.path())
        .map_err(|error| StorageError::Other(Box::new(error)))?;
    SlateDbStorage::open_with_options_on_store(
        SlateDbOptions::at_directory(directory.path()).with_settings(settings),
        Arc::new(ErroringObjectStore::new(Arc::new(object_store), faults)),
    )
}

#[test]
fn a_failed_open_leaves_no_database() {
    let directory = TempDir::new().unwrap();
    let faults = Arc::new(Faults::default());
    faults.fail_puts(usize::MAX);
    let result = open(&directory, Arc::clone(&faults));
    assert!(result.is_err(), "the open should have failed");
    assert!(
        faults.injected.load(Ordering::SeqCst) > 0,
        "the failure should have been injected"
    );

    // Nothing was written, so the database can be created once the object store works again.
    faults.fail_puts(0);
    let storage = open(&directory, faults).unwrap();
    storage.put(Ns::Spog, b"a", b"1").unwrap();
    assert_eq!(storage.get(Ns::Spog, b"a").unwrap(), Some(b"1".to_vec()));
}

#[test]
fn a_failed_commit_is_not_partially_visible_after_a_reopen() {
    let directory = TempDir::new().unwrap();
    let faults = Arc::new(Faults::default());
    let storage = open(&directory, Arc::clone(&faults)).unwrap();
    storage.put(Ns::Spog, b"first", b"1").unwrap();

    // SlateDB gives up on a writer whose write-ahead log cannot be written, with its retries
    // disabled: the commit reports an error instead of hanging.
    faults.fail_puts(1);
    storage.put(Ns::Spog, b"second", b"2").unwrap_err();
    drop(storage);

    // Reopening the database shows the committed key, not the failed one, and accepts writes
    // again: the failure did not corrupt anything.
    let storage = open(&directory, Arc::clone(&faults)).unwrap();
    assert_eq!(
        storage.get(Ns::Spog, b"first").unwrap(),
        Some(b"1".to_vec())
    );
    assert!(storage.get(Ns::Spog, b"second").unwrap().is_none());
    storage.put(Ns::Spog, b"third", b"3").unwrap();
    assert_eq!(
        storage.get(Ns::Spog, b"third").unwrap(),
        Some(b"3".to_vec())
    );
    storage.close().unwrap();
}

#[test]
fn a_commit_retries_a_transient_object_store_failure() {
    let directory = TempDir::new().unwrap();
    let faults = Arc::new(Faults::default());
    // The default retry policy of SlateDB is unbounded; a bounded one is enough to recover here.
    let storage = open_with_settings(
        &directory,
        Arc::clone(&faults),
        Settings {
            object_store_max_retries: Some(3),
            ..settings()
        },
    )
    .unwrap();
    faults.fail_puts(1);
    storage.put(Ns::Spog, b"retried", b"1").unwrap();
    assert_eq!(
        storage.get(Ns::Spog, b"retried").unwrap(),
        Some(b"1".to_vec())
    );
    storage.close().unwrap();
}

#[test]
fn a_failed_scan_yields_one_error_and_ends_the_iterator() {
    let directory = TempDir::new().unwrap();
    let faults = Arc::new(Faults::default());
    let storage = open(&directory, Arc::clone(&faults)).unwrap();
    storage.put(Ns::Spog, b"a", b"1").unwrap();
    storage.put(Ns::Spog, b"b", b"2").unwrap();
    storage.flush().unwrap();
    storage.close().unwrap();
    drop(storage);

    // Reopening makes the data live in the object store rather than in a memtable.
    let storage = open(&directory, Arc::clone(&faults)).unwrap();
    faults.fail_gets(usize::MAX);
    let mut scan = SlateDbScanState::new(storage.try_scan(Ns::Spog, b""));
    let first = scan.next_key_value();
    assert!(
        matches!(first, Some(Err(_))),
        "the iterator should yield its error once"
    );
    assert!(
        scan.next_key_value().is_none(),
        "the iterator should be over after its error"
    );

    // A new scan works once the object store does.
    faults.fail_gets(0);
    let mut scan = SlateDbScanState::new(storage.try_scan(Ns::Spog, b""));
    let items: Vec<_> = std::iter::from_fn(|| scan.next_key_value()).collect();
    assert_eq!(items.len(), 2, "the scan should see both keys: {items:?}");
    assert!(items.iter().all(Result::is_ok));
    storage.close().unwrap();
}
