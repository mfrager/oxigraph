//! One-way migration of a RocksDB database into a SlateDB one
//! (planning/slatedb/implementation_plan2.md, §7.3).
//!
//! The two backends share the same RDF key encoding (D2) and the SlateDB keyspace of a namespace is
//! the RocksDB column family with a one-byte prefix (D1), so a migration is a copy of raw
//! `(namespace, key, value)` pairs, metadata included. That is what keeps `oxversion` and the RDF
//! encoding version of the source store intact.

use crate::storage::error::StorageError;
use crate::storage::rocksdb::RocksDbStorage;
use crate::storage::slatedb::{SlateDbOptions, SlateDbStorage};
use crate::storage::slatedb_keyspace::{Ns, prefixed};
use crate::store::Store;
use std::io;
use std::path::Path;

/// Key of the progress marker in the metadata namespace of the target database.
const MIGRATION_STATE_KEY: &[u8] = b"migration_state";
/// Magic of the progress marker, so that a foreign key with the same name is not misread.
const MIGRATION_STATE_MAGIC: &[u8] = b"oxigraph-slatedb-migration 1";
/// Number of keys copied between two commits of the target database.
const MIGRATION_BATCH_SIZE: usize = 10_000;

/// What a migration did, for the caller to report.
#[derive(Clone, Debug)]
pub struct MigrationReport {
    /// Number of keys written to the target database by this run.
    pub copied_keys: u64,
    /// Whether the run resumed an interrupted migration instead of starting from scratch.
    pub resumed: bool,
}

/// Copies the RocksDB database of `source` into the SlateDB database described by `options`.
///
/// Refuses to run while another process writes the source. An interrupted migration resumes from
/// the last committed key unless `restart` is set. When the copy is over, the target is reopened
/// and compared with the source: every quad must match, in the same order.
pub(crate) fn migrate(
    source: &Path,
    options: SlateDbOptions,
    restart: bool,
) -> Result<MigrationReport, StorageError> {
    check_no_live_writer(source)?;
    // The source is bound read-only: a migration never writes it (D2, §7.3).
    let source_storage = RocksDbStorage::open_read_only(source)?;
    let target = SlateDbStorage::open_with_options(options.clone())?;
    let report = copy(&source_storage, &target, restart)?;
    target.close()?;
    drop(target);
    verify(source, options)?;
    Ok(report)
}

/// Fails if a writer holds the RocksDB lock of `path`.
///
/// RocksDB locks its `LOCK` file with a POSIX record lock, so taking the same lock is enough to know
/// whether a writer is alive — and closing the file gives the lock back.
#[cfg(unix)]
#[expect(
    unsafe_code,
    reason = "taking the RocksDB lock needs the libc fcntl API"
)]
#[expect(
    clippy::cast_possible_truncation,
    clippy::as_underscore,
    reason = "the fcntl field widths of the libc crate are platform dependent: the constants of \
              F_SETLK, F_RDLCK and SEEK_SET fit in them on every platform it supports"
)]
fn check_no_live_writer(path: &Path) -> Result<(), StorageError> {
    use std::os::unix::io::AsRawFd;

    let lock_path = path.join("LOCK");
    let lock_file = match std::fs::File::open(&lock_path) {
        Ok(file) => file,
        // A database that has never been opened has no lock file, so nobody holds it.
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    // SAFETY: `flock` is a plain C structure whose all-zero bit pattern is valid, and it is fully
    // initialized below before `fcntl` reads it.
    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    // A read lock conflicts with the write lock RocksDB takes, and unlike a write lock it can be
    // requested on a file descriptor opened for reading only, which is all we need here.
    lock.l_type = libc::F_RDLCK as _;
    lock.l_whence = libc::SEEK_SET as _;
    // SAFETY: `lock_file` owns a valid file descriptor for the duration of the call, and `lock`
    // outlives it.
    if unsafe { libc::fcntl(lock_file.as_raw_fd(), libc::F_SETLK, &lock) } == -1 {
        let error = io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::EACCES | libc::EAGAIN) => Err(StorageError::Other(
                format!(
                    "The RocksDB database {} is in use by another process. Close it before migrating it",
                    path.display()
                )
                .into(),
            )),
            _ => Err(error.into()),
        };
    }
    // Dropping `lock_file` releases the lock right away.
    Ok(())
}

/// The live writer check is only implemented where RocksDB takes a POSIX record lock.
#[cfg(not(unix))]
fn check_no_live_writer(_path: &Path) -> Result<(), StorageError> {
    Ok(())
}

/// Where an interrupted migration stopped, and which source it was copying.
#[derive(Clone, Debug, Eq, PartialEq)]
struct MigrationState {
    /// Number of entries of each namespace of the source, in namespace order.
    counts: Vec<(Ns, u64)>,
    /// Last namespace and key copied, computed keys included.
    last_ns: Ns,
    last_key: Vec<u8>,
}

fn copy(
    source: &RocksDbStorage,
    target: &SlateDbStorage,
    restart: bool,
) -> Result<MigrationReport, StorageError> {
    let counts = source.count_per_namespace()?;
    let previous = if restart {
        None
    } else {
        read_state(target)?
    };
    let resumed = previous.is_some();
    let (mut last_ns, mut last_key) = match previous {
        Some(state) => {
            if state.counts != counts {
                return Err(StorageError::Other(
                    "The source database changed since the interrupted migration. Run it again with --restart to copy it from the beginning"
                        .into(),
                ));
            }
            (state.last_ns, state.last_key)
        }
        None => (Ns::Meta, Vec::new()),
    };

    let mut copied_keys = 0;
    let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(MIGRATION_BATCH_SIZE);
    source.export_raw(|ns, key, value| {
        if (ns.as_byte(), key) <= (last_ns.as_byte(), last_key.as_slice()) {
            // Already copied by an interrupted run.
            return Ok(());
        }
        batch.push((prefixed(ns, key), value.to_vec()));
        copied_keys += 1;
        if batch.len() >= MIGRATION_BATCH_SIZE {
            commit_batch(target, &mut batch)?;
            last_ns = ns;
            last_key = key.to_vec();
            write_state(
                target,
                &MigrationState {
                    counts: counts.clone(),
                    last_ns,
                    last_key: last_key.clone(),
                },
            )?;
        }
        Ok(())
    })?;
    if !batch.is_empty() {
        commit_batch(target, &mut batch)?;
    }
    write_state(
        target,
        &MigrationState {
            counts: counts.clone(),
            last_ns,
            last_key,
        },
    )?;
    target.flush()?;
    Ok(MigrationReport {
        copied_keys,
        resumed,
    })
}

fn commit_batch(
    target: &SlateDbStorage,
    batch: &mut Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<(), StorageError> {
    target.write_prefixed(&std::mem::take(batch), &[])?;
    Ok(())
}

fn read_state(target: &SlateDbStorage) -> Result<Option<MigrationState>, StorageError> {
    let Some(value) = target.get_meta(MIGRATION_STATE_KEY)? else {
        return Ok(None);
    };
    decode_state(&value).map(Some).ok_or_else(|| {
        StorageError::Other(
            "The target database holds a migration marker this version cannot read. Run the migration again with --restart"
                .into(),
        )
    })
}

fn write_state(target: &SlateDbStorage, state: &MigrationState) -> Result<(), StorageError> {
    target.write_prefixed(
        &[(
            prefixed(Ns::Meta, MIGRATION_STATE_KEY),
            encode_state(state),
        )],
        &[],
    )?;
    target.flush()
}

fn encode_state(state: &MigrationState) -> Vec<u8> {
    let mut buffer = MIGRATION_STATE_MAGIC.to_vec();
    buffer.push(u8::try_from(state.counts.len()).unwrap_or(u8::MAX));
    for (ns, count) in &state.counts {
        buffer.push(ns.as_byte());
        buffer.extend_from_slice(&count.to_be_bytes());
    }
    buffer.push(state.last_ns.as_byte());
    buffer.extend_from_slice(&u32::try_from(state.last_key.len()).unwrap_or(u32::MAX).to_be_bytes());
    buffer.extend_from_slice(&state.last_key);
    buffer
}

fn decode_state(value: &[u8]) -> Option<MigrationState> {
    let mut reader = value.strip_prefix(MIGRATION_STATE_MAGIC)?;
    let (namespace_count, rest) = reader.split_first()?;
    reader = rest;
    let mut counts = Vec::with_capacity(usize::from(*namespace_count));
    for _ in 0..*namespace_count {
        let (namespace, rest) = reader.split_first()?;
        let (count, rest) = rest.split_at_checked(8)?;
        counts.push((
            Ns::from_byte(*namespace)?,
            u64::from_be_bytes(count.try_into().ok()?),
        ));
        reader = rest;
    }
    let (last_ns, rest) = reader.split_first()?;
    let (key_length, rest) = rest.split_at_checked(4)?;
    let key_length = usize::try_from(u32::from_be_bytes(key_length.try_into().ok()?)).ok()?;
    let last_key = rest.get(..key_length)?;
    Some(MigrationState {
        counts,
        last_ns: Ns::from_byte(*last_ns)?,
        last_key: last_key.to_vec(),
    })
}

/// Compares the copy with the source: the target must validate and yield exactly the same quads in
/// the same order, which is what a dump comparison would check without holding a dump in memory.
fn verify(source: &Path, options: SlateDbOptions) -> Result<(), StorageError> {
    let source = Store::open_read_only(source)?;
    let target = Store::open_with_slate_db(options)?;
    target.validate()?;
    if source.len()? != target.len()? {
        return Err(StorageError::Other(
            format!(
                "The migration is incomplete: the source has {} quads and the target {}",
                source.len()?,
                target.len()?
            )
            .into(),
        ));
    }
    let mut source_quads = source.iter();
    let mut target_quads = target.iter();
    let mut index = 0_u64;
    loop {
        match (source_quads.next(), target_quads.next()) {
            (None, None) => return Ok(()),
            (Some(source_quad), Some(target_quad)) => {
                if source_quad? != target_quad? {
                    return Err(StorageError::Other(
                        format!(
                            "The migration is corrupted: the source and the target differ at quad {index}"
                        )
                        .into(),
                    ));
                }
                index += 1;
            }
            _ => {
                return Err(StorageError::Other(
                    "The migration is corrupted: the source and the target do not have the same number of quads"
                        .into(),
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{NamedNode, Quad};
    use tempfile::TempDir;

    #[test]
    fn state_round_trip() {
        let state = MigrationState {
            counts: vec![(Ns::Meta, 1), (Ns::Id2Str, 42)],
            last_ns: Ns::Dspo,
            last_key: b"a-key".to_vec(),
        };
        assert_eq!(decode_state(&encode_state(&state)), Some(state));
    }

    #[test]
    fn state_rejects_foreign_values() {
        assert_eq!(decode_state(b""), None);
        assert_eq!(decode_state(b"something else"), None);
        let mut state = encode_state(&MigrationState {
            counts: Vec::new(),
            last_ns: Ns::Meta,
            last_key: Vec::new(),
        });
        state.push(0); // A trailing byte the format does not know is ignored, the rest is read.
        assert!(decode_state(&state[..4]).is_none(), "a truncated marker");
    }

    #[test]
    fn migrates_a_rocksdb_database() {
        let source_directory = TempDir::new().unwrap();
        let target_directory = TempDir::new().unwrap();
        let source = Store::open(source_directory.path()).unwrap();
        let ex = NamedNode::new("http://example.com").unwrap();
        let graph: crate::model::GraphName = NamedNode::new("http://example.com/g")
            .unwrap()
            .into();
        for index in 0..500 {
            source
                .insert(Quad::new(
                    ex.clone(),
                    ex.clone(),
                    NamedNode::new(format!("http://example.com/{index}")).unwrap(),
                    graph.clone(),
                ))
                .unwrap();
            source
                .insert(Quad::new(
                    ex.clone(),
                    NamedNode::new(format!("http://example.com/p{index}")).unwrap(),
                    ex.clone(),
                    crate::model::GraphName::DefaultGraph,
                ))
                .unwrap();
        }
        drop(source);

        let options = SlateDbOptions::at_directory(target_directory.path());
        let report = migrate(source_directory.path(), options.clone(), false).unwrap();
        assert!(!report.resumed);
        // 500 quads in a named graph (6 keys each) and 500 in the default graph (3 keys each),
        // plus the `id2str` entries of their terms, the named graph itself and `oxversion`.
        assert!(report.copied_keys > 500 * 6 + 500 * 3);

        let target = Store::open_with_slate_db(options.clone()).unwrap();
        assert_eq!(target.len().unwrap(), 1000);
        assert!(
            target
                .quads_for_pattern(
                    None,
                    None,
                    None,
                    Some(&graph)
                )
                .count()
                == 500
        );
    }

    #[test]
    fn resumes_an_interrupted_migration() {
        let source_directory = TempDir::new().unwrap();
        let target_directory = TempDir::new().unwrap();
        let source = Store::open(source_directory.path()).unwrap();
        let ex = NamedNode::new("http://example.com").unwrap();
        for index in 0..300 {
            source
                .insert(Quad::new(
                    ex.clone(),
                    ex.clone(),
                    NamedNode::new(format!("http://example.com/{index}")).unwrap(),
                    crate::model::GraphName::DefaultGraph,
                ))
                .unwrap();
        }
        drop(source);

        let options = SlateDbOptions::at_directory(target_directory.path());
        // A first run that stopped in the middle: the first 100 keys are copied and the marker is
        // written, exactly like an interrupted migration leaves them.
        let target = SlateDbStorage::open_with_options(options.clone()).unwrap();
        let source = RocksDbStorage::open_read_only(source_directory.path()).unwrap();
        let counts = source.count_per_namespace().unwrap();
        let mut batch = Vec::new();
        let mut last = (Ns::Meta, Vec::new());
        source
            .export_raw(|ns, key, value| {
                if batch.len() < 100 {
                    batch.push((prefixed(ns, key), value.to_vec()));
                    last = (ns, key.to_vec());
                }
                Ok(())
            })
            .unwrap();
        target.write_prefixed(&batch, &[]).unwrap();
        write_state(
            &target,
            &MigrationState {
                counts,
                last_ns: last.0,
                last_key: last.1,
            },
        )
        .unwrap();
        target.close().unwrap();
        drop(target);

        let report = migrate(source_directory.path(), options.clone(), false).unwrap();
        assert!(report.resumed);
        let target = Store::open_with_slate_db(options).unwrap();
        assert_eq!(target.len().unwrap(), 300);
    }

    #[test]
    fn refuses_a_source_that_is_not_a_database() {
        let source_directory = TempDir::new().unwrap();
        let target_directory = TempDir::new().unwrap();
        let error = migrate(
            source_directory.path(),
            SlateDbOptions::at_directory(target_directory.path()),
            false,
        )
        .err();
        assert!(error.is_some(), "a directory without RocksDB data is refused");
    }
}
