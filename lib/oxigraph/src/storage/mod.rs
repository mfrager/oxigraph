use crate::model::{GraphName, NamedOrBlankNode, OxString, Quad};
pub use crate::storage::error::{CorruptionError, LoaderError, SerializerError, StorageError};
use crate::storage::memory::{
    MemoryDecodingGraphIterator, MemoryStorage, MemoryStorageBulkLoader, MemoryStorageReader,
    MemoryStorageTransaction, QuadIterator,
};
use crate::storage::numeric_encoder::{EncodedQuad, EncodedTerm, StrHash, StrLookup};
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
use crate::storage::rocksdb::{
    RocksDbChainedDecodingQuadIterator, RocksDbDecodingGraphIterator, RocksDbStorage,
    RocksDbStorageBulkLoader, RocksDbStorageOptions, RocksDbStorageReadableTransaction,
    RocksDbStorageReader, RocksDbStorageTransaction,
};
#[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
use crate::storage::slatedb::{
    SlateDbChainedDecodingQuadIterator, SlateDbDecodingGraphIterator, SlateDbStorage,
    SlateDbStorageBulkLoader, SlateDbStorageReadableTransaction, SlateDbStorageReader,
    SlateDbStorageTransaction,
};
#[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
pub use crate::storage::slatedb::{SlateDbDurability, SlateDbObjectStore, SlateDbOptions};
#[cfg(all(
    not(target_family = "wasm"),
    any(feature = "rocksdb", feature = "slatedb")
))]
use std::path::Path;
#[cfg(not(target_family = "wasm"))]
use std::{io, thread};

#[cfg(all(
    not(target_family = "wasm"),
    any(feature = "rocksdb", feature = "slatedb")
))]
mod binary_encoder;
#[cfg(all(
    not(target_family = "wasm"),
    any(feature = "rocksdb", feature = "slatedb")
))]
mod bulk_encoder;
mod error;
mod memory;
/// One-way RocksDB to SlateDB migration (planning/slatedb/implementation_plan2.md, §7.3).
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb", feature = "slatedb"))]
pub(crate) mod migration;
pub mod numeric_encoder;
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
mod rocksdb;
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
mod rocksdb_wrapper;
#[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
mod slatedb;
#[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
mod slatedb_bridge;
#[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
mod slatedb_keyspace;
pub mod small_string;

// Backend gate: code that only needs *some* on-disk backend, whichever it is, is gated on
//     all(not(target_family = "wasm"), any(feature = "rocksdb", feature = "slatedb"))
// and must keep compiling with `feature = "slatedb"` alone. The backend-neutral constructor
// `Storage::open` follows that gate and picks RocksDB when it is enabled, SlateDB otherwise. Code
// that only makes sense for one backend (`open_with_options`, `open_read_only`, `StorageOptions`)
// stays gated on its own feature.
pub const DEFAULT_BULK_LOAD_BATCH_SIZE: usize = 1_000_000;

#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct StorageOptions {
    max_open_files: Option<i32>,
    fd_reserve: Option<u32>,
}

#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
impl StorageOptions {
    pub(crate) fn new(max_open_files: Option<i32>, fd_reserve: Option<u32>) -> Self {
        Self {
            max_open_files,
            fd_reserve,
        }
    }
}

/// Low level storage primitives
#[derive(Clone)]
pub struct Storage {
    kind: StorageKind,
}

#[derive(Clone)]
enum StorageKind {
    #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
    RocksDb(RocksDbStorage),
    #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
    SlateDb(SlateDbStorage),
    Memory(MemoryStorage),
}

impl Storage {
    #[expect(clippy::unnecessary_wraps)]
    pub fn new() -> Result<Self, StorageError> {
        Ok(Self {
            kind: StorageKind::Memory(MemoryStorage::new()),
        })
    }

    /// Opens the default on-disk backend, creating its database if needed.
    ///
    /// RocksDB is preferred when both backends are enabled; it is the backend Oxigraph has always
    /// used. A build with only the `slatedb` feature opens a SlateDB database instead.
    #[cfg(all(
        not(target_family = "wasm"),
        any(feature = "rocksdb", feature = "slatedb")
    ))]
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        #[cfg(feature = "rocksdb")]
        {
            Ok(Self {
                kind: StorageKind::RocksDb(RocksDbStorage::open(path)?),
            })
        }
        #[cfg(not(feature = "rocksdb"))]
        {
            Ok(Self {
                kind: StorageKind::SlateDb(SlateDbStorage::open_at(path)?),
            })
        }
    }

    /// Opens the SlateDB database described by `options`, creating it if needed.
    #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
    pub fn open_slatedb_with_options(options: SlateDbOptions) -> Result<Self, StorageError> {
        Ok(Self {
            kind: StorageKind::SlateDb(SlateDbStorage::open_with_options(options)?),
        })
    }

    /// Opens the SlateDB database described by `options` without taking the writer role.
    #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
    pub fn open_slatedb_read_only(options: SlateDbOptions) -> Result<Self, StorageError> {
        Ok(Self {
            kind: StorageKind::SlateDb(SlateDbStorage::open_with_options(
                options.with_read_only(true),
            )?),
        })
    }

    #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
    pub fn open_with_options(path: &Path, options: StorageOptions) -> Result<Self, StorageError> {
        Ok(Self {
            kind: StorageKind::RocksDb(RocksDbStorage::open_with_options(
                path,
                RocksDbStorageOptions {
                    max_open_files: options.max_open_files,
                    fd_reserve: options.fd_reserve,
                },
            )?),
        })
    }

    #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
    pub fn open_read_only(path: &Path) -> Result<Self, StorageError> {
        Ok(Self {
            kind: StorageKind::RocksDb(RocksDbStorage::open_read_only(path)?),
        })
    }

    pub fn snapshot(&self) -> StorageReader<'static> {
        StorageReader {
            kind: match &self.kind {
                #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
                StorageKind::RocksDb(storage) => StorageReaderKind::RocksDb(storage.snapshot()),
                #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
                StorageKind::SlateDb(storage) => StorageReaderKind::SlateDb(storage.snapshot()),
                StorageKind::Memory(storage) => StorageReaderKind::Memory(storage.snapshot()),
            },
        }
    }

    #[cfg_attr(
        not(all(not(target_family = "wasm"), feature = "rocksdb")),
        expect(clippy::unnecessary_wraps)
    )]
    pub fn start_transaction(&self) -> Result<StorageTransaction<'_>, StorageError> {
        Ok(StorageTransaction {
            kind: match &self.kind {
                #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
                StorageKind::RocksDb(storage) => {
                    StorageTransactionKind::RocksDb(storage.start_transaction()?)
                }
                #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
                StorageKind::SlateDb(storage) => {
                    StorageTransactionKind::SlateDb(storage.start_transaction()?)
                }
                StorageKind::Memory(storage) => {
                    StorageTransactionKind::Memory(storage.start_transaction())
                }
            },
        })
    }

    #[cfg_attr(
        not(all(
            not(target_family = "wasm"),
            any(feature = "rocksdb", feature = "slatedb")
        )),
        expect(clippy::unnecessary_wraps)
    )]
    pub fn start_readable_transaction(
        &self,
    ) -> Result<StorageReadableTransaction<'_>, StorageError> {
        Ok(StorageReadableTransaction {
            kind: match &self.kind {
                #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
                StorageKind::RocksDb(storage) => {
                    StorageReadableTransactionKind::RocksDb(storage.start_readable_transaction()?)
                }
                #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
                StorageKind::SlateDb(storage) => StorageReadableTransactionKind::SlateDb(
                    storage.start_readable_transaction()?,
                ),
                StorageKind::Memory(storage) => {
                    StorageReadableTransactionKind::Memory(storage.start_transaction())
                }
            },
        })
    }

    #[cfg(all(
        not(target_family = "wasm"),
        any(feature = "rocksdb", feature = "slatedb")
    ))]
    #[cfg_attr(
        not(all(
            not(target_family = "wasm"),
            any(feature = "rocksdb", feature = "slatedb")
        )),
        expect(clippy::unnecessary_wraps)
    )]
    pub fn flush(&self) -> Result<(), StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageKind::RocksDb(storage) => storage.flush(),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageKind::SlateDb(storage) => storage.flush(),
            StorageKind::Memory(_) => Ok(()),
        }
    }

    #[cfg(all(
        not(target_family = "wasm"),
        any(feature = "rocksdb", feature = "slatedb")
    ))]
    #[cfg_attr(
        not(all(
            not(target_family = "wasm"),
            any(feature = "rocksdb", feature = "slatedb")
        )),
        expect(clippy::unnecessary_wraps)
    )]
    #[cfg_attr(
        all(not(feature = "rocksdb"), feature = "slatedb"),
        expect(clippy::unnecessary_wraps, reason = "both on-disk backends can compact")
    )]
    pub fn compact(&self) -> Result<(), StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageKind::RocksDb(storage) => storage.compact(),
            // SlateDB compacts itself in the background (planning/slatedb/implementation_plan2.md, D12).
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageKind::SlateDb(_) => Ok(()),
            StorageKind::Memory(_) => Ok(()),
        }
    }

    #[cfg(all(
        not(target_family = "wasm"),
        any(feature = "rocksdb", feature = "slatedb")
    ))]
    pub fn backup(
        &self,
        #[cfg_attr(
            not(any(feature = "rocksdb", feature = "slatedb")),
            expect(unused_variables, reason = "only on-disk databases have a backup")
        )]
        target_directory: &Path,
    ) -> Result<(), StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageKind::RocksDb(storage) => storage.backup(target_directory),
            // SlateDB backups are logical copies streamed from a snapshot (plan, D11).
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageKind::SlateDb(storage) => storage.backup(target_directory),
            StorageKind::Memory(_) => Err(StorageError::Other(
                "It is not possible to backup an in-memory database".into(),
            )),
        }
    }

    pub fn bulk_loader(&self) -> StorageBulkLoader<'_> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageKind::RocksDb(storage) => StorageBulkLoader {
                kind: StorageBulkLoaderKind::RocksDb(storage.bulk_loader()),
            },
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageKind::SlateDb(storage) => StorageBulkLoader {
                kind: StorageBulkLoaderKind::SlateDb(storage.bulk_loader()),
            },
            StorageKind::Memory(storage) => StorageBulkLoader {
                kind: StorageBulkLoaderKind::Memory(storage.bulk_loader()),
            },
        }
    }
}

#[must_use]
pub struct StorageReader<'a> {
    kind: StorageReaderKind<'a>,
}

enum StorageReaderKind<'a> {
    #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
    RocksDb(RocksDbStorageReader<'a>),
    #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
    SlateDb(SlateDbStorageReader),
    Memory(MemoryStorageReader<'a>),
}

#[cfg_attr(
    not(all(
        not(target_family = "wasm"),
        any(feature = "rocksdb", feature = "slatedb")
    )),
    expect(clippy::unnecessary_wraps)
)]
impl<'a> StorageReader<'a> {
    pub fn len(&self) -> Result<usize, StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageReaderKind::RocksDb(reader) => reader.len(),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageReaderKind::SlateDb(reader) => reader.len(),
            StorageReaderKind::Memory(reader) => Ok(reader.len()),
        }
    }

    pub fn is_empty(&self) -> Result<bool, StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageReaderKind::RocksDb(reader) => reader.is_empty(),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageReaderKind::SlateDb(reader) => reader.is_empty(),
            StorageReaderKind::Memory(reader) => Ok(reader.is_empty()),
        }
    }

    pub fn contains(&self, quad: &EncodedQuad) -> Result<bool, StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageReaderKind::RocksDb(reader) => reader.contains(quad),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageReaderKind::SlateDb(reader) => reader.contains(quad),
            StorageReaderKind::Memory(reader) => Ok(reader.contains(quad)),
        }
    }

    pub fn quads_for_pattern(
        &self,
        subject: Option<&EncodedTerm>,
        predicate: Option<&EncodedTerm>,
        object: Option<&EncodedTerm>,
        graph_name: Option<&EncodedTerm>,
    ) -> DecodingQuadIterator<'a> {
        DecodingQuadIterator {
            kind: match &self.kind {
                #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
                StorageReaderKind::RocksDb(reader) => DecodingQuadIteratorKind::RocksDb(
                    reader.quads_for_pattern(subject, predicate, object, graph_name),
                ),
                #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
                StorageReaderKind::SlateDb(reader) => DecodingQuadIteratorKind::SlateDb(
                    reader.quads_for_pattern(subject, predicate, object, graph_name),
                ),
                StorageReaderKind::Memory(reader) => DecodingQuadIteratorKind::Memory(
                    reader.quads_for_pattern(subject, predicate, object, graph_name),
                ),
            },
        }
    }

    pub fn named_graphs(&self) -> DecodingGraphIterator<'a> {
        DecodingGraphIterator {
            kind: match &self.kind {
                #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
                StorageReaderKind::RocksDb(reader) => {
                    DecodingGraphIteratorKind::RocksDb(reader.named_graphs())
                }
                #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
                StorageReaderKind::SlateDb(reader) => {
                    DecodingGraphIteratorKind::SlateDb(reader.named_graphs())
                }
                StorageReaderKind::Memory(reader) => {
                    DecodingGraphIteratorKind::Memory(reader.named_graphs())
                }
            },
        }
    }

    pub fn contains_named_graph(&self, graph_name: &EncodedTerm) -> Result<bool, StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageReaderKind::RocksDb(reader) => reader.contains_named_graph(graph_name),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageReaderKind::SlateDb(reader) => reader.contains_named_graph(graph_name),
            StorageReaderKind::Memory(reader) => Ok(reader.contains_named_graph(graph_name)),
        }
    }

    pub fn contains_str(&self, key: &StrHash) -> Result<bool, StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageReaderKind::RocksDb(reader) => reader.contains_str(key),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageReaderKind::SlateDb(reader) => reader.contains_str(key),
            StorageReaderKind::Memory(reader) => Ok(reader.contains_str(key)),
        }
    }

    /// Validate that all the storage invariants held in the data
    pub fn validate(&self) -> Result<(), StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageReaderKind::RocksDb(reader) => reader.validate(),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageReaderKind::SlateDb(reader) => reader.validate(),
            StorageReaderKind::Memory(reader) => reader.validate(),
        }
    }
}

#[must_use]
pub struct DecodingQuadIterator<'a> {
    kind: DecodingQuadIteratorKind<'a>,
}

enum DecodingQuadIteratorKind<'a> {
    #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
    RocksDb(RocksDbChainedDecodingQuadIterator<'a>),
    #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
    SlateDb(SlateDbChainedDecodingQuadIterator),
    Memory(QuadIterator<'a>),
}

impl Iterator for DecodingQuadIterator<'_> {
    type Item = Result<EncodedQuad, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            DecodingQuadIteratorKind::RocksDb(iter) => iter.next(),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            DecodingQuadIteratorKind::SlateDb(iter) => iter.next(),
            DecodingQuadIteratorKind::Memory(iter) => iter.next().map(Ok),
        }
    }
}

#[must_use]
pub struct DecodingGraphIterator<'a> {
    kind: DecodingGraphIteratorKind<'a>,
}

enum DecodingGraphIteratorKind<'a> {
    #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
    RocksDb(RocksDbDecodingGraphIterator<'a>),
    #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
    SlateDb(SlateDbDecodingGraphIterator),
    Memory(MemoryDecodingGraphIterator<'a>),
}

impl Iterator for DecodingGraphIterator<'_> {
    type Item = Result<EncodedTerm, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            DecodingGraphIteratorKind::RocksDb(iter) => iter.next(),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            DecodingGraphIteratorKind::SlateDb(iter) => iter.next(),
            DecodingGraphIteratorKind::Memory(iter) => iter.next().map(Ok),
        }
    }
}

impl StrLookup for StorageReader<'_> {
    fn get_str(&self, key: &StrHash) -> Result<Option<OxString>, StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageReaderKind::RocksDb(reader) => reader.get_str(key),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageReaderKind::SlateDb(reader) => reader.get_str(key),
            StorageReaderKind::Memory(reader) => reader.get_str(key),
        }
    }
}

#[must_use]
pub struct StorageTransaction<'a> {
    kind: StorageTransactionKind<'a>,
}

enum StorageTransactionKind<'a> {
    #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
    RocksDb(RocksDbStorageTransaction<'a>),
    #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
    SlateDb(SlateDbStorageTransaction),
    Memory(MemoryStorageTransaction<'a>),
}

#[cfg_attr(
    not(all(
        not(target_family = "wasm"),
        any(feature = "rocksdb", feature = "slatedb")
    )),
    expect(clippy::unnecessary_wraps)
)]
impl StorageTransaction<'_> {
    pub fn insert(&mut self, quad: Quad) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageTransactionKind::RocksDb(transaction) => transaction.insert(quad),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageTransactionKind::SlateDb(transaction) => transaction.insert(quad),
            StorageTransactionKind::Memory(transaction) => {
                transaction.insert(quad);
            }
        }
    }

    pub fn insert_named_graph(&mut self, graph_name: NamedOrBlankNode) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageTransactionKind::RocksDb(transaction) => {
                transaction.insert_named_graph(graph_name)
            }
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageTransactionKind::SlateDb(transaction) => {
                transaction.insert_named_graph(graph_name)
            }
            StorageTransactionKind::Memory(transaction) => {
                transaction.insert_named_graph(graph_name);
            }
        }
    }

    pub fn remove(&mut self, quad: &Quad) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageTransactionKind::RocksDb(transaction) => transaction.remove(quad),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageTransactionKind::SlateDb(transaction) => transaction.remove(quad),
            StorageTransactionKind::Memory(transaction) => transaction.remove(quad),
        }
    }

    pub fn clear_default_graph(&mut self) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageTransactionKind::RocksDb(transaction) => transaction.clear_default_graph(),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageTransactionKind::SlateDb(transaction) => transaction.clear_default_graph(),
            StorageTransactionKind::Memory(transaction) => {
                transaction.clear_graph(&GraphName::DefaultGraph)
            }
        }
    }

    pub fn clear_all_named_graphs(&mut self) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageTransactionKind::RocksDb(transaction) => transaction.clear_all_named_graphs(),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageTransactionKind::SlateDb(transaction) => transaction.clear_all_named_graphs(),
            StorageTransactionKind::Memory(transaction) => transaction.clear_all_named_graphs(),
        }
    }

    pub fn clear_all_graphs(&mut self) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageTransactionKind::RocksDb(transaction) => transaction.clear_all_graphs(),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageTransactionKind::SlateDb(transaction) => transaction.clear_all_graphs(),
            StorageTransactionKind::Memory(transaction) => transaction.clear_all_graphs(),
        }
    }

    pub fn remove_all_named_graphs(&mut self) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageTransactionKind::RocksDb(transaction) => transaction.remove_all_named_graphs(),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageTransactionKind::SlateDb(transaction) => transaction.remove_all_named_graphs(),
            StorageTransactionKind::Memory(transaction) => transaction.remove_all_named_graphs(),
        }
    }

    pub fn clear(&mut self) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageTransactionKind::RocksDb(transaction) => transaction.clear(),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageTransactionKind::SlateDb(transaction) => transaction.clear(),
            StorageTransactionKind::Memory(transaction) => transaction.clear(),
        }
    }

    pub fn commit(self) -> Result<(), StorageError> {
        match self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageTransactionKind::RocksDb(transaction) => transaction.commit(),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageTransactionKind::SlateDb(transaction) => transaction.commit(),
            StorageTransactionKind::Memory(transaction) => {
                transaction.commit();
                Ok(())
            }
        }
    }
}

#[must_use]
pub struct StorageReadableTransaction<'a> {
    kind: StorageReadableTransactionKind<'a>,
}

enum StorageReadableTransactionKind<'a> {
    #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
    RocksDb(RocksDbStorageReadableTransaction<'a>),
    #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
    SlateDb(SlateDbStorageReadableTransaction),
    Memory(MemoryStorageTransaction<'a>),
}

#[cfg_attr(
    not(all(
        not(target_family = "wasm"),
        any(feature = "rocksdb", feature = "slatedb")
    )),
    expect(clippy::unnecessary_wraps)
)]
impl StorageReadableTransaction<'_> {
    pub fn reader(&self) -> StorageReader<'_> {
        StorageReader {
            kind: match &self.kind {
                #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
                StorageReadableTransactionKind::RocksDb(transaction) => {
                    StorageReaderKind::RocksDb(transaction.reader())
                }
                #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
                StorageReadableTransactionKind::SlateDb(transaction) => {
                    StorageReaderKind::SlateDb(transaction.reader())
                }
                StorageReadableTransactionKind::Memory(transaction) => {
                    StorageReaderKind::Memory(transaction.reader())
                }
            },
        }
    }

    pub fn insert(&mut self, quad: Quad) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageReadableTransactionKind::RocksDb(transaction) => transaction.insert(quad),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageReadableTransactionKind::SlateDb(transaction) => transaction.insert(quad),
            StorageReadableTransactionKind::Memory(transaction) => {
                transaction.insert(quad);
            }
        }
    }

    pub fn insert_named_graph(&mut self, graph_name: NamedOrBlankNode) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageReadableTransactionKind::RocksDb(transaction) => {
                transaction.insert_named_graph(graph_name)
            }
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageReadableTransactionKind::SlateDb(transaction) => {
                transaction.insert_named_graph(graph_name)
            }
            StorageReadableTransactionKind::Memory(transaction) => {
                transaction.insert_named_graph(graph_name);
            }
        }
    }

    pub fn remove(&mut self, quad: &Quad) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageReadableTransactionKind::RocksDb(transaction) => transaction.remove(quad),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageReadableTransactionKind::SlateDb(transaction) => transaction.remove(quad),
            StorageReadableTransactionKind::Memory(transaction) => transaction.remove(quad),
        }
    }

    pub fn clear_graph(&mut self, graph_name: &GraphName) -> Result<(), StorageError> {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageReadableTransactionKind::RocksDb(transaction) => {
                transaction.clear_graph(graph_name)
            }
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageReadableTransactionKind::SlateDb(transaction) => {
                transaction.clear_graph(graph_name)
            }
            StorageReadableTransactionKind::Memory(transaction) => {
                transaction.clear_graph(graph_name);
                Ok(())
            }
        }
    }

    pub fn clear_all_named_graphs(&mut self) -> Result<(), StorageError> {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageReadableTransactionKind::RocksDb(transaction) => {
                transaction.clear_all_named_graphs()
            }
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageReadableTransactionKind::SlateDb(transaction) => {
                transaction.clear_all_named_graphs()
            }
            StorageReadableTransactionKind::Memory(transaction) => {
                transaction.clear_all_named_graphs();
                Ok(())
            }
        }
    }

    pub fn clear_all_graphs(&mut self) -> Result<(), StorageError> {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageReadableTransactionKind::RocksDb(transaction) => transaction.clear_all_graphs(),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageReadableTransactionKind::SlateDb(transaction) => transaction.clear_all_graphs(),
            StorageReadableTransactionKind::Memory(transaction) => {
                transaction.clear_all_graphs();
                Ok(())
            }
        }
    }

    pub fn remove_named_graph(
        &mut self,
        graph_name: &NamedOrBlankNode,
    ) -> Result<(), StorageError> {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageReadableTransactionKind::RocksDb(transaction) => {
                transaction.remove_named_graph(graph_name)
            }
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageReadableTransactionKind::SlateDb(transaction) => {
                transaction.remove_named_graph(graph_name)
            }
            StorageReadableTransactionKind::Memory(transaction) => {
                transaction.remove_named_graph(graph_name);
                Ok(())
            }
        }
    }

    pub fn remove_all_named_graphs(&mut self) -> Result<(), StorageError> {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageReadableTransactionKind::RocksDb(transaction) => {
                transaction.remove_all_named_graphs()
            }
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageReadableTransactionKind::SlateDb(transaction) => {
                transaction.remove_all_named_graphs()
            }
            StorageReadableTransactionKind::Memory(transaction) => {
                transaction.remove_all_named_graphs();
                Ok(())
            }
        }
    }

    pub fn clear(&mut self) -> Result<(), StorageError> {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageReadableTransactionKind::RocksDb(transaction) => transaction.clear(),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageReadableTransactionKind::SlateDb(transaction) => transaction.clear(),
            StorageReadableTransactionKind::Memory(transaction) => {
                transaction.clear();
                Ok(())
            }
        }
    }

    pub fn commit(self) -> Result<(), StorageError> {
        match self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageReadableTransactionKind::RocksDb(transaction) => transaction.commit(),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageReadableTransactionKind::SlateDb(transaction) => transaction.commit(),
            StorageReadableTransactionKind::Memory(transaction) => {
                transaction.commit();
                Ok(())
            }
        }
    }
}

#[must_use]
pub struct StorageBulkLoader<'a> {
    kind: StorageBulkLoaderKind<'a>,
}

enum StorageBulkLoaderKind<'a> {
    #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
    RocksDb(RocksDbStorageBulkLoader<'a>),
    #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
    SlateDb(SlateDbStorageBulkLoader),
    Memory(MemoryStorageBulkLoader<'a>),
}

impl StorageBulkLoader<'_> {
    pub fn on_progress(self, callback: impl Fn(u64) + Send + Sync + 'static) -> Self {
        match self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageBulkLoaderKind::RocksDb(loader) => Self {
                kind: StorageBulkLoaderKind::RocksDb(loader.on_progress(callback)),
            },
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageBulkLoaderKind::SlateDb(loader) => Self {
                kind: StorageBulkLoaderKind::SlateDb(loader.on_progress(callback)),
            },
            StorageBulkLoaderKind::Memory(loader) => Self {
                kind: StorageBulkLoaderKind::Memory(loader.on_progress(callback)),
            },
        }
    }

    pub fn without_atomicity(self) -> Self {
        match self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageBulkLoaderKind::RocksDb(loader) => Self {
                kind: StorageBulkLoaderKind::RocksDb(loader.without_atomicity()),
            },
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageBulkLoaderKind::SlateDb(loader) => Self {
                kind: StorageBulkLoaderKind::SlateDb(loader.without_atomicity()),
            },
            StorageBulkLoaderKind::Memory(loader) => Self {
                kind: StorageBulkLoaderKind::Memory(loader),
            },
        }
    }

    #[cfg_attr(
        any(
            target_family = "wasm",
            not(any(feature = "rocksdb", feature = "slatedb"))
        ),
        expect(clippy::unnecessary_wraps, unused_variables)
    )]
    pub fn load_batch(
        &mut self,
        quads: Vec<Quad>,
        max_num_threads: usize,
    ) -> Result<(), StorageError> {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageBulkLoaderKind::RocksDb(loader) => loader.load_batch(quads, max_num_threads),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageBulkLoaderKind::SlateDb(loader) => loader.load_batch(quads, max_num_threads),
            StorageBulkLoaderKind::Memory(loader) => {
                loader.load_batch(quads);
                Ok(())
            }
        }
    }

    #[cfg_attr(
        any(
            target_family = "wasm",
            not(any(feature = "rocksdb", feature = "slatedb"))
        ),
        expect(clippy::unnecessary_wraps)
    )]
    pub fn commit(self) -> Result<(), StorageError> {
        match self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
            StorageBulkLoaderKind::RocksDb(loader) => loader.commit(),
            #[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
            StorageBulkLoaderKind::SlateDb(loader) => loader.commit(),
            StorageBulkLoaderKind::Memory(loader) => {
                loader.commit();
                Ok(())
            }
        }
    }
}

#[cfg(not(target_family = "wasm"))]
pub fn map_thread_result<R>(result: thread::Result<R>) -> io::Result<R> {
    result.map_err(|e| {
        io::Error::other(if let Ok(e) = e.downcast::<&dyn std::fmt::Display>() {
            format!("A loader processed crashed with {e}")
        } else {
            "A loader processed crashed with and unknown error".into()
        })
    })
}
