# Storage Backend Abstraction for Oxigraph

## Purpose

Define a clean abstraction layer that allows Oxigraph to support multiple storage backends (RocksDB, SlateDB, FalkorDB) with swappable implementations, while preserving Oxigraph's RDF semantics, SPARQL engine, and transaction model.

This abstraction separates:
- **RDF semantics** (Oxigraph core) — terms, quads, SPARQL, transactions
- **Storage engine interface** — quad indexing, term dictionary, persistence, snapshots
- **Backend implementations** — RocksDB, SlateDB, FalkorDB (with native persistence)

---

## 1. Core Abstraction: `StorageBackend`

```rust
// lib/oxigraph/src/storage/backend.rs

use crate::model::{GraphName, NamedOrBlankNode, Quad, Term};
use crate::storage::numeric_encoder::{EncodedQuad, EncodedTerm, Decoder};
use crate::storage::{StorageError, StorageReader, StorageBulkLoader, StorageReadableTransaction};
use std::sync::Arc;

/// Capabilities a storage backend may support
#[derive(Clone, Debug, Default)]
pub struct BackendCapabilities {
    /// Supports durable persistence (WAL, checkpoints)
    pub durable: bool,
    /// Supports snapshot isolation (readers don't block writers)
    pub snapshot_isolation: bool,
    /// Supports read-only mode without writer fence
    pub read_only_mode: bool,
    /// Supports atomic bulk load
    pub atomic_bulk_load: bool,
    /// Supports non-atomic streaming bulk load
    pub streaming_bulk_load: bool,
    /// Supports backup to object store
    pub backup: bool,
    /// Supports named graphs natively
    pub named_graphs: bool,
    /// Supports efficient graph traversal (adjacency, multi-hop)
    pub graph_traversal: bool,
    /// Supports GraphBLAS / sparse matrix operations
    pub graphblas: bool,
    /// Supports MVCC / copy-on-write snapshots
    pub mvcc: bool,
    /// Supports object store (S3, etc.)
    pub object_store: bool,
    /// Supports compression
    pub compression: bool,
}

/// Configuration for opening a backend
#[derive(Clone, Debug)]
pub enum BackendConfig {
    /// In-memory only (for testing)
    Memory,
    /// RocksDB backend
    #[cfg(feature = "rocksdb")]
    RocksDb(crate::storage::StorageOptions),
    /// SlateDB backend
    #[cfg(feature = "slatedb")]
    SlateDb(crate::storage::SlateDbOptions),
    /// FalkorDB backend (with native persistence)
    #[cfg(feature = "falkordb")]
    FalkorDb(FalkorDbConfig),
}

/// FalkorDB-specific configuration
#[cfg(feature = "falkordb")]
#[derive(Clone, Debug)]
pub struct FalkorDbConfig {
    /// Data directory for persistence (WAL + checkpoints)
    pub data_dir: std::path::PathBuf,
    /// FalkorDB graph engine options
    pub graph_options: FalkorGraphOptions,
    /// Durability mode
    pub durability: FalkorDurability,
    /// Checkpoint interval (operations or time)
    pub checkpoint_interval: CheckpointInterval,
    /// WAL sync mode
    pub wal_sync: WalSyncMode,
    /// Maximum WAL size before forcing checkpoint
    pub max_wal_size: u64,
}

#[cfg(feature = "falkordb")]
#[derive(Clone, Debug)]
pub struct FalkorGraphOptions {
    /// Use typed relationships (one per predicate) vs generic RDF_EDGE
    pub typed_relationships: bool,
    /// Predicate type ID cache size
    pub predicate_cache_size: usize,
    /// Enable GraphBLAS traversal optimizations
    pub enable_graphblas: bool,
}

#[cfg(feature = "falkordb")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FalkorDurability {
    /// In-memory only, no durability
    #[default]
    None,
    /// Periodic checkpoints + WAL (async fsync)
    CheckpointWal,
    /// Full durability (WAL fsync on every commit + checkpoints)
    Full,
}

#[cfg(feature = "falkordb")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum WalSyncMode {
    /// No fsync (fastest, least durable)
    #[default]
    None,
    /// fsync periodically
    Periodic,
    /// fsync on every commit (most durable)
    Always,
}

#[cfg(feature = "falkordb")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CheckpointInterval {
    /// Checkpoint every N transactions
    Transactions(u64),
    /// Checkpoint every N seconds
    #[default]
    Seconds(u64),
    /// Checkpoint on WAL size
    WalSize(u64),
}

/// Main storage backend trait
pub trait StorageBackend: Send + Sync + 'static {
    /// Backend identifier (e.g., "rocksdb", "slatedb", "falkordb")
    const NAME: &'static str;

    /// Backend capabilities
    fn capabilities(&self) -> BackendCapabilities;

    /// Open/create the backend
    fn open(config: BackendConfig) -> Result<Self, StorageError>
    where
        Self: Sized;

    /// Open read-only (if supported)
    fn open_read_only(config: BackendConfig) -> Result<Self, StorageError>
    where
        Self: Sized;

    /// Get a consistent read snapshot
    fn snapshot(&self) -> Result<Box<dyn StorageReader>, StorageError>;

    /// Start a read-write transaction
    fn transaction(&self) -> Result<Box<dyn StorageReadableTransaction>, StorageError>;

    /// Get bulk loader
    fn bulk_loader(&self) -> Box<dyn StorageBulkLoader>;

    /// Flush all pending writes to durable storage
    fn flush(&self) -> Result<(), StorageError>;

    /// Close the backend
    fn close(&self) -> Result<(), StorageError>;

    /// Backup to directory
    fn backup(&self, target: &std::path::Path) -> Result<(), StorageError>;

    /// Get backend-specific statistics
    fn stats(&self) -> BackendStats;

    /// Validate storage integrity
    fn validate(&self) -> Result<(), StorageError>;
}

/// Backend statistics
#[derive(Clone, Debug, Default)]
pub struct BackendStats {
    pub quad_count: u64,
    pub named_graph_count: u64,
    pub term_count: u64,
    pub disk_usage_bytes: u64,
    pub memory_usage_bytes: u64,
    pub backend_specific: serde_json::Value,
}
```

---

## 2. Quad Access Interface: `StorageReader`

```rust
// lib/oxigraph/src/storage/backend.rs (continued)

use crate::storage::numeric_encoder::EncodedTerm;

/// Quad pattern for scanning
#[derive(Clone, Copy, Debug)]
pub struct QuadPattern<'a> {
    pub subject: Option<&'a EncodedTerm>,
    pub predicate: Option<&'a EncodedTerm>,
    pub object: Option<&'a EncodedTerm>,
    pub graph: Option<&'a EncodedTerm>,
}

/// Iterator over encoded quads
pub type EncodedQuadIterator = Box<dyn Iterator<Item = Result<EncodedQuad, StorageError>> + Send>;

/// Iterator over encoded graph names
pub type EncodedGraphIterator = Box<dyn Iterator<Item = Result<EncodedTerm, StorageError>> + Send>;

/// Read-only access to stored quads
pub trait StorageReader: Send + Sync {
    /// Scan quads matching a pattern
    fn quads_for_pattern(&self, pattern: QuadPattern<'_>) -> EncodedQuadIterator;

    /// Check if a specific quad exists
    fn contains(&self, quad: &EncodedQuad) -> Result<bool, StorageError>;

    /// Iterate all named graphs
    fn named_graphs(&self) -> EncodedGraphIterator;

    /// Check if a named graph exists
    fn contains_named_graph(&self, graph: &EncodedTerm) -> Result<bool, StorageError>;

    /// Lookup term string by hash
    fn get_str(&self, hash: &crate::storage::numeric_encoder::StrHash)
        -> Result<Option<crate::model::OxString>, StorageError>;

    /// Get total quad count (approximate OK)
    fn quad_count(&self) -> Result<u64, StorageError>;

    /// Get named graph count
    fn named_graph_count(&self) -> Result<u64, StorageError>;
}
```

---

## 3. Write Interface: `StorageReadableTransaction`

```rust
// lib/oxigraph/src/storage/backend.rs (continued)

/// Write operations buffered until commit
pub trait StorageReadableTransaction: StorageReader {
    /// Insert a quad
    fn insert(&mut self, quad: &Quad);

    /// Insert a named graph
    fn insert_named_graph(&mut self, graph: NamedOrBlankNode);

    /// Remove a quad
    fn remove(&mut self, quad: &Quad);

    /// Clear default graph
    fn clear_default_graph(&mut self);

    /// Clear all named graphs
    fn clear_all_named_graphs(&mut self);

    /// Clear everything
    fn clear(&mut self);

    /// Commit the transaction
    fn commit(self: Box<Self>) -> Result<(), StorageError>;

    /// Rollback (implicit on drop)
    fn rollback(self: Box<Self>);
}
```

---

## 4. Bulk Load Interface

```rust
// lib/oxigraph/src/storage/backend.rs (continued)

/// Bulk loading interface
pub trait StorageBulkLoader: Send + Sync {
    /// Load quads atomically (all-or-nothing)
    fn load_atomic(&mut self, quads: Vec<Quad>) -> Result<(), StorageError>;

    /// Load quads in streaming fashion (non-atomic, chunked)
    fn load_streaming(&mut self, quads: Vec<Quad>) -> Result<(), StorageError>;

    /// Finish and commit any pending data
    fn finish(&mut self) -> Result<(), StorageError>;
}
```

---

## 5. Term Dictionary Interface

```rust
// lib/oxigraph/src/storage/backend.rs (continued)

use crate::storage::numeric_encoder::{EncodedTerm, StrHash};

/// Term dictionary operations (shared by all backends)
pub trait TermDictionary: Send + Sync {
    /// Encode a term, returning its encoded form and whether it was new
    fn encode(&self, term: &Term) -> Result<(EncodedTerm, bool), StorageError>;

    /// Decode an encoded term
    fn decode(&self, encoded: &EncodedTerm) -> Result<Term, StorageError>;

    /// Get term by hash
    fn get_by_hash(&self, hash: StrHash) -> Result<Option<Term>, StorageError>;

    /// Get hash by term (for reverse lookup)
    fn get_hash(&self, term: &Term) -> Result<Option<StrHash>, StorageError>;

    /// Batch encode terms
    fn encode_batch(&self, terms: &[Term]) -> Result<Vec<EncodedTerm>, StorageError>;

    /// Persist dictionary changes (if backend separates dictionary from quads)
    fn flush(&self) -> Result<(), StorageError>;
}
```

---

## 6. Backend Registry

```rust
// lib/oxigraph/src/storage/backend.rs (continued)

use std::collections::HashMap;

/// Registry of available backends
pub struct BackendRegistry {
    backends: HashMap<&'static str, Box<dyn BackendFactory>>,
}

impl BackendRegistry {
    pub fn new() -> Self {
        Self { backends: HashMap::new() }
    }

    /// Register a backend factory
    pub fn register<B: StorageBackend + 'static>(&mut self) {
        self.backends.insert(B::NAME, Box::new(BackendFactoryImpl::<B>));
    }

    /// Get backend by name
    pub fn get(&self, name: &str) -> Option<&dyn BackendFactory> {
        self.backends.get(name).map(|b| b.as_ref())
    }

    /// List available backends
    pub fn available(&self) -> Vec<&'static str> {
        self.backends.keys().copied().collect()
    }
}

/// Factory for creating backend instances
pub trait BackendFactory: Send + Sync {
    fn create(&self, config: BackendConfig) -> Result<Box<dyn StorageBackend>, StorageError>;
    fn create_read_only(&self, config: BackendConfig) -> Result<Box<dyn StorageBackend>, StorageError>;
    fn capabilities(&self) -> BackendCapabilities;
    fn default_config(&self) -> BackendConfig;
}

struct BackendFactoryImpl<B: StorageBackend> {
    _phantom: std::marker::PhantomData<B>,
}

impl<B: StorageBackend + 'static> BackendFactory for BackendFactoryImpl<B> {
    fn create(&self, config: BackendConfig) -> Result<Box<dyn StorageBackend>, StorageError> {
        B::open(config).map(|b| Box::new(b) as Box<dyn StorageBackend>)
    }

    fn create_read_only(&self, config: BackendConfig) -> Result<Box<dyn StorageBackend>, StorageError> {
        B::open_read_only(config).map(|b| Box::new(b) as Box<dyn StorageBackend>)
    }

    fn capabilities(&self) -> BackendCapabilities {
        // Create a temporary instance to get capabilities
        // In practice, capabilities should be static
        B::open(BackendConfig::Memory).map(|b| b.capabilities()).unwrap_or_default()
    }

    fn default_config(&self) -> BackendConfig {
        BackendConfig::Memory
    }
}
```

---

## 7. Store Integration

```rust
// lib/oxigraph/src/store.rs (modifications)

use crate::storage::backend::{BackendConfig, BackendRegistry, StorageBackend};

impl Store {
    /// Open a store with a specific backend
    pub fn open_with_backend(config: BackendConfig) -> Result<Self, StorageError> {
        let registry = BackendRegistry::global(); // initialized at startup
        let backend_name = config.backend_name();
        let factory = registry.get(backend_name)
            .ok_or_else(|| StorageError::Other(format!("Unknown backend: {}", backend_name).into()))?;
        let backend = factory.create(config)?;
        Ok(Self { storage: Storage::new(backend)? })
    }

    /// Open read-only
    pub fn open_read_only_with_backend(config: BackendConfig) -> Result<Self, StorageError> {
        let registry = BackendRegistry::global();
        let backend_name = config.backend_name();
        let factory = registry.get(backend_name)
            .ok_or_else(|| StorageError::Other(format!("Unknown backend: {}", backend_name).into()))?;
        let backend = factory.create_read_only(config)?;
        Ok(Self { storage: Storage::new(backend)? })
    }
}
```

---

## 8. SlateDB Backend Implementation (Existing)

```rust
// lib/oxigraph/src/storage/slatedb_backend.rs

use crate::storage::{
    backend::{BackendCapabilities, BackendConfig, BackendStats, StorageBackend, StorageReader, StorageReadableTransaction, StorageBulkLoader, QuadPattern, EncodedQuadIterator, EncodedGraphIterator},
    slatedb::{SlateDbStorage, SlateDbOptions, SlateDbStorageReader, SlateDbStorageTransaction, SlateDbStorageBulkLoader},
    numeric_encoder::{EncodedTerm, EncodedQuad, StrHash},
    StorageError,
};
use crate::model::{Quad, NamedOrBlankNode, OxString};

pub struct SlateDbBackend {
    storage: SlateDbStorage,
    capabilities: BackendCapabilities,
}

impl StorageBackend for SlateDbBackend {
    const NAME: &'static str = "slatedb";

    fn capabilities(&self) -> BackendCapabilities {
        self.capabilities.clone()
    }

    fn open(config: BackendConfig) -> Result<Self, StorageError> {
        let BackendConfig::SlateDb(options) = config else {
            return Err(StorageError::Other("Expected SlateDb config".into()));
        };
        let storage = SlateDbStorage::open_with_options(options)?;
        let capabilities = BackendCapabilities {
            durable: true,
            snapshot_isolation: true,
            read_only_mode: true,
            atomic_bulk_load: true,
            streaming_bulk_load: true,
            backup: true,
            named_graphs: true,
            graph_traversal: false, // Not native graph engine
            graphblas: false,
            mvcc: false, // SlateDB has snapshot isolation but not graph MVCC
            object_store: true,
            compression: true,
        };
        Ok(Self { storage, capabilities })
    }

    fn open_read_only(config: BackendConfig) -> Result<Self, StorageError> {
        let mut options = match config {
            BackendConfig::SlateDb(o) => o,
            _ => return Err(StorageError::Other("Expected SlateDb config".into())),
        };
        options.read_only = true;
        Self::open(BackendConfig::SlateDb(options))
    }

    fn snapshot(&self) -> Result<Box<dyn StorageReader>, StorageError> {
        Ok(Box::new(SlateDbReaderAdapter(self.storage.snapshot())))
    }

    fn transaction(&self) -> Result<Box<dyn StorageReadableTransaction>, StorageError> {
        Ok(Box::new(SlateDbTransactionAdapter(self.storage.start_readable_transaction()?)))
    }

    fn bulk_loader(&self) -> Box<dyn StorageBulkLoader> {
        Box::new(SlateDbBulkLoaderAdapter(self.storage.bulk_loader()))
    }

    fn flush(&self) -> Result<(), StorageError> {
        self.storage.flush()
    }

    fn close(&self) -> Result<(), StorageError> {
        self.storage.close()
    }

    fn backup(&self, target: &std::path::Path) -> Result<(), StorageError> {
        self.storage.backup(target)
    }

    fn stats(&self) -> BackendStats {
        BackendStats::default()
    }

    fn validate(&self) -> Result<(), StorageError> {
        self.storage.snapshot()?.validate()
    }
}

// Adapter types wrapping existing SlateDB implementations
struct SlateDbReaderAdapter(SlateDbStorageReader);
impl StorageReader for SlateDbReaderAdapter { /* ... */ }

struct SlateDbTransactionAdapter(SlateDbStorageReadableTransaction);
impl StorageReader for SlateDbTransactionAdapter { /* ... */ }
impl StorageReadableTransaction for SlateDbTransactionAdapter { /* ... */ }

struct SlateDbBulkLoaderAdapter(SlateDbStorageBulkLoader);
impl StorageBulkLoader for SlateDbBulkLoaderAdapter { /* ... */ }
```

---

## 9. FalkorDB Backend Implementation (Native Persistence)

```rust
// lib/oxigraph/src/storage/falkordb_backend.rs

use crate::storage::{
    backend::{BackendCapabilities, BackendConfig, BackendStats, StorageBackend, StorageReader, StorageReadableTransaction, StorageBulkLoader, QuadPattern, EncodedQuadIterator, EncodedGraphIterator, FalkorDbConfig, FalkorGraphOptions, FalkorDurability, WalSyncMode, CheckpointInterval},
    numeric_encoder::{EncodedTerm, EncodedQuad, StrHash},
    StorageError,
};
use crate::model::{Quad, NamedOrBlankNode, OxString, Term, GraphName};
use std::sync::Arc;
use std::collections::HashMap;

/// FalkorDB graph engine integration (vendored/pinned)
mod falkor_graph {
    // use graph::graph::{graph::Graph, mvcc_graph::MvccGraph};
}

/// FalkorDB native persistence layer
mod falkor_persistence {
    // WAL, checkpoints, recovery
}

pub struct FalkorDbBackend {
    graph: Arc<falkor_graph::MvccGraph>,
    persistence: Arc<falkor_persistence::PersistenceLayer>,
    term_dict: FalkorTermDictionary,
    predicate_registry: PredicateRegistry,
    config: FalkorDbConfig,
    capabilities: BackendCapabilities,
}

impl StorageBackend for FalkorDbBackend {
    const NAME: &'static str = "falkordb";

    fn capabilities(&self) -> BackendCapabilities {
        self.capabilities.clone()
    }

    fn open(config: BackendConfig) -> Result<Self, StorageError> {
        let BackendConfig::FalkorDb(config) = config else {
            return Err(StorageError::Other("Expected FalkorDb config".into()));
        };

        // Ensure data directory exists
        std::fs::create_dir_all(&config.data_dir)?;

        // Initialize persistence layer (WAL + checkpoints)
        let persistence = Arc::new(falkor_persistence::PersistenceLayer::new(
            config.data_dir.clone(),
            config.durability,
            config.wal_sync,
            config.max_wal_size,
            config.checkpoint_interval,
        )?);

        // Load or create FalkorDB graph from checkpoint
        let graph = load_or_create_graph(&persistence, &config)?;

        // Wrap in MVCC
        let mvcc_graph = Arc::new(falkor_graph::MvccGraph::new(graph));

        // Build term dictionary and predicate registry from persistence
        let term_dict = FalkorTermDictionary::load(&persistence)?;
        let predicate_registry = PredicateRegistry::load(&persistence)?;

        let capabilities = BackendCapabilities {
            durable: config.durability != FalkorDurability::None,
            snapshot_isolation: true,
            read_only_mode: false, // FalkorDB MVCC handles this differently
            atomic_bulk_load: true,
            streaming_bulk_load: true,
            backup: true,
            named_graphs: true,
            graph_traversal: true,
            graphblas: config.graph_options.enable_graphblas,
            mvcc: true,
            object_store: false, // Local filesystem only
            compression: true, // In checkpoints
        };

        Ok(Self {
            graph: mvcc_graph,
            persistence,
            term_dict,
            predicate_registry,
            config,
            capabilities,
        })
    }

    fn open_read_only(config: BackendConfig) -> Result<Self, StorageError> {
        let mut config = match config {
            BackendConfig::FalkorDb(c) => c,
            _ => return Err(StorageError::Other("Expected FalkorDb config".into())),
        };
        config.durability = FalkorDurability::None;
        Self::open(BackendConfig::FalkorDb(config))
    }

    fn snapshot(&self) -> Result<Box<dyn StorageReader>, StorageError> {
        let graph_snapshot = self.graph.read(); // MVCC snapshot
        Ok(Box::new(FalkorReaderAdapter {
            graph: graph_snapshot,
            term_dict: self.term_dict.clone(),
            predicate_registry: self.predicate_registry.clone(),
        }))
    }

    fn transaction(&self) -> Result<Box<dyn StorageReadableTransaction>, StorageError> {
        Ok(Box::new(FalkorTransactionAdapter::new(
            Arc::clone(&self.graph),
            Arc::clone(&self.persistence),
            self.term_dict.clone(),
            self.predicate_registry.clone(),
            self.config.clone(),
        )))
    }

    fn bulk_loader(&self) -> Box<dyn StorageBulkLoader> {
        Box::new(FalkorBulkLoaderAdapter::new(
            Arc::clone(&self.graph),
            Arc::clone(&self.persistence),
            self.term_dict.clone(),
            self.predicate_registry.clone(),
            self.config.clone(),
        ))
    }

    fn flush(&self) -> Result<(), StorageError> {
        // Force checkpoint
        self.persistence.checkpoint(&self.graph.read())?;
        self.persistence.wal_flush()?;
        Ok(())
    }

    fn close(&self) -> Result<(), StorageError> {
        // Final checkpoint before closing
        self.persistence.checkpoint(&self.graph.read())?;
        self.persistence.wal_flush()?;
        self.persistence.close()?;
        Ok(())
    }

    fn backup(&self, target: &std::path::Path) -> Result<(), StorageError> {
        // Copy WAL + latest checkpoint to target directory
        self.persistence.backup(target)?;
        Ok(())
    }

    fn stats(&self) -> BackendStats {
        let graph_snap = self.graph.read();
        BackendStats {
            quad_count: graph_snap.quad_count(),
            named_graph_count: graph_snap.named_graph_count(),
            term_count: self.term_dict.len(),
            disk_usage_bytes: self.persistence.disk_usage(),
            memory_usage_bytes: graph_snap.memory_usage(),
            backend_specific: serde_json::json!({}),
        }
    }

    fn validate(&self) -> Result<(), StorageError> {
        // Validate graph consistency
        self.graph.read().validate()?;
        // Validate persistence integrity
        self.persistence.validate()?;
        Ok(())
    }
}

/// Term dictionary backed by native persistence + in-memory cache
struct FalkorTermDictionary {
    persistence: Arc<falkor_persistence::PersistenceLayer>,
    cache: dashmap::DashMap<StrHash, OxString>,
    // RDF term ID ↔ FalkorDB node ID mapping
    term_to_node: dashmap::DashMap<u128, u64>,
    node_to_term: dashmap::DashMap<u64, u128>,
}

impl FalkorTermDictionary {
    fn load(persistence: &Arc<falkor_persistence::PersistenceLayer>) -> Result<Self, StorageError> {
        // Load term mappings from latest checkpoint
        let mappings = persistence.load_term_mappings()?;
        let mut term_to_node = dashmap::DashMap::new();
        let mut node_to_term = dashmap::DashMap::new();
        for (term_id, node_id) in mappings {
            term_to_node.insert(term_id, node_id);
            node_to_term.insert(node_id, term_id);
        }
        Ok(Self {
            persistence: Arc::clone(persistence),
            cache: dashmap::DashMap::new(),
            term_to_node,
            node_to_term,
        })
    }

    fn get_or_create_node_id(&self, term_id: u128, persistence: &falkor_persistence::PersistenceLayer) -> u64 {
        *self.term_to_node.entry(term_id).or_insert_with(|| {
            let node_id = persistence.allocate_node_id();
            node_id
        })
    }
}

/// Predicate registry: RDF predicate ID → FalkorDB relationship type ID
struct PredicateRegistry {
    persistence: Arc<falkor_persistence::PersistenceLayer>,
    cache: dashmap::DashMap<u64, u64>, // predicate_id -> type_id
    reverse: dashmap::DashMap<u64, u64>, // type_id -> predicate_id
}

impl PredicateRegistry {
    fn load(persistence: &Arc<falkor_persistence::PersistenceLayer>) -> Result<Self, StorageError> {
        let mappings = persistence.load_predicate_registry()?;
        let mut cache = dashmap::DashMap::new();
        let mut reverse = dashmap::DashMap::new();
        for (predicate_id, type_id) in mappings {
            cache.insert(predicate_id, type_id);
            reverse.insert(type_id, predicate_id);
        }
        Ok(Self { persistence: Arc::clone(persistence), cache, reverse })
    }

    fn get_or_create_type_id(&self, predicate_id: u64, persistence: &falkor_persistence::PersistenceLayer) -> u64 {
        *self.cache.entry(predicate_id).or_insert_with(|| {
            let type_id = persistence.allocate_relationship_type_id();
            // Log to WAL for durability
            persistence.log_predicate_type(predicate_id, type_id);
            type_id
        })
    }
}

/// Reader adapter using FalkorDB graph snapshot
struct FalkorReaderAdapter {
    graph: falkor_graph::GraphSnapshot,
    term_dict: FalkorTermDictionary,
    predicate_registry: PredicateRegistry,
}
impl StorageReader for FalkorReaderAdapter {
    fn quads_for_pattern(&self, pattern: QuadPattern<'_>) -> EncodedQuadIterator {
        // Translate SPARQL pattern to FalkorDB graph operations
        Box::new(FalkorQuadIterator::new(&self.graph, pattern, &self.predicate_registry))
    }
    // ... other methods
}

/// Transaction adapter using FalkorDB MVCC fork + native persistence commit coordination
struct FalkorTransactionAdapter {
    graph_fork: falkor_graph::GraphFork,
    persistence_tx: falkor_persistence::PersistenceTransaction,
    term_dict: FalkorTermDictionary,
    predicate_registry: PredicateRegistry,
    config: FalkorDbConfig,
}
impl StorageReader for FalkorTransactionAdapter { /* ... */ }
impl StorageReadableTransaction for FalkorTransactionAdapter {
    fn insert(&mut self, quad: &Quad) {
        // 1. Encode terms, get/create node IDs via term_dict
        // 2. Get/create predicate type ID via predicate_registry
        // 3. Create relationship in graph fork
        // 4. Record operation in persistence transaction for WAL
    }

    fn commit(self: Box<Self>) -> Result<(), StorageError> {
        // Critical atomic commit sequence:
        // 1. Prepare persistence transaction (dictionary updates, graph ops log)
        // 2. Write to WAL (DURABLE - this is the linearization point)
        // 3. Publish graph fork (MVCC atomic pointer swap)
        // 4. Release previous snapshot
        // 5. Async checkpoint if interval reached
    }
}

/// Bulk loader using FalkorDB's create_relationships_bulk
struct FalkorBulkLoaderAdapter { /* ... */ }
impl StorageBulkLoader for FalkorBulkLoaderAdapter { /* ... */ }
```

---

## 10. FalkorDB Native Persistence Layer

```rust
// lib/oxigraph/src/storage/falkordb_persistence.rs

use crate::storage::StorageError;
use std::sync::{Arc, Mutex};
use std::fs::{File, OpenOptions};
use std::io::{Write, BufWriter, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// WAL entry for a single transaction
#[derive(serde::Serialize, serde::Deserialize)]
pub struct WalEntry {
    pub transaction_id: u64,
    pub timestamp: u64,
    pub operations: Vec<GraphOperation>,
    pub term_mappings: Vec<(u128, u64)>,
    pub predicate_types: Vec<(u64, u64)>,
    pub checksum: u64,
}

/// Graph operations for transaction log
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum GraphOperation {
    CreateNodes(Vec<u64>),
    CreateEdges {
        predicate_type_id: u64,
        subjects: Vec<u64>,
        objects: Vec<u64>,
        edge_ids: Vec<u64>,
        graph_ids: Vec<u64>,
    },
    DeleteEdges(Vec<u64>),
    UpdateAttributes(Vec<AttributeUpdate>),
    CreatePredicateType { predicate_id: u64, type_id: u64 },
    CreateTermMapping { term_id: u128, node_id: u64 },
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct AttributeUpdate { /* ... */ }

/// Checkpoint format (versioned, independent of Redis RDB)
#[derive(serde::Serialize, serde::Deserialize)]
pub struct GraphCheckpoint {
    pub version: u64,
    pub transaction_version: u64,
    pub node_count: u64,
    pub edge_count: u64,
    pub adjacency_matrix: SerializedMatrix,
    pub label_matrices: Vec<(u64, SerializedMatrix)>,
    pub relationship_tensors: Vec<(u64, SerializedTensor)>,
    pub attribute_stores: SerializedAttributes,
    pub predicate_registry: Vec<(u64, u64)>,
    pub term_mappings: Vec<(u128, u64)>,
    pub checksum: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct SerializedMatrix { /* GraphBLAS matrix serialization */ }
#[derive(serde::Serialize, serde::Deserialize)]
pub struct SerializedTensor { /* GraphBLAS tensor serialization */ }
#[derive(serde::Serialize, serde::Deserialize)]
pub struct SerializedAttributes { /* Attribute store serialization */ }

/// Native persistence layer for FalkorDB graph state
pub struct PersistenceLayer {
    data_dir: PathBuf,
    durability: FalkorDurability,
    wal_sync: WalSyncMode,
    max_wal_size: u64,
    checkpoint_interval: CheckpointInterval,
    
    // WAL state
    wal_file: Mutex<BufWriter<File>>,
    wal_transaction_id: AtomicU64,
    wal_size: AtomicU64,
    
    // Checkpoint state
    last_checkpoint_tx: AtomicU64,
    last_checkpoint_time: AtomicU64,
    
    // ID allocators (persisted in checkpoints)
    next_node_id: AtomicU64,
    next_type_id: AtomicU64,
}

impl PersistenceLayer {
    pub fn new(
        data_dir: PathBuf,
        durability: FalkorDurability,
        wal_sync: WalSyncMode,
        max_wal_size: u64,
        checkpoint_interval: CheckpointInterval,
    ) -> Result<Self, StorageError> {
        std::fs::create_dir_all(&data_dir)?;
        
        let wal_path = data_dir.join("wal.log");
        let wal_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)?;
        
        // Load latest checkpoint to restore state
        let (next_node_id, next_type_id, last_tx) = Self::load_latest_checkpoint(&data_dir)?;
        
        Ok(Self {
            data_dir,
            durability,
            wal_sync,
            max_wal_size,
            checkpoint_interval,
            wal_file: Mutex::new(BufWriter::new(wal_file)),
            wal_transaction_id: AtomicU64::new(last_tx),
            wal_size: AtomicU64::new(0),
            last_checkpoint_tx: AtomicU64::new(last_tx),
            last_checkpoint_time: AtomicU64::new(current_timestamp()),
            next_node_id: AtomicU64::new(next_node_id),
            next_type_id: AtomicU64::new(next_type_id),
        })
    }

    /// Write transaction to WAL (durable linearization point)
    pub fn write_wal(&self, operations: &[GraphOperation], term_mappings: &[(u128, u64)], predicate_types: &[(u64, u64)]) -> Result<u64, StorageError> {
        let tx_id = self.wal_transaction_id.fetch_add(1, Ordering::SeqCst) + 1;
        
        let entry = WalEntry {
            transaction_id: tx_id,
            timestamp: current_timestamp(),
            operations: operations.to_vec(),
            term_mappings: term_mappings.to_vec(),
            predicate_types: predicate_types.to_vec(),
            checksum: 0, // computed below
        };
        
        let serialized = bincode::serialize(&entry)?;
        let checksum = crc32fast::hash(&serialized);
        
        let mut entry_with_checksum = entry;
        entry_with_checksum.checksum = checksum;
        let final_bytes = bincode::serialize(&entry_with_checksum)?;
        
        let mut wal = self.wal_file.lock().unwrap();
        wal.write_all(&(final_bytes.len() as u32).to_le_bytes())?;
        wal.write_all(&final_bytes)?;
        
        // Sync based on durability mode
        match self.wal_sync {
            WalSyncMode::Always => wal.flush()?,
            WalSyncMode::Periodic => { /* background flusher handles */ }
            WalSyncMode::None => {}
        }
        
        self.wal_size.fetch_add(final_bytes.len() as u64, Ordering::Relaxed);
        
        Ok(tx_id)
    }

    /// Commit a transaction: write WAL + publish graph version
    pub fn commit_transaction(
        &self,
        graph: &Arc<MvccGraph>,
        fork: GraphFork,
        operations: Vec<GraphOperation>,
        term_mappings: Vec<(u128, u64)>,
        predicate_types: Vec<(u64, u64)>,
    ) -> Result<(), StorageError> {
        // 1. Write to WAL (DURABLE - linearization point)
        let tx_id = self.write_wal(&operations, &term_mappings, &predicate_types)?;
        
        // 2. Update persisted state
        for (term_id, node_id) in &term_mappings {
            // term mappings persisted in checkpoint
        }
        for (pred_id, type_id) in &predicate_types {
            // predicate registry persisted in checkpoint
        }
        
        // 3. Publish graph fork (atomic MVCC swap)
        graph.commit(fork)?;
        
        // 4. Check if checkpoint needed
        self.maybe_checkpoint(graph)?;
        
        Ok(())
    }

    /// Periodic checkpoint of graph state
    pub fn checkpoint(&self, graph_snapshot: &GraphSnapshot) -> Result<(), StorageError> {
        let checkpoint = GraphCheckpoint {
            version: CURRENT_CHECKPOINT_VERSION,
            transaction_version: self.wal_transaction_id.load(Ordering::SeqCst),
            node_count: graph_snapshot.node_count(),
            edge_count: graph_snapshot.edge_count(),
            adjacency_matrix: serialize_matrix(graph_snapshot.adjacency()),
            label_matrices: graph_snapshot.labels().map(|(id, m)| (id, serialize_matrix(m))).collect(),
            relationship_tensors: graph_snapshot.relationship_tensors().map(|(id, t)| (id, serialize_tensor(t))).collect(),
            attribute_stores: serialize_attributes(graph_snapshot.attributes()),
            predicate_registry: self.export_predicate_registry(),
            term_mappings: self.export_term_mappings(),
            checksum: 0,
        };
        
        let serialized = bincode::serialize(&checkpoint)?;
        let checksum = crc32fast::hash(&serialized);
        
        let checkpoint_path = self.data_dir.join(format!("checkpoint_{}.bin", checkpoint.transaction_version));
        let mut file = File::create(&checkpoint_path)?;
        file.write_all(&serialized)?;
        file.flush()?;
        
        // Update checkpoint metadata
        self.last_checkpoint_tx.store(checkpoint.transaction_version, Ordering::SeqCst);
        self.last_checkpoint_time.store(current_timestamp(), Ordering::SeqCst);
        
        // Truncate WAL up to checkpoint transaction version
        self.truncate_wal(checkpoint.transaction_version)?;
        
        Ok(())
    }

    /// Recovery on startup
    pub fn recover(&self, graph: &mut Graph) -> Result<(), StorageError> {
        // 1. Load latest checkpoint
        let checkpoint = self.load_latest_checkpoint()?;
        graph.restore_from_checkpoint(&checkpoint)?;
        
        // 2. Replay WAL from checkpoint transaction version forward
        let mut wal_file = File::open(self.data_dir.join("wal.log"))?;
        let mut buffer = Vec::new();
        wal_file.read_to_end(&mut buffer)?;
        
        let mut offset = 0;
        while offset < buffer.len() {
            let len = u32::from_le_bytes(buffer[offset..offset+4].try_into().unwrap()) as usize;
            offset += 4;
            let entry_bytes = &buffer[offset..offset+len];
            offset += len;
            
            let entry: WalEntry = bincode::deserialize(entry_bytes)?;
            
            // Verify checksum
            let mut verify_entry = entry.clone();
            verify_entry.checksum = 0;
            let verify_bytes = bincode::serialize(&verify_entry)?;
            if crc32fast::hash(&verify_bytes) != entry.checksum {
                return Err(StorageError::Corruption("WAL checksum mismatch".into()));
            }
            
            if entry.transaction_version <= checkpoint.transaction_version {
                continue; // Already in checkpoint
            }
            
            // Replay operations
            for op in entry.operations {
                graph.apply_operation(&op)?;
            }
            
            // Restore term mappings and predicate types
            for (term_id, node_id) in entry.term_mappings {
                graph.set_term_mapping(term_id, node_id);
            }
            for (pred_id, type_id) in entry.predicate_types {
                graph.set_predicate_type(pred_id, type_id);
            }
        }
        
        Ok(())
    }

    // ... helper methods: load_latest_checkpoint, truncate_wal, backup, validate, etc.
}

/// Transaction context for persistence
pub struct PersistenceTransaction {
    operations: Vec<GraphOperation>,
    term_mappings: Vec<(u128, u64)>,
    predicate_types: Vec<(u64, u64)>,
}

impl PersistenceTransaction {
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
            term_mappings: Vec::new(),
            predicate_types: Vec::new(),
        }
    }

    pub fn add_operation(&mut self, op: GraphOperation) {
        self.operations.push(op);
    }

    pub fn add_term_mapping(&mut self, term_id: u128, node_id: u64) {
        self.term_mappings.push((term_id, node_id));
    }

    pub fn add_predicate_type(&mut self, predicate_id: u64, type_id: u64) {
        self.predicate_types.push((predicate_id, type_id));
    }
}
```

---

## 11. Pattern-to-Graph Translation (Key Innovation)

```rust
// lib/oxigraph/src/storage/falkordb_patterns.rs

use crate::storage::backend::QuadPattern;
use crate::storage::numeric_encoder::EncodedTerm;
use crate::storage::falkordb_backend::{PredicateRegistry, FalkorGraphSnapshot};

/// Translate SPARQL quad pattern to FalkorDB graph operation
pub fn execute_pattern(
    graph: &FalkorGraphSnapshot,
    pattern: QuadPattern<'_>,
    predicates: &PredicateRegistry,
) -> Box<dyn Iterator<Item = Result<EncodedQuad, StorageError>>> {
    match (pattern.subject, pattern.predicate, pattern.object, pattern.graph) {
        // S, P, O, G - exact quad check
        (Some(s), Some(p), Some(o), Some(g)) => Box::new(
            graph.has_edge(s.node_id(), p.type_id(predicates), o.node_id(), g.graph_id())
                .map(move |exists| Ok(EncodedQuad::new(*s, *p, *o, *g)))
                .filter(|r| r.as_ref().map(|e| e.exists).unwrap_or(false))
        ),

        // S, P, ?O, G - outgoing typed adjacency
        (Some(s), Some(p), None, Some(g)) => Box::new(
            graph.outgoing_edges(s.node_id(), p.type_id(predicates), g.graph_id())
                .map(move |o| Ok(EncodedQuad::new(*s, *p, o.into(), *g)))
        ),

        // ?S, P, O, G - incoming typed adjacency
        (None, Some(p), Some(o), Some(g)) => Box::new(
            graph.incoming_edges(o.node_id(), p.type_id(predicates), g.graph_id())
                .map(move |s| Ok(EncodedQuad::new(s.into(), *p, *o, *g)))
        ),

        // S, ?P, ?O, G - all outgoing from subject in graph
        (Some(s), None, None, Some(g)) => Box::new(
            graph.all_outgoing(s.node_id(), g.graph_id())
                .map(move |(p, o)| Ok(EncodedQuad::new(*s, p.into(), o.into(), *g)))
        ),

        // ?S, ?P, ?O, G - full graph scan (with graph filter)
        (None, None, None, Some(g)) => Box::new(
            graph.scan_graph(g.graph_id())
        ),

        // ... other 12 patterns
    }
}
```

---

## 12. Configuration & Feature Flags

```toml
# lib/oxigraph/Cargo.toml

[features]
default = ["rocksdb"]
rocksdb = []
slatedb = ["slatedb-dep"]
falkordb = ["falkordb-graph"]  # FalkorDB standalone, no SlateDB dependency

slatedb-dep = ["slatedb"]
falkordb-graph = []  # Vendored/pinned FalkorDB graph crate

# For testing without heavy deps
memory = []
```

---

## 13. Migration Path

| From | To | Tool |
|------|-----|------|
| RocksDB | SlateDB | `oxigraph migrate --from rocksdb --to slatedb` |
| RocksDB | FalkorDB | `oxigraph migrate --from rocksdb --to falkordb` |
| SlateDB | FalkorDB | `oxigraph migrate --from slatedb --to falkordb` |
| FalkorDB | SlateDB | `oxigraph migrate --from falkordb --to slatedb` |
| FalkorDB | RocksDB | `oxigraph migrate --from falkordb --to rocksdb` |

Migration copies quads verbatim; predicate registry and term dictionary are rebuilt.

---

## 14. Testing Strategy

```rust
// tests/storage_backend_conformance.rs

use oxigraph::store::Store;
use oxigraph::storage::backend::{BackendConfig, BackendRegistry};

fn test_backend(backend_name: &str, config: BackendConfig) {
    let store = Store::open_with_backend(config).unwrap();

    // Standard RDF/SPARQL conformance tests
    test_basic_insert(&store);
    test_quad_patterns(&store);
    test_named_graphs(&store);
    test_sparql_queries(&store);
    test_transactions(&store);
    test_bulk_load(&store);
    test_snapshot_isolation(&store);
    test_backup_restore(&store);
    test_concurrent_readers_writer(&store);
    test_crash_recovery(&store); // New: verify durability
}

#[test]
fn rocksdb_conformance() {
    test_backend("rocksdb", BackendConfig::RocksDb(Default::default()));
}

#[test]
fn slatedb_conformance() {
    test_backend("slatedb", BackendConfig::SlateDb(Default::default()));
}

#[test]
fn falkordb_conformance() {
    test_backend("falkordb", BackendConfig::FalkorDb(Default::default()));
}
```

---

## 15. Implementation Priority

| Phase | Component | Effort | Dependencies |
|-------|-----------|--------|--------------|
| 1 | `StorageBackend` trait + registry | 1 week | None |
| 2 | Refactor SlateDB to implement trait | 1 week | Phase 1 |
| 3 | Refactor RocksDB to implement trait | 1 week | Phase 1 |
| 4 | FalkorDB graph crate integration (PoC) | 2 weeks | FalkorDB 6.x source |
| 5 | FalkorDB backend + native persistence | 3 weeks | Phase 2, 4 |
| 6 | Pattern-to-graph translation optimizer | 2 weeks | Phase 5 |
| 7 | Checkpoint/recovery + benchmarks | 2 weeks | Phase 5 |
| 8 | Migration tools + conformance tests | 1 week | Phase 2, 3, 5 |

---

## 16. Key Design Decisions Summary

| Decision | Rationale |
|----------|-----------|
| **Trait-based backend abstraction** | Clean separation, testable, swappable |
| **FalkorDB native persistence (WAL + checkpoints)** | No external dependency; unified storage model; leverages FalkorDB's own serialization |
| **Direct graph crate integration (not Cypher)** | Eliminates Redis/Cypher overhead; native GraphBLAS/MVCC |
| **MVCC fork + WAL commit atomicity** | WAL write = linearization point; graph fork published after |
| **Predicate → typed relationship** | Leverages FalkorDB's per-type sparse tensors |
| **Term dictionary in native persistence** | Rebuildable from checkpoint; no separate KV store needed |
| **Pattern translation in backend** | SPARQL planner stays in Oxigraph; backend provides fast primitives |
| **Feature-gated backends** | Zero-cost for users who don't need them |

---

## 17. Open Questions

1. **Licensing**: FalkorDB graph crate is SSPL-1.0. Legal review required before shipping.
2. **Graph crate stability**: Internal crate, not public API. May need vendored fork.
3. **Memory pressure**: FalkorDB is in-memory graph engine. Large datasets need RAM. Checkpoints provide disk spill but graph matrices stay resident during operation.
4. **Predicate explosion**: 10K+ predicates = 10K+ sparse tensors. Generic `RDF_EDGE` fallback needed.
5. **GraphBLAS reproducibility**: Checkpoint must capture exact matrix state for deterministic recovery.
6. **WAL format evolution**: Versioned WAL entries needed for forward compatibility.

---

## 18. Related Documents

- `../slatedb/` — SlateDB implementation details (independent backend)
- `../falkordb/integration_overview1.md` — FalkorDB integration analysis (updated for native persistence)
- `../../lib/oxigraph/src/storage/mod.rs` — Current storage module structure
- `../../lib/oxigraph/src/store.rs` — Store API