# FalkorDB Integration & Storage Abstraction Implementation Plan

## Overview

This plan covers 7 phases to:
1. **Implement the `StorageBackend` abstraction layer** (shared infrastructure)
2. **Port SlateDB to the new abstraction** (validation of abstraction)
3. **Build FalkorDB integration with native persistence** (core deliverable)
4. **Benchmark all three backends** (RocksDB, SlateDB, FalkorDB)

Each phase has explicit deliverables, acceptance criteria, and dependencies. Phases 1–3 can proceed in parallel after Phase 1 completes.

---

## Phase 1: StorageBackend Abstraction Layer (Foundation)

**Duration**: 1 week  
**Owner**: Storage team  
**Dependencies**: None  
**Blocks**: Phases 2, 3, 4

### 1.1 Create Abstraction Module Structure

```
lib/oxigraph/src/storage/
├── backend.rs           # Core traits: StorageBackend, StorageReader, StorageReadableTransaction, StorageBulkLoader, TermDictionary
├── backend_registry.rs  # BackendRegistry, BackendFactory, global registration
├── mod.rs               # Re-exports, Storage enum dispatch
├── backend_config.rs    # BackendConfig, BackendCapabilities, BackendStats, FalkorDbConfig, etc.
└── slatedb_backend.rs   # SlateDB adapter (moved from slatedb.rs)
├── rocksdb_backend.rs   # RocksDB adapter (new)
├── falkordb_backend.rs  # FalkorDB adapter (new, Phase 4)
├── falkordb_persistence.rs # FalkorDB native persistence (Phase 4)
├── falkordb_patterns.rs # Pattern-to-graph translation (Phase 4)
└── slatedb/             # Existing SlateDB implementation (unchanged)
    ├── slatedb.rs
    ├── slatedb_bridge.rs
    ├── slatedb_keyspace.rs
    └── ...
```

### 1.2 Implement Core Traraits (`backend.rs`)

**Deliverables**:
- `StorageBackend` trait with all required methods
- `StorageReader` trait with 16 quad-pattern scan methods
- `StorageReadableTransaction` trait with buffered writes + commit/rollback
- `StorageBulkLoader` trait with atomic + streaming load
- `TermDictionary` trait for shared term encoding

**Acceptance Criteria**:
- [ ] All traits compile with `Send + Sync + 'static` bounds
- [ ] QuadPattern covers all 16 SPARQL triple/quad patterns
- [ ] EncodedQuadIterator and EncodedGraphIterator type aliases work
- [ ] No backend-specific types leak into trait definitions

### 1.3 Implement Backend Registry (`backend_registry.rs`)

**Deliverables**:
- `BackendRegistry` with `register()`, `get()`, `available()`
- `BackendFactory` trait for type-erased construction
- `BackendFactoryImpl<B>` blanket implementation
- Global registry initialization in `Storage::new()`

**Acceptance Criteria**:
- [ ] Backends register via `inventory` or explicit `init()` call
- [ ] `Store::open_with_backend(config)` dispatches correctly
- [ ] Unknown backend name returns clear error

### 1.4 Configuration Types (`backend_config.rs`)

**Deliverables**:
- `BackendConfig` enum: `Memory`, `RocksDb`, `SlateDb`, `FalkorDb`
- `BackendCapabilities` struct with 12 boolean flags
- `BackendStats` struct with common + backend-specific fields
- `FalkorDbConfig`, `FalkorGraphOptions`, `FalkorDurability`, `WalSyncMode`, `CheckpointInterval`
- `SlateDbOptions` re-export (unchanged)
- `StorageOptions` re-export for RocksDB (unchanged)

**Acceptance Criteria**:
- [ ] All config types implement `Clone + Debug`
- [ ] Feature-gated configs only compile with respective features
- [ ] Default implementations provide sensible values

### 1.5 Refactor `Store` and `Storage` Dispatch

**Deliverables**:
- `Store::open_with_backend(config: BackendConfig) -> Result<Self>`
- `Store::open_read_only_with_backend(config: BackendConfig) -> Result<Self>`
- `Storage` enum wraps `Box<dyn StorageBackend>` with trait delegation
- `StorageReader` enum wraps `Box<dyn StorageReader>`
- `StorageReadableTransaction` enum wraps `Box<dyn StorageReadableTransaction>`

**Acceptance Criteria**:
- [ ] Existing `Store::new()` (memory) still works
- [ ] All SPARQL evaluation paths work through trait dispatch
- [ ] Zero overhead for memory backend (direct calls)
- [ ] No breaking changes to public `Store` API

### 1.6 Testing Infrastructure

**Deliverables**:
- `tests/backend_conformance.rs` — parameterized conformance suite
- `tests/backend_property.rs` — property-based tests for all backends
- `benches/backend_bench.rs` — criterion benchmarks with backend parameter

**Acceptance Criteria**:
- [ ] Conformance tests run against all registered backends
- [ ] Property tests catch backend-specific bugs
- [ ] Benchmarks produce comparable metrics

---

## Phase 2: Port SlateDB to Abstraction Layer

**Duration**: 1 week  
**Owner**: Storage team  
**Dependencies**: Phase 1 complete  
**Blocks**: Phase 7 (benchmarks)

### 2.1 Create `slatedb_backend.rs` Adapter

**Files**: `lib/oxigraph/src/storage/slatedb_backend.rs`

**Implementation**:
```rust
pub struct SlateDbBackend {
    storage: SlateDbStorage,
    capabilities: BackendCapabilities,
}

impl StorageBackend for SlateDbBackend {
    const NAME: &'static str = "slatedb";
    
    fn open(config: BackendConfig) -> Result<Self> { ... }
    fn open_read_only(config: BackendConfig) -> Result<Self> { ... }
    fn snapshot(&self) -> Result<Box<dyn StorageReader>> { ... }
    fn transaction(&self) -> Result<Box<dyn StorageReadableTransaction>> { ... }
    fn bulk_loader(&self) -> Box<dyn StorageBulkLoader> { ... }
    // ... flush, close, backup, stats, validate
}
```

**Adapter Types**:
- `SlateDbReaderAdapter(SlateDbStorageReader)` → `StorageReader`
- `SlateDbTransactionAdapter(SlateDbStorageReadableTransaction)` → `StorageReadableTransaction`
- `SlateDbBulkLoaderAdapter(SlateDbStorageBulkLoader)` → `StorageBulkLoader`

### 2.2 Register SlateDB Backend

**File**: `lib/oxigraph/src/storage/mod.rs`
```rust
#[cfg(feature = "slatedb")]
inventory::submit! { BackendRegistration::new(SlateDbBackend::NAME, || Box::new(SlateDbBackend)) }
```

### 2.3 Update CLI Integration

**File**: `cli/src/main.rs`
- Add `--backend` flag accepting `rocksdb`, `slatedb`, `falkordb`, `memory`
- Map to appropriate `BackendConfig` variant
- Preserve existing `--object-store` for SlateDB

### 2.4 Validation

**Acceptance Criteria**:
- [ ] All existing SlateDB tests pass unchanged
- [ ] `oxigraph serve --backend slatedb` works
- [ ] `oxigraph load --backend slatedb` works
- [ ] `oxigraph migrate --from rocksdb --to slatedb` works
- [ ] Read-only mode works (`SlateDbOptions::with_read_only(true)`)
- [ ] Backup/restore works
- [ ] Conformance suite passes 100%

---

## Phase 3: Port RocksDB to Abstraction Layer

**Duration**: 1 week  
**Owner**: Storage team  
**Dependencies**: Phase 1 complete  
**Blocks**: Phase 7 (benchmarks)

### 3.1 Create `rocksdb_backend.rs` Adapter

**Files**: `lib/oxigraph/src/storage/rocksdb_backend.rs`

**Implementation**: Wrap existing `RocksDbStorage` (from `rocksdb.rs`) with adapter types similar to SlateDB.

**Key Differences from SlateDB**:
- No async bridge needed (RocksDB is synchronous)
- Column families map directly to `BackendCapabilities`
- `object_store: false`, `graph_traversal: false`, `graphblas: false`, `mvcc: false`

### 3.2 Register RocksDB Backend

**File**: `lib/oxigraph/src/storage/mod.rs`
```rust
#[cfg(feature = "rocksdb")]
inventory::submit! { BackendRegistration::new(RocksDbBackend::NAME, || Box::new(RocksDbBackend)) }
```

### 3.3 Update CLI Integration

- Ensure `--backend rocksdb` works (default)
- Preserve existing `StoreOptions` mapping

### 3.4 Validation

**Acceptance Criteria**:
- [ ] All existing RocksDB tests pass unchanged
- [ ] `oxigraph serve --backend rocksdb` works (default)
- [ ] All SPARQL conformance tests pass
- [ ] Migration tools work bidirectionally
- [ ] Performance within 5% of pre-abstraction baseline

---

## Phase 4: FalkorDB Graph Crate Integration (PoC)

**Duration**: 2 weeks  
**Owner**: Graph team  
**Dependencies**: Phase 1 complete  
**Blocks**: Phase 5

### 4.1 Vendored FalkorDB Graph Crate

**Action**: Fork/pin FalkorDB 6.0.4 `graph` workspace crate into `third_party/falkordb-graph/`

**Rationale**:
- Internal crate, not published to crates.io
- SSPL-1.0 license requires legal review before publishing
- Vendoring allows patching for Oxigraph integration

**Files**:
```
third_party/falkordb-graph/
├── Cargo.toml           # Patched: remove SSPL, add MIT dual-license header
├── src/
│   ├── graph/
│   │   ├── graph.rs
│   │   ├── mvcc_graph.rs
│   │   ├── graphblas/
│   │   └── ...
│   ├── storage/
│   │   ├── index_backend.rs
│   │   ├── attr_backend.rs
│   │   └── ...
│   └── lib.rs
```

**Patches Required**:
1. Expose `Graph`, `MvccGraph`, `GraphSnapshot`, `GraphFork` publicly
2. Add `quad_count()`, `named_graph_count()`, `memory_usage()` methods
3. Add `validate()` for consistency checking
4. Expose typed adjacency iterators: `outgoing_edges()`, `incoming_edges()`, `all_outgoing()`
5. Add `has_edge(s, type, o, graph_id)` for exact quad check
6. Add `scan_graph(graph_id)` for full graph iteration
7. Expose `create_relationships_bulk()` for bulk loading
8. Add `apply_operation()` for WAL replay

### 4.2 Build FalkorDB Graph Crate Standalone

**Deliverable**: `third_party/falkordb-graph` compiles as `cdylib` and `rlib`

**Acceptance Criteria**:
- [ ] `cargo build --release` succeeds
- [ ] Unit tests in FalkorDB graph crate pass
- [ ] No dependencies on Redis module crate

### 4.3 Proof-of-Concept: RDF → FalkorDB Mapping

**File**: `lib/oxigraph/src/storage/falkordb_poc.rs` (throwaway)

**Implement**:
```rust
// 1. Create RDF nodes in FalkorDB
let node_id = graph.create_node(term_id)?;

// 2. Map predicate to relationship type
let type_id = graph.get_or_create_relationship_type(predicate_iri)?;

// 3. Create typed relationship
graph.create_relationship(source, type_id, target, graph_id, edge_id)?;

// 4. Bulk create
graph.create_relationships_bulk(&type_name, &srcs, &dsts, &edge_ids)?;

// 5. Query patterns
let objects = graph.outgoing_edges(subject, type_id, graph_id);
```

**Acceptance Criteria**:
- [ ] 1M triples load in < 30 seconds
- [ ] SPARQL `S,P,?O` pattern executes via typed adjacency
- [ ] Duplicate quad detection works (uniqueness mapping)
- [ ] Named graph filtering works via edge metadata
- [ ] MVCC snapshots provide consistent reads

---

## Phase 5: FalkorDB Backend with Native Persistence

**Duration**: 3 weeks  
**Owner**: Graph + Storage teams  
**Dependencies**: Phases 1, 4 complete  
**Blocks**: Phase 7

### 5.1 Native Persistence Layer (`falkordb_persistence.rs`)

#### 5.1.1 WAL Implementation

**Structures**:
```rust
pub struct WalEntry {
    pub transaction_id: u64,
    pub timestamp: u64,
    pub operations: Vec<GraphOperation>,
    pub term_mappings: Vec<(u128, u64)>,
    pub predicate_types: Vec<(u64, u64)>,
    pub checksum: u64,
}

pub enum GraphOperation {
    CreateNodes(Vec<u64>),
    CreateEdges { predicate_type_id: u64, subjects: Vec<u64>, objects: Vec<u64>, edge_ids: Vec<u64>, graph_ids: Vec<u64> },
    DeleteEdges(Vec<u64>),
    UpdateAttributes(Vec<AttributeUpdate>),
    CreatePredicateType { predicate_id: u64, type_id: u64 },
    CreateTermMapping { term_id: u128, node_id: u64 },
}
```

**WAL File Format**:
```
[entry_len: u32][entry_bytes: Vec<u8>][entry_len: u32][entry_bytes: Vec<u8]]...
```
Each entry: bincode-serialized `WalEntry` with CRC32 checksum.

**Durability Modes**:
- `WalSyncMode::None` — buffered, no fsync
- `WalSyncMode::Periodic` — background flusher every 100ms
- `WalSyncMode::Always` — fsync on every commit

#### 5.1.2 Checkpoint Implementation

**Checkpoint Format** (versioned, bincode + CRC32):
```rust
pub struct GraphCheckpoint {
    pub version: u64,                    // format version
    pub transaction_version: u64,        // WAL position
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
```

**Serialization**:
- GraphBLAS matrices → CSR format (indptr, indices, values)
- Tensors → COO format (src, dst, edge_id, predicate_type)
- Attributes → key-value blocks

#### 5.1.3 Recovery Protocol

```rust
fn recover(&self, graph: &mut Graph) -> Result<()> {
    // 1. Find latest checkpoint
    let checkpoint = self.load_latest_checkpoint()?;
    
    // 2. Restore graph state
    graph.restore_from_checkpoint(&checkpoint)?;
    
    // 3. Replay WAL from checkpoint.transaction_version + 1
    let mut wal = File::open(self.wal_path)?;
    let mut buffer = Vec::new();
    wal.read_to_end(&mut buffer)?;
    
    let mut offset = 0;
    while offset < buffer.len() {
        let len = u32::from_le_bytes(...) as usize;
        let entry: WalEntry = bincode::deserialize(&buffer[offset+4..offset+4+len])?;
        
        // Verify checksum
        if crc32fast::hash(&serialize_without_checksum(&entry)) != entry.checksum {
            return Err(Corruption("WAL checksum mismatch"));
        }
        
        if entry.transaction_id <= checkpoint.transaction_version {
            offset += 4 + len;
            continue;
        }
        
        // Replay operations
        for op in entry.operations {
            graph.apply_operation(&op)?;
        }
        for (term_id, node_id) in entry.term_mappings {
            graph.set_term_mapping(term_id, node_id);
        }
        for (pred_id, type_id) in entry.predicate_types {
            graph.set_predicate_type(pred_id, type_id);
        }
        
        offset += 4 + len;
    }
    
    Ok(())
}
```

#### 5.1.4 PersistenceLayer API

```rust
impl PersistenceLayer {
    pub fn new(data_dir, durability, wal_sync, max_wal_size, checkpoint_interval) -> Result<Self>
    pub fn write_wal(&self, ops, term_mappings, pred_types) -> Result<u64>
    pub fn commit_transaction(&self, graph, fork, ops, term_mappings, pred_types) -> Result<()>
    pub fn checkpoint(&self, graph_snapshot) -> Result<()>
    pub fn recover(&self, graph) -> Result<()>
    pub fn backup(&self, target_dir) -> Result<()>
    pub fn validate(&self) -> Result<()>
    pub fn wal_flush(&self) -> Result<()>
    pub fn close(&self) -> Result<()>
}
```

### 5.2 Term Dictionary & Predicate Registry

**File**: `falkordb_backend.rs`

```rust
struct FalkorTermDictionary {
    persistence: Arc<PersistenceLayer>,
    cache: DashMap<StrHash, OxString>,
    term_to_node: DashMap<u128, u64>,
    node_to_term: DashMap<u64, u128>,
}

struct PredicateRegistry {
    persistence: Arc<PersistenceLayer>,
    cache: DashMap<u64, u64>,  // predicate_id -> type_id
    reverse: DashMap<u64, u64>, // type_id -> predicate_id
}
```

**Load from checkpoint on startup**:
```rust
fn load(persistence: &Arc<PersistenceLayer>) -> Result<Self> {
    let mappings = persistence.load_term_mappings()?; // from latest checkpoint
    // ... populate DashMaps
}
```

**Get-or-create with WAL logging**:
```rust
fn get_or_create_type_id(&self, pred_id: u64, persistence: &PersistenceLayer) -> u64 {
    *self.cache.entry(pred_id).or_insert_with(|| {
        let type_id = persistence.allocate_relationship_type_id();
        persistence.log_predicate_type(pred_id, type_id); // async WAL append
        type_id
    })
}
```

### 5.3 Pattern-to-Graph Translation (`falkordb_patterns.rs`)

**Core Function**:
```rust
pub fn execute_pattern(
    graph: &GraphSnapshot,
    pattern: QuadPattern<'_>,
    predicates: &PredicateRegistry,
) -> EncodedQuadIterator {
    match (pattern.subject, pattern.predicate, pattern.object, pattern.graph) {
        // S, P, O, G — exact check
        (Some(s), Some(p), Some(o), Some(g)) => Box::new(
            graph.has_edge(s.node_id(), predicates.get_type_id(p), o.node_id(), g.graph_id())
                .map(move |exists| if exists { Ok(EncodedQuad::new(*s, *p, *o, *g)) } else { ... })
        ),
        
        // S, P, ?O, G — outgoing typed adjacency
        (Some(s), Some(p), None, Some(g)) => Box::new(
            graph.outgoing_edges(s.node_id(), predicates.get_type_id(p), g.graph_id())
                .map(move |o| Ok(EncodedQuad::new(*s, *p, o.into(), *g)))
        ),
        
        // ?S, P, O, G — incoming typed adjacency
        (None, Some(p), Some(o), Some(g)) => Box::new(
            graph.incoming_edges(o.node_id(), predicates.get_type_id(p), g.graph_id())
                .map(move |s| Ok(EncodedQuad::new(s.into(), *p, *o, *g)))
        ),
        
        // S, ?P, ?O, G — all outgoing
        (Some(s), None, None, Some(g)) => Box::new(
            graph.all_outgoing(s.node_id(), g.graph_id())
                .map(move |(p, o)| Ok(EncodedQuad::new(*s, p.into(), o.into(), *g)))
        ),
        
        // ?S, ?P, ?O, G — full scan
        (None, None, None, Some(g)) => Box::new(
            graph.scan_graph(g.graph_id())
        ),
        
        // Default graph variants (graph = None → default_graph_id)
        // ... 11 more patterns
    }
}
```

### 5.4 Transaction Adapter

```rust
struct FalkorTransactionAdapter {
    graph_fork: GraphFork,
    persistence_tx: PersistenceTransaction,
    term_dict: FalkorTermDictionary,
    predicate_registry: PredicateRegistry,
    config: FalkorDbConfig,
}

impl StorageReadableTransaction for FalkorTransactionAdapter {
    fn insert(&mut self, quad: &Quad) {
        // 1. Encode terms → term_ids
        // 2. Get/create node_ids via term_dict
        // 3. Get/create predicate type_id via predicate_registry
        // 4. Create relationship in graph_fork
        // 5. Record in persistence_tx for WAL
    }
    
    fn commit(self: Box<Self>) -> Result<()> {
        // ATOMIC COMMIT SEQUENCE:
        // 1. Prepare persistence transaction
        // 2. Write WAL (LINEARIZATION POINT)
        // 3. Publish graph fork (MVCC atomic swap)
        // 4. Release previous snapshot
        // 5. Trigger async checkpoint if needed
    }
}
```

### 5.5 Bulk Loader

```rust
struct FalkorBulkLoaderAdapter {
    graph: Arc<MvccGraph>,
    persistence: Arc<PersistenceLayer>,
    term_dict: FalkorTermDictionary,
    predicate_registry: PredicateRegistry,
    config: FalkorDbConfig,
    buffer: Vec<Quad>,
}

impl StorageBulkLoader for FalkorBulkLoaderAdapter {
    fn load_atomic(&mut self, quads: Vec<Quad>) -> Result<()> {
        // Single transaction, all-or-nothing
    }
    
    fn load_streaming(&mut self, quads: Vec<Quad>) -> Result<()> {
        // Chunked: use create_relationships_bulk() per predicate
        // Commit every N quads
    }
}
```

### 5.6 Backend Registration & Config

**File**: `falkordb_backend.rs`
- Implement `StorageBackend` trait
- Register in `mod.rs` with `#[cfg(feature = "falkordb")]`

**Feature Flag** (Cargo.toml):
```toml
falkordb = ["falkordb-graph"]  # No slatedb dependency
falkordb-graph = []  # Vendored third_party/falkordb-graph
```

### 5.7 Validation Checklist

**Acceptance Criteria**:
- [ ] `Store::open_with_backend(BackendConfig::FalkorDb(...))` works
- [ ] All 16 quad patterns return correct results
- [ ] Transactions provide snapshot isolation
- [ ] Crash recovery: kill -9 during write, restart → exact last committed state
- [ ] WAL fsync modes work (None/Periodic/Always)
- [ ] Checkpoints create valid recoverable snapshots
- [ ] Backup/restore works
- [ ] Bulk load: 10M triples in < 60s (streaming), < 120s (atomic)
- [ ] Conformance suite passes 100%
- [ ] Memory usage < 2x dataset size (GraphBLAS overhead)

---

## Phase 6: Query Optimizer Integration

**Duration**: 2 weeks  
**Owner**: Query team  
**Dependencies**: Phase 5 complete  
**Blocks**: Phase 7 (full benchmarks)

### 6.1 Cost Model for FalkorDB Backend

**File**: `lib/oxigraph/src/planner/falkordb_cost.rs`

**Cost Factors**:
| Operation | Cost Model |
|-----------|------------|
| `S,P,O,G` exact check | O(1) — direct edge lookup |
| `S,P,?O,G` outgoing | O(out_degree(S, P)) — typed adjacency scan |
| `?S,P,O,G` incoming | O(in_degree(O, P)) — reverse typed adjacency |
| `S,?P,?O,G` all outgoing | O(out_degree(S)) — all adjacency |
| `?S,?P,?O,G` full scan | O(|E|) — tensor iteration |
| Join (hash) | O(left + right) — standard |
| Join (graph traversal) | O(min_degree) — traverse from selective side |

**Selectivity Estimation**:
- Use predicate registry statistics (edge count per type)
- Use node degree histograms
- Fallback to uniform if no stats

### 6.2 Join Reordering for Graph Patterns

**Rule**: Prefer graph-native joins when both sides share a variable that maps to node ID.

```rust
// SPARQL: ?x :knows ?y . ?y :worksFor ?z
// Plan: traverse :knows from ?x, then :worksFor from ?y
// Instead of: hash join on ?y
```

**Implementation**:
- Extend `LogicalPlan` with `GraphTraversal` operator
- Add rewrite rule: `Join(GraphTraversal, GraphTraversal) → GraphTraversal`
- Cost-based choice between hash join and graph traversal

### 6.3 GraphBLAS Multi-Hop Optimization

**Pattern**: `?x :parent+ ?ancestor` (recursive property path)

**Optimization**:
- Detect transitive closure patterns in SPARQL algebra
- Compile to `Graph::reachability(source, type_id, max_depth)`
- Use GraphBLAS `mxm` (matrix multiply) for k-hop

**Fallback**: If depth unbounded or predicate not typed, use iterative deepening.

### 6.4 Statistics Collection

**Background Task** (every 5 min):
- Sample node degrees per predicate type
- Update predicate edge counts
- Update named graph quad counts
- Persist to checkpoint metadata

### 6.5 Validation

**Acceptance Criteria**:
- [ ] Simple graph patterns (1-2 hops) use native traversal
- [ ] Join order chooses graph traversal when beneficial
- [ ] Recursive paths use GraphBLAS when available
- [ ] Cost estimates within 2x of actual runtime
- [ ] No regression on non-graph queries (fallback to hash join)

---

## Phase 7: Benchmarking & Release Preparation

**Duration**: 2 weeks  
**Owner**: Full team  
**Dependencies**: Phases 2, 3, 5, 6 complete

### 7.1 Benchmark Suite

**File**: `benches/backend_bench.rs`

**Workloads**:
| Workload | Description | Dataset Sizes |
|----------|-------------|---------------|
| `point_lookup` | `S,P,O,G` exact match | 100K, 1M, 10M, 100M |
| `s_p_o` | `S,P,?O` single predicate | 100K, 1M, 10M |
| `spo_join` | `S,P,?O . ?O,P2,?Z` 2-hop | 100K, 1M, 10M |
| `star_query` | `S,?P,?O` star pattern | 100K, 1M |
| `graph_traversal` | `?x :knows+ ?y` 3-hop | 100K, 1M |
| `bulk_load` | Load N triples | 100K, 1M, 10M, 100M |
| `concurrent_rw` | 8 readers + 1 writer | 1M |
| `crash_recovery` | Kill -9, restart, verify | 1M |

**Metrics**:
- Latency: p50, p95, p99 (ms)
- Throughput: ops/sec, triples/sec
- Memory: RSS peak (MB)
- Disk: bytes written (bulk load, checkpoint, WAL)
- Recovery time: seconds to consistent state

### 7.2 Benchmark Execution

**Matrix**: 3 backends × 8 workloads × 4 dataset sizes = 96 data points

**Automation**:
```bash
# Run all benchmarks
cargo bench --features rocksdb,slatedb,falkordb --bench backend_bench

# Output: benches/results/{backend}_{workload}_{size}.json
```

**CI Integration**:
- Nightly benchmark run on dedicated hardware
- Track regressions > 10%
- Publish results to GitHub Pages

### 7.3 Expected Performance Targets

| Metric | RocksDB | SlateDB | FalkorDB |
|--------|---------|---------|----------|
| Point lookup (p99) | < 2ms | < 3ms | < 1ms |
| 1-hop traversal (p99) | < 5ms | < 8ms | < 2ms |
| 3-hop traversal (p99) | < 50ms | < 100ms | < 10ms |
| Bulk load (10M triples) | 60s | 45s | 30s |
| Memory (10M triples) | 2 GB | 1.5 GB | 3 GB* |
| Crash recovery (1M) | < 5s | < 3s | < 2s |

*FalkorDB higher due to GraphBLAS matrices; mitigated by typed relationship pruning

### 7.4 Documentation & Migration Guide

**Files**:
- `docs/falkordb.md` — backend overview, config, tuning
- `docs/migration.md` — RocksDB→FalkorDB, SlateDB→FalkorDB
- `docs/benchmarks.md` — published results, hardware specs

### 7.5 Release Checklist

- [ ] All conformance tests pass on all 3 backends
- [ ] Benchmarks published, no >10% regression vs baseline
- [ ] Legal review: SSPL-1.0 for FalkorDB graph crate resolved
- [ ] Cargo features documented: `rocksdb` (default), `slatedb`, `falkordb`
- [ ] Migration tools tested bidirectionally
- [ ] CHANGELOG.md updated
- [ ] Version bump: 0.5.0 (new backend)

---

## Dependency Graph

```
Phase 1 (1w) ──┬── Phase 2 (1w) ──┐
               │                  │
               ├── Phase 3 (1w)   ├── Phase 7 (2w)
               │                  │
               └── Phase 4 (2w) ──┤
                                  │
                    Phase 5 (3w) ──┤
                                  │
                    Phase 6 (2w) ──┘
```

**Critical Path**: Phase 1 → Phase 4 → Phase 5 → Phase 6 → Phase 7 = **9 weeks**

**Parallelizable**: Phases 2 & 3 can run concurrently with Phase 4 after Phase 1.

---

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| FalkorDB graph crate API changes | Medium | High | Vendor fork; pin to 6.0.4; minimize surface area |
| SSPL licensing blocks release | High | Blocker | Legal review in Phase 4; fallback to Cypher API if needed |
| GraphBLAS memory explosion | Medium | High | Predicate cardinality monitoring; generic RDF_EDGE fallback |
| WAL corruption on crash | Low | High | CRC32 per entry; periodic validation; fsync modes |
| Pattern translation bugs | Medium | Medium | Exhaustive conformance tests; property-based testing |
| Benchmark hardware variability | Medium | Medium | Dedicated CI runner; statistical significance (n≥10) |

---

## Team Allocation

| Role | Phase 1 | Phase 2 | Phase 3 | Phase 4 | Phase 5 | Phase 6 | Phase 7 |
|------|---------|---------|---------|---------|---------|---------|---------|
| Storage Engineer 1 | Lead | Lead | | | Lead | | Lead |
| Storage Engineer 2 | | | Lead | | | | |
| Graph Engineer 1 | | | | Lead | Lead | Lead | |
| Graph Engineer 2 | | | | | Lead | Lead | |
| QA Engineer | | Support | Support | | | | Lead |
| DevOps | | | | | | | Lead |

---

## Deliverable Summary

| Phase | Key Deliverable | Location |
|-------|-----------------|----------|
| 1 | `StorageBackend` trait + registry | `lib/oxigraph/src/storage/backend*.rs` |
| 2 | SlateDB adapter | `lib/oxigraph/src/storage/slatedb_backend.rs` |
| 3 | RocksDB adapter | `lib/oxigraph/src/storage/rocksdb_backend.rs` |
| 4 | Vendored FalkorDB graph crate + PoC | `third_party/falkordb-graph/` + `falkordb_poc.rs` |
| 5 | FalkorDB backend + native persistence | `falkordb_backend.rs`, `falkordb_persistence.rs`, `falkordb_patterns.rs` |
| 6 | Cost model + join optimizer | `planner/falkordb_cost.rs` |
| 7 | Benchmarks + docs + release | `benches/`, `docs/`, `CHANGELOG.md` |

---

## Appendix: File Creation Checklist

### New Files (Phase 1)
- [ ] `lib/oxigraph/src/storage/backend.rs`
- [ ] `lib/oxigraph/src/storage/backend_registry.rs`
- [ ] `lib/oxigraph/src/storage/backend_config.rs`
- [ ] `lib/oxigraph/src/storage/slatedb_backend.rs`
- [ ] `lib/oxigraph/src/storage/rocksdb_backend.rs`
- [ ] `tests/backend_conformance.rs`
- [ ] `tests/backend_property.rs`
- [ ] `benches/backend_bench.rs`

### New Files (Phase 4)
- [ ] `third_party/falkordb-graph/` (vendored)
- [ ] `lib/oxigraph/src/storage/falkordb_poc.rs` (throwaway)

### New Files (Phase 5)
- [ ] `lib/oxigraph/src/storage/falkordb_backend.rs`
- [ ] `lib/oxigraph/src/storage/falkordb_persistence.rs`
- [ ] `lib/oxigraph/src/storage/falkordb_patterns.rs`

### New Files (Phase 6)
- [ ] `lib/oxigraph/src/planner/falkordb_cost.rs`

### Modified Files
- [ ] `lib/oxigraph/src/storage/mod.rs` — re-exports, registration
- [ ] `lib/oxigraph/src/store.rs` — `open_with_backend()`
- [ ] `cli/src/main.rs` — `--backend` flag
- [ ] `Cargo.toml` — `falkordb` feature, vendored dependency
- [ ] `docs/falkordb.md`, `docs/migration.md`, `docs/benchmarks.md`