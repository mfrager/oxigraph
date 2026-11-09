# Five-phase implementation plan: SlateDB integration into Oxigraph

Goal: Implement SlateDB as an alternative persistent storage backend for Oxigraph, retaining existing RDF storage semantics, index encodings, transaction guarantees and query behavior.

Architecture: One SlateDB database per Oxigraph store, with eleven binary-prefixed logical keyspaces. Preserve Oxigraph's existing key/value serialization. Use a dedicated Tokio runtime to bridge SlateDB's asynchronous API to Oxigraph's synchronous storage interface.

## Phase 1 — Storage foundation and backend abstraction

Deliverable: Working SlateDB adapter

Objective: Establish the SlateDB storage layer independently of Oxigraph's higher-level RDF operations.

### Implementation

1. Pin compatible versions of Oxigraph, SlateDB and Tokio.
2. Introduce a `slatedb` Cargo feature without removing RocksDB.
3. Implement the SlateDB database lifecycle:
   - Open and initialize.
   - Configure object storage.
   - Close and flush.
   - Recover after process restart.
4. Implement a dedicated Tokio runtime bridge.
5. Implement binary-prefixed keyspaces.

Define the namespace registry:

| ID       | Keyspace |
| -------- | -------- |
| `0x0001` | `id2str` |
| `0x0002` | `spog`   |
| `0x0003` | `posg`   |
| `0x0004` | `ospg`   |
| `0x0005` | `gspo`   |
| `0x0006` | `gpos`   |
| `0x0007` | `gosp`   |
| `0x0008` | `dspo`   |
| `0x0009` | `dpos`   |
| `0x000A` | `dosp`   |
| `0x000B` | `graphs` |

### Suggested Rust interfaces

```
trait KeyValueStore {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    fn delete(&self, key: &[u8]) -> Result<()>;
    fn scan(&self, range: KeyRange) -> Result<Iterator>;
}

trait StorageBackend {
    fn open(config: StorageConfig) -> Result<Self>;
    fn reader(&self) -> Result<StorageReader>;
    fn transaction(&self) -> Result<StorageTransaction>;
    fn flush(&self) -> Result<()>;
}
```

These are conceptual interfaces; adapt signatures to Oxigraph's existing traits and error types.

### Completion criteria

- SlateDB opens and closes reliably.
- Binary namespace isolation works.
- Ordered key scans preserve existing key ordering.
- Reads and writes survive process restarts.
- RocksDB backend remains functional.

## Phase 2 — RDF indexes, readers and transactions

Deliverable: Functionally complete storage backend

Objective: Implement Oxigraph's actual RDF storage operations.

### Implementation

1. Connect the eleven logical keyspaces to the existing Oxigraph index implementation.
2. Preserve all existing RDF key serialization and ordering.
3. Implement:
   - Point lookups.
   - Prefix and range scans.
   - Ordered iterators.
   - Snapshot readers.
   - Transactional writes.
   - Transactional deletes.
4. Implement graph metadata and dictionary operations.
5. Integrate SlateDB transaction handling with Oxigraph's existing transaction lifecycle.

### Transaction model

```
Oxigraph Transaction
        |
        v
SlateDB Transaction
        |
        +-- Dictionary updates
        +-- SPOG updates
        +-- POSG updates
        +-- OSPG updates
        +-- GSPO updates
        +-- GPOS updates
        +-- GOSP updates
        +-- Default graph indexes
        +-- Graph metadata
        |
        v
Atomic Commit
        |
        v
Durability policy
```

### Important engineering requirements

- All index mutations belonging to one RDF transaction must commit atomically.
- Readers must observe consistent snapshots.
- Iterator lifetimes must not outlive the underlying snapshot.
- Transaction conflicts must map correctly to Oxigraph errors.
- Avoid blocking the Tokio runtime from within its own worker threads.
- Distinguish transaction commit from object-store durability.

### Completion criteria

- RDF insertion, lookup and deletion work.
- SPARQL query evaluation can use SlateDB readers.
- Transaction rollback and commit work.
- Concurrent reads and writes preserve correctness.
- Snapshot and isolation tests pass.

## Phase 3 — Deletion, bulk loading and performance

Deliverable: Production-capable storage operations

Objective: Optimize the backend for realistic RDF workloads.

### Implementation

Deletion

- Single-quad deletion across all affected indexes.
- Multi-quad transactional deletion.
- Graph deletion through ordered range scans.
- Store-wide clearing.
- Optional generation-based index replacement.

For large graph deletion, use a visibility-safe strategy rather than exposing partially deleted data. Evaluate transaction-size limits and whether graph-level tombstones or generation-based indexes are necessary.

Bulk loading

- Implement bounded write buffering.
- Batch dictionary and index writes.
- Avoid unnecessary per-record durability waits.
- Provide explicit flush and completion semantics.
- Measure object-store request frequency.

Performance

- Add configurable block caching.
- Tune WAL flush intervals.
- Tune L0 SST sizes and compaction.
- Implement bounded iterator prefetching.
- Avoid allocating large intermediate result sets.
- Benchmark different batch sizes and concurrent reader counts.

### Initial tuning configuration

```
[slatedb]
flush_interval_ms = 1000
l0_sst_size_mb = 64
max_unflushed_bytes_mb = 256
iterator_prefetch_items = 1024
write_batch_items = 1000
```

Treat these as initial experimental values, not guaranteed optimal settings.

### Benchmark workloads

| Workload          | Measurement        |
| ----------------- | ------------------ |
| RDF insertion     | Quads/sec          |
| RDF deletion      | Quads/sec          |
| Dictionary lookup | Latency            |
| Index range scan  | Throughput         |
| SPARQL queries    | Latency            |
| Bulk import       | Total duration     |
| Concurrent reads  | Throughput         |
| Recovery          | Startup duration   |
| S3 operations     | Requests and bytes |

### Completion criteria

- Large imports complete without unbounded memory growth.
- Graph deletion preserves transactional visibility.
- Query performance is measured against RocksDB.
- Storage and compaction behavior are observable.
- Performance bottlenecks are documented.

## Phase 4 — Configuration, migration and RustFS integration

Deliverable: Deployable alternative backend

Objective: Make SlateDB usable as a persistent Oxigraph deployment and provide a migration path.

### Implementation

Backend selection

```
[storage]
backend = "slatedb"

[slatedb]
endpoint = "http://rustfs:9000"
bucket = "oxigraph"
region = "us-east-1"
path = "database"
```

Support credentials through environment variables or a secret provider.

Storage configuration

- Local filesystem/object-store configuration.
- S3-compatible endpoints.
- RustFS configuration.
- WAL and durability policies.
- Cache settings.
- Compaction settings.
- Shutdown and flush behavior.

Migration utility

Build a RocksDB-to-SlateDB importer:

```
Existing RocksDB store
        |
        v
Read-only index iteration
        |
        v
Preserve existing binary keys
        |
        v
Bounded SlateDB write batches
        |
        v
Index consistency validation
        |
        v
Open migrated Oxigraph store
```

The importer must:

- Preserve every key and value.
- Preserve dictionary IDs.
- Validate index cardinalities.
- Detect incomplete imports.
- Support resumable migration or clean restart.
- Prevent writes to the source during migration.

RustFS replication

Configure:

- Bucket versioning.
- Asynchronous replication.
- Existing-object backfill.
- Replication failure monitoring.
- Retention and deletion policies.
- Periodic recovery testing.

Do not treat asynchronous object replication as a consistent database snapshot. Implement snapshot-aware backup and recovery procedures.

### Completion criteria

- Oxigraph can be configured to use SlateDB without application-level changes.
- Existing RocksDB stores can be migrated.
- RustFS can serve as the S3-compatible object store.
- Replica restoration is tested successfully.
- Credentials and storage configuration are production-safe.

## Phase 5 — Reliability, compatibility and release

Deliverable: Release-ready backend

Objective: Establish confidence in correctness, recovery and operational behavior.

### Implementation

Correctness testing

- Run the existing Oxigraph test suite against both backends.
- Compare RDF query results.
- Test all index permutations.
- Test named and default graphs.
- Test duplicate insertion and deletion.
- Test transaction isolation and rollback.
- Test concurrent readers and writers.

Fault injection

- Kill the process during writes.
- Interrupt object-store connectivity.
- Simulate failed flushes.
- Simulate failed compaction.
- Restart with unflushed WAL data.
- Test incomplete RustFS replication.
- Test recovery from the secondary bucket.

Observability

Expose metrics for:

- Read/write latency.
- Transaction duration.
- Commit and durability latency.
- WAL size and flush frequency.
- Memtable and SST sizes.
- Compaction activity.
- Object-store requests and bytes.
- Iterator performance.
- Cache hit ratios.
- Recovery failures.

Release work

- Document backend limitations.
- Document performance characteristics.
- Publish migration instructions.
- Add operational configuration examples.
- Add RustFS deployment and recovery documentation.
- Keep RocksDB as the default until SlateDB passes acceptance criteria.

### Completion criteria

- Existing Oxigraph behavior is preserved.
- Crash recovery is repeatable.
- Migration and restore procedures are documented and tested.
- Performance is understood relative to RocksDB.
- No unresolved data-loss or transaction-consistency issues remain.

## Overall implementation sequence

1. Phase 1 — Foundation

   Database adapter, namespaces, runtime bridge and lifecycle.
2. Phase 2 — RDF semantics

   Indexes, readers, snapshots and atomic transactions.
3. Phase 3 — Performance

   Deletion, bulk loading, caching and benchmarks.
4. Phase 4 — Deployment

   Configuration, migration, S3 and RustFS replication.
5. Phase 5 — Production readiness

   Fault tolerance, compatibility, recovery and release.

## Key architectural decisions

| Area              | Decision                                |
| ----------------- | --------------------------------------- |
| Database layout   | One SlateDB database per Oxigraph store |
| Logical indexes   | Eleven binary-prefixed keyspaces        |
| RDF serialization | Preserve existing Oxigraph encoding     |
| Async integration | Dedicated Tokio runtime bridge          |
| Transactions      | SlateDB atomic transactions             |
| Durability        | Explicit commit/durability policy       |
| Large deletion    | Visibility-safe range deletion          |
| Bulk import       | Bounded batch writes                    |
| Object storage    | S3-compatible backend                   |
| Deployment        | RustFS supported                        |
| Replication       | Asynchronous, snapshot-aware recovery   |
| Compatibility     | Retain RocksDB backend                  |

Critical milestone: Phase 2 is the architectural proof. If SlateDB cannot reproduce Oxigraph's existing transaction, snapshot and ordered-iterator semantics without compromising correctness, resolve that before investing in bulk-loading optimizations or RustFS deployment.
