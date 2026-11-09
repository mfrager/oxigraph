# Compact design plan: SlateDB backend for Oxigraph

## Objective

Add SlateDB as an alternative persistent storage backend while preserving Oxigraph's existing RDF storage semantics, index layouts, transaction behavior and query interfaces.

Core design decision: use one SlateDB database with prefixed logical keyspaces corresponding to Oxigraph's existing RocksDB column families. Preserve Oxigraph's existing key encoding; do not redesign the RDF indexes as part of this integration.

## 1. Storage architecture

```
Oxigraph Storage API
       |
       v
Storage backend abstraction
       |
       +-- RocksDB backend (existing)
       |
       +-- SlateDB backend (new)
                |
                +-- Tokio runtime bridge
                |
                +-- SlateDB database
                       |
                       +-- id2str
                       +-- spog
                       +-- posg
                       +-- ospg
                       +-- gspo
                       +-- gpos
                       +-- gosp
                       +-- dspo
                       +-- dpos
                       +-- dosp
                       +-- graphs
```

Each logical keyspace uses a binary prefix:

`namespace_id || existing_oxigraph_key`

Keep namespace identifiers stable and centrally defined. All index ordering and RDF key serialization remain owned by Oxigraph.

## 2. Implement the backend adapter

Add a new backend module, keeping RocksDB intact.

Suggested structure:

```
storage/
  backend/
    mod.rs
    rocksdb.rs
    slatedb.rs
    slatedb/
      keyspace.rs
      reader.rs
      transaction.rs
      iterator.rs
      runtime.rs
      config.rs
```

Core types:

```
struct SlateDbBackend {
    db: Arc<slatedb::Db>,
    runtime: RuntimeBridge,
    keyspaces: KeyspaceRegistry,
}

struct Keyspace {
    id: u16,
    name: &'static str,
}

struct RuntimeBridge {
    // Dedicated Tokio runtime and synchronous request interface
}
```

Implement the operations required by Oxigraph's existing storage backend:

- `get`, `put`, `delete`
- Ordered range scans and prefix scans
- Snapshot readers
- Atomic write transactions
- Iterators with existing ordering semantics
- Database open, close and flush

Use SlateDB's `Db`, `DbTransaction`, `DbSnapshot` and `WriteBatch` where appropriate. SlateDB 0.18.1 provides asynchronous scans, atomic write batches and transactional reads/writes.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.rs\&sz=32)

Rust+2



## 3. Handle synchronous versus asynchronous APIs

Oxigraph's storage implementation is synchronous; SlateDB is asynchronous.

For the first implementation:

- Run SlateDB on a dedicated Tokio runtime.
- Provide a synchronous request/response bridge.
- Keep async execution off Oxigraph's calling threads where practical.
- Implement iterators using bounded channels or paged prefetching.
- Avoid creating a new runtime for each operation.
- Ensure the bridge cannot deadlock when Oxigraph is used inside another Tokio runtime.

Do not attempt a wholesale async refactor of Oxigraph.

## 4. Preserve transaction semantics

This is the most important correctness requirement.

A single RDF update can affect several indexes and dictionary entries. Those writes must commit atomically.

Use one SlateDB transaction for each Oxigraph storage transaction:

```
Begin transaction
    |
    +-- Update ID dictionary
    +-- Update SPOG
    +-- Update POSG
    +-- Update OSPG
    +-- Update GSPO / GPOS / GOSP
    +-- Update default-graph indexes
    +-- Update graph metadata
    |
Commit
    |
Await durability according to policy
```

Use the transaction isolation level required to preserve Oxigraph's existing concurrency guarantees. Test conflict detection and snapshot behavior explicitly.

Important: SlateDB transaction commit and object-store durability are separate events. Its commit returns a write handle; durability requires waiting on that handle or flushing the database.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.rs\&sz=32)

Rust+1



## 5. Implement deletion and bulk operations

| Operation              | Implementation                                                                 |
| ---------------------- | ------------------------------------------------------------------------------ |
| Single quad deletion   | Atomic deletes across all relevant indexes                                     |
| Multiple quad deletion | Transactional batches                                                          |
| Graph deletion         | Range scan and batched deletion                                                |
| Clear entire store     | Generation-based keyspace replacement, if compatible with Oxigraph's semantics |
| Bulk import            | Buffered writes with controlled commit sizes                                   |
| Index rebuild          | Rebuild into a new generation, then switch visibility                          |

Graph deletion needs special care: a very large graph may exceed practical transaction sizes. Do not expose partially deleted graphs unless Oxigraph's transaction and visibility semantics explicitly permit it. A generation or tombstone-based approach may be necessary.

## 6. Add configuration

Introduce a Cargo feature:

```
[features]
rocksdb = ["oxrocksdb-sys"]
slatedb = ["dep:slatedb", "dep:tokio"]
```

The precise feature wiring should follow the pinned Oxigraph release. Current Oxigraph 0.5.13 exposes RocksDB as an optional dependency, making an alternative backend feature a natural integration point.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.rs\&sz=32)

Docs.rs+1



Suggested configuration:

```
[storage]
backend = "slatedb"

[slatedb]
endpoint = "http://rustfs:9000"
bucket = "oxigraph"
region = "us-east-1"
path = "database"
flush_interval_ms = 1000
l0_sst_size_mb = 64
max_unflushed_bytes_mb = 256
```

These are proposed settings, not SlateDB defaults. Use the S3-compatible object-store implementation for RustFS and keep credentials in environment variables or a secret provider.

Start with:

- WAL enabled.
- A 1-second flush interval.
- Configurable local block cache.
- Configurable SST cache.
- Explicit durability policy for commits.

SlateDB documents configurable WAL flushing, SST thresholds and object-store-backed persistence.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.rs\&sz=32)

Docs.rs+1



## 7. Migration and testing

Migration utility

Implement a one-time RocksDB-to-SlateDB migration:

1. Open RocksDB read-only.
2. Iterate its logical indexes and dictionary.
3. Preserve existing serialized keys and values.
4. Write to SlateDB in bounded batches.
5. Validate counts and index consistency.
6. Open the migrated database with Oxigraph.
7. Run RDF and SPARQL verification.

Do not copy RocksDB SST files into SlateDB.

Test matrix

- Exact key ordering and prefix boundaries.
- All RDF index permutations.
- Named and default graph operations.
- Duplicate insertions and missing deletions.
- Concurrent readers and writers.
- Transaction rollback and conflict detection.
- Snapshot isolation.
- Large graph deletion.
- Restart and crash recovery.
- Object-store interruption and recovery.
- Bulk loading and query performance.
- RustFS replication and restore testing.

## 8. Implementation phases

| Phase                 | Deliverable                                          |
| --------------------- | ---------------------------------------------------- |
| 1. Backend foundation | SlateDB adapter, namespaces, runtime bridge          |
| 2. Storage semantics  | Readers, iterators, transactions, deletion           |
| 3. Integration        | Cargo feature, configuration, open/close, durability |
| 4. Migration          | RocksDB importer and validation                      |
| 5. Production testing | Performance, crash recovery, RustFS backup/restore   |

## Design decisions to keep fixed

1. One SlateDB database per Oxigraph store.
2. Eleven logical keyspaces using binary prefixes.
3. Existing Oxigraph RDF key encodings unchanged.
4. All related index updates committed atomically.
5. Synchronous Oxigraph API retained initially.
6. RocksDB remains available as a separate backend.
7. RustFS is the object-storage layer, not part of the Oxigraph storage abstraction.

Recommended first milestone: build a standalone Rust prototype implementing the existing Oxigraph keyspace operations against SlateDB, including ordered scans, transactions and restart recovery. Validate it before integrating the adapter into Oxigraph itself.
