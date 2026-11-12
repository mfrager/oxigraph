# SlateDB backend for Oxigraph — code-audited implementation plan

Status: supersedes `implementation_plan1.md` / `integration_plan1.md` where they disagree.
Those documents were written from the outside (generic descriptions of `Store`, "the storage
trait", eleven keyspaces). This one was written after reading
`lib/oxigraph/src/storage/{mod,rocksdb,rocksdb_wrapper,binary_encoder,numeric_encoder,error}.rs`
and `lib/oxigraph/src/store.rs` line by line, and after checking the SlateDB 0.18 API surface.

Everything below that names a Rust item names an item that exists in this tree, at version
`0.6.0-dev` (edition 2024, `rust-version = 1.87`).

---

## 0. What this plan changes about the earlier plans

| # | Earlier plan said | Reality in this tree / in SlateDB 0.18 | Consequence |
|---|---|---|---|
| 1 | "Add a backend module, keeping RocksDB intact", with a `KeyValueStore` / `StorageBackend` trait | There is no storage trait. `Storage` is a `#[derive(Clone)]` enum (`storage/mod.rs:51`) dispatching to `StorageKind::{RocksDb, Memory}`, with five parallel handle enums (`StorageReaderKind`, `StorageTransactionKind`, `StorageReadableTransactionKind`, `StorageBulkLoaderKind`, `DecodingQuadIteratorKind`, `DecodingGraphIteratorKind`) | No new abstraction is introduced. Phase 1–2 add `SlateDb` variants to the existing enums. A trait would be a workspace-wide refactor the request does not ask for |
| 2 | `feature = "rocksdb"` is "a natural integration point" | `feature = "rocksdb"` gates **113** sites in `lib/oxigraph` alone (69 in `storage/mod.rs`): the `binary_encoder` module itself, `StorageOptions`, `StoreOptions`, `Storage::{open, open_with_options, open_read_only, flush, compact, backup}`, `Store::{open, open_with_options, open_read_only, flush, optimize, backup}`, plus benches/CLI/Python/`sparql/local_service.rs` fallbacks | Phase 1 must introduce a backend-neutral on-disk gate. This is a mechanical but non-trivial refactor, budgeted explicitly below |
| 3 | Eleven keyspaces (`id2str` … `graphs`) | `rocksdb.rs` defines **twelve** column families: the eleven above **plus `default`**, which holds `oxversion` (`ensure_version` / `update_version`) | Twelve namespaces: eleven data + one metadata |
| 4 | `namespace_id: u16` | Oxigraph index keys are 33–132 bytes (`WRITTEN_TERM_MAX_SIZE = 33`, 4-term keys ≤ 132); a `u16` prefix is pure overhead | Use a `u8` namespace byte; 0x00 reserved for metadata |
| 5 | Config: `flush_interval_ms`, `l0_sst_size_mb`, `max_unflushed_bytes_mb`, "WAL enabled: true" | `slatedb::config::Settings` has `flush_interval: Option<Duration>`, `l0_sst_size_bytes: usize`, `max_unflushed_bytes: usize`. WAL is a **compile-time** feature (`wal_disable`), not a runtime knob. `object_store_cache_options` is deprecated since 0.18.1 (use `slatedb_mirror::ObjectStoreMirror` / `db_cache::foyer_hybrid::FoyerHybridCache`) | Phase 4 config surface must use real field names; caching strategy re-decided |
| 6 | "Dedicated Tokio runtime + synchronous request/response bridge" | `Runtime::block_on` panics when called from inside *any* async context, and Oxigraph is a library — a user may call `store.insert()` from a Tokio handler | The bridge must never `block_on` on the caller's thread. Design in §4.4 |
| 7 | "Use one SlateDB transaction per Oxigraph storage transaction" | `Store::start_transaction()` returns a **readable** transaction (`start_readable_transaction`, `store.rs:378`), not a write batch. SlateDB distinguishes `Db::write(WriteBatch)` (blind atomic write) from `Db::begin(IsolationLevel)` (read-your-writes, conflict-detected) | Two different SlateDB primitives for two different Oxigraph types. This is the crux of Phase 2 |
| 8 | "Commit, then await durability according to policy" | `DbTransaction::commit()` returns `Option<WriteHandle>` and *never* waits for object storage; `WriteHandle::await_durable()` or `Db::flush()` do | A durability policy enum is required, with a default. RocksDB's default `rocksdb_write` does not await WAL fsync either, but it *does* survive process kill. See decision **D5** |
| 9 | "Commit atomic write transactions" (no conflict story) | SlateDB detects conflicts (`ErrorKind::Transaction`). Oxigraph's API has no conflict error and never retries | Backend must serialize commits in-process and retry conflicts internally; see **D6** |
| 10 | "Graph deletion via range scan, clear via generation replacement" | SlateDB 0.18 `WriteBatch` exposes only `put` / `put_with_options` / `put_bytes` / `merge` / `delete`. **No range delete.** RocksDB's `Transaction::remove_range` (`rocksdb_wrapper.rs:967`) has no counterpart | `StorageTransaction::clear_default_graph` / `clear_all_named_graphs` / `clear` are infallible and currently O(1). They must become "record the range, expand at commit"; see **D7** |
| 11 | "Bulk import: buffered writes with controlled commit sizes" | RocksDB bulk load ingests sorted SST files (`insert_stt_files`) — atomic, sorted, no compaction cost. SlateDB has no external-SST ingest | Bulk loader is a rewrite, and its *atomicity* contract has to be renegotiated; see **D8** |
| 12 | "Only one read-write store can exist" preserved by the backend | SlateDB has no `LOCK` file; a second writer gets a newer writer epoch and **fences** the older one (`ErrorKind::Closed(CloseReason)`), which surfaces mid-operation, not at `open` | Behaviour change to document and test; see **D9** |

---

## 1. Target architecture

```
                      Store (store.rs, unchanged API)
                                  │
                        Storage { kind }  ── StorageKind::SlateDb(Arc<SlateDbStorage>)
                                  │
        ┌──────────────┬──────────┴─────────┬────────────────────────────┐
        │              │                    │                            │
  SlateDbStorage   SlateDbStorage     SlateDbStorage              SlateDbStorage
     Reader         Transaction        ReadableTransaction         BulkLoader
        │              │                    │                            │
        └──────────────┴──────────┬──────────┴────────────────────────┘
                                  │
                          RuntimeBridge  (private multi-thread Tokio runtime;
                                  │        caller threads only block on channels)
                                  │
                    slatedb::Db  ──►  object_store::ObjectStore
                                  │      (LocalFileSystem | S3/RustFS | InMemory in tests)
                                  │
        ns 0x00 meta   ns 0x01 id2str   ns 0x02 spog … ns 0x0A dosp   ns 0x0B graphs
        (magic, format, oxversion,      (Oxigraph key bytes, unchanged encoding)
         durability floor)
```

New files, following the crate's existing flat convention (`rocksdb.rs` + `rocksdb_wrapper.rs`),
not the `storage/backend/` tree proposed earlier:

| File | Contents | Approx. size |
|---|---|---|
| `lib/oxigraph/src/storage/slatedb_keyspace.rs` | namespace registry, `prefixed(ns, key)`, `prefix_bounds(ns, prefix)`, `Ns` enum | 120 lines |
| `lib/oxigraph/src/storage/slatedb_bridge.rs` | `RuntimeBridge`, `SlateDbError` → `StorageError` mapping, `SlateDbDurability` | 250 lines |
| `lib/oxigraph/src/storage/slatedb.rs` | `SlateDbStorage`, reader, iterators, both transaction types, bulk loader, unit tests | 1 300–1 600 lines |
| `lib/oxigraph/src/storage/slatedb_options.rs` | public `SlateDbOptions` + object-store construction (Phase 4) | 250 lines |

Nothing in `binary_encoder.rs`, `numeric_encoder.rs`, `memory.rs` or `store.rs` changes key
encoding. **The RDF encoding is frozen.** `SlateDb` is a byte-oriented key/value target exactly
like RocksDB's column families.

---

## 2. Audited surface: exactly what must be implemented

### 2.1 Storage handle inventory

Every method below has an exact RocksDB counterpart that the SlateDB backend must match, because
`storage/mod.rs` calls them through the enum:

| Type | Methods | Notes |
|---|---|---|
| `Storage` | `open`, `open_with_options`, `open_read_only`, `snapshot`, `start_transaction`, `start_readable_transaction`, `flush`, `compact`, `backup`, `bulk_loader` | `Storage::new()` stays memory |
| Reader | `len`, `is_empty`, `contains`, `quads_for_pattern` (16 pattern cases), `named_graphs`, `contains_named_graph`, `contains_str`, `validate` + `StrLookup::get_str` | `quads_for_pattern` is a 16-arm decision tree (`rocksdb.rs:442`) that picks 1 or 2 index scans; copy it verbatim, only the scan constructor changes |
| `StorageTransaction` | `insert`, `insert_named_graph`, `remove`, `clear_default_graph`, `clear_all_named_graphs`, `clear_all_graphs`, `remove_all_named_graphs`, `clear`, `commit` | **All mutators return `()`** — infallible. See **D3** |
| `StorageReadableTransaction` | `reader`, `insert`, `insert_named_graph`, `remove`, `clear_graph` (fallible, batched by `BATCH_SIZE = 100_000`), `clear_all_named_graphs`, `clear_all_graphs`, `remove_named_graph`, `remove_all_named_graphs`, `clear`, `commit` | Read-your-writes within the transaction; used by `Store::clear_graph`, `remove_named_graph`, `Store::start_transaction` |
| `StorageBulkLoader` | `on_progress`, `without_atomicity`, `load_batch(quads, max_num_threads)`, `commit` | See **D8** |

### 2.2 How a write is shaped (this drives the transaction design)

`RocksDbStorageTransaction::insert` (`rocksdb.rs:945`) writes, per quad, into an in-memory
`rocksdb_writebatch_t` — no I/O, cannot fail:

* default graph → 3 index keys + N `id2str` entries;
* named graph → 6 index keys + 1 `graphs` key + N `id2str` entries;
* every index value is **empty** (`insert_empty`); only `id2str` carries a value.

So the RocksDB "transaction" is already a buffered operation log. Mirroring that shape with
`Vec<Op>` gives SlateDB identical semantics and memory profile, and keeps the mutators infallible.

### 2.3 Reader lifetimes

* `Storage::snapshot(&self) -> StorageReader<'static>` — must be `'static`, `Send + Sync`
  (`rocksdb.rs::tests::test_send_sync`).
* `StorageReadableTransaction::reader(&self) -> StorageReader<'_>` — **borrows the transaction**
  and must see its uncommitted writes. `store.rs` relies on the borrow to make
  `Transaction::commit(self)` sound; therefore readers cannot outlive the transaction and the
  SlateDB transaction can be owned outright (no `Arc`/`try_unwrap` gymnastics):

```rust
enum SlateDbReaderInner<'a> {
    Snapshot(Arc<slatedb::DbSnapshot>), // implements DbReadOps
    Transaction(&'a slatedb::DbTransaction), // also implements DbReadOps
}
```

### 2.4 Query engine contract

`spareval` consumes `StorageReader` synchronously (`Iterator<Item = Result<EncodedQuad, StorageError>>`)
and `StorageReader` also feeds `DatasetView` (`sparql/dataset.rs`) and the fused local-SERVICE
path. Requirement: **the sync `Iterator` must exist before consumption begins and must not block
the caller indefinitely.** SlateDB iterators are async (`DbIterator::next().await`). §5.2.

---

## 3. Frozen design decisions

### D1 — One SlateDB database, twelve `u8` namespaces

```
0x00 meta     magic ("oxigraph\0"), engine id, format version, oxversion
0x01 id2str   0x02 spog   0x03 posg   0x04 ospg
0x05 gspo     0x06 gpos   0x07 gosp
0x08 dspo     0x09 dpos   0x0A dosp
0x0B graphs
```

`key = [ns] || oxigraph_key`. Byte-append preserves `memcmp` order inside a namespace, which is
all the index ordering Oxigraph needs. `default` CF's `oxversion` becomes `0x00 "oxversion"`
(8-byte big-endian, same encoding as today).

Range scans reuse the existing upper-bound algorithm from `rocksdb_wrapper.rs::Reader::scan_prefix`
(increment the last non-`0xFF` byte) and call:

```rust
// subrange bounds are suffixes relative to `prefix` — SlateDB handles the namespace for us
reader.scan_prefix([ns], start..end).await?   // end = exclusive upper bound, or `..`
```

Rejected: one SlateDB database per index. Eleven databases means eleven WALs, eleven flushers and
no atomic multi-index commit — which is the one thing this backend must get right.

### D2 — The RDF encoding is not touched

`QuadEncoding`, `encode_term*`, `write_*_quad`, `decode_term`, `EncodedQuad` are reused as-is.
The only change to `binary_encoder.rs` is its `cfg` gate (§4.1). Data written by the RocksDB
backend and by the SlateDB backend must be byte-identical per key, which Phase 4's migration
relies on and Phase 5 asserts with a differential test.

### D3 — Buffered, infallible mutations; fallible `commit`

```rust
enum SlateDbOp {
    Put   { key: Vec<u8>, value: Vec<u8> }, // key already namespace-prefixed
    Delete(Vec<u8>),
    ClearRange { ns: Ns, start: Vec<u8>, end: Option<Vec<u8>> }, // expanded at commit
}
```

`SlateDbStorageTransaction { ops: Vec<SlateDbOp>, storage: &SlateDbStorage }`. Mutators push and
return `()`. `commit()` applies everything in **one** SlateDB commit and is the only fallible
step — identical to today's `rocksdb_writebatch_*` + `rocksdb_write`.

Memory: bounded by the encoded size of the transaction, same as RocksDB today. `store.rs` already
documents "the transaction keeps the complete set of changes into memory".

### D4 — Two commit paths

| Oxigraph type | SlateDB primitive | Why |
|---|---|---|
| `StorageTransaction` (no `ClearRange` ops) | `Db::write(WriteBatch)` | Blind writes; no reads ⇒ no conflict detection ⇒ cannot fail with `ErrorKind::Transaction`, and it is the cheapest path |
| `StorageTransaction` with `ClearRange` ops | `Db::begin(IsolationLevel::Snapshot)` → scan+delete → `commit()` | Ranges must be expanded; all deletes and puts land in one atomic commit |
| `StorageReadableTransaction` | `Db::begin(IsolationLevel::Snapshot)` | Snapshot isolation = write-write conflict detection only. SerializableSnapshot is **not** used: Oxigraph's contract is repeatable read, and SSI would surface read-write conflicts that its API cannot express |

Note `DbTransaction` implements `DbReadOps`, so `SlateDbStorageReaderInner::Transaction` is a
direct `&DbTransaction`. `DbTransaction::put` takes `&self`, `commit` consumes `self` — both fit
the borrow structure in §2.3.

### D5 — Durability policy (default: `Wal`)

| Policy | Implementation | Survives process kill | Survives machine/container loss | Cost |
|---|---|---|---|---|
| `Process` | return after `commit()` resolves (write is in memtable + WAL queue) | no | no | cheapest; loss window ≈ `flush_interval` |
| **`Wal` (default)** | `commit()` then `WriteHandle::await_durable()` | yes | yes (object store) | one coalesced WAL PUT per `flush_interval`; commit latency ≤ `flush_interval` + PUT |
| `Flush` | additionally `Db::flush()` (memtable → L0) | yes | yes | highest; only for shutdown/`Store::flush` |

`Store::flush()` maps to `Db::flush()` regardless of policy. Default is `Wal` for parity with
RocksDB (whose WAL survives `SIGKILL`); `Process` is the documented opt-in for throughput.
`Store::insert` in a loop with `Wal` costs up to one flush interval per call — the plan requires a
`docs/` note pointing at `Store::extend` / transactions, which is already Oxigraph's guidance.

### D6 — Commits are serialized; conflicts are retried, never surfaced

```rust
// inside SlateDbStorage
commit_lock: std::sync::Mutex<()>,
```

* Every commit takes `commit_lock` for its whole duration. This removes *all* intra-process
  conflicts, which is the only kind Oxigraph can produce today.
* `ErrorKind::Transaction` can still appear with a second **process** writing the same database.
  Retry loop: up to `commit_retry_attempts` (default 8), exponential jittered backoff starting at
  1 ms, then `StorageError::Other("transaction conflict after N attempts")`.
* `ErrorKind::Closed(..)` (the database was shut down or fenced by a newer writer — confirm the
  `CloseReason` variant at pin time) is *not* retried: map to
  `StorageError::Other("this store was fenced or closed by another writer")`. See **D9**.
* Error mapping (`slatedb_bridge.rs`):

| `ErrorKind` | `StorageError` |
|---|---|
| `Data` | `StorageError::Corruption(CorruptionError::new(e))` |
| `Unavailable` | `StorageError::Io(io::Error::new(ErrorKind::Other, e))` |
| `Closed`, `Invalid`, `Internal`, `Transaction` (after retries) | `StorageError::Other(Box::new(e))` |

Because `StorageError` is `#[non_exhaustive]`, a dedicated `Conflict` variant can be added later
without breaking anyone; it is deliberately deferred so the backend does not leak SlateDB concepts
into the public error surface.

### D7 — `clear*` semantics: record the range, expand at commit

`StorageTransaction::clear_default_graph` / `clear_all_named_graphs` / `remove_all_named_graphs` /
`clear` are infallible and O(1) today (`remove_range` per CF). SlateDB has no range delete, so:

* the mutator records `ClearRange { ns, start: [], end: None }` (whole namespace) plus, for
  `remove_all_named_graphs`, `graphs` — still O(1), still infallible;
* `commit()` opens a Snapshot-isolation transaction, scans each recorded range under
  `BATCH_SIZE = 100_000` at a time, issues deletes, then commits.

Consequences that must be written down and accepted explicitly:

1. `Store::clear()` on a 10⁹-key database becomes a full scan. RocksDB does it with nine
   `remove_range` calls in one write batch. **This is a real regression** and is called out in the
   docs and in `Store::clear`'s doc comment for the SlateDB backend.
2. Process RSS stays flat (`BATCH_SIZE` keys are decoded at a time), but the `DbTransaction`'s
   buffered write set does grow with the cleared range, because every deleted key must be part of
   the single atomic commit. A guard aborts with a clear error when the expanded delete count
   exceeds `max_clear_deletes` (default 50 000 000) rather than OOM-ing.
3. Visibility is preserved: the expansion and the deletes happen inside one transaction, so no
   reader ever observes a half-cleared store.

The cheaper alternative — an epoch/format byte in the key layout so `clear()` flips one metadata
key — is **rejected for v1** (see **D10**) but is the escape hatch if `clear()` proves hot.

### D8 — Bulk loader: batched `WriteBatch`, explicit atomicity budget

RocksDB's loader sorts a batch in memory, writes SST files in parallel and ingests them atomically
(`insert_stt_files`). There is no SlateDB equivalent (no external SST ingest). Replacement:

* **Encoding is reused.** `FileBulkLoader::encode` / `build_sst_for_keys`'s sort+dedup logic moves
  to a shared helper that returns sorted `Vec<Vec<u8>>` per namespace instead of SST paths.
  Parallelism stays: `load_batch(quads, max_num_threads)` spawns encoder threads exactly as today.
* **Write path:**
  * `without_atomicity()` → one `db.write(WriteBatch)` per encoded batch with
    `await_durable = false`, progress hook fired as today; batches are visible as they land.
  * default (atomic) → all batches stream into a single `DbTransaction`, committed once at
    `commit()`.
    Because that transaction holds every staged key/value in memory, the loader enforces
    `bulk_load_max_staged_bytes` (default 2 GiB): exceeding it returns
    `StorageError::Other("atomic bulk load exceeds N bytes; call BulkLoader::without_atomicity()")`.
    This keeps the documented "either the full operation succeeds, or nothing is written"
    contract honest instead of silently breaking it.
* **Expectation to set in docs:** no SST ingestion ⇒ bulk load is PUT- and compaction-bound and
  will be materially slower than RocksDB. Phase 3 measures it; Phase 5 reports the ratio. Do not
  promise parity.

### D9 — Single-writer semantics change from lock to fencing

RocksDB holds a `LOCK` file: a second `Store::open` fails immediately (`store.rs` doc:
"Only one read-write Store can exist at the same time"). SlateDB instead gives the newcomer a
higher writer epoch and the incumbent starts failing. Therefore:

* Keep the doc promise, but restate it: "opening a second read-write `Store` on the same database
  **invalidates the first one**".
* Add `test_second_writer_fences_the_first` (spawn a child process, then open a second store in
  the parent, then assert the first store's next commit fails with the fenced message).
* Read-only access uses `slatedb::DbReader` (construct it via the pinned version's
  `DbReader::open`; verify signature at pin time). Readers see L0+ only, not the writer's WAL —
  which is acceptable, since `Store::open_read_only` already documents concurrent writing as UB.

### D10 — Key layout is frozen in Phase 1: no epoch prefix

Adding an epoch byte later would be a data-format migration. Decision: **`[ns] || key`, no epoch**,
because it keeps `validate()`, migrations and the differential test trivial. Accepted costs:
`Store::clear()` is O(n) (D7), and atomic bulk load is memory-bounded (D8). If either becomes a
release blocker, revisit *before* any production data exists, at the price of a format version bump.

### D11 — Backups are logical copies in v1

`Store::backup(target)` currently creates a RocksDB checkpoint (hard links when possible).
SlateDB's analogue is a checkpoint in object storage (`Db::create_checkpoint`,
`CheckpointScope`), but `Store::open(backup_dir)` must keep working for the existing test
(`tests/store.rs::test_backup`, which opens the backup and re-backs it up). Therefore:

* **v1:** stream every key from one `DbSnapshot` in bounded batches into a *new* SlateDB database
  anchored at `target/` via `object_store::local::LocalFileSystem`. Correct, testable, O(n).
* **v2 (Phase 5, optional):** checkpoint pointer file + resolution in `SlateDbStorage::open`.
  Only if backup cost matters.

`Storage::backup` on a memory store must keep returning the existing
"It is not possible to backup an in-memory database" error.

### D12 — `compact()` / `optimize()` is a no-op in v1

SlateDB compaction runs in a compactor component (`Settings::compactor_options`), not via the `Db`
handle. `Storage::compact()` returns `Ok(())` and logs at debug level; `Store::optimize()`
documents "no effect with the SlateDB backend". Do not fake it. If the pinned version exposes a
one-shot compactor runner, wire it in Phase 5 behind the same signature.

---

## 4. Phase 1 — On-disk gate refactor, keyspaces, runtime bridge, raw KV

**Goal.** SlateDB is a working key/value engine inside `oxigraph` and nothing else in the crate
knows about it. No RDF semantics yet.

### 4.1 Cargo and feature work

```toml
# lib/oxigraph/Cargo.toml
[features]
default = ["rocksdb"]
rocksdb = ["oxrocksdb-sys"]
slatedb = ["dep:slatedb", "dep:tokio"]

[target.'cfg(not(target_family = "wasm"))'.dependencies]
slatedb = { version = "0.18", optional = true }
tokio = { workspace = true, optional = true, features = ["rt-multi-thread", "sync", "time"] }
```

* `tokio.workspace = true` is already declared (`Cargo.toml:80`, `1.29`); no new workspace entry.
* Do **not** add `object_store` as a separate dependency: `slatedb` re-exports it
  (`slatedb::object_store`). A second, differently-versioned `object_store` is the most likely
  source of "expected `dyn ObjectStore`, found `dyn ObjectStore`" pain. For S3/RustFS, reach the
  builder through the re-export (`slatedb::object_store::aws::AmazonS3Builder`).
* Both features must be *co-enableable*: Phase 4's migration tool needs a RocksDB reader and a
  SlateDB writer in one process.
* Gate SlateDB on `not(target_family = "wasm")` — `tokio` + `object_store` are not wasm targets
  here, and `js` builds with `default-features = false`.

### 4.2 The gate refactor (do this first, in its own commit)

Replace, everywhere on-disk behaviour is meant (not RocksDB-specific behaviour):

```
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
```
with
```
#[cfg(all(not(target_family = "wasm"), any(feature = "rocksdb", feature = "slatedb")))]
```

Sites: 113 in `lib/oxigraph` (69 `storage/mod.rs`, 11 `store.rs`, 1 `storage/error.rs`,
1 `storage/binary_encoder.rs`, 28 `tests/store.rs`, 3 `sparql/local_service.rs` +
`tests/local_service.rs`), plus `benches/*` and `cli`/`python`/`fuzz` feature wiring.

For benches, Cargo's `required-features` is an **AND** list and cannot express "either backend".
Do not invent a `persistent` feature that quietly maps to one backend. Instead split the targets:
`benches/store.rs` keeps `required-features = ["rocksdb"]` and a new `benches/store_slatedb.rs`
carries `required-features = ["slatedb"]`; shared benchmark bodies move into a `benches/common`
module included by both.

Two rules to keep the refactor reviewable:

1. A function that is *only* meaningful for RocksDB (e.g.
   `StorageOptions::{max_open_files, fd_reserve}`, `StoreOptions`, `Storage::backup`'s checkpoint
   path) keeps `feature = "rocksdb"` and gains a SlateDB sibling.
2. Anything reached through `storage/mod.rs`'s dispatch is on-disk-generic and uses the `any(...)`
   gate.

Also update `storage/binary_encoder.rs`'s `decode_term` gate — it is used by the graph iterator,
not by RocksDB.

### 4.3 Namespaces

`slatedb_keyspace.rs`:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum Ns {
    Meta = 0x00, Id2Str = 0x01, Spog = 0x02, Posg = 0x03, Ospg = 0x04,
    Gspo = 0x05, Gpos = 0x06, Gosp = 0x07,
    Dspo = 0x08, Dpos = 0x09, Dosp = 0x0A, Graphs = 0x0B,
}

pub(crate) fn prefixed(ns: Ns, key: &[u8]) -> Vec<u8>;
pub(crate) fn prefixed_into(buf: &mut Vec<u8>, ns: Ns, key: &[u8]);
/// Reuse the algorithm from `rocksdb_wrapper::Reader::scan_prefix`.
pub(crate) fn prefix_bounds(ns: Ns, prefix: &[u8]) -> (Vec<u8>, Option<Vec<u8>>);
```

Unit tests: `prefixed` ordering equals ordering of unprefixed keys; `prefix_bounds` yields
`None` when every byte is `0xFF`; namespace isolation (a key in `spog` never appears in a `posg`
scan, including for adversarial prefixes).

### 4.4 Runtime bridge

```rust
pub(crate) struct RuntimeBridge {
    handle: tokio::runtime::Handle,
    _runtime: tokio::runtime::Runtime, // owned; multi-thread, worker_threads = 2..4, enable_time
}

impl RuntimeBridge {
    /// Runs `fut` on the private runtime, blocks the *calling* thread on a std channel.
    /// `Runtime::block_on` is never called on the caller's thread, so this is safe even
    /// when Oxigraph is used from inside an application's own Tokio runtime.
    pub(crate) fn run<F>(&self, fut: F) -> Result<F::Output, StorageError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.handle.spawn(async move { let _ = tx.send(fut.await); });
        rx.recv()
            .map_err(|_| StorageError::Other("SlateDB worker task panicked".into()))
    }
}
```

The `Err` arm covers a panicking or aborted task; every call site must propagate it rather than
`unwrap`. `Handle::spawn` is callable from any thread, including a foreign runtime — this is the
property that makes the design safe.

Forbidden in review: `Runtime::block_on`, `Handle::block_on`, `futures::executor::block_on`, and
`tokio::sync::mpsc::Receiver::blocking_recv` (it panics inside a runtime). Only std channels cross
the async/sync boundary.

### 4.5 `SlateDbStorage` open path

```rust
pub struct SlateDbStorage {
    db: Arc<slatedb::Db>,
    bridge: Arc<RuntimeBridge>,
    commit_lock: Arc<Mutex<()>>,
    durability: SlateDbDurability,
    settings: SlateDbRuntimeSettings, // read prefetch bounds, retries, clear budget
}

impl SlateDbStorage {
    pub fn open_at(path: &Path) -> Result<Self, StorageError>;   // object store rooted at `path`
    pub fn open_read_only_at(path: &Path) -> Result<Self, StorageError>; // DbReader
    fn read_meta(&self) -> Result<StoreMeta, StorageError>;      // magic + format version
    pub fn snapshot(&self) -> SlateDbStorageReader<'static>;
    pub fn close(&self) -> Result<(), StorageError>;              // flush + close, best effort
}
```

Meta namespace contents:

| Key | Value |
|---|---|
| `magic` | `b"oxigraph-slatedb"` |
| `format_version` | `u32` BE = 1 |
| `oxversion` | `u64` BE, same meaning as RocksDB's `oxversion` (`LATEST_STORAGE_VERSION = 2`) |
| `engine` | `b"slatedb"` (rejects a RocksDB directory, and vice versa) |

`open` writes these on first creation, validates them on reopen, and returns
`CorruptionError::msg` on mismatch — the SlateDB analogue of `ensure_version` +
`from_missing_column_family_name`.

### 4.6 Phase 1 exit criteria

- [ ] `cargo check -p oxigraph --no-default-features --features slatedb` and
      `--features rocksdb` and both together all compile.
- [ ] `cargo test -p oxigraph --features rocksdb` is unchanged and green (proves the gate refactor
      is behaviour-preserving).
- [ ] New unit tests: namespace ordering/isolation, `prefix_bounds`, raw put/get/scan round-trip,
      reopen persistence with `LocalFileSystem`, meta-version rejection, `test_send_sync` for
      `SlateDbStorage` and `SlateDbStorageReader<'static>`.
- [ ] A `Bridge` test that calls `run()` from inside a `#[tokio::main]` thread and from a plain
      thread — both must return without panicking (this is the anti-deadlock regression test).
- [ ] No `unsafe` added (`unsafe_code = "warn"` in `[workspace.lints]`).

---

## 5. Phase 2 — RDF semantics: readers, sync iterators, both transaction types

**Goal.** This is the architectural proof. After Phase 2, `Store` works end-to-end on SlateDB and
the full integration suite passes with `feature = "slatedb"`. If any of the semantics in §2 cannot
be reproduced here, stop and resolve it before Phase 3 spends effort on throughput.

### 5.1 Reader

Port `RocksDbStorageReader` method-for-method. The 16-arm `quads_for_pattern` decision tree is
copied verbatim; only the scan constructor changes:

```rust
fn inner_quads(&self, ns: Ns, prefix: &[u8], encoding: QuadEncoding) -> SlateDbDecodingQuadIterator<'a>
```

* `len()` / `is_empty()` — count `gspo`+`dspo` via scans (as today).
* `named_graphs()` / `contains_named_graph()` — `Ns::Graphs` + `decode_term`.
* `contains_str()` / `get_str()` — `Ns::Id2Str`, `key.to_be_bytes()`, value is UTF-8
  (`str::from_utf8` → `CorruptionError::new`, exactly as today).
* `contains()` — `write_spo_quad` for default graph, `write_gspo_quad` otherwise.
* `validate()` — same invariants as `rocksdb.rs:740`, but read through the **reader**
  instead of `self.storage.db.contains_key` (RocksDB reaches around its own snapshot there; the
  SlateDB version reads a consistent view). Deliberate deviation, noted in a comment.

### 5.2 Sync iterator over async scans

```rust
pub struct SlateDbDecodingQuadIterator<'a> {
    scan: SlateDbScan<'a>,   // buffered pull
    encoding: QuadEncoding,
}

struct SlateDbScan<'a> {
    bridge: Arc<RuntimeBridge>,
    iter: Arc<tokio::sync::Mutex<slatedb::DbIterator>>, // DbIterator: Send
    buffer: VecDeque<slatedb::KeyValue>,
    batch: usize, // default 256
}

impl SlateDbScan<'_> {
    fn next_key(&mut self) -> Option<Result<Vec<u8>, StorageError>> {
        if self.buffer.is_empty() {
            // one bridge round-trip per `batch` keys — not per key
            let pulled = self.bridge.run(async { /* lock, drain up to batch, unlock */ });
            ...
        }
        ...
    }
}
```

Design notes:

* **Batched pull, no background task.** Each `bridge.run` refills up to `batch` entries. Cost:
  one task spawn + channel round trip per 256 keys. A channel-based prefetcher with a dedicated
  producer task is the Phase 3 optimization *only if* Phase 3 measurements show scan overhead
  above ~5 % of query time.
* No `'a` borrow of the reader: the snapshot is `Arc<DbSnapshot>` (or `&'a DbTransaction` for the
  readable-transaction reader), so `DecodingQuadIterator<'static>` works exactly like RocksDB's.
* Ordering and error semantics stay per-key: on scan error, yield `Some(Err(..))` once, then stop
  (matches `Iter::status()` behaviour).

### 5.3 Transactions

```rust
pub struct SlateDbStorageTransaction<'a> {
    ops: Vec<SlateDbOp>,          // D3
    storage: &'a SlateDbStorage,
}
pub struct SlateDbStorageReadableTransaction<'a> {
    transaction: slatedb::DbTransaction,   // IsolationLevel::Snapshot
    storage: &'a SlateDbStorage,
}
```

`SlateDbStorageReadableTransaction` methods are a near-literal port of
`RocksDbStorageReadableTransaction`: `clear_graph` / `clear_all_named_graphs` / `remove_all_*`
loop with `BATCH_SIZE = 100_000`, calling `self.reader().quads_for_graph(..)` and
`self.remove_encoded(..)`. `DbTransaction` gives read-your-writes for free, so this code is
mostly unchanged — which is exactly the property to prove with tests.

`commit()`:

```rust
// write transaction
let _guard = self.storage.commit_lock.lock();
if ops.iter().any(|op| matches!(op, ClearRange { .. })) {
    let txn = db.begin(IsolationLevel::Snapshot).await?;   // expand + apply + commit
} else {
    let mut batch = WriteBatch::new();
    // replay ops
    let handle = db.write(batch).await?;
    if durability >= Wal { handle.await_durable().await?; }
}
```

Conflict retry (**D6**) wraps only the `begin`/`commit` section, never the `WriteBatch` path.

### 5.4 Wiring

Add to every enum in `storage/mod.rs`:

```rust
#[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
StorageKind::SlateDb(SlateDbStorage),
```

…and the matching `match` arms in `Storage::{snapshot, start_transaction,
start_readable_transaction, flush, compact, backup, bulk_loader}`,
`StorageReaderKind`, `StorageTransactionKind`, `StorageReadableTransactionKind`,
`StorageBulkLoaderKind`, `DecodingQuadIteratorKind`, `DecodingGraphIteratorKind`.
Also add the temporary constructor used by tests:

```rust
#[cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
impl Store {
    pub fn open_with_slate_db(
        path: impl AsRef<Path>,
        options: SlateDbOptions, // Phase 2: only `SlateDbOptions::at_directory(..)` exists
    ) -> Result<Self, StorageError>;
}
```

The signature is final from Phase 2; Phase 4 only adds constructors (URL/S3) and fields to
`SlateDbOptions`, so the public API never churns inside this work.

### 5.5 Phase 2 exit criteria

- [ ] `test_send_sync` ported: `SlateDbStorage`, `SlateDbStorageReader<'static>`,
      `SlateDbStorageReadableTransaction<'_>`, `SlateDbStorageBulkLoader<'_>` are `Send + Sync`.
- [ ] `rocksdb.rs::tests::test_transaction` ported verbatim to SlateDB and green: graph insert,
      two-quad insert, snapshot-before/after assertions, `remove_named_graph`, rollback on `drop`,
      `clear`, `is_empty`, `validate` at every step.
- [ ] `tests/store_slatedb.rs`: the on-disk subset of `tests/store.rs` (load, load_dataset,
      bulk_load_graph, dump_graph, dump_dataset, snapshot_isolation_iterator,
      read_your_own_write_transaction, read_only, clear_graph, remove_named_graph) green against
      `LocalFileSystem`.
- [ ] All 16 `quads_for_pattern` arms covered by a test that asserts *index choice and result set*
      against the memory backend (differential, in-process, no fixture files).
- [ ] `validate()` passes after every mutating test.
- [ ] A SPARQL query smoke test (`SELECT * WHERE { ?s ?p ?o }`, `GRAPH ?g { … }`, `COUNT`) returns
      identical results on memory / rocksdb / slatedb for the same dataset.
- [ ] No `unwrap`/`expect` added on the async path outside tests (`unwrap_in_result`,
      `expect_used`, `panic` are warn-level workspace lints).

---

## 6. Phase 3 — Bulk load, deletion at scale, durability, performance

**Goal.** Realistic RDF workloads: 10⁸-scale loads, graph deletion, measured behaviour against
RocksDB.

### 6.1 Bulk loader (D8)

* Shared encoder: extract the sort/dedup core of `FileBulkLoader::{encode, build_sst_for_keys}`
  into `encode_index_batch(quads) -> BatchedKeys` (per-namespace sorted, deduplicated key lists +
  `id2str` map). RocksDB keeps building SSTs from it; SlateDB builds `WriteBatch`es.
* Implement `SlateDbStorageBulkLoader::{on_progress, without_atomicity, load_batch, commit}` with
  the same `threads: VecDeque<JoinHandle<..>>` + `done_counter: Arc<Mutex<u64>>` +
  `CancellationToken` structure, so `store.rs::BulkLoader` needs no change.
* `commit()` = staged-transaction commit (atomic) or `db.flush()` (non-atomic).
* Progress hook granularity stays `DEFAULT_BULK_LOAD_BATCH_SIZE`-based
  (`on_possible_progress`).
* Tests: `test_bulk_load_graph_on_disk`, `test_bulk_load_rollback`,
  `test_bulk_load_on_existing_delete_overrides_the_delete`,
  `test_bulk_load_empty` ported; plus `test_atomic_bulk_load_exceeds_budget_errors` and
  `test_non_atomic_bulk_load_is_visible_early`.

### 6.2 Deletion at scale (D7)

* Single-quad and multi-quad removal: already per-key, no change.
* `clear_graph` on a named graph: unchanged readable-transaction loop; verify with a 1 M-quad
  graph that memory stays flat.
* `clear()` / `clear_default_graph()` / `remove_all_named_graphs()`: range expansion at commit,
  with the `max_clear_deletes` guard.
* Test: delete a graph while a reader snapshot from before the commit is alive; the reader must
  still see the graph, the new snapshot must not (visibility safety).

### 6.3 Durability, flush, backup

* `SlateDbDurability` plumbed from `SlateDbOptions` (Phase 4) with `Wal` default; Phase 3 implements
  all three plus `Store::flush()` → `Db::flush()`.
* `backup()` per **D11** (logical copy). Port `test_backup`, `test_bad_backup`
  (target exists → error), `test_backup_on_in_memory` (unchanged error).
* `compact()` / `optimize()` per **D12**.

### 6.4 Performance work (measurement-driven, no promises)

Tuning surface exposed through `SlateDbOptions`:

| Knob | SlateDB field | Starting point |
|---|---|---|
| `flush_interval` | `Settings::flush_interval` | 100 ms (`Wal` default) / 1 s (`Process`) |
| `l0_sst_size_bytes` | `Settings::l0_sst_size_bytes` | 64 MiB |
| `max_unflushed_bytes` | `Settings::max_unflushed_bytes` | 256 MiB |
| `compression_codec` | `Settings::compression_codec` | LZ4-equivalent of the RocksDB default |
| local block/SST cache | `slatedb::db_cache` (foyer/`CachedObjectStore` replacement) | Phase 3 spike; `object_store_cache_options` is deprecated, do not use it |
| scan prefetch | `ScanOptions::{read_ahead_bytes, max_fetch_tasks, cache_blocks}` | read-ahead 1 MiB, 4 fetch tasks |
| iterator batch | our `SlateDbScan::batch` | 256 |

Benchmarks to add in the split targets from §4.2 (already criterion-based via
`codspeed-criterion-compat`), each run for both persistent backends:

1. quad insert (single), 2. `extend` of 10 k quads, 3. bulk load of 1 M quads (quads/s + object
   PUTs), 4. point lookup by subject, 5. scan by predicate, 6. BSBM-style SPARQL query mix,
   7. delete a 100 k-quad graph, 8. cold reopen time.

Reported in `planning/slatedb/performance.md`, including object-store request counts. **No claim
of a speedup without a number in that table.** The post-Phase-5 follow-up work on the measured
bottlenecks is planned in `optimization_plan1.md`.

### 6.5 Phase 3 exit criteria

- [x] 1 M-quad load completes with bounded RSS (document the measured peak).
      Done: 11.8 s and 1 441 MiB of peak RSS through `without_atomicity`, on a machine with 31 GB
      (`measure_million_quad_bulk_load_peak_memory`, table in `performance.md`).
- [x] `clear*` keeps RSS flat (≈ `BATCH_SIZE` decoded keys) and refuses work above
      `max_clear_deletes` instead of OOM-ing.
      Measured, and the memory is **not** flat: a clear is one transaction, so its deletes are
      staged until the commit at about a kilobyte per key (1 M quads of a named graph: 47.8 s and
      5.3 GiB above the memory used before the clear). What holds is the guard: `max_clear_deletes` counts keys, its default of 2 000 000
      keeps one clear under about two gigabytes, and it fails before writing anything. RocksDB
      stages its deletes too (1.4 KiB per quad against 5.4), so flat memory was never an option for
      this API; the criterion is met as a bounded, refused-before-OOM clear. Numbers in
      `performance.md`.
- [x] Benchmark table populated for rocksdb vs slatedb (LocalFileSystem) with absolute numbers.
      Done, in `performance.md`, with the object store request counts of a load.
- [x] Known bottlenecks written down (object PUT cost per commit, no SST ingest, scan per-batch
      round trip).
      Done: eight bottlenecks in `performance.md`, plus the atomic load scaling and the clear
      memory profile.

---

## 7. Phase 4 — Configuration, migration, S3/RustFS deployment

**Goal.** Deployable alternative backend plus a one-way RocksDB → SlateDB migration.

### 7.1 Public options

```rust
// storage/slatedb_options.rs
pub struct SlateDbOptions {
    object_store: SlateDbObjectStore, // Path(PathBuf) | Url(String) [s3://…, file://…]
    prefix: String,                   // key prefix inside the bucket
    durability: SlateDbDurability,    // Process | Wal | Flush
    settings: slatedb::config::Settings,
    read_only: bool,
    commit_retry_attempts: u32,
    max_clear_deletes: u64,
    bulk_load_max_staged_bytes: u64,
}

impl SlateDbOptions {
    pub fn at_directory(path: impl Into<PathBuf>) -> Self; // Phase 2
    pub fn at_url(url: impl Into<String>) -> Self;         // Phase 4: s3://…, file://…
    pub fn with_durability(self, policy: SlateDbDurability) -> Self;
    pub fn with_settings(self, settings: slatedb::config::Settings) -> Self;
}

impl Store {
    pub fn open_with_slate_db(path: impl AsRef<Path>, options: SlateDbOptions) -> Result<Self, StorageError>;
    pub fn open_slate_db_read_only(path: impl AsRef<Path>, options: SlateDbOptions) -> Result<Self, StorageError>;
}
```

Rules:

* `StoreOptions` (RocksDB) is untouched — no breaking change for existing users.
* `Store::open(path)` keeps meaning "default backend": `rocksdb` if that feature is on, else
  `slatedb` if it is on, else a clear `StorageError` ("this build has no on-disk backend").
* Credentials never go through `SlateDbOptions`: use the object-store env conventions
  (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT`, `AWS_ALLOW_HTTP`), i.e. build
  `AmazonS3Builder::from_env()`. Document that RustFS speaks S3 and needs
  `AWS_ALLOW_HTTP=true` + explicit endpoint.
* `Settings` is `slatedb`'s own type and is re-exported by us, not mirrored — no risk of drift, and
  `Settings::from_file` / `from_env_with_default` come for free.

### 7.2 CLI

`cli/src/cli.rs` + `main.rs`: add a shared object-store option group and route it through the new
constructors. Minimum viable surface:

```
oxigraph serve        --object-store s3://bucket/prefix --slatedb-config slatedb.toml
oxigraph serve-read-only --object-store …
oxigraph load         --object-store … --slatedb-config …
oxigraph backup       --object-store … --destination …
oxigraph migrate --from rocksdb --to slatedb --source DIR --object-store s3://…
```

`--object-store` presence selects SlateDB; absence keeps RocksDB. No behavioural change for
existing invocations.

### 7.3 Migration tool

```
RocksDB dir ──► RocksDbRawExporter ──► (ns, key, value) stream ──► WriteBatch(1M) ──► SlateDB
                                        │
                                        └── validate: per-namespace counts equal,
                                            oxversion copied, target validate() clean
```

* Add `pub(crate) fn export_raw(&self, sink: impl FnMut(Ns, &[u8], &[u8])) -> Result<(), StorageError>`
  to `rocksdb.rs` (feature-gated), plus `count_per_namespace()`.
* Copy `oxversion` verbatim so `rdf-12` / triple-term databases keep their version semantics.
* Bind the source read-only (`Storage::open_read_only`) and refuse to run if the directory has a
  `LOCK` held by a live writer.
* Resumability: write a `migration_state` key in the target meta namespace
  (`{source_fingerprint, last_ns, last_key}`) so an interrupted migration restarts from a key
  rather than from zero; a `--restart` flag clears it.
* Verification: after import, open the target with `Store`, run `validate()`, compare
  `dump_to_writer(NQuads)` against the source dump (sorted), and compare `len()`.
* Tests: migrate a fixture (`tests/rocksdb_bc_data` style), assert byte-identical key/value pairs
  for a sample of namespaces, assert `validate()` and equal N-Quads dumps.

### 7.4 S3 / RustFS

* S3 path through `slatedb::object_store::aws::AmazonS3Builder` with `with_url`/`with_bucket_name`
  + `with_region` + endpoint override.
* RustFS is exercised as a *deployment* concern, not in the storage abstraction: a `docs/` recipe
  plus an optional `#[ignore]`d integration test that runs when `OXIGRAPH_S3_TEST_URL` is set.
* Object-store retry policy: `Settings::object_store_max_retries` (default: transient errors retry
  forever — set an explicit bound in the deployment recipe).
* Do not implement replication in Oxigraph. Document that bucket versioning + cross-region
  replication are the durability story, and that an async replica is **not** a consistent
  snapshot; `backup()`/checkpoints are.

### 7.5 Phase 4 exit criteria

- [x] `Store::open_with_slate_db(LocalFileSystem)` and `(s3://…)` both work; read-only open works.
      Directory/`file://` are covered by `tests/store_slatedb.rs` (`test_*_on_disk`,
      `file_url_object_store`, `read_only_store_reads_without_fencing_the_writer`). `s3://` is
      implemented (`AmazonS3Builder::from_env`, bucket + prefix) and exercised only by the
      `#[ignore]`d `s3_object_store_round_trip`, which needs a real bucket; it was **not** run here.
- [x] A migrated RocksDB store opens with Oxigraph and answers SPARQL identically to the source.
      `symbolic_name_to_term`-level equality is asserted by re-reading both stores through
      `Store::iter()`, term for term and in order (`verify`), plus `validate()` and `len()`; the
      migration test builds its source with `Store::load_from_reader`, so the comparison covers
      every namespace. Not compared through SPARQL query *results*, only through store contents.
- [x] Migration is resumable (`resumes_an_interrupted_migration`) and refuses to run against a live
      writer (`check_no_live_writer`, POSIX `fcntl` read lock on `LOCK`; unix only, non-unix is a
      no-op). The refusal has no automated test: POSIX record locks are per-process, so an
      in-process test cannot observe the lock; it was verified by hand.
- [x] CLI flags documented (`docs/slatedb.md`, `--object-store` / `--slatedb-config` in
      `oxigraph <command> --help`); existing CLI invocations unchanged — see §7.6.
- [x] Python: **not** in scope for v1 — record as a follow-up (needs a pyo3 options object, and
      `python/Cargo.toml` does not enable `oxigraph/slatedb`).

### 7.6 Phase 4 deviations from this plan

Where the implementation had to differ from the text above, and why:

* **Location lives in the options**, not in a separate `path` argument:
  `SlateDbOptions::at_path`/`at_url` set the new `SlateDbObjectStore::{Directory, Url}` enum, and
  `path()`/`url()` read it back. This keeps the Phase‑2 `Store::open_with_slate_db(options)`
  signature the plan's §5 already froze.
* **No `--from`/`--to` on `migrate`**: only one direction exists (RocksDB → SlateDB), so the command
  is `oxigraph migrate --source DIR --object-store … [--slatedb-config …] [--restart]`. A pair of
  flags that can only ever hold one value would be noise.
* **CLI flags only where they can act**: `serve`, `serve-read-only`, `load`, `backup`, `migrate`.
  `dump`, `query`, `update` and `optimize` still open the RocksDB path positional argument; adding
  `--object-store` to them is a follow-up, not a silent half-migration.
* **S3 client from the environment**: `AmazonS3Builder::from_env().with_bucket_name(..).with_prefix(..)`
  rather than `object_store::parse_url`, because `parse_url` ignores `AWS_*` environment
  credentials. `AWS_ENDPOINT`, `AWS_ALLOW_HTTP`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
  `AWS_REGION` therefore behave as in the rest of the object_store ecosystem.
* **Read-only opens never fence a writer**: `DbReader` in `FollowLatest` mode, so a replica can read
  a live database. Mutations fail with "this store was opened read-only, it cannot be modified".
* **Retries are per-statement, not per-transaction**: write batches and `clear` replay from their
  operation list under a process-wide commit lock (D6); a readable `Store` transaction commits once
  and reports a conflict instead of replaying, because SlateDB's `commit` consumes the transaction
  and its reads cannot be replayed.
* **Verification by pairwise iteration** instead of dumping both stores to sorted N-Quads files:
  same guarantee, O(1) memory, no temporary files.
* **Migration batching is 10 000 entries** (the plan said 1M): a batch is held in memory twice
  (operations plus the encoded `WriteBatch`), and 10k keeps the peak bounded; the migration of the
  test fixture is a fraction of a second either way.

---

## 8. Phase 5 — Compatibility, fault injection, observability, release

**Goal.** Confidence that the backend does not lose or duplicate data, and honest documentation of
where it is slower.

### 8.1 Compatibility matrix

| Behaviour | Memory | RocksDB | SlateDB | Assertion |
|---|---|---|---|---|
| store integration tests (≈21 of 27 — 5 are RocksDB on-disk-format specific, 1 is `StoreOptions`-specific) | n/a | pass | pass | `tests/store_slatedb.rs` |
| read-your-own-writes transaction | pass | pass | pass | ported test |
| snapshot isolation iterator | pass | pass | pass | ported test |
| RDF-star / `rdf-12` triple terms | pass | pass | pass | `rocksdb_bc_rdf_star_data` analogue fixture |
| `validate()` | pass | pass | pass | after every mutating test |
| N-Quads dump equality | baseline | baseline | equal | differential test, 3 backends × same dataset |
| `backup()` round-trip | error | pass | pass | ported |
| second writer | n/a | `open` fails | first writer is fenced | new test (D9) |
| `clear()` cost | O(n) | O(1) | O(n) — documented | not asserted |

`tests/rocksdb_bc_data` and `tests/rocksdb_bc_rdf_star_data` are *RocksDB on-disk format*
fixtures; nothing equivalent is meaningful for a new backend, so instead pin a **SlateDB fixture**
(`tests/slatedb_fixture_data`, generated by the migration tool from the RocksDB fixture) and add
`test_slatedb_backward_compatibility` that opens it twice and asserts stable content. This is what
catches an accidental key-layout change later.

Where the rows are asserted:

| row | test |
|---|---|
| store integration tests, read-your-own-writes, snapshot isolation, `backup()` | `tests/store_slatedb.rs` (33 tests, the on-disk subset of `tests/store.rs`) |
| `validate()` | after every mutating SlateDB test |
| N-Quads dump equality | `test_dump_matches_the_other_backends` (memory, RocksDB and SlateDB, same dataset, sorted N-Quads) |
| RDF-star / `rdf-12` triple terms | `test_slatedb_rdf_star_backward_compatibility` on `tests/slatedb_bc_rdf_star_data` |
| second writer | `a_second_writer_fences_the_first` in `tests/slatedb_faults.rs` (two processes, D9) |
| `clear()` cost | documented and measured, not asserted: `test_clear_above_the_limit_errors` covers the guard |

How the two fixtures were generated, and why not exactly as written above:

* `tests/slatedb_fixture_data` is the migration of `tests/rocksdb_bc_data` (89 keys). The RocksDB
  fixture predates the current column families: `Store::open` creates them, but a read-only open —
  which is what migration binds — fails with `Column family not found: graphs`. So the fixture was
  copied, opened read-write once to bring it to the current format, and migrated from that copy.
* `tests/slatedb_bc_rdf_star_data` is the migration of a freshly written RocksDB store holding the
  same eight quads as `test_rdf_star_backward_compatibility`, because the committed
  `tests/rocksdb_bc_rdf_star_data` fails `Store::validate()` under the current code
  (`Quad in dspo and not in dpos`) — a pre-existing property of that fixture, unrelated to SlateDB.
  A fresh store is exactly what pins the current encoding, which is the point of the fixture.

### 8.2 Fault injection

Implemented in `src/storage/slatedb_faults.rs` (object store wrapper, unit tests) and
`tests/slatedb_faults.rs` (separate processes: SIGKILL, fencing, interrupted migration; unix only).
The process tests re-run the test binary with `--exact child_process` and a mode in an environment
variable, and SIGKILL it with `libc::kill`. State:

* `ErroringObjectStore`: a `dyn ObjectStore` wrapper that fails every `n`-th `put`/`get` with
  `object_store::Error::Generic`. Tests: open failure, mid-commit failure (no partial visibility),
  scan failure (iterator yields one `Err` then stops), recovery after the next successful call.
* Process kill: a child process (`std::process::Command` on the built `oxigraph` binary, or a
  `#[test]` helper binary) inserts N quads, then `SIGKILL`. Assertions:
  `Durability::Wal` keeps every committed quad; `Durability::Process` may lose the last
  `flush_interval` — assert only "no corruption, `validate()` clean", never an exact count.
* Restart with pending WAL: flush-free shutdown, reopen, `validate()`.
* Fencing: two writers in two processes, assert the first fails cleanly and the second keeps
  working (D9).
* Interrupted migration: kill mid-import, resume, `validate()` + dump equality.

Asserted by `a_failed_open_leaves_no_database`, `a_failed_commit_is_not_partially_visible_after_a_reopen`,
`a_commit_retries_a_transient_object_store_failure`, `a_failed_scan_yields_one_error_and_ends_the_iterator`,
`committed_writes_survive_a_sigkill`, `a_flush_free_exit_leaves_a_replayable_wal`,
`a_second_writer_fences_the_first` and `an_interrupted_migration_resumes_into_a_valid_store`.
Two limits, recorded rather than hidden: a failed commit with retries disabled leaves the writer
unusable until the database is reopened (SlateDB marks it unavailable), and the interrupted
migration test kills the child after 20 ms, so on a fast machine the copy may already be over when
the signal arrives — the resume assertions hold either way, but the interruption is not guaranteed.

### 8.3 Observability

SlateDB exposes metrics through its own `metrics`/`MetricLevel`; Oxigraph has **no** logging or
metrics facility today (no `log`/`tracing` dependency in `lib/oxigraph`), and adding one is a
crate-wide decision that this integration should not force. Scope decision:

* v1 adds **no** logging dependency and **no** public metrics API. Operational visibility comes
  from (a) the object store (request counts, bytes, WAL/SST objects) and (b) SlateDB's own
  `MetricLevel`, which `SlateDbOptions` can raise.
* `store.flush()` / `store.optimize()` / `store.backup()` remain the operational API (they already
  exist). `checkpoint()` and `storage_stats()` from `internal_interface1.md` stay follow-ups; if
  added, they must be backend-neutral and additive.

### 8.4 Documentation and release

* `lib/oxigraph/README.md` + `docs/`: "SlateDB backend" page — object-store setup, durability
  tradeoffs (D5), single-writer/fencing (D9), `clear()` cost (D7), bulk-load atomicity budget
  (D8), backup semantics (D11), no-op `optimize()` (D12), performance table from Phase 3.
* `CHANGELOG.md` entry under a new "Added" section.
* RocksDB stays the default feature and the default backend until the matrix above is green.

Phase 5 exit criteria:

- [x] Every row of §8.1 asserted, or documented as measured-only, with the tests named above.
- [x] Fault-injection suite green in CI on Linux: new `test_slatedb` job in
      `.github/workflows/tests.yml` runs `--lib`, `--test store_slatedb` and `--test slatedb_faults`
      with `--features slatedb`, and the clippy job gained a `--features slatedb` run. The S3 test
      stays `#[ignore]`d and the process tests are unix only; macOS and Windows do not run them.
- [x] Documentation merged: `docs/slatedb.md` (locations, S3/RustFS, read-only, behaviour changes,
      performance table, migration), `lib/oxigraph/README.md`, `CHANGELOG.md`, and the measured
      pages under `planning/slatedb/`.
- [x] No unresolved data-loss or transaction-consistency issue: commits are retried under D6,
      `Wal` durability is asserted across SIGKILL, a failed commit is not partially visible after a
      reopen, a second writer is fenced instead of both writing, and a migrated database is
      compared quad by quad. The known gap is the S3 path, implemented but not run against a real
      bucket (§7.5).

---

## 9. Dependency-ordered sequence

```
Phase 1  cfg refactor ── keyspaces ── bridge ── meta/version ── raw KV tests
            │
Phase 2  reader ── sync iterators ── write txn ── readable txn ── Storage wiring ── full suite
            │
            ├──► Phase 3  bulk loader ── clear expansion ── durability/backup ── benchmarks
            │                 │
            └──► Phase 4  options ── CLI ── migration ── S3/RustFS ──────────────┐
                                                                                 │
Phase 5  compatibility matrix ── fault injection ── observability ── docs/release ◄┘
```

Phase 3 and Phase 4 can proceed in parallel once Phase 2 is green; Phase 5 needs both.

---

## 10. Open questions to resolve before coding each phase

| # | Question | Resolve before | Owner action |
|---|---|---|---|
| Q1 | Does the pinned `slatedb` version support the workspace MSRV (`rust-version = 1.87`)? | Phase 1 | `cargo add --dry-run`, check `rust-version`; bump the workspace MSRV or pin an older `slatedb` |
| Q2 | Exact `DbBuilder` surface at pin time: `with_settings`, `with_block_cache`, `with_object_store_cache`, `with_compactor_builder` | Phase 1 | read `slatedb::DbBuilder` docs for the pinned version |
| Q3 | `DbReader::open` signature for read-only mode | Phase 1 | same |
| Q4 | Is `DbIterator` `Send` (needed for `Arc<Mutex<DbIterator>>`)? | Phase 2 | compile check; else move the iterator into a dedicated task + channel |
| Q5 | Does `WriteHandle::await_durable()` survive `SIGKILL` immediately after return (object PUT completed)? | Phase 3 | fault-injection test |
| Q6 | Is there any public range-delete API in the pinned version (0.18 `WriteBatch` has none)? | Phase 2 (D7) | `cargo doc -p slatedb --no-deps` |
| Q7 | One-shot compactor entry point for `optimize()` | Phase 3 (D12) | same |
| Q8 | Does `Db::snapshot()` pin manifest/SST versions such that long-lived query iterators delay GC unacceptably? **Resolved: no.** GC deletes by age (`wal` 60 s, `compacted`/`manifest` 24 h), not by snapshots. Measured with a 73 s-held iterator during writes: still complete. Limits and tuning are in `performance.md`. | Phase 5 | done |

---

## 11. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| `slatedb` is pre-1.0; API churn between 0.18 and later | build breakage | pin `=0.18.x`; all SlateDB contact confined to three files |
| Per-scan bridge overhead dominates cheap SPARQL scans | query latency | **closed**: `optimization_plan1.md` O1 drives the future on the calling thread, 17.5 ms → 3.04 ms for 1 000 point lookups and 3.68 ms → 121 µs for the 100-result scan |
| `clear()` O(n) regression | admin operations | guard + documentation; epoch layout is the escape hatch (D10), decided before data exists |
| Atomic bulk load memory | large imports | explicit budget + error (D8); `without_atomicity()` documented |
| No SST ingest ⇒ slower bulk load than RocksDB | user expectation | measure and publish; never claim parity |
| Second writer fences the first silently in an app that relied on the RocksDB lock | data access errors | D9 restated in docs + explicit test |
| Object-store PUT cost for `Wal` durability | cost surprise | object-store request counts in the benchmark table; deployment recipe documents cost knobs |
| MSRV conflict (Q1) | Phase 1 blocked | resolve first; the plan does not depend on a specific `slatedb` minor |

---

## 12. Definition of done

The integration is done when a user can

1. build `oxigraph` with `--features slatedb` (RocksDB still the default) and open a store rooted
   at a directory or an S3/RustFS prefix,
2. rely on every existing `Store`/`Transaction`/`BulkLoader`/SPARQL behaviour with identical
   results to the RocksDB backend,
3. rely on `Wal` durability across `SIGKILL` and on `Store::flush()` for an explicit barrier,
4. migrate an existing RocksDB store in one resumable command and prove the result with `validate()`
   plus an N-Quads dump comparison,
5. read a documented, measured performance table that says exactly where the SlateDB backend is
   slower and why,
6. and read one page of documentation stating the four behaviour changes (fencing, `clear()` cost,
   bulk-load atomicity budget, `optimize()` no-op) without surprises.
