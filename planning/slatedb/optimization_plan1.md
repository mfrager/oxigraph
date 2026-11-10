# SlateDB backend — optimization plan

Status: written after Phase 5 shipped (commit `ce7e4624`, on top of `f247a569`), against `slatedb`
0.18.1. It supersedes nothing; `implementation_plan2.md` remains authoritative for the design
decisions (D1–D12) this plan must not reopen.

Scope, as agreed with the request:

* only `lib/oxigraph/src/storage/{slatedb,slatedb_bridge}.rs` and the surrounding wiring change —
  **the `slatedb` crate itself is not patched, forked or replaced**;
* RocksDB stays the default backend, the RDF key encoding is untouched, no new public API unless an
  item explicitly says so;
* every claim below is a measurement on this tree, and every item ends with a number to reproduce
  or a decision to record. **No speedup is claimed without a number in `performance.md`.**

## 1. Where the time goes today

Re-run of the phase 3 benchmark on the Phase 5 tree, same machine as `performance.md` (i7-6700HQ,
31.4 GB, rustc 1.96.1, release + LTO, LocalFileSystem), criterion 10 samples,
`cargo bench -p oxigraph --features slatedb --bench store_slatedb -- --noplot`:

| workload | RocksDB | SlateDB | ratio |
| --- | --- | --- | --- |
| 100 × `Store::insert`, `Wal` | 1.34 ms | 10.12 s | 7 500× |
| 100 × `Store::insert`, `Process` | 1.34 ms | 2.91 ms | 2.2× |
| `Store::extend` of 10 000 quads | 45.3 ms | 126 ms | 2.8× |
| bulk load 100 000 quads, atomic | 399 ms | 969 ms | 2.4× |
| bulk load 100 000 quads, non atomic | 399 ms | 726 ms | 1.8× |
| 1 000 point lookups (`Store::contains`) | 1.81 ms | 17.2 ms | 9.5× |
| predicate scan, 100 of 1 000 quads | 348 µs | 3.58 ms | 10.3× |
| SPARQL mix, 3 queries over 1 000 quads | 3.91 ms | 35.9 ms | 9.2× |
| `clear_graph` of 100 000 quads | 2.21 s | 2.29 s | 1.04× |
| cold reopen of 100 000 quads | 44.2 ms | 453 ms | 10.2× |

Layer costs, measured with a temporary `#[cfg(test)]` probe that has since been reverted:

| step | cost | note |
| --- | --- | --- |
| one `RuntimeBridge::run` hop (spawn + `mpsc::sync_channel(1).recv()`) | 9.4–18 µs | dominates a point lookup |
| `Db::snapshot()` on the writer | ≈ 0 | not worth touching |
| one `DbSnapshot::get` | 2.1 µs | local FS, warm OS cache |
| `SlateDbStorage::get` end to end | 15.9–17.9 µs | hop + get + copy |
| one `StrLookup::get_str` (a hashed term) | 15.5 µs | same path |
| full scan of 1 000 quads | 32–36 ms | 2 dictionary lookups per quad |
| predicate scan, 100 results | 4.25–4.48 ms | |
| pattern scan, 1 result | 96–107 µs | one batch refill, one hop |
| `SlateDbStorageReader::len` of 1 000 quads | 1.4–2.0 ms | counts encoded quads, no lookups |

So the gap has three separate causes: two implementation costs and one policy choice.

1. **The sync → async bridge.** Spawning the future and waiting on a channel costs 9–18 µs per call,
   which is ~85 % of a 17 µs point lookup and 15.5 µs of every dictionary lookup.
2. **One round trip per hashed term.** Decoding a quad asks `id2str` once per string term, so a scan
   costs 32–43 µs per quad even though the raw key scan is 2–4 µs per key, and no term is ever
   remembered between quads or between queries.
3. **`Wal` durability quantizes commits** to `flush_interval` (100 ms): 101 / 21 / 6.2 / 2.05 ms per
   commit at 100 / 20 / 5 / 1 ms. This is a policy cost, not an implementation defect.

A prototype of item O1 (a temporary `Handle::block_on` branch in `RuntimeBridge::run`, reverted)
measured: hop 0.05 µs, `get` 2.7–3.3 µs, `get_str` 3.35 µs, 1 000 `contains` 3.0–3.5 ms (1.7× RocksDB),
full scan of 1 000 quads 6.2–8.4 ms, predicate scan of 100 results 0.91–1.35 ms, single-result scan
18.7–20.6 µs. Those are the numbers the items below are aiming at.

What is left after that is engine-bound and is **not** addressed here: bulk load (no SST ingestion),
cold open (manifest replay), compaction, the per-get 2.1 µs of the LSM itself. See §4.

## 2. Items

Ordered by dependency, not by size. Each item is one commit and can be reverted alone.

### O1 — Drive the future on the calling thread when it is not a runtime thread

**Why.** Every storage call goes through `RuntimeBridge::run`
(`lib/oxigraph/src/storage/slatedb_bridge.rs`), which always spawns on the private runtime and blocks
on a std channel: 9.4–18 µs of pure scheduling per call, against 2.1 µs of actual `db.get`. The
module docs currently forbid `Handle::block_on` outright (that ban is what keeps the bridge safe
inside an embedding application's runtime) and the anti-deadlock test
`runs_futures_from_inside_another_tokio_runtime` protects exactly that case.

**Change.** `RuntimeBridge::run` gets two paths:

```rust
pub(crate) fn run<F>(&self, future: F) -> Result<F::Output, StorageError>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    if Handle::try_current().is_ok() {
        // Already on a runtime thread: blocking here would panic, and `Handle::block_on` would
        // deadlock. Keep the spawn + channel path. This is also what nested calls take.
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        self.handle.spawn(async move {
            if sender.send(future.await).is_err() {
                // The caller gave up, so there is nobody left to hand the value to.
            }
        });
        return receiver
            .recv()
            .map_err(|_| StorageError::Other("the SlateDB worker task failed".into()));
    }
    // A plain thread: drive the future here. The runtime keeps running the background tasks
    // (WAL flush, compaction) on its workers.
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        self.handle.block_on(future)
    })) {
        Ok(output) => Ok(output),
        // `Handle::block_on` propagates the panic; the contract of this method, and of the
        // channel path above, is an error instead.
        Err(_) => Err(StorageError::Other("the SlateDB worker task failed".into())),
    }
}
```

Invariants to keep, in the order they can bite:

* **Never `block_on` on a runtime thread.** The `Handle::try_current()` guard is the only thing that
  makes this safe; it must stay the first statement, and `block_on` must stay inside the `Err` arm.
* **Panic contract.** `catch_unwind` with `AssertUnwindSafe` is required, otherwise
  `reports_a_task_that_panics` fails: `Handle::block_on` resumes the panic on the caller instead of
  returning an error. The future is dropped on unwind, exactly like a panicking spawned task, so no
  partial state escapes. `AssertUnwindSafe` is sound here because nothing observable outlives the
  call.
* **Nested calls** still take the channel path (inside `block_on`, `try_current()` is `Ok`), so the
  existing "2 workers, no nested spawn from a worker" reasoning is unchanged; add a comment saying
  so.
* **Timers and I/O** still work on the fast path: `Handle::block_on` only drives the given future,
  the runtime workers drive the driver, so `tokio::time::sleep` inside a storage future is fine.
* **Runtime shutdown.** `run` borrows the bridge, and `drop` is the only `shutdown_background`
  caller, so `block_on` is never called on a shut-down runtime; state it in the doc comment.
* Rewrite the module docs: the ban becomes "`Handle::block_on` only after `try_current()` proved the
  caller is not on a runtime thread"; the reason (embedding in a Tokio app) stays.

**Tests** (in the existing `#[cfg(test)] mod tests` of the bridge):

* `runs_futures_from_a_plain_thread` — now the fast-path test; keep it.
* `runs_futures_from_inside_another_tokio_runtime` — the fallback/anti-deadlock test; keep it.
* `reports_a_task_that_panics` — must pass through the new `catch_unwind` path unchanged.
* new `reports_a_task_that_panics_inside_another_tokio_runtime` — same error string through the
  channel path.
* new `runs_a_future_that_awaits_a_timer_from_a_plain_thread` — `tokio::time::sleep(1 ms)` then a
  value; this is the test that would catch a fast path without a live driver.
* optional `leaves_no_runtime_context_on_the_caller` — `Handle::try_current().is_err()` right after
  `run` on a plain thread. If this ever fails, every later call silently takes the slow path.

**Expected.** 1 000 `contains` 17.2 ms → ~3.2 ms; scan of 1 000 quads 33 ms → ~7 ms; predicate scan
3.58 ms → ~1.1 ms; single-result scan ~100 µs → ~19 µs; writes unchanged (`Wal` is the flush
interval, `Process` is ~2.9 ms per 100 inserts). Measured with the prototype, to be re-measured with
the committed version and written into `performance.md` in the same commit.

**Risk.** Low: the fallback is unchanged, and the whole storage suite (which runs on plain `#[test]`
threads for most tests, but also inside the Tokio-based tests) covers both paths.

### O2 — Remember `id2str` results for the life of the store

**Why.** `StrLookup for SlateDbStorageReader` (`lib/oxigraph/src/storage/slatedb.rs`) does one
`self.get(Ns::Id2Str, …)` per hashed term, i.e. 15.5 µs today and 3.35 µs after O1, per term, even
though a hash always maps to the same string. Predicates, classes, datatypes and language tags are
extremely repetitive across quads and across queries, and there is no cache at any layer.

**Change.** A private, bounded, content-addressed cache owned by `SlateDbStorage` and shared by
every reader it hands out:

```rust
/// Number of `id2str` entries kept per store. 16 384 entries is a few megabytes at most and
/// covers the repeated terms of any realistic query mix.
const ID2STR_CACHE_CAPACITY: usize = 16_384;

/// Same shape as the in-memory backend's dictionary (`storage/memory.rs`): `dashmap` keyed by
/// `StrHash` with `StrHashHasher`, so a hit is a shard read and a refcount bump.
struct Id2StrCache {
    entries: DashMap<StrHash, OxString, BuildHasherDefault<StrHashHasher>>,
}
```

* `SlateDbStorage` gains `id2str_cache: Arc<Id2StrCache>`, filled in its single `Self { … }` literal
  next to `handle`, `bridge` and `config`.
* `SlateDbStorage::snapshot()` and `SlateDbStorageReadableTransaction::reader()` clone the `Arc` into
  the `SlateDbStorageReader`, so reader clones and later queries share it (`SlateDbStorageReader`
  keeps `#[derive(Clone)]`). The readable transaction keeps its own clone of the `Arc`, taken in
  `start_readable_transaction`, so every reader it hands out shares one cache with the store.
* `get_str` becomes: hit → return the cached `OxString` (refcount bump, no allocation); miss →
  existing lookup, decode, insert, return. Do not cache misses or errors.
* Stay bounded without an LRU: on insert, if `len() >= ID2STR_CACHE_CAPACITY`, remove one
  arbitrary existing key (`entries.iter().next()`) before inserting. Random replacement, not FIFO
  or LRU — O(1), no second lock, no per-hit bookkeeping. The workload that overflows the cache is
  a distinct-term scan, where the eviction policy barely matters; if a measured hit rate says
  otherwise, an insertion-order queue can be added later behind the same type.

**Correctness.** `StrHash` is content-derived: the writer that stores an `id2str` entry under a hash
always stores the same string for it, so no invalidation is needed for inserts, deletes, `clear`,
rollback, flush, read-only stores or a readable transaction's own writes. A cached value is exactly
as correct as the store's own hash-is-identity assumption (already relied on everywhere). Cache only
successfully decoded values, so invalid UTF-8 keeps failing on every read like today.

**Memory.** Bounded by `ID2STR_CACHE_CAPACITY` entries: the `DashMap` slot plus the 16-byte hash and
the string payload per entry, so ~2–4 MiB for long IRIs. It is per `SlateDbStorage`, not per reader
or per query. Document the bound on the constant, not in the user docs.

**Tests** (unit tests on the cache, plus one integration test):

* `the_id2str_cache_returns_inserted_values`
* `the_id2str_cache_keeps_inserted_values_below_capacity`
* `the_id2str_cache_stays_bounded` (N ≫ capacity inserts → `len() <= capacity`)
* `a_term_string_survives_the_term_being_deleted_and_reinserted` — insert a quad, read it through a
  fresh reader, `clear_graph`, read again through the same reader: same string, which documents the
  content-addressed invariant.

**Expected.** A cache hit is ~0.05 µs plus a refcount bump against 3.35 µs, so every repeated term
removes ~3.3 µs from a scan. The benchmark dataset is the worst case for a dictionary cache and still
repeats: distinct subjects (`s{i}`), ten predicates (`p{i % 10}`), inline literals that need no lookup.
That is two hashed lookups per quad with about one of them repeating, so roughly 3 µs off a
6–8 µs/quad scan (≈ 40–50 %) after O1; real data with repeated classes, datatypes and language tags
hits more often. To be measured per workload, not promised as a single ratio.

**Risk.** Low. A stale value can only be a wrong string if two different strings share a `StrHash`,
which would already corrupt the store. `dashmap` shards the map, so a multi-threaded server does not
contend on one lock the way a `Mutex<HashMap>` would.

### O3 — Enable SlateDB's block cache (`foyer` feature)

**Why.** `lib/oxigraph/Cargo.toml` pins `slatedb` with `default-features = false` and `["aws", "lz4"]`
on purpose (see the comment there), which drops `foyer`. SlateDB 0.18 uses `db_cache::SplitCache` by
default *only when that feature is compiled in*, with a 512 MiB block cache plus 64 MiB of metadata
(`DEFAULT_BLOCK_CACHE_CAPACITY`, `DEFAULT_MAX_CAPACITY`). Today **there is no block cache at all**: every
`get` re-reads and re-decompresses its SST block through the object store API. That is part of why one
`DbSnapshot::get` is 2.1 µs on a local filesystem with a warm OS page cache.

**Change.** Add `"foyer"` to the `slatedb` feature list in `lib/oxigraph/Cargo.toml` and update that
comment (it currently lists the block cache among the things `default-features = false` gives up).
**No Rust code change is needed**: with the feature present, `DbBuilder` installs a default
`SplitCache`. Verify with `cargo tree -p oxigraph --features slatedb -e features -i slatedb` showing
`slatedb feature "foyer"`, and with a probe that the second read of the same block is faster than the
first.

**This one needs an explicit go-ahead** before it is implemented: it changes the feature selection of
our `slatedb` dependency (not its version or source), adds build time and a foyer dependency, and can
raise RSS by up to ~576 MiB. The plan is written so that the decision can be "no" without affecting
the other items.

**Expected.** Unknown until measured; this is a read-path item, so the benchmarks that matter are
point lookup, predicate scan, SPARQL mix and cold reopen. A win ≥ 10 % on at least one read workload
with an RSS increase that fits the deployment story keeps it. On S3/RustFS the effect should be
strictly larger than on a local FS, because it removes network round trips, but that is a hypothesis
until the RustFS numbers exist.

**Follow-ups, only if the measurement justifies them:** document the extra memory in
`docs/slatedb.md` §Performance; if users need to opt out, add
`SlateDbOptions::with_db_cache_disabled()` (a one-line builder method plus a test) rather than
exposing a cache-size knob — the plan does not add one otherwise.

**Tests.** The whole storage suite must stay green (a cache must not change results, including after
`clear` and reopen). The perf evidence is the benchmark, plus the ignored 1 M-quad peak-RSS test to
quantify the memory delta.

**Risk.** Medium: more memory and build time for a speedup that may be small on a local filesystem.
Negative results are recorded in `performance.md` and the feature flag is removed again.

### O4 — Resolve the dictionary terms of a scan batch in one hop (conditional)

**Why.** After O1 and O2, a scan still pays one `get` per *distinct* hashed term, ~3.3 µs each, so a
full 1 000-quad scan stays at ~4–5 ms (the RocksDB predicate scan returning 100 of those quads is
348 µs). Locally those gets are CPU-bound in the LSM (2.1 µs), so issuing them concurrently buys
little; over S3/RustFS they are round trips, and 256 of them per batch is where the time goes.

**Do not implement this before measuring.** The gate: on RustFS (or a cold local block cache, if O3
is rejected) run a 1 000-quad predicate scan with O2's cache disabled and count the wall-clock share
of `id2str` gets. Implement only if that share is ≥ 20 %, and keep it only if the result beats the
same scan without prefetch by ≥ 10 %.

**Design if it passes.** Prefetch into the O2 cache from the quad decoding iterator, so the decode
plumbing (`EncodedQuad` → `Quad` in the RDF layer) is untouched:

* `SlateDbDecodingQuadIterator` gains `cache: Arc<Id2StrCache>`, `source: SlateDbReadSource`,
  `bridge: Arc<RuntimeBridge>` and a `VecDeque<Result<EncodedQuad, StorageError>>` of at most
  `SCAN_BATCH_SIZE` decoded quads. `SlateDbStorageReader::inner_quads` already builds this iterator
  and has all three.
* `SlateDbReadSource` gets a private `handle(&self) -> Option<SlateDbReadHandle>` helper (a clone of
  the `Arc` inside `Ready`, `None` for `Failed`, whose error the normal `get_str` reports), because
  the prefetch must reach `SlateDbReadHandle::get_inner` inside one task rather than call the
  one-hop-per-key `SlateDbReadSource::get`.
* `next()` refills that deque from the existing `SlateDbScan`, decodes keys with
  `self.encoding.decode(key)` as today, collects the `StrHash` of every term not already cached, and
  issues **one** `run_slatedb` call that runs the
  `handle.get_inner(prefixed(Ns::Id2Str, hash.to_be_bytes()))` futures concurrently and inserts the
  decoded strings into the cache. `futures-util` is a **dev**-dependency today, so this item either
  promotes `futures-util.workspace = true` to the non-wasm regular dependencies (same workspace
  entry, `join_all`) or uses `tokio::task::JoinSet`, available through the already-enabled
  `rt-multi-thread` feature. No new external crate either way; prefer `JoinSet` only if the task
  spawn cost per lookup measures below the round-trip it hides.
* Errors of the prefetch are swallowed: the cache stays cold and the eventual `StrLookup::get_str`
  reports the error with its normal message. This keeps the iterator's error contract (one error,
  then the end of the scan) unchanged.
* Only quad iterators prefetch; the graph iterator has a handful of entries.
* `quad_str_hashes(&EncodedQuad) -> impl Iterator<Item = StrHash>` must match **exhaustively** every
  `EncodedTerm` variant that carries a `StrHash` (`NamedNode.iri_id`, `BigBlankNode.id_id`,
  `BigStringLiteral.value_id`, the `*LangStringLiteral.*_id` and `*TypedLiteral.*_id` variants) and do
  nothing for the inline `SmallString`/`SmallBlankNode`/numerical variants. No `_` arm: a new variant
  must fail to compile rather than silently skip its lookup.

**Tests.** `scans_with_prefetch_decode_every_term_kind` — a store with IRIs, blank nodes, plain,
language-tagged and typed literals, read through `Store::iter()` and compared to the inserted quads;
plus the existing scan tests, which must be unchanged. Memory stays bounded by
`SCAN_BATCH_SIZE` decoded quads (tens of KiB).

**Expected.** Nothing until measured. If it ships, report it as a separate line in the benchmark table
(object-store scan with and without prefetch).

**Risk.** Medium: extra buffering and a per-batch hop on a path that is already correct. The
exhaustive match is what keeps it from silently under-prefetching.

### O5 — Publish the durability/latency trade-off instead of changing it

**Why.** `Wal` durability is a phase 3 design decision (D5) and the single largest line in the table:
101 ms per single-quad commit at the default 100 ms `flush_interval`, against 16 µs for RocksDB, which
does not fsync. `SlateDbOptions::with_flush_interval` and `SlateDbDurability` already expose every
knob; what is missing is a measured table so that a user can choose.

Measured with a temporary probe (reverted): 101 / 21 / 6.2 / 2.05 ms per commit at 100 / 20 / 5 / 1 ms
of `flush_interval`; `Store::extend` of 10 000 quads stays flat at 124–135 ms across all four, because
one commit amortizes the wait.

**Change (documentation and one reproducible probe, no behaviour change):**

* add an ignored test `measure_commit_latency_by_flush_interval` to
  `lib/oxigraph/tests/store_slatedb.rs`, following the existing `measure_*` convention: for each
  interval, insert N single quads on a fresh store with `Wal` durability and print µs/commit.
* add its table to `docs/slatedb.md` §"Backup, replication and durability" under the durability
  bullets, with the recommendation: batch writes per commit (`extend`, bulk load) or accept the
  `Process` trade-off where losing the last `flush_interval` on a killed process is acceptable.
* extend bottleneck 1 of `planning/slatedb/performance.md` with the numbers.

**Explicitly not changing:** the default stays `Wal` with a 100 ms flush interval, and no new
"low latency" preset is added. Reducing the interval multiplies WAL objects on object stores; that is
a deployment decision, not a library default.

**Tests.** None beyond the ignored measurement test compiling and running.

**Risk.** None to behaviour; the risk is presenting the table as a speedup. It is not one — it is a
knob whose cost must be visible.

## 3. Sequencing

```
O1 bridge fast path ──► O2 id2str cache ──► O3 block cache (approval) ──► O4 prefetch (measure first)
                                     └──► re-run the whole benchmark ──► O5 durability table/docs
```

1. **O1** first: it is the largest win, it is isolated to one file, and it makes every later
   measurement less noisy.
2. **O2** next: small, safe, and it changes the numbers O4 has to be judged against.
3. **O3** after O2 and only with a go-ahead; if it is accepted, O4's local gate can be re-evaluated
   with the block cache on.
4. **O4** only if its gate passes on the object store.
5. **O5** last so the published table is the final one.

After O1 + O2 (and O3 if accepted), re-run the full benchmark and update
`planning/slatedb/performance.md` and the table in `docs/slatedb.md` **in the same commit** as the
numbers, including the 1 M-quad peak-RSS figures when O3 changes them.

## 4. Deliberately out of this plan

| idea | why not |
| --- | --- |
| Changing the default durability to `Process` or the default `flush_interval` | reopens D5; trades a documented crash guarantee for latency. The knobs exist, O5 makes the cost visible |
| Scan prefetch tuning (`read_ahead_bytes`, `max_fetch_tasks`) | measured at 1 MiB/4, 64 KiB/1, 4 KiB/1 and 4 MiB/8: all within noise on a local FS |
| Disabling compression to speed up reads | `Settings::compression_codec = None` is possible, but there is no measurement yet and it trades footprint for read CPU; would need its own before/after table |
| Bulk load, cold open, compaction | engine-bound (no SST ingestion, manifest replay, compaction scheduling). Reported in `performance.md`, not fixable in the integration |
| Key encoding, namespace prefix, `SCAN_BATCH_SIZE` | frozen by D2/D10 and measured as not the bottleneck |
| Sharding or replacing the bridge runtime | the bridge cost was the scheduling hop, not the runtime; worker count is irrelevant after O1 |
| A second-level cache in front of scans | would duplicate SlateDB's versioning and snapshot semantics; O2/O3 cover the measured cost |

## 5. Verification

Per item, the smallest set that covers the change, then the full set before the plan is declared
done. From the Phase 5 runs:

```bash
# fast, per item
cargo test -p oxigraph --features slatedb --lib
cargo test -p oxigraph --features slatedb --test store_slatedb
cargo test -p oxigraph --features slatedb --doc

# before finishing (Phase 5 numbers: 46 lib, 31 store_slatedb + 4 ignored, 63 doctests)
cargo test -p oxigraph --features slatedb --test slatedb_faults
cargo test -p oxigraph --features slatedb --test store
cargo clippy -p oxigraph --features slatedb --all-targets
RUSTDOCFLAGS="-D warnings" cargo doc -p oxigraph --no-deps
```

CI runs clippy on rustc 1.95 with `-D warnings -D clippy::all`, so `catch_unwind`/`AssertUnwindSafe`,
the new mutex type and the cache struct must be clippy-clean at warn level in this workspace. Tests
are plain `#[test]` threads by default (fast path) and Tokio tests in the store suite (fallback
path), which is why both must stay green.

Benchmark protocol: the commands of `performance.md` §"How to reproduce", three runs per workload,
median of the criterion point estimates, same machine, release + LTO, LocalFileSystem for continuity
and RustFS for the object-store items. Every published number names the commit it was measured on.

**Known measurement gap:** every number in this document and in `performance.md` is LocalFileSystem.
S3/RustFS point-lookup and scan numbers do not exist yet; O3's and O4's decisions depend on them.

## 6. Risks and rollback

| risk | mitigation |
| --- | --- |
| `Handle::block_on` on a runtime thread → panic/deadlock | the `try_current()` guard is the first statement; the existing embedding test and the new panic-in-runtime test cover it |
| Lost panic → process abort instead of `StorageError` | `catch_unwind` + the two panic tests, on both paths |
| O2 returns a stale string | impossible while hashes are content-derived; the invariant is documented on the cache and tested after `clear` + reinsert |
| O2 memory growth | hard entry cap, FIFO eviction, one cache per store; measured RSS in the 1 M-quad test |
| O3 memory/build cost | bounded by the 512 MiB block + 64 MiB metadata default; explicit approval, revert is one Cargo line |
| O4 extra latency on cached scans | gated on a measurement, one extra hop per 256 quads only, reverted if < 10 % |
| A published ratio that does not reproduce | every item ships its own benchmark rerun in the same commit; estimates stay out of `performance.md` |

Rollback is per item: O1 is one function plus tests, O2 is additive, O3 is one Cargo feature, O4 is
one iterator, O5 is documentation. No item depends on another being kept, except that O4's numbers
are judged against O1 + O2.

## 7. Exit criteria

- [ ] O1 merged: bridge tests pass on both paths, and `performance.md` shows the new point-lookup
      and scan numbers with the same benchmark command.
- [ ] O2 merged: bounded cache with unit tests, `get_str` hit path measured, and the scan/SPARQL
      numbers re-measured with the cache on.
- [ ] O3 decided: either the `foyer` feature is enabled with a measured read win and the RSS delta
      documented, or the measurement and its "no" are recorded in `performance.md`.
- [ ] O4 decided: shipped with a measured object-store win ≥ 10 %, or rejected with the measured
      share of `id2str` gets below the gate.
- [ ] O5 published: the `flush_interval` table is in `docs/slatedb.md` and bottleneck 1 of
      `performance.md`, and the default durability is still `Wal` at 100 ms.
- [ ] `docs/slatedb.md` and `performance.md` tables are from **one** post-change run, not a mix of
      old and new numbers.
- [ ] Full verification set above is green, the RDF encoding and the `Storage`/`Store` API are
      unchanged, and RocksDB is still the default backend.
- [ ] `planning/slatedb/implementation_plan2.md` §11 is updated where an item retires a listed risk
      (the "per-scan bridge overhead" row is the first one O1 closes).
