# SlateDB backend: phase 3 measurements

Exit criteria of §6.5 require absolute numbers, not ratios: a benchmark table of RocksDB against
SlateDB, a million quad load whose peak memory is measured and documented, a `clear*` path whose
memory does not grow with the number of deleted quads, and the known bottlenecks written down.

Everything below was measured on one machine, on this working tree, with the release profile. The
benchmark table, the O1/O2 section and bottleneck 1 were re-measured after the changes of
`optimization_plan1.md`; every other table is unchanged by them and was not re-run.

```
CPU     Intel Core i7-6700HQ (4 cores, 8 threads), 2.60 GHz
RAM     31.4 GB
rustc   1.96.1
cargo   1.96.1
storage local filesystem
```

## How to reproduce

```bash
# RocksDB (default, unchanged) and SlateDB side by side
cargo bench -p oxigraph --features slatedb --bench store_slatedb -- --noplot

# a single workload
cargo bench -p oxigraph --features slatedb --bench store_slatedb -- --quick "phase3 bulk load"

# bigger loads and deletions (default is 100 000 quads)
OXIGRAPH_BENCH_QUADS=1000000 cargo bench -p oxigraph --features slatedb --bench store_slatedb -- --noplot

# the million quad load and its peak resident set size
OXIGRAPH_BULK_NON_ATOMIC=1 OXIGRAPH_BENCH_QUADS=1000000 \
  cargo test -p oxigraph --no-default-features --features slatedb --test store_slatedb \
  --release -- --ignored --nocapture measure_million_quad_bulk_load_peak_memory
```

`benches/store.rs` is untouched: the phase 3 workloads live in `benches/store_slatedb.rs` with
their bodies in `benches/common/mod.rs`, and `Store` is opened on a fresh `TempDir` for every
sample unless stated otherwise. Criterion reports a confidence interval for the mean; the point
estimate of that interval is used below.

## Benchmark table

| workload | RocksDB | SlateDB | note |
| --- | --- | --- | --- |
| insert 100 quads, one `Store::insert` each, `Wal` durability | 1.33 ms (13 µs/commit) | 10.12 s (101 ms/commit) | each commit waits for the WAL flush, see bottleneck 1 |
| insert 100 quads, one `Store::insert` each, `Process` durability | 1.33 ms | 2.83 ms (28 µs/commit) | no durability wait, closest to RocksDB's default |
| `Store::extend` of 10 000 quads | 45.8 ms (218 k quads/s) | 129 ms (78 k quads/s) | one write batch, `Wal` durability |
| bulk load of 100 000 quads, atomic | 399 ms (250 k quads/s) | 981 ms (102 k quads/s) | RocksDB builds SSTs, SlateDB writes batches |
| bulk load of 100 000 quads, `without_atomicity` | 399 ms (250 k quads/s) | 701 ms (143 k quads/s) | the recommended SlateDB path |
| 1 000 point lookups by subject (`Store::contains`) | 1.77 ms (1.8 µs/lookup) | 3.04 ms (3.0 µs/lookup) | one storage round trip per lookup, now driven on the caller thread |
| scan by predicate over 1 000 quads, 100 results | 351 µs | 121 µs | 1.2 µs/result against 3.5 µs/result, the dictionary cache pays off |
| SPARQL mix, 3 queries over 1 000 quads | 3.97 ms | 1.55 ms | projection, count and ordered `LIMIT` |
| `Store::clear_graph` of a 100 000 quad named graph | 2.17 s (46 k quads/s) | 2.19 s (46 k quads/s) | both delete quad by quad in batches |
| cold reopen of a 100 000 quad store, then `Store::len` | 47.2 ms | 457 ms | SlateDB replays its manifest and WAL objects |

The read path now beats RocksDB: a point lookup is 1.7 times faster, the 100-result scan 2.9 times
and the SPARQL mix 2.6 times, because the bridge no longer pays a scheduling hop per call and the
repeated `id2str` lookups are cached (see the next section). RocksDB still wins every write and
startup workload and stays the default backend: the gap is the expected price of a log structured
store on top of an object store abstraction, one durable write per commit, no native SST ingestion,
and a manifest replay at startup. Deletion at scale is where both backends cost the same, because
both are bound by the number of keys deleted rather than by the storage engine.

## The bridge and the dictionary cache (O1 and O2)

Two changes to the integration (planned in `optimization_plan1.md`) moved the read path, measured
with the same command and on the same tree as the table above:

| workload | before | after | change |
| --- | --- | --- | --- |
| 1 000 point lookups (`Store::contains`) | 17.5 ms | 3.04 ms | 5.8× faster |
| scan by predicate, 100 of 1 000 quads | 3.68 ms | 121 µs | 30× faster |
| SPARQL mix, 3 queries over 1 000 quads | 36.1 ms | 1.55 ms | 23× faster |
| cold reopen, then `Store::len` | 453 ms | 457 ms | unchanged |
| 100 single-quad commits (`Wal`) | 10.12 s | 10.12 s | unchanged, it is the flush interval |
| `Store::extend` of 10 000 quads | 127 ms | 129 ms | unchanged |
| bulk load of 100 000 quads | 948 ms | 981 ms | unchanged |

* **The bridge drives the future on the calling thread** (`RuntimeBridge::run`). Spawning the
  future and waiting on a channel cost 9.4–18 µs per call, which was 85 % of a 17 µs point lookup
  and 15.5 µs of every dictionary lookup. The caller is now checked with `Handle::try_current()`:
  on a plain thread the future is driven by `Handle::block_on` (0.05 µs), and inside a Tokio runtime
  the spawn-and-channel path stays, which is what keeps an embedding application from deadlocking.
  A panic is still reported as a `StorageError` on both paths.
* **The decoded `id2str` strings are cached** per store (`Id2StrCache` in
  `lib/oxigraph/src/storage/slatedb.rs`): a bounded `DashMap` of 16 384 entries keyed by `StrHash`,
  shared by every snapshot and readable transaction of the store. A hash always maps to the same
  string, so no invalidation is needed; a hit is a shard read and a refcount bump. Decoding a quad
  asked for its subject, predicate, object, datatype and language strings every time, and the
  predicate scan of the benchmark repeats its predicate on all 100 results.

One measurement kept the change honest: the fast path is **worse for the opening path**. SlateDB's
builder replays the manifest and the write-ahead log by yielding thousands of times, and each yield
of a `Handle::block_on`-driven future parks and unparks the calling thread. Driving the whole open
through the fast path measured a 100 000 quad cold open of 549–771 ms against 351–364 ms on a
worker, so `RuntimeBridge::run_on_worker` keeps the builder (and every per-operation call keeps the
fast path). With that split, the open alone is 347–382 ms and `Store::len` dropped from 130 ms to
91 ms.

Not done, and why:

* **SlateDB's block cache (`foyer`) stays off.** The dependency keeps `default-features = false`, so
  there is still no block cache; enabling it is a one-line feature change that raises the memory
  ceiling by up to 576 MiB and needs an explicit decision, which has not been made.
* **No batched `id2str` prefetch.** The plan gates it on a measured share of at least 20 % of scan
  time in dictionary gets over an object store, and there are no S3/RustFS numbers yet: on the local
  filesystem those gets are now CPU-bound LSM reads of ~3 µs and the predicate scan is already
  faster than RocksDB.

## A million quad load

Measured with the ignored `measure_million_quad_bulk_load_peak_memory` test, loading through
`Store::bulk_loader().without_atomicity()` — the documented path for a load of that size.

| quads | encoding and writes | commit (WAL flush) | total | peak RSS |
| --- | --- | --- | --- | --- |
| 1 000 000 | 6.16 s | 3.25 s | 11.8 s | **1 441 MiB** |
| 2 000 000 | 15.3 s | 3.43 s | 27.8 s | 1 898 MiB |

The peak stays bounded because a non atomic load is written in chunks of 50 000 quads
(`NON_ATOMIC_BULK_LOAD_CHUNK`), so memory follows the four encode threads and the SlateDB memtable
rather than the size of the load: doubling the quads from 1 to 2 million costs 35 % more memory and
not twice as much. `Store::validate` was skipped at that size (bottleneck 6).

## Deletion at scale

Measured with the ignored `measure_million_quad_clear_peak_memory` test, which samples the
resident memory every 10 ms while the deletion runs and reports the highest sample above the
memory used before it:

| operation | quads | time | peak RSS above the start |
| --- | --- | --- | --- |
| `clear_graph` of the whole graph | 50 000 | 1.09 s | +248 MiB |
| `clear_graph` of the whole graph | 200 000 | 5.9 s | +983 MiB |
| `clear_graph` of the whole graph | 1 000 000 | 47.8 s | +5 447 MiB |
| `clear` (delete range expansion) | 200 000 | 7.9 s | +789 MiB |
| `clear` (delete range expansion) | 1 000 000 | 46.2 s | +5 334 MiB |
| RocksDB `clear_graph` for comparison | 200 000 | 5.4 s | +276 MiB |

Both clear paths are **not** flat in memory: a clear is one transaction, so its deletes are staged
until the commit, about a kilobyte each, i.e. 5.4 KiB per deleted named graph quad (six keys) or
2.7 KiB per default graph quad (three keys). RocksDB stages its deletes in a `WriteBatch` too and
costs 1.4 KiB per quad here, so this is a property of the shared transaction design rather than of
SlateDB, but SlateDB pays about four times more per key. What the guard buys is therefore not
flat memory but a failure instead of an out of memory kill: `max_clear_deletes` counts keys and
fails the clear before anything is written, and its default of 2 000 000 keys keeps one clear under
roughly two gigabytes, enough for a third of a million quads of a named graph.

The time is what one would expect from one big write: 1 M quads are 6 M keys in a single batch,
which is also why the clearest bottleneck below (large single writes) matters here.

## Object store requests

Counted with `strace -f -c -e trace=openat,rename,link,unlink,fsync,fdatasync,write,read` around
the million quad load test (200 000 quads, non atomic, `LocalFileSystem`):

| syscall | calls | per loaded quad |
| --- | --- | --- |
| `write` | 15 036 | 0.075 |
| `read` | 16 293 | 0.081 |
| `openat` | 2 547 (229 failed) | 0.013 |
| `unlink` | 33 | — |
| `rename` | 3 | — |

The counts are modest because the load streams 50 000 quad chunks into one write each, and because
`LocalFileSystem` writes its objects in place instead of staging and renaming them; on an object
store each of those writes would be a request with its own latency, which is what bottleneck 1 is
about. The same trace shows no `fsync` call, so SlateDB's durability on a local filesystem comes
from the write completing rather than from an fsync.

## Atomic load scaling

The default atomic load stages every quad in a single write batch and commits it once, so its cost
grows with the size of that one write:

| quads | atomic total | peak RSS | outcome |
| --- | --- | --- | --- |
| 100 000 | 968 ms | — | fine |
| 200 000 | 2.2 s | 802 MiB | fine |
| 500 000 | — | — | did not finish in 400 s |
| 1 000 000 | — | — | not attempted atomically; `without_atomicity` takes 11.8 s |

The cliff is SlateDB's write path on a single batch of several million keys, not the encoding:
500 000 quads are 1.5 million keys in one write. `bulk_load_max_staged_bytes` (2 GiB by default)
still guards the memory of an atomic load, and `without_atomicity` is what makes a 10⁸ quad load
possible, at the price of visibility during the load.

## On disk footprint

Bytes and files of the store directory right after a load, before SlateDB's background compaction
has caught up:

| quads | RocksDB | SlateDB |
| --- | --- | --- |
| 100 000 | 17 files, 20.9 MB | 10 files, 18.7 MB |
| 1 000 000 | 17 files, 206 MB | 41 files, 382 MB |

SlateDB keeps the WAL and the L0 SSTs it wrote until they are compacted, so its footprint is
larger than RocksDB's while a load is still settling; the 100 000 quad case is already compacted
and smaller.

## Held iterators and garbage collection (Q8)

Q8 asked whether a long-lived snapshot (a `Store::iter()` held by a query) pins manifest and SST
objects such that garbage collection is delayed unacceptably. The answer is the opposite of the
question: SlateDB's garbage collector deletes objects by **age**, not by the snapshots that
reference them, so a held snapshot never delays it. The risk is that it deletes something the
snapshot still needs.

Measured with the ignored `measure_a_held_iterator` test: a `Store::iter()` created on a 2 000 quad
store was read once, then held while 6 more write-and-flush rounds ran over 73 s, well past the
default `wal_options.min_age` of 60 s. The iterator then produced all 2 000 quads it had
snapshotted, with no error. The reason is that a flush moves data into level 0 tables under
`compacted/`, whose default `min_age` is 24 hours, so the objects a flushed snapshot needs outlive
the wait by orders of magnitude; only WAL objects live 60 s, and they are no longer referenced once
the data is flushed.

The defaults, and therefore the limits of a held iterator:

| directory | default `min_age` | what a held snapshot relies on |
| --- | --- | --- |
| `wal/` | 60 s | nothing, once the data is in a level 0 table |
| `compacted/` (L0 and sorted runs) | 24 h | the data of the snapshot |
| `manifest/` | 24 h | the version of the tree the snapshot reads |

So a query iterator may safely live for minutes, and is only at risk if it is held for longer than
the `compacted/` and `manifest/` age (a day with the defaults), or if `min_age` is lowered. A
deployment that needs iterators held for days must raise `garbage_collector_options.compacted_options.min_age`
and `...manifest_options.min_age` above that lifetime. This is also why reading a database that
another process is writing is documented as undefined behavior for a read-only reader: the same
age-based deletion applies to it, and it can sit on a version far older than the minimum age.

## Known bottlenecks

1. **One durable flush per commit.** With `Wal` durability each commit waits for the next WAL
   flush, which happens at most every `flush_interval` (100 ms by default), so the latency of a
   small commit is the interval. Measured with the ignored
   `measure_commit_latency_by_flush_interval` test, 20 single-quad commits per interval:

   | `flush_interval` | one single-quad commit | one commit of 10 000 quads |
   | --- | --- | --- |
   | 100 ms (default) | 101 ms | 131 ms |
   | 20 ms | 21.3 ms | 99 ms |
   | 5 ms | 6.1 ms | 94 ms |
   | 1 ms | 2.1 ms | 79 ms |

   Batching writes amortizes the wait instead of shortening it (`extend` of 10 000 quads is 129 ms)
   and `Process` durability removes it (28 µs/commit) at the price of losing the last commits of a
   killed process. A shorter interval multiplies the number of WAL objects in the object store, so
   it is a deployment decision, not a better default.
2. **No SST ingestion.** The bulk loader cannot hand files to SlateDB the way the RocksDB loader
   does: it encodes quads and streams write batches, so it is 2.5 times slower than RocksDB and
   bound by writes and compaction rather than by the CPU.
3. **Startup replays the manifest and the write-ahead log.** A 100 000 quad store takes 457 ms to
   reopen against 47 ms for RocksDB, and 350–380 ms of that is `Db::build` itself. That path is
   yield-bound rather than CPU-bound: driving it with `Handle::block_on` costs a park and an unpark
   per yield and measured 549–771 ms, which is why it keeps the worker path.
4. **Large single writes collapse.** A commit of 1.5 million keys takes minutes (see above), which
   is why non atomic loads are chunked at 50 000 quads and why the atomic path is meant for
   reasonably sized files. This is a SlateDB side effect, visible in the atomic scaling table.
5. **Atomic loads are memory bound.** 200 000 quads already peak at 802 MiB because the whole load
   is held in memory until `commit`; the 2 GiB budget stops a load well before the machine runs out
   of memory, and a failed budget check leaves nothing behind.
6. **Clearing a big graph costs about a kilobyte per deleted key.** A clear is a single
   transaction, so its deletes are staged until the commit: 1 M quads of a named graph take 47.8 s
   and 5.3 GiB above the memory the process already used. `max_clear_deletes` (2 000 000 keys by
   default) turns a clear that would not fit into an error before it writes anything; raise it only
   when the machine has the memory, about a gigabyte per million keys.
7. **`Store::validate` is not usable on large stores.** It checks every quad with point lookups in
   two to five indexes, so it costs a round trip per quad and per index: minutes on a million
   quads. It stays a debugging and testing helper, not something to run on a production store.
8. **Footprint lags behind compaction.** 382 bytes per quad right after a million quad load against
   206 for RocksDB, with 41 objects to open at the next startup, which is part of the 449 ms cold
   reopen.

## Deviations from the plan

* **D8 says the atomic load stages into one `DbTransaction`.** It stages into one `WriteBatch`
  instead, and the write batch is written with the durability policy at `commit`. A SlateDB
  transaction of that size is slower (1.33 s against 968 ms for 100 000 quads) and, like the write
  batch, it holds every key in memory, so nothing is lost by leaving the transaction out. The
  atomicity promise is unchanged: the load becomes visible at `commit` or not at all.
* **A non atomic load writes in chunks of 50 000 quads** (`NON_ATOMIC_BULK_LOAD_CHUNK`) even though
  `Store` hands the loader batches of up to a million quads. Without it, a million quad load is one
  write of three million keys, which does not finish in any reasonable time (see the atomic scaling
  table).
* **`max_clear_deletes` defaults to 2 000 000 keys** instead of the 50 000 000 the plan implies: at
  the measured kilobyte per pending delete, the larger bound would not prevent the out of memory
  kill it exists for.
* **Compression is LZ4** (`Settings::compression_codec`), which needs the `lz4` feature of the
  `slatedb` crate: it is enabled on the dependency in `lib/oxigraph/Cargo.toml`.
