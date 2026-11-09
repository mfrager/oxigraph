# SlateDB backend: phase 3 measurements

Exit criteria of §6.5 require absolute numbers, not ratios: a benchmark table of RocksDB against
SlateDB, a million quad load whose peak memory is measured and documented, a `clear*` path whose
memory does not grow with the number of deleted quads, and the known bottlenecks written down.

Everything below was measured on one machine, on this working tree, with the release profile.

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
| insert 100 quads, one `Store::insert` each, `Wal` durability | 1.63 ms (16 µs/commit) | 10.12 s (101 ms/commit) | each commit waits for the WAL flush, see bottleneck 1 |
| insert 100 quads, one `Store::insert` each, `Process` durability | 1.63 ms | 2.87 ms (29 µs/commit) | no durability wait, closest to RocksDB's default |
| `Store::extend` of 10 000 quads | 46.4 ms (215 k quads/s) | 121 ms (83 k quads/s) | one write batch, `Wal` durability |
| bulk load of 100 000 quads, atomic | 406 ms (246 k quads/s) | 968 ms (103 k quads/s) | RocksDB builds SSTs, SlateDB writes batches |
| bulk load of 100 000 quads, `without_atomicity` | 406 ms (246 k quads/s) | 731 ms (137 k quads/s) | the recommended SlateDB path |
| 1 000 point lookups by subject (`Store::contains`) | 1.83 ms (1.8 µs/lookup) | 17.5 ms (17 µs/lookup) | one storage round trip per lookup |
| scan by predicate over 1 000 quads, 100 results | 363 µs | 3.68 ms | 3.6 µs/result against 37 µs/result |
| SPARQL mix, 3 queries over 1 000 quads | 3.88 ms | 36.1 ms | projection, count and ordered `LIMIT` |
| `Store::clear_graph` of a 100 000 quad named graph | 2.25 s (44 k quads/s) | 2.35 s (43 k quads/s) | both delete quad by quad in batches |
| cold reopen of a 100 000 quad store, then `Store::len` | 47.1 ms | 449 ms | SlateDB replays its manifest and WAL objects |

No workload is faster with SlateDB, and RocksDB stays the default backend. The gap is the expected
price of a log structured store on top of an object store abstraction: one write per commit, one
round trip per point lookup, and no native SST ingestion. Deletion at scale is the one place where
both backends cost the same, because both are bound by the number of keys deleted rather than by
the storage engine.

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

## Known bottlenecks

1. **One durable flush per commit.** With `Wal` durability each commit waits for the next WAL
   flush, which happens at most every `flush_interval` (100 ms by default): 101 ms per single quad
   `Store::insert` above. Batching writes amortizes it (`extend` of 10 000 quads is 121 ms) and
   `Process` durability removes it (29 µs/commit), at the price of losing the last commits on a
   crash. `SlateDbOptions::with_flush_interval` trades write amplification for that latency.
2. **No SST ingestion.** The bulk loader cannot hand files to SlateDB the way the RocksDB loader
   does: it encodes quads and streams write batches, so it is 2.4 times slower than RocksDB and
   bound by writes and compaction rather than by the CPU.
3. **One round trip per point lookup and per scan batch.** Every `Store::contains` is 17 µs against
   1.8 µs for RocksDB, and a scan fetches 256 key batches, so a 100 result scan is 3.7 ms. The scan
   prefetch (`with_scan_prefetch`, 1 MiB read-ahead, 4 fetch tasks by default) only helps large
   scans.
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
