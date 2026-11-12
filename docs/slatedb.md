# The SlateDB backend

Oxigraph can store its database in [SlateDB](https://slatedb.io/), an LSM tree written to an object
store. The RocksDB backend stays the default; this one is meant for deployments where the data should
live in S3 or in an S3 compatible store (RustFS, MinIO, Ceph…) instead of a local disk.

* Enable it in Rust with the `slatedb` feature of the `oxigraph` crate, and use
  `Store::open_with_slate_db(options)` / `Store::open_slate_db_read_only(options)`.
* The command line toolkit of this repository is built with both backends: `--object-store` selects
  SlateDB, its absence keeps RocksDB, so existing invocations are unchanged.
* Both backends share the RDF key encoding, so a RocksDB database can be migrated into a SlateDB one
  with `oxigraph migrate` (see below).

## Where the data lives

`SlateDbOptions` describes the location:

| Rust                                   | Command line                             | Location                      |
| -------------------------------------- | ---------------------------------------- | ----------------------------- |
| `SlateDbOptions::at_directory(path)`   | `--location DIR` (RocksDB) or `file://`  | a directory of this machine   |
| `SlateDbOptions::at_url("file:///path")` | `--object-store file:///path`          | the same thing, as a URL      |
| `SlateDbOptions::at_url("s3://bucket/prefix")` | `--object-store s3://bucket/prefix` | an S3 bucket and key prefix   |
| `SlateDbOptions::at_object_store(client, "prefix")` | — | an object store client the application builds |

`with_prefix` adds a key prefix below the one of the URL. A SlateDB database is a handful of objects
under that prefix: do not point two databases at the same prefix.

## S3 credentials, region and endpoint

By default the backend builds its S3 client from the AWS environment variables, exactly like the AWS
CLI. An application that keeps its credentials elsewhere, needs a client per database, or sits
behind a proxy builds the client itself and passes it to `SlateDbOptions::at_object_store` (see
[A client built in code](#a-client-built-in-code)).

| Variable                          | Meaning                                                     |
| --------------------------------- | ----------------------------------------------------------- |
| `AWS_ACCESS_KEY_ID`               | Access key                                                  |
| `AWS_SECRET_ACCESS_KEY`           | Secret key                                                  |
| `AWS_SESSION_TOKEN`               | Session token, if any                                       |
| `AWS_REGION` / `AWS_DEFAULT_REGION` | Region, `us-east-1` for RustFS and MinIO                  |
| `AWS_ENDPOINT`                    | Endpoint of an S3 compatible store, scheme included         |
| `AWS_ALLOW_HTTP`                  | `true` to allow the plain HTTP endpoint of a local store    |
| `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` | `false` keeps the path-style addressing S3 compatible stores need |

S3 compatible stores use path-style addressing, which is the default, so only the endpoint, the
region and `AWS_ALLOW_HTTP=true` are usually needed.

### A client built in code

`SlateDbOptions::at_object_store` takes an `object_store` client and the prefix of the database
inside it. This is the path for everything the environment cannot express: credentials read from a
secret manager, an endpoint or region per database instead of per process, a proxy, a timeout, or
one client shared by several databases.

```rust
use std::sync::Arc;
use oxigraph::store::{object_store, SlateDbOptions, Store};
use object_store::aws::AmazonS3Builder;

let client = AmazonS3Builder::new()
    .with_bucket_name("bucket")
    .with_region("us-east-1")
    .with_endpoint("http://localhost:9000")
    .with_allow_http(true)
    .with_access_key_id(access_key)
    .with_secret_access_key(secret_key)
    .build()?;
let store = Store::open_with_slate_db(SlateDbOptions::at_object_store(Arc::new(client), "db"))?;
```

The `object_store` crate is re-exported as `oxigraph::store::object_store`, so the client is built
on the version SlateDB itself uses. The `AWS_*` variables are only read by the default path: start
from `AmazonS3Builder::from_env()` instead of `new()` to keep them and override one setting.

### RustFS recipe

[RustFS](https://rustfs.com/) speaks the S3 API on port 9000 and reports the region `us-east-1`:

```bash
docker run -d --name rustfs \
  -p 9000:9000 -p 9001:9001 \
  -v rustfs-data:/data \
  -e RUSTFS_ACCESS_KEY=oxigraph \
  -e RUSTFS_SECRET_KEY=changeme \
  -e RUSTFS_ADDRESS=":9000" \
  rustfs/rustfs:latest /data

# One bucket for the database, with any S3 client:
aws --endpoint-url http://localhost:9000 s3 mb s3://oxigraph

export AWS_ACCESS_KEY_ID=oxigraph
export AWS_SECRET_ACCESS_KEY=changeme
export AWS_REGION=us-east-1
export AWS_ENDPOINT=http://localhost:9000
export AWS_ALLOW_HTTP=true

oxigraph load --object-store s3://oxigraph/db --file data.nq --format nq
oxigraph serve --object-store s3://oxigraph/db
```

### From a `.env` file

The `oxigraph-s3` runner of this repository (`tools/s3-runner`) keeps the same settings in a `.env`
file, and exports the `AWS_*` variables above from them before running the CLI:

```bash
cargo run -p oxigraph-s3-runner -- --init   # writes .env.example
cp .env.example .env                        # then fill in the key pair
cargo run -p oxigraph-s3-runner -- serve --bind 0.0.0.0:7878
```

It splits the endpoint into `S3_HOST`, `S3_PORT` and `S3_SCHEME`, and takes the bucket, the base
path, the key pair and the region from `S3_BUCKET`, `S3_BASE_PATH`, `S3_ACCESS_KEY`,
`S3_SECRET_KEY` and `S3_REGION`. `--dry-run` prints the command and the variables it would run
with, credentials hidden.

## Object store retries

SlateDB retries the transient object store errors of a foreground or background operation
indefinitely by default, which keeps a store alive through a network hiccup but can also hide a dead
endpoint. Give a bound in a settings file, in TOML, and pass it with `--slatedb-config`:

```toml
# oxigraph.toml
object_store_max_retries = 10
flush_interval = "100ms"
l0_sst_size_bytes = 67108864
max_unflushed_bytes = 268435456
compression_codec = "Lz4"
```

A settings file replaces the built-in settings of the backend (100 ms flush interval, 64 MiB L0
tables, 256 MiB of unflushed writes, LZ4 compression) instead of patching them: `Settings::from_file`
merges the file over SlateDB's own defaults, so a key left out of the file falls back to SlateDB's
value and not the backend's. Of the four settings above, only two differ between the two sets, so a
file that sets neither ends up with 1 GiB of unflushed writes and no compression at all. State them
again. Values are spelled like the Rust types, hence `"Lz4"` and not `"lz4"`. Unknown keys, typos
included, are silently ignored rather than reported, so a misspelt option leaves the store on its
default. In Rust, the same file is read with `Settings::from_file` and handed to
`SlateDbOptions::with_settings`; the type is re-exported as `oxigraph::store::SlateDbSettings`.

## Read-only access

`oxigraph serve-read-only --object-store …` and `Store::open_slate_db_read_only` read the database
through a SlateDB reader that follows the latest manifest without writing anything, so opening it
does not take the writer role away from the process writing the database. Reading a database that
another process is actively writing remains undefined behavior: SlateDB garbage collection deletes
objects by age, not by the readers that use them, and may delete objects the reader still needs.
The default ages are 60 s for the write-ahead log and 24 h for the manifest and the tables, so a
reader of a live database can break after a few minutes. Use read-only access on a database at
rest, raise the `garbage_collector_options` ages above the lifetime of your readers, or read a
`backup` instead. The same limits apply to a query iterator held open for a long time; see the
[performance page](../planning/slatedb/performance.md).

## Backup, replication and durability

* A commit with the default `SlateDbDurability::Wal` policy answers once the write is durable in the
  object store, which is the durability the RocksDB backend gets from its write-ahead log.
  `SlateDbDurability::Process` returns as soon as the write is queued and can lose up to the last
  flush interval on a killed process; `SlateDbDurability::Flush` also moves the memtable to a level 0
  table before answering.
* `Store::backup(directory)` and `oxigraph backup` write a logical copy of a snapshot. It is the only
  consistent copy of the database: an asynchronous replica of the bucket is a lagging view of a
  moving LSM tree, not a snapshot of a database.
* Bucket versioning plus cross-region replication is the durability story for a real deployment, and
  the `backup` operation the point-in-time one. Oxigraph does not implement replication itself.

The cost of `Wal` durability depends on `flush_interval`, because a commit waits for the next
write-ahead-log flush. Measured on the machine of the
[performance page](../planning/slatedb/performance.md), reproducibly with
`cargo test -p oxigraph --features slatedb --test store_slatedb --release -- --ignored --nocapture
measure_commit_latency_by_flush_interval`:

| `flush_interval` | 20 single-quad commits | one commit of 10 000 quads |
| --- | --- | --- |
| 100 ms (default) | 101 ms per commit | 131 ms |
| 20 ms | 21.3 ms per commit | 99 ms |
| 5 ms | 6.1 ms per commit | 94 ms |
| 1 ms | 2.1 ms per commit | 79 ms |

Batch the writes of a workload into one commit (`Store::extend`, the bulk loader, one transaction)
before shortening the interval: batching removes the wait from the critical path of every quad,
while a shorter interval multiplies the write-ahead-log objects the object store has to keep.

## Behaviour changes against the RocksDB backend

The RDF encoding, the SPARQL results and the `Store` API are the same. Four things behave
differently, and none of them is configurable away:

### One writer at a time: a second writer fences the first

SlateDB allows a single writer per database. Opening the database for writing takes the writer role
from whoever holds it, even in another process: the previous writer's next operation fails with
`this store was fenced or closed by another writer`. There is no lock to wait on, so a deployment
that starts a second server on the same database loses writes in the first one instead of refusing
to start. Read-only opens (`serve-read-only`, `Store::open_slate_db_read_only`) do not take the
writer role, and `backup` does not either.

### `clear` and `clear_graph` cost one pending key per deleted quad

SlateDB has no range delete, so a clear is expanded into one delete per key of the deleted quads:
six keys for a named graph quad and three for a default graph quad. They are staged in the
transaction until its commit, at about a kilobyte each, so memory grows with the size of the
cleared graph — 1 M quads of a named graph takes about 5.3 GiB above the memory the process already
used. `max_clear_deletes` (2 000 000 keys by default) fails a clear that would not fit instead of
letting the process be killed; raise it only with the memory to match, about a gigabyte per million
keys. Clearing costs the same time as with RocksDB: both backends are bound by the number of keys
deleted.

### A bulk load stages everything in memory unless it is non atomic

`Store::bulk_loader()` without `without_atomicity()` keeps every encoded quad in memory until its
single commit, so the load becomes visible at once or not at all. That is bounded by
`bulk_load_max_staged_bytes` (2 GiB by default), which fails the load before the process runs out of
memory, and it is slow above roughly 200 000 quads because one enormous write is much worse than
several. `without_atomicity()` writes chunks of 50 000 quads, makes the load visible while it runs
and is the path to use for millions of quads. RocksDB has the same atomicity knob but builds SST
files instead, so its atomic load does not scale the same way.

### `optimize` does nothing

SlateDB compacts in the background on its own schedule. `Store::optimize()` and
`oxigraph optimize` succeed immediately and change nothing; do not expect them to shrink the object
store or to speed up later queries. The operational levers are the SlateDB settings
(`compactor_options`, `l0_sst_size_bytes`, `max_unflushed_bytes`) and the object store's own
lifecycle rules.

## Performance

The same workloads on the same machine, RocksDB against SlateDB (details and the method in
[`planning/slatedb/performance.md`](../planning/slatedb/performance.md)):

| workload | RocksDB | SlateDB |
| --- | --- | --- |
| 100 `Store::insert`, `Wal` durability | 1.33 ms | 10.12 s |
| 100 `Store::insert`, `Process` durability | 1.33 ms | 2.83 ms |
| `Store::extend` of 10 000 quads | 45.8 ms | 129 ms |
| atomic bulk load of 100 000 quads | 399 ms | 981 ms |
| non atomic bulk load of 100 000 quads | 399 ms | 701 ms |
| 1 000 point lookups (`Store::contains`) | 1.77 ms | 3.04 ms |
| scan returning 100 of 1 000 quads | 351 µs | 121 µs |
| 3 SPARQL queries over 1 000 quads | 3.97 ms | 1.55 ms |
| `clear_graph` of a 100 000 quad named graph | 2.17 s | 2.19 s |
| cold reopen of a 100 000 quad store | 47.2 ms | 457 ms |

RocksDB stays the default backend because it wins every write and startup workload: SlateDB pays
one durable flush per commit when `Wal` durability is asked for (101 ms against 13 µs per
single-insert commit, removed by `Process` durability), has no SST ingestion for bulk loads, and
replays its manifest at startup. The read path is the other way round, since a lookup no longer pays
a scheduling hop and the dictionary lookups are cached: a point lookup is 1.7 times faster than with
RocksDB, a 100-result scan 2.9 times and the SPARQL mix 2.6 times. The measurements and the known
bottlenecks are in the performance page.

The object store bill follows: a bulk load costs about 0.075 write and 0.081 read request per loaded
quad on a local filesystem, and every commit that waits for its WAL is a request too. Bucket
versioning plus lifecycle rules are the cost knobs of a real deployment.

## Observability

Oxigraph has no logging or metrics facility of its own, and the SlateDB backend does not add one.
What a deployment can watch instead:

* the object store: request counts, bytes, the objects under the database prefix, and their age
  (the garbage collector deletes by age, see the read-only section);
* SlateDB's own metrics, enabled with `metric_level` in the settings file
  (`Disabled`, `Info`, `Debug` or `Trace`, `Info` being enough to see the flushes and compactions),
  which SlateDB reports through its `metrics` facade;
* `Store::len()`, `Store::validate()` and `Store::backup()` as the operational API.

`optimize` does not help here: it is a no-op (see the behaviour changes above).

## Migrating a RocksDB database

```bash
oxigraph migrate --source /var/lib/oxigraph --object-store s3://oxigraph/db
```

* The source is only read, and the migration refuses to run while another process has it open.
* Keys, values and metadata are copied verbatim, so the target keeps the `oxversion` and RDF
  encoding version of the source.
* It writes a marker in the metadata keyspace of the target between batches: an interrupted
  migration resumes from its last committed key. `--restart` copies the source again from the
  beginning; run it when the source changed since the interruption.
* When the copy is over, the target is reopened and every quad is compared with the source in key
  order, and `validate()` must pass.

