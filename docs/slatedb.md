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

`with_prefix` adds a key prefix below the one of the URL. A SlateDB database is a handful of objects
under that prefix: do not point two databases at the same prefix.

## S3 credentials, region and endpoint

Credentials never go through `SlateDbOptions`: the backend builds its S3 client with the AWS
environment variables, exactly like the AWS CLI.

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
compression_codec = "lz4"
```

A settings file replaces the built-in settings of the backend (100 ms flush interval, 64 MiB L0
tables, 256 MiB of unflushed writes, LZ4 compression), so state them again. In Rust, the same file
is read with `Settings::from_file` and handed to `SlateDbOptions::with_settings`; the type is
re-exported as `oxigraph::store::SlateDbSettings`.

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
| 100 `Store::insert`, `Wal` durability | 1.63 ms | 10.12 s |
| 100 `Store::insert`, `Process` durability | 1.63 ms | 2.87 ms |
| `Store::extend` of 10 000 quads | 46.4 ms | 121 ms |
| atomic bulk load of 100 000 quads | 406 ms | 968 ms |
| non atomic bulk load of 100 000 quads | 406 ms | 731 ms |
| 1 000 point lookups (`Store::contains`) | 1.83 ms | 17.5 ms |
| scan returning 100 of 1 000 quads | 363 µs | 3.68 ms |
| 3 SPARQL queries over 1 000 quads | 3.88 ms | 36.1 ms |
| `clear_graph` of a 100 000 quad named graph | 2.25 s | 2.35 s |
| cold reopen of a 100 000 quad store | 47.1 ms | 449 ms |

No workload is faster with SlateDB, which is why RocksDB stays the default backend. The costs are
the expected price of an LSM tree on top of an object store abstraction: one durable flush per
commit when `Wal` durability is asked for (101 ms against 16 µs per single-insert commit, removed by
`Process` durability), one round trip per point lookup, no SST ingestion for bulk loads, and a
manifest replay at startup. The list of known bottlenecks is in the performance page.

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

