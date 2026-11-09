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
another process is actively writing remains undefined behavior: SlateDB garbage collection may
delete objects the reader still needs. Use read-only access on a database at rest, or read a
`backup` instead.

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

## What it costs

SlateDB writes every commit to the object store, so a database costs object store requests and
latency, not just disk: about 0.075 PUT and 0.081 GET per loaded quad on a bulk load, and a point
lookup costs a round trip to the store when it is not cached. Measures of the backend against
RocksDB, and the list of known bottlenecks, are in
[`planning/slatedb/performance.md`](../planning/slatedb/performance.md).
