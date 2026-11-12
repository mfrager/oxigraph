# Guide: running the CLI on S3 with the `oxigraph-s3` runner

How to put an `oxigraph` database on S3, RustFS, MinIO or another S3 compatible store by keeping the
connection settings in a `.env` file instead of a handful of `export` lines.

The backend itself (key layout, settings, retries, migration) is documented in
[`docs/slatedb.md`](../../docs/slatedb.md); the terse reference of the runner is
[`tools/s3-runner/README.md`](../../tools/s3-runner/README.md). This guide is the operator path: what
to install, what to write, what to run, and what each failure message means.

## 1. Quick start

```bash
# The CLI and the runner, built from this workspace.
cargo build -p oxigraph-cli -p oxigraph-s3-runner --release

# One bucket, created once with any S3 client (the runner never creates it).
AWS_ACCESS_KEY_ID=oxigraph AWS_SECRET_ACCESS_KEY=changeme \
  aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://oxigraph

# The template, next to where the .env file goes.
./target/release/oxigraph-s3 --init
cp .env.example .env
$EDITOR .env                                    # host, port, bucket, key pair

./target/release/oxigraph-s3 --dry-run serve    # what it would run, credentials hidden
./target/release/oxigraph-s3 serve              # localhost:7878
```

`--init` writes `<env file>.example` (`.env.example` by default) and refuses to replace an existing
one unless `--force` is given. It never writes `.env` itself, so the file with the secrets is always
created by a human.

## 2. What the runner does

It is an outer wrapper around the CLI: it links no library code, does not open the database and does
not change RocksDB, SlateDB or the RDF encoding.

```
.env  ──dotenvy──▶  S3_HOST/PORT/SCHEME/BUCKET/BASE_PATH/ACCESS_KEY/SECRET_KEY/REGION/SESSION_TOKEN
                            │
                            ├──▶  AWS_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
                            │     AWS_DEFAULT_REGION, AWS_ALLOW_HTTP, AWS_VIRTUAL_HOSTED_STYLE_REQUEST
                            │     (and AWS_SESSION_TOKEN when set)
                            ▼
oxigraph-s3 ARGS  ──▶  oxigraph ARGS --object-store s3://<bucket>/<base path>
```

The child process gets those variables, has `AWS_ENDPOINT_URL_S3` and (when no session token is
configured) `AWS_SESSION_TOKEN` removed — both would otherwise override or contradict the file — and
receives `--object-store` appended when the caller did not pass one. Its exit code becomes the
runner's exit code.

Anything the runner does not understand is forwarded verbatim, so `--bind`, `--cors`,
`--slatedb-config`, `--file`, `--lenient` and the rest of the CLI surface keep working.

## 3. Prerequisites

- **A CLI with the SlateDB backend.** The workspace CLI is built with it unconditionally, so
  `--object-store` is available; a CLI built elsewhere has to be built from this branch.
- **The runner**, `oxigraph-s3`, from `cargo build -p oxigraph-s3-runner`, or `cargo run -p
  oxigraph-s3-runner --`. Find the `oxigraph` executable with `--bin` or `OXIGRAPH_BIN` when it is
  not on the `PATH`.
- **An existing bucket.** S3 credentials usually may not create buckets, and the backend fails
  rather than creating one.
- **One writer at a time.** A SlateDB database is single-writer. Do not run `serve`, `load` or
  `migrate` against the same bucket prefix concurrently, and do not run a writer and a
  `serve-read-only` against it either: opening a database another process is writing is undefined
  behavior. Separate deployments need separate `S3_BASE_PATH` values.
- **A trusted endpoint.** `S3_SCHEME=https` is only useful if the TLS certificate of the endpoint
  validates against the client's trust store; a self-signed certificate needs a client built in code
  (`SlateDbOptions::at_object_store`), which the runner does not expose.

## 4. Prepare the endpoint

### RustFS or MinIO on this machine

```bash
docker run -d --name rustfs \
  -p 9000:9000 -p 9001:9001 \
  -v rustfs-data:/data \
  -e RUSTFS_ACCESS_KEY=oxigraph \
  -e RUSTFS_SECRET_KEY=changeme \
  -e RUSTFS_ADDRESS=":9000" \
  rustfs/rustfs:latest /data

aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://oxigraph
```

Matching `.env`:

```dotenv
S3_HOST=127.0.0.1
S3_PORT=9000
S3_SCHEME=http
S3_BUCKET=oxigraph
S3_BASE_PATH=slatedb
S3_ACCESS_KEY=oxigraph
S3_SECRET_KEY=changeme
S3_REGION=us-east-1
```

### AWS S3

```dotenv
S3_HOST=s3.eu-west-1.amazonaws.com
S3_PORT=
S3_SCHEME=https
S3_BUCKET=my-oxigraph-bucket
S3_BASE_PATH=slatedb
S3_ACCESS_KEY=AKIA...
S3_SECRET_KEY=...
S3_REGION=eu-west-1
```

`S3_REGION` has to be the region of the bucket, and the key pair has to allow `s3:ListBucket`,
`s3:GetObject` and `s3:PutObject` on `arn:aws:s3:::my-oxigraph-bucket/slatedb/*`. Temporary
credentials also need `S3_SESSION_TOKEN`.

### Any other S3 compatible store

Point `S3_HOST`/`S3_PORT`/`S3_SCHEME` at its endpoint and keep path-style addressing (the runner
always sets `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false`, which is what these stores expect). Stores that
need a virtual-hosted bucket (`bucket.host`) will not work through the runner.

## 5. The `.env` file

| Variable           | Required | Default          | Accepted values                                        |
| ------------------ | -------- | ---------------- | ------------------------------------------------------ |
| `S3_HOST`          | yes      | —                | Host only: no `://`, no path (`127.0.0.1`, `s3.eu-west-1.amazonaws.com`) |
| `S3_PORT`          | no       | scheme's default | `0`–`65535`, empty for the default port                |
| `S3_SCHEME`        | no       | `http`           | `http` or `https`                                      |
| `S3_BUCKET`        | yes      | —                | 3–63 chars, lowercase letters, digits, `.` and `-`     |
| `S3_BASE_PATH`     | no       | root of the bucket | letters, digits, `.`, `-`, `_`, `~`, `/`; surrounding `/` are stripped |
| `S3_ACCESS_KEY`    | yes      | —                | Access key                                             |
| `S3_SECRET_KEY`    | yes      | —                | Secret key                                             |
| `S3_SESSION_TOKEN` | no       | unset            | Token of temporary credentials                         |
| `S3_REGION`        | no       | `us-east-1`      | Region of the bucket                                   |
| `OXIGRAPH_BIN`     | no       | `oxigraph`       | Path of the executable, same as `--bin`                |

Rules worth knowing:

- **The environment wins over the file.** `dotenvy` only fills in variables that are not already
  set, so a single value can be overridden for one run: `S3_BASE_PATH=scratch oxigraph-s3 serve`.
- **Empty means unset.** `S3_PORT=` and `S3_BASE_PATH=` are valid and mean "default port" and "root
  of the bucket"; an empty `S3_ACCESS_KEY` is reported as missing.
- **The file is mandatory.** The runner refuses to start without a readable `.env`, even when every
  `S3_*` variable is already in the environment (see [§9](#9-deploying-without-a-real-env-file)).
- **Where it looks.** Without `--env-file`, `.env` is searched from the current directory upward, so
  running `oxigraph-s3` from the repository root does not see a file in `tools/s3-runner/`. Run from
  the directory that holds the file, or pass `--env-file path/to/.env`.
- **Values are read as written.** No percent-decoding and no shell expansion: `S3_BASE_PATH` must be
  URL-safe characters only, and a secret containing `#` or spaces has to be quoted (`S3_SECRET_KEY="a
  b#c"`), which `dotenvy` supports.
- **The secrets stay in the file.** `--dry-run` prints them as `<hidden>`, and nothing is logged by
  the runner. `.env` is in the repository's `.gitignore`, so the template gets committed and the file
  with the keys does not.

## 6. Run it

Check the plan first. With the RustFS `.env` above:

```console
$ oxigraph-s3 --dry-run serve --bind 0.0.0.0:7878
oxigraph serve --bind 0.0.0.0:7878 --object-store s3://oxigraph/slatedb
  AWS_ACCESS_KEY_ID=<hidden>
  AWS_SECRET_ACCESS_KEY=<hidden>
  AWS_REGION=us-east-1
  AWS_DEFAULT_REGION=us-east-1
  AWS_ENDPOINT=http://127.0.0.1:9000
  AWS_ENDPOINT_URL=http://127.0.0.1:9000
  AWS_ALLOW_HTTP=true
  AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
  AWS_ENDPOINT_URL_S3 (unset)
  AWS_SESSION_TOKEN (unset)
```

Then run it for real:

```console
$ oxigraph-s3 serve --bind 0.0.0.0:7878
```

`serve` is the default command, so `oxigraph-s3` alone starts the server on its own default,
`localhost:7878`. Opening a prefix for writing creates the database there immediately — `manifest/`,
`wal/` and `compactions/` objects appear before the first triple — so an empty object list after a
successful start means the writer holds a different prefix than you listed.

Runner options come before the CLI arguments, which have to come last:

```
oxigraph-s3 [--env-file PATH] [--init [--force]] [--dry-run] [--bin PATH] -- <command> [args…]
```

## 7. Recipes

```bash
# HTTP server, read-write (the default command).
oxigraph-s3 serve --bind 0.0.0.0:7878

# HTTP server, read-only: no other process may write the same database meanwhile.
oxigraph-s3 serve-read-only

# Bulk load, atomically; --lenient tolerates invalid IRIs, --format overrides the extension.
oxigraph-s3 load --file data.nq --format nq
oxigraph-s3 load --file wikidata.nt.gz --lenient --non-atomic

# From stdin, the format is then mandatory.
cat data.ttl | oxigraph-s3 load --format ttl

# A logical copy of the database into a local directory.
oxigraph-s3 backup --destination ./backup-2026-11-10

# A RocksDB database into the bucket. --restart resumes after an interruption.
oxigraph-s3 migrate --source /var/lib/oxigraph --restart

# Bound the object store retries and set the flush policy.
oxigraph-s3 serve --slatedb-config oxigraph.toml
```

`--slatedb-config` replaces the built-in SlateDB settings instead of patching them: a file has to
state `flush_interval`, `l0_sst_size_bytes`, `max_unflushed_bytes` and `compression_codec` again, or
accept SlateDB's own defaults for the ones it omits (see `docs/slatedb.md`).

Passing `--object-store` yourself is allowed and disables the URL the runner would append — useful to
write to a `file://` prefix with the same `.env`, or to keep an explicit `s3://` URL. The variables
are still exported from the file in that case.

```bash
oxigraph-s3 load --object-store file:///tmp/staging --file data.nq
```

`query`, `dump`, `update` and `optimize` take no `--object-store`: they open a local RocksDB
directory with `--location`, so the runner refuses them instead of silently starting an empty
in-memory store. For a database in S3 these go through the HTTP server (`curl`, `oxigraph-s3 serve`)
or through the Rust API; for a local one, run `oxigraph` directly.

## 8. File or environment?

The runner reads a file; the CLI itself reads the environment. Two rules follow:

- A deployment that already injects real environment variables (Kubernetes secrets, systemd
  `EnvironmentFile=`, a container orchestrator) does not need the runner at all: call `oxigraph`
  directly with `--object-store s3://bucket/prefix` and the `AWS_*` variables set.
- A deployment that ships a `.env` file (a compose file mounted next to the database, a directory of
  per-tenant files, an operator's laptop) uses the runner, one file per database, selected with
  `--env-file`.

## 9. Deploying without a real env file

The `.env` file is mandatory, so a container that only has environment variables supplies an empty
one:

```bash
: > /run/oxigraph/.env
oxigraph-s3 --env-file /run/oxigraph/.env serve --bind 0.0.0.0:7878
```

`/dev/null` does not work, it is not a regular file. A systemd unit therefore looks like:

```ini
[Service]
EnvironmentFile=/etc/oxigraph/s3.env
ExecStart=/usr/local/bin/oxigraph-s3 --env-file /etc/oxigraph/empty.env serve --bind 0.0.0.0:7878
Restart=on-failure
```

Note that the `S3_*` variables of the environment win, so such a unit can also deliberately override
one value of a file-based configuration.

## 10. Troubleshooting

| Message or symptom | Cause | Fix |
| --- | --- | --- |
| `No .env file was found from the current directory upward` | No `.env` in the working directory or above it | Run from the right directory, or pass `--env-file PATH`; `--init` writes a template |
| `The .env file PATH does not exist` | `--env-file` points somewhere else | Check the path; the runner does not create it |
| `.env.example already exists: pass --force to replace it` | `--init` run twice | `--init --force`, or copy the existing template |
| `S3_ACCESS_KEY is not set` (or `S3_HOST`, `S3_BUCKET`, `S3_SECRET_KEY`) | Missing or empty value | Fill it in the `.env`, or export it for one run |
| `S3_HOST must not include a scheme` | `S3_HOST=http://127.0.0.1` | `S3_HOST=127.0.0.1` and `S3_SCHEME=http` |
| `S3_HOST must not include a path` | `S3_HOST=127.0.0.1/bucket` | Put the path in `S3_BASE_PATH`, the bucket in `S3_BUCKET` |
| `S3_PORT must be a port number between 0 and 65535` | Non-numeric port | `S3_PORT=9000`, or leave it empty |
| `S3_BUCKET must only contain lowercase letters, digits, . and -` | Uppercase or `_` in the bucket name | Rename the bucket; S3 bucket names are lowercase |
| `S3_BASE_PATH must only contain letters, digits, ., -, _, ~ and /` | Space, `%`, `+`, `&`… in the path | The path is not percent-decoded: use URL-safe characters |
| `Could not run the oxigraph executable oxigraph` | No `oxigraph` on the `PATH` | `--bin target/release/oxigraph` or `OXIGRAPH_BIN=…` |
| `` `query` does not accept `--object-store` `` | A command that opens no database was given to the runner | Use the HTTP server, or run the CLI command directly |
| `` `--location` selects the local backend of oxigraph `` | RocksDB location given to the runner | Run `oxigraph` directly for a local database |
| `AccessDenied` / `InvalidAccessKeyId` / `SignatureDoesNotMatch` | Wrong key pair, or a bucket the key may not touch | Check the credentials and the bucket policy; the error comes from the store, not the runner |
| `NoSuchBucket` | The bucket was never created | `aws --endpoint-url … s3 mb s3://bucket`, or create it in the console |
| A TLS error on a plain-HTTP endpoint | `S3_SCHEME=https` against `http://…` | `S3_SCHEME=http` and `AWS_ALLOW_HTTP` follows it |
| Requests take a long time, then fail | The endpoint or port is unreachable, and SlateDB retries by default | Check `AWS_ENDPOINT` in `--dry-run`; bound the retries with `--slatedb-config` |
| Requests go to AWS although the file points elsewhere | `AWS_ENDPOINT_URL_S3` or `AWS_ENDPOINT_URL` in the environment, when running `oxigraph` without the runner | The runner unsets `AWS_ENDPOINT_URL_S3`; otherwise unset both yourself |
| `A Tokio 1.x context was found, but IO is disabled` | A build from before the runtime bridge gained its IO driver | Rebuild from this branch |
| The store is empty after `load` | A different `S3_BASE_PATH` or bucket than the server's | Compare the `--object-store` URLs of `--dry-run` for both commands |
| Two processes, one database: corruption or a failed commit | SlateDB is single-writer | Stop the other writer; give each deployment its own prefix |

## 11. Verifying a deployment

```bash
# 1. The plan is the one you expect: URL, endpoint, region, path-style.
oxigraph-s3 --dry-run

# 2. The server answers, and the store it opened is the expected one.
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:7878/
curl -s http://127.0.0.1:7878/query \
  -H 'content-type: application/sparql-query' \
  --data 'SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }'

# 3. The objects are under the expected prefix.
aws --endpoint-url http://127.0.0.1:9000 s3 ls --recursive s3://oxigraph/slatedb/
```

A check of the S3 wiring that needs no credentials: point `S3_HOST`/`S3_PORT` at a plain HTTP
listener (`python3 -m http.server 9123`) and watch the first request the CLI sends,
`GET /<bucket>?list-type=2&prefix=<base path>/manifest/`. The path-style bucket and the prefix in
that line are the two things that go wrong most often.

## 12. Limits of the runner

It stays a `.env`-to-environment translator. Deliberate limits, with the escape hatch for each:

| Not covered | Use instead |
| --- | --- |
| Creating the bucket, lifecycle rules, bucket policies | An S3 client or the provider's console |
| A custom TLS root, a proxy, a timeout, one client per database | `SlateDbOptions::at_object_store` in Rust — `docs/slatedb.md`, "A client built in code" |
| Credentials from a secret manager, refreshed tokens | Inject real `AWS_*` variables and call `oxigraph` directly |
| Virtual-hosted-style addressing (`bucket.host`) | `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` is forced to `false`; use the Rust client |
| Percent-encoded or non-ASCII prefixes | Only URL-safe characters are accepted |
| RocksDB locations (`--location`) | `oxigraph` directly |
| Retry, flush and compression tuning | `--slatedb-config` (the runner forwards it) |
| Multi-writer or shared-prefix deployments | SlateDB is single-writer: one prefix, one writer |

## 13. Where the code is

| Path | Contents |
| --- | --- |
| `tools/s3-runner/src/main.rs` | Argument parsing, `S3Config`, `Plan`, validation, 13 unit tests |
| `tools/s3-runner/.env.example` | The template, embedded with `include_str!` and written by `--init` |
| `tools/s3-runner/README.md` | Short reference: options, variables, what is exported |
| `lib/oxigraph/src/storage/slatedb_bridge.rs` | Tokio runtime bridge; `.enable_io()` is what lets the S3 client open sockets |
| `docs/slatedb.md` | The backend: locations, credentials, settings, retries, migration |
| `cli/src/cli.rs` | `--object-store` and `--slatedb-config` of each command |
