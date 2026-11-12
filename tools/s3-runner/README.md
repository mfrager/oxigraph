# oxigraph-s3 runner

`oxigraph-s3` runs the [oxigraph CLI](../../cli) on a SlateDB database held in an S3 compatible
object store, with the connection settings kept in a `.env` file: host, port, bucket, base path,
access key and secret key. It is a thin wrapper, for development and for deployments that would
rather ship a `.env` file than a shell script full of exports. It is not published on crates.io,
and `oxigraph` stays the executable that owns the database.

## Usage

```bash
# Write the template next to where the .env file goes, then fill in the key pair.
cargo run -p oxigraph-s3-runner -- --init
cp .env.example .env

# Everything after the runner options goes to oxigraph, `serve` being the default command.
cargo run -p oxigraph-s3-runner -- serve --bind 0.0.0.0:7878

# The same, with an oxigraph built by this workspace.
cargo run -p oxigraph-s3-runner -- --bin target/debug/oxigraph serve
```

`--dry-run` prints the command and the variables it would run with, and hides the credentials:

```text
oxigraph serve --bind 0.0.0.0:7878 --object-store s3://oxigraph/slatedb
  AWS_ACCESS_KEY_ID=<hidden>
  ...
  AWS_ENDPOINT=http://127.0.0.1:9000
  AWS_ALLOW_HTTP=true
```

## Options

| Option              | Meaning                                                                    |
| ------------------- | -------------------------------------------------------------------------- |
| `--env-file <PATH>` | The file to read, `.env` being the default (found from the current directory upward) |
| `--init`            | Writes the template as `<env file>.example` and exits                      |
| `--force`           | Replaces an existing template                                              |
| `--dry-run`         | Prints the command and the variables instead of running it                 |
| `--bin <PATH>`      | The oxigraph executable, also `OXIGRAPH_BIN`, `oxigraph` on the `PATH` by default |

## The `.env` file

| Variable          | Meaning                                                                 |
| ----------------- | ----------------------------------------------------------------------- |
| `S3_HOST`         | Endpoint host, without scheme or port                                   |
| `S3_PORT`         | Endpoint port, empty for the default port of the scheme                 |
| `S3_SCHEME`       | `http` (RustFS, MinIO) or `https` (AWS S3), `http` by default           |
| `S3_BUCKET`       | Bucket to use, it has to exist                                          |
| `S3_BASE_PATH`    | Path of the database inside the bucket, empty for its root              |
| `S3_ACCESS_KEY`   | Access key                                                              |
| `S3_SECRET_KEY`   | Secret key                                                              |
| `S3_SESSION_TOKEN`| Session token, when the keys are temporary                              |
| `S3_REGION`       | Region, `us-east-1` by default                                          |

The variables of the environment win over the file, so a single value can be overridden without
editing it. `serve`, `serve-read-only`, `load`, `backup` and `migrate` are the commands the runner
configures, since they are the ones that accept `--object-store`; passing `--object-store` yourself
keeps the URL as given, and `--location` is refused because the runner does not configure the local
backend.

## What it exports

`S3_HOST`, `S3_PORT` and `S3_SCHEME` become `AWS_ENDPOINT` (`AWS_ENDPOINT_URL` as well), and
`AWS_ALLOW_HTTP` follows the scheme. The credentials become `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`, the region both `AWS_REGION` and
`AWS_DEFAULT_REGION`, and `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false` keeps the path-style addressing
that the compatible stores need. `AWS_ENDPOINT_URL_S3`, which would take precedence over the
endpoint of the file, is removed from the environment of the child process.
