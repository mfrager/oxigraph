#![expect(clippy::print_stderr, clippy::print_stdout)]

//! Runs the `oxigraph` CLI on a SlateDB database held in an S3 compatible object store, with the
//! connection settings read from a `.env` file.
//!
//! The settings of the file are turned into the `AWS_*` variables that the S3 client of SlateDB
//! reads, and the database is opened with `--object-store s3://<bucket>/<base path>`. Everything
//! else is passed to `oxigraph` as is:
//!
//! ```sh
//! oxigraph-s3 serve --bind 0.0.0.0:7878
//! oxigraph-s3 --dry-run load dump.nq
//! ```
//!
//! `oxigraph-s3 --init` writes the template of the `.env` file.

use anyhow::{Context, anyhow, bail, ensure};
use clap::Parser;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, ExitStatus};

/// The template written by `--init`: the documentation of the `.env` file format.
const TEMPLATE: &str = include_str!("../.env.example");

/// The command of `oxigraph` run when none is given.
const DEFAULT_COMMAND: &str = "serve";

/// The `oxigraph` commands that accept `--object-store`.
const STORE_COMMANDS: &[&str] = &["serve", "serve-read-only", "load", "backup", "migrate"];

/// The region used when `S3_REGION` is not set.
const DEFAULT_REGION: &str = "us-east-1";

/// The variable that would take precedence over `AWS_ENDPOINT`, and so over the `.env` file.
const ENDPOINT_OVERRIDE: &str = "AWS_ENDPOINT_URL_S3";

#[derive(Debug, Parser)]
#[command(
    name = "oxigraph-s3",
    version,
    about = "Runs oxigraph on an S3 backed SlateDB database configured by a .env file",
    after_help = "The arguments are passed to `oxigraph`, `serve` being the default command. The \
                  `oxigraph` arguments have to come last: `oxigraph-s3 --dry-run serve --bind \
                  0.0.0.0:7878`."
)]
struct RunnerArgs {
    /// The .env file to read, `.env` being the default.
    #[arg(long, value_name = "PATH")]
    env_file: Option<PathBuf>,
    /// Writes the .env template next to the env file, as `.env.example`, and exits.
    #[arg(long)]
    init: bool,
    /// Overwrites the .env template when it already exists.
    #[arg(long, requires = "init")]
    force: bool,
    /// Prints the command and the variables it would run with, instead of running it.
    #[arg(long)]
    dry_run: bool,
    /// The oxigraph executable to run.
    #[arg(
        long,
        value_name = "PATH",
        env = "OXIGRAPH_BIN",
        default_value = "oxigraph"
    )]
    bin: PathBuf,
    /// The command and options of oxigraph, for example `serve --bind 0.0.0.0:7878`.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    arguments: Vec<String>,
}

fn main() -> ExitCode {
    match run(&RunnerArgs::parse()) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &RunnerArgs) -> anyhow::Result<ExitCode> {
    let env_file = args
        .env_file
        .clone()
        .unwrap_or_else(|| PathBuf::from(".env"));
    if args.init {
        write_template(&env_file, args.force)?;
        return Ok(ExitCode::SUCCESS);
    }
    let arguments = forwarded_arguments(&args.arguments)?;
    load_env(args.env_file.as_deref())?;
    let config = S3Config::from_env()?;
    let plan = Plan::new(args.bin.clone(), arguments, &config);
    if args.dry_run {
        plan.print();
        return Ok(ExitCode::SUCCESS);
    }
    plan.run()
}

/// Writes the `.env` template, refusing to replace an existing one unless `force` is set.
fn write_template(env_file: &Path, force: bool) -> anyhow::Result<()> {
    let template = template_path(env_file);
    ensure!(
        force || !template.exists(),
        "{} already exists: pass `--force` to replace it",
        template.display()
    );
    if let Some(parent) = template
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("Could not create the directory {}", parent.display()))?;
    }
    fs::write(&template, TEMPLATE)
        .with_context(|| format!("Could not write the template {}", template.display()))?;
    eprintln!(
        "Wrote the S3 template to {}.\nCopy it to {}, fill in the values, then run `oxigraph-s3 serve`.",
        template.display(),
        env_file.display()
    );
    Ok(())
}

/// The path of the template of an env file: `.env.example` for `.env`.
fn template_path(env_file: &Path) -> PathBuf {
    let mut path = env_file.as_os_str().to_os_string();
    path.push(".example");
    PathBuf::from(path)
}

/// Reads the `.env` file into the environment, without overriding variables that are already set.
fn load_env(env_file: Option<&Path>) -> anyhow::Result<()> {
    let Some(env_file) = env_file else {
        return dotenvy::dotenv().map(|_| ()).map_err(|error| {
            if error.not_found() {
                anyhow!(
                    "No .env file was found from the current directory upward: run \
                     `oxigraph-s3 --init` to write a template, copy it to `.env` and fill it in"
                )
            } else {
                anyhow::Error::new(error).context("Could not read the .env file")
            }
        });
    };
    ensure!(
        env_file.is_file(),
        "The .env file {} does not exist: run `oxigraph-s3 --env-file {} --init` to write a template",
        env_file.display(),
        env_file.display()
    );
    dotenvy::from_path(env_file)
        .with_context(|| format!("Could not read the .env file {}", env_file.display()))
}

/// Checks the arguments passed to oxigraph and returns the command to run, `serve` being the
/// default.
fn forwarded_arguments(arguments: &[String]) -> anyhow::Result<Vec<String>> {
    ensure!(
        !has_option(arguments, "--location"),
        "`--location` selects the local backend of oxigraph, which this runner does not configure: \
         run `oxigraph --location` directly, or pass `--object-store` to choose another object store"
    );
    let arguments: Vec<String> = if arguments.is_empty() {
        vec![DEFAULT_COMMAND.to_owned()]
    } else {
        arguments.to_vec()
    };
    if !has_option(&arguments, "--object-store") {
        let command = arguments.first().map_or(DEFAULT_COMMAND, String::as_str);
        ensure!(
            STORE_COMMANDS.contains(&command),
            "`{command}` does not accept `--object-store`: this runner configures the object store \
             of `serve`, `serve-read-only`, `load`, `backup` and `migrate`"
        );
    }
    Ok(arguments)
}

/// Whether `--option` or `--option=value` is one of `arguments`.
fn has_option(arguments: &[String], option: &str) -> bool {
    arguments.iter().any(|argument| {
        argument == option
            || argument
                .strip_prefix(option)
                .is_some_and(|value| value.starts_with('='))
    })
}

/// The connection settings of the S3 endpoint, read from the `.env` file.
#[derive(Debug)]
struct S3Config {
    scheme: String,
    host: String,
    port: Option<u16>,
    bucket: String,
    base_path: Option<String>,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    region: String,
}

impl S3Config {
    fn from_env() -> anyhow::Result<Self> {
        let scheme = match optional("S3_SCHEME")? {
            Some(scheme) => scheme.to_ascii_lowercase(),
            None => "http".to_owned(),
        };
        ensure!(
            scheme == "http" || scheme == "https",
            "S3_SCHEME must be `http` or `https`, not `{scheme}`"
        );
        let host = required("S3_HOST")?;
        ensure!(
            !host.contains("://"),
            "S3_HOST must not include a scheme, `{host}`: set S3_SCHEME to `http` or `https`"
        );
        ensure!(
            !host.contains('/'),
            "S3_HOST must not include a path, `{host}`: put the path of the database in S3_BASE_PATH"
        );
        let port = match optional("S3_PORT")? {
            Some(port) => Some(port.parse().with_context(|| {
                format!("S3_PORT must be a port number between 0 and 65535, not `{port}`")
            })?),
            None => None,
        };
        let bucket = required("S3_BUCKET")?;
        validate_bucket(&bucket)?;
        let base_path = match optional("S3_BASE_PATH")? {
            Some(base_path) => {
                let base_path = base_path.trim_matches('/').to_owned();
                if base_path.is_empty() {
                    None
                } else {
                    validate_base_path(&base_path)?;
                    Some(base_path)
                }
            }
            None => None,
        };
        Ok(Self {
            scheme,
            host,
            port,
            bucket,
            base_path,
            access_key: required("S3_ACCESS_KEY")?,
            secret_key: required("S3_SECRET_KEY")?,
            session_token: optional("S3_SESSION_TOKEN")?,
            region: optional("S3_REGION")?.unwrap_or_else(|| DEFAULT_REGION.to_owned()),
        })
    }

    /// The URL of the database, given to `--object-store`.
    fn location(&self) -> String {
        match &self.base_path {
            Some(base_path) => format!("s3://{}/{base_path}", self.bucket),
            None => format!("s3://{}", self.bucket),
        }
    }

    /// The endpoint the S3 client connects to.
    fn endpoint(&self) -> String {
        match self.port {
            Some(port) => format!("{}://{}:{port}", self.scheme, self.host),
            None => format!("{}://{}", self.scheme, self.host),
        }
    }

    /// The variables `AmazonS3Builder::from_env` reads.
    fn environment(&self) -> Vec<(&'static str, String)> {
        let mut environment = vec![
            ("AWS_ACCESS_KEY_ID", self.access_key.clone()),
            ("AWS_SECRET_ACCESS_KEY", self.secret_key.clone()),
            // Both are read, and the client does not define which one wins.
            ("AWS_REGION", self.region.clone()),
            ("AWS_DEFAULT_REGION", self.region.clone()),
            ("AWS_ENDPOINT", self.endpoint()),
            ("AWS_ENDPOINT_URL", self.endpoint()),
            (
                "AWS_ALLOW_HTTP",
                if self.scheme == "http" {
                    "true"
                } else {
                    "false"
                }
                .to_owned(),
            ),
            // RustFS, MinIO and the other compatible stores need path-style addressing.
            ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false".to_owned()),
        ];
        if let Some(session_token) = &self.session_token {
            environment.push(("AWS_SESSION_TOKEN", session_token.clone()));
        }
        environment
    }

    /// The variables of the parent environment that would contradict the settings above.
    fn overridden_variables(&self) -> Vec<&'static str> {
        let mut overridden = vec![ENDPOINT_OVERRIDE];
        if self.session_token.is_none() {
            // A token inherited from the parent belongs to its own credentials, not to ours.
            overridden.push("AWS_SESSION_TOKEN");
        }
        overridden
    }
}

/// Everything the runner does, computed before anything is run or printed.
#[derive(Debug)]
struct Plan {
    program: PathBuf,
    arguments: Vec<String>,
    environment: Vec<(&'static str, String)>,
    overridden: Vec<&'static str>,
}

impl Plan {
    fn new(program: PathBuf, mut arguments: Vec<String>, config: &S3Config) -> Self {
        if !has_option(&arguments, "--object-store") {
            arguments.push("--object-store".to_owned());
            arguments.push(config.location());
        }
        Self {
            program,
            arguments,
            environment: config.environment(),
            overridden: config.overridden_variables(),
        }
    }

    fn print(&self) {
        let command = std::iter::once(self.program.display().to_string())
            .chain(self.arguments.iter().cloned())
            .collect::<Vec<_>>()
            .join(" ");
        println!("{command}");
        for (name, value) in &self.environment {
            let value = if is_secret(name) { "<hidden>" } else { value };
            println!("  {name}={value}");
        }
        for name in &self.overridden {
            println!("  {name} (unset)");
        }
    }

    fn run(&self) -> anyhow::Result<ExitCode> {
        let mut command = Command::new(&self.program);
        command.args(&self.arguments);
        for (name, value) in &self.environment {
            command.env(name, value);
        }
        for name in &self.overridden {
            command.env_remove(name);
        }
        let status = command.status().with_context(|| {
            format!(
                "Could not run the oxigraph executable {}: pass `--bin` or set OXIGRAPH_BIN to its path",
                self.program.display()
            )
        })?;
        Ok(exit_code(status))
    }
}

/// The exit code of oxigraph, so that the runner fails like it does.
fn exit_code(status: ExitStatus) -> ExitCode {
    status
        .code()
        .and_then(|code| u8::try_from(code).ok())
        .map_or(ExitCode::FAILURE, ExitCode::from)
}

fn is_secret(name: &str) -> bool {
    matches!(
        name,
        "AWS_ACCESS_KEY_ID" | "AWS_SECRET_ACCESS_KEY" | "AWS_SESSION_TOKEN"
    )
}

/// Reads a variable, treating an empty value as unset.
fn optional(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) => {
            let value = value.trim();
            Ok((!value.is_empty()).then(|| value.to_owned()))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error @ env::VarError::NotUnicode(_)) => {
            Err(anyhow::Error::new(error).context(format!("{name} is not valid UTF-8")))
        }
    }
}

/// Reads a variable that has no sensible default.
fn required(name: &str) -> anyhow::Result<String> {
    optional(name)?.with_context(|| {
        format!(
            "{name} is not set: add it to the .env file, see the template of `oxigraph-s3 --init`"
        )
    })
}

fn validate_bucket(bucket: &str) -> anyhow::Result<()> {
    ensure!(
        (3..=63).contains(&bucket.len()),
        "S3_BUCKET must be between 3 and 63 characters long, not `{bucket}`"
    );
    ensure!(
        bucket.chars().all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '-' | '.')
        }),
        "S3_BUCKET must only contain lowercase letters, digits, `.` and `-`, not `{bucket}`"
    );
    Ok(())
}

fn validate_base_path(base_path: &str) -> anyhow::Result<()> {
    if let Some(character) = base_path
        .chars()
        .find(|character| !is_base_path_character(*character))
    {
        bail!(
            "S3_BASE_PATH must only contain letters, digits, `.`, `-`, `_`, `~` and `/`, but \
             `{base_path}` contains `{character}`: the path goes into a URL and is not \
             percent-decoded"
        );
    }
    Ok(())
}

fn is_base_path_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_' | '~' | '/')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> S3Config {
        S3Config {
            scheme: "http".to_owned(),
            host: "127.0.0.1".to_owned(),
            port: Some(9000),
            bucket: "oxigraph".to_owned(),
            base_path: Some("slatedb".to_owned()),
            access_key: "access".to_owned(),
            secret_key: "secret".to_owned(),
            session_token: None,
            region: DEFAULT_REGION.to_owned(),
        }
    }

    fn plan_for(arguments: &[&str]) -> anyhow::Result<Plan> {
        let arguments = arguments
            .iter()
            .map(|argument| (*argument).to_owned())
            .collect::<Vec<_>>();
        Ok(Plan::new(
            PathBuf::from("oxigraph"),
            forwarded_arguments(&arguments)?,
            &config(),
        ))
    }

    fn variable<'a>(environment: &'a [(&'static str, String)], name: &str) -> Option<&'a str> {
        environment
            .iter()
            .find(|(variable, _)| *variable == name)
            .map(|(_, value)| value.as_str())
    }

    #[test]
    fn the_location_is_the_bucket_and_the_base_path() {
        assert_eq!(config().location(), "s3://oxigraph/slatedb");
    }

    #[test]
    fn an_empty_base_path_is_the_root_of_the_bucket() {
        let mut config = config();
        config.base_path = None;
        assert_eq!(config.location(), "s3://oxigraph");
    }

    #[test]
    fn the_endpoint_is_the_scheme_the_host_and_the_port() {
        assert_eq!(config().endpoint(), "http://127.0.0.1:9000");
        let mut config = config();
        config.scheme = "https".to_owned();
        config.port = None;
        assert_eq!(config.endpoint(), "https://127.0.0.1");
    }

    #[test]
    fn the_environment_describes_the_endpoint_and_the_credentials() {
        let environment = config().environment();
        assert_eq!(
            variable(&environment, "AWS_ENDPOINT"),
            Some("http://127.0.0.1:9000")
        );
        assert_eq!(variable(&environment, "AWS_ACCESS_KEY_ID"), Some("access"));
        assert_eq!(
            variable(&environment, "AWS_SECRET_ACCESS_KEY"),
            Some("secret")
        );
        assert_eq!(variable(&environment, "AWS_REGION"), Some(DEFAULT_REGION));
        assert_eq!(variable(&environment, "AWS_ALLOW_HTTP"), Some("true"));
        assert_eq!(
            variable(&environment, "AWS_VIRTUAL_HOSTED_STYLE_REQUEST"),
            Some("false")
        );
        assert_eq!(variable(&environment, "AWS_SESSION_TOKEN"), None);
    }

    #[test]
    fn https_forbids_plain_http_and_a_session_token_is_forwarded() {
        let mut config = config();
        config.scheme = "https".to_owned();
        config.session_token = Some("token".to_owned());
        let environment = config.environment();
        assert_eq!(variable(&environment, "AWS_ALLOW_HTTP"), Some("false"));
        assert_eq!(variable(&environment, "AWS_SESSION_TOKEN"), Some("token"));
    }

    #[test]
    fn an_inherited_endpoint_override_is_removed() {
        assert!(config().overridden_variables().contains(&ENDPOINT_OVERRIDE));
        let mut config = config();
        config.session_token = Some("token".to_owned());
        assert!(!config.overridden_variables().contains(&"AWS_SESSION_TOKEN"));
    }

    #[test]
    fn serve_is_the_default_and_gets_the_object_store() {
        let plan = plan_for(&[]).unwrap();
        assert_eq!(
            plan.arguments.join(" "),
            "serve --object-store s3://oxigraph/slatedb"
        );
    }

    #[test]
    fn the_arguments_of_the_caller_come_first() {
        let plan = plan_for(&["serve", "--bind", "0.0.0.0:7878"]).unwrap();
        assert_eq!(
            plan.arguments.join(" "),
            "serve --bind 0.0.0.0:7878 --object-store s3://oxigraph/slatedb"
        );
        let plan = plan_for(&["load", "dump.nq"]).unwrap();
        assert_eq!(
            plan.arguments.join(" "),
            "load dump.nq --object-store s3://oxigraph/slatedb"
        );
    }

    #[test]
    fn an_object_store_of_the_caller_is_kept() {
        let plan = plan_for(&["serve", "--object-store", "s3://other/db"]).unwrap();
        assert_eq!(
            plan.arguments.join(" "),
            "serve --object-store s3://other/db"
        );
        let plan = plan_for(&["serve", "--object-store=s3://other/db"]).unwrap();
        assert_eq!(
            plan.arguments.join(" "),
            "serve --object-store=s3://other/db"
        );
    }

    #[test]
    fn a_local_database_is_refused() {
        for arguments in [
            &["serve", "--location", "db"][..],
            &["serve", "--location=db"],
        ] {
            let error = plan_for(arguments).unwrap_err().to_string();
            assert!(error.contains("--location"), "{error}");
        }
    }

    #[test]
    fn a_command_without_an_object_store_is_refused() {
        let error = plan_for(&["query", "SELECT * WHERE {}"])
            .unwrap_err()
            .to_string();
        assert!(error.contains("`query` does not accept"), "{error}");
    }

    #[test]
    fn the_template_lists_every_setting() {
        for setting in [
            "S3_HOST",
            "S3_PORT",
            "S3_BUCKET",
            "S3_BASE_PATH",
            "S3_ACCESS_KEY",
            "S3_SECRET_KEY",
            "S3_SCHEME",
            "S3_REGION",
        ] {
            assert!(
                TEMPLATE.contains(setting),
                "{setting} is missing from the template"
            );
        }
    }

    #[test]
    fn the_template_of_an_env_file_sits_next_to_it() {
        assert_eq!(template_path(Path::new(".env")), Path::new(".env.example"));
        assert_eq!(
            template_path(Path::new("conf/dev.env")),
            Path::new("conf/dev.env.example")
        );
    }
}
