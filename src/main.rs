#![warn(clippy::all, clippy::nursery, clippy::pedantic)]
// This nursery lint misfires on values held across `.await` points and moved
// into later calls (such as byte-budget permits and output handles), where an
// earlier drop is impossible or would change behavior.
#![allow(clippy::significant_drop_tightening)]
use std::{
    collections::HashSet,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use clap::{ArgAction, CommandFactory, Parser};
use clap_complete::{Shell, generate};
use redis_monitor::commands::{self, Categories, Flags};
use tokio::{sync::watch, task::JoinSet};

use crate::{
    config::{Map, ServerAuth},
    connection::{Cluster, Monitor, ServerAddr, TlsConfig},
    filter::{Filter, FilterPattern, LineFilter},
    output::{Formatter, OutputKind},
    pipeline::{BatchConfig, IoMessage, OUTPUT_BYTE_BUDGET, Pipeline},
};

mod config;
mod connection;
mod filter;
mod output;
mod pipeline;
mod stats;

#[derive(Parser, Debug)]
#[command(
    name = "redis-monitor",
    about = "A utility to monitor one or more RESP compatible servers",
    after_help = r#"Format specifiers:
  %S   Short form of server and client address
  %sa  Full address of the server (host:port or unix path)
  %sh  Host part of the server address
  %sp  Port part of the server address (or basename of unix path)
  %sn  Name of the server instance if it is set
  %ca  Full address of the client (ip:port or unix path)
  %ch  Host part of the client address
  %cp  Port part of the client address (or basename of unix path)
  %d   The database number
  %t   The timestamp as reported by MONITOR
  %l   The full command and all arguments
  %C   Argument 0 (the command)
  %a   Arguments 1..N
  %%   A literal percent sign

  The default formats are:
    Single instance:    "%t [%d %ca] %l";
    Multiple Instances: "%t [%S %d] %l";

Examples:
  # Monitor localhost:6379 by default
  redis-monitor

  # Monitor a cluster expecting one node to be 127.0.0.1:6379
  redis-monitor -c 6379

  # Monitor two standalone instances
  redis-monitor host1:6379 host2:6379

  # Run while filtering specific commands
  redis-monitor --filter get --filter set
  redis-monitor --filter '!get' --filter '!set'
  redis-monitor --filter '/(?i)^geo/'

  # Match keys, excluding any command touching a private key
  redis-monitor --key-filter '/^user:/' --key-filter '!private'

  # Index command arguments or discovered keys (both zero-based)
  redis-monitor --filter '[2]/^bar$/'
  redis-monitor --key-filter '[1]=[0]literal-key'

  # Filtering by command flags and categories
  redis-monitor --flags write,@hash"#
)]
#[allow(clippy::struct_excessive_bools)]
struct Options {
    #[command(subcommand)]
    cmd: Option<Cmd>,

    #[arg(short, long, help = "Treat each instance like its a cluster seed")]
    cluster: bool,

    #[arg(short, long, help = "How to format each MONITOR line")]
    format: Option<String>,

    #[arg(short, long, help = "Also connect and MONITOR cluster replicas")]
    replicas: bool,

    #[arg(long)]
    config_file: Option<PathBuf>,

    #[arg(long, help = "Disable colored output")]
    no_color: bool,

    #[arg(long, help = "Only show commands for a specific database")]
    db: Option<u64>,

    #[arg(short, long, help = "Redis user")]
    user: Option<String>,

    #[arg(short, long, short_alias = 'a', help = "Redis password")]
    pass: Option<String>,

    #[clap(long, action = clap::ArgAction::Append,
           help = "Filter command names, or [N] arguments (command is 0); /regex/, =literal, !exclude")]
    filter: Vec<FilterPattern>,

    #[arg(long, action = ArgAction::Append, value_name = "PATTERN",
        help = "Filter keys, or [N]th key (first key is 0); /regex/, =literal, !exclude")]
    key_filter: Vec<FilterPattern>,

    #[arg(
        long,
        action = ArgAction::Append,
        value_name = "FLAG|@CATEGORY",
        help = "Require flags (e.g. write) and/or categories (e.g. @hash)"
    )]
    flags: Vec<String>,

    #[arg(
        short,
        long,
        default_value = "plain",
        help = "How to serialize the output. Values: plain, json, php, csv, resp"
    )]
    output: OutputKind,

    #[arg(long, help = "Connect using TLS")]
    tls: bool,

    #[arg(long, help = "Disable TLS certificate verification")]
    insecure: bool,

    #[arg(long, help = "Path to CA cert for TLS")]
    tls_ca: Option<PathBuf>,

    #[arg(long, help = "Path to client cert for TLS")]
    tls_cert: Option<PathBuf>,

    #[arg(long, help = "Path to client private key for TLS")]
    tls_key: Option<PathBuf>,

    #[arg(short, long, help = "Display the version and exit")]
    version: bool,

    #[arg(
        long,
        value_name = "SECONDS",
        value_parser = parse_interval,
        help = "Periodically report per-command statistics (plain output only)"
    )]
    stats: Option<Duration>,

    #[arg(long, help = "Read from stdin instead of connecting to servers")]
    stdin: bool,

    #[arg(
        long,
        help = "Enable producer batching for higher throughput (may delay records by 5 ms)"
    )]
    batch: bool,

    #[arg(
        long,
        value_name = "N",
        value_parser = clap::value_parser!(u16).range(1..),
        help = "Worker threads for reading, filtering, and formatting \
                (default: available CPUs, at most 16)"
    )]
    threads: Option<u16>,

    #[arg(
        long,
        help = "Output debug information such as detailed filter info"
    )]
    debug: bool,

    pub instances: Vec<String>,
}

#[derive(clap::Subcommand, Debug)]
enum Cmd {
    #[command(about = "Generate shell completion scripts")]
    Completions {
        #[arg(value_enum, help = "The shell to generate completions for")]
        shell: Shell,
    },
}

const VERSION: &str = env!("CARGO_PKG_VERSION");
const GIT_HASH: &str = env!("GIT_HASH");
const GIT_DIRTY: &str = env!("GIT_DIRTY");

const DEFAULT_SINGLE_FORMAT: &str = "%t [%d %ca] %l";
const DEFAULT_MULTI_FORMAT: &str = "%t [%S %d] %l";
/// Idle workers are cheap, but beyond this more rarely helps: the output
/// thread becomes the limit first.
const DEFAULT_MAX_THREADS: usize = 16;

fn parse_interval(s: &str) -> Result<Duration> {
    let secs: f64 = s.parse().context("Invalid number")?;
    if secs.is_nan() || secs <= 0.0 {
        bail!("Value must be positive");
    }
    Duration::try_from_secs_f64(secs).map_err(|_| anyhow!("Value is too large"))
}

impl Options {
    fn parse_flags<'a, I>(it: I) -> Result<commands::Filter>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut flags = Flags::empty();
        let mut acl = Categories::empty();

        for raw in it {
            for tok in raw.split(',') {
                let s = tok.trim();
                if s.is_empty() {
                    continue;
                }

                if let Ok(c) = Categories::from_str(s) {
                    acl |= c;
                } else if let Ok(f) = Flags::from_str(s) {
                    flags |= f;
                } else {
                    bail!("Unknown command flag or @category '{s}'");
                }
            }
        }

        Ok(commands::Filter {
            flags: (!flags.is_empty()).then_some(flags),
            categories: (!acl.is_empty()).then_some(acl),
        })
    }

    fn get_tls_config(&self) -> Result<Option<Arc<TlsConfig>>> {
        if self.tls {
            Ok(Some(Arc::new(TlsConfig::new(
                self.insecure,
                self.tls_ca.as_deref(),
                self.tls_cert.as_deref(),
                self.tls_key.as_deref(),
            )?)))
        } else {
            Ok(None)
        }
    }

    fn get_server_auth(&self) -> ServerAuth {
        ServerAuth::from_user_pass(self.user.as_deref(), self.pass.as_deref())
    }

    fn line_filter(&self) -> Result<LineFilter> {
        let names = Filter::for_command(self.filter.clone())?;
        let flags = Self::parse_flags(self.flags.iter().map(String::as_str))?;
        let keys = Filter::try_from(self.key_filter.clone())?;
        Ok(LineFilter::new(self.db, names, keys, flags))
    }

    /// Per-command statistics apply to plain output only.
    fn stats_interval(&self) -> Option<Duration> {
        self.stats.filter(|_| self.output == OutputKind::Plain)
    }

    fn worker_threads(&self) -> usize {
        self.threads.map_or_else(
            || {
                std::thread::available_parallelism()
                    .map_or(1, std::num::NonZero::get)
                    .min(DEFAULT_MAX_THREADS)
            },
            usize::from,
        )
    }
}

// Treat each instance as a cluster seed. Seeds of the same cluster discover
// the same nodes, so each node address is monitored only once.
fn process_cluster_instances(
    opt: &Options,
    tls: Option<&Arc<TlsConfig>>,
    auth: &ServerAuth,
) -> Result<Vec<Monitor>> {
    let mut seen = HashSet::new();
    let mut monitors = Vec::new();
    let mut add = |id: &str, addr: &ServerAddr| {
        if seen.insert(addr.clone()) {
            monitors.push(Monitor::new(
                Some(id),
                addr.clone(),
                tls.cloned(),
                auth.clone(),
            ));
        }
    };

    for input in &opt.instances {
        let address = ServerAddr::from_str(input).with_context(|| {
            format!("Invalid Redis Cluster seed address '{input}'")
        })?;
        let cluster = Cluster::from_seed(&address, auth, tls.map(Arc::as_ref))
            .with_context(|| {
                format!(
                    "Failed to discover a Redis Cluster from seed '{input}' \
                 (resolved to {address}). The seed must be reachable and have \
                 Redis Cluster enabled; remove --cluster to monitor a \
                 standalone instance"
                )
            })?;

        for primary in cluster.get_nodes() {
            add(&primary.id, &primary.addr);
            if opt.replicas {
                for replica in &primary.replicas {
                    add(&replica.id, &replica.addr);
                }
            }
        }
    }

    Ok(monitors)
}

// Take the array of instances provided on the command line and attempt to map
// them to one or more instances. These can either be named instances like
// mycluster` which were loaded from our config file, or be in some parsable
// form like "host:port", or "redis://...".
fn process_instances(
    cfg: &Map,
    opt: &Options,
    tls: Option<&Arc<TlsConfig>>,
    auth: &ServerAuth,
) -> Result<Vec<Monitor>> {
    let mut monitors = Vec::new();

    for instance in &opt.instances {
        if let Some(entry) = cfg.get(instance) {
            monitors.extend(Monitor::from_config_entry(instance, entry)?);
        } else {
            let address = ServerAddr::from_str(instance).with_context(|| {
                format!(
                    "Unable to parse '{instance}' as a Redis address, and no \
                     configuration entry with that name exists"
                )
            })?;
            monitors.push(Monitor::new(
                None,
                address,
                tls.cloned(),
                auth.clone(),
            ));
        }
    }

    Ok(monitors)
}

fn version_string() -> String {
    let git_display = format!(
        "{GIT_HASH}{}",
        if GIT_DIRTY == "yes" { "-dirty" } else { "" }
    );

    format!("redis-monitor v{VERSION} (git {git_display})")
}

fn format_preamble(monitors: &[Monitor]) -> String {
    let addresses = monitors
        .iter()
        .map(|m| m.address.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    format!("MONITOR: {addresses}")
}

/// Start the output thread and build the settings shared by every source.
fn start_pipeline(
    opt: &Options,
    format: &str,
    shutdown: &watch::Sender<bool>,
) -> Result<(Pipeline, std::thread::JoinHandle<Result<()>>)> {
    let filter = opt.line_filter()?;
    let formatter = Arc::new(Formatter::new(opt.output, format));
    let batch = BatchConfig::new(opt.batch);
    let stats = opt
        .stats_interval()
        .map(|interval| (interval, Arc::new(Mutex::default())));

    let (io, output) = pipeline::start_output(
        formatter.header(),
        batch.queue_capacity(),
        OUTPUT_BYTE_BUDGET,
        stats.clone(),
        shutdown.clone(),
    )?;

    let pipeline = Pipeline {
        io,
        filter,
        formatter,
        batch,
        stats: stats.map(|(_, shared)| shared),
    };
    Ok((pipeline, output))
}

async fn run_stdin(opt: Options, shutdown: watch::Sender<bool>) -> Result<()> {
    if !opt.key_filter.is_empty() {
        bail!(
            "--key-filter needs COMMAND metadata from a live server and cannot \
             be used with --stdin"
        );
    }
    if !opt.flags.is_empty() {
        bail!(
            "--flags needs COMMAND metadata from a live server and cannot be \
             used with --stdin"
        );
    }

    let format = opt.format.as_deref().unwrap_or(DEFAULT_SINGLE_FORMAT);
    let (pipeline, output) = start_pipeline(&opt, format, &shutdown)?;
    let io = pipeline.io.clone();

    io.send(IoMessage::Preamble("MONITOR: stdin".into()))
        .await?;
    pipeline::run_from_reader(
        "stdin",
        pipeline::ReadAhead::spawn(std::io::stdin())?,
        pipeline,
        shutdown.subscribe(),
    )
    .await;

    pipeline::finish_output(io, output).await?;
    pipeline::print_final_stats();

    Ok(())
}

async fn run_wire(opt: Options, shutdown: watch::Sender<bool>) -> Result<()> {
    // Validate filters before connecting to anything.
    opt.line_filter()?;
    let cfg = Map::load(opt.config_file.as_deref())?;
    let instances: Vec<String> = if opt.instances.is_empty() {
        vec!["localhost:6379".to_string()]
    } else {
        opt.instances.clone()
    };

    let tls = opt.get_tls_config()?;
    let auth = opt.get_server_auth();
    let opt = Options { instances, ..opt };

    // Cluster discovery uses blocking connections. It runs before any source
    // task starts, so there is nothing else for it to block.
    let monitors = if opt.cluster {
        process_cluster_instances(&opt, tls.as_ref(), &auth)?
    } else {
        process_instances(&cfg, &opt, tls.as_ref(), &auth)?
    };

    let format = opt.format.as_deref().unwrap_or(if monitors.len() > 1 {
        DEFAULT_MULTI_FORMAT
    } else {
        DEFAULT_SINGLE_FORMAT
    });
    let (pipeline, output) = start_pipeline(&opt, format, &shutdown)?;
    let io = pipeline.io.clone();

    io.send(IoMessage::Preamble(format_preamble(&monitors)))
        .await?;

    let mut tasks = JoinSet::new();
    for mon in monitors {
        tasks.spawn(pipeline::run_monitor(
            mon,
            pipeline.clone(),
            shutdown.subscribe(),
        ));
    }
    drop(pipeline);

    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            eprintln!("Monitor task failed: {e}");
        }
    }

    pipeline::finish_output(io, output).await?;
    pipeline::print_final_stats();

    Ok(())
}

async fn run(opt: Options, shutdown: watch::Sender<bool>) -> Result<()> {
    if opt.stdin {
        run_stdin(opt, shutdown).await
    } else {
        run_wire(opt, shutdown).await
    }
}

async fn async_main(opt: Options) -> Result<()> {
    let (shutdown_tx, _) = watch::channel(false);
    let run = run(opt, shutdown_tx.clone());
    tokio::pin!(run);

    tokio::select! {
        r = &mut run => r,
        _ = tokio::signal::ctrl_c() => {
            eprintln!(
                "\nCtrl-C received, shutting down (press again to force)..."
            );
            shutdown_tx.send_replace(true);
            tokio::select! {
                r = &mut run => r,
                _ = tokio::signal::ctrl_c() => std::process::exit(130),
            }
        }
    }
}

fn main() -> Result<()> {
    let opt: Options = Options::parse();

    if opt.version {
        eprintln!("{}", version_string());
        return Ok(());
    }

    if let Some(Cmd::Completions { shell }) = opt.cmd {
        let mut cmd = Options::command();
        generate(shell, &mut cmd, "redis-monitor", &mut std::io::stdout());
        return Ok(());
    }

    if opt.debug {
        let filter = opt.line_filter()?;
        eprintln!("{filter:#?}");
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(opt.worker_threads())
        .enable_all()
        .build()
        .context("Failed to start the async runtime")?;
    let result = runtime.block_on(async_main(opt));
    // A stdin read parked on the blocking pool cannot be cancelled; don't let
    // it hold the process open after everything else has finished.
    runtime.shutdown_timeout(Duration::from_millis(100));
    result
}

#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod key_fixture;

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use redis_monitor::commands::Command;

    use super::*;
    fn empty_filter() -> LineFilter {
        LineFilter::new(
            None,
            Filter::new(Vec::new()).unwrap(),
            Filter::new(Vec::new()).unwrap(),
            commands::Filter::default(),
        )
    }

    #[test]
    fn batching_is_disabled_by_default_and_can_be_enabled() {
        assert!(!Options::try_parse_from(["redis-monitor"]).unwrap().batch);
        assert!(
            Options::try_parse_from(["redis-monitor", "--batch"])
                .unwrap()
                .batch
        );
        assert_eq!(
            Options::try_parse_from(["redis-monitor", "--no-batch"])
                .unwrap_err()
                .kind(),
            clap::error::ErrorKind::UnknownArgument
        );
    }

    fn db_filter(db: u64) -> LineFilter {
        LineFilter::new(
            Some(db),
            Filter::new(Vec::new()).unwrap(),
            Filter::new(Vec::new()).unwrap(),
            commands::Filter::default(),
        )
    }

    #[test]
    fn db_filter_selects_only_the_requested_database() {
        let filter = db_filter(3);

        assert!(filter.matches(
            None,
            br#"1.0 [3 127.0.0.1:1] "GET" "k""#,
            &mut Vec::new()
        ));
        assert!(!filter.matches(
            None,
            br#"1.0 [0 127.0.0.1:1] "GET" "k""#,
            &mut Vec::new()
        ));
        assert!(!filter.matches(
            None,
            br#"1.0 [30 127.0.0.1:1] "GET" "k""#,
            &mut Vec::new()
        ));
        assert!(!filter.matches(None, b"1.0 ", &mut Vec::new()));
        assert!(!filter.matches(None, b"OK", &mut Vec::new()));
        assert!(!filter.matches(
            None,
            br#"1.0 [99999999999999999999 127.0.0.1:1] "GET""#,
            &mut Vec::new()
        ));
    }

    #[test]
    fn db_option_is_applied_to_the_line_filter() {
        let opt =
            Options::try_parse_from(["redis-monitor", "--db", "2"]).unwrap();
        let filter = opt.line_filter().unwrap();

        assert!(filter.matches(
            None,
            br#"1.0 [2 127.0.0.1:1] "GET""#,
            &mut Vec::new()
        ));
        assert!(!filter.matches(
            None,
            br#"1.0 [1 127.0.0.1:1] "GET""#,
            &mut Vec::new()
        ));
    }

    #[test]
    fn flags_reject_unknown_names() {
        let error = Options::parse_flags(["write,wrtie"]).unwrap_err();
        assert!(error.to_string().contains("wrtie"));

        let filter = Options::parse_flags(["write, @hash", "fast"]).unwrap();
        assert_eq!(filter.flags, Some(Flags::WRITE | Flags::FAST));
        assert_eq!(filter.categories, Some(Categories::HASH));
        assert!(Options::parse_flags([" , "]).unwrap().is_empty());
    }

    #[test]
    fn stats_interval_must_be_positive_and_finite() {
        assert_eq!(parse_interval("0.5").unwrap(), Duration::from_millis(500));
        for bad in ["0", "-1", "nan", "inf", "1e300", "x"] {
            assert!(parse_interval(bad).is_err(), "accepted {bad}");
        }
    }

    #[tokio::test]
    async fn stdin_rejects_flag_filters_it_cannot_apply() {
        let opt = Options::try_parse_from([
            "redis-monitor",
            "--stdin",
            "--flags",
            "write",
        ])
        .unwrap();
        let (shutdown, _) = watch::channel(false);

        let error = run(opt, shutdown).await.unwrap_err();

        assert!(error.to_string().contains("--flags"));
    }

    fn key_filter(patterns: &[&str]) -> LineFilter {
        let mut options = vec!["redis-monitor"];
        for pattern in patterns {
            options.extend(["--key-filter", pattern]);
        }
        Options::try_parse_from(options)
            .unwrap()
            .line_filter()
            .unwrap()
    }

    #[test]
    fn key_filter_sees_keys_and_combines_with_existing_filters() {
        let lookup = key_fixture::lookup();
        let filter = key_filter(&["/^user:/", "!private"]);
        assert!(filter.needs_cmds());
        for (line, expected) in [
            (
                br#"1.0 [0 127.0.0.1:1] "SET" "user:1" "value""#.as_slice(),
                true,
            ),
            (br#"1.0 [0 127.0.0.1:1] "SET" "other" "user:1""#, false),
            (br#"1.0 [0 127.0.0.1:1] "MGET" "user:1" "private:2""#, false),
            (br#"1.0 [0 127.0.0.1:1] "SET" "user:1" "private""#, true),
            (br#"1.0 [0 127.0.0.1:1] "PING""#, false),
            (br#"1.0 [0 127.0.0.1:1] "UNKNOWN" "user:1""#, false),
            (
                br#"1.0 [0 127.0.0.1:1] "SET" "user:1" "unterminated"#
                    .as_slice(),
                false,
            ),
            (br#"broken "GET" "user:1""#, false),
        ] {
            assert_eq!(
                filter.matches(Some(&lookup), line, &mut Vec::new()),
                expected,
                "{line:?}"
            );
        }
        let options = Options::try_parse_from([
            "redis-monitor",
            "--key-filter",
            "user:",
            "--filter",
            "set",
            "--flags",
            "write",
            "--db",
            "2",
        ])
        .unwrap();
        let filter = options.line_filter().unwrap();
        assert!(filter.matches(
            Some(&lookup),
            br#"1.0 [2 127.0.0.1:1] "SET" "user:1" "v""#,
            &mut Vec::new()
        ));
        assert!(!filter.matches(
            Some(&lookup),
            br#"1.0 [0 127.0.0.1:1] "SET" "user:1" "v""#,
            &mut Vec::new()
        ));
        assert!(!filter.matches(
            Some(&lookup),
            br#"1.0 [2 127.0.0.1:1] "GET" "user:1""#,
            &mut Vec::new()
        ));
    }

    #[test]
    fn key_filter_handles_decoded_arguments_for_all_command_fixtures() {
        use std::fmt::Write;
        let lookup = key_fixture::lookup();
        let filter = key_filter(&["/^a$/"]);
        for case in key_fixture::cases() {
            let mut line = format!("1.0 [0 127.0.0.1:1] \"{}\"", case.command);
            for arg in &case.args {
                line.push_str(" \"");
                for byte in *arg {
                    write!(line, "\\x{byte:02x}").unwrap();
                }
                line.push('"');
            }
            assert_eq!(
                filter.matches(Some(&lookup), line.as_bytes(), &mut Vec::new()),
                case.keys.contains(&b"a".as_slice()),
                "{line}"
            );
        }
        let filter = key_filter(&[r"/(?-u:^user:\xff\x00$)/"]);
        assert!(filter.matches(
            Some(&lookup),
            br#"1.0 [0 127.0.0.1:1] "GET" "user:\xff\x00""#,
            &mut Vec::new()
        ));
        let filter = key_filter(&["/^$/"]);
        assert!(filter.matches(
            Some(&lookup),
            br#"1.0 [0 127.0.0.1:1] "GET" """#,
            &mut Vec::new()
        ));
    }

    #[test]
    fn key_filter_unknown_discovery_does_not_bypass_exclusions() {
        let lookup = key_fixture::lookup();
        let filter = key_filter(&["!private"]);
        let ping = br#"1.0 [0 127.0.0.1:1] "PING""#;
        assert!(filter.matches(Some(&lookup), ping, &mut Vec::new()));
        assert!(!filter.matches(None, ping, &mut Vec::new()));
        assert!(!filter.matches(
            Some(&lookup),
            br#"1.0 [0 127.0.0.1:1] "UNKNOWN" "public""#,
            &mut Vec::new()
        ));
        let redis::Value::Array(mut rows) = key_fixture::reply() else {
            unreachable!()
        };
        let redis::Value::Array(get) = &mut rows[0] else {
            unreachable!()
        };
        get[8] = redis::Value::Nil;
        let lookup: commands::Lookup =
            Command::from_reply(&redis::Value::Array(rows))
                .unwrap()
                .into();
        assert!(!filter.matches(
            Some(&lookup),
            br#"1.0 [0 127.0.0.1:1] "GET" "public""#,
            &mut Vec::new()
        ));
    }

    #[tokio::test]
    async fn stdin_rejects_key_filters_without_metadata() {
        let opt = Options::try_parse_from([
            "redis-monitor",
            "--stdin",
            "--key-filter",
            "user:",
        ])
        .unwrap();
        let (shutdown, _) = watch::channel(false);
        assert!(
            run(opt, shutdown)
                .await
                .unwrap_err()
                .to_string()
                .contains("--key-filter")
        );
        assert!(
            Options::try_parse_from(["redis-monitor", "--key-filter", "/[/"])
                .is_err()
        );
    }

    #[test]
    #[ignore = "manual release key-filter microbenchmark"]
    fn benchmark_key_filter() {
        use std::hint::black_box;
        let lookup = key_fixture::lookup();
        let lines: [&[u8]; 3] = [
            br#"1.0 [0 127.0.0.1:1] "SET" "user:1" "value""#,
            br#"1.0 [0 127.0.0.1:1] "MSET" "a" "value" "user:2" "value""#,
            br#"1.0 [0 127.0.0.1:1] "XREAD" "STREAMS" "a" "user:3" "0" "$""#,
        ];
        for (name, filter) in [
            ("disabled", empty_filter()),
            ("accept", key_filter(&["user:"])),
            ("reject", key_filter(&["missing"])),
            ("regex", key_filter(&["/^user:/"])),
            ("indexed key", key_filter(&["[0]user:", "[1]user:"])),
            (
                "indexed key regex",
                key_filter(&["[0]/^user:/", "[1]/^user:/"]),
            ),
            (
                "indexed arg",
                Options::try_parse_from([
                    "redis-monitor",
                    "--filter",
                    "[1]user:",
                    "--filter",
                    "[3]user:",
                ])
                .unwrap()
                .line_filter()
                .unwrap(),
            ),
        ] {
            let mut args = Vec::new();
            let mut samples = Vec::new();
            for _ in 0..7 {
                let start = Instant::now();
                for _ in 0..100_000 {
                    for line in lines {
                        black_box(filter.matches(
                            Some(&lookup),
                            black_box(line),
                            &mut args,
                        ));
                    }
                }
                samples.push(start.elapsed().as_secs_f64() * 1e9 / 300_000.0);
            }
            samples.sort_by(f64::total_cmp);
            eprintln!(
                "{name}: median {:.2} ns/record, {samples:.2?}",
                samples[3]
            );
        }
    }

    #[test]
    fn positional_key_filter_counts_keys_instead_of_arguments() {
        let lookup = key_fixture::lookup();
        for (pattern, line, expected) in [
            ("[3]three", br#"1.0 [0 127.0.0.1:1] "MSET" "zero" "z" "one" "o" "two" "t" "three" "t""#.as_slice(), true),
            ("[2]three", br#"1.0 [0 127.0.0.1:1] "MSET" "zero" "z" "one" "o" "two" "t" "three" "t""#, false),
            ("[1]/^one$/", br#"1.0 [0 127.0.0.1:1] "XREAD" "STREAMS" "zero" "one" "0" "$""#, true),
            ("[1]/^one$/", br#"1.0 [0 127.0.0.1:1] "XREAD" "COUNT" "10" "STREAMS" "zero" "one" "0" "$""#, true),
            ("[1]/^one$/", br#"1.0 [0 127.0.0.1:1] "XREADGROUP" "GROUP" "g" "c" "STREAMS" "zero" "one" ">" ">""#, true),
            ("[0]/^one$/", br#"1.0 [0 127.0.0.1:1] "OBJECT" "ENCODING" "one""#, true),
            ("[0]=[3]three", br#"1.0 [0 127.0.0.1:1] "GET" "[3]three""#, true),
            (r"[0]/(?-u:^user:\xff$)/", br#"1.0 [0 127.0.0.1:1] "GET" "user:\xff""#, true),
            ("[1]zero", br#"1.0 [0 127.0.0.1:1] "MGET" "zero" "zero""#, true),
            ("[0]", br#"1.0 [0 127.0.0.1:1] "PING""#, false),
            ("![0]", br#"1.0 [0 127.0.0.1:1] "PING""#, true),
        ] {
            assert_eq!(key_filter(&[pattern]).matches(Some(&lookup), line, &mut Vec::new()), expected, "{pattern}: {line:?}");
        }
        let filter = key_filter(&["[0]zero", "![1]one"]);
        assert!(!filter.matches(
            Some(&lookup),
            br#"1.0 [0 127.0.0.1:1] "MGET" "zero" "one""#,
            &mut Vec::new()
        ));
    }

    #[test]
    fn positional_command_filters_work_without_metadata_and_keep_default_scope()
    {
        let compile = |patterns: &[&str]| {
            let mut options = vec!["redis-monitor", "--stdin"];
            for pattern in patterns {
                options.extend(["--filter", pattern]);
            }
            Options::try_parse_from(options)
                .unwrap()
                .line_filter()
                .unwrap()
        };
        for (pattern, line, expected) in [
            (
                "[0]/^GET$/",
                br#"1.0 [0 127.0.0.1:1] "GET" "foo""#.as_slice(),
                true,
            ),
            (
                "[2]/^bar$/",
                br#"1.0 [0 127.0.0.1:1] "SET" "FOO" "bar""#,
                true,
            ),
            ("bar", br#"1.0 [0 127.0.0.1:1] "SET" "FOO" "bar""#, false),
            ("[1]bar", br#"1.0 [0 127.0.0.1:1] "SET" "FOO" "bar""#, false),
            ("[2]bar", br#"1.0 [0 127.0.0.1:1] "GET" "FOO""#, false),
            ("![2]bar", br#"1.0 [0 127.0.0.1:1] "GET" "FOO""#, true),
            ("[1]=!foo", br#"1.0 [0 127.0.0.1:1] "GET" "!foo""#, true),
            ("[1]=/foo/", br#"1.0 [0 127.0.0.1:1] "GET" "/foo/""#, true),
            ("[1]=[0]foo", br#"1.0 [0 127.0.0.1:1] "GET" "[0]foo""#, true),
            (
                r"[1]/(?-u:^\xff$)/",
                br#"1.0 [0 127.0.0.1:1] "GET" "\xff""#,
                true,
            ),
            (
                "![2]bar",
                br#"1.0 [0 127.0.0.1:1] "SET" "FOO" "unterminated"#,
                false,
            ),
            ("[1]foo", br#"bad "GET" "foo""#, false),
        ] {
            let filter = compile(&[pattern]);
            assert!(!filter.needs_cmds());
            assert_eq!(
                filter.matches(None, line, &mut Vec::new()),
                expected,
                "{pattern}: {line:?}"
            );
        }
        let set = br#"1.0 [0 127.0.0.1:1] "SET" "FOO" "bar""#;
        assert!(compile(&["GET", "[2]bar"]).matches(
            None,
            set,
            &mut Vec::new()
        ));
        assert!(!compile(&["SET", "![2]bar"]).matches(
            None,
            set,
            &mut Vec::new()
        ));
        assert!(!compile(&["!SET", "[2]bar"]).matches(
            None,
            set,
            &mut Vec::new()
        ));
    }

    #[test]
    fn argument_and_key_positions_combine_and_decode_once() {
        let lookup = key_fixture::lookup();
        let opt = Options::try_parse_from([
            "redis-monitor",
            "--filter",
            "[4]=o",
            "--key-filter",
            "[1]=one",
        ])
        .unwrap();
        let filter = opt.line_filter().unwrap();
        let mut args = Vec::new();
        assert!(filter.matches(
            Some(&lookup),
            br#"1.0 [0 127.0.0.1:1] "MSET" "zero" "z" "one" "o""#,
            &mut args
        ));
        assert_eq!(args.len(), 4);
        assert!(!filter.matches(
            Some(&lookup),
            br#"1.0 [0 127.0.0.1:1] "MSET" "zero" "z" "two" "o""#,
            &mut args
        ));
        assert!(!filter.matches(
            Some(&lookup),
            br#"1.0 [0 127.0.0.1:1] "MSET" "zero" "z" "one" "x""#,
            &mut args
        ));
    }

    #[test]
    fn worker_threads_must_be_positive() {
        let opt = Options::try_parse_from(["redis-monitor", "--threads", "3"])
            .unwrap();
        assert_eq!(opt.worker_threads(), 3);
        assert!(
            Options::try_parse_from(["redis-monitor", "--threads", "0"])
                .is_err()
        );
        let default = Options::try_parse_from(["redis-monitor"]).unwrap();
        assert!((1..=DEFAULT_MAX_THREADS).contains(&default.worker_threads()));
    }
}
