#![warn(clippy::all, clippy::nursery, clippy::pedantic)]
//#![allow(clippy::non_ascii_literal)]
//#![allow(clippy::must_use_candidate)]
use crate::{
    config::{Map, ServerAuth},
    connection::Cluster,
    connection::Monitor,
    filter::FilterPattern,
    monitor::Line,
};
use anyhow::{Result, anyhow};
use clap::Parser;
use colored::Color;
use connection::{ServerAddr, TlsConfig};
use filter::Filter;
use futures::stream::FuturesUnordered;
use output::OutputKind;
use rand::{Rng, rng};
use std::{
    collections::HashSet, convert::From, path::PathBuf, str::FromStr,
    sync::Arc, time::Instant,
};
use tokio::{
    io::AsyncBufReadExt,
    sync::mpsc,
    time::{Duration, sleep},
};

//mod commands;
mod config;
mod connection;
mod filter;
mod monitor;
mod output;
mod stats;

#[derive(Parser, Debug)]
#[command(
    name = "redis-monitor",
    about = "A utility to monitor one or more RESP compatible servers",
    after_help = r#"Format specifiers:
  %sa  Full address of the server (host:port or unix path)
  %sh  Host part of the server address
  %sp  Port part of the server address (or basename of unix path)
  %Sn  Name of the server instance if it is set
  %ca  Full address of the client (ip:port or unix path)
  %ch  Host part of the client address
  %cp  Port part of the client address (or basename of unix path)
  %d   The database number
  %t   The timestamp as reported by MONITOR
  %C   The command name
  %A   The rest of the command arguments

  The default format is: "[%sa %ca %d] %t - \"%C\" %a";

Examples:
  # Monitor a cluster expecting one node to be 127.0.0.1:6379
  redis-monitor -c 6379
  # Monitor two standalone instances
  redis-monitor host1:6379 host2:6379"#
)]
#[allow(clippy::struct_excessive_bools)]
struct Options {
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
           help = "One or more patterns to either filter out or in")]
    filter: Vec<FilterPattern>,

    #[arg(
        short,
        long,
        default_value = "plain",
        help = "How to format the output"
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

    #[arg(long, value_parser = validate_positive_f64)]
    stats: Option<f64>,

    pub instances: Vec<String>,
}

const VERSION: &str = env!("CARGO_PKG_VERSION");
const GIT_HASH: &str = env!("GIT_HASH");
const GIT_DIRTY: &str = env!("GIT_DIRTY");

const DEFAULT_FORMAT: &'static str = "[%sA %cA %d] %t - \"%C\" %a";

fn validate_positive_f64(s: &str) -> Result<f64> {
    match s.parse::<f64>() {
        Ok(val) if val > 0.0 => Ok(val),
        Ok(_) => Err(anyhow!("Value must be positive".to_string())),
        Err(_) => Err(anyhow!("Invalid number".to_string())),
    }
}

impl Options {
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
}

// Treat each instnace as a potential cluster seed. This means that if more than
// one seeds of the same cluster are passed we may map the same keyspace more than
// once. This is fine, but we should be aware of it.
fn process_cluster_instances(
    opt: &Options,
    tls: Option<&Arc<TlsConfig>>,
    auth: &ServerAuth,
) -> Vec<Monitor> {
    let addresses = opt.instances.iter().map(|addr| {
        ServerAddr::from_str(addr).unwrap_or_else(|_| {
            panic!("Unable to interpret '{addr:?}' as a server address");
        })
    });

    addresses
        .flat_map(|address| {
            Cluster::from_seed(&address)
                .unwrap_or_else(|_| {
                    panic!(
                        "Unable to interpret '{address:?}' as a cluster address"
                    );
                })
                .get_nodes()
                .iter()
                .flat_map(|primary| {
                    let mut nodes = vec![primary];
                    if opt.replicas {
                        nodes.extend(&primary.replicas);
                    }

                    nodes.into_iter().map(|n| {
                        Monitor::new(
                            Some(&n.id),
                            n.addr.clone(),
                            tls.cloned(),
                            auth.clone(),
                            None,
                            opt.format.as_deref(),
                        )
                    })
                })
                .collect::<Vec<_>>()
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

// Take the array of instances provided on the command line and attempt to map
// them to one ore more instances. These can either be named instances like
// mycluster` which were loaded from our config file, or be in some parsable
// form like "host:port", or "redis://...".
fn process_instances(
    cfg: &Map,
    opt: &Options,
    tls: Option<&Arc<TlsConfig>>,
    auth: &ServerAuth,
) -> Vec<Monitor> {
    opt.instances
        .iter()
        .flat_map(|inst| {
            cfg.get(inst).map_or_else(
                || {
                    ServerAddr::from_str(inst).map_or_else(
                        |_| {
                            panic!(
                            "Unable to parse '{inst}' as an address or named instance"
                        );
                        },
                        |addr| {
                            let monitor = Monitor::new(
                                None,
                                addr,
                                tls.cloned(),
                                auth.clone(),
                                None,
                                opt.format.as_deref(),
                            );

                            vec![monitor]
                        }
                    )
                },
                |entry| Monitor::from_config_entry(inst, entry),
            )
        })
        .collect()
}

#[derive(Debug)]
struct Backoff {
    retries: u32,
    max_delay: Duration,
}

#[derive(Debug)]
pub struct MonitorMessage {
    pub server: Arc<ServerAddr>,
    pub color: Option<Color>,
    line: String,
}

impl Backoff {
    const MIN_DELAY: Duration = Duration::from_millis(50);
    const MAX_DELAY: Duration = Duration::from_secs(1);

    const fn new() -> Self {
        Self {
            retries: 0,
            max_delay: Self::MAX_DELAY,
        }
    }

    fn delay(&mut self) -> Duration {
        let mut rng = rng();

        self.retries += 1;
        let shift_amount =
            (self.retries.min(6) + rng.random_range(0..3)) as u32;
        let base_delay = Self::MIN_DELAY.as_millis();
        let delay_ms =
            (base_delay << shift_amount).try_into().unwrap_or(u64::MAX);

        Duration::from_millis(
            delay_ms.min(
                self.max_delay
                    .as_millis()
                    .try_into()
                    .expect("Delay too long"),
            ),
        )
    }

    const fn reset(&mut self) {
        self.retries = 0;
    }
}

async fn run_monitor(mon: Monitor, tx: mpsc::Sender<MonitorMessage>) {
    let server = Arc::new(mon.address.clone());
    let mut backoff = Backoff::new();

    loop {
        match mon.clone().connect().await {
            Ok((_, mut reader)) => {
                backoff.reset();

                let mut line = String::new();
                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) => break,
                        Ok(_) => {
                            let msg = MonitorMessage {
                                server: server.clone(),
                                color: mon.color,
                                line: line[1..].trim_end().to_string(),
                            };
                            if let Err(e) = tx.send(msg).await {
                                eprintln!(
                                    "{server} Failed to send message: {e}"
                                );
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("{server} Read error {e}");
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                if backoff.retries == 0 {
                    eprintln!("{server} Error connecting {e}");
                }
            }
        }

        sleep(backoff.delay()).await;
    }
}

fn verseion_string() -> String {
    let git_display = format!(
        "{GIT_HASH}{}",
        if GIT_DIRTY == "yes" { "-dirty" } else { "" }
    );

    format!("redis-monitor v{VERSION} (git {git_display})")
}

fn print_stats(stats: &stats::CommandStats, output: OutputKind) {
    let mut stats = stats.get_stats();
    stats.sort_by(|a, b| b.count.cmp(&a.count));

    if output == OutputKind::Json {
        println!(
            "{}",
            serde_json::to_string(&stats).unwrap_or_else(|e| {
                eprintln!("Failed to serialize stats to JSON: {e}");
                "[]".to_string()
            })
        );
    } else {
        println!(
            "[stats]: {}",
            stats
                .iter()
                .filter_map(|s| {
                    if s.count > 0 {
                        Some(format!("{}=[{}, {}]", s.name, s.count, s.bytes))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt: Options = Options::parse();
    let cfg = Map::load(opt.config_file.as_deref())?;

    if opt.version {
        eprintln!("{}", verseion_string());
        return Ok(());
    }

    if opt.instances.is_empty() {
        eprintln!("Must pass at least one unstance (host/port or name)");
        std::process::exit(1);
    }

    let tls = opt.get_tls_config()?;
    let auth = opt.get_server_auth();

    let seeds = if opt.cluster {
        process_cluster_instances(&opt, tls.as_ref(), &auth)
    } else {
        process_instances(&cfg, &opt, tls.as_ref(), &auth)
    };

    let (tx, mut rx) = mpsc::channel::<MonitorMessage>(1000);

    let tasks = FuturesUnordered::new();

    let format = opt.format.as_deref().unwrap_or(DEFAULT_FORMAT);
    let mut writer = opt.output.get_writer(std::io::stdout(), format);

    writer.preamble(&seeds)?;

    for mon in seeds {
        tasks.push(tokio::spawn(run_monitor(mon, tx.clone())));
    }

    let mut stats = if opt.output == OutputKind::Plain {
        opt.stats.map(|_| stats::CommandStats::new())
    } else {
        None
    };

    let interval = Duration::from_secs_f64(opt.stats.unwrap_or(1.0));
    let filter: Filter = opt.filter.into();
    let mut tick = Instant::now();

    while let Some(MonitorMessage { server, line, .. }) = rx.recv().await {
        if !filter.check(&line) {
            continue;
        }

        let parsed = match Line::from_line(&line, opt.output.need_args()) {
            Ok((_, line)) => line,
            Err(e) => {
                eprintln!("Failed to parse line: {e} <- input: {line:?}");
                continue;
            }
        };

        if let Some(ref mut stats) = stats {
            stats.incr(parsed.cmd, line.len());
            if tick.elapsed() >= interval {
                print_stats(stats, opt.output);
                tick = Instant::now();
            }
        }

        writer.write_line(&server, &parsed)?;
    }

    Ok(())
}
