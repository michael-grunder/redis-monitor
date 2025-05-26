#![warn(clippy::all, clippy::nursery, clippy::pedantic)]
//#![allow(clippy::non_ascii_literal)]
//#![allow(clippy::must_use_candidate)]
use crate::{
    config::{Map, ServerAuth},
    connection::Cluster,
    connection::Monitor,
    filter::Filter,
    monitor::Line,
};
use anyhow::Result;
use clap::Parser;
use colored::{Color, ColoredString, Colorize};
use connection::{ServerAddr, TlsConfig};
use futures::stream::FuturesUnordered;
use rand::{Rng, rng};
use serde::{Deserialize, Deserializer, de};
use std::{
    collections::HashSet, convert::From, default::Default, path::PathBuf,
    str::FromStr, sync::Arc,
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
mod stats;

#[derive(Debug, Clone, Default)]
struct CsvArgument(Vec<String>);

#[derive(Parser, Debug)]
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

    #[arg(long, help = "Only include commands matching comma-separated list")]
    include: Option<CsvArgument>,

    #[arg(long, help = "Exclude commands matching comma-separated list")]
    exclude: Option<CsvArgument>,

    #[arg(long, help = "Only show commands for a specific database")]
    db: Option<u64>,

    #[arg(short, long, help = "Redis user")]
    user: Option<String>,

    #[arg(short, long, help = "Redis password")]
    pass: Option<String>,

    #[arg(short, long, help = "Output in JSON format")]
    json: bool,

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

    pub instances: Vec<String>,
}

const VERSION: &str = env!("CARGO_PKG_VERSION");
const GIT_HASH: &str = env!("GIT_HASH");
const GIT_DIRTY: &str = env!("GIT_DIRTY");

impl FromStr for CsvArgument {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut set: HashSet<String> = s
            .split(',')
            .filter_map(|s| {
                if s.is_empty() {
                    None
                } else {
                    Some(s.trim().to_owned())
                }
            })
            .collect();

        Ok(Self(set.drain().collect()))
    }
}

impl From<CsvArgument> for Vec<String> {
    fn from(arg: CsvArgument) -> Self {
        arg.0
    }
}

impl<'de> Deserialize<'de> for CsvArgument {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        FromStr::from_str(&s).map_err(de::Error::custom)
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
    // First make sure we can turn them all into RedisAddr
    let addresses = opt.instances.iter().map(|i| {
        ServerAddr::from_str(i).unwrap_or_else(|_| {
            panic!("Unable to interpret '{i:?}' as a redis address");
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
                            opt.format.clone(),
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
                                opt.format.clone(),
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
struct RetryBackoff {
    retries: u32,
    max_delay: Duration,
}

#[derive(Debug)]
pub struct MonitorMessage {
    pub prefix: Arc<str>,
    pub address: Arc<ServerAddr>,
    pub color: Option<Color>,
    line: String,
}

impl RetryBackoff {
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
    let prefix: Arc<str> = Arc::from(mon.format.clone());
    let address = Arc::new(mon.address.clone());
    let mut backoff = RetryBackoff::new();

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
                                prefix: prefix.clone(),
                                address: address.clone(),
                                color: mon.color,
                                line: line[1..].trim_end().to_string(),
                            };
                            if let Err(e) = tx.send(msg).await {
                                eprintln!(
                                    "[{prefix}] Failed to send message: {e}"
                                );
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("[{prefix}] Read error {e}");
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                if backoff.retries == 0 {
                    eprintln!("[{prefix}] Error connecting {e}");
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

#[tokio::main]
async fn main() -> Result<()> {
    let opt: Options = Options::parse();
    let cfg = Map::load(opt.config_file.as_deref())?;

    if opt.version {
        println!("{}", verseion_string());
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
    let filter = Filter::from_args(
        opt.include.unwrap_or_default().into(),
        opt.exclude.unwrap_or_default().into(),
    );

    let tasks = FuturesUnordered::new();

    for mon in seeds {
        if opt.json {
            println!("{}\n", serde_json::to_string(&mon.address)?);
        } else {
            println!("MONITOR: {}", mon.address);
        }
        tasks.push(tokio::spawn(run_monitor(mon, tx.clone())));
    }

    let format_prefix: Box<dyn Fn(&str) -> ColoredString> = if opt.no_color {
        Box::new(|p| format!("{p}").normal())
    } else {
        Box::new(|p| format!("{p}").bold())
    };

    let mut stats = stats::CommandStats::new();

    while let Some(MonitorMessage { prefix, line, .. }) = rx.recv().await {
        if !opt.json && (opt.db.is_none() && filter.is_empty()) {
            println!("{} {}", format_prefix(&prefix), line);
            continue;
        }

        let parsed = match Line::from_line(&line, opt.json) {
            Ok((_, line)) => line,
            Err(e) => {
                eprintln!("Failed to parse line: {e} <- input: {line:?}");
                continue;
            }
        };

        stats.incr(parsed.cmd, line.len());

        if matches!(opt.db, Some(db) if db != parsed.db)
            || filter.filter(parsed.cmd)
        {
            continue;
        }

        if opt.json {
            println!("{}", serde_json::to_string(&parsed)?);
        } else {
            println!("{} {}", format_prefix(&prefix), line);
        }
    }

    Ok(())
}
