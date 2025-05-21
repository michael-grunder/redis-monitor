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
use connection::ServerAddr;
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
struct Options {
    #[arg(short, long, help = "Treat each instance like its a cluster seed")]
    cluster: bool,

    #[arg(short, long, help = "How to format each MONITOR line")]
    format: Option<String>,

    #[arg(short, long)]
    replicas: bool,

    #[arg(short, long)]
    list: bool,

    #[arg(long)]
    config_file: Option<String>,

    #[arg(long)]
    no_color: bool,

    #[arg(long)]
    include: Option<CsvArgument>,

    #[arg(long)]
    exclude: Option<CsvArgument>,

    #[arg(long)]
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

    pub instances: Vec<String>,
}

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

// Treat each instnace as a potential cluster seed. This means that if more than
// one seeds of the same cluster are passed we may map the same keyspace more than
// once. This is fine, but we should be aware of it.
fn process_cluster_instances(opt: &Options, auth: &ServerAuth) -> Vec<Monitor> {
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
                            Some(n.id.clone()),
                            n.addr.clone(),
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
fn process_instances(cfg: &Map, opt: &Options) -> Vec<Monitor> {
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
                            let mut addr: Monitor = addr.into();
                            if let Some(format) = &opt.format {
                                addr.format = format.clone();
                            }
                            vec![addr]
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
    attempt: u32,
    max_delay: Duration,
}

#[derive(Debug)]
pub struct MonitorMessage {
    pub prefix: Arc<str>,
    pub address: Arc<ServerAddr>,
    pub color: Arc<Option<Color>>,
    line: String,
}

impl RetryBackoff {
    const MIN_DELAY: Duration = Duration::from_millis(50);
    const MAX_DELAY: Duration = Duration::from_secs(1);

    fn new() -> Self {
        Self {
            attempt: 0,
            max_delay: Self::MAX_DELAY,
        }
    }

    fn delay(&mut self) -> Duration {
        let mut rng = rng();

        self.attempt += 1;
        let delay_ms = (Self::MIN_DELAY.as_millis()
            << self.attempt.min(6) + rng.random_range(0..100))
            as u64;

        Duration::from_millis(delay_ms.min(self.max_delay.as_millis() as u64))
    }
}

async fn run_monitor(mon: Monitor, tx: mpsc::Sender<MonitorMessage>) {
    let prefix: Arc<str> = Arc::from(mon.format.clone());
    let address = Arc::new(mon.address.clone());
    let color = Arc::new(mon.color.clone());

    let mut backoff = RetryBackoff::new();

    loop {
        match mon.clone().connect().await {
            Ok((_, mut reader)) => {
                let mut line = String::new();
                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) => break,
                        Ok(_) => {
                            let msg = MonitorMessage {
                                prefix: prefix.clone(),
                                address: address.clone(),
                                color: color.clone(),
                                line: line[1..].trim_end().to_string(),
                            };
                            if tx.send(msg).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("[{prefix}] Read error {e:?}");
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                if backoff.attempt == 0 {
                    eprintln!("[{prefix}] Error connecting {e}");
                }
            }
        }

        sleep(backoff.delay()).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt: Options = Options::parse();
    let cfg = Map::load(opt.config_file.as_ref());

    if opt.instances.is_empty() {
        eprintln!("Must pass at least one unstance (host/port or name)");
        std::process::exit(1);
    }

    let auth =
        ServerAuth::from_user_pass(opt.user.as_deref(), opt.pass.as_deref());

    let seeds = if opt.cluster {
        process_cluster_instances(&opt, &auth)
    } else {
        process_instances(&cfg, &opt)
    };

    let (tx, mut rx) = mpsc::channel::<MonitorMessage>(1000);
    let filter = Filter::from_args(
        opt.include.unwrap_or_default().into(),
        opt.exclude.unwrap_or_default().into(),
    );

    let tasks = FuturesUnordered::new();

    for mon in seeds {
        println!("MONITOR: {}", mon.address);
        tasks.push(tokio::spawn(run_monitor(mon, tx.clone())));
    }

    let format_prefix: Box<dyn Fn(&str) -> ColoredString> = if opt.no_color {
        Box::new(|p| format!("[{}]", p).normal())
    } else {
        Box::new(|p| format!("[{}]", p).bold())
    };

    let mut stats = stats::CommandStats::new();

    while let Some(MonitorMessage { prefix, line, .. }) = rx.recv().await {
        if opt.db.is_none() && filter.is_empty() {
            println!("{} {}", format_prefix(&prefix), line);
            continue;
        }

        let parsed = match Line::from_line(&line, false) {
            Ok((_, line)) => line,
            Err(e) => {
                eprintln!("Failed to parse line: {e:?} <- input: {line:?}");
                continue;
            }
        };

        stats.incr(parsed.cmd, line.len());

        if matches!(opt.db, Some(db) if db != parsed.db)
            || filter.filter(parsed.cmd)
        {
            continue;
        }

        println!("{} {}", format_prefix(&prefix), line);
    }

    Ok(())
}
