use crate::{
    config::{ConfigEntry, ConfigFile, RedisAuth},
    connection::{Cluster, GetHost, GetPort},
    filter::Filter,
    stats::CommandStats,
};
use tokio::io::AsyncBufReadExt;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::task;

use anyhow::{Context, Result};
use clap::Parser;
use colored::{Color, Colorize};
use connection::RedisAddr;
use futures::stream::*;
use regex::Regex;
use serde::{de, Deserialize, Deserializer};
use std::{
    collections::HashSet,
    convert::{AsRef, From},
    str::FromStr,
};

mod config;
mod connection;
mod filter;
mod stats;

#[derive(Debug, Clone, Default)]
struct CsvArgument(Vec<String>);

#[derive(Parser, Debug)]
struct Options {
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

    pub instances: Vec<String>,
}

#[derive(Clone)]
struct MonitoredInstance {
    // The name this instance belongs to (if it's from our config.toml)
    name: Option<String>,

    // The address itself
    addr: RedisAddr,

    auth: Option<RedisAuth>,

    // Format string (defaults to {host}:{port}
    fmt: String,

    color: Option<Color>,

    stats: CommandStats,
}

impl FromStr for CsvArgument {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut set: HashSet<String> = s
            .split(',')
            .filter_map(|s| {
                if !s.is_empty() {
                    Some(s.trim().to_owned())
                } else {
                    None
                }
            })
            .collect();

        Ok(Self(set.drain().collect()))
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

impl MonitoredInstance {
    fn make_fmt_string(name: &Option<String>, addr: &RedisAddr, fmt: &str) -> String {
        let fmt = fmt.to_owned();
        let mut fmt = fmt.replace("{host}", &addr.get_host());

        if let Some(name) = name {
            fmt = fmt.replace("{name}", name);
        };

        if let Some(port) = addr.get_port() {
            fmt = fmt.replace("{port}", &format!("{port}"));
        }

        fmt
    }

    fn new(
        name: Option<String>,
        addr: RedisAddr,
        auth: Option<RedisAuth>,
        color: Option<Color>,
        fmt: Option<String>,
    ) -> Self {
        let fmt =
            Self::make_fmt_string(&name, &addr, &fmt.unwrap_or_else(|| "{host}:{port}".into()));

        Self {
            name,
            addr,
            auth,
            color,
            stats: CommandStats::new(),
            fmt,
        }
    }

    fn from_config_entry(name: &str, entry: &ConfigEntry) -> Vec<Self> {
        if entry.cluster {
            let c = Cluster::from_seeds(&entry.get_addresses()).expect("Can't get cluster nodes");
            c.get_primary_nodes()
                .iter()
                .map(|primary| {
                    Self::new(
                        Some(name.to_owned()),
                        primary.addr.to_owned(),
                        entry.get_auth(),
                        entry.get_color(),
                        entry.format.clone(),
                    )
                })
                .collect()
        } else {
            entry
                .get_addresses()
                .iter()
                .map(|addr| {
                    Self::new(
                        Some(name.to_owned()),
                        addr.to_owned(),
                        entry.get_auth(),
                        entry.get_color(),
                        entry.format.clone(),
                    )
                })
                .collect()
        }
    }
}

impl From<RedisAddr> for MonitoredInstance {
    fn from(addr: RedisAddr) -> Self {
        Self::new(None, addr, None, None, Some("{host}:{port}".to_owned()))
    }
}

async fn get_monitor<T: AsRef<str>>(
    url: T,
    auth: &Option<RedisAuth>,
) -> Result<redis::aio::Monitor> {
    let cli = redis::Client::open(url.as_ref()).context("Failed to open connection to")?;
    let mut con = cli.get_async_connection().await?;

    if let Some(auth) = auth {
        if !auth.auth(&mut con).await {
            panic!("Failed to authenticate connection!");
        }
    }

    let mut mon = con.into_monitor();
    mon.monitor().await?;
    Ok(mon)
}

async fn get_monitor_pairs(
    instances: Vec<MonitoredInstance>,
) -> Result<Vec<(MonitoredInstance, redis::aio::Monitor)>> {
    let mut res = vec![];

    for instance in instances {
        let url = instance.addr.get_url_string();
        println!("MONITOR {url} {}", instance.fmt);

        let mon = get_monitor(&url, &instance.auth)
            .await
            .with_context(|| format!("Failed to get connection for '{url}'"))?;
        res.push((instance, mon));
    }

    Ok(res)
}

// Take the array of instances provided on the command line and attempt to map them to one ore more
// instances.  These can either be named instances like `mycluster` which were loaded from our
// config file, or be in some parsable form like "host:port", or "redis://...".
fn process_instances(cfg: &ConfigFile, instances: &[String]) -> Vec<MonitoredInstance> {
    instances
        .iter()
        .flat_map(|instance| {
            if let Some(entry) = cfg.get(instance) {
                MonitoredInstance::from_config_entry(instance, entry)
            } else if let Ok(addr) = RedisAddr::from_str(instance) {
                vec![addr.into()]
            } else {
                panic!("Unable to interpret '{instance}' as a redis address or named instance");
            }
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt: Options = Options::parse();
    let cfg = ConfigFile::load(opt.config_file);

    if opt.instances.is_empty() {
        eprintln!("Must pass at least one redis instance (either host/port or named instance)");
        std::process::exit(1);
    }

    let seeds = process_instances(&cfg, &opt.instances);
    let pairs = get_monitor_pairs(seeds).await.unwrap();
    let filter = Filter::from_args(
        opt.include.unwrap_or_default().0,
        opt.exclude.unwrap_or_default().0,
    );

    let mut streams = futures::stream::select_all(pairs.into_iter().map(move |(info, c)| {
        c.into_on_message::<String>()
            .map(move |c| (info.clone(), c))
    }));

    //`1672008540.915665 [0 127.0.0.1:34336] "BLMPOP" "0.20000000000000001" "2" "{bl}1" "{bl}2" "LEFT"`
    //let re = Regex::new(r##"\A(?P<timestamp>\d+\.\d+)\s+\[(?P<database>\d+)\s+(?P<client>\S+)\]\s+"(?P<command>\S+)"\s+(?P<duration>\S+)\s+(?P<keyspace_event>\S+)\s+(?P<key1>\S+)\s*(?P<key2>\S+)?\s*(?P<side>\S+)?"##).unwrap();

    let re = Regex::new(r#"(?P<timestamp>\d+\.\d+)\s+\[(?P<database>\d+)\s+(?P<client>\S+)\]\s+"(?P<command>\S+)" (?P<args>.*)"#)
        .unwrap();

    // Spawn a task to read from stdin
    let mut reader = BufReader::new(tokio::io::stdin());
    task::spawn(async move {
        // Read a line of input from the user
        let mut input = String::new();
        while reader.read_line(&mut input).await.is_ok() {
            println!("Input: {input}");
            if input.to_lowercase().starts_with("quit") {
                println!("Quit detected, exiting.");
                std::process::exit(0);
            }
            input.truncate(0);
        }
    });

    while let Some((mut instance, msg)) = streams.next().await {
        let captures = re.captures(&msg);

        let cmd = &captures.unwrap()["command"];

        instance.stats.incr(cmd, msg.len());

        if filter.filter(cmd) {
            continue;
        }

        if !opt.no_color {
            let msg = if let Some(color) = instance.color {
                msg.color(color).to_string()
            } else {
                msg
            };

            let hdr = instance.fmt.bold();
            println!("{hdr} {msg}");
        } else {
            println!("{} {msg}", instance.fmt);
        }
    }

    Ok(())
}
