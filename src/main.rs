use crate::{
    config::{ConfigEntry, ConfigFile},
    connection::{Cluster, GetHost, GetPort},
    stats::CommandStats,
};
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
mod stats;

#[derive(Debug, Clone, Default)]
struct CommandFilter(HashSet<String>);

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
    filter: Option<CommandFilter>,

    pub instances: Vec<String>,
}

#[derive(Clone)]
struct MonitoredInstance {
    // The name this instance belongs to (if it's from our config.toml)
    name: Option<String>,

    // The address itself
    addr: RedisAddr,

    // Format string (defaults to {host}:{port}
    fmt: String,

    color: Option<Color>,

    stats: CommandStats,
}

impl FromStr for CommandFilter {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let set: HashSet<String> = s
            .split(',')
            .filter_map(|s| {
                if !s.is_empty() {
                    Some(s.trim().to_uppercase())
                } else {
                    None
                }
            })
            .collect();

        Ok(Self { 0: set })
    }
}

impl<'de> Deserialize<'de> for CommandFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        FromStr::from_str(&s).map_err(de::Error::custom)
    }
}

impl CommandFilter {
    fn new() -> Self {
        Self(HashSet::new())
    }

    fn add_command(&mut self, cmd: &str) {
        self.0.insert(cmd.to_owned());
    }

    fn add_commands(&mut self, src: CommandFilter) {
        for cmd in src.0 {
            self.add_command(&cmd);
        }
    }

    fn filter(&self, cmd: &str) -> bool {
        if self.0.is_empty() {
            return false;
        }

        !self.0.contains(cmd)
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
        color: Option<Color>,
        fmt: Option<String>,
    ) -> Self {
        let fmt =
            Self::make_fmt_string(&name, &addr, &fmt.unwrap_or_else(|| "{host}:{port}".into()));

        Self {
            name,
            addr,
            color,
            stats: CommandStats::new(),
            fmt,
        }
    }

    fn from_config_entry(name: &str, entry: &ConfigEntry) -> Vec<Self> {
        if entry.cluster {
            let c = Cluster::from_seeds(&entry.addresses).expect("Can't get cluster nodes");
            c.get_primary_nodes()
                .iter()
                .map(|primary| {
                    Self::new(
                        Some(name.to_owned()),
                        primary.addr.to_owned(),
                        entry.get_color(),
                        entry.format.clone(),
                    )
                })
                .collect()
        } else {
            entry
                .addresses
                .iter()
                .map(|addr| {
                    Self::new(
                        Some(name.to_owned()),
                        addr.to_owned(),
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
        Self::new(None, addr, None, Some("{host}:{port}".to_owned()))
    }
}

async fn get_monitor<T: AsRef<str>>(url: T) -> Result<redis::aio::Monitor> {
    let cli = redis::Client::open(url.as_ref()).context("Failed to open connection to")?;
    let mut mon = cli.get_async_connection().await?.into_monitor();
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

        let mon = get_monitor(&url)
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
    let filter = opt.filter.unwrap_or_default();

    let mut streams = futures::stream::select_all(pairs.into_iter().map(move |(info, c)| {
        c.into_on_message::<String>()
            .map(move |c| (info.clone(), c))
    }));

    //`1672008540.915665 [0 127.0.0.1:34336] "BLMPOP" "0.20000000000000001" "2" "{bl}1" "{bl}2" "LEFT"`
    //let re = Regex::new(r##"\A(?P<timestamp>\d+\.\d+)\s+\[(?P<database>\d+)\s+(?P<client>\S+)\]\s+"(?P<command>\S+)"\s+(?P<duration>\S+)\s+(?P<keyspace_event>\S+)\s+(?P<key1>\S+)\s*(?P<key2>\S+)?\s*(?P<side>\S+)?"##).unwrap();

    let re = Regex::new(r#"(?P<timestamp>\d+\.\d+)\s+\[(?P<database>\d+)\s+(?P<client>\S+)\]\s+"(?P<command>\S+)" (?P<args>.*)"#)
        .unwrap();

    while let Some((instance, msg)) = streams.next().await {
        let captures = re.captures(&msg);

        //println!("{captures:#?}");

        let cmd = &captures.unwrap()["command"];

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
            println!("[{hdr}] {msg}");
        } else {
            println!("[{}] {msg}", instance.fmt);
        }
    }

    Ok(())
}
