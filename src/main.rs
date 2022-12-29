use crate::{
    config::{ConfigFile, RedisAuth},
    filter::Filter,
    monitor::{MonitorArgs, MonitoredInstance},
    stats::CommandStats,
};
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::task;

use anyhow::{Context, Result};
use clap::Parser;
use colored::Colorize;
use connection::RedisAddr;
use futures::stream::*;
use regex::Regex;
use serde::{de, Deserialize, Deserializer};
use std::{
    collections::HashSet,
    convert::{AsRef, From},
    default::Default,
    str::FromStr,
};

mod config;
mod connection;
mod filter;
mod monitor;
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

impl CsvArgument {
    pub fn to_vec(&self) -> Vec<String> {
        self.0.to_owned()
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
        let url = instance.get_url_string();
        println!("MONITOR {url} {}", instance.fmt_str());

        let mon = get_monitor(&url, instance.get_auth())
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
        opt.include.unwrap_or_default().to_vec(),
        opt.exclude.unwrap_or_default().to_vec(),
    );

    let mut streams = futures::stream::select_all(pairs.into_iter().map(move |(info, c)| {
        c.into_on_message::<String>()
            .map(move |c| (info.clone(), c))
    }));

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
        let line = MonitorArgs::from_line(&msg).expect("Failed to parse line");

        instance.incr_stats(line.cmd, msg.len());

        if filter.filter(line.cmd) {
            continue;
        }

        if !opt.no_color {
            let msg = if let Some(color) = instance.get_color() {
                msg.color(color).to_string()
            } else {
                msg
            };

            let hdr = instance.fmt_str().bold();
            println!("{hdr} {msg}");
        } else {
            println!("{} {msg}", instance.fmt_str());
        }
    }

    Ok(())
}
