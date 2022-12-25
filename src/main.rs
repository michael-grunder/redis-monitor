use crate::{
    config::ConfigEntry,
    config::ConfigFile,
    connection::{Cluster, GetHost, GetPort},
};
use anyhow::{Context, Result};
use clap::Parser;
use connection::RedisAddr;
use futures::stream::*;
use std::{
    convert::{AsRef, From},
    str::FromStr,
};

mod config;
mod connection;

#[derive(Parser, Debug)]
struct Options {
    #[arg(short, long)]
    replicas: bool,

    #[arg(short, long)]
    list: bool,

    #[arg(long)]
    config_file: Option<String>,

    pub instances: Vec<String>,
}

#[derive(Clone)]
struct MonitoredInstance {
    // The name this instance belongs to (if it's from our config.toml)
    name: Option<String>,

    // The address itself
    addr: RedisAddr,

    // Is it a redis cluster
    cluster: bool,

    // Format string (defaults to {host}:{port}
    fmt: String,
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

    fn new(name: Option<String>, addr: RedisAddr, cluster: bool, fmt: Option<String>) -> Self {
        let fmt =
            Self::make_fmt_string(&name, &addr, &fmt.unwrap_or_else(|| "{host}:{port}".into()));

        Self {
            name,
            addr,
            cluster,
            fmt,
        }
    }

    fn from_config_entry(name: &str, entry: &ConfigEntry) -> Vec<Self> {
        entry
            .addresses
            .iter()
            .map(|addr| {
                Self::new(
                    Some(name.to_owned()),
                    addr.to_owned(),
                    entry.cluster,
                    entry.format.clone(),
                )
            })
            .collect()
    }
}

impl From<RedisAddr> for MonitoredInstance {
    fn from(addr: RedisAddr) -> Self {
        Self::new(None, addr, false, Some("{host}:{port}".to_owned()))
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

    println!("{cfg:#?}");

    if opt.instances.is_empty() {
        eprintln!("Must pass at least one redis instance (either host/port or named instance)");
        std::process::exit(1);
    }

    let seeds = process_instances(&cfg, &opt.instances);
    let pairs = get_monitor_pairs(seeds).await.unwrap();

    let mut streams = futures::stream::select_all(pairs.into_iter().map(move |(info, c)| {
        c.into_on_message::<String>()
            .map(move |c| (info.clone(), c))
    }));

    while let Some((instance, msg)) = streams.next().await {
        println!("[{}] {msg}", instance.fmt);
    }

    Ok(())
}
