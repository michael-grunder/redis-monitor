use crate::{config::ConfigFile, connection::Cluster};
use anyhow::{Context, Result};
use clap::Parser;
use connection::RedisAddr;
use futures::stream::*;
use lazy_static::lazy_static;
use redis::FromRedisValue;
use regex::Regex;
use std::convert::AsRef;

mod config;
mod connection;

#[derive(Parser, Debug)]
struct Options {
    #[arg(short, long)]
    cluster: bool,

    #[arg(short, long)]
    replicas: bool,

    #[arg(short, long)]
    name: Option<String>,

    #[arg(long)]
    config_file: Option<String>,

    pub instances: Vec<RedisAddr>,
}

async fn get_monitor<T: AsRef<str>>(url: T) -> Result<redis::aio::Monitor> {
    let cli = redis::Client::open(url.as_ref()).context("Failed to open connection to")?;
    let mut mon = cli.get_async_connection().await?.into_monitor();
    mon.monitor().await?;
    Ok(mon)
}

async fn get_connection_pairs(
    addresses: Vec<RedisAddr>,
) -> Result<Vec<(RedisAddr, redis::aio::Monitor)>> {
    let mut res = vec![];

    for addr in addresses {
        let url = addr.get_url_string();
        println!("MONITOR {url}");

        let mon = get_monitor(&url)
            .await
            .with_context(|| format!("Failed to get connection for '{url}'"))?;
        res.push((addr, mon));
    }

    Ok(res)
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt: Options = Options::parse();
    let cfg = ConfigFile::load(opt.config_file);

    if opt.instances.is_empty() && opt.name.is_none() {
        eprintln!("Must pass at least one redis endpoint. (HOST:PORT, URI, etc)");
        std::process::exit(1);
    }

    let (addresses, cluster) = if let Some(name) = opt.name {
        let e = cfg.get(&name);
        (e.addresses.to_vec(), e.cluster.unwrap_or(false))
    } else {
        (opt.instances.to_vec(), opt.cluster)
    };

    let addresses = if cluster {
        Cluster::from_seeds(&addresses).unwrap().get_primaries()
    } else {
        addresses
    };

    let connections = get_connection_pairs(addresses).await.unwrap();

    let mut streams = futures::stream::select_all(connections.into_iter().map(move |(info, c)| {
        c.into_on_message::<String>()
            .map(move |c| (info.clone(), c))
    }));

    while let Some((uri, msg)) = streams.next().await {
        println!("[{uri}] {msg}");
    }

    Ok(())
}
