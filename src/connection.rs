use anyhow::{anyhow, Context, Result};
use redis::{Client, Connection, Value};
use serde::{de, Deserialize, Deserializer};
use std::convert::AsRef;
use std::hash::{Hash, Hasher};
use std::{collections::HashSet, str::FromStr};

#[derive(Debug, Eq, Clone)]
pub enum RedisAddr {
    Tcp(String, u16),
    Unix(String),
}

pub trait GetHost {
    fn get_host(&self) -> String;
}

pub trait GetPort {
    fn get_port(&self) -> Option<u16>;
}

#[derive(Debug, Eq, Clone)]
pub struct ClusterNode {
    pub id: String,
    pub addr: RedisAddr,
    pub replicas: HashSet<ClusterNode>,
}

#[derive(Debug)]
pub struct Cluster(HashSet<ClusterNode>);

impl<'de> Deserialize<'de> for RedisAddr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        FromStr::from_str(&s).map_err(de::Error::custom)
    }
}

impl PartialEq for RedisAddr {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Tcp(ahost, aport), Self::Tcp(bhost, bport)) => ahost == bhost && aport == bport,
            (Self::Unix(apath), Self::Unix(bpath)) => apath == bpath,
            _ => false,
        }
    }
}

impl PartialEq for ClusterNode {
    fn eq(&self, other: &Self) -> bool {
        self.addr == other.addr
    }
}

impl Hash for RedisAddr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Tcp(host, port) => {
                host.hash(state);
                port.hash(state);
            }
            Self::Unix(path) => path.hash(state),
        }
    }
}

impl Hash for ClusterNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
    }
}

impl std::fmt::Display for RedisAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Tcp(host, port) => write!(f, "{host}:{port}"),
            Self::Unix(path) => write!(f, "{path}"),
        }
    }
}

impl GetHost for RedisAddr {
    fn get_host(&self) -> String {
        match self {
            Self::Tcp(host, _) => host.to_string(),
            Self::Unix(path) => path.into(),
        }
    }
}

impl GetPort for RedisAddr {
    fn get_port(&self) -> Option<u16> {
        match self {
            Self::Tcp(_, port) => Some(*port),
            _ => None,
        }
    }
}

impl RedisAddr {
    pub fn from_tcp_addr<T: AsRef<str>>(host: T, port: u16) -> Self {
        Self::Tcp(host.as_ref().to_string(), port)
    }

    pub fn from_path<T: AsRef<str>>(path: T) -> Self {
        Self::Unix(path.as_ref().to_string())
    }

    pub fn get_url_string(&self) -> String {
        match self {
            Self::Tcp(host, port) => format!("redis://{host}:{port}"),
            Self::Unix(path) => format!("unix://{path}"),
        }
    }

    fn get_connection(&self) -> Result<Connection> {
        let uri = self.get_url_string();
        let cli =
            Client::open(&*uri).with_context(|| format!("Failed to open connection to {uri}"))?;
        let con = cli
            .get_connection()
            .context("Failed to get connection from client")?;

        Ok(con)
    }
}

impl std::str::FromStr for RedisAddr {
    type Err = anyhow::Error;

    fn from_str(addr: &str) -> Result<Self, Self::Err> {
        match addr.parse::<u16>() {
            Ok(port) => Ok(Self::from_tcp_addr("127.0.0.1", port)),
            _ => {
                if !addr.contains('/') {
                    let v: Vec<&str> = addr.split(':').collect();
                    if v.len() == 2 {
                        let port = v[1].parse::<u16>().unwrap();
                        Ok(Self::from_tcp_addr(v[0], port))
                    } else {
                        Err(anyhow!("Don't know how to parse address"))
                    }
                } else {
                    Ok(Self::from_path(addr))
                }
            }
        }
    }
}

impl ClusterNode {
    pub fn new(host: &str, port: u16, id: &str) -> Self {
        Self {
            id: id.to_string(),
            addr: RedisAddr::from_tcp_addr(host, port),
            replicas: HashSet::new(),
        }
    }

    pub fn add_replica(&mut self, node: ClusterNode) {
        if !self.replicas.contains(&node) {
            self.replicas.insert(node);
        }
    }
}

impl<S> From<&(S, u16, S)> for ClusterNode
where
    S: AsRef<str>,
{
    fn from(input: &(S, u16, S)) -> Self {
        Self::new(input.0.as_ref(), input.1, input.2.as_ref())
    }
}

//impl<'a> GetConnection for &'a RedisAddr {
//}

impl Cluster {
    fn new(primaries: HashSet<ClusterNode>) -> Self {
        Self(primaries)
    }

    fn parse_slot_bulk(host: &Value, port: &Value, id: &Value) -> Option<(String, u16, String)> {
        match (host, port, id) {
            (Value::Data(ref host), Value::Int(port), Value::Data(ref id)) => Some((
                String::from_utf8_lossy(host).to_string(),
                *port as u16,
                String::from_utf8_lossy(id).to_string(),
            )),
            _ => None,
        }
    }

    fn parse_nodes(nodes: &[Value]) -> Vec<(String, u16, String)> {
        nodes
            .iter()
            .filter_map(|node| {
                if let Value::Bulk(node) = node {
                    Self::parse_slot_bulk(&node[0], &node[1], &node[2])
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn from_seeds(seeds: &[RedisAddr]) -> Result<Self> {
        for seed in seeds {
            if let Ok(mut con) = seed.get_connection() {
                if let Ok(primaries) = Self::get_nodes(&mut con) {
                    return Ok(Self::new(primaries));
                }
            }
        }

        Err(anyhow!(
            "Unable to map cluster with any of the {} provided seeds",
            seeds.len()
        ))
    }

    fn get_nodes(con: &mut Connection) -> Result<HashSet<ClusterNode>> {
        let mut primaries = HashSet::new();

        let value = redis::cmd("CLUSTER")
            .arg("SLOTS")
            .query(con)
            .context("Failed to excute CLUSTER SLOTS")?;

        if let Value::Bulk(items) = value {
            let mut iter = items.into_iter();

            while let Some(Value::Bulk(item)) = iter.next() {
                if item.len() < 3 {
                    continue;
                }

                let entries = Self::parse_nodes(&item[2..]);

                if entries.is_empty() {
                    continue;
                }

                let mut primary: ClusterNode = (&entries[0]).into();

                if !primaries.contains(&primary) {
                    for replica in &entries[1..] {
                        let replica: ClusterNode = replica.into();
                        primary.add_replica(replica);
                    }
                    primaries.insert(primary);
                }
            }
        }

        Ok(primaries)
    }

    pub fn get_primary_nodes(&self) -> Vec<ClusterNode> {
        self.0.iter().map(|node| node.clone()).collect()
    }

    pub fn get_primaries(&self) -> Vec<RedisAddr> {
        self.0.iter().map(|node| node.addr.clone()).collect()
    }
}
