use crate::{ServerAuth, config::Entry};
use anyhow::{Context, Result, anyhow};
use colored::Color;
use redis::{Client, Connection, Value};
use serde::{Deserialize, Deserializer, de};
use std::{
    collections::HashSet,
    convert::AsRef,
    fs,
    hash::{Hash, Hasher},
    io::{Cursor, Write},
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpStream, UnixStream},
};
use tokio_rustls::{
    TlsConnector, TlsStream, client::TlsStream as TokioTlsStream,
};

use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
};

#[derive(Debug)]
pub enum Stream {
    Tcp(TcpStream),
    Tls(TokioTlsStream<TcpStream>),
    Unix(UnixStream),
}

#[derive(Debug)]
pub struct TlsConfig {
    pub insecure: bool,
    pub ca: Option<Vec<CertificateDer<'static>>>,
    pub cert: Option<CertificateDer<'static>>,
    pub key: Option<PrivateKeyDer<'static>>,
}

#[derive(Debug, Clone)]
pub struct Monitor {
    pub address: ServerAddr,
    pub auth: ServerAuth,
    pub color: Option<Color>,
    pub format: String,
}

#[derive(Debug, Eq, Clone)]
pub enum ServerAddr {
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
    pub addr: ServerAddr,
    pub replicas: HashSet<ClusterNode>,
}

#[derive(Debug)]
pub struct Cluster(HashSet<ClusterNode>);

impl PartialEq for Monitor {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address
    }
}

impl Eq for Monitor {}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(s).poll_read(cx, buf),
            Self::Unix(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(s).poll_write(cx, buf),
            Self::Unix(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s).poll_flush(cx),
            Self::Unix(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s).poll_shutdown(cx),
            Self::Unix(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl<'de> Deserialize<'de> for ServerAddr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        FromStr::from_str(&s).map_err(de::Error::custom)
    }
}

impl PartialEq for ServerAddr {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Self::Tcp(host, port) => match other {
                Self::Tcp(other_host, other_port) => {
                    host == other_host && port == other_port
                }
                _ => false,
            },
            Self::Unix(path) => match other {
                Self::Unix(other_path) => path == other_path,
                _ => false,
            },
        }
    }
}

impl PartialEq for ClusterNode {
    fn eq(&self, other: &Self) -> bool {
        self.addr == other.addr
    }
}

impl Hash for ServerAddr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Tcp(host, port) => {
                0u8.hash(state);
                host.hash(state);
                port.hash(state);
            }
            Self::Unix(path) => {
                1u8.hash(state);
                path.hash(state)
            }
        }
    }
}

impl Hash for Monitor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // TODO: Theoretically we could have one address with different auth
        self.address.hash(state);
    }
}

impl Hash for ClusterNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
    }
}

impl std::fmt::Display for ServerAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Tcp(host, port) => write!(f, "{host}:{port}"),
            Self::Unix(path) => write!(f, "{path}"),
        }
    }
}

impl GetHost for ServerAddr {
    fn get_host(&self) -> String {
        match self {
            Self::Tcp(host, _) => host.to_string(),
            Self::Unix(path) => path.into(),
        }
    }
}

impl GetPort for ServerAddr {
    fn get_port(&self) -> Option<u16> {
        match self {
            Self::Tcp(_, port) => Some(*port),
            Self::Unix(_) => None,
        }
    }
}

impl ServerAddr {
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
        let cli = Client::open(&*uri)
            .with_context(|| format!("Failed to open connection to {uri}"))?;
        let con = cli
            .get_connection()
            .context("Failed to get connection from client")?;

        Ok(con)
    }
}

impl std::str::FromStr for ServerAddr {
    type Err = anyhow::Error;

    fn from_str(addr: &str) -> Result<Self, Self::Err> {
        addr.parse::<u16>().map_or_else(
            |_| {
                if addr.contains('/') {
                    Ok(Self::from_path(addr))
                } else {
                    let v: Vec<&str> = addr.split(':').collect();
                    if v.len() == 2 {
                        let port = v[1].parse::<u16>().unwrap();
                        Ok(Self::from_tcp_addr(v[0], port))
                    } else {
                        Err(anyhow!("Don't know how to parse address"))
                    }
                }
            },
            |port| Ok(Self::from_tcp_addr("127.0.0.1", port)),
        )
    }
}

impl ClusterNode {
    pub fn new(host: &str, port: u16, id: &str) -> Self {
        Self {
            id: id.to_string(),
            addr: ServerAddr::from_tcp_addr(host, port),
            replicas: HashSet::new(),
        }
    }

    pub fn add_replica(&mut self, node: Self) {
        self.replicas.insert(node);
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

impl Cluster {
    const fn new(primaries: HashSet<ClusterNode>) -> Self {
        Self(primaries)
    }

    fn parse_slot_bulk(
        host: &Value,
        port: &Value,
        id: &Value,
    ) -> Option<(String, u16, String)> {
        match (host, port, id) {
            (
                Value::BulkString(host),
                Value::Int(port),
                Value::BulkString(id),
            ) => Some((
                String::from_utf8_lossy(host).to_string(),
                u16::try_from(*port).expect("Failed to convert port to u16"),
                String::from_utf8_lossy(id).to_string(),
            )),
            _ => None,
        }
    }

    fn parse_nodes(nodes: &[Value]) -> Vec<(String, u16, String)> {
        nodes
            .iter()
            .filter_map(|node| {
                if let Value::Array(node) = node {
                    Self::parse_slot_bulk(&node[0], &node[1], &node[2])
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn from_seed(seed: &ServerAddr) -> Result<Self> {
        let mut con = seed.get_connection()?;
        Ok(Self::new(Self::exec_slots(&mut con)?))
    }

    pub fn from_seeds(seeds: &[ServerAddr]) -> Result<Self> {
        for seed in seeds {
            if let Ok(mut con) = seed.get_connection() {
                if let Ok(primaries) = Self::exec_slots(&mut con) {
                    return Ok(Self::new(primaries));
                }
            }
        }

        Err(anyhow!(
            "Unable to map cluster with any of the {} provided seeds",
            seeds.len()
        ))
    }

    fn exec_slots(con: &mut Connection) -> Result<HashSet<ClusterNode>> {
        let mut primaries = HashSet::new();

        let value = redis::cmd("CLUSTER")
            .arg("SLOTS")
            .query(con)
            .context("Failed to excute CLUSTER SLOTS")?;

        if let Value::Array(items) = value {
            let mut iter = items.into_iter();

            while let Some(Value::Array(item)) = iter.next() {
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
                        primary.add_replica(replica.into());
                    }
                    primaries.insert(primary);
                }
            }
        }

        Ok(primaries)
    }

    pub fn get_nodes(&self) -> Vec<ClusterNode> {
        self.0.iter().cloned().collect()
    }
}

impl Monitor {
    pub fn from_config_entry(name: &str, entry: &Entry) -> Vec<Self> {
        if entry.cluster {
            let c = Cluster::from_seeds(&entry.get_addresses())
                .expect("Can't get cluster nodes");
            c.get_nodes()
                .iter()
                .map(|primary| {
                    Self::new(
                        Some(name.to_owned()),
                        primary.addr.clone(),
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

    fn make_fmt_string(
        name: Option<&String>,
        address: &ServerAddr,
        format: &str,
    ) -> String {
        let mut fmt = format.to_owned();

        let vars: &[(&'static str, Option<String>)] = &[
            ("{address}", Some(address.to_string())),
            ("{host}", Some(address.get_host().to_string())),
            ("{port}", address.get_port().map(|p| p.to_string())),
            ("{name}", name.map(|n| n.to_string())),
        ];

        for (var, value) in vars {
            if let Some(v) = value {
                fmt = fmt.replace(var, v);
            }
        }

        fmt
    }

    pub fn new(
        name: Option<String>,
        address: ServerAddr,
        auth: ServerAuth,
        color: Option<Color>,
        format: Option<String>,
    ) -> Self {
        let format = Self::make_fmt_string(
            name.as_ref(),
            &address,
            &format.unwrap_or_else(|| "{address}".into()),
        );

        Self {
            address,
            auth,
            color,
            format,
        }
    }

    fn to_resp<S: AsRef<str>>(args: &[S]) -> Vec<u8> {
        assert!(!args.is_empty(), "Empty RESP commands are invalid");

        let mut out = vec![];
        write!(&mut out, "*{}\r\n", args.len()).unwrap();

        for arg in args {
            let s = arg.as_ref();
            write!(&mut out, "${}\r\n", s.len()).unwrap();
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }

        out
    }

    async fn send_resp(resp: &[u8], s: &mut Stream) -> Result<()> {
        s.write_all(resp).await?;
        s.flush().await?;

        Ok(())
    }

    async fn read_line_reply(s: &mut Stream) -> Result<String> {
        let mut reader = BufReader::new(s);
        let mut line = String::new();
        reader.read_line(&mut line).await?;

        if line.starts_with("+") {
            Ok(line[1..].trim_end().to_string())
        } else if line.starts_with("-") {
            Err(anyhow!(line[1..].trim_end().to_string()))
        } else {
            Err(anyhow!("Unexpected server reply: {}", line.trim_end()))
        }
    }

    async fn try_auth(auth: &ServerAuth, s: &mut Stream) -> Result<()> {
        let resp = match (&auth.user, &auth.pass) {
            (Some(user), Some(pass)) => {
                Self::to_resp(&[user.as_str(), pass.as_str()])
            }
            (None, Some(pass)) => Self::to_resp(&[pass.as_str()]),
            _ => return Ok(()),
        };

        Self::send_resp(&resp, s).await?;
        Self::read_line_reply(s).await?;

        Ok(())
    }

    async fn try_monitor(s: &mut Stream) -> Result<()> {
        let resp = Self::to_resp(&["MONITOR"]);

        Self::send_resp(&resp, s).await?;
        Self::read_line_reply(s).await?;

        Ok(())
    }

    pub async fn connect(self) -> Result<(Self, BufReader<Stream>)> {
        let mut stream = match &self.address {
            ServerAddr::Tcp(host, port) => {
                let stream = TcpStream::connect((host.as_str(), *port)).await?;
                Stream::Tcp(stream)
            }
            ServerAddr::Unix(path) => {
                let stream = UnixStream::connect(path).await?;
                Stream::Unix(stream)
            }
        };

        Self::try_auth(&self.auth, &mut stream)
            .await
            .context("Failed to authenticate")?;

        Self::try_monitor(&mut stream)
            .await
            .context("Failed to send MONITOR command")?;

        Ok((self, BufReader::new(stream)))
    }
}

impl From<ServerAddr> for Monitor {
    fn from(addr: ServerAddr) -> Self {
        let fmt = match addr {
            ServerAddr::Tcp(_, _) => "{host}:{port}",
            ServerAddr::Unix(_) => "{host}",
        };

        Self::new(
            None,
            addr,
            ServerAuth::default(),
            None,
            Some(fmt.to_owned()),
        )
    }
}

impl TlsConfig {
    fn load_ca(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
        let buf = fs::read(&path)
            .with_context(|| format!("Failed to read CA file: {path:?}"))?;

        let mut c = Cursor::new(buf);
        let parsed = rustls_pemfile::certs(&mut c)
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to parse CA certs")?;

        Ok(parsed
            .into_iter()
            .map(CertificateDer::from)
            .collect::<Vec<_>>())
    }

    fn load_cert(path: &Path) -> Result<CertificateDer<'static>> {
        let buf = fs::read(&path)
            .with_context(|| format!("Failed to read cert file: {path:?}"))?;

        let mut c = Cursor::new(buf);
        let parsed = rustls_pemfile::certs(&mut c)
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to parse cert")?;

        Ok(CertificateDer::from(parsed[0].clone()))
    }

    fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
        let buf = fs::read(&path)
            .with_context(|| format!("Failed to read key file: {path:?}"))?;

        let key = rustls_pemfile::private_key(&mut &buf[..])
            .context("Failed to parse private key")?
            .ok_or_else(|| anyhow!("No private key found in file: {path:?}"))?;

        Ok(key)
    }

    pub fn new(
        insecure: bool,
        ca: Option<PathBuf>,
        cert: Option<PathBuf>,
        key: Option<PathBuf>,
    ) -> Result<Self> {
        let ca = ca.as_ref().map(|p| Self::load_ca(p)).transpose()?;
        let cert = cert.as_ref().map(|p| Self::load_cert(p)).transpose()?;
        let key = key.as_ref().map(|p| Self::load_key(p)).transpose()?;

        Ok(Self {
            insecure,
            ca,
            cert,
            key,
        })
    }
}
