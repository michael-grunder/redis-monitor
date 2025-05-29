use crate::{ServerAuth, config::Entry};
use anyhow::{Context, Result, anyhow};
use colored::Color;
use redis::{Client, Connection, Value};
use rustls::client::danger::ServerCertVerifier;
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
};
use serde::Serialize;

use serde::{Deserialize, Deserializer, de};
use std::{
    collections::HashSet,
    convert::AsRef,
    fs,
    hash::{Hash, Hasher},
    io::{Cursor, Write},
    path::Path,
    pin::Pin,
    str::FromStr,
    sync::Arc,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpStream, UnixStream},
};
use tokio_rustls::{TlsConnector, client::TlsStream as ClientTlsStream};

#[derive(Debug)]
pub enum Stream {
    Tcp(TcpStream),
    Tls(Box<ClientTlsStream<TcpStream>>),
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
    pub tls: Option<Arc<TlsConfig>>,
    pub auth: ServerAuth,
    pub color: Option<Color>,
    pub format: String,
}

#[derive(Debug, Eq, Clone, Serialize)]
pub enum ServerAddr {
    Tcp(String, u16),
    Unix(String),
}

pub trait GetHost {
    fn get_host(&self) -> &str;
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
                Self::Unix(_) => false,
            },
            Self::Unix(path) => match other {
                Self::Unix(other_path) => path == other_path,
                Self::Tcp(..) => false,
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
                path.hash(state);
            }
        }
    }
}

impl Hash for Monitor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address.hash(state);
        self.auth.hash(state);
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
    fn get_host(&self) -> &str {
        match self {
            Self::Tcp(host, _) => host,
            Self::Unix(path) => path,
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

impl<S1, S2> From<&(S1, u16, S2)> for ClusterNode
where
    S1: AsRef<str>,
    S2: AsRef<str>,
{
    fn from(input: &(S1, u16, S2)) -> Self {
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
            .map_err(|e| anyhow!("Failed to execute CLUSTER SLOTS: {e}"))?;

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

                for replica in &entries[1..] {
                    primary.add_replica(replica.into());
                }

                primaries.insert(primary);
            }
        }

        Ok(primaries)
    }

    pub fn get_nodes(&self) -> Vec<ClusterNode> {
        self.0.iter().cloned().collect()
    }
}

impl Monitor {
    const DEFAULT_FORMAT: &'static str = "[%A]";

    pub fn from_config_entry(name: &str, entry: &Entry) -> Vec<Self> {
        if entry.cluster {
            let c = Cluster::from_seeds(&entry.get_addresses())
                .expect("Can't get cluster nodes");
            c.get_nodes()
                .iter()
                .map(|primary| {
                    let tls = entry.get_tls_config().unwrap_or_else(|e| {
                        panic!("Failed to create TLS config: {e}")
                    });

                    Self::new(
                        Some(name),
                        primary.addr.clone(),
                        tls.map(Arc::new),
                        entry.get_auth(),
                        entry.get_color(),
                        entry.format.as_deref(),
                    )
                })
                .collect()
        } else {
            entry
                .get_addresses()
                .iter()
                .map(|addr| {
                    Self::new(
                        Some(name),
                        addr.to_owned(),
                        None, // TODO: TLS config
                        entry.get_auth(),
                        entry.get_color(),
                        entry.format.as_deref(),
                    )
                })
                .collect()
        }
    }

    fn make_format_string(
        name: Option<&str>,
        address: &ServerAddr,
        format: &str,
    ) -> String {
        let mut format = format.to_owned();

        let vars: &[(&'static str, Option<String>)] = &[
            ("%A", Some(address.to_string())),
            ("%h", Some(address.get_host().into())),
            ("%p", address.get_port().map(|p| p.to_string())),
            ("%n", name.map(std::string::ToString::to_string)),
        ];

        for (var, value) in vars {
            format = format.replace(var, value.as_deref().unwrap_or(""));
        }

        format
    }

    pub fn new(
        name: Option<&str>,
        address: ServerAddr,
        tls: Option<Arc<TlsConfig>>,
        auth: ServerAuth,
        color: Option<Color>,
        format: Option<&str>,
    ) -> Self {
        let format = Self::make_format_string(
            name,
            &address,
            format.unwrap_or(Self::DEFAULT_FORMAT),
        );

        Self {
            address,
            tls,
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

        match line.chars().next() {
            Some('+') => Ok(line[1..].to_string()),
            Some('-') => Err(anyhow!("Server Error: {}", &line[1..])),
            Some(c) => Err(anyhow!("Got reply-type byte '{c}': {line}",)),
            _ => Err(anyhow!("Received empty line from server")),
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

                if let Some(tls) = &self.tls {
                    let stream = tls.initialize_tls(stream, host).await?;
                    Stream::Tls(Box::new(stream))
                } else {
                    Stream::Tcp(stream)
                }
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

impl TlsConfig {
    fn load_ca(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
        let buf = fs::read(path)
            .map_err(|e| anyhow!("Failed to read CA file: {e}"))?;

        let mut c = Cursor::new(buf);
        let parsed = rustls_pemfile::certs(&mut c)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow!("Failed to parse CA certs: {e}"))?;

        Ok(parsed.into_iter().collect::<Vec<_>>())
    }

    fn load_cert(path: &Path) -> Result<CertificateDer<'static>> {
        let buf = fs::read(path)
            .map_err(|e| anyhow!("Failed to read cert file: {e}"))?;

        let mut c = Cursor::new(buf);
        let parsed = rustls_pemfile::certs(&mut c)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow!("Failed to parse certs: {e}"))?;

        Ok(parsed[0].clone())
    }

    fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
        let buf = fs::read(path).with_context(|| {
            format!("Failed to read key file: {}", path.display())
        })?;

        let key = rustls_pemfile::private_key(&mut &buf[..])
            .map_err(|e| anyhow!("Failed to parse private key: {e}"))?
            .ok_or_else(|| {
                anyhow!("No private key found in file: {}", path.display())
            })?;

        Ok(key)
    }

    pub async fn initialize_tls(
        &self,
        stream: TcpStream,
        host: &str,
    ) -> Result<ClientTlsStream<TcpStream>> {
        let config = if self.insecure {
            let verifier = Arc::new(NoVerifier);
            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(verifier)
                .with_no_client_auth()
        } else {
            let mut root_cert_store = RootCertStore::empty();

            if let Some(ca_certs) = &self.ca {
                for cert in ca_certs {
                    root_cert_store.add(cert.clone()).map_err(|e| {
                        anyhow!("Failed to add CA certificate: {e}")
                    })?;
                }
            } else if !self.insecure {
                let native_certs = rustls_native_certs::load_native_certs();

                if !native_certs.errors.is_empty() {
                    let error_messages: Vec<String> = native_certs
                        .errors
                        .into_iter()
                        .map(|e| e.to_string())
                        .collect();
                    let error_summary = error_messages.join("; ");
                    return Err(anyhow!(
                        "Failed to load some system certificates: {error_summary}",
                    ));
                }

                for cert in native_certs.certs {
                    root_cert_store.add(cert).map_err(|e| {
                        anyhow!("Failed to add native certificate: {e}")
                    })?;
                }
            }

            let config =
                ClientConfig::builder().with_root_certificates(root_cert_store);

            if let (Some(cert), Some(key)) = (&self.cert, &self.key) {
                config
                    .with_client_auth_cert(vec![cert.clone()], key.clone_key())
                    .map_err(|e| anyhow!("Failed to set client auth: {e}"))?
            } else {
                config.with_no_client_auth()
            }
        };

        let connector = TlsConnector::from(Arc::new(config));
        let server_name = ServerName::try_from(host)
            .map_err(|e| anyhow!("Failed to create server name: {e}"))?;

        let tls_stream = connector
            .connect(server_name.to_owned(), stream)
            .await
            .map_err(|e| anyhow!("TLS handshake failed: {e}"))?;

        Ok(tls_stream)
    }

    pub fn new(
        insecure: bool,
        ca: Option<&Path>,
        cert: Option<&Path>,
        key: Option<&Path>,
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

#[derive(Debug)]
struct NoVerifier;

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error>
    {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error>
    {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::ED25519,
        ]
    }
}
