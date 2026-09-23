use std::string::ToString;
use std::{
    collections::HashSet,
    convert::AsRef,
    fs,
    hash::{Hash, Hasher},
    io::{Cursor, Write},
    net::{IpAddr, Ipv6Addr},
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use bytes::BytesMut;
use colored::Color;
use redis::{
    Client, Connection, ConnectionAddr, ConnectionInfo, IntoConnectionInfo,
    RedisConnectionInfo, Value,
};
use rustls::client::danger::ServerCertVerifier;
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
};
use serde::Serialize;
use serde::{Deserialize, Deserializer, de};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpStream, UnixStream},
};
use tokio_rustls::{TlsConnector, client::TlsStream as ClientTlsStream};

use crate::{ServerAuth, config::Entry};

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
    pub name: Option<String>,
    pub address: ServerAddr,
    pub tls: Option<Arc<TlsConfig>>,
    pub auth: ServerAuth,
    pub color: Option<Color>,
}

#[derive(Debug, Eq, Clone, Serialize)]
pub enum ServerAddr {
    Tcp(String, u16, #[serde(skip)] Option<IpAddr>),
    Unix(String),
}

pub trait GetHost {
    fn get_host(&self) -> &str;
}

#[derive(Debug, Eq, Clone)]
pub struct ClusterNode {
    pub id: String,
    pub addr: ServerAddr,
    pub replicas: HashSet<Self>,
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
            Self::Tcp(host, port, _) => match other {
                Self::Tcp(other_host, other_port, _) => {
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
            Self::Tcp(host, port, _) => {
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
            // Bracket IPv6 literals so the port separator is unambiguous.
            Self::Tcp(host, port, _) if host.contains(':') => {
                write!(f, "[{host}]:{port}")
            }
            Self::Tcp(host, port, _) => write!(f, "{host}:{port}"),
            Self::Unix(path) => write!(f, "{path}"),
        }
    }
}

impl GetHost for ServerAddr {
    fn get_host(&self) -> &str {
        match self {
            Self::Tcp(host, ..) => host,
            Self::Unix(path) => path,
        }
    }
}

impl ServerAddr {
    pub fn from_tcp_addr<T: AsRef<str>>(host: T, port: u16) -> Self {
        let host = host.as_ref();
        Self::Tcp(host.to_string(), port, host.parse().ok())
    }

    pub fn from_path<T: AsRef<str>>(path: T) -> Self {
        Self::Unix(path.as_ref().to_string())
    }

    fn get_connection(
        &self,
        auth: &ServerAuth,
        tls: Option<&TlsConfig>,
    ) -> Result<Connection> {
        let cli = Client::open(connection_info(self, auth, tls))
            .with_context(|| format!("Failed to open connection to {self}"))?;
        let con = cli.get_connection().map_err(|e| {
            anyhow!("Failed to get connection from client: {e}")
        })?;

        Ok(con)
    }
}

/// Build `redis` crate connection settings for an auxiliary (non-MONITOR)
/// connection such as cluster discovery or `COMMAND` metadata.
pub fn connection_info(
    address: &ServerAddr,
    auth: &ServerAuth,
    tls: Option<&TlsConfig>,
) -> ConnectionInfo {
    let addr = match address {
        ServerAddr::Tcp(host, port, _) => tls.map_or_else(
            || ConnectionAddr::Tcp(host.clone(), *port),
            |tls| ConnectionAddr::TcpTls {
                host: host.clone(),
                port: *port,
                insecure: tls.insecure,
                tls_params: None,
            },
        ),
        ServerAddr::Unix(path) => ConnectionAddr::Unix(PathBuf::from(path)),
    };

    let mut redis = RedisConnectionInfo::default();
    if let Some(user) = &auth.user {
        redis = redis.set_username(user);
    }
    if let Some(pass) = &auth.pass {
        redis = redis.set_password(pass);
    }

    addr.into_connection_info()
        .expect("ConnectionAddr::into_connection_info cannot fail")
        .set_redis_settings(redis)
}

const DEFAULT_PORT: u16 = 6379;

impl std::str::FromStr for ServerAddr {
    type Err = anyhow::Error;

    /// Accepts `port`, `host`, `host:port`, `[ipv6]`, `[ipv6]:port`, a bare
    /// IPv6 literal, or a unix socket path (anything containing `/`).
    fn from_str(addr: &str) -> Result<Self, Self::Err> {
        let parse_port = |port: &str| {
            port.parse::<u16>()
                .with_context(|| format!("Invalid port '{port}' in '{addr}'"))
        };

        if let Ok(port) = addr.parse::<u16>() {
            return Ok(Self::from_tcp_addr("127.0.0.1", port));
        }
        if addr.contains('/') {
            return Ok(Self::from_path(addr));
        }
        if addr.parse::<Ipv6Addr>().is_ok() {
            return Ok(Self::from_tcp_addr(addr, DEFAULT_PORT));
        }

        let (host, port) = if let Some(rest) = addr.strip_prefix('[') {
            let (host, rest) = rest.split_once(']').ok_or_else(|| {
                anyhow!("Missing ']' in bracketed address '{addr}'")
            })?;
            let port = match rest {
                "" => DEFAULT_PORT,
                _ => parse_port(rest.strip_prefix(':').ok_or_else(|| {
                    anyhow!("Expected ':<port>' after ']' in '{addr}'")
                })?)?,
            };
            (host, port)
        } else if let Some((host, port)) = addr.split_once(':') {
            (host, parse_port(port)?)
        } else {
            (addr, DEFAULT_PORT)
        };

        if host.is_empty() {
            bail!("Missing host in address '{addr}'");
        }

        Ok(Self::from_tcp_addr(host, port))
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
    ) -> Result<(String, u16, String)> {
        match (host, port, id) {
            (
                Value::BulkString(host),
                Value::Int(port),
                Value::BulkString(id),
            ) => {
                let port = u16::try_from(*port).map_err(|_| {
                    anyhow!(
                        "Redis Cluster returned an out-of-range node port: \
                         {port}"
                    )
                })?;
                Ok((
                    String::from_utf8_lossy(host).to_string(),
                    port,
                    String::from_utf8_lossy(id).to_string(),
                ))
            }
            _ => bail!(
                "Redis Cluster returned a node with invalid host, port, or ID \
                 fields"
            ),
        }
    }

    fn parse_nodes(nodes: &[Value]) -> Result<Vec<(String, u16, String)>> {
        nodes
            .iter()
            .enumerate()
            .map(|(index, node)| {
                let Value::Array(node) = node else {
                    bail!("Redis Cluster node {index} is not an array");
                };
                let [host, port, id, ..] = node.as_slice() else {
                    bail!(
                        "Redis Cluster node {index} has {} fields; expected at \
                         least 3",
                        node.len()
                    );
                };

                Self::parse_slot_bulk(host, port, id).with_context(|| {
                    format!("Invalid Redis Cluster node {index}")
                })
            })
            .collect()
    }

    pub fn from_seed(
        seed: &ServerAddr,
        auth: &ServerAuth,
        tls: Option<&TlsConfig>,
    ) -> Result<Self> {
        let mut con = seed.get_connection(auth, tls)?;
        Ok(Self::new(Self::exec_slots(&mut con)?))
    }

    pub fn from_seeds(
        seeds: &[ServerAddr],
        auth: &ServerAuth,
        tls: Option<&TlsConfig>,
    ) -> Result<Self> {
        let mut last_error = None;
        for seed in seeds {
            match Self::from_seed(seed, auth, tls) {
                Ok(cluster) => return Ok(cluster),
                Err(error) => last_error = Some((seed, error)),
            }
        }

        let Some((seed, error)) = last_error else {
            bail!("No Redis Cluster seeds were configured");
        };
        Err(error).with_context(|| {
            format!(
                "Failed to discover a Redis Cluster from any of the {} \
                 configured seeds; last attempted {seed}",
                seeds.len()
            )
        })
    }

    fn exec_slots(con: &mut Connection) -> Result<HashSet<ClusterNode>> {
        let mut primaries = HashSet::new();

        let value = redis::cmd("CLUSTER")
            .arg("SLOTS")
            .query(con)
            .map_err(|e| anyhow!("Failed to execute CLUSTER SLOTS: {e}"))?;

        let Value::Array(items) = value else {
            bail!("CLUSTER SLOTS returned a non-array response");
        };

        for (slot_index, item) in items.into_iter().enumerate() {
            let Value::Array(item) = item else {
                bail!("CLUSTER SLOTS entry {slot_index} is not an array");
            };
            if item.len() < 3 {
                bail!(
                    "CLUSTER SLOTS entry {slot_index} has {} fields; expected \
                     at least 3",
                    item.len()
                );
            }

            let entries = Self::parse_nodes(&item[2..]).with_context(|| {
                format!("Invalid CLUSTER SLOTS entry {slot_index}")
            })?;
            let Some((primary, replicas)) = entries.split_first() else {
                bail!(
                    "CLUSTER SLOTS entry {slot_index} does not contain a \
                     primary node"
                );
            };
            let mut primary: ClusterNode = primary.into();

            for replica in replicas {
                primary.add_replica(replica.into());
            }

            primaries.insert(primary);
        }

        if primaries.is_empty() {
            bail!("CLUSTER SLOTS returned no primary nodes");
        }

        Ok(primaries)
    }

    pub fn get_nodes(&self) -> Vec<ClusterNode> {
        self.0.iter().cloned().collect()
    }
}

impl Monitor {
    pub fn from_config_entry(name: &str, entry: &Entry) -> Result<Vec<Self>> {
        let addresses = entry.get_addresses().with_context(|| {
            format!("Invalid configuration for instance '{name}'")
        })?;
        let tls = entry
            .get_tls_config()
            .with_context(|| {
                format!("Failed to configure TLS for instance '{name}'")
            })?
            .map(Arc::new);

        if entry.cluster {
            let c = Cluster::from_seeds(
                &addresses,
                &entry.get_auth(),
                tls.as_deref(),
            )
            .with_context(|| {
                format!(
                    "Failed to discover the cluster for configured instance \
                     '{name}'"
                )
            })?;
            Ok(c.get_nodes()
                .into_iter()
                .map(|primary| {
                    Self::new(
                        Some(name),
                        primary.addr,
                        tls.clone(),
                        entry.get_auth(),
                        entry.get_color(),
                    )
                })
                .collect())
        } else {
            Ok(addresses
                .into_iter()
                .map(|addr| {
                    Self::new(
                        Some(name),
                        addr,
                        tls.clone(),
                        entry.get_auth(),
                        entry.get_color(),
                    )
                })
                .collect())
        }
    }

    pub fn new(
        name: Option<&str>,
        address: ServerAddr,
        tls: Option<Arc<TlsConfig>>,
        auth: ServerAuth,
        color: Option<Color>,
    ) -> Self {
        Self {
            name: name.map(ToString::to_string),
            address,
            tls,
            auth,
            color,
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

    async fn read_line_reply(reader: &mut BufReader<Stream>) -> Result<String> {
        let mut line = String::new();

        reader.read_line(&mut line).await?;
        let line = line.trim_end();

        match line.chars().next() {
            Some('+') => Ok(line[1..].to_string()),
            Some('-') => Err(anyhow!("Server Error: {}", &line[1..])),
            Some(c) => Err(anyhow!("Got reply-type byte '{c}': {line}")),
            _ => Err(anyhow!("Received empty line from server")),
        }
    }

    async fn try_auth(
        auth: &ServerAuth,
        s: &mut BufReader<Stream>,
    ) -> Result<()> {
        let resp = match (&auth.user, &auth.pass) {
            (Some(user), Some(pass)) => {
                Self::to_resp(&["AUTH", user.as_str(), pass.as_str()])
            }
            (None, Some(pass)) => Self::to_resp(&["AUTH", pass.as_str()]),
            _ => return Ok(()),
        };

        Self::send_resp(&resp, s.get_mut()).await?;
        Self::read_line_reply(s).await?;

        Ok(())
    }

    async fn try_monitor(s: &mut BufReader<Stream>) -> Result<()> {
        let resp = Self::to_resp(&["MONITOR"]);

        Self::send_resp(&resp, s.get_mut()).await?;
        Self::read_line_reply(s).await?;

        Ok(())
    }

    /// Connect, authenticate, and enter MONITOR mode. Returns the stream and
    /// any bytes that arrived after the MONITOR reply: a busy server often
    /// sends its first records in the same segment as `+OK`.
    pub async fn connect(&self) -> Result<(Stream, BytesMut)> {
        let stream = match &self.address {
            ServerAddr::Tcp(host, port, _) => {
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

        let mut reader = BufReader::new(stream);
        Self::try_auth(&self.auth, &mut reader).await?;
        Self::try_monitor(&mut reader).await?;

        let pending = BytesMut::from(reader.buffer());
        Ok((reader.into_inner(), pending))
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

        parsed.into_iter().next().ok_or_else(|| {
            anyhow!("No certificate found in cert file: {}", path.display())
        })
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

#[cfg(test)]
mod tests {
    use super::ServerAddr;

    #[test]
    fn server_address_serialization_omits_normalized_ip_cache() {
        let address = ServerAddr::from_tcp_addr("127.0.0.1", 6379);

        assert_eq!(
            serde_json::to_string(&address).unwrap(),
            r#"{"Tcp":["127.0.0.1",6379]}"#
        );
    }

    #[test]
    fn parses_host_port_and_ipv6_forms() {
        let cases: &[(&str, &str, u16)] = &[
            ("7000", "127.0.0.1", 7000),
            ("redis.example", "redis.example", 6379),
            ("redis.example:7000", "redis.example", 7000),
            ("::1", "::1", 6379),
            ("[::1]", "::1", 6379),
            ("[2001:db8::1]:7000", "2001:db8::1", 7000),
        ];

        for (input, host, port) in cases {
            let ServerAddr::Tcp(h, p, _) = input.parse::<ServerAddr>().unwrap()
            else {
                panic!("expected TCP address for {input}");
            };
            assert_eq!((h.as_str(), p), (*host, *port), "{input}");
        }

        assert!(matches!(
            "/tmp/redis.sock".parse::<ServerAddr>().unwrap(),
            ServerAddr::Unix(_)
        ));
    }

    #[test]
    fn rejects_malformed_addresses() {
        for input in [
            "",
            ":6379",
            "host:",
            "host:99999",
            "[::1",
            "[::1]x",
            "a:b:c",
        ] {
            assert!(input.parse::<ServerAddr>().is_err(), "accepted {input}");
        }
    }

    #[test]
    fn ipv6_addresses_display_with_brackets() {
        assert_eq!(
            ServerAddr::from_tcp_addr("::1", 6379).to_string(),
            "[::1]:6379"
        );
        assert_eq!(
            ServerAddr::from_tcp_addr("127.0.0.1", 6379).to_string(),
            "127.0.0.1:6379"
        );
    }

    #[tokio::test]
    async fn records_sent_with_the_monitor_reply_are_kept() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener =
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 64];
            let _ = socket.read(&mut request).await.unwrap();
            // A busy server's first record often shares a segment with +OK.
            socket
                .write_all(b"+OK\r\n+1.0 [0 127.0.0.1:1] \"PING\"\r\n")
                .await
                .unwrap();
            socket
        });

        let monitor = super::Monitor::new(
            None,
            ServerAddr::from_tcp_addr("127.0.0.1", port),
            None,
            crate::ServerAuth::default(),
            None,
        );
        let (_stream, pending) = monitor.connect().await.unwrap();

        assert_eq!(&pending[..], b"+1.0 [0 127.0.0.1:1] \"PING\"\r\n");
        drop(server.await.unwrap());
    }
}
