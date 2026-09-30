use std::string::ToString;
use std::{
    collections::{HashMap, HashSet},
    convert::AsRef,
    fs,
    hash::{Hash, Hasher},
    io::{Cursor, Write},
    net::{IpAddr, Ipv6Addr},
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use bytes::BytesMut;
use redis::{
    Client, ClientTlsConfig, ConnectionAddr, IntoConnectionInfo,
    RedisConnectionInfo, TlsCertificates, Value,
};
use rustls::client::danger::ServerCertVerifier;
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
};
use serde::{Deserialize, Deserializer, de};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpStream, UnixStream},
};
use tokio_rustls::{TlsConnector, client::TlsStream as ClientTlsStream};

use crate::ServerAuth;

#[derive(Debug)]
pub enum Stream {
    Tcp(TcpStream),
    Tls(Box<ClientTlsStream<TcpStream>>),
    Unix(UnixStream),
}

/// TLS settings, validated and compiled once at startup.
pub struct TlsConfig {
    insecure: bool,
    /// PEM file contents, for auxiliary connections made by the `redis`
    /// crate (cluster discovery and `COMMAND` metadata).
    ca_pem: Option<Vec<u8>>,
    client_pem: Option<ClientTlsConfig>,
    /// Shared by every MONITOR connection.
    connector: TlsConnector,
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfig")
            .field("insecure", &self.insecure)
            .field("custom_ca", &self.ca_pem.is_some())
            .field("client_cert", &self.client_pem.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct Monitor {
    pub name: Option<String>,
    pub address: ServerAddr,
    pub tls: Option<Arc<TlsConfig>>,
    pub auth: ServerAuth,
}

#[derive(Debug, Eq, Clone)]
pub enum ServerAddr {
    /// Host, port, and the host parsed as an IP address when it is one.
    Tcp(String, u16, Option<IpAddr>),
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
}

/// Build a `redis` crate client for an auxiliary (non-MONITOR) connection
/// such as cluster discovery or `COMMAND` metadata, using the same
/// credentials and TLS settings as MONITOR connections.
///
/// # Errors
/// Returns an error if the client cannot be configured.
pub fn client(
    address: &ServerAddr,
    auth: &ServerAuth,
    tls: Option<&TlsConfig>,
) -> Result<Client> {
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

    let info = addr
        .into_connection_info()
        .with_context(|| format!("Invalid connection settings for {address}"))?
        .set_redis_settings(redis);
    let client = match tls {
        Some(tls) if tls.ca_pem.is_some() || tls.client_pem.is_some() => {
            Client::build_with_tls(
                info,
                TlsCertificates {
                    client_tls: tls.client_pem.clone(),
                    root_cert: tls.ca_pem.clone(),
                },
            )
        }
        _ => Client::open(info),
    };
    client
        .with_context(|| format!("Failed to configure a client for {address}"))
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
                if host.is_empty() || id.is_empty() || port == 0 {
                    bail!(
                        "Redis Cluster returned an empty host/ID or zero port"
                    );
                }
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

    pub async fn from_seed(
        seed: &ServerAddr,
        auth: &ServerAuth,
        tls: Option<&TlsConfig>,
    ) -> Result<Self> {
        // This includes connection setup, authentication, and the query. Dropping
        // the async future cancels discovery during shutdown.
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut con = client(seed, auth, tls)?
                .get_multiplexed_async_connection()
                .await
                .context("Failed to connect for cluster discovery")?;
            let value = redis::cmd("CLUSTER")
                .arg("SLOTS")
                .query_async(&mut con)
                .await
                .context("Failed to execute CLUSTER SLOTS")?;
            Self::from_slots(value)
        })
        .await
        .context("Cluster discovery timed out after 10 seconds")?
    }

    pub async fn from_seeds(
        seeds: &[ServerAddr],
        auth: &ServerAuth,
        tls: Option<&TlsConfig>,
    ) -> Result<Self> {
        let mut last_error = None;
        for seed in seeds {
            match Self::from_seed(seed, auth, tls).await {
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

    pub(crate) fn from_slots(value: Value) -> Result<Self> {
        let mut primaries: HashSet<ClusterNode> = HashSet::new();
        let mut identities = HashMap::new();
        let mut addresses = HashMap::new();

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
            if !matches!((&item[0], &item[1]), (Value::Int(start), Value::Int(end))
                if (0..=16383).contains(start) && (*start..=16383).contains(end))
            {
                bail!("Invalid CLUSTER SLOTS range at entry {slot_index}");
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
            for (index, (host, port, id)) in entries.iter().enumerate() {
                let address = ServerAddr::from_tcp_addr(host, *port);
                let identity = (address.clone(), index == 0);
                if identities
                    .insert(id.clone(), identity.clone())
                    .is_some_and(|old| old != identity)
                    || addresses
                        .insert(address, id.clone())
                        .is_some_and(|old| old != *id)
                {
                    bail!(
                        "CLUSTER SLOTS returned conflicting identities or roles for node {id}"
                    );
                }
            }
            let mut primary: ClusterNode = primary.into();

            for replica in replicas {
                primary.add_replica(replica.into());
            }

            if let Some(previous) = primaries.take(&primary) {
                primary.replicas.extend(previous.replicas);
            }
            primaries.insert(primary);
        }

        if primaries.is_empty() {
            bail!("CLUSTER SLOTS returned no primary nodes");
        }

        Ok(Self::new(primaries))
    }

    pub fn get_nodes(&self) -> Vec<ClusterNode> {
        self.0.iter().cloned().collect()
    }
}

impl Monitor {
    pub fn new(
        name: Option<&str>,
        address: ServerAddr,
        tls: Option<Arc<TlsConfig>>,
        auth: ServerAuth,
    ) -> Self {
        Self {
            name: name.map(ToString::to_string),
            address,
            tls,
            auth,
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
    fn read(path: &Path, what: &str) -> Result<Vec<u8>> {
        fs::read(path).with_context(|| {
            format!("Failed to read {what} file {}", path.display())
        })
    }

    fn parse_certs(
        pem: &[u8],
        path: &Path,
    ) -> Result<Vec<CertificateDer<'static>>> {
        let certs = rustls_pemfile::certs(&mut Cursor::new(pem))
            .collect::<Result<Vec<_>, _>>()
            .with_context(|| {
                format!("Failed to parse certificates in {}", path.display())
            })?;
        if certs.is_empty() {
            bail!("No certificate found in {}", path.display());
        }
        Ok(certs)
    }

    fn parse_key(pem: &[u8], path: &Path) -> Result<PrivateKeyDer<'static>> {
        rustls_pemfile::private_key(&mut Cursor::new(pem))
            .with_context(|| {
                format!("Failed to parse private key in {}", path.display())
            })?
            .ok_or_else(|| {
                anyhow!("No private key found in {}", path.display())
            })
    }

    fn roots(
        ca: Option<Vec<CertificateDer<'static>>>,
    ) -> Result<RootCertStore> {
        let mut roots = RootCertStore::empty();
        if let Some(ca) = ca {
            for cert in ca {
                roots.add(cert).context("Failed to add CA certificate")?;
            }
            return Ok(roots);
        }

        let native = rustls_native_certs::load_native_certs();
        if !native.errors.is_empty() {
            let errors: Vec<String> =
                native.errors.iter().map(ToString::to_string).collect();
            bail!(
                "Failed to load some system certificates: {}",
                errors.join("; ")
            );
        }
        for cert in native.certs {
            roots
                .add(cert)
                .context("Failed to add native certificate")?;
        }
        Ok(roots)
    }

    /// Load and validate TLS files, and build the connector shared by every
    /// MONITOR connection.
    ///
    /// # Errors
    /// Returns an error for unreadable or invalid files, a certificate
    /// without a key (or vice versa), or unusable system roots.
    pub fn new(
        insecure: bool,
        ca: Option<&Path>,
        cert: Option<&Path>,
        key: Option<&Path>,
    ) -> Result<Self> {
        let ca_pem = ca.map(|path| Self::read(path, "CA")).transpose()?;
        let ca_certs = ca
            .zip(ca_pem.as_deref())
            .map(|(path, pem)| Self::parse_certs(pem, path))
            .transpose()?;

        let (client_pem, client_auth) = match (cert, key) {
            (Some(cert_path), Some(key_path)) => {
                let cert_pem = Self::read(cert_path, "certificate")?;
                let key_pem = Self::read(key_path, "private key")?;
                let certs = Self::parse_certs(&cert_pem, cert_path)?;
                let key = Self::parse_key(&key_pem, key_path)?;
                (
                    Some(ClientTlsConfig {
                        client_cert: cert_pem,
                        client_key: key_pem,
                    }),
                    Some((certs, key)),
                )
            }
            (None, None) => (None, None),
            _ => bail!(
                "A TLS client certificate and private key must be given \
                 together"
            ),
        };

        let builder = if insecure {
            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerifier))
        } else {
            ClientConfig::builder()
                .with_root_certificates(Self::roots(ca_certs)?)
        };
        let config = match client_auth {
            Some((certs, key)) => builder
                .with_client_auth_cert(certs, key)
                .context("Failed to set TLS client authentication")?,
            None => builder.with_no_client_auth(),
        };

        Ok(Self {
            insecure,
            ca_pem,
            client_pem,
            connector: TlsConnector::from(Arc::new(config)),
        })
    }

    pub async fn initialize_tls(
        &self,
        stream: TcpStream,
        host: &str,
    ) -> Result<ClientTlsStream<TcpStream>> {
        let server_name = ServerName::try_from(host.to_owned())
            .map_err(|e| anyhow!("Invalid TLS server name '{host}': {e}"))?;

        self.connector
            .connect(server_name, stream)
            .await
            .context("TLS handshake failed")
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
    fn tls_client_certificate_and_key_must_be_paired() {
        let path = std::path::Path::new("unused.pem");
        for (cert, key) in [(Some(path), None), (None, Some(path))] {
            let error = super::TlsConfig::new(true, None, cert, key)
                .unwrap_err()
                .to_string();
            assert!(error.contains("together"), "{error}");
        }
        assert!(super::TlsConfig::new(true, None, None, None).is_ok());
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
        );
        let (_stream, pending) = monitor.connect().await.unwrap();

        assert_eq!(&pending[..], b"+1.0 [0 127.0.0.1:1] \"PING\"\r\n");
        drop(server.await.unwrap());
    }
}
