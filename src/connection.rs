use std::string::ToString;
use std::{
    collections::HashMap,
    convert::AsRef,
    fs,
    io::Cursor,
    net::{IpAddr, Ipv6Addr},
    path::{Path, PathBuf},
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
    io::{
        AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt,
        BufReader,
    },
    net::{TcpStream, UnixStream},
};
use tokio_rustls::{TlsConnector, client::TlsStream as ClientTlsStream};

use crate::ServerAuth;

/// A MONITOR connection: TCP, TLS over TCP, or a Unix socket. Dispatch is
/// dynamic, but each read moves up to 64 KiB, so the call cost is negligible.
pub trait Connection: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Connection for T {}
pub type Stream = Box<dyn Connection>;

/// Longest handshake reply accepted, so a misbehaving server cannot grow the
/// reply buffer without bound.
const MAX_REPLY: u64 = 4096;

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ServerAddr {
    /// Host, port, and the host parsed as an IP address when it is one. The
    /// IP is derived from the host by `from_tcp_addr`, the only constructor,
    /// so derived equality and hashing agree with comparing host and port.
    Tcp(String, u16, Option<IpAddr>),
    Unix(String),
}

/// A cluster member as reported by `CLUSTER SLOTS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNode {
    pub id: String,
    pub addr: ServerAddr,
    pub primary: bool,
}

/// Cluster members, each with a unique ID and address, sorted by ID.
#[derive(Debug)]
pub struct Cluster(Vec<ClusterNode>);

impl<'de> Deserialize<'de> for ServerAddr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        FromStr::from_str(&s).map_err(de::Error::custom)
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

impl ServerAddr {
    /// The host, or the socket path.
    pub fn host(&self) -> &str {
        match self {
            Self::Tcp(host, ..) => host,
            Self::Unix(path) => path,
        }
    }

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

/// A `CLUSTER SLOTS` node entry: `[host, port, id, ...]`.
fn parse_slot_node(node: &Value) -> Result<(ServerAddr, String)> {
    let Value::Array(fields) = node else {
        bail!("node is not an array");
    };
    let [
        Value::BulkString(host),
        Value::Int(port),
        Value::BulkString(id),
        ..,
    ] = fields.as_slice()
    else {
        bail!("node lacks host, port, and ID fields");
    };
    let port = u16::try_from(*port)
        .ok()
        .filter(|&port| port != 0)
        .ok_or_else(|| anyhow!("node has an invalid port: {port}"))?;
    if host.is_empty() || id.is_empty() {
        bail!("node has an empty host or ID");
    }
    Ok((
        ServerAddr::from_tcp_addr(String::from_utf8_lossy(host), port),
        String::from_utf8_lossy(id).into_owned(),
    ))
}

impl Cluster {
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

    /// Parse `CLUSTER SLOTS`. Each slot range lists its primary, then its
    /// replicas. Nodes must keep one address and role across ranges.
    pub(crate) fn from_slots(value: Value) -> Result<Self> {
        let Value::Array(ranges) = value else {
            bail!("CLUSTER SLOTS returned a non-array response");
        };
        let mut nodes: HashMap<String, ClusterNode> = HashMap::new();
        let mut owners: HashMap<ServerAddr, String> = HashMap::new();
        for (index, range) in ranges.iter().enumerate() {
            let context = || format!("Invalid CLUSTER SLOTS entry {index}");
            let Value::Array(fields) = range else {
                bail!("CLUSTER SLOTS entry {index} is not an array");
            };
            let [Value::Int(start), Value::Int(end), members @ ..] =
                fields.as_slice()
            else {
                bail!("CLUSTER SLOTS entry {index} lacks a slot range");
            };
            if !(0..=16383).contains(start) || !(*start..=16383).contains(end) {
                bail!("Invalid CLUSTER SLOTS range at entry {index}");
            }
            if members.is_empty() {
                bail!("CLUSTER SLOTS entry {index} has no primary node");
            }
            for (position, member) in members.iter().enumerate() {
                let (addr, id) =
                    parse_slot_node(member).with_context(context)?;
                let node = ClusterNode {
                    id,
                    addr,
                    primary: position == 0,
                };
                let owner = owners
                    .entry(node.addr.clone())
                    .or_insert_with(|| node.id.clone());
                let known = nodes
                    .entry(node.id.clone())
                    .or_insert_with(|| node.clone());
                if *owner != node.id || *known != node {
                    bail!(
                        "CLUSTER SLOTS returned conflicting identities or roles \
                         for node {}",
                        node.id
                    );
                }
            }
        }
        if nodes.is_empty() {
            bail!("CLUSTER SLOTS returned no nodes");
        }
        let mut nodes: Vec<_> = nodes.into_values().collect();
        nodes.sort_unstable_by(|a, b| a.id.cmp(&b.id));
        Ok(Self(nodes))
    }

    pub fn nodes(&self) -> &[ClusterNode] {
        &self.0
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

    /// Connect, authenticate, and enter MONITOR mode. AUTH and MONITOR are
    /// pipelined in one round trip. Returns the stream and any bytes that
    /// arrived after the MONITOR reply: a busy server often sends its first
    /// records in the same segment as `+OK`.
    pub async fn connect(&self) -> Result<(Stream, BytesMut)> {
        let mut stream: Stream = match &self.address {
            ServerAddr::Tcp(host, port, _) => {
                let stream = TcpStream::connect((host.as_str(), *port)).await?;
                match &self.tls {
                    Some(tls) => {
                        Box::new(tls.initialize_tls(stream, host).await?)
                    }
                    None => Box::new(stream),
                }
            }
            ServerAddr::Unix(path) => {
                Box::new(UnixStream::connect(path).await?)
            }
        };

        let mut commands: Vec<&[&str]> = Vec::new();
        let auth = match (&self.auth.user, &self.auth.pass) {
            (Some(user), Some(pass)) => vec!["AUTH", user, pass],
            (None, Some(pass)) => vec!["AUTH", pass],
            _ => Vec::new(),
        };
        if !auth.is_empty() {
            commands.push(&auth);
        }
        commands.push(&["MONITOR"]);
        let mut request = Vec::new();
        for args in &commands {
            request.extend(format!("*{}\r\n", args.len()).bytes());
            for arg in *args {
                request.extend(format!("${}\r\n{arg}\r\n", arg.len()).bytes());
            }
        }
        stream.write_all(&request).await?;
        stream.flush().await?;

        let mut reader = BufReader::new(stream);
        for _ in &commands {
            read_reply(&mut reader).await?;
        }
        let pending = BytesMut::from(reader.buffer());
        Ok((reader.into_inner(), pending))
    }
}

/// Read one simple-string reply such as `+OK`, failing on an error reply.
async fn read_reply(reader: &mut BufReader<Stream>) -> Result<()> {
    let mut line = Vec::new();
    (&mut *reader)
        .take(MAX_REPLY)
        .read_until(b'\n', &mut line)
        .await?;
    if !line.ends_with(b"\n") {
        bail!("Connection closed or reply too long during the handshake");
    }
    let line = String::from_utf8_lossy(&line);
    let line = line.trim_end();
    match line.strip_prefix('+') {
        Some(_) => Ok(()),
        None => match line.strip_prefix('-') {
            Some(error) => bail!("Server error: {error}"),
            None => bail!("Unexpected handshake reply: {line}"),
        },
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

    /// Serve one connection: record the request bytes received before
    /// `reply` is sent, then send it and keep the socket open.
    async fn handshake(
        auth: crate::ServerAuth,
        reply: Vec<u8>,
    ) -> (anyhow::Result<()>, Vec<u8>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener =
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 256];
            let len = socket.read(&mut request).await.unwrap();
            request.truncate(len);
            socket.write_all(&reply).await.unwrap();
            (socket, request)
        });
        let monitor = super::Monitor::new(
            None,
            ServerAddr::from_tcp_addr("127.0.0.1", port),
            None,
            auth,
        );
        let result = monitor.connect().await.map(drop);
        let (_socket, request) = server.await.unwrap();
        (result, request)
    }

    #[tokio::test]
    async fn auth_and_monitor_are_pipelined_in_one_request() {
        let auth = crate::ServerAuth::from_user_pass(Some("u"), Some("p"));
        let (result, request) =
            handshake(auth, b"+OK\r\n+OK\r\n".to_vec()).await;
        result.unwrap();
        assert_eq!(
            request,
            b"*3\r\n$4\r\nAUTH\r\n$1\r\nu\r\n$1\r\np\r\n\
              *1\r\n$7\r\nMONITOR\r\n"
        );
    }

    #[tokio::test]
    async fn handshake_errors_and_oversized_replies_are_rejected() {
        let auth = crate::ServerAuth::from_user_pass(None, Some("bad"));
        let (result, _) = handshake(
            auth,
            b"-WRONGPASS invalid\r\n-NOAUTH required\r\n".to_vec(),
        )
        .await;
        let error = result.unwrap_err().to_string();
        assert!(error.contains("WRONGPASS"), "{error}");

        let endless = vec![b'+'; 10_000];
        let (result, _) =
            handshake(crate::ServerAuth::default(), endless).await;
        assert!(result.unwrap_err().to_string().contains("too long"));
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
