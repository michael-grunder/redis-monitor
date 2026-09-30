use std::{
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use config::{Config, File, FileFormat};
use serde::Deserialize;

use crate::connection::{ServerAddr, TlsConfig};

/// Config files searched, in order, in the current then home directory.
const DEFAULT_CFGFILE_NAMES: &[&str] =
    &[".redis-monitor", ".redis-monitor.toml"];

#[derive(Debug)]
pub struct Map(HashMap<String, Entry>);

#[derive(Debug, Clone, Default)]
pub struct ServerAuth {
    pub user: Option<String>,
    pub pass: Option<String>,
}

/// Credentials and TLS settings, shared by the CLI and config file entries.
#[derive(Debug, Default, Deserialize, clap::Args)]
pub struct ConnectionArgs {
    #[arg(short, long, help = "Redis user")]
    user: Option<String>,

    #[arg(short, long, short_alias = 'a', help = "Redis password")]
    pass: Option<String>,

    #[arg(long, help = "Connect using TLS")]
    #[serde(default)]
    tls: bool,

    #[arg(long, help = "Disable TLS certificate verification")]
    #[serde(default)]
    insecure: bool,

    #[arg(long, help = "Path to CA cert for TLS")]
    tls_ca: Option<PathBuf>,

    #[arg(long, help = "Path to client cert for TLS")]
    tls_cert: Option<PathBuf>,

    #[arg(long, help = "Path to client private key for TLS")]
    tls_key: Option<PathBuf>,
}

impl ConnectionArgs {
    pub fn auth(&self) -> ServerAuth {
        ServerAuth::from_user_pass(self.user.as_deref(), self.pass.as_deref())
    }

    /// Load TLS files when TLS is enabled.
    ///
    /// # Errors
    /// Returns an error for unreadable or invalid TLS settings.
    pub fn tls_config(&self) -> Result<Option<Arc<TlsConfig>>> {
        self.tls
            .then(|| {
                TlsConfig::new(
                    self.insecure,
                    self.tls_ca.as_deref(),
                    self.tls_cert.as_deref(),
                    self.tls_key.as_deref(),
                )
                .map(Arc::new)
            })
            .transpose()
    }
}

#[derive(Debug, Deserialize)]
pub struct Entry {
    addresses: Option<Vec<ServerAddr>>,
    path: Option<String>,
    host: Option<String>,
    port: Option<u16>,

    #[serde(flatten)]
    pub connection: ConnectionArgs,

    #[serde(default)]
    pub cluster: bool,
    // Unknown keys, including the formerly accepted per-entry `format` and
    // `color` settings, are ignored.
}

impl ServerAuth {
    pub fn from_user_pass(user: Option<&str>, pass: Option<&str>) -> Self {
        Self {
            user: user.map(str::to_owned),
            pass: pass.map(str::to_owned),
        }
    }
}

impl Map {
    fn find() -> Result<Option<PathBuf>> {
        let mut dirs = vec![env::current_dir().context(
            "Failed to determine the current directory while looking for a config file",
        )?];
        dirs.extend(env::var_os("HOME").map(PathBuf::from));
        Ok(dirs
            .iter()
            .flat_map(|dir| DEFAULT_CFGFILE_NAMES.iter().map(|n| dir.join(n)))
            .find(|path| path.exists()))
    }

    /// Load `path`, or the first default config file found, or nothing.
    ///
    /// # Errors
    /// Returns an error for unreadable or invalid config files.
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let Some(path) = path
            .map(Path::to_path_buf)
            .map_or_else(Self::find, |p| Ok(Some(p)))?
        else {
            return Ok(Self(HashMap::new()));
        };
        let entries = Config::builder()
            .add_source(File::from(path.clone()).format(FileFormat::Toml))
            .build()
            .and_then(Config::try_deserialize)
            .with_context(|| {
                format!("Failed to load config file {}", path.display())
            })?;
        Ok(Self(entries))
    }

    pub fn get<'a>(&'a self, name: &str) -> Option<&'a Entry> {
        self.0.get(name)
    }
}

impl Entry {
    pub fn get_addresses(&self) -> Result<Vec<ServerAddr>> {
        match (&self.host, self.port, &self.addresses, &self.path) {
            (Some(host), Some(port), ..) => {
                Ok(vec![ServerAddr::from_tcp_addr(host, port)])
            }
            (Some(_), None, ..) | (None, Some(_), ..) => {
                bail!("'host' and 'port' must be specified together")
            }
            (None, None, Some(addresses), _) if addresses.is_empty() => {
                bail!("'addresses' must contain at least one Redis address")
            }
            (None, None, Some(addresses), _) => Ok(addresses.clone()),
            (None, None, None, Some(path)) if path.is_empty() => {
                bail!("'path' must not be empty")
            }
            (None, None, None, Some(path)) => {
                Ok(vec![ServerAddr::from_path(path)])
            }
            (None, None, None, None) => bail!(
                "missing Redis address; specify 'host' with 'port', \
                 'addresses', or 'path'"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Map;

    #[test]
    fn entries_share_connection_settings_and_validate_addresses() {
        let dir = std::env::temp_dir()
            .join(format!("redis-monitor-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // Extensionless, like the default `.redis-monitor`.
        let path = dir.join(".redis-monitor");
        std::fs::write(
            &path,
            r#"
            [tls]
            host = "cache"
            port = 6380
            user = "u"
            pass = "p"
            tls = true
            insecure = true
            [plain]
            addresses = ["a:1", "[::1]:2"]
            [partial]
            host = "x"
            [empty]
            addresses = []
            [none]
            cluster = true
            "#,
        )
        .unwrap();
        let map = Map::load(Some(&path)).unwrap();
        std::fs::remove_dir_all(dir).unwrap();

        let tls = map.get("tls").unwrap();
        assert_eq!(tls.get_addresses().unwrap()[0].to_string(), "cache:6380");
        assert_eq!(tls.connection.auth().user.as_deref(), Some("u"));
        assert!(tls.connection.tls_config().unwrap().is_some());
        let plain = map.get("plain").unwrap();
        assert_eq!(plain.get_addresses().unwrap().len(), 2);
        assert!(plain.connection.tls_config().unwrap().is_none());
        for (name, error) in [
            ("partial", "together"),
            ("empty", "at least one"),
            ("none", "missing Redis address"),
        ] {
            let message = map
                .get(name)
                .unwrap()
                .get_addresses()
                .unwrap_err()
                .to_string();
            assert!(message.contains(error), "{name}: {message}");
        }
        assert!(
            Map::load(Some("/nonexistent/redis-monitor.toml".as_ref()))
                .is_err()
        );
    }
}
