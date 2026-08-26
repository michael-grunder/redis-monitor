use std::{
    collections::HashMap,
    convert::AsRef,
    env,
    hash::{Hash, Hasher},
    iter::IntoIterator,
    option::Option,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Context, Result, anyhow, bail};
use colored::Color;
use config::{Config, File, FileFormat};
use redis::cmd;
use serde::{Deserialize, Deserializer, de};

use crate::connection::{ServerAddr, TlsConfig};

const DEFAULT_CFGFILE_NAMES: &[&str] = &[".redis-monitor"];
const DEFAULT_CFGFILE_EXT: &[&str] = &["", "toml"];

#[derive(Debug)]
pub struct Map(HashMap<String, Entry>);

#[derive(Debug)]
pub struct DisplayColor(Color);

#[derive(Debug, Clone, Default)]
pub struct ServerAuth {
    pub user: Option<String>,
    pub pass: Option<String>,
}

impl Hash for ServerAuth {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.user.hash(state);
        self.pass.hash(state);
    }
}

impl<'a> IntoIterator for &'a Map {
    type Item = <&'a HashMap<String, Entry> as IntoIterator>::Item;
    type IntoIter = <&'a HashMap<String, Entry> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Debug, Deserialize)]
pub struct Entry {
    addresses: Option<Vec<ServerAddr>>,
    path: Option<String>,
    host: Option<String>,
    port: Option<u16>,

    user: Option<String>,
    pass: Option<String>,

    tls: Option<bool>,
    insecure: Option<bool>,
    tls_ca: Option<PathBuf>,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,

    #[serde(default)]
    pub cluster: bool,

    pub format: Option<String>,

    pub color: Option<DisplayColor>,
}

impl FromStr for DisplayColor {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Color::from_str(s)
            .map_or_else(|()| Ok(Self(Color::Black)), |color| Ok(Self(color)))
    }
}

impl<'de> Deserialize<'de> for DisplayColor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        FromStr::from_str(&s).map_err(de::Error::custom)
    }
}

impl ServerAuth {
    pub fn from_user_pass(user: Option<&str>, pass: Option<&str>) -> Self {
        Self {
            user: user.map(std::borrow::ToOwned::to_owned),
            pass: pass.map(std::borrow::ToOwned::to_owned),
        }
    }

    pub async fn auth(
        &self,
        con: &mut redis::aio::ConnectionManager,
    ) -> Result<()> {
        if self.user.is_none() && self.pass.is_none() {
            return Ok(());
        }

        let mut command = cmd("AUTH");

        if let Some(user) = &self.user {
            command.arg(user);
        }
        if let Some(pass) = &self.pass {
            command.arg(pass);
        }

        match command.query_async(con).await {
            Ok(redis::Value::Okay) => Ok(()),
            other => {
                bail!("AUTH failed: unexpected response from Redis: {other:?}")
            }
        }
    }
}

impl Map {
    fn find() -> Result<Option<PathBuf>> {
        let mut search_paths = vec![env::current_dir()
            .context("Failed to determine the current directory while looking for a config file")?];
        if let Some(home) = env::var_os("HOME") {
            search_paths.push(home.into());
        }

        for path in &search_paths {
            for file in DEFAULT_CFGFILE_NAMES {
                for ext in DEFAULT_CFGFILE_EXT {
                    let filename = if ext.is_empty() {
                        (*file).to_string()
                    } else {
                        format!("{file}.{ext}")
                    };

                    let f = PathBuf::from(filename);
                    let check: PathBuf = [path, &f].iter().collect();

                    if check.exists() {
                        return Ok(Some(check));
                    }
                }
            }
        }

        Ok(None)
    }

    fn from_toml_file<P: AsRef<str>>(
        path: P,
    ) -> Result<HashMap<String, Entry>> {
        let s = Config::builder()
            .add_source(File::new(path.as_ref(), FileFormat::Toml))
            .build()
            .map_err(|e| {
                anyhow!("Failed to read config file {}: {e}", path.as_ref())
            })?;

        let s = s.try_deserialize().map_err(|e| {
            anyhow!("Failed to deserialize config file {}: {e}", path.as_ref())
        })?;

        Ok(s)
    }

    fn from_default_toml_file() -> Result<Option<HashMap<String, Entry>>> {
        let Some(file) = Self::find()? else {
            return Ok(None);
        };
        let path = file.to_str().ok_or_else(|| {
            anyhow!("Invalid UTF-8 in config file path: {}", file.display())
        })?;

        Self::from_toml_file(path).map(Some)
    }

    pub fn load(path: Option<&Path>) -> Result<Self> {
        let cfg = match path {
            Some(p) => {
                let path_str = p.to_str().ok_or_else(|| {
                    anyhow!(
                        "Invalid UTF-8 in config file path: {}",
                        p.display()
                    )
                })?;
                Some(Self::from_toml_file(path_str)?)
            }
            None => Self::from_default_toml_file()?,
        };

        Ok(Self(cfg.unwrap_or_default()))
    }

    pub fn get<'a>(&'a self, name: &str) -> Option<&'a Entry> {
        self.0.get(name)
    }
}

impl Entry {
    pub fn get_color(&self) -> Option<Color> {
        self.color.as_ref().map(|c| c.0)
    }

    fn host_port(&self) -> Option<(String, u16)> {
        match (&self.host, &self.port) {
            (Some(host), Some(port)) => Some((host.to_owned(), *port)),
            _ => None,
        }
    }

    pub fn get_auth(&self) -> ServerAuth {
        ServerAuth::from_user_pass(self.user.as_deref(), self.pass.as_deref())
    }

    pub fn get_addresses(&self) -> Result<Vec<ServerAddr>> {
        if let Some((host, port)) = self.host_port() {
            Ok(vec![ServerAddr::from_tcp_addr(host, port)])
        } else if self.host.is_some() || self.port.is_some() {
            bail!("'host' and 'port' must be specified together")
        } else if let Some(addresses) = &self.addresses {
            if addresses.is_empty() {
                bail!("'addresses' must contain at least one Redis address")
            }
            Ok(addresses.to_owned())
        } else if let Some(path) = &self.path {
            if path.is_empty() {
                bail!("'path' must not be empty")
            }
            Ok(vec![ServerAddr::from_path(path)])
        } else {
            bail!(
                "missing Redis address; specify 'host' with 'port', \
                 'addresses', or 'path'"
            )
        }
    }

    pub fn get_tls_config(&self) -> Result<Option<TlsConfig>> {
        if self.tls.unwrap_or(false) {
            let tls = TlsConfig::new(
                self.insecure.unwrap_or(false),
                self.tls_ca.as_deref(),
                self.tls_cert.as_deref(),
                self.tls_key.as_deref(),
            )?;
            Ok(Some(tls))
        } else {
            Ok(None)
        }
    }
}
