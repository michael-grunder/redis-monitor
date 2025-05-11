use crate::connection::RedisAddr;
use anyhow::{Context, Result};
use colored::Color;
use config::{Config, File, FileFormat};
use redis::{Cmd, cmd};

use serde::{Deserialize, Deserializer, de};
use std::{
    collections::HashMap, convert::AsRef, env, iter::IntoIterator,
    option::Option, path::PathBuf, str::FromStr,
};

const DEFAULT_CFGFILE_NAMES: &[&str] = &[".redis-monitor", "redis-monitor"];
const DEFAULT_CFGFILE_EXT: &[&str] = &["", "toml"];

#[derive(Debug)]
pub struct Map(HashMap<String, Entry>);

#[derive(Debug)]
pub struct DisplayColor(Color);

#[derive(Debug, Clone)]
pub enum RedisAuth {
    UserPass(String, String),
    Pass(String),
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
    addresses: Option<Vec<RedisAddr>>,
    path: Option<String>,
    host: Option<String>,
    port: Option<u16>,

    user: Option<String>,
    pass: Option<String>,

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

impl RedisAuth {
    fn from_pass(pass: &str) -> Self {
        Self::Pass(pass.to_owned())
    }

    fn from_user_pass(user: &str, pass: &str) -> Self {
        Self::UserPass(user.to_owned(), pass.to_owned())
    }

    pub fn get_command(&self) -> Cmd {
        let mut command = cmd("AUTH");

        match self {
            Self::UserPass(user, pass) => {
                command.arg(user).arg(pass);
            }
            Self::Pass(pass) => {
                command.arg(pass);
            }
        }

        command
    }

    pub async fn auth(&self, con: &mut redis::aio::ConnectionManager) -> bool {
        let command = self.get_command();
        matches!(command.query_async(con).await, Ok(redis::Value::Okay))
    }
}

impl Map {
    fn find() -> Option<PathBuf> {
        let search_paths: Vec<PathBuf> = vec![
            env::current_dir().unwrap(),
            env::var("HOME").unwrap().into(),
        ];

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
                        return Some(check);
                    }
                }
            }
        }

        None
    }

    fn from_toml_file<P: AsRef<str>>(
        path: P,
    ) -> Result<HashMap<String, Entry>> {
        let s = Config::builder()
            .add_source(File::new(path.as_ref(), FileFormat::Toml))
            .build()
            .context("TODO:  Error handling")?;

        let s = s.try_deserialize().context("Unable to open config file")?;

        Ok(s)
    }

    fn from_default_toml_file() -> Option<HashMap<String, Entry>> {
        Self::find().map(|filename| {
            let str = filename.to_str().expect("TODO:  Can't unwrap filename");
            Self::from_toml_file(str).expect("Failed to load config file")
        })
    }

    pub fn load(path: Option<impl AsRef<str>>) -> Self {
        let cfg = path.map_or_else(Self::from_default_toml_file, |path| {
            Some(
                Self::from_toml_file(path).expect("Failed to load config file"),
            )
        });

        Self(cfg.unwrap_or_default())
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

    pub fn get_auth(&self) -> Option<RedisAuth> {
        match (&self.user, &self.pass) {
            (Some(user), Some(pass)) => {
                Some(RedisAuth::from_user_pass(user, pass))
            }
            (None, Some(pass)) => Some(RedisAuth::from_pass(pass)),
            _ => None,
        }
    }

    pub fn get_addresses(&self) -> Vec<RedisAddr> {
        if let Some((host, port)) = self.host_port() {
            vec![RedisAddr::from_tcp_addr(host, port)]
        } else if let Some(addresses) = &self.addresses {
            addresses.to_owned()
        } else if let Some(path) = &self.path {
            vec![RedisAddr::from_path(path)]
        } else {
            panic!("Could not determine one or more Redis addresses");
        }
    }
}
