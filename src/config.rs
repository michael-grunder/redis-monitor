use crate::connection::RedisAddr;
use anyhow::{Context, Result};
use config::{Config, File, FileFormat};
use serde::Deserialize;
use std::{collections::HashMap, convert::AsRef, env, option::Option, path::Path};

#[derive(Debug)]
pub struct ConfigFile(HashMap<String, ConfigEntry>);

#[derive(Debug, Deserialize)]
pub struct ConfigEntry {
    pub addresses: Vec<RedisAddr>,
    pub cluster: Option<bool>,
}

pub fn default_config_file() -> String {
    let home = env::var("HOME").expect("Failed to read HOME directory");
    format!("{home}/.redis-monitor.toml")
}

impl ConfigFile {
    fn from_toml_file<P: AsRef<str>>(path: P) -> Result<HashMap<String, ConfigEntry>> {
        let s = Config::builder()
            .add_source(File::new(path.as_ref(), FileFormat::Toml))
            .build()
            .context("TODO:  Error handling")?;

        let s = s.try_deserialize().context("Unable to open config file")?;

        println!("{s:#?}");
        Ok(s)
    }

    fn from_default_toml_file() -> Option<HashMap<String, ConfigEntry>> {
        let file = default_config_file();
        let path = Path::new(&file);

        if !path.exists() {
            return None;
        }

        Some(Self::from_toml_file(path.to_str().unwrap()).expect("TODO"))
    }

    pub fn load(path: Option<impl AsRef<str>>) -> Self {
        let cfg = if let Some(path) = path {
            Some(Self::from_toml_file(path).expect("Failed to load config file"))
        } else {
            Self::from_default_toml_file()
        };

        Self(cfg.unwrap_or(HashMap::new()))
    }

    pub fn get<'a>(&'a self, name: &str) -> &'a ConfigEntry {
        match self.0.get(name) {
            Some(entry) => entry,
            _ => panic!("Didn't find {name} in config file"),
        }
    }
}
