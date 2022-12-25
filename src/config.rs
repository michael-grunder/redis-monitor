use crate::connection::RedisAddr;
use anyhow::{Context, Result};
use colored::Color;
use config::{Config, File, FileFormat};
use serde::{de, Deserialize, Deserializer};
use std::{
    collections::HashMap, convert::AsRef, env, iter::IntoIterator, option::Option, path::PathBuf,
    str::FromStr,
};

const DEFAULT_CFGFILE_NAMES: &[&str] = &[".redis-monitor", "redis-monitor"];
const DEFAULT_CFGFILE_EXT: &[&str] = &["", "toml"];

#[derive(Debug)]
pub struct ConfigFile(HashMap<String, ConfigEntry>);

#[derive(Debug)]
pub struct DisplayColor(Color);

impl<'a> IntoIterator for &'a ConfigFile {
    type Item = <&'a HashMap<String, ConfigEntry> as IntoIterator>::Item;
    type IntoIter = <&'a HashMap<String, ConfigEntry> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Debug, Deserialize)]
pub struct ConfigEntry {
    pub addresses: Vec<RedisAddr>,

    #[serde(default)]
    pub cluster: bool,

    pub format: Option<String>,

    pub color: Option<DisplayColor>,
}

impl FromStr for DisplayColor {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(color) = Color::from_str(s) {
            Ok(Self(color))
        } else {
            Ok(Self(Color::Black))
        }
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

impl ConfigFile {
    fn find() -> Option<PathBuf> {
        let search_paths: Vec<PathBuf> = vec![
            env::current_dir().unwrap(),
            env::var("HOME").unwrap().into(),
        ];

        for path in &search_paths {
            for file in DEFAULT_CFGFILE_NAMES {
                for ext in DEFAULT_CFGFILE_EXT {
                    let filename = if ext.is_empty() {
                        file.to_string()
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

    fn from_toml_file<P: AsRef<str>>(path: P) -> Result<HashMap<String, ConfigEntry>> {
        let s = Config::builder()
            .add_source(File::new(path.as_ref(), FileFormat::Toml))
            .build()
            .context("TODO:  Error handling")?;

        let s = s.try_deserialize().context("Unable to open config file")?;

        Ok(s)
    }

    fn from_default_toml_file() -> Option<HashMap<String, ConfigEntry>> {
        if let Some(filename) = Self::find() {
            let str = filename.to_str().expect("TODO:  Can't unwrap filename");
            Some(Self::from_toml_file(str).expect("Failed to load config file"))
        } else {
            None
        }
    }

    pub fn load(path: Option<impl AsRef<str>>) -> Self {
        let cfg = if let Some(path) = path {
            Some(Self::from_toml_file(path).expect("Failed to load config file"))
        } else {
            Self::from_default_toml_file()
        };

        Self(cfg.unwrap_or_default())
    }

    pub fn get<'a>(&'a self, name: &str) -> Option<&'a ConfigEntry> {
        match self.0.get(name) {
            Some(entry) => Some(entry),
            _ => None,
        }
    }
}

impl ConfigEntry {
    pub fn get_color(&self) -> Color {
        if let Some(c) = &self.color {
            c.0
        } else {
            Color::Black
        }
    }
}
