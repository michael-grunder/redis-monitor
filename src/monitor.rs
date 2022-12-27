use crate::config::{ConfigEntry, RedisAuth};
use crate::connection::*;
use crate::CommandStats;
use colored::Color;

#[derive(Clone)]
pub struct MonitoredInstance {
    // The name this instance belongs to (if it's from our config.toml)
    name: Option<String>,

    // The address itself
    addr: RedisAddr,

    auth: Option<RedisAuth>,

    // Format string (defaults to {host}:{port}
    fmt: String,

    color: Option<Color>,

    stats: CommandStats,
}

impl MonitoredInstance {
    fn make_fmt_string(name: &Option<String>, addr: &RedisAddr, fmt: &str) -> String {
        let fmt = fmt.to_owned();
        let mut fmt = fmt.replace("{host}", &addr.get_host());

        if let Some(name) = name {
            fmt = fmt.replace("{name}", name);
        };

        if let Some(port) = addr.get_port() {
            fmt = fmt.replace("{port}", &format!("{port}"));
        }

        fmt
    }

    fn new(
        name: Option<String>,
        addr: RedisAddr,
        auth: Option<RedisAuth>,
        color: Option<Color>,
        fmt: Option<String>,
    ) -> Self {
        let fmt =
            Self::make_fmt_string(&name, &addr, &fmt.unwrap_or_else(|| "{host}:{port}".into()));

        Self {
            name,
            addr,
            auth,
            color,
            stats: CommandStats::new(),
            fmt,
        }
    }

    pub fn from_config_entry(name: &str, entry: &ConfigEntry) -> Vec<Self> {
        if entry.cluster {
            let c = Cluster::from_seeds(&entry.get_addresses()).expect("Can't get cluster nodes");
            c.get_primary_nodes()
                .iter()
                .map(|primary| {
                    Self::new(
                        Some(name.to_owned()),
                        primary.addr.to_owned(),
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

    pub fn fmt_str(&self) -> &str {
        &self.fmt
    }

    pub fn get_color(&self) -> Option<Color> {
        self.color
    }

    pub fn get_url_string(&self) -> String {
        self.addr.get_url_string()
    }

    pub fn incr_stats(&mut self, cmd: &str, bytes: usize) {
        self.stats.incr(cmd, bytes);
    }

    pub fn get_auth(&self) -> &Option<RedisAuth> {
        &self.auth
    }
}

impl From<RedisAddr> for MonitoredInstance {
    fn from(addr: RedisAddr) -> Self {
        Self::new(None, addr, None, None, Some("{host}:{port}".to_owned()))
    }
}
