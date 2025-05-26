use std::collections::HashMap;

use serde::Serialize;

#[derive(Debug, Clone)]
pub struct Stat {
    count: usize,
    bytes: usize,
}

#[derive(Debug, Clone)]
pub struct CommandStats(HashMap<String, Stat>);

#[derive(Debug, Clone, Serialize)]
pub struct CommandStat {
    pub name: String,
    pub count: usize,
    pub bytes: usize,
}

impl Stat {
    pub const fn new() -> Self {
        Self { count: 0, bytes: 0 }
    }

    pub const fn incr(&mut self, bytes: usize) {
        self.count += 1;
        self.bytes += bytes;
    }
}

impl CommandStats {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn incr(&mut self, cmd: &str, bytes: usize) {
        match self.0.get_mut(cmd) {
            Some(v) => v.incr(bytes),
            None => {
                self.0.insert(cmd.to_string(), Stat::new());
            }
        }
    }

    pub fn get_stats(&self) -> Vec<CommandStat> {
        self.0
            .iter()
            .map(|(name, stat)| CommandStat {
                name: name.clone(),
                count: stat.count,
                bytes: stat.bytes,
            })
            .collect()
    }
}
