use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CommandStat {
    count: usize,
    bytes: usize,
}

impl CommandStat {
    pub fn new() -> Self {
        Self { count: 0, bytes: 0 }
    }

    pub fn incr(&mut self, bytes: usize) {
        self.count += 1;
        self.bytes += bytes;
    }
}

#[derive(Debug, Clone)]
pub struct CommandStats(HashMap<String, CommandStat>);

impl CommandStats {
    pub fn new() -> Self {
        Self { 0: HashMap::new() }
    }

    pub fn incr(&mut self, cmd: &str, bytes: usize) {
        self.0
            .entry(cmd.to_string())
            .or_insert(CommandStat::new())
            .incr(bytes);
    }
}
