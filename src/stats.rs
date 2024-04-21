use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Stat {
    count: usize,
    bytes: usize,
}

impl Stat {
    pub const fn new() -> Self {
        Self { count: 0, bytes: 0 }
    }

    pub fn incr(&mut self, bytes: usize) {
        self.count += 1;
        self.bytes += bytes;
    }
}

#[derive(Debug, Clone)]
pub struct Map(HashMap<String, Stat>);

impl Map {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn incr(&mut self, cmd: &str, bytes: usize) {
        self.0
            .entry(cmd.to_string())
            .or_insert_with(Stat::new)
            .incr(bytes);
    }
}
