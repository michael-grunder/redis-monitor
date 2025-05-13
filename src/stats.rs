use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Stat {
    count: usize,
    bytes: usize,
}

#[derive(Debug, Clone)]
pub struct Map(HashMap<String, Stat>);

impl Stat {
    pub const fn new() -> Self {
        Self { count: 0, bytes: 0 }
    }

    pub const fn incr(&mut self, bytes: usize) {
        self.count += 1;
        self.bytes += bytes;
    }
}

impl Map {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn incr(&mut self, cmd: &str, bytes: usize) {
        match self.0.get_mut(cmd) {
            Some(v) => v.incr(bytes),
            None => {
                self.0.insert(cmd.to_string(), Stat::new());
            }
        };
    }
}
