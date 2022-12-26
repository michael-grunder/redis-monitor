use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CommandStats(HashMap<String, usize>);

impl CommandStats {
    pub fn new() -> Self {
        Self { 0: HashMap::new() }
    }

    pub fn incr(&mut self, cmd: &str) {
        *self.0.entry(cmd.to_string()).or_insert(0) += 1;
    }
}
