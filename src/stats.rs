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
        if let Some(stat) = self.0.get_mut(cmd) {
            stat.incr(bytes);
        } else {
            let mut stat = Stat::new();
            stat.incr(bytes);
            self.0.insert(cmd.to_owned(), stat);
        }
    }

    pub fn try_incr(&mut self, line: &[u8], bytes: usize) {
        if let Some(cmd) = Self::find_cmd(line) {
            let cmd = std::str::from_utf8(cmd).unwrap_or("unknown");
            self.incr(cmd, bytes);
        }
    }

    fn find_cmd(s: &[u8]) -> Option<&[u8]> {
        let i = memchr::memchr(b'"', s)?;
        let r = &s[i + 1..];
        let j = memchr::memchr(b'"', r)?;
        Some(&r[..j])
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

#[cfg(test)]
mod tests {
    use super::CommandStats;

    #[test]
    fn first_occurrence_of_a_command_is_counted() {
        let mut stats = CommandStats::new();
        stats.try_incr(br#"1.0 [0 127.0.0.1:1] "GET" "k""#, 10);
        stats.try_incr(br#"1.0 [0 127.0.0.1:1] "GET" "k""#, 5);
        stats.try_incr(br#"1.0 [0 127.0.0.1:1] "SET" "k" "v""#, 7);

        let mut stats = stats.get_stats();
        stats.sort_by(|a, b| a.name.cmp(&b.name));
        let counts: Vec<_> = stats
            .iter()
            .map(|s| (s.name.as_str(), s.count, s.bytes))
            .collect();
        assert_eq!(counts, [("GET", 2, 15), ("SET", 1, 7)]);
    }
}
