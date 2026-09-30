use std::{collections::HashMap, fmt};

use redis_monitor::monitor;

/// Cumulative per-command record counts and MONITOR line bytes.
#[derive(Debug, Default)]
pub struct CommandStats(HashMap<Box<[u8]>, Stat>);

#[derive(Debug, Default, Clone, Copy)]
struct Stat {
    count: u64,
    bytes: u64,
}

impl CommandStats {
    /// Count a record under its command name. Records without one are
    /// ignored; the output reports them as invalid.
    pub fn record(&mut self, line: &[u8]) {
        let Some(cmd) = monitor::command_name(line) else {
            return;
        };
        // Look up by borrowed bytes; allocate only for a new command name.
        let stat = if let Some(stat) = self.0.get_mut(cmd) {
            stat
        } else {
            self.0.entry(cmd.into()).or_default()
        };
        stat.count += 1;
        stat.bytes += line.len() as u64;
    }

    /// Add these counts to `total` and reset them, keeping allocated names
    /// so later merges do not allocate.
    pub fn merge_into(&mut self, total: &mut Self) {
        for (name, stat) in &mut self.0 {
            if stat.count == 0 {
                continue;
            }
            let sum = if let Some(sum) = total.0.get_mut(name) {
                sum
            } else {
                total.0.entry(name.clone()).or_default()
            };
            sum.count += stat.count;
            sum.bytes += stat.bytes;
            *stat = Stat::default();
        }
    }
}

/// `NAME=[count, bytes]` entries sorted by command name.
impl fmt::Display for CommandStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entries: Vec<_> = self.0.iter().collect();
        entries.sort_unstable_by_key(|(name, _)| *name);
        for (i, (name, stat)) in entries.into_iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(
                f,
                "{}=[{}, {}]",
                String::from_utf8_lossy(name),
                stat.count,
                stat.bytes
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::CommandStats;

    #[test]
    fn commands_are_counted_and_reported_in_name_order() {
        let mut stats = CommandStats::default();
        stats.record(br#"1.0 [0 127.0.0.1:1] "SET" "k" "v""#);
        stats.record(br#"1.0 [0 127.0.0.1:1] "GET" "k""#);
        stats.record(br#"1.0 [0 127.0.0.1:1] "GET" "key""#);
        stats.record(b"no command here");

        assert_eq!(stats.to_string(), "GET=[2, 60], SET=[1, 33]");
    }

    #[test]
    fn merging_adds_counts_and_resets_the_source() {
        let mut total = CommandStats::default();
        let mut local = CommandStats::default();
        local.record(br#"1.0 [0 127.0.0.1:1] "GET" "k""#);
        local.merge_into(&mut total);
        local.record(br#"1.0 [0 127.0.0.1:1] "GET" "k""#);
        local.record(br#"1.0 [0 127.0.0.1:1] "DEL" "k""#);
        local.merge_into(&mut total);
        local.merge_into(&mut total);

        assert_eq!(total.to_string(), "DEL=[1, 29], GET=[2, 58]");
        assert_eq!(local.to_string(), "DEL=[0, 0], GET=[0, 0]");
    }
}
