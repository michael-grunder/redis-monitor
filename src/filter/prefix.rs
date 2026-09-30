//! Client and recorded-time selectors, compiled once before ingest starts.
use std::{
    cmp::Ordering,
    net::{IpAddr, SocketAddr},
    str::FromStr,
};

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Datelike, Utc};
use chrono_tz::Tz;
use croner::{
    Cron,
    parser::{CronParser, Seconds, Year},
};
use redis_monitor::monitor::{Client, Record};

#[derive(Clone, Debug)]
pub enum ClientSelector {
    Tcp(SocketAddr),
    Unix(String),
    Lua,
    Unknown,
}

impl FromStr for ClientSelector {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "lua" => Ok(Self::Lua),
            "-" => Ok(Self::Unknown),
            _ => {
                if let Some(path) = value.strip_prefix("unix:") {
                    ensure!(
                        !path.is_empty(),
                        "client Unix path must not be empty"
                    );
                    Ok(Self::Unix(path.to_owned()))
                } else {
                    Ok(Self::Tcp(value.parse().context(
                        "client must be IP:port, [IPv6]:port, unix:PATH, lua, or -; use --client-host for an IP without a port",
                    )?))
                }
            }
        }
    }
}

impl ClientSelector {
    fn matches(&self, client: Client<'_>) -> bool {
        match (self, client) {
            (Self::Tcp(addr), Client::Tcp { ip, port }) => {
                addr.ip() == ip && addr.port() == port
            }
            (Self::Unix(expected), Client::Unix(path)) => expected == path,
            (Self::Lua, Client::Lua) | (Self::Unknown, Client::Unknown) => true,
            _ => false,
        }
    }
}

// Normalized decimal digits compare lexicographically without rounding. This
// also preserves sub-nanosecond boundaries and accepts the parser's full u64
// seconds range. Configuration owns digits; input comparison borrows them.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct Timestamp {
    seconds: u64,
    fraction: Vec<u8>,
}

fn fraction(digits: &[u8]) -> &[u8] {
    let end = digits.iter().rposition(|&b| b != b'0').map_or(0, |i| i + 1);
    &digits[..end]
}

impl FromStr for Timestamp {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        if value.contains(['T', 't']) {
            let date = DateTime::parse_from_rfc3339(value).context(
                "expected an RFC 3339 timestamp with an explicit offset",
            )?;
            ensure!(
                date.timestamp_subsec_nanos() < 1_000_000_000,
                "leap-second bounds are not supported"
            );
            let seconds = u64::try_from(date.timestamp())
                .context("timestamp must not precede the Unix epoch")?;
            // Chrono validates the date and offset; retain the original digits
            // because its nanosecond representation truncates longer fractions.
            let digits = value.split_once('.').map_or(&[][..], |(_, tail)| {
                let end = tail.bytes().take_while(u8::is_ascii_digit).count();
                &tail.as_bytes()[..end]
            });
            return Ok(Self {
                seconds,
                fraction: fraction(digits).to_vec(),
            });
        }
        let (seconds, digits) =
            value.split_once('.').map_or((value, ""), |(s, f)| (s, f));
        ensure!(
            !seconds.is_empty()
                && seconds.bytes().all(|b| b.is_ascii_digit())
                && digits.bytes().all(|b| b.is_ascii_digit())
                && (!value.contains('.') || !digits.is_empty()),
            "expected nonnegative Unix seconds (optionally fractional) or RFC 3339"
        );
        Ok(Self {
            seconds: seconds
                .parse()
                .context("timestamp seconds overflow u64")?,
            fraction: fraction(digits.as_bytes()).to_vec(),
        })
    }
}

impl Timestamp {
    fn compare_record(&self, seconds: u64, digits: &[u8]) -> Ordering {
        self.seconds
            .cmp(&seconds)
            .then_with(|| self.fraction.as_slice().cmp(digits))
    }
}

#[derive(Clone, Debug)]
pub struct TimeRange {
    start: Option<Timestamp>,
    end: Option<Timestamp>,
}

impl FromStr for TimeRange {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        let (start, end) = value.split_once('/').or_else(|| value.split_once('-'))
            .context("time range must be LOW-HIGH, LOW-, -HIGH (Unix seconds), or START/END (RFC 3339)")?;
        let bound = |text: &str, label: &str| -> Result<Option<Timestamp>> {
            if text.is_empty() {
                Ok(None)
            } else {
                text.parse::<Timestamp>().map(Some).map_err(|error| {
                    let message =
                        format!("invalid time range {label}: {error:#}");
                    error.context(message)
                })
            }
        };
        let start = bound(start, "start")?;
        let end = bound(end, "end")?;
        ensure!(
            start.is_some() || end.is_some(),
            "time range needs at least one bound"
        );
        if let (Some(start), Some(end)) = (&start, &end) {
            ensure!(start < end, "time range start must be before end");
        }
        Ok(Self { start, end })
    }
}

impl TimeRange {
    fn matches(&self, seconds: u64, digits: &[u8]) -> bool {
        self.start.as_ref().is_none_or(|start| {
            start.compare_record(seconds, digits) != Ordering::Greater
        }) && self.end.as_ref().is_none_or(|end| {
            end.compare_record(seconds, digits) == Ordering::Greater
        })
    }
}

#[derive(Clone, Debug)]
pub struct TimeCron(Cron);

impl FromStr for TimeCron {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        ensure!(
            value.split_whitespace().count() == 5,
            "time cron must have five fields: minute hour day-of-month month day-of-week"
        );
        // Five-field schedules usually trigger at second zero. For a record
        // selector the entire matching minute is included, so prepend '*'.
        let cron = CronParser::builder()
            .seconds(Seconds::Required)
            .year(Year::Disallowed)
            .build()
            .parse(&format!("* {value}"))
            .map_err(|error| {
                let message = format!("invalid time cron: {error}");
                anyhow::Error::new(error).context(message)
            })?;
        Ok(Self(cron))
    }
}

#[derive(Clone, Debug)]
pub struct PrefixFilter {
    clients: Vec<ClientSelector>,
    hosts: Vec<IpAddr>,
    ranges: Vec<TimeRange>,
    crons: Vec<TimeCron>,
    timezone: Tz,
}

impl Default for PrefixFilter {
    fn default() -> Self {
        Self {
            clients: Vec::new(),
            hosts: Vec::new(),
            ranges: Vec::new(),
            crons: Vec::new(),
            timezone: Tz::UTC,
        }
    }
}

impl PrefixFilter {
    pub const fn new(
        clients: Vec<ClientSelector>,
        hosts: Vec<IpAddr>,
        ranges: Vec<TimeRange>,
        crons: Vec<TimeCron>,
        timezone: Tz,
    ) -> Self {
        Self {
            clients,
            hosts,
            ranges,
            crons,
            timezone,
        }
    }

    pub const fn is_empty(&self) -> bool {
        self.clients.is_empty()
            && self.hosts.is_empty()
            && self.ranges.is_empty()
            && self.crons.is_empty()
    }

    pub fn matches(&self, record: &Record<'_>) -> bool {
        if !self.clients.is_empty()
            && !self
                .clients
                .iter()
                .any(|selector| selector.matches(record.client))
        {
            return false;
        }
        if !self.hosts.is_empty()
            && !matches!(record.client, Client::Tcp { ip, .. } if self.hosts.contains(&ip))
        {
            return false;
        }
        if self.ranges.is_empty() && self.crons.is_empty() {
            return true;
        }
        let (seconds, digits) = record.timestamp_parts();
        if !self.ranges.is_empty()
            && !self
                .ranges
                .iter()
                .any(|range| range.matches(seconds, fraction(digits)))
        {
            return false;
        }
        if self.crons.is_empty() {
            return true;
        }
        // Timestamps outside Chrono's calendar cannot match calendar windows.
        let Some(date) = i64::try_from(seconds)
            .ok()
            .and_then(|s| DateTime::<Utc>::from_timestamp(s, 0))
        else {
            return false;
        };
        // Keep timezone conversion away from Chrono's representable endpoints;
        // Croner itself only supports years 1..=5000.
        if date.year() > croner::YEAR_UPPER_LIMIT + 1 {
            return false;
        }
        let date = date.with_timezone(&self.timezone);
        self.crons.iter().any(|cron| {
            // Compiled patterns and valid calendar fields cannot produce a
            // component-range error. Fail closed if the dependency rejects one.
            cron.0.is_time_matching(&date).unwrap_or(false)
        })
    }
}
