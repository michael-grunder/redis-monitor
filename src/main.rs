#![warn(clippy::all, clippy::nursery, clippy::pedantic)]
//#![allow(clippy::non_ascii_literal)]
//#![allow(clippy::must_use_candidate)]
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::{
    collections::HashSet, convert::From, fmt, ops::Range, path::PathBuf,
    str::FromStr, time::Instant,
};

use anyhow::{Context, Result, anyhow};
use bytes::{Bytes, BytesMut};
use clap::{ArgAction, CommandFactory, Parser};
use clap_complete::{Shell, generate};
use colored::Color;
use connection::{ServerAddr, TlsConfig};
use filter::Filter;
use output::OutputKind;
use rand::{RngExt, rngs::ThreadRng};
use redis::{
    Client, ConnectionAddr, ConnectionInfo, IntoConnectionInfo,
    RedisConnectionInfo, aio::ConnectionManager as RedisConnectionManager,
};
use tokio::{
    io::{self, AsyncRead, AsyncReadExt, BufReader},
    sync::{OwnedSemaphorePermit, Semaphore, watch},
    task::JoinSet,
    time::{Duration, sleep, sleep_until},
};

use crate::{
    commands::{Categories, Command, Flags},
    config::{Map, ServerAuth},
    connection::{Cluster, Monitor},
    filter::FilterPattern,
    output::OutputHandler,
};

mod commands;
mod config;
mod connection;
mod filter;
mod monitor;
mod output;
mod stats;

#[derive(Parser, Debug)]
#[command(
    name = "redis-monitor",
    about = "A utility to monitor one or more RESP compatible servers",
    after_help = r#"Format specifiers:
  %S   Short form of server and client address
  %sa  Full address of the server (host:port or unix path)
  %sh  Host part of the server address
  %sp  Port part of the server address (or basename of unix path)
  %Sn  Name of the server instance if it is set
  %ca  Full address of the client (ip:port or unix path)
  %ch  Host part of the client address
  %cp  Port part of the client address (or basename of unix path)
  %d   The database number
  %t   The timestamp as reported by MONITOR
  %l   The full command and all arguments
  %C   Argument 0 (the command)
  %a   Arguments 1..N

  The default formats are:
    Single instance:    "%t [%d %ca] %l";
    Multiple Instances: "%t [%S %d] %l";

Examples:
  # Monitor localhost:6379 by default
  redis-monitor

  # Monitor a cluster expecting one node to be 127.0.0.1:6379
  redis-monitor -c 6379

  # Monitor two standalone instances
  redis-monitor host1:6379 host2:6379

  # Run while filtering specific commands
  redis-monitor --filter get --filter set
  redis-monitor --filter '!get' --filter '!set'
  redis-monitor --filter '/^geo/'

  # Filtering by command flags and categories
  redis-monitor --flags write,@hash"#
)]
#[allow(clippy::struct_excessive_bools)]
struct Options {
    #[command(subcommand)]
    cmd: Option<Cmd>,

    #[arg(short, long, help = "Treat each instance like its a cluster seed")]
    cluster: bool,

    #[arg(short, long, help = "How to format each MONITOR line")]
    format: Option<String>,

    #[arg(short, long, help = "Also connect and MONITOR cluster replicas")]
    replicas: bool,

    #[arg(long)]
    config_file: Option<PathBuf>,

    #[arg(long, help = "Disable colored output")]
    no_color: bool,

    #[arg(long, help = "Only show commands for a specific database")]
    db: Option<u64>,

    #[arg(short, long, help = "Redis user")]
    user: Option<String>,

    #[arg(short, long, short_alias = 'a', help = "Redis password")]
    pass: Option<String>,

    #[clap(long, action = clap::ArgAction::Append,
           help = "One or more literal or regex patterns to filter command names")]
    filter: Vec<FilterPattern>,

    #[arg(
        long,
        action = ArgAction::Append,
        value_name = "FLAG|@CATEGORY",
        help = "Require flags (e.g. write) and/or categories (e.g. @hash)"
    )]
    flags: Vec<String>,

    #[arg(
        short,
        long,
        default_value = "plain",
        help = "How to serialize the output. Values: plain, json, php, csv, resp"
    )]
    output: OutputKind,

    #[arg(long, help = "Connect using TLS")]
    tls: bool,

    #[arg(long, help = "Disable TLS certificate verification")]
    insecure: bool,

    #[arg(long, help = "Path to CA cert for TLS")]
    tls_ca: Option<PathBuf>,

    #[arg(long, help = "Path to client cert for TLS")]
    tls_cert: Option<PathBuf>,

    #[arg(long, help = "Path to client private key for TLS")]
    tls_key: Option<PathBuf>,

    #[arg(short, long, help = "Display the version and exit")]
    version: bool,

    #[arg(long, value_parser = validate_positive_f64)]
    stats: Option<f64>,

    #[arg(long, help = "Read from stdin instead of connecting to servers")]
    stdin: bool,

    #[arg(
        long,
        help = "Disable producer batching for the lowest output latency"
    )]
    no_batch: bool,

    #[arg(
        long,
        help = "Output debug information such as detailed filter info"
    )]
    debug: bool,

    pub instances: Vec<String>,
}

#[derive(clap::Subcommand, Debug)]
enum Cmd {
    #[command(about = "Generate shell completion scripts")]
    Completions {
        #[arg(value_enum, help = "The shell to generate completions for")]
        shell: Shell,
    },
}

const VERSION: &str = env!("CARGO_PKG_VERSION");
const GIT_HASH: &str = env!("GIT_HASH");
const GIT_DIRTY: &str = env!("GIT_DIRTY");

const DEFAULT_SINGLE_FORMAT: &str = "%t [%d %ca] %l";
const DEFAULT_MULTI_FORMAT: &str = "%t [%S %d] %l";
const DEFAULT_BATCH_RECORDS: usize = 64;
const DEFAULT_BATCH_BYTES: usize = 256 * 1024;
const DEFAULT_BATCH_DELAY: Duration = Duration::from_millis(5);
const OUTPUT_BYTE_BUDGET: usize = 64 * 1024 * 1024;
const OUTPUT_BATCH_CAPACITY: usize = 1024;
const OUTPUT_IMMEDIATE_CAPACITY: usize = 16_384;

// Simple wrapper tat just eprintln!s with a [WARNING] prefix
macro_rules !warn {
    ($($arg:tt)*) => {
        eprintln!("[WARNING] {}", format!($($arg)*));
    };
}

fn validate_positive_f64(s: &str) -> Result<f64> {
    match s.parse::<f64>() {
        Ok(val) if val > 0.0 => Ok(val),
        Ok(_) => Err(anyhow!("Value must be positive".to_string())),
        Err(_) => Err(anyhow!("Invalid number".to_string())),
    }
}

impl From<Vec<String>> for commands::Filter {
    fn from(flags: Vec<String>) -> Self {
        Options::parse_flags(flags.iter().map(String::as_str))
    }
}

impl Options {
    fn parse_flags<'a, I>(it: I) -> commands::Filter
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut flags = Flags::empty();
        let mut acl = Categories::empty();

        for raw in it {
            for tok in raw.split(',') {
                let s = tok.trim();
                if s.is_empty() {
                    continue;
                }

                if let Ok(c) = Categories::from_str(s) {
                    acl |= c;
                } else if let Ok(f) = Flags::from_str(s) {
                    flags |= f;
                }
            }
        }

        commands::Filter {
            flags: (!flags.is_empty()).then_some(flags),
            categories: (!acl.is_empty()).then_some(acl),
        }
    }

    fn get_tls_config(&self) -> Result<Option<Arc<TlsConfig>>> {
        if self.tls {
            Ok(Some(Arc::new(TlsConfig::new(
                self.insecure,
                self.tls_ca.as_deref(),
                self.tls_cert.as_deref(),
                self.tls_key.as_deref(),
            )?)))
        } else {
            Ok(None)
        }
    }

    fn get_server_auth(&self) -> ServerAuth {
        ServerAuth::from_user_pass(self.user.as_deref(), self.pass.as_deref())
    }
}

// Treat each instance as a potential cluster seed. This means that if more than
// one seeds of the same cluster are passed we may map the same keyspace more
// than once. This is fine, but we should be aware of it.
fn process_cluster_instances(
    opt: &Options,
    tls: Option<&Arc<TlsConfig>>,
    auth: &ServerAuth,
) -> Vec<Monitor> {
    let addresses = opt.instances.iter().map(|addr| {
        ServerAddr::from_str(addr).unwrap_or_else(|_| {
            panic!("Unable to interpret '{addr:?}' as a server address");
        })
    });

    addresses
        .flat_map(|address| {
            Cluster::from_seed(&address)
                .unwrap_or_else(|_| {
                    panic!(
                        "Unable to interpret '{address:?}' as a cluster address"
                    );
                })
                .get_nodes()
                .iter()
                .flat_map(|primary| {
                    let mut nodes = vec![primary];
                    if opt.replicas {
                        nodes.extend(&primary.replicas);
                    }

                    nodes.into_iter().map(|n| {
                        Monitor::new(
                            Some(&n.id),
                            n.addr.clone(),
                            tls.cloned(),
                            auth.clone(),
                            None,
                        )
                    })
                })
                .collect::<Vec<_>>()
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

// Take the array of instances provided on the command line and attempt to map
// them to one or more instances. These can either be named instances like
// mycluster` which were loaded from our config file, or be in some parsable
// form like "host:port", or "redis://...".
fn process_instances(
    cfg: &Map,
    opt: &Options,
    tls: Option<&Arc<TlsConfig>>,
    auth: &ServerAuth,
) -> Vec<Monitor> {
    opt.instances
        .iter()
        .flat_map(|inst| {
            cfg.get(inst).map_or_else(
                || {
                    ServerAddr::from_str(inst).map_or_else(
                        |_| {
                            panic!(
                            "Unable to parse '{inst}' as an address or named instance"
                        );
                        },
                        |addr| {
                            let monitor = Monitor::new(
                                None,
                                addr,
                                tls.cloned(),
                                auth.clone(),
                                None,
                            );

                            vec![monitor]
                        }
                    )
                },
                |entry| Monitor::from_config_entry(inst, entry),
            )
        })
        .collect()
}

#[derive(Debug, Default)]
struct Stalls {
    total: AtomicU64,
    current: AtomicU64,
}

#[derive(Debug, Default)]
struct IoStats {
    total: AtomicU64,
    filtered: AtomicU64,
    stalls: Stalls,
}

#[derive(Debug)]
struct LocalStats {
    batch_size: u64,
    total: u64,
    filtered: u64,
}

#[derive(Debug)]
struct Backoff {
    retries: u32,
    max_delay: Duration,
}

#[derive(Clone)]
struct LineFilter {
    empty: bool,
    names: Filter,
    flags: commands::Filter,
}

#[derive(Debug, Clone)]
struct MonitorSource {
    server: Arc<ServerAddr>,
    name: Arc<Option<String>>,
    #[allow(dead_code)]
    color: Option<Color>,
}

#[derive(Debug)]
struct MonitorChunk {
    data: Bytes,
    lines: Vec<Range<usize>>,
}

#[derive(Debug)]
struct MonitorBatch {
    source: MonitorSource,
    chunks: Vec<MonitorChunk>,
    records: usize,
    retained_bytes: usize,
    permit: Option<OwnedSemaphorePermit>,
}

#[derive(Debug, Copy, Clone)]
struct BatchConfig {
    records: usize,
    bytes: usize,
    delay: Duration,
}

#[derive(Debug)]
struct BatchSender {
    io: IoHandle,
    source: MonitorSource,
    config: BatchConfig,
    batch: Option<MonitorBatch>,
    deadline: Option<tokio::time::Instant>,
}

type IoSender = flume::Sender<IoMessage>;

#[derive(Debug, Clone)]
struct IoHandle {
    tx: IoSender,
    budget: Arc<Semaphore>,
    byte_budget: usize,
}

#[derive(Debug)]
enum IoMessage {
    Preamble(Arc<[Monitor]>),
    Batch(MonitorBatch),
    Shutdown,
}

static IO_STATS: IoStats = IoStats {
    total: AtomicU64::new(0),
    filtered: AtomicU64::new(0),
    stalls: Stalls {
        total: AtomicU64::new(0),
        current: AtomicU64::new(0),
    },
};

impl IoStats {
    fn snapshot(&self) -> (u64, u64, u64) {
        let total = self.total.load(Ordering::Relaxed);
        let filtered = self.filtered.load(Ordering::Relaxed);

        let stalls_current = self.stalls.current.load(Ordering::Relaxed);
        let stalls_total =
            self.stalls.total.load(Ordering::Relaxed) + stalls_current;

        (total, filtered, stalls_total)
    }

    fn stall(&self) {
        self.stalls.current.fetch_add(1, Ordering::Relaxed);
    }

    fn fold(&self) -> (u64, u64) {
        let current = self.stalls.current.swap(0, Ordering::Relaxed);
        let prev = self.stalls.total.fetch_add(current, Ordering::Relaxed);
        let total = prev + current;

        (current, total)
    }
}

impl IoHandle {
    async fn send(
        &self,
        message: IoMessage,
    ) -> Result<(), flume::SendError<IoMessage>> {
        send_io_message(&self.tx, message, &IO_STATS).await
    }

    fn permit_count(&self, bytes: usize) -> u32 {
        u32::try_from(bytes.min(self.byte_budget))
            .expect("output byte budget must fit in u32")
    }

    fn try_reserve_bytes(&self, bytes: usize) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.budget)
            .try_acquire_many_owned(self.permit_count(bytes))
            .ok()
    }

    async fn reserve_bytes(&self, bytes: usize) -> OwnedSemaphorePermit {
        Arc::clone(&self.budget)
            .acquire_many_owned(self.permit_count(bytes))
            .await
            .expect("output byte-budget semaphore is never closed")
    }
}

impl BatchConfig {
    const fn new(enabled: bool) -> Self {
        if enabled {
            Self {
                records: DEFAULT_BATCH_RECORDS,
                bytes: DEFAULT_BATCH_BYTES,
                delay: DEFAULT_BATCH_DELAY,
            }
        } else {
            Self {
                records: 1,
                bytes: usize::MAX,
                delay: Duration::ZERO,
            }
        }
    }
}

impl MonitorBatch {
    const fn new(source: MonitorSource) -> Self {
        Self {
            source,
            chunks: Vec::new(),
            records: 0,
            retained_bytes: 0,
            permit: None,
        }
    }

    fn add_chunk(
        &mut self,
        data: Bytes,
        lines: Vec<Range<usize>>,
        permit: OwnedSemaphorePermit,
    ) {
        self.records += lines.len();
        self.retained_bytes += data.len();
        self.chunks.push(MonitorChunk { data, lines });

        if let Some(current) = &mut self.permit {
            current.merge(permit);
        } else {
            self.permit = Some(permit);
        }
    }

    fn lines(&self) -> impl Iterator<Item = &[u8]> {
        self.chunks.iter().flat_map(|chunk| {
            chunk.lines.iter().map(|range| &chunk.data[range.clone()])
        })
    }
}

impl BatchSender {
    const fn new(
        io: IoHandle,
        source: MonitorSource,
        config: BatchConfig,
    ) -> Self {
        Self {
            io,
            source,
            config,
            batch: None,
            deadline: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.batch.as_ref().is_none_or(|batch| batch.records == 0)
    }

    fn remaining_records(&self) -> usize {
        self.batch.as_ref().map_or(self.config.records, |batch| {
            self.config.records.saturating_sub(batch.records)
        })
    }

    fn remaining_bytes(&self) -> usize {
        self.batch.as_ref().map_or(self.config.bytes, |batch| {
            self.config.bytes.saturating_sub(batch.retained_bytes)
        })
    }

    const fn deadline(&self) -> Option<tokio::time::Instant> {
        self.deadline
    }

    async fn reserve_chunk(
        &mut self,
        bytes: usize,
    ) -> Result<OwnedSemaphorePermit, flume::SendError<IoMessage>> {
        if let Some(permit) = self.io.try_reserve_bytes(bytes) {
            return Ok(permit);
        }

        // Never wait for more byte-budget capacity while holding a partial
        // batch: with many producers doing the same, that would deadlock.
        self.flush().await?;
        IO_STATS.stall();
        Ok(self.io.reserve_bytes(bytes).await)
    }

    async fn add_chunk(
        &mut self,
        data: Bytes,
        lines: Vec<Range<usize>>,
        permit: OwnedSemaphorePermit,
    ) -> Result<(), flume::SendError<IoMessage>> {
        debug_assert!(!lines.is_empty());

        if !self.is_empty()
            && (lines.len() > self.remaining_records()
                || data.len() > self.remaining_bytes())
        {
            self.flush().await?;
        }

        let was_empty = self.is_empty();
        self.batch
            .get_or_insert_with(|| MonitorBatch::new(self.source.clone()))
            .add_chunk(data, lines, permit);

        if was_empty {
            self.deadline =
                Some(tokio::time::Instant::now() + self.config.delay);
        }

        let full = self.remaining_records() == 0
            || self.remaining_bytes() == 0
            || !self.config.delay.is_zero()
                && self.deadline.is_some_and(|deadline| {
                    deadline <= tokio::time::Instant::now()
                });

        if full || self.config.delay.is_zero() {
            self.flush().await?;
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<(), flume::SendError<IoMessage>> {
        self.deadline = None;
        let Some(batch) = self.batch.take() else {
            return Ok(());
        };

        self.io.send(IoMessage::Batch(batch)).await
    }
}

async fn send_io_message(
    tx: &IoSender,
    message: IoMessage,
    stats: &IoStats,
) -> Result<(), flume::SendError<IoMessage>> {
    match tx.try_send(message) {
        Ok(()) => Ok(()),
        Err(flume::TrySendError::Full(message)) => {
            stats.stall();
            tx.send_async(message).await
        }
        Err(flume::TrySendError::Disconnected(message)) => {
            Err(flume::SendError(message))
        }
    }
}

impl LocalStats {
    const fn new(batch_size: u64) -> Self {
        Self {
            batch_size,
            total: 0,
            filtered: 0,
        }
    }

    fn tick(&mut self) {
        self.total += 1;
        if self.total.is_multiple_of(self.batch_size) {
            self.fold();
        }
    }

    const fn filtered(&mut self) {
        self.filtered += 1;
    }

    #[inline]
    fn fold(&mut self) {
        if self.total > 0 {
            IO_STATS.total.fetch_add(self.total, Ordering::Relaxed);
        }

        if self.filtered > 0 {
            IO_STATS
                .filtered
                .fetch_add(self.filtered, Ordering::Relaxed);
        }

        self.total = 0;
        self.filtered = 0;
    }
}

impl fmt::Debug for LineFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LineFilter")
            .field("empty", &self.empty)
            .field("names", &self.names)
            .field("flags", &self.flags)
            .finish()
    }
}

impl LineFilter {
    fn from_options(opt: &Options) -> Self {
        let names: Filter = opt.filter.clone().into();
        let flags = opt.flags.clone().into();
        Self::new(names, flags)
    }

    const fn new(names: Filter, flags: commands::Filter) -> Self {
        let empty = names.is_empty() && flags.is_empty();
        Self {
            empty,
            names,
            flags,
        }
    }

    #[inline]
    fn cmd(line: &[u8]) -> Option<&[u8]> {
        let start = memchr::memchr(b'"', line)?;
        let rest = &line[start + 1..];
        let end_rel = memchr::memchr(b'"', rest)?;
        let end = start + 1 + end_rel;

        Some(&line[start + 1..end])
    }

    #[inline]
    const fn needs_cmds(&self) -> bool {
        !self.flags.is_empty()
    }

    #[inline]
    fn matches(
        &self,
        commands: Option<&commands::Lookup>,
        line: &[u8],
    ) -> bool {
        if self.empty {
            return true;
        }

        // We need to extract the command to do anything useful
        let Some(cmd) = Self::cmd(line) else {
            eprintln!(
                "Unable to extract command from line: {}",
                String::from_utf8_lossy(line)
            );
            return true;
        };

        if !self.names.matches(cmd) {
            return false;
        }

        if self.flags.is_empty() {
            return true;
        }

        commands.is_none_or(|lu| lu.matches_bytes_or(cmd, self.flags, true))
    }
}

impl MonitorSource {
    const fn new(
        server: Arc<ServerAddr>,
        name: Arc<Option<String>>,
        color: Option<Color>,
    ) -> Self {
        Self {
            server,
            name,
            color,
        }
    }
}

impl Backoff {
    const MIN_DELAY: Duration = Duration::from_millis(50);
    const MAX_DELAY: Duration = Duration::from_secs(1);

    const fn new() -> Self {
        Self {
            retries: 0,
            max_delay: Self::MAX_DELAY,
        }
    }

    fn delay(&mut self) -> Duration {
        let mut rng = ThreadRng::default();

        self.retries += 1;
        let shift_amount = self.retries.min(6) + rng.random_range(0..3);
        let base_delay = Self::MIN_DELAY.as_millis();
        let delay_ms =
            (base_delay << shift_amount).try_into().unwrap_or(u64::MAX);

        Duration::from_millis(
            delay_ms.min(
                self.max_delay
                    .as_millis()
                    .try_into()
                    .expect("Delay too long"),
            ),
        )
    }

    const fn reset(&mut self) {
        self.retries = 0;
    }
}

struct FrameScan {
    end: usize,
    lines: Vec<Range<usize>>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum StreamExit {
    End,
    Shutdown,
    OutputClosed,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ReadEvent {
    Data,
    End,
}

fn scan_frames(
    buf: &[u8],
    filter: &LineFilter,
    commands: Option<&commands::Lookup>,
    stats: &mut LocalStats,
    max_records: usize,
    max_bytes: usize,
) -> Option<FrameScan> {
    let mut start = 0;
    let mut records = 0;
    let mut lines = Vec::new();

    while records < max_records {
        let Some(nl) = memchr::memchr(b'\n', &buf[start..])
            .map(|relative| relative + start)
        else {
            break;
        };
        let wire_end = nl + 1;

        if records > 0 && wire_end > max_bytes {
            break;
        }

        let mut line_start = start;
        let mut line_end = nl;
        if line_end > line_start && buf[line_end - 1] == b'\r' {
            line_end -= 1;
        }
        if line_start < line_end && buf[line_start] == b'+' {
            line_start += 1;
        }

        stats.tick();
        if filter.matches(commands, &buf[line_start..line_end]) {
            lines.push(line_start..line_end);
        } else {
            stats.filtered();
        }

        start = wire_end;
        records += 1;
        if wire_end >= max_bytes {
            break;
        }
    }

    (records > 0).then_some(FrameScan { end: start, lines })
}

async fn drain_frames(
    buf: &mut BytesMut,
    filter: &LineFilter,
    commands: Option<&commands::Lookup>,
    stats: &mut LocalStats,
    sender: &mut BatchSender,
) -> Result<(), flume::SendError<IoMessage>> {
    while let Some(first_nl) = memchr::memchr(b'\n', buf) {
        let first_wire_len = first_nl + 1;
        if !sender.is_empty()
            && (sender.remaining_records() == 0
                || first_wire_len > sender.remaining_bytes())
        {
            sender.flush().await?;
        }

        let Some(scan) = scan_frames(
            buf,
            filter,
            commands,
            stats,
            sender.remaining_records(),
            sender.remaining_bytes(),
        ) else {
            break;
        };

        if scan.lines.is_empty() {
            let _ = buf.split_to(scan.end);
            continue;
        }

        let permit = sender.reserve_chunk(scan.end).await?;
        let data = buf.split_to(scan.end).freeze();
        sender.add_chunk(data, scan.lines, permit).await?;
    }

    Ok(())
}

async fn consume_reader<R>(
    mut reader: R,
    filter: &LineFilter,
    commands: Option<&commands::Lookup>,
    stats: &mut LocalStats,
    sender: &mut BatchSender,
    shutdown: &mut watch::Receiver<bool>,
    accept_trailing_frame: bool,
) -> StreamExit
where
    R: AsyncRead + Unpin,
{
    let mut buf = BytesMut::with_capacity(16 * 1024);

    loop {
        if drain_frames(&mut buf, filter, commands, stats, sender)
            .await
            .is_err()
        {
            return StreamExit::OutputClosed;
        }

        if *shutdown.borrow() {
            let _ = sender.flush().await;
            return StreamExit::Shutdown;
        }

        let read = async {
            match reader.read_buf(&mut buf).await {
                Ok(0) => ReadEvent::End,
                Ok(_) => ReadEvent::Data,
                Err(e) => {
                    eprintln!("{} read error {e}", sender.source.server);
                    ReadEvent::End
                }
            }
        };

        let outcome = if let Some(deadline) = sender.deadline() {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        Some(StreamExit::Shutdown)
                    } else {
                        None
                    }
                }
                () = sleep_until(deadline) => {
                    if sender.flush().await.is_err() {
                        Some(StreamExit::OutputClosed)
                    } else {
                        None
                    }
                }
                read_event = read => match read_event {
                    ReadEvent::Data => None,
                    ReadEvent::End => Some(StreamExit::End),
                },
            }
        } else {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        Some(StreamExit::Shutdown)
                    } else {
                        None
                    }
                }
                read_event = read => match read_event {
                    ReadEvent::Data => None,
                    ReadEvent::End => Some(StreamExit::End),
                },
            }
        };

        match outcome {
            Some(StreamExit::End) => {
                if accept_trailing_frame && !buf.is_empty() {
                    buf.extend_from_slice(b"\n");
                }
                let _ = drain_frames(&mut buf, filter, commands, stats, sender)
                    .await;
                let _ = sender.flush().await;
                return StreamExit::End;
            }
            Some(StreamExit::Shutdown) => {
                let _ = sender.flush().await;
                return StreamExit::Shutdown;
            }
            Some(StreamExit::OutputClosed) => {
                return StreamExit::OutputClosed;
            }
            None => {}
        }
    }
}

async fn run_from_reader<R>(
    name: &str,
    reader: R,
    io: IoHandle,
    filter: LineFilter,
    config: BatchConfig,
    mut shutdown: watch::Receiver<bool>,
) where
    R: AsyncRead + Unpin,
{
    let source = MonitorSource::new(
        Arc::new(ServerAddr::from_path(name)),
        Arc::new(None),
        None,
    );
    let mut sender = BatchSender::new(io, source, config);
    let mut stats = LocalStats::new(1000);
    let _ = consume_reader(
        reader,
        &filter,
        None,
        &mut stats,
        &mut sender,
        &mut shutdown,
        true,
    )
    .await;
    drop(sender);
    stats.fold();
}

async fn run_monitor(
    mon: Monitor,
    filter: LineFilter,
    io: IoHandle,
    config: BatchConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    let source = MonitorSource::new(
        Arc::new(mon.address.clone()),
        Arc::new(mon.name.clone()),
        mon.color,
    );
    let mut sender = BatchSender::new(io, source, config);
    let mut backoff = Backoff::new();
    let mut stats = LocalStats::new(1000);
    let cmds = if filter.needs_cmds() {
        load_cmds(&mon).await
    } else {
        None
    };

    loop {
        if *shutdown.borrow() {
            break;
        }

        match mon.clone().connect().await {
            Ok((_, reader)) => {
                backoff.reset();

                match consume_reader(
                    reader,
                    &filter,
                    cmds.as_ref(),
                    &mut stats,
                    &mut sender,
                    &mut shutdown,
                    false,
                )
                .await
                {
                    StreamExit::Shutdown | StreamExit::OutputClosed => break,
                    StreamExit::End => {
                        eprintln!("{} connection closed", sender.source.server);
                    }
                }
            }
            Err(e) => {
                if backoff.retries == 0 {
                    eprintln!("{} Error connecting {e}", sender.source.server);
                }
            }
        }

        tokio::select! {
            () = sleep(backoff.delay()) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }

    let _ = sender.flush().await;
    drop(sender);
    stats.fold();
}

#[derive(Debug)]
enum Control {
    Shutdown,
    Continue,
}

async fn load_cmds(mon: &Monitor) -> Option<commands::Lookup> {
    let addr = mon.address.to_string();

    let mut manager = match connection_manager_for_monitor(mon).await {
        Ok(manager) => manager,
        Err(err) => {
            eprintln!(
                "{addr} failed to create COMMAND metadata connection: {err}"
            );
            return None;
        }
    };

    if let Err(err) = mon.auth.auth(&mut manager).await {
        eprintln!("{addr} AUTH failed while loading COMMAND metadata: {err}");
        return None;
    }

    match Command::load(&mut manager).await {
        Ok(commands) => Some(commands.into()),
        Err(err) => {
            eprintln!("{addr} failed to load COMMAND metadata: {err}");
            None
        }
    }
}

async fn connection_manager_for_monitor(
    mon: &Monitor,
) -> Result<RedisConnectionManager> {
    let addr = mon.address.to_string();
    let client = Client::open(connection_info_from_monitor(mon))
        .with_context(|| format!("Failed to create Redis client for {addr}"))?;

    client
        .get_connection_manager()
        .await
        .with_context(|| format!("Failed to open async connection to {addr}"))
}

fn print_final_stats() {
    let (total, filtered, stalls) = IO_STATS.snapshot();
    eprintln!(
        "Processed {total} lines (filtered: {filtered}), backpressure stalls: \
        {stalls}"
    );
}

fn connection_info_from_monitor(mon: &Monitor) -> ConnectionInfo {
    let addr = match &mon.address {
        ServerAddr::Tcp(host, port, _) => {
            if mon.tls.is_some() {
                ConnectionAddr::TcpTls {
                    host: host.clone(),
                    port: *port,
                    insecure: mon.tls.as_ref().is_some_and(|cfg| cfg.insecure),
                    tls_params: None,
                }
            } else {
                ConnectionAddr::Tcp(host.clone(), *port)
            }
        }
        ServerAddr::Unix(path) => ConnectionAddr::Unix(PathBuf::from(path)),
    };

    let mut redis = RedisConnectionInfo::default();
    if let Some(user) = &mon.auth.user {
        redis = redis.set_username(user);
    }
    if let Some(pass) = &mon.auth.pass {
        redis = redis.set_password(pass);
    }

    addr.into_connection_info()
        .expect("ConnectionAddr::into_connection_info cannot fail")
        .set_redis_settings(redis)
}

struct OutputStats {
    commands: stats::CommandStats,
    interval: Duration,
    tick: Instant,
}

impl OutputStats {
    fn new(interval: Duration) -> Self {
        Self {
            commands: stats::CommandStats::new(),
            interval,
            tick: Instant::now(),
        }
    }

    fn record(&mut self, line: &[u8], w: &mut dyn OutputHandler) {
        self.commands.try_incr(line, line.len());
        if self.tick.elapsed() >= self.interval {
            if let Err(e) = w
                .write_stats(&self.commands.get_stats())
                .and_then(|()| w.flush())
            {
                eprintln!("Error writing stats: {e}");
            }
            self.tick = Instant::now();
        }
    }
}

impl IoMessage {
    fn process(
        self,
        w: &mut dyn OutputHandler,
        stats: &mut Option<OutputStats>,
    ) -> Control {
        match self {
            Self::Preamble(servers) => {
                eprintln!("{}", format_preamble(&servers));
            }
            Self::Batch(batch) => {
                for line in batch.lines() {
                    if line == b"OK" {
                        continue;
                    }

                    if let Some(stats) = stats.as_mut() {
                        stats.record(line, w);
                    }

                    if let Err(e) = w.write_raw_line(
                        &batch.source.server,
                        batch.source.name.as_ref().as_deref(),
                        line,
                    ) {
                        eprintln!("Error handling record: {e}");
                    }
                }
            }
            Self::Shutdown => return Control::Shutdown,
        }

        Control::Continue
    }
}

fn format_preamble(monitor: &[Monitor]) -> String {
    let addresses = monitor
        .iter()
        .map(|m| m.address.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    format!("MONITOR: {addresses}")
}

fn start_io_thread(
    output_kind: OutputKind,
    format: &str,
    size: usize,
    byte_budget: usize,
    stats_interval: Option<Duration>,
) -> (IoHandle, std::thread::JoinHandle<Result<()>>) {
    const DRAIN_MAX: usize = 16;

    let (tx, rx) = flume::bounded::<IoMessage>(size);
    let budget = Arc::new(Semaphore::new(byte_budget));

    let fmt = format.to_string();
    let jh = std::thread::spawn(move || -> Result<()> {
        let stdout = std::io::stdout();
        let mut out = std::io::BufWriter::with_capacity(1 << 20, stdout.lock());
        let mut writer = output_kind.get_writer(&mut out, &fmt);
        let mut stats = stats_interval.map(OutputStats::new);
        let mut last = Instant::now();
        let mut shutdown = false;

        while !shutdown {
            let Ok(first) = rx.recv() else { break };

            match first.process(writer.as_mut(), &mut stats) {
                Control::Shutdown => break,
                Control::Continue => {}
            }

            for msg in rx.try_iter().take(DRAIN_MAX - 1) {
                match msg.process(writer.as_mut(), &mut stats) {
                    Control::Shutdown => {
                        shutdown = true;
                        break;
                    }
                    Control::Continue => {}
                }
            }

            if last.elapsed() >= Duration::from_secs(1) {
                let (curr, tot) = IO_STATS.fold();
                if curr > 0 {
                    warn!("backpressure stalls (curr: {curr}, total: {tot})");
                }

                last = Instant::now();
            }

            writer.flush()?;
        }

        Ok(())
    });

    (
        IoHandle {
            tx,
            budget,
            byte_budget,
        },
        jh,
    )
}

fn version_string() -> String {
    let git_display = format!(
        "{GIT_HASH}{}",
        if GIT_DIRTY == "yes" { "-dirty" } else { "" }
    );

    format!("redis-monitor v{VERSION} (git {git_display})")
}

async fn finish_io(
    io_tx: IoHandle,
    io_jh: std::thread::JoinHandle<Result<()>>,
) {
    let _ = io_tx.send(IoMessage::Shutdown).await;
    if let Err(e) = io_jh
        .join()
        .unwrap_or_else(|e| Err(anyhow!("IO thread panicked: {e:?}")))
    {
        eprintln!("IO thread error: {e}");
    }
}

async fn run_stdin(
    opt: Options,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let format = opt
        .format
        .clone()
        .unwrap_or_else(|| DEFAULT_SINGLE_FORMAT.to_string());
    let batch_config = BatchConfig::new(!opt.no_batch);
    let queue_size = if opt.no_batch {
        OUTPUT_IMMEDIATE_CAPACITY
    } else {
        OUTPUT_BATCH_CAPACITY
    };

    let (io_tx, io_jh) = start_io_thread(
        opt.output,
        &format,
        queue_size,
        OUTPUT_BYTE_BUDGET,
        None,
    );

    let pseudo = Monitor::new(
        Some("stdin"),
        ServerAddr::from_path("stdin"),
        None,
        ServerAuth::default(),
        None,
    );

    let preamble: Arc<[Monitor]> = Arc::from(vec![pseudo]);

    io_tx
        .send(IoMessage::Preamble(Arc::clone(&preamble)))
        .await?;

    let filter: LineFilter = LineFilter::from_options(&opt);
    let stdin = io::stdin();
    let reader = BufReader::new(stdin);
    run_from_reader(
        "stdin",
        reader,
        io_tx.clone(),
        filter,
        batch_config,
        shutdown,
    )
    .await;

    finish_io(io_tx, io_jh).await;

    Ok(())
}

async fn run_wire(opt: Options, shutdown: watch::Receiver<bool>) -> Result<()> {
    let cfg = Map::load(opt.config_file.as_deref())?;
    let instances: Vec<String> = if opt.instances.is_empty() {
        vec!["localhost:6379".to_string()]
    } else {
        opt.instances.clone()
    };

    let tls = opt.get_tls_config()?;
    let auth = opt.get_server_auth();
    let opt = Options { instances, ..opt };

    let seeds = if opt.cluster {
        process_cluster_instances(&opt, tls.as_ref(), &auth)
    } else {
        process_instances(&cfg, &opt, tls.as_ref(), &auth)
    };

    let mut tasks = JoinSet::new();

    let format = opt.format.clone().unwrap_or_else(|| {
        if seeds.len() > 1 {
            DEFAULT_MULTI_FORMAT.to_string()
        } else {
            DEFAULT_SINGLE_FORMAT.to_string()
        }
    });

    let stats_interval = if opt.output == OutputKind::Plain {
        opt.stats.map(Duration::from_secs_f64)
    } else {
        None
    };
    let filter = LineFilter::from_options(&opt);
    let batch_config = BatchConfig::new(!opt.no_batch);
    let queue_size = if opt.no_batch {
        OUTPUT_IMMEDIATE_CAPACITY
    } else {
        OUTPUT_BATCH_CAPACITY
    };

    let (io_tx, io_jh) = start_io_thread(
        opt.output,
        &format,
        queue_size,
        OUTPUT_BYTE_BUDGET,
        stats_interval,
    );

    let preamble: Arc<[Monitor]> = Arc::from(seeds);
    io_tx
        .send(IoMessage::Preamble(Arc::clone(&preamble)))
        .await?;

    for mon in preamble.iter().cloned() {
        let io_task = io_tx.clone();
        let filter_clone = filter.clone();
        let shutdown_task = shutdown.clone();
        tasks.spawn(async move {
            run_monitor(
                mon,
                filter_clone,
                io_task,
                batch_config,
                shutdown_task,
            )
            .await;
        });
    }

    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            eprintln!("Monitor task failed: {e}");
        }
    }

    finish_io(io_tx, io_jh).await;

    Ok(())
}

async fn run(opt: Options, shutdown: watch::Receiver<bool>) -> Result<()> {
    if opt.stdin {
        run_stdin(opt, shutdown).await
    } else {
        run_wire(opt, shutdown).await
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let opt: Options = Options::parse();

    if opt.version {
        eprintln!("{}", version_string());
        return Ok(());
    }

    if let Some(Cmd::Completions { shell }) = opt.cmd {
        let mut cmd = Options::command();
        generate(shell, &mut cmd, "redis-monitor", &mut std::io::stdout());
        return Ok(());
    }

    if opt.debug {
        let filter = LineFilter::from_options(&opt);
        eprintln!("{filter:#?}");
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let run = run(opt, shutdown_rx);
    tokio::pin!(run);

    let res = tokio::select! {
        r = &mut run => r,
        _ = tokio::signal::ctrl_c() => {
            eprintln!("\nCtrl-C received, shutting down...");
            let _ = shutdown_tx.send(true);
            run.await
        }
    };

    print_final_stats();

    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitor::Line;
    use tokio::io::AsyncWriteExt;

    #[derive(Default)]
    struct TestWriter {
        lines: usize,
    }

    impl OutputHandler for TestWriter {
        fn write_line(
            &mut self,
            _server: &ServerAddr,
            _name: Option<&str>,
            _line: &Line,
        ) -> Result<()> {
            self.lines += 1;
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn test_source(name: &str) -> MonitorSource {
        MonitorSource::new(
            Arc::new(ServerAddr::from_path(name)),
            Arc::new(None),
            None,
        )
    }

    fn test_batch(source: &str, lines: &[&[u8]]) -> MonitorBatch {
        let len = lines.iter().map(|line| line.len()).sum::<usize>();
        let mut data = Vec::with_capacity(len);
        let mut ranges = Vec::with_capacity(lines.len());
        for line in lines {
            let start = data.len();
            data.extend_from_slice(line);
            ranges.push(start..data.len());
        }

        let budget = Arc::new(Semaphore::new(len.max(1)));
        let permit = budget
            .try_acquire_many_owned(u32::try_from(len.max(1)).unwrap())
            .unwrap();
        let mut batch = MonitorBatch::new(test_source(source));
        batch.add_chunk(Bytes::from(data), ranges, permit);
        batch
    }

    fn test_message(line: &'static [u8]) -> IoMessage {
        IoMessage::Batch(test_batch("stdin", &[line]))
    }

    fn test_io(
        capacity: usize,
        byte_budget: usize,
    ) -> (IoHandle, flume::Receiver<IoMessage>) {
        let (tx, rx) = flume::bounded(capacity);
        (
            IoHandle {
                tx,
                budget: Arc::new(Semaphore::new(byte_budget)),
                byte_budget,
            },
            rx,
        )
    }

    fn empty_filter() -> LineFilter {
        LineFilter::new(Filter::new(Vec::new()), commands::Filter::default())
    }

    async fn reader_batches(
        input: &[u8],
        config: BatchConfig,
    ) -> Vec<MonitorBatch> {
        let (io, rx) = test_io(32, 1024 * 1024);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        run_from_reader(
            "stdin",
            input,
            io,
            empty_filter(),
            config,
            shutdown_rx,
        )
        .await;

        rx.try_iter()
            .filter_map(|message| match message {
                IoMessage::Batch(batch) => Some(batch),
                IoMessage::Preamble(_) | IoMessage::Shutdown => None,
            })
            .collect()
    }

    #[test]
    fn batching_is_enabled_by_default_and_can_be_disabled() {
        assert!(!Options::try_parse_from(["redis-monitor"]).unwrap().no_batch);
        assert!(
            Options::try_parse_from(["redis-monitor", "--no-batch"])
                .unwrap()
                .no_batch
        );
    }

    #[tokio::test]
    async fn batches_records_by_default_and_preserves_source_order() {
        let batches = reader_batches(
            b"one\ntwo\nthree\n",
            BatchConfig {
                records: 3,
                bytes: 1024,
                delay: Duration::from_mins(1),
            },
        )
        .await;

        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0].lines().collect::<Vec<_>>(),
            [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()]
        );
        drop(batches);
    }

    #[tokio::test]
    async fn no_batch_sends_each_record_immediately() {
        let batches =
            reader_batches(b"one\ntwo\nthree\n", BatchConfig::new(false)).await;

        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].lines().next(), Some(b"one".as_slice()));
        assert_eq!(batches[1].lines().next(), Some(b"two".as_slice()));
        assert_eq!(batches[2].lines().next(), Some(b"three".as_slice()));
        drop(batches);
    }

    #[tokio::test]
    async fn stdin_preserves_a_final_record_without_a_newline() {
        let batches = reader_batches(b"one", BatchConfig::new(true)).await;

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].lines().next(), Some(b"one".as_slice()));
        drop(batches);
    }

    #[tokio::test]
    async fn byte_limit_splits_batches_without_dropping_records() {
        let batches = reader_batches(
            b"one\ntwo\n",
            BatchConfig {
                records: 64,
                bytes: 4,
                delay: Duration::from_mins(1),
            },
        )
        .await;

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].lines().next(), Some(b"one".as_slice()));
        assert_eq!(batches[1].lines().next(), Some(b"two".as_slice()));
        drop(batches);
    }

    #[test]
    fn truncated_wire_frame_is_not_scanned() {
        let mut stats = LocalStats::new(1000);

        assert!(
            scan_frames(
                b"partial",
                &empty_filter(),
                None,
                &mut stats,
                64,
                1024,
            )
            .is_none()
        );
        assert_eq!(stats.total, 0);
    }

    #[test]
    fn batches_from_different_sources_remain_atomic() {
        let (tx, rx) = flume::bounded(3);
        tx.send(IoMessage::Batch(test_batch("a", &[b"a1", b"a2"])))
            .unwrap();
        tx.send(IoMessage::Batch(test_batch("b", &[b"b1", b"b2"])))
            .unwrap();

        let IoMessage::Batch(a) = rx.recv().unwrap() else {
            panic!("expected source a batch");
        };
        let IoMessage::Batch(b) = rx.recv().unwrap() else {
            panic!("expected source b batch");
        };

        assert_eq!(a.source.server.to_string(), "a");
        assert_eq!(a.lines().collect::<Vec<_>>(), [b"a1", b"a2"]);
        assert_eq!(b.source.server.to_string(), "b");
        assert_eq!(b.lines().collect::<Vec<_>>(), [b"b1", b"b2"]);
    }

    #[tokio::test]
    async fn oversized_record_uses_the_whole_budget_but_is_accepted() {
        let (io, _rx) = test_io(1, 4);
        let permit = io.reserve_bytes(1024).await;

        assert_eq!(io.budget.available_permits(), 0);
        drop(permit);
        assert_eq!(io.budget.available_permits(), 4);
    }

    #[tokio::test]
    async fn byte_budget_blocks_a_second_batch_until_output_releases_first() {
        let (io, rx) = test_io(2, 5);
        io.send(IoMessage::Batch({
            let mut batch = MonitorBatch::new(test_source("first"));
            batch.add_chunk(
                Bytes::from_static(b"first"),
                std::iter::once(0..5).collect(),
                io.reserve_bytes(5).await,
            );
            batch
        }))
        .await
        .unwrap();

        let mut blocked = Box::pin(io.reserve_bytes(5));
        for _ in 0..100 {
            assert!(futures::poll!(blocked.as_mut()).is_pending());
        }

        drop(rx.recv().unwrap());
        drop(blocked.await);
        assert_eq!(io.budget.available_permits(), 5);
    }

    #[tokio::test]
    async fn shutdown_flushes_a_partial_batch() {
        let (io, rx) = test_io(2, 1024);
        let observer = io.clone();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut input, reader) = tokio::io::duplex(64);
        let task = tokio::spawn(run_from_reader(
            "stdin",
            reader,
            io,
            empty_filter(),
            BatchConfig {
                records: 64,
                bytes: 1024,
                delay: Duration::from_mins(1),
            },
            shutdown_rx,
        ));

        input.write_all(b"one\n").await.unwrap();
        for _ in 0..100 {
            if observer.budget.available_permits() < 1024 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(observer.budget.available_permits() < 1024);
        drop(observer);

        shutdown_tx.send(true).unwrap();
        task.await.unwrap();
        let IoMessage::Batch(batch) = rx.recv().unwrap() else {
            panic!("expected flushed batch");
        };
        assert_eq!(batch.lines().next(), Some(b"one".as_slice()));
    }

    #[test]
    fn ignores_standalone_ok_reply_after_parse_failure() {
        let mut writer = TestWriter::default();
        let mut stats = None;

        let control = test_message(b"OK").process(&mut writer, &mut stats);

        assert!(matches!(control, Control::Continue));
        assert_eq!(writer.lines, 0);
    }

    #[test]
    fn malformed_record_does_not_discard_the_rest_of_its_batch() {
        let mut writer = TestWriter::default();
        let mut stats = None;
        let valid = b"1.000000 [0 127.0.0.1:1] \"PING\"";

        let control =
            IoMessage::Batch(test_batch("stdin", &[b"PONG", valid.as_slice()]))
                .process(&mut writer, &mut stats);

        assert!(matches!(control, Control::Continue));
        assert_eq!(writer.lines, 1);
    }

    #[tokio::test]
    async fn full_output_queue_awaits_without_dropping_or_recounting() {
        let stats = IoStats::default();
        let (tx, rx) = flume::bounded(1);
        tx.send(test_message(b"first")).unwrap();

        let mut blocked =
            Box::pin(send_io_message(&tx, test_message(b"second"), &stats));

        for _ in 0..100 {
            assert!(futures::poll!(blocked.as_mut()).is_pending());
        }
        assert_eq!(stats.snapshot().2, 1);

        let IoMessage::Batch(first) = rx.recv().unwrap() else {
            panic!("expected first record");
        };
        assert_eq!(first.lines().next(), Some(b"first".as_slice()));

        blocked.await.unwrap();
        let IoMessage::Batch(second) = rx.recv().unwrap() else {
            panic!("expected second record");
        };
        assert_eq!(second.lines().next(), Some(b"second".as_slice()));

        send_io_message(&tx, IoMessage::Shutdown, &stats)
            .await
            .unwrap();
        assert!(matches!(rx.recv().unwrap(), IoMessage::Shutdown));
        assert_eq!(stats.snapshot().2, 1);
    }

    #[tokio::test]
    async fn output_queue_disconnection_is_reported_without_a_stall() {
        let stats = IoStats::default();
        let (tx, rx) = flume::bounded(1);
        drop(rx);

        assert!(
            send_io_message(&tx, test_message(b"record"), &stats)
                .await
                .is_err()
        );
        assert_eq!(stats.snapshot().2, 0);
    }
}
