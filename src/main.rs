#![warn(clippy::all, clippy::nursery, clippy::pedantic)]
//#![allow(clippy::non_ascii_literal)]
//#![allow(clippy::must_use_candidate)]
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::{
    collections::HashSet, convert::From, path::PathBuf, str::FromStr,
    time::Instant,
};

use anyhow::{Context, Result, anyhow};
use bytes::{Bytes, BytesMut};
use clap::{CommandFactory, Parser};
use clap_complete::{Shell, generate};
use colored::Color;
use connection::{ServerAddr, TlsConfig};
use filter::Filter;
use futures::stream::FuturesUnordered;
use output::OutputKind;
use rand::{Rng, rng};
use redis::{
    Client, ConnectionAddr, ConnectionInfo, RedisConnectionInfo,
    aio::ConnectionManager as RedisConnectionManager,
};
use tokio::{
    io::{self, AsyncBufRead, AsyncBufReadExt, AsyncReadExt, BufReader},
    sync::mpsc,
    time::{Duration, sleep},
};

use crate::{
    commands::Command,
    config::{Map, ServerAuth},
    connection::{Cluster, Monitor},
    filter::FilterPattern,
    monitor::Line,
    output::OutputHandler,
    stats::CommandStat,
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
  # Monitor a cluster expecting one node to be 127.0.0.1:6379
  redis-monitor -c 6379
  # Monitor two standalone instances
  redis-monitor host1:6379 host2:6379"#
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
           help = "One or more patterns to either filter out or in")]
    filter: Vec<FilterPattern>,

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

fn validate_positive_f64(s: &str) -> Result<f64> {
    match s.parse::<f64>() {
        Ok(val) if val > 0.0 => Ok(val),
        Ok(_) => Err(anyhow!("Value must be positive".to_string())),
        Err(_) => Err(anyhow!("Invalid number".to_string())),
    }
}

impl Options {
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
// one seeds of the same cluster are passed we may map the same keyspace more than
// once. This is fine, but we should be aware of it.
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

#[derive(Debug)]
struct Backoff {
    retries: u32,
    max_delay: Duration,
}

#[derive(Debug)]
struct MonitorMessage {
    pub server: Arc<ServerAddr>,
    pub name: Arc<Option<String>>,
    #[allow(dead_code)]
    pub color: Option<Color>,
    #[allow(dead_code)]
    pub commands: Arc<Option<HashSet<Command>>>,
    line: Bytes,
}

type IoSender = flume::Sender<IoMessage>;

#[derive(Debug, Clone)]
struct IoHandle {
    tx: IoSender,
    stalls: Arc<AtomicU64>,
}

#[derive(Debug)]
enum IoMessage {
    Preamble(Arc<[Monitor]>),
    Stats(Vec<CommandStat>),
    Message(MonitorMessage),
    Shutdown,
}

impl MonitorMessage {
    const fn new(
        server: Arc<ServerAddr>,
        name: Arc<Option<String>>,
        color: Option<Color>,
        commands: Arc<Option<HashSet<Command>>>,
        line: Bytes,
    ) -> Self {
        Self {
            server,
            name,
            color,
            commands,
            line,
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
        let mut rng = rng();

        self.retries += 1;
        let shift_amount =
            (self.retries.min(6) + rng.random_range(0..3)) as u32;
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

async fn run_from_reader<R>(
    name: &str,
    mut reader: R,
    tx: mpsc::Sender<MonitorMessage>,
) where
    R: AsyncBufRead + Unpin,
{
    let server = Arc::new(ServerAddr::from_path(name));
    let name_arc = Arc::new(None::<String>);

    let mut buf = Vec::with_capacity(16 * 1024);

    loop {
        buf.clear();

        match reader.read_until(b'\n', &mut buf).await {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => {
                eprintln!("{server} read error {e}");
                break;
            }
        }

        while buf.last().is_some_and(|b| *b == b'\n' || *b == b'\r') {
            buf.pop();
        }

        if buf.first().is_some_and(|b| *b == b'+') {
            buf.remove(0);
        }

        let line = Bytes::from(buf.clone());

        let msg = MonitorMessage::new(
            Arc::clone(&server),
            Arc::clone(&name_arc),
            None,
            Arc::new(None),
            line,
        );

        if let Err(e) = tx.send(msg).await {
            eprintln!("{server} tx.send failure: {e}");
            break;
        }
    }
}

async fn run_stdin_shim(tx: mpsc::Sender<MonitorMessage>) {
    let stdin = io::stdin();
    let reader = BufReader::new(stdin);
    run_from_reader("stdin", reader, tx).await;
}

async fn run_monitor(mon: Monitor, tx: mpsc::Sender<MonitorMessage>) {
    let server = Arc::new(mon.address.clone());
    let name = Arc::new(mon.name.clone());
    let mut backoff = Backoff::new();
    let mut commands = Arc::new(None);

    loop {
        if commands.as_ref().is_none() {
            if let Some(loaded) = load_command_metadata(&mon).await {
                commands = Arc::new(Some(loaded));
            }
        }

        match mon.clone().connect().await {
            Ok((_, mut reader)) => {
                backoff.reset();

                let mut buf = BytesMut::with_capacity(16 * 1024);

                loop {
                    while let Some(nl) = memchr::memchr(b'\n', &buf) {
                        let mut line = buf.split_to(nl + 1).freeze();
                        if line.ends_with(b"\n") {
                            line.truncate(line.len() - 1);
                        }
                        if line.ends_with(b"\r") {
                            line.truncate(line.len() - 1);
                        }
                        if line.starts_with(b"+") {
                            line = line.slice(1..);
                        }

                        let msg = MonitorMessage::new(
                            Arc::clone(&server),
                            Arc::clone(&name),
                            mon.color,
                            Arc::clone(&commands),
                            line,
                        );

                        if let Err(e) = tx.send(msg).await {
                            eprintln!("{server} tx.send failure: {e}");
                            break;
                        }
                    }

                    match reader.read_buf(&mut buf).await {
                        Ok(0) => {
                            eprintln!("{server} connection closed");
                            break;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("{server} read error {e}");
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                if backoff.retries == 0 {
                    eprintln!("{server} Error connecting {e}");
                }
            }
        }

        sleep(backoff.delay()).await;
    }
}

#[derive(Debug)]
enum Control {
    Shutdown,
    Continue,
}

async fn load_command_metadata(mon: &Monitor) -> Option<HashSet<Command>> {
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
        Ok(commands) => Some(commands),
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

fn connection_info_from_monitor(mon: &Monitor) -> ConnectionInfo {
    let addr = match &mon.address {
        ServerAddr::Tcp(host, port) => {
            if mon.tls.is_some() {
                ConnectionAddr::TcpTls {
                    host: host.clone(),
                    port: *port,
                    insecure: mon
                        .tls
                        .as_ref()
                        .map(|cfg| cfg.insecure)
                        .unwrap_or(false),
                    tls_params: None,
                }
            } else {
                ConnectionAddr::Tcp(host.clone(), *port)
            }
        }
        ServerAddr::Unix(path) => ConnectionAddr::Unix(PathBuf::from(path)),
    };

    let redis = RedisConnectionInfo {
        username: mon.auth.user.clone(),
        password: mon.auth.pass.clone(),
        ..RedisConnectionInfo::default()
    };

    ConnectionInfo { addr, redis }
}

impl IoMessage {
    fn process(
        self,
        w: &mut dyn OutputHandler,
        need_args: bool,
    ) -> Result<Control> {
        match self {
            Self::Preamble(servers) => {
                eprintln!("{}", format_preamble(&servers));
            }
            Self::Stats(s) => {
                w.write_stats(&s)?;
                w.flush()?;
            }
            Self::Message(m) => {
                let parsed = match Line::from_line_bytes(&m.line, need_args) {
                    Ok((_, line)) => line,
                    Err(e) => {
                        let s = String::from_utf8_lossy(&m.line);
                        return Err(anyhow!(
                            "Failed to parse line '{s}' ({e})"
                        ));
                    }
                };

                w.write_line(&m.server, m.name.as_ref().as_deref(), &parsed)?;
            }
            Self::Shutdown => return Ok(Control::Shutdown),
        }

        Ok(Control::Continue)
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
) -> (IoHandle, std::thread::JoinHandle<Result<()>>) {
    const BATCH_MAX: usize = 1024;

    let (tx, rx) = flume::bounded::<IoMessage>(size);

    let fmt = format.to_string();
    let need_args = output_kind.need_args();
    let stalls = Arc::new(AtomicU64::new(0));
    let stalls_io = Arc::clone(&stalls);

    let jh = std::thread::spawn(move || -> Result<()> {
        let stdout = std::io::stdout();
        let mut out = std::io::BufWriter::with_capacity(1 << 20, stdout.lock());
        let mut writer = output_kind.get_writer(&mut out, &fmt);
        let mut last = Instant::now();
        let mut total = 0u64;
        let mut shutdown = false;

        while !shutdown {
            let Ok(first) = rx.recv() else { break };

            match first.process(writer.as_mut(), need_args) {
                Ok(Control::Shutdown) => break,
                Ok(Control::Continue) => {}
                Err(e) => {
                    eprintln!("Error handling message: {e}");
                }
            }

            for msg in rx.try_iter().take(BATCH_MAX - 1) {
                match msg.process(writer.as_mut(), need_args) {
                    Ok(Control::Shutdown) => {
                        shutdown = true;
                        break;
                    }
                    Ok(Control::Continue) => {}
                    Err(e) => {
                        eprintln!("Error handling message: {e}");
                    }
                }
            }

            if last.elapsed() >= Duration::from_secs(1) {
                let n = stalls_io.swap(0, Ordering::Relaxed);
                if n > 0 {
                    total = total.saturating_add(n);
                    eprintln!(
                        "[WARNING] Stalled {n} times due to backpressure (total {total})",
                    );
                }

                last = Instant::now();
            }

            writer.flush()?;
        }

        Ok(())
    });

    (IoHandle { tx, stalls }, jh)
}

fn version_string() -> String {
    let git_display = format!(
        "{GIT_HASH}{}",
        if GIT_DIRTY == "yes" { "-dirty" } else { "" }
    );

    format!("redis-monitor v{VERSION} (git {git_display})")
}

async fn run_stdin(opt: Options) -> Result<()> {
    let format = opt
        .format
        .clone()
        .unwrap_or_else(|| DEFAULT_SINGLE_FORMAT.to_string());

    let (io_tx, io_jh) = start_io_thread(opt.output, &format, 65536);

    let pseudo = Monitor::new(
        Some("stdin"),
        ServerAddr::from_path("stdin"),
        None,
        ServerAuth::default(),
        None,
    );

    let preamble: Arc<[Monitor]> = Arc::from(vec![pseudo]);

    io_tx.tx.send(IoMessage::Preamble(Arc::clone(&preamble)))?;

    let (tx, mut rx) = mpsc::channel::<MonitorMessage>(16384);

    tokio::spawn(async move {
        run_stdin_shim(tx).await;
    });

    let filter: Filter = opt.filter.into();

    while let Some(message) = rx.recv().await {
        if !filter.matches(&message.line) {
            continue;
        }

        let mut msg = IoMessage::Message(message);
        loop {
            match io_tx.tx.try_send(msg) {
                Ok(()) => break,
                Err(flume::TrySendError::Full(m)) => {
                    io_tx.stalls.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                    msg = m;
                }
                Err(flume::TrySendError::Disconnected(_)) => {
                    eprintln!("io thread disconnected");
                    break;
                }
            }
        }
    }

    let _ = io_tx.tx.send(IoMessage::Shutdown);
    if let Err(e) = io_jh
        .join()
        .unwrap_or_else(|e| Err(anyhow!("IO thread panicked: {e:?}")))
    {
        eprintln!("IO thread error: {e}");
    }

    Ok(())
}

async fn run_wire(opt: Options) -> Result<()> {
    let cfg = Map::load(opt.config_file.as_deref())?;

    if opt.instances.is_empty() {
        eprintln!("Must pass at least one unstance (host/port or name)");
        std::process::exit(1);
    }

    let tls = opt.get_tls_config()?;
    let auth = opt.get_server_auth();

    let seeds = if opt.cluster {
        process_cluster_instances(&opt, tls.as_ref(), &auth)
    } else {
        process_instances(&cfg, &opt, tls.as_ref(), &auth)
    };

    let (tx, mut rx) = mpsc::channel::<MonitorMessage>(16384);

    let tasks = FuturesUnordered::new();

    let format = opt.format.clone().unwrap_or_else(|| {
        if seeds.len() > 1 {
            DEFAULT_MULTI_FORMAT.to_string()
        } else {
            DEFAULT_SINGLE_FORMAT.to_string()
        }
    });

    let mut stats = if opt.output == OutputKind::Plain {
        opt.stats.map(|_| stats::CommandStats::new())
    } else {
        None
    };

    let interval = Duration::from_secs_f64(opt.stats.unwrap_or(1.0));
    let filter: Filter = opt.filter.into();
    let mut tick = Instant::now();

    let (io_tx, io_jh) = start_io_thread(opt.output, &format, 65536);

    let preamble: Arc<[Monitor]> = Arc::from(seeds);
    io_tx.tx.send(IoMessage::Preamble(Arc::clone(&preamble)))?;

    for mon in preamble.iter().cloned() {
        let tx_task = tx.clone();
        tasks.push(tokio::spawn(async move {
            run_monitor(mon, tx_task).await;
        }));
    }

    drop(tx);

    while let Some(message) = rx.recv().await {
        if !filter.matches(&message.line) {
            continue;
        }

        if let Some(ref mut stats) = stats {
            stats.try_incr(&message.line, message.line.len());
            if tick.elapsed() >= interval {
                let s = stats.get_stats();
                io_tx.tx.send(IoMessage::Stats(s)).unwrap_or_else(|e| {
                    eprintln!("Failed to send stats: {e}");
                });
                tick = Instant::now();
            }
        }

        let mut msg = IoMessage::Message(message);

        loop {
            match io_tx.tx.try_send(msg) {
                Ok(()) => break,
                Err(flume::TrySendError::Full(m)) => {
                    io_tx.stalls.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                    msg = m;
                }
                Err(flume::TrySendError::Disconnected(_m)) => {
                    eprintln!("io thread disconnected");
                    break;
                }
            }
        }
    }

    let _ = io_tx.tx.send(IoMessage::Shutdown);
    if let Err(e) = io_jh
        .join()
        .unwrap_or_else(|e| Err(anyhow!("IO thread panicked: {e:?}")))
    {
        eprintln!("IO thread error: {e}");
    }

    Ok(())
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

    if opt.stdin {
        run_stdin(opt).await
    } else {
        run_wire(opt).await
    }
}
