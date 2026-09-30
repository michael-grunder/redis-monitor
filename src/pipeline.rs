//! The record pipeline.
//!
//! Each source runs as a task that frames records from its stream, filters
//! them, and formats accepted records into byte batches. Formatting in the
//! sources spreads per-record work across the runtime's worker threads; the
//! single output thread only writes finished batches, in the order each source
//! sent them. There is no ordering guarantee across sources.
//!
//! Queued batches are bounded by count (the channel) and by bytes (a shared
//! semaphore budget), so a slow consumer applies backpressure to the sources
//! instead of dropping records or growing without bound.
use std::{
    io::{Read, Write},
    pin::Pin,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::Instant,
};

use anyhow::{Context, Result, anyhow};
use bytes::{Buf, Bytes, BytesMut};
use rand::{RngExt, rngs::ThreadRng};
use redis::aio::ConnectionManager;
use redis_monitor::commands::{self, Command};
use tokio::{
    io::{AsyncRead, AsyncReadExt, ReadBuf},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch},
    time::{Duration, sleep, sleep_until},
};

use crate::{
    connection::{self, Monitor},
    filter::LineFilter,
    output::{FormatError, Formatter, Source},
    stats::CommandStats,
};

const DEFAULT_BATCH_RECORDS: usize = 64;
const DEFAULT_BATCH_BYTES: usize = 256 * 1024;
const DEFAULT_BATCH_DELAY: Duration = Duration::from_millis(5);
/// Formatted bytes that may be queued for output across all sources.
pub const OUTPUT_BYTE_BUDGET: usize = 64 * 1024 * 1024;
const OUTPUT_BATCH_CAPACITY: usize = 1024;
const OUTPUT_IMMEDIATE_CAPACITY: usize = 16_384;
const READ_CHUNK: usize = 64 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// `eprintln!` with a `[WARNING]` prefix.
macro_rules! warn {
    ($($arg:tt)*) => {
        eprintln!("[WARNING] {}", format!($($arg)*))
    };
}

#[derive(Debug, Default)]
struct Stalls {
    total: AtomicU64,
    current: AtomicU64,
}

/// Process-wide counters. They are telemetry only, so relaxed ordering is
/// sufficient; sources aggregate locally and fold in periodically.
#[derive(Debug, Default)]
pub struct IoStats {
    total: AtomicU64,
    filtered: AtomicU64,
    /// Records that could not be formatted. Updated only on that cold path.
    invalid: AtomicU64,
    stalls: Stalls,
}

pub static IO_STATS: IoStats = IoStats {
    total: AtomicU64::new(0),
    filtered: AtomicU64::new(0),
    invalid: AtomicU64::new(0),
    stalls: Stalls {
        total: AtomicU64::new(0),
        current: AtomicU64::new(0),
    },
};

impl IoStats {
    /// `(processed, filtered, stalls, invalid)`
    fn snapshot(&self) -> (u64, u64, u64, u64) {
        let total = self.total.load(Ordering::Relaxed);
        let filtered = self.filtered.load(Ordering::Relaxed);
        let invalid = self.invalid.load(Ordering::Relaxed);

        let stalls_current = self.stalls.current.load(Ordering::Relaxed);
        let stalls_total =
            self.stalls.total.load(Ordering::Relaxed) + stalls_current;

        (total, filtered, stalls_total, invalid)
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

pub fn print_final_stats() {
    let (total, filtered, stalls, invalid) = IO_STATS.snapshot();
    eprintln!(
        "Processed {total} lines (filtered: {filtered}), backpressure stalls: \
        {stalls}, invalid records: {invalid}"
    );
}

/// Reports invalid records on stderr, at most `LIMIT` per reporting tick
/// across all sources, so a stream of garbage cannot flood stderr.
struct InvalidReports {
    reported: AtomicU32,
    suppressed: AtomicU64,
}

static INVALID: InvalidReports = InvalidReports {
    reported: AtomicU32::new(0),
    suppressed: AtomicU64::new(0),
};

impl InvalidReports {
    const LIMIT: u32 = 10;

    fn report(&self, error: &FormatError) {
        IO_STATS.invalid.fetch_add(1, Ordering::Relaxed);
        if self.reported.fetch_add(1, Ordering::Relaxed) < Self::LIMIT {
            eprintln!("Error handling record: {error}");
        } else {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn tick(&self) {
        self.reported.store(0, Ordering::Relaxed);
        let suppressed = self.suppressed.swap(0, Ordering::Relaxed);
        if suppressed > 0 {
            warn!("suppressed {suppressed} invalid record messages");
        }
    }
}

/// Per-source counters folded into [`IO_STATS`] every `batch_size` records.
#[derive(Debug)]
struct LocalStats {
    batch_size: u64,
    total: u64,
    filtered: u64,
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

/// Per-command statistics shared by every source.
pub type SharedStats = Arc<Mutex<CommandStats>>;

fn lock(stats: &SharedStats) -> std::sync::MutexGuard<'_, CommandStats> {
    // Counters remain meaningful even if another thread panicked mid-update.
    stats.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A source's command counts, merged into the shared totals once per read
/// instead of locking per record.
struct SourceStats {
    local: CommandStats,
    shared: SharedStats,
}

impl SourceStats {
    fn publish(&mut self) {
        self.local.merge_into(&mut lock(&self.shared));
    }
}

#[derive(Debug)]
struct Backoff {
    retries: u32,
}

impl Backoff {
    const MIN_DELAY: Duration = Duration::from_millis(50);
    const MAX_DELAY: Duration = Duration::from_secs(1);

    const fn new() -> Self {
        Self { retries: 0 }
    }

    /// Exponential backoff with jitter, capped at `MAX_DELAY`.
    fn delay(&mut self) -> Duration {
        self.retries = self.retries.saturating_add(1);
        let shift =
            self.retries.min(6) + ThreadRng::default().random_range(0..3);
        Self::MIN_DELAY
            .saturating_mul(1 << shift)
            .min(Self::MAX_DELAY)
    }

    const fn reset(&mut self) {
        self.retries = 0;
    }
}

#[derive(Debug, Copy, Clone)]
pub struct BatchConfig {
    records: usize,
    bytes: usize,
    delay: Duration,
}

impl BatchConfig {
    /// With batching enabled, sources hold records for up to
    /// `DEFAULT_BATCH_DELAY` to coalesce them. Otherwise nothing is ever held
    /// back: every complete record already in the read buffer is handed off
    /// at once, so records that arrived in the same read share a message but
    /// no record waits for later input.
    pub const fn new(enabled: bool) -> Self {
        if enabled {
            Self {
                records: DEFAULT_BATCH_RECORDS,
                bytes: DEFAULT_BATCH_BYTES,
                delay: DEFAULT_BATCH_DELAY,
            }
        } else {
            Self {
                records: usize::MAX,
                bytes: DEFAULT_BATCH_BYTES,
                delay: Duration::ZERO,
            }
        }
    }

    /// Output queue length for this batching mode.
    pub const fn queue_capacity(&self) -> usize {
        if self.delay.is_zero() {
            OUTPUT_IMMEDIATE_CAPACITY
        } else {
            OUTPUT_BATCH_CAPACITY
        }
    }
}

/// Formatted records ready to write. The permit holds the batch's share of
/// the byte budget until the output thread drops the batch.
#[derive(Debug)]
pub struct Batch {
    data: Vec<u8>,
    records: usize,
    permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub enum IoMessage {
    Preamble(String),
    Batch(Batch),
    Shutdown,
}

/// The output queue has closed, so there is nowhere left to send records.
#[derive(Debug)]
struct OutputClosed;

impl<T> From<flume::SendError<T>> for OutputClosed {
    fn from(_: flume::SendError<T>) -> Self {
        Self
    }
}

/// Sends batches to the output thread, subject to the byte budget.
#[derive(Debug, Clone)]
pub struct IoHandle {
    tx: flume::Sender<IoMessage>,
    budget: Arc<Semaphore>,
    byte_budget: usize,
}

impl IoHandle {
    fn new(tx: flume::Sender<IoMessage>, byte_budget: usize) -> Self {
        Self {
            tx,
            budget: Arc::new(Semaphore::new(byte_budget)),
            byte_budget,
        }
    }

    pub async fn send(
        &self,
        message: IoMessage,
    ) -> Result<(), flume::SendError<IoMessage>> {
        send_io_message(&self.tx, message, &IO_STATS).await
    }

    /// A batch larger than the whole budget takes all of it, so it is still
    /// accepted once everything else has drained.
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

async fn send_io_message(
    tx: &flume::Sender<IoMessage>,
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

/// Accumulates one source's formatted records into batches.
#[derive(Debug)]
struct BatchSender {
    io: IoHandle,
    config: BatchConfig,
    pending: Option<Batch>,
    deadline: Option<tokio::time::Instant>,
}

impl BatchSender {
    const fn new(io: IoHandle, config: BatchConfig) -> Self {
        Self {
            io,
            config,
            pending: None,
            deadline: None,
        }
    }

    const fn is_empty(&self) -> bool {
        self.pending.is_none()
    }

    fn remaining_records(&self) -> usize {
        self.pending.as_ref().map_or(self.config.records, |batch| {
            self.config.records.saturating_sub(batch.records)
        })
    }

    fn remaining_bytes(&self) -> usize {
        self.pending.as_ref().map_or(self.config.bytes, |batch| {
            self.config.bytes.saturating_sub(batch.data.len())
        })
    }

    async fn reserve(
        &mut self,
        bytes: usize,
    ) -> Result<OwnedSemaphorePermit, OutputClosed> {
        if let Some(permit) = self.io.try_reserve_bytes(bytes) {
            return Ok(permit);
        }

        // Never wait for more byte-budget capacity while holding a partial
        // batch: with many sources doing the same, that would deadlock.
        self.flush().await?;
        IO_STATS.stall();
        Ok(self.io.reserve_bytes(bytes).await)
    }

    async fn add(
        &mut self,
        data: Vec<u8>,
        records: usize,
        permit: OwnedSemaphorePermit,
    ) -> Result<(), OutputClosed> {
        if !self.is_empty()
            && (records > self.remaining_records()
                || data.len() > self.remaining_bytes())
        {
            self.flush().await?;
        }

        if let Some(batch) = &mut self.pending {
            batch.data.extend_from_slice(&data);
            batch.records += records;
            batch.permit.merge(permit);
        } else {
            self.pending = Some(Batch {
                data,
                records,
                permit,
            });
            self.deadline =
                Some(tokio::time::Instant::now() + self.config.delay);
        }

        let full = self.remaining_records() == 0
            || self.remaining_bytes() == 0
            || self.deadline.is_some_and(|deadline| {
                deadline <= tokio::time::Instant::now()
            });
        if full {
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), OutputClosed> {
        self.deadline = None;
        if let Some(batch) = self.pending.take() {
            self.io.send(IoMessage::Batch(batch)).await?;
        }
        Ok(())
    }
}

/// Settings shared by every source.
#[derive(Clone)]
pub struct Pipeline {
    pub io: IoHandle,
    pub filter: LineFilter,
    pub formatter: Arc<Formatter>,
    pub batch: BatchConfig,
    pub stats: Option<SharedStats>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum StreamExit {
    End,
    Shutdown,
    OutputClosed,
}

/// The records consumed by one scan of the read buffer.
struct Scan {
    /// Bytes of input consumed, through the last record's newline.
    end: usize,
    /// Records formatted into the output buffer.
    accepted: usize,
}

/// One source's framing, filtering, and formatting state.
struct Producer {
    source: Arc<Source>,
    filter: LineFilter,
    formatter: Arc<Formatter>,
    commands: Option<commands::Lookup>,
    stats: LocalStats,
    command_stats: Option<SourceStats>,
    sender: BatchSender,
}

impl Producer {
    fn new(source: Source, pipeline: Pipeline) -> Self {
        Self {
            source: Arc::new(source),
            filter: pipeline.filter,
            formatter: pipeline.formatter,
            commands: None,
            stats: LocalStats::new(1000),
            command_stats: pipeline.stats.map(|shared| SourceStats {
                local: CommandStats::default(),
                shared,
            }),
            sender: BatchSender::new(pipeline.io, pipeline.batch),
        }
    }

    /// Frame, filter, and format complete records at the start of `buf`,
    /// whose first newline is at `first_nl`, appending output to `out`.
    /// Returns `None` when no record fits the limits.
    fn scan(
        &mut self,
        buf: &[u8],
        first_nl: usize,
        max_records: usize,
        max_bytes: usize,
        out: &mut Vec<u8>,
    ) -> Option<Scan> {
        let mut start = 0;
        let mut records = 0;
        let mut accepted = 0;
        // Reused across records borrowing this read buffer. Dropped after the
        // scan so neither decoded values nor pathological argument counts are
        // retained.
        let mut args = Vec::new();
        let mut next_nl = Some(first_nl);

        while records < max_records {
            let Some(nl) = next_nl else {
                break;
            };
            let wire_end = nl + 1;
            if records > 0 && wire_end > max_bytes {
                break;
            }

            // Strip an optional CR and RESP simple-string prefix.
            let mut line = &buf[start..nl];
            line = line.strip_suffix(b"\r").unwrap_or(line);
            line = line.strip_prefix(b"+").unwrap_or(line);

            self.stats.tick();
            if !self.filter.matches(self.commands.as_ref(), line, &mut args) {
                self.stats.filtered();
            } else if line != b"OK" {
                // A standalone OK is the reply to MONITOR itself.
                match self.formatter.format(out, &self.source, line, &mut args)
                {
                    Ok(()) => {
                        accepted += 1;
                        if let Some(stats) = &mut self.command_stats {
                            stats.local.record(line);
                        }
                    }
                    Err(error) => INVALID.report(&error),
                }
            }

            start = wire_end;
            records += 1;
            if wire_end >= max_bytes {
                break;
            }
            next_nl = memchr::memchr(b'\n', &buf[start..]).map(|nl| nl + start);
        }

        (records > 0).then_some(Scan {
            end: start,
            accepted,
        })
    }

    /// Send every complete record in `buf`. `searched` is how many leading
    /// bytes are already known to contain no newline, so a record arriving
    /// over many reads is scanned once rather than once per read.
    async fn drain(
        &mut self,
        buf: &mut BytesMut,
        searched: &mut usize,
    ) -> Result<(), OutputClosed> {
        loop {
            let Some(first_nl) = memchr::memchr(b'\n', &buf[*searched..])
                .map(|nl| nl + *searched)
            else {
                *searched = buf.len();
                return Ok(());
            };
            *searched = 0;
            if self.sender.remaining_records() == 0 {
                self.sender.flush().await?;
            }

            let max_bytes = self.sender.config.bytes;
            let mut out = Vec::with_capacity(buf.len().min(max_bytes));
            let Some(scan) = self.scan(
                &buf[..],
                first_nl,
                self.sender.remaining_records(),
                max_bytes,
                &mut out,
            ) else {
                return Ok(());
            };
            buf.advance(scan.end);

            if let Some(stats) = &mut self.command_stats {
                stats.publish();
            }
            if out.is_empty() {
                continue;
            }
            let permit = self.sender.reserve(out.len()).await?;
            self.sender.add(out, scan.accepted, permit).await?;
        }
    }

    /// Consume `buf` and then `reader` until it ends, shutdown is requested,
    /// or output closes. With `accept_trailing_frame`, a final record without
    /// a newline is kept; otherwise it is treated as truncated and discarded.
    async fn consume<R>(
        &mut self,
        mut buf: BytesMut,
        mut reader: R,
        shutdown: &mut watch::Receiver<bool>,
        accept_trailing_frame: bool,
    ) -> StreamExit
    where
        R: AsyncRead + Unpin,
    {
        let mut searched = 0;
        let source = Arc::clone(&self.source);

        loop {
            if self.drain(&mut buf, &mut searched).await.is_err() {
                return StreamExit::OutputClosed;
            }

            if *shutdown.borrow() {
                return self.finish(StreamExit::Shutdown).await;
            }

            // Guarantee a reasonably sized read: when the buffer is exactly
            // full, `BytesMut` would otherwise offer only a few bytes of spare
            // capacity.
            buf.reserve(READ_CHUNK);
            let read = async {
                match reader.read_buf(&mut buf).await {
                    Ok(0) => false,
                    Ok(_) => true,
                    Err(e) => {
                        eprintln!("{source} read error {e}");
                        false
                    }
                }
            };
            let deadline = self.sender.deadline;

            tokio::select! {
                biased;
                () = shutdown_requested(shutdown) => {
                    return self.finish(StreamExit::Shutdown).await;
                }
                () = sleep_until_deadline(deadline) => {
                    if self.sender.flush().await.is_err() {
                        return StreamExit::OutputClosed;
                    }
                }
                more = read => {
                    if !more {
                        break;
                    }
                }
            }
        }

        if accept_trailing_frame && !buf.is_empty() {
            buf.extend_from_slice(b"\n");
        }
        if self.drain(&mut buf, &mut searched).await.is_err() {
            return StreamExit::OutputClosed;
        }
        self.finish(StreamExit::End).await
    }

    /// Hand off any partial batch, then report `exit`.
    async fn finish(&mut self, exit: StreamExit) -> StreamExit {
        match self.sender.flush().await {
            Ok(()) => exit,
            Err(OutputClosed) => StreamExit::OutputClosed,
        }
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        self.stats.fold();
    }
}

/// Resolves once shutdown is requested (or the shutdown sender is gone).
async fn shutdown_requested(shutdown: &mut watch::Receiver<bool>) {
    let _ = shutdown.wait_for(|stop| *stop).await;
}

/// Sleeps until `deadline`, or forever when there is none.
async fn sleep_until_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// A blocking reader (such as stdin) read on a dedicated thread, a few
/// chunks ahead of the consumer, so reading overlaps with filtering and
/// formatting. Tokio's own stdin reads only while polled, which serializes
/// the two.
pub struct ReadAhead {
    rx: mpsc::Receiver<std::io::Result<Bytes>>,
    chunk: Bytes,
}

impl ReadAhead {
    const CHUNK: usize = 256 * 1024;
    /// Chunks read ahead of the consumer, bounding memory use.
    const DEPTH: usize = 4;

    /// # Errors
    /// Returns an error if the reader thread cannot be spawned.
    pub fn spawn(mut reader: impl Read + Send + 'static) -> Result<Self> {
        let (tx, rx) = mpsc::channel(Self::DEPTH);
        std::thread::Builder::new()
            .name("reader".into())
            .spawn(move || {
                let mut scratch = vec![0; Self::CHUNK];
                loop {
                    // One `read` returns whatever is available, so records
                    // are not held back waiting for a full chunk.
                    let chunk = match reader.read(&mut scratch) {
                        Ok(0) => break,
                        Ok(n) => Ok(Bytes::copy_from_slice(&scratch[..n])),
                        Err(e)
                            if e.kind() == std::io::ErrorKind::Interrupted =>
                        {
                            continue;
                        }
                        Err(e) => Err(e),
                    };
                    let failed = chunk.is_err();
                    // A closed channel means the consumer has stopped.
                    if tx.blocking_send(chunk).is_err() || failed {
                        break;
                    }
                }
            })
            .context("Failed to start the reader thread")?;
        Ok(Self {
            rx,
            chunk: Bytes::new(),
        })
    }
}

impl AsyncRead for ReadAhead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        while self.chunk.is_empty() {
            match std::task::ready!(self.rx.poll_recv(cx)) {
                Some(Ok(chunk)) => self.chunk = chunk,
                Some(Err(e)) => return std::task::Poll::Ready(Err(e)),
                // End of input.
                None => return std::task::Poll::Ready(Ok(())),
            }
        }
        let len = self.chunk.len().min(buf.remaining());
        buf.put_slice(&self.chunk.split_to(len));
        std::task::Poll::Ready(Ok(()))
    }
}

/// Read MONITOR records from a byte stream such as stdin.
pub async fn run_from_reader<R>(
    name: &str,
    reader: R,
    pipeline: Pipeline,
    mut shutdown: watch::Receiver<bool>,
) where
    R: AsyncRead + Unpin,
{
    let source = Source::from_reader(name);
    let mut producer = Producer::new(source, pipeline);
    producer
        .consume(BytesMut::new(), reader, &mut shutdown, true)
        .await;
}

/// Monitor one server until shutdown, reconnecting with backoff.
pub async fn run_monitor(
    mon: Monitor,
    pipeline: Pipeline,
    mut shutdown: watch::Receiver<bool>,
) {
    let needs_cmds = pipeline.filter.needs_cmds();
    let needs_keys = pipeline.filter.needs_keys();
    let source = Source::new(&mon.address, mon.name.clone());
    let mut producer = Producer::new(source, pipeline);
    let mut backoff = Backoff::new();

    loop {
        let connected = tokio::select! {
            biased;
            () = shutdown_requested(&mut shutdown) => break,
            result = tokio::time::timeout(CONNECT_TIMEOUT, mon.connect()) => {
                result.unwrap_or_else(|_| {
                    Err(anyhow!("timed out after {CONNECT_TIMEOUT:?}"))
                })
            }
        };

        match connected {
            Ok((stream, pending)) => {
                // Load metadata lazily so unavailable servers can recover.
                // Each new MONITOR connection gets a fresh command table.
                if producer.commands.is_none() && needs_cmds {
                    producer.commands = tokio::select! {
                        biased;
                        () = shutdown_requested(&mut shutdown) => break,
                        cmds = load_cmds(&mon, needs_keys) => cmds,
                    };
                }

                if needs_keys && producer.commands.is_none() {
                    // Never emit unfiltered records when metadata is required.
                    // Back off before reconnecting so failures cannot spin.
                    drop(stream);
                    drop(pending);
                    tokio::select! {
                        biased;
                        () = shutdown_requested(&mut shutdown) => break,
                        () = sleep(backoff.delay()) => {}
                    }
                    continue;
                }
                backoff.reset();

                // Start with anything that arrived along with the MONITOR
                // reply.
                match producer
                    .consume(pending, stream, &mut shutdown, false)
                    .await
                {
                    StreamExit::Shutdown | StreamExit::OutputClosed => break,
                    StreamExit::End => {
                        producer.commands = None;
                        eprintln!("{} connection closed", producer.source);
                    }
                }
            }
            Err(e) => {
                if backoff.retries == 0 {
                    eprintln!("{} Error connecting {e}", producer.source);
                }
            }
        }

        tokio::select! {
            biased;
            () = shutdown_requested(&mut shutdown) => break,
            () = sleep(backoff.delay()) => {}
        }
    }

    // On shutdown, hand off anything still pending; if output has already
    // closed, there is nowhere left to send it.
    let _ = producer.sender.flush().await;
}

async fn load_cmds(
    mon: &Monitor,
    key_filter: bool,
) -> Option<commands::Lookup> {
    let effect = if key_filter {
        "key filtering cannot run; reconnecting"
    } else {
        "--flags will not filter this source"
    };
    let addr = &mon.address;
    let load = async {
        let client = connection::client(addr, &mon.auth, mon.tls.as_deref())?;
        let mut manager: ConnectionManager = client
            .get_connection_manager()
            .await
            .context("Failed to open async connection")?;
        Command::load(&mut manager).await
    };

    match tokio::time::timeout(CONNECT_TIMEOUT, load).await {
        Ok(Ok(commands)) => Some(commands.into()),
        Ok(Err(err)) => {
            eprintln!(
                "{addr} failed to load COMMAND metadata ({effect}): {err:#}"
            );
            None
        }
        Err(_) => {
            eprintln!("{addr} timed out loading COMMAND metadata ({effect})");
            None
        }
    }
}

/// Periodic `--stats` reports.
struct StatsReporter {
    shared: SharedStats,
    interval: Duration,
    tick: Instant,
}

impl StatsReporter {
    /// Checked once per output drain rather than per record.
    fn report_if_due(&mut self) {
        if self.tick.elapsed() >= self.interval {
            eprintln!("[stats]: {}", lock(&self.shared));
            self.tick = Instant::now();
        }
    }
}

/// Write batches until shutdown or disconnection. Output is flushed whenever
/// the queue runs empty, so buffering never delays a record that has no
/// successor waiting behind it.
fn write_output(
    rx: &flume::Receiver<IoMessage>,
    out: &mut impl Write,
    mut header: Option<&[u8]>,
    mut stats: Option<StatsReporter>,
) -> Result<()> {
    const DRAIN_MAX: usize = 16;

    let mut last = Instant::now();
    'outer: while let Ok(first) = rx.recv() {
        let drained =
            std::iter::once(first).chain(rx.try_iter().take(DRAIN_MAX - 1));
        for message in drained {
            match message {
                IoMessage::Preamble(text) => eprintln!("{text}"),
                IoMessage::Batch(batch) => {
                    if let Some(header) = header.take() {
                        out.write_all(header)?;
                    }
                    out.write_all(&batch.data)?;
                }
                IoMessage::Shutdown => break 'outer,
            }
        }

        if let Some(stats) = &mut stats {
            stats.report_if_due();
        }

        if last.elapsed() >= Duration::from_secs(1) {
            let (curr, tot) = IO_STATS.fold();
            if curr > 0 {
                warn!("backpressure stalls (curr: {curr}, total: {tot})");
            }
            INVALID.tick();
            last = Instant::now();
        }

        if rx.is_empty() {
            out.flush()?;
        }
    }

    INVALID.tick();
    out.flush()?;
    Ok(())
}

fn is_broken_pipe(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|e| e.kind() == std::io::ErrorKind::BrokenPipe)
    })
}

/// Start the output thread writing to stdout.
///
/// # Errors
/// Returns an error if the thread cannot be spawned.
pub fn start_output(
    header: Option<&'static [u8]>,
    capacity: usize,
    byte_budget: usize,
    stats: Option<(Duration, SharedStats)>,
    shutdown: watch::Sender<bool>,
) -> Result<(IoHandle, std::thread::JoinHandle<Result<()>>)> {
    let (tx, rx) = flume::bounded::<IoMessage>(capacity);
    let stats = stats.map(|(interval, shared)| StatsReporter {
        shared,
        interval,
        tick: Instant::now(),
    });

    let jh = std::thread::Builder::new()
        .name("output".into())
        .spawn(move || -> Result<()> {
            let stdout = std::io::stdout();
            let mut out =
                std::io::BufWriter::with_capacity(1 << 20, stdout.lock());
            let result = write_output(&rx, &mut out, header, stats);

            // If output stopped early, make sure no source keeps waiting on
            // it: dropping the receiver releases queued byte-budget permits
            // and fails pending sends, and the shutdown signal stops idle
            // sources.
            drop(rx);
            shutdown.send_replace(true);

            match result {
                // The reader went away (e.g. `redis-monitor | head`).
                Err(e) if is_broken_pipe(&e) => Ok(()),
                result => result,
            }
        })
        .context("Failed to start the output thread")?;

    Ok((IoHandle::new(tx, byte_budget), jh))
}

/// Ask the output thread to finish, and wait for it.
///
/// # Errors
/// Returns the output thread's error, if any.
pub async fn finish_output(
    io: IoHandle,
    jh: std::thread::JoinHandle<Result<()>>,
) -> Result<()> {
    // The thread may already have stopped (for example on a broken pipe);
    // its result below is what matters.
    let _ = io.send(IoMessage::Shutdown).await;
    drop(io);
    let result = tokio::task::spawn_blocking(move || jh.join())
        .await
        .context("Output thread join task failed")?
        .map_err(|error| anyhow!("Output thread panicked: {error:?}"))?;
    result.context("Output thread failed")
}

#[cfg(test)]
pub mod tests {
    use redis_monitor::commands;
    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::{
        connection::ServerAddr,
        filter::{Filter, LineFilter},
        key_fixture,
        output::OutputKind,
    };

    fn empty_filter() -> LineFilter {
        LineFilter::new(
            None,
            Filter::new(Vec::new()).unwrap(),
            Filter::new(Vec::new()).unwrap(),
            commands::Filter::default(),
        )
    }

    pub fn test_io(
        capacity: usize,
        byte_budget: usize,
    ) -> (IoHandle, flume::Receiver<IoMessage>) {
        let (tx, rx) = flume::bounded(capacity);
        (IoHandle::new(tx, byte_budget), rx)
    }

    pub fn pipeline(io: IoHandle, batch: BatchConfig) -> Pipeline {
        Pipeline {
            io,
            filter: empty_filter(),
            formatter: Arc::new(Formatter::Raw),
            batch,
            stats: None,
        }
    }

    fn producer(io: IoHandle, batch: BatchConfig) -> Producer {
        Producer::new(
            Source::new(&ServerAddr::from_path("test"), None),
            pipeline(io, batch),
        )
    }

    /// A batch holding `data` and a permit for its size.
    fn test_batch(data: &[u8]) -> Batch {
        let budget = Arc::new(Semaphore::new(data.len().max(1)));
        let permit = budget
            .try_acquire_many_owned(u32::try_from(data.len().max(1)).unwrap())
            .unwrap();
        Batch {
            data: data.to_vec(),
            records: 1,
            permit,
        }
    }

    /// The lines of each batch received so far.
    fn batch_lines(rx: &flume::Receiver<IoMessage>) -> Vec<Vec<Vec<u8>>> {
        rx.try_iter()
            .filter_map(|message| match message {
                IoMessage::Batch(batch) => Some(
                    batch
                        .data
                        .split_inclusive(|&b| b == b'\n')
                        .map(|line| line.strip_suffix(b"\n").unwrap().to_vec())
                        .collect(),
                ),
                IoMessage::Preamble(_) | IoMessage::Shutdown => None,
            })
            .collect()
    }

    async fn reader_batches(
        input: &[u8],
        config: BatchConfig,
    ) -> Vec<Vec<Vec<u8>>> {
        let (io, rx) = test_io(32, 1024 * 1024);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        run_from_reader("stdin", input, pipeline(io, config), shutdown_rx)
            .await;
        batch_lines(&rx)
    }

    #[tokio::test]
    async fn batching_coalesces_records_and_preserves_source_order() {
        let batches =
            reader_batches(b"one\ntwo\nthree\n", BatchConfig::new(true)).await;

        assert_eq!(batches, [[b"one".as_slice(), b"two", b"three"]]);
    }

    #[tokio::test]
    async fn default_hands_off_each_read_without_holding_records() {
        let (io, rx) = test_io(4, 1024);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut input, reader) = tokio::io::duplex(64);
        let mut task = Box::pin(run_from_reader(
            "stdin",
            reader,
            pipeline(io, BatchConfig::new(false)),
            shutdown_rx,
        ));

        // Records that arrive together are handed off together...
        input.write_all(b"one\ntwo\n").await.unwrap();
        assert!(futures::poll!(task.as_mut()).is_pending());
        assert_eq!(batch_lines(&rx), [[b"one", b"two"]]);

        // ...and a later record is not held back waiting for more input.
        input.write_all(b"three\n").await.unwrap();
        assert!(futures::poll!(task.as_mut()).is_pending());
        assert_eq!(batch_lines(&rx), [[b"three"]]);

        drop(input);
        task.await;
    }

    #[tokio::test]
    async fn stdin_preserves_a_final_record_without_a_newline() {
        let batches = reader_batches(b"one", BatchConfig::new(true)).await;

        assert_eq!(batches, [[b"one"]]);
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

        assert_eq!(batches, [[b"one"], [b"two"]]);
    }

    #[tokio::test]
    async fn truncated_wire_frame_is_discarded_at_end_of_stream() {
        let (io, rx) = test_io(4, 1024);
        let mut producer = producer(io, BatchConfig::new(false));
        let (_shutdown_tx, mut shutdown) = watch::channel(false);

        let exit = producer
            .consume(
                BytesMut::new(),
                &b"one\npartial"[..],
                &mut shutdown,
                false,
            )
            .await;

        assert_eq!(exit, StreamExit::End);
        assert_eq!(producer.stats.total, 1);
        drop(producer);
        assert_eq!(batch_lines(&rx), [[b"one"]]);
    }

    #[tokio::test]
    async fn bytes_received_with_the_monitor_reply_come_first() {
        let (io, rx) = test_io(4, 1024);
        let mut producer = producer(io, BatchConfig::new(false));
        let (_shutdown_tx, mut shutdown) = watch::channel(false);

        producer
            .consume(
                BytesMut::from(&b"first\nsec"[..]),
                &b"ond\nthird\n"[..],
                &mut shutdown,
                false,
            )
            .await;

        drop(producer);
        assert_eq!(
            batch_lines(&rx).concat(),
            [b"first".as_slice(), b"second", b"third"]
        );
    }

    #[tokio::test]
    async fn record_spanning_many_reads_is_framed_intact() {
        let (io, rx) = test_io(64, 1024 * 1024);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        // A small pipe forces the large record to arrive over many reads.
        let (mut input, reader) = tokio::io::duplex(97);
        let large = vec![b'x'; 200_000];
        let mut expected = large.clone();
        expected.extend_from_slice(b"\nnext\n");
        let writer = tokio::spawn(async move {
            input.write_all(&expected).await.unwrap();
        });

        run_from_reader(
            "stdin",
            reader,
            pipeline(io, BatchConfig::new(false)),
            shutdown_rx,
        )
        .await;
        writer.await.unwrap();

        assert_eq!(batch_lines(&rx).concat(), [large, b"next".to_vec()]);
    }

    #[test]
    fn scan_strips_framing_and_counts_rejections() {
        let (io, _rx) = test_io(4, 1024);
        let mut producer = producer(io, BatchConfig::new(false));
        producer.filter = LineFilter::new(
            None,
            Filter::new(Vec::new()).unwrap(),
            Filter::new(vec!["user:".parse().unwrap()]).unwrap(),
            commands::Filter::default(),
        );
        producer.commands = Some(key_fixture::lookup());
        producer.stats = LocalStats::new(u64::MAX);
        let input = b"+1.0 [0 127.0.0.1:1] \"SET\" \"user:1\" \"v\"\r\n\
                      1.0 [0 127.0.0.1:1] \"SET\" \"other\" \"user:2\"\n\
                      +OK\r\n\
                      1.0 [0 127.0.0.1:1] \"GET\" \"user:3\"\n";
        let mut out = Vec::new();

        let scan = producer
            .scan(
                input,
                memchr::memchr(b'\n', input).unwrap(),
                usize::MAX,
                usize::MAX,
                &mut out,
            )
            .unwrap();

        assert_eq!(scan.end, input.len());
        assert_eq!(scan.accepted, 2);
        assert_eq!(producer.stats.total, 4);
        // OK has no keys, so the key filter rejects it.
        assert_eq!(producer.stats.filtered, 2);
        assert_eq!(
            out,
            b"1.0 [0 127.0.0.1:1] \"SET\" \"user:1\" \"v\"\n\
              1.0 [0 127.0.0.1:1] \"GET\" \"user:3\"\n"
        );
    }

    #[test]
    fn standalone_ok_reply_is_skipped_without_filters() {
        let (io, _rx) = test_io(4, 1024);
        let mut producer = producer(io, BatchConfig::new(false));
        let mut out = Vec::new();

        let scan = producer.scan(b"OK\n", 2, 64, 1024, &mut out).unwrap();

        assert_eq!((scan.end, scan.accepted), (3, 0));
        assert!(out.is_empty());
    }

    #[test]
    fn malformed_record_does_not_discard_the_rest_of_its_read() {
        let (io, _rx) = test_io(4, 1024);
        let mut producer = producer(io, BatchConfig::new(false));
        producer.formatter = Arc::new(Formatter::new(OutputKind::Plain, "%l"));
        let input = b"PONG\n1.000000 [0 127.0.0.1:1] \"PING\"\n";
        let mut out = Vec::new();

        let scan = producer.scan(input, 4, 64, 1024, &mut out).unwrap();

        assert_eq!(scan.accepted, 1);
        assert_eq!(out, b"\"PING\"\n");
    }

    #[tokio::test]
    async fn sources_format_records_before_sending() {
        let (io, rx) = test_io(4, 1024);
        let mut pipeline = pipeline(io, BatchConfig::new(false));
        pipeline.formatter = Arc::new(Formatter::new(OutputKind::Json, ""));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        run_from_reader(
            "stdin",
            &b"1.5 [2 lua] \"GET\" \"k\"\n"[..],
            pipeline,
            shutdown_rx,
        )
        .await;

        assert_eq!(
            batch_lines(&rx),
            [[br#"{"timestamp":1.5,"db":2,"addr":"lua","cmd":"GET","args":["k"]}"#]]
        );
    }

    #[tokio::test]
    async fn json_source_identity_survives_merging_and_backpressure() {
        let (io, rx) = test_io(1, 128);
        let mut pipeline = pipeline(io, BatchConfig::new(false));
        pipeline.formatter =
            Arc::new(Formatter::new(OutputKind::JsonSource, ""));
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let mut second_shutdown = shutdown_rx.clone();
        let mut first = Producer::new(
            Source::new(
                &ServerAddr::from_tcp_addr("127.0.0.1", 6379),
                Some("primary".into()),
            ),
            pipeline.clone(),
        );
        let mut second = Producer::new(
            Source::new(&ServerAddr::from_tcp_addr("::1", 6380), None),
            pipeline,
        );
        let input = &b"1.5 [2 lua] \"GET\" \"first\"\ninvalid\n1.5 [2 lua] \"GET\" \"last\""[..];
        let drain = async move {
            let mut records = Vec::new();
            while let Ok(message) = rx.recv_async().await {
                if let IoMessage::Batch(batch) = message {
                    for line in batch.data.split_inclusive(|&b| b == b'\n') {
                        records.push(
                            serde_json::from_slice::<serde_json::Value>(line)
                                .unwrap(),
                        );
                    }
                }
            }
            records
        };
        let ((), (), records) = tokio::join!(
            async move {
                first
                    .consume(BytesMut::new(), input, &mut shutdown_rx, true)
                    .await;
            },
            async move {
                second
                    .consume(BytesMut::new(), input, &mut second_shutdown, true)
                    .await;
            },
            drain,
        );
        assert_eq!(records.len(), 4);
        for (address, name) in
            [("127.0.0.1:6379", Some("primary")), ("[::1]:6380", None)]
        {
            let from_source: Vec<_> = records
                .iter()
                .filter(|record| record["source"]["address"] == address)
                .collect();
            assert_eq!(from_source.len(), 2);
            for (record, arg) in from_source.iter().zip(["first", "last"]) {
                assert_eq!(record["source"]["name"], serde_json::json!(name));
                assert_eq!(record["addr"], "lua");
                assert_eq!(record["args"], serde_json::json!([arg]));
            }
        }
    }

    #[tokio::test]
    async fn command_statistics_are_merged_across_sources() {
        let shared = SharedStats::default();
        let (io, _rx) = test_io(8, 1024);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        for input in [
            &b"1.0 [0 lua] \"GET\" \"k\"\n1.0 [0 lua] \"SET\" \"k\" \"v\"\n"[..],
            b"1.0 [0 lua] \"GET\" \"key\"\nnot a record\n",
        ] {
            let mut pipeline = pipeline(io.clone(), BatchConfig::new(false));
            pipeline.formatter =
                Arc::new(Formatter::new(OutputKind::Plain, "%l"));
            pipeline.stats = Some(Arc::clone(&shared));
            run_from_reader("stdin", input, pipeline, shutdown_rx.clone())
                .await;
        }

        assert_eq!(lock(&shared).to_string(), "GET=[2, 44], SET=[1, 25]");
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
        io.send(IoMessage::Batch(Batch {
            data: b"first".to_vec(),
            records: 1,
            permit: io.reserve_bytes(5).await,
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
            pipeline(
                io,
                BatchConfig {
                    records: 64,
                    bytes: 1024,
                    delay: Duration::from_mins(1),
                },
            ),
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
        assert_eq!(batch_lines(&rx), [[b"one"]]);
    }

    #[tokio::test]
    async fn full_output_queue_awaits_without_dropping_or_recounting() {
        let stats = IoStats::default();
        let (tx, rx) = flume::bounded(1);
        tx.send(IoMessage::Batch(test_batch(b"first\n"))).unwrap();

        let mut blocked = Box::pin(send_io_message(
            &tx,
            IoMessage::Batch(test_batch(b"second\n")),
            &stats,
        ));

        for _ in 0..100 {
            assert!(futures::poll!(blocked.as_mut()).is_pending());
        }
        assert_eq!(stats.snapshot().2, 1);

        let IoMessage::Batch(first) = rx.recv().unwrap() else {
            panic!("expected the first batch");
        };
        assert_eq!(first.data, b"first\n");
        blocked.await.unwrap();
        assert_eq!(batch_lines(&rx), [[b"second"]]);

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
            send_io_message(&tx, IoMessage::Batch(test_batch(b"x")), &stats)
                .await
                .is_err()
        );
        assert_eq!(stats.snapshot().2, 0);
    }

    /// Records writes and flushes.
    #[derive(Default)]
    struct TestOutput {
        data: Vec<u8>,
        flushes: usize,
    }

    impl Write for TestOutput {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.data.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }

    #[test]
    fn output_writes_the_header_once_and_flushes_when_idle() {
        let (tx, rx) = flume::bounded(8);
        for data in [&b"a\n"[..], b"b\n"] {
            tx.send(IoMessage::Batch(test_batch(data))).unwrap();
        }
        tx.send(IoMessage::Shutdown).unwrap();
        let mut out = TestOutput::default();

        write_output(&rx, &mut out, Some(b"header\n"), None).unwrap();

        assert_eq!(out.data, b"header\na\nb\n");
        // Everything was queued together, so only the final flush happens.
        assert_eq!(out.flushes, 1);
    }

    /// Output whose reader has gone away, like stdout piped into `head`.
    struct ClosedOutput;

    impl Write for ClosedOutput {
        fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn output_loop_stops_on_output_failure() {
        let (tx, rx) = flume::bounded(4);
        tx.send(IoMessage::Batch(test_batch(b"a\n"))).unwrap();
        tx.send(IoMessage::Batch(test_batch(b"b\n"))).unwrap();

        let error =
            write_output(&rx, &mut ClosedOutput, None, None).unwrap_err();

        assert!(is_broken_pipe(&error));
        assert_eq!(rx.len(), 1, "the loop must stop at the first failure");
    }

    #[test]
    fn backoff_grows_with_jitter_and_is_capped() {
        let mut backoff = Backoff::new();
        let first = backoff.delay();
        assert!(first >= Backoff::MIN_DELAY * 2, "{first:?}");
        for _ in 0..100 {
            assert!(backoff.delay() <= Backoff::MAX_DELAY);
        }
        backoff.retries = u32::MAX;
        assert_eq!(backoff.delay(), Backoff::MAX_DELAY);
    }

    #[tokio::test]
    async fn shutdown_interrupts_a_stalled_connection_attempt() {
        // Accept the connection but never answer MONITOR.
        let listener =
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
            drop(socket);
        });

        let (io, _rx) = test_io(4, 1024);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let monitor = Monitor::new(
            None,
            ServerAddr::from_tcp_addr("127.0.0.1", port),
            None,
            crate::config::ServerAuth::default(),
        );
        let mut task = Box::pin(run_monitor(
            monitor,
            pipeline(io, BatchConfig::new(false)),
            shutdown_rx,
        ));
        // Drive the task until it is parked in the handshake.
        for _ in 0..10 {
            assert!(futures::poll!(task.as_mut()).is_pending());
            tokio::task::yield_now().await;
        }

        shutdown_tx.send_replace(true);
        assert!(futures::poll!(task.as_mut()).is_ready());
        server.abort();
    }
}
