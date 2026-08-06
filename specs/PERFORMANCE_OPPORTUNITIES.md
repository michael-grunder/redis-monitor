# Performance opportunities

Date: 2026-08-06  
Revision reviewed: `c613143`  
Build profiled: `cargo build --release`

This report ranks opportunities by expected benefit relative to implementation
effort. The percentages below are estimates for the workloads where each change
applies, not measured before/after claims. Each change should be benchmarked in
isolation before it is retained.

## Measurement summary

The replay input, `monitor.log`, contains 69,827 records and 45,376,940 bytes.
Its average line is 650 bytes and its largest line is 3.3 MB, so it covers both
ordinary commands and unusually large payloads. `hyperfine` used at least eight
runs after two warmups, writing output to `/dev/null`:

| Mode | Mean time | Records/s | Input MB/s |
| --- | ---: | ---: | ---: |
| Plain, default format | 109.7 ms | 636k | 414 |
| Plain, `%l` | 105.4 ms | 663k | 431 |
| Plain, reject-all literal filter | 93.7 ms | 745k | 484 |
| RESP | 292.1 ms | 239k | 155 |
| JSON | 379.0 ms | 184k | 120 |

Live tests used Valkey 8.1.0 on loopback and pipelined GET/SET traffic. A
single-source two-million-record `perf` run reported 126,471 failed full-queue
send attempts. The equivalent two-source run reported 417,103. A 21-node
cluster run (`--cluster --replicas`) saturated exactly two `redis-monitor`
threads: the current-thread Tokio executor and the output thread. Sampling was
evenly split between them.

The machine was a dual-socket Intel Xeon Platinum 8160 with 48 physical cores,
Linux 6.1, and Rust 1.97.1. The machine was not isolated or CPU-pinned, so the
wall-clock results should be treated as directional. The `perf` and allocation
profiles were consistent across the single-source, two-source, and cluster
workloads.

## 1. Remove the per-record allocation in the default multi-instance format

**Estimated improvement:** 5-15% more plain-output throughput for the default
multi-instance format, with roughly two allocation calls removed per record.  
**Implementation lift:** Small; approximately half a day including focused
tests and a benchmark.  
**Confidence:** High.

`PlainWriter::w_client_server_short` compares a server host string with
`chost.to_string()` for every `%S` token. Formatting an `Ipv4Addr` into a fresh
`String` is especially costly because `%S` is the default when more than one
instance is monitored.

Heaptrack recorded only 2,133 allocation calls while processing 100,000
single-instance records, but 405,017 calls while processing 200,000 records in
the two-instance default format. It attributed 400,031 calls to growing the
temporary string used to format the IPv4 address. The multi-instance `perf`
profile also showed IPv4 formatting and `String` growth in this path.

**Proposed change:** Normalize the server address once during cold setup. Store
an optional parsed `IpAddr` (or an equivalent precomputed comparison key) next
to the server display metadata, then compare `IpAddr` values directly. Keep the
original host string for display and for hostnames that cannot be normalized.
Also write the selected server/client port values directly without constructing
temporary strings. Add cases for IPv4, IPv6, hostnames, Unix sockets, and
different server/client hosts.

## 2. Await full output queues instead of polling with `try_send` and yielding

**Estimated improvement:** 3-15% more throughput when output is saturated,
with a larger reduction in producer CPU use and scheduler churn. Little change
is expected when the queue never fills.  
**Implementation lift:** Small; approximately half to one day.  
**Confidence:** High that CPU use improves; medium for the throughput estimate.

Both `run_wire` and `run_stdin` retry `flume::Sender::try_send` in a loop and
call `tokio::task::yield_now()` after every full result. This repeatedly polls a
queue whose consumer is a different OS thread. The stall counter therefore
counts failed polls rather than distinct backpressure episodes. Under load, the
main runtime spends useful cycles rescheduling producers that can only add more
pressure to the already full downstream queue.

**Proposed change:** Use flume's existing `send_async` future so a producer is
woken when capacity becomes available. Count the transition into a blocked send
once, and preserve disconnected-channel and cancellation behavior. A focused
slow-writer test should prove that no records are dropped, shutdown completes,
and the producer does not spin. Benchmark both a `/dev/null` sink and a
rate-limited pipe.

## 3. Compile a byte-oriented parse plan for plain output

**Estimated improvement:** 15-30% more plain-output throughput, depending on
the selected format; likely largest for the single-instance default format.  
**Implementation lift:** Medium; approximately two to four days.  
**Confidence:** Medium-high.

Every accepted record is fully converted into `Line`: the timestamp is parsed
to `f64`, the database to `u64`, and the client address to `IpAddr`. Plain output
then formats those values back to text. In the single-source live profile,
float-to-decimal formatting alone accounted for about 7% of cycles. IPv4 and
integer formatting consumed several more percent, while much of the 14.6% in
`IoMessage::process` was parsing. The default output mostly reproduces bytes
already present in the MONITOR line.

**Proposed change:** Have `compile_format` produce a `ParsePlan` describing the
fields actually required. Parse a `LineView` containing validated byte ranges
for timestamp, database, client address, command, and arguments. Plain writers
should copy those original slices when numeric or typed values are not needed;
structured writers can request the current typed conversions. A narrowly
guarded whole-line fast path is reasonable for formats proven byte-equivalent.

This must retain malformed-input validation and the current output contract,
including escaping and spacing. Regression tests should compare every format
token against the existing implementation before the old path is removed.

## 4. Batch records and collapse the two queue handoffs

**Estimated improvement:** 20-40% higher saturation throughput for short
records and many producers, plus substantially fewer queue operations and
reference-count updates.  
**Implementation lift:** Large; approximately three to six days.  
**Confidence:** High that this is a major cost; medium for the gain until a
prototype is measured.

Each wire record currently travels through a Tokio MPSC channel and then a
flume channel before reaching the writer. In the single-source live profile,
self time in flume send/receive, its contended mutex, Tokio MPSC send/receive,
and semaphore operations totaled roughly one third of sampled cycles.
`BytesMut::split_to` plus shared-buffer clone/drop operations consumed another
8%. Every message also clones the server and name `Arc`s.

The bounds are expressed in records (16,384 plus 65,536), not bytes. That gives
predictable item counts but not predictable memory: the replay contains a 3.3
MB record, and a malicious or accidental stream of very large records can make
the nominally bounded queues retain an impractical amount of memory.

**Proposed change:** Let each monitor producer accumulate a small batch bounded
by record count, total bytes, and a short latency deadline. Send source metadata
once per batch, and represent records as ranges into one shared input chunk
where practical. Route batches directly to the output queue unless the central
task has a demonstrated ordering responsibility that cannot move into batch
metadata.

Backpressure should be governed by a global byte budget, using permits acquired
before retaining a chunk and released after output. Keep a smaller item bound
as a secondary guard. Tests must cover per-source ordering, cross-source
behavior, oversized single frames, a slow writer, disconnect, and shutdown.

## 5. Remove avoidable structured-output collections and address strings

**Estimated improvement:** 10-25% for JSON/CSV/PHP and 5-15% for RESP, with
roughly two to four fewer allocations per common record.  
**Implementation lift:** Medium; approximately two to four days in incremental
steps.  
**Confidence:** High for allocation reduction, medium for elapsed-time gain.

Structured output materializes a `Vec<Cow<[u8]>>` in `parse_escaped_args`.
JSON and CSV serialization then build another `Vec<Cow<str>>` in
`serialize_args_as_strings`. `ClientAddr::serialize` also calls
`format!("{ip}:{port}")` for every TCP record, and PHP clones argument bytes
into `ByteBuf`s.

On the replay workload, heaptrack measured:

| Mode | Allocation calls | Calls/record |
| --- | ---: | ---: |
| Plain | 76,998 | 1.10 |
| RESP | 257,929 | 3.69 |
| JSON | 467,365 | 6.69 |

The plain count includes stdin ownership, so the difference is the useful
comparison. Heaptrack identified repeated `RawVec` growth in
`parse_escaped_args` and one or more address-string growth calls per JSON line.

**Proposed change:** Start with low-risk changes: serialize the existing
argument slice through a custom `SerializeSeq` wrapper instead of collecting a
second vector, serialize addresses through a bounded stack buffer or serializer
display adapter, and let PHP borrow unescaped bytes where its serializer API
allows. Then measure an inline argument-descriptor representation (for example,
a small inline vector of byte ranges with owned storage only for escaped
arguments). Preserve the rule that malformed input is rejected before a partial
record is committed to output.

## 6. Remove the two-core ceiling for many-instance workloads

**Estimated improvement:** A 1.5-3x higher processing ceiling with many busy
instances when parsing/filtering is the bottleneck; little gain when the final
sink alone is saturated.  
**Implementation lift:** Large; approximately four to eight days after the
queue and parsing work above.  
**Confidence:** High for the current ceiling, medium for the gain.

The binary uses `#[tokio::main(flavor = "current_thread")]`, so all connections,
framing, early filters, and the central forwarding loop share one core. Parsing,
formatting, and writing share one other core. During the offered 21-node cluster
test, `perf` found only those two busy threads and sampled them evenly even
though the host has 48 physical cores.

**Proposed change:** Do not merely switch runtime flavors and accept more
contention. First apply batching and byte-budgeted backpressure. Then benchmark
a bounded multi-thread runtime or a small number of ingest shards. If the writer
remains dominant, move parse/format work to ordered batch workers and leave the
output thread responsible only for ordered buffered writes. Assign sequence
numbers before parallel formatting and bound the reorder window so a slow batch
cannot cause unbounded memory growth.

Measure with 1, 3, and 21 active sources, accept-most and reject-most filters,
and both plain and JSON output. Preserve per-source ordering and document the
cross-source ordering guarantee.

## 7. Stop cloning every stdin record

**Estimated improvement:** 10-25% more stdin replay throughput and materially
lower allocation pressure/peak retained memory.  
**Implementation lift:** Medium; approximately one to two days, or smaller if
implemented with the batch framing work.  
**Confidence:** High.

`run_from_reader` reuses a `Vec` for reading but calls `buf.clone()` for every
line before constructing `Bytes`. It also uses `remove(0)` for RESP simple-string
input, which shifts the rest of a potentially very large record. Heaptrack
recorded about one allocation per plain replay record and attributed 28.6 MB of
peak consumption to stdin record ownership.

**Proposed change:** Use a chunked `BytesMut` framer like the wire path and emit
byte ranges/batches, or transfer whole buffers through a small recycling pool.
Strip the optional leading `+` by slicing, never shifting the payload. Apply the
same byte-budgeted backpressure used by network producers so replaying the 3.3
MB fixture cannot multiply retained memory unexpectedly.

## 8. Amortize optional statistics work

**Estimated improvement:** 5-15% in `--stats` mode; negligible when statistics
are disabled.  
**Implementation lift:** Small to medium; approximately one to two days.  
**Confidence:** Medium.

With statistics enabled, the central loop scans the command again, hashes it,
updates a `HashMap`, and calls `Instant::elapsed()` for every record. Filtering
and parsing may independently scan the same command bytes. At high record rates,
the time check is unnecessary per-record work.

**Proposed change:** Carry a command byte range discovered during early
filtering/framing so stats and parsing can reuse it. Check the reporting deadline
once per local batch (or every fixed number of records) while ensuring the
maximum report delay remains bounded. Keep command counters local to ingest
shards if parallel ingestion is introduced, then merge only at report time.
Include stats accuracy and interval-boundary tests.

## 9. Make output flush policy byte/time based

**Estimated improvement:** 3-10% at sparse-to-medium rates or when records arrive
one at a time; minimal change during full 1,024-record drains.  
**Implementation lift:** Small to medium; approximately one to two days.  
**Confidence:** Medium-low until tested with a pipe or terminal sink.

The output thread flushes after every receive/drain iteration. At saturation the
drain usually amortizes this over many records, but at lower occupancy it can
flush once per record and turn buffering into repeated write syscalls. The
current behavior does provide good interactive latency, so removing flushes
unconditionally would be a regression.

**Proposed change:** Flush after a byte threshold, a short maximum latency, a
stats message, or shutdown. Select separate defaults for a terminal and a pipe
only if measurement justifies the added policy. Verify partial writes, broken
pipes, timely interactive output, stats visibility, and shutdown flushes with a
deterministic writer test.

## Suggested implementation order

Implement findings 1 and 2 as isolated quick wins. Prototype finding 3 next,
because it reduces work without changing concurrency. Findings 4 and 5 then
address the dominant queue and structured-output costs. Re-profile before
undertaking finding 6: the earlier changes will determine whether ingest,
formatting, or the sink is the remaining scaling limit. Findings 7-9 can be
scheduled by feature priority and can share the batching infrastructure.

For every hot-path change, retain a release-mode replay benchmark plus an
end-to-end multi-producer workload with a slow-consumer case. Report both
records/s and bytes/s, allocation count, peak memory, and backpressure time (not
failed-poll count).
