# Performance opportunities (historical review)

Date: 2026-08-06
Revision reviewed: `c613143`
Build profiled: `cargo build --release`

## Status reviewed on 2026-09-29

The measurements and estimates below describe revision `c613143`, not the
current implementation. A source review at `6944473` found:

| Finding | Current status |
| --- | --- |
| 1. Structured-output allocations | Partly implemented: JSON streams argument strings, CSV writes fields directly, addresses use serializer display formatting, and PHP borrows argument bytes. The parsed argument vector and escaped-argument allocations remain. |
| 2. Many-instance CPU ceiling | Still applicable as an architectural concern: one current-thread Tokio runtime handles ingest and one output thread parses, formats, and writes. Producers now send directly to the writer; the central forwarding loop is gone. Batching and byte-budgeted backpressure are implemented. Re-profile before estimating a current scaling limit. |
| 3. Stdin record cloning | Implemented: stdin and network readers share `BytesMut` framing, transfer chunks with record ranges, and strip `+` by slicing. There is no per-line `buf.clone()` or `remove(0)`. |
| 4. Statistics overhead | Partly implemented: the output thread checks the interval once per drain. Command rescanning and per-command hash-map updates remain. |
| 5. Flush policy | Still open: output flushes after each drain (now up to 16 queue messages, each potentially containing many records) and at shutdown. |

The shared 64 MiB budget accounts for queued and producer-held batch data; it is
not an RSS limit. Input framing buffers and oversized records can exceed it.
Backpressure counters now count capacity-wait episodes, so they cannot be
compared directly with the failed-send counts in the original profile.

No new timings were collected for this documentation review. `monitor.log` is a
local, untracked input, so the original replay cannot be reproduced from a clean
checkout alone. The current repository enables `target-cpu=native` by default;
use `RUSTFLAGS='' cargo build --release` for a portable target baseline, and
record the flags used for any comparison. See the
[README](../README.md#recorded-performance-measurements) for later measurements
and available benchmark commands.

## Original findings

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

## 1. Remove avoidable structured-output collections and address strings

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

| Mode  | Allocation calls | Calls/record |
| ---   |             ---: |         ---: |
| Plain |           76,998 |         1.10 |
| RESP  |          257,929 |         3.69 |
| JSON  |          467,365 |         6.69 |

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

## 2. Remove the two-core ceiling for many-instance workloads

**Estimated improvement:** A 1.5-3x higher processing ceiling with many busy
instances when parsing/filtering is the bottleneck; little gain when the final
sink alone is saturated.
**Implementation lift:** Large; approximately four to eight days after the
queue and parsing work above.
**Confidence:** High for the current ceiling, medium for the gain.

At the reviewed revision, the binary used a current-thread Tokio runtime, so
all connections, framing, early filters, and the central forwarding loop shared
one core. Parsing, formatting, and writing shared one other core. During the
offered 21-node cluster test, `perf` found only those two busy threads and sampled them evenly even
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

## 3. Stop cloning every stdin record

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

## 4. Amortize optional statistics work

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

## 5. Make output flush policy byte/time based

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

## Next measurements

Re-profile the current pipeline before ranking the remaining work. Finding 3
and the prerequisites for finding 2 have been implemented. Measure the remaining
argument allocations in finding 1, many-source scaling in finding 2, and the
statistics/flush costs in findings 4 and 5 independently. The original estimates
are not expected gains for today's implementation.

For every hot-path change, retain a release-mode replay benchmark plus an
end-to-end multi-producer workload with a slow-consumer case. Report both
records/s and bytes/s, allocation count, peak memory, and backpressure time (not
failed-poll count).
