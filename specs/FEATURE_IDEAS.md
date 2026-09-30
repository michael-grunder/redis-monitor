# Feature ideas for redis-monitor

Code review: 2026-09-30, revision `32eff16`.

These are proposals, not implemented options or performance claims. The review
covered the CLI/configuration, connections, framing, filters, command metadata,
statistics, output, and existing regression tests. The ranking favors useful
extensions to the current pipeline over adding another processing layer.

| Priority | Idea | Main benefit | Relative scope |
| --- | --- | --- | --- |
| 1 | Preserve source identity in structured output | Make multi-server captures attributable | Small–medium |
| 2 | Statistics and source health independent of output | Explain traffic, quiet sources, and bottlenecks | Medium |
| 3 | Saved command metadata for offline analysis | Use key/category filters on captured logs | Medium |
| 4 | Explicit input and buffering limits | Make resource behavior configurable and predictable | Medium |
| 5 | Client and time-window filters | Narrow investigations without extra text-processing tools | Small–medium |
| 6 | Refresh cluster membership while running | Keep monitoring relevant nodes as topology changes | Large |

## 1. Preserve source identity in structured output

**Implemented on 2026-09-30.** `--source` includes server address and instance
name with any output format: JSON/PHP objects, CSV columns, a RESP capture
envelope, or a plain prefix. `--format` selects native structured fields or
interpolates plain text. Omitted formats use direct serializers. This replaces
the initial `json-source` format. Unknown identities have explicit null/empty
representations. See the [output documentation](../README.md#output-and-formatting)
and [release measurements](OUTPUT_FORMAT_MEASUREMENTS.md). The original proposal
below is retained for context.

**Current gap.** Plain output can include `%sa` and `%sn`, but JSON/PHP's
`Structured` record and the CSV writer omit the monitored server. Their `addr`
field identifies the client. Combining two servers into JSON therefore loses
information that is already available to the formatter.

**Proposed first version.** Add an opt-in structured schema containing server
address and instance name, starting with JSON. Keep client address distinct and
define how stdin's unknown server identity is represented. Version the schema
or require an explicit option so existing consumers keep their current fields.
For example, an additional `source` object could contain `address` and `name`.

**Why worthwhile.** Captures become useful for comparing shards and attributing
commands to an instance without reconstructing provenance from stderr. This is
also a foundation for offline analysis of multiple sources.

**Implementation and checks.** Extend `Source`, `Structured`, and
`Formatter::format` in [src/output.rs](src/output.rs). Borrow the source strings
prepared during connection setup. Extend the golden fixtures and add a
two-source test, including unnamed instances and stdin. Measure the JSON
throughput and output-byte increase. RESP should retain its command-array
meaning; a capture envelope would need a separate format.

## 2. Statistics and source health independent of output

**Current gap.** `--stats` is cumulative, command-oriented, and enabled only for
plain output. Reports depend on output drains, so idle input does not produce a
heartbeat. Backpressure reports count wait episodes but do not show how long
producers waited. Connection diagnostics are separate text messages.

**Proposed first version.** Support periodic interval rates and cumulative
totals alongside every output format, with optional per-source breakdowns.
Report records/s, MONITOR bytes/s, accepted/filtered/invalid counts, connection
state, reconnect count, and time spent waiting for output capacity. Emit reports
on stderr, including during idle periods; offer JSON reports for automation.

Distinguish filter rejection from unavailable key discovery, which currently
shares the filtered total. Define counters precisely: current command counters
are updated after formatting, before the writer confirms a successful write.
Call these accepted/formatted counts, or carry batch counts to the writer if
the report promises successful output. Also distinguish MONITOR line bytes
from serialized output bytes.

**Why worthwhile.** A quiet terminal could mean no traffic, restrictive filters,
a disconnected source, or slow output. One report could make that distinction
visible. A later opt-in summary-only mode could show top commands or keys
without printing every record; key tracking would need bounded storage and
explicitly documented approximation if used.

**Implementation and checks.** Build on `CommandStats` in
[src/stats.rs](src/stats.rs), and `SourceStats`, `IoStats`, `StatsReporter`, and
the output loop in [src/pipeline.rs](src/pipeline.rs). Preserve local aggregation;
avoid per-record timers or shared locks. A reporter must remain responsive even
if stdout is blocked. Bound command/group cardinality for arbitrary stdin.
Test idle reports, reconnect transitions, rejection reasons, output failure,
and slow consumers; measure enabled and disabled overhead separately.

## 3. Saved command metadata for offline analysis

**Current gap.** `run_stdin` rejects `--key-filter` and `--flags` because it has
no server metadata. The library already has `Command::from_reply` for offline
metadata parsing, and tests use a saved command fixture.

**Proposed first version.** Add a command to export a server's command metadata
and an option to load that snapshot with stdin. Save a format version and source
provenance with the metadata. Start with one snapshot per input capture; mixed
server versions/modules require an explicit source-to-snapshot mapping later.

**Why worthwhile.** Investigations could rerun key and category filters on an
existing capture without maintaining access to the original server. Saved
metadata would also make bug reports and filter regression cases reproducible.

**Implementation and checks.** Reuse the command parser and lookup in
[src/commands.rs](src/commands.rs), wire snapshot loading into `run_stdin` in
[src/main.rs](src/main.rs), and use the existing
[command fixtures](tests/fixtures/command.json) for tests. Define a portable,
byte-preserving snapshot representation rather than serializing an internal
Rust type without a schema. Bound snapshot size and reject malformed versions.
Preserve rejection when key discovery is unavailable, and explicitly define
unknown-command behavior for flag filters. Stale snapshots cannot prove they
match a capture; retain provenance and make that limitation visible.

## 4. Explicit input and buffering limits

**Current gap.** The output budget is fixed at 64 MiB, while input records have
no size limit. `Producer::consume` keeps growing an incomplete frame until a
newline arrives. The output byte budget therefore cannot bound input memory,
especially with many sources. Oversized formatted records can also exceed it.

**Proposed first version.** Expose an output queue byte budget and an optional
maximum input record size. Keep existing unlimited-record behavior as the
compatibility default initially. When a configured limit is exceeded, fail
with source and limit context by default. An explicit skip policy could discard
through the next newline, count the discarded record, and resume with bounded
memory. Never truncate and emit a command as if it were complete.

**Why worthwhile.** Operators could choose whether unusually large payloads are
acceptable and tune buffering for smaller machines or many simultaneous nodes.

**Implementation and checks.** Enforce the record bound during framing in
[src/pipeline.rs](src/pipeline.rs), before argument decoding or formatting, and
propagate fatal limit errors to a nonzero process exit. Account for handshake
bytes and stdin read-ahead as well as normal socket reads. Document that these
controls still are not a hard RSS limit: source count, allocation capacity, and
serialization expansion matter. Test exact boundaries, never-terminated input,
CRLF, stdin EOF, resynchronization, and many oversized producers against a slow
writer. Retain a release stress workload measuring peak memory and throughput.

## 5. Client and time-window filters

**Current gap.** Filters select database, command/argument patterns, flags, and
keys. The parser already exposes the client and timestamp, but the CLI cannot
select a particular client or an incident's timestamp range directly.

**Proposed first version.** Add client-host/exact-client selectors and inclusive
start/exclusive end timestamps. Keep special client forms (`lua`, Unix paths,
and `-`) explicit. Time ranges should apply to the recorded MONITOR timestamp;
duration-based capture limits would be a separate feature. Combine these
selectors with existing filters using AND.

**Why worthwhile.** This supports questions such as “what did this application
connection send during the incident?” without relying on text matches that
could accidentally match an argument value.

**Implementation and checks.** Extend `Options` and `LineFilter` in
[src/main.rs](src/main.rs) and [src/filter.rs](src/filter.rs), reusing the
byte-oriented prefix parser in [src/monitor.rs](src/monitor.rs). Parse selector
configuration once and reject before decoding arguments where possible. Define
timestamp precision without introducing floating-point boundary surprises.
Do not stop reading at the first timestamp beyond the range: merged captures
need not be globally sorted. Test IPv4/IPv6, special clients, exact timestamp
boundaries, malformed prefixes, and combined filters; benchmark both
accept-most and reject-most cases.

## 6. Refresh cluster membership while running

**Implemented on 2026-09-30.** Membership now refreshes by default every 30
seconds, configurable with `--cluster-refresh`, using asynchronous discovery
and draining reconciliation. `--replicas` applies to named clusters as well.
The original proposal below is retained for context; startup CLI seeds still
must each be reachable, while periodic discovery uses candidate fallback. See
the [current behavior](../README.md#connections-and-named-instances).

**Current gap.** Discovery creates a fixed list of monitors at startup.
`run_monitor` reconnects to its original address, but there is no supervisor
reconciling that list with later cluster membership or role changes. Named
cluster entries and CLI cluster mode also differ in seed fallback and replica
selection.

**Proposed first version.** Offer an opt-in discovery interval. Reconcile node
IDs, addresses, and desired roles; start newly selected nodes and drain monitors
that are no longer selected. Reuse healthy nodes as discovery candidates and
retain current monitors if a refresh fails. Apply consistent primary/replica
selection and seed fallback to named and CLI clusters.

**Why worthwhile.** Long-running sessions could follow newly added primaries or
address changes without a manual restart.

**Implementation and checks.** Add supervision around the `JoinSet` in
[src/main.rs](src/main.rs) and reuse `Cluster` in
[src/connection.rs](src/connection.rs). Recurring discovery must use async I/O
or a bounded blocking task; startup's blocking discovery cannot simply move
onto a periodic Tokio worker loop. Bound discovery time and connection count,
deduplicate active nodes, and expose transitions through the health reports.
Test promotion, removal, address changes, failed seeds, repeated discovery,
and shutdown during reconciliation. Report coverage gaps; reconnecting cannot
recover the stream missed while disconnected. Preserve per-source order and
avoid promising global timestamp order or exactly-once coverage.

## Suggested starting point

Start with source identity and offline metadata: both expose information or
capabilities the code already largely has. Develop statistics next to make
later operational work easier to assess. Prioritize configurable record limits
sooner if oversized captures or constrained deployments are common. Cluster
refresh is valuable but warrants its own lifecycle design and test harness.

For implementation, retain the repository's correctness checks and add portable
release before/after measurements wherever the record path changes. Include
multiple producers, large arguments, selective filters, structured output, and
a slow sink. The existing [performance review](specs/PERFORMANCE_OPPORTUNITIES.md)
documents earlier optimizations; its historical estimates are not evidence for
the cost or benefit of these proposals.
