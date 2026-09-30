# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## Unreleased

### Added

- Refresh cluster membership every 30 seconds, configurable with
  `--cluster-refresh SECONDS`, for CLI and named clusters. Preserve unchanged
  connections, reconcile node/role/address changes, and retain the last topology
  if discovery fails. Drain retiring monitors before starting additions so slow
  output cannot accumulate generations of blocked tasks.
- Add `--threads N` to set the number of worker threads that read, filter, and
  format records (default: available CPUs, at most 16).
- Expose allocation-free MONITOR record parsing as `redis_monitor::monitor`
  (`Record::parse` and `Record::decode_args`).
- Report the number of invalid records in the final summary, and document `%%`
  (a literal percent sign) in the CLI help.

- Add `[N]pattern` selectors to both filters: `--filter` indexes command
  arguments (command at 0), while `--key-filter` indexes only discovered keys
  (first key at 0). Add `=literal` to match syntax characters literally, with
  optional leading `!` exclusions. Missing positions match nothing, and malformed
  selectors are rejected. Positional argument filters also work with stdin.

- Add repeatable `--key-filter` literal/regex patterns with `!` exclusions,
  matching decoded key arguments only. Exclusions veto the whole command;
  unavailable key discovery rejects records. Require live `COMMAND` metadata,
  retry metadata failures with backoff, and refresh metadata on reconnect.

- Add allocation-free, binary-safe command key discovery from Redis/Valkey
  `COMMAND` metadata, with subcommand resolution, legacy fixed-key support,
  range/count/keyword key specs, and local SORT/MIGRATE/GEORADIUS handling.
  Unsupported or malformed key discovery returns explicit errors. This exposes
  a library API used by key-scoped filtering.

### Changed

- Use cancellable asynchronous cluster discovery with a 10-second timeout per
  candidate, trying known members before original seeds during refresh. Apply
  `--replicas` to named clusters too, and use the multi-source plain format for
  clusters even when initially monitoring one node.
- Format records in each source's task on a multi-threaded runtime instead of
  on the single output thread, which now only writes finished bytes. Throughput
  with many sources now scales with cores: 8 fast sources went from 3.3 to 30.7
  million records/s (plain) and from 1.6 to 15.9 million records/s (JSON).
  Per-source order is preserved; the byte budget and `--batch` limits now apply
  to formatted output.
- Replace the two nom-based MONITOR parsers with a single byte-oriented
  scanner, cutting prefix parsing from 104 to 77 ns per short record, and decode
  arguments in linear time. JSON and RESP replay of a 45 MB capture are about
  2x and 3x faster.
- Flush output whenever the output queue runs empty, rather than after every
  drain of up to 16 messages.
- Read stdin on a dedicated thread a few chunks ahead of formatting.
- Report at most 10 invalid records per second, with a bounded excerpt of each
  and a count of suppressed messages, and stop warning once per record when a
  command filter sees a line without a command.
- Print `--stats` entries sorted by command name, counting only records that
  were written.
- Load and validate TLS files and the system trust store once at startup
  instead of on every connection attempt.
- Look up commands for `--flags` and `--key-filter` directly from record bytes
  without UTF-8 validation, and list flag and category names in a fixed order
  without duplicate aliases in `--debug` output.
- Write CSV directly with RFC 4180 quoting, and remove the unused `arcstr`,
  `colored`, `csv`, `nom`, and `url` dependencies. `futures` is now a
  development-only dependency.
- Split the pipeline (framing, batching, backpressure, and the output thread)
  out of `main.rs` into its own module.

- Without `--batch`, hand off every complete record from each read together
  instead of one message per record. No record waits for more input, so latency
  is unchanged, but queue and wakeup overhead drop sharply: replaying 2 million
  short records is about 8x faster (2.57 s to 0.31 s), and replaying large
  payloads is about 3x faster.
- Read up to 64 KiB per socket or stdin read and remove the redundant
  `BufReader` layer in front of the framing buffer.
- Serialize JSON/CSV arguments and client addresses without per-record
  intermediate collections or strings, and serialize PHP arguments without
  copying them.
- Check the `--stats` reporting interval once per output drain instead of once
  per record.
- Reject unknown `--flags` names, and reject `--flags` with `--stdin`, instead
  of silently ignoring them.
- A second Ctrl-C now exits immediately.

- Batch records per producer and route them directly to the output thread,
  reducing queue contention and repeated source-metadata updates. Batching is
  bounded by record count, bytes, and a 5 ms deadline, and is opt-in with
  `--batch`. By default, hand off complete records already available from each
  read together, without waiting for more input. This replaces `--no-batch` and
  applies to both stdin and live connections; per-source order is preserved,
  with no cross-source ordering guarantee.
- Bound queued record data with a shared 64 MiB byte budget while retaining a
  smaller item-count guard and lossless backpressure, including support for
  oversized individual records.
- Elide per-record address allocations in plain multi-source output by
  normalizing server IP addresses once and writing address components directly.
- Await capacity on saturated output queues instead of polling, and count each
  discrete backpressure episode once.
- Compile plain-output formats into byte-oriented parse plans so common fields
  can be validated and copied from the input without typed round trips.

### Tests/CI

- Cover cluster refresh failures, promotion, address changes, repeated remaps,
  seed fallback/deduplication, named cluster settings, malformed topology, and
  cancellation during initial and periodic discovery. Add a finite multi-source
  release replay workload in `benches/cluster_refresh.py`.
- Add byte-for-byte golden tests of every output kind and several plain formats
  over an adversarial record corpus, including one-byte-at-a-time input.
- Add parser tests for every client form, numeric bounds, error offsets,
  Redis-escaped round trips of all byte values, unescaped quote payloads, and
  truncated or arbitrary input; add a parser microbenchmark
  (`cargo bench --bench parse`).
- Cover framing across many small reads, bytes received with the MONITOR reply,
  formatting in sources, statistics merging, idle flushing, CSV quoting, TLS
  certificate/key pairing, and backoff limits.

- Cover positional filters across command arguments, MSET/stream/subcommand keys,
  escaping, binary data, missing positions, combined exclusions, and stdin CLI
  output. Extend the release microbenchmark for selectors.

- Cover key-filter combinations, decoded/binary keys, unsupported commands,
  keyless commands, rejection counts, and stdin validation. Add a release filter
  microbenchmark.

- Add command metadata fixtures, adversarial key-spec tests, an optional live
  `COMMAND GETKEYS` comparison in RESP2/RESP3, and a release extraction benchmark.

- Add regression tests for the MONITOR handshake, CSV/PHP/JSON output, module
  command names, `--db`, `--flags`, `--stats` validation, address parsing,
  output failure, and cancelling a stalled connection.

### Documentation

- Add `FEATURE_IDEAS.md` with six code-grounded feature proposals, scoped first
  versions, implementation pointers, and validation considerations.
- Document the parallel pipeline, `--threads`, flushing, invalid-record
  reporting, TLS behavior, format token details, the parsing library API, the
  golden tests, and new performance measurements.

- Remove README instructions for an untracked local benchmark helper while
  retaining historical measurements and the Rust microbenchmark command.
- Refresh the README against the current CLI and implementation: document
  config discovery and named instances, TLS, stdin, output schemas, metadata
  failure behavior, statistics, batching limits, and build CPU settings.
  Clarify inactive formatting/color settings;
  label historical measurements and update the performance report's status.
- Correct the instance-name format token to `%sn` in the README and CLI help,
  and make the CLI's GEO regex example match uppercase command names.
- Correct the agent guide's project name (also exposed through `CLAUDE.md`).
- Expand the agent guide with firehose-oriented performance requirements,
  measurement practices, Rust abstraction tradeoffs, and completion checks.
- Add a measured, ranked report of potential throughput and resource-use
  improvements.

### Fixed

- Render `%%` as a single `%` in `--format`; previously it printed `%%`.
- Stop appending a trailing space to `%l` for commands without arguments.
- Print `%t` from the timestamp text instead of rounding through a float, which
  changed timestamps with more than about 16 significant digits.
- Bracket IPv6 client addresses (`[::1]:6379`) in `%ca`, `%S`, and structured
  output, so the port separator is unambiguous.
- Write `%a` argument bytes unchanged, like `%l`, instead of replacing invalid
  UTF-8.
- Reject timestamps whose parts exceed 64 bits in structured output, as plain
  output already did.
- Decode arguments in linear time; payloads with many quotes or backslashes were
  rescanned for every literal run.
- Search each partial record for a newline once rather than on every read; a
  record spanning many reads was rescanned from its start each time.
- Use `--tls-ca`, `--tls-cert`, and `--tls-key` for cluster discovery and
  `COMMAND` metadata connections, which previously used only the system trust
  store and no client certificate.
- Reject `--tls-cert` without `--tls-key` (and vice versa) instead of silently
  connecting without a client certificate, and present a configured client
  certificate with `--insecure`.
- Deduplicate cluster nodes by address alone; monitors previously hashed
  their credentials but compared only addresses, violating `Hash`/`Eq`
  consistency.
- Saturate the reconnect attempt counter instead of overflowing it.

- Preserve both regex and literal patterns with identical text when deduplicating
  filters; they have different matching behavior.

- Parse RESP3 command key-spec maps and metadata sets, retain nested subcommand metadata, and
  preserve unrecognized specs instead of silently discarding them.

- Keep MONITOR records that the server sends in the same read as the `+OK`
  reply; these were previously discarded with the handshake buffer.
- Fix CSV output, which failed on every record.
- Parse module commands whose names contain punctuation, such as `FT.SEARCH`
  and `JSON.SET`, instead of reporting them as malformed.
- Implement `--db`, which was accepted but ignored.
- Count the first occurrence of each command in `--stats` output.
- Stop all sources and exit cleanly when stdout closes (for example when piping
  into `head`) instead of hanging on idle sources or reporting a broken pipe
  error; other output failures still exit nonzero.
- Make connection attempts cancellable with Ctrl-C and time them out after 10
  seconds.
- Retry loading `COMMAND` metadata for `--flags` after reconnecting instead of
  disabling flag filtering permanently when a server is down at startup.
- Use the configured credentials and TLS mode for cluster discovery and
  `COMMAND` metadata connections, and apply TLS settings to non-cluster config
  file entries.
- Parse and display IPv6 instance addresses (`[::1]:6379`).
- Reject non-finite or out-of-range `--stats` intervals instead of panicking.
- Parse structured-output timestamps with correct rounding and without a debug
  assertion that malformed input could trigger.
- Build without `git` installed or outside a git checkout.

- Report invalid instance arguments, config discovery and validation failures,
  TLS certificate errors, and named or direct Redis Cluster discovery failures
  with contextual nonzero exits instead of panicking.
- Reject malformed `CLUSTER SLOTS` node data, including incomplete nodes and
  out-of-range ports, without panicking.
- Propagate output-thread failures to the process exit status.
- Ignore standalone `OK` replies from Redis MONITOR setup instead of reporting
  them as parse errors.
- Preserve quoted JSON-like, serialized PHP, and literal-backslash Redis MONITOR arguments when serializing structured outputs.
