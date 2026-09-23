# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## Unreleased

### Changed

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
  `--batch`. By default, hand off each accepted record individually for minimum
  output latency. This replaces `--no-batch` and applies to both stdin and live
  connections; per-source order is preserved, with no cross-source ordering
  guarantee.
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

- Add regression tests for the MONITOR handshake, CSV/PHP/JSON output, module
  command names, `--db`, `--flags`, `--stats` validation, address parsing,
  output failure, and cancelling a stalled connection.

### Documentation

- Expand the agent guide with firehose-oriented performance requirements,
  measurement practices, Rust abstraction tradeoffs, and completion checks.
- Add a measured, ranked report of potential throughput and resource-use
  improvements.

### Fixed

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
