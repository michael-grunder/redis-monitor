# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## Unreleased

### Changed

- Batch records per producer and route them directly to the output thread,
  reducing queue contention and repeated source-metadata updates. Batching is
  bounded by record count, bytes, and a 5 ms deadline, and can be disabled with
  `--no-batch` for minimum output latency.
- Bound queued record data with a shared 64 MiB byte budget while retaining a
  smaller item-count guard and lossless backpressure, including support for
  oversized individual records.
- Elide per-record address allocations in plain multi-source output by
  normalizing server IP addresses once and writing address components directly.
- Await capacity on saturated output queues instead of polling, and count each
  discrete backpressure episode once.
- Compile plain-output formats into byte-oriented parse plans so common fields
  can be validated and copied from the input without typed round trips.

### Documentation

- Expand the agent guide with firehose-oriented performance requirements,
  measurement practices, Rust abstraction tradeoffs, and completion checks.
- Add a measured, ranked report of potential throughput and resource-use
  improvements.

### Fixed

- Report invalid instance arguments, config discovery and validation failures,
  TLS certificate errors, and named or direct Redis Cluster discovery failures
  with contextual nonzero exits instead of panicking.
- Reject malformed `CLUSTER SLOTS` node data, including incomplete nodes and
  out-of-range ports, without panicking.
- Propagate output-thread failures to the process exit status.
- Ignore standalone `OK` replies from Redis MONITOR setup instead of reporting
  them as parse errors.
- Preserve quoted JSON-like, serialized PHP, and literal-backslash Redis MONITOR arguments when serializing structured outputs.
