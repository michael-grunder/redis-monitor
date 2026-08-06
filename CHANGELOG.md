# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## Unreleased

### Changed

- Elide per-record address allocations in plain multi-source output by
  normalizing server IP addresses once and writing address components directly.
- Await capacity on saturated output queues instead of polling, and count each
  discrete backpressure episode once.

### Documentation

- Expand the agent guide with firehose-oriented performance requirements,
  measurement practices, Rust abstraction tradeoffs, and completion checks.
- Add a measured, ranked report of potential throughput and resource-use
  improvements.

### Fixed

- Ignore standalone `OK` replies from Redis MONITOR setup instead of reporting
  them as parse errors.
- Preserve quoted JSON-like, serialized PHP, and literal-backslash Redis MONITOR arguments when serializing structured outputs.
