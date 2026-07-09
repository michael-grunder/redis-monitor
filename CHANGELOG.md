# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## Unreleased

### Fixed

- Ignore standalone `OK` replies from Redis MONITOR setup instead of reporting
  them as parse errors.
- Preserve quoted JSON-like, serialized PHP, and literal-backslash Redis MONITOR arguments when serializing structured outputs.
