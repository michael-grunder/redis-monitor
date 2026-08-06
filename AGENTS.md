# reditop Agent Guide

## Mission and Priorities

`redis-monitor` consumes, filters, and renders high-volume Redis `MONITOR`
streams, often from many instances at once. Treat sustained throughput and
predictable resource use as core product requirements, not optional polish.

Changes should preserve all of the following:

1. Correct parsing, filtering, ordering, and output.
2. High aggregate throughput under bursty, multi-instance workloads.
3. Bounded memory use and explicit backpressure behavior.
4. Robust operation in the presence of malformed input, slow output, network
   failures, and shutdown.
5. Clear, idiomatic, maintainable Rust.

Do not trade correctness for speed. Do not add complexity for a speculative
optimization: measure first, make the smallest effective change, and retain a
benchmark or regression test when practical.

## Working in This Repository

Before changing code, identify where the change sits in the data path:

`network/stdin -> framing -> early filtering -> parsing -> stats -> formatting -> output`

Keep hot per-record work separate from cold configuration, connection setup,
and reporting paths. Read the relevant modules and tests before introducing a
new abstraction. Prefer focused changes, preserve unrelated user work, and do
not weaken tests or lints to make a change pass.

## Performance Engineering

### Workload Model

Optimize for a continuous firehose from many independent Redis instances, not
only a single connection or a small fixture. Representative performance tests
should include:

- Multiple concurrent producers.
- Both short common commands and large/complex argument payloads.
- Filters that reject most records as well as filters that accept most records.
- Plain and structured output modes where relevant.
- A slow or saturated output consumer to exercise backpressure.

The important metrics are records/second and bytes/second, followed by CPU
usage, allocation rate, resident/peak memory, tail latency, queue stalls, and
whether any records are dropped. Record the workload, build profile, hardware,
and before/after results so measurements are reproducible.

### Hot-Path Rules

- Keep incoming data byte-oriented. Do not validate or allocate UTF-8 strings
  unless the selected behavior actually requires them.
- Borrow slices from input buffers where lifetimes permit. Avoid per-record
  `String`, `Vec`, `format!`, `to_string`, `clone`, and reference-count churn in
  the ingest, filter, parse, and output loops.
- Reuse buffers and reserve capacity when a measured size distribution supports
  it. Do not retain pathological peak-sized buffers indefinitely without a
  deliberate bound or shrink strategy.
- Perform cheap rejection before expensive parsing, command lookup, argument
  decoding, serialization, or formatting. Compile filters and other reusable
  state once, outside the record loop.
- Batch and buffer reads, channel drains, serialization, and writes where doing
  so improves throughput without breaking responsiveness, ordering, flush, or
  shutdown semantics.
- Avoid a syscall, lock acquisition, task spawn, timer lookup, or log message per
  record. Aggregate or rate-limit repeated diagnostics while retaining enough
  information to diagnose failures.
- Keep blocking work off Tokio worker threads. If work is moved to a dedicated
  thread or blocking pool, account for transfer, scheduling, and queue costs.
- Keep channel bounds and full/disconnected behavior explicit. Never replace
  backpressure with silent data loss. Any intentional dropping policy must be a
  documented user-facing feature with counters and tests.
- Use the weakest correct atomic ordering, but document non-obvious concurrency
  invariants. Avoid shared atomics and mutexes in the per-record path when local
  aggregation can reduce contention.
- Treat integer overflow, partial writes, truncated frames, cancellation, and
  shutdown as normal design cases. Counters may be relaxed for telemetry only
  when exact synchronization is not part of correctness.

### Measurement Rules

- Evaluate runtime performance using an optimized build. Debug-build timings
  are not evidence for production performance.
- Profile before optimizing so work targets an observed hot path. Use
  wall-clock benchmarks plus an appropriate profiler or allocation tool when
  the cause is uncertain.
- Use microbenchmarks for parsers, filters, and serializers, and an end-to-end
  replay/stress workload for pipeline, scheduling, buffering, and output
  changes. Neither substitutes for the other.
- Compare against a stable baseline over enough samples to distinguish a real
  improvement from noise. Report regressions honestly, including memory or
  binary-size costs.
- A change expected to affect a hot path must include before/after evidence. Add
  or update a benchmark/replay fixture when that is the practical way to keep
  the result from regressing.
- CPU-specific tuning such as `target-cpu=native` may be used for local
  investigation, but portable release behavior remains the baseline unless the
  project explicitly changes its supported deployment targets.
- Do not add `#[inline(always)]`, unsafe code, a custom allocator, or a custom
  data structure based on intuition alone. Require measurement and document the
  reason and tradeoffs.

## Idiomatic Rust and Abstraction Choices

- Model invariants with types, enums, newtypes, ownership, and lifetimes so
  invalid states are difficult to represent.
- Prefer borrowing and explicit ownership transfer over defensive cloning.
- Use traits to express meaningful shared behavior and subsystem boundaries,
  not merely to hide one concrete implementation.
- Use generics and static dispatch where they improve reuse, type safety, or a
  measured hot path. Keep generic surfaces narrow: move substantial
  type-independent work into non-generic functions to limit monomorphization.
- Account for the compile-time and binary-size cost of each concrete generic
  instantiation. When changing widely instantiated code, inspect build time and
  binary size as well as runtime speed.
- Prefer an enum for a small closed set of variants. Prefer `dyn Trait` at a
  genuinely extensible or cold boundary, or where reduced code size outweighs
  dispatch cost. Base hot-path static-versus-dynamic dispatch decisions on
  measurement.
- Prefer iterator and zero-cost abstractions when they remain clear and compile
  efficiently. Watch for hidden allocation from `collect`, boxing, formatting,
  trait objects, and convenience conversions.
- Use concrete, well-scoped types internally. Avoid premature public
  generalization and complicated bounds that provide no current value.
- Prefer exhaustive matches and explicit state transitions over scattered
  boolean special cases.
- Keep `unsafe` blocks small and exceptional. State each safety invariant next
  to the block, provide a safe interface, and add tests that exercise boundary
  conditions. An optimization is not justified unless profiling shows the safe
  implementation matters.

## Error Handling and Resilience

- Do not ignore fallible return values. Propagate errors with useful context or
  handle them deliberately and document why continuation is safe.
- Do not panic on malformed network/stdin data or ordinary operational
  failures. Reserve `expect` and unreachable states for invariants established
  locally, and explain invariants that are not obvious.
- Preserve root causes when crossing task, thread, parsing, or I/O boundaries.
- Ensure reconnect, cancellation, channel closure, writer failure, and shutdown
  paths release resources and do not deadlock or spin.
- Avoid unbounded queues, collections, retry loops, and diagnostic output.
  Backoff and retry policies must have intentional reset and shutdown behavior.

## Dependencies

Prefer a mature, maintained crate over a custom implementation when it meets the
correctness and performance requirements. Before adding a dependency:

- Check whether an existing dependency or the standard library already solves
  the problem.
- Evaluate maintenance, license, unsafe usage, transitive dependency cost,
  enabled features, compile time, binary size, and hot-path behavior.
- Disable unnecessary default features and keep feature selection explicit.
- Do not replace a proven specialized crate with bespoke code without benchmark
  and maintenance evidence.

## Tests and Documentation

- Add or update tests whenever behavior changes. Cover success, rejection,
  malformed/truncated input, boundary sizes, and relevant concurrency or
  backpressure behavior.
- Add regression tests for every bug fix. Prefer deterministic tests; avoid
  arbitrary sleeps and timing thresholds unless there is no reliable
  alternative.
- Parser and framing changes should include adversarial inputs and should be
  suitable for fuzz/property testing. Keep discovered failures as regression
  fixtures.
- Performance-sensitive changes should retain a benchmark or replay workload
  when practical, but performance tests must not replace correctness tests.
- Update `README.md` when user-facing behavior, options, output, build steps, or
  documented workflows change.
- Update `CHANGELOG.md` under `## Unreleased` for repository changes, using the
  Keep a Changelog categories below. Documentation-only changes do not require
  new behavior tests, but they still require the verification commands.

## Required Before Completion

These checks are mandatory. Run them from the repository root after the final
edit:

```text
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

If formatting fails, run `cargo fmt --all`, then repeat all three checks. Address
reported issues rather than suppressing them without a documented reason. If a
required command cannot run, do not claim successful completion; report the
command and blocker explicitly.

Also complete the applicable test, benchmark, README, and changelog work above.

## Changelog Format

Use the standard [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
format. Group unreleased entries under one or more of:

- `### Added`
- `### Changed`
- `### Fixed`
- `### Deprecated`
- `### Removed`
- `### Tests/CI`
- `### Documentation`
