# Client and recorded-time filters: release measurements

Measured 2026-09-30 against baseline `96b0a1b`. Both binaries use
`RUSTFLAGS='' cargo build --release`: portable CPU settings, opt-level 3, thin
LTO, one codegen unit. The repository's native-CPU setting was overridden.
Host: Linux x86-64, dual Xeon Platinum 8160, 96 logical CPUs, rustc 1.98.1.
Runs were not CPU-pinned and the host was not isolated. Figures are observations,
not throughput guarantees or evidence for a speculative optimization.

The change is in early filtering, before metadata lookup and argument decoding.
New selectors compile during setup; record checks borrow the prefix, compare
exact decimal digits, and evaluate compiled cron fields. They do not allocate
argument buffers, read wall clocks, search for future cron occurrences, or
modify queue/backpressure policy. Accepted records still pass through the
formatter's parser. No attempt was made to optimize that additional parse.

## Four-source TCP replay

`benches/source_output.py` starts four local mock MONITOR servers together and
runs four worker threads. Each block contains 1,024 SET/GET records with escaped
quotes in their values. Short runs use 512 blocks per source (2,097,156 input
records including four final markers). Large runs add 4,096 payload bytes and
use 32 blocks per source (131,076 records). The Python sink drains and counts
stdout. Every run verifies output count, processed/filtered counts, zero invalid
records, and a successful shutdown. No count discrepancies occurred.

Baseline/candidate runs alternate. Tables give medians of three samples, except
large JSON with filters disabled, which was extended to nine samples per binary
because its wall-clock measurements varied considerably. MB/s uses decimal MB;
CPU is child user+system seconds, excluding the Python producer/sink. RSS is
child peak RSS in MiB, reported by `getrusage`.

### New filters disabled

| Workload | Before M records/s | After M records/s | Before/after input MB/s | Before/after CPU s | Before/after peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Short, plain | 7.99 | 7.83 | 407 / 399 | 0.394 / 0.397 | 67.4 / 69.4 |
| Short, JSON | 4.37 | 4.30 | 223 / 219 | 1.063 / 1.084 | 56.4 / 53.0 |
| Large, JSON | 0.117 | 0.100 | 486 / 415 | 1.124 / 0.983 | 85.8 / 88.3 |
| Slow sink, plain, 32 blocks/source | 0.908 | 0.909 | 46.3 / 46.3 | 0.036 / 0.038 | 20.9 / 20.9 |

Short disabled cases were about 1.5–2% slower in these samples. Large JSON
showed a **14.6% wall-throughput regression** despite using about 12.5% less CPU.
Its elapsed samples spanned 0.989–1.233 s before and 1.020–1.350 s after. This
must not be presented as unchanged performance.

A separate five-sample stdin diagnostic replayed 131,072 identical 4 KiB SET
records from a cached file to `/dev/null`. Median JSON elapsed time was
0.672 s before and 0.528 s after; plain was 0.125 s before and 0.166 s after
(plain ranges overlapped: 0.121–0.215 s and 0.130–0.205 s). Thus the large JSON
socket result does not show a general JSON processing slowdown. The conflicting
CPU/stdin/socket results suggest scheduling or producer/sink interaction, but
this is an inference, not a diagnosed cause. No code change was made to chase it.
Allocation counts and per-record tail latency were not measured.

### New filters enabled

`--selector` varies clients or timestamps independently of argument contents.
Accept-most keeps 15/16 records; `--reject` keeps 1/16. Timestamps alternate
between 1.5 and 600.5, exercising out-of-order input as well as recurring
minute membership. Final markers always satisfy the chosen selector.

| Selector, short plain | Accept-most M records/s | Reject-most M records/s | Accept/reject input MB/s | Accept/reject CPU s | Accept/reject peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Exact client | 7.73 | 17.92 | 394 / 914 | 0.569 / 0.299 | 38.2 / 20.9 |
| Client host | 7.73 | 18.35 | 394 / 936 | 0.550 / 0.294 | 38.0 / 20.9 |
| Time range | 7.07 | 15.96 | 374 / 844 | 0.615 / 0.330 | 42.6 / 21.0 |
| Cron window, UTC | 6.54 | 12.71 | 346 / 672 | 0.757 / 0.423 | 26.4 / 21.0 |

| Cron workload | Accept-most M records/s | Reject-most M records/s | Accept/reject input MB/s | Accept/reject CPU s | Accept/reject peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Short, JSON | 3.75 | 10.98 | 198 / 581 | 1.419 / 0.479 | 26.4 / 20.9 |
| Large, JSON | 0.114 | 0.624 | 472 / 2588 | 0.975 / 0.211 | 92.9 / 63.9 |
| Short, plain, slow sink | 0.950 | 11.41 | 50.3 / 603 | 0.850 / 0.422 | 86.2 / 20.9 |

The slow sink sleeps 1 ms after each read of up to 64 KiB. The enabled run uses
512 blocks/source, exceeding the output queue's 64 MiB budget in total accepted
output. It completes with correct counts and shutdown under backpressure.
Enabled/disabled cases emit different amounts of output; faster rejection is
not evidence that the filter itself is free. RSS includes input, output, runtime,
and allocator storage; the output budget is not a process memory limit.

Reproduce representative runs (loopback sockets must be permitted):

```sh
RUSTFLAGS='' cargo build --release
python3 benches/source_output.py target/release/redis-monitor --output plain --blocks 512
python3 benches/source_output.py target/release/redis-monitor --output json --payload 4096 --blocks 32
python3 benches/source_output.py target/release/redis-monitor --selector client --blocks 512 --output plain
python3 benches/source_output.py target/release/redis-monitor --selector range --reject --blocks 512 --output plain
python3 benches/source_output.py target/release/redis-monitor --selector cron --blocks 512 --output json
python3 benches/source_output.py target/release/redis-monitor --selector cron --reject --payload 4096 --blocks 32 --output json
python3 benches/source_output.py target/release/redis-monitor --selector cron --blocks 512 --output plain --slow-ms 1
```

## Filter microbenchmark

`RUSTFLAGS='' cargo test --release --bin redis-monitor benchmark_prefix_filter
-- --ignored --nocapture` retains seven samples of 300,000 calls, using GET,
MGET, and a SET with a large escaped value. Includes prefix parsing where needed;
does not include framing, formatting, output, or argument decoding. Median ns
per record: disabled 3.04; IP host accept/reject 88.54/84.33; range accept/reject
96.04/116.76; UTC cron accept/reject 163.95/130.23; America/Los_Angeles cron
accept 185.29; existing command-name filter 43.02. This is an optimized test
binary; debug timings printed by all-target correctness tests are not used.

## Dependencies and build cost

No existing dependency provided calendar or cron matching. Croner 4.0.0 (MIT)
provides maintained cron parsing/matching instead of a local cron implementation.
Chrono 0.4.45 and chrono-tz 0.10.4 (MIT/Apache-2.0) supply RFC 3339 parsing and
IANA timezone rules. Only `std` and the Croner `chrono` backend are enabled;
clock/local-time, serde, jiff, and dynamic timezone lookup are unnecessary.
The timezone database is bundled and updates with the dependency, not the OS.

Resolution added 19 packages, including builder/enum proc-macro dependencies
and PHF timezone tables. Source inspection found no unsafe blocks in Croner or
chrono-tz; Chrono contains internal unsafe code behind its safe calendar API.
No unsafe code was added here. The release executable grew from 9,093,584 to
11,146,288 bytes (+22.6%, about 1.96 MiB). A warm-dependency baseline rebuild
took 17.64 s; the first candidate build, including compilation of new dependencies,
took 22.24 s; a subsequent candidate rebuild took 17.56 s. These are incremental
workspace observations, not clean-build comparisons. Cargo.lock remains ignored
according to the repository's existing policy.
