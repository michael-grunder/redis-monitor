# Cluster refresh replay measurements

Date: 2026-09-30. Baseline: `f8cf61a`; candidate: cluster refresh implementation.
The final output-closure fix affects task retirement, not the timed streaming
path; that path is also covered by the topology lifecycle tests.

## Setup

- Linux 6.1.0-50-amd64; two Intel Xeon Platinum 8160 sockets, 48 physical cores.
- Rust 1.98.1; portable release builds (`RUSTFLAGS='' cargo build --release`),
  four application worker threads. Both builds used the same local `Cargo.lock`.
- Four concurrent mock cluster primaries on loopback. Each sends a finite
  sequence of one GET and fifteen SETs per block, followed by a PING marker.
  Values contain escaped quotes; payload cases add the indicated bytes.
- Stdout is read and counted by the Python workload. The slow sink waits 2 ms
  after each read of up to 64 KiB. All expected accepted records must arrive
  before SIGINT; processed, filtered, and invalid counts are checked at exit.
- Five alternating baseline/candidate samples for the first four rows; three
  for the slow sink. The default-interval control is interleaved with short
  plain samples. These are non-isolated, sink-inclusive measurements, not the
  application's maximum throughput or a real Redis server benchmark.

The candidate's stress setting is `--cluster-refresh 0.01`: an interval 3,000
times shorter than the default 30 seconds, allowing repeated discovery within
short replays. Baseline discovery runs only at startup.

## Results

Medians; input MB/s uses decimal megabytes and includes MONITOR framing bytes.
CPU seconds and peak RSS include process startup and shutdown.

| Workload | Records | Baseline records/s | Candidate records/s | Baseline → candidate input MB/s | Baseline → candidate CPU seconds | Baseline → candidate peak RSS (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain, short, default 30 s interval | 1,280,004 | 1,302,777 | 1,313,419 | 61.23 → 61.73 | 1.611 → 1.644 | 21,024 → 21,068 |
| Plain, short, 10 ms interval | 1,280,004 | 1,302,777 | 1,272,771 | 61.23 → 59.82 | 1.611 → 1.673 | 21,024 → 21,036 |
| JSON, 2,048-byte payload, 10 ms interval | 128,004 | 227,429 | 226,565 | 476.45 → 474.64 | 0.713 → 0.718 | 130,796 → 131,580 |
| JSON, same payload, reject SET, 10 ms interval | 128,004 | 827,395 | 752,048 | 1,733.34 → 1,575.49 | 0.183 → 0.202 | 21,020 → 21,052 |
| JSON, 256-byte payload, slow sink, 10 ms interval | 640,004 | 80,313 | 78,775 | 24.33 → 23.87 | 1.087 → 1.380 | 129,788 → 128,560 |

Default-interval throughput was within observed run-to-run variation (baseline
0.954–1.153 s, candidate 0.962–0.993 s). These runs finish before the first
30-second refresh, so they measure supervisor overhead while discovery is idle.
They do not directly measure the cost of a default-interval refresh.

At 10 ms, median throughput fell about 2.3%, 0.4%, 9.1%, and 1.9% respectively.
The reject-most case had baseline times of 0.151–0.169 s versus 0.161–0.182 s;
the first comparison series showed a smaller regression, so the exact percentage
is noisy. The repeated connections/queries also contend with the Python producer
and sink. Periodic discovery is deliberately outside the per-record path, but
very short intervals are not free. These results do not establish zero overhead
on a real cluster.

Median discovery-query counts, including startup, were 92, 47, 16, and 695 for
the stress rows, versus one for the baseline. Plain and reject-most runs had no
queue stalls. Large JSON recorded 614 → 617 median stalls; the slow sink recorded
564 → 561. Accepted output counts matched in every run; no invalid records or
unexpected MONITOR reconnections occurred. RSS remained comparable; it can exceed
the output byte budget because framing and formatting storage are separate.

The measured release executable grew from 9,013,728 to 9,109,344 bytes (about
1.1%). No new dependencies, per-record timers, or per-record synchronization were
introduced. Allocation rates and tail latency were not measured by this replay.

## Reproduce

Save a portable release build of `f8cf61a` as `BASELINE`, then build the candidate
using the same lockfile and compiler flags. The helper requires Python 3.11+ on
Linux and no third-party packages. Run from the repository root:

```sh
RUSTFLAGS='' cargo build --release
python3 benches/cluster_refresh.py BASELINE
python3 benches/cluster_refresh.py target/release/redis-monitor
python3 benches/cluster_refresh.py target/release/redis-monitor --refresh 0.01

# Run each of these once with BASELINE (omit --refresh) and once with candidate.
python3 benches/cluster_refresh.py target/release/redis-monitor --refresh 0.01 \
  --blocks 2000 --payload 2048 --output json
python3 benches/cluster_refresh.py target/release/redis-monitor --refresh 0.01 \
  --blocks 2000 --payload 2048 --output json --reject
python3 benches/cluster_refresh.py target/release/redis-monitor --refresh 0.01 \
  --blocks 10000 --payload 256 --output json --slow-ms 2
```

Repeat in alternating order. Each invocation emits a JSON result with elapsed
time, records/s, bytes/s, discovery-query and stall counts, child CPU time, and
peak RSS. Topology transitions themselves are tested with controlled mock-server
responses in `src/topology.rs`, including failed discovery, role/address changes,
blocked retirement, output closure, and cancellation.
