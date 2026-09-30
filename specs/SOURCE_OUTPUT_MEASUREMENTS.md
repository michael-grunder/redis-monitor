# Source identity output measurements

Historical measurements of the initial implementation. `--output json-source`
has since been replaced by `--output json --source`; see the current
[field-selection and source measurements](OUTPUT_FORMAT_MEASUREMENTS.md).

Measured 2026-09-30 against baseline `a3d861d`, on Linux x86-64 with two
Intel Xeon Platinum 8160 CPUs (96 logical CPUs), Rust 1.98.1. Both binaries
used the portable release profile (`RUSTFLAGS=''`, overriding the repository's
native-CPU setting): opt-level 3, thin LTO, one codegen unit, panic abort.
The candidate adds `--output json-source`. No dependencies were added.

## Formatter measurement

The ignored `output::tests::benchmark_json` test measures parsing, argument
decoding, and JSON serialization into a reused buffer. Seven samples of
100,000 records per case; medians below. The same test was first run against
the baseline with only `OutputKind::Json` in its format list.

```sh
RUSTFLAGS='' cargo test --release --bin redis-monitor benchmark_json -- --ignored --nocapture
```

The source is `127.0.0.1:6379`, named `primary`. The short record is a GET;
the large SET argument repeats `value\"\x00` 256 times.

| Case | Baseline JSON ns/record | Candidate JSON ns/record | Candidate json-source ns/record | JSON bytes/record | json-source bytes/record |
| --- | ---: | ---: | ---: | ---: | ---: |
| Short | 355.38 | 352.32 | 413.89 | 77 | 132 |
| Large escaped argument | 11466.25 | 10685.53 | 10497.75 | 3408 | 3463 |

Adding this source costs 55 bytes per record, or 71.4% for the short record
and 1.6% for the large record. Short serialization time increased 17.5% versus
candidate legacy JSON. Large-record measurements varied between runs; their
apparent improvement is not evidence that adding identity speeds serialization.
Source strings are borrowed, with no added per-record string allocation.

## Multi-server replay

`benches/source_output.py` provides four concurrent mock Redis servers, four
monitor worker threads, and a stdout consumer that counts all records/bytes.
It verifies the expected output count, processed/filtered summary, zero invalid
records, successful shutdown, and no trailing records. It does not validate
each record's contents; golden and multi-source Rust tests cover those.

Each producer sends blocks of 1,024 records (one GET per fifteen SETs), followed
by a PING. Arguments include escaped quotes. All servers start streaming after
the final MONITOR handshake. Timing includes socket ingestion and stdout drain,
but excludes connection setup. Source identities are unnamed loopback servers
with five-digit ephemeral ports. No records were dropped in any measured run.

Build/copy the baseline before applying the feature, then build the candidate:

```sh
RUSTFLAGS='' cargo build --release
cp target/release/redis-monitor /tmp/redis-monitor-before-source
# Apply the feature, then:
RUSTFLAGS='' cargo build --release
python3 benches/source_output.py target/release/redis-monitor --output json-source --blocks 512
```

For each workload, run five rounds alternating baseline JSON, candidate JSON,
and candidate json-source. For plain output, alternate the two binaries using
`--output plain`. All other settings are the script defaults.

| Workload | Replay arguments |
| --- | --- |
| Short | `--blocks 512` |
| Large | `--blocks 32 --payload 4096` |
| Reject 15/16 | `--blocks 512 --payload 128 --reject` |
| Slow stdout | `--blocks 256 --slow-ms 1` |
| Plain | `--blocks 512 --output plain` |

Median input million records/s (sample minimum–maximum in parentheses):

| Workload | Baseline | Candidate legacy format | Candidate json-source |
| --- | ---: | ---: | ---: |
| Short | 3.916 (3.871–3.934) | 3.938 (3.909–3.947) | 2.931 (2.794–2.992) |
| Large | 0.112 (0.107–0.125) | 0.116 (0.099–0.128) | 0.097 (0.096–0.110) |
| Reject 15/16 | 13.604 (13.276–14.503) | 13.662 (13.545–14.516) | 12.713 (12.370–12.835) |
| Slow stdout | 0.509 (0.502–0.525) | 0.506 (0.503–0.512) | 0.324 (0.317–0.332) |
| Plain | 7.645 (7.602–7.915) | 7.715 (7.682–7.872) | N/A |

Median input MB/s (decimal), in the same order:

| Workload | Baseline | Candidate legacy format | Candidate json-source |
| --- | ---: | ---: | ---: |
| Short | 199.7 | 200.8 | 149.5 |
| Large | 463.7 | 481.0 | 401.4 |
| Reject 15/16 | 2435.2 | 2445.5 | 2275.6 |
| Slow stdout | 26.0 | 25.8 | 16.5 |
| Plain | 389.9 | 393.5 | N/A |

Output bytes are identical between baseline and candidate legacy formats.
For these unnamed servers, source identity adds 51 bytes per emitted record:

| Workload | Legacy output bytes | json-source output bytes | Increase |
| --- | ---: | ---: | ---: |
| Short | 188743924 | 295698880 | 56.7% |
| Large | 548667636 | 555352512 | 1.2% |
| Reject 15/16 | 28573940 | 35258816 | 23.4% |
| Slow stdout | 94372084 | 147849664 | 56.7% |

Median child peak RSS in KiB and child user+system CPU seconds per run, measured
by `getrusage(RUSAGE_CHILDREN)` in a fresh benchmark process for each sample:

| Workload | Baseline RSS / CPU | Candidate legacy RSS / CPU | Candidate json-source RSS / CPU |
| --- | ---: | ---: | ---: |
| Short | 56028 / 1.132 | 56900 / 1.117 | 95788 / 1.322 |
| Large | 88344 / 1.166 | 88336 / 1.168 | 105504 / 1.221 |
| Reject 15/16 | 41424 / 0.338 | 41052 / 0.335 | 47116 / 0.347 |
| Slow stdout | 80712 / 0.632 | 80952 / 0.625 | 99780 / 0.746 |
| Plain | 49740 / 0.490 | 50272 / 0.485 | N/A |

Legacy output throughput remained within about 4% of baseline medians. Opt-in
identity reduced short replay throughput by 25.6%, reject-most throughput by
6.9%, and slow-output throughput by 36.0% versus candidate JSON. Large replay
throughput fell 16.6%, with overlapping sample ranges. The larger output also
increased peak RSS: queue occupancy and producer buffers depend on sink speed
and serialized size, while the existing byte budget continues to apply to
formatted bytes. The budget is not a hard process RSS cap.

These are sink-inclusive Python-driven replays on a shared host, not maximum
Rust throughput claims. The slow sink sleeps 1 ms after each read of at most
64 KiB; it is not an exact byte-rate limiter. Allocation rate, tail latency,
and stall duration were not measured. The portable release binary grew from
9,109,600 to 9,111,032 bytes (+1,432 bytes).
