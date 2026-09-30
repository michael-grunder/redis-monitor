# Output selection and source measurements

Measured 2026-09-30, baseline `b014f06` versus the `--source`/structured
`--format` change. Linux x86-64, two Intel Xeon Platinum 8160 CPUs, 96 logical
CPUs; Rust 1.98.1. Both binaries use portable `RUSTFLAGS=''` release builds
(opt-level 3, thin LTO, one codegen unit, panic abort). No dependencies added.

> **Superseded:** default JSON, PHP, and CSV output now use the field
> selection `%t %d %ca %C %a`, which after optimization is faster than the
> dedicated serializers measured here. See the README's "Unified structured
> output" measurements.

## Default versus selected fields

Default serializers do not compile or traverse a field/interpolation plan.
An explicit structured format compiles once before connecting. Selected text
fields share a scratch buffer during each input scan; it is released at the
end of that scan rather than retaining a pathological record's capacity.
Argument validation/decoding still applies to every structured record.

The ignored formatter benchmark uses seven samples of 100,000 records, reused
output and scratch buffers, a named source `127.0.0.1:6379` / `primary`, and
short GET or escaped large SET records. Run:

```sh
RUSTFLAGS='' cargo test --release --bin redis-monitor benchmark_json -- --ignored --nocapture
```

Median ns/record and serialized bytes/record:

| Mode | Short ns | Short bytes | Large ns | Large bytes |
| --- | ---: | ---: | ---: | ---: |
| Baseline JSON | 344.38 | 77 | 10821.37 | 3408 |
| Baseline json-source | 408.10 | 132 | 10930.68 | 3463 |
| Default JSON | 319.59 | 77 | 10670.50 | 3408 |
| JSON --source | 383.75 | 132 | 10714.57 | 3463 |
| JSON --format '%t %d %ca %C %a' | 348.78 | 77 | 10495.10 | 3408 |
| JSON --format '%C %a' | 193.06 | 29 | 10383.70 | 3360 |

Selecting all default fields costs about 9% more time for the short record than
the direct serializer. Selecting fewer fields saves both serialization work
and bytes. Large escaped arguments dominate these records; small timing
differences there should not be treated as a demonstrated improvement.

## Multi-server replay

The retained `benches/source_output.py` drives four simultaneous mock servers,
four monitor threads, and a stdout consumer. It checks expected output record
counts, processed/filtered counters, zero invalid records, and clean shutdown
with no trailing records. All measured runs passed. Rust tests separately
verify field values, native types, escaping, source attribution, ordering,
malformed input, and backpressure. Existing default golden fixtures are unchanged.

Each block is 1,024 records: one GET per fifteen SETs, with escaped quotes in
the arguments. Each source ends with a PING. Timing starts after all MONITOR
handshakes and includes stdout draining. Five rounds alternate the listed
variants for each workload. No CPU pinning; this is a shared host. These are
sink-inclusive replay measurements, not maximum formatter throughput.

Build and save the baseline before applying the change:

```sh
RUSTFLAGS='' cargo build --release
cp target/release/redis-monitor /tmp/redis-monitor-before-format
# Apply the change and rebuild.
RUSTFLAGS='' cargo build --release
python3 benches/source_output.py target/release/redis-monitor --output json --blocks 512
python3 benches/source_output.py target/release/redis-monitor --output json --source --blocks 512
python3 benches/source_output.py target/release/redis-monitor --output json --format '%C %a' --blocks 512
```

Use these arguments for the workloads below:

- Short: `--blocks 512`.
- Large: `--blocks 32 --payload 4096`.
- Reject: `--blocks 512 --payload 128 --reject` (reject 15/16 records).
- Slow: `--blocks 128 --slow-ms 1` (pause after each read of at most 64 KiB).
- Plain: `--blocks 512 --output plain`; add `--format '%t [%d %ca] %l'`
  to the baseline so it emits the same bytes as the new source-free default.
- Selected defaults: `--format '%t %d %ca %C %a'`.
- Command + args: `--format '%C %a'`.

Input throughput, medians and ranges across the five samples (decimal MB/s):

| Workload | Mode | Million records/s | Range | MB/s |
| --- | --- | ---: | --- | ---: |
| short | Baseline JSON | 4.138 | 4.060–4.177 | 211.1 |
| short | Default JSON | 3.893 | 3.867–4.193 | 198.5 |
| short | JSON + source | 2.783 | 2.778–2.958 | 142.0 |
| short | JSON selected defaults | 3.918 | 3.793–4.101 | 199.8 |
| short | JSON command + args | 6.849 | 6.726–6.925 | 349.3 |
| large | Baseline JSON | 0.104 | 0.098–0.118 | 431.3 |
| large | Default JSON | 0.111 | 0.100–0.130 | 458.6 |
| large | JSON command + args | 0.113 | 0.099–0.130 | 466.8 |
| reject | Baseline JSON | 13.551 | 13.380–14.052 | 2425.7 |
| reject | Default JSON | 13.681 | 13.574–14.206 | 2449.0 |
| reject | JSON command + args | 14.960 | 14.563–15.309 | 2677.9 |
| slow | Baseline JSON | 0.514 | 0.511–0.520 | 26.2 |
| slow | Default JSON | 0.515 | 0.505–0.526 | 26.2 |
| slow | JSON + source | 0.330 | 0.327–0.336 | 16.8 |
| slow | JSON command + args | 1.103 | 1.085–1.132 | 56.2 |
| plain | Baseline plain (explicit template) | 7.818 | 7.748–7.916 | 398.7 |
| plain | Default plain | 7.803 | 7.733–7.861 | 398.0 |

Resource and output costs. Peak child RSS and user+system CPU time are medians,
measured by `getrusage(RUSAGE_CHILDREN)` in a fresh benchmark process per sample:

| Workload | Mode | Output bytes | Peak RSS KiB | CPU seconds |
| --- | --- | ---: | ---: | ---: |
| short | Baseline JSON | 188743924 | 45544 | 1.136 |
| short | Default JSON | 188743924 | 45840 | 1.143 |
| short | JSON + source | 295698880 | 94080 | 1.374 |
| short | JSON selected defaults | 188743924 | 41516 | 1.202 |
| short | JSON command + args | 88080484 | 21308 | 0.764 |
| large | Baseline JSON | 548667636 | 87436 | 1.338 |
| large | Default JSON | 548667636 | 88192 | 1.169 |
| large | JSON command + args | 542376036 | 89116 | 1.144 |
| reject | Baseline JSON | 28573940 | 40168 | 0.345 |
| reject | Default JSON | 28573940 | 41132 | 0.341 |
| reject | JSON command + args | 22282340 | 35580 | 0.315 |
| slow | Baseline JSON | 47186164 | 46304 | 0.320 |
| slow | Default JSON | 47186164 | 46256 | 0.321 |
| slow | JSON + source | 73925056 | 70420 | 0.392 |
| slow | JSON command + args | 22020196 | 24240 | 0.214 |
| plain | Baseline plain (explicit template) | 102760524 | 69148 | 0.396 |
| plain | Default plain | 102760524 | 68644 | 0.400 |

Default JSON's short-replay median was 5.9% below baseline, despite a faster
formatter microbenchmark and similar child CPU time; the sample ranges overlap.
Large/default JSON was 6.3% higher, reject-most was 1.0% higher, and slow/default
JSON and equivalent plain output were essentially unchanged. This is mixed
end-to-end evidence, not a claim of an across-the-board speedup.

For short replay, selecting only command/arguments emitted 53.3% fewer bytes
than default JSON and improved median throughput from 3.89 to 6.85 million
input records/s. The slow sink improved from 0.515 to 1.103 million records/s,
mostly because it had fewer bytes to drain. Adding source identity to unnamed
five-digit loopback addresses adds 51 bytes per emitted record; the larger
stream lowers throughput and can increase queue occupancy and peak RSS.
The existing output byte budget still applies to serialized bytes and is not
a hard RSS limit. No records were dropped.

The release binary grew from 9,111,032 to 9,130,840 bytes (+19,808, about 0.22%).
Incremental release rebuilds took about 17 seconds before and after; no clean
dependency-build comparison was made. Allocation rate, tail latency, and stall
duration were not measured. The slow sink is delay-based, not an exact byte-rate
limiter.

