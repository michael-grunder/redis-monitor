# redis-monitor

A CLI utility for consuming, filtering, and formatting Redis/Valkey `MONITOR`
streams from one or more servers, or from stdin.

## Building

Use a Rust toolchain that supports the project's Rust 2024 edition and current
dependencies.

```sh
git clone git@github.com:michael-grunder/redis-monitor.git
cd redis-monitor
cargo build --release
./target/release/redis-monitor --help
```

The examples below assume `redis-monitor` is on your `PATH`; otherwise use
`./target/release/redis-monitor`.

The repository's `.cargo/config.toml` enables `target-cpu=native` for default
builds. To build for the target's default CPU instead, override those flags:

```sh
RUSTFLAGS='' cargo build --release
```

### Building a static binary

On Linux, install the MUSL target and a matching C compiler/linker (for example,
`musl-gcc` for a native x86-64 Linux build), then run:

```sh
rustup target add x86_64-unknown-linux-musl
cargo build --profile release-static --target x86_64-unknown-linux-musl
```

The binary is written to
`target/x86_64-unknown-linux-musl/release-static/redis-monitor`.
`release-static` inherits the release profile; the MUSL target provides static
linking. The repository config selects a baseline x86-64 CPU for this target.
TLS still needs system CA certificates or an explicit `--tls-ca` file at runtime.

## Usage

```text
redis-monitor [OPTIONS] [INSTANCES]... [COMMAND]
```

Run `redis-monitor --help` for the full option reference, or
`redis-monitor --version` for the package version and build revision.

```sh
# Monitor localhost:6379 by default
redis-monitor

# Monitor two standalone instances
redis-monitor host1:6379 host2:6379

# Discover cluster primaries, and optionally include replicas
redis-monitor --cluster 6379
redis-monitor --cluster --replicas 6379

# Match command names (case-insensitive substrings), or exclude them
redis-monitor --filter get --filter set
redis-monitor --filter '!get' --filter '!set'

# Regexes are case-sensitive unless requested otherwise
redis-monitor --filter '/(?i)^geo/'

# Require both the write flag and the hash category
redis-monitor --flags write,@hash

# Generate completions (bash, elvish, fish, powershell, or zsh)
redis-monitor completions zsh > _redis-monitor
```

### Connections and named instances

Instances accept a port (`6379`, using `127.0.0.1`), a host, `host:port`,
`[ipv6]:port`, `[ipv6]`, a bare IPv6 address, or a Unix socket path containing
`/`. Hosts without a port use 6379. Redis URLs such as `redis://host:6379` are
not supported as CLI instance arguments.

For direct addresses, use `--user`/`-u` and `--pass`/`-p` (`-a` is also a
password alias). Enable TLS explicitly with `--tls`; `--tls-ca` supplies a PEM
CA bundle, and `--tls-cert` plus `--tls-key` supplies a client certificate and
private key. Without `--tls-ca`, TLS uses the system trust store. `--insecure`
disables certificate verification; the current MONITOR TLS path also omits
client authentication in this mode.

Named instances are TOML tables loaded from `--config-file PATH`, or the first
file found in this order:

1. `./.redis-monitor`
2. `./.redis-monitor.toml`
3. `$HOME/.redis-monitor`
4. `$HOME/.redis-monitor.toml`

For example, save this as `.redis-monitor.toml`:

```toml
[local]
host = "127.0.0.1"
port = 6379

[cache]
addresses = ["cache-a:6379", "cache-b:6379"]
user = "monitor"
pass = "replace-me"
tls = true
tls_ca = "/path/to/ca.pem"
# tls_cert = "/path/to/client.pem"
# tls_key = "/path/to/client-key.pem"

[socket]
path = "/run/redis/redis.sock"

[production]
cluster = true
addresses = ["seed-a:6379", "seed-b:6379"]
```

```sh
redis-monitor local
redis-monitor --config-file .redis-monitor.toml local cache
redis-monitor --format '%t [%sn %sa %d] %l' production
```

Use one address form per entry: `host` and `port` together, a nonempty
`addresses` list, or `path`. Naming an entry selects it; loading a config does
not automatically monitor every entry. A named entry uses its own credentials
and TLS settings, without inheriting the CLI connection options.

A named entry with `cluster = true` tries its seeds until discovery succeeds
and monitors primaries. The CLI `--cluster` mode instead treats every positional
argument as a direct seed address, bypassing named entries, and every seed must
be reachable. `--replicas` applies only to that CLI cluster mode. Discovery runs
at startup; it is not periodically refreshed.

Use CLI `--format` to control plain output. Per-entry `format` and `color`
settings are currently accepted but do not affect output; `--no-color` also has
no effect because the current writers emit no color.

### Reading from stdin

```sh
redis-monitor --stdin --filter '[2]/^bar$/' < monitor.log
redis-monitor --stdin --output json < monitor.log
redis-cli MONITOR | redis-monitor --stdin --format '%C %a'
```

Stdin accepts newline-delimited MONITOR records, with LF or CRLF and an optional
RESP simple-string `+` prefix. A final stdin record without a newline is also
processed. Standalone `OK` replies are ignored. A truncated network record is
discarded when its connection closes.

`--stdin` bypasses server connections and config loading. Command/argument
filters and `--db` work here; `--flags` and `--key-filter` are rejected because
they require live `COMMAND` metadata.

### Output and formatting

Records go to stdout. Connection messages, parse errors, debug information,
statistics, and the final processed/filtered/backpressure summary go to stderr.

| `--output` | Record representation |
| --- | --- |
| `plain` (default) | MONITOR-style text, customizable with `--format`/`-f` |
| `json` | One JSON object per line with `timestamp`, `db`, `addr`, `cmd`, and an `args` array |
| `php` | One PHP-serialized record per line with the same fields |
| `csv` | Header `timestamp,db,addr,cmd,args`, then four metadata columns and one column per argument; row widths vary |
| `resp` | A RESP array of bulk strings containing the command and arguments, without timestamp, database, or address metadata |

Structured outputs decode MONITOR argument escapes and preserve quoted content,
including JSON-like values, serialized PHP values, and literal backslash
sequences. JSON replaces invalid UTF-8 argument bytes with the Unicode replacement
character; PHP, CSV, and RESP preserve decoded argument bytes. Structured output
does not include the monitored server address or configured instance name.
`--format` applies only to plain output.

| Format token | Value |
| --- | --- |
| `%S` | Short form of server and client address |
| `%sa` | Full server address (`host:port` or Unix path) |
| `%sh` | Server host |
| `%sp` | Server port, or basename of its Unix path |
| `%sn` | Configured instance name, or cluster node ID in CLI cluster mode, when set; otherwise `-` |
| `%ca` | Full client address |
| `%ch` | Client host |
| `%cp` | Client port, or basename of its Unix path |
| `%d` | Database number |
| `%t` | MONITOR timestamp |
| `%l` | Full quoted command and arguments |
| `%C` | Command name (argument 0) |
| `%a` | Arguments 1..N |

The default is `%t [%d %ca] %l` for one source or stdin, and `%t [%S %d] %l`
for multiple resolved server connections. Module commands such as `FT.SEARCH`
and `JSON.SET` are supported.

### Database, flags, and statistics

`--db N` selects the database in each MONITOR record. It does not change which
databases the server monitors.

`--flags` accepts comma-separated names and can be repeated. All requested
flags and categories must be present; unknown names are errors. Metadata is
loaded after connecting and refreshed on reconnect. If loading fails without
`--key-filter` enabled, a diagnostic is printed and flag filtering is bypassed for
that source until the next reconnect. Commands absent from the metadata also
pass flag filtering. Other configured filters still apply. Key filtering has
stricter metadata requirements, described below.

`--stats SECONDS` accepts a positive, finite interval (including fractional
seconds). In plain mode it reports cumulative per-command counts and MONITOR
line bytes after filtering, on stderr. The interval is checked after output
drains, so an idle source does not trigger reports. With structured output,
`--stats` is ignored. The final processed/filtered/backpressure summary is
independent of this option. `--debug` prints the compiled filter configuration
to stderr.

### Batching, ordering, and shutdown

By default, each source hands off complete accepted records already available
from a read together, with a 256 KiB chunk target and no wait for more input.
`--batch` additionally coalesces records across reads, with a 64-record limit,
a 256 KiB target, and a 5 ms producer hold deadline. Slow output can delay either
mode beyond that deadline. A single oversized record is kept intact.

Queued and producer-held batches share a 64 MiB byte budget. Queue length is
also bounded: 16,384 messages by default or 1,024 with `--batch`. Slow output
applies backpressure instead of silently dropping accepted records. A record
larger than the byte budget temporarily consumes the full budget. This is not
a process memory cap: input framing buffers, incomplete records, allocation
capacity, and serialization storage also consume memory, and there is no maximum
input record size.

Per-source order is preserved. Across sources, output is not sorted by timestamp
and has no total ordering guarantee. A single queue handoff is written without
interleaving another source's records, but a large read may be split into multiple
handoffs. `--batch` can change cross-source interleaving. The former `--no-batch`
flag is no longer accepted; omit `--batch` for the default behavior.

MONITOR connection attempts time out after 10 seconds and retry with backoff;
disconnected sources reconnect. Cluster discovery failures instead exit nonzero
with context. Invalid addresses, malformed config files, incomplete named
entries, and invalid TLS files also report contextual errors.

Ctrl-C requests shutdown and drains pending output; a second Ctrl-C exits
immediately. If stdout closes (for example `redis-monitor | head`), all sources
stop and the process exits successfully. Other output failures exit nonzero.

## Filtering by keys

Use `--key-filter` to match only a command's key arguments:

```sh
redis-monitor --key-filter 'user:'
redis-monitor --key-filter '/^user:[0-9]+$/' --key-filter '!private'
redis-monitor --filter set --key-filter 'session:' --db 2
```

Like `--filter`, plain patterns are ASCII case-insensitive literal substrings;
`/regex/` patterns are case-sensitive unless the regex enables `(?i)`.
Redis key identity remains case-sensitive: use an anchored regex for an exact
case-sensitive match. Prefix either form with `!` to exclude. Quote patterns
so the shell does not interpret them.

Repeated inclusions are ORed: at least one key must match one inclusion.
An unindexed exclusion matching **any** key rejects the entire command, even
if another key matched an inclusion. Indexed patterns only examine the selected
key; any matching exclusion still rejects the whole command. Patterns are
evaluated against each decoded key separately, never against values, non-key
arguments, or concatenated keys.
With exclusions alone, commands with no keys pass. With inclusions, they do not.
`--db`, `--filter`, `--flags`, and `--key-filter` must all pass when combined.

Each monitored server must permit `COMMAND` to provide its command metadata.
`--key-filter` is unavailable with `--stdin`. If metadata loading fails, the
source logs the failure and reconnects with backoff; it does not emit records
with key filtering bypassed. Metadata is refreshed after reconnecting.
Malformed records and commands whose keys cannot be completely identified
(including unknown commands and unsupported module specs) are rejected and
counted in the final filtered total, including with exclusion-only filters.
Key discovery itself makes no per-command server requests.

Argument decoding runs after the cheaper database and flag filters, and after
command-name filtering when no argument index beyond zero is requested. It runs
only when key filtering or an argument index beyond zero requires it.
Unescaped arguments borrow the input buffer;
argument storage is reused within each scanned chunk and released afterwards.
Escaped arguments need decoding allocations. Structured output currently
parses accepted arguments again on the output thread.

## Positional filters and literal syntax

Both filter options accept `[N]pattern` with a **zero-based** index, but index
different sequences:

- `--filter`: all command arguments, with the command name at index 0.
  For `SET FOO bar`, indices 0, 1, and 2 select `SET`, `FOO`, and `bar`.
- `--key-filter`: only discovered keys, with the first key at index 0.
  For `MSET zero z one o two t three t`, `[3]` selects `three`.
  For `XREAD COUNT 10 STREAMS zero one 0 $`, `[1]` selects `one`, irrespective
  of the options before `STREAMS`. Duplicate key arguments retain their positions;
  ordering is the discovery iterator's spec order, then argument order per spec.

Unindexed `--filter` patterns still match only the command name; unindexed
`--key-filter` patterns still match any key. Indexed and unindexed inclusions
within one option are ORed. Any matching exclusion within that option vetoes
the command. A missing position matches nothing: it cannot satisfy an inclusion
or trigger an exclusion. Positional `--filter` works with `--stdin` and does not
need command metadata; `--key-filter` always does.

The syntax is an optional `!`, followed by an optional `[N]`, followed by the
pattern. A pattern beginning with `=` treats **everything after that equals
sign as literal text**; `/regex/` selects a regex; other text is a literal
substring. Literal matching remains case-insensitive, not an equality test.

```sh
redis-monitor --filter '[0]/^GET$/'
redis-monitor --filter '[2]/^bar$/'
redis-monitor --key-filter '[3]three'
redis-monitor --key-filter '[1]/^user:/' --key-filter '![0]/^private:/'
```

| Pattern | Meaning |
| --- | --- |
| `=[0]foo` | Literal `[0]foo` in the option's default scope |
| `[1]=[0]foo` | Literal `[0]foo` at index 1 |
| `[1]=/foo/` | Literal `/foo/` at index 1 |
| `[1]=!important` | Literal `!important` at index 1 |
| `!=!private` | Exclude literal `!private` in the default scope |
| `==value` | Literal `=value` in the default scope |

Only one leading selector is interpreted; regex contents are not parsed as
selectors. Bare leading `[` is reserved for selectors, and malformed, negative,
range, or overflowing indices are errors. Prefix a literal leading bracket,
equals sign, exclamation mark, or regex-looking string with `=` as above.
Shell quotes preserve the pattern passed to the program; they do not disable
pattern syntax. Empty patterns match any existing value, including an empty key,
but cannot match a missing position.

Matching compiles patterns by position once and scans the borrowed iterator
without collecting keys or allocating storage based on the requested index.
Argument and key filters share a single decoding pass when both need it.

## Command key discovery (library)

The `redis_monitor::commands` module resolves decoded command arguments to a
borrowed iterator of key bytes. Load `Command::load(&mut connection).await?` once
per server and convert its result into a `Lookup`. `Command::from_reply` also
accepts a saved `redis::Value` reply for offline use.

```rust,ignore
let lookup: redis_monitor::commands::Lookup = commands.into();
// The command name is separate; args contains arguments 1..N.
let args: &[&[u8]] = &[b"first", b"value", b"second", b"other value"];
let keys = lookup.keys(b"MSET", args)?;
// keys yields b"first", b"second", borrowing args without allocating.
```

`Lookup::keys` also accepts the parser's `&[Cow<[u8]>]`, handles subcommands
case-insensitively, and supports binary/empty key names. It validates all key
specifications before returning an iterator, allowing a caller to short-circuit
safely. Keys follow spec order and argument order within each spec; duplicate
keys and overlapping specs are retained. Since keys can be interleaved with
values, the result is an iterator rather than a contiguous slice.

Discovery supports index/keyword searches, ranges, key counts, and legacy
fixed-position metadata. `SORT`, `SORT_RO`, `MIGRATE`, and the writable
`GEORADIUS` variants have local grammar handling. `not_key` arguments such as
sharded pub/sub channels are excluded. SORT's data-dependent BY/GET expansions
are not argument keys and cannot be discovered from the argument array.
Unknown commands, unsupported/incomplete module specs, and legacy `movablekeys`
metadata without usable specs return `KeyError`, rather than an incomplete key
set. Invalid arity, counts, and positions also return errors; this is not a full
command syntax validator. Extraction makes no server requests.

The CLI uses this API when `--key-filter` is enabled. Unindexed `--filter`
command-name matching is unchanged.

## Development and verification

Run the required checks from the repository root:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Normal tests use local fixtures and temporary loopback listeners; they do not
require a running Redis server. Manual benchmarks and the live-server comparison
are separate opt-in checks. See [AGENTS.md](AGENTS.md) for contribution and
performance requirements, and [CHANGELOG.md](CHANGELOG.md) for unreleased changes.

Run the retained extraction benchmark with
`RUSTFLAGS='' cargo bench --bench command_keys`.
It compares lookup alone, borrowed key iteration, and collecting keys into a
vector; includes short, binary, multi-key, and large-value cases; and exercises
four concurrent readers. `KEY_BENCH_ROUNDS` adjusts iterations per sample; `KEY_BENCH_MODE=borrow`
limits measurement to borrowed iteration for allocation profiling.
The fixture tests run without a server. To additionally compare against a local
Redis 7+/Valkey server in RESP2 and RESP3:

```sh
KEY_TEST_REDIS_URL=redis://127.0.0.1:6379 \
  cargo test --test command_keys compare_live_command_getkeys -- --ignored
```

The fixture in `tests/fixtures/command.json` was captured with Valkey 8.1.0
`COMMAND INFO` for the commands named in its rows. It retains the server's key
specifications and subcommands. Valkey 8.1's `COMMAND GETKEYS` can misclassify
`GEORADIUS` destination names equal to `STORE`/`STOREDIST`; those cases have
separate grammar regression expectations.

## Recorded performance measurements

These measurements were recorded when key discovery, key filtering, and
positional filtering were introduced. They are historical comparisons, not
fresh measurements of the current checkout. Use optimized builds and explicitly
override the repository's native CPU flags for portable comparisons.
The earlier [performance opportunities report](specs/PERFORMANCE_OPPORTUNITIES.md)
includes a status review of its original recommendations.

On an Intel Xeon Platinum 8160 (Linux x86-64, rustc 1.98.1), the portable
optimized bench profile measured medians of 41.68 ns/command for lookup alone,
81.06 ns for lookup plus borrowed extraction, and 110.62 ns when collecting
keys. Seven samples each replayed 100,000 rounds of the 27-case workload; four
concurrent readers reached 46.80 million commands/s. Lookup alone is the
pre-extraction baseline, not an equivalent implementation: there was no prior
extractor to compare against. These are extraction microbenchmarks, not MONITOR
pipeline throughput. The CLI only performs key discovery when `--key-filter`
is enabled.
Heaptrack found no increase in allocation count when doubling borrowed-extraction
rounds from 1,000 to 2,000 (2,140 allocations in each run, including four
concurrent readers); allocations are
confined to setup, fixture assertions, reporting, and thread startup.

### Key-filter replay measurements

On the same Xeon 8160 / Linux x86-64 / rustc 1.98.1 host, portable release
builds replayed four concurrent simulated MONITOR sources. Short-record tests
used 500,000 SET records per source and five samples; large-record tests used
10,000 records per source with 4 KiB values and three samples. Values deliberately
contain matching text to detect accidental filtering of non-key arguments.

| Workload | Median seconds | Input million records/s | Input MB/s |
| --- | ---: | ---: | ---: |
| Before this option, plain, no key filter | 0.364 | 5.50 | 298 |
| After, plain, key filter disabled | 0.358 | 5.58 | 302 |
| Plain, key filter accepts 90% | 0.712 | 2.81 | 152 |
| Plain, key filter accepts 10% | 0.738 | 2.71 | 149 |
| 4 KiB values, plain, accepts 90% | 0.563 | 0.071 | 295 |
| 4 KiB values, JSON, accepts 90% | 0.899 | 0.045 | 185 |
| Same JSON workload, reader delays 2 ms per chunk | 5.760 | 0.007 | 29 |

The disabled difference is within run-to-run noise. Enabling key filtering adds
argument parsing and validation, roughly halving short-record throughput in this
workload. Every replay verified the exact output count and processed/filtered
counters, including slow-output and transient metadata-failure recovery runs. A separate
escaped-value slow-reader run exercised 1,387 backpressure stalls with exact
output counts.
The release filter microbenchmark (SET, MSET, and XREAD records)
measured about 360 ns/record with key filtering, versus 3 ns with filters disabled.
The release executable grew from 8,977,040 to 8,998,224 bytes (about 0.24%).

The replay commands below require `scripts/bench_key_filter.py`, which is
present in this workspace but is not tracked in the repository. A clean checkout
can run the retained Rust benchmarks, but needs that helper to reproduce these
end-to-end replays. With the helper available:

```sh
RUSTFLAGS='' cargo build --release
RUSTFLAGS='' cargo test --release --bin redis-monitor benchmark_key_filter -- --ignored --nocapture
python3 scripts/bench_key_filter.py --records 500000
python3 scripts/bench_key_filter.py --key-filter --records 500000 --accept-per-ten 1
python3 scripts/bench_key_filter.py --key-filter --records 10000 --payload 4096 --output json --slow-ms 2
python3 scripts/bench_key_filter.py --key-filter --records 1000 --metadata-failures 1 --samples 1
```

The replay starts temporary local TCP servers and does not access Redis data.
Use `--escaped-payload` to exercise decoding allocations, `--producers` to change
source count, and `--binary` to compare another build.

### Positional-filter measurements

On the same host and portable release profile, the positional implementation
replayed four sources with 500,000 SET records each. The following are medians
of five samples (three for the 10%-accepted case):

| Workload | Before selectors, seconds | After selectors, seconds | After, million records/s |
| --- | ---: | ---: | ---: |
| Plain, filters disabled | 0.348 | 0.357 | 5.60 |
| Unindexed key regex, accepts 90% | 0.735 | 0.766 | 2.61 |
| Indexed key regex, accepts 90% | — | 0.777 | 2.58 |
| Indexed key regex, accepts 10% | — | 0.769 | 2.60 |

The final comparison shows about 4% lower unindexed key-filter throughput;
earlier comparisons varied from 7–10% lower. Indexing adds about 1% relative
to the new unindexed path. This is a small but measurable cost, despite avoiding
per-record key collections. Profiling still attributes most CPU time to parsing.
The SET/MSET/XREAD microbenchmark measured 367 ns/record for literal key filters
(357 ns before), 380 ns for indexed literals, 363/372 ns for unindexed/indexed
regexes, and 299 ns for indexed arguments without key discovery. Disabled
filters remained around 3–4 ns/record. The release executable grew from
8,998,224 to 9,025,104 bytes (0.30%); an incremental release rebuild took about
15 seconds.

The replay also verifies second-key selection in MSET and XREAD, positional
argument filtering without metadata, and escaped 4 KiB values through JSON
output with a slow reader. All replay checks require exact output and
processed/filtered counts. Reproduce these cases with:

```sh
python3 scripts/bench_key_filter.py --key-filter --positioned --records 500000
python3 scripts/bench_key_filter.py --key-filter --positioned --accept-per-ten 1 --records 500000 --samples 3
python3 scripts/bench_key_filter.py --key-filter --positioned --command mset
python3 scripts/bench_key_filter.py --key-filter --positioned --command xread
python3 scripts/bench_key_filter.py --positioned --command xread
python3 scripts/bench_key_filter.py --key-filter --positioned --records 10000 --payload 4096 --escaped-payload --output json --slow-ms 2
```
