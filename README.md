# redis-monitor

A cli utility for monitoring one or more RESP compatible servers.

## Building

```bash
git clone git@github.com:michael-grunder/redis-monitor.git
cd redis-monitor
cargo build --release

```

### Building a static binary

To produce a static binary with no runtime dependencies, compile with the `release-static` profile against the MUSL target:

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --profile release-static --target x86_64-unknown-linux-musl
```

The resulting binary in `target/x86_64-unknown-linux-musl/release-static/redis-monitor` is fully self-contained.
## Usage

```
A utility to monitor one or more RESP compatible servers

Usage: redis-monitor [OPTIONS] [INSTANCES]... [COMMAND]

Commands:
  completions  Generate shell completion scripts
  help         Print this message or the help of the given subcommand(s)

Arguments:
  [INSTANCES]...  

Options:
  -c, --cluster
          Treat each instance like its a cluster seed
  -f, --format <FORMAT>
          How to format each MONITOR line
  -r, --replicas
          Also connect and MONITOR cluster replicas
      --config-file <CONFIG_FILE>
          
      --no-color
          Disable colored output
      --db <DB>
          Only show commands for a specific database
  -u, --user <USER>
          Redis user
  -p, --pass <PASS>
          Redis password
      --filter <FILTER>
          Filter command names, or [N] arguments (command is 0); /regex/, =literal, !exclude
      --key-filter <PATTERN>
          Filter keys, or [N]th key (first key is 0); /regex/, =literal, !exclude
      --flags <FLAG|@CATEGORY>
          Require flags (e.g. write) and/or categories (e.g. @hash)
  -o, --output <OUTPUT>
          How to serialize the output. Values: plain, json, php, csv, resp [default:
          plain]
      --tls
          Connect using TLS
      --insecure
          Disable TLS certificate verification
      --tls-ca <TLS_CA>
          Path to CA cert for TLS
      --tls-cert <TLS_CERT>
          Path to client cert for TLS
      --tls-key <TLS_KEY>
          Path to client private key for TLS
  -v, --version
          Display the version and exit
      --stats <SECONDS>
          Periodically report per-command statistics (plain output only)
      --stdin
          Read from stdin instead of connecting to servers
      --batch
          Enable producer batching for higher throughput (may delay records by 5 ms)
      --debug
          Output debug information such as detailed filter info
  -h, --help
          Print help

Format specifiers:
  %S   Short form of server and client address
  %sa  Full address of the server (host:port or unix path)
  %sh  Host part of the server address
  %sp  Port part of the server address (or basename of unix path)
  %Sn  Name of the server instance if it is set
  %ca  Full address of the client (ip:port or unix path)
  %ch  Host part of the client address
  %cp  Port part of the client address (or basename of unix path)
  %d   The database number
  %t   The timestamp as reported by MONITOR
  %l   The full command and all arguments
  %C   Argument 0 (the command)
  %a   Arguments 1..N

  The default formats are:
    Single instance:    "%t [%d %ca] %l";
    Multiple Instances: "%t [%S %d] %l";

Structured outputs (`json`, `php`, `csv`, and `resp`) parse MONITOR arguments as
strings and preserve quoted argument content, including JSON-like values,
serialized PHP values, and literal backslash sequences. CSV output starts with a
`timestamp,db,addr,cmd,args` header and writes one column per argument.
Module commands with punctuation in their names, such as `FT.SEARCH` or
`JSON.SET`, are parsed like any other command.
Standalone `OK` replies from entering MONITOR mode are ignored.

Instances may be given as `port`, `host`, `host:port`, `[ipv6]:port`, a bare
IPv6 address, or a unix socket path. `--db` keeps only commands executed against
that database. Unknown `--flags` names are rejected, and `--flags` cannot be used
with `--stdin` because it needs `COMMAND` metadata from a live server; that
metadata is refreshed on each successful connection to a source.
Connection attempts time out after 10 seconds and are retried with backoff.
Ctrl-C shuts down gracefully; press it a second time to exit immediately. If
the output is closed (for example `redis-monitor | head`), every source stops
and the process exits successfully.
When `--cluster` is used, every instance must be a reachable Redis Cluster seed.
Discovery failures exit nonzero with the failing seed, underlying Redis or I/O
error, and a hint to remove `--cluster` for standalone instances.
Invalid instance arguments, malformed discovered or explicit config files,
incomplete named instances, invalid TLS files, and malformed cluster metadata
also exit nonzero with contextual errors rather than panic.

By default, each source never holds a record back: after every read it hands
all complete accepted records (up to 256 KiB) to the output thread at once, so
records that arrived together share one handoff without adding latency. Use
`--batch` to additionally hold records for up to 5 ms, batching up to 64
records or 256 KiB per source before handoff. In both modes, queued and producer-held records share a 64 MiB byte
budget, with a bounded queue length as a secondary
guard. This preserves every accepted record and applies backpressure instead of
dropping data when output is slow. A record larger than the byte budget is
allowed through while temporarily consuming the full budget.

Records from an individual source retain their original order in both modes.
There is no total ordering guarantee between sources, and output is not sorted
by timestamp. Each handoff is atomic at the output queue, so records from one
source that arrived in the same read are never interleaved with another
source's; with `--batch`, the longer hold can further change how records from
different sources are interleaved. The former `--no-batch` flag has been removed; omit `--batch` for
individual record handoff.

Examples:
  # Monitor localhost:6379 by default
  redis-monitor

  # Monitor a cluster expecting one node to be 127.0.0.1:6379
  redis-monitor -c 6379

  # Monitor two standalone instances
  redis-monitor host1:6379 host2:6379

  # Run while filtering specific commands
  redis-monitor --filter get --filter set
  redis-monitor --filter '!get' --filter '!set'
  redis-monitor --filter '/^geo/'

  # Filtering by command flags and categories
  redis-monitor --flags write,@hash
```

## Filtering by keys

Use `--key-filter` to match only a command's key arguments:

```sh
redis-monitor --key-filter 'user:'
redis-monitor --key-filter '/^user:[0-9]+$/' --key-filter '!private'
redis-monitor --filter set --key-filter 'session:' --db 2
```

Like `--filter`, plain patterns are case-insensitive literal substrings;
`/regex/` patterns are case-sensitive unless the regex enables `(?i)`.
Redis key identity remains case-sensitive: use an anchored regex for an exact
case-sensitive match. Prefix either form with `!` to exclude. Quote patterns
so the shell does not interpret them.

Repeated inclusions are ORed: at least one key must match one inclusion.
An unindexed exclusion matching **any** key rejects the entire command, even
if another key matched an inclusion. Indexed patterns only examine the selected
key; any matching exclusion still rejects the whole command. Patterns are evaluated against each decoded key
separately, never against values, non-key arguments, or concatenated keys.
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

Argument decoding runs after the cheaper database/command-only/flag filters,
and only when key filtering or an argument index beyond zero requires it.
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

Run the retained extraction benchmark with `cargo bench --bench command_keys`.
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

To reproduce or vary the workload:

```sh
cargo build --release
cargo test --release --bin redis-monitor benchmark_key_filter -- --ignored --nocapture
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
