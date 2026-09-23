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
          One or more literal or regex patterns to filter command names
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
metadata is loaded on the first successful connection to each source.
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
