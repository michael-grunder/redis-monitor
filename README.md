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
      --stats <STATS>
          
      --stdin
          Read from stdin instead of connecting to servers
      --no-batch
          Disable producer batching for the lowest output latency
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
serialized PHP values, and literal backslash sequences.
Standalone `OK` replies from entering MONITOR mode are ignored.
When `--cluster` is used, every instance must be a reachable Redis Cluster seed.
Discovery failures exit nonzero with the failing seed, underlying Redis or I/O
error, and a hint to remove `--cluster` for standalone instances.
Invalid instance arguments, malformed discovered or explicit config files,
incomplete named instances, invalid TLS files, and malformed cluster metadata
also exit nonzero with contextual errors rather than panic.

By default, each source batches up to 64 records or 256 KiB for at most 5 ms
before handing them directly to the output thread. Queued and producer-held
batches share a 64 MiB byte budget, with a bounded batch count as a secondary
guard. This preserves every accepted record and applies backpressure instead of
dropping data when output is slow. A record larger than the byte budget is
allowed through while temporarily consuming the full budget.

Records from an individual source retain their original order. With multiple
sources, each source's batches are atomic at the output queue, but there is no
total ordering guarantee between sources. Use `--no-batch` to hand off each
accepted record individually when minimum output latency is more important than
throughput under load.

When monitoring Redis directly from an interactive terminal, press `/` to edit
a live command-name filter and Enter to apply it. The prompt starts with the
current live filter, so press `/`, erase it with Backspace, and press Enter to
clear it. Escape cancels an edit. Live filters use the same syntax as
`--filter`, including `!` exclusions and `/.../` regular expressions, and are
applied in addition to filters supplied on the command line. Interactive
filtering is disabled with `--stdin`, because standard input carries the
MONITOR stream in that mode.

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
