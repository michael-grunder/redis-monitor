# redis-monitor

A cli utility for monitoring one or more RESP compatible servers.

## Building

```bash
git clone git@github.com:michael-grunder/redis-monitor.git
cd redis-monitor
cargo build --release

```
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
  -c, --cluster                    Treat each instance like its a cluster seed
  -f, --format <FORMAT>            How to format each MONITOR line
  -r, --replicas                   Also connect and MONITOR cluster replicas
      --config-file <CONFIG_FILE>
      --no-color                   Disable colored output
      --db <DB>                    Only show commands for a specific database
  -u, --user <USER>                Redis user
  -p, --pass <PASS>                Redis password
      --filter <FILTER>            One or more patterns to either filter out or in
  -o, --output <OUTPUT>            How to serialize the output. Values: plain, json, php,
                                   csv, resp [default: plain]
      --tls                        Connect using TLS
      --insecure                   Disable TLS certificate verification
      --tls-ca <TLS_CA>            Path to CA cert for TLS
      --tls-cert <TLS_CERT>        Path to client cert for TLS
      --tls-key <TLS_KEY>          Path to client private key for TLS
  -v, --version                    Display the version and exit
      --stats <STATS>
      --stdin                      Read from stdin instead of connecting to servers
  -h, --help                       Print help

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

Examples:
  # Monitor a cluster expecting one node to be 127.0.0.1:6379
  redis-monitor -c 6379
  # Monitor two standalone instances
  redis-monitor host1:6379 host2:6379
```
