# redis-monitor

A cli utility for monitoring one or more RESP compatible servers.

## Building

```bash
git clone git@github.com:michael-grunder/redis-monitor.git
cd redis-monitor
cargo build --release
```

## Usage

```bash
A utility to monitor one or more RESP compatible servers

Usage: redis-monitor [OPTIONS] [INSTANCES]...

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
  -j, --json                       Output in JSON format
      --tls                        Connect using TLS
      --insecure                   Disable TLS certificate verification
      --tls-ca <TLS_CA>            Path to CA cert for TLS
      --tls-cert <TLS_CERT>        Path to client cert for TLS
      --tls-key <TLS_KEY>          Path to client private key for TLS
  -v, --version                    Display the version and exit
      --stats <STATS>
  -h, --help                       Print help

Format specifiers:
  %A  Full address (host:port or unix path)
  %h  Host part of the address
  %n  Name of the monitor if one exists
  %p  The 'short' name (port if TCP and basename(path) if a unix socket)

Examples:
  # Monitor a cluster expecting one node to be 127.0.0.1:6379
  redis-monitor -c 6379
  # Monitor two standalone instances
  redis-monitor host1:6379 host2:6379```
