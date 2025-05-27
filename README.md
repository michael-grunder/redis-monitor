# redis-monitor

A cli utility for monitoring one or more RESP compatible servers.

## Building

```bash
git clone git@github.com:michael-grunder/redis-monitor.git
cd redis-monitor
cargo build --release
```

```bash
A utility to monitor one or more RESP compatible servers

Usage: redis-monitor [OPTIONS] [INSTANCES]...

Arguments:
  [INSTANCES]...

Options:
  -c, --cluster                    Treat each instance as a cluster seed
  -f, --format <FORMAT>            Format for each MONITOR line
  -r, --replicas                   Also connect and MONITOR cluster replicas
      --config-file <CONFIG_FILE>  Path to configuration file
      --no-color                   Disable colored output
      --include <INCLUDE>          Include commands matching comma-separated list
      --exclude <EXCLUDE>          Exclude commands matching comma-separated list
      --db <DB>                    Show commands for a specific database
  -u, --user <USER>                Redis username
  -p, --pass <PASS>                Redis password
  -j, --json                       Output in JSON format
      --tls                        Connect using TLS
      --insecure                   Disable TLS certificate verification
      --tls-ca <TLS_CA>            Path to CA certificate for TLS
      --tls-cert <TLS_CERT>        Path to client certificate for TLS
      --tls-key <TLS_KEY>          Path to client private key for TLS
  -v, --version                    Display the version and exit
      --stats <STATS>              Show stats every N seconds (default: 1.0 if no argument)
  -h, --help                       Print help
```
