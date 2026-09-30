//! Formatting MONITOR records for output.
//!
//! A [`Formatter`] is immutable and shared by every source, so records are
//! formatted in parallel by the tasks that read them. Each record is appended
//! to a byte buffer; the output thread only writes finished buffers.
use std::{
    borrow::Cow,
    fmt,
    io::{self, Write},
    net::IpAddr,
    str::FromStr,
};

use anyhow::{Error, Result, anyhow};
use lexical_core::FormattedSize;
use serde::{Serialize, Serializer};

mod fields;
use fields::Fields;
use serde_bytes::Bytes as SerBytes;
use serde_php as php;

use redis_monitor::monitor::{Client, MAX_TCP_ADDR_LEN, Record, write_uint};

use crate::connection::{GetHost, ServerAddr};

/// Longest excerpt of an invalid record included in its error message.
const INVALID_EXCERPT: usize = 256;

/// A record that could not be formatted, usually because it is not a valid
/// MONITOR record. Callers report and skip these.
#[derive(Debug)]
pub struct FormatError(String);

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FormatError {}

impl FormatError {
    /// Describe the failure with a bounded excerpt of the record.
    fn new(line: &[u8], error: impl fmt::Display) -> Self {
        let excerpt = &line[..line.len().min(INVALID_EXCERPT)];
        let excerpt = String::from_utf8_lossy(excerpt);
        Self(if excerpt.len() < line.len() {
            format!(
                "Failed to parse line '{excerpt}...' ({} bytes, {error})",
                line.len()
            )
        } else {
            format!("Failed to parse line '{excerpt}' ({error})")
        })
    }
}

/// A monitored server, with the text used by `%S` and `%s*` tokens rendered
/// once instead of per record.
#[derive(Debug)]
pub struct Source {
    origin: SourceOrigin,
    name: Option<String>,
    ip: Option<IpAddr>,
    addr: String,
    host: String,
    port: String,
}

#[derive(Debug, Copy, Clone)]
enum SourceOrigin {
    Server,
    Reader,
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.addr)
    }
}

impl Source {
    pub fn new(server: &ServerAddr, name: Option<String>) -> Self {
        let (ip, port) = match server {
            ServerAddr::Tcp(_, port, ip) => (*ip, port.to_string()),
            ServerAddr::Unix(path) => {
                (None, path.rsplit('/').next().unwrap_or(path).to_owned())
            }
        };
        Self {
            origin: SourceOrigin::Server,
            name,
            ip,
            addr: server.to_string(),
            host: server.get_host().to_owned(),
            port,
        }
    }

    /// Keep the reader label for diagnostics/plain output, without claiming
    /// it identifies the server that originally produced the captured records.
    pub fn from_reader(label: &str) -> Self {
        let mut source = Self::new(&ServerAddr::from_path(label), None);
        source.origin = SourceOrigin::Reader;
        source
    }

    fn structured(&self) -> StructuredSource<'_> {
        StructuredSource {
            address: match self.origin {
                SourceOrigin::Server => Some(self.addr.as_str()),
                SourceOrigin::Reader => None,
            },
            name: self.name.as_deref(),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OutputKind {
    Plain,
    Json,
    Csv,
    Resp,
    Php,
}

impl FromStr for OutputKind {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "plain" => Ok(Self::Plain),
            "resp" => Ok(Self::Resp),
            "json" => Ok(Self::Json),
            "csv" => Ok(Self::Csv),
            "php" => Ok(Self::Php),
            _ => Err(anyhow!(
                "Invalid output format '{s}'. Supported: \
                 plain, resp, json, csv, php"
            )),
        }
    }
}

/// Scratch storage lives for one input-buffer scan, so large field values are
/// reused within a scan but never retained indefinitely. Defaults allocate none.
#[derive(Default)]
pub struct Scratch<'a> {
    pub args: Vec<Cow<'a, [u8]>>,
    value: Vec<u8>,
}

/// Default serializers never construct or walk an interpolation plan.
#[derive(Debug)]
pub enum Formatter {
    PlainDefault {
        source: bool,
    },
    Plain {
        format: PlainFormat,
        source: bool,
    },
    Json {
        source: bool,
    },
    Csv {
        source: bool,
    },
    Resp {
        source: bool,
    },
    Php {
        source: bool,
    },
    Selected(Fields),
    #[cfg(test)]
    Raw,
}

impl Formatter {
    pub fn new(
        kind: OutputKind,
        format: Option<&str>,
        source: bool,
    ) -> Result<Self> {
        if let Some(format) = format {
            return Ok(if kind == OutputKind::Plain {
                Self::Plain {
                    format: PlainFormat::new(format),
                    source,
                }
            } else {
                Self::Selected(Fields::new(kind, format, source)?)
            });
        }
        Ok(match kind {
            OutputKind::Plain => Self::PlainDefault { source },
            OutputKind::Json => Self::Json { source },
            OutputKind::Csv => Self::Csv { source },
            OutputKind::Resp => Self::Resp { source },
            OutputKind::Php => Self::Php { source },
        })
    }

    pub fn header(&self) -> Option<&[u8]> {
        match self {
            Self::Csv { source: false } => {
                Some(b"timestamp,db,addr,cmd,args\n")
            }
            Self::Csv { source: true } => {
                Some(b"source_address,source_name,timestamp,db,addr,cmd,args\n")
            }
            Self::Selected(fields) if fields.kind == OutputKind::Csv => {
                Some(&fields.header)
            }
            _ => None,
        }
    }

    /// Append a complete record, leaving `out` unchanged on invalid input.
    pub fn format<'a>(
        &self,
        out: &mut Vec<u8>,
        source: &Source,
        line: &'a [u8],
        scratch: &mut Scratch<'a>,
    ) -> Result<(), FormatError> {
        let start = out.len();
        let result = match self {
            Self::PlainDefault { source: include } => {
                let record = Record::parse(line)
                    .map_err(|e| FormatError::new(line, e))?;
                if *include {
                    write_plain_source(out, source)
                } else {
                    Ok(())
                }
                .and_then(|()| write_default_plain(out, &record))
            }
            Self::Plain {
                format,
                source: include,
            } => {
                let record = Record::parse(line)
                    .map_err(|e| FormatError::new(line, e))?;
                if *include {
                    write_plain_source(out, source)
                } else {
                    Ok(())
                }
                .and_then(|()| format.write(out, source, &record))
            }
            Self::Json { source: include } => {
                let record = parse_structured(line, &mut scratch.args)?;
                let mut structured =
                    Structured::new(&record, TextArgs(&scratch.args));
                if *include {
                    structured.source = Some(source.structured());
                }
                serde_json::to_writer(&mut *out, &structured)
                    .map_err(io::Error::from)
            }
            Self::Php { source: include } => {
                let record = parse_structured(line, &mut scratch.args)?;
                let mut structured =
                    Structured::new(&record, ByteArgs(&scratch.args));
                if *include {
                    structured.source = Some(source.structured());
                }
                php::to_writer(&mut *out, &structured).map_err(io::Error::other)
            }
            Self::Csv { source: include } => {
                let record = parse_structured(line, &mut scratch.args)?;
                if *include {
                    write_csv_source(out, source);
                }
                write_csv(out, &record, &scratch.args)
            }
            Self::Resp { source: include } => {
                let record = parse_structured(line, &mut scratch.args)?;
                if *include {
                    write_resp_source(out, source)
                } else {
                    Ok(())
                }
                .and_then(|()| write_resp(out, &record, &scratch.args))
            }
            Self::Selected(fields) => {
                let record = parse_structured(line, &mut scratch.args)?;
                fields.write(out, source, &record, scratch)
            }
            #[cfg(test)]
            Self::Raw => out.write_all(line),
        };
        if let Err(error) = result {
            out.truncate(start);
            return Err(FormatError::new(line, error));
        }
        let resp = matches!(self, Self::Resp { .. })
            || matches!(self, Self::Selected(fields) if fields.kind == OutputKind::Resp);
        if !resp {
            out.push(b'\n');
        }
        Ok(())
    }
}

fn write_plain_source(out: &mut Vec<u8>, source: &Source) -> io::Result<()> {
    let source = source.structured();
    write!(
        out,
        "[{} {}] ",
        source.address.unwrap_or("-"),
        source.name.unwrap_or("-")
    )
}

fn write_default_plain(
    out: &mut Vec<u8>,
    record: &Record<'_>,
) -> io::Result<()> {
    record.write_timestamp(out)?;
    out.push(b' ');
    if let Some(tail) = record.default_tail {
        return out.write_all(tail);
    }
    out.push(b'[');
    write_uint(out, record.db)?;
    out.push(b' ');
    record.client.write_addr(out)?;
    out.extend_from_slice(b"] ");
    write_full_line(out, record)
}

fn write_full_line(out: &mut Vec<u8>, record: &Record<'_>) -> io::Result<()> {
    out.push(b'"');
    out.extend_from_slice(record.cmd);
    out.push(b'"');
    if !record.args.is_empty() {
        out.push(b' ');
        out.write_all(record.args)?;
    }
    Ok(())
}

fn write_csv_source(out: &mut Vec<u8>, source: &Source) {
    let source = source.structured();
    csv_field(out, source.address.unwrap_or("").as_bytes());
    out.push(b',');
    csv_field(out, source.name.unwrap_or("").as_bytes());
    out.push(b',');
}

/// Opt-in RESP envelope: [[address, name], command-or-selected-fields].
fn write_resp_source(out: &mut Vec<u8>, source: &Source) -> io::Result<()> {
    out.extend_from_slice(b"*2\r\n*2\r\n");
    let source = source.structured();
    resp_optional(out, source.address)?;
    resp_optional(out, source.name)
}

fn resp_optional(out: &mut Vec<u8>, value: Option<&str>) -> io::Result<()> {
    if let Some(value) = value {
        resp_bulk(out, value.as_bytes())
    } else {
        out.write_all(b"$-1\r\n")
    }
}

/// Parse a record and decode all of its arguments, as structured output needs.
fn parse_structured<'a>(
    line: &'a [u8],
    args: &mut Vec<Cow<'a, [u8]>>,
) -> Result<Record<'a>, FormatError> {
    let record = Record::parse(line).map_err(|e| FormatError::new(line, e))?;
    record
        .decode_args(args)
        .map_err(|e| FormatError::new(line, e))?;
    Ok(record)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FormatToken {
    Literal(Vec<u8>),
    ClientServerShort,
    ServerAddress,
    ServerName,
    ServerHost,
    ServerPort,
    ClientAddress,
    ClientHost,
    ClientPort,
    Timestamp,
    Database,
    Command,
    Arguments,
    FullLine,
}

/// Formats whose output can often be copied straight from the input.
#[derive(Debug, Copy, Clone)]
enum FastFormat {
    None,
    /// `%l`
    FullLine,
    /// `%t [%d %ca] %l`
    DefaultSingle,
}

/// A compiled `--format` string.
#[derive(Debug)]
pub struct PlainFormat {
    tokens: Vec<FormatToken>,
    fast: FastFormat,
}

impl PlainFormat {
    fn new(format: &str) -> Self {
        let tokens = compile_format(format);
        let fast = match tokens.as_slice() {
            [FormatToken::FullLine] => FastFormat::FullLine,
            [
                FormatToken::Timestamp,
                FormatToken::Literal(first),
                FormatToken::Database,
                FormatToken::Literal(second),
                FormatToken::ClientAddress,
                FormatToken::Literal(third),
                FormatToken::FullLine,
            ] if first == b" [" && second == b" " && third == b"] " => {
                FastFormat::DefaultSingle
            }
            _ => FastFormat::None,
        };
        Self { tokens, fast }
    }

    fn write(
        &self,
        w: &mut Vec<u8>,
        source: &Source,
        record: &Record<'_>,
    ) -> io::Result<()> {
        match self.fast {
            FastFormat::FullLine => {
                if let Some(full_line) = record.full_line {
                    return w.write_all(full_line);
                }
            }
            FastFormat::DefaultSingle => {
                if let Some(tail) = record.default_tail {
                    record.write_timestamp(w)?;
                    w.write_all(b" ")?;
                    return w.write_all(tail);
                }
            }
            FastFormat::None => {}
        }

        for token in &self.tokens {
            token.write(w, source, record)?;
        }
        Ok(())
    }
}

impl FormatToken {
    fn write(
        &self,
        w: &mut Vec<u8>,
        source: &Source,
        record: &Record<'_>,
    ) -> io::Result<()> {
        match self {
            Self::Literal(bytes) => w.write_all(bytes)?,
            Self::ClientServerShort => match record.client {
                Client::Tcp { ip, port } if source.ip == Some(ip) => {
                    w.write_all(source.port.as_bytes())?;
                    w.write_all(b" ")?;
                    write_uint(w, port)?;
                }
                client => {
                    w.write_all(source.addr.as_bytes())?;
                    w.write_all(b" ")?;
                    client.write_addr(w)?;
                }
            },
            Self::ServerAddress => {
                w.write_all(source.addr.as_bytes())?;
            }
            Self::ServerName => {
                w.write_all(source.name.as_deref().unwrap_or("-").as_bytes())?;
            }
            Self::ServerHost => {
                w.write_all(source.host.as_bytes())?;
            }
            Self::ServerPort => {
                w.write_all(source.port.as_bytes())?;
            }
            Self::ClientAddress => record.client.write_addr(w)?,
            Self::ClientHost => record.client.write_host(w)?,
            Self::ClientPort => record.client.write_port(w)?,
            Self::Timestamp => record.write_timestamp(w)?,
            Self::Database => write_uint(w, record.db)?,
            Self::Command => w.write_all(record.cmd)?,
            Self::Arguments => w.write_all(record.args)?,
            Self::FullLine => write_full_line(w, record)?,
        }
        Ok(())
    }
}

fn compile_format(fmt: &str) -> Vec<FormatToken> {
    fn push_literal(tokens: &mut Vec<FormatToken>, lit: &mut Vec<u8>) {
        if !lit.is_empty() {
            tokens.push(FormatToken::Literal(std::mem::take(lit)));
        }
    }

    let mut tokens = vec![];
    let mut lit = vec![];
    let mut it = fmt.bytes();

    while let Some(b) = it.next() {
        if b != b'%' {
            lit.push(b);
            continue;
        }

        // Unknown or incomplete specifiers are kept literally.
        let token = match it.next() {
            Some(b'%') => {
                lit.push(b'%');
                continue;
            }
            Some(b's') => match it.next() {
                Some(b'a') => FormatToken::ServerAddress,
                Some(b'h') => FormatToken::ServerHost,
                Some(b'p') => FormatToken::ServerPort,
                Some(b'n') => FormatToken::ServerName,
                other => {
                    lit.extend(b"%s".iter().copied().chain(other));
                    continue;
                }
            },
            Some(b'c') => match it.next() {
                Some(b'a') => FormatToken::ClientAddress,
                Some(b'h') => FormatToken::ClientHost,
                Some(b'p') => FormatToken::ClientPort,
                other => {
                    lit.extend(b"%c".iter().copied().chain(other));
                    continue;
                }
            },
            Some(b'S') => FormatToken::ClientServerShort,
            Some(b't') => FormatToken::Timestamp,
            Some(b'd') => FormatToken::Database,
            Some(b'C') => FormatToken::Command,
            Some(b'a') => FormatToken::Arguments,
            Some(b'l') => FormatToken::FullLine,
            other => {
                lit.extend(std::iter::once(b'%').chain(other));
                continue;
            }
        };

        push_literal(&mut tokens, &mut lit);
        tokens.push(token);
    }

    push_literal(&mut tokens, &mut lit);
    tokens
}

/// The fields shared by JSON and PHP output. `A` selects how arguments are
/// represented.
#[derive(Serialize)]
struct Structured<'r, A> {
    timestamp: f64,
    db: u64,
    addr: Addr<'r>,
    cmd: &'r str,
    args: A,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<StructuredSource<'r>>,
}

/// Source strings are prepared once at connection setup and borrowed here.
#[derive(Serialize)]
struct StructuredSource<'s> {
    address: Option<&'s str>,
    name: Option<&'s str>,
}

impl<'r, A> Structured<'r, A> {
    fn new(record: &'r Record<'r>, args: A) -> Self {
        Self {
            timestamp: record.timestamp(),
            db: record.db,
            addr: Addr(&record.client),
            cmd: record.cmd_str(),
            args,
            source: None,
        }
    }
}

/// The client address as text, rendered on the stack.
fn addr_text<'b>(
    client: &'b Client<'_>,
    buf: &'b mut [u8; MAX_TCP_ADDR_LEN],
) -> &'b str {
    match client {
        Client::Unix(path) => path,
        Client::Lua => "lua",
        Client::Unknown => "-",
        tcp @ Client::Tcp { .. } => tcp.render_tcp(buf).unwrap_or("-"),
    }
}

/// Serializes a client address without allocating.
struct Addr<'r>(&'r Client<'r>);

impl Serialize for Addr<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(addr_text(self.0, &mut [0; MAX_TCP_ADDR_LEN]))
    }
}

/// Arguments as strings, replacing invalid UTF-8.
struct TextArgs<'r>(&'r [Cow<'r, [u8]>]);

impl Serialize for TextArgs<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(self.0.iter().map(|arg| String::from_utf8_lossy(arg)))
    }
}

/// Arguments as byte strings, without copying them.
struct ByteArgs<'r>(&'r [Cow<'r, [u8]>]);

impl Serialize for ByteArgs<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(self.0.iter().map(|arg| SerBytes::new(arg)))
    }
}

fn resp_bulk(out: &mut Vec<u8>, bytes: &[u8]) -> io::Result<()> {
    out.write_all(b"$")?;
    write_uint(out, bytes.len() as u64)?;
    out.write_all(b"\r\n")?;
    out.write_all(bytes)?;
    out.write_all(b"\r\n")
}

/// A RESP array of bulk strings: the command, then its arguments.
fn write_resp(
    out: &mut Vec<u8>,
    record: &Record<'_>,
    args: &[Cow<'_, [u8]>],
) -> io::Result<()> {
    out.write_all(b"*")?;
    write_uint(out, 1 + args.len() as u64)?;
    out.write_all(b"\r\n")?;
    resp_bulk(out, record.cmd)?;
    args.iter().try_for_each(|arg| resp_bulk(out, arg))
}

/// `timestamp,db,addr,cmd` followed by one column per argument.
fn write_csv(
    out: &mut Vec<u8>,
    record: &Record<'_>,
    args: &[Cow<'_, [u8]>],
) -> io::Result<()> {
    write!(out, "{}", record.timestamp())?;
    out.push(b',');
    let mut db = [0; u64::FORMATTED_SIZE_DECIMAL];
    csv_field(out, lexical_core::write(record.db, &mut db));
    out.push(b',');
    csv_field(
        out,
        addr_text(&record.client, &mut [0; MAX_TCP_ADDR_LEN]).as_bytes(),
    );
    out.push(b',');
    csv_field(out, record.cmd);
    for arg in args {
        out.push(b',');
        csv_field(out, arg);
    }
    Ok(())
}

/// Write one RFC 4180 field, quoting it only when it contains a delimiter,
/// quote, or line break, and doubling embedded quotes.
fn csv_field(out: &mut Vec<u8>, field: &[u8]) {
    let needs_quotes = memchr::memchr3(b',', b'"', b'\n', field).is_some()
        || memchr::memchr(b'\r', field).is_some();
    if !needs_quotes {
        out.extend_from_slice(field);
        return;
    }
    out.push(b'"');
    for part in field.split_inclusive(|&b| b == b'"') {
        out.extend_from_slice(part);
        if part.ends_with(b"\"") {
            out.push(b'"');
        }
    }
    out.push(b'"');
}

#[cfg(test)]
mod tests {

    use super::{
        FormatToken, Formatter, OutputKind, Scratch, Source, compile_format,
        csv_field,
    };
    use crate::connection::ServerAddr;

    /// Portable release measurement, separate from correctness tests:
    /// `RUSTFLAGS='' cargo test --release --bin redis-monitor benchmark_json -- --ignored --nocapture`
    #[test]
    #[ignore = "release throughput measurement"]
    fn benchmark_json() {
        use std::{hint::black_box, time::Instant};

        let source = Source::new(
            &ServerAddr::from_tcp_addr("127.0.0.1", 6379),
            Some("primary".into()),
        );
        let large = format!(
            r#"1.5 [0 127.0.0.1:49152] "SET" "key" "{}""#,
            r#"value\"\x00"#.repeat(256)
        );
        for (label, line) in [
            ("short", &br#"1.5 [0 127.0.0.1:49152] "GET" "key""#[..]),
            ("large", large.as_bytes()),
        ] {
            for (include, format) in [
                (false, None),
                (true, None),
                (false, Some("%t %d %ca %C %a")),
                (false, Some("%C %a")),
            ] {
                let kind = OutputKind::Json;
                let formatter = Formatter::new(kind, format, include).unwrap();
                let mut out = Vec::new();
                let mut args = Scratch::default();
                let mut samples = Vec::new();
                for _ in 0..7 {
                    let start = Instant::now();
                    for _ in 0..100_000 {
                        out.clear();
                        formatter
                            .format(
                                &mut out,
                                &source,
                                black_box(line),
                                &mut args,
                            )
                            .unwrap();
                        black_box(&out);
                    }
                    samples
                        .push(start.elapsed().as_secs_f64() * 1e9 / 100_000.0);
                }
                samples.sort_by(f64::total_cmp);
                println!(
                    "{label} {kind:?} source={include} format={format:?}: {:.2} ns/record; {} bytes/record; samples {samples:.2?}",
                    samples[3],
                    out.len()
                );
            }
        }
    }

    fn render(
        kind: OutputKind,
        format: &str,
        server: &ServerAddr,
        input: &[u8],
    ) -> Result<Vec<u8>, super::FormatError> {
        let mut output = Vec::new();
        Formatter::new(
            kind,
            (kind == OutputKind::Plain).then_some(format),
            false,
        )
        .unwrap()
        .format(
            &mut output,
            &Source::new(server, None),
            input,
            &mut Scratch::default(),
        )?;
        Ok(output)
    }

    fn plain(format: &str, server: &ServerAddr, input: &[u8]) -> String {
        String::from_utf8(
            render(OutputKind::Plain, format, server, input).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn short_addresses_elide_matching_ipv4_and_ipv6_hosts() {
        let v4 = ServerAddr::from_tcp_addr("127.0.0.1", 6379);
        let v6 = ServerAddr::from_tcp_addr("2001:db8::1", 6379);
        assert_eq!(
            plain("%S", &v4, br#"1.0 [0 127.0.0.1:49152] "PING""#),
            "6379 49152\n"
        );
        assert_eq!(
            plain("%S", &v6, br#"1.0 [0 [2001:0db8::0:1]:49152] "PING""#),
            "6379 49152\n"
        );
    }

    #[test]
    fn short_addresses_preserve_full_addresses_for_nonmatching_hosts() {
        let cases = [
            (
                ServerAddr::from_tcp_addr("127.0.0.1", 6379),
                &br#"1.0 [0 127.0.0.2:49152] "PING""#[..],
                "127.0.0.1:6379 127.0.0.2:49152\n",
            ),
            (
                ServerAddr::from_tcp_addr("redis.example", 6379),
                br#"1.0 [0 127.0.0.1:49152] "PING""#,
                "redis.example:6379 127.0.0.1:49152\n",
            ),
            (
                ServerAddr::from_path("/run/redis/server.sock"),
                br#"1.0 [0 unix:/run/redis/client.sock] "PING""#,
                "/run/redis/server.sock /run/redis/client.sock\n",
            ),
            (
                ServerAddr::from_tcp_addr("::1", 6379),
                br#"1.0 [0 [::2]:1] "PING""#,
                "[::1]:6379 [::2]:1\n",
            ),
        ];

        for (server, input, expected) in cases {
            assert_eq!(plain("%S", &server, input), expected);
        }
    }

    #[test]
    fn server_tokens_use_the_source() {
        let server = ServerAddr::from_tcp_addr("127.0.0.1", 6379);
        let input = br#"1.0 [0 127.0.0.1:49152] "PING""#;
        let mut output = Vec::new();
        Formatter::new(OutputKind::Plain, Some("%sa|%sh|%sp|%sn"), false)
            .unwrap()
            .format(
                &mut output,
                &Source::new(&server, Some("primary".into())),
                input,
                &mut Scratch::default(),
            )
            .unwrap();
        assert_eq!(output, b"127.0.0.1:6379|127.0.0.1|6379|primary\n");

        let server = ServerAddr::from_path("/run/redis/server.sock");
        assert_eq!(
            plain("%sp|%sn|%ch|%cp", &server, input),
            "server.sock|-|127.0.0.1|49152\n"
        );
    }

    #[test]
    fn json_source_preserves_identity_and_legacy_fields() {
        let cases = [
            (
                Source::new(
                    &ServerAddr::from_tcp_addr("redis.example", 6379),
                    Some("primary\"\n東京".into()),
                ),
                serde_json::json!({"address": "redis.example:6379", "name": "primary\"\n東京"}),
            ),
            (
                Source::new(&ServerAddr::from_tcp_addr("::1", 6380), None),
                serde_json::json!({"address": "[::1]:6380", "name": null}),
            ),
            (
                Source::new(
                    &ServerAddr::from_path("/tmp/redis\"\\socket"),
                    Some(String::new()),
                ),
                serde_json::json!({"address": "/tmp/redis\"\\socket", "name": ""}),
            ),
            // A server socket named "stdin" is distinct from unknown identity.
            (
                Source::new(&ServerAddr::from_path("stdin"), None),
                serde_json::json!({"address": "stdin", "name": null}),
            ),
            (
                Source::from_reader("stdin"),
                serde_json::json!({"address": null, "name": null}),
            ),
        ];
        let input = br#"1.5 [2 127.0.0.1:49152] "SET" "key" "\xff\"\n""#;
        for (source, expected) in cases {
            let mut out = Vec::new();
            let mut args = Scratch::default();
            Formatter::new(OutputKind::Json, None, true)
                .unwrap()
                .format(&mut out, &source, input, &mut args)
                .unwrap();
            assert_eq!(memchr::memchr_iter(b'\n', &out).count(), 1);
            let mut value: serde_json::Value =
                serde_json::from_slice(&out).unwrap();
            assert_eq!(
                value.as_object_mut().unwrap().remove("source"),
                Some(expected)
            );
            assert_eq!(value["addr"], "127.0.0.1:49152");
            out.clear();
            Formatter::new(OutputKind::Json, None, false)
                .unwrap()
                .format(&mut out, &source, input, &mut args)
                .unwrap();
            assert_eq!(
                value,
                serde_json::from_slice::<serde_json::Value>(&out).unwrap()
            );
        }
    }

    #[test]
    fn selected_fields_render_addresses_and_raw_command_text() {
        let source = Source::new(
            &ServerAddr::from_tcp_addr("::1", 6379),
            Some("primary".into()),
        );
        let formatter = Formatter::new(
            OutputKind::Json,
            Some("%sa %sn %sh %sp %ca %ch %cp %S %l"),
            false,
        )
        .unwrap();
        let mut out = Vec::new();
        // Noncanonical whitespace exercises the full-command fallback.
        formatter
            .format(
                &mut out,
                &source,
                b"1.5 [2 [::1]:49152] \"GET\"  \"a\"",
                &mut Scratch::default(),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&out).unwrap(),
            serde_json::json!({
                "server_address": "[::1]:6379", "server_name": "primary", "server_host": "::1", "server_port": "6379",
                "addr": "[::1]:49152", "client_host": "::1", "client_port": "49152", "addresses": "6379 49152", "line": "\"GET\" \"a\""
            })
        );
        out.clear();
        formatter
            .format(
                &mut out,
                &Source::from_reader("stdin"),
                b"1.5 [0 lua] \"PING\"",
                &mut Scratch::default(),
            )
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        for key in [
            "server_address",
            "server_name",
            "server_host",
            "server_port",
        ] {
            assert!(value[key].is_null(), "{key}");
        }
    }

    #[test]
    fn source_metadata_is_escaped_in_php_csv_and_resp() {
        let source = Source::new(
            &ServerAddr::from_path("/tmp/a,b"),
            Some("a\"b".into()),
        );
        let input = br#"1.0 [0 lua] "PING""#;
        for format in [None, Some("%C")] {
            for kind in [OutputKind::Csv, OutputKind::Php, OutputKind::Resp] {
                let mut out = Vec::new();
                Formatter::new(kind, format, true)
                    .unwrap()
                    .format(&mut out, &source, input, &mut Scratch::default())
                    .unwrap();
                match kind {
                    OutputKind::Csv => assert!(out.starts_with(b"\"/tmp/a,b\",\"a\"\"b\",")),
                    OutputKind::Php => assert!(out.ends_with(b"s:6:\"source\";a:2:{s:7:\"address\";s:8:\"/tmp/a,b\";s:4:\"name\";s:3:\"a\"b\";}}\n")),
                    OutputKind::Resp => {
                        let redis::Value::Array(envelope) = redis::parse_redis_value(&out).unwrap() else { panic!("not an envelope"); };
                        assert_eq!(envelope[0], redis::Value::Array(vec![redis::Value::BulkString(b"/tmp/a,b".to_vec()), redis::Value::BulkString(b"a\"b".to_vec())]));
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    #[test]
    fn format_percent_escapes_and_unknown_specifiers_are_literal() {
        assert_eq!(
            compile_format("%%|%%t|%x|%s|%sx|%c|%"),
            [FormatToken::Literal(b"%|%t|%x|%s|%sx|%c|%".to_vec())]
        );
        assert_eq!(
            compile_format("a%tb"),
            [
                FormatToken::Literal(b"a".to_vec()),
                FormatToken::Timestamp,
                FormatToken::Literal(b"b".to_vec()),
            ]
        );
    }

    #[test]
    fn fast_paths_match_the_general_formatter() {
        let server = ServerAddr::from_tcp_addr("127.0.0.1", 6379);
        let inputs: &[&[u8]] = &[
            br#"1783484211.311904 [3 127.0.0.1:49152] "SET" "key" "value""#,
            br#"1783484211.300000 [0 127.0.0.1:1] "PING""#,
            br#"1783484211.300000 [0 127.0.0.1:1] "PING" "#,
            br#"0001.5 [007 010.0.0.1:0080] "GET"  "k""#,
            br#"1.5 [0 lua] "GET" "k""#,
            br#"1.5 [0 ] "GET" "k""#,
            br#"1.5 [0 [::1]:5] "GET" "k""#,
            br#"1.5 [0 unix:/tmp/sock] "GET" "k""#,
            b"1.5\t[0\t1.2.3.4:5]\t\"GET\"\t\"k\"",
        ];
        for input in inputs {
            for fast in ["%l", "%t [%d %ca] %l"] {
                // A trailing "%%" defeats the fast path; strip what it adds.
                let general = plain(&format!("{fast}%%"), &server, input);
                let general = general.strip_suffix("%\n").unwrap();
                assert_eq!(
                    plain(fast, &server, input),
                    format!("{general}\n"),
                    "{fast} for {}",
                    String::from_utf8_lossy(input)
                );
            }
        }
    }

    #[test]
    fn malformed_records_fail_without_writing() {
        let server = ServerAddr::from_tcp_addr("127.0.0.1", 6379);
        let malformed: &[&[u8]] = &[
            b"",
            br#"x.0 [0 127.0.0.1:1] "PING""#,
            br#"18446744073709551616.0 [0 127.0.0.1:1] "PING""#,
            br#"1.0 [18446744073709551616 127.0.0.1:1] "PING""#,
            br#"1.0 [0 256.0.0.1:1] "PING""#,
            br#"1.0 [0 127.0.0.1:65536] "PING""#,
            br#"1.0 [0 [not-ip]:1] "PING""#,
            br#"1.0 [0 127.0.0.1:1] "BAD CMD""#,
            br#"1.0 [0 127.0.0.1:1] "BAD\"CMD""#,
            b"1.0 [0 127.0.0.1:1] \"PING",
        ];

        for kind in [
            OutputKind::Plain,
            OutputKind::Json,
            OutputKind::Csv,
            OutputKind::Resp,
            OutputKind::Php,
        ] {
            for (format, include) in
                [(None, false), (None, true), (Some("%C %a"), true)]
            {
                for input in malformed {
                    let mut output = b"previous".to_vec();
                    assert!(
                        Formatter::new(kind, format, include)
                            .unwrap()
                            .format(
                                &mut output,
                                &Source::new(&server, None),
                                input,
                                &mut Scratch::default(),
                            )
                            .is_err()
                    );
                    assert_eq!(output, b"previous", "{kind:?}");
                }
            }
        }
    }

    #[test]
    fn invalid_record_messages_are_bounded() {
        let server = ServerAddr::from_tcp_addr("127.0.0.1", 6379);
        let input = vec![b'x'; 100_000];
        let error = render(OutputKind::Plain, "%l", &server, &input)
            .unwrap_err()
            .to_string();
        assert!(error.len() < 400, "{error}");
        assert!(error.contains("(100000 bytes"), "{error}");
    }

    #[test]
    fn plain_output_keeps_raw_argument_text() {
        let server = ServerAddr::from_tcp_addr("127.0.0.1", 6379);
        // Only structured output validates arguments.
        assert_eq!(
            plain("%l", &server, br#"1.0 [0 127.0.0.1:1] "SET" "unterminated"#),
            "\"SET\" \"unterminated\n"
        );
        // Arguments are written as the input bytes, like `%l`.
        assert_eq!(
            render(
                OutputKind::Plain,
                "%C|%a",
                &server,
                b"1.0 [0 127.0.0.1:1] \"FT.SEARCH\" \"idx\" \"\xff\""
            )
            .unwrap(),
            b"FT.SEARCH|\"idx\" \"\xff\"\n"
        );
    }

    #[test]
    fn scratch_arguments_are_reused_across_records() {
        let source = Source::new(&ServerAddr::from_path("stdin"), None);
        let inputs: [&[u8]; 2] = [
            br#"1.0 [0 127.0.0.1:1] "SET" "a" "b" "c""#,
            br#"1.0 [0 127.0.0.1:1] "GET" "k""#,
        ];
        let formatter = Formatter::new(OutputKind::Resp, None, false).unwrap();
        let mut output = Vec::new();
        let mut args = Scratch::default();
        for input in inputs {
            formatter
                .format(&mut output, &source, input, &mut args)
                .unwrap();
        }
        assert_eq!(
            output,
            b"*4\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n\
              *2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
        );
    }

    #[test]
    fn csv_fields_are_quoted_only_when_necessary() {
        for (field, expected) in [
            (&b"plain"[..], &b"plain"[..]),
            (b"", b""),
            (b"a,b", b"\"a,b\""),
            (b"say \"hi\"", b"\"say \"\"hi\"\"\""),
            (b"\"", b"\"\"\"\""),
            (b"line\nbreak", b"\"line\nbreak\""),
            (b"cr\r", b"\"cr\r\""),
            (b"tab\tand space", b"tab\tand space"),
            (b"\xff\x00", b"\xff\x00"),
        ] {
            let mut out = Vec::new();
            csv_field(&mut out, field);
            assert_eq!(out, expected, "{}", String::from_utf8_lossy(field));
        }
    }
}
