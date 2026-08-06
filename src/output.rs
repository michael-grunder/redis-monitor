use std::{io::Write, str::FromStr};

use anyhow::{Error, Result, anyhow};
use serde::{Serialize, Serializer, ser::SerializeStruct};
use serde_bytes::{ByteBuf as SerByteBuf, Bytes as SerBytes};
use serde_php as php;

use crate::{
    connection::{GetHost, ServerAddr},
    monitor::{
        ClientAddr, ClientAddrView, Line, LineArgs, LineView, ParsePlan,
    },
    stats::CommandStat,
};

struct PhpLine<'a>(&'a Line<'a>);

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OutputKind {
    Plain,
    Json,
    Csv,
    Resp,
    Php,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Copy, Clone)]
enum FastFormat {
    None,
    FullLine,
    DefaultSingle,
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

impl Serialize for PhpLine<'_> {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let l = self.0;
        let mut st = s.serialize_struct("Line", 5)?;
        st.serialize_field("timestamp", &l.timestamp)?;
        st.serialize_field("db", &l.db)?;
        st.serialize_field("addr", &l.addr)?;
        st.serialize_field("cmd", &l.cmd)?;

        match &l.args {
            LineArgs::Parsed(v) => {
                // Vec<Vec<u8>> -> Vec<ByteBuf>
                let vb: Vec<SerByteBuf> = v
                    .iter()
                    .map(|b| SerByteBuf::from(b.clone().into_owned()))
                    .collect();
                st.serialize_field("args", &vb)?;
            }
            LineArgs::Raw(raw) => {
                // Borrowed bytes are fine as &Bytes.
                st.serialize_field("args", &SerBytes::new(raw))?;
            }
        }

        st.end()
    }
}
impl OutputKind {
    pub fn get_writer<'a, W: Write + 'a>(
        self,
        writer: W,
        format: &str,
    ) -> Box<dyn OutputHandler + 'a> {
        match self {
            Self::Plain => Box::new(PlainWriter::new(writer, format)),
            Self::Csv => Box::new(CsvWriter {
                writer: csv::WriterBuilder::new()
                    .flexible(true)
                    .from_writer(writer),
            }),
            Self::Json => Box::new(JsonWriter { writer }),
            Self::Php => Box::new(PhpWriter { writer }),
            Self::Resp => Box::new(RespWriter { writer }),
        }
    }
}

pub trait OutputHandler {
    fn write_raw_line(
        &mut self,
        server: &ServerAddr,
        name: Option<&str>,
        input: &[u8],
    ) -> Result<()> {
        let (_, line) = Line::from_line_bytes(input, true)
            .map_err(|error| invalid_line(input, error))?;
        self.write_line(server, name, &line)
    }

    fn write_line(
        &mut self,
        server: &ServerAddr,
        name: Option<&str>,
        line: &Line,
    ) -> Result<()>;

    fn write_stats(&mut self, stats: &[CommandStat]) -> Result<()> {
        eprintln!(
            "[stats]: {}",
            stats
                .iter()
                .filter_map(|s| {
                    if s.count > 0 {
                        Some(format!("{}=[{}, {}]", s.name, s.count, s.bytes))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join(", ")
        );

        Ok(())
    }

    fn flush(&mut self) -> Result<()>;
}

#[derive(Debug)]
struct PlainWriter<W: Write> {
    writer: W,
    format: Vec<FormatToken>,
    parse_plan: ParsePlan,
    fast_format: FastFormat,
}

#[derive(Debug)]
struct CsvWriter<W: Write> {
    writer: csv::Writer<W>,
}

#[derive(Debug)]
struct JsonWriter<W: Write> {
    writer: W,
}

#[derive(Debug)]
struct RespWriter<W: Write> {
    writer: W,
}

#[derive(Debug)]
struct PhpWriter<W: Write> {
    writer: W,
}

impl<W: Write> OutputHandler for PlainWriter<W> {
    fn write_raw_line(
        &mut self,
        server: &ServerAddr,
        name: Option<&str>,
        input: &[u8],
    ) -> Result<()> {
        let (_, line) = LineView::from_line_bytes(input, self.parse_plan)
            .map_err(|error| invalid_line(input, error))?;
        self.write_view(server, name, &line)
    }

    fn write_line(
        &mut self,
        server: &ServerAddr,
        name: Option<&str>,
        line: &Line,
    ) -> Result<()> {
        let (w, format) = (&mut self.writer, &self.format);

        for f in format {
            match f {
                FormatToken::Literal(v) => w.write_all(v)?,
                FormatToken::ClientServerShort => {
                    Self::w_client_server_short(w, server, &line.addr)?;
                }
                FormatToken::ServerName => {
                    w.write_all(name.unwrap_or("-").as_bytes())?;
                }
                FormatToken::ServerAddress => write!(w, "{server}")?,
                FormatToken::ServerHost => {
                    w.write_all(server.get_host().as_bytes())?;
                }
                FormatToken::ServerPort => {
                    Self::w_server_port(w, server)?;
                }
                FormatToken::ClientAddress => write!(w, "{}", line.addr)?,
                FormatToken::ClientHost => {
                    Self::w_client_host(w, &line.addr)?;
                }
                FormatToken::ClientPort => {
                    Self::w_client_port(w, &line.addr)?;
                }
                FormatToken::Timestamp => {
                    write!(w, "{}", line.timestamp)?;
                }
                FormatToken::Database => write!(w, "{}", line.db)?,
                FormatToken::Command => write!(w, "{}", line.cmd)?,
                FormatToken::Arguments => write!(w, "{}", line.args)?,
                FormatToken::FullLine => {
                    w.write_all(b"\"")?;
                    w.write_all(line.cmd.as_bytes())?;
                    w.write_all(b"\"")?;

                    match &line.args {
                        LineArgs::Raw(s) => {
                            w.write_all(b" ")?;
                            w.write_all(s)?;
                        }
                        LineArgs::Parsed(v) => {
                            for arg in v {
                                w.write_all(b" \"")?;
                                w.write_all(arg.as_ref())?;
                                w.write_all(b"\"")?;
                            }
                        }
                    }
                }
            }
        }

        self.writer.write_all(b"\n")?;

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush().map_err(|e| anyhow!(e))
    }
}

impl<W: Write> PlainWriter<W> {
    fn push_literal(v: &mut Vec<FormatToken>, lit: &mut Vec<u8>) {
        if !lit.is_empty() {
            v.push(FormatToken::Literal(std::mem::take(lit)));
        }
    }

    fn compile_format(fmt: &str) -> (Vec<FormatToken>, ParsePlan) {
        let mut res = vec![];
        let mut lit = vec![];

        let mut it = fmt.as_bytes().iter().copied().peekable();

        while let Some(b) = it.next() {
            if b != b'%' {
                lit.push(b);
                continue;
            }
            if it.peek() == Some(&b'%') {
                lit.push(b'%');
                continue;
            }

            Self::push_literal(&mut res, &mut lit);

            let o = match it.next() {
                Some(b's') => match it.next() {
                    Some(b'a') => FormatToken::ServerAddress,
                    Some(b'h') => FormatToken::ServerHost,
                    Some(b'p') => FormatToken::ServerPort,
                    Some(b'n') => FormatToken::ServerName,
                    Some(x) => {
                        lit.extend_from_slice(&[b'%', b's', x]);
                        continue;
                    }
                    None => {
                        lit.extend_from_slice(b"%s");
                        continue;
                    }
                },
                Some(b'c') => match it.next() {
                    Some(b'a') => FormatToken::ClientAddress,
                    Some(b'h') => FormatToken::ClientHost,
                    Some(b'p') => FormatToken::ClientPort,
                    Some(x) => {
                        lit.extend_from_slice(&[b'%', b'c', x]);
                        continue;
                    }
                    None => {
                        lit.extend_from_slice(b"%c");
                        continue;
                    }
                },
                Some(b'S') => FormatToken::ClientServerShort,
                Some(b't') => FormatToken::Timestamp,
                Some(b'd') => FormatToken::Database,
                Some(b'C') => FormatToken::Command,
                Some(b'a') => FormatToken::Arguments,
                Some(b'l') => FormatToken::FullLine,
                Some(x) => {
                    lit.extend_from_slice(&[b'%', x]);
                    continue;
                }
                None => {
                    lit.push(b'%');
                    continue;
                }
            };

            res.push(o);
        }

        Self::push_literal(&mut res, &mut lit);

        let parse_plan = ParsePlan {
            client_ip: res
                .iter()
                .any(|token| matches!(token, FormatToken::ClientServerShort)),
            timestamp: res
                .iter()
                .any(|token| matches!(token, FormatToken::Timestamp)),
        };

        (res, parse_plan)
    }

    fn write_view(
        &mut self,
        server: &ServerAddr,
        name: Option<&str>,
        line: &LineView<'_>,
    ) -> Result<()> {
        let (writer, format) = (&mut self.writer, &self.format);

        match self.fast_format {
            FastFormat::FullLine => {
                if let Some(full_line) = line.full_line {
                    writer.write_all(full_line)?;
                    writer.write_all(b"\n")?;
                    return Ok(());
                }
            }
            FastFormat::DefaultSingle => {
                if let Some(tail) = line.single_default_tail {
                    Self::w_timestamp_view(writer, line)?;
                    writer.write_all(b" ")?;
                    writer.write_all(tail)?;
                    writer.write_all(b"\n")?;
                    return Ok(());
                }
            }
            FastFormat::None => {}
        }

        for token in format {
            match token {
                FormatToken::Literal(bytes) => writer.write_all(bytes)?,
                FormatToken::ClientServerShort => {
                    Self::w_client_server_short_view(
                        writer, server, &line.addr,
                    )?;
                }
                FormatToken::ServerName => {
                    writer.write_all(name.unwrap_or("-").as_bytes())?;
                }
                FormatToken::ServerAddress => write!(writer, "{server}")?,
                FormatToken::ServerHost => {
                    writer.write_all(server.get_host().as_bytes())?;
                }
                FormatToken::ServerPort => {
                    Self::w_server_port(writer, server)?;
                }
                FormatToken::ClientAddress => {
                    Self::w_client_address_view(writer, &line.addr)?;
                }
                FormatToken::ClientHost => {
                    Self::w_client_host_view(writer, &line.addr)?;
                }
                FormatToken::ClientPort => {
                    Self::w_client_port_view(writer, &line.addr)?;
                }
                FormatToken::Timestamp => {
                    Self::w_timestamp_view(writer, line)?;
                }
                FormatToken::Database => {
                    Self::w_uint_bytes(writer, line.db)?;
                }
                FormatToken::Command => {
                    writer.write_all(line.cmd.as_bytes())?;
                }
                FormatToken::Arguments => writer
                    .write_all(String::from_utf8_lossy(line.args).as_bytes())?,
                FormatToken::FullLine => {
                    writer.write_all(b"\"")?;
                    writer.write_all(line.cmd.as_bytes())?;
                    writer.write_all(b"\" ")?;
                    writer.write_all(line.args)?;
                }
            }
        }

        self.writer.write_all(b"\n")?;
        Ok(())
    }

    fn w_client_server_short(
        writer: &mut W,
        server: &ServerAddr,
        client: &ClientAddr,
    ) -> Result<()> {
        if let ServerAddr::Tcp(_, sport, Some(server_ip)) = server
            && let ClientAddr::Tcp(chost, cport) = client
            && server_ip == chost
        {
            write!(writer, "{sport} {cport}")?;
            return Ok(());
        }

        write!(writer, "{server} {client}")?;

        Ok(())
    }

    fn w_client_server_short_view(
        writer: &mut W,
        server: &ServerAddr,
        client: &ClientAddrView<'_>,
    ) -> Result<()> {
        if let ServerAddr::Tcp(_, server_port, Some(server_ip)) = server
            && let ClientAddrView::Tcp {
                port,
                ip: Some(client_ip),
                ..
            } = client
            && server_ip == client_ip
        {
            write!(writer, "{server_port} ")?;
            Self::w_uint_bytes(writer, port)?;
            return Ok(());
        }

        write!(writer, "{server} ")?;
        Self::w_client_address_view(writer, client)
    }

    fn w_client_address_view(
        writer: &mut W,
        client: &ClientAddrView<'_>,
    ) -> Result<()> {
        match client {
            ClientAddrView::Path(path) => writer.write_all(path.as_bytes())?,
            ClientAddrView::Tcp {
                host,
                port,
                ip,
                bracketed,
            } => {
                if *bracketed {
                    if let Some(ip) = ip {
                        write!(writer, "{ip}")?;
                    } else {
                        writer.write_all(host)?;
                    }
                } else {
                    Self::w_ipv4_host(writer, host)?;
                }
                writer.write_all(b":")?;
                Self::w_uint_bytes(writer, port)?;
            }
            ClientAddrView::Lua => writer.write_all(b"lua")?,
            ClientAddrView::Unknown => writer.write_all(b"-")?,
        }
        Ok(())
    }

    fn w_client_host_view(
        writer: &mut W,
        client: &ClientAddrView<'_>,
    ) -> Result<()> {
        match client {
            ClientAddrView::Tcp {
                host,
                ip,
                bracketed,
                ..
            } => {
                if *bracketed {
                    if let Some(ip) = ip {
                        write!(writer, "{ip}")?;
                    } else {
                        writer.write_all(host)?;
                    }
                } else {
                    Self::w_ipv4_host(writer, host)?;
                }
            }
            ClientAddrView::Path(_)
            | ClientAddrView::Lua
            | ClientAddrView::Unknown => writer.write_all(b"-")?,
        }
        Ok(())
    }

    fn w_client_port_view(
        writer: &mut W,
        client: &ClientAddrView<'_>,
    ) -> Result<()> {
        match client {
            ClientAddrView::Tcp { port, .. } => {
                Self::w_uint_bytes(writer, port)?;
            }
            ClientAddrView::Path(path) => writer.write_all(
                path.rsplit('/')
                    .next()
                    .filter(|name| !name.is_empty())
                    .unwrap_or("-")
                    .as_bytes(),
            )?,
            ClientAddrView::Lua => writer.write_all(b"lua")?,
            ClientAddrView::Unknown => writer.write_all(b"-")?,
        }
        Ok(())
    }

    fn w_uint_bytes(writer: &mut W, bytes: &[u8]) -> Result<()> {
        let first_nonzero = bytes
            .iter()
            .position(|byte| *byte != b'0')
            .unwrap_or_else(|| bytes.len().saturating_sub(1));
        writer.write_all(&bytes[first_nonzero..])?;
        Ok(())
    }

    fn w_ipv4_host(writer: &mut W, host: &[u8]) -> Result<()> {
        let mut octets = host.split(|byte| *byte == b'.').peekable();
        while let Some(octet) = octets.next() {
            Self::w_uint_bytes(writer, octet)?;
            if octets.peek().is_some() {
                writer.write_all(b".")?;
            }
        }
        Ok(())
    }

    fn w_timestamp(writer: &mut W, timestamp: &[u8]) -> Result<()> {
        let Some(dot) = timestamp.iter().position(|byte| *byte == b'.') else {
            writer.write_all(timestamp)?;
            return Ok(());
        };
        let (integer, fraction_with_dot) = timestamp.split_at(dot);
        let fraction = &fraction_with_dot[1..];
        Self::w_uint_bytes(writer, integer)?;

        if let Some(last_nonzero) = fraction.iter().rposition(|b| *b != b'0') {
            writer.write_all(b".")?;
            writer.write_all(&fraction[..=last_nonzero])?;
        }
        Ok(())
    }

    fn w_timestamp_view(writer: &mut W, line: &LineView<'_>) -> Result<()> {
        if let Some(timestamp) = line.typed_timestamp {
            write!(writer, "{timestamp}")?;
            Ok(())
        } else {
            Self::w_timestamp(writer, line.timestamp)
        }
    }

    fn w_server_port(writer: &mut W, server: &ServerAddr) -> Result<()> {
        match server {
            ServerAddr::Tcp(_, port, _) => write!(writer, "{port}")?,
            ServerAddr::Unix(path) => writer.write_all(
                path.rsplit('/').next().unwrap_or(path).as_bytes(),
            )?,
        }
        Ok(())
    }

    fn w_client_host(writer: &mut W, client: &ClientAddr) -> Result<()> {
        match client {
            ClientAddr::Tcp(ip, _) => write!(writer, "{ip}")?,
            ClientAddr::Path(_) | ClientAddr::Lua | ClientAddr::Unknown => {
                writer.write_all(b"-")?;
            }
        }
        Ok(())
    }

    fn w_client_port(writer: &mut W, client: &ClientAddr) -> Result<()> {
        match client {
            ClientAddr::Tcp(_, port) => write!(writer, "{port}")?,
            ClientAddr::Path(path) => writer.write_all(
                path.rsplit('/')
                    .next()
                    .filter(|s| !s.is_empty())
                    .unwrap_or("-")
                    .as_bytes(),
            )?,
            ClientAddr::Lua => writer.write_all(b"lua")?,
            ClientAddr::Unknown => writer.write_all(b"-")?,
        }
        Ok(())
    }

    fn new(writer: W, format: &str) -> Self {
        let (format, parse_plan) = Self::compile_format(format);
        let fast_format = match format.as_slice() {
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
        Self {
            writer,
            format,
            parse_plan,
            fast_format,
        }
    }
}

fn invalid_line(input: &[u8], error: impl std::fmt::Display) -> anyhow::Error {
    let line = String::from_utf8_lossy(input);
    anyhow!("Failed to parse line '{line}' ({error})")
}

impl<W: Write> OutputHandler for CsvWriter<W> {
    fn write_line(
        &mut self,
        _server: &ServerAddr,
        _name: Option<&str>,
        line: &Line,
    ) -> Result<()> {
        self.writer.serialize(line)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush().map_err(|e| anyhow!(e))
    }
}

impl<W: Write> OutputHandler for JsonWriter<W> {
    fn write_line(
        &mut self,
        _server: &ServerAddr,
        _name: Option<&str>,
        parsed: &Line,
    ) -> Result<()> {
        serde_json::to_writer(&mut self.writer, parsed)?;
        writeln!(&mut self.writer)?;
        Ok(())
    }

    fn write_stats(&mut self, stats: &[CommandStat]) -> Result<()> {
        let data = serde_json::to_value(stats)
            .map_err(|e| anyhow!("Failed to serialize stats to JSON: {e}"))?;

        self.writer.write_all(data.to_string().as_bytes())?;
        self.writer.write_all(b"\n")?;

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush().map_err(|e| anyhow!(e))
    }
}

impl<W: Write> OutputHandler for RespWriter<W> {
    fn write_line(
        &mut self,
        _server: &ServerAddr,
        _name: Option<&str>,
        parsed: &Line,
    ) -> Result<()> {
        parsed.write_resp(&mut self.writer)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush().map_err(|e| anyhow!(e))
    }
}

impl<W: Write> OutputHandler for PhpWriter<W> {
    fn write_line(
        &mut self,
        _server: &ServerAddr,
        _name: Option<&str>,
        parsed: &Line,
    ) -> Result<()> {
        let buf = php::to_vec(&PhpLine(parsed))?;
        self.writer.write_all(&buf)?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }

    fn write_stats(&mut self, stats: &[CommandStat]) -> Result<()> {
        let buf = php::to_vec(stats)?;
        self.writer.write_all(&buf)?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush().map_err(|e| anyhow!(e))
    }
}

#[cfg(test)]
mod tests {
    use std::{hint::black_box, io::Write, net::IpAddr};

    use super::{OutputHandler, PlainWriter};
    use crate::{
        connection::ServerAddr,
        monitor::{ClientAddr, Line, LineArgs},
    };

    fn render(
        format: &str,
        server: &ServerAddr,
        client: ClientAddr<'_>,
    ) -> String {
        let line = Line::new(0.0, 0, client, "PING", LineArgs::Raw(b""));
        let mut output = Vec::new();
        PlainWriter::new(&mut output, format)
            .write_line(server, None, &line)
            .unwrap();
        String::from_utf8(output).unwrap()
    }

    fn render_raw(
        format: &str,
        server: &ServerAddr,
        name: Option<&str>,
        input: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        let mut output = Vec::new();
        PlainWriter::new(&mut output, format)
            .write_raw_line(server, name, input)?;
        Ok(output)
    }

    fn ip(address: &str) -> IpAddr {
        address.parse().unwrap()
    }

    #[derive(Default)]
    struct ByteCounter(usize);

    impl Write for ByteCounter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 += black_box(bytes.len());
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn short_addresses_elide_matching_ipv4_and_ipv6_hosts() {
        let cases = [
            ("127.0.0.1", "127.0.0.1", "6379 49152\n"),
            ("2001:db8::1", "2001:db8::1", "6379 49152\n"),
        ];

        for (server_host, client_host, expected) in cases {
            let server = ServerAddr::from_tcp_addr(server_host, 6379);
            let client = ClientAddr::from_addr(ip(client_host), 49152);
            assert_eq!(render("%S", &server, client), expected);
        }
    }

    #[test]
    fn short_addresses_preserve_full_addresses_for_nonmatching_hosts() {
        let cases = [
            (
                ServerAddr::from_tcp_addr("127.0.0.1", 6379),
                ClientAddr::from_addr(ip("127.0.0.2"), 49152),
                "127.0.0.1:6379 127.0.0.2:49152\n",
            ),
            (
                ServerAddr::from_tcp_addr("redis.example", 6379),
                ClientAddr::from_addr(ip("127.0.0.1"), 49152),
                "redis.example:6379 127.0.0.1:49152\n",
            ),
            (
                ServerAddr::from_path("/run/redis/server.sock"),
                ClientAddr::from_path("/run/redis/client.sock"),
                "/run/redis/server.sock /run/redis/client.sock\n",
            ),
        ];

        for (server, client, expected) in cases {
            assert_eq!(render("%S", &server, client), expected);
        }
    }

    #[test]
    fn address_components_are_written_without_temporary_strings() {
        let server = ServerAddr::from_tcp_addr("127.0.0.1", 6379);
        let client = ClientAddr::from_addr(ip("127.0.0.1"), 49152);
        assert_eq!(
            render("%sp %ch %cp", &server, client),
            "6379 127.0.0.1 49152\n"
        );

        let server = ServerAddr::from_path("/run/redis/server.sock");
        let client = ClientAddr::from_path("/run/redis/client.sock");
        assert_eq!(
            render("%sp %ch %cp", &server, client),
            "server.sock - client.sock\n"
        );
    }

    #[test]
    fn byte_view_matches_typed_output_for_every_format_token() {
        let input =
            br#"1783484211.311904 [3 127.0.0.1:49152] "SET" "key" "value""#;
        let (_, line) = Line::from_line_bytes(input, false).unwrap();
        let server = ServerAddr::from_tcp_addr("127.0.0.1", 6379);
        let formats = [
            "literal",
            "%%",
            "%S",
            "%sa",
            "%sh",
            "%sp",
            "%sn",
            "%ca",
            "%ch",
            "%cp",
            "%t",
            "%d",
            "%C",
            "%a",
            "%l",
            "%t [%d %ca] %l",
            "%t [%S %d] %l",
        ];

        for format in formats {
            let mut expected = Vec::new();
            PlainWriter::new(&mut expected, format)
                .write_line(&server, Some("primary"), &line)
                .unwrap();
            assert_eq!(
                render_raw(format, &server, Some("primary"), input).unwrap(),
                expected,
                "format {format}"
            );
        }
    }

    #[test]
    fn byte_view_matches_typed_client_address_variants() {
        let server = ServerAddr::from_tcp_addr("2001:db8::1", 6379);
        let inputs: &[&[u8]] = &[
            br#"1783484211.311904 [0 [2001:0db8:0:0:0:0:0:1]:49152] "PING""#,
            br#"1783484211.311904 [0 unix:/run/redis/client.sock] "PING""#,
            br#"1783484211.311904 [0 lua] "PING""#,
            br#"1783484211.311904 [0 ] "PING""#,
        ];

        for input in inputs {
            let (_, line) = Line::from_line_bytes(input, false).unwrap();
            let mut expected = Vec::new();
            PlainWriter::new(&mut expected, "%S|%ca|%ch|%cp")
                .write_line(&server, None, &line)
                .unwrap();
            assert_eq!(
                render_raw("%S|%ca|%ch|%cp", &server, None, input).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn byte_view_matches_typed_numeric_canonicalization() {
        let input = br#"0000000001.230000 [0003 001.002.003.004:00005] "PING""#;
        let server = ServerAddr::from_tcp_addr("1.2.3.4", 6379);
        let (_, line) = Line::from_line_bytes(input, false).unwrap();
        let format = "%t|%d|%S|%ca|%ch|%cp";
        let mut expected = Vec::new();
        PlainWriter::new(&mut expected, format)
            .write_line(&server, None, &line)
            .unwrap();

        assert_eq!(render_raw(format, &server, None, input).unwrap(), expected);

        for input in [
            &br#"0.10000000000000001 [0 1.2.3.4:5] "PING""#[..],
            &br#"12345678901.123456 [0 1.2.3.4:5] "PING""#[..],
        ] {
            let (_, line) = Line::from_line_bytes(input, false).unwrap();
            let mut expected = Vec::new();
            PlainWriter::new(&mut expected, "%t")
                .write_line(&server, None, &line)
                .unwrap();
            assert_eq!(
                render_raw("%t", &server, None, input).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn byte_view_rejects_malformed_monitor_prefixes_before_writing() {
        let server = ServerAddr::from_tcp_addr("127.0.0.1", 6379);
        let malformed: &[&[u8]] = &[
            b"",
            br#"x.0 [0 127.0.0.1:1] "PING""#,
            br#"18446744073709551616.0 [0 127.0.0.1:1] "PING""#,
            br#"1.0 [18446744073709551616 127.0.0.1:1] "PING""#,
            br#"1.0 [0 256.0.0.1:1] "PING""#,
            br#"1.0 [0 127.0.0.1:65536] "PING""#,
            br#"1.0 [0 [not-ip]:1] "PING""#,
            br#"1.0 [0 127.0.0.1:1] "BAD-CMD""#,
            b"1.0 [0 127.0.0.1:1] \"PING",
        ];

        for input in malformed {
            let mut output = Vec::new();
            let error = PlainWriter::new(&mut output, "%l")
                .write_raw_line(&server, None, input)
                .unwrap_err();
            assert!(error.to_string().contains("Failed to parse line"));
            assert!(output.is_empty());
        }
    }

    #[test]
    fn byte_view_preserves_raw_argument_validation_contract() {
        let input = br#"1.0 [0 127.0.0.1:1] "SET" "unterminated"#;
        let server = ServerAddr::from_tcp_addr("127.0.0.1", 6379);

        assert_eq!(
            render_raw("%l", &server, None, input).unwrap(),
            b"\"SET\" \"unterminated\n"
        );
    }

    #[test]
    #[ignore = "manual optimized-build throughput benchmark"]
    fn benchmark_byte_view_default_multi_source_format() {
        let server = ServerAddr::from_tcp_addr("127.0.0.1", 6379);
        let line =
            br#"1783484211.311904 [0 127.0.0.1:49152] "GET" "benchmark-key""#;
        let mut writer =
            PlainWriter::new(ByteCounter::default(), "%t [%S %d] %l");

        for _ in 0..10_000_000 {
            writer
                .write_raw_line(black_box(&server), None, black_box(line))
                .unwrap();
        }

        black_box(writer.writer.0);
    }

    #[test]
    #[ignore = "comparison baseline for the byte-view benchmark"]
    fn benchmark_typed_default_multi_source_format() {
        let server = ServerAddr::from_tcp_addr("127.0.0.1", 6379);
        let input =
            br#"1783484211.311904 [0 127.0.0.1:49152] "GET" "benchmark-key""#;
        let mut writer =
            PlainWriter::new(ByteCounter::default(), "%t [%S %d] %l");

        for _ in 0..10_000_000 {
            let (_, line) =
                Line::from_line_bytes(black_box(input), false).unwrap();
            writer
                .write_line(black_box(&server), None, black_box(&line))
                .unwrap();
        }

        black_box(writer.writer.0);
    }
}
