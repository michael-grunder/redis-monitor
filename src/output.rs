use std::{io::Write, str::FromStr};

use anyhow::{Error, Result, anyhow};
use serde::{Serialize, Serializer, ser::SerializeStruct};
use serde_bytes::{ByteBuf as SerByteBuf, Bytes as SerBytes};
use serde_php as php;

use crate::{
    connection::{GetHost, ServerAddr},
    monitor::{ClientAddr, Line, LineArgs},
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
    pub fn need_args(self) -> bool {
        self != Self::Plain
    }

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

    fn compile_format(fmt: &str) -> Vec<FormatToken> {
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

        res
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
        Self {
            writer,
            format: Self::compile_format(format),
        }
    }
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
    #[ignore = "manual optimized-build throughput benchmark"]
    fn benchmark_default_multi_source_format() {
        let server = ServerAddr::from_tcp_addr("127.0.0.1", 6379);
        let line = Line::new(
            1_783_484_211.311_904,
            0,
            ClientAddr::from_addr(ip("127.0.0.1"), 49152),
            "GET",
            LineArgs::Raw(b"\"benchmark-key\""),
        );
        let mut writer =
            PlainWriter::new(ByteCounter::default(), "%t [%S %d] %l");

        for _ in 0..10_000_000 {
            writer
                .write_line(black_box(&server), None, black_box(&line))
                .unwrap();
        }

        black_box(writer.writer.0);
    }
}
