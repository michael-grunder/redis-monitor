use std::{io::Write, str::FromStr};

use anyhow::{Error, Result, anyhow};

use crate::{
    connection::{GetHost, Monitor, ServerAddr},
    monitor::{ClientAddr, Line},
};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OutputKind {
    Plain,
    Json,
    Csv,
    Resp,
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
            _ => Err(anyhow!(
                "Invalid output format '{s}'. Supported formats: plain, resp, json, xml",
            )),
        }
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
            Self::Resp => Box::new(RespWriter { writer }),
        }
    }
}

pub trait OutputHandler {
    fn preamble(&mut self, _monitor: &[Monitor]) -> Result<()> {
        Ok(())
    }

    fn write_line(
        &mut self,
        server: &ServerAddr,
        name: Option<&str>,
        line: &Line,
    ) -> Result<()>;
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

impl<W: Write> OutputHandler for PlainWriter<W> {
    fn preamble(&mut self, monitor: &[Monitor]) -> Result<()> {
        let addresses = monitor
            .iter()
            .map(|m| m.address.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        writeln!(self.writer, "MONITOR: {addresses}")?;

        Ok(())
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
                    write!(w, "{}", name.unwrap_or("-"))?;
                }
                FormatToken::ServerAddress => write!(w, "{server}")?,
                FormatToken::ServerHost => Self::w_host(w, server)?,
                FormatToken::ServerPort => Self::w_port(w, server)?,
                FormatToken::ClientAddress => write!(w, "{}", line.addr)?,
                FormatToken::ClientHost => {
                    write!(w, "{}", line.addr.get_host())?;
                }
                FormatToken::ClientPort => {
                    write!(w, "{}", line.addr.get_short_name())?;
                }
                FormatToken::Timestamp => {
                    write!(w, "{}", line.timestamp)?;
                }
                FormatToken::Database => write!(w, "{}", line.db)?,
                FormatToken::Command => write!(w, "{}", line.cmd)?,
                FormatToken::Arguments => write!(w, "{}", line.args)?,
                FormatToken::FullLine => {
                    write!(w, r#""{}" {}"#, line.cmd, line.args)?;
                }
            }
        }

        self.writer.write_all(b"\n")?;

        Ok(())
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
        let mut it = fmt.as_bytes().iter().copied().peekable();
        let mut lit = vec![];

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
        if let ServerAddr::Tcp(shost, sport) = server
            && let ClientAddr::Tcp(chost, cport) = client
            && shost == &chost.to_string()
        {
            write!(writer, "{sport} {cport}")?;
            return Ok(());
        }

        write!(writer, "{server} {client}")?;

        Ok(())
    }

    fn w_host(writer: &mut W, server: &ServerAddr) -> Result<()> {
        writer.write_all(server.get_host().as_bytes())?;
        Ok(())
    }

    fn w_port(writer: &mut W, server: &ServerAddr) -> Result<()> {
        match server.get_short_name() {
            Some(s) => writer.write_all(s.as_bytes())?,
            None => writer.write_all(b"")?,
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
        self.writer.flush()?;
        Ok(())
    }
}

impl<W: Write> OutputHandler for JsonWriter<W> {
    fn preamble(&mut self, monitor: &[Monitor]) -> Result<()> {
        let display: Vec<String> =
            monitor.iter().map(|m| m.address.to_string()).collect();
        serde_json::to_writer(&mut self.writer, &display)?;

        writeln!(&mut self.writer)?;
        Ok(())
    }

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
}
