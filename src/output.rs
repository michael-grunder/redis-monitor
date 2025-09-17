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
enum LineOutput {
    Literal(Vec<u8>),
    ServerAddress,
    ServerHost,
    ServerPort,
    ClientAddress,
    ClientHost,
    ClientPort,
    Timestamp,
    Database,
    Command,
    Arguments,
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

    fn write_line(&mut self, server: &ServerAddr, line: &Line) -> Result<()>;
}

#[derive(Debug)]
struct PlainWriter<W: Write> {
    writer: W,
    format: Vec<LineOutput>,
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

    fn write_line(&mut self, server: &ServerAddr, line: &Line) -> Result<()> {
        let (w, format) = (&mut self.writer, &self.format);

        for f in format {
            match f {
                LineOutput::Literal(v) => w.write_all(&v)?,
                LineOutput::ServerAddress => write!(w, "{}", server)?,
                LineOutput::ServerHost => Self::w_host(w, server)?,
                LineOutput::ServerPort => Self::w_port(w, server)?,
                LineOutput::ClientAddress => match line.addr {
                    ClientAddr::Path(p) => w.write_all(p.as_bytes())?,
                    ClientAddr::Tcp((ip, port)) => {
                        w.write_all(ip.to_string().as_bytes())?;
                        w.write_all(b":")?;
                        w.write_all(port.to_string().as_bytes())?;
                    }
                    _ => w.write_all(b"-")?,
                },
                LineOutput::ClientHost => match line.addr {
                    ClientAddr::Path(p) => write!(w, "{}", p)?,
                    ClientAddr::Tcp((ip, _port)) => {
                        w.write_all(ip.to_string().as_bytes())?;
                    }
                    _ => w.write_all(b"-")?,
                },
                LineOutput::ClientPort => match line.addr {
                    ClientAddr::Path(_) => w.write_all(b"-")?,
                    ClientAddr::Tcp((_ip, port)) => {
                        write!(w, "{}", port)?;
                    }
                    _ => w.write_all(b"-")?,
                },
                LineOutput::Timestamp => {
                    write!(w, "{}", line.timestamp)?;
                }
                LineOutput::Database => write!(w, "{}", line.db)?,
                LineOutput::Command => write!(w, "{}", line.cmd)?,
                LineOutput::Arguments => write!(w, "{}", line.args)?,
            }
        }

        self.writer.write_all(b"\n")?;

        Ok(())
    }
}

impl<W: Write> PlainWriter<W> {
    fn push_literal(v: &mut Vec<LineOutput>, lit: &mut Vec<u8>) {
        if !lit.is_empty() {
            v.push(LineOutput::Literal(std::mem::take(lit)));
        }
    }

    fn compile_format(fmt: &str) -> Vec<LineOutput> {
        let mut res = vec![];
        let mut it = fmt.as_bytes().iter().copied().peekable();
        let mut lit = vec![];

        while let Some(b) = it.next() {
            if b != b'%' {
                lit.push(b);
                continue;
            }
            if it.next() == Some(b'%') {
                lit.push(b'%');
                continue;
            }

            Self::push_literal(&mut res, &mut lit);

            let o = match it.next() {
                Some(b's') => match it.next() {
                    Some(b'a') => LineOutput::ServerAddress,
                    Some(b'h') => LineOutput::ServerHost,
                    Some(b'p') => LineOutput::ServerPort,
                    Some(x) => {
                        lit.extend_from_slice(&[b'%', b's', x]);
                        continue;
                    }
                    None => {
                        lit.extend_from_slice(&[b'%', b's']);
                        continue;
                    }
                },
                Some(b'c') => match it.next() {
                    Some(b'a') => LineOutput::ClientAddress,
                    Some(b'h') => LineOutput::ClientHost,
                    Some(b'p') => LineOutput::ClientPort,
                    Some(x) => {
                        lit.extend_from_slice(&[b'%', b'c', x]);
                        continue;
                    }
                    None => {
                        lit.extend_from_slice(&[b'%', b'c']);
                        continue;
                    }
                },
                Some(b't') => LineOutput::Timestamp,
                Some(b'd') => LineOutput::Database,
                Some(b'C') => LineOutput::Command,
                Some(b'a') => LineOutput::Arguments,
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
    fn write_line(&mut self, _server: &ServerAddr, line: &Line) -> Result<()> {
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
        parsed: &Line,
    ) -> Result<()> {
        parsed.write_resp(&mut self.writer)?;
        Ok(())
    }
}
