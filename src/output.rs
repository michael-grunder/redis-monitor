use std::{io::Write, str::FromStr};

use anyhow::{Error, Result, anyhow};

use crate::{connection::Monitor, monitor::Line};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OutputKind {
    Plain,
    Json,
    Csv,
    Resp,
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
                "Invalid output format '{s}'. Supported formats: json, text, xml",
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
    ) -> Box<dyn OutputHandler + 'a> {
        match self {
            Self::Plain => Box::new(PlainWriter { writer }),
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
    fn preamble(&mut self, _monitor: &Monitor) -> Result<()> {
        Ok(())
    }

    fn write_line(&mut self, prefix: &str, line: &Line) -> Result<()>;
}

#[derive(Debug)]
struct PlainWriter<W: Write> {
    writer: W,
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
    fn preamble(&mut self, monitor: &Monitor) -> Result<()> {
        writeln!(self.writer, "MONITOR: {}", monitor.address)?;
        Ok(())
    }

    fn write_line(&mut self, prefix: &str, line: &Line) -> Result<()> {
        writeln!(self.writer, "{} {}", prefix, line.args)?;
        Ok(())
    }
}

impl<W: Write> OutputHandler for CsvWriter<W> {
    fn write_line(&mut self, _prefix: &str, line: &Line) -> Result<()> {
        self.writer.serialize(line)?;
        self.writer.flush()?;
        Ok(())
    }
}

impl<W: Write> OutputHandler for JsonWriter<W> {
    fn preamble(&mut self, monitor: &Monitor) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &monitor.address)?;
        writeln!(&mut self.writer)?;
        Ok(())
    }

    fn write_line(&mut self, _prefix: &str, parsed: &Line) -> Result<()> {
        serde_json::to_writer(&mut self.writer, parsed)?;
        writeln!(&mut self.writer)?;
        Ok(())
    }
}

impl<W: Write> OutputHandler for RespWriter<W> {
    fn write_line(&mut self, _prefix: &str, parsed: &Line) -> Result<()> {
        parsed.write_resp(&mut self.writer)?;
        Ok(())
    }
}
