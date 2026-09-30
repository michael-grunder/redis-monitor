//! Explicit structured field selection. Default output never enters this module.
use std::{
    borrow::Cow,
    io::{self, Write},
};

use anyhow::{Result, anyhow};
use redis_monitor::monitor::{Record, write_uint};
use serde::{Serialize, Serializer, ser::SerializeMap};
use serde_bytes::Bytes as SerBytes;
use serde_php as php;

use super::{
    ByteArgs, FormatToken, OutputKind, Scratch, Source, SourceOrigin, TextArgs,
    compile_format, csv_field, resp_bulk, write_resp_source,
};

#[derive(Debug)]
pub struct Fields {
    pub(super) kind: OutputKind,
    source: bool,
    tokens: Vec<FormatToken>,
    pub(super) header: Vec<u8>,
}

impl Fields {
    pub(super) fn new(
        kind: OutputKind,
        format: &str,
        source: bool,
    ) -> Result<Self> {
        let mut tokens = Vec::new();
        for token in compile_format(format) {
            if let FormatToken::Literal(bytes) = &token {
                if bytes.iter().all(|b| b.is_ascii_whitespace() || *b == b',') {
                    continue;
                }
                return Err(anyhow!(
                    "Structured --format accepts field tokens separated by spaces or commas; literals and unknown tokens are not supported"
                ));
            }
            if tokens.contains(&token) {
                return Err(anyhow!(
                    "Duplicate structured field '{}'",
                    token.key()
                ));
            }
            tokens.push(token);
        }
        if tokens.is_empty() {
            return Err(anyhow!(
                "Structured --format must select at least one field"
            ));
        }
        let mut header = Vec::new();
        if source {
            header.extend_from_slice(b"source_address,source_name,");
        }
        for (i, token) in tokens.iter().enumerate() {
            if i > 0 {
                header.push(b',');
            }
            header.extend_from_slice(token.key().as_bytes());
        }
        header.push(b'\n');
        Ok(Self {
            kind,
            source,
            tokens,
            header,
        })
    }

    pub(super) fn write(
        &self,
        out: &mut Vec<u8>,
        source: &Source,
        record: &Record<'_>,
        scratch: &mut Scratch<'_>,
    ) -> io::Result<()> {
        match self.kind {
            OutputKind::Json => self
                .write_map(
                    &mut serde_json::Serializer::new(out),
                    source,
                    record,
                    scratch,
                    true,
                )
                .map_err(io::Error::other),
            OutputKind::Php => self.write_php(out, source, record, scratch),
            OutputKind::Csv => {
                let mut separator = if self.source {
                    let identity = source.structured();
                    csv_field(out, identity.address.unwrap_or("").as_bytes());
                    out.push(b',');
                    csv_field(out, identity.name.unwrap_or("").as_bytes());
                    true
                } else {
                    false
                };
                for token in &self.tokens {
                    let value = token.value(
                        record,
                        source,
                        &scratch.args,
                        &mut scratch.value,
                    )?;
                    if let Value::Args(args) = value {
                        for arg in args {
                            if separator {
                                out.push(b',');
                            }
                            csv_field(out, arg);
                            separator = true;
                        }
                        continue;
                    }
                    if separator {
                        out.push(b',');
                    }
                    match value {
                        Value::Null => {}
                        Value::Bytes(bytes) => csv_field(out, bytes),
                        Value::Timestamp(t) => write!(out, "{t}")?,
                        Value::Database(db) => write_uint(out, db)?,
                        Value::Args(_) => {
                            unreachable!("arguments handled above")
                        }
                    }
                    separator = true;
                }
                Ok(())
            }
            OutputKind::Resp => self.write_resp(out, source, record, scratch),
            // Only structured kinds construct Fields, in Formatter::new.
            OutputKind::Plain => unreachable!("plain output uses PlainFormat"),
        }
    }

    fn write_php(
        &self,
        out: &mut Vec<u8>,
        source: &Source,
        record: &Record<'_>,
        scratch: &mut Scratch<'_>,
    ) -> io::Result<()> {
        // serde_php does not expose its Serializer. Write the array
        // framing here and delegate all keys/values to its serializer.
        write!(out, "a:{}:{{", self.tokens.len() + usize::from(self.source))?;
        for token in &self.tokens {
            let value = token.value(
                record,
                source,
                &scratch.args,
                &mut scratch.value,
            )?;
            php::to_writer(&mut *out, token.key()).map_err(io::Error::other)?;
            php::to_writer(&mut *out, &SerializedValue { value, text: false })
                .map_err(io::Error::other)?;
        }
        if self.source {
            php::to_writer(&mut *out, "source").map_err(io::Error::other)?;
            php::to_writer(&mut *out, &source.structured())
                .map_err(io::Error::other)?;
        }
        out.push(b'}');
        Ok(())
    }

    fn write_resp(
        &self,
        out: &mut Vec<u8>,
        source: &Source,
        record: &Record<'_>,
        scratch: &mut Scratch<'_>,
    ) -> io::Result<()> {
        if self.source {
            write_resp_source(out, source)?;
        }
        write!(out, "*{}\r\n", self.tokens.len())?;
        for token in &self.tokens {
            let value = token.value(
                record,
                source,
                &scratch.args,
                &mut scratch.value,
            )?;
            match value {
                Value::Null => out.extend_from_slice(b"$-1\r\n"),
                Value::Bytes(bytes) => resp_bulk(out, bytes)?,
                Value::Args(args) => {
                    write!(out, "*{}\r\n", args.len())?;
                    for arg in args {
                        resp_bulk(out, arg)?;
                    }
                }
                Value::Timestamp(_) | Value::Database(_) => {
                    scratch.value.clear();
                    token.write(&mut scratch.value, source, record)?;
                    resp_bulk(out, &scratch.value)?;
                }
            }
        }
        Ok(())
    }

    fn write_map<S: Serializer>(
        &self,
        serializer: S,
        source: &Source,
        record: &Record<'_>,
        scratch: &mut Scratch<'_>,
        text: bool,
    ) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(
            self.tokens.len() + usize::from(self.source),
        ))?;
        for token in &self.tokens {
            let value = token
                .value(record, source, &scratch.args, &mut scratch.value)
                .map_err(serde::ser::Error::custom)?;
            map.serialize_entry(token.key(), &SerializedValue { value, text })?;
        }
        if self.source {
            map.serialize_entry("source", &source.structured())?;
        }
        map.end()
    }
}

enum Value<'a> {
    Null,
    Timestamp(f64),
    Database(u64),
    Bytes(&'a [u8]),
    Args(&'a [Cow<'a, [u8]>]),
}

struct SerializedValue<'a> {
    value: Value<'a>,
    text: bool,
}

impl Serialize for SerializedValue<'_> {
    fn serialize<S: Serializer>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match &self.value {
            Value::Null => serializer.serialize_none(),
            Value::Timestamp(t) => serializer.serialize_f64(*t),
            Value::Database(db) => serializer.serialize_u64(*db),
            Value::Bytes(bytes) if self.text => {
                String::from_utf8_lossy(bytes).serialize(serializer)
            }
            Value::Bytes(bytes) => SerBytes::new(bytes).serialize(serializer),
            Value::Args(args) if self.text => {
                TextArgs(args).serialize(serializer)
            }
            Value::Args(args) => ByteArgs(args).serialize(serializer),
        }
    }
}

impl FormatToken {
    fn key(&self) -> &'static str {
        match self {
            Self::ClientServerShort => "addresses",
            Self::ServerAddress => "server_address",
            Self::ServerName => "server_name",
            Self::ServerHost => "server_host",
            Self::ServerPort => "server_port",
            Self::ClientAddress => "addr",
            Self::ClientHost => "client_host",
            Self::ClientPort => "client_port",
            Self::Timestamp => "timestamp",
            Self::Database => "db",
            Self::Command => "cmd",
            Self::Arguments => "args",
            Self::FullLine => "line",
            Self::Literal(_) => unreachable!("Fields::new rejects literals"),
        }
    }

    fn value<'a>(
        &self,
        record: &'a Record<'a>,
        source: &'a Source,
        args: &'a [Cow<'a, [u8]>],
        scratch: &'a mut Vec<u8>,
    ) -> io::Result<Value<'a>> {
        let optional = |s: Option<&'a str>| {
            s.map_or(Value::Null, |s| Value::Bytes(s.as_bytes()))
        };
        Ok(match self {
            Self::Timestamp => Value::Timestamp(record.timestamp()),
            Self::Database => Value::Database(record.db),
            Self::Arguments => Value::Args(args),
            Self::Command => Value::Bytes(record.cmd),
            Self::ServerAddress => optional(source.structured().address),
            Self::ServerName => optional(source.name.as_deref()),
            Self::ServerHost | Self::ServerPort
                if matches!(source.origin, SourceOrigin::Reader) =>
            {
                Value::Null
            }
            Self::ServerHost => Value::Bytes(source.host.as_bytes()),
            Self::ServerPort => Value::Bytes(source.port.as_bytes()),
            Self::FullLine if record.full_line.is_some() => {
                Value::Bytes(record.full_line.unwrap())
            }
            _ => {
                scratch.clear();
                self.write(scratch, source, record)?;
                Value::Bytes(scratch)
            }
        })
    }
}
