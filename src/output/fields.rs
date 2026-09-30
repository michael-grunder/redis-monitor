//! Structured output: JSON, PHP, CSV, and selected RESP fields. Without
//! `--format`, JSON, PHP, and CSV records are the fields `%t %d %ca %C %a`.
use std::{
    borrow::Cow,
    io::{self, Write},
};

use anyhow::{Result, bail};
use redis_monitor::monitor::{Client, Record, write_uint};
use serde::{Serialize, Serializer};

use lexical_core::FormattedSize;

use super::{
    FormatToken, OutputKind, Source, SourceOrigin, StructuredSource,
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
                bail!(
                    "Structured --format accepts field tokens separated by spaces or commas; literals and unknown tokens are not supported"
                );
            }
            if tokens.contains(&token) {
                bail!("Duplicate structured field '{}'", token.key());
            }
            tokens.push(token);
        }
        if tokens.is_empty() {
            bail!("Structured --format must select at least one field");
        }
        let mut keys = Vec::new();
        if source {
            keys.extend(["source_address", "source_name"]);
        }
        keys.extend(tokens.iter().map(FormatToken::key));
        let mut header = keys.join(",").into_bytes();
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
        args: &[Cow<'_, [u8]>],
        text: &mut Vec<u8>,
    ) -> io::Result<()> {
        match self.kind {
            OutputKind::Json => {
                self.encode(Json::default(), out, source, record, args, text)
            }
            OutputKind::Php => {
                self.encode(Php, out, source, record, args, text)
            }
            OutputKind::Csv => {
                self.encode(Csv::default(), out, source, record, args, text)
            }
            OutputKind::Resp => {
                self.encode(Resp, out, source, record, args, text)
            }
            // Formatter::new routes plain output to PlainFormat.
            OutputKind::Plain => unreachable!("plain output uses PlainFormat"),
        }
    }

    /// Encode one record: the fields in order, plus the source if requested.
    /// Values are produced one at a time; rendered text reuses `text`.
    fn encode(
        &self,
        mut encoding: impl Encoding,
        out: &mut Vec<u8>,
        source: &Source,
        record: &Record<'_>,
        args: &[Cow<'_, [u8]>],
        text: &mut Vec<u8>,
    ) -> io::Result<()> {
        let identity = self.source.then(|| source.structured());
        encoding.open(out, self.tokens.len(), identity.as_ref())?;
        for token in &self.tokens {
            let value = token.value(record, source, args, text)?;
            encoding.field(out, token.key(), value)?;
        }
        encoding.close(out, identity.as_ref())
    }
}

/// How a structured format frames a record and encodes its fields. Per-field
/// methods are `#[inline]`: out-of-line calls measured about 5% slower for
/// short JSON records.
trait Encoding {
    /// Start a record of `fields` fields, plus `source` if present.
    fn open(
        &mut self,
        out: &mut Vec<u8>,
        fields: usize,
        source: Option<&StructuredSource<'_>>,
    ) -> io::Result<()> {
        let _ = (out, fields, source);
        Ok(())
    }

    fn field(
        &mut self,
        out: &mut Vec<u8>,
        key: &str,
        value: Value<'_>,
    ) -> io::Result<()>;

    /// Finish the record, adding a trailing `source` if the format has one.
    fn close(
        &mut self,
        out: &mut Vec<u8>,
        source: Option<&StructuredSource<'_>>,
    ) -> io::Result<()> {
        let _ = (out, source);
        Ok(())
    }
}

/// A JSON object; the source is a trailing nested `source` object.
#[derive(Default)]
struct Json {
    entries: usize,
}

impl Json {
    /// `"key":`, preceded by a separator. Keys are static ASCII identifiers
    /// that need no escaping, so the framing is written directly and only
    /// values go through the serializer.
    #[inline]
    fn key(&mut self, out: &mut Vec<u8>, key: &str) {
        if self.entries > 0 {
            out.push(b',');
        }
        self.entries += 1;
        out.push(b'"');
        out.extend_from_slice(key.as_bytes());
        out.extend_from_slice(b"\":");
    }
}

impl Encoding for Json {
    fn open(
        &mut self,
        out: &mut Vec<u8>,
        _: usize,
        _: Option<&StructuredSource<'_>>,
    ) -> io::Result<()> {
        out.push(b'{');
        Ok(())
    }

    #[inline]
    fn field(
        &mut self,
        out: &mut Vec<u8>,
        key: &str,
        value: Value<'_>,
    ) -> io::Result<()> {
        self.key(out, key);
        if let Value::Ascii(ascii) = value {
            // Never needs escaping.
            out.push(b'"');
            out.extend_from_slice(ascii);
            out.push(b'"');
            return Ok(());
        }
        Ok(serde_json::to_writer(&mut *out, &JsonValue(value))?)
    }

    fn close(
        &mut self,
        out: &mut Vec<u8>,
        source: Option<&StructuredSource<'_>>,
    ) -> io::Result<()> {
        if let Some(source) = source {
            self.key(out, "source");
            let mut nested = Self::default();
            nested.open(out, 2, None)?;
            nested.field(out, "address", Value::text(source.address))?;
            nested.field(out, "name", Value::text(source.name))?;
            nested.close(out, None)?;
        }
        out.push(b'}');
        Ok(())
    }
}

/// A PHP `serialize()` array; the source is a trailing nested array.
struct Php;

impl Php {
    #[inline]
    fn str(out: &mut Vec<u8>, bytes: &[u8]) -> io::Result<()> {
        out.extend_from_slice(b"s:");
        write_uint(out, bytes.len() as u64)?;
        out.extend_from_slice(b":\"");
        out.extend_from_slice(bytes);
        out.extend_from_slice(b"\";");
        Ok(())
    }

    #[inline]
    fn int(out: &mut Vec<u8>, n: u64) -> io::Result<()> {
        out.extend_from_slice(b"i:");
        write_uint(out, n)?;
        out.push(b';');
        Ok(())
    }

    /// `a:N:{`; the caller writes `N` key/value pairs and the closing `}`.
    fn array(out: &mut Vec<u8>, len: usize) -> io::Result<()> {
        out.extend_from_slice(b"a:");
        write_uint(out, len as u64)?;
        out.extend_from_slice(b":{");
        Ok(())
    }
}

impl Encoding for Php {
    fn open(
        &mut self,
        out: &mut Vec<u8>,
        fields: usize,
        source: Option<&StructuredSource<'_>>,
    ) -> io::Result<()> {
        Self::array(out, fields + usize::from(source.is_some()))
    }

    #[inline]
    fn field(
        &mut self,
        out: &mut Vec<u8>,
        key: &str,
        value: Value<'_>,
    ) -> io::Result<()> {
        Self::str(out, key.as_bytes())?;
        match value {
            Value::Null => out.extend_from_slice(b"N;"),
            Value::Timestamp(record) => {
                write!(out, "d:{};", record.timestamp())?;
            }
            Value::Database(db) => Self::int(out, db)?,
            Value::Args(args) => {
                Self::array(out, args.len())?;
                for (index, arg) in args.iter().enumerate() {
                    Self::int(out, index as u64)?;
                    Self::str(out, arg)?;
                }
                out.push(b'}');
            }
            string => Self::str(out, string.bytes().unwrap_or_default())?,
        }
        Ok(())
    }

    fn close(
        &mut self,
        out: &mut Vec<u8>,
        source: Option<&StructuredSource<'_>>,
    ) -> io::Result<()> {
        if let Some(source) = source {
            Self::str(out, b"source")?;
            Self::array(out, 2)?;
            self.field(out, "address", Value::text(source.address))?;
            self.field(out, "name", Value::text(source.name))?;
            out.push(b'}');
        }
        out.push(b'}');
        Ok(())
    }
}

/// A CSV row: source columns first, then one column per field, except that
/// arguments take one column each.
#[derive(Default)]
struct Csv {
    columns: usize,
}

impl Csv {
    #[inline]
    fn separate(&mut self, out: &mut Vec<u8>) {
        if self.columns > 0 {
            out.push(b',');
        }
        self.columns += 1;
    }
}

impl Encoding for Csv {
    fn open(
        &mut self,
        out: &mut Vec<u8>,
        _: usize,
        source: Option<&StructuredSource<'_>>,
    ) -> io::Result<()> {
        if let Some(source) = source {
            for field in [source.address, source.name] {
                self.separate(out);
                csv_field(out, field.unwrap_or_default().as_bytes());
            }
        }
        Ok(())
    }

    #[inline]
    fn field(
        &mut self,
        out: &mut Vec<u8>,
        _: &str,
        value: Value<'_>,
    ) -> io::Result<()> {
        if let Value::Args(args) = value {
            for arg in args {
                self.separate(out);
                csv_field(out, arg);
            }
            return Ok(());
        }
        self.separate(out);
        match value {
            Value::Timestamp(record) => write!(out, "{}", record.timestamp())?,
            Value::Database(db) => write_uint(out, db)?,
            string => csv_field(out, string.bytes().unwrap_or_default()),
        }
        Ok(())
    }
}

/// A RESP array of fields, wrapped in `[[address, name], fields]` when the
/// source is included. Numbers are sent as their text.
struct Resp;

impl Encoding for Resp {
    fn open(
        &mut self,
        out: &mut Vec<u8>,
        fields: usize,
        source: Option<&StructuredSource<'_>>,
    ) -> io::Result<()> {
        if let Some(source) = source {
            write_resp_source(out, source)?;
        }
        write!(out, "*{fields}\r\n")
    }

    #[inline]
    fn field(
        &mut self,
        out: &mut Vec<u8>,
        _: &str,
        value: Value<'_>,
    ) -> io::Result<()> {
        match value {
            Value::Null => out.extend_from_slice(b"$-1\r\n"),
            Value::Timestamp(record) => {
                // At most 20 + 1 + 20 bytes: `<u64>.<u64>` digits.
                const CAPACITY: usize = 64;
                let mut buf = [0; CAPACITY];
                let mut cursor = &mut buf[..];
                record.write_timestamp(&mut cursor)?;
                let len = CAPACITY - cursor.len();
                resp_bulk(out, &buf[..len])?;
            }
            Value::Database(db) => {
                let mut digits = [0; u64::FORMATTED_SIZE_DECIMAL];
                resp_bulk(out, lexical_core::write(db, &mut digits))?;
            }
            Value::Args(args) => {
                write!(out, "*{}\r\n", args.len())?;
                for arg in args {
                    resp_bulk(out, arg)?;
                }
            }
            string => resp_bulk(out, string.bytes().unwrap_or_default())?,
        }
        Ok(())
    }
}

/// A field value in its native type.
enum Value<'a> {
    Null,
    /// The record's timestamp: a float, or its MONITOR text for RESP.
    Timestamp(&'a Record<'a>),
    Database(u64),
    /// Printable ASCII without quotes or backslashes, such as command names
    /// and rendered TCP addresses: valid text that never needs escaping.
    Ascii(&'a [u8]),
    /// Text known to be valid UTF-8.
    Text(&'a str),
    /// Arbitrary bytes, such as MONITOR's escaped argument text.
    Bytes(&'a [u8]),
    Args(&'a [Cow<'a, [u8]>]),
}

impl<'a> Value<'a> {
    fn text(text: Option<&'a str>) -> Self {
        text.map_or(Value::Null, Value::Text)
    }

    /// The bytes of a string-like value.
    const fn bytes(&self) -> Option<&'a [u8]> {
        match *self {
            Value::Ascii(bytes) | Value::Bytes(bytes) => Some(bytes),
            Value::Text(text) => Some(text.as_bytes()),
            _ => None,
        }
    }
}

/// Serializes a value as JSON, replacing invalid UTF-8 in byte strings.
struct JsonValue<'a>(Value<'a>);

impl Serialize for JsonValue<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match &self.0 {
            Value::Null => s.serialize_none(),
            Value::Timestamp(record) => s.serialize_f64(record.timestamp()),
            Value::Database(db) => s.serialize_u64(*db),
            Value::Text(text) => s.serialize_str(text),
            Value::Ascii(bytes) | Value::Bytes(bytes) => {
                s.serialize_str(&lossy(bytes))
            }
            Value::Args(args) => {
                s.collect_seq(args.iter().map(|arg| lossy(arg)))
            }
        }
    }
}

/// Text for JSON, replacing invalid UTF-8. Validation's ASCII fast path is
/// much quicker than `from_utf8_lossy` for the common, valid case.
fn lossy(bytes: &[u8]) -> Cow<'_, str> {
    std::str::from_utf8(bytes)
        .map_or_else(|_| String::from_utf8_lossy(bytes), Cow::Borrowed)
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
        let reader = matches!(source.origin, SourceOrigin::Reader);
        Ok(match self {
            Self::Timestamp => Value::Timestamp(record),
            Self::Database => Value::Database(record.db),
            Self::Arguments => Value::Args(args),
            Self::Command => Value::Ascii(record.cmd),
            Self::ServerAddress => Value::text(source.structured().address),
            Self::ServerName => Value::text(source.name.as_deref()),
            Self::ServerHost | Self::ServerPort if reader => Value::Null,
            Self::ServerHost => Value::Text(&source.host),
            Self::ServerPort => Value::Text(&source.port),
            Self::FullLine if record.full_line.is_some() => {
                Value::Bytes(record.full_line.unwrap_or_default())
            }
            Self::ClientAddress
                if !matches!(record.client, Client::Unix(_)) =>
            {
                // IP addresses, ports, `lua`, and `-` are plain ASCII.
                scratch.clear();
                record.client.write_addr(scratch)?;
                Value::Ascii(scratch)
            }
            _ => {
                // Rendered text: addresses and ports are UTF-8; `%l`
                // fallbacks copy MONITOR's escaped argument bytes.
                scratch.clear();
                self.write(scratch, source, record)?;
                let rendered: &'a [u8] = scratch;
                std::str::from_utf8(rendered)
                    .map_or(Value::Bytes(rendered), Value::Text)
            }
        })
    }
}
