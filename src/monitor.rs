use std::{
    borrow::Cow,
    io::Write,
    net::{IpAddr, Ipv4Addr},
};

use anyhow::Result;
use nom::{
    Err, IResult, Parser,
    branch::alt,
    bytes::{
        complete::{is_not, tag, take_until, take_while, take_while_m_n},
        take_while1,
    },
    combinator::{map_res, peek, value, verify},
    error::{ErrorKind, FromExternalError, ParseError},
    sequence::preceded,
};
use serde::{Serialize, Serializer};

#[derive(Debug, Serialize)]
pub enum LineArgs<'a> {
    #[serde(with = "serde_bytes")]
    Raw(&'a [u8]),
    #[serde(with = "serde_vec_bytes")]
    Parsed(Vec<Cow<'a, [u8]>>),
}

// helper so Vec<Cow<[u8]>> serializes as bytes not arrays of ints
mod serde_vec_bytes {
    use std::borrow::Cow;

    use serde::{Serializer, ser::SerializeSeq};
    use serde_bytes::Bytes;

    pub fn serialize<S>(v: &Vec<Cow<[u8]>>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = s.serialize_seq(Some(v.len()))?;
        for arg in v {
            seq.serialize_element(&Bytes::new(arg))?;
        }
        seq.end()
    }
}

#[derive(Debug, Serialize)]
pub struct Line<'a> {
    pub timestamp: f64,
    pub db: u64,
    pub addr: ClientAddr<'a>,
    pub cmd: &'a str,
    #[serde(serialize_with = "serialize_args_as_strings")]
    pub args: LineArgs<'a>,
}

#[derive(Debug, Copy, Clone, Default)]
pub struct ParsePlan {
    pub client_ip: bool,
    pub timestamp: bool,
}

#[derive(Debug)]
pub struct LineView<'a> {
    pub timestamp: &'a [u8],
    pub typed_timestamp: Option<f64>,
    pub db: &'a [u8],
    pub addr: ClientAddrView<'a>,
    pub cmd: &'a str,
    pub args: &'a [u8],
    pub full_line: Option<&'a [u8]>,
    pub single_default_tail: Option<&'a [u8]>,
}

#[derive(Debug)]
pub enum ClientAddrView<'a> {
    Path(&'a str),
    Tcp {
        host: &'a [u8],
        port: &'a [u8],
        ip: Option<IpAddr>,
        bracketed: bool,
    },
    Lua,
    Unknown,
}

#[derive(Debug)]
pub enum ClientAddr<'a> {
    Path(&'a str),
    Tcp(IpAddr, u16),
    Lua,
    Unknown,
}

impl std::fmt::Display for LineArgs<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LineArgs::Raw(raw) => {
                // Lossy is OK for display; writers can handle bytes.
                f.write_str(&String::from_utf8_lossy(raw))
            }
            LineArgs::Parsed(args) => {
                let mut it = args.iter();
                if let Some(first) = it.next() {
                    write!(
                        f,
                        "\"{}\"",
                        String::from_utf8_lossy(first.as_ref())
                    )?;
                }
                for a in it {
                    write!(f, " \"{}\"", String::from_utf8_lossy(a.as_ref()))?;
                }
                Ok(())
            }
        }
    }
}

#[inline]
fn space0b(i: &[u8]) -> IResult<&[u8], &[u8]> {
    take_while(|b| matches!(b, b' ' | b'\t'))(i)
}

#[inline]
fn digit1b(i: &[u8]) -> IResult<&[u8], &[u8]> {
    take_while(|b| (b as char).is_ascii_digit())(i).and_then(|(r, s)| {
        if s.is_empty() {
            Err(nom::Err::Error(nom::error::Error::new(
                i,
                nom::error::ErrorKind::Digit,
            )))
        } else {
            Ok((r, s))
        }
    })
}

#[inline]
fn parse_u64(i: &[u8]) -> IResult<&[u8], u64> {
    map_res(digit1b, lexical_core::parse::<u64>).parse(i)
}

#[inline]
fn parse_u64_bytes(i: &[u8]) -> IResult<&[u8], &[u8]> {
    map_res(digit1b, |digits| {
        lexical_core::parse::<u64>(digits).map(|_| digits)
    })
    .parse(i)
}

#[inline]
fn parse_f64(i: &[u8]) -> IResult<&[u8], f64> {
    // Parse <digits> '.' <digits> with no allocation.
    let (i, int_bytes) = digit1b(i)?;
    let (i, _) = tag(".")(i)?;
    let (i, frac_bytes) = digit1b(i)?;

    // Fast integer parses from bytes.
    let int = lexical_core::parse::<u64>(int_bytes).map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(
            i,
            nom::error::ErrorKind::Float,
        ))
    })?;
    let frac = lexical_core::parse::<u64>(frac_bytes).map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(
            i,
            nom::error::ErrorKind::Float,
        ))
    })?;

    // Mother of god, Rust sometimes...
    let len_clamped = frac_bytes.len().min(i32::MAX as usize);
    let exp = i32::try_from(len_clamped).unwrap_or(i32::MAX);
    let scale = 10f64.powi(exp);

    debug_assert!(int <= (1u64 << 53));

    #[allow(clippy::cast_precision_loss)]
    let val = (int as f64) + (frac as f64) / scale;

    Ok((i, val))
}

impl<'a> Line<'a> {
    fn is_structural_quote(input: &[u8]) -> bool {
        if !matches!(input.first(), Some(b'"')) {
            return false;
        }

        let after_quote = &input[1..];
        let next_non_space = after_quote
            .iter()
            .position(|b| !matches!(b, b' ' | b'\t' | b'\r' | b'\n'));

        match next_non_space {
            None => true,
            Some(0) => false,
            Some(idx) => after_quote[idx] == b'"',
        }
    }

    fn parse_escaped_hex<E>(input: &'a [u8]) -> IResult<&'a [u8], u8, E>
    where
        E: ParseError<&'a [u8]>
            + FromExternalError<&'a [u8], std::num::ParseIntError>,
    {
        let (input, _) = tag("x")(input)?;
        let (input, hex) =
            take_while_m_n(2, 2, |c: u8| (c as char).is_ascii_hexdigit())(
                input,
            )?;
        let s = unsafe { std::str::from_utf8_unchecked(hex) };
        let v = u8::from_str_radix(s, 16).map_err(|e| {
            nom::Err::Failure(E::from_external_error(input, ErrorKind::Fail, e))
        })?;
        Ok((input, v))
    }

    fn parse_escaped_char<E>(input: &'a [u8]) -> IResult<&'a [u8], u8, E>
    where
        E: ParseError<&'a [u8]>
            + FromExternalError<&'a [u8], std::num::ParseIntError>,
    {
        preceded(
            tag("\\"),
            alt((
                Self::parse_escaped_hex,
                value(b'\n', tag("n")),
                value(b'\r', tag("r")),
                value(b'\t', tag("t")),
                value(0x07, tag("a")),
                value(0x08, tag("b")),
                value(0x0C, tag("f")),
                value(b'\\', tag("\\")),
                value(b'/', tag("/")),
                value(b'"', tag("\"")),
                value(b' ', tag(" ")),
            )),
        )
        .parse(input)
    }

    /// Non-empty block of bytes that doesn't include `\` or `"`
    fn parse_literal<E: ParseError<&'a [u8]>>(
        input: &'a [u8],
    ) -> IResult<&'a [u8], &'a [u8], E> {
        let not_quote_slash = is_not("\\\"");
        verify(not_quote_slash, |s: &[u8]| !s.is_empty()).parse(input)
    }

    fn parse_escaped_string<E>(
        input: &'a [u8],
    ) -> IResult<&'a [u8], Cow<'a, [u8]>, E>
    where
        E: ParseError<&'a [u8]>
            + FromExternalError<&'a [u8], std::num::ParseIntError>,
    {
        let (mut input, _) = tag("\"")(input)?;
        let content_start = input;
        let mut owned: Option<Vec<u8>> = None;

        loop {
            if input.is_empty() {
                return Err(nom::Err::Error(E::from_error_kind(
                    input,
                    ErrorKind::Tag,
                )));
            }

            if Self::is_structural_quote(input) {
                let consumed = content_start.len().saturating_sub(input.len());
                let (input_after_quote, _) = tag("\"")(input)?;

                let cow = owned.map_or_else(
                    || Cow::Borrowed(&content_start[..consumed]),
                    Cow::Owned,
                );
                return Ok((input_after_quote, cow));
            }

            if matches!(input.first(), Some(b'"')) {
                let consumed = content_start.len().saturating_sub(input.len());
                let buf = owned.get_or_insert_with(|| {
                    let mut v = Vec::with_capacity(consumed + 8);
                    v.extend_from_slice(&content_start[..consumed]);
                    v
                });
                buf.push(b'"');
                input = &input[1..];
                continue;
            }

            if matches!(input.first(), Some(b'\\')) {
                let consumed = content_start.len().saturating_sub(input.len());
                let buf = owned.get_or_insert_with(|| {
                    let mut v = Vec::with_capacity(consumed + 8);
                    v.extend_from_slice(&content_start[..consumed]);
                    v
                });
                if let Ok((next, byte)) = Self::parse_escaped_char::<E>(input) {
                    buf.push(byte);
                    input = next;
                } else {
                    buf.push(b'\\');
                    input = &input[1..];
                }
                continue;
            }

            let (next, literal) = Self::parse_literal::<E>(input)?;
            if let Some(buf) = &mut owned {
                buf.extend_from_slice(literal);
            }
            input = next;
        }
    }

    fn parse_escaped_args<E>(
        mut input: &'a [u8],
    ) -> IResult<&'a [u8], Vec<Cow<'a, [u8]>>, E>
    where
        E: ParseError<&'a [u8]>
            + FromExternalError<&'a [u8], std::num::ParseIntError>,
    {
        let mut args = Vec::new();

        while !input.is_empty() {
            let (next, arg) = Self::parse_escaped_string(input)?;
            args.push(arg);

            let space_len = next
                .iter()
                .take_while(|b| matches!(b, b' ' | b'\t'))
                .count();
            let space = &next[..space_len];
            let after_space = &next[space_len..];
            if after_space.is_empty() {
                input = after_space;
                break;
            }
            if space.is_empty() {
                return Err(nom::Err::Error(E::from_error_kind(
                    after_space,
                    ErrorKind::Space,
                )));
            }
            input = after_space;
        }

        Ok((input, args))
    }

    // aaa.bbb.ccc.ddd:port
    fn parse_ipv4(input: &[u8]) -> IResult<&[u8], (IpAddr, u16)> {
        let (input, a) =
            map_res(digit1b, lexical_core::parse::<u8>).parse(input)?;
        let (input, _) = tag(".")(input)?;
        let (input, b) =
            map_res(digit1b, lexical_core::parse::<u8>).parse(input)?;
        let (input, _) = tag(".")(input)?;
        let (input, c) =
            map_res(digit1b, lexical_core::parse::<u8>).parse(input)?;
        let (input, _) = tag(".")(input)?;
        let (input, d) =
            map_res(digit1b, lexical_core::parse::<u8>).parse(input)?;
        let (input, _) = tag(":")(input)?;
        let (input, port) =
            map_res(digit1b, lexical_core::parse::<u16>).parse(input)?;
        let ip = IpAddr::V4(Ipv4Addr::new(a, b, c, d));
        Ok((input, (ip, port)))
    }

    // [::1]:53374
    fn parse_ipv6(input: &[u8]) -> IResult<&[u8], (IpAddr, u16)> {
        let (input, _) = tag("[")(input)?;
        let (input, ipb) = take_until("]")(input)?;
        let (input, _) = tag("]")(input)?;
        let (input, _) = tag(":")(input)?;
        let (input, port) =
            map_res(digit1b, lexical_core::parse::<u16>).parse(input)?;
        let addrs = std::str::from_utf8(ipb).map_err(|_| {
            Err::Error(ParseError::from_error_kind(
                input,
                nom::error::ErrorKind::Tag,
            ))
        })?;
        let ip = addrs.parse().map_err(|_| {
            Err::Error(ParseError::from_error_kind(
                input,
                nom::error::ErrorKind::Tag,
            ))
        })?;
        Ok((input, (ip, port)))
    }

    fn parse_unix(input: &[u8]) -> IResult<&[u8], &str> {
        let (input, _) = tag("unix:")(input)?;
        let (input, pathb) = take_until("]")(input)?;
        let paths = std::str::from_utf8(pathb).map_err(|_| {
            nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::MapRes,
            ))
        })?;
        Ok((input, paths))
    }

    #[inline]
    fn parse_unknown(input: &[u8]) -> IResult<&[u8], &str> {
        let (input, _) = peek(tag("]")).parse(input)?;
        Ok((input, ""))
    }

    #[inline]
    fn parse_lua(input: &[u8]) -> IResult<&[u8], ()> {
        let (input, _) = tag("lua").parse(input)?;
        Ok((input, ()))
    }

    fn parse_client(input: &[u8]) -> IResult<&[u8], ClientAddr<'_>> {
        if let Ok((input, path)) = Self::parse_unix(input) {
            Ok((input, ClientAddr::from_path(path)))
        } else if let Ok((input, (addr, port))) = Self::parse_ipv4(input) {
            Ok((input, ClientAddr::from_addr(addr, port)))
        } else if let Ok((input, (addr, port))) = Self::parse_ipv6(input) {
            Ok((input, ClientAddr::from_addr(addr, port)))
        } else if let Ok((input, ())) = Self::parse_lua(input) {
            Ok((input, ClientAddr::Lua))
        } else if let Ok((input, _)) = Self::parse_unknown(input) {
            Ok((input, ClientAddr::Unknown))
        } else {
            Err(Err::Error(ParseError::from_error_kind(
                input,
                ErrorKind::Tag,
            )))
        }
    }

    fn parse_source(input: &[u8]) -> IResult<&[u8], (u64, ClientAddr<'_>)> {
        let (input, _) = tag("[")(input)?;
        let (input, db) = parse_u64(input)?;
        let (input, _) = space0b(input)?;
        let (input, addr) = Self::parse_client(input)?;
        let (input, _) = tag("]")(input)?;
        Ok((input, (db, addr)))
    }

    #[inline]
    const fn is_cmd_char(b: u8) -> bool {
        matches!(b,
            b'A'..=b'Z' |
            b'a'..=b'z' |
            b'0'..=b'9' |
            b'_'
        )
    }

    #[inline]
    fn parse_quoted_ascii_cmd(input: &[u8]) -> IResult<&[u8], &str> {
        let (input, _) = tag("\"")(input)?;
        let (input, cmd_bytes) = take_while1(Self::is_cmd_char).parse(input)?;
        let (input, _) = tag("\"")(input)?;
        // Safe: ASCII verified
        let cmd = unsafe { std::str::from_utf8_unchecked(cmd_bytes) };
        Ok((input, cmd))
    }

    pub fn from_line_bytes(
        input: &'a [u8],
        parse_args: bool,
    ) -> IResult<&'a [u8], Self> {
        let (input, timestamp) = parse_f64(input)?;
        let (input, _) = space0b(input)?;
        let (input, (db, addr)) = Self::parse_source(input)?;
        let (input, _) = space0b(input)?;
        let (input, cmd) = Self::parse_quoted_ascii_cmd(input)?;
        let (input, _) = space0b(input)?;

        let args = if parse_args {
            let (input, args) = Self::parse_escaped_args(input)?;
            if !input.is_empty() {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    ErrorKind::Tag,
                )));
            }
            LineArgs::Parsed(args)
        } else {
            LineArgs::Raw(input)
        };

        Ok((input, Self::new(timestamp, db, addr, cmd, args)))
    }

    fn write_bulk_bytes(writer: &mut dyn Write, bytes: &[u8]) -> Result<()> {
        write!(writer, "${}\r\n", bytes.len())?;
        writer.write_all(bytes)?;
        writer.write_all(b"\r\n")?;
        Ok(())
    }

    pub fn write_resp(&self, writer: &mut dyn Write) -> Result<()> {
        let args = match &self.args {
            LineArgs::Parsed(v) => v,
            LineArgs::Raw(_) => panic!("write_resp needs Parsed args"),
        };

        let total_count = 1 + args.len();
        write!(writer, "*{total_count}\r\n")?;

        Self::write_bulk_bytes(writer, self.cmd.as_bytes())?;
        for arg in args {
            Self::write_bulk_bytes(writer, arg.as_ref())?;
        }
        Ok(())
    }

    pub const fn new(
        timestamp: f64,
        db: u64,
        addr: ClientAddr<'a>,
        cmd: &'a str,
        args: LineArgs<'a>,
    ) -> Self {
        Self {
            timestamp,
            db,
            addr,
            cmd,
            args,
        }
    }
}

impl<'a> LineView<'a> {
    fn is_canonical_uint(bytes: &[u8]) -> bool {
        bytes.len() == 1 || bytes.first() != Some(&b'0')
    }

    fn is_canonical_client(client: &ClientAddrView<'_>) -> bool {
        match client {
            ClientAddrView::Tcp {
                host,
                port,
                bracketed: false,
                ..
            } => {
                host.split(|byte| *byte == b'.')
                    .all(Self::is_canonical_uint)
                    && Self::is_canonical_uint(port)
            }
            ClientAddrView::Lua => true,
            ClientAddrView::Path(_)
            | ClientAddrView::Tcp {
                bracketed: true, ..
            }
            | ClientAddrView::Unknown => false,
        }
    }

    fn parse_timestamp(
        input: &'a [u8],
        plan: ParsePlan,
    ) -> IResult<&'a [u8], (&'a [u8], Option<f64>)> {
        let start = input;
        let (input, integer) = parse_u64_bytes(input)?;
        let (input, _) = tag(".")(input)?;
        let (input, fraction) = parse_u64_bytes(input)?;
        let consumed = start.len() - input.len();
        let timestamp = &start[..consumed];
        let typed =
            if plan.timestamp && (integer.len() > 10 || fraction.len() != 6) {
                Some(parse_f64(start)?.1)
            } else {
                None
            };
        Ok((input, (timestamp, typed)))
    }

    fn parse_port(input: &'a [u8]) -> IResult<&'a [u8], &'a [u8]> {
        map_res(digit1b, |digits| {
            lexical_core::parse::<u16>(digits).map(|_| digits)
        })
        .parse(input)
    }

    fn parse_ipv4(
        input: &'a [u8],
        plan: ParsePlan,
    ) -> IResult<&'a [u8], ClientAddrView<'a>> {
        let host_start = input;
        let (input, a) =
            map_res(digit1b, lexical_core::parse::<u8>).parse(input)?;
        let (input, _) = tag(".")(input)?;
        let (input, b) =
            map_res(digit1b, lexical_core::parse::<u8>).parse(input)?;
        let (input, _) = tag(".")(input)?;
        let (input, c) =
            map_res(digit1b, lexical_core::parse::<u8>).parse(input)?;
        let (input, _) = tag(".")(input)?;
        let (input, d) =
            map_res(digit1b, lexical_core::parse::<u8>).parse(input)?;
        let host_len = host_start.len() - input.len();
        let host = &host_start[..host_len];
        let (input, _) = tag(":")(input)?;
        let (input, port) = Self::parse_port(input)?;
        let ip = plan
            .client_ip
            .then(|| IpAddr::V4(Ipv4Addr::new(a, b, c, d)));

        Ok((
            input,
            ClientAddrView::Tcp {
                host,
                port,
                ip,
                bracketed: false,
            },
        ))
    }

    fn parse_ipv6(input: &'a [u8]) -> IResult<&'a [u8], ClientAddrView<'a>> {
        let (input, _) = tag("[")(input)?;
        let (input, host) = take_until("]")(input)?;
        let (input, _) = tag("]")(input)?;
        let (input, _) = tag(":")(input)?;
        let (input, port) = Self::parse_port(input)?;
        let host_str = std::str::from_utf8(host).map_err(|_| {
            Err::Error(ParseError::from_error_kind(input, ErrorKind::Tag))
        })?;
        let ip = host_str.parse().map_err(|_| {
            Err::Error(ParseError::from_error_kind(input, ErrorKind::Tag))
        })?;

        Ok((
            input,
            ClientAddrView::Tcp {
                host,
                port,
                ip: Some(ip),
                bracketed: true,
            },
        ))
    }

    fn parse_client(
        input: &'a [u8],
        plan: ParsePlan,
    ) -> IResult<&'a [u8], ClientAddrView<'a>> {
        if let Ok((input, path)) = Line::parse_unix(input) {
            Ok((input, ClientAddrView::Path(path)))
        } else if let Ok(result) = Self::parse_ipv4(input, plan) {
            Ok(result)
        } else if let Ok(result) = Self::parse_ipv6(input) {
            Ok(result)
        } else if let Ok((input, ())) = Line::parse_lua(input) {
            Ok((input, ClientAddrView::Lua))
        } else if let Ok((input, _)) = Line::parse_unknown(input) {
            Ok((input, ClientAddrView::Unknown))
        } else {
            Err(Err::Error(ParseError::from_error_kind(
                input,
                ErrorKind::Tag,
            )))
        }
    }

    fn parse_source(
        input: &'a [u8],
        plan: ParsePlan,
    ) -> IResult<&'a [u8], (&'a [u8], ClientAddrView<'a>, bool)> {
        let (input, _) = tag("[")(input)?;
        let (input, db) = parse_u64_bytes(input)?;
        let before_space = input;
        let (input, _) = space0b(input)?;
        let canonical_space = before_space.len() - input.len() == 1
            && before_space.first() == Some(&b' ');
        let (input, addr) = Self::parse_client(input, plan)?;
        let (input, _) = tag("]")(input)?;
        let canonical = canonical_space
            && Self::is_canonical_uint(db)
            && Self::is_canonical_client(&addr);
        Ok((input, (db, addr, canonical)))
    }

    pub fn from_line_bytes(
        input: &'a [u8],
        plan: ParsePlan,
    ) -> IResult<&'a [u8], Self> {
        let (input, (timestamp, typed_timestamp)) =
            Self::parse_timestamp(input, plan)?;
        let before_source_space = input;
        let (input, _) = space0b(input)?;
        let canonical_source_space = before_source_space.len() - input.len()
            == 1
            && before_source_space.first() == Some(&b' ');
        let source_start = input;
        let (input, (db, addr, canonical_source)) =
            Self::parse_source(input, plan)?;
        let before_command_space = input;
        let (input, _) = space0b(input)?;
        let canonical_command_space = before_command_space.len() - input.len()
            == 1
            && before_command_space.first() == Some(&b' ');
        let full_line_start = input;
        let (input, cmd) = Line::parse_quoted_ascii_cmd(input)?;
        let before_args_space = input;
        let (args, _) = space0b(input)?;
        let canonical_args_space = before_args_space.len() - args.len() == 1
            && before_args_space.first() == Some(&b' ');
        let full_line = canonical_args_space.then_some(full_line_start);
        let single_default_tail = (canonical_source_space
            && canonical_source
            && canonical_command_space
            && canonical_args_space)
            .then_some(source_start);

        Ok((
            args,
            Self {
                timestamp,
                typed_timestamp,
                db,
                addr,
                cmd,
                args,
                full_line,
                single_default_tail,
            },
        ))
    }
}

impl<'a> ClientAddr<'a> {
    pub const fn from_path(path: &'a str) -> Self {
        Self::Path(path)
    }

    pub const fn from_addr(addr: IpAddr, port: u16) -> Self {
        Self::Tcp(addr, port)
    }
}

impl std::fmt::Display for ClientAddr<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientAddr::Path(path) => write!(f, "{path}"),
            ClientAddr::Tcp(addr, port) => write!(f, "{addr}:{port}"),
            ClientAddr::Lua => write!(f, "lua"),
            ClientAddr::Unknown => write!(f, "-"),
        }
    }
}

impl Serialize for ClientAddr<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Path(p) => serializer.serialize_str(p),
            Self::Tcp(ip, port) => {
                serializer.serialize_str(&format!("{ip}:{port}"))
            }
            Self::Lua => serializer.serialize_str("lua"),
            Self::Unknown => serializer.serialize_str("-"),
        }
    }
}

fn serialize_args_as_strings<S>(
    args: &LineArgs<'_>,
    s: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match args {
        LineArgs::Parsed(v) => {
            let strs: Vec<Cow<'_, str>> = v
                .iter()
                .map(|a| bytes_to_structured_string(a.as_ref()))
                .collect();
            strs.serialize(s)
        }
        LineArgs::Raw(raw) => {
            let s1 = bytes_to_structured_string(raw);
            s.serialize_str(&s1)
        }
    }
}

fn bytes_to_structured_string(bytes: &[u8]) -> Cow<'_, str> {
    String::from_utf8_lossy(bytes)
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{Line, LineArgs};

    #[test]
    fn parses_argument_containing_unescaped_json_quotes() {
        let payload =
            r#"{"id":"996048d52cd44ebd24cf","wp":{"hits":1131},"relay":null}"#;
        let line = format!(
            r#"1783484211.311904 [0 127.0.0.1:52460] "ZADD" "analytics:measurements" "1783484211.3117671" "{payload}""#
        );

        let (_, parsed) = Line::from_line_bytes(line.as_bytes(), true).unwrap();

        let LineArgs::Parsed(args) = &parsed.args else {
            panic!("expected parsed args");
        };
        assert_eq!(args.len(), 3);
        assert_eq!(args[2].as_ref(), payload.as_bytes());

        let json = serde_json::to_string(&parsed).unwrap();
        let value: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["args"][2], payload);
    }

    #[test]
    fn parses_escaped_quotes_inside_argument() {
        let line = br#"1783484211.311904 [0 127.0.0.1:52460] "SET" "key" "hello \"there\"""#;

        let (_, parsed) = Line::from_line_bytes(line, true).unwrap();

        let LineArgs::Parsed(args) = parsed.args else {
            panic!("expected parsed args");
        };
        assert_eq!(args.len(), 2);
        assert_eq!(args[1].as_ref(), b"hello \"there\"");
    }

    #[test]
    fn parses_unescaped_html_attribute_quotes_inside_argument() {
        let payload = br#"<iframe src="https://example.test/video" width="500" height="281"></iframe>"#;
        let mut line =
            b"1783484211.311904 [0 127.0.0.1:52460] \"SET\" \"key\" \""
                .to_vec();
        line.extend_from_slice(payload);
        line.extend_from_slice(b"\" \"NX\" \"EX\" \"604800\"");

        let (_, parsed) = Line::from_line_bytes(&line, true).unwrap();

        let LineArgs::Parsed(args) = parsed.args else {
            panic!("expected parsed args");
        };
        assert_eq!(args.len(), 5);
        assert_eq!(args[0].as_ref(), b"key");
        assert_eq!(args[1].as_ref(), payload);
        assert_eq!(args[2].as_ref(), b"NX");
    }

    #[test]
    fn parses_php_serialized_empty_string_quotes_inside_argument() {
        let payload =
            br#"a:2:{s:11:"description";s:0:"";s:5:"count";s:1:"1";}"#;
        let mut line =
            b"1783484211.311904 [0 127.0.0.1:52460] \"SET\" \"terms:1\" \""
                .to_vec();
        line.extend_from_slice(payload);
        line.extend_from_slice(b"\" \"NX\" \"EX\" \"604800\"");

        let (_, parsed) = Line::from_line_bytes(&line, true).unwrap();

        let LineArgs::Parsed(args) = parsed.args else {
            panic!("expected parsed args");
        };
        assert_eq!(args.len(), 5);
        assert_eq!(args[0].as_ref(), b"terms:1");
        assert_eq!(args[1].as_ref(), payload);
        assert_eq!(args[2].as_ref(), b"NX");
    }

    #[test]
    fn parses_php_serialized_object_argument() {
        let payload = br#"O:8:"stdClass":9:{s:7:"term_id";s:4:"1504";s:4:"name";s:11:"VIP Recipes";s:11:"description";s:0:"";}"#;
        let mut line =
            b"1783484180.262905 [0 127.0.0.1:40960] \"SET\" \"terms:1504\" \""
                .to_vec();
        line.extend_from_slice(payload);
        line.extend_from_slice(b"\" \"NX\" \"EX\" \"604800\"");

        let (_, parsed) = Line::from_line_bytes(&line, true).unwrap();

        let LineArgs::Parsed(args) = parsed.args else {
            panic!("expected parsed args");
        };
        assert_eq!(args.len(), 5);
        assert_eq!(args[0].as_ref(), b"terms:1504");
        assert_eq!(args[1].as_ref(), payload);
        assert_eq!(args[2].as_ref(), b"NX");
    }

    #[test]
    fn preserves_unknown_backslash_sequences_inside_argument() {
        let payload = br#"a:2:{s:5:"class";s:15:"Foo\Bar\Baz";s:5:"quote";s:8:"it\'s ok";}"#;
        let mut line =
            b"1783484211.311904 [0 127.0.0.1:52460] \"SET\" \"key\" \""
                .to_vec();
        line.extend_from_slice(payload);
        line.extend_from_slice(b"\" \"NX\" \"EX\" \"604800\"");

        let (_, parsed) = Line::from_line_bytes(&line, true).unwrap();

        let LineArgs::Parsed(args) = parsed.args else {
            panic!("expected parsed args");
        };
        assert_eq!(args.len(), 5);
        assert_eq!(args[0].as_ref(), b"key");
        assert_eq!(args[1].as_ref(), payload);
    }
}
