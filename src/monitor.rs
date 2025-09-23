use anyhow::Result;
use serde::{Serialize, Serializer};
//use serde::ser::{Serialize, Serializer};
use std::{
    borrow::Cow,
    io::Write,
    net::{IpAddr, Ipv4Addr},
};

use nom::{
    Err, IResult, Parser,
    branch::alt,
    bytes::complete::{is_not, tag, take_until, take_while, take_while_m_n},
    combinator::{map, map_res, opt, peek, value, verify},
    error::{ErrorKind, FromExternalError, ParseError},
    multi::{fold_many0, separated_list0},
    sequence::preceded,
};

#[derive(Debug, Serialize)]
pub enum LineArgs<'a> {
    #[serde(with = "serde_bytes")]
    Raw(&'a [u8]),
    #[serde(with = "serde_vec_bytes")]
    Parsed(Vec<Vec<u8>>),
}

// helper so Vec<Vec<u8>> serializes as bytes not arrays of ints
mod serde_vec_bytes {
    use serde::{Serialize, Serializer};
    pub fn serialize<S>(v: &Vec<Vec<u8>>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        v.serialize(s)
    }
}

#[derive(Debug, Serialize)]
pub struct Line<'a> {
    pub timestamp: f64,
    pub db: u64,
    pub addr: ClientAddr<'a>,
    pub cmd: &'a str,
    pub args: LineArgs<'a>,
}

#[derive(Debug)]
pub enum ClientAddr<'a> {
    Path(&'a str),
    Tcp(IpAddr, u16),
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringFragment<'a> {
    Literal(&'a [u8]),
    EscapedByte(u8),
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
                    write!(f, "\"{}\"", String::from_utf8_lossy(first))?;
                }
                for a in it {
                    write!(f, " \"{}\"", String::from_utf8_lossy(a))?;
                }
                Ok(())
            }
        }
    }
}

/* ----------------------------- bytes helpers ---------------------------- */

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
fn alpha1b(i: &[u8]) -> IResult<&[u8], &[u8]> {
    take_while(|b| (b as char).is_ascii_alphabetic())(i).and_then(|(r, s)| {
        if s.is_empty() {
            Err(nom::Err::Error(nom::error::Error::new(
                i,
                nom::error::ErrorKind::Alpha,
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

    let scale = 10f64.powi(frac_bytes.len() as i32);
    let val = (int as f64) + (frac as f64) / scale;

    Ok((i, val))
}

impl<'a> Line<'a> {
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

    fn parse_fragment<E>(
        input: &'a [u8],
    ) -> IResult<&'a [u8], StringFragment<'a>, E>
    where
        E: ParseError<&'a [u8]>
            + FromExternalError<&'a [u8], std::num::ParseIntError>,
    {
        alt((
            map(Self::parse_literal, StringFragment::Literal),
            map(Self::parse_escaped_char, StringFragment::EscapedByte),
        ))
        .parse(input)
    }

    fn parse_escaped_string<E>(input: &'a [u8]) -> IResult<&'a [u8], Vec<u8>, E>
    where
        E: ParseError<&'a [u8]>
            + FromExternalError<&'a [u8], std::num::ParseIntError>,
    {
        let mut build = fold_many0(
            Self::parse_fragment,
            Vec::<u8>::new,
            |mut out, frag| {
                match frag {
                    StringFragment::Literal(s) => out.extend_from_slice(s),
                    StringFragment::EscapedByte(b) => out.push(b),
                }
                out
            },
        );

        let (input, _) = tag("\"")(input)?;
        let (input, bytes) = build.parse(input)?;
        let (input, _) = tag("\"")(input)?;
        let (input, _) = opt(tag(" ")).parse(input)?;

        Ok((input, bytes))
    }

    //fn parse_u8_from_bytes<T: std::str::FromStr>(
    //    input: &[u8],
    //) -> IResult<&[u8], T> {
    //    map_res(
    //        recognize(many1(|i| digit1b(i))),
    //        |s: &[u8]| -> Result<T, T::Err> {
    //            let strv = unsafe { std::str::from_utf8_unchecked(s) };
    //            strv.parse::<T>()
    //        },
    //    )
    //    .parse(input)
    //}

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
        let ips = std::str::from_utf8(ipb).map_err(|_| {
            Err::Error(ParseError::from_error_kind(
                input,
                nom::error::ErrorKind::Tag,
            ))
        })?;
        let ip = ips.parse().map_err(|_| {
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

    fn parse_unknown(input: &[u8]) -> IResult<&[u8], &str> {
        let (input, _) = peek(tag("]")).parse(input)?;
        Ok((input, ""))
    }

    fn parse_client(input: &[u8]) -> IResult<&[u8], ClientAddr<'_>> {
        if let Ok((input, path)) = Self::parse_unix(input) {
            Ok((input, ClientAddr::from_path(path)))
        } else if let Ok((input, (addr, port))) = Self::parse_ipv4(input) {
            Ok((input, ClientAddr::from_addr(addr, port)))
        } else if let Ok((input, (addr, port))) = Self::parse_ipv6(input) {
            Ok((input, ClientAddr::from_addr(addr, port)))
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
    fn parse_quoted_ascii_cmd(input: &[u8]) -> IResult<&[u8], &str> {
        let (input, _) = tag("\"")(input)?;
        let (input, cmd_bytes) = alpha1b(input)?;
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
        let (input, _) = opt(tag(" ")).parse(input)?;

        let args = if parse_args {
            let (_, args) =
                separated_list0(space0b, Self::parse_escaped_string)
                    .parse(input)?;
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
            Self::write_bulk_bytes(writer, arg)?;
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

impl<'a> ClientAddr<'a> {
    pub const fn from_path(path: &'a str) -> Self {
        Self::Path(path)
    }

    pub const fn from_addr(addr: IpAddr, port: u16) -> Self {
        Self::Tcp(addr, port)
    }

    pub fn get_short_name(&self) -> Cow<'_, str> {
        match self {
            Self::Path(p) => match p.rsplit('/').next() {
                Some(name) if !name.is_empty() => Cow::Borrowed(name),
                _ => Cow::Borrowed("-"),
            },
            Self::Tcp(_, port) => Cow::Owned(port.to_string()),
            Self::Unknown => Cow::Borrowed("-"),
        }
    }

    pub fn get_host(&self) -> String {
        match self {
            ClientAddr::Tcp(ip, _port) => ip.to_string(),
            _ => "-".to_string(),
        }
    }
}

impl std::fmt::Display for ClientAddr<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientAddr::Path(path) => write!(f, "{path}"),
            ClientAddr::Tcp(addr, port) => write!(f, "{addr}:{port}"),
            ClientAddr::Unknown => write!(f, "-"),
        }
    }
}

impl<'a> Serialize for ClientAddr<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Path(p) => serializer.serialize_str(p),
            Self::Tcp(ip, port) => {
                serializer.serialize_str(&format!("{ip}:{port}"))
            }
            Self::Unknown => serializer.serialize_str("-"),
        }
    }
}
