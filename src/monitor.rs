use anyhow::Result;
use serde::Serialize;
use std::{
    borrow::Cow,
    io::Write,
    net::{IpAddr, Ipv4Addr},
};

use nom::{
    Err, IResult, Parser,
    branch::alt,
    bytes::complete::{is_not, tag, take_until, take_while_m_n},
    character::complete::{alpha1, char, digit1, space0},
    combinator::{map, map_opt, map_res, opt, peek, recognize, value, verify},
    error::{ErrorKind, FromExternalError, ParseError},
    multi::{fold_many0, many1, separated_list0},
    number::complete::double,
    sequence::preceded,
};

#[derive(Debug, Serialize)]
pub enum LineArgs<'a> {
    Raw(&'a str),
    Parsed(Vec<String>),
}

#[derive(Debug, Serialize)]
pub struct Line<'a> {
    pub timestamp: f64,
    pub db: u64,
    pub addr: ClientAddr<'a>,
    pub cmd: &'a str,
    pub args: LineArgs<'a>,
}

#[derive(Debug, Serialize)]
pub enum ClientAddr<'a> {
    Path(&'a str),
    Tcp(IpAddr, u16),
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringFragment<'a> {
    Literal(&'a str),
    EscapedChar(char),
}

impl std::fmt::Display for LineArgs<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LineArgs::Raw(raw) => write!(f, "{raw}"),
            LineArgs::Parsed(args) => {
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }
                    write!(f, "{arg}")?;
                }
                Ok(())
            }
        }
    }
}

impl<'a> Line<'a> {
    fn parse_escaped_hex<E>(input: &'a str) -> IResult<&'a str, char, E>
    where
        E: ParseError<&'a str>
            + FromExternalError<&'a str, std::num::ParseIntError>,
    {
        let (input, _) = char('x').parse(input)?;

        let parse_hex = take_while_m_n(2, 2, |c: char| c.is_ascii_hexdigit());

        let parse_u8 =
            map_res(parse_hex, move |hex| u8::from_str_radix(hex, 16));

        map_opt(parse_u8, |value| std::char::from_u32(u32::from(value)))
            .parse(input)
    }

    fn parse_escaped_char<E>(input: &'a str) -> IResult<&'a str, char, E>
    where
        E: ParseError<&'a str>
            + FromExternalError<&'a str, std::num::ParseIntError>,
    {
        preceded(
            char('\\'),
            alt((
                Self::parse_escaped_hex,
                value('\n', char('n')),
                value('\r', char('r')),
                value('\t', char('t')),
                value('\x07', char('a')),
                value('\u{08}', char('b')),
                value('\u{0C}', char('f')),
                value('\\', char('\\')),
                value('/', char('/')),
                value('"', char('"')),
                value(' ', char(' ')),
            )),
        )
        .parse(input)
    }

    /// Parse a non-empty block of text that doesn't include \ or "
    fn parse_literal<E: ParseError<&'a str>>(
        input: &'a str,
    ) -> IResult<&'a str, &'a str, E> {
        let not_quote_slash = is_not("\"\\");

        verify(not_quote_slash, |s: &str| !s.is_empty()).parse(input)
    }

    fn parse_fragment<E>(
        input: &'a str,
    ) -> IResult<&'a str, StringFragment<'a>, E>
    where
        E: ParseError<&'a str>
            + FromExternalError<&'a str, std::num::ParseIntError>,
    {
        alt((
            map(Self::parse_literal, StringFragment::Literal),
            map(Self::parse_escaped_char, StringFragment::EscapedChar),
        ))
        .parse(input)
    }

    /// Parse a string. Use a loop of `parse_fragment` and push all of the fragments
    /// into an output string.
    fn parse_escaped_string<E>(input: &'a str) -> IResult<&'a str, String, E>
    where
        E: ParseError<&'a str>
            + FromExternalError<&'a str, std::num::ParseIntError>,
    {
        // fold_many0 is the equivalent of iterator::fold. It runs a parser in a loop,
        // and for each output value, calls a folding function on each output value.
        let mut build_string = fold_many0(
            // Our parser function– parses a single string fragment
            Self::parse_fragment,
            // Our init value, an empty string
            String::new,
            // Our folding function. For each fragment, append the fragment to the
            // string.
            |mut string, fragment| {
                match fragment {
                    StringFragment::Literal(s) => string.push_str(s),
                    StringFragment::EscapedChar(c) => string.push(c),
                }
                string
            },
        );

        let (input, _) = tag("\"")(input)?;
        let (input, string) = build_string.parse(input)?;
        let (input, _) = tag("\"")(input)?;
        let (input, _) = opt(char(' ')).parse(input)?;

        Ok((input, string))
    }

    fn parse_from_str<T: std::str::FromStr>(input: &str) -> IResult<&str, T> {
        map_res(recognize(many1(digit1)), |s: &str| s.parse::<T>()).parse(input)
    }

    // aaa.bbb.ccc.ddd (127.0.0.1)
    fn parse_ipv4(input: &str) -> IResult<&str, (IpAddr, u16)> {
        let (input, a) = Self::parse_from_str::<u8>(input)?;
        let (input, _) = tag(".")(input)?;
        let (input, b) = Self::parse_from_str::<u8>(input)?;
        let (input, _) = tag(".")(input)?;
        let (input, c) = Self::parse_from_str::<u8>(input)?;
        let (input, _) = tag(".")(input)?;
        let (input, d) = Self::parse_from_str::<u8>(input)?;

        let (input, _) = tag(":")(input)?;
        let (input, port) = Self::parse_from_str::<u16>(input)?;

        let ip = IpAddr::V4(Ipv4Addr::new(a, b, c, d));

        Ok((input, (ip, port)))
    }

    // [0 [::1]:53374]
    fn parse_ipv6(input: &str) -> IResult<&str, (IpAddr, u16)> {
        let (input, _) = tag("[")(input)?;
        let (input, ip) = take_until("]")(input)?;
        let (input, _) = tag("]")(input)?;
        let (input, _) = tag(":")(input)?;
        let (input, port) = Self::parse_from_str::<u16>(input)?;

        let ip = ip.parse().map_err(|_| {
            Err::Error(ParseError::from_error_kind(
                input,
                nom::error::ErrorKind::Tag,
            ))
        })?;

        Ok((input, (ip, port)))
    }

    fn parse_unix(input: &str) -> IResult<&str, &str> {
        let (input, _) = tag("unix:")(input)?;
        let (input, path) = take_until("]")(input)?;
        Ok((input, path))
    }

    fn parse_unknown(input: &str) -> IResult<&str, &str> {
        let (input, _) = peek(tag("]")).parse(input)?;
        Ok((input, ""))
    }

    fn parse_client(input: &str) -> IResult<&str, ClientAddr<'_>> {
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

    fn parse_source(input: &str) -> IResult<&str, (u64, ClientAddr<'_>)> {
        let (input, _) = tag("[")(input)?;
        let (input, db) = Self::parse_from_str::<u64>(input)?;
        let (input, _) = space0(input)?;
        let (input, addr) = Self::parse_client(input)?;
        let (input, _) = tag("]")(input)?;

        Ok((input, (db, addr)))
    }

    pub fn from_line(
        input: &'a str,
        parse_args: bool,
    ) -> IResult<&'a str, Self> {
        let (input, timestamp) = double(input)?;
        let (input, _) = space0(input)?;
        let (input, (db, addr)) = Self::parse_source(input)?;

        let (input, _) = space0(input)?;
        let (input, _) = tag("\"")(input)?;
        let (input, cmd) = recognize(many1(alpha1)).parse(input)?;

        let (input, _) = tag("\"").parse(input)?;
        let (input, _) = opt(char(' ')).parse(input)?;

        let args = if parse_args {
            let (_, args) = separated_list0(space0, Self::parse_escaped_string)
                .parse(input)?;
            LineArgs::Parsed(args)
        } else {
            LineArgs::Raw(input)
        };

        Ok((input, Self::new(timestamp, db, addr, cmd, args)))
    }

    fn write_bulk_string(writer: &mut dyn Write, string: &str) -> Result<()> {
        write!(writer, "${}\r\n{string}\r\n", string.len())?;
        Ok(())
    }

    pub fn write_resp(&self, writer: &mut dyn Write) -> Result<()> {
        let args = match &self.args {
            LineArgs::Parsed(v) => v,
            LineArgs::Raw(_) => {
                panic!("write_resp called with LineArgs::Raw");
            }
        };

        let total_count = 1 + args.len();

        write!(writer, "*{total_count}\r\n")?;

        Self::write_bulk_string(writer, self.cmd)?;

        for arg in args {
            Self::write_bulk_string(writer, arg)?;
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
