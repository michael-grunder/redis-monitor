use crate::{
    config::{Entry, RedisAuth},
    connection::{Cluster, GetHost, GetPort, RedisAddr},
    stats::Map as CommandStats,
};
use colored::Color;
use serde::Serialize;
use std::{
    borrow::Cow,
    hash::Hash,
    net::{IpAddr, Ipv4Addr},
};

use nom::{
    Err, IResult, Parser,
    branch::alt,
    bytes::complete::{is_not, tag, take_until, take_while_m_n},
    character::complete::{alpha1, char, digit1, space0},
    combinator::{map, map_opt, map_res, opt, recognize, value, verify},
    error::{ErrorKind, FromExternalError, ParseError},
    multi::{fold_many0, many0, many1},
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
    Tcp((IpAddr, u16)),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringFragment<'a> {
    Literal(&'a str),
    EscapedChar(char),
}

impl<'a> Line<'a> {
    fn parse_escaped_hex<E>(input: &'a str) -> IResult<&'a str, char, E>
    where
        E: ParseError<&'a str>
            + FromExternalError<&'a str, std::num::ParseIntError>,
    {
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
    pub fn parse_escaped_string<E>(
        input: &'a str,
    ) -> IResult<&'a str, String, E>
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
    pub fn parse_ipv4(input: &str) -> IResult<&str, (IpAddr, u16)> {
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
    pub fn parse_ipv6(input: &str) -> IResult<&str, (IpAddr, u16)> {
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

    pub fn parse_unix(input: &str) -> IResult<&str, &str> {
        let (input, _) = tag("unix:")(input)?;
        let (input, path) = take_until("]")(input)?;
        Ok((input, path))
    }

    fn parse_client(input: &str) -> IResult<&str, ClientAddr> {
        if let Ok((input, path)) = Self::parse_unix(input) {
            Ok((input, ClientAddr::from_path(path)))
        } else if let Ok((input, (addr, port))) = Self::parse_ipv4(input) {
            Ok((input, ClientAddr::from_addr(addr, port)))
        } else if let Ok((input, (addr, port))) = Self::parse_ipv6(input) {
            Ok((input, ClientAddr::from_addr(addr, port)))
        } else {
            Err(Err::Error(ParseError::from_error_kind(
                input,
                ErrorKind::Tag,
            )))
        }
    }

    fn parse_source(input: &str) -> IResult<&str, (u64, ClientAddr)> {
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
    ) -> IResult<&'a str, Line<'a>> {
        let (input, timestamp) = double(input)?;
        let (input, _) = space0(input)?;
        let (input, (db, addr)) = Self::parse_source(input)?;

        let (input, _) = space0(input)?;
        let (input, _) = tag("\"")(input)?;
        let (input, cmd) = recognize(many1(alpha1)).parse(input)?;

        let (input, _) = tag("\"").parse(input)?;
        let (input, _) = opt(char(' ')).parse(input)?;

        let args = if parse_args {
            let (_, args) = many0(Self::parse_escaped_string).parse(input)?;
            LineArgs::Parsed(args)
        } else {
            LineArgs::Raw(input)
        };

        Ok((input, Self::new(timestamp, db, addr, cmd, args)))
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
        Self::Tcp((addr, port))
    }
}

#[derive(Clone)]
pub struct Instance {
    // The name this instance belongs to (if it's from our config.toml)
    name: Option<String>,

    // The address itself
    addr: RedisAddr,

    auth: Option<RedisAuth>,

    // Format string (defaults to {host}:{port}
    fmt: String,

    color: Option<Color>,

    stats: CommandStats,
}

impl Hash for Instance {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
    }
}

impl PartialEq for Instance {
    fn eq(&self, other: &Self) -> bool {
        self.addr == other.addr
    }
}

impl Eq for Instance {}

impl Instance {
    fn make_fmt_string(
        name: Option<&String>,
        addr: &RedisAddr,
        fmt: &str,
    ) -> String {
        let mut fmt = fmt.to_owned();

        let vars: &[(&'static str, Option<String>)] = &[
            ("{host}", Some(addr.get_host().to_string())),
            ("{port}", addr.get_port().map(|p| p.to_string())),
            ("{name}", name.map(|n| n.to_string())),
        ];

        for (var, value) in vars {
            if let Some(v) = value {
                fmt = fmt.replace(var, v);
            }
        }

        fmt
    }

    pub fn new(
        name: Option<String>,
        addr: RedisAddr,
        auth: Option<RedisAuth>,
        color: Option<Color>,
        fmt: Option<String>,
    ) -> Self {
        let fmt = Self::make_fmt_string(
            name.as_ref(),
            &addr,
            &fmt.unwrap_or_else(|| "{host}:{port}".into()),
        );

        Self {
            name,
            addr,
            auth,
            color,
            stats: CommandStats::new(),
            fmt,
        }
    }

    //pub fn from_cluster_seeds(
    //    addresses: &[RedisAddr],
    //) -> anyhow::Result<Vec<Self>> {
    //    for address in addresses {
    //        let cluster = Cluster::from_seeds(&[address.clone()])?;
    //    }
    //    let cluster = Cluster::from_seeds(addresses)?;
    //    Ok(vec![])
    //}

    pub fn from_config_entry(name: &str, entry: &Entry) -> Vec<Self> {
        if entry.cluster {
            let c = Cluster::from_seeds(&entry.get_addresses())
                .expect("Can't get cluster nodes");
            c.get_nodes()
                .iter()
                .map(|primary| {
                    Self::new(
                        Some(name.to_owned()),
                        primary.addr.clone(),
                        entry.get_auth(),
                        entry.get_color(),
                        entry.format.clone(),
                    )
                })
                .collect()
        } else {
            entry
                .get_addresses()
                .iter()
                .map(|addr| {
                    Self::new(
                        Some(name.to_owned()),
                        addr.to_owned(),
                        entry.get_auth(),
                        entry.get_color(),
                        entry.format.clone(),
                    )
                })
                .collect()
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn fmt_str(&self) -> &str {
        &self.fmt
    }

    pub const fn get_color(&self) -> Option<Color> {
        self.color
    }

    pub fn get_url_string(&self) -> String {
        self.addr.get_url_string()
    }

    pub fn incr_stats(&mut self, cmd: &str, bytes: usize) {
        self.stats.incr(cmd, bytes);
    }

    pub const fn get_auth(&self) -> Option<&RedisAuth> {
        self.auth.as_ref()
    }
}

impl From<RedisAddr> for Instance {
    fn from(addr: RedisAddr) -> Self {
        let fmt = match addr {
            RedisAddr::Tcp(_, _) => "{host}:{port}",
            RedisAddr::Unix(_) => "{host}",
        };

        Self::new(None, addr, None, None, Some(fmt.to_owned()))
    }
}
