use crate::config::{ConfigEntry, RedisAuth};
use crate::connection::*;
use crate::CommandStats;
use colored::Color;
use nom::{
    bytes::complete::{escaped, tag, take_until},
    character::complete::{alpha1, digit1, none_of, one_of, space0},
    combinator::{map_res, recognize},
    error::{ErrorKind, ParseError},
    multi::{many0, many1},
    number::complete::double,
    Err, IResult,
};
use std::net::{IpAddr, Ipv4Addr};

#[derive(Debug)]
pub struct MonitorLine<'a> {
    pub timestamp: f64,
    pub db: u64,
    pub addr: ClientAddr<'a>,
    pub cmd: &'a str,
    pub args: Vec<&'a str>,
}

#[derive(Debug)]
pub enum ClientAddr<'a> {
    Path(&'a str),
    Tcp((IpAddr, u16)),
}

impl<'a> MonitorLine<'a> {
    fn parse_from_str<T: std::str::FromStr>(input: &str) -> IResult<&str, T> {
        map_res(recognize(many1(digit1)), |s: &str| s.parse::<T>())(input)
    }

    fn parse_escaped_arg(input: &str) -> IResult<&str, &str> {
        let (input, _) = space0(input)?;
        let (input, _) = tag("\"")(input)?;
        let (input, arg) = escaped(none_of(r#"\""#), '\\', one_of(r#"\"rxabn"#))(input)?;
        let (input, _) = tag("\"")(input)?;
        let (input, _) = space0(input)?;
        Ok((input, arg))
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

    pub fn from_line(input: &'a str) -> IResult<&str, MonitorLine> {
        let (input, timestamp) = double(input)?;
        let (input, _) = space0(input)?;
        let (input, (db, addr)) = Self::parse_source(input)?;

        let (input, _) = space0(input)?;
        let (input, _) = tag("\"")(input)?;
        let (input, cmd) = recognize(many1(alpha1))(input)?;
        let (input, _) = tag("\"")(input)?;
        let (input, args) = many0(Self::parse_escaped_arg)(input)?;

        Ok((input, Self::new(timestamp, db, addr, cmd, args)))
    }

    pub fn new(
        timestamp: f64,
        db: u64,
        addr: ClientAddr<'a>,
        cmd: &'a str,
        args: Vec<&'a str>,
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
    pub fn from_path(path: &'a str) -> Self {
        Self::Path(path)
    }

    pub fn from_addr(addr: IpAddr, port: u16) -> Self {
        Self::Tcp((addr, port))
    }
}

#[derive(Clone)]
pub struct MonitoredInstance {
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

impl MonitoredInstance {
    fn make_fmt_string(name: &Option<String>, addr: &RedisAddr, fmt: &str) -> String {
        let fmt = fmt.to_owned();
        let mut fmt = fmt.replace("{host}", &addr.get_host());

        if let Some(name) = name {
            fmt = fmt.replace("{name}", name);
        };

        if let Some(port) = addr.get_port() {
            fmt = fmt.replace("{port}", &format!("{port}"));
        }

        fmt
    }

    fn new(
        name: Option<String>,
        addr: RedisAddr,
        auth: Option<RedisAuth>,
        color: Option<Color>,
        fmt: Option<String>,
    ) -> Self {
        let fmt =
            Self::make_fmt_string(&name, &addr, &fmt.unwrap_or_else(|| "{host}:{port}".into()));

        Self {
            name,
            addr,
            auth,
            color,
            stats: CommandStats::new(),
            fmt,
        }
    }

    pub fn from_config_entry(name: &str, entry: &ConfigEntry) -> Vec<Self> {
        if entry.cluster {
            let c = Cluster::from_seeds(&entry.get_addresses()).expect("Can't get cluster nodes");
            c.get_primary_nodes()
                .iter()
                .map(|primary| {
                    Self::new(
                        Some(name.to_owned()),
                        primary.addr.to_owned(),
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

    pub fn fmt_str(&self) -> &str {
        &self.fmt
    }

    pub fn get_color(&self) -> Option<Color> {
        self.color
    }

    pub fn get_url_string(&self) -> String {
        self.addr.get_url_string()
    }

    pub fn incr_stats(&mut self, cmd: &str, bytes: usize) {
        self.stats.incr(cmd, bytes);
    }

    pub fn get_auth(&self) -> &Option<RedisAuth> {
        &self.auth
    }
}

impl From<RedisAddr> for MonitoredInstance {
    fn from(addr: RedisAddr) -> Self {
        let fmt = match addr {
            RedisAddr::Tcp(_, _) => "{host}:{port}",
            _ => "{host}",
        };

        Self::new(None, addr, None, None, Some(fmt.to_owned()))
    }
}
