//! Byte-oriented parsing of Redis `MONITOR` records:
//!
//! ```text
//! <secs>.<fraction> [<db> <client>] "<command>" "<arg>" ...
//! ```
//!
//! [`Record::parse`] validates and borrows the record prefix without
//! allocating. Arguments stay escaped until a caller needs their bytes, when
//! [`Record::decode_args`] decodes them into reusable scratch storage.
use std::{
    borrow::Cow,
    fmt,
    io::{self, Write},
    net::{IpAddr, Ipv4Addr},
};

use lexical_core::FormattedSize;

/// Where and why a record failed to parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseError {
    pub offset: usize,
    pub expected: &'static str,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "expected {} at byte {}", self.expected, self.offset)
    }
}

impl std::error::Error for ParseError {}

type Result<T> = std::result::Result<T, ParseError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Client<'a> {
    Tcp { ip: IpAddr, port: u16 },
    Unix(&'a str),
    Lua,
    Unknown,
}

/// A validated record whose fields borrow the input line.
#[derive(Debug)]
pub struct Record<'a> {
    /// `<secs>.<fraction>`, where both parts are digits that fit a `u64`.
    timestamp: &'a [u8],
    /// Offset of the '.' in `timestamp`.
    dot: usize,
    pub db: u64,
    pub client: Client<'a>,
    /// Printable ASCII, excluding quotes, backslashes, and spaces.
    pub cmd: &'a [u8],
    /// The still-escaped arguments following the command.
    pub args: &'a [u8],
    /// `"<command>" <args>`, when the input spells it exactly as `%l`
    /// renders it.
    pub full_line: Option<&'a [u8]>,
    /// Everything after the timestamp, when the input spells it exactly as
    /// the default single-instance format renders it.
    pub default_tail: Option<&'a [u8]>,
}

/// Extract the command name (between the first pair of quotes) without
/// validating the rest of the record.
#[inline]
#[must_use]
pub fn command_name(line: &[u8]) -> Option<&[u8]> {
    let start = memchr::memchr(b'"', line)? + 1;
    let len = memchr::memchr(b'"', &line[start..])?;
    Some(&line[start..start + len])
}

/// Extract the database number without validating the rest of the record.
#[inline]
#[must_use]
pub fn database(line: &[u8]) -> Option<u64> {
    let start = memchr::memchr(b'[', line)? + 1;
    let digits = &line[start..];
    let len = digits
        .iter()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(digits.len());
    lexical_core::parse(&digits[..len]).ok()
}

/// A forward-only cursor over one record.
struct Scanner<'a> {
    line: &'a [u8],
    /// The unconsumed suffix of `line`.
    rest: &'a [u8],
}

impl<'a> Scanner<'a> {
    const fn new(line: &'a [u8]) -> Self {
        Self { line, rest: line }
    }

    /// Offset of the cursor within the line.
    const fn pos(&self) -> usize {
        self.line.len() - self.rest.len()
    }

    const fn err(&self, expected: &'static str) -> ParseError {
        ParseError {
            offset: self.pos(),
            expected,
        }
    }

    const fn eat(&mut self, byte: u8) -> bool {
        match self.rest {
            [first, rest @ ..] if *first == byte => {
                self.rest = rest;
                true
            }
            _ => false,
        }
    }

    const fn expect(&mut self, byte: u8, expected: &'static str) -> Result<()> {
        if self.eat(byte) {
            Ok(())
        } else {
            Err(self.err(expected))
        }
    }

    fn take_while(&mut self, pred: impl Fn(u8) -> bool) -> &'a [u8] {
        let len = self
            .rest
            .iter()
            .position(|&b| !pred(b))
            .unwrap_or(self.rest.len());
        let (taken, rest) = self.rest.split_at(len);
        self.rest = rest;
        taken
    }

    /// Skip spaces and tabs, reporting whether exactly one space was skipped.
    fn space(&mut self) -> Separator {
        match self.take_while(|b| matches!(b, b' ' | b'\t')) {
            b"" => Separator::None,
            b" " => Separator::Single,
            _ => Separator::Other,
        }
    }

    /// One or more digits whose value fits `T`. Returns the value and
    /// whether its spelling is canonical (no leading zeros).
    fn number<T: TryFrom<u64>>(
        &mut self,
        expected: &'static str,
    ) -> Result<(T, bool)> {
        let before = self.rest;
        let digits = self.take_while(|b| b.is_ascii_digit());
        // Up to 19 digits cannot overflow a `u64`; only longer spellings
        // (such as leading zeros) need checked arithmetic.
        let value = if digits.len() < 20 {
            Some(
                digits
                    .iter()
                    .fold(0u64, |v, &d| v * 10 + u64::from(d - b'0')),
            )
        } else {
            digits.iter().try_fold(0u64, |v, &d| {
                v.checked_mul(10)?.checked_add(u64::from(d - b'0'))
            })
        };
        match value.and_then(|v| T::try_from(v).ok()) {
            Some(value) if !digits.is_empty() => {
                Ok((value, digits.len() == 1 || digits[0] != b'0'))
            }
            _ => {
                self.rest = before;
                Err(self.err(expected))
            }
        }
    }

    fn take_until(
        &mut self,
        byte: u8,
        expected: &'static str,
    ) -> Result<&'a [u8]> {
        let len = memchr::memchr(byte, self.rest)
            .ok_or_else(|| self.err(expected))?;
        let (taken, rest) = self.rest.split_at(len);
        self.rest = rest;
        Ok(taken)
    }

    /// Returns the timestamp text and the offset of its '.'.
    fn timestamp(&mut self) -> Result<(&'a [u8], usize)> {
        let start = self.rest;
        self.number::<u64>("timestamp seconds")?;
        let dot = start.len() - self.rest.len();
        self.expect(b'.', "'.' in timestamp")?;
        self.number::<u64>("timestamp fraction")?;
        Ok((&start[..start.len() - self.rest.len()], dot))
    }

    /// `<client>` inside the source brackets. Returns whether its spelling is
    /// exactly how it is rendered.
    fn client(&mut self) -> Result<(Client<'a>, bool)> {
        if let Some(rest) = self.rest.strip_prefix(b"unix:") {
            self.rest = rest;
            let path = self.take_until(b']', "']' after unix path")?;
            let path = std::str::from_utf8(path)
                .map_err(|_| self.err("UTF-8 unix path"))?;
            return Ok((Client::Unix(path), false));
        }
        if let Some(rest) = self.rest.strip_prefix(b"lua") {
            self.rest = rest;
            return Ok((Client::Lua, true));
        }
        match self.rest.first() {
            Some(b'0'..=b'9') => {
                let mut octets = [0u8; 4];
                let mut canonical = true;
                for (i, octet) in octets.iter_mut().enumerate() {
                    if i > 0 {
                        self.expect(b'.', "'.' in IPv4 address")?;
                    }
                    let (value, plain) = self.number("IPv4 octet")?;
                    *octet = value;
                    canonical &= plain;
                }
                self.expect(b':', "':' after IPv4 address")?;
                let (port, plain) = self.number("client port")?;
                let ip = IpAddr::V4(Ipv4Addr::from(octets));
                Ok((Client::Tcp { ip, port }, canonical && plain))
            }
            Some(b'[') => {
                self.rest = &self.rest[1..];
                let host = self.take_until(b']', "']' after IPv6 address")?;
                let ip = std::str::from_utf8(host)
                    .ok()
                    .and_then(|host| host.parse().ok())
                    .ok_or_else(|| self.err("IP address"))?;
                self.rest = &self.rest[1..];
                self.expect(b':', "':' after IPv6 address")?;
                let (port, _) = self.number("client port")?;
                Ok((Client::Tcp { ip, port }, false))
            }
            Some(b']') => Ok((Client::Unknown, false)),
            _ => Err(self.err("client address")),
        }
    }

    fn command(&mut self) -> Result<&'a [u8]> {
        self.expect(b'"', "'\"' before command")?;
        let name = self.take_while(is_command_byte);
        if name.is_empty() {
            return Err(self.err("command name"));
        }
        self.expect(b'"', "'\"' after command")?;
        Ok(name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Separator {
    None,
    Single,
    Other,
}

/// Module commands commonly contain punctuation such as `FT.SEARCH`; quotes,
/// backslashes, and spaces never appear in a valid unescaped name.
const fn is_command_byte(b: u8) -> bool {
    b.is_ascii_graphic() && b != b'"' && b != b'\\'
}

impl<'a> Record<'a> {
    /// Validate a record prefix, borrowing every field from `line`.
    ///
    /// # Errors
    /// Returns the first offset that does not match the record grammar.
    /// Arguments are validated separately by [`Self::decode_args`].
    pub fn parse(line: &'a [u8]) -> Result<Self> {
        let mut s = Scanner::new(line);
        let (timestamp, dot) = s.timestamp()?;
        let source_space = s.space();
        let source_start = s.pos();
        s.expect(b'[', "'[' before source")?;
        let (db, canonical_db) = s.number("database")?;
        let db_space = s.space();
        let (client, canonical_client) = s.client()?;
        s.expect(b']', "']' after source")?;
        let command_space = s.space();
        let command_start = s.pos();
        let cmd = s.command()?;
        let args_space = s.space();
        let args = s.rest;

        // `%l` renders `"<command>"` plus a single space and the arguments,
        // when there are any.
        let canonical_args = if args.is_empty() {
            args_space == Separator::None
        } else {
            args_space == Separator::Single
        };
        let full_line = canonical_args.then(|| &line[command_start..]);
        let default_tail = (canonical_args
            && source_space == Separator::Single
            && canonical_db
            && db_space == Separator::Single
            && canonical_client
            && command_space == Separator::Single)
            .then(|| &line[source_start..]);

        Ok(Self {
            timestamp,
            dot,
            db,
            client,
            cmd,
            args,
            full_line,
            default_tail,
        })
    }

    /// The command name as text. Command names are ASCII, so this never
    /// needs to validate anything but is kept off the plain-output path.
    #[must_use]
    pub fn cmd_str(&self) -> &'a str {
        std::str::from_utf8(self.cmd).unwrap_or_default()
    }

    /// The timestamp as a float, correctly rounded from its text. Values
    /// with more than about 16 significant digits lose precision.
    #[must_use]
    pub fn timestamp(&self) -> f64 {
        // Parsing validated `<digits>.<digits>`, which always parses.
        lexical_core::parse(self.timestamp).unwrap_or(f64::NAN)
    }

    /// Decode arguments into `args`, replacing its contents. Unescaped
    /// arguments are borrowed; only arguments containing escapes allocate.
    ///
    /// # Errors
    /// Returns an error for unterminated or unseparated arguments. `args` may
    /// then hold a partial decode and must not be used.
    pub fn decode_args(&self, args: &mut Vec<Cow<'a, [u8]>>) -> Result<()> {
        decode_args(self.args, args)
    }

    /// Write the timestamp as MONITOR text normalized like a decimal number:
    /// no leading zeros in the seconds and no trailing zeros in the fraction.
    ///
    /// # Errors
    /// Returns any error from `w`.
    pub fn write_timestamp(&self, w: &mut impl Write) -> io::Result<()> {
        let secs = &self.timestamp[..self.dot];
        let fraction = &self.timestamp[self.dot + 1..];
        // Both parts are non-empty digit strings.
        let first_nonzero = secs
            .iter()
            .position(|&b| b != b'0')
            .unwrap_or(secs.len() - 1);
        w.write_all(&secs[first_nonzero..])?;
        if let Some(last) = fraction.iter().rposition(|&b| b != b'0') {
            w.write_all(b".")?;
            w.write_all(&fraction[..=last])?;
        }
        Ok(())
    }
}

/// Room for any rendered TCP client address, such as `[<ipv6>]:65535`.
pub const MAX_TCP_ADDR_LEN: usize = 64;

impl Client<'_> {
    /// Write the address as `ip:port`, `[ipv6]:port`, a unix path, `lua`,
    /// or `-`.
    ///
    /// # Errors
    /// Returns any error from `w`.
    pub fn write_addr(&self, w: &mut impl Write) -> io::Result<()> {
        match self {
            Client::Tcp { ip, port } => {
                if ip.is_ipv6() {
                    w.write_all(b"[")?;
                    write_ip(w, *ip)?;
                    w.write_all(b"]:")?;
                } else {
                    write_ip(w, *ip)?;
                    w.write_all(b":")?;
                }
                write_uint(w, *port)
            }
            Client::Unix(path) => w.write_all(path.as_bytes()),
            Client::Lua => w.write_all(b"lua"),
            Client::Unknown => w.write_all(b"-"),
        }
    }

    /// Write the host part: the IP address, or `-` for non-TCP clients.
    ///
    /// # Errors
    /// Returns any error from `w`.
    pub fn write_host(&self, w: &mut impl Write) -> io::Result<()> {
        match self {
            Client::Tcp { ip, .. } => write_ip(w, *ip),
            Client::Unix(_) | Client::Lua | Client::Unknown => {
                w.write_all(b"-")
            }
        }
    }

    /// Write the port, the unix socket's file name, `lua`, or `-`.
    ///
    /// # Errors
    /// Returns any error from `w`.
    pub fn write_port(&self, w: &mut impl Write) -> io::Result<()> {
        match self {
            Client::Tcp { port, .. } => write_uint(w, *port),
            Client::Unix(path) => w.write_all(
                path.rsplit('/')
                    .next()
                    .filter(|name| !name.is_empty())
                    .unwrap_or("-")
                    .as_bytes(),
            ),
            Client::Lua => w.write_all(b"lua"),
            Client::Unknown => w.write_all(b"-"),
        }
    }

    /// Render the address into a stack buffer, returning it as text.
    /// Returns `None` for unix paths, which callers should use directly.
    pub fn render_tcp<'b>(
        &self,
        buf: &'b mut [u8; MAX_TCP_ADDR_LEN],
    ) -> Option<&'b str> {
        if !matches!(self, Client::Tcp { .. }) {
            return None;
        }
        let mut cursor = &mut buf[..];
        self.write_addr(&mut cursor).ok()?;
        let len = MAX_TCP_ADDR_LEN - cursor.len();
        std::str::from_utf8(&buf[..len]).ok()
    }
}

fn write_ip(w: &mut impl Write, ip: IpAddr) -> io::Result<()> {
    match ip {
        IpAddr::V4(ip) => {
            for (i, octet) in ip.octets().into_iter().enumerate() {
                if i > 0 {
                    w.write_all(b".")?;
                }
                write_uint(w, octet)?;
            }
            Ok(())
        }
        // Rare enough that the formatting machinery's cost is irrelevant.
        IpAddr::V6(ip) => write!(w, "{ip}"),
    }
}

/// Write an unsigned integer in decimal without the formatting machinery.
///
/// # Errors
/// Returns any error from `w`.
pub fn write_uint(w: &mut impl Write, n: impl Into<u64>) -> io::Result<()> {
    let mut buf = [0u8; u64::FORMATTED_SIZE_DECIMAL];
    w.write_all(lexical_core::write(n.into(), &mut buf))
}

/// Decode escaped, quoted MONITOR arguments into `args`.
///
/// Redis escapes quotes inside arguments, but some producers do not, so a
/// quote only closes an argument when followed by optional whitespace and then
/// either the end of the record or another opening quote. Unknown escape
/// sequences are preserved literally.
///
/// # Errors
/// Returns an error for unterminated or unseparated arguments. `args` may
/// then hold a partial decode and must not be used.
pub fn decode_args<'a>(
    input: &'a [u8],
    args: &mut Vec<Cow<'a, [u8]>>,
) -> Result<()> {
    args.clear();
    let mut pos = 0;
    while pos < input.len() {
        if input[pos] != b'"' {
            return Err(ParseError {
                offset: pos,
                expected: "'\"' before argument",
            });
        }
        let (arg, end) = decode_arg(input, pos + 1)?;
        args.push(arg);

        let spaces = input[end..]
            .iter()
            .take_while(|b| matches!(b, b' ' | b'\t'))
            .count();
        if spaces == 0 && end < input.len() {
            return Err(ParseError {
                offset: end,
                expected: "space between arguments",
            });
        }
        pos = end + spaces;
    }
    Ok(())
}

/// Decode one argument whose content starts at `start`. Returns the argument
/// and the offset just past its closing quote. Runs in linear time: each byte
/// is scanned once, jumping between quotes and backslashes.
fn decode_arg(input: &[u8], start: usize) -> Result<(Cow<'_, [u8]>, usize)> {
    let mut owned: Option<Vec<u8>> = None;
    let mut pos = start;
    loop {
        let special = memchr::memchr2(b'"', b'\\', &input[pos..])
            .map(|len| pos + len)
            .ok_or(ParseError {
                offset: input.len(),
                expected: "closing '\"'",
            })?;
        if let Some(buf) = &mut owned {
            buf.extend_from_slice(&input[pos..special]);
        }

        if input[special] == b'"' && closes_argument(&input[special + 1..]) {
            let arg = owned.map_or_else(
                || Cow::Borrowed(&input[start..special]),
                Cow::Owned,
            );
            return Ok((arg, special + 1));
        }

        // First escape or embedded quote: switch to an owned copy, which
        // already includes everything scanned so far.
        let buf = owned.get_or_insert_with(|| input[start..special].to_vec());
        if input[special] == b'"' {
            buf.push(b'"');
            pos = special + 1;
        } else if let Some((byte, len)) = unescape(&input[special + 1..]) {
            buf.push(byte);
            pos = special + 1 + len;
        } else {
            buf.push(b'\\');
            pos = special + 1;
        }
    }
}

/// Whether a quote followed by `after` closes an argument.
fn closes_argument(after: &[u8]) -> bool {
    match after
        .iter()
        .position(|b| !matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
    {
        None => true,
        Some(0) => false,
        Some(next) => after[next] == b'"',
    }
}

/// Decode the escape following a backslash, returning the byte and the
/// number of input bytes consumed after the backslash.
fn unescape(input: &[u8]) -> Option<(u8, usize)> {
    const fn nibble(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }

    let byte = match *input.first()? {
        b'x' => {
            let hi = nibble(*input.get(1)?)?;
            let lo = nibble(*input.get(2)?)?;
            return Some(((hi << 4) | lo, 3));
        }
        b'n' => b'\n',
        b'r' => b'\r',
        b't' => b'\t',
        b'a' => 0x07,
        b'b' => 0x08,
        b'f' => 0x0C,
        c @ (b'\\' | b'/' | b'"' | b' ') => c,
        _ => return None,
    };
    Some((byte, 1))
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::{Client, Record, command_name, database, decode_args};

    fn args(line: &[u8]) -> Vec<Vec<u8>> {
        let record = Record::parse(line).unwrap();
        let mut args = Vec::new();
        record.decode_args(&mut args).unwrap();
        args.into_iter().map(Cow::into_owned).collect()
    }

    /// Escape like Redis `sdscatrepr`, which MONITOR uses for arguments.
    fn repr(bytes: &[u8]) -> Vec<u8> {
        let mut out = b"\"".to_vec();
        for &b in bytes {
            match b {
                b'\\' | b'"' => out.extend([b'\\', b]),
                b'\n' => out.extend(b"\\n"),
                b'\r' => out.extend(b"\\r"),
                b'\t' => out.extend(b"\\t"),
                0x07 => out.extend(b"\\a"),
                0x08 => out.extend(b"\\b"),
                _ if b.is_ascii_graphic() || b == b' ' => out.push(b),
                _ => out.extend(format!("\\x{b:02x}").bytes()),
            }
        }
        out.push(b'"');
        out
    }

    #[test]
    fn parses_every_field_of_a_record() {
        let record = Record::parse(
            br#"1783484211.311904 [3 127.0.0.1:52460] "SET" "k" "v""#,
        )
        .unwrap();
        assert_eq!(record.db, 3);
        assert_eq!(
            record.client,
            Client::Tcp {
                ip: "127.0.0.1".parse().unwrap(),
                port: 52460
            }
        );
        assert_eq!(record.cmd, b"SET");
        assert_eq!(record.args, br#""k" "v""#);
        assert_eq!(record.full_line, Some(&br#""SET" "k" "v""#[..]));
        assert_eq!(
            record.default_tail,
            Some(&br#"[3 127.0.0.1:52460] "SET" "k" "v""#[..])
        );
    }

    #[test]
    fn parses_every_client_form() {
        for (client, expected) in [
            ("127.0.0.1:1", "127.0.0.1:1"),
            ("[::1]:6379", "[::1]:6379"),
            ("[2001:0db8::1]:1", "[2001:db8::1]:1"),
            ("unix:/tmp/redis.sock", "/tmp/redis.sock"),
            ("lua", "lua"),
            ("", "-"),
        ] {
            let line = format!(r#"1.0 [0 {client}] "PING""#);
            let record = Record::parse(line.as_bytes()).unwrap();
            let mut out = Vec::new();
            record.client.write_addr(&mut out).unwrap();
            assert_eq!(String::from_utf8(out).unwrap(), expected, "{line}");
        }
    }

    #[test]
    fn numeric_fields_are_range_checked() {
        let ok = |line: &str| Record::parse(line.as_bytes()).is_ok();
        assert!(ok(
            r#"18446744073709551615.18446744073709551615 [0 lua] "X""#
        ));
        assert!(!ok(r#"18446744073709551616.0 [0 lua] "X""#));
        assert!(!ok(r#"1.18446744073709551616 [0 lua] "X""#));
        assert!(ok(r#"1.0 [18446744073709551615 lua] "X""#));
        assert!(!ok(r#"1.0 [18446744073709551616 lua] "X""#));
        // Leading zeros may exceed 19 digits without overflowing.
        assert!(ok(r#"000000000000000000000001.0 [0 lua] "X""#));
        assert!(ok(r#"1.0 [0 255.255.255.255:65535] "X""#));
        assert!(!ok(r#"1.0 [0 256.0.0.1:1] "X""#));
        assert!(!ok(r#"1.0 [0 1.2.3.4:65536] "X""#));
        assert!(!ok(r#"1.0 [0 1.2.3:4] "X""#));
        assert!(!ok(r#"1. [0 lua] "X""#));
        assert!(!ok(r#".1 [0 lua] "X""#));
    }

    #[test]
    fn errors_report_the_failing_offset() {
        let error = Record::parse(br#"1.0 [0 lua] "BAD CMD""#).unwrap_err();
        assert_eq!(error.offset, 16);
        assert_eq!(error.expected, "'\"' after command");
        assert_eq!(
            Record::parse(b"").unwrap_err().to_string(),
            "expected timestamp seconds at byte 0"
        );
    }

    #[test]
    fn timestamps_are_parsed_with_correct_rounding() {
        for text in ["1783484211.311904", "0.1", "1783484211.999999"] {
            let line = format!(r#"{text} [0 127.0.0.1:1] "PING""#);
            let record = Record::parse(line.as_bytes()).unwrap();
            assert_eq!(
                record.timestamp().to_bits(),
                text.parse::<f64>().unwrap().to_bits()
            );
        }
    }

    #[test]
    fn timestamp_text_is_normalized_without_rounding() {
        for (text, expected) in [
            ("1783484211.311904", "1783484211.311904"),
            ("1783484211.300000", "1783484211.3"),
            ("1783484211.000000", "1783484211"),
            ("0001.5", "1.5"),
            ("0.0", "0"),
            ("12345678901.123456", "12345678901.123456"),
        ] {
            let line = format!(r#"{text} [0 lua] "PING""#);
            let mut out = Vec::new();
            Record::parse(line.as_bytes())
                .unwrap()
                .write_timestamp(&mut out)
                .unwrap();
            assert_eq!(String::from_utf8(out).unwrap(), expected);
        }
    }

    #[test]
    fn module_command_names_are_accepted() {
        for cmd in ["FT.SEARCH", "json.set", "_FT.DEBUG", "CF.ADD"] {
            let line = format!(r#"1.0 [0 127.0.0.1:1] "{cmd}" "k""#);
            let record = Record::parse(line.as_bytes()).unwrap();
            assert_eq!(record.cmd_str(), cmd);
        }
    }

    #[test]
    fn escaped_arguments_round_trip_all_bytes() {
        let values: Vec<Vec<u8>> = vec![
            b"plain".to_vec(),
            Vec::new(),
            (0..=255).collect(),
            b"\"quoted\" \\backslash\\ \r\n\t\x07\x08".to_vec(),
            b"\" \"looks like a separator\" \"".to_vec(),
        ];
        let mut line = br#"1.0 [0 lua] "SET""#.to_vec();
        for value in &values {
            line.push(b' ');
            line.extend(repr(value));
        }
        assert_eq!(args(&line), values);
    }

    #[test]
    fn unescaped_arguments_are_borrowed() {
        let record = Record::parse(br#"1.0 [0 lua] "SET" "k" "a\"b""#).unwrap();
        let mut decoded = Vec::new();
        record.decode_args(&mut decoded).unwrap();
        assert!(matches!(decoded[0], Cow::Borrowed(b"k")));
        assert!(matches!(&decoded[1], Cow::Owned(v) if v == b"a\"b"));
    }

    #[test]
    fn parses_argument_containing_unescaped_json_quotes() {
        let payload =
            r#"{"id":"996048d52cd44ebd24cf","wp":{"hits":1131},"relay":null}"#;
        let line = format!(
            r#"1783484211.311904 [0 127.0.0.1:52460] "ZADD" "analytics:measurements" "1783484211.3117671" "{payload}""#
        );
        assert_eq!(args(line.as_bytes())[2], payload.as_bytes());
    }

    #[test]
    fn parses_unescaped_html_and_serialized_php_quotes() {
        for payload in [
            &br#"<iframe src="https://example.test/video" width="500" height="281"></iframe>"#[..],
            br#"a:2:{s:11:"description";s:0:"";s:5:"count";s:1:"1";}"#,
            br#"O:8:"stdClass":9:{s:7:"term_id";s:4:"1504";s:11:"description";s:0:"";}"#,
        ] {
            let mut line = br#"1.0 [0 127.0.0.1:1] "SET" "key" ""#.to_vec();
            line.extend_from_slice(payload);
            line.extend_from_slice(br#"" "NX" "EX" "604800""#);
            assert_eq!(
                args(&line),
                [&b"key"[..], payload, b"NX", b"EX", b"604800"]
            );
        }
    }

    #[test]
    fn unknown_and_invalid_escapes_are_preserved_literally() {
        let line =
            br#"1.0 [0 lua] "SET" "\x00\xfF\x7a" "\xZZ" "Foo\Bar it\'s" "\x4""#;
        assert_eq!(
            args(line),
            [&b"\x00\xff\x7a"[..], br"\xZZ", br"Foo\Bar it\'s", br"\x4"]
        );
    }

    #[test]
    fn malformed_arguments_are_rejected() {
        for input in [
            &br#""unterminated"#[..],
            br#""a" garbage"#,
            br"bare",
            br#""trailing backslash\"#,
        ] {
            assert!(
                decode_args(input, &mut Vec::new()).is_err(),
                "{}",
                String::from_utf8_lossy(input)
            );
        }
        // Trailing whitespace after the last argument is accepted.
        assert!(decode_args(br#""a" "#, &mut Vec::new()).is_ok());
        // A quote followed directly by more text cannot close an argument.
        let mut decoded = Vec::new();
        decode_args(br#""a""b""#, &mut decoded).unwrap();
        assert_eq!(decoded, [&br#"a""b"#[..]]);
    }

    #[test]
    fn prefix_helpers_extract_without_full_validation() {
        let line = br#"1.0 [12 lua] "GET" "k""#;
        assert_eq!(command_name(line), Some(&b"GET"[..]));
        assert_eq!(database(line), Some(12));
        assert_eq!(command_name(b"no quotes"), None);
        assert_eq!(database(b"no brackets"), None);
    }

    /// Every prefix of valid records, and pseudo-random bytes, must parse or
    /// fail cleanly: never panic, and never report an offset past the input.
    #[test]
    fn truncated_and_arbitrary_input_never_panics() {
        let valid: &[&[u8]] = &[
            br#"1783484211.311904 [0 127.0.0.1:52460] "SET" "k" "a\"b\x00""#,
            br#"1.0 [0 [::1]:1] "GET" "k""#,
            br#"1.0 [0 unix:/tmp/s] "GET" "k""#,
            br#"1.0 [0 lua] "EVAL" "return 1" "0""#,
        ];
        let check = |input: &[u8]| {
            match Record::parse(input) {
                Ok(record) => {
                    let _ = record.decode_args(&mut Vec::new());
                }
                Err(error) => assert!(error.offset <= input.len()),
            }
            let _ = decode_args(input, &mut Vec::new());
        };
        for line in valid {
            for end in 0..=line.len() {
                check(&line[..end]);
            }
        }
        let alphabet = b" \t.:[]\"\\x0123456789abfuniLlua";
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        for _ in 0..20_000 {
            let len = usize::try_from(state % 48).unwrap();
            let input: Vec<u8> = (0..len)
                .map(|_| {
                    // xorshift64
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    alphabet[usize::try_from(state % alphabet.len() as u64)
                        .unwrap()]
                })
                .collect();
            check(&input);
        }
    }
}
