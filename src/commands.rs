use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    fmt,
    hash::{Hash, Hasher},
    ops::BitOr,
    str::FromStr,
    sync::LazyLock,
};

use anyhow::Result;
use bitflags::bitflags;
use redis::{self, RedisError, aio::ConnectionManager};

#[derive(Debug, Clone)]
pub struct Metadata {
    pub name: String,
    pub flags: Flags,
    pub categories: Categories,
}

#[repr(transparent)]
struct CiStr(str);

#[derive(Debug, Clone)]
pub struct Lookup(HashSet<Metadata>);

#[derive(Debug)]
#[allow(dead_code)]
pub struct Command {
    name: String,
    arity: i64,
    flags: Flags,
    first_key: i64,
    last_key: i64,
    step_count: i64,
    categories: Categories,
}

#[derive(Clone, Copy, Default)]
pub struct Filter {
    pub flags: Option<Flags>,
    pub categories: Option<Categories>,
}

impl CiStr {
    #[inline]
    const fn from_str(s: &str) -> &Self {
        // SAFETY: CiStr is #[repr(transparent)] over str
        unsafe { &*(std::ptr::from_ref::<str>(s) as *const Self) }
    }

    fn ascii_eq_ignore_ascii_case(a: &[u8], b: &[u8]) -> bool {
        #[inline]
        const fn lower(b: u8) -> u8 {
            if b.is_ascii_uppercase() { b + 32 } else { b }
        }

        a.len() == b.len()
            && a.iter()
                .zip(b.iter())
                .all(|(lhs, rhs)| lower(*lhs) == lower(*rhs))
    }
}

impl PartialEq for CiStr {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        Self::ascii_eq_ignore_ascii_case(self.0.as_bytes(), other.0.as_bytes())
    }
}

impl Eq for CiStr {}

impl Hash for CiStr {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        for b in self.0.as_bytes() {
            let lb = if b.is_ascii_uppercase() { b + 32 } else { *b };
            lb.hash(state);
        }
    }
}

impl Borrow<CiStr> for Metadata {
    #[inline]
    fn borrow(&self) -> &CiStr {
        CiStr::from_str(&self.name)
    }
}

impl PartialEq for Metadata {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.name.eq_ignore_ascii_case(&other.name)
    }
}

impl Eq for Metadata {}

impl Hash for Metadata {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        CiStr::from_str(&self.name).hash(state);
    }
}

impl PartialEq for Command {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Command {}

impl Hash for Command {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

trait BitMask: Copy + BitOr<Output = Self> {
    fn empty() -> Self;
}

static FLAG_MAP: LazyLock<HashMap<&'static str, Flags>> = LazyLock::new(|| {
    HashMap::from([
        ("admin", Flags::ADMIN),
        ("allow_busy", Flags::ALLOW_BUSY),
        ("asking", Flags::ASKING),
        ("blocking", Flags::BLOCKING),
        ("denyoom", Flags::DENYOOM),
        ("fast", Flags::FAST),
        ("loading", Flags::LOADING),
        ("module", Flags::MODULE),
        ("movablekeys", Flags::MOVABLEKEYS),
        ("no_async_loading", Flags::NO_ASYNC_LOADING),
        ("no_auth", Flags::NO_AUTH),
        ("no_mandatory_keys", Flags::NO_MANDATORY_KEYS),
        ("no_multi", Flags::NO_MULTI),
        ("noscript", Flags::NOSCRIPT),
        ("pubsub", Flags::PUBSUB),
        ("readonly", Flags::READONLY),
        ("ro", Flags::READONLY),
        ("skip_monitor", Flags::SKIP_MONITOR),
        ("skip_slowlog", Flags::SKIP_SLOWLOG),
        ("stale", Flags::STALE),
        ("write", Flags::WRITE),
        ("wo", Flags::WRITE),
    ])
});

bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct Flags: u32 {
        const ADMIN              = 1 << 0;
        const ALLOW_BUSY         = 1 << 1;
        const ASKING             = 1 << 2;
        const BLOCKING           = 1 << 3;
        const DENYOOM            = 1 << 4;
        const FAST               = 1 << 5;
        const LOADING            = 1 << 6;
        const MODULE             = 1 << 7;
        const MOVABLEKEYS        = 1 << 8;
        const NO_ASYNC_LOADING   = 1 << 9;
        const NO_AUTH            = 1 << 10;
        const NO_MANDATORY_KEYS  = 1 << 11;
        const NO_MULTI           = 1 << 12;
        const NOSCRIPT           = 1 << 13;
        const PUBSUB             = 1 << 14;
        const READONLY           = 1 << 15;
        const SKIP_MONITOR       = 1 << 16;
        const SKIP_SLOWLOG       = 1 << 17;
        const STALE              = 1 << 18;
        const WRITE              = 1 << 19;
    }
}

impl FromStr for Flags {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let k = s.trim().to_ascii_lowercase();
        FLAG_MAP.get(k.as_str()).copied().ok_or(())
    }
}

impl fmt::Display for Flags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for name in self.names() {
            if !first {
                write!(f, ",")?;
            }
            write!(f, "{name}")?;
            first = false;
        }
        Ok(())
    }
}

impl Flags {
    pub fn names(self) -> impl Iterator<Item = &'static str> {
        FLAG_MAP.iter().filter_map(move |(k, v)| {
            if self.contains(*v) { Some(*k) } else { None }
        })
    }

    //    pub fn to_vec(self) -> Vec<String> {
    //        self.names().map(std::string::ToString::to_string).collect()
    //    }
}

impl BitMask for Flags {
    fn empty() -> Self {
        Self::empty()
    }
}

static CATEGORY_MAP: LazyLock<HashMap<&'static str, Categories>> =
    LazyLock::new(|| {
        HashMap::from([
            ("@admin", Categories::ADMIN),
            ("@bitmap", Categories::BITMAP),
            ("@blocking", Categories::BLOCKING),
            ("@connection", Categories::CONNECTION),
            ("@dangerous", Categories::DANGEROUS),
            ("@fast", Categories::FAST),
            ("@geo", Categories::GEO),
            ("@hash", Categories::HASH),
            ("@hyperloglog", Categories::HYPERLOGLOG),
            ("@keyspace", Categories::KEYSPACE),
            ("@list", Categories::LIST),
            ("@pubsub", Categories::PUBSUB),
            ("@read", Categories::READ),
            ("@scripting", Categories::SCRIPTING),
            ("@set", Categories::SET),
            ("@slow", Categories::SLOW),
            ("@sortedset", Categories::SORTEDSET),
            ("@stream", Categories::STREAM),
            ("@string", Categories::STRING),
            ("@transaction", Categories::TRANSACTION),
            ("@write", Categories::WRITE),
        ])
    });

bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct Categories: u32 {
        const ADMIN       = 1 << 0;
        const BITMAP      = 1 << 1;
        const BLOCKING    = 1 << 2;
        const CONNECTION  = 1 << 3;
        const DANGEROUS   = 1 << 4;
        const FAST        = 1 << 5;
        const GEO         = 1 << 6;
        const HASH        = 1 << 7;
        const HYPERLOGLOG = 1 << 8;
        const KEYSPACE    = 1 << 9;
        const LIST        = 1 << 10;
        const PUBSUB      = 1 << 11;
        const READ        = 1 << 12;
        const SCRIPTING   = 1 << 13;
        const SET         = 1 << 14;
        const SLOW        = 1 << 15;
        const SORTEDSET   = 1 << 16;
        const STREAM      = 1 << 17;
        const STRING      = 1 << 18;
        const TRANSACTION = 1 << 19;
        const WRITE       = 1 << 20;
    }
}

impl FromStr for Categories {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let k = s.trim().to_ascii_lowercase();
        CATEGORY_MAP.get(k.as_str()).copied().ok_or(())
    }
}

impl BitMask for Categories {
    fn empty() -> Self {
        Self::empty()
    }
}

impl fmt::Display for Categories {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for name in self.names() {
            if !first {
                write!(f, ",")?;
            }
            write!(f, "{name}")?;
            first = false;
        }
        Ok(())
    }
}

impl Categories {
    pub fn names(self) -> impl Iterator<Item = &'static str> {
        CATEGORY_MAP.iter().filter_map(move |(k, v)| {
            if self.contains(*v) { Some(*k) } else { None }
        })
    }

    //pub fn to_vec(self) -> Vec<String> {
    //    self.names().map(std::string::ToString::to_string).collect()
    //}
}

impl Filter {
    pub const fn is_empty(&self) -> bool {
        self.flags.is_none() && self.categories.is_none()
    }

    #[inline]
    pub const fn matches(self, f: Flags, c: Categories) -> bool {
        if let Some(req_flags) = self.flags
            && !f.contains(req_flags)
        {
            return false;
        }

        if let Some(req_cats) = self.categories
            && !c.contains(req_cats)
        {
            return false;
        }

        true
    }
}

impl Command {
    fn parse_mask<'a, I, T>(it: I) -> T
    where
        I: IntoIterator<Item = &'a str>,
        T: BitMask + FromStr<Err = ()>,
    {
        it.into_iter()
            .filter_map(|s| s.parse::<T>().ok())
            .fold(T::empty(), |a, x| a | x)
    }

    fn iter_simplestring(arr: &[redis::Value]) -> impl Iterator<Item = &str> {
        arr.iter().filter_map(|v| match v {
            redis::Value::SimpleString(s) => Some(s.as_str()),
            redis::Value::BulkString(bytes) => std::str::from_utf8(bytes).ok(),
            _ => None,
        })
    }

    fn from_redis_values(values: &[redis::Value]) -> Option<Self> {
        let (
            name,
            arity,
            flags,
            first_key,
            last_key,
            step,
            acl,
            _history,
            _tips,
        ) = match values {
            [name, arity, flags, first_key, last_key, step, acl, ..] => {
                (name, arity, flags, first_key, last_key, step, acl, (), ())
            }
            _ => return None,
        };

        let name = match name {
            redis::Value::BulkString(bytes) => {
                String::from_utf8(bytes.clone()).ok()?
            }
            redis::Value::SimpleString(s) => s.clone(),
            _ => return None,
        };

        let arity = match arity {
            redis::Value::Int(x) => *x,
            _ => return None,
        };
        let first_key = match first_key {
            redis::Value::Int(x) => *x,
            _ => return None,
        };
        let last_key = match last_key {
            redis::Value::Int(x) => *x,
            _ => return None,
        };
        let step_count = match step {
            redis::Value::Int(x) => *x,
            _ => return None,
        };

        let flags = match flags {
            redis::Value::Array(a) => {
                Self::parse_mask(Self::iter_simplestring(a))
            }
            _ => Flags::empty(),
        };

        let categories = match acl {
            redis::Value::Array(a) => {
                Self::parse_mask(Self::iter_simplestring(a))
            }
            _ => Categories::empty(),
        };

        Some(Self {
            name,
            arity,
            flags,
            first_key,
            last_key,
            step_count,
            categories,
        })
    }

    pub async fn load(con: &mut ConnectionManager) -> Result<HashSet<Self>> {
        let commands: Vec<Vec<redis::Value>> = redis::cmd("COMMAND")
            .query_async(con)
            .await
            .map_err(|err| {
                RedisError::from((
                    redis::ErrorKind::IoError,
                    "Failed to execute COMMAND command",
                    err.to_string(),
                ))
            })?;

        let mut set = HashSet::new();
        for row in &commands {
            if let Some(cmd) = Self::from_redis_values(row) {
                set.insert(cmd);
            }
        }
        Ok(set)
    }

    //    pub fn name(&self) -> &str {
    //        &self.name
    //    }
    //
    //    pub const fn flags(&self) -> Flags {
    //        self.flags
    //    }
    //
    //    pub const fn categories(&self) -> Categories {
    //        self.categories
    //    }
}

impl From<HashSet<Command>> for Lookup {
    fn from(commands: HashSet<Command>) -> Self {
        let mut set = HashSet::new();
        for cmd in commands {
            let metadata = Metadata {
                name: cmd.name,
                flags: cmd.flags,
                categories: cmd.categories,
            };
            set.insert(metadata);
        }
        Self(set)
    }
}

impl Lookup {
    #[inline]
    pub fn get(&self, cmd: &str) -> Option<&Metadata> {
        self.0.get(CiStr::from_str(cmd))
    }

    #[inline]
    pub fn get_bytes(&self, cmd: &[u8]) -> Option<&Metadata> {
        std::str::from_utf8(cmd).ok().and_then(|s| self.get(s))
    }

    // Apply `filt`; if the command isn't in the table, return `unknown`.
    //#[inline]
    //pub fn matches_or(&self, cmd: &str, filt: Filter, unknown: bool) -> bool {
    //    self.get(cmd)
    //        .map_or(unknown, |m| filt.matches(m.flags, m.categories))
    //}

    #[inline]
    pub fn matches_bytes_or(
        &self,
        cmd: &[u8],
        filt: Filter,
        unknown: bool,
    ) -> bool {
        self.get_bytes(cmd)
            .map_or(unknown, |m| filt.matches(m.flags, m.categories))
    }
}

impl fmt::Debug for Filter {
    fn fmt(&self, fm: &mut fmt::Formatter<'_>) -> fmt::Result {
        let flags = self
            .flags
            .map_or_else(|| "<none>".into(), |f| f.to_string());
        let cats = self
            .categories
            .map_or_else(|| "<none>".into(), |c| c.to_string());

        fm.debug_struct("CmdFilter")
            .field("flags", &flags)
            .field("categories", &cats)
            .finish()
    }
}
