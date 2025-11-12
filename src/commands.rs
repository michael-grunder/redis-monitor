use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    ops::BitOr,
    str::FromStr,
    sync::LazyLock,
};

use anyhow::Result;
use bitflags::bitflags;
use redis::{self, RedisError, aio::ConnectionManager};

#[derive(Debug)]
pub struct Command {
    name: String,
    arity: i64,
    flags: Flags,
    first_key: i64,
    last_key: i64,
    step_count: i64,
    categories: Categories,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Filter {
    pub flags: Option<Flags>,
    pub categories: Option<Categories>,
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

impl Flags {
    pub fn names(self) -> impl Iterator<Item = &'static str> {
        FLAG_MAP.iter().filter_map(move |(k, v)| {
            if self.contains(*v) { Some(*k) } else { None }
        })
    }

    pub fn to_vec(self) -> Vec<String> {
        self.names().map(|s| s.to_string()).collect()
    }
}

impl BitMask for Flags {
    fn empty() -> Self {
        Flags::empty()
    }
}

static CATEGORY_MAP: LazyLock<HashMap<&'static str, Categories>> =
    LazyLock::new(|| {
        HashMap::from([
            ("admin", Categories::ADMIN),
            ("allow_busy", Categories::ALLOW_BUSY),
            ("blocking", Categories::BLOCKING),
            ("asking", Categories::ASKING),
            ("denyoom", Categories::DENYOOM),
            ("fast", Categories::FAST),
            ("loading", Categories::LOADING),
            ("module", Categories::MODULE),
            ("movablekeys", Categories::MOVABLEKEYS),
            ("no_mandatory_keys", Categories::NO_MANDATORY_KEYS),
            ("no_multi", Categories::NO_MULTI),
            ("noscript", Categories::NOSCRIPT),
            ("pubsub", Categories::PUBSUB),
            ("readonly", Categories::READONLY),
            ("skip_monitor", Categories::SKIP_MONITOR),
            ("skip_slowlog", Categories::SKIP_SLOWLOG),
            ("stale", Categories::STALE),
            ("write", Categories::WRITE),
        ])
    });

bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct Categories: u32 {
        const ADMIN              = 1 << 0;
        const ALLOW_BUSY         = 1 << 1;
        const BLOCKING           = 1 << 2;
        const ASKING             = 1 << 3;
        const DENYOOM            = 1 << 4;
        const FAST               = 1 << 5;
        const LOADING            = 1 << 6;
        const MODULE             = 1 << 7;
        const MOVABLEKEYS        = 1 << 8;
        const NO_MANDATORY_KEYS  = 1 << 9;
        const NO_MULTI           = 1 << 10;
        const NOSCRIPT           = 1 << 11;
        const PUBSUB             = 1 << 12;
        const READONLY           = 1 << 13;
        const SKIP_MONITOR       = 1 << 14;
        const SKIP_SLOWLOG       = 1 << 15;
        const STALE              = 1 << 16;
        const WRITE              = 1 << 17;
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
        Categories::empty()
    }
}

impl Categories {
    pub fn names(self) -> impl Iterator<Item = &'static str> {
        CATEGORY_MAP.iter().filter_map(move |(k, v)| {
            if self.contains(*v) { Some(*k) } else { None }
        })
    }

    pub fn to_vec(self) -> Vec<String> {
        self.names().map(|s| s.to_string()).collect()
    }
}

impl Filter {
    pub fn is_empty(&self) -> bool {
        self.flags.is_none() && self.categories.is_none()
    }

    #[inline]
    pub fn matches(self, f: Flags, c: Categories) -> bool {
        if let Some(req_flags) = self.flags {
            if !f.contains(req_flags) {
                return false;
            }
        }

        if let Some(req_cats) = self.categories {
            if !c.contains(req_cats) {
                return false;
            }
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

    //fn parse_flags<'a, I>(it: I) -> Flags
    //where
    //    I: IntoIterator<Item = &'a str>,
    //{
    //    let mut f = Flags::empty();
    //    for s in it {
    //        if let Ok(flag) = Flags::from_str(s) {
    //            f |= flag;
    //        }
    //    }

    //    f
    //}

    //fn parse_categories<'a, I>(it: I) -> Categories
    //where
    //    I: IntoIterator<Item = &'a str>,
    //{
    //    let mut c = Categories::empty();
    //    for s in it {
    //        if let Ok(cat) = Categories::from_str(s) {
    //            c |= cat;
    //        }
    //    }

    //    c
    //}

    fn iter_simplestring<'a>(
        arr: &'a [redis::Value],
    ) -> impl Iterator<Item = &'a str> {
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

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn flags(&self) -> Flags {
        self.flags
    }

    pub fn categories(&self) -> Categories {
        self.categories
    }
}
