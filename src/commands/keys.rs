//! Local key discovery. Argument indices in metadata include the command at 0;
//! this API takes the command separately, like `monitor::Line` does.
use std::{fmt, iter::FusedIterator};

use super::{
    BeginSearch, FindKeys, Flags, KeySpec, KeySpecFlags, Lookup, Metadata,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyError {
    UnknownCommand,
    InvalidArguments,
    UnsupportedSpec,
}

impl fmt::Display for KeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnknownCommand => "unknown command or subcommand",
            Self::InvalidArguments => {
                "invalid command arity, key count, or key positions"
            }
            Self::UnsupportedSpec => {
                "command key specifications cannot be resolved locally"
            }
        })
    }
}

impl std::error::Error for KeyError {}

type Result<T> = std::result::Result<T, KeyError>;

// Zero-based positions in the arguments (excluding the command).
#[derive(Debug, Clone, Copy, Default)]
struct KeyRange {
    next: usize,
    count: usize,
    step: usize,
}

impl KeyRange {
    fn new(
        first: usize,
        count: usize,
        step: usize,
        len: usize,
    ) -> Result<Self> {
        if step == 0 || first > len {
            return Err(KeyError::InvalidArguments);
        }
        if count > 0
            && count
                .checked_sub(1)
                .and_then(|n| n.checked_mul(step))
                .and_then(|n| first.checked_add(n))
                .is_none_or(|last| last >= len)
        {
            return Err(KeyError::InvalidArguments);
        }
        Ok(Self {
            next: first,
            count,
            step,
        })
    }

    const fn pop(&mut self) -> Option<usize> {
        if self.count == 0 {
            return None;
        }
        let index = self.next;
        self.count -= 1;
        if self.count != 0 {
            self.next += self.step; // Validated by new().
        }
        Some(index)
    }
}

/// An allocation-free view of key arguments, borrowing the original bytes.
///
/// All specs are validated before construction, so even short-circuit consumers
/// cannot mistake an unsupported/invalid command for a partial set of keys.
/// Keys follow metadata spec order, then argument order within each spec. Repeated
/// keys (and overlapping specs) are retained. No key bytes are decoded or copied.
#[derive(Debug, Clone)]
pub struct Keys<'m, 'a, A> {
    args: &'a [A],
    current: KeyRange,
    extra: KeyRange,
    specs: &'m [KeySpec],
}

impl<'a, A: AsRef<[u8]>> Iterator for Keys<'_, 'a, A> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(index) = self.current.pop() {
                return Some(self.args[index].as_ref());
            }
            if self.extra.count != 0 {
                self.current = std::mem::take(&mut self.extra);
            } else if let Some((spec, rest)) = self.specs.split_first() {
                self.specs = rest;
                // Construction validated every spec against these same immutable
                // args. Recompute only later ranges to avoid allocating storage.
                self.current = spec
                    .resolve(self.args)
                    .expect("key specs validated when constructing Keys");
            } else {
                return None;
            }
        }
    }
}

impl<A: AsRef<[u8]>> FusedIterator for Keys<'_, '_, A> {}

impl Lookup {
    /// Resolve a command (and any subcommand) and borrow its key arguments.
    /// `args` excludes the command name and contains already decoded bytes.
    /// Accepts byte slices, `Vec<u8>`, or the parser's `Cow<[u8]>` directly.
    ///
    /// # Errors
    /// Returns [`KeyError`] for unknown commands, unsupported specs, or invalid
    /// arity/counts/positions. It never returns a partial set on error.
    pub fn keys<'m, 'a, A: AsRef<[u8]>>(
        &'m self,
        command: &[u8],
        args: &'a [A],
    ) -> Result<Keys<'m, 'a, A>> {
        let mut metadata =
            self.get_bytes(command).ok_or(KeyError::UnknownCommand)?;
        let mut depth = 0;
        while !metadata.subcommands.0.is_empty() {
            let subcommand =
                args.get(depth).ok_or(KeyError::InvalidArguments)?;
            metadata = metadata
                .subcommands
                .get_bytes(subcommand.as_ref())
                .ok_or(KeyError::UnknownCommand)?;
            depth += 1;
        }
        metadata.keys(args)
    }
}

impl Metadata {
    /// Extract key arguments using this command's metadata. Subcommand args
    /// still include the subcommand tokens, as Redis key positions do.
    ///
    /// Checks arity and key-related structure, not the entire command grammar.
    /// `Ok(empty)` means no key arguments; unsupported specs return an error.
    /// SORT's data-dependent BY/GET expansions are not argument keys.
    ///
    /// # Errors
    /// Returns [`KeyError`] for unknown commands, unsupported specs, or invalid
    /// arity/counts/positions. It never returns a partial set on error.
    pub fn keys<'m, 'a, A: AsRef<[u8]>>(
        &'m self,
        args: &'a [A],
    ) -> Result<Keys<'m, 'a, A>> {
        let argument_count = args
            .len()
            .checked_add(1)
            .ok_or(KeyError::InvalidArguments)?;
        let arity = usize::try_from(self.arity.unsigned_abs())
            .map_err(|_| KeyError::UnsupportedSpec)?;
        if self.arity == 0
            || (self.arity > 0 && argument_count != arity)
            || (self.arity < 0 && argument_count < arity)
        {
            return Err(KeyError::InvalidArguments);
        }
        if !self.subcommands.0.is_empty() {
            return Err(KeyError::UnknownCommand);
        }
        let mut keys = Keys {
            args,
            current: KeyRange::default(),
            extra: KeyRange::default(),
            specs: &[],
        };
        // Only builtins have these grammars. A module with an unfamiliar spec
        // must never be interpreted as a builtin by accident.
        if !self.flags.contains(Flags::MODULE) {
            if self.name.eq_ignore_ascii_case("sort")
                || self.name.eq_ignore_ascii_case("sort_ro")
            {
                (keys.current, keys.extra) =
                    sort_keys(args, self.name.eq_ignore_ascii_case("sort"))?;
                return Ok(keys);
            }
            if self.name.eq_ignore_ascii_case("migrate") {
                keys.current = migrate_keys(args)?;
                return Ok(keys);
            }
            // STORE/STOREDIST can themselves be destination key names; a plain
            // keyword search would misinterpret them. Last destination wins.
            let geo_start = if self.name.eq_ignore_ascii_case("georadius") {
                Some(5)
            } else if self.name.eq_ignore_ascii_case("georadiusbymember") {
                Some(4)
            } else {
                None
            };
            if let Some(start) = geo_start {
                (keys.current, keys.extra) = geo_keys(args, start)?;
                return Ok(keys);
            }
        }
        if self.key_specs.is_empty() {
            // Pre-key-spec metadata describes shard channels as legacy keys.
            // They route like keys but are not members of the keyspace.
            if !self.flags.contains(Flags::MODULE)
                && ["ssubscribe", "sunsubscribe", "spublish"]
                    .iter()
                    .any(|name| self.name.eq_ignore_ascii_case(name))
            {
                return Ok(keys);
            }
            if self.flags.contains(Flags::MOVABLEKEYS) {
                return Err(KeyError::UnsupportedSpec);
            }
            if self.first_key != 0 {
                let first = position(self.first_key, args.len())?;
                let last = position(self.last_key, args.len())?;
                keys.current = range(first, last, self.step_count, args.len())?;
            }
        } else {
            for (index, spec) in self.key_specs.iter().enumerate() {
                let resolved = spec.resolve(args)?;
                if index == 0 {
                    keys.current = resolved;
                }
            }
            keys.specs = &self.key_specs[1..];
        }
        Ok(keys)
    }
}

// Positive positions count from the command (1 is args[0]); negative from end.
fn position(index: i64, len: usize) -> Result<usize> {
    match index.cmp(&0) {
        std::cmp::Ordering::Greater => {
            usize::try_from(index - 1).map_err(|_| KeyError::InvalidArguments)
        }
        std::cmp::Ordering::Less => {
            let back = usize::try_from(index.unsigned_abs())
                .map_err(|_| KeyError::InvalidArguments)?;
            len.checked_sub(back).ok_or(KeyError::InvalidArguments)
        }
        std::cmp::Ordering::Equal => Err(KeyError::InvalidArguments),
    }
}

fn offset(base: usize, delta: i64) -> Result<usize> {
    let delta =
        usize::try_from(delta).map_err(|_| KeyError::UnsupportedSpec)?;
    base.checked_add(delta).ok_or(KeyError::InvalidArguments)
}

fn step(value: i64) -> Result<usize> {
    usize::try_from(value)
        .ok()
        .filter(|step| *step > 0)
        .ok_or(KeyError::UnsupportedSpec)
}

fn range(
    first: usize,
    last: usize,
    stride: i64,
    len: usize,
) -> Result<KeyRange> {
    let step = step(stride)?;
    if last >= len {
        return Err(KeyError::InvalidArguments);
    }
    let distance = last.checked_sub(first).ok_or(KeyError::InvalidArguments)?;
    KeyRange::new(first, distance / step + 1, step, len)
}

impl KeySpec {
    fn resolve<A: AsRef<[u8]>>(&self, args: &[A]) -> Result<KeyRange> {
        if self.flags.contains(KeySpecFlags::NOT_KEY) {
            return Ok(KeyRange::default());
        }
        if self.flags.contains(KeySpecFlags::INCOMPLETE)
            || matches!(self.find_keys, FindKeys::Unknown)
        {
            return Err(KeyError::UnsupportedSpec);
        }
        let first = match &self.begin_search {
            BeginSearch::Index { index } => position(*index, args.len())?,
            BeginSearch::Keyword {
                keyword,
                start_from,
            } => {
                if *start_from == 0 {
                    return Err(KeyError::UnsupportedSpec);
                }
                let Ok(start) = position(*start_from, args.len()) else {
                    return Ok(KeyRange::default());
                };
                if start >= args.len() {
                    return Ok(KeyRange::default());
                }
                let matches = |arg: &A| {
                    arg.as_ref().eq_ignore_ascii_case(keyword.as_bytes())
                };
                let found = if *start_from > 0 {
                    args[start..].iter().position(matches).map(|i| start + i)
                } else {
                    args[..=start].iter().rposition(matches)
                };
                let Some(found) = found else {
                    return Ok(KeyRange::default());
                };
                found + 1
            }
            BeginSearch::Unknown => return Err(KeyError::UnsupportedSpec),
        };
        match self.find_keys {
            FindKeys::Range {
                last_key,
                key_step,
                limit,
            } => {
                if limit < 0 || (limit != 0 && last_key != -1) {
                    return Err(KeyError::UnsupportedSpec);
                }
                let last = if last_key >= 0 {
                    offset(first, last_key)?
                } else if limit > 0 {
                    let limit = usize::try_from(limit)
                        .map_err(|_| KeyError::UnsupportedSpec)?;
                    let remaining = args
                        .len()
                        .checked_sub(first)
                        .ok_or(KeyError::InvalidArguments)?;
                    // A divided tail (e.g. XREAD keys/IDs) must have whole groups.
                    if remaining == 0 || remaining % limit != 0 {
                        return Err(KeyError::InvalidArguments);
                    }
                    first + remaining / limit - 1
                } else {
                    position(last_key, args.len())?
                };
                range(first, last, key_step, args.len())
            }
            FindKeys::Keynum {
                keynum_idx,
                first_key,
                key_step,
            } => {
                let count_at = offset(first, keynum_idx)?;
                let bytes = args
                    .get(count_at)
                    .ok_or(KeyError::InvalidArguments)?
                    .as_ref();
                let count = key_count(bytes)?;
                KeyRange::new(
                    offset(first, first_key)?,
                    count,
                    step(key_step)?,
                    args.len(),
                )
            }
            FindKeys::Unknown => Err(KeyError::UnsupportedSpec),
        }
    }
}

fn key_count(bytes: &[u8]) -> Result<usize> {
    // Redis integers are canonical decimal: no sign, leading zeros or whitespace.
    if bytes.is_empty()
        || (bytes.len() > 1 && bytes[0] == b'0')
        || !bytes.iter().all(u8::is_ascii_digit)
    {
        return Err(KeyError::InvalidArguments);
    }
    let count = lexical_core::parse::<u64>(bytes)
        .map_err(|_| KeyError::InvalidArguments)?;
    if count > i64::MAX as u64 {
        return Err(KeyError::InvalidArguments);
    }
    usize::try_from(count).map_err(|_| KeyError::InvalidArguments)
}

fn sort_keys<A: AsRef<[u8]>>(
    args: &[A],
    allow_store: bool,
) -> Result<(KeyRange, KeyRange)> {
    let source = KeyRange::new(0, 1, 1, args.len())?;
    let mut dest = KeyRange::default();
    let mut i = 1;
    while i < args.len() {
        let token = args[i].as_ref();
        let skip = if token.eq_ignore_ascii_case(b"LIMIT") {
            2
        } else if token.eq_ignore_ascii_case(b"BY")
            || token.eq_ignore_ascii_case(b"GET")
        {
            1
        } else if token.eq_ignore_ascii_case(b"STORE") && allow_store {
            dest = KeyRange::new(i + 1, 1, 1, args.len())?;
            1
        } else if [b"ASC".as_slice(), b"DESC", b"ALPHA"]
            .iter()
            .any(|v| token.eq_ignore_ascii_case(v))
        {
            0
        } else {
            return Err(KeyError::InvalidArguments);
        };
        i += skip + 1;
        if i > args.len() {
            return Err(KeyError::InvalidArguments);
        }
    }
    Ok((source, dest))
}

fn migrate_keys<A: AsRef<[u8]>>(args: &[A]) -> Result<KeyRange> {
    if args.len() < 5 {
        return Err(KeyError::InvalidArguments);
    }
    let mut i = 5;
    while i < args.len() {
        let token = args[i].as_ref();
        if token.eq_ignore_ascii_case(b"KEYS") {
            if !args[2].as_ref().is_empty() || i + 1 == args.len() {
                return Err(KeyError::InvalidArguments);
            }
            return KeyRange::new(i + 1, args.len() - i - 1, 1, args.len());
        }
        let skip = if token.eq_ignore_ascii_case(b"AUTH") {
            1
        } else if token.eq_ignore_ascii_case(b"AUTH2") {
            2
        } else if token.eq_ignore_ascii_case(b"COPY")
            || token.eq_ignore_ascii_case(b"REPLACE")
        {
            0
        } else {
            return Err(KeyError::InvalidArguments);
        };
        i += skip + 1;
        if i > args.len() {
            return Err(KeyError::InvalidArguments);
        }
    }
    KeyRange::new(2, 1, 1, args.len())
}

fn geo_keys<A: AsRef<[u8]>>(
    args: &[A],
    start: usize,
) -> Result<(KeyRange, KeyRange)> {
    let source = KeyRange::new(0, 1, 1, args.len())?;
    let mut dest = KeyRange::default();
    let mut i = start;
    while i < args.len() {
        let token = args[i].as_ref();
        if token.eq_ignore_ascii_case(b"STORE")
            || token.eq_ignore_ascii_case(b"STOREDIST")
        {
            dest = KeyRange::new(i + 1, 1, 1, args.len())?;
            i += 2;
        } else if token.eq_ignore_ascii_case(b"COUNT") {
            i += 2;
        } else {
            i += 1;
        }
        if i > args.len() {
            return Err(KeyError::InvalidArguments);
        }
    }
    Ok((source, dest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::Categories;
    use std::collections::HashSet;

    fn metadata(specs: Vec<KeySpec>) -> Metadata {
        Metadata {
            name: "module.command".into(),
            arity: -1,
            flags: Flags::MODULE,
            categories: Categories::empty(),
            first_key: 0,
            last_key: 0,
            step_count: 0,
            subcommands: Lookup(HashSet::new()),
            key_specs: specs,
        }
    }

    fn spec(begin_search: BeginSearch, find_keys: FindKeys) -> KeySpec {
        KeySpec {
            flags: KeySpecFlags::RO,
            begin_search,
            find_keys,
        }
    }

    fn fixed(index: i64, last_key: i64, key_step: i64, limit: i64) -> KeySpec {
        spec(
            BeginSearch::Index { index },
            FindKeys::Range {
                last_key,
                key_step,
                limit,
            },
        )
    }

    #[test]
    fn reverse_keywords_optional_clauses_and_strided_counts() {
        let meta = metadata(vec![spec(
            BeginSearch::Keyword {
                keyword: "KEYS".into(),
                start_from: -2,
            },
            FindKeys::Range {
                last_key: -1,
                key_step: 1,
                limit: 0,
            },
        )]);
        let args = [b"KEYS".as_slice(), b"a", b"kEyS", b"b"];
        assert_eq!(meta.keys(&args).unwrap().collect::<Vec<_>>(), vec![b"b"]);
        assert_eq!(meta.keys(&[b"key".as_slice()]).unwrap().count(), 0);
        assert_eq!(
            meta.keys(&[b"KEYS".as_slice(), b"a"])
                .unwrap()
                .collect::<Vec<_>>(),
            vec![b"a"]
        );
        let meta = metadata(vec![spec(
            BeginSearch::Index { index: 1 },
            FindKeys::Keynum {
                keynum_idx: 1,
                first_key: 2,
                key_step: 2,
            },
        )]);
        let args = [b"option".as_slice(), b"2", b"a", b"value", b"b", b"value"];
        assert_eq!(
            meta.keys(&args).unwrap().collect::<Vec<_>>(),
            vec![b"a", b"b"]
        );
    }

    #[test]
    fn unknown_incomplete_and_not_key_are_distinct() {
        for unsupported in [
            spec(BeginSearch::Unknown, FindKeys::Unknown),
            KeySpec {
                flags: KeySpecFlags::INCOMPLETE,
                ..fixed(1, 0, 1, 0)
            },
            spec(
                BeginSearch::Keyword {
                    keyword: "ABSENT".into(),
                    start_from: 1,
                },
                FindKeys::Unknown,
            ),
        ] {
            let meta = metadata(vec![fixed(1, 0, 1, 0), unsupported]);
            assert_eq!(
                meta.keys(&[b"a".as_slice()]).unwrap_err(),
                KeyError::UnsupportedSpec
            );
        }
        let meta = metadata(vec![KeySpec {
            flags: KeySpecFlags::NOT_KEY | KeySpecFlags::INCOMPLETE,
            begin_search: BeginSearch::Unknown,
            find_keys: FindKeys::Unknown,
        }]);
        assert_eq!(meta.keys(&[b"channel".as_slice()]).unwrap().count(), 0);
    }

    #[test]
    fn spec_boundaries_do_not_panic_or_escape_arguments() {
        let extremes = [i64::MIN, -100, -2, -1, 0, 1, 2, 3, 100, i64::MAX];
        let args = [b"2".as_slice(), b"a", b"b", b"c"];
        for index in extremes {
            for last_key in extremes {
                for key_step in extremes {
                    for limit in [i64::MIN, -1, 0, 1, 2, i64::MAX] {
                        let meta = metadata(vec![fixed(
                            index, last_key, key_step, limit,
                        )]);
                        if let Ok(keys) = meta.keys(&args) {
                            let keys: Vec<_> = keys.collect();
                            assert!(keys.len() <= args.len());
                            assert!(keys.iter().all(|key| args.contains(key)));
                        }
                    }
                    let meta = metadata(vec![spec(
                        BeginSearch::Index { index },
                        FindKeys::Keynum {
                            keynum_idx: last_key,
                            first_key: 1,
                            key_step,
                        },
                    )]);
                    if let Ok(keys) = meta.keys(&args) {
                        assert!(keys.count() <= args.len());
                    }
                }
            }
        }
        assert!(metadata(vec![fixed(1, 100, 1000, 0)]).keys(&args).is_err());
    }

    #[test]
    fn iterator_is_fused_and_repeated_keys_are_retained() {
        let meta = metadata(vec![fixed(1, 0, 1, 0), fixed(1, 1, 1, 0)]);
        let args = [b"same".as_slice(), b"same"];
        let mut keys = meta.keys(&args).unwrap();
        for _ in 0..3 {
            assert_eq!(keys.next(), Some(b"same".as_slice()));
        }
        assert_eq!(keys.next(), None);
        assert_eq!(keys.next(), None);
    }
}
