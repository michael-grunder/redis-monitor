use std::{collections::BTreeMap, fmt, str::FromStr};

use aho_corasick::{AhoCorasick, AhoCorasickBuilder};
use anyhow::{Context, Result};
use redis_monitor::{
    commands,
    monitor::{self, Args},
};
use regex::bytes::Regex;

#[derive(Debug, Clone)]
pub struct FilterPattern {
    exclude: bool,
    position: Option<usize>,
    pattern: Pattern,
}

#[derive(Debug, Clone)]
pub enum Pattern {
    Literal(String),
    Regex(Regex),
}

impl Pattern {
    #[inline]
    fn as_str(&self) -> &str {
        match self {
            Self::Literal(lit) => lit.as_str(),
            Self::Regex(re) => re.as_str(),
        }
    }
}

impl PartialEq for Pattern {
    fn eq(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
            && self.as_str() == other.as_str()
    }
}

impl Eq for Pattern {}

impl FromStr for FilterPattern {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let (negate, s) = s
            .strip_prefix('!')
            .map_or((false, s), |stripped| (true, stripped));

        let (position, s) = if let Some(rest) = s.strip_prefix('[') {
            let (index, pattern) = rest.split_once(']').ok_or_else(|| {
                anyhow::anyhow!(
                    "Unclosed filter index; use =text to match literal brackets"
                )
            })?;
            anyhow::ensure!(
                !index.is_empty() && index.bytes().all(|b| b.is_ascii_digit()),
                "Filter index must be a nonnegative integer; use =text to match literal brackets"
            );
            let index = index
                .parse::<usize>()
                .context("Filter index is too large")?;
            (Some(index), pattern)
        } else {
            (None, s)
        };

        let pattern = if let Some(literal) = s.strip_prefix('=') {
            Pattern::Literal(literal.to_owned())
        } else if let Some(inner) =
            s.strip_prefix('/').and_then(|s| s.strip_suffix('/'))
        {
            let re = Regex::new(inner)
                .map_err(|e| anyhow::anyhow!("Invalid regex '{inner}': {e}"))?;
            Pattern::Regex(re)
        } else {
            Pattern::Literal(s.to_string())
        };

        Ok(Self {
            exclude: negate,
            position,
            pattern,
        })
    }
}

#[derive(Debug, Clone)]
enum Matcher {
    Literals(AhoCorasick),
    Regexes(Vec<Regex>),
}

impl Matcher {
    #[inline]
    fn is_match(&self, value: &[u8]) -> bool {
        match self {
            Self::Literals(ac) => ac.is_match(value),
            Self::Regexes(res) => res.iter().any(|re| re.is_match(value)),
        }
    }
}

#[derive(Clone, Default)]
struct Matchers {
    include: Vec<Matcher>,
    exclude: Vec<Matcher>,
}

#[derive(Clone, Debug)]
struct PositionedMatchers {
    position: usize,
    matchers: Matchers,
}

#[derive(Clone, Debug)]
pub struct Filter {
    unscoped: Matchers,
    // Sorted and grouped once during setup; matching advances one cursor.
    positioned: Vec<PositionedMatchers>,
    has_includes: bool,
    has_excludes: bool,
}

impl TryFrom<Vec<FilterPattern>> for Filter {
    type Error = anyhow::Error;

    fn try_from(patterns: Vec<FilterPattern>) -> Result<Self> {
        Self::new(patterns)
    }
}

impl Filter {
    pub const fn is_empty(&self) -> bool {
        !self.has_includes && !self.has_excludes
    }

    /// Only positions beyond the command name require argument decoding.
    pub fn needs_args(&self) -> bool {
        self.positioned
            .last()
            .is_some_and(|group| group.position > 0)
    }

    pub fn new(patterns: Vec<FilterPattern>) -> Result<Self> {
        Self::build(patterns, false)
    }

    /// Unindexed --filter patterns retain their command-name-only scope.
    pub fn for_command(patterns: Vec<FilterPattern>) -> Result<Self> {
        Self::build(patterns, true)
    }

    fn build(patterns: Vec<FilterPattern>, command: bool) -> Result<Self> {
        let mut groups = BTreeMap::new();
        let mut result = Self {
            unscoped: Matchers::default(),
            positioned: Vec::new(),
            has_includes: false,
            has_excludes: false,
        };
        for pattern in patterns {
            let position = pattern.position.or_else(|| command.then_some(0));
            let (include, exclude) = groups
                .entry(position)
                .or_insert_with(|| (Vec::new(), Vec::new()));
            if pattern.exclude {
                result.has_excludes = true;
                exclude.push(pattern.pattern);
            } else {
                result.has_includes = true;
                include.push(pattern.pattern);
            }
        }
        for (position, (include, exclude)) in groups {
            let matchers = Matchers::new(include, exclude)?;
            if let Some(position) = position {
                result
                    .positioned
                    .push(PositionedMatchers { position, matchers });
            } else {
                result.unscoped = matchers;
            }
        }
        Ok(result)
    }

    /// Match values in their original iterator order, without collecting them.
    /// Missing positions match nothing; any exclusion vetoes the entire set.
    pub fn matches_values<'a>(
        &self,
        values: impl IntoIterator<Item = &'a [u8]>,
    ) -> bool {
        let mut included = !self.has_includes;
        let mut positioned = self.positioned.iter().peekable();
        for (index, value) in values.into_iter().enumerate() {
            if !self.unscoped.admit(value, &mut included) {
                return false;
            }
            if let Some(group) =
                positioned.next_if(|group| group.position == index)
                && !group.matchers.admit(value, &mut included)
            {
                return false;
            }
            if included && !self.has_excludes {
                return true;
            }
            // Later values can neither match nor veto.
            if positioned.peek().is_none() && self.unscoped.is_empty() {
                return included;
            }
        }
        included
    }

    #[inline]
    /// Match the command name alone: position 0 of the arguments. This is
    /// the per-record path for unindexed `--filter` patterns, so it avoids
    /// the general iterator.
    pub fn matches(&self, command: &[u8]) -> bool {
        if self.is_empty() {
            return true;
        }
        let mut included = !self.has_includes;
        let first = self.positioned.first().filter(|group| group.position == 0);
        self.unscoped.admit(command, &mut included)
            && first.is_none_or(|group| {
                group.matchers.admit(command, &mut included)
            })
            && included
    }
}

impl Matchers {
    const fn is_empty(&self) -> bool {
        self.include.is_empty() && self.exclude.is_empty()
    }

    /// Record whether `value` is included; returns false if it is excluded.
    #[inline]
    fn admit(&self, value: &[u8], included: &mut bool) -> bool {
        if self.excludes(value) {
            return false;
        }
        *included = *included || self.includes(value);
        true
    }

    fn includes(&self, value: &[u8]) -> bool {
        self.include.iter().any(|matcher| matcher.is_match(value))
    }

    fn excludes(&self, value: &[u8]) -> bool {
        self.exclude.iter().any(|matcher| matcher.is_match(value))
    }

    fn new(include: Vec<Pattern>, exclude: Vec<Pattern>) -> Result<Self> {
        let (inc_lits, inc_res) = Self::split_patterns(include);
        let (exc_lits, exc_res) = Self::split_patterns(exclude);
        let include = Self::build_matchers(&inc_lits, inc_res)
            .context("Failed to compile inclusive filters")?;
        let exclude = Self::build_matchers(&exc_lits, exc_res)
            .context("Failed to compile exclusive filters")?;
        Ok(Self { include, exclude })
    }

    fn split_patterns(
        mut patterns: Vec<Pattern>,
    ) -> (Vec<Vec<u8>>, Vec<Regex>) {
        // Deduplicate by kind and text without cloning compiled regexes or
        // using their internally mutable caches as hash keys.
        patterns.sort_unstable_by(|left, right| {
            matches!(left, Pattern::Regex(_))
                .cmp(&matches!(right, Pattern::Regex(_)))
                .then_with(|| left.as_str().cmp(right.as_str()))
        });
        patterns.dedup();
        let mut lits = Vec::new();
        let mut res = Vec::new();

        for p in patterns {
            match p {
                Pattern::Literal(s) => lits.push(s.into_bytes()),
                Pattern::Regex(r) => res.push(r),
            }
        }

        (lits, res)
    }

    fn build_matchers(
        lits: &[Vec<u8>],
        res: Vec<Regex>,
    ) -> Result<Vec<Matcher>> {
        let mut out = Vec::new();

        if !lits.is_empty() {
            let ac = AhoCorasickBuilder::new()
                .ascii_case_insensitive(true)
                .build(lits)?;
            out.push(Matcher::Literals(ac));
        }

        if !res.is_empty() {
            out.push(Matcher::Regexes(res));
        }

        Ok(out)
    }
}

fn matcher_counts(v: &[Matcher]) -> (usize, usize) {
    let mut lits = 0;
    let mut regs = 0;

    for m in v {
        match m {
            Matcher::Literals(_) => lits += 1,
            Matcher::Regexes(rs) => regs += rs.len(),
        }
    }

    (lits, regs)
}

impl fmt::Debug for Matchers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (inc_lit, inc_re) = matcher_counts(&self.include);
        let (exc_lit, exc_re) = matcher_counts(&self.exclude);

        f.debug_struct("Matchers")
            .field(
                "includes",
                &format_args!("{inc_lit} literal set(s), {inc_re} regex(es)"),
            )
            .field(
                "excludes",
                &format_args!("{exc_lit} literal set(s), {exc_re} regex(es)"),
            )
            .finish()
    }
}

/// Every per-record filter: database, command names and positional
/// arguments, command flags/categories, and keys.
#[derive(Clone)]
pub struct LineFilter {
    empty: bool,
    db: Option<u64>,
    names: Filter,
    keys: Filter,
    flags: commands::Filter,
}

impl fmt::Debug for LineFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LineFilter")
            .field("empty", &self.empty)
            .field("db", &self.db)
            .field("names", &self.names)
            .field("keys", &self.keys)
            .field("flags", &self.flags)
            .finish()
    }
}

impl LineFilter {
    pub const fn new(
        db: Option<u64>,
        names: Filter,
        keys: Filter,
        flags: commands::Filter,
    ) -> Self {
        let empty = db.is_none()
            && names.is_empty()
            && keys.is_empty()
            && flags.is_empty();
        Self {
            empty,
            db,
            names,
            keys,
            flags,
        }
    }

    /// Whether `COMMAND` metadata is needed to apply the filter.
    #[inline]
    pub const fn needs_cmds(&self) -> bool {
        !self.flags.is_empty() || !self.keys.is_empty()
    }

    /// Whether key discovery is required, making metadata mandatory.
    #[inline]
    pub const fn needs_keys(&self) -> bool {
        !self.keys.is_empty()
    }

    /// Decide whether a record is kept, using the cheapest checks first.
    /// Arguments are decoded into `args` only for positional or key filters.
    #[inline]
    pub fn matches<'a>(
        &self,
        commands: Option<&commands::Lookup>,
        line: &'a [u8],
        args: &mut Args<'a>,
    ) -> bool {
        if self.empty {
            return true;
        }

        if let Some(db) = self.db
            && monitor::database(line) != Some(db)
        {
            return false;
        }

        if self.names.is_empty()
            && self.flags.is_empty()
            && self.keys.is_empty()
        {
            return true;
        }

        let positional_args = self.names.needs_args();
        let Some(cmd) = monitor::command_name(line) else {
            // Without a command, only filters that inspect arguments or keys
            // can reject the record; the output reports it as invalid.
            return self.keys.is_empty() && !positional_args;
        };

        if !positional_args && !self.names.matches(cmd) {
            return false;
        }

        if !self.flags.is_empty()
            && commands
                .is_some_and(|lu| !lu.matches_bytes_or(cmd, self.flags, true))
        {
            return false;
        }
        if self.keys.is_empty() && !positional_args {
            return true;
        }
        if !self.keys.is_empty() && commands.is_none() {
            return false;
        }
        let Ok(record) = monitor::Record::parse(line) else {
            return false;
        };
        let Ok(args) = args.decode(&record) else {
            return false;
        };
        let cmd = record.cmd;
        if positional_args
            && !self.names.matches_values(
                std::iter::once(cmd).chain(args.iter().map(AsRef::as_ref)),
            )
        {
            return false;
        }
        if self.keys.is_empty() {
            return true;
        }
        commands.is_some_and(|commands| {
            commands
                .keys(cmd, args)
                .is_ok_and(|keys| self.keys.matches_values(keys))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(patterns: &[&str]) -> Filter {
        Filter::new(patterns.iter().map(|s| s.parse().unwrap()).collect())
            .unwrap()
    }

    #[test]
    fn any_include_matches_but_any_exclusion_vetoes_the_set() {
        let filter =
            filter(&["user:", "/^session:/", "!secret", "!/^private:/"]);
        assert!(filter.matches_values([b"other".as_slice(), b"USER:1"]));
        assert!(filter.matches_values([b"session:1".as_slice()]));
        assert!(!filter.matches_values([b"user:1".as_slice(), b"secret"]));
        assert!(
            !filter.matches_values([b"private:1".as_slice(), b"session:1"])
        );
        assert!(!filter.matches_values([b"user".as_slice(), b":1"]));
        assert!(!filter.matches_values(std::iter::empty()));
    }

    #[test]
    fn exclusion_only_and_empty_keys() {
        let filter = filter(&["!secret"]);
        assert!(filter.matches_values(std::iter::empty()));
        assert!(filter.matches_values([b"".as_slice(), b"public"]));
        assert!(!filter.matches_values([b"public".as_slice(), b"secret"]));
        assert!(self::filter(&["/^$/"]).matches_values([b"".as_slice()]));
        assert!(
            self::filter(&[r"/(?-u:\xff)/"])
                .matches_values([b"\xff".as_slice()])
        );
    }

    #[test]
    fn positional_patterns_select_only_the_requested_values() {
        let values = [b"zero".as_slice(), b"one", b"two", b"three"];
        assert!(filter(&["[3]three"]).matches_values(values));
        assert!(!filter(&["[2]three"]).matches_values(values));
        assert!(filter(&["[0]missing", "[3]/^three$/"]).matches_values(values));
        assert!(!filter(&["[0]zero", "![3]three"]).matches_values(values));
        assert!(filter(&["[0]zero", "![2]three"]).matches_values(values));
        assert!(!filter(&["zero", "![3]three"]).matches_values(values));
        assert!(!filter(&["[3]three", "!zero"]).matches_values(values));
        assert!(filter(&["[9]missing", "one"]).matches_values(values));
        assert!(!filter(&["[9]one"]).matches_values(values));
        assert!(filter(&["![9]one"]).matches_values(values));
        assert!(!filter(&["[0]"]).matches_values(std::iter::empty()));
        assert!(filter(&["[0]"]).matches_values([b"".as_slice()]));
        assert!(
            filter(&["[1]zero"]).matches_values([b"zero".as_slice(), b"zero"])
        );
    }

    #[test]
    fn force_literal_preserves_all_remaining_characters() {
        for (pattern, literal) in [
            ("=[0]foo", "[0]foo"),
            ("[0]=[0]foo", "[0]foo"),
            ("[0]=/foo/", "/foo/"),
            ("[0]=!foo", "!foo"),
            ("==foo", "=foo"),
            ("=[-1]foo", "[-1]foo"),
            ("=", ""),
            ("=foo\\bar", "foo\\bar"),
        ] {
            assert!(
                filter(&[pattern]).matches_values([literal.as_bytes()]),
                "{pattern}"
            );
        }
        assert!(!filter(&["[0]=/foo/"]).matches_values([b"foo".as_slice()]));
        assert!(
            !filter(&["!=!private"]).matches_values([b"!private".as_slice()])
        );
        assert!(
            !filter(&["![1]=[0]foo"])
                .matches_values([b"other".as_slice(), b"[0]foo"])
        );
        assert!(
            filter(&[r"/^\[0\]foo$/"]).matches_values([b"[0]foo".as_slice()])
        );
        assert!(filter(&["[0]/[0]/"]).matches_values([b"0".as_slice()]));
    }

    #[test]
    fn invalid_positions_are_errors_and_large_positions_do_not_allocate() {
        for pattern in [
            "[",
            "[]foo",
            "[-1]foo",
            "[+1]foo",
            "[1:3]foo",
            "[1,3]foo",
            "[a]foo",
            "[ 1]foo",
            "[1",
            "[184467440737095516160]foo",
            "[0]/[/",
        ] {
            assert!(pattern.parse::<FilterPattern>().is_err(), "{pattern}");
        }
        let pattern = format!("[{}]foo", usize::MAX);
        assert!(!filter(&[&pattern]).matches_values([b"foo".as_slice()]));
        assert!(
            filter(&["[01]one"]).matches_values([b"zero".as_slice(), b"one"])
        );
    }

    #[test]
    fn deduplication_keeps_regex_literal_and_position_distinctions() {
        let filter =
            filter(&["[0]=^foo$", "[0]/^foo$/", "[1]/^foo$/", "[1]/^foo$/"]);
        assert!(filter.matches_values([b"^foo$".as_slice()]));
        assert!(filter.matches_values([b"foo".as_slice()]));
        assert!(filter.matches_values([b"other".as_slice(), b"foo"]));
        assert!(!filter.matches_values([
            b"other".as_slice(),
            b"other",
            b"foo"
        ]));
    }

    #[test]
    fn command_matching_agrees_with_the_general_matcher() {
        let values: [&[u8]; 4] = [b"GET", b"get", b"SET", b""];
        for patterns in [
            &["GET"][..],
            &["!GET"],
            &["[0]/^G/", "!SET"],
            &["/E/", "[1]x"],
            &["[1]x"],
            &["![0]get", "set"],
        ] {
            let filter = filter(patterns);
            for value in values {
                assert_eq!(
                    filter.matches(value),
                    filter.matches_values([value]),
                    "{patterns:?} {value:?}"
                );
            }
        }
    }

    #[test]
    fn command_default_scope_is_zero_and_only_later_positions_need_args() {
        let compile = |patterns: &[&str]| {
            Filter::for_command(
                patterns.iter().map(|s| s.parse().unwrap()).collect(),
            )
            .unwrap()
        };
        let names = compile(&["GET"]);
        assert!(!names.needs_args());
        assert!(!names.matches_values([b"SET".as_slice(), b"GET"]));
        assert!(names.matches(b"GET"));
        assert!(!compile(&["[0]/^GET$/"]).needs_args());
        let names = compile(&["GET", "[1]value"]);
        assert!(names.needs_args());
        assert!(names.matches_values([b"SET".as_slice(), b"value"]));
        assert!(
            !compile(&["[1]value", "!GET"])
                .matches_values([b"GET".as_slice(), b"value"])
        );
    }
}
