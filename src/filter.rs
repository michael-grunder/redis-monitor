use std::{
    collections::HashSet,
    fmt,
    hash::{Hash, Hasher},
    str::FromStr,
};

use aho_corasick::{AhoCorasick, AhoCorasickBuilder};
use anyhow::{Context, Result};
use regex::bytes::Regex;

#[derive(Debug, Clone)]
pub enum FilterPattern {
    Include(Pattern),
    Exclude(Pattern),
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
        self.as_str() == other.as_str()
    }
}

impl Eq for Pattern {}

impl Hash for Pattern {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl FromStr for FilterPattern {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let (negate, s) = s
            .strip_prefix('!')
            .map_or((false, s), |stripped| (true, stripped));

        let pattern = if let Some(inner) =
            s.strip_prefix('/').and_then(|s| s.strip_suffix('/'))
        {
            let re = Regex::new(inner)
                .map_err(|e| anyhow::anyhow!("Invalid regex '{inner}': {e}"))?;
            Pattern::Regex(re)
        } else {
            Pattern::Literal(s.to_string())
        };

        Ok(if negate {
            Self::Exclude(pattern)
        } else {
            Self::Include(pattern)
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

#[derive(Clone)]
pub struct Filter {
    include: Vec<Matcher>,
    exclude: Vec<Matcher>,
}

impl TryFrom<Vec<FilterPattern>> for Filter {
    type Error = anyhow::Error;

    fn try_from(patterns: Vec<FilterPattern>) -> Result<Self> {
        Self::new(patterns)
    }
}

impl Filter {
    pub const fn is_empty(&self) -> bool {
        self.include.is_empty() && self.exclude.is_empty()
    }

    fn unique_patterns(patterns: &[Pattern]) -> Vec<Pattern> {
        patterns
            .iter()
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn new(patterns: Vec<FilterPattern>) -> Result<Self> {
        let mut include = Vec::new();
        let mut exclude = Vec::new();

        for pattern in patterns {
            match pattern {
                FilterPattern::Include(p) => include.push(p),
                FilterPattern::Exclude(p) => exclude.push(p),
            }
        }

        let include = Self::unique_patterns(&include);
        let exclude = Self::unique_patterns(&exclude);

        let (inc_lits, inc_res) = Self::split_patterns(include);
        let (exc_lits, exc_res) = Self::split_patterns(exclude);

        let include = Self::build_matchers(&inc_lits, inc_res)
            .context("Failed to compile inclusive filters")?;
        let exclude = Self::build_matchers(&exc_lits, exc_res)
            .context("Failed to compile exclusive filters")?;

        Ok(Self { include, exclude })
    }

    fn split_patterns(patterns: Vec<Pattern>) -> (Vec<Vec<u8>>, Vec<Regex>) {
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

    const fn has_includes(&self) -> bool {
        !self.include.is_empty()
    }

    /// Match a set of values without joining or collecting them. Any exclusion
    /// vetoes the entire set, even after an earlier value matched an inclusion.
    pub fn matches_values<'a>(
        &self,
        values: impl IntoIterator<Item = &'a [u8]>,
    ) -> bool {
        let mut included = !self.has_includes();
        for value in values {
            if self.exclude.iter().any(|matcher| matcher.is_match(value)) {
                return false;
            }
            if !included {
                included =
                    self.include.iter().any(|matcher| matcher.is_match(value));
            }
            if included && self.exclude.is_empty() {
                return true;
            }
        }
        included
    }

    #[inline]
    pub fn matches(&self, command: &[u8]) -> bool {
        // If a non-empty exclude matches, reject immediately.
        if self.exclude.iter().any(|matcher| matcher.is_match(command)) {
            return false;
        }

        // Trivial success: No includes defined.
        if !self.has_includes() {
            return true;
        }

        // Require at least one include match.
        self.include.iter().any(|matcher| matcher.is_match(command))
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

impl fmt::Debug for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (inc_lit, inc_re) = matcher_counts(&self.include);
        let (exc_lit, exc_re) = matcher_counts(&self.exclude);

        f.debug_struct("NameFilter")
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
}
