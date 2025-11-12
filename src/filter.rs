use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
    str::FromStr,
};

use aho_corasick::{AhoCorasick, AhoCorasickBuilder};
use anyhow::Result;
use memchr::memchr;
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

#[derive(Debug, Clone)]
pub struct Filter {
    include: Vec<Matcher>,
    exclude: Vec<Matcher>,
}

impl From<Vec<FilterPattern>> for Filter {
    fn from(patterns: Vec<FilterPattern>) -> Self {
        Self::new(patterns)
    }
}

impl Filter {
    fn unique_patterns(patterns: &[Pattern]) -> Vec<Pattern> {
        patterns
            .iter()
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn new(patterns: Vec<FilterPattern>) -> Self {
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

        let include = Self::build_matchers(&inc_lits, inc_res);
        let exclude = Self::build_matchers(&exc_lits, exc_res);

        Self { include, exclude }
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

    fn build_matchers(lits: &[Vec<u8>], res: Vec<Regex>) -> Vec<Matcher> {
        let mut out = Vec::new();

        if !lits.is_empty() {
            let ac = AhoCorasickBuilder::new()
                .ascii_case_insensitive(true)
                .build(lits)
                .unwrap_or_else(|e| {
                    panic!("Failed to build Aho-Corasick automaton: {e}")
                });
            out.push(Matcher::Literals(ac));
        }

        if !res.is_empty() {
            out.push(Matcher::Regexes(res));
        }

        out
    }

    const fn has_includes(&self) -> bool {
        !self.include.is_empty()
    }

    #[inline]
    fn cmd(line: &[u8]) -> Option<&[u8]> {
        let start = memchr(b'"', line)?;
        let rest = &line[start + 1..];
        let end_rel = memchr(b'"', rest)?;
        let end = start + 1 + end_rel;

        Some(&line[start + 1..end])
    }

    #[inline]
    pub fn matches(&self, value: &[u8]) -> bool {
        let value = Self::cmd(value).unwrap_or(value);

        // If a non-empty exclude matches, reject immediately.
        if self.exclude.iter().any(|matcher| matcher.is_match(value)) {
            return false;
        }

        // Trivial success: No includes defined.
        if !self.has_includes() {
            return true;
        }

        // Require at least one include match.
        self.include.iter().any(|matcher| matcher.is_match(value))
    }
}
