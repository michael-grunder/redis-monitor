use aho_corasick::{AhoCorasick, AhoCorasickBuilder};
use anyhow::Result;
use regex::bytes::Regex;
use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
    str::FromStr,
};

#[derive(Debug, Clone)]
pub enum FilterPattern {
    Include(Pattern),
    Exclude(Pattern),
}

#[derive(Debug, Clone)]
pub enum Pattern {
    // Keep String so equality/hash use exact user text.
    Literal(String),
    // Bytes regex engine for matching on &[u8].
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
pub struct Filter {
    // literals → AC, optional because you may have none
    lit_include: Option<AhoCorasick>,
    lit_exclude: Option<AhoCorasick>,
    // regexes → just Vec
    re_include: Vec<Regex>,
    re_exclude: Vec<Regex>,
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

        // separate literals and regexes
        let mut lit_inc = Vec::new();
        let mut lit_exc = Vec::new();
        let mut re_inc = Vec::new();
        let mut re_exc = Vec::new();

        for p in include {
            match p {
                Pattern::Literal(s) => lit_inc.push(s.into_bytes()),
                Pattern::Regex(r) => re_inc.push(r),
            }
        }

        for p in exclude {
            match p {
                Pattern::Literal(s) => lit_exc.push(s.into_bytes()),
                Pattern::Regex(r) => re_exc.push(r),
            }
        }

        // build AC for literals if we have any
        let lit_include = if lit_inc.is_empty() {
            None
        } else {
            Some(
                AhoCorasickBuilder::new()
                    .ascii_case_insensitive(true)
                    .build(&lit_inc)
                    .unwrap_or_else(|e| {
                        panic!("Failed to build Aho-Corasick automaton: {e}")
                    }),
            )
        };

        let lit_exclude = if lit_exc.is_empty() {
            None
        } else {
            Some(
                AhoCorasickBuilder::new()
                    .ascii_case_insensitive(true)
                    .build(&lit_exc)
                    .unwrap_or_else(|e| {
                        panic!("Failed to build Aho-Corasick automaton: {e}")
                    }),
            )
        };

        Self {
            lit_include,
            lit_exclude,
            re_include: re_inc,
            re_exclude: re_exc,
        }
    }

    fn has_includes(&self) -> bool {
        self.lit_include.is_some() || !self.re_include.is_empty()
    }

    #[inline]
    pub fn check(&self, value: &[u8]) -> bool {
        // Short circuit if any excludes match.
        if let Some(ac) = &self.lit_exclude {
            if ac.is_match(value) {
                return false;
            }
        }

        // Short circuit regex excludes match
        if self.re_exclude.iter().any(|re| re.is_match(value)) {
            return false;
        }

        if !self.has_includes() {
            return true;
        }

        // Now check literal includes
        if let Some(ac) = &self.lit_include {
            if ac.is_match(value) {
                return true;
            }
        }

        // Finally check regex includes
        self.re_include.iter().any(|re| re.is_match(value))
    }
}
