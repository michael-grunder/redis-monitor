use anyhow::Result;
use regex::Regex;
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
    Literal(String),
    Regex(Regex),
}

impl Pattern {
    fn check(&self, value: &str) -> bool {
        match self {
            Self::Literal(lit) => value.contains(lit),
            Self::Regex(re) => re.is_match(value),
        }
    }

    fn as_str(&self) -> &str {
        match self {
            Self::Literal(lit) => lit,
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
    include: Vec<Pattern>,
    exclude: Vec<Pattern>,
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

        Self {
            include: Self::unique_patterns(&include),
            exclude: Self::unique_patterns(&exclude),
        }
    }

    pub fn check(&self, value: &str) -> bool {
        if self.exclude.iter().any(|p| p.check(value)) {
            return false;
        }

        self.include.is_empty() || self.include.iter().any(|p| p.check(value))
    }
}
