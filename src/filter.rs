use anyhow::{Result, bail};
use regex::Regex;
use std::str::FromStr;

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
            Pattern::Literal(lit) => value.contains(lit),
            Pattern::Regex(re) => re.is_match(value),
        }
    }
}

impl FromStr for FilterPattern {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let (negate, s) = if s.starts_with('!') {
            (true, &s[1..])
        } else {
            (false, s)
        };

        let pattern = if let Some(inner) =
            s.strip_prefix('/').and_then(|s| s.strip_suffix('/'))
        {
            let re = Regex::new(inner).map_err(|e| {
                anyhow::anyhow!("Invalid regex '{}': {}", inner, e)
            })?;
            Pattern::Regex(re)
        } else {
            Pattern::Literal(s.to_string())
        };

        Ok(if negate {
            FilterPattern::Exclude(pattern)
        } else {
            FilterPattern::Include(pattern)
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
    pub fn new(patterns: Vec<FilterPattern>) -> Self {
        let mut include = Vec::new();
        let mut exclude = Vec::new();

        for pattern in patterns {
            match pattern {
                FilterPattern::Include(p) => include.push(p),
                FilterPattern::Exclude(p) => exclude.push(p),
            }
        }

        Self { include, exclude }
    }

    pub fn check(&self, value: &str) -> bool {
        if self.exclude.iter().any(|p| p.check(value)) {
            return false;
        }

        self.include.is_empty() || self.include.iter().any(|p| p.check(value))
    }
}
