use std::str::FromStr;

use regex::Regex;
use serde::{Deserialize, Deserializer};

#[derive(Debug, Clone)]
pub enum FilterArg {
    Literal(String),
    Regex(regex::Regex),
}

pub struct Filter(Vec<FilterArg>);

impl FromStr for FilterArg {
    type Err = String; // or a custom error if you prefer

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(re) = s.strip_prefix('/').and_then(|s| s.strip_suffix('/'))
        {
            Regex::new(re)
                .map(FilterArg::Regex)
                .map_err(|e| e.to_string())
        } else {
            Ok(FilterArg::Literal(s.to_string()))
        }
    }
}

impl FilterArg {
    fn check(&self, value: &str) -> bool {
        match self {
            FilterArg::Literal(literal) => value.contains(literal),
            FilterArg::Regex(regex) => regex.is_match(value),
        }
    }
}

impl Filter {
    pub fn check(&self, value: &str) -> bool {
        self.0.iter().any(|arg| arg.check(value))
    }
}

impl From<Vec<FilterArg>> for Filter {
    fn from(args: Vec<FilterArg>) -> Self {
        Filter(args)
    }
}
