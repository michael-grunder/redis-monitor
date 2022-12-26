use std::{
    cmp::{Eq, PartialEq},
    collections::HashSet,
    convert::From,
    hash::{Hash, Hasher},
    str::FromStr,
};

#[derive(Debug, Eq)]
struct CaselessString(String);

#[derive(Debug)]
pub struct Filter {
    include: HashSet<CaselessString>,
    exclude: HashSet<CaselessString>,
}

impl PartialEq for CaselessString {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_ignore_ascii_case(&other.0)
    }
}

impl Hash for CaselessString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_ascii_lowercase().hash(state)
    }
}

impl From<String> for CaselessString {
    fn from(s: String) -> Self {
        Self { 0: s.to_owned() }
    }
}

impl Filter {
    pub fn new() -> Self {
        Self {
            include: HashSet::new(),
            exclude: HashSet::new(),
        }
    }

    pub fn add_include(&mut self, s: &str) -> bool {
        self.include.insert(s.to_owned().into())
    }

    pub fn add_exclude(&mut self, s: &str) -> bool {
        self.exclude.insert(s.to_owned().into())
    }

    fn should_include(&self, value: &str) -> bool {
        self.include.is_empty() || self.include.contains(value)
    }

    fn should_exclude(&self, value: &str) -> bool {
        !self.exclude.contains(value)
    }

    pub fn filter(&self, value: &str) -> bool {
        self.should_include(value) && !self.should_exclude(value)
    }
}
