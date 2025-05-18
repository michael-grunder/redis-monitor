use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
};

#[derive(Debug, Clone, Eq)]
struct FilterString(String);

#[derive(Debug)]
pub struct Filter {
    include: HashSet<FilterString>,
    exclude: HashSet<FilterString>,
}

impl From<&str> for FilterString {
    fn from(s: &str) -> Self {
        FilterString(s.to_ascii_lowercase())
    }
}

impl From<String> for FilterString {
    fn from(s: String) -> Self {
        FilterString(s.into())
    }
}

impl PartialEq for FilterString {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_ignore_ascii_case(&other.0)
    }
}

impl Hash for FilterString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for b in self.0.bytes() {
            state.write_u8(b.to_ascii_lowercase());
        }
    }
}

impl std::borrow::Borrow<str> for FilterString {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl Filter {
    pub fn is_empty(&self) -> bool {
        self.include.is_empty() && self.exclude.is_empty()
    }

    fn should_include(&self, value: &str) -> bool {
        self.include.is_empty() || self.include.contains(value)
    }

    fn should_exclude(&self, value: &str) -> bool {
        !self.exclude.is_empty() && !self.exclude.contains(value)
    }

    pub fn filter(&self, value: &str) -> bool {
        !self.should_include(&value) || self.should_exclude(&value)
    }

    pub fn from_args(include: Vec<String>, exclude: Vec<String>) -> Self {
        Self {
            include: include.into_iter().map(Into::into).collect(),
            exclude: exclude.into_iter().map(Into::into).collect(),
        }
    }
}
