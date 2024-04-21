use std::collections::HashSet;

#[derive(Debug)]
pub struct Filter {
    include: HashSet<String>,
    exclude: HashSet<String>,
}

impl Filter {
    fn should_include(&self, value: &str) -> bool {
        if self.include.is_empty() {
            return true;
        }

        if self.include.contains(value) {
            return true;
        }

        false
    }

    fn should_exclude(&self, value: &str) -> bool {
        if self.exclude.is_empty() {
            false
        } else {
            !self.exclude.contains(value)
        }
    }

    pub fn filter(&self, value: &str) -> bool {
        let value = value.to_lowercase();

        !self.should_include(&value) || self.should_exclude(&value)

        //        if self.should_include(&value) {
        //            self.should_exclude(&value)
        //        } else {
        //            true
        //        }
    }

    pub fn from_args(include: Vec<String>, exclude: Vec<String>) -> Self {
        Self {
            include: include.into_iter().map(|m| m.to_lowercase()).collect(),
            exclude: exclude.into_iter().map(|m| m.to_lowercase()).collect(),
        }
    }
}
