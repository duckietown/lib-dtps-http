use std::fmt::{Debug, Formatter};
use std::ops::Add;

use colored::Colorize;

use crate::{divide_in_components, vec_concat, DTPSError, DTPSR};

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TopicName {
    /// ["a", "b", "c"]
    components: Vec<String>,
    /// "a/b/c/"
    relative_url: String,
    /// "a/b/c"
    dash_sep: String,
}

impl Debug for TopicName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let a = "Topic(".green();
        let b = ")".green();
        write!(f, "{}{}{}", a, self.relative_url.yellow(), b)
        // f.write_str("TopicName(")?;
        // f.write_str(&self.dotted)?;
        // f.write_str(")")?;
        // f.finish()

        // f.debug_struct("TopicName")
        //     .field("components", &self.components)
        //     .field("dotted", &self.dotted)
        //     .finish()
    }
}

impl TopicName {
    /// Gives a list of components, e.g. "a/b/c" -> ["a", "b", "c"]
    pub fn as_components(&self) -> &Vec<String> {
        &self.components
    }
    pub fn to_components(&self) -> Vec<String> {
        self.components.clone()
    }

    pub fn is_root(&self) -> bool {
        self.components.is_empty()
    }

    pub fn to_relative_url(&self) -> String {
        self.as_relative_url().to_string()
    }
    /// Formats as  "a/b/c/" (no initial /)
    pub fn as_relative_url(&self) -> &str {
        &self.relative_url
    }

    /// Formats as  "a/b/c"
    pub fn to_dash_sep(&self) -> String {
        self.as_dash_sep().to_string()
    }
    pub fn as_dash_sep(&self) -> &str {
        &self.dash_sep
    }
    pub fn from_dash_sep<S: AsRef<str>>(s: S) -> DTPSR<Self> {
        let s = s.as_ref();

        if s.ends_with('/') {
            return DTPSError::other("did not expect this to end in /");
        }
        let components = divide_in_components(s, '/');

        Ok(Self::from_components(&components))
    }

    pub fn from_relative_url<S: AsRef<str>>(s: S) -> DTPSR<Self> {
        let s = s.as_ref();
        let components = divide_in_components(s, '/');
        Ok(Self::from_components(&components))
    }

    pub fn root() -> Self {
        Self::from_components(&vec![])
    }

    pub fn from_components(v: &Vec<String>) -> Self {
        let components = v.clone();

        let relative_url = if components.len() == 0 {
            "".to_string()
        } else {
            components.join("/") + "/"
        };
        let dash_sep = components.join("/");
        TopicName {
            components,
            relative_url,
            dash_sep,
        }
    }
    pub fn add_prefix(&self, v: &Vec<String>) -> Self {
        let a = vec_concat(&v, &self.components);
        TopicName::from_components(&a)
    }
}

impl Add for TopicName {
    type Output = TopicName;

    fn add(self, other: Self) -> TopicName {
        other.add_prefix(&self.components)
    }
}

impl<'a> Add for &'a TopicName {
    type Output = TopicName;

    fn add(self, other: Self) -> Self::Output {
        other.add_prefix(&self.components)
    }
}
