use crate::{divide_in_components, make_rel_url, vec_concat};
use std::fmt::{Debug, Formatter};
use std::ops::Add;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TopicName {
    components: Vec<String>,
    dotted: String,
}
use colored::Colorize;

impl Debug for TopicName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let a = "Topic(".green();
        let b = ")".green();
        write!(f, "{}{}{}", a, self.dotted.yellow(), b)
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
    // formats as "a.b.c"
    pub fn as_dotted(&self) -> &str {
        &self.dotted
    }

    pub fn to_dotted(&self) -> String {
        self.dotted.to_string()
    }
    /// Formats as  "a/b/c/" (no initial /)
    pub fn as_relative_url(&self) -> String {
        let components = self.as_components();
        make_rel_url(&components)
    }
    // pub fn new<S: AsRef<str>>(s: S) -> Self {
    //     Self::parse_dotted(s)
    //     TopicName(s.as_ref().to_string())
    // }

    pub fn root() -> Self {
        TopicName {
            components: vec![],
            dotted: "".to_string(),
        }
    }

    pub fn from_dotted<S: AsRef<str>>(s: S) -> Self {
        let s = s.as_ref();
        let components = divide_in_components(s, '.');
        let dotted2 = components.join(".");
        TopicName {
            components: components.clone(),
            dotted: dotted2,
        }
    }
    pub fn from_components(v: &Vec<String>) -> Self {
        let components = v.clone();
        let dotted2 = components.join(".");
        TopicName {
            components: components.clone(),
            dotted: dotted2,
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

// impl<S: AsRef<str>> From<S> for TopicName {
//     fn from(s: S) -> Self {
//         TopicName(s.as_ref().to_string())
//     }
// }
//
// impl Into<String> for TopicName {
//     fn into(self) -> String {
//         self.0
//     }
// }
