use crate::{divide_in_components, vec_concat, DTPSR};
use std::fmt::{Debug, Formatter};
use std::ops::Add;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TopicName {
    components: Vec<String>,
    relative_url: String,
}
use colored::Colorize;

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
    // formats as "a.b.c"
    // pub fn as_dotted(&self) -> &str {
    //     &self.dotted
    // }

    // pub fn to_dotted(&self) -> String {
    //     self.dotted.to_string()
    // }

    pub fn to_relative_url(&self) -> String {
        self.as_relative_url().to_string()
    }
    /// Formats as  "a/b/c/" (no initial /)
    pub fn as_relative_url(&self) -> &str {
        &self.relative_url
    }
    pub fn from_relative_url<S: AsRef<str>>(s: S) -> DTPSR<Self> {
        let s = s.as_ref();
        let components = divide_in_components(s, '/');
        let relative = components.join("/");
        Ok(TopicName {
            components: components.clone(),
            relative_url: relative,
        })
    }

    pub fn root() -> Self {
        TopicName {
            components: vec![],
            relative_url: "".to_string(),
        }
    }

    pub fn from_components(v: &Vec<String>) -> Self {
        let components = v.clone();
        let dotted2 = components.join("/");
        TopicName {
            components: components.clone(),
            relative_url: dotted2,
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
