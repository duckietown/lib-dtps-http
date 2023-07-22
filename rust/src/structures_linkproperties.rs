use std::cmp::{min, Ordering};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::ops::Add;
use std::path::PathBuf;

use derive_more::Constructor;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::signals_logic::TopicProperties;
use crate::urls::join_ext;
use crate::utils::divide_in_components;
use crate::{join_con, RawData, TopicName};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LinkBenchmark {
    pub complexity: u32,
    pub bandwidth: u32,
    pub latency: f32,
    pub reliability: f32,
    pub hops: i32,
}

impl LinkBenchmark {
    pub fn identity() -> Self {
        Self {
            complexity: 0,
            bandwidth: 1_000_000_000,
            latency: 0.0,
            reliability: 1.0,
            hops: 0,
        }
    }
}

impl Eq for LinkBenchmark {}

impl Add for LinkBenchmark {
    type Output = LinkBenchmark;

    fn add(self, rhs: Self) -> Self::Output {
        LinkBenchmark {
            complexity: self.complexity + rhs.complexity,
            bandwidth: min(self.bandwidth, rhs.bandwidth),
            latency: self.latency + rhs.latency,
            reliability: self.reliability * rhs.reliability,
            hops: self.hops + rhs.hops,
        }
    }
}

impl PartialEq<Self> for LinkBenchmark {
    fn eq(&self, other: &Self) -> bool {
        return self.complexity == other.complexity
            && self.bandwidth == other.bandwidth
            && self.latency == other.latency
            && self.reliability == other.reliability
            && self.hops == other.hops;
    }
}

impl PartialOrd<Self> for LinkBenchmark {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        return Some(self.cmp(other));
    }
}

impl Ord for LinkBenchmark {
    fn cmp(&self, other: &Self) -> Ordering {
        self.complexity
            .cmp(&other.complexity)
            .then_with(|| {
                self.bandwidth
                    .partial_cmp(&other.bandwidth)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| {
                self.latency
                    .partial_cmp(&other.latency)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| {
                self.reliability
                    .partial_cmp(&other.reliability)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| self.hops.cmp(&other.hops))
    }
}
