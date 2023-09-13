use std::{
    cmp::{
        min,
        Ordering,
    },
    ops::Add,
};

use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
pub struct LinkBenchmark {
    /// integer (complexity sums)
    pub complexity: u32,

    /// bytes/s
    pub bandwidth: u32,

    /// nanoseconds
    pub latency_ns: u128,

    /// 0...100; multiplied together
    pub reliability_percent: u8,

    pub hops: i32,
}

impl LinkBenchmark {
    pub fn identity() -> Self {
        Self {
            complexity: 0,
            bandwidth: 1_000_000_000,
            latency_ns: 0,
            reliability_percent: 100,
            hops: 0,
        }
    }
}

impl Add for LinkBenchmark {
    type Output = LinkBenchmark;

    fn add(self, rhs: Self) -> Self::Output {
        LinkBenchmark {
            complexity: self.complexity + rhs.complexity,
            bandwidth: min(self.bandwidth, rhs.bandwidth),
            latency_ns: self.latency_ns + rhs.latency_ns,
            reliability_percent: ((self.reliability_percent as u32) * (rhs.reliability_percent as u32) / (100 * 100))
                as u8,
            hops: self.hops + rhs.hops,
        }
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
            .then_with(|| self.bandwidth.partial_cmp(&other.bandwidth).unwrap_or(Ordering::Equal))
            .then_with(|| {
                self.latency_ns
                    .partial_cmp(&other.latency_ns)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| {
                self.reliability_percent
                    .partial_cmp(&other.reliability_percent)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| self.hops.cmp(&other.hops))
    }
}
