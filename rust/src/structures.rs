use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;

use derive_more::Constructor;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::urls::join_ext;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TopicsIndexWire {
    pub node_id: String,
    pub node_started: i64,
    pub node_app_data: HashMap<String, Vec<u8>>,
    pub topics: HashMap<String, TopicRefWire>,
}

#[derive(Debug, Clone)]
pub struct TopicsIndexInternal {
    pub node_id: String,
    pub node_started: i64,
    pub node_app_data: HashMap<String, Vec<u8>>,
    pub topics: HashMap<String, TopicRefInternal>,
}

impl TopicsIndexInternal {
    pub fn to_wire(self, use_rel: Option<String>) -> TopicsIndexWire {
        let mut topics = HashMap::new();
        for (topic_name, topic_ref_internal) in self.topics {
            let topic_ref_wire = topic_ref_internal.to_wire(use_rel.clone());
            topics.insert(topic_name, topic_ref_wire);
        }
        TopicsIndexWire {
            node_id: self.node_id,
            node_started: self.node_started,
            node_app_data: self.node_app_data,
            topics,
        }
    }
    pub fn from_wire(wire: TopicsIndexWire, conbase: &TypeOfConnection) -> Self {
        let mut topics = HashMap::new();
        for (topic_name, topic_ref_wire) in &wire.topics {
            let topic_ref_internal = TopicRefInternal::from_wire(topic_ref_wire, conbase);
            topics.insert(topic_name.clone(), topic_ref_internal);
        }
        TopicsIndexInternal {
            node_id: wire.node_id,
            node_started: wire.node_started,
            node_app_data: wire.node_app_data,
            topics,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TopicRefWire {
    pub unique_id: String,
    pub origin_node: String,
    pub app_data: HashMap<String, Vec<u8>>,
    pub reachability: Vec<TopicReachabilityWire>,
    pub created: i64,
}

#[derive(Debug, Clone)]
pub struct TopicRefInternal {
    pub unique_id: String,
    pub origin_node: String,
    pub app_data: HashMap<String, Vec<u8>>,
    pub reachability: Vec<TopicReachabilityInternal>,
    pub created: i64,
}

impl TopicRefInternal {
    pub fn to_wire(&self, use_rel: Option<String>) -> TopicRefWire {
        let mut reachability = Vec::new();
        for topic_reachability_internal in &self.reachability {
            let topic_reachability_wire = topic_reachability_internal.to_wire(use_rel.clone());
            reachability.push(topic_reachability_wire);
        }
        TopicRefWire {
            unique_id: self.unique_id.clone(),
            origin_node: self.origin_node.clone(),
            app_data: self.app_data.clone(),
            created: self.created,
            reachability,
        }
    }
    pub fn from_wire(wire: &TopicRefWire, conbase: &TypeOfConnection) -> Self {
        let mut reachability = Vec::new();
        for topic_reachability_wire in &wire.reachability {
            let topic_reachability_internal =
                TopicReachabilityInternal::from_wire(topic_reachability_wire, conbase);
            reachability.push(topic_reachability_internal);
        }
        TopicRefInternal {
            unique_id: wire.unique_id.clone(),
            origin_node: wire.origin_node.clone(),
            app_data: wire.app_data.clone(),
            created: wire.created,
            reachability,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LinkBenchmark {
    pub complexity: u32,
    pub bandwidth: f32,
    pub latency: f32,
    pub reliability: f32,
    pub hops: i32,
}

impl Eq for LinkBenchmark {}

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ForwardingStep {
    pub forwarding_node: String,
    pub forwarding_node_connects_to: String,

    pub performance: LinkBenchmark,
}

#[derive(Debug, Clone)]
pub struct UnixCon {
    pub scheme: String,
    pub socket_name: String,
    pub path: String,
    pub query: Option<String>,
}

impl Display for UnixCon {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UnixCon(")?;
        write!(f, "{}", self.socket_name)?;
        write!(f, "{}", self.path)?;
        if let Some(query) = &self.query {
            write!(f, "?{}", query)?;
        }
        write!(f, ")")?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum TypeOfConnection {
    /// a
    TCP(Url),
    /// b
    UNIX(UnixCon),
    /// c
    Relative(String, Option<String>), // path, query
}

impl TypeOfConnection {
    pub fn to_string(&self) -> String {
        match self {
            TypeOfConnection::TCP(url) => url.to_string(),
            TypeOfConnection::Relative(s, q) => match q {
                Some(query) => {
                    let mut s = s.clone();
                    s.push_str("?");
                    s.push_str(&query);
                    s
                }
                None => s.clone(),
            },
            TypeOfConnection::UNIX(unixcon) => {
                let mut s = unixcon.scheme.clone();
                s.push_str("://");
                s.push_str(&unixcon.socket_name);
                s.push_str(&unixcon.path);
                if let Some(query) = &unixcon.query {
                    s.push_str("?");
                    s.push_str(&query);
                }
                s
            }
        }
    }
}

impl fmt::Display for TypeOfConnection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TypeOfConnection::TCP(url) => write!(f, "TCP({:?})", url.to_string()),
            TypeOfConnection::UNIX(unix_con) => write!(f, "UNIX({})", unix_con),
            TypeOfConnection::Relative(s, q) => write!(f, "Relative({},{:?})", s, q),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TopicReachabilityInternal {
    pub con: TypeOfConnection,
    pub answering: String,
    pub forwarders: Vec<ForwardingStep>,
    pub benchmark: LinkBenchmark,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TopicReachabilityWire {
    pub url: String,
    pub answering: String,
    pub forwarders: Vec<ForwardingStep>,
    pub benchmark: LinkBenchmark,
}

impl TopicReachabilityInternal {
    pub fn to_wire(&self, use_rel: Option<String>) -> TopicReachabilityWire {
        let con = match use_rel {
            None => self.con.clone(),
            Some(use_patch) => match &self.con {
                TypeOfConnection::TCP(_) => self.con.clone(),
                TypeOfConnection::UNIX(_) => self.con.clone(),
                TypeOfConnection::Relative(_, query) => {
                    TypeOfConnection::Relative(use_patch.clone(), query.clone())
                }
            },
        };

        TopicReachabilityWire {
            url: con.to_string(),
            answering: self.answering.clone(),
            forwarders: self.forwarders.clone(),
            benchmark: self.benchmark.clone(),
        }
    }
    pub fn from_wire(wire: &TopicReachabilityWire, conbase: &TypeOfConnection) -> Self {
        TopicReachabilityInternal {
            con: join_ext(conbase, &wire.url).unwrap(),
            answering: wire.answering.clone(),
            forwarders: wire.forwarders.clone(),
            benchmark: wire.benchmark.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResourceAvailabilityWire {
    pub url: String,
    pub available_until: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Constructor)]
pub struct MinMax<T: Ord + Clone> {
    pub min: T,
    pub max: T,
}

pub fn merge_minmax<T: Ord + Clone>(minmax1: &MinMax<T>, minmax2: &MinMax<T>) -> MinMax<T> {
    let mut min = minmax1.min.clone();
    let mut max = minmax1.max.clone();
    if minmax2.min < min {
        min = minmax2.min.clone();
    }
    if minmax2.max > max {
        max = minmax2.max.clone();
    }
    MinMax::new(min, max)
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Constructor)]
pub struct Clocks {
    pub logical: HashMap<String, MinMax<usize>>,
    pub wall: HashMap<String, MinMax<i64>>,
}

pub fn merge_to<T: Ord + Clone>(
    x: &mut HashMap<String, MinMax<T>>,
    y: &HashMap<String, MinMax<T>>,
) {
    for (key, minmax) in y {
        if let Some(minmax2) = x.get(key) {
            x.insert(key.clone(), merge_minmax(minmax, minmax2));
        } else {
            x.insert(key.clone(), minmax.clone());
        }
    }
}

pub fn merge_clocks(clock1: &Clocks, clock2: &Clocks) -> Clocks {
    let mut logical = HashMap::new();
    let mut wall = HashMap::new();

    merge_to(&mut logical, &clock1.logical);
    merge_to(&mut logical, &clock2.logical);
    merge_to(&mut wall, &clock1.wall);
    merge_to(&mut wall, &clock2.wall);

    Clocks::new(logical, wall)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataReady {
    pub origin_node: String,
    pub unique_id: String,
    pub sequence: usize,
    pub time_inserted: i64,
    pub digest: String,
    pub content_type: String,
    pub content_length: usize,
    pub clocks: Clocks,
    pub availability: Vec<ResourceAvailabilityWire>,
    pub chunks_arriving: usize,
}

#[derive(Debug, Clone)]
pub struct FoundMetadata {
    pub alternative_urls: Vec<TypeOfConnection>,
    pub answering: Option<String>,
    pub events_url: Option<TypeOfConnection>,
    pub events_data_inline_url: Option<TypeOfConnection>,
    pub latency_ns: u128, // nanoseconds
}
