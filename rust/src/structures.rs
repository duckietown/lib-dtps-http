use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TopicsIndex {
    pub node_id: String,
    pub node_started: i64,
    pub node_app_data: HashMap<String, Vec<u8>>,
    pub topics: HashMap<String, TopicRef>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TopicRef {
    pub unique_id: String,
    pub origin_node: String,
    pub app_data: HashMap<String, Vec<u8>>,
    pub reachability: Vec<TopicReachability>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LinkBenchmark {
    pub complexity: i32,
    pub bandwidth: f32,
    pub latency: f32,
    pub reliability: f32,
    pub hops: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ForwardingStep {
    pub forwarding_node: String,
    pub forwarding_node_connects_to: String,

    pub performance: LinkBenchmark,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TopicReachability {
    pub url: String,
    pub answering: String,
    pub forwarders: Vec<ForwardingStep>,
    pub benchmark: LinkBenchmark,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResourceAvailability {
    pub url: String,
    pub available_until: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataReady {
    pub sequence: usize,
    pub digest: String,
    pub content_type: String,
    pub content_length: usize,
    pub availability: Vec<ResourceAvailability>,
    pub chunks_arriving: usize,
}

#[derive(Debug, Clone)]
pub struct FoundMetadata {
    pub alternative_urls: Vec<Url>,
    pub answering: Option<String>,
    pub events_url: Option<Url>,
    pub events_data_inline_url: Option<Url>,
}
