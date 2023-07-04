use std::collections::HashMap;

use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json;
use tokio::sync::mpsc;
use warp::Filter;

use maplit::hashmap;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TopicsIndex {
    pub node_id: String,
    pub topics: HashMap<String, TopicRef>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TopicRef {
    pub unique_id: String,
    pub origin_node: String,
    pub app_static_data: Option<serde_json::Value>,
    pub reachability: Vec<TopicReachability>,
    pub debug_topic_type: String,
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
pub struct DataReady {
    pub sequence: i32,
    pub digest: String,
    pub urls: Vec<String>,
}
