use bytes::Bytes;
use std::collections::HashMap;

use chrono::Local;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};

use crate::constants::*;
use crate::object_queues::*;
use crate::signals_logic::TopicProperties;
use crate::structures::TypeOfConnection::Relative;
use crate::structures::*;
use crate::types::*;
use crate::TypeOfConnection::Same;
use crate::{get_queue_id, get_random_node_id, topics_index};

#[derive(Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub level: String,
    pub msg: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ForwardInfo {
    url: String,
}

#[derive(Debug)]
pub struct ServerState {
    pub node_started: i64,
    pub node_app_data: HashMap<String, Vec<u8>>,
    pub node_id: String,
    pub oqs: HashMap<TopicName, ObjectQueue>,
    pub forwards: HashMap<TopicName, ForwardInfo>,
    pub blobs: HashMap<String, Vec<u8>>,
    advertise_urls: Vec<String>,
}

impl ServerState {
    pub fn new(node_app_data: Option<HashMap<String, Vec<u8>>>) -> Self {
        let node_app_data = match node_app_data {
            Some(x) => x,
            None => HashMap::new(),
        };
        // let node_id = Uuid::new_v4().to_string();
        let node_id = get_random_node_id();

        let mut oqs = HashMap::new();

        let link_benchmark = LinkBenchmark {
            complexity: 0,
            latency: 0.0,
            bandwidth: 1_000_000_000.0,
            reliability: 1.0,
            hops: 0,
        };
        let now = Local::now().timestamp_nanos();
        let app_data = node_app_data.clone();
        let tr = TopicRefInternal {
            unique_id: node_id.clone(),
            origin_node: node_id.clone(),
            app_data,
            created: now,
            reachability: vec![TopicReachabilityInternal {
                con: Same(),
                answering: node_id.clone(),
                forwarders: vec![],
                benchmark: link_benchmark,
            }],
            properties: TopicProperties {
                streamable: true,
                pushable: true,
                readable: true,
                immutable: false,
            },
        };
        oqs.insert("".to_string(), ObjectQueue::new(tr));

        let node_started = Local::now().timestamp_nanos();
        let mut ss = ServerState {
            node_id,
            node_started,
            node_app_data,
            oqs,
            blobs: HashMap::new(),
            forwards: HashMap::new(),
            advertise_urls: vec![],
        };
        ss.new_topic(TOPIC_LIST_CLOCK, None);
        ss.new_topic(TOPIC_LIST_AVAILABILITY, None);
        ss.new_topic(TOPIC_LIST_NAME, None);
        ss.new_topic(TOPIC_LOGS, None);
        return ss;
    }

    pub fn add_advertise_url(&mut self, url: &str) {
        if self.advertise_urls.contains(&url.to_string()) {
            return;
        }
        self.advertise_urls.push(url.to_string());
        self.publish_object_as_json(TOPIC_LIST_AVAILABILITY, &self.advertise_urls.clone(), None);
    }
    pub fn get_advertise_urls(&self) -> Vec<String> {
        self.advertise_urls.clone()
    }
    pub fn log_message(&mut self, msg: String, level: &str) {
        let log_entry = LogEntry {
            level: level.to_string(),
            msg,
        };
        self.publish_object_as_json(TOPIC_LOGS, &log_entry, None);
    }
    pub fn debug(&mut self, msg: String) {
        debug!("{}", msg);
        self.log_message(msg, "debug");
    }
    pub fn info(&mut self, msg: String) {
        info!("{}", msg);
        self.log_message(msg, "info");
    }
    pub fn error(&mut self, msg: String) {
        error!("{}", msg);

        self.log_message(msg, "error");
    }
    pub fn warn(&mut self, msg: String) {
        warn!("{}", msg);
        self.log_message(msg, "warn");
    }

    pub fn new_topic(&mut self, topic_name: &str, app_data: Option<HashMap<String, Vec<u8>>>) {
        let topic_name = topic_name.to_string();
        let uuid = get_queue_id(&self.node_id, &topic_name);
        // let uuid = Uuid::new_v4();
        let app_data = app_data.unwrap_or_else(HashMap::new);

        let link_benchmark = LinkBenchmark {
            complexity: 0,
            latency: 0.0,
            bandwidth: 1_000_000_000.0,
            reliability: 1.0,
            hops: 0,
        };
        let origin_node = self.node_id.clone();
        let now = Local::now().timestamp_nanos();
        let tr = TopicRefInternal {
            unique_id: uuid.to_string(),
            origin_node,
            app_data,
            created: now,
            reachability: vec![TopicReachabilityInternal {
                con: Relative(format!("{}/", topic_name), None),
                answering: self.node_id.clone(),
                forwarders: vec![],
                benchmark: link_benchmark,
            }],
            properties: TopicProperties {
                streamable: true,
                pushable: false,
                readable: true,
                immutable: false,
            },
        };
        let oqs = &mut self.oqs;

        oqs.insert(topic_name.clone(), ObjectQueue::new(tr));
        let mut topics: Vec<String> = Vec::new();

        for topic_name in oqs.keys() {
            topics.push(topic_name.clone());
        }

        let index = topics_index(self);
        self.publish_object_as_cbor("", &index, None);

        self.publish_object_as_json(TOPIC_LIST_NAME, &topics.clone(), None);
    }
    pub fn make_sure_topic_exists(&mut self, topic_name: &str) {
        if !self.oqs.contains_key(topic_name) {
            info!("Queue {:?} does not exist, creating it.", topic_name);
            return self.new_topic(topic_name, None);
        }
    }

    pub fn publish(
        &mut self,
        topic_name: &str,
        content: &[u8],
        content_type: &str,
        clocks: Option<Clocks>,
    ) -> DataSaved {
        self.make_sure_topic_exists(topic_name);
        let v = content.to_vec();
        let data = RawData {
            content: Bytes::from(v),
            content_type: content_type.to_string(),
        };
        self.save_blob(&data.digest(), &data.content);
        let oq = self.oqs.get_mut(topic_name).unwrap();

        return oq.push(&data, clocks);
    }
    pub fn save_blob(&mut self, digest: &str, content: &[u8]) {
        self.blobs.insert(digest.to_string(), content.to_vec());
    }
    pub fn get_blob(&self, digest: &str) -> Option<&Vec<u8>> {
        return self.blobs.get(digest);
    }
    pub fn get_blob_bytes(&self, digest: &str) -> Option<Bytes> {
        self.blobs.get(digest).map(|v| Bytes::from(v.clone()))
    }
    pub fn publish_object_as_json<T: Serialize>(
        &mut self,
        topic_name: &str,
        object: &T,
        clocks: Option<Clocks>,
    ) -> DataSaved {
        let data_json = serde_json::to_string(object).unwrap();
        return self.publish_json(topic_name, data_json.as_str(), clocks);
    }

    pub fn publish_object_as_cbor<T: Serialize>(
        &mut self,
        topic_name: &str,
        object: &T,
        clocks: Option<Clocks>,
    ) -> DataSaved {
        let data_cbor = serde_cbor::to_vec(object).unwrap();
        return self.publish_cbor(topic_name, &data_cbor, clocks);
    }
    pub fn publish_cbor(
        &mut self,
        topic_name: &str,
        content: &[u8],
        clocks: Option<Clocks>,
    ) -> DataSaved {
        self.publish(topic_name, content, "application/cbor", clocks)
    }
    pub fn publish_json(
        &mut self,
        topic_name: &str,
        json_content: &str,
        clocks: Option<Clocks>,
    ) -> DataSaved {
        let bytesdata = json_content.as_bytes().to_vec();
        self.publish(topic_name, &bytesdata, "application/json", clocks)
    }
    pub fn publish_yaml(
        &mut self,
        topic_name: &str,
        yaml_content: &str,
        clocks: Option<Clocks>,
    ) -> DataSaved {
        let bytesdata = yaml_content.as_bytes().to_vec();
        self.publish(topic_name, &bytesdata, "application/yaml", clocks)
    }
    pub fn publish_plain(
        &mut self,
        topic_name: &str,
        text_content: &str,
        clocks: Option<Clocks>,
    ) -> DataSaved {
        let bytesdata = text_content.as_bytes().to_vec();
        self.publish(topic_name, &bytesdata, "text/plain", clocks)
    }
}
