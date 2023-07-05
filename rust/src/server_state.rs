use std::collections::HashMap;

use serde::Serialize;
use serde_json;
use uuid::Uuid;

use crate::constants::*;
use crate::object_queues::*;
use crate::structures::*;
use crate::types::*;

#[derive(Debug)]
pub struct ServerState {
    pub node_id: String,
    pub oqs: HashMap<TopicName, ObjectQueue>,
}

impl ServerState {
    pub fn new() -> Self {
        let uuid = Uuid::new_v4();

        let mut ss = ServerState {
            node_id: uuid.to_string(),
            oqs: HashMap::new(),
        };

        ss.new_topic(TOPIC_LIST_NAME);
        return ss;
    }

    pub fn new_topic(&mut self, topic_name: &str) -> () {
        let uuid = Uuid::new_v4();

        let link_benchmark = LinkBenchmark {
            complexity: 0,
            latency: 0.0,
            bandwidth: 1_000_000_000.0,
            reliability: 1.0,
            hops: 0,
        };
        let node_id = self.node_id.clone();
        let tr = TopicRef {
            unique_id: uuid.to_string(),
            origin_node: node_id.clone(),
            app_static_data: None,
            reachability: vec![TopicReachability {
                url: format!("topics/{}/", topic_name),
                answering: node_id.clone(),
                forwarders: vec![],
                benchmark: link_benchmark,
            }],
            debug_topic_type: "local".to_string(),
        };
        let oqs = &mut self.oqs;

        oqs.insert(topic_name.to_string(), ObjectQueue::new(tr));
        let mut topics: Vec<String> = Vec::new();

        for topic_name in oqs.keys() {
            topics.push(topic_name.clone());
        }

        self.publish_object_as_json(TOPIC_LIST_NAME, &topics.clone());
    }
    fn make_sure_topic_exists(&mut self, topic_name: &str) -> () {
        if !self.oqs.contains_key(topic_name) {
            self.new_topic(topic_name);
        }
    }

    pub fn publish(
        &mut self,
        topic_name: &str,
        content: &Vec<u8>,
        content_type: &str,
    ) -> DataSaved {
        self.make_sure_topic_exists(topic_name);
        let data = RawData {
            content: content.clone(),
            content_type: content_type.to_string(),
        };
        let oq = self.oqs.get_mut(topic_name).unwrap();
        return oq.push(&data);
    }
    pub fn publish_object_as_json<T: Serialize>(
        &mut self,
        topic_name: &str,
        object: &T,
    ) -> DataSaved {
        let data_json = serde_json::to_string(object).unwrap();
        return self.publish_json(topic_name, data_json.as_str());
    }

    pub fn publish_json(&mut self, topic_name: &str, json_content: &str) -> DataSaved {
        let bytesdata = json_content.as_bytes().to_vec();
        return self.publish(topic_name, &bytesdata, "application/json");
    }
    pub fn publish_plain(&mut self, topic_name: &str, text_content: &str) -> DataSaved {
        let bytesdata = text_content.as_bytes().to_vec();
        return self.publish(topic_name, &bytesdata, "text/plain");
    }
}
