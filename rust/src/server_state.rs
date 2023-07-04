use std::collections::HashMap;

use serde_json;
use uuid::Uuid;
use crate::object_queues::*;
use crate::structures::*;
use crate::types::*;
use crate::constants::*;

#[derive(Debug)]
pub struct ServerState {
   pub  node_id: String,
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

        let tr = TopicRef {
            unique_id: uuid.to_string(),
            origin_node: self.node_id.clone(),
            app_static_data: None,
            reachability: vec![
                TopicReachability {
                    url: format!("topics/{}/", topic_name),
                    answering: self.node_id.clone(),
                    forwarders: vec![],
                    benchmark: link_benchmark,
                }
            ],
            debug_topic_type: "local".to_string(),
        };
        self.oqs.insert(topic_name.to_string(), ObjectQueue::new(tr));

        // get a list of all the topics in the server in json
        let topics = self.oqs.keys().collect::<Vec<&String>>();
        let topics_json = serde_json::to_string(&topics).unwrap();

        self.push(TOPIC_LIST_NAME, &RawData {
            content: topics_json.as_bytes().to_vec(),
            content_type: "application/json".to_string(),
        });
    }
    fn make_sure_topic_exists(&mut self, topic_name: &str) -> () {
        if !self.oqs.contains_key(topic_name) {
            self.new_topic(topic_name);
        }
    }
    pub fn push(&mut self, topic_name: &str, data: &RawData) -> DataSaved {
        self.make_sure_topic_exists(topic_name);
        let oq = self.oqs.get_mut(topic_name).unwrap();
        oq.push(data)
    }
}
