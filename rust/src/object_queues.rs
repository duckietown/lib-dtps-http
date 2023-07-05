use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sha256::digest;
use tokio::sync::broadcast;

use crate::structures::TopicRef;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RawData {
    pub content: Vec<u8>,
    pub content_type: String,
}

impl RawData {
    pub fn digest(&self) -> String {
        let d = digest(self.content.as_slice());
        format!("sha256:{}", d)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataSaved {
    pub index: usize,
    pub time_inserted: i32,
    pub content_type: String,
    pub content_length: usize,
    pub digest: String,
}

#[derive(Debug)]
pub struct ObjectQueue {
    pub sequence: Vec<DataSaved>,
    pub data: HashMap<String, RawData>,

    pub tx: broadcast::Sender<usize>,
    pub seq: usize,
    pub tr: TopicRef,
}

impl ObjectQueue {
    pub fn new(tr: TopicRef) -> Self {
        let (tx, _rx) = broadcast::channel(1024);
        ObjectQueue {
            seq: 0,
            sequence: Vec::new(),
            data: HashMap::new(),
            tx,
            tr,
        }
    }

    pub fn push(&mut self, data: &RawData) -> DataSaved {
        let this_seq = self.seq;
        self.seq += 1;
        let digest = data.digest();
        self.data.insert(digest.clone(), data.clone());

        let saved_data = DataSaved {
            index: this_seq,
            time_inserted: 0, // FIXME
            digest: digest.clone(),
            content_type: data.content_type.clone(),
            content_length: data.content.len(),
        };
        self.sequence.push(saved_data.clone());
        if self.tx.receiver_count() > 0 {
            self.tx.send(this_seq).unwrap();
        }
        return saved_data;
    }
}
