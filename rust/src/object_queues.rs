use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sha1::Sha1;
use tokio::sync::broadcast;

use crate::structures::TopicRef;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RawData {
    pub content: Vec<u8>,
    pub content_type: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataSaved {
    pub index: usize,
    pub time_inserted: i32,
    pub digest: String,
}

#[derive(Debug)]
pub struct ObjectQueue {
    pub sequence: Vec<DataSaved>,
    pub data: HashMap<usize, RawData>,

    pub tx: broadcast::Sender<usize>,
    // rx: broadcast::Receiver<usize>,
    pub seq: usize,
    // name: TopicName,
    pub tr: TopicRef,
}

impl ObjectQueue {
    pub fn new(tr: TopicRef) -> Self {
        let (tx, _rx) = broadcast::channel(16);
        ObjectQueue {
            seq: 0,
            sequence: Vec::new(),
            data: HashMap::new(),
            tx,
            // rx,
            // name,
            tr,
        }
    }

    pub fn push(&mut self, data: &RawData) -> DataSaved {
        let this_seq = self.seq;
        self.seq += 1;
        // save the data in the hashmap
        self.data.insert(this_seq, data.clone());
        // save the index in the sequence

        let mut hasher = Sha1::new();
        // Update the hasher with the byte array
        hasher.update(data.content.as_slice());
        let hex_string = hasher.digest().to_string();

        let saved_data = DataSaved {
            index: this_seq,
            time_inserted: 0, // FIXME
            digest: hex_string,
        };
        self.sequence.push(saved_data.clone());
        if self.tx.receiver_count() > 0 {
            self.tx.send(this_seq).unwrap();
        }
        return saved_data
    }
}
