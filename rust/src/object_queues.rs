use bytes::Bytes;
use std::collections::HashMap;

use chrono::Local;
use schemars::JsonSchema;

use serde::{Deserialize, Serialize};
use sha256::digest;
use tokio::sync::broadcast;

use crate::structures::TopicRefInternal;
use crate::{merge_clocks, Clocks, DataReady, MinMax, Notification, DTPSR};

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq)]
pub struct RawData {
    pub content: Bytes,
    pub content_type: String,
}

impl RawData {
    pub fn new<S: AsRef<[u8]>, T: AsRef<str>>(content: S, content_type: T) -> RawData {
        RawData {
            content: Bytes::from(content.as_ref().to_vec()),
            content_type: content_type.as_ref().to_string(),
        }
    }
    pub fn digest(&self) -> String {
        let d = digest(self.content.as_ref());
        format!("sha256:{}", d)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataSaved {
    pub origin_node: String,
    pub unique_id: String,
    pub index: usize,
    pub time_inserted: i64,
    pub clocks: Clocks,
    pub content_type: String,
    pub content_length: usize,
    pub digest: String,
}

#[derive(Debug)]
pub struct ObjectQueue {
    pub stored: Vec<usize>,
    pub saved: HashMap<usize, DataSaved>,

    pub tx: broadcast::Sender<usize>,
    pub seq: usize,
    pub tr: TopicRefInternal,
    pub max_history: Option<usize>,

    pub tx_notification: broadcast::Sender<InsertNotification>,
}

#[derive(Debug, Clone)]
pub struct InsertNotification {
    pub data_saved: DataSaved,
    pub raw_data: RawData,
}

impl ObjectQueue {
    pub fn new(tr: TopicRefInternal, max_history: Option<usize>) -> Self {
        let (tx, _rx) = broadcast::channel(1024);
        let (tx_notification, _rx) = broadcast::channel(1024);
        if let Some(max_history) = max_history {
            assert!(max_history > 0);
        }
        ObjectQueue {
            seq: 0,
            stored: Vec::new(),
            saved: HashMap::new(),
            // data: HashMap::new(),
            tx,
            tr,
            tx_notification,
            max_history,
        }
    }

    pub fn all_data(&self) -> Vec<DataSaved> {
        let mut v = Vec::new();
        for index in &self.stored {
            let data = self.saved.get(index).unwrap();
            v.push(data.clone());
        }
        v
    }

    pub fn push(&mut self, data: &RawData, previous_clocks: Option<Clocks>) -> DTPSR<DataSaved> {
        let now = Local::now().timestamp_nanos();

        let mut clocks = self.current_clocks(now);

        if let Some(previous_clocks) = previous_clocks {
            clocks = merge_clocks(&clocks, &previous_clocks);
        }

        let this_seq = self.seq;
        self.seq += 1;
        let digest = data.digest();

        let saved_data = DataSaved {
            origin_node: self.tr.origin_node.clone(),
            unique_id: self.tr.unique_id.clone(),
            index: this_seq,
            time_inserted: now,
            clocks: clocks.clone(),
            digest: digest.clone(),
            content_type: data.content_type.clone(),
            content_length: data.content.len(),
        };
        self.saved.insert(saved_data.index, saved_data.clone());
        self.stored.push(saved_data.index);
        if let Some(max_history) = self.max_history {
            while self.stored.len() > max_history {
                let index = self.stored.remove(0);
                self.saved.remove(&index);
            }
        }

        if self.tx.receiver_count() > 0 {
            self.tx.send(saved_data.index).unwrap(); // can only fail if there are no receivers
        }
        if self.tx_notification.receiver_count() > 0 {
            let notification = InsertNotification {
                data_saved: saved_data.clone(),
                raw_data: data.clone(),
            };
            self.tx_notification.send(notification).unwrap(); // can only fail if there are no receivers
        }
        return Ok(saved_data);
    }

    pub fn subscribe_insert_notification(&self) -> broadcast::Receiver<InsertNotification> {
        return self.tx_notification.subscribe();
    }

    fn current_clocks(&self, now: i64) -> Clocks {
        let mut clocks = Clocks::default();
        let this_seq = self.seq;

        let my_id = self.tr.unique_id.clone();
        if this_seq > 0 {
            let based_on = this_seq - 1;
            clocks
                .logical
                .insert(my_id.clone(), MinMax::new(based_on, based_on));
        }
        clocks.wall.insert(my_id.clone(), MinMax::new(now, now));
        clocks
    }
}
