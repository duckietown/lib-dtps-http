use bytes::Bytes;
use std::collections::HashMap;

use chrono::Local;
use derive_more::Constructor;
use serde::{Deserialize, Serialize};
use sha256::digest;
use tokio::sync::broadcast;

use crate::structures::TopicRefInternal;
use crate::{merge_clocks, Clocks, MinMax};
use serde_bytes::*;

#[derive(Serialize, Deserialize, Debug, Clone, Constructor)]
pub struct RawData {
    // #[serde(with = "serde_bytes")]
    pub content: Bytes,
    pub content_type: String,
}

impl RawData {
    pub fn digest(&self) -> String {
        // if self.content.len() < 16 {
        //     return self.content.clone();
        // }
        // let md5s = md5::compute(&self.content);
        // return format!("md5:{:x}", md5s);

        let d = digest(self.content.as_ref());
        format!("sha256:{}", d)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataSaved {
    pub index: usize,
    pub time_inserted: i64,
    pub clocks: Clocks,
    pub content_type: String,
    pub content_length: usize,
    pub digest: String,
}

#[derive(Debug)]
pub struct ObjectQueue {
    pub sequence: Vec<DataSaved>,
    // pub data: HashMap<String, RawData>,
    pub tx: broadcast::Sender<usize>,
    pub seq: usize,
    pub tr: TopicRefInternal,
}

impl ObjectQueue {
    pub fn new(tr: TopicRefInternal) -> Self {
        let (tx, _rx) = broadcast::channel(1024);
        ObjectQueue {
            seq: 0,
            sequence: Vec::new(),
            // data: HashMap::new(),
            tx,
            tr,
        }
    }

    pub fn push_data(
        &mut self,
        content_type: &str,
        content: &Vec<u8>,
        previous_clocks: Option<Clocks>,
    ) -> DataSaved {
        let data = RawData {
            content: content.clone().into(),
            content_type: content_type.to_string(),
        };
        self.push(&data, previous_clocks)
    }
    pub fn push(&mut self, data: &RawData, previous_clocks: Option<Clocks>) -> DataSaved {
        let now = Local::now().timestamp_nanos();

        let mut clocks = self.current_clocks(now);

        if let Some(previous_clocks) = previous_clocks {
            clocks = merge_clocks(&clocks, &previous_clocks);
        }

        let this_seq = self.seq;
        self.seq += 1;
        let digest = data.digest();

        let saved_data = DataSaved {
            index: this_seq,
            time_inserted: now,
            clocks: clocks.clone(),
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
