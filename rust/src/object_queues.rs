use bytes::Bytes;

use chrono::Local;
use schemars::JsonSchema;

use serde::{Deserialize, Serialize};
use sha256::digest;
use tokio::sync::broadcast;

use crate::structures::TopicRefInternal;
use crate::{merge_clocks, Clocks, DataReady, MinMax, Notification, DTPSR};

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
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

    pub tx_notification: broadcast::Sender<InsertNotification>,
}

#[derive(Debug, Clone)]
pub struct InsertNotification {
    pub data_saved: DataSaved,
    pub raw_data: RawData,
}

impl ObjectQueue {
    pub fn new(tr: TopicRefInternal) -> Self {
        let (tx, _rx) = broadcast::channel(1024);
        let (tx_notification, _rx) = broadcast::channel(1024);
        ObjectQueue {
            seq: 0,
            sequence: Vec::new(),
            // data: HashMap::new(),
            tx,
            tr,
            tx_notification,
        }
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
            index: this_seq,
            time_inserted: now,
            clocks: clocks.clone(),
            digest: digest.clone(),
            content_type: data.content_type.clone(),
            content_length: data.content.len(),
        };
        self.sequence.push(saved_data.clone());
        if self.tx.receiver_count() > 0 {
            self.tx.send(this_seq).unwrap(); // can only fail if there are no receivers
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
