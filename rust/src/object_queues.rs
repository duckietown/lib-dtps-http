use std::collections::HashMap;

use bytes::Bytes;
use chrono::Local;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha256::digest;
use tokio::sync::broadcast;

use crate::structures::TopicRefInternal;
use crate::{
    identify_presentation, merge_clocks, Clocks, ContentPresentation, DTPSError, MinMax,
    ServerState, CONTENT_TYPE_CBOR, CONTENT_TYPE_JSON, DTPSR,
};

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
    pub fn cbor<S: AsRef<[u8]>>(content: S) -> RawData {
        Self::new(content, CONTENT_TYPE_CBOR)
    }
    pub fn json<S: AsRef<[u8]>>(content: S) -> RawData {
        Self::new(content, CONTENT_TYPE_JSON)
    }
    pub fn digest(&self) -> String {
        let d = digest(self.content.as_ref());
        format!("sha256:{}", d)
    }

    pub fn represent_as_json<T: Serialize>(x: T) -> DTPSR<Self> {
        Ok(RawData::new(serde_json::to_vec(&x)?, CONTENT_TYPE_JSON))
    }
    pub fn represent_as_cbor<T: Serialize>(x: T) -> DTPSR<Self> {
        Ok(RawData::new(serde_cbor::to_vec(&x)?, CONTENT_TYPE_CBOR))
    }
    pub fn from_cbor_value(x: &serde_cbor::Value) -> DTPSR<Self> {
        Ok(RawData::cbor(serde_cbor::to_vec(x)?))
    }
    pub fn from_json_value(x: &serde_json::Value) -> DTPSR<Self> {
        Ok(RawData::json(serde_json::to_vec(x)?))
    }
    /// Deserializes the content of this object as a given type.
    /// The type must implement serde::Deserialize.
    /// It works only for CBOR, JSON and YAML.
    pub fn interpret<'a, T>(&'a self) -> DTPSR<T>
    where
        T: Deserialize<'a> + Clone,
    {
        match self.presentation() {
            ContentPresentation::CBOR => {
                let v: T = serde_cbor::from_slice::<T>(&self.content)?;
                Ok(v.clone())
            }
            ContentPresentation::JSON => {
                let v: T = serde_json::from_slice::<T>(&self.content)?;
                Ok(v.clone())
            }
            ContentPresentation::YAML => {
                let v: T = serde_yaml::from_slice::<T>(&self.content)?;
                Ok(v.clone())
            }
            ContentPresentation::PlainText => DTPSError::other("cannot interpret plain text"),
            ContentPresentation::Other => DTPSError::other("cannot interpret unknown content type"),
        }
    }

    pub fn presentation(&self) -> ContentPresentation {
        identify_presentation(&self.content_type)
    }

    pub fn try_translate(&self, ct: &str) -> DTPSR<Self> {
        if self.content_type == ct {
            return Ok(self.clone());
        }
        let mine = identify_presentation(self.content_type.as_str());
        let desired = identify_presentation(ct);

        let value = self.get_as_cbor()?;
        let bytes = match desired {
            ContentPresentation::CBOR => serde_cbor::to_vec(&value)?,
            ContentPresentation::JSON => serde_json::to_vec(&value)?,
            ContentPresentation::YAML => Bytes::from(serde_yaml::to_string(&value)?).to_vec(),
            ContentPresentation::PlainText | ContentPresentation::Other => {
                return DTPSError::other(format!(
                    "cannot translate from {:?} to {:?}",
                    mine, desired
                ));
            }
        };
        Ok(RawData::new(bytes, ct))
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

    pub fn you_are_being_deleted(&self) {

        // is it all done with the drop?
    }
    pub fn push(
        &mut self,
        data0: &RawData,
        previous_clocks: Option<Clocks>,
    ) -> DTPSR<(RawData, DataSaved, Vec<String>)> {
        let expect = self.tr.content_info.storage.content_type.clone();
        let obtained = data0.content_type.clone();
        let same = expect == obtained;
        let data = if !same {
            match data0.try_translate(&expect) {
                Ok(d) => d,
                Err(e) => {
                    // panic!("{e}");
                    return Err(e);
                }
            }
        } else {
            data0.clone()
        };
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
        let mut dropped = Vec::new();
        if let Some(max_history) = self.max_history {
            while self.stored.len() > max_history {
                let index = self.stored.remove(0);
                let ds = self.saved.remove(&index).unwrap();
                dropped.push(ds.digest);
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
        return Ok((data, saved_data, dropped));
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
