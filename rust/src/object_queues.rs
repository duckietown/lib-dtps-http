use std::collections::HashMap;

use chrono::Local;

use tokio::sync::broadcast;

use crate::{
    merge_clocks, utils::time_nanos_i64, Clocks, DataSaved, ListenURLEvents, MinMax, RawData, TopicRefInternal, DTPSR,
};

#[derive(Debug)]
pub struct ObjectQueue {
    pub stored: Vec<usize>,
    pub saved: HashMap<usize, DataSaved>,

    pub tx: broadcast::Sender<usize>,
    pub seq: usize,
    pub tr: TopicRefInternal,
    pub max_history: Option<usize>,

    pub tx_notification: broadcast::Sender<ListenURLEvents>,
}

#[derive(Debug, Clone, PartialEq)]
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
        let now = time_nanos_i64();

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
            clocks,
            digest,
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
            self.tx_notification
                .send(ListenURLEvents::InsertNotification(notification))
                .unwrap(); // can only fail if there are no receivers
        }
        Ok((data, saved_data, dropped))
    }

    pub fn subscribe_insert_notification(&self) -> broadcast::Receiver<ListenURLEvents> {
        self.tx_notification.subscribe()
    }

    fn current_clocks(&self, now: i64) -> Clocks {
        let mut clocks = Clocks::default();
        let this_seq = self.seq;

        let my_id = self.tr.unique_id.clone();
        if this_seq > 0 {
            let based_on = this_seq - 1;
            clocks.logical.insert(my_id.clone(), MinMax::new(based_on, based_on));
        }
        clocks.wall.insert(my_id, MinMax::new(now, now));
        clocks
    }
}
