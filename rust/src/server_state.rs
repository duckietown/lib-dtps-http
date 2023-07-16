use std::collections::HashMap;

use anyhow::Context;
use bytes::Bytes;
use chrono::{Duration, Local};
use log::{debug, error, info, warn};
use maplit::hashmap;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use crate::constants::*;
use crate::object_queues::*;
use crate::signals_logic::TopicProperties;
use crate::structures::*;
use crate::types::*;
use crate::TypeOfConnection::Same;
use crate::{
    divide_in_components, get_index, get_metadata, get_queue_id, get_random_node_id, vec_concat,
    DTPSError, DTPSR,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub level: String,
    pub msg: String,
}

#[derive(Debug)]
pub struct ForwardInfo {
    pub url: TypeOfConnection,
    pub md: FoundMetadata,
    pub index_internal: TopicsIndexInternal,
}

#[derive(Debug)]
pub struct ServerState {
    pub node_started: i64,
    pub node_app_data: HashMap<String, NodeAppData>,
    pub node_id: String,
    pub oqs: HashMap<TopicName, ObjectQueue>,
    pub proxied: HashMap<TopicName, ForwardInfo>,
    pub blobs: HashMap<String, Vec<u8>>,
    advertise_urls: Vec<String>,
}

impl ServerState {
    pub fn new(node_app_data: Option<HashMap<String, NodeAppData>>) -> DTPSR<Self> {
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
                pushable: false,
                readable: true,
                immutable: false,
            },
            accept_content_type: vec![],
            produces_content_type: vec![CONTENT_TYPE_DTPS_INDEX_CBOR.to_string()],
            examples: vec![],
        };
        oqs.insert("".to_string(), ObjectQueue::new(tr));

        let node_started = Local::now().timestamp_nanos();
        let mut ss = ServerState {
            node_id,
            node_started,
            node_app_data,
            oqs,
            blobs: HashMap::new(),
            proxied: HashMap::new(),
            advertise_urls: vec![],
        };
        let p = TopicProperties {
            streamable: true,
            pushable: false,
            readable: true,
            immutable: false,
        };

        ss.new_topic(TOPIC_LIST_NAME, None, "application/json", &p)?;
        ss.new_topic(TOPIC_LIST_CLOCK, None, "text/plain", &p)?;
        ss.new_topic(TOPIC_LIST_AVAILABILITY, None, "application/json", &p)?;
        ss.new_topic(TOPIC_LOGS, None, "application/yaml", &p)?;
        Ok(ss)
    }

    pub fn add_advertise_url(&mut self, url: &str) -> DTPSR<()> {
        if self.advertise_urls.contains(&url.to_string()) {
            return Ok(());
        }
        self.advertise_urls.push(url.to_string());
        self.publish_object_as_json(TOPIC_LIST_AVAILABILITY, &self.advertise_urls.clone(), None)?;
        Ok(())
    }
    pub fn get_advertise_urls(&self) -> Vec<String> {
        self.advertise_urls.clone()
    }
    pub fn log_message(&mut self, msg: String, level: &str) -> DTPSR<()> {
        let log_entry = LogEntry {
            level: level.to_string(),
            msg,
        };
        self.publish_object_as_json(TOPIC_LOGS, &log_entry, None)?;
        Ok(())
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

    pub fn new_topic(
        &mut self,
        topic_name: &str,
        app_data: Option<HashMap<String, NodeAppData>>,
        content_type: &str,
        properties: &TopicProperties,
    ) -> DTPSR<()> {
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
                con: Same(),
                // con: Relative(format!("{}/", topic_name), None),
                answering: self.node_id.clone(),
                forwarders: vec![],
                benchmark: link_benchmark,
            }],
            properties: properties.clone(),
            accept_content_type: vec![content_type.to_string()],
            produces_content_type: vec![content_type.to_string()],
            examples: vec![],
        };
        let oqs = &mut self.oqs;

        oqs.insert(topic_name.clone(), ObjectQueue::new(tr));
        let mut topics: Vec<String> = Vec::new();

        for topic_name in oqs.keys() {
            topics.push(topic_name.clone());
        }

        self.update_my_topic()?;

        self.publish_object_as_json(TOPIC_LIST_NAME, &topics.clone(), None)?;

        Ok(())
    }
    fn update_my_topic(&mut self) -> DTPSR<()> {
        let index_internal = self.create_topic_index();
        let index = index_internal.to_wire(None);
        self.publish_object_as_cbor("", &index, None)?;
        Ok(())
    }
    // pub fn make_sure_topic_exists(&mut self, topic_name: &str,
    // p: &TopicProperties) {
    //     if !self.oqs.contains_key(topic_name) {
    //         info!("Queue {:?} does not exist, creating it.", topic_name);
    //         return self.new_topic(topic_name, None, p);
    //     }
    // }

    pub fn publish(
        &mut self,
        topic_name: &str,
        content: &[u8],
        content_type: &str,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        // self.make_sure_topic_exists(topic_name, &p);
        let v = content.to_vec();
        let data = RawData {
            content: Bytes::from(v),
            content_type: content_type.to_string(),
        };
        self.save_blob(&data.digest(), &data.content);
        if !self.oqs.contains_key(topic_name) {
            return Err(DTPSError::TopicNotFound(topic_name.to_string()));
        }
        let oq = self.oqs.get_mut(topic_name).unwrap();

        oq.push(&data, clocks)
    }
    pub fn save_blob(&mut self, digest: &str, content: &[u8]) {
        self.blobs.insert(digest.to_string(), content.to_vec());
    }
    pub fn get_blob(&self, digest: &str) -> Option<&Vec<u8>> {
        return self.blobs.get(digest);
    }

    pub fn get_blob_bytes(&self, digest: &str) -> DTPSR<Bytes> {
        let x = self.blobs.get(digest).map(|v| Bytes::from(v.clone()));
        match x {
            Some(v) => Ok(v),
            None => {
                let msg = format!("Blob {:#?} not found", digest);
                Err(DTPSError::InternalInconsistency(msg))
            }
        }
    }
    pub fn publish_object_as_json<T: Serialize>(
        &mut self,
        topic_name: &str,
        object: &T,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let data_json = serde_json::to_string(object).unwrap();
        return self.publish_json(topic_name, data_json.as_str(), clocks);
    }

    pub fn publish_object_as_cbor<T: Serialize>(
        &mut self,
        topic_name: &str,
        object: &T,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let data_cbor = serde_cbor::to_vec(object).unwrap();
        return self.publish_cbor(topic_name, &data_cbor, clocks);
    }
    pub fn publish_cbor(
        &mut self,
        topic_name: &str,
        content: &[u8],
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        self.publish(topic_name, content, "application/cbor", clocks)
    }
    pub fn publish_json(
        &mut self,
        topic_name: &str,
        json_content: &str,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let bytesdata = json_content.as_bytes().to_vec();
        self.publish(topic_name, &bytesdata, "application/json", clocks)
    }
    pub fn publish_yaml(
        &mut self,
        topic_name: &str,
        yaml_content: &str,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let bytesdata = yaml_content.as_bytes().to_vec();
        self.publish(topic_name, &bytesdata, "application/yaml", clocks)
    }
    pub fn publish_plain(
        &mut self,
        topic_name: &str,
        text_content: &str,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let bytesdata = text_content.as_bytes().to_vec();
        self.publish(topic_name, &bytesdata, "text/plain", clocks)
    }

    pub async fn add_proxied(&mut self, proxied_name: String, url: TypeOfConnection) -> DTPSR<()> {
        let (md, index_internal) = loop {
            match get_proxy_info(&url).await {
                Ok(s) => break s,
                Err(e) => {
                    error!(
                        "add_proxied: error getting proxy info for proxied {:?} at {}: \n {}",
                        proxied_name, url, e
                    );
                    info!("add_proxied: retrying in 2 seconds");
                    sleep(std::time::Duration::from_secs(2)).await;
                    continue;
                }
            }
        };
        //
        // let index_internal =
        //     loop {
        //         match (
        //             md = get_metadata(&url).await
        //                 .with_context(|| {
        //                     format!(
        //                         "Error getting metadata for proxied {:?} at {}",
        //                         proxied_name, url
        //                     )
        //                 })?;
        //
        //             debug!("add_proxied: md:\n{:#?}", md);
        //             get_index(&url).await
        //         ) {}
        //
        //         //     .or_else(
        //         //     |e| {
        //         //         error!("add_proxied: error getting index for proxied {:?} at {}: \n {}", proxied_name, url, e);
        //         //         Err(e)
        //         //     }
        //         // )?;
        //
        //         // sleep 5 seconds
        //         tokio::time::sleep(Duration::from_secs(5)).await;
        //     }
        //     ;

        // let md = match get_metadata(&url).await {
        //     Ok(md) => md,
        //     Err(e) => {
        //         return DTPSError::not_reachable(format!(
        //             "Error getting metadata for proxied {:?} at {}: \n {}",
        //             proxied_name, url, e
        //         ));
        //     }
        // };

        self.proxied.insert(
            proxied_name.clone(),
            ForwardInfo {
                url: url.clone(),
                md,
                index_internal,
            },
        );
        debug!("add_proxied: done, now:\n{:#?}", self.proxied);
        self.update_my_topic()?;

        // add_context!(std::fs::read_to_string("my_file.txt"), "Failed to read file");

        Ok(())
    }

    pub fn create_topic_index(&self) -> TopicsIndexInternal {
        let mut topics: HashMap<TopicName, TopicRefInternal> = hashmap! {};

        for (topic_name, oq) in self.oqs.iter() {
            // debug!("topics_index: topic_name: {:#?}", topic_name);
            let url = get_url_from_topic_name(topic_name);
            let tr = oq.tr.add_path(&url);
            topics.insert(topic_name.clone(), tr);
        }
        let mut l = hashmap! {};
        // debug!("topics_index: proxied: {:#?}", self.proxied);
        for (proxied, fi) in self.proxied.iter() {
            let prefix = divide_in_components(&proxied, '.');

            let url = get_url_from_topic_name(proxied);
            let tr = fi.index_internal.add_path(url);
            l.insert(
                proxied.clone(),
                tr.topics.keys().cloned().collect::<Vec<_>>(),
            );
            for (a, b) in tr.topics {
                let its = divide_in_components(&a, '.');
                let full: Vec<String> = vec_concat(&prefix, &its);

                let rurl = make_rel_url(&prefix);
                let b2 = b.add_path(&rurl);

                let dotsep = full.join(".");
                topics.insert(dotsep, b2);
            }
        }
        // let mut node_app_data = hashmap! {};
        // "here".to_string() => NodeAppData::from("2")};
        //
        // node_app_data.insert("@create_topic_index1".to_string(),
        //                      NodeAppData::from(format!("{:#?}", l)));
        // node_app_data.insert("@create_topic_index2".to_string(),
        //                      NodeAppData::from(format!("{:#?}", self.proxied)));
        let topics_index = TopicsIndexInternal {
            node_id: self.node_id.clone(),
            node_started: self.node_started,
            node_app_data: self.node_app_data.clone(),
            topics,
        };

        debug!("topics_index:\n{:#?}", topics_index);

        return topics_index;
    }
}

async fn get_proxy_info(url: &TypeOfConnection) -> DTPSR<(FoundMetadata, TopicsIndexInternal)> {
    let md = get_metadata(url)
        .await
        .with_context(|| format!("Error getting metadata for proxied at {}", url))?;
    let index_internal = get_index(url)
        .await
        .with_context(|| format!("Error getting index for proxied at {}", url))?;
    Ok((md, index_internal))
}
