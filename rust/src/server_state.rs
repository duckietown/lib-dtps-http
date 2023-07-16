use std::collections::HashMap;
use std::future::Future;

use anyhow::Context;
use bytes::Bytes;
use chrono::Local;
use futures::StreamExt;
use log::{debug, error, info, warn};
use maplit::hashmap;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use crate::constants::*;
use crate::object_queues::*;
use crate::signals_logic::TopicProperties;
use crate::structures::*;
use crate::types::*;
use crate::{
    get_events_stream_inline, get_index, get_metadata, get_queue_id, get_random_node_id, get_stats,
    DTPSError, ServerStateAccess, UrlResult, DTPSR, MASK_ORIGIN,
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

    pub mounted_at: TopicName,
    // pub handle: JoinHandle<DTPSR<()>>,
}

#[derive(Debug)]
pub struct ProxiedTopicInfo {
    pub tr_original: TopicRefInternal,
    // pub tr: TopicRefInternal,
    pub from_subscription: String,
    pub its_topic_name: TopicName,
    // pub handle: JoinHandle<DTPSR<()>>,
    pub data_url: TypeOfConnection,

    pub reachability_we_used: TopicReachabilityInternal,
    // pub forwarding_steps: Vec<ForwardingStep>,
    pub link_benchmark_last: LinkBenchmark,
    // pub link_benchmark_total: LinkBenchmark,
}

#[derive(Debug)]
pub struct ServerState {
    pub node_started: i64,
    pub node_app_data: HashMap<String, NodeAppData>,
    pub node_id: String,

    /// Our queues
    pub oqs: HashMap<TopicName, ObjectQueue>,
    /// The topics that we proxy
    pub proxied_topics: HashMap<TopicName, ProxiedTopicInfo>,

    pub proxied: HashMap<String, ForwardInfo>,
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

        let mut oqs: HashMap<TopicName, ObjectQueue> = HashMap::new();

        // let link_benchmark = LinkBenchmark::identity();
        let now = Local::now().timestamp_nanos();
        let app_data = node_app_data.clone();
        let tr = TopicRefInternal {
            unique_id: node_id.clone(),
            origin_node: node_id.clone(),
            app_data,
            created: now,
            reachability: vec![
                //     TopicReachabilityInternal {
                //     con: Same(),
                //     answering: node_id.clone(),
                //     forwarders: vec![],
                //     benchmark: link_benchmark,
                // }
            ],
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
        oqs.insert(TopicName::root(), ObjectQueue::new(tr));

        let node_started = Local::now().timestamp_nanos();
        let mut ss = ServerState {
            node_id,
            node_started,
            node_app_data,
            oqs,
            proxied_topics: HashMap::new(),
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

        ss.new_topic(
            &TopicName::from_dotted(TOPIC_LIST_NAME),
            None,
            "application/json",
            &p,
        )?;
        ss.new_topic(
            &TopicName::from_dotted(TOPIC_LIST_CLOCK),
            None,
            "text/plain",
            &p,
        )?;
        ss.new_topic(
            &TopicName::from_dotted(TOPIC_LIST_AVAILABILITY),
            None,
            "application/json",
            &p,
        )?;
        ss.new_topic(
            &TopicName::from_dotted(TOPIC_LOGS),
            None,
            "application/yaml",
            &p,
        )?;
        Ok(ss)
    }

    pub fn add_advertise_url(&mut self, url: &str) -> DTPSR<()> {
        if self.advertise_urls.contains(&url.to_string()) {
            return Ok(());
        }
        self.advertise_urls.push(url.to_string());
        self.publish_object_as_json(
            &TopicName::from_dotted(TOPIC_LIST_AVAILABILITY),
            &self.advertise_urls.clone(),
            None,
        )?;
        Ok(())
    }
    pub fn get_advertise_urls(&self) -> Vec<String> {
        self.advertise_urls.clone()
    }
    pub fn log_message(&mut self, msg: String, level: &str) {
        let log_entry = LogEntry {
            level: level.to_string(),
            msg,
        };
        let x = self.publish_object_as_json(&TopicName::from_dotted(TOPIC_LOGS), &log_entry, None);
        if let Err(e) = x {
            error!("Error publishing log message: {:?}", e);
        }
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

    pub fn new_proxy_topic(
        &mut self,
        from_subscription: String,
        its_topic_name: &TopicName,
        topic_name: &TopicName,
        tr_original: &TopicRefInternal,
        reachability_we_used: TopicReachabilityInternal,
        // forwarding_steps: Vec<ForwardingStep>,
        link_benchmark_last: LinkBenchmark,
        // link_benchmark_total: LinkBenchmark,
        // data_url: TypeOfConnection,
    ) -> DTPSR<()> {
        if self.proxied_topics.contains_key(topic_name) {
            return Err(DTPSError::TopicAlreadyExists(topic_name.to_dotted()));
        }

        // // TODO: need to add forwarders info
        // let mut reachability = vec![];
        // for r in &tr_original.reachability {
        //     reachability.push(r.clone());
        // }
        // let tr = TopicRefInternal {
        //     unique_id: tr_original.unique_id.clone(),
        //     origin_node: tr_original.origin_node.clone(),
        //     app_data: tr_original.app_data.clone(),
        //     created: tr_original.created,
        //     reachability: reachability,
        //     properties: tr_original.properties.clone(),
        //     accept_content_type: tr_original.accept_content_type.clone(),
        //     produces_content_type: tr_original.produces_content_type.clone(),
        //     examples: tr_original.examples.clone(),
        // };
        let data_url = reachability_we_used.con.clone();
        self.proxied_topics.insert(
            topic_name.clone(),
            ProxiedTopicInfo {
                tr_original: tr_original.clone(),
                // tr,
                from_subscription: from_subscription.clone(),
                its_topic_name: its_topic_name.clone(),

                link_benchmark_last,
                data_url,
                reachability_we_used,
            },
        );
        info!("New proxy topic {:?} -> {:?}", topic_name, its_topic_name);
        self.update_my_topic()
    }
    pub fn has_topic(&self, topic_name: &TopicName) -> bool {
        self.oqs.contains_key(topic_name) || self.proxied_topics.contains_key(topic_name)
    }
    pub fn new_topic(
        &mut self,
        topic_name: &TopicName,
        app_data: Option<HashMap<String, NodeAppData>>,
        content_type: &str,
        properties: &TopicProperties,
    ) -> DTPSR<()> {
        if self.oqs.contains_key(topic_name) {
            return Err(DTPSError::TopicAlreadyExists(topic_name.to_dotted()));
        }
        // let topic_name = topic_name.to_string();
        let uuid = get_queue_id(&self.node_id, &topic_name);
        // let uuid = Uuid::new_v4();
        let app_data = app_data.unwrap_or_else(HashMap::new);

        // let link_benchmark = LinkBenchmark:: identity();
        let origin_node = self.node_id.clone();
        let now = Local::now().timestamp_nanos();
        let tr = TopicRefInternal {
            unique_id: uuid.to_string(),
            origin_node,
            app_data,
            created: now,
            reachability: vec![
                // TopicReachabilityInternal {
                // con: Same(),
                // // con: Relative(format!("{}/", topic_name), None),
                // answering: self.node_id.clone(),
                // forwarders: vec![],
                // benchmark: link_benchmark,
                // }
            ],
            properties: properties.clone(),
            accept_content_type: vec![content_type.to_string()],
            produces_content_type: vec![content_type.to_string()],
            examples: vec![],
        };
        let oqs = &mut self.oqs;

        oqs.insert(topic_name.clone(), ObjectQueue::new(tr));
        info!("New topic: {:?}", topic_name);

        self.update_my_topic()
    }
    fn update_my_topic(&mut self) -> DTPSR<()> {
        let index_internal = self.create_topic_index();
        let index = index_internal.to_wire(None);
        self.publish_object_as_cbor(&TopicName::root(), &index, None)?;

        let mut topics: Vec<String> = Vec::new();
        let oqs = &mut self.oqs;

        for topic_name in oqs.keys() {
            topics.push(topic_name.to_dotted());
        }
        self.publish_object_as_json(
            &TopicName::from_dotted(TOPIC_LIST_NAME),
            &topics.clone(),
            None,
        )?;

        Ok(())
    }
    // pub fn make_sure_topic_exists(&mut self, topic_name: &str,
    // p: &TopicProperties) {
    //     if !self.oqs.contains_key(topic_name) {
    //         info!("Queue {:?} does not exist, creating it.", topic_name);
    //         return self.new_topic(topic_name, None, p);
    //     }
    // }

    pub fn publish<B: AsRef<[u8]>, C: AsRef<str>>(
        &mut self,
        topic_name: &TopicName,
        content: B,
        content_type: C,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let content_type = content_type.as_ref();
        let content = content.as_ref();
        // self.make_sure_topic_exists(topic_name, &p);
        let v = content.to_vec();
        let data = RawData {
            content: Bytes::from(v),
            content_type: content_type.to_string(),
        };
        self.save_blob(&data.digest(), &data.content);
        if !self.oqs.contains_key(&topic_name) {
            return Err(DTPSError::TopicNotFound(topic_name.to_dotted()));
        }
        let oq = self.oqs.get_mut(&topic_name).unwrap();

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
        topic_name: &TopicName,
        object: &T,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let data_json = serde_json::to_string(object).unwrap();
        return self.publish_json(topic_name, data_json.as_str(), clocks);
    }

    pub fn publish_object_as_cbor<T: Serialize>(
        &mut self,
        topic_name: &TopicName,
        object: &T,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let data_cbor = serde_cbor::to_vec(object).unwrap();
        return self.publish_cbor(topic_name, &data_cbor, clocks);
    }
    pub fn publish_cbor(
        &mut self,
        topic_name: &TopicName,
        content: &[u8],
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        self.publish(topic_name, content, "application/cbor", clocks)
    }
    pub fn publish_json(
        &mut self,
        topic_name: &TopicName,
        json_content: &str,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let bytesdata = json_content.as_bytes().to_vec();
        self.publish(topic_name, &bytesdata, "application/json", clocks)
    }
    pub fn publish_yaml(
        &mut self,
        topic_name: &TopicName,
        yaml_content: &str,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let bytesdata = yaml_content.as_bytes().to_vec();
        self.publish(topic_name, &bytesdata, "application/yaml", clocks)
    }
    pub fn publish_plain(
        &mut self,
        topic_name: &TopicName,
        text_content: &str,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let bytesdata = text_content.as_bytes().to_vec();
        self.publish(topic_name, &bytesdata, "text/plain", clocks)
    }

    pub fn create_topic_index(&self) -> TopicsIndexInternal {
        let mut topics: HashMap<TopicName, TopicRefInternal> = hashmap! {};

        for (topic_name, oq) in self.oqs.iter() {
            // debug!("topics_index: topic_name: {:#?}", topic_name);
            // let url = topic_name.as_relative_url();
            let mut tr = oq.tr.clone();

            tr.reachability.push(TopicReachabilityInternal {
                con: TypeOfConnection::Relative(topic_name.as_relative_url(), None),
                answering: self.node_id.clone(),
                forwarders: vec![],
                benchmark: LinkBenchmark::identity(),
            });

            // let tr = oq.tr.add_path(&url);
            topics.insert(topic_name.clone(), tr);
        }
        for (topic_name, oq) in self.proxied_topics.iter() {
            // debug!("topics_index: topic_name: {:#?}", topic_name);
            // let url = topic_name.as_relative_url();
            // let tr = oq.tr.add_path(&url);

            let mut tr = oq.tr_original.clone();

            let mut forwarders = oq.reachability_we_used.forwarders.clone();

            forwarders.push(ForwardingStep {
                forwarding_node: self.node_id.clone(),
                forwarding_node_connects_to: oq.data_url.to_string(),

                performance: oq.link_benchmark_last.clone(),
            });

            let total = oq.reachability_we_used.benchmark.clone() + oq.link_benchmark_last.clone();

            if MASK_ORIGIN {
                tr.reachability.clear();
            }

            tr.reachability.push(TopicReachabilityInternal {
                con: TypeOfConnection::Relative(topic_name.as_relative_url(), None),
                answering: self.node_id.clone(),
                forwarders,
                benchmark: total,
            });

            topics.insert(topic_name.clone(), tr);
        }
        // let mut node_app_data = hashmap! {};
        // "here".to_string() => NodeAppData::from("2")};
        //
        // node_app_data.insert("@create_topic_index1".to_string(),
        //                      NodeAppData::from(format!("{:#?}", l)));
        // node_app_data.insert("@create_topic_index2".to_string(),
        //                      NodeAppData::from(format!("{:#?}", self.proxied)));

        let mut node_app_data = self.node_app_data.clone();
        // node_app_data.insert(
        //     "@ServerStatE::create_topic_index".to_string(),
        //     NodeAppData::from(format!("{:#?}", self.proxied_topics)),
        // );
        let topics_index = TopicsIndexInternal {
            node_id: self.node_id.clone(),
            node_started: self.node_started,
            node_app_data,
            topics,
        };

        // debug!("topics_index:\n{:#?}", topics_index);

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

pub async fn show_errors<X, F: Future<Output = DTPSR<X>>>(future: F) {
    match future.await {
        Ok(_) => {}
        Err(e) => {
            error!("{}", e);
            // error!("Error: {:#?}", e.backtrace());
        }
    }
}

pub async fn observe_proxy(
    subcription_name: String,
    mounted_at: TopicName,
    url: TypeOfConnection,
    ss_mutex: ServerStateAccess,
) -> DTPSR<()> {
    let (md, index_internal_at_t0) = loop {
        match get_proxy_info(&url).await {
            Ok(s) => break s,
            Err(e) => {
                error!(
                    "observe_proxy: error getting proxy info for proxied {:?} at {}: \n {}",
                    subcription_name, url, e
                );
                info!("observe_proxy: retrying in 2 seconds");
                sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        }
    };

    let who_answers = md.clone().answering.unwrap().clone();

    let stats = get_stats(&url, md.clone().answering.unwrap().clone().as_ref()).await;
    let link1 = match stats {
        UrlResult::Inaccessible(_) => {
            /// TODO
            LinkBenchmark::identity() // ok
        }
        UrlResult::WrongNodeAnswering => {
            /// TODO
            LinkBenchmark::identity() // ok
        }
        UrlResult::Accessible(l) => l,
    };

    {
        let mut ss = ss_mutex.lock().await;
        ss.proxied.insert(
            subcription_name.clone(),
            ForwardInfo {
                mounted_at: mounted_at.clone(),
                url: url.clone(),
                md: md.clone(),
                index_internal: index_internal_at_t0.clone(),
            },
        );
    }

    // let (tx, rx) = mpsc::unbounded_channel();
    let inline_url = md.events_data_inline_url.unwrap().clone();
    //
    let (handle, mut stream) = get_events_stream_inline(inline_url).await;

    {
        let mut ss = ss_mutex.lock().await;
        add_from_response(
            &mut ss,
            who_answers.clone(),
            &subcription_name,
            &mounted_at,
            &index_internal_at_t0,
            link1.clone(),
            url.to_string(),
        )?;
    }
    while let Some(notification) = stream.next().await {
        // debug!("observe_proxy: got notification: {:#?}", notification);
        // add_from_response(&mut ss, &subcription_name,
        //                 &  mounted_at,&index_internal,);
        //
        let x0: TopicsIndexWire = serde_cbor::from_slice(&notification.rd.content).unwrap();
        let ti = TopicsIndexInternal::from_wire(x0, &url);

        {
            let mut ss = ss_mutex.lock().await;
            add_from_response(
                &mut ss,
                who_answers.clone(),
                &subcription_name,
                &mounted_at,
                &ti,
                link1.clone(),
                url.to_string(),
            )?;
        }
    }

    // debug!("observe_proxy: done, now:\n{:#?}", ss.proxied);
    // ss.update_my_topic()?;

    Ok(())
}

pub fn add_from_response(
    s: &mut ServerState,
    who_answers: String,
    subscription: &String,
    mounted_at: &TopicName,
    tii: &TopicsIndexInternal,
    link_benchmark1: LinkBenchmark,
    connected_url: String,
) -> DTPSR<()> {
    // let mut l = hashmap! {};
    debug!("topics_index: tii: \n{:#?}", tii);
    for (its_topic_name, tr) in tii.topics.iter() {
        if its_topic_name.is_root() {
            continue; // TODO
        }

        let available_as = mounted_at + its_topic_name;

        if s.has_topic(&available_as) {
            continue;
        }

        if tr.reachability.len() == 0 {
            return DTPSError::not_reachable(format!(
                "topic {:?} of subscription {:?} is not reachable:\n{:#?}",
                its_topic_name, subscription, tr
            ));
        }
        let reachability_we_used = tr.reachability.get(0).unwrap();
        // let mut forwarders = we_use.forwarders.clone();
        //
        // forwarders.push(ForwardingStep {
        //     forwarding_node: s.node_id.clone(),
        //     forwarding_node_connects_to: connected_url.clone(),
        //
        //     performance: link_benchmark1.clone(),
        // });
        // let link_benchmark_total = we_use.benchmark.clone() + link_benchmark1.clone();

        // let we_use_url = tr.reachability.get(0).unwrap().con.clone();
        s.new_proxy_topic(
            subscription.clone(),
            &its_topic_name,
            &available_as,
            tr,
            reachability_we_used.clone(),
            link_benchmark1.clone(),
        )?;
    }
    Ok(())
    //         let prefix = divide_in_components(&proxied, '.');
    //
    //         let url = get_url_from_topic_name(proxied);
    //         let tr = fi.index_internal.add_path(url);
    //         l.insert(
    //             proxied.clone(),
    //             tr.topics.keys().cloned().collect::<Vec<_>>(),
    //         );
    //         for (a, b) in tr.topics {
    //             let its = divide_in_components(&a, '.');
    //             let full: Vec<String> = vec_concat(&prefix, &its);
    //
    //             let rurl = make_rel_url(&prefix);
    //             let b2 = b.add_path(&rurl);
    //
    //             let dotsep = full.join(".");
    //             topics.insert(dotsep, b2);
    //         }
    //     }
}
