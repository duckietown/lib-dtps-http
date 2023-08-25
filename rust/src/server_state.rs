use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::path::{Path, PathBuf};

use anyhow::Context;
use bytes::Bytes;
use chrono::Local;
use futures::StreamExt;
use indent::indent_all_with;
use log::{debug, error, info, warn};
use maplit::hashmap;
use path_clean::PathClean;
use schemars::schema::RootSchema;
use schemars::{schema_for, JsonSchema};
use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString, ToString};
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::constants::*;
use crate::object_queues::*;
use crate::signals_logic::TopicProperties;
use crate::structures::*;
use crate::structures_linkproperties::LinkBenchmark;
use crate::types::*;
use crate::{
    context, error_with_info, get_events_stream_inline, get_index, get_metadata, get_queue_id,
    get_random_node_id, get_stats, invalid_input, not_available, not_implemented, not_reachable,
    sniff_type_resource, DTPSError, ServerStateAccess, TypeOfResource, UrlResult, DTPSR,
    MASK_ORIGIN,
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
}

#[derive(Debug)]
pub struct ProxiedTopicInfo {
    pub tr_original: TopicRefInternal,
    pub from_subscription: String,
    pub its_topic_name: TopicName,
    pub data_url: TypeOfConnection,

    pub reachability_we_used: TopicReachabilityInternal,
    pub link_benchmark_last: LinkBenchmark,
}

#[derive(Debug, Clone)]
pub struct OtherProxyInfo {
    pub con: TypeOfConnection,
}

#[derive(Debug, Clone)]
pub struct LocalDirInfo {
    pub unique_id: String,
    pub local_dir: String,
    pub from_subscription: String,
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

    /// Filesystem match
    pub local_dirs: HashMap<TopicName, LocalDirInfo>,

    /// The subscriptions to other nodes
    pub proxied: HashMap<String, ForwardInfo>,

    /// The proxied other resources
    pub proxied_other: HashMap<TopicName, OtherProxyInfo>,
    pub blobs: HashMap<String, Vec<u8>>,

    advertise_urls: Vec<String>,

    status_tx: mpsc::UnboundedSender<ComponentStatusNotification>,
    status_rx: mpsc::UnboundedReceiver<ComponentStatusNotification>,

    pub aliases: HashMap<TopicName, TopicName>,
}

/// Taken from this: http://supervisord.org/subprocess.html
#[derive(EnumString, Serialize, Deserialize, Display, Debug, Clone, JsonSchema)]
pub enum Status {
    /// The process has been stopped due to a stop request or has never been started.
    STOPPED,
    /// The process is starting due to a start request.
    STARTING,
    /// The process is running.
    RUNNING,
    /// The process entered the STARTING state but subsequently exited too quickly (before the time defined in startsecs) to move to the RUNNING state.
    BACKOFF,
    /// The process is stopping due to a stop request.
    STOPPING,
    /// The process exited from the RUNNING state (expectedly or unexpectedly).
    EXITED,
    /// The process could not be started successfully.
    FATAL,
    /// The process is in an unknown state (programming error).
    UNKNOWN,
}

//
//  pub fn compose(a: Status, b: Status) -> Status {
//      match a {
//          Status::STOPPED => {}
//          Status::STARTING => {}
//          Status::RUNNING => {}
//          Status::BACKOFF => {}
//          Status::STOPPING => {}
//          Status::EXITED => {}
//          Status::FATAL => {}
//          Status::UNKNOWN => {}
//      }
//
//
//      // match (a, b) {
//      //     (Status::RUNNING(), Status::RUNNING()) => Status::RUNNING(),
//      // }
// }
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ComponentStatusNotification {
    pub component: TopicName,
    pub status: Status,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StatusSummary {
    pub components: HashMap<TopicName, Status>,
    pub comments: HashMap<TopicName, String>,
}

impl StatusSummary {
    pub fn incorporate(&mut self, csn: ComponentStatusNotification) {
        self.components.insert(csn.component.clone(), csn.status);
        if let Some(x) = csn.comment {
            self.comments.insert(csn.component.clone(), x);
        }
    }
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
            reachability: vec![],
            properties: TopicProperties {
                streamable: true,
                pushable: false,
                readable: true,
                immutable: false,
            },
            content_info: ContentInfo {
                accept_content_type: vec![],
                produces_content_type: vec![CONTENT_TYPE_DTPS_INDEX_CBOR.to_string()],
                storage_content_type: vec![],
                jschema: Some(schema_for!(TopicsIndexWire)),
                examples: vec![],
            },
        };
        oqs.insert(TopicName::root(), ObjectQueue::new(tr));

        let node_started = Local::now().timestamp_nanos();

        let (status_tx, status_rx) = mpsc::unbounded_channel::<ComponentStatusNotification>();

        let mut ss = ServerState {
            node_id,
            node_started,
            node_app_data,
            oqs,
            proxied_other: HashMap::new(),
            proxied_topics: HashMap::new(),
            blobs: HashMap::new(),
            proxied: HashMap::new(),
            advertise_urls: vec![],
            local_dirs: HashMap::new(),
            status_tx,
            status_rx,
            aliases: HashMap::new(),
        };
        let p = TopicProperties {
            streamable: true,
            pushable: false,
            readable: true,
            immutable: false,
        };

        ss.new_topic(
            &TopicName::from_relative_url(TOPIC_LIST_NAME)?,
            None,
            "application/json",
            &p,
            None,
        )?;
        ss.new_topic(
            &TopicName::from_relative_url(TOPIC_LIST_CLOCK)?,
            None,
            "application/json",
            &p,
            Some(schema_for!(i64)),
        )?;
        ss.new_topic(
            &TopicName::from_relative_url(TOPIC_LIST_AVAILABILITY)?,
            None,
            "application/json",
            &p,
            None,
        )?;
        ss.new_topic(
            &TopicName::from_relative_url(TOPIC_LOGS)?,
            None,
            "application/yaml",
            &p,
            None,
        )?;

        ss.new_topic(
            &TopicName::from_relative_url(TOPIC_STATE_NOTIFICATION)?,
            None,
            "application/yaml",
            &p,
            Some(schema_for!(ComponentStatusNotification)),
        )?;

        ss.new_topic(
            &TopicName::from_relative_url(TOPIC_STATE_SUMMARY)?,
            None,
            "application/yaml",
            &p,
            None,
        )?;

        Ok(ss)
    }
    pub fn add_alias(&mut self, new: &TopicName, existing: &TopicName) {
        // TODO: check for errors
        self.aliases.insert(new.clone(), existing.clone());
    }
    pub fn add_advertise_url(&mut self, url: &str) -> DTPSR<()> {
        if self.advertise_urls.contains(&url.to_string()) {
            return Ok(());
        }
        self.advertise_urls.push(url.to_string());
        self.publish_object_as_json(
            &TopicName::from_relative_url(TOPIC_LIST_AVAILABILITY)?,
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
        let x = self.publish_object_as_json(
            &TopicName::from_relative_url(TOPIC_LOGS).unwrap(),
            &log_entry,
            None,
        );
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
        link_benchmark_last: LinkBenchmark,
    ) -> DTPSR<()> {
        if self.proxied_topics.contains_key(topic_name) {
            return Err(DTPSError::TopicAlreadyExists(topic_name.to_relative_url()));
        }

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

    pub fn new_filesystem_mount(
        &mut self,
        from_subscription: &String,
        topic_name: &TopicName,
        fp: FilePaths,
    ) -> DTPSR<()> {
        if self.local_dirs.contains_key(&topic_name) {
            return Err(DTPSError::TopicAlreadyExists(topic_name.to_relative_url()));
        }

        let local_dir = match fp {
            FilePaths::Absolute(s) => PathBuf::from(s),
            FilePaths::Relative(s) => {
                absolute_path(s).map_err(|e| DTPSError::Other(e.to_string()))?
            }
        };

        // let local_dir = PathBuf::from(path);
        info!(
            "New local folder {:?} -> {:?}",
            topic_name,
            local_dir.to_str()
        );
        let uuid = get_queue_id(&self.node_id, &topic_name);
        self.local_dirs.insert(
            topic_name.clone(),
            LocalDirInfo {
                unique_id: uuid,
                local_dir: local_dir.as_path().to_str().unwrap().to_string(),
                from_subscription: from_subscription.clone(),
            },
        );
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
        schema: Option<RootSchema>,
    ) -> DTPSR<()> {
        if self.oqs.contains_key(topic_name) {
            return Err(DTPSError::TopicAlreadyExists(topic_name.to_relative_url()));
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
            content_info: ContentInfo {
                accept_content_type: vec![content_type.to_string()],
                produces_content_type: vec![content_type.to_string()],
                storage_content_type: vec![content_type.to_string()],
                jschema: schema,
                examples: vec![],
            },
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
            topics.push(topic_name.to_relative_url());
        }
        self.publish_object_as_json(
            &TopicName::from_relative_url(TOPIC_LIST_NAME)?,
            &topics.clone(),
            None,
        )?;

        Ok(())
    }

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
            return Err(DTPSError::TopicNotFound(topic_name.to_relative_url()));
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

    pub fn publish_object_as_yaml<T: Serialize>(
        &mut self,
        topic_name: &TopicName,
        object: &T,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let data_yaml = serde_yaml::to_string(object).unwrap();
        return self.publish_yaml(topic_name, data_yaml.as_str(), clocks);
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
            let mut tr = oq.tr.clone();

            tr.reachability.push(TopicReachabilityInternal {
                con: TypeOfConnection::Relative(topic_name.to_relative_url(), None),
                answering: self.node_id.clone(),
                forwarders: vec![],
                benchmark: LinkBenchmark::identity(),
            });

            // let tr = oq.tr.add_path(&url);
            topics.insert(topic_name.clone(), tr);
        }
        for (topic_name, oq) in self.proxied_topics.iter() {
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
                con: TypeOfConnection::Relative(topic_name.to_relative_url(), None),
                answering: self.node_id.clone(),
                forwarders,
                benchmark: total,
            });

            topics.insert(topic_name.clone(), tr);
        }

        for (topic_name, pinfo) in self.proxied_other.iter() {
            let mut forwarders = vec![];

            forwarders.push(ForwardingStep {
                forwarding_node: self.node_id.clone(),
                forwarding_node_connects_to: pinfo.con.to_string(),

                performance: LinkBenchmark::identity(), // TODO:
            });

            let prop = TopicProperties {
                streamable: false,
                pushable: false,
                readable: true,
                immutable: false,
            };

            let mut tr = TopicRefInternal {
                unique_id: pinfo.con.to_string(),
                origin_node: pinfo.con.to_string(),
                app_data: Default::default(),
                reachability: vec![],
                created: 0,
                properties: prop,
                content_info: ContentInfo {
                    accept_content_type: vec![],
                    produces_content_type: vec![],
                    storage_content_type: vec![],
                    examples: vec![],
                    jschema: None,
                },
            };

            tr.reachability.push(TopicReachabilityInternal {
                con: TypeOfConnection::Relative(topic_name.to_relative_url(), None),
                answering: self.node_id.clone(),
                forwarders,
                benchmark: LinkBenchmark::identity(),
            });

            topics.insert(topic_name.clone(), tr);
        }

        for (topic_name, pinfo) in self.local_dirs.iter() {
            let prop = TopicProperties {
                streamable: false,
                pushable: false,
                readable: true,
                immutable: true,
            };

            let app_data = hashmap! {
                "path".to_string() => pinfo.local_dir.clone(),
            };
            let mut tr = TopicRefInternal {
                unique_id: pinfo.unique_id.clone(),
                origin_node: self.node_id.clone(),
                app_data,
                reachability: vec![],
                created: 0,
                properties: prop,
                content_info: ContentInfo {
                    accept_content_type: vec![],
                    produces_content_type: vec![],
                    storage_content_type: vec![],
                    examples: vec![],
                    jschema: None,
                },
            };

            tr.reachability.push(TopicReachabilityInternal {
                con: TypeOfConnection::Relative(topic_name.to_relative_url(), None),
                answering: self.node_id.clone(),
                forwarders: vec![],
                benchmark: LinkBenchmark::identity(),
            });

            topics.insert(topic_name.clone(), tr);
        }
        for (alias, original) in &self.aliases {
            let tr = TopicRefInternal {
                unique_id: "".to_string(),
                origin_node: "".to_string(),
                app_data: Default::default(),
                reachability: vec![],
                created: 0,
                properties: TopicProperties {
                    streamable: false,
                    pushable: false,
                    readable: false,
                    immutable: false,
                },
                content_info: ContentInfo {
                    accept_content_type: vec![],
                    storage_content_type: vec![],
                    produces_content_type: vec![],
                    jschema: None,
                    examples: vec![],
                },
            };
            topics.insert(alias.clone(), tr);
        }

        TopicsIndexInternal { topics }
    }

    pub fn subscribe_insert_notification(&self, tn: &TopicName) -> Receiver<InsertNotification> {
        let q = self.oqs.get(tn).unwrap();
        q.subscribe_insert_notification()
    }

    pub fn get_last_insert(&self, tn: &TopicName) -> DTPSR<Option<InsertNotification>> {
        let q = self.get_queue(tn)?;
        match q.sequence.last() {
            None => Ok(None),
            Some(data_saved) => {
                let content = self.get_blob_bytes(&data_saved.digest)?;

                Ok(Some(InsertNotification {
                    data_saved: data_saved.clone(),
                    raw_data: RawData {
                        content,
                        content_type: data_saved.content_type.clone(),
                    },
                }))
            }
        }
    }

    pub fn get_queue(&self, tn: &TopicName) -> DTPSR<&ObjectQueue> {
        match self.oqs.get(tn) {
            None => {
                let s = format!("Could not find topic {}", tn.as_dash_sep());
                Err(DTPSError::TopicNotFound(s))
            }
            Some(q) => Ok(q),
        }
    }

    pub fn send_status_notification(
        &mut self,
        component: &TopicName,
        status: Status,
        message: Option<String>,
    ) -> DTPSR<()> {
        let tn = TopicName::from_relative_url(TOPIC_STATE_NOTIFICATION)?;
        let ob = ComponentStatusNotification {
            component: component.clone(),
            status,
            comment: message,
        };
        self.publish_object_as_cbor(&tn, &ob, None)?;
        Ok(())
    }
}

async fn get_proxy_info(url: &TypeOfConnection) -> DTPSR<(FoundMetadata, TopicsIndexInternal)> {
    let md = context!(
        get_metadata(url).await,
        "Error getting metadata for proxied at {}",
        url,
    )?;

    let index_url = match &md.index {
        None => {
            return not_reachable!("Cannot find index url for proxy at {url}:\n{md:?}");
        }
        Some(u) => u.clone(),
    };
    let index_internal = context!(
        get_index(&index_url).await,
        "Error getting the index for {url} index {index_url}"
    )?;
    Ok((md, index_internal))
}

pub async fn show_errors<X, F: Future<Output = DTPSR<X>>>(
    ssa: Option<ServerStateAccess>,
    desc: String,
    future: F,
) {
    let tn = TopicName::from_dash_sep(&desc).unwrap();
    if let Some(ssa) = &ssa {
        let mut ss = ssa.lock().await;
        ss.send_status_notification(&tn, Status::RUNNING, None)
            .unwrap();
    };

    let message = match future.await {
        Ok(_) => None,
        Err(e) => {
            // e.anyhow_kind().print_backtrace();
            // let ef = format!("{}\n---\n{:?}", e, e);
            let ef = format!("{:?}", e);
            error_with_info!("Error in async {desc}:\n{}", indent_all_with("| ", &ef));
            // error!("Error: {:#?}", e.backtrace());
            Some(ef)
        }
    };

    if let Some(ssa) = &ssa {
        let mut ss = ssa.lock().await;
        ss.send_status_notification(&tn, Status::EXITED, message)
            .unwrap();
    }
}

pub async fn observe_proxy(
    subcription_name: String,
    mounted_at: TopicName,
    url: TypeOfConnection,
    ss_mutex: ServerStateAccess,
) -> DTPSR<()> {
    if let TypeOfConnection::File(_, fp) = url {
        let mut ss = ss_mutex.lock().await;
        ss.new_filesystem_mount(&subcription_name, &mounted_at, fp)?;
        return Ok(());
    }

    let rtype = loop {
        match sniff_type_resource(&url).await {
            Ok(s) => break s,
            Err(e) => {
                warn!("Cannot sniff {} \n{:?}", subcription_name, e);
                info!("observe_proxy: retrying in 2 seconds");
                sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        }
    };

    match rtype {
        TypeOfResource::Other => {
            handle_proxy_other(subcription_name, mounted_at, url, ss_mutex).await
        }
        TypeOfResource::DTPSTopic => {
            not_implemented!("observe_proxy: TypeOfResource::DTPSTopic")
        }
        TypeOfResource::DTPSIndex => {
            observe_node_proxy(subcription_name, mounted_at, url, ss_mutex).await
        }
    }
}

pub async fn handle_proxy_other(
    _subcription_name: String,
    mounted_at: TopicName,
    url: TypeOfConnection,
    ss_mutex: ServerStateAccess,
) -> DTPSR<()> {
    let mut ss = ss_mutex.lock().await;
    let info = OtherProxyInfo { con: url.clone() };

    ss.proxied_other.insert(mounted_at, info);

    Ok(())
}

pub async fn observe_node_proxy(
    subcription_name: String,
    mounted_at: TopicName,
    url: TypeOfConnection,
    ss_mutex: ServerStateAccess,
) -> DTPSR<()> {
    let (md, index_internal_at_t0) = loop {
        match get_proxy_info(&url).await {
            Ok(s) => break s,
            Err(e) => {
                warn!(
                    "observe_proxy: error getting proxy info for proxied {:?}:\n{:?}",
                    subcription_name, e
                );
                info!("observe_proxy: retrying in 2 seconds");
                sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        }
    };

    let who_answers = match &md.answering {
        None => {
            return not_available!("Nobody is answering {url:}:\n{md:?}");
        }
        Some(n) => n.clone(),
    };

    {
        let ss = ss_mutex.lock().await;
        if who_answers == ss.node_id {
            return invalid_input!(
                "observe_node_proxy: invalid proxy connection: we find ourselves at the connection {url}"
            );
        }
    }

    let stats = get_stats(&url, who_answers.as_ref()).await;
    let link1 = match stats {
        UrlResult::Inaccessible(_) => {
            // TODO
            LinkBenchmark::identity() // ok
        }
        UrlResult::WrongNodeAnswering => {
            // TODO
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
    let (_handle, mut stream) = get_events_stream_inline(&inline_url).await;

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

    Ok(())
}

pub fn add_from_response(
    s: &mut ServerState,
    _who_answers: String,
    subscription: &String,
    mounted_at: &TopicName,
    tii: &TopicsIndexInternal,
    link_benchmark1: LinkBenchmark,
    _connected_url: String,
) -> DTPSR<()> {
    // debug!("topics_index: tii: \n{:#?}", tii);
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
}

pub fn absolute_path(path: impl AsRef<Path>) -> std::io::Result<PathBuf> {
    let path = path.as_ref();

    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()?.join(path)
    }
    .clean();

    Ok(absolute_path)
}
