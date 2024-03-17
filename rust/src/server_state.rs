use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    env,
    fmt::Debug,
    future::Future,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::Context;
use bytes::Bytes;
use futures::StreamExt;
use indent::indent_all_with;
use maplit::hashmap;
use path_clean::PathClean;
use schemars::{schema::RootSchema, schema_for, JsonSchema};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use strum_macros::{Display, EnumString};
use tokio::{
    sync::{
        broadcast::{error::RecvError, Receiver, Receiver as BroadcastReceiver},
        mpsc,
    },
    time::sleep,
};

use crate::client_link_benchmark::{compute_best_alternative, get_stats, incompatible, UrlResult};
use crate::get_events_stream_inline;
use crate::get_index;
use crate::time_nanos_i64;
use crate::wrap_recv;
use crate::TypeOfResource;
use crate::{
    context, debug_with_info, dtpserror_context, dtpserror_other, error_with_info, get_queue_id, get_random_node_id,
    info_with_info, internal_assertion,
    internal_jobs::{InternalJobManager, JobFunctionType},
    invalid_input, is_prefix_of, not_available, not_implemented, not_reachable,
    shared_statuses::SharedStatusNotification,
    signals_logic::{GetStream, Pushable},
    signals_logic_resolve::interpret_path,
    types::CompositeName,
    warn_with_info, Clocks, ContentInfo, DTPSError, DataSaved, FilePaths, ForwardingStep, FoundMetadata,
    InsertNotification, LinkBenchmark, ListenURLEvents, NodeAppData, ObjectQueue, RawData, ServerStateAccess,
    TopicName, TopicProperties, TopicReachabilityInternal, TopicRefInternal, TopicsIndexInternal, TopicsIndexWire,
    TypeOfConnection, CONTENT_TYPE_CBOR, CONTENT_TYPE_DTPS_INDEX_CBOR, CONTENT_TYPE_JSON, CONTENT_TYPE_PLAIN,
    CONTENT_TYPE_YAML, DTPSR, MASK_ORIGIN, TOPIC_CONNECTIONS, TOPIC_LIST_AVAILABILITY, TOPIC_LIST_CLOCK,
    TOPIC_LIST_NAME, TOPIC_LOGS, TOPIC_PROXIED, TOPIC_STATE_NOTIFICATION, TOPIC_STATE_SUMMARY,
};
use crate::{get_metadata, sniff_type_resource};

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct ProxyJob {
    pub node_id: Option<String>,
    pub urls: Vec<String>,
    pub mask_origin: bool, // TODO: mask_origin not supported in Rust
}

type Proxied = HashMap<String, ProxyJob>;

#[derive(Debug, Clone, PartialEq)]
pub enum ServiceMode {
    /// Ignore messages that were produced while we were disconnected
    BestEffort,
    /// Make sure to send all messages by asking history if some was missing
    AllMessages,
    /// At the beginning, get all the history as well (if history supported)
    AllMessagesSinceStart,
}

impl FromStr for ServiceMode {
    type Err = DTPSError;

    fn from_str(s: &str) -> DTPSR<Self> {
        match s {
            "BestEffort" => Ok(Self::BestEffort),
            "AllMessages" => Ok(Self::AllMessages),
            "AllMessagesSinceStart" => Ok(Self::AllMessagesSinceStart),
            _ => DTPSError::other(format!("Unknown service mode: {:?}", s)),
        }
    }
}

impl ServiceMode {
    fn as_str(&self) -> &'static str {
        match self {
            Self::BestEffort => "BestEffort",
            Self::AllMessages => "AllMessages",
            Self::AllMessagesSinceStart => "AllMessagesSinceStart",
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct ConnectionJobWire {
    /// Source topic (dash separated)
    pub source: String,
    /// Target topic (dash separated)
    pub target: String,
    pub service_mode: String,
}

#[derive(Debug, Clone)]
pub struct ConnectionJob {
    pub source: TopicName,
    pub target: TopicName,
    pub service_mode: ServiceMode,
}

// type Connections = HashMap<CompositeName, ConnectionJob>;
type ConnectionsWire = HashMap<String, ConnectionJobWire>;

impl ConnectionJob {
    pub fn from_wire(wire: &ConnectionJobWire) -> DTPSR<Self> {
        let source = TopicName::from_dash_sep(&wire.source)?;
        let target = TopicName::from_dash_sep(&wire.target)?;
        let service_mode = ServiceMode::from_str(&wire.service_mode)?;
        Ok(Self {
            source,
            target,
            service_mode,
        })
    }

    pub fn to_wire(&self) -> ConnectionJobWire {
        ConnectionJobWire {
            source: self.source.to_dash_sep(),
            target: self.target.to_dash_sep(),
            service_mode: self.service_mode.as_str().to_string(),
        }
    }

    /// Parses a string of the form `a/b -> c/d`
    pub fn from_string(s: &str) -> DTPSR<Self> {
        match s.find("->") {
            None => {
                invalid_input!("Cannot find \"->\" in string {s:?}")
            }
            Some(i) => {
                // divide in 2
                let before = s[..i].trim();
                let after = s[i + "->".len()..].trim();
                let source = TopicName::from_dash_sep(before)?;
                let target = TopicName::from_dash_sep(after)?;
                let service_mode = ServiceMode::BestEffort;
                Ok(Self {
                    source,
                    target,
                    service_mode,
                })
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub level: String,
    pub msg: String,
}

#[derive(Debug)]
pub struct ForwardInfoEstablished {
    pub using: TypeOfConnection,
    pub md: FoundMetadata,
    pub index_internal: TopicsIndexInternal,
}

#[derive(Debug)]
pub struct ForwardInfo {
    pub urls: Vec<TypeOfConnection>,

    // pub handle: Option<tokio::task::JoinHandle<()>>,
    pub job_name: TopicName,

    pub established: Option<ForwardInfoEstablished>,
    pub mounted_at: TopicName,
}

#[derive(Debug)]
pub struct ProxiedTopicInfo {
    pub tr_original: TopicRefInternal,
    pub from_subscription: TopicName,
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
}

#[derive(Debug, Clone)]
pub struct SavedBlob {
    pub content: Vec<u8>,
    pub who_needs_it: HashSet<(String, usize)>,
    // will not be removed until who_needs_it is empty and the deadline has passed
    pub deadline: i64,
}

#[derive(Debug)]
pub struct ServerState {
    pub node_started: i64,
    pub node_app_data: HashMap<String, NodeAppData>,
    pub node_id: String,

    /// Our queues
    pub oqs: HashMap<TopicName, ObjectQueue>,
    /// The subscriptions to other nodes
    pub proxied: HashMap<TopicName, ForwardInfo>,

    /// The topics that we proxy
    pub proxied_topics: HashMap<TopicName, ProxiedTopicInfo>,

    /// Filesystem match
    pub local_dirs: HashMap<TopicName, LocalDirInfo>,

    /// The other proxied resources (like other websites)
    pub proxied_other: HashMap<TopicName, OtherProxyInfo>,

    pub blobs: HashMap<String, SavedBlob>,
    pub blobs_forgotten: HashMap<String, i64>,

    advertise_urls: Vec<String>,

    status_tx: mpsc::UnboundedSender<ComponentStatusNotification>,
    status_rx: mpsc::UnboundedReceiver<ComponentStatusNotification>,

    pub aliases: HashMap<TopicName, TopicName>,

    pub job_manager: InternalJobManager,
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
        let node_app_data = node_app_data.unwrap_or_default();
        let node_id = format!("rust-{}", get_random_node_id());

        let mut oqs: HashMap<TopicName, ObjectQueue> = HashMap::new();

        let now = time_nanos_i64();
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
                has_history: true,
                patchable: true,
            },
            content_info: ContentInfo::simple(CONTENT_TYPE_DTPS_INDEX_CBOR, Some(schema_for!(TopicsIndexWire))),
        };
        oqs.insert(TopicName::root(), ObjectQueue::new(tr, Some(10)));

        let node_started = time_nanos_i64();

        let (status_tx, status_rx) = mpsc::unbounded_channel::<ComponentStatusNotification>();

        let mut ss = ServerState {
            node_id,
            node_started,
            node_app_data,
            oqs,
            proxied_other: HashMap::new(),
            proxied_topics: HashMap::new(),
            blobs: HashMap::new(),
            blobs_forgotten: HashMap::new(),
            proxied: HashMap::new(),
            advertise_urls: vec![],
            local_dirs: HashMap::new(),
            status_tx,
            status_rx,
            aliases: HashMap::new(),
            job_manager: InternalJobManager::new(),
        };

        let p = TopicProperties {
            streamable: true,
            pushable: false,
            readable: true,
            immutable: false,
            has_history: true,
            patchable: false,
        };

        ss.new_topic(
            &TopicName::from_dash_sep(TOPIC_LIST_NAME)?,
            None,
            CONTENT_TYPE_JSON,
            &p,
            None,
            Some(10),
        )?;

        ss.new_topic(
            &TopicName::from_dash_sep(TOPIC_LIST_CLOCK)?,
            None,
            CONTENT_TYPE_JSON,
            &p,
            Some(schema_for!(i64)),
            Some(10),
        )?;
        ss.new_topic(
            &TopicName::from_dash_sep(TOPIC_LIST_AVAILABILITY)?,
            None,
            CONTENT_TYPE_JSON,
            &p,
            None,
            Some(10),
        )?;
        ss.new_topic(
            &TopicName::from_dash_sep(TOPIC_LOGS)?,
            None,
            CONTENT_TYPE_JSON,
            &p,
            None,
            Some(10),
        )?;

        ss.new_topic(
            &TopicName::from_dash_sep(TOPIC_STATE_NOTIFICATION)?,
            None,
            CONTENT_TYPE_YAML,
            &p,
            Some(schema_for!(ComponentStatusNotification)),
            Some(10),
        )?;

        ss.new_topic(
            &TopicName::from_dash_sep(TOPIC_STATE_SUMMARY)?,
            None,
            CONTENT_TYPE_YAML,
            &p,
            None,
            Some(10),
        )?;

        let p = TopicProperties {
            streamable: true,
            pushable: false,
            readable: true,
            immutable: false,
            has_history: true,
            patchable: true,
        };

        ss.new_topic(
            &TopicName::from_dash_sep(TOPIC_PROXIED)?,
            None,
            CONTENT_TYPE_JSON,
            &p,
            Some(schema_for!(Proxied)),
            Some(1),
        )?;
        ss.publish_json(&TopicName::from_dash_sep(TOPIC_PROXIED)?, "{}", None)?;

        let p = TopicProperties {
            streamable: true,
            pushable: false,
            readable: true,
            immutable: false,
            has_history: true,
            patchable: true,
        };

        ss.new_topic(
            &TopicName::from_dash_sep(TOPIC_CONNECTIONS)?,
            None,
            CONTENT_TYPE_JSON,
            &p,
            Some(schema_for!(ConnectionsWire)),
            Some(1),
        )?;
        ss.publish_json(&TopicName::from_dash_sep(TOPIC_CONNECTIONS)?, "{}", None)?;

        Ok(ss)
    }
    /// Modifies a local queue by applying a function that acts on the deserialized value
    pub async fn modify_topic<AT, T, F>(&mut self, topic_name: AT, f: F) -> DTPSR<DataSaved>
    where
        F: FnOnce(&mut T) -> DTPSR<()>,
        AT: AsRef<TopicName>,
        T: Serialize + DeserializeOwned + JsonSchema + Clone + Debug,
    {
        let last_insert = match self.get_last_insert(&topic_name)? {
            Some(x) => x,
            None => {
                return Err(DTPSError::Other("Cannot get last insert".to_string()));
            }
        };
        let content_type = last_insert.raw_data.content_type.clone();

        let mut value = last_insert.raw_data.interpret_owned::<T>()?;
        f(&mut value)?;

        let new_data = RawData::encode_as(&value, &content_type)?;
        // TODO: add new clocks
        self.publish_raw_data(topic_name, &new_data, None)
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
        let x = self.publish_object_as_json(&TopicName::from_relative_url(TOPIC_LOGS).unwrap(), &log_entry, None);
        if let Err(e) = x {
            error_with_info!("Error publishing log message: {:?}", e);
        }
    }
    pub fn debug(&mut self, msg: String) {
        debug_with_info!("{}", msg);
        self.log_message(msg, "debug");
    }
    pub fn info(&mut self, msg: String) {
        debug_with_info!("{}", msg);
        self.log_message(msg, "info");
    }
    pub fn error(&mut self, msg: String) {
        error_with_info!("{}", msg);

        self.log_message(msg, "error");
    }
    pub fn warn(&mut self, msg: String) {
        warn_with_info!("{}", msg);
        self.log_message(msg, "warn");
    }
    pub fn add_proxy_connection(
        &mut self,
        topic_name: &TopicName,
        cons: &Vec<TypeOfConnection>,
        expect_node_id: Option<String>,
        ssa: ServerStateAccess,
    ) -> DTPSR<()> {
        if self.proxied.contains_key(topic_name) {
            return Err(DTPSError::TopicAlreadyExists(topic_name.to_dash_sep()));
        }
        let ssa2 = ssa.clone();
        let topic_name2 = topic_name.clone();

        let topic_name = topic_name.clone();
        let cons2 = cons.clone();
        let connect_job: JobFunctionType = Box::new(move || {
            let topic_name = topic_name.clone();
            let cons = cons2.clone();
            let ssa = ssa.clone();
            let expect_node_id = expect_node_id.clone();

            Box::pin(
                async move { observe_node_proxy(topic_name.clone(), cons.clone(), expect_node_id, ssa.clone()).await },
            )
        });

        self.job_manager
            .add_job(&topic_name2, "proxy listening job", connect_job, false, true, 1.0, ssa2)?;

        let fi = ForwardInfo {
            urls: cons.clone(),
            job_name: topic_name2.clone(),
            // handle: Some(handle),
            established: None,
            mounted_at: topic_name2.clone(),
        };
        self.proxied.insert(topic_name2.clone(), fi);

        Ok(())
    }
    pub fn remove_proxy_connection(&mut self, topic_name: &TopicName) -> DTPSR<()> {
        if !self.proxied.contains_key(topic_name) {
            return Err(DTPSError::TopicNotFound(topic_name.to_dash_sep()));
        }
        debug_with_info!("Removing proxy connection {:?}", topic_name);
        self.proxied.remove(topic_name);
        self.proxied_topics.retain(|k, _| {
            if is_prefix_of(topic_name.as_components(), k.as_components()).is_some() {
                debug_with_info!("Removing proxy topic {:?}", k);
                false
            } else {
                true
            }
        });
        Ok(())
    }

    pub async fn remove_topic_to_topic_connection(&mut self, connection_name: &CompositeName) -> DTPSR<()> {
        self.remove_topic_to_topic_connection_(connection_name).await?;

        let connections_name = TopicName::from_dash_sep(TOPIC_CONNECTIONS)?;

        self.modify_topic(connections_name, |connections: &mut ConnectionsWire| {
            let connection_name = connection_name.to_dash_sep();
            connections.remove(&connection_name);
            Ok(())
        })
        .await?;

        Ok(())
    }

    pub async fn remove_topic_to_topic_connection_(
        &mut self,
        connection_name: &CompositeName,
    ) -> DTPSR<SharedStatusNotification> {
        let prefix = CompositeName::from_relative_url("connections")?;
        let job_name = prefix + connection_name.clone();
        self.job_manager.remove_job(&job_name)
    }

    pub async fn add_topic_to_topic_connection(
        &mut self,
        connection_name: &CompositeName,
        connection_job: &ConnectionJob,
        ssa: ServerStateAccess,
    ) -> DTPSR<()> {
        self.add_topic_to_topic_connection_(connection_name, connection_job, ssa)
            .await?;

        let connections_name = TopicName::from_dash_sep(TOPIC_CONNECTIONS)?;
        self.modify_topic(connections_name, |connections: &mut ConnectionsWire| {
            let connection_name = connection_name.to_dash_sep();
            let connection = connection_job.to_wire();
            connections.insert(connection_name, connection);
            Ok(())
        })
        .await?;

        Ok(())
    }

    pub async fn add_topic_to_topic_connection_(
        &mut self,
        connection_name: &CompositeName,
        connection_job: &ConnectionJob,
        ssa: ServerStateAccess,
    ) -> DTPSR<()> {
        let prefix = CompositeName::from_relative_url("connections")?;
        let job_name = prefix + connection_name.clone();

        self.start_connection(&job_name, connection_name, connection_job, ssa)
            .await?;
        Ok(())
    }

    async fn start_connection(
        &mut self,
        job_name: &CompositeName,
        connection_name: &CompositeName,
        connection_job: &ConnectionJob,
        ssa: ServerStateAccess,
    ) -> DTPSR<()> {
        // let event = SharedStatusNotification::new(job_name.as_dash_sep());
        // let event_clone = event.clone();
        let connection_name = connection_name.clone();
        let connection_job = connection_job.clone();

        let ssa2 = ssa.clone();
        let connect_job: JobFunctionType = Box::new(move || {
            let connection_name = connection_name.clone();
            let connection_job = connection_job.clone();
            let ssa = ssa.clone();
            // let event_clone = event_clone.clone();

            Box::pin(async move {
                let res = run_connection_job(connection_name, connection_job, ssa).await;
                dtpserror_context!(res, "Connection job finished with error",)?;
                Ok(())
            })
        });

        self.job_manager
            .add_job(job_name, "Connection job", connect_job, true, true, 1.0, ssa2)?;

        Ok(())
    }

    pub fn new_proxy_topic(
        &mut self,
        from_subscription: &TopicName,
        its_topic_name: &TopicName,
        topic_name: &TopicName,
        tr_original: &TopicRefInternal,
        reachability_we_used: TopicReachabilityInternal,
        link_benchmark_last: LinkBenchmark,
    ) -> DTPSR<()> {
        if self.proxied_topics.contains_key(topic_name) {
            return Err(DTPSError::TopicAlreadyExists(topic_name.to_dash_sep()));
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
        debug_with_info!("New proxy topic {:?} -> {:?}", topic_name, its_topic_name);
        self.update_my_topic()
    }

    pub fn new_filesystem_mount(&mut self, topic_name: &TopicName, fp: FilePaths) -> DTPSR<()> {
        if self.local_dirs.contains_key(topic_name) {
            return Err(DTPSError::TopicAlreadyExists(topic_name.to_relative_url()));
        }

        let local_dir = match fp {
            FilePaths::Absolute(s) => PathBuf::from(s),
            FilePaths::Relative(s) => absolute_path(s).map_err(|e| DTPSError::Other(e.to_string()))?,
        };

        // let local_dir = PathBuf::from(path);
        debug_with_info!("New local folder {:?} -> {:?}", topic_name, local_dir.to_str());
        let uuid = get_queue_id(&self.node_id, topic_name);
        self.local_dirs.insert(
            topic_name.clone(),
            LocalDirInfo {
                unique_id: uuid,
                local_dir: local_dir.as_path().to_str().unwrap().to_string(),
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
        max_history: Option<usize>,
    ) -> DTPSR<()> {
        let content_info = ContentInfo::simple(content_type, schema);
        self.new_topic_ci(topic_name, app_data, properties, &content_info, max_history)
    }
    pub fn new_topic_ci(
        &mut self,
        topic_name: &TopicName,
        app_data: Option<HashMap<String, NodeAppData>>,
        properties: &TopicProperties,
        content_info: &ContentInfo,
        max_history: Option<usize>,
    ) -> DTPSR<()> {
        if self.oqs.contains_key(topic_name) {
            return Err(DTPSError::TopicAlreadyExists(topic_name.to_relative_url()));
        }
        let uuid = get_queue_id(&self.node_id, topic_name);
        let app_data = app_data.unwrap_or_default();

        let origin_node = self.node_id.clone();
        let now = time_nanos_i64();
        let tr = TopicRefInternal {
            unique_id: uuid,
            origin_node,
            app_data,
            created: now,
            reachability: vec![],
            properties: properties.clone(),
            content_info: content_info.clone(),
        };
        let oqs = &mut self.oqs;

        oqs.insert(topic_name.clone(), ObjectQueue::new(tr, max_history));
        info_with_info!("New topic: {:?}", topic_name);

        self.update_my_topic()
    }

    pub fn remove_topic(&mut self, topic_name: &TopicName) -> DTPSR<()> {
        if !self.oqs.contains_key(topic_name) {
            return Err(DTPSError::TopicNotFound(topic_name.to_relative_url()));
        }
        info_with_info!("Removing topic {topic_name:?}");
        let dropped_digests = {
            let mut todrop = Vec::new();
            let oq = self.oqs.get_mut(topic_name).unwrap();
            for data_saved in oq.saved.values() {
                todrop.push((data_saved.digest.clone(), data_saved.index));
            }
            oq.you_are_being_deleted();
            todrop
        };

        for (digest, index) in dropped_digests {
            self.release_blob(&digest, topic_name.as_dash_sep(), index);
        }

        self.oqs.remove(topic_name);
        self.update_my_topic()
    }

    fn update_my_topic(&mut self) -> DTPSR<()> {
        let index_internal = self.create_topic_index();
        let index = index_internal.to_wire(None);
        let data_cbor = serde_cbor::to_vec(&index).unwrap();
        // self.publish(topic_name, content, CONTENT_TYPE_CBOR, clocks)content_
        self.publish(&TopicName::root(), &data_cbor, CONTENT_TYPE_DTPS_INDEX_CBOR, None)?;

        let mut topics: Vec<String> = Vec::new();
        let oqs = &mut self.oqs;

        for topic_name in oqs.keys() {
            topics.push(topic_name.to_relative_url());
        }
        self.publish_object_as_json(&TopicName::from_relative_url(TOPIC_LIST_NAME)?, &topics.clone(), None)?;

        Ok(())
    }

    pub fn publish_raw_data<A: AsRef<TopicName>, B: AsRef<RawData>>(
        &mut self,
        topic_name: A,
        raw_data: B,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let rd = raw_data.as_ref();
        self.publish(topic_name.as_ref(), &rd.content, &rd.content_type, clocks)
    }
    pub fn publish<B: AsRef<[u8]>, C: AsRef<str>>(
        &mut self,
        topic_name: &TopicName,
        content: B,
        content_type: C,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        if !self.oqs.contains_key(topic_name) {
            return Err(DTPSError::TopicNotFound(topic_name.to_relative_url()));
        }

        let data0 = RawData::new(content, content_type);

        let oq = self.oqs.get_mut(topic_name).unwrap();
        let (data, ds, dropped_digests) = oq.push(&data0, clocks)?;
        // Note: we now transform the data (possibly) to the expected content type
        let new_digest = ds.digest.clone();
        let comment = format!("Index = {}", ds.index);
        if ds.digest != data.digest() {
            panic!("Internal inconsistency: digest mismatch");
        }
        self.save_blob(&new_digest, &data.content, topic_name.as_dash_sep(), ds.index, &comment);
        for (digest, i) in dropped_digests {
            self.release_blob(&digest, topic_name.as_dash_sep(), i);
        }
        self.cleanup_blobs();
        Ok(ds)
    }

    pub fn guarantee_blob_exists(&mut self, digest: &str, seconds: f64) {
        // debug_with_info!("Guarantee blob {digest} exists for {seconds} seconds more");
        let now = time_nanos_i64();
        let deadline = now + (seconds * 1_000_000_000.0) as i64;
        match self.blobs.get_mut(digest) {
            None => {
                error_with_info!("Blob {digest} not found");
            }
            Some(sb) => {
                sb.deadline = max(sb.deadline, deadline);
            }
        }
    }
    pub fn cleanup_blobs(&mut self) {
        let now = time_nanos_i64();
        let mut todrop = Vec::new();
        for (digest, sb) in self.blobs.iter() {
            let no_one_needs_it = sb.who_needs_it.is_empty();
            let deadline_passed = now > sb.deadline;
            if no_one_needs_it && deadline_passed {
                todrop.push(digest.clone());
            }
        }
        for digest in todrop {
            // debug_with_info!("Dropping blob {digest} because deadline passed");
            self.blobs.remove(&digest);

            self.blobs_forgotten.insert(digest, now);
        }
    }
    pub fn release_blob(&mut self, digest: &str, who: &str, i: usize) {
        // log::debug!("Del blob {digest} for {who:?}");

        match self.blobs.get_mut(digest) {
            None => {
                if self.blobs_forgotten.contains_key(digest) {
                    warn_with_info!("Blob {digest} to forget already forgotten (who = {who}).");
                } else {
                    error_with_info!("Blob {digest} to forget is completely unknown.");
                }
            }
            Some(sb) => {
                let who_i = (who.to_string(), i);
                if sb.who_needs_it.contains(&who_i) {
                    sb.who_needs_it.remove(&who_i);
                } else {
                    warn_with_info!("Blob {digest} to forget was not needed by {who}");
                }

                if sb.who_needs_it.is_empty() {
                    let now = time_nanos_i64();
                    let deadline_passed = now > sb.deadline;
                    if deadline_passed {
                        self.blobs.remove(digest);
                        self.blobs_forgotten.insert(digest.to_string(), now);
                    }
                }
            }
        }
    }

    pub fn save_blob(&mut self, digest: &str, content: &[u8], who: &str, i: usize, comment: &str) {
        let _ = comment;
        // log::debug!("Add blob {digest} for {who:?}: {comment}");
        let who_i = (who.to_string(), i);
        match self.blobs.get_mut(digest) {
            None => {
                let mut sb = SavedBlob {
                    content: content.to_vec(),
                    who_needs_it: HashSet::new(),
                    deadline: 0,
                };

                sb.who_needs_it.insert(who_i);
                self.blobs.insert(digest.to_string(), sb);
            }
            Some(sb) => {
                sb.who_needs_it.insert(who_i);
            }
        }
    }

    pub fn save_blob_for_time(&mut self, digest: &str, content: &[u8], delta_seconds: f64) {
        let time_nanos_delta = (delta_seconds * 1_000_000_000.0) as i64;
        let nanos_now = time_nanos_i64();
        let deadline = nanos_now + time_nanos_delta;
        match self.blobs.get_mut(digest) {
            None => {
                let sb = SavedBlob {
                    content: content.to_vec(),
                    who_needs_it: HashSet::new(),
                    deadline,
                };
                self.blobs.insert(digest.to_string(), sb);
            }
            Some(sb) => {
                sb.deadline = max(sb.deadline, deadline);
            }
        }
    }

    pub fn get_blob(&self, digest: &str) -> Option<&Vec<u8>> {
        return self.blobs.get(digest).map(|v| &v.content);
    }

    pub fn get_blob_bytes(&self, digest: &str) -> DTPSR<Bytes> {
        let x = self.blobs.get(digest).map(|v| Bytes::from(v.content.clone()));
        match x {
            Some(v) => Ok(v),
            None => match self.blobs_forgotten.get(digest) {
                Some(ts) => {
                    let now = time_nanos_i64();
                    let delta = now - ts;
                    let seconds = delta as f64 / 1_000_000_000.0;
                    let msg = format!("Blob {:#?} not found. It was forgotten {} seconds ago", digest, seconds);
                    Err(DTPSError::NotAvailable(msg))
                }
                None => {
                    let msg = format!("Blob {:#?} was never saved", digest);
                    Err(DTPSError::ResourceNotFound(msg)) // should be resource not found
                }
            },
        }
    }

    pub fn publish_object_as_json<T: Serialize>(
        &mut self,
        topic_name: &TopicName,
        object: &T,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let data_json = serde_json::to_string(object)?;
        return self.publish_json(topic_name, data_json.as_str(), clocks);
    }

    pub fn publish_object_as_yaml<T: Serialize>(
        &mut self,
        topic_name: &TopicName,
        object: &T,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let data_yaml = serde_yaml::to_string(object)?;
        return self.publish_yaml(topic_name, data_yaml.as_str(), clocks);
    }

    pub fn publish_object_as_cbor<T: Serialize>(
        &mut self,
        topic_name: &TopicName,
        object: &T,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let data_cbor = serde_cbor::to_vec(object)?;
        self.publish_cbor(topic_name, &data_cbor, clocks)
    }

    pub fn publish_cbor(&mut self, topic_name: &TopicName, content: &[u8], clocks: Option<Clocks>) -> DTPSR<DataSaved> {
        self.publish(topic_name, content, CONTENT_TYPE_CBOR, clocks)
    }

    pub fn publish_json(
        &mut self,
        topic_name: &TopicName,
        json_content: &str,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let bytesdata = json_content.as_bytes().to_vec();
        self.publish(topic_name, &bytesdata, CONTENT_TYPE_JSON, clocks)
    }

    pub fn publish_yaml(
        &mut self,
        topic_name: &TopicName,
        yaml_content: &str,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let bytesdata = yaml_content.as_bytes().to_vec();
        self.publish(topic_name, &bytesdata, CONTENT_TYPE_JSON, clocks)
    }

    pub fn publish_plain(
        &mut self,
        topic_name: &TopicName,
        text_content: &str,
        clocks: Option<Clocks>,
    ) -> DTPSR<DataSaved> {
        let bytesdata = text_content.as_bytes().to_vec();
        self.publish(topic_name, &bytesdata, CONTENT_TYPE_PLAIN, clocks)
    }

    //noinspection RsConstantConditionIf
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
                forwarding_node_connects_to: oq.data_url.to_url_repr(),

                performance: oq.link_benchmark_last.clone(),
            });

            let total = oq.reachability_we_used.benchmark.clone() + oq.link_benchmark_last.clone();

            if *MASK_ORIGIN {
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
            let forwarders = vec![ForwardingStep {
                forwarding_node: self.node_id.clone(),
                forwarding_node_connects_to: pinfo.con.to_url_repr(),

                performance: LinkBenchmark::identity(), // TODO:
            }];

            let prop = TopicProperties {
                streamable: false,
                pushable: false,
                readable: true,
                immutable: false,
                has_history: false,
                patchable: false,
            };

            let mut tr = TopicRefInternal {
                unique_id: pinfo.con.to_url_repr(),   // XXX
                origin_node: pinfo.con.to_url_repr(), // XXX
                app_data: Default::default(),
                reachability: vec![],
                created: 0,
                properties: prop,
                content_info: ContentInfo::generic(),
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
                has_history: false,
                patchable: false,
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
                content_info: ContentInfo::generic(),
            };

            tr.reachability.push(TopicReachabilityInternal {
                con: TypeOfConnection::Relative(topic_name.to_relative_url(), None),
                answering: self.node_id.clone(),
                forwarders: vec![],
                benchmark: LinkBenchmark::identity(),
            });

            topics.insert(topic_name.clone(), tr);
        }
        for (alias, target) in self.aliases.iter() {
            let tr = if topics.contains_key(target) {
                let mut orig = topics.get(target).unwrap().clone();
                orig.app_data.insert("aliased_to".to_string(), target.to_relative_url());
                orig
            } else {
                TopicRefInternal {
                    unique_id: "".to_string(),   // FIXME
                    origin_node: "".to_string(), // FIXME
                    app_data: hashmap! {
                        "aliased_to".to_string() => target.to_relative_url(),
                        "comment".to_string() => "Target not existing yet.".to_string(),
                    },
                    reachability: vec![], // not reachable
                    created: 0,
                    properties: TopicProperties {
                        // all false
                        streamable: false,
                        pushable: false,
                        readable: false,
                        immutable: false,
                        has_history: false,
                        patchable: false,
                    },
                    content_info: ContentInfo::generic(),
                }
            };
            topics.insert(alias.clone(), tr);
        }

        TopicsIndexInternal { topics }
    }

    pub fn subscribe_insert_notification(&self, tn: &TopicName) -> DTPSR<Receiver<ListenURLEvents>> {
        let q = match self.oqs.get(tn) {
            None => {
                let s = format!("Could not find topic {}", tn.as_dash_sep());
                return Err(DTPSError::TopicNotFound(s));
            }
            Some(q) => q,
        };
        Ok(q.subscribe_insert_notification())
    }

    pub fn get_last_insert_assert_exists<AT: AsRef<TopicName>>(&self, topic_name: AT) -> DTPSR<InsertNotification> {
        match self.get_last_insert(topic_name.as_ref())? {
            None => internal_assertion!("{:?} is empty but it should never be.", topic_name.as_ref()),

            Some(s) => Ok(s),
        }
    }
    pub fn get_last_insert<AT: AsRef<TopicName>>(&self, topic_name: AT) -> DTPSR<Option<InsertNotification>> {
        let q = self.get_queue(topic_name.as_ref())?;
        match q.stored.last() {
            None => Ok(None),
            Some(index) => {
                let data_saved = q.saved.get(index).unwrap();
                let digest = &data_saved.digest;
                let content = context!(
                    self.get_blob_bytes(digest),
                    "Error getting blob {} for topic {:?}",
                    digest,
                    topic_name.as_ref().as_dash_sep(),
                )?;

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
        let tn = TopicName::from_dash_sep(TOPIC_STATE_NOTIFICATION)?;
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
    let md = context!(get_metadata(url).await, "Error getting metadata for proxied at {}", url,)?;

    let meta_url = match &md.meta_url {
        None => {
            return not_reachable!("Cannot find meta_url for proxy at {url}:\n{md:?}");
        }
        Some(u) => u.clone(),
    };
    let index_internal = context!(
        get_index(&meta_url).await,
        "Error getting the index for {url} index {meta_url}"
    )?;
    Ok((md, index_internal))
}

pub async fn show_errors<X, F: Future<Output = DTPSR<X>>>(ssa: Option<ServerStateAccess>, desc: String, future: F) {
    let tn = TopicName::from_dash_sep(&desc).unwrap();
    if let Some(ssa) = &ssa {
        let mut ss = ssa.lock().await;
        ss.send_status_notification(&tn, Status::RUNNING, None).unwrap();
    };

    let message = match future.await {
        Ok(_) => None,
        Err(e) => {
            // e.anyhow_kind().print_backtrace();
            // let ef = format!("{}\n---\n{:?}", e, e);
            let ef = format!("{:?}", e);
            error_with_info!("Error in async {desc}:\n{}", indent_all_with("| ", &ef));
            // error_with_info!("Error: {:#?}", e.backtrace());
            Some(ef)
        }
    };

    if let Some(ssa) = &ssa {
        let mut ss = ssa.lock().await;
        ss.send_status_notification(&tn, Status::EXITED, message).unwrap();
    }
}

pub async fn sniff_and_start_proxy(
    mounted_at: TopicName,
    url: TypeOfConnection,
    ss_mutex: ServerStateAccess,
) -> DTPSR<()> {
    if let TypeOfConnection::File(_, fp) = url {
        let mut ss = ss_mutex.lock().await;
        ss.new_filesystem_mount(&mounted_at, fp)?;
        return Ok(());
    }

    let rtype = loop {
        match sniff_type_resource(&url).await {
            Ok(s) => break s,
            Err(e) => {
                warn_with_info!("Cannot sniff:\n{e:?}");
                debug_with_info!("observe_proxy: retrying in 2 seconds");
                sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        }
    };

    match rtype {
        TypeOfResource::Other => handle_proxy_other(mounted_at, url, ss_mutex).await,
        TypeOfResource::DTPSTopic => {
            not_implemented!("observe_proxy: TypeOfResource::DTPSTopic")
        }
        TypeOfResource::DTPSIndex { node_id } => {
            let mut ss = ss_mutex.lock().await;
            let urls = vec![url.clone()];
            ss.add_proxy_connection(&mounted_at, &urls, Some(node_id), ss_mutex.clone())?;
            Ok(())
            // not_implemented!("observe_proxy: TypeOfResource::DTPSIndex")
            // ss.add_proxy_connection(&subcription_name, &url, ss_mutex).await
            // observe_node_proxy(subcription_name, mounted_at, url, ss_mutex).await
        }
    }
}

pub async fn handle_proxy_other(
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
    mounted_at: TopicName,
    cons: Vec<TypeOfConnection>,
    expect_node_id: Option<String>,
    ss_mutex: ServerStateAccess,
) -> DTPSR<()> {
    info_with_info!("observe_node_proxy: observing proxy at {}", mounted_at.as_dash_sep());
    let cons_set = HashSet::from_iter(cons.clone().into_iter());
    let url = compute_best_alternative(&cons_set, expect_node_id.clone()).await?;
    let (md, index_internal_at_t0) = loop {
        match get_proxy_info(&url).await {
            Ok(s) => break s,
            Err(e) => {
                warn_with_info!(
                    "observe_proxy: error getting proxy info for proxied {:?}:\n{:?}",
                    mounted_at,
                    e
                );
                info_with_info!("observe_proxy: retrying in 2 seconds");
                sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        }
    };

    info_with_info!("observe_node_proxy: found first information");

    let who_answers = match &md.answering {
        None => {
            error_with_info!("observe_node_proxy: nobody answering");
            return not_available!("Nobody is answering {url:}:\n{md:?}");
        }
        Some(n) => {
            if incompatible(&expect_node_id, &md.answering) {
                error_with_info!("observe_node_proxy: wrong node answering {n} at {url}");
                return not_available!(
                "observe_node_proxy: invalid proxy connection: we expect {expect_node_id:?} but we find {n} at {url}",
                );
            }

            n.clone()
        }
    };

    {
        let ss = ss_mutex.lock().await;
        if who_answers == ss.node_id {
            error_with_info!("observe_node_proxy: we find ourselves at {url}");
            return invalid_input!(
                "observe_node_proxy: invalid proxy connection: we find ourselves at the connection {url}"
            );
        }
    }

    info_with_info!("observe_node_proxy: finding stats");
    let stats = get_stats(&url, Some(who_answers)).await;

    info_with_info!("observe_node_proxy: obtained stats");
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
        let info = ss.proxied.get_mut(&mounted_at).unwrap();
        info_with_info!("observe_node_proxy: setting established");

        info.established = Some(ForwardInfoEstablished {
            using: url.clone(),
            md: md.clone(),
            index_internal: index_internal_at_t0.clone(),
        });
    }

    {
        let mut ss = ss_mutex.lock().await;
        add_from_response(&mut ss, &mounted_at, &index_internal_at_t0, link1.clone())?;
    }

    if md.content_type != CONTENT_TYPE_DTPS_INDEX_CBOR {
        info_with_info!("observe_node_proxy: will exit because this is a queue not an index");
        return Ok(());
    }

    let inline_url = md.events_data_inline_url.unwrap().clone();
    let (_handle, rx) = get_events_stream_inline(&inline_url).await;

    for_each_from_stream(rx, |lue| async {
        // debug_with_info!("observe_node_proxy: obained a notification {:?}", lue);
        let notification = match lue {
            ListenURLEvents::InsertNotification(not) => not,
            ListenURLEvents::WarningMsg(_)
            | ListenURLEvents::ErrorMsg(_)
            | ListenURLEvents::FinishedMsg(_)
            | ListenURLEvents::SilenceMsg(_) => return Ok(()),
        };

        if notification.raw_data.content_type != CONTENT_TYPE_DTPS_INDEX_CBOR {
            return dtpserror_other!(
                "This function expects to work on an index endpoint with mime type {}.\n{:#?}",
                CONTENT_TYPE_DTPS_INDEX_CBOR,
                notification
            );
        }
        let x0 = context!(
            serde_cbor::from_slice::<TopicsIndexWire>(&notification.raw_data.content),
            "Error deserializing TopicsIndexWire\n url: {}\n{:#?}",
            inline_url,
            notification
        )?;

        let ti = TopicsIndexInternal::from_wire(&x0, &url);

        {
            let mut ss = ss_mutex.lock().await;
            add_from_response(&mut ss, &mounted_at, &ti, link1.clone())?;
        }
        Ok(())
    })
    .await?;

    dtpserror_other!("The proxied disconnected")
    //
    // info_with_info!("observe_node_proxy: finished observing proxy at {url}");
    //
    // Ok(())
}

pub async fn for_each_from_stream<T, F, Fut>(mut rx: BroadcastReceiver<T>, f: F) -> DTPSR<()>
where
    T: Clone,
    F: Fn(T) -> Fut,
    Fut: Future<Output = DTPSR<()>>,
{
    loop {
        match rx.recv().await {
            Ok(msg) => f(msg).await?,
            Err(e) => {
                match e {
                    RecvError::Closed => break,
                    RecvError::Lagged(_) => {
                        debug_with_info!("lagged");
                        continue;
                    }
                };
            }
        };
    }

    Ok(())
}

pub fn add_from_response(
    s: &mut ServerState,
    mounted_at: &TopicName,
    tii: &TopicsIndexInternal,
    link_benchmark1: LinkBenchmark,
) -> DTPSR<()> {
    // debug_with_info!("topics_index: tii: \n{:#?}", tii);
    let ntopics = tii.topics.len();
    for (its_topic_name, tr) in tii.topics.iter() {
        // There is a case where we proxy a queue not an index
        if its_topic_name.is_root() && ntopics > 1 {
            // ok
            continue; // TODO
        }

        let available_as = mounted_at + its_topic_name;

        if s.has_topic(&available_as) {
            continue;
        }

        if tr.reachability.is_empty() {
            return DTPSError::not_reachable(format!(
                "topic {:?} of subscription {:?} is not reachable:\n{:#?}",
                its_topic_name, mounted_at, tr
            ));
        }
        let reachability_we_used = tr.reachability.first().unwrap();

        s.new_proxy_topic(
            mounted_at,
            its_topic_name,
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

async fn run_connection_job(
    connection_name: CompositeName,
    connection_job: ConnectionJob,
    ssa: ServerStateAccess,
    // tx: SharedStatusNotification,
) -> DTPSR<()> {
    // TODO: check already exists or not
    let (source, target) = {
        let ss = ssa.lock().await;

        let source = context!(
            interpret_path(connection_job.source.as_relative_url(), &hashmap! {}, &None, &ss).await,
            "Cannot obtain source for connection_job\n {connection_job:?}"
        )?;
        let target = context!(
            interpret_path(connection_job.target.as_relative_url(), &hashmap! {}, &None, &ss).await,
            "Cannot obtain target for connection_job\n {connection_job:?}"
        )?;
        (source, target)
    };
    debug_with_info!(
        "Connection job: {}: Connecting source:\n{source:#?}\n with\n {target:#?}",
        connection_name.as_dash_sep()
    );

    let stream1 = source.get_data_stream("not needed", ssa.clone()).await?;
    debug_with_info!("Source stream obtained");

    let mut stream1 = match stream1.stream {
        None => {
            let s = format!(
                "Source {source:?} is not available as a stream for connection {}",
                connection_name.as_dash_sep()
            );
            error_with_info!("{}", s);
            // tx.notify(false, &s).await?;
            return not_available!("{}", s);
        }
        Some(x) => x,
    };

    // tx.notify(true, "found source and target").await?;
    while let Some(x) = wrap_recv(&mut stream1).await {
        debug_with_info!(
            "Connection job: {}: Got data {:?} from source {source:?} with {target:?}",
            connection_name.as_dash_sep(),
            x
        );

        match x {
            ListenURLEvents::InsertNotification(inot) => {
                let presented_as = "n/a";
                context!(
                    target
                        .push(presented_as, ssa.clone(), &inot.raw_data, &inot.data_saved.clocks)
                        .await,
                    "Connection job: {}: Error pushing data to target\n{target:?}\nfrom source\n{source:?}",
                    connection_name.as_dash_sep(),
                )?;
            }
            ListenURLEvents::WarningMsg(_)
            | ListenURLEvents::ErrorMsg(_)
            | ListenURLEvents::FinishedMsg(_)
            | ListenURLEvents::SilenceMsg(_) => {
                debug_with_info!("ignoring {:?}", x)
            }
        }

        // let data = x?;
        // let data_saved = data.data_saved;
        // let raw_data = data.raw_data;
        // let target = target.clone();
        // let ssa = ssa.clone();
        // let connection_name = connection_name.clone();
        // let _ = tokio::spawn(async move {
        //     let mut ss = ssa.lock().await;
        //     let _ = ss.publish_raw_data(&target, &raw_data, None);
        //     let _ = ss.send_status_notification(&connection_name, Status::RUNNING, None);
        //     Ok(())
        // });
    }

    debug_with_info!("Connection job: {}: Source stream ended", connection_name.as_dash_sep());

    Ok(())
}
