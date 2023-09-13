use std::{
    cmp::max,
    collections::{
        HashMap,
        HashSet,
    },
    env,
    future::Future,
    path::{
        Path,
        PathBuf,
    },
};

use anyhow::Context;
use bytes::Bytes;
use chrono::Local;
use futures::StreamExt;
use indent::indent_all_with;
use maplit::hashmap;
use path_clean::PathClean;
use schemars::{
    schema::RootSchema,
    schema_for,
    JsonSchema,
};
use serde::{
    Deserialize,
    Serialize,
};
use strum_macros::{
    Display,
    EnumString,
};
use tokio::{
    sync::{
        broadcast::Receiver,
        mpsc,
    },
    time::sleep,
};

use crate::{
    compute_best_alternative,
    context,
    debug_with_info,
    error_with_info,
    get_events_stream_inline,
    get_index,
    get_metadata,
    get_queue_id,
    get_random_node_id,
    get_stats,
    info_with_info,
    invalid_input,
    is_prefix_of,
    not_available,
    not_implemented,
    not_reachable,
    sniff_type_resource,
    warn_with_info,
    Clocks,
    ContentInfo,
    DTPSError,
    DataSaved,
    FilePaths,
    ForwardingStep,
    FoundMetadata,
    InsertNotification,
    LinkBenchmark,
    NodeAppData,
    ObjectQueue,
    RawData,
    ServerStateAccess,
    TopicName,
    TopicProperties,
    TopicReachabilityInternal,
    TopicRefInternal,
    TopicsIndexInternal,
    TopicsIndexWire,
    TypeOfConnection,
    TypeOfResource,
    UrlResult,
    CONTENT_TYPE_CBOR,
    CONTENT_TYPE_DTPS_INDEX_CBOR,
    CONTENT_TYPE_JSON,
    CONTENT_TYPE_PLAIN,
    CONTENT_TYPE_YAML,
    DTPSR,
    MASK_ORIGIN,
    TOPIC_CONNECTIONS,
    TOPIC_LIST_AVAILABILITY,
    TOPIC_LIST_CLOCK,
    TOPIC_LIST_NAME,
    TOPIC_LOGS,
    TOPIC_PROXIED,
    TOPIC_STATE_NOTIFICATION,
    TOPIC_STATE_SUMMARY,
};

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct ProxyJob {
    pub node_id: String,
    pub urls: Vec<String>,
}

type Proxied = HashMap<String, ProxyJob>;

use std::str::FromStr;

#[derive(Debug, PartialEq)]
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
pub struct ConnectionJob {
    /// Source topic (dash separated)
    pub source: String,
    /// Target topic (dash separated)
    pub target: String,
    pub service_mode: String,
}

type Connections = HashMap<String, ConnectionJob>;

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

    pub handle: Option<tokio::task::JoinHandle<()>>,
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
    pub who_needs_it: HashSet<String>,
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

    /// The proxied other resources
    pub proxied_other: HashMap<TopicName, OtherProxyInfo>,
    pub blobs: HashMap<String, SavedBlob>,
    pub blobs_forgotten: HashMap<String, i64>,

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
        let node_id = format!("rust-{}", get_random_node_id());

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
                has_history: true,
                patchable: true,
            },
            content_info: ContentInfo::simple(CONTENT_TYPE_DTPS_INDEX_CBOR, Some(schema_for!(TopicsIndexWire))),
        };
        oqs.insert(TopicName::root(), ObjectQueue::new(tr, Some(10)));

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
            blobs_forgotten: HashMap::new(),
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
            Some(schema_for!(Connections)),
            Some(1),
        )?;
        ss.publish_json(&TopicName::from_dash_sep(TOPIC_CONNECTIONS)?, "{}", None)?;

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
        expect_node_id: String,
        ssa: ServerStateAccess,
    ) -> DTPSR<()> {
        if self.proxied.contains_key(topic_name) {
            return Err(DTPSError::TopicAlreadyExists(topic_name.to_dash_sep()));
        }
        let future = observe_node_proxy(topic_name.clone(), cons.clone(), expect_node_id, ssa.clone());

        let handle = tokio::spawn(show_errors(
            Some(ssa),
            format!(
                "observe_node_proxy: {topic_name}",
                topic_name = topic_name.as_dash_sep()
            ),
            future,
        ));
        let fi = ForwardInfo {
            urls: cons.clone(),
            handle: Some(handle),
            established: None,
            mounted_at: topic_name.clone(),
        };
        self.proxied.insert(topic_name.clone(), fi);

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

    pub fn new_filesystem_mount(
        &mut self,
        // from_subscription: &String,
        topic_name: &TopicName,
        fp: FilePaths,
    ) -> DTPSR<()> {
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
        let now = Local::now().timestamp_nanos();
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
                todrop.push(data_saved.digest.clone());
            }
            oq.you_are_being_deleted();
            todrop
        };

        for digest in dropped_digests {
            self.release_blob(&digest, topic_name.as_dash_sep());
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
        self.save_blob(&new_digest, &data.content, topic_name.as_dash_sep(), &comment);
        for digest in dropped_digests {
            self.release_blob(&digest, topic_name.as_dash_sep());
        }
        self.cleanup_blobs();
        Ok(ds)
    }

    pub fn guarantee_blob_exists(&mut self, digest: &str, seconds: f64) {
        // debug_with_info!("Guarantee blob {digest} exists for {seconds} seconds more");
        let now = Local::now().timestamp_nanos();
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
        let now = Local::now().timestamp_nanos();
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
    pub fn release_blob(&mut self, digest: &str, who: &str) {
        // log::debug!("Del blob {digest} for {who:?}");

        match self.blobs.get_mut(digest) {
            None => {
                warn_with_info!("Blob {digest} not found");
            }
            Some(sb) => {
                sb.who_needs_it.remove(who);
                if sb.who_needs_it.is_empty() {
                    let now = Local::now().timestamp_nanos();
                    let deadline_passed = now > sb.deadline;
                    if deadline_passed {
                        self.blobs.remove(digest);
                        self.blobs_forgotten.insert(digest.to_string(), now);
                    }
                }
            }
        }
    }

    pub fn save_blob(&mut self, digest: &str, content: &[u8], who: &str, comment: &str) {
        let _ = comment;
        // log::debug!("Add blob {digest} for {who:?}: {comment}");

        match self.blobs.get_mut(digest) {
            None => {
                let mut sb = SavedBlob {
                    content: content.to_vec(),
                    who_needs_it: HashSet::new(),
                    deadline: 0,
                };
                sb.who_needs_it.insert(who.to_string());
                self.blobs.insert(digest.to_string(), sb);
            }
            Some(sb) => {
                sb.who_needs_it.insert(who.to_string());
            }
        }
    }

    pub fn save_blob_for_time(&mut self, digest: &str, content: &[u8], delta_seconds: f64) {
        let time_nanos_delta = (delta_seconds * 1_000_000_000.0) as i64;
        let nanos_now = Local::now().timestamp_nanos();
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
                    let now = Local::now().timestamp_nanos();
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
            let forwarders = vec![ForwardingStep {
                forwarding_node: self.node_id.clone(),
                forwarding_node_connects_to: pinfo.con.to_string(),

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
                unique_id: pinfo.con.to_string(),
                origin_node: pinfo.con.to_string(),
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
        for alias in self.aliases.keys() {
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
                    has_history: false,
                    patchable: false,
                },
                content_info: ContentInfo::generic(), // TODO: we should put content info of original
            };
            topics.insert(alias.clone(), tr);
        }

        TopicsIndexInternal { topics }
    }

    pub fn subscribe_insert_notification(&self, tn: &TopicName) -> DTPSR<Receiver<InsertNotification>> {
        let q = match self.oqs.get(tn) {
            None => {
                let s = format!("Could not find topic {}", tn.as_dash_sep());
                return Err(DTPSError::TopicNotFound(s));
            }
            Some(q) => q,
        };
        Ok(q.subscribe_insert_notification())
    }

    pub fn get_last_insert(&self, tn: &TopicName) -> DTPSR<Option<InsertNotification>> {
        let q = self.get_queue(tn)?;
        match q.stored.last() {
            None => Ok(None),
            Some(index) => {
                let data_saved = q.saved.get(index).unwrap();
                let digest = &data_saved.digest;
                let content = context!(
                    self.get_blob_bytes(digest),
                    "Error getting blob {digest} for topic {tn:?}",
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
            return not_reachable!("Cannot find index url for proxy at {url}:\n{md:?}");
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
            ss.add_proxy_connection(&mounted_at, &urls, node_id, ss_mutex.clone())?;
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
    expect_node_id: String,
    ss_mutex: ServerStateAccess,
) -> DTPSR<()> {
    let cons_set = HashSet::from_iter(cons.clone().into_iter());
    let url = compute_best_alternative(&cons_set, &expect_node_id).await?;
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

    let who_answers = match &md.answering {
        None => {
            return not_available!("Nobody is answering {url:}:\n{md:?}");
        }
        Some(n) => {
            if n != &expect_node_id {
                return not_available!(
                    "observe_node_proxy: invalid proxy connection: we expect {expect_node_id} but we find {n} at {url}",
                );
            };
            n.clone()
        }
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
        let info = ss.proxied.get_mut(&mounted_at).unwrap();
        info.established = Some(ForwardInfoEstablished {
            using: url.clone(),
            md: md.clone(),
            index_internal: index_internal_at_t0.clone(),
        });
    }

    {
        let mut ss = ss_mutex.lock().await;
        add_from_response(
            &mut ss,
            // who_answers.clone(),
            &mounted_at,
            &index_internal_at_t0,
            link1.clone(),
            // url.to_string(),
        )?;
    }

    let inline_url = md.events_data_inline_url.unwrap().clone();
    let (_handle, mut stream) = get_events_stream_inline(&inline_url).await;

    while let Some(notification) = stream.next().await {
        let x0: TopicsIndexWire = serde_cbor::from_slice(&notification.raw_data.content).unwrap();
        let ti = TopicsIndexInternal::from_wire(&x0, &url);

        {
            let mut ss = ss_mutex.lock().await;
            add_from_response(
                &mut ss,
                // who_answers.clone(),
                &mounted_at,
                &ti,
                link1.clone(),
                // url.to_string(),
            )?;
        }
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
    for (its_topic_name, tr) in tii.topics.iter() {
        if its_topic_name.is_root() {
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
        let reachability_we_used = tr.reachability.get(0).unwrap();

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
