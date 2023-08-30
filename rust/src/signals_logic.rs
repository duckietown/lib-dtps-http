use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use anyhow::Context;
use async_recursion::async_recursion;
use async_trait::async_trait;
use bytes::Bytes;
use log::{debug, error};
use maplit::hashmap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_cbor::Value as CBORValue;
use serde_cbor::Value::Null as CBORNull;
use serde_cbor::Value::Text as CBORText;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;

use crate::cbor_manipulation::{get_as_cbor, get_inside};
use crate::signals_logic::ResolvedData::{NotAvailableYet, NotFound, Regular};
use crate::utils::{divide_in_components, is_prefix_of};
use crate::websocket_signals::ChannelInfo;
use crate::TypeOfConnection::Relative;
use crate::{
    context, error_with_info, get_channel_info_message, get_dataready, get_rawdata,
    not_implemented, utils, ContentInfo, DTPSError, DataReady, ForwardingStep, History,
    InsertNotification, LinkBenchmark, OtherProxyInfo, RawData, ServerState, ServerStateAccess,
    TopicName, TopicReachabilityInternal, TopicRefInternal, TopicsIndexInternal, TypeOfConnection,
    CONTENT_TYPE_DTPS_INDEX_CBOR, CONTENT_TYPE_TOPIC_HISTORY_CBOR, DTPSR, REL_URL_META,
    URL_HISTORY,
};

#[derive(Debug, Clone)]
pub enum Transforms {
    GetInside(Vec<String>),
}

impl Transforms {
    pub fn get_inside(&self, s: &str) -> Self {
        match self {
            Transforms::GetInside(vs) => {
                let mut vs = vs.clone();
                vs.push(s.to_string());
                Transforms::GetInside(vs)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct SourceComposition {
    pub topic_name: TopicName,
    pub is_root: bool,
    pub compose: HashMap<Vec<String>, Box<TypeOFSource>>,
    pub unique_id: String,
    pub origin_node: String,
}

impl DataProps for SourceComposition {
    fn get_properties(&self) -> TopicProperties {
        let mut immutable = true;
        let mut streamable = false;
        let pushable = false;
        let mut readable = true;
        let mut has_history = false;

        for (_k, v) in self.compose.iter() {
            let p = v.get_properties();

            // immutable: ALL immutable
            immutable = immutable && p.immutable;
            // streamable: ANY streamable
            streamable = streamable || p.streamable;
            // readable: ALL readable
            readable = readable && p.readable;
        }

        TopicProperties {
            streamable,
            pushable,
            readable,
            immutable,
            has_history,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ForwardedQueue {
    pub subscription: String,
    pub his_topic_name: TopicName,
    pub my_topic_name: TopicName,
    pub properties: TopicProperties,
}

#[derive(Debug, Clone)]
pub enum TypeOFSource {
    ForwardedQueue(ForwardedQueue),
    OurQueue(TopicName, Vec<String>, TopicProperties),
    MountedDir(TopicName, String, TopicProperties),
    MountedFile(TopicName, String, TopicProperties),
    Compose(SourceComposition),
    Transformed(Box<TypeOFSource>, Transforms),
    Digest(String, String),
    Deref(SourceComposition),
    Index(Box<TypeOFSource>),
    Aliased(TopicName, Option<Box<TypeOFSource>>),
    History(Box<TypeOFSource>),

    OtherProxied(OtherProxied),
}

impl TypeOFSource {
    pub fn get_inside(&self, s: &str) -> DTPSR<Self> {
        match self {
            TypeOFSource::ForwardedQueue(_q) => {
                not_implemented!("get_inside for {self:#?} with {s:?}")
            }
            TypeOFSource::OurQueue(_, _, _p) => {
                // TODO: this is where you would check the schema
                // not_implemented!("get_inside for {self:#?} with {s:?}")
                Ok(TypeOFSource::Transformed(
                    Box::new(self.clone()),
                    Transforms::GetInside(vec![s.to_string()]),
                ))
            }
            TypeOFSource::MountedDir(topic_name, path, props) => {
                let p = PathBuf::from(&path);
                let inside = p.join(s);
                if inside.exists() {
                    if inside.is_dir() {
                        Ok(TypeOFSource::MountedDir(
                            topic_name.clone(),
                            inside.to_str().unwrap().to_string(),
                            props.clone(),
                        ))
                    } else if inside.is_file() {
                        Ok(TypeOFSource::MountedFile(
                            topic_name.clone(),
                            inside.to_str().unwrap().to_string(),
                            props.clone(),
                        ))
                    } else {
                        not_implemented!("get_inside for {self:#?} with {s:?}")
                    }
                } else {
                    Err(DTPSError::TopicNotFound(format!(
                        "File not found: {}",
                        p.join(s).to_str().unwrap()
                    )))
                }

                // not_implemented!("get_inside for {self:#?} with {s:?}")
            }
            TypeOFSource::Compose(_c) => {
                not_implemented!("get_inside for {self:#?} with {s:?}")
            }
            TypeOFSource::Transformed(source, transform) => {
                // not_implemented!("get_inside for {self:#?} with {s:?}")
                Ok(TypeOFSource::Transformed(
                    source.clone(),
                    transform.get_inside(s),
                ))
            }

            TypeOFSource::MountedFile(_, _, _) => {
                let v = vec![s.to_string()];
                let transform = Transforms::GetInside(v);
                let tr = TypeOFSource::Transformed(Box::new(self.clone()), transform);
                Ok(tr)
            }
            TypeOFSource::Index(_) => {
                not_implemented!("get_inside for {self:#?} with {s:?}")
            }
            TypeOFSource::Digest(_, _) => {
                not_implemented!("get_inside for {self:#?} with {s:?}")
            }
            TypeOFSource::Deref(_c) => {
                not_implemented!("get_inside for {self:#?} with {s:?}")
            }
            TypeOFSource::OtherProxied(_) => {
                not_implemented!("get_inside for {self:#?} with {s:?}")
            }
            TypeOFSource::Aliased(_, _) => {
                not_implemented!("get_inside for {self:#?} with {s:?}")
            }
            TypeOFSource::History(_) => {
                not_implemented!("get_inside for {self:#?} with {s:?}")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct OtherProxied {
    path_and_query: String,
    op: OtherProxyInfo,
}

#[derive(Debug)]
pub enum ResolvedData {
    RawData(RawData),
    Regular(CBORValue),
    NotAvailableYet(String),
    NotFound(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TopicProperties {
    pub streamable: bool,
    pub pushable: bool,
    pub readable: bool,
    pub immutable: bool,
    pub has_history: bool,
}

impl TopicProperties {
    pub fn rw() -> Self {
        TopicProperties {
            streamable: true,
            pushable: true,
            readable: true,
            immutable: false,
            has_history: true,
        }
    }
}

#[async_trait]
pub trait ResolveDataSingle {
    async fn resolve_data_single(
        &self,
        presented_as: &str,
        ss_mutex: ServerStateAccess,
    ) -> DTPSR<ResolvedData>;
}

#[async_trait]
pub trait GetMeta {
    async fn get_meta_index(
        &self,
        presented_as: &str,
        ss_mutex: ServerStateAccess,
    ) -> DTPSR<TopicsIndexInternal>;
}

pub trait DataProps {
    fn get_properties(&self) -> TopicProperties;
}

#[async_trait]
impl ResolveDataSingle for TypeOFSource {
    async fn resolve_data_single(
        &self,
        presented_as: &str,
        ss_mutex: ServerStateAccess,
    ) -> DTPSR<ResolvedData> {
        match self {
            TypeOFSource::Digest(digest, content_type) => {
                let ss = ss_mutex.lock().await;
                let data = ss.get_blob_bytes(digest)?;
                let rd = RawData::new(data, content_type);
                Ok(ResolvedData::RawData(rd))
            }
            TypeOFSource::ForwardedQueue(q) => {
                let ss = ss_mutex.lock().await;
                let use_url = &ss.proxied_topics.get(&q.my_topic_name).unwrap().data_url;

                let rd = get_rawdata(use_url).await?;

                Ok(ResolvedData::RawData(rd))
            }
            TypeOFSource::OurQueue(q, _, _) => our_queue(q, ss_mutex).await,
            TypeOFSource::Compose(_sc) => {
                let index = self.get_meta_index(presented_as, ss_mutex).await?;
                // debug!("Compose index intenral:\n {:#?}", index);
                let to_wire = index.to_wire(None);

                // convert to cbor
                let cbor_bytes = serde_cbor::to_vec(&to_wire).unwrap();
                let raw_data = RawData {
                    content: Bytes::from(cbor_bytes),
                    content_type: CONTENT_TYPE_DTPS_INDEX_CBOR.to_string(),
                };
                Ok(ResolvedData::RawData(raw_data))
            }
            TypeOFSource::Transformed(source, transforms) => {
                let data = source
                    .resolve_data_single(presented_as, ss_mutex.clone())
                    .await?;
                transform(data, transforms).await
            }
            TypeOFSource::Deref(sc) => single_compose(sc, presented_as, ss_mutex).await,
            TypeOFSource::OtherProxied(op) => resolve_proxied(op).await,

            TypeOFSource::MountedDir(topic_name, _, _comps) => {
                let ss = ss_mutex.lock().await;
                let the_dir = ss.local_dirs.get(topic_name).unwrap();
                let the_path = PathBuf::from(&the_dir.local_dir);
                if !the_path.exists() {
                    return Ok(ResolvedData::NotFound(format!("Not found: {:?}", the_path)));
                }
                if the_path.is_dir() {
                    drop(ss);
                    let index = self.get_meta_index(presented_as, ss_mutex).await?;
                    // debug!("Compose index intenral:\n {:#?}", index);
                    let to_wire = index.to_wire(None);

                    // convert to cbor
                    let cbor_bytes = serde_cbor::to_vec(&to_wire).unwrap();
                    let raw_data = RawData {
                        content: Bytes::from(cbor_bytes),
                        content_type: CONTENT_TYPE_DTPS_INDEX_CBOR.to_string(),
                    };
                    return Ok(ResolvedData::RawData(raw_data));
                }
                not_implemented!("MountedDir: {self:?}")
            }
            TypeOFSource::MountedFile(_, filename, _) => {
                let data = std::fs::read(filename)?;
                let content_type = mime_guess::from_path(&filename)
                    .first()
                    .unwrap_or(mime::APPLICATION_OCTET_STREAM);
                let rd = RawData::new(data, content_type);

                Ok(ResolvedData::RawData(rd))
            }
            TypeOFSource::Index(inside) => {
                let x = inside.get_meta_index(presented_as, ss_mutex).await?;
                // not_implemented!("get_inside for {self:#?} with {s:?}")
                let xw = x.to_wire(None);
                // convert to cbor
                let cbor_bytes = serde_cbor::to_vec(&xw).unwrap();
                let raw_data = RawData {
                    content: Bytes::from(cbor_bytes),
                    content_type: CONTENT_TYPE_DTPS_INDEX_CBOR.to_string(),
                };
                return Ok(ResolvedData::RawData(raw_data));
            }
            TypeOFSource::Aliased(_, _) => {
                not_implemented!("resolve_data_single for {self:#?} with {self:?}")
            }
            TypeOFSource::History(s) => {
                let x: &TypeOFSource = s;
                match x {
                    TypeOFSource::OurQueue(topic, _, _) => {
                        let ss = ss_mutex.lock().await;
                        let q = ss.get_queue(topic)?;
                        let mut available: HashMap<usize, DataReady> = HashMap::new();
                        for index in q.stored.iter() {
                            let s = q.saved.get(index).unwrap();
                            available.insert(s.index, get_dataready(s));
                        }
                        let history = History { available };
                        let bytes = serde_cbor::to_vec(&history).unwrap();
                        return Ok(ResolvedData::RawData(RawData::new(
                            bytes,
                            CONTENT_TYPE_TOPIC_HISTORY_CBOR,
                        )));
                    }
                    _ => {
                        not_implemented!("resolve_data_single for {self:#?} with {self:?}")
                    }
                }

                // not_implemented!("resolve_data_single for {self:#?} with {self:?}")
            }
        }
    }
}

pub async fn resolve_proxied(op: &OtherProxied) -> DTPSR<ResolvedData> {
    let con0 = op.op.con.clone();

    let rest = &op.path_and_query;

    let con = con0.join(&rest)?;

    debug!("Proxied: {:?} -> {:?}", con0, con);

    let rd = get_rawdata(&con).await?;
    // let resp = context!(
    //     make_request(&con, hyper::Method::GET, b"",None, None).await,
    //     "Cannot make request to {:?}",
    //     con
    // )?;
    // let content_type = get_content_type(&resp);
    //
    // let body_bytes = context!(
    //     hyper::body::to_bytes(resp.into_body()).await,
    //     "Cannot get body bytes"
    // )?;
    // let rd = RawData::new(body_bytes, content_type);

    Ok(ResolvedData::RawData(rd))
}

impl DataProps for TypeOFSource {
    fn get_properties(&self) -> TopicProperties {
        match self {
            TypeOFSource::Digest(_, _) => TopicProperties {
                streamable: false,
                pushable: false,
                readable: true,
                immutable: true,
                has_history: false,
            },
            TypeOFSource::ForwardedQueue(q) => q.properties.clone(),
            TypeOFSource::OurQueue(_, _, props) => props.clone(),

            TypeOFSource::Compose(sc) => sc.get_properties(),
            TypeOFSource::Transformed(s, _) => s.get_properties(),
            TypeOFSource::Deref(d) => d.get_properties(),
            TypeOFSource::OtherProxied(_) => {
                TopicProperties {
                    streamable: false,
                    pushable: false,
                    readable: true,
                    immutable: false, // maybe we can say it true sometime
                    has_history: false,
                }
            }
            TypeOFSource::MountedDir(_, _, props) => props.clone(),
            TypeOFSource::MountedFile(_, _, props) => props.clone(),
            TypeOFSource::Index(s) => s.get_properties(),
            TypeOFSource::Aliased(_, _) => {
                todo!("get_properties for {self:#?} with {self:?}")
            }
            TypeOFSource::History(_) => {
                TopicProperties {
                    streamable: false,
                    pushable: false,
                    readable: true,
                    immutable: false, // maybe we can say it true sometime
                    has_history: false,
                }
            }
        }
    }
}

pub struct DataStream {
    pub channel_info: ChannelInfo,

    /// The first data (if available)
    pub first: Option<InsertNotification>,

    /// The stream (or none if no more data is coming through)
    pub stream: Option<Receiver<InsertNotification>>,

    /// handles of couroutines needed for making this happen
    pub handles: Vec<JoinHandle<()>>,
}
// use tokio::stream::StreamExt;

impl TypeOFSource {
    #[async_recursion]
    pub async fn get_data_stream(
        &self,
        presented_as: &str,
        ssa: ServerStateAccess,
    ) -> DTPSR<DataStream> {
        match self {
            TypeOFSource::Digest(_, _) => {
                todo!("get_stream for {self:#?} with {self:?}");
                let x = self.resolve_data_single(presented_as, ssa).await?;
            }
            TypeOFSource::ForwardedQueue(q) => {
                todo!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::OurQueue(topic_name, _, props) => {
                let (rx, first, channel_info) = {
                    let ss = ssa.lock().await;

                    let rx = ss.subscribe_insert_notification(topic_name);
                    let first = ss.get_last_insert(topic_name)?;
                    let oq = ss.get_queue(topic_name)?;
                    let channel_info = get_channel_info_message(oq);
                    (rx, first, channel_info)
                };
                // let stream = UnboundedReceiverStream::new(rx);

                Ok(DataStream {
                    channel_info,
                    first,
                    stream: Some(rx),
                    handles: vec![],
                })
            }

            TypeOFSource::Compose(sc) => get_stream_compose(presented_as, ssa, sc).await,
            TypeOFSource::Transformed(s, _) => {
                todo!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::Deref(d) => {
                todo!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::OtherProxied(_) => {
                todo!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::MountedDir(_, _, props) => {
                todo!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::MountedFile(_, _, props) => {
                todo!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::Index(s) => {
                todo!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::Aliased(_, _) => {
                todo!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::History(_) => {
                todo!("get_stream for {self:#?} with {self:?}")
            }
        }
    }
}

async fn get_stream_compose(
    presented_as: &str,
    ssa: ServerStateAccess,
    sc: &SourceComposition,
) -> DTPSR<DataStream> {
    let mut components = HashMap::new();
    let mut handles = Vec::new();

    for (k, v) in sc.compose.iter() {
        let mut component_stream = v.get_data_stream(presented_as, ssa.clone()).await?;
        handles.append(&mut component_stream.handles);
        components.insert(k.clone(), component_stream);
    }

    let data_stream = DataStream {
        channel_info: ChannelInfo {
            queue_created: 0,
            num_total: 0,
            newest: None,
            oldest: None,
        },
        first: None,
        stream: None,
        handles: vec![],
    };
    Ok(data_stream)
}

async fn transform(data: ResolvedData, transform: &Transforms) -> DTPSR<ResolvedData> {
    let d = match data {
        Regular(d) => d,
        NotAvailableYet(s) => return Ok(NotAvailableYet(s.clone())),
        NotFound(s) => return Ok(NotFound(s.clone())),
        ResolvedData::RawData(rd) => {
            let x = get_as_cbor(&rd)?;
            x
        }
    };
    match &transform {
        Transforms::GetInside(path) => {
            debug!("Get inside: {:?}", d);
            let inside = get_inside(vec![], &d, path);
            match inside {
                Ok(d) => Ok(ResolvedData::Regular(d)),
                Err(e) => Err(DTPSError::Other(format!("Error getting inside: {}", e))),
            }
        }
    }
}

async fn our_queue(topic_name: &TopicName, ss_mutex: ServerStateAccess) -> DTPSR<ResolvedData> {
    let ss = ss_mutex.lock().await;
    if !ss.oqs.contains_key(topic_name) {
        return Ok(NotFound(format!("No queue with name {:?}", topic_name)));
    }
    let oq = ss.oqs.get(topic_name).unwrap();

    return match oq.stored.last() {
        None => {
            let s = format!("No data in queue {:?}", topic_name);
            Ok(NotAvailableYet(s))
        }
        Some(v) => {
            let data_saved = oq.saved.get(v).unwrap();
            let content = ss.get_blob_bytes(&data_saved.digest)?;
            let raw_data = RawData::new(content, &data_saved.content_type);
            Ok(ResolvedData::RawData(raw_data))
        }
    };
}

pub const MASK_ORIGIN: bool = false;

#[async_trait]
impl GetMeta for TypeOFSource {
    async fn get_meta_index(
        &self,
        presented_url: &str,
        ss_mutex: ServerStateAccess,
    ) -> DTPSR<TopicsIndexInternal> {
        let x = self.get_meta_index_(presented_url, ss_mutex).await?;

        let root = TopicName::root();
        if !x.topics.contains_key(&root) {
            panic!("At url {presented_url}, invalid meta index for {self:?}");
        }
        Ok(x)
    }
}

fn make_relative(base: &str, url: &str) -> String {
    if base.starts_with("/") || url.starts_with("/") {
        panic!("neither should start with /: {base:?} {url:?}")
    }
    let base = url::Url::parse(&format!("http://example.org/{base}")).unwrap();
    let target = url::Url::parse(&format!("http://example.org/{url}")).unwrap();
    let rurl = base.make_relative(&target).unwrap();
    rurl
}

#[cfg(test)]
mod test {
    use crate::signals_logic::make_relative;

    #[test]
    fn t1() {
        assert_eq!(make_relative("clock5/:meta/", "clock5/"), "../");
        assert_eq!(make_relative("clock5/", "clock5/:meta/"), ":meta/");
    }
}

impl TypeOFSource {
    //noinspection RsConstantConditionIf
    async fn get_meta_index_(
        &self,
        presented_url: &str,
        ss_mutex: ServerStateAccess,
    ) -> DTPSR<TopicsIndexInternal> {
        // debug!("get_meta_index: {:?}", self);
        return match self {
            TypeOFSource::ForwardedQueue(q) => {
                let ss = ss_mutex.lock().await;
                let node_id = ss.node_id.clone();
                let the_data = ss.proxied_topics.get(&q.my_topic_name).unwrap();
                let mut topics: HashMap<TopicName, TopicRefInternal> = hashmap! {};

                let mut tr = the_data.tr_original.clone();

                let topic_url = q.my_topic_name.as_relative_url();
                let rurl = make_relative(&presented_url[1..], topic_url);

                let mut the_forwarders = the_data.reachability_we_used.forwarders.clone();

                the_forwarders.push(ForwardingStep {
                    forwarding_node: node_id.to_string(),
                    forwarding_node_connects_to: "".to_string(),

                    performance: the_data.link_benchmark_last.clone(),
                });
                let link_benchmark_total = the_data.reachability_we_used.benchmark.clone()
                    + the_data.link_benchmark_last.clone();

                if MASK_ORIGIN {
                    tr.reachability.clear();
                }
                tr.reachability.push(TopicReachabilityInternal {
                    con: TypeOfConnection::Relative(rurl.to_string(), None),
                    answering: node_id.clone(),
                    forwarders: the_forwarders,
                    benchmark: link_benchmark_total.clone(),
                });

                topics.insert(TopicName::root(), tr);
                // let n: NodeAppData = NodeAppData::from("dede");
                let index = TopicsIndexInternal {
                    // node_id: "".to_string(),
                    // node_started: 0,
                    // node_app_data: hashmap! {"here2".to_string() => n},
                    topics,
                };
                Ok(index)

                // Err(DTPSError::NotImplemented("get_meta_index for forwarded queue".to_string()))
            }
            TypeOFSource::OurQueue(topic_name, _, _) => {
                if topic_name.is_root() {
                    panic!("get_meta_index for empty topic name");
                }
                let ss = ss_mutex.lock().await;
                let node_id = ss.node_id.clone();

                let oq = ss.oqs.get(topic_name).unwrap();
                let mut tr = oq.tr.clone();

                // debug!("topic_name = {topic_name:?} presented_url = {presented_url:?}");

                // let presented_url = presented_as.as_relative_url();
                let topic_url = topic_name.as_relative_url();

                let rurl = make_relative(&presented_url[1..], topic_url);
                debug!("topic_url = {topic_url} presented = {presented_url} rulr = {rurl}");
                tr.reachability.push(TopicReachabilityInternal {
                    con: TypeOfConnection::Relative(rurl, None),
                    answering: node_id.clone(),
                    forwarders: vec![],
                    benchmark: LinkBenchmark::identity(), //ok
                });

                let mut topics: HashMap<TopicName, TopicRefInternal> = hashmap! {};
                topics.insert(TopicName::root(), tr);
                let index = TopicsIndexInternal { topics };
                Ok(index)
            }
            TypeOFSource::Compose(sc) => get_sc_meta(sc, presented_url, ss_mutex).await,
            TypeOFSource::Transformed(_, _) => Err(DTPSError::NotImplemented(
                "get_meta_index for Transformed".to_string(),
            )),
            TypeOFSource::Digest(_, _) => Err(DTPSError::NotImplemented(
                "get_meta_index for Digest".to_string(),
            )),
            TypeOFSource::Deref(_) => Err(DTPSError::NotImplemented(
                "get_meta_index for Deref".to_string(),
            )),
            TypeOFSource::OtherProxied(_) => {
                not_implemented!("OtherProxied: {self:?}")
            }
            TypeOFSource::MountedDir(_, the_path, props) => {
                // let ss = ss_mutex.lock().await;
                // let dir = ss.local_dirs.get(topic_name).unwrap();
                let d = PathBuf::from(the_path);
                if !d.exists() {
                    return Err(DTPSError::TopicNotFound(format!(
                        "MountedDir: {:?} does not exist",
                        d
                    )));
                }
                let mut topics: HashMap<TopicName, TopicRefInternal> = hashmap! {};

                debug!("MountedDir: {:?}", d);
                let inside = d.read_dir().unwrap();
                for x in inside {
                    let filename = x?.file_name().to_str().unwrap().to_string();
                    let mut tr = TopicRefInternal {
                        unique_id: "".to_string(),
                        origin_node: "".to_string(),
                        app_data: Default::default(),
                        reachability: vec![],
                        created: 0,
                        properties: props.clone(),
                        content_info: ContentInfo::generic(),
                    };
                    tr.reachability.push(TopicReachabilityInternal {
                        con: TypeOfConnection::Relative(filename.clone(), None),
                        answering: "".to_string(),
                        forwarders: vec![],
                        benchmark: LinkBenchmark::identity(), //ok
                    });

                    topics.insert(TopicName::from_components(&vec![filename]), tr);
                }

                Ok(TopicsIndexInternal {
                    // node_id: "".to_string(),
                    // node_started: 0,
                    // node_app_data: hashmap! {
                    //     "path".to_string() => NodeAppData::from(the_path),
                    // },
                    topics,
                })
            }
            TypeOFSource::MountedFile(_, _, _) => {
                not_implemented!("get_meta_index: {self:?}")
            }
            TypeOFSource::Index(_) => {
                not_implemented!("get_meta_index: {self:?}")
            }
            TypeOFSource::Aliased(_, _) => {
                not_implemented!("get_meta_index: {self:?}")
            }
            TypeOFSource::History(_) => {
                not_implemented!("get_meta_index: {self:?}")
            }
        };
    }
}

async fn get_sc_meta(
    sc: &SourceComposition,
    presented_as: &str,
    ss_mutex: ServerStateAccess,
) -> DTPSR<TopicsIndexInternal> {
    // if sc.is_root {
    //     let ss = ss_mutex.lock().await;
    //     return Ok(ss.create_topic_index());
    // }
    let mut topics: HashMap<TopicName, TopicRefInternal> = hashmap! {};
    // let mut node_app_data = HashMap::new();

    // let debug_s = String::new();
    for (prefix, inside) in sc.compose.iter() {
        let x = inside
            .get_meta_index(presented_as, ss_mutex.clone())
            .await?;

        for (a, b) in x.topics {
            let tr = b.clone();

            // tr.app_data
            //     .insert("created by get_sc_meta".to_string(), NodeAppData::from(""));

            let _url = a.as_relative_url();

            let a_with_prefix = a.add_prefix(&prefix);

            let rurl = a_with_prefix.as_relative_url();
            let _b2 = b.add_path(&rurl);

            topics.insert(a_with_prefix, tr);
        }
    }
    let properties = sc.get_properties();

    // get the minimum created time
    let created = topics.values().map(|v| v.created).min().unwrap();

    let topic_url = sc.topic_name.as_relative_url();
    let rurl = make_relative(&presented_as[1..], topic_url);

    topics.insert(
        TopicName::root(),
        TopicRefInternal {
            unique_id: sc.unique_id.clone(),
            origin_node: sc.origin_node.clone(),
            app_data: Default::default(),
            reachability: vec![TopicReachabilityInternal {
                con: Relative(rurl, None),
                answering: "".to_string(), // FIXME
                forwarders: vec![],
                benchmark: LinkBenchmark::identity(),
            }],
            created,
            properties,
            content_info: ContentInfo::generic(),
        },
    );
    let index = TopicsIndexInternal { topics };
    Ok(index)
}

async fn single_compose(
    sc: &SourceComposition,
    presented_as: &str,
    ss_mutex: ServerStateAccess,
) -> DTPSR<ResolvedData> {
    let mut result_dict: serde_cbor::value::Value = serde_cbor::value::Value::Map(BTreeMap::new());

    for (prefix, source) in &sc.compose {
        let mut the_result_to_put: &mut serde_cbor::value::Value = {
            let mut current: &mut serde_cbor::value::Value = &mut result_dict;
            for component in &prefix[..prefix.len() - 1] {
                if let serde_cbor::value::Value::Map(inside) = current {
                    let the_key = CBORText(component.clone().into());
                    if !inside.contains_key(&the_key) {
                        inside.insert(
                            the_key.clone(),
                            serde_cbor::value::Value::Map(BTreeMap::new()),
                        );
                    }
                    current = inside.get_mut(&the_key).unwrap();
                } else {
                    panic!("not a map");
                }
            }
            current
        };

        let where_to_put =
            if let serde_cbor::value::Value::Map(where_to_put) = &mut the_result_to_put {
                where_to_put
            } else {
                panic!("not a map");
            };
        let key_to_put = CBORText(prefix.last().unwrap().clone().into());
        let key_to_put2 = CBORText(format!("{}?", prefix.last().unwrap()));

        let value = source
            .resolve_data_single(presented_as, ss_mutex.clone())
            .await?;

        match value {
            Regular(x) => {
                where_to_put.insert(key_to_put, x);
            }
            NotAvailableYet(x) => {
                // TODO: do more here
                where_to_put.insert(key_to_put, CBORNull);
                where_to_put.insert(key_to_put2, CBORText(x.into()));
            }
            NotFound(_) => {}
            ResolvedData::RawData(rd) => {
                let x = get_as_cbor(&rd)?;
                where_to_put.insert(key_to_put, x);
            }
        }
    }

    Ok(Regular(result_dict))
}

#[async_recursion]
pub async fn interpret_path(
    path: &str,
    query: &HashMap<String, String>,
    referrer: &Option<String>,
    ss: &ServerState,
) -> DTPSR<TypeOFSource> {
    debug!("interpret_path: path: {}", path);
    let path_components0 = divide_in_components(&path, '/');
    let path_components = path_components0.clone();
    let ends_with_dash = path.ends_with('/');

    if path_components.contains(&"!".to_string()) {
        return DTPSError::other(format!(
            "interpret_path: Cannot have ! in path: {:?}",
            path_components
        ));
    }
    {
        let deref: String = ":deref".to_string();

        if path_components.contains(&deref) {
            let i = path_components.iter().position(|x| x == &deref).unwrap();
            let before = path_components.get(0..i).unwrap().to_vec();
            let after = path_components.get(i + 1..).unwrap().to_vec();
            debug!("interpret_path: before: {:?} after: {:?}", before, after);

            let before2 = before.join("/") + "/";
            let interpret_before = interpret_path(&before2, query, referrer, ss).await?;

            let first_part = match interpret_before {
                TypeOFSource::Compose(sc) => TypeOFSource::Deref(sc),
                _ => {
                    return DTPSError::other(format!(
                        "interpret_path: deref: before is not Compose: {:?}",
                        interpret_before
                    ));
                }
            };
            return resolve_extra_components(&first_part, &after);
        }
    }
    {
        let history_marker: String = URL_HISTORY.to_string();

        if path_components.contains(&history_marker) {
            let i = path_components
                .iter()
                .position(|x| x == &history_marker)
                .unwrap();
            let before = path_components.get(0..i).unwrap().to_vec();
            let after = path_components.get(i + 1..).unwrap().to_vec();
            debug!("interpret_path: before: {:?} after: {:?}", before, after);

            let before2 = before.join("/") + "/";
            let interpret_before = interpret_path(&before2, query, referrer, ss).await?;

            let first_part = TypeOFSource::History(Box::new(interpret_before));
            return resolve_extra_components(&first_part, &after);
        }
    }
    {
        let index_marker = REL_URL_META.to_string();
        if path_components.contains(&index_marker) {
            let i = path_components
                .iter()
                .position(|x| x == &index_marker)
                .unwrap();
            let before = path_components.get(0..i).unwrap().to_vec();
            let after = path_components.get(i + 1..).unwrap().to_vec();

            let before2 = before.join("/") + "/";
            let interpret_before = interpret_path(&before2, query, referrer, ss).await?;

            let first_part = TypeOFSource::Index(Box::new(interpret_before));
            return resolve_extra_components(&first_part, &after);
        }
    }

    if path_components.len() > 1 {
        if path_components.first().unwrap() == ":ipfs" {
            if path_components.len() != 3 {
                return DTPSError::other(format!(
                    "Wrong number of components: {:?}; expected 3",
                    path_components
                ));
            }
            let digest = path_components.get(1).unwrap();
            let content_type = path_components.get(2).unwrap();
            let content_type = content_type.replace("_", "/");
            return Ok(TypeOFSource::Digest(
                digest.to_string(),
                content_type.to_string(),
            ));
        }
    }
    {
        for (mounted_at, info) in &ss.proxied_other {
            if let Some((_, b)) = is_prefix_of(mounted_at.as_components(), &path_components) {
                let mut path_and_query = b.join("/");
                if ends_with_dash && b.len() > 0 {
                    path_and_query.push('/');
                }
                path_and_query.push_str(&utils::format_query(&query));

                let other = OtherProxied {
                    // reached_at: mounted_at.clone(),
                    path_and_query,
                    op: info.clone(),
                };
                return Ok(TypeOFSource::OtherProxied(other));
            }
        }
    }

    let all_sources = iterate_type_of_sources(&ss, true);

    resolve(&path_components, ends_with_dash, &all_sources)
}

fn resolve(
    path_components: &Vec<String>,
    ends_with_dash: bool,
    all_sources: &Vec<(TopicName, TypeOFSource)>,
) -> DTPSR<TypeOFSource> {
    let mut subtopics: Vec<(TopicName, Vec<String>, Vec<String>, TypeOFSource)> = vec![];
    let mut subtopics_vec = vec![];
    // log::debug!(" = all_sources =\n{:#?} ", all_sources);
    for (k, source) in all_sources.iter() {
        // debug!(" = k = {:?} ", k);
        // let topic_components = divide_in_components(&k, '.');
        let topic_components = k.as_components();
        subtopics_vec.push(topic_components.clone());

        if topic_components.len() == 0 {
            continue;
        }

        match is_prefix_of(&topic_components, &path_components) {
            None => {}
            Some((_, rest)) => {
                return resolve_extra_components(source, &rest);
            }
        };

        // log::debug!("k: {:?} = components = {:?} ", k, components);

        let (matched, rest) = match is_prefix_of(&path_components, &topic_components) {
            None => continue,

            Some(rest) => rest,
        };

        // debug!("pushing: {k:?}");
        subtopics.push((k.clone(), matched, rest, source.clone()));
    }

    // debug!("subtopics: {subtopics:?}");
    if subtopics.len() == 0 {
        let s = format!(
            "Cannot find a matching topic for {:?}.\nMy Topics: {:?}\n",
            path_components, subtopics_vec
        );
        error!("{}", s);
        return Err(DTPSError::TopicNotFound(s));
    }
    let origin_node = "self".to_string();
    let y = path_components.join("/");
    let unique_id = format!("{origin_node}:{y}");
    let topic_name = TopicName::from_components(path_components);
    let mut sc = SourceComposition {
        topic_name,
        is_root: false,
        compose: hashmap! {},
        unique_id,
        origin_node,
    };
    // let ss = ss_mutex.lock().await;
    for (_, _, rest, source) in subtopics {
        sc.compose.insert(rest.clone(), Box::new(source));
    }
    // log::debug!("  \n{sc:#?} ");
    Ok(TypeOFSource::Compose(sc))
}

pub fn iterate_type_of_sources(
    s: &ServerState,
    add_aliases: bool,
) -> Vec<(TopicName, TypeOFSource)> {
    let mut res: Vec<(TopicName, TypeOFSource)> = vec![];
    for (topic_name, x) in s.proxied_topics.iter() {
        let fq = ForwardedQueue {
            subscription: x.from_subscription.clone(),
            his_topic_name: x.its_topic_name.clone(),
            my_topic_name: topic_name.clone(),
            properties: x.tr_original.properties.clone(),
        };
        res.push((topic_name.clone(), TypeOFSource::ForwardedQueue(fq)));
    }

    for (topic_name, oq) in s.oqs.iter() {
        res.push((
            topic_name.clone(),
            TypeOFSource::OurQueue(
                topic_name.clone(),
                topic_name.to_components(),
                oq.tr.properties.clone(),
            ),
        ));
    }

    if add_aliases {
        let sources_no_aliases = iterate_type_of_sources(s, false);

        for (topic_name, new_topic) in s.aliases.iter() {
            let resolved = resolve(new_topic.as_components(), false, &sources_no_aliases);
            let r = match resolved {
                Ok(rr) => rr,
                Err(e) => {
                    let s1 = topic_name.as_dash_sep();
                    let s2 = new_topic.as_dash_sep();
                    error_with_info!("Cannot resolve alias of {s1} -> {s2} yet:\n{e}");
                    continue;
                }
            };
            res.push((topic_name.clone(), r));
        }
    }

    for (topic_name, oq) in s.local_dirs.iter() {
        let props = TopicProperties {
            streamable: false,
            pushable: false,
            readable: true,
            immutable: false,
            has_history: false,
        };
        res.push((
            topic_name.clone(),
            TypeOFSource::MountedDir(topic_name.clone(), oq.local_dir.clone(), props),
        ));
    }
    // sort res by length of topic_name
    res.sort_by(|a, b| a.0.as_components().len().cmp(&b.0.as_components().len()));
    // now reverse
    res.reverse();
    res
}

fn resolve_extra_components(source: &TypeOFSource, rest: &Vec<String>) -> DTPSR<TypeOFSource> {
    let mut cur = source.clone();
    for a in rest.iter() {
        cur = context!(
            cur.get_inside(&a),
            "interpret_path: cannot match for {a:?} rest: {rest:?}",
        )?;
    }
    Ok(cur)
}
