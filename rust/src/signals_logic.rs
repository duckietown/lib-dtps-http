use std::{
    cmp::{
        max,
        min,
    },
    collections::{
        BTreeMap,
        HashMap,
        HashSet,
    },
    fmt::Debug,
    path::PathBuf,
};

use anyhow::Context;
use async_recursion::async_recursion;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Local;
use futures::StreamExt;
use json_patch::{
    patch,
    Patch,
    PatchOperation,
};
use maplit::hashmap;
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};
use serde_cbor::{
    Value as CBORValue,
    Value::{
        Null as CBORNull,
        Text as CBORText,
    },
};
use tokio::{
    sync::broadcast::{
        error::RecvError,
        Receiver,
        Sender,
    },
    task::JoinHandle,
};

use crate::{
    client::get_rawdata_status,
    context,
    debug_with_info,
    divide_in_components,
    error_with_info,
    get_channel_info_message,
    get_dataready,
    get_inside,
    get_rawdata,
    is_prefix_of,
    merge_clocks,
    not_implemented,
    parse_url_ext,
    unescape_json_patch,
    utils,
    warn_with_info,
    ChannelInfo,
    Clocks,
    ContentInfo,
    DTPSError,
    DataReady,
    DataSaved,
    ForwardingStep,
    InsertNotification,
    LinkBenchmark,
    OtherProxyInfo,
    ProxyJob,
    RawData,
    ResolvedData,
    ResolvedData::{
        NotAvailableYet,
        NotFound,
        Regular,
    },
    ServerState,
    ServerStateAccess,
    TopicName,
    TopicProperties,
    TopicReachabilityInternal,
    TopicRefAdd,
    TopicRefInternal,
    TopicsIndexInternal,
    TopicsIndexWire,
    TypeOfConnection,
    TypeOfConnection::Relative,
    CONTENT_TYPE_DTPS_INDEX_CBOR,
    CONTENT_TYPE_TOPIC_HISTORY_CBOR,
    DTPSR,
    REL_URL_META,
    TOPIC_PROXIED,
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
    pub compose: HashMap<Vec<String>, Box<TypeOFSource>>,
    pub unique_id: String,
    pub origin_node: String,
}

impl DataProps for SourceComposition {
    fn get_properties(&self) -> TopicProperties {
        let pushable = false;

        let mut immutable = true;
        let mut streamable = false;
        let mut readable = true;
        let has_history = false;
        let patchable = true;

        for (_k, v) in self.compose.iter() {
            let p = v.get_properties();

            // immutable: ALL immutable
            immutable = immutable && p.immutable;
            // streamable: ANY streamable
            streamable = streamable || p.streamable;
            // readable: ALL readable
            readable = readable && p.readable;
            // patchable: ANY patchable
            // patchable = patchable || p.patchable;
        }

        TopicProperties {
            streamable,
            pushable,
            readable,
            immutable,
            has_history,
            patchable,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ForwardedQueue {
    pub subscription: TopicName,
    pub his_topic_name: TopicName,
    pub my_topic_name: TopicName,
    pub properties: TopicProperties,
}

#[derive(Debug, Clone)]
pub enum TypeOFSource {
    /// A remote queue
    ForwardedQueue(ForwardedQueue),
    /// Our queue
    OurQueue(TopicName, TopicProperties),

    MountedDir(TopicName, String, TopicProperties),
    MountedFile {
        topic_name: TopicName,
        filename: String,
        properties: TopicProperties,
    },
    Compose(SourceComposition),
    Transformed(Box<TypeOFSource>, Transforms),
    Digest(String, String),
    Deref(SourceComposition),
    Index(Box<TypeOFSource>),
    Aliased(TopicName, Option<Box<TypeOFSource>>),
    History(Box<TypeOFSource>),

    OtherProxied(OtherProxied),
}

#[async_trait]
pub trait Patchable {
    async fn patch(&self, presented_as: &str, ss_mutex: ServerStateAccess, patch: &Patch) -> DTPSR<()>;
}

#[async_trait]
pub trait ResolveDataSingle {
    async fn resolve_data_single(&self, presented_as: &str, ss_mutex: ServerStateAccess) -> DTPSR<ResolvedData>;
}

impl TypeOFSource {
    pub fn get_inside(&self, s: &str) -> DTPSR<Self> {
        match self {
            TypeOFSource::ForwardedQueue(_q) => Ok(TypeOFSource::Transformed(
                Box::new(self.clone()),
                Transforms::GetInside(vec![s.to_string()]),
            )),
            TypeOFSource::OurQueue(..) => {
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
                        Ok(TypeOFSource::MountedFile {
                            topic_name: topic_name.clone(),
                            filename: inside.to_str().unwrap().to_string(),

                            properties: props.clone(),
                        })
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
                Ok(TypeOFSource::Transformed(source.clone(), transform.get_inside(s)))
            }

            TypeOFSource::MountedFile { .. } => {
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
            TypeOFSource::Deref(_c) => Ok(TypeOFSource::Transformed(
                Box::new(self.clone()),
                Transforms::GetInside(vec![s.to_string()]),
            )),
            TypeOFSource::OtherProxied(_) => {
                not_implemented!("get_inside for {self:#?} with {s:?}")
            }
            TypeOFSource::Aliased(_, _) => Ok(TypeOFSource::Transformed(
                Box::new(self.clone()),
                Transforms::GetInside(vec![s.to_string()]),
            )),
            TypeOFSource::History(_) => Ok(TypeOFSource::Transformed(
                Box::new(self.clone()),
                Transforms::GetInside(vec![s.to_string()]),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OtherProxied {
    path_and_query: String,
    op: OtherProxyInfo,
}

#[async_trait]
pub trait GetMeta {
    async fn get_meta_index(&self, presented_as: &str, ss_mutex: ServerStateAccess) -> DTPSR<TopicsIndexInternal>;
}

pub trait DataProps {
    fn get_properties(&self) -> TopicProperties;
}

#[async_trait]
impl ResolveDataSingle for TypeOFSource {
    async fn resolve_data_single(&self, presented_as: &str, ss_mutex: ServerStateAccess) -> DTPSR<ResolvedData> {
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

                let (status, rd) = get_rawdata_status(use_url).await?;
                if status == 209 {
                    let msg = "This topic does not have any data yet".to_string();
                    Ok(ResolvedData::NotAvailableYet(msg))
                } else {
                    Ok(ResolvedData::RawData(rd))
                }
            }
            TypeOFSource::OurQueue(q, _) => resolve_our_queue(q, ss_mutex).await,
            TypeOFSource::Compose(_sc) => {
                let index = self.get_meta_index(presented_as, ss_mutex).await?;
                // debug_with_info!("Compose index intenral:\n {:#?}", index);
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
                let data = source.resolve_data_single(presented_as, ss_mutex.clone()).await?;
                transform(data, transforms)
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
                    // debug_with_info!("Compose index intenral:\n {:#?}", index);
                    let to_wire = index.to_wire(None);

                    // convert to cbor
                    let cbor_bytes = serde_cbor::to_vec(&to_wire).unwrap();
                    let raw_data = RawData {
                        content: Bytes::from(cbor_bytes),
                        content_type: CONTENT_TYPE_DTPS_INDEX_CBOR.to_string(),
                    };
                    return Ok(ResolvedData::RawData(raw_data));
                }
                not_implemented!("MountedDir:\n{self:#?}")
            }
            TypeOFSource::MountedFile { filename, .. } => {
                let data = std::fs::read(filename)?;
                let content_type = mime_guess::from_path(&filename)
                    .first()
                    .unwrap_or(mime::APPLICATION_OCTET_STREAM);
                let rd = RawData::new(data, content_type);

                Ok(ResolvedData::RawData(rd))
            }
            TypeOFSource::Index(inside) => {
                let x = inside.get_meta_index(presented_as, ss_mutex).await?;
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
                not_implemented!("resolve_data_single for:\n{self:#?}")
            }
            TypeOFSource::History(s) => {
                let x: &TypeOFSource = s;
                match x {
                    TypeOFSource::OurQueue(topic, _) => {
                        let ss = ss_mutex.lock().await;
                        let q = ss.get_queue(topic)?;
                        let mut available: HashMap<usize, DataReady> = HashMap::new();
                        for index in q.stored.iter() {
                            let s = q.saved.get(index).unwrap();
                            available.insert(s.index, get_dataready(s));
                        }
                        let history = available;
                        let bytes = serde_cbor::to_vec(&history).unwrap();
                        return Ok(ResolvedData::RawData(RawData::new(
                            bytes,
                            CONTENT_TYPE_TOPIC_HISTORY_CBOR,
                        )));
                    }
                    TypeOFSource::Compose(sc) => {
                        if sc.topic_name.is_root() {
                            let ds2 = TypeOFSource::History(Box::new(TypeOFSource::OurQueue(
                                TopicName::root(),
                                TopicProperties::rw(),
                            )));
                            return ds2.resolve_data_single(presented_as, ss_mutex).await;
                        }
                        not_implemented!("resolve_data_single for:\n{self:#?}")
                    }
                    _ => {
                        not_implemented!("resolve_data_single for:\n{self:#?}")
                    }
                }
            }
        }
    }
}

pub async fn resolve_proxied(op: &OtherProxied) -> DTPSR<ResolvedData> {
    let con0 = op.op.con.clone();

    let rest = &op.path_and_query;

    let con = con0.join(&rest)?;

    debug_with_info!("Proxied: {:?} -> {:?}", con0, con);

    let rd = get_rawdata(&con).await?;

    Ok(ResolvedData::RawData(rd))
}

impl DataProps for TypeOFSource {
    fn get_properties(&self) -> TopicProperties {
        match self {
            TypeOFSource::Digest(..) => TopicProperties {
                streamable: false,
                pushable: false,
                readable: true,
                immutable: true,
                has_history: false,
                patchable: false,
            },
            TypeOFSource::ForwardedQueue(q) => q.properties.clone(),
            TypeOFSource::OurQueue(_, props) => props.clone(),

            TypeOFSource::Compose(sc) => sc.get_properties(),
            TypeOFSource::Transformed(s, _) => s.get_properties(), // ok but when do we do the history?
            TypeOFSource::Deref(d) => d.get_properties(),
            TypeOFSource::OtherProxied(_) => {
                TopicProperties {
                    streamable: false,
                    pushable: false,
                    readable: true,
                    immutable: false, // maybe we can say it true sometime
                    has_history: false,
                    patchable: false,
                }
            }
            TypeOFSource::MountedDir(_, _, props) => props.clone(),
            TypeOFSource::MountedFile { properties, .. } => properties.clone(),
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
                    patchable: false,
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
    pub stream: Option<tokio::sync::broadcast::Receiver<InsertNotification>>,

    /// handles of couroutines needed for making this happen
    pub handles: Vec<JoinHandle<DTPSR<()>>>,
}
// use tokio::stream::StreamExt;

async fn get_data_stream_from_url(con: &TypeOfConnection) -> DTPSR<DataStream> {
    todo!("not implemented");
}

impl TypeOFSource {
    #[async_recursion]
    pub async fn get_data_stream(&self, presented_as: &str, ssa: ServerStateAccess) -> DTPSR<DataStream> {
        match self {
            TypeOFSource::Digest(_, _) => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::ForwardedQueue(q) => {
                // not_implemented!("get_stream for {self:#?} with {self:?}")
                let use_url = {
                    let ss = ssa.lock().await;
                    ss.proxied_topics.get(&q.my_topic_name).unwrap().data_url.clone()
                };
                get_data_stream_from_url(&use_url).await
            }
            TypeOFSource::OurQueue(topic_name, ..) => {
                let (rx, first, channel_info) = {
                    let ss = ssa.lock().await;

                    let rx = ss.subscribe_insert_notification(topic_name)?;
                    let first = ss.get_last_insert(topic_name)?;
                    let oq = ss.get_queue(topic_name)?;
                    let channel_info = get_channel_info_message(oq);
                    (rx, first, channel_info)
                };

                Ok(DataStream {
                    channel_info,
                    first,
                    stream: Some(rx),
                    handles: vec![],
                })
            }

            TypeOFSource::Compose(sc) => get_stream_compose_meta(presented_as, ssa, sc).await,
            TypeOFSource::Transformed(tos, tr) => {
                get_stream_transform(presented_as, ssa, tos, tr).await
                // not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::Deref(d) => get_stream_compose_data(presented_as, ssa, d).await,
            TypeOFSource::OtherProxied(_) => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::MountedDir(_, _, _props) => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::MountedFile { .. } => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::Index(_s) => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::Aliased(_, _) => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::History(_) => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ActualUpdate {
    pub component: Vec<String>,
    pub data: ResolvedData,
    pub clocks: Clocks,
}

#[derive(Debug, Clone)]
pub enum SingleUpdates {
    Update(ActualUpdate),
    Finished(Vec<String>),
}

async fn put_together(
    mut first: CBORValue, // BTreeMap<CBORValue, CBORValue>,
    mut clocks0: Clocks,
    mut active_components: HashSet<Vec<String>>,
    mut rx: tokio::sync::broadcast::Receiver<SingleUpdates>,
    tx_out: tokio::sync::broadcast::Sender<InsertNotification>,
) -> DTPSR<()> {
    if let CBORValue::Map(..) = first {
    } else {
        return Err(DTPSError::Other(format!("First value is not a map")));
    };
    let mut index = 1;
    loop {
        let msg = rx.recv().await?;
        match msg {
            SingleUpdates::Update(ActualUpdate {
                component,
                data,
                clocks,
            }) => {
                if !active_components.contains(&component) {
                    warn_with_info!("Received update for inactive component: {:?}", component);
                    continue;
                }
                clocks0 = merge_clocks(&clocks0, &clocks);
                putinside(&mut first, &component, data)?;
                let rd = RawData::from_cbor_value(&first)?;
                let time_inserted = Local::now().timestamp_nanos();
                let data_saved = DataSaved {
                    origin_node: "".to_string(),
                    unique_id: "".to_string(),
                    index,
                    time_inserted,
                    clocks: clocks0.clone(),
                    content_type: rd.content_type.clone(),
                    content_length: rd.content.len(),
                    digest: rd.digest().clone(),
                };
                let notification = InsertNotification {
                    data_saved,
                    raw_data: rd,
                };
                if tx_out.receiver_count() > 0 {
                    tx_out.send(notification).unwrap();
                }
                index += 1;
            }
            SingleUpdates::Finished(which) => {
                if !active_components.contains(&which) {
                    warn_with_info!("Received update for inactive component: {:?}", which);
                } else {
                    debug_with_info!("Finished  for active component: {:?}", which);
                    active_components.remove(&which);
                    if active_components.is_empty() {
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn listen_to_updates(
    component: Vec<String>,
    mut rx: tokio::sync::broadcast::Receiver<InsertNotification>,
    tx: tokio::sync::broadcast::Sender<SingleUpdates>,
) -> DTPSR<()> {
    loop {
        match rx.recv().await {
            Ok(m) => {
                if tx.receiver_count() > 0 {
                    tx.send(SingleUpdates::Update(ActualUpdate {
                        component: component.clone(),
                        data: ResolvedData::RawData(m.raw_data),
                        clocks: m.data_saved.clocks,
                    }))
                    .unwrap();
                }
            }
            Err(e) => match e {
                RecvError::Closed => {
                    break;
                }
                RecvError::Lagged(e) => {
                    warn_with_info!("Lagged: {e}");
                }
            },
        }
    }
    tx.send(SingleUpdates::Finished(component)).unwrap();
    Ok(())
}

async fn filter_stream<T, U, F, G>(
    mut receiver: Receiver<T>,
    sender: Sender<U>,
    f: F,
    filter_same: bool,
    mut last: Option<U>,
    are_they_same: G,
) -> DTPSR<()>
where
    F: Fn(T) -> DTPSR<U>,
    T: Send + Clone + Debug,
    U: Send + Clone + Debug,
    G: Fn(&U, &U) -> bool,
{
    loop {
        match receiver.recv().await {
            Ok(m) => {
                let u = f(m)?;
                if filter_same {
                    if let Some(a) = &last {
                        if are_they_same(a, &u) {
                            continue;
                        }
                    }
                    last = Some(u.clone());
                }
                if sender.receiver_count() != 0 {
                    match sender.send(u) {
                        Ok(_) => {}
                        Err(e) => {
                            warn_with_info!("Cannot send: {e:?}");
                        }
                    }
                }
            }
            Err(e) => match e {
                RecvError::Closed => {
                    break;
                }
                RecvError::Lagged(e) => {
                    warn_with_info!("Lagged: {e}");
                }
            },
        }
    }
    Ok(())
}

// fn adapt_cbor_map<'a, F, T1, T2>(in1: &InsertNotification, f: F, content_type: String,
//                              unique_id_suffix: String) -> DTPSR<InsertNotification>
//     where F: FnOnce(&T1) -> T2,
//           T1: Clone + Debug + Deserialize<'a>,
//           T2: Serialize  + Clone + Debug ,
// {
//     let in1 = in1.clone();
//     let rd0 = in1.raw_data.clone();
//     let content: Vec<u8> = in1.raw_data.content.to_vec().clone();
//     let rd = {
//         // let v1 = serde_cbor::from_slice(&content)?;
//         let v1 = rd0.interpret::<T1>()?;
//         let v1_ = v1.clone();
//         let v2 = f(&v1_);
//         // let v2 = match f(v1) {
//         //     Ok(v) => v,
//         //     Err(e) => {
//         //         return Err(DTPSError::Other(format!("Cannot run function")));
//         //     }
//         // };
//         not_implemented!();
//         // let cbor_bytes = serde_cbor::to_vec(&v2)?;
//         // let rd = RawData::new(&cbor_bytes, content_type);
//         // rd.clone()
//     };
//     not_implemented!("adapt_cbor_map")
//     //
//     // let ds = in1.data_saved.clone();
//     //
//     // let data_saved = DataSaved {
//     //     origin_node: ds.origin_node.clone(),
//     //     unique_id: format!("{}:{}", ds.unique_id, unique_id_suffix),
//     //     index: ds.index,
//     //     time_inserted: ds.time_inserted,
//     //     clocks: ds.clocks.clone(),
//     //     content_type: rd.content_type.clone(),
//     //     content_length: rd.content.len(),
//     //     digest: rd.digest().clone(),
//     // };
//     // Ok(InsertNotification {
//     //     data_saved,
//     //     raw_data: rd,
//     // })
// }

fn filter_index(data1: &TopicsIndexWire, prefix: TopicName, presented_as: String) -> DTPSR<TopicsIndexWire> {
    let con = TypeOfConnection::Relative(presented_as, None);
    let data1 = TopicsIndexInternal::from_wire(&data1, &con);
    let mut data2 = TopicsIndexInternal::default();
    for (k, v) in data1.topics.iter() {
        match is_prefix_of(prefix.as_components(), k.as_components()) {
            None => {}
            Some((_matched, extra_elements)) => {
                let tn2 = TopicName::from_components(&extra_elements);
                data2.topics.insert(tn2, v.clone());
            }
        }
    }
    let data2 = data2.to_wire(None); // XXX

    Ok(data2)
}

fn filter_func(
    in1: InsertNotification,
    prefix: TopicName,
    presented_as: String,
    unique_id: String,
) -> DTPSR<InsertNotification> {
    // let f = |x: &TopicsIndexWire| filter_index(x, prefix, presented_as).unwrap();

    let v1 = in1.raw_data.interpret::<TopicsIndexWire>()?;
    let v2 = filter_index(&v1, prefix.clone(), presented_as.clone())?;
    let rd = RawData::represent_as_cbor_ct(v2, CONTENT_TYPE_DTPS_INDEX_CBOR)?;
    //
    // adapt_cbor_map(&in1, f, CONTENT_TYPE_DTPS_INDEX_CBOR.to_string(), "filtered".to_string())
    // not_implemented!("filter_func")
    let ds = &in1.data_saved;
    let data_saved = DataSaved {
        origin_node: ds.origin_node.clone(),
        unique_id,
        index: ds.index,
        time_inserted: ds.time_inserted,
        clocks: ds.clocks.clone(),
        content_type: rd.content_type.clone(),
        content_length: rd.content.len(),
        digest: rd.digest().clone(),
    };
    Ok(InsertNotification {
        data_saved,
        raw_data: rd,
    })
}

async fn transform_for(
    receiver: Receiver<InsertNotification>,
    out_stream_sender: Sender<InsertNotification>,
    prefix: TopicName,
    presented_as: String,
    unique_id: String,
    first: Option<InsertNotification>,
) -> DTPSR<()> {
    let compare = |a: &InsertNotification, b: &InsertNotification| a.raw_data == b.raw_data;
    let f = |in1: InsertNotification| filter_func(in1, prefix.clone(), presented_as.clone(), unique_id.clone());
    filter_stream(receiver, out_stream_sender, f, true, first, compare).await
}

#[async_recursion]
async fn get_stream_compose_meta(
    presented_as: &str,
    ssa: ServerStateAccess,
    sc: &SourceComposition,
) -> DTPSR<DataStream> {
    let tn = TopicName::root();
    let ds0 = TypeOFSource::OurQueue(tn.clone(), TopicProperties::rw());
    let stream0 = ds0.get_data_stream(presented_as, ssa.clone()).await?;
    let mut handles = stream0.handles;
    let receiver = stream0.stream.unwrap();
    let (out_stream_sender, out_stream_recv) = tokio::sync::broadcast::channel(1024);

    let unique_id = sc.unique_id.clone();

    let in0 = filter_func(
        stream0.first.unwrap(),
        sc.topic_name.clone(),
        presented_as.to_string(),
        unique_id.clone(),
    )?;

    let future = transform_for(
        receiver,
        out_stream_sender,
        sc.topic_name.clone(),
        presented_as.to_string(),
        unique_id.clone(),
        Some(in0.clone()),
    );
    // let handle1 = tokio::spawn(show_errors(
    //     Some(ssa.clone()),
    //     "receiver".to_string(), future,
    // ));
    handles.push(tokio::spawn(future));

    let queue_created: i64 = Local::now().timestamp_nanos();
    let num_total = 1;

    let data_stream = DataStream {
        channel_info: ChannelInfo {
            queue_created,
            num_total,
            newest: None,
            oldest: None,
        },
        first: Some(in0),
        stream: Some(out_stream_recv),
        handles,
    };
    Ok(data_stream)
}

fn filter_transform(in1: InsertNotification, t: &Transforms, unique_id: String) -> DTPSR<InsertNotification> {
    let v1 = in1.raw_data.get_as_cbor()?;
    let v2 = t.apply(v1)?;
    let rd = RawData::from_cbor_value(&v2)?;

    let ds = &in1.data_saved;
    let data_saved = DataSaved {
        origin_node: ds.origin_node.clone(),
        unique_id,
        index: ds.index,
        time_inserted: ds.time_inserted,
        clocks: ds.clocks.clone(),
        content_type: rd.content_type.clone(),
        content_length: rd.content.len(),
        digest: rd.digest().clone(),
    };
    // FIXME: need to save the blob
    Ok(InsertNotification {
        data_saved,
        raw_data: rd,
    })
}

async fn apply_transformer(
    receiver: Receiver<InsertNotification>,
    out_stream_sender: Sender<InsertNotification>,
    transform: Transforms,
    unique_id: String,
    first: Option<InsertNotification>,
) -> DTPSR<()> {
    let are_they_same = |a: &InsertNotification, b: &InsertNotification| a.raw_data == b.raw_data;
    let f = |in1: InsertNotification| filter_transform(in1, &transform, unique_id.clone());
    filter_stream(receiver, out_stream_sender, f, true, first, are_they_same).await
}

#[async_recursion]
async fn get_stream_transform(
    presented_as: &str,
    ssa: ServerStateAccess,
    ds0: &TypeOFSource,
    transform: &Transforms,
) -> DTPSR<DataStream> {
    let stream0 = ds0.get_data_stream(presented_as, ssa.clone()).await?;
    let mut handles = stream0.handles;
    let receiver = stream0.stream.unwrap();
    let (out_stream_sender, out_stream_recv) = tokio::sync::broadcast::channel(1024);

    let unique_id = "XXX".to_string();

    let in0 = filter_transform(stream0.first.unwrap(), transform, unique_id.clone())?;

    let future = apply_transformer(
        receiver,
        out_stream_sender,
        transform.clone(),
        unique_id.clone(),
        Some(in0.clone()),
    );
    handles.push(tokio::spawn(future));

    let queue_created: i64 = Local::now().timestamp_nanos();
    let num_total = 1;

    let data_stream = DataStream {
        channel_info: ChannelInfo {
            queue_created,
            num_total,
            newest: None,
            oldest: None,
        },
        first: Some(in0),
        stream: Some(out_stream_recv),
        handles,
    };
    Ok(data_stream)
}

#[async_recursion]
async fn get_stream_compose_data(
    presented_as: &str,
    ssa: ServerStateAccess,
    sc: &SourceComposition,
) -> DTPSR<DataStream> {
    // let mut components = HashMap::new();
    let mut handles = Vec::new();
    let mut queue_created: i64 = Local::now().timestamp_nanos();
    let mut num_total = 0;
    let mut time_inserted = 0;
    let clocks0 = Clocks::default();
    let mut first = serde_cbor::value::Value::Map(BTreeMap::new());

    let mut components_active = HashSet::new();
    let mut components_inactive = HashSet::new();
    let (tx, rx) = tokio::sync::broadcast::channel(1024);
    let (out_stream_sender, out_stream_recv) = tokio::sync::broadcast::channel(1024);

    for (k, v) in sc.compose.iter() {
        let ts: &TypeOFSource = v;
        let mut cs = match ts.get_data_stream(presented_as, ssa.clone()).await {
            Ok(x) => x,
            Err(e) => match e {
                DTPSError::NotImplemented(_) => {
                    warn_with_info!(
                        "Not implemented get_data_stream() for component {:?}:\n{:?}\n{:?}",
                        k,
                        v,
                        e
                    );
                    continue;
                }
                _ => {
                    error_with_info!("Error while creating data stream: {:?}", e);
                    return Err(e);
                }
            },
        };
        queue_created = min(queue_created, cs.channel_info.queue_created);
        num_total = max(num_total, cs.channel_info.num_total);

        handles.append(&mut cs.handles);

        match cs.stream {
            None => {
                components_inactive.insert(k.clone());
            }
            Some(st) => {
                components_active.insert(k.clone());
                // let mut st = st.clone();
                let h = tokio::spawn(listen_to_updates(k.clone(), st, tx.clone()));
                handles.push(h);
            }
        }

        match &cs.first {
            None => {
                let value = ResolvedData::Regular(CBORNull);
                putinside(&mut first, k, value)?;
            }
            Some(sd) => {
                time_inserted = max(time_inserted, sd.data_saved.time_inserted);
                let resolved_data = ResolvedData::RawData(sd.raw_data.clone());
                putinside(&mut first, k, resolved_data)?;
            }
        }
    }

    let first_val = crate::RawData::from_cbor_value(&first)?;
    let data_saved = DataSaved {
        origin_node: "".to_string(),
        unique_id: "".to_string(),
        index: 0,
        time_inserted,
        clocks: clocks0.clone(),
        content_type: first_val.content_type.clone(),
        content_length: first_val.content.len(),
        digest: first_val.digest().clone(),
    };
    let handle_merge = tokio::spawn(put_together(
        first.clone(),
        clocks0,
        components_active,
        rx,
        out_stream_sender,
    ));
    handles.push(handle_merge);

    let data_stream = DataStream {
        channel_info: ChannelInfo {
            queue_created,
            num_total,
            newest: None,
            oldest: None,
        },
        first: Some(InsertNotification {
            data_saved,
            raw_data: first_val,
        }),
        stream: Some(out_stream_recv),
        handles,
    };
    Ok(data_stream)
}
fn transform(data: ResolvedData, transform: &Transforms) -> DTPSR<ResolvedData> {
    let d = match data {
        Regular(d) => d,
        NotAvailableYet(s) => return Ok(NotAvailableYet(s.clone())),
        NotFound(s) => return Ok(NotFound(s.clone())),
        ResolvedData::RawData(rd) => rd.get_as_cbor()?,
    };
    Ok(ResolvedData::Regular(transform.apply(d)?))
}

impl Transforms {
    fn apply(&self, data: CBORValue) -> DTPSR<CBORValue> {
        match self {
            Transforms::GetInside(path) => {
                // debug_with_info!("Get inside: {:?}", d);
                let inside = context!(get_inside(vec![], &data, path), "Error getting inside: {path:?}")?;
                Ok(inside)
            }
        }
    }
}

async fn resolve_our_queue(topic_name: &TopicName, ss_mutex: ServerStateAccess) -> DTPSR<ResolvedData> {
    let ss = ss_mutex.lock().await;
    if !ss.oqs.contains_key(topic_name) {
        return Ok(NotFound(format!("No queue with name {:?}", topic_name.as_dash_sep())));
    }
    let oq = ss.oqs.get(topic_name).unwrap();

    return match oq.stored.last() {
        None => {
            let s = format!("No data in queue {:?}", topic_name.as_dash_sep());
            Ok(NotAvailableYet(s))
        }
        Some(v) => {
            let data_saved = oq.saved.get(v).unwrap();
            let content = context!(
                ss.get_blob_bytes(&data_saved.digest),
                "Cannot get blob bytes for topic {:?}:\n {data_saved:#?}",
                topic_name.as_dash_sep(),
            )?;
            let raw_data = RawData::new(content, &data_saved.content_type);
            // debug_with_info!(" {topic_name:?} -> {raw_data:?}");
            Ok(ResolvedData::RawData(raw_data))
        }
    };
}

pub const MASK_ORIGIN: bool = false;

#[async_trait]
impl GetMeta for TypeOFSource {
    async fn get_meta_index(&self, presented_url: &str, ss_mutex: ServerStateAccess) -> DTPSR<TopicsIndexInternal> {
        let x = self.get_meta_index_(presented_url, ss_mutex).await?;

        let root = TopicName::root();
        if !x.topics.contains_key(&root) {
            panic!("At url {presented_url}, invalid meta index for {self:?}");
        }
        Ok(x)
    }
}

fn make_relative(base: &str, url: &str) -> String {
    // if base.starts_with("/") || url.starts_with("/") {
    //     return DTPSError::invalid_input!("neither should start with /: {base:?} {url:?}")
    // }
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
    async fn get_meta_index_(&self, presented_url: &str, ss_mutex: ServerStateAccess) -> DTPSR<TopicsIndexInternal> {
        // debug_with_info!("get_meta_index: {:?}", self);
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
                let link_benchmark_total =
                    the_data.reachability_we_used.benchmark.clone() + the_data.link_benchmark_last.clone();

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
            TypeOFSource::OurQueue(topic_name, _) => {
                // if topic_name.is_root() {
                //     panic!("get_meta_index for empty topic name");
                // }
                let ss = ss_mutex.lock().await;
                let node_id = ss.node_id.clone();

                let oq = ss.oqs.get(topic_name).unwrap();
                let mut tr = oq.tr.clone();

                // debug_with_info!("topic_name = {topic_name:?} presented_url = {presented_url:?}");

                // let presented_url = presented_as.as_relative_url();
                let topic_url = topic_name.as_relative_url();

                let rurl = make_relative(&presented_url[1..], topic_url);
                // debug_with_info!("topic_url = {topic_url} presented = {presented_url} rulr = {rurl}");
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
            TypeOFSource::Transformed(_, _) => {
                Err(DTPSError::NotImplemented("get_meta_index for Transformed".to_string()))
            }
            TypeOFSource::Digest(_, _) => Err(DTPSError::NotImplemented("get_meta_index for Digest".to_string())),
            TypeOFSource::Deref(_) => Err(DTPSError::NotImplemented("get_meta_index for Deref".to_string())),
            TypeOFSource::OtherProxied(_) => {
                not_implemented!("OtherProxied: {self:?}")
            }
            TypeOFSource::MountedDir(_, the_path, props) => {
                // let ss = ss_mutex.lock().await;
                // let dir = ss.local_dirs.get(topic_name).unwrap();
                let d = PathBuf::from(the_path);
                if !d.exists() {
                    return Err(DTPSError::TopicNotFound(format!("MountedDir: {:?} does not exist", d)));
                }
                let mut topics: HashMap<TopicName, TopicRefInternal> = hashmap! {};

                debug_with_info!("MountedDir: {:?}", d);
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
            TypeOFSource::MountedFile { .. } => {
                not_implemented!("get_meta_index: {self:?}")
            }
            TypeOFSource::Index(..) => {
                not_implemented!("get_meta_index: {self:?}")
            }
            TypeOFSource::Aliased(..) => {
                not_implemented!("get_meta_index: {self:?}")
            }
            TypeOFSource::History(..) => {
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
    let mut topics: HashMap<TopicName, TopicRefInternal> = hashmap! {};

    for (prefix, inside) in sc.compose.iter() {
        let x = inside.get_meta_index(presented_as, ss_mutex.clone()).await?;

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

fn get_result_to_put(result_dict: &mut serde_cbor::value::Value, prefix: Vec<String>) -> &mut serde_cbor::value::Value {
    let mut current: &mut serde_cbor::value::Value = result_dict;
    for component in &prefix[..prefix.len() - 1] {
        if let serde_cbor::value::Value::Map(inside) = current {
            let the_key = CBORText(component.clone().into());
            if !inside.contains_key(&the_key) {
                inside.insert(the_key.clone(), serde_cbor::value::Value::Map(BTreeMap::new()));
            }
            current = inside.get_mut(&the_key).unwrap();
        } else {
            panic!("not a map");
        }
    }
    current
}

fn putinside(result_dict: &mut serde_cbor::value::Value, prefix: &Vec<String>, what: ResolvedData) -> DTPSR<()> {
    let mut the_result_to_put = get_result_to_put(result_dict, prefix.clone());

    let where_to_put = if let serde_cbor::value::Value::Map(where_to_put) = &mut the_result_to_put {
        where_to_put
    } else {
        panic!("not a map");
    };

    let key_to_put = CBORText(prefix.last().unwrap().clone().into());
    let key_to_put2 = CBORText(format!("{}?", prefix.last().unwrap()));

    match what {
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
            let prefix_str = prefix.join("/");
            let x = context!(
                rd.get_as_cbor(),
                "Cannot get data as cbor for component {prefix_str:#?}\n{rd:#?}"
            )?;
            where_to_put.insert(key_to_put, x);
        }
    }
    Ok(())
}

async fn single_compose(
    sc: &SourceComposition,
    presented_as: &str,
    ss_mutex: ServerStateAccess,
) -> DTPSR<ResolvedData> {
    let mut result_dict: serde_cbor::value::Value = serde_cbor::value::Value::Map(BTreeMap::new());

    for (prefix, source) in &sc.compose {
        let ss_i = ss_mutex.clone();
        let value = source.resolve_data_single(presented_as, ss_i).await?;
        putinside(&mut result_dict, prefix, value)?;
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
    // debug_with_info!("interpret_path: path: {}", path);
    let path_components0 = divide_in_components(&path, '/');
    let path_components = path_components0.clone();
    let ends_with_dash = path.ends_with('/');

    if path_components.contains(&"!".to_string()) {
        return DTPSError::other(format!("interpret_path: Cannot have ! in path: {:?}", path_components));
    }
    {
        let deref: String = ":deref".to_string();

        if path_components.contains(&deref) {
            let i = path_components.iter().position(|x| x == &deref).unwrap();
            let before = path_components.get(0..i).unwrap().to_vec();
            let after = path_components.get(i + 1..).unwrap().to_vec();
            debug_with_info!("interpret_path: before: {:?} after: {:?}", before, after);

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
            let i = path_components.iter().position(|x| x == &history_marker).unwrap();
            let before = path_components.get(0..i).unwrap().to_vec();
            let after = path_components.get(i + 1..).unwrap().to_vec();
            debug_with_info!("interpret_path: before: {:?} after: {:?}", before, after);

            let before2 = before.join("/") + "/";
            let interpret_before = interpret_path(&before2, query, referrer, ss).await?;

            let first_part = TypeOFSource::History(Box::new(interpret_before));
            return resolve_extra_components(&first_part, &after);
        }
    }
    {
        let index_marker = REL_URL_META.to_string();
        if path_components.contains(&index_marker) {
            let i = path_components.iter().position(|x| x == &index_marker).unwrap();
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
                return DTPSError::other(format!("Wrong number of components: {:?}; expected 3", path_components));
            }
            let digest = path_components.get(1).unwrap();
            let content_type = path_components.get(2).unwrap();
            let content_type = content_type.replace("_", "/");
            return Ok(TypeOFSource::Digest(digest.to_string(), content_type.to_string()));
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
    resolve(&ss.node_id, &path_components, ends_with_dash, &all_sources)
}

fn resolve(
    origin_node: &str,
    path_components: &Vec<String>,
    _ends_with_dash: bool,
    all_sources: &Vec<(TopicName, TypeOFSource)>,
) -> DTPSR<TypeOFSource> {
    let mut subtopics: Vec<(TopicName, Vec<String>, Vec<String>, TypeOFSource)> = vec![];
    let mut subtopics_vec = vec![];

    // if path_components.len() == 0 && ends_with_dash {
    //     p =
    //     return Ok(TypeOFSource::OurQueue(TopicName::root(), p));
    // }
    // debug_with_info!(" = all_sources =\n{:#?} ", all_sources);
    for (k, source) in all_sources.iter() {
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

        // debug_with_info!("k: {:?} = components = {:?} ", k, components);

        let (matched, rest) = match is_prefix_of(&path_components, &topic_components) {
            None => continue,

            Some(rest) => rest,
        };

        // debug_with_info!("pushing: {k:?}");
        subtopics.push((k.clone(), matched, rest, source.clone()));
    }

    // debug_with_info!("subtopics: {subtopics:?}");
    if subtopics.len() == 0 {
        let s = format!(
            "Cannot find a matching topic for {:?}.\nMy Topics: {:?}\n",
            path_components, subtopics_vec
        );
        error_with_info!("{}", s);
        return Err(DTPSError::TopicNotFound(s));
    }
    let y = path_components.join("/");
    let unique_id = format!("{origin_node}:{y}");
    let topic_name = TopicName::from_components(path_components);
    let mut sc = SourceComposition {
        topic_name,
        compose: hashmap! {},
        unique_id,
        origin_node: origin_node.to_string(),
    };
    for (_, _, rest, source) in subtopics {
        sc.compose.insert(rest.clone(), Box::new(source));
    }
    Ok(TypeOFSource::Compose(sc))
}

pub fn iterate_type_of_sources(s: &ServerState, add_aliases: bool) -> Vec<(TopicName, TypeOFSource)> {
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
            TypeOFSource::OurQueue(topic_name.clone(), oq.tr.properties.clone()),
        ));
    }

    if add_aliases {
        let sources_no_aliases = iterate_type_of_sources(s, false);

        for (topic_name, new_topic) in s.aliases.iter() {
            let resolved = resolve(&s.node_id, new_topic.as_components(), false, &sources_no_aliases);
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
            patchable: false,
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

#[async_trait]
impl Patchable for TypeOFSource {
    async fn patch(&self, presented_as: &str, ss_mutex: ServerStateAccess, patch: &Patch) -> DTPSR<()> {
        match self {
            TypeOFSource::ForwardedQueue(_) => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::OurQueue(topic_name, ..) => {
                if topic_name.as_dash_sep() == TOPIC_PROXIED {
                    patch_proxied(ss_mutex, &topic_name, patch).await
                } else {
                    patch_our_queue(ss_mutex, patch, topic_name).await
                }
            }
            TypeOFSource::MountedDir(..) => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::MountedFile { .. } => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::Compose(sc) => patch_composition(ss_mutex, patch, sc).await,
            TypeOFSource::Transformed(ts_inside, transform) => {
                patch_transformed(ss_mutex, presented_as, patch, ts_inside, transform).await
            }
            TypeOFSource::Digest(..) => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::Deref(..) => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::Index(..) => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::Aliased(..) => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::History(..) => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::OtherProxied(..) => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
        }
    }
}

pub fn add_prefix_to_patch_op(op: &PatchOperation, prefix: &str) -> PatchOperation {
    match op {
        PatchOperation::Add(y) => {
            let mut y = y.clone();
            y.path = format!("{prefix}{path}", path = y.path, prefix = prefix);
            PatchOperation::Add(y)
        }
        PatchOperation::Remove(y) => {
            let mut y = y.clone();
            y.path = format!("{prefix}{path}", path = y.path, prefix = prefix);
            PatchOperation::Remove(y)
        }
        PatchOperation::Replace(y) => {
            let mut y = y.clone();
            y.path = format!("{prefix}{path}", path = y.path, prefix = prefix);
            PatchOperation::Replace(y)
        }
        PatchOperation::Move(y) => {
            let mut y = y.clone();
            y.from = format!("{prefix}{from}", from = y.from, prefix = prefix);
            y.path = format!("{prefix}{path}", path = y.path, prefix = prefix);
            PatchOperation::Move(y)
        }
        PatchOperation::Copy(y) => {
            let mut y = y.clone();
            y.from = format!("{prefix}{from}", from = y.from, prefix = prefix);
            y.path = format!("{prefix}{path}", path = y.path, prefix = prefix);
            PatchOperation::Copy(y)
        }
        PatchOperation::Test(y) => {
            let mut y = y.clone();
            y.path = format!("{prefix}{path}", path = y.path, prefix = prefix);
            PatchOperation::Test(y)
        }
    }
}

pub fn add_prefix_to_patch(patch: &Patch, prefix: &str) -> Patch {
    // let mut new_patch = Patch::();
    let mut ops: Vec<PatchOperation> = Vec::new();
    for op in &patch.0 {
        let op1 = add_prefix_to_patch_op(op, prefix);
        ops.push(op1);
    }
    Patch(ops)
}

async fn patch_composition(ss_mutex: ServerStateAccess, patch: &Patch, sc: &SourceComposition) -> DTPSR<()> {
    let mut ss = ss_mutex.lock().await;

    for x in &patch.0 {
        match x {
            PatchOperation::Add(ao) => {
                let topic_name = topic_name_from_json_pointer(ao.path.as_str())?;
                let value = ao.value.clone();
                let tra: TopicRefAdd = serde_json::from_value(value)?;
                ss.new_topic_ci(
                    &topic_name,
                    Some(tra.app_data),
                    &tra.properties,
                    &tra.content_info,
                    None,
                )?;
            }
            PatchOperation::Remove(ro) => {
                let topic_name = topic_name_from_json_pointer(ro.path.as_str())?;
                ss.remove_topic(&topic_name)?;
            }
            PatchOperation::Replace(_)
            | PatchOperation::Move(_)
            | PatchOperation::Copy(_)
            | PatchOperation::Test(_) => {
                return Err(DTPSError::NotImplemented(format!(
                    "patch_composition: {sc:#?} with {patch:?}"
                )));
            }
        }
    }

    Ok(())
}

pub fn topic_name_from_json_pointer(path: &str) -> DTPSR<TopicName> {
    let mut components = Vec::new();
    for p in path.split('/') {
        if p.len() == 0 {
            continue;
        }
        components.push(p.to_string());
    }

    Ok(TopicName::from_components(&components))
}

async fn patch_transformed(
    ss_mutex: ServerStateAccess,
    presented_as: &str,
    patch: &Patch,
    ts: &TypeOFSource,
    transform: &Transforms,
) -> DTPSR<()> {
    debug_with_info!("patch_transformed:\n{ts:#?}\n---\n{transform:?}\n---\n{patch:?}");
    match transform {
        Transforms::GetInside(path) => {
            let mut prefix = String::new();
            for p in path {
                prefix.push('/');
                prefix.push_str(p);
            }
            let patch2 = add_prefix_to_patch(patch, &prefix);
            // debug_with_info!("redirecting patch:\nbefore: {patch:#?} after: \n{patch2:#?}");
            ts.patch(presented_as, ss_mutex, &patch2).await
        }
    }
}

async fn patch_our_queue(
    ss_mutex: ServerStateAccess,
    // _presented_as: &str,
    patch: &Patch,
    topic_name: &TopicName,
) -> DTPSR<()> {
    let (data_saved, raw_data) = {
        let ss = ss_mutex.lock().await;
        let oq = ss.get_queue(topic_name)?;
        // todo: if EMPTY...
        if oq.saved.is_empty() {
            return Err(DTPSError::TopicNotFound(format!(
                "patch_our_queue: {topic_name:?} is empty",
                topic_name = topic_name
            )));
        }
        let last = oq.stored.last().unwrap();
        let data_saved = oq.saved.get(last).unwrap();
        let content = ss.get_blob_bytes(&data_saved.digest)?;
        let raw_data = RawData::new(content, &data_saved.content_type);
        (data_saved.clone(), raw_data)
    };
    let mut x: serde_json::Value = raw_data.get_as_json()?;
    let x0 = x.clone();

    context!(json_patch::patch(&mut x, patch), "cannot apply patch")?;
    if x == x0 {
        debug_with_info!("The patch didn't change anything:\n {patch:?}");
        return Ok(());
    }

    let new_content: RawData = RawData::encode_from_json(&x, &data_saved.content_type)?;
    let new_clocks = data_saved.clocks.clone();

    let mut ss = ss_mutex.lock().await;

    // oq.push(&new_content, Some(new_clocks))?;
    let ds = ss.publish(
        &topic_name,
        &new_content.content,
        &data_saved.content_type,
        Some(new_clocks),
    )?;

    if ds.index != data_saved.index + 1 {
        // should never happen because we have acquired the lock
        panic!("there were some modifications inside: {ds:?} != {data_saved:?}");
    }

    Ok(())
}

async fn patch_proxied(ss_mutex: ServerStateAccess, topic_name: &TopicName, p: &Patch) -> DTPSR<()> {
    let mut ss = ss_mutex.lock().await;

    let (data_saved, raw_data) = match ss.get_last_insert(&topic_name)? {
        None => {
            // should not happen
            panic!("patch_proxied: {topic_name:?} is empty");
        }
        Some(s) => (s.data_saved, s.raw_data),
    };

    let x0: serde_json::Value = raw_data.get_as_json()?;
    let mut x = x0.clone();
    // debug_with_info!("patching:\n---{x0:?}---\n{p:?}\n");
    patch(&mut x, p)?;
    if x == x0 {
        debug_with_info!("The patch didn't change anything:\n {p:?}");
        return Ok(());
    }
    // debug_with_info!("patching:\n---\n{x0:?}\n---\n{x:?}");

    for po in &p.0 {
        match po {
            PatchOperation::Add(ao) => {
                let pj: ProxyJob = serde_json::from_value(ao.value.clone())?;
                let key = unescape_json_patch(&ao.path)[1..].to_string();
                let topic_name = TopicName::from_dash_sep(key)?;
                let mut urls = Vec::new();
                for u in pj.urls.iter() {
                    match parse_url_ext(u) {
                        Ok(u) => urls.push(u),
                        Err(e) => {
                            error_with_info!("cannot parse url: {u:?} {e:?}");
                        }
                    };
                }

                debug_with_info!("adding proxy: topic_name = {topic_name:?} urls = {urls:?}",);

                ss.add_proxy_connection(&topic_name, &urls, pj.node_id, ss_mutex.clone())?;
            }
            PatchOperation::Remove(ro) => {
                let key = unescape_json_patch(&ro.path)[1..].to_string();
                let topic_name = TopicName::from_dash_sep(key)?;
                ss.remove_proxy_connection(&topic_name)?;
            }
            PatchOperation::Replace(_)
            | PatchOperation::Move(_)
            | PatchOperation::Copy(_)
            | PatchOperation::Test(_) => {
                return Err(DTPSError::NotImplemented(format!("operation invalid with {p:?}")));
            }
        }
    }
    let new_content: RawData = RawData::encode_from_json(&x, &data_saved.content_type)?;
    let new_clocks = data_saved.clocks.clone();

    let ds = ss.publish(
        &topic_name,
        &new_content.content,
        &data_saved.content_type,
        Some(new_clocks),
    )?;

    if ds.index != data_saved.index + 1 {
        panic!("there were some modifications inside: {ds:?} != {data_saved:?}");
    }

    Ok(())
}
