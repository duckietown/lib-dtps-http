use std::{
    collections::HashMap,
    fmt::Debug,
};
use tokio::sync::broadcast::Receiver as BroadcastReceiver;

use anyhow::Context;

use async_trait::async_trait;

use json_patch::Patch;

use serde_cbor::Value as CBORValue;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::{
    context,
    get_inside,
    ChannelInfo,
    Clocks,
    InsertNotification,
    ListenURLEvents,
    OtherProxyInfo,
    RawData,
    ResolvedData,
    ServerStateAccess,
    TopicName,
    TopicProperties,
    TopicsIndexInternal,
    DTPSR,
};

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
pub trait Pushable {
    async fn push(&self, ss_mutex: ServerStateAccess, data: &RawData, clocks: &Clocks) -> DTPSR<()>;
}

#[async_trait]
pub trait ResolveDataSingle {
    async fn resolve_data_single(&self, presented_as: &str, ss_mutex: ServerStateAccess) -> DTPSR<ResolvedData>;
}

#[async_trait]
pub trait GetStream {
    async fn get_data_stream(&self, presented_as: &str, ssa: ServerStateAccess) -> DTPSR<DataStream>;
}

#[async_trait]
pub trait GetMeta {
    async fn get_meta_index(&self, presented_as: &str, ss_mutex: ServerStateAccess) -> DTPSR<TopicsIndexInternal>;
}

pub trait DataProps {
    fn get_properties(&self) -> TopicProperties;
}

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

#[derive(Debug, Clone)]
pub struct ForwardedQueue {
    pub subscription: TopicName,
    pub his_topic_name: TopicName,
    pub my_topic_name: TopicName,
    pub properties: TopicProperties,
}

#[derive(Debug, Clone)]
pub struct OtherProxied {
    pub path_and_query: String,
    pub op: OtherProxyInfo,
}

pub struct DataStream {
    pub channel_info: ChannelInfo,

    /// The first data (if available)
    pub first: Option<InsertNotification>,

    /// The stream (or none if no more data is coming through)
    pub stream: Option<BroadcastReceiver<ListenURLEvents>>,

    /// handles of couroutines needed for making this happen
    pub handles: Vec<JoinHandle<DTPSR<()>>>,
}

#[derive(Debug, Clone)]
pub struct ActualUpdate {
    pub component: Vec<String>,
    pub data: ResolvedData,
    pub clocks: Clocks,
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

impl Transforms {
    pub fn apply(&self, data: CBORValue) -> DTPSR<CBORValue> {
        match self {
            Transforms::GetInside(path) => {
                // debug_with_info!("Get inside: {:?}", d);
                let inside = context!(get_inside(vec![], &data, path), "Error getting inside: {path:?}")?;
                Ok(inside)
            }
        }
    }
}

pub const MASK_ORIGIN: bool = false;
