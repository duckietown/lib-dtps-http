use std::{collections::HashMap, env, fmt::Debug};

use anyhow::Context;
use async_trait::async_trait;
use json_patch::Patch;
use lazy_static::lazy_static;
use serde_cbor::Value as CBORValue;
use tokio::{sync::broadcast::Receiver as BroadcastReceiver, task::JoinHandle};

use crate::{
    context, dtpserror_context, get_inside, utils::is_truthy, ChannelInfo, Clocks, InsertNotification, ListenURLEvents,
    OtherProxyInfo, RawData, ResolvedData, ServerStateAccess, TopicName, TopicProperties, TopicsIndexInternal, DTPSR,
    ENV_MASK_ORIGIN,
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

//  ../node/    ("indice"
//   .../node/out1
//   .../node/out2

// sub ../node/          -> ti notifica quando i topic cambiano

// {'out1':  TopicProperties(), 'out2': }

// sub ../node/:deref:/  -> ti notiifan quando i dati cambiano

//  {'out1': 'data1', 'out2': 'data2'}

// TODO: create Jira issue for this
// sub ../node/:deref:/:diff:/

// [{'op': reaplce, path='/' , value: {'out1': 'data1', 'out2': 'data2'} ]
// [{'op': replace, 'path': '/out1', 'value': 'data1'}]

// TODO: create statistic endpoint
// ../topic/:stats:/

// {
//
// }

// TODO: tell node not to produce data if nobody is looking at it

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

impl Transforms {
    pub fn apply(&self, data: CBORValue) -> DTPSR<CBORValue> {
        match self {
            Transforms::GetInside(path) => {
                // debug_with_info!("Get inside: {:?}", d);
                let inside = dtpserror_context!(get_inside(vec![], &data, path), "Error getting inside: {path:?}")?;
                Ok(inside)
            }
        }
    }
}

pub static DEFAULT_MASK_ORIGIN: bool = false;

pub fn should_use_origin() -> bool {
    // Retrieve the environment variable's value.
    match env::var(ENV_MASK_ORIGIN) {
        Ok(value) => {
            // If the environment variable is set, check its truthiness.
            match is_truthy(&value) {
                None => DEFAULT_MASK_ORIGIN,
                Some(x) => x,
            }
        }
        // If the environment variable is not set, return the default value.
        Err(_) => DEFAULT_MASK_ORIGIN,
    }
}

lazy_static! {
    // Define a default value for mask origin that can be used throughout the program.
    pub static  ref MASK_ORIGIN: bool = should_use_origin();

}
