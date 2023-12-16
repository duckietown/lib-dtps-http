use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use derive_more::Constructor;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_cbor::Value as CBORValue;

use crate::{divide_in_components, object_queues::InsertNotification, Clocks, TypeOfConnection, DTPSR};

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq)]
pub struct RawData {
    pub content: Bytes,
    pub content_type: String,
}

impl AsRef<RawData> for RawData {
    fn as_ref(&self) -> &RawData {
        &self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MsgClientToServer {
    RawData(RawData),
}

#[derive(Debug, Clone, Serialize, Deserialize, Constructor, PartialEq)]
pub struct ChannelInfoDesc {
    pub sequence: usize,
    pub time_inserted: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChannelInfo {
    pub queue_created: i64,
    pub num_total: usize,
    pub newest: Option<ChannelInfoDesc>,
    pub oldest: Option<ChannelInfoDesc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Chunk {
    pub digest: String,
    pub i: usize,
    pub n: usize,
    pub index: usize,
    pub data: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FinishedMsg {
    pub comment: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SilenceMsg {
    pub dt: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ErrorMsg {
    pub comment: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WarningMsg {
    pub comment: String,
}

impl WarningMsg {
    pub fn to_string(&self) -> String {
        format!("Channel warning: {}", self.comment)
    }
}

impl ErrorMsg {
    pub fn to_string(&self) -> String {
        format!("Channel error: {}", self.comment)
    }
}

/// These are the raw messages that are sent over the websocket.
/// The API will mask things like DataReady and Chunk and in the end
/// The user will get one of ListenURLEvents.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MsgServerToClient {
    ChannelInfo(ChannelInfo),
    //
    DataReady(DataReady),
    Chunk(Chunk),
    //
    WarningMsg(WarningMsg),
    ErrorMsg(ErrorMsg),
    FinishedMsg(FinishedMsg),
    //
    SilenceMsg(SilenceMsg),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PushResult {
    pub result: bool,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MsgWebsocketPushServerToClient {
    PushResult(PushResult),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MsgWebsocketPushClientToServer {
    RawData(RawData),
}

/// These are the messages that a user of the API will get when,
/// for example, listening to a URL.
#[derive(Debug, PartialEq, Clone)]
pub enum ListenURLEvents {
    InsertNotification(InsertNotification),
    WarningMsg(WarningMsg),
    ErrorMsg(ErrorMsg),
    FinishedMsg(FinishedMsg),
    SilenceMsg(SilenceMsg),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DataSaved {
    pub origin_node: String,
    pub unique_id: String,
    pub index: usize,
    pub time_inserted: i64,
    pub clocks: Clocks,
    pub content_type: String,
    pub content_length: usize,
    pub digest: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DataReady {
    pub origin_node: String,
    pub unique_id: String,
    pub sequence: usize,
    pub time_inserted: i64,
    pub clocks: Clocks,
    pub content_type: String,
    pub content_length: usize,
    pub digest: String,

    pub availability: Vec<ResourceAvailabilityWire>,
    pub chunks_arriving: usize,
}

impl DataReady {
    pub fn as_data_saved(&self) -> DataSaved {
        DataSaved {
            origin_node: self.origin_node.clone(),
            unique_id: self.unique_id.clone(),
            index: self.sequence,
            time_inserted: self.time_inserted,
            clocks: self.clocks.clone(),
            content_type: self.content_type.clone(),
            content_length: self.content_length,
            digest: self.digest.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ResolvedData {
    RawData(RawData),
    Regular(CBORValue),
    NotAvailableYet(String),
    NotFound(String),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ResourceAvailabilityWire {
    pub url: String,
    pub available_until: f64,
}

pub type History = HashMap<usize, DataReady>;

#[derive(Debug, Clone)]
pub struct FoundMetadata {
    pub base_url: TypeOfConnection,
    pub alternative_urls: HashSet<TypeOfConnection>,
    pub answering: Option<String>,

    pub events_url: Option<TypeOfConnection>,
    pub events_data_inline_url: Option<TypeOfConnection>,
    pub meta_url: Option<TypeOfConnection>,
    pub history_url: Option<TypeOfConnection>,

    pub connections_url: Option<TypeOfConnection>,
    pub proxied_url: Option<TypeOfConnection>,

    //  url for webhook push protocol
    pub stream_push_url: Option<TypeOfConnection>,

    /// nanoseconds
    pub latency_ns: u128,
    pub content_type: String,
}

impl FoundMetadata {
    pub fn get_answering(&self) -> DTPSR<String> {
        match &self.answering {
            None => {
                let base = self.base_url.to_string();
                let msg = format!("Metadata says this is not a DTPS node:\nconbase: {base}");
                Err(anyhow::anyhow!(msg).into())
            }
            Some(x) => Ok(x.clone()),
        }
    }
}

pub fn get_url_from_topic_name(topic_name: &str) -> String {
    let components = divide_in_components(topic_name, '.');
    make_rel_url(&components)
}

pub fn make_rel_url(a: &Vec<String>) -> String {
    let mut url = String::new();
    for c in a {
        url.push_str(c);
        url.push('/');
    }
    url
}

#[cfg(test)]
mod test {
    use schemars::schema_for;

    use crate::TopicsIndexWire;

    #[test]
    fn testjson() {
        let schema = schema_for!(TopicsIndexWire);
        eprintln!("{}", serde_json::to_string_pretty(&schema).unwrap());
    }
}

#[derive(Debug)]
pub enum TypeOfResource {
    Other,
    DTPSTopic,
    DTPSIndex { node_id: String },
}
