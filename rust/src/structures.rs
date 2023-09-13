use std::{
    collections::{
        HashMap,
        HashSet,
    },
    fmt,
    fmt::Display,
    ops::Add,
    path::PathBuf,
};

use bytes::Bytes;
use derive_more::Constructor;
use maplit::hashmap;
use schemars::{
    schema::RootSchema,
    JsonSchema,
};
use serde::{
    Deserialize,
    Serialize,
};
use serde_cbor::Value as CBORValue;
use sha256::digest;
use url::Url;

use crate::{
    divide_in_components,
    identify_presentation,
    join_con,
    join_ext,
    parse_url_ext,
    Clocks,
    ContentPresentation,
    DTPSError,
    LinkBenchmark,
    TopicName,
    TypeOfConnection,
    CONTENT_TYPE_CBOR,
    CONTENT_TYPE_JSON,
    DTPSR,
};

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq)]
pub struct RawData {
    pub content: bytes::Bytes,
    pub content_type: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct DataFromChannel {
    pub data_ready: DataReady,
    pub raw_data: RawData,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DataReady {
    pub origin_node: String,
    pub unique_id: String,
    pub sequence: usize,
    pub time_inserted: i64,
    pub digest: String,
    pub content_type: String,
    pub content_length: usize,
    pub clocks: Clocks,
    pub availability: Vec<ResourceAvailabilityWire>,
    pub chunks_arriving: usize,
}

// #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
// pub struct History {
//     pub available: HashMap<usize, DataReady>,
// }
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
                return Err(anyhow::anyhow!(msg).into());
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
        url.push_str(&c);
        url.push_str("/");
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
