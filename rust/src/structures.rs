use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::Display;
use std::ops::Add;
use std::path::PathBuf;

use bytes::Bytes;
use derive_more::Constructor;
use maplit::hashmap;
use schemars::schema::RootSchema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_cbor::Value as CBORValue;
use sha256::digest;
use url::Url;

use crate::divide_in_components;
use crate::join_ext;
use crate::{
    identify_presentation, ContentPresentation, DTPSError, CONTENT_TYPE_CBOR, CONTENT_TYPE_JSON,
};
use crate::{join_con, TopicName};
use crate::{parse_url_ext, LinkBenchmark, DTPSR};

pub type NodeAppData = String;

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq)]
pub struct RawData {
    pub content: bytes::Bytes,
    pub content_type: String,
}

impl RawData {
    pub fn new<S: AsRef<[u8]>, T: AsRef<str>>(content: S, content_type: T) -> RawData {
        RawData {
            content: Bytes::from(content.as_ref().to_vec().clone()).clone(),
            content_type: content_type.as_ref().to_string().clone(),
        }
    }
    pub fn cbor<S: AsRef<[u8]>>(content: S) -> RawData {
        Self::new(content, CONTENT_TYPE_CBOR)
    }
    pub fn json<S: AsRef<[u8]>>(content: S) -> RawData {
        Self::new(content, CONTENT_TYPE_JSON)
    }
    pub fn digest(&self) -> String {
        let d = digest(self.content.as_ref());
        format!("sha256:{}", d)
    }

    pub fn represent_as_json<T: Serialize>(x: T) -> DTPSR<Self> {
        Ok(RawData::new(serde_json::to_vec(&x)?, CONTENT_TYPE_JSON))
    }
    pub fn represent_as_cbor<T: Serialize>(x: T) -> DTPSR<Self> {
        Ok(RawData::new(serde_cbor::to_vec(&x)?, CONTENT_TYPE_CBOR))
    }
    pub fn represent_as_cbor_ct<T: Serialize>(x: T, ct: &str) -> DTPSR<Self> {
        Ok(RawData::new(serde_cbor::to_vec(&x)?, ct.to_string()))
    }
    pub fn from_cbor_value(x: &serde_cbor::Value) -> DTPSR<Self> {
        Ok(RawData::cbor(serde_cbor::to_vec(x)?))
    }
    pub fn from_json_value(x: &serde_json::Value) -> DTPSR<Self> {
        Ok(RawData::json(serde_json::to_vec(x)?))
    }
    /// Deserializes the content of this object as a given type.
    /// The type must implement serde::Deserialize.
    /// It works only for CBOR, JSON and YAML.
    pub fn interpret<'a, T>(&'a self) -> DTPSR<T>
    where
        T: Deserialize<'a> + Clone,
    {
        match self.presentation() {
            ContentPresentation::CBOR => {
                let v: T = serde_cbor::from_slice::<T>(&self.content)?;
                Ok(v)
            }
            ContentPresentation::JSON => {
                let v: T = serde_json::from_slice::<T>(&self.content)?;
                Ok(v)
            }
            ContentPresentation::YAML => {
                let v: T = serde_yaml::from_slice::<T>(&self.content)?;
                Ok(v)
            }
            ContentPresentation::PlainText => DTPSError::other("cannot interpret plain text"),
            ContentPresentation::Other => DTPSError::other("cannot interpret unknown content type"),
        }
    }

    pub fn presentation(&self) -> ContentPresentation {
        identify_presentation(&self.content_type)
    }

    pub fn try_translate(&self, ct: &str) -> DTPSR<Self> {
        if self.content_type == ct {
            return Ok(self.clone());
        }
        let mine = identify_presentation(self.content_type.as_str());
        let desired = identify_presentation(ct);

        let value = self.get_as_cbor()?;
        let bytes = match desired {
            ContentPresentation::CBOR => serde_cbor::to_vec(&value)?,
            ContentPresentation::JSON => serde_json::to_vec(&value)?,
            ContentPresentation::YAML => Bytes::from(serde_yaml::to_string(&value)?).to_vec(),
            ContentPresentation::PlainText | ContentPresentation::Other => {
                return DTPSError::other(format!(
                    "cannot translate from {:?} to {:?}",
                    mine, desired
                ));
            }
        };
        Ok(RawData::new(bytes, ct))
    }
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TopicProperties {
    pub streamable: bool,
    pub pushable: bool,
    pub readable: bool,
    pub immutable: bool,
    pub has_history: bool,
    pub patchable: bool,
}

impl TopicProperties {
    pub fn rw() -> Self {
        TopicProperties {
            streamable: true,
            pushable: true,
            readable: true,
            immutable: false,
            has_history: true,
            patchable: true, // XXX
        }
    }
    pub fn ro() -> Self {
        TopicProperties {
            streamable: true,
            pushable: false,
            readable: true,
            immutable: false,
            has_history: false,
            patchable: false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
pub struct TopicsIndexWire {
    pub topics: HashMap<String, TopicRefWire>,
}

#[derive(Debug, Clone, Default)]
pub struct TopicsIndexInternal {
    pub topics: HashMap<TopicName, TopicRefInternal>,
}

impl TopicsIndexInternal {
    pub fn to_wire(self, use_rel: Option<String>) -> TopicsIndexWire {
        let mut topics: HashMap<String, TopicRefWire> = HashMap::new();
        for (topic_name, topic_ref_internal) in self.topics {
            let topic_ref_wire = topic_ref_internal.to_wire(use_rel.clone());
            topics.insert(topic_name.to_dash_sep(), topic_ref_wire);
        }
        TopicsIndexWire { topics }
    }
    pub fn from_wire(wire: &TopicsIndexWire, conbase: &TypeOfConnection) -> Self {
        let mut topics: HashMap<TopicName, TopicRefInternal> = HashMap::new();
        for (topic_name, topic_ref_wire) in &wire.topics {
            let topic_ref_internal = TopicRefInternal::from_wire(topic_ref_wire, conbase);
            topics.insert(
                TopicName::from_dash_sep(topic_name).unwrap(),
                topic_ref_internal,
            );
        }
        Self { topics }
    }

    pub fn add_path<S: AsRef<str>>(&self, rel: S) -> Self {
        let mut topics: HashMap<TopicName, _> = HashMap::new();
        for (topic_name, topic_ref_internal) in &self.topics {
            let t = topic_ref_internal.add_path(rel.as_ref());
            topics.insert(topic_name.clone(), t);
        }
        Self { topics }
    }
}

type ContentType = String;

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
pub struct DataDesc {
    pub content_type: ContentType,
    pub jschema: Option<RootSchema>,
    pub examples: Vec<RawData>,
}

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
pub struct ContentInfo {
    pub accept: HashMap<String, DataDesc>,
    pub storage: DataDesc,
    pub produces_content_type: Vec<ContentType>,
}

impl ContentInfo {
    pub fn simple<S: AsRef<str>>(ct: S, jschema: Option<RootSchema>) -> Self {
        let ct = ct.as_ref().to_string();
        let dd = DataDesc {
            content_type: ct.clone(),
            jschema: jschema.clone(),
            examples: vec![],
        };
        ContentInfo {
            accept: hashmap! {"".to_string() => dd.clone()},
            storage: DataDesc {
                content_type: ct.clone(),
                jschema: jschema.clone(),
                examples: vec![],
            },
            produces_content_type: vec![ct.clone()],
        }
    }

    pub fn generic() -> Self {
        ContentInfo {
            accept: Default::default(),

            produces_content_type: vec!["*".to_string()],

            storage: DataDesc {
                content_type: "*".to_string(),
                jschema: None,
                examples: vec![],
            },
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
pub struct TopicRefWire {
    pub unique_id: String,
    pub origin_node: String,
    pub app_data: HashMap<String, NodeAppData>,
    pub reachability: Vec<TopicReachabilityWire>,
    pub created: i64,
    pub properties: TopicProperties,
    pub content_info: ContentInfo,
}

#[derive(Debug, Clone)]
pub struct TopicRefInternal {
    pub unique_id: String,
    pub origin_node: String,
    pub app_data: HashMap<String, NodeAppData>,
    pub reachability: Vec<TopicReachabilityInternal>,
    pub created: i64,
    pub properties: TopicProperties,
    pub content_info: ContentInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TopicRefAdd {
    pub app_data: HashMap<String, NodeAppData>,
    pub properties: TopicProperties,
    pub content_info: ContentInfo,
}

impl TopicRefInternal {
    pub fn to_wire(&self, use_rel: Option<String>) -> TopicRefWire {
        let mut reachability = Vec::new();
        for topic_reachability_internal in &self.reachability {
            let topic_reachability_wire = topic_reachability_internal.to_wire(use_rel.clone());
            reachability.push(topic_reachability_wire);
        }
        TopicRefWire {
            unique_id: self.unique_id.clone(),
            origin_node: self.origin_node.clone(),
            app_data: self.app_data.clone(),
            created: self.created,
            reachability,
            properties: self.properties.clone(),
            content_info: self.content_info.clone(),
        }
    }
    pub fn from_wire(wire: &TopicRefWire, conbase: &TypeOfConnection) -> Self {
        let mut reachability = Vec::new();
        for topic_reachability_wire in &wire.reachability {
            let topic_reachability_internal =
                TopicReachabilityInternal::from_wire(topic_reachability_wire, conbase);
            reachability.push(topic_reachability_internal);
        }
        TopicRefInternal {
            unique_id: wire.unique_id.clone(),
            origin_node: wire.origin_node.clone(),
            app_data: wire.app_data.clone(),
            created: wire.created,
            reachability,
            properties: wire.properties.clone(),
            content_info: wire.content_info.clone(),
        }
    }
    pub fn add_path(&self, rel: &str) -> Self {
        let mut reachability = Vec::new();
        for topic_reachability_internal in &self.reachability {
            let t = topic_reachability_internal.add_path(rel);
            reachability.push(t);
        }
        return Self {
            unique_id: self.unique_id.clone(),
            origin_node: self.origin_node.clone(),
            app_data: Default::default(),
            created: self.created,
            reachability,
            properties: self.properties.clone(),
            content_info: self.content_info.clone(),
        };
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
pub struct ForwardingStep {
    pub forwarding_node: String,
    pub forwarding_node_connects_to: String,

    pub performance: LinkBenchmark,
}

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub struct UnixCon {
    pub scheme: String,
    pub socket_name: String,
    pub path: String,
    pub query: Option<String>,
}

impl UnixCon {
    pub fn from_path(path: &str) -> Self {
        Self {
            scheme: "unix+http".to_string(),
            socket_name: path.to_string(),
            path: "/".to_string(),
            query: None,
        }
    }
}

impl Display for UnixCon {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UnixCon(")?;
        write!(f, "[{}]", self.socket_name)?;
        write!(f, " {}", self.path)?;
        if let Some(query) = &self.query {
            write!(f, "?{}", query)?;
        }
        write!(f, ")")?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub enum FilePaths {
    Absolute(String),
    Relative(String),
}

pub fn join_and_normalize(path1: &str, path2: &str) -> String {
    let mut path = PathBuf::from(path1);
    path.push(path2);
    path.canonicalize()
        .unwrap_or(path)
        .to_str()
        .unwrap()
        .to_string()
}

pub fn normalize_path(path1: &str) -> String {
    let path = PathBuf::from(path1);
    let mut p = path
        .canonicalize()
        .unwrap_or(path)
        .to_str()
        .unwrap()
        .to_string();
    if p.len() > 1 {
        p = p.trim_end_matches("/").to_string();
    }
    return p;
}

impl FilePaths {
    pub(crate) fn add_prefix(&self, prefix: &str) -> FilePaths {
        match self {
            FilePaths::Absolute(s) => FilePaths::Absolute(s.clone()),
            FilePaths::Relative(s) => FilePaths::Relative(join_and_normalize(prefix, s)),
        }
    }
    pub(crate) fn join(&self, suffix: &str) -> FilePaths {
        match self {
            FilePaths::Absolute(s) => FilePaths::Absolute(join_and_normalize(s, suffix)),
            FilePaths::Relative(s) => FilePaths::Relative(join_and_normalize(s, suffix)),
        }
    }
}

impl Display for FilePaths {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FilePaths::Absolute(s) => write!(f, "{}", s),
            FilePaths::Relative(s) => write!(f, "./{}", s),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub enum TypeOfConnection {
    /// TCP Connection
    TCP(Url),
    /// Unix socket connection
    UNIX(UnixCon),
    /// A file or dir in the filesystem
    File(Option<String>, FilePaths),

    /// Path relative to context (used in indices)
    Relative(String, Option<String>),

    /// Exactly same context
    Same(),
}

impl TypeOfConnection {
    pub fn from_string(s: &str) -> DTPSR<Self> {
        parse_url_ext(s)
    }

    pub fn unix_socket(path: &str) -> Self {
        return Self::UNIX(UnixCon::from_path(path));
    }
    pub fn to_string(&self) -> String {
        match self {
            TypeOfConnection::TCP(url) => url.to_string(),
            TypeOfConnection::Relative(s, q) => match q {
                Some(query) => {
                    let mut s = s.clone();
                    s.push_str("?");
                    s.push_str(&query);
                    s
                }
                None => s.clone(),
            },
            TypeOfConnection::UNIX(unixcon) => {
                let mut s = unixcon.scheme.clone();
                s.push_str("://");
                s.push_str(&unixcon.socket_name);
                s.push_str(&unixcon.path);
                if let Some(query) = &unixcon.query {
                    s.push_str("?");
                    s.push_str(&query);
                }
                s
            }
            TypeOfConnection::Same() => "".to_string(),
            TypeOfConnection::File(_, path) => path.clone().to_string(),
        }
    }
}

impl fmt::Display for TypeOfConnection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TypeOfConnection::File(hostname, path) => {
                write!(f, "File(host={:?},path={:?})", hostname, path)
            }
            TypeOfConnection::TCP(url) => write!(f, "TCP({:?})", url.to_string()),
            TypeOfConnection::UNIX(unix_con) => write!(f, "UNIX({})", unix_con),
            TypeOfConnection::Relative(s, q) => write!(f, "Relative({},{:?})", s, q),
            TypeOfConnection::Same() => {
                write!(f, "Same()")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TopicReachabilityInternal {
    pub con: TypeOfConnection,
    pub answering: String,
    pub forwarders: Vec<ForwardingStep>,
    pub benchmark: LinkBenchmark,
}

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
pub struct TopicReachabilityWire {
    pub url: String,
    pub answering: String,
    pub forwarders: Vec<ForwardingStep>,
    pub benchmark: LinkBenchmark,
}

impl TopicReachabilityInternal {
    pub fn to_wire(&self, use_rel: Option<String>) -> TopicReachabilityWire {
        let con = match use_rel {
            None => self.con.clone(),
            Some(use_patch) => match &self.con {
                TypeOfConnection::TCP(_) => self.con.clone(),
                TypeOfConnection::UNIX(_) => self.con.clone(),
                TypeOfConnection::Same() => self.con.clone(),
                TypeOfConnection::File(..) => self.con.clone(),
                TypeOfConnection::Relative(_, query) => {
                    TypeOfConnection::Relative(use_patch.clone(), query.clone())
                }
            },
        };

        TopicReachabilityWire {
            url: con.to_string(),
            answering: self.answering.clone(),
            forwarders: self.forwarders.clone(),
            benchmark: self.benchmark.clone(),
        }
    }
    pub fn from_wire(wire: &TopicReachabilityWire, conbase: &TypeOfConnection) -> Self {
        TopicReachabilityInternal {
            con: join_ext(conbase, &wire.url).unwrap(),
            answering: wire.answering.clone(),
            forwarders: wire.forwarders.clone(),
            benchmark: wire.benchmark.clone(),
        }
    }
    pub fn add_path(&self, rel: &str) -> Self {
        TopicReachabilityInternal {
            con: join_con(rel, &self.con).unwrap(),
            answering: self.answering.clone(),
            forwarders: self.forwarders.clone(),
            benchmark: self.benchmark.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ResourceAvailabilityWire {
    pub url: String,
    pub available_until: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Constructor, PartialEq)]
pub struct MinMax<T: Ord + Clone + PartialEq> {
    pub min: T,
    pub max: T,
}

pub fn merge_minmax<T: Ord + Clone>(minmax1: &MinMax<T>, minmax2: &MinMax<T>) -> MinMax<T> {
    let mut min = minmax1.min.clone();
    let mut max = minmax1.max.clone();
    if minmax2.min < min {
        min = minmax2.min.clone();
    }
    if minmax2.max > max {
        max = minmax2.max.clone();
    }
    MinMax::new(min, max)
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Constructor, PartialEq)]
pub struct Clocks {
    pub logical: HashMap<String, MinMax<usize>>,
    pub wall: HashMap<String, MinMax<i64>>,
}

pub fn merge_to<T: Ord + Clone>(
    x: &mut HashMap<String, MinMax<T>>,
    y: &HashMap<String, MinMax<T>>,
) {
    for (key, minmax) in y {
        if let Some(minmax2) = x.get(key) {
            x.insert(key.clone(), merge_minmax(minmax, minmax2));
        } else {
            x.insert(key.clone(), minmax.clone());
        }
    }
}

pub fn merge_clocks(clock1: &Clocks, clock2: &Clocks) -> Clocks {
    let mut logical = HashMap::new();
    let mut wall = HashMap::new();

    merge_to(&mut logical, &clock1.logical);
    merge_to(&mut logical, &clock2.logical);
    merge_to(&mut wall, &clock1.wall);
    merge_to(&mut wall, &clock2.wall);

    Clocks::new(logical, wall)
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct History {
    pub available: HashMap<usize, DataReady>,
}

#[derive(Debug, Clone)]
pub struct FoundMetadata {
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
