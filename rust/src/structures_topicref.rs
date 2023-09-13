use std::collections::HashMap;

use maplit::hashmap;
use schemars::{
    schema::RootSchema,
    JsonSchema,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    join_con,
    join_ext,
    LinkBenchmark,
    RawData,
    TopicName,
    TypeOfConnection,
};

pub type NodeAppData = String;

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

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
pub struct ForwardingStep {
    pub forwarding_node: String,
    pub forwarding_node_connects_to: String,

    pub performance: LinkBenchmark,
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
            let topic_reachability_internal = TopicReachabilityInternal::from_wire(topic_reachability_wire, conbase);
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
            topics.insert(TopicName::from_dash_sep(topic_name).unwrap(), topic_ref_internal);
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

impl TopicReachabilityInternal {
    pub fn to_wire(&self, use_rel: Option<String>) -> TopicReachabilityWire {
        let con = match use_rel {
            None => self.con.clone(),
            Some(use_patch) => match &self.con {
                TypeOfConnection::TCP(_) => self.con.clone(),
                TypeOfConnection::UNIX(_) => self.con.clone(),
                TypeOfConnection::Same() => self.con.clone(),
                TypeOfConnection::File(..) => self.con.clone(),
                TypeOfConnection::Relative(_, query) => TypeOfConnection::Relative(use_patch.clone(), query.clone()),
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
