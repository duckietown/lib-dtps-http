use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_recursion::async_recursion;
use async_trait::async_trait;
use bytes::Bytes;
use log::debug;
use maplit::hashmap;
use maud::PreEscaped;
use serde::{Deserialize, Serialize};
use serde_cbor::Value as CBORValue;
use serde_cbor::Value::Null as CBORNull;
use serde_cbor::Value::Text as CBORText;
use tokio::sync::Mutex;

use crate::cbor_manipulation::{get_as_cbor, get_inside};
use crate::signals_logic::ResolvedData::{NotAvailableYet, NotFound, Regular};
use crate::utils::{divide_in_components, is_prefix_of, vec_concat};
use crate::{
    make_rel_url, DTPSError, NodeAppData, RawData, ServerState, ServerStateAccess, TopicName,
    TopicRefInternal, TopicsIndexInternal, CONTENT_TYPE_DTPS_INDEX_CBOR, DTPSR,
};

#[derive(Debug, Clone)]
pub enum Transforms {
    GetInside(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct SourceComposition {
    is_root: bool,
    compose: HashMap<Vec<String>, Box<TypeOFSource>>,
}

impl DataProps for SourceComposition {
    fn get_properties(&self) -> TopicProperties {
        let mut immutable = true;
        let mut streamable = false;
        let pushable = false;
        let mut readable = true;

        for (k, v) in self.compose.iter() {
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
        }
    }
}

#[derive(Debug, Clone)]
pub enum TypeOFSource {
    ForwardedQueue(String),
    OurQueue(String, Vec<String>, TopicProperties),
    Compose(SourceComposition),
    Transformed(Box<TypeOFSource>, Transforms),
    Digest(String, String),
    Deref(SourceComposition),
}

// #[derive(Debug, Clone)]
// struct MakeSignal {
//     source_type: TypeOFSource,
//     transforms: Transforms,
// }
#[derive(Debug)]
pub enum ResolvedData {
    RawData(RawData),
    Regular(CBORValue),
    NotAvailableYet(String),
    NotFound(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicProperties {
    pub streamable: bool,
    pub pushable: bool,
    pub readable: bool,
    pub immutable: bool,
}

#[async_trait]
pub trait ResolveDataSingle {
    async fn resolve_data_single(&self, ss_mutex: ServerStateAccess) -> DTPSR<ResolvedData>;
}

#[async_trait]
pub trait GetMeta {
    async fn get_meta_index(&self, ss_mutex: ServerStateAccess) -> DTPSR<TopicsIndexInternal>;
}

#[async_trait]
pub trait GetHTML {
    async fn get_meta_index(
        &self,
        ss_mutex: ServerStateAccess,
        headers: HashMap<String, String>,
    ) -> DTPSR<PreEscaped<String>>;
}

pub trait DataProps {
    fn get_properties(&self) -> TopicProperties;
}

#[async_trait]
impl ResolveDataSingle for TypeOFSource {
    async fn resolve_data_single(&self, ss_mutex: ServerStateAccess) -> DTPSR<ResolvedData> {
        match self {
            TypeOFSource::Digest(digest, content_type) => {
                let ss = ss_mutex.lock().await;
                let data = ss.get_blob_bytes(digest)?;
                let rd = RawData::new(data, content_type);
                Ok(ResolvedData::RawData(rd))
            }
            TypeOFSource::ForwardedQueue(q) => Err(DTPSError::NotImplemented(
                "not implemented TypeOFSource::ForwardedQueue".to_string(),
            )),
            TypeOFSource::OurQueue(q, _, _) => {
                if false && q == &"" {
                    let ss = ss_mutex.lock().await;
                    let index_internal = ss.create_topic_index();
                    let index = index_internal.to_wire(None);

                    let cbor_bytes = serde_cbor::to_vec(&index).unwrap();
                    let raw_data = RawData {
                        content: Bytes::from(cbor_bytes),
                        content_type: CONTENT_TYPE_DTPS_INDEX_CBOR.to_string(),
                    };
                    Ok(ResolvedData::RawData(raw_data))
                } else {
                    our_queue(q, ss_mutex).await
                }
            }
            TypeOFSource::Compose(sc) => {
                let index = self.get_meta_index(ss_mutex).await?;
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
                let data = source.resolve_data_single(ss_mutex.clone()).await?;
                transform(data, transforms).await
            }
            TypeOFSource::Deref(sc) => single_compose(sc, ss_mutex).await,
        }
    }
}

impl DataProps for TypeOFSource {
    fn get_properties(&self) -> TopicProperties {
        match self {
            TypeOFSource::Digest(_, _) => TopicProperties {
                streamable: false,
                pushable: false,
                readable: true,
                immutable: true,
            },
            TypeOFSource::ForwardedQueue(_) => {
                todo!()
            }
            TypeOFSource::OurQueue(_, _, props) => props.clone(),

            TypeOFSource::Compose(sc) => sc.get_properties(),
            TypeOFSource::Transformed(s, _) => s.get_properties(),
            TypeOFSource::Deref(d) => d.get_properties(),
        }
    }
}

async fn transform(data: ResolvedData, transform: &Transforms) -> DTPSR<ResolvedData> {
    let d = match data {
        Regular(d) => d,
        NotAvailableYet(s) => return Ok(NotAvailableYet(s.clone())),
        NotFound(s) => return Ok(NotFound(s.clone())),
        ResolvedData::RawData(rd) => {
            let x = get_as_cbor(&rd);
            x
        }
    };
    match transform {
        Transforms::GetInside(path) => {
            let inside = get_inside(vec![], &d, path);
            match inside {
                Ok(d) => Ok(ResolvedData::Regular(d)),
                Err(e) => Err(DTPSError::Other(format!("Error getting inside: {}", e))),
            }
        }
    }
}

async fn our_queue(topic_name: &str, ss_mutex: ServerStateAccess) -> DTPSR<ResolvedData> {
    let ss = ss_mutex.lock().await;
    if !ss.oqs.contains_key(topic_name) {
        return Ok(NotFound(format!("No queue with name {}", topic_name)));
    }
    let oq = ss.oqs.get(topic_name).unwrap();

    return match oq.sequence.last() {
        None => {
            let s = format!("No data in queue {}", topic_name);
            Ok(NotAvailableYet(s))
        }
        Some(v) => {
            let content = ss.get_blob_bytes(&v.digest)?;
            let raw_data = RawData::new(content, &v.content_type);
            Ok(ResolvedData::RawData(raw_data))
        }
    };
}

#[async_trait]
impl GetMeta for TypeOFSource {
    async fn get_meta_index(&self, ss_mutex: ServerStateAccess) -> DTPSR<TopicsIndexInternal> {
        // debug!("get_meta_index: {:?}", self);
        return match self {
            TypeOFSource::ForwardedQueue(_) => {
                todo!()
            }
            TypeOFSource::OurQueue(topic_name, _, _) => {
                if topic_name == "" {
                    panic!("get_meta_index for empty topic name");
                }
                let ss = ss_mutex.lock().await;
                let oq = ss.oqs.get(topic_name).unwrap();
                let tr0 = &oq.tr; //.to_wire(Some("".to_string()));
                                  // let components = divide_in_components(topic_name, '.');
                                  // let as_url = make_rel_url(rel_index);
                                  // let tr1 = tr0.add_path(&as_url);
                                  // let tr1 = TopicRefInternal {
                                  //     unique_id: tr0.unique_id.clone(),
                                  //     origin_node: tr0.origin_node.clone(),
                                  //     app_data: tr0.app_data.clone(),
                                  //     reachability: vec![],
                                  //     created: 0,
                                  //     properties: tr0.properties.clone(),
                                  // };

                let mut topics: HashMap<TopicName, TopicRefInternal> = hashmap! {};
                topics.insert("".to_string(), tr0.clone());
                let n: NodeAppData = NodeAppData::from("dede");
                let index = TopicsIndexInternal {
                    node_id: "".to_string(),
                    node_started: 0,
                    node_app_data: hashmap! {"here".to_string() => n},
                    topics,
                };
                Ok(index)
            }
            TypeOFSource::Compose(sc) => get_sc_meta(sc, ss_mutex).await,
            TypeOFSource::Transformed(_, _) => {
                todo!()
            }
            TypeOFSource::Digest(_, _) => {
                todo!()
            }
            TypeOFSource::Deref(_) => {
                todo!()
            }
        };
    }
}

async fn get_sc_meta(
    sc: &SourceComposition,
    ss_mutex: ServerStateAccess,
) -> DTPSR<TopicsIndexInternal> {
    if sc.is_root {
        let ss = ss_mutex.lock().await;
        return Ok(ss.create_topic_index());
    }
    // debug!("get_sc_meta: START: {:?}", sc);
    let mut topics: HashMap<TopicName, TopicRefInternal> = hashmap! {};
    let mut node_app_data = HashMap::new();

    let mut debug_s = String::new();
    for (prefix, inside) in sc.compose.iter() {
        // let inside = inside.resolve_data_single(ss_mutex.clone()).await?;
        // let inside = match inside {
        //     ResolvedData::Regular(d) => d,
        //     _ => return Err(format!("Error getting inside")),
        // };
        // let inside = match inside {
        //     CBORValue::Map(m) => m,
        //     _ => return Err(format!("Error getting inside")),
        // };
        // for (topic_name, p1, prefix) in inside {
        //     let oq = ss.oqs.get(&topic_name).unwrap();
        //     let rel = prefix.join("/");
        //
        //     let rurl = format!("{}/", rel);
        //
        //     let dotsep = prefix.join(".");
        //     topics.insert(dotsep, oq.tr.to_wire(Some(rurl)));
        // }
        // let rel = prefix.join("/");

        // let rurl = format!("{}/", rel);
        // let s = format!("get_sc_meta: prefix: {:?}, rurl: {}\n inside {:#?}", prefix, rurl,
        //                 inside);
        // debug_s.push_str(&s);
        let x = inside.get_meta_index(ss_mutex.clone()).await?;
        // let inside = ts.get_meta_index(ss_mutex.clone()).await?;

        // debug!("get_sc_meta: {:#?} inside: {:#?}", prefix, x);

        // let dotsep = prefix.join(".");

        for (a, b) in x.topics {
            let its = divide_in_components(&a, '.');
            let full: Vec<String> = vec_concat(&prefix, &its);

            let rurl = make_rel_url(&prefix);
            let b2 = b.add_path(&rurl);

            let dotsep = full.join(".");
            topics.insert(dotsep, b2);
        }

        // topics.insert(dotsep, oq.tr.to_wire(Some(rurl)));
    }
    // for (topic_name, p1, prefix) in subtopics {
    //     let oq = ss.oqs.get(&topic_name).unwrap();
    //     let rel = prefix.join("/");
    //
    //     let rurl = format!("{}/", rel);
    //
    //     let dotsep = prefix.join(".");
    //     topics.insert(dotsep, oq.tr.to_wire(Some(rurl)));
    // }

    node_app_data.insert("get_sc_meta".to_string(), debug_s);
    let ss = ss_mutex.lock().await;

    let index = TopicsIndexInternal {
        node_id: ss.node_id.clone(),
        node_started: ss.node_started,
        node_app_data,
        topics,
    };
    //
    // return topics_index;
    //
    //     let index = topics_index(&ss);
    //     let cbor_bytes = serde_cbor::to_vec(&index).unwrap();
    //     cbor_bytes
    //
    //     debug!("get_sc_meta: END: {:#?}", index);
    Ok(index)
}

async fn single_compose(
    sc: &SourceComposition,
    ss_mutex: ServerStateAccess,
) -> DTPSR<ResolvedData> {
    // let ss = ss_mutex.lock().await;

    // let mut result_dict = BTreeMap::new();
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

        let value = source.resolve_data_single(ss_mutex.clone()).await?;

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
                let x = get_as_cbor(&rd);
                where_to_put.insert(key_to_put, x);
            }
        }
    }

    Ok(Regular(result_dict))
}

#[async_recursion]
pub async fn interpret_path(path: &str, ss_mutex: ServerStateAccess) -> DTPSR<TypeOFSource> {
    let path_components0 = divide_in_components(&path, '/');
    let path_components = path_components0.clone();
    interpret_path_components(&path_components, ss_mutex.clone()).await
}

#[async_recursion]
pub async fn interpret_path_components(
    path_components: &Vec<String>,
    ss_mutex: ServerStateAccess,
) -> DTPSR<TypeOFSource> {
    if path_components.contains(&"!".to_string()) {
        return DTPSError::other(format!(
            "interpret_path: Cannot have ! in path: {:?}",
            path_components
        ));
    }
    let deref: String = ":deref".to_string();
    if path_components.contains(&deref) {
        let i = path_components.iter().position(|x| x == &deref).unwrap();
        let before = path_components.get(0..i).unwrap().to_vec();
        let after = path_components.get(i + 1..).unwrap().to_vec();
        debug!("interpret_path: before: {:?} after: {:?}", before, after);

        let interpret_before = interpret_path_components(&before, ss_mutex.clone()).await?;

        let first_part = match interpret_before {
            TypeOFSource::Compose(sc) => TypeOFSource::Deref(sc),
            _ => {
                return DTPSError::other(format!(
                    "interpret_path: deref: before is not Compose: {:?}",
                    interpret_before
                ));
            }
        };
        return if after.len() == 0 {
            Ok(first_part)
        } else {
            Ok(TypeOFSource::Transformed(
                Box::new(first_part),
                Transforms::GetInside(after),
            ))
        };
    }

    if path_components.len() == 0 {
        let ss = ss_mutex.lock().await;
        // let q = ss.oqs.get("").unwrap();
        let mut topics = hashmap! {};
        for (topic_name, oq) in &ss.oqs {
            if topic_name == "" {
                continue;
            }
            let prefix = divide_in_components(&topic_name, '.');
            // let rurl = make_rel_url(&prefix.clone());
            let val = TypeOFSource::OurQueue(
                topic_name.clone(),
                prefix.clone(),
                oq.tr.properties.clone(),
            );

            topics.insert(prefix.clone(), Box::new(val));
        }
        let sc0 = TypeOFSource::Compose(SourceComposition {
            is_root: true,
            compose: topics,
        });
        return Ok(sc0);
    }

    if path_components.len() > 1 {
        if path_components.first().unwrap() == "ipfs" {
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

        // return Err("Empty path".to_string());
    }

    // look for all the keys in our queues
    let keys: Vec<String> = {
        let ss = ss_mutex.lock().await;
        ss.oqs.keys().cloned().collect()
    };
    log::debug!(" = path_components = {:?} ", path_components);
    let mut subtopics: Vec<(String, Vec<String>, Vec<String>)> = vec![];
    let mut subtopics_vec = vec![];
    for k in &keys {
        let topic_components = divide_in_components(&k, '.');

        subtopics_vec.push(topic_components.clone());

        if topic_components.len() == 0 {
            continue;
        }

        match is_prefix_of(&topic_components, &path_components) {
            None => {}
            Some((p1, rest)) => {
                let ss = ss_mutex.lock().await;
                let oq = ss.oqs.get(k).unwrap();
                let source_type = TypeOFSource::OurQueue(k.clone(), p1, oq.tr.properties.clone());
                return if rest.len() == 0 {
                    Ok(source_type)
                } else {
                    Ok(TypeOFSource::Transformed(
                        Box::new(source_type),
                        Transforms::GetInside(rest),
                    ))
                };
            }
        };

        // log::debug!("k: {:?} = components = {:?} ", k, components);

        let (matched, rest) = match is_prefix_of(&path_components, &topic_components) {
            None => continue,

            Some(rest) => rest,
        };

        subtopics.push((k.clone(), matched, rest));
    }

    eprintln!("subtopics: {:?}", subtopics);
    if subtopics.len() == 0 {
        let s = format!(
            "Cannot find a matching topic for {:?}.\nMy Topics: {:?}\n",
            path_components, subtopics_vec
        );
        return Err(DTPSError::TopicNotFound(s));
    }

    let mut sc = SourceComposition {
        is_root: false,
        compose: hashmap! {},
    };
    let ss = ss_mutex.lock().await;
    for (topic_name, p1, rest) in subtopics {
        let oq = ss.oqs.get(&topic_name).unwrap();
        let source_type = TypeOFSource::OurQueue(topic_name.clone(), p1, oq.tr.properties.clone());
        sc.compose.insert(rest.clone(), Box::new(source_type));
    }

    Ok(TypeOFSource::Compose(sc))
}
