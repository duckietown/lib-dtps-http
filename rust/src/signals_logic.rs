use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use maplit::hashmap;
use serde::{Deserialize, Serialize};
use serde_cbor::Value as CBORValue;
use serde_cbor::Value::Null as CBORNull;
use serde_cbor::Value::Text as CBORText;
use serde_yaml;
use tokio::sync::Mutex;

use crate::cbor_manipulation::{get_as_cbor, get_inside};
use crate::signals_logic::ResolvedData::{NotAvailableYet, NotFound, Regular};
use crate::utils::{divide_in_components, is_prefix_of};
use crate::{RawData, ServerState};

#[derive(Debug, Clone)]
pub enum Transforms {
    GetInside(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct SourceComposition {
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
    OurQueue(String, TopicProperties),
    Compose(SourceComposition),
    Transformed(Box<TypeOFSource>, Transforms),
    Digest(String, String),
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
    async fn resolve_data_single(
        &self,
        ss_mutex: Arc<Mutex<ServerState>>,
    ) -> Result<ResolvedData, String>;
}

pub trait DataProps {
    fn get_properties(&self) -> TopicProperties;
}

#[async_trait]
impl ResolveDataSingle for TypeOFSource {
    async fn resolve_data_single(
        &self,
        ss_mutex: Arc<Mutex<ServerState>>,
    ) -> Result<ResolvedData, String> {
        match self {
            TypeOFSource::Digest(digest, content_type) => {
                let ss = ss_mutex.lock().await;
                let data = ss.get_blob_bytes(digest);
                match data {
                    Some(content) => {
                        let rd = RawData {
                            content,
                            content_type: content_type.clone(),
                        };
                        Ok(ResolvedData::RawData(rd))
                    }
                    None => Err(format!("Error getting blob: {}", digest)),
                }
            }
            TypeOFSource::ForwardedQueue(q) => {
                todo!()
            }
            TypeOFSource::OurQueue(q, _) => our_queue(q, ss_mutex).await,
            TypeOFSource::Compose(sc) => single_compose(sc, ss_mutex).await,
            TypeOFSource::Transformed(source, transforms) => {
                let data = source.resolve_data_single(ss_mutex.clone()).await?;
                transform(data, transforms).await
            }
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
            TypeOFSource::OurQueue(_, props) => props.clone(),

            TypeOFSource::Compose(sc) => sc.get_properties(),
            TypeOFSource::Transformed(s, _) => s.get_properties(),
        }
    }
}

async fn transform(data: ResolvedData, transform: &Transforms) -> Result<ResolvedData, String> {
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
                Err(e) => Err(format!("Error getting inside: {}", e)),
            }
        }
    }
}

async fn our_queue(
    topic_name: &str,
    ss_mutex: Arc<Mutex<ServerState>>,
) -> Result<ResolvedData, String> {
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
            let content = match ss.get_blob_bytes(&v.digest) {
                None => return Err(format!("Cannot get data from digest")),
                Some(y) => y,
            };
            let raw_data = RawData {
                content,
                content_type: v.content_type.clone(),
            };
            Ok(ResolvedData::RawData(raw_data))
        }
    };
}

async fn single_compose(
    sc: &SourceComposition,
    ss_mutex: Arc<Mutex<ServerState>>,
) -> Result<ResolvedData, String> {
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

pub async fn interpret_path(
    path_components: &Vec<String>,
    ss_mutex: Arc<Mutex<ServerState>>,
) -> Result<TypeOFSource, String> {
    if path_components.contains(&"!".to_string()) {
        return Err(format!(
            "interpret_path: Cannot have ! in path: {:?}",
            path_components
        ));
    }
    if path_components.len() == 0 {
        let ss = ss_mutex.lock().await;
        let q = ss.oqs.get("").unwrap();
        return Ok(TypeOFSource::OurQueue(
            "".to_string(),
            q.tr.properties.clone(),
        ));
    }

    if path_components.len() > 1 {
        if path_components.first().unwrap() == "ipfs" {
            if path_components.len() != 3 {
                return Err(format!(
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
            Some((_, rest)) => {
                let ss = ss_mutex.lock().await;
                let oq = ss.oqs.get(k).unwrap();
                let source_type = TypeOFSource::OurQueue(k.clone(), oq.tr.properties.clone());
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
        return Err(s);
    }

    let mut sc = SourceComposition {
        compose: hashmap! {},
    };
    let ss = ss_mutex.lock().await;
    for (topic_name, _, rest) in subtopics {
        let oq = ss.oqs.get(&topic_name).unwrap();
        let source_type = TypeOFSource::OurQueue(topic_name.clone(), oq.tr.properties.clone());
        sc.compose.insert(rest.clone(), Box::new(source_type));
    }

    Ok(TypeOFSource::Compose(sc))
}
