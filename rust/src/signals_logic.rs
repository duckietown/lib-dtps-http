use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use anyhow::Context;
use async_recursion::async_recursion;
use async_trait::async_trait;
use bytes::Bytes;
use log::{debug, error};
use maplit::hashmap;
use serde::{Deserialize, Serialize};
use serde_cbor::Value as CBORValue;
use serde_cbor::Value::Null as CBORNull;
use serde_cbor::Value::Text as CBORText;

use crate::cbor_manipulation::{get_as_cbor, get_inside};
use crate::client::make_request;
use crate::signals_logic::ResolvedData::{NotAvailableYet, NotFound, Regular};
use crate::utils::{divide_in_components, is_prefix_of};
use crate::{
    context, get_content_type, get_rawdata, not_implemented, DTPSError, ForwardingStep,
    LinkBenchmark, NodeAppData, OtherProxyInfo, RawData, ServerState, ServerStateAccess, TopicName,
    TopicReachabilityInternal, TopicRefInternal, TopicsIndexInternal, TypeOfConnection,
    CONTENT_TYPE_DTPS_INDEX_CBOR, DTPSR,
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

    OtherProxied(OtherProxied),
}

impl TypeOFSource {
    pub fn get_inside(&self, s: &str) -> DTPSR<Self> {
        match self {
            TypeOFSource::ForwardedQueue(q) => {
                not_implemented!("get_inside for {self:?} with {s:?}")
            }
            TypeOFSource::OurQueue(_, _, p) => {
                not_implemented!("get_inside for {self:?} with {s:?}")
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
                        not_implemented!("get_inside for {self:?} with {s:?}")
                    }
                } else {
                    Err(DTPSError::TopicNotFound(format!(
                        "File not found: {}",
                        p.join(s).to_str().unwrap()
                    )))
                }

                // not_implemented!("get_inside for {self:?} with {s:?}")
            }
            TypeOFSource::Compose(c) => {
                not_implemented!("get_inside for {self:?} with {s:?}")
            }
            TypeOFSource::Transformed(source, transform) => {
                // not_implemented!("get_inside for {self:?} with {s:?}")
                Ok(TypeOFSource::Transformed(
                    source.clone(),
                    transform.get_inside(s),
                ))
            }
            TypeOFSource::Digest(_, _) => {
                not_implemented!("get_inside for {self:?} with {s:?}")
            }
            TypeOFSource::Deref(c) => {
                not_implemented!("get_inside for {self:?} with {s:?}")
            }
            TypeOFSource::OtherProxied(_) => {
                not_implemented!("get_inside for {self:?} with {s:?}")
            }
            TypeOFSource::MountedFile(_, _, _) => {
                let v = vec![s.to_string()];
                let transform = Transforms::GetInside(v);
                let tr = TypeOFSource::Transformed(Box::new(self.clone()), transform);
                Ok(tr)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct OtherProxied {
    reached_at: TopicName,
    path_and_query: String,
    // query: Option<String>,
    op: OtherProxyInfo,
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
        presented_as: &TopicName,
        ss_mutex: ServerStateAccess,
    ) -> DTPSR<ResolvedData>;
}

#[async_trait]
pub trait GetMeta {
    async fn get_meta_index(
        &self,
        presented_as: &TopicName,
        ss_mutex: ServerStateAccess,
    ) -> DTPSR<TopicsIndexInternal>;
}

// #[async_trait]
// pub trait GetHTML {
//     async fn get_meta_index(
//         &self,
//         presented_as: &TopicName,
//         ss_mutex: ServerStateAccess,
//         headers: HashMap<String, String>,
//     ) -> DTPSR<PreEscaped<String>>;
// }

pub trait DataProps {
    fn get_properties(&self) -> TopicProperties;
}

#[async_trait]
impl ResolveDataSingle for TypeOFSource {
    async fn resolve_data_single(
        &self,
        presented_as: &TopicName,
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
            TypeOFSource::OurQueue(q, _, _) => {
                // if false && q.is_root() {
                //     let ss = ss_mutex.lock().await;
                //     let index_internal = ss.create_topic_index();
                //     let index = index_internal.to_wire(None);
                //
                //     let cbor_bytes = serde_cbor::to_vec(&index).unwrap();
                //     let raw_data = RawData {
                //         content: Bytes::from(cbor_bytes),
                //         content_type: CONTENT_TYPE_DTPS_INDEX_CBOR.to_string(),
                //     };
                //     Ok(ResolvedData::RawData(raw_data))
                // } else {
                our_queue(q, ss_mutex).await
                // }
            }
            TypeOFSource::Compose(sc) => {
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

            TypeOFSource::MountedDir(topic_name, _, comps) => {
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
        }
    }
}

pub async fn resolve_proxied(op: &OtherProxied) -> DTPSR<ResolvedData> {
    let con0 = op.op.con.clone();

    let rest = &op.path_and_query;

    let con = con0.join(&rest)?;

    debug!("Proxied: {:?} -> {:?}", con0, con);

    let resp = context!(
        make_request(&con, hyper::Method::GET).await,
        "Cannot make request to {:?}",
        con
    )?;
    let content_type = get_content_type(&resp);

    let body_bytes = context!(
        hyper::body::to_bytes(resp.into_body()).await,
        "Cannot get body bytes"
    )?;
    let rd = RawData::new(body_bytes, content_type);

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
                }
            }
            TypeOFSource::MountedDir(_, _, props) => props.clone(),
            TypeOFSource::MountedFile(_, _, props) => props.clone(),
        }
    }
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

    return match oq.sequence.last() {
        None => {
            let s = format!("No data in queue {:?}", topic_name);
            Ok(NotAvailableYet(s))
        }
        Some(v) => {
            let content = ss.get_blob_bytes(&v.digest)?;
            let raw_data = RawData::new(content, &v.content_type);
            Ok(ResolvedData::RawData(raw_data))
        }
    };
}

pub const MASK_ORIGIN: bool = false;

#[async_trait]
impl GetMeta for TypeOFSource {
    async fn get_meta_index(
        &self,
        presented_as: &TopicName,
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

                let np = presented_as.as_components().len();
                let components = q.my_topic_name.as_components();
                let rel_components = components[np..].to_vec();
                let rel_topic_name = TopicName::from_components(&rel_components);
                let rurl = rel_topic_name.as_relative_url();

                let mut the_forwarders = the_data.reachability_we_used.forwarders.clone();

                the_forwarders.push(ForwardingStep {
                    forwarding_node: node_id.to_string(),
                    forwarding_node_connects_to: "".to_string(),

                    performance: the_data.link_benchmark_last.clone(),
                });
                let link_benchmark_total = the_data.reachability_we_used.benchmark.clone()
                    + the_data.link_benchmark_last.clone();
                let rurl = format!("HERE/{}", rurl);

                if MASK_ORIGIN {
                    tr.reachability.clear();
                }
                tr.reachability.push(TopicReachabilityInternal {
                    con: TypeOfConnection::Relative(rurl, None),
                    answering: node_id.clone(),
                    forwarders: the_forwarders,
                    benchmark: link_benchmark_total.clone(),
                });

                topics.insert(TopicName::root(), tr);
                let n: NodeAppData = NodeAppData::from("dede");
                let index = TopicsIndexInternal {
                    node_id: "".to_string(),
                    node_started: 0,
                    node_app_data: hashmap! {"here2".to_string() => n},
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

                let np = presented_as.as_components().len();
                let components = topic_name.as_components();
                let rel_components = components[np..].to_vec();
                let rel_topic_name = TopicName::from_components(&rel_components);
                let rurl = rel_topic_name.to_relative_url();

                tr.reachability.push(TopicReachabilityInternal {
                    con: TypeOfConnection::Relative(rurl, None),
                    answering: node_id.clone(),
                    forwarders: vec![],
                    benchmark: LinkBenchmark::identity(), //ok
                });

                let mut topics: HashMap<TopicName, TopicRefInternal> = hashmap! {};
                topics.insert(TopicName::root(), tr);
                let n: NodeAppData = NodeAppData::from("dede");
                let index = TopicsIndexInternal {
                    node_id: "".to_string(),
                    node_started: 0,
                    node_app_data: hashmap! {"here".to_string() => n},
                    topics,
                };
                Ok(index)
            }
            TypeOFSource::Compose(sc) => get_sc_meta(sc, presented_as, ss_mutex).await,
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
                    // if filename.starts_with(".") {
                    //     continue;
                    // }
                    let mut tr = TopicRefInternal {
                        unique_id: "".to_string(),
                        origin_node: "".to_string(),
                        app_data: Default::default(),
                        reachability: vec![],
                        created: 0,
                        properties: props.clone(),
                        accept_content_type: vec![],
                        produces_content_type: vec![],
                        examples: vec![],
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
                    node_id: "".to_string(),
                    node_started: 0,
                    node_app_data: hashmap! {
                        "path".to_string() => NodeAppData::from(the_path),
                    },
                    topics,
                })
            }
            TypeOFSource::MountedFile(_, _, _) => {
                not_implemented!("MountedFile: {self:?}")
            }
        };
    }
}

async fn get_sc_meta(
    sc: &SourceComposition,
    presented_as: &TopicName,
    ss_mutex: ServerStateAccess,
) -> DTPSR<TopicsIndexInternal> {
    if sc.is_root {
        let ss = ss_mutex.lock().await;
        return Ok(ss.create_topic_index());
    }
    let node_id;
    {
        let ss = ss_mutex.lock().await;
        node_id = ss.node_id.clone();
    }
    // debug!("get_sc_meta: START: {:?}", sc);
    let mut topics: HashMap<TopicName, TopicRefInternal> = hashmap! {};
    let mut node_app_data = HashMap::new();

    let debug_s = String::new();
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
        let x = inside
            .get_meta_index(presented_as, ss_mutex.clone())
            .await?;
        // let inside = ts.get_meta_index(ss_mutex.clone()).await?;

        // debug!("get_sc_meta: {:#?} inside: {:#?}", prefix, x);

        // let dotsep = prefix.join(".");

        for (a, b) in x.topics {
            let mut tr = b.clone();

            tr.app_data
                .insert("created by get_sc_meta".to_string(), NodeAppData::from(""));
            //
            // match is_prefix_of(&presented_as.as_components(),  &a) {
            //
            // }

            let url = a.as_relative_url();
            //
            // tr.reachability.push(
            //     TopicReachabilityInternal {
            //         con: TypeOfConnection::Relative(url, None),
            //         answering: node_id.clone(),
            //         forwarders: vec![],
            //         benchmark: LinkBenchmark::identity(),
            //     }
            //
            // );

            // let its = divide_in_components(&a, '.');
            //
            // let full: Vec<String> = vec_concat(&prefix, &its);

            let a_with_prefix = a.add_prefix(&prefix);

            let rurl = a_with_prefix.as_relative_url();
            let b2 = b.add_path(&rurl);

            topics.insert(a_with_prefix, tr);
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
    presented_as: &TopicName,
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
    ss_mutex: ServerStateAccess,
) -> DTPSR<TypeOFSource> {
    debug!("interpret_path: path: {}", path);
    let path_components0 = divide_in_components(&path, '/');
    let path_components = path_components0.clone();

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

        let before2 = before.join("/") + "/";
        let interpret_before = interpret_path(&before2, query, referrer, ss_mutex.clone()).await?;

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
            if topic_name.is_root() {
                continue;
            }
            let prefix = topic_name.as_components();
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

        // return Err("Empty path".to_string());
    }

    // look for all the keys in our queues
    // let keys: Vec<TopicName> = {
    //     let ss = ss_mutex.lock().await;
    //     ss.oqs.keys().cloned().collect()
    // };
    // log::debug!(" = path_components = {:?} ", path_components);

    {
        let ss = ss_mutex.lock().await;
        for (mounted_at, info) in &ss.proxied_other {
            if let Some((_, b)) = is_prefix_of(mounted_at.as_components(), &path_components) {
                let mut path_and_query = b.join("/");
                path_and_query.push_str(&format_query(&query));

                let other = OtherProxied {
                    reached_at: mounted_at.clone(),
                    path_and_query,
                    op: info.clone(),
                };
                return Ok(TypeOFSource::OtherProxied(other));
            }
        }
    }

    let mut subtopics: Vec<(TopicName, Vec<String>, Vec<String>, TypeOFSource)> = vec![];
    let mut subtopics_vec = vec![];

    let all_sources = {
        let ss = ss_mutex.lock().await;
        iterate_type_of_sources(&ss)
    };

    log::debug!(" = all_sources =\n{:#?} ", all_sources);
    for (k, source) in all_sources.iter() {
        debug!(" = k = {:?} ", k);
        // let topic_components = divide_in_components(&k, '.');
        let topic_components = k.as_components();
        subtopics_vec.push(topic_components.clone());

        if topic_components.len() == 0 {
            continue;
        }

        match is_prefix_of(&topic_components, &path_components) {
            None => {}
            Some((_, rest)) => {
                let mut cur = source.clone();
                for a in rest.iter() {
                    cur = cur.get_inside(&a)?;
                }
                return Ok(cur);
                // Ok(TypeOFSource::Transformed(
                //     Box::new(source.clone()),
                //     Transforms::GetInside(rest),
                // ))
            }
        };

        // log::debug!("k: {:?} = components = {:?} ", k, components);

        let (matched, rest) = match is_prefix_of(&path_components, &topic_components) {
            None => continue,

            Some(rest) => rest,
        };

        debug!("pushing: {k:?}");
        subtopics.push((k.clone(), matched, rest, source.clone()));
    }

    debug!("subtopics: {subtopics:?}");
    if subtopics.len() == 0 {
        let s = format!(
            "Cannot find a matching topic for {:?}.\nMy Topics: {:?}\n",
            path_components, subtopics_vec
        );
        error!("{}", s);
        return Err(DTPSError::TopicNotFound(s));
    }

    let mut sc = SourceComposition {
        is_root: false,
        compose: hashmap! {},
    };
    // let ss = ss_mutex.lock().await;
    for (_, _, rest, source) in subtopics {
        // let oq = ss.oqs.get(&topic_name).unwrap();
        // let source_type = TypeOFSource::OurQueue(topic_name.clone(), p1, oq.tr.properties.clone());
        sc.compose.insert(rest.clone(), Box::new(source));
    }
    log::debug!("  \n{sc:#?} ");
    Ok(TypeOFSource::Compose(sc))
}

pub fn iterate_type_of_sources(s: &ServerState) -> HashMap<TopicName, TypeOFSource> {
    let mut res = hashmap! {};
    for (topic_name, x) in s.proxied_topics.iter() {
        let fq = ForwardedQueue {
            subscription: x.from_subscription.clone(),
            his_topic_name: x.its_topic_name.clone(),
            my_topic_name: topic_name.clone(),
            properties: x.tr_original.properties.clone(),
        };
        res.insert(topic_name.clone(), TypeOFSource::ForwardedQueue(fq));

        // let x = ForwardedQueue {
        //     proxied: "".to_string(),
        //     his_topic_name: (),
        //     my_topic_name: (),
        // }
        //
        // res.insert(topic_name.clone(), TypeOFSource::Proxy(x.clone()));
    }

    for (topic_name, oq) in s.oqs.iter() {
        res.insert(
            topic_name.clone(),
            TypeOFSource::OurQueue(
                topic_name.clone(),
                topic_name.to_components(),
                oq.tr.properties.clone(),
            ),
        );
    }

    for (topic_name, oq) in s.local_dirs.iter() {
        let props = TopicProperties {
            streamable: false,
            pushable: false,
            readable: true,
            immutable: false,
        };
        res.insert(
            topic_name.clone(),
            TypeOFSource::MountedDir(topic_name.clone(), oq.local_dir.clone(), props),
        );
    }
    res
}

fn format_query(q: &HashMap<String, String>) -> String {
    if q.len() == 0 {
        return "".to_string();
    }

    let mut res = String::from("?");
    for (k, v) in q {
        res.push_str(k);
        res.push_str("=");
        res.push_str(v);
        res.push_str("&");
    }
    res
}
