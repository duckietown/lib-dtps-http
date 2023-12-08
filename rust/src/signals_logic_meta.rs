use std::{collections::HashMap, path::PathBuf};

use async_trait::async_trait;

use futures::StreamExt;

use maplit::hashmap;

use crate::{
    debug_with_info, not_implemented, urls::make_relative, ContentInfo, DTPSError, DataProps, ForwardingStep, GetMeta,
    LinkBenchmark, ServerStateAccess, SourceComposition, TopicName, TopicReachabilityInternal, TopicRefInternal,
    TopicsIndexInternal, TypeOFSource, TypeOfConnection, TypeOfConnection::Relative, DTPSR, MASK_ORIGIN,
};

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

            let a_with_prefix = a.add_prefix(prefix);

            let rurl = a_with_prefix.as_relative_url();
            let _b2 = b.add_path(rurl);

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

#[async_trait]
impl GetMeta for TypeOFSource {
    async fn get_meta_index(&self, presented_as: &str, ss_mutex: ServerStateAccess) -> DTPSR<TopicsIndexInternal> {
        let x = self.get_meta_index_(presented_as, ss_mutex).await?;

        let root = TopicName::root();
        if !x.topics.contains_key(&root) {
            panic!("At url {presented_as}, invalid meta index for {self:?}");
        }
        Ok(x)
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

                if *MASK_ORIGIN {
                    tr.reachability.clear();
                }
                tr.reachability.push(TopicReachabilityInternal {
                    con: Relative(rurl, None),
                    answering: node_id,
                    forwarders: the_forwarders,
                    benchmark: link_benchmark_total,
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
                    con: Relative(rurl, None),
                    answering: node_id,
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
                        con: Relative(filename.clone(), None),
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
