use std::{
    collections::{
        BTreeMap,
        HashMap,
    },
    path::PathBuf,
};

use anyhow::Context;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    client::get_rawdata_status,
    context,
    debug_with_info,
    get_dataready,
    get_rawdata,
    not_implemented,
    signals_logic::{
        GetMeta,
        OtherProxied,
        ResolveDataSingle,
        SourceComposition,
        TypeOFSource,
    },
    signals_logic_streams::transform,
    utils_cbor::putinside,
    DataReady,
    RawData,
    ResolvedData,
    ResolvedData::{
        NotAvailableYet,
        NotFound,
        Regular,
    },
    ServerStateAccess,
    TopicName,
    TopicProperties,
    CONTENT_TYPE_DTPS_INDEX_CBOR,
    CONTENT_TYPE_TOPIC_HISTORY_CBOR,
    DTPSR,
};

#[async_trait]
impl ResolveDataSingle for TypeOFSource {
    async fn resolve_data_single(&self, presented_as: &str, ss_mutex: ServerStateAccess) -> DTPSR<ResolvedData> {
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

                let (status, rd) = get_rawdata_status(use_url).await?;
                if status == 209 {
                    let msg = "This topic does not have any data yet".to_string();
                    Ok(ResolvedData::NotAvailableYet(msg))
                } else {
                    Ok(ResolvedData::RawData(rd))
                }
            }
            TypeOFSource::OurQueue(q, _) => resolve_our_queue(q, ss_mutex).await,
            TypeOFSource::Compose(_sc) => {
                let index = self.get_meta_index(presented_as, ss_mutex).await?;
                // debug_with_info!("Compose index intenral:\n {:#?}", index);
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
                let data = source.resolve_data_single(presented_as, ss_mutex.clone()).await?;

                transform(data, transforms)
            }
            TypeOFSource::Deref(sc) => single_compose(sc, presented_as, ss_mutex).await,
            TypeOFSource::OtherProxied(op) => resolve_proxied(op).await,

            TypeOFSource::MountedDir(topic_name, _, _comps) => {
                let ss = ss_mutex.lock().await;
                let the_dir = ss.local_dirs.get(topic_name).unwrap();
                let the_path = PathBuf::from(&the_dir.local_dir);
                if !the_path.exists() {
                    return Ok(ResolvedData::NotFound(format!("Not found: {:?}", the_path)));
                }
                if the_path.is_dir() {
                    drop(ss);
                    let index = self.get_meta_index(presented_as, ss_mutex).await?;
                    // debug_with_info!("Compose index intenral:\n {:#?}", index);
                    let to_wire = index.to_wire(None);

                    // convert to cbor
                    let cbor_bytes = serde_cbor::to_vec(&to_wire).unwrap();
                    let raw_data = RawData {
                        content: Bytes::from(cbor_bytes),
                        content_type: CONTENT_TYPE_DTPS_INDEX_CBOR.to_string(),
                    };
                    return Ok(ResolvedData::RawData(raw_data));
                }
                not_implemented!("MountedDir:\n{self:#?}")
            }
            TypeOFSource::MountedFile { filename, .. } => {
                let data = std::fs::read(filename)?;
                let content_type = mime_guess::from_path(&filename)
                    .first()
                    .unwrap_or(mime::APPLICATION_OCTET_STREAM);
                let rd = RawData::new(data, content_type);

                Ok(ResolvedData::RawData(rd))
            }
            TypeOFSource::Index(inside) => {
                let x = inside.get_meta_index(presented_as, ss_mutex).await?;
                let xw = x.to_wire(None);
                // convert to cbor
                let cbor_bytes = serde_cbor::to_vec(&xw).unwrap();
                let raw_data = RawData {
                    content: Bytes::from(cbor_bytes),
                    content_type: CONTENT_TYPE_DTPS_INDEX_CBOR.to_string(),
                };
                return Ok(ResolvedData::RawData(raw_data));
            }
            TypeOFSource::Aliased(_, _) => {
                not_implemented!("resolve_data_single for:\n{self:#?}")
            }
            TypeOFSource::History(s) => {
                let x: &TypeOFSource = s;
                match x {
                    TypeOFSource::OurQueue(topic, _) => {
                        let ss = ss_mutex.lock().await;
                        let q = ss.get_queue(topic)?;
                        let mut available: HashMap<usize, DataReady> = HashMap::new();
                        for index in q.stored.iter() {
                            let s = q.saved.get(index).unwrap();
                            available.insert(s.index, get_dataready(s));
                        }
                        let history = available;
                        let bytes = serde_cbor::to_vec(&history).unwrap();
                        return Ok(ResolvedData::RawData(RawData::new(
                            bytes,
                            CONTENT_TYPE_TOPIC_HISTORY_CBOR,
                        )));
                    }
                    TypeOFSource::Compose(sc) => {
                        if sc.topic_name.is_root() {
                            let ds2 = TypeOFSource::History(Box::new(TypeOFSource::OurQueue(
                                TopicName::root(),
                                TopicProperties::rw(),
                            )));
                            return ds2.resolve_data_single(presented_as, ss_mutex).await;
                        }
                        not_implemented!("resolve_data_single for:\n{self:#?}")
                    }
                    _ => {
                        not_implemented!("resolve_data_single for:\n{self:#?}")
                    }
                }
            }
        }
    }
}

pub async fn resolve_proxied(op: &OtherProxied) -> DTPSR<ResolvedData> {
    let con0 = op.op.con.clone();

    let rest = &op.path_and_query;

    let con = con0.join(&rest)?;

    debug_with_info!("Proxied: {:?} -> {:?}", con0, con);

    let rd = get_rawdata(&con).await?;

    Ok(ResolvedData::RawData(rd))
}

async fn resolve_our_queue(topic_name: &TopicName, ss_mutex: ServerStateAccess) -> DTPSR<ResolvedData> {
    let ss = ss_mutex.lock().await;
    if !ss.oqs.contains_key(topic_name) {
        return Ok(NotFound(format!("No queue with name {:?}", topic_name.as_dash_sep())));
    }
    let oq = ss.oqs.get(topic_name).unwrap();

    return match oq.stored.last() {
        None => {
            let s = format!("No data in queue {:?}", topic_name.as_dash_sep());
            Ok(NotAvailableYet(s))
        }
        Some(v) => {
            let data_saved = oq.saved.get(v).unwrap();
            let content = context!(
                ss.get_blob_bytes(&data_saved.digest),
                "Cannot get blob bytes for topic {:?}:\n {data_saved:#?}",
                topic_name.as_dash_sep(),
            )?;
            let raw_data = RawData::new(content, &data_saved.content_type);
            // debug_with_info!(" {topic_name:?} -> {raw_data:?}");
            Ok(ResolvedData::RawData(raw_data))
        }
    };
}

async fn single_compose(
    sc: &SourceComposition,
    presented_as: &str,
    ss_mutex: ServerStateAccess,
) -> DTPSR<ResolvedData> {
    let mut result_dict: serde_cbor::value::Value = serde_cbor::value::Value::Map(BTreeMap::new());

    for (prefix, source) in &sc.compose {
        let ss_i = ss_mutex.clone();
        let value = source.resolve_data_single(presented_as, ss_i).await?;
        putinside(&mut result_dict, prefix, value)?;
    }

    Ok(Regular(result_dict))
}
