use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
};
use tokio::sync::{broadcast as tokio_broadcast, mpsc as tokio_mpsc};

use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use http::StatusCode;

use crate::signals_logic::ForwardedQueue;
use crate::types::unique_reader_id;
use crate::{
    context, debug_with_info, get_dataready, get_resolved, make_request2, not_implemented, putinside,
    signals_logic_streams::transform, DataReady, FoundMetadata, GetMeta, OtherProxied, RawData, ResolveDataSingle,
    ResolvedData, ResponseResult, RicherRawData, ServerStateAccess, SourceComposition, TopicName, TopicProperties,
    TypeOFSource, CONTENT_TYPE_DTPS_INDEX_CBOR, CONTENT_TYPE_TOPIC_HISTORY_CBOR, DTPSR,
};
use crate::{get_rawdata, get_rawdata_status};
use crate::{DataSaved, ResponseUnobtained};

#[async_trait]
impl ResolveDataSingle for TypeOFSource {
    async fn resolve_data_single(&self, presented_as: &str, ss_mutex: ServerStateAccess) -> DTPSR<ResolvedData> {
        match self {
            TypeOFSource::SingleUse(su) => {
                let mut ss = ss_mutex.lock().await;
                let data = ss.blob_manager.get_blob_once(&su.digest, &su.reader, su.seq)?;

                let raw_data = RawData::new(data, &su.content_type, Some(su.digest.clone()));
                let rrd: RicherRawData = RicherRawData {
                    raw_data,
                    metadata: FoundMetadata::empty(),
                };
                Ok(ResolvedData::RicherRawData(rrd))
            }
            TypeOFSource::ForwardedQueue(q) => resolve_data_single_forwarded_queue(q, presented_as, ss_mutex).await,
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
                    digest: None,
                };

                Ok(ResolvedData::from_raw_data(raw_data))
            }
            TypeOFSource::Transformed(source, transforms) => {
                let data = source.resolve_data_single(presented_as, ss_mutex.clone()).await?;
                transform(data.clone(), transforms)
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
                        digest: None,
                    };
                    return Ok(ResolvedData::from_raw_data(raw_data));
                }
                not_implemented!("MountedDir:\n{self:#?}")
            }
            TypeOFSource::MountedFile { filename, .. } => {
                let data = std::fs::read(filename)?;
                let content_type = mime_guess::from_path(filename)
                    .first()
                    .unwrap_or(mime::APPLICATION_OCTET_STREAM);
                let rd = RawData::new(data, content_type, None);

                Ok(ResolvedData::from_raw_data(rd))
            }
            TypeOFSource::Index(inside) => {
                let x = inside.get_meta_index(presented_as, ss_mutex).await?;
                let xw = x.to_wire(None);
                // convert to cbor
                let cbor_bytes = serde_cbor::to_vec(&xw).unwrap();
                let raw_data = RawData {
                    content: Bytes::from(cbor_bytes),
                    content_type: CONTENT_TYPE_DTPS_INDEX_CBOR.to_string(),
                    digest: None,
                };
                return Ok(ResolvedData::from_raw_data(raw_data));
            }
            TypeOFSource::Aliased(_, _) => {
                not_implemented!("resolve_data_single for:\n{self:#?}")
            }
            TypeOFSource::History(s) => {
                let x: &TypeOFSource = s;
                match x {
                    TypeOFSource::OurQueue(topic, _) => {
                        let mut ss = ss_mutex.lock().await;
                        let dss: Vec<DataSaved> = {
                            let mut res = vec![];
                            let q = ss.get_queue(topic)?;
                            for index in q.stored.iter() {
                                let s = q.saved.get(index).unwrap();
                                res.push(s.clone());
                            }
                            res
                        };
                        let mut available: HashMap<usize, DataReady> = HashMap::new();
                        let reader_id = unique_reader_id();
                        for s in dss {
                            available.insert(s.index, get_dataready(&mut ss.blob_manager, &s, &reader_id));
                        }
                        let history = available;
                        let bytes = serde_cbor::to_vec(&history).unwrap();
                        let raw_data = RawData::new(bytes, CONTENT_TYPE_TOPIC_HISTORY_CBOR, None);
                        return Ok(ResolvedData::from_raw_data(raw_data));
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

    let con = con0.join(rest)?;

    debug_with_info!("Proxied: {:?} -> {:?}", con0, con);

    get_resolved(&con, None).await
    //
    // let rd = get_rawdata(&con).await?;
    //
    // Ok(ResolvedData::RawData(rd))
}

async fn resolve_our_queue(topic_name: &TopicName, ss_mutex: ServerStateAccess) -> DTPSR<ResolvedData> {
    let ss = ss_mutex.lock().await;
    if !ss.oqs.contains_key(topic_name) {
        return Ok(ResolvedData::NotFound(format!(
            "No queue with name {:?}",
            topic_name.as_dash_sep()
        )));
    }
    let oq = ss.oqs.get(topic_name).unwrap();

    return match oq.stored.last() {
        None => {
            let s = format!("No data in queue {:?}", topic_name.as_dash_sep());
            Ok(ResolvedData::NotAvailableYet(s))
        }
        Some(v) => {
            let data_saved = oq.saved.get(v).unwrap();
            let content = context!(
                ss.blob_manager.get_blob_bytes(&data_saved.digest),
                "Cannot get blob bytes for topic {:?}:\n {data_saved:#?}",
                topic_name.as_dash_sep(),
            )?;
            let raw_data = RawData::new(content, &data_saved.content_type, Some(data_saved.digest.clone()));
            // debug_with_info!(" {topic_name:?} -> {raw_data:?}");
            Ok(ResolvedData::from_raw_data(raw_data))
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
        let value = context!(
            source.resolve_data_single(presented_as, ss_i).await,
            "Cannot resolve data for prefix {:#?}",
            prefix
        )?;
        putinside(&mut result_dict, prefix, value)?;
    }

    Ok(ResolvedData::from_cborvalue(result_dict))
}

async fn resolve_data_single_forwarded_queue(
    fq: &ForwardedQueue,
    presented_as: &str,
    ss_mutex: ServerStateAccess,
) -> DTPSR<ResolvedData> {
    let ss = ss_mutex.lock().await;
    let pt = ss.proxied_topics.get(&fq.my_topic_name).unwrap();
    let use_url = &pt.data_url;

    let r2 = make_request2(use_url, hyper::Method::GET, b"", None, None).await?;

    let rd_res: DTPSR<ResolvedData> = r2.into();

    let mut rd = rd_res?;
    if pt.mask_origin {
        if let ResolvedData::RicherRawData(rrd) = rd {
            let mut rrd = rrd;
            rrd.metadata = FoundMetadata::empty();
            rd = ResolvedData::RicherRawData(rrd);
        }
    }

    Ok(rd)
}
