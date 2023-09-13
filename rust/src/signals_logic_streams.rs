use std::{
    cmp::{
        max,
        min,
    },
    collections::{
        BTreeMap,
        HashMap,
        HashSet,
    },
    fmt::Debug,
    path::PathBuf,
};

use anyhow::Context;
use async_recursion::async_recursion;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Local;
use futures::StreamExt;
use json_patch::{
    patch,
    Patch,
    PatchOperation,
};
use maplit::hashmap;
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};
use serde_cbor::{
    Value as CBORValue,
    Value::{
        Null as CBORNull,
        Text as CBORText,
    },
};
use tokio::{
    sync::broadcast::{
        error::RecvError,
        Receiver,
        Sender,
    },
    task::JoinHandle,
};

use crate::{
    client::get_rawdata_status,
    context,
    debug_with_info,
    divide_in_components,
    error_with_info,
    get_channel_info_message,
    get_dataready,
    get_inside,
    get_rawdata,
    is_prefix_of,
    merge_clocks,
    not_implemented,
    parse_url_ext,
    signals_logic::{
        ActualUpdate,
        DataStream,
        GetStream,
        SourceComposition,
        Transforms,
        TypeOFSource,
    },
    unescape_json_patch,
    utils,
    utils_cbor::putinside,
    warn_with_info,
    ChannelInfo,
    Clocks,
    ContentInfo,
    DTPSError,
    DataReady,
    DataSaved,
    ForwardingStep,
    InsertNotification,
    LinkBenchmark,
    OtherProxyInfo,
    ProxyJob,
    RawData,
    ResolvedData,
    ResolvedData::{
        NotAvailableYet,
        NotFound,
        Regular,
    },
    ServerState,
    ServerStateAccess,
    TopicName,
    TopicProperties,
    TopicReachabilityInternal,
    TopicRefAdd,
    TopicRefInternal,
    TopicsIndexInternal,
    TopicsIndexWire,
    TypeOfConnection,
    TypeOfConnection::Relative,
    CONTENT_TYPE_DTPS_INDEX_CBOR,
    CONTENT_TYPE_TOPIC_HISTORY_CBOR,
    DTPSR,
    REL_URL_META,
    TOPIC_PROXIED,
    URL_HISTORY,
};

#[async_recursion]
async fn get_stream_compose_data(
    presented_as: &str,
    ssa: ServerStateAccess,
    sc: &SourceComposition,
) -> DTPSR<DataStream> {
    // let mut components = HashMap::new();
    let mut handles = Vec::new();
    let mut queue_created: i64 = Local::now().timestamp_nanos();
    let mut num_total = 0;
    let mut time_inserted = 0;
    let clocks0 = Clocks::default();
    let mut first = serde_cbor::value::Value::Map(BTreeMap::new());

    let mut components_active = HashSet::new();
    let mut components_inactive = HashSet::new();
    let (tx, rx) = tokio::sync::broadcast::channel(1024);
    let (out_stream_sender, out_stream_recv) = tokio::sync::broadcast::channel(1024);

    for (k, v) in sc.compose.iter() {
        let ts: &TypeOFSource = v;
        let mut cs = match ts.get_data_stream(presented_as, ssa.clone()).await {
            Ok(x) => x,
            Err(e) => match e {
                DTPSError::NotImplemented(_) => {
                    warn_with_info!("Not implemented get_data_stream() for component {k:?}:\n{v:?}\n{e:?}");
                    continue;
                }
                _ => {
                    error_with_info!("Error while creating data stream: {:?}", e);
                    return Err(e);
                }
            },
        };
        queue_created = min(queue_created, cs.channel_info.queue_created);
        num_total = max(num_total, cs.channel_info.num_total);

        handles.append(&mut cs.handles);

        match cs.stream {
            None => {
                components_inactive.insert(k.clone());
            }
            Some(st) => {
                components_active.insert(k.clone());
                // let mut st = st.clone();
                let h = tokio::spawn(listen_to_updates(k.clone(), st, tx.clone()));
                handles.push(h);
            }
        }

        match &cs.first {
            None => {
                let value = ResolvedData::Regular(CBORNull);
                putinside(&mut first, k, value)?;
            }
            Some(sd) => {
                time_inserted = max(time_inserted, sd.data_saved.time_inserted);
                let resolved_data = ResolvedData::RawData(sd.raw_data.clone());
                putinside(&mut first, k, resolved_data)?;
            }
        }
    }

    let first_val = crate::RawData::from_cbor_value(&first)?;
    let data_saved = DataSaved {
        origin_node: "".to_string(),
        unique_id: "".to_string(),
        index: 0,
        time_inserted,
        clocks: clocks0.clone(),
        content_type: first_val.content_type.clone(),
        content_length: first_val.content.len(),
        digest: first_val.digest(),
    };
    let handle_merge = tokio::spawn(put_together(
        first.clone(),
        clocks0,
        components_active,
        rx,
        out_stream_sender,
    ));
    handles.push(handle_merge);

    let data_stream = DataStream {
        channel_info: ChannelInfo {
            queue_created,
            num_total,
            newest: None,
            oldest: None,
        },
        first: Some(InsertNotification {
            data_saved,
            raw_data: first_val,
        }),
        stream: Some(out_stream_recv),
        handles,
    };
    Ok(data_stream)
}

#[async_trait]
impl GetStream for TypeOFSource {
    // #[async_recursion]
    async fn get_data_stream(&self, presented_as: &str, ssa: ServerStateAccess) -> DTPSR<DataStream> {
        match self {
            TypeOFSource::Digest(_, _) => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::ForwardedQueue(q) => {
                // not_implemented!("get_stream for {self:#?} with {self:?}")
                let use_url = {
                    let ss = ssa.lock().await;
                    ss.proxied_topics.get(&q.my_topic_name).unwrap().data_url.clone()
                };
                get_data_stream_from_url(&use_url).await
            }
            TypeOFSource::OurQueue(topic_name, ..) => {
                let (rx, first, channel_info) = {
                    let ss = ssa.lock().await;

                    let rx = ss.subscribe_insert_notification(topic_name)?;
                    let first = ss.get_last_insert(topic_name)?;
                    let oq = ss.get_queue(topic_name)?;
                    let channel_info = get_channel_info_message(oq);
                    (rx, first, channel_info)
                };

                Ok(DataStream {
                    channel_info,
                    first,
                    stream: Some(rx),
                    handles: vec![],
                })
            }

            TypeOFSource::Compose(sc) => get_stream_compose_meta(presented_as, ssa, sc).await,
            TypeOFSource::Transformed(tos, tr) => {
                get_stream_transform(presented_as, ssa, tos, tr).await
                // not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::Deref(d) => get_stream_compose_data(presented_as, ssa, d).await,
            TypeOFSource::OtherProxied(_) => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::MountedDir(_, _, _props) => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::MountedFile { .. } => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::Index(_s) => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::Aliased(_, _) => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::History(_) => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
        }
    }
}

async fn listen_to_updates(
    component: Vec<String>,
    mut rx: tokio::sync::broadcast::Receiver<InsertNotification>,
    tx: tokio::sync::broadcast::Sender<SingleUpdates>,
) -> DTPSR<()> {
    loop {
        match rx.recv().await {
            Ok(m) => {
                if tx.receiver_count() > 0 {
                    tx.send(SingleUpdates::Update(ActualUpdate {
                        component: component.clone(),
                        data: ResolvedData::RawData(m.raw_data),
                        clocks: m.data_saved.clocks,
                    }))
                    .unwrap();
                }
            }
            Err(e) => match e {
                RecvError::Closed => {
                    break;
                }
                RecvError::Lagged(e) => {
                    warn_with_info!("Lagged: {e}");
                }
            },
        }
    }
    tx.send(SingleUpdates::Finished(component)).unwrap();
    Ok(())
}

async fn filter_stream<T, U, F, G>(
    mut receiver: Receiver<T>,
    sender: Sender<U>,
    f: F,
    filter_same: bool,
    mut last: Option<U>,
    are_they_same: G,
) -> DTPSR<()>
where
    F: Fn(T) -> DTPSR<U>,
    T: Send + Clone + Debug,
    U: Send + Clone + Debug,
    G: Fn(&U, &U) -> bool,
{
    loop {
        match receiver.recv().await {
            Ok(m) => {
                let u = f(m)?;
                if filter_same {
                    if let Some(a) = &last {
                        if are_they_same(a, &u) {
                            continue;
                        }
                    }
                    last = Some(u.clone());
                }
                if sender.receiver_count() != 0 {
                    match sender.send(u) {
                        Ok(_) => {}
                        Err(e) => {
                            warn_with_info!("Cannot send: {e:?}");
                        }
                    }
                }
            }
            Err(e) => match e {
                RecvError::Closed => {
                    break;
                }
                RecvError::Lagged(e) => {
                    warn_with_info!("Lagged: {e}");
                }
            },
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub enum SingleUpdates {
    Update(ActualUpdate),
    Finished(Vec<String>),
}

async fn put_together(
    mut first: CBORValue, // BTreeMap<CBORValue, CBORValue>,
    mut clocks0: Clocks,
    mut active_components: HashSet<Vec<String>>,
    mut rx: tokio::sync::broadcast::Receiver<SingleUpdates>,
    tx_out: tokio::sync::broadcast::Sender<InsertNotification>,
) -> DTPSR<()> {
    if let CBORValue::Map(..) = first {
    } else {
        return Err(DTPSError::Other(format!("First value {first:?} is not a map")));
    };
    let mut index = 1;
    loop {
        let msg = rx.recv().await?;
        match msg {
            SingleUpdates::Update(ActualUpdate {
                component,
                data,
                clocks,
            }) => {
                if !active_components.contains(&component) {
                    warn_with_info!("Received update for inactive component: {:?}", component);
                    continue;
                }
                clocks0 = merge_clocks(&clocks0, &clocks);
                putinside(&mut first, &component, data)?;
                let rd = RawData::from_cbor_value(&first)?;
                let time_inserted = Local::now().timestamp_nanos();
                let data_saved = DataSaved {
                    origin_node: "".to_string(),
                    unique_id: "".to_string(),
                    index,
                    time_inserted,
                    clocks: clocks0.clone(),
                    content_type: rd.content_type.clone(),
                    content_length: rd.content.len(),
                    digest: rd.digest().clone(),
                };
                let notification = InsertNotification {
                    data_saved,
                    raw_data: rd,
                };
                if tx_out.receiver_count() > 0 {
                    tx_out.send(notification).unwrap();
                }
                index += 1;
            }
            SingleUpdates::Finished(which) => {
                if !active_components.contains(&which) {
                    warn_with_info!("Received update for inactive component: {:?}", which);
                } else {
                    debug_with_info!("Finished  for active component: {:?}", which);
                    active_components.remove(&which);
                    if active_components.is_empty() {
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn transform_for(
    receiver: Receiver<InsertNotification>,
    out_stream_sender: Sender<InsertNotification>,
    prefix: TopicName,
    presented_as: String,
    unique_id: String,
    first: Option<InsertNotification>,
) -> DTPSR<()> {
    let compare = |a: &InsertNotification, b: &InsertNotification| a.raw_data == b.raw_data;
    let f = |in1: InsertNotification| filter_func(in1, prefix.clone(), presented_as.clone(), unique_id.clone());
    filter_stream(receiver, out_stream_sender, f, true, first, compare).await
}

#[async_recursion]
async fn get_stream_compose_meta(
    presented_as: &str,
    ssa: ServerStateAccess,
    sc: &SourceComposition,
) -> DTPSR<DataStream> {
    let tn = TopicName::root();
    let ds0 = TypeOFSource::OurQueue(tn.clone(), TopicProperties::rw());
    let stream0 = ds0.get_data_stream(presented_as, ssa.clone()).await?;
    let mut handles = stream0.handles;
    let receiver = stream0.stream.unwrap();
    let (out_stream_sender, out_stream_recv) = tokio::sync::broadcast::channel(1024);

    let unique_id = sc.unique_id.clone();

    let in0 = filter_func(
        stream0.first.unwrap(),
        sc.topic_name.clone(),
        presented_as.to_string(),
        unique_id.clone(),
    )?;

    let future = transform_for(
        receiver,
        out_stream_sender,
        sc.topic_name.clone(),
        presented_as.to_string(),
        unique_id,
        Some(in0.clone()),
    );
    // let handle1 = tokio::spawn(show_errors(
    //     Some(ssa.clone()),
    //     "receiver".to_string(), future,
    // ));
    handles.push(tokio::spawn(future));

    let queue_created: i64 = Local::now().timestamp_nanos();
    let num_total = 1;

    let data_stream = DataStream {
        channel_info: ChannelInfo {
            queue_created,
            num_total,
            newest: None,
            oldest: None,
        },
        first: Some(in0),
        stream: Some(out_stream_recv),
        handles,
    };
    Ok(data_stream)
}

fn filter_transform(in1: InsertNotification, t: &Transforms, unique_id: String) -> DTPSR<InsertNotification> {
    let v1 = in1.raw_data.get_as_cbor()?;
    let v2 = t.apply(v1)?;
    let rd = RawData::from_cbor_value(&v2)?;

    let ds = &in1.data_saved;
    let data_saved = DataSaved {
        origin_node: ds.origin_node.clone(),
        unique_id,
        index: ds.index,
        time_inserted: ds.time_inserted,
        clocks: ds.clocks.clone(),
        content_type: rd.content_type.clone(),
        content_length: rd.content.len(),
        digest: rd.digest(),
    };
    // FIXME: need to save the blob
    Ok(InsertNotification {
        data_saved,
        raw_data: rd,
    })
}

async fn apply_transformer(
    receiver: Receiver<InsertNotification>,
    out_stream_sender: Sender<InsertNotification>,
    transform: Transforms,
    unique_id: String,
    first: Option<InsertNotification>,
) -> DTPSR<()> {
    let are_they_same = |a: &InsertNotification, b: &InsertNotification| a.raw_data == b.raw_data;
    let f = |in1: InsertNotification| filter_transform(in1, &transform, unique_id.clone());
    filter_stream(receiver, out_stream_sender, f, true, first, are_they_same).await
}

#[async_recursion]
async fn get_stream_transform(
    presented_as: &str,
    ssa: ServerStateAccess,
    ds0: &TypeOFSource,
    transform: &Transforms,
) -> DTPSR<DataStream> {
    let stream0 = ds0.get_data_stream(presented_as, ssa.clone()).await?;
    let mut handles = stream0.handles;
    let receiver = stream0.stream.unwrap();
    let (out_stream_sender, out_stream_recv) = tokio::sync::broadcast::channel(1024);

    let unique_id = "XXX".to_string();

    let in0 = filter_transform(stream0.first.unwrap(), transform, unique_id.clone())?;

    let future = apply_transformer(
        receiver,
        out_stream_sender,
        transform.clone(),
        unique_id,
        Some(in0.clone()),
    );
    handles.push(tokio::spawn(future));

    let queue_created: i64 = Local::now().timestamp_nanos();
    let num_total = 1;

    let data_stream = DataStream {
        channel_info: ChannelInfo {
            queue_created,
            num_total,
            newest: None,
            oldest: None,
        },
        first: Some(in0),
        stream: Some(out_stream_recv),
        handles,
    };
    Ok(data_stream)
}

fn filter_index(data1: &TopicsIndexWire, prefix: TopicName, presented_as: String) -> DTPSR<TopicsIndexWire> {
    let con = TypeOfConnection::Relative(presented_as, None);
    let data1 = TopicsIndexInternal::from_wire(data1, &con);
    let mut data2 = TopicsIndexInternal::default();
    for (k, v) in data1.topics.iter() {
        match is_prefix_of(prefix.as_components(), k.as_components()) {
            None => {}
            Some((_matched, extra_elements)) => {
                let tn2 = TopicName::from_components(&extra_elements);
                data2.topics.insert(tn2, v.clone());
            }
        }
    }
    let data2 = data2.to_wire(None); // XXX

    Ok(data2)
}

fn filter_func(
    in1: InsertNotification,
    prefix: TopicName,
    presented_as: String,
    unique_id: String,
) -> DTPSR<InsertNotification> {
    // let f = |x: &TopicsIndexWire| filter_index(x, prefix, presented_as).unwrap();

    let v1 = in1.raw_data.interpret::<TopicsIndexWire>()?;
    let v2 = filter_index(&v1, prefix, presented_as)?;
    let rd = RawData::represent_as_cbor_ct(v2, CONTENT_TYPE_DTPS_INDEX_CBOR)?;
    //
    // adapt_cbor_map(&in1, f, CONTENT_TYPE_DTPS_INDEX_CBOR.to_string(), "filtered".to_string())
    // not_implemented!("filter_func")
    let ds = &in1.data_saved;
    let data_saved = DataSaved {
        origin_node: ds.origin_node.clone(),
        unique_id,
        index: ds.index,
        time_inserted: ds.time_inserted,
        clocks: ds.clocks.clone(),
        content_type: rd.content_type.clone(),
        content_length: rd.content.len(),
        digest: rd.digest(),
    };
    Ok(InsertNotification {
        data_saved,
        raw_data: rd,
    })
}

pub fn transform(data: ResolvedData, transform: &Transforms) -> DTPSR<ResolvedData> {
    let d = match data {
        Regular(d) => d,
        NotAvailableYet(s) => return Ok(NotAvailableYet(s)),
        NotFound(s) => return Ok(NotFound(s)),
        ResolvedData::RawData(rd) => rd.get_as_cbor()?,
    };
    Ok(ResolvedData::Regular(transform.apply(d)?))
}

async fn get_data_stream_from_url(_con: &TypeOfConnection) -> DTPSR<DataStream> {
    todo!("not implemented");
}
