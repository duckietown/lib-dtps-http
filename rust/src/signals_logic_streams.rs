use std::{
    cmp::{
        max,
        min,
    },
    collections::{
        BTreeMap,
        HashSet,
    },
    fmt::Debug,
};
use tokio::sync::broadcast::Receiver as BroadcastReceiver;

use async_recursion::async_recursion;
use async_trait::async_trait;
use chrono::Local;
use serde_cbor::{
    Value as CBORValue,
    Value::Null as CBORNull,
};
use tokio::sync::broadcast::error::RecvError;

use crate::{
    client::{
        get_events_stream_inline,
        get_rawdata_status,
    },
    debug_with_info,
    error_with_info,
    get_channel_info_message,
    get_metadata,
    is_prefix_of,
    merge_clocks,
    not_implemented,
    putinside,
    utils::time_nanos_i64,
    warn_with_info,
    ActualUpdate,
    ChannelInfo,
    Clocks,
    DTPSError,
    DataSaved,
    DataStream,
    ErrorMsg,
    FinishedMsg,
    GetStream,
    InsertNotification,
    ListenURLEvents,
    RawData,
    ResolvedData,
    ServerStateAccess,
    SilenceMsg,
    SourceComposition,
    TopicName,
    TopicProperties,
    TopicsIndexInternal,
    TopicsIndexWire,
    Transforms,
    TypeOFSource,
    TypeOfConnection,
    WarningMsg,
    CONTENT_TYPE_DTPS_INDEX_CBOR,
    DTPSR,
};

#[async_recursion]
async fn get_stream_compose_data(
    presented_as: &str,
    ssa: ServerStateAccess,
    sc: &SourceComposition,
) -> DTPSR<DataStream> {
    // let mut components = HashMap::new();
    let mut handles = Vec::new();
    let mut queue_created: i64 = time_nanos_i64();
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

    let first_val = RawData::from_cbor_value(&first)?;
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
    async fn get_data_stream(&self, presented_as: &str, ssa: ServerStateAccess) -> DTPSR<DataStream> {
        match self {
            TypeOFSource::Digest(..) => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::ForwardedQueue(q) => {
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
                // debug_with_info!("get_data_stream() for {topic_name:?} with {first:?}");
                Ok(DataStream {
                    channel_info,
                    first,
                    stream: Some(rx),
                    handles: vec![],
                })
            }

            TypeOFSource::Compose(sc) => get_stream_compose_meta(presented_as, ssa, sc).await,
            TypeOFSource::Transformed(tos, tr) => get_stream_transform(presented_as, ssa, tos, tr).await,

            TypeOFSource::Deref(d) => get_stream_compose_data(presented_as, ssa, d).await,
            TypeOFSource::OtherProxied(..) => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::MountedDir(..) => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::MountedFile { .. } => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::Index(..) => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::Aliased(..) => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
            TypeOFSource::History(..) => {
                not_implemented!("get_stream for {self:#?} with {self:?}")
            }
        }
    }
}

async fn listen_to_updates(
    component: Vec<String>,
    mut rx: BroadcastReceiver<ListenURLEvents>,
    tx: tokio::sync::broadcast::Sender<SingleUpdates>,
) -> DTPSR<()> {
    loop {
        match rx.recv().await {
            Ok(m) => {
                let component = component.clone();
                let msgs = match m {
                    ListenURLEvents::InsertNotification(m) => {
                        vec![SingleUpdates::Update(ActualUpdate {
                            component,
                            data: ResolvedData::RawData(m.raw_data),
                            clocks: m.data_saved.clocks,
                        })]
                    }
                    ListenURLEvents::WarningMsg(m) => {
                        vec![SingleUpdates::WarningMsg(component, m)]
                    }
                    ListenURLEvents::ErrorMsg(m) => {
                        vec![SingleUpdates::ErrorMsg(component, m)]
                    }
                    ListenURLEvents::FinishedMsg(m) => {
                        vec![SingleUpdates::FinishedMsg(component, m)]
                    }
                    ListenURLEvents::SilenceMsg(m) => {
                        vec![SingleUpdates::SilenceMsg(component, m)]
                    }
                };
                for to_send in msgs {
                    if tx.receiver_count() > 0 {
                        tx.send(to_send).unwrap();
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
    tx.send(SingleUpdates::Finished(component)).unwrap();
    Ok(())
}

async fn filter_stream<T, U, F, G>(
    mut receiver: BroadcastReceiver<T>,
    sender: tokio::sync::broadcast::Sender<U>,
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
    FinishedMsg(Vec<String>, FinishedMsg),
    ErrorMsg(Vec<String>, ErrorMsg),
    WarningMsg(Vec<String>, WarningMsg),
    SilenceMsg(Vec<String>, SilenceMsg),
}

async fn put_together(
    mut first: CBORValue, // BTreeMap<CBORValue, CBORValue>,
    mut clocks0: Clocks,
    mut active_components: HashSet<Vec<String>>,
    mut rx: BroadcastReceiver<SingleUpdates>,
    tx_out: tokio::sync::broadcast::Sender<ListenURLEvents>,
) -> DTPSR<()> {
    if let CBORValue::Map(..) = first {
    } else {
        return Err(DTPSError::Other(format!("First value {first:?} is not a map")));
    };
    let mut index = 1;
    loop {
        let msg = rx.recv().await?;
        let to_send = match msg {
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
                let time_inserted = time_nanos_i64();
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
                index += 1;

                vec![ListenURLEvents::InsertNotification(InsertNotification {
                    data_saved,
                    raw_data: rd,
                })]
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
                vec![]
            }
            SingleUpdates::FinishedMsg(comp, m) => {
                vec![]
            }
            SingleUpdates::ErrorMsg(comp, m) => {
                vec![ListenURLEvents::ErrorMsg(m)] // TODO: add component
            }
            SingleUpdates::WarningMsg(comp, m) => {
                vec![ListenURLEvents::WarningMsg(m)] // TODO: add component
            }
            SingleUpdates::SilenceMsg(comp, m) => {
                // TODO only if didn't send in a while
                vec![ListenURLEvents::SilenceMsg(m)] // TODO: add component
            }
        };
        for ts in to_send {
            if tx_out.receiver_count() > 0 {
                tx_out.send(ts).unwrap();
            }
        }
    }

    Ok(())
}

async fn transform_for(
    receiver: BroadcastReceiver<ListenURLEvents>,
    out_stream_sender: tokio::sync::broadcast::Sender<ListenURLEvents>,
    prefix: TopicName,
    presented_as: String,
    unique_id: String,
    first: Option<ListenURLEvents>,
) -> DTPSR<()> {
    let compare = |a: &ListenURLEvents, b: &ListenURLEvents| a == b;
    let f = |in1: ListenURLEvents| filter_func_outer(in1, prefix.clone(), presented_as.clone(), unique_id.clone());
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
        Some(ListenURLEvents::InsertNotification(in0.clone())),
    );
    handles.push(tokio::spawn(future));

    let queue_created: i64 = time_nanos_i64();
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

fn filter_transform_outer(in1: ListenURLEvents, t: &Transforms, unique_id: String) -> DTPSR<ListenURLEvents> {
    match in1 {
        ListenURLEvents::InsertNotification(x) => {
            Ok(ListenURLEvents::InsertNotification(filter_transform(x, t, unique_id)?))
        }
        ListenURLEvents::WarningMsg(_)
        | ListenURLEvents::ErrorMsg(_)
        | ListenURLEvents::FinishedMsg(_)
        | ListenURLEvents::SilenceMsg(_) => Ok(in1),
    }
}

async fn apply_transformer(
    receiver: BroadcastReceiver<ListenURLEvents>,
    out_stream_sender: tokio::sync::broadcast::Sender<ListenURLEvents>,
    transform: Transforms,
    unique_id: String,
    first: Option<ListenURLEvents>,
) -> DTPSR<()> {
    let are_they_same = |a: &ListenURLEvents, b: &ListenURLEvents| a == b;
    let f = |in1: ListenURLEvents| filter_transform_outer(in1, &transform, unique_id.clone());
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
    let (out_stream_sender, out_stream_recv) = tokio::sync::broadcast::channel::<ListenURLEvents>(1024);

    let unique_id = "XXX".to_string();

    let in0 = filter_transform(stream0.first.unwrap(), transform, unique_id.clone())?;

    let future = apply_transformer(
        receiver,
        out_stream_sender,
        transform.clone(),
        unique_id,
        Some(ListenURLEvents::InsertNotification(in0.clone())),
    );
    handles.push(tokio::spawn(future));

    let queue_created: i64 = time_nanos_i64();
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
    let v1 = in1.raw_data.interpret::<TopicsIndexWire>()?;
    let v2 = filter_index(&v1, prefix, presented_as)?;
    let rd = RawData::represent_as_cbor_ct(v2, CONTENT_TYPE_DTPS_INDEX_CBOR)?;

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

fn filter_func_outer(
    in1: ListenURLEvents,
    prefix: TopicName,
    presented_as: String,
    unique_id: String,
) -> DTPSR<ListenURLEvents> {
    match in1 {
        ListenURLEvents::InsertNotification(x) => Ok(ListenURLEvents::InsertNotification(filter_func(
            x,
            prefix,
            presented_as,
            unique_id,
        )?)),
        ListenURLEvents::WarningMsg(_)
        | ListenURLEvents::ErrorMsg(_)
        | ListenURLEvents::FinishedMsg(_)
        | ListenURLEvents::SilenceMsg(_) => Ok(in1),
    }
}

pub fn transform(data: ResolvedData, transform: &Transforms) -> DTPSR<ResolvedData> {
    let d = match data {
        ResolvedData::Regular(d) => d,
        ResolvedData::NotAvailableYet(s) => return Ok(ResolvedData::NotAvailableYet(s)),
        ResolvedData::NotFound(s) => return Ok(ResolvedData::NotFound(s)),
        ResolvedData::RawData(rd) => rd.get_as_cbor()?,
    };
    Ok(ResolvedData::Regular(transform.apply(d)?))
}

async fn get_data_stream_from_url(con: &TypeOfConnection) -> DTPSR<DataStream> {
    let md = get_metadata(con).await?;
    let inline_url = md.events_data_inline_url.unwrap();
    let (handle, stream) = get_events_stream_inline(&inline_url).await;

    let handles = vec![handle];

    let queue_created: i64 = time_nanos_i64();
    let num_total = 1;

    let (status, raw_data) = get_rawdata_status(con).await?;
    let first = if status == http::StatusCode::NO_CONTENT {
        None
    } else {
        Some(InsertNotification {
            data_saved: DataSaved {
                // FIXME
                origin_node: "".to_string(),
                unique_id: "".to_string(),
                index: 0,
                time_inserted: 0,
                clocks: Default::default(),
                content_type: raw_data.content_type.clone(),
                content_length: raw_data.content.len(),
                digest: raw_data.digest(),
            },
            raw_data,
        })
    };

    let data_stream = DataStream {
        channel_info: ChannelInfo {
            queue_created,
            num_total,
            newest: None,
            oldest: None,
        },
        first,
        stream: Some(stream),
        handles,
    };
    Ok(data_stream)
}
