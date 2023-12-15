use std::{any::Any, collections::HashSet, fmt::Debug, os::unix::fs::FileTypeExt, path::Path, time::Duration};

use anyhow::Context;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use hex;
use hyper::{self, Client};
use hyper_tls::HttpsConnector;
use hyperlocal::UnixClientExt;
use json_patch::{AddOperation, Patch as JsonPatch, Patch, PatchOperation, RemoveOperation};
use maplit::hashmap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::{
    sync::broadcast::{error::RecvError, Receiver, Receiver as BroadcastReceiver},
    task::JoinHandle,
    time::timeout,
};
use tungstenite::{Message as TM, Message};
use warp::reply::Response;

use crate::signals_logic::MASK_ORIGIN;
use crate::{
    context, debug_with_info, error_with_info, get_content_type, info_with_info, internal_assertion, join_ext,
    not_available, not_implemented, not_reachable, object_queues::InsertNotification, open_websocket_connection,
    parse_url_ext, put_header_accept, put_header_content_type, server_state::ConnectionJob, time_nanos,
    types::CompositeName, warn_with_info, DTPSError, DataSaved, ErrorMsg, FinishedMsg, FoundMetadata, History,
    LinkBenchmark, LinkHeader, ListenURLEvents, MsgClientToServer, MsgServerToClient, ProxyJob, RawData, TopicName,
    TopicRefAdd, TopicsIndexInternal, TopicsIndexWire, TypeOfConnection, CONTENT_TYPE_DTPS_INDEX,
    CONTENT_TYPE_DTPS_INDEX_CBOR, CONTENT_TYPE_PATCH_JSON, CONTENT_TYPE_TOPIC_HISTORY_CBOR, DTPSR,
    HEADER_CONTENT_LOCATION, HEADER_NODE_ID, REL_CONNECTIONS, REL_EVENTS_DATA, REL_EVENTS_NODATA, REL_HISTORY,
    REL_META, REL_PROXIED, REL_STREAM_PUSH,
};

/// Note: need to have use futures::{StreamExt} in scope to use this
pub async fn get_events_stream_inline(
    url: &TypeOfConnection,
) -> (JoinHandle<DTPSR<()>>, BroadcastReceiver<ListenURLEvents>) {
    let (tx, rx) = tokio::sync::broadcast::channel(1024);
    let inline_url = url.clone();
    let handle = tokio::spawn(listen_events_websocket(inline_url, tx));
    // let stream = UnboundedReceiverStream::new(rx);
    (handle, rx)
}

pub async fn wrap_recv<T>(r: &mut BroadcastReceiver<T>) -> Option<T>
where
    T: Clone,
{
    loop {
        match r.recv().await {
            Ok(x) => return Some(x),
            Err(e) => match e {
                RecvError::Closed => return None,
                RecvError::Lagged(_) => {
                    debug_with_info!("lagged");
                    continue;
                }
            },
        };
    }
}
//
// #[macro_export]
// macro_rules! loop_broadcast_receiver {
//     ( ($rx: ident, $value: ident) $body: block) =>  {
//         while let Some($value) = $crate::wrap_recv(&mut $rx).await { $body }
//     }
// }

pub async fn estimate_latencies(which: TopicName, md: FoundMetadata) {
    let inline_url = md.events_data_inline_url.unwrap().clone();

    let (handle, mut rx) = get_events_stream_inline(&inline_url).await;

    // keep track of the latencies in a vector and compute the mean

    let mut latencies_ns = Vec::new();
    let mut index = 0;

    while let Some(lue) = wrap_recv(&mut rx).await {
        // convert a string to integer
        match lue {
            ListenURLEvents::InsertNotification(notification) => {
                let string = String::from_utf8(notification.raw_data.content.to_vec()).unwrap();

                // let nanos = serde_json::from_slice::<u128>(&notification.rd.content).unwrap();
                let nanos: u128 = string.parse().unwrap();
                let nanos_here = time_nanos();
                let diff = nanos_here - nanos;
                if index > 0 {
                    // ignore the first one
                    latencies_ns.push(diff);
                }
                if latencies_ns.len() > 10 {
                    latencies_ns.remove(0);
                }
                if !latencies_ns.is_empty() {
                    let latencies_sum_ns: u128 = latencies_ns.iter().sum();
                    let latencies_mean_ns = (latencies_sum_ns) / (latencies_ns.len() as u128);
                    let latencies_min_ns = *latencies_ns.iter().min().unwrap();
                    let latencies_max_ns = *latencies_ns.iter().max().unwrap();
                    info_with_info!(
                        "{:?} latency: {:.3}ms   (last {} : mean: {:.3}ms  min: {:.3}ms  max {:.3}ms )",
                        which,
                        ms_from_ns(diff),
                        latencies_ns.len(),
                        ms_from_ns(latencies_mean_ns),
                        ms_from_ns(latencies_min_ns),
                        ms_from_ns(latencies_max_ns)
                    );
                }
            }
            ListenURLEvents::WarningMsg(msg) => {
                warn_with_info!("{}", msg.to_string());
            }
            ListenURLEvents::ErrorMsg(msg) => {
                error_with_info!("{}", msg.to_string());
            }
            ListenURLEvents::FinishedMsg(msg) => {
                info_with_info!("finished: {}", msg.comment);
            }
            ListenURLEvents::SilenceMsg(_) => {}
        }

        index += 1;
    }
    match handle.await {
        Ok(_) => {}
        Err(e) => {
            error_with_info!("error in handle: {:?}", e);
        }
    };
}

pub fn ms_from_ns(ns: u128) -> f64 {
    (ns as f64) / 1_000_000.0
}

pub async fn send_to_server<T>(tx: &mut futures::channel::mpsc::UnboundedSender<TM>, m: &T) -> DTPSR<()>
where
    T: Serialize,
{
    let ascbor = serde_cbor::to_vec(m)?;
    let msg = TM::binary(ascbor);
    match tx.send(msg).await {
        Ok(_) => return Ok(()),
        Err(e) => {
            return Err(DTPSError::Other(e.to_string()));
        }
    }
}

// returns None if closed
pub async fn receive_from_server<T>(rx: &mut Receiver<TM>) -> DTPSR<Option<T>>
where
    T: DeserializeOwned,
{
    let msg = match rx.recv().await {
        Ok(msg) => msg,
        Err(e) => {
            return match e {
                RecvError::Closed => Ok(None),
                RecvError::Lagged(_) => {
                    let s = "lagged".to_string();
                    DTPSError::other(s)
                }
            };
        }
    };
    if !msg.is_binary() {
        // pragma: no cover
        let s = format!("unexpected message, expected binary {msg:#?}");
        return DTPSError::other(s);
    }
    let data = msg.clone().into_data();

    let msg_from_server: T = match serde_cbor::from_slice::<T>(&data) {
        Ok(dr_) => {
            // debug_with_info!("dr: {:#?}", dr_);
            dr_
        }
        Err(e) => {
            // pragma: no cover
            let rawvalue = serde_cbor::from_slice::<serde_cbor::Value>(&data);
            let s = format!(
                " cannot parse cbor as MsgServerToClient: {:#?}\n{:#?}\n{:#?}",
                e,
                msg.type_id(),
                rawvalue,
            );
            error_with_info!("{}", s);
            return DTPSError::other(s);
        }
    };
    Ok(Some(msg_from_server))
}

pub async fn listen_events_websocket(
    con: TypeOfConnection,
    tx: tokio::sync::broadcast::Sender<ListenURLEvents>,
) -> DTPSR<()> {
    let wsc = open_websocket_connection(&con).await?;
    let prefix = format!("listen_events_websocket({con})");
    // debug_with_info!("starting to listen to events for {} on {:?}", con, read);
    let mut index: u32 = 0;
    let mut rx = wsc.get_incoming().await;

    let first = receive_from_server(&mut rx).await?;
    match first {
        Some(MsgServerToClient::ChannelInfo(..)) => {}
        _ => {
            let s = format!("Expected ChannelInfo, got {first:?}");
            error_with_info!("{}", s);
            return DTPSError::other(s);
        }
    }

    loop {
        let msg_from_server = receive_from_server(&mut rx).await?;
        let dr = match msg_from_server {
            None => {
                break;
            }
            Some(MsgServerToClient::DataReady(dr_)) => dr_,

            _ => {
                let s = format!("{prefix}: message #{index}: unexpected message: {msg_from_server:#?}");
                error_with_info!("{}", s);
                return DTPSError::other(s);
            }
        };
        index += 1;

        let content: Vec<u8> = if dr.chunks_arriving == 0 {
            if dr.availability.is_empty() {
                let s = format!("{prefix}: message #{index}: availability is empty. listening to {con}",);
                return DTPSError::other(s);
            }
            let avail = dr.availability.get(0).unwrap();
            let url = con.join(&avail.url)?;

            get_rawdata(&url).await?.content.to_vec()
        } else {
            let mut content: Vec<u8> = Vec::with_capacity(dr.content_length);
            for _ in 0..(dr.chunks_arriving) {
                let msg_from_server = receive_from_server(&mut rx).await?;

                let chunk = match msg_from_server {
                    None => break,

                    Some(x) => match x {
                        MsgServerToClient::Chunk(chunk) => chunk,

                        MsgServerToClient::DataReady(..)
                        | MsgServerToClient::ChannelInfo(..)
                        | MsgServerToClient::WarningMsg(..)
                        | MsgServerToClient::SilenceMsg(..)
                        | MsgServerToClient::ErrorMsg(..)
                        | MsgServerToClient::FinishedMsg(..) => {
                            let s = format!("{prefix}: unexpected message : {x:#?}");
                            return DTPSError::other(s);
                        }
                    },
                };

                let data = chunk.data;
                content.extend_from_slice(&data);

                index += 1;
            }
            content
        };

        if content.len() != dr.content_length {
            // pragma: no cover
            let s = format!(
                "{prefix}: unexpected content length: {} != {}",
                content.len(),
                dr.content_length
            );
            return DTPSError::other(s);
        }
        let content_type = dr.content_type.clone();
        let rd = RawData {
            content: Bytes::from(content),
            content_type,
        };
        let notification = ListenURLEvents::InsertNotification(InsertNotification {
            data_saved: dr.as_data_saved(),
            raw_data: rd,
        });

        if tx.send(notification).is_err() {
            break;
        }
    }
    Ok(())
}

pub async fn get_rawdata_status(con: &TypeOfConnection) -> DTPSR<(http::StatusCode, RawData)> {
    let method = hyper::Method::GET;

    let resp = make_request(con, method, b"", None, None).await?;
    // TODO: send more headers
    Ok((resp.status(), interpret_resp(con, resp).await?))
}

pub async fn get_rawdata(con: &TypeOfConnection) -> DTPSR<RawData> {
    let method = hyper::Method::GET;
    let resp = make_request(con, method, b"", None, None).await?;
    // TODO: send more headers
    interpret_resp(con, resp).await
}

pub async fn get_rawdata_accept(con: &TypeOfConnection, accept: Option<&str>) -> DTPSR<RawData> {
    let method = hyper::Method::GET;
    let resp = make_request(con, method, b"", None, accept).await?;
    // TODO: send more headers
    interpret_resp(con, resp).await
}

pub async fn interpret_resp(con: &TypeOfConnection, resp: Response) -> DTPSR<RawData> {
    if resp.status().is_success() {
        let content_type = get_content_type(&resp);
        // Get the response body bytes.
        let content = hyper::body::to_bytes(resp.into_body()).await?;
        Ok(RawData { content, content_type })
    } else {
        let url = con.to_string();
        let code = resp.status().as_u16();
        let as_s = resp.status().as_str().to_string();
        let content = hyper::body::to_bytes(resp.into_body()).await?;
        let string = String::from_utf8(content.to_vec()).unwrap();
        Err(DTPSError::FailedRequest(url, code, as_s, string))
    }
}

#[derive(Debug)]
pub enum TypeOfResource {
    Other,
    DTPSTopic,
    DTPSIndex { node_id: String },
}

pub async fn sniff_type_resource(con: &TypeOfConnection) -> DTPSR<TypeOfResource> {
    let md = get_metadata(con).await?;
    let content_type = &md.content_type;
    debug_with_info!("content_type: {:#?}", content_type);

    if content_type.contains(CONTENT_TYPE_DTPS_INDEX) {
        match &md.answering {
            None => {
                debug_with_info!("This looks like an index but could not get answering:\n{md:#?}");
                Ok(TypeOfResource::Other)
            }
            Some(node_id) => Ok(TypeOfResource::DTPSIndex {
                node_id: node_id.clone(),
            }),
        }
    } else {
        Ok(TypeOfResource::Other)
    }
}

pub async fn post_json<T>(con: &TypeOfConnection, value: &T) -> DTPSR<RawData>
where
    T: Serialize,
{
    let rd = RawData::encode_as_json(value)?;
    post_data(con, &rd).await
}

pub async fn post_cbor<T>(con: &TypeOfConnection, value: &T) -> DTPSR<RawData>
where
    T: Serialize,
{
    let rd = RawData::encode_as_cbor(value)?;
    post_data(con, &rd).await
}

pub async fn post_data(con: &TypeOfConnection, rd: &RawData) -> DTPSR<RawData> {
    let resp = context!(
        make_request(con, hyper::Method::POST, &rd.content, Some(&rd.content_type), None).await,
        "Cannot make request to {}",
        con.to_string(),
    )?;

    let (is_success, as_string) = (resp.status().is_success(), resp.status().to_string());
    let content_type = get_content_type(&resp);
    let body_bytes = context!(hyper::body::to_bytes(resp.into_body()).await, "Cannot get body bytes")?;
    if !is_success {
        // pragma: no cover
        let body_text = String::from_utf8_lossy(&body_bytes);
        return not_available!("Request is not a success: for {con}\n{as_string:?}\n{body_text}");
    }
    // let x0: DataSaved = context!(serde_cbor::from_slice(&body_bytes), "Cannot interpret as CBOR: {body_bytes:?}")?;
    let x0: RawData = RawData {
        content: body_bytes,
        content_type,
    };

    Ok(x0)
}

pub async fn get_history(con: &TypeOfConnection) -> DTPSR<History> {
    let rd = get_rawdata(con).await?;
    let content_type = rd.content_type;
    if content_type != CONTENT_TYPE_TOPIC_HISTORY_CBOR {
        // pragma: no cover
        return not_available!("Expected content type {CONTENT_TYPE_TOPIC_HISTORY_CBOR}, obtained {content_type} ");
    }
    let x = serde_cbor::from_slice::<History>(&rd.content);
    match x {
        Ok(x) => Ok(x),
        Err(e) => {
            // pragma: no cover
            let value: serde_cbor::Value = serde_cbor::from_slice(&rd.content)?;
            let s = format!("cannot parse as CBOR:\n{:#?}", value);
            error_with_info!("{}", s);
            error_with_info!("content: {:#?}", e);
            DTPSError::other(s)
        }
    }
}

pub async fn get_index(con: &TypeOfConnection) -> DTPSR<TopicsIndexInternal> {
    let rd = get_rawdata(con).await?;

    // let h = resp.headers();
    // debug_with_info!("headers: {h:?}");

    // debug_with_info!("{con:?}: content type {content_type}");
    let content_type = rd.content_type;
    if content_type != CONTENT_TYPE_DTPS_INDEX_CBOR {
        // pragma: no cover
        return not_available!("Expected content type {CONTENT_TYPE_DTPS_INDEX_CBOR}, obtained {content_type} ");
    }
    let x = serde_cbor::from_slice::<TopicsIndexWire>(&rd.content);
    if x.is_err() {
        // pragma: no cover
        let value: serde_cbor::Value = serde_cbor::from_slice(&rd.content)?;
        let s = format!("cannot parse as CBOR:\n{:#?}", value);
        error_with_info!("{}", s);
        error_with_info!("content: {:#?}", x);
        return DTPSError::other(s);
    }
    let x0 = x.unwrap();

    let ti = TopicsIndexInternal::from_wire(&x0, con);

    Ok(ti)
}

#[derive(Debug, Clone)]
pub enum UrlResult {
    /// Cannot reach this URL
    Inaccessible(String),
    /// We cannot find the expected node answering
    WrongNodeAnswering,
    /// It is accessbile with this benchmark
    Accessible(LinkBenchmark),
}

pub async fn get_stats(con: &TypeOfConnection, expect_node_id: Option<String>) -> UrlResult {
    let md = get_metadata(con).await;
    let complexity = match con {
        TypeOfConnection::TCP(_) => 2,
        TypeOfConnection::UNIX(_) => 1,
        TypeOfConnection::Relative(_, _) => {
            panic!("unexpected relative url here: {}", con);
        }
        TypeOfConnection::Same() => {
            panic!("not expected here {}", con);
        }
        TypeOfConnection::File(..) => 0,
    };
    let reliability_percent = match con {
        TypeOfConnection::TCP(_) => 70,
        TypeOfConnection::UNIX(_) => 90,
        TypeOfConnection::Relative(_, _) => {
            panic!("unexpected relative url here: {}", con);
        }
        TypeOfConnection::Same() => {
            panic!("not expected here {}", con);
        }
        TypeOfConnection::File(..) => 100,
    };
    match md {
        Err(err) => {
            let s = format!("cannot get metadata for {:?}: {}", con, err);

            UrlResult::Inaccessible(s)
        }
        Ok(md_) => {
            if incompatible(&expect_node_id, &md_.answering) {
                UrlResult::WrongNodeAnswering
            } else {
                let latency_ns = md_.latency_ns;
                let lb = LinkBenchmark {
                    complexity,
                    bandwidth: 100_000_000,
                    latency_ns,
                    reliability_percent,
                    hops: 1,
                };
                UrlResult::Accessible(lb)
            }
        }
    }
}

pub fn incompatible(expect_node_id: &Option<String>, answering: &Option<String>) -> bool {
    match (expect_node_id, answering) {
        (None, None) => false,
        (None, Some(_)) => false,
        (Some(_), None) => true,
        (Some(e), Some(a)) => e != a,
    }
}

pub async fn compute_best_alternative(
    alternatives: &HashSet<TypeOfConnection>,
    expect_node_id: Option<String>,
) -> DTPSR<TypeOfConnection> {
    let mut possible_urls: Vec<TypeOfConnection> = Vec::new();
    let mut possible_stats: Vec<LinkBenchmark> = Vec::new();
    let mut i = 0;
    let n = alternatives.len();
    for alternative in alternatives.iter() {
        i += 1;
        debug_with_info!("Trying {}/{}: {}", i, n, alternative);
        let result_future = get_stats(alternative, expect_node_id.clone());

        let result = match timeout(Duration::from_millis(2000), result_future).await {
            Ok(r) => r,
            Err(_) => {
                debug_with_info!("-> Timeout: {}", alternative);
                continue;
            }
        };

        match result {
            UrlResult::Inaccessible(why) => {
                debug_with_info!("-> Inaccessible: {}", why);
            }
            UrlResult::WrongNodeAnswering => {
                debug_with_info!("-> Wrong node answering");
            }
            UrlResult::Accessible(link_benchmark) => {
                debug_with_info!("-> Accessible: {:?}", link_benchmark);
                possible_urls.push(alternative.clone());
                possible_stats.push(link_benchmark);
            }
        }
    }
    // if no alternative is accessible, return None
    if possible_urls.is_empty() {
        return Err(DTPSError::ResourceNotReachable(
            "no alternative are accessible".to_string(),
        ));
        // return Err("no alternative are accessible".into());
    }
    // get the index of minimum possible_stats
    let min_index = possible_stats
        .iter()
        .enumerate()
        .min_by_key(|&(_, item)| item)
        .unwrap()
        .0;
    let best_url = possible_urls[min_index].clone();
    debug_with_info!(
        "Best is {}: {} with {:?}",
        min_index,
        best_url,
        possible_stats[min_index]
    );
    Ok(best_url)
}

pub async fn check_unix_socket(file_path: &str) -> DTPSR<()> {
    let path = Path::new(file_path);
    match tokio::fs::metadata(path).await {
        Ok(md) => {
            let is_socket = md.file_type().is_socket();
            if !is_socket {
                not_reachable!("File {file_path} exists but it is not a socket.")
            } else {
                Ok(())
            }
        }
        Err(e) => {
            // check parent directory
            match path.parent() {
                None => {
                    not_available!("Cannot get parent directory of: {file_path:?}")
                }
                Some(parent) => {
                    if !parent.exists() {
                        not_available!("Socket not available and parent directory does not exist: {file_path:?}")
                    } else {
                        not_available!("Parent dir exists but socket does not exist: {file_path:?}: \n {e}")
                    }
                }
            }
        }
    }
}

pub async fn make_request(
    conbase: &TypeOfConnection,
    method: hyper::Method,
    body: &[u8],
    content_type: Option<&str>,
    accept: Option<&str>,
) -> DTPSR<Response> {
    let use_url = match conbase {
        TypeOfConnection::TCP(url) => url.clone().to_string(),
        TypeOfConnection::UNIX(uc) => {
            context!(check_unix_socket(&uc.socket_name).await, "cannot use unix socket {uc}",)?;

            let h = hex::encode(&uc.socket_name);
            let p0 = format!("unix://{}{}", h, uc.path);
            match uc.query {
                None => p0,
                Some(_) => {
                    let p1 = format!("{}?{}", p0, uc.query.as_ref().unwrap());
                    p1
                }
            }
        }

        TypeOfConnection::Relative(_, _) => {
            return internal_assertion!("cannot handle a relative url get_metadata: {conbase}");
        }
        TypeOfConnection::Same() => {
            return internal_assertion!("!!! not expected to reach here: {conbase}");
        }
        TypeOfConnection::File(..) => {
            return not_implemented!("read from file to implement: {conbase}");
        }
    };

    let mut req0 = context!(
        hyper::Request::builder()
            .method(&method)
            .uri(use_url.as_str())
            // .header("user-agent", "the-awesome-agent/007")
            .body(hyper::Body::from(body.to_vec())),
        "cannot build request for {method} {use_url}",
    )?;
    if let Some(x) = accept {
        let h = req0.headers_mut();
        put_header_accept(h, x);
    }

    if let Some(x) = content_type {
        let h = req0.headers_mut();
        put_header_content_type(h, x);
    }

    let resp = context!(
        match conbase {
            TypeOfConnection::TCP(url) => {
                if url.scheme() == "https" {
                    let https = HttpsConnector::new();
                    let client = Client::builder().build::<_, hyper::Body>(https);
                    client.request(req0).await
                } else {
                    let client = hyper::Client::new();
                    client.request(req0).await
                }
            }
            TypeOfConnection::UNIX(_) => {
                let client = Client::unix();
                client.request(req0).await
            }

            TypeOfConnection::Relative(_, _) => {
                return not_available("cannot handle a relative url get_metadata");
            }
            TypeOfConnection::Same() => {
                return not_available("cannot handle a Same url to get_metadata");
            }
            TypeOfConnection::File(..) => {
                return internal_assertion!("not supposed to reach here: {conbase}");
            }
        },
        "make_request(): cannot make {} request for connection {} \
        (use_url={})",
        method,
        conbase,
        use_url.as_str()
    )?;

    Ok(resp)
}

pub async fn get_metadata(conbase: &TypeOfConnection) -> DTPSR<FoundMetadata> {
    // current time in nano seconds
    let start = time_nanos();

    let resp = make_request(conbase, hyper::Method::HEAD, b"", None, None).await?;
    let status = resp.status();
    if !status.is_success() {
        let desc = status.as_str();
        let msg = format!("cannot get metadata for {conbase}: {desc}");
        return Err(DTPSError::FailedRequest(
            msg,
            status.as_u16(),
            conbase.to_string(),
            desc.to_string(),
        ));
    }
    let end = time_nanos();

    let latency_ns = end - start;

    // get the headers from the response
    let headers = resp.headers();

    // get all the HEADER_CONTENT_LOCATION in the response
    let alternatives0 = headers.get_all(HEADER_CONTENT_LOCATION);
    // debug_with_info!("alternatives0: {:#?}", alternatives0);
    // convert into a vector of strings
    let alternative_urls: Vec<String> = alternatives0.iter().map(string_from_header_value).collect();

    // convert into a vector of URLs
    let mut alternative_urls: Vec<TypeOfConnection> =
        alternative_urls.iter().map(|x| parse_url_ext(x).unwrap()).collect();
    alternative_urls.push(conbase.clone());

    let answering = headers.get(HEADER_NODE_ID).map(string_from_header_value);

    let mut links = hashmap! {};
    for v in headers.get_all("link") {
        let link = LinkHeader::from_header_value(&string_from_header_value(v));

        let rel = match link.get("rel") {
            None => {
                continue;
            }
            Some(r) => r,
        };
        links.insert(rel, link);
    }
    let alternative_urls: HashSet<_> = alternative_urls.iter().cloned().collect();

    let get_if_exists = |w: &str| -> Option<TypeOfConnection> {
        if let Some(l) = links.get(w) {
            let url = l.url.clone();
            let url = join_ext(conbase, &url).ok()?;
            Some(url)
        } else {
            None
        }
    };
    let content_type = get_content_type(&resp);
    let events_url = get_if_exists(REL_EVENTS_NODATA);
    let events_data_inline_url = get_if_exists(REL_EVENTS_DATA);
    let meta_url = get_if_exists(REL_META);
    let history_url = get_if_exists(REL_HISTORY);
    let connections_url = get_if_exists(REL_CONNECTIONS);
    let proxied_url = get_if_exists(REL_PROXIED);
    let stream_push_url = get_if_exists(REL_STREAM_PUSH);
    let base_url = conbase.clone();
    let md = FoundMetadata {
        base_url,
        alternative_urls,
        events_url,
        answering,
        events_data_inline_url,
        latency_ns,
        meta_url,
        history_url,
        content_type,
        proxied_url,
        connections_url,
        stream_push_url,
    };

    Ok(md)
}

pub async fn create_topic(
    conbase: &TypeOfConnection,
    topic_name: &TopicName,
    tr: &TopicRefAdd,
) -> DTPSR<TypeOfConnection> {
    let mut path: String = String::new();
    for t in topic_name.as_components() {
        path.push('/');
        path.push_str(t);
    }

    let value = serde_json::to_value(tr)?;

    let add_operation = AddOperation { path, value };
    let operation1 = PatchOperation::Add(add_operation);
    let patch = Patch(vec![operation1]);
    patch_data(conbase, &patch).await?;
    let con = conbase.join(topic_name.as_relative_url())?;
    Ok(con)
}

pub async fn delete_topic(conbase: &TypeOfConnection, topic_name: &TopicName) -> DTPSR<()> {
    let mut path: String = String::new();
    for t in topic_name.as_components() {
        path.push('/');
        path.push_str(t);
    }

    let remove_operation = RemoveOperation { path };
    let operation1 = PatchOperation::Remove(remove_operation);
    let patch = Patch(vec![operation1]);

    patch_data(conbase, &patch).await
}

pub fn escape_json_patch(s: &str) -> String {
    s.replace('~', "~0").replace('/', "~1")
}

pub fn unescape_json_patch(s: &str) -> String {
    s.replace("~1", "/").replace("~0", "~")
}

pub async fn add_proxy(
    conbase: &TypeOfConnection,
    mountpoint: &TopicName,
    node_id: Option<String>,
    urls: &[TypeOfConnection],
    mask_origin: bool,
) -> DTPSR<()> {
    let md = get_metadata(conbase).await?;
    let url = match md.proxied_url {
        None => {
            return not_available!(
                "cannot add proxy: no proxied url in metadata for {}",
                conbase.to_string()
            );
        }
        Some(url) => url,
    };

    let patch = create_add_proxy_patch(mountpoint, node_id, urls, mask_origin)?;

    patch_data(&url, &patch).await
}

fn create_add_proxy_patch(
    mountpoint: &TopicName,
    node_id: Option<String>,
    urls: &[TypeOfConnection],
    mask_origin: bool,
) -> Result<Patch, DTPSError> {
    let urls = urls.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    let pj = ProxyJob {
        node_id,
        urls,
        mask_origin,
    };

    let mut path: String = String::new();
    path.push('/');
    path.push_str(escape_json_patch(mountpoint.as_dash_sep()).as_str());
    let value = serde_json::to_value(pj)?;

    let add_operation = AddOperation { path, value };
    let operation1 = PatchOperation::Add(add_operation);
    let patch = json_patch::Patch(vec![operation1]);
    Ok(patch)
}

pub async fn remove_proxy(conbase: &TypeOfConnection, mountpoint: &TopicName) -> DTPSR<()> {
    let md = get_metadata(conbase).await?;
    let url = match md.proxied_url {
        None => {
            return not_available!(
                "cannot remove proxy: no proxied url in metadata for {}",
                conbase.to_string()
            );
        }
        Some(url) => url,
    };

    let patch = create_remove_proxy_patch(mountpoint);

    patch_data(&url, &patch).await
}

fn create_remove_proxy_patch(mountpoint: &TopicName) -> Patch {
    let mut path: String = String::new();
    path.push('/');
    path.push_str(escape_json_patch(mountpoint.as_dash_sep()).as_str());

    let remove_operation = RemoveOperation { path };
    let operation1 = PatchOperation::Remove(remove_operation);
    json_patch::Patch(vec![operation1])
}

pub async fn add_tpt_connection(
    conbase: &TypeOfConnection,
    connection_name: &CompositeName,
    connection_job: &ConnectionJob,
) -> DTPSR<()> {
    let md = get_metadata(conbase).await?;
    let url = match md.connections_url {
        None => {
            return not_available!(
                "cannot remove connection: no connections_url in metadata for {}",
                conbase.to_string()
            );
        }
        Some(url) => url,
    };

    let patch = create_add_tpt_connection_patch(connection_name, connection_job)?;

    patch_data(&url, &patch).await
}

fn create_add_tpt_connection_patch(
    connection_name: &CompositeName,
    connection_job: &ConnectionJob,
) -> Result<Patch, DTPSError> {
    let mut path: String = String::new();
    path.push('/');
    path.push_str(escape_json_patch(connection_name.as_dash_sep()).as_str());

    let wire = connection_job.to_wire();
    let value = serde_json::to_value(wire)?;

    let add_operation = AddOperation { path, value };
    let operation1 = PatchOperation::Add(add_operation);
    let patch = json_patch::Patch(vec![operation1]);
    Ok(patch)
}

pub async fn remove_tpt_connection(conbase: &TypeOfConnection, connection_name: &CompositeName) -> DTPSR<()> {
    let md = get_metadata(conbase).await?;
    let url = match md.connections_url {
        None => {
            return not_available!(
                "cannot remove connection: no connections_url in metadata for {}",
                conbase.to_string()
            );
        }
        Some(url) => url,
    };

    let patch = create_remove_tpt_connection_patch(connection_name);

    patch_data(&url, &patch).await
}

fn create_remove_tpt_connection_patch(connection_name: &CompositeName) -> Patch {
    let mut path: String = String::new();
    path.push('/');
    path.push_str(escape_json_patch(connection_name.as_dash_sep()).as_str());

    let remove_operation = RemoveOperation { path };
    let operation1 = PatchOperation::Remove(remove_operation);
    json_patch::Patch(vec![operation1])
}

pub async fn patch_data(conbase: &TypeOfConnection, patch: &JsonPatch) -> DTPSR<()> {
    let json_data = serde_json::to_vec(patch)?;
    // debug_with_info!("patch_data out: {:#?}", String::from_utf8(json_data.clone()));
    let resp = make_request(
        conbase,
        hyper::Method::PATCH,
        &json_data,
        Some(CONTENT_TYPE_PATCH_JSON),
        None,
    )
    .await?;
    let status = resp.status();
    if !status.is_success() {
        let desc = status.as_str();
        let msg = format!("cannot patch:\nurl: {conbase}\n---\n{desc}");
        Err(DTPSError::FailedRequest(
            msg,
            status.as_u16(),
            conbase.to_string(),
            desc.to_string(),
        ))
    } else {
        Ok(())
    }
}

fn string_from_header_value(header_value: &hyper::header::HeaderValue) -> String {
    header_value.to_str().unwrap().to_string()
}
