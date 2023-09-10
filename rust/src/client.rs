use std::any::Any;
use std::collections::HashSet;
use std::fmt::Debug;
use std::os::unix::fs::FileTypeExt;
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use futures::StreamExt;
use hex;
use hyper;
use hyper::Client;
use hyper_tls::HttpsConnector;
use hyperlocal::UnixClientExt;
use json_patch::{AddOperation, Patch as JsonPatch, PatchOperation, RemoveOperation};
use log::{debug, error, info};
use maplit::hashmap;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tungstenite::Message as TM;
use warp::reply::Response;

use crate::open_websocket_connection;
use crate::time_nanos;
use crate::websocket_signals::MsgServerToClient;
use crate::TypeOfConnection::Same;
use crate::TypeOfConnection::{Relative, TCP, UNIX};
use crate::UrlResult::{Accessible, Inaccessible, WrongNodeAnswering};
use crate::{
    context, error_with_info, internal_assertion, not_available, not_implemented, not_reachable,
    put_header_accept, put_header_content_type, DTPSError, DataSaved, History, ProxyJob, RawData,
    TopicName, TopicRefAdd, CONTENT_TYPE_DTPS_INDEX, CONTENT_TYPE_DTPS_INDEX_CBOR,
    CONTENT_TYPE_PATCH_JSON, CONTENT_TYPE_TOPIC_HISTORY_CBOR, DTPSR, REL_HISTORY, TOPIC_PROXIED,
};
use crate::{join_ext, parse_url_ext};
use crate::{DataReady, FoundMetadata, TopicsIndexInternal, TopicsIndexWire, TypeOfConnection};
use crate::{LinkBenchmark, LinkHeader, REL_EVENTS_DATA, REL_EVENTS_NODATA, REL_META};
use crate::{HEADER_CONTENT_LOCATION, HEADER_NODE_ID};

/// Note: need to have use futures::{StreamExt} in scope to use this
pub async fn get_events_stream_inline(
    url: &TypeOfConnection,
) -> (JoinHandle<DTPSR<()>>, UnboundedReceiverStream<Notification>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let inline_url = url.clone();
    let handle = tokio::spawn(listen_events_websocket(inline_url, tx));
    let stream = UnboundedReceiverStream::new(rx);
    (handle, stream)
}

pub async fn estimate_latencies(which: TopicName, md: FoundMetadata) {
    // let (tx, rx) = mpsc::unbounded_channel();
    let inline_url = md.events_data_inline_url.unwrap().clone();

    let (handle, mut stream) = get_events_stream_inline(&inline_url).await;

    // keep track of the latencies in a vector and compute the mean

    let mut latencies_ns = Vec::new();
    let mut index = 0;
    while let Some(notification) = stream.next().await {
        // convert a string to integer

        let string = String::from_utf8(notification.rd.content.to_vec()).unwrap();

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
        if latencies_ns.len() > 0 {
            let latencies_sum_ns: u128 = latencies_ns.iter().sum();
            let latencies_mean_ns = (latencies_sum_ns) / (latencies_ns.len() as u128);
            let latencies_min_ns = *latencies_ns.iter().min().unwrap();
            let latencies_max_ns = *latencies_ns.iter().max().unwrap();
            info!(
                "{:?} latency: {:.3}ms   (last {} : mean: {:.3}ms  min: {:.3}ms  max {:.3}ms )",
                which,
                ms_from_ns(diff),
                latencies_ns.len(),
                ms_from_ns(latencies_mean_ns),
                ms_from_ns(latencies_min_ns),
                ms_from_ns(latencies_max_ns)
            );
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

#[derive(Debug, PartialEq, Clone)]
pub struct Notification {
    pub dr: DataReady,
    pub rd: RawData,
}

pub async fn receive_from_server(rx: &mut Receiver<TM>) -> DTPSR<Option<MsgServerToClient>> {
    let msg = match rx.recv().await {
        Ok(msg) => msg,
        Err(e) => {
            return match e {
                RecvError::Closed => Ok(None),
                RecvError::Lagged(_) => {
                    let s = format!("lagged");
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

    let msg_from_server: MsgServerToClient;
    match serde_cbor::from_slice::<MsgServerToClient>(&data) {
        Ok(dr_) => {
            // debug!("dr: {:#?}", dr_);
            msg_from_server = dr_;
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
            error!("{}", s);
            return DTPSError::other(s);
        }
    };
    Ok(Some(msg_from_server))
}

pub async fn listen_events_websocket(
    con: TypeOfConnection,
    tx: UnboundedSender<Notification>,
) -> DTPSR<()> {
    let wsc = open_websocket_connection(&con).await?;
    let prefix = format!("listen_events_websocket({con})");
    // debug!("starting to listen to events for {} on {:?}", con, read);
    let mut index: u32 = 0;
    let mut rx = wsc.get_incoming().await;

    let first = receive_from_server(&mut rx).await?;
    match first {
        Some(MsgServerToClient::ChannelInfo(..)) => {}
        _ => {
            // pragma: no cover
            let s = format!("Expected ChannelInfo, got {first:?}");
            log::error!("{}", s);
            return DTPSError::other(s);
        }
    }

    loop {
        let msg_from_server = receive_from_server(&mut rx).await?;
        let dr = match msg_from_server {
            None => {
                // debug!("end of stream");
                break;
            }
            Some(MsgServerToClient::DataReady(dr_)) => dr_,

            _ => {
                // pragma: no cover
                let s =
                    format!("{prefix}: message #{index}: unexpected message: {msg_from_server:#?}");
                log::error!("{}", s);
                return DTPSError::other(s);
            }
        };
        index += 1;

        let content: Vec<u8> = if dr.chunks_arriving == 0 {
            // error_with_info!(
            //     "{prefix}: message #{}: no chunks arriving. listening to {}",
            //     index,
            //     con
            // );
            if dr.availability.is_empty() {
                let s = format!(
                    "{prefix}: message #{}: availability is empty. listening to {}",
                    index, con
                );
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
                    Some(MsgServerToClient::DataReady(..) | MsgServerToClient::ChannelInfo(..))
                    | None => {
                        // pragma: no cover
                        let s = format!("{prefix}: unexpected message : {msg_from_server:#?}");
                        return DTPSError::other(s);
                    }
                    Some(MsgServerToClient::Chunk(chunk)) => chunk,
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
        let notification = Notification { dr, rd };

        match tx.send(notification) {
            Ok(_) => {}
            Err(_) => {
                // this is not an error, it just means that the receiver has been dropped
                break;
            }
        }
    }
    Ok(())
}

pub async fn get_rawdata(con: &TypeOfConnection) -> DTPSR<RawData> {
    let resp = make_request(con, hyper::Method::GET, b"", None, None).await?;
    // TODO: send more headers
    interpret_resp(con, resp).await
}

pub async fn interpret_resp(con: &TypeOfConnection, resp: Response) -> DTPSR<RawData> {
    if resp.status().is_success() {
        let content_type = get_content_type(&resp);
        // Get the response body bytes.
        let content = hyper::body::to_bytes(resp.into_body()).await?;
        Ok(RawData {
            content,
            content_type,
        })
    } else {
        let url = con.to_string();
        let code = resp.status().as_u16();
        let as_s = resp.status().as_str().to_string();
        let content = hyper::body::to_bytes(resp.into_body()).await?;
        let string = String::from_utf8(content.to_vec()).unwrap();
        return Err(DTPSError::FailedRequest(url, code, as_s, string));
    }
}

#[derive(Debug)]
pub enum TypeOfResource {
    Other,
    DTPSTopic,
    DTPSIndex,
}

pub fn get_content_type<T>(resp: &http::Response<T>) -> String {
    let content_type = resp
        .headers()
        .get("content-type")
        .map(|x| x.to_str().unwrap().to_string())
        .unwrap_or("application/octet-stream".to_string());
    content_type
}

pub async fn sniff_type_resource(con: &TypeOfConnection) -> DTPSR<TypeOfResource> {
    let rd = get_rawdata(con).await?;
    // let resp = context!(
    //     make_request(con, hyper::Method::GET, b"", None, None).await,
    //     "Cannot make request to {}",
    //     con.to_string(),
    // )?;
    let content_type = rd.content_type;
    debug!("content_type: {:#?}", content_type);

    if content_type.contains(CONTENT_TYPE_DTPS_INDEX) {
        Ok(TypeOfResource::DTPSIndex)
    } else {
        Ok(TypeOfResource::Other)
    }
}

pub async fn post_data(con: &TypeOfConnection, rd: &RawData) -> DTPSR<DataSaved> {
    let resp = context!(
        make_request(
            con,
            hyper::Method::POST,
            &rd.content,
            Some(&rd.content_type),
            None
        )
        .await,
        "Cannot make request to {}",
        con.to_string(),
    )?;

    let (is_success, as_string) = (resp.status().is_success(), resp.status().to_string());
    let body_bytes = context!(
        hyper::body::to_bytes(resp.into_body()).await,
        "Cannot get body bytes"
    )?;
    if !is_success {
        // pragma: no cover
        let body_text = String::from_utf8_lossy(&body_bytes);
        return not_available!("Request is not a success: for {con}\n{as_string:?}\n{body_text}");
    }
    let x0: DataSaved = context!(
        serde_cbor::from_slice(&body_bytes),
        "Cannot interpret as CBOR"
    )?;

    Ok(x0)
}

pub async fn get_history(con: &TypeOfConnection) -> DTPSR<History> {
    let rd = get_rawdata(con).await?;
    let content_type = rd.content_type;
    if content_type != CONTENT_TYPE_TOPIC_HISTORY_CBOR {
        // pragma: no cover
        return not_available!(
            "Expected content type {CONTENT_TYPE_TOPIC_HISTORY_CBOR}, obtained {content_type} "
        );
    }
    let x = serde_cbor::from_slice::<History>(&rd.content);
    if x.is_err() {
        // pragma: no cover
        let value: serde_cbor::Value = serde_cbor::from_slice(&rd.content)?;
        let s = format!("cannot parse as CBOR:\n{:#?}", value);
        log::error!("{}", s);
        log::error!("content: {:#?}", x);
        return DTPSError::other(s);
    }
    let x0 = x.unwrap();
    Ok(x0)
}

pub async fn get_index(con: &TypeOfConnection) -> DTPSR<TopicsIndexInternal> {
    let rd = get_rawdata(con).await?;

    // let h = resp.headers();
    // debug!("headers: {h:?}");

    // debug!("{con:?}: content type {content_type}");
    let content_type = rd.content_type;
    if content_type != CONTENT_TYPE_DTPS_INDEX_CBOR {
        // pragma: no cover
        return not_available!(
            "Expected content type {CONTENT_TYPE_DTPS_INDEX_CBOR}, obtained {content_type} "
        );
    }
    let x = serde_cbor::from_slice::<TopicsIndexWire>(&rd.content);
    if x.is_err() {
        // pragma: no cover
        let value: serde_cbor::Value = serde_cbor::from_slice(&rd.content)?;
        let s = format!("cannot parse as CBOR:\n{:#?}", value);
        log::error!("{}", s);
        log::error!("content: {:#?}", x);
        return DTPSError::other(s);
    }
    let x0 = x.unwrap();
    // let x0: TopicsIndexWire = context!(
    //     serde_cbor::from_slice(&rd.content),
    //     "Cannot interpret as CBOR"
    // )?;

    let ti = TopicsIndexInternal::from_wire(&x0, con);

    // debug!("get_index: {:#?}\n {:#?}", con, ti);
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

pub async fn get_stats(con: &TypeOfConnection, expect_node_id: &str) -> UrlResult {
    let md = get_metadata(con).await;
    let complexity = match con {
        TCP(_) => 2,
        UNIX(_) => 1,
        Relative(_, _) => {
            panic!("unexpected relative url here: {}", con);
        }
        Same() => {
            panic!("not expected here {}", con);
        }
        TypeOfConnection::File(..) => 0,
    };
    let reliability_percent = match con {
        TCP(_) => 70,
        UNIX(_) => 90,
        Relative(_, _) => {
            panic!("unexpected relative url here: {}", con);
        }
        Same() => {
            panic!("not expected here {}", con);
        }
        TypeOfConnection::File(..) => 100,
    };
    match md {
        Err(err) => {
            let s = format!("cannot get metadata for {:?}: {}", con, err);

            Inaccessible(s.to_string())
        }
        Ok(md_) => {
            return match md_.answering {
                None => WrongNodeAnswering,
                Some(answering) => {
                    if answering != expect_node_id {
                        WrongNodeAnswering
                    } else {
                        let latency_ns = md_.latency_ns;
                        let lb = LinkBenchmark {
                            complexity,
                            bandwidth: 100_000_000,
                            latency_ns,
                            reliability_percent,
                            hops: 1,
                        };
                        Accessible(lb)
                    }
                }
            };
        }
    }
}

pub async fn compute_best_alternative(
    alternatives: &HashSet<TypeOfConnection>,
    expect_node_id: &str,
) -> DTPSR<TypeOfConnection> {
    let mut possible_urls: Vec<TypeOfConnection> = Vec::new();
    let mut possible_stats: Vec<LinkBenchmark> = Vec::new();
    let mut i = 0;
    let n = alternatives.len();
    for alternative in alternatives.iter() {
        i += 1;
        debug!("Trying {}/{}: {}", i, n, alternative);
        let result_future = get_stats(alternative, expect_node_id);

        let result = match timeout(Duration::from_millis(2000), result_future).await {
            Ok(r) => r,
            Err(_) => {
                debug!("-> Timeout: {}", alternative);
                continue;
            }
        };

        match result {
            Inaccessible(why) => {
                debug!("-> Inaccessible: {}", why);
            }
            WrongNodeAnswering => {
                debug!("-> Wrong node answering");
            }
            Accessible(link_benchmark) => {
                debug!("-> Accessible: {:?}", link_benchmark);
                possible_urls.push(alternative.clone());
                possible_stats.push(link_benchmark.into());
            }
        }
    }
    // if no alternative is accessible, return None
    if possible_urls.len() == 0 {
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
    debug!(
        "Best is {}: {} with {:?}",
        min_index, best_url, possible_stats[min_index]
    );
    return Ok(best_url);
}

fn check_unix_socket(file_path: &str) -> DTPSR<()> {
    if let Ok(metadata) = std::fs::metadata(file_path) {
        // debug!("metadata for {}: {:?}", file_path, metadata);
        let is_socket = metadata.file_type().is_socket();
        if is_socket {
            Ok(())
        } else {
            not_reachable!("File {file_path} exists but it is not a socket.")
        }
    } else {
        Err(DTPSError::NotAvailable(format!(
            "Socket {file_path} does not exist."
        )))
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
        TCP(url) => url.clone().to_string(),
        UNIX(uc) => {
            context!(
                check_unix_socket(&uc.socket_name),
                "cannot use unix socket {uc}",
            )?;

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

        Relative(_, _) => {
            return internal_assertion!("cannot handle a relative url get_metadata: {conbase}");
        }
        Same() => {
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
    // debug!("alternatives0: {:#?}", alternatives0);
    // convert into a vector of strings
    let alternative_urls: Vec<String> =
        alternatives0.iter().map(string_from_header_value).collect();

    // convert into a vector of URLs
    let mut alternative_urls: Vec<TypeOfConnection> = alternative_urls
        .iter()
        .map(|x| parse_url_ext(x).unwrap())
        .collect();
    alternative_urls.push(conbase.clone());

    let answering = headers.get(HEADER_NODE_ID).map(string_from_header_value);
    //
    // if answering == None {
    //     debug!("Cannot find header {HEADER_NODE_ID} in response {headers:?}")
    // }
    let mut links = hashmap! {};
    for v in headers.get_all("link") {
        let link = LinkHeader::from_header_value(&string_from_header_value(v));

        // debug!("Link = {link:?}");

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
            let url = join_ext(&conbase, &url).ok()?;
            Some(url)
        } else {
            None
        }
    };

    let events_url = get_if_exists(REL_EVENTS_NODATA);
    let events_data_inline_url = get_if_exists(REL_EVENTS_DATA);
    let meta_url = get_if_exists(REL_META);
    let history_url = get_if_exists(REL_HISTORY);

    let md = FoundMetadata {
        alternative_urls,
        events_url,
        answering,
        events_data_inline_url,
        latency_ns,
        meta_url,
        history_url,
    };
    // info!("Logging metadata: {md:#?} headers {headers:#?}");

    Ok(md)
}

pub async fn create_topic(
    conbase: &TypeOfConnection,
    topic_name: &TopicName,
    tr: &TopicRefAdd,
) -> DTPSR<()> {
    let mut path: String = String::new();
    for t in topic_name.as_components() {
        path.push_str("/");
        path.push_str(t);
    }

    let value = serde_json::to_value(&tr)?;

    let add_operation = AddOperation { path, value };
    let operation1 = PatchOperation::Add(add_operation);
    let patch = json_patch::Patch(vec![operation1]);
    patch_data(conbase, &patch).await
}

pub async fn delete_topic(conbase: &TypeOfConnection, topic_name: &TopicName) -> DTPSR<()> {
    let mut path: String = String::new();
    for t in topic_name.as_components() {
        path.push_str("/");
        path.push_str(t);
    }

    let remove_operation = RemoveOperation { path };
    let operation1 = PatchOperation::Remove(remove_operation);
    let patch = json_patch::Patch(vec![operation1]);

    patch_data(conbase, &patch).await
}

pub fn escape_json_patch(s: &str) -> String {
    s.replace("~", "~0").replace("/", "~1")
}

pub fn unescape_json_patch(s: &str) -> String {
    s.replace("~1", "/").replace("~0", "~")
}

pub async fn add_proxy(
    conbase: &TypeOfConnection,
    mountpoint: &TopicName,
    url: &TypeOfConnection,
) -> DTPSR<()> {
    let pj = ProxyJob {
        // mounted_at: mountpoint.as_dash_sep().to_string(),
        url: url.to_string(),
    };

    let mut path: String = String::new();
    path.push_str("/");
    path.push_str(escape_json_patch(mountpoint.as_dash_sep()).as_str());
    debug!("patch path: {}", path);
    let value = serde_json::to_value(&pj)?;

    let add_operation = AddOperation { path, value };
    let operation1 = PatchOperation::Add(add_operation);
    let patch = json_patch::Patch(vec![operation1]);
    let tn = TopicName::from_dash_sep(TOPIC_PROXIED)?;

    let proxied_topic = conbase.join(tn.as_relative_url())?;
    patch_data(&proxied_topic, &patch).await
}

pub async fn remove_proxy(conbase: &TypeOfConnection, mountpoint: &TopicName) -> DTPSR<()> {
    let mut path: String = String::new();
    path.push_str("/");
    path.push_str(escape_json_patch(mountpoint.as_dash_sep()).as_str());

    let remove_operation = RemoveOperation { path };
    let operation1 = PatchOperation::Remove(remove_operation);
    let patch = json_patch::Patch(vec![operation1]);
    let tn = TopicName::from_dash_sep(TOPIC_PROXIED)?;

    let proxied_topic = conbase.join(tn.as_relative_url())?;
    patch_data(&proxied_topic, &patch).await
}

pub async fn patch_data(conbase: &TypeOfConnection, patch: &JsonPatch) -> DTPSR<()> {
    let json_data = serde_json::to_vec(patch)?;
    // debug!("patch_data out: {:#?}", String::from_utf8(json_data.clone()));
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
        return Err(DTPSError::FailedRequest(
            msg,
            status.as_u16(),
            conbase.to_string(),
            desc.to_string(),
        ));
    }

    Ok(())
}

fn string_from_header_value(header_value: &hyper::header::HeaderValue) -> String {
    header_value.to_str().unwrap().to_string()
}
