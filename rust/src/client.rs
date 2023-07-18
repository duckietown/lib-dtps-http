use std::any::Any;
use std::fmt::Debug;
use std::os::unix::fs::FileTypeExt;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use base64;
use base64::{engine::general_purpose, Engine as _};
use bytes::Bytes;

use hex;
use hyper;
use hyper::Client;
use hyper_tls::HttpsConnector;
use hyperlocal::UnixClientExt;
use log::{debug, error, info};
use rand::Rng;
use tokio::net::{TcpStream, UnixStream};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{client_async_with_config, connect_async, MaybeTlsStream, WebSocketStream};
use tungstenite::handshake::client::Request;
use url::Url;
use warp::reply::Response;
use warp::ws::Message;

use crate::constants::{
    HEADER_CONTENT_LOCATION, HEADER_NODE_ID, HEADER_SEE_EVENTS, HEADER_SEE_EVENTS_INLINE_DATA,
};
use crate::structures::TypeOfConnection::{Relative, TCP, UNIX};
use crate::structures::{
    DataReady, FoundMetadata, LinkBenchmark, TopicsIndexInternal, TopicsIndexWire, TypeOfConnection,
};
use crate::urls::{join_ext, parse_url_ext};
use crate::utils::time_nanos;
use crate::websocket_abstractions::open_websocket_connection;
use crate::websocket_signals::MsgServerToClient;
use crate::TypeOfConnection::Same;
use crate::UrlResult::{Accessible, Inaccessible, WrongNodeAnswering};
use crate::{
    context, error_with_info, internal_assertion, not_available, not_implemented, not_reachable,
    show_errors, DTPSError, RawData, TopicName, UnixCon, CONTENT_TYPE_DTPS_INDEX, DTPSR,
};

/// Note: need to have use futures::{StreamExt} in scope to use this
pub async fn get_events_stream_inline(
    url: TypeOfConnection,
) -> (JoinHandle<DTPSR<()>>, UnboundedReceiverStream<Notification>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let inline_url = url.clone();
    let handle = tokio::spawn(listen_events_url_inline(inline_url, tx));
    let stream = UnboundedReceiverStream::new(rx);
    (handle, stream)
}
use futures::StreamExt;
pub async fn listen_events(which: TopicName, md: FoundMetadata) {
    // let (tx, rx) = mpsc::unbounded_channel();
    let inline_url = md.events_data_inline_url.unwrap().clone();

    let (handle, mut stream) = get_events_stream_inline(inline_url).await;

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

    // }
    // handle.await.unwrap().unwrap();
}

pub fn ms_from_ns(ns: u128) -> f64 {
    (ns as f64) / 1_000_000.0
}

#[derive(Debug)]
pub struct Notification {
    pub dr: DataReady,
    pub rd: RawData,
}

pub async fn listen_events_url_inline(
    con: TypeOfConnection,
    tx: UnboundedSender<Notification>,
) -> DTPSR<()> {
    let wsc = open_websocket_connection(&con).await?;

    // debug!("starting to listen to events for {} on {:?}", con, read);
    let mut index: u32 = 0;
    let mut rx = wsc.get_incoming().await;
    loop {
        let msg = match rx.recv().await {
            Ok(msg) => msg,
            Err(e) => match e {
                RecvError::Closed => {
                    break;
                }
                RecvError::Lagged(_) => {
                    error_with_info!("lagged");
                    continue;
                }
            },
        };

        if !msg.is_binary() {
            debug!("unexpected message #{}: {:#?}", index, msg);
            continue;
        } else {
            let data = msg.clone().into_data();
            // parse as cbor
            // let dr: DataReady;
            let msg_from_server: MsgServerToClient;
            match serde_cbor::from_slice::<MsgServerToClient>(&data) {
                Ok(dr_) => {
                    // debug!("dr: {:#?}", dr_);
                    msg_from_server = dr_;
                }
                Err(e) => {
                    let rawvalue = serde_cbor::from_slice::<serde_cbor::Value>(&data);
                    debug!(
                        "message #{}: cannot parse cbor as MsgServerToClient: {:#?}\n{:#?}\n{:#?}",
                        index,
                        e,
                        msg.type_id(),
                        rawvalue,
                    );
                    continue;
                }
            }
            let dr = match msg_from_server {
                MsgServerToClient::DataReady(dr_) => dr_,
                _ => {
                    debug!(
                        "message #{}: unexpected message: {:#?}",
                        index, msg_from_server
                    );
                    continue;
                }
            };
            if dr.chunks_arriving == 0 {
                error_with_info!(
                    "message #{}: no chunks arriving. listening to {}",
                    index,
                    con
                );
                continue;
            }
            // debug!("message #{}: {:#?}", index, dr);
            index += 1;
            let mut content: Vec<u8> = Vec::with_capacity(dr.content_length);
            for _ in 0..(dr.chunks_arriving) {
                let msg = match rx.recv().await {
                    Ok(msg) => msg,
                    Err(e) => match e {
                        RecvError::Closed => {
                            error_with_info!("unexpected end of stream");
                            break;
                        }
                        RecvError::Lagged(_) => {
                            error_with_info!("lagged");
                            continue;
                        }
                    },
                };

                if msg.is_binary() {
                    let data = msg.into_data();
                    content.extend(data);
                } else {
                    error_with_info!("unexpected message #{}: {:#?}", index, msg);
                }
                index += 1;
            }
            if content.len() != dr.content_length {
                error_with_info!(
                    "unexpected content length: {} != {}",
                    content.len(),
                    dr.content_length
                );
                continue;
            }
            let content_type = dr.content_type.clone();
            let rd = RawData {
                content: Bytes::from(content),
                content_type,
            };
            let notification = Notification { dr, rd };
            match tx.send(notification) {
                Ok(_) => {}
                Err(e) => {
                    error_with_info!("cannot send data: {}", e);
                    break;
                }
            }
        }
    }
    Ok(())
}

pub async fn get_rawdata(con: &TypeOfConnection) -> DTPSR<RawData> {
    let resp = make_request(con, hyper::Method::GET).await?;
    // TODO: send more headers

    //  .header("Accept", "application/cbor")
    let content_type = resp
        .headers()
        .get("content-type")
        .map(|x| x.to_str().unwrap().to_string())
        .unwrap_or("application/octet-stream".to_string());
    // Get the response body bytes.
    let body_bytes = hyper::body::to_bytes(resp.into_body()).await?;
    Ok(RawData {
        content: body_bytes,
        content_type,
    })
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
    let resp = context!(
        make_request(con, hyper::Method::GET).await,
        "Cannot make request to {}",
        con.to_string(),
    )?;
    let content_type = get_content_type(&resp);
    debug!("content_type: {:#?}", content_type);

    if content_type.contains(CONTENT_TYPE_DTPS_INDEX) {
        Ok(TypeOfResource::DTPSIndex)
    } else {
        Ok(TypeOfResource::Other)
    }
}

pub async fn get_index(con: &TypeOfConnection) -> DTPSR<TopicsIndexInternal> {
    let resp = context!(
        make_request(con, hyper::Method::GET).await,
        "Cannot make request to {:?}",
        con
    )?;

    let body_bytes = context!(
        hyper::body::to_bytes(resp.into_body()).await,
        "Cannot get body bytes"
    )?;

    let x0: TopicsIndexWire = context!(
        serde_cbor::from_slice(&body_bytes),
        "Cannot interpret as CBOR"
    )?;

    let ti = TopicsIndexInternal::from_wire(x0, con);

    // debug!("get_index: {:#?}\n {:#?}", con, ti);
    Ok(ti)
}

#[derive(Debug, Clone)]
pub enum UrlResult {
    /// a
    Inaccessible(String),
    /// b
    WrongNodeAnswering,
    /// c
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
    let reliability = match con {
        TCP(_) => 0.7,
        UNIX(_) => 0.9,
        Relative(_, _) => {
            panic!("unexpected relative url here: {}", con);
        }
        Same() => {
            panic!("not expected here {}", con);
        }
        TypeOfConnection::File(..) => 1.0,
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
                        let latency = (md_.latency_ns as f32) / 1_000_000_000.0;
                        let lb = LinkBenchmark {
                            complexity,
                            bandwidth: 100_000_000,
                            latency,
                            reliability,
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
    alternatives: &Vec<TypeOfConnection>,
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
        debug!("metadata for {}: {:?}", file_path, metadata);
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

pub async fn make_request(conbase: &TypeOfConnection, method: hyper::Method) -> DTPSR<Response> {
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

    let req0 = context!(
        hyper::Request::builder()
            .method(&method)
            .uri(use_url.as_str())
            // .header("user-agent", "the-awesome-agent/007")
            .body(hyper::Body::from("")),
        "cannot build request for {} {}",
        method,
        use_url.as_str()
    )?;

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
        "make_request(): cannot make {} request for connection {:?} \
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

    let resp = make_request(conbase, hyper::Method::HEAD).await?;
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
    let events_url = headers
        .get(HEADER_SEE_EVENTS)
        .map(string_from_header_value)
        .map(|x| join_ext(&conbase, &x).ok())
        .flatten();

    let events_data_inline_url = headers
        .get(HEADER_SEE_EVENTS_INLINE_DATA)
        .map(string_from_header_value)
        .map(|x| join_ext(&conbase, &x).ok())
        .flatten();
    let answering = headers.get(HEADER_NODE_ID).map(string_from_header_value);
    let md = FoundMetadata {
        alternative_urls,
        events_url,
        answering,
        events_data_inline_url,
        latency_ns,
    };
    // debug!("headers for {} {:#?} {:#?}", url, headers, md);

    Ok(md)
}

fn string_from_header_value(header_value: &hyper::header::HeaderValue) -> String {
    header_value.to_str().unwrap().to_string()
}
//
// struct WebsocketOpen {
//     pub tx: UnboundedSender<Notification>,
//     pub rx: UnboundedReceiver<Notification>,
//     pub join_handle: JoinHandle<DTPSR<()>>,
// }
