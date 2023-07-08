extern crate dtps_http;
extern crate url;

use std::any::Any;
use std::error;
use std::error::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use futures::future::join_all;
use futures::StreamExt;
use hex;
use hyper;
use hyper::Client;
use hyperlocal::UnixClientExt;
use log::{debug, error, info, warn};
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tungstenite::connect_async;
use url::Url;
use warp::reply::Response;

use dtps_http::constants::{
    HEADER_CONTENT_LOCATION, HEADER_NODE_ID, HEADER_SEE_EVENTS, HEADER_SEE_EVENTS_INLINE_DATA,
};
use dtps_http::logs::init_logging;
use dtps_http::object_queues::RawData;
use dtps_http::structures::TypeOfConnection::{Relative, TCP, UNIX};
use dtps_http::structures::{
    DataReady, FoundMetadata, LinkBenchmark, TopicsIndexInternal, TopicsIndexWire, TypeOfConnection,
};
use dtps_http::urls::{join_ext, parse_url_ext};

use crate::UrlResult::{Accessible, Inaccessible, WrongNodeAnswering};

/// Parameters for client
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct StatsArgs {
    /// base URL to open
    #[arg(long)]
    url: String,

    /// Cloudflare tunnel to start
    #[arg(long)]
    inline_data: bool,
}

async fn listen_events(md: FoundMetadata, topic_name: String) {
    let (tx, rx) = mpsc::unbounded_channel();
    warn!("listening to events on {}", topic_name);
    let inline_url = md.events_data_inline_url.unwrap().clone();
    let handle = spawn(listen_events_url_inline(inline_url, tx));
    let mut stream = UnboundedReceiverStream::new(rx);

    // keep track of the latencies in a vector and compute the mean

    let mut latencies_ns = Vec::new();
    let mut index = 0;
    while let Some(notification) = stream.next().await {
        // debug!("got event for {}: {:?} {:?}", topic_name, notification,
        // notification.rd.content);
        if topic_name == "clock" {
            // parse the data as json
            let nanos = serde_json::from_slice::<u128>(&notification.rd.content).unwrap();
            let nanos_here = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let diff = nanos_here - nanos;
            // let diff_ms = diff as f64 / 1_000_000.0;
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
                info!("{:12} latency: {:.3}ms   (last {} : mean: {:.3}ms  min: {:.3}ms  max {:.3}ms )", topic_name, ms_from_ns(diff),
                         latencies_ns.len(),
                         ms_from_ns(latencies_mean_ns),
                ms_from_ns(latencies_min_ns), ms_from_ns(latencies_max_ns));
            }
        }

        index += 1;
    }

    handle.await.unwrap();
}

fn ms_from_ns(ns: u128) -> f64 {
    (ns as f64) / 1_000_000.0
}

#[derive(Debug)]
pub struct Notification {
    pub dr: DataReady,
    pub rd: RawData,
}

async fn listen_events_url_inline(con: TypeOfConnection, tx: UnboundedSender<Notification>) {
    let mut url = match con {
        TCP(url_) => {
            // debug!("connecting to {:?}", url_);
            url_.clone()
        }

        UNIX(uc) => {
            panic!("not implemented unix connection: {:?}", uc);
        }
        Relative(_, _) => {
            panic!("not expected here {}", con);
        }
    };
    // let mut url = md.events_data_inline_url.unwrap().clone();
    // replace https with wss, and http with ws
    if url.scheme() == "https" {
        url.set_scheme("wss").unwrap();
    } else if url.scheme() == "http" {
        url.set_scheme("ws").unwrap();
    } else {
        panic!("unexpected scheme: {}", url.scheme());
    }
    let connection_res = connect_async(url.clone()).await;
    // debug!("connection: {:#?}", connection);
    let connection;
    match connection_res {
        Ok(c) => {
            connection = c;
        }
        Err(err) => {
            error!("could not connect to {}: {}", url, err);
            return;
        }
    }
    let (ws_stream, _response) = connection;

    // debug!("Connected to the server");
    // debug!("Response HTTP code: {}", response.status());
    // debug!("Response contains the following headers:");
    // for (header, value) in response.headers().iter() {
    //     debug!("* {:?} {:?}", header, value);
    // }

    let (_write, mut read) = ws_stream.split();
    //
    // let send_msg = write.send(Message::Text("Hello WebSocket".into()));
    // tokio::task::spawn(send_msg);
    let mut index: u32 = 0;
    loop {
        let msg = read.next().await.unwrap().unwrap();
        if !msg.is_binary() {
            debug!("unexpected message #{}: {:#?}", index, msg);
            continue;
        } else {
            let data = msg.clone().into_data();
            // parse as cbor
            let dr: DataReady;
            match serde_cbor::from_slice::<DataReady>(&data) {
                Ok(dr_) => {
                    // debug!("dr: {:#?}", dr_);
                    dr = dr_;
                }
                Err(e) => {
                    debug!(
                        "message #{}: cannot parse cbor as DataReady: {:#?}\n{:#?}",
                        index,
                        e,
                        msg.type_id()
                    );
                    continue;
                }
            }
            if dr.chunks_arriving == 0 {
                error!(
                    "message #{}: no chunks arriving. listening to {}",
                    index, url
                );
                continue;
            }
            // debug!("message #{}: {:#?}", index, dr);
            index += 1;
            let mut content: Vec<u8> = Vec::with_capacity(dr.content_length);
            for _ in 0..(dr.chunks_arriving) {
                let msg = read.next().await.unwrap().unwrap();
                if msg.is_binary() {
                    let data = msg.into_data();
                    content.extend(data);
                } else {
                    error!("unexpected message #{}: {:#?}", index, msg);
                }
                index += 1;
            }
            if content.len() != dr.content_length {
                error!(
                    "unexpected content length: {} != {}",
                    content.len(),
                    dr.content_length
                );
                continue;
            }

            let rd = RawData {
                content,
                content_type: dr.content_type.clone(),
            };
            let notification = Notification { dr, rd };
            match tx.send(notification) {
                Ok(_) => {}
                Err(e) => {
                    error!("cannot send data: {}", e);
                    break;
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    init_logging();
    debug!("now waiting 1 seconds so that the server can start");
    sleep(Duration::from_secs(1)).await;

    let args = StatsArgs::parse();
    // print!("{} {}", args.url, args.inline_data);

    let bc = parse_url_ext(args.url.as_str())?;
    // debug!("connection base: {:#?}", bc);
    let md = get_metadata(&bc).await?;
    // debug!("metadata:\n{:#?}", md);
    match md.answering {
        None => {
            info!("no answering url found");
            return Err("no answering url found".into());
        }
        Some(_) => {}
    }

    let best =
        compute_best_alternative(&md.alternative_urls, md.answering.unwrap().as_str()).await?;
    warn!("Best connection: {} ", best);

    let x = get_index(&best).await?;

    warn!("Internal: {:#?} ", x);
    //
    // if best.is_none() {
    //     info!("no alternative url found");
    //     return;
    // }
    // let use_url = best.unwrap();
    //
    // if use_url.is_none() {
    //     info!("no alternative url found");
    //     return;
    // }

    // debug!("{:#?}", x);
    let mut handles: Vec<JoinHandle<_>> = Vec::new();

    for (topic_name, topic_info) in &x.topics {
        // debug!("{}", topic_name);
        for r in &topic_info.reachability {
            // let real_uri = urlbase.join(&r.url).unwrap();
            // debug!("{}  {} -> {}", topic_name, r.url, real_uri);
            let md_res = get_metadata(&r.con).await;
            // debug!("md for {}: {:#?}", topic_name, md_res);
            let md;
            match md_res {
                Ok(md_) => {
                    md = md_;
                }
                Err(_) => {
                    warn!("cannot get metadata for {:?}", r.con);
                    continue;
                }
            }

            let handle = spawn(listen_events(md, topic_name.clone()));
            handles.push(handle);
        }
    }
    // listen to all spawned tasks
    let results = join_all(handles).await;

    for result in results {
        match result {
            Ok(val) => debug!("Finished task with result: {:?}", val),
            Err(err) => debug!("Task returned an error: {:?}", err),
        }
    }

    Ok(())
}

pub async fn get_index(
    con: &TypeOfConnection,
) -> Result<TopicsIndexInternal, Box<dyn error::Error>> {
    let resp = make_request(con, hyper::Method::GET).await?;
    // TODO: send more headers

    //  .header("Accept", "application/cbor")

    // Get the response body bytes.
    let body_bytes = hyper::body::to_bytes(resp.into_body()).await?;
    let x0: TopicsIndexWire = serde_cbor::from_slice(&body_bytes).unwrap();
    let ti = TopicsIndexInternal::from_wire(x0, con);

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

async fn get_stats(con: &TypeOfConnection, expect_node_id: &str) -> UrlResult {
    let md = get_metadata(con).await;
    let complexity = match con {
        TCP(_) => 1,
        UNIX(_) => 0,
        Relative(_, _) => {
            panic!("unexpected relative url here: {}", con);
        }
    };
    let reliability = match con {
        TCP(_) => 0.9,
        UNIX(_) => 1.0,
        Relative(_, _) => {
            panic!("unexpected relative url here: {}", con);
        }
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
                            bandwidth: 100_000_000.0,
                            latency,
                            reliability,
                            hops: 0,
                        };
                        Accessible(lb)
                    }
                }
            };
        }
    }
}

// async fn get_stats_unix_socket(url: Url) -> UrlResult {
//     Inaccessible
// }
//
// async fn get_stats_for_url(url: Url) -> UrlResult {
//     match url.scheme() {
//         "https" => get_stats_http(url).await,
//         "http" => get_stats_http(url).await,
//         "http+unix" => get_stats_unix_socket(url).await,
//         &_ => { panic!("unexpected scheme: {}", url.scheme()); }
//     }
// }

pub async fn compute_best_alternative(
    alternatives: &Vec<TypeOfConnection>,
    expect_node_id: &str,
) -> Result<TypeOfConnection, Box<dyn error::Error>> {
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
        return Err("no alternative are accessible".into());
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
//
// pub async fn get_metadata(tc: &TypeOfConnection) -> Result<FoundMetadata, Box<dyn error::Error>> {
//     match tc {
//         TypeOfConnection::TCP(url) => get_metadata_http(url).await,
//         TypeOfConnection::UNIX(_path) => {
//             Err("unix socket not supported yet for get_metadata()".into())
//         }
//         TypeOfConnection::Relative(_, _) => {
//             Err("cannot handle a relative url get_metadata()".into())
//         }
//     }
// }

pub async fn make_request(
    conbase: &TypeOfConnection,
    method: hyper::Method,
) -> Result<Response, Box<dyn error::Error>> {
    let use_url = match conbase {
        TCP(url) => url.clone().to_string(),
        UNIX(uc) => {
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
            return Err("cannot handle a relative url get_metadata()".into());
        }
    };

    let req0 = hyper::Request::builder()
        .method(method)
        .uri(use_url.as_str())
        // .header("user-agent", "the-awesome-agent/007")
        .body(hyper::Body::from(""))?;

    let resp = match conbase {
        TypeOfConnection::TCP(_) => {
            let client = hyper::Client::new();
            client.request(req0).await?
        }
        TypeOfConnection::UNIX(_) => {
            let client = Client::unix();
            client.request(req0).await?
        }

        TypeOfConnection::Relative(_, _) => {
            return Err("cannot handle a relative url get_metadata()".into());
        }
    };

    Ok(resp)
}

pub async fn get_metadata(
    conbase: &TypeOfConnection,
) -> Result<FoundMetadata, Box<dyn error::Error>> {
    // current time in nano seconds
    let start = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let resp = make_request(conbase, hyper::Method::HEAD).await?;
    let end = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

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
