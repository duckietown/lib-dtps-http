extern crate dtps_http;
extern crate url;
use log::{debug, info};
use std::any::Any;
use std::error;
use std::time::Duration;

use clap::Parser;
use futures::future::join_all;
use futures::StreamExt;
use hyper;
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tungstenite::connect_async;
use url::Url;

use dtps_http::constants::{
    HEADER_CONTENT_LOCATION, HEADER_NODE_ID, HEADER_SEE_EVENTS, HEADER_SEE_EVENTS_INLINE_DATA,
};
use dtps_http::logs::init_logging;
use dtps_http::object_queues::RawData;
use dtps_http::structures::{DataReady, FoundMetadata, TopicsIndex};

// use futures_util::StreamExt;

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

    let handle = spawn(listen_events_url(md, tx));
    let mut stream = UnboundedReceiverStream::new(rx);

    // keep track of the latencies in a vector and compute the mean

    let mut latencies_ns = Vec::new();
    let mut index = 0;
    while let Some(rd) = stream.next().await {
        if topic_name == "clock" {
            // parse the data as json
            let nanos = serde_json::from_slice::<u128>(&rd.content).unwrap();
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

async fn listen_events_url(md: FoundMetadata, tx: UnboundedSender<RawData>) {
    let mut url = md.events_data_inline_url.unwrap().clone();
    // replace https with wss, and http with ws
    if url.scheme() == "https" {
        url.set_scheme("wss").unwrap();
    } else if url.scheme() == "http" {
        url.set_scheme("ws").unwrap();
    } else {
        panic!("unexpected scheme: {}", url.scheme());
    }
    let connection = connect_async(url.clone()).await;
    // debug!("connection: {:#?}", connection);
    let (ws_stream, response) = connection.expect("Failed to connect");

    debug!("Connected to the server");
    debug!("Response HTTP code: {}", response.status());
    debug!("Response contains the following headers:");
    for (header, value) in response.headers().iter() {
        debug!("* {:?} {:?}", header, value);
    }

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
            index += 1;
            let mut content: Vec<u8> = Vec::with_capacity(dr.content_length);
            for _ in 0..(dr.chunks_arriving) {
                let msg = read.next().await.unwrap().unwrap();
                if msg.is_binary() {
                    let data = msg.into_data();
                    content.extend(data);
                } else {
                    debug!("unexpected message #{}: {:#?}", index, msg);
                }
                index += 1;
            }

            let rd = RawData {
                content,
                content_type: dr.content_type.clone(),
            };
            tx.send(rd).unwrap();
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

    let urlbase = args.url.parse::<Url>().unwrap();
    // debug!("urlbase: {:#?}", urlbase);
    let x = get_index(urlbase.clone()).await.unwrap();

    // debug!("{:#?}", x);
    let mut handles: Vec<JoinHandle<_>> = Vec::new();

    for (topic_name, topic_info) in x.topics {
        debug!("{}", topic_name);
        for r in topic_info.reachability {
            let real_uri = urlbase.join(&r.url).unwrap();
            // debug!("{}  {} -> {}", topic_name, r.url, real_uri);

            let md = get_metadata(real_uri).await;
            // debug!("md: {:#?}", md);
            let handle = spawn(listen_events(md, topic_name.clone()));
            handles.push(handle);
            // make a request to the real_uri
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

pub async fn get_index(url: Url) -> Result<TopicsIndex, Box<dyn error::Error>> {
    let client = hyper::Client::new();

    let req = hyper::Request::builder()
        .method(hyper::Method::GET)
        .uri(url.as_str())
        .header("Accept", "application/cbor")
        // .header("user-agent", "the-awesome-agent/007")
        .body(hyper::Body::from(""))?;

    // Pass our request builder object to our client.
    let resp = client.request(req).await?;

    // Get the response body bytes.
    let body_bytes = hyper::body::to_bytes(resp.into_body()).await?;
    let x = serde_cbor::from_slice(&body_bytes).unwrap();
    // let x: TopicsIndex = serde_json::from_str(&body).unwrap();
    Ok(x)
}

pub async fn get_metadata(url: Url) -> FoundMetadata {
    let client = hyper::Client::new();

    let req = hyper::Request::builder()
        .method(hyper::Method::HEAD)
        .uri(url.as_str())
        // .header("user-agent", "the-awesome-agent/007")
        .body(hyper::Body::from(""))
        .unwrap();

    // Pass our request builder object to our client.
    let resp = client.request(req).await.unwrap();

    // get the headers from the response
    let headers = resp.headers();
    // get all the HEADER_CONTENT_LOCATION in the response
    let alternatives0 = headers.get_all(HEADER_CONTENT_LOCATION);
    debug!("alternatives0: {:#?}", alternatives0);
    // convert into a vector of strings
    let alternative_urls: Vec<String> =
        alternatives0.iter().map(string_from_header_value).collect();
    // convert into a vector of URLs
    let alternative_urls: Vec<Url> = alternative_urls
        .iter()
        .map(|x| Url::parse(x).unwrap())
        .collect();
    let events_url = headers
        .get(HEADER_SEE_EVENTS)
        .map(string_from_header_value)
        .map(|x| url.join(&x).unwrap());
    let events_data_inline_url = headers
        .get(HEADER_SEE_EVENTS_INLINE_DATA)
        .map(string_from_header_value)
        .map(|x| url.join(&x).unwrap());
    let answering = headers.get(HEADER_NODE_ID).map(string_from_header_value);
    FoundMetadata {
        alternative_urls,
        events_url,
        answering,
        events_data_inline_url,
    }
}

fn string_from_header_value(header_value: &hyper::header::HeaderValue) -> String {
    header_value.to_str().unwrap().to_string()
}
//
// fn parse_better(url: Url) -> Url {
//     let mut new_url = url.clone();
//     // replace %2F with / in the host
//     let new_host = new_url.host().unwrap().to_string().replace("%2F", "/");
//     new_url.set_host(Some(new_host.as_str())).unwrap();
//     new_url
// }
//   # logger.info(f"headers : {resp.headers}")
//                     if HEADER_CONTENT_LOCATION in resp.headers:
//                         alternatives0 = cast(list[URLString], resp.headers.getall(HEADER_CONTENT_LOCATION))
//                     else:
//                         alternatives0 = []
//
//                     events_url = resp.headers.get(HEADER_SEE_EVENTS, None)
//                     if events_url is not None:
//                         events_url = cast(URLWSOffline, join(url, events_url))
//                     events_url_data = resp.headers.get(HEADER_SEE_EVENTS_INLINE_DATA, None)
//                     if events_url_data is not None:
//                         events_url_data = cast(URLWSInline, join(url, events_url_data))
//
//                     if HEADER_NODE_ID not in resp.headers:
//                         answering = None
//                     else:
//                         answering = NodeID(resp.headers[HEADER_NODE_ID])
