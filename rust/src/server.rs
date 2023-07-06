use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use futures::{SinkExt, StreamExt};
use maplit::hashmap;
use serde::{Deserialize, Serialize};
use serde_json;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;
use tungstenite::http::{HeaderMap, HeaderValue, StatusCode};
use warp::{Filter, Rejection, Reply};
use warp::http::header;
use warp::hyper::Body;
use warp::path::end as endbar;
use warp::reply::Response;
use warp::reply::with_status;
use std::net::{SocketAddr, ToSocketAddrs};

use crate::constants::*;
use crate::object_queues::*;
use crate::server_state::ServerState;
use crate::structures::*;
use crate::types::*;

pub struct DTPSServer {
    pub mutex: Arc<Mutex<ServerState>>,
}

impl DTPSServer {
    pub fn new() -> Self {
        let ss = ServerState::new();
        let server_private = Arc::new(Mutex::new(ss));
        DTPSServer {
            mutex: server_private,
        }
    }
    pub async fn serve(&mut self,
    one_addr: SocketAddr

    ) {
        let server_state_access: Arc<Mutex<ServerState>> = self.mutex.clone();

        let clone_access = warp::any().map(move || server_state_access.clone());


        // root GET /
        let root_route = endbar().and(clone_access.clone()).and_then(root_handler);

        let topic_address = warp::path!("topics" / String).and(endbar());

        // HEAD request
        let topic_generic_route_get = topic_address
            .and(warp::get())
            .and(clone_access.clone())
            .and_then(handler_topic_generic);

        // GET request
        let topic_generic_route_head = topic_address
            .and(warp::head())
            .and(clone_access.clone())
            .and_then(handler_topic_generic_head);

        // POST request
        let topic_post = topic_address
            .and(warp::post())
            .and(clone_access.clone())
            .and(warp::header::headers_cloned())
            .and(warp::body::bytes())
            .and_then(handle_topic_post);

        // GET request for data
        let topic_generic_route_data = warp::get()
            .and(warp::path!("topics" / String / "data" / String))
            .and(endbar())
            .and(clone_access.clone())
            .and_then(handler_topic_generic_data);

        // websockets for events
        let topic_generic_events_route = warp::path!("topics" / String / "events")
            .and(endbar())
            .and(clone_access.clone())
            .and_then(check_exists)
            .and(warp::query::<EventsQuery>())
            .and(warp::ws())
            .and(clone_access.clone())
            // .and_then(handle_websocket_generic_pre);
            .map({
                move |c: String,
                      q: EventsQuery,
                      ws: warp::ws::Ws,
                      state1: Arc<Mutex<ServerState>>| {
                    let send_data = match q.send_data {
                        Some(x) => x != 0,
                        None => false,
                    };
                    ws.on_upgrade(move |socket| {
                        handle_websocket_generic(socket, state1, c, send_data)
                    })
                }
            });

        let the_routes = topic_generic_events_route
            .or(topic_generic_route_head)
            .or(topic_generic_route_get)
            .or(root_route)
            .or(topic_generic_route_data)
            .or(topic_post);



        let tcp_server = warp::serve(the_routes).run(one_addr);

        // use getaddrs::InterfaceAddrs;
        //
        // let addrs = InterfaceAddrs::query_system()
        //     .expect("System has no network interfaces.");
        //
        // for addr in addrs {
        //     println!("{}: {:?}", addr.name, addr.address);
        // }

        tcp_server.await;

        // let unix_socket = "/tmp/mine.sock";
        // let unix_listener = UnixListener::bind(unix_socket).unwrap();
        //
        // let unix_routes = warp::any().and(the_routes.clone());
        //
        // //
        // // let unix_server = warp::serve(unix_routes)
        // //     .run_incoming(unix_listener);
        //
        // loop {
        //     let (stream, _) = unix_listener.accept().await.unwrap();
        //     let unix_routes = unix_routes.clone();
        //     tokio::spawn(async move {
        //           let service = warp::service(unix_routes);
        //     let request = warp::test::request().method(warp::http::Method::GET).path("/");
        //     let response = service.call(request).await.unwrap();
        //     });
        // }

        // use tokio_stream::wrappers::UnixListenerStream;
        // let incoming = UnixListenerStream::new(unix_listener);

        // let listener = UnixListener::bind("/tmp/warp.sock").unwrap();

        // let listener = UnixListener::bind(sockpath).unwrap();
        // let incoming = UnixStream::connect(listener).unwrap();
        // let a = UnixListenerStream;
        // warp::serve(the_routes).run_incoming(incoming).await;
        // warp::serve(the_routes).run(incoming).await;

        // futures::try_join!(tcp_server, unix_server).unwrap();
    }

    pub fn get_lock(&self) -> Arc<Mutex<ServerState>> {
        self.mutex.clone()
    }
}

async fn check_exists(
    topic_name: String,
    ss_mutex: Arc<Mutex<ServerState>>,
) -> Result<String, Rejection> {
    let ss = ss_mutex.lock().await;

    match ss.oqs.get(topic_name.as_str()) {
        None => Err(warp::reject::not_found()),
        // Some(_) => Err(warp::reject::not_found()),
        Some(_) => Ok(topic_name),
    }
}

async fn root_handler(ss_mutex: Arc<Mutex<ServerState>>) -> Result<impl Reply, Rejection> {
    let ss = ss_mutex.lock().await;
    let index = topics_index(&ss);

    let data_bytes = serde_json::to_string(&index).unwrap();

    let mut resp = Response::new(Body::from(data_bytes));
    let headers = resp.headers_mut();

    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );

    put_common_headers(&ss, headers);

    Ok(with_status(resp, StatusCode::OK))
}
// fn print_type_of<T>(_: &T) {
//     println!("{}", std::any::type_name::<T>())
// // }
//
// async fn handle_websocket_generic_pre(c: String, q: EventsQuery, ws: warp::ws::Ws, state1: Arc<Mutex<ServerState>>) {
//     let state2 = state1.clone();
//     let send_data = match q.send_data {
//         Some(x) => x != 0,
//         None => false,
//     };
//     ws.on_upgrade(move |socket| {
//         handle_websocket_generic(socket, state2.clone(), c, send_data)
//     });
//     return
// }

async fn handle_websocket_generic(
    ws: warp::ws::WebSocket,
    state: Arc<Mutex<ServerState>>,
    topic_name: String,
    send_data: bool,
) -> () {
    println!("handle_websocket_generic: {}", topic_name);
    let (mut ws_tx, _ws_rx) = ws.split();

    let mut rx2: Receiver<usize>;
    {
        // important: release the lock
        let ss0 = state.lock().await;

        let oq: &ObjectQueue;
        match ss0.oqs.get(topic_name.as_str()) {
            None => {
                // TODO: we shouldn't be here
                ws_tx.close().await.unwrap();
                return;
            }
            Some(y) => oq = y,
        }
        match oq.sequence.last() {
            None => {}
            Some(last) => {
                let the_availability = vec![ResourceAvailability {
                    url: format!("../data/{}/", last.digest),
                    available_until: epoch() + 60.0,
                }];
                let nchunks = if send_data { 1 } else { 0 };
                let dr = DataReady {
                    sequence: last.index,
                    digest: last.digest.clone(),
                    content_type: last.content_type.clone(),
                    content_length: last.content_length.clone(),
                    availability: the_availability,
                    chunks_arriving: nchunks,
                };

                let message = warp::ws::Message::text(serde_json::to_string(&dr).unwrap());
                ws_tx.send(message).await.unwrap();

                if send_data {
                    let message_data = oq.data.get(&last.digest).unwrap();
                    let message = warp::ws::Message::binary(message_data.content.clone());
                    ws_tx.send(message).await.unwrap();
                }
            }
        }

        rx2 = oq.tx.subscribe();
    }

    loop {
        let r = rx2.recv().await;
        let message;
        match r {
            Ok(_message) => {
                message = _message;
            }
            Err(RecvError::Closed) => {
                // The sender got dropped, we should exit
                break;
            }
            Err(RecvError::Lagged(_)) => {
                println!("Lagged!");
                continue;
            }
        }
        // println!(
        //     "Received update for topic {}: index {}",
        //     topic_name, message
        // );
        let ss2 = state.lock().await;
        let oq2 = ss2.oqs.get(&topic_name).unwrap();
        let this_one: &DataSaved = oq2.sequence.get(message).unwrap();
        let the_availability = vec![ResourceAvailability {
            url: format!("../data/{}/", this_one.digest),
            available_until: epoch() + 60.0,
        }];
        let nchunks = if send_data { 1 } else { 0 };
        let dr2 = DataReady {
            sequence: this_one.index,
            digest: this_one.digest.clone(),
            content_type: this_one.content_type.clone(),
            content_length: this_one.content_length,
            availability: the_availability,
            chunks_arriving: nchunks,
        };

        let message = warp::ws::Message::text(serde_json::to_string(&dr2).unwrap());
        match ws_tx.send(message).await {
            Ok(_) => {}
            Err(e) => {
                println!("Error sending message: {}", e);
                continue;
            }
        }
        if send_data {
            let message_data = oq2.data.get(&this_one.digest).unwrap();
            let message = warp::ws::Message::binary(message_data.content.clone());
            match ws_tx.send(message).await {
                Ok(_) => {}
                Err(e) => {
                    println!("Error sending message: {}", e);
                    continue;
                }
            }
        }
    }
}

pub fn epoch() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

pub fn topics_index(ss: &ServerState) -> TopicsIndex {
    let mut topics: HashMap<TopicName, TopicRef> = hashmap! {};

    for (topic_name, oq) in ss.oqs.iter() {
        topics.insert(topic_name.clone(), oq.tr.clone());
    }

    let topics_index = TopicsIndex {
        node_id: ss.node_id.clone(),
        topics,
    };

    return topics_index;
}

pub fn get_header_with_default(headers: &HeaderMap, key: &str, default: &str) -> String {
    return match headers.get(key) {
        None => default.to_string(),
        Some(v) => v.to_str().unwrap().to_string(),
    };
}


const CONTENT_TYPE: &str = "content-type";
const OCTET_STREAM: &str = "application/octet-stream";

async fn handle_topic_post(
    topic_name: String,
    ss_mutex: Arc<Mutex<ServerState>>,
    headers: HeaderMap,
    data: warp::hyper::body::Bytes,
) -> Result<impl Reply, Rejection> {
    let mut ss = ss_mutex.lock().await;


    let content_type =
        get_header_with_default(&headers, CONTENT_TYPE, OCTET_STREAM);

    let x: &mut ObjectQueue;
    match ss.oqs.get_mut(topic_name.as_str()) {
        None => {
            return Err(warp::reject::not_found());
        }
        Some(y) => x = y,
    };

    let byte_vector: Vec<u8> = data.to_vec().clone();

    x.push_data(&content_type, &byte_vector);

    let ok = "ok";
    Ok(warp::reply::json(&ok))
}

async fn handler_topic_generic(
    topic_name: String,
    ss_mutex: Arc<Mutex<ServerState>>,
) -> Result<impl Reply, Rejection> {
    let digest: String;
    {
        let ss = ss_mutex.lock().await;

        let x: &ObjectQueue;
        match ss.oqs.get(topic_name.as_str()) {
            None => {
                return Err(warp::reject::not_found());
                // panic!("Not found")
                // return Err(Rejection{ reason: Reason::NotFound, Reason::NotFound});
            }
            Some(y) => x = y,
        }

        // get the last element in the vector
        let last = x.sequence.last().unwrap();
        digest = last.digest.clone();
    }
    return handler_topic_generic_data(topic_name, digest, ss_mutex.clone()).await;
}


async fn handler_topic_generic_head(
    topic_name: String,
    ss_mutex: Arc<Mutex<ServerState>>,
) -> Result<Response, Rejection> {
    let ss = ss_mutex.lock().await;

    let x: &ObjectQueue;
    match ss.oqs.get(topic_name.as_str()) {
        None => {
            return Err(warp::reject::not_found());
            // panic!("Not found")
            // return Err(Rejection{ reason: Reason::NotFound, Reason::NotFound});
        }
        Some(y) => x = y,
    }
    let empty_vec: Vec<u8> = Vec::new();
    let mut resp = Response::new(Body::from(empty_vec));

    let h = resp.headers_mut();

    h.insert(
        HEADER_DATA_ORIGIN_NODE_ID,
        HeaderValue::from_str(x.tr.origin_node.as_str()).unwrap(),
    );
    h.insert(
        HEADER_DATA_UNIQUE_ID,
        HeaderValue::from_str(x.tr.unique_id.as_str()).unwrap(),
    );


    h.insert(HEADER_SEE_EVENTS, HeaderValue::from_static("events/"));
    h.insert(
        HEADER_SEE_EVENTS_INLINE_DATA,
        HeaderValue::from_static("events/?send_data=1"),
    );

    put_common_headers(&ss, h);

    Ok(resp.into())
}

async fn handler_topic_generic_data(
    topic_name: String,
    digest: String,
    ss_mutex: Arc<Mutex<ServerState>>,
) -> Result<Response, Rejection> {
    let ss = ss_mutex.lock().await;

    let x: &ObjectQueue;
    match ss.oqs.get(topic_name.as_str()) {
        None => {
            return Err(warp::reject::not_found());
        }
        Some(y) => x = y,
    }

    let data = x.data.get(&digest).unwrap();
    let data_bytes = data.content.clone();
    let content_type = data.content_type.clone();

    let mut resp = Response::new(Body::from(data_bytes));

    resp.headers_mut().insert(
        HEADER_DATA_ORIGIN_NODE_ID,
        HeaderValue::from_str(x.tr.origin_node.as_str()).unwrap(),
    );
    resp.headers_mut().insert(
        HEADER_DATA_UNIQUE_ID,
        HeaderValue::from_str(x.tr.unique_id.as_str()).unwrap(),
    );

    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(content_type.clone().as_str()).unwrap(),
    );
    // see events
    resp.headers_mut()
        .insert(HEADER_SEE_EVENTS, HeaderValue::from_static("events/"));
    resp.headers_mut().insert(
        HEADER_SEE_EVENTS_INLINE_DATA,
        HeaderValue::from_static("events/?send_data=1"),
    );

    put_common_headers(&ss, resp.headers_mut());

    Ok(resp.into())
}

pub fn put_common_headers(ss: &ServerState, headers: &mut HeaderMap<HeaderValue>) {
    headers.insert(
        header::SERVER,
        HeaderValue::from_static("lib-dtps/rust/0.0.0"),
    );
    headers.insert(
        HEADER_NODE_ID,
        HeaderValue::from_str(ss.node_id.as_str()).unwrap(),
    );
}

#[derive(Serialize, Deserialize)]
struct EventsQuery {
    send_data: Option<u8>,
}
