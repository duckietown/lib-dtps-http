use std::collections::HashMap;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use maplit::hashmap;
use serde_json;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;
use warp::http::header;
use warp::hyper::Body;
use warp::reply::Response;
use warp::{Filter, Rejection};

use crate::constants::*;
use crate::object_queues::*;
use crate::server_state::ServerState;
use crate::structures::*;
use crate::types::*;
use serde::{Deserialize, Serialize};

pub struct DTPSServer {
    pub mutex: Arc<Mutex<ServerState>>,
}

impl DTPSServer {
    pub fn new() -> Self {
        let ss = ServerState::new();

        // println!("ss: {:?}", ss);
        let server_private = Arc::new(Mutex::new(ss));
        DTPSServer {
            mutex: server_private,
        }
    }
    pub async fn serve(&mut self) {
        let server_state_access: &Arc<Mutex<ServerState>> = &self.mutex;

        let topic_generic_route = warp::path!("topics" / String)
            .and(warp::path::end())
            .and_then({
                let state = server_state_access.clone();
                move |c| handler_topic_generic(state.clone(), c)
            });
        let topic_generic_route_data = warp::path!("topics" / String / "data" / String)
            .and(warp::path::end())
            .and_then({
                let state = server_state_access.clone();
                move |c, digest| handler_topic_generic_data(state.clone(), c, digest)
            });

        let topic_generic_events_route = warp::path!("topics" / String / "events")
            .and(warp::path::end())
            .and_then({
                let statec = server_state_access.clone();
                move |c| check_exists(statec.clone(), c)
            })
            .and(warp::query::<EventsQuery>())
            .and(warp::ws())
            .map({
                let state1 = server_state_access.clone();
                move |c: String, q: EventsQuery, ws: warp::ws::Ws| {
                    let state2 = state1.clone();
                    let send_data = match q.send_data {
                        Some(x) => x != 0,
                        None => false,
                    };
                    ws.on_upgrade(move |socket| {
                        handle_websocket_generic(socket, state2.clone(), c, send_data)
                    })
                }
            });

        let root_route = warp::path::end().and_then({
            let state = server_state_access.clone();
            move || root_handler(state.clone())
        });

        let the_routes = topic_generic_events_route
            .or(topic_generic_route)
            .or(root_route)
            .or(topic_generic_route_data);

        warp::serve(the_routes).run(([127, 0, 0, 1], 8000)).await;
    }

    pub fn get_lock(&self) -> Arc<Mutex<ServerState>> {
        self.mutex.clone()
    }
}

async fn check_exists(
    ss_mutex: Arc<Mutex<ServerState>>,
    topic_name: String,
) -> Result<String, warp::Rejection> {
    let ss = ss_mutex.lock().await;

    match ss.oqs.get(topic_name.as_str()) {
        None => Err(warp::reject::not_found()),
        // Some(_) => Err(warp::reject::not_found()),
        Some(_) => Ok(topic_name),
    }
}

async fn root_handler(
    ss_mutex: Arc<Mutex<ServerState>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let ss = ss_mutex.lock().await;
    let index = topics_index(&ss);
    Ok(warp::reply::json(&index))
}

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
                    available_until: 0.0,
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

        // let message_data = oq.data.get(&last.digest).unwrap();

        rx2 = oq.tx.subscribe();
    }

    loop {
        let r = rx2.recv().await;
        match r {
            Ok(message) => {
                println!(
                    "Received update for topic {}: index {}",
                    topic_name, message
                );
                let ss2 = state.lock().await;
                let oq2 = ss2.oqs.get(&topic_name).unwrap();
                let this_one: &DataSaved = oq2.sequence.get(message).unwrap();
                let the_availability = vec![ResourceAvailability {
                    url: format!("../data/{}/", this_one.digest),
                    available_until: 0.0,
                }];
                let nchunks = if send_data { 1 } else { 0 };
                let dr2 = DataReady {
                    sequence: this_one.index,
                    digest: this_one.digest.clone(),
                    content_type: this_one.content_type.clone(),
                    content_length: this_one.content_length.clone(),
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
            Err(RecvError::Closed) => {
                // The sender got dropped, we should exit
                break;
            }
            Err(RecvError::Lagged(_)) => {
                println!("Lagged!");
            }
        }
    }
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

async fn handler_topic_generic(
    ss_mutex: Arc<Mutex<ServerState>>,
    topic_name: String,
) -> Result<impl warp::Reply, Rejection> {
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
    return handler_topic_generic_data(ss_mutex.clone(), topic_name, digest).await;
}

async fn handler_topic_generic_data(
    ss_mutex: Arc<Mutex<ServerState>>,
    topic_name: String,
    digest: String,
) -> Result<warp::reply::Response, Rejection> {
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
        HEADER_NODE_ID,
        header::HeaderValue::from_str(ss.node_id.as_str()).unwrap(),
    );
    resp.headers_mut().insert(
        HEADER_DATA_ORIGIN_NODE_ID,
        header::HeaderValue::from_str(x.tr.origin_node.as_str()).unwrap(),
    );
    resp.headers_mut().insert(
        HEADER_DATA_UNIQUE_ID,
        header::HeaderValue::from_str(x.tr.unique_id.as_str()).unwrap(),
    );

    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_str(content_type.clone().as_str()).unwrap(),
    );
    resp.headers_mut().insert(
        HEADER_SEE_EVENTS,
        header::HeaderValue::from_static("events/"),
    );
    resp.headers_mut().insert(
        HEADER_SEE_EVENTS_INLINE_DATA,
        header::HeaderValue::from_static("events/?send_data=1"),
    );

    Ok(resp.into())
}

#[derive(Serialize, Deserialize)]
struct EventsQuery {
    send_data: Option<u8>,
}
