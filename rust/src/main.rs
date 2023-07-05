use std::collections::HashMap;
use std::sync::Arc;

use chrono::prelude::*;
use futures::{SinkExt, StreamExt};
use maplit::hashmap;
use serde_json;
use tokio::spawn;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};
use warp::http::header;
use warp::hyper::Body;
use warp::reply::Response;
use warp::{Filter, Rejection};

use constants::*;
use object_queues::*;
use server_state::*;
use structures::*;
use types::*;

mod constants;
mod object_queues;
mod server_state;
mod structures;
mod types;

async fn clock_go(state: Arc<Mutex<ServerState>>, topic_name: &str, interval_s: f32) {
    let mut clock = interval(Duration::from_secs_f32(interval_s));
    clock.tick().await;
    loop {
        clock.tick().await;
        let mut ss = state.lock().await;
        let current_datetime = Local::now();
        let datetime_string = current_datetime.to_rfc3339();

        let inserted = ss.publish_plain(topic_name, datetime_string.as_str());

        println!("inserted {}: {:?}", topic_name, inserted);
    }
}

#[tokio::main]
async fn main() {
    let ss = ServerState::new();

    // println!("ss: {:?}", ss);
    let server_private = Arc::new(Mutex::new(ss));
    let server_state_access: &Arc<Mutex<ServerState>> = &server_private;

    let topic_generic_route = warp::path!("topics" / String)
        .and(warp::path::end())
        .and_then({
            let state = server_state_access.clone();
            move |c| handler_topic_generic(state.clone(), c)
        });
    let topic_generic_route_data = warp::path!("topics" / String / "data" / usize).and_then({
        let state = server_state_access.clone();
        move |c, index| handler_topic_generic_data(state.clone(), c, index)
    });

    let topic_generic_events_route = warp::path!("topics" / String / "events")
        .and(warp::path::end())
        .and_then({
            let statec = server_state_access.clone();
            move |c| check_exists(statec.clone(), c)
        })
        .and(warp::ws())
        .map({
            let state1 = server_state_access.clone();
            move |c: String, ws: warp::ws::Ws| {
                let state2 = state1.clone();

                ws.on_upgrade(move |socket| handle_websocket_generic(socket, state2.clone(), c))
            }
        });

    let root_route = warp::path::end().and_then({
        let state = server_state_access.clone();
        move || root_handler(state.clone())
    });

    let routes = topic_generic_events_route
        .or(topic_generic_route)
        .or(root_route)
        .or(topic_generic_route_data);

    let server_state_access_ = server_state_access.clone();

    spawn(clock_go(server_state_access_.clone(), "clock5", 5.0));
    spawn(clock_go(server_state_access_.clone(), "clock7", 7.0));
    spawn(clock_go(server_state_access_.clone(), "clock11", 11.0));

    warp::serve(routes).run(([127, 0, 0, 1], 8000)).await;
}

async fn check_exists(
    ss_mutex: Arc<Mutex<ServerState>>,
    topic_name: String,
) -> Result<String, warp::Rejection>
// -> Result<impl warp::Reply, warp::Rejection>
{
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

        let last = oq.sequence.last().unwrap();

        let dr = DataReady {
            sequence: last.index,
            digest: last.digest.clone(),
            urls: vec![format!("../data/{}", last.index)],
        };

        let message = warp::ws::Message::text(serde_json::to_string(&dr).unwrap());
        ws_tx.send(message).await.unwrap();

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
                let dr2 = DataReady {
                    sequence: this_one.index,
                    digest: this_one.digest.clone(),
                    urls: vec![format!("../data/{}", this_one.index)],
                };

                let message = warp::ws::Message::text(serde_json::to_string(&dr2).unwrap());
                match ws_tx.send(message).await {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Error sending message: {}", e);
                        continue;
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
    let seq: usize;
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
        seq = last.index;
    }
    return handler_topic_generic_data(ss_mutex.clone(), topic_name, seq).await;
}

async fn handler_topic_generic_data(
    ss_mutex: Arc<Mutex<ServerState>>,
    topic_name: String,
    index: usize,
) -> Result<warp::reply::Response, Rejection> {
    let ss = ss_mutex.lock().await;

    let x: &ObjectQueue;
    match ss.oqs.get(topic_name.as_str()) {
        None => {
            return Err(warp::reject::not_found());
        }
        Some(y) => x = y,
    }

    let data = x.data.get(&index).unwrap();
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

    Ok(resp.into())
}
