use std::collections::HashMap;
use std::sync::Arc;

use chrono::prelude::*;
use futures::{SinkExt, StreamExt};
use maplit::hashmap;
use serde_json;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;
use warp::Filter;
use warp::http::header;
use warp::hyper::Body;
use warp::reply::Response;

mod structures;
mod constants;
mod object_queues;
mod server_state;
mod types;


use structures::*;
use object_queues::*;
use server_state::*;
use types::*;
use constants::*;

#[tokio::main]
async fn main() {


    let ss = ServerState::new();

    println!("ss: {:?}", ss);
    let server_private = Arc::new(Mutex::new(ss));
    let server_state_access: &Arc<Mutex<ServerState>> = &server_private;




    let server_state_access_ = server_state_access.clone();
    tokio::spawn(
        async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(3));

            let topic_name = "clock";

            loop {
                interval.tick().await;
                let mut ss = server_state_access_.lock().await;
                let current_datetime = Local::now();
                let datetime_string = current_datetime.to_rfc3339();

                let inserted = ss.push(topic_name, &RawData {
                    content: datetime_string.as_bytes().to_vec(),
                    content_type: "text/plain".to_string(),
                });

                println!("inserted: {:?}", inserted);
            }
        });


    let topic_generic_route = warp::path!("topics" / String ).and(warp::path::end()).and_then({
        let state = server_state_access.clone();
        move |c| handler_topic_generic(state.clone(), c)
    });
    let topic_generic_route_data = warp::path!("topics" / String / "data"/ usize ).and_then({
        let state = server_state_access.clone();
        move |c, index| handler_topic_generic_data(state.clone(), c, index)
    });

    let topic_generic_events_route = warp::path!("topics" / String / "events")
        .and(warp::path::end())
        .and(warp::ws())
        .map({
            let state1 = server_state_access.clone();
            move |c: String, ws: warp::ws::Ws, | {
                let state2 = state1.clone();
                ws.on_upgrade(move |socket| handle_connection_generic(socket, state2.clone(), c))
            }
        });

    let root_route = warp::path::end().and_then({
        let state = server_state_access.clone();
        move || root_handler(state.clone())
    });


    let routes = topic_generic_events_route.or(topic_generic_route).or(root_route).or(topic_generic_route_data);


    warp::serve(routes).run(([127, 0, 0, 1], 8000)).await;
}

async fn root_handler(ss_mutex:
                      Arc<Mutex<ServerState>>) -> Result<impl warp::Reply, warp::Rejection> {
    let ss = ss_mutex.lock().await;
    let index = topics_index(&ss);
    Ok(warp::reply::json(&index))
}


async fn handle_connection_generic(ws: warp::ws::WebSocket, state: Arc<Mutex<ServerState>>,
                                   topic_name: String) {
    let (mut ws_tx, _ws_rx) = ws.split();


    let mut rx2: Receiver<usize>;
    { // important: release the lock
        let ss0 = state.lock().await;

        let oq = ss0.oqs.get(&topic_name).unwrap();
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
                println!("Received update for topic {}: available index {}", message, message);
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
                        continue
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


async fn handler_topic_generic(ss_mutex:
                               Arc<Mutex<ServerState>>, topic_name: String) -> Result<impl warp::Reply, warp::Rejection> {
    let seq: usize;
    {
        let ss = ss_mutex.lock().await;

        let x: &ObjectQueue = ss.oqs.get(topic_name.as_str()).unwrap();
        // get the last element in the vector
        let last = x.sequence.last().unwrap();
        seq = last.index;
    }
    return handler_topic_generic_data(ss_mutex.clone(), topic_name, seq).await;


}


async fn handler_topic_generic_data(ss_mutex:
                                    Arc<Mutex<ServerState>>, topic_name: String,
                                    index: usize) -> Result<impl warp::Reply, warp::Rejection> {
    let ss = ss_mutex.lock().await;

    let x: &ObjectQueue = ss.oqs.get(topic_name.as_str()).unwrap();


    let data = x.data.get(&index).unwrap();
    let data_bytes = data.content.clone();
    let content_type = data.content_type.clone();

    let mut reply = Response::new(Body::from(data_bytes));


    reply.headers_mut().insert(
        HEADER_NODE_ID,
        header::HeaderValue::from_str(ss.node_id.as_str()).unwrap(),
    );
    reply.headers_mut().insert(
        HEADER_DATA_ORIGIN_NODE_ID,
        header::HeaderValue::from_str(x.tr.origin_node.as_str()).unwrap(),
    );
    reply.headers_mut().insert(
        HEADER_DATA_UNIQUE_ID,
        header::HeaderValue::from_str(x.tr.unique_id.as_str()).unwrap(),
    );

    reply.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_str(content_type.clone().as_str()).unwrap(),
    );
    reply.headers_mut().insert(
        HEADER_SEE_EVENTS,
        header::HeaderValue::from_static("events/"),
    );

    Ok::<_, warp::Rejection>(reply)
}
