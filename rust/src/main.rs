use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json;
use tokio::sync::Mutex;
use warp::Filter;
use warp::http::header;
use warp::hyper::Body;
use warp::reply::Response;

use maplit::hashmap;
use structures::*;
use uuid::Uuid;

mod structures;

// define some constants:

static HEADER_SEE_EVENTS: &'static str = "X-dtps-events";

static HEADER_NODE_ID: &'static str = "X-DTPS-Node-ID";
static HEADER_NODE_PASSED_THROUGH: &'static str = "X-DTPS-Node-ID-Passed-Through";
static HEADER_LINK_BENCHMARK: &'static str = "X-DTPS-link-benchmark";
static HEADER_DATA_UNIQUE_ID: &'static str = "X-DTPS-data-unique-id";
static HEADER_DATA_ORIGIN_NODE_ID: &'static str = "X-DTPS-data-origin-node";

#[tokio::main]
async fn main() {
    // let uuid = Uuid::new_v4();
    let node_id: &str = "node-uuid";
    // use function e() from src/lib-dt-dtps-http/rust/src/structures.rs
    // to create a node with a topic list

    let state = Arc::new(Mutex::new(0u32));
    //
    // let state_for_task = Arc::clone(&state);
    // let state_for_route = Arc::clone(&state);

    let state_for_tread = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(3));
        let state_mine = state_for_tread.clone();
        loop {
            interval.tick().await;
            let mut counter = state_mine.lock().await;
            *counter += 1;
            println!("Counter updated: {}", *counter);
        }
    });


    let topic_topiclist_route = warp::path!("topics" / "__topic_list").and(warp::path::end()).and_then({
        let state = state.clone();
        move || handler_topic_topiclist(node_id,
                                        state.clone())
    });

    let topic_topiclist_events_route = warp::path!("topics" / "__topic_list" / "events")
        .and(warp::path::end())
        .and(warp::ws())
        .map({
            let state1 = state.clone();
            move |ws: warp::ws::Ws| {
                let state2 = state1.clone();
                ws.on_upgrade(move |socket| handle_connection(socket, state2.clone()))
            }
        });

    let topic_clock_route = warp::path!("topics" / "clock").and(warp::path::end()).and_then({
        let state = state.clone();
        move || handler_topic_clock(node_id,
                                    state.clone())
    });


    let topic_clock_events_route = warp::path!("topics" / "clock" / "events")
        .and(warp::path::end())
        .and(warp::ws())
        .map({
            let state1 = state.clone();
            move |ws: warp::ws::Ws| {
                let state2 = state1.clone();
                ws.on_upgrade(move |socket| handle_connection(socket, state2.clone()))
            }
        });


    let root_route = warp::path::end().and_then(
        move || root_handler(node_id));


    let routes = topic_topiclist_route.or(topic_topiclist_events_route).or(root_route).or(topic_clock_route).or(topic_clock_events_route);


    warp::serve(routes).run(([127, 0, 0, 1], 8000)).await;
}

async fn root_handler(my_node_id: &str) -> Result<impl warp::Reply, warp::Rejection> {
    let index = topics_index(my_node_id);
    Ok(warp::reply::json(&index))
}



async fn handle_connection(ws: warp::ws::WebSocket, state: Arc<Mutex<u32>>) {
    let (mut ws_tx, mut ws_rx) = ws.split();

    let mut i: i32 = 0;
    loop {
        // let topic = vec!["hi"];
        i +=  1;
        let dr =  DataReady {
            sequence: i,
            digest: "".to_string(),
            urls: vec!["..".to_string()],
        };
        let message = warp::ws::Message::text(serde_json::to_string(&dr).unwrap());
        ws_tx.send(message).await.unwrap();
        // wait 1 second
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
    //
    // while let Some(Ok(_msg)) = ws_rx.next().await {
    //     let val = state.lock().await;
    //     let topic = Topic { sequence: *val };
    //     let message = warp::ws::Message::text(serde_json::to_string(&topic).unwrap());
    //     ws_tx.send(message).await.unwrap();
    // }
}


#[derive(Serialize, Deserialize)]
struct Topic {
    sequence: u32,
}


pub fn topics_index(node_id: &str) -> TopicsIndex {
    // create a uuid
    // let node_id = Uuid::new_v4().to_string();

    let topic_list_id = Uuid::new_v4().to_string();

    let reachability = vec![
        TopicReachability {
            url: "topics/__topic_list/".to_string(),
            answering: String::from(node_id),
            forwarders: vec![],
            benchmark: LinkBenchmark {
                complexity: 0,
                bandwidth: 1000000000.0,
                latency: 0.0,
                reliability: 1.0,
                hops: 0,
            },
        },
    ];

    let topic_list_topic = TopicRef {
        unique_id: topic_list_id,
        origin_node: String::from(node_id),
        app_static_data: None,
        reachability: reachability,
        debug_topic_type: "local".to_string(),
    };

    let topic_clock_id = Uuid::new_v4().to_string();

    let reachability_clock = vec![
        TopicReachability {
            url: "topics/clock/".to_string(),
            answering: String::from(node_id),
            forwarders: vec![],
            benchmark: LinkBenchmark {
                complexity: 0,
                bandwidth: 1000000000.0,
                latency: 0.0,
                reliability: 1.0,
                hops: 0,
            },
        },
    ];
    let topic_list_clock = TopicRef {
        unique_id: topic_clock_id,
        origin_node: String::from(node_id),
        app_static_data: None,
        reachability: reachability_clock,
        debug_topic_type: "local".to_string(),
    };


    let topics = hashmap! {
        "__topic_list".to_string() => topic_list_topic,
        "clock".to_string() => topic_list_clock,
    };


    let topics_index = TopicsIndex {
        node_id: String::from(node_id),
        topics,
    };


    let json = serde_json::to_string(&topics_index).unwrap();

    println!("{}", json);

    return topics_index;
}


async fn handler_topic_clock(my_node_id: &str, state_for_route:
Arc<Mutex<u32>>) -> Result<impl warp::Reply, warp::Rejection> {
    let state_val = state_for_route.lock().await;
    let topic = Topic { sequence: *state_val };
    let json = serde_json::to_string(&topic).unwrap();

    let mut reply = Response::new(Body::from(json));

    // let header_value: Result<header::HeaderValue, _> = my_node_id.into();

    reply.headers_mut().insert(
        HEADER_NODE_ID,
        header::HeaderValue::from_str(&my_node_id).unwrap(),
    );
    reply.headers_mut().insert(
        HEADER_SEE_EVENTS,
        header::HeaderValue::from_static("events/"),
    );

    Ok::<_, warp::Rejection>(reply)
}


async fn handler_topic_topiclist(my_node_id: &str, state_for_route:
Arc<Mutex<u32>>) -> Result<impl warp::Reply, warp::Rejection> {
    // let state_val = state_for_route.lock().await;
    let topics = vec!["__topic_list", "clock"];
    let json = serde_json::to_string(&topics).unwrap();

    let mut reply = Response::new(Body::from(json));

    // let header_value: Result<header::HeaderValue, _> = my_node_id.into();

    reply.headers_mut().insert(
        HEADER_NODE_ID,
        header::HeaderValue::from_str(&my_node_id).unwrap(),
    );
    reply.headers_mut().insert(
        HEADER_SEE_EVENTS,
        header::HeaderValue::from_static("events/"),
    );

    Ok::<_, warp::Rejection>(reply)
}
