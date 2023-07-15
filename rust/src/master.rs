use std::collections::{BTreeMap, HashMap};
use std::string::ToString;
use std::sync::Arc;

use futures::StreamExt;
use log::debug;
use maplit::hashmap;
use maud::html;
use serde_yaml;
use tokio::sync::Mutex;
use tungstenite::http::{HeaderMap, HeaderValue, StatusCode};
use warp::http::header;
use warp::hyper::Body;
use warp::reply::Response;

use crate::cbor_manipulation::{display_printable, get_as_cbor, get_inside};
use crate::html_utils::make_html;
use crate::signals_logic::{interpret_path, TypeOFSource};
use crate::utils::{divide_in_components, get_good_url_for_components, is_prefix_of};
use crate::{
    get_accept_header, handle_websocket_generic, handler_topic_generic, HandlersResponse,
    ServerState, TopicName, TopicRefWire, TopicsIndexWire, HEADER_SEE_EVENTS,
    HEADER_SEE_EVENTS_INLINE_DATA,
};

pub async fn serve_master(
    path: warp::path::FullPath,
    query: HashMap<String, String>,
    ss_mutex: Arc<Mutex<ServerState>>,
    headers: HeaderMap,
) -> HandlersResponse {
    log::debug!("serve_master: path={:?} ", path);
    log::debug!("serve_master: query={:?} ", query);
    let dereference = query.contains_key("dereference");
    // get the components of the path

    let path_components = divide_in_components(path.as_str(), '/');
    let good_url = get_good_url_for_components(&path_components);
    if good_url != path.as_str() {
        let good_relative_url = format!("./{}/", path_components.last().unwrap());

        debug!(
            "Redirecting {:?} to {:?}; rel = {:?}",
            path.as_str(),
            good_url,
            good_relative_url
        );

        let text_response = format!("Redirecting to {}", good_url);
        let res = http::Response::builder()
            .status(StatusCode::MOVED_PERMANENTLY)
            .header(header::LOCATION, good_relative_url)
            .body(Body::from(text_response))
            .unwrap();

        return Ok(res);
    }

    let matched = interpret_path(&path_components, ss_mutex.clone()).await;
    debug!("serve_master: matched: {:?}", matched);
    let ds = match matched {
        Ok(ds) => ds,
        Err(s) => {
            let s = format!("path_components: {:?}\n{}", path_components, s);
            let res = http::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(s))
                .unwrap();
            return Ok(res);
        }
    };

    log::debug!("serve_master: path_components={:?} ", path_components);

    // let first = path_components.remove(0);
    let keys: Vec<String> = {
        let ss = ss_mutex.lock().await;
        ss.oqs.keys().cloned().collect()
    };
    log::debug!(" = path_components = {:?} ", path_components);
    let mut subtopics: Vec<(String, Vec<String>, Vec<String>)> = vec![];
    for k in keys {
        let k2 = k.clone();
        let components: Vec<String> = k2
            .split('.')
            .map(|x| x.to_string())
            .collect::<Vec<String>>();
        if components.len() == 0 {
            continue;
        }
        match is_prefix_of(&components, &path_components) {
            None => {}
            Some((m1, m2)) => {
                return go_queue(k.clone(), m2, ss_mutex, headers).await;
            }
        };

        log::debug!("k: {:?} = components = {:?} ", k, components);

        let (matched, rest) = match is_prefix_of(&path_components, &components) {
            None => continue,

            Some(rest) => rest,
        };

        subtopics.push((k.clone(), matched, rest));
    }

    eprintln!("subtopics: {:?}", subtopics);
    if subtopics.len() == 0 {
        return Err(warp::reject::not_found());
    }
    let cbor_bytes = if !dereference {
        let ss = ss_mutex.lock().await;
        let mut topics: HashMap<TopicName, TopicRefWire> = hashmap! {};

        for (topic_name, p1, prefix) in subtopics {
            let oq = ss.oqs.get(&topic_name).unwrap();
            let rel = prefix.join("/");

            let rurl = format!("{}/", rel);

            let dotsep = prefix.join(".");
            topics.insert(dotsep, oq.tr.to_wire(Some(rurl)));
        }

        let index = TopicsIndexWire {
            node_id: ss.node_id.clone(),
            node_started: ss.node_started,
            node_app_data: ss.node_app_data.clone(),
            topics,
        };
        //
        // return topics_index;
        //
        //     let index = topics_index(&ss);
        let cbor_bytes = serde_cbor::to_vec(&index).unwrap();
        cbor_bytes
    } else {
        let ss = ss_mutex.lock().await;

        // let mut result_dict = BTreeMap::new();
        let mut result_dict: serde_cbor::value::Value =
            serde_cbor::value::Value::Map(BTreeMap::new());
        for (k, p1, prefix) in subtopics {
            let mut the_result_to_put: &mut serde_cbor::value::Value = {
                let mut current: &mut serde_cbor::value::Value = &mut result_dict;
                for component in &prefix[..prefix.len() - 1] {
                    if let serde_cbor::value::Value::Map(inside) = current {
                        let the_key = serde_cbor::value::Value::Text(component.clone().into());
                        if !inside.contains_key(&the_key) {
                            inside.insert(
                                the_key.clone(),
                                serde_cbor::value::Value::Map(BTreeMap::new()),
                            );
                        }
                        current = inside.get_mut(&the_key).unwrap();
                    } else {
                        panic!("not a map");
                    }
                }
                current
            };

            let where_to_put =
                if let serde_cbor::value::Value::Map(where_to_put) = &mut the_result_to_put {
                    where_to_put
                } else {
                    panic!("not a map");
                };
            let key_to_put = serde_cbor::value::Value::Text(prefix.last().unwrap().clone().into());

            let oq = ss.oqs.get(&k).unwrap();
            match oq.sequence.last() {
                None => {
                    where_to_put.insert(key_to_put, serde_cbor::value::Value::Null);
                }
                Some(v) => {
                    let data = match oq.data.get(&v.digest) {
                        None => return Err(warp::reject::not_found()),
                        Some(y) => y,
                    };
                    let cbor_data = get_as_cbor(data);

                    where_to_put.insert(key_to_put, cbor_data);
                }
            }
        }

        // let cbor = serde_cbor::value::Value::Map(result_dict);

        let cbor_bytes: Vec<u8> = serde_cbor::to_vec(&result_dict).unwrap();
        // let as_yaml = serde_yaml::to_string(&cbor).unwrap();
        cbor_bytes
    };

    return visualize_data(
        "title".to_string(),
        "application/cbor",
        &cbor_bytes,
        headers,
    )
    .await;
}

pub async fn go_queue(
    topic_name: String,
    components: Vec<String>,
    ss_mutex: Arc<Mutex<ServerState>>,
    headers: HeaderMap,
) -> HandlersResponse {
    if components.len() == 0 {
        return handler_topic_generic(topic_name, ss_mutex, headers).await;
    }
    let ss = ss_mutex.lock().await;
    let oq = match ss.oqs.get(&topic_name) {
        None => return Err(warp::reject::not_found()),
        Some(x) => x,
    };

    let digest = match oq.sequence.last() {
        None => {
            let mut res = http::Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::from(""))
                .unwrap();
            let h = res.headers_mut();

            // let suffix = format!("topics/{}/", topic_name);
            // put_alternative_locations(&ss, h, &suffix);
            // put_common_headers(&ss, h);

            return Ok(res);
        }

        Some(y) => y.digest.clone(),
    };

    let data = match oq.data.get(&digest) {
        None => return Err(warp::reject::not_found()),
        Some(x) => x,
    };

    let c = get_as_cbor(data);

    let x = get_inside(vec![], &c, &components);
    return match x {
        Err(s) => {
            debug!("Error: {}", s);
            Err(warp::reject::not_found())
        }
        Ok(q) => {
            let cbor_bytes: Vec<u8> = serde_cbor::to_vec(&q).unwrap();
            visualize_data(
                "title".to_string(),
                "application/cbor",
                &cbor_bytes,
                headers,
            )
            .await
        }
    };
    //
    // eprintln!(
    //     "go_queue: topic_name = {:?} components = {:?}",
    //     topic_name, components
    // );
    // return Err(warp::reject::not_found());
}

pub async fn visualize_data(
    title: String,
    content_type: &str,
    content: &[u8],
    headers: HeaderMap,
) -> HandlersResponse {
    let accept_headers: Vec<String> = get_accept_header(&headers);
    let display = display_printable(content_type, content);
    if accept_headers.contains(&"text/html".to_string()) {
        let x = make_html(
            &title,
            html! {

                 p { "This data is presented as HTML because you requested it as such."}
                         p { "Content type: " code { (content_type) } }
                         p { "Content length: " (content.len()) }

                pre {
                    code {
                                 (display)
                    }
                }
            },
        );

        let markup = x.into_string();
        let mut resp = Response::new(Body::from(markup));
        let headers = resp.headers_mut();

        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/html"));

        // put_alternative_locations(&ss, headers, "");
        return Ok(resp);
        // return Ok(with_status(resp, StatusCode::OK));
    };

    let mut resp = Response::new(Body::from(content.to_vec()));
    let h = resp.headers_mut();
    // let suffix = format!("topics/{}/", topic_name);
    // put_alternative_locations(&ss, h, &suffix);
    //
    // h.insert(
    //     HEADER_DATA_ORIGIN_NODE_ID,
    //     HeaderValue::from_str(x.tr.origin_node.as_str()).unwrap(),
    // );
    // h.insert(
    //     HEADER_DATA_UNIQUE_ID,
    //     HeaderValue::from_str(x.tr.unique_id.as_str()).unwrap(),
    // );

    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(content_type).unwrap(),
    );
    // see events
    h.insert(HEADER_SEE_EVENTS, HeaderValue::from_static("events/"));
    h.insert(
        HEADER_SEE_EVENTS_INLINE_DATA,
        HeaderValue::from_static("events/?send_data=1"),
    );

    // put_common_headers(&ss, resp.headers_mut());

    Ok(resp.into())
}

pub async fn handle_websocket_generic2(
    path: warp::path::FullPath,
    ws: warp::ws::WebSocket,
    state: Arc<Mutex<ServerState>>,
    // topic_name: String,
    send_data: bool,
) -> () {
    // remove the last "events"
    let components = divide_in_components(path.as_str(), '/');
    // remove last onw
    let components = components[0..components.len() - 1].to_vec();
    debug!("handle_websocket_generic2: {:?}", components);
    let matched = interpret_path(&components, state.clone()).await;
    debug!("handle_websocket_generic2: matched: {:?}", matched);

    let ds = match matched {
        Ok(ds) => ds,
        Err(_) => {
            ws.close().await.unwrap();
            /// how to send error?
            return;
        }
    };

    match ds {
        TypeOFSource::OurQueue(topic_name) => {
            return handle_websocket_generic(ws, state, topic_name, send_data).await;
        }
        TypeOFSource::Compose(sc) => {
            panic!("not implemented");
        }
        TypeOFSource::NodeRoot => {
            panic!("not implemented");
        }
        TypeOFSource::ForwardedQueue(_) => {
            panic!("not implemented");
        }
        TypeOFSource::Transformed(_, _) => {
            panic!("not implemented");
        }
    }

    //
    // // debug!("handle_websocket_generic: {}", topic_name);
    // let (mut ws_tx, mut ws_rx) = ws.split();
    // let channel_info_message: MsgServerToClient;
    // let mut rx2: Receiver<usize>;
    // {
    //     // important: release the lock
    //     let ss0 = state.lock().await;
    //
    //     let oq: &ObjectQueue;
    //     match ss0.oqs.get(&topic_name) {
    //         None => {
    //             // TODO: we shouldn't be here
    //             ws_tx.close().await.unwrap();
    //             return;
    //         }
    //         Some(y) => oq = y,
    //     }
    //     match oq.sequence.last() {
    //         None => {}
    //         Some(last) => {
    //             let the_availability = vec![ResourceAvailabilityWire {
    //                 url: format!("../data/{}/", last.digest),
    //                 available_until: epoch() + 60.0,
    //             }];
    //             let nchunks = if send_data { 1 } else { 0 };
    //             let dr = DataReady {
    //                 unique_id: oq.tr.unique_id.clone(),
    //                 origin_node: oq.tr.origin_node.clone(),
    //                 sequence: last.index,
    //                 time_inserted: last.time_inserted,
    //                 digest: last.digest.clone(),
    //                 content_type: last.content_type.clone(),
    //                 content_length: last.content_length.clone(),
    //                 clocks: last.clocks.clone(),
    //                 availability: the_availability,
    //                 chunks_arriving: nchunks,
    //             };
    //             let m = MsgServerToClient::DataReady(dr);
    //             let message = warp::ws::Message::binary(serde_cbor::to_vec(&m).unwrap());
    //             ws_tx.send(message).await.unwrap();
    //
    //             if send_data {
    //                 let message_data = oq.data.get(&last.digest).unwrap();
    //                 let message = warp::ws::Message::binary(message_data.content.clone());
    //                 ws_tx.send(message).await.unwrap();
    //             }
    //         }
    //     }
    //
    //     let c = ChannelInfo {
    //         queue_created: 0,
    //         num_total: 0,
    //         newest: None,
    //         oldest: None,
    //     };
    //     channel_info_message = MsgServerToClient::ChannelInfo(c);
    //
    //     rx2 = oq.tx.subscribe();
    // }
    // let state2_for_receive = state.clone();
    // let topic_name2 = topic_name.clone();
    //
    // tokio::spawn(async move {
    //     let topic_name = topic_name2.clone();
    //     loop {
    //         match ws_rx.next().await {
    //             None => {
    //                 // debug!("ws_rx.next() returned None");
    //                 // finished = true;
    //                 break;
    //             }
    //             Some(Ok(msg)) => {
    //                 if msg.is_binary() {
    //                     let raw_data = msg.clone().into_bytes();
    //                     // let v: serde_cbor::Value = serde_cbor::from_slice(&raw_data).unwrap();
    //                     //
    //                     // debug!("ws_rx.next() returned {:#?}", v);
    //                     //
    //                     let ms: MsgClientToServer = match serde_cbor::from_slice(&raw_data) {
    //                         Ok(x) => x,
    //                         Err(err) => {
    //                             debug!("ws_rx.next() cannot nterpret error {:#?}", err);
    //                             continue;
    //                         }
    //                     };
    //                     debug!("ws_rx.next() returned {:#?}", ms);
    //                     match ms {
    //                         MsgClientToServer::RawData(rd) => {
    //                             let mut ss0 = state2_for_receive.lock().await;
    //
    //                             // let oq: &ObjectQueue=  ss0.oqs.get(topic_name.as_str()).unwrap();
    //
    //                             let _ds =
    //                                 ss0.publish(&topic_name, &rd.content, &rd.content_type, None);
    //                         }
    //                     }
    //                 }
    //             }
    //             Some(Err(err)) => {
    //                 debug!("ws_rx.next() returned error {:#?}", err);
    //                 // match err {
    //                 //     Error { .. } => {}
    //                 // }
    //             }
    //         }
    //     }
    // });
    // let message = warp::ws::Message::binary(serde_cbor::to_vec(&channel_info_message).unwrap());
    // match ws_tx.send(message).await {
    //     Ok(_) => {}
    //     Err(e) => {
    //         debug!("Error sending ChannelInfo message: {}", e);
    //     }
    // }
    //
    // // now wait for one message at least
    //
    // loop {
    //     let r = rx2.recv().await;
    //     let message;
    //     match r {
    //         Ok(_message) => message = _message,
    //         Err(RecvError::Closed) => break,
    //         Err(RecvError::Lagged(_)) => {
    //             debug!("Lagged!");
    //             continue;
    //         }
    //     }
    //
    //     let ss2 = state.lock().await;
    //     let oq2 = ss2.oqs.get(&topic_name).unwrap();
    //     let this_one: &DataSaved = oq2.sequence.get(message).unwrap();
    //     let the_availability = vec![ResourceAvailabilityWire {
    //         url: format!("../data/{}/", this_one.digest),
    //         available_until: epoch() + 60.0,
    //     }];
    //     let nchunks = if send_data { 1 } else { 0 };
    //     let dr2 = DataReady {
    //         origin_node: oq2.tr.origin_node.clone(),
    //         unique_id: oq2.tr.unique_id.clone(),
    //         sequence: this_one.index,
    //         time_inserted: this_one.time_inserted,
    //         digest: this_one.digest.clone(),
    //         content_type: this_one.content_type.clone(),
    //         content_length: this_one.content_length,
    //         clocks: this_one.clocks.clone(),
    //         availability: the_availability,
    //         chunks_arriving: nchunks,
    //     };
    //
    //     let out_message: MsgServerToClient = MsgServerToClient::DataReady(dr2);
    //
    //     let message = warp::ws::Message::binary(serde_cbor::to_vec(&out_message).unwrap());
    //     match ws_tx.send(message).await {
    //         Ok(_) => {}
    //         Err(e) => {
    //             debug!("Error sending DataReady message: {}", e);
    //             break; // TODO: do better handling
    //         }
    //     }
    //     if send_data {
    //         let message_data = oq2.data.get(&this_one.digest).unwrap();
    //         let message = warp::ws::Message::binary(message_data.content.clone());
    //         match ws_tx.send(message).await {
    //             Ok(_) => {}
    //             Err(e) => {
    //                 debug!("Error sending binary data message: {}", e);
    //                 break; // TODO: do better handling
    //             }
    //         }
    //     }
    // }
    // debug!("handle_websocket_generic: {} - done", topic_name);
}
