use std::collections::HashMap;
use std::string::ToString;
use std::sync::Arc;

use bytes::Bytes;
use log::debug;
use maud::{html, PreEscaped};
use serde_yaml;
use tokio::sync::Mutex;
use tungstenite::http::{HeaderMap, HeaderValue, StatusCode};
use warp::http::header;
use warp::hyper::Body;
use warp::reply::Response;

use crate::cbor_manipulation::{display_printable, get_as_cbor, get_inside};
use crate::html_utils::make_html;
use crate::signals_logic::{
    interpret_path, DataProps, ResolveDataSingle, ResolvedData, TopicProperties, TypeOFSource,
};
use crate::utils::{divide_in_components, get_good_url_for_components};
use crate::{
    get_accept_header, handle_websocket_generic, handler_topic_generic, object_queues,
    root_handler, serve_static_file_path, HandlersResponse, RawData, ServerState,
    HEADER_SEE_EVENTS, HEADER_SEE_EVENTS_INLINE_DATA, JAVASCRIPT_SEND,
};

pub async fn serve_master_post(
    path: warp::path::FullPath,
    query: HashMap<String, String>,
    ss_mutex: Arc<Mutex<ServerState>>,
    headers: HeaderMap,
) -> HandlersResponse {
    todo!()
}

pub async fn serve_master_head(
    path: warp::path::FullPath,
    query: HashMap<String, String>,
    ss_mutex: Arc<Mutex<ServerState>>,
    headers: HeaderMap,
) -> HandlersResponse {
    todo!()
}

pub async fn serve_master_get(
    path: warp::path::FullPath,
    query: HashMap<String, String>,
    ss_mutex: Arc<Mutex<ServerState>>,
    headers: HeaderMap,
) -> HandlersResponse {
    let path_str = path.as_str();
    let pat = "/!/";

    // if path_str.starts_with(pat)  {
    //     // only preserve the part after the !/
    //     let new_path_str = &path_str[pat.len()..];
    //
    //     let text_response = format!("Redirecting to {:?} -> {:?} ", path_str, new_path_str);
    //
    //     debug!("serve_master: removing initial /!/ directly ={:?} => {:?} ", path, new_path_str);
    //
    //     let res = http::Response::builder()
    //         .status(StatusCode::MOVED_PERMANENTLY)
    //         .header(header::LOCATION, new_path_str)
    //         .body(Body::from(text_response))
    //         .unwrap();
    //     return Ok(res);
    // }

    let new_path = if path_str.contains(pat) {
        // only preserve the part after the !/

        let index = path_str.rfind(pat).unwrap();
        let path_str2 = &path_str[index + pat.len() - 1..];

        let res = path_str2.to_string().replace("/!!", "/!");

        debug!(
            "serve_master: normalizing /!/:\n   {:?}\n-> {:?} ",
            path, res
        );
        res
    } else {
        path_str.to_string()
    };

    if path_str != new_path {
        debug!("serve_master: path={:?} => {:?} ", path, new_path);
    }
    return serve_master0(&new_path, query, ss_mutex, headers).await;
}

pub async fn serve_master0(
    path_str: &str,
    query: HashMap<String, String>,
    ss_mutex: Arc<Mutex<ServerState>>,
    headers: HeaderMap,
) -> HandlersResponse {
    if path_str.len() > 250 {
        panic!("Path too long: {:?}", path_str);
    }

    // log::debug!("serve_master: path={:?} ", path);
    // log::debug!("serve_master: query={:?} ", query);
    let dereference = query.contains_key("dereference");
    // get the components of the path

    let path_components0 = divide_in_components(path_str, '/');
    let path_components = path_components0.clone();
    // // if there is a ! in the path_components, ignore all the PREVIOUS items
    // let bang = "!".to_string();
    // if path_components.contains(&bang) {
    //     let index = path_components.iter().position(|x| x == &bang).unwrap();
    //     let rest = path_components.iter().skip(index + 1).cloned().collect::<Vec<String>>();
    //     path_components = rest;
    // }

    let STATIC_PREFIX = "static".to_string();

    match path_components.first() {
        None => {}
        Some(first) => {
            if first == &STATIC_PREFIX {
                let pathstr = path_str;

                // find "static/" in path and give the rest
                let index = pathstr.find("static/").unwrap();
                let path = &pathstr[index + "static/".len()..];

                return serve_static_file_path(path).await;
            }
        }
    }

    //     let path_components = path_components.iter().skip(1).cloned().collect::<Vec<String>>();
    //     let path = path_components.join("/");
    //     let path = format!("static/{}", path);
    //     let res = warp::reply::with_header(
    //         warp::reply::with_header(
    //             warp::fs::file(path),
    //             "Content-Type",
    //             "text/html; charset=utf-8",
    //         ),
    //         "Cache-Control",
    //         "no-cache",
    //     );
    //     return Ok(res);
    // }

    if !path_components.contains(&STATIC_PREFIX) {
        let good_url = get_good_url_for_components(&path_components);
        if good_url != path_str {
            let good_relative_url = format!("./{}/", path_components.last().unwrap());

            debug!(
                "Redirecting\n - {}\n->  {};\n rel = {}\n",
                path_str, good_url, good_relative_url
            );

            let text_response = format!("Redirecting to {}", good_url);
            let res = http::Response::builder()
                .status(StatusCode::MOVED_PERMANENTLY)
                .header(header::LOCATION, good_relative_url)
                .body(Body::from(text_response))
                .unwrap();

            // return Ok(res);
        }
    }

    let matched = interpret_path(&path_components, ss_mutex.clone()).await;
    debug!(
        "serve_master:\npaths: {:#?}\nmatched: {:#?}",
        path_components, matched
    );
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

    let ds_props = ds.get_properties();

    match ds {
        TypeOFSource::OurQueue(topic_name, _, _) => {
            return if topic_name != "" {
                handler_topic_generic(topic_name, ss_mutex.clone(), headers).await
            } else {
                root_handler(ss_mutex.clone(), headers).await
            };
        }
        _ => {}
    }
    let resd0 = ds.resolve_data_single(ss_mutex.clone()).await;

    let resd = match resd0 {
        Ok(x) => x,
        Err(s) => {
            let res = http::Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(s))
                .unwrap();
            return Ok(res);
        }
    };
    // debug!("serve_master: resolved: {:#?}", resd);
    let rd: object_queues::RawData = match resd {
        ResolvedData::RawData(rd) => rd,
        ResolvedData::Regular(reg) => {
            let cbor_bytes = serde_cbor::to_vec(&reg).unwrap();
            let bytes = Bytes::from(cbor_bytes);
            object_queues::RawData::new(bytes, "application/cbor".to_string())
        }
        ResolvedData::NotAvailableYet(s) => {
            let res = http::Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::from(s))
                .unwrap();
            return Ok(res);
        }
        ResolvedData::NotFound(s) => {
            let res = http::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(s))
                .unwrap();
            return Ok(res);
        }
    };
    return visualize_data(
        &ds_props,
        path_str.to_string(),
        &rd.content_type,
        &rd.content,
        headers,
    )
    .await;

    // let ss = ss_mutex.lock().await;
    //         let mut topics: HashMap<TopicName, TopicRefWire> = hashmap! {};
    //
    //         for (topic_name, p1, prefix) in subtopics {
    //             let oq = ss.oqs.get(&topic_name).unwrap();
    //             let rel = prefix.join("/");
    //
    //             let rurl = format!("{}/", rel);
    //
    //             let dotsep = prefix.join(".");
    //             topics.insert(dotsep, oq.tr.to_wire(Some(rurl)));
    //         }
    //
    //         let index = TopicsIndexWire {
    //             node_id: ss.node_id.clone(),
    //             node_started: ss.node_started,
    //             node_app_data: ss.node_app_data.clone(),
    //             topics,
    //         };
    //         //
    //         // return topics_index;
    //         //
    //         //     let index = topics_index(&ss);
    //         let cbor_bytes = serde_cbor::to_vec(&index).unwrap();
    //         cbor_bytes
    //
    //     log::debug!("serve_master: rd={:?} ", rd);
    //
    //     log::debug!("serve_master: path_components={:?} ", path_components);
    //
    //     // let first = path_components.remove(0);
    //     let keys: Vec<String> = {
    //         let ss = ss_mutex.lock().await;
    //         ss.oqs.keys().cloned().collect()
    //     };
    //     log::debug!(" = path_components = {:?} ", path_components);
    //     let mut subtopics: Vec<(String, Vec<String>, Vec<String>)> = vec![];
    //     for k in keys {
    //         let k2 = k.clone();
    //         let components: Vec<String> = k2
    //             .split('.')
    //             .map(|x| x.to_string())
    //             .collect::<Vec<String>>();
    //         if components.len() == 0 {
    //             continue;
    //         }
    //         match is_prefix_of(&components, &path_components) {
    //             None => {}
    //             Some((m1, m2)) => {
    //                 return go_queue(k.clone(), m2, ss_mutex, headers).await;
    //             }
    //         };
    //
    //         log::debug!("k: {:?} = components = {:?} ", k, components);
    //
    //         let (matched, rest) = match is_prefix_of(&path_components, &components) {
    //             None => continue,
    //
    //             Some(rest) => rest,
    //         };
    //
    //         subtopics.push((k.clone(), matched, rest));
    //     }
    //
    //     eprintln!("subtopics: {:?}", subtopics);
    //     if subtopics.len() == 0 {
    //         return Err(warp::reject::not_found());
    //     }
    //     let cbor_bytes = if !dereference {
    //         let ss = ss_mutex.lock().await;
    //         let mut topics: HashMap<TopicName, TopicRefWire> = hashmap! {};
    //
    //         for (topic_name, p1, prefix) in subtopics {
    //             let oq = ss.oqs.get(&topic_name).unwrap();
    //             let rel = prefix.join("/");
    //
    //             let rurl = format!("{}/", rel);
    //
    //             let dotsep = prefix.join(".");
    //             topics.insert(dotsep, oq.tr.to_wire(Some(rurl)));
    //         }
    //
    //         let index = TopicsIndexWire {
    //             node_id: ss.node_id.clone(),
    //             node_started: ss.node_started,
    //             node_app_data: ss.node_app_data.clone(),
    //             topics,
    //         };
    //         //
    //         // return topics_index;
    //         //
    //         //     let index = topics_index(&ss);
    //         let cbor_bytes = serde_cbor::to_vec(&index).unwrap();
    //         cbor_bytes
    //     } else {
    //         let ss = ss_mutex.lock().await;
    //
    //         // let mut result_dict = BTreeMap::new();
    //         let mut result_dict: serde_cbor::value::Value =
    //             serde_cbor::value::Value::Map(BTreeMap::new());
    //         for (k, p1, prefix) in subtopics {
    //             let mut the_result_to_put: &mut serde_cbor::value::Value = {
    //                 let mut current: &mut serde_cbor::value::Value = &mut result_dict;
    //                 for component in &prefix[..prefix.len() - 1] {
    //                     if let serde_cbor::value::Value::Map(inside) = current {
    //                         let the_key = serde_cbor::value::Value::Text(component.clone().into());
    //                         if !inside.contains_key(&the_key) {
    //                             inside.insert(
    //                                 the_key.clone(),
    //                                 serde_cbor::value::Value::Map(BTreeMap::new()),
    //                             );
    //                         }
    //                         current = inside.get_mut(&the_key).unwrap();
    //                     } else {
    //                         panic!("not a map");
    //                     }
    //                 }
    //                 current
    //             };
    //
    //             let where_to_put =
    //                 if let serde_cbor::value::Value::Map(where_to_put) = &mut the_result_to_put {
    //                     where_to_put
    //                 } else {
    //                     panic!("not a map");
    //                 };
    //             let key_to_put = serde_cbor::value::Value::Text(prefix.last().unwrap().clone().into());
    //
    //             let oq = ss.oqs.get(&k).unwrap();
    //             match oq.sequence.last() {
    //                 None => {
    //                     where_to_put.insert(key_to_put, serde_cbor::value::Value::Null);
    //                 }
    //                 Some(v) => {
    //                     let data = match oq.data.get(&v.digest) {
    //                         None => return Err(warp::reject::not_found()),
    //                         Some(y) => y,
    //                     };
    //                     let cbor_data = get_as_cbor(data);
    //
    //                     where_to_put.insert(key_to_put, cbor_data);
    //                 }
    //             }
    //         }
    //
    //         // let cbor = serde_cbor::value::Value::Map(result_dict);
    //
    //         let cbor_bytes: Vec<u8> = serde_cbor::to_vec(&result_dict).unwrap();
    //         // let as_yaml = serde_yaml::to_string(&cbor).unwrap();
    //         cbor_bytes
    //     };
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

    let (content_type, digest) = match oq.sequence.last() {
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

        Some(y) => (y.content_type.clone(), y.digest.clone()),
    };

    let content = match ss.get_blob_bytes(&digest) {
        None => return Err(warp::reject::not_found()),
        Some(x) => x,
    };
    let raw_data = RawData {
        content_type: content_type.clone(),
        content: content,
    };
    let c = get_as_cbor(&raw_data);

    let x = get_inside(vec![], &c, &components);
    return match x {
        Err(s) => {
            debug!("Error: {}", s);
            Err(warp::reject::not_found())
        }
        Ok(q) => {
            let cbor_bytes: Vec<u8> = serde_cbor::to_vec(&q).unwrap();
            let properties = TopicProperties {
                streamable: true,
                pushable: true,
                readable: true,
                immutable: false,
            };
            visualize_data(
                &properties,
                topic_name.to_string(),
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
    properties: &TopicProperties,
    title: String,
    content_type: &str,
    content: &[u8],
    headers: HeaderMap,
) -> HandlersResponse {
    let accept_headers: Vec<String> = get_accept_header(&headers);
    let display = display_printable(content_type, content);
    let default_content_type = "application/yaml";
    let initial_value = "{}";
    let js = PreEscaped(JAVASCRIPT_SEND);
    if accept_headers.contains(&"text/html".to_string()) {
        let x = make_html(
            &title,
            html! {

                 p { "This data is presented as HTML because you requested it as such."}
                         p { "Content type: " code { (content_type) } }
                         p { "Content length: " (content.len()) }



                  div  {

                @if properties.streamable {
                    h3 { "notifications using websockets"}
                    div {pre   id="result" { "result will appear here" }}
                }
            }
            div {
                @if properties.pushable {
                        h3 {"push JSON to queue"}

                        textarea id="myTextAreaContentType" { (default_content_type) };
                        textarea id="myTextArea" { (initial_value) };

                        br;

                        button id="myButton" { "push" };
                }



            } // div
                @if properties.pushable || properties.streamable {

                 script src="../../static/send.js" {};

                script type="text/javascript" { (js) };
                }

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
        TypeOFSource::OurQueue(topic_name, _, _) => {
            return handle_websocket_generic(ws, state, topic_name, send_data).await;
        }
        TypeOFSource::Compose(sc) => {
            panic!("not implemented");
        }
        TypeOFSource::Digest(digest, content_type) => {
            panic!("not implemented");
        }
        TypeOFSource::ForwardedQueue(_) => {
            panic!("not implemented");
        }
        TypeOFSource::Transformed(_, _) => {
            panic!("not implemented");
        }
        TypeOFSource::Deref(_) => {
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
