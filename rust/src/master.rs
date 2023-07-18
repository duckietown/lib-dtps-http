use std::collections::HashMap;
use std::string::ToString;

use bytes::Bytes;
use futures::stream::{SplitSink, SplitStream};
use futures::SinkExt;
use futures::StreamExt;
use log::{debug, error};
use maud::{html, PreEscaped};
use tokio::spawn;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tungstenite::http::{HeaderMap, HeaderValue, StatusCode};
use warp::http::header;
use warp::hyper::Body;
use warp::reply::Response;
use warp::ws::Message;

use crate::cbor_manipulation::display_printable;
use crate::html_utils::make_html;
use crate::signals_logic::{
    interpret_path, DataProps, GetMeta, ResolveDataSingle, ResolvedData, TopicProperties,
    TypeOFSource,
};
use crate::utils::{divide_in_components, get_good_url_for_components};
use crate::websocket_signals::MsgClientToServer;
use crate::{
    do_receiving, error_with_info, get_accept_header, handle_topic_post, handle_websocket_queue,
    handler_topic_generic, not_implemented, object_queues, put_alternative_locations,
    put_common_headers, receive_from_websocket, root_handler, serve_static_file_path,
    HandlersResponse, ObjectQueue, ServerStateAccess, TopicName, TopicsIndexInternal, DTPSR,
    EVENTS_SUFFIX, EVENTS_SUFFIX_DATA, HEADER_DATA_ORIGIN_NODE_ID, HEADER_DATA_UNIQUE_ID,
    HEADER_SEE_EVENTS, HEADER_SEE_EVENTS_INLINE_DATA, JAVASCRIPT_SEND,
};

//
// fn format_query(params: &HashMap<String, String>) -> Option<String> {
//     if params.is_empty() {
//         return None;
//     }
//     let mut query = String::new();
//
//     for (key, value) in params.iter() {
//         if !query.is_empty() {
//             query.push('&');
//         }
//         query.push_str(&format!("{}={}", key, value));
//     }
//
//     Some(query)
// }

pub async fn serve_master_post(
    path: warp::path::FullPath,
    query: HashMap<String, String>,
    ss_mutex: ServerStateAccess,
    headers: HeaderMap,
    data: hyper::body::Bytes,
) -> HandlersResponse {
    let path_str = path_normalize(path);
    // let query_str = format_query(&query);
    let referrer = get_referrer(&headers);
    let matched = interpret_path(&path_str, &query, &referrer, ss_mutex.clone()).await;

    let ds = match matched {
        Ok(ds) => ds,
        Err(s) => {
            // return Err<s.into()>;
            // let s = format!("Cannot resolve the path {:?}:\n{}", path_components, s);
            let res = http::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(s.to_string()))
                .unwrap();
            return Ok(res);
        }
    };

    let p = ds.get_properties();
    if !p.pushable {
        let res = http::Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::from("Cannot push to this topic"))
            .unwrap();
        return Ok(res);
    }

    match ds {
        TypeOFSource::OurQueue(topic_name, _, _) => {
            debug!("Pushing to topic {:?}", topic_name);
            return handle_topic_post(&topic_name, ss_mutex, headers, data).await;
        }
        _ => {
            let res = http::Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Body::from("We dont support push to this"))
                .unwrap();
            return Ok(res);
        }
    }

    // async fn handle_topic_post(
    //     topic_name: String,
    //     ss_mutex: ServerStateAccess,
    //     headers: HeaderMap,
    //     data: hyper::body::Bytes,
    // ) -> Result<impl Reply, Rejection> {}
    // todo!()
}

pub async fn serve_master_head(
    path: warp::path::FullPath,
    query: HashMap<String, String>,
    ss_mutex: ServerStateAccess,
    headers: HeaderMap,
) -> HandlersResponse {
    let path_str = path_normalize(path);
    // debug!("serve_master_head: path_str: {}", path_str);
    let referrer = get_referrer(&headers);

    let matched = interpret_path(&path_str, &query, &referrer, ss_mutex.clone()).await;
    let ss = ss_mutex.lock().await;

    // debug!(
    //     "serve_master:\npaths: {:#?}\nmatched: {:#?}",
    //     path_components, matched
    // );
    let ds = match matched {
        Ok(ds) => ds,
        Err(s) => {
            // let s = format!("Cannot resolve the path {:?}:\n{}", path_components, s);
            // debug!("serve_master_head: path_str: {}\n{}", path_str, s);

            let res = http::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(s.to_string()))
                .unwrap();
            return Ok(res);
        }
    };
    // debug!("serve_master_head: ds: {:?}", ds);
    let (topic_name, x) = {
        match ds {
            TypeOFSource::OurQueue(topic_name, _, _) => {
                let x: &ObjectQueue;
                match ss.oqs.get(&topic_name) {
                    None => return Err(warp::reject::not_found()),

                    Some(y) => x = y,
                }
                (topic_name, x)
            }
            _ => {
                if path_str == "/" {
                    let tr = TopicName::root();
                    let x = ss.oqs.get(&tr).unwrap();
                    (tr, x)
                } else {
                    let res = http::Response::builder()
                        .status(StatusCode::METHOD_NOT_ALLOWED)
                        .body(Body::from("We dont support push to this"))
                        .unwrap();
                    return Ok(res);
                }
            }
        }
    };

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

    h.insert(HEADER_SEE_EVENTS, HeaderValue::from_static(EVENTS_SUFFIX));
    h.insert(
        HEADER_SEE_EVENTS_INLINE_DATA,
        HeaderValue::from_static(EVENTS_SUFFIX_DATA),
    );

    let suffix = topic_name.as_relative_url();

    put_alternative_locations(&ss, h, &suffix);
    put_common_headers(&ss, h);
    Ok(resp.into())
}

fn path_normalize(path: warp::path::FullPath) -> String {
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

        // debug!(
        //     "serve_master: normalizing /!/:\n   {:?}\n-> {:?} ",
        //     path, res
        // );
        res
    } else {
        path_str.to_string()
    };

    if path_str != new_path {
        // debug!("serve_master: path={:?} => {:?} ", path, new_path);
    }

    new_path
}

const STATIC_PREFIX: &str = "static";

fn get_referrer(headers: &HeaderMap) -> Option<String> {
    let referrer = headers.get(header::REFERER);
    if let Some(x) = referrer {
        let s = x.to_str().unwrap();
        Some(String::from(s))
    } else {
        None
    }
}

pub async fn serve_master_get(
    path: warp::path::FullPath,
    query: HashMap<String, String>,
    ss_mutex: ServerStateAccess,
    headers: HeaderMap,
) -> HandlersResponse {
    // get referrer
    let referrer = get_referrer(&headers);
    debug!("GET {} ", path.as_str());
    debug!("Referrer {:?} ", referrer);

    let path_str = path_normalize(path);

    // debug!("After proxy {} ", path_str);
    // debug!("headers:\n{:?}", headers);
    // let path_str = path_normalize(path);
    if path_str.len() > 250 {
        panic!("Path too long: {:?}", path_str);
    }

    // log::debug!("serve_master: path={:?} ", path);
    // log::debug!("serve_master: query={:?} ", query);
    // let dereference = query.contains_key("dereference");
    // get the components of the path

    let path_components0 = divide_in_components(&path_str, '/');
    let path_components = path_components0.clone();
    // // if there is a ! in the path_components, ignore all the PREVIOUS items
    // let bang = "!".to_string();
    // if path_components.contains(&bang) {
    //     let index = path_components.iter().position(|x| x == &bang).unwrap();
    //     let rest = path_components.iter().skip(index + 1).cloned().collect::<Vec<String>>();
    //     path_components = rest;
    // }

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

    if false {
        if !path_components.contains(&STATIC_PREFIX.to_string()) {
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

                // TODO: renable
                return Ok(res);
            }
        }
    }

    // let ds = context!(
    let ds = interpret_path(&path_str, &query, &referrer, ss_mutex.clone()).await?;

    debug!("serve_master: ds={:?} ", ds);
    //     "Cannot interpret the path {:?}:\n",
    //     path_components,
    // );
    // .map_err(todtpserror)?;

    let ds_props = ds.get_properties();

    match ds {
        TypeOFSource::OurQueue(topic_name, _, _) => {
            return if true || topic_name.is_root() {
                handler_topic_generic(&topic_name, ss_mutex.clone(), headers).await
            } else {
                root_handler(ss_mutex.clone(), headers).await
            };
        }
        _ => {}
    }
    let presented_as = TopicName::from_components(&path_components0);

    let resd0 = ds
        .resolve_data_single(&presented_as, ss_mutex.clone())
        .await;

    let resd = match resd0 {
        Ok(x) => x,
        Err(s) => {
            let res = http::Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(s.to_string()))
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
    let extra_html = match ds {
        TypeOFSource::Compose(_) | TypeOFSource::MountedDir(..) => {
            match ds.get_meta_index(&presented_as, ss_mutex.clone()).await {
                Ok(index) => make_index_html(&index),
                Err(err) => {
                    let s = format!("Cannot make index html: {}", err);
                    html! {p { (s) }}
                }
            }
        }
        _ => {
            html! {}
        }
    };

    return visualize_data(
        &ds_props,
        path_str.to_string(),
        extra_html,
        &rd.content_type,
        &rd.content,
        headers,
    )
    .await;
}
//
// pub async fn go_queue(
//     topic_name: &TopicName,
//     components: Vec<String>,
//     ss_mutex: ServerStateAccess,
//     headers: HeaderMap,
// ) -> HandlersResponse {
//     if components.len() == 0 {
//         return handler_topic_generic(topic_name, ss_mutex, headers).await;
//     }
//     let ss = ss_mutex.lock().await;
//     let oq = match ss.oqs.get(&topic_name) {
//         None => return Err(warp::reject::not_found()),
//         Some(x) => x,
//     };
//
//     let (content_type, digest) = match oq.sequence.last() {
//         None => {
//             let res = http::Response::builder()
//                 .status(StatusCode::NO_CONTENT)
//                 .body(Body::from(""))
//                 .unwrap();
//             // let h = res.headers_mut();
//
//             // let suffix = format!("topics/{}/", topic_name);
//             // put_alternative_locations(&ss, h, &suffix);
//             // put_common_headers(&ss, h);
//
//             return Ok(res);
//         }
//
//         Some(y) => (y.content_type.clone(), y.digest.clone()),
//     };
//
//     let content = match ss.get_blob_bytes(&digest) {
//         Err(_e) => return Err(warp::reject::not_found()),
//         Ok(x) => x,
//     };
//     let raw_data = RawData {
//         content_type: content_type.clone(),
//         content,
//     };
//     let c = get_as_cbor(&raw_data)?;
//
//     let x = get_inside(vec![], &c, &components);
//     return match x {
//         Err(s) => {
//             debug!("Error: {}", s);
//             Err(warp::reject::not_found())
//         }
//         Ok(q) => {
//             let cbor_bytes: Vec<u8> = serde_cbor::to_vec(&q).unwrap();
//             let properties = TopicProperties {
//                 streamable: true,
//                 pushable: true,
//                 readable: true,
//                 immutable: false,
//             };
//             visualize_data(
//                 &properties,
//                 topic_name.to_relative_url(),
//                 html! {},
//                 "application/cbor",
//                 &cbor_bytes,
//                 headers,
//             )
//             .await
//         }
//     };
//     //
//     // eprintln!(
//     //     "go_queue: topic_name = {:?} components = {:?}",
//     //     topic_name, components
//     // );
//     // return Err(warp::reject::not_found());
// }

fn is_html(content_type: &str) -> bool {
    content_type.starts_with("text/html")
}

fn is_image(content_type: &str) -> bool {
    content_type.starts_with("image/")
}

pub async fn visualize_data(
    properties: &TopicProperties,
    title: String,
    extra_html: PreEscaped<String>,
    content_type: &str,
    content: &[u8],
    headers: HeaderMap,
) -> HandlersResponse {
    let accept_headers: Vec<String> = get_accept_header(&headers);
    let display = display_printable(content_type, content);
    let default_content_type = "application/yaml";
    let initial_value = "{}";
    let js = PreEscaped(JAVASCRIPT_SEND);
    if !is_html(content_type)
        && !is_image(content_type)
        && accept_headers.contains(&"text/html".to_string())
    {
        let x = make_html(
            &title,
            html! {

                 p { "This data is presented as HTML because you requested it as such."}
                         p { "Content type: " code { (content_type) } }
                         p { "Content length: " (content.len()) }

                            (extra_html)

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

                 script src="!/static/send.js" {};

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
    h.insert(HEADER_SEE_EVENTS, HeaderValue::from_static(EVENTS_SUFFIX));
    h.insert(
        HEADER_SEE_EVENTS_INLINE_DATA,
        HeaderValue::from_static(EVENTS_SUFFIX_DATA),
    );

    // put_common_headers(&ss, resp.headers_mut());

    Ok(resp.into())
}

pub async fn handle_websocket_generic2(
    path: String,
    ws: warp::ws::WebSocket,
    state: ServerStateAccess,
    send_data: bool,
) -> () {
    let (mut ws_tx, ws_rx) = ws.split();
    let (receiver, join_handle) = receive_from_websocket(ws_rx);

    // let msg = Message::text("closing with error");
    //            let _ = ws_tx.send(msg).await.map_err(|e| {
    //                error_with_info!("Cnnot send closing error: {:?}", e);
    //            });
    //            let msg = Message::close();
    //            let _ = ws_tx.send(msg).await.map_err(|e| {
    //                error_with_info!("Cnnot send closing error: {:?}", e);
    //            });
    //    return;

    match handle_websocket_generic2_(path, &mut ws_tx, receiver, state, send_data).await {
        Ok(_) => (),
        Err(e) => {
            let close_code: u16 = 1006; // Close codes 4000-4999 are available for private use
            let s = format!("handle_websocket_generic2: {}", e);
            error_with_info!("{s}");
            let msg = Message::text("closing with error");
            let _ = ws_tx.send(msg).await.map_err(|e| {
                error_with_info!("Cnnot send closing error: {:?}", e);
            });
            let msg = Message::close_with(close_code, s);
            let _ = ws_tx.send(msg).await.map_err(|e| {
                error_with_info!("Cnnot send closing error: {:?}", e);
            });
        }
    }

    let _ = join_handle.await;
}

pub async fn handle_websocket_generic2_(
    path: String,
    ws_tx: &mut SplitSink<warp::ws::WebSocket, Message>,
    // ws_rx: SplitStream<warp::ws::WebSocket>,
    receiver: UnboundedReceiverStream<MsgClientToServer>,
    state: ServerStateAccess,
    send_data: bool,
) -> DTPSR<()> {
    let referrer = None;
    let query = HashMap::new();
    // remove the last "events"
    let components = divide_in_components(&path, '/');
    // remove last onw
    // let components = components[0..components.len() - 1].to_vec();

    // if components.len() == 0 {
    //     return handle_websocket_queue(ws_tx, ws_rx, state, TopicName::root(), send_data).await;
    // }
    // debug!("handle_websocket_generic2: {:?}", components);
    let ds = interpret_path(&path, &query, &referrer, state.clone()).await?;
    // debug!("events: matched: {:?}", matched);

    // let ds = match matched {
    //     Ok(ds) => ds,
    //     Err(_) => {
    //         ws.close().await.unwrap();
    //         // how to send error?
    //         not_available!(format!("not available: {:?}", path.as_str()))
    //     }
    // };

    return match ds {
        TypeOFSource::Compose(sc) => {
            if sc.is_root {
                let topic_name = TopicName::root();
                spawn(do_receiving(topic_name.clone(), state.clone(), receiver));

                return handle_websocket_queue(ws_tx, state, topic_name, send_data).await;
            } else {
                not_implemented!("handle_websocket_generic2 not implemented TypeOFSource::Compose")
            }
        }
        TypeOFSource::OurQueue(topic_name, _, _) => {
            spawn(do_receiving(topic_name.clone(), state.clone(), receiver));

            return handle_websocket_queue(ws_tx, state, topic_name, send_data).await;
        }
        TypeOFSource::Digest(_digest, _content_type) => {
            not_implemented!("handle_websocket_generic2 not implemented TypeOFSource::Digest")
        }
        TypeOFSource::ForwardedQueue(_) => {
            not_implemented!("handle_websocket_generic2 not implemented for {ds:?}")
        }
        TypeOFSource::Transformed(_, _) => {
            not_implemented!("handle_websocket_generic2 not implemented for {ds:?}")
        }
        TypeOFSource::Deref(_) => {
            not_implemented!("handle_websocket_generic2 not implemented for {ds:?}")
        }
        TypeOFSource::OtherProxied(_) => {
            not_implemented!("handle_websocket_generic2 not implemented {ds:?}")
        }
        TypeOFSource::MountedDir(..) => {
            not_implemented!("handle_websocket_generic2 not implemented {ds:?}")
        }
        TypeOFSource::MountedFile(..) => {
            not_implemented!("handle_websocket_generic2 not implemented {ds:?}")
        }
    };
}

pub fn handle_websocket_forwarded() {}

pub fn make_index_html(index: &TopicsIndexInternal) -> PreEscaped<String> {
    let mut keys: Vec<&str> = index.topics.keys().map(|k| k.as_relative_url()).collect();

    keys.sort();
    let mut topic2url = Vec::new();
    // first get all the versions of the components
    let mut all_comps: Vec<Vec<String>> = vec![Vec::new()];

    for topic_name in keys.iter() {
        if topic_name == &"" {
            continue;
        }
        let topic_name = TopicName::from_relative_url(topic_name).unwrap();
        let components = topic_name.as_components();

        for i in 0..components.len() {
            let mut v = Vec::new();
            for j in 0..i + 1 {
                v.push(components[j].clone());
            }
            if v.len() == 0 {
                continue;
            }
            if !all_comps.contains(&v) {
                all_comps.push(v);
            }
        }
    }
    for topic_vecs in all_comps.iter() {
        if topic_vecs.len() == 0 {
            continue;
        }
        let mut url = String::new();
        for c in topic_vecs {
            url.push_str(&c);
            url.push_str("/");
        }

        let topic_name = TopicName::from_components(topic_vecs).to_relative_url();
        topic2url.push((topic_name, url));
    }

    // for topic_name in keys.iter() {
    //     if topic_name == &"" {
    //         continue;
    //     }
    //     let mut url = String::new();
    //     let components = divide_in_components(topic_name, '.');
    //     for c in components {
    //         url.push_str(&c);
    //         url.push_str("/");
    //     }
    //     topic2url.push((topic_name, url));
    // }

    let x = make_html(
        "index",
        html! {


                                  dl {
                       dt {   a href ={("../")} { code {("up")}
                                   }}
                                      dd{
                                      "One level up "
                                      }

                                  // h3 { "Dereference" }
                     dt{   a href ={(":deref/")} { code {(":deref")} }
        } dd {
                                  "Dereference as a single topic"

                                  }
                                      }

              h3 { "Topics" }

                ul {
                    @ for (topic_name,
                    url) in topic2url.iter() {

                    li { a href ={(url)} { code {(topic_name)} }}
                    }
                }

                // h2 { "Index answer presented in YAML" }
                //
                // pre {
                //     code {
                //     (serde_yaml::to_string( & index).unwrap())
                //     }
                // }
            },
    );
    x
}
