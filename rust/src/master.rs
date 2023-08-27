use std::collections::HashMap;
use std::string::ToString;

use bytes::Bytes;
use futures::stream::SplitSink;
use futures::SinkExt;
use futures::StreamExt;
use log::{debug, error};
use maud::{html, PreEscaped};
use tokio::spawn;
use tokio::sync::broadcast::error::RecvError;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tungstenite::http::{HeaderMap, StatusCode};
use tungstenite::Message as TungsteniteMessage;
use warp::http::header;
use warp::hyper::Body;
use warp::reply::Response;
use warp::ws::Message as WarpMessage;

use crate::cbor_manipulation::display_printable;
use crate::html_utils::make_html;
use crate::signals_logic::{
    interpret_path, DataProps, GetMeta, ResolveDataSingle, ResolvedData, TopicProperties,
    TypeOFSource,
};
use crate::utils::{divide_in_components, get_good_url_for_components};
use crate::utils_headers::get_accept_header;
use crate::websocket_abstractions::open_websocket_connection;
use crate::websocket_signals::{MsgClientToServer, MsgServerToClient};
use crate::{
    do_receiving, error_with_info, get_header_with_default, get_metadata,
    get_series_of_messages_for_notification_, handle_topic_post, handle_websocket_queue,
    make_request, not_implemented, object_queues, put_alternative_locations,
    receive_from_websocket, send_as_ws_cbor, serve_static_file_path, utils_headers, utils_mime,
    DataStream, HandlersResponse, ObjectQueue, RawData, ServerStateAccess, TopicName,
    TopicsIndexInternal, CONTENT_TYPE, DTPSR, JAVASCRIPT_SEND, OCTET_STREAM,
};

pub async fn serve_master_post(
    path: warp::path::FullPath,
    query: HashMap<String, String>,
    ss_mutex: ServerStateAccess,
    headers: HeaderMap,
    data: hyper::body::Bytes,
) -> HandlersResponse {
    let path_str = path_normalize(&path);
    let referrer = get_referrer(&headers);

    let matched: DTPSR<TypeOFSource> = {
        let ss = ss_mutex.lock().await;
        interpret_path(&path_str, &query, &referrer, &ss).await
    };

    let content_type = get_header_with_default(&headers, CONTENT_TYPE, OCTET_STREAM);
    let byte_vector: Vec<u8> = data.to_vec().clone();
    let rd = RawData::new(byte_vector, content_type);

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
        let s = format!("Cannot push to {path_str:?} because the topic is not pushable:\n{ds:?}");
        error!(" {s}");
        let res = http::Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::from(s.to_string()))
            .unwrap();
        return Ok(res);
    }

    return match ds {
        TypeOFSource::OurQueue(topic_name, _, _) => {
            debug!("Pushing to topic {:?}", topic_name);
            handle_topic_post(&topic_name, ss_mutex, &rd).await
        }
        TypeOFSource::ForwardedQueue(fq) => {
            let con = {
                let ss = ss_mutex.lock().await;
                let sub = ss.proxied.get(&fq.subscription).unwrap();
                sub.url.join(fq.his_topic_name.as_relative_url())?
            };
            let resp = make_request(
                &con,
                hyper::Method::POST,
                &rd.content,
                Some(&rd.content_type),
                None,
            )
            .await?;
            if !resp.status().is_success() {
                let s = format!("The proxied request did not succeed. con ={con} ");
                error_with_info!("{s}");
            }

            Ok(resp)
        }
        _ => {
            let s = format!("We do not support POST to {path_str}: {ds:?}");
            error_with_info!("{s}");
            let res = http::Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Body::from(s.to_string()))
                .unwrap();
            Ok(res)
        }
    };
}

pub async fn serve_master_head(
    path: warp::path::FullPath,
    query: HashMap<String, String>,
    ss_mutex: ServerStateAccess,
    headers: HeaderMap,
) -> HandlersResponse {
    let path_str = path_normalize(&path);
    // debug!("serve_master_head: path_str: {}", path_str);
    let referrer = get_referrer(&headers);
    let matched: DTPSR<TypeOFSource> = {
        let ss = ss_mutex.lock().await;
        interpret_path(&path_str, &query, &referrer, &ss).await
    };

    let ds = match matched {
        Ok(ds) => ds,
        Err(s) => {
            // debug!("serve_master_head: path_str: {}\n{}", path_str, s);

            let res = http::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(s.to_string()))
                .unwrap();
            return Ok(res);
        }
    };
    // debug!("serve_master_head: ds: {:?}", ds);
    match &ds {
        TypeOFSource::OurQueue(topic_name, _, _) => {
            let x: &ObjectQueue;
            let ss = ss_mutex.lock().await;
            match ss.oqs.get(&topic_name) {
                None => return Err(warp::reject::not_found()),

                Some(y) => x = y,
            }
            let empty_vec: Vec<u8> = Vec::new();
            let mut resp = Response::new(Body::from(empty_vec));
            let h = resp.headers_mut();
            utils_headers::put_common_headers(&ss, h);
            utils_headers::put_meta_headers(h, &ds.get_properties());
            utils_headers::put_source_headers(h, &x.tr.origin_node, &x.tr.unique_id);

            let suffix = topic_name.as_relative_url();
            put_alternative_locations(&ss, h, &suffix);
            return Ok(resp.into());
        }
        TypeOFSource::ForwardedQueue(fq) => {
            let tr = {
                let ss = ss_mutex.lock().await;
                let pti = ss.proxied_topics.get(&fq.my_topic_name).unwrap();
                pti.tr_original.clone()
            };
            let empty_vec: Vec<u8> = Vec::new();
            let mut resp = Response::new(Body::from(empty_vec));
            let h = resp.headers_mut();
            utils_headers::put_source_headers(h, &tr.origin_node, &tr.unique_id);
            utils_headers::put_meta_headers(h, &ds.get_properties());
            // let suffix = topic_name.as_relative_url();
            // put_alternative_locations(&ss, h, &suffix);
            {
                let ss = ss_mutex.lock().await;
                utils_headers::put_common_headers(&ss, h);
            }
            return Ok(resp.into());
        }

        TypeOFSource::MountedDir(_, _, _) => {
            // return serve_master_get(path, query, ss_mutex, headers).await;
            // error!("HEAD not supported for path = {path_str}, ds = {ds:?}");
            //
            // not_supported
        }
        TypeOFSource::MountedFile(_, _, _) => {
            // error!("HEAD not supported for path = {path_str}, ds = {ds:?}");

            // not_supported
        }
        TypeOFSource::Compose(c) => {
            if c.is_root {
                let ss = ss_mutex.lock().await;
                let topic_name = TopicName::root();
                let x: &ObjectQueue = ss.oqs.get(&topic_name).unwrap();

                let empty_vec: Vec<u8> = Vec::new();
                let mut resp = Response::new(Body::from(empty_vec));
                let h = resp.headers_mut();
                utils_headers::put_source_headers(h, &x.tr.origin_node, &x.tr.unique_id);

                utils_headers::put_meta_headers(h, &ds.get_properties());
                let suffix = topic_name.as_relative_url();
                put_alternative_locations(&ss, h, &suffix);
                utils_headers::put_common_headers(&ss, h);
                return Ok(resp.into());
            } else {
            }
        }
        TypeOFSource::Transformed(..) => {}
        TypeOFSource::Digest(..) => {}
        TypeOFSource::Deref(..) => {}
        TypeOFSource::OtherProxied(_) => {}
        TypeOFSource::Index(..) => {}
        TypeOFSource::Aliased(..) => {}
        TypeOFSource::History(..) => {
            // error!("HEAD not supported for path = {path_str}, ds = {ds:?}");

            // not_supported
        }
    }

    serve_master_get(path, query, ss_mutex, headers).await
}

fn path_normalize(path: &warp::path::FullPath) -> String {
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

    let path_str = path_normalize(&path);

    if path_str.len() > 250 {
        panic!("Path too long: {:?}", path_str);
    }

    let path_components0 = divide_in_components(&path_str, '/');
    let path_components = path_components0.clone();

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
    let ds = {
        let ss = ss_mutex.lock().await;
        interpret_path(&path_str, &query, &referrer, &ss).await
    }?;

    debug!("serve_master: ds={:?} ", ds);

    // let ds_props = ds.get_properties();

    // match ds {
    //     TypeOFSource::OurQueue(topic_name, _, _) => {
    //         return if true || topic_name.is_root() {
    //             handler_topic_generic(&topic_name, ss_mutex.clone(), headers).await
    //         } else {
    //             root_handler(ss_mutex.clone(), headers).await
    //         };
    //     }
    //     _ => {}
    // }
    // let presented_as = TopicName::from_components(&path_components0);

    let resd0 = ds.resolve_data_single(&path_str, ss_mutex.clone()).await;

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
            match ds.get_meta_index(&path_str, ss_mutex.clone()).await {
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

    // let x = ds.get_meta_index(&path_str, ss_mutex.clone()).await?;
    // let ds_props = &(x.topics.get(&TopicName::root()).unwrap().properties);

    let ds_props = ds.get_properties();
    return visualize_data(
        &ds_props,
        path_str.to_string(),
        extra_html,
        &rd.content_type,
        &rd.content,
        headers,
        ss_mutex,
    )
    .await;
}

fn make_friendly_visualization(
    properties: &TopicProperties,
    title: String,
    extra_html: PreEscaped<String>,
    content_type: &str,
    content: &[u8],
) -> HandlersResponse {
    let display = display_printable(content_type, content);
    let default_content_type = "application/yaml";
    let initial_value = "{}";
    let js = PreEscaped(JAVASCRIPT_SEND);
    let x = make_html(
        &title,
        html! {

            p { "This data is presented as HTML because you requested it as such."}
            p { "Content type: " code { (content_type) } }
            p { "Content length: " (content.len()) }

            (extra_html)

            div {
                @if properties.streamable {
                    h3 { "notifications using websockets"}
                    div {pre   id="result" { "result will appear here" }}
                }
            }

            div {
                @if properties.pushable {
                    h3 {"push to queue"}

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

    utils_headers::put_header_content_type(headers, "text/html");
    return Ok(resp);
}

pub async fn visualize_data(
    properties: &TopicProperties,
    title: String,
    extra_html: PreEscaped<String>,
    content_type: &str,
    content: &[u8],
    headers: HeaderMap,
    ssa: ServerStateAccess,
) -> HandlersResponse {
    let accept_headers: Vec<String> = get_accept_header(&headers);

    if !utils_mime::is_html(content_type)
        && !utils_mime::is_image(content_type)
        && accept_headers.contains(&"text/html".to_string())
    {
        make_friendly_visualization(properties, title, extra_html, content_type, content)
    } else {
        let mut resp = Response::new(Body::from(content.to_vec()));
        let h = resp.headers_mut();

        utils_headers::put_header_content_type(h, content_type);
        utils_headers::put_meta_headers(h, properties);
        {
            let ss = ssa.lock().await;
            utils_headers::put_common_headers(&ss, h);
        }
        // debug!("the response is {resp:?}");
        Ok(resp.into())
    }
}

pub async fn handle_websocket_generic2(
    path: String,
    ws: warp::ws::WebSocket,
    state: ServerStateAccess,
    send_data: bool,
) -> () {
    let (mut ws_tx, ws_rx) = ws.split();
    let (receiver, join_handle) = receive_from_websocket(ws_rx);

    match handle_websocket_generic2_(path, &mut ws_tx, receiver, state, send_data).await {
        Ok(_) => (),
        Err(e) => {
            let close_code: u16 = 1006; // Close codes 4000-4999 are available for private use
            let s = format!("handle_websocket_generic2: {}", e);
            error_with_info!("{s}");
            let msg = WarpMessage::text("closing with error");
            let _ = ws_tx.send(msg).await.map_err(|e| {
                error_with_info!("Cnnot send closing error: {:?}", e);
            });
            let msg = WarpMessage::close_with(close_code, s);
            let _ = ws_tx.send(msg).await.map_err(|e| {
                error_with_info!("Cnnot send closing error: {:?}", e);
            });
        }
    }

    let _ = join_handle.await;
}

pub async fn handle_websocket_generic2_(
    path: String,
    ws_tx: &mut SplitSink<warp::ws::WebSocket, WarpMessage>,
    receiver: UnboundedReceiverStream<MsgClientToServer>,
    state: ServerStateAccess,
    send_data: bool,
) -> DTPSR<()> {
    let referrer = None;
    let query = HashMap::new();

    let ds = {
        let ss = state.lock().await;
        interpret_path(&path, &query, &referrer, &ss).await
    }?;

    // let ds = interpret_path(&path, &query, &referrer, state.clone()).await?;

    return match &ds {
        TypeOFSource::Compose(sc) => {
            if sc.is_root {
                let topic_name = TopicName::root();
                spawn(do_receiving(topic_name.clone(), state.clone(), receiver));

                return handle_websocket_queue(ws_tx, state, topic_name, send_data).await;
            } else {
                let stream = ds.get_data_stream(path.as_str(), state).await?;

                handle_websocket_data_stream(ws_tx, stream, send_data).await
                // not_implemented!("handle_websocket_generic2 not implemented TypeOFSource::Compose")
            }
        }
        TypeOFSource::OurQueue(topic_name, _, _) => {
            spawn(do_receiving(topic_name.clone(), state.clone(), receiver));

            return handle_websocket_queue(ws_tx, state, topic_name.clone(), send_data).await;
        }

        TypeOFSource::ForwardedQueue(fq) => {
            handle_websocket_forwarded(
                state,
                fq.subscription.clone(),
                fq.his_topic_name.clone(),
                ws_tx,
                receiver,
                send_data,
            )
            .await
            // not_implemented!("handle_websocket_generic2 not implemented for {ds:?}")
        }
        // TypeOFSource::Digest(_digest, _content_type) => {
        //     not_implemented!("handle_websocket_generic2 not implemented TypeOFSource::Digest")
        // }
        //
        // TypeOFSource::Transformed(_, _) => {
        //     not_implemented!("handle_websocket_generic2 not implemented for {ds:?}")
        // }
        // TypeOFSource::Deref(_) => {
        //     not_implemented!("handle_websocket_generic2 not implemented for {ds:?}")
        // }
        // TypeOFSource::OtherProxied(_) => {
        //     not_implemented!("handle_websocket_generic2 not implemented {ds:?}")
        // }
        // TypeOFSource::MountedDir(..) => {
        //     not_implemented!("handle_websocket_generic2 not implemented {ds:?}")
        // }
        // TypeOFSource::MountedFile(..) => {
        //     not_implemented!("handle_websocket_generic2 not implemented {ds:?}")
        // }
        // TypeOFSource::Index(..) => {
        //     not_implemented!("handle_websocket_generic2 not implemented {ds:?}")
        // }
        // TypeOFSource::Aliased(..) => {
        //     not_implemented!("handle_websocket_generic2 not implemented {ds:?}")
        // }
        _ => {
            let stream = ds.get_data_stream(path.as_str(), state).await?;

            handle_websocket_data_stream(ws_tx, stream, send_data).await
        }
    };
}

pub async fn handle_websocket_data_stream(
    ws_tx: &mut SplitSink<warp::ws::WebSocket, warp::ws::Message>,
    data_stream: DataStream,
    send_data: bool,
) -> DTPSR<()> {
    let mut starting_messaging = vec![];

    starting_messaging.push(MsgServerToClient::ChannelInfo(data_stream.channel_info));

    if let Some(first) = data_stream.first {
        let mut for_this = get_series_of_messages_for_notification_(send_data, &first).await;
        starting_messaging.append(&mut for_this);
    }

    send_as_ws_cbor(&starting_messaging, ws_tx).await?;

    let mut stream = match data_stream.stream {
        None => {
            return Ok(());
        }
        Some(x) => x,
    };
    loop {
        let r = stream.recv().await?;

        let for_this = get_series_of_messages_for_notification_(send_data, &r).await;
        send_as_ws_cbor(&for_this, ws_tx).await?;
    }
    Ok(())
}

pub async fn handle_websocket_forwarded(
    state: ServerStateAccess,
    subscription: String,
    its_topic_name: TopicName,
    ws_tx: &mut SplitSink<warp::ws::WebSocket, warp::ws::Message>,
    receiver: UnboundedReceiverStream<MsgClientToServer>,
    send_data: bool,
) -> DTPSR<()> {
    let url = {
        let ss = state.lock().await;
        let sub = ss.proxied.get(&subscription).unwrap();

        let url = sub.url.join(its_topic_name.as_relative_url())?;
        url
    };

    let md = get_metadata(&url).await?;

    let use_url = if send_data {
        md.events_data_inline_url.unwrap()
    } else {
        md.events_url.unwrap()
    };

    let wsc = open_websocket_connection(&use_url).await?;

    let mut incoming = wsc.get_incoming().await;

    loop {
        let msg = match incoming.recv().await {
            Ok(msg) => msg,
            Err(e) => match e {
                RecvError::Closed => {
                    break;
                }
                RecvError::Lagged(_) => {
                    error_with_info!("lagged");
                    continue;
                }
            },
        };
        let x = warp_from_tungstenite(msg)?;
        match ws_tx.send(x).await {
            Ok(_) => {}
            Err(e) => {
                // match e {
                //     Error { .. } => {}
                // }
                error_with_info!("Cannot send message: {e}")
            }
        }
    }
    Ok(())
}

fn warp_from_tungstenite(msg: TungsteniteMessage) -> DTPSR<WarpMessage> {
    match msg {
        TungsteniteMessage::Text(text) => Ok(WarpMessage::text(text)),
        TungsteniteMessage::Binary(data) => Ok(WarpMessage::binary(data)),
        TungsteniteMessage::Ping(data) => Ok(WarpMessage::ping(data)),
        TungsteniteMessage::Pong(data) => Ok(WarpMessage::pong(data)),
        TungsteniteMessage::Close(Some(frame)) => {
            let code: u16 = frame.code.into();
            Ok(WarpMessage::close_with(code, frame.reason))
        }
        TungsteniteMessage::Close(None) => Ok(WarpMessage::close()),
        TungsteniteMessage::Frame(..) => {
            return not_implemented!("we should never get here: {msg}");
        }
    }
}

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

    let x = make_html(
        "index",
        html! {
            dl {
                dt { a href ={("../")} { code {("up")} }}
                dd { "One level up"}

                dt { a href ={(":deref/")} { code {(":deref")} }}
                dd { "Dereference as a single topic" }
            }

            h3 { "Topics" }

            ul {
                @ for (topic_name, url) in topic2url.iter() {

                    li {
                        a href ={(url)} {
                            code {(topic_name)}
                        }
                    }
                }
            }

        },
    );
    x
}
