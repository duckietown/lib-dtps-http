use std::{
    collections::HashMap,
    string::ToString,
};

use bytes::Bytes;
use futures::{
    stream::SplitSink,
    SinkExt,
    StreamExt,
};
use maud::{
    html,
    PreEscaped,
};
use tokio::sync::broadcast::error::RecvError;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tungstenite::{
    http::{
        HeaderMap,
        StatusCode,
    },
    protocol::frame::coding::CloseCode,
};
use warp::{
    http::header,
    hyper::Body,
    reply::Response,
    ws::Message as WarpMessage,
};

use crate::{
    clocks::Clocks,
    debug_with_info,
    display_printable,
    divide_in_components,
    error_with_info,
    get_accept_header,
    get_header_with_default,
    get_series_of_messages_for_notification_,
    handle_topic_post,
    interpret_path,
    make_html,
    make_request,
    put_alternative_locations,
    receive_from_websocket,
    send_as_ws_cbor,
    serve_static_file_path,
    signals_logic::Pushable,
    utils_headers,
    utils_headers::put_patchable_headers,
    utils_mime,
    DTPSError,
    DataProps,
    DataStream,
    ErrorMsg,
    FinishedMsg,
    GetMeta,
    GetStream,
    HandlersResponse,
    ListenURLEvents,
    MsgClientToServer,
    MsgServerToClient,
    ObjectQueue,
    Patchable,
    RawData,
    ResolveDataSingle,
    ResolvedData,
    ServerStateAccess,
    TopicName,
    TopicProperties,
    TopicsIndexInternal,
    TypeOFSource,
    WarningMsg,
    CONTENT_TYPE,
    CONTENT_TYPE_OCTET_STREAM,
    CONTENT_TYPE_PATCH_CBOR,
    CONTENT_TYPE_PATCH_JSON,
    CONTENT_TYPE_TEXT_HTML,
    CONTENT_TYPE_TEXT_PLAIN,
    CONTENT_TYPE_YAML,
    DTPSR,
    JAVASCRIPT_SEND,
};

pub async fn serve_master_post(
    path: warp::path::FullPath,
    query: HashMap<String, String>,
    ss_mutex: ServerStateAccess,
    headers: HeaderMap,
    data: Bytes,
) -> HandlersResponse {
    let path_str = path_normalize(&path);
    let referrer = get_referrer(&headers);

    let matched: DTPSR<TypeOFSource> = {
        let ss = ss_mutex.lock().await;
        interpret_path(&path_str, &query, &referrer, &ss).await
    };

    let content_type = get_header_with_default(&headers, CONTENT_TYPE, CONTENT_TYPE_OCTET_STREAM);
    let byte_vector: Vec<u8> = data.to_vec();
    let rd = RawData::new(byte_vector, content_type);

    let ds = match matched {
        Ok(ds) => ds,
        Err(s) => return s.into(),
    };

    let p = ds.get_properties();
    if !p.pushable {
        let s = format!("Cannot push to {path_str:?} because the topic is not pushable:\n{ds:?}");
        error_with_info!(" {s}");
        let res = http::Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::from(s))
            .unwrap();
        return Ok(res);
    }
    let res = ds.push(ss_mutex.clone(), &rd, &Clocks::default()).await;

    match res {
        Ok(_) => {
            // FIXME: (post) need to return the location of the new resource
            let res = http::Response::builder()
                .status(StatusCode::OK)
                .body(Body::from("Push ok"))
                .unwrap();
            Ok(res)
        }
        Err(e) => {
            error_with_info!("Push not ok: {}", e);
            e.as_handler_response()
        }
    }

    // return match ds {
    //     TypeOFSource::OurQueue(topic_name, _) => {
    //         debug_with_info!("Pushing to topic {:?}", topic_name);
    //         handle_topic_post(&topic_name, ss_mutex, &rd).await
    //     }
    //     TypeOFSource::ForwardedQueue(fq) => {
    //         let con = {
    //             let ss = ss_mutex.lock().await;
    //             let sub = ss.proxied.get(&fq.subscription).unwrap();
    //             match &sub.established {
    //                 None => {
    //                     let msg = "Subscription not established";
    //                     let res = http::Response::builder()
    //                         .status(StatusCode::NOT_FOUND) //ok
    //                         .body(Body::from(msg.to_string()))
    //                         .unwrap();
    //                     return Ok(res);
    //                 }
    //                 Some(est) => est.using.join(fq.his_topic_name.as_relative_url())?,
    //             }
    //         };
    //         let resp = make_request(&con, hyper::Method::POST, &rd.content, Some(&rd.content_type), None).await?;
    //         if !resp.status().is_success() {
    //             let s = format!("The proxied request did not succeed. con ={con} ");
    //             error_with_info!("{s}");
    //         }
    //
    //         Ok(resp)
    //     }
    //     TypeOFSource::Transformed(source, t) => {
    //         let clocks = Clocks::default();
    //
    //         ds.push(ss_mutex.clone(), &rd, &clocks).await
    //         //
    //         // debug_with_info!("Pushing to transformed {source:?} / {t:?} with rd = {rd:?}");
    //         // let clocks = Clocks::default();
    //         // source.push(ss_mutex, &rd,& clocks).await?;
    //         // let s = "";
    //         // let res = http::Response::builder()
    //         //     .status(StatusCode::OK)
    //         //     .body(Body::from(s))
    //         //     .unwrap();
    //         // Ok(res)
    //
    //         // debug_with_info!("Pushing to topic {:?}", topic_name);
    //         // handle_topic_post(&topic_name, ss_mutex, &rd).await
    //     }
    //     _ => {
    //         let s = format!("We do not support POST to {path_str}: {ds:?}");
    //         error_with_info!("{s}");
    //         let res = http::Response::builder()
    //             .status(StatusCode::METHOD_NOT_ALLOWED)
    //             .body(Body::from(s))
    //             .unwrap();
    //         Ok(res)
    //     }
    // };
}

pub async fn serve_master_patch(
    path: warp::path::FullPath,
    query: HashMap<String, String>,
    ss_mutex: ServerStateAccess,
    headers: HeaderMap,
    data: Bytes,
) -> HandlersResponse {
    let path_str = path_normalize(&path);
    let referrer = get_referrer(&headers);

    let matched: DTPSR<TypeOFSource> = {
        let ss = ss_mutex.lock().await;
        interpret_path(&path_str, &query, &referrer, &ss).await
    };

    let content_type = get_header_with_default(&headers, CONTENT_TYPE, CONTENT_TYPE_OCTET_STREAM);
    let result = if content_type == CONTENT_TYPE_PATCH_JSON {
        let r = serde_json::from_slice::<json_patch::Patch>(&data);
        // debug_with_info!("orig: {:?}", data);
        // debug_with_info!("Parsed patch: {:?}", r);
        match r {
            Ok(x) => x,
            Err(e) => {
                let s = format!("Cannot parse the patch as JSON: {data:?}\n{e}");
                error_with_info!("{s}");
                return DTPSError::from(e).into();
            }
        }
    } else if content_type == CONTENT_TYPE_PATCH_CBOR {
        let r = serde_cbor::from_slice::<json_patch::Patch>(&data);
        match r {
            Ok(x) => x,
            Err(e) => {
                let s = format!("Cannot parse the patch as JSON: {data:?}\n{e}");
                error_with_info!("{s}");
                let res = http::Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from(s))
                    .unwrap();
                return Ok(res);
            }
        }
    } else {
        let s = format!("We do not support PATCH with content type {content_type}");
        error_with_info!("{s}");
        let res = http::Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from(s))
            .unwrap();
        return Ok(res);
    };

    let ds = match matched {
        Ok(ds) => ds,
        Err(s) => return s.into(),
    };

    let p = ds.get_properties();
    if !p.patchable {
        let s = format!("Cannot patch to {path_str:?} because the topic is not patchable:\n{ds:#?}");
        error_with_info!(" {s}");
        let res = http::Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::from(s))
            .unwrap();
        return Ok(res);
    }

    let x = ds.patch(&path_str, ss_mutex.clone(), &result).await;
    match x {
        Ok(_) => {
            let res = http::Response::builder()
                .status(StatusCode::OK)
                .body(Body::from("Patch ok1"))
                .unwrap();
            Ok(res)
        }
        Err(e) => {
            error_with_info!("Patch not ok: {}", e);
            e.as_handler_response()
        }
    }
}

pub async fn serve_master_head(
    path: warp::path::FullPath,
    query: HashMap<String, String>,
    ss_mutex: ServerStateAccess,
    headers: HeaderMap,
) -> HandlersResponse {
    debug_with_info!("HEAD {} ", path.as_str());

    let path_str = path_normalize(&path);
    // debug_with_info!("serve_master_head: path_str: {}", path_str);
    let referrer = get_referrer(&headers);
    let matched: DTPSR<TypeOFSource> = {
        let ss = ss_mutex.lock().await;
        interpret_path(&path_str, &query, &referrer, &ss).await
    };

    let ds = match matched {
        Ok(ds) => ds,
        Err(s) => {
            // error_with_info!("serve_master_head: {path:?} matched error: {s}");
            return s.into();
        }
    };
    debug_with_info!("serve_master_head: ds: {:?}", ds);
    match &ds {
        TypeOFSource::OurQueue(topic_name, _) => {
            let ss = ss_mutex.lock().await;
            let x: &ObjectQueue = match ss.oqs.get(topic_name) {
                None => return Err(warp::reject::not_found()),

                Some(y) => y,
            };
            let empty_vec: Vec<u8> = Vec::new();
            let mut resp = Response::new(Body::from(empty_vec));
            let h = resp.headers_mut();
            utils_headers::put_common_headers(&ss, h);
            utils_headers::put_meta_headers(h, &ds.get_properties());
            utils_headers::put_source_headers(h, &x.tr.origin_node, &x.tr.unique_id);
            if topic_name.is_root() {
                put_patchable_headers(h)?;
            }
            let suffix = topic_name.as_relative_url();
            put_alternative_locations(&ss, h, suffix);
            return Ok(resp);
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
            {
                let ss = ss_mutex.lock().await;
                utils_headers::put_common_headers(&ss, h);
            }
            return Ok(resp);
        }

        TypeOFSource::MountedDir(_, _, _) => {
            // return serve_master_get(path, query, ss_mutex, headers).await;
            // error_with_info!("HEAD not supported for path = {path_str}, ds = {ds:?}");
            //
            // not_supported
        }
        TypeOFSource::MountedFile { .. } => {
            // error_with_info!("HEAD not supported for path = {path_str}, ds = {ds:?}");

            // not_supported
        }
        TypeOFSource::Compose(_c) => {
            // if c.topic_name.is_root() {
            //     let ss = ss_mutex.lock().await;
            //     let topic_name = TopicName::root();
            //     let x: &ObjectQueue = ss.oqs.get(&topic_name).unwrap();
            //
            //     let empty_vec: Vec<u8> = Vec::new();
            //     let mut resp = Response::new(Body::from(empty_vec));
            //     let h = resp.headers_mut();
            //     utils_headers::put_source_headers(h, &x.tr.origin_node, &x.tr.unique_id);
            //
            //     utils_headers::put_meta_headers(h, &ds.get_properties());
            //     let suffix = topic_name.as_relative_url();
            //     put_alternative_locations(&ss, h, &suffix);
            //     utils_headers::put_common_headers(&ss, h);
            //     return Ok(resp.into());
            // } else {
            // }
        }
        TypeOFSource::Transformed(..) => {}
        TypeOFSource::Digest(..) => {}
        TypeOFSource::Deref(..) => {}
        TypeOFSource::OtherProxied(_) => {}
        TypeOFSource::Index(..) => {}
        TypeOFSource::Aliased(..) => {}
        TypeOFSource::History(..) => {
            // error_with_info!("HEAD not supported for path = {path_str}, ds = {ds:?}");

            // not_supported
        }
    }
    debug_with_info!("HEAD not supported for path = {path_str}, ds = {ds:?}");
    let x = serve_master_get(path, query, ss_mutex, headers).await;
    // debug_with_info!("response: {x:?}");
    // match x {
    //     Ok(r) => {
    //         let mut resp = r.into_response();
    //         let h = resp.headers_mut();
    //         h.insert(header::CONTENT_LENGTH, "0".parse().unwrap());
    //         Ok(resp)
    //     }
    //     Err(e) => Err(e),
    // }
    x
}

fn path_normalize(path: &warp::path::FullPath) -> String {
    let path_str = path.as_str();
    let pat = "/!/";

    if path_str.contains(pat) {
        // only preserve the part after the !/

        let index = path_str.rfind(pat).unwrap();
        let path_str2 = &path_str[index + pat.len() - 1..];

        path_str2.to_string().replace("/!!", "/!")

        // debug_with_info!(
        //     "serve_master: normalizing /!/:\n   {:?}\n-> {:?} ",
        //     path, res
        // );
    } else {
        path_str.to_string()
    }
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
    debug_with_info!("GET {} ", path.as_str());
    // debug_with_info!("Referrer {:?} ", referrer);

    let path_str = path_normalize(&path);

    if path_str.len() > 250 {
        panic!("Path too long: {:?}", path_str);
    }

    let path_components0 = divide_in_components(&path_str, '/');
    let path_components = path_components0.clone();

    match path_components.first() {
        None => {}
        Some(first) => {
            if first == STATIC_PREFIX {
                let pathstr = path_str;

                // find "static/" in path and give the rest
                let index = pathstr.find("static/").unwrap();
                let path = &pathstr[index + "static/".len()..];

                return serve_static_file_path(path).await;
            }
        }
    }

    // if false {
    //     if !path_components.contains(&STATIC_PREFIX.to_string()) {
    //         let good_url = get_good_url_for_components(&path_components);
    //         if good_url != path_str {
    //             let good_relative_url = format!("./{}/", path_components.last().unwrap());
    //
    //             debug_with_info!(
    //                 "Redirecting\n - {}\n->  {};\n rel = {}\n",
    //                 path_str,
    //                 good_url,
    //                 good_relative_url
    //             );
    //
    //             let text_response = format!("Redirecting to {}", good_url);
    //             let res = http::Response::builder()
    //                 .status(StatusCode::MOVED_PERMANENTLY)
    //                 .header(header::LOCATION, good_relative_url)
    //                 .body(Body::from(text_response))
    //                 .unwrap();
    //
    //             // TODO: renable
    //             return Ok(res);
    //         }
    //     }
    // }
    let ds = {
        let ss = ss_mutex.lock().await;
        interpret_path(&path_str, &query, &referrer, &ss).await
    }?;

    debug_with_info!("serve_master: ds={:?} ", ds);
    let r = ds.resolve_data_single(&path_str, ss_mutex.clone()).await;
    // let resd0 = context!(r, "Cannot resolve_data_single for {path_str}");

    let resd = match r {
        Ok(x) => x,
        Err(s) => {
            return s.into();
        }
    };
    let rd: RawData = match resd {
        ResolvedData::RawData(rd) => rd,
        ResolvedData::Regular(reg) => {
            let cbor_bytes = serde_cbor::to_vec(&reg).unwrap();
            let bytes = Bytes::from(cbor_bytes);
            RawData::cbor(bytes)
        }
        ResolvedData::NotAvailableYet(s) => {
            let accept_headers: Vec<String> = get_accept_header(&headers);

            let use_content_type = if accept_headers.contains(&"text/html".to_string()) {
                CONTENT_TYPE_TEXT_HTML
            } else {
                CONTENT_TYPE_OCTET_STREAM
            };

            let res = http::Response::builder()
                .status(StatusCode::NO_CONTENT)
                .header(header::CONTENT_TYPE, use_content_type)
                .body(Body::from(s))
                .unwrap();
            return Ok(res);
        }
        ResolvedData::NotFound(s) => {
            let msg = format!("Not found - {s}");
            let res = http::Response::builder()
                .status(StatusCode::NOT_FOUND) //ok
                .header(header::CONTENT_TYPE, CONTENT_TYPE_TEXT_PLAIN)
                .body(Body::from(msg))
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

    let ds_props = ds.get_properties();
    visualize_data(
        &path_str,
        &ds_props,
        path_str.to_string(),
        extra_html,
        &rd.content_type,
        &rd.content,
        headers,
        ss_mutex,
    )
    .await
}

fn make_friendly_visualization(
    properties: &TopicProperties,
    title: String,
    extra_html: PreEscaped<String>,
    content_type: &str,
    content: &[u8],
) -> HandlersResponse {
    let display = display_printable(content_type, content);
    let default_content_type = CONTENT_TYPE_YAML;
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

            h3 { "data received " }
            pre id="data_field" {
                code {
                    (display)
                }
            }
            img id="data_field_image" src=""  {}

        },
    );

    let markup = x.into_string();
    let mut resp = Response::new(Body::from(markup));
    let headers = resp.headers_mut();

    utils_headers::put_header_content_type(headers, "text/html");
    Ok(resp)
}

pub async fn visualize_data(
    path: &str,
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

        if path == "/" {
            put_patchable_headers(h)?;
        }
        utils_headers::put_header_content_type(h, content_type);
        utils_headers::put_meta_headers(h, properties);
        {
            let ss = ssa.lock().await;
            utils_headers::put_common_headers(&ss, h);
        }
        // debug_with_info!("the response is {resp:?}");
        Ok(resp)
    }
}

pub async fn handle_websocket_generic2(
    path: String,
    ws: warp::ws::WebSocket,
    state: ServerStateAccess,
    send_data: bool,
) {
    debug_with_info!("WEBSOCKET {path} send_data={send_data}");
    let (mut ws_tx, ws_rx) = ws.split();
    let (receiver, join_handle) = receive_from_websocket(ws_rx);

    match handle_websocket_generic2_(path, &mut ws_tx, receiver, state, send_data).await {
        Ok(_) => {
            let msg = WarpMessage::close_with(CloseCode::Normal, "ok");
            let _ = ws_tx.send(msg).await.map_err(|e| {
                debug_with_info!("Cannot send closing code: {:?}", e);
            });
        }
        Err(e) => {
            let s = format!("handle_websocket_generic2:\n{}", e);
            debug_with_info!("{s}");
            let msgs = vec![MsgServerToClient::ErrorMsg(ErrorMsg { comment: s.clone() })];
            send_as_ws_cbor(&msgs, &mut ws_tx).await.unwrap();

            // get first 16 chars
            let reason = s.chars().take(16).collect::<String>();
            let msg = WarpMessage::close_with(CloseCode::Error, reason);
            let _ = ws_tx.send(msg).await.map_err(|e| {
                debug_with_info!("Cannot send closing error: {:?}", e);
            });
        }
    }

    let _ = join_handle.await;
}

pub async fn handle_websocket_generic2_(
    path: String,
    ws_tx: &mut SplitSink<warp::ws::WebSocket, WarpMessage>,
    _receiver: UnboundedReceiverStream<MsgClientToServer>,
    state: ServerStateAccess,
    send_data: bool,
) -> DTPSR<()> {
    let referrer = None;
    let query = HashMap::new();

    let ds = {
        let ss = state.lock().await;
        interpret_path(&path, &query, &referrer, &ss).await
    }?;
    let stream = ds.get_data_stream(path.as_str(), state.clone()).await?;

    handle_websocket_data_stream(ws_tx, stream, send_data, state.clone()).await
}

const DELTA_WEBSOCKET_AVAIL: f64 = 30.0;

pub async fn handle_websocket_data_stream(
    ws_tx: &mut SplitSink<warp::ws::WebSocket, warp::ws::Message>,
    data_stream: DataStream,
    send_data: bool,
    ssa: ServerStateAccess,
) -> DTPSR<()> {
    let mut starting_messaging = vec![];

    starting_messaging.push(MsgServerToClient::ChannelInfo(data_stream.channel_info));

    if let Some(first) = data_stream.first {
        let mut ss = ssa.lock().await;
        let mut for_this =
            get_series_of_messages_for_notification_(send_data, &first, DELTA_WEBSOCKET_AVAIL, &mut ss).await;

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
        match stream.recv().await {
            Ok(r) => {
                let for_this = match r {
                    ListenURLEvents::InsertNotification(r) => {
                        let mut ss = ssa.lock().await;

                        get_series_of_messages_for_notification_(send_data, &r, DELTA_WEBSOCKET_AVAIL, &mut ss).await
                    }
                    ListenURLEvents::WarningMsg(m) => {
                        vec![MsgServerToClient::WarningMsg(m)]
                    }
                    ListenURLEvents::ErrorMsg(m) => {
                        vec![MsgServerToClient::ErrorMsg(m)]
                    }
                    ListenURLEvents::FinishedMsg(m) => {
                        vec![MsgServerToClient::FinishedMsg(m)]
                    }
                    ListenURLEvents::SilenceMsg(m) => {
                        vec![MsgServerToClient::SilenceMsg(m)]
                    }
                };

                send_as_ws_cbor(&for_this, ws_tx).await?;
            }
            Err(e) => match e {
                RecvError::Closed => {
                    let msgs = vec![MsgServerToClient::FinishedMsg(FinishedMsg {
                        comment: "finished".to_string(),
                    })];
                    send_as_ws_cbor(&msgs, ws_tx).await?;
                    break;
                }
                RecvError::Lagged(n) => {
                    let msgs = vec![MsgServerToClient::WarningMsg(WarningMsg {
                        comment: format!("Skipped {n} messages."),
                    })];
                    send_as_ws_cbor(&msgs, ws_tx).await?;
                    continue;
                }
            },
        };
    }
    ws_tx.close().await?;
    Ok(())
}
//
// pub async fn handle_websocket_forwarded(
//     state: ServerStateAccess,
//     subscription: TopicName,
//     its_topic_name: TopicName,
//     ws_tx: &mut SplitSink<warp::ws::WebSocket, warp::ws::Message>,
//     _receiver: UnboundedReceiverStream<MsgClientToServer>,
//     send_data: bool,
// ) -> DTPSR<()> {
//     let url = {
//         let ss = state.lock().await;
//
//         let sub = ss.proxied.get(&subscription).unwrap();
//
//         match &sub.established {
//             None => {
//                 return not_available!("Subscription not established");
//             }
//             Some(est) => {
//                 let url = est.using.join(its_topic_name.as_relative_url())?;
//                 url
//             }
//         }
//     };
//
//     let md = get_metadata(&url).await?;
//
//     let use_url = if send_data {
//         md.events_data_inline_url.unwrap()
//     } else {
//         md.events_url.unwrap()
//     };
//
//     let wsc = open_websocket_connection(&use_url).await?;
//
//     let mut incoming = wsc.get_incoming().await;
//
//     loop {
//         let msg = match incoming.recv().await {
//             Ok(msg) => msg,
//             Err(e) => match e {
//                 RecvError::Closed => {
//                     break;
//                 }
//                 RecvError::Lagged(_) => {
//                     error_with_info!("lagged");
//                     continue;
//                 }
//             },
//         };
//         let x = warp_from_tungstenite(msg)?;
//         match ws_tx.send(x).await {
//             Ok(_) => {}
//             Err(e) => {
//                 // match e {
//                 //     Error { .. } => {}
//                 // }
//                 error_with_info!("Cannot send message: {e}")
//             }
//         }
//     }
//     Ok(())
// }
//w

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
            if v.is_empty() {
                continue;
            }
            if !all_comps.contains(&v) {
                all_comps.push(v);
            }
        }
    }
    for topic_vecs in all_comps.iter() {
        if topic_vecs.is_empty() {
            continue;
        }
        let mut url = String::new();
        for c in topic_vecs {
            url.push_str(c);
            url.push('/');
        }

        let topic_name = TopicName::from_components(topic_vecs).to_relative_url();
        topic2url.push((topic_name, url));
    }

    make_html(
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
    )
}
