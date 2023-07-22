use bytes::Bytes;
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::path::Path;
use std::string::ToString;
use std::sync::Arc as StdArc;

use chrono::Local;
use clap::Parser;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use indent::indent_all_with;
use log::{debug, error, info, warn};
use maud::PreEscaped;
use maud::{html, DOCTYPE};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_yaml;
use tokio::net::UnixListener;
use tokio::process::Command;
use tokio::signal::unix::SignalKind;
use tokio::spawn;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};
use tokio_stream::wrappers::{UnboundedReceiverStream, UnixListenerStream};
use tungstenite::http::{HeaderMap, HeaderValue, StatusCode};
use warp::http::header;
use warp::hyper::Body;
use warp::reply::Response;
use warp::{Filter, Rejection};

use crate::cloudflare::CloudflareTunnel;
use crate::constants::*;
use crate::html_utils::make_html;
use crate::logs::get_id_string;
use crate::master::{
    handle_websocket_generic2, put_common_headers, put_header_content_type, put_header_location,
    put_meta_headers, put_source_headers, serve_master_get, serve_master_head, serve_master_post,
};
use crate::object_queues::*;
use crate::server_state::*;
use crate::structures::*;
use crate::utils::divide_in_components;
use crate::websocket_signals::{
    ChannelInfo, ChannelInfoDesc, Chunk, MsgClientToServer, MsgServerToClient,
};
use crate::{
    error_other, error_with_info, format_digest_path, handle_rejection, not_available,
    parse_url_ext, utils, DTPSError, TopicName, DTPSR,
};

pub type HandlersResponse = Result<http::Response<hyper::Body>, Rejection>;
pub type ServerStateAccess = StdArc<TokioMutex<ServerState>>;

pub struct DTPSServer {
    pub listen_address: Option<SocketAddr>,
    pub mutex: ServerStateAccess,
    pub cloudflare_tunnel_name: Option<String>,
    pub cloudflare_executable: String,
    pub unix_path: Option<String>,
    initial_proxy: HashMap<String, TypeOfConnection>,
}

impl DTPSServer {
    pub async fn new(
        listen_address: Option<SocketAddr>,
        cloudflare_tunnel_name: Option<String>,
        cloudflare_executable: String,
        unix_path: Option<String>,
        initial_proxy: HashMap<String, TypeOfConnection>,
    ) -> DTPSR<Self> {
        let ss = ServerState::new(None)?;

        let mutex: ServerStateAccess = StdArc::new(TokioMutex::new(ss));

        Ok(DTPSServer {
            listen_address,
            mutex,
            cloudflare_tunnel_name,
            cloudflare_executable,
            unix_path,
            initial_proxy,
        })
    }
    pub async fn serve(&mut self) -> DTPSR<()> {
        // get current pid
        let pid = std::process::id();
        if pid == 1 {
            warn!(
                "WARNING: Running as PID 1. This is not recommended because CTRL-C may not work."
            );
            warn!("If running through `docker run`, use `docker run --init` to avoid this.")
        }

        let server_state_access: ServerStateAccess = self.mutex.clone();

        let clone_access = warp::any().map(move || server_state_access.clone());

        // let static_route = warp::path("static").and(warp::fs::dir(dir_to_pathbuf(STATIC_FILES)));
        let master_route_get = warp::path::full()
            .and(warp::query::<HashMap<String, String>>())
            .and(warp::get())
            .and(clone_access.clone())
            .and(warp::header::headers_cloned())
            .and_then(serve_master_get);

        let master_route_post = warp::path::full()
            .and(warp::query::<HashMap<String, String>>())
            .and(warp::post())
            .and(clone_access.clone())
            .and(warp::header::headers_cloned())
            .and(warp::body::bytes())
            .and_then(serve_master_post);

        let master_route_head = warp::path::full()
            .and(warp::query::<HashMap<String, String>>())
            .and(warp::head())
            .and(clone_access.clone())
            .and(warp::header::headers_cloned())
            .and_then(serve_master_head);

        let topic_generic_events_route2 = warp::path::full()
            .and_then(|path: warp::path::FullPath| async move {
                let path1 = path.as_str();
                let segments: Vec<String> = divide_in_components(path1, '/');
                let last = segments.last();
                if last == Some(&EVENTS_SUFFIX.to_string()) {
                    let start = path.as_str().rfind(EVENTS_SUFFIX).unwrap();
                    let part = &path.as_str()[..start];
                    // debug!("websocket raw {path1:?} -> {part:?}");
                    Ok(part.to_string())
                } else {
                    Err(warp::reject::not_found()) // should be not_available
                }
            })
            .and(clone_access.clone())
            .and(warp::query::<EventsQuery>())
            .and(warp::ws())
            .map({
                move |path: String, state1: ServerStateAccess, q: EventsQuery, ws: warp::ws::Ws| {
                    let send_data = match q.send_data {
                        Some(x) => x != 0,
                        None => false,
                    };
                    let path = path.to_string();
                    ws.on_upgrade(move |socket| {
                        handle_websocket_generic2(path, socket, state1, send_data)
                    })
                }
            });

        let the_routes = topic_generic_events_route2
            .or(master_route_head)
            .or(master_route_get)
            .or(master_route_post)
            .recover(handle_rejection);
        // .or(static_route)
        // .or(static_route_empty)
        // .or(static_route2)
        // .or(topic_generic_events_route)
        // .or(topic_generic_route_head)
        // .or(topic_generic_route_get)
        // .or(root_route)
        // .or(topic_generic_route_data)
        // .or(topic_post);
        let mut handles = vec![];

        if let Some(address) = self.listen_address {
            let tcp_server = warp::serve(the_routes.clone()).run(address);

            {
                let mut s = self.mutex.lock().await;

                info!("Listening on port {}", address.port());

                let s1 = format!("http://localhost:{}{}", address.port(), "/");
                s.add_advertise_url(&s1)?;

                let s2 = address.to_string().clone();
                if !s2.contains("0.0.0.0") {
                    s.add_advertise_url(&s2)?;
                }

                for host in platform::get_other_addresses() {
                    let x = format!("http://{}:{}{}", host, address.port(), "/");
                    s.add_advertise_url(&x)?;
                }

                use crate::platform;
                use gethostname::gethostname;
                let hostname = gethostname();
                let hostname = hostname.to_string_lossy();
                let x = format!("http://{}:{}{}", hostname, address.port(), "/");
                s.add_advertise_url(&x)?;
            }

            handles.push(tokio::spawn(tcp_server));
        }

        // if tunnel is given ,start
        if let (Some(tunnel_file), Some(address)) =
            (&self.cloudflare_tunnel_name, self.listen_address)
        {
            let port = address.port();
            // open the file as json and get TunnelID
            let contents = std::fs::File::open(tunnel_file).expect("file not found");
            let tunnel_info: CloudflareTunnel =
                serde_json::from_reader(contents).expect("error while reading file");
            let hoststring_127 = format!("127.0.0.1:{}", port);
            let cmdline = [
                "tunnel",
                "run",
                "--protocol",
                "http2",
                // "--no-autoupdate",
                "--cred-file",
                &tunnel_file,
                "--url",
                &hoststring_127,
                &tunnel_info.TunnelID,
            ];
            info!("starting tunnel: {:?}", cmdline);

            let mut child = Command::new(&self.cloudflare_executable)
                .args(cmdline)
                // .stdout(Stdio::piped())
                .spawn()
                .expect("Failed to start ping process");

            let handle = spawn(async move {
                let output = child.wait().await;
                error!("tunnel exited: {:?}", output);
            });
            handles.push(handle);
        }

        let ssa = self.mutex.clone();
        let node_id = self.get_node_id().await;
        let socketanme = format!("dtps-{}-rust.sock", node_id);
        let unix_path = env::temp_dir().join(socketanme);

        let mut unix_paths: Vec<String> = vec![unix_path.to_str().unwrap().to_string()];
        match &self.unix_path {
            Some(x) => {
                unix_paths.push(x.clone());
            }
            None => {}
        }

        for (i, unix_path) in unix_paths.iter().enumerate() {
            let the_path = Path::new(&unix_path);
            warn!("Opening socket on: {the_path:?}");
            let dirname = the_path.parent().unwrap();
            if !dirname.exists() {
                warn!("Creating dirname: {dirname:?}");
                std::fs::create_dir_all(dirname).unwrap();
            }
            // remove the socket if it exists
            if the_path.exists() {
                warn!("removing existing socket: {:?}", unix_path);
                std::fs::remove_file(&unix_path).unwrap();
            }

            let listener = match UnixListener::bind(&unix_path) {
                Ok(l) => l,

                Err(e) => {
                    error_with_info!("error binding to unix socket {}: {:?}", unix_path, e);
                    error_with_info!("note that this is not supported on Docker+OS X");
                    if i == 0 {
                        // this is our default
                        continue;
                    } else {
                        error_with_info!("Returning error because this was specified by user.");
                        return Err(e.into());
                    }
                }
            };

            let stream = UnixListenerStream::new(listener);
            let handle = spawn(warp::serve(the_routes.clone()).run_incoming(stream));
            // info!("Listening on {:?}", unix_path);
            let unix_url = format!("http+unix://{}/", unix_path.replace("/", "%2F"));
            {
                let mut s = ssa.lock().await;
                s.info(format!("Listening on {:?}", unix_path));
                s.add_advertise_url(&unix_url.to_string())?;
            }
            handles.push(handle);
        }
        // let (shutdown_send, shutdown_recv) = mpsc::unbounded_channel();

        spawn(clock_go(
            self.get_lock(),
            TopicName::from_relative_url(TOPIC_LIST_CLOCK)?,
            1.0,
        ));

        if self.initial_proxy.len() > 0 {
            // let s = ssa.lock().await;
            let mut i = 0;
            for (k, v) in self.initial_proxy.clone() {
                let subname = format!("proxy-{}", i);
                let mounted_at = TopicName::from_relative_url(&k)?;
                self.add_proxied(&subname, &mounted_at, v.clone()).await?;

                i += 1;
            }
            info!("Proxies started");
        }
        {
            let ss = ssa.lock().await;
            info!(
                "Server started. Advertised URLs: \n{}\n",
                indent_all_with(" ", ss.get_advertise_urls().join("\n"))
            );
        }

        let mut sig_hup = tokio::signal::unix::signal(SignalKind::hangup())?;
        let mut sig_term = tokio::signal::unix::signal(SignalKind::terminate())?;
        let mut sig_int = tokio::signal::unix::signal(SignalKind::interrupt())?;

        let pid = std::process::id();
        // s.info(format!("PID: {}", pid));
        info!("PID: {}", pid);
        let res: DTPSR<()>;
        // let first = handles.get_mut(0).unwrap();
        // first.await;
        tokio::select! {

            _ = sig_int.recv() => {
                info!("SIGINT received");
                    res = Err(DTPSError::Interrupted);
            },
            _ = sig_hup.recv() => {
                info!("SIGHUP received: gracefully shutting down");
                res = Ok(());
            },
            _ = sig_term.recv() => {
                info!("SIGTERM received: gracefully shutting down");
                res = Ok(());

            },
                // _ = futures::future::join_all(&handles) => {
                //     info!("shutdown received");
                //     // return Err("shutdown received".into());
                // },
        }
        // cancel all the handles
        for handle in handles {
            handle.abort();
        }

        // let results = futures::future::join_all(handles).await;
        //
        // for result in results {
        //     match result {
        //         Ok(value) => println!("Got: {:?}", value),
        //         Err(e) => eprintln!("Error: {:?}", e),
        //     }
        // };
        res
    }

    pub fn get_lock(&self) -> ServerStateAccess {
        self.mutex.clone()
    }
    pub async fn get_node_id(&self) -> String {
        let lock = self.get_lock();
        let ss = lock.lock().await;
        ss.node_id.clone()
    }

    pub async fn add_proxied(
        &mut self,
        subcription_name: &String,
        mounted_at: &TopicName,
        url: TypeOfConnection,
    ) -> DTPSR<()> {
        // let (md, index_internal) = loop {
        //     match get_proxy_info(&url).await {
        //         Ok(s) => break s,
        //         Err(e) => {
        //             error_with_info!(
        //                 "add_proxied: error getting proxy info for proxied {:?} at {}: \n {}",
        //                 proxied_name, url, e
        //             );
        //             info!("add_proxied: retrying in 2 seconds");
        //             sleep(std::time::Duration::from_secs(2)).await;
        //             continue;
        //         }
        //     }
        // };
        //
        // let index_internal =
        //     loop {
        //         match (
        //             md = get_metadata(&url).await
        //                 .with_context(|| {
        //                     format!(
        //                         "Error getting metadata for proxied {:?} at {}",
        //                         proxied_name, url
        //                     )
        //                 })?;
        //
        //             debug!("add_proxied: md:\n{:#?}", md);
        //             get_index(&url).await
        //         ) {}
        //
        //         //     .or_else(
        //         //     |e| {
        //         //         error_with_info!("add_proxied: error getting index for proxied {:?} at {}: \n {}", proxied_name, url, e);
        //         //         Err(e)
        //         //     }
        //         // )?;
        //
        //         // sleep 5 seconds
        //         tokio::time::sleep(Duration::from_secs(5)).await;
        //     }
        //     ;

        // let md = match get_metadata(&url).await {
        //     Ok(md) => md,
        //     Err(e) => {
        //         return DTPSError::not_reachable(format!(
        //             "Error getting metadata for proxied {:?} at {}: \n {}",
        //             proxied_name, url, e
        //         ));
        //     }
        // };

        let ssa = self.get_lock();

        // let proxied_name = TopicName::from_dotted(proxied_name);
        // let subcription_name =format!("{}-{}", proxied_name, "proxy").to_string();
        let future = observe_proxy(
            subcription_name.clone(),
            mounted_at.clone(),
            url.clone(),
            ssa,
        );
        let _handle = tokio::spawn(show_errors(
            format!("observe_proxy {subcription_name} : {url}"),
            future,
        ));

        //
        // self.proxied.insert(
        //     proxied_name.clone(),
        //     ForwardInfo {
        //         url: url.clone(),
        //         md,
        //         index_internal,
        //         handle,
        //     },
        // );
        // debug!("add_proxied: done, now:\n{:#?}", self.proxied);
        // self.update_my_topic()?;

        // add_context!(std::fs::read_to_string("my_file.txt"), "Failed to read file");

        Ok(())
    }
}

// async fn check_exists(
//     topic_name: String,
//     ss_mutex: ServerStateAccess,
// ) -> Result<String, Rejection> {
//     let ss = ss_mutex.lock().await;
//
//     match ss.oqs.get(topic_name.as_str()) {
//         None => Err(warp::reject::not_found()),
//         // Some(_) => Err(warp::reject::not_found()),
//         Some(_) => Ok(topic_name),
//     }
// }

pub fn get_accept_header(headers: &HeaderMap) -> Vec<String> {
    let accept_header = headers.get("accept");
    match accept_header {
        Some(x) => {
            let accept_header = x.to_str().unwrap();
            let accept_header = accept_header
                .split(",")
                .map(|x| x.trim().to_string())
                .collect();
            accept_header
        }
        None => vec![],
    }
}

pub async fn root_handler(ss_mutex: ServerStateAccess, headers: HeaderMap) -> HandlersResponse {
    let ss = ss_mutex.lock().await;
    let index_internal = ss.create_topic_index();
    let index = index_internal.to_wire(None);

    let accept_headers: Vec<String> = get_accept_header(&headers);

    // check if 'text/html' in the accept header

    if accept_headers.contains(&"text/html".to_string()) {
        let mut keys: Vec<&str> = index.topics.keys().map(|k| k.as_str()).collect();

        keys.sort();
        let mut topic2url = Vec::new();

        for topic_name in keys.iter() {
            if topic_name == &"" {
                continue;
            }
            let mut url = String::new();
            let components = divide_in_components(topic_name, '.');
            for c in components {
                url.push_str(&c);
                url.push_str("/");
            }
            topic2url.push((topic_name, url));
        }

        let x = html! {
            (DOCTYPE)

            html {
                head {
                      link rel="icon" type="image/png" href="/static/favicon.png" ;
                    link rel="stylesheet" href="/static/style.css" ;
                    title { "DTPS Server" }
                }
                body {
            h1 { "DTPS Server" }
            p.intro {
                "This response coming to you in HTML because you requested it like this."
            }
            p {
                "Node ID: " code {(ss.node_id)}
            }
            p {
                "Node app data:"
            }

            h2 { "Topics" }
            ul {
                @for (topic_name, url)  in topic2url.iter() {

                    li { a href={(url)} { code {(topic_name)} }}
                }
            }

            h2 { "Index answer presented in YAML" }

            pre {
                code {
                    (serde_yaml::to_string(&index).unwrap())
                }
            }}}
        };
        let markup = x.into_string();
        let body = Body::from(markup);
        let mut resp = Response::new(body);
        let headers = resp.headers_mut();
        put_header_content_type(headers, "text/html");

        put_alternative_locations(&ss, headers, "");

        Ok(resp)
    } else {
        let data_bytes = serde_cbor::to_vec(&index).unwrap();

        let mut resp = Response::new(Body::from(data_bytes));
        let headers = resp.headers_mut();
        put_header_content_type(headers, "application/cbor");

        put_common_headers(&ss, headers);
        put_alternative_locations(&ss, headers, "");

        Ok(resp)
    }
}

pub async fn handle_websocket_queue(
    ws_tx: &mut SplitSink<warp::ws::WebSocket, warp::ws::Message>,
    // ws_rx: SplitStream<warp::ws::WebSocket>,
    state: ServerStateAccess,
    topic_name: TopicName,
    send_data: bool,
) -> DTPSR<()> {
    // debug!("handle_websocket_queue: {}", topic_name);

    let channel_info_message: MsgServerToClient;
    let mut rx2: Receiver<usize>;
    {
        // important: release the lock
        let ss0 = state.lock().await;

        let oq: &ObjectQueue;
        match ss0.oqs.get(&topic_name) {
            None => {
                // TODO: we shouldn't be here
                // ws_tx.close().await.unwrap();
                return not_available!("topic not found");
            }
            Some(y) => oq = y,
        }
        let num_total;
        let newest;
        let oldest;
        match oq.sequence.last() {
            None => {
                num_total = 0;
                newest = None;
            }
            Some(last) => {
                let the_availability = vec![ResourceAvailabilityWire {
                    url: format_digest_path(&last.digest, &last.content_type),
                    available_until: utils::epoch() + 60.0,
                }];
                let nchunks = if send_data { 1 } else { 0 };
                let dr = DataReady {
                    unique_id: oq.tr.unique_id.clone(),
                    origin_node: oq.tr.origin_node.clone(),
                    sequence: last.index,
                    time_inserted: last.time_inserted,
                    digest: last.digest.clone(),
                    content_type: last.content_type.clone(),
                    content_length: last.content_length.clone(),
                    clocks: last.clocks.clone(),
                    availability: the_availability,
                    chunks_arriving: nchunks,
                };
                let m = MsgServerToClient::DataReady(dr);
                let message = warp::ws::Message::binary(serde_cbor::to_vec(&m).unwrap());
                ws_tx.send(message).await.unwrap();

                if send_data {
                    let message_data = ss0.get_blob(&last.digest).unwrap();
                    let message = warp::ws::Message::binary(message_data.clone());
                    ws_tx.send(message).await.unwrap();
                }
                num_total = last.index + 1;
                newest = Some(ChannelInfoDesc::new(last.index, last.time_inserted));
            }
        }

        match oq.sequence.first() {
            None => {
                oldest = None;
            }
            Some(first) => {
                oldest = Some(ChannelInfoDesc::new(first.index, first.time_inserted));
            }
        }

        let c = ChannelInfo {
            queue_created: oq.tr.created,
            num_total,
            newest,
            oldest,
        };
        channel_info_message = MsgServerToClient::ChannelInfo(c);

        rx2 = oq.tx.subscribe();
    }
    // let state2_for_receive = state.clone();
    // let topic_name2 = topic_name.clone();

    let message = warp::ws::Message::binary(serde_cbor::to_vec(&channel_info_message).unwrap());
    match ws_tx.send(message).await {
        Ok(_) => {}
        Err(e) => {
            debug!("Error sending ChannelInfo message: {}", e);
        }
    }

    // now wait for one message at least

    loop {
        let r = rx2.recv().await;
        let message;
        match r {
            Ok(_message) => message = _message,
            Err(RecvError::Closed) => break,
            Err(RecvError::Lagged(_)) => {
                debug!("Lagged!");
                continue;
            }
        }

        let ss2 = state.lock().await;
        let oq2 = ss2.oqs.get(&topic_name).unwrap();
        let this_one: &DataSaved = oq2.sequence.get(message).unwrap();
        let the_availability = vec![ResourceAvailabilityWire {
            url: format_digest_path(&this_one.digest, &this_one.content_type),
            available_until: utils::epoch() + 60.0,
        }];
        let nchunks = if send_data { 1 } else { 0 };
        let dr2 = DataReady {
            origin_node: oq2.tr.origin_node.clone(),
            unique_id: oq2.tr.unique_id.clone(),
            sequence: this_one.index,
            time_inserted: this_one.time_inserted,
            digest: this_one.digest.clone(),
            content_type: this_one.content_type.clone(),
            content_length: this_one.content_length,
            clocks: this_one.clocks.clone(),
            availability: the_availability,
            chunks_arriving: nchunks,
        };

        let out_message: MsgServerToClient = MsgServerToClient::DataReady(dr2);

        let message = warp::ws::Message::binary(serde_cbor::to_vec(&out_message).unwrap());
        match ws_tx.send(message).await {
            Ok(_) => {}
            Err(e) => {
                debug!("Error sending DataReady message: {}", e);
                break; // TODO: do better handling
            }
        }
        if send_data {
            let content = ss2.get_blob(&this_one.digest).unwrap();
            let chunk = MsgServerToClient::Chunk(Chunk {
                digest: this_one.digest.clone(),
                i: 0,
                n: nchunks,
                index: 0,
                data: Bytes::from(content.clone()),
            });
            let message = warp::ws::Message::binary(serde_cbor::to_vec(&chunk).unwrap());
            // let message = warp::ws::Message::binary(content.clone());
            match ws_tx.send(message).await {
                Ok(_) => {}
                Err(e) => {
                    debug!("Error sending binary data message: {}", e);
                    break; // TODO: do better handling
                }
            }
        }
    }
    Ok(())
    // debug!("handle_websocket_queue: {} - done", topic_name);
}

pub fn get_header_with_default(headers: &HeaderMap, key: &str, default: &str) -> String {
    return match headers.get(key) {
        None => default.to_string(),
        Some(v) => v.to_str().unwrap().to_string(),
    };
}

pub async fn handle_topic_post(
    topic_name: &TopicName,
    ss_mutex: ServerStateAccess,
    headers: HeaderMap,
    data: hyper::body::Bytes,
) -> HandlersResponse {
    let mut ss = ss_mutex.lock().await;
    // let topic_name= TopicName::from_dotted(topic_name);

    let content_type = get_header_with_default(&headers, CONTENT_TYPE, OCTET_STREAM);

    // let x: &mut ObjectQueue;
    // match ss.oqs.get_mut(topic_name.as_str()) {
    //     None => {
    //         return Err(warp::reject::not_found());
    //     }
    //     Some(y) => x = y,
    // };

    let byte_vector: Vec<u8> = data.to_vec().clone();

    let ds = ss.publish(&topic_name, &byte_vector, &content_type, None)?;

    // convert to cbor
    let ds_cbor = serde_cbor::to_vec(&ds).unwrap();
    let res = http::Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/cbor")
        .body(Body::from(ds_cbor))
        .unwrap();
    return Ok(res);
}

async fn handler_topic_html_summary(
    topic_name: &TopicName,
    ss_mutex: ServerStateAccess,
) -> Result<http::Response<Body>, Rejection> {
    let ss = ss_mutex.lock().await;

    let x: &ObjectQueue;
    match ss.oqs.get(topic_name) {
        None => return Err(warp::reject::not_found()),

        Some(y) => x = y,
    }

    let (default_content_type, initial_value) = match x.sequence.last() {
        None => ("application/yaml".to_string(), "{}".to_string()),
        Some(l) => {
            let digest = &l.digest;
            let content = match ss.get_blob(digest) {
                None => {
                    return Err(warp::reject::not_found());
                }
                Some(c) => c,
            };
            let initial = match l.content_type.as_str() {
                "application/yaml" | "application/json" => {
                    String::from_utf8(content.to_vec()).unwrap()
                }
                _ => "{}".to_string(),
            };
            (l.content_type.clone(), initial)
        }
    };
    let now = Local::now().timestamp_nanos();

    let format_elapsed = |a| -> String { utils::format_nanos(now - a) };

    let data_or_digest = |data: &DataSaved| -> PreEscaped<String> {
        let printable = match data.content_type.as_str() {
            "application/yaml" | "application/x-yaml" | "text/yaml" | "text/vnd.yaml"
            | "application/json" => true,
            _ => false,
        };
        let url = format_digest_path(&data.digest, &data.content_type);

        if data.content_length <= data.digest.len() {
            if printable {
                let rd = ss.get_blob(&data.digest).unwrap();
                let s = String::from_utf8(rd.clone()).unwrap();
                html! {
                  (s)
                }
            } else {
                let s = format!("{} bytes", data.content_length);
                html! { a href=(url) { (s) } }
            }
        } else {
            html! { a href=(url) { (data.digest.clone()) } }
        }
    };
    let mut latencies: Vec<i64> = vec![];
    for (i, data) in x.sequence.iter().enumerate() {
        if i > 0 {
            latencies.push(data.time_inserted - x.sequence[i - 1].time_inserted);
        } else {
            latencies.push(0);
        }
    }

    let x = make_html(
        &topic_name.as_relative_url(),
        html! {


            p { "This response coming to you in HTML because you requested it."}
            p { "Origin ID: " code {(x.tr.origin_node)} }
            p { "Topic ID: " code {(x.tr.unique_id)} }

            div  {
                h3 { "notifications using websockets"}
                div {pre   id="result" { "result will appear here" }}
            }
            div {
                h3 {"push JSON to queue"}

                textarea id="myTextAreaContentType" { (default_content_type) };
                textarea id="myTextArea" { (initial_value) };

                br;

                button id="myButton" { "push" };

                script src="!/static/send.js" {};

                script type="text/javascript" { (PreEscaped(JAVASCRIPT_SEND)) };
            } // div

            h2 { "Queue" }

            table {
                thead {
                    tr {
                        th { "Sequence" }
                        th { "Elapsed" }
                        th { "Delta" }
                        th { "Content Type" }
                        th { "Length" }
                        th { "Digest or data" }
                    }
                }
                tbody {
                    @for (i, data) in x.sequence.iter().enumerate().rev().take(100) {
                        tr {
                            td { (data.index) }
                            td { (format_elapsed(data.time_inserted)) }
                            td { (utils::format_nanos(latencies[i]))}
                            td { code {(data.content_type)} }
                            td { (data.content_length) }
                            td { code { (data_or_digest(data)) } }
                        }
                    }
                }
            } // table


        },
    );
    let markup = x.into_string();

    Ok(http::Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(markup))
        .unwrap())
}

// language=javascript
pub const JAVASCRIPT_SEND: &str = r##"


"##;

pub async fn handler_topic_generic(
    topic_name: &TopicName,
    ss_mutex: ServerStateAccess,
    headers: HeaderMap,
) -> Result<http::Response<Body>, Rejection> {
    let accept_headers: Vec<String> = get_accept_header(&headers);

    if accept_headers.contains(&"text/html".to_string()) {
        return handler_topic_html_summary(topic_name, ss_mutex).await;
    };

    // check if the client is requesting an HTML page

    let digest: String;
    let content_type: String;
    {
        let ss = ss_mutex.lock().await;

        let x: &ObjectQueue;
        match ss.oqs.get(topic_name) {
            None => return Err(warp::reject::not_found()),

            Some(y) => x = y,
        }

        // get the last element in the vector
        match x.sequence.last() {
            None => {
                let mut res = http::Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .body(Body::from(""))
                    .unwrap();
                let h = res.headers_mut();

                // let suffix = format!("{}/", topic_name);
                put_alternative_locations(&ss, h, &topic_name.as_relative_url());
                put_common_headers(&ss, h);

                return Ok(res);
            }

            Some(y) => {
                digest = y.digest.clone();
                content_type = y.content_type.clone();
            }
        }
    }
    return handler_topic_generic_data(topic_name, content_type, digest, ss_mutex.clone(), headers)
        .await;
}

async fn handler_topic_generic_data(
    topic_name: &TopicName,
    content_type: String,
    digest: String,
    ss_mutex: ServerStateAccess,
    headers: HeaderMap,
) -> Result<Response, Rejection> {
    let accept_headers: Vec<String> = get_accept_header(&headers);

    let ss = ss_mutex.lock().await;

    let x: &ObjectQueue;
    match ss.oqs.get(topic_name) {
        None => return Err(warp::reject::not_found()),
        Some(y) => x = y,
    }

    let content = match ss.get_blob(&digest) {
        None => return Err(warp::reject::not_found()),
        Some(y) => y,
    };
    let data_bytes = content.clone();

    if accept_headers.contains(&"text/html".to_string()) {
        let display = match content_type.as_str() {
            "application/yaml" => String::from_utf8(data_bytes).unwrap().to_string(),
            "application/json" => {
                let val: serde_json::Value = serde_json::from_slice(&data_bytes).unwrap();
                let pretty = serde_json::to_string_pretty(&val).unwrap();
                pretty
            }
            "application/cbor" => {
                let val: serde_cbor::Value = serde_cbor::from_slice(&data_bytes).unwrap();
                match serde_yaml::to_string(&val) {
                    Ok(x) => format!("CBOR displayed as YAML:\n\n{}", x),
                    Err(e) => format!("Cannot format CBOR as YAML: {}\nRaw CBOR:\n{:?}", e, val),
                }
            }
            _ => format!("Cannot display content type {}", content_type).to_string(),
        };
        let x = html! {
            (DOCTYPE)

            html {
                head {
                      link rel="icon" type="image/png" href="/static/favicon.png" ;
                    link rel="stylesheet" href="/static/style.css" ;
                    title { (digest) }
                }
                body {
             p { "This data is presented as HTML because you requested it as such."}
                     p { "Content type: " code { (content_type) } }
                     p { "Content length: " (content.len()) }

            pre {
                code {
                             (display)
                    // (serde_yaml::to_string(&index).unwrap())
                }
            }}}
        };

        let markup = x.into_string();
        let mut resp = Response::new(Body::from(markup));
        let headers = resp.headers_mut();
        put_header_content_type(headers, "text/html");
        put_alternative_locations(&ss, headers, "");
        Ok(resp)
        // return Ok(with_status(resp, StatusCode::OK));
    } else {
        let mut resp = Response::new(Body::from(data_bytes));
        let h = resp.headers_mut();
        put_alternative_locations(&ss, h, &topic_name.as_relative_url());
        put_source_headers(h, &x.tr.origin_node, &x.tr.unique_id);
        put_meta_headers(h);
        put_common_headers(&ss, h);
        put_header_content_type(h, &content_type);
        Ok(resp.into())
    }
}

pub fn put_alternative_locations(
    ss: &ServerState,
    headers: &mut HeaderMap<HeaderValue>,
    suffix: &str,
) {
    for x in ss.get_advertise_urls().iter() {
        let x_suff = format!("{}{}", x, suffix);
        put_header_content_type(headers, "text/html");
        put_header_location(headers, &x_suff);
    }
}

#[derive(Serialize, Deserialize)]
struct EventsQuery {
    send_data: Option<u8>,
}

const DEFAULT_PORT: u16 = 8000;
const DEFAULT_HOST: &str = "0.0.0.0";
const DEFAULT_CLOUDFLARE_EXECUTABLE: &str = "cloudflared";

/// DTPS HTTP server
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct ServerArgs {
    /// TCP Port to bind to
    #[arg(long, default_value_t = DEFAULT_PORT)]
    tcp_port: u16,

    /// Hostname to bind to
    #[arg(long, default_value_t = DEFAULT_HOST.to_string())]
    tcp_host: String,

    /// Optional UNIX path to listen to
    #[arg(long)]
    unix_path: Option<String>,

    /// Cloudflare tunnel to start
    #[arg(long)]
    tunnel: Option<String>,

    /// Cloudflare executable filename
    #[arg(long, default_value_t = DEFAULT_CLOUDFLARE_EXECUTABLE.to_string())]
    cloudflare_executable: String,

    #[arg(long)]
    proxy: Vec<String>,
}

pub fn address_from_host_port(host: &str, port: u16) -> DTPSR<SocketAddr> {
    let hoststring = format!("{}:{}", host, port);

    let s: Result<SocketAddr, _> = hoststring.parse();
    match s {
        Ok(x) => return Ok(x),
        Err(e) => {
            debug!("Cannot parse {}: {}", hoststring, e);
            error_other(format!("Cannot parse {}: {}", hoststring, e))
        }
    }
}

pub async fn create_server_from_command_line() -> DTPSR<DTPSServer> {
    let args = ServerArgs::parse();

    let listen_address = if args.tcp_port > 0 {
        Some(address_from_host_port(
            args.tcp_host.as_str(),
            args.tcp_port,
        )?)
    } else {
        None
    };
    let mut proxy: HashMap<String, TypeOfConnection> = HashMap::new();
    for p in args.proxy.iter() {
        // parse name=value
        let parts = p.splitn(2, '=').collect::<Vec<_>>();

        if parts.len() != 2 {
            return error_other(format!("Invalid proxy specification: {}", p));
        }
        let name = parts.get(0).unwrap();
        let value = parts.get(1).unwrap();
        let url = parse_url_ext(value)?;
        proxy.insert(name.to_string(), url);
    }
    if proxy.len() != 0 {
        debug!("Proxy:\n{:#?}", proxy);
    }
    DTPSServer::new(
        listen_address,
        args.tunnel.clone(),
        args.cloudflare_executable.clone(),
        args.unix_path,
        proxy,
    )
    .await
}

async fn clock_go(state: ServerStateAccess, topic_name: TopicName, interval_s: f32) {
    let mut clock = interval(Duration::from_secs_f32(interval_s));
    clock.tick().await;
    loop {
        clock.tick().await;
        let mut ss = state.lock().await;
        // let datetime_string = Local::now().to_rfc3339();
        // get the current time in nanoseconds
        let now = Local::now().timestamp_nanos();
        let s = format!("{}", now);
        let _inserted = ss.publish_json(&topic_name, &s, None);

        // debug!("inserted {}: {:?}", topic_name, inserted);
    }
}

pub fn receive_from_websocket<T: DeserializeOwned + Clone + Send + 'static>(
    ws_rx: SplitStream<warp::ws::WebSocket>,
) -> (UnboundedReceiverStream<T>, JoinHandle<()>) {
    let (tx, rx) = mpsc::unbounded_channel();

    let handle = tokio::spawn(show_errors("websocket".to_string(), pull_(ws_rx, tx)));

    let stream = UnboundedReceiverStream::new(rx);
    (stream, handle)
}

pub async fn pull_<T: DeserializeOwned + Clone + Send>(
    mut ws_rx: SplitStream<warp::ws::WebSocket>,
    tx: UnboundedSender<T>,
) -> DTPSR<()> {
    loop {
        match ws_rx.next().await {
            None => {
                // debug!("ws_rx.next() returned None");
                // finished = true;
                break;
            }
            Some(Ok(msg)) => {
                if msg.is_binary() {
                    let raw_data = msg.as_bytes().to_vec().clone();
                    // let v: serde_cbor::Value = serde_cbor::from_slice(&raw_data).unwrap();
                    //
                    // debug!("ws_rx.next() returned {:#?}", v);
                    //

                    let ms: T = match serde_cbor::from_slice(raw_data.as_slice()) {
                        Ok(x) => x,
                        Err(err) => {
                            debug!("ws_rx.next() cannot interpret error {:#?}", err);
                            continue;
                        }
                    };
                    // let c = serde_cbor::to_vec(&ms).unwrap();
                    tx.send(ms.clone()).unwrap();
                    // debug!("ws_rx.next() returned {:#?}", ms);
                    // match ms {
                    //     MsgClientToServer::RawData(rd) => {
                    //         let mut ss0 = ss_mutex.lock().await;
                    //
                    //         // let oq: &ObjectQueue=  ss0.oqs.get(topic_name.as_str()).unwrap();
                    //
                    //         let _ds =
                    //             ss0.publish(&topic_name, &rd.content, &rd.content_type, None);
                    //     }
                    // }
                }
            }
            Some(Err(err)) => {
                debug!("ws_rx.next() returned error {:#?}", err);
                // match err {
                //     Error { .. } => {}
                // }
            }
        }
    }
    Ok(())
}

pub async fn do_receiving(
    topic_name2: TopicName,
    ss_mutex: ServerStateAccess,
    mut receiver: UnboundedReceiverStream<MsgClientToServer>,
) {
    let topic_name = topic_name2.clone();
    loop {
        match receiver.next().await {
            None => {
                debug!("do_receiving: receiver.next() returned None");
                // finished = true;
                break;
            }
            Some(MsgClientToServer::RawData(rd)) => {
                let mut ss0 = ss_mutex.lock().await;

                // let oq: &ObjectQueue=  ss0.oqs.get(topic_name.as_str()).unwrap();

                let _ds = ss0.publish(&topic_name, &rd.content, &rd.content_type, None);
            }
        }
    }
}
