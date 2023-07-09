use std::collections::HashMap;
use std::env;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::Path;
use std::process::Command;
use std::string::ToString;
use std::sync::Arc;
use std::time::SystemTime;

use clap::Parser;
use futures::{SinkExt, StreamExt};
use log::{debug, info, warn};
use maplit::hashmap;
use maud::html;
use serde::{Deserialize, Serialize};
use serde_yaml;
use tokio::net::UnixListener;
use tokio::signal::unix::SignalKind;
use tokio::spawn;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnixListenerStream;
use tungstenite::http::{HeaderMap, HeaderValue, StatusCode};
use warp::http::header;
use warp::hyper::Body;
use warp::path::end as endbar;
use warp::reply::{with_status, Response};
use warp::{Filter, Rejection, Reply};

#[cfg(target_os = "linux")]
use getaddrs::InterfaceAddrs;

use crate::constants::*;
use crate::logs::get_id_string;
use crate::object_queues::*;
use crate::server_state::*;
use crate::structures::*;
use crate::types::*;

pub struct DTPSServer {
    pub listen_address: Option<SocketAddr>,
    pub mutex: Arc<Mutex<ServerState>>,
    pub cloudflare_tunnel_name: Option<String>,
    pub cloudflare_executable: String,
    pub unix_path: Option<String>,
}

impl DTPSServer {
    pub fn new(
        listen_address: Option<SocketAddr>,
        cloudflare_tunnel_name: Option<String>,
        cloudflare_executable: String,
        unix_path: Option<String>,
    ) -> Self {
        let ss = ServerState::new(None);
        let mutex = Arc::new(Mutex::new(ss));
        DTPSServer {
            listen_address,
            mutex,
            cloudflare_tunnel_name,
            cloudflare_executable,
            unix_path,
        }
    }
    pub async fn serve(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // get current pid
        let pid = std::process::id();
        if pid == 1 {
            warn!(
                "WARNING: Running as PID 1. This is not recommended because CTRL-C may not work."
            );
            warn!("If running through `docker run`, use `docker run --init` to avoid this.")
        }

        let server_state_access: Arc<Mutex<ServerState>> = self.mutex.clone();

        let clone_access = warp::any().map(move || server_state_access.clone());

        // root GET /
        let root_route = endbar()
            .and(clone_access.clone())
            .and(warp::header::headers_cloned())
            .and_then(root_handler);

        let topic_address = warp::path!("topics" / String).and(endbar());

        // GEt request
        let topic_generic_route_get = topic_address
            .and(warp::get())
            .and(clone_access.clone())
            .and_then(handler_topic_generic);

        // HEAD request
        let topic_generic_route_head = topic_address
            .and(warp::head())
            .and(clone_access.clone())
            .and_then(handler_topic_generic_head);

        // POST request
        let topic_post = topic_address
            .and(warp::post())
            .and(clone_access.clone())
            .and(warp::header::headers_cloned())
            .and(warp::body::bytes())
            .and_then(handle_topic_post);

        // GET request for data
        let topic_generic_route_data = warp::get()
            .and(warp::path!("topics" / String / "data" / String))
            .and(endbar())
            .and(clone_access.clone())
            .and_then(handler_topic_generic_data);

        // websockets for events
        let topic_generic_events_route = warp::path!("topics" / String / "events")
            .and(endbar())
            .and(clone_access.clone())
            .and_then(check_exists)
            .and(warp::query::<EventsQuery>())
            .and(warp::ws())
            .and(clone_access.clone())
            // .and_then(handle_websocket_generic_pre);
            .map({
                move |c: String,
                      q: EventsQuery,
                      ws: warp::ws::Ws,
                      state1: Arc<Mutex<ServerState>>| {
                    let send_data = match q.send_data {
                        Some(x) => x != 0,
                        None => false,
                    };
                    ws.on_upgrade(move |socket| {
                        handle_websocket_generic(socket, state1, c, send_data)
                    })
                }
            });

        let the_routes = topic_generic_events_route
            .or(topic_generic_route_head)
            .or(topic_generic_route_get)
            .or(root_route)
            .or(topic_generic_route_data)
            .or(topic_post);

        let mut handles = vec![];

        if let Some(address) = self.listen_address {
            let tcp_server = warp::serve(the_routes.clone()).run(address);

            {
                let mut s = self.mutex.lock().await;

                s.advertise_urls.push(address.to_string());

                get_other_addresses();
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

            let child = Command::new(&self.cloudflare_executable)
                .args(cmdline)
                // .stdout(Stdio::piped())
                .spawn()
                .expect("Failed to start ping process");

            debug!("Started process: {}", child.id());
        }

        // let lock = self.mutex.clone();
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

        for unix_path in unix_paths {
            let the_path = Path::new(&unix_path);
            // remove the socket if it exists
            if the_path.exists() {
                warn!("removing existing socket: {:?}", unix_path);
                std::fs::remove_file(&unix_path).unwrap();
            }

            let listener = UnixListener::bind(&unix_path).unwrap();

            let stream = UnixListenerStream::new(listener);
            let handle = spawn(warp::serve(the_routes.clone()).run_incoming(stream));
            info!("Listening on {:?}", unix_path);
            let unix_url = format!("http+unix://{}/", unix_path.replace("/", "%2F"));
            {
                let mut s = self.mutex.lock().await;
                s.advertise_urls.push(unix_url.to_string());
            }
            handles.push(handle);
        }
        // let (shutdown_send, shutdown_recv) = mpsc::unbounded_channel();

        let mut sig_hup = tokio::signal::unix::signal(SignalKind::hangup())?;
        let mut sig_term = tokio::signal::unix::signal(SignalKind::terminate())?;
        let mut sig_int = tokio::signal::unix::signal(SignalKind::interrupt())?;

        let pid = std::process::id();
        info!("PID: {}", pid);
        let res: Result<(), Box<dyn std::error::Error>>;
        tokio::select! {

            _ = sig_int.recv() => {
                info!("SIGINT received");
                    res = Err("SIGINT received".into());
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

    pub fn get_lock(&self) -> Arc<Mutex<ServerState>> {
        self.mutex.clone()
    }
    pub async fn get_node_id(&self) -> String {
        let lock = self.get_lock();
        let ss = lock.lock().await;
        ss.node_id.clone()
    }
}

async fn check_exists(
    topic_name: String,
    ss_mutex: Arc<Mutex<ServerState>>,
) -> Result<String, Rejection> {
    let ss = ss_mutex.lock().await;

    match ss.oqs.get(topic_name.as_str()) {
        None => Err(warp::reject::not_found()),
        // Some(_) => Err(warp::reject::not_found()),
        Some(_) => Ok(topic_name),
    }
}

fn get_accept_header(headers: &HeaderMap) -> Vec<String> {
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

async fn root_handler(
    ss_mutex: Arc<Mutex<ServerState>>,
    headers: HeaderMap,
) -> Result<impl Reply, Rejection> {
    let ss = ss_mutex.lock().await;
    let index = topics_index(&ss);

    let accept_headers: Vec<String> = get_accept_header(&headers);
    // check if 'text/html' in the accept header
    if accept_headers.contains(&"text/html".to_string()) {
        let x = html! {
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
                @for (topic_name, _topic) in index.topics.iter() {
                    li { a href={"topics/"  (topic_name) "/"} { code {(topic_name)} }}
                }
            }

            h2 { "Index answer presented in YAML" }

            pre {
                code {
                    (serde_yaml::to_string(&index).unwrap())
                }
            }
        };
        let markup = x.into_string();
        let mut resp = Response::new(Body::from(markup));
        let headers = resp.headers_mut();

        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/html"));

        put_alternative_locations(&ss, headers, "");
        Ok(with_status(resp, StatusCode::OK))
    } else {
        let data_bytes = serde_cbor::to_vec(&index).unwrap();

        let mut resp = Response::new(Body::from(data_bytes));
        let headers = resp.headers_mut();

        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/cbor"),
        );

        put_common_headers(&ss, headers);
        put_alternative_locations(&ss, headers, "");

        Ok(with_status(resp, StatusCode::OK))
    }
}

async fn handle_websocket_generic(
    ws: warp::ws::WebSocket,
    state: Arc<Mutex<ServerState>>,
    topic_name: String,
    send_data: bool,
) -> () {
    debug!("handle_websocket_generic: {}", topic_name);
    let (mut ws_tx, mut ws_rx) = ws.split();

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
                let the_availability = vec![ResourceAvailabilityWire {
                    url: format!("../data/{}/", last.digest),
                    available_until: epoch() + 60.0,
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

                let message = warp::ws::Message::binary(serde_cbor::to_vec(&dr).unwrap());
                ws_tx.send(message).await.unwrap();

                if send_data {
                    let message_data = oq.data.get(&last.digest).unwrap();
                    let message = warp::ws::Message::binary(message_data.content.clone());
                    ws_tx.send(message).await.unwrap();
                }
            }
        }

        rx2 = oq.tx.subscribe();
    }
    // let mut finished = false;
    tokio::spawn(async move {
        loop {
            match ws_rx.next().await {
                None => {
                    debug!("ws_rx.next() returned None");
                    // finished = true;
                    break;
                }
                Some(Ok(msg)) => {
                    debug!("ws_rx.next() returned {:#?}", msg);
                }
                Some(Err(err)) => {
                    debug!("ws_rx.next() returned error {:#?}", err);
                    // match err {
                    //     Error { .. } => {}
                    // }
                }
            }
        }
    });
    loop {
        // if finished {
        //     break;
        // }
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
        // debug!(
        //     "Received update for topic {}: index {}",
        //     topic_name, message
        // );
        let ss2 = state.lock().await;
        let oq2 = ss2.oqs.get(&topic_name).unwrap();
        let this_one: &DataSaved = oq2.sequence.get(message).unwrap();
        let the_availability = vec![ResourceAvailabilityWire {
            url: format!("../data/{}/", this_one.digest),
            available_until: epoch() + 60.0,
        }];
        let nchunks = if send_data { 1 } else { 0 };
        let dr2 = DataReady {
            sequence: this_one.index,
            digest: this_one.digest.clone(),
            content_type: this_one.content_type.clone(),
            content_length: this_one.content_length,
            availability: the_availability,
            chunks_arriving: nchunks,
        };

        let message = warp::ws::Message::binary(serde_cbor::to_vec(&dr2).unwrap());
        match ws_tx.send(message).await {
            Ok(_) => {}
            Err(e) => {
                debug!("Error sending message: {}", e);
                break; // TODO: do better handling
            }
        }
        if send_data {
            let message_data = oq2.data.get(&this_one.digest).unwrap();
            let message = warp::ws::Message::binary(message_data.content.clone());
            match ws_tx.send(message).await {
                Ok(_) => {}
                Err(e) => {
                    debug!("Error sending message: {}", e);
                    break; // TODO: do better handling
                }
            }
        }
    }
    debug!("handle_websocket_generic: {} - done", topic_name);
}

pub fn epoch() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

pub fn topics_index(ss: &ServerState) -> TopicsIndexWire {
    let mut topics: HashMap<TopicName, TopicRefWire> = hashmap! {};

    for (topic_name, oq) in ss.oqs.iter() {
        topics.insert(topic_name.clone(), oq.tr.to_wire());
    }

    let topics_index = TopicsIndexWire {
        node_id: ss.node_id.clone(),
        node_started: ss.node_started,
        node_app_data: ss.node_app_data.clone(),
        topics,
    };

    return topics_index;
}

pub fn get_header_with_default(headers: &HeaderMap, key: &str, default: &str) -> String {
    return match headers.get(key) {
        None => default.to_string(),
        Some(v) => v.to_str().unwrap().to_string(),
    };
}

async fn handle_topic_post(
    topic_name: String,
    ss_mutex: Arc<Mutex<ServerState>>,
    headers: HeaderMap,
    data: hyper::body::Bytes,
) -> Result<impl Reply, Rejection> {
    let mut ss = ss_mutex.lock().await;

    let content_type = get_header_with_default(&headers, CONTENT_TYPE, OCTET_STREAM);

    let x: &mut ObjectQueue;
    match ss.oqs.get_mut(topic_name.as_str()) {
        None => {
            return Err(warp::reject::not_found());
        }
        Some(y) => x = y,
    };

    let byte_vector: Vec<u8> = data.to_vec().clone();

    x.push_data(&content_type, &byte_vector);

    let ok = "ok";
    Ok(warp::reply::json(&ok))
}

async fn handler_topic_generic(
    topic_name: String,
    ss_mutex: Arc<Mutex<ServerState>>,
) -> Result<impl Reply, Rejection> {
    let digest: String;
    {
        let ss = ss_mutex.lock().await;

        let x: &ObjectQueue;
        match ss.oqs.get(topic_name.as_str()) {
            None => return Err(warp::reject::not_found()),

            Some(y) => x = y,
        }

        // get the last element in the vector
        match x.sequence.last() {
            None => {
                let resp = Response::new(Body::from(""));

                let (mut parts, body) = resp.into_parts();

                parts.status = StatusCode::NO_CONTENT;
                let mut resp = Response::from_parts(parts, body);

                let h = resp.headers_mut();

                let suffix = format!("topics/{}/", topic_name);
                put_alternative_locations(&ss, h, &suffix);
                put_common_headers(&ss, h);

                return Ok(resp.into());
            }

            Some(y) => digest = y.digest.clone(),
        }
        // let last = x.sequence.last().unwrap();
        // digest = last.digest.clone();
    }
    return handler_topic_generic_data(topic_name, digest, ss_mutex.clone()).await;
}

async fn handler_topic_generic_head(
    topic_name: String,
    ss_mutex: Arc<Mutex<ServerState>>,
) -> Result<Response, Rejection> {
    let ss = ss_mutex.lock().await;

    let x: &ObjectQueue;
    match ss.oqs.get(topic_name.as_str()) {
        None => return Err(warp::reject::not_found()),

        Some(y) => x = y,
    }
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

    h.insert(HEADER_SEE_EVENTS, HeaderValue::from_static("events/"));
    h.insert(
        HEADER_SEE_EVENTS_INLINE_DATA,
        HeaderValue::from_static("events/?send_data=1"),
    );

    let suffix = format!("topics/{}/", topic_name);
    put_alternative_locations(&ss, h, &suffix);
    put_common_headers(&ss, h);

    Ok(resp.into())
}

async fn handler_topic_generic_data(
    topic_name: String,
    digest: String,
    ss_mutex: Arc<Mutex<ServerState>>,
) -> Result<Response, Rejection> {
    let ss = ss_mutex.lock().await;

    let x: &ObjectQueue;
    match ss.oqs.get(topic_name.as_str()) {
        None => return Err(warp::reject::not_found()),
        Some(y) => x = y,
    }

    let data = x.data.get(&digest).unwrap();
    let data_bytes = data.content.clone();
    let content_type = data.content_type.clone();

    let mut resp = Response::new(Body::from(data_bytes));
    let h = resp.headers_mut();
    let suffix = format!("topics/{}/", topic_name);
    put_alternative_locations(&ss, h, &suffix);

    h.insert(
        HEADER_DATA_ORIGIN_NODE_ID,
        HeaderValue::from_str(x.tr.origin_node.as_str()).unwrap(),
    );
    h.insert(
        HEADER_DATA_UNIQUE_ID,
        HeaderValue::from_str(x.tr.unique_id.as_str()).unwrap(),
    );

    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(content_type.clone().as_str()).unwrap(),
    );
    // see events
    h.insert(HEADER_SEE_EVENTS, HeaderValue::from_static("events/"));
    h.insert(
        HEADER_SEE_EVENTS_INLINE_DATA,
        HeaderValue::from_static("events/?send_data=1"),
    );

    put_common_headers(&ss, resp.headers_mut());

    Ok(resp.into())
}

pub fn put_alternative_locations(
    ss: &ServerState,
    headers: &mut HeaderMap<HeaderValue>,
    suffix: &str,
) {
    for x in ss.advertise_urls.iter() {
        let x_suff = format!("{}{}", x, suffix);
        headers.insert(
            "Content-Location",
            HeaderValue::from_str(x_suff.as_str()).unwrap(),
        );
    }
}

pub fn put_common_headers(ss: &ServerState, headers: &mut HeaderMap<HeaderValue>) {
    headers.insert(
        header::SERVER,
        HeaderValue::from_str(get_id_string().as_str()).unwrap(),
    );
    headers.insert(
        HEADER_NODE_ID,
        HeaderValue::from_str(ss.node_id.as_str()).unwrap(),
    );
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
}

pub fn address_from_host_port(host: &str, port: u16) -> SocketAddr {
    let hoststring = format!("{}:{}", host, port);
    let mut addrs_iter = hoststring.to_socket_addrs().unwrap();
    let one_addr = addrs_iter.next().unwrap();
    one_addr
}

pub fn create_server_from_command_line() -> DTPSServer {
    let args = ServerArgs::parse();

    let listen_address = if args.tcp_port > 0 {
        Some(address_from_host_port(
            args.tcp_host.as_str(),
            args.tcp_port,
        ))
    } else {
        None
    };

    let server = DTPSServer::new(
        listen_address,
        args.tunnel.clone(),
        args.cloudflare_executable.clone(),
        args.unix_path,
    );

    server
}

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CloudflareTunnel {
    pub AccountTag: String,
    pub TunnelSecret: String,
    pub TunnelID: String,
    pub TunnelName: String,
}

#[cfg(target_os = "linux")]
fn get_other_addresses() -> Vec<String> {
    println!("You are running Linux!");

    let addrs = InterfaceAddrs::query_system().expect("System has no network interfaces.");

    for addr in addrs {
        debug!("{}: {:?}", addr.name, addr.address);
    }
    debug!("You are running Linux - using other addresses");
    return Vec::new();
}

#[cfg(not(target_os = "linux"))]
fn get_other_addresses() -> Vec<String> {
    debug!("You are not running Linux - ignoring other addresses");
    return Vec::new();
}
