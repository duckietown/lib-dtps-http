use std::collections::HashMap;
use std::env;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::Path;
use std::process::Command;
use std::string::ToString;
use std::sync::Arc;
use std::time::SystemTime;

use chrono::Local;
use clap::Parser;
use futures::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use maplit::hashmap;
use maud::PreEscaped;
use maud::{html, DOCTYPE};
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
use crate::static_files::{serve_static_file, STATIC_FILES};
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

        // let static_route = warp::path("static").and(warp::fs::dir(dir_to_pathbuf(STATIC_FILES)));
        let static_route = warp::path("static")
            .and(warp::path::tail())
            .and_then(serve_static_file);

        log::info!(" {:?}", STATIC_FILES);

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
            .and(warp::header::headers_cloned())
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
            .and(warp::header::headers_cloned())
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
            .or(topic_post)
            .or(static_route);

        let mut handles = vec![];

        if let Some(address) = self.listen_address {
            let tcp_server = warp::serve(the_routes.clone()).run(address);

            {
                let mut s = self.mutex.lock().await;

                s.add_advertise_url(&address.to_string());

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

        for (i, unix_path) in unix_paths.iter().enumerate() {
            let the_path = Path::new(&unix_path);
            // remove the socket if it exists
            if the_path.exists() {
                warn!("removing existing socket: {:?}", unix_path);
                std::fs::remove_file(&unix_path).unwrap();
            }

            let listener = match UnixListener::bind(&unix_path) {
                Ok(l) => l,

                Err(e) => {
                    error!("error binding to unix socket {}: {:?}", unix_path, e);
                    error!("note that this is not supported on Docker+OS X");
                    if i == 0 {
                        // this is our default
                        continue;
                    } else {
                        error!("Returning error because this was specified by user.");
                        return Err(e.into());
                    }
                }
            };

            let stream = UnixListenerStream::new(listener);
            let handle = spawn(warp::serve(the_routes.clone()).run_incoming(stream));
            // info!("Listening on {:?}", unix_path);
            let unix_url = format!("http+unix://{}/", unix_path.replace("/", "%2F"));
            {
                let mut s = self.mutex.lock().await;
                s.info(format!("Listening on {:?}", unix_path));
                s.add_advertise_url(&unix_url.to_string());
            }
            handles.push(handle);
        }
        // let (shutdown_send, shutdown_recv) = mpsc::unbounded_channel();

        spawn(clock_go(self.get_lock(), TOPIC_LIST_CLOCK, 1.0));

        let mut sig_hup = tokio::signal::unix::signal(SignalKind::hangup())?;
        let mut sig_term = tokio::signal::unix::signal(SignalKind::terminate())?;
        let mut sig_int = tokio::signal::unix::signal(SignalKind::interrupt())?;

        let pid = std::process::id();
        // s.info(format!("PID: {}", pid));
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
        let mut keys: Vec<&str> = index.topics.keys().map(|k| k.as_str()).collect();

        keys.sort();

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
                @for topic_name  in keys.iter() {
                    li { a href={"topics/"  (topic_name) "/"} { code {(topic_name)} }}
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
    // debug!("handle_websocket_generic: {}", topic_name);
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
                    time_inserted: last.time_inserted,
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
                    // debug!("ws_rx.next() returned None");
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
            url: format!("../data/{}/", this_one.digest),
            available_until: epoch() + 60.0,
        }];
        let nchunks = if send_data { 1 } else { 0 };
        let dr2 = DataReady {
            sequence: this_one.index,
            time_inserted: this_one.time_inserted,
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
                debug!("Error sending DataReady message: {}", e);
                break; // TODO: do better handling
            }
        }
        if send_data {
            let message_data = oq2.data.get(&this_one.digest).unwrap();
            let message = warp::ws::Message::binary(message_data.content.clone());
            match ws_tx.send(message).await {
                Ok(_) => {}
                Err(e) => {
                    debug!("Error sending binary data message: {}", e);
                    break; // TODO: do better handling
                }
            }
        }
    }
    // debug!("handle_websocket_generic: {} - done", topic_name);
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

fn format_nanos(n: i64) -> String {
    let ms = (n as f64) / 1_000_000.0;
    format!("{:.3}ms", ms)
}

async fn handler_topic_html_summary(
    topic_name: String,
    ss_mutex: Arc<Mutex<ServerState>>,
) -> Result<http::Response<Body>, Rejection> {
    let ss = ss_mutex.lock().await;

    let x: &ObjectQueue;
    match ss.oqs.get(topic_name.as_str()) {
        None => return Err(warp::reject::not_found()),

        Some(y) => x = y,
    }

    let (default_content_type, initial_value) = match x.sequence.last() {
        None => ("application/yaml".to_string(), "".to_string()),
        Some(l) => {
            let digest = &l.digest;
            let data = x.data.get(digest).unwrap();
            let initial = match l.content_type.as_str() {
                "application/yaml" | "application/json" => {
                    String::from_utf8(data.content.clone()).unwrap()
                }
                _ => "{}".to_string(),
            };
            (data.content_type.clone(), initial)
        }
    };
    let now = Local::now().timestamp_nanos();

    let format_elapsed = |a| -> String { format_nanos(now - a) };

    let data_or_digest = |data: &DataSaved| -> PreEscaped<String> {
        let printable = match data.content_type.as_str() {
            "application/yaml" | "application/x-yaml" | "text/yaml" | "text/vnd.yaml"
            | "application/json" => true,
            _ => false,
        };
        let url = format!("data/{}/", data.digest);

        if data.content_length <= data.digest.len() {
            if printable {
                let rd = x.data.get(&data.digest).unwrap();
                let s = String::from_utf8(rd.content.clone()).unwrap();
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

    let x = html! {
        (DOCTYPE)
        html {
            head {
                link rel="icon" type="image/png" href="/static/favicon.png" ;
                link rel="stylesheet" href="/static/style.css" ;

                script src="https://cdn.jsdelivr.net/npm/cbor-js@0.1.0/cbor.min.js" {};
                script src="https://cdnjs.cloudflare.com/ajax/libs/js-yaml/4.1.0/js-yaml.min.js" {};
            }
            body {
                h1 { code {(topic_name)} }

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
                                td { (format_nanos(latencies[i]))}
                                td { code {(data.content_type)} }
                                td { (data.content_length) }
                                td { code { (data_or_digest(data)) } }
                            }
                        }
                    }
                } // table



            } // body
        } // html
    }; // html!
    let markup = x.into_string();

    Ok(http::Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(markup))
        .unwrap())
}

// language=javascript
const JAVASCRIPT_SEND: &str = r##"// Select the button and textarea by their IDs
const button = document.getElementById('myButton');
const textarea = document.getElementById('myTextArea');
const textarea_content_type = document.getElementById('myTextAreaContentType');
button.addEventListener('click', () => {
    const content_type = textarea_content_type.value;
    const content_json = jsyaml.load(textarea.value);
    let data;
    if (content_type === "application/json") {
        data = JSON.stringify(content_json);
    } else if (content_type === "application/cbor") {
        data = CBOR.encode(content_json);
    } else if (content_type === "application/yaml") {
        data = jsyaml.dump(content_json);

    } else {
        alert("Unknown content type: " + content_type);
        return;
    }
    // const content_cbor = CBOR.encode(content_json);

    fetch('.', {
        method: 'POST',
        headers: {'Content-Type': content_type},
        body: data
    })
        .then(response => response.json())
        .then(data => console.log(data))
        .catch(error => console.error('Error:', error));
});

function subscribeWebSocket(url, fieldId) {
    // Initialize a new WebSocket connection
    var socket = new WebSocket(url);

    // Connection opened
    socket.addEventListener('open', function (event) {
        console.log('WebSocket connection established');
       var field = document.getElementById(fieldId);

        if (field) {
            field.textContent = 'WebSocket connection established';
        }
    });

    // Listen for messages
    socket.addEventListener('message', async function (event) {

        let message = await convert(event);

        let now = (performance.now() + performance.timeOrigin) * 1000.0* 1000.0;
        let diff = now - message.time_inserted;

        let diff_ms = diff / 1000.0 / 1000.0;
        console.log("diff", now, message.time_inserted, diff);

        let s = "Received this notification with " + diff_ms.toFixed(3) + " ms latency:\n";
        // console.log('Message from server: ', message);

        // Find the field by ID and update its content
        var field = document.getElementById(fieldId);
        if (field) {
            field.textContent = s + JSON.stringify(message, null, 4);
        }
    });

    // Connection closed
    socket.addEventListener('close', function (event) {
        console.log('WebSocket connection closed');
         var field = document.getElementById(fieldId);
        if (field) {
            field.textContent = 'WebSocket connection closed';
        }
    });

    // Connection error
    socket.addEventListener('error', function (event) {
        console.error('WebSocket error: ', event);
           var field = document.getElementById(fieldId);
        if (field) {
            field.textContent = 'WebSocket error';
        }
    });
}

async function convert(event) {
    if (event.data instanceof ArrayBuffer) {
        // The data is an ArrayBuffer - decode it as CBOR
        return CBOR.decode(event.data);
    } else if (event.data instanceof Blob) {
        try {
            const arrayBuffer = await readFileAsArrayBuffer(event.data);
            return CBOR.decode(arrayBuffer);
        } catch (error) {
            console.error('Error reading blob: ', error);
            return 42;
        }
    }

}

function readFileAsArrayBuffer(blob) {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();

        reader.onloadend = () => resolve(reader.result);
        reader.onerror = reject;

        reader.readAsArrayBuffer(blob);
    });
}


document.addEventListener("DOMContentLoaded", function () {
    var s = ((window.location.protocol === "https:") ? "wss://" : "ws://") + window.location.host + window.location.pathname + "events/";


    subscribeWebSocket(s, 'result');
});

"##;

async fn handler_topic_generic(
    topic_name: String,
    ss_mutex: Arc<Mutex<ServerState>>,
    headers: HeaderMap,
) -> Result<http::Response<Body>, Rejection> {
    let accept_headers: Vec<String> = get_accept_header(&headers);

    if accept_headers.contains(&"text/html".to_string()) {
        return handler_topic_html_summary(topic_name, ss_mutex).await;
    };

    // check if the client is requesting an HTML page

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
                let mut res = http::Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .body(Body::from(""))
                    .unwrap();
                let h = res.headers_mut();
                // let resp = Response::new(Body::from(""));
                //
                // let (mut parts, body) = resp.into_parts();
                //
                // parts.status = StatusCode::NO_CONTENT;
                // let mut resp = Response::from_parts(parts, body);
                //
                // let h = resp.headers_mut();

                let suffix = format!("topics/{}/", topic_name);
                put_alternative_locations(&ss, h, &suffix);
                put_common_headers(&ss, h);

                return Ok(res);

                // return Ok(warp::reply::with_status(warp::reply::html(Body::from("")), StatusCode::NO_CONTENT));
            }

            Some(y) => digest = y.digest.clone(),
        }
        // let last = x.sequence.last().unwrap();
        // digest = last.digest.clone();
    }
    return handler_topic_generic_data(topic_name, digest, ss_mutex.clone(), headers).await;
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
    headers: HeaderMap,
) -> Result<Response, Rejection> {
    let accept_headers: Vec<String> = get_accept_header(&headers);

    let ss = ss_mutex.lock().await;

    let x: &ObjectQueue;
    match ss.oqs.get(topic_name.as_str()) {
        None => return Err(warp::reject::not_found()),
        Some(y) => x = y,
    }

    let data = match x.data.get(&digest) {
        None => return Err(warp::reject::not_found()),
        Some(y) => y,
    };
    let data_bytes = data.content.clone();
    let content_type = data.content_type.clone();

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
                     p { "Content length: " (data.content.len()) }

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

        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/html"));

        put_alternative_locations(&ss, headers, "");
        return Ok(resp);
        // return Ok(with_status(resp, StatusCode::OK));
    };

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
    for x in ss.get_advertise_urls().iter() {
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

use tokio::time::{interval, Duration};
async fn clock_go(state: Arc<Mutex<ServerState>>, topic_name: &str, interval_s: f32) {
    let mut clock = interval(Duration::from_secs_f32(interval_s));
    clock.tick().await;
    loop {
        clock.tick().await;
        let mut ss = state.lock().await;
        // let datetime_string = Local::now().to_rfc3339();
        // get the current time in nanoseconds
        let now = Local::now().timestamp_nanos();
        let s = format!("{}", now);
        let _inserted = ss.publish_json(topic_name, &s);

        // debug!("inserted {}: {:?}", topic_name, inserted);
    }
}
