use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    path::Path,
    string::ToString,
    sync::Arc as StdArc,
};

use chrono::Local;
use clap::Parser;
use futures::{
    stream::{
        SplitSink,
        SplitStream,
    },
    SinkExt,
    StreamExt,
};
use indent::indent_all_with;
use maplit::hashmap;
use maud::{
    html,
    PreEscaped,
    DOCTYPE,
};
use serde::{
    de::DeserializeOwned,
    Deserialize,
    Serialize,
};
use serde_yaml;
use tokio::{
    net::{
        TcpListener,
        UnixListener,
    },
    signal::unix::SignalKind,
    spawn,
    sync::{
        broadcast::{
            error::RecvError,
            Receiver,
        },
        mpsc,
        mpsc::UnboundedSender,
        Mutex as TokioMutex,
    },
    task::JoinHandle,
    time::{
        interval,
        Duration,
    },
};
use tokio_stream::wrappers::{
    UnboundedReceiverStream,
    UnixListenerStream,
};
use tungstenite::http::{
    HeaderMap,
    HeaderValue,
    StatusCode,
};
use warp::{
    hyper::Body,
    reply::Response,
    Filter,
    Rejection,
};

use crate::{
    cloudflare::open_cloudflare,
    constants::*,
    debug_with_info,
    divide_in_components,
    epoch,
    error_other,
    error_with_info,
    format_digest_path,
    format_nanos,
    handle_rejection,
    handle_websocket_generic2,
    html_utils::make_html,
    info_with_info,
    internal_jobs::JobFunctionType,
    interpret_path,
    invalid_input,
    parse_url_ext,
    put_common_headers,
    put_header_content_type,
    put_header_location,
    serve_master_get,
    serve_master_head,
    serve_master_patch,
    serve_master_post,
    server_state::ConnectionJob,
    show_errors,
    sniff_and_start_proxy,
    types::CompositeName,
    utils_headers,
    warn_with_info,
    ChannelInfo,
    ChannelInfoDesc,
    Chunk,
    ComponentStatusNotification,
    DTPSError,
    DataReady,
    DataSaved,
    InsertNotification,
    ListenURLEvents,
    MsgClientToServer,
    MsgServerToClient,
    ObjectQueue,
    RawData,
    ResourceAvailabilityWire,
    ServerState,
    Status,
    StatusSummary,
    TopicName,
    TypeOfConnection,
    DTPSR,
};

const AVAILABILITY_LENGTH_SEC: f64 = 60.0;

pub type HandlersResponse = Result<http::Response<Body>, Rejection>;
pub type ServerStateAccess = StdArc<TokioMutex<ServerState>>;

pub struct DTPSServer {
    pub listen_address: Option<SocketAddr>,
    pub mutex: ServerStateAccess,
    pub cloudflare_tunnel_name: Option<String>,
    pub cloudflare_executable: String,
    pub unix_path: Option<String>,
    initial_proxy: HashMap<String, TypeOfConnection>,
    topic_connections: Vec<ConnectionJob>,
}

impl DTPSServer {
    pub async fn new(
        listen_address: Option<SocketAddr>,
        cloudflare_tunnel_name: Option<String>,
        cloudflare_executable: String,
        unix_path: Option<String>,
        initial_proxy: HashMap<String, TypeOfConnection>,
        topic_connections: Vec<ConnectionJob>,
        aliases: Vec<TopicAlias>,
    ) -> DTPSR<Self> {
        let mut ss = ServerState::new(None)?;
        for alias in &aliases {
            ss.add_alias(&alias.new, &alias.existing);
        }

        let mutex: ServerStateAccess = StdArc::new(TokioMutex::new(ss));

        Ok(DTPSServer {
            listen_address,
            mutex,
            cloudflare_tunnel_name,
            cloudflare_executable,
            unix_path,
            initial_proxy,
            topic_connections,
        })
    }
    pub async fn start_serving(&mut self) -> DTPSR<Vec<JoinHandle<()>>> {
        // get current pid
        let pid = std::process::id();
        if pid == 1 {
            warn_with_info!("WARNING: Running as PID 1. This is not recommended because CTRL-C may not work.");
            warn_with_info!("If running through `docker run`, use `docker run --init` to avoid this.")
        }

        let ssa: ServerStateAccess = self.mutex.clone();

        let clone_access = warp::any().map(move || ssa.clone());

        let master_route_get = warp::path::full()
            .and(warp::query::<HashMap<String, String>>())
            .and(warp::get())
            .and(clone_access.clone())
            .and(warp::header::headers_cloned())
            .and_then(serve_master_get);

        let master_route_head = warp::path::full()
            .and(warp::query::<HashMap<String, String>>())
            .and(warp::head())
            .and(clone_access.clone())
            .and(warp::header::headers_cloned())
            .and_then(serve_master_head);

        let master_route_post = warp::path::full()
            .and(warp::query::<HashMap<String, String>>())
            .and(warp::post())
            .and(clone_access.clone())
            .and(warp::header::headers_cloned())
            .and(warp::body::bytes())
            .and_then(serve_master_post);

        let master_route_patch = warp::path::full()
            .and(warp::query::<HashMap<String, String>>())
            .and(warp::patch())
            .and(clone_access.clone())
            .and(warp::header::headers_cloned())
            .and(warp::body::bytes())
            .and_then(serve_master_patch);

        let topic_generic_events_route2 = warp::path::full()
            .and_then(|path: warp::path::FullPath| async move {
                let path1 = path.as_str();
                let segments: Vec<String> = divide_in_components(path1, '/');
                let last = segments.last();
                if last == Some(&EVENTS_SUFFIX.to_string()) {
                    let start = path.as_str().rfind(EVENTS_SUFFIX).unwrap();
                    let part = &path.as_str()[..start];
                    // debug_with_info!("websocket raw {path1:?} -> {part:?}");
                    Ok(part.to_string())
                } else {
                    Err(warp::reject::reject())
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

                    ws.on_upgrade(move |socket| handle_websocket_generic2(path, socket, state1, send_data))
                }
            });

        let the_routes = topic_generic_events_route2
            .or(master_route_head)
            .or(master_route_get)
            .or(master_route_post)
            .or(master_route_patch)
            .recover(handle_rejection);

        let mut handles = vec![];

        let ssa = self.get_lock();

        let rx = {
            let ss = ssa.lock().await;
            let tsn = TopicName::from_dash_sep(TOPIC_STATE_NOTIFICATION)?;
            let oq = ss.oqs.get(&tsn).unwrap();
            oq.subscribe_insert_notification()
        };

        let ssa2 = ssa.clone();
        handles.push(spawn(show_errors(
            Some(ssa.clone()),
            "collect_statuses".to_string(),
            collect_statuses(ssa2, rx),
        )));

        if let Some(address) = self.listen_address {
            let the_routes_cloned = the_routes.clone();
            let serve_job: JobFunctionType = Box::new(move || {
                let the_routes_cloned = the_routes_cloned.clone();

                Box::pin(async move {
                    {
                        #[allow(clippy::let_unit_value)]
                        match TcpListener::bind(&address).await {
                            Ok(_) => {
                                // The port is available, proceed to start the Warp server
                            }
                            Err(e) => {
                                // The port is already in use, handle the error gracefully
                                warn_with_info!("Failed to bind to address {}: {}", address, e);
                                return Ok(());
                            }
                        };
                    }

                    warp::serve(the_routes_cloned).run(address).await;
                    Ok(())
                })
            });

            {
                let mut ss = self.mutex.lock().await;
                let job_name = CompositeName::from_dash_sep("server/tcp")?;
                ss.job_manager
                    .add_job(&job_name, "TCP server", serve_job, false, true, 5.0, self.mutex.clone())?;
            }

            {
                let mut ss = self.mutex.lock().await;

                info_with_info!("Listening on port {}", address.port());

                let s1 = format!("http://localhost:{}{}", address.port(), "/");
                ss.add_advertise_url(&s1)?;

                let s2 = address.to_string();
                if !s2.contains("0.0.0.0") {
                    ss.add_advertise_url(&s2)?;
                }

                for host in platform::get_other_addresses() {
                    let x = format!("http://{}:{}{}", host, address.port(), "/");
                    ss.add_advertise_url(&x)?;
                }

                use crate::platform;
                use gethostname::gethostname;
                let hostname = gethostname();
                let hostname = hostname.to_string_lossy();
                let x = format!("http://{}:{}{}", hostname, address.port(), "/");
                ss.add_advertise_url(&x)?;
            }
        }

        // if tunnel is given ,start
        if let (Some(tunnel_file), Some(address)) = (&self.cloudflare_tunnel_name, self.listen_address) {
            let port = address.port();
            let handle = open_cloudflare(port, tunnel_file, &self.cloudflare_executable).await;

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
            warn_with_info!("Opening socket on: {the_path:?}");
            let dirname = the_path.parent().unwrap();
            if !dirname.exists() {
                warn_with_info!("Creating dirname: {dirname:?}");
                std::fs::create_dir_all(dirname).unwrap();
            }
            // remove the socket if it exists
            if the_path.exists() {
                warn_with_info!("removing existing socket: {:?}", unix_path);
                std::fs::remove_file(unix_path).unwrap();
            }

            let the_routes_cloned = the_routes.clone();
            let unix_url = format!("http+unix://{}/", unix_path.replace('/', "%2F"));

            let unix_path = unix_path.clone();
            let serve_job: JobFunctionType = Box::new(move || {
                let the_routes_cloned = the_routes_cloned.clone();
                let unix_path = unix_path.clone();
                Box::pin(async move {
                    let listener = match UnixListener::bind(unix_path.clone()) {
                        Ok(l) => l,

                        Err(e) => {
                            let msg = format!("error binding to unix socket {}: {:?}", unix_path, e);
                            error_with_info!("note that this is not supported on Docker+OS X");
                            return Err(msg);
                        }
                    };

                    let stream = UnixListenerStream::new(listener);

                    warp::serve(the_routes_cloned).run_incoming(stream).await;
                    Ok(())
                })
            });
            {
                let mut ss = self.mutex.lock().await;
                let job_name = CompositeName::from_dash_sep(format!("server/unix/{i}"))?;

                ss.job_manager.add_job(
                    &job_name,
                    "Unix server",
                    serve_job,
                    false,
                    true,
                    5.0,
                    self.mutex.clone(),
                )?;
            }

            {
                let mut s = ssa.lock().await;
                // s.info(format!("Listening on {:?}", unix_path));
                s.add_advertise_url(&unix_url.to_string())?;
            }
        }

        let ssa_job = self.get_lock();
        let clock_job: JobFunctionType = Box::new(move || {
            let ssa = ssa_job.clone();
            Box::pin(async move {
                let topic_name = TopicName::from_relative_url(TOPIC_LIST_CLOCK).unwrap();
                clock_go(ssa.clone(), topic_name, 1.0).await;
                Ok(())
            })
        });
        {
            let mut ss = self.mutex.lock().await;
            let job_name = CompositeName::from_dash_sep("internal/clock")?;

            ss.job_manager.add_job(
                &job_name,
                "clock update server",
                clock_job,
                true,
                true,
                5.0,
                self.mutex.clone(),
            )?;
        }

        spawn(clock_go(
            self.get_lock(),
            TopicName::from_relative_url(TOPIC_LIST_CLOCK)?,
            1.0,
        ));

        if !self.initial_proxy.is_empty() {
            // let s = ssa.lock().await;
            // let mut i = 0;
            for (k, v) in self.initial_proxy.clone() {
                let mounted_at = TopicName::from_relative_url(&k)?;
                self.add_proxied(&mounted_at, v.clone()).await?;

                // i += 1;
            }
            info_with_info!("Proxies started");
        }
        {
            let ss = self.mutex.lock().await;
            info_with_info!(
                "Server started. Advertised URLs: \n{}\n",
                indent_all_with(" ", ss.get_advertise_urls().join("\n"))
            );
        }

        for (i, con) in self.topic_connections.iter().enumerate() {
            let component_name = TopicName::from_components(&["connections".to_string(), format!("{i}")]);

            let mut ss = self.mutex.lock().await;

            ss.add_topic_to_topic_connection(&component_name, con, ssa.clone())
                .await?;
        }

        Ok(handles)
    }
    pub async fn serve(&mut self) -> DTPSR<()> {
        let handles = self.start_serving().await?;
        let mut sig_hup = tokio::signal::unix::signal(SignalKind::hangup())?;
        let mut sig_term = tokio::signal::unix::signal(SignalKind::terminate())?;
        let mut sig_int = tokio::signal::unix::signal(SignalKind::interrupt())?;

        let pid = std::process::id();
        info_with_info!("PID: {}", pid);
        let res: DTPSR<()>;
        tokio::select! {

            _ = sig_int.recv() => {
                info_with_info!("SIGINT received");
                    res = Err(DTPSError::Interrupted);
            },
            _ = sig_hup.recv() => {
                info_with_info!("SIGHUP received: gracefully shutting down");
                res = Ok(());
            },
            _ = sig_term.recv() => {
                info_with_info!("SIGTERM received: gracefully shutting down");
                res = Ok(());

            },
                // _ = futures::future::join_all(&handles) => {
                //     info_with_info!("shutdown received");
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

    pub async fn add_proxied(&mut self, mounted_at: &TopicName, url: TypeOfConnection) -> DTPSR<JoinHandle<()>> {
        let ssa = self.get_lock();

        let future = sniff_and_start_proxy(mounted_at.clone(), url, ssa.clone());
        let handle = spawn(show_errors(
            Some(ssa),
            format!("proxied/{mounted_at}", mounted_at = mounted_at.as_dash_sep()),
            future,
        ));

        Ok(handle)
    }
}

pub async fn root_handler(ss_mutex: ServerStateAccess, headers: HeaderMap) -> HandlersResponse {
    let ss = ss_mutex.lock().await;
    let index_internal = ss.create_topic_index();
    let index = index_internal.to_wire(None);

    let accept_headers: Vec<String> = utils_headers::get_accept_header(&headers);

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
                url.push('/');
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
        put_header_content_type(headers, CONTENT_TYPE_CBOR);

        put_common_headers(&ss, headers);
        put_alternative_locations(&ss, headers, "");

        Ok(resp)
    }
}

pub fn get_channel_info_message(oq: &ObjectQueue) -> ChannelInfo {
    let num_total;
    let newest;
    let oldest = match oq.stored.first() {
        None => None,
        Some(first) => {
            let d = oq.saved.get(first).unwrap();
            Some(ChannelInfoDesc::new(d.index, d.time_inserted))
        }
    };

    match oq.stored.last() {
        None => {
            num_total = 0;
            newest = None;
        }
        Some(last) => {
            let d = oq.saved.get(last).unwrap();

            num_total = d.index + 1;

            newest = Some(ChannelInfoDesc::new(d.index, d.time_inserted));
        }
    }

    ChannelInfo {
        queue_created: oq.tr.created,
        num_total,
        newest,
        oldest,
    }
}

pub fn get_dataready(this_one: &DataSaved) -> DataReady {
    let availability = vec![ResourceAvailabilityWire {
        url: format_digest_path(&this_one.digest, &this_one.content_type),
        available_until: epoch() + 60.0,
    }];

    let nchunks = 0;
    DataReady {
        origin_node: this_one.origin_node.clone(),
        unique_id: this_one.unique_id.clone(),
        sequence: this_one.index,
        time_inserted: this_one.time_inserted,
        digest: this_one.digest.clone(),
        content_type: this_one.content_type.clone(),
        content_length: this_one.content_length,
        clocks: this_one.clocks.clone(),
        availability,
        chunks_arriving: nchunks,
    }
}

pub async fn get_series_of_messages_for_notification_(
    send_data: bool,
    insert_notification: &InsertNotification,
    delta_availability: f64,
    ss: &mut ServerState,
) -> Vec<MsgServerToClient> {
    let this_one = &insert_notification.data_saved;
    let mut out = vec![];
    let the_availability = if send_data {
        vec![]
    } else {
        let vec1 = vec![ResourceAvailabilityWire {
            url: format_digest_path(&this_one.digest, &this_one.content_type),
            available_until: epoch() + delta_availability,
        }];
        vec1
    };

    ss.save_blob_for_time(
        &this_one.digest,
        &insert_notification.raw_data.content,
        delta_availability,
    );

    let nchunks = if send_data { 1 } else { 0 };
    let dr2 = DataReady {
        origin_node: this_one.origin_node.clone(),
        unique_id: this_one.unique_id.clone(),
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
    out.push(out_message);

    if send_data {
        let content = &insert_notification.raw_data.content;
        let chunk = MsgServerToClient::Chunk(Chunk {
            digest: this_one.digest.clone(),
            i: 0,
            n: nchunks,
            index: 0,
            data: content.clone(),
        });
        out.push(chunk);
    }
    out
}

pub async fn handle_websocket_queue(
    ws_tx: &mut SplitSink<warp::ws::WebSocket, warp::ws::Message>,
    ssa: ServerStateAccess,
    topic_name: TopicName,
    send_data: bool,
) -> DTPSR<()> {
    let mut rx2 = {
        let mut starting_messaging = vec![];

        // important: release the lock
        let rx = {
            let (rx, inot) = {
                let ss0 = ssa.lock().await;

                let oq = ss0.get_queue(&topic_name)?;

                let channel_info_message = MsgServerToClient::ChannelInfo(get_channel_info_message(oq));
                starting_messaging.push(channel_info_message);

                let inot = ss0.get_last_insert(&topic_name)?;
                let rx = oq.subscribe_insert_notification();
                (rx, inot)
            };

            if let Some(x) = &inot {
                let mut ss0 = ssa.lock().await;

                let mut for_this =
                    get_series_of_messages_for_notification_(send_data, x, AVAILABILITY_LENGTH_SEC, &mut ss0).await;
                starting_messaging.append(&mut for_this);
            }

            rx
        };

        send_as_ws_cbor(&starting_messaging, ws_tx).await?;
        rx
    };
    loop {
        let r = match rx2.recv().await {
            Ok(x) => x,
            Err(e) => match e {
                RecvError::Closed => {
                    break;
                }
                RecvError::Lagged(_) => {
                    warn_with_info!("Lagged");
                    continue;
                }
            },
        };
        let for_this = {
            match r {
                ListenURLEvents::InsertNotification(not) => {
                    let mut ss = ssa.lock().await;

                    get_series_of_messages_for_notification_(send_data, &not, AVAILABILITY_LENGTH_SEC, &mut ss).await
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
            }
        };
        send_as_ws_cbor(&for_this, ws_tx).await?;
    }
    Ok(())
}

pub async fn send_as_ws_cbor<T: Serialize>(
    data: &Vec<T>,
    ws_tx: &mut SplitSink<warp::ws::WebSocket, warp::ws::Message>,
) -> DTPSR<()> {
    for x in data {
        let bytes = serde_cbor::to_vec(&x).unwrap();
        let message = warp::ws::Message::binary(bytes);

        ws_tx.send(message).await?;
    }
    Ok(())
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
    rd: &RawData,
    // headers: HeaderMap,
    // data: hyper::body::Bytes,
) -> HandlersResponse {
    let mut ss = ss_mutex.lock().await;

    // let content_type = get_header_with_default(&headers, CONTENT_TYPE, OCTET_STREAM);
    //
    //
    // let byte_vector: Vec<u8> = data.to_vec().clone();

    let ds = ss.publish(topic_name, &rd.content, &rd.content_type, None)?;

    Ok(construct_response_cbor(&ds))
}

pub fn construct_response_cbor<T: Serialize>(ob: &T) -> http::Response<Body> {
    let ds_cbor = serde_cbor::to_vec(&ob).unwrap();
    http::Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, CONTENT_TYPE_CBOR)
        .body(Body::from(ds_cbor))
        .unwrap()
}

async fn handler_topic_html_summary(
    topic_name: &TopicName,
    ss_mutex: ServerStateAccess,
) -> Result<http::Response<Body>, Rejection> {
    let ss = ss_mutex.lock().await;

    let x = ss.get_queue(topic_name)?;

    let (default_content_type, initial_value) = match x.stored.last() {
        None => ("application/yaml".to_string(), "{}".to_string()),
        Some(index) => {
            let l = x.saved.get(index).unwrap();
            let digest = &l.digest;
            let content = match ss.get_blob(digest) {
                None => {
                    return Err(warp::reject::not_found());
                }
                Some(c) => c,
            };
            let initial = match l.content_type.as_str() {
                "application/yaml" | "application/json" => String::from_utf8(content.to_vec()).unwrap(),
                _ => "{}".to_string(),
            };
            (l.content_type.clone(), initial)
        }
    };
    let now = Local::now().timestamp_nanos();

    let format_elapsed = |a| -> String { format_nanos(now - a) };

    let data_or_digest = |data: &DataSaved| -> PreEscaped<String> {
        let printable = matches!(
            data.content_type.as_str(),
            "application/yaml" | "application/x-yaml" | "text/yaml" | "text/vnd.yaml" | "application/json"
        );
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
    for i in &x.stored {
        let data = x.saved.get(i).unwrap();
        if x.saved.contains_key(&(i - 1)) {
            let data_prev = x.saved.get(&(i - 1)).unwrap();
            latencies.push(data.time_inserted - data_prev.time_inserted);
        } else {
            latencies.push(0);
        }
    }

    let x = make_html(
        topic_name.as_relative_url(),
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
                    @for (i, data) in x.all_data().iter().enumerate().rev().take(100) {
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

pub fn put_alternative_locations(ss: &ServerState, headers: &mut HeaderMap<HeaderValue>, suffix: &str) {
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

    #[arg(long)]
    connect: Vec<String>,

    #[arg(long)]
    alias: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TopicAlias {
    pub new: TopicName,
    pub existing: TopicName,
}

impl TopicAlias {
    /// Parses a string of the form `a/b -> c/d`
    pub fn from_string(s: &str) -> DTPSR<Self> {
        match s.find('=') {
            None => {
                invalid_input!("Cannot find -> in string {s:?}")
            }
            Some(i) => {
                // divide in 2
                let before = s[..i].trim();
                let after = s[i + "=".len()..].trim();
                let new = TopicName::from_dash_sep(before)?;
                let existing = TopicName::from_dash_sep(after)?;

                Ok(Self { new, existing })
            }
        }
    }
}

pub fn address_from_host_port(host: &str, port: u16) -> DTPSR<SocketAddr> {
    let hoststring = format!("{}:{}", host, port);

    let s: Result<SocketAddr, _> = hoststring.parse();
    match s {
        Ok(x) => Ok(x),
        Err(e) => {
            debug_with_info!("Cannot parse {}: {}", hoststring, e);
            error_other(format!("Cannot parse {}: {}", hoststring, e))
        }
    }
}

pub async fn create_server_from_command_line() -> DTPSR<DTPSServer> {
    let args = ServerArgs::parse();

    let listen_address = if args.tcp_port > 0 {
        Some(address_from_host_port(args.tcp_host.as_str(), args.tcp_port)?)
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
        #[allow(clippy::get_first)]
        let name = parts.get(0).unwrap();
        let value = parts.get(1).unwrap();
        let url = parse_url_ext(value)?;
        proxy.insert(name.to_string(), url);
    }
    if !proxy.is_empty() {
        debug_with_info!("Proxy:\n{:#?}", proxy);
    }
    let topic_connections = args
        .connect
        .iter()
        .map(|x| ConnectionJob::from_string(x).unwrap())
        .collect::<Vec<ConnectionJob>>();
    let aliases = args
        .alias
        .iter()
        .map(|x| TopicAlias::from_string(x).unwrap())
        .collect::<Vec<TopicAlias>>();
    DTPSServer::new(
        listen_address,
        args.tunnel.clone(),
        args.cloudflare_executable.clone(),
        args.unix_path,
        proxy,
        topic_connections,
        aliases,
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

        // debug_with_info!("inserted {}: {:?}", topic_name, inserted);
    }
}

pub fn receive_from_websocket<T: DeserializeOwned + Clone + Send + 'static>(
    ws_rx: SplitStream<warp::ws::WebSocket>,
) -> (UnboundedReceiverStream<T>, JoinHandle<()>) {
    let (tx, rx) = mpsc::unbounded_channel();

    let handle = spawn(show_errors(None, "websocket".to_string(), pull_(ws_rx, tx)));

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
                // debug_with_info!("ws_rx.next() returned None");
                // finished = true;
                break;
            }
            Some(Ok(msg)) => {
                if msg.is_binary() {
                    let raw_data = msg.as_bytes().to_vec().clone();
                    // let v: serde_cbor::Value = serde_cbor::from_slice(&raw_data).unwrap();
                    //
                    // debug_with_info!("ws_rx.next() returned {:#?}", v);
                    //

                    let ms: T = match serde_cbor::from_slice(raw_data.as_slice()) {
                        Ok(x) => x,
                        Err(err) => {
                            debug_with_info!("ws_rx.next() cannot interpret error {:#?}", err);
                            continue;
                        }
                    };
                    // let c = serde_cbor::to_vec(&ms).unwrap();

                    tx.send(ms.clone()).unwrap();
                }
            }
            Some(Err(err)) => {
                debug_with_info!("ws_rx.next() returned error {:#?}", err);
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
                debug_with_info!("do_receiving: receiver.next() returned None");
                // finished = true;
                break;
            }
            Some(MsgClientToServer::RawData(rd)) => {
                let mut ss0 = ss_mutex.lock().await;

                let _ds = ss0.publish(&topic_name, &rd.content, &rd.content_type, None);
            }
        }
    }
}

pub async fn collect_statuses(ssa: ServerStateAccess, mut rx: Receiver<ListenURLEvents>) -> DTPSR<()> {
    let mut cur = StatusSummary {
        components: Default::default(),
        comments: Default::default(),
    };
    let res = TopicName::from_dash_sep(TOPIC_STATE_SUMMARY).unwrap();

    {
        let mut ss = ssa.lock().await;

        ss.publish_object_as_cbor(&res, &cur, None)?;
    }
    loop {
        let lue = match rx.recv().await {
            Ok(lue) => lue,
            Err(e) => match e {
                RecvError::Closed => {
                    debug_with_info!("collect_statuses: finished collecting");
                    break;
                }
                RecvError::Lagged(_) => {
                    error_with_info!("collect_statuses: lagged");
                    continue;
                }
            },
        };

        match lue {
            ListenURLEvents::InsertNotification(inot) => {
                let csn0 = inot.raw_data.interpret::<ComponentStatusNotification>();

                let csn = if let Err(e) = csn0 {
                    error_with_info!("Cannot parse status notification: {:?}", e);
                    continue;
                } else {
                    csn0.unwrap()
                };

                cur.incorporate(csn);
                {
                    let mut ss = ssa.lock().await;
                    let r = ss.publish_object_as_cbor(&res, &cur, None);
                    if let Err(e) = r {
                        error_with_info!("Cannot publish status summary: {:?}", e);
                        continue;
                    }
                }

                {
                    let mut ss = ssa.lock().await;
                    match ss.publish_object_as_cbor(&res, &cur, None) {
                        Ok(_) => {}
                        Err(e) => {
                            error_with_info!("Cannot publish status summary: {:?}", e);
                        }
                    }
                }
            }
            ListenURLEvents::WarningMsg(_) => {} // TODO: do something?
            ListenURLEvents::ErrorMsg(_) => {}
            ListenURLEvents::FinishedMsg(_) => {}
            ListenURLEvents::SilenceMsg(_) => {}
        }
    }

    Ok(())
}
