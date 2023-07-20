use std::fmt::Debug;

use anyhow::Context;
use async_trait::async_trait;
use base64;
use base64::{engine::general_purpose, Engine as _};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use hex;
use hyper;
use hyperlocal::UnixClientExt;
use log::{debug, error};
use rand::Rng;
use tokio::net::{TcpStream, UnixStream};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{client_async_with_config, connect_async, MaybeTlsStream, WebSocketStream};
use tungstenite::handshake::client::Request;
use tungstenite::Message as TM;
use url::Url;

use crate::structures::TypeOfConnection;
use crate::structures::TypeOfConnection::{Relative, TCP, UNIX};
use crate::TypeOfConnection::Same;
use crate::{
    error_with_info, not_implemented, not_reachable, show_errors, DTPSError, UnixCon, DTPSR,
};

#[async_trait]
pub trait GenericSocketConnection: Send + Sync {
    fn get_received_headers(&self) -> Vec<(String, String)>;

    async fn get_incoming(&self) -> broadcast::Receiver<TM>;
    async fn send_outgoing(&self) -> futures::channel::mpsc::UnboundedSender<TM>;

    fn get_handles(&self) -> &Vec<JoinHandle<()>>;
}

pub async fn open_websocket_connection(
    con: &TypeOfConnection,
) -> DTPSR<Box<dyn GenericSocketConnection>> {
    match con {
        TCP(url) => open_websocket_connection_tcp(url).await,
        TypeOfConnection::File(_, _) => {
            not_implemented!("File connection not implemented")
        }
        UNIX(uc) => open_websocket_connection_unix(uc).await,
        Relative(_, _) => {
            not_implemented!("Relative connection not implemented")
        }
        Same() => {
            not_implemented!("Same connection not implemented")
        }
    }
}

struct MPMC<T> {
    // we actually need it to send
    incoming_sender: broadcast::Sender<T>,
    incoming_receiver: broadcast::Receiver<T>,
    outgoing_sender: futures::channel::mpsc::UnboundedSender<T>,
    handles: Vec<JoinHandle<()>>,
}

struct AnySocketConnection {
    // pub ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    pub response: tungstenite::handshake::client::Response,

    pub mmpc: MPMC<TM>,
}
//
// struct UnixSocketConnection {
//     // pub ws_stream: WebSocketStream<UnixStream>,
//     pub response: tungstenite::handshake::client::Response,
//     pub mmpc: MPMC<TM >,
// }

impl AnySocketConnection {
    pub fn from_tcp(
        ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
        response: tungstenite::handshake::client::Response,
    ) -> Self {
        // single producer multiple consumer
        let (incoming_sender, incoming_receiver) = broadcast::channel::<TM>(1280);

        // mpsc
        let (outgoing_sender, outgoing_receiver) = futures::channel::mpsc::unbounded::<TM>();
        //
        let (sink, stream) = ws_stream.split();

        let handle1 = tokio::spawn(show_errors(
            "receiver".to_string(),
            read_websocket_stream(stream, incoming_sender.clone()),
        ));

        let handle2 = tokio::spawn(show_errors(
            "sender".to_string(),
            write_websocket_stream(outgoing_receiver, sink),
        ));

        let handles = vec![handle1, handle2];
        Self {
            // ws_stream,
            response,
            mmpc: MPMC {
                incoming_sender,
                incoming_receiver,
                outgoing_sender,
                // outgoing_receiver,
                handles,
            },
        }
    }

    pub fn from_unix(
        ws_stream: WebSocketStream<UnixStream>,
        response: tungstenite::handshake::client::Response,
    ) -> Self {
        // single producer multiple consumer
        let (incoming_sender, incoming_receiver) = broadcast::channel::<TM>(1280);

        // mpsc
        let (outgoing_sender, outgoing_receiver) = futures::channel::mpsc::unbounded::<TM>();

        let (sink, stream) = ws_stream.split();

        let handle1 = tokio::spawn(show_errors(
            "receiver".to_string(),
            read_websocket_stream(stream, incoming_sender.clone()),
        ));

        let handle2 = tokio::spawn(show_errors(
            "sender".to_string(),
            write_websocket_stream(outgoing_receiver, sink),
        ));

        let handles = vec![handle1, handle2];
        Self {
            // ws_stream,
            response,
            mmpc: MPMC {
                incoming_sender,
                incoming_receiver,
                outgoing_sender,
                // outgoing_receiver,
                handles,
            },
        }
    }
}
async fn read_websocket_stream<S: Debug, T: StreamExt<Item = Result<S, tungstenite::Error>>>(
    mut source: SplitStream<T>,
    incoming_sender: broadcast::Sender<S>,
) -> DTPSR<()> {
    loop {
        match source.next().await {
            Some(msgr) => {
                // info!("received message: {:?}", msg);
                match msgr {
                    Ok(msg) => {
                        if incoming_sender.receiver_count() > 0 {
                            match incoming_sender.send(msg) {
                                Ok(_) => {}
                                Err(e) => {
                                    error_with_info!("error in incoming_sender: {e}");
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error_with_info!("error in read_websocket_stream: {:?}", e);
                        break;
                    }
                }
            }
            None => {
                error_with_info!("read_websocket_stream: None");
                break;
            }
        }
    }

    Ok(())
}

async fn write_websocket_stream<S: Debug, E: Debug, T: SinkExt<S, Error = E>>(
    mut outgoing_receiver: futures::channel::mpsc::UnboundedReceiver<S>,
    mut sink: SplitSink<T, S>,
) -> DTPSR<()> {
    loop {
        let m = outgoing_receiver.next().await;
        match m {
            None => break,
            Some(x) => match sink.send(x).await {
                Ok(_) => {}
                Err(e_) => {
                    error_with_info!("error in write_websocket_stream: {e_:?}");
                    break;
                }
            },
        }
    }
    Ok(())
}

#[async_trait]
impl GenericSocketConnection for AnySocketConnection {
    fn get_received_headers(&self) -> Vec<(String, String)> {
        self.response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string()))
            .collect()
    }

    async fn get_incoming(&self) -> broadcast::Receiver<TM> {
        self.mmpc.incoming_sender.subscribe()
    }

    async fn send_outgoing(&self) -> futures::channel::mpsc::UnboundedSender<TM> {
        self.mmpc.outgoing_sender.clone()
    }

    fn get_handles(&self) -> &Vec<JoinHandle<()>> {
        return &self.mmpc.handles;
    }
}

pub async fn open_websocket_connection_tcp(url: &Url) -> DTPSR<Box<dyn GenericSocketConnection>> {
    // replace https with wss, and http with ws
    let mut url = url.clone();
    if url.scheme() == "https" {
        url.set_scheme("wss").unwrap();
    } else if url.scheme() == "http" {
        url.set_scheme("ws").unwrap();
    } else {
        panic!("unexpected scheme: {}", url.scheme());
    }
    let connection_res = connect_async(url.clone()).await;
    // debug!("connection: {:#?}", connection);
    let connection;
    match connection_res {
        Ok(c) => {
            connection = c;
        }
        Err(err) => {
            return not_reachable!("could not connect to {url}: tungstenite {:?}", err);
        }
    }
    let (ws_stream, response) = connection;
    debug!("TCP response: {:#?}", response);

    let tcp = AnySocketConnection::from_tcp(ws_stream, response);

    Ok(Box::new(tcp))
}

pub async fn open_websocket_connection_unix(
    uc: &UnixCon,
) -> DTPSR<Box<dyn GenericSocketConnection>> {
    let stream_res = UnixStream::connect(uc.socket_name.clone()).await;
    let stream = match stream_res {
        Ok(s) => s,
        Err(err) => {
            return DTPSError::not_reachable(format!(
                "could not connect to {}: {}",
                uc.socket_name, err
            ));
        }
    };
    // let ready = stream.ready(Interest::WRITABLE).await.unwrap();

    let mut path = uc.path.clone();
    match &uc.query {
        None => {}
        Some(q) => {
            path.push_str("?");
            path.push_str(&q);
        }
    }
    let connection_id = generate_websocket_key();

    let url = format!("ws://localhost{}", path);
    let request = Request::builder()
        .uri(url)
        .header("Host", "localhost")
        .header("Upgrade", "websocket")
        .header("Connection", "Upgrade")
        .header("Sec-WebSocket-Key", connection_id)
        .header("Sec-WebSocket-Version", "13")
        .header("Host", "localhost")
        .body(())
        .unwrap();
    let (socket_stream, response) = {
        let config = WebSocketConfig {
            max_send_queue: None,
            max_message_size: None,
            max_frame_size: None,
            accept_unmasked_frames: false,
        };
        match client_async_with_config(request, stream, Some(config)).await {
            Ok(s) => s,
            Err(err) => {
                error_with_info!("could not connect to {}: {}", uc.socket_name, err);
                return DTPSError::other(format!(
                    "could not connect to {}: {}",
                    uc.socket_name, err
                ));
            }
        }
    };

    debug!("WS response: {:#?}", response);
    // use_stream = EitherStream::UnixStream(socket_stream);
    let res = AnySocketConnection::from_unix(socket_stream, response);
    return Ok(Box::new(res));
}

fn generate_websocket_key() -> String {
    let mut rng = rand::thread_rng();
    let random_bytes: Vec<u8> = (0..16).map(|_| rng.gen()).collect();
    general_purpose::URL_SAFE_NO_PAD.encode(&random_bytes)
}
