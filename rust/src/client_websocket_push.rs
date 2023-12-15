use anyhow::Context;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use hex;
use hyper::{self, Client};
use hyper_tls::HttpsConnector;
use hyperlocal::UnixClientExt;
use json_patch::{AddOperation, Patch as JsonPatch, Patch, PatchOperation, RemoveOperation};
use maplit::hashmap;
use serde::{Deserialize, Serialize};
use std::{any::Any, collections::HashSet, fmt::Debug, os::unix::fs::FileTypeExt, path::Path, time::Duration};
use tokio::{
    sync::broadcast::{error::RecvError, Receiver, Receiver as BroadcastReceiver},
    task::JoinHandle,
    time::timeout,
};
use tungstenite::Message as TM;
use warp::reply::Response;

use crate::signals_logic::MASK_ORIGIN;
use crate::{
    context, debug_with_info, error_with_info, get_content_type, get_metadata, info_with_info, internal_assertion,
    join_ext, not_available, not_implemented, not_reachable, object_queues::InsertNotification,
    open_websocket_connection, parse_url_ext, put_header_accept, put_header_content_type, send_to_server,
    server_state::ConnectionJob, time_nanos, types::CompositeName, warn_with_info, DTPSError, DataSaved, ErrorMsg,
    FinishedMsg, FoundMetadata, History, LinkBenchmark, LinkHeader, ListenURLEvents, MsgClientToServer,
    MsgServerToClient, MsgWebsocketPushClientToServer, MsgWebsocketPushServerToClient, ProxyJob, PushResult, RawData,
    TopicName, TopicRefAdd, TopicsIndexInternal, TopicsIndexWire, TypeOfConnection, CONTENT_TYPE_DTPS_INDEX,
    CONTENT_TYPE_DTPS_INDEX_CBOR, CONTENT_TYPE_PATCH_JSON, CONTENT_TYPE_TOPIC_HISTORY_CBOR, DTPSR,
    HEADER_CONTENT_LOCATION, HEADER_NODE_ID, REL_CONNECTIONS, REL_EVENTS_DATA, REL_EVENTS_NODATA, REL_HISTORY,
    REL_META, REL_PROXIED, REL_STREAM_PUSH,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum MsgFromPusher {
    RawData(RawData),
    FinishedMsg(FinishedMsg),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum MsgToPusher {
    PushResult(bool),
    ErrorMsg(ErrorMsg),
    FinishedMsg(FinishedMsg),
}

use std::future::Future;

use crate::client::receive_from_server;
use crate::internal_jobs::JobFunctionType;
use crate::websocket_abstractions::GenericSocketConnection;
use async_trait::async_trait;
use std::boxed::Box;
use std::pin::Pin;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;

#[async_trait]
pub trait WebsocketPushInterface {
    async fn push(&mut self, raw_data: &RawData) -> DTPSR<()>;
    async fn stop(&mut self) -> DTPSR<()>;
}

struct WebsocketPush {
    con: TypeOfConnection,
    wsc: Box<dyn GenericSocketConnection>,
    rx: tokio::sync::broadcast::Receiver<TM>,
    tx: futures::channel::mpsc::UnboundedSender<TM>,
}

impl WebsocketPush {
    pub async fn new(con: TypeOfConnection) -> DTPSR<Self> {
        let wsc = open_websocket_connection(&con).await?;
        let tx = wsc.send_outgoing().await;
        let rx = wsc.get_incoming().await;
        Ok(Self { con, wsc, tx, rx })
    }
}

#[async_trait]
impl WebsocketPushInterface for WebsocketPush {
    async fn push(&mut self, raw_data: &RawData) -> DTPSR<()> {
        let m = MsgWebsocketPushClientToServer::RawData(raw_data.clone());
        send_to_server(&mut self.tx, &m).await?;

        let back: Option<MsgWebsocketPushServerToClient> = receive_from_server(&mut self.rx).await?;
        match back {
            None => {
                return Err(DTPSError::Other("Push failed: no message received".to_string()));
            }
            Some(MsgWebsocketPushServerToClient::PushResult(PushResult { result, message })) => {
                if result {
                    Ok(())
                } else {
                    Err(DTPSError::Other(format!("Push failed: {message}")))
                }
            }
        }
    }

    async fn stop(&mut self) -> DTPSR<()> {
        let handles = self.wsc.get_handles();
        // close all handles
        for h in handles {
            h.abort();
            // h.abort().await?;
        }
        Ok(())
    }
}

pub async fn websocket_push(con: TypeOfConnection) -> DTPSR<Box<dyn WebsocketPushInterface>> {
    let md = get_metadata(&con).await?;
    let con = match md.stream_push_url {
        None => {
            return Err(DTPSError::NotAvailable("No stream push URL configured".to_string()));
        }
        Some(u) => u,
    };

    return Ok(Box::new(WebsocketPush::new(con).await?));
}

//
// pub async fn push_events_(
//     con: TypeOfConnection,
//     mut rx1: tokio::sync::mpsc::Receiver<MsgFromPusher>,
//     mut tx2: tokio::sync::mpsc::Sender<MsgToPusher>,
//
// ) -> DTPSR<()> {
//
//     let mut wsc_sender = wsc.send_outgoing().await;
//     let mut receiver = wsc.get_incoming().await;
//
//     loop {
//
//         let m = rx.recv().await;
//         let mfp = match m {
//             None => {
//                 break;
//             }
//             Some(rd) => rd,
//         };
//         match mfp {
//
//
//         }
//         let tosend = MsgClientToServer::RawData(rd);
//         let ascbor = serde_cbor::to_vec(&tosend)?;
//         let msg = TM::binary(ascbor);
//
//         let wsc_sender.send(msg).await?;
//
//
//         let received = receiver.recv().await?;
//         match received {
//             Message::Text(data) => {
//                 if data == "ok" {
//                     tx.send(true).unwrap();
//                 } else {
//                     tx.send(false).unwrap();
//                 }
//             }
//             Message::Binary(_) |
//             Message::Ping(_) |
//             Message::Pong(_) |
//             Message::Frame(_) => {}
//
//             Message::Close(_) => {
//                 tx
//                 break;
//             }
//         }
//
//
//     }
//     Ok(())
// }
