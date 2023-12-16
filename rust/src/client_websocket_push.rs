use std::boxed::Box;
use std::fmt::Debug;

use async_trait::async_trait;
use futures::StreamExt;
use hex;
use hyper::{self};
use serde::{Deserialize, Serialize};
use tungstenite::Message as TM;

use crate::client_metadata::get_metadata;
use crate::utils_websocket::receive_from_server;
use crate::utils_websocket::send_to_server;
use crate::websocket_abstractions::GenericSocketConnection;
use crate::{
    open_websocket_connection, DTPSError, ErrorMsg, FinishedMsg, MsgWebsocketPushClientToServer,
    MsgWebsocketPushServerToClient, PushResult, RawData, TypeOfConnection, DTPSR,
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
