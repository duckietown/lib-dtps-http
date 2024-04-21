use async_trait::async_trait;
use futures::StreamExt;
use hyper::{self};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tokio::sync::{broadcast as tokio_broadcast, mpsc as tokio_mpsc};

use crate::{
    open_websocket_connection, DTPSError, ErrorMsg, FinishedMsg, MsgWebsocketPushClientToServer,
    MsgWebsocketPushServerToClient, PushResult, RawData, TypeOfConnection, DTPSR,
};

use crate::get_metadata;
use crate::receive_from_server;
use crate::send_to_server;
use crate::websocket_abstractions::AnySocketConnection;

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

// #[async_trait]
// pub trait WebsocketPushInterface {
//     async fn push(&mut self, raw_data: &RawData) -> DTPSR<()>;
//     async fn stop(&mut self) -> DTPSR<()>;
// }

// struct WebsocketPush {
//     // con: TypeOfConnection,
//     wsc: AnySocketConnection,
//     rx: mpsc::Receiver<TM>,
//     tx: mpsc::UnboundedSender<TM>,
// }
//
// impl WebsocketPush {
//     pub async fn new(con: TypeOfConnection) -> DTPSR<Self> {
//         let wsc = open_websocket_connection(&con).await?;
//         let tx: mpsc::UnboundedSender<_> = wsc.send_outgoing().await;
//         let rx: mpsc::Receiver<_> = wsc.get_incoming().await;
//         Ok(Self { wsc, tx, rx })
//     }
// }

pub async fn websocket_push(con: TypeOfConnection) -> DTPSR<AnySocketConnection> {
    let md = get_metadata(&con).await?;
    let con = match md.stream_push_url {
        None => {
            return Err(DTPSError::NotAvailable("No stream push URL configured".to_string()));
        }
        Some(u) => u,
    };

    let c = open_websocket_connection(&con).await?;
    Ok(c)
}
