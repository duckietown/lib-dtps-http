use std::boxed::Box;
use std::fmt::Debug;

use async_trait::async_trait;
use futures::StreamExt;
use hyper::{self};
use serde::{Deserialize, Serialize};
use tungstenite::Message as TM;

use crate::get_metadata;
use crate::receive_from_server;
use crate::send_to_server;
use crate::GenericSocketConnection;
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
    // con: TypeOfConnection,
    wsc: Box<dyn GenericSocketConnection>,
    rx: tokio::sync::broadcast::Receiver<TM>,
    tx: futures::channel::mpsc::UnboundedSender<TM>,
}

impl WebsocketPush {
    pub async fn new(con: TypeOfConnection) -> DTPSR<Self> {
        let wsc = open_websocket_connection(&con).await?;
        let tx = wsc.send_outgoing().await;
        let rx = wsc.get_incoming().await;
        Ok(Self { wsc, tx, rx })
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

    Ok(Box::new(WebsocketPush::new(con).await?))
}
