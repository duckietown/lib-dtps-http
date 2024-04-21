use std::fmt::Debug;

use futures::StreamExt;
use hyper::{self};
use serde::{Deserialize, Serialize};

use crate::get_metadata;
use crate::websocket_abstractions::AnySocketConnection;
use crate::{open_websocket_connection, DTPSError, ErrorMsg, FinishedMsg, RawData, TypeOfConnection, DTPSR};

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
