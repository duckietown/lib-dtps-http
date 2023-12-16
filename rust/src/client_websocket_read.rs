use bytes::Bytes;
use tokio::sync::broadcast::Receiver as BroadcastReceiver;
use tokio::task::JoinHandle;

use crate::connections::TypeOfConnection;
use crate::error_with_info;
use crate::object_queues::InsertNotification;
use crate::websocket_abstractions::{open_websocket_connection, GenericSocketConnection};
use crate::{client_verbs, utils_websocket, DTPSError, ListenURLEvents, MsgServerToClient, RawData, DTPSR};

/// Note: need to have use futures::{StreamExt} in scope to use this
pub async fn get_events_stream_inline(
    url: &TypeOfConnection,
) -> (JoinHandle<DTPSR<()>>, BroadcastReceiver<ListenURLEvents>) {
    let (tx, rx) = tokio::sync::broadcast::channel(1024);
    let inline_url = url.clone();
    let handle = tokio::spawn(listen_events_websocket(inline_url, tx));
    // let stream = UnboundedReceiverStream::new(rx);
    (handle, rx)
}

pub async fn listen_events_websocket(
    con: TypeOfConnection,
    tx: tokio::sync::broadcast::Sender<ListenURLEvents>,
) -> DTPSR<()> {
    let wsc = open_websocket_connection(&con).await?;
    let prefix = format!("listen_events_websocket({con})");
    // debug_with_info!("starting to listen to events for {} on {:?}", con, read);
    let mut index: u32 = 0;
    let mut rx = wsc.get_incoming().await;

    let first = utils_websocket::receive_from_server(&mut rx).await?;
    match first {
        Some(MsgServerToClient::ChannelInfo(..)) => {}
        _ => {
            let s = format!("Expected ChannelInfo, got {first:?}");
            error_with_info!("{}", s);
            return DTPSError::other(s);
        }
    }

    loop {
        let msg_from_server = utils_websocket::receive_from_server(&mut rx).await?;
        let dr = match msg_from_server {
            None => {
                break;
            }
            Some(MsgServerToClient::DataReady(dr_)) => dr_,

            _ => {
                let s = format!("{prefix}: message #{index}: unexpected message: {msg_from_server:#?}");
                error_with_info!("{}", s);
                return DTPSError::other(s);
            }
        };
        index += 1;

        let content: Vec<u8> = if dr.chunks_arriving == 0 {
            if dr.availability.is_empty() {
                let s = format!("{prefix}: message #{index}: availability is empty. listening to {con}",);
                return DTPSError::other(s);
            }
            let avail = dr.availability.get(0).unwrap();
            let url = con.join(&avail.url)?;

            client_verbs::get_rawdata(&url).await?.content.to_vec()
        } else {
            let mut content: Vec<u8> = Vec::with_capacity(dr.content_length);
            for _ in 0..(dr.chunks_arriving) {
                let msg_from_server = utils_websocket::receive_from_server(&mut rx).await?;

                let chunk = match msg_from_server {
                    None => break,

                    Some(x) => match x {
                        MsgServerToClient::Chunk(chunk) => chunk,

                        MsgServerToClient::DataReady(..)
                        | MsgServerToClient::ChannelInfo(..)
                        | MsgServerToClient::WarningMsg(..)
                        | MsgServerToClient::SilenceMsg(..)
                        | MsgServerToClient::ErrorMsg(..)
                        | MsgServerToClient::FinishedMsg(..) => {
                            let s = format!("{prefix}: unexpected message : {x:#?}");
                            return DTPSError::other(s);
                        }
                    },
                };

                let data = chunk.data;
                content.extend_from_slice(&data);

                index += 1;
            }
            content
        };

        if content.len() != dr.content_length {
            // pragma: no cover
            let s = format!(
                "{prefix}: unexpected content length: {} != {}",
                content.len(),
                dr.content_length
            );
            return DTPSError::other(s);
        }
        let content_type = dr.content_type.clone();
        let rd = RawData {
            content: Bytes::from(content),
            content_type,
        };
        let notification = ListenURLEvents::InsertNotification(InsertNotification {
            data_saved: dr.as_data_saved(),
            raw_data: rd,
        });

        if tx.send(notification).is_err() {
            break;
        }
    }
    Ok(())
}
