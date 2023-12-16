use std::any::Any;

use futures::SinkExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tungstenite::Message as TM;

use crate::error_with_info;
use crate::{DTPSError, DTPSR};

pub async fn send_to_server<T>(tx: &mut futures::channel::mpsc::UnboundedSender<TM>, m: &T) -> DTPSR<()>
where
    T: Serialize,
{
    let ascbor = serde_cbor::to_vec(m)?;
    let msg = TM::binary(ascbor);
    return match tx.send(msg).await {
        Ok(_) => Ok(()),
        Err(e) => Err(DTPSError::Other(e.to_string())),
    };
}

// returns None if closed
pub async fn receive_from_server<T>(rx: &mut Receiver<TM>) -> DTPSR<Option<T>>
where
    T: DeserializeOwned,
{
    let msg = match rx.recv().await {
        Ok(msg) => msg,
        Err(e) => {
            return match e {
                RecvError::Closed => Ok(None),
                RecvError::Lagged(_) => {
                    let s = "lagged".to_string();
                    DTPSError::other(s)
                }
            };
        }
    };
    if !msg.is_binary() {
        // pragma: no cover
        let s = format!("unexpected message, expected binary {msg:#?}");
        return DTPSError::other(s);
    }
    let data = msg.clone().into_data();

    let msg_from_server: T = match serde_cbor::from_slice::<T>(&data) {
        Ok(dr_) => {
            // debug_with_info!("dr: {:#?}", dr_);
            dr_
        }
        Err(e) => {
            // pragma: no cover
            let rawvalue = serde_cbor::from_slice::<serde_cbor::Value>(&data);
            let s = format!(
                " cannot parse cbor as MsgServerToClient: {:#?}\n{:#?}\n{:#?}",
                e,
                msg.type_id(),
                rawvalue,
            );
            error_with_info!("{}", s);
            return DTPSError::other(s);
        }
    };
    Ok(Some(msg_from_server))
}
