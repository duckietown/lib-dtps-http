use crate::connections::TypeOfConnection;
use crate::{client_verbs, DTPSError, History, CONTENT_TYPE_TOPIC_HISTORY_CBOR, DTPSR};
use crate::{error_with_info, not_available};

pub async fn get_history(con: &TypeOfConnection) -> DTPSR<History> {
    let rd = client_verbs::get_rawdata(con).await?;
    let content_type = rd.content_type;
    if content_type != CONTENT_TYPE_TOPIC_HISTORY_CBOR {
        // pragma: no cover
        return not_available!("Expected content type {CONTENT_TYPE_TOPIC_HISTORY_CBOR}, obtained {content_type} ");
    }
    let x = serde_cbor::from_slice::<History>(&rd.content);
    match x {
        Ok(x) => Ok(x),
        Err(e) => {
            // pragma: no cover
            let value: serde_cbor::Value = serde_cbor::from_slice(&rd.content)?;
            let s = format!("cannot parse as CBOR:\n{:#?}", value);
            error_with_info!("{}", s);
            error_with_info!("content: {:#?}", e);
            DTPSError::other(s)
        }
    }
}
