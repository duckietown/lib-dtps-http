use crate::TypeOfConnection;
use crate::{error_with_info, not_available};
use crate::{get_rawdata, DTPSError, CONTENT_TYPE_DTPS_INDEX_CBOR, DTPSR};
use crate::{TopicsIndexInternal, TopicsIndexWire};

pub async fn get_index(con: &TypeOfConnection) -> DTPSR<TopicsIndexInternal> {
    let rd = get_rawdata(con).await?;

    // let h = resp.headers();
    // debug_with_info!("headers: {h:?}");

    // debug_with_info!("{con:?}: content type {content_type}");
    let content_type = rd.content_type;
    if content_type != CONTENT_TYPE_DTPS_INDEX_CBOR {
        // pragma: no cover
        return not_available!("Expected content type {CONTENT_TYPE_DTPS_INDEX_CBOR}, obtained {content_type} ");
    }
    let x = serde_cbor::from_slice::<TopicsIndexWire>(&rd.content);
    if x.is_err() {
        // pragma: no cover
        let value: serde_cbor::Value = serde_cbor::from_slice(&rd.content)?;
        let s = format!("cannot parse as CBOR:\n{:#?}", value);
        error_with_info!("{}", s);
        error_with_info!("content: {:#?}", x);
        return DTPSError::other(s);
    }
    let x0 = x.unwrap();

    let ti = TopicsIndexInternal::from_wire(&x0, con);

    Ok(ti)
}
