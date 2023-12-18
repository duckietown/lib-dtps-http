use anyhow::Context;
use serde::Serialize;

use crate::connections::TypeOfConnection;
use crate::{context, post_data, DataSaved, RawData, DTPSR};

pub async fn publish(con: &TypeOfConnection, data: &RawData) -> DTPSR<DataSaved> {
    let r = post_data(con, data).await?;
    let ds = context!(
        r.rd.interpret_owned::<DataSaved>(),
        "Cannot interpret response to {con}:\n{:?}",
        r.rd
    )?;
    Ok(ds)
}

pub async fn publish_json<T>(con: &TypeOfConnection, value: &T) -> DTPSR<DataSaved>
where
    T: Serialize,
{
    let rd = RawData::encode_as_json(value)?;
    publish(con, &rd).await
}

pub async fn publish_cbor<T>(con: &TypeOfConnection, value: &T) -> DTPSR<DataSaved>
where
    T: Serialize,
{
    let rd = RawData::encode_as_cbor(value)?;

    let ds = context!(publish(con, &rd).await, "publishing to {con}")?;
    Ok(ds)
}
