use anyhow::Context;
use serde::Serialize;

use crate::connections::TypeOfConnection;
use crate::local_optimization::{optimized_publish};
use crate::{context, post_data, DataSaved, RawData, DTPSR};

pub async fn publish(con: &TypeOfConnection, data: &RawData) -> DTPSR<DataSaved> {
    // Try optimized publish first for local connections
    optimized_publish(con, data).await
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
