use serde::Serialize;

use crate::client_websocket_push::WebsocketPushInterface;
use crate::connections::TypeOfConnection;
use crate::server_state::ConnectionJob;
use crate::structures_topicref::{TopicRefAdd, TopicsIndexInternal};
use crate::{CompositeName, DataSaved, FoundMetadata, History, RawData, TopicName, DTPSR};

pub struct DTPSLowLevel {}

impl DTPSLowLevel {
    pub fn new() -> Self {
        DTPSLowLevel {}
    }

    pub async fn add_tpt_connection(
        conbase: &TypeOfConnection,
        connection_name: &CompositeName,
        connection_job: &ConnectionJob,
    ) -> DTPSR<()> {
        crate::client_tpt::add_tpt_connection(conbase, connection_name, connection_job).await
    }

    pub async fn remove_tpt_connection(conbase: &TypeOfConnection, connection_name: &CompositeName) -> DTPSR<()> {
        crate::client_tpt::remove_tpt_connection(conbase, connection_name).await
    }

    pub async fn get_index(con: &TypeOfConnection) -> DTPSR<TopicsIndexInternal> {
        crate::client_index::get_index(con).await
    }

    pub async fn get_metadata(con: &TypeOfConnection) -> DTPSR<FoundMetadata> {
        crate::client_metadata::get_metadata(con).await
    }

    pub async fn websocket_push(con: TypeOfConnection) -> DTPSR<Box<dyn WebsocketPushInterface>> {
        crate::client_websocket_push::websocket_push(con).await
    }
    pub async fn create_topic(
        conbase: &TypeOfConnection,
        topic_name: &TopicName,
        tr: &TopicRefAdd,
    ) -> DTPSR<TypeOfConnection> {
        crate::client_topics::create_topic(conbase, topic_name, tr).await
    }

    pub async fn delete_topic(conbase: &TypeOfConnection, topic_name: &TopicName) -> DTPSR<()> {
        crate::client_topics::delete_topic(conbase, topic_name).await
    }

    pub async fn publish(con: &TypeOfConnection, data: &RawData) -> DTPSR<DataSaved> {
        crate::client_publish::publish(con, data).await
    }

    pub async fn publish_json<T>(con: &TypeOfConnection, value: &T) -> DTPSR<DataSaved>
    where
        T: Serialize,
    {
        crate::client_publish::publish_json(con, value).await
    }

    pub async fn publish_cbor<T>(con: &TypeOfConnection, value: &T) -> DTPSR<DataSaved>
    where
        T: Serialize,
    {
        crate::client_publish::publish_cbor(con, value).await
    }

    pub async fn add_proxy(
        conbase: &TypeOfConnection,
        mountpoint: &TopicName,
        node_id: Option<String>,
        urls: &[TypeOfConnection],
        mask_origin: bool,
    ) -> DTPSR<()> {
        crate::client_proxy::add_proxy(conbase, mountpoint, node_id, urls, mask_origin).await
    }

    pub async fn remove_proxy(conbase: &TypeOfConnection, mountpoint: &TopicName) -> DTPSR<()> {
        crate::client_proxy::remove_proxy(conbase, mountpoint).await
    }
    pub async fn get_history(con: &TypeOfConnection) -> DTPSR<History> {
        crate::client_history::get_history(con).await
    }
}
