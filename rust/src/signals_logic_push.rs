use anyhow::Context;
use async_trait::async_trait;
use futures::StreamExt;
use json_patch::{
    patch,
    Patch,
    PatchOperation,
};

use crate::{
    client::post_data,
    clocks::Clocks,
    context,
    debug_with_info,
    error_with_info,
    internal_assertion,
    invalid_input,
    not_implemented,
    parse_url_ext,
    server_state::{
        ConnectionJob,
        ConnectionJobWire,
    },
    signals_logic::Pushable,
    unescape_json_patch,
    DTPSError,
    Patchable,
    ProxyJob,
    RawData,
    ServerStateAccess,
    SourceComposition,
    TopicName,
    TopicRefAdd,
    Transforms,
    TypeOFSource,
    DTPSR,
    TOPIC_CONNECTIONS,
    TOPIC_PROXIED,
};

#[async_trait]
impl Pushable for TypeOFSource {
    async fn push(&self, ssa: ServerStateAccess, data: &RawData, clocks: &Clocks) -> DTPSR<()> {
        match self {
            TypeOFSource::ForwardedQueue(q) => {
                let ss = ssa.lock().await;
                let use_url = &ss.proxied_topics.get(&q.my_topic_name).unwrap().data_url;

                post_data(use_url, data).await?;
                Ok(())
            }
            TypeOFSource::OurQueue(topic_name, ..) => {
                let mut ss = ssa.lock().await;
                let clocks = Some(clocks.clone());
                ss.publish_raw_data(topic_name, data, clocks)?;
                Ok(())
            }

            TypeOFSource::OtherProxied(..) => {
                not_implemented!("push for {self:#?} with {self:?}")
            }

            TypeOFSource::MountedDir(..) => {
                not_implemented!("push for {self:#?} with {self:?}")
            }
            TypeOFSource::MountedFile { .. } => {
                not_implemented!("push for {self:#?} with {self:?}")
            }
            // These are not valid for push
            TypeOFSource::Compose(..)
            | TypeOFSource::Transformed(..)
            | TypeOFSource::Digest(..)
            | TypeOFSource::Deref(..)
            | TypeOFSource::Index(..)
            | TypeOFSource::Aliased(..)
            | TypeOFSource::History(..) => {
                invalid_input!("push for {self:#?} with {self:?}")
            }
        }
    }
}
