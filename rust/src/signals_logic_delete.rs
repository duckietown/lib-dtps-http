use async_trait::async_trait;
use futures::StreamExt;
use json_patch::{patch, Patch, PatchOperation};
use log::info;
use tokio::sync::{broadcast as tokio_broadcast, mpsc as tokio_mpsc};

use crate::signals_logic::Deletable;
use crate::utils_patch::unescape_json_patch;
use crate::{
    debug_with_info, dtpserror_context, dtpserror_other, error_with_info, internal_assertion, invalid_input,
    make_request, not_implemented, parse_url_ext, patch_data, ConnectionJob, ConnectionJobWire, DTPSError, DataSaved,
    Patchable, ProxyJob, RawData, ServerStateAccess, SourceComposition, TopicName, TopicRefAdd, Transforms,
    TypeOFSource, DTPSR, TOPIC_CONNECTIONS, TOPIC_PROXIED,
};

#[async_trait]
impl Deletable for TypeOFSource {
    async fn delete(&self, presented_as: &str, ssa: ServerStateAccess) -> DTPSR<()> {
        // debug_with_info!("patching {self:#?} with {patch:#?}");
        match self {
            TypeOFSource::ForwardedQueue(fq) => {
                let con = {
                    let ss = ssa.lock().await;
                    let sub = ss.proxied.get(&fq.subscription).unwrap();
                    match &sub.established {
                        None => {
                            // let msg =
                            return dtpserror_other!("Subscription not established");
                            // let res = http::Response::builder()
                            //     .status(StatusCode::NOT_FOUND) //ok
                            //     .body(Body::from(msg.to_string()))
                            //     .unwrap();
                            // return Ok(res);
                        }
                        Some(est) => est.using.join(fq.his_topic_name.as_relative_url())?,
                    }
                };
                // let use_url = &ss.proxied_topics.get(&q.my_topic_name).unwrap().data_url;
                let empty_body_bytes = vec![];
                make_request(&con, hyper::Method::DELETE, &empty_body_bytes, None, None).await?;
                Ok(())
            }
            TypeOFSource::OurQueue(topic_name, ..) => {
                {
                    let mut ss = ssa.lock().await;
                    let properties = &ss.oqs.get(topic_name).unwrap().tr.properties;

                    return if properties.droppable {
                        ss.oqs.remove(topic_name);
                        Ok(())
                    } else {
                        dtpserror_other!("Cannot delete a non-droppable queue")
                    };
                };
            }
            TypeOFSource::MountedDir(..) => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::MountedFile { .. } => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::Compose(sc) => {
                return dtpserror_other!("Cannot delete a derived resource");
            }
            TypeOFSource::Transformed(ts_inside, transform) => {
                return dtpserror_other!("Cannot delete a derived resource");
            }
            TypeOFSource::SingleUse(..) => {
                not_implemented!("delete for {self:#?} with {self:?}")
            }
            TypeOFSource::Deref(..) => {
                not_implemented!("delete for {self:#?} with {self:?}")
            }
            TypeOFSource::Index(..) => {
                not_implemented!("delete for {self:#?} with {self:?}")
            }
            TypeOFSource::Aliased(..) => {
                not_implemented!("delete for {self:#?} with {self:?}")
            }
            TypeOFSource::History(..) => {
                invalid_input!("delete for {self:#?} with {self:?}")
            }
            TypeOFSource::OtherProxied(..) => {
                not_implemented!("delete for {self:#?} with {self:?}")
            }
        }
    }
}
