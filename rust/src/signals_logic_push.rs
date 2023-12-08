use anyhow::Context;
use async_trait::async_trait;
use futures::StreamExt;
use json_patch::{patch, Patch, PatchOperation, ReplaceOperation};

use crate::{
    client::post_data,
    clocks::Clocks,
    context, debug_with_info, dtpserror_other, error_with_info, internal_assertion, invalid_input, not_implemented,
    parse_url_ext,
    server::handle_topic_post,
    server_state::{ConnectionJob, ConnectionJobWire},
    signals_logic::Pushable,
    signals_logic_patch::add_prefix_to_patch,
    unescape_json_patch, DTPSError, Patchable, ProxyJob, RawData, ServerStateAccess, SourceComposition, TopicName,
    TopicRefAdd, Transforms, TypeOFSource, DTPSR, TOPIC_CONNECTIONS, TOPIC_PROXIED,
};

#[async_trait]
impl Pushable for TypeOFSource {
    async fn push(&self, ssa: ServerStateAccess, data: &RawData, clocks: &Clocks) -> DTPSR<()> {
        match self {
            TypeOFSource::ForwardedQueue(fq) => {
                // let ss = ssa.lock().await;
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

                let rd = post_data(&con, data).await?;
                Ok(())
            }
            TypeOFSource::OurQueue(topic_name, ..) => {
                let mut ss = ssa.lock().await;
                let clocks = Some(clocks.clone());
                // handle_topic_post(&topic_name, ss_mutex, &rd).await
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
            TypeOFSource::Transformed(source, transform) => {
                match transform {
                    Transforms::GetInside(components) => {
                        let mut prefix = String::new();
                        for p in components {
                            prefix.push('/');
                            prefix.push_str(p);
                        }
                        let value = data.get_as_json()?;
                        let ops = vec![
                            PatchOperation::Replace(
                                ReplaceOperation {
                                    path: prefix,
                                    value,
                                }
                            )
                        ];
                        let patch = Patch(ops);

                        // let patch2 = add_prefix_to_patch(patch, &prefix);
                        // debug_with_info!("redirecting patch:\nbefore: {patch:#?} after: \n{patch2:#?}");
                        let presented_as = "unset";  // TODO: fix this
                        source.patch(presented_as, ssa, &patch).await
                    }
                }
            }
            // These are not valid for push
            TypeOFSource::Compose(..)
            // | TypeOFSource::Transformed(..)
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
