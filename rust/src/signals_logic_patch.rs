use anyhow::Context;
use async_trait::async_trait;
use futures::StreamExt;
use json_patch::{
    patch,
    Patch,
    PatchError,
    PatchOperation,
};

use crate::{
    context,
    debug_with_info,
    dtpserror_context,
    error_with_info,
    internal_assertion,
    invalid_input,
    not_implemented,
    parse_url_ext,
    server_state::ServerState,
    unescape_json_patch,
    ConnectionJob,
    ConnectionJobWire,
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
impl Patchable for TypeOFSource {
    async fn patch(&self, presented_as: &str, ssa: ServerStateAccess, patch: &Patch) -> DTPSR<()> {
        debug_with_info!("patching {self:#?} with {patch:#?}");
        match self {
            TypeOFSource::ForwardedQueue(_) => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::OurQueue(topic_name, ..) => {
                dtpserror_context!(
                    if topic_name.as_dash_sep() == TOPIC_PROXIED {
                        patch_proxied(ssa.clone(), topic_name, patch).await
                    } else if topic_name.as_dash_sep() == TOPIC_CONNECTIONS {
                        patch_connection(ssa.clone(), topic_name, patch).await
                    } else {
                        patch_our_queue(ssa, patch, topic_name).await
                    },
                    "cannot patch our queue {} with this patch\n{patch:#?}",
                    topic_name.as_dash_sep()
                )
            }
            TypeOFSource::MountedDir(..) => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::MountedFile { .. } => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::Compose(sc) => patch_composition(ssa, patch, sc).await,
            TypeOFSource::Transformed(ts_inside, transform) => {
                patch_transformed(ssa, presented_as, patch, ts_inside, transform).await
            }
            TypeOFSource::Digest(..) => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::Deref(..) => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::Index(..) => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::Aliased(..) => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::History(..) => {
                invalid_input!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::OtherProxied(..) => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
        }
    }
}

pub fn add_prefix_to_patch_op(op: &PatchOperation, prefix: &str) -> PatchOperation {
    match op {
        PatchOperation::Add(y) => {
            let mut y = y.clone();
            y.path = format!("{prefix}{path}", path = y.path, prefix = prefix);
            PatchOperation::Add(y)
        }
        PatchOperation::Remove(y) => {
            let mut y = y.clone();
            y.path = format!("{prefix}{path}", path = y.path, prefix = prefix);
            PatchOperation::Remove(y)
        }
        PatchOperation::Replace(y) => {
            let mut y = y.clone();
            y.path = format!("{prefix}{path}", path = y.path, prefix = prefix);
            PatchOperation::Replace(y)
        }
        PatchOperation::Move(y) => {
            let mut y = y.clone();
            y.from = format!("{prefix}{from}", from = y.from, prefix = prefix);
            y.path = format!("{prefix}{path}", path = y.path, prefix = prefix);
            PatchOperation::Move(y)
        }
        PatchOperation::Copy(y) => {
            let mut y = y.clone();
            y.from = format!("{prefix}{from}", from = y.from, prefix = prefix);
            y.path = format!("{prefix}{path}", path = y.path, prefix = prefix);
            PatchOperation::Copy(y)
        }
        PatchOperation::Test(y) => {
            let mut y = y.clone();
            y.path = format!("{prefix}{path}", path = y.path, prefix = prefix);
            PatchOperation::Test(y)
        }
    }
}

pub fn add_prefix_to_patch(patch: &Patch, prefix: &str) -> Patch {
    // let mut new_patch = Patch::();
    let mut ops: Vec<PatchOperation> = Vec::new();
    for op in &patch.0 {
        let op1 = add_prefix_to_patch_op(op, prefix);
        ops.push(op1);
    }
    Patch(ops)
}

async fn patch_composition(ss_mutex: ServerStateAccess, patch: &Patch, sc: &SourceComposition) -> DTPSR<()> {
    let mut ss = ss_mutex.lock().await;

    for x in &patch.0 {
        match x {
            PatchOperation::Add(ao) => {
                let topic_name = topic_name_from_json_pointer(ao.path.as_str())?;
                let value = ao.value.clone();
                let tra: TopicRefAdd = serde_json::from_value(value)?;
                ss.new_topic_ci(
                    &topic_name,
                    Some(tra.app_data),
                    &tra.properties,
                    &tra.content_info,
                    None,
                )?;
            }
            PatchOperation::Remove(ro) => {
                let topic_name = topic_name_from_json_pointer(ro.path.as_str())?;
                ss.remove_topic(&topic_name)?;
            }
            PatchOperation::Replace(_)
            | PatchOperation::Move(_)
            | PatchOperation::Copy(_)
            | PatchOperation::Test(_) => {
                return Err(DTPSError::NotImplemented(format!(
                    "patch_composition: {sc:#?} with {patch:?}"
                )));
            }
        }
    }

    Ok(())
}

pub fn topic_name_from_json_pointer(path: &str) -> DTPSR<TopicName> {
    let mut components = Vec::new();
    for p in path.split('/') {
        if p.is_empty() {
            continue;
        }
        components.push(p.to_string());
    }

    Ok(TopicName::from_components(&components))
}

async fn patch_transformed(
    ss_mutex: ServerStateAccess,
    presented_as: &str,
    patch: &Patch,
    ts: &TypeOFSource,
    transform: &Transforms,
) -> DTPSR<()> {
    debug_with_info!("patch_transformed:\n{ts:#?}\n---\n{transform:?}\n---\n{patch:?}");
    match transform {
        Transforms::GetInside(path) => {
            let mut prefix = String::new();
            for p in path {
                prefix.push('/');
                prefix.push_str(p);
            }
            let patch2 = add_prefix_to_patch(patch, &prefix);
            // debug_with_info!("redirecting patch:\nbefore: {patch:#?} after: \n{patch2:#?}");
            ts.patch(presented_as, ss_mutex, &patch2).await
        }
    }
}

async fn patch_our_queue(ssa: ServerStateAccess, patch: &Patch, topic_name: &TopicName) -> DTPSR<()> {
    let mut ss = ssa.lock().await;

    let (data_saved, raw_data) = {
        // let ss = ss_mutex.lock().await;

        let oq = ss.get_queue(topic_name)?;
        if oq.saved.is_empty() {
            return invalid_input!("patch_our_queue: {topic_name:?} is empty");
        }
        let last = oq.stored.last().unwrap();
        let data_saved = oq.saved.get(last).unwrap();
        let content = ss.get_blob_bytes(&data_saved.digest)?;
        let raw_data = RawData::new(content, &data_saved.content_type);
        (data_saved.clone(), raw_data)
    };
    let mut x: serde_json::Value = raw_data.get_as_json()?;
    let x0 = x.clone();
    match json_patch::patch(&mut x, patch) {
        Ok(_) => {}
        Err(e) => {
            return invalid_input!("cannot apply patch to {topic_name:?} with {patch:?}: {e:?}",);
        }
    }
    if x == x0 {
        debug_with_info!("The patch didn't change anything:\n {patch:?}");
        return Ok(());
    }

    let new_content: RawData = RawData::encode_from_json(&x, &data_saved.content_type)?;
    let new_clocks = data_saved.clocks.clone();

    let ds = ss.publish(
        topic_name,
        &new_content.content,
        &data_saved.content_type,
        Some(new_clocks),
    )?;

    if ds.index != data_saved.index + 1 {
        // should never happen because we have acquired the lock
        return internal_assertion!(
            "there were some modifications inside ds.index={} data_saved.index={}:\n{ds:#?}\n!=\n{data_saved:#?}",
            ds.index,
            data_saved.index
        );
    }

    Ok(())
}

async fn patch_proxied(ss_mutex: ServerStateAccess, topic_name: &TopicName, p: &Patch) -> DTPSR<()> {
    let mut ss = ss_mutex.lock().await;

    let s = ss.get_last_insert_assert_exists(topic_name)?;

    let x0: serde_json::Value = s.raw_data.get_as_json()?;
    let mut x = x0.clone();
    // debug_with_info!("patching:\n---{x0:?}---\n{p:?}\n");
    patch(&mut x, p)?;
    if x == x0 {
        debug_with_info!("The patch didn't change anything:\n {p:?}");
        return Ok(());
    }
    // debug_with_info!("patching:\n---\n{x0:?}\n---\n{x:?}");

    for po in &p.0 {
        match po {
            PatchOperation::Add(ao) => {
                let pj: ProxyJob = serde_json::from_value(ao.value.clone())?;
                let key = unescape_json_patch(&ao.path)[1..].to_string();
                let topic_name = TopicName::from_dash_sep(key)?;
                let mut urls = Vec::new();
                for u in pj.urls.iter() {
                    match parse_url_ext(u) {
                        Ok(u) => urls.push(u),
                        Err(e) => {
                            error_with_info!("cannot parse url: {u:?} {e:?}");
                        }
                    };
                }

                debug_with_info!("adding proxy: topic_name = {topic_name:?} urls = {urls:?}",);

                ss.add_proxy_connection(&topic_name, &urls, pj.node_id, ss_mutex.clone())?;
            }
            PatchOperation::Remove(ro) => {
                let key = unescape_json_patch(&ro.path)[1..].to_string();
                let topic_name = TopicName::from_dash_sep(key)?;
                ss.remove_proxy_connection(&topic_name)?;
            }
            PatchOperation::Replace(_)
            | PatchOperation::Move(_)
            | PatchOperation::Copy(_)
            | PatchOperation::Test(_) => {
                return Err(DTPSError::NotImplemented(format!("operation invalid with {p:?}")));
            }
        }
    }
    let new_content: RawData = RawData::encode_from_json(&x, &s.data_saved.content_type)?;
    let new_clocks = s.data_saved.clocks.clone();

    let ds = ss.publish(
        topic_name,
        &new_content.content,
        &s.data_saved.content_type,
        Some(new_clocks),
    )?;

    if ds.index != s.data_saved.index + 1 {
        return internal_assertion!("there were some modifications inside: {ds:?} != {:?}", s.data_saved);
    }

    Ok(())
}

async fn patch_connection(ssa: ServerStateAccess, topic_name: &TopicName, p: &Patch) -> DTPSR<()> {
    let mut ss = ssa.lock().await;
    let s = ss.get_last_insert_assert_exists(topic_name)?;
    let x0: serde_json::Value = s.raw_data.get_as_json()?;
    let mut x = x0.clone();
    // debug_with_info!("patching:\n---{x0:?}---\n{p:?}\n");
    patch(&mut x, p)?;
    if x == x0 {
        debug_with_info!("The patch didn't change anything:\n {p:?}");
        return Ok(());
    }
    if p.0.len() != 1 {
        return not_implemented!("PATCH operation only allowed of length 1 with {p:?}");
    }
    let po = &p.0[0];

    let notification = match po {
        PatchOperation::Add(ao) => {
            let pj: ConnectionJobWire = serde_json::from_value(ao.value.clone())?;
            let pj = ConnectionJob::from_wire(&pj)?;
            let key = unescape_json_patch(&ao.path)[1..].to_string();
            let name = TopicName::from_dash_sep(key)?;
            ss.add_topic_to_topic_connection_(&name, &pj, ssa.clone()).await?
        }
        PatchOperation::Remove(ao) => {
            let key = unescape_json_patch(&ao.path)[1..].to_string();
            let name = TopicName::from_dash_sep(key)?;
            ss.remove_topic_to_topic_connection_(&name).await?
        }
        PatchOperation::Replace(_) | PatchOperation::Move(_) | PatchOperation::Copy(_) | PatchOperation::Test(_) => {
            return not_implemented!("PATCH operation invalid with {p:?}");
        }
    };

    let new_content: RawData = RawData::encode_from_json(&x, &s.data_saved.content_type)?;
    let new_clocks = s.data_saved.clocks.clone();

    let ds = ss.publish(
        topic_name,
        &new_content.content,
        &s.data_saved.content_type,
        Some(new_clocks),
    )?;

    if ds.index != s.data_saved.index + 1 {
        return internal_assertion!(
            "there were some modifications inside ds.index={} data_saved.index={}:\norigin:\n{:#?}\nnew found:\n{:#?}",
            ds.index,
            s.data_saved.index,
            s.data_saved,
            ds
        );
    }

    drop(ss);
    notification.wait().await?;
    Ok(())
}
