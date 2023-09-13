use anyhow::Context;

use async_trait::async_trait;

use futures::StreamExt;
use json_patch::{
    patch,
    Patch,
    PatchOperation,
};

use crate::{
    context,
    debug_with_info,
    error_with_info,
    not_implemented,
    parse_url_ext,
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
    TOPIC_PROXIED,
};

#[async_trait]
impl Patchable for TypeOFSource {
    async fn patch(&self, presented_as: &str, ss_mutex: ServerStateAccess, patch: &Patch) -> DTPSR<()> {
        match self {
            TypeOFSource::ForwardedQueue(_) => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::OurQueue(topic_name, ..) => {
                if topic_name.as_dash_sep() == TOPIC_PROXIED {
                    patch_proxied(ss_mutex, topic_name, patch).await
                } else {
                    patch_our_queue(ss_mutex, patch, topic_name).await
                }
            }
            TypeOFSource::MountedDir(..) => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::MountedFile { .. } => {
                not_implemented!("patch for {self:#?} with {self:?}")
            }
            TypeOFSource::Compose(sc) => patch_composition(ss_mutex, patch, sc).await,
            TypeOFSource::Transformed(ts_inside, transform) => {
                patch_transformed(ss_mutex, presented_as, patch, ts_inside, transform).await
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
                not_implemented!("patch for {self:#?} with {self:?}")
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

async fn patch_our_queue(
    ss_mutex: ServerStateAccess,
    // _presented_as: &str,
    patch: &Patch,
    topic_name: &TopicName,
) -> DTPSR<()> {
    let (data_saved, raw_data) = {
        let ss = ss_mutex.lock().await;
        let oq = ss.get_queue(topic_name)?;
        // todo: if EMPTY...
        if oq.saved.is_empty() {
            return Err(DTPSError::TopicNotFound(format!(
                "patch_our_queue: {topic_name:?} is empty",
                topic_name = topic_name
            )));
        }
        let last = oq.stored.last().unwrap();
        let data_saved = oq.saved.get(last).unwrap();
        let content = ss.get_blob_bytes(&data_saved.digest)?;
        let raw_data = RawData::new(content, &data_saved.content_type);
        (data_saved.clone(), raw_data)
    };
    let mut x: serde_json::Value = raw_data.get_as_json()?;
    let x0 = x.clone();

    context!(json_patch::patch(&mut x, patch), "cannot apply patch")?;
    if x == x0 {
        debug_with_info!("The patch didn't change anything:\n {patch:?}");
        return Ok(());
    }

    let new_content: RawData = RawData::encode_from_json(&x, &data_saved.content_type)?;
    let new_clocks = data_saved.clocks.clone();

    let mut ss = ss_mutex.lock().await;

    // oq.push(&new_content, Some(new_clocks))?;
    let ds = ss.publish(
        topic_name,
        &new_content.content,
        &data_saved.content_type,
        Some(new_clocks),
    )?;

    if ds.index != data_saved.index + 1 {
        // should never happen because we have acquired the lock
        panic!("there were some modifications inside: {ds:?} != {data_saved:?}");
    }

    Ok(())
}

async fn patch_proxied(ss_mutex: ServerStateAccess, topic_name: &TopicName, p: &Patch) -> DTPSR<()> {
    let mut ss = ss_mutex.lock().await;

    let (data_saved, raw_data) = match ss.get_last_insert(topic_name)? {
        None => {
            // should not happen
            panic!("patch_proxied: {topic_name:?} is empty");
        }
        Some(s) => (s.data_saved, s.raw_data),
    };

    let x0: serde_json::Value = raw_data.get_as_json()?;
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
    let new_content: RawData = RawData::encode_from_json(&x, &data_saved.content_type)?;
    let new_clocks = data_saved.clocks.clone();

    let ds = ss.publish(
        topic_name,
        &new_content.content,
        &data_saved.content_type,
        Some(new_clocks),
    )?;

    if ds.index != data_saved.index + 1 {
        panic!("there were some modifications inside: {ds:?} != {data_saved:?}");
    }

    Ok(())
}
