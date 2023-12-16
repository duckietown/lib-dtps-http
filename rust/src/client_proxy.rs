use json_patch::{AddOperation, Patch, PatchOperation, RemoveOperation};

use crate::ProxyJob;
use crate::TypeOfConnection;
use crate::{client_verbs, not_available, utils_patch, DTPSError, TopicName, DTPSR};

pub async fn add_proxy(
    conbase: &TypeOfConnection,
    mountpoint: &TopicName,
    node_id: Option<String>,
    urls: &[TypeOfConnection],
    mask_origin: bool,
) -> DTPSR<()> {
    let md = crate::get_metadata(conbase).await?;
    let url = match md.proxied_url {
        None => {
            return not_available!(
                "cannot add proxy: no proxied url in metadata for {}",
                conbase.to_string()
            );
        }
        Some(url) => url,
    };

    let patch = create_add_proxy_patch(mountpoint, node_id, urls, mask_origin)?;

    client_verbs::patch_data(&url, &patch).await
}

fn create_add_proxy_patch(
    mountpoint: &TopicName,
    node_id: Option<String>,
    urls: &[TypeOfConnection],
    mask_origin: bool,
) -> Result<Patch, DTPSError> {
    let urls = urls.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    let pj = ProxyJob {
        node_id,
        urls,
        mask_origin,
    };

    let mut path: String = String::new();
    path.push('/');
    path.push_str(utils_patch::escape_json_patch(mountpoint.as_dash_sep()).as_str());
    let value = serde_json::to_value(pj)?;

    let add_operation = AddOperation { path, value };
    let operation1 = PatchOperation::Add(add_operation);
    let patch = json_patch::Patch(vec![operation1]);
    Ok(patch)
}

pub async fn remove_proxy(conbase: &TypeOfConnection, mountpoint: &TopicName) -> DTPSR<()> {
    let md = crate::get_metadata(conbase).await?;
    let url = match md.proxied_url {
        None => {
            return not_available!(
                "cannot remove proxy: no proxied url in metadata for {}",
                conbase.to_string()
            );
        }
        Some(url) => url,
    };

    let patch = create_remove_proxy_patch(mountpoint);

    client_verbs::patch_data(&url, &patch).await
}

fn create_remove_proxy_patch(mountpoint: &TopicName) -> Patch {
    let mut path: String = String::new();
    path.push('/');
    path.push_str(utils_patch::escape_json_patch(mountpoint.as_dash_sep()).as_str());

    let remove_operation = RemoveOperation { path };
    let operation1 = PatchOperation::Remove(remove_operation);
    json_patch::Patch(vec![operation1])
}
