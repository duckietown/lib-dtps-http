use json_patch::{AddOperation, Patch, PatchOperation, RemoveOperation};

use crate::not_available;
use crate::CompositeName;
use crate::ConnectionJob;
use crate::TypeOfConnection;
use crate::{client_verbs, utils_patch, DTPSError, DTPSR};

pub async fn add_tpt_connection(
    conbase: &TypeOfConnection,
    connection_name: &CompositeName,
    connection_job: &ConnectionJob,
) -> DTPSR<()> {
    let md = crate::get_metadata(conbase).await?;
    let url = match md.connections_url {
        None => {
            return not_available!(
                "cannot remove connection: no connections_url in metadata for {}",
                conbase.to_url_repr()
            );
        }
        Some(url) => url,
    };

    let patch = create_add_tpt_connection_patch(connection_name, connection_job)?;

    client_verbs::patch_data(&url, &patch).await.map(|_| ())
}

fn create_add_tpt_connection_patch(
    connection_name: &CompositeName,
    connection_job: &ConnectionJob,
) -> Result<Patch, DTPSError> {
    let mut path: String = String::new();
    path.push('/');
    path.push_str(utils_patch::escape_json_patch(connection_name.as_dash_sep()).as_str());

    let wire = connection_job.to_wire();
    let value = serde_json::to_value(wire)?;

    let add_operation = AddOperation { path, value };
    let operation1 = PatchOperation::Add(add_operation);
    let patch = Patch(vec![operation1]);
    Ok(patch)
}

pub async fn remove_tpt_connection(conbase: &TypeOfConnection, connection_name: &CompositeName) -> DTPSR<()> {
    let md = crate::get_metadata(conbase).await?;
    let url = match md.connections_url {
        None => {
            return not_available!(
                "cannot remove connection: no connections_url in metadata for {}",
                conbase.to_url_repr()
            );
        }
        Some(url) => url,
    };

    let patch = create_remove_tpt_connection_patch(connection_name);

    client_verbs::patch_data(&url, &patch).await.map(|_| ())
}

fn create_remove_tpt_connection_patch(connection_name: &CompositeName) -> Patch {
    let mut path: String = String::new();
    path.push('/');
    path.push_str(utils_patch::escape_json_patch(connection_name.as_dash_sep()).as_str());

    let remove_operation = RemoveOperation { path };
    let operation1 = PatchOperation::Remove(remove_operation);
    Patch(vec![operation1])
}
