use json_patch::{AddOperation, Patch, PatchOperation, RemoveOperation};

use crate::connections::TypeOfConnection;
use crate::structures_topicref::TopicRefAdd;
use crate::{client_verbs, TopicName, DTPSR};

pub async fn create_topic(
    conbase: &TypeOfConnection,
    topic_name: &TopicName,
    tr: &TopicRefAdd,
) -> DTPSR<TypeOfConnection> {
    let mut path: String = String::new();
    for t in topic_name.as_components() {
        path.push('/');
        path.push_str(t);
    }

    let value = serde_json::to_value(tr)?;

    let pointer = jsonptr::PointerBuf::try_from(path).map_err(|e| {
        // You can customize the error handling here if needed
        // For now, just convert to a string error
        format!("Invalid JSON pointer: {}", e)
    })?;
    let add_operation = AddOperation { path: pointer, value };
    let operation1 = PatchOperation::Add(add_operation);
    let patch = Patch(vec![operation1]);
    client_verbs::patch_data(conbase, &patch).await?;
    let con = conbase.join(topic_name.as_relative_url())?;
    Ok(con)
}

pub async fn delete_topic(conbase: &TypeOfConnection, topic_name: &TopicName) -> DTPSR<()> {
    let mut path: String = String::new();
    for t in topic_name.as_components() {
        path.push('/');
        path.push_str(t);
    }

    let pointer = jsonptr::PointerBuf::try_from(path).map_err(|e| {
        format!("Invalid JSON pointer: {}", e)
    })?;
    let remove_operation = RemoveOperation { path: pointer };
    let operation1 = PatchOperation::Remove(remove_operation);
    let patch = Patch(vec![operation1]);

    client_verbs::patch_data(conbase, &patch).await.map(|_| ())
}
