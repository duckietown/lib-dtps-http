use futures::{
    SinkExt,
    StreamExt,
};
use tokio::{
    process::Command,
    task::JoinHandle,
};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::{
    client::get_rawdata_accept,
    debug_with_info,
    get_events_stream_inline,
    get_history,
    get_index,
    get_metadata,
    get_rawdata,
    info_with_info,
    interpret_resp,
    make_request,
    DTPSError,
    DataFromChannel,
    FoundMetadata,
    ListenURLEvents,
    TopicName,
    TypeOfConnection,
    DTPSR,
    TOPIC_LIST_CLOCK,
};

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use crate::{
        init_logging,
        TypeOfConnection,
    };

    use super::*;

    #[tokio::test]
    async fn test_python1() -> DTPSR<()> {
        init_logging();
        let path = "/tmp/dtps-tests/test_python1/socket";
        let cmd = vec!["dtps-http-py-server-example-clock", "--unix-path", path];

        // create process given by command above
        let mut child = Command::new(cmd[0]).args(&cmd[1..]).spawn().unwrap();

        // await everything ready
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        let con = TypeOfConnection::unix_socket(path);

        let res = check_server(&con).await;

        child.kill().await?;
        res.unwrap();
        Ok(())
    }
}

pub async fn check_server(con: &TypeOfConnection) -> DTPSR<()> {
    let md = get_metadata(con).await?;
    info_with_info!("metadata: {:#?}", md);
    let index = get_index(con).await?;

    info_with_info!("index: {:#?}", index);

    let mut handles: Vec<JoinHandle<DTPSR<()>>> = vec![];

    for (topic, data) in &index.topics {
        let r0 = data.reachability.get(0).unwrap();

        let handle = tokio::spawn(check_topic(topic.clone(), r0.con.clone()));

        handles.push(handle);
    }

    // wait for all handles to finish
    for handle in handles {
        handle.await??;
    }
    let topic_clock = TopicName::from_dash_sep(TOPIC_LIST_CLOCK)?;

    let clock = index.topics.get(&topic_clock).unwrap();
    let r = clock.reachability.get(0).unwrap();
    let md = get_metadata(&r.con).await?;
    if md.events_data_inline_url.is_none() {
        return Err(DTPSError::from("events_data_inline_url is None"));
    }
    let (handle, stream) = get_events_stream_inline(&md.events_data_inline_url.unwrap()).await;

    read_notifications(handle, stream, 3).await?;

    let (handle, stream) = get_events_stream_inline(&md.events_url.unwrap()).await;

    read_notifications(handle, stream, 3).await?;

    Ok(())
}

fn check_complete_metadata(md: &FoundMetadata) -> DTPSR<()> {
    if md.answering.is_none() {
        return Err(DTPSError::from("answering is None"));
    }
    if md.events_url.is_none() {
        return Err(DTPSError::from("events_url is None"));
    }
    if md.events_data_inline_url.is_none() {
        return Err(DTPSError::from("events_data_inline_url is None"));
    }
    if md.meta_url.is_none() {
        return Err(DTPSError::from("meta is None"));
    }
    if md.history_url.is_none() {
        return Err(DTPSError::from("history_url is None"));
    }
    Ok(())
}

async fn read_notifications(
    handle: JoinHandle<DTPSR<()>>,
    mut stream: UnboundedReceiverStream<ListenURLEvents>,
    nmin: usize,
) -> DTPSR<()> {
    let mut i = 0;
    loop {
        let ne = stream.next().await;
        match ne {
            None => {
                debug_with_info!("finished stream");
                break;
            }
            Some(notification) => {
                debug_with_info!("clock notification: {:#?}", notification);

                i += 1;
            }
        }
        if i >= nmin {
            break;
        }
    }
    drop(stream);
    handle.await??;

    if i < nmin {
        return Err(DTPSError::from("not enough notifications"));
    }

    Ok(())
}

async fn check_topic(topic: TopicName, con: TypeOfConnection) -> DTPSR<()> {
    debug_with_info!("check_topic {topic:?}...");
    let x = get_rawdata_accept(&con, Some("text/html")).await?;
    //
    // let resp = make_request(&con, hyper::Method::GET, b"", None, Some("text/html")).await?;
    // let x = interpret_resp(&con, resp).await?;
    // debug_with_info!("check_topic {topic:?}... {x:?}");
    assert_eq!(x.content_type, "text/html", "ok {}", con);

    let md = get_metadata(&con).await?;

    info_with_info!("{topic:#?}: {md:#?}");

    // check_complete_metadata(&md)?;
    if md.meta_url.is_none() {
        return Err(DTPSError::from(format!("{:?}: meta is None", topic.as_dash_sep())));
    }

    let index = get_index(&md.meta_url.unwrap()).await?;
    debug_with_info!("check_topic {topic:?} {index:?}");

    if let Some(x) = &md.history_url {
        let history = get_history(x).await?;
        debug_with_info!("check_topic {topic:?} {history:?}");
    }

    debug_with_info!("check_topic {topic:?} OK");

    Ok(())
}
