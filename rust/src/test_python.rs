use futures::{SinkExt, StreamExt};
use tokio::{
    sync::broadcast::{error::RecvError, Receiver as BroadcastReceiver},
    task::JoinHandle,
};

use crate::get_events_stream_inline;
use crate::get_history;
use crate::get_index;
use crate::get_metadata;
use crate::get_rawdata_accept;
use crate::{
    debug_with_info, info_with_info, DTPSError, ListenURLEvents, TopicName, TypeOfConnection, DTPSR, TOPIC_LIST_CLOCK,
};

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use crate::test_fixtures::instance_python_test_fixture;
    use crate::test_range;

    use super::*;

    #[tokio::test]
    async fn test_python1() -> DTPSR<()> {
        let cf = instance_python_test_fixture().await?;

        let res = check_server(&cf.con).await;

        cf.finish().await?;

        res
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

async fn read_notifications(
    handle: JoinHandle<DTPSR<()>>,
    mut rx: BroadcastReceiver<ListenURLEvents>,
    nmin: usize,
) -> DTPSR<()> {
    let mut i = 0;
    loop {
        match rx.recv().await {
            Ok(x) => {
                debug_with_info!("clock notification: {:#?}", x);

                i += 1;
            }
            Err(e) => {
                match e {
                    RecvError::Closed => {
                        debug_with_info!("finished stream");

                        break;
                    }
                    RecvError::Lagged(_) => continue, // TODO: warning
                }
            }
        };

        if i >= nmin {
            break;
        }
    }
    drop(rx);
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
