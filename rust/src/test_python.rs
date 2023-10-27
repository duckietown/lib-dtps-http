use futures::{
    SinkExt,
    StreamExt,
};
use tempfile::tempdir;

use tokio::{
    process::Command,
    sync::broadcast::{
        error::RecvError,
        Receiver as BroadcastReceiver,
    },
    task::JoinHandle,
};

use crate::{
    client::get_rawdata_accept,
    debug_with_info,
    get_events_stream_inline,
    get_history,
    get_index,
    get_metadata,
    info_with_info,
    DTPSError,
    ListenURLEvents,
    TopicName,
    TypeOfConnection,
    DTPSR,
    TOPIC_LIST_CLOCK,
};

#[cfg(test)]
mod tests {
    use log::info;
    use std::fmt::Debug;

    use crate::{
        init_logging,
        TypeOfConnection,
    };

    use super::*;

    #[tokio::test]
    async fn test_python1() -> DTPSR<()> {
        init_logging();
        // generate a temp dir
        let dir = tempdir()?;
        let path0 = dir.path().join("socket");

        let path = path0.to_str().unwrap();
        // let path2 = "/tmp/dtps-tests/test_python1/socket2";
        // create directory
        // tokio::fs::create_dir_all("/tmp/dtps-tests/test_python1").await?;
        let cmd = vec!["dtps-http-py-server-example-clock", "--unix-path", path];

        // create process given by command above
        let mut child = Command::new(cmd[0])
            .args(&cmd[1..])
            .stdout(std::process::Stdio::inherit()) // Inherit the parent's stdout
            .stderr(std::process::Stdio::inherit()) // Inherit the parent's stderr
            .spawn()?;

        // wait that the socket exists but not more than 5 seconds
        let t0 = tokio::time::Instant::now();
        loop {
            let elapsed = t0.elapsed().as_secs();
            if tokio::fs::metadata(path).await.is_ok() {
                info!("found socket {path} after {elapsed} seconds");
                break;
            } else {
                if elapsed > 5 {
                    return Err(DTPSError::from("socket not found"));
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }

        // // await everything ready
        // tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        let con = TypeOfConnection::unix_socket(path);

        let res = check_server(&con).await;

        child.kill().await?;
        // if res.is_err() {
        //
        //     ret
        //     let res = res.unwrap_err();
        //     error_with_info!("error: {:#?}", res);
        // }
        // res.unwrap();
        // Ok(())
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
//
// fn check_complete_metadata(md: &FoundMetadata) -> DTPSR<()> {
//     if md.answering.is_none() {
//         return Err(DTPSError::from("answering is None"));
//     }
//     if md.events_url.is_none() {
//         return Err(DTPSError::from("events_url is None"));
//     }
//     if md.events_data_inline_url.is_none() {
//         return Err(DTPSError::from("events_data_inline_url is None"));
//     }
//     if md.meta_url.is_none() {
//         return Err(DTPSError::from("meta is None"));
//     }
//     if md.history_url.is_none() {
//         return Err(DTPSError::from("history_url is None"));
//     }
//     Ok(())
// }

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
