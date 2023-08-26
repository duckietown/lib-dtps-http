use futures::{SinkExt, StreamExt};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::{get_index, get_metadata, DTPSError, Notification, TypeOfConnection, DTPSR};

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use crate::{
        get_events_stream_inline, get_index, get_metadata, init_logging, DTPSError, FoundMetadata,
        TopicName, TypeOfConnection, TOPIC_LIST_CLOCK,
    };

    use super::*;

    #[tokio::test]
    async fn test_python1() -> DTPSR<()> {
        init_logging();
        let path = "/tmp/ex1";
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

    async fn check_server(con: &TypeOfConnection) -> DTPSR<()> {
        let md = get_metadata(&con).await?;
        log::info!("metadata: {:#?}", md);
        let index = get_index(&con).await?;

        log::info!("index: {:#?}", index);

        for (topic, data) in &index.topics {
            for r in &data.reachability {
                let md = get_metadata(&r.con).await?;

                log::info!("{topic:#?}: {md:#?}");

                check_complete_metadata(&md)?;

                // let (_handle, mut stream) = get_events_stream_inline(&r.con).await;
                // while let Some(event) = stream.next().await {
                //     log::info!("event: {:#?}", event);
                // }
                // log::info!("end of stream");
                //
            }
            //
            // log::info!("metadata: {:#?}", md);
            // let index = get_index(&con).await?;
            // log::info!("index: {:#?}", index);
            // let data = get_data(&con, &topic).await?;
            // log::info!("data: {:#?}", data);
        }

        let topic_clock = TopicName::from_dash_sep(TOPIC_LIST_CLOCK)?;

        let clock = index.topics.get(&topic_clock).unwrap();
        let r = clock.reachability.get(0).unwrap();
        let md = get_metadata(&r.con).await?;
        if md.events_data_inline_url == None {
            return Err(DTPSError::from("events_data_inline_url is None"));
        }
        let (handle, stream) = get_events_stream_inline(&md.events_data_inline_url.unwrap()).await;

        read_notifications(handle, stream, 3).await?;

        let (handle, stream) = get_events_stream_inline(&md.events_url.unwrap()).await;

        read_notifications(handle, stream, 3).await?;

        Ok(())
    }

    fn check_complete_metadata(md: &FoundMetadata) -> DTPSR<()> {
        if md.answering == None {
            return Err(DTPSError::from("answering is None"));
        }
        if md.events_url == None {
            return Err(DTPSError::from("events_url is None"));
        }
        if md.events_data_inline_url == None {
            return Err(DTPSError::from("events_data_inline_url is None"));
        }
        Ok(())
    }

    async fn read_notifications(
        handle: JoinHandle<DTPSR<()>>,
        mut stream: UnboundedReceiverStream<Notification>,
        nmin: usize,
    ) -> DTPSR<()> {
        let mut i = 0;
        loop {
            let ne = stream.next().await;
            match ne {
                None => {
                    log::debug!("finished stream");
                    break;
                }
                Some(notification) => {
                    log::debug!("clock notification: {:#?}", notification);

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
}
