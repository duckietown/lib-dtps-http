use tokio::process::Command;

use crate::{get_index, get_metadata, TypeOfConnection, DTPSR};

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use crate::{
        get_index, get_metadata, init_logging, DTPSError, FoundMetadata, TypeOfConnection,
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

        for (topic, data) in index.topics {
            for r in data.reachability {
                let md = get_metadata(&r.con).await?;

                log::info!("{topic:#?}: {md:#?}");

                check_complete_metadata(&md)?;
            }
            //
            // log::info!("metadata: {:#?}", md);
            // let index = get_index(&con).await?;
            // log::info!("index: {:#?}", index);
            // let data = get_data(&con, &topic).await?;
            // log::info!("data: {:#?}", data);
        }

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
}
