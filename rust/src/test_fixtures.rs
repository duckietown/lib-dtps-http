use crate::{
    connections::TypeOfConnection, get_metadata, init_logging, DTPSError, DTPSServer, ServerStateAccess, TopicName,
    TopicProperties, CONTENT_TYPE_JSON, DTPSR,
};
use chrono::Duration;
use futures::{SinkExt, StreamExt};
use log::info;
use maplit::hashmap;
use tempfile::tempdir;
use tokio::task::JoinHandle;

use crate::client::check_unix_socket;
use tokio::{
    process::{Child, Command},
    sync::broadcast::{error::RecvError, Receiver as BroadcastReceiver},
};

pub struct ConnectionFixture {
    pub handles: Vec<JoinHandle<()>>,
    pub con: TypeOfConnection,
    pub children: Vec<Child>,
    pub tmp_dir: Option<tempfile::TempDir>,
}

impl ConnectionFixture {
    pub async fn finish(self) -> DTPSR<()> {
        for handle in self.handles {
            handle.abort();
        }

        for mut child in self.children {
            child.kill().await?;
        }

        Ok(())
    }
}

pub struct TestFixture {
    pub server: DTPSServer,
    pub ssa: ServerStateAccess,
    pub cf: ConnectionFixture,
    // pub handles: Vec<JoinHandle<()>>,
    // pub con: TypeOfConnection,
}

impl TestFixture {
    pub async fn finish(self) -> DTPSR<()> {
        self.cf.finish().await
    }
}

pub async fn instance_python_test_fixture() -> DTPSR<ConnectionFixture> {
    init_logging();
    // generate a temp dir
    let dir = tempdir()?;
    let path0 = dir.path().join("socket");

    let path = path0.to_str().unwrap();
    let cmd = vec!["dtps-http-py-server-example-clock", "--unix-path", path];

    // create process given by command above
    let child = Command::new(cmd[0])
        .args(&cmd[1..])
        .stdout(std::process::Stdio::inherit()) // Inherit the parent's stdout
        .stderr(std::process::Stdio::inherit()) // Inherit the parent's stderr
        .spawn()?;

    // wait that the socket exists but not more than 5 seconds
    let t0 = tokio::time::Instant::now();
    let max_wait_ms = 5000;
    let interval_wait_ms = 100;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(interval_wait_ms)).await;

        let elapsed = t0.elapsed().as_millis();
        if tokio::fs::metadata(path).await.is_ok() {
            info!("found socket {path} after {elapsed} ms");
            check_unix_socket(path).await?;
            break;
        } else {
            if elapsed > max_wait_ms {
                let s = format!("socket not found after {elapsed} ms: {path}");
                return Err(DTPSError::from("socket not found"));
            }
        }
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    let con = TypeOfConnection::unix_socket(path);

    get_metadata(&con).await?;
    let res = ConnectionFixture {
        handles: vec![],
        children: vec![child],
        con,
        tmp_dir: Some(dir),
    };
    Ok(res)
}

pub async fn instance_rust() -> TestFixture {
    init_logging();
    let mut server = DTPSServer::new(None, None, "cloudflare".to_string(), None, hashmap! {}, vec![], vec![])
        .await
        .unwrap();

    let handles = server.start_serving().await.unwrap();

    let ssa = server.get_lock();

    let con = {
        let ss = ssa.lock().await;
        let advertised_urls = ss.get_advertise_urls();
        let url = advertised_urls.get(0).unwrap();
        crate::parse_url_ext(url).unwrap()
    };

    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    TestFixture {
        server,
        ssa,
        cf: ConnectionFixture {
            handles,
            con,
            children: vec![],
            tmp_dir: None,
        },
    }
}

#[cfg(test)]
pub mod tests {
    use crate::{
        connections::TypeOfConnection, init_logging, DTPSServer, ServerStateAccess, TopicName, TopicProperties,
        CONTENT_TYPE_JSON, DTPSR,
    };
    use chrono::Duration;
    use maplit::hashmap;
    use rstest::{fixture, rstest};
    use tokio::task::JoinHandle;
}
