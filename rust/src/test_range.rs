#[cfg(test)]
mod tests {

    use rstest::*;

    struct TestFixture {
        pub ssa: ServerStateAccess,
        pub handles: Vec<JoinHandle<()>>,
        pub con: TypeOfConnection,
    }

    impl TestFixture {
        pub fn finish(self) -> DTPSR<()> {
            for handle in self.handles {
                handle.abort();
            }

            Ok(())
        }
    }
    #[fixture]
    async fn instance() -> TestFixture {
        let unix_path = "/tmp/test1";
        let mut server = DTPSServer::new(
            None,
            None,
            "cloudflare".to_string(),
            Some(unix_path.to_string()),
            hashmap! {},
            vec![],
        )
        .await
        .unwrap();

        let handles = server.start_serving().await.unwrap();

        let ssa = server.get_lock();

        let con = TypeOfConnection::UNIX(UnixCon {
            scheme: "http+unix".to_string(),
            socket_name: unix_path.to_string(),
            path: "/".to_string(),
            query: None,
        });

        tokio::time::sleep(Duration::from_millis(1000)).await;

        TestFixture { ssa, handles, con }
    }

    // Bring the function into scope

    use log::debug;
    use maplit::hashmap;
    use std::time::Duration;
    use tokio::task::JoinHandle;

    use crate::{
        get_metadata, DTPSServer, FilePaths, ServerStateAccess, TopicName, TopicProperties,
        TypeOfConnection, UnixCon, DTPSR,
    };

    use super::*;

    #[rstest]
    #[awt]
    #[tokio::test]
    async fn another(#[future] instance: TestFixture) -> DTPSR<()> {
        // wait for 2 seconds

        let md = get_metadata(&instance.con).await;
        eprintln!("found {md:?}");

        {
            let mut ss = instance.ssa.lock().await;
            let topic_name = TopicName::from_dash_sep("a/b")?;
            ss.new_topic(
                &topic_name,
                None,
                "application/cbor",
                &TopicProperties::rw(),
                None,
            )?;
            for i in 0..10 {
                ss.publish_object_as_cbor(&topic_name, &i, None)?;
            }
        }

        instance.finish()
    }

    #[tokio::test]
    async fn test1() {}
}
