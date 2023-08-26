#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use log::debug;
    use maplit::hashmap;
    use rstest::*;
    use tokio::task::JoinHandle;

    use crate::{
        get_events_stream_inline, get_rawdata, init_logging, parse_url_ext, post_data, RawData,
    };
    use crate::{
        get_metadata, DTPSServer, ServerStateAccess, TopicName, TopicProperties, TypeOfConnection,
        DTPSR,
    };

    struct TestFixture {
        pub server: DTPSServer,
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
    async fn instance2() -> TestFixture {
        instance().await
    }

    #[fixture]
    async fn instance() -> TestFixture {
        init_logging();
        let mut server = DTPSServer::new(
            None,
            None,
            "cloudflare".to_string(),
            None,
            hashmap! {},
            vec![],
            vec![],
        )
        .await
        .unwrap();

        let handles = server.start_serving().await.unwrap();

        let ssa = server.get_lock();

        let con = {
            let ss = ssa.lock().await;
            let advertised_urls = ss.get_advertise_urls();
            let url = advertised_urls.get(0).unwrap();
            parse_url_ext(url).unwrap()
        };

        tokio::time::sleep(Duration::from_millis(1000)).await;

        TestFixture {
            server,
            ssa,
            handles,
            con,
        }
    }

    #[rstest]
    #[awt]
    #[tokio::test]
    async fn another(#[future] instance: TestFixture) -> DTPSR<()> {
        let _md = get_metadata(&instance.con).await;

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

    #[rstest]
    #[awt]
    #[tokio::test]
    async fn check_aliases(#[future] instance: TestFixture) -> DTPSR<()> {
        // wait for 2 seconds

        let md = get_metadata(&instance.con).await;
        // eprintln!("found {md:?}");

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
                let h = hashmap! {"value" => i};
                ss.publish_object_as_cbor(&topic_name, &h, None)?;
            }

            let alias = TopicName::from_dash_sep("c/d")?;
            ss.add_alias(&alias, &topic_name);
        }

        let con_original = instance.con.join("a/b/")?;
        let con_alias = instance.con.join("c/d/")?;

        let md_original = get_metadata(&con_original).await;
        let md_alias = get_metadata(&con_alias).await;

        eprintln!("md_original {md_original:#?}");
        eprintln!("md_alias {md_alias:#?}");

        let data_original = get_rawdata(&con_original).await?;
        let data_alias = get_rawdata(&con_alias).await?;

        eprintln!("data_original {data_original:#?}");
        eprintln!("data_alias {data_alias:#?}");

        assert_eq!(data_original.content_type, "application/cbor");
        assert_eq!(data_original, data_alias);
        tokio::time::sleep(Duration::from_millis(1000)).await;

        instance.finish()?;
        Ok(())
    }

    #[rstest]
    #[awt]
    #[tokio::test]
    async fn stream_events(#[future] instance: TestFixture) -> DTPSR<()> {
        // wait for 2 seconds
        init_logging();

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
        }
        let con_original = instance.con.join("a/b/")?;

        let md = get_metadata(&con_original).await?;

        let url = md.events_data_inline_url.unwrap();
        let (handle, mut receiver) = get_events_stream_inline(&url).await;

        let object = 42;
        let data = serde_cbor::to_vec(&object)?;
        let rd = RawData::new(data, "application/cbor");
        let ds = post_data(&con_original, &rd).await?;
        debug!("post resulted in {ds:?}");
        let notification = receiver.next().await.unwrap();
        assert_eq!(rd, notification.rd);

        instance.finish()?;
        handle.abort();
        Ok(())
    }

    #[rstest]
    #[awt]
    #[tokio::test]
    async fn get_inside_struct(#[future] instance: TestFixture) -> DTPSR<()> {
        // wait for 2 seconds
        init_logging();
        let topic_name = TopicName::from_dash_sep("a/topic")?;

        {
            let mut ss = instance.ssa.lock().await;
            ss.new_topic(
                &topic_name,
                None,
                "application/cbor",
                &TopicProperties::rw(),
                None,
            )?;
        }
        let con_original = instance.con.join(topic_name.as_relative_url())?;

        let object = hashmap! {
            "one" => vec![
                hashmap!{"b"=>42},
                hashmap!{},
            ]
        };

        let data = serde_cbor::to_vec(&object)?;
        let rd = RawData::new(data, "application/cbor");
        let ds = post_data(&con_original, &rd).await?;
        debug!("post resulted in {ds:?}");

        let get_inside = con_original.join("one/0/b/")?;
        let rd2 = get_rawdata(&get_inside).await?;
        let converted: serde_cbor::Value = serde_cbor::from_slice(&rd2.content)?;

        let expected = serde_cbor::Value::Integer(42);
        debug!("Converted {converted:?}");

        assert_eq!(expected, converted);
        instance.finish()?;
        Ok(())
    }

    #[rstest]
    #[awt]
    #[tokio::test]
    async fn check_proxy(
        #[future] instance: TestFixture,
        #[future] mut instance2: TestFixture,
    ) -> DTPSR<()> {
        init_logging();
        let instance = instance;
        let mut instance2 = instance2;

        let topic_name = TopicName::from_dash_sep("a/b")?;

        {
            let mut ss = instance.ssa.lock().await;
            ss.new_topic(
                &topic_name,
                None,
                "application/cbor",
                &TopicProperties::rw(),
                None,
            )?;
            for i in 0..10 {
                let h = hashmap! {"value" => i};
                ss.publish_object_as_cbor(&topic_name, &h, None)?;
            }

            let alias = TopicName::from_dash_sep("c/d")?;
            ss.add_alias(&alias, &topic_name);
        }
        let con_original = instance.con.join(topic_name.as_relative_url())?;

        let mounted_at = TopicName::from_dash_sep("mounted/here")?;
        // Now proxy the thing
        {
            // let mut ss2 = instance2.ssa.lock().await;
            let subname = format!("sub1");

            instance2
                .server
                .add_proxied(&subname, &mounted_at, con_original.clone())
                .await?;
        }

        let con_proxied = instance2.con.join(mounted_at.as_relative_url())?;

        debug!("ask metadata for con_original {con_original:#?}");
        let md_original = get_metadata(&con_original).await;
        debug!("ask metadata for con_proxied {con_proxied:#?}");
        let md_proxied = get_metadata(&con_proxied).await;

        debug!("md_original {md_original:#?}");
        debug!("md_proxied {md_proxied:#?}");

        debug!("get data for con_original {con_original:#?}");
        let data_original = get_rawdata(&con_original).await?;
        debug!("get data for con_proxied {con_proxied:#?}");
        let data_proxied = get_rawdata(&con_proxied).await?;

        debug!("data_original {con_original:#} {data_original:#?}");
        debug!("data_proxied {con_proxied:#} {data_proxied:#?}");

        assert_eq!(data_original.content_type, "application/cbor");
        assert_eq!(data_original, data_proxied);
        instance.finish()?;
        instance2.finish()?;
        Ok(())
    }

    #[tokio::test]
    async fn test1() {}
}
