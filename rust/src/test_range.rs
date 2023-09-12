#![allow(unused_mut)]

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use json_patch::*;
    use maplit::hashmap;
    use rstest::*;
    use schemars::{
        schema_for,
        JsonSchema,
    };
    use serde::{
        Deserialize,
        Serialize,
    };
    use serde_json::json;
    use tokio::{
        process::Command,
        task::JoinHandle,
    };

    use crate::{
        add_proxy,
        create_topic,
        debug_with_info,
        delete_topic,
        error_with_info,
        get_events_stream_inline,
        get_metadata,
        get_rawdata,
        init_logging,
        patch_data,
        post_data,
        remove_proxy,
        test_python::check_server,
        ContentInfo,
        DTPSError,
        DTPSServer,
        RawData,
        ServerStateAccess,
        TopicName,
        TopicProperties,
        TopicRefAdd,
        TypeOfConnection,
        CONTENT_TYPE_CBOR,
        CONTENT_TYPE_JSON,
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
    async fn check_server_answers(#[future] instance: TestFixture) -> DTPSR<()> {
        let x = check_server(&instance.con).await;
        if let Err(ei) = &x {
            error_with_info!("check_server failed:\n{}", ei);
        }
        x
    }

    #[rstest]
    #[awt]
    #[tokio::test]
    async fn another(#[future] instance: TestFixture) -> DTPSR<()> {
        let _md = get_metadata(&instance.con).await;

        {
            let mut ss = instance.ssa.lock().await;
            let topic_name = TopicName::from_dash_sep("a/b")?;
            ss.new_topic(&topic_name, None, CONTENT_TYPE_CBOR, &TopicProperties::rw(), None, None)?;
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
        let _md = get_metadata(&instance.con).await;
        // eprintln!("found {md:?}");

        {
            let mut ss = instance.ssa.lock().await;
            let topic_name = TopicName::from_dash_sep("a/b")?;
            ss.new_topic(&topic_name, None, CONTENT_TYPE_CBOR, &TopicProperties::rw(), None, None)?;
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

        assert_eq!(data_original.content_type, CONTENT_TYPE_CBOR);
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
            ss.new_topic(&topic_name, None, CONTENT_TYPE_CBOR, &TopicProperties::rw(), None, None)?;
        }
        let con_original = instance.con.join("a/b/")?;

        let md = get_metadata(&con_original).await?;

        let url = md.events_data_inline_url.unwrap();
        let (handle, mut receiver) = get_events_stream_inline(&url).await;

        let object = 42;
        let data = serde_cbor::to_vec(&object)?;
        let rd = RawData::cbor(data);
        let ds = post_data(&con_original, &rd).await?;
        debug_with_info!("post resulted in {ds:?}");
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
            ss.new_topic(&topic_name, None, CONTENT_TYPE_CBOR, &TopicProperties::rw(), None, None)?;
        }
        let con_original = instance.con.join(topic_name.as_relative_url())?;

        let object = hashmap! {
            "one" => vec![
                hashmap!{"b"=>42},
                hashmap!{},
            ]
        };

        let data = serde_cbor::to_vec(&object)?;
        let rd = RawData::cbor(data);
        let ds = post_data(&con_original, &rd).await?;
        debug_with_info!("post resulted in {ds:?}");

        let get_inside = con_original.join("one/0/b/")?;
        let rd2 = get_rawdata(&get_inside).await?;
        let converted: serde_cbor::Value = serde_cbor::from_slice(&rd2.content)?;

        let expected = serde_cbor::Value::Integer(42);
        debug_with_info!("Converted {converted:?}");

        assert_eq!(expected, converted);
        instance.finish()?;
        Ok(())
    }

    #[allow(unused_mut)]
    #[rstest]
    #[awt]
    #[tokio::test]
    async fn check_proxy(#[future] instance: TestFixture, #[future] mut instance2: TestFixture) -> DTPSR<()> {
        init_logging();
        let instance = instance;
        let mut instance2 = instance2;

        let topic_name = TopicName::from_dash_sep("a/b")?;
        let n = 10;

        {
            let mut ss = instance.ssa.lock().await;
            ss.new_topic(&topic_name, None, CONTENT_TYPE_CBOR, &TopicProperties::rw(), None, None)?;
            for i in 0..n {
                let h = hashmap! {"value" => i};
                ss.publish_object_as_cbor(&topic_name, &h, None)?;
            }

            let alias = TopicName::from_dash_sep("c/d")?;
            ss.add_alias(&alias, &topic_name);
        }
        let con_original = instance.con.join(topic_name.as_relative_url())?;

        let mounted_at = TopicName::from_dash_sep("mounted/here")?;

        instance2.server.add_proxied(&mounted_at, con_original.clone()).await?;

        let con_proxied = instance2.con.join(mounted_at.as_relative_url())?;

        debug_with_info!("ask metadata for con_original {con_original:#?}");
        let md_original = get_metadata(&con_original).await;
        debug_with_info!("ask metadata for con_proxied {con_proxied:#?}");
        let md_proxied = get_metadata(&con_proxied).await;

        debug_with_info!("md_original {md_original:#?}");
        debug_with_info!("md_proxied {md_proxied:#?}");

        debug_with_info!("get data for con_original {con_original:#?}");
        let data_original = get_rawdata(&con_original).await?;
        debug_with_info!("get data for con_proxied {con_proxied:#?}");
        let data_proxied = get_rawdata(&con_proxied).await?;

        debug_with_info!("data_original {con_original:#} {data_original:#?}");
        debug_with_info!("data_proxied {con_proxied:#} {data_proxied:#?}");

        assert_eq!(data_original.content_type, CONTENT_TYPE_CBOR);
        assert_eq!(data_original, data_proxied);

        // now let's get the value inside
        let con_original_inside = con_original.join("value/")?;
        let con_proxied_inside = con_proxied.join("value/")?;
        debug_with_info!("get data for con_original_inside {con_original_inside:#?}");
        let inside_original = get_rawdata(&con_original_inside).await?;
        debug_with_info!("get data for con_proxied_inside {con_proxied_inside:#?}");
        let inside_proxied = get_rawdata(&con_proxied_inside).await?;
        assert_eq!(inside_original, inside_proxied);
        let i: i64 = serde_cbor::from_slice(&inside_original.content)?;
        assert_eq!(i, n - 1);
        instance.finish()?;
        instance2.finish()?;
        Ok(())
    }

    #[rstest]
    #[awt]
    #[tokio::test]
    async fn check_proxy_websocket_01(#[future] instance: TestFixture, #[future] instance2: TestFixture) -> DTPSR<()> {
        check_proxy_websocket(instance, instance2, "").await
    }

    #[rstest]
    #[awt]
    #[tokio::test]
    async fn check_proxy_websocket_02(#[future] instance: TestFixture, #[future] instance2: TestFixture) -> DTPSR<()> {
        check_proxy_websocket(instance, instance2, ":deref/").await
    }
    #[rstest]
    #[awt]
    #[tokio::test]
    async fn check_proxy_websocket_03(#[future] instance: TestFixture, #[future] instance2: TestFixture) -> DTPSR<()> {
        check_proxy_websocket(instance, instance2, "mounted/").await
    }
    #[rstest]
    #[awt]
    #[tokio::test]
    async fn check_proxy_websocket_04_mounted_dtps_clock(
        #[future] instance: TestFixture,
        #[future] instance2: TestFixture,
    ) -> DTPSR<()> {
        check_proxy_websocket(instance, instance2, "mounted/dtps/clock/").await
    }
    #[rstest]
    #[awt]
    #[tokio::test]
    async fn check_proxy_websocket_05_dtps_clock(
        #[future] instance: TestFixture,
        #[future] instance2: TestFixture,
    ) -> DTPSR<()> {
        check_proxy_websocket(instance, instance2, "dtps/clock/").await
    }
    #[rstest]
    #[awt]
    #[tokio::test]
    async fn check_proxy_websocket_06_dtps(
        #[future] instance: TestFixture,
        #[future] instance2: TestFixture,
    ) -> DTPSR<()> {
        check_proxy_websocket(instance, instance2, "dtps/").await
    }
    #[rstest]
    #[awt]
    #[tokio::test]
    async fn check_proxy_websocket_04_mounted_dtps(
        #[future] instance: TestFixture,
        #[future] instance2: TestFixture,
    ) -> DTPSR<()> {
        check_proxy_websocket(instance, instance2, "mounted/dtps/").await
    }
    async fn check_proxy_websocket(instance: TestFixture, mut instance2: TestFixture, path: &str) -> DTPSR<()> {
        init_logging();

        let mounted_at = TopicName::from_dash_sep("mounted")?;
        instance2.server.add_proxied(&mounted_at, instance.con.clone()).await?;
        let url = instance2.con.to_string();
        let url = format!("{url}{path}");
        let cmd = vec![
            "dtps-http-py-listen",
            "--max-time",
            "5",
            "--max-messages",
            "5",
            "--raise-on-error",
            "--url",
            url.as_str(),
        ];

        // create process given by command above
        let mut child = Command::new(cmd[0]).args(&cmd[1..]).spawn().unwrap();

        let status = child.wait().await?;
        if !status.success() {
            return DTPSError::other(format!("child process failed with error {status}"));
        }

        instance.finish()?;
        instance2.finish()?;
        Ok(())
    }

    #[rstest]
    #[awt]
    #[tokio::test]
    async fn check_patch(#[future] instance: TestFixture) -> DTPSR<()> {
        init_logging();

        let topic_name = TopicName::from_dash_sep("a/b")?;

        {
            let mut ss = instance.ssa.lock().await;
            ss.new_topic(&topic_name, None, CONTENT_TYPE_CBOR, &TopicProperties::rw(), None, None)?;
            let h = hashmap! {"value" => "initial"};
            ss.publish_object_as_cbor(&topic_name, &h, None)?;
        }

        let con_original = instance.con.join(topic_name.as_relative_url())?;

        let replace = ReplaceOperation {
            path: "/value".to_string(),
            value: serde_json::Value::String("new".to_string()),
        };
        let operation = PatchOperation::Replace(replace);
        let patch = json_patch::Patch(vec![operation]);
        patch_data(&con_original, &patch).await?;

        // now test something that should fail
        let replace = ReplaceOperation {
            path: "/NOTEXISTING".to_string(),
            value: serde_json::Value::String("new".to_string()),
        };
        let operation = PatchOperation::Replace(replace);
        let patch = json_patch::Patch(vec![operation]);
        patch_data(&con_original, &patch).await.expect_err("should fail");

        // now let's see if we can replace entirely

        let replace = ReplaceOperation {
            path: "".to_string(),
            value: json!({"A": {"B": ["C", "D"]}}),
        };
        let operation = PatchOperation::Replace(replace);
        let patch = json_patch::Patch(vec![operation]);
        patch_data(&con_original, &patch).await?;

        // now check the addressing
        let b_address = con_original.join("A/B/")?;
        let add_operation1 = AddOperation {
            path: "/0".to_string(),
            value: json!("start"),
        };
        let add_operation2 = AddOperation {
            path: "/-".to_string(),
            value: json!("end"),
        };
        let operation1 = PatchOperation::Add(add_operation1);
        let operation2 = PatchOperation::Add(add_operation2);
        let patch = json_patch::Patch(vec![operation1, operation2]);
        patch_data(&b_address, &patch).await?;

        instance.finish()?;
        Ok(())
    }

    #[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq)]
    struct ExampleData {
        pub a: i64,
        pub b: Vec<String>,
    }

    #[rstest]
    #[awt]
    #[tokio::test]
    async fn check_create_topic(#[future] instance: TestFixture) -> DTPSR<()> {
        init_logging();

        let topic_name = TopicName::from_dash_sep("a/b")?;

        let properties = TopicProperties {
            streamable: true,
            pushable: true,
            readable: true,
            immutable: false,
            has_history: true,
            patchable: true,
        };

        let content_info = ContentInfo::simple(CONTENT_TYPE_JSON, Some(schema_for!(ExampleData)));
        let tr = TopicRefAdd {
            app_data: hashmap! {},
            properties,
            content_info,
        };

        //first time ok
        create_topic(&instance.con, &topic_name, &tr).await?;

        let address = instance.con.join(topic_name.as_relative_url())?;
        let data = ExampleData {
            a: 42,
            b: vec!["a".to_string(), "b".to_string()],
        };

        post_data(&address, &RawData::represent_as_json(&data)?).await?;

        // second time should fail
        create_topic(&instance.con, &topic_name, &tr)
            .await
            .expect_err("should fail");
        // first time ok
        delete_topic(&instance.con, &topic_name).await?;
        // second time should fail
        delete_topic(&instance.con, &topic_name).await.expect_err("should fail");

        let topic_name = TopicName::root();
        create_topic(&instance.con, &topic_name, &tr)
            .await
            .expect_err("should fail");
        delete_topic(&instance.con, &topic_name).await.expect_err("should fail");

        instance.finish()?;
        Ok(())
    }

    #[rstest]
    #[awt]
    #[tokio::test]
    async fn check_proxy_manual(#[future] instance: TestFixture, #[future] mut instance2: TestFixture) -> DTPSR<()> {
        init_logging();
        let instance = instance;
        let mut instance2 = instance2;

        let topic_name = TopicName::from_dash_sep("a/b")?;
        let n = 10;

        {
            let mut ss = instance.ssa.lock().await;
            ss.new_topic(&topic_name, None, CONTENT_TYPE_CBOR, &TopicProperties::rw(), None, None)?;
            for i in 0..n {
                let h = hashmap! {"value" => i};
                ss.publish_object_as_cbor(&topic_name, &h, None)?;
            }
        }

        let mounted_at = TopicName::from_dash_sep("mounted/here")?;

        let node_id = instance.server.get_node_id().await;
        let urls = vec![instance.con.clone()];
        add_proxy(&instance2.con, &mounted_at, &node_id, &urls).await?;
        add_proxy(&instance2.con, &mounted_at, &node_id, &urls).await?;
        // sleep 5 seconds
        tokio::time::sleep(Duration::from_millis(2000)).await;

        let con_proxied = instance2.con.join(mounted_at.as_relative_url())?;
        let _rd = get_rawdata(&con_proxied).await?;

        remove_proxy(&instance2.con, &mounted_at).await?;

        instance.finish()?;
        instance2.finish()?;
        Ok(())
    }
}
