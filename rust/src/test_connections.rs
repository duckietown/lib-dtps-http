#[cfg(test)]
pub mod tests {
    use std::time::Duration;

    use maplit::hashmap;
    use rstest::rstest;

    use crate::{
        add_proxy, add_tpt_connection,
        client::remove_tpt_connection,
        debug_with_info, init_logging, post_json,
        test_fixtures::TestFixture,
        test_range::tests::{instance, node1, node2, switchboard},
        ConnectionJob, ServiceMode, TopicName, TopicProperties, CONTENT_TYPE_CBOR, DTPSR,
    };

    #[rstest]
    #[awt]
    #[tokio::test]
    async fn check_connection_local_to_local(#[future] instance: TestFixture) -> DTPSR<()> {
        init_logging();
        let topic_name1 = TopicName::from_dash_sep("topic1")?;
        let topic_name2 = TopicName::from_dash_sep("topic2")?;
        let connection_name = TopicName::from_dash_sep("connection1")?;

        {
            let mut ss = instance.ssa.lock().await;

            ss.new_topic(
                &topic_name1,
                None,
                CONTENT_TYPE_CBOR,
                &TopicProperties::rw(),
                None,
                None,
            )?;
            ss.new_topic(
                &topic_name2,
                None,
                CONTENT_TYPE_CBOR,
                &TopicProperties::rw(),
                None,
                None,
            )?;
        }

        let connection_job = ConnectionJob {
            source: topic_name1.clone(),
            target: topic_name2.clone(),
            service_mode: ServiceMode::AllMessages,
        };

        {
            let mut ss = instance.ssa.lock().await;
            ss.add_topic_to_topic_connection(&connection_name, &connection_job, instance.ssa.clone())
                .await?
        }
        .wait()
        .await?;

        {
            let mut ss = instance.ssa.lock().await;
            ss.remove_topic_to_topic_connection(&connection_name).await?
        }
        .wait()
        .await?;

        {
            let mut ss = instance.ssa.lock().await;
            ss.add_topic_to_topic_connection(&connection_name, &connection_job, instance.ssa.clone())
                .await?
        }
        .wait()
        .await?;

        let n = 5;
        for i in 0..n {
            let h = hashmap! {"value" => i};
            {
                let mut ss = instance.ssa.lock().await;
                ss.publish_object_as_cbor(&topic_name1, &h, None)?;
            }
        }

        // sleep 2 seconds
        tokio::time::sleep(Duration::from_millis(2000)).await;
        {
            let ss = instance.ssa.lock().await;
            let topic1 = ss.get_queue(&topic_name1)?;
            debug_with_info!("topic1 {:#?}", topic1.saved);
            assert_eq!(topic1.saved.len(), n);
            let topic2 = ss.get_queue(&topic_name2)?;
            debug_with_info!("topic2 {:#?}", topic2.saved);
            assert_eq!(topic2.saved.len(), n);
        }

        Ok(())
    }

    #[rstest]
    #[awt]
    #[tokio::test]
    async fn check_connection_local_to_local_http(#[future] instance: TestFixture) -> DTPSR<()> {
        init_logging();
        let topic_name1 = TopicName::from_dash_sep("topic1")?;
        let topic_name2 = TopicName::from_dash_sep("topic2")?;
        let connection_name = TopicName::from_dash_sep("connection1")?;

        {
            let mut ss = instance.ssa.lock().await;

            ss.new_topic(
                &topic_name1,
                None,
                CONTENT_TYPE_CBOR,
                &TopicProperties::rw(),
                None,
                None,
            )?;
            ss.new_topic(
                &topic_name2,
                None,
                CONTENT_TYPE_CBOR,
                &TopicProperties::rw(),
                None,
                None,
            )?;
        }

        let connection_job = ConnectionJob {
            source: topic_name1.clone(),
            target: topic_name2.clone(),
            service_mode: ServiceMode::AllMessages,
        };

        tokio::time::sleep(Duration::from_millis(1000)).await;
        add_tpt_connection(&instance.cf.con, &connection_name, &connection_job).await?;
        remove_tpt_connection(&instance.cf.con, &connection_name).await?;
        add_tpt_connection(&instance.cf.con, &connection_name, &connection_job).await?;

        let n = 5;
        for i in 0..n {
            let h = hashmap! {"value" => i};
            {
                let mut ss = instance.ssa.lock().await;
                ss.publish_object_as_cbor(&topic_name1, &h, None)?;
            }
        }

        // sleep 2 seconds
        tokio::time::sleep(Duration::from_millis(1000)).await;
        {
            let ss = instance.ssa.lock().await;
            let topic1 = ss.get_queue(&topic_name1)?;
            debug_with_info!("topic1 {:#?}", topic1.saved);
            assert_eq!(topic1.saved.len(), n);
            let topic2 = ss.get_queue(&topic_name2)?;
            debug_with_info!("topic2 {:#?}", topic2.saved);
            assert_eq!(topic2.saved.len(), n);
        }

        Ok(())
    }

    #[rstest]
    #[awt]
    #[tokio::test]
    async fn check_connection_remote_remote_rust(
        #[future] switchboard: TestFixture,
        #[future] node1: TestFixture,
        #[future] node2: TestFixture,
    ) -> DTPSR<()> {
        init_logging();

        let switchboard = switchboard;
        let node1 = node1;
        let node2 = node2;

        let node1_prefix = TopicName::from_dash_sep("node1")?;
        let node2_prefix = TopicName::from_dash_sep("node2")?;
        let topic1 = TopicName::from_dash_sep("topic1")?;
        let topic2 = TopicName::from_dash_sep("topic2")?;

        let node1_topic1 = node1_prefix.clone() + topic1.clone();
        let node2_topic2 = node2_prefix.clone() + topic2.clone();

        let urls = vec![node1.cf.con.clone()];
        let mask_origin = false;
        add_proxy(
            &switchboard.cf.con,
            &node1_prefix,
            Some(node1.server.get_node_id().await),
            &urls,
            mask_origin,
        )
        .await?;
        let urls = vec![node2.cf.con.clone()];
        add_proxy(
            &switchboard.cf.con,
            &node2_prefix,
            Some(node2.server.get_node_id().await),
            &urls,
            mask_origin,
        )
        .await?;

        let connection_name = TopicName::from_dash_sep("connection1")?;
        let cn = ConnectionJob {
            source: node1_topic1.clone(),
            target: node2_topic2.clone(),
            service_mode: ServiceMode::BestEffort,
        };
        add_tpt_connection(&switchboard.cf.con, &connection_name, &cn).await?;

        let topic1_url = node1.cf.con.join(topic1.as_relative_url())?;
        let n: usize = 5;
        for i in 0..n {
            post_json(&topic1_url, &i).await?;
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        }

        {
            let ss = node1.ssa.lock().await;
            let topic1 = ss.get_queue(&topic1)?;
            debug_with_info!("topic1 {:#?}", topic1.saved);
            assert_eq!(topic1.saved.len(), n);
        }
        {
            let ss = node2.ssa.lock().await;
            let topic2 = ss.get_queue(&topic2)?;
            debug_with_info!("topic2 {:#?}", topic2.saved);
            assert_eq!(topic2.saved.len(), n);
        }

        switchboard.finish().await?;
        node1.finish().await?;
        node2.finish().await?;
        Ok(())
    }
}
