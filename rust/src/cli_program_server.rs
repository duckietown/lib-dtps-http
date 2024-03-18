use maplit::hashmap;
use rand::Rng;
use schemars::schema_for;
use tokio::{
    spawn,
    time::{interval, Duration},
};

use crate::client_publish::{publish, publish_cbor};
use crate::client_topics::create_topic;
use crate::connections::TypeOfConnection;
use crate::structures_topicref::{Bounds, ContentInfo, TopicRefAdd};
use crate::test_fixtures::instance_rust;
use crate::time_nanos_i64;
use crate::{
    create_server_from_command_line, debug_with_info, error_with_info, get_rawdata_status, get_resolved, init_logging,
    show_errors, DTPSServer, ServerStateAccess, TopicName, TopicProperties, CONTENT_TYPE_JSON, DTPSR,
};

async fn clock_go(state: ServerStateAccess, topic_name: &str, interval_s: f32) -> DTPSR<()> {
    let mut clock = interval(Duration::from_secs_f32(interval_s));

    clock.tick().await;
    clock.tick().await;
    {
        let mut ss = state.lock().await;
        let props = TopicProperties::ro();
        // let data = HashMap::new();
        ss.new_topic(
            &TopicName::from_relative_url(topic_name)?,
            None,
            "application/json",
            &props,
            Some(schema_for!(i64)),
            Bounds::max_length(10),
        )?;
    }
    loop {
        clock.tick().await;
        let mut ss = state.lock().await;
        // let datetime_string = Local::now().to_rfc3339();
        // get the current time in nanoseconds
        let now = time_nanos_i64();
        let s = format!("{}", now);
        let _inserted = ss.publish_json(&TopicName::from_relative_url(topic_name)?, &s, None)?;

        // debug_with_info!("inserted {}: {:?}", topic_name, inserted);
    }
    // Ok(())
}

async fn clock() -> DTPSR<()> {
    init_logging();
    let mut server = create_server_from_command_line().await?;

    //
    // // spawn(clock_go(server.get_lock(), "clock", 1.0));
    // spawn(show_errors(
    //     Some(server.get_lock()),
    //     "clock".to_string(),
    //     clock_go(server.get_lock(), "dtps/clock", 5.0),
    // ));
    // spawn(show_errors(
    //     Some(server.get_lock()),
    //     "clock15".to_string(),
    //     clock_go(server.get_lock(), "clock15", 15.0),
    // ));
    // spawn(show_errors(
    //     Some(server.get_lock()),
    //     "clock30".to_string(),
    //     clock_go(server.get_lock(), "clock30", 30.0),
    // ));

    server.serve().await
}

pub async fn cli_server() -> Result<(), Box<dyn std::error::Error>> {
    //
    match clock().await {
        Ok(_) => Ok(()),
        Err(e) => {
            error_with_info!("Error in serving:\n{:?}", e);

            // error!("Source: {}", e.source().unwrap());

            // exit with error code
            std::process::exit(1);
        }
    }
}

pub async fn cli_stress_test() -> Result<(), Box<dyn std::error::Error>> {
    //
    match stress_test().await {
        Ok(_) => Ok(()),
        Err(e) => {
            error_with_info!("Error in serving:\n{:?}", e);

            // error!("Source: {}", e.source().unwrap());

            // exit with error code
            std::process::exit(1);
        }
    }
}

async fn stress_test() -> DTPSR<()> {
    init_logging();
    let s = instance_rust().await;

    let topic = TopicName::from_relative_url("stress_test")?;

    let properties = TopicProperties::rw();

    let content_info = ContentInfo::simple(CONTENT_TYPE_JSON, None);
    let tr = TopicRefAdd {
        app_data: hashmap! {},
        properties,
        content_info,
        bounds: Bounds::max_length(2),
    };

    let con_topic = create_topic(&s.cf.con, &topic, &tr).await?;

    // generate a random 1000 bytes string
    let mut rng = rand::thread_rng();
    let data: String = (0..1000 * 1000)
        .map(|_| {
            let c: char = rng.gen();
            c
        })
        .collect();

    let mut i = 0;
    let interval = Duration::from_millis(10);
    loop {
        tokio::time::sleep(interval).await;

        i += 1;
        let s = format!("{}: {}", i, data);

        publish_cbor(&con_topic, &s).await?;

        if i % 100 == 0 {
            let status = get_rawdata_status(&con_topic).await?;
            debug_with_info!("status: {:?}", status.0);
        }
    }

    Ok(())
}
