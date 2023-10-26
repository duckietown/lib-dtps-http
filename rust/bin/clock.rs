extern crate dtps_http;

use chrono::prelude::*;
use schemars::schema_for;
use tokio::{
    spawn,
    time::{
        interval,
        Duration,
    },
};

use dtps_http::{
    create_server_from_command_line,
    error_with_info,
    init_logging,
    show_errors,
    utils::time_nanos_i64,
    ServerStateAccess,
    TopicName,
    TopicProperties,
    DTPSR,
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
            None,
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
    spawn(show_errors(
        Some(server.get_lock()),
        "clock5".to_string(),
        clock_go(server.get_lock(), "clock5", 5.0),
    ));
    spawn(show_errors(
        Some(server.get_lock()),
        "clock15".to_string(),
        clock_go(server.get_lock(), "clock15", 15.0),
    ));
    spawn(show_errors(
        Some(server.get_lock()),
        "clock30".to_string(),
        clock_go(server.get_lock(), "clock30", 30.0),
    ));

    server.serve().await
}

#[tokio::main]
async fn main() -> () {
    match clock().await {
        Ok(_) => return,
        Err(e) => {
            error_with_info!("Error in serving:\n{:?}", e);

            // error!("Source: {}", e.source().unwrap());

            // exit with error code
            std::process::exit(1);
        }
    }
}
