extern crate dtps_http;

use std::sync::Arc;

use chrono::prelude::*;
use tokio::spawn;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};

use dtps_http::logs::init_logging;
use dtps_http::server::*;
use dtps_http::server_state::*;

async fn clock_go(state: Arc<Mutex<ServerState>>, topic_name: &str, interval_s: f32) {
    let mut clock = interval(Duration::from_secs_f32(interval_s));
    clock.tick().await;
    loop {
        clock.tick().await;
        let mut ss = state.lock().await;
        // let datetime_string = Local::now().to_rfc3339();
        // get the current time in nanoseconds
        let now = Local::now().timestamp_nanos();
        let s = format!("{}", now);
        let _inserted = ss.publish_json(topic_name, &s);

        // debug!("inserted {}: {:?}", topic_name, inserted);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();
    let mut server = create_server_from_command_line();

    spawn(clock_go(server.get_lock(), "clock", 1.0));
    spawn(clock_go(server.get_lock(), "clock5", 5.0));
    spawn(clock_go(server.get_lock(), "clock7", 7.0));
    spawn(clock_go(server.get_lock(), "clock11", 11.0));

    server.serve().await
}
