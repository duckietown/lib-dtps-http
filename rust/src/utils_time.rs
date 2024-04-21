use crate::types::Time;
use std::time::SystemTime;
use tokio::sync::{broadcast as tokio_broadcast, mpsc as tokio_mpsc};

pub fn epoch() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

pub fn format_nanos(n: Time) -> String {
    let ms = (n as f64) / 1_000_000.0;
    format!("{:.3}ms", ms)
}

pub fn format_delay(start: Time, stop: Time) -> String {
    let delta = stop - start;
    format_nanos(delta)
}
pub fn time_nanos() -> u128 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

pub fn time_nanos_i64() -> Time {
    time_nanos() as i64
}
