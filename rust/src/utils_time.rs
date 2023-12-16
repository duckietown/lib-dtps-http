use std::time::SystemTime;

pub fn epoch() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

pub fn format_nanos(n: i64) -> String {
    let ms = (n as f64) / 1_000_000.0;
    format!("{:.3}ms", ms)
}

pub fn time_nanos() -> u128 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

pub fn time_nanos_i64() -> i64 {
    time_nanos() as i64
}
