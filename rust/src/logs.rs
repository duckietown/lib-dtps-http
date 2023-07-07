use std::env;

fn get_exec_name() -> Option<String> {
    std::env::current_exe()
        .ok()
        .and_then(|pb| pb.file_name().map(|s| s.to_os_string()))
        .and_then(|s| s.into_string().ok())
}

pub static DEFAULT_LOG_LEVEL: &'static str = "warn,dtps_http=info";
pub fn init_logging () {

    let s = get_exec_name().unwrap();
    let version = env!("CARGO_PKG_VERSION");
    let pkg_name = env!("CARGO_PKG_NAME");
    eprintln!("{s} ({pkg_name} {version}) ");
    if env::var("RUST_LOG").is_err() {
        eprintln!("RUST_LOG not set: setting it to {DEFAULT_LOG_LEVEL}");
        env::set_var("RUST_LOG", DEFAULT_LOG_LEVEL)
    }else {
        eprintln!("RUST_LOG is set to {:?}", env::var("RUST_LOG"));
    }
    env_logger::init();

}
