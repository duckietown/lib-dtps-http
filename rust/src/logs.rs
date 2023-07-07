use std::env;

use crate::built_info;

fn get_exec_name() -> Option<String> {
    env::current_exe()
        .ok()
        .and_then(|pb| pb.file_name().map(|s| s.to_os_string()))
        .and_then(|s| s.into_string().ok())
}

pub static DEFAULT_LOG_LEVEL: &'static str = "warn,dtps_http=info";

pub fn get_id_string() -> String {
    let exec_name = get_exec_name().unwrap_or("unknown".to_string());
    let git_version = built_info::GIT_VERSION.unwrap_or("unknown");
    let git_commit_hash = built_info::GIT_COMMIT_HASH.unwrap_or("unknown");
    let git_branch = built_info::GIT_HEAD_REF.unwrap_or("unknown");
    let git_branch = git_branch.trim_start_matches("refs/heads/");
    let arch = built_info::CFG_TARGET_ARCH;
    let family = built_info::CFG_FAMILY;
    let os = built_info::CFG_OS;
    let profile = built_info::PROFILE;

    // let version = env!("CARGO_PKG_VERSION");
    let pkg_name = built_info::PKG_NAME;
    format!("{exec_name} ({pkg_name} {git_version})  {family}/{os} {arch} ({git_branch} {git_commit_hash} {profile})")
}

pub fn init_logging() {
    eprintln!("{}", get_id_string());

    if env::var("RUST_LOG").is_err() {
        eprintln!("RUST_LOG not set: setting it to {DEFAULT_LOG_LEVEL}");
        env::set_var("RUST_LOG", DEFAULT_LOG_LEVEL)
    } else {
        eprintln!("RUST_LOG is set to {:?}", env::var("RUST_LOG"));
    }
    env_logger::init();
}
