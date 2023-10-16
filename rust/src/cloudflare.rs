use serde::{
    Deserialize,
    Serialize,
};

use tokio::{
    process::Command,
    spawn,
    task::JoinHandle,
};

use crate::{
    error_with_info,
    info_with_info,
};

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CloudflareTunnel {
    pub AccountTag: String,
    pub TunnelSecret: String,
    pub TunnelID: String,
    pub TunnelName: String,
}

pub async fn open_cloudflare(port: u16, tunnel_file: &str, cloudflare_executable: &str) -> JoinHandle<()> {
    let contents = std::fs::File::open(tunnel_file).expect("file not found");
    let tunnel_info: CloudflareTunnel = serde_json::from_reader(contents).expect("error while reading file");
    let hoststring_127 = format!("127.0.0.1:{}", port);
    let cmdline = [
        "tunnel",
        "run",
        "--protocol",
        "http2",
        // "--no-autoupdate",
        "--cred-file",
        tunnel_file,
        "--url",
        &hoststring_127,
        &tunnel_info.TunnelID,
    ];
    info_with_info!("starting tunnel: {:?}", cmdline);

    let mut child = Command::new(cloudflare_executable)
        .args(cmdline)
        // .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to start ping process");

    spawn(async move {
        let output = child.wait().await;
        error_with_info!("tunnel exited: {:?}", output);
    })
}
