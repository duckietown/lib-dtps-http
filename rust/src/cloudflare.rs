use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::path::Path;
use std::string::ToString;
use std::sync::Arc as StdArc;
use tokio::task::JoinHandle;

use chrono::Local;
use clap::Parser;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use indent::indent_all_with;
use log::{debug, error, info, warn};
use maud::PreEscaped;
use maud::{html, DOCTYPE};
use serde::de::DeserializeOwned;
use serde_yaml;
use tokio::net::UnixListener;
use tokio::process::Command;
use tokio::signal::unix::SignalKind;
use tokio::spawn;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex as TokioMutex;
use tokio::time::{interval, Duration};
use tokio_stream::wrappers::{UnboundedReceiverStream, UnixListenerStream};
use tungstenite::http::{HeaderMap, HeaderValue, StatusCode};

use warp::hyper::Body;
use warp::reply::Response;
use warp::{Filter, Rejection};

use crate::constants::*;
use crate::html_utils::make_html;

use crate::master::{
    handle_websocket_generic2, serve_master_get, serve_master_head, serve_master_post,
};
use crate::object_queues::*;
use crate::server_state::*;
use crate::structures::*;
use crate::utils::divide_in_components;
use crate::utils_headers::{
    put_common_headers, put_header_content_type, put_header_location, put_meta_headers,
    put_source_headers,
};
use crate::websocket_signals::{
    ChannelInfo, ChannelInfoDesc, Chunk, MsgClientToServer, MsgServerToClient,
};
use crate::{
    error_other, error_with_info, format_digest_path, handle_rejection, not_available,
    parse_url_ext, utils, DTPSError, TopicName, DTPSR,
};

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CloudflareTunnel {
    pub AccountTag: String,
    pub TunnelSecret: String,
    pub TunnelID: String,
    pub TunnelName: String,
}

pub async fn open_cloudflare(
    port: u16,
    tunnel_file: &str,
    cloudflare_executable: &str,
) -> JoinHandle<()> {
    let contents = std::fs::File::open(tunnel_file).expect("file not found");
    let tunnel_info: CloudflareTunnel =
        serde_json::from_reader(contents).expect("error while reading file");
    let hoststring_127 = format!("127.0.0.1:{}", port);
    let cmdline = [
        "tunnel",
        "run",
        "--protocol",
        "http2",
        // "--no-autoupdate",
        "--cred-file",
        &tunnel_file,
        "--url",
        &hoststring_127,
        &tunnel_info.TunnelID,
    ];
    info!("starting tunnel: {:?}", cmdline);

    let mut child = Command::new(cloudflare_executable)
        .args(cmdline)
        // .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to start ping process");

    let handle = spawn(async move {
        let output = child.wait().await;
        error!("tunnel exited: {:?}", output);
    });
    handle
}
