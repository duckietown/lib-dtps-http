use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, ToSocketAddrs};
use std::process::{Command, Stdio};
use std::string::ToString;
use std::sync::Arc;
use std::sync::mpsc::{channel, Sender};
use std::thread;

use chrono::prelude::*;
use clap::Parser;
use tokio::spawn;
use tokio::sync::Mutex;
use tokio::time::{Duration, interval};

use server::*;
use server_state::*;

mod constants;
mod object_queues;
mod server;
mod server_state;
mod structures;
mod types;

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
        let inserted = ss.publish_json(topic_name, &s);

        // println!("inserted {}: {:?}", topic_name, inserted);
    }
}


const DEFAULT_PORT: u16 = 8000;
const DEFAULT_HOST: &str = "0.0.0.0";
const DEFAULT_CLOUDFLARE_EXECUTABLE: &str = "cloudflared";

/// DTPS HTTP server
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// TCP Port to bind to
    #[arg(long, default_value_t = DEFAULT_PORT)]
    tcp_port: u16,

    /// Hostname to bind to
    #[arg(long, default_value_t = DEFAULT_HOST.to_string())]
    tcp_host: String,

    /// Cloudflare tunnel to start
    #[arg(long)]
    tunnel: Option<String>,

    /// Cloudflare executable filename
    #[arg(long, default_value_t = DEFAULT_CLOUDFLARE_EXECUTABLE.to_string())]
    cloudflare_executable: String,

}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let hoststring = format!("{}:{}", args.tcp_host, args.tcp_port);
    let mut addrs_iter = hoststring.to_socket_addrs().unwrap();
    let one_addr = addrs_iter.next().unwrap();
    println!("dtps-http/rust server listening on {one_addr}");

    let mut server = DTPSServer::new();

    spawn(clock_go(server.get_lock(), "clock", 1.0));
    spawn(clock_go(server.get_lock(), "clock5", 5.0));
    spawn(clock_go(server.get_lock(), "clock7", 7.0));
    spawn(clock_go(server.get_lock(), "clock11", 11.0));

    let server_proc = server.serve(one_addr);

    // if tunnel is given ,start
    if let Some(tunnel_name) = args.tunnel {
        let hoststring_127 = format!("127.0.0.1:{}", args.tcp_port);
        let cmdline = [
            // "cloudflared",
            "tunnel",
            "run",
            "--protocol", "http2",
            "--cred-file",
            &tunnel_name,
            "--url",
            &hoststring_127,
            &tunnel_name,
        ];
        println!("starting tunnel: {:?}", cmdline);

        let child = Command::new(args.cloudflare_executable)
            .args(cmdline)
            // .stdout(Stdio::piped())
            .spawn()
            .expect("Failed to start ping process");

        println!("Started process: {}", child.id());

        // let tunnel_proc = server.start_tunnel(tunnel_name);
        // let _ = tokio::join!(server_proc.await, tunnel_proc.await);
        // return;
    }

    return server_proc.await;
}
