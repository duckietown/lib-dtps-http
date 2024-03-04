extern crate dtps_http;

use dtps_http::{cli_server, cli_stats, cli_subscribe};
use std::{env, process};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.is_empty() {
        println!("Usage: program <subcommand> [<args>]");
        process::exit(1);
    }

    match args[1].as_str() {
        "server" => cli_server().await,
        "subscribe" => cli_subscribe().await,
        "stats" => cli_stats().await,
        _ => {
            println!("Invalid subcommand. Usage: program <subcommand> [<args>]");
            process::exit(2);
        }
    }
}
