use dtps_http::cli_server;

extern crate dtps_http;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // check first argument
    // if first argument is "listen", call cli_listen()
    // if first argument is "subscribe", call cli_subscribe()
    // if first argument is "stats", call cli_stats()
    // if first argument is "server", call cli_server()
    // else, print usage

    cli_server().await
}
