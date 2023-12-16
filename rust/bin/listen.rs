use dtps_http::cli_listen;

#[tokio::main]
async fn main() -> () {
    cli_listen().await;
}
