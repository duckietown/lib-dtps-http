use dtps_http::cli_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    cli_server().await
}
