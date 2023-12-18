use dtps_http::cli_subscribe;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    cli_subscribe().await
}
