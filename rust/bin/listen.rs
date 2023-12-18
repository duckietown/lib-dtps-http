use dtps_http::cli_listen;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    cli_listen().await
}
