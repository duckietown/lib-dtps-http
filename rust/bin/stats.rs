use dtps_http::cli_stats;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    cli_stats().await
}
