use dtps_http::cli_stress_test;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    cli_stress_test().await
}
