extern crate url;

use std::error;

use clap::Parser;

use crate::get_events_stream_inline;
use crate::get_metadata;
use crate::wrap_recv;
use crate::{debug_with_info, init_logging, parse_url_ext};

/// Parameters for client
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct ListenArgs {
    /// base URL to open
    #[arg(long)]
    url: String,

    /// Expected node ID
    #[arg(long)]
    expect: Option<String>,

    /// Maximum length in seconds to run
    #[arg(long, default_value_t = 1000000)]
    max_time: u64,
    /// Maximum number of messages to receive
    #[arg(long, default_value_t = 1000000)]
    max_messages: u64,

    /// Whether to raise an error if the server returns an error
    #[arg(long)]
    raise_on_error: bool,
}

pub async fn cli_listen() -> Result<(), Box<dyn error::Error>> {
    init_logging();
    let args = ListenArgs::parse();
    // print!("{} {}", args.url, args.inline_data);

    let bc = parse_url_ext(args.url.as_str())?;
    let md = get_metadata(&bc).await?;
    md.get_answering()?;
    let inline_url = md.events_data_inline_url.ok_or_else(|| "No inline URL found")?;

    let (handle, mut stream) = get_events_stream_inline(&inline_url).await;

    while let Some(msg) = wrap_recv(&mut stream).await {
        debug_with_info!("msg: {:#?}", msg);
    }

    Ok(())
}
