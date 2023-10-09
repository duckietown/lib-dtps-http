extern crate dtps_http;
extern crate url;

use std::error;

use clap::Parser;
use futures::future::join_all;
use tokio::{
    spawn,
    task::JoinHandle,
};

use dtps_http::{
    compute_best_alternative,
    debug_with_info,
    estimate_latencies,
    get_index,
    get_metadata,
    init_logging,
    parse_url_ext,
    warn_with_info,
};

/// Parameters for client
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct StatsArgs {
    /// base URL to open
    #[arg(long)]
    url: String,

    /// Cloudflare tunnel to start
    #[arg(long)]
    inline_data: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    init_logging();
    // debug_with_info!("now waiting 1 seconds so that the server can start");
    // sleep(Duration::from_secs(1)).await;

    let args = StatsArgs::parse();
    // print!("{} {}", args.url, args.inline_data);

    let bc = parse_url_ext(args.url.as_str())?;
    // debug_with_info!("connection base: {:#?}", bc);
    let md = get_metadata(&bc).await?;
    // debug_with_info!("metadata:\n{:#?}", md);
    md.get_answering()?;
    let best = compute_best_alternative(&md.alternative_urls, md.answering.unwrap().as_str()).await?;
    warn_with_info!("Best connection: {} ", best);

    let x = get_index(&best).await?;

    warn_with_info!("Internal: {:#?} ", x);
    //
    // if best.is_none() {
    //     info_with_info!("no alternative url found");
    //     return;
    // }
    // let use_url = best.unwrap();
    //
    // if use_url.is_none() {
    //     info_with_info!("no alternative url found");
    //     return;
    // }

    // debug_with_info!("{:#?}", x);
    let mut handles: Vec<JoinHandle<_>> = Vec::new();

    for (topic_name, topic_info) in &x.topics {
        // debug_with_info!("{}", topic_name);
        for r in &topic_info.reachability {
            // let real_uri = urlbase.join(&r.url).unwrap();
            // debug_with_info!("{}  {} -> {}", topic_name, r.url, real_uri);
            let md_res = get_metadata(&r.con).await;
            // debug_with_info!("md for {}: {:#?}", topic_name, md_res);
            let md;
            match md_res {
                Ok(md_) => {
                    md = md_;
                }
                Err(_) => {
                    warn_with_info!("cannot get metadata for {:?}", r.con);
                    continue;
                }
            }

            if topic_name.as_relative_url().contains("clock") {
                let handle = spawn(estimate_latencies(topic_name.clone(), md));
                handles.push(handle);
            }
        }
    }
    // listen to all spawned tasks
    let results = join_all(handles).await;

    for result in results {
        match result {
            Ok(val) => debug_with_info!("Finished task with result: {:?}", val),
            Err(err) => debug_with_info!("Task returned an error: {:?}", err),
        }
    }

    Ok(())
}
