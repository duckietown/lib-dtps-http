use std::collections::HashSet;
use std::os::unix::fs::FileTypeExt;
use std::path::Path;
use std::time::Duration;

use tokio::time::timeout;

use crate::connections::TypeOfConnection;
use crate::structures_linkproperties::LinkBenchmark;
use crate::{debug_with_info, not_available, not_reachable};
use crate::{DTPSError, DTPSR};

#[derive(Debug, Clone)]
pub enum UrlResult {
    /// Cannot reach this URL
    Inaccessible(String),
    /// We cannot find the expected node answering
    WrongNodeAnswering,
    /// It is accessbile with this benchmark
    Accessible(LinkBenchmark),
}

pub async fn get_stats(con: &TypeOfConnection, expect_node_id: Option<String>) -> UrlResult {
    let md = crate::get_metadata(con).await;
    let complexity = match con {
        TypeOfConnection::TCP(_) => 2,
        TypeOfConnection::UNIX(_) => 1,
        TypeOfConnection::Relative(_, _) => {
            panic!("unexpected relative url here: {}", con);
        }
        TypeOfConnection::Same() => {
            panic!("not expected here {}", con);
        }
        TypeOfConnection::File(..) => 0,
    };
    let reliability_percent = match con {
        TypeOfConnection::TCP(_) => 70,
        TypeOfConnection::UNIX(_) => 90,
        TypeOfConnection::Relative(_, _) => {
            panic!("unexpected relative url here: {}", con);
        }
        TypeOfConnection::Same() => {
            panic!("not expected here {}", con);
        }
        TypeOfConnection::File(..) => 100,
    };
    match md {
        Err(err) => {
            let s = format!("cannot get metadata for {:?}: {}", con, err);

            UrlResult::Inaccessible(s)
        }
        Ok(md_) => {
            if incompatible(&expect_node_id, &md_.answering) {
                UrlResult::WrongNodeAnswering
            } else {
                let latency_ns = md_.latency_ns;
                let lb = LinkBenchmark {
                    complexity,
                    bandwidth: 100_000_000,
                    latency_ns,
                    reliability_percent,
                    hops: 1,
                };
                UrlResult::Accessible(lb)
            }
        }
    }
}

pub fn incompatible(expect_node_id: &Option<String>, answering: &Option<String>) -> bool {
    match (expect_node_id, answering) {
        (None, None) => false,
        (None, Some(_)) => false,
        (Some(_), None) => true,
        (Some(e), Some(a)) => e != a,
    }
}

pub async fn compute_best_alternative(
    alternatives: &HashSet<TypeOfConnection>,
    expect_node_id: Option<String>,
) -> DTPSR<TypeOfConnection> {
    let mut possible_urls: Vec<TypeOfConnection> = Vec::new();
    let mut possible_stats: Vec<LinkBenchmark> = Vec::new();
    let mut i = 0;
    let n = alternatives.len();
    for alternative in alternatives.iter() {
        i += 1;
        debug_with_info!("Trying {}/{}: {}", i, n, alternative);
        let result_future = get_stats(alternative, expect_node_id.clone());

        let result = match timeout(Duration::from_millis(2000), result_future).await {
            Ok(r) => r,
            Err(_) => {
                debug_with_info!("-> Timeout: {}", alternative);
                continue;
            }
        };

        match result {
            UrlResult::Inaccessible(why) => {
                debug_with_info!("-> Inaccessible: {}", why);
            }
            UrlResult::WrongNodeAnswering => {
                debug_with_info!("-> Wrong node answering");
            }
            UrlResult::Accessible(link_benchmark) => {
                debug_with_info!("-> Accessible: {:?}", link_benchmark);
                possible_urls.push(alternative.clone());
                possible_stats.push(link_benchmark);
            }
        }
    }
    // if no alternative is accessible, return None
    if possible_urls.is_empty() {
        return Err(DTPSError::ResourceNotReachable(
            "no alternative are accessible".to_string(),
        ));
        // return Err("no alternative are accessible".into());
    }
    // get the index of minimum possible_stats
    let min_index = possible_stats
        .iter()
        .enumerate()
        .min_by_key(|&(_, item)| item)
        .unwrap()
        .0;
    let best_url = possible_urls[min_index].clone();
    debug_with_info!(
        "Best is {}: {} with {:?}",
        min_index,
        best_url,
        possible_stats[min_index]
    );
    Ok(best_url)
}

pub async fn check_unix_socket(file_path: &str) -> DTPSR<()> {
    let path = Path::new(file_path);
    match tokio::fs::metadata(path).await {
        Ok(md) => {
            let is_socket = md.file_type().is_socket();
            if !is_socket {
                not_reachable!("File {file_path} exists but it is not a socket.")
            } else {
                Ok(())
            }
        }
        Err(e) => {
            // check parent directory
            match path.parent() {
                None => {
                    not_available!("Cannot get parent directory of: {file_path:?}")
                }
                Some(parent) => {
                    if !parent.exists() {
                        not_available!("Socket not available and parent directory does not exist: {file_path:?}")
                    } else {
                        not_available!("Parent dir exists but socket does not exist: {file_path:?}: \n {e}")
                    }
                }
            }
        }
    }
}
