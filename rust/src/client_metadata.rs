use std::collections::HashSet;

use maplit::hashmap;

use crate::utils_headers::{get_content_type, string_from_header_value, LinkHeader};
use crate::utils_time::time_nanos;
use crate::TypeOfResource;
use crate::{
    client_verbs, client_websocket_read, parse_url_ext, utils_queues, DTPSError, FoundMetadata, ListenURLEvents,
    TopicName, CONTENT_TYPE_DTPS_INDEX, DTPSR, HEADER_CONTENT_LOCATION, HEADER_NODE_ID, REL_CONNECTIONS,
    REL_EVENTS_DATA, REL_EVENTS_NODATA, REL_HISTORY, REL_META, REL_PROXIED, REL_STREAM_PUSH,
};
use crate::{debug_with_info, error_with_info, info_with_info, warn_with_info};
use crate::{join_ext, TypeOfConnection};

pub async fn estimate_latencies(which: TopicName, md: FoundMetadata) {
    let inline_url = md.events_data_inline_url.unwrap().clone();

    let (handle, mut rx) = client_websocket_read::get_events_stream_inline(&inline_url).await;

    // keep track of the latencies in a vector and compute the mean

    let mut latencies_ns = Vec::new();
    let mut index = 0;

    while let Some(lue) = utils_queues::wrap_recv(&mut rx).await {
        // convert a string to integer
        match lue {
            ListenURLEvents::InsertNotification(notification) => {
                let string = String::from_utf8(notification.raw_data.content.to_vec()).unwrap();

                // let nanos = serde_json::from_slice::<u128>(&notification.rd.content).unwrap();
                let nanos: u128 = string.parse().unwrap();
                let nanos_here = time_nanos();
                let diff = nanos_here - nanos;
                if index > 0 {
                    // ignore the first one
                    latencies_ns.push(diff);
                }
                if latencies_ns.len() > 10 {
                    latencies_ns.remove(0);
                }
                if !latencies_ns.is_empty() {
                    let latencies_sum_ns: u128 = latencies_ns.iter().sum();
                    let latencies_mean_ns = (latencies_sum_ns) / (latencies_ns.len() as u128);
                    let latencies_min_ns = *latencies_ns.iter().min().unwrap();
                    let latencies_max_ns = *latencies_ns.iter().max().unwrap();
                    info_with_info!(
                        "{:?} latency: {:.3}ms   (last {} : mean: {:.3}ms  min: {:.3}ms  max {:.3}ms )",
                        which,
                        ms_from_ns(diff),
                        latencies_ns.len(),
                        ms_from_ns(latencies_mean_ns),
                        ms_from_ns(latencies_min_ns),
                        ms_from_ns(latencies_max_ns)
                    );
                }
            }
            ListenURLEvents::WarningMsg(msg) => {
                warn_with_info!("{}", msg.to_string());
            }
            ListenURLEvents::ErrorMsg(msg) => {
                error_with_info!("{}", msg.to_string());
            }
            ListenURLEvents::FinishedMsg(msg) => {
                info_with_info!("finished: {}", msg.comment);
            }
            ListenURLEvents::SilenceMsg(_) => {}
        }

        index += 1;
    }
    match handle.await {
        Ok(_) => {}
        Err(e) => {
            error_with_info!("error in handle: {:?}", e);
        }
    };
}

pub fn ms_from_ns(ns: u128) -> f64 {
    (ns as f64) / 1_000_000.0
}

pub async fn sniff_type_resource(con: &TypeOfConnection) -> DTPSR<TypeOfResource> {
    let md = get_metadata(con).await?;
    let content_type = &md.content_type;
    debug_with_info!("content_type: {:#?}", content_type);

    if content_type.contains(CONTENT_TYPE_DTPS_INDEX) {
        match &md.answering {
            None => {
                debug_with_info!("This looks like an index but could not get answering:\n{md:#?}");
                Ok(TypeOfResource::Other)
            }
            Some(node_id) => Ok(TypeOfResource::DTPSIndex {
                node_id: node_id.clone(),
            }),
        }
    } else {
        Ok(TypeOfResource::Other)
    }
}

pub async fn get_metadata(conbase: &TypeOfConnection) -> DTPSR<FoundMetadata> {
    // current time in nano seconds
    let start = time_nanos();

    let resp = client_verbs::make_request(conbase, hyper::Method::HEAD, b"", None, None).await?;
    let status = resp.status();
    if !status.is_success() {
        let desc = status.as_str();
        let msg = format!("cannot get metadata for {conbase}: {desc}");
        return Err(DTPSError::FailedRequest(
            msg,
            status.as_u16(),
            conbase.to_string(),
            desc.to_string(),
        ));
    }
    let end = time_nanos();

    let latency_ns = end - start;

    // get the headers from the response
    let headers = resp.headers();

    // get all the HEADER_CONTENT_LOCATION in the response
    let alternatives0 = headers.get_all(HEADER_CONTENT_LOCATION);
    // debug_with_info!("alternatives0: {:#?}", alternatives0);
    // convert into a vector of strings
    let alternative_urls: Vec<String> = alternatives0.iter().map(string_from_header_value).collect();

    // convert into a vector of URLs
    let mut alternative_urls: Vec<TypeOfConnection> =
        alternative_urls.iter().map(|x| parse_url_ext(x).unwrap()).collect();
    alternative_urls.push(conbase.clone());

    let answering = headers.get(HEADER_NODE_ID).map(string_from_header_value);

    let mut links = hashmap! {};
    for v in headers.get_all("link") {
        let link = LinkHeader::from_header_value(&string_from_header_value(v));

        let rel = match link.get("rel") {
            None => {
                continue;
            }
            Some(r) => r,
        };
        links.insert(rel, link);
    }
    let alternative_urls: HashSet<_> = alternative_urls.iter().cloned().collect();

    let get_if_exists = |w: &str| -> Option<TypeOfConnection> {
        if let Some(l) = links.get(w) {
            let url = l.url.clone();
            let url = join_ext(conbase, &url).ok()?;
            Some(url)
        } else {
            None
        }
    };
    let content_type = get_content_type(&resp);
    let events_url = get_if_exists(REL_EVENTS_NODATA);
    let events_data_inline_url = get_if_exists(REL_EVENTS_DATA);
    let meta_url = get_if_exists(REL_META);
    let history_url = get_if_exists(REL_HISTORY);
    let connections_url = get_if_exists(REL_CONNECTIONS);
    let proxied_url = get_if_exists(REL_PROXIED);
    let stream_push_url = get_if_exists(REL_STREAM_PUSH);
    let base_url = conbase.clone();
    let md = FoundMetadata {
        base_url,
        alternative_urls,
        events_url,
        answering,
        events_data_inline_url,
        latency_ns,
        meta_url,
        history_url,
        content_type,
        proxied_url,
        connections_url,
        stream_push_url,
    };

    Ok(md)
}
