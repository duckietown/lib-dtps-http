pub static CONTENT_TYPE: &str = "content-type";

pub static CONTENT_TYPE_TEXT_HTML: &str = "text/html";
pub static CONTENT_TYPE_DTPS_INDEX: &str = "application/vnd.dt.dtps-index";
pub static CONTENT_TYPE_DTPS_INDEX_CBOR: &str = "application/vnd.dt.dtps-index+cbor";
pub static CONTENT_TYPE_TOPIC_HISTORY_CBOR: &str = "application/vnd.dt.dtps-history+cbor";
pub static HEADER_CONTENT_LOCATION: &str = "content-location";
pub static HEADER_DATA_ORIGIN_NODE_ID: &str = "x-dtps-data-origin-node";
pub static HEADER_DATA_UNIQUE_ID: &str = "x-dtps-data-unique-id";
pub static HEADER_LINK_BENCHMARK: &str = "x-dtps-link-benchmark";
pub static HEADER_NODE_ID: &str = "x-dtps-node-id";
// pub static HEADER_NODE_PASSED_THROUGH: &str = "x-dtps-node-id-passed-through"; // should be Via
pub static CONTENT_TYPE_OCTET_STREAM: &str = "application/octet-stream";
pub static CONTENT_TYPE_TEXT_PLAIN: &str = "text/plain";
pub static TOPIC_LIST_AVAILABILITY: &str = "dtps/availability";
pub static TOPIC_LIST_CLOCK: &str = "dtps/clock";
pub static TOPIC_LIST_NAME: &str = "dtps/topic_list";
pub static TOPIC_LOGS: &str = "dtps/logs";
pub static TOPIC_PROXIED: &str = "dtps/proxied";
pub static TOPIC_CONNECTIONS: &str = "dtps/connections";
pub static TOPIC_STATE_NOTIFICATION: &str = "dtps/states-notification";
pub static TOPIC_STATE_SUMMARY: &str = "dtps/state";
// pub static VENDOR_PREFIX: &str = "application/vnd.dt.";

pub static CONTENT_TYPE_CBOR: &str = "application/cbor";
pub static CONTENT_TYPE_JSON: &str = "application/json";
pub static CONTENT_TYPE_PATCH_CBOR: &str = "application/json-patch+cbor";
pub static CONTENT_TYPE_PATCH_JSON: &str = "application/json-patch+json";
pub static CONTENT_TYPE_PLAIN: &str = "text/plain";
pub static CONTENT_TYPE_YAML: &str = "application/yaml";
pub static EVENTS_SUFFIX: &str = ":events";
pub static REL_EVENTS_DATA: &str = "dtps-events-inline-data";
pub static REL_STREAM_PUSH: &str = "dtps-events-push";
pub static EVENTS_STREAM_PUSH_SUFFIX: &str = ":events-push";

pub static REL_EVENTS_NODATA: &str = "dtps-events";

pub static REL_PROXIED: &str = "dtps-proxied";
pub static REL_CONNECTIONS: &str = "dtps-connections";
pub static REL_HISTORY: &str = "dtps-history";
pub static REL_META: &str = "dtps-meta";
pub static REL_URL_DEREF: &str = ":deref";
pub static REL_URL_META: &str = ":meta";
pub static URL_HISTORY: &str = ":history";
pub static ENV_MASK_ORIGIN: &str = "DTPS_HTTP_MASK_ORIGIN";

#[cfg(test)]
mod test {
    use mime::Mime;

    use super::*;

    #[test]
    fn test_mime_parsing() {
        let content_type: Mime = CONTENT_TYPE_DTPS_INDEX_CBOR.parse().unwrap();
        eprintln!("content_type: {:#?}", content_type);
        eprintln!("type_: {:#?}", content_type.type_());
        eprintln!("subtype: {:#?}", content_type.subtype());
        eprintln!("suffix: {:#?}", content_type.suffix());
    }
}
