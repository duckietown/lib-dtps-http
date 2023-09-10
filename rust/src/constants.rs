pub static HEADER_NODE_ID: &'static str = "x-dtps-node-id";
pub static HEADER_NODE_PASSED_THROUGH: &'static str = "x-dtps-node-id-passed-through"; // should be Via
pub static HEADER_LINK_BENCHMARK: &'static str = "x-dtps-link-benchmark";
pub static HEADER_DATA_UNIQUE_ID: &'static str = "x-dtps-data-unique-id";
pub static HEADER_DATA_ORIGIN_NODE_ID: &'static str = "x-dtps-data-origin-node";
pub static HEADER_CONTENT_LOCATION: &'static str = "content-location";

pub static TOPIC_LIST_NAME: &'static str = "dtps/topic_list";
pub static TOPIC_LIST_CLOCK: &'static str = "dtps/clock";
pub static TOPIC_LOGS: &'static str = "dtps/logs";
pub static TOPIC_STATE_NOTIFICATION: &'static str = "dtps/states-notification";
pub static TOPIC_STATE_SUMMARY: &'static str = "dtps/state";
pub static TOPIC_LIST_AVAILABILITY: &'static str = "dtps/availability";

pub static TOPIC_PROXIED: &'static str = "dtps/proxied";

pub static CONTENT_TYPE: &'static str = "content-type";
pub static OCTET_STREAM: &'static str = "application/octet-stream";

pub static VENDOR_PREFIX: &'static str = "application/vnd.dt.";
pub static CONTENT_TYPE_DTPS_INDEX: &'static str = "application/vnd.dt.dtps-index";
pub static CONTENT_TYPE_DTPS_INDEX_CBOR: &'static str = "application/vnd.dt.dtps-index+cbor";
pub static CONTENT_TYPE_TOPIC_HISTORY_CBOR: &'static str = "application/vnd.dt.dtps-history+cbor";

pub static CONTENT_TYPE_JSON: &'static str = "application/json";
pub static CONTENT_TYPE_CBOR: &'static str = "application/cbor";
pub static CONTENT_TYPE_YAML: &'static str = "application/yaml";
pub static CONTENT_TYPE_PLAIN: &'static str = "text/plain";

pub static CONTENT_TYPE_PATCH_JSON: &'static str = "application/json-patch+json";
pub static CONTENT_TYPE_PATCH_CBOR: &'static str = "application/cbor-patch+cbor";
pub static EVENTS_SUFFIX: &'static str = ":events";
pub static REL_URL_META: &'static str = ":meta";
pub static REL_URL_DEREF: &'static str = ":deref";

pub static REL_HISTORY: &'static str = "dtps-history";
pub static URL_HISTORY: &'static str = ":history";

#[cfg(test)]

mod test {
    use mime::Mime;

    use super::*;

    #[test]
    fn test_mime_parsing() {
        let content_type: Mime = CONTENT_TYPE_DTPS_INDEX_CBOR.parse().unwrap();
        eprintln!("content_type: {:#?}", content_type);
        // eprintln!("source: {:#?}", content_type.source());
        eprintln!("type_: {:#?}", content_type.type_());
        eprintln!("subtype: {:#?}", content_type.subtype());
        eprintln!("suffix: {:#?}", content_type.suffix());
    }
}

pub static REL_EVENTS_NODATA: &'static str = "dtps-events";
pub static REL_EVENTS_DATA: &'static str = "dtps-events-inline-data";
pub static REL_META: &'static str = "dtps-meta";
