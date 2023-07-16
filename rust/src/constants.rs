pub static HEADER_SEE_EVENTS: &'static str = "X-dtps-events";
pub static HEADER_SEE_EVENTS_INLINE_DATA: &'static str = "X-dtps-events-inline-data";

pub static HEADER_NODE_ID: &'static str = "X-DTPS-Node-ID";
pub static HEADER_NODE_PASSED_THROUGH: &'static str = "X-DTPS-Node-ID-Passed-Through";
pub static HEADER_LINK_BENCHMARK: &'static str = "X-DTPS-link-benchmark";
pub static HEADER_DATA_UNIQUE_ID: &'static str = "X-DTPS-data-unique-id";
pub static HEADER_DATA_ORIGIN_NODE_ID: &'static str = "X-DTPS-data-origin-node";
pub static HEADER_CONTENT_LOCATION: &'static str = "Content-Location";

pub static TOPIC_LIST_NAME: &'static str = "dtps.topic_list";
pub static TOPIC_LIST_CLOCK: &'static str = "dtps.clock";
pub static TOPIC_LOGS: &'static str = "dtps.logs";
pub static TOPIC_LIST_AVAILABILITY: &'static str = "dtps.availability";

pub static CONTENT_TYPE: &'static str = "content-type";
pub static OCTET_STREAM: &'static str = "application/octet-stream";

pub static VENDOR_PREFIX: &'static str = "application/vnd.dt.";
pub static CONTENT_TYPE_DTPS_INDEX: &'static str = "application/vnd.dt.dtps-index";
pub static CONTENT_TYPE_DTPS_INDEX_CBOR: &'static str = "application/vnd.dt.dtps-index+cbor";

#[cfg(test)]

mod test {
    use super::*;

    use mime::Mime;

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
