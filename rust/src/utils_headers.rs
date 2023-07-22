use crate::{
    get_id_string, ServerState, CONTENT_TYPE_DTPS_INDEX_CBOR, EVENTS_SUFFIX, EVENTS_SUFFIX_DATA,
    HEADER_DATA_ORIGIN_NODE_ID, HEADER_DATA_UNIQUE_ID, HEADER_NODE_ID, HEADER_SEE_EVENTS,
    HEADER_SEE_EVENTS_INLINE_DATA, REL_EVENTS_DATA, REL_EVENTS_NODATA, REL_META,
};
use http::{header, HeaderMap, HeaderValue};

pub fn put_link_header(
    h: &mut HeaderMap<HeaderValue>,
    url: &str,
    rel: &str,
    content_type: Option<&str>,
) {
    let s = match content_type {
        None => {
            format!("<{url}>; rel={rel}")
        }
        Some(c) => {
            format!("<{url}>; rel={rel}; type={c}")
        }
    };
    h.append("Link", HeaderValue::from_str(&s).unwrap());
}

pub fn put_source_headers(h: &mut HeaderMap<HeaderValue>, origin_node: &str, unique_id: &str) {
    h.append(
        HEADER_DATA_ORIGIN_NODE_ID,
        HeaderValue::from_str(origin_node).unwrap(),
    );
    h.append(
        HEADER_DATA_UNIQUE_ID,
        HeaderValue::from_str(unique_id).unwrap(),
    );
}

pub fn put_header_location(h: &mut HeaderMap<HeaderValue>, location: &str) {
    h.append(
        header::CONTENT_LOCATION,
        HeaderValue::from_str(location).unwrap(),
    );
}

pub fn put_header_content_type(h: &mut HeaderMap<HeaderValue>, content_type: &str) {
    h.append(
        header::CONTENT_TYPE,
        HeaderValue::from_str(content_type).unwrap(),
    );
}

pub fn put_common_headers(ss: &ServerState, headers: &mut HeaderMap<HeaderValue>) {
    headers.append(
        header::SERVER,
        HeaderValue::from_str(get_id_string().as_str()).unwrap(),
    );
    headers.append(
        HEADER_NODE_ID,
        HeaderValue::from_str(ss.node_id.as_str()).unwrap(),
    );
}

pub fn put_meta_headers(h: &mut HeaderMap<HeaderValue>) {
    h.append(HEADER_SEE_EVENTS, HeaderValue::from_static(EVENTS_SUFFIX));
    h.append(
        HEADER_SEE_EVENTS_INLINE_DATA,
        HeaderValue::from_static(EVENTS_SUFFIX_DATA),
    );

    put_link_header(h, EVENTS_SUFFIX, REL_EVENTS_NODATA, Some("websocket"));
    put_link_header(h, EVENTS_SUFFIX_DATA, REL_EVENTS_DATA, Some("websocket"));
    put_link_header(h, ":meta", REL_META, Some(CONTENT_TYPE_DTPS_INDEX_CBOR));
}
