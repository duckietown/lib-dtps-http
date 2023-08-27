use std::collections::HashMap;

use http::{header, HeaderMap, HeaderValue};
use maplit::hashmap;

use crate::{
    get_id_string, ServerState, TopicProperties, CONTENT_TYPE_DTPS_INDEX_CBOR, EVENTS_SUFFIX,
    HEADER_DATA_ORIGIN_NODE_ID, HEADER_DATA_UNIQUE_ID, HEADER_NODE_ID, REL_EVENTS_DATA,
    REL_EVENTS_NODATA, REL_HISTORY, REL_META, REL_URL_META, URL_HISTORY,
};

#[derive(Debug, Clone, PartialEq)]
pub struct LinkHeader {
    pub url: String,
    pub attributes: HashMap<String, String>,
}

impl LinkHeader {
    pub fn get(&self, a: &str) -> Option<String> {
        let k = a.to_string();
        return self.attributes.get(&k).map(String::clone);
    }
    pub fn format_header_value(&self) -> String {
        let url = &self.url;

        let s = format!("<{url}>");
        let mut parts = vec![s];

        for (k, v) in &self.attributes {
            parts.push(format!("{k}={v}"));
        }

        parts.join("; ")
    }

    pub fn from_header_value(value: &str) -> Self {
        // first split according to ;
        let mut parts = value.split(';').collect::<Vec<_>>();
        let url = parts
            .remove(0)
            .trim()
            .trim_start_matches('<')
            .trim_end_matches('>')
            .to_string();

        let mut attributes = HashMap::new();
        for part in parts {
            let attr_parts: Vec<&str> = part.split('=').collect();
            if attr_parts.len() == 2 {
                let key = attr_parts[0].trim().to_string();
                let value = attr_parts[1].trim().to_string();
                attributes.insert(key, value);
            }
        }

        Self { url, attributes }
    }
}

pub fn put_link_header(
    h: &mut HeaderMap<HeaderValue>,
    url: &str,
    rel: &str,
    content_type: Option<&str>,
) {
    let mut l = LinkHeader {
        url: url.to_string(),
        attributes: hashmap! {
            "rel".to_string() => rel.to_string(),
        },
    };
    if let Some(content_type) = content_type {
        l.attributes
            .insert("type".to_string(), content_type.to_string());
    }

    let s = l.format_header_value();
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

pub fn put_header_accept(h: &mut HeaderMap<HeaderValue>, content_type: &str) {
    h.append(header::ACCEPT, HeaderValue::from_str(content_type).unwrap());
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

pub fn put_meta_headers(h: &mut HeaderMap<HeaderValue>, tp: &TopicProperties) {
    // h.append(HEADER_SEE_EVENTS, HeaderValue::from_static(EVENTS_SUFFIX));
    // h.append(
    //     HEADER_SEE_EVENTS_INLINE_DATA,
    //     HeaderValue::from_static(EVENTS_SUFFIX_DATA),
    // );
    if tp.streamable {
        put_link_header(
            h,
            &format!("{EVENTS_SUFFIX}/"),
            REL_EVENTS_NODATA,
            Some("websocket"),
        );
        put_link_header(
            h,
            &format!("{EVENTS_SUFFIX}/?send_data=1"),
            REL_EVENTS_DATA,
            Some("websocket"),
        );
    }

    put_link_header(
        h,
        &format!("{REL_URL_META}/"),
        REL_META,
        Some(CONTENT_TYPE_DTPS_INDEX_CBOR),
    );
    if tp.has_history {
        put_link_header(
            h,
            &format!("{URL_HISTORY}/"),
            REL_HISTORY,
            Some(CONTENT_TYPE_DTPS_INDEX_CBOR),
        );
    }
}

#[cfg(test)]
mod tests {

    // Bring the function into scope

    use maplit::hashmap;

    use crate::utils_headers::LinkHeader;

    #[test]
    fn link_parse_1() {
        let s = "<:events?send_data=1>; rel=dtps-events-inline-data; type=websocket";
        let found = LinkHeader::from_header_value(s);
        let expected = LinkHeader {
            url: ":events?send_data=1".to_string(),
            attributes: hashmap! {
                "rel".to_string() => "dtps-events-inline-data".to_string(),
                "type".to_string() => "websocket".to_string(),
            },
        };

        assert_eq!(found, expected);
    }
}

pub fn get_accept_header(headers: &HeaderMap) -> Vec<String> {
    let accept_header = headers.get("accept");
    match accept_header {
        Some(x) => {
            let accept_header = x.to_str().unwrap();
            let accept_header = accept_header
                .split(",")
                .map(|x| x.trim().to_string())
                .collect();
            accept_header
        }
        None => vec![],
    }
}
