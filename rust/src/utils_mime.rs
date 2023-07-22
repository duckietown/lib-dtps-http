pub fn is_html(content_type: &str) -> bool {
    content_type.starts_with("text/html")
}

pub fn is_image(content_type: &str) -> bool {
    content_type.starts_with("image/")
}

pub fn identify_content_presentation(content_type: &str) -> Option<&'static str> {
    if content_type.ends_with("cbor") {
        return Some("application/cbor");
    } else if content_type.ends_with("json") {
        return Some("application/json");
    } else if content_type.ends_with("yaml") {
        return Some("application/yaml");
    } else if content_type.starts_with("text/") {
        return Some("text/plain");
    } else {
        None
    }
}
