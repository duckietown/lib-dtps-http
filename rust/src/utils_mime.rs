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

pub enum ContentPresentation {
    CBOR,
    JSON,
    YAML,
    PlainText,
    Other,
}

pub fn identify_presentation(content_type: &str) -> ContentPresentation {
    return if content_type.ends_with("cbor") {
        ContentPresentation::CBOR
    } else if content_type.ends_with("json") {
        ContentPresentation::JSON
    } else if content_type.ends_with("yaml") {
        ContentPresentation::YAML
    } else if content_type.starts_with("text/") {
        ContentPresentation::PlainText
    } else {
        ContentPresentation::Other
    };
}
