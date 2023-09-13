use crate::{
    CONTENT_TYPE_CBOR,
    CONTENT_TYPE_JSON,
    CONTENT_TYPE_PLAIN,
    CONTENT_TYPE_YAML,
};

pub fn is_html(content_type: &str) -> bool {
    content_type.starts_with("text/html")
}

pub fn is_image(content_type: &str) -> bool {
    content_type.starts_with("image/")
}

pub fn identify_content_presentation(content_type: &str) -> Option<&'static str> {
    if content_type.ends_with("cbor") {
        Some(CONTENT_TYPE_CBOR)
    } else if content_type.ends_with("json") {
        Some(CONTENT_TYPE_JSON)
    } else if content_type.ends_with("yaml") {
        Some(CONTENT_TYPE_YAML)
    } else if content_type.starts_with("text/") {
        Some(CONTENT_TYPE_PLAIN)
    } else {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ContentPresentation {
    CBOR,
    JSON,
    YAML,
    PlainText,
    Other,
}

pub fn identify_presentation(content_type: &str) -> ContentPresentation {
    if content_type.ends_with("cbor") {
        ContentPresentation::CBOR
    } else if content_type.ends_with("json") {
        ContentPresentation::JSON
    } else if content_type.ends_with("yaml") {
        ContentPresentation::YAML
    } else if content_type.starts_with("text/") {
        ContentPresentation::PlainText
    } else {
        ContentPresentation::Other
    }
}
