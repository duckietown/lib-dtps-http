pub fn escape_json_patch(s: &str) -> String {
    s.replace('~', "~0").replace('/', "~1")
}

pub fn unescape_json_patch(s: &str) -> String {
    s.replace("~1", "/").replace("~0", "~")
}
