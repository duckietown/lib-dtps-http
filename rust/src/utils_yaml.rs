use maud::{html, Markup, Render};

pub fn generate_html_tree(input: &serde_yaml::Value) -> Markup {
    match input {
        serde_yaml::Value::String(s) => html! { (s) },
        serde_yaml::Value::Number(n) => html! { (n.to_string()) },
        serde_yaml::Value::Bool(b) => html! { (b) },
        serde_yaml::Value::Null => html! { code {"null"} },
        serde_yaml::Value::Sequence(seq) => {
            html! {
                // details {
                //     summary { "Array" }
                    ol {
                        @for value in seq {
                            li {
                                (generate_html_tree(value))
                            }
                        }
                    }
                // }
            }
        }
        serde_yaml::Value::Mapping(map) => {
            html! {
                details {
                    summary { "Object" }
                    ul {
                        @for (key, value) in map {
                            li {
                                details {
                                    summary { b { (key.as_str().unwrap_or("")) } }
                                    (generate_html_tree(value))
                                }
                            }
                        }
                    }
                }
            }
        }
        serde_yaml::Value::Tagged(tagged) => {
            html! {
                dl {
                    dt { (tagged.tag)}
                    dd { (generate_html_tree(&tagged.value))}
                }
            }
        }
    }
}

// pub fn cbor_from_json(input: &serde_json::Value) -> serde_cbor::Value {
//
// }
// pub fn cbor_from_yaml(input: &serde_json::Value) -> serde_cbor::Value {
//
// }

pub fn generate_html_from_cbor(input: &serde_cbor::Value, max_level_open: i32) -> Markup {
    match input {
        serde_cbor::Value::Text(s) => html! { code {"\""(s)"\"" }},
        serde_cbor::Value::Integer(n) => html! { (n.to_string()) },
        serde_cbor::Value::Bool(b) => html! { (b) },
        serde_cbor::Value::Null => html! { "null" },
        serde_cbor::Value::Array(arr) => {
            if arr.len() == 0 {
                html! { code{"[]"}}
            } else {
                html! {
                    // details {
                    //     summary { "Array" }
                        ol {
                            @for value in arr {
                                li {
                                    (generate_html_from_cbor(value,
                                    max_level_open-1))
                                }
                            }
                        }
                    // }
                }
            }
        }
        serde_cbor::Value::Map(map) => {
            if map.len() == 0 {
                html! { code{"{}"}}
            } else {
                let mut stuff = vec![];

                for (key, value) in map {
                    let k = generate_html_from_cbor(key, max_level_open - 1);
                    let v = generate_html_from_cbor(value, max_level_open - 1);

                    let is_long = v.render().into_string().len() > 120;
                    let x = if !is_long {
                        html! {
                            tr {
                                td {(k)}
                                td {(v)}
                            }

                        }
                    } else {
                        if max_level_open > 0 {
                            html! {
                                tr {
                                    td { (k)}
                                    td { (v) }
                                }

                            }
                        } else {
                            html! {
                                tr {
                                td {(k)}
                                td {details   { summary {} {(v)} }}
                                }
                            }
                        }
                    };
                    stuff.push(x);
                }

                html! {
                    table  class="Map"{
                   @for item in stuff {
                        (item)
                    }
                    }
                }
            }
        }

        _ => html! { "" }, // for other types of values
    }
}
