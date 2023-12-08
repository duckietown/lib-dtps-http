use maud::{html, Markup, Render};
use serde_cbor::Value;

//
// pub fn generate_html_tree(input: &serde_yaml::Value) -> Markup {
//     match input {
//         serde_yaml::Value::String(s) => html! { (s) },
//         serde_yaml::Value::Number(n) => html! { (n.to_string()) },
//         serde_yaml::Value::Bool(b) => html! { (b) },
//         serde_yaml::Value::Null => html! { code {"null"} },
//         serde_yaml::Value::Sequence(seq) => {
//             html! {
//                 // details {
//                 //     summary { "Array" }
//                     ol {
//                         @for value in seq {
//                             li {
//                                 (generate_html_tree(value))
//                             }
//                         }
//                     }
//                 // }
//             }
//         }
//         serde_yaml::Value::Mapping(map) => {
//             html! {
//                 details {
//                     summary { "Object" }
//                     ul {
//                         @for (key, value) in map {
//                             li {
//                                 details {
//                                     summary { b { (key.as_str().unwrap_or("")) } }
//                                     (generate_html_tree(value))
//                                 }
//                             }
//                         }
//                     }
//                 }
//             }
//         }
//         serde_yaml::Value::Tagged(tagged) => {
//             html! {
//                 dl {
//                     dt { (tagged.tag)}
//                     dd { (generate_html_tree(&tagged.value))}
//                 }
//             }
//         }
//     }
// }

pub fn generate_html_from_cbor(input: &Value, max_level_open: i32) -> Markup {
    match input {
        Value::Text(s) => html! { code {"\""(s)"\"" }},
        Value::Integer(n) => html! { (n.to_string()) },
        Value::Bool(b) => html! { (b) },
        Value::Null => html! { "null" },
        Value::Array(arr) => {
            if arr.is_empty() {
                html! { code{"[]"}}
            } else {
                let mut stuff = vec![];
                for value in arr {
                    stuff.push(generate_html_from_cbor(value, max_level_open - 1));
                }

                html! {

                    table class="array" {
                            @for item in stuff {
                                tr {
                                    td {(item)}
                                }
                            }

                   }
                }
            }
        }
        Value::Map(map) => {
            if map.is_empty() {
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
                    } else if max_level_open > 0 {
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

        Value::Float(f) => {
            html! { (f.to_string())}
        }
        Value::Bytes(b) => {
            html! { (format!("{b:?}")) }
        }
        Value::Tag(_tag_no, val) => generate_html_from_cbor(val, max_level_open - 1),
        Value::__Hidden => {
            html! {}
        }
    }
}
