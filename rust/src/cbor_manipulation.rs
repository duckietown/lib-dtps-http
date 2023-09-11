use maud::{
    html,
    PreEscaped,
};

use crate::{
    debug_with_info,
    identify_presentation,
    utils_mime,
    utils_yaml::generate_html_from_cbor,
    ContentPresentation,
    DTPSError,
    RawData,
    DTPSR,
};

impl RawData {
    pub fn get_as_cbor(self: &RawData) -> DTPSR<serde_cbor::Value> {
        let r: serde_cbor::Value = match identify_presentation(&self.content_type) {
            ContentPresentation::CBOR => serde_cbor::from_slice(&self.content)?,
            ContentPresentation::JSON => {
                let json_data: serde_json::Value = serde_json::from_slice(&self.content)?;
                serde_json::from_value(json_data)?
            }
            ContentPresentation::YAML => {
                let yaml_data: serde_yaml::Value = serde_yaml::from_slice(&self.content)?;
                serde_yaml::from_value(yaml_data)?
            }
            ContentPresentation::PlainText => {
                let s = format!("get_as_cbor: Cannot convert {} to cbor", self.content_type);
                return DTPSError::other(s);
            }
            _ => {
                let s = format!("get_as_cbor: Cannot parse content type {}", self.content_type);
                return DTPSError::other(s);
            }
        };
        Ok(r)
    }
    pub fn get_as_json(self: &RawData) -> DTPSR<serde_json::Value> {
        let r: serde_json::Value = match identify_presentation(&self.content_type) {
            ContentPresentation::CBOR => {
                let cbor_data: serde_cbor::Value = serde_cbor::from_slice(&self.content)?;
                serde_cbor::value::from_value(cbor_data)?
            }
            ContentPresentation::JSON => serde_json::from_slice(&self.content)?,
            ContentPresentation::YAML => {
                let yaml_data: serde_yaml::Value = serde_yaml::from_slice(&self.content)?;
                serde_yaml::from_value(yaml_data)?
            }
            ContentPresentation::PlainText => {
                let s = format!("get_as_json: Cannot convert {} to json", self.content_type);
                return DTPSError::other(s);
            }
            _ => {
                let s = format!("get_as_json: Cannot parse content type {}", self.content_type);
                return DTPSError::other(s);
            }
        };
        Ok(r)
    }

    pub fn encode_from_json(json_data: &serde_json::Value, target_content_type: &str) -> DTPSR<Self> {
        let bytes = match identify_presentation(target_content_type) {
            ContentPresentation::CBOR => serde_cbor::to_vec(&json_data)?,
            ContentPresentation::JSON => serde_json::to_vec(&json_data)?,
            ContentPresentation::YAML => serde_yaml::to_string(&json_data)?.as_bytes().to_vec(),
            ContentPresentation::PlainText | ContentPresentation::Other => {
                let s = format!("Cannot convert json to {target_content_type}");
                return DTPSError::other(s);
            }
        };
        Ok(RawData::new(bytes, target_content_type))
    }
}

pub fn get_inside(context: Vec<String>, data: &serde_cbor::Value, path: &Vec<String>) -> DTPSR<serde_cbor::Value> {
    debug_with_info!("get_inside: context: {:?}, data: {:?}, path: {:?}", context, data, path);
    let current = data;
    if path.len() == 0 {
        return Ok(current.clone());
    }
    let context_s = format!("Context: {}\n", context.join(""));
    let mut new_context = context.clone();
    let mut path = path.clone();
    let first = path.remove(0);
    let inside = match current {
        serde_cbor::value::Value::Array(a) => {
            let p: i64 = match first.parse() {
                Ok(x) => x,
                Err(e) => {
                    let s = format!("{}Cannot parse {} as usize: {}", context_s, first, e);
                    return DTPSError::other(s);
                }
            };

            if !(0 <= p && p < a.len() as i64) {
                let s = format!(
                    "{}Cannot find index {} for array of length {}",
                    context_s,
                    first,
                    a.len()
                );
                return DTPSError::other(s);
            } else {
                let p = p as usize;
                new_context.push(format!("[{}]", p));
                a.get(p).unwrap()
            }
        }
        serde_cbor::value::Value::Map(a) => {
            let key = serde_cbor::value::Value::Text(first.clone().into());
            match a.get(&key) {
                None => {
                    let available = a.keys().map(|x| format!("{:?}", x)).collect::<Vec<String>>().join(", ");
                    let s = format!(
                        "{}Cannot find key {} for map. Available: {}",
                        context_s, first, available
                    );

                    return DTPSError::other(s);
                }
                Some(v) => {
                    new_context.push(format!(".{}", first));
                    v
                }
            }
        }
        _ => {
            let context_s = context.join(" -> ");
            let s = format!("{context_s}: Cannot get inside this: {data:?}");
            return DTPSError::other(s);
        }
    };
    return get_inside(new_context, &inside, &path);
}

pub fn display_printable(content_type: &str, content: &[u8]) -> PreEscaped<String> {
    let identified = utils_mime::identify_presentation(content_type);

    match identified {
        ContentPresentation::YAML => {
            let bytes: Vec<u8> = content.to_vec();
            let s = String::from_utf8(bytes).unwrap().to_string();
            html! {
                pre {
                    code { (s)}
                }
            }
        }
        ContentPresentation::PlainText => {
            let bytes: Vec<u8> = content.to_vec();
            let s = String::from_utf8(bytes).unwrap().to_string();
            html! {
                pre {
                    code {
                        (s)
                    }
                }
            }
        }
        ContentPresentation::JSON => {
            let val: serde_json::Value = serde_json::from_slice(&content).unwrap();

            let pretty = serde_json::to_string_pretty(&val).unwrap();
            html! {
                pre {
                    code {
                        (pretty)
                    }
                }
            }
        }
        ContentPresentation::CBOR => {
            let val: serde_cbor::Value = serde_cbor::from_slice(&content).unwrap();

            generate_html_from_cbor(&val, 3)
            // match serde_yaml::to_string(&val) {
            //     Ok(x) => {
            //         html! { div { "CBOR displayed as YAML:"  (x) } }
            //
            //
            //     },
            //     Err(e) =>html!{
            //         "Cannot format CBOR as YAML"
            //     }
            //     // format!("Cannot format CBOR as YAML: {}\nRaw CBOR:\n{:?}", e, val),
            // }
        }
        _ => html! {
            "Cannot display content type " (content_type)
        },
    }
}
