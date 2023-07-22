use crate::{utils_mime, DTPSError, RawData, DTPSR};
use log::debug;

pub fn get_as_cbor(data: &RawData) -> DTPSR<serde_cbor::Value> {
    let cbor_data = match data.content_type.as_str() {
        "application/cbor" => {
            let cbor_data: serde_cbor::Value = serde_cbor::from_slice(&data.content).unwrap();
            cbor_data
        }
        "application/json" => {
            let json_data: serde_json::Value = serde_json::from_slice(&data.content).unwrap();
            // let cbor_data = serde_cbor::from_value(json_data).unwrap();
            // cbor_data
            let c = serde_json::from_value(json_data).unwrap();
            c
        }
        "application/yaml" | "text/x-yaml" => {
            let json_data: serde_yaml::Value = serde_yaml::from_slice(&data.content).unwrap();
            // let cbor_data = serde_cbor::from_value(json_data).unwrap();
            // cbor_data
            let c = serde_yaml::from_value(json_data).unwrap();
            c
        }
        _ => {
            let s = format!("Cannot parse content type {}", data.content_type);
            return DTPSError::other(s);
        }
    };
    Ok(cbor_data)
}

pub fn get_inside(
    context: Vec<String>,
    data: &serde_cbor::Value,
    path: &Vec<String>,
) -> DTPSR<serde_cbor::Value> {
    debug!(
        "get_inside: context: {:?}, data: {:?}, path: {:?}",
        context, data, path
    );
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
                    let available = a
                        .keys()
                        .map(|x| format!("{:?}", x))
                        .collect::<Vec<String>>()
                        .join(", ");
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

pub fn display_printable(content_type: &str, content: &[u8]) -> String {
    let identified = utils_mime::identify_content_presentation(content_type);
    if identified.is_none() {
        return format!("Cannot display content type {}", content_type).to_string();
    }

    match identified.unwrap() {
        "application/yaml" => {
            let bytes: Vec<u8> = content.to_vec();
            String::from_utf8(bytes).unwrap().to_string()
        }
        "text/plain" => {
            let bytes: Vec<u8> = content.to_vec();

            String::from_utf8(bytes).unwrap().to_string()
        }
        "application/json" => {
            let val: serde_json::Value = serde_json::from_slice(&content).unwrap();
            let pretty = serde_json::to_string_pretty(&val).unwrap();
            pretty
        }
        "application/cbor" => {
            let val: serde_cbor::Value = serde_cbor::from_slice(&content).unwrap();
            match serde_yaml::to_string(&val) {
                Ok(x) => format!("CBOR displayed as YAML:\n\n{}", x),
                Err(e) => format!("Cannot format CBOR as YAML: {}\nRaw CBOR:\n{:?}", e, val),
            }
        }
        _ => format!("Cannot display content type {}", content_type).to_string(),
    }
}
