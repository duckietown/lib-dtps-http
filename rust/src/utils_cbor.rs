use std::collections::BTreeMap;

use anyhow::Context;

use futures::StreamExt;

use serde_cbor::Value::{Null as CBORNull, Text as CBORText};

use crate::{
    context, debug_with_info, dtpserror_other, DTPSError, ResolvedData,
    ResolvedData::{NotAvailableYet, NotFound, Regular},
    DTPSR,
};

pub fn get_result_to_put(
    result_dict: &mut serde_cbor::value::Value,
    prefix: Vec<String>,
) -> &mut serde_cbor::value::Value {
    let mut current: &mut serde_cbor::value::Value = result_dict;
    for component in &prefix[..prefix.len() - 1] {
        if let serde_cbor::value::Value::Map(inside) = current {
            let the_key = CBORText(component.clone());
            if !inside.contains_key(&the_key) {
                inside.insert(the_key.clone(), serde_cbor::value::Value::Map(BTreeMap::new()));
            }
            current = inside.get_mut(&the_key).unwrap();
        } else {
            panic!("not a map");
        }
    }
    current
}

pub fn putinside(result_dict: &mut serde_cbor::value::Value, prefix: &Vec<String>, what: ResolvedData) -> DTPSR<()> {
    let mut the_result_to_put = get_result_to_put(result_dict, prefix.clone());

    let where_to_put = if let serde_cbor::value::Value::Map(where_to_put) = &mut the_result_to_put {
        where_to_put
    } else {
        panic!("not a map");
    };

    let key_to_put = CBORText(prefix.last().unwrap().clone());
    let key_to_put2 = CBORText(format!("{}?", prefix.last().unwrap()));

    match what {
        Regular(x) => {
            where_to_put.insert(key_to_put, x);
        }
        NotAvailableYet(x) => {
            // TODO: do more here
            where_to_put.insert(key_to_put, CBORNull);
            where_to_put.insert(key_to_put2, CBORText(x));
        }
        NotFound(_) => {}
        ResolvedData::RawData(rd) => {
            let prefix_str = prefix.join("/");
            let x = context!(
                rd.get_as_cbor(),
                "Cannot get data as cbor for component {prefix_str:#?}\n{rd:#?}"
            )?;
            where_to_put.insert(key_to_put, x);
        }
    }
    Ok(())
}

pub fn get_inside(context: Vec<String>, data: &serde_cbor::Value, path: &Vec<String>) -> DTPSR<serde_cbor::Value> {
    debug_with_info!("get_inside: context: {:?}, data: {:?}, path: {:?}", context, data, path);
    let current = data;
    if path.is_empty() {
        return Ok(current.clone());
    }
    let context_s = format!("Context: {:?}\n Path: {:?}\nCurrent: {:?}\n", context, path, current);
    let mut new_context = context.clone();
    let mut path = path.clone();
    let first = path.remove(0);
    let inside = match current {
        serde_cbor::value::Value::Array(a) => {
            let p: usize = match first.parse() {
                Ok(x) => x,
                Err(e) => {
                    return dtpserror_other!("{}Cannot parse {:?} as usize: {}\n", context_s, first, e);
                }
            };

            if p >= a.len() {
                return dtpserror_other!(
                    "{}Cannot find index {} for array of length {}\n",
                    context_s,
                    first,
                    a.len()
                );
                // return DTPSError::other(s);
            } else {
                new_context.push(format!("[{}]", p));
                a.get(p).unwrap()
            }
        }
        serde_cbor::value::Value::Map(a) => {
            // try to parse the string first as a integer
            let key = match first.parse::<i128>() {
                Ok(n) => serde_cbor::value::Value::Integer(n),
                Err(_) => serde_cbor::value::Value::Text(first.clone()),
            };
            match a.get(&key) {
                None => {
                    let available = a.keys().map(|x| format!("{:?}", x)).collect::<Vec<String>>().join(", ");
                    return dtpserror_other!(
                        "{}Cannot find key {} for map.\nAvailable: {}",
                        context_s,
                        first,
                        available
                    );
                }
                Some(v) => {
                    new_context.push(format!(".{}", first));
                    v
                }
            }
        }
        _ => {
            let context_s = context.join(" -> ");
            return dtpserror_other!("{context_s}: Cannot get inside this: {data:?}");
        }
    };
    get_inside(new_context, inside, &path)
}
