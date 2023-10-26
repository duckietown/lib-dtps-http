use std::{
    collections::HashMap,
    time::SystemTime,
};

pub fn divide_in_components(pstr: &str, sep: char) -> Vec<String> {
    let mut pstr = pstr.to_string();
    pstr = pstr.trim_start_matches(sep).to_string();
    pstr = pstr.trim_end_matches(sep).to_string();

    let path_components: Vec<String> = {
        if pstr.is_empty() {
            vec![]
        } else {
            pstr.as_str().split(sep).map(|x| x.to_string()).collect::<Vec<String>>()
        }
    };
    path_components
}

pub fn get_good_url_for_components(components: &Vec<String>) -> String {
    let mut url = String::new();
    url.push('/');
    for c in components {
        url.push_str(c);

        url.push('/');
    }
    url
}

pub fn vec_concat<T: Clone>(a: &[T], b: &[T]) -> Vec<T> {
    let mut c: Vec<T> = Vec::new();
    c.extend(a.iter().cloned());
    c.extend(b.iter().cloned());
    c
}

pub fn is_prefix_of<'a>(a: &'a Vec<String>, b: &'a Vec<String>) -> Option<(Vec<String>, Vec<String>)> {
    if a.len() > b.len() {
        return None;
    }
    for i in 0..a.len() {
        if a[i] != b[i] {
            return None;
        }
    }
    let matched: Vec<String> = b[0..a.len()].to_vec();
    let extra_elements: Vec<String> = b[a.len()..].to_vec();
    Some((matched, extra_elements))
}

pub fn epoch() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

pub fn format_nanos(n: i64) -> String {
    let ms = (n as f64) / 1_000_000.0;
    format!("{:.3}ms", ms)
}

pub fn time_nanos() -> u128 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

pub fn time_nanos_i64() -> i64 {
    time_nanos() as i64
}

pub fn format_query(q: &HashMap<String, String>) -> String {
    if q.is_empty() {
        return "".to_string();
    }

    let mut res = String::from("?");
    for (k, v) in q {
        res.push_str(k);
        res.push('=');
        res.push_str(v);
        res.push('&');
    }
    res
}
