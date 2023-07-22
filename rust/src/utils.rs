use std::time::SystemTime;

pub fn divide_in_components(pstr: &str, sep: char) -> Vec<String> {
    let mut pstr = pstr.to_string().clone();
    pstr = pstr.trim_start_matches(sep).to_string();
    pstr = pstr.trim_end_matches(sep).to_string();

    let path_components: Vec<String> = {
        if pstr == "" {
            vec![]
        } else {
            pstr.as_str()
                .split(sep)
                .map(|x| x.to_string())
                .collect::<Vec<String>>()
        }
    };
    path_components
}

pub fn get_good_url_for_components(components: &Vec<String>) -> String {
    let mut url = String::new();
    url.push('/');
    for c in components {
        url.push_str(c);

        url.push_str("/");
    }
    url
}

pub fn vec_concat<T: Clone>(a: &Vec<T>, b: &Vec<T>) -> Vec<T> {
    let mut c = a.clone();
    c.extend(b.iter().cloned());
    c
}

pub fn is_prefix_of<'a>(
    a: &'a Vec<String>,
    b: &'a Vec<String>,
) -> Option<(Vec<String>, Vec<String>)> {
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
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
