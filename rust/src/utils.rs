use std::collections::HashMap;

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

/// Determines if the given string slice represents a truthy or falsy value.
///
/// # Arguments
///
/// * `input` - A string slice that holds the value to be evaluated.
///
/// # Returns
///
/// * `Some(true)` if the value is truthy (e.g., "true", "True", "1", "yes").
/// * `Some(false)` if the value is falsy (e.g., "false", "False", "0", "no").
/// * `None` if the value does not match any truthy or falsy representation.
///
/// # Examples
///
/// ```
/// // You can use the function in this manner:
/// use dtps_http::utils::is_truthy;
/// let truthy_str = "True";
/// let result = is_truthy(truthy_str);
/// assert_eq!(result, Some(true));
///
/// let non_truthy_str = "not true or false";
/// let result = is_truthy(non_truthy_str);
/// assert_eq!(result, None);
/// ```
pub fn is_truthy(input: &str) -> Option<bool> {
    // Lowercase the input to make comparison case-insensitive
    match input.to_lowercase().as_str() {
        "true" | "1" | "yes" | "on" | "t" => Some(true),
        "false" | "0" | "no" | "off" | "f" | "" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_truthy() {
        assert_eq!(is_truthy("True"), Some(true));
        assert_eq!(is_truthy("false"), Some(false));
        assert_eq!(is_truthy(""), Some(false));
        assert_eq!(is_truthy("not sure"), None);
    }
}
