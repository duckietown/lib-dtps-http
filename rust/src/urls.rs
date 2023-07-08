use std::error;

use log::{debug, warn};
use url::{ParseError, Url};
use warp::query;

use crate::structures::TypeOfConnection::{Relative, TCP, UNIX};
use crate::structures::{TypeOfConnection, UnixCon};

pub fn parse_url_ext(s0: &str) -> Result<TypeOfConnection, Box<dyn error::Error>> {
    if s0.starts_with("http+unix://") {
        // divide in host and path at the / after the http+unix://
        // let mut parts = s.splitn(2, '/');
        // let host = parts.next().unwrap();
        // let path = parts.next().unwrap();
        let (query, s) = match s0.find("?") {
            Some(i) => (Some(s0[i..].to_string()), s0[..i].to_string()),
            None => (None, s0.to_string()),
        };
        let scheme_end = s.find("://").unwrap();
        let host_start = scheme_end + "://".len();
        let path_start = s[host_start..].find('/').unwrap() + host_start;

        let scheme = s[..scheme_end].to_string().clone();
        let host = &s[host_start..path_start];
        let path = (&s[path_start..].to_string()).clone();

        let socket_name = host.to_string().replace("%2F", "/");

        Ok(UNIX(UnixCon {
            scheme,
            socket_name,
            path: path.clone(),
            query,
        }))
    } else {
        match Url::parse(s0) {
            Ok(p) => Ok(TCP(p)),
            Err(e) => Err(Box::new(e)),
        }
    }
}

impl TypeOfConnection {
    pub fn join(&self, s: &str) -> Result<TypeOfConnection, Box<dyn error::Error>> {
        join_ext(&self, s)
    }
}

pub fn join_ext(
    conbase: &TypeOfConnection,
    s: &str,
) -> Result<TypeOfConnection, Box<dyn error::Error>> {
    if s.contains("://") {
        parse_url_ext(s)
    } else {
        match conbase.clone() {
            TCP(mut url) => {
                let (path2, query2) = join_path(url.path(), s);
                url.set_path(&path2);
                url.set_query(query2.as_deref());
                Ok(TCP(url))
            }
            Relative(path, _query) => {
                let (path2, query2) = join_path(path.as_str(), s);
                Ok(Relative(path2, query2))
            }
            UNIX(mut uc) => {
                let (path2, query2) = join_path(uc.path.as_str(), s);
                uc.path = path2;
                uc.query = query2;
                Ok(UNIX(uc))
            }
        }
    }
}

fn join_path(base: &str, s: &str) -> (String, Option<String>) {
    // split the query
    let (query, s) = match s.find("?") {
        Some(i) => (Some(s[i + 1..].to_string()), s[..i].to_string()),
        None => (None, s.to_string()),
    };
    if s.starts_with("/") {
        (s.to_string(), query)
    } else {
        if base.ends_with("/") {
            (format!("{}{}", base, s), query)
        } else {
            (format!("{}/{}", base, s), query)
        }
    }
}

#[cfg(test)]
mod tests {
    use log::warn;
    use url::Url;

    // Bring the function into scope
    use super::parse_url_ext;

    #[test]
    fn test_add_two() {
        // let x = Url::parse("/the/path?ade").unwrap();
        // let x = parse_url_ext("/the/path?ade").unwrap();
        // warn!("test_add_two {:?}", x);
    }
}
