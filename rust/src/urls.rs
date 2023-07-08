use log::{debug, warn};
use url::Url;

use crate::structures::TypeOfConnection::{Relative, TCP, UNIX};
use crate::structures::{TypeOfConnection, UnixCon};

pub fn parse_url_ext(s0: &str) -> Option<TypeOfConnection> {
    println!("parse_url_ext: {}", s0);

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

        Some(UNIX(UnixCon {
            scheme,
            socket_name,
            path: path.clone(),
            query,
        }))
    } else {
        match Url::parse(s0) {
            Ok(url) => Some(TCP(url)),
            Err(e) => None,
        }
    }
}

impl TypeOfConnection {
    pub fn join(&self, s: &str) -> Option<TypeOfConnection> {
        join_ext(&self, s)
    }
}
pub fn join_ext(conbase: &TypeOfConnection, s: &str) -> Option<TypeOfConnection> {
    if s.contains("://") {
        parse_url_ext(s)
    } else {
        match conbase.clone() {
            TCP(mut url) => {
                let path2 = join_path(url.path(), s);
                url.set_path(&path2);
                Some(TCP(url))
            }
            Relative(path) => {
                let path2 = join_path(path.as_str(), s);
                Some(Relative(path2))
            }
            UNIX(mut uc) => {
                let path2 = join_path(uc.path.as_str(), s);
                uc.path = path2;
                Some(UNIX(uc))
            }
        }
    }
}

fn join_path(base: &str, s: &str) -> String {
    if s.starts_with("/") {
        s.to_string()
    } else {
        format!("{}/{}", base, s)
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
