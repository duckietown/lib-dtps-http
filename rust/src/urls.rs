use log::info;
use url::Url;

use crate::structures::TypeOfConnection::{Relative, TCP, UNIX};
use crate::structures::{TypeOfConnection, UnixCon};
use crate::{DTPSError, DTPSR};

fn get_scheme_part(s0: &str) -> Option<&str> {
    let scheme_end = s0.find("://")?;
    Some(&s0[..scheme_end])
}

fn is_unix_scheme(s0: &str) -> bool {
    s0 == "unix" || s0 == "http+unix"
}

enum FoundScheme {
    Unix(String),
    Other(String),
    None,
}

fn get_scheme(s: &str) -> FoundScheme {
    let scheme = get_scheme_part(s);
    match scheme {
        Some(scheme) => {
            if is_unix_scheme(scheme) {
                FoundScheme::Unix(scheme.to_string())
            } else {
                FoundScheme::Other(scheme.to_string())
            }
        }
        None => FoundScheme::None,
    }
}

const SPECIAL_CHAR: &str = "%2F";

pub fn parse_url_ext(s0: &str) -> DTPSR<TypeOfConnection> {
    let scheme = get_scheme(s0);

    if let FoundScheme::None = scheme {
        return Ok(UNIX(UnixCon {
            scheme: "http+unix".to_string(),
            socket_name: s0.to_string(),
            path: "/".to_string(),
            query: None,
        }));
    }
    if let FoundScheme::Unix(_) = scheme {
        let (query, s) = match s0.find("?") {
            Some(i) => (Some(s0[i..].to_string()), s0[..i].to_string()),
            None => (None, s0.to_string()),
        };
        let scheme_end = s.find("://").unwrap();
        let scheme = s[..scheme_end].to_string().clone();

        let host_start = scheme_end + "://".len();
        if s0.contains(SPECIAL_CHAR) {
            let (socket_name, path) = match s[host_start..].find('/') {
                None => {
                    let path = "/".to_string();
                    let host = s[host_start..].to_string().clone();
                    let host = host.replace(SPECIAL_CHAR, "/");

                    (host, path)
                }
                Some(path_start0) => {
                    let path_start = path_start0 + host_start;
                    let path = (s[path_start..].to_string()).clone();

                    let host = s[host_start..path_start].to_string().clone();

                    let host = host.replace(SPECIAL_CHAR, "/");
                    (host, path)
                }
            };
            // let path_start = s[host_start..].find('/').unwrap() + host_start;
            let con = UnixCon {
                scheme,
                socket_name,
                path,
                query,
            };

            info!("UnixCon: parsed {:?} as {:?}", s, con);
            Ok(UNIX(con))
        } else {
            let path_start = s[host_start..].rfind('/').unwrap() + host_start;
            let socket_name = &s[host_start..path_start];
            let path = (&s[path_start..].to_string()).clone();
            Ok(UNIX(UnixCon {
                scheme,
                socket_name: socket_name.to_string(),
                path: path.clone(),
                query,
            }))
        }
    } else {
        match Url::parse(s0) {
            Ok(p) => Ok(TCP(p)),
            Err(e) => DTPSR::Err(DTPSError::Other(format!("Could not parse url: {}", e))),
        }
    }
}

impl TypeOfConnection {
    pub fn join(&self, s: &str) -> DTPSR<TypeOfConnection> {
        join_ext(&self, s)
    }
}

pub fn join_con(a: &str, b: &TypeOfConnection) -> DTPSR<TypeOfConnection> {
    match b {
        TCP(_) => Ok(b.clone()),
        UNIX(_) => Ok(b.clone()),
        Relative(path2, query2) => {
            let (path3, query3) = join_path(a, path2.as_str());
            Ok(Relative(path3, query2.clone()))
        }

        TypeOfConnection::Same() => Ok(Relative(a.to_string(), None)),
    }
}

pub fn join_ext(conbase: &TypeOfConnection, s: &str) -> DTPSR<TypeOfConnection> {
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

            TypeOfConnection::Same() => Ok(Relative(s.to_string(), None)),
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

    // Bring the function into scope

    use log::debug;

    #[test]
    fn url_parse_p1() {
        let s = "http+unix://%2Fsockets%2Fargo%2Fnode1%2F_node/";
        let x = super::parse_url_ext(s).unwrap();
        eprintln!("test_p1 {s:?} -> {x:?}");
        assert_eq!(
            x,
            super::TypeOfConnection::UNIX(super::UnixCon {
                scheme: "http+unix".to_string(),
                socket_name: "/sockets/argo/node1/_node".to_string(),
                path: "/".to_string(),
                query: None,
            })
        );
        // let x = parse_url_ext("/the/path?ade").unwrap();
        // warn!("test_add_two {:?}", x);
    }
    #[test]
    fn url_parse_p2() {
        // without end /
        let s = "http+unix://%2Fsockets%2Fargo%2Fnode1%2F_node";
        let x = super::parse_url_ext(s).unwrap();
        debug!("test_p1 {s:?} {x:?}");
        assert_eq!(
            x,
            super::TypeOfConnection::UNIX(super::UnixCon {
                scheme: "http+unix".to_string(),
                socket_name: "/sockets/argo/node1/_node".to_string(),
                path: "/".to_string(),
                query: None,
            })
        );
        // let x = parse_url_ext("/the/path?ade").unwrap();
        // warn!("test_add_two {:?}", x);
    }
}

pub fn format_digest_path(digest: &str, content_type: &str) -> String {
    return format!("!/:ipfs/{}/{}/", digest, content_type.replace("/", "_"));
}
