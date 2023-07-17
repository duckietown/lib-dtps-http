use log::info;
use url::Url;

use crate::structures::TypeOfConnection::{Relative, TCP, UNIX};
use crate::structures::{TypeOfConnection, UnixCon};
use crate::{normalize_path, DTPSError, FilePaths, DTPSR};

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

pub fn parse_url_ext(mut s0: &str) -> DTPSR<TypeOfConnection> {
    if s0.starts_with("file:") {
        s0 = &s0["file:".len()..];
        // collapse multiple slashes into one
        loop {
            let mut c = s0.chars();
            let first = c.next();
            let second = c.next();
            if first == Some('/') && second == Some('/') {
                s0 = &s0[1..];
            } else {
                break;
            }
        }
    }
    if s0.starts_with("/./") {
        let x = s0[3..].to_string();
        let x = normalize_path(&x);
        return Ok(TypeOfConnection::File(None, FilePaths::Relative(x)));
    }
    if s0.starts_with("/") {
        let x = normalize_path(s0);
        return Ok(TypeOfConnection::File(None, FilePaths::Absolute(x)));
    }
    if s0.starts_with("./") {
        let x = s0[2..].to_string();
        let x = normalize_path(&x);
        return Ok(TypeOfConnection::File(None, FilePaths::Relative(x)));
    }

    let scheme = get_scheme(s0);

    if let FoundScheme::None = scheme {
        let x = normalize_path(s0);

        return Ok(TypeOfConnection::File(None, FilePaths::Relative(x)));
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
            Ok(p) => {
                if p.scheme() == "file" {
                    eprintln!("parse raw: {p:?}");
                    Ok(TypeOfConnection::File(
                        p.host().map(|s| s.to_string()),
                        FilePaths::Absolute(p.path().to_string()),
                    ))
                } else {
                    Ok(TCP(p))
                }
            }
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
            let (path3, _query3) = join_path(a, path2.as_str());
            Ok(Relative(path3, query2.clone()))
        }

        TypeOfConnection::Same() => Ok(Relative(a.to_string(), None)),
        TypeOfConnection::File(hostname, path) => {
            // let (path3, query3) = join_path(a, path.add_prefix(a)as_str());
            Ok(TypeOfConnection::File(hostname.clone(), path.add_prefix(a)))
        }
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
            TypeOfConnection::File(hostname, path) => {
                Ok(TypeOfConnection::File(hostname, path.join(s)))
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

    // Bring the function into scope

    use log::debug;

    use crate::FilePaths;

    use super::*;

    #[test]
    fn normalize1() {
        assert_eq!(normalize_path("/the/path"), "/the/path");
        assert_eq!(normalize_path("/the/path/"), "/the/path");
    }

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

    #[test]
    fn url_parse_3() {
        // without end /
        let s = "file:///abs/file";
        //         file://localhost/etc/fstab
        // file:///etc/fstab
        let x = super::parse_url_ext(s).unwrap();
        debug!("test_p1 {s:?} {x:?}");
        assert_eq!(
            x,
            super::TypeOfConnection::File(None, FilePaths::Absolute("/abs/file".to_string()))
        );
        // let x = parse_url_ext("/the/path?ade").unwrap();
        // warn!("test_add_two {:?}", x);
    }

    #[test]
    fn url_parse_4() {
        // without end /
        let s = "./reldir";
        //         file://localhost/etc/fstab
        // file:///etc/fstab
        let x = super::parse_url_ext(s).unwrap();
        debug!("test_p1 {s:?} {x:?}");
        assert_eq!(
            x,
            super::TypeOfConnection::File(None, FilePaths::Relative("reldir".to_string()))
        );
        // let x = parse_url_ext("/the/path?ade").unwrap();
        // warn!("test_add_two {:?}", x);
    }
    #[test]
    fn url_parse_4c() {
        let s = "reldir";
        let x = super::parse_url_ext(s).unwrap();
        assert_eq!(
            x,
            super::TypeOfConnection::File(None, FilePaths::Relative("reldir".to_string()))
        );
        // let x = parse_url_ext("/the/path?ade").unwrap();
        // warn!("test_add_two {:?}", x);
    }

    #[test]
    fn url_parse_4b() {
        // without end /
        let s = "./reldir/";
        //         file://localhost/etc/fstab
        // file:///etc/fstab
        let x = super::parse_url_ext(s).unwrap();
        debug!("test_p1 {s:?} {x:?}");
        assert_eq!(
            x,
            super::TypeOfConnection::File(None, FilePaths::Relative("reldir".to_string()))
        );
        // let x = parse_url_ext("/the/path?ade").unwrap();
        // warn!("test_add_two {:?}", x);
    }

    #[test]
    fn url_parse_5() {
        // without end /
        let s = "/abs/dir/";
        //         file://localhost/etc/fstab
        // file:///etc/fstab
        let x = super::parse_url_ext(s).unwrap();
        assert_eq!(
            x,
            super::TypeOfConnection::File(None, FilePaths::Absolute("/abs/dir".to_string()))
        );
        // let x = parse_url_ext("/the/path?ade").unwrap();
        // warn!("test_add_two {:?}", x);
    }

    #[test]
    fn url_parse_6() {
        // without end /
        let s = "/abs/dir/";
        let x = super::parse_url_ext(s).unwrap();
        assert_eq!(
            x,
            super::TypeOfConnection::File(None, FilePaths::Absolute("/abs/dir".to_string()))
        );
    }

    #[test]
    fn url_parse_7() {
        let s = "file:/abs/dir/";
        let x = super::parse_url_ext(s).unwrap();
        assert_eq!(
            x,
            super::TypeOfConnection::File(None, FilePaths::Absolute("/abs/dir".to_string()))
        );
    }
    #[test]
    fn url_parse_8() {
        let s = "file://abs/dir/";
        let x = super::parse_url_ext(s).unwrap();
        assert_eq!(
            x,
            super::TypeOfConnection::File(None, FilePaths::Absolute("/abs/dir".to_string()))
        );
    }
    #[test]
    fn url_parse_9() {
        let s = "file:///abs/dir/";
        let x = super::parse_url_ext(s).unwrap();
        assert_eq!(
            x,
            super::TypeOfConnection::File(None, FilePaths::Absolute("/abs/dir".to_string()))
        );
    }
    #[test]
    fn url_parse_10() {
        let s = "file://./reldir/";
        let x = super::parse_url_ext(s).unwrap();
        assert_eq!(
            x,
            super::TypeOfConnection::File(None, FilePaths::Relative("reldir".to_string()))
        );
    }
}

pub fn format_digest_path(digest: &str, content_type: &str) -> String {
    return format!("!/:ipfs/{}/{}/", digest, content_type.replace("/", "_"));
}
