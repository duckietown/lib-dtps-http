use url::Url;

use crate::{
    info_with_info, normalize_path, DTPSError, FilePaths, TypeOfConnection,
    TypeOfConnection::{TCP, UNIX},
    UnixCon, DTPSR,
};

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

        let host_start0 = scheme_end + "://".len();
        let after_scheme = &s[host_start0..];

        if after_scheme.starts_with('[') {
            let end = after_scheme.find(']').unwrap();
            let socket_name = &after_scheme[1..end].to_string();
            let mut path = &after_scheme[end + 1..];
            if path.len() == 0 {
                path = "/";
            }
            let socket_name = socket_name.replace(SPECIAL_CHAR, "/");
            let con = UnixCon {
                scheme,
                socket_name,
                path: path.to_string(),
                query,
            };

            info_with_info!("UnixCon: parsed {:?} as {:?}", s, con);
            return Ok(UNIX(con));
        }

        if s0.contains(SPECIAL_CHAR) {
            let (socket_name, path) = match after_scheme.find('/') {
                None => {
                    let path = "/".to_string();
                    let host = after_scheme.to_string().clone();
                    let host = host.replace(SPECIAL_CHAR, "/");

                    (host, path)
                }
                Some(path_start0) => {
                    let path = after_scheme[path_start0..].to_string().clone();

                    let host = after_scheme[..path_start0].to_string().clone();

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

            info_with_info!("UnixCon: parsed {:?} as {:?}", s, con);
            Ok(UNIX(con))
        } else {
            let path_start = after_scheme.rfind('/').unwrap();
            let socket_name = &after_scheme[..path_start];
            let path = (&after_scheme[path_start..].to_string()).clone();
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

pub fn join_path(base: &str, s: &str) -> (String, Option<String>) {
    // split the query
    let (query, s) = match s.find("?") {
        Some(i) => (Some(s[i + 1..].to_string()), s[..i].to_string()),
        None => (None, s.to_string()),
    };
    if s.starts_with("/") {
        (s.to_string(), query)
    } else {
        if base.ends_with("/") {
            (canonical(format!("{}{}", base, s)), query)
        } else {
            (canonical(format!("{}/{}", base, s)), query)
        }
    }
}

fn canonical(s: String) -> String {
    normalize_dot_dot(&s)
}

fn normalize_dot_dot(path: &str) -> String {
    let mut stack = Vec::new();

    for segment in path.split('/') {
        match segment {
            "." => {}
            // "" | "." => {
            //     stac
            // }
            ".." => {
                stack.pop();
            }
            _ => stack.push(segment),
        }
    }

    stack.join("/")
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::{debug_with_info, FilePaths};

    use super::*;

    #[test]
    fn test_canonical() {
        assert_eq!(canonical("a/b/c/../d/".to_string()), "a/b/d/");
    }
    // Bring the function into scope

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
    }

    #[test]
    fn url_parse_p2() {
        // without end /
        let s = "http+unix://%2Fsockets%2Fargo%2Fnode1%2F_node";
        let x = super::parse_url_ext(s).unwrap();
        debug_with_info!("test_p1 {s:?} {x:?}");
        assert_eq!(
            x,
            super::TypeOfConnection::UNIX(super::UnixCon {
                scheme: "http+unix".to_string(),
                socket_name: "/sockets/argo/node1/_node".to_string(),
                path: "/".to_string(),
                query: None,
            })
        );
    }

    #[test]
    fn url_parse_square() {
        // without end /
        let s = "http+unix://[/sockets/argo/node1/_node]/the/path";
        let x = super::parse_url_ext(s).unwrap();
        debug_with_info!("test_p1 {s:?} {x:?}");
        assert_eq!(
            x,
            super::TypeOfConnection::UNIX(super::UnixCon {
                scheme: "http+unix".to_string(),
                socket_name: "/sockets/argo/node1/_node".to_string(),
                path: "/the/path".to_string(),
                query: None,
            })
        );
    }

    #[test]
    fn url_parse_3() {
        let s = "file:///abs/file";
        let x = super::parse_url_ext(s).unwrap();
        // debug_with_info!("test_p1 {s:?} {x:?}");
        assert_eq!(
            x,
            super::TypeOfConnection::File(None, FilePaths::Absolute("/abs/file".to_string()))
        );
    }

    #[test]
    fn url_parse_4() {
        // without end /
        let s = "./reldir";
        let x = super::parse_url_ext(s).unwrap();
        assert_eq!(
            x,
            super::TypeOfConnection::File(None, FilePaths::Relative("reldir".to_string()))
        );
    }

    #[test]
    fn url_parse_4c() {
        let s = "reldir";
        let x = super::parse_url_ext(s).unwrap();
        assert_eq!(
            x,
            super::TypeOfConnection::File(None, FilePaths::Relative("reldir".to_string()))
        );
    }

    #[test]
    fn url_parse_4b() {
        let s = "./reldir/";
        let x = super::parse_url_ext(s).unwrap();
        debug_with_info!("test_p1 {s:?} {x:?}");
        assert_eq!(
            x,
            super::TypeOfConnection::File(None, FilePaths::Relative("reldir".to_string()))
        );
    }

    #[test]
    fn url_parse_5() {
        let s = "/abs/dir/";
        let x = super::parse_url_ext(s).unwrap();
        assert_eq!(
            x,
            super::TypeOfConnection::File(None, FilePaths::Absolute("/abs/dir".to_string()))
        );
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

    // #[test]
    fn url_rel() {
        let s = parse_url_ext("file://abs/dir/").unwrap();
        let s2 = s.join("../").unwrap();
        assert_eq!(
            s2,
            super::TypeOfConnection::File(None, FilePaths::Absolute("/abs/".to_string()))
        );
    }

    #[rstest]
    #[case("http://localhost/hello/", "../", "http://localhost/")]
    fn test_rel(#[case] base: &str, #[case] join: &str, #[case] result: &str) -> DTPSR<()> {
        let con = parse_url_ext(base)?;
        let expect = parse_url_ext(result)?;
        let found = con.join(join)?;
        assert_eq!(expect, found);
        Ok(())
    }
}

pub fn format_digest_path(digest: &str, content_type: &str) -> String {
    format!("!/:ipfs/{}/{}/", digest, content_type.replace("/", "_"))
}

pub fn make_relative(base: &str, url: &str) -> String {
    // if base.starts_with("/") || url.starts_with("/") {
    //     return DTPSError::invalid_input!("neither should start with /: {base:?} {url:?}")
    // }
    let base = url::Url::parse(&format!("http://example.org/{base}")).unwrap();
    let target = url::Url::parse(&format!("http://example.org/{url}")).unwrap();
    base.make_relative(&target).unwrap()
}

#[cfg(test)]
mod test {
    use super::make_relative;

    #[test]
    fn t1() {
        assert_eq!(make_relative("clock5/:meta/", "clock5/"), "../");
        assert_eq!(make_relative("clock5/", "clock5/:meta/"), ":meta/");
    }
}
