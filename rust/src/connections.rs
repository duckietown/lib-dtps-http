use std::{fmt, fmt::Display, path::PathBuf};

use url::Url;

use crate::{join_path, parse_url_ext, DTPSR};

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub struct UnixCon {
    pub scheme: String,
    pub socket_name: String,
    pub path: String,
    pub query: Option<String>,
}

impl UnixCon {
    pub fn from_path(path: &str) -> Self {
        Self {
            scheme: "http+unix".to_string(),
            socket_name: path.to_string(),
            path: "/".to_string(),
            query: None,
        }
    }
}

impl Display for UnixCon {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UnixCon(")?;
        write!(f, "[{}]", self.socket_name)?;
        write!(f, " {}", self.path)?;
        if let Some(query) = &self.query {
            write!(f, "?{}", query)?;
        }
        write!(f, ")")?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub enum FilePaths {
    Absolute(String),
    Relative(String),
}

pub fn join_and_normalize(path1: &str, path2: &str) -> String {
    let mut path = PathBuf::from(path1);
    path.push(path2);
    path.canonicalize().unwrap_or(path).to_str().unwrap().to_string()
}

pub fn normalize_path(path1: &str) -> String {
    let path = PathBuf::from(path1);
    let mut p = path.canonicalize().unwrap_or(path).to_str().unwrap().to_string();
    if p.len() > 1 {
        p = p.trim_end_matches("/").to_string();
    }
    return p;
}

impl FilePaths {
    pub(crate) fn add_prefix(&self, prefix: &str) -> FilePaths {
        match self {
            FilePaths::Absolute(s) => FilePaths::Absolute(s.clone()),
            FilePaths::Relative(s) => FilePaths::Relative(join_and_normalize(prefix, s)),
        }
    }
    pub(crate) fn join(&self, suffix: &str) -> FilePaths {
        match self {
            FilePaths::Absolute(s) => FilePaths::Absolute(join_and_normalize(s, suffix)),
            FilePaths::Relative(s) => FilePaths::Relative(join_and_normalize(s, suffix)),
        }
    }
}

impl Display for FilePaths {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FilePaths::Absolute(s) => write!(f, "{}", s),
            FilePaths::Relative(s) => write!(f, "./{}", s),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub enum TypeOfConnection {
    /// TCP Connection
    TCP(Url),
    /// Unix socket connection
    UNIX(UnixCon),
    /// A file or dir in the filesystem
    File(Option<String>, FilePaths),

    /// Path relative to context (used in indices)
    Relative(String, Option<String>),

    /// Exactly same context
    Same(),
}

impl TypeOfConnection {
    pub fn from_string(s: &str) -> DTPSR<Self> {
        parse_url_ext(s)
    }

    pub fn unix_socket(path: &str) -> Self {
        Self::UNIX(UnixCon::from_path(path))
    }
    pub fn to_string(&self) -> String {
        match self {
            TypeOfConnection::TCP(url) => url.to_string(),
            TypeOfConnection::Relative(s, q) => match q {
                Some(query) => {
                    let mut s = s.clone();
                    s.push_str("?");
                    s.push_str(&query);
                    s
                }
                None => s.clone(),
            },
            TypeOfConnection::UNIX(unixcon) => {
                let mut s = unixcon.scheme.clone();
                s.push_str("://");
                let escaped = unixcon.socket_name.replace("/", "%2F");
                s.push_str(&escaped);
                s.push_str(&unixcon.path);
                if let Some(query) = &unixcon.query {
                    s.push_str("?");
                    s.push_str(&query);
                }
                s
            }
            TypeOfConnection::Same() => "".to_string(),
            TypeOfConnection::File(_, path) => path.clone().to_string(),
        }
    }
}

impl Display for TypeOfConnection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TypeOfConnection::File(hostname, path) => {
                write!(f, "File(host={:?},path={:?})", hostname, path)
            }
            TypeOfConnection::TCP(url) => write!(f, "TCP({:?})", url.to_string()),
            TypeOfConnection::UNIX(unix_con) => write!(f, "UNIX({})", unix_con),
            TypeOfConnection::Relative(s, q) => write!(f, "Relative({},{:?})", s, q),
            TypeOfConnection::Same() => {
                write!(f, "Same()")
            }
        }
    }
}

impl TypeOfConnection {
    pub fn join(&self, s: &str) -> DTPSR<TypeOfConnection> {
        join_ext(self, s)
    }
}

pub fn join_con(a: &str, b: &TypeOfConnection) -> DTPSR<TypeOfConnection> {
    match b {
        TypeOfConnection::TCP(_) => Ok(b.clone()),
        TypeOfConnection::UNIX(_) => Ok(b.clone()),
        TypeOfConnection::Relative(path2, query2) => {
            let (path3, _query3) = join_path(a, path2.as_str());
            Ok(TypeOfConnection::Relative(path3, query2.clone()))
        }

        TypeOfConnection::Same() => Ok(TypeOfConnection::Relative(a.to_string(), None)),
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
            TypeOfConnection::TCP(mut url) => {
                let (path2, query2) = join_path(url.path(), s);
                url.set_path(&path2);
                url.set_query(query2.as_deref());
                Ok(TypeOfConnection::TCP(url))
            }
            TypeOfConnection::Relative(path, _query) => {
                let (path2, query2) = join_path(path.as_str(), s);
                Ok(TypeOfConnection::Relative(path2, query2))
            }
            TypeOfConnection::UNIX(mut uc) => {
                let (path2, query2) = join_path(uc.path.as_str(), s);
                uc.path = path2;
                uc.query = query2;
                Ok(TypeOfConnection::UNIX(uc))
            }

            TypeOfConnection::Same() => Ok(TypeOfConnection::Relative(s.to_string(), None)),
            TypeOfConnection::File(hostname, path) => Ok(TypeOfConnection::File(hostname, path.join(s))),
        }
    }
}
