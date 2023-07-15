use thiserror::Error;
use warp::Rejection;

#[derive(Error, Debug)]
pub enum DTPSError {
    #[error("Internal inconsistency:\n{0}")]
    InternalInconsistency(String),

    #[error("Not available:\n{0}")]
    NotAvailable(String),

    #[error("Not implemented:\n{0}")]
    NotImplemented(String),

    #[error("Topic not found:\n{0}")]
    TopicNotFound(String),

    #[error("Unknown DTPS error")]
    Unknown,

    #[error("Other: {0}")]
    Other(String),

    #[error("Interrupted")]
    Interrupted,

    #[error("Not reachable:\n{0}")]
    ResourceNotReachable(String),

    #[error("Recursive error:\n{0}")]
    RecursiveError(Box<DTPSError>),

    #[error("std:\n{0}")]
    FromStdError(String),

    #[error("http:\n{0}")]
    FromHTTP(String),

    #[error("hyper:\n{0}")]
    FromHyper(String),
}

impl DTPSError {
    pub fn other<X, S: AsRef<str>>(s: S) -> Result<X, DTPSError> {
        Err(DTPSError::Other(s.as_ref().to_string()))
    }
}

impl DTPSError {
    pub fn not_reachable<X, S: AsRef<str>>(s: S) -> Result<X, DTPSError> {
        Err(DTPSError::ResourceNotReachable(s.as_ref().to_string()))
    }
}

pub type DTPSR<T> = Result<T, DTPSError>;

pub fn not_available<T, S: AsRef<str>>(s: S) -> Result<T, DTPSError> {
    Err(DTPSError::NotAvailable(s.as_ref().to_string()))
}

pub fn error_other<T, S: AsRef<str>>(s: S) -> Result<T, DTPSError> {
    Err(DTPSError::Other(s.as_ref().to_string()))
}

impl From<std::io::Error> for DTPSError {
    fn from(err: std::io::Error) -> DTPSError {
        DTPSError::FromStdError(format!("{}", err))
    }
}

impl From<hyper::Error> for DTPSError {
    fn from(err: hyper::Error) -> DTPSError {
        DTPSError::FromHyper(format!("{}", err))
    }
}

impl From<http::Error> for DTPSError {
    fn from(err: http::Error) -> DTPSError {
        DTPSError::FromHTTP(format!("converted: {}", err))
    }
}
//
// impl Into<Rejection> for DTPSError {
//     fn into(self) -> Rejection {
//         match self {
//             DTPSError::TopicNotFound(s) => warp::reject::not_found(),
//             _ => warp::reject::custom(self)
//         }
//     }
// }

impl warp::reject::Reject for DTPSError {}

macro_rules! add_context {
    ($func:expr, $ctx:expr) => {
        match $func {
            Ok(val) => Ok(val),
            Err(err) => Err(DTPSError::RecursiveError(Box::new(err), $ctx)),
        }
    };
}

// Usage
