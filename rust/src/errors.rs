use std::fmt::Debug;
use std::net::AddrParseError;

use anyhow::Result;
use http::StatusCode;
use indent::indent_all_with;
use log::{debug, error};
use tokio::sync::broadcast::error::SendError;
use warp::{Rejection, Reply};

#[derive(thiserror::Error, Debug)]
pub enum DTPSError {
    #[error("Internal inconsistency:\n{}", indent_inside(.0))]
    InternalInconsistency(String),

    #[error("Not available:\n{}", indent_inside(.0))]
    NotAvailable(String),
    #[error("InvalidInput:\n{}", indent_inside(.0))]
    InvalidInput(String),

    #[error("DTPSError: Not implemented:\n{}", indent_inside(.0))]
    NotImplemented(String),

    #[error("DTPSError: Topic not found:\n{}", indent_inside(.0))]
    TopicNotFound(String),
    #[error("DTPSError: Topic already exists:\n{}", indent_inside(.0))]
    TopicAlreadyExists(String),

    #[error("DTPSError: Unknown DTPS error")]
    Unknown,

    #[error("DTPSError: Other:\n{}", indent_inside(.0))]
    Other(String),

    #[error("DTPSError: Interrupted")]
    Interrupted,

    #[error("DTPSError: Not reachable:\n{}", indent_inside(.0))]
    ResourceNotReachable(String),

    #[error(transparent)]
    FromAnyhow(#[from] anyhow::Error),

    #[error(transparent)]
    FromIO(#[from] std::io::Error),

    #[error(transparent)]
    FromHyper(#[from] hyper::Error),

    #[error(transparent)]
    FromHTTP(#[from] http::Error),

    #[error(transparent)]
    NetworkError(#[from] AddrParseError),

    #[error(transparent)]
    CBORError(#[from] serde_cbor::Error),

    #[error(transparent)]
    JSONError(#[from] serde_json::Error),

    #[error(transparent)]
    YAMLError(#[from] serde_yaml::Error),

    #[error(transparent)]
    PatchError(#[from] json_patch::PatchError),

    #[error(transparent)]
    WarpError(#[from] warp::Error),
    // #[error(transparent)]
    // SendError(#[from] SendError),
    #[error("DTPSError: Error {1} - {2} for url {0} \n{}", indent_inside(.3))]
    FailedRequest(String, u16, String, String),

    #[error(transparent)]
    TokioRecvError(#[from] tokio::sync::broadcast::error::RecvError),
    #[error(transparent)]
    TokioJoinError(#[from] tokio::task::JoinError),
    #[error(transparent)]
    MPSCError(#[from] std::sync::mpsc::RecvError),
}

impl DTPSError {
    pub fn other<X, S: AsRef<str>>(s: S) -> Result<X, DTPSError> {
        Err(DTPSError::Other(s.as_ref().to_string()))
    }
    pub fn internal_assertion<X, S: AsRef<str>>(s: S) -> Result<X, DTPSError> {
        Err(DTPSError::InternalInconsistency(s.as_ref().to_string()))
    }
    pub fn not_reachable<X, S: AsRef<str>>(s: S) -> Result<X, DTPSError> {
        Err(DTPSError::ResourceNotReachable(s.as_ref().to_string()))
    }
    pub fn not_implemented<X, S: AsRef<str>>(s: S) -> Result<X, DTPSError> {
        Err(DTPSError::NotImplemented(s.as_ref().to_string()))
    }
    pub fn not_available<X, S: AsRef<str>>(s: S) -> Result<X, DTPSError> {
        Err(DTPSError::NotAvailable(s.as_ref().to_string()))
    }

    pub fn invalid_input<X, S: AsRef<str>>(s: S) -> Result<X, DTPSError> {
        Err(DTPSError::InvalidInput(s.as_ref().to_string()))
    }
}

impl From<&str> for DTPSError {
    fn from(item: &str) -> Self {
        DTPSError::Other(item.to_string())
    }
}

impl From<String> for DTPSError {
    fn from(item: String) -> Self {
        DTPSError::Other(item)
    }
}
impl From<&String> for DTPSError {
    fn from(item: &String) -> Self {
        DTPSError::Other(item.to_string())
    }
}

pub type DTPSR<T> = anyhow::Result<T, DTPSError>;

pub fn not_available<T, S: AsRef<str>>(s: S) -> Result<T, DTPSError> {
    Err(DTPSError::NotAvailable(s.as_ref().to_string()))
}

pub fn error_other<T, S: AsRef<str>>(s: S) -> Result<T, DTPSError> {
    Err(DTPSError::Other(s.as_ref().to_string()))
}

pub fn todtpserror(t: anyhow::Error) -> DTPSError {
    DTPSError::from(t)
}

pub fn indent_inside(s: &String) -> String {
    indent_all_with("| ", s)
}

impl DTPSError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            DTPSError::TopicNotFound(_) => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn is_temp_env_error(&self) -> bool {
        match self {
            DTPSError::TopicNotFound(_) => false,
            _ => true,
        }
    }
}

impl warp::reject::Reject for DTPSError {}

pub async fn handle_rejection(err: Rejection) -> std::result::Result<impl Reply, Rejection> {
    debug!("handle_rejection: {:?}", err);
    if let Some(custom_error) = err.find::<DTPSError>() {
        let status_code = custom_error.status_code();

        let error_message = format!("{}\n\n{:?}", status_code, custom_error);

        if status_code != 404 {
            error!("{}", error_message);
        }
        return Ok(warp::reply::with_status(error_message, status_code));
    }

    // handle other rejections
    Err(err)
}

pub fn just_log<E: Debug>(e: E) -> () {
    log::error!("Ignoring error: {:?}", e);
}

#[macro_export]
macro_rules! add_info {
    ($($u:expr),* $(,)?) => {{
        format!("{}:{}:\n{}", file!(), line!(),
            indent::indent_all_with("| ", format!($($u),*))
        )
    }};
}

#[macro_export]
macro_rules! error_with_info {
    ($($u:expr),* $(,)?) => {{
        log::error!("{}:{}:\n{}", file!(), line!(),
            indent::indent_all_with("| ", format!($($u),*))
        )
    }};
}

#[macro_export]
macro_rules! debug_with_info {
    ($($u:expr),* $(,)?) => {{
        log::debug!("{}:{}:\n{}", file!(), line!(),
            indent::indent_all_with("| ", format!($($u),*))
        )
    }};
}

#[macro_export]
macro_rules! context {
    ($t: expr,  $($u:expr),* $(,)?) => {{
        ($t).context( crate::add_info!($($u),*) )
    }};
}
#[macro_export]
macro_rules! internal_assertion {
    ($($u:expr),* $(,)?) => {{
        crate::DTPSError::internal_assertion(crate::add_info!($($u),*) )
    }};
}

#[macro_export]
macro_rules! not_implemented {
    ($($u:expr),* $(,)?) => {{
        crate::DTPSError::not_implemented(crate::add_info!($($u),*) )
    }};
}
#[macro_export]
macro_rules! not_available {
    ($($u:expr),* $(,)?) => {{
        crate::DTPSError::not_available(crate::add_info!($($u),*) )
    }};
}

#[macro_export]
macro_rules! invalid_input {
    ($($u:expr),* $(,)?) => {{
        crate::DTPSError::invalid_input(crate::add_info!($($u),*) )
    }};
}

#[macro_export]
macro_rules! not_reachable {
    ($($u:expr),* $(,)?) => {{
        crate::DTPSError::not_reachable(crate::add_info!($($u),*) )
    }};
}
