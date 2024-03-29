use std::{fmt::Debug, net::AddrParseError};

use anyhow::Result;
use http::StatusCode;
use hyper::Body;
use indent::indent_all_with;
use warp::{Rejection, Reply};

use crate::{debug_with_info, error_with_info, server::HandlersResponse};

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

    #[error("DTPSError: Mount pint not ready:\n{}", indent_inside(.0))]
    MountpointNotReady(String),

    #[error("DTPSError: Topic already exists:\n{}", indent_inside(.0))]
    TopicAlreadyExists(String),

    #[error("DTPSError: Resource not found:\n{}", indent_inside(.0))]
    ResourceNotFound(String),

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

    #[error("DTPSError: Error {1} - {2} for url {0} \n{}", indent_inside(.3))]
    FailedRequest(String, u16, String, String),

    #[error(transparent)]
    TokioRecvError(#[from] tokio::sync::broadcast::error::RecvError),
    #[error(transparent)]
    TokioJoinError(#[from] tokio::task::JoinError),
    // #[error(transparent)]
    // TokioSendError(#[from] tokio::sync::broadcast::error::SendError),
    #[error(transparent)]
    MPSCError(#[from] std::sync::mpsc::RecvError),
    // #[error(transparent)]
    // MPSCSendError(#[from] std::sync::mpsc::SendError),
    #[error("{context}---\n{source}")]
    Context { context: String, source: Box<DTPSError> },
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

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for DTPSError {
    fn from(item: tokio::sync::mpsc::error::SendError<T>) -> Self {
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

impl DTPSError {
    pub fn as_handler_response(&self) -> HandlersResponse {
        let s = self.to_string();
        let res = http::Response::builder()
            .status(self.status_code())
            .body(Body::from(s))
            .unwrap();
        Ok(res)
    }
}

impl Into<HandlersResponse> for DTPSError {
    fn into(self) -> HandlersResponse {
        self.as_handler_response()
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
            DTPSError::TopicNotFound(..) | DTPSError::ResourceNotFound(..) => StatusCode::NOT_FOUND,
            DTPSError::InvalidInput(..) => StatusCode::BAD_REQUEST,
            DTPSError::MountpointNotReady(..) => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn is_temp_env_error(&self) -> bool {
        !matches!(self, DTPSError::TopicNotFound(_))
    }
}

impl warp::reject::Reject for DTPSError {}

impl DTPSError {
    pub fn add_context(self, context: &str) -> Self {
        // let c1 = indent_inside(&context.to_string());
        let c1 = context.to_string();
        // add endline if not present
        let c1 = if c1.ends_with('\n') { c1 } else { format!("{}\n", c1) };
        match self {
            DTPSError::Context { context, source } => DTPSError::Context {
                context: format!("{}---\n{}", c1, context),
                source,
            },
            _ => DTPSError::Context {
                context: c1,
                source: Box::new(self),
            },
        }
    }
}
#[macro_export]
macro_rules! dtpserror_context {
    ($expr:expr, $($u:expr),* $(,)?) => {{
        let s = format!("{}:{}:\n{}", file!(), line!(),
            indent::indent_all_with("| ", format!($($u),*))
        );
        $expr.map_err(|e| e.add_context(&s))
    }};
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
        log::error!("{}:{}:\n{}", file!(), line!(), // ok
            indent::indent_all_with("| ", format!($($u),*))
        )
    }};
}

#[macro_export]
macro_rules! warn_with_info {
    ($($u:expr),* $(,)?) => {{
        log::warn!("{}:{}:\n{}", file!(), line!(), // ok
            indent::indent_all_with("| ", format!($($u),*))
        )
    }};
}

#[macro_export]
macro_rules! debug_with_info {
    ($($u:expr),* $(,)?) => {{
        log::debug!("{}:{}:\n{}", file!(), line!(), // OK
            indent::indent_all_with("| ", format!($($u),*))
        )
    }};
}
#[macro_export]
macro_rules! info_with_info {
    ($($u:expr),* $(,)?) => {{
        log::info!("{}:{}:\n{}", file!(), line!(), // OK
            indent::indent_all_with("| ", format!($($u),*))
        )
    }};
}

#[macro_export]
macro_rules! context {
    ($t: expr,  $($u:expr),* $(,)?) => {{
        ($t).context( $crate::add_info!($($u),*) )
    }};
}

#[macro_export]
macro_rules! internal_assertion {
    ($($u:expr),* $(,)?) => {{
        $crate::error_with_info!("INTERNAL ASSERTION: {}", $crate::add_info!($($u),*) );
        $crate::DTPSError::internal_assertion($crate::add_info!($($u),*) )
    }};
}

#[macro_export]
macro_rules! not_implemented {
    ($($u:expr),* $(,)?) => {{
        {
            $crate::error_with_info!("Not implemented: {}", $crate::add_info!($($u),*) );
            $crate::DTPSError::not_implemented($crate::add_info!($($u),*) )
        }

    }
    };
}
#[macro_export]
macro_rules! not_available {
    ($($u:expr),* $(,)?) => {{
        $crate::DTPSError::not_available($crate::add_info!($($u),*) )
    }};
}

#[macro_export]
macro_rules! invalid_input {
    ($($u:expr),* $(,)?) => {{
        $crate::DTPSError::invalid_input($crate::add_info!($($u),*) )
    }};
}

#[macro_export]
macro_rules! not_reachable {
    ($($u:expr),* $(,)?) => {{
        $crate::DTPSError::not_reachable($crate::add_info!($($u),*) )
    }};
}

#[macro_export]
macro_rules! dtpserror_other {
    ($($u:expr),* $(,)?) => {{
        $crate::DTPSError::other($crate::add_info!($($u),*) )
    }};
}

pub async fn handle_rejection(err: Rejection) -> std::result::Result<impl Reply, Rejection> {
    debug_with_info!("handle_rejection: {:#?}", err);
    if let Some(custom_error) = err.find::<DTPSError>() {
        let status_code = custom_error.status_code();

        let error_message = format!("Result: {}\n\n{}", status_code, custom_error);

        if status_code != 404 {
            error_with_info!("{}", error_message);
        }
        return Ok(warp::reply::with_status(error_message, status_code));
    }
    Err(err)
}
