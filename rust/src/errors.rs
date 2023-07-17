use anyhow::Result;
use http::StatusCode;
use indent::indent_all_with;
use log::{debug, error};
use std::fmt::Debug;
use std::net::AddrParseError;
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

    // #[error("Recursive error:\n{0}")]
    // RecursiveError(Box<DTPSError>),
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

pub type DTPSR<T> = anyhow::Result<T, DTPSError>;

// struct Mine<T, X> (  anyhow::Result<T, X>);

//
// impl<T, X> Mine<T, X> {
//     pub fn just_log(&self) -> () {
//         match self {
//             Ok(_) => (),
//             Err(e) => {
//                 log::error!("Error: {:?}", e);
//             }
//         }
//     }
// }

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

    // s.insert_str(0, "  ");
    // s.replace("\n", "\n  ")
}
//
// impl DTPSError {
//     fn con<S: AsRef<str>>(&self, s: S) -> Self {
//
//         self.context(s.as_ref()).unwrap()
//
//
//         // reject::custom(DTPSError::FromAnyhow(err))
//
//     }
// }
// //
// impl<T> Context<T, DTPSError> for anyhow::Result<T, DTPSError>
// {
//     fn context<C>(self, context: C) -> Result<T, DTPSError>
//     where
//         C: Display + Send + Sync + 'static,
//     {
//         // Not using map_err to save 2 useless frames off the captured backtrace
//         // in ext_context.
//         match self {
//             Ok(ok) => Ok(ok),
//             Err(error) => Err(error.ext_context(context)),
//         }
//     }
//
//     fn with_context<C, F>(self, context: F) -> Result<T, anyhow::Error>
//     where
//         C: Display + Send + Sync + 'static,
//         F: FnOnce() -> C,
//     {
//         match self {
//             Ok(ok) => Ok(ok),
//             Err(error) => Err(error.ext_context(context())),
//         }
//     }
// }

//
// impl<T> DTPSR<T>
// {
//     fn context<C>(self, context: C) -> Result<T, DTPSError>
//     where
//         C: Display + Send + Sync + 'static,
//     {
//         // Not using map_err to save 2 useless frames off the captured backtrace
//         // in ext_context.
//         match self {
//             Ok(ok) => Ok(ok),
//             Err(error) => Err(error.ext_context(context)),
//         }
//     }
//
//     fn with_context<C, F>(self, context: F) -> Result<T, anyhow::Error>
//     where
//         C: Display + Send + Sync + 'static,
//         F: FnOnce() -> C,
//     {
//         match self {
//             Ok(ok) => Ok(ok),
//             Err(error) => Err(error.ext_context(context())),
//         }
//     }
// }

// #[derive(Debug)]
// pub struct CustomAnyHowError(anyhow::Error);
//
// impl warp::reject::Reject for CustomAnyHowError {}
// impl warp::reject::Reject for anyhow::Error {}

// impl From<std::io::Error> for DTPSError {
//     fn from(err: std::io::Error) -> DTPSError {
//         DTPSError::FromStdError(format!("{}", err))
//     }
// }

// impl From<hyper::Error> for DTPSError {
//     fn from(err: hyper::Error) -> DTPSError {
//         DTPSError::FromHyper(format!("{}", err))
//     }
// }
//
// impl From<http::Error> for DTPSError {
//     fn from(err: http::Error) -> DTPSError {
//         DTPSError::FromHTTP(format!("converted: {}", err))
//     }
// }
//
// impl From<anyhow::Error> for DTPSError {
//     fn from(err: anyhow::Error) -> DTPSError {
//         DTPSError::Anyhow(err)
//     }
// }
//
// impl Into<Rejection> for DTPSError {
//     fn into(self) -> Rejection {
//         match self {
//             DTPSError::TopicNotFound(s) => warp::reject::not_found(),
//             _ => warp::reject::custom(self)
//         }
//     }
// }

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

impl warp::reject::Reject for DTPSError {
    // fn status_code(&self) -> StatusCode {
    //     match self {
    //         DTPSError::TopicNotFound(_) => StatusCode::NOT_FOUND,
    //         _ => StatusCode::INTERNAL_SERVER_ERROR
    //     }
    // }
}

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

//
// macro_rules! add_context {
//     ($func:expr, $ctx:expr) => {
//         match $func {
//             Ok(val) => Ok(val),
//             Err(err) => Err(DTPSError::RecursiveError(Box::new(err), $ctx)),
//         }
//     };
// }

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
        error!("{}:{}:\n{}", file!(), line!(),
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
        DTPSError::internal_assertion(crate::add_info!($($u),*) )
    }};
}

#[macro_export]
macro_rules! not_implemented {
    ($($u:expr),* $(,)?) => {{
        DTPSError::not_implemented(crate::add_info!($($u),*) )
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
