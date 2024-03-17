use std::time::Duration;

use anyhow::Context;
use http::StatusCode;
use hyper::Client;
use hyper::Error as HyperError;
use hyper_tls::HttpsConnector;
use hyperlocal::UnixClientExt;
use json_patch::Patch as JsonPatch;
use serde::Serialize;
use warp::reply::Response;

use crate::client_link_benchmark::check_unix_socket;
use crate::connections::TypeOfConnection;
use crate::utils_headers::{
    get_content_type, get_content_type_from_headers, put_header_accept, put_header_content_type,
};
use crate::{
    context, debug_with_info, error_with_info, internal_assertion, not_available, not_implemented, ResolvedData,
};
use crate::{DTPSError, RawData, CONTENT_TYPE_PATCH_JSON, DTPSR};

pub async fn get_rawdata_status(con: &TypeOfConnection) -> DTPSR<(http::StatusCode, RawData)> {
    let method = hyper::Method::GET;

    let resp = make_request(con, method, b"", None, None).await?;
    // TODO: send more headers
    Ok((resp.status(), interpret_resp(con, resp).await?))
}

pub async fn get_rawdata(con: &TypeOfConnection) -> DTPSR<RawData> {
    get_rawdata_accept(con, None).await
}

pub async fn get_rawdata_accept(con: &TypeOfConnection, accept: Option<&str>) -> DTPSR<RawData> {
    let method = hyper::Method::GET;
    let resp = make_request(con, method, b"", None, accept).await?;
    // TODO: send more headers
    interpret_resp(con, resp).await
}

pub async fn get_resolved(con: &TypeOfConnection, accept: Option<&str>) -> DTPSR<ResolvedData> {
    let x = make_request2(con, hyper::Method::GET, b"", None, accept).await?;
    return x.into();
}

pub async fn interpret_resp(con: &TypeOfConnection, resp: Response) -> DTPSR<RawData> {
    if resp.status().is_success() {
        let content_type = get_content_type(&resp);
        // Get the response body bytes.
        let content = hyper::body::to_bytes(resp.into_body()).await?;
        Ok(RawData { content, content_type })
    } else {
        let url = con.to_url_repr();
        let code = resp.status().as_u16();
        let as_s = resp.status().as_str().to_string();
        let content = hyper::body::to_bytes(resp.into_body()).await?;
        let string = String::from_utf8(content.to_vec()).unwrap();
        Err(DTPSError::FailedRequest(url, code, as_s, string))
    }
}

pub async fn post_json<T>(con: &TypeOfConnection, value: &T) -> DTPSR<PostResponse>
where
    T: Serialize,
{
    let rd = RawData::encode_as_json(value)?;
    post_data(con, &rd).await
}

pub async fn post_cbor<T>(con: &TypeOfConnection, value: &T) -> DTPSR<PostResponse>
where
    T: Serialize,
{
    let rd = RawData::encode_as_cbor(value)?;
    post_data(con, &rd).await
}

#[derive(Debug, Clone)]
pub struct PostResponse {
    pub rd: RawData,
    pub locations: Vec<TypeOfConnection>,
}

pub async fn post_data(con: &TypeOfConnection, rd: &RawData) -> DTPSR<PostResponse> {
    let resp = context!(
        make_request(con, hyper::Method::POST, &rd.content, Some(&rd.content_type), None).await,
        "Cannot make request to {}",
        con.to_url_repr(),
    )?;

    let (is_success, as_string) = (resp.status().is_success(), resp.status().to_string());
    let content_type = get_content_type(&resp);
    // iterate over all the locations
    let mut locations = vec![];
    for x in resp.headers().get_all("location") {
        let s = x.to_str().unwrap();
        let joined = context!(con.join(s), "Cannot parse url: {s:?}")?;
        locations.push(joined);
    }

    let content = context!(hyper::body::to_bytes(resp.into_body()).await, "Cannot get body bytes")?;
    if !is_success {
        // pragma: no cover
        let body_text = String::from_utf8_lossy(&content);
        return not_available!("Request is not a success: for {con}\n{as_string:?}\n{body_text}");
    }

    let x0: RawData = RawData { content, content_type };

    Ok(PostResponse { rd: x0, locations })
}

#[derive(Debug)]
pub enum ResponseUnobtained {
    // will be treated as special cases
    ConnectionRefused(String),
    Unreachable(String),
    DNSError(String),

    // will just be converted into a DTPSError
    TimedOut(String),
    ConnectionInterrupted(String),
    Generic(String),
}

impl Into<DTPSError> for ResponseUnobtained {
    fn into(self) -> DTPSError {
        match self {
            ResponseUnobtained::ConnectionRefused(x) => DTPSError::ResourceNotReachable(x),
            ResponseUnobtained::Unreachable(x) => DTPSError::ResourceNotReachable(x),
            ResponseUnobtained::DNSError(x) => DTPSError::ResourceNotReachable(x),

            ResponseUnobtained::ConnectionInterrupted(x) => DTPSError::Interrupted(x),
            ResponseUnobtained::TimedOut(x) => DTPSError::Other(x),

            ResponseUnobtained::Generic(x) => DTPSError::Other(x),
        }
    }
}

#[derive(Debug)]
pub struct ResponseObtained {
    pub status: hyper::StatusCode,
    pub headers: hyper::HeaderMap,
    pub raw_data: RawData,
}

#[derive(Debug)]
pub enum ResponseResult {
    ResponseObtained(ResponseObtained),
    ResponseUnobtained(ResponseUnobtained),
}

impl Into<DTPSR<ResolvedData>> for ResponseResult {
    fn into(self) -> DTPSR<ResolvedData> {
        return match self {
            ResponseResult::ResponseObtained(ro) => {
                if ro.status == StatusCode::NO_CONTENT {
                    let msg = "This topic does not have any data yet".to_string();
                    Ok(ResolvedData::NotAvailableYet(msg))
                } else if ro.status == StatusCode::NOT_FOUND {
                    let msg = String::from_utf8_lossy(&ro.raw_data.content).to_string();
                    Ok(ResolvedData::NotFound(msg))
                } else if ro.status == StatusCode::OK {
                    Ok(ResolvedData::RawData(ro.raw_data))
                } else if (ro.status == StatusCode::BAD_GATEWAY)
                    || (ro.status == StatusCode::SERVICE_UNAVAILABLE)
                    || (ro.status == StatusCode::GATEWAY_TIMEOUT)
                    || (ro.status == 530)
                {
                    let s = String::from_utf8_lossy(&ro.raw_data.content).to_string();
                    let msg = format!("Service Unavailable [{}]:\n{s}", ro.status);
                    Ok(ResolvedData::NotReachable(msg))
                } else {
                    // FIXME: should we check for redirects?
                    return not_implemented!("not sure how to deal with as_resolved: {ro:?}");
                }
            }
            ResponseResult::ResponseUnobtained(ru) => {
                match &ru {
                    // we map some specific errors to
                    ResponseUnobtained::ConnectionRefused(s) => Ok(ResolvedData::NotReachable(s.clone())),
                    ResponseUnobtained::Unreachable(s) => Ok(ResolvedData::NotReachable(s.clone())),
                    ResponseUnobtained::DNSError(s) => Ok(ResolvedData::NotReachable(s.clone())),

                    ResponseUnobtained::ConnectionInterrupted(_s)
                    | ResponseUnobtained::TimedOut(_s)
                    | ResponseUnobtained::Generic(_s) => Err(ru.into()),
                }
            }
        };
    }
}

pub async fn make_request2(
    conbase: &TypeOfConnection,
    method: hyper::Method,
    body: &[u8],
    content_type: Option<&str>,
    accept: Option<&str>,
) -> DTPSR<ResponseResult> {
    let use_url = match conbase {
        TypeOfConnection::TCP(url) => url.clone().to_string(),
        TypeOfConnection::UNIX(uc) => {
            context!(check_unix_socket(&uc.socket_name).await, "cannot use unix socket {uc}",)?;

            let h = hex::encode(&uc.socket_name);
            let p0 = format!("unix://{}{}", h, uc.path);
            match uc.query {
                None => p0,
                Some(_) => {
                    let p1 = format!("{}?{}", p0, uc.query.as_ref().unwrap());
                    p1
                }
            }
        }

        TypeOfConnection::Relative(_, _) => {
            return internal_assertion!("cannot handle a relative url: {conbase}");
        }
        TypeOfConnection::Same() => {
            return internal_assertion!("!!! not expected to reach here: {conbase}");
        }
        TypeOfConnection::File(..) => {
            return not_implemented!("read from file to implement: {conbase}");
        }
    };

    let mut req0 = context!(
        hyper::Request::builder()
            .method(&method)
            .uri(use_url.as_str())
            // .header("user-agent", "the-awesome-agent/007")
            .body(hyper::Body::from(body.to_vec())),
        "cannot build request for {method} {use_url}",
    )?;
    if let Some(x) = accept {
        let h = req0.headers_mut();
        put_header_accept(h, x);
    }

    if let Some(x) = content_type {
        let h = req0.headers_mut();
        put_header_content_type(h, x);
    }

    let r0 = match conbase {
        TypeOfConnection::TCP(url) => {
            if url.scheme() == "https" {
                let https = HttpsConnector::new();
                let client = Client::builder().build::<_, hyper::Body>(https);
                client.request(req0).await
            } else {
                // let client = hyper::Client::builder()
                //             .pool_idle_timeout(Duration::from_secs(30))
                //             // .http2_only(true)
                //             .build_http();

                let client = hyper::Client::new();
                client.request(req0).await
            }
        }
        TypeOfConnection::UNIX(_) => {
            let client = Client::unix();
            client.request(req0).await
        }

        TypeOfConnection::Relative(_, _) => {
            return not_available("cannot handle a relative url get_metadata");
        }
        TypeOfConnection::Same() => {
            return not_available("cannot handle a Same url to get_metadata");
        }
        TypeOfConnection::File(..) => {
            return internal_assertion!("not supposed to reach here: {conbase}");
        }
    };
    // debug_with_info!("make_request: {r0:?}");

    let resp = match r0 {
        Ok(x) => x,
        Err(e) => {
            debug_with_info!("make_request: {e:?}");

            let x: ResponseUnobtained = if e.is_connect() {
                let msg = e.to_string();
                if msg.contains("dns error") {
                    ResponseUnobtained::DNSError(msg)
                } else {
                    ResponseUnobtained::ConnectionRefused(msg)
                }
            } else if e.is_closed() {
                let msg = format!(
                    "The connection was closed before a response was received.\n{}",
                    e.to_string()
                );
                ResponseUnobtained::ConnectionInterrupted(msg)
            } else if e.is_timeout() {
                let msg = format!("Timeout.\n{}", e.to_string());
                ResponseUnobtained::TimedOut(msg)
            } else {
                let msg = format!("Generic response error.\n{}", e.to_string());
                // General error handling
                ResponseUnobtained::Generic(msg)
            };

            return Ok(ResponseResult::ResponseUnobtained(x));
        }
    };

    let status = resp.status().clone();
    // let status_string = resp.status().as_str().to_string();
    let headers = resp.headers().clone();
    let content = hyper::body::to_bytes(resp.into_body()).await?;
    let content_type = get_content_type_from_headers(&headers);
    let raw_data = RawData { content, content_type };
    // let string = String::from_utf8(content.to_vec()).unwrap();
    // Err(DTPSError::FailedRequest(url, code, as_s, string))

    Ok(ResponseResult::ResponseObtained(ResponseObtained {
        status,
        headers,
        raw_data,
    }))
}

pub async fn make_request(
    conbase: &TypeOfConnection,
    method: hyper::Method,
    body: &[u8],
    content_type: Option<&str>,
    accept: Option<&str>,
) -> DTPSR<Response> {
    let use_url = match conbase {
        TypeOfConnection::TCP(url) => url.clone().to_string(),
        TypeOfConnection::UNIX(uc) => {
            context!(check_unix_socket(&uc.socket_name).await, "cannot use unix socket {uc}",)?;

            let h = hex::encode(&uc.socket_name);
            let p0 = format!("unix://{}{}", h, uc.path);
            match uc.query {
                None => p0,
                Some(_) => {
                    let p1 = format!("{}?{}", p0, uc.query.as_ref().unwrap());
                    p1
                }
            }
        }

        TypeOfConnection::Relative(_, _) => {
            return internal_assertion!("cannot handle a relative url: {conbase}");
        }
        TypeOfConnection::Same() => {
            return internal_assertion!("!!! not expected to reach here: {conbase}");
        }
        TypeOfConnection::File(..) => {
            return not_implemented!("read from file to implement: {conbase}");
        }
    };

    let mut req0 = context!(
        hyper::Request::builder()
            .method(&method)
            .uri(use_url.as_str())
            // .header("user-agent", "the-awesome-agent/007")
            .body(hyper::Body::from(body.to_vec())),
        "cannot build request for {method} {use_url}",
    )?;
    if let Some(x) = accept {
        let h = req0.headers_mut();
        put_header_accept(h, x);
    }

    if let Some(x) = content_type {
        let h = req0.headers_mut();
        put_header_content_type(h, x);
    }

    let r0 = match conbase {
        TypeOfConnection::TCP(url) => {
            if url.scheme() == "https" {
                let https = HttpsConnector::new();
                let client = Client::builder().build::<_, hyper::Body>(https);
                client.request(req0).await
            } else {
                let client = hyper::Client::new();
                client.request(req0).await
            }
        }
        TypeOfConnection::UNIX(_) => {
            let client = Client::unix();
            client.request(req0).await
        }

        TypeOfConnection::Relative(_, _) => {
            return not_available("cannot handle a relative url get_metadata");
        }
        TypeOfConnection::Same() => {
            return not_available("cannot handle a Same url to get_metadata");
        }
        TypeOfConnection::File(..) => {
            return internal_assertion!("not supposed to reach here: {conbase}");
        }
    };
    //
    // let resp = match r0 {
    //     Ok(x) => x,
    //     Err(e) => {
    //         match e {
    //             hyper::Error(e) => {
    //                 return not_available!("make_request: {e:?}");
    //             }
    //             _ => {}
    //         }
    //         return not_available!("make_request: {e:?}");
    //     }
    // }
    // r0.map_err(|e| anyhow::anyhow!("make_request: {e:?}"))?;
    debug_with_info!("make_request:\n{use_url}\n{r0:?}");

    let resp = match r0 {
        Ok(x) => x,
        Err(e) => {
            return not_available!("Connection error: {e:?}");
        }
    };

    // let resp = context!(
    //     r0,
    //
    //     "make_request(): cannot make {} request for connection {} \
    //     (use_url={})",
    //     method,
    //     conbase,
    //     use_url.as_str()
    // )?;

    Ok(resp)
}

pub async fn patch_data(conbase: &TypeOfConnection, patch: &JsonPatch) -> DTPSR<RawData> {
    let json_data = serde_json::to_vec(patch)?;
    // debug_with_info!("patch_data out: {:#?}", String::from_utf8(json_data.clone()));
    let resp = make_request(
        conbase,
        hyper::Method::PATCH,
        &json_data,
        Some(CONTENT_TYPE_PATCH_JSON),
        None,
    )
    .await?;
    let status = resp.status();
    if !status.is_success() {
        let desc = status.as_str();
        let msg = format!("cannot patch:\nurl: {conbase}\n---\n{desc}");
        Err(DTPSError::FailedRequest(
            msg,
            status.as_u16(),
            conbase.to_url_repr(),
            desc.to_string(),
        ))
    } else {
        interpret_resp(conbase, resp).await
    }
}
