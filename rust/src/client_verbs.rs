use anyhow::Context;
use hyper::Client;
use hyper_tls::HttpsConnector;
use hyperlocal::UnixClientExt;
use json_patch::Patch as JsonPatch;
use serde::Serialize;
use warp::reply::Response;

use crate::client_link_benchmark::check_unix_socket;
use crate::connections::TypeOfConnection;
use crate::utils_headers::{get_content_type, put_header_accept, put_header_content_type};
use crate::{context, internal_assertion, not_available, not_implemented, DataSaved};
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

pub async fn interpret_resp(con: &TypeOfConnection, resp: Response) -> DTPSR<RawData> {
    if resp.status().is_success() {
        let content_type = get_content_type(&resp);
        // Get the response body bytes.
        let content = hyper::body::to_bytes(resp.into_body()).await?;
        Ok(RawData { content, content_type })
    } else {
        let url = con.to_string();
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
        con.to_string(),
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

    let body_bytes = context!(hyper::body::to_bytes(resp.into_body()).await, "Cannot get body bytes")?;
    if !is_success {
        // pragma: no cover
        let body_text = String::from_utf8_lossy(&body_bytes);
        return not_available!("Request is not a success: for {con}\n{as_string:?}\n{body_text}");
    }
    // let x0: DataSaved = context!(serde_cbor::from_slice(&body_bytes), "Cannot interpret as CBOR: {body_bytes:?}")?;
    let x0: RawData = RawData {
        content: body_bytes,
        content_type,
    };

    //
    // let locations = resp.headers().get_all("location").iter().map(|x| {
    //     let s = x.to_str().unwrap();
    //     context!(con.join(s), "Cannot parse url: {s:?}")?
    // }).collect();

    Ok(PostResponse { rd: x0, locations })
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
            return internal_assertion!("cannot handle a relative url get_metadata: {conbase}");
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

    let resp = context!(
        match conbase {
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
        },
        "make_request(): cannot make {} request for connection {} \
        (use_url={})",
        method,
        conbase,
        use_url.as_str()
    )?;

    Ok(resp)
}

pub async fn patch_data(conbase: &TypeOfConnection, patch: &JsonPatch) -> DTPSR<()> {
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
            conbase.to_string(),
            desc.to_string(),
        ))
    } else {
        Ok(())
    }
}
