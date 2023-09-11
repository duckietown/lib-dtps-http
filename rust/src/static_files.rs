use http::Response;
use hyper::Body;
use include_dir::{
    include_dir,
    Dir,
};
use maud::{
    html,
    DOCTYPE,
};
use mime_guess::from_path;
use warp::{
    Rejection,
    Reply,
};

use crate::HandlersResponse;

// Embed the static directory into the crate
pub const STATIC_FILES: Dir = include_dir!("$CARGO_MANIFEST_DIR/static");

pub async fn serve_static_file2(_s: String, path: warp::path::Tail) -> Result<impl Reply, Rejection> {
    serve_static_file(path).await
}

pub async fn serve_static_file_empty() -> Result<impl Reply, Rejection> {
    serve_static_file_path("").await
}

pub async fn reply_for_dir(path: &str) -> Result<Response<Body>, Rejection> {
    let dir = match STATIC_FILES.get_dir(path) {
        Some(dir) => dir,
        None => return Err(warp::reject::not_found()),
    };
    let mut items = vec![];
    for file in dir.files() {
        let path = file.path().to_str().unwrap();
        items.push(html! {
            li {
                a href=(path) { (path) }
            }
        });
    }

    let x = html! {
        (DOCTYPE)

        html {
            head {
                link rel="icon" type="image/png" href="!/static/favicon.png";
                link rel="stylesheet" href="!/static/style.css";
                title { "DTPS Server" }
            }
            body {

            ul {
                @for item in items {
                    (item)
                }
            }
        }}

    };
    let markup = x.into_string().clone();
    let resp = Response::new(Body::from(markup));

    Ok(resp)
}

pub async fn serve_static_file_path(path: &str) -> HandlersResponse {
    if path == "" {
        return reply_for_dir(path).await;
    }
    let _dir = match STATIC_FILES.get_dir(path) {
        Some(_dir) => {
            return reply_for_dir(path).await;
        }
        None => {}
    };

    // debug_with_info!("serve_static_file: path={}", path);
    let file = match STATIC_FILES.get_file(path) {
        Some(file) => file,
        None => return Err(warp::reject::not_found()),
    };
    let mime_type = from_path(path).first_or_octet_stream();
    let body = Body::from(file.contents().to_owned());
    let response = warp::http::Response::builder()
        .header("Content-Type", mime_type.to_string())
        .body(body)
        .unwrap();
    Ok(response)
}

pub async fn serve_static_file(path: warp::path::Tail) -> Result<Response<Body>, Rejection> {
    let path = path.as_str();
    serve_static_file_path(path).await
}
