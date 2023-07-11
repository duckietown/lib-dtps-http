use include_dir::{include_dir, Dir};
use mime_guess::from_path;
use warp::{Rejection, Reply};

// Embed the static directory into the crate
pub const STATIC_FILES: Dir = include_dir!("$CARGO_MANIFEST_DIR/static");

pub async fn serve_static_file(path: warp::path::Tail) -> Result<impl Reply, Rejection> {
    let path = path.as_str();
    // debug!("serve_static_file: path={}", path);
    let file = match STATIC_FILES.get_file(path) {
        Some(file) => file,
        None => return Err(warp::reject::not_found()),
    };
    let mime_type = from_path(path).first_or_octet_stream();
    let response = warp::http::Response::builder()
        .header("Content-Type", mime_type.to_string())
        .body(file.contents().to_owned());
    Ok(response)
}
