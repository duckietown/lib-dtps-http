use include_dir::{include_dir, Dir};
use log::debug;
use mime_guess::from_path;
use std::path::{Path, PathBuf};
use warp::{Rejection, Reply};

// Embed the static directory into the crate
pub const STATIC_FILES: Dir = include_dir!("static");

pub async fn serve_static_file(path: warp::path::Tail) -> Result<impl Reply, Rejection> {
    let path = path.as_str();
    debug!("serve_static_file: path={}", path);
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
//
// pub fn dir_to_pathbuf(dir: Dir) -> PathBuf {
//     let mut pathbuf = PathBuf::new();
//     for file in dir.files() {
//         let path = Path::new(file.path());
//         pathbuf.push(path);
//     }
//     pathbuf
// }

// async fn serve_static_file(path: warp::path::FullPath) -> Result<impl Reply, Rejection> {
//     let path = path.as_str();
//     let file = match STATIC_FILES.get_file(path) {
//         Some(file) => file,
//         None => return Ok(warp::reply::not_found()),
//     };
//     let mime_type = from_path(path).first_or_octet_stream();
//     Ok(warp::http::Response::builder()
//         .header("Content-Type", mime_type.to_string())
//         .body(file.contents().to_vec()))
// }
