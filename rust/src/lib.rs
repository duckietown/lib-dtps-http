#![allow(dead_code)]
#![allow(unused_variables)]
// #![allow(unused_imports)]

mod cbor_manipulation;
pub mod client;
mod cloudflare;
pub mod constants;
mod html_utils;
pub mod logs;
mod master;
pub mod misc;
pub mod object_queues;
mod platform;
pub mod server;
pub mod server_state;
pub mod signals_logic;
pub mod static_files;
pub mod structures;
pub mod types;
pub mod urls;
mod utils;
pub use utils::*;
pub mod errors;
mod websocket_signals;
pub use errors::*;

pub use client::*;
pub use constants::*;
pub use logs::*;
pub use misc::*;
pub use object_queues::*;
pub use server::*;
pub use server_state::*;
pub use signals_logic::*;
pub use static_files::*;
pub use structures::*;
pub use types::*;
pub use urls::*;
pub use DTPSServer;

pub mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}
