// #![cfg_attr(debug_assertions, allow(dead_code))]
// #![cfg_attr(debug_assertions, allow(unused_variables))]
#![cfg_attr(debug_assertions, allow(unused_imports))]

use cbor_manipulation::*;
pub use client::*;
pub use constants::*;
pub use errors::*;
use html_utils::*;
pub use logs::*;
use master::*;
pub use misc::*;
pub use object_queues::*;
pub use server::*;
pub use server_state::*;
pub use signals_logic::*;
pub use static_files::*;
pub use structures::*;
use structures_linkproperties::*;
pub use types::*;
pub use urls::*;
use utils::*;
use utils_headers::*;
use utils_mime::*;
use utils_yaml::*;
use websocket_abstractions::*;
use websocket_signals::*;
mod structures_linkproperties;
mod test_python;
mod test_range;
mod utils_yaml;

pub mod cbor_manipulation;
pub mod client;
pub mod cloudflare;
pub mod constants;
pub mod errors;
pub mod html_utils;
pub mod logs;
pub mod master;
pub mod misc;
pub mod object_queues;
pub mod platform;
pub mod server;
pub mod server_state;
pub mod signals_logic;
pub mod static_files;
pub mod structures;
pub mod types;
pub mod urls;
pub mod utils;
pub mod utils_headers;
pub mod utils_mime;
pub mod websocket_abstractions;
pub mod websocket_signals;

pub use utils::time_nanos;
pub use DTPSServer;

pub mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}
