// #![cfg_attr(debug_assertions, allow(dead_code))]
// #![cfg_attr(debug_assertions, allow(unused_variables))]
#![cfg_attr(debug_assertions, allow(unused_imports))]

pub use client::ms_from_ns;
pub use client::{compute_best_alternative, estimate_latencies, get_index, get_metadata};
pub use constants::*;
pub use errors::*;
pub use logs::init_logging;
pub use server::{create_server_from_command_line, DTPSServer, ServerStateAccess};
pub use server_state::show_errors;
pub use structures::*;
pub use types::TopicName;
pub use urls::parse_url_ext;
pub use utils::time_nanos;

use cbor_manipulation::*;
use client::*;
use html_utils::*;
use logs::*;
use master::*;
use misc::*;
use object_queues::*;
use server::*;
use server_state::*;
use signals_logic::*;
use static_files::*;
use structures_linkproperties::*;
use types::*;
use urls::*;
use utils::*;
use utils_headers::*;
use utils_mime::*;
use utils_yaml::*;
use websocket_abstractions::*;
use websocket_signals::*;

mod structures_linkproperties;

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
mod utils_yaml;
pub mod websocket_abstractions;
pub mod websocket_signals;

#[cfg(test)]
mod test_python;
#[cfg(test)]
mod test_range;

pub mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}
