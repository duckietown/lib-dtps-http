// #![cfg_attr(debug_assertions, allow(dead_code))]
// #![cfg_attr(debug_assertions, allow(unused_variables))]
#![cfg_attr(debug_assertions, allow(unused_imports))]

pub use indent::indent_all_with;

use cbor_manipulation::*;
use client::*;
pub use client::{compute_best_alternative, estimate_latencies, get_index, get_metadata, ms_from_ns};
use clocks::*;
use connections::*;
pub use constants::*;
pub use errors::*;
use html_utils::*;
pub use logs::init_logging;
use logs::*;

use misc::*;
use object_queues::*;
use server::*;
pub use server::{create_server_from_command_line, DTPSServer, ServerStateAccess};
pub use server_state::show_errors;
use server_state::*;
use signals_logic::*;
use static_files::*;
pub use structures::*;
use structures_linkproperties::*;

pub use structures_topicref::TopicProperties;
use structures_topicref::*;
pub use types::TopicName;

pub use urls::parse_url_ext;
use urls::*;
pub use utils::time_nanos;
use utils::*;
use utils_headers::*;
use utils_mime::*;

use websocket_abstractions::*;
use websocket_signals::*;

mod structures_linkproperties;

pub mod cbor_manipulation;
pub mod client;
pub mod clocks;
pub mod cloudflare;
pub mod connections;
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
pub mod signals_logic_patch;
mod signals_logic_resolvedata;
mod signals_logic_streams;
pub mod static_files;
pub mod structures;
pub mod structures_rawdata;
pub mod structures_topicref;
pub mod types;
pub mod urls;
pub mod utils;
mod utils_cbor;
pub mod utils_headers;
pub mod utils_mime;
mod utils_yaml;
pub mod websocket_abstractions;
pub mod websocket_signals;

use utils_cbor::*;

pub mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

mod internal_jobs;
mod signals_logic_meta;
mod signals_logic_props;
mod signals_logic_push;
mod signals_logic_resolve;
#[cfg(test)]
mod test_python;
#[cfg(test)]
mod test_range;

mod shared_statuses;
#[cfg(test)]
mod test_connections;
mod test_fixtures;

use internal_jobs::*;
use signals_logic_push::*;

use master::*;
use signals_logic_resolve::*;
