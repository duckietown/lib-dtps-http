// #![cfg_attr(debug_assertions, allow(dead_code))]
// #![cfg_attr(debug_assertions, allow(unused_variables))]
#![cfg_attr(debug_assertions, allow(unused_imports))]

pub use indent::indent_all_with;

use cbor_manipulation::*;
pub use cli_program_listen::cli_listen;
pub use cli_program_server::cli_server;
pub use cli_program_stats::cli_stats;
pub use cli_program_subscribe::cli_subscribe;
use client_history::*;
pub use client_index::get_index;
pub use client_link_benchmark::compute_best_alternative;
pub use client_metadata::estimate_latencies;
pub use client_metadata::get_metadata;
pub use client_metadata::ms_from_ns;
use client_metadata::*;
use client_proxy::*;
use client_publish::*;
use client_topics::*;
pub use client_verbs::*;
use client_verbs::*;
use client_websocket_push::*;
use client_websocket_push::*;
use client_websocket_read::*;
use clocks::*;
use connections::*;
pub use constants::*;
pub use errors::*;
use html_utils::*;
pub use interface::DTPSLowLevel;
pub use logs::init_logging;
use logs::*;
use master::*;
use misc::*;
use object_queues::*;
use server::*;
pub use server::{create_server_from_command_line, DTPSServer, ServerStateAccess};
pub use server_state::show_errors;
use server_state::*;
use signals_logic::*;
use signals_logic_resolve::*;
use static_files::*;
pub use structures::*;
use structures_linkproperties::*;
pub use structures_topicref::TopicProperties;
use structures_topicref::*;
pub use types::{CompositeName, TopicName};
pub use urls::parse_url_ext;
use urls::*;
use utils::*;
use utils_cbor::*;
use utils_headers::*;
use utils_mime::*;
use utils_queues::*;
pub use utils_time::time_nanos;
use utils_time::*;
use utils_websocket::*;
use websocket_abstractions::*;

mod structures_linkproperties;

pub mod cbor_manipulation;
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

pub mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

mod internal_jobs;
mod signals_logic_meta;
mod signals_logic_props;
mod signals_logic_publish;
mod signals_logic_resolve;
#[cfg(test)]
mod test_python;
#[cfg(test)]
mod test_range;

mod client_topics;
mod client_websocket_push;
mod shared_statuses;
#[cfg(test)]
mod test_connections;
mod test_fixtures;

mod client_proxy;

mod client_metadata;
mod client_tpt;
mod utils_patch;

mod cli_program_listen;
mod cli_program_server;
mod cli_program_stats;
mod cli_program_subscribe;
mod client_history;
mod client_index;
mod client_link_benchmark;
mod client_publish;
mod client_verbs;
mod client_websocket_read;
mod interface;
mod signals_logic_call;
mod test_requests;
mod utils_queues;
mod utils_time;
mod utils_websocket;
