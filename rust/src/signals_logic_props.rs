use std::{
    cmp::{
        max,
        min,
    },
    collections::{
        BTreeMap,
        HashMap,
        HashSet,
    },
    fmt::Debug,
    path::PathBuf,
};

use anyhow::Context;
use async_recursion::async_recursion;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Local;
use futures::StreamExt;
use json_patch::{
    patch,
    Patch,
    PatchOperation,
};
use maplit::hashmap;
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};
use serde_cbor::{
    Value as CBORValue,
    Value::{
        Null as CBORNull,
        Text as CBORText,
    },
};
use tokio::{
    sync::broadcast::{
        error::RecvError,
        Receiver,
        Sender,
    },
    task::JoinHandle,
};

use crate::{
    client::get_rawdata_status,
    context,
    debug_with_info,
    divide_in_components,
    error_with_info,
    get_channel_info_message,
    get_dataready,
    get_inside,
    get_rawdata,
    is_prefix_of,
    merge_clocks,
    not_implemented,
    parse_url_ext,
    signals_logic::{
        DataProps,
        ResolveDataSingle,
        SourceComposition,
        TypeOFSource,
    },
    unescape_json_patch,
    utils,
    utils_cbor::putinside,
    warn_with_info,
    ChannelInfo,
    Clocks,
    ContentInfo,
    DTPSError,
    DataReady,
    DataSaved,
    ForwardingStep,
    InsertNotification,
    LinkBenchmark,
    OtherProxyInfo,
    ProxyJob,
    RawData,
    ResolvedData,
    ResolvedData::{
        NotAvailableYet,
        NotFound,
        Regular,
    },
    ServerState,
    ServerStateAccess,
    TopicName,
    TopicProperties,
    TopicReachabilityInternal,
    TopicRefAdd,
    TopicRefInternal,
    TopicsIndexInternal,
    TopicsIndexWire,
    TypeOfConnection,
    TypeOfConnection::Relative,
    CONTENT_TYPE_DTPS_INDEX_CBOR,
    CONTENT_TYPE_TOPIC_HISTORY_CBOR,
    DTPSR,
    REL_URL_META,
    TOPIC_PROXIED,
    URL_HISTORY,
};

impl DataProps for TypeOFSource {
    fn get_properties(&self) -> TopicProperties {
        match self {
            TypeOFSource::Digest(..) => TopicProperties {
                streamable: false,
                pushable: false,
                readable: true,
                immutable: true,
                has_history: false,
                patchable: false,
            },
            TypeOFSource::ForwardedQueue(q) => q.properties.clone(),
            TypeOFSource::OurQueue(_, props) => props.clone(),

            TypeOFSource::Compose(sc) => sc.get_properties(),
            TypeOFSource::Transformed(s, _) => s.get_properties(), // ok but when do we do the history?
            TypeOFSource::Deref(d) => d.get_properties(),
            TypeOFSource::OtherProxied(_) => {
                TopicProperties {
                    streamable: false,
                    pushable: false,
                    readable: true,
                    immutable: false, // maybe we can say it true sometime
                    has_history: false,
                    patchable: false,
                }
            }
            TypeOFSource::MountedDir(_, _, props) => props.clone(),
            TypeOFSource::MountedFile { properties, .. } => properties.clone(),
            TypeOFSource::Index(s) => s.get_properties(),
            TypeOFSource::Aliased(_, _) => {
                todo!("get_properties for {self:#?} with {self:?}")
            }
            TypeOFSource::History(_) => {
                TopicProperties {
                    streamable: false,
                    pushable: false,
                    readable: true,
                    immutable: false, // maybe we can say it true sometime
                    has_history: false,
                    patchable: false,
                }
            }
        }
    }
}

impl DataProps for SourceComposition {
    fn get_properties(&self) -> TopicProperties {
        let pushable = false;

        let mut immutable = true;
        let mut streamable = false;
        let mut readable = true;
        let has_history = false;
        let patchable = true;

        for (_k, v) in self.compose.iter() {
            let p = v.get_properties();

            // immutable: ALL immutable
            immutable = immutable && p.immutable;
            // streamable: ANY streamable
            streamable = streamable || p.streamable;
            // readable: ALL readable
            readable = readable && p.readable;
            // patchable: ANY patchable
            // patchable = patchable || p.patchable;
        }

        TopicProperties {
            streamable,
            pushable,
            readable,
            immutable,
            has_history,
            patchable,
        }
    }
}
