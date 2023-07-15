use crate::{DataReady, RawData};
use derive_more::Constructor;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MsgClientToServer {
    RawData(RawData),
}

#[derive(Debug, Clone, Serialize, Deserialize, Constructor)]
pub struct ChannelInfoDesc {
    pub sequence: usize,
    pub time_inserted: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelInfo {
    pub queue_created: i64,
    pub num_total: usize,
    pub newest: Option<ChannelInfoDesc>,
    pub oldest: Option<ChannelInfoDesc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MsgServerToClient {
    DataReady(DataReady),
    ChannelInfo(ChannelInfo),
}
