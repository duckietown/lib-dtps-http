use bytes::Bytes;
use derive_more::Constructor;
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    DataReady,
    RawData,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MsgClientToServer {
    RawData(RawData),
}

#[derive(Debug, Clone, Serialize, Deserialize, Constructor, PartialEq)]
pub struct ChannelInfoDesc {
    pub sequence: usize,
    pub time_inserted: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChannelInfo {
    pub queue_created: i64,
    pub num_total: usize,
    pub newest: Option<ChannelInfoDesc>,
    pub oldest: Option<ChannelInfoDesc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Chunk {
    pub digest: String,
    pub i: usize,
    pub n: usize,
    pub index: usize,
    pub data: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MsgServerToClient {
    DataReady(DataReady),
    ChannelInfo(ChannelInfo),
    Chunk(Chunk),
}

#[cfg(test)]
mod test {
    use log::info;
    use serde_cbor::{
        from_slice,
        to_vec,
    };

    use crate::{
        info_with_info,
        init_logging,
    };

    // create a test that checks the serialization of MsgServerToClient to CBOR
    // and back
    use super::*;

    #[test]
    fn test_msg_server_to_client() {
        init_logging();
        let msg = MsgServerToClient::DataReady(DataReady {
            origin_node: "".to_string(),
            sequence: 456,
            time_inserted: 0,
            digest: "".to_string(),
            content_type: "".to_string(),
            content_length: 0,
            clocks: Default::default(),
            availability: vec![],
            unique_id: "".to_string(),
            chunks_arriving: 0,
        });
        let bytes = to_vec(&msg).unwrap();
        let value: serde_cbor::Value = serde_cbor::from_slice(&bytes).unwrap();
        info_with_info!("value: {:#?}", value);
        let msg2: MsgServerToClient = from_slice(&bytes).unwrap();
        assert_eq!(msg, msg2);
    }
}
