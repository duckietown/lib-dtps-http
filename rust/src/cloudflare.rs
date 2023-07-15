use serde::{Deserialize, Serialize};

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CloudflareTunnel {
    pub AccountTag: String,
    pub TunnelSecret: String,
    pub TunnelID: String,
    pub TunnelName: String,
}
