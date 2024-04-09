// TODO:
// 1) Renaming: use proper enums/structs and functions name
// 2) Move ucentral messages parser from connection server

use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CGWEventLog {
    pub serial: String,
    pub log: String,
    pub severity: i64,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CGWEventConnectParamsCaps {
    pub compatible: String,
    pub model: String,
    pub platform: String,
    pub label_macaddr: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CGWEventConnect {
    pub serial: String,
    pub firmware: String,
    pub uuid: u64,
    pub capabilities: CGWEventConnectParamsCaps,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum CGWEventType {
    Connect(CGWEventConnect),
    Log(CGWEventLog),
    Empty,
}

#[derive(Deserialize, Debug, Serialize)]
pub struct CGWDeviceChange {
    pub changed: String,
    pub old: String,
    pub new: String,
}

#[derive(Deserialize, Debug, Serialize)]
pub enum CGWToNBMessageType {
    InfrastructureDeviceCapabilitiesChanged,
}

#[derive(Deserialize, Debug, Serialize)]
pub struct CGWDeviceChangedData {
    #[serde(rename = "type")]
    pub msg_type: CGWToNBMessageType,
    pub infra_group_id: String,
    pub infra_group_infra_device: String,
    pub changes: Vec<CGWDeviceChange>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CGWEvent {
    pub serial: String,
    pub evt_type: CGWEventType,
}
