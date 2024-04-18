use std::str::FromStr;

use eui48::MacAddress;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio_tungstenite::tungstenite::protocol::Message;

use crate::{
    cgw_device::CGWDeviceType, cgw_ucentral_ap_parser::cgw_ucentral_ap_parse_message,
    cgw_ucentral_switch_parser::cgw_ucentral_switch_parse_message,
};

pub type CGWUcentralJRPCMessage = Map<String, Value>;

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CGWUCentralEventLog {
    pub serial: String,
    pub log: String,
    pub severity: i64,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CGWUCentralEventConnectParamsCaps {
    pub compatible: String,
    pub model: String,
    pub platform: String,
    pub label_macaddr: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CGWUCentralEventConnect {
    pub serial: String,
    pub firmware: String,
    pub uuid: u64,
    pub capabilities: CGWUCentralEventConnectParamsCaps,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum CGWUCentralEventType {
    Connect(CGWUCentralEventConnect),
    Log(CGWUCentralEventLog),
    Empty,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CGWUCentralEvent {
    pub serial: String,
    pub evt_type: CGWUCentralEventType,
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

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CGWUCentralCommandConfigure {
    pub serial: String,
    pub uuid: u64,
    pub when: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CGWUCentralCommandReboot {
    pub serial: String,
    pub when: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum CGWUCentralCommandType {
    Configure(CGWUCentralCommandConfigure),
    Reboot(CGWUCentralCommandReboot),
    Empty,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CGWUCentralCommand {
    pub serial: String,
    pub cmd_type: CGWUCentralCommandType,
    pub id: u64,
}

pub fn cgw_ucentral_parse_connect_event(
    message: Message,
) -> Result<CGWUCentralEvent, &'static str> {
    let msg = if let Ok(s) = message.into_text() {
        s
    } else {
        return Err("Message to string cast failed");
    };

    let map: CGWUcentralJRPCMessage = match serde_json::from_str(&msg) {
        Ok(m) => m,
        Err(e) => {
            error!("Failed to parse input json {e}");
            return Err("Failed to parse input json");
        }
    };

    if !map.contains_key("jsonrpc") {
        warn!("Received malformed JSONRPC msg");
        return Err("JSONRPC field is missing in message");
    }

    if !map.contains_key("method") {
        warn!("Received malformed JSONRPC msg");
        return Err("method field is missing in message");
    }

    if !map.contains_key("params") {
        warn!("Received JRPC <method> without params.");
        return Err("Received JRPC <method> without params");
    }

    let method = map["method"].as_str().unwrap();
    if method != "connect" {
        return Err("Device is not abiding the protocol: first message - CONNECT - expected");
    }

    let params = map.get("params").expect("Params are missing");
    let serial = MacAddress::from_str(params["serial"].as_str().unwrap())
        .unwrap()
        .to_hex_string()
        .to_uppercase();
    let firmware = params["firmware"].as_str().unwrap().to_string();
    let caps: CGWUCentralEventConnectParamsCaps =
        serde_json::from_value(params["capabilities"].clone()).unwrap();

    let event: CGWUCentralEvent = CGWUCentralEvent {
        serial: serial.clone(),
        evt_type: CGWUCentralEventType::Connect(CGWUCentralEventConnect {
            serial,
            firmware,
            uuid: 1,
            capabilities: caps,
        }),
    };

    return Ok(event);
}

pub fn cgw_ucentral_parse_command_message(
    message: Message,
) -> Result<CGWUCentralCommand, &'static str> {
    let msg = if let Ok(s) = message.into_text() {
        s
    } else {
        return Err("Message to string cast failed");
    };

    let map: CGWUcentralJRPCMessage = match serde_json::from_str(&msg) {
        Ok(m) => m,
        Err(e) => {
            error!("Failed to parse input json {e}");
            return Err("Failed to parse input json");
        }
    };

    if !map.contains_key("jsonrpc") {
        warn!("Received malformed JSONRPC msg");
        return Err("JSONRPC field is missing in message");
    }

    let method = map["method"].as_str().unwrap();
    if method != "connect" {
        return Err("Device is not abiding the protocol: first message - CONNECT - expected");
    }

    if method == "configure" {
        let params = map.get("params").expect("Params are missing");
        let serial = MacAddress::from_str(params["serial"].as_str().unwrap())
            .unwrap()
            .to_hex_string()
            .to_uppercase();
        let uuid = params["uuid"].as_u64().unwrap();
        let when = params["when"].to_string();
        let id = map
            .get("params")
            .expect("Params are missing")
            .as_u64()
            .unwrap();

        let config_command = CGWUCentralCommand {
            serial: serial.clone(),
            cmd_type: CGWUCentralCommandType::Configure(CGWUCentralCommandConfigure {
                serial,
                uuid,
                when,
            }),
            id,
        };

        return Ok(config_command);
    } else if method == "reboot" {
        let params = map.get("params").expect("Params are missing");
        let id = map.get("id").expect("Params are missing").as_u64().unwrap();
        let serial = MacAddress::from_str(params["serial"].as_str().unwrap())
            .unwrap()
            .to_hex_string()
            .to_uppercase();
        let when = params["when"].to_string();

        let reboot_command = CGWUCentralCommand {
            serial: serial.clone(),
            cmd_type: CGWUCentralCommandType::Reboot(CGWUCentralCommandReboot { serial, when }),
            id,
        };

        return Ok(reboot_command);
    }

    Err("Failed to parse command/method")
}

pub fn cgw_ucentral_event_parser(
    device_type: &CGWDeviceType,
    message: Message,
) -> Result<CGWUCentralEvent, &'static str> {
    match device_type {
        CGWDeviceType::CGWDeviceAP => cgw_ucentral_ap_parse_message(message),
        CGWDeviceType::CGWDeviceSwitch => cgw_ucentral_switch_parse_message(message),
    }
}
