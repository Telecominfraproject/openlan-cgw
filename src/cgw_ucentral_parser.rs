// TODO:
// 1) Renaming: use proper enums/structs and functions name
// 2) Move ucentral messages parser from connection server

use std::str::FromStr;

use serde_json::{Map, Value};

use eui48::MacAddress;
use serde::{Deserialize, Serialize};

use tokio_tungstenite::tungstenite::protocol::Message;

type CGWUcentralJRPCMessage = Map<String, Value>;

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
pub struct CGWUCentralEvent {
    pub serial: String,
    pub evt_type: CGWUCentralEventType,
}

pub fn cgw_parse_ucentral_event(map: &Map<String, Value>, method: &str) -> CGWUCentralEvent {
    if method == "log" {
        let params = map.get("params").expect("Params are missing");
        let mac_serial = MacAddress::from_str(params["serial"].as_str().unwrap()).unwrap();
        return CGWUCentralEvent {
            serial: mac_serial.to_hex_string().to_uppercase(),
            evt_type: CGWUCentralEventType::Log(CGWUCentralEventLog {
                serial: mac_serial.to_hex_string().to_uppercase(),
                log: params["log"].to_string(),
                severity: serde_json::from_value(params["severity"].clone()).unwrap(),
            }),
        };
    } else if method == "connect" {
        let params = map.get("params").expect("Params are missing");
        let mac_serial = MacAddress::from_str(params["serial"].as_str().unwrap()).unwrap();
        let label = MacAddress::from_str(params["capabilities"]["label_macaddr"].as_str().unwrap())
            .unwrap();
        return CGWUCentralEvent {
            serial: mac_serial.to_hex_string().to_uppercase(),
            evt_type: CGWUCentralEventType::Connect(CGWUCentralEventConnect {
                serial: mac_serial.to_hex_string().to_uppercase(),
                firmware: params["firmware"].to_string(),
                uuid: 1,
                capabilities: CGWUCentralEventConnectParamsCaps {
                    compatible: params["capabilities"]["compatible"].to_string(),
                    model: params["capabilities"]["model"].to_string(),
                    platform: params["capabilities"]["platform"].to_string(),
                    label_macaddr: label.to_hex_string().to_uppercase(),
                },
            }),
        };
    }

    CGWUCentralEvent {
        serial: String::from(""),
        evt_type: CGWUCentralEventType::Empty,
    }
}

pub async fn cgw_parse_ucentral_message(
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

    if map.contains_key("method") {
        if !map.contains_key("params") {
            warn!("Received JRPC <method> without params.");
            return Err("Received JRPC <method> without params");
        }

        let method = map["method"].as_str().unwrap();

        let event: CGWUCentralEvent = cgw_parse_ucentral_event(&map, method);

        match &event.evt_type {
            CGWUCentralEventType::Log(l) => {
                debug!("Received LOG evt from device {}: {}", l.serial, l.log);
            }
            CGWUCentralEventType::Connect(c) => {
                debug!(
                    "Received connect evt from device {}: type {}, fw {}",
                    c.serial, c.capabilities.platform, c.firmware
                );
            }
            _ => {
                warn!("received not yet implemented method {}", method);
                return Err("received not yet implemented method");
            }
        };

        return Ok(event);
    } else if map.contains_key("result") {
        info!("Processing <result> JSONRPC msg");
        info!("{:?}", map);
        return Err("Result handling is not yet implemented");
    }

    Err("Failed to parse event/method")
}
