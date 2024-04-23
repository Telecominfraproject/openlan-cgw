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

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CGWUCentralEventReply {
    pub id: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum CGWUCentralEventType {
    Connect(CGWUCentralEventConnect),
    State,
    Healthcheck,
    Log(CGWUCentralEventLog),
    Event,
    Alarm,
    WifiScan,
    CrashLog,
    RebootLog,
    CfgPending,
    DeviceUpdate,
    Ping,
    Recovery,
    VenueBroadcast,
    Reply(CGWUCentralEventReply),
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

#[derive(Debug, Deserialize, Serialize)]
pub enum CGWUCentralCommandType {
    Configure,
    Reboot,
    Powercycle,
    Upgrade,
    Factory,
    Prm,
    Leds,
    Trace,
    Wifiscan,
    Request,
    Event,
    Telemetry,
    RemoteAccess,
    Ping,
    Script,
    CertUpdate,
    Transfer,
}

impl FromStr for CGWUCentralCommandType {
    type Err = ();

    fn from_str(command: &str) -> Result<Self, Self::Err> {
        match command {
            "configure" => Ok(CGWUCentralCommandType::Configure),
            "reboot" => Ok(CGWUCentralCommandType::Reboot),
            "powercycle" => Ok(CGWUCentralCommandType::Powercycle),
            "upgrade" => Ok(CGWUCentralCommandType::Upgrade),
            "factory" => Ok(CGWUCentralCommandType::Factory),
            "rrm" => Ok(CGWUCentralCommandType::Prm),
            "leds" => Ok(CGWUCentralCommandType::Leds),
            "trace" => Ok(CGWUCentralCommandType::Trace),
            "wifiscan" => Ok(CGWUCentralCommandType::Wifiscan),
            "request" => Ok(CGWUCentralCommandType::Request),
            "event" => Ok(CGWUCentralCommandType::Event),
            "telemetry" => Ok(CGWUCentralCommandType::Telemetry),
            "remote_access" => Ok(CGWUCentralCommandType::RemoteAccess),
            "ping" => Ok(CGWUCentralCommandType::Ping),
            "script" => Ok(CGWUCentralCommandType::Script),
            "certupdate" => Ok(CGWUCentralCommandType::CertUpdate),
            "transfer" => Ok(CGWUCentralCommandType::Transfer),
            _ => Err(()),
        }
    }
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

    let params = map.get("params").unwrap();
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

    if !map.contains_key("method") {
        warn!("Received malformed JSONRPC msg");
        return Err("method field is missing in message");
    }

    if !map.contains_key("params") {
        warn!("Received malformed JSONRPC msg");
        return Err("params field is missing in message");
    }

    if !map.contains_key("id") {
        warn!("Received malformed JSONRPC msg");
        return Err("id field is missing in message");
    }

    let method = map["method"].as_str().unwrap();
    let command_type = CGWUCentralCommandType::from_str(method);
    match command_type {
        Ok(cmd_type) => {
            let params = map.get("params").unwrap();
            let serial = MacAddress::from_str(params["serial"].as_str().unwrap())
                .unwrap()
                .to_hex_string()
                .to_uppercase();
            let id = map.get("id").unwrap().as_u64().unwrap();
            let command = CGWUCentralCommand {
                cmd_type,
                serial,
                id,
            };

            Ok(command)
        }
        Err(_) => {
            return Err("Failed to parse command/method");
        }
    }
}

pub fn cgw_ucentral_event_parse(
    device_type: &CGWDeviceType,
    message: Message,
) -> Result<CGWUCentralEvent, &'static str> {
    match device_type {
        CGWDeviceType::CGWDeviceAP => cgw_ucentral_ap_parse_message(message),
        CGWDeviceType::CGWDeviceSwitch => cgw_ucentral_switch_parse_message(message),
    }
}
