use std::str::FromStr;

use eui48::MacAddress;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio_tungstenite::tungstenite::protocol::Message;

use crate::cgw_errors::{Error, Result};

use crate::{
    cgw_device::CGWDeviceType, cgw_ucentral_ap_parser::cgw_ucentral_ap_parse_message,
    cgw_ucentral_switch_parser::cgw_ucentral_switch_parse_message,
};

pub type CGWUCentralJRPCMessage = Map<String, Value>;

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CGWUCentralEventLog {
    pub serial: MacAddress,
    pub log: String,
    pub severity: i64,
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CGWUCentralEventConnectParamsCaps {
    pub compatible: String,
    pub model: String,
    pub platform: String,
    pub label_macaddr: String,
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CGWUCentralEventConnect {
    pub serial: MacAddress,
    pub firmware: String,
    pub uuid: u64,
    pub capabilities: CGWUCentralEventConnectParamsCaps,
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CGWUCentralEventStateLinks {
    pub local_port: String,
    #[serde(skip)]
    pub remote_serial: MacAddress,
    pub remote_port: String,
    pub is_downstream: bool,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum CGWUCentralEventStateClientsType {
    // Timestamp
    Wired(i64),
    // Timestamp, Ssid, Band
    Wireless(i64, String, String),
    // VID
    FDBClient(u16),
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct CGWUCentralEventStateClients {
    pub client_type: CGWUCentralEventStateClientsType,
    pub local_port: String,
    #[serde(skip)]
    pub remote_serial: MacAddress,
    pub remote_port: String,
    pub is_downstream: bool,
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CGWUCentralEventStateLLDPData {
    // links reported by the device:
    pub links: Vec<CGWUCentralEventStateLinks>,
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CGWUCentralEventStateClientsData {
    // links reported by the device (wired and wireless):
    pub links: Vec<CGWUCentralEventStateClients>,
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CGWUCentralEventState {
    // mac address of the device reporting the state evt
    pub local_mac: MacAddress,
    pub timestamp: i64,
    pub lldp_data: CGWUCentralEventStateLLDPData,
    pub clients_data: CGWUCentralEventStateClientsData,
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CGWUCentralEventReply {
    pub id: u64,
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CGWUCentralEventRealtimeEventWClientJoin {
    pub client: MacAddress,
    pub band: String,
    pub ssid: String,
    pub rssi: i64,
    pub channel: u64,
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CGWUCentralEventRealtimeEventWClientLeave {
    pub client: MacAddress,
    pub band: String,
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub enum CGWUCentralEventRealtimeEventType {
    WirelessClientJoin(CGWUCentralEventRealtimeEventWClientJoin),
    WirelessClientLeave(CGWUCentralEventRealtimeEventWClientLeave),
    #[default]
    None,
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CGWUCentralEventRealtimeEvent {
    pub evt_type: CGWUCentralEventRealtimeEventType,
    pub timestamp: i64,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum CGWUCentralEventType {
    Connect(CGWUCentralEventConnect),
    State(CGWUCentralEventState),
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
    RealtimeEvent(CGWUCentralEventRealtimeEvent),
    Reply(CGWUCentralEventReply),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CGWUCentralEvent {
    pub serial: MacAddress,
    pub evt_type: CGWUCentralEventType,
    pub decompressed: Option<String>,
}

#[derive(Deserialize, Debug, Serialize)]
pub struct CGWDeviceChange {
    pub changed: String,
    pub old: String,
    pub new: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
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
    #[default]
    None,
}

impl FromStr for CGWUCentralCommandType {
    type Err = ();

    fn from_str(command: &str) -> std::result::Result<Self, Self::Err> {
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
            "None" => Ok(CGWUCentralCommandType::None),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct CGWUCentralCommand {
    pub serial: MacAddress,
    pub cmd_type: CGWUCentralCommandType,
    pub id: u64,
}

pub fn cgw_ucentral_parse_connect_event(message: Message) -> Result<CGWUCentralEvent> {
    let msg = if let Ok(s) = message.into_text() {
        s
    } else {
        return Err(Error::UCentralParser("Message to string cast failed"));
    };

    let map: CGWUCentralJRPCMessage = serde_json::from_str(&msg)?;
    if !map.contains_key("jsonrpc") {
        warn!("Received malformed JSONRPC msg");
        return Err(Error::UCentralParser("JSONRPC field is missing in message"));
    }

    let method = map["method"].as_str().ok_or_else(|| {
        warn!("Received malformed JSONRPC msg.");
        Error::UCentralParser("method field is missing in message")
    })?;

    if method != "connect" {
        return Err(Error::UCentralParser(
            "Device is not abiding the protocol: first message - CONNECT - expected",
        ));
    }

    let params = map.get("params").ok_or_else(|| {
        warn!("Received JSONRPC <method> without params");
        Error::UCentralParser("Received JSONRPC <method> without params")
    })?;

    let serial = MacAddress::from_str(
        params["serial"]
            .as_str()
            .ok_or_else(|| Error::UCentralParser("Failed to parse serial from params"))?,
    )?;
    let firmware = params["firmware"]
        .as_str()
        .ok_or_else(|| Error::UCentralParser("Failed to parse firmware from params"))?
        .to_string();
    let caps: CGWUCentralEventConnectParamsCaps =
        serde_json::from_value(params["capabilities"].clone())?;

    let event: CGWUCentralEvent = CGWUCentralEvent {
        serial,
        evt_type: CGWUCentralEventType::Connect(CGWUCentralEventConnect {
            serial,
            firmware,
            uuid: 1,
            capabilities: caps,
        }),
        decompressed: None,
    };

    Ok(event)
}

pub fn cgw_ucentral_parse_command_message(message: &str) -> Result<CGWUCentralCommand> {
    let map: CGWUCentralJRPCMessage = match serde_json::from_str(message) {
        Ok(m) => m,
        Err(e) => {
            error!("Failed to parse input json {e}");
            return Err(Error::UCentralParser("Failed to parse input json"));
        }
    };

    if !map.contains_key("jsonrpc") {
        warn!("Received malformed JSONRPC msg");
        return Err(Error::UCentralParser("JSONRPC field is missing in message"));
    }

    if !map.contains_key("method") {
        warn!("Received malformed JSONRPC msg");
        return Err(Error::UCentralParser("method field is missing in message"));
    }

    if !map.contains_key("params") {
        warn!("Received malformed JSONRPC msg");
        return Err(Error::UCentralParser("params field is missing in message"));
    }

    if !map.contains_key("id") {
        warn!("Received malformed JSONRPC msg");
        return Err(Error::UCentralParser("id field is missing in message"));
    }

    let method = map["method"]
        .as_str()
        .ok_or_else(|| Error::UCentralParser("Failed to parse method"))?;
    let command_type = CGWUCentralCommandType::from_str(method);
    match command_type {
        Ok(cmd_type) => {
            let params = map
                .get("params")
                .ok_or_else(|| Error::UCentralParser("Failed to parse params"))?;
            let serial = MacAddress::from_str(
                params["serial"]
                    .as_str()
                    .ok_or_else(|| Error::UCentralParser("Failed to parse serial from params"))?,
            )?;
            let id = map["id"]
                .as_u64()
                .ok_or_else(|| Error::UCentralParser("Failed to parse id from params"))?;
            let command = CGWUCentralCommand {
                cmd_type,
                serial,
                id,
            };

            Ok(command)
        }
        Err(_) => Err(Error::UCentralParser("Failed to parse command/method")),
    }
}

pub fn cgw_ucentral_event_parse(
    device_type: &CGWDeviceType,
    message: &str,
    timestamp: i64,
) -> Result<CGWUCentralEvent> {
    match device_type {
        CGWDeviceType::CGWDeviceAP => cgw_ucentral_ap_parse_message(message, timestamp),
        CGWDeviceType::CGWDeviceSwitch => cgw_ucentral_switch_parse_message(message, timestamp),
    }
}
