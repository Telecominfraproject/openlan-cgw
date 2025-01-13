use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::str::FromStr;
use std::{collections::HashMap, fmt};

use eui48::MacAddress;

use jsonschema::JSONSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio_tungstenite::tungstenite::protocol::Message;
use url::Url;

use crate::cgw_app_args::{CGWValidationSchemaArgs, CGWValidationSchemaRef};
use crate::cgw_errors::{Error, Result};

use crate::{
    cgw_device::CGWDeviceType, cgw_ucentral_ap_parser::cgw_ucentral_ap_parse_message,
    cgw_ucentral_switch_parser::cgw_ucentral_switch_parse_message,
};

pub type CGWUCentralJRPCMessage = Map<String, Value>;

pub struct CGWUCentralConfigValidators {
    ap_schema: JSONSchema,
    switch_schema: JSONSchema,
}

impl CGWUCentralConfigValidators {
    pub fn new(uris: CGWValidationSchemaArgs) -> Result<CGWUCentralConfigValidators> {
        let ap_schema = cgw_initialize_json_validator(uris.ap_schema_uri)?;
        let switch_schema = cgw_initialize_json_validator(uris.switch_schema_uri)?;

        Ok(CGWUCentralConfigValidators {
            ap_schema,
            switch_schema,
        })
    }

    pub fn validate_config_message(&self, message: &str, device_type: CGWDeviceType) -> Result<()> {
        let msg: CGWUCentralJRPCMessage = match serde_json::from_str(message) {
            Ok(m) => m,
            Err(e) => {
                error!("Failed to parse input json {e}");
                return Err(Error::UCentralParser("Failed to parse input json"));
            }
        };

        let config = match msg.get("params") {
            Some(cfg) => cfg,
            None => {
                error!("Failed to get configs, invalid config message received");
                return Err(Error::UCentralParser(
                    "Failed to get configs, invalid config message received",
                ));
            }
        };

        let config = match config.get("config") {
            Some(cfg) => cfg,
            None => {
                error!("Failed to get config params, invalid config message received");
                return Err(Error::UCentralParser(
                    "Failed to get config params, invalid config message received",
                ));
            }
        };

        let result = match device_type {
            CGWDeviceType::CGWDeviceAP => self.ap_schema.validate(config),
            CGWDeviceType::CGWDeviceSwitch => self.switch_schema.validate(config),
            CGWDeviceType::CGWDeviceUnknown => {
                error!("Failed to validate configure message for device type unknown");
                return Err(Error::UCentralParser(
                    "Failed to validate configure message for device type unknown",
                ));
            }
        };

        let mut json_errors: String = String::new();
        if let Err(errors) = result {
            for error in errors {
                json_errors += &format!("JSON: Validation error: {}\n", error);
                json_errors += &format!("JSON: Instance path: {}\n", error.instance_path);
            }
            error!("{json_errors}");
            return Err(Error::UCentralValidator(json_errors));
        }

        Ok(())
    }
}

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
    #[serde(skip)]
    pub remote_serial: MacAddress,
    pub remote_port: String,
    pub is_downstream: bool,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum CGWUCentralEventStateClientsType {
    // Timestamp
    Wired(i64),
    // Timestamp
    Wireless(i64),
    // VID
    FDBClient(u16),
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct CGWUCentralEventStateClients {
    pub client_type: CGWUCentralEventStateClientsType,
    #[serde(skip)]
    pub remote_serial: MacAddress,
    pub remote_port: String,
    pub is_downstream: bool,
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CGWUCentralEventStateLLDPData {
    // links reported by the device:
    // local port (key), vector of links (value)
    pub links: HashMap<CGWUCentralEventStatePort, Vec<CGWUCentralEventStateLinks>>,
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CGWUCentralEventStateClientsData {
    // links reported by the device (wired and wireless):
    // Composed into hashmap of Port(key), and vector of links
    // seen on this particular port.
    pub links: HashMap<CGWUCentralEventStatePort, Vec<CGWUCentralEventStateClients>>,
}

// One 'slice' / part of edge (Mac + port);
// To make a proper complete edge two parts needed:
// SRC -> DST
#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub enum CGWUCentralEventStatePort {
    // Physical port description (port name)
    #[serde(skip)]
    PhysicalWiredPort(String),
    // Wireless port description (ssid, band)
    #[serde(skip)]
    WirelessPort(String, String),
}

impl fmt::Display for CGWUCentralEventStatePort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CGWUCentralEventStatePort::PhysicalWiredPort(port) => {
                write!(f, "{port}")
            }
            CGWUCentralEventStatePort::WirelessPort(ssid, band) => {
                write!(f, "WirelessClient({ssid},{band})")
            }
        }
    }
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
    Log,
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
    Unknown,
}

impl std::fmt::Display for CGWUCentralEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            CGWUCentralEventType::Connect(_) => write!(f, "connect"),
            CGWUCentralEventType::State(_) => write!(f, "state"),
            CGWUCentralEventType::Healthcheck => write!(f, "healthcheck"),
            CGWUCentralEventType::Log => write!(f, "log"),
            CGWUCentralEventType::Event => write!(f, "event"),
            CGWUCentralEventType::Alarm => write!(f, "alarm"),
            CGWUCentralEventType::WifiScan => write!(f, "wifiscan"),
            CGWUCentralEventType::CrashLog => write!(f, "crashlog"),
            CGWUCentralEventType::RebootLog => write!(f, "rebootLog"),
            CGWUCentralEventType::CfgPending => write!(f, "cfgpending"),
            CGWUCentralEventType::DeviceUpdate => write!(f, "deviceupdate"),
            CGWUCentralEventType::Ping => write!(f, "ping"),
            CGWUCentralEventType::Recovery => write!(f, "recovery"),
            CGWUCentralEventType::VenueBroadcast => write!(f, "venue_broadcast"),
            CGWUCentralEventType::RealtimeEvent(_) => {
                write!(f, "realtime_event")
            }
            CGWUCentralEventType::Reply(_) => write!(f, "reply"),
            CGWUCentralEventType::Unknown => write!(f, "unknown"),
        }
    }
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
    Rrm,
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
            "rrm" => Ok(CGWUCentralCommandType::Rrm),
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
        warn!("Received malformed JSONRPC msg!");
        return Err(Error::UCentralParser("JSONRPC field is missing in message"));
    }

    let method = map["method"].as_str().ok_or_else(|| {
        warn!("Received malformed JSONRPC msg!");
        Error::UCentralParser("method field is missing in message")
    })?;

    if method != "connect" {
        return Err(Error::UCentralParser(
            "Device is not abiding the protocol: first message - CONNECT - expected",
        ));
    }

    let params = map.get("params").ok_or_else(|| {
        warn!("Received JSONRPC <method> without params!");
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

    let uuid = params["uuid"]
        .as_u64()
        .ok_or_else(|| Error::UCentralParser("Failed to parse uuid from params"))?;

    let capabilities: CGWUCentralEventConnectParamsCaps =
        serde_json::from_value(params["capabilities"].clone())?;

    let event: CGWUCentralEvent = CGWUCentralEvent {
        serial,
        evt_type: CGWUCentralEventType::Connect(CGWUCentralEventConnect {
            serial,
            firmware,
            uuid,
            capabilities,
        }),
        decompressed: None,
    };

    Ok(event)
}

pub fn cgw_ucentral_parse_command_message(message: &str) -> Result<CGWUCentralCommand> {
    let map: CGWUCentralJRPCMessage = match serde_json::from_str(message) {
        Ok(m) => m,
        Err(e) => {
            error!("Failed to parse input json! Error: {e}");
            return Err(Error::UCentralParser("Failed to parse input json"));
        }
    };

    if !map.contains_key("jsonrpc") {
        warn!("Received malformed JSONRPC msg!");
        return Err(Error::UCentralParser("JSONRPC field is missing in message"));
    }

    if !map.contains_key("method") {
        warn!("Received malformed JSONRPC msg!");
        return Err(Error::UCentralParser("method field is missing in message"));
    }

    if !map.contains_key("params") {
        warn!("Received malformed JSONRPC msg!");
        return Err(Error::UCentralParser("params field is missing in message"));
    }

    if !map.contains_key("id") {
        warn!("Received malformed JSONRPC msg!");
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
    feature_topomap_enabled: bool,
    message: &str,
    timestamp: i64,
) -> Result<CGWUCentralEvent> {
    match device_type {
        CGWDeviceType::CGWDeviceAP => {
            cgw_ucentral_ap_parse_message(feature_topomap_enabled, message, timestamp)
        }
        CGWDeviceType::CGWDeviceSwitch => {
            cgw_ucentral_switch_parse_message(feature_topomap_enabled, message, timestamp)
        }
        CGWDeviceType::CGWDeviceUnknown => Err(Error::UCentralParser(
            "Failed to parse event message for device type unknown",
        )),
    }
}

fn cgw_get_json_validation_schema(schema_ref: CGWValidationSchemaRef) -> Result<serde_json::Value> {
    match schema_ref {
        CGWValidationSchemaRef::SchemaUri(url) => cgw_download_json_validation_schemas(url),
        CGWValidationSchemaRef::SchemaPath(path) => {
            cgw_load_json_validation_schemas(path.as_path())
        }
    }
}

fn cgw_download_json_validation_schemas(url: Url) -> Result<serde_json::Value> {
    let client = reqwest::blocking::Client::new();
    let response = match client.get(url.clone()).send() {
        Ok(r) => match r.text() {
            Ok(t) => t,
            Err(e) => {
                return Err(Error::UCentralValidator(format!(
                    "Failed to convert response from target URI {url} to text format: {e}"
                )));
            }
        },
        Err(e) => {
            return Err(Error::UCentralValidator(format!(
                "Failed to receive response from target URI {url}: {e}"
            )));
        }
    };

    match serde_json::from_str(&response) {
        Ok(json_schema) => Ok(json_schema),
        Err(e) => Err(Error::UCentralValidator(format!(
            "Failed to deserialize text response from target URI {url}: {e}"
        ))),
    }
}

fn cgw_load_json_validation_schemas(path: &Path) -> Result<serde_json::Value> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            return Err(Error::UCentralValidator(format!(
                "Failed to open TLS certificate file: {}. Error: {}",
                path.display(),
                e
            )));
        }
    };

    let reader = BufReader::new(file);
    match serde_json::from_reader(reader) {
        Ok(json_schema) => Ok(json_schema),
        Err(e) => Err(Error::UCentralValidator(format!(
            "Failed to read JSON schema from file {}! Error: {e}",
            path.display()
        ))),
    }
}

pub fn cgw_initialize_json_validator(schema_ref: CGWValidationSchemaRef) -> Result<JSONSchema> {
    let schema = match cgw_get_json_validation_schema(schema_ref) {
        Ok(sch) => sch,
        Err(e) => {
            return Err(Error::UCentralValidator(e.to_string()));
        }
    };

    match JSONSchema::compile(&schema) {
        Ok(json_schema) => Ok(json_schema),
        Err(e) => Err(Error::UCentralValidator(format!(
            "Failed to compile input schema to validation tree: {e}",
        ))),
    }
}
