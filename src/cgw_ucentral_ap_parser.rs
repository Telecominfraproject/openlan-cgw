use std::str::FromStr;

use eui48::MacAddress;
use tokio_tungstenite::tungstenite::protocol::Message;

use crate::cgw_ucentral_parser::{
    CGWUCentralEvent, CGWUCentralEventConnect, CGWUCentralEventConnectParamsCaps,
    CGWUCentralEventLog, CGWUCentralEventReply, CGWUCentralEventType, CGWUcentralJRPCMessage,
};

pub fn cgw_ucentral_ap_parse_message(message: Message) -> Result<CGWUCentralEvent, &'static str> {
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
        if method == "log" {
            let params = map.get("params").expect("Params are missing");
            let mac_serial = MacAddress::from_str(params["serial"].as_str().unwrap()).unwrap();

            let log_event = CGWUCentralEvent {
                serial: mac_serial.to_hex_string().to_uppercase(),
                evt_type: CGWUCentralEventType::Log(CGWUCentralEventLog {
                    serial: mac_serial.to_hex_string().to_uppercase(),
                    log: params["log"].to_string(),
                    severity: serde_json::from_value(params["severity"].clone()).unwrap(),
                }),
            };

            return Ok(log_event);
        } else if method == "connect" {
            let params = map.get("params").expect("Params are missing");
            let serial = MacAddress::from_str(params["serial"].as_str().unwrap())
                .unwrap()
                .to_hex_string()
                .to_uppercase();
            let firmware = params["firmware"].as_str().unwrap().to_string();
            let caps: CGWUCentralEventConnectParamsCaps =
                serde_json::from_value(params["capabilities"].clone()).unwrap();

            let connect_event = CGWUCentralEvent {
                serial: serial.clone(),
                evt_type: CGWUCentralEventType::Connect(CGWUCentralEventConnect {
                    serial,
                    firmware,
                    uuid: 1,
                    capabilities: caps,
                }),
            };

            return Ok(connect_event);
        }
    } else if map.contains_key("result") {
        if !map.contains_key("id") {
            warn!("Received JRPC <result> without id.");
            return Err("Received JRPC <result> without id");
        }

        let id = map.get("id").unwrap().as_u64().unwrap();
        let reply_event = CGWUCentralEvent {
            serial: Default::default(),
            evt_type: CGWUCentralEventType::Reply(CGWUCentralEventReply { id }),
        };

        return Ok(reply_event);
    }

    Err("Failed to parse event/method")
}
