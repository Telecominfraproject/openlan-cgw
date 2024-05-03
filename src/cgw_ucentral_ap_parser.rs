use std::str::FromStr;

use base64::prelude::*;
use eui48::MacAddress;
use flate2::read::ZlibDecoder;
use serde_json::Value;
use std::io::prelude::*;

use crate::cgw_ucentral_parser::{
    CGWUCentralEvent, CGWUCentralEventConnect, CGWUCentralEventConnectParamsCaps,
    CGWUCentralEventLog, CGWUCentralEventReply, CGWUCentralEventState,
    CGWUCentralEventStateLLDPData, CGWUCentralEventStateLLDPDataLinks, CGWUCentralEventType,
    CGWUcentralJRPCMessage,
};

fn parse_lldp_data(data: &Value) -> Vec<CGWUCentralEventStateLLDPDataLinks> {
    let mut links: Vec<CGWUCentralEventStateLLDPDataLinks> = Vec::new();

    if let Value::Object(map) = data {
        let directions = [
            (map["upstream"].as_object().unwrap(), false),
            (map["downstream"].as_object().unwrap(), true),
        ];

        for (d, is_downstream) in directions {
            for (key, value) in d {
                let data = value.as_array().unwrap()[0].as_object().unwrap();

                let local_port = key.to_string().replace("WAN", "eth0").replace("LAN", "eth");
                let remote_mac = MacAddress::from_str(data["mac"].as_str().unwrap()).unwrap();
                let remote_port = data["port_id"].as_str().unwrap().to_string();

                links.push(CGWUCentralEventStateLLDPDataLinks {
                    local_port,
                    remote_mac,
                    remote_port,
                    is_downstream,
                });
            }
        }
    }

    links
}

pub fn cgw_ucentral_ap_parse_message(message: &String) -> Result<CGWUCentralEvent, &'static str> {
    let map: CGWUcentralJRPCMessage = match serde_json::from_str(message) {
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
        } else if method == "state" {
            let params = map.get("params").expect("Params are missing");

            if let Value::String(compressed_data) = &params["compress_64"] {
                let decoded_data = match BASE64_STANDARD.decode(compressed_data) {
                    Ok(d) => d,
                    Err(e) => {
                        warn!("Failed to decode base64+zip state evt {e}");
                        return Err("Failed to decode base64+zip state evt");
                    }
                };
                let mut d = ZlibDecoder::new(&decoded_data[..]);
                let mut unzipped_data = String::new();
                if let Err(e) = d.read_to_string(&mut unzipped_data) {
                    warn!("Failed to decompress decrypted state message {e}");
                    return Err("Failed to decompress decrypted state message");
                }

                let state_map: CGWUcentralJRPCMessage = match serde_json::from_str(&unzipped_data) {
                    Ok(m) => m,
                    Err(e) => {
                        error!("Failed to parse input state message {e}");
                        return Err("Failed to parse input state message");
                    }
                };

                let serial = MacAddress::from_str(state_map["serial"].as_str().unwrap()).unwrap();

                if let Value::Object(state_map) = &state_map["state"] {
                    let state_event = CGWUCentralEvent {
                        serial: serial.to_hex_string().to_uppercase(),
                        evt_type: CGWUCentralEventType::State(CGWUCentralEventState {
                            lldp_data: CGWUCentralEventStateLLDPData {
                                local_mac: serial,
                                links: parse_lldp_data(&state_map["lldp-peers"]),
                            },
                        }),
                    };

                    return Ok(state_event);
                }

                return Err("Parsed, decompressed state message but failed to find state object");
            } else if let Value::Object(state_map) = &params["state"] {
                let serial = MacAddress::from_str(state_map["serial"].as_str().unwrap()).unwrap();

                let state_event = CGWUCentralEvent {
                    serial: serial.to_hex_string().to_uppercase(),
                    evt_type: CGWUCentralEventType::State(CGWUCentralEventState {
                        lldp_data: CGWUCentralEventStateLLDPData {
                            local_mac: serial,
                            links: parse_lldp_data(&state_map["lldp-peers"]),
                        },
                    }),
                };

                return Ok(state_event);
            }
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
