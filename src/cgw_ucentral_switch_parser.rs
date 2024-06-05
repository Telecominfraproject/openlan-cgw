use eui48::MacAddress;
use serde_json::Value;
use std::str::FromStr;

use crate::cgw_ucentral_parser::{
    CGWUCentralEvent, CGWUCentralEventLog, CGWUCentralEventState, CGWUCentralEventStateClientsData,
    CGWUCentralEventStateLLDPData, CGWUCentralEventStateLinks, CGWUCentralEventType,
    CGWUCentralJRPCMessage,
};

fn parse_lldp_data(data: &Value, upstream_port: Option<String>) -> Vec<CGWUCentralEventStateLinks> {
    let mut links: Vec<CGWUCentralEventStateLinks> = Vec::new();

    if let Value::Object(map) = data {
        let directions = [
            map["upstream"].as_object().unwrap(),
            map["downstream"].as_object().unwrap(),
        ];

        for d in directions {
            for (key, value) in d {
                let data = value.as_array().unwrap()[0].as_object().unwrap();

                let local_port = key.to_string();
                let remote_serial = MacAddress::from_str(data["mac"].as_str().unwrap()).unwrap();
                let remote_port = data["port"].as_str().unwrap().to_string();
                let is_downstream: bool = {
                    if let Some(ref port) = upstream_port {
                        *port != local_port
                    } else {
                        true
                    }
                };

                links.push(CGWUCentralEventStateLinks {
                    local_port,
                    remote_serial,
                    remote_port,
                    is_downstream,
                });
            }
        }
    }

    links
}

pub fn cgw_ucentral_switch_parse_message(
    message: &String,
    timestamp: i64,
) -> Result<CGWUCentralEvent, &'static str> {
    let map: CGWUCentralJRPCMessage = match serde_json::from_str(message) {
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
            let serial = MacAddress::from_str(params["serial"].as_str().unwrap()).unwrap();

            let log_event = CGWUCentralEvent {
                serial,
                evt_type: CGWUCentralEventType::Log(CGWUCentralEventLog {
                    serial,
                    log: params["log"].to_string(),
                    severity: serde_json::from_value(params["severity"].clone()).unwrap(),
                }),
            };

            return Ok(log_event);
        } else if method == "state" {
            let params = map.get("params").expect("Params are missing");

            if let Value::Object(state_map) = &params["state"] {
                let serial = MacAddress::from_str(params["serial"].as_str().unwrap()).unwrap();
                let mut upstream_port: Option<String> = None;
                if state_map.contains_key("default-gateway") {
                    if let Value::Array(default_gw) = &state_map["default-gateway"] {
                        if let Some(gw) = default_gw.get(0) {
                            if let Value::String(port) = &gw["out-port"] {
                                upstream_port = Some(port.as_str().to_string());
                            }
                        }
                    }
                }

                let state_event = CGWUCentralEvent {
                    serial,
                    evt_type: CGWUCentralEventType::State(CGWUCentralEventState {
                        timestamp,
                        local_mac: serial,
                        lldp_data: CGWUCentralEventStateLLDPData {
                            links: parse_lldp_data(&state_map["lldp-peers"], upstream_port),
                        },
                        clients_data: CGWUCentralEventStateClientsData { links: Vec::new() },
                    }),
                };

                return Ok(state_event);
            }
        }
    } else if map.contains_key("result") {
        info!("Processing <result> JSONRPC msg");
        info!("{:?}", map);
        return Err("Result handling is not yet implemented");
    }

    Err("Failed to parse event/method")
}
