use eui48::MacAddress;
use serde_json::Value;
use std::{collections::HashMap, str::FromStr};

use crate::cgw_errors::{Error, Result};

use crate::cgw_ucentral_parser::{
    CGWUCentralEvent, CGWUCentralEventLog, CGWUCentralEventState, CGWUCentralEventStateClients,
    CGWUCentralEventStateClientsData, CGWUCentralEventStateClientsType,
    CGWUCentralEventStateLLDPData, CGWUCentralEventStateLinks, CGWUCentralEventStatePort,
    CGWUCentralEventType, CGWUCentralJRPCMessage,
};

fn parse_lldp_data(
    data: &Value,
    links: &mut HashMap<CGWUCentralEventStatePort, Vec<CGWUCentralEventStateLinks>>,
    upstream_port: &Option<String>,
) -> Result<()> {
    if let Value::Object(map) = data {
        let directions = [
            map["upstream"]
                .as_object()
                .ok_or_else(|| Error::UCentralParser("Failed to parse upstream interfaces"))?,
            map["downstream"]
                .as_object()
                .ok_or_else(|| Error::UCentralParser("Failed to parse downstream interfaces"))?,
        ];

        for d in directions {
            for (key, value) in d {
                let data = value
                    .as_array()
                    .ok_or_else(|| Error::UCentralParser("Failed to cast value to array"))?[0]
                    .as_object()
                    .ok_or_else(|| Error::UCentralParser("Failed to cast array to object"))?;

                let local_port = key.to_string();
                let remote_serial = MacAddress::from_str(
                    data["mac"]
                        .as_str()
                        .ok_or_else(|| Error::UCentralParser("Failed to parse mac address"))?,
                )?;
                let remote_port = data["port"]
                    .as_str()
                    .ok_or_else(|| Error::UCentralParser("Failed to prase port"))?
                    .to_string();
                let is_downstream: bool = {
                    if let Some(ref port) = upstream_port {
                        *port != local_port
                    } else {
                        true
                    }
                };

                let local_port = CGWUCentralEventStatePort::PhysicalWiredPort(local_port);

                let clients_data = CGWUCentralEventStateLinks {
                    remote_serial,
                    remote_port,
                    is_downstream,
                };

                links.insert(local_port, vec![clients_data]);
            }
        }
    }

    Ok(())
}

/*
Example of "mac-forwarding-table" in json format:
...
"mac-forwarding-table": {
    "overflow": false,
    "Ethernet0":    {
        "1":    ["90:3c:b3:6a:e3:59"]
    },
...
*/
fn parse_fdb_data(
    data: &Value,
    links: &mut HashMap<CGWUCentralEventStatePort, Vec<CGWUCentralEventStateClients>>,
    upstream_port: &Option<String>,
) -> Result<()> {
    if let Value::Object(map) = data {
        for (k, v) in map.iter() {
            if let Value::Object(port) = v {
                let local_port = k.to_string();
                if let Some(ref upstream) = upstream_port {
                    if local_port == *upstream {
                        continue;
                    }
                }

                let local_port = CGWUCentralEventStatePort::PhysicalWiredPort(local_port);

                // We iterate on a per-port basis, means this is our first
                // iteration on this particular port, safe to create empty
                // vec and populate it later on.
                links.insert(local_port.clone(), Vec::new());

                let mut existing_vec = links.get_mut(&local_port);

                for (k, v) in port.iter() {
                    let vid = {
                        match u16::from_str(k.as_str()) {
                            Ok(v) => v,
                            Err(e) => {
                                warn!("Failed to convert vid {k} to u16! Error: {e}");
                                continue;
                            }
                        }
                    };

                    if let Value::Array(macs) = v {
                        for mac in macs.iter() {
                            let remote_serial =
                                MacAddress::from_str(mac.as_str().ok_or_else(|| {
                                    Error::UCentralParser("Failed to parse mac address")
                                })?)?;

                            let clients_data = CGWUCentralEventStateClients {
                                client_type: CGWUCentralEventStateClientsType::FDBClient(vid),
                                remote_serial,
                                remote_port: format!("<VLAN{}>", vid),
                                is_downstream: true,
                            };

                            if let Some(ref mut existing_vec) = existing_vec {
                                existing_vec.push(clients_data);
                            } else {
                                warn!("Unexpected: tried to push clients_data [{}:{}], while hashmap entry (key) for it does not exist!",
                                      local_port, clients_data.remote_port);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

pub fn cgw_ucentral_switch_parse_message(
    feature_topomap_enabled: bool,
    message: &str,
    timestamp: i64,
) -> Result<CGWUCentralEvent> {
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

    if map.contains_key("method") {
        let method = map["method"].as_str().ok_or_else(|| {
            warn!("Received JRPC <method> without params!");
            Error::UCentralParser("Received JRPC <method> without params")
        })?;
        if method == "log" {
            let params = map
                .get("params")
                .ok_or_else(|| Error::UCentralParser("Params are missing"))?;
            let serial = MacAddress::from_str(
                params["serial"]
                    .as_str()
                    .ok_or_else(|| Error::UCentralParser("Failed to parse serial from params"))?,
            )?;

            let log_event = CGWUCentralEvent {
                serial,
                evt_type: CGWUCentralEventType::Log(CGWUCentralEventLog {
                    serial,
                    log: params["log"].to_string(),
                    severity: serde_json::from_value(params["severity"].clone())?,
                }),
                decompressed: None,
            };

            return Ok(log_event);
        } else if method == "state" {
            let params = map
                .get("params")
                .ok_or_else(|| Error::UCentralParser("Params are missing"))?;

            if let Value::Object(state_map) = &params["state"] {
                let serial =
                    MacAddress::from_str(params["serial"].as_str().ok_or_else(|| {
                        Error::UCentralParser("Failed to parse serial from params")
                    })?)?;
                let mut upstream_port: Option<String> = None;
                let mut lldp_links: HashMap<
                    CGWUCentralEventStatePort,
                    Vec<CGWUCentralEventStateLinks>,
                > = HashMap::new();

                // We can reuse <clients> logic as used in AP, it's safe and OK
                // since under the hood FDB macs are basically
                // switch's <wired clients>, where underlying client type
                // (FDBClient) will have all additional met info, like VID
                let mut clients_links: HashMap<
                    CGWUCentralEventStatePort,
                    Vec<CGWUCentralEventStateClients>,
                > = HashMap::new();

                if feature_topomap_enabled {
                    if state_map.contains_key("default-gateway") {
                        if let Value::Array(default_gw) = &state_map["default-gateway"] {
                            if let Some(gw) = default_gw.first() {
                                if let Value::String(port) = &gw["out-port"] {
                                    upstream_port = Some(port.as_str().to_string());
                                }
                            }
                        }
                    }

                    if state_map.contains_key("lldp-peers") {
                        parse_lldp_data(&state_map["lldp-peers"], &mut lldp_links, &upstream_port)?;
                    }

                    if state_map.contains_key("mac-forwarding-table") {
                        parse_fdb_data(
                            &state_map["mac-forwarding-table"],
                            &mut clients_links,
                            &upstream_port,
                        )?;
                    }
                }

                let state_event = CGWUCentralEvent {
                    serial,
                    evt_type: CGWUCentralEventType::State(CGWUCentralEventState {
                        timestamp,
                        local_mac: serial,
                        lldp_data: CGWUCentralEventStateLLDPData { links: lldp_links },
                        clients_data: CGWUCentralEventStateClientsData {
                            links: clients_links,
                        },
                    }),
                    decompressed: None,
                };

                return Ok(state_event);
            }
        }
    } else if map.contains_key("result") {
        info!("Processing <result> JSONRPC msg");
        info!("{:?}", map);
        return Err(Error::UCentralParser(
            "Result handling is not yet implemented",
        ));
    }

    Err(Error::UCentralParser("Failed to parse event/method"))
}
