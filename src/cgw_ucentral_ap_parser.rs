use base64::prelude::*;
use eui48::MacAddress;
use flate2::read::ZlibDecoder;
use serde_json::{Map, Value};
use std::io::prelude::*;
use std::{collections::HashMap, str::FromStr};

use crate::cgw_errors::{Error, Result};

use crate::cgw_ucentral_parser::{
    CGWUCentralEvent, CGWUCentralEventConnect, CGWUCentralEventConnectParamsCaps,
    CGWUCentralEventLog, CGWUCentralEventRealtimeEvent, CGWUCentralEventRealtimeEventType,
    CGWUCentralEventRealtimeEventWClientJoin, CGWUCentralEventRealtimeEventWClientLeave,
    CGWUCentralEventReply, CGWUCentralEventState, CGWUCentralEventStateClients,
    CGWUCentralEventStateClientsData, CGWUCentralEventStateClientsType,
    CGWUCentralEventStateLLDPData, CGWUCentralEventStateLinks, CGWUCentralEventType,
    CGWUCentralJRPCMessage,
};

fn parse_lldp_data(
    lldp_peers: &Map<String, Value>,
    links: &mut Vec<CGWUCentralEventStateLinks>,
) -> Result<()> {
    let directions = [
        (
            lldp_peers["upstream"]
                .as_object()
                .ok_or_else(|| Error::UCentralParser("Failed to parse upstream interfaces"))?,
            false,
        ),
        (
            lldp_peers["downstream"]
                .as_object()
                .ok_or_else(|| Error::UCentralParser("Failed to parse downstream interfaces"))?,
            true,
        ),
    ];

    for (d, is_downstream) in directions {
        for (key, value) in d {
            let data = value
                .as_array()
                .ok_or_else(|| Error::UCentralParser("Failed to cast value to array"))?[0]
                .as_object()
                .ok_or_else(|| Error::UCentralParser("Failed to cast array to object"))?;
            let local_port = key.to_string().replace("WAN", "eth0").replace("LAN", "eth");
            let remote_serial = MacAddress::from_str(
                data["mac"]
                    .as_str()
                    .ok_or_else(|| Error::UCentralParser("Failed to parse mac address"))?,
            )?;
            let remote_port = data["port_id"]
                .as_str()
                .ok_or_else(|| Error::UCentralParser("Failed to prase port"))?
                .to_string();

            links.push(CGWUCentralEventStateLinks {
                local_port,
                remote_serial,
                remote_port,
                is_downstream,
            });
        }
    }

    Ok(())
}

fn parse_wireless_ssids_info(
    ssids: &Vec<Value>,
    ssids_map: &mut HashMap<String, (String, String)>,
) {
    for s in ssids {
        if let Value::Object(ssid) = s {
            if !ssid.contains_key("band")
                || !ssid.contains_key("bssid")
                || !ssid.contains_key("ssid")
            {
                continue;
            }

            let band_value = {
                if let Value::String(band_str) = &ssid["band"] {
                    band_str.clone()
                } else {
                    continue;
                }
            };
            let bssid_value = {
                if let Value::String(bssid_str) = &ssid["bssid"] {
                    bssid_str.clone()
                } else {
                    continue;
                }
            };
            let ssid_value = {
                if let Value::String(ssid_str) = &ssid["ssid"] {
                    ssid_str.clone()
                } else {
                    continue;
                }
            };

            ssids_map.insert(bssid_value, (ssid_value, band_value));
        }
    }
}

fn parse_wireless_clients_data(
    ssids: &Vec<Value>,
    links: &mut Vec<CGWUCentralEventStateClients>,
    upstream_ifaces: &[String],
    ssids_map: &HashMap<String, (String, String)>,
    timestamp: i64,
) -> Result<()> {
    for s in ssids {
        if let Value::Object(ssid) = s {
            let local_port = {
                if let Value::String(port) = &ssid["iface"] {
                    port.clone()
                } else {
                    warn!("Failed to retrieve local_port for {:?}", ssid);
                    continue;
                }
            };

            // Upstream WAN iface? Not supported
            if upstream_ifaces.iter().any(|i| *i == local_port) {
                continue;
            }

            if !ssid.contains_key("associations") {
                warn!("Failed to retrieve associations for {local_port}");
                continue;
            }

            if let Value::Array(associations) = &ssid["associations"] {
                for association in associations {
                    let mut ts = 0i64;
                    if let Value::Object(map) = association {
                        if !map.contains_key("station") || !map.contains_key("connected") {
                            continue;
                        }

                        let bssid_value = {
                            if let Value::String(bssid) = &map["bssid"] {
                                bssid.clone()
                            } else {
                                continue;
                            }
                        };

                        let (ssid, band) = {
                            if let Some(v) = ssids_map.get(&bssid_value) {
                                (v.0.clone(), v.1.clone())
                            } else {
                                warn!("Failed to get ssid/band value for {bssid_value}");
                                continue;
                            }
                        };

                        let remote_serial =
                            MacAddress::from_str(map["station"].as_str().ok_or_else(|| {
                                Error::UCentralParser("Failed to parse mac address")
                            })?)?;

                        // Time, for how long this connection's been associated
                        // with the AP that reports this data.
                        if let Value::Number(t) = &map["connected"] {
                            ts = t.as_i64().ok_or_else(|| {
                                Error::UCentralParser("Failed to parse timestamp")
                            })?;
                        }

                        links.push(CGWUCentralEventStateClients {
                            client_type: CGWUCentralEventStateClientsType::Wireless(
                                // Track timestamp of initial connection:
                                // if we receive state evt <now>, substract
                                // connected since from it, to get
                                // original connection timestamp.
                                timestamp - ts,
                                ssid,
                                band,
                            ),
                            local_port: local_port.clone(),
                            remote_serial,
                            // TODO: rework remote_port to have Band, RSSI, chan etc
                            // for an edge.
                            remote_port: "<Wireless-client>".to_string(),
                            is_downstream: true,
                        });
                    }
                }
            }
        }
    }

    Ok(())
}

fn parse_wired_clients_data(
    clients: &Vec<Value>,
    links: &mut Vec<CGWUCentralEventStateClients>,
    upstream_ifaces: &[String],
    timestamp: i64,
) -> Result<()> {
    for client in clients {
        let local_port = {
            if let Value::Array(arr) = &client["ports"] {
                match arr[0].as_str() {
                    Some(s) => s.to_string(),
                    None => {
                        warn!(
                            "Failed to get clients port string for {:?}, skipping",
                            client
                        );
                        continue;
                    }
                }
            } else {
                warn!("Failed to parse clients port for {:?}, skipping", client);
                continue;
            }
        };

        // Skip wireless clients info
        if local_port.contains("wlan") || local_port.contains("WLAN") {
            continue;
        }

        // TODO: W/A for now: ignore any upstream-reported clients,
        // because it includes ARP neighbours and clients.
        // The logic to process uplink neighbors properly should be
        // much more complicated and should be treated as a
        // separate case.
        // This logic also overlaps the uplink switch's neighbor
        // detection logic (LLDP, ARP, FDB etc), and building topo map
        // based purely on AP's input could result in invalid
        // map formation.
        if upstream_ifaces.iter().any(|i| *i == local_port) {
            continue;
        }

        let remote_serial = MacAddress::from_str(
            client["mac"]
                .as_str()
                .ok_or_else(|| Error::UCentralParser("Failed to parse mac address"))?,
        )?;

        links.push(CGWUCentralEventStateClients {
            // Wired clients don't have <connected since> data.
            // Treat <now> as latest connected ts.
            client_type: CGWUCentralEventStateClientsType::Wired(timestamp),
            local_port,
            remote_serial,
            // TODO: rework remote_port to have speed / duplex characteristics
            // for an edge.
            remote_port: "<Wired-client>".to_string(),
            is_downstream: true,
        });
    }

    Ok(())
}

fn parse_interface_data(
    interface: &Map<String, Value>,
    links: &mut Vec<CGWUCentralEventStateClients>,
    upstream_ifaces: &[String],
    timestamp: i64,
) -> Result<()> {
    if interface.contains_key("clients") {
        if let Value::Array(clients) = &interface["clients"] {
            parse_wired_clients_data(clients, links, upstream_ifaces, timestamp)?;
        }
    }

    if interface.contains_key("ssids") {
        let mut ssids_map: HashMap<String, (String, String)> = HashMap::new();
        if let Value::Array(ssids) = &interface["ssids"] {
            parse_wireless_ssids_info(ssids, &mut ssids_map);
            parse_wireless_clients_data(ssids, links, upstream_ifaces, &ssids_map, timestamp)?;
        }
    }

    Ok(())
}

fn parse_link_state_data(
    link_state: &Map<String, Value>,
    upstream_ifaces: &mut Vec<String>,
    downstream_ifaces: &mut Vec<String>,
) {
    if let Value::Object(upstream_obj) = &link_state["upstream"] {
        for (k, _v) in upstream_obj.iter() {
            upstream_ifaces.push(k.to_string());
        }
    }

    if let Value::Object(downstream_obj) = &link_state["downstream"] {
        for (k, _v) in downstream_obj.iter() {
            downstream_ifaces.push(k.to_string());
        }
    }
}

fn parse_state_event_data(
    feature_topomap_enabled: bool,
    map: CGWUCentralJRPCMessage,
    timestamp: i64,
) -> Result<CGWUCentralEvent> {
    if !map.contains_key("params") {
        return Err(Error::UCentralParser(
            "Invalid state event received: params is missing",
        ));
    }

    let params = &map["params"];

    if let Value::String(compressed_data) = &params["compress_64"] {
        let decoded_data = match BASE64_STANDARD.decode(compressed_data) {
            Ok(d) => d,
            Err(e) => {
                warn!("Failed to decode base64+zip state evt {e}");
                return Err(Error::UCentralParser(
                    "Failed to decode base64+zip state evt",
                ));
            }
        };
        let mut d = ZlibDecoder::new(&decoded_data[..]);
        let mut unzipped_data = String::new();
        if let Err(e) = d.read_to_string(&mut unzipped_data) {
            warn!("Failed to decompress decrypted state message {e}");
            return Err(Error::UCentralParser(
                "Failed to decompress decrypted state message",
            ));
        }

        let state_map: CGWUCentralJRPCMessage = match serde_json::from_str(&unzipped_data) {
            Ok(m) => m,
            Err(e) => {
                error!("Failed to parse input state message {e}");
                return Err(Error::UCentralParser("Failed to parse input state message"));
            }
        };

        let serial = MacAddress::from_str(
            state_map["serial"]
                .as_str()
                .ok_or_else(|| Error::UCentralParser("Failed to parse mac address"))?,
        )?;

        if let Value::Object(state_map) = &state_map["state"] {
            let mut lldp_links: Vec<CGWUCentralEventStateLinks> = Vec::new();
            let mut clients_links: Vec<CGWUCentralEventStateClients> = Vec::new();

            if feature_topomap_enabled {
                if state_map.contains_key("lldp-peers") {
                    if let Value::Object(v) = &state_map["lldp-peers"] {
                        parse_lldp_data(v, &mut lldp_links)?;
                    }
                }

                let mut upstream_ifaces: Vec<String> = Vec::new();
                let mut downstream_ifaces: Vec<String> = Vec::new();

                if state_map.contains_key("link-state") {
                    if let Value::Object(obj) = &state_map["link-state"] {
                        parse_link_state_data(obj, &mut upstream_ifaces, &mut downstream_ifaces);
                    }
                }

                if let Value::Array(arr) = &state_map["interfaces"] {
                    for interface in arr {
                        if let Value::Object(iface) = interface {
                            parse_interface_data(
                                iface,
                                &mut clients_links,
                                &upstream_ifaces,
                                timestamp,
                            )?;
                        }
                    }
                }
            }

            // Replace compressed data
            let mut origin_msg = map.clone();
            let params_value = match Value::from_str(unzipped_data.as_str()) {
                Ok(val) => val,
                Err(_e) => {
                    return Err(Error::ConnectionProcessor(
                        "Failed to cast decompressed message to JSON Value",
                    ));
                }
            };
            if let Some(value) = origin_msg.get_mut("params") {
                *value = params_value;
            }

            let kafka_msg = match serde_json::to_string(&origin_msg) {
                Ok(msg) => msg,
                Err(_e) => {
                    return Err(Error::ConnectionProcessor(
                        "Failed to create decompressed Event message",
                    ));
                }
            };

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
                decompressed: Some(kafka_msg),
            };

            return Ok(state_event);
        }

        return Err(Error::UCentralParser(
            "Parsed, decompressed state message but failed to find state object",
        ));
    } else if let Value::Object(state_map) = &params["state"] {
        let serial = MacAddress::from_str(
            params["serial"]
                .as_str()
                .ok_or_else(|| Error::UCentralParser("Failed to parse mac address"))?,
        )?;
        let mut lldp_links: Vec<CGWUCentralEventStateLinks> = Vec::new();
        let mut clients_links: Vec<CGWUCentralEventStateClients> = Vec::new();

        if state_map.contains_key("lldp-peers") {
            if let Value::Object(v) = &state_map["lldp-peers"] {
                parse_lldp_data(v, &mut lldp_links)?;
            }
        }

        let mut upstream_ifaces: Vec<String> = Vec::new();
        let mut downstream_ifaces: Vec<String> = Vec::new();

        if state_map.contains_key("link-state") {
            if let Value::Object(obj) = &state_map["link-state"] {
                parse_link_state_data(obj, &mut upstream_ifaces, &mut downstream_ifaces);
            }
        }

        if let Value::Array(arr) = &state_map["interfaces"] {
            for interface in arr {
                if let Value::Object(iface) = interface {
                    parse_interface_data(iface, &mut clients_links, &upstream_ifaces, timestamp)?;
                }
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

    Err(Error::UCentralParser("Failed to parse state event"))
}

fn parse_realtime_event_data(
    map: CGWUCentralJRPCMessage,
    timestamp: i64,
) -> Result<CGWUCentralEvent> {
    if !map.contains_key("params") {
        return Err(Error::UCentralParser(
            "Invalid event received: params is missing",
        ));
    }

    let params = &map["params"];

    let serial = {
        match params["serial"].as_str() {
            Some(v) => match MacAddress::from_str(v) {
                Ok(serial) => serial,
                Err(_) => {
                    return Err(Error::UCentralParser(
                        "Invalid event received: serial is an invalid MAC address",
                    ));
                }
            },
            None => {
                return Err(Error::UCentralParser(
                    "Invalid event received: serial is missing",
                ));
            }
        }
    };
    let events = match &params["data"]["event"] {
        Value::Array(events) => events,
        _ => {
            return Err(Error::UCentralParser(
                "Invalid event received: data:event missing",
            ));
        }
    };

    if events.len() < 2 {
        warn!("Received malformed event: number of event values < 2");
        return Err(Error::UCentralParser(
            "Received malformed event: number of event values < 2",
        ));
    }

    // We don't actually care about this TS, but it's a format-abiding requirement
    // put onto the AP's (we expect ts to be there).
    match &events[0] {
        Value::Number(ts) => {
            if ts.as_i64().is_none() {
                warn!("Received malformed event: missing timestamp");
                return Err(Error::UCentralParser(
                    "Received malformed event: missing timestamp",
                ));
            }
        }
        _ => {
            warn!("Received malformed event: missing timestamp");
            return Err(Error::UCentralParser(
                "Received malformed event: missing timestamp",
            ));
        }
    };

    let event_data = match &events[1] {
        Value::Object(v) => v,
        _ => {
            warn!("Received malformed event: missing timestamp");
            return Err(Error::UCentralParser(
                "Received malformed event: missing timestamp",
            ));
        }
    };

    if !event_data.contains_key("type") {
        warn!("Received malformed event: missing type");
        return Err(Error::UCentralParser(
            "Received malformed event: missing type",
        ));
    }

    let evt_type = match &event_data["type"] {
        Value::String(t) => t,
        _ => {
            warn!("Received malformed event: type is of wrongful underlying format/type");
            return Err(Error::UCentralParser(
                "Received malformed event: type is of wrongful underlying format/type",
            ));
        }
    };

    let evt_payload = match &event_data["payload"] {
        Value::Object(d) => d,
        _ => {
            warn!("Received malformed event: payload is of wrongful underlying format/type");
            return Err(Error::UCentralParser(
                "Received malformed event: payload is of wrongful underlying format/type",
            ));
        }
    };

    match evt_type.as_str() {
        "client.join" => {
            if !evt_payload.contains_key("band")
                || !evt_payload.contains_key("client")
                || !evt_payload.contains_key("ssid")
                || !evt_payload.contains_key("rssi")
                || !evt_payload.contains_key("channel")
            {
                warn!("Received malformed client.join event: band, rssi, ssid, channel and client are required");
                return Err(Error::UCentralParser("Received malformed client.join event: band, rssi, ssid, channel and client are required"));
            }

            let band = {
                match &evt_payload["band"] {
                    Value::String(s) => s,
                    _ => {
                        warn!("Received malformed client.join event: band is of wrongful underlying format/type");
                        return Err(Error::UCentralParser(
                            "Received malformed client.join event: band is of wrongful underlying format/type",
                        ));
                    }
                }
            };
            let client = {
                match &evt_payload["client"] {
                    Value::String(s) => match MacAddress::from_str(s.as_str()) {
                        Ok(v) => v,
                        Err(_) => {
                            warn!("Received malformed client.join event: client is a malformed MAC address");
                            return Err(Error::UCentralParser(
                                "Received malformed client.join event: client is a malformed MAC address",
                            ));
                        }
                    },
                    _ => {
                        warn!("Received malformed client.join event: client is of wrongful underlying format/type");
                        return Err(Error::UCentralParser(
                            "Received malformed client.join event: client is of wrongful underlying format/type",
                        ));
                    }
                }
            };
            let ssid = {
                match &evt_payload["ssid"] {
                    Value::String(s) => s,
                    _ => {
                        warn!("Received malformed client.join event: ssid is of wrongful underlying format/type");
                        return Err(Error::UCentralParser(
                            "Received malformed client.join event: ssid is of wrongful underlying format/type",
                            ));
                    }
                }
            };
            let rssi = {
                match &evt_payload["rssi"] {
                    Value::Number(s) => match s.as_i64() {
                        Some(v) => v,
                        None => {
                            warn!("Received malformed client.join event: rssi is NaN?");
                            return Err(Error::UCentralParser(
                                "Received malformed client.join event: rssi is NaN?",
                            ));
                        }
                    },
                    _ => {
                        warn!("Received malformed client.join event: rssi is of wrongful underlying format/type");
                        return Err(Error::UCentralParser(
                            "Received malformed client.join event: rssi is of wrongful underlying format/type",
                        ));
                    }
                }
            };
            let channel = {
                match &evt_payload["channel"] {
                    Value::Number(s) => match s.as_u64() {
                        Some(v) => v,
                        None => {
                            warn!("Received malformed client.join event: channel is NaN?");
                            return Err(Error::UCentralParser(
                                "Received malformed client.join event: channel is NaN?",
                            ));
                        }
                    },
                    _ => {
                        warn!("Received malformed client.join event: channel is of wrongful underlying format/type");
                        return Err(Error::UCentralParser(
                            "Received malformed client.join event: channel is of wrongful underlying format/type",
                        ));
                    }
                }
            };

            Ok(CGWUCentralEvent {
                serial,
                evt_type: CGWUCentralEventType::RealtimeEvent(CGWUCentralEventRealtimeEvent {
                    // For client.join the timestamp is <now> (whenever event's
                    // been received)
                    timestamp,
                    evt_type: CGWUCentralEventRealtimeEventType::WirelessClientJoin(
                        CGWUCentralEventRealtimeEventWClientJoin {
                            client,
                            band: band.to_string(),
                            ssid: ssid.to_string(),
                            rssi,
                            channel,
                        },
                    ),
                }),
                decompressed: None,
            })
        }
        "client.leave" => {
            if !evt_payload.contains_key("band")
                || !evt_payload.contains_key("client")
                || !evt_payload.contains_key("connected_time")
            {
                warn!("Received malformed client.leave event: client, band and connected_time is required");
                return Err(Error::UCentralParser("Received malformed client.leave event: client, band and connected_time is required"));
            }

            let band = {
                match &evt_payload["band"] {
                    Value::String(s) => s,
                    _ => {
                        warn!("Received malformed client.leave event: band is of wrongful underlying format/type");
                        return Err(Error::UCentralParser(
                            "Received malformed client.leave event: band is of wrongful underlying format/type",
                        ));
                    }
                }
            };
            let client = {
                match &evt_payload["client"] {
                    Value::String(s) => match MacAddress::from_str(s.as_str()) {
                        Ok(v) => v,
                        Err(_) => {
                            warn!("Received malformed client.leave event: client is a malformed MAC address");
                            return Err(Error::UCentralParser(
                                "Received malformed client.leave event: client is a malformed MAC address",
                            ));
                        }
                    },
                    _ => {
                        warn!("Received malformed client.leave event: client is of wrongful underlying format/type");
                        return Err(Error::UCentralParser(
                            "Received malformed client.leave event: client is of wrongful underlying format/type",
                        ));
                    }
                }
            };
            let connected_time = {
                match &evt_payload["connected_time"] {
                    Value::Number(s) => match s.as_i64() {
                        Some(v) => v,
                        None => {
                            warn!("Received malformed client.leave event: connected_time is NaN?");
                            return Err(Error::UCentralParser(
                                "Received malformed client.leave event: connected_time is NaN?",
                            ));
                        }
                    },
                    _ => {
                        warn!("Received malformed client.leave event: connected_time is of wrongful underlying format/type");
                        return Err(Error::UCentralParser(
                            "Received malformed client.leave event: connected_time is of wrongful underlying format/type",
                        ));
                    }
                }
            };
            Ok(CGWUCentralEvent {
                serial,
                evt_type: CGWUCentralEventType::RealtimeEvent(CGWUCentralEventRealtimeEvent {
                    // For client.leave the timestamp is: <now> substracted with
                    // the time device's been connected to the AP,
                    // which translates into:
                    // timestamp is equal to whenever connection's been
                    // established originally.
                    // And in case if <newer> client.join/state message
                    // is already registered within the CGW,
                    // this event will be simply dropped.
                    timestamp: timestamp - connected_time,
                    evt_type: CGWUCentralEventRealtimeEventType::WirelessClientLeave(
                        CGWUCentralEventRealtimeEventWClientLeave {
                            client,
                            band: band.to_string(),
                        },
                    ),
                }),
                decompressed: None,
            })
        }
        _ => {
            warn!("Received unknown event: {evt_type}");
            Err(Error::UCentralParser("Received unknown event"))
        }
    }
}

pub fn cgw_ucentral_ap_parse_message(
    feature_topomap_enabled: bool,
    message: &str,
    timestamp: i64,
) -> Result<CGWUCentralEvent> {
    let map: CGWUCentralJRPCMessage = match serde_json::from_str(message) {
        Ok(m) => m,
        Err(e) => {
            error!("Failed to parse input json {e}");
            return Err(Error::UCentralParser("Failed to parse input json"));
        }
    };

    if map.contains_key("method") {
        let method = map["method"].as_str().ok_or_else(|| {
            warn!("Received malformed JSONRPC msg");
            Error::UCentralParser("JSONRPC field is missing in message")
        })?;
        if method == "log" {
            let params = map.get("params").ok_or_else(|| {
                warn!("Received JRPC <method> without params.");
                Error::UCentralParser("Received JRPC <method> without params")
            })?;
            let serial = MacAddress::from_str(
                params["serial"]
                    .as_str()
                    .ok_or_else(|| Error::UCentralParser("Failed to parse seial from params"))?,
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
        } else if method == "connect" {
            let params = map
                .get("params")
                .ok_or_else(|| Error::UCentralParser("Params are missing"))?;
            let serial = MacAddress::from_str(
                params["serial"]
                    .as_str()
                    .ok_or_else(|| Error::UCentralParser("Failed to parse seial from params"))?,
            )?;
            let firmware = params["firmware"]
                .as_str()
                .ok_or_else(|| Error::UCentralParser("Failed to parse firmware from params"))?
                .to_string();
            let caps: CGWUCentralEventConnectParamsCaps =
                serde_json::from_value(params["capabilities"].clone())?;

            let connect_event = CGWUCentralEvent {
                serial,
                evt_type: CGWUCentralEventType::Connect(CGWUCentralEventConnect {
                    serial,
                    firmware,
                    uuid: 1,
                    capabilities: caps,
                }),
                decompressed: None,
            };

            return Ok(connect_event);
        } else if method == "state" {
            return parse_state_event_data(feature_topomap_enabled, map, timestamp);
        } else if method == "event" {
            if feature_topomap_enabled {
                return parse_realtime_event_data(map, timestamp);
            } else {
                return Err(Error::UCentralParser(
                    "Received unexpected event while topo map feature is disabled",
                ));
            }
        }
    } else if map.contains_key("result") {
        if !map.contains_key("id") {
            warn!("Received JRPC <result> without id.");
            return Err(Error::UCentralParser("Received JRPC <result> without id"));
        }

        let id = map["id"]
            .as_u64()
            .ok_or_else(|| Error::UCentralParser("Failed to parse id"))?;
        let reply_event = CGWUCentralEvent {
            serial: Default::default(),
            evt_type: CGWUCentralEventType::Reply(CGWUCentralEventReply { id }),
            decompressed: None,
        };

        return Ok(reply_event);
    }

    Err(Error::UCentralParser("Failed to parse event/method"))
}
