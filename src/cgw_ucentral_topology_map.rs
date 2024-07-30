use crate::{
    cgw_device::CGWDeviceType,
    cgw_ucentral_parser::{
        CGWUCentralEvent, CGWUCentralEventRealtimeEventType, CGWUCentralEventStateClientsType,
        CGWUCentralEventStateLinks, CGWUCentralEventStatePort, CGWUCentralEventType,
    },
};
use petgraph::dot::{Config, Dot};
use petgraph::{
    graph::{EdgeIndex, NodeIndex},
    stable_graph::StableGraph,
    visit::NodeRef,
    Direction,
};

use std::{collections::HashMap, fmt, str::FromStr, sync::Arc};
use tokio::sync::RwLock;

use eui48::MacAddress;

type ClientLastSeenTimestamp = i64;

// We have to track the 'origin' of any node we add to topo map,
// because deletion decision should be made on the following basis:
// - direct WSS connection should be always kept in the topo map,
//   and only erased when disconnect happens;
// - any nodes, that are added to topo map as lldp peers
//   should be deleted when the node that reported them gets deleted;
#[derive(Debug, Clone)]
enum CGWUCentralTopologyMapNodeOrigin {
    UCentralDevice(CGWDeviceType),
    StateLLDPPeer,
}

#[derive(Debug)]
struct CGWUCentralTopologyMapConnectionParams {
    mac: MacAddress,
    last_seen: ClientLastSeenTimestamp,
}

#[derive(Debug)]
struct CGWUCentralTopologyMapConnectionsData {
    // Infra node list = list of mac addresses (clients, fdb entires,
    // arp neighbors etc etc) reported on single port
    infra_nodes_list: Vec<CGWUCentralTopologyMapConnectionParams>,

    // (Optional) node parent that we know for sure we're directly
    // connected to (LLDP data, for example); Could be either
    // UCentral connected device, or lldp-peer that AP reports
    // (it's also possible that this peer will reconnect later-on
    // as a UCentral connect, but for some reason still haven't, and
    // AP already _sees_ it).
    // This is used whenever child node gets disconnected / removed:
    // we have to make sure we remove _connection_ between UCentral
    // node A (parent) with node B (child)
    // In case if parent get's removed though, there's no need to notify parent,
    // however. This is because whenever _child_ node sends a new state meessage,
    // connection to _parent_ node will be deduced from that data (basically,
    // update upon receiving new state message from AP for example).
    //
    // The _connection_ (edge) creation is parent-driven, however:
    // In the following example AP1 is connected to SW1:
    // SW1 < - > AP1
    // In case if AP1 sends state data before SW1 doest, the internal data about
    // wifi clients and so on will be populated, but the connection with SW1
    // and AP1 will only 'appear' in the topo map once the switch sends a
    // state message with explicitly stating, that it _sees_ the AP1 directly.
    //
    // The only exception is when AP reports uplink lldp peer and it's not
    // a UCentral device / still not connected:
    // then AP is responsible to firsly create lldp-peer-node, and report
    // it as AP's parent (and also clear / remove it upon AP disconnect event,
    // as the _knowledge_ about this lldp-peer-node comes _only_ from the AP
    // itself)
    parent_topology_node_mac: Option<MacAddress>,

    // In case if AP/Switch detects non-ucentral downstream LLDP peers,
    // it should track the list of them for later-on removal (in case if
    // this ucentral device get's disconnected).
    // Hashmap is used for O(1) access.
    child_lldp_nodes: HashMap<MacAddress, ()>,
}

#[derive(Debug)]
struct CGWUCentralTopologyMapConnections {
    links_list: HashMap<CGWUCentralEventStatePort, CGWUCentralTopologyMapConnectionsData>,
}

#[derive(Debug)]
struct CGWUCentralTopologyMapData {
    // Device nodes are only created upon receiving a new UCentral connection,
    // or LLDP peer info.
    topology_nodes: HashMap<
        MacAddress,
        (
            CGWUCentralTopologyMapNodeOrigin,
            CGWUCentralTopologyMapConnections,
        ),
    >,
}

#[derive(Debug)]
pub struct CGWUCentralTopologyMap {
    data: Arc<RwLock<CGWUCentralTopologyMapData>>,
}

lazy_static! {
    pub static ref CGW_UCENTRAL_TOPOLOGY_MAP: CGWUCentralTopologyMap = CGWUCentralTopologyMap {
        data: Arc::new(RwLock::new(CGWUCentralTopologyMapData {
            topology_nodes: HashMap::new(),
        }))
    };
}

impl CGWUCentralTopologyMap {
    pub fn get_ref() -> &'static Self {
        &CGW_UCENTRAL_TOPOLOGY_MAP
    }

    pub async fn insert_device(&self, topology_node_mac: &MacAddress, platform: &str) {
        let device_type = match CGWDeviceType::from_str(platform) {
            Ok(t) => t,
            Err(_) => {
                warn!(
                    "Tried to insert {} into tomo map, but failed to parse it's platform string",
                    topology_node_mac
                );
                return;
            }
        };
        let mut lock = self.data.write().await;
        let map_connections = CGWUCentralTopologyMapConnections {
            links_list: HashMap::new(),
        };

        lock.topology_nodes.insert(
            *topology_node_mac,
            (
                CGWUCentralTopologyMapNodeOrigin::UCentralDevice(device_type),
                map_connections,
            ),
        );
    }

    pub async fn remove_device(&self, topology_node_mac: &MacAddress) {
        let mut lock = self.data.write().await;
        Self::clear_related_nodes(&mut lock, topology_node_mac);
        lock.topology_nodes.remove(topology_node_mac);
    }

    pub async fn process_state_message(&self, device_type: &CGWDeviceType, evt: CGWUCentralEvent) {
        if let CGWUCentralEventType::State(s) = evt.evt_type {
            let topology_node_mac: &MacAddress = &s.local_mac;
            // Clear any related (child, or parent nodes we explicitly
            // created).
            // The child/parent node can be explicitly created,
            // if information about the node is only deduced from
            // lldp peer information, and the underlying node is not
            // a ucentral device.
            {
                let mut lock = self.data.write().await;
                Self::clear_related_nodes(&mut lock, topology_node_mac);
            }

            // Mac address of upstream node with local port we see the peer on
            let mut upstream_lldp_node: Option<(MacAddress, CGWUCentralEventStatePort)> = None;
            let mut downstream_lldp_nodes: HashMap<CGWUCentralEventStatePort, MacAddress> =
                HashMap::new();
            let mut nodes_to_create: Vec<(
                MacAddress,
                (
                    CGWUCentralTopologyMapNodeOrigin,
                    CGWUCentralTopologyMapConnections,
                ),
            )> = Vec::new();

            // Map connections that will be populated on behalf of device
            // that sent the state data itself.
            let mut map_connections = CGWUCentralTopologyMapConnections {
                links_list: HashMap::new(),
            };

            // Start with LLDP processing, as it's the backbone core
            // of deducing whether we have some umanaged (non-ucentral)
            // devices / nodes.
            for (local_port, links) in s.lldp_data.links {
                for link in links {
                    let mut lldp_peer_map_connections = CGWUCentralTopologyMapConnections {
                        links_list: HashMap::new(),
                    };
                    let mut lldp_peer_map_conn_data = CGWUCentralTopologyMapConnectionsData {
                            infra_nodes_list: Vec::new(),
                            parent_topology_node_mac: None,
                            child_lldp_nodes: HashMap::new(),
                    };

                    if link.is_downstream {
                        // We create this downstream node, which means we say that
                        // we're the <parent> node for this lldp-downstream-node
                        // to-be-created.
                        lldp_peer_map_conn_data.parent_topology_node_mac = Some(*topology_node_mac);
                        downstream_lldp_nodes.insert(local_port.clone(), link.remote_serial);
                    } else {
                        // Use only single upstream lldp peer (only 1 supported)
                        if let None = upstream_lldp_node {
                            debug!(
                                "{} Found parent (lldp) upstream node: {} on local port {:?}",
                                *topology_node_mac, link.remote_serial, local_port
                            );
                            upstream_lldp_node = Some((link.remote_serial, local_port.clone()));

                            // We create this upstream node, which means we say that
                            // we're the <child> node for this lldp-upstream-node
                            // to-be-created. We also populate links of this node
                            // with our - and only our single - mac address.
                            let mut child_lldp_nodes: HashMap<MacAddress, ()> = HashMap::new();

                            child_lldp_nodes.insert(*topology_node_mac, ());

                            lldp_peer_map_conn_data.child_lldp_nodes = child_lldp_nodes;

                            map_connections.links_list.insert(
                                CGWUCentralEventStatePort::PhysicalWiredPort(
                                    link.remote_port.clone(),
                                ),
                                lldp_peer_map_conn_data,
                            );
                        } else {
                            // Already found one upstream peer, skip this one;
                            continue;
                        }
                    }

                    nodes_to_create.push((
                        link.remote_serial,
                        (
                            CGWUCentralTopologyMapNodeOrigin::StateLLDPPeer,
                            lldp_peer_map_connections,
                        ),
                    ));
                }
            }

            for (local_port, links) in s.clients_data.links.into_iter() {
                let mut local_map_conn_data = CGWUCentralTopologyMapConnectionsData {
                        infra_nodes_list: Vec::new(),
                        parent_topology_node_mac: None,
                        child_lldp_nodes: HashMap::new(),
                };

                // We're processing the link reports for the port that is
                // also a port that <points> to upstream LLDP peer
                if let Some((lldp_peer_mac, ref lldp_peer_local_port)) = upstream_lldp_node {
                    if *lldp_peer_local_port == local_port {
                        // Upstream link's been already handled;
                        continue;
                        //local_map_conn_data.parent_topology_node_mac = Some(lldp_peer_mac);
                    }
                }

                // Will be skipped, in case if upstream processing took
                // place: it's not allowed by design to have the same
                // mac be an upstream as well as downstream peer;
                //
                // We're processing the link reports for the port that is
                // also a port that <points> to downstream LLDP peer.
                // This means, that any entries reported by this port
                // should be added to topo map on behalf of downstream
                // lldp peer, not this device directly.
                if let Some(lldp_peer_mac) = downstream_lldp_nodes.get(&local_port) {
                    let mut downstream_lldp_peer_map_conn_data = CGWUCentralTopologyMapConnectionsData {
                            infra_nodes_list: Vec::new(),
                            parent_topology_node_mac: None,
                            child_lldp_nodes: HashMap::new(),
                    };

                    // We made sure we filled downstream peer's
                    // links_list data with macs we see on this port.
                    // This means, that we don't need to add them
                    // again to our device's local links_list.
                    // We can continue processing next bulk of devices
                    // on different port.
                    for link_seen_on_port in links {

                        // Treat state timestamp as edge-creation timestamp only for
                        // events that do not report explicit connection timestamp
                        // (no association establishment timestamp for wired clients,
                        // however present for wireless for example).
                        let mut link_timestamp = s.timestamp;

                        downstream_lldp_peer_map_conn_data.infra_nodes_list.push(
                            CGWUCentralTopologyMapConnectionParams {
                                mac: link_seen_on_port.remote_serial,
                                last_seen: link_timestamp,
                            },
                            );
                    }

                    map_connections
                        .links_list
                        .insert("unknown_infra_unknown_port".to_string(), downstream_lldp_peer_map_conn_data);
                    local_map_conn_data.child_lldp_nodes.insert(lldp_peer_mac,());

                    // We 'filled' downstream peers links with data we see on
                    // this port (because, most-likely underlying peer is a
                    // switch, and these mac's originate from it).
                    // No need to fill <our> context with the same macs.
                    continue;
                }

                for link_seen_on_port in links {
                    // Treat state timestamp as edge-creation timestamp only for
                    // events that do not report explicit connection timestamp
                    // (no association establishment timestamp for wired clients,
                    // however present for wireless for example).
                    let mut link_timestamp = s.timestamp;

                    local_map_conn_data.infra_nodes_list.push(
                        CGWUCentralTopologyMapConnectionParams {
                            mac: link_seen_on_port.remote_serial,
                            last_seen: link_timestamp,
                        },
                    );
                }

                map_connections
                    .links_list
                    .insert(local_port, local_map_conn_data);
            }

            // Also add _this_ node that reported state to the list of added nodes;
            nodes_to_create.push((
                *topology_node_mac,
                (
                    CGWUCentralTopologyMapNodeOrigin::UCentralDevice(*device_type),
                    map_connections,
                ),
            ));

            let mut lock = self.data.write().await;
            for (node_mac, (node_origin, node_connections)) in nodes_to_create.into_iter() {
                // Unconditionally insert/replace our node;
                if node_mac == *topology_node_mac {
                    Self::add_node(&mut lock, node_mac, node_origin, node_connections);
                } else {
                    // Skip UCentral-device (not this device/node) controlled
                    // topomap entries.
                    // We only add nodes that we explicitly created.
                    // On the next iteration of state data our lldp-peer-partners
                    // will update topo map on their own, if we didn't here.
                    if let Some((existing_node_origin, _)) = lock.topology_nodes.get(&node_mac)
                    {
                        if let CGWUCentralTopologyMapNodeOrigin::UCentralDevice(_) =
                            existing_node_origin
                            {
                                continue;
                            }
                    }

                    // It's clear that this node is created by us in this iteration of
                    // lldp parsing, so it's safe to add it.
                    Self::add_node(&mut lock, node_mac, node_origin, node_connections);
                }
            }
        }
    }

    pub async fn process_device_topology_event(
        &self,
        _device_type: &CGWDeviceType,
        evt: CGWUCentralEvent,
    ) {
        // With realtime events, we want to make them absolutely synchronous:
        // Since we could possibly handle <Join> event for a MAC that was
        // previously present, but never received <Leave> for it,
        // we want to traverse through the whole topo map (including
        // infra node list) and find _which_ exactly node we should
        // remove from which device's links list.
        //
        // Same applies for <Leave> event: it's possible that it's a
        // late-leave message (client already joined another AP,
        // and we successfully handled that event), we might want
        // to check the timestamp of this message, with addition-timestamp
        // of existing mac inside the infra node list and then decide upon.

        //let mut lock = self.data.write().await;

        if let CGWUCentralEventType::RealtimeEvent(rt) = evt.evt_type {
            if let CGWUCentralEventRealtimeEventType::WirelessClientJoin(rt_j) = &rt.evt_type {
            } else if let CGWUCentralEventRealtimeEventType::WirelessClientLeave(rt_l) = rt.evt_type
            {
            }
        }
    }

    fn add_node(
        map_data: &mut CGWUCentralTopologyMapData,
        node_mac: MacAddress,
        origin: CGWUCentralTopologyMapNodeOrigin,
        connections: CGWUCentralTopologyMapConnections,
    ) {
        // This operation only covers non-ucentral-controlled devices,
        // so it shouldn't affect UCentral-controlled node's add perf;
        //
        // Special case check / handling:
        //              lldp                          lldp
        // UC_DEVICE_1 ------> <unknown lldp switch> ------> UC_DEVICE_2
        //
        // We are inserting the <unknown lldp switch>.
        // It's possible, that either UC_DEVICE_1 or UC_DEVICE_2 reported it,
        // but with current design - with direct node replace fast approach - it
        // means that we either lose parent or child relation.
        //
        // Try to restore it (if any) - basically, a merge operation.
        //
        // The downside is that we have to traverse through both child and
        // parent nodes, and it could be potentially a case when we have to
        // restore multiple relations: both parent and few child, consider
        // the following example:
        //
        // -- 1.UC_DEVICE_1 reports <unknown lldp switch>, topomap state:
        //      (parent)   lldp           (child)
        //    UC_DEVICE_1 ------> <unknown lldp switch>
        //
        // -- 2.Some UC_DEVICE_2 connects to CGW, and it's also connected
        //    to <unknown lldp switch>. Whenever UC_DEVICE_2 reports
        //    it's state message, we have to make sure the topomap state
        //    would be the following:
        //
        //      (parent)   lldp      (child, parent)     lldp   (child)
        //    UC_DEVICE_1 ------> <unknown lldp switch> ------> UC_DEVICE_2
        //
        // -- 3.We do this, by making sure we <preserve> the data about
        //    UC_DEVICE_1 <parent> connection in the <unknown lldp switch>
        //
        // -- 4.Same appliest for <preserving> child links;
        //    Consider some UC_DEVICE_3 connects, and it's also connected
        //    to the <unknown lldp switch>, the perfect topo map state
        //    should be:
        //
        //      (parent)   lldp      (child, parent)     lldp   (child)
        //    UC_DEVICE_1 ------> <unknown lldp switch> ------> UC_DEVICE_2
        //                                 |
        //                                 |             lldp   (child)
        //                                 -------------------> UC_DEVICE_3
        if let Some((old_node_origin, old_node_connections)) =
            map_data.topology_nodes.remove(&node_mac)
        {
            if let CGWUCentralTopologyMapNodeOrigin::StateLLDPPeer = old_node_origin {}
        }

        map_data
            .topology_nodes
            .insert(node_mac, (origin, connections));
    }

    fn clear_related_nodes(
        map_data: &mut CGWUCentralTopologyMapData,
        topology_node_mac: &MacAddress,
    ) {
        let mut nodes_to_remove: Vec<MacAddress> = Vec::new();

        // Stored to later-on find grandparent macs of this particular
        // child. Used to deduce whether <parent> should be removed alongside
        // this <topology_node_mac> that is being processed.
        let mut parent_node_macs: Vec<MacAddress> = Vec::new();

        // Stored to later-on find grandchild macs of this particular
        // child. Used to deduce whether <child> should be removed alongside
        // this <topology_node_mac> that is being processed.
        let mut child_node_macs: Vec<MacAddress> = Vec::new();

        // Stored grandchild and grandparent related nodes to current
        // <topology_node_mac> that is being cleared up.
        //
        // Grandchild hashmap: Key = parent, value = vec of child macs.
        let mut grandchild_node_macs: HashMap<MacAddress, Vec<MacAddress>> = HashMap::new();
        // Grandparent hashmap: Key = parent, value = vec of child macs.
        let mut grandparent_node_macs: HashMap<MacAddress, Vec<MacAddress>> = HashMap::new();

        // We found this node in our topo map:
        //   - clear child nodes (if this node <owns> them directly)
        //   - clear parent node (if this node <owns> it directly)
        //
        // First, try to fill parent / child macs into a vec for later
        // traversal / checks.
        if let Some((origin, connections)) = map_data.topology_nodes.get(topology_node_mac) {
            nodes_to_remove.push(*topology_node_mac);
            for link in connections.links_list.values() {
                for child_mac in link.child_lldp_nodes.keys() {
                    child_node_macs.push(*child_mac);
                }

                if let Some(parent_mac) = link.parent_topology_node_mac {
                    parent_node_macs.push(parent_mac);
                }
            }
        };

        // Traverse through child nodes, find grandchild nodes (if any).
        for child_mac in child_node_macs {
            // Special case check / handling:
            //   (parent)   lldp      (child, parent)     lldp   (child)
            // UC_DEVICE_1 ------> <unknown lldp switch> ------> UC_DEVICE_2
            //
            // For the UC_DEVICE_1, the child is <unknown lldp switch>,
            // and grandchildren of UC_DEVICE_1 is also a UCentral device,
            // (UC_DEVICE_2),
            // which means if we're clearing related nodes for
            // UC_DEVICE_1, we have to make sure we won't be deleting <all>
            // child nodes (including <unknown lldp switch> and
            // grandchildren UC_DEVICE_2).

            if let Some((_, child_connections)) = map_data.topology_nodes.get_mut(&child_mac) {
                for child_links in child_connections.links_list.values_mut() {
                    if let Some(child_parent_mac) = child_links.parent_topology_node_mac {
                        if child_parent_mac == *topology_node_mac {
                            child_links.parent_topology_node_mac = None;
                        }
                    }
                }

                for child_links in child_connections.links_list.values() {
                    //   (parent)   lldp      (child, parent)     lldp   (child)
                    // UC_DEVICE_1 ------> <unknown lldp switch> ------> UC_DEVICE_2
                    //                     ^^^^^^^^^^^^^^^^^^^^^
                    // <child_links> is pointing to <unknown lldp switch>

                    for grandchild_mac in child_links.child_lldp_nodes.keys() {
                        //   (parent)   lldp      (child, parent)     lldp   (child)
                        // UC_DEVICE_1 ------> <unknown lldp switch> ------> UC_DEVICE_2
                        //                                                   ^^^^^^^^^^^
                        // <grandchild_node> is pointing to UC_DEVICE_2

                        if let Some(v) = grandchild_node_macs.get_mut(&child_mac) {
                            v.push(*grandchild_mac);
                        } else {
                            grandchild_node_macs.insert(child_mac, vec![*grandchild_mac]);
                        }
                    }
                }
            }
        }

        // Traverse through parent nodes, find grandparent nodes (if any).
        for parent_mac in parent_node_macs {
            // Special case check / handling:
            //   (parent)   lldp      (child, parent)     lldp   (child)
            // UC_DEVICE_1 ------> <unknown lldp switch> ------> UC_DEVICE_2
            //
            // For the UC_DEVICE_2, the parent is <unknown lldp switch>
            // and grandparent in UC_DEVICE_1 which is also a UCentral
            // device which means, if we're clearing related nodes for
            // UC_DEVICE_2, we have to make sure we won't be deleting <all>
            // parent nodes (including <unknown lldp switch> and
            // grandparent UC_DEVICE_1).

            if let Some((_, parent_connections)) = map_data.topology_nodes.get(&parent_mac) {
                for parent_links in parent_connections.links_list.values() {
                    //   (parent)   lldp      (child, parent)     lldp   (child)
                    // UC_DEVICE_1 ------> <unknown lldp switch> ------> UC_DEVICE_2
                    //                     ^^^^^^^^^^^^^^^^^^^^^
                    // <parent_links> is pointing to <unknown lldp switch>

                    if let Some(grandparent_mac) = parent_links.parent_topology_node_mac {
                        //   (parent)   lldp      (child, parent)     lldp   (child)
                        // UC_DEVICE_1 ------> <unknown lldp switch> ------> UC_DEVICE_2
                        // ^^^^^^^^^^^
                        // <grandparent_mac> is pointing to UC_DEVICE_1
                        if let Some(v) = grandparent_node_macs.get_mut(&parent_mac) {
                            v.push(grandparent_mac);
                        } else {
                            grandparent_node_macs.insert(parent_mac.clone(), vec![grandparent_mac]);
                        }
                    }
                }
            }
        }

        for (child_mac, grandchild_macs) in grandchild_node_macs.iter() {
            let mut child_node_should_be_removed = true;

            for grandchild_mac in grandchild_macs.iter() {
                if let Some((grandchild_node_origin, _)) =
                    map_data.topology_nodes.get(&grandchild_mac)
                {
                    if let CGWUCentralTopologyMapNodeOrigin::UCentralDevice(_) =
                        grandchild_node_origin
                    {
                        //   (parent)   lldp      (child, parent)     lldp   (child)
                        // UC_DEVICE_1 ------> <unknown lldp switch> ------> UC_DEVICE_2
                        //                                                   ^^^^^^^^^^^
                        // <childs_parent_node_origin> is pointing to UC_DEVICE_2 origin
                        // and since it's a UCentral device, we can't delete
                        // the UC_DEVICE_1 child (<unknown lldp switch>),
                        // because the information about <unknown lldp switch>
                        // can be received from either UC_DEVICE_1 or UC_DEVICE_2.

                        debug!(
                            "Not removing child {} - has grandparent ucentral",
                            child_mac
                        );
                        child_node_should_be_removed = false;
                        break;
                    }
                }
            }

            //   (parent)   lldp      (child)
            // UC_DEVICE_1 ------> <unknown lldp switch>
            //                     ^^^^^^^^^^^^^^^^^^^^^
            // <unknown lldp switch> is going to be removed, as it has
            // only single direct UCentral parent (this) device.

            if child_node_should_be_removed {
                nodes_to_remove.push(*child_mac);
            }
        }

        for (parent_mac, grandparent_macs) in grandchild_node_macs.iter() {
            let mut parent_node_should_be_removed = true;

            for grandparent_mac in grandparent_macs.iter() {
                if let Some((grandparent_node_origin, _)) =
                    map_data.topology_nodes.get(&grandparent_mac)
                {
                    if let CGWUCentralTopologyMapNodeOrigin::UCentralDevice(_) =
                        grandparent_node_origin
                    {
                        //   (parent)   lldp      (child, parent)     lldp   (child)
                        // UC_DEVICE_1 ------> <unknown lldp switch> ------> UC_DEVICE_2
                        // ^^^^^^^^^^^
                        // <grandparent_node_origin> is pointing to UC_DEVICE_1 origin
                        // and since it's a UCentral device, we can't delete
                        // the UC_DEVICE_2 parent (<unknown lldp switch>),
                        // because the information about <unknown lldp switch>
                        // can be received from either UC_DEVICE_1 or UC_DEVICE_2.

                        debug!(
                            "Not removing parent {} - has grandparent ucentral",
                            parent_mac
                        );
                        parent_node_should_be_removed = false;

                        break;
                    }
                }
            }

            //   (parent)             lldp   (child)
            // <unknown lldp switch> ------> UC_DEVICE_1
            // ^^^^^^^^^^^^^^^^^^^^^
            //
            // <unknown lldp switch> is going to be removed, as it has
            // only single direct UCentral child (this) device.

            if parent_node_should_be_removed {
                nodes_to_remove.push(*parent_mac);
            }
        }

        for node_to_remove in nodes_to_remove {
            let mut node_should_be_removed = false;

            if let Some((origin, _)) = map_data.topology_nodes.get(&node_to_remove) {
                match origin {
                    CGWUCentralTopologyMapNodeOrigin::UCentralDevice(_) => (),
                    _ => node_should_be_removed = true,
                }
            }

            if node_should_be_removed {
                map_data.topology_nodes.remove(&node_to_remove);
            }
        }
    }

    pub async fn debug_dump_map(&self) {
        let mut lock = self.data.read().await;
        debug!("Topo: {:?}", lock.topology_nodes);
        /*
            let lock = self.data.read().await;
            let dotfmt = format!(
                "{:?}",
                Dot::with_attr_getters(
                    &lock.graph,
                    &[Config::NodeNoLabel, Config::EdgeNoLabel],
                    &|_, er| { format!("label = \"{}\"", er.weight()) },
                    &|_, nr| { format!("label = \"{}\" shape=\"record\"", nr.weight()) }
                )
            )
            .replace("digraph {", "digraph {\n\trankdir=LR;\n");
            debug!(
                "graph dump: {} {}\n{}",
                lock.node_idx_map.len(),
                lock.edge_idx_map.len(),
                dotfmt
            );
        */
    }
}
