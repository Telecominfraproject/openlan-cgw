use crate::{
    cgw_device::CGWDeviceType,
    cgw_ucentral_parser::{
        CGWUCentralEvent, CGWUCentralEventRealtimeEventType, CGWUCentralEventStateClientsType,
        CGWUCentralEventType,
    },
};
use petgraph::dot::{Config, Dot};
use petgraph::{
    graph::{EdgeIndex, NodeIndex},
    stable_graph::StableGraph,
    visit::NodeRef,
    Direction,
};

use std::{collections::HashMap, fmt, sync::Arc};
use tokio::sync::RwLock;

use eui48::MacAddress;

type WirelessClientBand = String;
type WirelessClientSsid = String;

// One 'slice' / part of edge (Mac + port);
// To make a proper complete edge two parts needed:
// SRC -> DST
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CGWUCentralTopologySubEdgePort {
    // Used in <SRC> subedge
    PhysicalWiredPort(String),
    WirelessPort,

    // Used in <DST> subedge
    // Wired client reported by AP (no dst port info available)
    // TODO: Duplex speed?
    WiredClient,
    // Wieless client reported by AP: SSID + Band
    WirelessClient(WirelessClientSsid, WirelessClientBand),

    WiredFDBClient(u16),
}

impl fmt::Display for CGWUCentralTopologySubEdgePort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CGWUCentralTopologySubEdgePort::PhysicalWiredPort(port) => {
                write!(f, "{port}")
            }
            CGWUCentralTopologySubEdgePort::WirelessPort => {
                write!(f, "WirelessPort")
            }
            CGWUCentralTopologySubEdgePort::WiredClient => {
                write!(f, "WiredClient")
            }
            CGWUCentralTopologySubEdgePort::WirelessClient(ssid, band) => {
                write!(f, "WirelessClient({ssid},{band})")
            }
            CGWUCentralTopologySubEdgePort::WiredFDBClient(vid) => {
                write!(f, "VLAN_{vid}")
            }
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CGWUCentralTopologySubEdge {
    pub serial: MacAddress,
    pub port: CGWUCentralTopologySubEdgePort,
}

// Complete edge consisting of SRC -> DST 'sub-edges'
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CGWUCentralTopologyEdge(CGWUCentralTopologySubEdge, CGWUCentralTopologySubEdge);

type EdgeCreationTimestamp = i64;

// We have to track the 'origin' of any node we add to topo map,
// because deletion decision should be made on the following basis:
// - direct WSS connection should be always kept in the topo map,
//   and only erased when disconnect happens;
// - any nodes, that are added to topo map as 'clients' (lldp peers,
//   wired and wireless clients, fdb info) should be deleted when the node
//   that reported them gets deleted;
//   however if 'client' node also exists in topo map, and is currently connected
//   to CGW (WSS), then it should be left untouched;
#[derive(Debug, Clone)]
enum CGWUCentralTopologyMapNodeOrigin {
    UCentralDevice,
    StateLLDPPeer,
    StateWiredWireless,
}

// We have to track the 'origin' of any edge we add to topo map,
// because deletion decision should be made on the following basis:
// - 'client.leave' should remove edge (only if join timestamp < leave timestamp);
// - 'client.join' should remove edge and create (potentially with new SRC node)
//   a new edge with device/node that reports this event.
//   Only, if join timestamp > <previous> join timestamp (state evt, realtime join evt)
#[derive(Debug, Clone)]
enum CGWUCentralTopologyMapEdgeOrigin {
    StateLLDPPeer,
    StateWiredWireless(EdgeCreationTimestamp),
}

#[derive(Debug)]
struct CGWUCentralTopologyMapData {
    node_idx_map: HashMap<MacAddress, (NodeIndex, CGWUCentralTopologyMapNodeOrigin)>,
    edge_idx_map: HashMap<CGWUCentralTopologyEdge, (EdgeIndex, CGWUCentralTopologyMapEdgeOrigin)>,
    graph: StableGraph<MacAddress, String>,
}

#[derive(Debug)]
pub struct CGWUCentralTopologyMap {
    data: Arc<RwLock<CGWUCentralTopologyMapData>>,
}

lazy_static! {
    pub static ref CGW_UCENTRAL_TOPOLOGY_MAP: CGWUCentralTopologyMap = CGWUCentralTopologyMap {
        data: Arc::new(RwLock::new(CGWUCentralTopologyMapData {
            node_idx_map: HashMap::new(),
            edge_idx_map: HashMap::new(),
            graph: StableGraph::new(),
        }))
    };
}

impl CGWUCentralTopologyMap {
    pub fn get_ref() -> &'static Self {
        &CGW_UCENTRAL_TOPOLOGY_MAP
    }

    pub async fn insert_device(&self, serial: &MacAddress) {
        let mut lock = self.data.write().await;
        Self::add_node(
            &mut lock,
            serial,
            CGWUCentralTopologyMapNodeOrigin::UCentralDevice,
        );
    }

    pub async fn remove_device(&self, serial: &MacAddress) {
        let mut lock = self.data.write().await;
        Self::remove_node(&mut lock, serial);
    }

    pub async fn process_state_message(&self, _device_type: &CGWDeviceType, evt: CGWUCentralEvent) {
        let mut lock = self.data.write().await;

        if let CGWUCentralEventType::State(s) = evt.evt_type {
            // To make sure any leftovers are handled, node that reports
            // state message is getting purged and recreated:
            // since state message hold <all> necessary information,
            // we can safely purge all edge info and recreate it from
            // the state message. Any missed / deleted by mistake
            // edges will appear on the next iteration of state / realtime event
            // processing.
            Self::remove_node(&mut lock, &s.local_mac);

            // Re-create node with origin being UCentralDevice, as this
            // device is directly connected to the CGW.
            Self::add_node(
                &mut lock,
                &s.local_mac,
                CGWUCentralTopologyMapNodeOrigin::UCentralDevice,
            );

            // Start with LLDP info processing
            for link in s.lldp_data.links {
                let subedge_src = CGWUCentralTopologySubEdge {
                    serial: s.local_mac,
                    port: CGWUCentralTopologySubEdgePort::PhysicalWiredPort(link.local_port),
                };
                let subedge_dst = CGWUCentralTopologySubEdge {
                    serial: link.remote_serial,
                    port: CGWUCentralTopologySubEdgePort::PhysicalWiredPort(link.remote_port),
                };

                // No duplicates can exists, since it's LLDP data
                // (both uCentral and underlying LLDP agents do not
                // support multiple LLDP clients over single link,
                // so it's not expect to have duplicates here).
                // Hence we have to try and remove any duplicates
                // we can find (either based on SRC or DST device's
                // subedge info.
                Self::remove_edge(&mut lock, &subedge_src);
                Self::remove_edge(&mut lock, &subedge_dst);

                // Any neighbour seen in LLDP is added to the graph.
                // Whenever parent (entity reporting the LLDP data)
                // get's removed - neighbour nodes and connected
                // edges will be purged.
                Self::add_node(
                    &mut lock,
                    &link.remote_serial,
                    CGWUCentralTopologyMapNodeOrigin::StateLLDPPeer,
                );

                if link.is_downstream {
                    Self::add_edge(
                        &mut lock,
                        CGWUCentralTopologyEdge(subedge_src, subedge_dst),
                        CGWUCentralTopologyMapEdgeOrigin::StateLLDPPeer,
                    );
                } else {
                    Self::add_edge(
                        &mut lock,
                        CGWUCentralTopologyEdge(subedge_dst, subedge_src),
                        CGWUCentralTopologyMapEdgeOrigin::StateLLDPPeer,
                    );
                }
            }

            // Clients data processing:
            // add all nodes seen in clients;
            // add new edges;
            for link in &s.clients_data.links {
                // Treat state timestamp as edge-creation timestamp only for
                // events that do not report explicit connection timestamp
                // (no association establishment timestamp for wired clients,
                // however present for wireless for example).
                let mut link_timestamp = s.timestamp;
                let (subedge_src, subedge_dst) = {
                    if let CGWUCentralEventStateClientsType::Wired(_) = link.client_type {
                        // We can safely skip <Wired Client> MAC from adding if
                        // it already exists (could be due to previous LLDP
                        // info processed).
                        if lock.node_idx_map.contains_key(&link.remote_serial) {
                            continue;
                        }
                        (
                            CGWUCentralTopologySubEdge {
                                serial: s.local_mac,
                                port: CGWUCentralTopologySubEdgePort::PhysicalWiredPort(
                                    link.local_port.clone(),
                                ),
                            },
                            CGWUCentralTopologySubEdge {
                                serial: link.remote_serial,
                                // TODO: Duplex speed?
                                port: CGWUCentralTopologySubEdgePort::WiredClient,
                            },
                        )
                    } else if let CGWUCentralEventStateClientsType::Wireless(ts, ssid, band) =
                        &link.client_type
                    {
                        // Since wireless association explicitly reports the
                        // timestamp for when link's been established, we can
                        // use this value reported from AP.
                        // For any other case (LLDP, wired), we use
                        // the event's base timestamp value;
                        link_timestamp = *ts;

                        (
                            CGWUCentralTopologySubEdge {
                                serial: s.local_mac,
                                port: CGWUCentralTopologySubEdgePort::WirelessPort,
                            },
                            CGWUCentralTopologySubEdge {
                                serial: link.remote_serial,
                                port: CGWUCentralTopologySubEdgePort::WirelessClient(
                                    ssid.clone(),
                                    band.clone(),
                                ),
                            },
                        )
                    } else if let CGWUCentralEventStateClientsType::FDBClient(vid) =
                        &link.client_type
                    {
                        // We can safely skip <FDBClient> MAC from adding if
                        // it already exists (could be due to previous LLDP
                        // info processed).
                        if lock.node_idx_map.contains_key(&link.remote_serial) {
                            continue;
                        }
                        (
                            CGWUCentralTopologySubEdge {
                                serial: s.local_mac,
                                port: CGWUCentralTopologySubEdgePort::PhysicalWiredPort(
                                    link.local_port.clone(),
                                ),
                            },
                            CGWUCentralTopologySubEdge {
                                serial: link.remote_serial,
                                port: CGWUCentralTopologySubEdgePort::WiredFDBClient(*vid),
                            },
                        )
                    } else {
                        continue;
                    }
                };

                // In case when client silently migrates from AP1 to AP2,
                // we have to explicitly remove that <edge> from AP1,
                // and 'migrate' it to AP2.
                // Do this only using <subedge_dst>, to make sure
                // we clear only unique destination (wifi client on band X,
                // for example) edge counterparts.
                // NOTE: deleting subedge will remove both SRC and DST
                // from map, as map stores them separately.
                Self::remove_edge(&mut lock, &subedge_dst);

                Self::add_node(
                    &mut lock,
                    &link.remote_serial,
                    CGWUCentralTopologyMapNodeOrigin::StateWiredWireless,
                );

                if link.is_downstream {
                    Self::add_edge(
                        &mut lock,
                        CGWUCentralTopologyEdge(subedge_src, subedge_dst),
                        CGWUCentralTopologyMapEdgeOrigin::StateWiredWireless(link_timestamp),
                    );
                } else {
                    Self::add_edge(
                        &mut lock,
                        CGWUCentralTopologyEdge(subedge_dst, subedge_src),
                        CGWUCentralTopologyMapEdgeOrigin::StateWiredWireless(link_timestamp),
                    );
                }
            }
        }
    }

    pub async fn process_device_topology_event(
        &self,
        _device_type: &CGWDeviceType,
        evt: CGWUCentralEvent,
    ) {
        struct ExistingEdge {
            idx: EdgeIndex,
            timestamp: EdgeCreationTimestamp,
            key: CGWUCentralTopologyEdge,
        }
        let mut lock = self.data.write().await;
        let mut existing_edge: Option<ExistingEdge> = None;

        if let CGWUCentralEventType::RealtimeEvent(rt) = evt.evt_type {
            if let CGWUCentralEventRealtimeEventType::WirelessClientJoin(rt_j) = &rt.evt_type {
                for key in lock.edge_idx_map.keys() {
                    // Try to find <existing> edge:
                    // we're looking for an edge with  wireless client
                    // (<mac>) with specific (<band>) properties.
                    // However, the check is global:
                    // We do not care <which> AP reported the client initially:
                    // since the new client can appear on any given AP that
                    // is connected to us, we have to make sure that if
                    // AP2 receives client.join, and client serial is already
                    // associated with AP1, the connection edge between
                    // AP1 and <client.serial> should be purged,
                    // and then assigned to AP2.
                    //
                    // This only applies, however, to the join message.
                    // Late-leave events should be ignored, in case if
                    // client appears on new/other AP.
                    if let CGWUCentralTopologySubEdgePort::WirelessClient(_, dst_band) = &key.1.port
                    {
                        if key.1.serial == rt_j.client && *dst_band == *rt_j.band {
                            if let Some((
                                edge_idx,
                                CGWUCentralTopologyMapEdgeOrigin::StateWiredWireless(
                                    edge_timestamp,
                                ),
                            )) = lock.edge_idx_map.get(key)
                            {
                                existing_edge = Some(ExistingEdge {
                                    idx: *edge_idx,
                                    timestamp: *edge_timestamp,
                                    key: key.to_owned(),
                                });
                                break;
                            }
                        }
                    }
                }
                if let Some(e) = existing_edge {
                    // New client joined, and new event timestamp is bigger (newer):
                    //   - delete existing edge from map;
                    //   - update graph;
                    if rt.timestamp > e.timestamp {
                        let _ = lock.graph.remove_edge(e.idx);

                        // Remove SRC (tuple idx 0 == src) -> DST (idx 1 == dst) edge
                        let mut edge = CGWUCentralTopologyEdge(e.key.0, e.key.1);
                        let _ = lock.edge_idx_map.remove(&edge);

                        // We do not delete the leaf-disconnected bode,
                        // as we will try to recreate it later on anyways.

                        // Remove DST (tuple idx 1 == dst) -> SRC (idx 0 == src) edge
                        edge = CGWUCentralTopologyEdge(edge.1, edge.0);
                        let _ = lock.edge_idx_map.remove(&edge);
                    } else {
                        warn!(
                            "Received late join event: event ts {:?} vs existing edge ts {:?}",
                            rt.timestamp, e.timestamp
                        );
                        // New event is a late-reported / processed event;
                        // We can safely skip it;
                        return;
                    }
                }

                // Now simply update internall state:
                //   - create node (if doesnt exist already)
                //   - create edge;
                //   - update graph;
                Self::add_node(
                    &mut lock,
                    &rt_j.client,
                    CGWUCentralTopologyMapNodeOrigin::StateWiredWireless,
                );

                let (subedge_src, subedge_dst) = {
                    (
                        CGWUCentralTopologySubEdge {
                            serial: evt.serial,
                            port: CGWUCentralTopologySubEdgePort::WirelessPort,
                        },
                        CGWUCentralTopologySubEdge {
                            serial: rt_j.client,
                            port: CGWUCentralTopologySubEdgePort::WirelessClient(
                                rt_j.ssid.clone(),
                                rt_j.band.clone(),
                            ),
                        },
                    )
                };
                Self::add_edge(
                    &mut lock,
                    CGWUCentralTopologyEdge(subedge_src, subedge_dst),
                    CGWUCentralTopologyMapEdgeOrigin::StateWiredWireless(rt.timestamp),
                );
            } else if let CGWUCentralEventRealtimeEventType::WirelessClientLeave(rt_l) = rt.evt_type
            {
                for key in lock.edge_idx_map.keys() {
                    // Try to find <existing> edge:
                    // we're looking for an edge with  wireless client
                    // (<mac>) with specific (<band>) properties, which is also
                    // reported by the <same> AP, as it's a leave event
                    // (AP1 can't expect us to delete existing edge, if AP2
                    // is already associated with this client)
                    if let CGWUCentralTopologySubEdgePort::WirelessClient(_, dst_band) = &key.1.port
                    {
                        if key.1.serial == rt_l.client && *dst_band == *rt_l.band &&
                           // Part that checks if AP that reports <client.leave> also
                           // is associated with this client.
                           // If not - it's a 'late' leave event that can be ignored.
                           key.0.serial == evt.serial
                        {
                            if let Some((
                                edge_idx,
                                CGWUCentralTopologyMapEdgeOrigin::StateWiredWireless(
                                    edge_timestamp,
                                ),
                            )) = lock.edge_idx_map.get(key)
                            {
                                existing_edge = Some(ExistingEdge {
                                    idx: *edge_idx,
                                    timestamp: *edge_timestamp,
                                    key: key.to_owned(),
                                });
                                break;
                            }
                        }
                    }
                }
                if let Some(e) = existing_edge {
                    // We still have to check whether this leave message
                    // is newer than the existing timestamp:
                    // It's possible that state + leave events were shuffled,
                    // in a way that leave gets processed only after state's
                    // processing's been completed.
                    // This results in a discardtion of the late leave event.
                    if rt.timestamp > e.timestamp {
                        let _ = lock.graph.remove_edge(e.idx);

                        // Remove SRC (tuple idx 0 == src) -> DST (idx 1 == dst) edge
                        let mut edge = CGWUCentralTopologyEdge(e.key.0, e.key.1);
                        let _ = lock.edge_idx_map.remove(&edge);

                        // Also remove dst node if it's a leaf-disconnected node
                        Self::remove_disconnected_leaf_node(&mut lock, &edge.1.serial);

                        // Remove DST (tuple idx 1 == dst) -> SRC (idx 0 == src) edge
                        edge = CGWUCentralTopologyEdge(edge.1, edge.0);
                        let _ = lock.edge_idx_map.remove(&edge);
                    } else {
                        warn!(
                            "Received late leave event: event ts {:?} vs existing edge ts {:?}",
                            rt.timestamp, e.timestamp
                        );
                        // New event is a late-reported / processed event;
                        // We can safely skip it;
                        return;
                    }
                }
            }
        }
    }

    fn add_node(
        data: &mut CGWUCentralTopologyMapData,
        node_mac: &MacAddress,
        origin: CGWUCentralTopologyMapNodeOrigin,
    ) -> NodeIndex {
        match data.node_idx_map.get_mut(node_mac) {
            None => {
                let idx = data.graph.add_node(*node_mac);
                let _ = data.node_idx_map.insert(*node_mac, (idx, origin));
                idx
            }

            Some((idx, existing_origin)) => {
                if let CGWUCentralTopologyMapNodeOrigin::UCentralDevice = existing_origin {
                    *idx
                } else {
                    *existing_origin = origin;
                    *idx
                }
            }
        }
        // TODO: handle <already present in the map> case:
        // this either means that we detected this node at some new
        // position, or this is a "silent" reconnect / re-appearence;
        // e.g. delete all connected edges, child nodes etc etc;
    }

    // Checks before removal, safe to call
    fn remove_disconnected_leaf_node(data: &mut CGWUCentralTopologyMapData, node_mac: &MacAddress) {
        let mut node_idx_to_remove: Option<NodeIndex> = None;

        if let Some((node_idx, origin)) = data.node_idx_map.get(node_mac) {
            // Skip this node, as it's origin is known from
            // uCentral connection, not state data.
            if let CGWUCentralTopologyMapNodeOrigin::UCentralDevice = origin {
                debug!("Not removing disconnected leaf {:?} - reason: uCentral device (direct connection to CGW)", node_mac);
                return;
            }

            let mut edges = data
                .graph
                // We're interested only if there are <incoming> edges for
                // this (potentially) disconnected leaf-node
                .neighbors_directed(*node_idx, Direction::Incoming)
                .detach();

            if edges.next_edge(&data.graph).is_none() {
                node_idx_to_remove = Some(*node_idx);
            }
        }

        if let Some(node_idx) = node_idx_to_remove {
            debug!("MAC {:?} is a disconnected leaf node, removing", node_mac);
            data.node_idx_map.remove(node_mac);
            data.graph.remove_node(node_idx);
        }
    }

    fn remove_node(data: &mut CGWUCentralTopologyMapData, node_mac: &MacAddress) {
        if let Some((node, _)) = data.node_idx_map.remove(node_mac) {
            // 'Potential' list of nodes we can safely remove.
            // Duplicates may exist, because multiple edges can originate
            // from src node (AP, switch) to a single other node
            // (for example client's connected both through
            // the WiFi and the cable, or client's connected
            // to the AP on multiple bands etc).
            //
            // Not every node from this list gets removed, as
            // once again: node (client) can be connected to
            // multiple APs at once on different bands,
            // or client's seen for example both on WiFi
            // and cable.
            let mut nodes_to_remove: Vec<NodeIndex> = Vec::new();

            let mut edges_to_remove: Vec<EdgeIndex> = Vec::new();
            let mut map_edge_keys_to_remove: Vec<CGWUCentralTopologyEdge> = Vec::new();
            let mut edges = [
                data.graph
                    .neighbors_directed(node, Direction::Outgoing)
                    .detach(),
                data.graph
                    .neighbors_directed(node, Direction::Incoming)
                    .detach(),
            ];

            while let Some(edge) = edges[0].next_edge(&data.graph) {
                // We iterate over edges that are connected with this SRC
                // node, and collect all the destination Node indexes,
                // to check them afterwards whether they still have
                // some edges connected to them.
                // If not - we remove the nodes out off the internal map.
                // NOTE: It's possible that two Websocket devices are
                // connected and we'll try to remove DST node even though
                // knowledge about this device's presence in our map
                // originates from WSS  connection, not state message.
                // However, internal map also has meta information
                // about the origin of appearence in map, hence
                // it solves the issue.
                // (if node.origin == WSS then <do not remove node>)
                //
                // NOTE: we do this only for <Outgoing> neighbors
                // From treeview-graph perspective, we're clearing <leaf>
                // nodes that originate from this <node_mac> device.
                if let Some((_, node_dst)) = data.graph.edge_endpoints(edge) {
                    nodes_to_remove.push(node_dst);
                }

                data.graph.remove_edge(edge);
                edges_to_remove.push(edge);
            }

            while let Some(edge) = edges[1].next_edge(&data.graph) {
                data.graph.remove_edge(edge);
                edges_to_remove.push(edge);
            }

            for node_idx in nodes_to_remove {
                let mut node_edges = data
                    .graph
                    .neighbors_directed(node_idx, Direction::Incoming)
                    .detach();

                // Check if at least one edge is connecting this
                // Node; If not - purge it, but only if this node
                // has been added through the means of State message
                // or realtime events.
                //
                // If it's an active WSS connection we have established,
                // we should skip this node, as it's not our responsibility
                // here to destroy it.
                if node_edges.next_edge(&data.graph).is_none() {
                    let mut node_to_remove: Option<&MacAddress> = None;
                    if let Some(node_weight) = data.graph.node_weight(node_idx) {
                        if let Some((_, origin)) = data.node_idx_map.get(node_weight) {
                            // Skip this node, as it's origin is known from
                            // uCentral connection, not state data.
                            if let CGWUCentralTopologyMapNodeOrigin::UCentralDevice = origin {
                                debug!("Not removing disconnected leaf {:?} - reason: uCentral device (direct connection to CGW)", node_weight);
                                continue;
                            }

                            node_to_remove = Some(node_weight);
                        }
                    }

                    if let Some(node_mac) = node_to_remove {
                        data.node_idx_map.remove(node_mac);
                        data.graph.remove_node(node_idx);
                    }
                }
            }

            for (k, e) in &data.edge_idx_map {
                for x in &edges_to_remove {
                    if *x == e.0 {
                        map_edge_keys_to_remove.push(k.to_owned());
                    }
                }
            }

            for key in map_edge_keys_to_remove {
                data.edge_idx_map.remove(&key);
            }
            data.graph.remove_node(node);
        }
    }

    fn add_edge(
        data: &mut CGWUCentralTopologyMapData,
        edge: CGWUCentralTopologyEdge,
        origin: CGWUCentralTopologyMapEdgeOrigin,
    ) {
        let node_src_subedge: CGWUCentralTopologySubEdge = edge.0;
        let node_dst_subedge: CGWUCentralTopologySubEdge = edge.1;
        let (node_src_idx, node_dst_idx) = {
            (
                match data.node_idx_map.get(&node_src_subedge.serial) {
                    Some((idx, _)) => *idx,
                    None => {
                        warn!(
                            "Tried to add edge for non-existing node {:?}",
                            node_src_subedge.serial
                        );
                        return;
                    }
                },
                match data.node_idx_map.get(&node_dst_subedge.serial) {
                    Some((idx, _)) => *idx,
                    None => {
                        warn!(
                            "Tried to add edge for non-existing node {:?}",
                            node_dst_subedge.serial
                        );
                        return;
                    }
                },
            )
        };

        let edge_idx = data.graph.add_edge(
            node_src_idx,
            node_dst_idx,
            format!("{}<->{}", node_src_subedge.port, node_dst_subedge.port),
        );

        data.edge_idx_map.insert(
            CGWUCentralTopologyEdge(node_src_subedge.clone(), node_dst_subedge.clone()),
            (edge_idx, origin.clone()),
        );
        data.edge_idx_map.insert(
            CGWUCentralTopologyEdge(node_dst_subedge, node_src_subedge),
            (edge_idx, origin.clone()),
        );
    }

    fn remove_edge(data: &mut CGWUCentralTopologyMapData, subedge: &CGWUCentralTopologySubEdge) {
        let mut keys_to_remove: Vec<CGWUCentralTopologyEdge> = Vec::new();

        for key in data.edge_idx_map.keys() {
            if key.0 == *subedge || key.1 == *subedge {
                let key_to_remove = key.to_owned();
                keys_to_remove.push(key_to_remove);
            }
        }

        if let Some(key) = keys_to_remove.first() {
            if let Some((edge_idx, _)) = data.edge_idx_map.get(key) {
                data.graph.remove_edge(*edge_idx);
            }
        }

        for k in keys_to_remove {
            let _ = data.edge_idx_map.remove(&k);
        }
    }

    pub async fn debug_dump_map(&self) {
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
    }
}
