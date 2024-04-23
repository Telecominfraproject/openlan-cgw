use crate::{
    cgw_device::CGWDeviceType,
    cgw_ucentral_parser::{CGWUCentralEvent, CGWUCentralEventType},
};
use petgraph::dot::{Config, Dot};
use petgraph::{
    graph::{EdgeIndex, NodeIndex},
    stable_graph::StableGraph,
    visit::NodeRef,
    Direction,
};

use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use eui48::MacAddress;

// One 'slice' / part of edge (Mac + port);
// To make a proper complete edge two parts needed:
// SRC -> DST
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CGWUcentralTopologySubEdge {
    pub mac: MacAddress,
    pub port: String,
}

// Complete edge consisting of SRC -> DST 'sub-edges'
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CGWUcentralTopologyEdge(CGWUcentralTopologySubEdge, CGWUcentralTopologySubEdge);

#[derive(Debug)]
struct CGWUcentralTopologyMapData {
    node_idx_map: HashMap<MacAddress, NodeIndex>,
    edge_idx_map: HashMap<CGWUcentralTopologyEdge, EdgeIndex>,
    graph: StableGraph<String, String>,
}

#[derive(Debug)]
pub struct CGWUcentralTopologyMap {
    data: Arc<RwLock<CGWUcentralTopologyMapData>>,
}

lazy_static! {
    pub static ref CGW_UCENTRAL_TOPOLOGY_MAP: CGWUcentralTopologyMap = CGWUcentralTopologyMap {
        data: Arc::new(RwLock::new(CGWUcentralTopologyMapData {
            node_idx_map: HashMap::new(),
            edge_idx_map: HashMap::new(),
            graph: StableGraph::new(),
        }))
    };
}

impl CGWUcentralTopologyMap {
    pub fn get_ref() -> &'static Self {
        &CGW_UCENTRAL_TOPOLOGY_MAP
    }

    pub async fn insert_device(self: &Self, mac: &MacAddress) {
        let mut lock = self.data.write().await;
        Self::add_node(&mut lock, mac);
    }

    pub async fn remove_device(self: &Self, mac: &MacAddress) {
        let mut lock = self.data.write().await;
        Self::remove_node(&mut lock, mac);
    }

    pub async fn process_state_message(
        self: &Self,
        _device_type: &CGWDeviceType,
        evt: CGWUCentralEvent,
    ) {
        let mut lock = self.data.write().await;

        if let CGWUCentralEventType::State(s) = evt.evt_type {
            // Start with LLDP info processing
            for link in s.lldp_data.links {
                let subedge_src = CGWUcentralTopologySubEdge {
                    mac: s.lldp_data.local_mac,
                    port: link.local_port,
                };
                let subedge_dst = CGWUcentralTopologySubEdge {
                    mac: link.remote_mac,
                    port: link.remote_port,
                };
                // Any neighbour seen in LLDP is added to the graph.
                // Whenever parent (entity reporting the LLDP data)
                // get's removed - neighbour nodes and connected
                // edges will be purged.
                Self::add_node(&mut lock, &link.remote_mac);

                // No duplicates can exists, since it's LLDP data
                // (both uCentral and underlying LLDP agents do not
                // support multiple LLDP clients over single link,
                // so it's not expect to have duplicates here).
                // Hence we have to try and remove any duplicates
                // we can find (either based on SRC or DST device's
                // subedge info.
                Self::remove_edge(&mut lock, &subedge_src);
                Self::remove_edge(&mut lock, &subedge_dst);

                if link.is_downstream {
                    Self::add_edge(&mut lock, CGWUcentralTopologyEdge(subedge_src, subedge_dst));
                } else {
                    Self::add_edge(&mut lock, CGWUcentralTopologyEdge(subedge_dst, subedge_src));
                }
            }
        }
    }

    pub async fn process_device_topology_event(self: &Self) {}

    fn add_node(data: &mut CGWUcentralTopologyMapData, node_mac: &MacAddress) -> NodeIndex {
        match data.node_idx_map.get(node_mac) {
            None => {
                let idx = data.graph.add_node(node_mac.to_hex_string());
                let _ = data.node_idx_map.insert(node_mac.clone(), idx);
                idx
            }
            Some(idx) => *idx,
        }
        // TODO: handle <already present in the map> case:
        // this either means that we detected this node at some new
        // position, or this is a "silent" reconnect / re-appearence;
        // e.g. delete all connected edges, child nodes etc etc;
    }

    fn remove_node(data: &mut CGWUcentralTopologyMapData, node_mac: &MacAddress) {
        if let Some(node) = data.node_idx_map.remove(node_mac) {
            let mut edges_to_remove: Vec<EdgeIndex> = Vec::new();
            let mut map_keys_to_remove: Vec<CGWUcentralTopologyEdge> = Vec::new();
            let edges = [
                data.graph
                    .neighbors_directed(node, Direction::Outgoing)
                    .detach(),
                data.graph
                    .neighbors_directed(node, Direction::Incoming)
                    .detach(),
            ];

            for mut x in edges {
                while let Some(edge) = x.next_edge(&data.graph) {
                    data.graph.remove_edge(edge);
                    edges_to_remove.push(edge);
                }
            }
            for (k, e) in &data.edge_idx_map {
                for x in &edges_to_remove {
                    if x == e {
                        map_keys_to_remove.push(k.to_owned());
                    }
                }
            }
            for x in map_keys_to_remove {
                data.edge_idx_map.remove(&x);
            }
            data.graph.remove_node(node);
        }
    }

    fn add_edge(data: &mut CGWUcentralTopologyMapData, edge: CGWUcentralTopologyEdge) {
        let node_src_subedge: CGWUcentralTopologySubEdge = edge.0;
        let node_dst_subedge: CGWUcentralTopologySubEdge = edge.1;
        let node_src_idx = Self::add_node(data, &node_src_subedge.mac);
        let node_dst_idx = Self::add_node(data, &node_dst_subedge.mac);

        let edge_idx = data.graph.add_edge(
            node_src_idx,
            node_dst_idx,
            format!("{}<->{}", node_src_subedge.port, node_dst_subedge.port),
        );

        data.edge_idx_map.insert(
            CGWUcentralTopologyEdge(node_src_subedge.clone(), node_dst_subedge.clone()),
            edge_idx,
        );
        data.edge_idx_map.insert(
            CGWUcentralTopologyEdge(node_dst_subedge, node_src_subedge),
            edge_idx,
        );
    }

    fn remove_edge(data: &mut CGWUcentralTopologyMapData, subedge: &CGWUcentralTopologySubEdge) {
        let mut keys_to_remove: Vec<CGWUcentralTopologyEdge> = Vec::new();

        for key in data.edge_idx_map.keys() {
            if key.0 == *subedge || key.1 == *subedge {
                let key_to_remove = key.to_owned();
                keys_to_remove.push(key_to_remove);
            }
        }

        if let Some(key) = keys_to_remove.get(0) {
            if let Some(v) = data.edge_idx_map.get(key) {
                data.graph.remove_edge(*v);
            }
        }

        for k in keys_to_remove {
            let _ = data.edge_idx_map.remove(&k);
        }
    }

    pub async fn debug_dump_map(self: &Self) {
        let lock = self.data.read().await;
        let dotfmt = format!(
            "{:?}",
            Dot::with_attr_getters(
                &lock.graph,
                &[Config::NodeNoLabel, Config::EdgeNoLabel],
                &|_, er| { format!("label = \"{}\"", er.weight()) },
                &|_, nr| { format!("label = \"{}\" shape=\"record\"", nr.weight()) }
            )
        );
        info!("graph dump:\n{}", dotfmt);
    }
}
