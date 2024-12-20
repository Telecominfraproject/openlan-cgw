use crate::{
    cgw_connection_server::CGWConnectionServer,
    cgw_device::CGWDeviceType,
    cgw_nb_api_listener::{
        cgw_construct_client_join_msg, cgw_construct_client_leave_msg,
        cgw_construct_client_migrate_msg,
    },
    cgw_ucentral_parser::{
        CGWUCentralEvent, CGWUCentralEventRealtimeEventType, CGWUCentralEventStateClientsType,
        CGWUCentralEventStatePort, CGWUCentralEventType,
    },
};

use tokio::{
    runtime::Runtime,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
    time::{sleep, Duration},
};

use std::{collections::HashMap, str::FromStr, sync::Arc};
use tokio::sync::RwLock;

use eui48::MacAddress;

type ClientLastSeenTimestamp = i64;

// Client mac, ssid, band
type ClientsJoinList = Vec<(MacAddress, String, String)>;

// Client mac, band
type ClientsLeaveList = Vec<(MacAddress, String)>;

// Client mac, new AP mac, band, ssid
type ClientsMigrateList = Vec<(MacAddress, MacAddress, String, String)>;

// Last seen, ssid, band
type ClientsConnectedList = (ClientLastSeenTimestamp, String, String);

struct CGWTopologyMapQueueMessage {
    evt: CGWUCentralEvent,
    dev_type: CGWDeviceType,
    node_mac: MacAddress,
    gid: i32,
    conn_server: Arc<CGWConnectionServer>,
}

type CGWTopologyMapQueueRxHandle = UnboundedReceiver<CGWTopologyMapQueueMessage>;
type CGWTopologyMapQueueTxHandle = UnboundedSender<CGWTopologyMapQueueMessage>;

// We have to track the 'origin' of any node we add to topo map,
// because deletion decision should be made on the following basis:
// - direct WSS connection should be always kept in the topo map,
//   and only erased when disconnect happens;
// - any nodes, that are added to topo map as lldp peers
//   should be deleted when the node that reported them gets deleted;
#[derive(Debug, Clone)]
enum CGWUCentralTopologyMapNodeOrigin {
    UCentralDevice,
    StateLLDPPeer,
}

#[derive(Debug)]
struct CGWUCentralTopologyMapConnectionParams {
    // TODO: actually use for graph building;
    // Currently, unused.
    #[allow(unused)]
    mac: MacAddress,
    #[allow(unused)]
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
            // This hashmap is needed to keep track of _all_ topomap nodes
            // connected (directly reported) by this device, to detect
            // _connect_ / _disconnect_ events:
            // we need to keep track whenever we see newly connected WiFi client,
            // or an event of such device getting disconnected.
            // This need to be found out fast, hence sacrifice memory in the
            // name of faster lookup;
            // LIMITATION:
            //   * Works only on a per-group basis (if wifi-client migrates to
            //     another GID, this event would be missed)
            //     (as per current implementation).
            // Track key:client mac, values:last seen timestamp, ssid and band
            HashMap<MacAddress, ClientsConnectedList>,
        ),
    >,
}

#[derive(Debug)]
pub struct CGWUCentralTopologyMap {
    // Stored on a per-gid basis
    data: Arc<
        RwLock<
            HashMap<
                i32,
                (
                    CGWUCentralTopologyMapData,
                    // This hashmap is needed to keep track of _all_ topomap nodes
                    // connected (directly reported) by this device, to detect _migration_
                    // process:
                    // we need to keep track whenever WiFi client of AP_1, for example,
                    // 'silently' migrates to AP_2.
                    //
                    // We should also track the last time seen value of this
                    // client / node, to make appropriate decision
                    // whenever leave/join/migrate happens.
                    //
                    // LIMITATION:
                    //   * Works only on a per-group basis (if wifi-client migrates to
                    //     another GID, this event would be missed)
                    //     (as per current implementation).
                    // Track key:client mac, values:parent AP mac, last seen timestamp, ssid and band
                    HashMap<MacAddress, (MacAddress, ClientLastSeenTimestamp, String, String)>,
                ),
            >,
        >,
    >,
    queue: (
        Arc<CGWTopologyMapQueueTxHandle>,
        Arc<Mutex<CGWTopologyMapQueueRxHandle>>,
    ),
    started: Mutex<bool>,
}

lazy_static! {
    pub static ref CGW_UCENTRAL_TOPOLOGY_MAP: CGWUCentralTopologyMap = CGWUCentralTopologyMap {
        data: Arc::new(RwLock::new(HashMap::new())),
        queue: {
            let (tx, rx) = unbounded_channel::<CGWTopologyMapQueueMessage>();
            (Arc::new(tx), Arc::new(Mutex::new(rx)))
        },
        started: Mutex::new(false),
    };
}

impl CGWUCentralTopologyMap {
    pub fn get_ref() -> &'static Self {
        &CGW_UCENTRAL_TOPOLOGY_MAP
    }

    pub async fn start(&self, rt: &Runtime) {
        let mut started = self.started.lock().await;

        if !*started {
            *started = true;
            rt.spawn(async move {
                CGWUCentralTopologyMap::process_queue().await;
            });
        }
    }

    async fn process_queue() {
        info!("TopoMap: queue processor started.");
        let topo_map = CGWUCentralTopologyMap::get_ref();

        let buf_capacity = 2000;
        let mut buf: Vec<CGWTopologyMapQueueMessage> = Vec::with_capacity(buf_capacity);
        let mut num_of_msg_read = 0;

        loop {
            let mut rx_mbox = topo_map.queue.1.lock().await;

            if num_of_msg_read < buf_capacity {
                // Try to recv_many, but don't sleep too much
                // in case if no messaged pending and we have
                // TODO: rework to pull model (pull on demand),
                // compared to curr impl: push model (nb api listener forcefully
                // pushed all fetched data from kafka).
                // Currently recv_many may sleep if previous read >= 1,
                // but no new messages pending
                //
                // It's also possible that this logic staggers the processing,
                // in case when every new message is received <=9 ms for example:
                // Single message received, waiting for new up to 10 ms.
                // New received on 9th ms. Repeat.
                // And this could repeat up untill buffer is full, or no new messages
                // appear on the 10ms scale.
                // Highly unlikly scenario, but still possible.
                let rd_num = tokio::select! {
                    v = rx_mbox.recv_many(&mut buf, buf_capacity - num_of_msg_read) => {
                        v
                    }
                    _v = sleep(Duration::from_millis(100)) => {
                        0
                    }
                };
                num_of_msg_read += rd_num;

                // We read some messages, try to continue and read more
                // If none read - break from recv, process all buffers that've
                // been filled-up so far (both local and remote).
                // Upon done - repeat.
                if rd_num >= 1 || num_of_msg_read == 0 {
                    continue;
                }
            }

            debug!("Received {num_of_msg_read} events from devices, processing...");

            while !buf.is_empty() {
                let m = buf.remove(0);
                match m.evt.evt_type {
                    CGWUCentralEventType::State(_) => {
                        topo_map
                            .process_state_message(
                                &m.dev_type,
                                &m.node_mac,
                                m.evt,
                                m.gid,
                                m.conn_server,
                            )
                            .await;
                    }
                    CGWUCentralEventType::RealtimeEvent(_) => {
                        topo_map
                            .process_device_topology_event(
                                &m.dev_type,
                                &m.node_mac,
                                m.evt,
                                m.gid,
                                m.conn_server,
                            )
                            .await;
                    }
                    _ => {}
                }
            }

            debug!("Done processing {num_of_msg_read} events from devices");

            buf.clear();
            num_of_msg_read = 0;
        }
    }

    pub fn enqueue_event(
        &self,
        evt: CGWUCentralEvent,
        dev_type: CGWDeviceType,
        node_mac: MacAddress,
        gid: i32,
        conn_server: Arc<CGWConnectionServer>,
    ) {
        let _ = self.queue.0.send(CGWTopologyMapQueueMessage {
            evt,
            dev_type,
            node_mac,
            gid,
            conn_server,
        });
    }

    pub async fn remove_gid(&self, gid: i32) {
        let mut lock = self.data.write().await;
        lock.remove(&gid);
    }

    pub async fn insert_device(&self, topology_node_mac: &MacAddress, platform: &str, gid: i32) {
        // TODO: rework to use device / accept deivce, rather then trying to
        // parse string once again.
        if CGWDeviceType::from_str(platform).is_err() {
            warn!(
                "Tried to insert {topology_node_mac} into tomo map, but failed to parse it's platform string"
            );
            return;
        }

        let map_connections = CGWUCentralTopologyMapConnections {
            links_list: HashMap::new(),
        };

        let mut lock = self.data.write().await;

        // Clear occurance of this mac from ANY of the groups.
        // This can only happen whenever device get's GID assigned from
        // 0 (unassigned) to some specific GID, for example:
        // was gid 0 - we created node initially - then NB's assigned device
        // to a specific GID.
        for (_gid, (v, _)) in lock.iter_mut() {
            let _ = v.topology_nodes.remove(topology_node_mac);
        }

        // Try to insert new topo-map node, however it's possible that it's the
        // first isert:
        //   - if first time GID is being manipulated - we also have to create
        //     a hashmap that controls this GID;
        //   - if exists - simply insert new topomap node into existing GID map.
        if let Some((ref mut topology_map_data, _)) = lock.get_mut(&gid) {
            topology_map_data.topology_nodes.insert(
                *topology_node_mac,
                (
                    CGWUCentralTopologyMapNodeOrigin::UCentralDevice,
                    map_connections,
                    HashMap::new(),
                ),
            );
        } else {
            let mut topology_map_data: CGWUCentralTopologyMapData = CGWUCentralTopologyMapData {
                topology_nodes: HashMap::new(),
            };
            topology_map_data.topology_nodes.insert(
                *topology_node_mac,
                (
                    CGWUCentralTopologyMapNodeOrigin::UCentralDevice,
                    map_connections,
                    HashMap::new(),
                ),
            );
            lock.insert(gid, (topology_map_data, HashMap::new()));
        }
    }

    pub async fn remove_device(
        &self,
        topology_node_mac: &MacAddress,
        gid: i32,
        // TODO: remove this Arc:
        // Dirty hack for now: pass Arc ref of srv to topo map;
        // Future rework and refactoring would require to separate
        // NB api from being an internal obj of conn_server to be a
        // standalone (singleton?) object.
        conn_server: Arc<CGWConnectionServer>,
    ) {
        let mut lock = self.data.write().await;
        // Disconnected clients (seen before, don't see now) client mac from -> AP mac
        let mut clients_leave_list: ClientsLeaveList = Vec::new();

        if let Some((ref mut topology_map_data, ref mut existing_nodes_map)) = lock.get_mut(&gid) {
            Self::clear_related_nodes(topology_map_data, topology_node_mac);
            if let Some((_, _, removed_clients_list)) =
                topology_map_data.topology_nodes.remove(topology_node_mac)
            {
                // We have to clear Per-device connected clients from global
                // map.
                for client_mac in removed_clients_list.keys() {
                    if let Some((
                        _existing_client_parent_node_mac,
                        _existing_client_node_last_seen_ts,
                        _existing_client_ssid,
                        existing_client_band,
                    )) = existing_nodes_map.remove(client_mac)
                    {
                        clients_leave_list.push((*client_mac, existing_client_band));
                    }
                }
            }
        }

        if !clients_leave_list.is_empty() {
            Self::handle_clients_leave(
                *topology_node_mac,
                clients_leave_list,
                gid,
                conn_server.clone(),
            );
        }
    }

    // We still want to have an easy access for node mac that reported this event,
    // hence it's easier to just pass it as an argument, rather then fetching
    // it from the array itself.
    fn handle_clients_join(
        node_mac: MacAddress,
        clients_list: ClientsJoinList,
        gid: i32,

        // TODO: remove this Arc:
        // Dirty hack for now: pass Arc ref of srv to topo map;
        // Future rework and refactoring would require to separate
        // NB api from being an internal obj of conn_server to be a
        // standalone (singleton?) object.
        conn_server: Arc<CGWConnectionServer>,
    ) {
        if clients_list.is_empty() {
            return;
        }

        // We have AP mac, iterate only over keys - client macs
        for (client_mac, new_ssid, new_band) in clients_list {
            let msg = cgw_construct_client_join_msg(gid, client_mac, node_mac, new_ssid, new_band);
            if let Ok(r) = msg {
                let _ = conn_server.enqueue_mbox_message_from_device_to_nb_api_c(gid, r);
            } else {
                warn!("Failed to convert client leave event to string!");
            }
        }
    }

    // We still want to have an easy access for node mac that reported this event,
    // hence it's easier to just pass it as an argument, rather then fetching
    // it from the array itself.
    fn handle_clients_leave(
        node_mac: MacAddress,
        clients_list: ClientsLeaveList,
        gid: i32,

        // TODO: remove this Arc:
        // Dirty hack for now: pass Arc ref of srv to topo map;
        // Future rework and refactoring would require to separate
        // NB api from being an internal obj of conn_server to be a
        // standalone (singleton?) object.
        conn_server: Arc<CGWConnectionServer>,
    ) {
        if clients_list.is_empty() {
            return;
        }

        // We have AP mac, iterate only over keys - client macs
        for (client_mac, band) in clients_list {
            let msg = cgw_construct_client_leave_msg(gid, client_mac, node_mac, band);
            if let Ok(r) = msg {
                let _ = conn_server.enqueue_mbox_message_from_device_to_nb_api_c(gid, r);
            } else {
                warn!("Failed to convert client leave event to string!");
            }
        }
    }

    // We still want to have an easy access for node mac that reported this event,
    // hence it's easier to just pass it as an argument, rather then fetching
    // it from the array itself.
    fn handle_clients_migrate(
        clients_list: ClientsMigrateList,
        gid: i32,

        // TODO: remove this Arc:
        // Dirty hack for now: pass Arc ref of srv to topo map;
        // Future rework and refactoring would require to separate
        // NB api from being an internal obj of conn_server to be a
        // standalone (singleton?) object.
        conn_server: Arc<CGWConnectionServer>,
    ) {
        if clients_list.is_empty() {
            return;
        }

        // We have AP mac, iterate only over keys - client macs
        for (client_mac, new_parent_ap_mac, new_band, new_ssid) in clients_list {
            let msg = cgw_construct_client_migrate_msg(
                gid,
                client_mac,
                new_parent_ap_mac,
                new_ssid,
                new_band,
            );
            if let Ok(r) = msg {
                let _ = conn_server.enqueue_mbox_message_from_device_to_nb_api_c(gid, r);
            } else {
                warn!("Failed to convert client leave event to string!");
            }
        }
    }

    // Process state message in an ublocking-manner as long as possible:
    //   * Function does alot of unnecessary (on the first glance) cloning
    //     and allocations, but it's needed to make sure we block the topomap
    //     for as short period of time as possible, to not clog/starve any
    //     other topo map users (other connections/devices).
    //   * All the allocations and copying is done to make sure at the end
    //     of the function we only do alterations to the topomap itself,
    //     and other calculations and tree-traverals should be kept at minimum.
    // Overall design is part of software optimizations / approach to ublock
    // other threads accessing the topomap.
    pub async fn process_state_message(
        &self,
        device_type: &CGWDeviceType,
        topology_node_mac: &MacAddress,
        evt: CGWUCentralEvent,
        gid: i32,

        // TODO: remove this Arc:
        // Dirty hack for now: pass Arc ref of srv to topo map;
        // Future rework and refactoring would require to separate
        // NB api from being an internal obj of conn_server to be a
        // standalone (singleton?) object.
        conn_server: Arc<CGWConnectionServer>,
    ) {
        if let CGWUCentralEventType::State(s) = evt.evt_type {
            // Clear any related (child, or parent nodes we explicitly
            // created).
            // The child/parent node can be explicitly created,
            // if information about the node is only deduced from
            // lldp peer information, and the underlying node is not
            // a ucentral device.
            {
                let mut lock = self.data.write().await;
                if let Some((ref mut topology_map_data, _)) = lock.get_mut(&gid) {
                    Self::clear_related_nodes(topology_map_data, topology_node_mac);
                } else {
                    error!("Unexpected: GID {gid} doesn't exists (should've been created prior to state processing)!");
                    return;
                }
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
                    HashMap<MacAddress, ClientsConnectedList>,
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
                    let lldp_peer_map_connections = CGWUCentralTopologyMapConnections {
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
                        if upstream_lldp_node.is_none() {
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
                            // Don't care about _child_ nodes tracking for LLDP
                            // peers - create empty map.
                            HashMap::new(),
                        ),
                    ));
                }
            }

            // List (map) of child nodes that are directly connected to the
            // parent node that reports state event message.
            //
            // We need to catch any connected/disconnected/migrated events
            // based on this data.
            let mut new_connected_child_clients_map: HashMap<MacAddress, ClientsConnectedList> =
                HashMap::new();

            for (local_port, links) in s.clients_data.links.into_iter() {
                // Filled on a per-port basis.
                let mut local_map_conn_data = CGWUCentralTopologyMapConnectionsData {
                    infra_nodes_list: Vec::new(),
                    parent_topology_node_mac: None,
                    child_lldp_nodes: HashMap::new(),
                };

                // We're processing the link reports for the port that is
                // also a port that <points> to upstream LLDP peer
                if let Some((lldp_peer_mac, ref lldp_peer_local_port)) = upstream_lldp_node {
                    if *lldp_peer_local_port == local_port {
                        local_map_conn_data.parent_topology_node_mac = Some(lldp_peer_mac);
                    }
                }

                // Will be skipped, in case if upstream processing took
                // place: it's not allowed by design to have the same
                // mac be an upstream as well as downstream peer;
                if let Some(lldp_peer_mac) = downstream_lldp_nodes.get(&local_port) {
                    local_map_conn_data
                        .child_lldp_nodes
                        .insert(*lldp_peer_mac, ());
                }

                for link_seen_on_port in links {
                    // Treat state timestamp as edge-creation timestamp only for
                    // events that do not report explicit connection timestamp
                    // (no association establishment timestamp for wired clients,
                    // however present for wireless for example).
                    let link_timestamp = {
                        if let CGWUCentralEventStateClientsType::Wireless(ts) =
                            link_seen_on_port.client_type
                        {
                            // We need to track on a port-agnostic level macs
                            // of wireles clients to easily track down the
                            // migrated/disconnected/connected clients fast.
                            if let CGWDeviceType::CGWDeviceAP = device_type {
                                if let CGWUCentralEventStatePort::WirelessPort(ref ssid, ref band) =
                                    local_port
                                {
                                    new_connected_child_clients_map.insert(
                                        link_seen_on_port.remote_serial,
                                        (ts, ssid.clone(), band.clone()),
                                    );
                                }
                            }
                            ts
                        } else if let CGWUCentralEventStateClientsType::Wired(ts) =
                            link_seen_on_port.client_type
                        {
                            ts
                        } else {
                            s.timestamp
                        }
                    };

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
                    CGWUCentralTopologyMapNodeOrigin::UCentralDevice,
                    map_connections,
                    new_connected_child_clients_map,
                ),
            ));

            let mut lock = self.data.write().await;
            if let Some((ref mut topology_map_data, ref mut existing_nodes_map)) =
                lock.get_mut(&gid)
            {
                for (node_mac, (node_origin, node_connections, connected_child_clients_map)) in
                    nodes_to_create.into_iter()
                {
                    // Unconditionally insert/replace our AP / switch node;
                    if node_mac == *topology_node_mac {
                        // For APs we have a special handling of tracking wifi devices:
                        // track newly connected, disconnected or migrates
                        // events.
                        if let CGWDeviceType::CGWDeviceAP = device_type {
                            // New connected clients (first time seen) client mac -> on AP mac,
                            // ssid, band
                            let mut clients_join_list: ClientsJoinList = Vec::new();
                            // Disconnected clients (seen before, don't see now) client mac from -> AP mac, band
                            let mut clients_leave_list: ClientsLeaveList = Vec::new();
                            // Migrated client mac -> to (AP mac, ssid, band)
                            let mut clients_migrate_list: ClientsMigrateList = Vec::new();

                            // We also have to iterate through wireless clients
                            // to detect client connect/disconnect/migrate events.
                            for (client_mac, (last_seen_ts, ssid, band)) in
                                connected_child_clients_map.iter()
                            {
                                if let Some((
                                    existing_client_parent_node_mac,
                                    existing_client_node_last_seen_ts,
                                    existing_client_ssid,
                                    existing_client_band,
                                )) = existing_nodes_map.remove(client_mac)
                                {
                                    // We know that existing node for some reason has not _our_ parent mac,
                                    // means it's either we're late in seeing this MAC (it's long go
                                    // migrated), or this is an actual migration event detected:
                                    //   * compare current TS with existing, if current is higher
                                    if existing_client_parent_node_mac != node_mac {
                                        if *last_seen_ts >= existing_client_node_last_seen_ts {
                                            // Update TS, update mac of AP - owner, generate
                                            // migrate event
                                            existing_nodes_map.insert(
                                                *client_mac,
                                                (
                                                    node_mac,
                                                    *last_seen_ts,
                                                    ssid.clone(),
                                                    band.clone(),
                                                ),
                                            );
                                            clients_migrate_list.push((
                                                *client_mac,
                                                node_mac,
                                                ssid.clone(),
                                                band.clone(),
                                            ));

                                            // Since it's a migrate event, also __remove__ client
                                            // from client list of a node which client's migrated.
                                            if let Some((_, _, ref mut old_clients_data)) =
                                                topology_map_data
                                                    .topology_nodes
                                                    .get_mut(&existing_client_parent_node_mac)
                                            {
                                                let _ = old_clients_data.remove(client_mac);
                                            }
                                        } else {
                                            // Turns out, latest evt timestamp is
                                            // more recent than this one;
                                            // Ignore this one (skip),
                                            // and make sure we insert removed
                                            // data back as it was.
                                            existing_nodes_map.insert(
                                                *client_mac,
                                                (
                                                    existing_client_parent_node_mac,
                                                    existing_client_node_last_seen_ts,
                                                    existing_client_ssid,
                                                    existing_client_band,
                                                ),
                                            );
                                            continue;
                                        }
                                    } else {
                                        // Just update the TS
                                        existing_nodes_map.insert(
                                            *client_mac,
                                            (
                                                node_mac,
                                                *last_seen_ts,
                                                existing_client_ssid,
                                                existing_client_band,
                                            ),
                                        );
                                    }
                                } else {
                                    // Create new entry, generate connected event
                                    existing_nodes_map.insert(
                                        *client_mac,
                                        (node_mac, *last_seen_ts, ssid.clone(), band.clone()),
                                    );
                                    clients_join_list.push((
                                        *client_mac,
                                        ssid.clone(),
                                        band.clone(),
                                    ));
                                }
                            }

                            // Lastly, we have to detect disconnected client:
                            // a disconnected client is present in old cache data
                            // of a node, but is missing from current report.
                            // We also have to make sure we don't report
                            // migrate twice - if it's missing from current
                            // state report, we still have to make sure
                            // it's not connected to another AP.
                            if let Some((_, _, old_clients_data)) =
                                topology_map_data.topology_nodes.get(&node_mac)
                            {
                                for old_client_data_mac in old_clients_data.keys() {
                                    // Tricky way to skip found macs;
                                    // We don't want to have double-borrow;
                                    if connected_child_clients_map.contains_key(old_client_data_mac)
                                    {
                                        continue;
                                    }

                                    // We're past check, which means there's a mac present in old data,
                                    // but missing in new report: either disconnected
                                    // or migrated - check this here.

                                    // It seems like mac is present in missing from current report,
                                    // but was present in previous one:
                                    //   - check if by any chance the client's migrated
                                    //     to some other AP, or just plain disconnected
                                    if let Some((
                                        existing_client_parent_node_mac,
                                        existing_client_node_last_seen_ts,
                                        existing_client_ssid,
                                        existing_client_band,
                                    )) = existing_nodes_map.remove(old_client_data_mac)
                                    {
                                        if existing_client_parent_node_mac == node_mac {
                                            // Parent mac is the same == disconnected event
                                            // And we keep track only for disconnected events,
                                            // the migration was reported by another AP.
                                            clients_leave_list
                                                .push((*old_client_data_mac, existing_client_band));
                                        } else {
                                            existing_nodes_map.insert(
                                                *old_client_data_mac,
                                                (
                                                    existing_client_parent_node_mac,
                                                    existing_client_node_last_seen_ts,
                                                    existing_client_ssid,
                                                    existing_client_band,
                                                ),
                                            );
                                        }
                                    }
                                }
                            }

                            if !clients_join_list.is_empty() {
                                Self::handle_clients_join(
                                    *topology_node_mac,
                                    clients_join_list,
                                    gid,
                                    conn_server.clone(),
                                );
                            }

                            if !clients_leave_list.is_empty() {
                                Self::handle_clients_leave(
                                    *topology_node_mac,
                                    clients_leave_list,
                                    gid,
                                    conn_server.clone(),
                                );
                            }

                            if !clients_migrate_list.is_empty() {
                                Self::handle_clients_migrate(
                                    clients_migrate_list,
                                    gid,
                                    conn_server.clone(),
                                );
                            }
                        }

                        Self::add_node(
                            topology_map_data,
                            node_mac,
                            node_origin,
                            node_connections,
                            connected_child_clients_map,
                        );
                    } else {
                        // Skip UCentral-device (not this device/node) controlled
                        // topomap entries.
                        // We only add nodes that we explicitly created.
                        // On the next iteration of state data our lldp-peer-partners
                        // will update topo map on their own, if we didn't here.
                        if let Some((CGWUCentralTopologyMapNodeOrigin::UCentralDevice, _, _)) =
                            topology_map_data.topology_nodes.get(&node_mac)
                        {
                            continue;
                        }

                        // It's clear that this node is created by us in this iteration of
                        // lldp parsing, so it's safe to add it.
                        Self::add_node(
                            topology_map_data,
                            node_mac,
                            node_origin,
                            node_connections,
                            connected_child_clients_map,
                        );
                    }
                }
            } else {
                error!("Unexpected: GID {gid} doesn't exists (should've been created prior to state processing)!");
            }
        }
    }

    pub async fn process_device_topology_event(
        &self,
        _device_type: &CGWDeviceType,
        topology_node_mac: &MacAddress,
        evt: CGWUCentralEvent,
        gid: i32,

        // TODO: remove this Arc:
        // Dirty hack for now: pass Arc ref of srv to topo map;
        // Future rework and refactoring would require to separate
        // NB api from being an internal obj of conn_server to be a
        // standalone (singleton?) object.
        conn_server: Arc<CGWConnectionServer>,
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

        // New connected clients (first time seen) client mac -> on AP mac,
        // ssid, band
        let mut clients_join_list: ClientsJoinList = Vec::new();
        // Disconnected clients (seen before, don't see now) client mac from -> AP mac, band
        let mut clients_leave_list: ClientsLeaveList = Vec::new();
        // Migrated client mac -> to (AP mac, ssid, band)
        let mut clients_migrate_list: ClientsMigrateList = Vec::new();

        let mut lock = self.data.write().await;
        if let Some((ref mut topology_map_data, ref mut existing_nodes_map)) = lock.get_mut(&gid) {
            if let CGWUCentralEventType::RealtimeEvent(rt) = evt.evt_type {
                if let CGWUCentralEventRealtimeEventType::WirelessClientJoin(rt_j) = &rt.evt_type {
                    if let Some((
                        existing_client_parent_node_mac,
                        existing_client_node_last_seen_ts,
                        existing_client_ssid,
                        existing_client_band,
                    )) = existing_nodes_map.remove(&rt_j.client)
                    {
                        // We know that existing node for some reason has not _our_ parent mac,
                        // means it's either we're late in seeing this MAC (it's long go
                        // migrated), or this is an actual migration event detected:
                        //   * compare current TS with existing, if current is higher
                        if existing_client_parent_node_mac != evt.serial {
                            if rt.timestamp >= existing_client_node_last_seen_ts {
                                // Update TS, update mac of AP - owner, generate
                                // migrate event
                                existing_nodes_map.insert(
                                    rt_j.client,
                                    (
                                        evt.serial,
                                        rt.timestamp,
                                        rt_j.ssid.clone(),
                                        rt_j.band.clone(),
                                    ),
                                );
                                clients_migrate_list.push((
                                    rt_j.client,
                                    evt.serial,
                                    rt_j.ssid.clone(),
                                    rt_j.band.clone(),
                                ));

                                // Since it's a migrate event, also __remove__ client
                                // from client list of a node which client's migrated.
                                if let Some((_, _, ref mut old_clients_data)) = topology_map_data
                                    .topology_nodes
                                    .get_mut(&existing_client_parent_node_mac)
                                {
                                    let _ = old_clients_data.remove(&rt_j.client);
                                }
                            } else {
                                existing_nodes_map.insert(
                                    rt_j.client,
                                    (
                                        existing_client_parent_node_mac,
                                        existing_client_node_last_seen_ts,
                                        existing_client_ssid,
                                        existing_client_band,
                                    ),
                                );
                            }
                        } else {
                            // Just update the TS
                            existing_nodes_map.insert(
                                rt_j.client,
                                (
                                    evt.serial,
                                    rt.timestamp,
                                    rt_j.ssid.clone(),
                                    rt_j.band.clone(),
                                ),
                            );
                        }
                    } else {
                        // Create new entry, generate connected event
                        existing_nodes_map.insert(
                            rt_j.client,
                            (
                                evt.serial,
                                rt.timestamp,
                                rt_j.ssid.clone(),
                                rt_j.band.clone(),
                            ),
                        );
                        clients_join_list.push((rt_j.client, rt_j.ssid.clone(), rt_j.band.clone()));
                    }
                } else if let CGWUCentralEventRealtimeEventType::WirelessClientLeave(rt_l) =
                    rt.evt_type
                {
                    // Unconditionally remove this client from our clients list.
                    if let Some((_, _, ref mut old_clients_data)) =
                        topology_map_data.topology_nodes.get_mut(&evt.serial)
                    {
                        let _ = old_clients_data.remove(&rt_l.client);
                    }

                    if let Some((
                        existing_client_parent_node_mac,
                        existing_client_node_last_seen_ts,
                        existing_client_ssid,
                        existing_client_band,
                    )) = existing_nodes_map.remove(&rt_l.client)
                    {
                        if existing_client_parent_node_mac == evt.serial {
                            clients_leave_list.push((rt_l.client, existing_client_band));
                        } else {
                            existing_nodes_map.insert(
                                rt_l.client,
                                (
                                    existing_client_parent_node_mac,
                                    existing_client_node_last_seen_ts,
                                    existing_client_ssid,
                                    existing_client_band,
                                ),
                            );
                        }
                    }
                }
            }
        }

        if !clients_join_list.is_empty() {
            Self::handle_clients_join(
                *topology_node_mac,
                clients_join_list,
                gid,
                conn_server.clone(),
            );
        }

        if !clients_leave_list.is_empty() {
            Self::handle_clients_leave(
                *topology_node_mac,
                clients_leave_list,
                gid,
                conn_server.clone(),
            );
        }

        if !clients_migrate_list.is_empty() {
            Self::handle_clients_migrate(clients_migrate_list, gid, conn_server.clone());
        }
    }

    fn add_node(
        map_data: &mut CGWUCentralTopologyMapData,
        new_node_mac: MacAddress,
        new_origin: CGWUCentralTopologyMapNodeOrigin,
        mut new_connections: CGWUCentralTopologyMapConnections,
        connected_child_clients_map: HashMap<MacAddress, ClientsConnectedList>,
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
        // It could be potentially a case when we have to
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
        //
        //  However, all this should be done fast, as we're interested in
        //  only in restoring missing links on a per-port basis.
        if let Some((CGWUCentralTopologyMapNodeOrigin::StateLLDPPeer, old_node_connections, _)) =
            map_data.topology_nodes.remove(&new_node_mac)
        {
            for (old_port, old_conn_data) in old_node_connections.links_list.into_iter() {
                // We want to fill missing port links on old node:
                //   * check if _new_ links have old port entry;
                //   * if not - most likely information about this link
                //     originates from some other UCentral device -
                //     means we have to _restore_ it here
                new_connections
                    .links_list
                    .entry(old_port)
                    .or_insert(old_conn_data);
            }
        }

        map_data.topology_nodes.insert(
            new_node_mac,
            (new_origin, new_connections, connected_child_clients_map),
        );
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
        if let Some((_origin, connections, _connected_child_nodes_map)) =
            map_data.topology_nodes.get(topology_node_mac)
        {
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

            if let Some((_, child_connections, _)) = map_data.topology_nodes.get_mut(&child_mac) {
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

            if let Some((_, parent_connections, _)) = map_data.topology_nodes.get(&parent_mac) {
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
                            grandparent_node_macs.insert(parent_mac, vec![grandparent_mac]);
                        }
                    }
                }
            }
        }

        for (child_mac, grandchild_macs) in grandchild_node_macs.iter() {
            let mut child_node_should_be_removed = true;

            for grandchild_mac in grandchild_macs.iter() {
                if let Some((CGWUCentralTopologyMapNodeOrigin::UCentralDevice, _, _)) =
                    map_data.topology_nodes.get(grandchild_mac)
                {
                    //   (parent)   lldp      (child, parent)     lldp   (child)
                    // UC_DEVICE_1 ------> <unknown lldp switch> ------> UC_DEVICE_2
                    //                                                   ^^^^^^^^^^^
                    // <childs_parent_node_origin> is pointing to UC_DEVICE_2 origin
                    // and since it's a UCentral device, we can't delete
                    // the UC_DEVICE_1 child (<unknown lldp switch>),
                    // because the information about <unknown lldp switch>
                    // can be received from either UC_DEVICE_1 or UC_DEVICE_2.
                    child_node_should_be_removed = false;
                    break;
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
                if let Some((CGWUCentralTopologyMapNodeOrigin::UCentralDevice, _, _)) =
                    map_data.topology_nodes.get(grandparent_mac)
                {
                    //   (parent)   lldp      (child, parent)     lldp   (child)
                    // UC_DEVICE_1 ------> <unknown lldp switch> ------> UC_DEVICE_2
                    // ^^^^^^^^^^^
                    // <grandparent_node_origin> is pointing to UC_DEVICE_1 origin
                    // and since it's a UCentral device, we can't delete
                    // the UC_DEVICE_2 parent (<unknown lldp switch>),
                    // because the information about <unknown lldp switch>
                    // can be received from either UC_DEVICE_1 or UC_DEVICE_2.
                    parent_node_should_be_removed = false;

                    break;
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

            if let Some((origin, _, _)) = map_data.topology_nodes.get(&node_to_remove) {
                match origin {
                    CGWUCentralTopologyMapNodeOrigin::UCentralDevice => (),
                    _ => node_should_be_removed = true,
                }
            }

            if node_should_be_removed {
                let _ = map_data.topology_nodes.remove(&node_to_remove);
            }
        }
    }
}
