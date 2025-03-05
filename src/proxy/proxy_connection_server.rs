use cgw_common::{
    cgw_errors::{Error, Result},
    cgw_tls::cgw_tls_get_cn_from_stream,
    cgw_app_args::AppArgs,
    cgw_devices_cache::CGWDevicesCache,
};

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    sync::Mutex,
};
use tokio::{
    net::TcpStream,
    runtime::Runtime,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        RwLock,
    },
    time::{sleep, Duration},
};

use eui48::MacAddress;

use crate::{
    proxy_runtime::{proxy_get_runtime, ProxyRuntimeType},
    proxy_connection_processor::{
        ProxyConnectionProcessor,
        ProxyConnectionProcessorReqMsg,
    },
    proxy_remote_discovery::ProxyRemoteDiscovery,
};

use lazy_static::lazy_static;

lazy_static! {
    static ref LAST_CGW_INDEX: Mutex<usize> = Mutex::new(0);
}

#[derive(Debug, Clone)]
struct ConnectionInfo {
    mbox_tx: UnboundedSender<ProxyConnectionProcessorReqMsg>,
    connected_to_cgw_id: Option<i32>,
    connected_to_group_id: i32,
    first_assignment: bool,
}

type ProxyConnmapType = Arc<RwLock<HashMap<MacAddress, ConnectionInfo>>>;

#[derive(Debug)]
struct ProxyConnMap {
    map: ProxyConnmapType,
}

impl ProxyConnMap {
    pub fn new() -> Self {
        let hash_map: HashMap<MacAddress, ConnectionInfo> = HashMap::new();
        let map: Arc<RwLock<HashMap<MacAddress, ConnectionInfo>>> = Arc::new(RwLock::new(hash_map));

        ProxyConnMap { map }
    }
}

type ProxyConnectionServerMboxRx = UnboundedReceiver<ProxyConnectionServerReqMsg>;
type ProxyConnectionServerMboxTx = UnboundedSender<ProxyConnectionServerReqMsg>;

// The following pair used internally by server itself to bind
// Processor's Req/Res
#[derive(Debug)]
pub enum ProxyConnectionServerReqMsg {
    // Connection-related messages
    AddNewConnection(
        MacAddress,
        UnboundedSender<ProxyConnectionProcessorReqMsg>,
    ),
    ConnectionClosed(MacAddress),
}

pub struct ProxyConnectionServer {
    // ProxyConnectionServer write into this mailbox,
    // and other corresponding Server task Reads RX counterpart
    mbox_internal_tx: ProxyConnectionServerMboxTx,

    // Object that owns underlying mac:connection map
    connmap: ProxyConnMap,

    // Runtime that schedules all the WSS-messages related tasks
    wss_rx_tx_runtime: Arc<Runtime>,

    // Dedicated runtime (threadpool) for handling internal mbox:
    // ACK/nACK connection, handle duplicates (clone/open) etc.
    mbox_internal_runtime_handle: Arc<Runtime>,

    // Interface used to access all discovered CGW instances
    proxy_remote_discovery: Arc<ProxyRemoteDiscovery>,

    // Internal CGW Devices cache
    // Key: device MAC, Value: Device
    devices_cache: Arc<RwLock<CGWDevicesCache>>,

    last_sync_timestamp: RwLock<i64>,
}

impl ProxyConnectionServer {
    pub async fn new(app_args: &AppArgs) -> Result<Arc<Self>> {
        let wss_runtime_handle = match proxy_get_runtime(ProxyRuntimeType::WssRxTx) {
            Ok(ret_runtime) => match ret_runtime {
                Some(runtime) => runtime,
                None => {
                    return Err(Error::ConnectionServer(format!(
                        "Failed to find runtime type {:?}",
                        ProxyRuntimeType::WssRxTx
                    )));
                }
            },
            Err(e) => {
                return Err(Error::ConnectionServer(format!(
                    "Failed to get runtime type {:?}! Error: {e}",
                    ProxyRuntimeType::WssRxTx
                )));
            }
        };

        let internal_mbox_runtime_handle = match proxy_get_runtime(ProxyRuntimeType::MboxInternal) {
            Ok(ret_runtime) => match ret_runtime {
                Some(runtime) => runtime,
                None => {
                    return Err(Error::ConnectionServer(format!(
                        "Failed to find runtime type {:?}",
                        ProxyRuntimeType::WssRxTx
                    )));
                }
            },
            Err(e) => {
                return Err(Error::ConnectionServer(format!(
                    "Failed to get runtime type {:?}! Error: {e}",
                    ProxyRuntimeType::WssRxTx
                )));
            }
        };

        let (internal_tx, internal_rx) = unbounded_channel::<ProxyConnectionServerReqMsg>();

        let proxy_remote_discovery = match ProxyRemoteDiscovery::new(app_args).await {
            Ok(d) => d,
            Err(e) => {
                error!(
                    "Can't create Proxy Connection server! Remote Discovery create failed! Error: {e}"
                );
                return Err(Error::ConnectionServer(format!(
                            "Can't create Proxy Connection server! Remote Discovery create failed! Error: {e}"
                )));
            }
        };

        let server = Arc::new(ProxyConnectionServer {
            connmap: ProxyConnMap::new(),
            wss_rx_tx_runtime: wss_runtime_handle,
            mbox_internal_runtime_handle: internal_mbox_runtime_handle,
            mbox_internal_tx: internal_tx,
            proxy_remote_discovery: Arc::new(proxy_remote_discovery),
            devices_cache: Arc::new(RwLock::new(CGWDevicesCache::new())),
            last_sync_timestamp: RwLock::new(0i64),
        });

        let server_clone = server.clone();
        // Task for processing mbox_internal_rx, task owns the RX part
        server.mbox_internal_runtime_handle.spawn(async move {
            server_clone.process(internal_rx).await;
        });

        // Sync RAM cache with Redis.
        if let Err(e) = server
            .proxy_remote_discovery
            .sync_devices_cache_with_redis(server.devices_cache.clone())
            .await
        {
            error!("Failed to sync Device cache! Error: {e}");
        }

        Ok(server)
    }

    pub async fn ack_connection(
        self: Arc<Self>,
        socket: TcpStream,
        tls_acceptor: tokio_rustls::TlsAcceptor,
        addr: SocketAddr,
    ) {
        // Only ACK connection. We will either drop it or accept it once processor starts
        // (we'll handle it via "mailbox" notify handle in process)
        let server_clone = self.clone();

        self.wss_rx_tx_runtime.spawn(async move {
            // Accept the TLS connection.
            let (client_cn, tls_stream) = match tls_acceptor.accept(socket).await {
                Ok(stream) => match cgw_tls_get_cn_from_stream(&stream).await {
                    Ok(cn) => (cn, stream),
                    Err(e) => {
                        error!("Failed to read client CN! Error: {e}");
                        return;
                    }
                },
                Err(e) => {
                    error!("Failed to accept connection: Error {e}");
                    return;
                }
            };

            let conn_processor = ProxyConnectionProcessor::new(server_clone, addr);
            if let Err(e) = conn_processor
                .start(tls_stream, client_cn)
                .await
            {
                error!("Failed to start connection processor! Error: {e}");
            }
        });
    }

    async fn process(self: Arc<Self>, mut rx_mbox: ProxyConnectionServerMboxRx) {
        debug!("process entry");

        let buf_capacity = 1000;
        let mut buf: Vec<ProxyConnectionServerReqMsg> = Vec::with_capacity(buf_capacity);
        let mut num_of_msg_read = 0;
        let should_resync: Arc<RwLock<bool>> = Arc::new(RwLock::new(false));

        loop {
            // Handle Redis updates that might require a resync
            let mut timestamp = self.last_sync_timestamp.write().await;
            if let Ok(true) = self.proxy_remote_discovery.check_redis_updated(&mut *timestamp).await {
                *should_resync.write().await = true;

                if let Err(e) = self.proxy_remote_discovery.sync_remote_cgw_map().await {
                    error!("Failed to sync remote CGW map after detecting changes: {}", e);
                }

                if let Err(e) = self.proxy_remote_discovery.sync_gid_to_cgw_map().await {
                    error!("Failed to sync remote CGW gid to map after detecting changes: {}", e);
                }

                if let Err(e) = self
                    .proxy_remote_discovery
                    .sync_devices_cache_with_redis(self.devices_cache.clone())
                    .await
                {
                    error!("Failed to sync Device cache! Error: {e}");
                }

                info!("Redis update detected, scheduling resync");
            }

            // Handle periodic tick for connection management
            let mut resync_needed = *should_resync.read().await;
            if resync_needed {
                debug!("Running periodic connection management: should resync = {}", resync_needed);

                if let Err(e) = self.manage_device_connections(&mut resync_needed).await {
                    error!("Error in device connection management: {}", e);
                }

                *should_resync.write().await = resync_needed;
            }

            // Handle incoming messages
            if num_of_msg_read < buf_capacity {
                // Try to recv_many, but don't sleep too much
                // in case if no messaged pending
                let rd_num = tokio::select! {
                    v = rx_mbox.recv_many(&mut buf, buf_capacity - num_of_msg_read) => {
                        v
                    }
                    _v = sleep(Duration::from_millis(10)) => {
                        0
                    }
                };
                num_of_msg_read += rd_num;

                // We read some messages, try to continue and read more
                // If none read - break from recv, process all buffers that've
                // been filled-up so far (both local and remote).
                // Upon done - repeat.
                if rd_num >= 1 {
                    if num_of_msg_read < 100 {
                        continue;
                    }
                } else if num_of_msg_read == 0 {
                    continue;
                }
            }

            let mut connmap_w_lock = self.connmap.map.write().await;

            while !buf.is_empty() {
                let msg = buf.remove(0);

                if let ProxyConnectionServerReqMsg::AddNewConnection(
                    device_mac,
                    conn_processor_mbox_tx,
                ) = msg
                {
                    // if connection is unique: simply insert new conn
                    //
                    // if duplicate exists: notify server about such incident.
                    // it's up to server to notify underlying task that it should
                    // drop the connection.
                    // from now on simply insert new connection into hashmap and proceed on
                    // processing it.
                    if let Some(conn_info) = connmap_w_lock.remove(&device_mac) {
                        tokio::spawn(async move {
                            warn!("Duplicate connection (mac: {}) detected! Closing OLD connection in favor of NEW!", device_mac);
                            let msg: ProxyConnectionProcessorReqMsg =
                                ProxyConnectionProcessorReqMsg::AddNewConnectionShouldClose;
                            if let Err(e) = conn_info.mbox_tx.send(msg) {
                                warn!("Failed to send notification about duplicate connection! Error: {e}")
                            }
                        });
                    }

                    // clone a sender handle, as we still have to send ACK back using underlying
                    // tx mbox handle
                    let conn_processor_mbox_tx_clone = conn_processor_mbox_tx.clone();
                    let device_mac_clone = device_mac.clone();
                    let should_resync_clone = should_resync.clone();

                    info!(
                        "Connection map: connection with {} established, new num_of_connections: {}",
                        device_mac,
                        connmap_w_lock.len() + 1
                    );

                    // Trigger resync on next tick
                    *should_resync_clone.write().await = true;

                    let msg: ProxyConnectionProcessorReqMsg = ProxyConnectionProcessorReqMsg::AddNewConnectionAck;

                    if let Err(e) = conn_processor_mbox_tx_clone.send(msg) {
                        error!("Failed to send NewConnection message! Error: {e}");
                    } else {
                        let updated_con_info = ConnectionInfo {
                            mbox_tx: conn_processor_mbox_tx_clone,
                            connected_to_cgw_id: None,
                            connected_to_group_id: 0,
                            first_assignment: true,
                        };

                        debug!("Device {} connected, pending group assignment", device_mac_clone);
                        connmap_w_lock.insert(device_mac_clone, updated_con_info);
                    }
                } else if let ProxyConnectionServerReqMsg::ConnectionClosed(device_mac) = msg {
                    // Check if this connection exists
                    if let Some(conn_info) = connmap_w_lock.get_mut(&device_mac) {
                        // If the connection has an assigned CGW, just reset that assignment
                        if conn_info.connected_to_cgw_id.is_some() {
                            info!(
                                "Connection map: CGW connection broken for {}, resetting CGW assignment",
                                device_mac
                            );
                            conn_info.connected_to_cgw_id = None;
                        } else {
                            info!(
                                "Connection map: removed {} serial from connmap, new num_of_connections: {}",
                                device_mac,
                                connmap_w_lock.len() - 1
                            );
                            connmap_w_lock.remove(&device_mac);
                        }
                    } else {
                        debug!("Received ConnectionClosed for unknown device: {}", device_mac);
                    }
                }
            }

            buf.clear();
            num_of_msg_read = 0;
        }
    }

    async fn manage_device_connections(&self, should_resync: &mut bool) -> Result<()> {
        if !*should_resync {
            return Ok(());
        }

        *should_resync = false;

        // Sync remote cgw on change
        if let Err(e) = self.proxy_remote_discovery.sync_remote_cgw_map().await {
            error!("Failed to sync remote CGW map: {}", e);
        }

        if let Err(e) = self.proxy_remote_discovery.sync_gid_to_cgw_map().await {
            error!("Failed to sync remote CGW gid to map after detecting changes: {}", e);
        }

        info!("Managing device connections, resync required");
        let devices_cache_read = self.devices_cache.read().await;
        let mut connmap_w_lock = self.connmap.map.write().await;

        for (mac, conn_info) in connmap_w_lock.iter_mut() {
            debug!("Conn info: \n {:?} \n", conn_info);
            // Handle unassigned connections
            if conn_info.connected_to_group_id == 0 {
                if let Some(device) = devices_cache_read.get_device(mac) {
                    // Device found in cache, get group ID and owner
                    let device_group_id = device.get_device_group_id();
                    if device_group_id != 0 {
                        if let Some(group_owner_id) = self.proxy_remote_discovery.get_infra_group_owner_id(device_group_id).await {
                            if let Err(e) = self.set_peer_connection(mac, conn_info, group_owner_id, device_group_id).await {
                                error!("Failed to set peer for device {}: {}", mac, e);
                                continue;
                            }
                            debug!("Assigned device {} to group {} on CGW {}", mac, device_group_id, group_owner_id);
                            continue;
                        }
                    } else {
                        if conn_info.first_assignment {
                            debug!("Device {} first assignment, will try again later", mac);
                            // Set should_resync to true so we retry later
                            conn_info.first_assignment = false;
                            *should_resync = true;
                            continue;
                        }
                        if let None = conn_info.connected_to_cgw_id {
                            match self.get_round_robin_cgw_id().await {
                                Ok(round_robin_cgw_id) => {
                                    if let Err(e) = self.set_peer_connection(mac, conn_info, round_robin_cgw_id, 0).await {
                                        error!("Failed to set round-robin peer for device {}: {}", mac, e);
                                    }
                                    debug!("Assigned unregistered device {} to round-robin CGW {}", mac, round_robin_cgw_id);
                                },
                                Err(e) => {
                                    error!("Failed to get round-robin CGW ID: {}", e);
                                }
                            }
                        }
                        warn!("No CGW assigned for group ID {} of device {}", device_group_id, mac);
                        continue;
                    }
                }

                // If device not in cache
                if let None = devices_cache_read.get_device(mac) {
                    if conn_info.first_assignment {
                        debug!("Device {} first assignment, will try again later", mac);
                        // Set should_resync to true so we retry later
                        conn_info.first_assignment = false;
                        *should_resync = true;
                        continue;
                    }

                    if let None = conn_info.connected_to_cgw_id {
                        match self.get_round_robin_cgw_id().await {
                            Ok(round_robin_cgw_id) => {
                                if let Err(e) = self.set_peer_connection(mac, conn_info, round_robin_cgw_id, 0).await {
                                    error!("Failed to set round-robin peer for device {}: {}", mac, e);
                                    continue;
                                }
                                debug!("Assigned unregistered device {} to round-robin CGW {}", mac, round_robin_cgw_id);
                            },
                            Err(e) => {
                                error!("Failed to get round-robin CGW ID: {}", e);
                            }
                        }
                        continue;
                    } else {
                        // Already has CGW assigned, skip
                        continue;
                    }
                }
            } else {
                // Handle assigned connections (group_id != 0)
                // Check if device exists in cache
                let device_result = devices_cache_read.get_device(mac);
                if let None = device_result  {
                    // Device was in group but not in cache anymore
                    // Get round-robin CGW ID
                    match self.get_round_robin_cgw_id().await {
                        Ok(round_robin_cgw_id) => {
                            // Set peer connection
                            if let Err(e) = self.set_peer_connection(mac, conn_info, round_robin_cgw_id, 0).await {
                                error!("Failed to reset peer for removed device {}: {}", mac, e);
                                continue;
                            }
                            debug!("Device {} no longer in cache, reassigned to round-robin CGW {}", mac, round_robin_cgw_id);
                        },
                        Err(e) => {
                            error!("Failed to get round-robin CGW ID: {}", e);
                        }
                    }
                } else if let Some(cached_device) = device_result {
                    // Device exists in cache
                    let device_group_id = cached_device.get_device_group_id();
                    // Check if group ID changed
                    if conn_info.connected_to_group_id != device_group_id {
                        if let Some(group_owner_id) = self.proxy_remote_discovery.get_infra_group_owner_id(device_group_id).await {
                            if let Err(e) = self.set_peer_connection(mac, conn_info, group_owner_id, device_group_id).await {
                                error!("Failed to update peer for device {} with new group {}: {}", mac, device_group_id, e);
                                continue;
                            }
                            debug!("Updated device {} from group {} to group {} on CGW {}",
                                   mac, conn_info.connected_to_group_id, device_group_id, group_owner_id);
                        } else {
                            warn!("No CGW assigned for updated group ID {} of device {}", device_group_id, mac);
                        }
                    } else {
                        // Group hasn't changed, check if CGW ID was changed but still in with same GID
                        if let Some(group_owner_id) = self.proxy_remote_discovery.get_infra_group_owner_id(device_group_id).await {
                            if let Some(current_cgw_id) = conn_info.connected_to_cgw_id {
                                if group_owner_id != current_cgw_id {
                                    if let Err(e) = self.set_peer_connection(mac, conn_info, group_owner_id, device_group_id).await {
                                        error!("Failed to update peer for device {} with new group owner (CGD) ID {}: {}", mac, group_owner_id, e);
                                        continue;
                                    }
                                    debug!("Updated device {} CGW from {} to {} (same group {})",
                                        mac, current_cgw_id, group_owner_id, device_group_id);
                                }
                            } else {
                                debug!("Unusual state: Device {} has group {} but No CGW assignment.", mac, device_group_id);
                            }
                        } else {
                            warn!("No CGW assigned for current group ID {} of device {}", device_group_id, mac);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn enqueue_mbox_message_to_proxy_server(&self, req: ProxyConnectionServerReqMsg) {
        if let Err(e) = self.mbox_internal_tx.send(req) {
            error!("Failed to send message to Proxy server (internal)! Error: {e}");
        }
    }

    async fn set_peer_connection(&self,
        mac: &MacAddress,
        conn_info: &mut ConnectionInfo,
        cgw_id: i32,
        group_id: i32
    ) -> Result<()> {
        debug!("set_peer_connection, cgw_id: {}, group_id: {}", cgw_id, group_id);

        conn_info.connected_to_cgw_id = Some(cgw_id);
        conn_info.connected_to_group_id = group_id;

        // Get the socket address for the CGW instance
        let (host, port) = match self.proxy_remote_discovery.get_shard_host_and_wss_port(cgw_id).await {
            Ok((host, port)) => (host, port),
            Err(e) => {
                error!("Failed to get peer address for device {}: {}", mac, e);
                return Err(Error::ConnectionServer(format!(
                    "Failed to get peer address for device {}: {}", mac, e
                )));
            }
        };

        let peer_msg = ProxyConnectionProcessorReqMsg::SetPeer(format!("{}:{}", host, port));
        if let Err(e) = conn_info.mbox_tx.send(peer_msg) {
            error!("Failed to send ConnectToPeer message for device {}: {}", mac, e);
            return Err(Error::ConnectionServer(format!(
                "Failed to send ConnectToPeer message for device {}: {}", mac, e
            )));
        }

        Ok(())
    }

    async fn get_round_robin_cgw_id(&self) -> Result<i32> {
        let available_cgw_ids = match self.proxy_remote_discovery.get_available_cgw_ids().await {
            Ok(ids) => ids,
            Err(e) => {
                return Err(Error::ConnectionServer(format!(
                    "Failed to get available CGW IDs: {}", e
                )));
            }
        };

        if available_cgw_ids.is_empty() {
            return Err(Error::ConnectionServer(
                "No available CGW IDs for round-robin assignment".to_string()
            ));
        }

        let index = {
            let mut last_index = LAST_CGW_INDEX.lock().unwrap();
            let current = *last_index;
            // Update for next time
            *last_index = (current + 1) % available_cgw_ids.len();
            current
        };

        debug!("Selected CGW ID {} for round-robin (index {})", available_cgw_ids[index], index);

        Ok(available_cgw_ids[index])
    }
}
