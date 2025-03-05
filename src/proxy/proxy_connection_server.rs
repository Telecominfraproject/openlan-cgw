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

#[derive(Debug, Clone)]
struct ConnectionInfo {
    mbox_tx: UnboundedSender<ProxyConnectionProcessorReqMsg>,
    connected_to_cgw_id: Option<i32>,
    connected_to_group_id: i32,
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
        SocketAddr,
        UnboundedSender<ProxyConnectionProcessorReqMsg>,
    ),
    ConnectionClosed(MacAddress),
}

pub struct ProxyConnectionServer {
    local_cgw_id: i32,
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

    // Dedicated runtime (threadpool) for handling (relaying) msgs:
    // relay-task is spawned inside it, and the produced stream of
    // remote-cgw messages is being relayed inside this context
    mbox_relay_msg_runtime_handle: Arc<Runtime>,

    // Interface used to access all discovered CGW instances
    proxy_remote_discovery: Arc<ProxyRemoteDiscovery>,

    // Internal CGW Devices cache
    // Key: device MAC, Value: Device
    devices_cache: Arc<RwLock<CGWDevicesCache>>,
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

        let relay_msg_mbox_runtime_handle = match proxy_get_runtime(ProxyRuntimeType::MboxRelay) {
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
            local_cgw_id: app_args.cgw_id,
            connmap: ProxyConnMap::new(),
            wss_rx_tx_runtime: wss_runtime_handle,
            mbox_internal_runtime_handle: internal_mbox_runtime_handle,
            mbox_internal_tx: internal_tx,
            proxy_remote_discovery: Arc::new(proxy_remote_discovery),
            mbox_relay_msg_runtime_handle: relay_msg_mbox_runtime_handle,
            devices_cache: Arc::new(RwLock::new(CGWDevicesCache::new())),
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

        loop {
            if num_of_msg_read < buf_capacity {
                // Try to recv_many, but don't sleep too much
                // in case if no messaged pending and we have
                // TODO: rework?
                // Currently recv_many may sleep if previous read >= 1,
                // but no new messages pending
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
                    ip_addr,
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
                    let ip_addr_clone = ip_addr.clone();
                    let device_mac_clone = device_mac.clone();
                    let server_clone = self.clone();

                    info!(
                        "Connection map: connection with {} established, new num_of_connections: {}",
                        device_mac,
                        connmap_w_lock.len() + 1
                    );

                    // Insert the new connection info with default values
                    let con_info = ConnectionInfo {
                        mbox_tx: conn_processor_mbox_tx,
                        connected_to_cgw_id: None,
                        connected_to_group_id: -1,
                    };
                    connmap_w_lock.insert(device_mac, con_info);

                    tokio::spawn(async move {
                        let msg: ProxyConnectionProcessorReqMsg =
                        ProxyConnectionProcessorReqMsg::AddNewConnectionAck;
                
                        if let Err(e) = conn_processor_mbox_tx_clone.send(msg) {
                            error!("Failed to send NewConnection message! Error: {e}");
                        } else {
                            // If successfully sent the ACK message, now look up the CGW info using our new function
                            if let Some((cgw_address, cgw_id, group_id)) = server_clone.get_cgw_address_for_device(&device_mac_clone).await {
                                // Send the SetPeer message with the dynamic address
                                debug!("\n\n\n CGW ADDDREEESSSSS {})", cgw_address);
                                let peer_msg = ProxyConnectionProcessorReqMsg::SetPeer(cgw_address);

                                if let Err(e) = conn_processor_mbox_tx_clone.send(peer_msg) {
                                    error!("Failed to send SetPeer message! Error: {e}");
                                } else {
                                    // Only update connmap if both messages were sent successfully
                                    // Create updated connection info for the update
                                    let updated_con_info = ConnectionInfo {
                                        mbox_tx: conn_processor_mbox_tx_clone,
                                        connected_to_cgw_id: Some(cgw_id),
                                        connected_to_group_id: group_id,
                                    };
                                    server_clone.connmap_update(device_mac_clone, updated_con_info).await;
                                    debug!("Device {} connected to CGW ID {} (group {})", device_mac_clone, cgw_id, group_id);
                                }
                            } else {
                                error!("Failed to get CGW address for device {}", device_mac_clone);
                            }
                        }
                    });
                } else if let ProxyConnectionServerReqMsg::ConnectionClosed(device_mac) = msg {
                    info!(
                        "Connection map: removed {} serial from connmap, new num_of_connections: {}",
                        device_mac,
                        connmap_w_lock.len() - 1
                    );
                    connmap_w_lock.remove(&device_mac);
                }
            }

            buf.clear();
            num_of_msg_read = 0;
        }
    }

    async fn connmap_update(&self, device_mac: MacAddress, con_info: ConnectionInfo) {
        let mut connmap_w_lock = self.connmap.map.write().await;

        connmap_w_lock.insert(device_mac, con_info);
        debug!("Updated connection info for device: {}", device_mac);
    }

    pub async fn enqueue_mbox_message_to_proxy_server(&self, req: ProxyConnectionServerReqMsg) {
        if let Err(e) = self.mbox_internal_tx.send(req) {
            error!("Failed to send message to Proxy server (internal)! Error: {e}");
        }
    }

    pub async fn get_cgw_address_for_device(&self, device_mac: &MacAddress) -> Option<(std::net::SocketAddr, i32, i32)> {
        // Look up the device in the devices cache to find its group
        let device_group_id = {
            let devices_cache_read = self.devices_cache.read().await;
            match devices_cache_read.get_device(device_mac) {
                Some(device) => device.get_device_group_id(),
                None => {
                    error!("Device {} not found in cache, cannot determine group ID", device_mac);
                    return None;
                }
            }
        };
        
        // Now look up which CGW shard handles this group
        let cgw_id = match self.proxy_remote_discovery.get_infra_group_owner_id(device_group_id).await {
            Some(id) => id,
            None => {
                error!("No CGW assigned for group ID {}", device_group_id);
                return None;
            }
        };
        
        // Now get the CGW shard info (IP and port)
        match self.proxy_remote_discovery.get_shard_host_and_server_port(cgw_id).await {
            Ok((host, port)) => {
                // Construct a SocketAddr from the shard info
                match format!("{}:{}", host, port).parse() {
                    Ok(addr) => Some((addr, cgw_id, device_group_id)),
                    Err(e) => {
                        error!("Failed to parse CGW address for shard {}: Error {}", cgw_id, e);
                        None
                    }
                }
            },
            Err(e) => {
                error!("CGW ID {} not found in remote_cgws_map: {}", cgw_id, e);
                None
            }
        }
    }
    
}