use crate::cgw_device::{
    cgw_detect_device_chages, CGWDevice, CGWDeviceCapabilities, CGWDeviceState,
};
use crate::cgw_nb_api_listener::{
    cgw_construct_device_capabilities_changed_msg, cgw_construct_device_enqueue_response,
    cgw_construct_foreign_infra_connection_msg, cgw_construct_infra_group_create_response,
    cgw_construct_infra_group_delete_response, cgw_construct_infra_group_device_add_response,
    cgw_construct_infra_group_device_del_response, cgw_construct_rebalance_group_response,
    cgw_construct_unassigned_infra_connection_msg,
};
use crate::cgw_tls::cgw_tls_get_cn_from_stream;
use crate::cgw_ucentral_messages_queue_manager::{
    CGWUCentralMessagesQueueItem, CGW_MESSAGES_QUEUE,
};
use crate::cgw_ucentral_parser::cgw_ucentral_parse_command_message;
use crate::cgw_ucentral_topology_map::CGWUCentralTopologyMap;
use crate::AppArgs;

use crate::{
    cgw_connection_processor::{CGWConnectionProcessor, CGWConnectionProcessorReqMsg},
    cgw_db_accessor::CGWDBInfrastructureGroup,
    cgw_devices_cache::CGWDevicesCache,
    cgw_metrics::{
        CGWMetrics, CGWMetricsCounterOpType, CGWMetricsCounterType, CGWMetricsHealthComponent,
        CGWMetricsHealthComponentStatus,
    },
    cgw_nb_api_listener::CGWNBApiClient,
    cgw_remote_discovery::CGWRemoteDiscovery,
};

use crate::cgw_errors::{Error, Result};

use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::{
    net::TcpStream,
    runtime::{Builder, Runtime},
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        RwLock,
    },
    time::{sleep, Duration},
};

use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Map, Value};

use serde::{Deserialize, Serialize};

use uuid::Uuid;

use eui48::MacAddress;

type CGWConnmapType =
    Arc<RwLock<HashMap<MacAddress, UnboundedSender<CGWConnectionProcessorReqMsg>>>>;

#[derive(Debug)]
struct CGWConnMap {
    map: CGWConnmapType,
}

impl CGWConnMap {
    pub fn new() -> Self {
        let hash_map: HashMap<MacAddress, UnboundedSender<CGWConnectionProcessorReqMsg>> =
            HashMap::new();
        let map: Arc<RwLock<HashMap<MacAddress, UnboundedSender<CGWConnectionProcessorReqMsg>>>> =
            Arc::new(RwLock::new(hash_map));

        CGWConnMap { map }
    }
}

type CGWConnectionServerMboxRx = UnboundedReceiver<CGWConnectionServerReqMsg>;
type CGWConnectionServerMboxTx = UnboundedSender<CGWConnectionServerReqMsg>;
type CGWConnectionServerNBAPIMboxTx = UnboundedSender<CGWConnectionNBAPIReqMsg>;
type CGWConnectionServerNBAPIMboxRx = UnboundedReceiver<CGWConnectionNBAPIReqMsg>;

// The following pair used internally by server itself to bind
// Processor's Req/Res
#[derive(Debug)]
pub enum CGWConnectionServerReqMsg {
    // Connection-related messages
    AddNewConnection(
        MacAddress,
        CGWDeviceCapabilities,
        UnboundedSender<CGWConnectionProcessorReqMsg>,
    ),
    ConnectionClosed(MacAddress),
}

#[derive(Debug)]
pub enum CGWConnectionNBAPIReqMsgOrigin {
    FromNBAPI,
    FromRemoteCGW,
}

#[derive(Debug)]
pub enum CGWConnectionNBAPIReqMsg {
    // Enqueue Key, Request, bool = isMessageRelayed
    EnqueueNewMessageFromNBAPIListener(String, String, CGWConnectionNBAPIReqMsgOrigin),
}

pub struct CGWConnectionServer {
    // Allow client certificate mismatch
    allow_mismatch: bool,

    local_cgw_id: i32,
    // CGWConnectionServer write into this mailbox,
    // and other correspondig Server task Reads RX counterpart
    mbox_internal_tx: CGWConnectionServerMboxTx,

    // Object that owns underlying mac:connection map
    connmap: CGWConnMap,

    // Runtime that schedules all the WSS-messages related tasks
    wss_rx_tx_runtime: Arc<Runtime>,

    // Dedicated runtime (threadpool) for handling internal mbox:
    // ACK/nACK connection, handle duplicates (clone/open) etc.
    mbox_internal_runtime_handle: Arc<Runtime>,

    // Dedicated runtime (threadpool) for handling NB-API mbox:
    // RX NB-API requests, parse, relay (if needed)
    mbox_nb_api_runtime_handle: Arc<Runtime>,

    // Dedicated runtime (threadpool) for handling NB-API TX routine:
    // TX NB-API requests (if async send is needed)
    mbox_nb_api_tx_runtime_handle: Arc<Runtime>,

    // Dedicated runtime (threadpool) for handling (relaying) msgs:
    // relay-task is spawned inside it, and the produced stream of
    // remote-cgw messages is being relayed inside this context
    mbox_relay_msg_runtime_handle: Arc<Runtime>,

    // Dedicated runtime (threadpool) for handling disconnected devices message queue
    // Iterate over list of disconnected devices - dequeue aged messages
    queue_timeout_handle: Arc<Runtime>,

    // CGWConnectionServer write into this mailbox,
    // and other correspondig NB API client is responsible for doing an RX over
    // receive handle counterpart
    nb_api_client: Arc<CGWNBApiClient>,

    // Interface used to access all discovered CGW instances
    // (used for relaying non-local CGW requests from NB-API to target CGW)
    cgw_remote_discovery: Arc<CGWRemoteDiscovery>,

    // Handler that helps this object to wrap relayed NB-API messages
    // dedicated for this particular local CGW instance
    mbox_relayed_messages_handle: CGWConnectionServerNBAPIMboxTx,

    // Internal CGW Devices cache
    // Key: device MAC, Value: Device
    devices_cache: Arc<RwLock<CGWDevicesCache>>,

    // User-supplied arguments can disable state/realtime events
    // processing by underlying connections processors.
    pub feature_topomap_enabled: bool,
}

enum CGWNBApiParsedMsgType {
    InfrastructureGroupCreate,
    InfrastructureGroupDelete,
    InfrastructureGroupInfraAdd(Vec<MacAddress>),
    InfrastructureGroupInfraDel(Vec<MacAddress>),
    InfrastructureGroupInfraMsg(MacAddress, String),
    RebalanceGroups,
}

struct CGWNBApiParsedMsg {
    uuid: Uuid,
    gid: i32,
    msg_type: CGWNBApiParsedMsgType,
}

impl CGWNBApiParsedMsg {
    fn new(uuid: Uuid, gid: i32, msg_type: CGWNBApiParsedMsgType) -> CGWNBApiParsedMsg {
        CGWNBApiParsedMsg {
            uuid,
            gid,
            msg_type,
        }
    }
}

impl CGWConnectionServer {
    pub async fn new(app_args: &AppArgs) -> Result<Arc<Self>> {
        let wss_runtime_handle = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(app_args.wss_args.wss_t_num)
                .thread_name_fn(|| {
                    static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                    let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                    format!("cgw-wss-t-{}", id)
                })
                .thread_stack_size(3 * 1024 * 1024)
                .enable_all()
                .build()?,
        );
        let internal_mbox_runtime_handle = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("cgw-mbox")
                .thread_stack_size(1024 * 1024)
                .enable_all()
                .build()?,
        );
        let nb_api_mbox_runtime_handle = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("cgw-mbox-nbapi")
                .thread_stack_size(1024 * 1024)
                .enable_all()
                .build()?,
        );
        let relay_msg_mbox_runtime_handle = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("cgw-relay-mbox-nbapi")
                .thread_stack_size(1024 * 1024)
                .enable_all()
                .build()?,
        );
        let nb_api_mbox_tx_runtime_handle = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("cgw-mbox-nbapi-tx")
                .thread_stack_size(1024 * 1024)
                .enable_all()
                .build()?,
        );
        let queue_timeout_handle = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("cgw-queue-timeout")
                .thread_stack_size(1024 * 1024)
                .enable_all()
                .build()?,
        );

        let (internal_tx, internal_rx) = unbounded_channel::<CGWConnectionServerReqMsg>();
        let (nb_api_tx, nb_api_rx) = unbounded_channel::<CGWConnectionNBAPIReqMsg>();

        // Give NB API client a handle where it can do a TX (CLIENT -> CGW_SERVER)
        // RX is handled in internal_mbox of CGW_Server
        let nb_api_c = match CGWNBApiClient::new(app_args.cgw_id, &app_args.kafka_args, &nb_api_tx)
        {
            Ok(c) => c,
            Err(e) => {
                error!(
                    "Can't create CGW Connection server: NB API client create failed: {:?}",
                    e
                );
                return Err(Error::ConnectionServer(format!(
                    "Can't create CGW Connection server: NB API client create failed: {:?}",
                    e
                )));
            }
        };

        let cgw_remote_discovery = match CGWRemoteDiscovery::new(app_args).await {
            Ok(d) => d,
            Err(e) => {
                error!(
                    "Can't create CGW Connection server: Remote Discovery create failed: {:?}",
                    e
                );
                return Err(Error::ConnectionServer(format!(
                    "Can't create CGW Connection server: Remote Discovery create failed: {:?}",
                    e,
                )));
            }
        };

        let server = Arc::new(CGWConnectionServer {
            allow_mismatch: app_args.wss_args.allow_mismatch,
            local_cgw_id: app_args.cgw_id,
            connmap: CGWConnMap::new(),
            wss_rx_tx_runtime: wss_runtime_handle,
            mbox_internal_runtime_handle: internal_mbox_runtime_handle,
            mbox_nb_api_runtime_handle: nb_api_mbox_runtime_handle,
            mbox_nb_api_tx_runtime_handle: nb_api_mbox_tx_runtime_handle,
            mbox_internal_tx: internal_tx,
            queue_timeout_handle,
            nb_api_client: nb_api_c,
            cgw_remote_discovery: Arc::new(cgw_remote_discovery),
            mbox_relayed_messages_handle: nb_api_tx,
            mbox_relay_msg_runtime_handle: relay_msg_mbox_runtime_handle,
            devices_cache: Arc::new(RwLock::new(CGWDevicesCache::new())),
            feature_topomap_enabled: app_args.feature_topomap_enabled,
        });

        let server_clone = server.clone();
        // Task for processing mbox_internal_rx, task owns the RX part
        server.mbox_internal_runtime_handle.spawn(async move {
            server_clone.process_internal_mbox(internal_rx).await;
        });

        let server_clone = server.clone();
        server.mbox_nb_api_runtime_handle.spawn(async move {
            server_clone.process_internal_nb_api_mbox(nb_api_rx).await;
        });

        server.queue_timeout_handle.spawn(async move {
            let queue_lock = CGW_MESSAGES_QUEUE.read().await;
            queue_lock.start_queue_timeout_manager().await;
        });

        // Sync RAM cache with PostgressDB.
        server
            .cgw_remote_discovery
            .sync_device_to_gid_cache(server.devices_cache.clone())
            .await;

        tokio::spawn(async move {
            CGWMetrics::get_ref()
                .change_component_health_status(
                    CGWMetricsHealthComponent::ConnectionServer,
                    CGWMetricsHealthComponentStatus::Ready,
                )
                .await;
        });

        Ok(server)
    }

    pub async fn enqueue_mbox_message_to_cgw_server(&self, req: CGWConnectionServerReqMsg) {
        let _ = self.mbox_internal_tx.send(req);
    }

    pub fn enqueue_mbox_message_from_device_to_nb_api_c(
        &self,
        group_id: i32,
        req: String,
    ) -> Result<()> {
        let nb_api_client_clone = self.nb_api_client.clone();
        tokio::spawn(async move {
            let key = group_id.to_string();
            let _ = nb_api_client_clone
                .enqueue_mbox_message_from_cgw_server(key, req)
                .await;
        });

        Ok(())
    }

    pub fn enqueue_mbox_message_from_cgw_to_nb_api(&self, gid: i32, req: String) {
        let nb_api_client_clone = self.nb_api_client.clone();
        self.mbox_nb_api_tx_runtime_handle.spawn(async move {
            let _ = nb_api_client_clone
                .enqueue_mbox_message_from_cgw_server(gid.to_string(), req)
                .await;
        });
    }

    pub async fn enqueue_mbox_relayed_message_to_cgw_server(&self, key: String, req: String) {
        let msg = CGWConnectionNBAPIReqMsg::EnqueueNewMessageFromNBAPIListener(
            key,
            req,
            CGWConnectionNBAPIReqMsgOrigin::FromRemoteCGW,
        );
        let _ = self.mbox_relayed_messages_handle.send(msg);
    }

    fn parse_nbapi_msg(&self, pload: &str) -> Option<CGWNBApiParsedMsg> {
        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupCreate {
            r#type: String,
            infra_group_id: String,
            infra_name: String,
            infra_shard_id: i32,
            uuid: Uuid,
        }
        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupDelete {
            r#type: String,
            infra_group_id: String,
            uuid: Uuid,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupInfraAdd {
            r#type: String,
            infra_group_id: String,
            infra_group_infra_devices: Vec<MacAddress>,
            uuid: Uuid,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupInfraDel {
            r#type: String,
            infra_group_id: String,
            infra_group_infra_devices: Vec<MacAddress>,
            uuid: Uuid,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupMsgJSON {
            r#type: String,
            infra_group_id: String,
            mac: MacAddress,
            msg: Map<String, Value>,
            uuid: Uuid,
        }

        let map: Map<String, Value> = serde_json::from_str(pload).ok()?;

        let rc = map.get(&String::from("type"))?;
        let msg_type = rc.as_str()?;

        let rc = map.get(&String::from("infra_group_id"))?;
        let group_id: i32 = rc.as_str()?.parse().ok()?;

        match msg_type {
            "infrastructure_group_create" => {
                let json_msg: InfraGroupCreate = serde_json::from_str(pload).ok()?;
                return Some(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::InfrastructureGroupCreate,
                ));
            }
            "infrastructure_group_delete" => {
                let json_msg: InfraGroupDelete = serde_json::from_str(pload).ok()?;
                return Some(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::InfrastructureGroupDelete,
                ));
            }
            "infrastructure_group_device_add" => {
                let json_msg: InfraGroupInfraAdd = serde_json::from_str(pload).ok()?;
                return Some(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::InfrastructureGroupInfraAdd(
                        json_msg.infra_group_infra_devices,
                    ),
                ));
            }
            "infrastructure_group_device_del" => {
                let json_msg: InfraGroupInfraDel = serde_json::from_str(pload).ok()?;
                return Some(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::InfrastructureGroupInfraDel(
                        json_msg.infra_group_infra_devices,
                    ),
                ));
            }
            "infrastructure_group_device_message" => {
                let json_msg: InfraGroupMsgJSON = serde_json::from_str(pload).ok()?;
                debug!("{:?}", json_msg);
                return Some(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::InfrastructureGroupInfraMsg(
                        json_msg.mac,
                        serde_json::to_string(&json_msg.msg).ok()?,
                    ),
                ));
            }
            "rebalance_groups" => {
                let json_msg: InfraGroupMsgJSON = serde_json::from_str(pload).ok()?;
                return Some(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::RebalanceGroups,
                ));
            }
            &_ => {
                debug!("Unknown type {msg_type} received");
            }
        }

        None
    }

    fn notify_devices_on_gid_change(self: Arc<Self>, mac_list: Vec<MacAddress>, new_gid: i32) {
        tokio::spawn(async move {
            // If we receive NB API add/del infra req,
            // and under storm of connections,
            // both conn ACK / as well as processing of this
            // request degrades heavily, due to single
            // sync.prim used in both - connmap lock;
            // TODO: investigate potential for resolving this
            // performance drop (lazy cache?)
            let connmap_r_lock = self.connmap.map.read().await;
            let msg: CGWConnectionProcessorReqMsg =
                CGWConnectionProcessorReqMsg::GroupIdChanged(new_gid);

            for mac in mac_list.iter() {
                match connmap_r_lock.get(mac) {
                    Some(c) => {
                        let _ = c.send(msg.clone());
                        debug!("Notified {mac} about GID change (->{new_gid})");
                    }
                    None => {
                        warn!("Wanted to notify {mac} about GID change (->{new_gid}), but device doesn't exist in map (Not connected still?)");
                    }
                }
            }
        });
    }

    async fn process_internal_nb_api_mbox(
        self: Arc<Self>,
        mut rx_mbox: CGWConnectionServerNBAPIMboxRx,
    ) {
        debug!("process_nb_api_mbox entry");

        let buf_capacity = 2000;
        let mut buf: Vec<CGWConnectionNBAPIReqMsg> = Vec::with_capacity(buf_capacity);
        let mut num_of_msg_read = 0;
        // As of now, expect at max 100 CGWS remote instances without buffers realloc
        // This only means that original capacity of all buffers is allocated to <100>,
        // it can still increase on demand or need automatically (upon insert, push_back etc)
        let cgw_buf_prealloc_size = 100;

        loop {
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
                    _v = sleep(Duration::from_millis(10)) => {
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

            debug!("Received {num_of_msg_read} messages from NB API, processing...");

            // We rely on this map only for a single iteration of received messages:
            // say, we receive 10 messages but 20 in queue, this means that gid->cgw_id
            // cache is clear at first, the filled up when processing first 10 messages,
            // the clear/reassigned again for next 10 msgs (10->20).
            // This is done to ensure that we don't fallback for redis too much,
            // but still somewhat fully rely on it.
            //
            let _ = self.cgw_remote_discovery.sync_gid_to_cgw_map().await;

            // TODO: rework to avoid re-allocating these buffers on each loop iteration
            // (get mut slice of vec / clear when done?)
            let mut relayed_cgw_msg_buf: Vec<(i32, CGWConnectionNBAPIReqMsg)> =
                Vec::with_capacity(num_of_msg_read + 1);
            let mut local_cgw_msg_buf: Vec<CGWConnectionNBAPIReqMsg> =
                Vec::with_capacity(num_of_msg_read + 1);

            while !buf.is_empty() {
                let msg = buf.remove(0);

                let CGWConnectionNBAPIReqMsg::EnqueueNewMessageFromNBAPIListener(
                    key,
                    payload,
                    origin,
                ) = msg;

                let gid_numeric = match key.parse::<i32>() {
                    Err(e) => {
                        warn!("Invalid KEY received from KAFKA bus message, ignoring\n{e}");
                        continue;
                    }
                    Ok(v) => v,
                };

                let parsed_msg = match self.parse_nbapi_msg(&payload) {
                    Some(val) => val,
                    None => {
                        warn!("Failed to parse recv msg with key {key}, discarded");
                        continue;
                    }
                };

                // The one shard that received add/del is responsible for
                // handling it at place.
                // Any other msg is either relayed / handled locally later.
                // The reason for this is following: current shard is responsible
                // for assignment of GID to shard, thus it has to make
                // assignment as soon as possible to deduce relaying action in
                // the following message pool that is being handled.
                // Same for delete.
                if let CGWNBApiParsedMsg {
                    uuid,
                    gid,
                    msg_type: CGWNBApiParsedMsgType::InfrastructureGroupCreate,
                } = parsed_msg
                {
                    // DB stuff - create group for remote shards to be aware of change
                    let group = CGWDBInfrastructureGroup {
                        id: gid,
                        reserved_size: 1000i32,
                        actual_size: 0i32,
                    };
                    match self.cgw_remote_discovery.create_infra_group(&group).await {
                        Ok(_dst_cgw_id) => {
                            if let Ok(resp) = cgw_construct_infra_group_create_response(
                                gid,
                                String::default(),
                                uuid,
                                true,
                                None,
                            ) {
                                self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                            } else {
                                error!("Failed to construct infra_group_create message");
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Create group gid {gid}, uuid {uuid} request failed, reason: {:?}",
                                e
                            );

                            if let Ok(resp) = cgw_construct_infra_group_create_response(
                                gid,
                                String::default(),
                                uuid,
                                false,
                                Some(format!("Failed to create new group: {:?}", e)),
                            ) {
                                self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                            } else {
                                error!("Failed to construct infra_group_create message");
                            }

                            warn!("Create group gid {gid} received, but it already exists, uuid {uuid}");
                        }
                    }
                    // This type of msg is handled in place, not added to buf
                    // for later processing.
                    continue;
                } else if let CGWNBApiParsedMsg {
                    uuid,
                    gid,
                    msg_type: CGWNBApiParsedMsgType::InfrastructureGroupDelete,
                } = parsed_msg
                {
                    let lock = self.devices_cache.clone();
                    match self
                        .cgw_remote_discovery
                        .destroy_infra_group(gid, lock)
                        .await
                    {
                        Ok(()) => {
                            if let Ok(resp) =
                                cgw_construct_infra_group_delete_response(gid, uuid, true, None)
                            {
                                self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                            } else {
                                error!("Failed to construct infra_group_delete message");
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Destroy group gid {gid}, uuid {uuid} request failed, reason: {:?}",
                                e
                            );

                            if let Ok(resp) = cgw_construct_infra_group_delete_response(
                                gid,
                                uuid,
                                false,
                                Some(format!("Failed to delete group: {:?}", e)),
                            ) {
                                self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                            } else {
                                error!("Failed to construct infra_group_delete message");
                            }

                            warn!("Destroy group gid {gid} received, but it does not exist");
                        }
                    }
                    // This type of msg is handled in place, not added to buf
                    // for later processing.
                    continue;
                }

                // We received NB API msg, check origin:
                // If it's a relayed message, we must not relay it further
                // If msg originated from Kafka originally, it's safe to relay it (if needed)
                if let CGWConnectionNBAPIReqMsgOrigin::FromRemoteCGW = origin {
                    local_cgw_msg_buf.push(
                        CGWConnectionNBAPIReqMsg::EnqueueNewMessageFromNBAPIListener(
                            key,
                            payload,
                            CGWConnectionNBAPIReqMsgOrigin::FromRemoteCGW,
                        ),
                    );
                    continue;
                }

                match self
                    .cgw_remote_discovery
                    .get_infra_group_owner_id(gid_numeric)
                    .await
                {
                    Some(dst_cgw_id) => {
                        if dst_cgw_id == self.local_cgw_id {
                            local_cgw_msg_buf.push(
                                CGWConnectionNBAPIReqMsg::EnqueueNewMessageFromNBAPIListener(
                                    key, payload, origin,
                                ),
                            );
                        } else {
                            relayed_cgw_msg_buf.push((
                                dst_cgw_id,
                                CGWConnectionNBAPIReqMsg::EnqueueNewMessageFromNBAPIListener(
                                    key, payload, origin,
                                ),
                            ));
                        }
                    }
                    None => {
                        if let Ok(resp) = cgw_construct_device_enqueue_response(
                            Uuid::default(),
                            false,
                            Some(format!(
                                "Received message for unknown group {gid_numeric} - unassigned?"
                            )),
                        ) {
                            self.enqueue_mbox_message_from_cgw_to_nb_api(gid_numeric, resp);
                        } else {
                            error!("Failed to construct device_enqueue message");
                        }

                        warn!("Received msg for gid {gid_numeric}, while this group is unassigned to any of CGWs: rejecting");
                    }
                }
            }

            let discovery_clone = self.cgw_remote_discovery.clone();
            let self_clone = self.clone();

            // Future to Handle (relay) messages for remote CGW
            let relay_task_hdl = self.mbox_relay_msg_runtime_handle.spawn(async move {
                let mut remote_cgws_map: HashMap<String, (i32, Vec<(String, String)>)> =
                    HashMap::with_capacity(cgw_buf_prealloc_size);

                while !relayed_cgw_msg_buf.is_empty() {
                    let msg = relayed_cgw_msg_buf.remove(0);
                    let (
                        dst_cgw_id,
                        CGWConnectionNBAPIReqMsg::EnqueueNewMessageFromNBAPIListener(
                            key,
                            payload,
                            _origin,
                        ),
                    ) = msg;
                    debug!(
                        "Received MSG for remote CGW k:{}, local id {} relaying msg to remote...",
                        key, self_clone.local_cgw_id
                    );
                    if let Some(v) = remote_cgws_map.get_mut(&key) {
                        v.1.push((key, payload));
                    } else {
                        let mut tmp_vec: Vec<(String, String)> =
                            Vec::with_capacity(num_of_msg_read);
                        tmp_vec.push((key.clone(), payload));
                        remote_cgws_map.insert(key, (dst_cgw_id, tmp_vec));
                    }
                }

                for value in remote_cgws_map.into_values() {
                    let discovery_clone = discovery_clone.clone();
                    let cgw_id = value.0;
                    let msg_stream = value.1;
                    let self_clone = self_clone.clone();
                    tokio::spawn(async move {
                        if (discovery_clone
                            .relay_request_stream_to_remote_cgw(cgw_id, msg_stream)
                            .await)
                            .is_err()
                        {
                            if let Ok(resp) = cgw_construct_device_enqueue_response(
                                Uuid::default(),
                                false,
                                Some(format!("Failed to relay MSG stream to remote CGW{cgw_id}")),
                            ) {
                                self_clone.enqueue_mbox_message_from_cgw_to_nb_api(-1, resp);
                            } else {
                                error!("Failed to construct device_enqueue message");
                            }
                        }
                    });
                }
            });

            // Handle messages for local CGW
            // Parse all messages first, then process
            // TODO: try to parallelize at least parsing of msg:
            // iterate each msg, get index, spawn task that would
            // write indexed parsed msg into output parsed msg buf.
            while !local_cgw_msg_buf.is_empty() {
                let msg = local_cgw_msg_buf.remove(0);
                let CGWConnectionNBAPIReqMsg::EnqueueNewMessageFromNBAPIListener(
                    key,
                    payload,
                    _origin,
                ) = msg;

                let gid_numeric = match key.parse::<i32>() {
                    Err(e) => {
                        warn!("Invalid KEY received from KAFKA bus message, ignoring\n{e}");
                        continue;
                    }
                    Ok(v) => v,
                };

                debug!(
                    "Received message for local CGW k:{key}, local id {}",
                    self.local_cgw_id
                );

                if let Some(msg) = self.parse_nbapi_msg(&payload) {
                    match msg {
                        CGWNBApiParsedMsg {
                            uuid,
                            gid,
                            msg_type: CGWNBApiParsedMsgType::InfrastructureGroupInfraAdd(mac_list),
                        } => {
                            if (self
                                .cgw_remote_discovery
                                .get_infra_group_owner_id(gid_numeric)
                                .await)
                                .is_none()
                            {
                                if let Ok(resp) = cgw_construct_infra_group_device_add_response(
                                    gid,
                                    mac_list.clone(),
                                    uuid,
                                    false,
                                    Some(format!("Failed to add infra list to nonexisting group, gid {gid}, uuid {uuid}")),
                                ) {
                                    self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                } else {
                                    error!("Failed to construct infra_group_device_add message");
                                }

                                warn!("Unexpected: tried to add infra list to nonexisting group, gid {gid}, uuid {uuid}");
                            }

                            let devices_cache_lock = self.devices_cache.clone();
                            match self
                                .cgw_remote_discovery
                                .create_ifras_list(gid, mac_list.clone(), devices_cache_lock)
                                .await
                            {
                                Ok(()) => {
                                    // All mac's GIDs been successfully changed;
                                    // Notify all of them about the change.
                                    self.clone()
                                        .notify_devices_on_gid_change(mac_list.clone(), gid);

                                    if let Ok(resp) = cgw_construct_infra_group_device_add_response(
                                        gid, mac_list, uuid, true, None,
                                    ) {
                                        self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                    } else {
                                        error!(
                                            "Failed to construct infra_group_device_add message"
                                        );
                                    }
                                }
                                Err(macs) => {
                                    if let Error::RemoteDiscoveryFailedInfras(mac_addresses) = macs
                                    {
                                        // We have a full list of macs we've tried to <add> to GID;
                                        // Remove elements from cloned list based on whethere
                                        // they're present in <failed list>
                                        // Whenever done - notify corresponding conn.processors,
                                        // that they should updated their state to have GID
                                        // change reflected;
                                        let mut macs_to_notify = mac_list.clone();
                                        macs_to_notify.retain(|&m| !mac_addresses.contains(&m));

                                        // Do so, only if there are at least any <successfull>
                                        // GID changes;
                                        if !macs_to_notify.is_empty() {
                                            self.clone()
                                                .notify_devices_on_gid_change(macs_to_notify, gid);
                                        }

                                        if let Ok(resp) = cgw_construct_infra_group_device_add_response(
                                            gid,
                                            mac_addresses,
                                            uuid,
                                            false,
                                            Some(format!("Failed to create few MACs from infras list (partial create), gid {gid}, uuid {uuid}")),
                                        ) {
                                            self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                        } else {
                                            error!(
                                                "Failed to construct infra_group_device_add message"
                                            );
                                        }

                                        warn!("Failed to create few MACs from infras list (partial create)");
                                        continue;
                                    }
                                }
                            }
                        }
                        CGWNBApiParsedMsg {
                            uuid,
                            gid,
                            msg_type: CGWNBApiParsedMsgType::InfrastructureGroupInfraDel(mac_list),
                        } => {
                            if (self
                                .cgw_remote_discovery
                                .get_infra_group_owner_id(gid_numeric)
                                .await)
                                .is_none()
                            {
                                if let Ok(resp) = cgw_construct_infra_group_device_del_response(
                                    gid,
                                    mac_list.clone(),
                                    uuid,
                                    false,
                                    Some(format!("Failed to delete MACs from infra list, gid {gid}, uuid {uuid}: group does not exist.")),
                                ) {
                                    self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                } else {
                                    error!(
                                        "Failed to construct infra_group_device_del message"
                                    );
                                }

                                warn!("Unexpected: tried to delete infra list from nonexisting group (gid {gid}, uuid {uuid}");
                            }

                            let lock = self.devices_cache.clone();
                            match self
                                .cgw_remote_discovery
                                .destroy_ifras_list(gid, mac_list.clone(), lock)
                                .await
                            {
                                Ok(()) => {
                                    // All mac's GIDs been successfully changed;
                                    // Notify all of them about the change.
                                    //
                                    // Group del == unassigned (0)
                                    self.clone()
                                        .notify_devices_on_gid_change(mac_list.clone(), 0i32);

                                    if let Ok(resp) = cgw_construct_infra_group_device_del_response(
                                        gid, mac_list, uuid, true, None,
                                    ) {
                                        self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                    } else {
                                        error!(
                                            "Failed to construct infra_group_device_del message"
                                        );
                                    }
                                }
                                Err(macs) => {
                                    if let Error::RemoteDiscoveryFailedInfras(mac_addresses) = macs
                                    {
                                        // We have a full list of macs we've tried to <del> from GID;
                                        // Remove elements from cloned list based on whethere
                                        // they're present in <failed list>
                                        // Whenever done - notify corresponding conn.processors,
                                        // that they should updated their state to have GID
                                        // change reflected;
                                        let mut macs_to_notify = mac_list.clone();
                                        macs_to_notify.retain(|&m| !mac_addresses.contains(&m));

                                        // Do so, only if there are at least any <successfull>
                                        // GID changes;
                                        if !macs_to_notify.is_empty() {
                                            self.clone()
                                                .notify_devices_on_gid_change(macs_to_notify, 0i32);
                                        }

                                        if let Ok(resp) = cgw_construct_infra_group_device_del_response(
                                            gid,
                                            mac_addresses,
                                            uuid,
                                            false,
                                            Some(format!("Failed to destroy few MACs from infras list (partial delete), gid {gid}, uuid {uuid}")),
                                        ) {
                                            self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                        } else {
                                            error!(
                                                "Failed to construct infra_group_device_del message"
                                            );
                                        }

                                        warn!("Failed to destroy few MACs from infras list (partial delete)");
                                        continue;
                                    }
                                }
                            }
                        }
                        CGWNBApiParsedMsg {
                            uuid,
                            gid,
                            msg_type:
                                CGWNBApiParsedMsgType::InfrastructureGroupInfraMsg(device_mac, msg),
                        } => {
                            if (self
                                .cgw_remote_discovery
                                .get_infra_group_owner_id(gid_numeric)
                                .await)
                                .is_none()
                            {
                                if let Ok(resp) = cgw_construct_device_enqueue_response(
                                    uuid,
                                    false,
                                    Some(format!("Failed to sink down msg to device of nonexisting group, gid {gid}, uuid {uuid}: group does not exist.")),
                                ) {
                                    self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                } else {
                                    error!("Failed to construct device_enqueue message");
                                }

                                warn!("Unexpected: tried to sink down msg to device of nonexisting group (gid {gid}, uuid {uuid}");
                            }

                            // 1. Parse message from NB
                            if let Ok(parsed_cmd) = cgw_ucentral_parse_command_message(&msg.clone())
                            {
                                let queue_msg: CGWUCentralMessagesQueueItem =
                                    CGWUCentralMessagesQueueItem::new(parsed_cmd, msg);

                                // 2. Add message to queue
                                {
                                    let queue_lock = CGW_MESSAGES_QUEUE.read().await;
                                    let _ =
                                        queue_lock.push_device_message(device_mac, queue_msg).await;
                                }
                            } else {
                                error!("Failed to parse UCentral command");
                            }
                        }
                        CGWNBApiParsedMsg {
                            uuid,
                            gid,
                            msg_type: CGWNBApiParsedMsgType::RebalanceGroups,
                        } => {
                            debug!(
                                "Received Rebalance Groups request, gid {}, uuid {}",
                                uuid, gid
                            );
                            match self.cgw_remote_discovery.rebalance_all_groups().await {
                                Ok(groups_res) => {
                                    if let Ok(resp) = cgw_construct_rebalance_group_response(
                                        gid, uuid, true, None,
                                    ) {
                                        self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                    } else {
                                        error!("Failed to construct rebalance_group message");
                                    }

                                    debug!("Rebalancing groups completed successfully, # of rebalanced groups {groups_res}");
                                }
                                Err(e) => {
                                    warn!(
                                        "Rebalance groups uuid {uuid} request failed, reason: {:?}",
                                        e
                                    );

                                    if let Ok(resp) = cgw_construct_rebalance_group_response(
                                        gid,
                                        uuid,
                                        false,
                                        Some(format!("Failed to rebalance groups: {:?}", e)),
                                    ) {
                                        self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                    } else {
                                        error!("Failed to construct rebalance_group message");
                                    }

                                    warn!("Rebalancing groups failed, error {:?}", e);
                                }
                            }
                        }
                        _ => {
                            debug!(
                                "Received unimplemented/unexpected group create/del msg, ignoring"
                            );
                        }
                    }
                } else {
                    error!("Failed to parse msg from NBAPI (malformed?)");
                    continue;
                }
            }

            // Do not proceed parsing local / remote msgs untill previous relaying has been
            // finished
            _ = tokio::join!(relay_task_hdl);

            buf.clear();
            num_of_msg_read = 0;
        }
    }

    async fn process_internal_mbox(self: Arc<Self>, mut rx_mbox: CGWConnectionServerMboxRx) {
        debug!("process_internal_mbox entry");

        let buf_capacity = 1000;
        let mut buf: Vec<CGWConnectionServerReqMsg> = Vec::with_capacity(buf_capacity);
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

                if let CGWConnectionServerReqMsg::AddNewConnection(
                    device_mac,
                    caps,
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
                    if let Some(c) = connmap_w_lock.remove(&device_mac) {
                        tokio::spawn(async move {
                            warn!("Duplicate connection (mac:{}) detected, closing OLD connection in favor of NEW", device_mac);
                            let msg: CGWConnectionProcessorReqMsg =
                                CGWConnectionProcessorReqMsg::AddNewConnectionShouldClose;
                            let _ = c.send(msg);
                        });
                    } else {
                        CGWMetrics::get_ref().change_counter(
                            CGWMetricsCounterType::ConnectionsNum,
                            CGWMetricsCounterOpType::Inc,
                        );
                    }

                    // clone a sender handle, as we still have to send ACK back using underlying
                    // tx mbox handle
                    let conn_processor_mbox_tx_clone = conn_processor_mbox_tx.clone();

                    info!(
                        "connmap: connection with {} established, new num_of_connections:{}",
                        device_mac,
                        connmap_w_lock.len() + 1
                    );

                    // Received new connection - check if infra exist in cache
                    // If exists - it already should have assigned group
                    // If not - simply add to cache - set gid == 0, devices should't remain in DB
                    let mut devices_cache = self.devices_cache.write().await;
                    let mut device_group_id: i32 = 0;
                    if let Some(device) = devices_cache.get_device(&device_mac) {
                        device_group_id = device.get_device_group_id();
                        device.set_device_state(CGWDeviceState::CGWDeviceConnected);

                        let group_id = device.get_device_group_id();
                        if let Some(group_owner_id) = self
                            .cgw_remote_discovery
                            .get_infra_group_owner_id(group_id)
                            .await
                        {
                            if group_owner_id != self.local_cgw_id {
                                if let Ok(resp) = cgw_construct_foreign_infra_connection_msg(
                                    group_id,
                                    device_mac,
                                    self.local_cgw_id,
                                    group_owner_id,
                                ) {
                                    self.enqueue_mbox_message_from_cgw_to_nb_api(group_id, resp);
                                } else {
                                    error!("Failed to construct foreign_infra_connection message");
                                }

                                debug!("Detected foreign infra {} connection. Group: {}, Group Shard Owner: {}", device_mac.to_hex_string(), group_id, group_owner_id);
                            }
                        } else {
                            if let Ok(resp) = cgw_construct_unassigned_infra_connection_msg(
                                device_mac,
                                self.local_cgw_id,
                            ) {
                                self.enqueue_mbox_message_from_cgw_to_nb_api(group_id, resp);
                            } else {
                                error!("Failed to construct unassigned_infra_connection message");
                            }

                            debug!(
                                "Detected unassigned infra {} connection.",
                                device_mac.to_hex_string()
                            );
                        }

                        let changes =
                            cgw_detect_device_chages(&device.get_device_capabilities(), &caps);
                        match changes {
                            Some(diff) => {
                                if let Ok(resp) = cgw_construct_device_capabilities_changed_msg(
                                    device_mac,
                                    device.get_device_group_id(),
                                    &diff,
                                ) {
                                    self.enqueue_mbox_message_from_cgw_to_nb_api(
                                        device.get_device_group_id(),
                                        resp,
                                    );
                                } else {
                                    error!(
                                        "Failed to construct device_capabilities_changed message"
                                    );
                                }
                            }
                            None => {
                                debug!(
                                    "Capabilities for device: {} was not changed!",
                                    device_mac.to_hex_string()
                                )
                            }
                        }
                        device.update_device_capabilities(&caps);
                    } else {
                        let default_caps: CGWDeviceCapabilities = Default::default();
                        let changes = cgw_detect_device_chages(&default_caps, &caps);
                        match changes {
                            Some(diff) => {
                                if let Ok(resp) = cgw_construct_device_capabilities_changed_msg(
                                    device_mac, 0, &diff,
                                ) {
                                    self.enqueue_mbox_message_from_cgw_to_nb_api(0, resp);
                                } else {
                                    error!(
                                        "Failed to construct device_capabilities_changed message"
                                    );
                                }
                            }
                            None => {
                                debug!(
                                    "Capabilities for device: {} was not changed!",
                                    device_mac.to_hex_string()
                                )
                            }
                        }

                        devices_cache.add_device(
                            &device_mac,
                            &CGWDevice::new(CGWDeviceState::CGWDeviceConnected, 0, false, caps),
                        );

                        if let Ok(resp) = cgw_construct_unassigned_infra_connection_msg(
                            device_mac,
                            self.local_cgw_id,
                        ) {
                            self.enqueue_mbox_message_from_cgw_to_nb_api(0, resp);
                        } else {
                            error!("Failed to construct unassigned_infra_connection message");
                        }

                        debug!(
                            "Detected unassigned infra {} connection.",
                            device_mac.to_hex_string()
                        );
                    }

                    if self.feature_topomap_enabled {
                        let topo_map = CGWUCentralTopologyMap::get_ref();
                        topo_map.insert_device(&device_mac).await;
                    }

                    connmap_w_lock.insert(device_mac, conn_processor_mbox_tx);

                    tokio::spawn(async move {
                        let msg: CGWConnectionProcessorReqMsg =
                            CGWConnectionProcessorReqMsg::AddNewConnectionAck(device_group_id);
                        let _ = conn_processor_mbox_tx_clone.send(msg);
                    });
                } else if let CGWConnectionServerReqMsg::ConnectionClosed(device_mac) = msg {
                    // Insert device to disconnected device list
                    {
                        let queue_lock = CGW_MESSAGES_QUEUE.read().await;
                        queue_lock.device_disconnected(&device_mac).await;
                    }
                    info!(
                        "connmap: removed {} serial from connmap, new num_of_connections:{}",
                        device_mac,
                        connmap_w_lock.len() - 1
                    );
                    connmap_w_lock.remove(&device_mac);

                    let mut devices_cache = self.devices_cache.write().await;
                    if let Some(device) = devices_cache.get_device(&device_mac) {
                        if device.get_device_remains_in_db() {
                            device.set_device_state(CGWDeviceState::CGWDeviceDisconnected);
                        } else {
                            devices_cache.del_device(&device_mac);
                        }
                    }

                    if self.feature_topomap_enabled {
                        let topo_map = CGWUCentralTopologyMap::get_ref();
                        topo_map.remove_device(&device_mac).await;
                    }

                    CGWMetrics::get_ref().change_counter(
                        CGWMetricsCounterType::ConnectionsNum,
                        CGWMetricsCounterOpType::Dec,
                    );
                }
            }

            buf.clear();
            num_of_msg_read = 0;
        }
    }

    pub async fn ack_connection(
        self: Arc<Self>,
        socket: TcpStream,
        tls_acceptor: tokio_rustls::TlsAcceptor,
        addr: SocketAddr,
        conn_idx: i64,
    ) {
        // Only ACK connection. We will either drop it or accept it once processor starts
        // (we'll handle it via "mailbox" notify handle in process_internal_mbox)
        let server_clone = self.clone();

        self.wss_rx_tx_runtime.spawn(async move {
            // Accept the TLS connection.
            let (client_cn, tls_stream) = match tls_acceptor.accept(socket).await {
                Ok(stream) => match cgw_tls_get_cn_from_stream(&stream).await {
                    Ok(cn) => (cn, stream),
                    Err(e) => {
                        error!("Failed to read client CN. Error: {}", e.to_string());
                        return;
                    }
                },
                Err(e) => {
                    error!("Failed to accept connection: Error {}", e);
                    return;
                }
            };

            let allow_mismatch = server_clone.allow_mismatch;
            let conn_processor = CGWConnectionProcessor::new(server_clone, conn_idx, addr);
            let _ = conn_processor
                .start(tls_stream, client_cn, allow_mismatch)
                .await;
        });
    }

    pub async fn cleanup_redis(&self) {
        self.cgw_remote_discovery.cleanup_redis().await;
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        cgw_errors::Result,
        cgw_ucentral_ap_parser::cgw_ucentral_ap_parse_message,
        cgw_ucentral_parser::{CGWUCentralEvent, CGWUCentralEventType},
    };

    fn get_connect_json_msg() -> &'static str {
        r#"
        {
            "jsonrpc": "2.0",
            "method": "connect",
            "params": {
                "serial": "00000000ca4b",
                "firmware": "SONiC-OS-4.1.0_vs_daily_221213_1931_422-campus",
                "uuid": 1,
                "capabilities": {
                "compatible": "+++x86_64-kvm_x86_64-r0",
                "model": "DellEMC-S5248f-P-25G-DPB",
                "platform": "switch",
                "label_macaddr": "00:00:00:00:ca:4b"
                }
            }
        }"#
    }

    fn get_log_json_msg() -> &'static str {
        r#"
        {
            "jsonrpc": "2.0",
            "method": "log",
            "params": {
                "serial": "00000000ca4b",
                "log": "uc-client: connection error: Unable to connect",
                "severity": 3
            }
        }"#
    }

    #[test]
    fn can_parse_connect_event() -> Result<()> {
        let msg = get_connect_json_msg();
        let event: CGWUCentralEvent = cgw_ucentral_ap_parse_message(msg, 0)?;

        match event.evt_type {
            CGWUCentralEventType::Connect(_) => {
                debug!("Assertion passed")
            }
            _ => {
                error!("Expected event to be of <Connect> type");
            }
        }

        Ok(())
    }

    #[test]

    fn can_parse_log_event() -> Result<()> {
        let msg = get_log_json_msg();
        let event: CGWUCentralEvent = cgw_ucentral_ap_parse_message(msg, 0)?;

        match event.evt_type {
            CGWUCentralEventType::Log(_) => {
                debug!("Assertion passed")
            }
            _ => {
                error!("Expected event to be of <Log> type");
            }
        }

        Ok(())
    }
}
