use crate::cgw_device::{
    cgw_detect_device_chages, CGWDevice, CGWDeviceCapabilities, CGWDeviceState, CGWDeviceType,
};
use crate::cgw_nb_api_listener::{
    cgw_construct_foreign_infra_connection_msg, cgw_construct_infra_capabilities_changed_msg,
    cgw_construct_infra_enqueue_response, cgw_construct_infra_group_create_response,
    cgw_construct_infra_group_delete_response, cgw_construct_infra_group_infras_add_response,
    cgw_construct_infra_group_infras_del_response, cgw_construct_infra_join_msg,
    cgw_construct_infra_leave_msg, cgw_construct_infra_request_result_msg,
    cgw_construct_rebalance_group_response, cgw_construct_unassigned_infra_connection_msg,
};
use crate::cgw_runtime::{cgw_get_runtime, CGWRuntimeType};
use crate::cgw_tls::cgw_tls_get_cn_from_stream;
use crate::cgw_ucentral_messages_queue_manager::{
    CGWUCentralMessagesQueueItem, CGWUCentralMessagesQueueState, CGW_MESSAGES_QUEUE,
    TIMEOUT_MANAGER_DURATION,
};
use crate::cgw_ucentral_parser::{
    cgw_ucentral_parse_command_message, CGWUCentralCommand, CGWUCentralCommandType,
    CGWUCentralConfigValidators,
};
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

use std::str::FromStr;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::{
    net::TcpStream,
    runtime::Runtime,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        RwLock,
    },
    time::{sleep, Duration},
};

use tokio::time;

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
        SocketAddr,
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

    // UCentral command messages validators
    // for access points and switches
    config_validator: CGWUCentralConfigValidators,

    // User-supplied arguments can disable state/realtime events
    // processing by underlying connections processors.
    pub feature_topomap_enabled: bool,
}

enum CGWNBApiParsedMsgType {
    InfrastructureGroupCreate,
    InfrastructureGroupCreateToShard(i32),
    InfrastructureGroupDelete,
    InfrastructureGroupInfrasAdd(Vec<MacAddress>),
    InfrastructureGroupInfrasDel(Vec<MacAddress>),
    InfrastructureGroupInfraMsg(MacAddress, String, Option<u64>),
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
        let wss_runtime_handle = match cgw_get_runtime(CGWRuntimeType::WssRxTx) {
            Ok(ret_runtime) => match ret_runtime {
                Some(runtime) => runtime,
                None => {
                    return Err(Error::ConnectionServer(format!(
                        "Failed to find runtime type {:?}",
                        CGWRuntimeType::WssRxTx
                    )));
                }
            },
            Err(e) => {
                return Err(Error::ConnectionServer(format!(
                    "Failed to get runtime type {:?}! Error: {e}",
                    CGWRuntimeType::WssRxTx
                )));
            }
        };

        let internal_mbox_runtime_handle = match cgw_get_runtime(CGWRuntimeType::MboxInternal) {
            Ok(ret_runtime) => match ret_runtime {
                Some(runtime) => runtime,
                None => {
                    return Err(Error::ConnectionServer(format!(
                        "Failed to find runtime type {:?}",
                        CGWRuntimeType::WssRxTx
                    )));
                }
            },
            Err(e) => {
                return Err(Error::ConnectionServer(format!(
                    "Failed to get runtime type {:?}! Error: {e}",
                    CGWRuntimeType::WssRxTx
                )));
            }
        };

        let nb_api_mbox_runtime_handle = match cgw_get_runtime(CGWRuntimeType::MboxNbApiRx) {
            Ok(ret_runtime) => match ret_runtime {
                Some(runtime) => runtime,
                None => {
                    return Err(Error::ConnectionServer(format!(
                        "Failed to find runtime type {:?}",
                        CGWRuntimeType::WssRxTx
                    )));
                }
            },
            Err(e) => {
                return Err(Error::ConnectionServer(format!(
                    "Failed to get runtime type {:?}! Error: {e}",
                    CGWRuntimeType::WssRxTx
                )));
            }
        };

        let nb_api_mbox_tx_runtime_handle = match cgw_get_runtime(CGWRuntimeType::MboxNbApiTx) {
            Ok(ret_runtime) => match ret_runtime {
                Some(runtime) => runtime,
                None => {
                    return Err(Error::ConnectionServer(format!(
                        "Failed to find runtime type {:?}",
                        CGWRuntimeType::WssRxTx
                    )));
                }
            },
            Err(e) => {
                return Err(Error::ConnectionServer(format!(
                    "Failed to get runtime type {:?}! Error: {e}",
                    CGWRuntimeType::WssRxTx
                )));
            }
        };

        let relay_msg_mbox_runtime_handle = match cgw_get_runtime(CGWRuntimeType::MboxRelay) {
            Ok(ret_runtime) => match ret_runtime {
                Some(runtime) => runtime,
                None => {
                    return Err(Error::ConnectionServer(format!(
                        "Failed to find runtime type {:?}",
                        CGWRuntimeType::WssRxTx
                    )));
                }
            },
            Err(e) => {
                return Err(Error::ConnectionServer(format!(
                    "Failed to get runtime type {:?}! Error: {e}",
                    CGWRuntimeType::WssRxTx
                )));
            }
        };

        let queue_timeout_handle = match cgw_get_runtime(CGWRuntimeType::QueueTimeout) {
            Ok(ret_runtime) => match ret_runtime {
                Some(runtime) => runtime,
                None => {
                    return Err(Error::ConnectionServer(format!(
                        "Failed to find runtime type {:?}",
                        CGWRuntimeType::WssRxTx
                    )));
                }
            },
            Err(e) => {
                return Err(Error::ConnectionServer(format!(
                    "Failed to get runtime type {:?}! Error: {e}",
                    CGWRuntimeType::WssRxTx
                )));
            }
        };

        let (internal_tx, internal_rx) = unbounded_channel::<CGWConnectionServerReqMsg>();
        let (nb_api_tx, nb_api_rx) = unbounded_channel::<CGWConnectionNBAPIReqMsg>();

        // Give NB API client a handle where it can do a TX (CLIENT -> CGW_SERVER)
        // RX is handled in internal_mbox of CGW_Server
        let nb_api_c = match CGWNBApiClient::new(app_args.cgw_id, &app_args.kafka_args, &nb_api_tx)
        {
            Ok(c) => c,
            Err(e) => {
                error!(
                    "Can't create CGW Connection server! NB API client create failed! Error: {e}"
                );
                return Err(Error::ConnectionServer(format!(
                    "Can't create CGW Connection server! NB API client create failed! Error: {e}"
                )));
            }
        };

        let cgw_remote_discovery = match CGWRemoteDiscovery::new(app_args).await {
            Ok(d) => d,
            Err(e) => {
                error!(
                    "Can't create CGW Connection server! Remote Discovery create failed! Error: {e}"
                );
                return Err(Error::ConnectionServer(format!(
                    "Can't create CGW Connection server! Remote Discovery create failed! Error: {e}"
                )));
            }
        };

        // TODO: proper fix.
        // Ugly W/A for now;
        // The reason behind this change (W/A), is that underlying validator
        // uses sync call, which panics (due to it being called in async
        // context).
        // The proper fix would to be refactor all constructors to be sync,
        // but use spawn_blocking where needed in contextes that rely on the
        // underlying async calls.
        let app_args_clone = app_args.validation_schema.clone();
        let get_config_validator_fut = tokio::task::spawn_blocking(move || {
            CGWUCentralConfigValidators::new(app_args_clone).unwrap()
        });
        let config_validator = match get_config_validator_fut.await {
            Ok(res) => res,
            Err(e) => {
                error!("Failed to retrieve json config validators! Error: {e}");
                return Err(Error::ConnectionServer(format!(
                    "Failed to retrieve json config validators! Error: {e}"
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
            config_validator,
        });

        let server_clone = server.clone();
        // Task for processing mbox_internal_rx, task owns the RX part
        server.mbox_internal_runtime_handle.spawn(async move {
            server_clone.process_internal_mbox(internal_rx).await;
        });

        let server_clone = server.clone();
        let ifras_capacity = app_args.cgw_group_infras_capacity;
        CGWMetrics::get_ref().change_counter(
            CGWMetricsCounterType::GroupInfrasCapacity,
            CGWMetricsCounterOpType::Set(ifras_capacity.into()),
        );
        server.mbox_nb_api_runtime_handle.spawn(async move {
            server_clone
                .process_internal_nb_api_mbox(nb_api_rx, ifras_capacity)
                .await;
        });

        let server_clone = server.clone();
        server.queue_timeout_handle.spawn(async move {
            server_clone.start_queue_timeout_manager().await;
        });

        if let Err(e) = server.cgw_remote_discovery.sync_devices_cache().await {
            error!("Failed to sync infras with Redis devices cache! Error: {e}");
        }

        // Sync RAM cache with Redis.
        if let Err(e) = server
            .cgw_remote_discovery
            .sync_devices_cache_with_redis(server.devices_cache.clone())
            .await
        {
            error!("Failed to sync Device cache! Error: {e}");
        }

        tokio::spawn(async move {
            CGWMetrics::get_ref()
                .change_component_health_status(
                    CGWMetricsHealthComponent::ConnectionServer,
                    CGWMetricsHealthComponentStatus::Ready,
                )
                .await;
        });

        if server.feature_topomap_enabled {
            info!("Topomap enabled, starting queue processor...");
            let topo_map = CGWUCentralTopologyMap::get_ref();
            topo_map.start(&server.wss_rx_tx_runtime).await;
        }

        Ok(server)
    }

    pub async fn enqueue_mbox_message_to_cgw_server(&self, req: CGWConnectionServerReqMsg) {
        if let Err(e) = self.mbox_internal_tx.send(req) {
            error!("Failed to send message to CGW server (internal)! Error: {e}");
        }
    }

    pub fn enqueue_mbox_message_from_device_to_nb_api_c(
        &self,
        group_id: i32,
        req: String,
    ) -> Result<()> {
        let nb_api_client_clone = self.nb_api_client.clone();
        tokio::spawn(async move {
            let key = group_id.to_string();
            nb_api_client_clone
                .enqueue_mbox_message_from_cgw_server(key, req)
                .await;
        });

        Ok(())
    }

    pub fn enqueue_mbox_message_from_cgw_to_nb_api(&self, gid: i32, req: String) {
        let nb_api_client_clone = self.nb_api_client.clone();
        self.mbox_nb_api_tx_runtime_handle.spawn(async move {
            nb_api_client_clone
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

        if let Err(e) = self.mbox_relayed_messages_handle.send(msg) {
            error!("Failed to handle relayed message to CGW server! Error: {e}");
        }
    }

    fn parse_nbapi_msg(&self, pload: &str) -> Option<CGWNBApiParsedMsg> {
        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupCreate {
            r#type: String,
            infra_group_id: String,
            uuid: Uuid,
        }
        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupCreateToShard {
            r#type: String,
            infra_group_id: String,
            shard_id: i32,
            uuid: Uuid,
        }
        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupDelete {
            r#type: String,
            infra_group_id: String,
            uuid: Uuid,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupInfrasAdd {
            r#type: String,
            infra_group_id: String,
            infra_group_infras: Vec<MacAddress>,
            uuid: Uuid,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupInfrasDel {
            r#type: String,
            infra_group_id: String,
            infra_group_infras: Vec<MacAddress>,
            uuid: Uuid,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupMsgJSON {
            r#type: String,
            infra_group_id: String,
            infra_group_infra: MacAddress,
            msg: Map<String, Value>,
            uuid: Uuid,
            timeout: Option<u64>,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct RebalanceGroups {
            r#type: String,
            infra_group_id: String,
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
            "infrastructure_group_create_to_shard" => {
                let json_msg: InfraGroupCreateToShard = serde_json::from_str(pload).ok()?;
                return Some(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::InfrastructureGroupCreateToShard(
                        json_msg.shard_id,
                    ),
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
            "infrastructure_group_infras_add" => {
                let json_msg: InfraGroupInfrasAdd = serde_json::from_str(pload).ok()?;
                return Some(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::InfrastructureGroupInfrasAdd(
                        json_msg.infra_group_infras,
                    ),
                ));
            }
            "infrastructure_group_infras_del" => {
                let json_msg: InfraGroupInfrasDel = serde_json::from_str(pload).ok()?;
                return Some(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::InfrastructureGroupInfrasDel(
                        json_msg.infra_group_infras,
                    ),
                ));
            }
            "infrastructure_group_infra_message_enqueue" => {
                let json_msg: InfraGroupMsgJSON = serde_json::from_str(pload).ok()?;
                debug!("{:?}", json_msg);
                return Some(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::InfrastructureGroupInfraMsg(
                        json_msg.infra_group_infra,
                        serde_json::to_string(&json_msg.msg).ok()?,
                        json_msg.timeout,
                    ),
                ));
            }
            "rebalance_groups" => {
                let json_msg: RebalanceGroups = serde_json::from_str(pload).ok()?;
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

    fn notify_devices_on_gid_change(self: Arc<Self>, infras_list: Vec<MacAddress>, new_gid: i32) {
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

            for mac in infras_list.iter() {
                if let Some(c) = connmap_r_lock.get(mac) {
                    match c.send(msg.clone()) {
                        Ok(_) => debug!("Notified {mac} about GID change (->{new_gid})"),
                        Err(e) => warn!("Failed to send GID change notification! Error: {e}"),
                    }
                }
            }
        });
    }

    async fn get_redis_last_update_timestamp(&self) -> i64 {
        match self
            .cgw_remote_discovery
            .get_redis_last_update_timestamp()
            .await
        {
            Ok(timestamp) => timestamp,
            Err(e) => {
                error!("{e}");
                0i64
            }
        }
    }

    async fn process_internal_nb_api_mbox(
        self: Arc<Self>,
        mut rx_mbox: CGWConnectionServerNBAPIMboxRx,
        infras_capacity: i32,
    ) {
        debug!("process_nb_api_mbox entry");

        let buf_capacity = 2000;
        let mut buf: Vec<CGWConnectionNBAPIReqMsg> = Vec::with_capacity(buf_capacity);
        let mut num_of_msg_read = 0;
        // As of now, expect at max 100 CGWS remote instances without buffers realloc
        // This only means that original capacity of all buffers is allocated to <100>,
        // it can still increase on demand or need automatically (upon insert, push_back etc)
        let cgw_buf_prealloc_size = 100;
        let mut last_update_timestamp: i64 = self.get_redis_last_update_timestamp().await;

        let mut partition_array_idx: usize = 0;
        let mut local_shard_partition_key: Option<String>;

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

                if rd_num == 0 {
                    let current_timestamp: i64 = self.get_redis_last_update_timestamp().await;

                    if last_update_timestamp != current_timestamp {
                        if let Err(e) = self.cgw_remote_discovery.sync_gid_to_cgw_map().await {
                            error!("process_internal_nb_api_mbox: failed to sync GID to CGW map! Error: {e}");
                        }

                        if let Err(e) = self.cgw_remote_discovery.sync_devices_cache().await {
                            error!("process_internal_nb_api_mbox: failed to sync Devices cache! Error: {e}");
                        }

                        if let Err(e) = self
                            .cgw_remote_discovery
                            .sync_devices_cache_with_redis(self.devices_cache.clone())
                            .await
                        {
                            error!("Failed to sync Device cache! Error: {e}");
                        }

                        last_update_timestamp = current_timestamp;
                    }
                }

                // We read some messages, try to continue and read more
                // If none read - break from recv, process all buffers that've
                // been filled-up so far (both local and remote).
                // Upon done - repeat.
                if rd_num >= 1 || num_of_msg_read == 0 {
                    continue;
                }
            }

            debug!("Received {num_of_msg_read} messages from NB API, processing...");

            let partition_mapping = self.nb_api_client.get_partition_to_local_shard_mapping();
            debug!(
                "Kafka partitions idx:key mapping info: {:?}",
                partition_mapping
            );
            if !partition_mapping.is_empty() {
                partition_array_idx += 1;
                if partition_array_idx >= partition_mapping.len() {
                    partition_array_idx = 0;
                }
                local_shard_partition_key = Some(partition_mapping[partition_array_idx].1.clone());

                debug!(
                    "Using kafka key '{}' for kafka partition idx '{}'",
                    partition_mapping[partition_array_idx].1,
                    partition_mapping[partition_array_idx].0
                );
            } else {
                warn!("Cannot get partition to local shard mapping, won't be able to return kafka routing key in NB request replies!");
                // Clear previously used partition key
                local_shard_partition_key = None;
            }

            // We rely on this map only for a single iteration of received messages:
            // say, we receive 10 messages but 20 in queue, this means that gid->cgw_id
            // cache is clear at first, the filled up when processing first 10 messages,
            // the clear/reassigned again for next 10 msgs (10->20).
            // This is done to ensure that we don't fallback for redis too much,
            // but still somewhat fully rely on it.
            //

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

                let parsed_msg = match self.parse_nbapi_msg(&payload) {
                    Some(val) => val,
                    None => {
                        warn!("Failed to parse recv msg with key {key}, discarded!");

                        if let Ok(resp) = cgw_construct_infra_enqueue_response(
                            self.local_cgw_id,
                            Uuid::default(),
                            false,
                            Some(format!("Failed to parse NB API message with key {key}")),
                            local_shard_partition_key.clone(),
                        ) {
                            self.enqueue_mbox_message_from_cgw_to_nb_api(-1, resp);
                        } else {
                            error!("Failed to construct device_enqueue message!");
                        }

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
                        reserved_size: infras_capacity,
                        actual_size: 0i32,
                    };
                    match self
                        .cgw_remote_discovery
                        .create_infra_group(&group, None)
                        .await
                    {
                        Ok(_dst_cgw_id) => {
                            // We successfully updated both SQL and REDIS
                            // cache. In order to keep it in sync with local
                            // one, we have to make sure we <save> latest
                            // update timestamp locally, to prevent CGW
                            // from trying to update it in next iteration
                            // of the main loop, while this very own
                            // local shard _is_ responsible for timestamp
                            // update.
                            last_update_timestamp = self.get_redis_last_update_timestamp().await;

                            if let Ok(resp) = cgw_construct_infra_group_create_response(
                                gid,
                                self.local_cgw_id,
                                uuid,
                                true,
                                None,
                            ) {
                                self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                            } else {
                                error!("Failed to construct infra_group_create message!");
                            }
                        }
                        Err(e) => {
                            warn!("Create group gid {gid}, uuid {uuid} request failed! Error: {e}");

                            if let Ok(resp) = cgw_construct_infra_group_create_response(
                                gid,
                                self.local_cgw_id,
                                uuid,
                                false,
                                Some(format!("Failed to create new group! Error: {e}")),
                            ) {
                                self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                            } else {
                                error!("Failed to construct infra_group_create message!");
                            }
                        }
                    }
                    // This type of msg is handled in place, not added to buf
                    // for later processing.
                    continue;
                } else if let CGWNBApiParsedMsg {
                    uuid,
                    gid,
                    msg_type: CGWNBApiParsedMsgType::InfrastructureGroupCreateToShard(shard_id),
                } = parsed_msg
                {
                    // DB stuff - create group for remote shards to be aware of change
                    let group = CGWDBInfrastructureGroup {
                        id: gid,
                        reserved_size: infras_capacity,
                        actual_size: 0i32,
                    };
                    match self
                        .cgw_remote_discovery
                        .create_infra_group(&group, Some(shard_id))
                        .await
                    {
                        Ok(_dst_cgw_id) => {
                            // We successfully updated both SQL and REDIS
                            // cache. In order to keep it in sync with local
                            // one, we have to make sure we <save> latest
                            // update timestamp locally, to prevent CGW
                            // from trying to update it in next iteration
                            // of the main loop, while this very own
                            // local shard _is_ responsible for timestamp
                            // update.
                            last_update_timestamp = self.get_redis_last_update_timestamp().await;

                            if let Ok(resp) = cgw_construct_infra_group_create_response(
                                gid,
                                self.local_cgw_id,
                                uuid,
                                true,
                                None,
                            ) {
                                self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                            } else {
                                error!("Failed to construct infra_group_create message!");
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Create group gid {gid}, uuid {uuid} request failed, reason: {e}",
                            );

                            if let Ok(resp) = cgw_construct_infra_group_create_response(
                                gid,
                                self.local_cgw_id,
                                uuid,
                                false,
                                Some(format!(
                                    "Failed to create new group to shard id {shard_id}! Error {e}"
                                )),
                            ) {
                                self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                            } else {
                                error!("Failed to construct infra_group_create message!");
                            }
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
                            // We successfully updated both SQL and REDIS
                            // cache. In order to keep it in sync with local
                            // one, we have to make sure we <save> latest
                            // update timestamp locally, to prevent CGW
                            // from trying to update it in next iteration
                            // of the main loop, while this very own
                            // local shard _is_ responsible for timestamp
                            // update.
                            last_update_timestamp = self.get_redis_last_update_timestamp().await;

                            // We try to help free topomap memory usage
                            // by notifying it whenever GID get's destroyed.
                            // Howover, for allocation we let topo map
                            // handle it's mem alloc whenever necessary
                            // on it's own, when data from specific gid
                            // arrives - we rely on topo map to
                            // allocate necessary structures on it's own.
                            //
                            // In this way, we make sure that we handle
                            // properly the GID resoration scenario:
                            // if CGW restarts and loads GID info from
                            // DB, there would be no notification about
                            // create/del, and there's no need to
                            // oflload this responsibility to
                            // remote_discovery module for example,
                            // due to the fact that CGW is not designed
                            // for management of redis without CGW knowledge:
                            // if something disrupts the redis state / sql
                            // state without CGW's prior knowledge,
                            // it's not a responsibility of CGW to be aware
                            // of such changes and handle it correspondingly.
                            if self.feature_topomap_enabled {
                                let topo_map = CGWUCentralTopologyMap::get_ref();
                                topo_map.remove_gid(gid).await;
                            }

                            if let Ok(resp) = cgw_construct_infra_group_delete_response(
                                gid,
                                self.local_cgw_id,
                                uuid,
                                true,
                                None,
                            ) {
                                self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                            } else {
                                error!("Failed to construct infra_group_delete message!");
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Destroy group gid {gid}, uuid {uuid} request failed! Error: {e}"
                            );

                            if let Ok(resp) = cgw_construct_infra_group_delete_response(
                                gid,
                                self.local_cgw_id,
                                uuid,
                                false,
                                Some(format!("Failed to delete group! Error: {e}")),
                            ) {
                                self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                            } else {
                                error!("Failed to construct infra_group_delete message!");
                            }

                            warn!("Destroy group gid {gid} received, but it does not exist!");
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

                // We need to know which dst GID this request should be
                // forwarded to.
                // In order to get it, match to <any> parsed msg, and
                // get only gid field.
                let gid: i32 = match parsed_msg {
                    CGWNBApiParsedMsg { gid, .. } => gid,
                };

                match self
                    .cgw_remote_discovery
                    .get_infra_group_owner_id(gid)
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
                        // Failed to find destination GID shard ID owner
                        // It is more likely GID does not exist
                        // Add request to local message buffer - let CGW process request
                        // We relayig on uderlaying logic to handle specific request and manage corner cases
                        local_cgw_msg_buf.push(
                            CGWConnectionNBAPIReqMsg::EnqueueNewMessageFromNBAPIListener(
                                key, payload, origin,
                            ),
                        );
                    }
                }
            }

            let discovery_clone = self.cgw_remote_discovery.clone();
            let self_clone = self.clone();
            let local_shard_partition_key_clone = local_shard_partition_key.clone();

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
                        "Received MSG for remote CGW key: {}, local id {}, relaying msg to remote...",
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
                    let local_shard_partition_key_clone = local_shard_partition_key_clone.clone();
                    tokio::spawn(async move {
                        if (discovery_clone
                            .relay_request_stream_to_remote_cgw(cgw_id, msg_stream)
                            .await)
                            .is_err()
                        {
                            if let Ok(resp) = cgw_construct_infra_enqueue_response(
                                self_clone.local_cgw_id,
                                Uuid::default(),
                                false,
                                Some(format!("Failed to relay MSG stream to remote CGW{cgw_id}")),
                                local_shard_partition_key_clone,
                            ) {
                                self_clone.enqueue_mbox_message_from_cgw_to_nb_api(-1, resp);
                            } else {
                                error!("Failed to construct device_enqueue message!");
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

                debug!(
                    "Received message for local CGW: key '{key}', local shard id '{}'",
                    self.local_cgw_id
                );

                if let Some(msg) = self.parse_nbapi_msg(&payload) {
                    match msg {
                        CGWNBApiParsedMsg {
                            uuid,
                            gid,
                            msg_type:
                                CGWNBApiParsedMsgType::InfrastructureGroupInfrasAdd(infras_list),
                        } => {
                            if (self
                                .cgw_remote_discovery
                                .get_infra_group_owner_id(gid)
                                .await)
                                .is_none()
                            {
                                if let Ok(resp) = cgw_construct_infra_group_infras_add_response(
                                    gid,
                                    infras_list.clone(),
                                    self.local_cgw_id,
                                    uuid,
                                    false,
                                    Some(format!("Failed to add infra list to nonexisting group, gid {gid}, uuid {uuid}")),
                                    local_shard_partition_key.clone(),
                                ) {
                                    self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                } else {
                                    error!("Failed to construct infra_group_device_add message!");
                                }

                                warn!("Unexpected: tried to add infra list to nonexisting group, gid {gid}, uuid {uuid}!");
                                continue;
                            }

                            let devices_cache_lock = self.devices_cache.clone();
                            match self
                                .cgw_remote_discovery
                                .create_ifras_list(gid, infras_list.clone(), devices_cache_lock)
                                .await
                            {
                                Ok(success_ifras) => {
                                    // Not all mac's GIDs might been successfully changed;
                                    // Notify all of them about the change.
                                    self.clone()
                                        .notify_devices_on_gid_change(success_ifras.clone(), gid);

                                    if let Ok(resp) = cgw_construct_infra_group_infras_add_response(
                                        gid,
                                        // Empty vec: no infras assign <failed>
                                        Vec::new(),
                                        self.local_cgw_id,
                                        uuid,
                                        true,
                                        None,
                                        local_shard_partition_key.clone(),
                                    ) {
                                        self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                    } else {
                                        error!(
                                            "Failed to construct infra_group_device_add message!"
                                        );
                                    }

                                    let devices_cache_read = self.devices_cache.read().await;
                                    for mac in success_ifras {
                                        let queue_lock = CGW_MESSAGES_QUEUE.read().await;
                                        if queue_lock.check_messages_queue_exists(&mac).await {
                                            queue_lock
                                                .set_device_queue_state(
                                                    &mac,
                                                    CGWUCentralMessagesQueueState::RxTx,
                                                )
                                                .await;
                                        }

                                        if let Some(dev) = devices_cache_read.get_device(&mac) {
                                            if dev.get_device_state()
                                                == CGWDeviceState::CGWDeviceConnected
                                            {
                                                let dev_gid = dev.get_device_group_id();
                                                let changes = cgw_detect_device_chages(
                                                    &CGWDeviceCapabilities::default(),
                                                    &dev.get_device_capabilities(),
                                                );
                                                match changes {
                                                    Some(diff) => {
                                                        if let Ok(resp) =
                                                            cgw_construct_infra_capabilities_changed_msg(
                                                                mac,
                                                                dev_gid,
                                                                &diff,
                                                                self.local_cgw_id,
                                                            )
                                                        {
                                                            self.enqueue_mbox_message_from_cgw_to_nb_api(
                                                                dev_gid,
                                                                resp,
                                                            );
                                                        } else {
                                                            error!(
                                                        "Failed to construct device_capabilities_changed message!"
                                                    );
                                                        }
                                                    }
                                                    None => {
                                                        debug!(
                                                            "Capabilities for device: {} was not changed",
                                                            mac.to_hex_string()
                                                        )
                                                    }
                                                }
                                            } else {
                                                queue_lock
                                                    .device_disconnected(
                                                        &mac,
                                                        dev.get_device_group_id(),
                                                    )
                                                    .await;
                                            }
                                        }
                                    }
                                }
                                Err(macs) => {
                                    if let Error::RemoteDiscoveryFailedInfras(failed_infras) = macs
                                    {
                                        // We have a full list of macs we've tried to <add> to GID;
                                        // Remove elements from cloned list based on whethere
                                        // they're present in <failed list>
                                        // Whenever done - notify corresponding conn.processors,
                                        // that they should updated their state to have GID
                                        // change reflected;
                                        let mut macs_to_notify = infras_list.clone();
                                        macs_to_notify.retain(|&m| !failed_infras.contains(&m));

                                        // Do so, only if there are at least any <successfull>
                                        // GID changes;
                                        if !macs_to_notify.is_empty() {
                                            self.clone()
                                                .notify_devices_on_gid_change(macs_to_notify, gid);
                                        }

                                        if let Ok(resp) = cgw_construct_infra_group_infras_add_response(
                                            gid,
                                            failed_infras,
                                            self.local_cgw_id,
                                            uuid,
                                            false,
                                            Some(format!("Failed to create few MACs from infras list (partial create), gid {gid}, uuid {uuid}")),
                                            local_shard_partition_key.clone(),
                                        ) {
                                            self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                        } else {
                                            error!(
                                                "Failed to construct infra_group_device_add message!"
                                            );
                                        }

                                        warn!("Failed to create few MACs from infras list (partial create)!");
                                        continue;
                                    }
                                }
                            }
                        }
                        CGWNBApiParsedMsg {
                            uuid,
                            gid,
                            msg_type:
                                CGWNBApiParsedMsgType::InfrastructureGroupInfrasDel(infras_list),
                        } => {
                            if (self
                                .cgw_remote_discovery
                                .get_infra_group_owner_id(gid)
                                .await)
                                .is_none()
                            {
                                if let Ok(resp) = cgw_construct_infra_group_infras_del_response(
                                    gid,
                                    infras_list.clone(),
                                    self.local_cgw_id,
                                    uuid,
                                    false,
                                    Some(format!("Failed to delete MACs from infra list, gid {gid}, uuid {uuid}: group does not exist")),
                                    local_shard_partition_key.clone(),
                                ) {
                                    self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                } else {
                                    error!(
                                        "Failed to construct infra_group_device_del message!"
                                    );
                                }

                                warn!("Unexpected: tried to delete infra list from nonexisting group (gid {gid}, uuid {uuid}!");
                                continue;
                            }

                            let lock = self.devices_cache.clone();
                            match self
                                .cgw_remote_discovery
                                .destroy_ifras_list(gid, infras_list.clone(), lock)
                                .await
                            {
                                Ok(()) => {
                                    // All mac's GIDs been successfully changed;
                                    // Notify all of them about the change.
                                    //
                                    // Group del == unassigned (0)
                                    self.clone()
                                        .notify_devices_on_gid_change(infras_list.clone(), 0i32);

                                    if let Ok(resp) = cgw_construct_infra_group_infras_del_response(
                                        gid,
                                        // Empty vec: no infras de-assign <failed>
                                        Vec::new(),
                                        self.local_cgw_id,
                                        uuid,
                                        true,
                                        None,
                                        local_shard_partition_key.clone(),
                                    ) {
                                        self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                    } else {
                                        error!(
                                            "Failed to construct infra_group_device_del message!"
                                        );
                                    }
                                }
                                Err(macs) => {
                                    if let Error::RemoteDiscoveryFailedInfras(failed_infras) = macs
                                    {
                                        // We have a full list of macs we've tried to <del> from GID;
                                        // Remove elements from cloned list based on whethere
                                        // they're present in <failed list>
                                        // Whenever done - notify corresponding conn.processors,
                                        // that they should updated their state to have GID
                                        // change reflected;
                                        let mut macs_to_notify = infras_list.clone();
                                        macs_to_notify.retain(|&m| !failed_infras.contains(&m));

                                        // Do so, only if there are at least any <successfull>
                                        // GID changes;
                                        if !macs_to_notify.is_empty() {
                                            self.clone()
                                                .notify_devices_on_gid_change(macs_to_notify, 0i32);
                                        }

                                        if let Ok(resp) = cgw_construct_infra_group_infras_del_response(
                                            gid,
                                            failed_infras,
                                            self.local_cgw_id,
                                            uuid,
                                            false,
                                            Some(format!("Failed to destroy few MACs from infras list (partial delete), gid {gid}, uuid {uuid}")),
                                            local_shard_partition_key.clone(),
                                        ) {
                                            self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                        } else {
                                            error!(
                                                "Failed to construct infra_group_device_del message!"
                                            );
                                        }

                                        warn!("Failed to destroy few MACs from infras list (partial delete)!");
                                        continue;
                                    }
                                }
                            }
                        }
                        CGWNBApiParsedMsg {
                            uuid,
                            gid,
                            msg_type:
                                CGWNBApiParsedMsgType::InfrastructureGroupInfraMsg(
                                    device_mac,
                                    msg,
                                    timeout,
                                ),
                        } => {
                            if (self
                                .cgw_remote_discovery
                                .get_infra_group_owner_id(gid)
                                .await)
                                .is_none()
                            {
                                if let Ok(resp) = cgw_construct_infra_enqueue_response(
                                    self.local_cgw_id,
                                    uuid,
                                    false,
                                    Some(format!("Failed to sink down msg to device of nonexisting group, gid {gid}, uuid {uuid}: group does not exist")),
                                    local_shard_partition_key.clone(),
                                ) {
                                    self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                } else {
                                    error!("Failed to construct device_enqueue message!");
                                }

                                warn!("Unexpected: tried to sink down msg to device of nonexisting group (gid {gid}, uuid {uuid}!");
                                continue;
                            }

                            // 1. Parse message from NB
                            if let Ok(parsed_cmd) = cgw_ucentral_parse_command_message(&msg.clone())
                            {
                                let devices_cache = self.devices_cache.read().await;
                                match devices_cache.get_device(&device_mac) {
                                    Some(infra) => {
                                        if parsed_cmd.cmd_type == CGWUCentralCommandType::Configure
                                        {
                                            match self.config_validator.validate_config_message(
                                                &msg,
                                                infra.get_device_type(),
                                            ) {
                                                Ok(()) => {
                                                    // 3. Add message to queue
                                                    self.enqueue_ifrastructure_request(
                                                        device_mac,
                                                        infra.get_device_state(),
                                                        infra.get_device_group_id(),
                                                        parsed_cmd,
                                                        msg,
                                                        uuid,
                                                        timeout,
                                                        local_shard_partition_key.clone(),
                                                    )
                                                    .await;
                                                }
                                                Err(e) => {
                                                    error!("Failed to validate config message! Invalid configure message for device: {device_mac}!");
                                                    if let Ok(resp) = cgw_construct_infra_enqueue_response(
                                                        self.local_cgw_id,
                                                        uuid,
                                                        false,
                                                        Some(format!("Failed to validate config message! Invalid configure message for device: {device_mac}, uuid {uuid}\nError: {e}")),
                                                        local_shard_partition_key.clone(),
                                                ) {
                                                    self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                                } else {
                                                    error!("Failed to construct device_enqueue message!");
                                                }
                                                    continue;
                                                }
                                            }
                                        } else {
                                            self.enqueue_ifrastructure_request(
                                                device_mac,
                                                infra.get_device_state(),
                                                infra.get_device_group_id(),
                                                parsed_cmd,
                                                msg,
                                                uuid,
                                                timeout,
                                                local_shard_partition_key.clone(),
                                            )
                                            .await;
                                        }
                                    }
                                    None => {
                                        error!("Failed to validate config message! Device {device_mac} does not exist in cache!");
                                        continue;
                                    }
                                }
                            } else {
                                if let Ok(resp) = cgw_construct_infra_enqueue_response(
                                    self.local_cgw_id,
                                    uuid,
                                    false,
                                    Some(format!("Failed to parse command message to device: {device_mac}, uuid {uuid}")),
                                    local_shard_partition_key.clone(),
                                ) {
                                    self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                } else {
                                    error!("Failed to construct device_enqueue message!");
                                }
                                error!("Failed to parse UCentral command!");
                            }
                        }
                        CGWNBApiParsedMsg {
                            uuid,
                            gid,
                            msg_type: CGWNBApiParsedMsgType::RebalanceGroups,
                        } => {
                            debug!("Received Rebalance Groups request, gid {gid}, uuid {uuid}");
                            match self.cgw_remote_discovery.rebalance_all_groups().await {
                                Ok(groups_res) => {
                                    if let Ok(resp) = cgw_construct_rebalance_group_response(
                                        gid,
                                        self.local_cgw_id,
                                        uuid,
                                        true,
                                        None,
                                    ) {
                                        self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                    } else {
                                        error!("Failed to construct rebalance_group message!");
                                    }

                                    debug!("Rebalancing groups completed successfully. Number of rebalanced groups {groups_res}");
                                }
                                Err(e) => {
                                    warn!(
                                        "Rebalance groups uuid {uuid} request failed! Error: {e}"
                                    );

                                    if let Ok(resp) = cgw_construct_rebalance_group_response(
                                        gid,
                                        self.local_cgw_id,
                                        uuid,
                                        false,
                                        Some(format!("Failed to rebalance groups! Error: {e}")),
                                    ) {
                                        self.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp);
                                    } else {
                                        error!("Failed to construct rebalance_group message!");
                                    }

                                    warn!("Rebalancing groups failed! Error {e}");
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
                    if let Ok(resp) = cgw_construct_infra_enqueue_response(
                        self.local_cgw_id,
                        Uuid::default(),
                        false,
                        Some(format!("Failed to parse NB API message with key {key}")),
                        local_shard_partition_key.clone(),
                    ) {
                        self.enqueue_mbox_message_from_cgw_to_nb_api(-1, resp);
                    } else {
                        error!("Failed to construct device_enqueue message!");
                    }

                    error!("Failed to parse msg from NBAPI (malformed?)!");
                    continue;
                }
            }

            // Do not proceed parsing local / remote msgs untill previous relaying has been
            // finished
            match tokio::join!(relay_task_hdl) {
                (Ok(_),) => debug!("Successfully completed relay tasks!"),
                (Err(e),) => warn!("Failed to complete relay tasks! Error: {e}"),
            }

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
                    ip_addr,
                    caps,
                    conn_processor_mbox_tx,
                ) = msg
                {
                    let device_platform: String = caps.platform.clone();

                    // if connection is unique: simply insert new conn
                    //
                    // if duplicate exists: notify server about such incident.
                    // it's up to server to notify underlying task that it should
                    // drop the connection.
                    // from now on simply insert new connection into hashmap and proceed on
                    // processing it.
                    if let Some(c) = connmap_w_lock.remove(&device_mac) {
                        tokio::spawn(async move {
                            warn!("Duplicate connection (mac: {}) detected! Closing OLD connection in favor of NEW!", device_mac);
                            let msg: CGWConnectionProcessorReqMsg =
                                CGWConnectionProcessorReqMsg::AddNewConnectionShouldClose;
                            if let Err(e) = c.send(msg) {
                                warn!("Failed to send notification about duplicate connection! Error: {e}")
                            }
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
                        "Connection map: connection with {} established, new num_of_connections: {}",
                        device_mac,
                        connmap_w_lock.len() + 1
                    );

                    let device_type = match CGWDeviceType::from_str(caps.platform.as_str()) {
                        Ok(dev_type) => dev_type,
                        Err(_e) => {
                            error!("Failed to parse device {device_mac} type!");
                            CGWDeviceType::CGWDeviceUnknown
                        }
                    };

                    // Received new connection - check if infra exist in cache
                    // If exists - it already should have assigned group
                    // If not - simply add to cache - set gid == 0, devices should't remain in DB
                    let mut devices_cache = self.devices_cache.write().await;
                    let mut device_group_id: i32 = 0;
                    if let Some(device) = devices_cache.get_device_mut(&device_mac) {
                        device_group_id = device.get_device_group_id();
                        device.set_device_state(CGWDeviceState::CGWDeviceConnected);
                        device.set_device_type(device_type);

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
                                    error!("Failed to construct foreign_infra_connection message!");
                                }

                                debug!("Detected foreign infra {} connection. Group: {}, Group Shard Owner: {}", device_mac.to_hex_string(), group_id, group_owner_id);
                            }

                            let changes =
                                cgw_detect_device_chages(&device.get_device_capabilities(), &caps);
                            match changes {
                                Some(diff) => {
                                    if let Ok(resp) = cgw_construct_infra_capabilities_changed_msg(
                                        device_mac,
                                        device.get_device_group_id(),
                                        &diff,
                                        self.local_cgw_id,
                                    ) {
                                        self.enqueue_mbox_message_from_cgw_to_nb_api(
                                            device.get_device_group_id(),
                                            resp,
                                        );
                                    } else {
                                        error!(
                                        "Failed to construct device_capabilities_changed message!"
                                    );
                                    }
                                }
                                None => {
                                    debug!(
                                        "Capabilities for device: {} was not changed",
                                        device_mac.to_hex_string()
                                    )
                                }
                            }
                        } else {
                            if let Ok(resp) = cgw_construct_unassigned_infra_connection_msg(
                                device_mac,
                                self.local_cgw_id,
                            ) {
                                self.enqueue_mbox_message_from_cgw_to_nb_api(group_id, resp);
                            } else {
                                error!("Failed to construct unassigned_infra_connection message!");
                            }

                            debug!(
                                "Detected unassigned infra {} connection",
                                device_mac.to_hex_string()
                            );
                        }

                        device.update_device_capabilities(&caps);
                        match serde_json::to_string(device) {
                            Ok(device_json) => {
                                if let Err(e) = self
                                    .cgw_remote_discovery
                                    .add_device_to_redis_cache(&device_mac, &device_json)
                                    .await
                                {
                                    error!("{e}");
                                }
                            }
                            Err(e) => {
                                error!("Failed to serialize device to json string! Error: {e}");
                            }
                        }
                    } else {
                        let device: CGWDevice = CGWDevice::new(
                            device_type,
                            CGWDeviceState::CGWDeviceConnected,
                            0,
                            false,
                            caps,
                        );
                        devices_cache.add_device(&device_mac, &device);

                        match serde_json::to_string(&device) {
                            Ok(device_json) => {
                                if let Err(e) = self
                                    .cgw_remote_discovery
                                    .add_device_to_redis_cache(&device_mac, &device_json)
                                    .await
                                {
                                    error!("{e}");
                                }
                            }
                            Err(e) => {
                                error!("Failed to serialize device to json string! Error: {e}");
                            }
                        }

                        if let Ok(resp) = cgw_construct_unassigned_infra_connection_msg(
                            device_mac,
                            self.local_cgw_id,
                        ) {
                            self.enqueue_mbox_message_from_cgw_to_nb_api(0, resp);
                        } else {
                            error!("Failed to construct unassigned_infra_connection message!");
                        }

                        debug!(
                            "Detected unassigned infra {} connection",
                            device_mac.to_hex_string()
                        );
                    }

                    if self.feature_topomap_enabled {
                        let topo_map = CGWUCentralTopologyMap::get_ref();
                        topo_map
                            .insert_device(&device_mac, device_platform.as_str(), device_group_id)
                            .await;
                    }

                    if let Ok(resp) = cgw_construct_infra_join_msg(
                        device_group_id,
                        device_mac,
                        ip_addr,
                        self.local_cgw_id,
                    ) {
                        self.enqueue_mbox_message_from_cgw_to_nb_api(device_group_id, resp);
                    } else {
                        error!("Failed to construct device_join message!");
                    }

                    connmap_w_lock.insert(device_mac, conn_processor_mbox_tx);

                    tokio::spawn(async move {
                        let msg: CGWConnectionProcessorReqMsg =
                            CGWConnectionProcessorReqMsg::AddNewConnectionAck(device_group_id);
                        if let Err(e) = conn_processor_mbox_tx_clone.send(msg) {
                            error!("Failed to send NewConnection message! Error: {e}");
                        }
                    });
                } else if let CGWConnectionServerReqMsg::ConnectionClosed(device_mac) = msg {
                    let mut device_group_id: i32 = 0;
                    info!(
                        "Connection map: removed {} serial from connmap, new num_of_connections: {}",
                        device_mac,
                        connmap_w_lock.len() - 1
                    );
                    connmap_w_lock.remove(&device_mac);

                    let mut devices_cache = self.devices_cache.write().await;
                    if let Some(device) = devices_cache.get_device_mut(&device_mac) {
                        device_group_id = device.get_device_group_id();
                        if device.get_device_remains_in_db() {
                            device.set_device_state(CGWDeviceState::CGWDeviceDisconnected);

                            match serde_json::to_string(device) {
                                Ok(device_json) => {
                                    if let Err(e) = self
                                        .cgw_remote_discovery
                                        .add_device_to_redis_cache(&device_mac, &device_json)
                                        .await
                                    {
                                        error!("{e}");
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to serialize device to json string! Error: {e}");
                                }
                            }
                        } else {
                            devices_cache.del_device(&device_mac);
                            if let Err(e) = self
                                .cgw_remote_discovery
                                .del_device_from_redis_cache(&device_mac)
                                .await
                            {
                                error!("{e}");
                            }
                        }
                    }

                    // Insert device to disconnected device list
                    {
                        let queue_lock = CGW_MESSAGES_QUEUE.read().await;
                        queue_lock
                            .device_disconnected(&device_mac, device_group_id)
                            .await;
                    }

                    if self.feature_topomap_enabled {
                        let topo_map = CGWUCentralTopologyMap::get_ref();
                        topo_map
                            .remove_device(&device_mac, device_group_id, self.clone())
                            .await;
                    }

                    if let Ok(resp) = cgw_construct_infra_leave_msg(
                        device_group_id,
                        device_mac,
                        self.local_cgw_id,
                    ) {
                        self.enqueue_mbox_message_from_cgw_to_nb_api(device_group_id, resp);
                    } else {
                        error!("Failed to construct device_leave message!");
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
                        error!("Failed to read client CN! Error: {e}");
                        return;
                    }
                },
                Err(e) => {
                    error!("Failed to accept connection: Error {e}");
                    return;
                }
            };

            let allow_mismatch = server_clone.allow_mismatch;
            let conn_processor = CGWConnectionProcessor::new(server_clone, addr);
            if let Err(e) = conn_processor
                .start(tls_stream, client_cn, allow_mismatch)
                .await
            {
                error!("Failed to start connection processor! Error: {e}");
            }
        });
    }

    pub async fn cleanup_redis(&self) {
        self.cgw_remote_discovery.cleanup_redis().await;
    }

    async fn enqueue_ifrastructure_request(
        &self,
        mac: MacAddress,
        infra_state: CGWDeviceState,
        infra_gid: i32,
        command: CGWUCentralCommand,
        message: String,
        uuid: Uuid,
        timeout: Option<u64>,
        local_shard_partition_key: Option<String>,
    ) {
        if (infra_state == CGWDeviceState::CGWDeviceConnected)
            || (infra_state == CGWDeviceState::CGWDeviceDisconnected
                && (command.cmd_type == CGWUCentralCommandType::Configure
                    || command.cmd_type == CGWUCentralCommandType::Upgrade))
        {
            let queue_msg: CGWUCentralMessagesQueueItem =
                CGWUCentralMessagesQueueItem::new(command, message, uuid, timeout);
            let queue_lock = CGW_MESSAGES_QUEUE.read().await;

            let resp_result = match queue_lock.push_device_message(mac, queue_msg).await {
                Ok(replaced_item) => match replaced_item {
                    Some(req) => cgw_construct_infra_enqueue_response(
                        self.local_cgw_id,
                        req.uuid,
                        false,
                        Some("Request replaced with new!".to_string()),
                        local_shard_partition_key,
                    ),
                    None => cgw_construct_infra_enqueue_response(
                        self.local_cgw_id,
                        uuid,
                        true,
                        None,
                        local_shard_partition_key,
                    ),
                },
                Err(e) => cgw_construct_infra_enqueue_response(
                    self.local_cgw_id,
                    uuid,
                    false,
                    Some(e.to_string()),
                    local_shard_partition_key,
                ),
            };

            match resp_result {
                Ok(resp) => self.enqueue_mbox_message_from_cgw_to_nb_api(infra_gid, resp),
                Err(e) => {
                    error!("Failed to construct infra_request_result message! Error: {e}")
                }
            }
        } else {
            if let Ok(resp) = cgw_construct_infra_enqueue_response(
                self.local_cgw_id,
                uuid,
                false,
                Some(format!(
                    "Device {mac} is disconnected! Accepting only Configure and Upgrade requests!"
                )),
                local_shard_partition_key,
            ) {
                self.enqueue_mbox_message_from_cgw_to_nb_api(infra_gid, resp);
            } else {
                error!("Failed to construct infra_request_result message!");
            }
        }
    }

    pub async fn start_queue_timeout_manager(&self) {
        loop {
            // Wait for 10 seconds
            time::sleep(TIMEOUT_MANAGER_DURATION).await;

            // iterate over disconnected devices
            let queue_lock = CGW_MESSAGES_QUEUE.read().await;
            let failed_requests = queue_lock.iterate_over_disconnected_devices().await;

            for (infra, requests) in failed_requests {
                for req in requests {
                    if let Ok(resp) = cgw_construct_infra_request_result_msg(
                        self.local_cgw_id,
                        req.1.uuid,
                        req.1.command.id,
                        false,
                        Some(format!("Request failed due to infra {} disconnect", infra)),
                    ) {
                        self.enqueue_mbox_message_from_cgw_to_nb_api(req.0, resp);
                    } else {
                        error!("Failed to construct  message!");
                    }
                }
            }
        }
    }

    pub fn get_local_id(&self) -> i32 {
        self.local_cgw_id
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
        let event: CGWUCentralEvent = cgw_ucentral_ap_parse_message(false, msg, 0)?;

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
        let event: CGWUCentralEvent = cgw_ucentral_ap_parse_message(false, msg, 0)?;

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
