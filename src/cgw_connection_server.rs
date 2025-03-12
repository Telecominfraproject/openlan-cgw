use crate::cgw_device::{
    cgw_detect_device_changes, CGWDevice, CGWDeviceCapabilities, CGWDeviceState, CGWDeviceType,
    OldNew,
};
use crate::cgw_nb_api_listener::{
    cgw_construct_cgw_alert_message, cgw_construct_cloud_header,
    cgw_construct_foreign_infra_connection_msg, cgw_construct_infra_capabilities_changed_msg,
    cgw_construct_infra_enqueue_response, cgw_construct_infra_group_create_response,
    cgw_construct_infra_group_delete_response, cgw_construct_infra_group_infras_add_response,
    cgw_construct_infra_group_infras_del_response,
    cgw_construct_infra_group_infras_set_cloud_header_response,
    cgw_construct_infra_group_set_cloud_header_response, cgw_construct_infra_join_msg,
    cgw_construct_infra_leave_msg, cgw_construct_infra_request_result_msg,
    cgw_construct_rebalance_group_response, cgw_construct_unassigned_infra_join_msg,
    cgw_construct_unassigned_infra_leave_msg, cgw_get_timestamp_16_digits, CGWKafkaProducerTopic,
    ConsumerMetadata,
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
use std::{
    collections::{HashMap, HashSet},
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
        String,
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
    // and other corresponding Server task Reads RX counterpart
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
    // and other corresponding NB API client is responsible for doing an RX over
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

    pub infras_capacity: i32,
    pub local_shard_partition_key: RwLock<Option<String>>,
    pub last_update_timestamp: RwLock<i64>,
}

#[derive(Debug, Clone)]
enum CGWNBApiParsedMsgType {
    InfrastructureGroupCreate(Option<String>, Option<ConsumerMetadata>),
    InfrastructureGroupCreateToShard(i32, Option<String>, Option<ConsumerMetadata>),
    InfrastructureGroupDelete(Option<ConsumerMetadata>),
    InfrastructureGroupInfrasAdd(Vec<MacAddress>, Option<String>, Option<ConsumerMetadata>),
    InfrastructureGroupInfrasDel(Vec<MacAddress>, Option<ConsumerMetadata>),
    InfrastructureGroupInfraMsg(MacAddress, String, Option<u64>, Option<ConsumerMetadata>),
    InfrastructureGroupSetCloudHeader(Option<String>, Option<ConsumerMetadata>),
    InfrastructureGroupInfrasSetCloudHeader(
        Vec<MacAddress>,
        Option<String>,
        Option<ConsumerMetadata>,
    ),
    RebalanceGroups(Option<ConsumerMetadata>),
}

#[derive(Debug, Clone)]
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

    async fn handle(self, server: Arc<CGWConnectionServer>) {
        let timestamp: i64 = cgw_get_timestamp_16_digits();

        match self.msg_type.clone() {
            CGWNBApiParsedMsgType::InfrastructureGroupCreate(cloud_header, consumer_metadata) => {
                self.handle_infrastructure_group_create(
                    server,
                    cloud_header,
                    consumer_metadata,
                    timestamp,
                )
                .await
            }
            CGWNBApiParsedMsgType::InfrastructureGroupCreateToShard(
                shard_id,
                cloud_header,
                consumer_metadata,
            ) => {
                self.handle_infrastructure_group_create_to_shard(
                    server,
                    shard_id,
                    cloud_header,
                    consumer_metadata,
                    timestamp,
                )
                .await
            }
            CGWNBApiParsedMsgType::InfrastructureGroupDelete(consumer_metadata) => {
                self.handle_infrastructure_group_delete(server, consumer_metadata, timestamp)
                    .await
            }
            CGWNBApiParsedMsgType::InfrastructureGroupInfrasAdd(
                infras_list,
                cloud_header,
                consumer_metadata,
            ) => {
                self.handle_infrastructure_group_infras_add(
                    server,
                    infras_list,
                    cloud_header,
                    consumer_metadata,
                    timestamp,
                )
                .await
            }
            CGWNBApiParsedMsgType::InfrastructureGroupInfrasDel(infras_list, consumer_metadata) => {
                self.handle_infrastructure_group_infras_del(
                    server,
                    infras_list,
                    consumer_metadata,
                    timestamp,
                )
                .await
            }
            CGWNBApiParsedMsgType::InfrastructureGroupInfraMsg(
                infra,
                msg,
                timeout,
                consumer_metadata,
            ) => {
                self.handle_infrastructure_group_infra_msg(
                    server,
                    infra,
                    msg,
                    timeout,
                    consumer_metadata,
                    timestamp,
                )
                .await
            }
            CGWNBApiParsedMsgType::InfrastructureGroupSetCloudHeader(
                cloud_header,
                consumer_metadata,
            ) => {
                self.handle_infrastructure_group_set_cloud_header(
                    server,
                    cloud_header,
                    consumer_metadata,
                    timestamp,
                )
                .await
            }
            CGWNBApiParsedMsgType::InfrastructureGroupInfrasSetCloudHeader(
                infras_list,
                cloud_header,
                consumer_metadata,
            ) => {
                self.handle_infrastructure_group_infras_set_cloud_header(
                    server,
                    infras_list,
                    cloud_header,
                    consumer_metadata,
                    timestamp,
                )
                .await
            }
            CGWNBApiParsedMsgType::RebalanceGroups(consumer_metadata) => {
                self.handle_rebalance_groups(server, consumer_metadata, timestamp)
                    .await
            }
        }
    }

    async fn handle_infrastructure_group_create(
        &self,
        server: Arc<CGWConnectionServer>,
        cloud_header: Option<String>,
        consumer_metadata: Option<ConsumerMetadata>,
        timestamp: i64,
    ) {
        let response: Result<String>;
        let uuid = self.uuid;
        let gid = self.gid;
        // DB stuff - create group for remote shards to be aware of change
        let group = CGWDBInfrastructureGroup {
            id: gid,
            reserved_size: server.infras_capacity,
            actual_size: 0i32,
            cloud_header,
        };
        let partition = match consumer_metadata.clone() {
            Some(data) => data.sender_partition,
            None => None,
        };

        match server
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
                {
                    let mut ts_w_lock = server.last_update_timestamp.write().await;
                    *ts_w_lock = server.get_redis_last_update_timestamp().await;
                }

                response = cgw_construct_infra_group_create_response(
                    gid,
                    server.local_cgw_id,
                    uuid,
                    true,
                    None,
                    consumer_metadata,
                    timestamp,
                );
            }
            Err(e) => {
                warn!("Create group gid {gid}, uuid {uuid} request failed! Error: {e}");

                response = cgw_construct_infra_group_create_response(
                    gid,
                    server.local_cgw_id,
                    uuid,
                    false,
                    Some(format!("Failed to create new group! Error: {e}")),
                    consumer_metadata,
                    timestamp,
                );
            }
        }

        if let Ok(resp) = response {
            server.enqueue_mbox_message_from_cgw_to_nb_api(
                gid,
                resp,
                CGWKafkaProducerTopic::CnCRes,
                partition,
            );
        } else {
            error!("Failed to construct infra_group_create message!");
        }
    }

    async fn handle_infrastructure_group_create_to_shard(
        &self,
        server: Arc<CGWConnectionServer>,
        shard_id: i32,
        cloud_header: Option<String>,
        consumer_metadata: Option<ConsumerMetadata>,
        timestamp: i64,
    ) {
        let response: Result<String>;
        let uuid = self.uuid;
        let gid = self.gid;
        // DB stuff - create group for remote shards to be aware of change
        let group = CGWDBInfrastructureGroup {
            id: gid,
            reserved_size: server.infras_capacity,
            actual_size: 0i32,
            cloud_header,
        };
        let partition = match consumer_metadata.clone() {
            Some(data) => data.sender_partition,
            None => None,
        };

        match server
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
                {
                    let mut ts_w_lock = server.last_update_timestamp.write().await;
                    *ts_w_lock = server.get_redis_last_update_timestamp().await;
                }

                response = cgw_construct_infra_group_create_response(
                    gid,
                    server.local_cgw_id,
                    uuid,
                    true,
                    None,
                    consumer_metadata,
                    timestamp,
                );
            }
            Err(e) => {
                warn!("Create group gid {gid}, uuid {uuid} request failed, reason: {e}");

                response = cgw_construct_infra_group_create_response(
                    gid,
                    server.local_cgw_id,
                    uuid,
                    false,
                    Some(format!(
                        "Failed to create new group to shard id {shard_id}! Error {e}"
                    )),
                    consumer_metadata,
                    timestamp,
                );
            }
        }

        if let Ok(resp) = response {
            server.enqueue_mbox_message_from_cgw_to_nb_api(
                gid,
                resp,
                CGWKafkaProducerTopic::CnCRes,
                partition,
            );
        } else {
            error!("Failed to construct infra_group_create message!");
        }
    }

    async fn handle_infrastructure_group_delete(
        &self,
        server: Arc<CGWConnectionServer>,
        consumer_metadata: Option<ConsumerMetadata>,
        timestamp: i64,
    ) {
        let response: Result<String>;
        let lock = server.devices_cache.clone();
        let uuid = self.uuid;
        let gid = self.gid;
        let partition = match consumer_metadata.clone() {
            Some(data) => data.sender_partition,
            None => None,
        };

        match server
            .cgw_remote_discovery
            .destroy_infra_group(gid, lock)
            .await
        {
            Ok(()) => {
                // We try to help free topomap memory usage
                // by notifying it whenever GID get's destroyed.
                // However, for allocation we let topomap
                // handle it's mem alloc whenever necessary
                // on it's own, when data from specific gid
                // arrives - we rely on topomap to
                // allocate necessary structures on it's own.
                //
                // In this way, we make sure that we handle
                // properly the GID restoration scenario:
                // if CGW restarts and loads GID info from
                // DB, there would be no notification about
                // create/del, and there's no need to
                // offload this responsibility to
                // remote_discovery module for example,
                // due to the fact that CGW is not designed
                // for management of redis without CGW knowledge:
                // if something disrupts the redis state / sql
                // state without CGW's prior knowledge,
                // it's not a responsibility of CGW to be aware
                // of such changes and handle it correspondingly.
                if server.feature_topomap_enabled {
                    let topomap = CGWUCentralTopologyMap::get_ref();
                    topomap.remove_gid(gid).await;
                }

                response = cgw_construct_infra_group_delete_response(
                    gid,
                    server.local_cgw_id,
                    uuid,
                    true,
                    None,
                    consumer_metadata,
                    timestamp,
                );
            }
            Err(e) => {
                warn!("Destroy group gid {gid}, uuid {uuid} request failed! Error: {e}");

                response = cgw_construct_infra_group_delete_response(
                    gid,
                    server.local_cgw_id,
                    uuid,
                    false,
                    Some(format!("Failed to delete group! Error: {e}")),
                    consumer_metadata,
                    timestamp,
                );
            }
        }

        if let Ok(resp) = response {
            server.enqueue_mbox_message_from_cgw_to_nb_api(
                gid,
                resp,
                CGWKafkaProducerTopic::CnCRes,
                partition,
            );
        } else {
            error!("Failed to construct infra_group_delete message!");
        }
    }

    async fn handle_infrastructure_group_infras_add(
        &self,
        server: Arc<CGWConnectionServer>,
        infras_list: Vec<MacAddress>,
        cloud_header: Option<String>,
        consumer_metadata: Option<ConsumerMetadata>,
        timestamp: i64,
    ) {
        let local_shard_partition_key = {
            // Drop lock as soon as we return value
            server.local_shard_partition_key.read().await
        };
        let devices_cache_lock = server.devices_cache.clone();
        let uuid = self.uuid;
        let gid = self.gid;
        let partition = match consumer_metadata.clone() {
            Some(data) => data.sender_partition,
            None => None,
        };

        if (server
            .cgw_remote_discovery
            .get_infra_group_owner_id(gid)
            .await)
            .is_none()
        {
            if let Ok(resp) = cgw_construct_infra_group_infras_add_response(
                gid,
                infras_list.clone(),
                server.local_cgw_id,
                uuid,
                false,
                Some(format!(
                    "Failed to add infra list to nonexisting group, gid {gid}, uuid {uuid}"
                )),
                local_shard_partition_key.clone(),
                consumer_metadata,
                timestamp,
            ) {
                server.enqueue_mbox_message_from_cgw_to_nb_api(
                    gid,
                    resp,
                    CGWKafkaProducerTopic::CnCRes,
                    partition,
                );
            } else {
                error!("Failed to construct infra_group_device_add message!");
            }

            warn!(
                "Unexpected: tried to add infra list to nonexisting group, gid {gid}, uuid {uuid}!"
            );
            return;
        }

        let mut is_success = false;
        let mut failed_infras = Vec::new();
        let mut error_message = None;
        let mut successful_infras = Vec::new();

        match server
            .cgw_remote_discovery
            .create_infras_list(gid, infras_list.clone(), devices_cache_lock, cloud_header)
            .await
        {
            Ok(success_infras) => {
                // Not all mac's GIDs might been successfully changed;
                // Notify all of them about the change.
                server
                    .clone()
                    .notify_devices_on_gid_change(success_infras.clone(), gid);

                successful_infras = success_infras;
                is_success = true;
            }
            Err(Error::RemoteDiscoveryFailedInfras(failed_infras_result)) => {
                // We have a full list of macs we've tried to <add> to GID;
                // Remove elements from cloned list based on where there
                // they're present in <failed list>
                // Whenever done - notify corresponding conn.processors,
                // that they should updated their state to have GID
                // change reflected;
                let mut infras_to_notify = infras_list.clone();
                infras_to_notify.retain(|&m| !failed_infras_result.contains(&m));

                // Do so, only if there are at least any <successful>
                // GID changes;
                if !infras_to_notify.is_empty() {
                    server
                        .clone()
                        .notify_devices_on_gid_change(infras_to_notify.clone(), gid);
                    successful_infras = infras_to_notify;
                }

                failed_infras = failed_infras_result;
                error_message = Some(format!("Failed to create few MACs from infras list (partial create), gid {gid}, uuid {uuid}"));
                warn!("Failed to create few MACs from infras list (partial create)!");
            }
            _ => {
                error_message = Some(format!(
                    "Unknown error during infrastructure creation, gid {gid}, uuid {uuid}"
                ));
                warn!("Unknown error during infrastructure creation!");
            }
        }

        if let Ok(resp) = cgw_construct_infra_group_infras_add_response(
            gid,
            failed_infras,
            server.local_cgw_id,
            uuid,
            is_success,
            error_message,
            local_shard_partition_key.clone(),
            consumer_metadata,
            timestamp,
        ) {
            server.enqueue_mbox_message_from_cgw_to_nb_api(
                gid,
                resp,
                CGWKafkaProducerTopic::CnCRes,
                partition,
            );
        } else {
            error!("Failed to construct infra_group_device_add message!");
        }

        // Process successfully added MAC addresses - only if we have any
        if successful_infras.is_empty() {
            return;
        }

        let devices_cache_read = server.devices_cache.read().await;
        for infra in successful_infras {
            let queue_lock = CGW_MESSAGES_QUEUE.read().await;

            // Update queue state if it exists
            if queue_lock.check_messages_queue_exists(&infra).await {
                queue_lock
                    .set_device_queue_state(&infra, CGWUCentralMessagesQueueState::RxTx)
                    .await;
            }

            // Process device if it exists in cache
            if let Some(dev) = devices_cache_read.get_device(&infra) {
                if dev.get_device_state() == CGWDeviceState::CGWDeviceConnected {
                    let dev_gid = dev.get_device_group_id();
                    let changes = cgw_detect_device_changes(
                        &CGWDeviceCapabilities::default(),
                        &dev.get_device_capabilities(),
                    );

                    // Process device capabilities changes if any
                    if let Some(diff) = changes {
                        // Get cloud headers
                        let group_cloud_header = server
                            .cgw_remote_discovery
                            .get_group_cloud_header(dev_gid)
                            .await;

                        let infras_cloud_header = server
                            .cgw_remote_discovery
                            .get_group_infra_cloud_header(dev_gid, &infra)
                            .await;

                        let cloud_header =
                            cgw_construct_cloud_header(group_cloud_header, infras_cloud_header);

                        // Send capabilities changed message
                        if let Ok(resp) = cgw_construct_infra_capabilities_changed_msg(
                            infra,
                            dev_gid,
                            &diff,
                            server.local_cgw_id,
                            cloud_header,
                            timestamp,
                        ) {
                            server.enqueue_mbox_message_from_cgw_to_nb_api(
                                dev_gid,
                                resp,
                                CGWKafkaProducerTopic::Connection,
                                partition,
                            );
                        } else {
                            error!("Failed to construct device_capabilities_changed message!");
                        }
                    } else {
                        debug!(
                            "Capabilities for device: {} was not changed",
                            infra.to_hex_string()
                        );
                    }
                } else {
                    // Device is not connected, update its status
                    queue_lock
                        .device_disconnected(&infra, dev.get_device_group_id())
                        .await;
                }
            }
        }
    }

    async fn handle_infrastructure_group_infras_del(
        &self,
        server: Arc<CGWConnectionServer>,
        infras_list: Vec<MacAddress>,
        consumer_metadata: Option<ConsumerMetadata>,
        timestamp: i64,
    ) {
        let local_shard_partition_key = {
            // Drop lock as soon as we return value
            server.local_shard_partition_key.read().await
        };
        let uuid = self.uuid;
        let gid = self.gid;
        let partition = match consumer_metadata.clone() {
            Some(data) => data.sender_partition,
            None => None,
        };

        if (server
            .cgw_remote_discovery
            .get_infra_group_owner_id(gid)
            .await)
            .is_none()
        {
            if let Ok(resp) = cgw_construct_infra_group_infras_del_response(
                gid,
                infras_list.clone(),
                server.local_cgw_id,
                uuid,
                false,
                Some(format!("Failed to delete MACs from infra list, gid {gid}, uuid {uuid}: group does not exist")),
                local_shard_partition_key.clone(),
                consumer_metadata,
                timestamp,
            ) {
                server.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp, CGWKafkaProducerTopic::CnCRes, partition);
            } else {
                error!("Failed to construct infra_group_device_del message!");
            }

            warn!("Unexpected: tried to delete infra list from nonexisting group (gid {gid}, uuid {uuid})!");
            return;
        }

        let lock = server.devices_cache.clone();
        let mut is_success = false;
        let mut failed_infras = Vec::new();
        let mut error_message = None;

        match server
            .cgw_remote_discovery
            .destroy_infras_list(gid, infras_list.clone(), lock)
            .await
        {
            Ok(()) => {
                // All mac's GIDs been successfully changed;
                // Notify all of them about the change.
                //
                // Group del == unassigned (0)
                server
                    .clone()
                    .notify_devices_on_gid_change(infras_list.clone(), 0i32);

                is_success = true;
            }
            Err(infras) => {
                if let Error::RemoteDiscoveryFailedInfras(failed_infras_result) = infras {
                    // We have a full list of macs we've tried to <del> from GID;
                    // Remove elements from cloned list based on where there
                    // they're present in <failed list>
                    // Whenever done - notify corresponding conn.processors,
                    // that they should updated their state to have GID
                    // change reflected;
                    let mut infras_to_notify = infras_list.clone();
                    infras_to_notify.retain(|&m| !failed_infras_result.contains(&m));

                    // Do so, only if there are at least any <successful>
                    // GID changes;
                    if !infras_to_notify.is_empty() {
                        server
                            .clone()
                            .notify_devices_on_gid_change(infras_to_notify, 0i32);
                    }

                    warn!("Failed to destroy few MACs from infras list (partial delete)!");

                    failed_infras = failed_infras_result;
                    error_message = Some(format!("Failed to destroy few MACs from infras list (partial delete), gid {gid}, uuid {uuid}"));
                } else {
                    // Handle other possible error types
                    error_message = Some(format!(
                        "Unknown error occurred while deleting MACs, gid {gid}, uuid {uuid}"
                    ));
                }
            }
        };

        if let Ok(resp) = cgw_construct_infra_group_infras_del_response(
            gid,
            failed_infras,
            server.local_cgw_id,
            uuid,
            is_success,
            error_message,
            local_shard_partition_key.clone(),
            consumer_metadata,
            timestamp,
        ) {
            server.enqueue_mbox_message_from_cgw_to_nb_api(
                gid,
                resp,
                CGWKafkaProducerTopic::CnCRes,
                partition,
            );
        } else {
            error!("Failed to construct infra_group_device_del message!");
        }
    }

    async fn handle_infrastructure_group_infra_msg(
        &self,
        server: Arc<CGWConnectionServer>,
        device_mac: MacAddress,
        msg: String,
        timeout: Option<u64>,
        consumer_metadata: Option<ConsumerMetadata>,
        timestamp: i64,
    ) {
        let local_shard_partition_key = {
            // Drop lock as soon as we return value
            server.local_shard_partition_key.read().await
        };
        let uuid = self.uuid;
        let gid = self.gid;
        let partition = match consumer_metadata.clone() {
            Some(data) => data.sender_partition,
            None => None,
        };

        if (server
            .cgw_remote_discovery
            .get_infra_group_owner_id(gid)
            .await)
            .is_none()
        {
            if let Ok(resp) = cgw_construct_infra_enqueue_response(
                server.local_cgw_id,
                uuid,
                false,
                Some(format!("Failed to sink down msg to device of nonexisting group, gid {gid}, uuid {uuid}: group does not exist")),
                local_shard_partition_key.clone(),
                consumer_metadata,
                timestamp,
            ) {
                server.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp, CGWKafkaProducerTopic::CnCRes, partition);
            } else {
                error!("Failed to construct device_enqueue message!");
            }

            warn!("Unexpected: tried to sink down msg to device of nonexisting group (gid {gid}, uuid {uuid}!");
            return;
        }

        // 1. Parse message from NB
        let parse_result = cgw_ucentral_parse_command_message(&msg.clone());
        let parsed_cmd: CGWUCentralCommand = match parse_result {
            Ok(cmd) => cmd,
            Err(_e) => {
                // Parsing error
                if let Ok(resp) = cgw_construct_infra_enqueue_response(
                    server.local_cgw_id,
                    uuid,
                    false,
                    Some(format!(
                        "Failed to parse command message to device: {device_mac}, uuid {uuid}"
                    )),
                    local_shard_partition_key.clone(),
                    consumer_metadata,
                    timestamp,
                ) {
                    server.enqueue_mbox_message_from_cgw_to_nb_api(
                        gid,
                        resp,
                        CGWKafkaProducerTopic::CnCRes,
                        partition,
                    );
                } else {
                    error!("Failed to construct device_enqueue message!");
                }
                error!("Failed to parse UCentral command!");
                return;
            }
        };

        // 2. Get device information
        let devices_cache = server.devices_cache.read().await;
        let infra = match devices_cache.get_device(&device_mac) {
            Some(device) => device,
            None => {
                // CGW does not know anyting about destination infra!
                // Send response to NB.
                if let Ok(resp) = cgw_construct_infra_enqueue_response(
                    server.local_cgw_id,
                    uuid,
                    false,
                    Some(format!("Failed to process infrastructure_group_infra_msg! CGW does not know anything about infara {device_mac}!")),
                    local_shard_partition_key.clone(),
                    consumer_metadata.clone(),
                    timestamp,
                ) {
                    server.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp, CGWKafkaProducerTopic::CnCRes, partition);
                } else {
                    error!("Failed to construct infra_enqueue message!");
                }

                error!("Failed to validate config message! Device {device_mac} does not exist in cache!");
                return;
            }
        };

        // 3. Process the command
        if parsed_cmd.cmd_type == CGWUCentralCommandType::Configure {
            // Device type: if type is unknown - skipp infrastructure group infra msg processing
            // The device type is known only after receiving CONNECT message from infra.
            // The device type presented in Capabilites section
            if infra.get_device_type() == CGWDeviceType::CGWDeviceUnknown {
                if let Ok(resp) = cgw_construct_infra_enqueue_response(
                    server.local_cgw_id,
                    uuid,
                    false,
                    Some(format!("Configure message uuid {uuid} skipped! Infra {device_mac} has not sent connect message yet")),
                    local_shard_partition_key.clone(),
                    consumer_metadata.clone(),
                    timestamp,
                ) {
                    server.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp, CGWKafkaProducerTopic::CnCRes, partition);
                } else {
                    error!("Failed to construct infra_enqueue message!");
                }
            }

            let validation_result = server
                .config_validator
                .validate_config_message(&msg, infra.get_device_type());

            if let Err(e) = validation_result {
                error!("Failed to validate config message! Invalid configure message for device: {device_mac}!");
                if let Ok(resp) = cgw_construct_infra_enqueue_response(
                    server.local_cgw_id,
                    uuid,
                    false,
                    Some(format!("Failed to validate config message! Invalid configure message for device: {device_mac}, uuid {uuid}\nError: {e}")),
                    local_shard_partition_key.clone(),
                    consumer_metadata,
                    timestamp,
                ) {
                    server.enqueue_mbox_message_from_cgw_to_nb_api(gid, resp, CGWKafkaProducerTopic::CnCRes, partition);
                } else {
                    error!("Failed to construct infra_enqueue message!");
                }
                return;
            }
        }

        // 4. Add message to queue
        server
            .enqueue_infrastructure_request(
                (
                    device_mac,
                    infra.get_device_state(),
                    infra.get_device_group_id(),
                ),
                parsed_cmd,
                msg,
                uuid,
                timeout,
                local_shard_partition_key.clone(),
                consumer_metadata,
                timestamp,
            )
            .await;
    }

    async fn handle_infrastructure_group_set_cloud_header(
        &self,
        server: Arc<CGWConnectionServer>,
        cloud_header: Option<String>,
        consumer_metadata: Option<ConsumerMetadata>,
        timestamp: i64,
    ) {
        let response: Result<String>;
        let uuid = self.uuid;
        let gid = self.gid;
        let partition = match consumer_metadata.clone() {
            Some(data) => data.sender_partition,
            None => None,
        };

        // Check if group exist
        match server.cgw_remote_discovery.get_group_from_db(gid).await {
            Some(_group) => {
                server
                    .cgw_remote_discovery
                    .update_group_to_header_map(&gid, cloud_header)
                    .await;

                // Check if Redis updated
                if let Err(e) = server
                    .cgw_remote_discovery
                    .update_infra_group_header_redis(gid)
                    .await
                {
                    response = cgw_construct_infra_group_set_cloud_header_response(
                        gid,
                        server.local_cgw_id,
                        uuid,
                        false,
                        Some(e.to_string()),
                        consumer_metadata,
                        timestamp,
                    );
                } else {
                    response = cgw_construct_infra_group_set_cloud_header_response(
                        gid,
                        server.local_cgw_id,
                        uuid,
                        true,
                        None,
                        consumer_metadata,
                        timestamp,
                    );
                }
            }
            None => {
                response = cgw_construct_infra_group_set_cloud_header_response(
                    gid,
                    server.local_cgw_id,
                    uuid,
                    false,
                    Some(format!("Group id {gid} does not exist!")),
                    consumer_metadata,
                    timestamp,
                );
            }
        }

        if let Ok(resp) = response {
            server.enqueue_mbox_message_from_cgw_to_nb_api(
                gid,
                resp,
                CGWKafkaProducerTopic::CnCRes,
                partition,
            );
        } else {
            error!("Failed to construct infra_group_set_cloud_header_response message!");
        }
    }

    async fn handle_infrastructure_group_infras_set_cloud_header(
        &self,
        server: Arc<CGWConnectionServer>,
        infras_list: Vec<MacAddress>,
        cloud_header: Option<String>,
        consumer_metadata: Option<ConsumerMetadata>,
        timestamp: i64,
    ) {
        let response: Result<String>;
        let uuid = self.uuid;
        let gid = self.gid;
        let partition = match consumer_metadata.clone() {
            Some(data) => data.sender_partition,
            None => None,
        };

        match server
            .cgw_remote_discovery
            .get_group_infras_from_db(gid)
            .await
        {
            Some(group_infras) => {
                let infras_set: HashSet<MacAddress> =
                    group_infras.iter().map(|infra| infra.mac).collect();

                let mut success_infras: Vec<MacAddress> = Vec::with_capacity(infras_set.len());
                let mut failed_infras: Vec<MacAddress> = Vec::with_capacity(infras_set.len());

                for infra in infras_list.clone() {
                    if infras_set.contains(&infra) {
                        success_infras.push(infra);
                    } else {
                        failed_infras.push(infra);
                    }
                }

                server
                    .cgw_remote_discovery
                    .update_group_infras_header_map(gid, success_infras, cloud_header)
                    .await;

                if let Err(e) = server
                    .cgw_remote_discovery
                    .update_infra_group_infras_header_redis(gid)
                    .await
                {
                    // We failed to insert each infra - redis was not updated!
                    response = cgw_construct_infra_group_infras_set_cloud_header_response(
                        gid,
                        infras_list,
                        server.local_cgw_id,
                        uuid,
                        false,
                        Some(e.to_string()),
                        consumer_metadata,
                        timestamp,
                    );
                } else {
                    response = match failed_infras.is_empty() {
                        true => {
                            // Successfully update group infras to cloud header map
                            cgw_construct_infra_group_infras_set_cloud_header_response(
                                gid,
                                failed_infras,
                                server.local_cgw_id,
                                uuid,
                                true,
                                None,
                                consumer_metadata,
                                timestamp,
                            )
                        }
                        false => {
                            // Failed to update group infras to cloud header map for few MACs
                            cgw_construct_infra_group_infras_set_cloud_header_response(
                                    gid,
                                    failed_infras,
                                    server.local_cgw_id,
                                    uuid,
                                    false,
                                    Some(format!("Failed to update group {gid} infras cloud header map! Partial update!")),
                                    consumer_metadata,
                                    timestamp,
                                )
                        }
                    };
                }
            }
            None => {
                response = cgw_construct_infra_group_set_cloud_header_response(
                    gid,
                    server.local_cgw_id,
                    uuid,
                    false,
                    Some(format!("Group id {gid} does not exist!")),
                    consumer_metadata,
                    timestamp,
                );
            }
        }

        if let Ok(resp) = response {
            server.enqueue_mbox_message_from_cgw_to_nb_api(
                gid,
                resp,
                CGWKafkaProducerTopic::CnCRes,
                partition,
            );
        } else {
            error!("Failed to construct infra_group_set_cloud_header_response message!");
        }
    }

    async fn handle_rebalance_groups(
        &self,
        server: Arc<CGWConnectionServer>,
        consumer_metadata: Option<ConsumerMetadata>,
        timestamp: i64,
    ) {
        let gid = self.gid;
        let uuid = self.uuid;
        let partition = match consumer_metadata.clone() {
            Some(data) => data.sender_partition,
            None => None,
        };

        debug!("Received Rebalance Groups request, gid {gid}, uuid {uuid}");

        match server.cgw_remote_discovery.rebalance_all_groups().await {
            Ok(groups_res) => {
                if let Ok(resp) = cgw_construct_rebalance_group_response(
                    gid,
                    server.local_cgw_id,
                    uuid,
                    true,
                    None,
                    consumer_metadata,
                    timestamp,
                ) {
                    server.enqueue_mbox_message_from_cgw_to_nb_api(
                        gid,
                        resp,
                        CGWKafkaProducerTopic::CnCRes,
                        partition,
                    );
                } else {
                    error!("Failed to construct rebalance_group message!");
                }

                debug!("Rebalancing groups completed successfully. Number of rebalanced groups {groups_res}");
            }
            Err(e) => {
                warn!("Rebalance groups uuid {uuid} request failed! Error: {e}");

                if let Ok(resp) = cgw_construct_rebalance_group_response(
                    gid,
                    server.local_cgw_id,
                    uuid,
                    false,
                    Some(format!("Failed to rebalance groups! Error: {e}")),
                    consumer_metadata,
                    timestamp,
                ) {
                    server.enqueue_mbox_message_from_cgw_to_nb_api(
                        gid,
                        resp,
                        CGWKafkaProducerTopic::CnCRes,
                        partition,
                    );
                } else {
                    error!("Failed to construct rebalance_group message!");
                }

                warn!("Rebalancing groups failed! Error {e}");
            }
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
        // but use spawn_blocking where needed in contexts that rely on the
        // underlying async calls.
        let app_args_clone = app_args.validation_schema.clone();
        let get_config_validator_fut =
            tokio::task::spawn_blocking(move || CGWUCentralConfigValidators::new(app_args_clone));
        let config_validator = match get_config_validator_fut.await {
            Ok(res) => match res {
                Ok(validator) => validator,
                Err(e) => {
                    error!(
                        "Can't create CGW Connection server: Config validator create failed: {e}"
                    );

                    return Err(Error::ConnectionServer(format!(
                        "Can't create CGW Connection server: Config validator create failed: {e}",
                    )));
                }
            },
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
            infras_capacity: app_args.cgw_group_infras_capacity,
            local_shard_partition_key: RwLock::new(None),
            last_update_timestamp: RwLock::new(0i64),
        });

        let server_clone = server.clone();
        // Task for processing mbox_internal_rx, task owns the RX part
        server.mbox_internal_runtime_handle.spawn(async move {
            server_clone.process_internal_mbox(internal_rx).await;
        });

        let server_clone = server.clone();
        CGWMetrics::get_ref().change_counter(
            CGWMetricsCounterType::GroupInfrasCapacity,
            CGWMetricsCounterOpType::Set(app_args.cgw_group_infras_capacity.into()),
        );
        server.mbox_nb_api_runtime_handle.spawn(async move {
            server_clone.process_internal_nb_api_mbox(nb_api_rx).await;
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
            let topomap = CGWUCentralTopologyMap::get_ref();
            topomap.start(&server.wss_rx_tx_runtime).await;
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
        topic: CGWKafkaProducerTopic,
    ) -> Result<()> {
        let nb_api_client_clone = self.nb_api_client.clone();
        tokio::spawn(async move {
            let key = group_id.to_string();
            nb_api_client_clone
                .enqueue_mbox_message_from_cgw_server(key, req, topic, None)
                .await;
        });

        Ok(())
    }

    pub fn enqueue_mbox_message_from_cgw_to_nb_api(
        &self,
        gid: i32,
        req: String,
        topic: CGWKafkaProducerTopic,
        partition: Option<i32>,
    ) {
        let nb_api_client_clone = self.nb_api_client.clone();
        self.mbox_nb_api_tx_runtime_handle.spawn(async move {
            nb_api_client_clone
                .enqueue_mbox_message_from_cgw_server(gid.to_string(), req, topic, partition)
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

    fn parse_nbapi_msg(&self, payload: &str) -> Result<CGWNBApiParsedMsg> {
        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupCreate {
            r#type: String,
            infra_group_id: i32,
            uuid: Uuid,
            // Cloud header is an opaque type from the perspective of CGW.
            // We're actually agnostic to whatever content there is.
            //
            // Moreover, we also support <header:NULL> format
            // to ease out NULL-ing procedure from the cloud perspective.
            #[serde(default, rename = "cloud-header")]
            group_cloud_header: Option<Map<String, Value>>,
            consumer_metadata: Option<ConsumerMetadata>,
        }
        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupCreateToShard {
            r#type: String,
            infra_group_id: i32,
            shard_id: i32,
            uuid: Uuid,
            // Cloud header is an opaque type from the perspective of CGW.
            // We're actually agnostic to whatever content there is.
            //
            // Moreover, we also support <header:NULL> format
            // to ease out NULL-ing procedure from the cloud perspective.
            #[serde(default, rename = "cloud-header")]
            group_cloud_header: Option<Map<String, Value>>,
            consumer_metadata: Option<ConsumerMetadata>,
        }
        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupDelete {
            r#type: String,
            infra_group_id: i32,
            uuid: Uuid,
            consumer_metadata: Option<ConsumerMetadata>,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupInfrasAdd {
            r#type: String,
            infra_group_id: i32,
            infra_group_infras: Vec<MacAddress>,
            uuid: Uuid,
            // Cloud header is an opaque type from the perspective of CGW.
            // We're actually agnostic to whatever content there is.
            //
            // Moreover, we also support <header:NULL> format
            // to ease out NULL-ing procedure from the cloud perspective.
            #[serde(default, rename = "cloud-header")]
            infras_cloud_header: Option<Map<String, Value>>,
            consumer_metadata: Option<ConsumerMetadata>,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupInfrasDel {
            r#type: String,
            infra_group_id: i32,
            infra_group_infras: Vec<MacAddress>,
            uuid: Uuid,
            consumer_metadata: Option<ConsumerMetadata>,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupMsgJSON {
            r#type: String,
            infra_group_id: i32,
            infra_group_infra: MacAddress,
            msg: Map<String, Value>,
            uuid: Uuid,
            timeout: Option<u64>,
            consumer_metadata: Option<ConsumerMetadata>,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct RebalanceGroups {
            r#type: String,
            infra_group_id: i32,
            uuid: Uuid,
            consumer_metadata: Option<ConsumerMetadata>,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupSetCloudHeader {
            r#type: String,
            infra_group_id: i32,
            // Cloud header is an opaque type from the perspective of CGW.
            // We're actually agnostic to whatever content there is.
            //
            // Moreover, we also support <header:NULL> format
            // to ease out NULL-ing procedure from the cloud perspective.
            #[serde(default, rename = "cloud-header")]
            group_cloud_header: Option<Map<String, Value>>,
            uuid: Uuid,
            consumer_metadata: Option<ConsumerMetadata>,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupInfrasSetCloudHeader {
            r#type: String,
            infra_group_id: i32,
            infra_group_infras: Vec<MacAddress>,
            // Cloud header is an opaque type from the perspective of CGW.
            // We're actually agnostic to whatever content there is.
            //
            // Moreover, we also support <header:NULL> format
            // to ease out NULL-ing procedure from the cloud perspective.
            #[serde(rename = "cloud-header")]
            infras_cloud_header: Option<Map<String, Value>>,
            uuid: Uuid,
            consumer_metadata: Option<ConsumerMetadata>,
        }

        let map: Map<String, Value> = serde_json::from_str(payload)?;

        let rc = map.get("type").ok_or(Error::ConnectionServer(
            "parse_nbapi_msg: failed to get message type!".to_string(),
        ))?;
        let msg_type = rc.as_str().ok_or(Error::ConnectionServer(format!(
            "parse_nbapi_msg: failed to convert message type {rc} to string!"
        )))?;

        let rc = map.get("infra_group_id").ok_or(Error::ConnectionServer(
            "parse_nbapi_msg: failed to get infra_group_id!".to_string(),
        ))?;
        let group_id: i32 = rc.as_i64().ok_or(Error::ConnectionServer(format!(
            "parse_nbapi_msg: failed to convert infra_group_id {rc} as i32!"
        )))? as i32;

        let rc = map.get("consumer_metadata");
        let consumer_metadata: Option<ConsumerMetadata> = match rc {
            Some(value) => match serde_json::from_value(value.clone()) {
                Ok(metadata) => metadata,
                Err(e) => {
                    return Err(Error::ConnectionServer(format!(
                        "Failed to parse {msg_type} message consumer_metadata! Error: {e}"
                    )));
                }
            },
            None => None,
        };

        match msg_type {
            "infrastructure_group_create" => {
                let json_msg: InfraGroupCreate = match serde_json::from_str(payload) {
                    Ok(json) => json,
                    Err(e) => {
                        return Err(Error::ConnectionServer(format!(
                            "Failed to parse infrastructure_group_create message! Error: {e}"
                        )));
                    }
                };
                let cloud_header: Option<String> = match json_msg.group_cloud_header {
                    Some(ref header) => Some(serde_json::to_string(header)?),
                    None => None,
                };
                return Ok(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::InfrastructureGroupCreate(
                        cloud_header,
                        consumer_metadata,
                    ),
                ));
            }
            "infrastructure_group_create_to_shard" => {
                let json_msg: InfraGroupCreateToShard = match serde_json::from_str(payload) {
                    Ok(json) => json,
                    Err(e) => {
                        return Err(Error::ConnectionServer(format!("Failed to parse infrastructure_group_create_to_shard message! Error: {e}")));
                    }
                };
                let cloud_header: Option<String> = match json_msg.group_cloud_header {
                    Some(ref header) => Some(serde_json::to_string(header)?),
                    None => None,
                };
                return Ok(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::InfrastructureGroupCreateToShard(
                        json_msg.shard_id,
                        cloud_header,
                        consumer_metadata,
                    ),
                ));
            }
            "infrastructure_group_delete" => {
                let json_msg: InfraGroupDelete = match serde_json::from_str(payload) {
                    Ok(json) => json,
                    Err(e) => {
                        return Err(Error::ConnectionServer(format!(
                            "Failed to parse infrastructure_group_delete message! Error: {e}"
                        )));
                    }
                };
                return Ok(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::InfrastructureGroupDelete(consumer_metadata),
                ));
            }
            "infrastructure_group_infras_add" => {
                let json_msg: InfraGroupInfrasAdd = match serde_json::from_str(payload) {
                    Ok(json) => json,
                    Err(e) => {
                        return Err(Error::ConnectionServer(format!(
                            "Failed to parse infrastructure_group_infras_add message! Error: {e}"
                        )));
                    }
                };
                let cloud_header: Option<String> = match json_msg.infras_cloud_header {
                    Some(ref header) => Some(serde_json::to_string(header)?),
                    None => None,
                };
                return Ok(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::InfrastructureGroupInfrasAdd(
                        json_msg.infra_group_infras,
                        cloud_header,
                        consumer_metadata,
                    ),
                ));
            }
            "infrastructure_group_infras_del" => {
                let json_msg: InfraGroupInfrasDel = match serde_json::from_str(payload) {
                    Ok(json) => json,
                    Err(e) => {
                        return Err(Error::ConnectionServer(format!(
                            "Failed to parse infrastructure_group_infras_del message! Error: {e}"
                        )));
                    }
                };
                return Ok(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::InfrastructureGroupInfrasDel(
                        json_msg.infra_group_infras,
                        consumer_metadata,
                    ),
                ));
            }
            "infrastructure_group_infra_message_enqueue" => {
                let json_msg: InfraGroupMsgJSON = match serde_json::from_str(payload) {
                    Ok(json) => json,
                    Err(e) => {
                        return Err(Error::ConnectionServer(format!("Failed to parse infrastructure_group_infra_message_enqueue message! Error: {e}")));
                    }
                };
                return Ok(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::InfrastructureGroupInfraMsg(
                        json_msg.infra_group_infra,
                        serde_json::to_string(&json_msg.msg)?,
                        json_msg.timeout,
                        consumer_metadata,
                    ),
                ));
            }
            "rebalance_groups" => {
                let json_msg: RebalanceGroups = match serde_json::from_str(payload) {
                    Ok(json) => json,
                    Err(e) => {
                        return Err(Error::ConnectionServer(format!(
                            "Failed to parse rebalance_groups message! Error: {e}"
                        )));
                    }
                };
                return Ok(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::RebalanceGroups(consumer_metadata),
                ));
            }
            "infrastructure_group_set_cloud_header" => {
                let json_msg: InfraGroupSetCloudHeader = match serde_json::from_str(payload) {
                    Ok(json) => json,
                    Err(e) => {
                        return Err(Error::ConnectionServer(format!("Failed to parse infrastructure_group_set_cloud_header message! Error: {e}")));
                    }
                };
                let cloud_header: Option<String> = match json_msg.group_cloud_header {
                    Some(ref header) => Some(serde_json::to_string(header)?),
                    None => None,
                };
                return Ok(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::InfrastructureGroupSetCloudHeader(
                        cloud_header,
                        consumer_metadata,
                    ),
                ));
            }
            "infrastructure_group_infras_set_cloud_header" => {
                let json_msg: InfraGroupInfrasSetCloudHeader = match serde_json::from_str(payload) {
                    Ok(json) => json,
                    Err(e) => {
                        return Err(Error::ConnectionServer(format!("Failed to parse infrastructure_group_infras_set_cloud_header message! Error: {e}")));
                    }
                };
                let cloud_header: Option<String> = match json_msg.infras_cloud_header {
                    Some(ref header) => Some(serde_json::to_string(header)?),
                    None => None,
                };
                return Ok(CGWNBApiParsedMsg::new(
                    json_msg.uuid,
                    group_id,
                    CGWNBApiParsedMsgType::InfrastructureGroupInfrasSetCloudHeader(
                        json_msg.infra_group_infras,
                        cloud_header,
                        consumer_metadata,
                    ),
                ));
            }
            &_ => {
                debug!("Unknown type {msg_type} received");
            }
        }

        Err(Error::ConnectionServer(format!(
            "Unknown type {msg_type} received!"
        )))
    }

    fn notify_device_on_foreign_connection(self: Arc<Self>, mac: MacAddress, shard_id_owner: i32) {
        tokio::spawn(async move {
            let (destination_shard_host, destination_wss_port) = match self
                .cgw_remote_discovery
                .get_shard_host_and_wss_port(shard_id_owner)
                .await
            {
                Ok((host, port)) => (host, port),
                Err(e) => {
                    error!("Failed to get shard {shard_id_owner} info! Error: {e}");
                    return;
                }
            };

            let connmap_r_lock = self.connmap.map.read().await;
            let msg: CGWConnectionProcessorReqMsg = CGWConnectionProcessorReqMsg::ForeignConnection(
                (destination_shard_host.clone(), destination_wss_port),
            );

            if let Some(c) = connmap_r_lock.get(&mac) {
                match c.send(msg.clone()) {
                    Ok(_) => {
                        debug!("Notified {mac} about foreign connection. Shard hostname: {destination_shard_host}, wss port: {destination_wss_port}")
                    }
                    Err(e) => warn!("Failed to send GID change notification! Error: {e}"),
                }
            }
        });
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

    async fn kafka_partition_key_update(&self) {
        let partition_mapping = self.nb_api_client.get_partition_to_local_shard_mapping();
        let partition_key: Option<String>;

        debug!(
            "Kafka partitions idx:key mapping info: {:?}",
            partition_mapping
        );
        if !partition_mapping.is_empty() {
            partition_key = Some(partition_mapping[0].1.clone());

            debug!(
                "Using kafka key '{}' for kafka partition idx '{}'",
                partition_mapping[0].1, partition_mapping[0].0
            );
        } else {
            warn!("Cannot get partition to local shard mapping, won't be able to return kafka routing key in NB request replies!");
            // Clear previously used partition key
            partition_key = None;
        }

        let mut kafka_key_w_lock = self.local_shard_partition_key.write().await;

        *kafka_key_w_lock = partition_key;
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
        let mut last_update_timestamp: i64 = {
            let mut ts_w_lock = self.last_update_timestamp.write().await;
            *ts_w_lock = self.get_redis_last_update_timestamp().await;
            *ts_w_lock
        };

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
                // And this could repeat up until buffer is full, or no new messages
                // appear on the 10ms scale.
                // Highly unlikely scenario, but still possible.
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

                        {
                            let mut ts_w_lock = self.last_update_timestamp.write().await;
                            *ts_w_lock = current_timestamp;
                            last_update_timestamp = current_timestamp
                        }

                        let mut infras_list: Vec<MacAddress> = Vec::new();
                        let connmap_r_lock = self.connmap.map.read().await;

                        for (infra_mac, _) in connmap_r_lock.iter() {
                            match self.devices_cache.read().await.get_device(infra_mac) {
                                Some(infra) => {
                                    if infra.get_device_group_id() == 0 {
                                        infras_list.push(*infra_mac);
                                    }
                                }
                                None => {
                                    infras_list.push(*infra_mac);
                                }
                            }
                        }

                        if !infras_list.is_empty() {
                            self.clone().notify_devices_on_gid_change(infras_list, 0);
                        }
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

            let timestamp = cgw_get_timestamp_16_digits();

            self.kafka_partition_key_update().await;

            local_shard_partition_key = {
                let kafka_key_r_lock = self.local_shard_partition_key.read().await;
                kafka_key_r_lock.clone()
            };

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
                    Ok(val) => val,
                    Err(e) => {
                        warn!("Failed to parse recv msg with key {key}, discarded! Error: {e}");

                        if let Ok(resp) = cgw_construct_cgw_alert_message(
                            self.local_cgw_id,
                            e.to_string(),
                            payload,
                            timestamp,
                        ) {
                            self.enqueue_mbox_message_from_cgw_to_nb_api(
                                -1,
                                resp,
                                CGWKafkaProducerTopic::CnCRes,
                                None,
                            );
                        } else {
                            error!("Failed to construct infra_enqueue message!");
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
                if let CGWNBApiParsedMsgType::InfrastructureGroupCreate(..) = parsed_msg.msg_type {
                    parsed_msg.clone().handle(self.clone()).await;
                    // This type of msg is handled in place, not added to buf
                    // for later processing.
                    continue;
                } else if let CGWNBApiParsedMsgType::InfrastructureGroupCreateToShard(..) =
                    parsed_msg.msg_type
                {
                    parsed_msg.clone().handle(self.clone()).await;
                    // This type of msg is handled in place, not added to buf
                    // for later processing.
                    continue;
                } else if let CGWNBApiParsedMsgType::InfrastructureGroupDelete(..) =
                    parsed_msg.msg_type
                {
                    parsed_msg.clone().handle(self.clone()).await;
                    // This type of msg is handled in place, not added to buf
                    // for later processing.
                    continue;
                } else if let CGWNBApiParsedMsgType::InfrastructureGroupSetCloudHeader(..) =
                    parsed_msg.msg_type
                {
                    parsed_msg.clone().handle(self.clone()).await;
                    // This type of msg is handled in place, not added to buf
                    // for later processing.
                    continue;
                } else if let CGWNBApiParsedMsgType::InfrastructureGroupInfrasSetCloudHeader(..) =
                    parsed_msg.msg_type
                {
                    parsed_msg.clone().handle(self.clone()).await;
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
                let CGWNBApiParsedMsg { gid, .. } = parsed_msg;

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
                        // We relaying on underlying logic to handle specific request and manage corner cases
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
                    let timestamp = cgw_get_timestamp_16_digits();
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
                                None,
                                timestamp,
                            ) {
                                self_clone.enqueue_mbox_message_from_cgw_to_nb_api(-1, resp, CGWKafkaProducerTopic::CnCRes, None);
                            } else {
                                error!("Failed to construct infra_enqueue message!");
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

                match self.parse_nbapi_msg(&payload) {
                    Ok(msg) => {
                        msg.handle(self.clone()).await;
                    }
                    Err(e) => {
                        error!("Failed to parse NB API message with key {key}! Error: {e}");

                        if let Ok(resp) = cgw_construct_cgw_alert_message(
                            self.local_cgw_id,
                            e.to_string(),
                            payload,
                            timestamp,
                        ) {
                            self.enqueue_mbox_message_from_cgw_to_nb_api(
                                -1,
                                resp,
                                CGWKafkaProducerTopic::CnCRes,
                                None,
                            );
                        } else {
                            error!("Failed to construct infra_enqueue message!");
                        }

                        error!("Failed to parse msg from NBAPI (malformed?)!");
                        continue;
                    }
                }
            }

            // Do not proceed parsing local / remote msgs until previous relaying has been
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
            let timestamp = cgw_get_timestamp_16_digits();

            while !buf.is_empty() {
                let msg = buf.remove(0);

                if let CGWConnectionServerReqMsg::AddNewConnection(
                    device_mac,
                    ip_addr,
                    caps,
                    conn_processor_mbox_tx,
                    orig_connect_message,
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

                    let mut devices_cache = self.devices_cache.write().await;
                    let cache_item = devices_cache.get_device_mut(&device_mac);
                    let mut device_group_id: i32 = 0;
                    let mut group_owner_shard_id: i32 = 0;
                    let mut capability_changes: Option<HashMap<String, OldNew>> = None;
                    let mut foreign_infra_join: bool = false;

                    if let Some(infra) = cache_item {
                        device_group_id = infra.get_device_group_id();
                        let current_device_caps = infra.get_device_capabilities();

                        if let Some(group_owner_id) = self
                            .cgw_remote_discovery
                            .get_infra_group_owner_id(device_group_id)
                            .await
                        {
                            foreign_infra_join = self.local_cgw_id != group_owner_id;
                            group_owner_shard_id = group_owner_id;
                        }

                        // Update cached infra with new info
                        infra.set_device_state(CGWDeviceState::CGWDeviceConnected);
                        infra.set_device_type(device_type);
                        infra.update_device_capabilities(&caps);

                        // Update Redis Cache
                        match serde_json::to_string(infra) {
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

                        // Detect Capabilities changes as device exists in Cache
                        capability_changes = cgw_detect_device_changes(&current_device_caps, &caps);
                    } else {
                        // Infra is not found in RAM Cache - try to get it from PostgreSQL DB
                        if let Some(infra) = self
                            .cgw_remote_discovery
                            .get_infra_from_db(device_mac)
                            .await
                        {
                            device_group_id = infra.infra_group_id;
                            if let Some(group_owner_id) = self
                                .cgw_remote_discovery
                                .get_infra_group_owner_id(device_group_id)
                                .await
                            {
                                foreign_infra_join = self.local_cgw_id != group_owner_id;
                                group_owner_shard_id = group_owner_id;
                            }
                        } else {
                            let device: CGWDevice = CGWDevice::new(
                                device_type,
                                CGWDeviceState::CGWDeviceConnected,
                                device_group_id,
                                false,
                                caps,
                            );
                            devices_cache.add_device(&device_mac, &device);

                            // Update Redis Cache
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
                        }
                    }

                    // Construct cloud header
                    let group_cloud_header: Option<String> = self
                        .cgw_remote_discovery
                        .get_group_cloud_header(device_group_id)
                        .await;
                    let infras_cloud_header: Option<String> = self
                        .cgw_remote_discovery
                        .get_group_infra_cloud_header(device_group_id, &device_mac)
                        .await;

                    let cloud_header: Option<String> =
                        cgw_construct_cloud_header(group_cloud_header, infras_cloud_header);

                    // Check if foreign infra connection - send event
                    if foreign_infra_join {
                        debug!("Detected foreign infra {} connection. Group: {}, Group Shard Owner: {}", device_mac.to_hex_string(), device_group_id, group_owner_shard_id);

                        if let Ok(resp) = cgw_construct_foreign_infra_connection_msg(
                            device_group_id,
                            device_mac,
                            ip_addr,
                            self.local_cgw_id,
                            group_owner_shard_id,
                            cloud_header.clone(),
                            timestamp,
                        ) {
                            self.enqueue_mbox_message_from_cgw_to_nb_api(
                                device_group_id,
                                resp,
                                CGWKafkaProducerTopic::Connection,
                                None,
                            );
                        } else {
                            error!("Failed to construct foreign_infra_connection message!");
                        }

                        self.clone()
                            .notify_device_on_foreign_connection(device_mac, group_owner_shard_id);
                    }

                    // Send [un]assigned infra join message
                    let unassigned_infra_join: bool = device_group_id == 0;
                    let join_message = match unassigned_infra_join {
                        true => {
                            debug!(
                                "Detected unassigned infra {} connection, group id {}, group owner id: {}",
                                device_mac.to_hex_string(), device_group_id, group_owner_shard_id
                            );
                            cgw_construct_unassigned_infra_join_msg(
                                device_mac,
                                ip_addr,
                                self.local_cgw_id,
                                orig_connect_message,
                                timestamp,
                            )
                        }
                        false => {
                            debug!(
                                "Detected assigned infra {} connection, group id {}, group owner id: {}",
                                device_mac.to_hex_string(), device_group_id, group_owner_shard_id
                            );
                            cgw_construct_infra_join_msg(
                                device_group_id,
                                device_mac,
                                ip_addr,
                                self.local_cgw_id,
                                orig_connect_message,
                                cloud_header,
                                timestamp,
                            )
                        }
                    };

                    if let Ok(resp) = join_message {
                        self.enqueue_mbox_message_from_cgw_to_nb_api(
                            device_group_id,
                            resp,
                            CGWKafkaProducerTopic::Connection,
                            None,
                        );
                    } else {
                        error!("Failed to construct [un]assigned_infra_join message!");
                    }

                    // Check where there capabilities change event should be sent
                    if let Some(diff) = capability_changes {
                        let group_cloud_header: Option<String> = self
                            .cgw_remote_discovery
                            .get_group_cloud_header(device_group_id)
                            .await;
                        let infras_cloud_header: Option<String> = self
                            .cgw_remote_discovery
                            .get_group_infra_cloud_header(device_group_id, &device_mac)
                            .await;

                        let cloud_header: Option<String> =
                            cgw_construct_cloud_header(group_cloud_header, infras_cloud_header);

                        if let Ok(resp) = cgw_construct_infra_capabilities_changed_msg(
                            device_mac,
                            device_group_id,
                            &diff,
                            self.local_cgw_id,
                            cloud_header,
                            timestamp,
                        ) {
                            self.enqueue_mbox_message_from_cgw_to_nb_api(
                                device_group_id,
                                resp,
                                CGWKafkaProducerTopic::Connection,
                                None,
                            );
                        } else {
                            error!("Failed to construct device_capabilities_changed message!");
                        }
                    }

                    // Update topomap
                    if self.feature_topomap_enabled {
                        let topomap = CGWUCentralTopologyMap::get_ref();
                        topomap
                            .insert_device(&device_mac, device_platform.as_str(), device_group_id)
                            .await;
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
                    } else {
                        // Infra is not found in RAM Cache - try to get it from PostgreSQL DB
                        if let Some(infra) = self
                            .cgw_remote_discovery
                            .get_infra_from_db(device_mac)
                            .await
                        {
                            device_group_id = infra.infra_group_id;
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
                        let topomap = CGWUCentralTopologyMap::get_ref();
                        topomap
                            .remove_device(&device_mac, device_group_id, self.clone(), timestamp)
                            .await;
                    }

                    let group_cloud_header: Option<String> = self
                        .cgw_remote_discovery
                        .get_group_cloud_header(device_group_id)
                        .await;
                    let infras_cloud_header: Option<String> = self
                        .cgw_remote_discovery
                        .get_group_infra_cloud_header(device_group_id, &device_mac)
                        .await;

                    let cloud_header: Option<String> =
                        cgw_construct_cloud_header(group_cloud_header, infras_cloud_header);

                    // Send [un]assigned infra leave message
                    let unassigned_infra_leave: bool = device_group_id == 0;
                    let leave_message = match unassigned_infra_leave {
                        true => cgw_construct_unassigned_infra_leave_msg(
                            device_mac,
                            self.local_cgw_id,
                            timestamp,
                        ),

                        false => cgw_construct_infra_leave_msg(
                            device_group_id,
                            device_mac,
                            self.local_cgw_id,
                            cloud_header,
                            timestamp,
                        ),
                    };

                    if let Ok(resp) = leave_message {
                        self.enqueue_mbox_message_from_cgw_to_nb_api(
                            device_group_id,
                            resp,
                            CGWKafkaProducerTopic::Connection,
                            None,
                        );
                    } else {
                        error!("Failed to construct [un]assigned_infra_leave message!");
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

    async fn enqueue_infrastructure_request(
        &self,
        infra: (MacAddress, CGWDeviceState, i32),
        command: CGWUCentralCommand,
        message: String,
        uuid: Uuid,
        timeout: Option<u64>,
        local_shard_partition_key: Option<String>,
        consumer_metadata: Option<ConsumerMetadata>,
        timestamp: i64,
    ) {
        let resp_partition: Option<i32> = match consumer_metadata.clone() {
            Some(data) => data.sender_partition,
            None => None,
        };
        if (infra.1 == CGWDeviceState::CGWDeviceConnected)
            || (infra.1 == CGWDeviceState::CGWDeviceDisconnected
                && (command.cmd_type == CGWUCentralCommandType::Configure
                    || command.cmd_type == CGWUCentralCommandType::Upgrade))
        {
            let queue_msg: CGWUCentralMessagesQueueItem = CGWUCentralMessagesQueueItem::new(
                command,
                message,
                uuid,
                timeout,
                consumer_metadata.clone(),
            );
            let queue_lock = CGW_MESSAGES_QUEUE.read().await;

            let mut resp_result: Result<String> = Ok(String::default());
            let mut replaced_result: Option<Result<String>> = None;
            let mut replaced_partition: Option<i32> = None;

            let timestamp = cgw_get_timestamp_16_digits();

            match queue_lock.push_device_message(infra.0, queue_msg).await {
                Ok(replaced_item) => match replaced_item {
                    Some(item) => {
                        replaced_partition = match item.consumer_metadata.clone() {
                            Some(data) => data.sender_partition,
                            None => None,
                        };
                        replaced_result = Some(cgw_construct_infra_enqueue_response(
                            self.local_cgw_id,
                            item.uuid,
                            false,
                            Some("Request replaced with new!".to_string()),
                            local_shard_partition_key,
                            item.consumer_metadata,
                            timestamp,
                        ));
                    }
                    None => {
                        resp_result = cgw_construct_infra_enqueue_response(
                            self.local_cgw_id,
                            uuid,
                            true,
                            None,
                            local_shard_partition_key,
                            consumer_metadata,
                            timestamp,
                        );
                    }
                },
                Err(e) => {
                    resp_result = cgw_construct_infra_enqueue_response(
                        self.local_cgw_id,
                        uuid,
                        false,
                        Some(e.to_string()),
                        local_shard_partition_key,
                        consumer_metadata,
                        timestamp,
                    );
                }
            }

            // Update NB on current (enqueued) message status
            match resp_result {
                Ok(resp) => self.enqueue_mbox_message_from_cgw_to_nb_api(
                    infra.2,
                    resp,
                    CGWKafkaProducerTopic::CnCRes,
                    resp_partition,
                ),
                Err(e) => {
                    error!("Failed to construct infra_request_result message! Error: {e}")
                }
            }

            // If new unqueued message replaced old one - update NB as well!
            if let Some(replaced_msg_result) = replaced_result {
                match replaced_msg_result {
                    Ok(resp) => self.enqueue_mbox_message_from_cgw_to_nb_api(
                        infra.2,
                        resp,
                        CGWKafkaProducerTopic::CnCRes,
                        replaced_partition,
                    ),
                    Err(e) => {
                        error!("Failed to construct infra_request_result message! Error: {e}")
                    }
                }
            }
        } else if let Ok(resp) = cgw_construct_infra_enqueue_response(
            self.local_cgw_id,
            uuid,
            false,
            Some(format!(
                "Device {} is disconnected! Accepting only Configure and Upgrade requests!",
                infra.0
            )),
            local_shard_partition_key,
            consumer_metadata,
            timestamp,
        ) {
            self.enqueue_mbox_message_from_cgw_to_nb_api(
                infra.2,
                resp,
                CGWKafkaProducerTopic::CnCRes,
                resp_partition,
            );
        } else {
            error!("Failed to construct infra_request_result message!");
        }
    }

    pub async fn start_queue_timeout_manager(&self) {
        loop {
            // Wait for 10 seconds
            time::sleep(TIMEOUT_MANAGER_DURATION).await;

            // iterate over disconnected devices
            let queue_lock = CGW_MESSAGES_QUEUE.read().await;
            let failed_requests = queue_lock.iterate_over_disconnected_devices().await;
            let timestamp = cgw_get_timestamp_16_digits();

            for (infra, requests) in failed_requests {
                for (group_id, req) in requests {
                    let group_cloud_header: Option<String> = self
                        .cgw_remote_discovery
                        .get_group_cloud_header(group_id)
                        .await;
                    let infras_cloud_header: Option<String> = self
                        .cgw_remote_discovery
                        .get_group_infra_cloud_header(group_id, &infra)
                        .await;

                    let cloud_header: Option<String> =
                        cgw_construct_cloud_header(group_cloud_header, infras_cloud_header);

                    let partition = match req.consumer_metadata.clone() {
                        Some(data) => data.sender_partition,
                        None => None,
                    };

                    // Requests does not receive Reply - so payload is None
                    if let Ok(resp) = cgw_construct_infra_request_result_msg(
                        self.local_cgw_id,
                        req.uuid,
                        req.command.id,
                        cloud_header,
                        false,
                        Some(format!("Request failed due to infra {} disconnect", infra)),
                        req.consumer_metadata,
                        None,
                        timestamp,
                    ) {
                        self.enqueue_mbox_message_from_cgw_to_nb_api(
                            group_id,
                            resp,
                            CGWKafkaProducerTopic::CnCRes,
                            partition,
                        );
                    } else {
                        error!("Failed to construct  message!");
                    }
                }
            }
        }
    }

    pub async fn get_group_cloud_header(&self, group_id: i32) -> Option<String> {
        self.cgw_remote_discovery
            .get_group_cloud_header(group_id)
            .await
    }

    pub async fn get_group_infra_cloud_header(
        &self,
        group_id: i32,
        infra: &MacAddress,
    ) -> Option<String> {
        self.cgw_remote_discovery
            .get_group_infra_cloud_header(group_id, infra)
            .await
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
            CGWUCentralEventType::Log => {
                debug!("Assertion passed")
            }
            _ => {
                error!("Expected event to be of <Log> type");
            }
        }

        Ok(())
    }
}
