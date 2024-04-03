use crate::cgw_device::{CGWDevice, CGWDeviceState};
use crate::AppArgs;

use crate::{
    cgw_connection_processor::{CGWConnectionProcessor, CGWConnectionProcessorReqMsg},
    cgw_db_accessor::CGWDBInfrastructureGroup,
    cgw_devices_cache::CGWDevicesCache,
    cgw_metrics::{CGWMetrics, CGWMetricsCounterOpType, CGWMetricsCounterType},
    cgw_nb_api_listener::CGWNBApiClient,
    cgw_remote_discovery::CGWRemoteDiscovery,
};

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

type DeviceSerial = String;
type CGWConnmapType = Arc<RwLock<HashMap<String, UnboundedSender<CGWConnectionProcessorReqMsg>>>>;

#[derive(Debug)]
struct CGWConnMap {
    map: CGWConnmapType,
}

impl CGWConnMap {
    pub fn new() -> Self {
        let hash_map: HashMap<String, UnboundedSender<CGWConnectionProcessorReqMsg>> =
            HashMap::new();
        let map: Arc<RwLock<HashMap<String, UnboundedSender<CGWConnectionProcessorReqMsg>>>> =
            Arc::new(RwLock::new(hash_map));
        let connmap = CGWConnMap { map: map };
        connmap
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
    AddNewConnection(DeviceSerial, UnboundedSender<CGWConnectionProcessorReqMsg>),
    ConnectionClosed(DeviceSerial),
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
}

/*
 * TODO: split into base struct + enum type field
 * this requires alot of refactoring in places where msg is used
 * this is needed to always have uuid of malformed / discarded msg.
 * e.g.
 * struct CGWNBApiParsedMsg {
 *     uuid: Uuid,
 *     gid: i32,
 *     type: enum CGWNBApiParsedMsgType,
 * }
 *
 * enum CGWNBApiParsedMsgType {
 *     InfrastructureGroupCreate,
 *     ...
 *     InfrastructureGroupInfraMsg(DeviceSerial, String),
 * }
 */
enum CGWNBApiParsedMsg {
    // TODO: fix kafka_simulator to provide reserved_size
    InfrastructureGroupCreate(Uuid, i32),
    InfrastructureGroupDelete(Uuid, i32),
    InfrastructureGroupInfraAdd(Uuid, i32, Vec<DeviceSerial>),
    InfrastructureGroupInfraDel(Uuid, i32, Vec<DeviceSerial>),
    InfrastructureGroupInfraMsg(Uuid, i32, DeviceSerial, String),
    RebalanceGroups(Uuid),
}

impl CGWConnectionServer {
    pub async fn new(app_args: &AppArgs) -> Arc<Self> {
        let wss_runtime_handle = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(app_args.wss_t_num)
                .thread_name_fn(|| {
                    static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                    let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                    format!("cgw-wss-t-{}", id)
                })
                .thread_stack_size(3 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap(),
        );
        let internal_mbox_runtime_handle = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("cgw-mbox")
                .thread_stack_size(1 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap(),
        );
        let nb_api_mbox_runtime_handle = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("cgw-mbox-nbapi")
                .thread_stack_size(1 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap(),
        );
        let relay_msg_mbox_runtime_handle = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("cgw-relay-mbox-nbapi")
                .thread_stack_size(1 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap(),
        );
        let nb_api_mbox_tx_runtime_handle = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("cgw-mbox-nbapi-tx")
                .thread_stack_size(1 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap(),
        );

        let (internal_tx, internal_rx) = unbounded_channel::<CGWConnectionServerReqMsg>();
        let (nb_api_tx, nb_api_rx) = unbounded_channel::<CGWConnectionNBAPIReqMsg>();

        // Give NB API client a handle where it can do a TX (CLIENT -> CGW_SERVER)
        // RX is handled in internal_mbox of CGW_Server
        let nb_api_c = CGWNBApiClient::new(app_args, &nb_api_tx);

        let server = Arc::new(CGWConnectionServer {
            local_cgw_id: app_args.cgw_id,
            connmap: CGWConnMap::new(),
            wss_rx_tx_runtime: wss_runtime_handle,
            mbox_internal_runtime_handle: internal_mbox_runtime_handle,
            mbox_nb_api_runtime_handle: nb_api_mbox_runtime_handle,
            mbox_nb_api_tx_runtime_handle: nb_api_mbox_tx_runtime_handle,
            mbox_internal_tx: internal_tx,
            nb_api_client: nb_api_c,
            cgw_remote_discovery: Arc::new(CGWRemoteDiscovery::new(app_args).await),
            mbox_relayed_messages_handle: nb_api_tx,
            mbox_relay_msg_runtime_handle: relay_msg_mbox_runtime_handle,
            devices_cache: Arc::new(RwLock::new(CGWDevicesCache::new())),
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

        // Sync RAM cache with PostgressDB.
        server
            .cgw_remote_discovery
            .sync_device_to_gid_cache(server.devices_cache.clone())
            .await;
        server.devices_cache.write().await.dump_devices_cache();

        server
    }

    pub async fn enqueue_mbox_message_to_cgw_server(&self, req: CGWConnectionServerReqMsg) {
        let _ = self.mbox_internal_tx.send(req);
    }

    pub fn enqueue_mbox_message_from_device_to_nb_api_c(&self, mac: DeviceSerial, req: String) {
        let device_id = self
            .devices_cache
            .try_read()
            .unwrap()
            .get_device_from_cache_device_id(&mac.to_string())
            .unwrap();

        let key = device_id.to_string();
        let nb_api_client_clone = self.nb_api_client.clone();
        tokio::spawn(async move {
            let _ = nb_api_client_clone
                .enqueue_mbox_message_from_cgw_server(key, req)
                .await;
        });
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

    fn parse_nbapi_msg(&self, pload: &String) -> Option<CGWNBApiParsedMsg> {
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
            infra_group_infra_devices: Vec<String>,
            uuid: Uuid,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupInfraDel {
            r#type: String,
            infra_group_id: String,
            infra_group_infra_devices: Vec<String>,
            uuid: Uuid,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct InfraGroupMsgJSON {
            r#type: String,
            infra_group_id: String,
            mac: String,
            msg: Map<String, Value>,
            uuid: Uuid,
        }

        let rc = serde_json::from_str(pload);
        if let Err(e) = rc {
            error!("{e}\n{pload}");
            return None;
        }

        let map: Map<String, Value> = rc.unwrap();

        let rc = map.get(&String::from("type"));
        if let None = rc {
            error!("No msg_type found in\n{pload}");
            return None;
        }
        let rc = rc.unwrap();

        let msg_type = rc.as_str().unwrap();
        let rc = map.get(&String::from("infra_group_id"));
        if let None = rc {
            error!("No infra_group_id found in\n{pload}");
            return None;
        }
        let rc = rc.unwrap();
        let group_id: i32 = rc.as_str().unwrap().parse().unwrap();

        //debug!("Got msg {msg_type}, infra {group_id}");

        match msg_type {
            "infrastructure_group_create" => {
                let json_msg: InfraGroupCreate = serde_json::from_str(&pload).unwrap();
                //debug!("{:?}", json_msg);
                return Some(CGWNBApiParsedMsg::InfrastructureGroupCreate(
                    json_msg.uuid,
                    group_id,
                ));
            }
            "infrastructure_group_delete" => {
                let json_msg: InfraGroupDelete = serde_json::from_str(&pload).unwrap();
                //debug!("{:?}", json_msg);
                return Some(CGWNBApiParsedMsg::InfrastructureGroupDelete(
                    json_msg.uuid,
                    group_id,
                ));
            }
            "infrastructure_group_device_add" => {
                let json_msg: InfraGroupInfraAdd = serde_json::from_str(&pload).unwrap();
                //debug!("{:?}", json_msg);
                return Some(CGWNBApiParsedMsg::InfrastructureGroupInfraAdd(
                    json_msg.uuid,
                    group_id,
                    json_msg.infra_group_infra_devices,
                ));
            }
            "infrastructure_group_device_del" => {
                let json_msg: InfraGroupInfraDel = serde_json::from_str(&pload).unwrap();
                //debug!("{:?}", json_msg);
                return Some(CGWNBApiParsedMsg::InfrastructureGroupInfraDel(
                    json_msg.uuid,
                    group_id,
                    json_msg.infra_group_infra_devices,
                ));
            }
            "infrastructure_group_device_message" => {
                let json_msg: InfraGroupMsgJSON = serde_json::from_str(&pload).unwrap();
                debug!("{:?}", json_msg);
                return Some(CGWNBApiParsedMsg::InfrastructureGroupInfraMsg(
                    json_msg.uuid,
                    group_id,
                    json_msg.mac,
                    serde_json::to_string(&json_msg.msg).unwrap(),
                ));
            }
            "rebalance_groups" => {
                let json_msg: InfraGroupMsgJSON = serde_json::from_str(&pload).unwrap();
                return Some(CGWNBApiParsedMsg::RebalanceGroups(json_msg.uuid));
            }
            &_ => {
                debug!("Unknown type {msg_type} received");
            }
        }

        None
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

        let mut local_parsed_cgw_msg_buf: Vec<CGWNBApiParsedMsg> = Vec::with_capacity(buf_capacity);

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
                if rd_num >= 1 {
                    continue;
                } else {
                    if num_of_msg_read == 0 {
                        continue;
                    }
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
            self.cgw_remote_discovery.sync_gid_to_cgw_map().await;

            local_parsed_cgw_msg_buf.clear();

            // TODO: rework to avoid re-allocating these buffers on each loop iteration
            // (get mut slice of vec / clear when done?)
            let mut relayed_cgw_msg_buf: Vec<(i32, CGWConnectionNBAPIReqMsg)> =
                Vec::with_capacity(num_of_msg_read + 1);
            let mut local_cgw_msg_buf: Vec<CGWConnectionNBAPIReqMsg> =
                Vec::with_capacity(num_of_msg_read + 1);

            while !buf.is_empty() {
                let msg = buf.remove(0);

                if let CGWConnectionNBAPIReqMsg::EnqueueNewMessageFromNBAPIListener(
                    key,
                    payload,
                    origin,
                ) = msg
                {
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
                    if let CGWNBApiParsedMsg::InfrastructureGroupCreate(_uuid, gid) = parsed_msg {
                        // DB stuff - create group for remote shards to be aware of change
                        let group = CGWDBInfrastructureGroup {
                            id: gid,
                            reserved_size: 1000i32,
                            actual_size: 0i32,
                        };
                        match self.cgw_remote_discovery.create_infra_group(&group).await {
                            Ok(_dst_cgw_id) => {
                                self.enqueue_mbox_message_from_cgw_to_nb_api(
                                    gid,
                                    format!("Group has been created successfully gid {gid}"),
                                );
                            }
                            Err(_e) => {
                                self.enqueue_mbox_message_from_cgw_to_nb_api(
                                    gid,
                                    format!(
                                        "Failed to create new group (duplicate create?), gid {gid}"
                                    ),
                                );
                                warn!("Create group gid {gid} received, but it already exists");
                            }
                        }
                        // This type of msg is handled in place, not added to buf
                        // for later processing.
                        continue;
                    } else if let CGWNBApiParsedMsg::InfrastructureGroupDelete(uuid, gid) =
                        parsed_msg
                    {
                        let lock = self.devices_cache.clone();
                        match self
                            .cgw_remote_discovery
                            .destroy_infra_group(gid, lock)
                            .await
                        {
                            Ok(()) => {
                                self.enqueue_mbox_message_from_cgw_to_nb_api(
                                    gid,
                                    format!("Group has been destroyed successfully gid {gid}, uuid {uuid}"));
                            }
                            Err(_e) => {
                                self.enqueue_mbox_message_from_cgw_to_nb_api(
                                    gid,
                                    format!("Failed to destroy group (doesn't exist?), gid {gid}, uuid {uuid}"));
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
                        local_parsed_cgw_msg_buf.push(parsed_msg);
                        continue;
                    }

                    match self
                        .cgw_remote_discovery
                        .get_infra_group_owner_id(key.parse::<i32>().unwrap())
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
                            warn!("Received msg for gid {gid_numeric}, while this group is unassigned to any of CGWs: rejecting");
                            self.enqueue_mbox_message_from_cgw_to_nb_api(
                                gid_numeric,
                                format!("Received message for unknown group {gid_numeric} - unassigned?"));
                        }
                    }
                }
            }

            let discovery_clone = self.cgw_remote_discovery.clone();
            let self_clone = self.clone();

            // Future to Handle (relay) messages for remote CGW
            let relay_task_hdl = self.mbox_relay_msg_runtime_handle.spawn(async move {
                let mut remote_cgws_map: HashMap<String, (i32, Vec<(String, String)>)> = HashMap::with_capacity(cgw_buf_prealloc_size);

                while ! relayed_cgw_msg_buf.is_empty() {
                    let msg = relayed_cgw_msg_buf.remove(0);
                    if let (dst_cgw_id, CGWConnectionNBAPIReqMsg::EnqueueNewMessageFromNBAPIListener(key, payload, _origin)) = msg {
                        debug!("Received MSG for remote CGW k:{}, local id {} relaying msg to remote...", key, self_clone.local_cgw_id);
                        if let Some(v) = remote_cgws_map.get_mut(&key) {
                            v.1.push((key, payload));
                        } else {
                            let mut tmp_vec: Vec<(String, String)> = Vec::with_capacity(num_of_msg_read);
                            tmp_vec.push((key.clone(), payload));
                            remote_cgws_map.insert(key, (dst_cgw_id, tmp_vec));
                        }
                    }
                }

                for value in remote_cgws_map.into_values() {
                    let discovery_clone = discovery_clone.clone();
                    let cgw_id = value.0;
                    let msg_stream = value.1;
                    let self_clone = self_clone.clone();
                    tokio::spawn(async move {
                        if let Err(()) = discovery_clone.relay_request_stream_to_remote_cgw(cgw_id, msg_stream).await {
                            self_clone.enqueue_mbox_message_from_cgw_to_nb_api(
                                -1,
                                format!("Failed to relay MSG stream to remote CGW{cgw_id}, UUIDs: not implemented (TODO)"));
                        }
                    });
                }
            });

            // Handle messages for local CGW
            // Parse all messages first, then process
            // TODO: try to parallelize at least parsing of msg:
            // iterate each msg, get index, spawn task that would
            // write indexed parsed msg into output parsed msg buf.
            let connmap_clone = self.connmap.map.clone();
            while !local_cgw_msg_buf.is_empty() {
                let msg = local_cgw_msg_buf.remove(0);
                if let CGWConnectionNBAPIReqMsg::EnqueueNewMessageFromNBAPIListener(
                    key,
                    payload,
                    _origin,
                ) = msg
                {
                    let gid_numeric: i32 = key.parse::<i32>().unwrap();
                    debug!(
                        "Received message for local CGW k:{key}, local id {}",
                        self.local_cgw_id
                    );
                    let msg = self.parse_nbapi_msg(&payload);
                    if let None = msg {
                        error!("Failed to parse msg from NBAPI (malformed?)");
                        continue;
                    }

                    match msg.unwrap() {
                        CGWNBApiParsedMsg::InfrastructureGroupInfraAdd(uuid, gid, mac_list) => {
                            if let None = self
                                .cgw_remote_discovery
                                .get_infra_group_owner_id(gid_numeric)
                                .await
                            {
                                warn!("Unexpected: tried to add infra list to nonexisting group (gid {gid}, uuid {uuid}");
                                self.enqueue_mbox_message_from_cgw_to_nb_api(
                                    gid,
                                    format!("Failed to insert MACs from infra list, gid {gid}, uuid {uuid}: group does not exist."));
                            }

                            let lock = self.devices_cache.clone();
                            match self
                                .cgw_remote_discovery
                                .create_ifras_list(gid, mac_list, lock)
                                .await
                            {
                                Ok(()) => {
                                    self.enqueue_mbox_message_from_cgw_to_nb_api(
                                        gid,
                                        format!("Infra list has been created successfully gid {gid}, uuid {uuid}"));
                                }
                                Err(macs) => {
                                    self.enqueue_mbox_message_from_cgw_to_nb_api(
                                        gid,
                                        format!("Failed to insert few  MACs from infra list, gid {gid}, uuid {uuid}; List of failed MACs:{}",
                                                macs.iter().map(|x| x.to_string() + ",").collect::<String>()));
                                    warn!("Failed to create few MACs from infras list (partial create)");
                                    continue;
                                }
                            }
                        }
                        CGWNBApiParsedMsg::InfrastructureGroupInfraDel(uuid, gid, mac_list) => {
                            if let None = self
                                .cgw_remote_discovery
                                .get_infra_group_owner_id(gid_numeric)
                                .await
                            {
                                warn!("Unexpected: tried to delete infra list from nonexisting group (gid {gid}, uuid {uuid}");
                                self.enqueue_mbox_message_from_cgw_to_nb_api(
                                    gid,
                                    format!("Failed to delete MACs from infra list, gid {gid}, uuid {uuid}: group does not exist."));
                            }

                            let lock = self.devices_cache.clone();
                            match self
                                .cgw_remote_discovery
                                .destroy_ifras_list(gid, mac_list, lock)
                                .await
                            {
                                Ok(()) => {
                                    self.enqueue_mbox_message_from_cgw_to_nb_api(
                                        gid,
                                        format!("Infra list has been destroyed successfully gid {gid}, uuid {uuid}"));
                                }
                                Err(macs) => {
                                    self.enqueue_mbox_message_from_cgw_to_nb_api(
                                        gid,
                                        format!("Failed to destroy few MACs from infra list (not created?), gid {gid}, uuid {uuid}; List of failed MACs:{}",
                                                macs.iter().map(|x| x.to_string() + ",").collect::<String>()));
                                    warn!("Failed to destroy few MACs from infras list (partial delete)");
                                    continue;
                                }
                            }
                        }
                        CGWNBApiParsedMsg::InfrastructureGroupInfraMsg(uuid, gid, mac, msg) => {
                            if let None = self
                                .cgw_remote_discovery
                                .get_infra_group_owner_id(gid_numeric)
                                .await
                            {
                                warn!("Unexpected: tried to sink down msg to device of nonexisting group (gid {gid}, uuid {uuid}");
                                self.enqueue_mbox_message_from_cgw_to_nb_api(
                                    gid,
                                    format!("Failed to sink down msg to device of nonexisting group, gid {gid}, uuid {uuid}: group does not exist."));
                            }

                            debug!("Sending msg to device {mac}");
                            let rd_lock = connmap_clone.read().await;
                            let rc = rd_lock.get(&mac);
                            if let None = rc {
                                error!("Cannot find suitable connection for {mac}");
                                self.enqueue_mbox_message_from_cgw_to_nb_api(
                                    gid,
                                    format!("Failed to send msg (device not connected?), gid {gid}, uuid {uuid}"));
                                continue;
                            }

                            let proc_mbox_tx = rc.unwrap();
                            if let Err(_e) = proc_mbox_tx
                                .send(CGWConnectionProcessorReqMsg::SinkRequestToDevice(mac, msg))
                            {
                                error!(
                                    "Failed to send message to remote device (msg uuid({uuid}))"
                                );
                                self.enqueue_mbox_message_from_cgw_to_nb_api(
                                    gid,
                                    format!("Failed to send msg, gid {gid}, uuid {uuid}"),
                                );
                            }
                        }
                        CGWNBApiParsedMsg::RebalanceGroups(_Uuid) => {
                            debug!("Received Rebalance Groups request");
                            match self.cgw_remote_discovery.rebalance_all_groups().await {
                                Ok(groups_res) => {
                                    debug!("Rebalancing groups completed successfully, # of rebalanced groups {groups_res}");
                                }
                                Err(_e) => {}
                            }
                        }
                        _ => {
                            debug!(
                                "Received unimplemented/unexpected group create/del msg, ignoring"
                            );
                        }
                    }
                }
            }

            // Do not proceed parsing local / remote msgs untill previous relaying has been
            // finished
            tokio::join!(relay_task_hdl);

            buf.clear();
            num_of_msg_read = 0;
        }
        panic!("RX or TX counterpart of nb_api channel part destroyed, while processing task is still active");
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
                } else {
                    if num_of_msg_read == 0 {
                        continue;
                    }
                }
            }

            let mut connmap_w_lock = self.connmap.map.write().await;

            while !buf.is_empty() {
                let msg = buf.remove(0);

                if let CGWConnectionServerReqMsg::AddNewConnection(serial, conn_processor_mbox_tx) =
                    msg
                {
                    // if connection is unique: simply insert new conn
                    //
                    // if duplicate exists: notify server about such incident.
                    // it's up to server to notify underlying task that it should
                    // drop the connection.
                    // from now on simply insert new connection into hashmap and proceed on
                    // processing it.
                    let serial_clone: DeviceSerial = serial.clone();
                    if let Some(c) = connmap_w_lock.remove(&serial_clone) {
                        tokio::spawn(async move {
                            warn!("Duplicate connection (mac:{}) detected, closing OLD connection in favor of NEW", serial_clone);
                            let msg: CGWConnectionProcessorReqMsg =
                                CGWConnectionProcessorReqMsg::AddNewConnectionShouldClose;
                            c.send(msg).unwrap();
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
                        serial,
                        connmap_w_lock.len() + 1
                    );

                    // Received new connection - check if infra exist in cache
                    // If exists - it already should have assigned group
                    // If not - simply add to cache - set gid == 0, devices should't remain in SQL DB
                    let mut devices_cache = self.devices_cache.write().await;
                    if devices_cache.check_device_exists_in_cache(&serial) {
                        devices_cache.update_device_from_cache_device_state(
                            &serial,
                            CGWDeviceState::CGWDeviceConnected,
                        );
                    } else {
                        devices_cache.add_device_to_cache(
                            &serial,
                            &CGWDevice::new(CGWDeviceState::CGWDeviceConnected, 0, false),
                        );
                    }
                    devices_cache.dump_devices_cache();

                    connmap_w_lock.insert(serial, conn_processor_mbox_tx);

                    tokio::spawn(async move {
                        let msg: CGWConnectionProcessorReqMsg =
                            CGWConnectionProcessorReqMsg::AddNewConnectionAck;
                        conn_processor_mbox_tx_clone.send(msg).unwrap();
                    });
                } else if let CGWConnectionServerReqMsg::ConnectionClosed(serial) = msg {
                    info!(
                        "connmap: removed {} serial from connmap, new num_of_connections:{}",
                        serial,
                        connmap_w_lock.len() - 1
                    );
                    connmap_w_lock.remove(&serial);

                    let mut devices_cache = self.devices_cache.write().await;
                    if devices_cache.check_device_exists_in_cache(&serial) {
                        let remains_in_db = devices_cache
                            .get_device_from_cache_device_remains_in_sql_db(&serial)
                            .unwrap();
                        if remains_in_db {
                            devices_cache.update_device_from_cache_device_state(
                                &serial,
                                CGWDeviceState::CGWDeviceDisconnected,
                            );
                        } else {
                            devices_cache.del_device_from_cache(&serial);
                        }
                        devices_cache.dump_devices_cache();
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

        panic!("RX or TX counterpart of mbox_internal channel part destroyed, while processing task is still active");
    }

    pub async fn ack_connection(
        self: Arc<Self>,
        socket: TcpStream,
        tls_acceptor: tokio_native_tls::TlsAcceptor,
        addr: SocketAddr,
        conn_idx: i64,
    ) {
        // Only ACK connection. We will either drop it or accept it once processor starts
        // (we'll handle it via "mailbox" notify handle in process_internal_mbox)
        let server_clone = self.clone();

        self.wss_rx_tx_runtime.spawn(async move {
            // Accept the TLS connection.
            let tls_stream = match tls_acceptor.accept(socket).await {
                Ok(a) => a,
                Err(e) => {
                    warn!("Err {e}");
                    return;
                }
            };
            let conn_processor = CGWConnectionProcessor::new(server_clone, conn_idx, addr);
            conn_processor.start(tls_stream).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn can_parse_connect_event() {
        let msg = get_connect_json_msg();

        let map: Map<String, Value> =
            serde_json::from_str(msg).expect("Failed to parse input json");
        let method = map["method"].as_str().unwrap();
        let event: CGWEvent = cgw_parse_jrpc_event(&map, method.to_string());

        match event {
            CGWEvent::Connect(_) => {
                assert!(true);
            }
            _ => {
                assert!(false, "Expected event to be of <Connect> type");
            }
        }
    }

    #[test]
    fn can_parse_log_event() {
        let msg = get_log_json_msg();

        let map: Map<String, Value> =
            serde_json::from_str(msg).expect("Failed to parse input json");
        let method = map["method"].as_str().unwrap();
        let event: CGWEvent = cgw_parse_jrpc_event(&map, method.to_string());

        match event {
            CGWEvent::Log(_) => {
                assert!(true);
            }
            _ => {
                assert!(false, "Expected event to be of <Log> type");
            }
        }
    }
}
