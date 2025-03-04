use crate::{
    cgw_connection_server::{CGWConnectionServer, CGWConnectionServerReqMsg},
    cgw_nb_api_listener::{
        cgw_construct_cloud_header, cgw_construct_infra_join_msg,
        cgw_construct_infra_realtime_event_message, cgw_construct_infra_request_result_msg,
        cgw_construct_infra_state_event_message, cgw_construct_unassigned_infra_join_msg,
        cgw_get_timestamp_16_digits, CGWKafkaProducerTopic, ConsumerMetadata,
    },
    cgw_ucentral_messages_queue_manager::{
        CGWUCentralMessagesQueueItem, CGWUCentralMessagesQueueState, CGW_MESSAGES_QUEUE,
        MESSAGE_TIMEOUT_DURATION,
    },
    cgw_ucentral_topology_map::CGWUCentralTopologyMap,
};

use cgw_common::{
    cgw_errors::{Error, Result},
    cgw_ucentral_parser::{
        cgw_ucentral_event_parse, cgw_ucentral_parse_connect_event, CGWUCentralCommandType,
        CGWUCentralEventType,
    },
    cgw_device::{CGWDeviceCapabilities, CGWDeviceType},
};

use eui48::MacAddress;
use futures_util::{
    stream::{SplitSink, SplitStream},
    FutureExt, SinkExt, StreamExt,
};
use serde::Serialize;
use uuid::Uuid;

use std::{net::SocketAddr, str::FromStr, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::mpsc::{unbounded_channel, UnboundedReceiver},
    time::{sleep, Duration, Instant},
};
use tokio_rustls::server::TlsStream;
use tokio_tungstenite::{tungstenite::protocol::Message, WebSocketStream};
use tungstenite::Message::{Close, Ping, Text};

type SStream = SplitStream<WebSocketStream<TlsStream<TcpStream>>>;
type SSink = SplitSink<WebSocketStream<TlsStream<TcpStream>>, Message>;

#[derive(Debug, Serialize)]
pub struct ForeignConnection {
    pub r#type: &'static str,
    pub infra_group_infra: MacAddress,
    pub destination_shard_host: String,
    pub destination_wss_port: u16,
}

fn cgw_construct_foreign_connection_msg(
    infra_group_infra: MacAddress,
    destination_shard_host: String,
    destination_wss_port: u16,
) -> Result<String> {
    let unassigned_infra_msg = ForeignConnection {
        r#type: "foreign_connection",
        infra_group_infra,
        destination_shard_host,
        destination_wss_port,
    };

    Ok(serde_json::to_string(&unassigned_infra_msg)?)
}

#[derive(Debug, Clone)]
pub enum CGWConnectionProcessorReqMsg {
    // We got green light from server to process this connection on
    // Upon creation, this conn processor is <assigned> to specific GID,
    // meaning in any replies sent from device it should include provided
    // GID (used as kafka key).
    AddNewConnectionAck(i32),
    AddNewConnectionShouldClose,
    ForeignConnection((String, u16)),
    SinkRequestToDevice(CGWUCentralMessagesQueueItem),
    // Conn Server can request this specific Processor to change
    // it's internal GID value (infra list created - new gid,
    // infra list deleted - unassigned, e.g. GID 0).
    GroupIdChanged(i32),
}

#[derive(Debug)]
enum CGWConnectionState {
    IsActive,
    IsForcedToClose,
    IsDead,
    #[allow(dead_code)]
    IsStale,
    ClosedGracefully,
}

#[derive(Debug, PartialEq)]
enum CGWUCentralMessageProcessorState {
    Idle,
    ResultPending,
}

impl std::fmt::Display for CGWUCentralMessageProcessorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CGWUCentralMessageProcessorState::Idle => write!(f, "Idle"),
            CGWUCentralMessageProcessorState::ResultPending => write!(f, "ResultPending"),
        }
    }
}

pub struct CGWConnectionProcessor {
    cgw_server: Arc<CGWConnectionServer>,
    pub serial: MacAddress,
    pub addr: SocketAddr,
    pub group_id: i32,
    pub feature_topomap_enabled: bool,
    pub device_type: CGWDeviceType,
}

impl CGWConnectionProcessor {
    pub fn new(server: Arc<CGWConnectionServer>, addr: SocketAddr) -> Self {
        let conn_processor: CGWConnectionProcessor = CGWConnectionProcessor {
            cgw_server: server.clone(),
            serial: MacAddress::default(),
            addr,
            group_id: 0,
            feature_topomap_enabled: server.feature_topomap_enabled,
            // Default to AP, it's safe, as later-on it will be changed
            device_type: CGWDeviceType::CGWDeviceAP,
        };

        conn_processor
    }

    pub async fn start(
        mut self,
        tls_stream: TlsStream<TcpStream>,
        client_cn: MacAddress,
        allow_mismatch: bool,
    ) -> Result<()> {
        let ws_stream = tokio::select! {
            _val = tokio_tungstenite::accept_async(tls_stream) => {
                match _val {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to accept TLS stream from: {}! Reason: {}. Closing connection",
                               self.addr, e);
                        return Err(Error::ConnectionProcessor("Failed to accept TLS stream!"));
                    }
                }
            }
            // TODO: configurable duration (upon server creation)
            _val = sleep(Duration::from_millis(15000)) => {
                error!("Failed to accept TLS stream from: {}! Closing connection", self.addr);
                return Err(Error::ConnectionProcessor("Failed to accept TLS stream for too long"));
            }

        };

        let (sink, mut stream) = ws_stream.split();

        // check if we have any pending msgs (we expect connect at this point, protocol-wise)
        // TODO: rework to ignore any WS-related frames until we get a connect message,
        // however there's a caveat: we can miss some events logs etc from underlying device
        // rework should consider all the options
        let msg = tokio::select! {
            _val = stream.next() => {
                match _val {
                    Some(m) => m,
                    None => {
                        error!("No connect message received from: {}! Closing connection!", self.addr);
                        return Err(Error::ConnectionProcessor("No connect message received"));
                    }
                }
            }
            // TODO: configurable duration (upon server creation)
            _val = sleep(Duration::from_millis(30000)) => {
                error!("No message received from: {}! Closing connection", self.addr);
                return Err(Error::ConnectionProcessor("No message received for too long"));
            }
        };

        // we have a next() result, but it still may be underlying io error: check for it
        // break connection if we can't work with underlying ws connection (prot err etc)
        let message = match msg {
            Ok(m) => m,
            Err(e) => {
                error!(
                    "Established connection with device, but failed to receive any messages! Error: {e}"
                );
                return Err(Error::ConnectionProcessor(
                    "Established connection with device, but failed to receive any messages",
                ));
            }
        };

        debug!("Parse Connect Event");
        let evt = match cgw_ucentral_parse_connect_event(message.clone()) {
            Ok(event) => event,
            Err(e) => {
                error!(
                    "Failed to parse connect message from: {}! Error: {e}",
                    self.addr
                );
                return Err(Error::ConnectionProcessor(
                    "Failed to receive connect message",
                ));
            }
        };

        if !allow_mismatch {
            if evt.serial != client_cn {
                error!(
                    "The client MAC address {} and client certificate CN {} check failed!",
                    evt.serial.to_hex_string(),
                    client_cn.to_hex_string()
                );
                return Err(Error::ConnectionProcessor(
                    "Client certificate CN check failed",
                ));
            } else {
                debug!(
                    "The client MAC address {} and client certificate CN {} check passed",
                    evt.serial.to_hex_string(),
                    client_cn.to_hex_string()
                );
            }
        }

        debug!(
            "Parse Connect Event done! Device serial: {}",
            evt.serial.to_hex_string()
        );

        let mut caps: CGWDeviceCapabilities = Default::default();
        match evt.evt_type {
            CGWUCentralEventType::Connect(c) => {
                caps.firmware = c.firmware;
                caps.compatible = c.capabilities.compatible;
                caps.model = c.capabilities.model;
                caps.platform = c.capabilities.platform;
                caps.label_macaddr = c.capabilities.label_macaddr;
            }
            _ => warn!(
                "Device {} is not abiding the protocol! First message expected to receive: CONNECT!",
                evt.serial
            ),
        }

        self.serial = evt.serial;

        self.device_type = match CGWDeviceType::from_str(caps.platform.as_str()) {
            Ok(dev_type) => dev_type,
            Err(_) => {
                warn!("Failed to parse device {} type!", self.serial);
                return Err(Error::ConnectionProcessor("Failed to parse device type"));
            }
        };

        // TODO: we accepted tls stream and split the WS into RX TX part,
        // now we have to ASK cgw_connection_server's permission whether
        // we can proceed on with this underlying connection.
        // cgw_connection_server has an authoritative decision whether
        // we can proceed.
        debug!("Sending ACK request for device serial: {}", self.serial);
        let orig_connect_msg = message.into_text().unwrap_or_default();
        let (mbox_tx, mut mbox_rx) = unbounded_channel::<CGWConnectionProcessorReqMsg>();
        let msg = CGWConnectionServerReqMsg::AddNewConnection(
            evt.serial,
            self.addr,
            caps,
            mbox_tx,
            orig_connect_msg,
        );
        self.cgw_server
            .enqueue_mbox_message_to_cgw_server(msg)
            .await;

        let ack = mbox_rx.recv().await;
        debug!("Got ACK response for device serial: {}", self.serial);
        if let Some(m) = ack {
            match m {
                CGWConnectionProcessorReqMsg::AddNewConnectionAck(gid) => {
                    debug!(
                        "WebSocket connection established! Address: {}, serial: {} gid {gid}",
                        self.addr, evt.serial
                    );
                    self.group_id = gid;
                }
                _ => {
                    return Err(Error::ConnectionProcessor(
                        "Unexpected response from server! Expected: ACK/NOT ACK",
                    ));
                }
            }
        } else {
            info!("Connection server declined connection! WebSocket connection for address: {}, serial: {} cannot be established!",
                  self.addr, evt.serial);
            return Err(Error::ConnectionProcessor("WebSocket connection declined"));
        }

        // Remove device from disconnected device list
        // Only connection processor can <know> that connection's established,
        // however ConnServer knows about <disconnects>, hence
        // we handle connect here, while it's up to ConnServer to <remove>
        // connection (add it to <disconnected> list)
        // NOTE: this most like also would require a proper DISCONNECT_ACK
        // synchronization between processor / server, as there could be still
        // race conditions in case if duplicate connection occurs, for example.
        {
            let queue_lock = CGW_MESSAGES_QUEUE.read().await;
            queue_lock.device_connected(&self.serial).await;
        }

        // Check if device queue already exist
        // If yes - it could mean that we have device reconnection event
        // The possible reconnect reason could be: FW Upgrade or Factory reset
        // Need to make sure queue is unlocked to process requests
        // If no - create new message queue for device
        {
            let queue_lock = CGW_MESSAGES_QUEUE.read().await;
            if queue_lock.check_messages_queue_exists(&evt.serial).await {
                queue_lock
                    .set_device_queue_state(&evt.serial, CGWUCentralMessagesQueueState::RxTx)
                    .await;
            }
        }

        self.process_connection(stream, sink, mbox_rx).await;

        Ok(())
    }

    async fn process_wss_rx_msg(
        &self,
        msg: std::result::Result<Message, tungstenite::error::Error>,
        fsm_state: &mut CGWUCentralMessageProcessorState,
        pending_req_id: u64,
        pending_req_uuid: Uuid,
        pending_req_consumer_metadata: Option<ConsumerMetadata>,
    ) -> Result<CGWConnectionState> {
        // Make sure we always track the as accurate as possible the time
        // of receiving of the event (where needed).
        let timestamp = cgw_get_timestamp_16_digits();
        let mut kafka_msg: String = String::new();

        let group_cloud_header: Option<String> =
            self.cgw_server.get_group_cloud_header(self.group_id).await;
        let infras_cloud_header: Option<String> = self
            .cgw_server
            .get_group_infra_cloud_header(self.group_id, &self.serial)
            .await;

        let cloud_header: Option<String> =
            cgw_construct_cloud_header(group_cloud_header, infras_cloud_header);

        match msg {
            Ok(msg) => match msg {
                Close(_t) => {
                    return Ok(CGWConnectionState::ClosedGracefully);
                }
                Text(payload) => {
                    if let Ok(evt) = cgw_ucentral_event_parse(
                        &self.device_type,
                        self.feature_topomap_enabled,
                        &payload,
                        timestamp,
                    ) {
                        kafka_msg.clone_from(&payload);
                        let event_type_str: String = evt.evt_type.to_string();
                        match evt.evt_type {
                            CGWUCentralEventType::State(_) => {
                                if let Some(decompressed) = evt.decompressed.clone() {
                                    kafka_msg = decompressed;
                                }
                                if self.feature_topomap_enabled {
                                    let topomap = CGWUCentralTopologyMap::get_ref();

                                    // TODO: remove this Arc clone:
                                    // Dirty hack for now: pass Arc ref of srv to topomap;
                                    // Future rework and refactoring would require to separate
                                    // NB api from being an internal obj of conn_server to be a
                                    // standalone (singleton?) object.
                                    topomap.enqueue_event(
                                        evt,
                                        self.device_type,
                                        self.serial,
                                        self.group_id,
                                        self.cgw_server.clone(),
                                    );
                                }

                                if self.group_id == 0 {
                                    // This infra is unassigned - CGW SHOULD NOT
                                    // send State/Infra Realtime event to NB
                                    return Ok(CGWConnectionState::IsActive);
                                }

                                if let Ok(resp) = cgw_construct_infra_state_event_message(
                                    event_type_str,
                                    kafka_msg,
                                    self.cgw_server.get_local_id(),
                                    cloud_header,
                                    timestamp,
                                ) {
                                    self.cgw_server
                                        .enqueue_mbox_message_from_device_to_nb_api_c(
                                            self.group_id,
                                            resp,
                                            CGWKafkaProducerTopic::State,
                                        )?;
                                } else {
                                    error!("Failed to construct infra_state_event message!");
                                }
                            }
                            CGWUCentralEventType::Healthcheck => {
                                if self.group_id == 0 {
                                    // This infra is unassigned - CGW SHOULD NOT
                                    // send State/Infra Realtime event to NB
                                    return Ok(CGWConnectionState::IsActive);
                                }

                                if let Ok(resp) = cgw_construct_infra_state_event_message(
                                    event_type_str,
                                    kafka_msg,
                                    self.cgw_server.get_local_id(),
                                    cloud_header,
                                    timestamp,
                                ) {
                                    self.cgw_server
                                        .enqueue_mbox_message_from_device_to_nb_api_c(
                                            self.group_id,
                                            resp,
                                            CGWKafkaProducerTopic::State,
                                        )?;
                                } else {
                                    error!("Failed to construct infra_state_event message!");
                                }
                            }
                            CGWUCentralEventType::Reply(content) => {
                                if *fsm_state != CGWUCentralMessageProcessorState::ResultPending {
                                    error!(
                                        "Unexpected FSM state: {}! Expected: ResultPending",
                                        *fsm_state
                                    );
                                }

                                if content.id != pending_req_id {
                                    error!(
                                        "Pending request ID {} is not equal received reply ID {}!",
                                        pending_req_id, content.id
                                    );
                                }

                                let partition = match pending_req_consumer_metadata.clone() {
                                    Some(data) => data.sender_partition,
                                    None => None,
                                };

                                *fsm_state = CGWUCentralMessageProcessorState::Idle;
                                debug!("Got reply event for pending request id: {pending_req_id}");
                                if let Ok(resp) = cgw_construct_infra_request_result_msg(
                                    self.cgw_server.get_local_id(),
                                    pending_req_uuid,
                                    pending_req_id,
                                    cloud_header,
                                    true,
                                    None,
                                    pending_req_consumer_metadata,
                                    timestamp,
                                ) {
                                    self.cgw_server.enqueue_mbox_message_from_cgw_to_nb_api(
                                        self.group_id,
                                        resp,
                                        CGWKafkaProducerTopic::CnCRes,
                                        partition,
                                    );
                                } else {
                                    error!("Failed to construct infra_request_result message!");
                                }
                            }
                            CGWUCentralEventType::RealtimeEvent(_) => {
                                if self.feature_topomap_enabled {
                                    let topomap = CGWUCentralTopologyMap::get_ref();
                                    // TODO: remove this Arc clone:
                                    // Dirty hack for now: pass Arc ref of srv to topomap;
                                    // Future rework and refactoring would require to separate
                                    // NB api from being an internal obj of conn_server to be a
                                    // standalone (singleton?) object.
                                    topomap.enqueue_event(
                                        evt,
                                        self.device_type,
                                        self.serial,
                                        self.group_id,
                                        self.cgw_server.clone(),
                                    );
                                }

                                if self.group_id == 0 {
                                    // This infra is unassigned - CGW SHOULD NOT
                                    // send State/Infra Realtime event to NB
                                    return Ok(CGWConnectionState::IsActive);
                                }

                                if let Ok(resp) = cgw_construct_infra_realtime_event_message(
                                    event_type_str,
                                    kafka_msg,
                                    self.cgw_server.get_local_id(),
                                    cloud_header,
                                    timestamp,
                                ) {
                                    self.cgw_server
                                        .enqueue_mbox_message_from_device_to_nb_api_c(
                                            self.group_id,
                                            resp,
                                            CGWKafkaProducerTopic::InfraRealtime,
                                        )?;
                                } else {
                                    error!("Failed to construct infra_realtime_event message!");
                                }
                            }
                            CGWUCentralEventType::Connect(_) => {
                                error!("Expected to receive Connect event as one of the first message from infra during connection procedure!");
                            }
                            CGWUCentralEventType::Log
                            | CGWUCentralEventType::Event
                            | CGWUCentralEventType::Alarm
                            | CGWUCentralEventType::WifiScan
                            | CGWUCentralEventType::CrashLog
                            | CGWUCentralEventType::RebootLog
                            | CGWUCentralEventType::Ping
                            | CGWUCentralEventType::VenueBroadcast
                            | CGWUCentralEventType::CfgPending
                            | CGWUCentralEventType::DeviceUpdate
                            | CGWUCentralEventType::Recovery => {
                                if self.group_id == 0 {
                                    // This infra is unassigned - CGW SHOULD NOT
                                    // send State/Infra Realtime event to NB
                                    return Ok(CGWConnectionState::IsActive);
                                }

                                if let Ok(resp) = cgw_construct_infra_realtime_event_message(
                                    event_type_str,
                                    kafka_msg,
                                    self.cgw_server.get_local_id(),
                                    cloud_header,
                                    timestamp,
                                ) {
                                    self.cgw_server.enqueue_mbox_message_from_cgw_to_nb_api(
                                        self.group_id,
                                        resp,
                                        CGWKafkaProducerTopic::InfraRealtime,
                                        None,
                                    )
                                } else {
                                    error!("Failed to construct infra_realtime_event message!");
                                }
                            }
                            CGWUCentralEventType::Unknown => {
                                error!("Received unknown event type! Message payload: {kafka_msg}");
                            }
                        }
                    }

                    return Ok(CGWConnectionState::IsActive);
                }
                Ping(_t) => {
                    return Ok(CGWConnectionState::IsActive);
                }
                _ => {}
            },
            Err(e) => {
                if let tungstenite::error::Error::AlreadyClosed = e {
                    return Err(Error::ConnectionProcessor(
                        "Underlying connection's been closed",
                    ));
                }
            }
        }

        Ok(CGWConnectionState::IsActive)
    }

    async fn process_sink_mbox_rx_msg(
        &mut self,
        sink: &mut SSink,
        val: Option<CGWConnectionProcessorReqMsg>,
    ) -> Result<CGWConnectionState> {
        if let Some(msg) = val {
            let processor_mac = self.serial;
            let timestamp = cgw_get_timestamp_16_digits();
            match msg {
                CGWConnectionProcessorReqMsg::AddNewConnectionShouldClose => {
                    debug!("process_sink_mbox_rx_msg: AddNewConnectionShouldClose, processor (mac:{processor_mac}) (ACK OK)");
                    return Ok(CGWConnectionState::IsForcedToClose);
                }
                CGWConnectionProcessorReqMsg::SinkRequestToDevice(payload) => {
                    debug!("process_sink_mbox_rx_msg: SinkRequestToDevice, processor (mac: {processor_mac}) request for (mac: {}) payload: {}",
                        payload.command.serial,
                        payload.message.clone(),
                    );
                    sink.send(Message::text(payload.message)).await.ok();
                }
                CGWConnectionProcessorReqMsg::ForeignConnection((
                    destination_shard_host,
                    destination_wss_port,
                )) => {
                    if let Ok(resp) = cgw_construct_foreign_connection_msg(
                        processor_mac,
                        destination_shard_host,
                        destination_wss_port,
                    ) {
                        debug!("process_sink_mbox_rx_msg: ForeignConnection, processor (mac: {processor_mac}) payload: {}",
                        resp.clone()
                    );
                        sink.send(Message::text(resp)).await.ok();
                    } else {
                        error!("Failed to construct foreign_connection message!");
                    }
                }
                CGWConnectionProcessorReqMsg::GroupIdChanged(new_group_id) => {
                    debug!(
                        "Received GroupID change message: mac {} - old gid {} : new gid {}",
                        self.serial, self.group_id, new_group_id
                    );

                    match (self.group_id, new_group_id) {
                        (0, new_gid) if new_gid != 0 => {
                            let group_cloud_header: Option<String> =
                                self.cgw_server.get_group_cloud_header(self.group_id).await;
                            let infras_cloud_header: Option<String> = self
                                .cgw_server
                                .get_group_infra_cloud_header(self.group_id, &self.serial)
                                .await;

                            let cloud_header: Option<String> =
                                cgw_construct_cloud_header(group_cloud_header, infras_cloud_header);

                            debug!("Infra {} changed state from [unassigned] to [assigned]. Current group id: {}, new group id {}", self.serial, self.group_id, new_group_id);
                            if let Ok(unassigned_join) = cgw_construct_infra_join_msg(
                                new_gid,
                                self.serial,
                                self.addr,
                                self.cgw_server.get_local_id(),
                                String::default(),
                                cloud_header,
                                timestamp,
                            ) {
                                self.cgw_server.enqueue_mbox_message_from_cgw_to_nb_api(
                                    new_group_id,
                                    unassigned_join,
                                    CGWKafkaProducerTopic::Connection,
                                    None,
                                );
                            } else {
                                error!("Failed to construct infra_join message!");
                            }
                        }
                        (current_gid, 0) if current_gid != 0 => {
                            debug!("Infra {} changed state from [assigned] to [unassigned]. Current group id: {}, new group id {}", self.serial, self.group_id, new_group_id);
                            if let Ok(unassigned_join) = cgw_construct_unassigned_infra_join_msg(
                                self.serial,
                                self.addr,
                                self.cgw_server.get_local_id(),
                                String::default(),
                                timestamp,
                            ) {
                                self.cgw_server.enqueue_mbox_message_from_cgw_to_nb_api(
                                    new_group_id,
                                    unassigned_join,
                                    CGWKafkaProducerTopic::Connection,
                                    None,
                                );
                            } else {
                                error!("Failed to construct unassigned_infra_join message!");
                            }
                        }
                        _ => {
                            debug!("Infra {} group id was not changed! Current group id: {}, new group id {}", self.serial, self.group_id, new_group_id);
                        }
                    }

                    self.group_id = new_group_id;
                }
                _ => {
                    warn!("Received unknown mbox message: {:?}!", msg);
                    return Err(Error::ConnectionProcessor(
                        "Connection processor (Sink MBOX): received unexpected message",
                    ));
                }
            }
        }
        Ok(CGWConnectionState::IsActive)
    }

    async fn process_stale_connection_msg(
        &self,
        _last_contact: Instant,
    ) -> Result<CGWConnectionState> {
        // TODO: configurable duration (upon server creation)
        /*
        if Instant::now().duration_since(last_contact) > Duration::from_secs(70) {
            warn!(
                "Closing connection {} (idle for too long, stale)",
                self.addr
            );
            Ok(CGWConnectionState::IsStale)
        } else {
            Ok(CGWConnectionState::IsActive)
        }
        */
        Ok(CGWConnectionState::IsActive)
    }

    async fn process_connection(
        mut self,
        mut stream: SStream,
        mut sink: SSink,
        mut mbox_rx: UnboundedReceiver<CGWConnectionProcessorReqMsg>,
    ) {
        #[derive(Debug)]
        enum WakeupReason {
            Unspecified,
            WSSRxMsg(std::result::Result<Message, tungstenite::error::Error>),
            MboxRx(Option<CGWConnectionProcessorReqMsg>),
            StaleConnection,
            BrokenConnection,
        }

        let device_mac = self.serial;
        let mut pending_req_uuid = Uuid::default();
        let mut pending_req_id: u64 = 0;
        let mut pending_req_type: CGWUCentralCommandType;
        let mut fsm_state = CGWUCentralMessageProcessorState::Idle;
        let mut last_contact = Instant::now();
        let mut pending_req_consumer_metadata: Option<ConsumerMetadata> = None;

        // 1. Get last request timeout value
        // 2. Get last request id
        // 3. Update values
        let queue_lock = CGW_MESSAGES_QUEUE.read().await;
        let last_req_id = queue_lock
            .get_device_last_request_id(&device_mac)
            .await
            .unwrap_or_default();
        if last_req_id != 0 {
            pending_req_id = last_req_id;
            fsm_state = CGWUCentralMessageProcessorState::ResultPending;
        }

        loop {
            let mut wakeup_reason: WakeupReason = WakeupReason::Unspecified;
            let mut start_time: Instant = Instant::now();

            if let Some(val) = mbox_rx.recv().now_or_never() {
                wakeup_reason = WakeupReason::MboxRx(val);
            } else if fsm_state == CGWUCentralMessageProcessorState::Idle {
                if let CGWUCentralMessagesQueueState::RxTx =
                    queue_lock.get_device_queue_state(&device_mac).await
                {
                    // Check if message Queue is not empty
                    if let Some(queue_msg) = queue_lock.dequeue_device_message(&device_mac).await {
                        // Get message from queue, start measure request processing time
                        start_time = Instant::now();

                        pending_req_id = queue_msg.command.id;
                        pending_req_type = queue_msg.command.cmd_type.clone();
                        pending_req_uuid = queue_msg.uuid;
                        pending_req_consumer_metadata = queue_msg.consumer_metadata.clone();

                        let timeout = match queue_msg.timeout {
                            Some(secs) => Duration::from_secs(secs),
                            None => MESSAGE_TIMEOUT_DURATION,
                        };

                        wakeup_reason = WakeupReason::MboxRx(Some(
                            CGWConnectionProcessorReqMsg::SinkRequestToDevice(queue_msg),
                        ));

                        // Set new pending request timeout value
                        queue_lock
                            .set_device_last_req_info(
                                &device_mac,
                                pending_req_id,
                                timeout,
                                pending_req_consumer_metadata.clone(),
                            )
                            .await;

                        debug!("Got pending request with id: {}", pending_req_id);
                        if pending_req_type == CGWUCentralCommandType::Factory
                            || pending_req_type == CGWUCentralCommandType::Upgrade
                        {
                            queue_lock
                                .set_device_queue_state(
                                    &device_mac,
                                    CGWUCentralMessagesQueueState::Discard,
                                )
                                .await;
                        } else if pending_req_type == CGWUCentralCommandType::Reboot {
                            queue_lock
                                .set_device_queue_state(
                                    &device_mac,
                                    CGWUCentralMessagesQueueState::Rx,
                                )
                                .await;
                        }

                        fsm_state = CGWUCentralMessageProcessorState::ResultPending;
                    }
                }
            }

            if let WakeupReason::Unspecified = wakeup_reason {
                // 'next()' function returns a future that resolves to Some(item) if the stream produces an item,
                // or None if the stream is terminated (i.e., there are no more items to produce).
                // 'now_or_never()' function immediately polls the future and returns Some(output) if the future is ready,
                // or None if it is not ready.
                // As result from stream we got depth nesting: Option<Option<Result<Message, Error>>>
                // if let Some(val) = stream.next().now_or_never()
                match stream.next().now_or_never() {
                    Some(val) => {
                        // Top level Option => now_or_never():
                        match val {
                            // Internal level Option => next():
                            Some(res) => {
                                match res {
                                    // Received websocket connection message
                                    // 'Close' message - may be received on graceful connection close event
                                    Ok(msg) => wakeup_reason = WakeupReason::WSSRxMsg(Ok(msg)),
                                    // Received an error from stream
                                    Err(msg) => {
                                        wakeup_reason =
                                            WakeupReason::WSSRxMsg(std::result::Result::Err(msg))
                                    }
                                }
                            }
                            // Internal level Option => None: Stream is terminated (socket closed)
                            None => wakeup_reason = WakeupReason::BrokenConnection,
                        }
                    }
                    None => {
                        // No any message received from Stream
                        // Connection is still active / established, but no messages available
                        wakeup_reason = WakeupReason::StaleConnection;
                        sleep(Duration::from_millis(1000)).await;
                    }
                }
            }

            // Doesn't matter if connection was closed or terminated
            // Do message queue timeout tick and cleanup queue due to timeout
            // Or decrease timer value - on connection termination - background task
            // is responsible to cleanup queue
            if fsm_state == CGWUCentralMessageProcessorState::ResultPending {
                let elapsed_time = Instant::now() - start_time;

                // check request timeout value - if request get timed out - remove it from queue
                if queue_lock
                    .device_request_tick(&device_mac, elapsed_time)
                    .await
                {
                    let queue_lock = CGW_MESSAGES_QUEUE.read().await;
                    let flushed_requests = queue_lock.clear_device_message_queue(&device_mac).await;

                    let group_cloud_header: Option<String> =
                        self.cgw_server.get_group_cloud_header(self.group_id).await;
                    let infras_cloud_header: Option<String> = self
                        .cgw_server
                        .get_group_infra_cloud_header(self.group_id, &self.serial)
                        .await;

                    let cloud_header: Option<String> =
                        cgw_construct_cloud_header(group_cloud_header, infras_cloud_header);
                    let timestamp = cgw_get_timestamp_16_digits();

                    for req in flushed_requests {
                        let consumer_partition = req.consumer_metadata;
                        let partition = match consumer_partition.clone() {
                            Some(data) => data.sender_partition,
                            None => None,
                        };

                        if let Ok(resp) = cgw_construct_infra_request_result_msg(
                            self.cgw_server.get_local_id(),
                            req.uuid,
                            req.command.id,
                            cloud_header.clone(),
                            false,
                            Some(format!(
                                "Request flushed from infra queue {device_mac} due to previous request timeout!"
                            )),
                            consumer_partition,
                            timestamp,
                        ) {
                            // Currently Device Queue Manager does not store infras GID
                            self.cgw_server
                                .enqueue_mbox_message_from_cgw_to_nb_api(self.group_id, resp, CGWKafkaProducerTopic::CnCRes, partition);
                        } else {
                            error!("Failed to construct  message!");
                        }
                    }

                    queue_lock
                        .set_device_queue_state(&device_mac, CGWUCentralMessagesQueueState::RxTx)
                        .await;
                    queue_lock
                        .set_device_last_req_info(&device_mac, 0, Duration::ZERO, None)
                        .await;
                    let pending_req_partition = match pending_req_consumer_metadata.clone() {
                        Some(data) => data.sender_partition,
                        None => None,
                    };
                    fsm_state = CGWUCentralMessageProcessorState::Idle;
                    if let Ok(resp) = cgw_construct_infra_request_result_msg(
                        self.cgw_server.get_local_id(),
                        pending_req_uuid,
                        pending_req_id,
                        cloud_header,
                        false,
                        Some("Request timed out".to_string()),
                        pending_req_consumer_metadata,
                        timestamp,
                    ) {
                        self.cgw_server.enqueue_mbox_message_from_cgw_to_nb_api(
                            self.group_id,
                            resp,
                            CGWKafkaProducerTopic::CnCRes,
                            pending_req_partition,
                        );
                    } else {
                        error!("Failed to construct rebalance_group message!");
                    }
                    pending_req_consumer_metadata = None;
                    pending_req_id = 0;
                }
            }

            // Process WakeUp reason
            let rc = match wakeup_reason {
                WakeupReason::WSSRxMsg(res) => {
                    last_contact = Instant::now();
                    self.process_wss_rx_msg(
                        res,
                        &mut fsm_state,
                        pending_req_id,
                        pending_req_uuid,
                        pending_req_consumer_metadata.clone(),
                    )
                    .await
                }
                WakeupReason::MboxRx(mbox_message) => {
                    self.process_sink_mbox_rx_msg(&mut sink, mbox_message).await
                }
                WakeupReason::StaleConnection => {
                    self.process_stale_connection_msg(last_contact).await
                }
                WakeupReason::BrokenConnection => Ok(CGWConnectionState::IsDead),
                WakeupReason::Unspecified => {
                    Err(Error::ConnectionProcessor("Unspecified wakeup reason"))
                }
            };

            // Handle result of WakeUp reason processing
            match rc {
                Ok(state) => {
                    if let CGWConnectionState::IsActive = state {
                        continue;
                    } else if let CGWConnectionState::IsForcedToClose = state {
                        // Return, because server already closed our mbox tx counterpart (rx),
                        // hence we don't need to send ConnectionClosed message. Server
                        // already knows we're closed.
                        return;
                    } else if let CGWConnectionState::ClosedGracefully = state {
                        warn!(
                            "Remote client {} closed connection gracefully!",
                            self.serial.to_hex_string()
                        );
                        return self.send_connection_close_event().await;
                    } else if let CGWConnectionState::IsStale = state {
                        warn!(
                            "Remote client {} closed due to inactivity!",
                            self.serial.to_hex_string()
                        );
                        return self.send_connection_close_event().await;
                    } else if let CGWConnectionState::IsDead = state {
                        warn!(
                            "Remote client {} connection is dead!",
                            self.serial.to_hex_string()
                        );
                        return self.send_connection_close_event().await;
                    }
                }
                Err(e) => {
                    warn!("Connection processor closed! Error: {e}");
                    return self.send_connection_close_event().await;
                }
            }
        }
    }

    async fn send_connection_close_event(&self) {
        let msg = CGWConnectionServerReqMsg::ConnectionClosed(self.serial);
        self.cgw_server
            .enqueue_mbox_message_to_cgw_server(msg)
            .await;
        debug!(
            "MBOX_OUT: ConnectionClosed, processor (mac: {})",
            self.serial.to_hex_string()
        );
    }
}
