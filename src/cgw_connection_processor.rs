use crate::{
    cgw_connection_server::{CGWConnectionServer, CGWConnectionServerReqMsg},
    cgw_device::{CGWDeviceCapabilities, CGWDeviceType},
    cgw_errors::{Error, Result},
    cgw_nb_api_listener::cgw_construct_infra_request_result_msg,
    cgw_ucentral_messages_queue_manager::{
        CGWUCentralMessagesQueueItem, CGWUCentralMessagesQueueState, CGW_MESSAGES_QUEUE,
        MESSAGE_TIMEOUT_DURATION,
    },
    cgw_ucentral_parser::{
        cgw_ucentral_event_parse, cgw_ucentral_parse_connect_event, CGWUCentralCommandType,
        CGWUCentralEventType,
    },
    cgw_ucentral_topology_map::CGWUCentralTopologyMap,
};

use chrono::offset::Local;
use eui48::MacAddress;
use futures_util::{
    stream::{SplitSink, SplitStream},
    FutureExt, SinkExt, StreamExt,
};
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

#[derive(Debug, Clone)]
pub enum CGWConnectionProcessorReqMsg {
    // We got green light from server to process this connection on
    // Upon creation, this conn processor is <assigned> to specific GID,
    // meaning in any replies sent from device it should include provided
    // GID (used as kafka key).
    AddNewConnectionAck(i32),
    AddNewConnectionShouldClose,
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
    pub idx: i64,
    pub group_id: i32,
    pub feature_topomap_enabled: bool,
    pub device_type: CGWDeviceType,
}

impl CGWConnectionProcessor {
    pub fn new(server: Arc<CGWConnectionServer>, conn_idx: i64, addr: SocketAddr) -> Self {
        let conn_processor: CGWConnectionProcessor = CGWConnectionProcessor {
            cgw_server: server.clone(),
            serial: MacAddress::default(),
            addr,
            idx: conn_idx,
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
        let ws_stream = tokio_tungstenite::accept_async(tls_stream).await?;

        let (sink, mut stream) = ws_stream.split();

        // check if we have any pending msgs (we expect connect at this point, protocol-wise)
        // TODO: rework to ignore any WS-related frames untill we get a connect message,
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

        // we have a next() result, but it still may be undelying io error: check for it
        // break connection if we can't work with underlying ws connection (pror err etc)
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
        let evt = match cgw_ucentral_parse_connect_event(message) {
            Ok(e) => {
                debug!("Some: {:?}", e);
                e
            }
            Err(_e) => {
                error!(
                    "Failed to receive connect message from: {}! Closing connection!",
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
                    "The client MAC address {} and clinet certificate CN {} check failed!",
                    evt.serial.to_hex_string(),
                    client_cn.to_hex_string()
                );
                return Err(Error::ConnectionProcessor(
                    "Client certificate CN check failed",
                ));
            } else {
                debug!(
                    "The client MAC address {} and clinet certificate CN {} chech passed",
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
                caps.uuid = c.uuid;
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
        // cgw_connection_server has an authorative decision whether
        // we can proceed.
        debug!("Sending ACK request for device serial: {}", self.serial);
        let (mbox_tx, mut mbox_rx) = unbounded_channel::<CGWConnectionProcessorReqMsg>();
        let msg = CGWConnectionServerReqMsg::AddNewConnection(evt.serial, self.addr, caps, mbox_tx);
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
    ) -> Result<CGWConnectionState> {
        // Make sure we always track the as accurate as possible the time
        // of receiving of the event (where needed).
        let timestamp = Local::now();
        let mut kafaka_msg: String = String::new();

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
                        timestamp.timestamp(),
                    ) {
                        kafaka_msg.clone_from(&payload);
                        if let CGWUCentralEventType::State(_) = evt.evt_type {
                            if let Some(decompressed) = evt.decompressed.clone() {
                                kafaka_msg = decompressed;
                            }
                            if self.feature_topomap_enabled {
                                let topo_map = CGWUCentralTopologyMap::get_ref();

                                // TODO: remove this Arc clone:
                                // Dirty hack for now: pass Arc ref of srv to topo map;
                                // Future rework and refactoring would require to separate
                                // NB api from being an internal obj of conn_server to be a
                                // standalone (singleton?) object.
                                topo_map.enqueue_event(
                                    evt,
                                    self.device_type,
                                    self.serial,
                                    self.group_id,
                                    self.cgw_server.clone(),
                                );
                            }
                        } else if let CGWUCentralEventType::Reply(content) = evt.evt_type {
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

                            *fsm_state = CGWUCentralMessageProcessorState::Idle;
                            debug!("Got reply event for pending request id: {pending_req_id}");
                            if let Ok(resp) = cgw_construct_infra_request_result_msg(
                                pending_req_uuid,
                                pending_req_id,
                                true,
                                None,
                            ) {
                                self.cgw_server
                                    .enqueue_mbox_message_from_cgw_to_nb_api(self.group_id, resp);
                            } else {
                                error!("Failed to construct rebalance_group message!");
                            }
                        } else if let CGWUCentralEventType::RealtimeEvent(_) = evt.evt_type {
                            if self.feature_topomap_enabled {
                                let topo_map = CGWUCentralTopologyMap::get_ref();
                                // TODO: remove this Arc clone:
                                // Dirty hack for now: pass Arc ref of srv to topo map;
                                // Future rework and refactoring would require to separate
                                // NB api from being an internal obj of conn_server to be a
                                // standalone (singleton?) object.
                                topo_map.enqueue_event(
                                    evt,
                                    self.device_type,
                                    self.serial,
                                    self.group_id,
                                    self.cgw_server.clone(),
                                );
                            }
                        }
                    }

                    self.cgw_server
                        .enqueue_mbox_message_from_device_to_nb_api_c(self.group_id, kafaka_msg)?;
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
            match msg {
                CGWConnectionProcessorReqMsg::AddNewConnectionShouldClose => {
                    debug!("process_sink_mbox_rx_msg: AddNewConnectionShouldClose, processor (mac:{processor_mac}) (ACK OK)");
                    return Ok(CGWConnectionState::IsForcedToClose);
                }
                CGWConnectionProcessorReqMsg::SinkRequestToDevice(pload) => {
                    debug!("process_sink_mbox_rx_msg: SinkRequestToDevice, processor (mac: {processor_mac}) request for (mac: {}) payload: {}",
                        pload.command.serial,
                        pload.message.clone(),
                    );
                    sink.send(Message::text(pload.message)).await.ok();
                }
                CGWConnectionProcessorReqMsg::GroupIdChanged(new_group_id) => {
                    debug!(
                        "Received GroupID change message: mac {} - old gid {} : new gid {}",
                        self.serial, self.group_id, new_group_id
                    );
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
                        // Get message from queue, start measure requet processing time
                        start_time = Instant::now();

                        pending_req_id = queue_msg.command.id;
                        pending_req_type = queue_msg.command.cmd_type.clone();
                        pending_req_uuid = queue_msg.uuid;

                        let timeout = match queue_msg.timeout {
                            Some(secs) => Duration::from_secs(secs),
                            None => MESSAGE_TIMEOUT_DURATION,
                        };

                        wakeup_reason = WakeupReason::MboxRx(Some(
                            CGWConnectionProcessorReqMsg::SinkRequestToDevice(queue_msg),
                        ));

                        // Set new pending request timeout value
                        queue_lock
                            .set_device_last_req_info(&device_mac, pending_req_id, timeout)
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
                                    // 'Close' message - may be received on gracefull connection close event
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

                // check request timeout value - if request get timedout - remove it from queue
                if queue_lock
                    .device_request_tick(&device_mac, elapsed_time)
                    .await
                {
                    let queue_lock = CGW_MESSAGES_QUEUE.read().await;
                    let flushed_reqs = queue_lock.clear_device_message_queue(&device_mac).await;

                    for req in flushed_reqs {
                        if let Ok(resp) = cgw_construct_infra_request_result_msg(
                            req.uuid,
                            req.command.id,
                            false,
                            Some(format!(
                                "Reques flushed from infra queue {device_mac} due to previous request timeout!"
                            )),
                        ) {
                            // Currently Device Queue Manager does not store infars GID
                            self.cgw_server
                                .enqueue_mbox_message_from_cgw_to_nb_api(self.group_id, resp);
                        } else {
                            error!("Failed to construct  message!");
                        }
                    }

                    // reset request duration, request id and queue state
                    pending_req_id = 0;
                    queue_lock
                        .set_device_queue_state(&device_mac, CGWUCentralMessagesQueueState::RxTx)
                        .await;
                    queue_lock
                        .set_device_last_req_info(&device_mac, 0, Duration::ZERO)
                        .await;
                    fsm_state = CGWUCentralMessageProcessorState::Idle;
                    if let Ok(resp) = cgw_construct_infra_request_result_msg(
                        pending_req_uuid,
                        pending_req_id,
                        false,
                        Some(format!("Request timed out")),
                    ) {
                        self.cgw_server
                            .enqueue_mbox_message_from_cgw_to_nb_api(self.group_id, resp);
                    } else {
                        error!("Failed to construct rebalance_group message!");
                    }
                }
            }

            // Process WakeUp reason
            let rc = match wakeup_reason {
                WakeupReason::WSSRxMsg(res) => {
                    last_contact = Instant::now();
                    self.process_wss_rx_msg(res, &mut fsm_state, pending_req_id, pending_req_uuid)
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
