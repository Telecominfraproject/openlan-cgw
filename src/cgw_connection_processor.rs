use crate::{
    cgw_connection_server::{CGWConnectionServer, CGWConnectionServerReqMsg},
    cgw_device::{CGWDeviceCapabilities, CGWDeviceType},
    cgw_errors::{Error, Result},
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

#[derive(Debug)]
pub enum CGWConnectionProcessorReqMsg {
    // We got green light from server to process this connection on
    AddNewConnectionAck,
    AddNewConnectionShouldClose,
    SinkRequestToDevice(CGWUCentralMessagesQueueItem),
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

pub struct CGWConnectionProcessor {
    cgw_server: Arc<CGWConnectionServer>,
    pub serial: MacAddress,
    pub addr: SocketAddr,
    pub idx: i64,
}

impl CGWConnectionProcessor {
    pub fn new(server: Arc<CGWConnectionServer>, conn_idx: i64, addr: SocketAddr) -> Self {
        let conn_processor: CGWConnectionProcessor = CGWConnectionProcessor {
            cgw_server: server,
            serial: MacAddress::default(),
            addr,
            idx: conn_idx,
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
                        error!("no connect message received from {}, closing connection", self.addr);
                        return Err(Error::ConnectionProcessor("No connect message received"));
                    }
                }
            }
            // TODO: configurable duration (upon server creation)
            _val = sleep(Duration::from_millis(30000)) => {
                error!("no message received from {}, closing connection", self.addr);
                return Err(Error::ConnectionProcessor("No message receive for too long"));
            }
        };

        // we have a next() result, but it still may be undelying io error: check for it
        // break connection if we can't work with underlying ws connection (pror err etc)
        let message = match msg {
            Ok(m) => m,
            Err(e) => {
                error!(
                    "established connection with device, but failed to receive any messages\n{e}"
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
                    "failed to recv connect message from {}, closing connection",
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
                    "The client MAC address {} and clinet certificate CN {} chech passed!",
                    evt.serial.to_hex_string(),
                    client_cn.to_hex_string()
                );
            }
        }

        debug!("Done Parse Connect Event");

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
                "Device {} is not abiding the protocol: first message - CONNECT - expected",
                evt.serial
            ),
        }

        self.serial = evt.serial;
        let device_type = CGWDeviceType::from_str(caps.platform.as_str())?;

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
            } else {
                queue_lock.create_device_messages_queue(&evt.serial).await;
            }
        }

        // TODO: we accepted tls stream and split the WS into RX TX part,
        // now we have to ASK cgw_connection_server's permission whether
        // we can proceed on with this underlying connection.
        // cgw_connection_server has an authorative decision whether
        // we can proceed.
        let (mbox_tx, mut mbox_rx) = unbounded_channel::<CGWConnectionProcessorReqMsg>();
        let msg = CGWConnectionServerReqMsg::AddNewConnection(evt.serial, caps, mbox_tx);
        self.cgw_server
            .enqueue_mbox_message_to_cgw_server(msg)
            .await;

        let ack = mbox_rx.recv().await;
        if let Some(m) = ack {
            match m {
                CGWConnectionProcessorReqMsg::AddNewConnectionAck => {
                    debug!(
                        "websocket connection established: {} {}",
                        self.addr, evt.serial
                    );
                }
                _ => {
                    return Err(Error::ConnectionProcessor(
                        "Unexpected response from server, expected ACK/NOT ACK)",
                    ));
                }
            }
        } else {
            info!("connection server declined connection, websocket connection {} {} cannot be established",
                  self.addr, evt.serial);
            return Err(Error::ConnectionProcessor("Websocker connection declined"));
        }

        self.process_connection(stream, sink, mbox_rx, device_type)
            .await;

        Ok(())
    }

    async fn process_wss_rx_msg(
        &self,
        msg: std::result::Result<Message, tungstenite::error::Error>,
        device_type: CGWDeviceType,
        fsm_state: &mut CGWUCentralMessageProcessorState,
        pending_req_id: u64,
    ) -> Result<CGWConnectionState> {
        // Make sure we always track the as accurate as possible the time
        // of receiving of the event (where needed).
        let timestamp = Local::now();

        match msg {
            Ok(msg) => match msg {
                Close(_t) => {
                    return Ok(CGWConnectionState::ClosedGracefully);
                }
                Text(payload) => {
                    if let Ok(evt) =
                        cgw_ucentral_event_parse(&device_type, &payload, timestamp.timestamp())
                    {
                        if let CGWUCentralEventType::State(_) = evt.evt_type {
                            let topo_map = CGWUCentralTopologyMap::get_ref();
                            topo_map.process_state_message(&device_type, evt).await;
                            topo_map.debug_dump_map().await;
                        } else if let CGWUCentralEventType::Reply(content) = evt.evt_type {
                            assert_eq!(*fsm_state, CGWUCentralMessageProcessorState::ResultPending);
                            assert_eq!(content.id, pending_req_id);
                            *fsm_state = CGWUCentralMessageProcessorState::Idle;
                            debug!("Got reply event for pending request id: {}", pending_req_id);
                        } else if let CGWUCentralEventType::RealtimeEvent(_) = evt.evt_type {
                            let topo_map = CGWUCentralTopologyMap::get_ref();
                            topo_map
                                .process_device_topology_event(&device_type, evt)
                                .await;
                            topo_map.debug_dump_map().await;
                        }
                    }

                    self.cgw_server
                        .enqueue_mbox_message_from_device_to_nb_api_c(self.serial, payload)?;
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
        &self,
        sink: &mut SSink,
        val: Option<CGWConnectionProcessorReqMsg>,
    ) -> Result<CGWConnectionState> {
        if let Some(msg) = val {
            let processor_mac = self.serial;
            match msg {
                CGWConnectionProcessorReqMsg::AddNewConnectionShouldClose => {
                    debug!("MBOX_IN: AddNewConnectionShouldClose, processor (mac:{processor_mac}) (ACK OK)");
                    return Ok(CGWConnectionState::IsForcedToClose);
                }
                CGWConnectionProcessorReqMsg::SinkRequestToDevice(pload) => {
                    debug!("MBOX_IN: SinkRequestToDevice, processor (mac:{processor_mac}) req for (mac:{}) payload:{}",
                        pload.command.serial,
                        pload.message.clone(),
                    );
                    sink.send(Message::text(pload.message)).await.ok();
                }
                _ => {
                    return Err(Error::ConnectionProcessor(
                        "Sink MBOX: received unexpected message",
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
        self,
        mut stream: SStream,
        mut sink: SSink,
        mut mbox_rx: UnboundedReceiver<CGWConnectionProcessorReqMsg>,
        device_type: CGWDeviceType,
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
                        wakeup_reason = WakeupReason::MboxRx(Some(
                            CGWConnectionProcessorReqMsg::SinkRequestToDevice(queue_msg),
                        ));

                        // Set new pending request timeout value
                        queue_lock
                            .set_device_last_req_info(
                                &device_mac,
                                pending_req_id,
                                MESSAGE_TIMEOUT_DURATION,
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
            // Do message queue timeout tick and cleanup queue dut to timeout\
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
                    queue_lock.clear_device_message_queue(&device_mac).await;

                    // reset request duration, request id and queue state
                    pending_req_id = 0;
                    queue_lock
                        .set_device_queue_state(&device_mac, CGWUCentralMessagesQueueState::RxTx)
                        .await;
                    queue_lock
                        .set_device_last_req_info(&device_mac, 0, Duration::ZERO)
                        .await;
                    fsm_state = CGWUCentralMessageProcessorState::Idle;
                }
            }

            // Process WakeUp reason
            let rc = match wakeup_reason {
                WakeupReason::WSSRxMsg(res) => {
                    last_contact = Instant::now();
                    self.process_wss_rx_msg(res, device_type, &mut fsm_state, pending_req_id)
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
                            "Remote client {} closed connection gracefully",
                            self.serial.to_hex_string()
                        );
                        return self.send_connection_close_event().await;
                    } else if let CGWConnectionState::IsStale = state {
                        warn!(
                            "Remote client {} closed due to inactivity",
                            self.serial.to_hex_string()
                        );
                        return self.send_connection_close_event().await;
                    } else if let CGWConnectionState::IsDead = state {
                        warn!(
                            "Remote client {} connection is dead",
                            self.serial.to_hex_string()
                        );
                        return self.send_connection_close_event().await;
                    }
                }
                Err(e) => {
                    warn!("{:?}", e);
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
            "MBOX_OUT: ConnectionClosed, processor (mac:{})",
            self.serial.to_hex_string()
        );
    }
}
