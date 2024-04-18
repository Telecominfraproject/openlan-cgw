use crate::{
    cgw_connection_server::{CGWConnectionServer, CGWConnectionServerReqMsg},
    cgw_device::{CGWDeviceCapabilities, CGWDeviceType},
    cgw_ucentral_parser::{cgw_ucentral_parse_connect_event, CGWUCentralEventType},
};

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
use tokio_native_tls::TlsStream;
use tokio_tungstenite::{tungstenite::protocol::Message, WebSocketStream};
use tungstenite::Message::{Close, Ping, Text};

type SStream = SplitStream<WebSocketStream<TlsStream<TcpStream>>>;
type SSink = SplitSink<WebSocketStream<TlsStream<TcpStream>>, Message>;

#[derive(Debug)]
pub enum CGWConnectionProcessorReqMsg {
    // We got green light from server to process this connection on
    AddNewConnectionAck,
    AddNewConnectionShouldClose,
    SinkRequestToDevice(String, String),
}

#[derive(Debug)]
enum CGWConnectionState {
    IsActive,
    IsForcedToClose,
    IsDead,
    IsStale,
    ClosedGracefully,
}

pub struct CGWConnectionProcessor {
    cgw_server: Arc<CGWConnectionServer>,
    pub serial: Option<String>,
    pub addr: SocketAddr,
    pub idx: i64,
}

impl CGWConnectionProcessor {
    pub fn new(server: Arc<CGWConnectionServer>, conn_idx: i64, addr: SocketAddr) -> Self {
        let conn_processor: CGWConnectionProcessor = CGWConnectionProcessor {
            cgw_server: server,
            serial: None,
            addr: addr,
            idx: conn_idx,
        };

        conn_processor
    }

    pub async fn start(mut self, tls_stream: TlsStream<TcpStream>) {
        let ws_stream = tokio_tungstenite::accept_async(tls_stream)
            .await
            .expect("error during the websocket handshake occurred");

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
                        return;
                    }
                }
            }
            // TODO: configurable duration (upon server creation)
            _val = sleep(Duration::from_millis(30000)) => {
                error!("no message received from {}, closing connection", self.addr);
                return;
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
                return;
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
                return;
            }
        };

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

        self.serial = Some(evt.serial.clone());
        let device_type = CGWDeviceType::from_str(caps.platform.as_str()).unwrap();

        // TODO: we accepted tls stream and split the WS into RX TX part,
        // now we have to ASK cgw_connection_server's permission whether
        // we can proceed on with this underlying connection.
        // cgw_connection_server has an authorative decision whether
        // we can proceed.
        let (mbox_tx, mut mbox_rx) = unbounded_channel::<CGWConnectionProcessorReqMsg>();
        let msg = CGWConnectionServerReqMsg::AddNewConnection(evt.serial.clone(), caps, mbox_tx);
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
                _ => panic!("Unexpected response from server, expected ACK/NOT ACK)"),
            }
        } else {
            info!("connection server declined connection, websocket connection {} {} cannot be established",
                  self.addr, evt.serial);
            return;
        }

        self.process_connection(stream, sink, mbox_rx, device_type)
            .await;
    }

    async fn process_wss_rx_msg(
        &self,
        msg: Result<Message, tungstenite::error::Error>,
        device_type: CGWDeviceType,
    ) -> Result<CGWConnectionState, &'static str> {
        match msg {
            Ok(msg) => match msg {
                Close(_t) => {
                    return Ok(CGWConnectionState::ClosedGracefully);
                }
                Text(payload) => {
                    // let _ = cgw_ucentral_event_parser(parser, Message::from(payload.clone()));
                    self.cgw_server
                        .enqueue_mbox_message_from_device_to_nb_api_c(
                            self.serial.clone().unwrap(),
                            payload,
                        );
                    return Ok(CGWConnectionState::IsActive);
                }
                Ping(_t) => {
                    return Ok(CGWConnectionState::IsActive);
                }
                _ => {}
            },
            Err(e) => match e {
                tungstenite::error::Error::AlreadyClosed => {
                    return Err("Underlying connection's been closed");
                }
                _ => {}
            },
        }

        Ok(CGWConnectionState::IsActive)
    }

    async fn process_sink_mbox_rx_msg(
        &self,
        sink: &mut SSink,
        val: Option<CGWConnectionProcessorReqMsg>,
    ) -> Result<CGWConnectionState, &str> {
        if let Some(msg) = val {
            let processor_mac = self.serial.clone().unwrap();
            match msg {
                CGWConnectionProcessorReqMsg::AddNewConnectionShouldClose => {
                    debug!("MBOX_IN: AddNewConnectionShouldClose, processor (mac:{processor_mac}) (ACK OK)");
                    return Ok(CGWConnectionState::IsForcedToClose);
                }
                CGWConnectionProcessorReqMsg::SinkRequestToDevice(mac, pload) => {
                    debug!("MBOX_IN: SinkRequestToDevice, processor (mac:{processor_mac}) req for (mac:{mac}) payload:{pload}");
                    sink.send(Message::text(pload)).await.ok();
                }
                _ => panic!("Unexpected message received {:?}", msg),
            }
        }
        Ok(CGWConnectionState::IsActive)
    }

    async fn process_stale_connection_msg(
        &self,
        last_contact: Instant,
    ) -> Result<CGWConnectionState, &str> {
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
            WSSRxMsg(Result<Message, tungstenite::error::Error>),
            MboxRx(Option<CGWConnectionProcessorReqMsg>),
            Stale,
        }

        let mut last_contact = Instant::now();
        let mut poll_wss_first = true;

        // Get underlying wakeup reason and do initial parsion, like:
        // - check if WSS stream has a message or an (recv) error
        // - check if sinkmbox has a message or an (recv) error
        // - check if connection's been stale for X time
        //
        // TODO: try_next intead of sync .next? could potentially
        // skyrocket CPU usage.
        loop {
            let mut wakeup_reason: WakeupReason = WakeupReason::Unspecified;

            // TODO: refactor
            // Round-robin selection of stream to process:
            // first, get single message from WSS, then get a single msg from RX MBOX
            // It's done to ensure we process WSS and RX MBOX equally with same priority
            // Also, we have to make sure we don't sleep-wait for any of the streams to
            // make sure we don't cancel futures that are used for stream processing,
            // especially TCP stream, which is not cancel-safe
            if poll_wss_first {
                if let Some(val) = stream.next().now_or_never() {
                    if let Some(res) = val {
                        if let Ok(msg) = res {
                            wakeup_reason = WakeupReason::WSSRxMsg(Ok(msg));
                        } else if let Err(msg) = res {
                            wakeup_reason = WakeupReason::WSSRxMsg(Result::Err(msg));
                        }
                    } else if let None = val {
                        wakeup_reason = WakeupReason::WSSRxMsg(Result::Err(
                            tungstenite::error::Error::AlreadyClosed,
                        ));
                    }
                } else if let Some(val) = mbox_rx.recv().now_or_never() {
                    wakeup_reason = WakeupReason::MboxRx(val)
                }

                poll_wss_first = !poll_wss_first;
            } else {
                if let Some(val) = mbox_rx.recv().now_or_never() {
                    wakeup_reason = WakeupReason::MboxRx(val)
                } else if let Some(val) = stream.next().now_or_never() {
                    if let Some(res) = val {
                        if let Ok(msg) = res {
                            wakeup_reason = WakeupReason::WSSRxMsg(Ok(msg));
                        } else if let Err(msg) = res {
                            wakeup_reason = WakeupReason::WSSRxMsg(Result::Err(msg));
                        }
                    } else if let None = val {
                        wakeup_reason = WakeupReason::WSSRxMsg(Result::Err(
                            tungstenite::error::Error::AlreadyClosed,
                        ));
                    }
                }
                poll_wss_first = !poll_wss_first;
            }

            // TODO: somehow workaround the sleeping?
            // Both WSS and RX MBOX are empty: chill for a while
            if let WakeupReason::Unspecified = wakeup_reason {
                sleep(Duration::from_millis(1000)).await;
                wakeup_reason = WakeupReason::Stale;
            }

            let rc = match wakeup_reason {
                WakeupReason::WSSRxMsg(res) => {
                    last_contact = Instant::now();
                    self.process_wss_rx_msg(res, device_type).await
                }
                WakeupReason::MboxRx(mbox_message) => {
                    self.process_sink_mbox_rx_msg(&mut sink, mbox_message).await
                }
                WakeupReason::Stale => {
                    self.process_stale_connection_msg(last_contact.clone())
                        .await
                }
                _ => {
                    panic!("Failed to get wakeup reason for {} conn", self.addr);
                }
            };

            match rc {
                Err(e) => {
                    warn!("{}", e);
                    break;
                }
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
                            self.serial.clone().unwrap()
                        );
                        break;
                    } else if let CGWConnectionState::IsStale = state {
                        warn!(
                            "Remote client {} closed due to inactivity",
                            self.serial.clone().unwrap()
                        );
                        break;
                    }
                }
            }
        }

        let mac = self.serial.clone().unwrap();
        let msg = CGWConnectionServerReqMsg::ConnectionClosed(self.serial.unwrap());
        self.cgw_server
            .enqueue_mbox_message_to_cgw_server(msg)
            .await;
        debug!("MBOX_OUT: ConnectionClosed, processor (mac:{})", mac);
    }
}
