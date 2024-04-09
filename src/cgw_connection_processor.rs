use crate::{
    cgw_connection_server::{CGWConnectionServer, CGWConnectionServerReqMsg},
    cgw_device::CGWDeviceCapabilities,
    cgw_ucentral_parser::*,
};

use eui48::MacAddress;

use futures_util::{
    stream::{SplitSink, SplitStream},
    FutureExt, SinkExt, StreamExt,
};
use serde_json::{Map, Value};
use std::{net::SocketAddr, str::FromStr, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::mpsc::{unbounded_channel, UnboundedReceiver},
    time::{sleep, Duration, Instant},
};
use tokio_native_tls::TlsStream;
use tokio_tungstenite::{tungstenite::protocol::Message, WebSocketStream};
use tungstenite::Message::{Close, Ping, Text};

type CGWUcentralJRPCMessage = Map<String, Value>;
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

pub fn cgw_parse_jrpc_event(map: &Map<String, Value>, method: &str) -> CGWEvent {
    if method == "log" {
        let params = map.get("params").expect("Params are missing");
        let mac_serial = MacAddress::from_str(params["serial"].as_str().unwrap()).unwrap();
        return CGWEvent {
            serial: mac_serial.to_hex_string().to_uppercase(),
            evt_type: CGWEventType::Log(CGWEventLog {
                serial: mac_serial.to_hex_string().to_uppercase(),
                log: params["log"].to_string(),
                severity: serde_json::from_value(params["severity"].clone()).unwrap(),
            }),
        };
    } else if method == "connect" {
        let params = map.get("params").expect("Params are missing");
        let mac_serial = MacAddress::from_str(params["serial"].as_str().unwrap()).unwrap();
        let label = MacAddress::from_str(params["capabilities"]["label_macaddr"].as_str().unwrap())
            .unwrap();
        return CGWEvent {
            serial: mac_serial.to_hex_string().to_uppercase(),
            evt_type: CGWEventType::Connect(CGWEventConnect {
                serial: mac_serial.to_hex_string().to_uppercase(),
                firmware: params["firmware"].to_string(),
                uuid: 1,
                capabilities: CGWEventConnectParamsCaps {
                    compatible: params["capabilities"]["compatible"].to_string(),
                    model: params["capabilities"]["model"].to_string(),
                    platform: params["capabilities"]["platform"].to_string(),
                    label_macaddr: label.to_hex_string().to_uppercase(),
                },
            }),
        };
    }

    CGWEvent {
        serial: String::from(""),
        evt_type: CGWEventType::Empty,
    }
}

async fn cgw_parse_jrpc_message(message: Message) -> Result<CGWEvent, &'static str> {
    let msg = if let Ok(s) = message.into_text() {
        s
    } else {
        return Err("Message to string cast failed");
    };

    let map: CGWUcentralJRPCMessage = match serde_json::from_str(&msg) {
        Ok(m) => m,
        Err(e) => {
            error!("Failed to parse input json {e}");
            return Err("Failed to parse input json");
        }
    };

    if !map.contains_key("jsonrpc") {
        warn!("Received malformed JSONRPC msg");
        return Err("JSONRPC field is missing in message");
    }

    if map.contains_key("method") {
        if !map.contains_key("params") {
            warn!("Received JRPC <method> without params.");
            return Err("Received JRPC <method> without params");
        }

        let method = map["method"].as_str().unwrap();

        let event: CGWEvent = cgw_parse_jrpc_event(&map, method);

        match &event.evt_type {
            CGWEventType::Log(l) => {
                debug!("Received LOG evt from device {}: {}", l.serial, l.log);
            }
            CGWEventType::Connect(c) => {
                debug!(
                    "Received connect evt from device {}: type {}, fw {}",
                    c.serial, c.capabilities.platform, c.firmware
                );
            }
            _ => {
                warn!("received not yet implemented method {}", method);
                return Err("received not yet implemented method");
            }
        };

        return Ok(event);
    } else if map.contains_key("result") {
        info!("Processing <result> JSONRPC msg");
        info!("{:?}", map);
        return Err("Result handling is not yet implemented");
    }

    Err("Failed to parse event/method")
}

pub struct CGWConnectionProcessor {
    cgw_server: Arc<CGWConnectionServer>,
    pub serial: Option<String>,
    pub addr: SocketAddr,
    pub idx: i64,
}

impl CGWConnectionProcessor {
    pub fn new(
        server: Arc<CGWConnectionServer>,
        conn_idx: i64,
        addr: SocketAddr,
    ) -> Self {
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

        let evt = match cgw_parse_jrpc_message(message).await {
            Ok(e) => e,
            Err(_e) => {
                error!(
                    "failed to recv connect message from {}, closing connection",
                    self.addr
                );
                return;
            }
        };

        let mut caps: CGWDeviceCapabilities = Default::default();
        match evt.evt_type {
            CGWEventType::Connect(c) => {
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

        self.process_connection(stream, sink, mbox_rx).await;
    }

    async fn process_wss_rx_msg(
        &self,
        msg: Result<Message, tungstenite::error::Error>,
    ) -> Result<CGWConnectionState, &'static str> {
        match msg {
            Ok(msg) => match msg {
                Close(_t) => {
                    return Ok(CGWConnectionState::ClosedGracefully);
                }
                Text(payload) => {
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
                    self.process_wss_rx_msg(res).await
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
