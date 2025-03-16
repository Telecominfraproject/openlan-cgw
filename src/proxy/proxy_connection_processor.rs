use cgw_common::{
    cgw_errors::{Error, Result},
    cgw_ucentral_parser::cgw_ucentral_parse_connect_event,
};

use eui48::MacAddress;
use futures_util::{
    stream::{SplitSink, SplitStream},
    FutureExt, SinkExt, StreamExt,
};

use std::{net::SocketAddr, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::mpsc::{unbounded_channel, UnboundedReceiver},
    time::{sleep, Duration, Instant},
};
use tokio_rustls::server::TlsStream;
use tokio_tungstenite::{tungstenite::protocol::Message, WebSocketStream};
use tungstenite::Message::Close;

type SStream = SplitStream<WebSocketStream<TlsStream<TcpStream>>>;
type SSink = SplitSink<WebSocketStream<TlsStream<TcpStream>>, Message>;

type MaybeTlsStream = tokio_tungstenite::MaybeTlsStream<TcpStream>;
type CGWStream = WebSocketStream<MaybeTlsStream>;
type CGWSink = SplitSink<CGWStream, Message>;
type CGWSource = SplitStream<CGWStream>;

use crate::proxy_connection_server::{
    ProxyConnectionServer,
    ProxyConnectionServerReqMsg,
};

#[derive(Debug, Clone)]
pub enum ProxyConnectionProcessorReqMsg {
    AddNewConnectionAck,
    AddNewConnectionShouldClose,
    SetPeer(SocketAddr),
}

#[derive(Debug)]
enum ProxyConnectionState {
    IsActive,
    IsForcedToClose,
    IsDead,
    #[allow(dead_code)]
    IsStale,
    ClosedGracefully,
}

#[derive(Debug, PartialEq)]
enum ProxyUCentralMessageProcessorState {
    Idle,
    ResultPending,
}

impl std::fmt::Display for ProxyUCentralMessageProcessorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProxyUCentralMessageProcessorState::Idle => write!(f, "Idle"),
            ProxyUCentralMessageProcessorState::ResultPending => write!(f, "ResultPending"),
        }
    }
}

pub struct ProxyConnectionProcessor {
    proxy_server: Arc<ProxyConnectionServer>,
    pub serial: MacAddress,
    pub addr: SocketAddr,
    peer_addr: Option<SocketAddr>
}

impl ProxyConnectionProcessor {
    pub fn new(server: Arc<ProxyConnectionServer>, addr: SocketAddr) -> Self {
        let conn_processor: ProxyConnectionProcessor = ProxyConnectionProcessor {
            proxy_server: server.clone(),
            serial: MacAddress::default(),
            addr,
            peer_addr: None,
        };

        conn_processor
    }

    pub async fn start(
        mut self,
        tls_stream: TlsStream<TcpStream>,
        _client_cn: MacAddress,
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

        debug!(
            "Parse Connect Event done! Device serial: {}",
            evt.serial.to_hex_string()
        );

        self.serial = evt.serial;

        // TODO: we accepted tls stream and split the WS into RX TX part,
        // now we have to ASK proxy_connection_server's permission whether
        // we can proceed on with this underlying connection.
        // proxy_connection_server has an authoritative decision whether
        // we can proceed.
        debug!("Sending ACK request for device serial: {}", self.serial);
        let (mbox_tx, mut mbox_rx) = unbounded_channel::<ProxyConnectionProcessorReqMsg>();
        let msg = ProxyConnectionServerReqMsg::AddNewConnection(
            evt.serial,
            self.addr,
            mbox_tx,
        );
        self.proxy_server
            .enqueue_mbox_message_to_proxy_server(msg)
            .await;

        let ack = mbox_rx.recv().await;
        debug!("Got ACK response for device serial: {}", self.serial);
        if let Some(m) = ack {
            match m {
                ProxyConnectionProcessorReqMsg::AddNewConnectionAck => {
                    debug!(
                        "WebSocket connection established! Address: {}, serial: {}",
                        self.addr, evt.serial
                    );
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

        self.process_connection(stream, sink, mbox_rx, message).await;

        Ok(())
    }

    async fn process_connection(
        mut self,
        mut stream: SStream,
        mut sink: SSink,
        mut mbox_rx: UnboundedReceiver<ProxyConnectionProcessorReqMsg>,
        original_connect_message: Message
    ) {
        #[derive(Debug)]
        enum WakeupReason {
            Unspecified,
            ClientWSSMessage(std::result::Result<Message, tungstenite::error::Error>),
            CGWWSSMessage(std::result::Result<Message, tungstenite::error::Error>),
            MboxRx(Option<ProxyConnectionProcessorReqMsg>),
            StaleConnection,
            BrokenClientConnection,
            BrokenCGWConnection,
        }

        let mut last_contact = Instant::now();

        // References to the CGW connection that can be updated
        let mut cgw_sink: Option<CGWSink> = None;
        let mut cgw_stream: Option<CGWSource> = None;

        loop {
            let mut wakeup_reason: WakeupReason = WakeupReason::Unspecified;

            // Check for messages from the proxy server (mbox)
            if let Some(val) = mbox_rx.recv().now_or_never() {
                wakeup_reason = WakeupReason::MboxRx(val);
            } else {
                // Check for messages from the client
                match stream.next().now_or_never() {
                    Some(val) => {
                        match val {
                            Some(res) => wakeup_reason = WakeupReason::ClientWSSMessage(res),
                            None => wakeup_reason = WakeupReason::BrokenClientConnection,
                        }
                    },
                    None => {
                        // Check for messages from CGW if connection exists
                        if let Some(ref mut cgw_stream_inner) = cgw_stream {
                            match cgw_stream_inner.next().now_or_never() {
                                Some(val) => {
                                    match val {
                                        Some(res) => wakeup_reason = WakeupReason::CGWWSSMessage(res),
                                        None => wakeup_reason = WakeupReason::BrokenCGWConnection,
                                    }
                                },
                                None => {
                                    // For now consider a connection stale after 5 minutes of inactivity
                                    if Instant::now().duration_since(last_contact) > Duration::from_secs(300) {
                                        wakeup_reason = WakeupReason::StaleConnection;
                                    } else {
                                        // Sleep briefly to avoid busy waiting
                                        sleep(Duration::from_millis(100)).await;
                                    }
                                }
                            }
                        } else {
                            // No CGW connection yet, sleep briefly
                            sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }

            // Handle the wakeup reason
            match wakeup_reason {
                WakeupReason::ClientWSSMessage(res) => {
                    last_contact = Instant::now();
                    match res {
                        Ok(msg) => {
                            match msg {
                                Close(_) => {
                                    debug!("client {} requested graceful close", self.serial);
                                    // Try to close CGW connection gracefully if it exists
                                    if let Some(ref mut cgw_sink_inner) = cgw_sink {
                                        cgw_sink_inner.send(Message::Close(None)).await.ok();
                                    }
                                    break;
                                },
                                _ => {
                                    // Forward message to CGW if connection exists
                                    if let Some(ref mut cgw_sink_inner) = cgw_sink {
                                        debug!("Forwarding message from client {} to CGW", self.serial);
                                        if let Err(e) = cgw_sink_inner.send(msg).await {
                                            error!("Failed to forward client message to CGW: {}", e);

                                            // Attempt to reconnect if connection failed
                                            if let Some(addr) = self.peer_addr {
                                                debug!("Attempting to reconnect to CGW at {}", addr);
                                                if let Err(e) = self.handle_set_peer(addr, &mut cgw_sink, &mut cgw_stream, original_connect_message.clone()).await {
                                                    error!("Failed to reconnect to CGW: {}", e);
                                                    break;
                                                }
                                            } else {
                                                break;
                                            }
                                        }
                                    } else {
                                        debug!("Received client message but no CGW connection exists for {}", self.serial);
                                        // Buffer message or drop it as appropriate
                                    }
                                }
                            }
                        },
                        Err(e) => {
                            error!("Error receiving message from client {}: {}", self.serial, e);
                            break;
                        }
                    }
                },
                WakeupReason::CGWWSSMessage(res) => {
                    last_contact = Instant::now();
                    match res {
                        Ok(msg) => {
                            match msg {
                                Close(_) => {
                                    debug!("CGW requested graceful close for client {}", self.serial);
                                    // Try to close client connection gracefully
                                    sink.send(Message::Close(None)).await.ok();
                                    break;
                                },
                                _ => {
                                    // Forward message to client
                                    debug!("Forwarding message from CGW to client {}", self.serial);
                                    if let Err(e) = sink.send(msg).await {
                                        error!("Failed to forward CGW message to client {}: {}", self.serial, e);
                                        break;
                                    }
                                }
                            }
                        },
                        Err(e) => {
                            error!("Error receiving message from CGW for client {}: {}", self.serial, e);

                            // Attempt to reconnect if needed
                            if let Some(addr) = self.peer_addr {
                                debug!("Connection error, attempting to reconnect to CGW at {}", addr);
                                if let Err(e) = self.handle_set_peer(addr, &mut cgw_sink, &mut cgw_stream, original_connect_message.clone()).await {
                                    error!("Failed to reconnect to CGW: {}", e);
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                    }
                },
                WakeupReason::MboxRx(msg) => {
                    if let Some(server_msg) = msg {
                        match server_msg {
                            ProxyConnectionProcessorReqMsg::AddNewConnectionShouldClose => {
                                debug!("Proxy server requested to close connection for client {}", self.serial);
                                break;
                            },
                            ProxyConnectionProcessorReqMsg::SetPeer(peer_addr) => {
                                debug!("Received peer address update for client {}: {}", self.serial, peer_addr);

                                // peer_addr is already stored in the handle_set_peer method
                                // Establish connection to the new peer
                                match self.handle_set_peer(peer_addr, &mut cgw_sink, &mut cgw_stream, original_connect_message.clone()).await {
                                    Ok(_) => {
                                        debug!("Successfully established connection to peer {} for client {}",
                                               peer_addr, self.serial);
                                    },
                                    Err(e) => {
                                        error!("Failed to establish connection to peer {} for client {}: {}",
                                               peer_addr, self.serial, e);
                                    }
                                }
                            },
                            _ => {
                                debug!("Received unhandled server message for client {}: {:?}", self.serial, server_msg);
                            }
                        }
                    } else {
                        // mbox_rx channel closed - proxy server is shutting down
                        debug!("Proxy server channel closed for client {}", self.serial);
                        break;
                    }
                },
                WakeupReason::StaleConnection => {
                    warn!("Connection for client {} is stale, closing after {} seconds of inactivity",
                         self.serial, Instant::now().duration_since(last_contact).as_secs());
                    break;
                },
                WakeupReason::BrokenClientConnection => {
                    warn!("client {} connection broken", self.serial);
                    // Try to close CGW connection gracefully if it exists
                    if let Some(ref mut cgw_sink_inner) = cgw_sink {
                        cgw_sink_inner.send(Message::Close(None)).await.ok();
                    }
                    break;
                },
                WakeupReason::BrokenCGWConnection => {
                    warn!("CGW connection broken for client {}", self.serial);

                    // Attempt to reconnect if needed
                    if let Some(addr) = self.peer_addr {
                        debug!("Connection broken, attempting to reconnect to CGW at {}", addr);
                        if let Err(e) = self.handle_set_peer(addr, &mut cgw_sink, &mut cgw_stream, original_connect_message.clone()).await {
                            error!("Failed to reconnect to CGW: {}", e);
                            // Try to close client connection gracefully and exit
                            sink.send(Message::Close(None)).await.ok();
                            break;
                        }
                    } else {
                        // No peer address stored, can't reconnect
                        sink.send(Message::Close(None)).await.ok();
                        break;
                    }
                },
                WakeupReason::Unspecified => {
                    // No action needed, just loop again
                }
            }
        }

        // Connection is being closed, notify the proxy server
        debug!("Proxy connection for client {} is ending", self.serial);
        self.send_connection_close_event().await;
    }

    async fn handle_set_peer(&mut self,
        peer_addr: SocketAddr,
        cgw_sink: &mut Option<CGWSink>,
        cgw_stream: &mut Option<CGWSource>,
        original_connect_message: Message) -> Result<bool>
    {
        // Update the stored peer address
        self.peer_addr = Some(peer_addr);

        // If we already have an active connection, close it first
        if cgw_sink.is_some() && cgw_stream.is_some() {
            debug!("Closing existing CGW connection for client {} before establishing new one", self.serial);
            if let Some(sink) = cgw_sink {
                // Try to close the existing connection gracefully
                sink.send(Message::Close(None)).await.ok();
            }
            *cgw_sink = None;
            *cgw_stream = None;
        }

        // Establish a new connection to CGW
        let cgw_url = format!("ws://{}", peer_addr);
        debug!("Attempting to connect to CGW at: {}", cgw_url);

        let cgw_connection = tokio_tungstenite::connect_async(cgw_url).await;

        let (ws_stream, _response) = match cgw_connection {
            Ok((ws_stream, resp)) => {
                debug!("WebSocket handshake with CGW completed with status: {}", resp.status());
                (ws_stream, resp)
            },
            Err(e) => {
                error!("Failed to establish WebSocket connection with CGW at {}: {}", peer_addr, e);
                return Err(Error::ConnectionProcessor("Failed to connect to CGW"));
            }
        };

        let (sink, stream) = ws_stream.split();

        // Send initial proxy connect message to CGW
        let proxy_message = format!(
            "{{\"type\": \"proxy_connect\", \"peer_address\": \"{}\", \"cert_validated\": true, \"serial\": \"{}\"}}",
            self.addr,
            self.serial.to_hex_string()
        );

        let mut new_sink = sink;
        if let Err(e) = new_sink.send(Message::Text(proxy_message)).await {
            error!("Failed to send proxy info to CGW: {}", e);
            return Err(Error::ConnectionProcessor("Failed to send proxy info"));
        }

        if let Err(e) = new_sink.send(original_connect_message).await {
            error!("Failed to send original connect message to CGW: {}", e);
            return Err(Error::ConnectionProcessor("Failed to send original connect message"));
        }

        // Update the connection references
        *cgw_sink = Some(new_sink);
        *cgw_stream = Some(stream);

        info!("Proxy connection established for client {} to CGW at {}", self.serial, peer_addr);
        Ok(true)
    }

    async fn send_connection_close_event(&self) {
        let msg = ProxyConnectionServerReqMsg::ConnectionClosed(self.serial);
        self.proxy_server
            .enqueue_mbox_message_to_proxy_server(msg)
            .await;
        debug!(
            "MBOX_OUT: ConnectionClosed, processor (mac: {})",
            self.serial.to_hex_string()
        );
    }
}