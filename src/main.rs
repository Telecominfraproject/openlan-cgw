#![warn(rust_2018_idioms)]
mod cgw_app_args;
mod cgw_connection_processor;
mod cgw_connection_server;
mod cgw_db_accessor;
mod cgw_device;
mod cgw_devices_cache;
mod cgw_errors;
mod cgw_metrics;
mod cgw_nb_api_listener;
mod cgw_remote_client;
mod cgw_remote_discovery;
mod cgw_remote_server;
mod cgw_runtime;
mod cgw_tls;
mod cgw_ucentral_ap_parser;
mod cgw_ucentral_messages_queue_manager;
mod cgw_ucentral_parser;
mod cgw_ucentral_switch_parser;
mod cgw_ucentral_topology_map;

#[macro_use]
extern crate log;

#[macro_use]
extern crate lazy_static;

use cgw_app_args::AppArgs;
use cgw_runtime::cgw_initialize_runtimes;

use nix::sys::socket::{setsockopt, sockopt};
use tokio::{
    net::TcpListener,
    runtime::{Builder, Handle, Runtime},
    signal,
    sync::Notify,
    time::{sleep, Duration},
};

use std::{env, net::SocketAddr, str::FromStr, sync::Arc};

use rlimit::{setrlimit, Resource};

use cgw_connection_server::CGWConnectionServer;

use cgw_remote_server::CGWRemoteServer;

use cgw_metrics::CGWMetrics;

use cgw_tls::cgw_tls_create_acceptor;

use crate::cgw_errors::{Error, Result};

use tokio::net::TcpStream;

use std::os::unix::io::AsFd;

const CGW_TCP_KEEPALIVE_TIMEOUT: u32 = 30;
const CGW_TCP_KEEPALIVE_COUNT: u32 = 3;
const CGW_TCP_KEEPALIVE_INTERVAL: u32 = 10;

#[derive(Copy, Clone)]
enum AppCoreLogLevel {
    /// Print debug-level messages and above
    Debug,
    /// Print info-level messages and above
    Info,
}

impl FromStr for AppCoreLogLevel {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "debug" => Ok(AppCoreLogLevel::Debug),
            "info" => Ok(AppCoreLogLevel::Info),
            _ => Err(()),
        }
    }
}

pub struct AppCore {
    cgw_server: Arc<CGWConnectionServer>,
    main_runtime_handle: Arc<Handle>,
    grpc_server_runtime_handle: Arc<Runtime>,
    conn_ack_runtime_handle: Arc<Runtime>,
    args: AppArgs,
}

impl AppCore {
    async fn new(app_args: AppArgs) -> Result<Self> {
        Self::setup_app()?;
        let current_runtime = Arc::new(Handle::current());

        let stack_size: usize = 1024 * 1024;
        let c_ack_runtime_handle = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("cgw-c-ack")
                .thread_stack_size(stack_size)
                .enable_all()
                .build()?,
        );
        let rpc_runtime_handle = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("grpc-recv-t")
                .thread_stack_size(stack_size)
                .enable_all()
                .build()?,
        );

        let cgw_server = match CGWConnectionServer::new(&app_args).await {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to create CGW server! Error: {e}");
                return Err(e);
            }
        };

        Ok(AppCore {
            cgw_server,
            main_runtime_handle: current_runtime,
            conn_ack_runtime_handle: c_ack_runtime_handle,
            args: app_args,
            grpc_server_runtime_handle: rpc_runtime_handle,
        })
    }

    fn setup_app() -> Result<()> {
        let nofile_rlimit = Resource::NOFILE.get()?;
        info!("{:?}", nofile_rlimit);
        let nofile_hard_limit = nofile_rlimit.1;
        setrlimit(Resource::NOFILE, nofile_hard_limit, nofile_hard_limit)?;
        let nofile_rlimit = Resource::NOFILE.get()?;
        info!("{:?}", nofile_rlimit);

        Ok(())
    }

    async fn run(self: Arc<AppCore>, notifier: Arc<Notify>) {
        let main_runtime_handle: Arc<Handle> = self.main_runtime_handle.clone();
        let core_clone = self.clone();

        let cgw_remote_server = CGWRemoteServer::new(self.args.cgw_id, &self.args.grpc_args);
        let cgw_srv_clone = self.cgw_server.clone();
        let cgw_con_server = self.cgw_server.clone();
        self.grpc_server_runtime_handle.spawn(async move {
            debug!("cgw_remote_server.start entry");
            cgw_remote_server.start(cgw_srv_clone).await;
            debug!("cgw_remote_server.start exit");
        });

        main_runtime_handle.spawn(async move { server_loop(core_clone).await });

        loop {
            tokio::select! {
                // Cleanup if notified of received SIGHUP, SIGINT or SIGTERM
                _ = notifier.notified() => {
                    cgw_con_server.cleanup_redis().await;
                    break;
                },
                _ = async {
                    sleep(Duration::from_millis(5000)).await;
                } => {},
            }
        }
    }
}

async fn cgw_set_tcp_keepalive_options(stream: TcpStream) -> Result<TcpStream> {
    // Convert Tokio's TcpStream to std::net::TcpStream
    let std_stream = match stream.into_std() {
        Ok(stream) => stream,
        Err(e) => {
            error!("Failed to convert Tokio TcpStream into Std TcpStream");
            return Err(Error::Tcp(format!(
                "Failed to convert Tokio TcpStream into Std TcpStream: {}",
                e
            )));
        }
    };

    // Get the raw file descriptor (socket)
    let raw_fd = std_stream.as_fd();

    // Set the socket option to enable TCP keepalive
    if let Err(e) = setsockopt(&raw_fd, sockopt::KeepAlive, &true) {
        error!("Failed to enable TCP keepalive: {}", e);
        return Err(Error::Tcp("Failed to enable TCP keepalive".to_string()));
    }

    // Set the TCP_KEEPIDLE option (keepalive time)
    if let Err(e) = setsockopt(&raw_fd, sockopt::TcpKeepIdle, &CGW_TCP_KEEPALIVE_TIMEOUT) {
        error!("Failed to set TCP_KEEPIDLE: {}", e);
        return Err(Error::Tcp("Failed to set TCP_KEEPIDLE".to_string()));
    }

    // Set the TCP_KEEPINTVL option (keepalive interval)
    if let Err(e) = setsockopt(&raw_fd, sockopt::TcpKeepCount, &CGW_TCP_KEEPALIVE_COUNT) {
        error!("Failed to set TCP_KEEPINTVL: {}", e);
        return Err(Error::Tcp("Failed to set TCP_KEEPINTVL".to_string()));
    }

    // Set the TCP_KEEPCNT option (keepalive probes count)
    if let Err(e) = setsockopt(
        &raw_fd,
        sockopt::TcpKeepInterval,
        &CGW_TCP_KEEPALIVE_INTERVAL,
    ) {
        error!("Failed to set TCP_KEEPCNT: {}", e);
        return Err(Error::Tcp("Failed to set TCP_KEEPCNT".to_string()));
    }

    // Convert the std::net::TcpStream back to Tokio's TcpStream
    let stream = match TcpStream::from_std(std_stream) {
        Ok(stream) => stream,
        Err(e) => {
            error!("Failed to convert Std TcpStream into Tokio TcpStream");
            return Err(Error::Tcp(format!(
                "Failed to convert Std TcpStream into Tokio TcpStream: {}",
                e
            )));
        }
    };

    Ok(stream)
}

async fn server_loop(app_core: Arc<AppCore>) -> Result<()> {
    debug!("server_loop entry");

    debug!(
        "Starting WSS server, listening at {}:{}",
        app_core.args.wss_args.wss_ip, app_core.args.wss_args.wss_port
    );
    // Bind the server's socket
    let sockaddress = SocketAddr::new(
        std::net::IpAddr::V4(app_core.args.wss_args.wss_ip),
        app_core.args.wss_args.wss_port,
    );
    let listener: Arc<TcpListener> = match TcpListener::bind(sockaddress).await {
        Ok(listener) => Arc::new(listener),
        Err(e) => {
            error!("Failed to bind socket address {sockaddress}! Error: {e}");
            return Err(Error::ConnectionServer(format!(
                "Failed to bind socket address {sockaddress}! Error: {e}"
            )));
        }
    };

    let tls_acceptor = match cgw_tls_create_acceptor(&app_core.args.wss_args).await {
        Ok(acceptor) => acceptor,
        Err(e) => {
            error!("Failed to create TLS acceptor! Error: {e}");
            return Err(e);
        }
    };

    // Spawn explicitly in main thread: created task accepts connection,
    // but handling is spawned inside another threadpool runtime
    let app_core_clone = app_core.clone();
    let result = app_core
        .main_runtime_handle
        .spawn(async move {
            let mut conn_idx: i64 = 0;
            loop {
                let app_core_clone = app_core_clone.clone();
                let cgw_server_clone = app_core_clone.cgw_server.clone();
                let tls_acceptor_clone = tls_acceptor.clone();

                // Asynchronously wait for an inbound socket.
                let (socket, remote_addr) = match listener.accept().await {
                    Ok((sock, addr)) => (sock, addr),
                    Err(e) => {
                        error!("Failed to accept connection! Error: {e}");
                        continue;
                    }
                };

                let socket = match cgw_set_tcp_keepalive_options(socket).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!(
                            "Failed to set TCP keepalive options. Error: {}",
                            e.to_string()
                        );
                        break;
                    }
                };

                info!("Accept (ACK) connection: {conn_idx}, remote address: {remote_addr}");

                app_core_clone.conn_ack_runtime_handle.spawn(async move {
                    cgw_server_clone
                        .ack_connection(socket, tls_acceptor_clone, remote_addr)
                        .await;
                });

                conn_idx += 1;
            }
        })
        .await;

    match result {
        Ok(_) => info!("Application finished successfully!"),
        Err(e) => {
            error!("Application failed! Error: {e}");
        }
    }

    Ok(())
}

fn setup_logger(log_level: AppCoreLogLevel) {
    match log_level {
        AppCoreLogLevel::Debug => ::std::env::set_var("RUST_LOG", "ucentral_cgw=debug"),
        AppCoreLogLevel::Info => ::std::env::set_var("RUST_LOG", "ucentral_cgw=info"),
    }
    env_logger::init();
}

async fn signal_handler(shutdown_notify: Arc<Notify>) -> Result<()> {
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
    let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())?;
    let mut sighup = signal::unix::signal(signal::unix::SignalKind::hangup())?;

    tokio::select! {
        _ = sigterm.recv() => {
            info!("Received SIGTERM");
        },
        _ = sigint.recv() => {
            info!("Received SIGINT");
        },
        _ = sighup.recv() => {
            info!("Received SIGHUP");
        },
    }

    // Notify the main task to shutdown
    shutdown_notify.notify_one();
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = match AppArgs::parse() {
        Ok(app_args) => app_args,
        Err(e) => {
            setup_logger(AppCoreLogLevel::Info);
            error!("Failed to parse application args! Error: {e}");
            return Err(e);
        }
    };

    // Configure logger
    setup_logger(args.log_level);

    // Initialize runtimes
    if let Err(e) = cgw_initialize_runtimes(args.wss_args.wss_t_num) {
        error!("Failed to initialize CGW runtimes! Error: {e}");
        return Err(e);
    }

    if args.feature_topomap_enabled {
        warn!("CGW_FEATURE_TOPOMAP_ENABLE is set, TOPOMAP feature (unstable) will be enabled (realtime events / state processing) - heavy performance drop with high number of devices connected could be observed");
    }

    info!(
        "Starting CGW application, rev tag: {}",
        env::var("CGW_CONTAINER_BUILD_REV").unwrap_or("<CGW-unknown-tag>".to_string())
    );

    // Create a Notify instance to notify the main task of a shutdown signal
    let shutdown_notify = Arc::new(Notify::new());
    let shutdown_notify_clone = Arc::clone(&shutdown_notify);

    // Spawn a task to listen for SIGHUP, SIGINT, and SIGTERM signals
    tokio::spawn(async move {
        if let Err(e) = signal_handler(shutdown_notify_clone).await {
            error!("Failed to handle signal (SIGHUP, SIGINT, or SIGTERM)! Error: {e}");
        }
    });

    // Make sure metrics are available <before> any of the components
    // starts up;
    CGWMetrics::get_ref()
        .start(args.metrics_args.metrics_port)
        .await?;
    let app = Arc::new(AppCore::new(args).await?);

    app.run(shutdown_notify).await;

    Ok(())
}
