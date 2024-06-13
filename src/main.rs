#![warn(rust_2018_idioms)]
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

use tokio::{
    net::TcpListener,
    runtime::{Builder, Handle, Runtime},
    signal,
    sync::Notify,
    time::{sleep, Duration},
};

use std::{
    env,
    net::{Ipv4Addr, SocketAddr},
    str::FromStr,
    sync::Arc,
};

use rlimit::{setrlimit, Resource};

use cgw_connection_server::CGWConnectionServer;

use cgw_remote_server::CGWRemoteServer;

use cgw_metrics::CGWMetrics;

use cgw_tls::cgw_tls_create_acceptor;

use crate::cgw_errors::{Error, Result};

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

const CGW_DEFAULT_ID: i32 = 0;
const CGW_DEFAULT_WSS_T_NUM: usize = 4;
const CGW_DEFAULT_LOG_LEVEL: AppCoreLogLevel = AppCoreLogLevel::Debug;
const CGW_DEFAULT_WSS_IP: Ipv4Addr = Ipv4Addr::new(0, 0, 0, 0);
const CGW_DEFAULT_WSS_PORT: u16 = 15002;
const CGW_DEFAULT_WSS_CAS: &str = "cas.pem";
const CGW_DEFAULT_WSS_CERT: &str = "cert.pem";
const CGW_DEFAULT_WSS_KEY: &str = "key.pem";
const CGW_DEFAULT_GRPC_IP: Ipv4Addr = Ipv4Addr::new(0, 0, 0, 0);
const CGW_DEFAULT_GRPC_PORT: u16 = 50051;
const CGW_DEFAULT_KAFKA_IP: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);
const CGW_DEFAULT_KAFKA_PORT: u16 = 9092;
const CGW_DEFAULT_KAFKA_CONSUME_TOPIC: &str = "CnC";
const CGW_DEFAULT_KAFKA_PRODUCE_TOPIC: &str = "CnC_Res";
const CGW_DEFAULT_DB_IP: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);
const CGW_DEFAULT_DB_PORT: u16 = 6379;
const CGW_DEFAULT_DB_NAME: &str = "cgw";
const CGW_DEFAULT_DB_USERNAME: &str = "cgw";
const CGW_DEFAULT_DB_PASSWORD: &str = "123";
const CGW_DEFAULT_REDIS_IP: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);
const CGW_DEFAULT_REDIS_PORT: u16 = 5432;
const CGW_DEFAULT_ALLOW_CERT_MISMATCH: &str = "no";
const CGW_DEFAULT_METRICS_PORT: u16 = 8080;

/// CGW server
pub struct AppArgs {
    /// Loglevel of application
    log_level: AppCoreLogLevel,

    /// CGW unique identifier (u64)
    cgw_id: i32,

    /// Number of thread in a threadpool dedicated for handling secure websocket connections
    wss_t_num: usize,
    /// IP to listen for incoming WSS connection
    wss_ip: Ipv4Addr,
    /// PORT to listen for incoming WSS connection
    wss_port: u16,
    /// WSS CAS certificate (contains root and issuer certificates)
    wss_cas: String,
    /// WSS certificate
    wss_cert: String,
    /// WSS private key
    wss_key: String,

    /// IP to listen for incoming GRPC connection
    grpc_ip: Ipv4Addr,
    /// PORT to listen for incoming GRPC connection
    grpc_port: u16,

    /// IP to connect to KAFKA broker
    kafka_ip: Ipv4Addr,
    /// PORT to connect to KAFKA broker
    kafka_port: u16,
    /// KAFKA topic from where to consume messages
    #[allow(unused)]
    kafka_consume_topic: String,
    /// KAFKA topic where to produce messages
    #[allow(unused)]
    kafka_produce_topic: String,

    /// IP to connect to DB (PSQL)
    db_ip: Ipv4Addr,
    /// PORT to connect to DB (PSQL)
    db_port: u16,
    /// DB name to connect to in DB (PSQL)
    db_name: String,
    /// DB user name use with connection to in DB (PSQL)
    db_username: String,
    /// DB user password use with connection to in DB (PSQL)
    db_password: String,

    /// IP to connect to DB (REDIS)
    redis_db_ip: Ipv4Addr,
    /// PORT to connect to DB (REDIS)
    redis_db_port: u16,

    /// Allow Missmatch
    allow_mismatch: bool,

    // PORT to connect to Metrics
    metrics_port: u16,
}

impl AppArgs {
    fn parse() -> Self {
        let log_level: AppCoreLogLevel = match env::var("CGW_LOG_LEVEL") {
            Ok(val) => val.parse().ok().unwrap_or(CGW_DEFAULT_LOG_LEVEL),
            Err(_) => CGW_DEFAULT_LOG_LEVEL,
        };

        let cgw_id: i32 = match env::var("CGW_ID") {
            Ok(val) => val.parse().ok().unwrap_or(CGW_DEFAULT_ID),
            Err(_) => CGW_DEFAULT_ID,
        };

        let wss_t_num: usize = match env::var("DEFAULT_WSS_THREAD_NUM") {
            Ok(val) => val.parse().ok().unwrap_or(CGW_DEFAULT_WSS_T_NUM),
            Err(_) => CGW_DEFAULT_WSS_T_NUM,
        };

        let wss_ip: Ipv4Addr = match env::var("CGW_WSS_IP") {
            Ok(val) => Ipv4Addr::from_str(val.as_str()).unwrap_or(CGW_DEFAULT_WSS_IP),
            Err(_) => CGW_DEFAULT_WSS_IP,
        };

        let wss_port: u16 = match env::var("CGW_WSS_PORT") {
            Ok(val) => val.parse().ok().unwrap_or(CGW_DEFAULT_WSS_PORT),
            Err(_) => CGW_DEFAULT_WSS_PORT,
        };

        let wss_cas: String = env::var("CGW_WSS_CAS").unwrap_or(CGW_DEFAULT_WSS_CAS.to_string());
        let wss_cert: String = env::var("CGW_WSS_CERT").unwrap_or(CGW_DEFAULT_WSS_CERT.to_string());
        let wss_key: String = env::var("CGW_WSS_KEY").unwrap_or(CGW_DEFAULT_WSS_KEY.to_string());

        let grpc_ip: Ipv4Addr = match env::var("CGW_GRPC_IP") {
            Ok(val) => Ipv4Addr::from_str(val.as_str()).unwrap_or(CGW_DEFAULT_GRPC_IP),
            Err(_) => CGW_DEFAULT_GRPC_IP,
        };

        let grpc_port: u16 = match env::var("CGW_GRPC_PORT") {
            Ok(val) => val.parse().ok().unwrap_or(CGW_DEFAULT_GRPC_PORT),
            Err(_) => CGW_DEFAULT_GRPC_PORT,
        };

        let kafka_ip: Ipv4Addr = match env::var("CGW_KAFKA_IP") {
            Ok(val) => Ipv4Addr::from_str(val.as_str()).unwrap_or(CGW_DEFAULT_KAFKA_IP),
            Err(_) => CGW_DEFAULT_KAFKA_IP,
        };

        let kafka_port: u16 = match env::var("CGW_KAFKA_PORT") {
            Ok(val) => val.parse().ok().unwrap_or(CGW_DEFAULT_KAFKA_PORT),
            Err(_) => CGW_DEFAULT_KAFKA_PORT,
        };

        let kafka_consume_topic: String = env::var("CGW_KAFKA_CONSUMER_TOPIC")
            .unwrap_or(CGW_DEFAULT_KAFKA_CONSUME_TOPIC.to_string());
        let kafka_produce_topic: String = env::var("CGW_KAFKA_PRODUCER_TOPIC")
            .unwrap_or(CGW_DEFAULT_KAFKA_PRODUCE_TOPIC.to_string());

        let db_ip: Ipv4Addr = match env::var("CGW_DB_IP") {
            Ok(val) => Ipv4Addr::from_str(val.as_str()).unwrap_or(CGW_DEFAULT_DB_IP),
            Err(_) => CGW_DEFAULT_DB_IP,
        };

        let db_port: u16 = match env::var("CGW_DB_PORT") {
            Ok(val) => val.parse().ok().unwrap_or(CGW_DEFAULT_DB_PORT),
            Err(_) => CGW_DEFAULT_DB_PORT,
        };

        let db_name: String = env::var("CGW_DB_NAME").unwrap_or(CGW_DEFAULT_DB_NAME.to_string());
        let db_username: String =
            env::var("CGW_DB_USERNAME").unwrap_or(CGW_DEFAULT_DB_USERNAME.to_string());
        let db_password: String =
            env::var("CGW_DB_PASSWORD").unwrap_or(CGW_DEFAULT_DB_PASSWORD.to_string());

        let redis_db_ip: Ipv4Addr = match env::var("CGW_REDIS_IP") {
            Ok(val) => Ipv4Addr::from_str(val.as_str()).unwrap_or(CGW_DEFAULT_REDIS_IP),
            Err(_) => CGW_DEFAULT_KAFKA_IP,
        };

        let redis_db_port: u16 = match env::var("CGW_REDIS_PORT") {
            Ok(val) => val.parse().ok().unwrap_or(CGW_DEFAULT_REDIS_PORT),
            Err(_) => CGW_DEFAULT_REDIS_PORT,
        };

        let mismatch: String = env::var("CGW_ALLOW_CERT_MISMATCH")
            .unwrap_or(CGW_DEFAULT_ALLOW_CERT_MISMATCH.to_string());
        let allow_mismatch = mismatch == "yes";

        let metrics_port: u16 = match env::var("CGW_METRICS_PORT") {
            Ok(val) => val.parse().ok().unwrap_or(CGW_DEFAULT_METRICS_PORT),
            Err(_) => CGW_DEFAULT_METRICS_PORT,
        };

        AppArgs {
            log_level,
            cgw_id,
            wss_t_num,
            wss_ip,
            wss_port,
            wss_cas,
            wss_cert,
            wss_key,
            grpc_ip,
            grpc_port,
            kafka_ip,
            kafka_port,
            kafka_consume_topic,
            kafka_produce_topic,
            db_ip,
            db_port,
            db_name,
            db_username,
            db_password,
            redis_db_ip,
            redis_db_port,
            allow_mismatch,
            metrics_port,
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
                error!("Failed to create CGW server: {:?}", e);
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

        let cgw_remote_server = CGWRemoteServer::new(&self.args);
        let cgw_srv_clone = self.cgw_server.clone();
        let cgw_con_serv = self.cgw_server.clone();
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
                    cgw_con_serv.cleanup_redis().await;
                    break;
                },
                _ = async {
                    sleep(Duration::from_millis(5000)).await;
                } => {},
            }
        }
    }
}

async fn server_loop(app_core: Arc<AppCore>) -> Result<()> {
    debug!("sever_loop entry");

    debug!(
        "Starting WSS server, listening at {}:{}",
        app_core.args.wss_ip, app_core.args.wss_port
    );
    // Bind the server's socket
    let sockaddraddr = SocketAddr::new(
        std::net::IpAddr::V4(app_core.args.wss_ip),
        app_core.args.wss_port,
    );
    let listener: Arc<TcpListener> = match TcpListener::bind(sockaddraddr).await {
        Ok(listener) => Arc::new(listener),
        Err(_) => return Err(Error::Other("listener bind failed")),
    };

    info!("Started WSS server.");

    let tls_acceptor = cgw_tls_create_acceptor(&app_core.args).await?;

    // Spawn explicitly in main thread: created task accepts connection,
    // but handling is spawned inside another threadpool runtime
    let app_core_clone = app_core.clone();
    let _ = app_core
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
                        error!("Failed to Accept conn {e}\n");
                        continue;
                    }
                };

                app_core_clone.conn_ack_runtime_handle.spawn(async move {
                    cgw_server_clone
                        .ack_connection(socket, tls_acceptor_clone, remote_addr, conn_idx)
                        .await;
                });

                conn_idx += 1;
            }
        })
        .await;

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
    let args = AppArgs::parse();

    // Configure logger
    setup_logger(args.log_level);

    // Create a Notify instance to notify the main task of a shutdown signal
    let shutdown_notify = Arc::new(Notify::new());
    let shutdown_notify_clone = Arc::clone(&shutdown_notify);

    // Spawn a task to listen for SIGHUP, SIGINT, and SIGTERM signals
    tokio::spawn(async move {
        if let Err(e) = signal_handler(shutdown_notify_clone).await {
            error!("Failed to handle signal: {:?}", e);
        }
    });

    // Make sure metrics are available <before> any of the components
    // starts up;
    CGWMetrics::get_ref().start(args.metrics_port).await?;
    let app = Arc::new(AppCore::new(args).await?);

    app.run(shutdown_notify).await;

    Ok(())
}
