#![warn(rust_2018_idioms)]
mod cgw_connection_processor;
mod cgw_connection_server;
mod cgw_db_accessor;
mod cgw_device;
mod cgw_devices_cache;
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
    time::{sleep, Duration},
};

use tokio_rustls::{
    rustls::{server::WebPkiClientVerifier, RootCertStore, ServerConfig},
    TlsAcceptor,
};

use std::{
    env, io,
    net::{Ipv4Addr, SocketAddr},
    str::FromStr,
    sync::Arc,
};

use rlimit::{setrlimit, Resource};

use cgw_connection_server::CGWConnectionServer;

use cgw_remote_server::CGWRemoteServer;

use cgw_metrics::CGWMetrics;

use crate::cgw_tls::{cgw_tls_read_certs, cgw_tls_read_private_key};

#[derive(Copy, Clone)]
enum AppCoreLogLevel {
    /// Print debug-level messages and above
    Debug,
    /// Print info-level messages and above
    Info,
}

impl FromStr for AppCoreLogLevel {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
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

const CGW_CERTIFICATES_PATH: &str = "/etc/cgw/certs";

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
    async fn new(app_args: AppArgs) -> Self {
        Self::setup_app(&app_args);
        let current_runtime = Arc::new(Handle::current());

        let c_ack_runtime_handle = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("cgw-c-ack")
                .thread_stack_size(1 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap(),
        );
        let rpc_runtime_handle = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("grpc-recv-t")
                .thread_stack_size(1 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap(),
        );
        let app_core = AppCore {
            cgw_server: CGWConnectionServer::new(&app_args).await,
            main_runtime_handle: current_runtime,
            conn_ack_runtime_handle: c_ack_runtime_handle,
            args: app_args,
            grpc_server_runtime_handle: rpc_runtime_handle,
        };
        app_core
    }

    fn setup_app(args: &AppArgs) {
        let nofile_rlimit = Resource::NOFILE.get().unwrap();
        println!("{:?}", nofile_rlimit);
        let nofile_hard_limit = nofile_rlimit.1;
        assert!(setrlimit(Resource::NOFILE, nofile_hard_limit, nofile_hard_limit).is_ok());
        let nofile_rlimit = Resource::NOFILE.get().unwrap();
        println!("{:?}", nofile_rlimit);

        match args.log_level {
            AppCoreLogLevel::Debug => ::std::env::set_var("RUST_LOG", "ucentral_cgw=debug"),
            AppCoreLogLevel::Info => ::std::env::set_var("RUST_LOG", "ucentral_cgw=info"),
        }
        env_logger::init();
    }

    async fn run(self: Arc<AppCore>) {
        let main_runtime_handle = self.main_runtime_handle.clone();
        let core_clone = self.clone();

        let cgw_remote_server = CGWRemoteServer::new(&self.args);
        let cgw_srv_clone = self.cgw_server.clone();
        self.grpc_server_runtime_handle.spawn(async move {
            debug!("cgw_remote_server.start entry");
            cgw_remote_server.start(cgw_srv_clone).await;
            debug!("cgw_remote_server.start exit");
        });

        main_runtime_handle.spawn(async move { server_loop(core_clone).await });

        // TODO:
        // Add signal processing and etcetera app-related handlers.
        loop {
            sleep(Duration::from_millis(5000)).await;
        }
    }
}

// TODO: a method of an object (TlsAcceptor? CGWConnectionServer?), not a plain function
async fn server_loop(app_core: Arc<AppCore>) -> () {
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
        Err(e) => panic!("listener bind failed {e}"),
    };

    info!("Started WSS server.");

    // Read root/issuer certs.
    let cas_path = format!("{}/{}", CGW_CERTIFICATES_PATH, app_core.args.wss_cas);
    let cas = cgw_tls_read_certs(cas_path.as_str()).await.unwrap();

    // Read cert.
    let cert_path = format!("{}/{}", CGW_CERTIFICATES_PATH, app_core.args.wss_cert);
    let mut cert = cgw_tls_read_certs(cert_path.as_str()).await.unwrap();
    cert.extend(cas.clone());

    // Read private key.
    let key_path = format!("{}/{}", CGW_CERTIFICATES_PATH, app_core.args.wss_key);
    let key = cgw_tls_read_private_key(key_path.as_str()).await.unwrap();

    // Create the client certs verifier.
    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(cas);

    let client_verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .unwrap();

    // Create server config.
    let config = ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(cert, key)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))
        .unwrap();

    // Create the TLS acceptor.
    let tls_acceptor = TlsAcceptor::from(Arc::new(config));

    CGWMetrics::get_ref().start(&app_core.args).await;

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

                // TODO: we control tls_acceptor, thus at this stage it's our responsibility
                // to provide underlying certificates common name inside the ack_connection func
                // (CN == mac address of device)
                app_core_clone.conn_ack_runtime_handle.spawn(async move {
                    cgw_server_clone
                        .ack_connection(socket, tls_acceptor_clone, remote_addr, conn_idx)
                        .await;
                });

                conn_idx += 1;
            }
        })
        .await;
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = AppArgs::parse();
    let app = Arc::new(AppCore::new(args).await);

    app.run().await;
}
