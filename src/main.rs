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

#[macro_use]
extern crate log;

#[macro_use]
extern crate lazy_static;

use tokio::{
    net::TcpListener,
    runtime::{Builder, Handle, Runtime},
    time::{sleep, Duration},
};

use native_tls::Identity;
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};

use rlimit::{setrlimit, Resource};

use cgw_connection_server::CGWConnectionServer;

use cgw_remote_server::CGWRemoteServer;

use cgw_metrics::CGWMetrics;

use clap::{Parser, ValueEnum};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum AppCoreLogLevel {
    /// Print debug-level messages and above
    Debug,
    /// Print info-level messages and above
    ///
    ///
    Info,
}

/// CGW server
#[derive(Parser, Clone)]
#[command(version, about, long_about = None)]
pub struct AppArgs {
    /// CGW unique identifier (u64)
    #[arg(short, long, default_value_t = 0)]
    cgw_id: i32,

    /// Number of thread in a threadpool dedicated for handling secure websocket connections
    #[arg(short, long, default_value_t = 4)]
    wss_t_num: usize,
    /// Loglevel of application
    #[arg(value_enum, default_value_t = AppCoreLogLevel::Debug)]
    log_level: AppCoreLogLevel,

    /// IP to listen for incoming WSS connection
    #[arg(long, default_value_t = Ipv4Addr::new(0, 0, 0, 0))]
    wss_ip: Ipv4Addr,
    /// PORT to listen for incoming WSS connection
    #[arg(long, default_value_t = 15002)]
    wss_port: u16,

    /// IP to listen for incoming GRPC connection
    #[arg(long, default_value_t = Ipv4Addr::new(0, 0, 0, 0))]
    grpc_ip: Ipv4Addr,
    /// PORT to listen for incoming GRPC connection
    #[arg(long, default_value_t = 50051)]
    grpc_port: u16,

    /// IP to connect to KAFKA broker
    #[arg(long, default_value_t = Ipv4Addr::new(127, 0, 0, 1))]
    kafka_ip: Ipv4Addr,
    /// PORT to connect to KAFKA broker
    #[arg(long, default_value_t = 9092)]
    kafka_port: u16,
    /// KAFKA topic from where to consume messages
    #[arg(long, default_value_t = String::from("CnC"))]
    kafka_consume_topic: String,
    /// KAFKA topic where to produce messages
    #[arg(long, default_value_t = String::from("CnC_Res"))]
    kafka_produce_topic: String,

    /// IP to connect to DB (PSQL)
    #[arg(long, default_value_t = Ipv4Addr::new(127, 0, 0, 1))]
    db_ip: Ipv4Addr,
    /// PORT to connect to DB (PSQL)
    #[arg(long, default_value_t = 5432)]
    db_port: u16,
    /// DB name to connect to in DB (PSQL)
    #[arg(long, default_value_t = String::from("cgw"))]
    db_name: String,
    /// DB user name use with connection to in DB (PSQL)
    #[arg(long, default_value_t = String::from("cgw"))]
    db_username: String,
    /// DB user password use with connection to in DB (PSQL)
    #[arg(long, default_value_t = String::from("123"))]
    db_password: String,

    /// IP to connect to DB (REDIS)
    #[arg(long, default_value_t = Ipv4Addr::new(127, 0, 0, 1))]
    redis_db_ip: Ipv4Addr,
    /// PORT to connect to DB (REDIS)
    #[arg(long, default_value_t = 6379)]
    redis_db_port: u16,
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
    // Create the TLS acceptor.
    // TODO: custom acceptor
    let der = include_bytes!("localhost.crt");
    let key = include_bytes!("localhost.key");
    let cert = match Identity::from_pkcs8(der, key) {
        Ok(cert) => cert,
        Err(e) => panic!("Cannot create SSL identity from supplied cert\n{e}"),
    };

    let tls_acceptor =
        tokio_native_tls::TlsAcceptor::from(match native_tls::TlsAcceptor::builder(cert).build() {
            Ok(builder) => builder,
            Err(e) => panic!("Cannot create SSL-acceptor from supplied cert\n{e}"),
        });

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
