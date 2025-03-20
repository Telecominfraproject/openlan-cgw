#![warn(rust_2018_idioms)]
mod proxy_connection_processor;
mod proxy_connection_server;
mod proxy_runtime;
mod proxy_remote_discovery;

#[macro_use]
extern crate log;

use tokio::{
    net::TcpListener,
    runtime::{Builder, Handle, Runtime},
    sync::Notify,
    time::{sleep, Duration},
};

use std::{env, net::SocketAddr, sync::Arc};

use rlimit::{setrlimit, Resource};

use proxy_connection_server::ProxyConnectionServer;
use proxy_runtime::proxy_initialize_runtimes;

use cgw_common::{
    cgw_errors::{Error, Result},
    cgw_app_args::AppArgs,
    AppCoreLogLevel,
    cgw_tls::cgw_tls_create_acceptor,
    signal_handler,
    cgw_set_tcp_keepalive_options,
};

pub struct AppCore {
    proxy_server: Arc<ProxyConnectionServer>,
    main_runtime_handle: Arc<Handle>,
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
                .thread_name("proxy-c-ack")
                .thread_stack_size(stack_size)
                .enable_all()
                .build()?,
        );

        let proxy_server = match ProxyConnectionServer::new(&app_args).await {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to create Proxy server! Error: {e}");
                return Err(e);
            }
        };

        Ok(AppCore {
            proxy_server,
            main_runtime_handle: current_runtime,
            conn_ack_runtime_handle: c_ack_runtime_handle,
            args: app_args,
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

        main_runtime_handle.spawn(async move { server_loop(core_clone).await });

        loop {
            tokio::select! {
                // Cleanup if notified of received SIGHUP, SIGINT or SIGTERM
                _ = notifier.notified() => {
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
                let proxy_server_clone = app_core_clone.proxy_server.clone();
                let tls_acceptor_clone = tls_acceptor.clone();

                // Asynchronously wait for an inbound socket.
                let (socket, remote_addr) = match listener.accept().await {
                    Ok((sock, addr)) => (sock, addr),
                    Err(e) => {
                        error!("Failed to accept connection! Error: {e}");
                        continue;
                    }
                };

                let connection_time = std::time::Instant::now();

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
                    proxy_server_clone
                        .ack_connection(socket, tls_acceptor_clone, remote_addr, connection_time)
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
        AppCoreLogLevel::Debug => ::std::env::set_var("RUST_LOG", "proxy_cgw=debug"),
        AppCoreLogLevel::Info => ::std::env::set_var("RUST_LOG", "proxy_cgw=info"),
    }
    env_logger::init();
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
    if let Err(e) = proxy_initialize_runtimes(args.wss_args.wss_t_num) {
        error!("Failed to initialize Proxy runtimes! Error: {e}");
        return Err(e);
    }

    info!(
        "Starting Proxy application, rev tag: {}",
        env::var("PROXY_CONTAINER_BUILD_REV").unwrap_or("<PROXY-unknown-tag>".to_string())
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

    let app = Arc::new(AppCore::new(args).await?);

    app.run(shutdown_notify).await;

    Ok(())
}
