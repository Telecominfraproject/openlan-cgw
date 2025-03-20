pub mod cgw_errors;
pub mod cgw_app_args;
pub mod cgw_tls;
pub mod cgw_device;
pub mod cgw_devices_cache;
pub mod cgw_ucentral_parser;
pub mod cgw_ucentral_ap_parser;
pub mod cgw_ucentral_switch_parser;

use std::str::FromStr;
use std::sync::Arc;
use nix::sys::socket::{setsockopt, sockopt};
use tokio::{
    signal,
    sync::Notify,
    net::TcpStream,
};
use std::os::unix::io::AsFd;

use cgw_errors::{Error, Result};

#[macro_use]
extern crate log;

const CGW_TCP_KEEPALIVE_TIMEOUT: u32 = 30;
const CGW_TCP_KEEPALIVE_COUNT: u32 = 3;
const CGW_TCP_KEEPALIVE_INTERVAL: u32 = 10;

#[derive(Copy, Clone)]
pub enum AppCoreLogLevel {
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

pub async fn signal_handler(shutdown_notify: Arc<Notify>) -> Result<()> {
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

pub async fn cgw_set_tcp_keepalive_options(stream: TcpStream) -> Result<TcpStream> {
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
