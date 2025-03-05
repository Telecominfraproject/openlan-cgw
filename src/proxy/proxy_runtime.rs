use cgw_common::cgw_errors::{Error, Result};

use lazy_static::lazy_static;

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use tokio::runtime::{Builder, Runtime};

#[derive(Hash, Eq, PartialEq, Debug)]
pub enum ProxyRuntimeType {
    WssRxTx,
    MboxInternal,
    MboxNbApiRx,
    MboxNbApiTx,
    MboxRelay,
}

lazy_static! {
    static ref RUNTIMES: Mutex<HashMap<ProxyRuntimeType, Arc<Runtime>>> = Mutex::new(HashMap::new());
}

pub fn proxy_initialize_runtimes(wss_t_num: usize) -> Result<()> {
    let wss_runtime_handle = Arc::new(
        Builder::new_multi_thread()
            .worker_threads(wss_t_num)
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("proxy-wss-t-{}", id)
            })
            .thread_stack_size(3 * 1024 * 1024)
            .enable_all()
            .build()?,
    );
    let internal_mbox_runtime_handle = Arc::new(
        Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("proxy-mbox")
            .thread_stack_size(1024 * 1024)
            .enable_all()
            .build()?,
    );
    let nb_api_mbox_rx_runtime_handle = Arc::new(
        Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("proxy-mbox-nbapi")
            .thread_stack_size(1024 * 1024)
            .enable_all()
            .build()?,
    );
    let nb_api_mbox_tx_runtime_handle = Arc::new(
        Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("proxy-mbox-nbapi-tx")
            .thread_stack_size(1024 * 1024)
            .enable_all()
            .build()?,
    );
    let relay_msg_mbox_runtime_handle = Arc::new(
        Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("proxy-relay-mbox-nbapi")
            .thread_stack_size(1024 * 1024)
            .enable_all()
            .build()?,
    );

    let mut runtimes = match RUNTIMES.lock() {
        Ok(runtimes_lock) => runtimes_lock,
        Err(e) => {
            return Err(Error::Runtime(format!(
                "Failed to get runtimes lock! Error: {e}"
            )));
        }
    };

    runtimes.insert(ProxyRuntimeType::WssRxTx, wss_runtime_handle);
    runtimes.insert(ProxyRuntimeType::MboxInternal, internal_mbox_runtime_handle);
    runtimes.insert(ProxyRuntimeType::MboxNbApiRx, nb_api_mbox_rx_runtime_handle);
    runtimes.insert(ProxyRuntimeType::MboxNbApiTx, nb_api_mbox_tx_runtime_handle);
    runtimes.insert(ProxyRuntimeType::MboxRelay, relay_msg_mbox_runtime_handle);

    Ok(())
}

pub fn proxy_get_runtime(runtime_type: ProxyRuntimeType) -> Result<Option<Arc<Runtime>>> {
    let runtimes = match RUNTIMES.lock() {
        Ok(runtimes_lock) => runtimes_lock,
        Err(e) => {
            return Err(Error::Runtime(format!(
                "Failed to get runtimes lock! Error: {e}"
            )));
        }
    };

    Ok(runtimes.get(&runtime_type).cloned())
}
