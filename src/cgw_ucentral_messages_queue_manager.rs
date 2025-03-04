use eui48::MacAddress;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

use cgw_common::{
    cgw_errors::{Error, Result},
    cgw_ucentral_parser::{CGWUCentralCommand, CGWUCentralCommandType},
};
use crate::cgw_nb_api_listener::ConsumerMetadata;

#[derive(Clone, Copy, Debug, Default)]
pub enum CGWUCentralMessagesQueueState {
    Rx,
    #[default]
    RxTx,
    Discard,
    Unknown,
}

struct CGWUCentralMessagesQueue {
    queue: VecDeque<CGWUCentralMessagesQueueItem>,
    queue_state: CGWUCentralMessagesQueueState,
    last_req_id: u64,
    last_req_timeout: Duration,
    last_req_consumer_medata: Option<ConsumerMetadata>,
}

impl CGWUCentralMessagesQueue {
    fn new() -> Self {
        CGWUCentralMessagesQueue {
            queue: VecDeque::<CGWUCentralMessagesQueueItem>::new(),
            queue_state: CGWUCentralMessagesQueueState::default(),
            last_req_id: 0,
            last_req_timeout: Duration::ZERO,
            last_req_consumer_medata: None,
        }
    }

    fn set_state(&mut self, state: CGWUCentralMessagesQueueState) {
        self.queue_state = state;
    }

    fn get_state(&self) -> CGWUCentralMessagesQueueState {
        self.queue_state
    }

    fn get_item(&self, index: usize) -> Option<&CGWUCentralMessagesQueueItem> {
        self.queue.get(index)
    }

    fn set_last_req_id(&mut self, req_id: u64) {
        self.last_req_id = req_id;
    }

    fn get_last_req_id(&self) -> u64 {
        self.last_req_id
    }

    fn set_last_req_consumer_metadata(&mut self, req_metadata: Option<ConsumerMetadata>) {
        self.last_req_consumer_medata = req_metadata;
    }

    fn set_last_req_timeout(&mut self, req_timeout: Duration) {
        self.last_req_timeout = req_timeout;
    }

    fn remove_item(&mut self, index: usize) -> Option<CGWUCentralMessagesQueueItem> {
        self.queue.remove(index)
    }

    fn insert_item(&mut self, index: usize, value: CGWUCentralMessagesQueueItem) {
        self.queue.insert(index, value);
    }

    fn push_back_item(&mut self, value: CGWUCentralMessagesQueueItem) {
        self.queue.push_back(value);
    }

    fn queue_len(&self) -> usize {
        self.queue.len()
    }
}

#[derive(Clone, Debug, Default)]
pub struct CGWUCentralMessagesQueueItem {
    pub command: CGWUCentralCommand,
    pub message: String,
    pub uuid: Uuid,
    pub timeout: Option<u64>,
    pub consumer_metadata: Option<ConsumerMetadata>,
}

impl CGWUCentralMessagesQueueItem {
    pub fn new(
        command: CGWUCentralCommand,
        message: String,
        uuid: Uuid,
        timeout: Option<u64>,
        consumer_metadata: Option<ConsumerMetadata>,
    ) -> CGWUCentralMessagesQueueItem {
        CGWUCentralMessagesQueueItem {
            command,
            message,
            uuid,
            timeout,
            consumer_metadata,
        }
    }
}

pub struct CGWUCentralMessagesQueueManager {
    queue: Arc<RwLock<HashMap<MacAddress, Arc<RwLock<CGWUCentralMessagesQueue>>>>>,
    disconnected_devices: Arc<RwLock<HashMap<MacAddress, i32>>>,
}

const MESSAGE_QUEUE_REBOOT_MSG_INDEX: usize = 0;
const MESSAGE_QUEUE_CONFIGURE_MSG_INDEX: usize = 1;
const MESSAGE_QUEUE_OTHER_MSG_INDEX: usize = 2;

pub const TIMEOUT_MANAGER_DURATION: Duration = Duration::from_secs(10);
pub const MESSAGE_TIMEOUT_DURATION: Duration = Duration::from_secs(300);

lazy_static! {
    pub static ref CGW_MESSAGES_QUEUE: Arc<RwLock<CGWUCentralMessagesQueueManager>> =
        Arc::new(RwLock::new(CGWUCentralMessagesQueueManager {
            queue: Arc::new(RwLock::new(HashMap::<
                MacAddress,
                Arc<RwLock<CGWUCentralMessagesQueue>>,
            >::new(),)),
            disconnected_devices: Arc::new(RwLock::new(HashMap::<MacAddress, i32>::new()))
        }));
}

// The HashMap is used to store requests for device
// The HashMap key - device MAC, value - VecDeque as message queue
// There are two reserved items under index '0' and '1'
// Index '0' - store 'reboot' command
// Index '1' - store 'configure' command
// All rest used to store other messages types
impl CGWUCentralMessagesQueueManager {
    pub async fn create_device_messages_queue(&self, device_mac: &MacAddress) {
        if !self.check_messages_queue_exists(device_mac).await {
            debug!("Create queue message for device: {device_mac}");
            let new_queue: Arc<RwLock<CGWUCentralMessagesQueue>> =
                Arc::new(RwLock::new(CGWUCentralMessagesQueue::new()));

            new_queue.write().await.insert_item(
                MESSAGE_QUEUE_REBOOT_MSG_INDEX,
                CGWUCentralMessagesQueueItem::default(),
            );
            new_queue.write().await.insert_item(
                MESSAGE_QUEUE_CONFIGURE_MSG_INDEX,
                CGWUCentralMessagesQueueItem::default(),
            );

            let mut write_lock = self.queue.write().await;
            write_lock.insert(*device_mac, new_queue);
        }
    }

    pub async fn delete_device_messages_queue(&self, device_mac: &MacAddress) {
        let mut write_lock = self.queue.write().await;
        debug!("Remove queue message for device: {device_mac}");
        write_lock.remove(device_mac);
    }

    pub async fn clear_device_message_queue(
        &self,
        infra_mac: &MacAddress,
    ) -> Vec<CGWUCentralMessagesQueueItem> {
        debug!("Flush infra {infra_mac} queue due to timeout");
        let mut requests: Vec<CGWUCentralMessagesQueueItem> = Vec::new();

        while let Some(msg) = self.dequeue_device_message(infra_mac).await {
            requests.push(msg);
        }

        requests
    }

    pub async fn push_device_message(
        &self,
        device_mac: MacAddress,
        value: CGWUCentralMessagesQueueItem,
    ) -> Result<Option<CGWUCentralMessagesQueueItem>> {
        // 1. Message queue for device exist -> get mutable ref
        self.create_device_messages_queue(&device_mac).await;
        let container_lock = self.queue.read().await;

        let mut replaced_request: Option<CGWUCentralMessagesQueueItem> = None;

        let mut device_msg_queue = container_lock
            .get(&device_mac)
            .ok_or_else(|| {
                Error::UCentralMessagesQueue("Failed to get device message queue".to_string())
            })?
            .write()
            .await;
        let queue_state = device_msg_queue.get_state();

        debug!(
            "Push message for device: {}, queue state {:?}, command type {:?}",
            device_mac, queue_state, value.command.cmd_type
        );

        // 2. Check Queue Message state
        match queue_state {
            CGWUCentralMessagesQueueState::RxTx | CGWUCentralMessagesQueueState::Rx => {
                match value.command.cmd_type {
                    // 3. If new message type == Reboot then replace message under reserved index
                    CGWUCentralCommandType::Reboot => {
                        if let Some(current_reboot) =
                            device_msg_queue.remove_item(MESSAGE_QUEUE_REBOOT_MSG_INDEX)
                        {
                            if current_reboot.command != CGWUCentralCommand::default() {
                                replaced_request = Some(current_reboot);
                            }
                        }

                        device_msg_queue.insert_item(MESSAGE_QUEUE_REBOOT_MSG_INDEX, value);
                    }
                    // 4. If new message type == Configure then replace message under reserved index
                    CGWUCentralCommandType::Configure => {
                        if let Some(current_configure) =
                            device_msg_queue.remove_item(MESSAGE_QUEUE_CONFIGURE_MSG_INDEX)
                        {
                            if current_configure.command != CGWUCentralCommand::default() {
                                replaced_request = Some(current_configure);
                            }
                        }

                        device_msg_queue.insert_item(MESSAGE_QUEUE_CONFIGURE_MSG_INDEX, value);
                    }
                    // 5. If new message type == Other then push it back to queue
                    _ => {
                        device_msg_queue.push_back_item(value);
                    }
                }
            }
            CGWUCentralMessagesQueueState::Discard | CGWUCentralMessagesQueueState::Unknown => {
                let err_msg: String = format!(
                    "Device {} queue is in {:?} state - drop request {}",
                    device_mac, queue_state, value.command.id
                );
                debug!("{err_msg}");
                return Err(Error::UCentralMessagesQueue(err_msg));
            }
        }

        Ok(replaced_request)
    }

    pub async fn check_messages_queue_exists(&self, device_mac: &MacAddress) -> bool {
        let container_lock = self.queue.read().await;

        container_lock.get(device_mac).is_some()
    }

    pub async fn get_device_messages_queue_len(&self, device_mac: &MacAddress) -> usize {
        let mut queue_size: usize = 0;

        if self.check_messages_queue_exists(device_mac).await {
            let container_lock = self.queue.read().await;

            if let Some(device_msg_queue) = container_lock.get(device_mac) {
                let read_lock = device_msg_queue.read().await;
                queue_size = read_lock.queue_len();

                let default_msg = CGWUCentralCommand::default();
                if let Some(message) = read_lock.get_item(MESSAGE_QUEUE_REBOOT_MSG_INDEX) {
                    if message.command == default_msg {
                        queue_size -= 1;
                    }
                }

                if let Some(message) = read_lock.get_item(MESSAGE_QUEUE_CONFIGURE_MSG_INDEX) {
                    if message.command == default_msg {
                        queue_size -= 1;
                    }
                }
            }
        }

        queue_size
    }

    pub async fn set_device_last_req_info(
        &self,
        device_mac: &MacAddress,
        req_id: u64,
        req_timeout: Duration,
        req_metadata: Option<ConsumerMetadata>,
    ) {
        let container_lock = self.queue.read().await;
        if let Some(device_msg_queue) = container_lock.get(device_mac) {
            let mut write_lock = device_msg_queue.write().await;

            write_lock.set_last_req_id(req_id);
            write_lock.set_last_req_timeout(req_timeout);
            write_lock.set_last_req_consumer_metadata(req_metadata);
        }
    }

    pub async fn get_device_last_request_id(&self, device_mac: &MacAddress) -> Option<u64> {
        let container_lock = self.queue.read().await;
        let device_msg_queue = container_lock.get(device_mac)?.read().await;

        debug!(
            "Last request id for device {}: {}",
            device_mac,
            device_msg_queue.get_last_req_id()
        );

        Some(device_msg_queue.get_last_req_id())
    }

    // Dequeue messages according to priority:
    // 1. Check if reboot message exist in queue and return it if true
    // 2. Check if configure message exist in queue and return it if true
    // 3. Remove other message from queue and return it
    pub async fn dequeue_device_message(
        &self,
        device_mac: &MacAddress,
    ) -> Option<CGWUCentralMessagesQueueItem> {
        let ret_msg: CGWUCentralMessagesQueueItem;
        let default_msg = CGWUCentralCommand::default();

        if self.get_device_messages_queue_len(device_mac).await == 0 {
            return None;
        }

        let container_lock = self.queue.read().await;

        let mut device_msg_queue = container_lock.get(device_mac)?.write().await;
        let reboot_msg = device_msg_queue
            .get_item(MESSAGE_QUEUE_REBOOT_MSG_INDEX)?
            .clone();
        let configure_msg = device_msg_queue
            .get_item(MESSAGE_QUEUE_CONFIGURE_MSG_INDEX)?
            .clone();

        if reboot_msg.command != default_msg {
            ret_msg = reboot_msg;
            device_msg_queue.remove_item(MESSAGE_QUEUE_REBOOT_MSG_INDEX);
            device_msg_queue.insert_item(
                MESSAGE_QUEUE_REBOOT_MSG_INDEX,
                CGWUCentralMessagesQueueItem::default(),
            );
        } else if configure_msg.command != default_msg {
            ret_msg = configure_msg;
            device_msg_queue.remove_item(MESSAGE_QUEUE_CONFIGURE_MSG_INDEX);
            device_msg_queue.insert_item(
                MESSAGE_QUEUE_CONFIGURE_MSG_INDEX,
                CGWUCentralMessagesQueueItem::default(),
            );
        } else {
            ret_msg = device_msg_queue.remove_item(MESSAGE_QUEUE_OTHER_MSG_INDEX)?;
        }

        Some(ret_msg)
    }

    pub async fn get_device_queue_state(
        &self,
        device_mac: &MacAddress,
    ) -> CGWUCentralMessagesQueueState {
        let mut queue_state: CGWUCentralMessagesQueueState = CGWUCentralMessagesQueueState::Unknown;

        let container_lock = self.queue.read().await;
        if let Some(device_msg_queue) = container_lock.get(device_mac) {
            queue_state = device_msg_queue.read().await.get_state();
        }

        queue_state
    }

    pub async fn set_device_queue_state(
        &self,
        device_mac: &MacAddress,
        state: CGWUCentralMessagesQueueState,
    ) {
        let container_lock = self.queue.read().await;
        if let Some(device_msg_queue) = container_lock.get(device_mac) {
            device_msg_queue.write().await.set_state(state);
        }
    }

    pub async fn device_disconnected(&self, infra_mac: &MacAddress, infra_gid: i32) {
        let mut disconnected_lock = self.disconnected_devices.write().await;
        disconnected_lock.insert(*infra_mac, infra_gid);
    }

    pub async fn device_connected(&self, infra_mac: &MacAddress) {
        let mut disconnected_lock = self.disconnected_devices.write().await;
        disconnected_lock.remove(infra_mac);
    }

    pub async fn device_request_tick(&self, infra_mac: &MacAddress, elapsed: Duration) -> bool {
        let mut expired: bool = false;
        let container_read_lock = self.queue.read().await;

        if let Some(device_queue) = container_read_lock.get(infra_mac) {
            let mut write_lock = device_queue.write().await;
            write_lock.last_req_timeout = write_lock.last_req_timeout.saturating_sub(elapsed);

            if write_lock.last_req_timeout == Duration::ZERO {
                expired = true;
            }
        }

        expired
    }

    pub async fn iterate_over_disconnected_devices(
        &self,
    ) -> HashMap<MacAddress, Vec<(i32, CGWUCentralMessagesQueueItem)>> {
        let mut devices_to_flush: Vec<(i32, MacAddress)> = Vec::<(i32, MacAddress)>::new();

        {
            // 1. Check if disconnected device message queue is empty
            // If not empty - just do tick
            // Else - disconnected device and it queue should be removed
            let container_read_lock = self.disconnected_devices.read().await;
            for (infra_mac, infra_gid) in container_read_lock.iter() {
                if self.get_device_messages_queue_len(infra_mac).await > 0 {
                    // If device request is timed out - device and it queue should be removed
                    if self
                        .device_request_tick(infra_mac, TIMEOUT_MANAGER_DURATION)
                        .await
                    {
                        devices_to_flush.push((*infra_gid, *infra_mac));
                    }
                } else {
                    devices_to_flush.push((*infra_gid, *infra_mac));
                }
            }
        }

        let mut failed_requests: HashMap<MacAddress, Vec<(i32, CGWUCentralMessagesQueueItem)>> =
            HashMap::new();
        // 2. Remove disconnected device and it queue
        let mut container_write_lock = self.disconnected_devices.write().await;
        for (infra_gid, infra_mac) in devices_to_flush.iter() {
            let mut requests: Vec<(i32, CGWUCentralMessagesQueueItem)> = Vec::new();
            while let Some(msg) = self.dequeue_device_message(infra_mac).await {
                requests.push((*infra_gid, msg));
            }
            failed_requests.insert(*infra_mac, requests);
            self.delete_device_messages_queue(infra_mac).await;
            container_write_lock.remove(infra_mac);
        }

        failed_requests
    }
}
