use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::cgw_ucentral_parser::{CGWUCentralCommand, CGWUCentralCommandType};

#[derive(Clone, Debug, Default)]
pub struct CGWUCentralMessagesQueueItem {
    pub command: CGWUCentralCommand,
    pub message: String,
}

impl CGWUCentralMessagesQueueItem {
    pub fn new(
        command: CGWUCentralCommand,
        message: String,
    ) -> CGWUCentralMessagesQueueItem {
        CGWUCentralMessagesQueueItem {
            command,
            message,
        }
    }
}

pub struct CGWUCentralMessagesQueueManager {
    queue: Arc<RwLock<HashMap<String, Arc<RwLock<VecDeque<CGWUCentralMessagesQueueItem>>>>>>,
}

const MESSAGE_QUEUE_REBOOT_MSG_INDEX: usize = 0;
const MESSAGE_QUEUE_CONFIGURE_MSG_INDEX: usize = 1;
const MESSAGE_QUEUE_OTHER_MSG_INDEX: usize = 2;

lazy_static! {
    pub static ref CGW_MESSAGES_QUEUE: Arc<RwLock<CGWUCentralMessagesQueueManager>> =
        Arc::new(RwLock::new(CGWUCentralMessagesQueueManager {
            queue: Arc::new(RwLock::new(HashMap::<
                String,
                Arc<RwLock<VecDeque<CGWUCentralMessagesQueueItem>>>,
            >::new(),))
        }));
}

// The HashMap is used to store requests for device
// The HashMap key - device MAC, value - VecDeque as message queue
// There are two reserved items under index '0' and '1'
// Index '0' - store 'reboot' command
// Index '1' - store 'configure' command
// All rest used to store other messages types
impl CGWUCentralMessagesQueueManager {
    pub async fn create_device_messages_queue(&self, device_mac: &String) {
        if !self.check_messages_queue_exists(&device_mac).await {
            let new_queue: Arc<RwLock<VecDeque<CGWUCentralMessagesQueueItem>>> =
                Arc::new(RwLock::new(VecDeque::new()));

            new_queue.write().await.insert(
                MESSAGE_QUEUE_REBOOT_MSG_INDEX,
                CGWUCentralMessagesQueueItem::default(),
            );
            new_queue.write().await.insert(
                MESSAGE_QUEUE_CONFIGURE_MSG_INDEX,
                CGWUCentralMessagesQueueItem::default(),
            );

            let mut write_lock = self.queue.write().await;
            write_lock.insert(device_mac.clone(), new_queue);
        }
    }

    pub async fn delete_device_messages_queue(&mut self, device_mac: &String) {
        if self.check_messages_queue_exists(device_mac).await {
            let mut write_lock = self.queue.write().await;
            write_lock.remove(device_mac);
        } else {
            error!(
                "Trying to delete message queue for unexisting device: {}",
                device_mac
            );
        }
    }

    pub async fn push_device_message(
        &self,
        device_mac: &String,
        value: &CGWUCentralMessagesQueueItem,
    ) {
        // 1. Get current message type
        let new_cmd_type: CGWUCentralCommandType = value.command.cmd_type.clone();

        // 2. Create new message queue for device with MAC if it doesn't exist
        self.create_device_messages_queue(device_mac).await;

        // 3. Message queue for device exist -> get mutable ref
        let container_lock = self.queue.read().await;
        let mut device_msg_queue = container_lock.get(device_mac).unwrap().write().await;

        match new_cmd_type {
            // 3. If new message type == Reboot then replace message under reserved index
            CGWUCentralCommandType::Reboot => {
                device_msg_queue.remove(MESSAGE_QUEUE_REBOOT_MSG_INDEX);
                device_msg_queue.insert(MESSAGE_QUEUE_REBOOT_MSG_INDEX, value.clone());
            }

            // 4. If new message type == Configure then replace message under reserved index
            CGWUCentralCommandType::Configure => {
                device_msg_queue.remove(MESSAGE_QUEUE_CONFIGURE_MSG_INDEX);
                device_msg_queue.insert(MESSAGE_QUEUE_CONFIGURE_MSG_INDEX, value.clone());
            }
            // 5. If new message type == Other then push it back to queue
            _ => {
                device_msg_queue.push_back(value.clone());
            }
        }
    }

    pub async fn check_messages_queue_exists(&self, device_mac: &String) -> bool {
        let exist: bool;
        let container_lock = self.queue.read().await;
        match container_lock.get(device_mac) {
            Some(_) => exist = true,
            None => exist = false,
        }

        exist
    }

    pub async fn get_device_messages_queue_len(&self, device_mac: &String) -> usize {
        let mut queue_size: usize = 0;

        if self.check_messages_queue_exists(device_mac).await {
            let container_lock = self.queue.read().await;
            let device_msg_queue = container_lock.get(device_mac).unwrap().read().await;
            queue_size = device_msg_queue.len();

            let default_msg = CGWUCentralCommand::default();
            if device_msg_queue
                .get(MESSAGE_QUEUE_REBOOT_MSG_INDEX)
                .unwrap()
                .command
                == default_msg
            {
                queue_size -= 1;
            }
            if device_msg_queue
                .get(MESSAGE_QUEUE_CONFIGURE_MSG_INDEX)
                .unwrap()
                .command
                == default_msg
            {
                queue_size -= 1;
            }
        }

        queue_size
    }

    // Dequeue messages accoring to priority:
    // 1. Check if reboot message exist in queue and return it if true
    // 2. Check if configure message exist in queue and return it if true
    // 3. Remove other message from qeueu and return it
    pub async fn dequeue_device_message(
        &self,
        device_mac: &String,
    ) -> Option<CGWUCentralMessagesQueueItem> {
        let ret_msg: CGWUCentralMessagesQueueItem;
        let default_msg = CGWUCentralCommand::default();
        let container_lock = self.queue.read().await;

        if self.get_device_messages_queue_len(device_mac).await == 0 {
            return None;
        }

        let mut device_msg_queue = container_lock.get(device_mac).unwrap().write().await;
        let reboot_msg = device_msg_queue
            .get(MESSAGE_QUEUE_REBOOT_MSG_INDEX)
            .unwrap()
            .clone();
        let configure_msg = device_msg_queue
            .get(MESSAGE_QUEUE_CONFIGURE_MSG_INDEX)
            .unwrap()
            .clone();

        if reboot_msg.command != default_msg {
            ret_msg = reboot_msg;
            device_msg_queue.remove(MESSAGE_QUEUE_REBOOT_MSG_INDEX);
            device_msg_queue.insert(
                MESSAGE_QUEUE_REBOOT_MSG_INDEX,
                CGWUCentralMessagesQueueItem::default(),
            );
        } else if configure_msg.command != default_msg {
            ret_msg = configure_msg;
            device_msg_queue.remove(MESSAGE_QUEUE_CONFIGURE_MSG_INDEX);
            device_msg_queue.insert(
                MESSAGE_QUEUE_CONFIGURE_MSG_INDEX,
                CGWUCentralMessagesQueueItem::default(),
            );
        } else {
            ret_msg = device_msg_queue
                .remove(MESSAGE_QUEUE_OTHER_MSG_INDEX)
                .unwrap();
        }

        Some(ret_msg)
    }
}
