use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq)]
pub enum CGWDeviceState {
    CGWDeviceConnected,
    CGWDeviceDisconnected,
}

impl Default for CGWDeviceState {
    fn default() -> Self {
        CGWDeviceState::CGWDeviceDisconnected
    }
}

pub struct OldNew {
    pub old_value: String,
    pub new_value: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct CGWDeviceCapabilities {
    pub firmware: String,
    pub uuid: u64,
    pub compatible: String,
    pub model: String,
    pub platform: String,
    pub label_macaddr: String,
}

impl CGWDeviceCapabilities {
    pub fn update_device_capabilities(&mut self, new_capabilities: &CGWDeviceCapabilities) {
        self.firmware = new_capabilities.firmware.clone();
        self.uuid = new_capabilities.uuid;
        self.compatible = new_capabilities.compatible.clone();
        self.model = new_capabilities.model.clone();
        self.platform = new_capabilities.platform.clone();
        self.label_macaddr = new_capabilities.label_macaddr.clone();
    }
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct CGWDevice {
    state: CGWDeviceState,
    group_id: i32,
    remains_in_db: bool,
    capabilities: CGWDeviceCapabilities,
}

impl CGWDevice {
    pub fn new(
        state: CGWDeviceState,
        group_id: i32,
        remains_in_db: bool,
        capabilities: CGWDeviceCapabilities,
    ) -> CGWDevice {
        CGWDevice {
            state,
            group_id,
            remains_in_db,
            capabilities,
        }
    }

    pub fn set_device_state(&mut self, new_state: CGWDeviceState) {
        self.state = new_state;
    }

    pub fn get_device_state(&self) -> CGWDeviceState {
        self.state
    }

    pub fn set_device_group_id(&mut self, group_id: i32) {
        self.group_id = group_id;
    }

    pub fn get_device_group_id(&self) -> i32 {
        self.group_id
    }

    pub fn set_device_remains_in_db(&mut self, should_remains: bool) {
        self.remains_in_db = should_remains;
    }

    pub fn get_device_remains_in_db(&self) -> bool {
        self.remains_in_db
    }

    pub fn get_device_capabilities(&self) -> CGWDeviceCapabilities {
        self.capabilities.clone()
    }

    pub fn update_device_capabilities(&mut self, new_capabilities: &CGWDeviceCapabilities) {
        self.capabilities
            .update_device_capabilities(new_capabilities);
    }
}

pub fn cgw_detect_device_chages(
    cur_capabilities: &CGWDeviceCapabilities,
    new_capabilities: &CGWDeviceCapabilities,
) -> Option<HashMap<String, OldNew>> {
    let mut diff: HashMap<String, OldNew> = HashMap::new();

    if cur_capabilities.firmware != new_capabilities.firmware {
        diff.insert(
            "firmware".to_string(),
            OldNew {
                old_value: cur_capabilities.firmware.clone(),
                new_value: new_capabilities.firmware.clone(),
            },
        );
    }

    if cur_capabilities.uuid != new_capabilities.uuid {
        diff.insert(
            "uuid".to_string(),
            OldNew {
                old_value: cur_capabilities.uuid.to_string(),
                new_value: new_capabilities.uuid.to_string(),
            },
        );
    }
    if cur_capabilities.compatible != new_capabilities.compatible {
        diff.insert(
            "compatible".to_string(),
            OldNew {
                old_value: cur_capabilities.compatible.clone(),
                new_value: new_capabilities.compatible.clone(),
            },
        );
    }
    if cur_capabilities.model != new_capabilities.model {
        diff.insert(
            "model".to_string(),
            OldNew {
                old_value: cur_capabilities.model.clone(),
                new_value: new_capabilities.model.clone(),
            },
        );
    }
    if cur_capabilities.platform != new_capabilities.platform {
        diff.insert(
            "platform".to_string(),
            OldNew {
                old_value: cur_capabilities.platform.clone(),
                new_value: new_capabilities.platform.clone(),
            },
        );
    }

    if diff.is_empty() {
        return None;
    }

    Some(diff)
}
