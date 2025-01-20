use std::collections::HashMap;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub enum CGWDeviceType {
    CGWDeviceAP,
    CGWDeviceSwitch,
    #[default]
    CGWDeviceUnknown,
}

impl FromStr for CGWDeviceType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ap" => Ok(CGWDeviceType::CGWDeviceAP),
            "switch" => Ok(CGWDeviceType::CGWDeviceSwitch),
            "unknown" => Ok(CGWDeviceType::CGWDeviceUnknown),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Copy, Default, Deserialize, Serialize, PartialEq)]
pub enum CGWDeviceState {
    CGWDeviceConnected,
    #[default]
    CGWDeviceDisconnected,
}

pub struct OldNew {
    pub old_value: String,
    pub new_value: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct CGWDeviceCapabilities {
    pub firmware: String,
    pub compatible: String,
    pub model: String,
    pub platform: String,
    pub label_macaddr: String,
}

impl CGWDeviceCapabilities {
    pub fn update_device_capabilities(&mut self, new_capabilities: &CGWDeviceCapabilities) {
        self.firmware.clone_from(&new_capabilities.firmware);
        self.compatible.clone_from(&new_capabilities.compatible);
        self.model.clone_from(&new_capabilities.model);
        self.platform.clone_from(&new_capabilities.platform);
        self.label_macaddr
            .clone_from(&new_capabilities.label_macaddr);
    }
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct CGWDevice {
    dev_type: CGWDeviceType,
    state: CGWDeviceState,
    group_id: i32,
    remains_in_db: bool,
    capabilities: CGWDeviceCapabilities,
}

impl CGWDevice {
    pub fn new(
        dev_type: CGWDeviceType,
        state: CGWDeviceState,
        group_id: i32,
        remains_in_db: bool,
        capabilities: CGWDeviceCapabilities,
    ) -> CGWDevice {
        CGWDevice {
            dev_type,
            state,
            group_id,
            remains_in_db,
            capabilities,
        }
    }

    pub fn set_device_type(&mut self, dev_type: CGWDeviceType) {
        self.dev_type = dev_type;
    }

    pub fn get_device_type(&self) -> CGWDeviceType {
        self.dev_type
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

pub fn cgw_detect_device_changes(
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
