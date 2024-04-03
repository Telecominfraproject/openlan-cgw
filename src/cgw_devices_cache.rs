use serde::{Deserialize, Serialize};
use std::fs::File;
use std::{collections::HashMap, io::Write};

type DevicesCacheType = HashMap<String, CGWDevice>;

#[derive(Clone, Serialize, Deserialize)]
pub struct CGWDevice {
    group_id: i32,
}

#[derive(Serialize, Deserialize)]
pub struct CGWDevicesCache {
    cache: DevicesCacheType,
}

impl CGWDevice {
    pub fn new(group_id: i32) -> CGWDevice {
        CGWDevice { group_id }
    }

    pub fn set_device_group_id(&mut self, group_id: i32) {
        self.group_id = group_id;
    }

    pub fn get_device_group_id(&self) -> i32 {
        self.group_id
    }
}

impl CGWDevicesCache {
    pub fn new() -> CGWDevicesCache {
        let cache = DevicesCacheType::new();
        CGWDevicesCache { cache }
    }

    pub fn add_device_to_cache(&mut self, key: &String, value: &CGWDevice) -> bool {
        let status: bool;

        if self.check_device_exists_in_cache(key) {
            debug!(
                "Failed to add device {}. Requested item already exist.",
                key
            );
            status = false;
        } else {
            self.cache.insert(key.clone(), value.clone());
            status = true;
        }

        status
    }

    pub fn del_device_from_cache(&mut self, key: &String) -> bool {
        let status: bool;

        if self.check_device_exists_in_cache(key) {
            self.cache.remove(key);
            status = true;
        } else {
            debug!(
                "Failed to del device {}. Requested item does not exist.",
                key
            );
            status = false;
        }

        status
    }

    pub fn check_device_exists_in_cache(&self, key: &String) -> bool {
        let status: bool;
        match self.cache.get(key) {
            Some(_) => status = true,
            None => status = false,
        }

        status
    }

    pub fn update_device_from_cache_device_id(&mut self, key: &String, group_id: i32) -> bool {
        let status: bool;

        if let Some(value) = self.cache.get_mut(key) {
            (*value).set_device_group_id(group_id);
            status = true;
        } else {
            debug!(
                "Failed to update device {} id. Requested item does not exist.",
                key
            );
            status = false;
        }

        status
    }

    pub fn get_device_from_cache_device_id(&self, key: &String) -> Option<i32> {
        if let Some(value) = self.cache.get(key) {
            Some(value.get_device_group_id())
        } else {
            None
        }
    }

    pub fn dump_devices_cache(&self) {
        let json_output = serde_json::to_string_pretty(&self).unwrap();
        let file_path: String = "/var/devices_cache.json".to_string();

        debug!("Cache: {}", json_output);

        let mut fd = File::create(file_path).expect("Failed to create dump file!");
        fd.write(json_output.as_bytes())
            .expect("Failed to write dump!");
    }
}

impl Drop for CGWDevicesCache {
    fn drop(&mut self) {
        self.dump_devices_cache();
    }
}
