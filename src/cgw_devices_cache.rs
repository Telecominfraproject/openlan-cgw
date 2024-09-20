use crate::cgw_device::CGWDevice;
use eui48::MacAddress;
use serde::{Deserialize, Serialize};
use std::collections::hash_map;
use std::fs::File;
use std::{collections::HashMap, io::Write};

type DevicesCacheType = HashMap<MacAddress, CGWDevice>;

#[derive(Serialize, Deserialize)]
pub struct CGWDevicesCache {
    cache: DevicesCacheType,
}

pub struct CGWDevicesCacheIterMutable<'a> {
    iter: hash_map::IterMut<'a, MacAddress, CGWDevice>,
}

impl<'a> Iterator for CGWDevicesCacheIterMutable<'a> {
    type Item = (&'a MacAddress, &'a mut CGWDevice);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl CGWDevicesCache {
    pub fn new() -> CGWDevicesCache {
        let cache = DevicesCacheType::new();
        CGWDevicesCache { cache }
    }

    pub fn add_device(&mut self, key: &MacAddress, value: &CGWDevice) -> bool {
        let status: bool = if self.check_device_exists(key) {
            debug!(
                "Failed to add device {}. Requested item already exist.",
                key
            );
            false
        } else {
            self.cache.insert(*key, value.clone());
            true
        };

        status
    }

    pub fn del_device(&mut self, key: &MacAddress) -> bool {
        let status: bool = if self.check_device_exists(key) {
            self.cache.remove(key);
            true
        } else {
            debug!(
                "Failed to del device {}. Requested item does not exist.",
                key
            );
            false
        };

        status
    }

    pub fn check_device_exists(&self, key: &MacAddress) -> bool {
        self.cache.contains_key(key)
    }

    pub fn get_device_mut(&mut self, key: &MacAddress) -> Option<&mut CGWDevice> {
        if let Some(value) = self.cache.get_mut(key) {
            Some(value)
        } else {
            None
        }
    }

    pub fn get_device(&self, key: &MacAddress) -> Option<&CGWDevice> {
        if let Some(value) = self.cache.get(key) {
            Some(value)
        } else {
            None
        }
    }

    #[allow(dead_code)]
    pub fn get_device_id(&self, key: &MacAddress) -> Option<i32> {
        self.cache.get(key).map(|value| value.get_device_group_id())
    }

    pub fn iter_mut(&mut self) -> CGWDevicesCacheIterMutable<'_> {
        CGWDevicesCacheIterMutable {
            iter: self.cache.iter_mut(),
        }
    }

    pub fn dump_devices_cache(&self) {
        // Debug print - simply ignore errors if any!
        if let Ok(json_output) = serde_json::to_string_pretty(&self) {
            debug!("Cache: {}", json_output);

            if let Ok(mut fd) = File::create("/var/devices_cache.json") {
                let _ = fd.write_all(json_output.as_bytes());
            }
        };
    }
}

impl Drop for CGWDevicesCache {
    fn drop(&mut self) {
        self.dump_devices_cache();
    }
}
