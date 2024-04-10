use crate::cgw_device::{CGWDevice, CGWDeviceState};
use serde::{Deserialize, Serialize};
use std::collections::hash_map;
use std::fs::File;
use std::{collections::HashMap, io::Write};

type DevicesCacheType = HashMap<String, CGWDevice>;

#[derive(Serialize, Deserialize)]
pub struct CGWDevicesCache {
    cache: DevicesCacheType,
}

pub struct CGWDevicesCacheIterMutable<'a> {
    iter: hash_map::IterMut<'a, String, CGWDevice>,
}

pub struct CGWDevicesCacheIterImmutable<'a> {
    iter: hash_map::Iter<'a, String, CGWDevice>,
}

impl<'a> Iterator for CGWDevicesCacheIterMutable<'a> {
    type Item = (&'a String, &'a mut CGWDevice);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| (k, v))
    }
}

impl<'a> Iterator for CGWDevicesCacheIterImmutable<'a> {
    type Item = (&'a String, &'a CGWDevice);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| (k, v))
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

    pub fn update_device_from_cache_device_state(
        &mut self,
        key: &String,
        new_state: CGWDeviceState,
    ) -> bool {
        let status: bool;

        if let Some(value) = self.cache.get_mut(key) {
            (*value).set_device_state(new_state);
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

    pub fn update_device_from_cache_device_remains_in_sql_db(
        &mut self,
        key: &String,
        should_remains: bool,
    ) -> bool {
        let status: bool;

        if let Some(value) = self.cache.get_mut(key) {
            (*value).set_device_remains_in_sql_db(should_remains);
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

    pub fn get_device_from_cache_device_state(&self, key: &String) -> Option<CGWDeviceState> {
        if let Some(value) = self.cache.get(key) {
            Some(value.get_device_state())
        } else {
            None
        }
    }

    pub fn update_device_from_cache(&mut self, key: &String, device: &CGWDevice) -> bool {
        let status: bool;

        if let Some(value) = self.cache.get_mut(key) {
            (*value) = device.clone();
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

    pub fn get_device_from_cache_device_remains_in_sql_db(&self, key: &String) -> Option<bool> {
        if let Some(value) = self.cache.get(key) {
            Some(value.get_device_remains_in_sql_db())
        } else {
            None
        }
    }

    pub fn get_device_from_cache_device_id(&self, key: &String) -> Option<i32> {
        if let Some(value) = self.cache.get(key) {
            Some(value.get_device_group_id())
        } else {
            None
        }
    }

    pub fn iter_mut(&mut self) -> CGWDevicesCacheIterMutable<'_> {
        CGWDevicesCacheIterMutable {
            iter: self.cache.iter_mut(),
        }
    }

    pub fn iter(&mut self) -> CGWDevicesCacheIterImmutable<'_> {
        CGWDevicesCacheIterImmutable {
            iter: self.cache.iter(),
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
