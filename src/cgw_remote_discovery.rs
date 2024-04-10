use crate::{
    cgw_db_accessor::{CGWDBAccessor, CGWDBInfra, CGWDBInfrastructureGroup},
    cgw_device::{CGWDevice, CGWDeviceState},
    cgw_devices_cache::CGWDevicesCache,
    cgw_metrics::{CGWMetrics, CGWMetricsCounterOpType, CGWMetricsCounterType},
    cgw_remote_client::CGWRemoteClient,
    AppArgs,
};

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use redis_async::resp_array;

use eui48::MacAddress;

use tokio::sync::RwLock;

// Used in remote lookup
static REDIS_KEY_SHARD_ID_PREFIX: &'static str = "shard_id_";
static REDIS_KEY_SHARD_ID_FIELDS_NUM: usize = 12;
static REDIS_KEY_SHARD_VALUE_ASSIGNED_G_NUM: &'static str = "assigned_groups_num";

// Used in group assign / reassign
static REDIS_KEY_GID: &'static str = "group_id_";
static REDIS_KEY_GID_VALUE_GID: &'static str = "gid";
static REDIS_KEY_GID_VALUE_SHARD_ID: &'static str = "shard_id";

#[derive(Clone, Debug)]
pub struct CGWREDISDBShard {
    id: i32,
    server_ip: IpAddr,
    server_port: u16,
    assigned_groups_num: i32,
    capacity: i32,
    threshold: i32,
}

impl From<Vec<String>> for CGWREDISDBShard {
    fn from(values: Vec<String>) -> Self {
        assert!(
            values.len() >= REDIS_KEY_SHARD_ID_FIELDS_NUM,
            "Unexpected size of parsed vector: at least {REDIS_KEY_SHARD_ID_FIELDS_NUM} expected"
        );
        assert!(values[0] == "id", "redis.res[0] != id, unexpected.");
        assert!(
            values[2] == "server_ip",
            "redis.res[2] != server_ip, unexpected."
        );
        assert!(
            values[4] == "server_port",
            "redis.res[4] != server_port, unexpected."
        );
        assert!(
            values[6] == "assigned_groups_num",
            "redis.res[6] != assigned_groups_num, unexpected."
        );
        assert!(
            values[8] == "capacity",
            "redis.res[8] != capacity, unexpected."
        );
        assert!(
            values[10] == "threshold",
            "redis.res[10] != threshold, unexpected."
        );

        CGWREDISDBShard {
            id: values[1].parse::<i32>().unwrap(),
            server_ip: values[3].parse::<IpAddr>().unwrap(),
            server_port: values[5].parse::<u16>().unwrap(),
            assigned_groups_num: values[7].parse::<i32>().unwrap(),
            capacity: values[9].parse::<i32>().unwrap(),
            threshold: values[11].parse::<i32>().unwrap(),
        }
    }
}

impl Into<Vec<String>> for CGWREDISDBShard {
    fn into(self) -> Vec<String> {
        vec![
            "id".to_string(),
            self.id.to_string(),
            "server_ip".to_string(),
            self.server_ip.to_string(),
            "server_port".to_string(),
            self.server_port.to_string(),
            "assigned_groups_num".to_string(),
            self.assigned_groups_num.to_string(),
            "capacity".to_string(),
            self.capacity.to_string(),
            "threshold".to_string(),
            self.threshold.to_string(),
        ]
    }
}

#[derive(Clone)]
pub struct CGWRemoteConfig {
    pub remote_id: i32,
    pub server_ip: Ipv4Addr,
    pub server_port: u16,
}

impl CGWRemoteConfig {
    pub fn new(id: i32, ip_conf: Ipv4Addr, port: u16) -> Self {
        CGWRemoteConfig {
            remote_id: id,
            server_ip: ip_conf,
            server_port: port,
        }
    }
    pub fn to_socket_addr(&self) -> SocketAddr {
        SocketAddr::new(std::net::IpAddr::V4(self.server_ip), self.server_port)
    }
}

#[derive(Clone)]
pub struct CGWRemoteIface {
    pub shard: CGWREDISDBShard,
    client: CGWRemoteClient,
}

#[derive(Clone)]
pub struct CGWRemoteDiscovery {
    db_accessor: Arc<CGWDBAccessor>,
    redis_client: redis_async::client::paired::PairedConnection,
    gid_to_cgw_cache: Arc<RwLock<HashMap<i32, i32>>>,
    remote_cgws_map: Arc<RwLock<HashMap<i32, CGWRemoteIface>>>,
    local_shard_id: i32,
}

impl CGWRemoteDiscovery {
    pub async fn new(app_args: &AppArgs) -> Self {
        let rc = CGWRemoteDiscovery {
            db_accessor: Arc::new(CGWDBAccessor::new(app_args).await),
            redis_client: redis_async::client::paired::paired_connect(
                app_args.redis_db_ip.to_string(),
                app_args.redis_db_port,
            )
            .await
            .unwrap(),
            gid_to_cgw_cache: Arc::new(RwLock::new(HashMap::new())),
            local_shard_id: app_args.cgw_id,
            remote_cgws_map: Arc::new(RwLock::new(HashMap::new())),
        };

        let _ = rc.sync_gid_to_cgw_map().await;
        let _ = rc.sync_remote_cgw_map().await;

        if let None = rc.remote_cgws_map.read().await.get(&rc.local_shard_id) {
            let redisdb_shard_info = CGWREDISDBShard {
                id: app_args.cgw_id,
                server_ip: std::net::IpAddr::V4(app_args.grpc_ip.clone()),
                server_port: u16::try_from(app_args.grpc_port).unwrap(),
                assigned_groups_num: 0i32,
                capacity: 1000i32,
                threshold: 50i32,
            };
            let redis_req_data: Vec<String> = redisdb_shard_info.into();

            let _ = rc
                .redis_client
                .send::<i32>(resp_array![
                    "DEL",
                    format!("{REDIS_KEY_SHARD_ID_PREFIX}{}", app_args.cgw_id)
                ])
                .await;

            if let Err(e) = rc
                .redis_client
                .send::<String>(
                    resp_array![
                        "HSET",
                        format!("{REDIS_KEY_SHARD_ID_PREFIX}{}", app_args.cgw_id)
                    ]
                    .append(redis_req_data),
                )
                .await
            {
                panic!("Failed to create record about shard in REDIS, e:{e}");
            }
        }

        let _ = rc.sync_gid_to_cgw_map().await;
        let _ = rc.sync_remote_cgw_map().await;

        debug!(
            "Found {} remote CGWs",
            rc.remote_cgws_map.read().await.len() - 1
        );

        for (_key, val) in rc.remote_cgws_map.read().await.iter() {
            if val.shard.id == rc.local_shard_id {
                continue;
            }
            debug!(
                "Shard #{}, IP {}:{}",
                val.shard.id, val.shard.server_ip, val.shard.server_port
            );
        }

        rc
    }

    pub async fn sync_gid_to_cgw_map(&self) {
        let mut lock = self.gid_to_cgw_cache.write().await;

        // Clear hashmap
        lock.clear();

        let redis_keys: Vec<String> = match self
            .redis_client
            .send::<Vec<String>>(resp_array!["KEYS", format!("{}*", REDIS_KEY_GID)])
            .await
        {
            Err(e) => {
                panic!("Failed to get KEYS list from REDIS, e:{e}");
            }
            Ok(r) => r,
        };

        for key in redis_keys {
            let gid: i32 = match self
                .redis_client
                .send::<String>(resp_array!["HGET", &key, REDIS_KEY_GID_VALUE_GID])
                .await
            {
                Ok(res) => {
                    match res.parse::<i32>() {
                        Ok(res) => res,
                        Err(e) => {
                            warn!("Found proper key '{key}' entry, but failed to parse GID from it:\n{e}");
                            continue;
                        }
                    }
                }
                Err(e) => {
                    warn!("Found proper key '{key}' entry, but failed to fetch GID from it:\n{e}");
                    continue;
                }
            };
            let shard_id: i32 = match self
                .redis_client
                .send::<String>(resp_array!["HGET", &key, REDIS_KEY_GID_VALUE_SHARD_ID])
                .await
            {
                Ok(res) => match res.parse::<i32>() {
                    Ok(res) => res,
                    Err(e) => {
                        warn!("Found proper key '{key}' entry, but failed to parse SHARD_ID from it:\n{e}");
                        continue;
                    }
                },
                Err(e) => {
                    warn!("Found proper key '{key}' entry, but failed to fetch SHARD_ID from it:\n{e}");
                    continue;
                }
            };

            debug!("Found group {key}, gid: {gid}, shard_id: {shard_id}");

            match lock.insert(gid, shard_id) {
                None => continue,
                Some(_v) => warn!(
                    "Populated gid_to_cgw_map with previous value being alerady set, unexpected"
                ),
            }
        }

        let mut local_cgw_gid_num: i64 = 0;
        for (_key, val) in lock.iter() {
            if *val == self.local_shard_id {
                local_cgw_gid_num += 1;
            }
        }

        CGWMetrics::get_ref().change_counter(
            CGWMetricsCounterType::GroupsAssignedNum,
            CGWMetricsCounterOpType::Set(local_cgw_gid_num),
        );
    }

    pub async fn sync_device_to_gid_cache(&self, cache: Arc<RwLock<CGWDevicesCache>>) {
        if let Some(groups_infra) = self.db_accessor.get_all_infras().await {
            let mut devices_cache = cache.write().await;
            for item in groups_infra.iter() {
                devices_cache.add_device_to_cache(
                    &item.mac.to_string(eui48::MacAddressFormat::HexString),
                    &CGWDevice::new(
                        CGWDeviceState::CGWDeviceDisconnected,
                        item.infra_group_id,
                        true,
                    ),
                );
            }
        }
    }

    async fn sync_remote_cgw_map(&self) -> Result<(), &'static str> {
        let mut lock = self.remote_cgws_map.write().await;

        // Clear hashmap
        lock.clear();

        let redis_keys: Vec<String> = match self
            .redis_client
            .send::<Vec<String>>(resp_array![
                "KEYS",
                format!("{}*", REDIS_KEY_SHARD_ID_PREFIX)
            ])
            .await
        {
            Err(e) => {
                warn!("Failed to get cgw shard KEYS list from REDIS, e:{e}");
                return Err("Remote CGW Shards list fetch from REDIS failed");
            }
            Ok(r) => r,
        };

        for key in redis_keys {
            match self
                .redis_client
                .send::<Vec<String>>(resp_array!["HGETALL", &key])
                .await
            {
                Ok(res) => {
                    let shrd: CGWREDISDBShard = match CGWREDISDBShard::try_from(res) {
                        Ok(v) => v,
                        Err(_e) => {
                            warn!("Failed to parse CGWREDISDBShard, {key}");
                            continue;
                        }
                    };

                    let endpoint_str = String::from("http://")
                        + &shrd.server_ip.to_string()
                        + ":"
                        + &shrd.server_port.to_string();
                    let cgw_iface = CGWRemoteIface {
                        shard: shrd,
                        client: CGWRemoteClient::new(endpoint_str),
                    };
                    lock.insert(cgw_iface.shard.id, cgw_iface);
                }
                Err(e) => {
                    warn!("Found proper key '{key}' entry, but failed to fetch Shard info from it:\n{e}");
                    continue;
                }
            }
        }

        CGWMetrics::get_ref().change_counter(
            CGWMetricsCounterType::ActiveCGWNum,
            CGWMetricsCounterOpType::Set(i64::try_from(lock.len()).unwrap()),
        );

        Ok(())
    }

    pub async fn get_infra_group_owner_id(&self, gid: i32) -> Option<i32> {
        // try to use internal cache first
        if let Some(id) = self.gid_to_cgw_cache.read().await.get(&gid) {
            return Some(*id);
        }

        // then try to use redis
        self.sync_gid_to_cgw_map().await;

        if let Some(id) = self.gid_to_cgw_cache.read().await.get(&gid) {
            return Some(*id);
        }

        None
    }

    async fn increment_cgw_assigned_groups_num(&self, cgw_id: i32) -> Result<(), &'static str> {
        debug!("Incrementing assigned groups num cgw_id_{cgw_id}");

        if let Err(e) = self
            .redis_client
            .send::<i32>(resp_array![
                "HINCRBY",
                format!("{}{cgw_id}", REDIS_KEY_SHARD_ID_PREFIX),
                REDIS_KEY_SHARD_VALUE_ASSIGNED_G_NUM,
                "1"
            ])
            .await
        {
            warn!("Failed to increment CGW{cgw_id} assigned group num count, e:{e}");
            return Err("Failed to increment assigned group num count");
        }

        if cgw_id == self.local_shard_id {
            CGWMetrics::get_ref().change_counter(
                CGWMetricsCounterType::GroupsAssignedNum,
                CGWMetricsCounterOpType::Inc,
            );
        }
        Ok(())
    }

    async fn decrement_cgw_assigned_groups_num(&self, cgw_id: i32) -> Result<(), &'static str> {
        debug!("Decrementing assigned groups num cgw_id_{cgw_id}");

        if let Err(e) = self
            .redis_client
            .send::<i32>(resp_array![
                "HINCRBY",
                format!("{}{cgw_id}", REDIS_KEY_SHARD_ID_PREFIX),
                REDIS_KEY_SHARD_VALUE_ASSIGNED_G_NUM,
                "-1"
            ])
            .await
        {
            warn!("Failed to decrement CGW{cgw_id} assigned group num count, e:{e}");
            return Err("Failed to decrement assigned group num count");
        }

        if cgw_id == self.local_shard_id {
            CGWMetrics::get_ref().change_counter(
                CGWMetricsCounterType::GroupsAssignedNum,
                CGWMetricsCounterOpType::Dec,
            );
        }

        Ok(())
    }

    async fn get_infra_group_cgw_assignee(&self) -> Result<i32, &'static str> {
        let lock = self.remote_cgws_map.read().await;
        let mut hash_vec: Vec<(&i32, &CGWRemoteIface)> = lock.iter().collect();

        hash_vec.sort_by(|a, b| {
            b.1.shard
                .assigned_groups_num
                .cmp(&a.1.shard.assigned_groups_num)
        });

        for x in hash_vec {
            let max_capacity: i32 = x.1.shard.capacity + x.1.shard.threshold;
            if x.1.shard.assigned_groups_num + 1 <= max_capacity {
                debug!("Found CGW shard to assign group to (id {})", x.1.shard.id);
                return Ok(x.1.shard.id);
            }
        }

        warn!(
            "Every available CGW is exceeding capacity+threshold limit, using least loaded one..."
        );
        if let Some(least_loaded_cgw) = lock
            .iter()
            .min_by(|a, b| {
                a.1.shard
                    .assigned_groups_num
                    .cmp(&b.1.shard.assigned_groups_num)
            })
            .map(|(_k, _v)| _v)
        {
            warn!("Found least loaded CGW id: {}", least_loaded_cgw.shard.id);
            return Ok(least_loaded_cgw.shard.id);
        }

        return Err("Unexpected: Failed to find the least loaded CGW shard");
    }

    async fn assign_infra_group_to_cgw(&self, gid: i32) -> Result<i32, &'static str> {
        // Delete key (if exists), recreate with new owner
        let _ = self.deassign_infra_group_to_cgw(gid).await;

        let dst_cgw_id: i32 = match self.get_infra_group_cgw_assignee().await {
            Ok(v) => v,
            Err(e) => {
                warn!("Failed to assign {gid} to any shard, reason:{e}");
                return Err(e);
            }
        };

        if let Err(e) = self
            .redis_client
            .send::<String>(resp_array![
                "HSET",
                format!("{REDIS_KEY_GID}{gid}"),
                REDIS_KEY_GID_VALUE_GID,
                gid.to_string(),
                REDIS_KEY_GID_VALUE_SHARD_ID,
                dst_cgw_id.to_string()
            ])
            .await
        {
            error!("Failed to update REDIS gid{gid} owner to shard{dst_cgw_id}, e:{e}");
            return Err("Hot-cache (REDIS DB) update owner failed");
        }

        self.gid_to_cgw_cache.write().await.insert(gid, dst_cgw_id);

        debug!("REDIS: assigned gid{gid} to shard{dst_cgw_id}");

        Ok(dst_cgw_id)
    }

    pub async fn deassign_infra_group_to_cgw(&self, gid: i32) -> Result<(), &'static str> {
        if let Err(e) = self
            .redis_client
            .send::<i64>(resp_array!["DEL", format!("{REDIS_KEY_GID}{gid}")])
            .await
        {
            error!("Failed to deassigned REDIS gid{gid} owner, e:{e}");
            return Err("Hot-cache (REDIS DB) deassign owner failed");
        }

        debug!("REDIS: deassigned gid{gid} from controlled CGW");

        self.gid_to_cgw_cache.write().await.remove(&gid);

        Ok(())
    }

    pub async fn create_infra_group(
        &self,
        g: &CGWDBInfrastructureGroup,
    ) -> Result<i32, &'static str> {
        //TODO: transaction-based insert/assigned_group_num update (DB)
        let rc = self.db_accessor.insert_new_infra_group(g).await;
        if let Err(e) = rc {
            return Err(e);
        }

        let shard_id: i32 = match self.assign_infra_group_to_cgw(g.id).await {
            Ok(v) => v,
            Err(_e) => {
                let _ = self.db_accessor.delete_infra_group(g.id).await;
                return Err("Assign group to CGW shard failed");
            }
        };

        let rc = self.increment_cgw_assigned_groups_num(shard_id).await;
        if let Err(e) = rc {
            return Err(e);
        }

        Ok(shard_id)
    }

    pub async fn destroy_infra_group(
        &self,
        gid: i32,
        cache: Arc<RwLock<CGWDevicesCache>>,
    ) -> Result<(), &'static str> {
        let cgw_id: Option<i32> = self.get_infra_group_owner_id(gid).await;
        if let Some(id) = cgw_id {
            let _ = self.deassign_infra_group_to_cgw(gid).await;
            let _ = self.decrement_cgw_assigned_groups_num(id).await;
        }

        //TODO: transaction-based insert/assigned_group_num update (DB)
        let rc = self.db_accessor.delete_infra_group(gid).await;
        if let Err(e) = rc {
            return Err(e);
        } else {
            let mut devices_to_remove: Vec<String> = Vec::new();
            let mut device_cache = cache.write().await;
            for (key, device) in device_cache.iter_mut() {
                if device.get_device_group_id() == gid {
                    if device.get_device_state() == CGWDeviceState::CGWDeviceConnected {
                        device.set_device_remains_in_sql_db(false);
                        device.set_device_group_id(0);
                    } else {
                        devices_to_remove.push(key.clone());
                    }
                }
            }

            for key in devices_to_remove.iter() {
                device_cache.del_device_from_cache(key);
            }

            device_cache.dump_devices_cache();
        }

        Ok(())
    }

    pub async fn create_ifras_list(
        &self,
        gid: i32,
        infras: Vec<String>,
        cache: Arc<RwLock<CGWDevicesCache>>,
    ) -> Result<(), Vec<String>> {
        // TODO: assign list to shards; currently - only created bulk, no assignment
        let mut futures = Vec::with_capacity(infras.len());
        // Results store vec of MACs we failed to add
        let mut failed_infras: Vec<String> = Vec::with_capacity(futures.len());
        for x in infras.iter() {
            let db_accessor_clone = self.db_accessor.clone();
            let infra = CGWDBInfra {
                mac: MacAddress::parse_str(&x).unwrap(),
                infra_group_id: gid,
            };

            futures.push(tokio::spawn(async move {
                if let Err(_) = db_accessor_clone.insert_new_infra(&infra).await {
                    Err(infra.mac.to_string(eui48::MacAddressFormat::HexString))
                } else {
                    Ok(())
                }
            }));
        }

        for (i, future) in futures.iter_mut().enumerate() {
            match future.await {
                Ok(res) => {
                    if let Err(mac) = res {
                        failed_infras.push(mac);
                    } else {
                        let mut devices_cache = cache.write().await;
                        let device_mac = infras[i].clone();
                        if devices_cache.check_device_exists_in_cache(&device_mac) {
                            devices_cache.update_device_from_cache_device_id(&device_mac, gid);
                            devices_cache.update_device_from_cache_device_remains_in_sql_db(
                                &device_mac,
                                true,
                            );
                        } else {
                            devices_cache.add_device_to_cache(
                                &device_mac,
                                &CGWDevice::new(CGWDeviceState::CGWDeviceDisconnected, gid, true),
                            );
                        }
                        devices_cache.dump_devices_cache();
                    }
                }
                Err(_) => {
                    failed_infras.push(infras[i].clone());
                }
            }
        }

        if failed_infras.len() > 0 {
            return Err(failed_infras);
        }

        Ok(())
    }

    pub async fn destroy_ifras_list(
        &self,
        _gid: i32,
        infras: Vec<String>,
        cache: Arc<RwLock<CGWDevicesCache>>,
    ) -> Result<(), Vec<String>> {
        let mut futures = Vec::with_capacity(infras.len());
        // Results store vec of MACs we failed to add
        let mut failed_infras: Vec<String> = Vec::with_capacity(futures.len());
        for x in infras.iter() {
            let db_accessor_clone = self.db_accessor.clone();
            let mac = MacAddress::parse_str(&x).unwrap();

            futures.push(tokio::spawn(async move {
                if let Err(_) = db_accessor_clone.delete_infra(mac).await {
                    Err(mac.to_string(eui48::MacAddressFormat::HexString))
                } else {
                    Ok(())
                }
            }));
        }

        for (i, future) in futures.iter_mut().enumerate() {
            match future.await {
                Ok(res) => {
                    if let Err(mac) = res {
                        failed_infras.push(mac);
                    } else {
                        let mut devices_cache = cache.write().await;
                        let device_mac = infras[i].clone();
                        if devices_cache.check_device_exists_in_cache(&device_mac) {
                            if devices_cache
                                .get_device_from_cache_device_state(&device_mac)
                                .unwrap()
                                == CGWDeviceState::CGWDeviceConnected
                            {
                                devices_cache.update_device_from_cache_device_remains_in_sql_db(
                                    &device_mac,
                                    false,
                                );
                                devices_cache.update_device_from_cache_device_id(&device_mac, 0);
                            } else {
                                devices_cache.del_device_from_cache(&device_mac);
                            }
                            devices_cache.dump_devices_cache();
                        }
                    }
                }
                Err(_) => {
                    failed_infras.push(infras[i].clone());
                }
            }
        }

        if failed_infras.len() > 0 {
            return Err(failed_infras);
        }

        Ok(())
    }

    pub async fn relay_request_stream_to_remote_cgw(
        &self,
        shard_id: i32,
        stream: Vec<(String, String)>,
    ) -> Result<(), ()> {
        // try to use internal cache first
        if let Some(cl) = self.remote_cgws_map.read().await.get(&shard_id) {
            if let Err(()) = cl.client.relay_request_stream(stream).await {
                return Err(());
            }

            return Ok(());
        }

        // then try to use redis
        let _ = self.sync_remote_cgw_map().await;

        if let Some(cl) = self.remote_cgws_map.read().await.get(&shard_id) {
            if let Err(()) = cl.client.relay_request_stream(stream).await {
                return Err(());
            }
            return Ok(());
        }

        error!("No suitable CGW instance #{shard_id} was discovered, cannot relay msg");
        return Err(());
    }

    pub async fn rebalance_all_groups(&self) -> Result<u32, &'static str> {
        warn!("Executing group rebalancing procedure");

        let groups = match self.db_accessor.get_all_infra_groups().await {
            Some(list) => list,
            None => {
                warn!("Tried to execute rebalancing when 0 groups created in DB");
                return Err("Cannot do rebalancing due to absence of any groups created in DB");
            }
        };

        // Clear local cache
        self.gid_to_cgw_cache.write().await.clear();

        for (cgw_id, _val) in self.remote_cgws_map.read().await.iter() {
            if let Err(e) = self
                .redis_client
                .send::<i32>(resp_array![
                    "HSET",
                    format!("{}{cgw_id}", REDIS_KEY_SHARD_ID_PREFIX),
                    REDIS_KEY_SHARD_VALUE_ASSIGNED_G_NUM,
                    "0"
                ])
                .await
            {
                warn!("Failed to reset CGW{cgw_id} assigned group num count, e:{e}");
            }
        }

        for i in groups.iter() {
            let _ = self.sync_remote_cgw_map().await;
            let _ = self.sync_gid_to_cgw_map().await;
            match self.assign_infra_group_to_cgw(i.id).await {
                Ok(shard_id) => {
                    debug!("Rebalancing: assigned {} to shard {}", i.id, shard_id);
                    let _ = self.increment_cgw_assigned_groups_num(shard_id).await;
                }
                Err(_e) => {}
            }
        }

        let _ = self.sync_remote_cgw_map().await;
        let _ = self.sync_gid_to_cgw_map().await;

        Ok(0u32)
    }
}
