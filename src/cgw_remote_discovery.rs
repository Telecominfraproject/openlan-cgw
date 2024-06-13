use crate::{
    cgw_db_accessor::{CGWDBAccessor, CGWDBInfra, CGWDBInfrastructureGroup},
    cgw_device::{CGWDevice, CGWDeviceState},
    cgw_devices_cache::CGWDevicesCache,
    cgw_errors::{Error, Result},
    cgw_metrics::{
        CGWMetrics, CGWMetricsCounterOpType, CGWMetricsCounterType, CGWMetricsHealthComponent,
        CGWMetricsHealthComponentStatus,
    },
    cgw_remote_client::CGWRemoteClient,
    AppArgs,
};

use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};

use redis_async::resp_array;

use eui48::MacAddress;

use tokio::sync::RwLock;

// Used in remote lookup
static REDIS_KEY_SHARD_ID_PREFIX: &str = "shard_id_";
static REDIS_KEY_SHARD_ID_FIELDS_NUM: usize = 12;
static REDIS_KEY_SHARD_VALUE_ASSIGNED_G_NUM: &str = "assigned_groups_num";

// Used in group assign / reassign
static REDIS_KEY_GID: &str = "group_id_";
static REDIS_KEY_GID_VALUE_GID: &str = "gid";
static REDIS_KEY_GID_VALUE_SHARD_ID: &str = "shard_id";

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CGWREDISDBShard {
    id: i32,
    server_host: String,
    server_port: u16,
    assigned_groups_num: i32,
    capacity: i32,
    threshold: i32,
}

impl From<Vec<String>> for CGWREDISDBShard {
    fn from(values: Vec<String>) -> Self {
        if values.len() < REDIS_KEY_SHARD_ID_FIELDS_NUM {
            error!("Unexpected size of parsed vector: at least {REDIS_KEY_SHARD_ID_FIELDS_NUM} expected");
            return CGWREDISDBShard::default();
        }

        if values[0] != "id" {
            error!("redis.res[0] != id, unexpected.");
            return CGWREDISDBShard::default();
        } else if values[2] != "server_host" {
            error!("redis.res[2] != server_host, unexpected.");
            return CGWREDISDBShard::default();
        } else if values[4] != "server_port" {
            error!("redis.res[4] != server_port, unexpected.");
            return CGWREDISDBShard::default();
        } else if values[6] != "assigned_groups_num" {
            error!("redis.res[6] != assigned_groups_num, unexpected.");
            return CGWREDISDBShard::default();
        } else if values[8] != "capacity" {
            error!("redis.res[8] != capacity, unexpected.");
            return CGWREDISDBShard::default();
        } else if values[10] != "threshold" {
            error!("redis.res[10] != threshold, unexpected.");
            return CGWREDISDBShard::default();
        }

        let id = values[1].parse::<i32>().unwrap_or_default();
        let server_host = values[3].clone();
        let server_port = values[5].parse::<u16>().unwrap_or_default();
        let assigned_groups_num = values[7].parse::<i32>().unwrap_or_default();
        let capacity = values[9].parse::<i32>().unwrap_or_default();
        let threshold = values[11].parse::<i32>().unwrap_or_default();

        CGWREDISDBShard {
            id,
            server_host,
            server_port,
            assigned_groups_num,
            capacity,
            threshold,
        }
    }
}

impl From<CGWREDISDBShard> for Vec<String> {
    fn from(val: CGWREDISDBShard) -> Self {
        vec![
            "id".to_string(),
            val.id.to_string(),
            "server_host".to_string(),
            val.server_host,
            "server_port".to_string(),
            val.server_port.to_string(),
            "assigned_groups_num".to_string(),
            val.assigned_groups_num.to_string(),
            "capacity".to_string(),
            val.capacity.to_string(),
            "threshold".to_string(),
            val.threshold.to_string(),
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
    pub async fn new(app_args: &AppArgs) -> Result<Self> {
        debug!(
            "Trying to create redis db connection ({}:{})",
            app_args.redis_host, app_args.redis_port
        );
        let redis_client = match redis_async::client::paired::paired_connect(
            app_args.redis_host.clone(),
            app_args.redis_port,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                error!(
                    "Cant create CGW Remote Discovery client: Redis client create failed ({:?})",
                    e
                );
                return Err(Error::RemoteDiscovery("Redis client create failed"));
            }
        };

        let db_accessor = match CGWDBAccessor::new(app_args).await {
            Ok(c) => c,
            Err(e) => {
                error!(
                    "Cant create CGW Remote Discovery client: DB Accessor create failed ({:?})",
                    e
                );
                return Err(Error::RemoteDiscovery("DB Accessor create failed"));
            }
        };

        let rc = CGWRemoteDiscovery {
            db_accessor: Arc::new(db_accessor),
            redis_client,
            gid_to_cgw_cache: Arc::new(RwLock::new(HashMap::new())),
            local_shard_id: app_args.cgw_id,
            remote_cgws_map: Arc::new(RwLock::new(HashMap::new())),
        };

        if let Err(e) = rc.sync_gid_to_cgw_map().await {
            error!("Cant create CGW Remote Discovery client: Can't pull records data from REDIS (wrong redis host/port?) ({:?})", e);
            return Err(Error::RemoteDiscovery(
                "Failed to sync (sync_gid_to_cgw_map) gid to cgw map",
            ));
        }
        if let Err(e) = rc.sync_remote_cgw_map().await {
            error!("Cant create CGW Remote Discovery client: Can't pull records data from REDIS (wrong redis host/port?) ({:?})", e);
            return Err(Error::RemoteDiscovery(
                "Failed to sync (sync_remote_cgw_map) remote CGW info from REDIS",
            ));
        }

        if rc
            .remote_cgws_map
            .read()
            .await
            .get(&rc.local_shard_id)
            .is_none()
        {
            let redisdb_shard_info = CGWREDISDBShard {
                id: app_args.cgw_id,
                server_host: app_args.grpc_public_host.clone(),
                server_port: app_args.grpc_public_port,
                assigned_groups_num: 0i32,
                capacity: 1000i32,
                threshold: 50i32,
            };

            let redis_req_data: Vec<String> = redisdb_shard_info.into();

            if let Err(e) = rc
                .redis_client
                .send::<i32>(resp_array![
                    "DEL",
                    format!("{REDIS_KEY_SHARD_ID_PREFIX}{}", app_args.cgw_id)
                ])
                .await
            {
                warn!(
                    "Failed to destroy record about shard in REDIS, first launch? ({:?})",
                    e
                );
            }

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
                error!("Cant create CGW Remote Discovery client: Failed to create record about shard in REDIS: {:?}", e);
                return Err(Error::RemoteDiscovery(
                    "Failed to create record about shard in REDIS",
                ));
            }
        }

        if let Err(e) = rc.sync_gid_to_cgw_map().await {
            error!(
                "Cant create CGW Remote Discovery client: Can't pull records data from REDIS: {:?}",
                e
            );
            return Err(Error::RemoteDiscovery(
                "Failed to sync (sync_gid_to_cgw_map) gid to cgw map",
            ));
        }
        if let Err(e) = rc.sync_remote_cgw_map().await {
            error!(
                "Cant create CGW Remote Discovery client: Can't pull records data from REDIS: {:?}",
                e
            );
            return Err(Error::RemoteDiscovery(
                "Failed to sync (sync_remote_cgw_map) remote CGW info from REDIS",
            ));
        }

        debug!(
            "Found {} remote CGWs",
            rc.remote_cgws_map.read().await.len() - 1
        );

        for (_key, val) in rc.remote_cgws_map.read().await.iter() {
            if val.shard.id == rc.local_shard_id {
                continue;
            }
            debug!(
                "Shard #{}, Hostname/IP {}:{}",
                val.shard.id, val.shard.server_host, val.shard.server_port
            );
        }

        tokio::spawn(async move {
            CGWMetrics::get_ref()
                .change_component_health_status(
                    CGWMetricsHealthComponent::RedisConnection,
                    CGWMetricsHealthComponentStatus::Ready,
                )
                .await;
        });

        info!("Connectiong to REDIS DB has been established!");

        Ok(rc)
    }

    pub async fn sync_gid_to_cgw_map(&self) -> Result<()> {
        let mut lock = self.gid_to_cgw_cache.write().await;

        // Clear hashmap
        lock.clear();

        let redis_keys: Vec<String> = match self
            .redis_client
            .send::<Vec<String>>(resp_array!["KEYS", format!("{}*", REDIS_KEY_GID)])
            .await
        {
            Err(_) => {
                return Err(Error::RemoteDiscovery("Failed to get KEYS list from REDIS"));
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

        Ok(())
    }

    pub async fn sync_device_to_gid_cache(&self, cache: Arc<RwLock<CGWDevicesCache>>) {
        if let Some(groups_infra) = self.db_accessor.get_all_infras().await {
            let mut devices_cache = cache.write().await;
            for item in groups_infra.iter() {
                devices_cache.add_device(
                    &item.mac,
                    &CGWDevice::new(
                        CGWDeviceState::CGWDeviceDisconnected,
                        item.infra_group_id,
                        true,
                        Default::default(),
                    ),
                );
            }
        }
    }

    async fn sync_remote_cgw_map(&self) -> Result<()> {
        let mut lock = self.remote_cgws_map.write().await;

        // Clear hashmap
        lock.clear();

        let redis_keys: Vec<String> = self
            .redis_client
            .send::<Vec<String>>(resp_array![
                "KEYS",
                format!("{}*", REDIS_KEY_SHARD_ID_PREFIX)
            ])
            .await?;

        for key in redis_keys {
            match self
                .redis_client
                .send::<Vec<String>>(resp_array!["HGETALL", &key])
                .await
            {
                Ok(res) => {
                    let shrd: CGWREDISDBShard = CGWREDISDBShard::from(res);
                    if shrd == CGWREDISDBShard::default() {
                        warn!("Failed to parse CGWREDISDBShard, {key}");
                        continue;
                    }

                    let endpoint_str = String::from("http://")
                        + &shrd.server_host
                        + ":"
                        + &shrd.server_port.to_string();
                    let cgw_iface = CGWRemoteIface {
                        shard: shrd,
                        client: CGWRemoteClient::new(endpoint_str)?,
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
            CGWMetricsCounterOpType::Set(i64::try_from(lock.len())?),
        );

        Ok(())
    }

    pub async fn get_infra_group_owner_id(&self, gid: i32) -> Option<i32> {
        // try to use internal cache first
        if let Some(id) = self.gid_to_cgw_cache.read().await.get(&gid) {
            return Some(*id);
        }

        // then try to use redis
        let _ = self.sync_gid_to_cgw_map().await;

        if let Some(id) = self.gid_to_cgw_cache.read().await.get(&gid) {
            return Some(*id);
        }

        None
    }

    async fn increment_cgw_assigned_groups_num(&self, cgw_id: i32) -> Result<()> {
        debug!("Incrementing assigned groups num cgw_id_{cgw_id}");

        self.redis_client
            .send::<i32>(resp_array![
                "HINCRBY",
                format!("{}{cgw_id}", REDIS_KEY_SHARD_ID_PREFIX),
                REDIS_KEY_SHARD_VALUE_ASSIGNED_G_NUM,
                "1"
            ])
            .await?;

        if cgw_id == self.local_shard_id {
            CGWMetrics::get_ref().change_counter(
                CGWMetricsCounterType::GroupsAssignedNum,
                CGWMetricsCounterOpType::Inc,
            );
        }
        Ok(())
    }

    async fn decrement_cgw_assigned_groups_num(&self, cgw_id: i32) -> Result<()> {
        debug!("Decrementing assigned groups num cgw_id_{cgw_id}");

        self.redis_client
            .send::<i32>(resp_array![
                "HINCRBY",
                format!("{}{cgw_id}", REDIS_KEY_SHARD_ID_PREFIX),
                REDIS_KEY_SHARD_VALUE_ASSIGNED_G_NUM,
                "-1"
            ])
            .await?;

        if cgw_id == self.local_shard_id {
            CGWMetrics::get_ref().change_counter(
                CGWMetricsCounterType::GroupsAssignedNum,
                CGWMetricsCounterOpType::Dec,
            );
        }

        Ok(())
    }

    async fn get_infra_group_cgw_assignee(&self) -> Result<i32> {
        let lock = self.remote_cgws_map.read().await;
        let mut hash_vec: Vec<(&i32, &CGWRemoteIface)> = lock.iter().collect();

        hash_vec.sort_by(|a, b| {
            b.1.shard
                .assigned_groups_num
                .cmp(&a.1.shard.assigned_groups_num)
        });

        for x in hash_vec {
            let max_capacity: i32 = x.1.shard.capacity + x.1.shard.threshold;
            if x.1.shard.assigned_groups_num < max_capacity {
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

        Err(Error::RemoteDiscovery(
            "Unexpected: Failed to find the least loaded CGW shard",
        ))
    }

    async fn assign_infra_group_to_cgw(&self, gid: i32) -> Result<i32> {
        // Delete key (if exists), recreate with new owner
        let _ = self.deassign_infra_group_to_cgw(gid).await;

        let dst_cgw_id: i32 = self.get_infra_group_cgw_assignee().await?;

        self.redis_client
            .send::<String>(resp_array![
                "HSET",
                format!("{REDIS_KEY_GID}{gid}"),
                REDIS_KEY_GID_VALUE_GID,
                gid.to_string(),
                REDIS_KEY_GID_VALUE_SHARD_ID,
                dst_cgw_id.to_string()
            ])
            .await?;

        self.gid_to_cgw_cache.write().await.insert(gid, dst_cgw_id);

        debug!("REDIS: assigned gid{gid} to shard{dst_cgw_id}");

        Ok(dst_cgw_id)
    }

    pub async fn deassign_infra_group_to_cgw(&self, gid: i32) -> Result<()> {
        self.redis_client
            .send::<i64>(resp_array!["DEL", format!("{REDIS_KEY_GID}{gid}")])
            .await?;

        debug!("REDIS: deassigned gid{gid} from controlled CGW");

        self.gid_to_cgw_cache.write().await.remove(&gid);

        Ok(())
    }

    pub async fn create_infra_group(&self, g: &CGWDBInfrastructureGroup) -> Result<i32> {
        //TODO: transaction-based insert/assigned_group_num update (DB)
        self.db_accessor.insert_new_infra_group(g).await?;

        let shard_id: i32 = match self.assign_infra_group_to_cgw(g.id).await {
            Ok(v) => v,
            Err(_e) => {
                let _ = self.db_accessor.delete_infra_group(g.id).await;
                return Err(Error::RemoteDiscovery("Assign group to CGW shard failed"));
            }
        };

        let rc = self.increment_cgw_assigned_groups_num(shard_id).await;
        rc?;

        Ok(shard_id)
    }

    pub async fn destroy_infra_group(
        &self,
        gid: i32,
        cache: Arc<RwLock<CGWDevicesCache>>,
    ) -> Result<()> {
        let cgw_id: Option<i32> = self.get_infra_group_owner_id(gid).await;
        if let Some(id) = cgw_id {
            let _ = self.deassign_infra_group_to_cgw(gid).await;
            let _ = self.decrement_cgw_assigned_groups_num(id).await;
        }

        //TODO: transaction-based insert/assigned_group_num update (DB)
        self.db_accessor.delete_infra_group(gid).await?;

        let mut devices_to_remove: Vec<MacAddress> = Vec::new();
        let mut device_cache = cache.write().await;
        for (key, device) in device_cache.iter_mut() {
            if device.get_device_group_id() == gid {
                if device.get_device_state() == CGWDeviceState::CGWDeviceConnected {
                    device.set_device_remains_in_db(false);
                    device.set_device_group_id(0);
                } else {
                    devices_to_remove.push(*key);
                }
            }
        }

        for key in devices_to_remove.iter() {
            device_cache.del_device(key);
        }

        device_cache.dump_devices_cache();

        Ok(())
    }

    pub async fn create_ifras_list(
        &self,
        gid: i32,
        infras: Vec<MacAddress>,
        cache: Arc<RwLock<CGWDevicesCache>>,
    ) -> Result<()> {
        // TODO: assign list to shards; currently - only created bulk, no assignment
        let mut futures = Vec::with_capacity(infras.len());
        // Results store vec of MACs we failed to add
        let mut failed_infras: Vec<MacAddress> = Vec::with_capacity(futures.len());
        for x in infras.iter() {
            let db_accessor_clone = self.db_accessor.clone();
            let infra = CGWDBInfra {
                mac: *x,
                infra_group_id: gid,
            };

            futures.push(tokio::spawn(async move {
                if (db_accessor_clone.insert_new_infra(&infra).await).is_err() {
                    Err(infra.mac)
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
                        let device_mac = infras[i];

                        if let Some(device) = devices_cache.get_device(&device_mac) {
                            device.set_device_group_id(gid);
                            device.set_device_remains_in_db(true);
                        } else {
                            devices_cache.add_device(
                                &device_mac,
                                &CGWDevice::new(
                                    CGWDeviceState::CGWDeviceDisconnected,
                                    gid,
                                    true,
                                    Default::default(),
                                ),
                            );
                        }
                        devices_cache.dump_devices_cache();
                    }
                }
                Err(_) => {
                    failed_infras.push(infras[i]);
                }
            }
        }

        if !failed_infras.is_empty() {
            return Err(Error::RemoteDiscoveryFailedInfras(failed_infras));
        }

        Ok(())
    }

    pub async fn destroy_ifras_list(
        &self,
        _gid: i32,
        infras: Vec<MacAddress>,
        cache: Arc<RwLock<CGWDevicesCache>>,
    ) -> Result<()> {
        let mut futures = Vec::with_capacity(infras.len());
        // Results store vec of MACs we failed to add
        let mut failed_infras: Vec<MacAddress> = Vec::with_capacity(futures.len());
        for x in infras.iter() {
            let db_accessor_clone = self.db_accessor.clone();
            let mac = *x;

            futures.push(tokio::spawn(async move {
                if (db_accessor_clone.delete_infra(mac).await).is_err() {
                    Err(mac)
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
                        let device_mac = infras[i];
                        if let Some(device) = devices_cache.get_device(&device_mac) {
                            if device.get_device_state() == CGWDeviceState::CGWDeviceConnected {
                                device.set_device_remains_in_db(false);
                                device.set_device_group_id(0);
                            } else {
                                devices_cache.del_device(&device_mac);
                            }
                            devices_cache.dump_devices_cache();
                        }
                    }
                }
                Err(_) => {
                    failed_infras.push(infras[i]);
                }
            }
        }

        if !failed_infras.is_empty() {
            return Err(Error::RemoteDiscoveryFailedInfras(failed_infras));
        }

        Ok(())
    }

    pub async fn relay_request_stream_to_remote_cgw(
        &self,
        shard_id: i32,
        stream: Vec<(String, String)>,
    ) -> Result<()> {
        // try to use internal cache first
        if let Some(cl) = self.remote_cgws_map.read().await.get(&shard_id) {
            if let Err(_e) = cl.client.relay_request_stream(stream).await {
                error!(
                    "Failed to relay message. CGW{} seems to be unreachable at [{}:{}]",
                    shard_id, cl.shard.server_host, cl.shard.server_port
                );
            }

            return Ok(());
        }

        // then try to use redis
        let _ = self.sync_remote_cgw_map().await;
        if let Some(cl) = self.remote_cgws_map.read().await.get(&shard_id) {
            if let Err(_e) = cl.client.relay_request_stream(stream).await {
                error!(
                    "Failed to relay message. CGW{} seems to be unreachable at [{}:{}]",
                    shard_id, cl.shard.server_host, cl.shard.server_port
                );
            }
            return Ok(());
        }

        error!("No suitable CGW instance #{shard_id} was discovered, cannot relay msg");
        Err(Error::RemoteDiscovery(
            "No suitable CGW instance was discovered, cannot relay msg",
        ))
    }

    pub async fn rebalance_all_groups(&self) -> Result<u32> {
        warn!("Executing group rebalancing procedure");

        let groups = match self.db_accessor.get_all_infra_groups().await {
            Some(list) => list,
            None => {
                warn!("Tried to execute rebalancing when 0 groups created in DB");
                return Err(Error::RemoteDiscovery(
                    "Cannot do rebalancing due to absence of any groups created in DB",
                ));
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

    pub async fn cleanup_redis(&self) {
        debug!("Remove from Redis shard id {}", self.local_shard_id);
        // We are on de-init stage - ignore any errors on Redis clean-up
        let _ = self
            .redis_client
            .send::<i32>(resp_array![
                "DEL",
                format!("{REDIS_KEY_SHARD_ID_PREFIX}{}", self.local_shard_id)
            ])
            .await;
    }
}
