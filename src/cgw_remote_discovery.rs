use crate::{
    cgw_app_args::CGWRedisArgs,
    cgw_db_accessor::{CGWDBAccessor, CGWDBInfra, CGWDBInfrastructureGroup},
    cgw_device::{CGWDevice, CGWDeviceState, CGWDeviceType},
    cgw_devices_cache::CGWDevicesCache,
    cgw_errors::{Error, Result},
    cgw_metrics::{
        CGWMetrics, CGWMetricsCounterOpType, CGWMetricsCounterType, CGWMetricsHealthComponent,
        CGWMetricsHealthComponentStatus,
    },
    cgw_remote_client::CGWRemoteClient,
    cgw_tls::cgw_read_root_certs_dir,
    AppArgs,
};

use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use redis::{
    aio::MultiplexedConnection, Client, ConnectionInfo, RedisConnectionInfo, RedisResult,
    TlsCertificates, ToRedisArgs,
};

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
    redis_client: MultiplexedConnection,
    gid_to_cgw_cache: Arc<RwLock<HashMap<i32, i32>>>,
    remote_cgws_map: Arc<RwLock<HashMap<i32, CGWRemoteIface>>>,
    local_shard_id: i32,
}

async fn cgw_create_redis_client(redis_args: &CGWRedisArgs) -> Result<Client> {
    let redis_client_info = ConnectionInfo {
        addr: match redis_args.redis_tls {
            true => redis::ConnectionAddr::TcpTls {
                host: redis_args.redis_host.clone(),
                port: redis_args.redis_port,
                insecure: true,
                tls_params: None,
            },
            false => {
                redis::ConnectionAddr::Tcp(redis_args.redis_host.clone(), redis_args.redis_port)
            }
        },

        redis: RedisConnectionInfo {
            username: redis_args.redis_username.clone(),
            password: redis_args.redis_password.clone(),
            ..Default::default()
        },
    };

    match redis_args.redis_tls {
        true => {
            let root_cert = cgw_read_root_certs_dir().await.ok();

            let tls_certs: TlsCertificates = TlsCertificates {
                client_tls: None,
                root_cert,
            };

            match redis::Client::build_with_tls(redis_client_info, tls_certs) {
                Ok(client) => Ok(client),
                Err(e) => Err(Error::Redis(format!("Failed to start Redis Client: {}", e))),
            }
        }
        false => match redis::Client::open(redis_client_info) {
            Ok(client) => Ok(client),
            Err(e) => Err(Error::Redis(format!("Failed to start Redis Client: {}", e))),
        },
    }
}

impl CGWRemoteDiscovery {
    pub async fn new(app_args: &AppArgs) -> Result<Self> {
        debug!(
            "Trying to create redis db connection ({}:{})",
            app_args.redis_args.redis_host, app_args.redis_args.redis_port
        );

        let redis_client = match cgw_create_redis_client(&app_args.redis_args).await {
            Ok(c) => c,
            Err(e) => {
                error!(
                    "Can't create CGW Remote Discovery client: Redis client create failed ({:?})",
                    e
                );
                return Err(Error::RemoteDiscovery("Redis client create failed"));
            }
        };

        let redis_client = match redis_client
            .get_multiplexed_tokio_connection_with_response_timeouts(
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .await
        {
            Ok(conn) => conn,
            Err(e) => {
                error!(
                    "Can't create CGW Remote Discovery client: Get Redis async connection failed ({})",
                    e
                );
                return Err(Error::RemoteDiscovery("Redis client create failed"));
            }
        };

        let db_accessor = match CGWDBAccessor::new(&app_args.db_args).await {
            Ok(c) => c,
            Err(e) => {
                error!(
                    "Can't create CGW Remote Discovery client: DB Accessor create failed ({:?})",
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
            error!("Can't create CGW Remote Discovery client: Can't pull records data from REDIS (wrong redis host/port?) ({:?})", e);
            return Err(Error::RemoteDiscovery(
                "Failed to sync (sync_gid_to_cgw_map) gid to cgw map",
            ));
        }
        if let Err(e) = rc.sync_remote_cgw_map().await {
            error!("Can't create CGW Remote Discovery client: Can't pull records data from REDIS (wrong redis host/port?) ({:?})", e);
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
                server_host: app_args.grpc_args.grpc_public_host.clone(),
                server_port: app_args.grpc_args.grpc_public_port,
                assigned_groups_num: 0i32,
                capacity: 1000i32,
                threshold: 50i32,
            };

            let redis_req_data: Vec<String> = redisdb_shard_info.into();
            let mut con = rc.redis_client.clone();

            let res: RedisResult<()> = redis::cmd("DEL")
                .arg(format!("{REDIS_KEY_SHARD_ID_PREFIX}{}", app_args.cgw_id))
                .query_async(&mut con)
                .await;
            if res.is_err() {
                warn!(
                    "Failed to destroy record about shard in REDIS, first launch? ({})",
                    res.err().unwrap()
                );
            }

            let res: RedisResult<()> = redis::cmd("HSET")
                .arg(format!("{REDIS_KEY_SHARD_ID_PREFIX}{}", app_args.cgw_id))
                .arg(redis_req_data.to_redis_args())
                .query_async(&mut con)
                .await;
            if res.is_err() {
                error!("Can't create CGW Remote Discovery client: Failed to create record about shard in REDIS: {}", res.err().unwrap());
                return Err(Error::RemoteDiscovery(
                    "Failed to create record about shard in REDIS",
                ));
            }
        }

        if let Err(e) = rc.sync_gid_to_cgw_map().await {
            error!(
                "Can't create CGW Remote Discovery client: Can't pull records data from REDIS: {:?}",
                e
            );
            return Err(Error::RemoteDiscovery(
                "Failed to sync (sync_gid_to_cgw_map) gid to cgw map",
            ));
        }
        if let Err(e) = rc.sync_remote_cgw_map().await {
            error!(
                "Can't create CGW Remote Discovery client: Can't pull records data from REDIS: {:?}",
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

        info!("Connection to REDIS DB has been established!");

        Ok(rc)
    }

    pub async fn sync_gid_to_cgw_map(&self) -> Result<()> {
        let mut lock = self.gid_to_cgw_cache.write().await;

        // Clear hashmap
        lock.clear();
        let mut con = self.redis_client.clone();

        let redis_keys: Vec<String> = match redis::cmd("KEYS")
            .arg(format!("{REDIS_KEY_GID}*"))
            .query_async(&mut con)
            .await
        {
            Err(e) => {
                error!("Failed to sync gid to cgw map:\n{}", e);
                return Err(Error::RemoteDiscovery("Failed to get KEYS list from REDIS"));
            }
            Ok(keys) => keys,
        };

        for key in redis_keys {
            let gid: i32 = match redis::cmd("HGET")
                .arg(&key)
                .arg(REDIS_KEY_GID_VALUE_GID)
                .query_async(&mut con)
                .await
            {
                Ok(gid) => gid,
                Err(e) => {
                    warn!("Found proper key '{key}' entry, but failed to fetch GID from it:\n{e}");
                    continue;
                }
            };

            let shard_id: i32 = match redis::cmd("HGET")
                .arg(&key)
                .arg(REDIS_KEY_GID_VALUE_SHARD_ID)
                .query_async(&mut con)
                .await
            {
                Ok(shard_id) => shard_id,
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
                        CGWDeviceType::default(),
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

        let mut con = self.redis_client.clone();
        let redis_keys: Vec<String> = match redis::cmd("KEYS")
            .arg(format!("{REDIS_KEY_SHARD_ID_PREFIX}*"))
            .query_async(&mut con)
            .await
        {
            Ok(keys) => keys,
            Err(e) => {
                error!(
                    "Can't sync remote CGW map: Failed to get shard record in REDIS: {}",
                    e
                );
                return Err(Error::RemoteDiscovery("Failed to get KEYS list from REDIS"));
            }
        };

        for key in redis_keys {
            let res: RedisResult<Vec<String>> =
                redis::cmd("HGETALL").arg(&key).query_async(&mut con).await;

            match res {
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

        let mut con = self.redis_client.clone();
        let res: RedisResult<()> = redis::cmd("HINCRBY")
            .arg(format!("{}{cgw_id}", REDIS_KEY_SHARD_ID_PREFIX))
            .arg(REDIS_KEY_SHARD_VALUE_ASSIGNED_G_NUM)
            .arg("1")
            .query_async(&mut con)
            .await;
        if res.is_err() {
            error!(
                "Failed to increment assigned group number:\n{}",
                res.err().unwrap()
            );
            return Err(Error::RemoteDiscovery(
                "Failed to increment assigned group number",
            ));
        }

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

        let mut con = self.redis_client.clone();
        let res: RedisResult<()> = redis::cmd("HINCRBY")
            .arg(format!("{}{cgw_id}", REDIS_KEY_SHARD_ID_PREFIX))
            .arg(REDIS_KEY_SHARD_VALUE_ASSIGNED_G_NUM)
            .arg("-1")
            .query_async(&mut con)
            .await;
        if res.is_err() {
            error!(
                "Failed to decrement assigned group number:\n{}",
                res.err().unwrap()
            );
            return Err(Error::RemoteDiscovery(
                "Failed to decrement assigned groups number",
            ));
        }

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

        let mut con = self.redis_client.clone();
        let res: RedisResult<()> = redis::cmd("HSET")
            .arg(format!("{REDIS_KEY_GID}{gid}"))
            .arg(REDIS_KEY_GID_VALUE_GID)
            .arg(gid.to_string())
            .arg(REDIS_KEY_GID_VALUE_SHARD_ID)
            .arg(dst_cgw_id.to_string())
            .query_async(&mut con)
            .await;

        if res.is_err() {
            error!(
                "Failed to assign infra group {} to cgw {}:\n{}",
                gid,
                dst_cgw_id,
                res.err().unwrap()
            );
            return Err(Error::RemoteDiscovery(
                "Failed to assign infra group to cgw",
            ));
        }

        self.gid_to_cgw_cache.write().await.insert(gid, dst_cgw_id);

        debug!("REDIS: assigned gid{gid} to shard{dst_cgw_id}");

        Ok(dst_cgw_id)
    }

    pub async fn deassign_infra_group_to_cgw(&self, gid: i32) -> Result<()> {
        let mut con = self.redis_client.clone();
        let res: RedisResult<()> = redis::cmd("DEL")
            .arg(format!("{REDIS_KEY_GID}{gid}"))
            .query_async(&mut con)
            .await;

        if res.is_err() {
            error!(
                "Failed to deassign infra group {}:\n{}",
                gid,
                res.err().unwrap()
            );
            return Err(Error::RemoteDiscovery(
                "Failed to deassign infra group to cgw",
            ));
        }

        debug!("REDIS: deassigned gid {gid} from controlled CGW");

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

                        if let Some(device) = devices_cache.get_device_mut(&device_mac) {
                            device.set_device_group_id(gid);
                            device.set_device_remains_in_db(true);
                        } else {
                            devices_cache.add_device(
                                &device_mac,
                                &CGWDevice::new(
                                    CGWDeviceType::default(),
                                    CGWDeviceState::CGWDeviceDisconnected,
                                    gid,
                                    true,
                                    Default::default(),
                                ),
                            );
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
                        if let Some(device) = devices_cache.get_device_mut(&device_mac) {
                            if device.get_device_state() == CGWDeviceState::CGWDeviceConnected {
                                device.set_device_remains_in_db(false);
                                device.set_device_group_id(0);
                            } else {
                                devices_cache.del_device(&device_mac);
                            }
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

        let mut con = self.redis_client.clone();
        for (cgw_id, _val) in self.remote_cgws_map.read().await.iter() {
            let res: RedisResult<()> = redis::cmd("HSET")
                .arg(format!("{}{cgw_id}", REDIS_KEY_SHARD_ID_PREFIX))
                .arg(REDIS_KEY_SHARD_VALUE_ASSIGNED_G_NUM)
                .arg("0")
                .query_async(&mut con)
                .await;
            if res.is_err() {
                warn!(
                    "Failed to reset CGW{cgw_id} assigned group num count, e:{}",
                    res.err().unwrap()
                );
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
        let mut con = self.redis_client.clone();
        let _res: RedisResult<()> = redis::cmd("DEL")
            .arg(format!(
                "{REDIS_KEY_SHARD_ID_PREFIX}{}",
                self.local_shard_id
            ))
            .query_async(&mut con)
            .await;
    }
}
