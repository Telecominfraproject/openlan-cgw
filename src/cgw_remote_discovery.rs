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
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use redis::{
    aio::MultiplexedConnection, Client, ConnectionInfo, RedisConnectionInfo, RedisResult,
    TlsCertificates, ToRedisArgs,
};

use eui48::MacAddress;

use tokio::sync::RwLock;

use chrono::Utc;

// Used in remote lookup
static REDIS_KEY_SHARD_ID_PREFIX: &str = "shard_id_";
static REDIS_KEY_SHARD_ID_FIELDS_NUM: usize = 12;
static REDIS_KEY_SHARD_VALUE_ASSIGNED_G_NUM: &str = "assigned_groups_num";

// Used in group assign / reassign
static REDIS_KEY_GID: &str = "group_id_";
static REDIS_KEY_GID_VALUE_GID: &str = "gid";
static REDIS_KEY_GID_VALUE_SHARD_ID: &str = "shard_id";
static REDIS_KEY_GID_VALUE_INFRAS_CAPACITY: &str = "infras_capacity";
static REDIS_KEY_GID_VALUE_INFRAS_ASSIGNED: &str = "infras_assigned";

const CGW_REDIS_DEVICES_CACHE_DB: u32 = 1;

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
            error!("Unexpected size of parsed vector! At least {REDIS_KEY_SHARD_ID_FIELDS_NUM} expected!");
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
    redis_infra_cache_client: MultiplexedConnection,
    gid_to_cgw_cache: Arc<RwLock<HashMap<i32, i32>>>,
    remote_cgws_map: Arc<RwLock<HashMap<i32, CGWRemoteIface>>>,
    local_shard_id: i32,
}

pub async fn cgw_create_redis_client(redis_args: &CGWRedisArgs) -> Result<Client> {
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
                Err(e) => Err(Error::Redis(format!(
                    "Failed to start Redis client! Error: {e}"
                ))),
            }
        }
        false => match redis::Client::open(redis_client_info) {
            Ok(client) => Ok(client),
            Err(e) => Err(Error::Redis(format!(
                "Failed to start Redis client! Error: {e}"
            ))),
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
                    "Can't create CGW Remote Discovery client! Redis client create failed! Error: {e}"
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
                    "Can't create CGW Remote Discovery client! Get Redis async connection failed! Error: {e}"
                );
                return Err(Error::RemoteDiscovery("Redis client create failed"));
            }
        };

        /* Start Redis Infra Cache Client */
        let redis_infra_cache_client = match cgw_create_redis_client(&app_args.redis_args).await {
            Ok(c) => c,
            Err(e) => {
                error!(
                    "Can't create CGW Remote Discovery client! Redis infra cache client create failed! Error: {e}"
                );
                return Err(Error::RemoteDiscovery(
                    "Redis infra cache client create failed",
                ));
            }
        };

        let mut redis_infra_cache_client = match redis_infra_cache_client
            .get_multiplexed_tokio_connection_with_response_timeouts(
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .await
        {
            Ok(conn) => conn,
            Err(e) => {
                error!(
                    "Can't create CGW Remote Discovery client! Get Redis infra cache async connection failed! Error: {e}"
                );
                return Err(Error::RemoteDiscovery(
                    "Redis infra cache client create failed",
                ));
            }
        };

        let res: RedisResult<()> = redis::cmd("SELECT")
            .arg(CGW_REDIS_DEVICES_CACHE_DB.to_string())
            .query_async(&mut redis_infra_cache_client)
            .await;
        match res {
            Ok(_) => debug!(
                "Switched Redis infra cache client to Redis Database {CGW_REDIS_DEVICES_CACHE_DB}"
            ),
            Err(e) => {
                if e.is_io_error() {
                    Self::set_redis_health_state_not_ready(e.to_string()).await;
                }
                warn!(
                    "Failed to switch to Redis Database {CGW_REDIS_DEVICES_CACHE_DB}! Error: {e}"
                );
                return Err(Error::RemoteDiscovery("Failed to switch Redis Database"));
            }
        };

        /* End Redis Infra Cache Client */

        let db_accessor = match CGWDBAccessor::new(&app_args.db_args).await {
            Ok(c) => c,
            Err(e) => {
                error!(
                    "Can't create CGW Remote Discovery client! DB Accessor create failed! Error: {e}"
                );
                return Err(Error::RemoteDiscovery("DB Accessor create failed"));
            }
        };

        let rc = CGWRemoteDiscovery {
            db_accessor: Arc::new(db_accessor),
            redis_client,
            redis_infra_cache_client,
            gid_to_cgw_cache: Arc::new(RwLock::new(HashMap::new())),
            local_shard_id: app_args.cgw_id,
            remote_cgws_map: Arc::new(RwLock::new(HashMap::new())),
        };

        if let Err(e) = rc.set_redis_last_update_timestamp().await {
            error!("Can't create CGW Remote Discovery client! Failed to update Redis timestamp! Error: {e}");
            return Err(Error::RemoteDiscovery("Failed to update Redis timestamp"));
        }

        let assigned_groups_num: i32 = match rc.sync_gid_to_cgw_map().await {
            Ok(assigned_groups) => assigned_groups,
            Err(e) => {
                error!("Can't create CGW Remote Discovery client: Can't pull records data from REDIS (wrong redis host/port?) ({:?})", e);
                return Err(Error::RemoteDiscovery(
                    "Failed to sync (sync_gid_to_cgw_map) gid to cgw map",
                ));
            }
        };

        debug!(
            "Found {assigned_groups_num} assigned to CGW ID {}",
            app_args.cgw_id
        );

        if let Err(e) = rc.sync_remote_cgw_map().await {
            error!("Can't create CGW Remote Discovery client! Failed to sync remote CGW map! Error: {e}");
            return Err(Error::RemoteDiscovery(
                "Failed to sync (sync_remote_cgw_map) remote CGW info from REDIS",
            ));
        }

        let redisdb_shard_info = CGWREDISDBShard {
            id: app_args.cgw_id,
            server_host: app_args.grpc_args.grpc_public_host.clone(),
            server_port: app_args.grpc_args.grpc_public_port,
            assigned_groups_num,
            capacity: app_args.cgw_groups_capacity,
            threshold: app_args.cgw_groups_threshold,
        };

        CGWMetrics::get_ref().change_counter(
            CGWMetricsCounterType::GroupsCapacity,
            CGWMetricsCounterOpType::Set(app_args.cgw_groups_capacity.into()),
        );

        CGWMetrics::get_ref().change_counter(
            CGWMetricsCounterType::GroupsThreshold,
            CGWMetricsCounterOpType::Set(app_args.cgw_groups_threshold.into()),
        );

        let redis_req_data: Vec<String> = redisdb_shard_info.into();
        let mut con = rc.redis_client.clone();

        let res: RedisResult<()> = redis::cmd("DEL")
            .arg(format!("{REDIS_KEY_SHARD_ID_PREFIX}{}", app_args.cgw_id))
            .query_async(&mut con)
            .await;
        if let Err(e) = res {
            if e.is_io_error() {
                Self::set_redis_health_state_not_ready(e.to_string()).await;
            }
            warn!("Failed to destroy record about shard in REDIS! Error: {e}");
        }

        let res: RedisResult<()> = redis::cmd("HSET")
            .arg(format!("{REDIS_KEY_SHARD_ID_PREFIX}{}", app_args.cgw_id))
            .arg(redis_req_data.to_redis_args())
            .query_async(&mut con)
            .await;
        if let Err(e) = res {
            if e.is_io_error() {
                Self::set_redis_health_state_not_ready(e.to_string()).await;
            }
            error!("Can't create CGW Remote Discovery client! Failed to create record about shard in REDIS! Error: {e}");
            return Err(Error::RemoteDiscovery(
                "Failed to create record about shard in REDIS",
            ));
        }

        if let Err(e) = rc.sync_gid_to_cgw_map().await {
            error!(
                "Can't create CGW Remote Discovery client! Failed to sync GID to CGW map! Error: {e}"
            );
            return Err(Error::RemoteDiscovery(
                "Failed to sync (sync_gid_to_cgw_map) gid to cgw map",
            ));
        }
        if let Err(e) = rc.sync_remote_cgw_map().await {
            error!(
                "Can't create CGW Remote Discovery client! Failed to sync remote CGW map! Error: {e}"
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

    pub async fn sync_gid_to_cgw_map(&self) -> Result<i32> {
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
                if e.is_io_error() {
                    Self::set_redis_health_state_not_ready(e.to_string()).await;
                }
                error!("Failed to sync gid to cgw map! Error: {e}");
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
                    if e.is_io_error() {
                        Self::set_redis_health_state_not_ready(e.to_string()).await;
                    }
                    warn!("Found proper key '{key}' entry, but failed to fetch GID from it! Error: {e}");
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
                    if e.is_io_error() {
                        Self::set_redis_health_state_not_ready(e.to_string()).await;
                    }
                    warn!("Found proper key '{key}' entry, but failed to fetch SHARD_ID from it! Error: {e}");
                    continue;
                }
            };

            debug!("Found group {key}, gid: {gid}, shard_id: {shard_id}");

            match lock.insert(gid, shard_id) {
                None => continue,
                Some(_v) => warn!(
                    "Populated gid_to_cgw_map with previous value being already set, unexpected!"
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

        Ok(local_cgw_gid_num as i32)
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
                if e.is_io_error() {
                    Self::set_redis_health_state_not_ready(e.to_string()).await;
                }
                error!(
                    "Can't sync remote CGW map! Failed to get shard record in REDIS! Error: {e}"
                );
                return Err(Error::RemoteDiscovery("Failed to get KEYS list from REDIS"));
            }
        };

        for key in redis_keys {
            let res: RedisResult<Vec<String>> =
                redis::cmd("HGETALL").arg(&key).query_async(&mut con).await;

            match res {
                Ok(res) => {
                    let shard: CGWREDISDBShard = CGWREDISDBShard::from(res);
                    if shard == CGWREDISDBShard::default() {
                        warn!("Failed to parse CGWREDISDBShard, key: {key}!");
                        continue;
                    }

                    let endpoint_str = String::from("http://")
                        + &shard.server_host
                        + ":"
                        + &shard.server_port.to_string();
                    let cgw_iface = CGWRemoteIface {
                        shard,
                        client: CGWRemoteClient::new(endpoint_str)?,
                    };
                    lock.insert(cgw_iface.shard.id, cgw_iface);
                }
                Err(e) => {
                    if e.is_io_error() {
                        Self::set_redis_health_state_not_ready(e.to_string()).await;
                    }
                    warn!("Found proper key '{key}' entry, but failed to fetch Shard info from it! Error: {e}");
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
        if let Err(e) = self.sync_gid_to_cgw_map().await {
            error!("Failed to sync GID to CGW map! Error: {e}");
        }

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
        if let Err(e) = res {
            if e.is_io_error() {
                Self::set_redis_health_state_not_ready(e.to_string()).await;
            }
            error!("Failed to increment assigned groups number! Error: {e}");
            return Err(Error::RemoteDiscovery(
                "Failed to increment assigned groups number",
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
        if let Err(e) = res {
            if e.is_io_error() {
                Self::set_redis_health_state_not_ready(e.to_string()).await;
            }
            error!("Failed to decrement assigned groups number! Error: {e}");
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

    async fn increment_group_assigned_infras_num(
        &self,
        gid: i32,
        increment_value: i32,
    ) -> Result<()> {
        debug!(
            "Incrementing assigned infras num group_id_{gid}: increment value: {increment_value}"
        );

        let mut con = self.redis_client.clone();
        let res: RedisResult<()> = redis::cmd("HINCRBY")
            .arg(format!("{}{gid}", REDIS_KEY_GID))
            .arg(REDIS_KEY_GID_VALUE_INFRAS_ASSIGNED)
            .arg(&increment_value.to_string())
            .query_async(&mut con)
            .await;
        if let Err(e) = res {
            if e.is_io_error() {
                Self::set_redis_health_state_not_ready(e.to_string()).await;
            }
            error!("Failed to increment assigned infras number! Error: {e}");
            return Err(Error::RemoteDiscovery(
                "Failed to increment assigned infras number",
            ));
        }

        debug!("Incrementing assigned infras num group_id_{gid}: increment value: {increment_value} - metrics");
        CGWMetrics::get_ref()
            .change_group_counter(
                gid,
                CGWMetricsCounterType::GroupInfrasAssignedNum,
                CGWMetricsCounterOpType::IncBy(increment_value as i64),
            )
            .await;

        Ok(())
    }

    async fn decrement_group_assigned_infras_num(
        &self,
        gid: i32,
        decrement_value: i32,
    ) -> Result<()> {
        debug!(
            "Decrementing assigned infras num group_id_{gid}: decrement_value: {decrement_value}"
        );

        let mut con = self.redis_client.clone();
        let res: RedisResult<()> = redis::cmd("HINCRBY")
            .arg(format!("{}{gid}", REDIS_KEY_GID))
            .arg(REDIS_KEY_GID_VALUE_INFRAS_ASSIGNED)
            .arg(&(-decrement_value).to_string())
            .query_async(&mut con)
            .await;
        if let Err(e) = res {
            if e.is_io_error() {
                Self::set_redis_health_state_not_ready(e.to_string()).await;
            }
            error!("Failed to decrement assigned infras number! Error: {e}");
            return Err(Error::RemoteDiscovery(
                "Failed to decrement assigned infras number",
            ));
        }

        CGWMetrics::get_ref()
            .change_group_counter(
                gid,
                CGWMetricsCounterType::GroupInfrasAssignedNum,
                CGWMetricsCounterOpType::DecBy(decrement_value as i64),
            )
            .await;

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
                debug!("Found CGW shard to assign group to id {}", x.1.shard.id);
                return Ok(x.1.shard.id);
            }
        }

        Err(Error::RemoteDiscovery(
            "Unexpected: Failed to find the least loaded CGW shard",
        ))
    }

    async fn validate_infra_group_cgw_assignee(&self, shard_id: i32) -> Result<i32> {
        let lock = self.remote_cgws_map.read().await;

        match lock.get(&shard_id) {
            Some(instance) => {
                let max_capacity: i32 = instance.shard.capacity + instance.shard.threshold;
                if instance.shard.assigned_groups_num < max_capacity {
                    debug!("Found CGW shard to assign group to id {}", shard_id);
                    Ok(shard_id)
                } else {
                    Err(Error::RemoteDiscovery(
                        "Unexpected: Failed to find the least loaded CGW shard",
                    ))
                }
            }
            None => Err(Error::RemoteDiscovery(
                "Unexpected: Failed to find CGW shard",
            )),
        }
    }

    async fn assign_infra_group_to_cgw(
        &self,
        gid: i32,
        shard_id: Option<i32>,
        infras_capacity: i32,
        infras_assigned: i32,
    ) -> Result<i32> {
        // Delete key (if exists), recreate with new owner
        if let Err(e) = self.deassign_infra_group_to_cgw(gid).await {
            error!("assign_infra_group_to_cgw: failed to deassign infra group to CGW! Error: {e}");
        }

        // Sync CGWs to get latest data
        if let Err(e) = self.sync_remote_cgw_map().await {
            error!("Can't create CGW Remote Discovery client! Failed to sync remote CGW map! Error: {e}");
            return Err(Error::RemoteDiscovery(
                "Failed to sync remote CGW info from REDIS",
            ));
        }

        let dst_cgw_id: i32 = match shard_id {
            Some(dest_shard_id) => {
                self.validate_infra_group_cgw_assignee(dest_shard_id)
                    .await?
            }
            None => self.get_infra_group_cgw_assignee().await?,
        };

        let mut con = self.redis_client.clone();
        let res: RedisResult<()> = redis::cmd("HSET")
            .arg(format!("{REDIS_KEY_GID}{gid}"))
            .arg(REDIS_KEY_GID_VALUE_GID)
            .arg(gid.to_string())
            .arg(REDIS_KEY_GID_VALUE_SHARD_ID)
            .arg(dst_cgw_id.to_string())
            .arg(REDIS_KEY_GID_VALUE_INFRAS_CAPACITY)
            .arg(infras_capacity.to_string())
            .arg(REDIS_KEY_GID_VALUE_INFRAS_ASSIGNED)
            .arg(infras_assigned.to_string())
            .query_async(&mut con)
            .await;

        if let Err(e) = res {
            if e.is_io_error() {
                Self::set_redis_health_state_not_ready(e.to_string()).await;
            }
            error!("Failed to assign infra group {gid} to cgw {dst_cgw_id}! Error: {e}");
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

        if let Err(e) = res {
            if e.is_io_error() {
                Self::set_redis_health_state_not_ready(e.to_string()).await;
            }
            error!("Failed to deassign infra group {gid}! Error: {e}");
            return Err(Error::RemoteDiscovery(
                "Failed to deassign infra group to cgw",
            ));
        }

        debug!("REDIS: deassign gid {gid} from controlled CGW");

        self.gid_to_cgw_cache.write().await.remove(&gid);

        Ok(())
    }

    pub async fn create_infra_group(
        &self,
        g: &CGWDBInfrastructureGroup,
        dest_shard_id: Option<i32>,
    ) -> Result<i32> {
        //TODO: transaction-based insert/assigned_group_num update (DB)
        self.db_accessor.insert_new_infra_group(g).await?;

        let shard_id: i32 = match self
            .assign_infra_group_to_cgw(g.id, dest_shard_id, g.reserved_size, g.actual_size)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                error!("Assign group to CGW shard failed! Error: {e}");
                if let Err(e) = self.db_accessor.delete_infra_group(g.id).await {
                    error!("Assign group to CGW shard failed! Failed to delete infra group ID {}! Error: {e}", g.id);
                }
                return Err(e);
            }
        };

        if let Err(e) = self.increment_cgw_assigned_groups_num(shard_id).await {
            error!(
                "create_infra_group: failed to decrement assigned groups num to CGW! Error: {e}"
            );
        };

        if let Err(e) = self.set_redis_last_update_timestamp().await {
            error!("create_infra_group: failed update Redis timestamp! Error: {e}");
        }

        Ok(shard_id)
    }

    pub async fn destroy_infra_group(
        &self,
        gid: i32,
        cache: Arc<RwLock<CGWDevicesCache>>,
    ) -> Result<()> {
        let cgw_id: Option<i32> = self.get_infra_group_owner_id(gid).await;
        if let Some(id) = cgw_id {
            if let Err(e) = self.deassign_infra_group_to_cgw(gid).await {
                error!("destroy_infra_group: failed to deassign infra group to CGW! Error: {e}");
            }
            if let Err(e) = self.decrement_cgw_assigned_groups_num(id).await {
                error!("destroy_infra_group: failed to decrement assigned groups num to CGW! Error: {e}");
            }

            if let Err(e) = self.set_redis_last_update_timestamp().await {
                error!("destroy_infra_group: failed to update Redis timestamp! Error: {e}");
            }
        }

        //TODO: transaction-based insert/assigned_group_num update (DB)
        self.db_accessor.delete_infra_group(gid).await?;

        let mut devices_to_remove: Vec<MacAddress> = Vec::new();
        let mut devices_to_update: Vec<(MacAddress, CGWDevice)> = Vec::new();
        let mut device_cache = cache.write().await;
        for (key, device) in device_cache.iter_mut() {
            if device.get_device_group_id() == gid {
                if device.get_device_state() == CGWDeviceState::CGWDeviceConnected {
                    device.set_device_remains_in_db(false);
                    device.set_device_group_id(0);
                    devices_to_update.push((*key, device.clone()));
                } else {
                    devices_to_remove.push(*key);
                }
            }
        }

        for key in devices_to_remove.iter() {
            device_cache.del_device(key);
            if let Err(e) = self.del_device_from_redis_cache(key).await {
                error!("{e}");
            }
        }

        for (mac, device) in devices_to_update {
            match serde_json::to_string(&device) {
                Ok(device_json) => {
                    if let Err(e) = self.add_device_to_redis_cache(&mac, &device_json).await {
                        error!("{e}");
                    }
                }
                Err(e) => {
                    error!("Failed to serialize device to json string! Error: {e}");
                }
            }
        }

        CGWMetrics::get_ref().delete_group_counter(gid).await;

        Ok(())
    }

    pub async fn create_infras_list(
        &self,
        gid: i32,
        infras: Vec<MacAddress>,
        cache: Arc<RwLock<CGWDevicesCache>>,
    ) -> Result<Vec<MacAddress>> {
        // TODO: assign list to shards; currently - only created bulk, no assignment
        let mut futures = Vec::with_capacity(infras.len());
        // Results store vec of MACs we failed to add

        let infras_capacity = match self.get_group_infras_capacity(gid).await {
            Ok(capacity) => capacity,
            Err(e) => {
                error!("Failed to create infras list! Error: {e}");
                return Err(Error::RemoteDiscoveryFailedInfras(infras));
            }
        };

        let infras_assigned = match self.get_group_infras_assigned_num(gid).await {
            Ok(assigned) => assigned,
            Err(e) => {
                error!("Failed to create infras list! Error: {e}");
                return Err(Error::RemoteDiscoveryFailedInfras(infras));
            }
        };

        if infras.len() as i32 + infras_assigned > infras_capacity {
            error!("Failed to create infras list - GID {gid} has no enough capacity!");
            return Err(Error::RemoteDiscoveryFailedInfras(infras));
        }

        let mut success_infras: Vec<MacAddress> = Vec::with_capacity(futures.len());
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

        let mut assigned_infras_num: i32 = 0;
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

                            match serde_json::to_string(device) {
                                Ok(device_json) => {
                                    if let Err(e) = self
                                        .add_device_to_redis_cache(&device_mac, &device_json)
                                        .await
                                    {
                                        error!("{e}");
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to serialize device to json string! Error: {e}");
                                }
                            }
                        } else {
                            let device = CGWDevice::new(
                                CGWDeviceType::default(),
                                CGWDeviceState::CGWDeviceDisconnected,
                                gid,
                                true,
                                Default::default(),
                            );
                            devices_cache.add_device(&device_mac, &device);

                            match serde_json::to_string(&device) {
                                Ok(device_json) => {
                                    if let Err(e) = self
                                        .add_device_to_redis_cache(&device_mac, &device_json)
                                        .await
                                    {
                                        error!("{e}");
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to serialize device to json string! Error: {e}");
                                }
                            }
                        }
                        success_infras.push(device_mac);
                        assigned_infras_num += 1;
                    }
                }
                Err(_) => {
                    failed_infras.push(infras[i]);
                }
            }
        }

        // Update assigned infras num
        if let Err(e) = self
            .increment_group_assigned_infras_num(gid, assigned_infras_num)
            .await
        {
            error!("create_infras_list: failed to increment assigned infras num! Error: {e}");
        }

        if !failed_infras.is_empty() {
            return Err(Error::RemoteDiscoveryFailedInfras(failed_infras));
        }

        Ok(success_infras)
    }

    pub async fn destroy_infras_list(
        &self,
        gid: i32,
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

        let mut removed_infras: i32 = 0;
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

                                match serde_json::to_string(device) {
                                    Ok(device_json) => {
                                        if let Err(e) = self
                                            .add_device_to_redis_cache(&device_mac, &device_json)
                                            .await
                                        {
                                            error!("{e}");
                                        }
                                    }
                                    Err(e) => {
                                        error!(
                                            "Failed to serialize device to json string! Error: {e}"
                                        );
                                    }
                                }
                            } else {
                                devices_cache.del_device(&device_mac);
                                if let Err(e) = self.del_device_from_redis_cache(&device_mac).await
                                {
                                    error!("{e}");
                                }
                            }
                        }
                        removed_infras += 1;
                    }
                }
                Err(_) => {
                    failed_infras.push(infras[i]);
                }
            }
        }

        // Update assigned infras num
        if let Err(e) = self
            .decrement_group_assigned_infras_num(gid, removed_infras)
            .await
        {
            error!("destroy_infras_list: failed to decrement assigned infras num! Error: {e}");
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
            if let Err(e) = cl.client.relay_request_stream(stream).await {
                error!(
                    "Failed to relay message! CGW{} seems to be unreachable at [{}:{}]! Error: {}",
                    shard_id, cl.shard.server_host, cl.shard.server_port, e
                );

                return Err(e);
            }

            return Ok(());
        }

        // then try to use redis
        if let Err(e) = self.sync_remote_cgw_map().await {
            error!("relay_request_stream_to_remote_cgw: failed to sync remote CGW map! Error: {e}");
        }

        if let Some(cl) = self.remote_cgws_map.read().await.get(&shard_id) {
            if let Err(e) = cl.client.relay_request_stream(stream).await {
                error!(
                    "Failed to relay message! CGW{} seems to be unreachable at [{}:{}]. Error: {}",
                    shard_id, cl.shard.server_host, cl.shard.server_port, e
                );
            }
            return Ok(());
        }

        error!("No suitable CGW instance #{shard_id} was discovered, cannot relay msg!");
        Err(Error::RemoteDiscovery(
            "No suitable CGW instance was discovered, cannot relay msg",
        ))
    }

    pub async fn rebalance_all_groups(&self) -> Result<u32> {
        warn!("Executing group rebalancing procedure!");

        let groups = match self.db_accessor.get_all_infra_groups().await {
            Some(list) => list,
            None => {
                warn!("Tried to execute rebalancing when 0 groups created in DB!");
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
            if let Err(e) = res {
                if e.is_io_error() {
                    Self::set_redis_health_state_not_ready(e.to_string()).await;
                }
                warn!("Failed to reset CGW{cgw_id} assigned group num count! Error: {e}");
            }
        }

        for i in groups.iter() {
            if let Err(e) = self.sync_remote_cgw_map().await {
                error!("rebalance_all_groups: failed to sync remote CGW map! Error: {e}");
            }
            if let Err(e) = self.sync_gid_to_cgw_map().await {
                error!("rebalance_all_groups: failed to sync GID to CGW map! Error: {e}");
            }

            let infras_assigned: i32 = match self.get_group_infras_assigned_num(i.id).await {
                Ok(infras_num) => infras_num,
                Err(e) => {
                    warn!("Failed to execute rebalancing! Error: {e}");
                    return Err(Error::RemoteDiscovery(
                        "Cannot do rebalancing due to absence of any groups created in DB",
                    ));
                }
            };

            match self
                .assign_infra_group_to_cgw(i.id, None, i.reserved_size, infras_assigned)
                .await
            {
                Ok(shard_id) => {
                    debug!("Rebalancing: assigned gid {} to shard {}", i.id, shard_id);
                    if let Err(e) = self.increment_cgw_assigned_groups_num(shard_id).await {
                        error!("rebalance_all_groups: failed to increment assigned groups num! Error: {e}");
                    }
                }
                Err(_e) => {}
            }
        }

        if let Err(e) = self.set_redis_last_update_timestamp().await {
            error!("rebalance_all_groups: failed update Redis timestamp! Error: {e}");
        }

        if let Err(e) = self.sync_remote_cgw_map().await {
            error!("rebalance_all_groups: failed to sync remote CGW map! Error: {e}");
        }
        if let Err(e) = self.sync_gid_to_cgw_map().await {
            error!("rebalance_all_groups: failed to sync GID to CGW! Error: {e}");
        }

        Ok(0u32)
    }

    pub async fn cleanup_redis(&self) {
        debug!("Remove from Redis shard id {}", self.local_shard_id);
        // We are on de-init stage - ignore any errors on Redis clean-up
        let mut con = self.redis_client.clone();
        let res: RedisResult<()> = redis::cmd("DEL")
            .arg(format!(
                "{REDIS_KEY_SHARD_ID_PREFIX}{}",
                self.local_shard_id
            ))
            .query_async(&mut con)
            .await;
        match res {
            Ok(_) => info!(
                "Successfully cleaned up Redis for shard id {}",
                self.local_shard_id
            ),
            Err(e) => {
                if e.is_io_error() {
                    Self::set_redis_health_state_not_ready(e.to_string()).await;
                }
                error!(
                    "Failed to cleanup Redis for shard id {}! Error: {}",
                    self.local_shard_id, e
                );
            }
        }
    }

    pub async fn get_group_infras_capacity(&self, gid: i32) -> Result<i32> {
        let mut con = self.redis_client.clone();

        let capacity: i32 = match redis::cmd("HGET")
            .arg(format!("{}{gid}", REDIS_KEY_GID))
            .arg(REDIS_KEY_GID_VALUE_INFRAS_CAPACITY)
            .query_async(&mut con)
            .await
        {
            Ok(cap) => cap,
            Err(e) => {
                if e.is_io_error() {
                    Self::set_redis_health_state_not_ready(e.to_string()).await;
                }
                warn!("Failed to get infras capacity for GID {gid}! Error: {e}");
                return Err(Error::RemoteDiscovery("Failed to get infras capacity"));
            }
        };

        Ok(capacity)
    }

    pub async fn get_group_infras_assigned_num(&self, gid: i32) -> Result<i32> {
        let mut con = self.redis_client.clone();

        let infras_assigned: i32 = match redis::cmd("HGET")
            .arg(format!("{}{gid}", REDIS_KEY_GID))
            .arg(REDIS_KEY_GID_VALUE_INFRAS_ASSIGNED)
            .query_async(&mut con)
            .await
        {
            Ok(cap) => cap,
            Err(e) => {
                if e.is_io_error() {
                    Self::set_redis_health_state_not_ready(e.to_string()).await;
                }
                warn!("Failed to get infras assigned number for GID {gid}! Error: {e}");
                return Err(Error::RemoteDiscovery(
                    "Failed to get group infras assigned number",
                ));
            }
        };

        Ok(infras_assigned)
    }

    pub async fn add_device_to_redis_cache(
        &self,
        device_mac: &MacAddress,
        device_json: &str,
    ) -> Result<()> {
        let mut con = self.redis_infra_cache_client.clone();

        let key = format!("shard_id_{}|{}", self.local_shard_id, device_mac);
        let res: RedisResult<()> = redis::cmd("SET")
            .arg(&key)
            .arg(device_json)
            .query_async(&mut con)
            .await;

        match res {
            Ok(_) => debug!("Added device to Redis cache: {device_json}"),
            Err(e) => {
                if e.is_io_error() {
                    Self::set_redis_health_state_not_ready(e.to_string()).await;
                }
                warn!("Failed to add device to Redis cache! Error: {e}");
                return Err(Error::RemoteDiscovery(
                    "Failed to add device to Redis cache",
                ));
            }
        };

        Ok(())
    }

    pub async fn del_device_from_redis_cache(&self, device_mac: &MacAddress) -> Result<()> {
        let mut con = self.redis_infra_cache_client.clone();

        let key = format!("shard_id_{}|{}", self.local_shard_id, device_mac);
        let res: RedisResult<()> = redis::cmd("DEL").arg(&key).query_async(&mut con).await;

        match res {
            Ok(_) => debug!("Removed device from Redis cache: {}", device_mac),
            Err(e) => {
                if e.is_io_error() {
                    Self::set_redis_health_state_not_ready(e.to_string()).await;
                }
                warn!(
                    "Failed to remove device {} from Redis cache! Error: {e}",
                    device_mac.to_hex_string()
                );
                return Err(Error::RemoteDiscovery(
                    "Failed to update Redis devices cache",
                ));
            }
        };

        Ok(())
    }

    pub async fn sync_devices_cache_with_redis(
        &self,
        cache: Arc<RwLock<CGWDevicesCache>>,
    ) -> Result<()> {
        // flush cache
        let mut devices_cache = cache.write().await;
        devices_cache.flush_all();

        let mut con = self.redis_infra_cache_client.clone();
        let key = format!("shard_id_{}|*", self.local_shard_id);
        let redis_keys: Vec<String> = match redis::cmd("KEYS").arg(&key).query_async(&mut con).await
        {
            Err(e) => {
                if e.is_io_error() {
                    Self::set_redis_health_state_not_ready(e.to_string()).await;
                }
                error!(
                    "Failed to get devices cache from Redis for shard id {}, Error: {}",
                    self.local_shard_id, e
                );
                return Err(Error::RemoteDiscovery(
                    "Failed to get devices cache from Redis",
                ));
            }
            Ok(keys) => keys,
        };

        for key in redis_keys {
            let device_str: String = match redis::cmd("GET").arg(&key).query_async(&mut con).await {
                Ok(dev) => dev,
                Err(e) => {
                    if e.is_io_error() {
                        Self::set_redis_health_state_not_ready(e.to_string()).await;
                    }
                    error!(
                        "Failed to get devices cache from Redis for shard id {}, Error: {}",
                        self.local_shard_id, e
                    );
                    return Err(Error::RemoteDiscovery(
                        "Failed to get devices cache from Redis",
                    ));
                }
            };

            let mut splitted_key = key.split_terminator('|');
            let _shard_id = splitted_key.next();
            let device_mac = match splitted_key.next() {
                Some(mac) => match MacAddress::from_str(mac) {
                    Ok(mac_address) => mac_address,
                    Err(e) => {
                        error!(
                            "Failed to parse device mac address from key {}! Error: {}",
                            self.local_shard_id, e
                        );
                        return Err(Error::RemoteDiscovery(
                            "Failed to parse device mac address from key",
                        ));
                    }
                },
                None => {
                    error!(
                        "Failed to get device mac address from key {}!",
                        self.local_shard_id,
                    );
                    return Err(Error::RemoteDiscovery(
                        "Failed to get device mac address from key",
                    ));
                }
            };

            match serde_json::from_str(&device_str) {
                Ok(dev) => {
                    devices_cache.add_device(&device_mac, &dev);
                    CGWMetrics::get_ref()
                        .change_group_counter(
                            dev.get_device_group_id(),
                            CGWMetricsCounterType::GroupInfrasAssignedNum,
                            CGWMetricsCounterOpType::Inc,
                        )
                        .await;
                }
                Err(e) => {
                    error!("Failed to deserialize device from Redis cache! Error: {e}");
                    return Err(Error::RemoteDiscovery(
                        "Failed to deserialize device from Redis cache",
                    ));
                }
            };
        }

        Ok(())
    }

    pub async fn sync_devices_cache(&self) -> Result<()> {
        if let Some(infras_list) = self.db_accessor.get_all_infras().await {
            let mut con = self.redis_infra_cache_client.clone();
            let mut redis_keys: Vec<String> = match redis::cmd("KEYS")
                .arg(&format!("shard_id_{}|*", self.local_shard_id))
                .query_async(&mut con)
                .await
            {
                Err(e) => {
                    if e.is_io_error() {
                        Self::set_redis_health_state_not_ready(e.to_string()).await;
                    }
                    error!(
                        "Failed to get devices cache from Redis for shard id {}, Error: {}",
                        self.local_shard_id, e
                    );
                    return Err(Error::RemoteDiscovery(
                        "Failed to get devices cache from Redis",
                    ));
                }
                Ok(keys) => keys,
            };

            for infra in infras_list {
                redis_keys.retain(|key| {
                    !key.contains(&format!("shard_id_{}|{}", self.local_shard_id, infra.mac))
                });
            }

            for key in redis_keys {
                if let Err(res) = redis::cmd("DEL")
                    .arg(&key)
                    .query_async::<redis::aio::MultiplexedConnection, ()>(&mut con)
                    .await
                {
                    warn!("Failed to delete cache entry {}! Error: {}", key, res);
                }
            }
        }

        Ok(())
    }

    pub async fn set_redis_last_update_timestamp(&self) -> Result<()> {
        // Generate current UTC timestamp
        let mut con = self.redis_client.clone();
        let now = Utc::now();
        let timestamp = now.timestamp(); // Get seconds since the UNIX epoch

        let res: RedisResult<()> = redis::cmd("SET")
            .arg("last_update_timestamp")
            .arg(timestamp)
            .query_async(&mut con)
            .await;

        match res {
            Ok(_) => debug!("Updated Redis timestamp: {timestamp}"),
            Err(e) => {
                if e.is_io_error() {
                    Self::set_redis_health_state_not_ready(e.to_string()).await;
                }
                warn!("Failed update Redis timestamp! Error: {e}");
                return Err(Error::RemoteDiscovery("Failed update Redis timestamp"));
            }
        };

        Ok(())
    }

    pub async fn get_redis_last_update_timestamp(&self) -> Result<i64> {
        let mut con = self.redis_client.clone();

        let last_update_timestamp: i64 = match redis::cmd("GET")
            .arg("last_update_timestamp")
            .query_async(&mut con)
            .await
        {
            Ok(timestamp) => timestamp,
            Err(e) => {
                if e.is_io_error() {
                    Self::set_redis_health_state_not_ready(e.to_string()).await;
                }
                error!("Failed to get Redis last update timestamp! Error: {}", e);
                return Err(Error::RemoteDiscovery(
                    "Failed to get Redis last update timestamp",
                ));
            }
        };

        Ok(last_update_timestamp)
    }

    pub async fn set_redis_health_state_not_ready(error_message: String) {
        tokio::spawn(async move {
            CGWMetrics::get_ref()
                .change_component_health_status(
                    CGWMetricsHealthComponent::RedisConnection,
                    CGWMetricsHealthComponentStatus::NotReady(error_message),
                )
                .await;
        });
    }
}
