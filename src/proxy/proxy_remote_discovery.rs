use cgw_common::{
    cgw_errors::{Error, Result},
    cgw_app_args::{AppArgs, CGWRedisArgs},
    cgw_tls::cgw_read_root_certs_dir,
    cgw_devices_cache::CGWDevicesCache,
};

use std::{
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use redis::{
    aio::MultiplexedConnection, Client, ConnectionInfo, RedisConnectionInfo, RedisResult,
    TlsCertificates,
};

use eui48::MacAddress;
use tokio::sync::RwLock;

// Used in remote lookup
static REDIS_KEY_SHARD_ID_PREFIX: &str = "shard_id_";
static REDIS_KEY_SHARD_ID_FIELDS_NUM: usize = 12;
static REDIS_KEY_SHARD_DEVICE_CACHE_LAST_UPDATE_TIMESTAMP: &str = "_device_cache_last_update_timestamp";

// Used in group assign / reassign
static REDIS_KEY_GID: &str = "group_id_";
static REDIS_KEY_GID_VALUE_GID: &str = "gid";
static REDIS_KEY_GID_VALUE_SHARD_ID: &str = "shard_id";

const CGW_REDIS_DEVICES_CACHE_DB: u32 = 1;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CGWREDISDBShard {
    id: i32,
    server_host: String,
    server_port: u16,
    wss_port: u16,
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
        } else if values[6] != "wss_port" {
            error!("redis.res[6] != wss_port, unexpected.");
            return CGWREDISDBShard::default();
        } else if values[8] != "assigned_groups_num" {
            error!("redis.res[8] != assigned_groups_num, unexpected.");
            return CGWREDISDBShard::default();
        } else if values[10] != "capacity" {
            error!("redis.res[10] != capacity, unexpected.");
            return CGWREDISDBShard::default();
        } else if values[12] != "threshold" {
            error!("redis.res[12] != threshold, unexpected.");
            return CGWREDISDBShard::default();
        }

        let id = values[1].parse::<i32>().unwrap_or_default();
        let server_host = values[3].clone();
        let server_port = values[5].parse::<u16>().unwrap_or_default();
        let wss_port = values[7].parse::<u16>().unwrap_or_default();
        let assigned_groups_num = values[9].parse::<i32>().unwrap_or_default();
        let capacity = values[11].parse::<i32>().unwrap_or_default();
        let threshold = values[13].parse::<i32>().unwrap_or_default();

        CGWREDISDBShard {
            id,
            server_host,
            server_port,
            wss_port,
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
            "wss_port".to_string(),
            val.wss_port.to_string(),
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
pub struct ProxyRemoteDiscovery {
    redis_client: MultiplexedConnection,
    redis_infra_cache_client: MultiplexedConnection,
    gid_to_cgw_cache: Arc<RwLock<HashMap<i32, i32>>>,
    remote_cgws_map: Arc<RwLock<HashMap<i32, CGWREDISDBShard>>>,
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

impl ProxyRemoteDiscovery {
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

        let redis_client = {
            let max_retries = 3;
            let mut attempt = 0;

            loop {
                attempt += 1;

                match redis_client
                    .get_multiplexed_tokio_connection_with_response_timeouts(
                        Duration::from_secs(1),
                        Duration::from_secs(5),
                    )
                    .await
                {
                    Ok(conn) => break conn,
                    Err(e) => {
                        if attempt >= max_retries {
                            error!(
                                "Can't create CGW Remote Discovery client after {attempt} attempts! Get Redis async connection failed! Error: {e}"
                            );
                            return Err(Error::RemoteDiscovery("Redis client create failed after max retries"));
                        }
                        warn!("Redis connection attempt {attempt}/{max_retries} failed. Retrying in 1 second... Error: {e}");

                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
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
                warn!(
                    "Failed to switch to Redis Database {CGW_REDIS_DEVICES_CACHE_DB}! Error: {e}"
                );
                return Err(Error::RemoteDiscovery("Failed to switch Redis Database"));
            }
        };

        let rc = ProxyRemoteDiscovery {
            redis_client,
            redis_infra_cache_client,
            gid_to_cgw_cache: Arc::new(RwLock::new(HashMap::new())),
            remote_cgws_map: Arc::new(RwLock::new(HashMap::new())),
        };

        info!("Connection to REDIS DB has been established!");

        Ok(rc)
    }

    pub async fn sync_devices_cache_with_redis(
        &self,
        cache: Arc<RwLock<CGWDevicesCache>>,
    ) -> Result<()> {
        let mut devices_cache = cache.write().await;

        let mut con = self.redis_infra_cache_client.clone();
        let key = format!("shard_id_*|*");
        let redis_keys: Vec<String> = match redis::cmd("KEYS").arg(&key).query_async(&mut con).await
        {
            Err(e) => {
                error!("Failed to get devices cache from Redis for shard, Error: {e}");
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
                    error!("Failed to get devices cache from Redis for shard, Error: {e}");
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
                        error!("Failed to parse device mac address from key! Error: {}", e);
                        return Err(Error::RemoteDiscovery(
                            "Failed to parse device mac address from key",
                        ));
                    }
                },
                None => {
                    error!("Failed to get device mac address from key!");
                    return Err(Error::RemoteDiscovery(
                        "Failed to get device mac address from key",
                    ));
                }
            };

            match serde_json::from_str(&device_str) {
                Ok(dev) => {
                    devices_cache.replace_device(&device_mac, &dev);
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
                error!("Failed to sync gid to cgw map! Error: {e}");
                return Err(Error::RemoteDiscovery("Failed to get KEYS list from REDIS"));
            }
            Ok(keys) => keys,
        };

        let mut total_groups = 0;

        for key in redis_keys {
            let gid: i32 = match redis::cmd("HGET")
                .arg(&key)
                .arg(REDIS_KEY_GID_VALUE_GID)
                .query_async(&mut con)
                .await
            {
                Ok(gid) => gid,
                Err(e) => {
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
                    warn!("Found proper key '{key}' entry, but failed to fetch SHARD_ID from it! Error: {e}");
                    continue;
                }
            };

            debug!("Found group {key}, gid: {gid}, shard_id: {shard_id}");

            match lock.insert(gid, shard_id) {
                None => total_groups += 1,
                Some(_v) => warn!(
                    "Populated gid_to_cgw_map with previous value being already set, unexpected!"
                ),
            }
        }

        Ok(total_groups)
    }

    pub async fn sync_remote_cgw_map(&self) -> Result<()> {
        let mut lock = self.remote_cgws_map.write().await;

        // Clear hashmap
        lock.clear();

        let mut con = self.redis_client.clone();
        debug!("Searching for keys matching pattern: {REDIS_KEY_SHARD_ID_PREFIX}*");
        let redis_keys: Vec<String> = match redis::cmd("KEYS")
            .arg(format!("{REDIS_KEY_SHARD_ID_PREFIX}*"))
            .query_async::<_, Vec<String>>(&mut con)
            .await
        {
            Ok(keys) => {
                debug!("Found {} matching keys: {:?}", keys.len(), keys);
                keys
            },
            Err(e) => {
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

                    lock.insert(shard.id, shard);
                }
                Err(e) => {
                    warn!("Found proper key '{key}' entry, but failed to fetch Shard info from it! Error: {e}");
                    continue;
                }
            }
        }

        debug!("Final remote_cgws_map size: {}", lock.len());
        return Ok(());
    }

    pub async fn get_infra_group_owner_id(&self, gid: i32) -> Option<i32> {
        if gid == 0 {
            return None; // In case gid wasn't changed
        }
        // Try to use internal cache first
        if let Some(id) = self.gid_to_cgw_cache.read().await.get(&gid) {
            return Some(*id);
        }

        // Then try to sync and check again
        if let Err(e) = self.sync_gid_to_cgw_map().await {
            error!("Failed to sync GID to CGW map! Error: {e}");
        }

        // Check again after sync
        if let Some(id) = self.gid_to_cgw_cache.read().await.get(&gid) {
            return Some(*id);
        }

        None
    }

    pub async fn get_shard_host_and_wss_port(&self, shard_id: i32) -> Result<(String, u16)> {
        debug!("Getting shard host and server port for shard ID: {}", shard_id);

        if let Err(e) = self.sync_remote_cgw_map().await {
            error!("Failed to sync remote CGW map: {e}");
            return Err(Error::RemoteDiscovery(
                "Failed to sync (sync_remote_cgw_map) remote CGW info from REDIS",
            ));
        }

        let lock = self.remote_cgws_map.read().await;
        debug!("remote_cgws_map size after sync: {}", lock.len());
        debug!("Available shard IDs: {:?}", lock.keys().collect::<Vec<&i32>>());

        match lock.get(&shard_id) {
            Some(instance) => {
                debug!("Found shard {}: host={}, wss_port={}",
                       shard_id, instance.server_host, instance.wss_port);
                Ok((instance.server_host.clone(), instance.wss_port))
            },
            None => {
                error!("Shard ID {} not found in map", shard_id);
                Err(Error::RemoteDiscovery(
                    "Unexpected: Failed to find CGW shard",
                ))
            }
        }
    }

    pub async fn check_redis_updated(&self, last_sync_timestamp: &mut i64) -> Result<bool> {
        let original_timestamp = *last_sync_timestamp;
        let mut newest_timestamp = original_timestamp;
        let mut changes_detected = false;

        // Get all shard IDs from Redis to check for updates
        let mut con = self.redis_client.clone();
        let redis_keys: Vec<String> = match redis::cmd("KEYS")
            .arg(format!("{REDIS_KEY_SHARD_ID_PREFIX}*"))
            .query_async::<_, Vec<String>>(&mut con)
            .await
        {
            Ok(keys) => keys,
            Err(e) => {
                return Err(Error::RemoteDiscovery("Failed to get shard keys from Redis. Error: {e}"));
            }
        };

        for key in redis_keys {
            let shard_id: i32 = match key.strip_prefix(REDIS_KEY_SHARD_ID_PREFIX) {
                Some(id_str) => match id_str.parse() {
                    Ok(id) => id,
                    Err(_) => {
                        debug!("Skipping non-numeric shard ID in key: {}", key);
                        continue;
                    }
                },
                None => {
                    debug!("Unexpected key format: {}", key);
                    continue;
                }
            };

            let shard_update_timestamp = match self.get_shard_device_cache_last_update_timestamp(shard_id).await {
                Ok(timestamp) => timestamp,
                Err(e) => {
                    // debug!("Failed to get update timestamp for shard {}: {}", shard_id, e);
                    continue;
                }
            };

            if shard_update_timestamp > original_timestamp {
                changes_detected = true;
                debug!("Detected update in shard {}: timestamp {} > original sync {}",
                       shard_id, shard_update_timestamp, original_timestamp);
            }

            if shard_update_timestamp > newest_timestamp {
                newest_timestamp = shard_update_timestamp;
            }
        }

        if changes_detected {
            *last_sync_timestamp = newest_timestamp;
        }

        Ok(changes_detected)
    }

    async fn get_shard_device_cache_last_update_timestamp(&self, shard_id: i32) -> Result<i64> {
        let mut con = self.redis_infra_cache_client.clone();
        let key = format!("{}{}{}",
            REDIS_KEY_SHARD_ID_PREFIX,
            shard_id,
            REDIS_KEY_SHARD_DEVICE_CACHE_LAST_UPDATE_TIMESTAMP
        );

        let last_update_timestamp: i64 = match redis::cmd("GET")
            .arg(key)
            .query_async(&mut con)
            .await
        {
            Ok(timestamp) => timestamp,
            Err(e) => {
                return Err(Error::RemoteDiscovery(
                    "Failed to get Redis shard device cache last update timestamp. Error: {e}"
                ));
            }
        };

        Ok(last_update_timestamp)
    }

    pub async fn get_available_cgw_ids(&self) -> Result<Vec<i32>> {
        debug!("Getting available CGW IDs");
        let remote_cgws = self.remote_cgws_map.read().await;

        if remote_cgws.is_empty() {
            warn!("No CGW instances found in remote_cgws_map");
            return Err(Error::RemoteDiscovery("No CGW instances available"));
        }

        // Filter available CGWs based on capacity and threshold
        let mut available_cgws: Vec<i32> = remote_cgws
            .iter()
            .filter(|(_, shard)| {
                // Only include CGWs that have capacity to handle more groups
                shard.assigned_groups_num < shard.capacity &&
                // Allow some buffer using threshold
                (shard.capacity - shard.assigned_groups_num) > shard.threshold
            })
            .map(|(id, _)| *id)
            .collect();

        if available_cgws.is_empty() {
            // If no CGWs meet the capacity/threshold criteria, include all CGW IDs
            // as a fallback, so devices can still connect somewhere
            warn!("No CGWs with available capacity found, returning all CGW IDs");
            available_cgws = remote_cgws.keys().cloned().collect();
        }

        // Sort the CGW IDs for consistent round-robin selection
        available_cgws.sort();
        debug!("Found {} available CGW IDs: {:?}", available_cgws.len(), available_cgws);

        Ok(available_cgws)
    }
}
