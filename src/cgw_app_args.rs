use std::{
    env,
    net::Ipv4Addr,
    path::{Path, PathBuf},
    str::FromStr,
};

use url::Url;

use crate::{
    cgw_errors::{Error, Result},
    AppCoreLogLevel,
};

const CGW_DEFAULT_ID: i32 = 0;
const CGW_DEFAULT_GROUPS_CAPACITY: i32 = 1000;
const CGW_DEFAULT_GROUPS_THRESHOLD: i32 = 50;
const CGW_DEFAULT_GROUP_INFRAS_CAPACITY: i32 = 2000;
const CGW_DEFAULT_WSS_T_NUM: usize = 4;
const CGW_DEFAULT_LOG_LEVEL: AppCoreLogLevel = AppCoreLogLevel::Debug;
const CGW_DEFAULT_WSS_IP: Ipv4Addr = Ipv4Addr::new(0, 0, 0, 0);
const CGW_DEFAULT_WSS_PORT: u16 = 15002;
const CGW_DEFAULT_WSS_CAS: &str = "cas.pem";
const CGW_DEFAULT_WSS_CERT: &str = "cert.pem";
const CGW_DEFAULT_WSS_KEY: &str = "key.pem";
const CGW_DEFAULT_GRPC_LISTENING_IP: Ipv4Addr = Ipv4Addr::new(0, 0, 0, 0);
const CGW_DEFAULT_GRPC_LISTENING_PORT: u16 = 50051;
const CGW_DEFAULT_GRPC_PUBLIC_HOST: &str = "localhost";
const CGW_DEFAULT_GRPC_PUBLIC_PORT: u16 = 50051;
const CGW_DEFAULT_KAFKA_HOST: &str = "localhost";
const CGW_DEFAULT_KAFKA_PORT: u16 = 9092;
const CGW_DEFAULT_KAFKA_TLS: &str = "no";
const CGW_DEFAULT_KAFKA_CONSUME_TOPIC: &str = "cnc";
const CGW_DEFAULT_KAFKA_PRODUCE_TOPIC: &str = "cnc_res";
const CGW_DEFAULT_DB_HOST: &str = "localhost";
const CGW_DEFAULT_DB_PORT: u16 = 6379;
const CGW_DEFAULT_DB_NAME: &str = "cgw";
const CGW_DEFAULT_DB_USERNAME: &str = "cgw";
const CGW_DEFAULT_DB_PASSWORD: &str = "123";
const CGW_DEFAULT_DB_TLS: &str = "no";
const CGW_DEFAULT_REDIS_HOST: &str = "localhost";
const CGW_DEFAULT_REDIS_PORT: u16 = 6379;
const CGW_DEFAULT_REDIS_TLS: &str = "no";
const CGW_DEFAULT_ALLOW_CERT_MISMATCH: &str = "no";
const CGW_DEFAULT_METRICS_PORT: u16 = 8080;
const CGW_DEFAULT_TOPOMAP_STATE: bool = false;
const CGW_DEFAULT_NB_INFRA_TLS: &str = "no";
const CGW_DEFAULT_UCENTRAL_AP_DATAMODEL_URI: &str = "https://raw.githubusercontent.com/Telecominfraproject/wlan-ucentral-schema/main/ucentral.schema.json";
const CGW_DEFAULT_UCENTRAL_SWITCH_DATAMODEL_URI: &str = "https://raw.githubusercontent.com/Telecominfraproject/ols-ucentral-schema/main/ucentral.schema.json";

pub struct CGWWSSArgs {
    /// Number of thread in a threadpool dedicated for handling secure websocket connections
    pub wss_t_num: usize,
    /// IP to listen for incoming WSS connection
    pub wss_ip: Ipv4Addr,
    /// PORT to listen for incoming WSS connection
    pub wss_port: u16,
    /// WSS CAS certificate (contains root and issuer certificates)
    pub wss_cas: String,
    /// WSS certificate
    pub wss_cert: String,
    /// WSS private key
    pub wss_key: String,
    /// Allow Mismatch
    pub allow_mismatch: bool,
}

impl CGWWSSArgs {
    fn parse() -> Result<CGWWSSArgs> {
        let wss_t_num: usize = match env::var("DEFAULT_WSS_THREAD_NUM") {
            Ok(val) => match val.parse() {
                Ok(v) => v,
                Err(e) => {
                    return Err(Error::AppArgsParser(format!(
                        "Failed to parse DEFAULT_WSS_THREAD_NUM! Invalid value: {val}! Error: {e}"
                    )));
                }
            },
            Err(_) => CGW_DEFAULT_WSS_T_NUM,
        };

        let wss_ip: Ipv4Addr = match env::var("CGW_WSS_IP") {
            Ok(val) => match Ipv4Addr::from_str(val.as_str()) {
                Ok(v) => v,
                Err(e) => {
                    return Err(Error::AppArgsParser(format!(
                        "Failed to parse CGW_WSS_IP! Invalid value: {val}! Error: {e}"
                    )));
                }
            },
            Err(_) => CGW_DEFAULT_WSS_IP,
        };

        let wss_port: u16 = match env::var("CGW_WSS_PORT") {
            Ok(val) => match val.parse() {
                Ok(v) => v,
                Err(e) => {
                    return Err(Error::AppArgsParser(format!(
                        "Failed to parse CGW_WSS_PORT! Invalid value: {val}! Error: {e}"
                    )));
                }
            },
            Err(_) => CGW_DEFAULT_WSS_PORT,
        };

        let wss_cas: String = env::var("CGW_WSS_CAS").unwrap_or(CGW_DEFAULT_WSS_CAS.to_string());
        let wss_cert: String = env::var("CGW_WSS_CERT").unwrap_or(CGW_DEFAULT_WSS_CERT.to_string());
        let wss_key: String = env::var("CGW_WSS_KEY").unwrap_or(CGW_DEFAULT_WSS_KEY.to_string());

        let mismatch: String = env::var("CGW_ALLOW_CERT_MISMATCH")
            .unwrap_or(CGW_DEFAULT_ALLOW_CERT_MISMATCH.to_string());
        let allow_mismatch = mismatch == "yes";

        Ok(CGWWSSArgs {
            wss_t_num,
            wss_ip,
            wss_port,
            wss_cas,
            wss_cert,
            wss_key,
            allow_mismatch,
        })
    }
}

pub struct CGWGRPCArgs {
    /// IP to listen for incoming GRPC connection
    pub grpc_listening_ip: Ipv4Addr,
    /// PORT to listen for incoming GRPC connection
    pub grpc_listening_port: u16,
    /// IP or hostname for Redis Record
    pub grpc_public_host: String,
    /// PORT for Redis record
    pub grpc_public_port: u16,
}

impl CGWGRPCArgs {
    fn parse() -> Result<CGWGRPCArgs> {
        let grpc_listening_ip: Ipv4Addr = match env::var("CGW_GRPC_LISTENING_IP") {
            Ok(val) => match Ipv4Addr::from_str(val.as_str()) {
                Ok(v) => v,
                Err(e) => {
                    return Err(Error::AppArgsParser(format!(
                        "Failed to parse CGW_GRPC_LISTENING_IP! Invalid value: {val}! Error: {e}"
                    )));
                }
            },
            Err(_) => CGW_DEFAULT_GRPC_LISTENING_IP,
        };

        let grpc_listening_port: u16 = match env::var("CGW_GRPC_LISTENING_PORT") {
            Ok(val) => match val.parse() {
                Ok(v) => v,
                Err(e) => {
                    return Err(Error::AppArgsParser(format!(
                        "Failed to parse CGW_GRPC_LISTENING_PORT! Invalid value: {val}! Error: {e}"
                    )));
                }
            },
            Err(_) => CGW_DEFAULT_GRPC_LISTENING_PORT,
        };

        let grpc_public_host: String = match env::var("CGW_GRPC_PUBLIC_HOST") {
            Ok(val) => {
                // 1. Try to parse variable into IpAddress
                match Ipv4Addr::from_str(val.as_str()) {
                    // 2. If parsed - return IpAddress as String value
                    Ok(ip) => ip.to_string(),
                    // 3. If parse failed - probably hostname specified
                    Err(_e) => val,
                }
            }
            // Env. variable is not setup - use default value
            Err(_) => CGW_DEFAULT_GRPC_PUBLIC_HOST.to_string(),
        };

        let grpc_public_port: u16 = match env::var("CGW_GRPC_PUBLIC_PORT") {
            Ok(val) => match val.parse() {
                Ok(v) => v,
                Err(e) => {
                    return Err(Error::AppArgsParser(format!(
                        "Failed to parse CGW_GRPC_PUBLIC_PORT! Invalid value: {val}! Error: {e}"
                    )));
                }
            },
            Err(_) => CGW_DEFAULT_GRPC_PUBLIC_PORT,
        };

        Ok(CGWGRPCArgs {
            grpc_listening_ip,
            grpc_listening_port,
            grpc_public_host,
            grpc_public_port,
        })
    }
}

pub struct CGWKafkaArgs {
    /// IP or hostname to connect to KAFKA broker
    pub kafka_host: String,
    /// PORT to connect to KAFKA broker
    pub kafka_port: u16,
    /// KAFKA topic from where to consume messages
    #[allow(unused)]
    pub kafka_consume_topic: String,
    /// KAFKA topic where to produce messages
    #[allow(unused)]
    pub kafka_produce_topic: String,
    /// Utilize TLS connection with Kafka broker
    pub kafka_tls: bool,
    /// Certificate name to validate Kafka broker
    pub kafka_cert: String,
}

impl CGWKafkaArgs {
    fn parse() -> Result<CGWKafkaArgs> {
        let kafka_host: String = match env::var("CGW_KAFKA_HOST") {
            Ok(val) => {
                // 1. Try to parse variable into IpAddress
                match Ipv4Addr::from_str(val.as_str()) {
                    // 2. If parsed - return IpAddress as String value
                    Ok(ip) => ip.to_string(),
                    // 3. If parse failed - probably hostname specified
                    Err(_e) => val,
                }
            }
            // Env. variable is not setup - use default value
            Err(_) => CGW_DEFAULT_KAFKA_HOST.to_string(),
        };

        let kafka_port: u16 = match env::var("CGW_KAFKA_PORT") {
            Ok(val) => match val.parse() {
                Ok(v) => v,
                Err(e) => {
                    return Err(Error::AppArgsParser(format!(
                        "Failed to parse CGW_KAFKA_PORT! Invalid value: {val}! Error: {e}"
                    )));
                }
            },
            Err(_) => CGW_DEFAULT_KAFKA_PORT,
        };

        let kafka_consume_topic: String = env::var("CGW_KAFKA_CONSUMER_TOPIC")
            .unwrap_or(CGW_DEFAULT_KAFKA_CONSUME_TOPIC.to_string());
        let kafka_produce_topic: String = env::var("CGW_KAFKA_PRODUCER_TOPIC")
            .unwrap_or(CGW_DEFAULT_KAFKA_PRODUCE_TOPIC.to_string());

        let kafka_tls_var: String =
            env::var("CGW_KAFKA_TLS").unwrap_or(CGW_DEFAULT_KAFKA_TLS.to_string());
        let kafka_tls = kafka_tls_var == "yes";
        let kafka_cert: String = env::var("CGW_KAFKA_CERT").unwrap_or_default();

        Ok(CGWKafkaArgs {
            kafka_host,
            kafka_port,
            kafka_consume_topic,
            kafka_produce_topic,
            kafka_tls,
            kafka_cert,
        })
    }
}

pub struct CGWDBArgs {
    /// IP or hostname to connect to DB (PSQL)
    pub db_host: String,
    /// PORT to connect to DB (PSQL)
    pub db_port: u16,
    /// DB name to connect to in DB (PSQL)
    pub db_name: String,
    /// DB user name use with connection to in DB (PSQL)
    pub db_username: String,
    /// DB user password use with connection to in DB (PSQL)
    pub db_password: String,
    /// Utilize TLS connection with DB server
    pub db_tls: bool,
}

impl CGWDBArgs {
    fn parse() -> Result<CGWDBArgs> {
        let db_host: String = match env::var("CGW_DB_HOST") {
            Ok(val) => {
                // 1. Try to parse variable into IpAddress
                match Ipv4Addr::from_str(val.as_str()) {
                    // 2. If parsed - return IpAddress as String value
                    Ok(ip) => ip.to_string(),
                    // 3. If parse failed - probably hostname specified
                    Err(_e) => val,
                }
            }
            // Env. variable is not setup - use default value
            Err(_) => CGW_DEFAULT_DB_HOST.to_string(),
        };

        let db_port: u16 = match env::var("CGW_DB_PORT") {
            Ok(val) => match val.parse() {
                Ok(v) => v,
                Err(e) => {
                    return Err(Error::AppArgsParser(format!(
                        "Failed to parse CGW_DB_PORT! Invalid value: {val}! Error: {e}"
                    )));
                }
            },
            Err(_) => CGW_DEFAULT_DB_PORT,
        };

        let db_name: String = env::var("CGW_DB_NAME").unwrap_or(CGW_DEFAULT_DB_NAME.to_string());
        let db_username: String =
            env::var("CGW_DB_USERNAME").unwrap_or(CGW_DEFAULT_DB_USERNAME.to_string());
        let db_password: String =
            env::var("CGW_DB_PASSWORD").unwrap_or(CGW_DEFAULT_DB_PASSWORD.to_string());

        let db_tls_var: String = env::var("CGW_DB_TLS").unwrap_or(CGW_DEFAULT_DB_TLS.to_string());
        let db_tls = db_tls_var == "yes";

        Ok(CGWDBArgs {
            db_host,
            db_port,
            db_name,
            db_username,
            db_password,
            db_tls,
        })
    }
}

pub struct CGWRedisArgs {
    /// IP or hostname to connect to REDIS
    pub redis_host: String,
    /// PORT to connect to REDIS
    pub redis_port: u16,
    /// REDIS username
    pub redis_username: Option<String>,
    /// REDIS password
    pub redis_password: Option<String>,
    /// Utilize TLS connection with DB server
    pub redis_tls: bool,
}

impl CGWRedisArgs {
    fn parse() -> Result<CGWRedisArgs> {
        let redis_host: String = match env::var("CGW_REDIS_HOST") {
            Ok(val) => {
                // 1. Try to parse variable into IpAddress
                match Ipv4Addr::from_str(val.as_str()) {
                    // 2. If parsed - return IpAddress as String value
                    Ok(ip) => ip.to_string(),
                    // 3. If parse failed - probably hostname specified
                    Err(_e) => val,
                }
            }
            // Env. variable is not setup - use default value
            Err(_) => CGW_DEFAULT_REDIS_HOST.to_string(),
        };

        let redis_port: u16 = match env::var("CGW_REDIS_PORT") {
            Ok(val) => match val.parse() {
                Ok(v) => v,
                Err(e) => {
                    return Err(Error::AppArgsParser(format!(
                        "Failed to parse CGW_REDIS_PORT! Invalid value: {val}! Error: {e}"
                    )));
                }
            },
            Err(_) => CGW_DEFAULT_REDIS_PORT,
        };

        let redis_username: Option<String> = match env::var("CGW_REDIS_USERNAME") {
            Ok(username) => {
                if username.is_empty() {
                    None
                } else {
                    Some(username)
                }
            }
            Err(_) => None,
        };

        let redis_password: Option<String> = match env::var("CGW_REDIS_PASSWORD") {
            Ok(password) => {
                if password.is_empty() {
                    None
                } else {
                    Some(password)
                }
            }
            Err(_) => None,
        };

        let redis_tls_var: String =
            env::var("CGW_REDIS_TLS").unwrap_or(CGW_DEFAULT_REDIS_TLS.to_string());
        let redis_tls = redis_tls_var == "yes";

        Ok(CGWRedisArgs {
            redis_host,
            redis_port,
            redis_username,
            redis_password,
            redis_tls,
        })
    }
}

pub struct CGWMetricsArgs {
    // PORT to connect to Metrics
    pub metrics_port: u16,
}

impl CGWMetricsArgs {
    fn parse() -> Result<CGWMetricsArgs> {
        let metrics_port: u16 = match env::var("CGW_METRICS_PORT") {
            Ok(val) => match val.parse() {
                Ok(v) => v,
                Err(e) => {
                    return Err(Error::AppArgsParser(format!(
                        "Failed to parse CGW_METRICS_PORT! Invalid value: {val}! Error: {e}"
                    )));
                }
            },
            Err(_) => CGW_DEFAULT_METRICS_PORT,
        };

        Ok(CGWMetricsArgs { metrics_port })
    }
}

#[derive(Clone, Debug)]
pub enum CGWValidationSchemaRef {
    SchemaUri(Url),
    SchemaPath(PathBuf),
}

#[derive(Clone)]
pub struct CGWValidationSchemaArgs {
    // URI to AP data model schema
    pub ap_schema_uri: CGWValidationSchemaRef,
    // URI to Switch data model schema
    pub switch_schema_uri: CGWValidationSchemaRef,
}

impl CGWValidationSchemaArgs {
    fn parse() -> Result<CGWValidationSchemaArgs> {
        let ap_schema_uri: CGWValidationSchemaRef = match env::var("CGW_UCENTRAL_AP_DATAMODEL_URI")
        {
            Ok(uri) => {
                // CGW_UCENTRAL_AP_DATAMODEL_URI is set
                if Path::new(&uri).exists() {
                    // CGW_UCENTRAL_AP_DATAMODEL_URI - is path to local file and file exist
                    match PathBuf::from_str(&uri) {
                        Ok(path) => CGWValidationSchemaRef::SchemaPath(path),
                        Err(e) => {
                            return Err(Error::AppArgsParser(format!(
                                "Failed to parse CGW_UCENTRAL_AP_DATAMODEL_URI! Invalid URI: {uri}! Error: {e}"
                            )));
                        }
                    }
                } else {
                    match Url::parse(&uri) {
                        Ok(url) => CGWValidationSchemaRef::SchemaUri(url),
                        Err(e) => {
                            return Err(Error::AppArgsParser(format!(
                                                    "Failed to parse CGW_UCENTRAL_AP_DATAMODEL_URI! Invalid URI: {uri}! Error: {e}"
                                                )));
                        }
                    }
                }
            }
            // Environment variable was not set - use default
            Err(_e) => match Url::parse(CGW_DEFAULT_UCENTRAL_AP_DATAMODEL_URI) {
                // CGW_UCENTRAL_AP_DATAMODEL_URI was not set - try to use default
                Ok(uri) => CGWValidationSchemaRef::SchemaUri(uri),
                Err(e) => {
                    return Err(Error::AppArgsParser(format!(
                                            "Failed to parse default CGW_UCENTRAL_AP_DATAMODEL_URI! Invalid URI: {CGW_DEFAULT_UCENTRAL_AP_DATAMODEL_URI}! Error: {e}"
                                        )));
                }
            },
        };

        let switch_schema_uri: CGWValidationSchemaRef = match env::var(
            "CGW_UCENTRAL_SWITCH_DATAMODEL_URI",
        ) {
            Ok(uri) => {
                // CGW_UCENTRAL_SWITCH_DATAMODEL_URI is set
                if Path::new(&uri).exists() {
                    match PathBuf::from_str(&uri) {
                        Ok(path) => CGWValidationSchemaRef::SchemaPath(path),
                        Err(e) => {
                            return Err(Error::AppArgsParser(format!(
                                "Failed to parse CGW_UCENTRAL_SWITCH_DATAMODEL_URI! Invalid URI: {uri}! Error: {e}"
                            )));
                        }
                    }
                } else {
                    match Url::parse(&uri) {
                        Ok(url) => CGWValidationSchemaRef::SchemaUri(url),
                        Err(e) => {
                            return Err(Error::AppArgsParser(format!(
                                                    "Failed to parse CGW_UCENTRAL_SWITCH_DATAMODEL_URI! Invalid URI: {uri}! Error: {e}"
                                                )));
                        }
                    }
                }
            }
            // Environment variable was not set - use default
            Err(_e) => match Url::from_str(CGW_DEFAULT_UCENTRAL_SWITCH_DATAMODEL_URI) {
                Ok(url) => CGWValidationSchemaRef::SchemaUri(url),
                Err(e) => {
                    return Err(Error::AppArgsParser(format!(
                                            "Failed to parse default CGW_UCENTRAL_SWITCH_DATAMODEL_URI! Invalid value: {CGW_DEFAULT_UCENTRAL_SWITCH_DATAMODEL_URI}! Error: {e}"
                                        )));
                }
            },
        };

        Ok(CGWValidationSchemaArgs {
            ap_schema_uri,
            switch_schema_uri,
        })
    }
}

pub struct AppArgs {
    /// Log level of application
    pub log_level: AppCoreLogLevel,

    /// CGW unique identifier (i32)
    pub cgw_id: i32,

    /// CGW groups capacity (i32)
    pub cgw_groups_capacity: i32,

    /// CGW groups threshold (i32)
    pub cgw_groups_threshold: i32,

    /// CGW group infras capacity (i32)
    pub cgw_group_infras_capacity: i32,

    /// Topomap feature status (enabled/disabled)
    pub feature_topomap_enabled: bool,

    /// CGW Websocket args
    pub wss_args: CGWWSSArgs,

    /// CGW GRPC args
    pub grpc_args: CGWGRPCArgs,

    /// CGW Kafka args
    pub kafka_args: CGWKafkaArgs,

    /// CGW DB args
    pub db_args: CGWDBArgs,

    /// CGW Redis args
    pub redis_args: CGWRedisArgs,

    /// CGW Metrics args
    pub metrics_args: CGWMetricsArgs,

    /// CGW Validation schema URI args
    pub validation_schema: CGWValidationSchemaArgs,
}

impl AppArgs {
    pub fn parse() -> Result<Self> {
        let log_level: AppCoreLogLevel = match env::var("CGW_LOG_LEVEL") {
            Ok(val) => match val.parse() {
                Ok(v) => v,
                Err(_e) => {
                    return Err(Error::AppArgsParser(format!(
                        "Failed to parse CGW_LOG_LEVEL! Invalid value: {val}! Error: (unknown)"
                    )));
                }
            },
            Err(_) => CGW_DEFAULT_LOG_LEVEL,
        };

        let cgw_id: i32 = match env::var("CGW_ID") {
            Ok(val) => match val.parse() {
                Ok(v) => v,
                Err(e) => {
                    return Err(Error::AppArgsParser(format!(
                        "Failed to parse CGW_ID! Invalid value: {val}! Error: {e}"
                    )));
                }
            },
            Err(_) => CGW_DEFAULT_ID,
        };

        let cgw_groups_capacity: i32 = match env::var("CGW_GROUPS_CAPACITY") {
            Ok(val) => match val.parse() {
                Ok(v) => v,
                Err(e) => {
                    return Err(Error::AppArgsParser(format!(
                        "Failed to parse CGW_GROUPS_CAPACITY! Invalid value: {val}! Error: {e}"
                    )));
                }
            },
            Err(_) => CGW_DEFAULT_GROUPS_CAPACITY,
        };

        let cgw_groups_threshold: i32 = match env::var("CGW_GROUPS_THRESHOLD") {
            Ok(val) => match val.parse() {
                Ok(v) => v,
                Err(e) => {
                    return Err(Error::AppArgsParser(format!(
                        "Failed to parse CGW_GROUPS_CAPACITY! Invalid value: {val}! Error: {e}"
                    )));
                }
            },
            Err(_) => CGW_DEFAULT_GROUPS_THRESHOLD,
        };

        let cgw_group_infras_capacity: i32 = match env::var("CGW_GROUP_INFRAS_CAPACITY") {
            Ok(val) => match val.parse() {
                Ok(v) => v,
                Err(e) => {
                    return Err(Error::AppArgsParser(format!(
                        "Failed to parse CGW_GROUP_INFRAS_CAPACITY! Invalid value: {val}! Error: {e}"
                    )));
                }
            },
            Err(_) => CGW_DEFAULT_GROUP_INFRAS_CAPACITY,
        };

        let feature_topomap_enabled: bool = match env::var("CGW_FEATURE_TOPOMAP_ENABLE") {
            Ok(_) => true,
            Err(_) => CGW_DEFAULT_TOPOMAP_STATE,
        };

        let nb_infra_tls_var: String =
            env::var("CGW_NB_INFRA_TLS").unwrap_or(CGW_DEFAULT_NB_INFRA_TLS.to_string());
        let nb_infra_tls = nb_infra_tls_var == "yes";

        let wss_args = CGWWSSArgs::parse()?;
        let grpc_args = CGWGRPCArgs::parse()?;
        let mut kafka_args = CGWKafkaArgs::parse()?;
        let mut db_args = CGWDBArgs::parse()?;
        let mut redis_args = CGWRedisArgs::parse()?;
        let metrics_args = CGWMetricsArgs::parse()?;
        let validation_schema = CGWValidationSchemaArgs::parse()?;

        if nb_infra_tls {
            redis_args.redis_tls = nb_infra_tls;
            db_args.db_tls = nb_infra_tls;
            kafka_args.kafka_tls = nb_infra_tls;
        }

        Ok(AppArgs {
            log_level,
            cgw_id,
            feature_topomap_enabled,
            wss_args,
            grpc_args,
            kafka_args,
            db_args,
            redis_args,
            metrics_args,
            validation_schema,
            cgw_groups_capacity,
            cgw_groups_threshold,
            cgw_group_infras_capacity,
        })
    }
}
