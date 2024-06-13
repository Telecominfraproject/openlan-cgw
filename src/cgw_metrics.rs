use crate::cgw_errors::Result;
use crate::AppArgs;

use prometheus::{IntGauge, Registry};
use std::{collections::HashMap, fmt, sync::Arc};
use tokio::sync::{Mutex, RwLock};

use warp::{http::StatusCode, reply, Filter, Rejection, Reply};

lazy_static! {
    pub static ref ACTIVE_CGW_NUM: IntGauge = IntGauge::new(
        "cgw_active_shards_num",
        "Number of active CGWs (CGW to CGW conn established)"
    )
    .expect("metric cannot be created");
    pub static ref GROUPS_ASSIGNED_NUM: IntGauge = IntGauge::new(
        "cgw_groups_assigned_num",
        "Number of groups assigned to this particular shard"
    )
    .expect("metric canot be created");
    pub static ref GROUPS_CAPACITY: IntGauge = IntGauge::new(
        "cgw_groups_capacity",
        "Max limit (capacity) of groups this shard can handle"
    )
    .expect("metric can be created");
    pub static ref GROUPS_THRESHOLD: IntGauge = IntGauge::new(
        "cgw_groups_threshold",
        "Max threshold (extra capacity) of groups this shard can handle"
    )
    .expect("metric can be created");
    pub static ref CONNECTIONS_NUM: IntGauge = IntGauge::new(
        "cgw_connections_num",
        "Number of successfully established WSS connections (underlying Infra connections)"
    )
    .expect("metric can be created");
    pub static ref REGISTRY: Registry = Registry::new();
    pub static ref CGW_METRICS: CGWMetrics = CGWMetrics {
        started: Mutex::new(false),
        components_health: Arc::new(RwLock::new(HashMap::new())),
    };
}

#[derive(Eq, Hash, PartialEq)]
pub enum CGWMetricsHealthComponent {
    RedisConnection,
    DBConnection,
    KafkaConnection,
    ConnectionServer,
}

#[derive(PartialEq)]
pub enum CGWMetricsHealthComponentStatus {
    NotReady(String),
    Ready,
}

impl fmt::Display for CGWMetricsHealthComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CGWMetricsHealthComponent::RedisConnection => {
                write!(f, "REDIS Connection")
            }
            CGWMetricsHealthComponent::DBConnection => {
                write!(f, "SQL DB Connection")
            }
            CGWMetricsHealthComponent::KafkaConnection => {
                write!(f, "Kafka BOOTSTRAP server Connection")
            }
            CGWMetricsHealthComponent::ConnectionServer => {
                write!(f, "Main Secure WebSockets Server")
            }
        }
    }
}

impl fmt::Display for CGWMetricsHealthComponentStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CGWMetricsHealthComponentStatus::Ready => {
                write!(f, "Component status is - OK")
            }
            CGWMetricsHealthComponentStatus::NotReady(reason) => {
                write!(f, "Component status is - BAD, reason: {reason}",)
            }
        }
    }
}

pub enum CGWMetricsCounterType {
    ActiveCGWNum,
    GroupsAssignedNum,
    #[allow(dead_code)]
    GroupsCapacity,
    #[allow(dead_code)]
    GroupsThreshold,
    ConnectionsNum,
}

pub enum CGWMetricsCounterOpType {
    Inc,
    #[allow(dead_code)]
    IncBy(i64),
    Dec,
    #[allow(dead_code)]
    DecBy(i64),
    Set(i64),
}

pub struct CGWMetrics {
    started: Mutex<bool>,
    components_health:
        Arc<RwLock<HashMap<CGWMetricsHealthComponent, CGWMetricsHealthComponentStatus>>>,
}

impl CGWMetrics {
    pub fn get_ref() -> &'static Self {
        &CGW_METRICS
    }

    pub async fn start(&self, _app_args: &AppArgs) -> Result<()> {
        let mut started = self.started.lock().await;

        if *started {
            return Ok(());
        }

        debug!("Staring metrics engine...");

        *started = true;

        let mut lock = self.components_health.write().await;

        lock.insert(
            CGWMetricsHealthComponent::RedisConnection,
            CGWMetricsHealthComponentStatus::NotReady("Application is starting".to_string()),
        );
        lock.insert(
            CGWMetricsHealthComponent::DBConnection,
            CGWMetricsHealthComponentStatus::NotReady("Application is starting".to_string()),
        );
        lock.insert(
            CGWMetricsHealthComponent::KafkaConnection,
            CGWMetricsHealthComponentStatus::NotReady("Application is starting".to_string()),
        );
        lock.insert(
            CGWMetricsHealthComponent::ConnectionServer,
            CGWMetricsHealthComponentStatus::NotReady("Application is starting".to_string()),
        );

        // TODO: remove: W/A for now, as currently capacity / threshold
        // is non-configurable
        GROUPS_CAPACITY.set(1000i64);
        GROUPS_THRESHOLD.set(50i64);

        tokio::spawn(async move {
            if let Err(err) = register_custom_metrics() {
                warn!("Failed to register CGW Metrics: {:?}", err);
                return;
            };

            let metrics_route = warp::path!("metrics").and_then(metrics_handler);
            let health_route = warp::path!("health").and_then(health_handler);

            let routes = warp::get().and(metrics_route.or(health_route));
            warp::serve(routes).run(([0, 0, 0, 0], 8080)).await;
        });

        debug!("Metrics engine's been started!");
        Ok(())
    }

    pub async fn change_component_health_status(
        &self,
        counter: CGWMetricsHealthComponent,
        status: CGWMetricsHealthComponentStatus,
    ) {
        let mut lock = self.components_health.write().await;

        lock.insert(counter, status);
    }

    pub fn change_counter(&self, counter: CGWMetricsCounterType, op: CGWMetricsCounterOpType) {
        match counter {
            CGWMetricsCounterType::ActiveCGWNum => {
                if let CGWMetricsCounterOpType::Set(v) = op {
                    ACTIVE_CGW_NUM.set(v);
                }
            }
            CGWMetricsCounterType::GroupsAssignedNum => match op {
                CGWMetricsCounterOpType::Inc => {
                    GROUPS_ASSIGNED_NUM.inc();
                }
                CGWMetricsCounterOpType::Dec => {
                    GROUPS_ASSIGNED_NUM.dec();
                }
                CGWMetricsCounterOpType::Set(v) => {
                    GROUPS_ASSIGNED_NUM.set(v);
                }
                _ => {}
            },
            CGWMetricsCounterType::GroupsCapacity => {
                if let CGWMetricsCounterOpType::Set(v) = op {
                    ACTIVE_CGW_NUM.set(v);
                }
            }
            CGWMetricsCounterType::GroupsThreshold => {
                if let CGWMetricsCounterOpType::Set(v) = op {
                    ACTIVE_CGW_NUM.set(v);
                }
            }
            CGWMetricsCounterType::ConnectionsNum => match op {
                CGWMetricsCounterOpType::Inc => {
                    CONNECTIONS_NUM.inc();
                }
                CGWMetricsCounterOpType::Dec => {
                    CONNECTIONS_NUM.dec();
                }
                _ => {}
            },
        }
    }
}

fn register_custom_metrics() -> Result<()> {
    REGISTRY.register(Box::new(ACTIVE_CGW_NUM.clone()))?;

    REGISTRY.register(Box::new(GROUPS_ASSIGNED_NUM.clone()))?;

    REGISTRY.register(Box::new(GROUPS_CAPACITY.clone()))?;

    REGISTRY.register(Box::new(GROUPS_THRESHOLD.clone()))?;

    REGISTRY.register(Box::new(CONNECTIONS_NUM.clone()))?;

    Ok(())
}

async fn health_handler() -> std::result::Result<impl Reply, Rejection> {
    let metrics = CGWMetrics::get_ref();
    let lock = metrics.components_health.read().await;

    let mut healthy = true;
    let mut text_status = String::new();

    for (k, v) in lock.iter() {
        text_status.push_str(&format!("Component - {}, Status - {}\n", k, v));
        if let CGWMetricsHealthComponentStatus::NotReady(_) = v {
            healthy = false;
        }
    }

    if healthy {
        Ok("CGW: up and running (all components are healhy)".into_response())
    } else {
        Ok(reply::with_status(
            format!(
                "CGW: one or more of the components are not healhy:\n{}",
                text_status
            ),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .into_response())
    }
}

async fn metrics_handler() -> std::result::Result<impl Reply, Rejection> {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&REGISTRY.gather(), &mut buffer) {
        error!("could not encode custom metrics: {}", e);
    };
    let mut res = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            error!("custom metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    };
    buffer.clear();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        error!("could not encode prometheus metrics: {}", e);
    };
    let res_custom = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            error!("prometheus metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    };
    buffer.clear();

    res.push_str(&res_custom);
    Ok(res)
}
