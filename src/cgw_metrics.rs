use crate::cgw_errors::{Error, Result};
use crate::AppArgs;

use prometheus::{IntGauge, Registry};
use std::sync::Mutex;

use warp::{Filter, Rejection, Reply};

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
    };
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
}

impl CGWMetrics {
    pub fn get_ref() -> &'static Self {
        &CGW_METRICS
    }

    pub async fn start(&self, _app_args: &AppArgs) -> Result<()> {
        let mut started = match self.started.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(Error::Metrics("Failed to get lock guard")),
        };

        if *started {
            return Ok(());
        }

        *started = true;

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

            warp::serve(metrics_route).run(([0, 0, 0, 0], 8080)).await;
        });

        Ok(())
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
