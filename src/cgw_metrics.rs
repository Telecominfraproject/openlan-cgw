use crate::AppArgs;

use prometheus::{IntCounter, IntGauge, Registry};
use std::result::Result;
use std::sync::{Arc, Mutex};
use std::time::Duration;
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
    GroupsCapacity,
    GroupsThreshold,
    ConnectionsNum,
}

pub enum CGWMetricsCounterOpType {
    Inc,
    IncBy(i64),
    Dec,
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

    pub async fn start(self: &Self, app_args: &AppArgs) {
        let mut started = self.started.lock().unwrap();

        if *started == true {
            return;
        }

        *started = true;

        // TODO: remove: W/A for now, as currently capacity / threshold
        // is non-configurable
        GROUPS_CAPACITY.set(1000i64);
        GROUPS_THRESHOLD.set(50i64);

        tokio::spawn(async move {
            register_custom_metrics();

            let metrics_route = warp::path!("metrics").and_then(metrics_handler);

            warp::serve(metrics_route).run(([0, 0, 0, 0], 8080)).await;
        });
    }

    pub fn change_counter(
        self: &Self,
        counter: CGWMetricsCounterType,
        op: CGWMetricsCounterOpType,
    ) {
        match counter {
            CGWMetricsCounterType::ActiveCGWNum => match op {
                CGWMetricsCounterOpType::Set(v) => {
                    ACTIVE_CGW_NUM.set(v);
                }
                _ => {}
            },
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
            CGWMetricsCounterType::GroupsCapacity => match op {
                CGWMetricsCounterOpType::Set(v) => {
                    ACTIVE_CGW_NUM.set(v);
                }
                _ => {}
            },
            CGWMetricsCounterType::GroupsThreshold => match op {
                CGWMetricsCounterOpType::Set(v) => {
                    ACTIVE_CGW_NUM.set(v);
                }
                _ => {}
            },
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

fn register_custom_metrics() {
    REGISTRY
        .register(Box::new(ACTIVE_CGW_NUM.clone()))
        .expect("collector can't be registered");

    REGISTRY
        .register(Box::new(GROUPS_ASSIGNED_NUM.clone()))
        .expect("collector can't be registered");

    REGISTRY
        .register(Box::new(GROUPS_CAPACITY.clone()))
        .expect("collector can't be registered");

    REGISTRY
        .register(Box::new(GROUPS_THRESHOLD.clone()))
        .expect("collector can't be registered");

    REGISTRY
        .register(Box::new(CONNECTIONS_NUM.clone()))
        .expect("collector can't be registered");
}

async fn metrics_handler() -> Result<impl Reply, Rejection> {
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
