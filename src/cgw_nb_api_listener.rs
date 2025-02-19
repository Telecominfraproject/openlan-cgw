use crate::cgw_app_args::CGWKafkaArgs;
use crate::cgw_device::OldNew;
use crate::cgw_ucentral_parser::CGWDeviceChange;

use crate::cgw_connection_server::{CGWConnectionNBAPIReqMsg, CGWConnectionNBAPIReqMsgOrigin};
use crate::cgw_errors::{Error, Result};
use crate::cgw_metrics::{CGWMetrics, CGWMetricsHealthComponent, CGWMetricsHealthComponentStatus};
use crate::cgw_tls::CGW_TLS_NB_INFRA_CERTS_PATH;

use eui48::MacAddress;
use futures::stream::TryStreamExt;
use murmur2::murmur2;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::{
    consumer::{stream_consumer::StreamConsumer, Consumer, ConsumerContext, Rebalance},
    producer::future_producer::OwnedDeliveryResult,
    producer::{FutureProducer, FutureRecord},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::Range;
use std::sync::Arc;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use tokio::{
    runtime::{Builder, Runtime},
    sync::mpsc::UnboundedSender,
    time::Duration,
};
use uuid::Uuid;

type CGWConnectionServerMboxTx = UnboundedSender<CGWConnectionNBAPIReqMsg>;
type CGWKafkaConsumerType = StreamConsumer<CustomContext>;
type CGWKafkaProducerType = FutureProducer;

#[derive(EnumIter, Eq, Hash, PartialEq)]
pub enum CGWKafkaProducerTopic {
    CnCRes,
    Connection,
    State,
    InfraRealtime,
    Topology,
}

impl std::fmt::Display for CGWKafkaProducerTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            CGWKafkaProducerTopic::CnCRes => write!(f, "cnc_res"),
            CGWKafkaProducerTopic::Connection => write!(f, "connection"),
            CGWKafkaProducerTopic::State => write!(f, "state"),
            CGWKafkaProducerTopic::InfraRealtime => write!(f, "infra_realtime"),
            CGWKafkaProducerTopic::Topology => write!(f, "topology"),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct InfraGroupCreateResponse {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub reporter_shard_id: i32,
    pub uuid: Uuid,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InfraGroupDeleteResponse {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub reporter_shard_id: i32,
    pub uuid: Uuid,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InfraGroupInfrasAddResponse {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub failed_infras: Vec<MacAddress>,
    pub reporter_shard_id: i32,
    pub uuid: Uuid,
    pub success: bool,
    pub error_message: Option<String>,
    pub kafka_partition_key: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InfraGroupInfrasDelResponse {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub failed_infras: Vec<MacAddress>,
    pub reporter_shard_id: i32,
    pub uuid: Uuid,
    pub success: bool,
    pub error_message: Option<String>,
    pub kafka_partition_key: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InfraGroupInfraMessageEnqueueResponse {
    pub r#type: &'static str,
    pub reporter_shard_id: i32,
    pub uuid: Uuid,
    pub success: bool,
    pub error_message: Option<String>,
    pub kafka_partition_key: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InfraGroupInfraRequestResult {
    pub r#type: &'static str,
    pub reporter_shard_id: i32,
    pub uuid: Uuid,
    pub id: u64,
    #[serde(default, rename = "cloud-header")]
    pub cloud_header: Option<String>,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RebalanceGroupsResponse {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub reporter_shard_id: i32,
    pub uuid: Uuid,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InfraGroupInfraCapabilitiesChanged {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub infra_group_infra: MacAddress,
    pub changes: Vec<CGWDeviceChange>,
    pub reporter_shard_id: i32,
}

#[derive(Debug, Serialize)]
pub struct UnassignedInfraJoinMessage {
    pub r#type: &'static str,
    pub infra_group_infra: MacAddress,
    pub infra_public_ip: SocketAddr,
    pub reporter_shard_id: i32,
    pub connect_message_payload: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UnassignedInfraLeaveMessage {
    pub r#type: &'static str,
    pub infra_group_infra: MacAddress,
    pub reporter_shard_id: i32,
}

#[derive(Debug, Serialize)]
pub struct ForeignInfraConnection {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub infra_group_infra: MacAddress,
    pub infra_public_ip: SocketAddr,
    pub reporter_shard_id: i32,
    pub group_owner_shard_id: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct APClientJoinMessage {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub client: MacAddress,
    pub infra_group_infra: MacAddress,
    pub ssid: String,
    pub band: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct APClientLeaveMessage {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub client: MacAddress,
    pub infra_group_infra: MacAddress,
    pub band: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct APClientMigrateMessage {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub client: MacAddress,
    pub to_infra_group_infra_device: MacAddress,
    pub to_ssid: String,
    pub to_band: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InfraJoinMessage {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub infra_group_infra: MacAddress,
    pub infra_public_ip: SocketAddr,
    pub reporter_shard_id: i32,
    pub connect_message_payload: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InfraLeaveMessage {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub infra_group_infra: MacAddress,
    pub reporter_shard_id: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InfraStateEventMessage {
    pub r#type: &'static str,
    pub event_type: String,
    pub payload: String,
    pub reporter_shard_id: i32,
    #[serde(default, rename = "cloud-header")]
    pub cloud_header: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InfraRealtimeEventMessage {
    pub r#type: &'static str,
    pub event_type: String,
    pub payload: String,
    pub reporter_shard_id: i32,
    #[serde(default, rename = "cloud-header")]
    pub cloud_header: Option<String>,
}

/* Start */
#[derive(Debug, Serialize)]
pub struct InfraGroupSetCloudHeaderResponse {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub reporter_shard_id: i32,
    pub uuid: Uuid,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InfraGroupInfrasSetCloudHeaderResponse {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub failed_infras: Vec<MacAddress>,
    pub reporter_shard_id: i32,
    pub uuid: Uuid,
    pub success: bool,
    pub error_message: Option<String>,
}

/* End */

pub fn cgw_construct_infra_state_event_message(
    event_type: String,
    payload: String,
    reporter_shard_id: i32,
    cloud_header: Option<String>,
) -> Result<String> {
    let state_message = InfraStateEventMessage {
        r#type: "infrastructure_state_event_message",
        event_type,
        payload,
        reporter_shard_id,
        cloud_header,
    };

    Ok(serde_json::to_string(&state_message)?)
}

pub fn cgw_construct_infra_realtime_event_message(
    event_type: String,
    payload: String,
    reporter_shard_id: i32,
    cloud_header: Option<String>,
) -> Result<String> {
    let realtime_message = InfraRealtimeEventMessage {
        r#type: "infrastructure_realtime_event_message",
        event_type,
        payload,
        reporter_shard_id,
        cloud_header,
    };

    Ok(serde_json::to_string(&realtime_message)?)
}

pub fn cgw_construct_infra_group_create_response(
    infra_group_id: i32,
    reporter_shard_id: i32,
    uuid: Uuid,
    success: bool,
    error_message: Option<String>,
) -> Result<String> {
    let group_create = InfraGroupCreateResponse {
        r#type: "infrastructure_group_create_response",
        infra_group_id,
        reporter_shard_id,
        uuid,
        success,
        error_message,
    };

    Ok(serde_json::to_string(&group_create)?)
}

pub fn cgw_construct_infra_group_delete_response(
    infra_group_id: i32,
    reporter_shard_id: i32,
    uuid: Uuid,
    success: bool,
    error_message: Option<String>,
) -> Result<String> {
    let group_delete = InfraGroupDeleteResponse {
        r#type: "infrastructure_group_delete_response",
        reporter_shard_id,
        infra_group_id,
        uuid,
        success,
        error_message,
    };

    Ok(serde_json::to_string(&group_delete)?)
}

pub fn cgw_construct_infra_group_infras_add_response(
    infra_group_id: i32,
    failed_infras: Vec<MacAddress>,
    reporter_shard_id: i32,
    uuid: Uuid,
    success: bool,
    error_message: Option<String>,
    kafka_partition_key: Option<String>,
) -> Result<String> {
    let dev_add = InfraGroupInfrasAddResponse {
        r#type: "infrastructure_group_infras_add_response",
        infra_group_id,
        failed_infras,
        reporter_shard_id,
        uuid,
        success,
        error_message,
        kafka_partition_key,
    };

    Ok(serde_json::to_string(&dev_add)?)
}

pub fn cgw_construct_infra_group_infras_del_response(
    infra_group_id: i32,
    failed_infras: Vec<MacAddress>,
    reporter_shard_id: i32,
    uuid: Uuid,
    success: bool,
    error_message: Option<String>,
    kafka_partition_key: Option<String>,
) -> Result<String> {
    let dev_del = InfraGroupInfrasDelResponse {
        r#type: "infrastructure_group_infras_del_response",
        infra_group_id,
        failed_infras,
        reporter_shard_id,
        uuid,
        success,
        error_message,
        kafka_partition_key,
    };

    Ok(serde_json::to_string(&dev_del)?)
}

pub fn cgw_construct_infra_enqueue_response(
    reporter_shard_id: i32,
    uuid: Uuid,
    success: bool,
    error_message: Option<String>,
    kafka_partition_key: Option<String>,
) -> Result<String> {
    let dev_enq_resp = InfraGroupInfraMessageEnqueueResponse {
        r#type: "infrastructure_group_infra_message_enqueue_response",
        reporter_shard_id,
        uuid,
        success,
        error_message,
        kafka_partition_key,
    };

    Ok(serde_json::to_string(&dev_enq_resp)?)
}

pub fn cgw_construct_rebalance_group_response(
    infra_group_id: i32,
    reporter_shard_id: i32,
    uuid: Uuid,
    success: bool,
    error_message: Option<String>,
) -> Result<String> {
    let rebalance_resp = RebalanceGroupsResponse {
        r#type: "rebalance_groups_response",
        infra_group_id,
        reporter_shard_id,
        uuid,
        success,
        error_message,
    };

    Ok(serde_json::to_string(&rebalance_resp)?)
}

pub fn cgw_construct_infra_capabilities_changed_msg(
    infra_group_infra: MacAddress,
    infra_group_id: i32,
    diff: &HashMap<String, OldNew>,
    reporter_shard_id: i32,
) -> Result<String> {
    let mut changes: Vec<CGWDeviceChange> = Vec::new();

    for (name, values) in diff.iter() {
        changes.push(CGWDeviceChange {
            changed: name.clone(),
            old: values.old_value.clone(),
            new: values.new_value.clone(),
        });
    }

    let dev_cap_msg = InfraGroupInfraCapabilitiesChanged {
        r#type: "infrastructure_group_infra_capabilities_changed",
        infra_group_id,
        infra_group_infra,
        changes,
        reporter_shard_id,
    };

    Ok(serde_json::to_string(&dev_cap_msg)?)
}

pub fn cgw_construct_unassigned_infra_join_msg(
    infra_group_infra: MacAddress,
    infra_public_ip: SocketAddr,
    reporter_shard_id: i32,
    connect_message_payload: String,
) -> Result<String> {
    let unassigned_infra_msg = UnassignedInfraJoinMessage {
        r#type: "unassigned_infra_join",
        infra_group_infra,
        infra_public_ip,
        reporter_shard_id,
        connect_message_payload,
    };

    Ok(serde_json::to_string(&unassigned_infra_msg)?)
}

pub fn cgw_construct_unassigned_infra_leave_msg(
    infra_group_infra: MacAddress,
    reporter_shard_id: i32,
) -> Result<String> {
    let unassigned_infra_msg = UnassignedInfraLeaveMessage {
        r#type: "unassigned_infra_leave",
        infra_group_infra,
        reporter_shard_id,
    };

    Ok(serde_json::to_string(&unassigned_infra_msg)?)
}

pub fn cgw_construct_foreign_infra_connection_msg(
    infra_group_id: i32,
    infra_group_infra: MacAddress,
    infra_public_ip: SocketAddr,
    reporter_shard_id: i32,
    group_owner_shard_id: i32,
) -> Result<String> {
    let foreign_infra_msg = ForeignInfraConnection {
        r#type: "foreign_infra_connection",
        infra_group_id,
        infra_group_infra,
        infra_public_ip,
        reporter_shard_id,
        group_owner_shard_id,
    };

    Ok(serde_json::to_string(&foreign_infra_msg)?)
}

pub fn cgw_construct_client_join_msg(
    infra_group_id: i32,
    client: MacAddress,
    infra_group_infra: MacAddress,
    ssid: String,
    band: String,
) -> Result<String> {
    let client_join_msg = APClientJoinMessage {
        r#type: "ap_client_join",
        infra_group_id,
        client,
        infra_group_infra,
        ssid,
        band,
    };

    Ok(serde_json::to_string(&client_join_msg)?)
}

pub fn cgw_construct_client_leave_msg(
    infra_group_id: i32,
    client: MacAddress,
    infra_group_infra: MacAddress,
    band: String,
) -> Result<String> {
    let client_join_msg = APClientLeaveMessage {
        r#type: "ap_client_leave",
        infra_group_id,
        client,
        infra_group_infra,
        band,
    };

    Ok(serde_json::to_string(&client_join_msg)?)
}

pub fn cgw_construct_client_migrate_msg(
    infra_group_id: i32,
    client: MacAddress,
    to_infra_group_infra_device: MacAddress,
    to_ssid: String,
    to_band: String,
) -> Result<String> {
    let client_migrate_msg = APClientMigrateMessage {
        r#type: "ap_client_migrate",
        infra_group_id,
        client,
        to_infra_group_infra_device,
        to_ssid,
        to_band,
    };

    Ok(serde_json::to_string(&client_migrate_msg)?)
}

pub fn cgw_construct_infra_join_msg(
    infra_group_id: i32,
    infra_group_infra: MacAddress,
    infra_public_ip: SocketAddr,
    reporter_shard_id: i32,
    connect_message_payload: String,
) -> Result<String> {
    let infra_join_msg = InfraJoinMessage {
        r#type: "infra_join",
        infra_group_id,
        infra_group_infra,
        infra_public_ip,
        reporter_shard_id,
        connect_message_payload,
    };

    Ok(serde_json::to_string(&infra_join_msg)?)
}

pub fn cgw_construct_infra_leave_msg(
    infra_group_id: i32,
    infra_group_infra: MacAddress,
    reporter_shard_id: i32,
) -> Result<String> {
    let infra_leave_msg = InfraLeaveMessage {
        r#type: "infra_leave",
        infra_group_id,
        infra_group_infra,
        reporter_shard_id,
    };

    Ok(serde_json::to_string(&infra_leave_msg)?)
}

pub fn cgw_construct_infra_request_result_msg(
    reporter_shard_id: i32,
    uuid: Uuid,
    id: u64,
    cloud_header: Option<String>,
    success: bool,
    error_message: Option<String>,
) -> Result<String> {
    let infra_request_result = InfraGroupInfraRequestResult {
        r#type: "infra_request_result",
        reporter_shard_id,
        uuid,
        id,
        cloud_header,
        success,
        error_message,
    };

    Ok(serde_json::to_string(&infra_request_result)?)
}

pub fn cgw_construct_infra_group_set_cloud_header_response(
    infra_group_id: i32,
    reporter_shard_id: i32,
    uuid: Uuid,
    success: bool,
    error_message: Option<String>,
) -> Result<String> {
    let group_create = InfraGroupSetCloudHeaderResponse {
        r#type: "infrastructure_group_set_cloud_header_response",
        infra_group_id,
        reporter_shard_id,
        uuid,
        success,
        error_message,
    };

    Ok(serde_json::to_string(&group_create)?)
}

pub fn cgw_construct_infra_group_infras_set_cloud_header_response(
    infra_group_id: i32,
    failed_infras: Vec<MacAddress>,
    reporter_shard_id: i32,
    uuid: Uuid,
    success: bool,
    error_message: Option<String>,
) -> Result<String> {
    let group_create = InfraGroupInfrasSetCloudHeaderResponse {
        r#type: "infrastructure_group_infras_set_cloud_header_response",
        infra_group_id,
        failed_infras,
        reporter_shard_id,
        uuid,
        success,
        error_message,
    };

    Ok(serde_json::to_string(&group_create)?)
}

pub fn cgw_construct_cloud_header(
    group_cloud_header: Option<String>,
    infras_cloud_header: Option<String>,
) -> Option<String> {
    let cloud_header: Option<String> = match (group_cloud_header, infras_cloud_header) {
        (Some(group_header), Some(infras_header)) => {
            // Need to join both group and infras header into single cloud-header
            let group_header_map: HashMap<String, Value> = match serde_json::from_str(&group_header)
            {
                Ok(map) => map,
                Err(e) => {
                    println!("Failed to deserialize group header: {group_header}! Error: {e}");
                    // Do not break any process if cloud header serde fail
                    HashMap::default()
                }
            };

            let infras_header_map: HashMap<String, Value> =
                match serde_json::from_str(&infras_header) {
                    Ok(map) => map,
                    Err(e) => {
                        println!(
                            "Failed to deserialize infras header: {infras_header}! Error: {e}"
                        );
                        // Do not break any process if cloud header serde fail
                        HashMap::default()
                    }
                };

            let mut cloud_header_map: HashMap<String, Value> = group_header_map;
            cloud_header_map.extend(infras_header_map);

            match serde_json::to_string(&cloud_header_map) {
                Ok(cloud_header_str) => Some(cloud_header_str),
                Err(e) => {
                    error!("Failed to create cloud header! Error: {e}");
                    None
                }
            }
        }
        (Some(group_cloud_header), None) => Some(group_cloud_header),
        (None, Some(infras_cloud_header)) => Some(infras_cloud_header),
        (None, None) => None,
    };

    cloud_header
}

struct CGWConsumerContextData {
    // Tuple consisting of physical partition id (0,1,2.. etc)
    // and the corresponding _kafka routing key_, or just kafka key,
    // that can be used with this topic to access specified topic.
    // It can be used to optimize CGW to GID to Kafka topic mapping,
    // e.g. cloud has knowledge of what kafka key to use, to direct
    // a NB message to specific exact CGW, without the need of
    // alway backing to the use of relaying mechanism.
    // P.S. this optimization technic does not necessarily
    // make relaying obsolete. Relaying is still used to
    // forward at least one (first) NB request from
    // the shard that received message to the designated
    // recipient. Whenever recipient shard receives the NB
    // request and sends response back to NB services,
    // shard should reply back with routing_key included.
    // It's up to cloud (NB services) then to use specified
    // kafka key to make sure the kafka message reaches
    // recipient shard in exactly one hop (direct forwarding),
    // or omit kafka key completely to once again use the
    // relaying mechanism.
    partition_mapping: HashMap<u32, String>,
    assigned_partition_list: Vec<u32>,
    last_used_key_idx: u32,
    partition_num: usize,

    // A bit ugly, but we need a way to get
    // consumer (to retrieve partition num) whenever
    // client->context rebalance callback is being called.
    consumer_client: Option<Arc<CGWKafkaConsumerType>>,
}

struct CustomContext {
    ctx_data: std::sync::RwLock<CGWConsumerContextData>,
}

impl CGWConsumerContextData {
    fn recalculate_partition_to_key_mapping(&mut self, partition_num: usize) {
        const DEFAULT_HASH_SEED: u32 = 0x9747b28c;

        // The factor of 10 is selected to cover >=15000 of topics,
        // meaning with 15K partitions, this algorithm can still
        // confidently covert all 15K partitions with unique
        // kafka string-keys.
        // Even then, anything past 10K of partitions per topics
        // could be an overkill in the first place, hence
        // this algo should be sufficient.
        let loop_range = Range {
            start: 0,
            end: partition_num * 10,
        };
        let mut key_map: HashMap<u32, String> = HashMap::new();

        for x in loop_range {
            let key_str = x.to_string();
            let key_bytes = key_str.as_bytes();

            if key_map.len() == partition_num {
                break;
            }

            // Default partitioning uses the following formula:
            // toPositive(murmur2(keyBytes)) % numPartitions
            let hash_res = murmur2(key_bytes, DEFAULT_HASH_SEED) & 0x7fffffff;
            let part_idx = hash_res.rem_euclid(partition_num as u32);

            key_map.entry(part_idx).or_insert_with(|| {
                debug!("Inserted key '{key_str}' for '{part_idx}' partition");
                key_str
            });
        }

        info!(
            "Filled {} unique keys for {} of partitions",
            key_map.len(),
            partition_num
        );

        if key_map.len() != partition_num {
            // All this means, is that if some partition X has
            // no corresponding 1:1 kafka key.
            // From CGW perspective, this means that application
            // will always instruct NB to use a set of keys that
            // we were able to map, ignoring any other un-mapped
            // partitions, rendering them unused completely.
            // But it's up to NB still to either use or not provided
            // routing kafka key by CGW.
            warn!("Filled fulfill all range of kafka topics for 1:1 mapping, some partitions will not be mapped!");
        }

        self.partition_mapping = key_map;
    }

    fn get_partition_info(&mut self) -> (Vec<u32>, HashMap<u32, String>) {
        (
            self.assigned_partition_list.clone(),
            self.partition_mapping.clone(),
        )
    }
}

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance<'_>) {
        debug!("Pre rebalance entry");

        // We need to make sure the <before>
        // we're _actually_ assigned a partition list,
        // we don't fool any internal code that depends
        // on the topic list, and zero-out it when not
        // ready, and return anything only when it's
        // available.
        if let Ok(mut ctx) = self.ctx_data.write() {
            ctx.partition_mapping.clear();
            ctx.assigned_partition_list.clear();
            ctx.last_used_key_idx = 0;
            ctx.partition_num = 0;
        }

        let mut part_list = String::new();
        if let rdkafka::consumer::Rebalance::Assign(partitions) = rebalance {
            for x in partitions.elements() {
                part_list += &(x.partition().to_string() + " ");
            }
            debug!("pre_rebalance callback, assigned partition(s): {part_list}");
        }

        part_list.clear();

        if let rdkafka::consumer::Rebalance::Revoke(partitions) = rebalance {
            for x in partitions.elements() {
                part_list += &(x.partition().to_string() + " ");
            }
            debug!("pre_rebalance callback, revoked partition(s): {part_list}");
        }
    }

    fn post_rebalance(&self, rebalance: &Rebalance<'_>) {
        let mut assigned_partition_list: Vec<u32> = Vec::new();
        let mut part_list = String::new();

        if let rdkafka::consumer::Rebalance::Assign(partitions) = rebalance {
            for x in partitions.elements() {
                part_list += &(x.partition().to_string() + " ");
                assigned_partition_list.push(x.partition() as u32);
            }
            debug!("post_rebalance callback, assigned partition(s): {part_list}");
        }

        if let Ok(mut ctx) = self.ctx_data.write() {
            if let Some(consumer) = &ctx.consumer_client {
                if let Ok(metadata) =
                    consumer.fetch_metadata(Some(CONSUMER_TOPICS[0]), Duration::from_millis(2000))
                {
                    let topic = &metadata.topics()[0];
                    let partition_num: usize = topic.partitions().len();

                    debug!("topic: {}, partitions: {}", topic.name(), partition_num);

                    // We recalculate mapping only if the underlying
                    // _number_ of partitions's changed.
                    // Also, the underlying assignment to a specific
                    // partitions is irrelevant itself,
                    // as key:partition mapping changes only whenever
                    // underlying number of partitions is altered.
                    //
                    // This also means that the underlying block
                    // will get executed at least once throughout the
                    // CGW lifetime - at least once upon startup,
                    // whenever _this_ CGW consumer group
                    // consumer instance - CGW shard - is being
                    // assigned a list of partitions to consume from.
                    if ctx.partition_num != partition_num {
                        ctx.partition_num = partition_num;
                        ctx.assigned_partition_list = assigned_partition_list;

                        ctx.recalculate_partition_to_key_mapping(partition_num);
                    }
                } else {
                    warn!("Tried to fetch consumer metadata but failed. CGW will not be able to reply with optimized Kafka key for efficient routing!");
                }
            }
        }

        part_list.clear();

        if let rdkafka::consumer::Rebalance::Revoke(partitions) = rebalance {
            for x in partitions.elements() {
                part_list += &(x.partition().to_string() + " ");
            }
            debug!("post_rebalance callback, revoked partition(s): {part_list}");
        }

        tokio::spawn(async move {
            CGWMetrics::get_ref()
                .change_component_health_status(
                    CGWMetricsHealthComponent::KafkaConnection,
                    CGWMetricsHealthComponentStatus::Ready,
                )
                .await;
        });
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        let mut part_list = String::new();
        for x in _offsets.elements() {
            part_list += &(x.partition().to_string() + " ");
        }
        debug!("commit_callback callback, partition(s): {part_list}");
        debug!("Consumer callback: committing offsets: {:?}", result);
    }
}

static GROUP_ID: &str = "CGW";
const CONSUMER_TOPICS: &[&str] = &["cnc"];

struct CGWKafkaConsumer {
    consumer: Arc<CGWKafkaConsumerType>,
}

impl CGWKafkaConsumer {
    pub fn new(cgw_id: i32, kafka_args: &CGWKafkaArgs, topics: &[&str]) -> Result<Self> {
        // let topics_str = topics.join(", ");

        let consumer = Self::create_consumer(cgw_id, kafka_args, topics)?;

        debug!(
            "(consumer) Created lazy connection to kafka broker ({}:{}). Topics: {}",
            kafka_args.kafka_host,
            kafka_args.kafka_port,
            topics.join(", ")
        );

        Ok(CGWKafkaConsumer { consumer })
    }

    fn create_consumer(
        cgw_id: i32,
        kafka_args: &CGWKafkaArgs,
        topics: &[&str],
    ) -> Result<Arc<CGWKafkaConsumerType>> {
        let context = CustomContext {
            ctx_data: std::sync::RwLock::new(CGWConsumerContextData {
                partition_mapping: HashMap::new(),
                assigned_partition_list: Vec::new(),
                last_used_key_idx: 0u32,
                partition_num: 0usize,
                consumer_client: None,
            }),
        };

        let mut consumer_config = ClientConfig::new();
        consumer_config
            .set("group.id", GROUP_ID)
            .set("client.id", GROUP_ID.to_string() + &cgw_id.to_string())
            .set("group.instance.id", cgw_id.to_string())
            .set(
                "bootstrap.servers",
                kafka_args.kafka_host.clone() + ":" + &kafka_args.kafka_port.to_string(),
            )
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set_log_level(RDKafkaLogLevel::Debug);

        if kafka_args.kafka_tls {
            let cert_path = match kafka_args.kafka_cert.is_empty() {
                true => "probe".to_string(),
                false => format!("{CGW_TLS_NB_INFRA_CERTS_PATH}/{}", kafka_args.kafka_cert),
            };

            debug!("Kafka consumer certificate: {cert_path}");

            consumer_config
                .set("security.protocol", "SSL")
                .set("ssl.ca.location", &cert_path)
                .set("ssl.endpoint.identification.algorithm", "none");
        }

        let consumer: CGWKafkaConsumerType = match consumer_config.create_with_context(context) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to create kafka consumer from config! Error: {e}");
                return Err(Error::Kafka(e));
            }
        };

        let consumer = Arc::new(consumer);
        // Need to set this guy for context
        let consumer_clone = consumer.clone();

        if let Err(e) = consumer.subscribe(topics) {
            error!(
                "Kafka consumer was unable to subscribe to {:?}! Error: {e}",
                CONSUMER_TOPICS
            );
            return Err(Error::Kafka(e));
        };

        if let Ok(mut ctx) = consumer.context().ctx_data.write() {
            ctx.consumer_client = Some(consumer_clone);
        }

        Ok(consumer)
    }
}

struct CGWKafkaProducer {
    producer: CGWKafkaProducerType,
    topic: String,
}

impl CGWKafkaProducer {
    fn new(kafka_args: &CGWKafkaArgs, topic: String) -> Result<Self> {
        let producer: CGWKafkaProducerType = Self::create_producer(kafka_args)?;

        debug!(
            "(producer) Created lazy connection to kafka broker ({}:{}). Topic: {topic}",
            kafka_args.kafka_host, kafka_args.kafka_port,
        );

        Ok(CGWKafkaProducer { producer, topic })
    }

    fn create_producer(kafka_args: &CGWKafkaArgs) -> Result<CGWKafkaProducerType> {
        let mut producer_config = ClientConfig::new();
        producer_config
            .set(
                "bootstrap.servers",
                kafka_args.kafka_host.clone() + ":" + &kafka_args.kafka_port.to_string(),
            )
            .set("message.timeout.ms", "5000");

        if kafka_args.kafka_tls {
            let cert_path = match kafka_args.kafka_cert.is_empty() {
                true => "probe".to_string(),
                false => format!("{CGW_TLS_NB_INFRA_CERTS_PATH}/{}", kafka_args.kafka_cert),
            };

            debug!("Kafka producer certificate: {cert_path}");

            producer_config
                .set("security.protocol", "SSL")
                .set("ssl.ca.location", &cert_path)
                .set("ssl.endpoint.identification.algorithm", "none");
        }

        let producer: FutureProducer = match producer_config.create() {
            Ok(p) => p,
            Err(e) => {
                error!("Failed to create Kafka producer!");
                return Err(Error::Kafka(e));
            }
        };

        Ok(producer)
    }

    async fn send(&self, key: String, payload: String) -> OwnedDeliveryResult {
        self.producer
            .send(
                FutureRecord::to(&self.topic).key(&key).payload(&payload),
                Duration::from_secs(0),
            )
            .await
    }
}

struct CGWKafkaProducersMap {
    kafka_producer_map: HashMap<CGWKafkaProducerTopic, CGWKafkaProducer>,
}

impl CGWKafkaProducersMap {
    fn new(kafka_args: &CGWKafkaArgs) -> Result<CGWKafkaProducersMap> {
        let mut map: HashMap<CGWKafkaProducerTopic, CGWKafkaProducer> = HashMap::new();

        for topic in CGWKafkaProducerTopic::iter() {
            match CGWKafkaProducer::new(kafka_args, topic.to_string()) {
                Ok(producer) => map.insert(topic, producer),
                Err(e) => {
                    error!("Failed to create Kafka producer for topic: {topic}. Error: {e}");
                    return Err(e);
                }
            };
        }

        Ok(CGWKafkaProducersMap {
            kafka_producer_map: map,
        })
    }

    fn get(&self, key: CGWKafkaProducerTopic) -> Option<&CGWKafkaProducer> {
        self.kafka_producer_map.get(&key)
    }
}

pub struct CGWNBApiClient {
    working_runtime_handle: Runtime,
    cgw_server_tx_mbox: CGWConnectionServerMboxTx,
    producers: CGWKafkaProducersMap,
    consumer: Arc<CGWKafkaConsumer>,
    // TBD: split different implementations through a defined trait,
    // that implements async R W operations?
}

impl CGWNBApiClient {
    pub fn new(
        cgw_id: i32,
        kafka_args: &CGWKafkaArgs,
        cgw_tx: &CGWConnectionServerMboxTx,
    ) -> Result<Arc<Self>> {
        let working_runtime_h = Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("cgw-nb-api-l")
            .thread_stack_size(1024 * 1024)
            .enable_all()
            .build()?;

        let producers = CGWKafkaProducersMap::new(kafka_args)?;
        let consumer: Arc<CGWKafkaConsumer> =
            Arc::new(CGWKafkaConsumer::new(cgw_id, kafka_args, CONSUMER_TOPICS)?);
        let consumer_clone = consumer.clone();
        let cl = Arc::new(CGWNBApiClient {
            working_runtime_handle: working_runtime_h,
            cgw_server_tx_mbox: cgw_tx.clone(),
            producers,
            consumer: consumer_clone,
        });

        let cl_clone = cl.clone();
        cl.working_runtime_handle.spawn(async move {
            loop {
                let cl_clone = cl_clone.clone();
                let stream_processor =
                    consumer.consumer.stream().try_for_each(|borrowed_message| {
                        let cl_clone = cl_clone.clone();
                        async move {
                            // Process each message
                            // Borrowed messages can't outlive the consumer they are received from, so they need to
                            // be owned in order to be sent to a separate thread.
                            //record_owned_message_receipt(&owned_message).await;
                            let owned = borrowed_message.detach();

                            let key = match owned.key_view::<str>() {
                                None => "",
                                Some(Ok(s)) => s,
                                Some(Err(e)) => {
                                    warn!("Error while deserializing message payload! Error: {e}");
                                    ""
                                }
                            };

                            let payload = match owned.payload_view::<str>() {
                                None => "",
                                Some(Ok(s)) => s,
                                Some(Err(e)) => {
                                    warn!("Deserializing message payload failed! Error: {e}");
                                    ""
                                }
                            };
                            cl_clone
                                .enqueue_mbox_message_to_cgw_server(
                                    key.to_string(),
                                    payload.to_string(),
                                )
                                .await;
                            Ok(())
                        }
                    });

                if let Err(e) = stream_processor.await {
                    error!("Failed to create NB API Client! Error: {e}");
                }
            }
        });

        Ok(cl)
    }

    pub fn get_partition_to_local_shard_mapping(&self) -> Vec<(u32, String)> {
        let mut return_vec: Vec<(u32, String)> = Vec::new();
        if let Ok(mut ctx) = self.consumer.consumer.context().ctx_data.write() {
            let (assigned_partition_list, mut partition_mapping) = ctx.get_partition_info();

            if !partition_mapping.is_empty()
                && ctx.partition_num > 0
                && !assigned_partition_list.is_empty()
            {
                for x in assigned_partition_list {
                    if let Some(key) = partition_mapping.remove(&x) {
                        return_vec.push((x, key));
                    }
                }
            }
        }

        return_vec
    }

    pub async fn enqueue_mbox_message_from_cgw_server(
        &self,
        key: String,
        payload: String,
        topic: CGWKafkaProducerTopic,
    ) {
        if let Some(producer) = self.producers.get(topic) {
            let produce_future = producer.send(key, payload);

            if let Err((e, _)) = produce_future.await {
                error!("{e}")
            }
        } else {
            error!(
                "Failed to get kafka producer for {} topic!",
                CGWKafkaProducerTopic::CnCRes
            );
        }
    }

    async fn enqueue_mbox_message_to_cgw_server(&self, key: String, payload: String) {
        debug!("MBOX_OUT: EnqueueNewMessageFromNBAPIListener, key: {key}");
        let msg = CGWConnectionNBAPIReqMsg::EnqueueNewMessageFromNBAPIListener(
            key,
            payload,
            CGWConnectionNBAPIReqMsgOrigin::FromNBAPI,
        );

        if let Err(e) = self.cgw_server_tx_mbox.send(msg) {
            error!("Failed to send message to CGW server (remote)! Error: {e}");
        }
    }
}
