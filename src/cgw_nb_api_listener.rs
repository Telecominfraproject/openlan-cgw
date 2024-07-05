use crate::cgw_device::OldNew;
use crate::cgw_ucentral_parser::CGWDeviceChange;
use crate::AppArgs;

use crate::cgw_connection_server::{CGWConnectionNBAPIReqMsg, CGWConnectionNBAPIReqMsgOrigin};
use crate::cgw_errors::{Error, Result};
use crate::cgw_metrics::{CGWMetrics, CGWMetricsHealthComponent, CGWMetricsHealthComponentStatus};

use eui48::MacAddress;
use futures::stream::TryStreamExt;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::{
    consumer::{stream_consumer::StreamConsumer, Consumer, ConsumerContext, Rebalance},
    producer::{FutureProducer, FutureRecord},
};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::{
    runtime::{Builder, Runtime},
    sync::mpsc::UnboundedSender,
    time::Duration,
};
use uuid::Uuid;

type CGWConnectionServerMboxTx = UnboundedSender<CGWConnectionNBAPIReqMsg>;
type CGWCNCConsumerType = StreamConsumer<CustomContext>;
type CGWCNCProducerType = FutureProducer;

#[derive(Debug, Serialize)]
pub struct InfraGroupCreateResponse {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub infra_name: String,
    pub uuid: Uuid,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InfraGroupDeleteResponse {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub uuid: Uuid,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InfraGroupDeviceAddResponse {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub infra_group_infra_devices: Vec<MacAddress>,
    pub uuid: Uuid,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InfraGroupDeviceDelResponse {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub infra_group_infra_devices: Vec<MacAddress>,
    pub uuid: Uuid,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InfraGroupDeviceMessageEnqueueResponse {
    pub r#type: &'static str,
    pub uuid: Uuid,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RebalanceGroupsResponse {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub uuid: Uuid,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InfraGroupDeviceCapabilitiesChanged {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub infra_group_infra_device: MacAddress,
    pub changes: Vec<CGWDeviceChange>,
}

#[derive(Debug, Serialize)]
pub struct UnassignedInfraConnection {
    pub r#type: &'static str,
    pub infra_group_infra_device: MacAddress,
    pub reporter_shard_id: i32,
}

#[derive(Debug, Serialize)]
pub struct ForeignInfraConnection {
    pub r#type: &'static str,
    pub infra_group_id: i32,
    pub infra_group_infra_device: MacAddress,
    pub reporter_shard_id: i32,
    pub group_owner_shard_id: i32,
}

pub fn cgw_construct_infra_group_create_response(
    infra_group_id: i32,
    infra_name: String,
    uuid: Uuid,
    success: bool,
    error_message: Option<String>,
) -> Result<String> {
    let group_create = InfraGroupCreateResponse {
        r#type: "infrastructure_group_create",
        infra_group_id,
        infra_name,
        uuid,
        success,
        error_message,
    };

    Ok(serde_json::to_string(&group_create)?)
}

pub fn cgw_construct_infra_group_delete_response(
    infra_group_id: i32,
    uuid: Uuid,
    success: bool,
    error_message: Option<String>,
) -> Result<String> {
    let group_delete = InfraGroupDeleteResponse {
        r#type: "infrastructure_group_delete",
        infra_group_id,
        uuid,
        success,
        error_message,
    };

    Ok(serde_json::to_string(&group_delete)?)
}

pub fn cgw_construct_infra_group_device_add_response(
    infra_group_id: i32,
    infra_group_infra_devices: Vec<MacAddress>,
    uuid: Uuid,
    success: bool,
    error_message: Option<String>,
) -> Result<String> {
    let dev_add = InfraGroupDeviceAddResponse {
        r#type: "infrastructure_group_device_add_response",
        infra_group_id,
        infra_group_infra_devices,
        uuid,
        success,
        error_message,
    };

    Ok(serde_json::to_string(&dev_add)?)
}

pub fn cgw_construct_infra_group_device_del_response(
    infra_group_id: i32,
    infra_group_infra_devices: Vec<MacAddress>,
    uuid: Uuid,
    success: bool,
    error_message: Option<String>,
) -> Result<String> {
    let dev_del = InfraGroupDeviceDelResponse {
        r#type: "infrastructure_group_device_del_response",
        infra_group_id,
        infra_group_infra_devices,
        uuid,
        success,
        error_message,
    };

    Ok(serde_json::to_string(&dev_del)?)
}

pub fn cgw_construct_device_enqueue_response(
    uuid: Uuid,
    success: bool,
    error_message: Option<String>,
) -> Result<String> {
    let dev_enq_resp = InfraGroupDeviceMessageEnqueueResponse {
        r#type: "infrastructure_group_device_message_enqueu_response",
        uuid,
        success,
        error_message,
    };

    Ok(serde_json::to_string(&dev_enq_resp)?)
}

pub fn cgw_construct_rebalance_group_response(
    infra_group_id: i32,
    uuid: Uuid,
    success: bool,
    error_message: Option<String>,
) -> Result<String> {
    let rebalanse_resp = RebalanceGroupsResponse {
        r#type: "rebalance_groups_response",
        infra_group_id,
        uuid,
        success,
        error_message,
    };

    Ok(serde_json::to_string(&rebalanse_resp)?)
}

pub fn cgw_construct_device_capabilities_changed_msg(
    infra_group_infra_device: MacAddress,
    infra_group_id: i32,
    diff: &HashMap<String, OldNew>,
) -> Result<String> {
    let mut changes: Vec<CGWDeviceChange> = Vec::new();

    for (name, values) in diff.iter() {
        changes.push(CGWDeviceChange {
            changed: name.clone(),
            old: values.old_value.clone(),
            new: values.new_value.clone(),
        });
    }

    let dev_cap_msg = InfraGroupDeviceCapabilitiesChanged {
        r#type: "infrastructure_group_device_capabilities_changed",
        infra_group_id,
        infra_group_infra_device,
        changes,
    };

    Ok(serde_json::to_string(&dev_cap_msg)?)
}

pub fn cgw_construct_unassigned_infra_connection_msg(
    infra_group_infra_device: MacAddress,
    reporter_shard_id: i32,
) -> Result<String> {
    let unassigned_infra_msg = UnassignedInfraConnection {
        r#type: "unassigned_infra_connection",
        infra_group_infra_device,
        reporter_shard_id,
    };

    Ok(serde_json::to_string(&unassigned_infra_msg)?)
}

pub fn cgw_construct_foreign_infra_connection_msg(
    infra_group_id: i32,
    infra_group_infra_device: MacAddress,
    reporter_shard_id: i32,
    group_owner_shard_id: i32,
) -> Result<String> {
    let foreign_infra_msg = ForeignInfraConnection {
        r#type: "foreign_infra_connection",
        infra_group_id,
        infra_group_infra_device,
        reporter_shard_id,
        group_owner_shard_id,
    };

    Ok(serde_json::to_string(&foreign_infra_msg)?)
}

struct CustomContext;
impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance<'_>) {
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
        let mut part_list = String::new();

        if let rdkafka::consumer::Rebalance::Assign(partitions) = rebalance {
            for x in partitions.elements() {
                part_list += &(x.partition().to_string() + " ");
            }
            debug!("post_rebalance callback, assigned partition(s): {part_list}");
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

    fn commit_callback(&self, _result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        let mut part_list = String::new();
        for x in _offsets.elements() {
            part_list += &(x.partition().to_string() + " ");
        }
        debug!("commit_callback callback, partition(s): {part_list}");
        debug!("Consumer callback: commited offset");
    }
}

static GROUP_ID: &str = "CGW";
const CONSUMER_TOPICS: [&str; 1] = ["CnC"];
const PRODUCER_TOPICS: &str = "CnC_Res";

struct CGWCNCProducer {
    p: CGWCNCProducerType,
}

struct CGWCNCConsumer {
    c: CGWCNCConsumerType,
}

impl CGWCNCConsumer {
    pub fn new(app_args: &AppArgs) -> Result<Self> {
        let consum: CGWCNCConsumerType = Self::create_consumer(app_args)?;
        Ok(CGWCNCConsumer { c: consum })
    }

    fn create_consumer(app_args: &AppArgs) -> Result<CGWCNCConsumerType> {
        let context = CustomContext;

        let consumer: CGWCNCConsumerType = match ClientConfig::new()
            .set("group.id", GROUP_ID)
            .set(
                "client.id",
                GROUP_ID.to_string() + &app_args.cgw_id.to_string(),
            )
            .set("group.instance.id", app_args.cgw_id.to_string())
            .set(
                "bootstrap.servers",
                app_args.kafka_host.clone() + ":" + &app_args.kafka_port.to_string(),
            )
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            //.set("statistics.interval.ms", "30000")
            //.set("auto.offset.reset", "smallest")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(context)
        {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to create kafka consumer from config: {:?}", e);
                return Err(Error::Kafka(e));
            }
        };

        debug!(
            "(consumer) (producer) Created lazy connection to kafka broker ({}:{})...",
            app_args.kafka_host, app_args.kafka_port,
        );

        if let Err(e) = consumer.subscribe(&CONSUMER_TOPICS) {
            error!(
                "Kafka consumer was unable to subscribe to {:?}",
                CONSUMER_TOPICS
            );
            return Err(Error::Kafka(e));
        };

        Ok(consumer)
    }
}

impl CGWCNCProducer {
    pub fn new(app_args: &AppArgs) -> Result<Self> {
        let prod: CGWCNCProducerType = Self::create_producer(app_args)?;
        Ok(CGWCNCProducer { p: prod })
    }

    fn create_producer(app_args: &AppArgs) -> Result<CGWCNCProducerType> {
        let producer: FutureProducer = match ClientConfig::new()
            .set(
                "bootstrap.servers",
                app_args.kafka_host.clone() + ":" + &app_args.kafka_port.to_string(),
            )
            .set("message.timeout.ms", "5000")
            .create()
        {
            Ok(p) => p,
            Err(e) => {
                error!("Failed to create Kafka producer!");
                return Err(Error::Kafka(e));
            }
        };

        debug!(
            "(producer) Created lazy connection to kafka broker ({}:{})...",
            app_args.kafka_host, app_args.kafka_port,
        );

        Ok(producer)
    }
}

pub struct CGWNBApiClient {
    working_runtime_handle: Runtime,
    cgw_server_tx_mbox: CGWConnectionServerMboxTx,
    prod: CGWCNCProducer,
    // TBD: stplit different implementators through a defined trait,
    // that implements async R W operations?
}

impl CGWNBApiClient {
    pub fn new(app_args: &AppArgs, cgw_tx: &CGWConnectionServerMboxTx) -> Result<Arc<Self>> {
        let working_runtime_h = Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("cgw-nb-api-l")
            .thread_stack_size(1024 * 1024)
            .enable_all()
            .build()?;

        let cl = Arc::new(CGWNBApiClient {
            working_runtime_handle: working_runtime_h,
            cgw_server_tx_mbox: cgw_tx.clone(),
            prod: CGWCNCProducer::new(app_args)?,
        });

        let cl_clone = cl.clone();
        let consumer: CGWCNCConsumer = CGWCNCConsumer::new(app_args)?;
        cl.working_runtime_handle.spawn(async move {
            loop {
                let cl_clone = cl_clone.clone();
                let stream_processor = consumer.c.stream().try_for_each(|borrowed_message| {
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
                                warn!("Error while deserializing message payload: {:?}", e);
                                ""
                            }
                        };

                        let payload = match owned.payload_view::<str>() {
                            None => "",
                            Some(Ok(s)) => s,
                            Some(Err(e)) => {
                                warn!("Error while deserializing message payload: {:?}", e);
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
                let _ = stream_processor.await;
            }
        });

        Ok(cl)
    }

    pub async fn enqueue_mbox_message_from_cgw_server(&self, key: String, payload: String) {
        let produce_future = self.prod.p.send(
            FutureRecord::to(PRODUCER_TOPICS)
                .key(&key)
                .payload(&payload),
            Duration::from_secs(0),
        );

        if let Err((e, _)) = produce_future.await {
            error!("{:?}", e)
        }
    }

    async fn enqueue_mbox_message_to_cgw_server(&self, key: String, payload: String) {
        debug!("MBOX_OUT: EnqueueNewMessageFromNBAPIListener, k:{key}");
        let msg = CGWConnectionNBAPIReqMsg::EnqueueNewMessageFromNBAPIListener(
            key,
            payload,
            CGWConnectionNBAPIReqMsgOrigin::FromNBAPI,
        );
        let _ = self.cgw_server_tx_mbox.send(msg);
    }
}
