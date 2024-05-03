use crate::AppArgs;

use crate::cgw_connection_server::{CGWConnectionNBAPIReqMsg, CGWConnectionNBAPIReqMsgOrigin};

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
use std::sync::Arc;
use tokio::{
    runtime::{Builder, Runtime},
    sync::mpsc::UnboundedSender,
    time::Duration,
};

type CGWConnectionServerMboxTx = UnboundedSender<CGWConnectionNBAPIReqMsg>;
type CGWCNCConsumerType = StreamConsumer<CustomContext>;
type CGWCNCProducerType = FutureProducer;

struct CustomContext;
impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
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

    fn post_rebalance(&self, rebalance: &Rebalance) {
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

static GROUP_ID: &'static str = "CGW";
const CONSUMER_TOPICS: [&'static str; 1] = ["CnC"];
const PRODUCER_TOPICS: &'static str = "CnC_Res";

struct CGWCNCProducer {
    p: CGWCNCProducerType,
}

struct CGWCNCConsumer {
    c: CGWCNCConsumerType,
}

impl CGWCNCConsumer {
    pub fn new(app_args: &AppArgs) -> Self {
        let consum: CGWCNCConsumerType = Self::create_consumer(app_args);
        CGWCNCConsumer { c: consum }
    }

    fn create_consumer(app_args: &AppArgs) -> CGWCNCConsumerType {
        let context = CustomContext;

        debug!(
            "(consumer) Trying to connect to kafka broker ({}:{})...",
            app_args.kafka_ip.to_string(),
            app_args.kafka_port.to_string()
        );

        let consumer: CGWCNCConsumerType = ClientConfig::new()
            .set("group.id", GROUP_ID)
            .set(
                "client.id",
                GROUP_ID.to_string() + &app_args.cgw_id.to_string(),
            )
            .set("group.instance.id", app_args.cgw_id.to_string())
            .set(
                "bootstrap.servers",
                app_args.kafka_ip.to_string() + ":" + &app_args.kafka_port.to_string(),
            )
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            //.set("statistics.interval.ms", "30000")
            //.set("auto.offset.reset", "smallest")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(context)
            .expect("Consumer creation failed");

        consumer
            .subscribe(&CONSUMER_TOPICS)
            .expect("Failed to subscribe to {CONSUMER_TOPICS} topics");

        info!("Connected to kafka broker");

        consumer
    }
}

impl CGWCNCProducer {
    pub fn new(app_args: &AppArgs) -> Self {
        let prod: CGWCNCProducerType = Self::create_producer(&app_args);
        CGWCNCProducer { p: prod }
    }

    fn create_producer(app_args: &AppArgs) -> CGWCNCProducerType {
        let producer: FutureProducer = ClientConfig::new()
            .set(
                "bootstrap.servers",
                app_args.kafka_ip.to_string() + ":" + &app_args.kafka_port.to_string(),
            )
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error");

        debug!(
            "(producer) Trying to connect to kafka broker ({}:{})...",
            app_args.kafka_ip.to_string(),
            app_args.kafka_port.to_string()
        );
        producer
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
    pub fn new(app_args: &AppArgs, cgw_tx: &CGWConnectionServerMboxTx) -> Arc<Self> {
        let working_runtime_h = Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("cgw-nb-api-l")
            .thread_stack_size(1 * 1024 * 1024)
            .enable_all()
            .build()
            .unwrap();
        let cl = Arc::new(CGWNBApiClient {
            working_runtime_handle: working_runtime_h,
            cgw_server_tx_mbox: cgw_tx.clone(),
            prod: CGWCNCProducer::new(&app_args),
        });

        let cl_clone = cl.clone();
        let consumer: CGWCNCConsumer = CGWCNCConsumer::new(app_args);
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

        cl
    }

    pub async fn enqueue_mbox_message_from_cgw_server(&self, key: String, payload: String) {
        let produce_future = self.prod.p.send(
            FutureRecord::to(&PRODUCER_TOPICS)
                .key(&key)
                .payload(&payload),
            Duration::from_secs(0),
        );
        match produce_future.await {
            Err((e, _)) => println!("Error: {:?}", e),
            _ => {}
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
