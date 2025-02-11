use crate::cgw_app_args::{CGWKafkaArgs, CGWRedisArgs};
use crate::cgw_errors::{Error, Result};
use crate::cgw_remote_discovery::cgw_create_redis_client;

use rdkafka::admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;

use std::time::Duration;

use crate::cgw_tls::CGW_TLS_NB_INFRA_CERTS_PATH;

const CGW_KAFKA_TOPICS_LIST: [&str; 6] = [
    "cnc",
    "cnc_res",
    "connection",
    "infra_realtime",
    "state",
    "topology",
];

async fn cgw_get_active_cgw_number(redis_args: &CGWRedisArgs) -> Result<usize> {
    let redis_client = match cgw_create_redis_client(redis_args).await {
        Ok(client) => client,
        Err(e) => {
            return Err(Error::KafkaInit(format!(
                "Failed to create redis client! Error: {e}"
            )));
        }
    };

    let mut redis_connection = match redis_client
        .get_multiplexed_tokio_connection_with_response_timeouts(
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .await
    {
        Ok(conn) => conn,
        Err(e) => {
            return Err(Error::KafkaInit(format!(
                "Failed to get redis connection! Error: {e}"
            )));
        }
    };

    let redis_keys: Vec<String> = match redis::cmd("KEYS")
        .arg("shard_id_*".to_string())
        .query_async(&mut redis_connection)
        .await
    {
        Err(e) => {
            return Err(Error::KafkaInit(format!(
                "Failed to get shard_id list from REDIS! Error: {e}"
            )));
        }
        Ok(keys) => keys,
    };

    Ok(redis_keys.len())
}

fn cgw_create_kafka_admin(kafka_args: &CGWKafkaArgs) -> Result<AdminClient<DefaultClientContext>> {
    let mut admin_config: ClientConfig = ClientConfig::new();
    admin_config.set(
        "bootstrap.servers",
        format!("{}:{}", kafka_args.kafka_host, kafka_args.kafka_port),
    );

    if kafka_args.kafka_tls {
        let cert_path = format!("{CGW_TLS_NB_INFRA_CERTS_PATH}/{}", kafka_args.kafka_cert);
        admin_config
            .set("security.protocol", "SSL")
            .set("ssl.ca.location", &cert_path)
            .set("ssl.endpoint.identification.algorithm", "none");
    }

    let admin_client = match admin_config.create() {
        Ok(client) => client,
        Err(e) => {
            return Err(Error::KafkaInit(format!(
                "Failed to create kafka admin client! Error: {e}"
            )));
        }
    };

    Ok(admin_client)
}

fn cgw_get_kafka_topics(
    admin_client: &AdminClient<DefaultClientContext>,
) -> Result<Vec<(String, usize)>> {
    let metadata = match admin_client
        .inner()
        .fetch_metadata(None, Duration::from_millis(2000))
    {
        Ok(data) => data,
        Err(e) => {
            return Err(Error::KafkaInit(format!(
                "Failed to get kafka topics metadata! Error: {e}"
            )));
        }
    };

    let existing_topics: Vec<(String, usize)> = metadata
        .topics()
        .iter()
        .map(|t| (t.name().to_string(), t.partitions().len()))
        .collect();

    Ok(existing_topics)
}

async fn cgw_create_kafka_topics(
    admin_client: &AdminClient<DefaultClientContext>,
    topics_list: &[&str],
) -> Result<()> {
    let mut new_topics: Vec<NewTopic<'_>> = Vec::new();
    let default_replication: i32 = 1;
    let default_topic_partitions_num: i32 = 2;
    let default_cnc_topic_partitions_num: i32 = 1;

    for topic_name in topics_list {
        new_topics.push(NewTopic::new(
            *topic_name,
            if *topic_name == "cnc" {
                default_cnc_topic_partitions_num
            } else {
                default_topic_partitions_num
            },
            TopicReplication::Fixed(default_replication),
        ));
    }

    match admin_client
        .create_topics(&new_topics, &AdminOptions::new())
        .await
    {
        Ok(results) => {
            for result in results {
                match result {
                    Ok(topic) => info!("Successfully created topic: {}", topic),
                    Err((topic, err)) => {
                        return Err(Error::KafkaInit(format!(
                            "Failed to create topic {topic}!, Error: {err}"
                        )));
                    }
                }
            }
        }
        Err(e) => {
            return Err(Error::KafkaInit(format!(
                "Failed to create kafka topics! Error: {e}"
            )));
        }
    }

    Ok(())
}

async fn cgw_update_kafka_topics_partitions(
    admin_client: &AdminClient<DefaultClientContext>,
    topic_name: &str,
    partitions_num: usize,
) -> Result<()> {
    match admin_client
        .create_partitions(
            &[NewPartitions::new(topic_name, partitions_num)],
            &AdminOptions::new(),
        )
        .await
    {
        Ok(results) => {
            for result in results {
                match result {
                    Ok(topic) => {
                        info!("Successfully increased partitions for topic: {}", topic)
                    }
                    Err((topic, e)) => {
                        return Err(Error::KafkaInit(format!(
                            "Failed to update partitions num for {topic} topic! Error: {e}"
                        )));
                    }
                }
            }
        }
        Err(e) => {
            return Err(Error::KafkaInit(format!(
                "Failed to update topic partitions num for! Error: {e}"
            )));
        }
    }

    Ok(())
}

pub async fn cgw_init_kafka_topics(
    kafka_args: &CGWKafkaArgs,
    redis_args: &CGWRedisArgs,
) -> Result<()> {
    // Kafka topics creation is done at CGW start early begin
    // At that moment of time we do not create shard info record in Redis
    // So, just simply add 1 to received number of CGW instances
    let active_cgw_number = cgw_get_active_cgw_number(redis_args).await? + 1;
    let admin_client = cgw_create_kafka_admin(kafka_args)?;
    let mut existing_topics: Vec<(String, usize)> = cgw_get_kafka_topics(&admin_client)?;

    // Find missing topics
    let missing_topics: Vec<&str> = CGW_KAFKA_TOPICS_LIST
        .iter()
        .filter(|topic| !existing_topics.iter().any(|(name, _)| name == *topic))
        .copied()
        .collect();

    if !missing_topics.is_empty() {
        // If there is some topics exists - create missed
        info!("Missed kafka topics: {}", missing_topics.join(", "));
        info!("Creating missed kafka topics!");

        cgw_create_kafka_topics(&admin_client, &missing_topics).await?;
        existing_topics = cgw_get_kafka_topics(&admin_client)?;
    }

    match existing_topics.iter().find(|(key, _)| key == "cnc") {
        Some((topic_name, partitions_num)) => {
            if active_cgw_number > *partitions_num {
                error!("Updating number of partitions for cnc topic!");
                cgw_update_kafka_topics_partitions(&admin_client, topic_name, active_cgw_number)
                    .await?;
            }
        }
        None => {
            return Err(Error::KafkaInit(
                "Failed to find cnc topic in existing topics list!".to_string(),
            ));
        }
    }

    Ok(())
}
