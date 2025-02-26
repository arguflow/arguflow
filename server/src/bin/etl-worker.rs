use actix_web::web;
use broccoli_queue::brokers::broker::BrokerMessage;
use broccoli_queue::{error::BroccoliError, queue::BroccoliQueue};
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::error::Error;
use trieve_server::data::models::{EventType, UnifiedId, WorkerEvent};
use trieve_server::handlers::chunk_handler::{
    FullTextBoost, SemanticBoost, UpdateIngestionMessage,
};
use trieve_server::handlers::etl_handler::{EtlJobMessage, EtlJobRequest, EtlWebhookResponse};
use trieve_server::operators::chunk_operator::{get_chunk_boost_query, get_metadata_from_id_query};
use trieve_server::operators::clickhouse_operator::ClickHouseEvent;
use trieve_server::operators::dataset_operator::get_dataset_config_query;
use trieve_server::operators::etl_operator::get_all_chunks_for_dataset_id;
use trieve_server::operators::group_operator::get_groups_for_bookmark_query;
use trieve_server::{
    data::models::Pool, establish_connection, get_env, operators::clickhouse_operator::EventQueue,
};
use ureq::json;

#[derive(serde::Deserialize)]
pub struct Schema {
    id: String,
}

#[derive(serde::Deserialize)]
pub struct Input {
    input_id: String,
    s3_put_url: String,
}

async fn create_job(job: EtlJobRequest, pool: web::Data<Pool>) -> Result<(), BroccoliError> {
    let batch_etl_url = get_env!("BATCH_ETL_URL", "BATCH_ETL_URL is not set").to_string();
    let dataset_config = get_dataset_config_query(job.dataset_id, pool.clone())
        .await
        .map_err(|e| BroccoliError::Job(format!("Failed to get dataset config {:?}", e)))?;

    let mut schema = json!({
      "type": "object",
      "properties": {
        "tag_set": {
          "type": "array",
          "items": {
            "type": "string",
          }
        },
        "chunk_html": { "type": "string" }
      },
      "required": ["tag_set", "chunk_html"],
      "additionalProperties": false
    });
    if let Some(tag_enum) = job.payload.tag_enum {
        schema["properties"]["tag_set"]["items"]["enum"] = tag_enum
            .into_iter()
            .map(serde_json::Value::String)
            .collect();
    }

    let schema_id = ureq::post(format!("{}/api/schema", batch_etl_url).as_str())
        .send_json(json!({
            "name": format!("{}-schema", job.dataset_id),
            "schema": schema
        }))
        .map_err(|e| BroccoliError::Job(format!("Failed to create schema {:?}", e)))?
        .into_json::<Schema>()
        .map_err(|e| BroccoliError::Job(format!("Failed to create schema {:?}", e)))?
        .id;

    let input = ureq::post(format!("{}/api/input", batch_etl_url).as_str())
        .send_json(json!({}))
        .map_err(|e| BroccoliError::Job(format!("Failed to create input {:?}", e)))?
        .into_json::<Input>()
        .map_err(|e| BroccoliError::Job(format!("Failed to create input {:?}", e)))?;

    let chunks = get_all_chunks_for_dataset_id(job.dataset_id, dataset_config, pool)
        .await
        .map_err(|e| BroccoliError::Job(format!("Failed to get all chunks {:?}", e)))?;

    let joined_chunks = chunks.join("\n");
    // Create a reqwest client
    let client = Client::new();

    // Send the PUT request with the file content
    client
        .put(input.s3_put_url)
        .body(joined_chunks.clone().into_bytes())
        .send()
        .await
        .map_err(|e| BroccoliError::Job(format!("Failed to upload chunks {:?}", e)))?;

    ureq::post(format!("{}/api/job", batch_etl_url).as_str())
        .send_json(json!({
                "schema_id": schema_id,
                "input_id": input.input_id,
                "model": job.payload.model,
                "job_id": job.dataset_id,
                "system_prompt": job.payload.prompt,
                "image_options": {
                    "use_images": job.payload.include_images.unwrap_or(false),
                    "image_key": "image_urls"
                },
                "custom_id": "id",
                "webhook_url": format!("{}/api/etl/webhook", get_env!("BASE_SERVER_URL", "Server hostname for OpenID provider must be set" )),
            }))
        .map_err(|e| BroccoliError::Job(format!("Failed to create job {:?}", e)))?;

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchRequest {
    pub custom_id: String,
    pub response: Response,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub request_id: String,
    pub body: ResponseBody,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseBody {
    pub choices: Vec<Choice>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Choice {
    pub message: Message,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub content: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransformedChunk {
    pub chunk_html: String,
    pub tag_set: Vec<String>,
}

async fn webhook_response(
    job: EtlWebhookResponse,
    broccoli_queue: BroccoliQueue,
    pool: web::Data<Pool>,
) -> Result<(), BroccoliError> {
    let mut batches = vec![];

    ureq::get(job.batch_url.as_str())
        .call()
        .map_err(|e| BroccoliError::Job(format!("Failed to get batch {:?}", e)))?
        .into_reader()
        .read_to_end(&mut batches)
        .map_err(|e| BroccoliError::Job(format!("Failed to read batch {:?}", e)))?;

    let batch_requests: Vec<BatchRequest> = String::from_utf8_lossy(&batches)
        .split("\n")
        .filter(|x| !x.is_empty())
        .map(|x: &str| {
            let batch: BatchRequest = serde_json::from_str(x).unwrap();
            batch
        })
        .collect();

    for batch_request in batch_requests {
        let chunk_id: uuid::Uuid = batch_request
            .custom_id
            .replace("\"", "")
            .parse()
            .map_err(|e| BroccoliError::Job(format!("Failed to parse chunk id {:?}", e)))?;

        let dataset_id: uuid::Uuid = job.job_id.parse().unwrap();
        let mut original_chunk = get_metadata_from_id_query(chunk_id, dataset_id, pool.clone())
            .await
            .map_err(|e| BroccoliError::Job(format!("Failed to get chunk {:?}", e)))?;

        let transformed_chunk: Result<TransformedChunk, BroccoliError> = serde_json::from_str(
            &batch_request.response.body.choices[0]
                .message
                .content
                .clone(),
        )
        .map_err(|e| BroccoliError::Job(format!("Failed to parse transformed chunk {:?}", e)));

        let transformed_chunk = if let Ok(transformed_chunk) = transformed_chunk {
            transformed_chunk
        } else {
            log::error!("Failed to parse transformed chunk {:?}", transformed_chunk);
            continue;
        };

        original_chunk.chunk_html = Some(transformed_chunk.chunk_html);
        original_chunk.tag_set = Some(transformed_chunk.tag_set.into_iter().map(Some).collect());

        let boosts = get_chunk_boost_query(chunk_id, pool.clone())
            .await
            .map_err(|e| BroccoliError::Job(format!("Failed to get chunk boost {:?}", e)))?;

        let fulltext_boost = boosts.clone().and_then(|x| {
            x.fulltext_boost_phrase
                .zip(x.fulltext_boost_factor.map(|f| f as f32))
                .map(|(phrase, boost_factor)| FullTextBoost {
                    phrase,
                    boost_factor: boost_factor.into(),
                })
        });

        let semantic_boost = boosts.and_then(|x| {
            x.semantic_boost_phrase
                .zip(x.semantic_boost_factor.map(|f| f as f32))
                .map(|(phrase, distance_factor)| SemanticBoost {
                    phrase,
                    distance_factor,
                })
        });

        let chunk_group_ids: Vec<UnifiedId> =
            get_groups_for_bookmark_query(vec![chunk_id], dataset_id, pool.clone())
                .await
                .map_err(|e| {
                    BroccoliError::Job(format!("Failed to get chunk group ids {:?}", e))
                })?[0]
                .slim_groups
                .iter()
                .map(|x| UnifiedId::TrieveUuid(x.id))
                .collect();

        let message = UpdateIngestionMessage {
            chunk_metadata: original_chunk.clone().into(),
            dataset_id,
            group_ids: Some(chunk_group_ids),
            convert_html_to_text: Some(true),
            fulltext_boost: fulltext_boost.clone(),
            semantic_boost: semantic_boost.clone(),
            only_qdrant: Some(false),
        };

        broccoli_queue
            .publish("update_chunk_queue", None, &message, None)
            .await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    env_logger::builder()
        .target(env_logger::Target::Stdout)
        .filter_level(log::LevelFilter::Info)
        .init();

    let database_url = get_env!("DATABASE_URL", "DATABASE_URL is not set");

    let mut config = ManagerConfig::default();
    config.custom_setup = Box::new(establish_connection);

    let mgr = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new_with_config(
        database_url,
        config,
    );

    let pool = diesel_async::pooled_connection::deadpool::Pool::builder(mgr)
        .max_size(3)
        .build()
        .expect("Failed to create diesel_async pool");

    let web_pool = actix_web::web::Data::new(pool.clone());

    let event_queue = if std::env::var("USE_ANALYTICS")
        .unwrap_or("false".to_string())
        .parse()
        .unwrap_or(false)
    {
        log::info!("Analytics enabled");

        let clickhouse_client = clickhouse::Client::default()
            .with_url(
                std::env::var("CLICKHOUSE_URL").unwrap_or("http://localhost:8123".to_string()),
            )
            .with_user(std::env::var("CLICKHOUSE_USER").unwrap_or("default".to_string()))
            .with_password(std::env::var("CLICKHOUSE_PASSWORD").unwrap_or("".to_string()))
            .with_database(std::env::var("CLICKHOUSE_DATABASE").unwrap_or("default".to_string()))
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0");

        let mut event_queue = EventQueue::new(clickhouse_client.clone());
        event_queue.start_service();
        event_queue
    } else {
        log::info!("Analytics disabled");
        EventQueue::default()
    };

    let redis_url = get_env!("REDIS_URL", "REDIS_URL is not set");

    let redis_connections: u32 = std::env::var("REDIS_CONNECTIONS")
        .unwrap_or("2".to_string())
        .parse()
        .unwrap_or(2);

    let queue = BroccoliQueue::builder(redis_url)
        .pool_connections(redis_connections.try_into().unwrap())
        .failed_message_retry_strategy(Default::default())
        .build()
        .await?;

    queue
        .clone()
        .process_messages_with_handlers(
            "etl_queue",
            None,
            None,
            move |msg: BrokerMessage<EtlJobMessage>| {
                let pool = web_pool.clone();
                let broccoli_queue = queue.clone();
                async move { etl_worker(msg.payload, pool.clone(), broccoli_queue.clone()).await }
            },
            {
                let event_queue = event_queue.clone();
                move |msg| {
                    let event_queue = event_queue.clone();
                    async move {
                        match msg.payload {
                            EtlJobMessage::CreateJob(job) => {
                                event_queue
                                    .send(ClickHouseEvent::WorkerEvent(
                                        WorkerEvent::from_details(
                                            job.dataset_id,
                                            EventType::EtlStarted {
                                                prompt: job.payload.prompt,
                                                model: job.payload.model,
                                                tag_enum: job.payload.tag_enum,
                                                include_images: job.payload.include_images,
                                            },
                                        )
                                        .into(),
                                    ))
                                    .await;
                            }
                            EtlJobMessage::WebhookResponse(job) => {
                                event_queue
                                    .send(ClickHouseEvent::WorkerEvent(
                                        WorkerEvent::from_details(
                                            job.job_id.parse().unwrap(),
                                            EventType::EtlCompleted,
                                        )
                                        .into(),
                                    ))
                                    .await;
                            }
                        }

                        log::info!("Processed message: {:?}", msg.task_id);
                        Ok(())
                    }
                }
            },
            {
                let event_queue = event_queue.clone();
                move |msg, err| {
                    let event_queue = event_queue.clone();
                    async move {
                        match msg.payload {
                            EtlJobMessage::CreateJob(job) => {
                                event_queue
                                    .send(ClickHouseEvent::WorkerEvent(
                                        WorkerEvent::from_details(
                                            job.dataset_id,
                                            EventType::EtlFailed {
                                                error: err.to_string(),
                                            },
                                        )
                                        .into(),
                                    ))
                                    .await;
                            }
                            EtlJobMessage::WebhookResponse(job) => {
                                event_queue
                                    .send(ClickHouseEvent::WorkerEvent(
                                        WorkerEvent::from_details(
                                            job.job_id.parse().unwrap(),
                                            EventType::EtlFailed {
                                                error: err.to_string(),
                                            },
                                        )
                                        .into(),
                                    ))
                                    .await;
                            }
                        }
                        log::error!("Error processing message: {:?}", err);
                        Ok(())
                    }
                }
            },
        )
        .await?;

    Ok(())
}

pub async fn etl_worker(
    payload: EtlJobMessage,
    pool: web::Data<Pool>,
    broccoli_queue: BroccoliQueue,
) -> Result<(), BroccoliError> {
    match payload {
        EtlJobMessage::CreateJob(job) => {
            log::info!("Processing ETL job {:?}", job.dataset_id);

            create_job(job, pool.clone()).await?;
        }
        EtlJobMessage::WebhookResponse(data) => {
            log::info!("Processing ETL webhook response {:?}", data.job_id);

            webhook_response(data, broccoli_queue, pool.clone()).await?;
        }
    }

    Ok(())
}
