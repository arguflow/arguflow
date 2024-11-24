use chrono::NaiveDateTime;
use dateparser::DateTimeUtc;
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use futures_util::StreamExt;
use itertools::{izip, Itertools};
use qdrant_client::qdrant::{PointStruct, Vector};
use sentry::{Hub, SentryFutureExt};
use signal_hook::consts::SIGTERM;
use std::collections::HashMap;
use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc};
use tracing_subscriber::{prelude::*, EnvFilter, Layer};
use trieve_server::data::models::{
    self, ChunkBoost, ChunkMetadata, DatasetConfiguration, QdrantPayload, UnifiedId, WorkerEvent,
};
use trieve_server::errors::ServiceError;
use trieve_server::handlers::chunk_handler::{
    BulkUploadIngestionMessage, FullTextBoost, SemanticBoost, UpdateIngestionMessage,
    UploadIngestionMessage,
};
use trieve_server::handlers::group_handler::dataset_owns_group;
use trieve_server::operators::chunk_operator::{
    bulk_insert_chunk_metadata_query, bulk_revert_insert_chunk_metadata_query,
    get_row_count_for_organization_id_query, insert_chunk_boost, insert_chunk_metadata_query,
    update_chunk_boost_query, update_chunk_metadata_query, update_dataset_chunk_count,
};
use trieve_server::operators::clickhouse_operator::{ClickHouseEvent, EventQueue};
use trieve_server::operators::dataset_operator::{
    get_dataset_and_organization_from_dataset_id_query, get_dataset_by_id_query,
};
use trieve_server::operators::group_operator::get_groups_from_group_ids_query;
use trieve_server::operators::model_operator::{
    get_bm25_embeddings, get_dense_vector, get_dense_vectors, get_sparse_vectors,
};
use trieve_server::operators::parse_operator::{
    average_embeddings, coarse_doc_chunker, convert_html_to_text,
};
use trieve_server::operators::qdrant_operator::{
    bulk_upsert_qdrant_points_query, update_qdrant_point_query,
};
use trieve_server::{establish_connection, get_env};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum IngestionMessage {
    BulkUpload(BulkUploadIngestionMessage),
    Update(UpdateIngestionMessage),
}

fn main() {
    dotenvy::dotenv().ok();
    let sentry_url = std::env::var("SENTRY_URL");
    let _guard = if let Ok(sentry_url) = sentry_url {
        let guard = sentry::init((
            sentry_url,
            sentry::ClientOptions {
                release: sentry::release_name!(),
                traces_sample_rate: 1.0,
                ..Default::default()
            },
        ));

        tracing_subscriber::Registry::default()
            .with(sentry::integrations::tracing::layer())
            .with(
                tracing_subscriber::fmt::layer().with_filter(
                    EnvFilter::from_default_env()
                        .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into()),
                ),
            )
            .init();

        log::info!("Sentry monitoring enabled");
        Some(guard)
    } else {
        tracing_subscriber::Registry::default()
            .with(
                tracing_subscriber::fmt::layer().with_filter(
                    EnvFilter::from_default_env()
                        .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into()),
                ),
            )
            .init();

        None
    };

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

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime")
        .block_on(
            async move {
                let redis_url = get_env!("REDIS_URL", "REDIS_URL is not set");
                let redis_connections: u32 = std::env::var("REDIS_CONNECTIONS")
                    .unwrap_or("2".to_string())
                    .parse()
                    .unwrap_or(2);

                let redis_manager = bb8_redis::RedisConnectionManager::new(redis_url)
                    .expect("Failed to connect to redis");

                let redis_pool = bb8_redis::bb8::Pool::builder()
                    .max_size(redis_connections)
                    .connection_timeout(std::time::Duration::from_secs(2))
                    .build(redis_manager)
                    .await
                    .expect("Failed to create redis pool");

                let web_redis_pool = actix_web::web::Data::new(redis_pool);

                let event_queue = if std::env::var("USE_ANALYTICS")
                    .unwrap_or("false".to_string())
                    .parse()
                    .unwrap_or(false)
                {
                    log::info!("Analytics enabled");

                    let clickhouse_client = clickhouse::Client::default()
                        .with_url(
                            std::env::var("CLICKHOUSE_URL")
                                .unwrap_or("http://localhost:8123".to_string()),
                        )
                        .with_user(
                            std::env::var("CLICKHOUSE_USER").unwrap_or("default".to_string()),
                        )
                        .with_password(
                            std::env::var("CLICKHOUSE_PASSWORD").unwrap_or("".to_string()),
                        )
                        .with_database(
                            std::env::var("CLICKHOUSE_DATABASE").unwrap_or("default".to_string()),
                        )
                        .with_option("async_insert", "1")
                        .with_option("wait_for_async_insert", "0");

                    let mut event_queue = EventQueue::new(clickhouse_client.clone());
                    event_queue.start_service();
                    event_queue
                } else {
                    log::info!("Analytics disabled");
                    EventQueue::default()
                };
                let web_event_queue = actix_web::web::Data::new(event_queue);

                let should_terminate = Arc::new(AtomicBool::new(false));
                signal_hook::flag::register(SIGTERM, Arc::clone(&should_terminate))
                    .expect("Failed to register shutdown hook");

                ingestion_worker(should_terminate, web_redis_pool, web_pool, web_event_queue).await
            }
            .bind_hub(Hub::new_from_top(Hub::current())),
        );
}

#[tracing::instrument(skip(should_terminate, web_pool, redis_pool, event_queue))]
async fn ingestion_worker(
    should_terminate: Arc<AtomicBool>,
    redis_pool: actix_web::web::Data<models::RedisPool>,
    web_pool: actix_web::web::Data<models::Pool>,
    event_queue: actix_web::web::Data<EventQueue>,
) {
    log::info!("Starting ingestion service thread");

    let mut redis_conn_sleep = std::time::Duration::from_secs(1);

    #[allow(unused_assignments)]
    let mut opt_redis_connection = None;

    loop {
        let borrowed_redis_connection = match redis_pool.get().await {
            Ok(redis_connection) => Some(redis_connection),
            Err(err) => {
                log::error!("Failed to get redis connection outside of loop: {:?}", err);
                None
            }
        };

        if borrowed_redis_connection.is_some() {
            opt_redis_connection = borrowed_redis_connection;
            break;
        }

        tokio::time::sleep(redis_conn_sleep).await;
        redis_conn_sleep = std::cmp::min(redis_conn_sleep * 2, std::time::Duration::from_secs(300));
    }

    let mut redis_connection =
        opt_redis_connection.expect("Failed to get redis connection outside of loop");

    let mut broken_pipe_sleep = std::time::Duration::from_secs(10);
    let reqwest_client = reqwest::Client::new();

    loop {
        if should_terminate.load(Ordering::Relaxed) {
            log::info!("Shutting down");
            break;
        }

        let payload_result: Result<Vec<String>, redis::RedisError> = redis::cmd("brpoplpush")
            .arg("ingestion")
            .arg("processing")
            .arg(1.0)
            .query_async(&mut *redis_connection)
            .await;

        let serialized_message = match payload_result {
            Ok(payload) => {
                broken_pipe_sleep = std::time::Duration::from_secs(10);

                if payload.is_empty() {
                    continue;
                }

                payload
                    .first()
                    .expect("Payload must have a first element")
                    .clone()
            }
            Err(err) => {
                log::error!("Unable to process {:?}", err);

                if err.is_io_error() {
                    tokio::time::sleep(broken_pipe_sleep).await;
                    broken_pipe_sleep =
                        std::cmp::min(broken_pipe_sleep * 2, std::time::Duration::from_secs(300));
                }

                continue;
            }
        };

        let processing_chunk_ctx = sentry::TransactionContext::new(
            "ingestion worker processing chunk",
            "ingestion worker processing chunk",
        );
        let transaction = sentry::start_transaction(processing_chunk_ctx);
        let ingestion_message: IngestionMessage = match serde_json::from_str(&serialized_message) {
            Ok(message) => message,
            Err(err) => {
                log::error!(
                    "Failed to deserialize message, was not an IngestionMessage: {:?}",
                    err
                );
                transaction.finish();
                continue;
            }
        };

        let dataset_result: Result<models::Dataset, ServiceError> = match ingestion_message.clone()
        {
            IngestionMessage::Update(payload) => {
                get_dataset_by_id_query(UnifiedId::TrieveUuid(payload.dataset_id), web_pool.clone())
                    .await
            }
            IngestionMessage::BulkUpload(payload) => {
                get_dataset_by_id_query(UnifiedId::TrieveUuid(payload.dataset_id), web_pool.clone())
                    .await
            }
        };
        let dataset = match dataset_result {
            Ok(dataset) => dataset,
            Err(err) => {
                let _ = readd_error_to_queue(
                    ingestion_message,
                    err.clone(),
                    redis_pool.clone(),
                    event_queue.clone(),
                )
                .await;
                log::error!("Failed to get dataset; likely does not exist: {:?}", err);
                transaction.finish();
                continue;
            }
        };
        let dataset_config = DatasetConfiguration::from_json(dataset.server_configuration);

        match ingestion_message.clone() {
            IngestionMessage::BulkUpload(payload) => {
                match bulk_upload_chunks(
                    payload.clone(),
                    dataset_config.clone(),
                    web_pool.clone(),
                    reqwest_client.clone(),
                )
                .await
                {
                    Ok(chunk_ids) => {
                        log::info!("Uploaded {:} chunks", chunk_ids.len());

                        event_queue
                            .send(ClickHouseEvent::WorkerEvent(
                                WorkerEvent::from_details(
                                    payload.dataset_id,
                                    models::EventType::ChunksUploaded { chunk_ids },
                                )
                                .into(),
                            ))
                            .await;

                        let _ = redis::cmd("LREM")
                            .arg("processing")
                            .arg(1)
                            .arg(serialized_message)
                            .query_async::<redis::aio::MultiplexedConnection, usize>(
                                &mut *redis_connection,
                            )
                            .await;
                    }
                    Err(err) => {
                        log::error!("Failed to upload chunk: {:?}", err);

                        let _ = readd_error_to_queue(
                            ingestion_message,
                            err,
                            redis_pool.clone(),
                            event_queue.clone(),
                        )
                        .await;
                    }
                }
            }

            IngestionMessage::Update(payload) => {
                match update_chunk(payload.clone(), web_pool.clone(), dataset_config).await {
                    Ok(_) => {
                        log::info!("Updated chunk: {:?}", payload.chunk_metadata.id);
                        event_queue
                            .send(ClickHouseEvent::WorkerEvent(
                                WorkerEvent::from_details(
                                    payload.dataset_id,
                                    models::EventType::ChunkUpdated {
                                        chunk_id: payload.chunk_metadata.id,
                                    },
                                )
                                .into(),
                            ))
                            .await;

                        let _ = redis::cmd("LREM")
                            .arg("processing")
                            .arg(1)
                            .arg(serialized_message)
                            .query_async::<redis::aio::MultiplexedConnection, usize>(
                                &mut *redis_connection,
                            )
                            .await;
                    }
                    Err(err) => {
                        let _ = readd_error_to_queue(
                            ingestion_message,
                            err,
                            redis_pool.clone(),
                            event_queue.clone(),
                        )
                        .await;
                    }
                }
            }
        }
        transaction.finish();
    }
}

#[derive(Debug, Clone)]
pub struct ChunkDataWithEmbeddingText {
    pub chunk_metadata: ChunkMetadata,
    pub content: String,
    pub embedding_content: String,
    pub group_ids: Option<Vec<uuid::Uuid>>,
    pub upsert_by_tracking_id: bool,
    pub fulltext_boost: Option<FullTextBoost>,
    pub semantic_boost: Option<SemanticBoost>,
}

impl From<ChunkDataWithEmbeddingText> for models::ChunkData {
    fn from(data: ChunkDataWithEmbeddingText) -> Self {
        models::ChunkData {
            chunk_metadata: data.chunk_metadata,
            content: data.content,
            group_ids: data.group_ids,
            upsert_by_tracking_id: data.upsert_by_tracking_id,
            fulltext_boost: data.fulltext_boost,
            semantic_boost: data.semantic_boost,
        }
    }
}

#[tracing::instrument(skip(payload, web_pool))]
pub async fn bulk_upload_chunks(
    payload: BulkUploadIngestionMessage,
    dataset_config: DatasetConfiguration,
    web_pool: actix_web::web::Data<models::Pool>,
    reqwest_client: reqwest::Client,
) -> Result<Vec<uuid::Uuid>, ServiceError> {
    let tx_ctx = sentry::TransactionContext::new(
        "ingestion worker bulk_upload_chunk",
        "ingestion worker bulk_upload_chunk",
    );
    let transaction = sentry::start_transaction(tx_ctx);

    let precompute_transaction = transaction.start_child(
        "precomputing_data_before_insert",
        "precomputing some important data before insert",
    );

    let unlimited = std::env::var("UNLIMITED").unwrap_or("false".to_string());
    if unlimited == "false" {
        let dataset_org_plan_sub = get_dataset_and_organization_from_dataset_id_query(
            models::UnifiedId::TrieveUuid(payload.dataset_id),
            None,
            web_pool.clone(),
        )
        .await?;

        let chunk_count = get_row_count_for_organization_id_query(
            dataset_org_plan_sub.organization.organization.id,
            web_pool.clone(),
        )
        .await?;

        if chunk_count + payload.ingestion_messages.len()
            > dataset_org_plan_sub
                .organization
                .plan
                .unwrap_or_default()
                .chunk_count as usize
        {
            return Err(ServiceError::BadRequest(
                "Chunk count exceeds plan limit".to_string(),
            ));
        }
    }

    // Being blocked out because it is difficult to create multiple split_avg embeddings in batch
    let split_average_being_used = payload
        .ingestion_messages
        .iter()
        .any(|message| message.chunk.split_avg.unwrap_or(false));

    let upsert_by_tracking_id_being_used = payload
        .ingestion_messages
        .iter()
        .any(|message| message.upsert_by_tracking_id);

    let ingestion_data: Vec<ChunkDataWithEmbeddingText> = payload
        .ingestion_messages
        .iter()
        .map(|message| {
            let content = if message.chunk.convert_html_to_text.unwrap_or(true) {
                convert_html_to_text(&(message.chunk.chunk_html.clone().unwrap_or_default()))
            } else {
                message.chunk.chunk_html.clone().unwrap_or_default()
            };

            let qdrant_point_id = message.ingest_specific_chunk_metadata.qdrant_point_id;

            let chunk_tag_set = message.chunk.tag_set.clone().map(|tag_set| {
                tag_set
                    .into_iter()
                    .map(|tag| Some(tag.to_string()))
                    .collect::<Vec<Option<String>>>()
            });

            let timestamp = {
                message
                    .chunk
                    .time_stamp
                    .clone()
                    .and_then(|ts| -> Option<NaiveDateTime> {
                        ts.parse::<DateTimeUtc>()
                            .ok()
                            .map(|date| date.0.with_timezone(&chrono::Local).naive_local())
                    })
            };

            let chunk_tracking_id = message
                .chunk
                .tracking_id
                .clone()
                .filter(|chunk_tracking| !chunk_tracking.is_empty());

            let chunk_metadata = ChunkMetadata {
                id: message.ingest_specific_chunk_metadata.id,
                link: message.chunk.link.clone(),
                qdrant_point_id,
                created_at: chrono::Utc::now().naive_local(),
                updated_at: chrono::Utc::now().naive_local(),
                chunk_html: message.chunk.chunk_html.clone(),
                metadata: message.chunk.metadata.clone(),
                tracking_id: chunk_tracking_id,
                time_stamp: timestamp,
                location: message.chunk.location,
                dataset_id: payload.dataset_id,
                weight: message.chunk.weight.unwrap_or(0.0),
                image_urls: message
                    .chunk
                    .image_urls
                    .clone()
                    .map(|urls| urls.into_iter().map(Some).collect()),
                tag_set: chunk_tag_set,
                num_value: message.chunk.num_value,
            };

            ChunkDataWithEmbeddingText {
                chunk_metadata,
                content: content.clone(),
                embedding_content: message.chunk.semantic_content.clone().unwrap_or(content),
                group_ids: message.chunk.group_ids.clone(),
                upsert_by_tracking_id: message.upsert_by_tracking_id,
                fulltext_boost: message
                    .chunk
                    .fulltext_boost
                    .clone()
                    .filter(|boost| !boost.phrase.is_empty()),
                semantic_boost: message
                    .chunk
                    .semantic_boost
                    .clone()
                    .filter(|boost| !boost.phrase.is_empty()),
            }
        })
        .filter(|data| !data.content.is_empty())
        .collect();

    if split_average_being_used {
        let mut chunk_ids = vec![];
        // Split average or Collisions
        for (message, ingestion_data) in izip!(payload.ingestion_messages, ingestion_data) {
            let upload_chunk_result = upload_chunk(
                message,
                dataset_config.clone(),
                ingestion_data,
                web_pool.clone(),
                reqwest_client.clone(),
            )
            .await;

            if let Ok(chunk_uuid) = upload_chunk_result {
                chunk_ids.push(chunk_uuid);
            }
        }

        transaction.finish();
        return Ok(chunk_ids);
    }

    precompute_transaction.finish();

    let insert_tx = transaction.start_child(
        "calling_BULK_insert_chunk_metadata_query",
        "calling_BULK_insert_chunk_metadata_query",
    );

    let only_insert_qdrant = dataset_config.QDRANT_ONLY;

    let inserted_chunk_metadatas = if only_insert_qdrant {
        ingestion_data
            .clone()
            .into_iter()
            .map(|data| data.into())
            .collect_vec()
    } else {
        bulk_insert_chunk_metadata_query(
            ingestion_data
                .clone()
                .into_iter()
                .map(|data| data.into())
                .collect_vec(),
            payload.dataset_id,
            upsert_by_tracking_id_being_used,
            web_pool.clone(),
        )
        .await?
    };

    insert_tx.finish();

    if inserted_chunk_metadatas.is_empty() {
        // All collisions
        return Ok(vec![]);
    }

    // Only embed the things we get returned from here, this reduces the number of times we embed data that are just duplicates
    let embedding_content_and_boosts: Vec<(String, Option<FullTextBoost>, Option<SemanticBoost>)> =
        ingestion_data
            .iter()
            .map(|data| {
                (
                    data.embedding_content.clone(),
                    data.fulltext_boost.clone(),
                    data.semantic_boost.clone(),
                )
            })
            .collect();

    let inserted_chunk_metadata_ids: Vec<uuid::Uuid> = inserted_chunk_metadatas
        .iter()
        .map(|chunk_data| chunk_data.chunk_metadata.id)
        .unique()
        .collect();

    let embedding_transaction = transaction.start_child(
        "calling_create_all_embeddings",
        "calling_create_all_embeddings",
    );

    let embedding_vectors = match dataset_config.SEMANTIC_ENABLED {
        true => {
            let vectors = match get_dense_vectors(
                embedding_content_and_boosts
                    .iter()
                    .map(|(content, _, semantic_boost)| (content.clone(), semantic_boost.clone()))
                    .collect(),
                "doc",
                dataset_config.clone(),
                reqwest_client.clone(),
            )
            .await
            {
                Ok(vectors) => Ok(vectors),
                Err(err) => {
                    if !upsert_by_tracking_id_being_used {
                        bulk_revert_insert_chunk_metadata_query(
                            inserted_chunk_metadata_ids.clone(),
                            web_pool.clone(),
                        )
                        .await?;
                    }
                    Err(ServiceError::InternalServerError(format!(
                        "Failed to create embeddings: {:?}",
                        err
                    )))
                }
            }?;
            vectors.into_iter().map(Some).collect()
        }
        false => vec![None; embedding_content_and_boosts.len()],
    };

    // Assuming split average is false, Assume Explicit Vectors don't exist
    embedding_transaction.finish();

    let embedding_transaction = transaction.start_child(
        "calling_create_SPLADE_embeddings",
        "calling_create_SPLADE_embeddings",
    );

    let content_and_boosts: Vec<(String, Option<FullTextBoost>, Option<SemanticBoost>)> =
        ingestion_data
            .iter()
            .map(|data| {
                (
                    data.content.clone(),
                    data.fulltext_boost.clone(),
                    data.semantic_boost.clone(),
                )
            })
            .collect();

    let splade_vectors = if dataset_config.FULLTEXT_ENABLED {
        match get_sparse_vectors(
            content_and_boosts
                .iter()
                .map(|(content, boost, _)| (content.clone(), boost.clone()))
                .collect(),
            "doc",
            reqwest_client,
        )
        .await
        {
            Ok(vectors) => Ok(vectors),
            Err(err) => {
                if !upsert_by_tracking_id_being_used {
                    bulk_revert_insert_chunk_metadata_query(
                        inserted_chunk_metadata_ids.clone(),
                        web_pool.clone(),
                    )
                    .await?;
                }
                Err(err)
            }
        }
    } else {
        let content_size = content_and_boosts.len();

        Ok(std::iter::repeat(vec![(0, 0.0)])
            .take(content_size)
            .collect())
    }?;

    let bm25_vectors = if dataset_config.BM25_ENABLED
        && std::env::var("BM25_ACTIVE").unwrap_or("false".to_string()) == "true"
    {
        get_bm25_embeddings(
            content_and_boosts
                .iter()
                .map(|(content, boost, _)| (content.clone(), boost.clone()))
                .collect(),
            dataset_config.BM25_AVG_LEN,
            dataset_config.BM25_B,
            dataset_config.BM25_K,
        )
        .into_iter()
        .map(Some)
        .collect()
    } else {
        vec![None; content_and_boosts.len()]
    };

    embedding_transaction.finish();

    let qdrant_points = tokio_stream::iter(izip!(
        inserted_chunk_metadatas.clone(),
        embedding_vectors.iter(),
        splade_vectors.iter(),
        bm25_vectors.iter()
    ))
    .then(
        |(chunk_data, embedding_vector, splade_vector, bm25_vector)| async {
            let mut qdrant_point_id = chunk_data.chunk_metadata.qdrant_point_id;
            if only_insert_qdrant {
                if let Some(tracking_id) = chunk_data.clone().chunk_metadata.tracking_id {
                    qdrant_point_id =
                        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, tracking_id.as_bytes());
                }
            }

            let chunk_tags: Option<Vec<Option<String>>> =
                if let Some(ref group_ids) = chunk_data.group_ids {
                    Some(
                        get_groups_from_group_ids_query(group_ids.clone(), web_pool.clone())
                            .await?
                            .iter()
                            .filter_map(|group| group.tag_set.clone())
                            .flatten()
                            .dedup()
                            .collect(),
                    )
                } else {
                    None
                };

            let payload = QdrantPayload::new(
                chunk_data.chunk_metadata,
                chunk_data.group_ids,
                None,
                chunk_tags,
            );

            let mut vector_payload = HashMap::from([(
                "sparse_vectors".to_string(),
                Vector::from(splade_vector.clone()),
            )]);

            if let Some(vector) = embedding_vector.clone() {
                let vector_name = match vector.len() {
                    384 => "384_vectors",
                    512 => "512_vectors",
                    768 => "768_vectors",
                    1024 => "1024_vectors",
                    3072 => "3072_vectors",
                    1536 => "1536_vectors",
                    _ => {
                        return Err(ServiceError::BadRequest(
                            "Invalid embedding vector size".into(),
                        ))
                    }
                };
                vector_payload.insert(
                    vector_name.to_string().clone(),
                    Vector::from(vector.clone()),
                );
            }

            if let Some(bm25_vector) = bm25_vector.clone() {
                vector_payload.insert(
                    "bm25_vectors".to_string(),
                    Vector::from(bm25_vector.clone()),
                );
            }

            Ok(PointStruct::new(
                qdrant_point_id.to_string(),
                vector_payload,
                payload,
            ))
        },
    )
    .collect::<Vec<Result<PointStruct, ServiceError>>>()
    .await;

    if qdrant_points.iter().any(|point| point.is_err()) {
        Err(ServiceError::InternalServerError(
            "Failed to create qdrant points".to_string(),
        ))?;
    }

    let qdrant_points: Vec<PointStruct> = qdrant_points
        .into_iter()
        .filter_map(|point| point.ok())
        .collect();

    let insert_tx = transaction.start_child(
        "calling_BULK_create_new_qdrant_points_query",
        "calling_BULK_create_new_qdrant_points_query",
    );

    let create_point_result: Result<(), ServiceError> =
        bulk_upsert_qdrant_points_query(qdrant_points, dataset_config.clone()).await;

    insert_tx.finish();

    if !only_insert_qdrant {
        if let Err(err) = create_point_result {
            if !upsert_by_tracking_id_being_used {
                bulk_revert_insert_chunk_metadata_query(
                    inserted_chunk_metadata_ids,
                    web_pool.clone(),
                )
                .await?;
            }

            return Err(err);
        }
    } else {
        create_point_result?;
        update_dataset_chunk_count(
            payload.dataset_id,
            inserted_chunk_metadata_ids.len() as i32,
            web_pool.clone(),
        )
        .await?;
    }

    Ok(inserted_chunk_metadata_ids)
}

#[tracing::instrument(skip(payload, web_pool))]
async fn upload_chunk(
    mut payload: UploadIngestionMessage,
    dataset_config: DatasetConfiguration,
    ingestion_data: ChunkDataWithEmbeddingText,
    web_pool: actix_web::web::Data<models::Pool>,
    reqwest_client: reqwest::Client,
) -> Result<uuid::Uuid, ServiceError> {
    let dataset_id = payload.dataset_id;
    let qdrant_only = dataset_config.QDRANT_ONLY;
    let mut qdrant_point_id = uuid::Uuid::new_v4();
    if qdrant_only {
        if let Some(tracking_id) = payload.chunk.tracking_id.clone() {
            qdrant_point_id =
                uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, tracking_id.as_bytes());
        }
    }

    let content = match payload.chunk.convert_html_to_text.unwrap_or(true) {
        true => convert_html_to_text(&(payload.chunk.chunk_html.clone().unwrap_or_default())),
        false => payload.chunk.chunk_html.clone().unwrap_or_default(),
    };

    let pre_parsed_content = ingestion_data.embedding_content;
    let semantic_content = match payload.chunk.convert_html_to_text.unwrap_or(true) {
        true => convert_html_to_text(&pre_parsed_content),
        false => pre_parsed_content.clone(),
    };

    // Only embed the things we get returned from here, this reduces the number of times we embed data that are just duplicates
    let content_and_boosts: Vec<(String, Option<FullTextBoost>)> = vec![(
        ingestion_data.content.clone(),
        ingestion_data.fulltext_boost.clone(),
    )];

    let chunk_tag_set = payload.chunk.tag_set.clone().map(|tag_set| {
        tag_set
            .into_iter()
            .map(|tag| Some(tag.to_string()))
            .collect::<Vec<Option<String>>>()
    });

    let chunk_tracking_id = payload
        .chunk
        .tracking_id
        .clone()
        .filter(|chunk_tracking| !chunk_tracking.is_empty());

    let timestamp = {
        payload
            .chunk
            .time_stamp
            .clone()
            .map(|ts| -> Result<NaiveDateTime, ServiceError> {
                Ok(ts
                    .parse::<DateTimeUtc>()
                    .map_err(|_| ServiceError::BadRequest("Invalid timestamp format".to_string()))?
                    .0
                    .with_timezone(&chrono::Local)
                    .naive_local())
            })
            .transpose()?
    };

    let chunk_metadata = ChunkMetadata {
        id: payload.ingest_specific_chunk_metadata.id,
        link: payload.chunk.link.clone(),
        qdrant_point_id,
        created_at: chrono::Utc::now().naive_local(),
        updated_at: chrono::Utc::now().naive_local(),
        chunk_html: payload.chunk.chunk_html.clone(),
        metadata: payload.chunk.metadata.clone(),
        tracking_id: chunk_tracking_id,
        time_stamp: timestamp,
        location: payload.chunk.location,
        dataset_id: payload.ingest_specific_chunk_metadata.dataset_id,
        weight: payload.chunk.weight.unwrap_or(0.0),
        image_urls: payload
            .chunk
            .image_urls
            .map(|urls| urls.into_iter().map(Some).collect()),
        tag_set: chunk_tag_set,
        num_value: payload.chunk.num_value,
    };

    if content.is_empty() {
        return Err(ServiceError::BadRequest(
            "Chunk must not have empty chunk_html".into(),
        ));
    }

    let embedding_vector = match dataset_config.SEMANTIC_ENABLED {
        true => {
            let embedding = match payload.chunk.split_avg.unwrap_or(false) {
                true => {
                    let chunks = coarse_doc_chunker(semantic_content.clone(), None, false, 20);

                    let embeddings = get_dense_vectors(
                        chunks
                            .iter()
                            .map(|chunk| (chunk.clone(), payload.chunk.semantic_boost.clone()))
                            .collect(),
                        "doc",
                        dataset_config.clone(),
                        reqwest_client.clone(),
                    )
                    .await?;

                    average_embeddings(embeddings)?
                }
                false => {
                    let embedding_vectors = get_dense_vectors(
                        vec![(
                            semantic_content.clone(),
                            payload.chunk.semantic_boost.clone(),
                        )],
                        "doc",
                        dataset_config.clone(),
                        reqwest_client.clone(),
                    )
                    .await
                    .map_err(|err| {
                        ServiceError::InternalServerError(format!(
                            "Failed to create embedding: {:?}",
                            err
                        ))
                    })?;

                    embedding_vectors
                        .first()
                        .ok_or(ServiceError::InternalServerError(
                            "Failed to get first embedding".into(),
                        ))?
                        .clone()
                }
            };
            Some(embedding)
        }
        false => None,
    };

    let splade_vector = if dataset_config.FULLTEXT_ENABLED {
        let content_and_boosts: Vec<(String, Option<FullTextBoost>)> = content_and_boosts
            .clone()
            .into_iter()
            .map(|(content, boost)| {
                let boost = if boost.is_some() && boost.as_ref().unwrap().phrase.is_empty() {
                    None
                } else {
                    boost
                };

                (content, boost)
            })
            .collect();

        match get_sparse_vectors(content_and_boosts.clone(), "doc", reqwest_client).await {
            Ok(vectors) => Ok(vectors.first().expect("First vector must exist").clone()),
            Err(err) => Err(err),
        }
    } else {
        Ok(vec![(0, 0.0)])
    }?;

    let bm25_vector = if dataset_config.BM25_ENABLED
        && std::env::var("BM25_ACTIVE").unwrap_or("false".to_string()) == "true"
    {
        Some(
            get_bm25_embeddings(
                content_and_boosts,
                dataset_config.BM25_AVG_LEN,
                dataset_config.BM25_B,
                dataset_config.BM25_K,
            )
            .first()
            .expect("Vector Must exist")
            .clone(),
        )
    } else {
        None
    };

    //if collision is not nil, insert chunk with collision
    let chunk_metadata_id = {
        let original_id = payload.ingest_specific_chunk_metadata.id;
        let mut inserted_chunk_id = original_id;
        payload.ingest_specific_chunk_metadata.qdrant_point_id = qdrant_point_id;

        let group_tag_set = if qdrant_only {
            None
        } else {
            let inserted_chunk = insert_chunk_metadata_query(
                chunk_metadata.clone(),
                payload.chunk.group_ids.clone(),
                payload.dataset_id,
                payload.upsert_by_tracking_id,
                web_pool.clone(),
            )
            .await?;
            inserted_chunk_id = inserted_chunk.id;

            if payload.chunk.fulltext_boost.is_some() || payload.chunk.semantic_boost.is_some() {
                insert_chunk_boost(
                    ChunkBoost {
                        chunk_id: inserted_chunk.id,
                        fulltext_boost_phrase: payload
                            .chunk
                            .fulltext_boost
                            .clone()
                            .map(|x| x.phrase),
                        fulltext_boost_factor: payload.chunk.fulltext_boost.map(|x| x.boost_factor),
                        semantic_boost_phrase: payload
                            .chunk
                            .semantic_boost
                            .clone()
                            .map(|x| x.phrase),
                        semantic_boost_factor: payload
                            .chunk
                            .semantic_boost
                            .map(|x| x.distance_factor as f64),
                    },
                    web_pool.clone(),
                )
                .await?;
            }

            qdrant_point_id = inserted_chunk.qdrant_point_id;

            if let Some(ref group_ids) = payload.chunk.group_ids {
                Some(
                    get_groups_from_group_ids_query(group_ids.clone(), web_pool.clone())
                        .await?
                        .iter()
                        .filter_map(|group| group.tag_set.clone())
                        .flatten()
                        .dedup()
                        .collect(),
                )
            } else {
                None
            }
        };

        let qdrant_payload =
            QdrantPayload::new(chunk_metadata, payload.chunk.group_ids, None, group_tag_set);

        let vector_name = match &embedding_vector {
            Some(embedding_vector) => match embedding_vector.len() {
                384 => Some("384_vectors"),
                512 => Some("512_vectors"),
                768 => Some("768_vectors"),
                1024 => Some("1024_vectors"),
                3072 => Some("3072_vectors"),
                1536 => Some("1536_vectors"),
                _ => {
                    return Err(ServiceError::BadRequest(
                        "Invalid embedding vector size".into(),
                    ))
                }
            },
            None => None,
        };

        let mut vector_payload =
            HashMap::from([("sparse_vectors".to_string(), Vector::from(splade_vector))]);

        if embedding_vector.is_some() && vector_name.is_some() {
            vector_payload.insert(
                vector_name.unwrap().to_string(),
                Vector::from(embedding_vector.unwrap()),
            );
        }

        if let Some(bm25_vector) = bm25_vector.clone() {
            vector_payload.insert(
                "bm25_vectors".to_string(),
                Vector::from(bm25_vector.clone()),
            );
        }

        let point = PointStruct::new(
            qdrant_point_id.clone().to_string(),
            vector_payload,
            qdrant_payload,
        );

        let upsert_qdrant_point_result =
            bulk_upsert_qdrant_points_query(vec![point], dataset_config).await;

        if let Err(e) = upsert_qdrant_point_result {
            log::error!("Failed to create qdrant point: {:?}", e);

            if !qdrant_only && (payload.upsert_by_tracking_id || original_id == inserted_chunk_id) {
                bulk_revert_insert_chunk_metadata_query(vec![inserted_chunk_id], web_pool.clone())
                    .await?;
            }

            return Err(e);
        };
        if qdrant_only {
            update_dataset_chunk_count(dataset_id, 1_i32, web_pool.clone()).await?;
        }

        inserted_chunk_id
    };

    Ok(chunk_metadata_id)
}

#[tracing::instrument(skip(web_pool))]
async fn update_chunk(
    payload: UpdateIngestionMessage,
    web_pool: actix_web::web::Data<models::Pool>,
    dataset_config: DatasetConfiguration,
) -> Result<(), ServiceError> {
    let content = match payload.convert_html_to_text.unwrap_or(true) {
        true => convert_html_to_text(
            &(payload
                .chunk_metadata
                .chunk_html
                .clone()
                .unwrap_or_default()),
        ),
        false => payload
            .chunk_metadata
            .chunk_html
            .clone()
            .unwrap_or_default(),
    };

    if content.is_empty() {
        return Err(ServiceError::BadRequest(
            "Chunk must not have empty chunk_html".into(),
        ));
    }

    let chunk_metadata = payload.chunk_metadata.clone();

    let embedding_vector = match dataset_config.SEMANTIC_ENABLED {
        true => {
            let embedding = get_dense_vector(
                content.to_string(),
                payload.semantic_boost.clone(),
                "doc",
                dataset_config.clone(),
            )
            .await
            .map_err(|err| ServiceError::BadRequest(err.to_string()))?;
            Some(embedding)
        }
        false => None,
    };

    let splade_vector = if dataset_config.FULLTEXT_ENABLED {
        let reqwest_client = reqwest::Client::new();

        match get_sparse_vectors(
            vec![(content.clone(), payload.fulltext_boost.clone())],
            "doc",
            reqwest_client,
        )
        .await
        {
            Ok(v) => v.first().unwrap_or(&vec![(0, 0.0)]).clone(),
            Err(_) => vec![(0, 0.0)],
        }
    } else {
        vec![(0, 0.0)]
    };

    let bm25_vector = if dataset_config.BM25_ENABLED
        && std::env::var("BM25_ACTIVE").unwrap_or("false".to_string()) == "true"
    {
        let vecs = get_bm25_embeddings(
            vec![(content, payload.fulltext_boost.clone())],
            dataset_config.BM25_AVG_LEN,
            dataset_config.BM25_B,
            dataset_config.BM25_K,
        );

        vecs.first().cloned()
    } else {
        None
    };

    if let Some(group_ids) = payload.group_ids {
        let mut chunk_group_ids: Vec<uuid::Uuid> = vec![];
        for group_id in group_ids {
            let group = dataset_owns_group(group_id, payload.dataset_id, web_pool.clone())
                .await
                .map_err(|err| ServiceError::BadRequest(err.to_string()))?;
            chunk_group_ids.push(group.id);
        }

        update_chunk_metadata_query(
            chunk_metadata.clone().into(),
            Some(chunk_group_ids.clone()),
            payload.dataset_id,
            web_pool.clone(),
        )
        .await?;

        update_qdrant_point_query(
            // If the chunk is a collision, we don't want to update the qdrant point
            chunk_metadata.into(),
            embedding_vector,
            Some(chunk_group_ids),
            payload.dataset_id,
            splade_vector,
            bm25_vector,
            dataset_config,
            web_pool.clone(),
        )
        .await
        .map_err(|err| ServiceError::BadRequest(err.to_string()))?;
    } else {
        update_chunk_metadata_query(
            chunk_metadata.clone().into(),
            None,
            payload.dataset_id,
            web_pool.clone(),
        )
        .await?;

        update_qdrant_point_query(
            // If the chunk is a collision, we don't want to update the qdrant point
            chunk_metadata.into(),
            embedding_vector,
            None,
            payload.dataset_id,
            splade_vector,
            bm25_vector,
            dataset_config,
            web_pool.clone(),
        )
        .await
        .map_err(|err| ServiceError::BadRequest(err.to_string()))?;
    }

    // If boosts are changed, reflect changes to chunk_boosts table
    if payload.fulltext_boost.is_some() || payload.semantic_boost.is_some() {
        update_chunk_boost_query(
            ChunkBoost {
                chunk_id: payload.chunk_metadata.id,
                fulltext_boost_phrase: payload.fulltext_boost.clone().map(|x| x.phrase),
                fulltext_boost_factor: payload.fulltext_boost.map(|x| x.boost_factor),
                semantic_boost_phrase: payload.semantic_boost.clone().map(|x| x.phrase),
                semantic_boost_factor: payload.semantic_boost.map(|x| x.distance_factor as f64),
            },
            web_pool,
        )
        .await?;
    }

    Ok(())
}

#[tracing::instrument(skip(redis_pool, event_queue, error, message))]
pub async fn readd_error_to_queue(
    message: IngestionMessage,
    error: ServiceError,
    redis_pool: actix_web::web::Data<models::RedisPool>,
    event_queue: actix_web::web::Data<EventQueue>,
) -> Result<(), ServiceError> {
    if let ServiceError::DuplicateTrackingId(_) = error {
        log::info!("Duplicate");
        return Ok(());
    }

    if let IngestionMessage::BulkUpload(mut payload) = message {
        let old_payload_message = serde_json::to_string(&payload).map_err(|_| {
            ServiceError::InternalServerError("Failed to reserialize input for retry".to_string())
        })?;

        let mut redis_conn = redis_pool
            .get()
            .await
            .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

        let _ = redis::cmd("LREM")
            .arg("processing")
            .arg(1)
            .arg(old_payload_message.clone())
            .query_async::<redis::aio::MultiplexedConnection, usize>(&mut *redis_conn)
            .await;

        payload.attempt_number += 1;

        if payload.attempt_number == 10 {
            log::error!("Failed to insert data 10 times quitting {:?}", error);
            let count = payload.ingestion_messages.len();
            let chunk_ids = payload
                .ingestion_messages
                .iter()
                .map(|m| m.ingest_specific_chunk_metadata.id)
                .collect();

            event_queue
                .send(ClickHouseEvent::WorkerEvent(
                    WorkerEvent::from_details(
                        payload.dataset_id,
                        models::EventType::BulkChunkUploadFailed {
                            chunk_ids,
                            error: format!("Failed to upload {:} chunks: {:?}", count, error),
                        },
                    )
                    .into(),
                ))
                .await;

            let mut redis_conn = redis_pool
                .get()
                .await
                .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

            redis::cmd("lpush")
                .arg("dead_letters")
                .arg(old_payload_message)
                .query_async::<redis::aio::MultiplexedConnection, ()>(&mut *redis_conn)
                .await
                .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

            return Err(ServiceError::InternalServerError(format!(
                "Failed to create new qdrant point: {:?}",
                error
            )));
        }

        let new_payload_message = serde_json::to_string(&payload).map_err(|_| {
            ServiceError::InternalServerError("Failed to reserialize input for retry".to_string())
        })?;

        let mut redis_conn = redis_pool
            .get()
            .await
            .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

        log::error!(
            "Failed to insert data, re-adding {:?} retry: {:?}",
            error,
            payload.attempt_number
        );

        redis::cmd("lpush")
            .arg("ingestion")
            .arg(&new_payload_message)
            .query_async(&mut *redis_conn)
            .await
            .map_err(|err| ServiceError::BadRequest(err.to_string()))?
    }

    Ok(())
}
