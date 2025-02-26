use broccoli_queue::brokers::broker::BrokerMessage;
use broccoli_queue::error::BroccoliError;
use broccoli_queue::queue::{BroccoliQueue, ConsumeOptionsBuilder};
use chrono::NaiveDateTime;
use dateparser::DateTimeUtc;
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use futures_util::StreamExt;
use itertools::{izip, Itertools};
use qdrant_client::qdrant::{PointStruct, Vector};
use signal_hook::consts::SIGTERM;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{atomic::AtomicBool, Arc};
use trieve_server::data::models::{
    self, ChunkBoost, ChunkData, ChunkGroup, ChunkMetadata, DatasetConfiguration,
    PagefindIndexWorkerMessage, QdrantPayload, WorkerEvent,
};
use trieve_server::errors::ServiceError;
use trieve_server::handlers::chunk_handler::{
    BulkUploadIngestionMessage, FullTextBoost, SemanticBoost, UploadIngestionMessage,
};
use trieve_server::operators::chunk_operator::{
    bulk_insert_chunk_metadata_query, bulk_revert_insert_chunk_metadata_query,
    get_row_count_for_organization_id_query, insert_chunk_boost, insert_chunk_metadata_query,
    update_dataset_chunk_count,
};
use trieve_server::operators::clickhouse_operator::{ClickHouseEvent, EventQueue};
use trieve_server::operators::dataset_operator::{
    get_dataset_and_organization_from_dataset_id_query, get_dataset_by_id_query,
};
use trieve_server::operators::group_operator::{
    create_groups_query, get_group_ids_from_tracking_ids_query, get_groups_from_group_ids_query,
};
use trieve_server::operators::model_operator::{
    get_bm25_embeddings, get_dense_vectors, get_sparse_vectors,
};
use trieve_server::operators::parse_operator::{
    average_embeddings, coarse_doc_chunker, convert_html_to_text,
};
use trieve_server::operators::qdrant_operator::bulk_upsert_qdrant_points_query;
use trieve_server::{establish_connection, get_env};

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

    let redis_url = get_env!("REDIS_URL", "REDIS_URL is not set");
    let redis_connections: u32 = std::env::var("REDIS_CONNECTIONS")
        .unwrap_or("2".to_string())
        .parse()
        .unwrap_or(2);

    let redis_manager =
        bb8_redis::RedisConnectionManager::new(redis_url).expect("Failed to connect to redis");

    let redis_pool = bb8_redis::bb8::Pool::builder()
        .max_size(redis_connections)
        .connection_timeout(std::time::Duration::from_secs(2))
        .build(redis_manager)
        .await
        .expect("Failed to create redis pool");

    let queue = BroccoliQueue::builder(redis_url)
        .pool_connections(redis_connections.try_into().unwrap())
        .failed_message_retry_strategy(Default::default())
        .build()
        .await?;

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

    let web_event_queue = actix_web::web::Data::new(event_queue);
    let queue_name = std::env::var("INGESTION_QUEUE_NAME").unwrap_or("ingestion".to_string());

    let should_terminate = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(SIGTERM, Arc::clone(&should_terminate))
        .expect("Failed to register shutdown hook");

    let ingestion_web_pool = web_pool.clone();

    log::info!("Starting ingestion service thread");

    queue
        .process_messages_with_handlers(
            &queue_name,
            None,
            Some(ConsumeOptionsBuilder::new().fairness(true).build()),
            move |msg| {
                let pool = ingestion_web_pool.clone();
                async move { ingestion_worker(msg.payload, pool.clone()).await }
            },
            {
                let web_pool = web_pool.clone();
                let redis_pool = redis_pool.clone();
                move |msg: BrokerMessage<BulkUploadIngestionMessage>| {
                    let web_pool = web_pool.clone();
                    let redis_pool = redis_pool.clone();
                    let event_queue = web_event_queue.clone();

                    async move {
                        let chunk_ids = msg
                            .payload
                            .ingestion_messages
                            .iter()
                            .map(|message| message.ingest_specific_chunk_metadata.id)
                            .collect::<Vec<uuid::Uuid>>();

                        log::info!("Uploaded {:} chunks", chunk_ids.len());
                        let dataset_result: Result<models::Dataset, ServiceError> =
                            get_dataset_by_id_query(msg.payload.dataset_id, web_pool.clone()).await;

                        let dataset = match dataset_result {
                            Ok(dataset) => dataset,
                            Err(err) => {
                                log::error!(
                                    "Failed to get dataset; likely does not exist: {:?}",
                                    err
                                );
                                return Err(BroccoliError::Job(
                                    "Failed to get dataset".to_string(),
                                ));
                            }
                        };
                        let dataset_config =
                            DatasetConfiguration::from_json(dataset.server_configuration);

                        if dataset_config.PAGEFIND_ENABLED {
                            let pagefind_worker_message = PagefindIndexWorkerMessage {
                                dataset_id: msg.payload.dataset_id,
                                created_at: chrono::Utc::now().naive_utc(),
                                attempt_number: 0,
                            };

                            let serialized_message =
                                serde_json::to_string(&pagefind_worker_message).map_err(|_| {
                                    BroccoliError::Job(
                                        "Failed to serialize pagefind message".to_string(),
                                    )
                                })?;

                            let mut redis_conn = redis_pool
                                .get()
                                .await
                                .map_err(|err| BroccoliError::Job(err.to_string()))?;

                            redis::cmd("lpush")
                                .arg("pagefind-index-ingestion")
                                .arg(&serialized_message)
                                .query_async::<_, ()>(&mut *redis_conn)
                                .await
                                .map_err(|err| {
                                    BroccoliError::Job(format!(
                                        "Failed to push pagefind message: {:?}",
                                        err
                                    ))
                                })?;

                            log::info!("Queue'd dataset for pagefind indexing");
                        }

                        event_queue
                            .send(ClickHouseEvent::WorkerEvent(
                                WorkerEvent::from_details(
                                    msg.payload.dataset_id,
                                    models::EventType::ChunksUploaded { chunk_ids },
                                )
                                .into(),
                            ))
                            .await;

                        Ok(())
                    }
                }
            },
            |_msg, err| async move {
                log::error!("Failed to upload chunks: {:?}", err);
                Ok(())
            },
        )
        .await?;

    Ok(())
}

async fn ingestion_worker(
    ingestion_message: BulkUploadIngestionMessage,
    web_pool: actix_web::web::Data<models::Pool>,
) -> Result<(), BroccoliError> {
    log::info!("Selecting dataset for ingestion message");
    let dataset_result: Result<models::Dataset, ServiceError> =
        get_dataset_by_id_query(ingestion_message.dataset_id, web_pool.clone()).await;

    let dataset = match dataset_result {
        Ok(dataset) => dataset,
        Err(err) => {
            log::error!("Failed to get dataset; likely does not exist: {:?}", err);
            return Err(BroccoliError::Job("Failed to get dataset".to_string()));
        }
    };
    let dataset_config = DatasetConfiguration::from_json(dataset.server_configuration);

    log::info!(
        "Starting bulk upload of {} chunks for dataset_id: {:?}",
        ingestion_message.ingestion_messages.len(),
        ingestion_message.dataset_id
    );

    let reqwest_client = reqwest::Client::new();

    bulk_upload_chunks(
        ingestion_message.clone(),
        dataset_config.clone(),
        web_pool.clone(),
        reqwest_client.clone(),
    )
    .await
}

pub async fn bulk_upload_chunks(
    payload: BulkUploadIngestionMessage,
    dataset_config: DatasetConfiguration,
    web_pool: actix_web::web::Data<models::Pool>,
    reqwest_client: reqwest::Client,
) -> Result<(), BroccoliError> {
    let unlimited = std::env::var("UNLIMITED").unwrap_or("false".to_string());
    if unlimited == "false" && !dataset_config.QDRANT_ONLY {
        log::info!("Getting dataset, organization, and its plan+subscription information for dataset_id: {:?}", payload.dataset_id);

        let dataset_org_plan_sub = get_dataset_and_organization_from_dataset_id_query(
            models::UnifiedId::TrieveUuid(payload.dataset_id),
            None,
            web_pool.clone(),
        )
        .await?;

        log::info!(
            "Getting row count for organization_id {:?}",
            dataset_org_plan_sub.organization.organization.id
        );

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
            log::info!("Chunk count exceeds plan limit");
            return Err(BroccoliError::Job(
                "Chunk count exceeds plan limit".to_string(),
            ));
        }
    }

    let mut group_tracking_ids_to_group_ids: HashMap<String, uuid::Uuid> = HashMap::new();

    let all_group_tracking_ids: Vec<String> = payload
        .ingestion_messages
        .iter()
        .flat_map(|chunk| chunk.chunk.group_tracking_ids.clone().unwrap_or_default())
        .unique()
        .collect();

    if !all_group_tracking_ids.is_empty() {
        get_group_ids_from_tracking_ids_query(
            all_group_tracking_ids.clone(),
            payload.dataset_id,
            web_pool.clone(),
        )
        .await?
        .iter()
        .for_each(|(group_id, tracking_id)| {
            if let Some(tracking_id) = tracking_id {
                group_tracking_ids_to_group_ids.insert(tracking_id.clone(), *group_id);
            }
        });

        let new_groups: Vec<ChunkGroup> = all_group_tracking_ids
            .iter()
            .filter_map(|group_tracking_id| {
                if !group_tracking_ids_to_group_ids.contains_key(group_tracking_id) {
                    Some(ChunkGroup::from_details(
                        Some(group_tracking_id.clone()),
                        None,
                        payload.dataset_id,
                        Some(group_tracking_id.to_string()),
                        None,
                        None,
                    ))
                } else {
                    None
                }
            })
            .collect();

        log::info!("Creating {} new groups", new_groups.len());
        let created_groups = create_groups_query(new_groups, true, web_pool.clone()).await?;

        created_groups.iter().for_each(|group| {
            if let Some(tracking_id) = &group.tracking_id {
                group_tracking_ids_to_group_ids.insert(tracking_id.clone(), group.id);
            }
        });
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

    let all_group_ids = group_tracking_ids_to_group_ids
        .values()
        .copied()
        .collect::<Vec<uuid::Uuid>>();
    let all_groups = if all_group_ids.is_empty() {
        vec![]
    } else {
        log::info!("Getting all groups for {:?} group_ids", all_group_ids.len());
        get_groups_from_group_ids_query(all_group_ids, web_pool.clone()).await?
    };

    let ingestion_data: Vec<ChunkData> = payload
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

            let group_ids_from_group_tracking_ids: Vec<uuid::Uuid> =
                if let Some(group_tracking_ids) = message.chunk.group_tracking_ids.clone() {
                    group_tracking_ids
                        .iter()
                        .filter_map(|tracking_id| group_tracking_ids_to_group_ids.get(tracking_id))
                        .copied()
                        .collect()
                } else {
                    vec![]
                };

            let initial_group_ids = message.chunk.group_ids.clone().unwrap_or_default();
            let deduped_group_ids = group_ids_from_group_tracking_ids
                .into_iter()
                .chain(initial_group_ids.into_iter())
                .unique()
                .collect::<Vec<uuid::Uuid>>();

            ChunkData {
                chunk_metadata,
                content: content.clone(),
                embedding_content: message.chunk.semantic_content.clone().unwrap_or(content),
                group_ids: Some(deduped_group_ids),
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
        log::info!(
            "Uploading {} chunks one by one due to split_avg",
            ingestion_data.len()
        );

        let mut chunk_ids = vec![];
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

        return Ok(());
    }

    let qdrant_only = dataset_config.QDRANT_ONLY;

    let inserted_chunk_metadatas = if qdrant_only || payload.only_qdrant.unwrap_or(false) {
        ingestion_data.clone()
    } else {
        log::info!("Inserting {} chunks into database", ingestion_data.len());
        bulk_insert_chunk_metadata_query(
            ingestion_data.clone(),
            payload.dataset_id,
            upsert_by_tracking_id_being_used,
            web_pool.clone(),
        )
        .await?
    };

    if inserted_chunk_metadatas.is_empty() {
        // All collisions
        return Ok(());
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

    let embedding_vectors = match dataset_config.SEMANTIC_ENABLED {
        true => {
            log::info!(
                "Creating embeddings for {} chunks",
                embedding_content_and_boosts.len()
            );
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
                    log::error!("Failed to create embeddings: {:?}", err);
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
        log::info!(
            "Creating sparse vectors for {} chunks",
            content_and_boosts.len()
        );
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
                log::error!("Failed to create sparse vectors: {:?}", err);
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

    let qdrant_points = tokio_stream::iter(izip!(
        inserted_chunk_metadatas.clone(),
        embedding_vectors.iter(),
        splade_vectors.iter(),
        bm25_vectors.iter()
    ))
    .then(
        |(chunk_data, embedding_vector, splade_vector, bm25_vector)| async {
            let mut qdrant_point_id = chunk_data.chunk_metadata.qdrant_point_id;
            if qdrant_only {
                if let Some(tracking_id) = chunk_data.clone().chunk_metadata.tracking_id {
                    qdrant_point_id =
                        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, tracking_id.as_bytes());
                }
            }

            let group_tag_set: Option<Vec<Option<String>>> = if !qdrant_only {
                chunk_data
                    .group_ids
                    .as_ref()
                    .filter(|group_ids| !group_ids.is_empty())
                    .map(|group_ids| {
                        all_groups
                            .iter()
                            .filter_map(|group| {
                                if group_ids.contains(&group.id) {
                                    group.tag_set.clone()
                                } else {
                                    None
                                }
                            })
                            .flatten()
                            .dedup()
                            .collect()
                    })
            } else {
                None
            };

            let payload = QdrantPayload::new(
                chunk_data.chunk_metadata,
                chunk_data.group_ids,
                None,
                group_tag_set,
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

    log::info!("Bulk upserting {} qdrant points", qdrant_points.len());
    let create_point_result: Result<(), BroccoliError> =
        bulk_upsert_qdrant_points_query(qdrant_points, dataset_config.clone())
            .await
            .map_err(|err| {
                log::error!("Failed to create qdrant points: {:?}", err);
                BroccoliError::Job("Failed to create qdrant points".to_string())
            });

    if let Err(err) = create_point_result {
        if !upsert_by_tracking_id_being_used || !qdrant_only {
            bulk_revert_insert_chunk_metadata_query(inserted_chunk_metadata_ids, web_pool.clone())
                .await?;
        }

        return Err(err);
    }

    if qdrant_only {
        log::info!(
            "Updating dataset chunk count by {}",
            inserted_chunk_metadata_ids.len()
        );
        update_dataset_chunk_count(
            payload.dataset_id,
            inserted_chunk_metadata_ids.len() as i32,
            web_pool.clone(),
        )
        .await?;
    }

    Ok(())
}

async fn upload_chunk(
    mut payload: UploadIngestionMessage,
    dataset_config: DatasetConfiguration,
    ingestion_data: ChunkData,
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
        log::error!("Could not upload chunk because it must not have empty content");
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

        if let Some(embedding_vector) = embedding_vector.clone() {
            if let Some(vector_name) = vector_name {
                vector_payload.insert(
                    vector_name.to_string(),
                    Vector::from(embedding_vector.clone()),
                );
            }
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
