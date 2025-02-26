use std::collections::{HashMap, HashSet};

use broccoli_queue::queue::BroccoliQueue;
use diesel::prelude::*;
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use diesel_async::RunQueryDsl;
use itertools::Itertools;
use qdrant_client::qdrant::point_id::PointIdOptions;
use qdrant_client::qdrant::{GetPoints, PointId};
use trieve_server::data::models::{
    ChunkMetadataTypes, Dataset, DatasetConfiguration, IngestSpecificChunkMetadata,
};
use trieve_server::handlers::chunk_handler::{
    BulkUploadIngestionMessage, ChunkReqPayload, UploadIngestionMessage,
};
use trieve_server::operators::chunk_operator::{
    create_chunk_metadata, get_chunk_metadatas_from_point_ids,
};
use trieve_server::operators::qdrant_operator::{
    get_qdrant_collection_from_dataset_config, get_qdrant_connection,
};
use trieve_server::{errors::ServiceError, establish_connection, get_env};

struct CollectionToPointids {
    qdrant_collection_name: String,
    point_ids: HashSet<String>,
}

#[allow(clippy::print_stdout)]
#[tokio::main]
async fn main() -> Result<(), ServiceError> {
    println!("starting");
    dotenvy::dotenv().ok();
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

    let postgres_fetch_points_count: i64 = std::env::var("POSTGRES_FETCH_POINTS_COUNT")
        .unwrap_or("100".to_string())
        .parse::<i64>()
        .unwrap_or(100);

    let web_pool = actix_web::web::Data::new(pool.clone());

    let redis_url = get_env!("REDIS_URL", "REDIS_URL should be set");
    let queue_name = std::env::var("INGESTION_QUEUE_NAME").unwrap_or("sync_ingestion".to_string());

    let broccoli_queue = BroccoliQueue::builder(redis_url)
        .pool_connections(5.try_into().unwrap())
        .build()
        .await
        .expect("Failed to build broccoli queue");

    let start_offset = std::env::var("START_OFFSET")
        .map(|offset_str| uuid::Uuid::parse_str(&offset_str).ok().unwrap())
        .unwrap_or(uuid::Uuid::nil());

    let stop_offset = std::env::var("END_OFFSET")
        .map(|offset_str| uuid::Uuid::parse_str(&offset_str).ok().unwrap())
        .unwrap_or(uuid::Uuid::max());

    let mut offset = Some(start_offset);

    let dataset_id = std::env::var("dataset_id")
        .map(|dataset_str| uuid::Uuid::parse_str(&dataset_str).ok().unwrap())
        .unwrap_or(uuid::Uuid::max());

    while let Some(cur_offset) = offset {
        use trieve_server::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;
        use trieve_server::data::schema::chunk_metadata::dsl as chunk_metadata_columns;
        use trieve_server::data::schema::datasets::dsl as datasets_columns;

        let mut conn = pool.get().await.map_err(|_| {
            ServiceError::BadRequest("Could not get database connection".to_string())
        })?;

        println!("Fetching from postgres @ {}", cur_offset);

        // Get postgres_fetch_points_count chunk_metadata
        let qdrant_dataset_id_pairs = chunk_metadata_columns::chunk_metadata
            .select((
                chunk_metadata_columns::id,
                chunk_metadata_columns::qdrant_point_id,
                chunk_metadata_columns::dataset_id,
            ))
            .filter(chunk_metadata_columns::dataset_id.eq(dataset_id))
            .filter(chunk_metadata_columns::id.ge(cur_offset))
            .order_by(chunk_metadata_columns::id)
            .limit(postgres_fetch_points_count)
            .load::<(uuid::Uuid, uuid::Uuid, uuid::Uuid)>(&mut conn)
            .await
            .expect("Failed to query chunks");

        println!("Got {} ids", qdrant_dataset_id_pairs.len());

        let chunk_ids = qdrant_dataset_id_pairs
            .iter()
            .map(|(chunk_id, _, _)| chunk_id)
            .collect_vec();

        offset = chunk_ids.iter().max().copied().copied();
        if qdrant_dataset_id_pairs.len() < (postgres_fetch_points_count as usize) {
            println!("setting offset to None");
            offset = None;
        }

        if let Some(new_offset) = offset {
            if new_offset > stop_offset {
                offset = None;
            }
        }

        let groups_ids_to_chunk_ids = chunk_group_bookmarks_columns::chunk_group_bookmarks
            .select((
                chunk_group_bookmarks_columns::group_id,
                chunk_group_bookmarks_columns::chunk_metadata_id,
            ))
            .filter(chunk_group_bookmarks_columns::chunk_metadata_id.eq_any(chunk_ids))
            .load::<(uuid::Uuid, uuid::Uuid)>(&mut conn)
            .await
            .expect("Failed to query chunk groups");
        // for each find qdrant collection name via dataset id

        let dataset_id_config_pair = datasets_columns::datasets
            .filter(
                datasets_columns::id.eq_any(
                    &qdrant_dataset_id_pairs
                        .iter()
                        .map(|(_, _, dataset_id)| dataset_id)
                        .unique()
                        .collect_vec(),
                ),
            )
            .load::<Dataset>(&mut conn)
            .await
            .expect("Failed to load dataset settings");

        // Modified version that combines by collection name
        let collected_qdrant_ids: Vec<CollectionToPointids> = qdrant_dataset_id_pairs
            .into_iter()
            .filter_map(|(_, qdrant_point_id, dataset_id)| {
                dataset_id_config_pair
                    .iter()
                    .find(|dataset| dataset.id == dataset_id)
                    .map(|dataset| {
                        let dataset_config =
                            DatasetConfiguration::from_json(dataset.server_configuration.clone());
                        (
                            get_qdrant_collection_from_dataset_config(&dataset_config),
                            qdrant_point_id.to_string(),
                        )
                    })
            })
            .fold(
                HashMap::new(),
                |mut acc: HashMap<String, HashSet<String>>, (collection_name, point_id)| {
                    acc.entry(collection_name).or_default().insert(point_id);
                    acc
                },
            )
            .into_iter()
            .map(|(qdrant_collection_name, point_ids)| CollectionToPointids {
                point_ids,
                qdrant_collection_name,
            })
            .collect();

        // For each chunk, see if it exists in qdrant. use this id to check against prod
        for collection_pairs in collected_qdrant_ids {
            let qdrant_client = get_qdrant_connection(None, None)
                .await
                .expect("Failed to get qdrant connection");

            println!(
                "Checking if {} ids exist in {} collection",
                collection_pairs.point_ids.len(),
                collection_pairs.qdrant_collection_name,
            );

            let qdrant_point_ids_response = qdrant_client
                .get_points(GetPoints {
                    collection_name: collection_pairs.qdrant_collection_name,
                    ids: collection_pairs
                        .point_ids
                        .iter()
                        .map(|point_uuid| PointId::from(point_uuid.as_str()))
                        .collect(),
                    with_payload: Some(false.into()),
                    with_vectors: Some(false.into()),
                    read_consistency: None,
                    shard_key_selector: None,
                    timeout: None,
                })
                .await;

            match qdrant_point_ids_response {
                Ok(resp) => {
                    let qdrant_points_existing = resp
                        .result
                        .iter()
                        .filter_map(|point| {
                            point.id.as_ref().map(|point_id| {
                                match point_id.point_id_options.clone() {
                                    Some(PointIdOptions::Uuid(qdrant_uuid)) => qdrant_uuid,
                                    _ => unreachable!(),
                                }
                            })
                        })
                        .collect::<HashSet<String>>();

                    // Collect each missing point into list
                    let missing_points: Vec<uuid::Uuid> = collection_pairs
                        .point_ids
                        .difference(&qdrant_points_existing)
                        .filter_map(|string_id| uuid::Uuid::parse_str(string_id).ok())
                        .collect_vec();

                    // Reingest each set of missing points.

                    println!("{} Points are missing, reingesting", missing_points.len());

                    let chunk_metadatas =
                        get_chunk_metadatas_from_point_ids(missing_points, web_pool.clone())
                            .await?;

                    let chunk_messages: Vec<BulkUploadIngestionMessage> = chunk_metadatas
                        .iter()
                        .filter_map(|chunk| {
                            if let ChunkMetadataTypes::Metadata(chunk) = chunk {
                                let group_ids: Vec<uuid::Uuid> = groups_ids_to_chunk_ids
                                    .iter()
                                    .filter(|(_, chunk_id)| *chunk_id == chunk.id)
                                    .map(|(group_id, _)| group_id)
                                    .cloned()
                                    .collect();

                                let upload_message = ChunkReqPayload {
                                    chunk_html: chunk.chunk_html.clone(),
                                    semantic_content: None,
                                    link: chunk.link.clone(),
                                    tag_set: chunk.tag_set.clone().map(|tag_set| {
                                        tag_set.split(',').map(|tag| tag.to_string()).collect()
                                    }),
                                    num_value: chunk.num_value,
                                    metadata: chunk.metadata.clone(),
                                    tracking_id: chunk.tracking_id.clone(),
                                    upsert_by_tracking_id: Some(false),
                                    group_ids: Some(group_ids),
                                    group_tracking_ids: None,
                                    time_stamp: chunk
                                        .time_stamp
                                        .map(|timestamp| timestamp.clone().to_string()),
                                    location: chunk.location,
                                    image_urls: chunk.image_urls.clone().map(|image_urls| {
                                        image_urls
                                            .iter()
                                            .filter_map(|image| image.clone())
                                            .collect()
                                    }),
                                    weight: Some(chunk.weight),
                                    split_avg: None,
                                    convert_html_to_text: None,
                                    fulltext_boost: None,
                                    semantic_boost: None,
                                    high_priority: None,
                                };
                                let (mut message, _) =
                                    create_chunk_metadata(vec![upload_message], chunk.dataset_id)
                                        .expect("timestamp is valid");

                                let modified_messages = message
                                    .ingestion_messages
                                    .clone()
                                    .into_iter()
                                    .map(|message| UploadIngestionMessage {
                                        ingest_specific_chunk_metadata:
                                            IngestSpecificChunkMetadata {
                                                id: chunk.id,
                                                dataset_id: chunk.dataset_id,
                                                qdrant_point_id: chunk.qdrant_point_id,
                                            },
                                        ..message
                                    })
                                    .collect();

                                message.only_qdrant = Some(true);
                                message.ingestion_messages = modified_messages;

                                Some(message)
                            } else {
                                None
                            }
                        })
                        .collect();

                    for message in chunk_messages {
                        broccoli_queue
                            .publish(
                                &queue_name,
                                Some(message.dataset_id.to_string()),
                                &message,
                                None,
                            )
                            .await
                            .map_err(|err| ServiceError::BadRequest(err.to_string()))?;
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    Ok(())
}
