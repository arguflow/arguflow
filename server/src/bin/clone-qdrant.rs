use qdrant_client::{
    qdrant::{PointStruct, UpsertPointsBuilder},
    Qdrant,
};
use trieve_server::{
    errors::ServiceError, operators::qdrant_operator::scroll_qdrant_collection_ids_custom_url,
};
#[allow(clippy::print_stdout)]
#[tokio::main]
async fn main() -> Result<(), ServiceError> {
    dotenvy::dotenv().ok();

    env_logger::builder()
        .target(env_logger::Target::Stdout)
        .filter_level(log::LevelFilter::Info)
        .init();

    let origin_qdrant_url =
        std::env::var("ORIGIN_QDRANT_URL").expect("ORIGIN_QDRANT_URL is not set");
    let new_qdrant_url = std::env::var("NEW_QDRANT_URL").expect("NEW_QDRANT_URL is not set");
    let qdrant_api_key = std::env::var("QDRANT_API_KEY").expect("QDRANT_API_KEY is not set");
    let collection_to_clone =
        std::env::var("COLLECTION_TO_CLONE").expect("COLLECTION_TO_CLONE is not set");
    let timeout = std::env::var("QDRANT_TIMEOUT_SEC")
        .unwrap_or("60".to_string())
        .parse::<u64>()
        .unwrap_or(60);

    let sleep_wait_ms = std::env::var("TIMEOUT_MS")
        .unwrap_or("200".to_string())
        .parse::<u64>()
        .unwrap_or(200);

    let qdrant_batch_size = std::env::var("QDRANT_BATCH_SIZE")
        .unwrap_or("500".to_string())
        .parse::<u32>()
        .unwrap_or(500);

    let mut offset = Some(std::env::var("OFFSET_ID").unwrap_or(uuid::Uuid::nil().to_string()));

    while let Some(cur_offset) = offset {
        let original_qdrant_connection = Qdrant::from_url(&origin_qdrant_url)
            .api_key(qdrant_api_key.clone())
            .timeout(std::time::Duration::from_secs(timeout))
            .build()
            .map_err(|err| {
                ServiceError::BadRequest(format!("Failed to connect to new Qdrant {:?}", err))
            })?;
        let new_qdrant_connection = Qdrant::from_url(&new_qdrant_url)
            .api_key(qdrant_api_key.clone())
            .timeout(std::time::Duration::from_secs(timeout))
            .build()
            .map_err(|err| {
                ServiceError::BadRequest(format!("Failed to connect to new Qdrant {:?}", err))
            })?;

        log::info!(
            "Fetching points from original collection starting at: {}",
            cur_offset
        );

        let (origin_qdrant_points, new_offset) = scroll_qdrant_collection_ids_custom_url(
            collection_to_clone.clone(),
            Some(cur_offset.to_string()),
            Some(qdrant_batch_size),
            original_qdrant_connection,
        )
        .await
        .map_err(|err| {
            log::error!("Failed fetching points from qdrant {:?}", err);
            ServiceError::BadRequest(format!("Failed fetching points from qdrant {:?}", err))
        })?;

        let point_structs_to_upsert = origin_qdrant_points
            .iter()
            .filter_map(|retrieved_point| {
                let id = retrieved_point.id.clone();
                let payload = retrieved_point.payload.clone();
                let vectors = retrieved_point.vectors.clone();
                if let (Some(id), payload, Some(vectors)) = (id, payload, vectors) {
                    Some(PointStruct {
                        id: Some(id),
                        payload,
                        vectors: Some(vectors),
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<PointStruct>>();

        log::info!(
            "Upserting {} points into new Qdrant collection starting at: {}",
            point_structs_to_upsert.len(),
            cur_offset
        );

        new_qdrant_connection
            .upsert_points(UpsertPointsBuilder::new(
                collection_to_clone.clone(),
                point_structs_to_upsert,
            ))
            .await
            .map_err(|err| {
                log::error!("Failed inserting chunks to qdrant {:?}", err);
                ServiceError::BadRequest(format!("Failed inserting chunks to qdrant {:?}", err))
            })?;

        log::info!(
            "Setting offset to: {}",
            new_offset.clone().unwrap_or_default()
        );

        tokio::time::sleep(std::time::Duration::from_millis(sleep_wait_ms)).await;

        offset = new_offset;
    }

    Ok(())
}
