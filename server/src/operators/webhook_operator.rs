use actix_web::web;
use broccoli_queue::queue::BroccoliQueue;

use crate::{
    data::models::{DatasetConfiguration, Pool},
    errors::ServiceError,
    handlers::chunk_handler::ChunkReqPayload,
    operators::{chunk_operator::create_chunk_metadata, dataset_operator::get_dataset_by_id_query},
};

use super::chunk_operator::{delete_chunk_metadata_query, get_metadata_from_tracking_id_query};

pub async fn delete_content<T: Into<ChunkReqPayload>>(
    dataset_id: uuid::Uuid,
    value: T,
    pool: web::Data<Pool>,
) -> Result<(), ServiceError> {
    let chunk: ChunkReqPayload = value.into();
    let tracking_id_inner = chunk.tracking_id.ok_or(ServiceError::BadRequest(
        "Must provide a tracking_id to delete a chunk".to_string(),
    ))?;

    let full_dataset = get_dataset_by_id_query(dataset_id, pool.clone()).await?;

    let dataset_config = DatasetConfiguration::from_json(full_dataset.server_configuration.clone());

    let chunk_metadata =
        get_metadata_from_tracking_id_query(tracking_id_inner, dataset_id, pool.clone()).await?;

    let deleted_at = chrono::Utc::now().naive_utc();

    delete_chunk_metadata_query(
        vec![chunk_metadata.id],
        deleted_at,
        full_dataset,
        pool,
        dataset_config,
    )
    .await?;

    Ok(())
}

pub async fn publish_content<T: Into<ChunkReqPayload>>(
    dataset_id: uuid::Uuid,
    value: T,
    broccoli_queue: web::Data<BroccoliQueue>,
) -> Result<(), ServiceError> {
    let chunk: ChunkReqPayload = value.into();

    let (upsert_message, _) = create_chunk_metadata(vec![chunk.clone()], dataset_id)?;

    broccoli_queue
        .publish(
            "ingestion",
            Some(dataset_id.to_string()),
            &upsert_message,
            None,
        )
        .await
        .map_err(|e| {
            log::error!("Could not publish message {:?}", e);
            ServiceError::InternalServerError(e.to_string())
        })?;

    Ok(())
}
