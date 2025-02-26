use crate::data::models::{
    uuid_between, ChunkBoost, ChunkBoostChangeset, ChunkData, ChunkGroupBookmark,
    ChunkMetadataTable, ChunkMetadataTags, ChunkMetadataTypes, ContentChunkMetadata, Dataset,
    DatasetConfiguration, DatasetTags, DatasetUsageCount, IngestSpecificChunkMetadata,
    SlimChunkMetadata, SlimChunkMetadataTable, UnifiedId,
};
use crate::handlers::chunk_handler::{BulkUploadIngestionMessage, ChunkReqPayload};
use crate::handlers::chunk_handler::{ChunkFilter, UploadIngestionMessage};
use crate::operators::parse_operator::convert_html_to_text;
use crate::operators::qdrant_operator::{
    delete_points_from_qdrant, get_qdrant_collection_from_dataset_config, scroll_dataset_points,
};
use crate::{
    data::models::{ChunkMetadata, Pool},
    errors::ServiceError,
};
use actix_web::web;
use chrono::NaiveDateTime;
use clickhouse::Row;
use dateparser::DateTimeUtc;
use diesel::dsl::{not, sql};
use diesel::prelude::*;
use diesel::sql_types;
use diesel::upsert::excluded;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, RunQueryDsl};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use simsearch::{SearchOptions, SimSearch};
use std::collections::{HashMap, HashSet};
use time::OffsetDateTime;
use utoipa::ToSchema;

use super::search_operator::assemble_qdrant_filter;

pub async fn get_chunk_metadatas_from_point_ids(
    point_ids: Vec<uuid::Uuid>,
    pool: web::Data<Pool>,
) -> Result<Vec<ChunkMetadataTypes>, ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;
    use crate::data::schema::chunk_metadata_tags::dsl as chunk_metadata_tags_columns;
    use crate::data::schema::dataset_tags::dsl as dataset_tags_columns;

    let mut conn = pool
        .get()
        .await
        .expect("Failed to get connection from pool");

    // Get tag_set
    let chunk_metadata_pairs: Vec<(ChunkMetadataTable, Option<Vec<String>>)> =
        chunk_metadata_columns::chunk_metadata
            .left_join(
                chunk_metadata_tags_columns::chunk_metadata_tags
                    .on(chunk_metadata_tags_columns::chunk_metadata_id
                        .eq(chunk_metadata_columns::id)),
            )
            .left_join(
                dataset_tags_columns::dataset_tags
                    .on(dataset_tags_columns::id.eq(chunk_metadata_tags_columns::tag_id)),
            )
            .filter(chunk_metadata_columns::qdrant_point_id.eq_any(&point_ids))
            .select((
                ChunkMetadataTable::as_select(),
                sql::<sql_types::Array<sql_types::Text>>(
                    "array_remove(array_agg(dataset_tags.tag), null)",
                )
                .nullable(),
            ))
            .group_by(chunk_metadata_columns::id)
            .load::<(ChunkMetadataTable, Option<Vec<String>>)>(&mut conn)
            .await
            .map_err(|_| ServiceError::BadRequest("Failed to load metadata".to_string()))?;

    let chunk_metadatas = chunk_metadata_pairs
        .into_iter()
        .map(|(table, tag_set)| {
            ChunkMetadataTypes::Metadata(
                ChunkMetadata::from_table_and_tag_set(table, tag_set.unwrap_or_default()).into(),
            )
        })
        .collect();

    Ok(chunk_metadatas)
}

pub async fn get_point_ids_from_unified_chunk_ids(
    chunk_ids: Vec<UnifiedId>,
    dataset_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Vec<uuid::Uuid>, ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let qdrant_point_ids: Vec<uuid::Uuid> = match chunk_ids.get(0) {
        Some(UnifiedId::TrieveUuid(_)) => chunk_metadata_columns::chunk_metadata
            .filter(
                chunk_metadata_columns::id.eq_any(
                    &chunk_ids
                        .iter()
                        .map(|x| x.as_uuid().expect("Failed to convert to uuid"))
                        .collect::<Vec<uuid::Uuid>>(),
                ),
            )
            .filter(chunk_metadata_columns::dataset_id.eq(dataset_id))
            .select(chunk_metadata_columns::qdrant_point_id)
            .load::<uuid::Uuid>(&mut conn)
            .await
            .map_err(|_| ServiceError::BadRequest("Failed to load metadata".to_string()))?,
        Some(UnifiedId::TrackingId(_)) => chunk_metadata_columns::chunk_metadata
            .filter(
                chunk_metadata_columns::tracking_id.eq_any(
                    &chunk_ids
                        .iter()
                        .map(|x| x.as_tracking_id().expect("Failed to convert to String"))
                        .collect::<Vec<String>>(),
                ),
            )
            .filter(chunk_metadata_columns::dataset_id.eq(dataset_id))
            .select(chunk_metadata_columns::qdrant_point_id)
            .load::<uuid::Uuid>(&mut conn)
            .await
            .map_err(|_| ServiceError::BadRequest("Failed to load metadata".to_string()))?,
        None => vec![],
    };

    Ok(qdrant_point_ids)
}

pub struct ChunkMetadataWithQdrantId {
    pub metadata: ChunkMetadata,
    pub qdrant_id: uuid::Uuid,
}

pub async fn get_chunk_metadatas_from_point_ids_query(
    point_ids: Vec<uuid::Uuid>,
    pool: web::Data<Pool>,
) -> Result<Vec<ChunkMetadataTypes>, ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;
    use crate::data::schema::chunk_metadata_tags::dsl as chunk_metadata_tags_columns;
    use crate::data::schema::dataset_tags::dsl as dataset_tags_columns;

    let chunk_metadatas = {
        let mut conn = pool.get().await.map_err(|_e| {
            ServiceError::InternalServerError("Failed to get postgres connection".to_string())
        })?;

        let chunk_metadata_pair: Vec<(ChunkMetadataTable, Option<Vec<String>>)> =
            chunk_metadata_columns::chunk_metadata
                .left_join(chunk_metadata_tags_columns::chunk_metadata_tags.on(
                    chunk_metadata_tags_columns::chunk_metadata_id.eq(chunk_metadata_columns::id),
                ))
                .left_join(
                    dataset_tags_columns::dataset_tags
                        .on(dataset_tags_columns::id.eq(chunk_metadata_tags_columns::tag_id)),
                )
                .select((
                    ChunkMetadataTable::as_select(),
                    sql::<sql_types::Array<sql_types::Text>>(
                        "array_remove(array_agg(dataset_tags.tag), null)",
                    )
                    .nullable(),
                ))
                .filter(chunk_metadata_columns::qdrant_point_id.eq_any(&point_ids))
                .group_by(chunk_metadata_columns::id)
                .load::<(ChunkMetadataTable, Option<Vec<String>>)>(&mut conn)
                .await
                .map_err(|err| {
                    ServiceError::BadRequest(format!(
                        "Failed to load tagset and metadata: {:?}",
                        err
                    ))
                })?;

        chunk_metadata_pair
            .into_iter()
            .map(|(chunk_table, tag_set)| {
                ChunkMetadata::from_table_and_tag_set(chunk_table, tag_set.unwrap_or(vec![])).into()
            })
            .collect::<Vec<ChunkMetadataTypes>>()
    };

    Ok(chunk_metadatas)
}

pub async fn get_slim_chunks_from_point_ids_query(
    point_ids: Vec<uuid::Uuid>,
    pool: web::Data<Pool>,
) -> Result<Vec<ChunkMetadataTypes>, ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;
    use crate::data::schema::chunk_metadata_tags::dsl as chunk_metadata_tags_columns;
    use crate::data::schema::dataset_tags::dsl as dataset_tags_columns;

    let mut conn = pool
        .get()
        .await
        .expect("Failed to get connection from pool");

    let slim_chunk_pairs: Vec<(SlimChunkMetadataTable, Option<Vec<String>>)> =
        chunk_metadata_columns::chunk_metadata
            .left_join(
                chunk_metadata_tags_columns::chunk_metadata_tags
                    .on(chunk_metadata_tags_columns::chunk_metadata_id
                        .eq(chunk_metadata_columns::id)),
            )
            .left_join(
                dataset_tags_columns::dataset_tags
                    .on(dataset_tags_columns::id.eq(chunk_metadata_tags_columns::tag_id)),
            )
            .filter(chunk_metadata_columns::qdrant_point_id.eq_any(&point_ids))
            .select((
                (
                    chunk_metadata_columns::id,
                    chunk_metadata_columns::link,
                    chunk_metadata_columns::qdrant_point_id,
                    chunk_metadata_columns::created_at,
                    chunk_metadata_columns::updated_at,
                    chunk_metadata_columns::metadata,
                    chunk_metadata_columns::tracking_id,
                    chunk_metadata_columns::time_stamp,
                    chunk_metadata_columns::location,
                    chunk_metadata_columns::dataset_id,
                    chunk_metadata_columns::weight,
                    chunk_metadata_columns::image_urls,
                    chunk_metadata_columns::num_value,
                ),
                sql::<sql_types::Array<sql_types::Text>>(
                    "array_remove(array_agg(dataset_tags.tag), null)",
                )
                .nullable(),
            ))
            .group_by(chunk_metadata_columns::id)
            .load::<(SlimChunkMetadataTable, Option<Vec<String>>)>(&mut conn)
            .await
            .map_err(|_| {
                ServiceError::BadRequest("Failed to load slim_chunk_metadatas".to_string())
            })?;

    let slim_chunks = slim_chunk_pairs
        .into_iter()
        .map(|(table, tag_set)| {
            SlimChunkMetadata::from_table_and_tag_set(table, tag_set.unwrap_or(vec![])).into()
        })
        .collect();

    Ok(slim_chunks)
}

pub async fn get_content_chunk_from_point_ids_query(
    point_ids: Vec<uuid::Uuid>,
    pool: web::Data<Pool>,
) -> Result<Vec<ChunkMetadataTypes>, ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    let content_chunks = {
        let mut conn = pool.get().await.map_err(|_e| {
            ServiceError::InternalServerError("Failed to get postgres connection".to_string())
        })?;
        let content_chunk_metadatas: Vec<ContentChunkMetadata> =
            chunk_metadata_columns::chunk_metadata
                .select((
                    chunk_metadata_columns::id,
                    chunk_metadata_columns::qdrant_point_id,
                    chunk_metadata_columns::chunk_html,
                    chunk_metadata_columns::tracking_id,
                    chunk_metadata_columns::time_stamp,
                    chunk_metadata_columns::weight,
                    chunk_metadata_columns::image_urls,
                    chunk_metadata_columns::num_value,
                ))
                .filter(chunk_metadata_columns::qdrant_point_id.eq_any(&point_ids))
                .load(&mut conn)
                .await
                .map_err(|_| {
                    ServiceError::BadRequest("Failed to load content chunk metadatas".to_string())
                })?;

        content_chunk_metadatas
            .into_iter()
            .map(|content_chunk| content_chunk.into())
            .collect::<Vec<ChunkMetadataTypes>>()
    };

    Ok(content_chunks)
}

pub async fn get_random_chunk_metadatas_query(
    dataset_id: uuid::Uuid,
    limit: u64,
    pool: web::Data<Pool>,
) -> Result<Vec<ChunkMetadata>, ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    let mut conn = pool
        .get()
        .await
        .expect("Failed to get connection from pool");

    let get_lowest_id_future = chunk_metadata_columns::chunk_metadata
        .filter(chunk_metadata_columns::dataset_id.eq(dataset_id))
        .select(chunk_metadata_columns::id)
        .order_by(chunk_metadata_columns::id.asc())
        .first::<uuid::Uuid>(&mut conn);
    let get_highest_ids_future = chunk_metadata_columns::chunk_metadata
        .filter(chunk_metadata_columns::dataset_id.eq(dataset_id))
        .select(chunk_metadata_columns::id)
        .order_by(chunk_metadata_columns::id.desc())
        .limit(limit.try_into().unwrap_or(10))
        .load::<uuid::Uuid>(&mut conn);
    let (lowest_id, highest_ids) = futures::join!(get_lowest_id_future, get_highest_ids_future);
    let lowest_id: uuid::Uuid = lowest_id.map_err(|err| {
        ServiceError::BadRequest(format!(
            "Failed to load chunk with the lowest id in the dataset for random range: {:?}",
            err
        ))
    })?;
    let highest_ids: Vec<uuid::Uuid> = highest_ids.map_err(|err| {
        ServiceError::BadRequest(format!(
            "Failed to load chunks with the highest id in the dataset for random range: {:?}",
            err
        ))
    })?;
    let highest_id = match highest_ids.get(0) {
        Some(id) => *id,
        None => {
            return Err(ServiceError::NotFound(
                "Chunk with the highest id in the dataset not found for random range".to_string(),
            ))
        }
    };
    let random_uuid = uuid_between(lowest_id, highest_id);

    let chunk_metadatas: Vec<ChunkMetadataTable> = chunk_metadata_columns::chunk_metadata
        .filter(chunk_metadata_columns::dataset_id.eq(dataset_id))
        .filter(chunk_metadata_columns::id.gt(random_uuid))
        .order_by(chunk_metadata_columns::id.asc())
        .limit(limit.try_into().unwrap_or(10))
        .select(ChunkMetadataTable::as_select())
        .load::<ChunkMetadataTable>(&mut conn)
        .await
        .map_err(|_| ServiceError::BadRequest("Failed to load metadata".to_string()))?;

    let chunk_metadatas = chunk_metadatas
        .into_iter()
        .map(|chunk_metadata_table| {
            ChunkMetadata::from_table_and_tag_set(chunk_metadata_table, vec![])
        })
        .collect();

    Ok(chunk_metadatas)
}

pub async fn get_metadata_from_id_query(
    chunk_id: uuid::Uuid,
    dataset_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<ChunkMetadata, ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;
    use crate::data::schema::chunk_metadata_tags::dsl as chunk_metadata_tags_columns;
    use crate::data::schema::dataset_tags::dsl as dataset_tags_columns;
    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let (chunk_table, tag_set) = chunk_metadata_columns::chunk_metadata
        .left_join(
            chunk_metadata_tags_columns::chunk_metadata_tags
                .on(chunk_metadata_tags_columns::chunk_metadata_id.eq(chunk_metadata_columns::id)),
        )
        .left_join(
            dataset_tags_columns::dataset_tags
                .on(dataset_tags_columns::id.eq(chunk_metadata_tags_columns::tag_id)),
        )
        .filter(chunk_metadata_columns::id.eq(chunk_id))
        .filter(chunk_metadata_columns::dataset_id.eq(dataset_id))
        .select((
            ChunkMetadataTable::as_select(),
            sql::<sql_types::Array<sql_types::Text>>(
                "array_remove(array_agg(dataset_tags.tag), null)",
            )
            .nullable(),
        ))
        .group_by(chunk_metadata_columns::id)
        .first::<(ChunkMetadataTable, Option<Vec<String>>)>(&mut conn)
        .await
        .map_err(|_| {
            ServiceError::NotFound("Chunk with id not found in the specified dataset".to_string())
        })?;

    Ok(ChunkMetadata::from_table_and_tag_set(
        chunk_table,
        tag_set.unwrap_or(vec![]),
    ))
}

pub async fn get_metadata_from_tracking_id_query(
    tracking_id: String,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<ChunkMetadata, ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;
    use crate::data::schema::chunk_metadata_tags::dsl as chunk_metadata_tags_columns;
    use crate::data::schema::dataset_tags::dsl as dataset_tags_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let (chunk_table, tag_set) = chunk_metadata_columns::chunk_metadata
        .left_join(
            chunk_metadata_tags_columns::chunk_metadata_tags
                .on(chunk_metadata_tags_columns::chunk_metadata_id.eq(chunk_metadata_columns::id)),
        )
        .left_join(
            dataset_tags_columns::dataset_tags
                .on(dataset_tags_columns::id.eq(chunk_metadata_tags_columns::tag_id)),
        )
        .filter(chunk_metadata_columns::tracking_id.eq(tracking_id))
        .filter(chunk_metadata_columns::dataset_id.eq(dataset_uuid))
        .select((
            ChunkMetadataTable::as_select(),
            sql::<sql_types::Array<sql_types::Text>>(
                "array_remove(array_agg(dataset_tags.tag), null)",
            )
            .nullable(),
        ))
        .group_by(chunk_metadata_columns::id)
        .first::<(ChunkMetadataTable, Option<Vec<String>>)>(&mut conn)
        .await
        .map_err(|_| {
            ServiceError::NotFound(
                "Chunk with tracking_id not found in the specified dataset".to_string(),
            )
        })?;

    Ok(ChunkMetadata::from_table_and_tag_set(
        chunk_table,
        tag_set.unwrap_or(vec![]),
    ))
}

pub async fn get_metadata_from_ids_query(
    chunk_ids: Vec<uuid::Uuid>,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Vec<ChunkMetadata>, ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;
    use crate::data::schema::chunk_metadata_tags::dsl as chunk_metadata_tags_columns;
    use crate::data::schema::dataset_tags::dsl as dataset_tags_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let chunk_metadata_pairs: Vec<(ChunkMetadataTable, Option<Vec<String>>)> =
        chunk_metadata_columns::chunk_metadata
            .left_join(
                chunk_metadata_tags_columns::chunk_metadata_tags
                    .on(chunk_metadata_tags_columns::chunk_metadata_id
                        .eq(chunk_metadata_columns::id)),
            )
            .left_join(
                dataset_tags_columns::dataset_tags
                    .on(dataset_tags_columns::id.eq(chunk_metadata_tags_columns::tag_id)),
            )
            .filter(chunk_metadata_columns::id.eq_any(chunk_ids))
            .filter(chunk_metadata_columns::dataset_id.eq(dataset_uuid))
            .select((
                ChunkMetadataTable::as_select(),
                sql::<sql_types::Array<sql_types::Text>>(
                    "array_remove(array_agg(dataset_tags.tag), null)",
                )
                .nullable(),
            ))
            .group_by(chunk_metadata_columns::id)
            .load::<(ChunkMetadataTable, Option<Vec<String>>)>(&mut conn)
            .await
            .map_err(|_| ServiceError::BadRequest("Failed to load metadata".to_string()))?;

    let chunk_metadatas = chunk_metadata_pairs
        .into_iter()
        .map(|(table, tag_set)| {
            ChunkMetadata::from_table_and_tag_set(table, tag_set.unwrap_or(vec![]))
        })
        .collect();

    Ok(chunk_metadatas)
}

pub async fn get_metadata_from_tracking_ids_query(
    tracking_ids: Vec<String>,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Vec<ChunkMetadata>, ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;
    use crate::data::schema::chunk_metadata_tags::dsl as chunk_metadata_tags_columns;
    use crate::data::schema::dataset_tags::dsl as dataset_tags_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let chunk_metadata_pairs: Vec<(ChunkMetadataTable, Option<Vec<String>>)> =
        chunk_metadata_columns::chunk_metadata
            .left_join(
                chunk_metadata_tags_columns::chunk_metadata_tags
                    .on(chunk_metadata_tags_columns::chunk_metadata_id
                        .eq(chunk_metadata_columns::id)),
            )
            .left_join(
                dataset_tags_columns::dataset_tags
                    .on(dataset_tags_columns::id.eq(chunk_metadata_tags_columns::tag_id)),
            )
            .filter(chunk_metadata_columns::tracking_id.eq_any(tracking_ids))
            .filter(chunk_metadata_columns::dataset_id.eq(dataset_uuid))
            .select((
                ChunkMetadataTable::as_select(),
                sql::<sql_types::Array<sql_types::Text>>(
                    "array_remove(array_agg(dataset_tags.tag), null)",
                )
                .nullable(),
            ))
            .group_by(chunk_metadata_columns::id)
            .load::<(ChunkMetadataTable, Option<Vec<String>>)>(&mut conn)
            .await
            .map_err(|_| ServiceError::BadRequest("Failed to load metadata".to_string()))?;

    let chunk_metadatas = chunk_metadata_pairs
        .into_iter()
        .map(|(table, tag_set)| {
            ChunkMetadata::from_table_and_tag_set(table, tag_set.unwrap_or(vec![]))
        })
        .collect();

    Ok(chunk_metadatas)
}

pub async fn bulk_delete_chunks_query(
    filter: Option<ChunkFilter>,
    deleted_at: chrono::NaiveDateTime,
    dataset_id: uuid::Uuid,
    dataset_config: DatasetConfiguration,
    pool: web::Data<Pool>,
) -> Result<(), ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    if dataset_config.LOCKED {
        return Err(ServiceError::BadRequest(
            "Cannot bulk delete a locked dataset".to_string(),
        ));
    }

    let filter = assemble_qdrant_filter(filter, None, None, dataset_id, pool.clone()).await?;
    let qdrant_collection = get_qdrant_collection_from_dataset_config(&dataset_config);
    let mut conn = pool
        .clone()
        .get()
        .await
        .expect("Failed to get connection to db");
    let mut offset: Option<uuid::Uuid> = None;
    let mut first_iteration = true;

    while offset.is_some() || first_iteration {
        let (search_results, offset_id) =
            scroll_dataset_points(100, offset, None, dataset_config.clone(), filter.clone())
                .await?;
        let qdrant_point_ids: Vec<uuid::Uuid> = search_results
            .iter()
            .map(|search_result| search_result.point_id)
            .collect();

        log::info!(
            "Deleting {:?} chunks with point_ids",
            qdrant_point_ids.len()
        );

        if !dataset_config.QDRANT_ONLY {
            let deleted_point_ids = conn
                .transaction::<_, diesel::result::Error, _>(|conn| {
                    async move {
                        {
                            let deleted_ids_uuids: Vec<(uuid::Uuid, uuid::Uuid)> = diesel::delete(
                                chunk_metadata_columns::chunk_metadata
                                    .filter(
                                        chunk_metadata_columns::qdrant_point_id
                                            .eq_any(qdrant_point_ids.clone()),
                                    )
                                    .filter(chunk_metadata_columns::dataset_id.eq(dataset_id))
                                    .filter(chunk_metadata_columns::created_at.le(deleted_at)),
                            )
                            .returning((
                                chunk_metadata_columns::id,
                                chunk_metadata_columns::qdrant_point_id,
                            ))
                            .get_results::<(uuid::Uuid, uuid::Uuid)>(conn)
                            .await?;

                            let (_deleted_ids, deleted_point_ids): (
                                Vec<uuid::Uuid>,
                                Vec<uuid::Uuid>,
                            ) = deleted_ids_uuids.into_iter().unzip();

                            Ok(deleted_point_ids)
                        }
                    }
                    .scope_boxed()
                })
                .await;

            match deleted_point_ids {
                Ok(point_ids) => {
                    delete_points_from_qdrant(point_ids, qdrant_collection.clone()).await?;
                }
                Err(e) => {
                    log::error!("Failed to delete chunks: {:?}", e);
                    return Err(ServiceError::BadRequest(
                        "Failed to delete chunks".to_string(),
                    ));
                }
            }
        } else {
            delete_points_from_qdrant(qdrant_point_ids.clone(), qdrant_collection.clone()).await?;
            update_dataset_chunk_count(dataset_id, -(qdrant_point_ids.len() as i32), pool.clone())
                .await?;
        }

        offset = offset_id;
        first_iteration = false;
    }
    Ok(())
}

/// Only inserts, does not try to upsert data
#[allow(clippy::type_complexity)]

pub async fn bulk_insert_chunk_metadata_query(
    mut insertion_data: Vec<ChunkData>,
    dataset_uuid: uuid::Uuid,
    upsert_by_tracking_id: bool,
    pool: web::Data<Pool>,
) -> Result<Vec<ChunkData>, ServiceError> {
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    let mut conn = pool
        .clone()
        .get()
        .await
        .expect("Failed to get connection to db");

    let (chunks_with_tracking_id, chunks_without_tracking_id): (Vec<ChunkData>, Vec<ChunkData>) =
        insertion_data
            .clone()
            .into_iter()
            .partition(|data| data.chunk_metadata.tracking_id.is_some());
    let chunks_with_tracking_id: Vec<ChunkMetadataTable> = chunks_with_tracking_id
        .iter()
        .map(|data| data.chunk_metadata.clone().into())
        .unique_by(|chunk: &ChunkMetadataTable| (chunk.tracking_id.clone(), chunk.dataset_id))
        .collect();
    let chunk_metadatas_to_insert: Vec<ChunkMetadataTable> = chunks_without_tracking_id
        .iter()
        .map(|data| data.chunk_metadata.clone().into())
        .chain(chunks_with_tracking_id.clone().into_iter())
        .collect();
    log::info!(
        "Inserting tracking_id chunks: {:?}",
        chunks_with_tracking_id.len()
    );
    log::info!(
        "Inserting non-tracking_id chunks: {:?}",
        chunks_without_tracking_id.len()
    );

    let inserted_chunks = if upsert_by_tracking_id {
        let temp_inserted_chunks = diesel::insert_into(chunk_metadata_columns::chunk_metadata)
            .values(&chunk_metadatas_to_insert)
            .on_conflict((
                chunk_metadata_columns::tracking_id,
                chunk_metadata_columns::dataset_id,
            ))
            .do_update()
            .set((
                chunk_metadata_columns::link.eq(excluded(chunk_metadata_columns::link)),
                chunk_metadata_columns::chunk_html.eq(excluded(chunk_metadata_columns::chunk_html)),
                chunk_metadata_columns::metadata.eq(excluded(chunk_metadata_columns::metadata)),
                chunk_metadata_columns::time_stamp.eq(excluded(chunk_metadata_columns::time_stamp)),
                chunk_metadata_columns::weight.eq(excluded(chunk_metadata_columns::weight)),
                chunk_metadata_columns::location.eq(excluded(chunk_metadata_columns::location)),
                chunk_metadata_columns::image_urls.eq(excluded(chunk_metadata_columns::image_urls)),
                chunk_metadata_columns::num_value.eq(excluded(chunk_metadata_columns::num_value)),
            ))
            .returning(ChunkMetadataTable::as_select())
            .get_results::<ChunkMetadataTable>(&mut conn)
            .await
            .map_err(|e| {
                log::error!("Failed to upsert chunk_metadata: {:?}", e);

                ServiceError::BadRequest("Failed to upsert chunk_metadata".to_string())
            })?;

        insertion_data.retain(|chunk_data| {
            temp_inserted_chunks.iter().any(|inserted_chunk| {
                inserted_chunk.id == chunk_data.chunk_metadata.id
                    || inserted_chunk.tracking_id == chunk_data.chunk_metadata.tracking_id
            })
        });

        log::info!(
            "Inserted {:?} out of {:?} chunks with upsert=true",
            temp_inserted_chunks.len(),
            insertion_data.len()
        );

        temp_inserted_chunks
    } else {
        let temp_inserted_chunks = diesel::insert_into(chunk_metadata_columns::chunk_metadata)
            .values(&chunk_metadatas_to_insert)
            .on_conflict_do_nothing()
            .get_results::<ChunkMetadataTable>(&mut conn)
            .await
            .map_err(|e| {
                log::error!("Failed to insert chunk_metadata: {:?}", e);

                ServiceError::BadRequest("Failed to insert chunk_metadata".to_string())
            })?;

        insertion_data.retain(|chunk_data| {
            temp_inserted_chunks
                .iter()
                .any(|inserted_chunk| inserted_chunk.id == chunk_data.chunk_metadata.id)
        });

        log::info!(
            "Inserted {:?} out of {:?} chunks with upsert=false",
            temp_inserted_chunks.len(),
            insertion_data.len()
        );

        temp_inserted_chunks
    };

    let insertion_data = insertion_data
        .into_iter()
        .map(|chunk_data| {
            let chunk_metadata_table = inserted_chunks
                .iter()
                .find(|inserted_chunk: &&ChunkMetadataTable| {
                    inserted_chunk.id == chunk_data.chunk_metadata.id
                        || (inserted_chunk
                            .tracking_id
                            .as_ref()
                            .is_some_and(|tracking_id| !tracking_id.is_empty())
                            && inserted_chunk.tracking_id == chunk_data.chunk_metadata.tracking_id)
                })
                .expect("Will always be present due to previous retain")
                .clone();

            let tag_set = chunk_data
                .chunk_metadata
                .tag_set
                .clone()
                .unwrap_or_default()
                .iter()
                .filter_map(|maybe_tag| maybe_tag.clone())
                .collect_vec();
            let chunk_metadata =
                ChunkMetadata::from_table_and_tag_set(chunk_metadata_table, tag_set);

            ChunkData {
                chunk_metadata,
                content: chunk_data.content,
                embedding_content: chunk_data.embedding_content,
                group_ids: chunk_data.group_ids,
                upsert_by_tracking_id: chunk_data.upsert_by_tracking_id,
                fulltext_boost: chunk_data.fulltext_boost,
                semantic_boost: chunk_data.semantic_boost,
            }
        })
        .collect::<Vec<ChunkData>>();

    use crate::data::schema::chunk_boosts::dsl as chunk_boosts_columns;

    // Insert the fulltext and semantic boosts
    let boosts_to_insert = insertion_data
        .iter()
        .filter_map(|chunk_data| {
            if chunk_data.fulltext_boost.is_none() && chunk_data.semantic_boost.is_none() {
                return None;
            }
            return Some(ChunkBoost {
                chunk_id: chunk_data.chunk_metadata.id,
                fulltext_boost_phrase: chunk_data
                    .fulltext_boost
                    .as_ref()
                    .map(|boost| boost.phrase.clone()),
                fulltext_boost_factor: chunk_data
                    .fulltext_boost
                    .as_ref()
                    .map(|boost| boost.boost_factor),
                semantic_boost_phrase: chunk_data
                    .semantic_boost
                    .as_ref()
                    .map(|boost| boost.phrase.clone()),
                semantic_boost_factor: chunk_data
                    .semantic_boost
                    .as_ref()
                    .map(|boost| boost.distance_factor as f64),
            });
        })
        .unique_by(|boost| boost.chunk_id)
        .collect::<Vec<ChunkBoost>>();

    diesel::insert_into(chunk_boosts_columns::chunk_boosts)
        .values(boosts_to_insert)
        .on_conflict(chunk_boosts_columns::chunk_id)
        .do_update()
        .set((
            chunk_boosts_columns::fulltext_boost_phrase
                .eq(excluded(chunk_boosts_columns::fulltext_boost_phrase)),
            chunk_boosts_columns::fulltext_boost_factor
                .eq(excluded(chunk_boosts_columns::fulltext_boost_factor)),
            chunk_boosts_columns::semantic_boost_phrase
                .eq(excluded(chunk_boosts_columns::semantic_boost_phrase)),
            chunk_boosts_columns::semantic_boost_factor
                .eq(excluded(chunk_boosts_columns::semantic_boost_factor)),
        ))
        .execute(&mut conn)
        .await
        .map_err(|e| {
            log::error!("Failed to create chunk boosts {:}", e);
            ServiceError::InternalServerError("Failed to create chunk boosts".to_string())
        })?;

    let chunk_group_bookmarks_to_insert: Vec<ChunkGroupBookmark> = insertion_data
        .clone()
        .iter()
        .filter_map(|data| {
            data.group_ids.as_ref().map(|group_ids| {
                group_ids
                    .clone()
                    .iter()
                    .map(|group_id| {
                        ChunkGroupBookmark::from_details(*group_id, data.chunk_metadata.id)
                    })
                    .collect::<Vec<ChunkGroupBookmark>>()
            })
        })
        .flatten()
        .collect();

    diesel::insert_into(chunk_group_bookmarks_columns::chunk_group_bookmarks)
        .values(chunk_group_bookmarks_to_insert)
        .on_conflict_do_nothing()
        .execute(&mut conn)
        .await
        .map_err(|_| {
            log::error!("Failed to insert chunk into groups");
            ServiceError::BadRequest("Failed to insert chunk into groups".to_string())
        })?;

    let chunk_tags_to_chunk_id: Vec<(Vec<DatasetTags>, uuid::Uuid)> = insertion_data
        .clone()
        .iter()
        .filter_map(|data| {
            data.chunk_metadata.clone().tag_set.map(|tags| {
                let tags = tags
                    .into_iter()
                    .filter_map(|maybe_tag| {
                        maybe_tag.map(|tag| DatasetTags {
                            id: uuid::Uuid::new_v4(),
                            dataset_id: dataset_uuid,
                            tag,
                        })
                    })
                    .collect_vec();

                (tags, data.chunk_metadata.id)
            })
        })
        .collect_vec();

    use crate::data::schema::chunk_metadata_tags::dsl as chunk_metadata_tags_columns;
    use crate::data::schema::dataset_tags::dsl as dataset_tags_columns;
    // TODO, dedupe and bulk insert this.
    for (dataset_tags, chunk_uuid) in chunk_tags_to_chunk_id {
        diesel::insert_into(dataset_tags_columns::dataset_tags)
            .values(&dataset_tags)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await
            .map_err(|e| {
                log::error!("Failed to create dataset tag {:}", e);
                ServiceError::BadRequest("Failed to create dataset tag".to_string())
            })?;

        let tag_names = dataset_tags
            .clone()
            .into_iter()
            .map(|tag| tag.tag)
            .collect_vec();

        let dataset_tags_existing =
            get_dataset_tags_id_from_names(pool.clone(), dataset_uuid, tag_names).await?;

        let mut needed_dataset_tags = dataset_tags.clone();
        // Remove all conflicts
        needed_dataset_tags.retain(|dataset_tag| {
            // If it isn't found aka None, then it is not a conflicting one
            !dataset_tags_existing
                .iter()
                .any(|predicate_tag| predicate_tag.tag == dataset_tag.tag)
        });
        // Add Back preexisting ones
        needed_dataset_tags.extend(dataset_tags_existing);

        let mut chunk_metadata_tags: Vec<ChunkMetadataTags> = vec![];
        for dataset_tag in needed_dataset_tags {
            chunk_metadata_tags.push(ChunkMetadataTags {
                id: uuid::Uuid::new_v4(),
                chunk_metadata_id: chunk_uuid,
                tag_id: dataset_tag.id,
            });
        }

        diesel::insert_into(chunk_metadata_tags_columns::chunk_metadata_tags)
            .values(&chunk_metadata_tags)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await
            .map_err(|e| {
                log::error!("Failed to create chunk metadata tags {:}", e);
                ServiceError::BadRequest("Failed to create chunk metadata tags".to_string())
            })?;
    }

    Ok(insertion_data)
}

pub async fn get_optional_metadata_from_tracking_id_query(
    tracking_id: String,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Option<ChunkMetadata>, ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;
    use crate::data::schema::chunk_metadata_tags::dsl as chunk_metadata_tags_columns;
    use crate::data::schema::dataset_tags::dsl as dataset_tags_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let optional_chunk: Option<(ChunkMetadataTable, Option<Vec<String>>)> =
        chunk_metadata_columns::chunk_metadata
            .left_join(
                chunk_metadata_tags_columns::chunk_metadata_tags
                    .on(chunk_metadata_tags_columns::chunk_metadata_id
                        .eq(chunk_metadata_columns::id)),
            )
            .left_join(
                dataset_tags_columns::dataset_tags
                    .on(dataset_tags_columns::id.eq(chunk_metadata_tags_columns::tag_id)),
            )
            .filter(chunk_metadata_columns::tracking_id.eq(tracking_id))
            .filter(chunk_metadata_columns::dataset_id.eq(dataset_uuid))
            .select((
                ChunkMetadataTable::as_select(),
                sql::<sql_types::Array<sql_types::Text>>(
                    "array_remove(array_agg(dataset_tags.tag), null)",
                )
                .nullable(),
            ))
            .group_by(chunk_metadata_columns::id)
            .load::<(ChunkMetadataTable, Option<Vec<String>>)>(&mut conn)
            .await
            .map_err(|e| {
                log::error!(
                    "Failed to execute get_optional_metadata_from_tracking_id_query: {:?}",
                    e
                );

                ServiceError::BadRequest(
                    "Failed to execute get_optional_metadata_from_tracking_id_query".to_string(),
                )
            })?
            .pop();

    Ok(optional_chunk.map(|(chunk_table, tag_set)| {
        ChunkMetadata::from_table_and_tag_set(chunk_table, tag_set.unwrap_or(vec![]))
    }))
}

pub async fn insert_chunk_metadata_query(
    chunk_data: ChunkMetadata,
    group_ids: Option<Vec<uuid::Uuid>>,
    dataset_uuid: uuid::Uuid,
    upsert_by_tracking_id: bool,
    pool: web::Data<Pool>,
) -> Result<ChunkMetadata, ServiceError> {
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;
    use crate::data::schema::chunk_metadata_tags::dsl as chunk_metadata_tags_columns;
    use crate::data::schema::dataset_tags::dsl as dataset_tags_columns;

    if let Some(other_tracking_id) = chunk_data.tracking_id.clone() {
        if upsert_by_tracking_id {
            if let Some(existing_chunk) = get_optional_metadata_from_tracking_id_query(
                other_tracking_id.clone(),
                chunk_data.dataset_id,
                pool.clone(),
            )
            .await?
            {
                let mut update_chunk = chunk_data.clone();
                update_chunk.id = existing_chunk.id;
                update_chunk.created_at = existing_chunk.created_at;
                update_chunk.qdrant_point_id = existing_chunk.qdrant_point_id;

                let updated_chunk = update_chunk_metadata_query(
                    update_chunk,
                    group_ids,
                    dataset_uuid,
                    pool.clone(),
                )
                .await?;

                return Ok(updated_chunk);
            }
        }
    }

    let chunk_table: ChunkMetadataTable = chunk_data.clone().into();

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let data_updated = diesel::insert_into(chunk_metadata_columns::chunk_metadata)
        .values(&chunk_table)
        .on_conflict_do_nothing()
        .execute(&mut conn)
        .await
        .map_err(|e| match e {
            diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::UniqueViolation,
                _,
            ) => ServiceError::DuplicateTrackingId(
                chunk_data.tracking_id.clone().unwrap_or("".to_string()),
            ),
            diesel::result::Error::NotFound => ServiceError::DuplicateTrackingId(
                chunk_data.tracking_id.clone().unwrap_or("".to_string()),
            ),
            _ => {
                log::error!("Failed to insert chunk_metadata: {:?}", e);
                ServiceError::BadRequest(format!("Failed to insert chunk_metadata {:}", e))
            }
        })?;

    if data_updated == 0 {
        return Err(ServiceError::DuplicateTrackingId(
            chunk_data.tracking_id.clone().unwrap_or("".to_string()),
        ));
    }

    if let Some(group_ids) = group_ids {
        diesel::insert_into(chunk_group_bookmarks_columns::chunk_group_bookmarks)
            .values(
                &group_ids
                    .into_iter()
                    .map(|group_id| ChunkGroupBookmark::from_details(group_id, chunk_data.id))
                    .collect::<Vec<ChunkGroupBookmark>>(),
            )
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await
            .map_err(|e| {
                println!("Failed to insert chunk into groups {:?}", e);
                ServiceError::BadRequest("Failed to insert chunk into groups".to_string())
            })?;
    }

    if let Some(tag_set) = chunk_data.tag_set.clone() {
        let dataset_tags_to_add = tag_set
            .into_iter()
            .filter_map(|maybe_tag| {
                maybe_tag.map(|tag| DatasetTags {
                    id: uuid::Uuid::new_v4(),
                    dataset_id: dataset_uuid,
                    tag,
                })
            })
            .collect_vec();

        diesel::insert_into(dataset_tags_columns::dataset_tags)
            .values(&dataset_tags_to_add)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await
            .map_err(|e| {
                log::error!("Failed to create dataset tag {:}", e);
                ServiceError::BadRequest("Failed to create dataset tag".to_string())
            })?;

        // Get the proper dataset_tags to add chunk_metadata_tags

        let tag_names = dataset_tags_to_add
            .clone()
            .into_iter()
            .map(|tag| tag.tag)
            .collect_vec();

        let dataset_tags_existing =
            get_dataset_tags_id_from_names(pool, dataset_uuid, tag_names).await?;

        let mut needed_dataset_tags = dataset_tags_to_add.clone();
        // Remove all conflicts
        needed_dataset_tags.retain(|dataset_tag| {
            // If it isn't found aka None, then it is not a conflicting one
            !dataset_tags_existing
                .iter()
                .any(|predicate_tag| predicate_tag.tag == dataset_tag.tag)
        });
        // Add Back preexisting ones
        needed_dataset_tags.extend(dataset_tags_existing);

        let mut chunk_metadata_tags: Vec<ChunkMetadataTags> = vec![];
        for tag_dataset in needed_dataset_tags {
            chunk_metadata_tags.push(ChunkMetadataTags {
                id: uuid::Uuid::new_v4(),
                chunk_metadata_id: chunk_data.id,
                tag_id: tag_dataset.id,
            });
        }

        diesel::insert_into(chunk_metadata_tags_columns::chunk_metadata_tags)
            .values(&chunk_metadata_tags)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await
            .map_err(|e| {
                log::error!("Failed to create chunk metadata tags {:}", e);
                ServiceError::BadRequest("Failed to create chunk metadata tags".to_string())
            })?;
    }

    Ok(chunk_data)
}

pub async fn insert_chunk_boost(
    chunk_boost: ChunkBoost,
    pool: web::Data<Pool>,
) -> Result<ChunkBoost, ServiceError> {
    use crate::data::schema::chunk_boosts::dsl as chunk_boosts_columns;
    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;
    diesel::insert_into(chunk_boosts_columns::chunk_boosts)
        .values(&chunk_boost)
        .on_conflict_do_nothing()
        .execute(&mut conn)
        .await
        .map_err(|e| {
            log::error!("Failed to insert chunk boost {:}", e);
            ServiceError::BadRequest("Failed to insert chunk boost".to_string())
        })?;
    Ok(chunk_boost)
}

pub async fn get_dataset_tags_id_from_names(
    pool: web::Data<Pool>,
    dataset_id: uuid::Uuid,
    tag_names: Vec<String>,
) -> Result<Vec<DatasetTags>, ServiceError> {
    use crate::data::schema::dataset_tags::dsl as dataset_tags_columns;

    let mut conn = pool
        .get()
        .await
        .expect("Failed to get connection from pool");

    dataset_tags_columns::dataset_tags
        .filter(dataset_tags_columns::dataset_id.eq(dataset_id))
        .filter(dataset_tags_columns::tag.eq_any(&tag_names))
        .load::<DatasetTags>(&mut conn)
        .await
        .map_err(|_| ServiceError::NotFound("Dataset Tag Not found".to_string()))
}

/// Bulk revert, assumes upsert chunk_ids were not upserted, only enterted

pub async fn bulk_revert_insert_chunk_metadata_query(
    chunk_ids: Vec<uuid::Uuid>,
    pool: web::Data<Pool>,
) -> Result<(), ServiceError> {
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;
    use crate::data::schema::chunk_metadata::dsl::*;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    diesel::delete(chunk_metadata.filter(id.eq_any(&chunk_ids)))
        .execute(&mut conn)
        .await
        .map_err(|e| {
            log::error!("Failed to revert insert transaction: {:?}", e);

            ServiceError::BadRequest("Failed to revert insert transaction".to_string())
        })?;

    diesel::delete(
        chunk_group_bookmarks_columns::chunk_group_bookmarks
            .filter(chunk_group_bookmarks_columns::chunk_metadata_id.eq_any(&chunk_ids)),
    )
    .execute(&mut conn)
    .await
    .map_err(|e| {
        log::error!("Failed to revert chunk into groups action {:?}", e);
        ServiceError::BadRequest("Failed to revert chunk into groups action".to_string())
    })?;

    Ok(())
}

pub async fn update_chunk_metadata_query(
    chunk_data: ChunkMetadata,
    group_ids: Option<Vec<uuid::Uuid>>,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<ChunkMetadata, ServiceError> {
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;
    use crate::data::schema::chunk_metadata_tags::dsl as chunk_metadata_tags_columns;
    use crate::data::schema::dataset_tags::dsl as dataset_tags_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let updated_chunk: ChunkMetadataTable = diesel::update(
        chunk_metadata_columns::chunk_metadata
            .filter(chunk_metadata_columns::id.eq(chunk_data.id))
            .filter(chunk_metadata_columns::dataset_id.eq(dataset_uuid)),
    )
    .set((
        chunk_metadata_columns::link.eq(chunk_data.link),
        chunk_metadata_columns::chunk_html.eq(chunk_data.chunk_html),
        chunk_metadata_columns::metadata.eq(chunk_data.metadata),
        chunk_metadata_columns::tracking_id.eq(chunk_data.tracking_id),
        chunk_metadata_columns::time_stamp.eq(chunk_data.time_stamp),
        chunk_metadata_columns::location.eq(chunk_data.location),
        chunk_metadata_columns::weight.eq(chunk_data.weight),
        chunk_metadata_columns::image_urls.eq(chunk_data.image_urls),
        chunk_metadata_columns::num_value.eq(chunk_data.num_value),
    ))
    .get_result::<ChunkMetadataTable>(&mut conn)
    .await
    .map_err(|e| {
        log::error!("Failed to update chunk_metadata: {:?}", e);
        ServiceError::BadRequest("Failed to update chunk metadata".to_string())
    })?;

    if let Some(group_ids) = group_ids {
        let group_id1 = group_ids.clone();
        diesel::insert_into(chunk_group_bookmarks_columns::chunk_group_bookmarks)
            .values(
                &group_ids
                    .into_iter()
                    .map(|group_id| ChunkGroupBookmark::from_details(group_id, updated_chunk.id))
                    .collect::<Vec<ChunkGroupBookmark>>(),
            )
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await
            .map_err(|_| ServiceError::BadRequest("Failed to create bookmark".to_string()))?;

        diesel::delete(
            chunk_group_bookmarks_columns::chunk_group_bookmarks
                .filter(chunk_group_bookmarks_columns::chunk_metadata_id.eq(updated_chunk.id))
                .filter(not(
                    chunk_group_bookmarks_columns::group_id.eq_any(group_id1)
                )),
        )
        .execute(&mut conn)
        .await
        .map_err(|_| ServiceError::BadRequest("Failed to delete chunk bookmarks".to_string()))?;
    }

    match chunk_data.tag_set {
        Some(tag_set) => {
            use crate::data::schema::chunk_metadata_tags::dsl as chunk_metadata_tags_columns;

            let _ = diesel::delete(
                chunk_metadata_tags_columns::chunk_metadata_tags
                    .filter(chunk_metadata_tags_columns::chunk_metadata_id.eq(updated_chunk.id)),
            )
            .execute(&mut conn)
            .await;

            let dataset_tags_to_add = tag_set
                .clone()
                .into_iter()
                .filter_map(|maybe_tag| {
                    maybe_tag.map(|tag| DatasetTags {
                        id: uuid::Uuid::new_v4(),
                        dataset_id: dataset_uuid,
                        tag,
                    })
                })
                .collect_vec();

            diesel::insert_into(dataset_tags_columns::dataset_tags)
                .values(&dataset_tags_to_add)
                .on_conflict_do_nothing()
                .get_results::<DatasetTags>(&mut conn)
                .await
                .map_err(|e| {
                    log::error!("Failed to create dataset tag {:}", e);
                    ServiceError::BadRequest("Failed to create dataset tag".to_string())
                })?;

            // Get the proper dataset_tags to add chunk_metadata_tags
            let tag_names = dataset_tags_to_add
                .clone()
                .into_iter()
                .map(|tag| tag.tag)
                .collect_vec();

            let dataset_tags_existing =
                get_dataset_tags_id_from_names(pool, dataset_uuid, tag_names).await?;

            let mut needed_dataset_tags = dataset_tags_to_add.clone();
            // Remove all conflicts
            needed_dataset_tags.retain(|dataset_tag| {
                // If it isn't found aka None, then it is not a conflicting one
                !dataset_tags_existing
                    .iter()
                    .any(|predicate_tag| predicate_tag.tag == dataset_tag.tag)
            });
            // Add Back preexisting ones
            needed_dataset_tags.extend(dataset_tags_existing);

            let mut chunk_metadata_tags: Vec<ChunkMetadataTags> = vec![];
            for tag_dataset in needed_dataset_tags {
                chunk_metadata_tags.push(ChunkMetadataTags {
                    id: uuid::Uuid::new_v4(),
                    chunk_metadata_id: chunk_data.id,
                    tag_id: tag_dataset.id,
                });
            }

            diesel::insert_into(chunk_metadata_tags_columns::chunk_metadata_tags)
                .values(&chunk_metadata_tags)
                .on_conflict_do_nothing()
                .execute(&mut conn)
                .await
                .map_err(|e| {
                    log::error!("Failed to update chunk metadata tags {:}", e);
                    ServiceError::BadRequest("Failed to update chunk metadata tags".to_string())
                })?;

            Ok(ChunkMetadata::from_table_and_tag_set_option_string(
                updated_chunk,
                Some(tag_set),
            ))
        }
        None => {
            // Fetch tagset
            let chunk_tagset: Vec<String> = chunk_metadata_columns::chunk_metadata
                .inner_join(chunk_metadata_tags_columns::chunk_metadata_tags.on(
                    chunk_metadata_tags_columns::chunk_metadata_id.eq(chunk_metadata_columns::id),
                ))
                .inner_join(
                    dataset_tags_columns::dataset_tags
                        .on(dataset_tags_columns::id.eq(chunk_metadata_tags_columns::tag_id)),
                )
                .filter(chunk_metadata_columns::id.eq(&chunk_data.id))
                .select(dataset_tags_columns::tag)
                .load::<String>(&mut conn)
                .await
                .map_err(|_| ServiceError::BadRequest("Failed to load metadata".to_string()))?;

            Ok(ChunkMetadata::from_table_and_tag_set(
                updated_chunk,
                chunk_tagset,
            ))
        }
    }
}

pub async fn update_chunk_boost_query(
    chunk_boost: ChunkBoost,
    pool: web::Data<Pool>,
) -> Result<(), ServiceError> {
    use crate::data::schema::chunk_boosts::dsl as chunk_boosts_columns;
    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    // Create a changeset based on which fields are present
    let changes: ChunkBoostChangeset = chunk_boost.clone().into();

    diesel::update(
        chunk_boosts_columns::chunk_boosts
            .filter(chunk_boosts_columns::chunk_id.eq(chunk_boost.chunk_id)),
    )
    .set(&changes)
    .execute(&mut conn)
    .await
    .map_err(|e| {
        log::error!("Failed to update chunk boost {:}", e);
        ServiceError::BadRequest("Failed to update chunk boost".to_string())
    })?;

    Ok(())
}

pub async fn get_chunk_boost_query(
    chunk_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Option<ChunkBoost>, ServiceError> {
    use crate::data::schema::chunk_boosts::dsl as chunk_boosts_columns;
    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let chunk_boost: Option<ChunkBoost> = chunk_boosts_columns::chunk_boosts
        .filter(chunk_boosts_columns::chunk_id.eq(chunk_id))
        .first(&mut conn)
        .await
        .optional()
        .map_err(|_| ServiceError::NotFound("Chunk Boost Not found".to_string()))?;

    Ok(chunk_boost)
}

pub async fn delete_chunk_metadata_query(
    chunk_uuid: Vec<uuid::Uuid>,
    deleted_at: chrono::NaiveDateTime,
    dataset: Dataset,
    pool: web::Data<Pool>,
    dataset_config: DatasetConfiguration,
) -> Result<(), ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let transaction_result = conn
        .transaction::<_, diesel::result::Error, _>(|conn| {
            async move {
                {
                    let deleted_points = diesel::delete(
                        chunk_metadata_columns::chunk_metadata
                            .filter(chunk_metadata_columns::id.eq_any(chunk_uuid.clone()))
                            .filter(chunk_metadata_columns::dataset_id.eq(dataset.id))
                            .filter(chunk_metadata_columns::created_at.le(deleted_at)),
                    )
                    .returning(chunk_metadata_columns::qdrant_point_id)
                    .get_results::<uuid::Uuid>(conn)
                    .await?;

                    Ok(deleted_points)
                }
            }
            .scope_boxed()
        })
        .await;

    let qdrant_collection = get_qdrant_collection_from_dataset_config(&dataset_config);

    match transaction_result {
        Ok(deleted_points) => delete_points_from_qdrant(deleted_points, qdrant_collection)
            .await
            .map_err(|_e| {
                ServiceError::BadRequest("Failed to delete chunk from qdrant".to_string())
            })
            .map(|_| ()),
        Err(_) => Err(ServiceError::BadRequest(
            "Failed to delete chunk data".to_string(),
        )),
    }
}

pub async fn get_qdrant_id_from_chunk_id_query(
    chunk_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<uuid::Uuid, ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let qdrant_point_ids: Vec<uuid::Uuid> = chunk_metadata_columns::chunk_metadata
        .select(chunk_metadata_columns::qdrant_point_id)
        .filter(chunk_metadata_columns::id.eq(chunk_id))
        .load(&mut conn)
        .await
        .map_err(|_err| ServiceError::BadRequest("Failed to get qdrant_point_id".to_string()))?;

    match qdrant_point_ids.get(0) {
        Some(x) => Ok(*x),
        None => Err(ServiceError::BadRequest(
            "Failed to get qdrant_point_id for chunk_id".to_string(),
        )),
    }
}

pub async fn get_qdrant_ids_from_chunk_ids_query(
    chunk_ids: Vec<UnifiedId>,
    pool: web::Data<Pool>,
) -> Result<Vec<uuid::Uuid>, ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let qdrant_point_ids: Vec<uuid::Uuid> = match chunk_ids.get(0) {
        Some(UnifiedId::TrieveUuid(_)) => chunk_metadata_columns::chunk_metadata
            .select(chunk_metadata_columns::qdrant_point_id)
            .filter(
                chunk_metadata_columns::id.eq_any(
                    chunk_ids
                        .iter()
                        .map(|x| x.as_uuid().unwrap())
                        .collect::<Vec<uuid::Uuid>>(),
                ),
            )
            .load(&mut conn)
            .await
            .map_err(|_err| {
                ServiceError::BadRequest(
                    "Failed to get qdrant_point_id and collision_qdrant_id".to_string(),
                )
            })?,
        Some(UnifiedId::TrackingId(_)) => chunk_metadata_columns::chunk_metadata
            .select(chunk_metadata_columns::qdrant_point_id)
            .filter(
                chunk_metadata_columns::tracking_id.eq_any(
                    chunk_ids
                        .iter()
                        .map(|x| x.as_tracking_id().unwrap())
                        .collect::<Vec<String>>(),
                ),
            )
            .load(&mut conn)
            .await
            .map_err(|_err| {
                ServiceError::BadRequest(
                    "Failed to get qdrant_point_id and collision_qdrant_id".to_string(),
                )
            })?,
        None => {
            return Err(ServiceError::BadRequest(
                "Must pass in an ID to the condition".to_string(),
            ))
        }
    };

    Ok(qdrant_point_ids)
}

pub fn get_slice_from_vec_string(vec: Vec<String>, index: usize) -> Result<String, ServiceError> {
    match vec.get(index) {
        Some(x) => Ok(x.clone()),
        None => Err(ServiceError::BadRequest(
            "Index out of bounds for vec".to_string(),
        )),
    }
}

static STOP_WORDS: once_cell::sync::Lazy<Vec<String>> = once_cell::sync::Lazy::new(|| {
    include_str!("../stop-words.txt")
        .lines()
        .map(|x| x.to_string())
        .collect()
});

pub fn get_stop_words() -> &'static Vec<String> {
    &STOP_WORDS
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum HighlightStrategy {
    ExactMatch,
    V1,
}

#[allow(clippy::too_many_arguments)]
pub fn get_highlights_with_exact_match(
    chunk_html: Option<String>,
    query: String,
    threshold: Option<f64>,
    delimiters: Vec<char>,
    max_length: Option<u32>,
    max_num: Option<u32>,
    window_size: Option<u32>,
    pre_tag: Option<String>,
    post_tag: Option<String>,
) -> Result<(Option<String>, Vec<String>), ServiceError> {
    let content = convert_html_to_text(&chunk_html.clone().unwrap_or_default());
    let cleaned_query = query.replace(
        |c: char| (delimiters.contains(&c) && c != ' ') || c == '\"',
        "",
    );
    let pre_tag = pre_tag.unwrap_or("<mark><b>".to_string());
    let post_tag = post_tag.unwrap_or("</b></mark>".to_string());

    let stop_words = get_stop_words();
    let query_parts_split_by_stop_words: Vec<String> = cleaned_query
        .split_whitespace()
        .collect_vec()
        .chunk_by(|a, b| {
            !stop_words.contains(&a.to_lowercase()) && !stop_words.contains(&b.to_lowercase())
        })
        .map(|chunk| {
            chunk
                .iter()
                .filter_map(|word| match stop_words.contains(&word.to_lowercase()) {
                    true => None,
                    false => Some(word.to_string()),
                })
                .collect_vec()
        })
        .filter_map(|chunk| match chunk.is_empty() {
            true => None,
            false => Some(chunk.join(" ")),
        })
        .filter(|x| !x.is_empty())
        .collect_vec();
    let mut additional_multi_token_queries = query_parts_split_by_stop_words
        .clone()
        .into_iter()
        .filter_map(|part| {
            if part.split_whitespace().count() > 1 {
                Some(part)
            } else {
                None
            }
        })
        .collect_vec();
    let mut idxs_of_non_stop_words = query_parts_split_by_stop_words
        .iter()
        .filter_map(|part| cleaned_query.find(part))
        .collect_vec();
    idxs_of_non_stop_words.sort();
    let tweens = idxs_of_non_stop_words
        .iter()
        .zip(idxs_of_non_stop_words.iter().skip(1))
        .map(|(a, b)| (*a, *b))
        .collect_vec();
    let mut start_index = 0;
    for (start, end) in tweens {
        let mut valid_start_char_boundary = start;
        while !cleaned_query.is_char_boundary(valid_start_char_boundary)
            && valid_start_char_boundary > 0
        {
            valid_start_char_boundary -= 1;
        }
        let mut valid_end_char_boundary = end;
        while !cleaned_query.is_char_boundary(valid_end_char_boundary)
            && valid_end_char_boundary < cleaned_query.len()
        {
            valid_end_char_boundary += 1;
        }
        let query_split = cleaned_query
            .get(valid_start_char_boundary..valid_end_char_boundary)
            .unwrap_or_default()
            .trim()
            .to_string();
        if query_split
            .split_whitespace()
            .filter(|x| !stop_words.contains(&x.to_lowercase()))
            .count()
            > 1
        {
            additional_multi_token_queries.push(query_split);
        }
        start_index = valid_start_char_boundary;
    }
    additional_multi_token_queries.push(
        cleaned_query
            .get(start_index..)
            .unwrap_or_default()
            .trim()
            .to_string(),
    );
    let query_split = cleaned_query.split_whitespace().collect_vec();
    let num_words_in_query = query_split.len();
    let mut starting_length = 0;
    if !query_split.is_empty() {
        starting_length = query_split.len() - 1;
    }
    while starting_length > 0 {
        let mut current_skip = 0;
        while current_skip <= query_split.len() - starting_length {
            let split_skip = query_split
                .iter()
                .skip(current_skip)
                .take(starting_length)
                .map(|x| x.trim().to_string())
                .collect_vec()
                .join(" ");
            if starting_length > 2
                || split_skip
                    .split_whitespace()
                    .filter(|x| !stop_words.contains(&x.to_lowercase()))
                    .count()
                    >= 1
            {
                if split_skip.split_whitespace().count() > 1 {
                    additional_multi_token_queries.push(split_skip);
                } else {
                    additional_multi_token_queries.push(format!("{} ", split_skip));
                    additional_multi_token_queries.push(format!(" {}", split_skip));
                    additional_multi_token_queries.push(format!(" {} ", split_skip));
                };
            }
            current_skip += 1;
        }
        starting_length -= 1;
    }
    additional_multi_token_queries.retain(|x| !x.trim().is_empty());
    additional_multi_token_queries.insert(0, cleaned_query.clone());
    additional_multi_token_queries.insert(0, query.clone());
    additional_multi_token_queries = additional_multi_token_queries
        .into_iter()
        .map(|x| x.to_string())
        .unique()
        .collect_vec();
    additional_multi_token_queries.sort_by(|a, b| {
        // Use More effecient algo for ranking
        //
        if num_words_in_query > 20 {
            return b.trim().len().cmp(&a.trim().len());
        }

        let a_len = a.split_whitespace().count();
        let b_len: usize = b.split_whitespace().count();
        match b_len.cmp(&a_len) {
            std::cmp::Ordering::Equal => b.trim().len().cmp(&a.trim().len()),
            other => other,
        }
    });

    let mut cumulative_phrases: Vec<(String, Vec<String>)> = vec![];
    for potential_query in additional_multi_token_queries {
        if cumulative_phrases
            .iter()
            .any(|(cumulative_query, _)| cumulative_query.contains(potential_query.trim()))
        {
            continue;
        }

        let idxs_of_query_count_in_content = content
            .to_lowercase()
            .match_indices(&potential_query.to_lowercase())
            .map(|(i, _)| i)
            .collect_vec();
        let mut phrases = idxs_of_query_count_in_content
            .iter()
            .map(|i| {
                let mut start_valid_boundary = *i;
                while !content.is_char_boundary(start_valid_boundary) && start_valid_boundary > 0 {
                    start_valid_boundary -= 1;
                }
                let mut end_valid_boundary = *i + potential_query.len();
                while !content.is_char_boundary(end_valid_boundary)
                    && end_valid_boundary < content.len()
                {
                    end_valid_boundary += 1;
                }

                content
                    .get(start_valid_boundary..end_valid_boundary)
                    .unwrap_or_default()
                    .to_string()
            })
            .collect_vec();
        phrases.truncate(max_num.unwrap_or(3) as usize);
        if !phrases.is_empty() {
            cumulative_phrases.push((potential_query, phrases));
        }
    }

    if !cumulative_phrases.is_empty() {
        let phrases = cumulative_phrases
            .iter()
            .take(max_num.unwrap_or(3) as usize)
            .flat_map(|(_, phrases)| phrases.clone())
            .collect_vec();
        let new_output = Some(apply_highlights_to_html(
            chunk_html.clone().unwrap_or_default(),
            phrases
                .clone()
                .into_iter()
                .unique()
                .map(|x| x.trim().to_string())
                .collect_vec(),
            &pre_tag,
            &post_tag,
        ));

        let window = window_size.unwrap_or(0);
        if window == 0 {
            return Ok((
                new_output,
                phrases
                    .clone()
                    .into_iter()
                    .unique()
                    .take(max_num.unwrap_or(3) as usize)
                    .collect_vec(),
            ));
        }

        let half_window = std::cmp::max(window / 2, 1);
        let mut highlights_with_window = vec![];

        let matched_potential_queries = cumulative_phrases
            .iter()
            .take(max_num.unwrap_or(3) as usize)
            .map(|(x, _)| x.clone())
            .collect_vec();
        for potential_query in matched_potential_queries.clone() {
            let mut matched_idxs: Vec<usize> = content
                .to_lowercase()
                .match_indices(&potential_query.to_lowercase())
                .map(|(i, _)| i)
                .collect_vec();
            matched_idxs.sort();

            let mut grouped_idxs = if matched_idxs.len() == 1 {
                vec![(0, content.len())]
            } else {
                let tweens = matched_idxs
                    .iter()
                    .zip(matched_idxs.iter().skip(1))
                    .map(|(a, b)| (*a, *b))
                    .collect_vec();

                let mut start_index = 0;
                let mut splits = vec![];
                for (start, end) in tweens {
                    splits.push((start_index, end));
                    start_index = start + potential_query.len();
                }
                splits.push((start_index, content.len()));

                splits
            };
            let grouped_idxs_len = grouped_idxs.len();
            if let Some((start, end)) = grouped_idxs.last() {
                if *end != content.len() {
                    grouped_idxs[grouped_idxs_len - 1] = (*start, content.len());
                }
            }

            let content_splits: Vec<String> = grouped_idxs
                .iter()
                .map(|(start, end)| {
                    let mut start_valid_boundary = *start;
                    while !content.is_char_boundary(start_valid_boundary)
                        && start_valid_boundary > 0
                    {
                        start_valid_boundary -= 1;
                    }
                    let mut end_valid_boundary = *end;
                    while !content.is_char_boundary(end_valid_boundary)
                        && end_valid_boundary < content.len()
                    {
                        end_valid_boundary += 1;
                    }

                    content
                        .get(start_valid_boundary..end_valid_boundary)
                        .unwrap_or_default()
                        .to_string()
                })
                .collect_vec();

            let cur_highlights_with_window: Vec<String> = content_splits
                .iter()
                .map(|split| {
                    let idx_of_query = split
                        .to_lowercase()
                        .find(&potential_query.to_lowercase())
                        .unwrap_or(0);
                    let mut start_valid_boundary = idx_of_query;
                    while !split.is_char_boundary(start_valid_boundary) && start_valid_boundary > 0
                    {
                        start_valid_boundary -= 1;
                    }
                    let first_split = split.chars().take(start_valid_boundary).collect::<String>();

                    let mut end_valid_boundary = idx_of_query + potential_query.len();
                    while !split.is_char_boundary(end_valid_boundary)
                        && end_valid_boundary < split.len()
                    {
                        end_valid_boundary += 1;
                    }
                    let last_split = split
                        .chars()
                        .skip(idx_of_query + potential_query.len())
                        .collect::<String>();
                    let text_between_splits = split
                        .chars()
                        .skip(start_valid_boundary)
                        .take(end_valid_boundary - start_valid_boundary)
                        .collect::<String>();

                    let mut half_window_usize = half_window as usize;

                    let mut first_expansion = first_split
                        .split_inclusive(' ')
                        .rev()
                        .take(half_window_usize)
                        .collect::<Vec<&str>>()
                        .iter()
                        .rev()
                        .map(|x| x.to_string())
                        .collect::<Vec<String>>()
                        .join("");

                    if first_expansion.split_whitespace().count() < half_window_usize {
                        half_window_usize +=
                            half_window_usize - first_expansion.split_whitespace().count();
                    }

                    let last_expansion = last_split
                        .split_inclusive(' ')
                        .take(half_window_usize)
                        .collect::<Vec<&str>>()
                        .join("");

                    if last_expansion.split_whitespace().count() < half_window_usize {
                        half_window_usize +=
                            half_window_usize - last_expansion.split_whitespace().count();
                        first_expansion = first_split
                            .split_inclusive(' ')
                            .rev()
                            .take(half_window_usize)
                            .collect::<Vec<&str>>()
                            .iter()
                            .rev()
                            .map(|x| x.to_string())
                            .collect::<Vec<String>>()
                            .join("");
                    }

                    format!(
                        "{}{}{}",
                        first_expansion, text_between_splits, last_expansion
                    )
                })
                .collect_vec();
            highlights_with_window.extend(cur_highlights_with_window);
        }

        let idxs_of_highlights_with_window = highlights_with_window
            .iter()
            .map(|original_text_for_highlight| {
                (
                    content
                        .find(original_text_for_highlight)
                        .unwrap_or_default(),
                    original_text_for_highlight,
                )
            })
            .sorted_by(|(a, _), (b, _)| a.cmp(b))
            .collect_vec();
        let mut result_highlights_with_window = idxs_of_highlights_with_window
            .iter()
            .take(1_usize)
            .map(|highlight| (highlight.1.clone(), false))
            .collect_vec();
        for i in 1..idxs_of_highlights_with_window.len() {
            let current_start = idxs_of_highlights_with_window[i].0;
            let prev_end = idxs_of_highlights_with_window[i - 1].0
                + idxs_of_highlights_with_window[i - 1].1.len();

            if current_start < prev_end {
                let overlap = prev_end - current_start;
                let append_highlight = idxs_of_highlights_with_window[i]
                    .1
                    .chars()
                    .skip(overlap)
                    .collect::<String>();
                result_highlights_with_window.retain(|result_highlight| {
                    result_highlight.0 != idxs_of_highlights_with_window[i].1.clone()
                        && result_highlight.0 != idxs_of_highlights_with_window[i - 1].1.clone()
                });

                let merged_highlight = format!(
                    "{}{}",
                    idxs_of_highlights_with_window[i - 1].1,
                    append_highlight
                );
                result_highlights_with_window.push((merged_highlight, true));
            } else {
                result_highlights_with_window
                    .push((idxs_of_highlights_with_window[i].1.clone(), false));
            }
        }

        let final_highlights_with_window = result_highlights_with_window
            .into_iter()
            .filter_map(|(x, _)| {
                let mut new_x = x.clone();
                for potential_query in matched_potential_queries.clone() {
                    let mut query_idx =
                        match new_x.to_lowercase().find(&potential_query.to_lowercase()) {
                            Some(x) => x,
                            None => continue,
                        };
                    while !new_x.is_char_boundary(query_idx) && query_idx > 0 {
                        query_idx -= 1;
                    }
                    let mut query_end = query_idx + potential_query.len();
                    while !new_x.is_char_boundary(query_end) && query_end < new_x.len() {
                        query_end += 1;
                    }

                    if !new_x.is_char_boundary(query_idx) || !new_x.is_char_boundary(query_end) {
                        continue;
                    }

                    new_x = format!(
                        "{}{}{}{}{}",
                        &new_x.get(0..query_idx).unwrap_or_default(),
                        pre_tag,
                        &new_x.get(query_idx..query_end).unwrap_or_default(),
                        post_tag,
                        &new_x.get(query_end..).unwrap_or_default()
                    );
                }
                new_x = new_x.replace(&format!("{}{}", pre_tag, post_tag), "");

                if new_x != x {
                    Some(new_x)
                } else {
                    None
                }
            })
            .take(max_num.unwrap_or(3) as usize)
            .collect_vec();

        if !final_highlights_with_window.is_empty() {
            return Ok((new_output, final_highlights_with_window));
        }
    }

    if threshold.unwrap_or(0.8) >= 1.0 {
        return Ok((chunk_html, vec![]));
    }

    let search_options = SearchOptions::new().threshold(threshold.unwrap_or(0.8));
    let mut engine: SimSearch<usize> = SimSearch::new_with(search_options);
    let split_content = content
        .split_inclusive(|c: char| delimiters.contains(&c))
        .flat_map(|x| {
            x.to_string()
                .split_inclusive(' ')
                .map(|x| x.to_string())
                .collect::<Vec<String>>()
                .chunks(max_length.unwrap_or(5) as usize)
                .map(|x| x.join(""))
                .collect::<Vec<String>>()
        })
        .collect::<Vec<String>>();

    split_content.iter().enumerate().for_each(|(i, x)| {
        engine.insert(i, x);
    });

    let results: Vec<usize> = engine.search(&query);

    let mut matched_idxs = vec![];
    let mut matched_idxs_set = HashSet::new();
    for x in results.iter().take(max_num.unwrap_or(3) as usize) {
        matched_idxs_set.insert(*x);
        matched_idxs.push(*x);
    }

    matched_idxs.sort();

    let window = window_size.unwrap_or(0);
    if window == 0 {
        let phrases = matched_idxs
            .iter()
            .map(|x| split_content.get(*x))
            .filter_map(|x| x.map(|x| x.to_string()))
            .collect::<Vec<String>>();
        let new_output = Some(apply_highlights_to_html(
            chunk_html.unwrap_or_default().clone(),
            phrases.clone(),
            &pre_tag,
            &post_tag,
        ));
        return Ok((new_output, phrases.clone()));
    }

    let half_window = std::cmp::max(window / 2, 1);
    // edge case 1: When the half window size is greater than the length of left or right phrase,
    // we need to search further to get the correct windowed phrase
    // edge case 2: When two windowed phrases overlap, we need to trim the first one.
    let mut windowed_phrases = vec![];
    // Used to keep track of the number of words used in the phrase
    let mut used_phrases: HashMap<usize, usize> = HashMap::new();
    for idx in matched_idxs.clone() {
        let phrase = get_slice_from_vec_string(split_content.clone(), idx)?;
        let mut next_phrase = String::new();
        if idx < split_content.len() - 1 {
            let mut start = idx + 1;
            let mut count: usize = 0;
            while (count as u32) < half_window {
                if start >= split_content.len() || matched_idxs_set.contains(&start) {
                    break;
                }
                let slice = get_slice_from_vec_string(split_content.clone(), start)?;
                let candidate_words = slice
                    .split_inclusive(' ')
                    .take(half_window as usize - count)
                    .collect::<Vec<&str>>();
                used_phrases.insert(
                    start,
                    std::cmp::min(candidate_words.len(), half_window as usize - count),
                );
                count += candidate_words.len();
                next_phrase.push_str(&candidate_words.join(""));
                start += 1;
            }
        }
        let mut prev_phrase = String::new();
        if idx > 0 {
            let mut start = idx - 1;
            let mut count: usize = 0;
            while (count as u32) < half_window {
                let slice = get_slice_from_vec_string(split_content.clone(), start)?;
                let split_words = slice.split_inclusive(' ').collect::<Vec<&str>>();
                if matched_idxs_set.contains(&start) {
                    break;
                }
                if used_phrases.contains_key(&start)
                    && split_words.len()
                        > *used_phrases
                            .get(&start)
                            .ok_or(ServiceError::BadRequest("Index out of bounds".to_string()))?
                {
                    let remaining_count = half_window as usize - count;
                    let available_word_len = split_words.len()
                        - *used_phrases
                            .get(&start)
                            .ok_or(ServiceError::BadRequest("Index out of bounds".to_string()))?;
                    if remaining_count > available_word_len {
                        count += remaining_count - available_word_len;
                    } else {
                        break;
                    }
                }
                if used_phrases.contains_key(&start)
                    && split_words.len()
                        <= *used_phrases
                            .get(&start)
                            .ok_or(ServiceError::BadRequest("Index out of bounds".to_string()))?
                {
                    break;
                }
                let candidate_words = split_words
                    .into_iter()
                    .rev()
                    .take(half_window as usize - count)
                    .collect::<Vec<&str>>();
                count += candidate_words.len();
                prev_phrase = format!("{}{}", candidate_words.iter().rev().join(""), prev_phrase);
                if start == 0 {
                    break;
                }
                start -= 1;
            }
        }
        let highlighted_phrase = phrase.replace(
            phrase.trim(),
            &format!("{}{}{}", pre_tag, phrase.trim(), post_tag),
        );
        let windowed_phrase = format!("{}{}{}", prev_phrase, highlighted_phrase, next_phrase);
        windowed_phrases.push(windowed_phrase);
    }
    let matched_phrases = matched_idxs
        .clone()
        .iter()
        .filter_map(|x| split_content.get(*x).cloned())
        .collect::<Vec<String>>();
    let result_matches = if windowed_phrases.is_empty() {
        matched_phrases.clone()
    } else {
        windowed_phrases.clone()
    };

    let new_output = Some(apply_highlights_to_html(
        chunk_html.clone().unwrap_or_default(),
        matched_phrases,
        &pre_tag,
        &post_tag,
    ));
    Ok((new_output, result_matches))
}

#[allow(clippy::too_many_arguments)]

pub fn get_highlights(
    chunk_html: Option<String>,
    query: String,
    threshold: Option<f64>,
    delimiters: Vec<char>,
    max_length: Option<u32>,
    max_num: Option<u32>,
    window_size: Option<u32>,
    pre_tag: Option<String>,
    post_tag: Option<String>,
) -> Result<(Option<String>, Vec<String>), ServiceError> {
    let pre_tag = pre_tag.unwrap_or("<mark><b>".to_string());
    let post_tag = post_tag.unwrap_or("</b></mark>".to_string());

    let content = convert_html_to_text(&chunk_html.clone().unwrap_or_default());
    let search_options = SearchOptions::new().threshold(threshold.unwrap_or(0.8));
    let mut engine: SimSearch<usize> = SimSearch::new_with(search_options);
    let split_content = content
        .split_inclusive(|c: char| delimiters.contains(&c))
        .flat_map(|x| {
            x.to_string()
                .split_inclusive(' ')
                .map(|x| x.to_string())
                .collect::<Vec<String>>()
                .chunks(max_length.unwrap_or(5) as usize)
                .map(|x| x.join(""))
                .collect::<Vec<String>>()
        })
        .collect::<Vec<String>>();

    split_content.iter().enumerate().for_each(|(i, x)| {
        engine.insert(i, x);
    });

    let results: Vec<usize> = engine.search(&query);

    let mut matched_idxs = vec![];
    let mut matched_idxs_set = HashSet::new();
    for x in results.iter().take(max_num.unwrap_or(3) as usize) {
        matched_idxs_set.insert(*x);
        matched_idxs.push(*x);
    }

    matched_idxs.sort();

    let window = window_size.unwrap_or(0);
    if window == 0 {
        let phrases = matched_idxs
            .iter()
            .map(|x| split_content.get(*x))
            .filter_map(|x| x.map(|x| x.to_string()))
            .collect::<Vec<String>>();
        let new_output = Some(apply_highlights_to_html(
            chunk_html.clone().unwrap_or_default(),
            phrases.clone(),
            &pre_tag,
            &post_tag,
        ));
        return Ok((new_output, phrases));
    }

    let half_window = std::cmp::max(window / 2, 1);
    // edge case 1: When the half window size is greater than the length of left or right phrase,
    // we need to search further to get the correct windowed phrase
    // edge case 2: When two windowed phrases overlap, we need to trim the first one.
    let mut windowed_phrases = vec![];
    // Used to keep track of the number of words used in the phrase
    let mut used_phrases: HashMap<usize, usize> = HashMap::new();

    for idx in matched_idxs.clone() {
        let phrase = get_slice_from_vec_string(split_content.clone(), idx)?;
        let mut next_phrase = String::new();
        if idx < split_content.len() - 1 {
            let mut start = idx + 1;
            let mut count: usize = 0;
            while (count as u32) < half_window {
                if start >= split_content.len() || matched_idxs_set.contains(&start) {
                    break;
                }
                let slice = get_slice_from_vec_string(split_content.clone(), start)?;
                let candidate_words = slice
                    .split_inclusive(' ')
                    .take(half_window as usize - count)
                    .collect::<Vec<&str>>();
                used_phrases.insert(
                    start,
                    std::cmp::min(candidate_words.len(), half_window as usize - count),
                );
                count += candidate_words.len();
                next_phrase.push_str(&candidate_words.join(""));
                start += 1;
            }
        }
        let mut prev_phrase = String::new();
        if idx > 0 {
            let mut start = idx - 1;
            let mut count: usize = 0;
            while (count as u32) < half_window {
                let slice = get_slice_from_vec_string(split_content.clone(), start)?;
                let split_words = slice.split_inclusive(' ').collect::<Vec<&str>>();
                if matched_idxs_set.contains(&start) {
                    break;
                }
                if used_phrases.contains_key(&start)
                    && split_words.len()
                        > *used_phrases
                            .get(&start)
                            .ok_or(ServiceError::BadRequest("Index out of bounds".to_string()))?
                {
                    let remaining_count = half_window as usize - count;
                    let available_word_len = split_words.len()
                        - *used_phrases
                            .get(&start)
                            .ok_or(ServiceError::BadRequest("Index out of bounds".to_string()))?;
                    if remaining_count > available_word_len {
                        count += remaining_count - available_word_len;
                    } else {
                        break;
                    }
                }
                if used_phrases.contains_key(&start)
                    && split_words.len()
                        <= *used_phrases
                            .get(&start)
                            .ok_or(ServiceError::BadRequest("Index out of bounds".to_string()))?
                {
                    break;
                }
                let candidate_words = split_words
                    .into_iter()
                    .rev()
                    .take(half_window as usize - count)
                    .collect::<Vec<&str>>();
                count += candidate_words.len();
                prev_phrase = format!("{}{}", candidate_words.iter().rev().join(""), prev_phrase);
                if start == 0 {
                    break;
                }
                start -= 1;
            }
        }

        let highlighted_phrase = phrase.replace(
            phrase.trim(),
            &format!("{}{}{}", pre_tag, phrase.trim(), post_tag),
        );
        let windowed_phrase = format!("{}{}{}", prev_phrase, highlighted_phrase, next_phrase);
        windowed_phrases.push(windowed_phrase);
    }
    let matched_phrases = matched_idxs
        .clone()
        .iter()
        .filter_map(|x| split_content.get(*x).cloned())
        .collect::<Vec<String>>();
    let result_matches = if windowed_phrases.is_empty() {
        matched_phrases.clone()
    } else {
        windowed_phrases.clone()
    };

    let new_output = Some(apply_highlights_to_html(
        chunk_html.clone().unwrap_or_default(),
        matched_phrases,
        &pre_tag,
        &post_tag,
    ));
    Ok((new_output, result_matches))
}

fn apply_highlights_to_html(
    input_chunk_html: String,
    phrases: Vec<String>,
    pre_tag: &str,
    post_tag: &str,
) -> String {
    let mut replaced_phrases = HashSet::new();
    let mut chunk_html = input_chunk_html.clone();
    for phrase in phrases.clone() {
        let lower_case_trimmed_phrase = phrase.to_lowercase().trim().to_string();
        if replaced_phrases.contains(&lower_case_trimmed_phrase) || phrase.len() <= 1 {
            continue;
        }
        let replace_phrase = phrase.clone();
        let idxs_of_query_count_in_content = chunk_html
            .to_lowercase()
            .match_indices(&replace_phrase.to_lowercase())
            .map(|(i, _)| i)
            .collect_vec();
        let mut all_case_matches = vec![];
        for i in idxs_of_query_count_in_content {
            let mut start_valid_boundary = i;
            while !chunk_html.is_char_boundary(start_valid_boundary) && start_valid_boundary > 0 {
                start_valid_boundary -= 1;
            }
            let mut end_valid_boundary = i + replace_phrase.len();
            while !chunk_html.is_char_boundary(end_valid_boundary)
                && end_valid_boundary < chunk_html.len()
            {
                end_valid_boundary += 1;
            }
            if let Some(chunk_html_slice_to_replace) =
                &chunk_html.get(start_valid_boundary..end_valid_boundary)
            {
                all_case_matches.push(chunk_html_slice_to_replace.to_string());
            }
        }
        all_case_matches.iter().unique().for_each(|all_case_match| {
            chunk_html = chunk_html.replace(
                all_case_match,
                &format!("{}{}{}", pre_tag, all_case_match, post_tag),
            );
        });

        replaced_phrases.insert(lower_case_trimmed_phrase);
    }
    chunk_html
}

pub async fn get_row_count_for_organization_id_query(
    organization_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<usize, ServiceError> {
    use crate::data::schema::organization_usage_counts::dsl as organization_usage_counts_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let chunk_metadata_count = organization_usage_counts_columns::organization_usage_counts
        .filter(organization_usage_counts_columns::org_id.eq(organization_id))
        .select(organization_usage_counts_columns::chunk_count)
        .first::<i32>(&mut conn)
        .await
        .map_err(|_| {
            log::error!("Failed to get chunk count for organization");
            ServiceError::BadRequest("Failed to get chunk count for organization".to_string())
        })?;

    Ok(chunk_metadata_count as usize)
}

pub fn create_chunk_metadata(
    chunks: Vec<ChunkReqPayload>,
    dataset_uuid: uuid::Uuid,
) -> Result<(BulkUploadIngestionMessage, Vec<ChunkMetadata>), ServiceError> {
    let mut ingestion_messages = vec![];

    let mut chunk_metadatas = vec![];

    for chunk in chunks {
        let chunk_tag_set = chunk
            .tag_set
            .clone()
            .map(|tags| tags.into_iter().map(Some).collect::<Vec<Option<String>>>());

        let chunk_tracking_id = chunk
            .tracking_id
            .clone()
            .filter(|chunk_tracking| !chunk_tracking.is_empty());

        let timestamp = {
            chunk
                .time_stamp
                .clone()
                .map(|ts| -> Result<NaiveDateTime, ServiceError> {
                    Ok(ts
                        .parse::<DateTimeUtc>()
                        .map_err(|_| {
                            ServiceError::BadRequest("Invalid timestamp format".to_string())
                        })?
                        .0
                        .with_timezone(&chrono::Local)
                        .naive_local())
                })
                .transpose()?
        };

        let chunk_metadata = ChunkMetadata::from_details(
            &chunk.chunk_html.clone(),
            &chunk.link,
            &chunk_tag_set,
            uuid::Uuid::new_v4(),
            chunk.metadata.clone(),
            chunk_tracking_id,
            timestamp,
            chunk.location,
            chunk.image_urls.clone(),
            dataset_uuid,
            chunk.weight.unwrap_or(0.0),
            chunk.num_value,
        );
        chunk_metadatas.push(chunk_metadata.clone());

        let upload_message = UploadIngestionMessage {
            ingest_specific_chunk_metadata: IngestSpecificChunkMetadata {
                id: chunk_metadata.id,
                qdrant_point_id: chunk_metadata.qdrant_point_id,
                dataset_id: dataset_uuid,
            },
            dataset_id: dataset_uuid,
            chunk: chunk.clone(),
            upsert_by_tracking_id: chunk.upsert_by_tracking_id.unwrap_or(false),
        };

        ingestion_messages.push(upload_message);
    }

    Ok((
        BulkUploadIngestionMessage {
            attempt_number: 0,
            dataset_id: dataset_uuid,
            ingestion_messages,
            only_qdrant: None,
        },
        chunk_metadatas,
    ))
}

pub async fn get_pg_point_ids_from_qdrant_point_ids(
    qdrant_point_ids: Vec<uuid::Uuid>,
    pool: web::Data<Pool>,
) -> Result<Vec<(uuid::Uuid, uuid::Uuid)>, ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let chunk_ids: Vec<(uuid::Uuid, uuid::Uuid)> = chunk_metadata_columns::chunk_metadata
        .filter(chunk_metadata_columns::qdrant_point_id.eq_any(qdrant_point_ids))
        .select((
            chunk_metadata_columns::qdrant_point_id,
            chunk_metadata_columns::dataset_id,
        ))
        .load(&mut conn)
        .await
        .map_err(|_| ServiceError::BadRequest("Failed to get chunk ids".to_string()))?;

    Ok(chunk_ids)
}

pub async fn get_chunk_html_from_ids_query(
    chunk_ids: Vec<uuid::Uuid>,
    pool: web::Data<Pool>,
) -> Result<Option<Vec<(uuid::Uuid, String)>>, ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;
    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let chunk_htmls = chunk_metadata_columns::chunk_metadata
        .select((
            chunk_metadata_columns::id,
            chunk_metadata_columns::chunk_html.assume_not_null(),
        ))
        .filter(chunk_metadata_columns::id.eq_any(chunk_ids))
        .load::<(uuid::Uuid, String)>(&mut conn)
        .await
        .map_err(|_| ServiceError::NotFound("Failed to get chunk_htmls".to_string()))?;

    if chunk_htmls.is_empty() {
        return Ok(None);
    }
    Ok(Some(chunk_htmls))
}

pub async fn scroll_chunk_ids_for_dictionary_query(
    pool: web::Data<Pool>,
    dataset_id: uuid::Uuid,
    last_processed: Option<DatasetLastProcessed>,
    limit: i64,
    offset: uuid::Uuid,
) -> Result<Option<Vec<(uuid::Uuid, uuid::Uuid)>>, ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    let mut conn = pool
        .get()
        .await
        .map_err(|_| ServiceError::BadRequest("Could not get database connection".to_string()))?;

    let mut chunk_ids = chunk_metadata_columns::chunk_metadata
        .select((
            chunk_metadata_columns::id,
            chunk_metadata_columns::dataset_id,
        ))
        .filter(chunk_metadata_columns::dataset_id.eq(dataset_id))
        .filter(chunk_metadata_columns::id.gt(offset))
        .into_boxed();

    if let Some(last_processed) = last_processed {
        let last_processed =
            NaiveDateTime::from_timestamp(last_processed.last_processed.unix_timestamp(), 0);

        chunk_ids = chunk_ids.filter(chunk_metadata_columns::created_at.gt(last_processed));
    }

    let chunk_ids = chunk_ids
        .order_by(chunk_metadata_columns::id)
        .limit(limit)
        .load::<(uuid::Uuid, uuid::Uuid)>(&mut conn)
        .await
        .map_err(|_| {
            log::error!("Failed to scroll dataset ids for dictionary");
            ServiceError::InternalServerError(
                "Failed to scroll dataset ids for dictionary".to_string(),
            )
        })?;

    if chunk_ids.is_empty() {
        return Ok(None);
    }
    Ok(Some(chunk_ids))
}

pub async fn scroll_chunks_from_pg(
    pool: web::Data<Pool>,
    dataset_id: uuid::Uuid,
    limit: i64,
    offset: Option<uuid::Uuid>,
) -> Result<(Vec<ChunkMetadata>, Option<uuid::Uuid>), ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;
    use crate::data::schema::chunk_metadata_tags::dsl as chunk_metadata_tags_columns;
    use crate::data::schema::dataset_tags::dsl as dataset_tags_columns;

    let mut conn = pool
        .get()
        .await
        .map_err(|_| ServiceError::BadRequest("Could not get database connection".to_string()))?;

    let chunk_metadata_pairs: Vec<(ChunkMetadataTable, Option<Vec<String>>)> =
        chunk_metadata_columns::chunk_metadata
            .left_join(
                chunk_metadata_tags_columns::chunk_metadata_tags
                    .on(chunk_metadata_tags_columns::chunk_metadata_id
                        .eq(chunk_metadata_columns::id)),
            )
            .left_join(
                dataset_tags_columns::dataset_tags
                    .on(dataset_tags_columns::id.eq(chunk_metadata_tags_columns::tag_id)),
            )
            .filter(chunk_metadata_columns::dataset_id.eq(dataset_id))
            .filter(chunk_metadata_columns::id.gt(offset.unwrap_or(uuid::Uuid::nil())))
            .select((
                ChunkMetadataTable::as_select(),
                sql::<sql_types::Array<sql_types::Text>>(
                    "array_remove(array_agg(dataset_tags.tag), null)",
                )
                .nullable(),
            ))
            .group_by(chunk_metadata_columns::id)
            .order_by(chunk_metadata_columns::id)
            .limit(limit)
            .load::<(ChunkMetadataTable, Option<Vec<String>>)>(&mut conn)
            .await
            .map_err(|_| {
                log::error!("Failed to scroll dataset ids for dictionary");
                ServiceError::InternalServerError(
                    "Failed to scroll dataset ids for dictionary".to_string(),
                )
            })?;

    let chunk_metadatas: Vec<ChunkMetadata> = chunk_metadata_pairs
        .into_iter()
        .map(|(table, tag_set)| {
            ChunkMetadata::from_table_and_tag_set(table, tag_set.unwrap_or(vec![]))
        })
        .collect();

    let offset_id = chunk_metadatas.last().map(|x| x.id);

    Ok((chunk_metadatas, offset_id))
}

pub async fn update_dataset_chunk_count(
    dataset_id: uuid::Uuid,
    amount_to_increase: i32,
    pool: web::Data<Pool>,
) -> Result<(), ServiceError> {
    use crate::data::schema::dataset_usage_counts::dsl as dataset_usage_counts_columns;

    let mut conn = pool
        .get()
        .await
        .map_err(|_| ServiceError::BadRequest("Could not get database connection".to_string()))?;

    let updated_count = diesel::update(
        dataset_usage_counts_columns::dataset_usage_counts
            .filter(dataset_usage_counts_columns::dataset_id.eq(dataset_id))
            .filter(dataset_usage_counts_columns::chunk_count.is_not_null()),
    )
    .set(
        dataset_usage_counts_columns::chunk_count
            .eq(dataset_usage_counts_columns::chunk_count + amount_to_increase),
    )
    .returning(dataset_usage_counts_columns::chunk_count)
    .execute(&mut conn)
    .await;
    match updated_count {
        Ok(_) => Ok(()),
        Err(_) => {
            let new_dataset_usage_count =
                DatasetUsageCount::from_details(dataset_id, amount_to_increase);
            diesel::insert_into(dataset_usage_counts_columns::dataset_usage_counts)
                .values(&new_dataset_usage_count)
                .execute(&mut conn)
                .await
                .map_err(|_| {
                    log::error!("Failed to insert new dataset usage count");
                    ServiceError::InternalServerError(
                        "Failed to insert new dataset usage count".to_string(),
                    )
                })?;
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct DatasetLastProcessed {
    #[serde(with = "clickhouse::serde::uuid")]
    pub dataset_id: uuid::Uuid,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub last_processed: OffsetDateTime,
}

pub async fn get_last_processed_from_clickhouse(
    clickhouse_client: &clickhouse::Client,
    dataset_id: uuid::Uuid,
) -> Result<Option<DatasetLastProcessed>, ServiceError> {
    let query = format!(
        "SELECT dataset_id, min(last_processed) as last_processed FROM dataset_words_last_processed WHERE dataset_id = '{}' GROUP BY dataset_id LIMIT 1",
        dataset_id
    );

    let last_processed = clickhouse_client
        .query(&query)
        .fetch_optional::<DatasetLastProcessed>()
        .await
        .map_err(|_| {
            ServiceError::InternalServerError("Failed to get last processed".to_string())
        })?;

    Ok(last_processed)
}
