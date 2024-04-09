use crate::{
    data::models::{
        ChunkGroup, ChunkMetadata, Dataset, FileGroup, Pool, ServerDatasetConfiguration, UnifiedId,
    },
    operators::chunk_operator::delete_chunk_metadata_query,
};
use crate::{
    data::models::{
        ChunkGroupAndFileWithCount, ChunkGroupBookmark, ChunkMetadataWithCount,
        ChunkMetadataWithFileData, FullTextSearchResult, SlimGroup,
    },
    errors::ServiceError,
    operators::search_operator::get_metadata_query,
};
use actix_web::web;
use diesel::prelude::*;
use diesel::{
    dsl::sql,
    sql_types::{Int8, Text},
};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, RunQueryDsl};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[tracing::instrument(skip(pool))]
pub async fn get_group_from_tracking_id_query(
    tracking_id: String,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<ChunkGroup, ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;

    let mut conn = pool.get().await.unwrap();

    let group = chunk_group_columns::chunk_group
        .filter(chunk_group_columns::dataset_id.eq(dataset_uuid))
        .filter(chunk_group_columns::tracking_id.eq(tracking_id))
        .first::<ChunkGroup>(&mut conn)
        .await
        .map_err(|_err| ServiceError::BadRequest("Group not found".to_string()))?;

    Ok(group)
}

#[tracing::instrument(skip(pool))]
pub async fn get_groups_from_tracking_ids_query(
    tracking_ids: Vec<String>,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Vec<uuid::Uuid>, ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;

    let mut conn = pool.get().await.unwrap();

    let group_ids = chunk_group_columns::chunk_group
        .filter(chunk_group_columns::dataset_id.eq(dataset_uuid))
        .filter(chunk_group_columns::tracking_id.eq_any(tracking_ids))
        .select(chunk_group_columns::id)
        .load::<uuid::Uuid>(&mut conn)
        .await
        .map_err(|_err| ServiceError::BadRequest("Groups not found".to_string()))?;

    Ok(group_ids)
}

#[tracing::instrument(skip(pool))]
pub async fn update_group_by_tracking_id_query(
    tracking_id: String,
    dataset_uuid: uuid::Uuid,
    new_name: Option<String>,
    new_description: Option<String>,
    pool: web::Data<Pool>,
) -> Result<(), ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;

    let mut conn = pool.get().await.unwrap();

    diesel::update(
        chunk_group_columns::chunk_group
            .filter(chunk_group_columns::dataset_id.eq(dataset_uuid))
            .filter(chunk_group_columns::tracking_id.eq(tracking_id)),
    )
    .set((
        chunk_group_columns::name.eq(new_name.unwrap_or("".to_string())),
        chunk_group_columns::description.eq(new_description.unwrap_or("".to_string())),
    ))
    .execute(&mut conn)
    .await
    .map_err(|_err| ServiceError::BadRequest("Error updating group".to_string()))?;

    Ok(())
}

#[tracing::instrument(skip(pool))]
pub async fn create_group_query(
    new_group: ChunkGroup,
    pool: web::Data<Pool>,
) -> Result<ChunkGroup, ServiceError> {
    use crate::data::schema::chunk_group::dsl::*;

    let mut conn = pool.get().await.unwrap();

    diesel::insert_into(chunk_group)
        .values(&new_group)
        .execute(&mut conn)
        .await
        .map_err(|err| {
            log::error!("Error creating group {:}", err);
            ServiceError::BadRequest(format!("Error creating group {:?}", err))
        })?;

    Ok(new_group)
}

#[tracing::instrument(skip(pool))]
pub async fn get_groups_for_specific_dataset_query(
    page: u64,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Vec<ChunkGroupAndFileWithCount>, ServiceError> {
    use crate::data::schema::chunk_group::dsl::*;
    use crate::data::schema::dataset_group_counts::dsl as dataset_group_count_columns;
    use crate::data::schema::groups_from_files::dsl as groups_from_files_columns;

    let page = if page == 0 { 1 } else { page };
    let mut conn = pool.get().await.unwrap();
    let groups = chunk_group
        .left_outer_join(
            groups_from_files_columns::groups_from_files
                .on(id.eq(groups_from_files_columns::group_id)),
        )
        .left_outer_join(
            dataset_group_count_columns::dataset_group_counts
                .on(dataset_id.eq(dataset_group_count_columns::dataset_id.assume_not_null())),
        )
        .select((
            id,
            dataset_id,
            name,
            description,
            created_at,
            updated_at,
            groups_from_files_columns::file_id.nullable(),
            dataset_group_count_columns::group_count.nullable(),
            tracking_id,
        ))
        .order_by(updated_at.desc())
        .filter(dataset_id.eq(dataset_uuid))
        .into_boxed();

    let groups = groups
        .limit(10)
        .offset(((page - 1) * 10).try_into().unwrap_or(0))
        .load::<ChunkGroupAndFileWithCount>(&mut conn)
        .await
        .map_err(|_err| ServiceError::BadRequest("Error getting groups".to_string()))?;

    Ok(groups)
}

#[tracing::instrument(skip(pool))]
pub async fn get_group_by_id_query(
    group_id: uuid::Uuid,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<ChunkGroup, ServiceError> {
    use crate::data::schema::chunk_group::dsl::*;

    let mut conn = pool.get().await.unwrap();

    let group = chunk_group
        .filter(dataset_id.eq(dataset_uuid))
        .filter(id.eq(group_id))
        .first::<ChunkGroup>(&mut conn)
        .await
        .map_err(|_err| ServiceError::BadRequest("Group not found".to_string()))?;

    Ok(group)
}

#[tracing::instrument(skip(pool))]
pub async fn delete_group_by_id_query(
    group_id: uuid::Uuid,
    dataset: Dataset,
    delete_chunks: Option<bool>,
    pool: web::Data<Pool>,
    config: ServerDatasetConfiguration,
) -> Result<(), ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;
    use crate::data::schema::events::dsl as events_columns;
    use crate::data::schema::groups_from_files::dsl as groups_from_files_columns;
    use crate::data::schema::{
        chunk_collisions::dsl as chunk_collisions_columns, chunk_files::dsl as chunk_files_columns,
        chunk_metadata::dsl as chunk_metadata_columns,
    };

    let mut conn = pool.get().await.unwrap();

    let mut chunk_ids = vec![];
    let mut collisions = vec![];
    let delete_chunks = delete_chunks.unwrap_or(false);

    if delete_chunks {
        let chunks = chunk_group_bookmarks_columns::chunk_group_bookmarks
            .inner_join(chunk_metadata_columns::chunk_metadata)
            .filter(chunk_group_bookmarks_columns::group_id.eq(group_id))
            .select(ChunkMetadata::as_select())
            .load::<ChunkMetadata>(&mut conn)
            .await
            .map_err(|_err| ServiceError::BadRequest("Error getting chunks".to_string()))?;

        chunk_ids = chunks
            .iter()
            .filter_map(|chunk| {
                if chunk.qdrant_point_id.is_some() {
                    Some(chunk.id)
                } else {
                    None
                }
            })
            .collect();

        collisions = chunks
            .iter()
            .filter_map(|chunk| {
                if chunk.qdrant_point_id.is_none() {
                    Some(chunk.id)
                } else {
                    None
                }
            })
            .collect();
    }

    let transaction_result = conn
        .transaction::<_, diesel::result::Error, _>(|conn| {
            async move {
                diesel::delete(
                    events_columns::events
                        .filter(events_columns::event_type.eq("file_uploaded"))
                        .filter(
                            sql::<Text>(&format!("events.event_data->>'{}'", "group_id"))
                                .eq(group_id.to_string()),
                        ),
                )
                .execute(conn)
                .await?;

                if delete_chunks {
                    diesel::delete(
                        chunk_files_columns::chunk_files
                            .filter(chunk_files_columns::chunk_id.eq_any(collisions.clone())),
                    )
                    .execute(conn)
                    .await?;

                    diesel::delete(
                        chunk_collisions_columns::chunk_collisions
                            .filter(chunk_collisions_columns::chunk_id.eq_any(collisions.clone())),
                    )
                    .execute(conn)
                    .await?;
                    // there cannot be collisions for a collision, just delete the chunk_metadata without issue
                    diesel::delete(
                        chunk_metadata_columns::chunk_metadata
                            .filter(chunk_metadata_columns::id.eq_any(collisions.clone()))
                            .filter(chunk_metadata_columns::dataset_id.eq(dataset.id)),
                    )
                    .execute(conn)
                    .await?;
                }

                diesel::delete(
                    groups_from_files_columns::groups_from_files
                        .filter(groups_from_files_columns::group_id.eq(group_id)),
                )
                .execute(conn)
                .await?;

                diesel::delete(
                    chunk_group_bookmarks_columns::chunk_group_bookmarks
                        .filter(chunk_group_bookmarks_columns::group_id.eq(group_id)),
                )
                .execute(conn)
                .await?;

                diesel::delete(
                    chunk_group_columns::chunk_group
                        .filter(chunk_group_columns::id.eq(group_id))
                        .filter(chunk_group_columns::dataset_id.eq(dataset.id)),
                )
                .execute(conn)
                .await?;

                Ok(())
            }
            .scope_boxed()
        })
        .await;

    if delete_chunks {
        for chunk_id in chunk_ids {
            delete_chunk_metadata_query(chunk_id, dataset.clone(), pool.clone(), config.clone())
                .await?;
        }
    }

    match transaction_result {
        Ok(_) => Ok(()),
        Err(_) => Err(ServiceError::BadRequest("Error deleting group".to_string())),
    }
}

#[tracing::instrument(skip(pool))]
pub async fn update_chunk_group_query(
    group: ChunkGroup,
    new_name: Option<String>,
    new_description: Option<String>,
    new_metadata: Option<serde_json::Value>,
    new_tag_set: Option<String>,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<(), ServiceError> {
    use crate::data::schema::chunk_group::dsl::*;

    let mut conn = pool.get().await.unwrap();

    diesel::update(
        chunk_group
            .filter(id.eq(group.id))
            .filter(dataset_id.eq(dataset_uuid)),
    )
    .set((
        name.eq(new_name.unwrap_or(group.name)),
        description.eq(new_description.unwrap_or(group.description)),
        metadata.eq(new_metadata.unwrap_or(group.metadata.unwrap_or_default())),
        tag_set.eq(new_tag_set.unwrap_or(group.tag_set.unwrap_or_default())),
    ))
    .execute(&mut conn)
    .await
    .map_err(|_err| ServiceError::BadRequest("Error updating group".to_string()))?;

    //TODO: update bookmarks within the group

    Ok(())
}

#[tracing::instrument(skip(pool))]
pub async fn create_chunk_bookmark_query(
    pool: web::Data<Pool>,
    bookmark: ChunkGroupBookmark,
) -> Result<Option<uuid::Uuid>, ServiceError> {
    use crate::data::schema::chunk_group_bookmarks::dsl::*;
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    let mut conn = pool.get().await.unwrap();

    diesel::insert_into(chunk_group_bookmarks)
        .values(&bookmark)
        .on_conflict((group_id, chunk_metadata_id))
        .do_nothing()
        .execute(&mut conn)
        .await
        .map_err(|_err| {
            log::error!("Error creating bookmark {:}", _err);
            ServiceError::BadRequest("Error creating bookmark".to_string())
        })?;

    let qdrant_point_id = chunk_metadata_columns::chunk_metadata
        .filter(chunk_metadata_columns::id.eq(bookmark.chunk_metadata_id))
        .select(chunk_metadata_columns::qdrant_point_id)
        .first::<Option<uuid::Uuid>>(&mut conn)
        .await
        .map_err(|_err| {
            log::error!("Error getting qdrant_point_id {:}", _err);
            ServiceError::BadRequest("Error getting qdrant_point_id".to_string())
        })?;

    Ok(qdrant_point_id)
}
pub struct GroupsBookmarkQueryResult {
    pub metadata: Vec<ChunkMetadataWithFileData>,
    pub group: ChunkGroup,
    pub total_pages: i64,
}
#[tracing::instrument(skip(pool))]
pub async fn get_bookmarks_for_group_query(
    group_id: UnifiedId,
    page: u64,
    limit: Option<i64>,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<GroupsBookmarkQueryResult, ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;
    let page = if page == 0 { 1 } else { page };
    let limit = limit.unwrap_or(10);

    let mut conn = pool.get().await.unwrap();

    let group_uuid = match group_id {
        UnifiedId::TrackingId(id) => chunk_group_columns::chunk_group
            .filter(chunk_group_columns::tracking_id.eq(id))
            .select(chunk_group_columns::id)
            .first::<uuid::Uuid>(&mut conn)
            .await
            .map_err(|_| {
                ServiceError::BadRequest("Failed to find matching tracking id".to_string())
            })?,
        UnifiedId::TrieveUuid(id) => id,
    };

    let bookmark_metadata: Vec<(ChunkMetadataWithCount, ChunkGroup)> =
        chunk_metadata_columns::chunk_metadata
            .inner_join(chunk_group_bookmarks_columns::chunk_group_bookmarks.on(
                chunk_group_bookmarks_columns::chunk_metadata_id.eq(chunk_metadata_columns::id),
            ))
            .inner_join(
                chunk_group_columns::chunk_group
                    .on(chunk_group_columns::id.eq(chunk_group_bookmarks_columns::group_id)),
            )
            .filter(
                chunk_group_bookmarks_columns::group_id
                    .eq(group_uuid)
                    .and(chunk_group_columns::dataset_id.eq(dataset_uuid))
                    .and(chunk_metadata_columns::dataset_id.eq(dataset_uuid)),
            )
            .select((
                (
                    chunk_metadata_columns::id,
                    chunk_metadata_columns::content,
                    chunk_metadata_columns::link,
                    chunk_metadata_columns::qdrant_point_id,
                    chunk_metadata_columns::created_at,
                    chunk_metadata_columns::updated_at,
                    chunk_metadata_columns::tag_set,
                    chunk_metadata_columns::chunk_html,
                    chunk_metadata_columns::metadata,
                    chunk_metadata_columns::tracking_id,
                    chunk_metadata_columns::time_stamp,
                    chunk_metadata_columns::weight,
                    sql::<Int8>("count(*) OVER() AS full_count"),
                ),
                ChunkGroup::as_select(),
            ))
            .limit(limit)
            .offset(((page - 1) * limit as u64).try_into().unwrap_or(0))
            .load::<(ChunkMetadataWithCount, ChunkGroup)>(&mut conn)
            .await
            .map_err(|_err| ServiceError::BadRequest("Error getting bookmarks".to_string()))?;

    let chunk_group = if let Some(bookmark) = bookmark_metadata.get(0) {
        bookmark.1.clone()
    } else {
        chunk_group_columns::chunk_group
            .filter(chunk_group_columns::dataset_id.eq(dataset_uuid))
            .filter(chunk_group_columns::id.eq(group_uuid))
            .first::<ChunkGroup>(&mut conn)
            .await
            .map_err(|err| {
                sentry::capture_message(
                    &format!("Error getting group {:?}", err),
                    sentry::Level::Error,
                );
                log::error!("Error getting group {:?}", err);
                ServiceError::BadRequest("Error getting group".to_string())
            })?
    };

    let converted_chunks: Vec<FullTextSearchResult> = bookmark_metadata
        .iter()
        .map(|(chunk, _chunk_group)| {
            <ChunkMetadataWithCount as Into<FullTextSearchResult>>::into(chunk.clone())
        })
        .collect::<Vec<FullTextSearchResult>>();

    let chunk_metadata_with_file_id = get_metadata_query(converted_chunks, pool)
        .await
        .map_err(|_| ServiceError::BadRequest("Failed to load metadata".to_string()))?;

    let total_pages = match bookmark_metadata.get(0) {
        Some(metadata) => (metadata.0.count as f64 / 10.0).ceil() as i64,
        None => 0,
    };

    Ok(GroupsBookmarkQueryResult {
        metadata: chunk_metadata_with_file_id,
        group: chunk_group,
        total_pages,
    })
}
#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct BookmarkGroupResult {
    pub chunk_uuid: uuid::Uuid,
    pub slim_groups: Vec<SlimGroup>,
}

#[tracing::instrument(skip(pool))]
pub async fn get_groups_for_bookmark_query(
    chunk_ids: Vec<uuid::Uuid>,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Vec<BookmarkGroupResult>, ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;

    let mut conn = pool.get().await.unwrap();

    let groups: Vec<(SlimGroup, uuid::Uuid)> = chunk_group_columns::chunk_group
        .left_join(
            chunk_group_bookmarks_columns::chunk_group_bookmarks
                .on(chunk_group_columns::id.eq(chunk_group_bookmarks_columns::group_id)),
        )
        .filter(chunk_group_columns::dataset_id.eq(dataset_uuid))
        .filter(chunk_group_bookmarks_columns::chunk_metadata_id.eq_any(chunk_ids))
        .select((
            chunk_group_columns::id,
            chunk_group_columns::name,
            chunk_group_columns::dataset_id,
            chunk_group_bookmarks_columns::chunk_metadata_id.nullable(),
        ))
        .load::<(uuid::Uuid, String, uuid::Uuid, Option<uuid::Uuid>)>(&mut conn)
        .await
        .map_err(|_err| ServiceError::BadRequest("Error getting bookmarks".to_string()))?
        .into_iter()
        .map(|(id, name, dataset_id, chunk_id)| match chunk_id {
            Some(chunk_id) => (
                SlimGroup {
                    id,
                    name,
                    dataset_id,
                    of_current_dataset: dataset_id == dataset_uuid,
                },
                chunk_id,
            ),
            None => (
                SlimGroup {
                    id,
                    name,
                    dataset_id,
                    of_current_dataset: dataset_id == dataset_uuid,
                },
                uuid::Uuid::default(),
            ),
        })
        .collect();

    let bookmark_groups: Vec<BookmarkGroupResult> =
        groups.into_iter().fold(Vec::new(), |mut acc, item| {
            if item.1 == uuid::Uuid::default() {
                return acc;
            }

            //check if chunk in output already
            if let Some(output_item) = acc.iter_mut().find(|x| x.chunk_uuid == item.1) {
                //if it is, add group to it
                output_item.slim_groups.push(item.0);
            } else {
                //if not make new output item
                acc.push(BookmarkGroupResult {
                    chunk_uuid: item.1,
                    slim_groups: vec![item.0],
                });
            }
            acc
        });

    Ok(bookmark_groups)
}

#[tracing::instrument(skip(pool))]
pub async fn delete_chunk_from_group_query(
    chunk_id: uuid::Uuid,
    group_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Option<uuid::Uuid>, ServiceError> {
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    let mut conn = pool.get().await.unwrap();

    diesel::delete(
        chunk_group_bookmarks_columns::chunk_group_bookmarks
            .filter(chunk_group_bookmarks_columns::chunk_metadata_id.eq(chunk_id))
            .filter(chunk_group_bookmarks_columns::group_id.eq(group_id)),
    )
    .execute(&mut conn)
    .await
    .map_err(|_err| {
        log::error!("Error deleting bookmark {:}", _err);
        ServiceError::BadRequest("Error deleting bookmark".to_string())
    })?;

    let qdrant_point_id = chunk_metadata_columns::chunk_metadata
        .filter(chunk_metadata_columns::id.eq(chunk_id))
        .select(chunk_metadata_columns::qdrant_point_id)
        .first::<Option<uuid::Uuid>>(&mut conn)
        .await
        .map_err(|_err| {
            log::error!("Error getting qdrant_point_id {:}", _err);
            ServiceError::BadRequest("Error getting qdrant_point_id".to_string())
        })?;

    Ok(qdrant_point_id)
}

#[tracing::instrument(skip(pool))]
pub async fn create_group_from_file_query(
    group_id: uuid::Uuid,
    file_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<(), ServiceError> {
    use crate::data::schema::groups_from_files::dsl as groups_from_files_columns;

    let file_group = FileGroup::from_details(file_id, group_id);

    let mut conn = pool.get().await.unwrap();

    diesel::insert_into(groups_from_files_columns::groups_from_files)
        .values(&file_group)
        .execute(&mut conn)
        .await
        .map_err(|_err| {
            log::error!("Error creating group from file {:}", _err);
            ServiceError::BadRequest("Error creating group from file".to_string())
        })?;

    Ok(())
}

pub async fn get_point_ids_from_unified_group_ids(
    group_ids: Vec<UnifiedId>,
    dataset_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Vec<uuid::Uuid>, ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    let mut conn = pool.get().await.unwrap();

    let qdrant_point_ids: Vec<uuid::Uuid> = match group_ids[0] {
        UnifiedId::TrieveUuid(_) => chunk_group_columns::chunk_group
            .inner_join(chunk_group_bookmarks_columns::chunk_group_bookmarks)
            .inner_join(chunk_metadata_columns::chunk_metadata.on(
                chunk_group_bookmarks_columns::chunk_metadata_id.eq(chunk_metadata_columns::id),
            ))
            .filter(
                chunk_group_columns::id.eq_any(
                    &group_ids
                        .iter()
                        .map(|x| x.as_uuid().expect("Failed to convert to Uuid"))
                        .collect::<Vec<uuid::Uuid>>(),
                ),
            )
            .filter(chunk_metadata_columns::dataset_id.eq(dataset_id))
            .select(chunk_metadata_columns::qdrant_point_id)
            .load::<Option<uuid::Uuid>>(&mut conn)
            .await
            .map_err(|_| ServiceError::BadRequest("Failed to load metadata".to_string()))?
            .into_iter()
            .flatten()
            .collect(),
        UnifiedId::TrackingId(_) => chunk_group_columns::chunk_group
            .inner_join(chunk_group_bookmarks_columns::chunk_group_bookmarks)
            .inner_join(chunk_metadata_columns::chunk_metadata.on(
                chunk_group_bookmarks_columns::chunk_metadata_id.eq(chunk_metadata_columns::id),
            ))
            .filter(
                chunk_group_columns::tracking_id.eq_any(
                    &group_ids
                        .iter()
                        .map(|x| x.as_tracking_id().expect("Failed to convert to String"))
                        .collect::<Vec<String>>(),
                ),
            )
            .filter(chunk_metadata_columns::dataset_id.eq(dataset_id))
            .select(chunk_metadata_columns::qdrant_point_id)
            .load::<Option<uuid::Uuid>>(&mut conn)
            .await
            .map_err(|_| ServiceError::BadRequest("Failed to load metadata".to_string()))?
            .into_iter()
            .flatten()
            .collect(),
    };

    Ok(qdrant_point_ids)
}

pub async fn get_group_tracking_ids_from_group_ids_query(
    group_ids: Vec<uuid::Uuid>,
    pool: web::Data<Pool>,
) -> Result<Vec<Option<String>>, ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;

    let mut conn = pool.get().await.unwrap();

    chunk_group_columns::chunk_group
        .filter(chunk_group_columns::id.eq_any(&group_ids))
        .select(chunk_group_columns::tracking_id)
        .load::<Option<String>>(&mut conn)
        .await
        .map_err(|_| ServiceError::BadRequest("Failed to fetch group_tracking_id".to_string()))
}

pub async fn check_group_ids_exist_query(
    group_ids: Vec<uuid::Uuid>,
    dataset_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Vec<uuid::Uuid>, ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;

    let mut conn = pool.get().await.unwrap();

    let existing_group_ids: Vec<uuid::Uuid> = chunk_group_columns::chunk_group
        .filter(chunk_group_columns::dataset_id.eq(dataset_id))
        .filter(chunk_group_columns::id.eq_any(&group_ids))
        .select(chunk_group_columns::id)
        .load::<uuid::Uuid>(&mut conn)
        .await
        .map_err(|e| {
            log::error!("Error getting group ids for exist check {:?}", e);
            sentry::capture_message(
                &format!("Error getting group ids for exist check {:?}", e),
                sentry::Level::Error,
            );

            ServiceError::BadRequest("Failed to load group ids for exist check".to_string())
        })?;

    Ok(existing_group_ids)
}
