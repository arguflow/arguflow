use std::collections::HashSet;

use crate::data::models::{ChunkMetadataTags, DatasetTags};
use crate::errors::ServiceError;
use crate::get_env;
use crate::operators::qdrant_operator::{
    get_qdrant_collection_from_dataset_config, get_qdrant_connection,
    remove_bookmark_from_qdrant_query, update_group_tag_sets_in_qdrant_query,
};
use crate::{
    data::models::{
        ChunkGroup, ChunkGroupAndFileId, ChunkGroupBookmark, ChunkMetadataTable, Dataset,
        DatasetConfiguration, FileGroup, Pool, RedisPool, UnifiedId,
    },
    handlers::group_handler::GroupsBookmarkQueryResult,
    operators::chunk_operator::{delete_chunk_metadata_query, get_chunk_metadatas_from_point_ids},
};
use actix_web::web;
use diesel::prelude::*;
use diesel::upsert::excluded;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, RunQueryDsl};
use qdrant_client::qdrant;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub async fn get_group_from_tracking_id_query(
    tracking_id: String,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<ChunkGroupAndFileId, ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;
    use crate::data::schema::groups_from_files::dsl as groups_from_files_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let (group, file_id): (ChunkGroup, Option<uuid::Uuid>) = chunk_group_columns::chunk_group
        .left_join(
            groups_from_files_columns::groups_from_files
                .on(chunk_group_columns::id.eq(groups_from_files_columns::group_id)),
        )
        .filter(chunk_group_columns::dataset_id.eq(dataset_uuid))
        .filter(chunk_group_columns::tracking_id.eq(&tracking_id))
        .select((
            ChunkGroup::as_select(),
            groups_from_files_columns::file_id.nullable(),
        ))
        .first::<(ChunkGroup, Option<uuid::Uuid>)>(&mut conn)
        .await
        .map_err(|_| {
            ServiceError::NotFound(format!("Group with tracking_id {:} not found", tracking_id))
        })?;

    Ok(ChunkGroupAndFileId::from_group(group, file_id))
}

pub async fn get_group_ids_from_tracking_ids_query(
    tracking_ids: Vec<String>,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Vec<(uuid::Uuid, Option<String>)>, ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let group_id_tracking_ids = chunk_group_columns::chunk_group
        .filter(chunk_group_columns::dataset_id.eq(dataset_uuid))
        .filter(chunk_group_columns::tracking_id.eq_any(tracking_ids))
        .select((chunk_group_columns::id, chunk_group_columns::tracking_id))
        .load::<(uuid::Uuid, Option<String>)>(&mut conn)
        .await
        .map_err(|_err| ServiceError::BadRequest("Groups not found".to_string()))?;

    Ok(group_id_tracking_ids)
}

pub async fn update_group_by_tracking_id_query(
    tracking_id: String,
    dataset_uuid: uuid::Uuid,
    new_name: Option<String>,
    new_description: Option<String>,
    pool: web::Data<Pool>,
) -> Result<(), ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

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
    .map_err(|err| ServiceError::BadRequest(format!("Error updating group {:?}", err)))?;

    Ok(())
}

pub async fn create_groups_query(
    new_groups: Vec<ChunkGroup>,
    upsert_by_tracking_id: bool,
    pool: web::Data<Pool>,
) -> Result<Vec<ChunkGroup>, ServiceError> {
    if new_groups.is_empty() {
        return Ok(vec![]);
    }

    use crate::data::schema::chunk_group::dsl as chunk_group_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let mut inserted_groups = if upsert_by_tracking_id {
        diesel::insert_into(chunk_group_columns::chunk_group)
            .values(&new_groups)
            .on_conflict((
                chunk_group_columns::dataset_id,
                chunk_group_columns::tracking_id,
            ))
            .do_update()
            .set((
                chunk_group_columns::name.eq(excluded(chunk_group_columns::name)),
                chunk_group_columns::description.eq(excluded(chunk_group_columns::description)),
                chunk_group_columns::metadata.eq(excluded(chunk_group_columns::metadata)),
                chunk_group_columns::tag_set.eq(excluded(chunk_group_columns::tag_set)),
            ))
            .returning(ChunkGroup::as_select())
            .get_results::<ChunkGroup>(&mut conn)
            .await
            .map_err(|e| {
                log::error!("Error upserting groups {:?}", e);
                ServiceError::BadRequest("Error upserting groups".to_string())
            })?
    } else {
        diesel::insert_into(chunk_group_columns::chunk_group)
            .values(&new_groups)
            .on_conflict_do_nothing()
            .get_results::<ChunkGroup>(&mut conn)
            .await
            .map_err(|e| {
                log::error!("Error inserting groups {:?}", e);
                ServiceError::BadRequest("Error inserting groups".to_string())
            })?
    };
    if inserted_groups.is_empty() {
        inserted_groups = new_groups;
    }

    Ok(inserted_groups)
}

pub async fn get_groups_for_dataset_query(
    page: u64,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<(Vec<ChunkGroupAndFileId>, i32), ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;
    use crate::data::schema::dataset_group_counts::dsl as dataset_group_count_columns;
    use crate::data::schema::groups_from_files::dsl as groups_from_files_columns;

    let page = if page == 0 { 1 } else { page };
    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let group_count_result = dataset_group_count_columns::dataset_group_counts
        .filter(dataset_group_count_columns::dataset_id.eq(dataset_uuid))
        .select(dataset_group_count_columns::group_count)
        .first::<i32>(&mut conn)
        .await;

    let group_count: i32 = match group_count_result {
        Ok(count) => count,
        Err(_) => return Ok((vec![], 0)),
    };

    let groups = chunk_group_columns::chunk_group
        .filter(chunk_group_columns::dataset_id.eq(dataset_uuid))
        .order_by(chunk_group_columns::id.desc())
        .offset(((page - 1) * 10).try_into().unwrap_or(0))
        .limit(10)
        .load::<ChunkGroup>(&mut conn)
        .await
        .map_err(|_err| ServiceError::BadRequest("Error getting groups for dataset".to_string()))?;

    let file_ids = groups_from_files_columns::groups_from_files
        .filter(
            groups_from_files_columns::group_id
                .eq_any(groups.iter().map(|x| x.id).collect::<Vec<uuid::Uuid>>()),
        )
        .select((
            groups_from_files_columns::group_id,
            groups_from_files_columns::file_id,
        ))
        .load::<(uuid::Uuid, uuid::Uuid)>(&mut conn)
        .await
        .map_err(|_err| ServiceError::BadRequest("Error getting file ids".to_string()))?;

    let group_and_files = groups
        .into_iter()
        .map(|group| {
            let file_id = file_ids
                .iter()
                .find(|(group_id, _)| group.id == *group_id)
                .map(|(_, file_id)| *file_id);

            ChunkGroupAndFileId {
                id: group.id,
                dataset_id: group.dataset_id,
                name: group.name,
                description: group.description,
                tracking_id: group.tracking_id,
                tag_set: group.tag_set,
                metadata: group.metadata,
                file_id,
                created_at: group.created_at,
                updated_at: group.updated_at,
            }
        })
        .collect();

    Ok((group_and_files, group_count))
}

pub async fn get_group_by_id_query(
    group_id: uuid::Uuid,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<ChunkGroupAndFileId, ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;
    use crate::data::schema::groups_from_files::dsl as groups_from_files_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let (group, file_id): (ChunkGroup, Option<uuid::Uuid>) = chunk_group_columns::chunk_group
        .left_join(
            groups_from_files_columns::groups_from_files
                .on(chunk_group_columns::id.eq(groups_from_files_columns::group_id)),
        )
        .filter(chunk_group_columns::dataset_id.eq(dataset_uuid))
        .filter(chunk_group_columns::id.eq(group_id))
        .select((
            ChunkGroup::as_select(),
            groups_from_files_columns::file_id.nullable(),
        ))
        .first::<(ChunkGroup, Option<uuid::Uuid>)>(&mut conn)
        .await
        .map_err(|_| ServiceError::NotFound(format!("Group with id not found {:?}", group_id)))?;

    Ok(ChunkGroupAndFileId::from_group(group, file_id))
}

pub async fn delete_group_by_id_query(
    group_id: uuid::Uuid,
    dataset: Dataset,
    deleted_at: chrono::NaiveDateTime,
    delete_chunks: Option<bool>,
    pool: web::Data<Pool>,
    dataset_config: DatasetConfiguration,
) -> Result<(), ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;
    use crate::data::schema::groups_from_files::dsl as groups_from_files_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let delete_chunks = delete_chunks.unwrap_or(false);
    let chunks = chunk_group_bookmarks_columns::chunk_group_bookmarks
        .inner_join(chunk_metadata_columns::chunk_metadata)
        .filter(chunk_group_bookmarks_columns::group_id.eq(group_id))
        .select(ChunkMetadataTable::as_select())
        .load::<ChunkMetadataTable>(&mut conn)
        .await
        .map_err(|_err| ServiceError::BadRequest("Error getting chunks".to_string()))?;

    let transaction_result = conn
        .transaction::<_, diesel::result::Error, _>(|conn| {
            async move {
                diesel::delete(
                    groups_from_files_columns::groups_from_files
                        .filter(groups_from_files_columns::group_id.eq(group_id))
                        .filter(groups_from_files_columns::created_at.le(deleted_at)),
                )
                .execute(conn)
                .await?;

                diesel::delete(
                    chunk_group_bookmarks_columns::chunk_group_bookmarks
                        .filter(chunk_group_bookmarks_columns::group_id.eq(group_id))
                        .filter(chunk_group_bookmarks_columns::created_at.le(deleted_at)),
                )
                .execute(conn)
                .await?;

                diesel::delete(
                    chunk_group_columns::chunk_group
                        .filter(chunk_group_columns::id.eq(group_id))
                        .filter(chunk_group_columns::dataset_id.eq(dataset.id))
                        .filter(chunk_group_columns::created_at.le(deleted_at)),
                )
                .execute(conn)
                .await?;

                Ok(())
            }
            .scope_boxed()
        })
        .await;

    if delete_chunks {
        let chunk_ids = chunks.iter().map(|chunk| chunk.id).collect();
        delete_chunk_metadata_query(
            chunk_ids,
            deleted_at,
            dataset.clone(),
            pool.clone(),
            dataset_config.clone(),
        )
        .await?;
    } else {
        let remove_chunks_from_groups_futures = chunks.iter().map(|chunk| {
            remove_bookmark_from_qdrant_query(
                chunk.qdrant_point_id,
                group_id,
                dataset_config.clone(),
            )
        });

        futures::future::join_all(remove_chunks_from_groups_futures).await;
    }

    match transaction_result {
        Ok(_) => Ok(()),
        Err(_) => Err(ServiceError::BadRequest("Error deleting group".to_string())),
    }
}

pub async fn delete_group_by_file_id_query(
    file_id: uuid::Uuid,
    dataset: Dataset,
    deleted_at: chrono::NaiveDateTime,
    delete_chunks: Option<bool>,
    pool: web::Data<Pool>,
    dataset_config: DatasetConfiguration,
) -> Result<(), ServiceError> {
    use crate::data::schema::groups_from_files::dsl as groups_from_files_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let group_id: uuid::Uuid = groups_from_files_columns::groups_from_files
        .filter(groups_from_files_columns::file_id.eq(file_id))
        .select(groups_from_files_columns::group_id)
        .first::<uuid::Uuid>(&mut conn)
        .await
        .map_err(|_err| {
            ServiceError::BadRequest("Error getting group id for file_id".to_string())
        })?;

    delete_group_by_id_query(
        group_id,
        dataset,
        deleted_at,
        delete_chunks,
        pool,
        dataset_config,
    )
    .await
}

pub async fn update_chunk_group_query(
    group: ChunkGroup,
    pool: web::Data<Pool>,
) -> Result<ChunkGroup, ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let updated_group: ChunkGroup = diesel::update(
        chunk_group_columns::chunk_group
            .filter(chunk_group_columns::id.eq(group.id))
            .filter(chunk_group_columns::dataset_id.eq(group.dataset_id)),
    )
    .set((
        chunk_group_columns::name.eq(group.name),
        chunk_group_columns::description.eq(group.description),
        chunk_group_columns::tracking_id.eq(group.tracking_id),
        chunk_group_columns::metadata.eq(group.metadata),
        chunk_group_columns::tag_set.eq(group.tag_set),
    ))
    .get_result(&mut conn)
    .await
    .map_err(|err| ServiceError::BadRequest(format!("Error updating group {:?}", err)))?;

    Ok(updated_group)
}

pub async fn create_chunk_bookmark_query(
    pool: web::Data<Pool>,
    bookmark: ChunkGroupBookmark,
) -> Result<uuid::Uuid, ServiceError> {
    use crate::data::schema::chunk_group_bookmarks::dsl::*;
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

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
        .first::<uuid::Uuid>(&mut conn)
        .await
        .map_err(|_err| {
            log::error!("Error getting qdrant_point_id {:}", _err);
            ServiceError::BadRequest("Error getting qdrant_point_id".to_string())
        })?;

    Ok(qdrant_point_id)
}

pub async fn get_bookmarks_for_group_query(
    group_id: UnifiedId,
    page: u64,
    limit: Option<u64>,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<GroupsBookmarkQueryResult, ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;
    let mut conn = pool.clone().get().await.unwrap();

    let group_uuid = match group_id {
        UnifiedId::TrackingId(id) => chunk_group_columns::chunk_group
            .filter(chunk_group_columns::tracking_id.eq(&id))
            .filter(chunk_group_columns::dataset_id.eq(&dataset_uuid))
            .select(chunk_group_columns::id)
            .first::<uuid::Uuid>(&mut conn)
            .await
            .map_err(|_| {
                ServiceError::NotFound(format!("Group with tracking id not found {:}", id))
            })?,
        UnifiedId::TrieveUuid(id) => id,
    };

    let chunks_future = get_chunk_point_ids_in_chunk_group_query(
        group_uuid,
        page,
        limit.unwrap_or(10),
        dataset_uuid,
        pool.clone(),
    );

    let chunk_group_future = get_group_by_id_query(group_uuid, dataset_uuid, pool.clone());

    let chunk_count_future =
        get_chunk_metadata_count_in_chunk_group_query(group_uuid, pool.clone());

    let (chunk_metadata_point_ids, chunk_count_result, chunk_group_result) =
        futures::future::join3(chunks_future, chunk_count_future, chunk_group_future).await;

    let chunk_metadata_point_ids = chunk_metadata_point_ids?;
    let chunk_metadatas =
        get_chunk_metadatas_from_point_ids(chunk_metadata_point_ids, pool.clone()).await?;
    let chunk_metadata_string_tag_sets = chunk_metadatas
        .iter()
        .map(|chunk_metadata| chunk_metadata.metadata().into())
        .collect();

    let chunk_count = chunk_count_result?;
    let chunk_group = chunk_group_result?;

    Ok(GroupsBookmarkQueryResult {
        chunks: chunk_metadata_string_tag_sets,
        group: chunk_group,
        total_pages: (chunk_count as f64 / 10.0).ceil() as u64,
    })
}
#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct GroupsForChunk {
    pub chunk_uuid: uuid::Uuid,
    pub slim_groups: Vec<ChunkGroupAndFileId>,
}

pub async fn get_groups_for_bookmark_query(
    chunk_ids: Vec<uuid::Uuid>,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Vec<GroupsForChunk>, ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;
    use crate::data::schema::groups_from_files::dsl as groups_from_files_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let groups: Vec<(ChunkGroup, uuid::Uuid, Option<uuid::Uuid>)> =
        chunk_group_columns::chunk_group
            .left_join(
                chunk_group_bookmarks_columns::chunk_group_bookmarks
                    .on(chunk_group_columns::id.eq(chunk_group_bookmarks_columns::group_id)),
            )
            .left_join(
                groups_from_files_columns::groups_from_files
                    .on(chunk_group_columns::id.eq(groups_from_files_columns::group_id)),
            )
            .filter(chunk_group_columns::dataset_id.eq(dataset_uuid))
            .filter(chunk_group_bookmarks_columns::chunk_metadata_id.eq_any(chunk_ids))
            .select((
                (ChunkGroup::as_select()),
                chunk_group_bookmarks_columns::chunk_metadata_id.nullable(),
                groups_from_files_columns::file_id.nullable(),
            ))
            .load::<(ChunkGroup, Option<uuid::Uuid>, Option<uuid::Uuid>)>(&mut conn)
            .await
            .map_err(|_err| {
                ServiceError::BadRequest("Error getting groups for chunks".to_string())
            })?
            .into_iter()
            .map(|(chunk_group, chunk_id, file_id)| match chunk_id {
                Some(chunk_id) => (chunk_group, chunk_id, file_id),
                None => (chunk_group, uuid::Uuid::default(), file_id),
            })
            .collect();

    let bookmark_groups: Vec<GroupsForChunk> =
        groups.into_iter().fold(Vec::new(), |mut acc, item| {
            if item.1 == uuid::Uuid::default() {
                return acc;
            }

            //check if chunk in output already
            if let Some(output_item) = acc.iter_mut().find(|x| x.chunk_uuid == item.1) {
                //if it is, add group to it
                output_item
                    .slim_groups
                    .push(ChunkGroupAndFileId::from_group(item.0, item.2));
            } else {
                //if not make new output item
                acc.push(GroupsForChunk {
                    chunk_uuid: item.1,
                    slim_groups: vec![ChunkGroupAndFileId::from_group(item.0, item.2)],
                });
            }
            acc
        });

    Ok(bookmark_groups)
}

pub async fn delete_chunk_from_group_query(
    chunk_id: uuid::Uuid,
    group_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<uuid::Uuid, ServiceError> {
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

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
        .first::<uuid::Uuid>(&mut conn)
        .await
        .map_err(|_err| {
            log::error!("Error getting qdrant_point_id {:}", _err);
            ServiceError::BadRequest("Error getting qdrant_point_id".to_string())
        })?;

    Ok(qdrant_point_id)
}

pub async fn create_group_from_file_query(
    group_id: uuid::Uuid,
    file_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<(), ServiceError> {
    use crate::data::schema::groups_from_files::dsl as groups_from_files_columns;

    let file_group = FileGroup::from_details(file_id, group_id);

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

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

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let qdrant_point_ids: Vec<uuid::Uuid> = match group_ids.get(0) {
        Some(UnifiedId::TrieveUuid(_)) => chunk_group_columns::chunk_group
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
            .load::<uuid::Uuid>(&mut conn)
            .await
            .map_err(|_| ServiceError::BadRequest("Failed to load metadata".to_string()))?,
        Some(UnifiedId::TrackingId(_)) => chunk_group_columns::chunk_group
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
            .load::<uuid::Uuid>(&mut conn)
            .await
            .map_err(|_| ServiceError::BadRequest("Failed to load metadata".to_string()))?,
        _ => {
            vec![]
        }
    };

    Ok(qdrant_point_ids)
}

pub async fn get_groups_from_group_ids_query(
    group_ids: Vec<uuid::Uuid>,
    pool: web::Data<Pool>,
) -> Result<Vec<ChunkGroupAndFileId>, ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;
    use crate::data::schema::groups_from_files::dsl as groups_from_files_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let chunk_groups_and_files: Vec<(ChunkGroup, Option<uuid::Uuid>)> =
        chunk_group_columns::chunk_group
            .left_join(
                groups_from_files_columns::groups_from_files
                    .on(chunk_group_columns::id.eq(groups_from_files_columns::group_id)),
            )
            .filter(chunk_group_columns::id.eq_any(&group_ids))
            .select((
                ChunkGroup::as_select(),
                groups_from_files_columns::file_id.nullable(),
            ))
            .load::<(ChunkGroup, Option<uuid::Uuid>)>(&mut conn)
            .await
            .map_err(|_| ServiceError::BadRequest("Failed to fetch group".to_string()))?;

    Ok(chunk_groups_and_files
        .iter()
        .map(|(group, file_id)| ChunkGroupAndFileId::from_group(group.clone(), *file_id))
        .collect())
}

pub async fn check_group_ids_exist_query(
    group_ids: Vec<uuid::Uuid>,
    dataset_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Vec<uuid::Uuid>, ServiceError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let existing_group_ids: Vec<uuid::Uuid> = chunk_group_columns::chunk_group
        .filter(chunk_group_columns::dataset_id.eq(dataset_id))
        .filter(chunk_group_columns::id.eq_any(&group_ids))
        .select(chunk_group_columns::id)
        .load::<uuid::Uuid>(&mut conn)
        .await
        .map_err(|e| {
            log::error!("Error getting group ids for exist check {:?}", e);

            ServiceError::BadRequest("Failed to load group ids for exist check".to_string())
        })?;

    Ok(existing_group_ids)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GroupUpdateMessage {
    pub dataset_id: uuid::Uuid,
    pub group: ChunkGroup,
    pub prev_group: ChunkGroup,
    pub attempt_number: usize,
}

pub async fn soft_update_grouped_chunks_query(
    new_group: ChunkGroup,
    prev_group: ChunkGroup,
    redis_pool: web::Data<RedisPool>,
    dataset_id: uuid::Uuid,
) -> Result<(), ServiceError> {
    let mut redis_conn = redis_pool
        .get()
        .await
        .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

    let message = GroupUpdateMessage {
        dataset_id,
        group: new_group,
        prev_group,
        attempt_number: 0,
    };

    let serialized_message =
        serde_json::to_string(&message).map_err(|err| ServiceError::BadRequest(err.to_string()))?;

    redis::cmd("lpush")
        .arg("group_update_queue")
        .arg(&serialized_message)
        .query_async::<_, ()>(&mut *redis_conn)
        .await
        .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

    Ok(())
}

pub async fn update_grouped_chunks_query(
    prev_group: ChunkGroup,
    new_group: ChunkGroup,
    pool: web::Data<Pool>,
    dataset_config: DatasetConfiguration,
) -> Result<(), ServiceError> {
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;
    use crate::data::schema::chunk_metadata_tags::dsl as chunk_metadata_tags_columns;
    use crate::data::schema::dataset_tags::dsl as dataset_tags_columns;

    let mut offset = uuid::Uuid::nil();

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let qdrant_collection = get_qdrant_collection_from_dataset_config(&dataset_config);

    let group_id = new_group.id;
    let dataset_id = new_group.dataset_id;
    let prev_group_tag_set = prev_group
        .tag_set
        .unwrap_or_default()
        .iter()
        .filter_map(|tag_option| {
            if tag_option.clone().is_some_and(|tag| !tag.is_empty()) {
                tag_option.clone()
            } else {
                None
            }
        })
        .collect::<HashSet<String>>();
    let new_group_tag_set = new_group
        .tag_set
        .unwrap_or_default()
        .iter()
        .filter_map(|tag_option| {
            if tag_option.clone().is_some_and(|tag| !tag.is_empty()) {
                tag_option.clone()
            } else {
                None
            }
        })
        .collect::<HashSet<String>>();
    let tags_removed = prev_group_tag_set
        .iter()
        .filter(|tag| !new_group_tag_set.contains(&tag.to_string()))
        .cloned()
        .collect::<HashSet<String>>();

    let dataset_tags_to_attempt_inserting = new_group_tag_set
        .iter()
        .map(|tag| DatasetTags::from_details(dataset_id, tag.clone()))
        .collect::<Vec<DatasetTags>>();

    let mut dataset_tags: Vec<DatasetTags> =
        diesel::insert_into(dataset_tags_columns::dataset_tags)
            .values(dataset_tags_to_attempt_inserting.clone())
            .on_conflict_do_nothing()
            .get_results::<DatasetTags>(&mut conn)
            .await
            .map_err(|_| ServiceError::BadRequest("Failed to insert dataset tags".to_string()))?;
    if dataset_tags.len() < dataset_tags_to_attempt_inserting.len() {
        let existing_dataset_tags = dataset_tags_columns::dataset_tags
            .filter(
                dataset_tags_columns::dataset_id.eq(dataset_id).and(
                    dataset_tags_columns::tag.eq_any(
                        dataset_tags_to_attempt_inserting
                            .iter()
                            .map(|x| x.tag.clone())
                            .collect::<Vec<String>>(),
                    ),
                ),
            )
            .load::<DatasetTags>(&mut conn)
            .await
            .map_err(|_| {
                ServiceError::BadRequest("Failed to load existing dataset tags".to_string())
            })?;

        dataset_tags.extend(existing_dataset_tags);
    }

    loop {
        let chunk_point_ids: Vec<(uuid::Uuid, uuid::Uuid)> =
            chunk_group_bookmarks_columns::chunk_group_bookmarks
                .inner_join(chunk_metadata_columns::chunk_metadata.on(
                    chunk_metadata_columns::id.eq(chunk_group_bookmarks_columns::chunk_metadata_id),
                ))
                .filter(chunk_group_bookmarks_columns::group_id.eq(group_id))
                .filter(chunk_metadata_columns::id.gt(offset))
                .limit(120)
                .order_by(chunk_metadata_columns::id)
                .select((
                    chunk_metadata_columns::id,
                    chunk_metadata_columns::qdrant_point_id,
                ))
                .load::<(uuid::Uuid, uuid::Uuid)>(&mut conn)
                .await
                .map_err(|_| ServiceError::BadRequest("Failed to load chunks".to_string()))?;
        let (chunk_ids, point_ids): (Vec<uuid::Uuid>, Vec<uuid::Uuid>) =
            chunk_point_ids.iter().cloned().unzip();

        if chunk_ids.is_empty() {
            break;
        }

        offset = match chunk_ids.last() {
            Some(chunk_metadata_id) => *chunk_metadata_id,
            _ => {
                return Err(ServiceError::BadRequest(
                    "Failed to get last chunk id".to_string(),
                ))
            }
        };

        let chunk_metadata_tags: Vec<ChunkMetadataTags> =
            chunk_metadata_tags_columns::chunk_metadata_tags
                .filter(chunk_metadata_tags_columns::chunk_metadata_id.eq_any(&chunk_ids))
                .filter(
                    chunk_metadata_tags_columns::tag_id.eq_any(
                        dataset_tags
                            .iter()
                            .map(|x| x.id)
                            .collect::<Vec<uuid::Uuid>>(),
                    ),
                )
                .load::<ChunkMetadataTags>(&mut conn)
                .await
                .map_err(|_| {
                    ServiceError::BadRequest("Failed to load chunk metadata tags".to_string())
                })?;

        let mut chunk_metadata_tags_attempt_removing = vec![];
        let mut chunk_metadata_tags_attempt_inserting = vec![];
        for chunk_id in chunk_ids.iter() {
            for tag in tags_removed.iter() {
                if let Some(tag_id) = dataset_tags.iter().find(|x| x.tag == *tag).map(|x| x.id) {
                    if let Some(chunk_metadata_tag_id) = chunk_metadata_tags
                        .iter()
                        .find(|x| x.chunk_metadata_id == *chunk_id && x.tag_id == tag_id)
                        .map(|x| x.id)
                    {
                        chunk_metadata_tags_attempt_removing.push(chunk_metadata_tag_id);
                    }
                } else {
                    log::error!(
                        "Tag not found in dataset_tags during grupdate tags_removed step {:?}",
                        tag
                    );
                }
            }

            for tag in new_group_tag_set.iter() {
                if let Some(tag_id) = dataset_tags.iter().find(|x| x.tag == *tag).map(|x| x.id) {
                    chunk_metadata_tags_attempt_inserting
                        .push((ChunkMetadataTags::from_details(*chunk_id, tag_id),));
                } else {
                    log::error!(
                        "Tag not found in dataset_tags during grupdate new_group_tag_set step {:?}",
                        tag
                    );
                }
            }
        }

        diesel::delete(
            chunk_metadata_tags_columns::chunk_metadata_tags.filter(
                chunk_metadata_tags_columns::id.eq_any(&chunk_metadata_tags_attempt_removing),
            ),
        )
        .execute(&mut conn)
        .await
        .map_err(|_| {
            ServiceError::BadRequest("Failed to delete chunk metadata tags".to_string())
        })?;

        diesel::insert_into(chunk_metadata_tags_columns::chunk_metadata_tags)
            .values(&chunk_metadata_tags_attempt_inserting)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await
            .map_err(|_| {
                ServiceError::BadRequest("Failed to insert chunk metadata tags".to_string())
            })?;

        update_group_tag_sets_in_qdrant_query(
            qdrant_collection.clone(),
            prev_group_tag_set.iter().map(|x| x.to_string()).collect(),
            new_group_tag_set.iter().map(|x| x.to_string()).collect(),
            point_ids,
        )
        .await?;
    }

    Ok(())
}

pub async fn get_chunk_point_ids_in_chunk_group_query(
    group_id: uuid::Uuid,
    page: u64,
    limit: u64,
    dataset_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Vec<uuid::Uuid>, ServiceError> {
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let chunk_ids = chunk_group_bookmarks_columns::chunk_group_bookmarks
        .inner_join(chunk_metadata_columns::chunk_metadata)
        .filter(chunk_group_bookmarks_columns::group_id.eq(group_id))
        .filter(chunk_metadata_columns::dataset_id.eq(dataset_id))
        .select(chunk_metadata_columns::qdrant_point_id)
        .offset(((page - 1) * limit).try_into().unwrap_or(0))
        .order(chunk_metadata_columns::id)
        .limit(limit.try_into().unwrap_or(10))
        .load::<uuid::Uuid>(&mut conn)
        .await
        .map_err(|_| ServiceError::BadRequest("Failed to load chunks for group".to_string()))?;

    Ok(chunk_ids)
}

pub async fn get_chunk_metadata_count_in_chunk_group_query(
    group_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<i64, ServiceError> {
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let count = chunk_group_bookmarks_columns::chunk_group_bookmarks
        .filter(chunk_group_bookmarks_columns::group_id.eq(group_id))
        .count()
        .get_result::<i64>(&mut conn)
        .await
        .map_err(|_| {
            ServiceError::BadRequest("Failed to get count of chunks for group".to_string())
        })?;

    Ok(count)
}

pub async fn get_group_size_query(
    group_id: uuid::Uuid,
    dataset_id: uuid::Uuid,
    dataset_config: DatasetConfiguration,
) -> Result<u64, ServiceError> {
    let qdrant_connection = get_qdrant_connection(
        Some(get_env!("QDRANT_URL", "QDRANT_URL should be set")),
        Some(get_env!("QDRANT_API_KEY", "QDRANT_API_KEY should be set")),
    )
    .await?;
    let qdrant_collection = get_qdrant_collection_from_dataset_config(&dataset_config);

    let mut filter = qdrant::Filter::default();

    filter.must.push(qdrant::Condition::matches(
        "dataset_id",
        dataset_id.to_string(),
    ));
    filter.must.push(qdrant::Condition::matches(
        "group_ids",
        group_id.to_string(),
    ));

    let request = qdrant::CountPointsBuilder::new(qdrant_collection)
        .filter(filter)
        .build();

    let count = qdrant_connection
        .count(request)
        .await
        .map_err(|err| {
            log::error!("Error fetching point count {:?}", err);
            ServiceError::BadRequest("Error fetching point count".to_string())
        })?
        .result
        .map(|x| x.count);

    count.ok_or(ServiceError::BadRequest(
        "Error getting group size".to_string(),
    ))
}
