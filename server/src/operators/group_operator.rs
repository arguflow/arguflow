use crate::{
    data::models::{ChunkGroup, Pool},
    errors::DefaultError,
};
use crate::{
    data::models::{
        ChunkGroupAndFileWithCount, ChunkGroupBookmark, ChunkMetadataWithCount,
        ChunkMetadataWithFileData, FileGroup, FullTextSearchResult, SlimGroup,
    },
    diesel::{Connection, ExpressionMethods, QueryDsl, RunQueryDsl},
    errors::ServiceError,
    operators::search_operator::get_metadata_query,
};
use actix_web::web;
use diesel::{
    dsl::sql,
    sql_types::{Int8, Text},
    BoolExpressionMethods, JoinOnDsl, NullableExpressionMethods,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub fn create_group_query(
    new_group: ChunkGroup,
    pool: web::Data<Pool>,
) -> Result<(), DefaultError> {
    use crate::data::schema::chunk_group::dsl::*;

    let mut conn = pool.get().unwrap();

    diesel::insert_into(chunk_group)
        .values(&new_group)
        .execute(&mut conn)
        .map_err(|err| {
            log::error!("Error creating group {:}", err);
            DefaultError {
                message: "Error creating group",
            }
        })?;

    Ok(())
}

pub fn create_group_and_add_bookmarks_query(
    new_group: ChunkGroup,
    bookmark_ids: Vec<uuid::Uuid>,
    created_file_id: uuid::Uuid,
    given_dataset_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<ChunkGroup, DefaultError> {
    use crate::data::schema::chunk_group::dsl::*;

    let mut conn = pool.get().unwrap();

    chunk_group
        .filter(dataset_id.eq(given_dataset_id))
        .filter(id.eq(new_group.id))
        .first::<ChunkGroup>(&mut conn)
        .map_err(|_err| DefaultError {
            message: "Group not found, likely incorrect dataset_id",
        })?;

    let transaction_result = conn.transaction::<_, diesel::result::Error, _>(|conn| {
        diesel::insert_into(chunk_group)
            .values(&new_group)
            .execute(conn)?;

        use crate::data::schema::chunk_group_bookmarks::dsl::*;

        diesel::insert_into(chunk_group_bookmarks)
            .values(
                bookmark_ids
                    .iter()
                    .map(|bookmark| ChunkGroupBookmark::from_details(new_group.id, *bookmark))
                    .collect::<Vec<ChunkGroupBookmark>>(),
            )
            .execute(conn)?;

        use crate::data::schema::groups_from_files::dsl::*;

        diesel::insert_into(groups_from_files)
            .values(&FileGroup::from_details(created_file_id, new_group.id))
            .execute(conn)?;

        Ok(())
    });

    match transaction_result {
        Ok(_) => (),
        Err(err) => {
            log::error!("Error creating group {:}", err);
            return Err(DefaultError {
                message: "Error creating group",
            });
        }
    }
    Ok(new_group)
}

pub fn get_groups_for_specific_dataset_query(
    page: u64,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Vec<ChunkGroupAndFileWithCount>, DefaultError> {
    use crate::data::schema::chunk_group::dsl::*;
    use crate::data::schema::dataset_group_counts::dsl as dataset_group_count_columns;
    use crate::data::schema::groups_from_files::dsl as groups_from_files_columns;

    let page = if page == 0 { 1 } else { page };
    let mut conn = pool.get().unwrap();
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
        ))
        .order_by(updated_at.desc())
        .filter(dataset_id.eq(dataset_uuid))
        .into_boxed();

    let groups = groups
        .limit(10)
        .offset(((page - 1) * 10).try_into().unwrap_or(0))
        .load::<ChunkGroupAndFileWithCount>(&mut conn)
        .map_err(|_err| DefaultError {
            message: "Error getting groups",
        })?;

    Ok(groups)
}

pub fn get_group_by_id_query(
    group_id: uuid::Uuid,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<ChunkGroup, DefaultError> {
    use crate::data::schema::chunk_group::dsl::*;

    let mut conn = pool.get().unwrap();

    let group = chunk_group
        .filter(dataset_id.eq(dataset_uuid))
        .filter(id.eq(group_id))
        .first::<ChunkGroup>(&mut conn)
        .map_err(|_err| DefaultError {
            message: "Group not found",
        })?;

    Ok(group)
}

pub fn delete_group_by_id_query(
    group_id: uuid::Uuid,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<(), DefaultError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;
    use crate::data::schema::events::dsl as events_columns;
    use crate::data::schema::groups_from_files::dsl as groups_from_files_columns;

    let mut conn = pool.get().unwrap();

    let transaction_result = conn.transaction::<_, diesel::result::Error, _>(|conn| {
        diesel::delete(
            events_columns::events
                .filter(events_columns::event_type.eq("file_uploaded"))
                .filter(
                    sql::<Text>(&format!("events.event_data->>'{}'", "group_id"))
                        .eq(group_id.to_string()),
                ),
        )
        .execute(conn)?;

        diesel::delete(
            groups_from_files_columns::groups_from_files
                .filter(groups_from_files_columns::group_id.eq(group_id)),
        )
        .execute(conn)?;

        diesel::delete(
            chunk_group_bookmarks_columns::chunk_group_bookmarks
                .filter(chunk_group_bookmarks_columns::group_id.eq(group_id)),
        )
        .execute(conn)?;

        diesel::delete(
            chunk_group_columns::chunk_group
                .filter(chunk_group_columns::id.eq(group_id))
                .filter(chunk_group_columns::dataset_id.eq(dataset_uuid)),
        )
        .execute(conn)?;

        Ok(())
    });

    match transaction_result {
        Ok(_) => Ok(()),
        Err(_) => Err(DefaultError {
            message: "Error deleting group",
        }),
    }
}

pub fn update_chunk_group_query(
    group: ChunkGroup,
    new_name: Option<String>,
    new_description: Option<String>,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<(), DefaultError> {
    use crate::data::schema::chunk_group::dsl::*;

    let mut conn = pool.get().unwrap();

    diesel::update(
        chunk_group
            .filter(id.eq(group.id))
            .filter(dataset_id.eq(dataset_uuid)),
    )
    .set((
        name.eq(new_name.unwrap_or(group.name)),
        description.eq(new_description.unwrap_or(group.description)),
    ))
    .execute(&mut conn)
    .map_err(|_err| DefaultError {
        message: "Error updating group",
    })?;

    Ok(())
}

pub fn create_chunk_bookmark_query(
    pool: web::Data<Pool>,
    bookmark: ChunkGroupBookmark,
) -> Result<(), DefaultError> {
    use crate::data::schema::chunk_group_bookmarks::dsl::*;

    let mut conn = pool.get().unwrap();

    diesel::insert_into(chunk_group_bookmarks)
        .values(&bookmark)
        .execute(&mut conn)
        .map_err(|_err| {
            log::error!("Error creating bookmark {:}", _err);
            DefaultError {
                message: "Error creating bookmark",
            }
        })?;

    Ok(())
}
pub struct GroupsBookmarkQueryResult {
    pub metadata: Vec<ChunkMetadataWithFileData>,
    pub group: ChunkGroup,
    pub total_pages: i64,
}
pub fn get_bookmarks_for_group_query(
    group: uuid::Uuid,
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

    let mut conn = pool.get().unwrap();

    let bookmark_metadata: Vec<(ChunkMetadataWithCount, ChunkGroup)> =
        chunk_metadata_columns::chunk_metadata
            .left_join(chunk_group_bookmarks_columns::chunk_group_bookmarks.on(
                chunk_group_bookmarks_columns::chunk_metadata_id.eq(chunk_metadata_columns::id),
            ))
            .left_join(
                chunk_group_columns::chunk_group
                    .on(chunk_group_columns::id.eq(chunk_group_bookmarks_columns::group_id)),
            )
            .filter(
                chunk_group_bookmarks_columns::group_id
                    .eq(group)
                    .and(chunk_group_columns::dataset_id.eq(dataset_uuid))
                    .and(chunk_metadata_columns::dataset_id.eq(dataset_uuid)),
            )
            .select((
                (
                    chunk_metadata_columns::id,
                    chunk_metadata_columns::content,
                    chunk_metadata_columns::link,
                    chunk_metadata_columns::author_id,
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
                (
                    chunk_group_columns::id.assume_not_null(),
                    chunk_group_columns::name.assume_not_null(),
                    chunk_group_columns::description.assume_not_null(),
                    chunk_group_columns::created_at.assume_not_null(),
                    chunk_group_columns::updated_at.assume_not_null(),
                    chunk_group_columns::dataset_id.assume_not_null(),
                ),
            ))
            .limit(limit)
            .offset(((page - 1) * limit as u64).try_into().unwrap_or(0))
            .load::<(ChunkMetadataWithCount, ChunkGroup)>(&mut conn)
            .map_err(|_err| ServiceError::BadRequest("Error getting bookmarks".to_string()))?;

    let chunk_group = if let Some(bookmark) = bookmark_metadata.first() {
        bookmark.1.clone()
    } else {
        chunk_group_columns::chunk_group
            .filter(chunk_group_columns::id.eq(group))
            .filter(chunk_group_columns::dataset_id.eq(dataset_uuid))
            .first::<ChunkGroup>(&mut conn)
            .map_err(|_err| ServiceError::BadRequest("Error getting group".to_string()))?
    };

    let converted_chunks: Vec<FullTextSearchResult> = bookmark_metadata
        .iter()
        .map(|(chunk, _chunk_group)| {
            <ChunkMetadataWithCount as Into<FullTextSearchResult>>::into(chunk.clone())
        })
        .collect::<Vec<FullTextSearchResult>>();

    let chunk_metadata_with_file_id = get_metadata_query(converted_chunks, conn)
        .map_err(|_| ServiceError::BadRequest("Failed to load metadata".to_string()))?;

    let total_pages = match bookmark_metadata.first() {
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

pub fn get_groups_for_bookmark_query(
    chunk_ids: Vec<uuid::Uuid>,
    dataset_uuid: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Vec<BookmarkGroupResult>, DefaultError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;

    let mut conn = pool.get().unwrap();

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
        .limit(1000)
        .load::<(uuid::Uuid, String, uuid::Uuid, Option<uuid::Uuid>)>(&mut conn)
        .map_err(|_err| DefaultError {
            message: "Error getting bookmarks",
        })?
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
pub fn delete_bookmark_query(
    bookmark_id: uuid::Uuid,
    group_id: uuid::Uuid,
    dataset_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<(), DefaultError> {
    use crate::data::schema::chunk_group::dsl as chunk_group_columns;
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;

    let mut conn = pool.get().unwrap();

    chunk_group_columns::chunk_group
        .filter(chunk_group_columns::id.eq(group_id))
        .filter(chunk_group_columns::dataset_id.eq(dataset_id))
        .first::<ChunkGroup>(&mut conn)
        .map_err(|_err| DefaultError {
            message: "Group not found, likely incorrect dataset_id",
        })?;

    diesel::delete(
        chunk_group_bookmarks_columns::chunk_group_bookmarks
            .filter(chunk_group_bookmarks_columns::chunk_metadata_id.eq(bookmark_id))
            .filter(chunk_group_bookmarks_columns::group_id.eq(group_id)),
    )
    .execute(&mut conn)
    .map_err(|_err| {
        log::error!("Error deleting bookmark {:}", _err);
        DefaultError {
            message: "Error deleting bookmark",
        }
    })?;

    Ok(())
}
