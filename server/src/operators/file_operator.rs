use super::event_operator::create_event_query;
use super::group_operator::{create_group_from_file_query, create_group_query};
use super::parse_operator::{coarse_doc_chunker, convert_html_to_text};
use crate::data::models::FileDTO;
use crate::data::models::RedisPool;
use crate::data::models::{
    ChunkMetadata, Dataset, DatasetAndOrgWithSubAndPlan, EventType, ServerDatasetConfiguration,
};
use crate::handlers::auth_handler::AdminOnly;
use crate::handlers::chunk_handler::{ChunkData, CreateSingleChunkData, SingleQueuedChunkResponse};
use crate::operators::chunk_operator::delete_chunk_metadata_query;
use crate::{data::models::ChunkGroup, handlers::chunk_handler::ReturnQueuedChunk};
use crate::{data::models::Event, get_env};
use crate::{
    data::models::{File, Pool},
    errors::ServiceError,
    handlers::{
        auth_handler::LoggedUser,
        chunk_handler::{create_chunk, CreateChunkData},
        file_handler::UploadFileResult,
    },
};
use actix_web::{body::MessageBody, web};
use diesel::dsl::sql;
use diesel::prelude::*;
use diesel::sql_types::BigInt;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, RunQueryDsl};
use s3::{creds::Credentials, Bucket, Region};

#[tracing::instrument]
pub fn get_aws_bucket() -> Result<Bucket, ServiceError> {
    let aws_region_name = std::env::var("AWS_REGION").unwrap_or("".to_string());
    let s3_endpoint = get_env!("S3_ENDPOINT", "S3_ENDPOINT should be set").into();
    let s3_bucket_name = get_env!("S3_BUCKET", "S3_BUCKET should be set");

    let aws_region = Region::Custom {
        region: aws_region_name,
        endpoint: s3_endpoint,
    };

    let aws_credentials = if let Ok(creds) = Credentials::from_instance_metadata() {
        creds
    } else {
        let s3_access_key = get_env!("S3_ACCESS_KEY", "S3_ACCESS_KEY should be set").into();
        let s3_secret_key = get_env!("S3_SECRET_KEY", "S3_SECRET_KEY should be set").into();
        Credentials {
            access_key: Some(s3_access_key),
            secret_key: Some(s3_secret_key),
            security_token: None,
            session_token: None,
            expiration: None,
        }
    };

    let aws_bucket = Bucket::new(s3_bucket_name, aws_region, aws_credentials)
        .map_err(|e| {
            sentry::capture_message(
                &format!("Could not create or get bucket {:?}", e),
                sentry::Level::Error,
            );
            log::error!("Could not create or get bucket {:?}", e);
            ServiceError::BadRequest("Could not create or get bucket".to_string())
        })?
        .with_path_style();

    Ok(aws_bucket)
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip(pool))]
pub async fn create_file_query(
    file_id: uuid::Uuid,
    file_name: &str,
    file_size: i64,
    tag_set: Option<String>,
    metadata: Option<serde_json::Value>,
    link: Option<String>,
    time_stamp: Option<String>,
    dataset_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<File, ServiceError> {
    use crate::data::schema::files::dsl as files_columns;

    let mut conn = pool
        .get()
        .await
        .map_err(|_| ServiceError::BadRequest("Could not get database connection".to_string()))?;

    let new_file = File::from_details(
        Some(file_id),
        file_name,
        file_size,
        tag_set,
        metadata,
        link,
        time_stamp,
        dataset_id,
    );

    let created_file: File = diesel::insert_into(files_columns::files)
        .values(&new_file)
        .get_result(&mut conn)
        .await
        .map_err(|_| ServiceError::BadRequest("Could not create file, try again".to_string()))?;

    Ok(created_file)
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip(pool, redis_pool))]
pub async fn convert_doc_to_html_query(
    file_name: String,
    file_data: Vec<u8>,
    tag_set: Option<String>,
    description: Option<String>,
    link: Option<String>,
    metadata: Option<serde_json::Value>,
    create_chunks: Option<bool>,
    time_stamp: Option<String>,
    user: LoggedUser,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
    pool: web::Data<Pool>,
    redis_pool: web::Data<RedisPool>,
) -> Result<UploadFileResult, ServiceError> {
    let file_id = uuid::Uuid::new_v4();
    let file_id_query_clone = file_id;
    let file_name1 = file_name.clone();
    let file_data1 = file_data.clone();
    let tag_set1 = tag_set.clone();
    let dataset_org_plan_sub1 = dataset_org_plan_sub.clone();

    tokio::spawn(async move {
        let tika_url = std::env::var("TIKA_URL")
            .expect("TIKA_URL must be set")
            .to_string();

        let tika_client = reqwest::Client::new();
        let tika_response = tika_client
            .put(&format!("{}/tika", tika_url))
            .header("Accept", "text/html")
            .body(file_data.clone())
            .send()
            .await
            .map_err(|err| {
                log::error!("Could not send file to tika {:?}", err);
                ServiceError::BadRequest("Could not send file to tika".to_string())
            })?;

        let tike_html_converted_file_bytes = tika_response
            .bytes()
            .await
            .map_err(|err| {
                log::error!("Could not get tika response bytes {:?}", err);
                ServiceError::BadRequest("Could not get tika response bytes".to_string())
            })?
            .to_vec();
        let html_content = String::from_utf8_lossy(&tike_html_converted_file_bytes).to_string();

        // get file metadata from tika
        let tika_metadata_response = tika_client
            .put(&format!("{}/meta", tika_url))
            .header("Accept", "application/json")
            .body(file_data.clone())
            .send()
            .await
            .map_err(|err| {
                log::error!("Could not send file to tika {:?}", err);
                ServiceError::BadRequest("Could not send file to tika".to_string())
            })?;

        let mut tika_metadata_response_json: serde_json::Value =
            tika_metadata_response.json().await.map_err(|err| {
                log::error!("Could not get tika metadata response json {:?}", err);
                ServiceError::BadRequest("Could not get tika metadata response json".to_string())
            })?;

        if let Some(metadata) = metadata {
            match metadata.as_object() {
                Some(metadata) => {
                    for (key, value) in metadata {
                        tika_metadata_response_json[key] = value.clone();
                    }
                }
                _ => {
                    log::error!("Could not convert metadata to object {:?}", metadata);
                }
            }
        }

        let file_size_mb = (file_data.len() as f64 / 1024.0 / 1024.0).round() as i64;

        let created_file = create_file_query(
            file_id_query_clone,
            &file_name,
            file_size_mb,
            tag_set.clone(),
            Some(tika_metadata_response_json.clone()),
            link.clone(),
            time_stamp.clone(),
            dataset_org_plan_sub1.dataset.id,
            pool.clone(),
        )
        .await?;

        let bucket = get_aws_bucket()?;
        bucket
            .put_object(created_file.id.to_string(), file_data.as_slice())
            .await
            .map_err(|e| {
                log::error!("Could not upload file to S3 {:?}", e);
                ServiceError::BadRequest("Could not upload file to S3".to_string())
            })?;

        if create_chunks.is_some_and(|create_chunks_bool| !create_chunks_bool) {
            return Ok::<(), ServiceError>(());
        }

        let resp = create_chunks_with_handler(
            tag_set,
            file_name,
            created_file.id,
            description,
            Some(tika_metadata_response_json.clone()),
            time_stamp,
            link.clone(),
            user,
            html_content,
            dataset_org_plan_sub1,
            pool,
            redis_pool,
        )
        .await;

        if resp.is_err() {
            log::error!("Create chunks with handler failed {:?}", resp);
        }

        Ok(())
    });

    Ok(UploadFileResult {
        file_metadata: File::from_details(
            Some(file_id),
            &file_name1,
            file_data1.len().try_into().unwrap(),
            tag_set1,
            None,
            None,
            None,
            dataset_org_plan_sub.dataset.id,
        ),
    })
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip(pool, redis_pool))]
pub async fn create_chunks_with_handler(
    tag_set: Option<String>,
    file_name: String,
    created_file_id: uuid::Uuid,
    description: Option<String>,
    metadata: Option<serde_json::Value>,
    time_stamp: Option<String>,
    link: Option<String>,
    user: LoggedUser,
    html_content: String,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
    pool: web::Data<Pool>,
    redis_pool: web::Data<RedisPool>,
) -> Result<(), ServiceError> {
    let file_text = convert_html_to_text(&html_content);
    let chunk_htmls = coarse_doc_chunker(file_text);

    let mut chunk_ids: Vec<uuid::Uuid> = [].to_vec();

    let split_tag_set: Option<Vec<String>> =
        tag_set.map(|tag_set| tag_set.split(',').map(|x| x.to_string()).collect());

    let name = format!("Group for file {}", file_name);
    let converted_description = convert_html_to_text(&description.unwrap_or("".to_string()));

    let chunk_group = ChunkGroup::from_details(
        name.clone(),
        converted_description,
        dataset_org_plan_sub.dataset.id,
        None,
        None,
        None,
    );

    let chunk_group = create_group_query(chunk_group, pool.clone())
        .await
        .map_err(|e| {
            log::error!("Could not create group {:?}", e);
            ServiceError::BadRequest("Could not create group".to_string())
        })?;

    let group_id = chunk_group.id;

    create_group_from_file_query(group_id, created_file_id, pool.clone())
        .await
        .map_err(|e| {
            log::error!("Could not create group from file {:?}", e);
            e
        })?;

    for chunk_html in chunk_htmls {
        let create_chunk_data = ChunkData {
            chunk_html: Some(chunk_html.clone()),
            link: link.clone(),
            tag_set: split_tag_set.clone(),
            file_id: Some(created_file_id),
            metadata: metadata.clone(),
            group_ids: Some(vec![group_id]),
            group_tracking_ids: None,
            tracking_id: None,
            upsert_by_tracking_id: None,
            time_stamp: time_stamp.clone(),
            chunk_vector: None,
            weight: None,
            split_avg: None,
        };
        let web_json_create_chunk_data = web::Json(CreateChunkData::Single(CreateSingleChunkData(
            create_chunk_data,
        )));

        match create_chunk(
            web_json_create_chunk_data,
            pool.clone(),
            AdminOnly(user.clone()),
            dataset_org_plan_sub.clone(),
            redis_pool.clone(),
        )
        .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    let queued_chunk: ReturnQueuedChunk = serde_json::from_slice(
                        response.into_body().try_into_bytes().unwrap().as_ref(),
                    )
                    .map_err(|_err| {
                        ServiceError::BadRequest(
                            "Error creating chunk metadata's for file".to_string(),
                        )
                    })?;
                    match queued_chunk {
                        ReturnQueuedChunk::Single(SingleQueuedChunkResponse {
                            chunk_metadata,
                            pos_in_queue: _,
                        }) => chunk_ids.push(chunk_metadata.id),
                        _ => unreachable!("Only uploaded 1 chunk but multiple chunks returned"),
                    }
                }
            }
            Err(error) => {
                log::error!("Error creating chunk: {:?}", error.to_string());
            }
        }
    }

    create_event_query(
        Event::from_details(
            dataset_org_plan_sub.dataset.id,
            EventType::FileUploaded {
                group_id,
                file_name: name,
            },
        ),
        pool,
    )
    .await
    .map_err(|_| ServiceError::BadRequest("Thread error creating notification".to_string()))?;

    Ok(())
}

#[tracing::instrument(skip(pool))]
pub async fn get_file_query(
    file_uuid: uuid::Uuid,
    dataset_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<FileDTO, actix_web::Error> {
    use crate::data::schema::files::dsl as files_columns;

    let mut conn = pool
        .get()
        .await
        .map_err(|_| ServiceError::BadRequest("Could not get database connection".to_string()))?;

    let file_metadata: File = files_columns::files
        .filter(files_columns::id.eq(file_uuid))
        .filter(files_columns::dataset_id.eq(dataset_id))
        .get_result(&mut conn)
        .await
        .map_err(|_| ServiceError::NotFound)?;

    let bucket = get_aws_bucket()?;
    let s3_url = bucket
        .presign_get(file_metadata.id.to_string(), 300, None)
        .map_err(|_| ServiceError::BadRequest("Could not get presigned url".to_string()))?;

    let file_dto: FileDTO = file_metadata.into();
    let file_dto: FileDTO = FileDTO { s3_url, ..file_dto };

    Ok(file_dto)
}

#[tracing::instrument(skip(pool))]
pub async fn get_dataset_file_query(
    dataset_id: uuid::Uuid,
    page: u64,
    pool: web::Data<Pool>,
) -> Result<Vec<(File, i64, Option<uuid::Uuid>)>, actix_web::Error> {
    use crate::data::schema::files::dsl as files_columns;
    use crate::data::schema::groups_from_files::dsl as groups_from_files_columns;

    let mut conn = pool
        .get()
        .await
        .map_err(|_| ServiceError::BadRequest("Could not get database connection".to_string()))?;

    let file_metadata: Vec<(File, i64, Option<uuid::Uuid>)> = files_columns::files
        .left_join(
            groups_from_files_columns::groups_from_files
                .on(groups_from_files_columns::file_id.eq(files_columns::id)),
        )
        .filter(files_columns::dataset_id.eq(dataset_id))
        .select((
            File::as_select(),
            sql::<BigInt>("count(*) OVER()"),
            groups_from_files_columns::group_id.nullable(),
        ))
        .limit(10)
        .offset(((page - 1) * 10).try_into().unwrap_or(0))
        .load(&mut conn)
        .await
        .map_err(|_| ServiceError::NotFound)?;

    Ok(file_metadata)
}

#[tracing::instrument(skip(pool))]
pub async fn delete_file_query(
    file_uuid: uuid::Uuid,
    dataset: Dataset,
    delete_chunks: Option<bool>,
    pool: web::Data<Pool>,
    config: ServerDatasetConfiguration,
) -> Result<(), actix_web::Error> {
    use crate::data::schema::chunk_collisions::dsl as chunk_collisions_columns;
    use crate::data::schema::chunk_files::dsl as chunk_files_columns;
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;
    use crate::data::schema::files::dsl as files_columns;

    let mut conn = pool
        .get()
        .await
        .map_err(|_| ServiceError::BadRequest("Could not get database connection".to_string()))?;

    let mut chunk_ids = vec![];
    let mut collisions = vec![];
    let delete_chunks = delete_chunks.unwrap_or(false);

    if delete_chunks {
        let chunks = chunk_metadata_columns::chunk_metadata
            .inner_join(chunk_files_columns::chunk_files)
            .filter(chunk_files_columns::file_id.eq(file_uuid))
            .select(ChunkMetadata::as_select())
            .load::<ChunkMetadata>(&mut conn)
            .await
            .map_err(|_| ServiceError::NotFound)?;

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

    let file_metadata: File = files_columns::files
        .filter(files_columns::id.eq(file_uuid))
        .filter(files_columns::dataset_id.eq(dataset.id))
        .get_result(&mut conn)
        .await
        .map_err(|_| ServiceError::NotFound)?;

    let bucket = get_aws_bucket()?;
    bucket
        .delete_object(file_metadata.id.to_string())
        .await
        .map_err(|_| ServiceError::BadRequest("Could not delete file from S3".to_string()))?;

    let transaction_result = conn
        .transaction::<_, diesel::result::Error, _>(|conn| {
            async {
                diesel::delete(
                    chunk_files_columns::chunk_files
                        .filter(chunk_files_columns::file_id.eq(file_uuid)),
                )
                .execute(conn)
                .await?;

                diesel::delete(
                    files_columns::files
                        .filter(files_columns::id.eq(file_uuid))
                        .filter(files_columns::dataset_id.eq(dataset.clone().id)),
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
        Ok(_) => (),
        Err(e) => {
            log::error!("Error deleting file with transaction {:?}", e);
            return Err(ServiceError::BadRequest("Could not delete file".to_string()).into());
        }
    }

    Ok(())
}
