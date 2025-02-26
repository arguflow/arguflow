use super::chunk_operator::{create_chunk_metadata, get_row_count_for_organization_id_query};
use super::clickhouse_operator::{ClickHouseEvent, EventQueue};
use super::group_operator::{create_group_from_file_query, create_groups_query};
use super::parse_operator::{build_chunking_regex, coarse_doc_chunker};
use crate::data::models::ChunkGroup;
use crate::data::models::FileDTO;
use crate::data::models::{Dataset, DatasetAndOrgWithSubAndPlan, DatasetConfiguration, EventType};
use crate::handlers::chunk_handler::ChunkReqPayload;
use crate::handlers::file_handler::UploadFileReqPayload;
use crate::operators::group_operator::delete_group_by_file_id_query;
use crate::{data::models::WorkerEvent, get_env};
use crate::{
    data::models::{File, Pool},
    errors::ServiceError,
};
use actix_web::web;
use broccoli_queue::queue::BroccoliQueue;
use diesel::dsl::sql;
use diesel::prelude::*;
use diesel::sql_types::BigInt;
use diesel_async::RunQueryDsl;
use regex::Regex;
use s3::{creds::Credentials, Bucket, Region};
use std::collections::HashMap;

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
            log::error!("Could not create or get bucket {:?}", e);
            ServiceError::BadRequest("Could not create or get bucket".to_string())
        })?
        .with_path_style();

    Ok(*aws_bucket)
}

pub fn get_pagefind_aws_bucket() -> Result<Bucket, ServiceError> {
    let aws_region_name: String = std::env::var("AWS_REGION_PAGEFIND").unwrap_or("".to_string());
    let s3_endpoint = std::env::var("S3_ENDPOINT_PAGEFIND")
        .unwrap_or(get_env!("S3_ENDPOINT", "S3_ENDPOINT should be set").to_string());
    let s3_bucket_name = std::env::var("S3_BUCKET_PAGEFIND")
        .unwrap_or(get_env!("S3_BUCKET", "S3_BUCKET should be set").to_string());

    let aws_region = Region::Custom {
        region: aws_region_name,
        endpoint: s3_endpoint,
    };

    let aws_credentials = if let Ok(creds) = Credentials::from_instance_metadata() {
        creds
    } else {
        let s3_access_key = std::env::var("S3_ACCESS_KEY_PAGEFIND")
            .unwrap_or(get_env!("S3_ACCESS_KEY", "S3_ACCESS_KEY should be set").to_string());
        let s3_secret_key = std::env::var("S3_SECRET_KEY_PAGEFIND")
            .unwrap_or(get_env!("S3_SECRET_KEY", "S3_SECRET_KEY should be set").to_string());

        Credentials {
            access_key: Some(s3_access_key),
            secret_key: Some(s3_secret_key),
            security_token: None,
            session_token: None,
            expiration: None,
        }
    };

    let aws_bucket = Bucket::new(&s3_bucket_name, aws_region, aws_credentials)
        .map_err(|e| {
            log::error!("Could not create or get bucket {:?}", e);
            ServiceError::BadRequest("Could not create or get bucket".to_string())
        })?
        .with_path_style();

    Ok(*aws_bucket)
}

pub fn get_csvjsonl_aws_bucket() -> Result<Bucket, ServiceError> {
    let aws_region_name: String = std::env::var("AWS_REGION_CSVJSONL").unwrap_or("".to_string());
    let s3_endpoint = std::env::var("S3_ENDPOINT_CSVJSONL")
        .unwrap_or(get_env!("S3_ENDPOINT", "S3_ENDPOINT should be set").to_string());
    let s3_bucket_name = std::env::var("S3_BUCKET_CSVJSONL")
        .unwrap_or(get_env!("S3_BUCKET", "S3_BUCKET should be set").to_string());

    let aws_region = Region::Custom {
        region: aws_region_name,
        endpoint: s3_endpoint,
    };

    let aws_credentials = if let Ok(creds) = Credentials::from_instance_metadata() {
        creds
    } else {
        let s3_access_key = std::env::var("S3_ACCESS_KEY_CSVJSONL")
            .unwrap_or(get_env!("S3_ACCESS_KEY", "S3_ACCESS_KEY should be set").to_string());
        let s3_secret_key = std::env::var("S3_SECRET_KEY_CSVJSONL").unwrap_or(
            get_env!(
                "S3_SECRET_KEY_CSVJSONL",
                "S3_SECRET_KEY_CSVJSONL should be set"
            )
            .to_string(),
        );

        Credentials {
            access_key: Some(s3_access_key),
            secret_key: Some(s3_secret_key),
            security_token: None,
            session_token: None,
            expiration: None,
        }
    };

    let aws_bucket = Bucket::new(&s3_bucket_name, aws_region, aws_credentials)
        .map_err(|e| {
            log::error!("Could not create or get bucket {:?}", e);
            ServiceError::BadRequest("Could not create or get bucket".to_string())
        })?
        .with_path_style();

    Ok(*aws_bucket)
}

pub async fn create_file_query(
    file_id: uuid::Uuid,
    file_size: i64,
    upload_file_data: UploadFileReqPayload,
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
        &upload_file_data.file_name,
        file_size,
        upload_file_data
            .tag_set
            .map(|tag_set| tag_set.into_iter().map(Some).collect()),
        upload_file_data.metadata,
        upload_file_data.link,
        upload_file_data.time_stamp,
        dataset_id,
    );

    let created_file: File = diesel::insert_into(files_columns::files)
        .values(&new_file)
        .get_result(&mut conn)
        .await
        .map_err(|err| ServiceError::BadRequest(format!("Could not create file {:?}", err)))?;

    Ok(created_file)
}

pub fn preprocess_file_to_chunks(
    html_content: String,
    upload_file_data: UploadFileReqPayload,
) -> Result<Vec<String>, ServiceError> {
    let split_delims = if let Some(split_delims) = upload_file_data.split_delimiters {
        let filtered_delimeters: Vec<String> = split_delims
            .into_iter()
            .filter(|delim| !delim.is_empty())
            .collect::<Vec<String>>();

        if filtered_delimeters.is_empty() {
            None
        } else {
            Some(filtered_delimeters)
        }
    } else {
        None
    };

    let split_regex: Option<Regex> = split_delims
        .map(|delimiters| {
            build_chunking_regex(delimiters).map_err(|e| {
                log::error!("Could not parse chunking delimiters {:?}", e);
                ServiceError::BadRequest("Could not parse chunking delimiters".to_string())
            })
        })
        .transpose()?;

    let rebalance_chunks = upload_file_data.rebalance_chunks.unwrap_or(true);
    let target_splits_per_chunk = upload_file_data.target_splits_per_chunk.unwrap_or(20);

    let chunk_htmls = coarse_doc_chunker(
        html_content,
        split_regex,
        rebalance_chunks,
        target_splits_per_chunk,
    );

    log::info!(
        "Successfully chunked file into {} chunks",
        chunk_htmls.len()
    );

    Ok(chunk_htmls)
}

pub fn split_markdown_by_headings(markdown_text: &str) -> Vec<String> {
    let lines: Vec<&str> = markdown_text
        .trim()
        .lines()
        .filter(|x| !x.trim().is_empty())
        .collect();
    let mut chunks = Vec::new();
    let mut current_content = Vec::new();
    let mut pending_heading: Option<String> = None;

    fn is_heading(line: &str) -> bool {
        line.trim().starts_with('#')
    }

    fn save_chunk(chunks: &mut Vec<String>, content: &[String]) {
        if !content.is_empty() {
            chunks.push(content.join("\n").trim().to_string());
        }
    }

    for (i, line) in lines.iter().enumerate() {
        if is_heading(line) {
            if !current_content.is_empty() {
                save_chunk(&mut chunks, &current_content);
                current_content.clear();
            }

            if i + 1 < lines.len() && !is_heading(lines[i + 1]) {
                if let Some(heading) = pending_heading.take() {
                    current_content.push(heading);
                }
                current_content.push(line.to_string());
            } else {
                pending_heading = Some(line.to_string());
            }
        } else if !line.trim().is_empty() || !current_content.is_empty() {
            current_content.push(line.to_string());
        }
    }

    if !current_content.is_empty() {
        save_chunk(&mut chunks, &current_content);
    }

    if let Some(heading) = pending_heading {
        chunks.push(heading);
    }

    if chunks.is_empty() && !lines.is_empty() {
        chunks.push(lines.join("\n").trim().to_string());
    }

    chunks
}

#[allow(clippy::too_many_arguments)]
pub async fn create_file_chunks(
    created_file_id: uuid::Uuid,
    upload_file_data: UploadFileReqPayload,
    mut chunks: Vec<ChunkReqPayload>,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
    group_id: Option<uuid::Uuid>,
    pool: web::Data<Pool>,
    event_queue: web::Data<EventQueue>,
    broccoli_queue: BroccoliQueue,
) -> Result<(), ServiceError> {
    let name = upload_file_data.file_name.clone();

    if upload_file_data
        .pdf2md_options
        .is_some_and(|x| x.split_headings.unwrap_or(false))
    {
        let mut new_chunks = Vec::new();

        for chunk in chunks {
            let chunk_group = ChunkGroup::from_details(
                Some(format!(
                    "{}-page-{}",
                    name,
                    chunk.metadata.as_ref().unwrap_or(&serde_json::json!({
                        "page_num": 0
                    }))["page_num"]
                        .as_i64()
                        .unwrap_or(0)
                )),
                upload_file_data.description.clone(),
                dataset_org_plan_sub.dataset.id,
                upload_file_data.group_tracking_id.clone(),
                chunk.metadata.clone(),
                upload_file_data
                    .tag_set
                    .clone()
                    .map(|tag_set| tag_set.into_iter().map(Some).collect()),
            );

            let chunk_group_option = create_groups_query(vec![chunk_group], true, pool.clone())
                .await
                .map_err(|e| {
                    log::error!("Could not create group {:?}", e);
                    ServiceError::BadRequest("Could not create group".to_string())
                })?
                .pop();

            let chunk_group = match chunk_group_option {
                Some(group) => group,
                None => {
                    return Err(ServiceError::BadRequest(
                        "Could not create group from file".to_string(),
                    ));
                }
            };

            let group_id = chunk_group.id;

            create_group_from_file_query(group_id, created_file_id, pool.clone())
                .await
                .map_err(|e| {
                    log::error!("Could not create group from file {:?}", e);
                    e
                })?;

            let split_chunks =
                split_markdown_by_headings(chunk.chunk_html.as_ref().unwrap_or(&String::new()));

            for (i, split_chunk) in split_chunks.into_iter().enumerate() {
                new_chunks.push(ChunkReqPayload {
                    chunk_html: Some(split_chunk),
                    tracking_id: chunk.tracking_id.clone().map(|x| format!("{}-{}", x, i)),
                    group_ids: Some(vec![group_id]),
                    ..chunk.clone()
                });
            }
        }

        chunks = new_chunks;
    } else {
        chunks.iter_mut().for_each(|chunk| {
            chunk.group_ids = group_id.map(|id| vec![id]);
        });
    }

    let chunk_count = get_row_count_for_organization_id_query(
        dataset_org_plan_sub.organization.organization.id,
        pool.clone(),
    )
    .await?;

    if chunk_count + chunks.len()
        > dataset_org_plan_sub
            .organization
            .plan
            .clone()
            .unwrap_or_default()
            .chunk_count as usize
    {
        log::error!(
            "Chunk count {} exceeds plan limit {}",
            chunk_count + chunks.len(),
            dataset_org_plan_sub
                .organization
                .plan
                .unwrap_or_default()
                .chunk_count
        );
        return Err(ServiceError::BadRequest(
            "Chunk count exceeds plan limit".to_string(),
        ));
    }

    let chunk_segments = chunks
        .chunks(120)
        .map(|chunk_segment| chunk_segment.to_vec())
        .collect::<Vec<Vec<ChunkReqPayload>>>();

    log::info!("Queuing chunks for creation");
    for chunk_segment in chunk_segments {
        let (ingestion_message, chunk_metadatas) =
            create_chunk_metadata(chunk_segment, dataset_org_plan_sub.dataset.id).map_err(|e| {
                log::error!("Could not create chunk metadata {:?}", e);
                ServiceError::BadRequest("Could not create chunk metadata".to_string())
            })?;

        if chunk_metadatas.is_empty() {
            continue;
        }

        broccoli_queue
            .publish(
                "ingestion",
                Some(dataset_org_plan_sub.dataset.id.to_string()),
                &ingestion_message,
                None,
            )
            .await
            .map_err(|e| {
                log::error!("Could not publish message {:?}", e);
                ServiceError::BadRequest("Could not publish message".to_string())
            })?;
    }

    event_queue
        .send(ClickHouseEvent::WorkerEvent(
            WorkerEvent::from_details(
                dataset_org_plan_sub.dataset.id,
                EventType::FileUploaded {
                    file_id: created_file_id,
                    file_name: name,
                },
            )
            .into(),
        ))
        .await;

    Ok(())
}

pub async fn get_file_query(
    file_uuid: uuid::Uuid,
    dataset_id: uuid::Uuid,
    content_type: Option<String>,
    pool: web::Data<Pool>,
) -> Result<FileDTO, actix_web::Error> {
    use crate::data::schema::files::dsl as files_columns;

    let mut conn = pool
        .get()
        .await
        .map_err(|_| ServiceError::BadRequest("Could not get database connection".to_string()))?;

    let file: File = files_columns::files
        .filter(files_columns::id.eq(file_uuid))
        .filter(files_columns::dataset_id.eq(dataset_id))
        .get_result(&mut conn)
        .await
        .map_err(|e| {
            log::error!("File with specified id not found {:?}", e);

            ServiceError::NotFound("File with specified id not found".to_string())
        })?;

    let mut custom_queries = HashMap::new();

    match content_type {
        Some(content_type) => {
            if content_type == "application/pdf" {
                custom_queries.insert(
                    "response-content-disposition".into(),
                    format!("attachment; filename*=utf-8''{}", file.file_name),
                );
            } else {
                custom_queries.insert(
                    "response-content-disposition".into(),
                    format!("attachment; filename=\"{}\"", file.file_name),
                );
            }
            custom_queries.insert("response-content-type".into(), content_type.to_string());
        }
        None => {
            custom_queries.insert(
                "response-content-disposition".into(),
                format!("attachment; filename=\"{}\"", file.file_name),
            );
        }
    }

    let bucket = get_aws_bucket()?;
    let s3_url = bucket
        .presign_get(file.id.to_string(), 6000, Some(custom_queries))
        .await
        .map_err(|e| {
            log::error!("Could not get presigned url {:?}", e);

            ServiceError::NotFound("Could not get presigned url".to_string())
        })?;

    let file_dto: FileDTO = file.into();
    let file_dto: FileDTO = FileDTO { s3_url, ..file_dto };

    Ok(file_dto)
}

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
        .map_err(|_| ServiceError::NotFound("No dataset found".to_string()))?;

    Ok(file_metadata)
}

pub async fn delete_file_query(
    file_uuid: uuid::Uuid,
    delete_chunks: Option<bool>,
    dataset: Dataset,
    pool: web::Data<Pool>,
    dataset_config: DatasetConfiguration,
) -> Result<(), actix_web::Error> {
    use crate::data::schema::files::dsl as files_columns;

    let mut conn = pool
        .get()
        .await
        .map_err(|_| ServiceError::BadRequest("Could not get database connection".to_string()))?;

    let file_metadata: File = files_columns::files
        .filter(files_columns::id.eq(file_uuid))
        .filter(files_columns::dataset_id.eq(dataset.id))
        .get_result(&mut conn)
        .await
        .map_err(|_| ServiceError::NotFound("File with specified id not found".to_string()))?;

    let bucket = get_aws_bucket()?;
    bucket
        .delete_object(file_metadata.id.to_string())
        .await
        .map_err(|_| ServiceError::BadRequest("Could not delete file from S3".to_string()))?;

    let dataset_id = dataset.clone().id;

    if delete_chunks.is_some_and(|delete_chunks| delete_chunks) {
        delete_group_by_file_id_query(
            file_uuid,
            dataset,
            chrono::Utc::now().naive_utc(),
            Some(true),
            pool.clone(),
            dataset_config,
        )
        .await?;
    }

    diesel::delete(
        files_columns::files
            .filter(files_columns::id.eq(file_uuid))
            .filter(files_columns::dataset_id.eq(dataset_id)),
    )
    .execute(&mut conn)
    .await
    .map_err(|e| {
        log::error!("Error deleting file {:?}", e);
        ServiceError::BadRequest("Could not delete file".to_string())
    })?;

    Ok(())
}
