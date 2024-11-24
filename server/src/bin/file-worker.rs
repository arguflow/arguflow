use base64::Engine;
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use redis::aio::MultiplexedConnection;
use signal_hook::consts::SIGTERM;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tracing_subscriber::{prelude::*, EnvFilter, Layer};
use trieve_server::{
    data::models::{self, FileWorkerMessage},
    errors::ServiceError,
    establish_connection, get_env,
    operators::{
        clickhouse_operator::{ClickHouseEvent, EventQueue},
        dataset_operator::get_dataset_and_organization_from_dataset_id_query,
        file_operator::{
            create_file_chunks, create_file_query, get_aws_bucket, preprocess_file_to_chunks,
        },
    },
};

fn main() {
    dotenvy::dotenv().ok();
    tracing_subscriber::Registry::default()
        .with(
            tracing_subscriber::fmt::layer().with_filter(
                EnvFilter::from_default_env()
                    .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into()),
            ),
        )
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

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime")
        .block_on(async move {
            let redis_url = get_env!("REDIS_URL", "REDIS_URL is not set");
            let redis_connections: u32 = std::env::var("REDIS_CONNECTIONS")
                .unwrap_or("2".to_string())
                .parse()
                .unwrap_or(2);

            let redis_manager = bb8_redis::RedisConnectionManager::new(redis_url)
                .expect("Failed to connect to redis");

            let redis_pool = bb8_redis::bb8::Pool::builder()
                .max_size(redis_connections)
                .connection_timeout(std::time::Duration::from_secs(2))
                .build(redis_manager)
                .await
                .expect("Failed to create redis pool");

            let web_redis_pool = actix_web::web::Data::new(redis_pool);

            let event_queue = if std::env::var("USE_ANALYTICS")
                .unwrap_or("false".to_string())
                .parse()
                .unwrap_or(false)
            {
                log::info!("Analytics enabled");

                let clickhouse_client = clickhouse::Client::default()
                    .with_url(
                        std::env::var("CLICKHOUSE_URL")
                            .unwrap_or("http://localhost:8123".to_string()),
                    )
                    .with_user(std::env::var("CLICKHOUSE_USER").unwrap_or("default".to_string()))
                    .with_password(std::env::var("CLICKHOUSE_PASSWORD").unwrap_or("".to_string()))
                    .with_database(
                        std::env::var("CLICKHOUSE_DATABASE").unwrap_or("default".to_string()),
                    )
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

            let should_terminate = Arc::new(AtomicBool::new(false));
            signal_hook::flag::register(SIGTERM, Arc::clone(&should_terminate))
                .expect("Failed to register shutdown hook");

            file_worker(should_terminate, web_redis_pool, web_pool, web_event_queue).await
        });
}

async fn file_worker(
    should_terminate: Arc<AtomicBool>,
    redis_pool: actix_web::web::Data<models::RedisPool>,
    web_pool: actix_web::web::Data<models::Pool>,
    event_queue: actix_web::web::Data<EventQueue>,
) {
    log::info!("Starting file worker service thread");

    let mut redis_conn_sleep = std::time::Duration::from_secs(1);

    #[allow(unused_assignments)]
    let mut opt_redis_connection = None;

    loop {
        let borrowed_redis_connection = match redis_pool.get().await {
            Ok(redis_connection) => Some(redis_connection),
            Err(err) => {
                log::error!("Failed to get redis connection outside of loop: {:?}", err);
                None
            }
        };

        if borrowed_redis_connection.is_some() {
            opt_redis_connection = borrowed_redis_connection;
            break;
        }

        tokio::time::sleep(redis_conn_sleep).await;
        redis_conn_sleep = std::cmp::min(redis_conn_sleep * 2, std::time::Duration::from_secs(300));
    }

    let mut redis_connection =
        opt_redis_connection.expect("Failed to get redis connection outside of loop");

    let mut broken_pipe_sleep = std::time::Duration::from_secs(10);

    loop {
        if should_terminate.load(Ordering::Relaxed) {
            log::info!("Shutting down");
            break;
        }

        let payload_result: Result<Vec<String>, redis::RedisError> = redis::cmd("brpoplpush")
            .arg("file_ingestion")
            .arg("file_processing")
            .arg(1.0)
            .query_async(&mut redis_connection.clone())
            .await;

        let serialized_message = if let Ok(payload) = payload_result {
            broken_pipe_sleep = std::time::Duration::from_secs(10);

            if payload.is_empty() {
                continue;
            }

            payload
                .first()
                .expect("Payload must have a first element")
                .clone()
        } else {
            log::error!("Unable to process {:?}", payload_result);

            if payload_result.is_err_and(|err| err.is_io_error()) {
                tokio::time::sleep(broken_pipe_sleep).await;
                broken_pipe_sleep =
                    std::cmp::min(broken_pipe_sleep * 2, std::time::Duration::from_secs(300));
            }

            continue;
        };

        let file_worker_message: FileWorkerMessage =
            serde_json::from_str(&serialized_message).expect("Failed to parse file message");

        match upload_file(
            file_worker_message.clone(),
            web_pool.clone(),
            event_queue.clone(),
            redis_connection.clone(),
        )
        .await
        {
            Ok(Some(file_id)) => {
                log::info!("Uploaded file: {:?}", file_id);

                event_queue
                    .send(ClickHouseEvent::WorkerEvent(
                        models::WorkerEvent::from_details(
                            file_worker_message.dataset_id,
                            models::EventType::FileUploaded {
                                file_id,
                                file_name: file_worker_message.upload_file_data.file_name.clone(),
                            },
                        )
                        .into(),
                    ))
                    .await;

                let _ = redis::cmd("LREM")
                    .arg("file_processing")
                    .arg(1)
                    .arg(serialized_message)
                    .query_async::<redis::aio::MultiplexedConnection, usize>(&mut *redis_connection)
                    .await;
            }
            Ok(_) => {
                log::info!(
                    "File was uploaded with specification to not create chunks for it: {:?}",
                    file_worker_message.file_id
                );
            }
            Err(err) => {
                log::error!("Failed to upload file: {:?}", err);

                let _ = readd_error_to_queue(
                    file_worker_message,
                    err,
                    event_queue.clone(),
                    redis_pool.clone(),
                )
                .await;
            }
        };
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
pub struct CreateFileTaskResponse {
    pub id: uuid::Uuid,
    pub status: FileTaskStatus,
    pub pos_in_queue: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq)]
pub enum FileTaskStatus {
    Created,
    ProcessingFile(u32),
    ChunkingFile(u32),
    Completed,
    Failed,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct PollTaskResponse {
    pub id: String,
    pub file_name: String,
    pub file_url: Option<String>,
    pub total_document_pages: u32,
    pub pages_processed: u32,
    pub status: String,
    pub created_at: String,
    pub pages: Option<Vec<PdfToMdPage>>,
    pub pagination_token: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct PdfToMdPage {
    pub id: String,
    pub task_id: String,
    pub content: String,
    pub page_num: u32,
    pub usage: serde_json::Value,
    pub created_at: String,
}

async fn upload_file(
    file_worker_message: FileWorkerMessage,
    web_pool: actix_web::web::Data<models::Pool>,
    event_queue: actix_web::web::Data<EventQueue>,
    redis_conn: MultiplexedConnection,
) -> Result<Option<uuid::Uuid>, ServiceError> {
    let file_id = file_worker_message.file_id;

    let bucket = get_aws_bucket()?;
    let file_data = bucket
        .get_object(file_id.clone().to_string())
        .await
        .map_err(|e| {
            log::error!("Could not get file from S3 {:?}", e);
            ServiceError::BadRequest("File is not present in s3".to_string())
        })?
        .as_slice()
        .to_vec();

    let file_name = file_worker_message.upload_file_data.file_name.clone();

    let dataset_org_plan_sub = get_dataset_and_organization_from_dataset_id_query(
        models::UnifiedId::TrieveUuid(file_worker_message.dataset_id),
        None,
        web_pool.clone(),
    )
    .await?;

    if file_name.ends_with(".pdf") {
        if let Some(true) = file_worker_message.upload_file_data.use_pdf2md_ocr {
            log::info!("Using pdf2md for OCR for file");
            let pdf2md_url = std::env::var("PDF2MD_URL")
                .expect("PDF2MD_URL must be set")
                .to_string();

            let pdf2md_auth = std::env::var("PDF2MD_AUTH").unwrap_or("".to_string());

            let pdf2md_client = reqwest::Client::new();
            let encoded_file = base64::prelude::BASE64_STANDARD.encode(file_data.clone());

            let json_value = serde_json::json!({
                "file_name": file_name,
                "base64_file": encoded_file.clone()
            });

            log::info!("Sending file to pdf2md");
            let pdf2md_response = pdf2md_client
                .post(format!("{}/api/task", pdf2md_url))
                .header("Content-Type", "application/json")
                .header("Authorization", &pdf2md_auth)
                .json(&json_value)
                .send()
                .await
                .map_err(|err| {
                    log::error!("Could not send file to pdf2md {:?}", err);
                    ServiceError::BadRequest("Could not send file to pdf2md".to_string())
                })?;

            let response = pdf2md_response.json::<CreateFileTaskResponse>().await;

            let task_id = match response {
                Ok(response) => response.id,
                Err(err) => {
                    log::error!("Could not parse id from pdf2md {:?}", err);
                    return Err(ServiceError::BadRequest(format!(
                        "Could not parse id from pdf2md {:?}",
                        err
                    )));
                }
            };

            log::info!("Waiting on Task {}", task_id);
            #[allow(unused_assignments)]
            let mut chunk_htmls: Vec<String> = vec![];
            let mut pagination_token: Option<String> = None;
            let mut completed = false;

            loop {
                if completed && pagination_token.is_none() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                let request = if let Some(pagination_token) = &pagination_token {
                    log::info!(
                        "Polling on task {} with pagination token {}",
                        task_id,
                        pagination_token
                    );
                    pdf2md_client
                        .get(
                            format!(
                                "{}/api/task/{}?pagination_token={}",
                                pdf2md_url, task_id, pagination_token
                            )
                            .as_str(),
                        )
                        .header("Content-Type", "application/json")
                        .header("Authorization", &pdf2md_auth)
                        .send()
                        .await
                        .map_err(|err| {
                            log::error!("Could not send poll request to pdf2md {:?}", err);
                            ServiceError::BadRequest(format!(
                                "Could not send request to pdf2md {:?}",
                                err
                            ))
                        })?
                } else {
                    log::info!("Waiting on task {}", task_id);
                    pdf2md_client
                        .get(format!("{}/api/task/{}", pdf2md_url, task_id).as_str())
                        .header("Content-Type", "application/json")
                        .header("Authorization", &pdf2md_auth)
                        .send()
                        .await
                        .map_err(|err| {
                            log::error!("Could not send poll request to pdf2md {:?}", err);
                            ServiceError::BadRequest(format!(
                                "Could not send request to pdf2md {:?}",
                                err
                            ))
                        })?
                };

                let task_response = request.json::<PollTaskResponse>().await.map_err(|err| {
                    log::error!("Could not parse response from pdf2md {:?}", err);
                    ServiceError::BadRequest(format!(
                        "Could not parse response from pdf2md {:?}",
                        err
                    ))
                })?;

                if task_response.status == "Completed" && task_response.pages.is_some() {
                    completed = true;
                    pagination_token = task_response.pagination_token.clone();
                    if let Some(pages) = task_response.pages {
                        log::info!("Got {} pages from task {}", pages.len(), task_id);
                        for page in pages {
                            log::info!(".");
                            chunk_htmls.push(page.content.clone());
                        }
                    }

                    continue;
                } else {
                    log::info!("Task {} not ready", task_id);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            }

            // Poll Chunks from pdf chunks from service
            let file_size_mb = (file_data.len() as f64 / 1024.0 / 1024.0).round() as i64;
            let created_file = create_file_query(
                file_id,
                file_size_mb,
                file_worker_message.upload_file_data.clone(),
                file_worker_message.dataset_id,
                web_pool.clone(),
            )
            .await?;

            create_file_chunks(
                created_file.id,
                file_worker_message.upload_file_data,
                chunk_htmls,
                dataset_org_plan_sub,
                web_pool.clone(),
                event_queue.clone(),
                redis_conn,
            )
            .await?;

            return Ok(Some(file_id));
        }
    }

    let tika_url = std::env::var("TIKA_URL")
        .expect("TIKA_URL must be set")
        .to_string();

    let tika_client = reqwest::Client::new();

    let tika_response = tika_client
        .put(format!("{}/tika", tika_url))
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
    if html_content.is_empty() {
        return Err(ServiceError::BadRequest(
            "Could not parse file with tika".to_string(),
        ));
    }

    let file_size_mb = (file_data.len() as f64 / 1024.0 / 1024.0).round() as i64;

    let created_file = create_file_query(
        file_id,
        file_size_mb,
        file_worker_message.upload_file_data.clone(),
        file_worker_message.dataset_id,
        web_pool.clone(),
    )
    .await?;

    if file_worker_message
        .upload_file_data
        .create_chunks
        .is_some_and(|create_chunks_bool| !create_chunks_bool)
    {
        return Ok(None);
    }

    let dataset_org_plan_sub = get_dataset_and_organization_from_dataset_id_query(
        models::UnifiedId::TrieveUuid(file_worker_message.dataset_id),
        None,
        web_pool.clone(),
    )
    .await?;

    let Ok(chunk_htmls) =
        preprocess_file_to_chunks(html_content, file_worker_message.upload_file_data.clone())
    else {
        return Err(ServiceError::BadRequest("Could not parse file".to_string()));
    };

    create_file_chunks(
        created_file.id,
        file_worker_message.upload_file_data,
        chunk_htmls,
        dataset_org_plan_sub,
        web_pool.clone(),
        event_queue.clone(),
        redis_conn,
    )
    .await?;

    Ok(Some(file_id))
}

#[tracing::instrument(skip(redis_pool, event_queue))]
pub async fn readd_error_to_queue(
    mut payload: FileWorkerMessage,
    error: ServiceError,
    event_queue: actix_web::web::Data<EventQueue>,
    redis_pool: actix_web::web::Data<models::RedisPool>,
) -> Result<(), ServiceError> {
    let old_payload_message = serde_json::to_string(&payload).map_err(|_| {
        ServiceError::InternalServerError("Failed to reserialize input for retry".to_string())
    })?;

    payload.attempt_number += 1;

    let mut redis_conn = redis_pool
        .get()
        .await
        .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

    let _ = redis::cmd("LREM")
        .arg("file_processing")
        .arg(1)
        .arg(old_payload_message.clone())
        .query_async::<redis::aio::MultiplexedConnection, usize>(&mut *redis_conn)
        .await;

    if payload.attempt_number == 3 {
        log::error!("Failed to insert data 3 times quitting {:?}", error);

        event_queue
            .send(ClickHouseEvent::WorkerEvent(
                models::WorkerEvent::from_details(
                    payload.dataset_id,
                    models::EventType::FileUploadFailed {
                        file_id: payload.file_id,
                        error: error.to_string(),
                    },
                )
                .into(),
            ))
            .await;

        let mut redis_conn = redis_pool
            .get()
            .await
            .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

        redis::cmd("lpush")
            .arg("dead_letters_file")
            .arg(old_payload_message)
            .query_async::<redis::aio::MultiplexedConnection, ()>(&mut *redis_conn)
            .await
            .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

        return Err(ServiceError::InternalServerError(format!(
            "Failed to create new qdrant point: {:?}",
            error
        )));
    }

    let new_payload_message = serde_json::to_string(&payload).map_err(|_| {
        ServiceError::InternalServerError("Failed to reserialize input for retry".to_string())
    })?;

    let mut redis_conn = redis_pool
        .get()
        .await
        .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

    log::error!(
        "Failed to insert data, re-adding {:?} retry: {:?}",
        error,
        payload.attempt_number
    );

    redis::cmd("lpush")
        .arg("file_ingestion")
        .arg(&new_payload_message)
        .query_async::<redis::aio::MultiplexedConnection, ()>(&mut *redis_conn)
        .await
        .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

    Ok(())
}
