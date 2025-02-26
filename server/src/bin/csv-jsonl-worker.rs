use broccoli_queue::queue::BroccoliQueue;
use chrono::{DateTime, NaiveDateTime};
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use signal_hook::consts::SIGTERM;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio_stream::StreamExt;
use trieve_server::{
    data::models::{
        self, ChunkGroup, ChunkReqPayloadFields, ChunkReqPayloadMapping, CsvJsonlWorkerMessage,
        GeoInfo, GeoTypes,
    },
    errors::ServiceError,
    establish_connection, get_env,
    handlers::{
        chunk_handler::{ChunkReqPayload, FullTextBoost, SemanticBoost},
        file_handler::UploadFileReqPayload,
    },
    operators::{
        chunk_operator::create_chunk_metadata,
        clickhouse_operator::{ClickHouseEvent, EventQueue},
        file_operator::{create_file_query, get_csvjsonl_aws_bucket},
        group_operator::{create_group_from_file_query, create_groups_query},
    },
};

fn main() {
    dotenvy::dotenv().ok();
    env_logger::builder()
        .target(env_logger::Target::Stdout)
        .filter_level(log::LevelFilter::Info)
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

            let broccoli_queue = BroccoliQueue::builder(redis_url)
                .pool_connections(redis_connections.try_into().unwrap())
                .failed_message_retry_strategy(Default::default())
                .build()
                .await
                .expect("Failed to create broccoli queue");

            file_worker(
                should_terminate,
                web_redis_pool,
                web_pool,
                web_event_queue,
                broccoli_queue,
            )
            .await
        });
}

async fn file_worker(
    should_terminate: Arc<AtomicBool>,
    redis_pool: actix_web::web::Data<models::RedisPool>,
    web_pool: actix_web::web::Data<models::Pool>,
    event_queue: actix_web::web::Data<EventQueue>,
    broccoli_queue: BroccoliQueue,
) {
    log::info!("Starting csv-jsonl worker service thread");

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

        log::info!(
            "Retrying to get redis connection out of loop after {:?} secs",
            redis_conn_sleep
        );
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
            .arg("csv_jsonl_ingestion")
            .arg("csv_jsonl_processing")
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
                log::error!("IO broken pipe error, trying to acquire new connection");
                match redis_pool.get().await {
                    Ok(redis_conn) => {
                        log::info!("Got new redis connection after broken pipe! Resuming polling");
                        redis_connection = redis_conn;
                    }
                    Err(err) => {
                        log::error!(
                                "Failed to get redis connection after broken pipe, will try again after {broken_pipe_sleep:?} secs, err: {:?}",
                                err
                            );
                    }
                }

                tokio::time::sleep(broken_pipe_sleep).await;
                broken_pipe_sleep =
                    std::cmp::min(broken_pipe_sleep * 2, std::time::Duration::from_secs(300));
            }

            continue;
        };

        let csv_jsonl_worker_message: CsvJsonlWorkerMessage =
            serde_json::from_str(&serialized_message).expect("Failed to parse file message");

        let bucket = get_csvjsonl_aws_bucket().expect("Failed to get aws bucket");
        match bucket
            .head_object(csv_jsonl_worker_message.file_id.to_string())
            .await
        {
            Ok(url) => url,
            Err(err) => {
                log::error!(
                    "File {} has not yet been uploaded to the signed put url: {:?}",
                    csv_jsonl_worker_message.file_id.to_string(),
                    err.to_string(),
                );

                let _ = redis::cmd("LREM")
                    .arg("csv_jsonl_processing")
                    .arg(1)
                    .arg(serialized_message.clone())
                    .query_async::<redis::aio::MultiplexedConnection, usize>(&mut *redis_connection)
                    .await;

                if chrono::Utc::now().timestamp()
                    - csv_jsonl_worker_message.created_at.and_utc().timestamp()
                    > 8600
                {
                    event_queue
                        .send(ClickHouseEvent::WorkerEvent(
                            models::WorkerEvent::from_details(
                                csv_jsonl_worker_message.dataset_id,
                                models::EventType::CsvJsonlProcessingFailed {
                                    file_id: csv_jsonl_worker_message.file_id,
                                    error: "File was not uploaded to the signed PUT URL within 8600 seconds".to_string(),
                                },
                            )
                            .into(),
                        ))
                        .await;

                    let _ = redis::cmd("lpush")
                        .arg("dead_letters_csv_jsonl")
                        .arg(serialized_message)
                        .query_async::<redis::aio::MultiplexedConnection, ()>(
                            &mut *redis_connection,
                        )
                        .await;

                    continue;
                }

                let _ = redis::cmd("lpush")
                    .arg("csv_jsonl_ingestion")
                    .arg(serialized_message)
                    .query_async::<redis::aio::MultiplexedConnection, ()>(&mut *redis_connection)
                    .await;

                tokio::time::sleep(std::time::Duration::from_millis(500)).await;

                continue;
            }
        };

        let _ = process_csv_jsonl_file(
            csv_jsonl_worker_message,
            web_pool.clone(),
            broccoli_queue.clone(),
        )
        .await;
    }
}

async fn process_csv_jsonl_file(
    csv_jsonl_worker_message: CsvJsonlWorkerMessage,
    web_pool: actix_web::web::Data<models::Pool>,
    broccoli_queue: BroccoliQueue,
) -> Result<Option<uuid::Uuid>, ServiceError> {
    // get_object_stream from s3
    let bucket = get_csvjsonl_aws_bucket().map_err(|err| {
        log::error!("Failed to get aws bucket: {:?}", err);
        ServiceError::InternalServerError("Failed to get aws bucket".to_string())
    })?;

    log::info!(
        "Getting object stream for file id: {}",
        csv_jsonl_worker_message.file_id
    );

    let mut response_data_stream = bucket
        .get_object_stream(csv_jsonl_worker_message.file_id.to_string())
        .await
        .map_err(|err| {
            log::error!("Failed to get object stream: {:?}", err);
            ServiceError::InternalServerError("Failed to get object stream".to_string())
        })?;

    let chunk_group = ChunkGroup::from_details(
        Some(
            csv_jsonl_worker_message
                .create_presigned_put_url_data
                .file_name
                .clone(),
        ),
        csv_jsonl_worker_message
            .create_presigned_put_url_data
            .description
            .clone(),
        csv_jsonl_worker_message.dataset_id,
        csv_jsonl_worker_message
            .create_presigned_put_url_data
            .group_tracking_id
            .clone(),
        None,
        csv_jsonl_worker_message
            .create_presigned_put_url_data
            .tag_set
            .clone()
            .map(|tag_set| tag_set.into_iter().map(Some).collect()),
    );
    let chunk_group_option = create_groups_query(vec![chunk_group], true, web_pool.clone())
        .await
        .map_err(|e| {
            log::error!("Could not create group {:?}", e);
            ServiceError::BadRequest("Could not create group".to_string())
        })?
        .pop();
    let group_id = chunk_group_option
        .map(|group| group.id)
        .ok_or(ServiceError::BadRequest(
            "Could not create group".to_string(),
        ))?;

    log::info!("Group created with id: {:?}", group_id);

    let mut columns = vec![];
    let mut line = String::new();
    let mut bytes: bytes::BytesMut = bytes::BytesMut::new();
    let mut byte_count = 0;
    let mut chunk_req_payloads: Vec<ChunkReqPayload> = vec![];
    while let Some(chunk) = response_data_stream.bytes().next().await {
        let chunk_bytes = chunk.map_err(|err| {
            log::error!("Failed to get chunk from stream: {:?}", err);
            ServiceError::InternalServerError("Failed to get chunk from stream".to_string())
        })?;
        bytes.extend_from_slice(&chunk_bytes);
        let chunk = match String::from_utf8(bytes.to_vec()) {
            Ok(chunk) => {
                bytes.clear();
                chunk
            }
            Err(_) => {
                log::info!(
                    "Failed to convert bytes chunk to utf8, continuing with bytes append..."
                );
                continue;
            }
        };

        byte_count += chunk.len();

        let chunk_lines = chunk.split_inclusive('\n');
        for chunk_line in chunk_lines {
            if chunk_line.is_empty() {
                continue;
            }

            if chunk_line.ends_with('\n') {
                line.push_str(chunk_line.trim_end_matches('\n'));
                let object = match serde_json::from_str::<serde_json::Value>(&line) {
                    Ok(object) => Some(object),
                    Err(_) => {
                        if columns.is_empty() {
                            columns = parse_csv_line(&line);
                            None
                        } else {
                            let mut new_object = serde_json::Map::new();
                            let mut line_vals_iter = parse_csv_line(&line).into_iter();
                            for column in columns.iter() {
                                let key = column.trim();
                                let value: serde_json::Value = match line_vals_iter.next() {
                                    Some(val) => match val.trim() {
                                        "null" => serde_json::Value::Null,
                                        "None" => serde_json::Value::Null,
                                        val => {
                                            let number_value =
                                                if let Ok(int_val) = val.parse::<i64>() {
                                                    serde_json::Number::from_f64(int_val as f64)
                                                        .map(serde_json::Value::Number)
                                                } else if let Ok(float_val) = val.parse::<f64>() {
                                                    serde_json::Number::from_f64(float_val)
                                                        .map(serde_json::Value::Number)
                                                } else {
                                                    None
                                                };

                                            let bool_value = match val.to_lowercase().as_str() {
                                                "true" => Some(serde_json::Value::Bool(true)),
                                                "false" => Some(serde_json::Value::Bool(false)),
                                                _ => None,
                                            };

                                            if let Some(int_value) = number_value {
                                                int_value
                                            } else if let Some(bool_value) = bool_value {
                                                bool_value
                                            } else {
                                                serde_json::Value::String(val.to_string())
                                            }
                                        }
                                    },
                                    None => serde_json::Value::Null,
                                };
                                new_object.insert(key.to_string(), value);
                            }

                            Some(serde_json::Value::Object(new_object))
                        }
                    }
                };

                if let Some(object) = object {
                    let chunk_req_payload = convert_value_to_chunkreqpayload(
                        object,
                        Some(group_id),
                        csv_jsonl_worker_message
                            .create_presigned_put_url_data
                            .mappings
                            .clone()
                            .map(|mappings| mappings.0.clone())
                            .unwrap_or_default(),
                        csv_jsonl_worker_message
                            .create_presigned_put_url_data
                            .fulltext_boost_factor,
                        csv_jsonl_worker_message
                            .create_presigned_put_url_data
                            .semantic_boost_factor,
                    );
                    chunk_req_payloads.push(chunk_req_payload);

                    if chunk_req_payloads.len() >= 120 {
                        let (upsert_chunk_ingestion_message, upsert_chunk_metadatas) =
                            create_chunk_metadata(
                                chunk_req_payloads.clone(),
                                csv_jsonl_worker_message.dataset_id,
                            )?;

                        if !upsert_chunk_metadatas.is_empty() {
                            log::info!(
                                "Pushing chunk ingestion message to redis {:?}",
                                upsert_chunk_metadatas.len()
                            );
                            broccoli_queue
                                .publish(
                                    "ingestion",
                                    Some(csv_jsonl_worker_message.dataset_id.to_string()),
                                    &upsert_chunk_ingestion_message,
                                    None,
                                )
                                .await
                                .map_err(|err| ServiceError::BadRequest(err.to_string()))?;
                        }
                        chunk_req_payloads.clear();
                    }
                }
                line.clear();
            } else {
                line.push_str(chunk_line);
            }
        }
    }

    if !chunk_req_payloads.is_empty() {
        let (upsert_chunk_ingestion_message, upsert_chunk_metadatas) = create_chunk_metadata(
            chunk_req_payloads.clone(),
            csv_jsonl_worker_message.dataset_id,
        )?;

        if !upsert_chunk_metadatas.is_empty() {
            log::info!(
                "Pushing chunk ingestion message to redis after while {:?}",
                upsert_chunk_metadatas.len()
            );
            broccoli_queue
                .publish(
                    "ingestion",
                    Some(csv_jsonl_worker_message.dataset_id.to_string()),
                    &upsert_chunk_ingestion_message,
                    None,
                )
                .await
                .map_err(|err| ServiceError::BadRequest(err.to_string()))?;
        }
    }

    let file_size_mb = (byte_count as f64 / 1024.0 / 1024.0).round() as i64;
    let created_file = create_file_query(
        csv_jsonl_worker_message.file_id,
        file_size_mb,
        UploadFileReqPayload {
            file_name: csv_jsonl_worker_message
                .create_presigned_put_url_data
                .file_name,
            description: csv_jsonl_worker_message
                .create_presigned_put_url_data
                .description,
            group_tracking_id: csv_jsonl_worker_message
                .create_presigned_put_url_data
                .group_tracking_id,
            tag_set: csv_jsonl_worker_message
                .create_presigned_put_url_data
                .tag_set,
            link: csv_jsonl_worker_message.create_presigned_put_url_data.link,
            time_stamp: csv_jsonl_worker_message
                .create_presigned_put_url_data
                .time_stamp,
            metadata: csv_jsonl_worker_message
                .create_presigned_put_url_data
                .metadata,
            create_chunks: Some(false),
            rebalance_chunks: Some(false),
            split_delimiters: None,
            target_splits_per_chunk: None,
            pdf2md_options: None,
            split_avg: None,
            base64_file: "".to_string(),
        },
        csv_jsonl_worker_message.dataset_id,
        web_pool.clone(),
    )
    .await?;

    create_group_from_file_query(group_id, created_file.id, web_pool.clone())
        .await
        .map_err(|e| {
            log::error!("Could not create group from file {:?}", e);
            e
        })?;

    Ok(None)
}

fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current_field = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '"' => {
                if in_quotes {
                    if chars.peek() == Some(&'"') {
                        current_field.push('"');
                        chars.next();
                    } else {
                        in_quotes = false;
                    }
                } else {
                    in_quotes = true;
                }
            }
            ',' => {
                if in_quotes {
                    current_field.push(',');
                } else {
                    fields.push(current_field.clone());
                    current_field.clear();
                }
            }
            _ => {
                current_field.push(c);
            }
        }
    }

    fields.push(current_field);

    fields
}

fn convert_value_to_chunkreqpayload(
    value: serde_json::Value,
    group_id: Option<uuid::Uuid>,
    mappings: Vec<ChunkReqPayloadMapping>,
    fulltext_boost_factor: Option<f64>,
    semantic_boost_factor: Option<f64>,
) -> ChunkReqPayload {
    let cleaned_value = match value.clone() {
        serde_json::Value::Object(obj) => {
            let mut new_obj = serde_json::Map::new();
            for (key, value) in obj {
                if value.is_null() {
                    continue;
                }
                new_obj.insert(key, value);
            }
            serde_json::Value::Object(new_obj)
        }
        _ => value.clone(),
    };
    let chunk_html = match serde_json::to_string(&cleaned_value) {
        Ok(chunk_html) => Some(chunk_html),
        Err(_) => None,
    };
    let mut chunk_req_payload = ChunkReqPayload {
        chunk_html,
        semantic_content: None,
        link: None,
        tag_set: None,
        num_value: None,
        metadata: Some(value.clone()),
        tracking_id: None,
        upsert_by_tracking_id: Some(true),
        group_ids: Some(group_id.into_iter().collect()),
        group_tracking_ids: None,
        time_stamp: None,
        location: None,
        image_urls: None,
        weight: None,
        split_avg: None,
        convert_html_to_text: None,
        fulltext_boost: None,
        semantic_boost: None,
        high_priority: None,
    };

    let mut boost_phrase = String::new();
    let mut lat: Option<GeoTypes> = None;
    let mut lon: Option<GeoTypes> = None;

    for mapping in mappings {
        match mapping.chunk_req_payload_field {
            ChunkReqPayloadFields::Link => {
                let _ = value
                    .get(mapping.csv_jsonl_field)
                    .and_then(|val| val.as_str())
                    .map(|val| val.to_string())
                    .map(|val| chunk_req_payload.link = Some(val));
            }
            ChunkReqPayloadFields::TagSet => {
                let mut cur_tag_set = chunk_req_payload.tag_set.clone().unwrap_or_default();
                if let Some(val) = value.get(mapping.csv_jsonl_field) {
                    if let Some(arr) = val.as_array() {
                        for tag in arr {
                            if let Some(tag) = tag.as_str() {
                                cur_tag_set.push(tag.to_string());
                            }
                        }
                    } else if let Some(arr) = get_array_from_string(val.as_str().unwrap_or("")) {
                        for tag in arr {
                            cur_tag_set.push(tag);
                        }
                    } else if let Some(val) = val.as_str() {
                        cur_tag_set.push(val.to_string());
                    }
                }

                chunk_req_payload.tag_set = Some(cur_tag_set);
            }
            ChunkReqPayloadFields::NumValue => {
                let _ = value
                    .get(mapping.csv_jsonl_field)
                    .and_then(|val| val.as_f64())
                    .map(|val| chunk_req_payload.num_value = Some(val));
            }
            ChunkReqPayloadFields::TrackingId => {
                let _ = value
                    .get(mapping.csv_jsonl_field)
                    .map(|val| chunk_req_payload.tracking_id = Some(val.to_string()));
            }
            ChunkReqPayloadFields::GroupTrackingIds => {
                let mut cur_group_tracking_ids = chunk_req_payload
                    .group_tracking_ids
                    .clone()
                    .unwrap_or_default();
                if let Some(val) = value.get(mapping.csv_jsonl_field) {
                    if let Some(arr) = val.as_array() {
                        for url in arr {
                            if let Some(url) = url.as_str() {
                                cur_group_tracking_ids.push(url.to_string());
                            }
                        }
                    } else if let Some(arr) = get_array_from_string(val.as_str().unwrap_or("")) {
                        for url in arr {
                            cur_group_tracking_ids.push(url);
                        }
                    } else if let Some(val) = val.as_str() {
                        cur_group_tracking_ids.push(val.to_string());
                    }
                }

                chunk_req_payload.group_tracking_ids = Some(cur_group_tracking_ids);
            }
            ChunkReqPayloadFields::TimeStamp => {
                if let Some(val) = value.get(mapping.csv_jsonl_field) {
                    match val {
                        serde_json::Value::String(val) => {
                            let _ = val
                                .parse::<NaiveDateTime>()
                                .map(|val| chunk_req_payload.time_stamp = Some(val.to_string()));
                        }
                        serde_json::Value::Number(val) => {
                            let _ = val.as_i64().map(|val| {
                                chunk_req_payload.time_stamp =
                                    DateTime::from_timestamp(val, 0).map(|val| {
                                        val.with_timezone(&chrono::Local).naive_local().to_string()
                                    })
                            });
                        }
                        _ => {}
                    }
                }
            }
            ChunkReqPayloadFields::Lat => {
                let _ = value
                    .get(mapping.csv_jsonl_field)
                    .and_then(|val| val.as_f64())
                    .map(|val| lat = Some(GeoTypes::Float(val)));
            }
            ChunkReqPayloadFields::Lon => {
                let _ = value
                    .get(mapping.csv_jsonl_field)
                    .and_then(|val| val.as_f64())
                    .map(|val| lon = Some(GeoTypes::Float(val)));
            }
            ChunkReqPayloadFields::ImageUrls => {
                let mut cur_image_urls = chunk_req_payload.image_urls.clone().unwrap_or_default();
                if let Some(val) = value.get(mapping.csv_jsonl_field) {
                    if let Some(arr) = val.as_array() {
                        for url in arr {
                            if let Some(url) = url.as_str() {
                                cur_image_urls.push(url.to_string());
                            }
                        }
                    } else if let Some(arr) = get_array_from_string(val.as_str().unwrap_or("")) {
                        for url in arr {
                            cur_image_urls.push(url);
                        }
                    } else if let Some(val) = val.as_str() {
                        cur_image_urls.push(val.to_string());
                    }
                }

                chunk_req_payload.image_urls = Some(cur_image_urls);
            }
            ChunkReqPayloadFields::Weight => {
                let _ = value
                    .get(mapping.csv_jsonl_field)
                    .and_then(|val| val.as_f64())
                    .map(|val| chunk_req_payload.weight = Some(val));
            }
            ChunkReqPayloadFields::BoostPhrase => {
                let mut cur_boost_phrase = boost_phrase.clone();
                if let Some(val) = value.get(mapping.csv_jsonl_field) {
                    if let Some(val) = val.as_str() {
                        cur_boost_phrase.push_str(format!(" {}", val).as_str());
                    }
                }

                boost_phrase = cur_boost_phrase;
            }
        }
    }

    if let Some(fulltext_boost_factor) = fulltext_boost_factor {
        chunk_req_payload.fulltext_boost = Some(FullTextBoost {
            phrase: boost_phrase.clone(),
            boost_factor: fulltext_boost_factor,
        });
    }
    if let Some(semantic_boost_factor) = semantic_boost_factor {
        chunk_req_payload.semantic_boost = Some(SemanticBoost {
            phrase: boost_phrase,
            distance_factor: semantic_boost_factor as f32,
        });
    }
    if let Some(lat) = lat {
        if let Some(lon) = lon {
            chunk_req_payload.location = Some(GeoInfo { lat, lon });
        }
    }

    chunk_req_payload
}

fn get_array_from_string(string: &str) -> Option<Vec<String>> {
    if string.starts_with('[') && string.ends_with(']') {
        let line = &string[1..string.len() - 1];
        let values = parse_csv_line(line);
        Some(values)
    } else {
        None
    }
}
