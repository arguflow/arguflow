use super::auth_handler::{AdminOnly, LoggedUser};
#[cfg(not(feature = "hallucination-detection"))]
use crate::data::models::DummyHallucinationScore;
use crate::data::models::{
    escape_quotes, ChatMessageProxy, ChunkMetadata, ChunkMetadataStringTagSet, ChunkMetadataTypes,
    ChunkMetadataWithScore, ConditionType, ContextOptions, CountSearchMethod,
    DatasetAndOrgWithSubAndPlan, DatasetConfiguration, GeoInfo, HighlightOptions, ImageConfig,
    IngestSpecificChunkMetadata, MultiQuery, Pool, QdrantChunkMetadata, QueryTypes,
    RagQueryEventClickhouse, RecommendType, RecommendationEventClickhouse, RecommendationStrategy,
    RedisPool, RoleProxy, ScoreChunk, ScoreChunkDTO, SearchMethod, SearchModalities,
    SearchQueryEventClickhouse, SlimChunkMetadataWithScore, SortByField, SortOptions, TypoOptions,
    UnifiedId, UpdateSpecificChunkMetadata,
};
use crate::errors::ServiceError;
use crate::get_env;
use crate::middleware::api_version::APIVersion;
use crate::operators::chunk_operator::get_metadata_from_id_query;
use crate::operators::clickhouse_operator::{get_latency_from_header, ClickHouseEvent, EventQueue};
use crate::operators::dataset_operator::{
    get_dataset_usage_query, ChunkDeleteMessage, DeleteMessage,
};
use crate::operators::message_operator::get_text_from_audio;
use crate::operators::parse_operator::convert_html_to_text;
use crate::operators::qdrant_operator::{
    point_ids_exists_in_qdrant, recommend_qdrant_query, scroll_dataset_points,
};
use crate::operators::search_operator::{
    assemble_qdrant_filter, autocomplete_chunks_query, count_chunks_query, parse_query,
    search_chunks_query, search_hybrid_chunks, ParsedQuery, ParsedQueryTypes,
};
use crate::operators::{chunk_operator::*, crawl_operator};
use actix::Arbiter;
use actix_web::web::Bytes;
use actix_web::{web, HttpResponse};
use broccoli_queue::queue::BroccoliQueue;
use chrono::NaiveDateTime;
use crossbeam_channel::unbounded;
use dateparser::DateTimeUtc;
use itertools::Itertools;
use openai_dive::v1::api::Client;
use openai_dive::v1::resources::chat::{
    ChatCompletionParameters, ChatMessage, ChatMessageContent, ChatMessageContentPart,
    ChatMessageTextContentPart, DeltaChatMessage,
};
use openai_dive::v1::resources::chat::{ChatMessageImageContentPart, ImageUrlType};
use openai_dive::v1::resources::shared::StopToken;
use serde::{Deserialize, Serialize};
use serde_json::json;
use simple_server_timing_header::Timer;
use tokio_stream::StreamExt;
use utoipa::ToSchema;
#[cfg(feature = "hallucination-detection")]
use {
    crate::operators::message_operator::clean_markdown,
    hallucination_detection::{HallucinationDetector, HallucinationScore},
};

/// Boost the presence of certain tokens for fulltext (SPLADE) and keyword (BM25) search. I.e. boosting title phrases to priortize title matches or making sure that the listing for AirBNB itself ranks higher than companies who make software for AirBNB hosts by boosting the in-document-frequency of the AirBNB token (AKA word) for its official listing. Conceptually it multiples the in-document-importance second value in the tuples of the SPLADE or BM25 sparse vector of the chunk_html innerText for all tokens present in the boost phrase by the boost factor like so: (token, in-document-importance) -> (token, in-document-importance*boost_factor).
#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct FullTextBoost {
    /// The phrase to boost in the fulltext document frequency index
    pub phrase: String,
    /// Amount to multiplicatevly increase the frequency of the tokens in the phrase by
    pub boost_factor: f64,
}

/// Semantic boosting moves the dense vector of the chunk in the direction of the distance phrase for semantic search. I.e. you can force a cluster by moving every chunk for a PDF closer to its title or push a chunk with a chunk_html of "iphone" 25% closer to the term "flagship" by using the distance phrase "flagship" and a distance factor of 0.25. Conceptually it's drawing a line (euclidean/L2 distance) between the vector for the innerText of the chunk_html and distance_phrase then moving the vector of the chunk_html distance_factor*L2Distance closer to or away from the distance_phrase point along the line between the two points.
#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct SemanticBoost {
    /// Terms to embed in order to create the vector which is weighted summed with the chunk_html embedding vector
    pub phrase: String,
    /// Arbitrary float (positive or negative) specifying the multiplicate factor to apply before summing the phrase vector with the chunk_html embedding vector
    #[serde(alias = "boost_factor")]
    pub distance_factor: f32,
}

/// Scoring options provides ways to modify the sparse or dense vector created for the query in order to change how potential matches are scored. If not specified, this defaults to no modifications.
#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct ScoringOptions {
    /// Boost the presence of certain tokens for fulltext (SPLADE) and keyword (BM25) search. I.e. boosting title phrases to priortize title matches or making sure that the listing for AirBNB itself ranks higher than companies who make software for AirBNB hosts by boosting the in-document-frequency of the AirBNB token (AKA word) for its official listing. Conceptually it multiples the in-document-importance second value in the tuples of the SPLADE or BM25 sparse vector of the chunk_html innerText for all tokens present in the boost phrase by the boost factor like so: (token, in-document-importance) -> (token, in-document-importance*boost_factor).
    pub fulltext_boost: Option<FullTextBoost>,
    /// Semantic boosting moves the dense vector of the chunk in the direction of the distance phrase for semantic search. I.e. you can force a cluster by moving every chunk for a PDF closer to its title or push a chunk with a chunk_html of "iphone" 25% closer to the term "flagship" by using the distance phrase "flagship" and a distance factor of 0.25. Conceptually it's drawing a line (euclidean/L2 distance) between the vector for the innerText of the chunk_html and distance_phrase then moving the vector of the chunk_html distance_factor*L2Distance closer to or away from the distance_phrase point along the line between the two points.
    pub semantic_boost: Option<SemanticBoost>,
}

/// Request payload for creating a new chunk
#[derive(Serialize, Deserialize, Debug, ToSchema, Clone, Default)]
#[schema(title = "single", example = json!({
    "chunk_html": "<p>Some HTML content</p>",
    "link": "https://example.com",
    "tag_set": ["tag1", "tag2"],
    "metadata": {"key1": "value1", "key2": "value2"},
    "tracking_id": "tracking_id",
    "group_ids": ["d290f1ee-6c54-4b01-90e6-d701748f0851"],
    "group_tracking_ids": ["group_tracking_id"],
    "time_stamp": "2021-01-01 00:00:00.000",
    "location": {
        "lat": -34,
        "lon": 151
    },
    "image_urls": ["https://example.com/red", "https://example.com/blue"],
    "fulltext_boost": {"phrase": "foo", "boost_factor": 5.0},
    "semantic_boost": {"phrase": "flagship", "distance_factor": 0.5}
}))]
pub struct ChunkReqPayload {
    /// HTML content of the chunk. This can also be plaintext. The innerText of the HTML will be used to create the embedding vector. The point of using HTML is for convienience, as some users have applications where users submit HTML content.
    pub chunk_html: Option<String>,
    /// If semantic_content is present, it will be used for creating semantic embeddings instead of the innerText `chunk_html`. `chunk_html` will still be the only thing stored and always used for fulltext functionality. `chunk_html` must still be present for the chunk to be created properly.
    pub semantic_content: Option<String>,
    /// Link to the chunk. This can also be any string. Frequently, this is a link to the source of the chunk. The link value will not affect the embedding creation.
    pub link: Option<String>,
    /// Tag set is a list of tags. This can be used to filter chunks by tag. Unlike with metadata filtering, HNSW indices will exist for each tag such that there is not a performance hit for filtering on them.
    pub tag_set: Option<Vec<String>>,
    /// Num value is an arbitrary numerical value that can be used to filter chunks. This is useful for when you want to filter chunks by numerical value. There is no performance hit for filtering on num_value.
    pub num_value: Option<f64>,
    /// Metadata is a JSON object which can be used to filter chunks. This is useful for when you want to filter chunks by arbitrary metadata. Unlike with tag filtering, there is a performance hit for filtering on metadata.
    pub metadata: Option<serde_json::Value>,
    /// Tracking_id is a string which can be used to identify a chunk. This is useful for when you are coordinating with an external system and want to use the tracking_id to identify the chunk.
    pub tracking_id: Option<String>,
    /// Upsert when a chunk with the same tracking_id exists. By default this is false, and chunks will be ignored if another with the same tracking_id exists. If this is true, the chunk will be updated if a chunk with the same tracking_id exists.
    pub upsert_by_tracking_id: Option<bool>,
    /// Group ids are the Trieve generated ids of the groups that the chunk should be placed into. This is useful for when you want to create a chunk and add it to a group or multiple groups in one request. Groups with these Trieve generated ids must be created first, it cannot be arbitrarily created through this route.
    pub group_ids: Option<Vec<uuid::Uuid>>,
    /// Group tracking_ids are the user-assigned tracking_ids of the groups that the chunk should be placed into. This is useful for when you want to create a chunk and add it to a group or multiple groups in one request. If a group with the tracking_id does not exist, it will be created.
    pub group_tracking_ids: Option<Vec<String>>,
    /// Time_stamp should be an ISO 8601 combined date and time without timezone. It is used for time window filtering and recency-biasing search results.
    pub time_stamp: Option<String>,
    /// Location is a GeoInfo object which lets you specify a latitude and longitude which can be used later to filter results.
    pub location: Option<GeoInfo>,
    /// Image urls are a list of urls to images that are associated with the chunk. This is useful for when you want to associate images with a chunk.
    pub image_urls: Option<Vec<String>>,
    /// Weight is a float which can be used to bias search results. This is useful for when you want to bias search results for a chunk. The magnitude only matters relative to other chunks in the chunk's dataset dataset.
    pub weight: Option<f64>,
    /// Split avg is a boolean which tells the server to split the text in the chunk_html into smaller chunks and average their resulting vectors. This is useful for when you want to create a chunk from a large piece of text and want to split it into smaller chunks to create a more fuzzy average dense vector. The sparse vector will be generated normally with no averaging. By default this is false.
    pub split_avg: Option<bool>,
    /// Convert HTML to raw text before processing to avoid adding noise to the vector embeddings. By default this is true. If you are using HTML content that you want to be included in the vector embeddings, set this to false.
    pub convert_html_to_text: Option<bool>,
    ///  Full text boost is useful for when you want to boost certain phrases in the fulltext (SPLADE) and BM25 search results. I.e. making sure that the listing for AirBNB itself ranks higher than companies who make software for AirBNB hosts by boosting the in-document-frequency of the AirBNB token (AKA word) for its official listing. Conceptually it multiples the in-document-importance second value in the tuples of the SPLADE or BM25 sparse vector of the chunk_html innerText for all tokens present in the boost phrase by the boost factor like so: (token, in-document-importance) -> (token, in-document-importance*boost_factor).
    #[serde(alias = "boost_phrase")]
    pub fulltext_boost: Option<FullTextBoost>,
    /// Semantic boost is useful for moving the embedding vector of the chunk in the direction of the distance phrase. I.e. you can push a chunk with a chunk_html of "iphone" 25% closer to the term "flagship" by using the distance phrase "flagship" and a distance factor of 0.25. Conceptually it's drawing a line (euclidean/L2 distance) between the vector for the innerText of the chunk_html and distance_phrase then moving the vector of the chunk_html distance_factor*L2Distance closer to or away from the distance_phrase point along the line between the two points.
    #[serde(alias = "distance_phrase")]
    pub semantic_boost: Option<SemanticBoost>,
    /// High Priority allows you to place this chunk into a priority queue with its own ingestion workers. Can only be used by users with a Custom Pro plan.
    pub high_priority: Option<bool>,
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct FailedChunk {
    index: usize,
    message: String,
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
#[serde(untagged)]
pub enum ReturnQueuedChunk {
    /// All Chunks that have been queue'd with any errors filtered out
    Single(SingleQueuedChunkResponse),
    Batch(BatchQueuedChunkResponse),
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
#[schema(example = json!({
    "chunk_metadata": [{
        "content": "Some content",
        "link": "https://example.com",
        "tag_set": ["tag1", "tag2"],
        "metadata": {"key1": "value1", "key2": "value2"},
        "tracking_id": "tracking_id",
        "time_stamp": "2021-01-01 00:00:00.000",
        "weight": 0.5
    }],
    "pos_in_queue": 1
}))]
pub struct SingleQueuedChunkResponse {
    /// The chunk that got queue'd
    pub chunk_metadata: ChunkMetadata,
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
#[schema(title = "batch", example = json!({
    "chunk_metadata": [{
        "content": "Some content",
        "link": "https://example.com",
        "tag_set": ["tag1", "tag2"],
        "file_id": "d290f1ee-6c54-4b01-90e6-d701748f0851",
        "metadata": {"key1": "value1", "key2": "value2"},
        "tracking_id": "tracking_id",
        "time_stamp": "2021-01-01 00:00:00.000",
        "weight": 0.5
    },
    {
        "content": "Some content",
        "link": "https://example.com",
        "tag_set": ["tag1", "tag2"],
        "file_id": "d290f1ee-6c54-4b01-90e6-d701748f0851",
        "metadata": {"key1": "value1", "key2": "value2"},
        "tracking_id": "tracking_id",
        "time_stamp": "2021-01-01 00:00:00.000",
        "weight": 0.5
    }],
    "pos_in_queue": 2
}))]
pub struct BatchQueuedChunkResponse {
    // All the chunks that got queue'd
    pub chunk_metadata: Vec<ChunkMetadata>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UploadIngestionMessage {
    pub ingest_specific_chunk_metadata: IngestSpecificChunkMetadata,
    pub chunk: ChunkReqPayload,
    pub dataset_id: uuid::Uuid,
    pub upsert_by_tracking_id: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BulkUploadIngestionMessage {
    pub attempt_number: usize,
    pub only_qdrant: Option<bool>,
    pub dataset_id: uuid::Uuid,
    pub ingestion_messages: Vec<UploadIngestionMessage>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(title = "single", example = json!({
    "chunk_html": "<p>Some HTML content</p>",
    "link": "https://example.com",
    "tag_set": ["tag1", "tag2"],
    "metadata": {"key1": "value1", "key2": "value2"},
    "tracking_id": "tracking_id",
    "upsert_by_tracking_id": true,
    "group_ids": ["d290f1ee-6c54-4b01-90e6-d701748f0851"],
    "group_tracking_ids": ["group_tracking_id"],
    "time_stamp": "2021-01-01 00:00:00.000",
    "location": {
        "lat": -34,
        "lon": 151
    },
    "weight": 0.5,
    "split_avg": false,
    "convert_html_to_text": false,
}))]
pub struct CreateSingleChunkReqPayload(pub ChunkReqPayload);

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(title = "batch", example = json!([{
    "chunk_html": "<p>Some HTML content</p>",
    "link": "https://example.com",
    "tag_set": ["tag1", "tag2"],
    "metadata": {"key1": "value1", "key2": "value2"},
    "tracking_id": "tracking_id",
    "upsert_by_tracking_id": true,
    "group_ids": ["d290f1ee-6c54-4b01-90e6-d701748f0851"],
    "group_tracking_ids": ["group_tracking_id"],
    "time_stamp": "2021-01-01 00:00:00.000",
    "image_urls": ["https://example.com/red", "https://example.com/blue"],
    "location": {
        "lat": -34,
        "lon": 151
    },
}, {
    "chunk_html": "<p>Some more HTML content</p>",
    "link": "https://explain.com",
    "tag_set": ["tag3", "tag4"],
    "metadata": {"key1": "value1", "key2": "value2"},
    "tracking_id": "tracking_id",
    "upsert_by_tracking_id": true,
    "group_ids": ["d290f1ee-6c54-4b01-90e6-d701748f0851"],
    "group_tracking_ids": ["group_tracking_id"],
    "time_stamp": "2021-01-01 00:00:00.000",
    "image_urls": [],
    "location": {
        "lat": -34,
        "lon": 151
    },
    "weight": 0.5,
}]
))]
pub struct CreateBatchChunkReqPayload(pub Vec<ChunkReqPayload>);

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(untagged)]
pub enum CreateChunkReqPayloadEnum {
    Single(CreateSingleChunkReqPayload),
    Batch(CreateBatchChunkReqPayload),
}

/// Create or Upsert Chunk or Chunks
///
/// Create new chunk(s). If the chunk has the same tracking_id as an existing chunk, the request will fail. Once a chunk is created, it can be searched for using the search endpoint.
/// If uploading in bulk, the maximum amount of chunks that can be uploaded at once is 120 chunks. Auth'ed user or api key must have an admin or owner role for the specified dataset's organization.
#[utoipa::path(
    post,
    path = "/chunk",
    context_path = "/api",
    tag = "Chunk",
    request_body(content = CreateChunkReqPayloadEnum, description = "JSON request payload to create a new chunk (chunk)", content_type = "application/json"),
    responses(
        (status = 200, description = "JSON response payload containing the created chunk", body = ReturnQueuedChunk),
        (status = 426, description = "Error when upgrade is needed to process more chunks", body = ErrorResponseBody),
        (status = 413, description = "Error when more than 120 chunks are provided in bulk", body = ErrorResponseBody),
        (status = 400, description = "Error typically due to deserialization issues", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
    ),
    security(
        ("ApiKey" = ["admin"]),
    )
)]
pub async fn create_chunk(
    create_chunk_data: web::Json<CreateChunkReqPayloadEnum>,
    pool: web::Data<Pool>,
    _user: AdminOnly,
    fair_broccoli_queue: web::Data<BroccoliQueue>,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, actix_web::Error> {
    let chunks = match create_chunk_data.clone() {
        CreateChunkReqPayloadEnum::Single(chunk) => vec![chunk.0],
        CreateChunkReqPayloadEnum::Batch(chunks) => chunks.0,
    };
    let dataset_config =
        DatasetConfiguration::from_json(dataset_org_plan_sub.dataset.server_configuration.clone());

    let mut timer = Timer::new();

    let unlimited = std::env::var("UNLIMITED").unwrap_or("false".to_string());
    if unlimited == "false" {
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
            return Ok(HttpResponse::UpgradeRequired()
                .json(json!({"message": "Must upgrade your plan to add more chunks"})));
        }

        timer.add("get dataset count");
    }

    let chunk_limit: usize = std::env::var("BATCH_CHUNK_LIMIT")
        .unwrap_or("120".to_string())
        .parse()
        .unwrap_or(120);

    if chunks.len() > chunk_limit {
        return Err(ServiceError::PayloadTooLarge(
            "Too many chunks provided in bulk. The limit is 120 chunks per bulk upload".to_string(),
        )
        .into());
    }

    let chunks = chunks.into_iter().map(|chunk| {
        let non_empty_tracking_id = chunk
            .tracking_id
            .clone()
            .filter(|tracking_id| !tracking_id.is_empty());
        ChunkReqPayload {
            tracking_id: non_empty_tracking_id,
            ..chunk.clone()
        }
    });

    let (upsert_chunks, non_upsert_chunks): (Vec<ChunkReqPayload>, Vec<ChunkReqPayload>) =
        chunks.partition(|chunk| chunk.upsert_by_tracking_id.unwrap_or(false));

    let (non_upsert_chunk_ingestion_message, non_upsert_chunk_metadatas) =
        create_chunk_metadata(non_upsert_chunks, dataset_org_plan_sub.dataset.id)?;

    let (upsert_chunk_ingestion_message, upsert_chunk_metadatas) =
        create_chunk_metadata(upsert_chunks, dataset_org_plan_sub.dataset.id)?;

    let chunk_metadatas = non_upsert_chunk_metadatas
        .clone()
        .into_iter()
        .chain(upsert_chunk_metadatas.clone().into_iter())
        .dedup_by(|x, y| x.tracking_id == y.tracking_id)
        .collect::<Vec<ChunkMetadata>>();

    let is_premium = std::env::var("PREMIUM_ORGANIZATION_UUIDS")
        .unwrap_or("".to_string())
        .split(',')
        .map(|x| x.parse().ok())
        .collect::<Option<Vec<uuid::Uuid>>>()
        .unwrap_or(vec![])
        .contains(&dataset_org_plan_sub.organization.organization.id);

    if !non_upsert_chunk_metadatas.is_empty() {
        let prio_chunks_message: Vec<UploadIngestionMessage> = non_upsert_chunk_ingestion_message
            .ingestion_messages
            .clone()
            .into_iter()
            .filter(|msg| msg.chunk.high_priority.unwrap_or(false) && is_premium)
            .collect();

        let non_prio_chunks_message = non_upsert_chunk_ingestion_message
            .ingestion_messages
            .into_iter()
            .filter(|msg| !msg.chunk.high_priority.unwrap_or(false))
            .collect();

        // If the organization has a premium plan, send to the premium ingestion queue
        if is_premium && !prio_chunks_message.is_empty() {
            fair_broccoli_queue
                .publish(
                    "premium_ingestion",
                    Some(dataset_org_plan_sub.dataset.id.to_string()),
                    &BulkUploadIngestionMessage {
                        attempt_number: 0,
                        dataset_id: dataset_org_plan_sub.dataset.id,
                        ingestion_messages: prio_chunks_message.clone(),
                        only_qdrant: None,
                    },
                    None,
                )
                .await
                .map_err(|e| {
                    log::error!("Error publishing to queue: {:?}", e);
                    ServiceError::InternalServerError("Error publishing to queue".to_string())
                })?;
        } else if dataset_config.EMBEDDING_BASE_URL.contains("openai") {
            fair_broccoli_queue
                .publish(
                    "openai_ingestion",
                    Some(dataset_org_plan_sub.dataset.id.to_string()),
                    &BulkUploadIngestionMessage {
                        attempt_number: 0,
                        dataset_id: dataset_org_plan_sub.dataset.id,
                        ingestion_messages: non_prio_chunks_message,
                        only_qdrant: None,
                    },
                    None,
                )
                .await
                .map_err(|e| {
                    log::error!("Error publishing to queue: {:?}", e);
                    ServiceError::InternalServerError("Error publishing to queue".to_string())
                })?;
        } else {
            fair_broccoli_queue
                .publish(
                    "ingestion",
                    Some(dataset_org_plan_sub.dataset.id.to_string()),
                    &BulkUploadIngestionMessage {
                        attempt_number: 0,
                        dataset_id: dataset_org_plan_sub.dataset.id,
                        ingestion_messages: non_prio_chunks_message,
                        only_qdrant: None,
                    },
                    None,
                )
                .await
                .map_err(|e| {
                    log::error!("Error publishing to queue: {:?}", e);
                    ServiceError::InternalServerError("Error publishing to queue".to_string())
                })?;
        }
    }
    if !upsert_chunk_metadatas.is_empty() {
        let prio_chunks_message: Vec<UploadIngestionMessage> = upsert_chunk_ingestion_message
            .ingestion_messages
            .clone()
            .into_iter()
            .filter(|msg| msg.chunk.high_priority.unwrap_or(false) && is_premium)
            .collect();

        let non_prio_chunks_message = upsert_chunk_ingestion_message
            .ingestion_messages
            .into_iter()
            .filter(|msg| !msg.chunk.high_priority.unwrap_or(false))
            .collect();

        // If the organization has a premium plan, send to the premium ingestion queue
        if is_premium && !prio_chunks_message.is_empty() {
            fair_broccoli_queue
                .publish(
                    "premium_ingestion",
                    Some(dataset_org_plan_sub.dataset.id.to_string()),
                    &BulkUploadIngestionMessage {
                        attempt_number: 0,
                        dataset_id: dataset_org_plan_sub.dataset.id,
                        ingestion_messages: prio_chunks_message.clone(),
                        only_qdrant: None,
                    },
                    None,
                )
                .await
                .map_err(|e| {
                    log::error!("Error publishing to queue: {:?}", e);
                    ServiceError::InternalServerError("Error publishing to queue".to_string())
                })?;
        } else if dataset_config.EMBEDDING_BASE_URL.contains("openai") {
            fair_broccoli_queue
                .publish(
                    "openai_ingestion",
                    Some(dataset_org_plan_sub.dataset.id.to_string()),
                    &BulkUploadIngestionMessage {
                        attempt_number: 0,
                        dataset_id: dataset_org_plan_sub.dataset.id,
                        ingestion_messages: non_prio_chunks_message,
                        only_qdrant: None,
                    },
                    None,
                )
                .await
                .map_err(|e| {
                    log::error!("Error publishing to queue: {:?}", e);
                    ServiceError::InternalServerError("Error publishing to queue".to_string())
                })?;
        } else {
            fair_broccoli_queue
                .publish(
                    "ingestion",
                    Some(dataset_org_plan_sub.dataset.id.to_string()),
                    &BulkUploadIngestionMessage {
                        attempt_number: 0,
                        dataset_id: dataset_org_plan_sub.dataset.id,
                        ingestion_messages: non_prio_chunks_message,
                        only_qdrant: None,
                    },
                    None,
                )
                .await
                .map_err(|e| {
                    log::error!("Error publishing to queue: {:?}", e);
                    ServiceError::InternalServerError("Error publishing to queue".to_string())
                })?;
        }
    }

    let response = match create_chunk_data.into_inner() {
        CreateChunkReqPayloadEnum::Single(_) => ReturnQueuedChunk::Single(SingleQueuedChunkResponse {
            chunk_metadata: chunk_metadatas
                .get(0)
                .ok_or(ServiceError::BadRequest(
                    "Failed to queue a single chunk due to deriving 0 ingestion_messages from the request data".to_string(),
                ))?
                .clone(),
        }),
        CreateChunkReqPayloadEnum::Batch(_) => ReturnQueuedChunk::Batch(BatchQueuedChunkResponse {
            chunk_metadata: chunk_metadatas,
        }),
    };

    Ok(HttpResponse::Ok()
        .insert_header((Timer::header_key(), timer.header_value()))
        .json(response))
}

/// Delete Chunk
///
/// Delete a chunk by its id. Auth'ed user or api key must have an admin or owner role for the specified dataset's organization.
#[utoipa::path(
    delete,
    path = "/chunk/{chunk_id}",
    context_path = "/api",
    tag = "Chunk",
    responses(
        (status = 204, description = "Confirmation that the chunk with the id specified was deleted"),
        (status = 400, description = "Service error relating to finding a chunk by tracking_id", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
        ("chunk_id" = Option<uuid::Uuid>, Path, description = "Id of the chunk you want to fetch."),
    ),
    security(
        ("ApiKey" = ["admin"]),
    )
)]
pub async fn delete_chunk(
    chunk_id: web::Path<uuid::Uuid>,
    pool: web::Data<Pool>,
    _user: AdminOnly,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, actix_web::Error> {
    let dataset_config =
        DatasetConfiguration::from_json(dataset_org_plan_sub.dataset.server_configuration.clone());

    let chunk_id = chunk_id.into_inner();

    let deleted_at = chrono::Utc::now().naive_utc();

    delete_chunk_metadata_query(
        vec![chunk_id],
        deleted_at,
        dataset_org_plan_sub.dataset,
        pool,
        dataset_config,
    )
    .await?;

    Ok(HttpResponse::NoContent().finish())
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct BulkDeleteChunkPayload {
    /// Filter to apply to the chunks to delete
    pub filter: ChunkFilter,
}

/// Bulk Delete Chunks
///
/// Delete multiple chunks using a filter. Auth'ed user or api key must have an admin or owner role for the specified dataset's organization.

#[utoipa::path(
    delete,
    path = "/chunk",
    context_path = "/api",
    tag = "Chunk",
    request_body(content = BulkDeleteChunkPayload, description = "JSON request payload to speicy a filter to bulk delete chunks", content_type = "application/json"),
    responses(
        (status = 204, description = "Confirmation that the chunk with the id specified was deleted"),
        (status = 400, description = "Service error relating to finding a chunk by tracking_id", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
    ),
    security(
        ("ApiKey" = ["admin"]),
    )
)]
pub async fn bulk_delete_chunk(
    chunk_filter: web::Json<BulkDeleteChunkPayload>,
    redis_pool: web::Data<RedisPool>,
    _user: AdminOnly,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, actix_web::Error> {
    let mut redis_conn = redis_pool
        .get()
        .await
        .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

    let message = ChunkDeleteMessage {
        dataset_id: dataset_org_plan_sub.dataset.id,
        attempt_number: 0,
        filter: chunk_filter.into_inner().filter,
        deleted_at: chrono::Utc::now().naive_utc(),
    };

    let serialized_message = serde_json::to_string(&DeleteMessage::ChunkDelete(message))
        .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

    redis::cmd("lpush")
        .arg("delete_dataset_queue")
        .arg(&serialized_message)
        .query_async::<_, ()>(&mut *redis_conn)
        .await
        .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

    Ok(HttpResponse::NoContent().finish())
}

/// Delete Chunk By Tracking Id
///
/// Delete a chunk by tracking_id. This is useful for when you are coordinating with an external system and want to use the tracking_id to identify the chunk. Auth'ed user or api key must have an admin or owner role for the specified dataset's organization.
#[utoipa::path(
    delete,
    path = "/chunk/tracking_id/{tracking_id}",
    context_path = "/api",
    tag = "Chunk",
    responses(
        (status = 204, description = "Confirmation that the chunk with the tracking_id specified was deleted"),
        (status = 400, description = "Service error relating to finding a chunk by tracking_id", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
        ("tracking_id" = Option<String>, Path, description = "tracking_id of the chunk you want to delete"),
    ),
    security(
        ("ApiKey" = ["admin"]),
    )
)]
pub async fn delete_chunk_by_tracking_id(
    tracking_id: web::Path<String>,
    pool: web::Data<Pool>,
    _user: AdminOnly,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, actix_web::Error> {
    let tracking_id_inner = tracking_id.into_inner();
    let dataset_id = dataset_org_plan_sub.dataset.id;
    let dataset_config =
        DatasetConfiguration::from_json(dataset_org_plan_sub.dataset.server_configuration.clone());

    let chunk_metadata =
        get_metadata_from_tracking_id_query(tracking_id_inner, dataset_id, pool.clone()).await?;

    let deleted_at = chrono::Utc::now().naive_utc();

    delete_chunk_metadata_query(
        vec![chunk_metadata.id],
        deleted_at,
        dataset_org_plan_sub.dataset,
        pool,
        dataset_config,
    )
    .await?;

    Ok(HttpResponse::NoContent().finish())
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(example = json!({
    "chunk_id": "d290f1ee-6c54-4b01-90e6-d701748f0851",
    "link": "https://example.com",
    "chunk_html": "<p>Some HTML content</p>",
    "metadata": {"key1": "value1", "key2": "value2"},
    "time_stamp": "2021-01-01 00:00:00.000",
    "weight": 0.5,
    "group_ids": ["d290f1ee-6c54-4b01-90e6-d701748f0851"],
}))]
pub struct UpdateChunkReqPayload {
    /// Id of the chunk you want to update. You can provide either the chunk_id or the tracking_id. If both are provided, the chunk_id will be used.
    pub chunk_id: Option<uuid::Uuid>,
    /// Tracking_id of the chunk you want to update. This is required to match an existing chunk.
    pub tracking_id: Option<String>,
    /// Tag set is a list of tags. This can be used to filter chunks by tag. Unlike with metadata filtering, HNSW indices will exist for each tag such that there is not a performance hit for filtering on them. If no tag_set is provided, the existing tag_set will be used.
    pub tag_set: Option<Vec<String>>,
    /// Link of the chunk you want to update. This can also be any string. Frequently, this is a link to the source of the chunk. The link value will not affect the embedding creation. If no link is provided, the existing link will be used.
    pub link: Option<String>,
    ///Num value is an arbitrary numerical value that can be used to filter chunks. This is useful for when you want to filter chunks by numerical value. If no num_value is provided, the existing num_value will be used.
    pub num_value: Option<f64>,
    /// HTML content of the chunk you want to update. This can also be plaintext. The innerText of the HTML will be used to create the embedding vector. The point of using HTML is for convienience, as some users have applications where users submit HTML content. If no chunk_html is provided, the existing chunk_html will be used.
    pub chunk_html: Option<String>,
    /// The metadata is a JSON object which can be used to filter chunks. This is useful for when you want to filter chunks by arbitrary metadata. Unlike with tag filtering, there is a performance hit for filtering on metadata. If no metadata is provided, the existing metadata will be used.
    pub metadata: Option<serde_json::Value>,
    /// Time_stamp should be an ISO 8601 combined date and time without timezone. It is used for time window filtering and recency-biasing search results. If no time_stamp is provided, the existing time_stamp will be used.
    pub time_stamp: Option<String>,
    /// Weight is a float which can be used to bias search results. This is useful for when you want to bias search results for a chunk. The magnitude only matters relative to other chunks in the chunk's dataset dataset. If no weight is provided, the existing weight will be used.
    pub weight: Option<f64>,
    /// Group ids are the ids of the groups that the chunk should be placed into. This is useful for when you want to update a chunk and add it to a group or multiple groups in one request.
    pub group_ids: Option<Vec<uuid::Uuid>>,
    /// Group tracking_ids are the tracking_ids of the groups that the chunk should be placed into. This is useful for when you want to update a chunk and add it to a group or multiple groups in one request.
    pub group_tracking_ids: Option<Vec<String>>,
    /// Location is a GeoInfo object which lets you specify a latitude and longitude which can be used later to filter results.
    pub location: Option<GeoInfo>,
    /// Image urls are a list of urls to images that are associated with the chunk. This is useful for when you want to associate images with a chunk. If no image_urls are provided, the existing image_urls will be used.
    pub image_urls: Option<Vec<String>>,
    /// Convert HTML to raw text before processing to avoid adding noise to the vector embeddings. By default this is true. If you are using HTML content that you want to be included in the vector embeddings, set this to false.
    pub convert_html_to_text: Option<bool>,
    ///  Full text boost is useful for when you want to boost certain phrases in the fulltext (SPLADE) and BM25 search results. I.e. making sure that the listing for AirBNB itself ranks higher than companies who make software for AirBNB hosts by boosting the in-document-frequency of the AirBNB token (AKA word) for its official listing. Conceptually it multiples the in-document-importance second value in the tuples of the SPLADE or BM25 sparse vector of the chunk_html innerText for all tokens present in the boost phrase by the boost factor like so: (token, in-document-importance) -> (token, in-document-importance*boost_factor).
    #[serde(alias = "boost_phrase")]
    pub fulltext_boost: Option<FullTextBoost>,
    /// Semantic boost is useful for moving the embedding vector of the chunk in the direction of the distance phrase. I.e. you can push a chunk with a chunk_html of "iphone" 25% closer to the term "flagship" by using the distance phrase "flagship" and a distance factor of 0.25. Conceptually it's drawing a line (euclidean/L2 distance) between the vector for the innerText of the chunk_html and distance_phrase then moving the vector of the chunk_html distance_factor*L2Distance closer to or away from the distance_phrase point along the line between the two points.
    #[serde(alias = "distance_phrase")]
    pub semantic_boost: Option<SemanticBoost>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateIngestionMessage {
    pub chunk_metadata: UpdateSpecificChunkMetadata,
    pub dataset_id: uuid::Uuid,
    pub group_ids: Option<Vec<UnifiedId>>,
    pub convert_html_to_text: Option<bool>,
    pub fulltext_boost: Option<FullTextBoost>,
    pub semantic_boost: Option<SemanticBoost>,
    pub only_qdrant: Option<bool>,
}

/// Update Chunk
///
/// Update a chunk. If you try to change the tracking_id of the chunk to have the same tracking_id as an existing chunk, the request will fail. Auth'ed user or api key must have an admin or owner role for the specified dataset's organization.
#[utoipa::path(
    put,
    path = "/chunk",
    context_path = "/api",
    tag = "Chunk",
    request_body(content = UpdateChunkReqPayload, description = "JSON request payload to update a chunk (chunk)", content_type = "application/json"),
    responses(
        (status = 204, description = "No content Ok response indicating the chunk was updated as requested",),
        (status = 400, description = "Service error relating to to updating chunk, likely due to conflicting tracking_id", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
    ),
    security(
        ("ApiKey" = ["admin"]),
    )
)]
pub async fn update_chunk(
    update_chunk_data: web::Json<UpdateChunkReqPayload>,
    pool: web::Data<Pool>,
    _user: AdminOnly,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
    broccoli_queue: web::Data<BroccoliQueue>,
) -> Result<HttpResponse, actix_web::Error> {
    let dataset_id = dataset_org_plan_sub.dataset.id;
    let chunk_id = update_chunk_data.chunk_id;

    let chunk_metadata = if let Some(chunk_id) = chunk_id {
        get_metadata_from_id_query(chunk_id, dataset_id, pool).await?
    } else if let Some(tracking_id) = update_chunk_data.tracking_id.clone() {
        get_metadata_from_tracking_id_query(tracking_id.clone(), dataset_id, pool).await?
    } else {
        return Err(ServiceError::BadRequest(
            "Either chunk_id or tracking_id must be provided to update a chunk".into(),
        )
        .into());
    };

    let link = match update_chunk_data.link.clone() {
        Some(link) => Some(link),
        None => chunk_metadata.link,
    };

    let chunk_tracking_id = match update_chunk_data.tracking_id.clone() {
        Some(tracking_id) => {
            if tracking_id.is_empty() {
                None
            } else {
                Some(tracking_id)
            }
        }
        None => chunk_metadata.tracking_id,
    };

    let chunk_tag_set = update_chunk_data.tag_set.clone().map(|tag_set| {
        tag_set
            .into_iter()
            .map(Some)
            .collect::<Vec<Option<String>>>()
    });

    let chunk_html = match update_chunk_data.chunk_html.clone() {
        Some(chunk_html) => Some(chunk_html),
        None => chunk_metadata.chunk_html,
    };

    let chunk_metadata = ChunkMetadata::from_details_with_id(
        chunk_metadata.id,
        chunk_html,
        &link,
        &chunk_tag_set.or(chunk_metadata.tag_set),
        chunk_metadata.qdrant_point_id,
        <std::option::Option<serde_json::Value> as Clone>::clone(&update_chunk_data.metadata)
            .or(chunk_metadata.metadata),
        chunk_tracking_id,
        update_chunk_data
            .time_stamp
            .clone()
            .map(|ts| -> Result<NaiveDateTime, ServiceError> {
                //TODO: change all ts parsing to this crate
                Ok(ts
                    .parse::<DateTimeUtc>()
                    .map_err(|_| ServiceError::BadRequest("Invalid timestamp format".to_string()))?
                    .0
                    .with_timezone(&chrono::Local)
                    .naive_local())
            })
            .transpose()?
            .or(chunk_metadata.time_stamp),
        update_chunk_data.location.or(chunk_metadata.location),
        update_chunk_data.image_urls.clone().or(chunk_metadata
            .image_urls
            .map(|x| x.into_iter().map(|x| x.unwrap()).collect())),
        dataset_id,
        update_chunk_data.weight.unwrap_or(chunk_metadata.weight),
        update_chunk_data.num_value.or(chunk_metadata.num_value),
    );

    let group_ids = if let Some(group_ids) = update_chunk_data.group_ids.clone() {
        Some(
            group_ids
                .into_iter()
                .map(UnifiedId::from)
                .collect::<Vec<UnifiedId>>(),
        )
    } else {
        update_chunk_data
            .group_tracking_ids
            .clone()
            .map(|group_tracking_ids| {
                group_tracking_ids
                    .into_iter()
                    .map(UnifiedId::from)
                    .collect::<Vec<UnifiedId>>()
            })
    };

    let message = UpdateIngestionMessage {
        chunk_metadata: chunk_metadata.clone().into(),
        dataset_id,
        group_ids,
        convert_html_to_text: update_chunk_data.convert_html_to_text,
        fulltext_boost: update_chunk_data.fulltext_boost.clone(),
        semantic_boost: update_chunk_data.semantic_boost.clone(),
        only_qdrant: Some(false),
    };

    broccoli_queue
        .publish("update_chunk_queue", None, &message, None)
        .await
        .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

    Ok(HttpResponse::NoContent().finish())
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct UpdateChunkByTrackingIdData {
    /// Tracking_id of the chunk you want to update. This is required to match an existing chunk.
    tracking_id: String,
    /// Link of the chunk you want to update. This can also be any string. Frequently, this is a link to the source of the chunk. The link value will not affect the embedding creation. If no link is provided, the existing link will be used.
    link: Option<String>,
    /// HTML content of the chunk you want to update. This can also be plaintext. The innerText of the HTML will be used to create the embedding vector. The point of using HTML is for convienience, as some users have applications where users submit HTML content. If no chunk_html is provided, the existing chunk_html will be used.
    chunk_html: Option<String>,
    /// The metadata is a JSON object which can be used to filter chunks. This is useful for when you want to filter chunks by arbitrary metadata. Unlike with tag filtering, there is a performance hit for filtering on metadata. If no metadata is provided, the existing metadata will be used.
    metadata: Option<serde_json::Value>,
    /// Time_stamp should be an ISO 8601 combined date and time without timezone. It is used for time window filtering and recency-biasing search results. If no time_stamp is provided, the existing time_stamp will be used.
    time_stamp: Option<String>,
    /// Weight is a float which can be used to bias search results. This is useful for when you want to bias search results for a chunk. The magnitude only matters relative to other chunks in the chunk's dataset dataset. If no weight is provided, the existing weight will be used.
    weight: Option<f64>,
    /// Group ids are the ids of the groups that the chunk should be placed into. This is useful for when you want to update a chunk and add it to a group or multiple groups in one request.
    group_ids: Option<Vec<uuid::Uuid>>,
    /// Group tracking_ids are the tracking_ids of the groups that the chunk should be placed into. This is useful for when you want to update a chunk and add it to a group or multiple groups in one request.
    group_tracking_ids: Option<Vec<String>>,
    /// Convert HTML to raw text before processing to avoid adding noise to the vector embeddings. By default this is true. If you are using HTML content that you want to be included in the vector embeddings, set this to false.
    pub convert_html_to_text: Option<bool>,
}

/// Update Chunk By Tracking Id
///
/// Update a chunk by tracking_id. This is useful for when you are coordinating with an external system and want to use the tracking_id to identify the chunk. Auth'ed user or api key must have an admin or owner role for the specified dataset's organization.
#[utoipa::path(
    put,
    path = "/chunk/tracking_id/update",
    context_path = "/api",
    tag = "Chunk",
    request_body(content = UpdateChunkByTrackingIdData, description = "JSON request payload to update a chunk by tracking_id (chunks)", content_type = "application/json"),
    responses(
        (status = 204, description = "Confirmation that the chunk has been updated as per your request",),
        (status = 400, description = "Service error relating to to updating chunk", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
    ),
    security(
        ("ApiKey" = ["admin"]),
    )
)]
#[deprecated]

pub async fn update_chunk_by_tracking_id(
    update_chunk_data: web::Json<UpdateChunkByTrackingIdData>,
    pool: web::Data<Pool>,
    _user: AdminOnly,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
    broccoli_queue: web::Data<BroccoliQueue>,
) -> Result<HttpResponse, actix_web::Error> {
    if update_chunk_data.tracking_id.is_empty() {
        return Err(ServiceError::BadRequest(
            "Tracking id must be provided to update by tracking_id".into(),
        )
        .into());
    }
    let tracking_id = update_chunk_data.tracking_id.clone();

    let dataset_id = dataset_org_plan_sub.dataset.id;

    let chunk_metadata =
        get_metadata_from_tracking_id_query(tracking_id, dataset_id, pool.clone()).await?;

    let link = update_chunk_data
        .link
        .clone()
        .unwrap_or_else(|| chunk_metadata.link.clone().unwrap_or_default());

    let chunk_html = match update_chunk_data.chunk_html.clone() {
        Some(chunk_html) => Some(chunk_html),
        None => chunk_metadata.chunk_html,
    };

    let metadata = ChunkMetadata::from_details_with_id(
        chunk_metadata.id,
        chunk_html,
        &Some(link),
        &chunk_metadata.tag_set,
        chunk_metadata.qdrant_point_id,
        <std::option::Option<serde_json::Value> as Clone>::clone(&update_chunk_data.metadata)
            .or(chunk_metadata.metadata),
        Some(update_chunk_data.tracking_id.clone()),
        update_chunk_data
            .time_stamp
            .clone()
            .map(|ts| -> Result<NaiveDateTime, ServiceError> {
                //TODO: change all ts parsing to this crate
                Ok(ts
                    .parse::<DateTimeUtc>()
                    .map_err(|_| ServiceError::BadRequest("Invalid timestamp format".to_string()))?
                    .0
                    .with_timezone(&chrono::Local)
                    .naive_local())
            })
            .transpose()?
            .or(chunk_metadata.time_stamp),
        None,
        None,
        dataset_org_plan_sub.dataset.id,
        update_chunk_data.weight.unwrap_or(1.0),
        None,
    );
    let group_ids = if let Some(group_ids) = update_chunk_data.group_ids.clone() {
        Some(
            group_ids
                .into_iter()
                .map(UnifiedId::from)
                .collect::<Vec<UnifiedId>>(),
        )
    } else {
        update_chunk_data
            .group_tracking_ids
            .clone()
            .map(|group_tracking_ids| {
                group_tracking_ids
                    .into_iter()
                    .map(UnifiedId::from)
                    .collect::<Vec<UnifiedId>>()
            })
    };

    let message = UpdateIngestionMessage {
        chunk_metadata: metadata.clone().into(),
        dataset_id,
        group_ids,
        convert_html_to_text: update_chunk_data.convert_html_to_text,
        fulltext_boost: None,
        semantic_boost: None,
        only_qdrant: Some(false),
    };

    broccoli_queue
        .publish("update_chunk_queue", None, &message, None)
        .await
        .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

    Ok(HttpResponse::NoContent().finish())
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(example = json!({
    "must": [
        {
            "field": "tag_set",
            "match_all": ["A", "B"],
        },
        {
            "field": "num_value",
            "range": {
                "gte": 10,
                "lte": 25,
            }
        }
    ]
}))]
/// ChunkFilter is a JSON object which can be used to filter chunks. This is useful for when you want to filter chunks by arbitrary metadata. Unlike with tag filtering, there is a performance hit for filtering on metadata.
pub struct ChunkFilter {
    /// Only one of these field conditions has to match for the chunk to be included in the result set.
    pub should: Option<Vec<ConditionType>>,
    /// All of these field conditions have to match for the chunk to be included in the result set.
    pub must: Option<Vec<ConditionType>>,
    /// None of these field conditions can match for the chunk to be included in the result set.
    pub must_not: Option<Vec<ConditionType>>,
}

#[derive(Serialize, Clone, Debug, ToSchema)]
#[schema(example = json!({
    "search_type": "semantic",
    "query": "Some search query",
    "page": 1,
    "page_size": 10,
    "filters": {
        "should": [
            {
                "field": "metadata.key1",
                "match": ["value1", "value2"],
            }
        ],
        "must": [
            {
                "field": "num_value",
                "range": {
                    "gte": 0.0,
                    "lte": 1.0,
                    "gt": 0.0,
                    "lt": 1.0
                }
            }
        ],
        "must_not": [
            {
                "field": "metadata.key3",
                "match": ["value5", "value6"],
            }
        ]
    },
    "use_weights": true,
    "highlight_results": true,
    "highlight_delimiters": ["?", ",", ".", "!"],
    "score_threshold": 0.5
}))]
pub struct SearchChunksReqPayload {
    /// Can be either "semantic", "fulltext", "hybrid, or "bm25". If specified as "hybrid", it will pull in one page of both semantic and full-text results then re-rank them using scores from a cross encoder model. "semantic" will pull in one page of the nearest cosine distant vectors. "fulltext" will pull in one page of full-text results based on SPLADE. "bm25" will get one page of results scored using BM25 with the terms OR'd together.
    pub search_type: SearchMethod,
    /// Query is the search query. This can be any string. The query will be used to create an embedding vector and/or SPLADE vector which will be used to find the result set.  You can either provide one query, or multiple with weights. Multi-query only works with Semantic Search.
    pub query: QueryTypes,
    /// Page of chunks to fetch. Page is 1-indexed.
    pub page: Option<u64>,
    /// Page size is the number of chunks to fetch. This can be used to fetch more than 10 chunks at a time.
    pub page_size: Option<u64>,
    /// Get total page count for the query accounting for the applied filters. Defaults to false, but can be set to true when the latency penalty is acceptable (typically 50-200ms).
    pub get_total_pages: Option<bool>,
    /// Filters is a JSON object which can be used to filter chunks. This is useful for when you want to filter chunks by arbitrary metadata. Unlike with tag filtering, there is a performance hit for filtering on metadata.
    pub filters: Option<ChunkFilter>,
    /// Sort Options lets you specify different methods to rerank the chunks in the result set. If not specified, this defaults to the score of the chunks.
    pub sort_options: Option<SortOptions>,
    /// Scoring options provides ways to modify the sparse or dense vector created for the query in order to change how potential matches are scored. If not specified, this defaults to no modifications.
    pub scoring_options: Option<ScoringOptions>,
    /// Highlight Options lets you specify different methods to highlight the chunks in the result set. If not specified, this defaults to the score of the chunks.
    pub highlight_options: Option<HighlightOptions>,
    /// Set score_threshold to a float to filter out chunks with a score below the threshold for cosine distance metric. For Manhattan Distance, Euclidean Distance, and Dot Product, it will filter out scores above the threshold distance. This threshold applies before weight and bias modifications. If not specified, this defaults to no threshold. A threshold of 0 will default to no threshold.
    pub score_threshold: Option<f32>,
    /// Set slim_chunks to true to avoid returning the content and chunk_html of the chunks. This is useful for when you want to reduce amount of data over the wire for latency improvement (typically 10-50ms). Default is false.
    pub slim_chunks: Option<bool>,
    /// Set content_only to true to only returning the chunk_html of the chunks. This is useful for when you want to reduce amount of data over the wire for latency improvement (typically 10-50ms). Default is false.
    pub content_only: Option<bool>,
    /// If true, quoted and - prefixed words will be parsed from the queries and used as required and negated words respectively. Default is false.
    pub use_quote_negated_terms: Option<bool>,
    /// If true, stop words (specified in server/src/stop-words.txt in the git repo) will be removed. Queries that are entirely stop words will be preserved.
    pub remove_stop_words: Option<bool>,
    /// User ID is the id of the user who is making the request. This is used to track user interactions with the search results.
    pub user_id: Option<String>,
    /// Typo options lets you specify different methods to handle typos in the search query. If not specified, this defaults to no typo handling.
    pub typo_options: Option<TypoOptions>,
}

impl Default for SearchChunksReqPayload {
    fn default() -> Self {
        SearchChunksReqPayload {
            search_type: SearchMethod::Hybrid,
            query: QueryTypes::Single(SearchModalities::Text("".to_string())),
            page: Some(1),
            get_total_pages: None,
            page_size: Some(10),
            filters: None,
            sort_options: None,
            scoring_options: None,
            highlight_options: None,
            score_threshold: None,
            slim_chunks: None,
            content_only: None,
            use_quote_negated_terms: None,
            remove_stop_words: None,
            user_id: None,
            typo_options: None,
        }
    }
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Clone)]
#[schema(title = "V1")]
pub struct SearchChunkQueryResponseBody {
    pub score_chunks: Vec<ScoreChunkDTO>,
    pub corrected_query: Option<String>,
    pub total_chunk_pages: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(title = "V2")]
pub struct SearchResponseBody {
    pub id: uuid::Uuid,
    pub chunks: Vec<ScoreChunk>,
    pub corrected_query: Option<String>,
    pub total_pages: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(untagged)]
pub enum SearchResponseTypes {
    #[schema(title = "V2")]
    V2(SearchResponseBody),
    #[schema(title = "V1")]
    V1(SearchChunkQueryResponseBody),
}

impl SearchChunkQueryResponseBody {
    fn into_v2(self, search_id: uuid::Uuid) -> SearchResponseBody {
        SearchResponseBody {
            id: search_id,
            chunks: self
                .score_chunks
                .into_iter()
                .map(|chunk| chunk.into())
                .collect(),
            corrected_query: self.corrected_query,
            total_pages: self.total_chunk_pages,
        }
    }
}

pub fn is_audio(query: QueryTypes) -> bool {
    matches!(query, QueryTypes::Single(SearchModalities::Audio { .. }))
}

/// Search
///
/// This route provides the primary search functionality for the API. It can be used to search for chunks by semantic similarity, full-text similarity, or a combination of both. Results' `chunk_html` values will be modified with `<mark><b>` or custom specified tags for sub-sentence highlighting.
#[utoipa::path(
    post,
    path = "/chunk/search",
    context_path = "/api",
    tag = "Chunk",
    request_body(content = SearchChunksReqPayload, description = "JSON request payload to semantically search for chunks (chunks)", content_type = "application/json"),
    responses(
        (status = 200, description = "Chunks with embedding vectors which are similar to those in the request body", body = SearchResponseTypes),
        (status = 400, description = "Service error relating to searching", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
        ("X-API-Version" = Option<APIVersion>, Header, description = "The API version to use for this request. Defaults to V2 for orgs created after July 12, 2024 and V1 otherwise.")
    ),
    security(
        ("ApiKey" = ["readonly"]),
    )
)]
pub async fn search_chunks(
    data: web::Json<SearchChunksReqPayload>,
    _user: LoggedUser,
    pool: web::Data<Pool>,
    event_queue: web::Data<EventQueue>,
    redis_pool: web::Data<RedisPool>,
    api_version: APIVersion,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, actix_web::Error> {
    let dataset_config =
        DatasetConfiguration::from_json(dataset_org_plan_sub.dataset.server_configuration.clone());

    let mut data = data.into_inner();

    let parsed_query = match data.query.clone() {
        QueryTypes::Single(query) => ParsedQueryTypes::Single(
            parse_query(
                query.clone(),
                &dataset_org_plan_sub.dataset,
                data.use_quote_negated_terms,
                data.remove_stop_words,
            )
            .await?,
        ),
        QueryTypes::Multi(query) => {
            let parsed_queries = futures::future::join_all(query.into_iter().map(|multi_query| {
                let value = dataset_org_plan_sub.dataset.clone();
                async move {
                    let parsed_query = parse_query(
                        multi_query.query.clone(),
                        &value,
                        data.use_quote_negated_terms,
                        data.remove_stop_words,
                    )
                    .await?;
                    Ok((parsed_query, multi_query.weight))
                        as Result<(ParsedQuery, f32), ServiceError>
                }
            }))
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
            ParsedQueryTypes::Multi(parsed_queries)
        }
    };

    let query = match &parsed_query {
        ParsedQueryTypes::Single(query) => query.query.clone(),
        ParsedQueryTypes::Multi(ref query) => serde_json::to_string(
            &query
                .clone()
                .into_iter()
                .map(Into::into)
                .collect::<Vec<MultiQuery>>(),
        )
        .unwrap_or_default(),
    };

    if query.is_empty() {
        return Err(ServiceError::BadRequest("Query cannot be empty".to_string()).into());
    }

    data.score_threshold = data.score_threshold.filter(|threshold| *threshold != 0.0);

    let mut timer = Timer::new();

    let result_chunks = match data.search_type {
        SearchMethod::Hybrid => {
            search_hybrid_chunks(
                data.clone(),
                parsed_query.to_parsed_query()?,
                pool,
                redis_pool,
                dataset_org_plan_sub.dataset.clone(),
                &dataset_config,
                &mut timer,
            )
            .await?
        }
        _ => {
            search_chunks_query(
                data.clone(),
                parsed_query,
                pool,
                redis_pool,
                dataset_org_plan_sub.dataset.clone(),
                &dataset_config,
                &mut timer,
            )
            .await?
        }
    };
    timer.add("search_chunks");

    let search_id = uuid::Uuid::new_v4();

    if !dataset_config.DISABLE_ANALYTICS {
        let clickhouse_event = SearchQueryEventClickhouse {
            id: search_id,
            search_type: String::from("search"),
            query: query.clone(),
            request_params: serde_json::to_string(&data.clone()).unwrap_or_default(),
            latency: get_latency_from_header(timer.header_value()),
            top_score: result_chunks
                .score_chunks
                .first()
                .map(|x| x.score as f32)
                .unwrap_or(0.0),
            results: result_chunks
                .score_chunks
                .clone()
                .into_iter()
                .map(|x| {
                    let mut json = serde_json::to_value(&x).unwrap_or_default();
                    escape_quotes(&mut json);
                    json.to_string()
                })
                .collect(),
            dataset_id: dataset_org_plan_sub.dataset.id,
            created_at: time::OffsetDateTime::now_utc(),
            query_rating: String::from(""),
            user_id: data.user_id.clone().unwrap_or_default(),
        };

        event_queue
            .send(ClickHouseEvent::SearchQueryEvent(clickhouse_event))
            .await;
    }

    timer.add("send_to_clickhouse");

    if api_version == APIVersion::V2 {
        if is_audio(data.query.clone()) {
            return Ok(HttpResponse::Ok()
                .insert_header((Timer::header_key(), timer.header_value()))
                .insert_header((
                    "X-TR-Query",
                    query.replace(|c: char| c.is_ascii_control(), ""),
                ))
                .json(SearchResponseTypes::V2(result_chunks.into_v2(search_id))));
        } else {
            return Ok(HttpResponse::Ok()
                .insert_header((Timer::header_key(), timer.header_value()))
                .json(SearchResponseTypes::V2(result_chunks.into_v2(search_id))));
        }
    }

    Ok(HttpResponse::Ok()
        .insert_header((Timer::header_key(), timer.header_value()))
        .json(result_chunks))
}

#[derive(Serialize, Clone, Debug, ToSchema)]
#[schema(example = json!({
    "search_type": "semantic",
    "query": "Some search query",
    "page": 1,
    "page_size": 10,
    "filters": {
        "should": [
            {
                "field": "metadata.key1",
                "match": ["value1", "value2"],
                "range": {
                    "gte": 0.0,
                    "lte": 1.0,
                    "gt": 0.0,
                    "lt": 1.0
                }
            }
        ],
        "must": [
            {
                "field": "metadata.key2",
                "match": ["value3", "value4"],
                "range": {
                    "gte": 0.0,
                    "lte": 1.0,
                    "gt": 0.0,
                    "lt": 1.0
                }
            }
        ],
        "must_not": [
            {
                "field": "metadata.key3",
                "match": ["value5", "value6"],
                "range": {
                    "gte": 0.0,
                    "lte": 1.0,
                    "gt": 0.0,
                    "lt": 1.0
                }
            }
        ]
    },
    "recency_bias": 1.0,
    "use_weights": true,
    "highlight_results": true,
    "highlight_delimiters": ["?", ",", ".", "!"],
    "score_threshold": 0.5
}))]
pub struct AutocompleteReqPayload {
    /// Can be either "semantic", or "fulltext". "semantic" will pull in one page_size of the nearest cosine distant vectors. "fulltext" will pull in one page_size of full-text results based on SPLADE. "bm25" will pull in one page_size of results based on the BM25 algorithim
    pub search_type: SearchMethod,
    /// If specified to true, this will extend the search results to include non-exact prefix matches of the same search_type such that a full page_size of results are returned. Default is false.
    pub extend_results: Option<bool>,
    /// Query is the search query. This can be any string. The query will be used to create an embedding vector and/or SPLADE vector which will be used to find the result set.
    pub query: SearchModalities,
    /// Page size is the number of chunks to fetch. This can be used to fetch more than 10 chunks at a time.
    pub page_size: Option<u64>,
    /// Filters is a JSON object which can be used to filter chunks. This is useful for when you want to filter chunks by arbitrary metadata. Unlike with tag filtering, there is a performance hit for filtering on metadata.
    pub filters: Option<ChunkFilter>,
    /// Sort Options lets you specify different methods to rerank the chunks in the result set. If not specified, this defaults to the score of the chunks.
    pub sort_options: Option<SortOptions>,
    /// Scoring options provides ways to modify the sparse or dense vector created for the query in order to change how potential matches are scored. If not specified, this defaults to no modifications.
    pub scoring_options: Option<ScoringOptions>,
    /// Highlight Options lets you specify different methods to highlight the chunks in the result set. If not specified, this defaults to the score of the chunks.
    pub highlight_options: Option<HighlightOptions>,
    /// Set score_threshold to a float to filter out chunks with a score below the threshold. This threshold applies before weight and bias modifications. If not specified, this defaults to 0.0.
    pub score_threshold: Option<f32>,
    /// Set slim_chunks to true to avoid returning the content and chunk_html of the chunks. This is useful for when you want to reduce amount of data over the wire for latency improvement (typically 10-50ms). Default is false.
    pub slim_chunks: Option<bool>,
    /// Set content_only to true to only returning the chunk_html of the chunks. This is useful for when you want to reduce amount of data over the wire for latency improvement (typically 10-50ms). Default is false.
    pub content_only: Option<bool>,
    /// If true, quoted and - prefixed words will be parsed from the queries and used as required and negated words respectively. Default is false.
    pub use_quote_negated_terms: Option<bool>,
    /// If true, stop words (specified in server/src/stop-words.txt in the git repo) will be removed. Queries that are entirely stop words will be preserved.
    pub remove_stop_words: Option<bool>,
    /// User ID is the id of the user who is making the request. This is used to track user interactions with the search results.
    pub user_id: Option<String>,
    pub typo_options: Option<TypoOptions>,
}

impl From<AutocompleteReqPayload> for SearchChunksReqPayload {
    fn from(autocomplete_data: AutocompleteReqPayload) -> Self {
        SearchChunksReqPayload {
            search_type: autocomplete_data.search_type,
            query: QueryTypes::Single(autocomplete_data.query),
            page: Some(1),
            get_total_pages: None,
            page_size: autocomplete_data.page_size,
            filters: autocomplete_data.filters,
            sort_options: autocomplete_data.sort_options,
            scoring_options: autocomplete_data.scoring_options,
            highlight_options: autocomplete_data.highlight_options,
            score_threshold: autocomplete_data.score_threshold,
            slim_chunks: autocomplete_data.slim_chunks,
            content_only: autocomplete_data.content_only,
            use_quote_negated_terms: autocomplete_data.use_quote_negated_terms,
            remove_stop_words: autocomplete_data.remove_stop_words,
            user_id: autocomplete_data.user_id,
            typo_options: autocomplete_data.typo_options,
        }
    }
}

/// Autocomplete
///
/// This route provides the primary autocomplete functionality for the API. This prioritize prefix matching with semantic or full-text search.
#[utoipa::path(
    post,
    path = "/chunk/autocomplete",
    context_path = "/api",
    tag = "Chunk",
    request_body(content = AutocompleteReqPayload, description = "JSON request payload to semantically search for chunks (chunks)", content_type = "application/json"),
    responses(
        (status = 200, description = "Chunks with embedding vectors which are similar to those in the request body", body = SearchResponseTypes),
        (status = 400, description = "Service error relating to searching", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
        ("X-API-Version" = Option<APIVersion>, Header, description = "The API version to use for this request. Defaults to V2 for orgs created after July 12, 2024 and V1 otherwise.")
    ),
    security(
        ("ApiKey" = ["readonly"]),
    )
)]
pub async fn autocomplete(
    data: web::Json<AutocompleteReqPayload>,
    _user: LoggedUser,
    pool: web::Data<Pool>,
    event_queue: web::Data<EventQueue>,
    redis_pool: web::Data<RedisPool>,
    api_version: APIVersion,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, actix_web::Error> {
    let dataset_config =
        DatasetConfiguration::from_json(dataset_org_plan_sub.dataset.server_configuration.clone());

    let parsed_query = parse_query(
        data.query.clone(),
        &dataset_org_plan_sub.dataset,
        data.use_quote_negated_terms,
        data.remove_stop_words,
    )
    .await?;

    if parsed_query.query.is_empty() {
        return Err(ServiceError::BadRequest("Query cannot be empty".to_string()).into());
    }

    let mut timer = Timer::new();

    let result_chunks = autocomplete_chunks_query(
        data.clone(),
        parsed_query.clone(),
        pool,
        redis_pool,
        dataset_org_plan_sub.dataset.clone(),
        &dataset_config,
        &mut timer,
    )
    .await?;

    timer.add("autocomplete_chunks");

    let search_id = uuid::Uuid::new_v4();
    if !dataset_config.DISABLE_ANALYTICS {
        let clickhouse_event = SearchQueryEventClickhouse {
            id: search_id,
            search_type: String::from("autocomplete"),
            query: parsed_query.query.clone(),
            request_params: serde_json::to_string(&data.clone()).unwrap_or_default(),
            latency: get_latency_from_header(timer.header_value()),
            top_score: result_chunks
                .score_chunks
                .first()
                .map(|x| x.score as f32)
                .unwrap_or(0.0),
            results: result_chunks
                .score_chunks
                .clone()
                .into_iter()
                .map(|x| {
                    let mut json = serde_json::to_value(&x).unwrap_or_default();
                    escape_quotes(&mut json);
                    json.to_string()
                })
                .collect(),
            dataset_id: dataset_org_plan_sub.dataset.id,
            created_at: time::OffsetDateTime::now_utc(),
            query_rating: String::from(""),
            user_id: data.user_id.clone().unwrap_or_default(),
        };

        event_queue
            .send(ClickHouseEvent::SearchQueryEvent(clickhouse_event))
            .await;
    }

    timer.add("send_to_clickhouse");

    if api_version == APIVersion::V2 {
        if is_audio(QueryTypes::Single(data.query.clone())) {
            return Ok(HttpResponse::Ok()
                .insert_header((Timer::header_key(), timer.header_value()))
                .insert_header((
                    "X-TR-Query",
                    parsed_query
                        .query
                        .replace(|c: char| c.is_ascii_control(), ""),
                ))
                .json(SearchResponseTypes::V2(result_chunks.into_v2(search_id))));
        } else {
            return Ok(HttpResponse::Ok()
                .insert_header((Timer::header_key(), timer.header_value()))
                .json(SearchResponseTypes::V2(result_chunks.into_v2(search_id))));
        }
    }

    Ok(HttpResponse::Ok()
        .insert_header((Timer::header_key(), timer.header_value()))
        .json(result_chunks))
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(untagged)]
pub enum ChunkReturnTypes {
    V2(ChunkMetadata),
    V1(ChunkMetadataStringTagSet),
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct ScrollChunksResponseBody {
    pub chunks: Vec<ChunkMetadata>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct ScrollChunksReqPayload {
    /// Page size is the number of chunks to fetch. This can be used to fetch more than 10 chunks at a time.
    pub page_size: Option<u64>,
    /// Offset chunk id is the id of the chunk to start the page from. If not specified, this defaults to the first chunk in the dataset sorted by id ascending.
    pub offset_chunk_id: Option<uuid::Uuid>,
    /// Get total page count for the query accounting for the applied filters. Defaults to false, but can be set to true when the latency penalty is acceptable (typically 50-200ms).
    pub filters: Option<ChunkFilter>,
    /// Sort by lets you specify a key to sort the results by. If not specified, this defaults to the id's of the chunks. If specified, the field can be num_value, time_stamp, or any key in the chunk metadata. This key must be a numeric value within the payload.
    pub sort_by: Option<SortByField>,
}

/// Scroll Chunks
///
/// Get paginated chunks from your dataset with filters and custom sorting. If sort by is not specified, the results will sort by the id's of the chunks in ascending order. Sort by and offset_chunk_id cannot be used together; if you want to scroll with a sort by then you need to use a must_not filter with the ids you have already seen. There is a limit of 1000 id's in a must_not filter at a time.
#[utoipa::path(
    post,
    path = "/chunks/scroll",
    context_path = "/api",
    tag = "Chunk",
    request_body(content = ScrollChunksReqPayload, description = "JSON request payload to scroll through chunks (chunks)", content_type = "application/json"),
    responses(
        (status = 200, description = "Number of chunks equivalent to page_size starting from offset_chunk_id", body = ScrollChunksResponseBody),
        (status = 400, description = "Service error relating to scrolling chunks", body = ErrorResponseBody)
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
    ),
    security(
        ("ApiKey" = ["readonly"]),
    )
)]
pub async fn scroll_dataset_chunks(
    data: web::Json<ScrollChunksReqPayload>,
    _user: LoggedUser,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
    pool: web::Data<Pool>,
) -> Result<HttpResponse, actix_web::Error> {
    let dataset_config =
        DatasetConfiguration::from_json(dataset_org_plan_sub.dataset.server_configuration.clone());
    let dataset_id = dataset_org_plan_sub.dataset.id;
    let filters = data.filters.clone();

    let filter = assemble_qdrant_filter(filters, None, None, dataset_id, pool.clone()).await?;

    let qdrant_point_id_of_offset_chunk = match data.offset_chunk_id {
        Some(offset_chunk_id) => {
            let chunk =
                get_metadata_from_id_query(offset_chunk_id, dataset_id, pool.clone()).await?;
            Some(chunk.qdrant_point_id)
        }
        None => None,
    };

    let (search_results, _) = scroll_dataset_points(
        data.page_size.unwrap_or(10),
        qdrant_point_id_of_offset_chunk,
        data.sort_by.clone(),
        dataset_config.clone(),
        filter,
    )
    .await?;

    let chunks = if dataset_config.QDRANT_ONLY {
        search_results
            .iter()
            .map(|search_result| {
                ChunkMetadata::from(ChunkMetadataTypes::Metadata(
                    ChunkMetadataStringTagSet::from(QdrantChunkMetadata::from(
                        search_result.clone(),
                    )),
                ))
            })
            .collect()
    } else {
        let qdrant_point_ids: Vec<uuid::Uuid> = search_results
            .iter()
            .map(|search_result| search_result.point_id)
            .collect();

        let chunk_metadatas: Vec<ChunkMetadata> =
            get_chunk_metadatas_from_point_ids(qdrant_point_ids.clone(), pool)
                .await?
                .into_iter()
                .map(ChunkMetadata::from)
                .collect();

        qdrant_point_ids
            .into_iter()
            .filter_map(|point_id| {
                chunk_metadatas
                    .iter()
                    .find(|chunk| chunk.qdrant_point_id == point_id)
                    .cloned()
            })
            .collect()
    };

    let resp = ScrollChunksResponseBody { chunks };

    Ok(HttpResponse::Ok().json(resp))
}

/// Get Chunk By Id
///
/// Get a singular chunk by id.
#[utoipa::path(
    get,
    path = "/chunk/{chunk_id}",
    context_path = "/api",
    tag = "Chunk",
    responses(
        (status = 200, description = "chunk with the id that you were searching for", body = ChunkReturnTypes),
        (status = 400, description = "Service error relating to fidning a chunk by tracking_id", body = ErrorResponseBody),
        (status = 404, description = "Chunk not found", body = ErrorResponseBody)
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
        ("X-API-Version" = Option<APIVersion>, Header, description = "The API version to use for this request. Defaults to V2 for orgs created after July 12, 2024 and V1 otherwise."),
        ("chunk_id" = Option<uuid::Uuid>, Path, description = "Id of the chunk you want to fetch."),
    ),
    security(
        ("ApiKey" = ["readonly"]),
    )
)]
pub async fn get_chunk_by_id(
    chunk_id: web::Path<uuid::Uuid>,
    _user: LoggedUser,
    pool: web::Data<Pool>,
    api_version: APIVersion,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, ServiceError> {
    let chunk_id = chunk_id.into_inner();

    let dataset_configuration =
        DatasetConfiguration::from_json(dataset_org_plan_sub.dataset.server_configuration.clone());

    let chunk = get_metadata_from_id_query(chunk_id, dataset_org_plan_sub.dataset.id, pool).await?;
    let chunk_string_tag_set = ChunkMetadataStringTagSet::from(chunk);

    let point_id = chunk_string_tag_set.qdrant_point_id;
    let pointid_exists = point_ids_exists_in_qdrant(vec![point_id], dataset_configuration).await?;

    if pointid_exists {
        if api_version == APIVersion::V2 {
            return Ok(HttpResponse::Ok().json(ChunkMetadata::from(chunk_string_tag_set)));
        }
        Ok(HttpResponse::Ok().json(chunk_string_tag_set))
    } else {
        Err(ServiceError::NotFound("Chunk not found".to_string()))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[schema(example = json!({
    "search_type": "semantic",
    "query": "Some search query",
    "score_threshold": 0.5
}))]
pub struct CountChunksReqPayload {
    /// Can be either "semantic", "fulltext", or "bm25". "hybrid" is not supported due to latency limitations with using the reranker. These search types are applied without the reranker cross-encoder model, so if you are using it be aware that the count may not directly correlate with an actual search query.
    pub search_type: CountSearchMethod,
    /// Query is the search query. This can be any string. The query will be used to create an embedding vector and/or SPLADE vector which will be used to find the result set.
    pub query: QueryTypes,
    /// Filters is a JSON object which can be used to filter chunks. This is useful for when you want to filter chunks by arbitrary metadata. Unlike with tag filtering, there is a performance hit for filtering on metadata.
    pub filters: Option<ChunkFilter>,
    /// Set score_threshold to a float to filter out chunks with a score below the threshold. This threshold applies before weight and bias modifications. If not specified, this defaults to 0.0.
    pub score_threshold: Option<f32>,
    /// Set limit to restrict the maximum number of chunks to count. This is useful for when you want to reduce the latency of the count operation. By default the limit will be the number of chunks in the dataset.
    pub limit: Option<u64>,
    /// If true, quoted and - prefixed words will be parsed from the queries and used as required and negated words respectively. Default is false.
    pub use_quote_negated_terms: Option<bool>,
}

impl From<CountChunksReqPayload> for SearchChunksReqPayload {
    fn from(count_data: CountChunksReqPayload) -> Self {
        SearchChunksReqPayload {
            search_type: count_data.search_type.into(),
            query: count_data.query,
            page: Some(1),
            get_total_pages: None,
            page_size: count_data.limit,
            filters: count_data.filters,
            sort_options: None,
            scoring_options: None,
            highlight_options: None,
            score_threshold: count_data.score_threshold,
            slim_chunks: None,
            content_only: None,
            use_quote_negated_terms: count_data.use_quote_negated_terms,
            remove_stop_words: None,
            user_id: None,
            typo_options: None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct CountChunkQueryResponseBody {
    pub count: u32,
}

/// Count chunks above threshold
///
/// This route can be used to determine the number of chunk results that match a search query including score threshold and filters. It may be high latency for large limits. There is a dataset configuration imposed restriction on the maximum limit value (default 10,000) which is used to prevent DDOS attacks. Auth'ed user or api key must have an admin or owner role for the specified dataset's organization.
#[utoipa::path(
    post,
    path = "/chunk/count",
    context_path = "/api",
    tag = "Chunk",
    request_body(content = CountChunksReqPayload, description = "JSON request payload to count chunks for a search query", content_type = "application/json"),
    responses(
        (status = 200, description = "Number of chunks satisfying the query", body = CountChunkQueryResponseBody),
        (status = 404, description = "Failed to count chunks", body = ErrorResponseBody)
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
    ),
    security(
        ("ApiKey" = ["readonly"]),
    )
)]
pub async fn count_chunks(
    data: web::Json<CountChunksReqPayload>,
    _user: LoggedUser,
    pool: web::Data<Pool>,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, actix_web::Error> {
    let dataset_config =
        DatasetConfiguration::from_json(dataset_org_plan_sub.dataset.server_configuration.clone());

    let limit = match data.limit {
        Some(limit) => limit,
        None => {
            let dataset_usage =
                get_dataset_usage_query(dataset_org_plan_sub.dataset.id, pool.clone()).await?;
            dataset_usage.chunk_count as u64
        }
    };

    let search_req_data = CountChunksReqPayload {
        limit: Some(limit),
        ..data.clone()
    };

    let parsed_query = match data.query.clone() {
        QueryTypes::Single(query) => ParsedQueryTypes::Single(
            parse_query(
                query.clone(),
                &dataset_org_plan_sub.dataset,
                data.use_quote_negated_terms,
                None,
            )
            .await?,
        ),
        QueryTypes::Multi(query) => {
            let parsed_queries = futures::future::join_all(query.into_iter().map(|multi_query| {
                let value = dataset_org_plan_sub.dataset.clone();
                let data = data.clone();
                async move {
                    let parsed_query = parse_query(
                        multi_query.query.clone(),
                        &value,
                        data.use_quote_negated_terms,
                        None,
                    )
                    .await?;
                    Ok((parsed_query, multi_query.weight))
                        as Result<(ParsedQuery, f32), ServiceError>
                }
            }))
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
            ParsedQueryTypes::Multi(parsed_queries)
        }
    };

    if limit > dataset_config.MAX_LIMIT {
        return Err(ServiceError::BadRequest(format!(
            "Limit of {} is greater than the maximum limit of {}. Please reduce the limit.",
            limit, dataset_config.MAX_LIMIT
        ))
        .into());
    }

    let result_chunks = count_chunks_query(
        search_req_data.clone(),
        parsed_query,
        pool,
        dataset_org_plan_sub.dataset.clone(),
        &dataset_config,
    )
    .await?;

    Ok(HttpResponse::Ok().json(result_chunks))
}

/// Get Chunk By Tracking Id
///
/// Get a singular chunk by tracking_id. This is useful for when you are coordinating with an external system and want to use your own id as the primary reference for a chunk.
#[utoipa::path(
    get,
    path = "/chunk/tracking_id/{tracking_id}",
    context_path = "/api",
    tag = "Chunk",
    responses(
        (status = 200, description = "chunk with the tracking_id that you were searching for", body = ChunkReturnTypes),
        (status = 400, description = "Service error relating to fidning a chunk by tracking_id", body = ErrorResponseBody),
        (status = 404, description = "Chunk not found", body = ErrorResponseBody)
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
        ("X-API-Version" = Option<APIVersion>, Header, description = "The API version to use for this request. Defaults to V2 for orgs created after July 12, 2024 and V1 otherwise."),
        ("tracking_id" = Option<String>, Path, description = "tracking_id of the chunk you want to fetch"),
    ),
    security(
        ("ApiKey" = ["readonly"]),
    )
)]
pub async fn get_chunk_by_tracking_id(
    tracking_id: web::Path<String>,
    _user: LoggedUser,
    pool: web::Data<Pool>,
    api_version: APIVersion,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, ServiceError> {
    let dataset_configuration =
        DatasetConfiguration::from_json(dataset_org_plan_sub.dataset.server_configuration.clone());

    let chunk = get_metadata_from_tracking_id_query(
        tracking_id.into_inner(),
        dataset_org_plan_sub.dataset.id,
        pool,
    )
    .await?;
    let chunk_tag_set_string = ChunkMetadataStringTagSet::from(chunk);

    let point_id = chunk_tag_set_string.qdrant_point_id;

    let pointid_exists = point_ids_exists_in_qdrant(vec![point_id], dataset_configuration).await?;

    if pointid_exists {
        if api_version == APIVersion::V2 {
            return Ok(HttpResponse::Ok().json(ChunkMetadata::from(chunk_tag_set_string)));
        }
        Ok(HttpResponse::Ok().json(chunk_tag_set_string))
    } else {
        Err(ServiceError::NotFound("Chunk not found".to_string()))
    }
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct GetChunksData {
    pub ids: Vec<uuid::Uuid>,
}

/// Get Chunks By Ids
///
/// Get multiple chunks by multiple ids.
#[utoipa::path(
    post,
    path = "/chunks",
    context_path = "/api",
    tag = "Chunk",
    request_body(content = GetChunksData, description = "JSON request payload to get the chunks in the request", content_type = "application/json"),
    responses(
        (status = 200, description = "chunks with the id that you were searching for", body = Vec<ChunkReturnTypes>),
        (status = 400, description = "Service error relating to fidning a chunk by tracking_id", body = ErrorResponseBody),
        (status = 404, description = "Any one of the specified chunks not found", body = ErrorResponseBody)
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
        ("X-API-Version" = Option<APIVersion>, Header, description = "The API version to use for this request. Defaults to V2 for orgs created after July 12, 2024 and V1 otherwise.")
    ),
    security(
        ("ApiKey" = ["readonly"]),
    )
)]
pub async fn get_chunks_by_ids(
    chunk_payload: web::Json<GetChunksData>,
    _user: LoggedUser,
    pool: web::Data<Pool>,
    api_version: APIVersion,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, ServiceError> {
    let dataset_configuration =
        DatasetConfiguration::from_json(dataset_org_plan_sub.dataset.server_configuration.clone());

    let chunks = get_metadata_from_ids_query(
        chunk_payload.ids.clone(),
        dataset_org_plan_sub.dataset.id,
        pool,
    )
    .await?;
    let chunk_string_tag_sets = chunks
        .into_iter()
        .map(ChunkMetadataStringTagSet::from)
        .collect::<Vec<ChunkMetadataStringTagSet>>();

    let point_ids = chunk_string_tag_sets
        .iter()
        .map(|x| x.qdrant_point_id)
        .collect();

    let pointids_exists = point_ids_exists_in_qdrant(point_ids, dataset_configuration).await?;

    if pointids_exists {
        if api_version == APIVersion::V2 {
            return Ok(HttpResponse::Ok().json(
                chunk_string_tag_sets
                    .into_iter()
                    .map(ChunkMetadata::from)
                    .collect::<Vec<ChunkMetadata>>(),
            ));
        }
        Ok(HttpResponse::Ok().json(chunk_string_tag_sets))
    } else {
        Err(ServiceError::NotFound(
            "Any one of the specified chunks not found".to_string(),
        ))
    }
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct GetTrackingChunksData {
    pub tracking_ids: Vec<String>,
}

/// Get Chunks By Tracking Ids
///
/// Get multiple chunks by ids.
#[utoipa::path(
    post,
    path = "/chunks/tracking",
    context_path = "/api",
    tag = "Chunk",
    request_body(content = GetTrackingChunksData, description = "JSON request payload to get the chunks in the request", content_type = "application/json"),
    responses(
        (status = 200, description = "Chunks with one the ids which were specified", body = Vec<ChunkReturnTypes>),
        (status = 400, description = "Service error relating to finding a chunk by tracking_id", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
        ("X-API-Version" = Option<APIVersion>, Header, description = "The API version to use for this request. Defaults to V2 for orgs created after July 12, 2024 and V1 otherwise.")
    ),
    security(
        ("ApiKey" = ["readonly"]),
    )
)]
pub async fn get_chunks_by_tracking_ids(
    chunk_payload: web::Json<GetTrackingChunksData>,
    _user: LoggedUser,
    pool: web::Data<Pool>,
    api_version: APIVersion,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, ServiceError> {
    let dataset_configuration =
        DatasetConfiguration::from_json(dataset_org_plan_sub.dataset.server_configuration.clone());

    let chunks = get_metadata_from_tracking_ids_query(
        chunk_payload.tracking_ids.clone(),
        dataset_org_plan_sub.dataset.id,
        pool,
    )
    .await?;
    let chunk_string_tag_sets = chunks
        .into_iter()
        .map(ChunkMetadataStringTagSet::from)
        .collect::<Vec<ChunkMetadataStringTagSet>>();

    let point_ids = chunk_string_tag_sets
        .iter()
        .map(|x| x.qdrant_point_id)
        .collect();

    let pointids_exists = point_ids_exists_in_qdrant(point_ids, dataset_configuration).await?;

    if pointids_exists {
        if api_version == APIVersion::V2 {
            return Ok(HttpResponse::Ok().json(
                chunk_string_tag_sets
                    .into_iter()
                    .map(ChunkMetadata::from)
                    .collect::<Vec<ChunkMetadata>>(),
            ));
        }
        Ok(HttpResponse::Ok().json(chunk_string_tag_sets))
    } else {
        Err(ServiceError::NotFound(
            "Any one of the specified chunks not found".to_string(),
        ))
    }
}

#[derive(Serialize, Deserialize, Debug, ToSchema, Clone)]
pub struct RecommendChunksRequest {
    /// The ids of the chunks to be used as positive examples for the recommendation. The chunks in this array will be used to find similar chunks.
    pub positive_chunk_ids: Option<Vec<uuid::Uuid>>,
    /// The ids of the chunks to be used as negative examples for the recommendation. The chunks in this array will be used to filter out similar chunks.
    pub negative_chunk_ids: Option<Vec<uuid::Uuid>>,
    /// The tracking_ids of the chunks to be used as positive examples for the recommendation. The chunks in this array will be used to find similar chunks.
    pub positive_tracking_ids: Option<Vec<String>>,
    /// The tracking_ids of the chunks to be used as negative examples for the recommendation. The chunks in this array will be used to filter out similar chunks.
    pub negative_tracking_ids: Option<Vec<String>>,
    /// Strategy to use for recommendations, either "average_vector" or "best_score". The default is "average_vector". The "average_vector" strategy will construct a single average vector from the positive and negative samples then use it to perform a pseudo-search (You must provide at least one of either positive_chunk_ids or positive_tracking_ids for `average_vector` strategy). The "best_score" strategy is more advanced and navigates the HNSW with a heuristic of picking edges where the point is closer to the positive samples than it is the negatives for best score you can provide a list of only negatives or only positives or both.
    pub strategy: Option<RecommendationStrategy>,
    /// The type of recommendation to make. This lets you choose whether to recommend based off of `semantic` or `fulltext` similarity. The default is `semantic`.
    pub recommend_type: Option<RecommendType>,
    /// Filters to apply to the chunks to be recommended. This is a JSON object which contains the filters to apply to the chunks to be recommended. The default is None.
    pub filters: Option<ChunkFilter>,
    /// The number of chunks to return. This is the number of chunks which will be returned in the response. The default is 10.
    pub limit: Option<u64>,
    /// Set slim_chunks to true to avoid returning the content and chunk_html of the chunks. This is useful for when you want to reduce amount of data over the wire for latency improvement (typicall 10-50ms). Default is false.
    pub slim_chunks: Option<bool>,
    /// User ID is the id of the user who is making the request. This is used to track user interactions with the recommendation results.
    pub user_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(title = "V2")]
pub struct RecommendChunksResponseBody {
    pub id: uuid::Uuid,
    pub chunks: Vec<ScoreChunk>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(title = "V1")]
pub struct V1RecommendChunksResponseBody(pub Vec<ChunkMetadataWithScore>);

#[derive(Serialize, Deserialize, Debug, ToSchema)]
#[serde(untagged)]
pub enum RecommendResponseTypes {
    #[schema(title = "V2")]
    V2(RecommendChunksResponseBody),
    #[schema(title = "V1")]
    V1(V1RecommendChunksResponseBody),
}

/// Get Recommended Chunks
///
/// Get recommendations of chunks similar to the positive samples in the request and dissimilar to the negative.
#[utoipa::path(
    post,
    path = "/chunk/recommend",
    context_path = "/api",
    tag = "Chunk",
    request_body(content = RecommendChunksRequest, description = "JSON request payload to get recommendations of chunks similar to the chunks in the request", content_type = "application/json"),
    responses(
        (status = 200, description = "Chunks with embedding vectors which are similar to positives and dissimilar to negatives", body = RecommendResponseTypes),
        (status = 400, description = "Service error relating to to getting similar chunks", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
        ("X-API-Version" = Option<APIVersion>, Header, description = "The API version to use for this request. Defaults to V2 for orgs created after July 12, 2024 and V1 otherwise.")
    ),
    security(
        ("ApiKey" = ["readonly"]),
    )
)]
pub async fn get_recommended_chunks(
    data: web::Json<RecommendChunksRequest>,
    pool: web::Data<Pool>,
    _user: LoggedUser,
    event_queue: web::Data<EventQueue>,
    api_version: APIVersion,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, actix_web::Error> {
    let positive_chunk_ids = data.positive_chunk_ids.clone();
    let negative_chunk_ids = data.negative_chunk_ids.clone();
    let positive_tracking_ids = data.positive_tracking_ids.clone();
    let negative_tracking_ids = data.negative_tracking_ids.clone();
    let limit = data.limit.unwrap_or(10);
    let dataset_config =
        DatasetConfiguration::from_json(dataset_org_plan_sub.dataset.server_configuration);

    if positive_chunk_ids.is_none()
        && positive_tracking_ids.is_none()
        && data
            .strategy
            .clone()
            .unwrap_or(RecommendationStrategy::AverageVector)
            == RecommendationStrategy::AverageVector
    {
        return Err(ServiceError::BadRequest(
            "Either positive_chunk_ids or positive_tracking_ids must be provided for average_vector strategy".to_string(),
        )
        .into());
    }

    let dataset_id = dataset_org_plan_sub.dataset.id;

    let mut positive_qdrant_ids = vec![];

    let mut timer = Timer::new();

    timer.add("start extending tracking_ids and chunk_ids to qdrant_point_ids");

    if let Some(positive_chunk_ids) = positive_chunk_ids.clone() {
        positive_qdrant_ids.extend(
            get_point_ids_from_unified_chunk_ids(
                positive_chunk_ids
                    .into_iter()
                    .map(UnifiedId::TrieveUuid)
                    .collect(),
                dataset_id,
                pool.clone(),
            )
            .await
            .map_err(|err| {
                ServiceError::BadRequest(format!(
                    "Could not get positive qdrant_point_ids from positive_chunk_ids: {}",
                    err
                ))
            })?,
        )
    }

    if let Some(positive_chunk_tracking_ids) = positive_tracking_ids.clone() {
        positive_qdrant_ids.extend(
            get_point_ids_from_unified_chunk_ids(
                positive_chunk_tracking_ids
                    .into_iter()
                    .map(UnifiedId::TrackingId)
                    .collect(),
                dataset_id,
                pool.clone(),
            )
            .await
            .map_err(|err| {
                ServiceError::BadRequest(format!(
                    "Could not get positive qdrant_point_ids from positive_tracking_ids: {}",
                    err
                ))
            })?,
        )
    }

    let mut negative_qdrant_ids = vec![];

    if let Some(negative_chunk_ids) = negative_chunk_ids.clone() {
        negative_qdrant_ids.extend(
            get_point_ids_from_unified_chunk_ids(
                negative_chunk_ids
                    .into_iter()
                    .map(UnifiedId::TrieveUuid)
                    .collect(),
                dataset_id,
                pool.clone(),
            )
            .await
            .map_err(|err| {
                ServiceError::BadRequest(format!(
                    "Could not get negative qdrant_point_ids from negative_chunk_ids: {}",
                    err
                ))
            })?,
        )
    }

    if let Some(negative_chunk_tracking_ids) = negative_tracking_ids.clone() {
        negative_qdrant_ids.extend(
            get_point_ids_from_unified_chunk_ids(
                negative_chunk_tracking_ids
                    .into_iter()
                    .map(UnifiedId::TrackingId)
                    .collect(),
                dataset_id,
                pool.clone(),
            )
            .await
            .map_err(|err| {
                ServiceError::BadRequest(format!(
                    "Could not get negative qdrant_point_ids from negative_tracking_ids: {}",
                    err
                ))
            })?,
        )
    }

    if positive_qdrant_ids.is_empty()
        && data.strategy == Some(RecommendationStrategy::AverageVector)
    {
        return Err(
            ServiceError::BadRequest(
                "Positive chunk ids could not be found, must return at least 1 positive id for average vector strategy".to_string()
            ).into(),
        );
    } else if positive_qdrant_ids.is_empty() && negative_qdrant_ids.is_empty() {
        return Err(
            ServiceError::BadRequest(
                "No positive or negative chunk ids could be found. At least one positive or negative id must be provided.".to_string()
            ).into(),
        );
    }

    timer.add("fetched ids from postgres");

    let recommended_qdrant_results = recommend_qdrant_query(
        positive_qdrant_ids,
        negative_qdrant_ids,
        data.strategy.clone(),
        data.recommend_type.clone(),
        data.filters.clone(),
        limit,
        dataset_org_plan_sub.dataset.id,
        dataset_config.clone(),
        pool.clone(),
    )
    .await
    .map_err(|err| {
        ServiceError::BadRequest(format!("Could not get recommended chunks: {}", err))
    })?;

    timer.add("recommend_qdrant_query");

    let recommended_chunk_metadatas = match data.slim_chunks {
        Some(true) => get_slim_chunks_from_point_ids_query(
            recommended_qdrant_results
                .clone()
                .into_iter()
                .map(|recommend_qdrant_result| recommend_qdrant_result.point_id)
                .collect(),
            pool,
        )
        .await
        .map_err(|err| {
            ServiceError::BadRequest(format!(
                "Could not get recommended slim chunk_metadatas from qdrant_point_ids: {}",
                err
            ))
        })?,
        _ => get_chunk_metadatas_from_point_ids(
            recommended_qdrant_results
                .clone()
                .into_iter()
                .map(|recommend_qdrant_result| recommend_qdrant_result.point_id)
                .collect(),
            pool,
        )
        .await
        .map_err(|err| {
            ServiceError::BadRequest(format!(
                "Could not get recommended chunk_metadatas from qdrant_point_ids: {}",
                err
            ))
        })?,
    };

    let recommended_chunk_metadatas_with_score = recommended_chunk_metadatas
        .into_iter()
        .map(|chunk_metadata| {
            let score = recommended_qdrant_results
                .iter()
                .find(|recommend_qdrant_result| {
                    recommend_qdrant_result.point_id == chunk_metadata.qdrant_point_id()
                })
                .map(|recommend_qdrant_result| recommend_qdrant_result.score)
                .unwrap_or(0.0);

            ScoreChunk {
                chunk: chunk_metadata.into(),
                highlights: None,
                score,
            }
        })
        .collect::<Vec<ScoreChunk>>();

    let recommended_score_chunks = recommended_chunk_metadatas_with_score
        .into_iter()
        .sorted_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .collect::<Vec<ScoreChunk>>();

    timer.add("fetched metadata from point_ids");

    let recommendation_id = uuid::Uuid::new_v4();
    if !dataset_config.DISABLE_ANALYTICS {
        let clickhouse_event = RecommendationEventClickhouse {
            id: recommendation_id,
            recommendation_type: String::from("chunk"),
            positive_ids: positive_chunk_ids
                .unwrap_or_default()
                .into_iter()
                .map(|x| x.to_string())
                .collect(),
            negative_ids: negative_chunk_ids
                .unwrap_or_default()
                .into_iter()
                .map(|x| x.to_string())
                .collect(),
            positive_tracking_ids: positive_tracking_ids.unwrap_or_default(),
            negative_tracking_ids: negative_tracking_ids.unwrap_or_default(),
            request_params: serde_json::to_string(&data.clone()).unwrap_or_default(),
            top_score: recommended_score_chunks
                .first()
                .map(|x| x.score)
                .unwrap_or(0.0),
            results: recommended_score_chunks
                .iter()
                .map(|x| serde_json::to_string(x).unwrap_or_default())
                .collect(),
            dataset_id: dataset_org_plan_sub.dataset.id,
            created_at: time::OffsetDateTime::now_utc(),
            user_id: data.user_id.clone().unwrap_or_default(),
        };

        event_queue
            .send(ClickHouseEvent::RecommendationEvent(clickhouse_event))
            .await;
    }
    timer.add("send_to_clickhouse");

    if data.slim_chunks.unwrap_or(false) {
        let res = recommended_score_chunks
            .into_iter()
            .map(|chunk| chunk.into())
            .collect::<Vec<SlimChunkMetadataWithScore>>();

        return Ok(HttpResponse::PartialContent().json(res));
    }

    if api_version == APIVersion::V2 {
        let new_payload = RecommendChunksResponseBody {
            id: recommendation_id,
            chunks: recommended_score_chunks,
        };

        return Ok(HttpResponse::Ok()
            .insert_header((Timer::header_key(), timer.header_value()))
            .json(new_payload));
    }

    Ok(HttpResponse::Ok()
        .insert_header((Timer::header_key(), timer.header_value()))
        .json(recommended_score_chunks))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "prev_messages": [
        {
            "role": "user",
            "content": "How do I setup RAG with Trieve?",
        }
    ],
    "chunk_ids": ["d290f1ee-6c54-4b01-90e6-d701748f0851"],
    "prompt": "Respond to the instruction and include the doc numbers that you used in square brackets at the end of the sentences that you used the docs for:",
    "stream_response": true
}))]
pub struct GenerateOffChunksReqPayload {
    /// The previous messages to be placed into the chat history. There must be at least one previous message.
    pub prev_messages: Vec<ChatMessageProxy>,
    /// The ids of the chunks to be retrieved and injected into the context window for RAG.
    pub chunk_ids: Vec<uuid::Uuid>,
    /// Prompt will be used to tell the model what to generate in the next message in the chat. The default is 'Respond to the previous instruction and include the doc numbers that you used in square brackets at the end of the sentences that you used the docs for:'. You can also specify an empty string to leave the final message alone such that your user's final message can be used as the prompt. See docs.trieve.ai or contact us for more information.
    pub prompt: Option<String>,
    /// Audio input to be used in the chat. This will be used to generate the audio tokens for the model. The default is None.
    pub audio_input: Option<String>,
    /// Image URLs to be used in the chat. These will be used to generate the image tokens for the model. The default is None.
    pub image_urls: Option<Vec<String>>,
    /// Whether or not to stream the response. If this is set to true or not included, the response will be a stream. If this is set to false, the response will be a normal JSON response. Default is true.
    pub stream_response: Option<bool>,
    /// Set highlight_results to false for a slight latency improvement (1-10ms). If not specified, this defaults to true. This will add `<mark><b>` tags to the chunk_html of the chunks to highlight matching splits.
    pub highlight_results: Option<bool>,
    /// What sampling temperature to use, between 0 and 2. Higher values like 0.8 will make the output more random, while lower values like 0.2 will make it more focused and deterministic. Default is 0.5.
    pub temperature: Option<f32>,
    /// Frequency penalty is a number between -2.0 and 2.0. Positive values penalize new tokens based on their existing frequency in the text so far, decreasing the model's likelihood to repeat the same line verbatim. Default is 0.7.
    pub frequency_penalty: Option<f32>,
    /// Presence penalty is a number between -2.0 and 2.0. Positive values penalize new tokens based on whether they appear in the text so far, increasing the model's likelihood to talk about new topics. Default is 0.7.
    pub presence_penalty: Option<f32>,
    /// The maximum number of tokens to generate in the chat completion. Default is None.
    pub max_tokens: Option<u32>,
    /// Stop tokens are up to 4 sequences where the API will stop generating further tokens. Default is None.
    pub stop_tokens: Option<Vec<String>>,
    /// User ID is the id of the user who is making the request. This is used to track user interactions with the RAG results.
    pub user_id: Option<String>,
    /// Configuration for sending images to the llm
    pub image_config: Option<ImageConfig>,
    /// Context options to use for the completion. If not specified, all options will default to false.
    pub context_options: Option<ContextOptions>,
}

/// RAG on Specified Chunks
///
/// This endpoint exists as an alternative to the topic+message resource pattern where our Trieve handles chat memory. With this endpoint, the user is responsible for providing the context window and the prompt and the conversation is ephemeral.
#[utoipa::path(
    post,
    path = "/chunk/generate",
    context_path = "/api",
    tag = "Chunk",
    request_body(content = GenerateOffChunksReqPayload, description = "JSON request payload to perform RAG on some chunks (chunks)", content_type = "application/json"),
    responses(
        (status = 200, description = "This will be a HTTP stream of a string, check the chat or search UI for an example how to process this. Response if streaming.",
            headers(
                ("TR-QueryID" = uuid::Uuid, description = "Query ID that is used for tracking analytics")
            )
        ),
        (status = 200, description = "This will be a JSON response of a string containing the LLM's generated inference. Response if not streaming.", body = String,
            headers(
                ("TR-QueryID" = uuid::Uuid, description = "Query ID that is used for tracking analytics")
            )
        ),
        (status = 400, description = "Service error relating to to updating chunk, likely due to conflicting tracking_id", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
    ),
    security(
        ("ApiKey" = ["readonly"]),
    )
)]
pub async fn generate_off_chunks(
    data: web::Json<GenerateOffChunksReqPayload>,
    pool: web::Data<Pool>,
    event_queue: web::Data<EventQueue>,
    #[cfg(feature = "hallucination-detection")] hallucination_detector: web::Data<
        HallucinationDetector,
    >,
    _user: LoggedUser,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, actix_web::Error> {
    let prev_messages = data.prev_messages.clone();

    if prev_messages.iter().len() < 1 {
        return Err(
            ServiceError::BadRequest("There needs to be at least 1 prior message".into()).into(),
        );
    };

    let chunk_ids = data.chunk_ids.clone();
    let stream_response = data.stream_response;
    let context_options = data.context_options.clone();

    let mut chunks =
        get_metadata_from_ids_query(chunk_ids, dataset_org_plan_sub.dataset.id, pool).await?;

    let dataset_config =
        DatasetConfiguration::from_json(dataset_org_plan_sub.dataset.server_configuration);

    let base_url = dataset_config.LLM_BASE_URL;
    let rag_prompt = dataset_config.RAG_PROMPT.clone();
    let chosen_model = dataset_config.LLM_DEFAULT_MODEL;

    let base_url = if base_url.is_empty() {
        "https://openrouter.ai/api/v1".into()
    } else {
        base_url
    };

    let llm_api_key = if !dataset_config.LLM_API_KEY.is_empty() {
        dataset_config.LLM_API_KEY.clone()
    } else if base_url.contains("openai.com") {
        get_env!("OPENAI_API_KEY", "OPENAI_API_KEY for openai should be set").into()
    } else {
        get_env!(
            "LLM_API_KEY",
            "LLM_API_KEY for openrouter or self-hosted should be set"
        )
        .into()
    };

    let client = Client {
        headers: None,
        project: None,
        api_key: llm_api_key,
        http_client: reqwest::Client::new(),
        base_url,
        organization: None,
    };

    let mut messages: Vec<ChatMessage> = vec![ChatMessage::System {
        content: ChatMessageContent::Text(dataset_config.SYSTEM_PROMPT),
        name: None,
    }];

    check_completion_param_validity(
        data.temperature,
        data.frequency_penalty,
        data.presence_penalty,
        data.stop_tokens.clone(),
    )?;

    chunks.sort_by(|a, b| {
        data.chunk_ids
            .iter()
            .position(|&id| id == a.id)
            .unwrap()
            .cmp(&data.chunk_ids.iter().position(|&id| id == b.id).unwrap())
    });

    chunks.iter().enumerate().for_each(|(idx, chunk_metadata)| {
        let content =
            convert_html_to_text(&(chunk_metadata.chunk_html.clone().unwrap_or_default()));

        messages.push(ChatMessage::User {
            content: ChatMessageContent::Text(format!(
                "{{'doc': {}, 'text': '{}', {}}}",
                idx + 1,
                if context_options
                    .as_ref()
                    .is_some_and(|x| x.include_links.unwrap_or(false))
                    && chunk_metadata.link.is_some()
                {
                    format!(
                        "'link': '{}'",
                        chunk_metadata.link.clone().unwrap_or_default()
                    )
                } else {
                    "".to_string()
                },
                content
            )),
            name: None,
        });

        if let Some(image_config) = &data.image_config {
            if image_config.use_images.unwrap_or(false) {
                if let Some(image_urls) = chunk_metadata.image_urls.clone() {
                    messages.push(ChatMessage::User {
                        name: None,
                        content: ChatMessageContent::ContentPart(
                            image_urls
                                .iter()
                                .filter_map(|image| image.clone())
                                .take(image_config.images_per_chunk.unwrap_or(5))
                                .map(|url| {
                                    ChatMessageContentPart::Image(ChatMessageImageContentPart {
                                        r#type: "image_url".to_string(),
                                        image_url: ImageUrlType {
                                            url: url.to_string(),
                                            detail: None,
                                        },
                                    })
                                })
                                .collect::<Vec<_>>(),
                        ),
                    });
                }
            }
        }
    });

    let last_prev_message = if let Some(audio_input) = data.audio_input.clone() {
        ChatMessageProxy {
            role: RoleProxy::User,
            content: get_text_from_audio(&audio_input).await?,
        }
    } else {
        prev_messages
            .last()
            .expect("There needs to be at least 1 prior message")
            .clone()
    };
    let mut prev_messages = prev_messages.clone();

    prev_messages.truncate(prev_messages.len() - 1);

    prev_messages
        .iter()
        .for_each(|message| messages.push(ChatMessage::from(message.clone())));

    if let Some(image_urls) = data.image_urls.clone() {
        if !image_urls.is_empty() {
            messages.push(ChatMessage::User {
                name: None,
                content: ChatMessageContent::ContentPart(
                    image_urls
                        .iter()
                        .map(|url| {
                            ChatMessageContentPart::Image(ChatMessageImageContentPart {
                                r#type: "image_url".to_string(),
                                image_url: ImageUrlType {
                                    url: url.to_string(),
                                    detail: None,
                                },
                            })
                        })
                        .chain(std::iter::once(ChatMessageContentPart::Text(
                            ChatMessageTextContentPart {
                                r#type: "text".to_string(),
                                text:
                                    "These are the images that the user provided with their query."
                                        .to_string(),
                            },
                        )))
                        .collect::<Vec<_>>(),
                ),
            });
        }
    }

    messages.push(ChatMessage::User {
        content: ChatMessageContent::Text(format!(
            "{} {}",
            rag_prompt,
            last_prev_message.content.clone()
        )),
        name: None,
    });

    let parameters = ChatCompletionParameters {
        model: chosen_model,
        stream: stream_response,
        messages,
        top_p: None,
        n: None,
        temperature: Some(data.temperature.unwrap_or(0.5)),
        frequency_penalty: Some(data.frequency_penalty.unwrap_or(0.7)),
        presence_penalty: Some(data.presence_penalty.unwrap_or(0.7)),
        stop: data
            .stop_tokens
            .as_ref()
            .map(|stop_tokens| StopToken::Array(stop_tokens.clone())),
        max_completion_tokens: data.max_tokens,
        logit_bias: None,
        user: None,
        response_format: None,
        tools: None,
        tool_choice: None,
        logprobs: None,
        top_logprobs: None,
        seed: None,
        ..Default::default()
    };
    let query_id = uuid::Uuid::new_v4();

    if !stream_response.unwrap_or(true) {
        let assistant_completion =
            client
                .chat()
                .create(parameters.clone())
                .await
                .map_err(|err| {
                    ServiceError::BadRequest(format!(
                        "Bad response from LLM server provider: {}",
                        err
                    ))
                })?;

        let completion_content = match assistant_completion.choices.get(0) {
            Some(choice) => match &choice.message {
                ChatMessage::Assistant {
                    content: Some(ChatMessageContent::Text(content)),
                    ..
                }
                | ChatMessage::User {
                    content: ChatMessageContent::Text(content),
                    ..
                }
                | ChatMessage::System {
                    content: ChatMessageContent::Text(content),
                    ..
                } => content.clone(),
                _ => {
                    log::error!(
                        "ChatMessage of first choice did not have text or was either Tool or Function {:?}",
                        choice
                    );
                    "ChatMessage of first did not have text or was either Tool or Function"
                        .to_string()
                }
            },
            None => {
                return Err(ServiceError::InternalServerError(
                    "Failed to get response completion; no choices".into(),
                )
                .into())
            }
        };
        if !dataset_config.DISABLE_ANALYTICS {
            #[cfg(feature = "hallucination-detection")]
            let score = {
                let docs = chunks
                    .iter()
                    .map(|x| x.chunk_html.clone().unwrap_or_default())
                    .collect::<Vec<String>>();
                hallucination_detector
                    .detect_hallucinations(&clean_markdown(&completion_content), &docs)
                    .await
                    .map_err(|err| {
                        ServiceError::BadRequest(format!(
                            "Failed to detect hallucinations: {}",
                            err
                        ))
                    })?
            };

            #[cfg(not(feature = "hallucination-detection"))]
            let score = DummyHallucinationScore {
                total_score: 0.0,
                detected_hallucinations: vec![],
            };

            let clickhouse_rag_event = RagQueryEventClickhouse {
                id: query_id,
                created_at: time::OffsetDateTime::now_utc(),
                dataset_id: dataset_org_plan_sub.dataset.id,
                search_id: uuid::Uuid::nil(),
                results: vec![],
                json_results: chunks
                    .clone()
                    .into_iter()
                    .map(|x| {
                        let mut json = serde_json::to_value(&x).unwrap_or_default();
                        escape_quotes(&mut json);
                        json.to_string()
                    })
                    .collect(),
                top_score: 0.0,
                user_message: format!("{} {}", rag_prompt, last_prev_message.content.clone()),
                query_rating: String::new(),
                rag_type: "chosen_chunks".to_string(),
                llm_response: completion_content.clone(),
                user_id: data.user_id.clone().unwrap_or_default(),
                hallucination_score: score.total_score,
                detected_hallucinations: score.detected_hallucinations,
            };

            event_queue
                .send(ClickHouseEvent::RagQueryEvent(clickhouse_rag_event))
                .await;
        }

        if data.audio_input.is_some() {
            return Ok(HttpResponse::Ok()
                .insert_header(("TR-QueryID", query_id.to_string()))
                .insert_header((
                    "X-TR-Query",
                    last_prev_message
                        .content
                        .clone()
                        .replace(|c: char| c.is_ascii_control(), ""),
                ))
                .json(completion_content));
        } else {
            return Ok(HttpResponse::Ok()
                .insert_header(("TR-QueryID", query_id.to_string()))
                .json(completion_content));
        };
    }

    let (s, r) = unbounded::<String>();
    let stream = client
        .chat()
        .create_stream(parameters.clone())
        .await
        .unwrap();

    let last_message_arb = last_prev_message.content.clone();
    let user_id = data.user_id.clone().unwrap_or_default();
    Arbiter::new().spawn(async move {
        let chunk_v: Vec<String> = r.iter().collect();
        let completion = chunk_v.join("");
        if !dataset_config.DISABLE_ANALYTICS {
            #[cfg(feature = "hallucination-detection")]
            let score = {
                let docs = chunks
                    .iter()
                    .map(|x| x.chunk_html.clone().unwrap_or_default())
                    .collect::<Vec<String>>();

                hallucination_detector
                    .detect_hallucinations(&clean_markdown(&completion), &docs)
                    .await
                    .unwrap_or(HallucinationScore {
                        total_score: 0.0,
                        proper_noun_score: 0.0,
                        number_mismatch_score: 0.0,
                        unknown_word_score: 0.0,
                        detected_hallucinations: vec![],
                    })
            };

            #[cfg(not(feature = "hallucination-detection"))]
            let score = DummyHallucinationScore {
                total_score: 0.0,
                detected_hallucinations: vec![],
            };

            let clickhouse_rag_event = RagQueryEventClickhouse {
                id: uuid::Uuid::new_v4(),
                created_at: time::OffsetDateTime::now_utc(),
                dataset_id: dataset_org_plan_sub.dataset.id,
                search_id: uuid::Uuid::nil(),
                results: vec![],
                json_results: chunks
                    .clone()
                    .into_iter()
                    .map(|x| {
                        let mut json = serde_json::to_value(&x).unwrap_or_default();
                        escape_quotes(&mut json);
                        json.to_string()
                    })
                    .collect(),
                top_score: 0.0,
                user_message: format!("{} {}", rag_prompt, last_message_arb.clone()),
                rag_type: "chosen_chunks".to_string(),
                query_rating: String::new(),
                llm_response: completion,
                user_id,
                hallucination_score: score.total_score,
                detected_hallucinations: score.detected_hallucinations,
            };

            event_queue
                .send(ClickHouseEvent::RagQueryEvent(clickhouse_rag_event))
                .await;
        }
    });

    let completion_stream = stream.map(move |response| -> Result<Bytes, actix_web::Error> {
        match response {
            Ok(response) => {
                let chat_content = {
                    match response.choices.get(0) {
                        Some(choice) => {
                            if choice.finish_reason.is_some() {
                                "".to_string()
                            } else {
                                match &choice.delta {
                                    DeltaChatMessage::Assistant {
                                        content: Some(ChatMessageContent::Text(text)),
                                        ..
                                    }
                                    | DeltaChatMessage::User {
                                        content: ChatMessageContent::Text(text),
                                        ..
                                    }
                                    | DeltaChatMessage::System {
                                        content: ChatMessageContent::Text(text),
                                        ..
                                    }
                                    | DeltaChatMessage::Untagged {
                                        content: Some(ChatMessageContent::Text(text)),
                                        ..
                                    } => text.clone(),
                                    _ => {
                                        log::error!("Delta of first choice did not have text or was either Tool or Function {:?}", choice);
                                        "Delta of first did not have text or was either Tool or Function".to_string()
                                    }
                                }
                            }
                        }
                        None => "Failed to get first stream completion choice".to_string(),
                    }
                };

                s.send(chat_content.clone()).unwrap();

                Ok(Bytes::from(chat_content))
            }
            Err(e) => Err(ServiceError::InternalServerError(format!(
                "Model Response Error. Please try again later. {:?}",
                e
            ))
            .into()),
        }
    });

    if data.audio_input.is_some() {
        return Ok(HttpResponse::Ok()
            .insert_header(("TR-QueryID", query_id.to_string()))
            .insert_header((
                "X-TR-Query",
                last_prev_message
                    .content
                    .clone()
                    .replace(|c: char| c.is_ascii_control(), ""),
            ))
            .streaming(completion_stream));
    } else {
        return Ok(HttpResponse::Ok()
            .insert_header(("TR-QueryID", query_id.to_string()))
            .streaming(completion_stream));
    };
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "chunk_html": "",
    "heading_remove_strings": ["###", "##", "#"],
    "body_remove_strings": ["Warning:", "Note:"]
}))]
pub struct ChunkHtmlContentReqPayload {
    /// The HTML content to be split into chunks
    pub chunk_html: String,
    /// Text strings to remove from headings when creating chunks for each page
    pub heading_remove_strings: Option<Vec<String>>,
    /// Text strings to remove from body when creating chunks for each page
    pub body_remove_strings: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "chunks": [
        {
            "headings": ["Title Heading", "Sub Heading 1", "Sub Sub Heading 1"],
            "body": "This is the body of the content"
        },
        {
            "headings": ["Title Heading", "Sub Heading 1", "Sub Sub Heading 2"],
            "body": "This is the body of the content"
        }
        // ...
    ]
}))]
pub struct SplitHtmlResponse {
    pub chunks: Vec<ChunkedContent>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "headings": ["Title Heading", "Sub Heading 1", "Last SubHeading"],
    "body": "This is the body of the content"
}))]
pub struct ChunkedContent {
    /// The headings of the content in order of when they appear
    pub headings: Vec<String>,
    /// The body of the content
    pub body: String,
}

/// Split HTML Content into Chunks
///
/// This endpoint receives a single html string and splits it into chunks based on the headings and
/// body content. The headings are split based on heading html tags. chunk_html has a maximum size
/// of 256Kb.
#[utoipa::path(
    post,
    path = "/chunk/split",
    context_path = "/api",
    tag = "Chunk",
    request_body(content = ChunkHtmlContentReqPayload, description = "JSON request payload to perform RAG on some chunks (chunks)", content_type = "application/json"),
    responses(
        (
            status = 200, description = "This will be a JSON response of the chunks split from the HTML content with the headings and body",
            body = SplitHtmlResponse,
        ),
        (
            status = 413, description = "Payload too large, if the HTML contnet is greater than 256Kb",
            body = ErrorResponseBody,
        ),
    ),
)]
pub async fn split_html_content(
    body: web::Json<ChunkHtmlContentReqPayload>,
) -> Result<HttpResponse, ServiceError> {
    if body.chunk_html.bytes().len() >= 262_144 {
        return Err(ServiceError::PayloadTooLarge(
            "The HTML content is too large".to_string(),
        ));
    }

    let chunked_content = crawl_operator::chunk_html(
        &body.chunk_html,
        body.heading_remove_strings.clone(),
        body.body_remove_strings.clone(),
    );

    Ok(HttpResponse::Ok().json(SplitHtmlResponse {
        chunks: chunked_content
            .into_iter()
            .map(|(heading, body)| ChunkedContent {
                headings: vec![heading],
                body,
            })
            .collect(),
    }))
}

pub fn check_completion_param_validity(
    temperature: Option<f32>,
    frequency_penalty: Option<f32>,
    presence_penalty: Option<f32>,
    stop_tokens: Option<Vec<String>>,
) -> Result<(), ServiceError> {
    if let Some(temperature) = temperature {
        if !(0.0..=2.0).contains(&temperature) {
            return Err(ServiceError::BadRequest(
                "Temperature must be between 0 and 2".to_string(),
            ));
        }
    }

    if let Some(frequency_penalty) = frequency_penalty {
        if !(-2.0..=2.0).contains(&frequency_penalty) {
            return Err(ServiceError::BadRequest(
                "Frequency penalty must be between -2.0 and 2.0".to_string(),
            ));
        }
    }

    if let Some(presence_penalty) = presence_penalty {
        if !(-2.0..=2.0).contains(&presence_penalty) {
            return Err(ServiceError::BadRequest(
                "Presence penalty must be between -2.0 and 2.0".to_string(),
            ));
        }
    }

    if let Some(stop_tokens) = stop_tokens {
        if stop_tokens.len() > 4 {
            return Err(ServiceError::BadRequest(
                "Stop tokens must be less than or equal to 4".to_string(),
            ));
        }
    }

    Ok(())
}

#[derive(Serialize, Deserialize, Debug, ToSchema, Clone)]
/// Options for including an openapi spec in the crawl
#[schema(title = "CrawlOpenAPIOptions")]
pub struct CrawlOpenAPIOptions {
    /// OpenAPI json schema to be processed alongside the site crawl
    pub openapi_schema_url: String,
    /// Tag to look for to determine if a page should create an openapi route chunk instead of chunks from heading-split of the HTML
    pub openapi_tag: String,
}

#[derive(Serialize, Deserialize, Debug, ToSchema, Clone)]
#[serde(rename_all = "snake_case")]
/// Interval at which specified site should be re-scraped
pub enum CrawlInterval {
    Daily,
    Weekly,
    Monthly,
}
