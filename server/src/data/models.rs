#![allow(clippy::extra_unused_lifetimes)]

use super::schema::*;
use crate::errors::ServiceError;
use crate::get_env;
use crate::handlers::analytics_handler::CTRDataRequestBody;
use crate::handlers::chunk_handler::{
    AutocompleteReqPayload, ChunkFilter, CrawlOpenAPIOptions, FullTextBoost, ScoringOptions,
    SearchChunksReqPayload, SemanticBoost,
};
use crate::handlers::chunk_handler::{CrawlInterval, ScrollChunksReqPayload};
use crate::handlers::file_handler::{
    CreatePresignedUrlForCsvJsonlReqPayload, UploadFileReqPayload,
};
use crate::handlers::group_handler::{SearchOverGroupsReqPayload, SearchWithinGroupReqPayload};
use crate::handlers::message_handler::{
    CreateMessageReqPayload, EditMessageReqPayload, RegenerateMessageReqPayload,
};

use crate::handlers::page_handler::PublicPageParameters;
use crate::operators::analytics_operator::{
    CTRRecommendationsWithClicksResponse, CTRRecommendationsWithoutClicksResponse,
    CTRSearchQueryWithClicksResponse, CTRSearchQueryWithoutClicksResponse, HeadQueryResponse,
    LatencyGraphResponse, PopularFiltersResponse, QueryCountResponse, RagQueryResponse,
    RecommendationsEventResponse, SearchClusterResponse, SearchQueryResponse,
    SearchUsageGraphResponse,
};
use crate::operators::chunk_operator::{
    get_metadata_from_id_query, get_metadata_from_ids_query, HighlightStrategy,
};
use crate::operators::parse_operator::convert_html_to_text;
use crate::operators::search_operator::{
    get_group_metadata_filter_condition, get_group_tag_set_filter_condition, GroupScoreChunk,
    ParsedQuery, SearchResult,
};
use actix_web::web;
use chrono::{DateTime, NaiveDateTime};
use clickhouse::Row;
use dateparser::DateTimeUtc;
use derive_more::Display;
use diesel::expression::ValidGrouping;
use diesel::{
    deserialize::{self as deserialize, FromSql},
    pg::sql_types::Jsonb,
    pg::Pg,
    pg::PgValue,
    serialize::{self as serialize, IsNull, Output, ToSql},
    sql_types::Text,
};
use itertools::Itertools;
use minijinja::Environment;
use openai_dive::v1::resources::chat::{ChatMessage, ChatMessageContent};
use qdrant_client::qdrant::value::Kind;
use qdrant_client::qdrant::{GeoBoundingBox, GeoLineString, GeoPoint, GeoPolygon, GeoRadius};
use qdrant_client::{prelude::Payload, qdrant, qdrant::RetrievedPoint};
use rand::Rng;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::Write;
use time::OffsetDateTime;
use utoipa::ToSchema;

// type alias to use in multiple places
pub type Pool = diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>;
pub type RedisPool = bb8_redis::bb8::Pool<bb8_redis::RedisConnectionManager>;
pub type Templates<'a> = web::Data<Environment<'a>>;

pub fn uuid_between(uuid1: uuid::Uuid, uuid2: uuid::Uuid) -> uuid::Uuid {
    let num1 = u128::from_be_bytes(*uuid1.as_bytes());
    let num2 = u128::from_be_bytes(*uuid2.as_bytes());

    let (min_num, max_num) = if num1 < num2 {
        (num1, num2)
    } else {
        (num2, num1)
    };

    let diff = max_num - min_num;
    let mut rng = rand::thread_rng();

    let random_offset = rng.gen_range(0..=diff);

    let result_num = min_num + random_offset;

    let result_bytes = result_num.to_be_bytes();

    uuid::Uuid::from_bytes(result_bytes)
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, Selectable, Clone, ToSchema)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "email": "developers@trieve.ai",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "name": "Trieve",
}))]
#[diesel(table_name = users)]
pub struct User {
    pub id: uuid::Uuid,
    pub email: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub name: Option<String>,
    pub oidc_subject: String,
}

impl User {
    pub fn from_details_with_id<S: Into<String>, T: Into<String>>(
        id: T,
        email: S,
        name: Option<S>,
    ) -> Self {
        User {
            id: uuid::Uuid::new_v4(),
            oidc_subject: id.into(),
            email: email.into(),
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            name: name.map(|n| n.into()),
        }
    }
}

#[derive(
    Debug, Serialize, Deserialize, Queryable, Selectable, Insertable, ValidGrouping, Clone, ToSchema,
)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "owner_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "name": "Trieve",
    "deleted": false,
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "dataset_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
}))]
#[diesel(table_name = topics)]
pub struct Topic {
    pub id: uuid::Uuid,
    pub name: String,
    pub deleted: bool,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub dataset_id: uuid::Uuid,
    pub owner_id: String,
}

impl Topic {
    pub fn from_details<S: Into<String>>(name: S, owner_id: S, dataset_id: uuid::Uuid) -> Self {
        Topic {
            id: uuid::Uuid::new_v4(),
            name: name.into(),
            deleted: false,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            dataset_id,
            owner_id: owner_id.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, Clone, ToSchema)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "topic_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "sort_order": 1,
    "content": "Hello, world!",
    "role": "user",
    "deleted": false,
    "prompt_tokens": 300,
    "completion_tokens": 300,
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "dataset_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
}))]
#[diesel(table_name = messages)]
pub struct Message {
    pub id: uuid::Uuid,
    pub topic_id: uuid::Uuid,
    pub sort_order: i32,
    pub content: String,
    pub role: String,
    pub deleted: bool,
    pub prompt_tokens: Option<i32>,
    pub completion_tokens: Option<i32>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub dataset_id: uuid::Uuid,
}

impl From<Message> for ChatMessage {
    fn from(message: Message) -> Self {
        match message.role.as_str() {
            "system" => ChatMessage::System {
                content: ChatMessageContent::Text(message.content),
                name: None,
            },
            "user" => ChatMessage::User {
                content: ChatMessageContent::Text(message.content),
                name: None,
            },
            _ => ChatMessage::Assistant {
                content: Some(ChatMessageContent::Text(message.content)),
                refusal: None,
                tool_calls: None,
                name: None,
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum RoleProxy {
    System,
    User,
    Assistant,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(example=json!({
    "role": "user",
    "content": "Hello, world!"
}))]
pub struct ChatMessageProxy {
    pub role: RoleProxy,
    pub content: String,
}

impl From<ChatMessageProxy> for ChatMessage {
    fn from(message: ChatMessageProxy) -> Self {
        match message.role {
            RoleProxy::System => ChatMessage::System {
                content: ChatMessageContent::Text(message.content),
                name: None,
            },
            RoleProxy::User => ChatMessage::User {
                content: ChatMessageContent::Text(message.content),
                name: None,
            },
            RoleProxy::Assistant => ChatMessage::Assistant {
                content: Some(ChatMessageContent::Text(message.content)),
                refusal: None,
                tool_calls: None,
                name: None,
            },
        }
    }
}

impl Message {
    #![allow(clippy::too_many_arguments)]
    pub fn from_details<S: Into<String>, T: Into<uuid::Uuid>>(
        content: S,
        topic_id: T,
        sort_order: i32,
        role: String,
        prompt_tokens: Option<i32>,
        completion_tokens: Option<i32>,
        dataset_id: T,
        message_id: uuid::Uuid,
    ) -> Self {
        Message {
            id: message_id,
            topic_id: topic_id.into(),
            sort_order,
            content: content.into(),
            role,
            deleted: false,
            prompt_tokens,
            completion_tokens,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            dataset_id: dataset_id.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, ToSchema)]
#[serde(untagged)]
pub enum GeoTypes {
    Int(i64),
    Float(f64),
}

impl From<GeoTypes> for f64 {
    fn from(val: GeoTypes) -> Self {
        match val {
            GeoTypes::Int(i) => i as f64,
            GeoTypes::Float(f) => f,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, ToSchema, AsExpression)]
#[diesel(sql_type = Jsonb)]
/// Location bias lets you rank your results by distance from a location. If not specified, this has no effect. Bias allows you to determine how much of an effect the location of chunks will have on the search results. If not specified, this defaults to 0.0. We recommend setting this to 1.0 for a gentle reranking of the results, >3.0 for a strong reranking of the results.
pub struct GeoInfoWithBias {
    pub location: GeoInfo,
    /// Bias lets you specify how much of an effect the location of chunks will have on the search results. If not specified, this defaults to 0.0. We recommend setting this to 1.0 for a gentle reranking of the results, >3.0 for a strong reranking of the results.
    pub bias: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, ToSchema, AsExpression)]
#[diesel(sql_type = Jsonb)]
/// Location that you want to use as the center of the search.
pub struct GeoInfo {
    pub lat: GeoTypes,
    pub lon: GeoTypes,
}

impl GeoInfo {
    pub fn haversine_distance_to(&self, other: &GeoInfo) -> f64 {
        let lat1: f64 = self.lat.into();
        let lon1: f64 = self.lon.into();
        let lat2: f64 = other.lat.into();
        let lon2: f64 = other.lon.into();

        let r = 6371.0; // Earth radius in km

        let d_lat = (lat2 - lat1).to_radians();
        let d_lon = (lon2 - lon1).to_radians();
        let lat1_rad = lat1.to_radians();
        let lat2_rad = lat2.to_radians();
        let a = (d_lat / 2.0).sin().powi(2)
            + lat1_rad.cos() * lat2_rad.cos() * (d_lon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
        r * c
    }
}

impl FromSql<Jsonb, Pg> for GeoInfo {
    fn from_sql(bytes: PgValue) -> deserialize::Result<Self> {
        let bytes = bytes.as_bytes();

        if bytes.get(0) != Some(&1) {
            return Err("Unsupported JSONB encoding version".into());
        }
        serde_json::from_slice(&bytes[1..]).map_err(Into::into)
    }
}

impl ToSql<Jsonb, Pg> for GeoInfo {
    fn to_sql(&self, out: &mut Output<Pg>) -> serialize::Result {
        out.write_all(&[1])?;
        serde_json::to_writer(out, self)
            .map(|_| IsNull::No)
            .map_err(Into::into)
    }
}

impl Default for GeoInfo {
    fn default() -> Self {
        GeoInfo {
            lat: GeoTypes::Float(0.0),
            lon: GeoTypes::Float(0.0),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct UpdateSpecificChunkMetadata {
    pub id: uuid::Uuid,
    pub link: Option<String>,
    pub qdrant_point_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub chunk_html: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub tracking_id: Option<String>,
    pub time_stamp: Option<NaiveDateTime>,
    pub dataset_id: uuid::Uuid,
    pub weight: f64,
    pub location: Option<GeoInfo>,
    pub image_urls: Option<Vec<Option<String>>>,
    pub tag_set: Option<Vec<Option<String>>>,
    pub num_value: Option<f64>,
}

impl From<UpdateSpecificChunkMetadata> for ChunkMetadata {
    fn from(update_specific_chunk_metadata: UpdateSpecificChunkMetadata) -> Self {
        ChunkMetadata {
            id: update_specific_chunk_metadata.id,
            link: update_specific_chunk_metadata.link,
            qdrant_point_id: update_specific_chunk_metadata.qdrant_point_id,
            created_at: update_specific_chunk_metadata.created_at,
            updated_at: update_specific_chunk_metadata.updated_at,
            chunk_html: update_specific_chunk_metadata.chunk_html,
            metadata: update_specific_chunk_metadata.metadata,
            tracking_id: update_specific_chunk_metadata.tracking_id,
            time_stamp: update_specific_chunk_metadata.time_stamp,
            dataset_id: update_specific_chunk_metadata.dataset_id,
            weight: update_specific_chunk_metadata.weight,
            location: update_specific_chunk_metadata.location,
            image_urls: update_specific_chunk_metadata.image_urls,
            tag_set: update_specific_chunk_metadata.tag_set,
            num_value: update_specific_chunk_metadata.num_value,
        }
    }
}

impl From<ChunkMetadata> for UpdateSpecificChunkMetadata {
    fn from(chunk_metadata: ChunkMetadata) -> Self {
        UpdateSpecificChunkMetadata {
            id: chunk_metadata.id,
            link: chunk_metadata.link,
            qdrant_point_id: chunk_metadata.qdrant_point_id,
            created_at: chunk_metadata.created_at,
            updated_at: chunk_metadata.updated_at,
            chunk_html: chunk_metadata.chunk_html,
            metadata: chunk_metadata.metadata,
            tracking_id: chunk_metadata.tracking_id,
            time_stamp: chunk_metadata.time_stamp,
            dataset_id: chunk_metadata.dataset_id,
            weight: chunk_metadata.weight,
            location: chunk_metadata.location,
            image_urls: chunk_metadata.image_urls,
            tag_set: chunk_metadata.tag_set,
            num_value: chunk_metadata.num_value,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(title = "V2", example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "link": "https://trieve.ai",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "tag_set": "[tag1,tag2]",
    "chunk_html": "<p>Hello, world!</p>",
    "metadata": {"key": "value"},
    "tracking_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "time_stamp": "2021-01-01 00:00:00.000",
    "dataset_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "weight": 0.5,
}))]
pub struct ChunkMetadata {
    /// Unique identifier of the chunk, auto-generated uuid created by Trieve
    pub id: uuid::Uuid,
    /// Link to the chunk, should be a URL
    pub link: Option<String>,
    #[serde(skip_serializing)]
    pub qdrant_point_id: uuid::Uuid,
    /// Timestamp of the creation of the chunk
    pub created_at: chrono::NaiveDateTime,
    /// Timestamp of the last update of the chunk
    pub updated_at: chrono::NaiveDateTime,
    /// HTML content of the chunk, can also be an arbitrary string which is not HTML
    pub chunk_html: Option<String>,
    /// Metadata of the chunk, can be any JSON object
    pub metadata: Option<serde_json::Value>,
    /// Tracking ID of the chunk, can be any string, determined by the user. Tracking ID's are unique identifiers for chunks within a dataset. They are designed to match the unique identifier of the chunk in the user's system.
    pub tracking_id: Option<String>,
    /// Timestamp of the chunk, can be any timestamp. Specified by the user.
    pub time_stamp: Option<NaiveDateTime>,
    /// ID of the dataset which the chunk belongs to
    pub dataset_id: uuid::Uuid,
    /// Weight of the chunk, can be any float. Used as a multiplier on a chunk's relevance score for ranking purposes.
    pub weight: f64,
    /// Location of the chunk, can be any GeoInfo object. Used for location-filtered searches.
    pub location: Option<GeoInfo>,
    /// Image URLs of the chunk, can be any list of strings. Used for image search and RAG.
    pub image_urls: Option<Vec<Option<String>>>,
    /// Tag set of the chunk, can be any list of strings. Used for tag-filtered searches.
    pub tag_set: Option<Vec<Option<String>>>,
    /// Numeric value of the chunk, can be any float. Can represent the most relevant numeric value of the chunk, such as a price, quantity in stock, rating, etc.
    pub num_value: Option<f64>,
}

impl Default for ChunkMetadata {
    fn default() -> Self {
        ChunkMetadata {
            id: uuid::Uuid::new_v4(),
            link: None,
            qdrant_point_id: uuid::Uuid::new_v4(),
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            chunk_html: None,
            metadata: None,
            tracking_id: None,
            time_stamp: None,
            dataset_id: uuid::Uuid::new_v4(),
            weight: 0.0,
            location: None,
            image_urls: None,
            tag_set: None,
            num_value: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Selectable, Queryable, Insertable, Clone)]
#[diesel(table_name = chunk_metadata)]
pub struct ChunkMetadataTable {
    pub id: uuid::Uuid,
    pub link: Option<String>,
    pub qdrant_point_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub chunk_html: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub tracking_id: Option<String>,
    pub time_stamp: Option<NaiveDateTime>,
    pub dataset_id: uuid::Uuid,
    pub weight: f64,
    pub location: Option<GeoInfo>,
    pub image_urls: Option<Vec<Option<String>>>,
    pub num_value: Option<f64>,
}

impl From<ChunkMetadata> for ChunkMetadataTable {
    fn from(chunk_metadata: ChunkMetadata) -> Self {
        Self {
            id: chunk_metadata.id,
            link: chunk_metadata.link,
            qdrant_point_id: chunk_metadata.qdrant_point_id,
            created_at: chunk_metadata.created_at,
            updated_at: chunk_metadata.updated_at,
            chunk_html: chunk_metadata.chunk_html,
            metadata: chunk_metadata.metadata,
            tracking_id: chunk_metadata.tracking_id,
            time_stamp: chunk_metadata.time_stamp,
            dataset_id: chunk_metadata.dataset_id,
            weight: chunk_metadata.weight,
            location: chunk_metadata.location,
            image_urls: chunk_metadata.image_urls,
            num_value: chunk_metadata.num_value,
        }
    }
}

impl ChunkMetadata {
    #[allow(clippy::too_many_arguments)]
    pub fn from_details(
        chunk_html: &Option<String>,
        link: &Option<String>,
        tag_set: &Option<Vec<Option<String>>>,
        qdrant_point_id: uuid::Uuid,
        metadata: Option<serde_json::Value>,
        tracking_id: Option<String>,
        time_stamp: Option<NaiveDateTime>,
        location: Option<GeoInfo>,
        image_urls: Option<Vec<String>>,
        dataset_id: uuid::Uuid,
        weight: f64,
        num_value: Option<f64>,
    ) -> Self {
        ChunkMetadata {
            id: uuid::Uuid::new_v4(),
            chunk_html: chunk_html.clone(),
            link: link.clone(),
            qdrant_point_id,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            tag_set: tag_set.clone(),
            metadata,
            tracking_id,
            time_stamp,
            location,
            dataset_id,
            weight,
            image_urls: image_urls.map(|urls| urls.into_iter().map(Some).collect()),
            num_value,
        }
    }

    pub fn from_table_and_tag_set(
        chunk_metadata_table: ChunkMetadataTable,
        tag_set: Vec<String>,
    ) -> Self {
        Self {
            id: chunk_metadata_table.id,
            chunk_html: chunk_metadata_table.chunk_html,
            link: chunk_metadata_table.link,
            qdrant_point_id: chunk_metadata_table.qdrant_point_id,
            created_at: chunk_metadata_table.created_at,
            updated_at: chunk_metadata_table.updated_at,
            tag_set: Some(tag_set.into_iter().map(Some).collect()),
            metadata: chunk_metadata_table.metadata,
            tracking_id: chunk_metadata_table.tracking_id,
            time_stamp: chunk_metadata_table.time_stamp,
            location: chunk_metadata_table.location,
            dataset_id: chunk_metadata_table.dataset_id,
            weight: chunk_metadata_table.weight,
            image_urls: chunk_metadata_table.image_urls,
            num_value: chunk_metadata_table.num_value,
        }
    }

    pub fn from_table_and_tag_set_option_string(
        chunk_metadata_table: ChunkMetadataTable,
        tag_set: Option<Vec<Option<String>>>,
    ) -> Self {
        Self {
            id: chunk_metadata_table.id,
            chunk_html: chunk_metadata_table.chunk_html,
            link: chunk_metadata_table.link,
            qdrant_point_id: chunk_metadata_table.qdrant_point_id,
            created_at: chunk_metadata_table.created_at,
            updated_at: chunk_metadata_table.updated_at,
            tag_set,
            metadata: chunk_metadata_table.metadata,
            tracking_id: chunk_metadata_table.tracking_id,
            time_stamp: chunk_metadata_table.time_stamp,
            location: chunk_metadata_table.location,
            dataset_id: chunk_metadata_table.dataset_id,
            weight: chunk_metadata_table.weight,
            image_urls: chunk_metadata_table.image_urls,
            num_value: chunk_metadata_table.num_value,
        }
    }
}

impl ChunkMetadata {
    #[allow(clippy::too_many_arguments)]
    pub fn from_details_with_id<T: Into<uuid::Uuid>>(
        id: T,
        chunk_html: Option<String>,
        link: &Option<String>,
        tag_set: &Option<Vec<Option<String>>>,
        qdrant_point_id: uuid::Uuid,
        metadata: Option<serde_json::Value>,
        tracking_id: Option<String>,
        time_stamp: Option<NaiveDateTime>,
        location: Option<GeoInfo>,
        image_urls: Option<Vec<String>>,
        dataset_id: uuid::Uuid,
        weight: f64,
        num_value: Option<f64>,
    ) -> Self {
        ChunkMetadata {
            id: id.into(),
            chunk_html: chunk_html.clone(),
            link: link.clone(),
            qdrant_point_id,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            tag_set: tag_set.clone(),
            metadata,
            tracking_id,
            time_stamp,
            location,
            dataset_id,
            weight,
            image_urls: image_urls.map(|urls| urls.into_iter().map(Some).collect()),
            num_value,
        }
    }
}

impl From<ChunkMetadataStringTagSet> for ChunkMetadata {
    fn from(chunk_metadata_string_tag_set: ChunkMetadataStringTagSet) -> Self {
        ChunkMetadata {
            id: chunk_metadata_string_tag_set.id,
            link: chunk_metadata_string_tag_set.link,
            qdrant_point_id: chunk_metadata_string_tag_set.qdrant_point_id,
            created_at: chunk_metadata_string_tag_set.created_at,
            updated_at: chunk_metadata_string_tag_set.updated_at,
            chunk_html: chunk_metadata_string_tag_set.chunk_html,
            metadata: chunk_metadata_string_tag_set.metadata,
            tracking_id: chunk_metadata_string_tag_set.tracking_id,
            time_stamp: chunk_metadata_string_tag_set.time_stamp,
            dataset_id: chunk_metadata_string_tag_set.dataset_id,
            weight: chunk_metadata_string_tag_set.weight,
            location: chunk_metadata_string_tag_set.location,
            image_urls: chunk_metadata_string_tag_set.image_urls,
            tag_set: chunk_metadata_string_tag_set.tag_set.map(|tags| {
                let tags: Vec<Option<String>> =
                    tags.split(',').map(|tag| Some(tag.to_string())).collect();
                if tags.get(0).unwrap_or(&Some("".to_string())) == &Some("".to_string()) {
                    vec![]
                } else {
                    tags
                }
            }),
            num_value: chunk_metadata_string_tag_set.num_value,
        }
    }
}

impl From<SlimChunkMetadata> for ChunkMetadata {
    fn from(slim_chunk: SlimChunkMetadata) -> Self {
        ChunkMetadata {
            id: slim_chunk.id,
            chunk_html: Some("".to_string()),
            link: slim_chunk.link,
            qdrant_point_id: slim_chunk.qdrant_point_id,
            created_at: slim_chunk.created_at,
            updated_at: slim_chunk.updated_at,
            tag_set: slim_chunk
                .tag_set
                .map(|tags| tags.split(',').map(|tag| Some(tag.to_string())).collect()),
            metadata: slim_chunk.metadata,
            tracking_id: slim_chunk.tracking_id,
            time_stamp: slim_chunk.time_stamp,
            location: slim_chunk.location,
            dataset_id: slim_chunk.dataset_id,
            weight: slim_chunk.weight,
            image_urls: slim_chunk.image_urls,
            num_value: slim_chunk.num_value,
        }
    }
}

impl From<ContentChunkMetadata> for ChunkMetadata {
    fn from(content_chunk: ContentChunkMetadata) -> Self {
        ChunkMetadata {
            id: content_chunk.id,
            chunk_html: content_chunk.chunk_html,
            link: None,
            qdrant_point_id: content_chunk.qdrant_point_id,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            tag_set: None,
            metadata: None,
            tracking_id: content_chunk.tracking_id,
            time_stamp: content_chunk.time_stamp,
            location: None,
            dataset_id: uuid::Uuid::new_v4(),
            weight: content_chunk.weight,
            image_urls: content_chunk.image_urls,
            num_value: content_chunk.num_value,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IngestSpecificChunkMetadata {
    pub id: uuid::Uuid,
    pub dataset_id: uuid::Uuid,
    pub qdrant_point_id: uuid::Uuid,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "content": "Hello, world!",
    "link": "https://trieve.ai",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "tag_set": "tag1,tag2",
    "chunk_html": "<p>Hello, world!</p>",
    "metadata": {"key": "value"},
    "tracking_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "time_stamp": "2021-01-01 00:00:00.000",
    "dataset_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "weight": 0.5,
    "score": 0.9,
}))]
#[schema(title = "V1")]
pub struct ChunkMetadataWithScore {
    pub id: uuid::Uuid,
    pub link: Option<String>,
    #[serde(skip)]
    pub qdrant_point_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub tag_set: Option<String>,
    pub chunk_html: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub tracking_id: Option<String>,
    pub time_stamp: Option<NaiveDateTime>,
    pub dataset_id: uuid::Uuid,
    pub weight: f64,
    pub score: f32,
}

impl From<ChunkMetadataWithScore> for ScoreChunk {
    fn from(val: ChunkMetadataWithScore) -> Self {
        let score = val.score;
        ScoreChunk {
            chunk: NewChunkMetadataTypes::Metadata(val.into()),
            highlights: None,
            score,
        }
    }
}

impl From<ChunkMetadataWithScore> for ChunkMetadataStringTagSet {
    fn from(val: ChunkMetadataWithScore) -> Self {
        ChunkMetadataStringTagSet {
            id: val.id,
            link: val.link,
            qdrant_point_id: val.qdrant_point_id,
            created_at: val.created_at,
            updated_at: val.updated_at,
            tag_set: val.tag_set,
            chunk_html: val.chunk_html,
            metadata: val.metadata,
            tracking_id: val.tracking_id,
            time_stamp: val.time_stamp,
            dataset_id: val.dataset_id,
            weight: val.weight,
            location: None,
            image_urls: None,
            num_value: None,
        }
    }
}

impl From<ChunkMetadataWithScore> for ChunkMetadata {
    fn from(val: ChunkMetadataWithScore) -> Self {
        ChunkMetadata {
            id: val.id,
            link: val.link,
            qdrant_point_id: val.qdrant_point_id,
            created_at: val.created_at,
            updated_at: val.updated_at,
            tag_set: val.tag_set.map(|tags| {
                tags.split(',')
                    .filter(|tag| !tag.is_empty()) // Filter out empty strings
                    .map(|tag| Some(tag.to_string()))
                    .collect()
            }),
            chunk_html: val.chunk_html,
            metadata: val.metadata,
            tracking_id: val.tracking_id,
            time_stamp: val.time_stamp,
            dataset_id: val.dataset_id,
            weight: val.weight,
            location: None,
            image_urls: None,
            num_value: None,
        }
    }
}

impl From<(ChunkMetadata, f32)> for ChunkMetadataWithScore {
    fn from((chunk, score): (ChunkMetadata, f32)) -> Self {
        ChunkMetadataWithScore {
            id: chunk.id,
            link: chunk.link,
            qdrant_point_id: chunk.qdrant_point_id,
            created_at: chunk.created_at,
            updated_at: chunk.updated_at,
            tag_set: chunk.tag_set.map(|tags| {
                tags.into_iter()
                    .map(|tag| tag.unwrap_or_default())
                    .join(",")
            }),
            chunk_html: chunk.chunk_html,
            metadata: chunk.metadata,
            tracking_id: chunk.tracking_id,
            time_stamp: chunk.time_stamp,
            dataset_id: chunk.dataset_id,
            weight: chunk.weight,
            score,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, ToSchema, Clone)]
#[schema(example = json!({
    "metadata": [
        {
            "id": "d290f1ee-6c54-4b01-90e6-d701748f0851",
            "content": "Some content",
            "link": "https://example.com",
            "chunk_html": "<p>Some HTML content</p>",
            "metadata": {"key1": "value1", "key2": "value2"},
            "time_stamp": "2021-01-01 00:00:00.000",
            "weight": 0.5,
        }
    ],
    "highlights": ["highlight is two tokens: high, light", "whereas hello is only one token: hello"],
    "score": 0.5
}))]
pub struct ScoreChunkDTO {
    pub metadata: Vec<ChunkMetadataTypes>,
    pub highlights: Option<Vec<String>>,
    pub score: f64,
}

#[derive(Serialize, Deserialize, Debug, ToSchema, Clone)]
#[schema(example = json!({
    "chunk": {
        "id": "d290f1ee-6c54-4b01-90e6-d701748f0851",
        "content": "Some content",
        "link": "https://example.com",
        "chunk_html": "<p>Some HTML content</p>",
        "metadata": {"key1": "value1", "key2": "value2"},
        "time_stamp": "2021-01-01 00:00:00.000",
        "weight": 0.5,
    },
    "highlights": ["highlight is two tokens: high, light", "whereas hello is only one token: hello"],
    "score": 0.5
}))]
#[schema(title = "V2")]
pub struct ScoreChunk {
    pub chunk: NewChunkMetadataTypes,
    pub highlights: Option<Vec<String>>,
    pub score: f32,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[schema(title = "SlimChunkMetadataWithArrayTagSet")]
pub struct SlimChunkMetadataWithArrayTagSet {
    pub id: uuid::Uuid,
    pub link: Option<String>,
    #[serde(skip)]
    pub qdrant_point_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub tag_set: Option<Vec<String>>,
    pub metadata: Option<serde_json::Value>,
    pub tracking_id: Option<String>,
    pub time_stamp: Option<NaiveDateTime>,
    pub location: Option<GeoInfo>,
    pub dataset_id: uuid::Uuid,
    pub weight: f64,
    pub image_urls: Option<Vec<Option<String>>>,
    pub num_value: Option<f64>,
}

impl From<SlimChunkMetadata> for SlimChunkMetadataWithArrayTagSet {
    fn from(slim_chunk_metadata: SlimChunkMetadata) -> Self {
        SlimChunkMetadataWithArrayTagSet {
            id: slim_chunk_metadata.id,
            link: slim_chunk_metadata.link,
            qdrant_point_id: slim_chunk_metadata.qdrant_point_id,
            created_at: slim_chunk_metadata.created_at,
            updated_at: slim_chunk_metadata.updated_at,
            tag_set: slim_chunk_metadata
                .tag_set
                .map(|tag| tag.split(',').map(|tag| tag.to_string()).collect()),
            metadata: slim_chunk_metadata.metadata,
            tracking_id: slim_chunk_metadata.tracking_id,
            time_stamp: slim_chunk_metadata.time_stamp,
            location: slim_chunk_metadata.location,
            dataset_id: slim_chunk_metadata.dataset_id,
            weight: slim_chunk_metadata.weight,
            image_urls: slim_chunk_metadata.image_urls,
            num_value: slim_chunk_metadata.num_value,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[serde(untagged)]
pub enum NewChunkMetadataTypes {
    ID(SlimChunkMetadataWithArrayTagSet),
    Metadata(ChunkMetadata),
    Content(ContentChunkMetadata),
}

impl From<ChunkMetadataTypes> for NewChunkMetadataTypes {
    fn from(val: ChunkMetadataTypes) -> Self {
        match val {
            ChunkMetadataTypes::ID(slim_chunk_metadata) => {
                NewChunkMetadataTypes::ID(slim_chunk_metadata.into())
            }
            ChunkMetadataTypes::Metadata(chunk_metadata) => {
                NewChunkMetadataTypes::Metadata(chunk_metadata.into())
            }
            ChunkMetadataTypes::Content(content_chunk_metadata) => {
                NewChunkMetadataTypes::Content(content_chunk_metadata)
            }
        }
    }
}

impl ScoreChunkDTO {
    pub fn slim(&self) -> ScoreChunkDTO {
        let mut slim_chunk_dto = self.clone();
        slim_chunk_dto.metadata = slim_chunk_dto
            .metadata
            .into_iter()
            .map(|metadata| match metadata {
                ChunkMetadataTypes::ID(slim_chunk_metadata) => {
                    ChunkMetadataTypes::ID(slim_chunk_metadata)
                }
                ChunkMetadataTypes::Metadata(chunk_metadata) => {
                    ChunkMetadataTypes::ID(chunk_metadata.into())
                }
                ChunkMetadataTypes::Content(content_chunk_metadata) => {
                    ChunkMetadataTypes::ID(content_chunk_metadata.into())
                }
            })
            .collect();
        slim_chunk_dto
    }
}

impl From<ScoreChunkDTO> for ScoreChunk {
    fn from(score_chunk_dto: ScoreChunkDTO) -> Self {
        ScoreChunk {
            chunk: score_chunk_dto.metadata[0].clone().into(),
            highlights: score_chunk_dto.highlights,
            score: score_chunk_dto.score as f32,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, ToSchema, Clone)]
#[serde(untagged)]
pub enum ChunkMetadataTypes {
    ID(SlimChunkMetadata),
    Metadata(ChunkMetadataStringTagSet),
    Content(ContentChunkMetadata),
}

#[derive(Serialize, Deserialize, Debug, ToSchema, Clone)]
pub struct QdrantChunkMetadata {
    pub link: Option<String>,
    pub qdrant_point_id: uuid::Uuid,
    pub chunk_html: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub tracking_id: Option<String>,
    pub time_stamp: Option<NaiveDateTime>,
    pub dataset_id: uuid::Uuid,
    pub weight: f64,
    pub location: Option<GeoInfo>,
    pub image_urls: Option<Vec<String>>,
    pub tag_set: Option<Vec<String>>,
    pub num_value: Option<f64>,
    pub group_ids: Option<Vec<uuid::Uuid>>,
}

impl From<SearchResult> for QdrantChunkMetadata {
    fn from(search_result: SearchResult) -> Self {
        let link: Option<String> = match search_result.payload.get("link") {
            Some(qdrant::Value {
                kind: Some(Kind::StringValue(link)),
                ..
            }) => Some(link.clone()),
            _ => None,
        };
        let chunk_html: Option<String> = match search_result.payload.get("content") {
            Some(qdrant::Value {
                kind: Some(Kind::StringValue(content)),
                ..
            }) => Some(content.clone()),
            _ => None,
        };
        let metadata: Option<serde_json::Value> = match search_result.payload.get("metadata") {
            Some(qdrant::Value {
                kind: Some(Kind::StructValue(metadata)),
                ..
            }) => {
                let mut metadata_map = serde_json::Map::new();
                for (key, value) in metadata.fields.iter() {
                    match value {
                        qdrant::Value {
                            kind: Some(Kind::StringValue(value)),
                            ..
                        } => {
                            metadata_map
                                .insert(key.clone(), serde_json::Value::String(value.clone()));
                        }
                        qdrant::Value {
                            kind: Some(Kind::IntegerValue(value)),
                            ..
                        } => {
                            metadata_map.insert(
                                key.clone(),
                                serde_json::Value::Number(
                                    serde_json::Number::from_f64(*value as f64).unwrap(),
                                ),
                            );
                        }
                        _ => {}
                    }
                }
                Some(serde_json::Value::Object(metadata_map))
            }
            _ => None,
        };
        let tracking_id: Option<String> = match search_result.payload.get("tracking_id") {
            Some(qdrant::Value {
                kind: Some(Kind::StringValue(tracking_id)),
                ..
            }) => Some(tracking_id.clone()),
            _ => None,
        };
        let time_stamp: Option<NaiveDateTime> = match search_result.payload.get("time_stamp") {
            Some(qdrant::Value {
                kind: Some(Kind::StringValue(time_stamp)),
                ..
            }) => Some(NaiveDateTime::parse_from_str(time_stamp, "%Y-%m-%d %H:%M:%S%.f").unwrap()),
            _ => None,
        };
        let dataset_id: uuid::Uuid = match search_result.payload.get("dataset_id") {
            Some(qdrant::Value {
                kind: Some(Kind::StringValue(dataset_id)),
                ..
            }) => uuid::Uuid::parse_str(dataset_id).unwrap(),
            _ => uuid::Uuid::new_v4(),
        };
        let weight: f64 = match search_result.payload.get("weight") {
            Some(qdrant::Value {
                kind: Some(Kind::IntegerValue(weight)),
                ..
            }) => *weight as f64,
            Some(qdrant::Value {
                kind: Some(Kind::DoubleValue(weight)),
                ..
            }) => *weight,
            _ => 0 as f64,
        };
        let location = match search_result.payload.get("location") {
            Some(qdrant::Value {
                kind: Some(Kind::StructValue(location)),
                ..
            }) => {
                let lat = match location.fields.get("lat") {
                    Some(qdrant::Value {
                        kind: Some(Kind::DoubleValue(lat)),
                        ..
                    }) => *lat,
                    _ => 0.0,
                };
                let lon = match location.fields.get("lon") {
                    Some(qdrant::Value {
                        kind: Some(Kind::DoubleValue(lon)),
                        ..
                    }) => *lon,
                    _ => 0.0,
                };
                Some(GeoInfo {
                    lat: GeoTypes::Float(lat),
                    lon: GeoTypes::Float(lon),
                })
            }
            _ => None,
        };
        let images_urls: Option<Vec<String>> = match search_result.payload.get("image_urls") {
            Some(qdrant::Value {
                kind: Some(Kind::ListValue(image_urls)),
                ..
            }) => Some(
                image_urls
                    .iter()
                    .map(|url| match url {
                        qdrant::Value {
                            kind: Some(Kind::StringValue(url)),
                            ..
                        } => url.clone(),
                        _ => "".to_string(),
                    })
                    .collect(),
            ),
            _ => None,
        };
        let tag_set: Option<Vec<String>> = match search_result.payload.get("tag_set") {
            Some(qdrant::Value {
                kind: Some(Kind::ListValue(tag_set)),
                ..
            }) => Some(
                tag_set
                    .iter()
                    .map(|url| match url {
                        qdrant::Value {
                            kind: Some(Kind::StringValue(url)),
                            ..
                        } => url.clone(),
                        _ => "".to_string(),
                    })
                    .collect(),
            ),
            _ => None,
        };
        let num_value: Option<f64> = match search_result.payload.get("num_value") {
            Some(qdrant::Value {
                kind: Some(Kind::IntegerValue(num_value)),
                ..
            }) => Some(*num_value as f64),
            Some(qdrant::Value {
                kind: Some(Kind::DoubleValue(num_value)),
                ..
            }) => Some(*num_value),
            _ => None,
        };
        let group_ids: Option<Vec<uuid::Uuid>> = match search_result.payload.get("group_ids") {
            Some(qdrant::Value {
                kind: Some(Kind::ListValue(group_ids)),
                ..
            }) => Some(
                group_ids
                    .iter()
                    .filter_map(|id| match id {
                        qdrant::Value {
                            kind: Some(Kind::StringValue(id)),
                            ..
                        } => uuid::Uuid::parse_str(id).ok(),
                        _ => None,
                    })
                    .collect(),
            ),
            _ => None,
        };

        QdrantChunkMetadata {
            link,
            qdrant_point_id: search_result.point_id,
            chunk_html,
            metadata,
            tracking_id,
            time_stamp,
            dataset_id,
            weight,
            location,
            image_urls: images_urls,
            tag_set,
            num_value,
            group_ids,
        }
    }
}

impl From<ChunkMetadataTypes> for ChunkMetadata {
    fn from(val: ChunkMetadataTypes) -> Self {
        match val {
            ChunkMetadataTypes::ID(slim_chunk_metadata) => slim_chunk_metadata.into(),
            ChunkMetadataTypes::Metadata(chunk_metadata) => chunk_metadata.into(),
            ChunkMetadataTypes::Content(content_chunk_metadata) => content_chunk_metadata.into(),
        }
    }
}

impl From<NewChunkMetadataTypes> for ChunkMetadata {
    fn from(val: NewChunkMetadataTypes) -> Self {
        match val {
            NewChunkMetadataTypes::ID(slim_chunk_metadata_with_array_tag_set) => {
                ChunkMetadataStringTagSet::from(slim_chunk_metadata_with_array_tag_set).into()
            }
            NewChunkMetadataTypes::Metadata(chunk_metadata) => chunk_metadata,
            NewChunkMetadataTypes::Content(content_chunk_metadata) => content_chunk_metadata.into(),
        }
    }
}

impl From<ChunkMetadata> for ChunkMetadataTypes {
    fn from(chunk_metadata: ChunkMetadata) -> Self {
        ChunkMetadataTypes::Metadata(chunk_metadata.into())
    }
}

impl From<SlimChunkMetadata> for ChunkMetadataTypes {
    fn from(slim_chunk_metadata: SlimChunkMetadata) -> Self {
        ChunkMetadataTypes::ID(slim_chunk_metadata)
    }
}

impl From<ContentChunkMetadata> for ChunkMetadataTypes {
    fn from(content_chunk_metadata: ContentChunkMetadata) -> Self {
        ChunkMetadataTypes::Content(content_chunk_metadata)
    }
}

impl ChunkMetadataTypes {
    pub fn metadata(&self) -> ChunkMetadata {
        match self {
            ChunkMetadataTypes::Metadata(metadata) => metadata.clone().into(),
            ChunkMetadataTypes::ID(slim_metadata) => slim_metadata.clone().into(),
            ChunkMetadataTypes::Content(content_metadata) => content_metadata.clone().into(),
        }
    }

    pub fn set_chunk_html(self, chunk_html: Option<String>) -> Self {
        match self {
            ChunkMetadataTypes::Metadata(mut metadata) => {
                metadata.chunk_html = chunk_html;
                ChunkMetadataTypes::Metadata(metadata)
            }
            ChunkMetadataTypes::Content(mut content_metadata) => {
                content_metadata.chunk_html = chunk_html;
                ChunkMetadataTypes::Content(content_metadata)
            }
            ChunkMetadataTypes::ID(slim_chunk) => ChunkMetadataTypes::ID(slim_chunk),
        }
    }

    pub fn chunk_html(&self) -> Option<String> {
        match self {
            ChunkMetadataTypes::Metadata(metadata) => metadata.chunk_html.clone(),
            ChunkMetadataTypes::Content(content_metadata) => content_metadata.chunk_html.clone(),
            ChunkMetadataTypes::ID(_) => None,
        }
    }

    pub fn dataset_id(&self) -> Option<uuid::Uuid> {
        match self {
            ChunkMetadataTypes::Metadata(metadata) => Some(metadata.dataset_id),
            ChunkMetadataTypes::ID(slim_metadata) => Some(slim_metadata.dataset_id),
            ChunkMetadataTypes::Content(_) => None,
        }
    }

    pub fn qdrant_point_id(&self) -> uuid::Uuid {
        match self {
            ChunkMetadataTypes::Metadata(metadata) => metadata.qdrant_point_id,
            ChunkMetadataTypes::ID(slim_metadata) => slim_metadata.qdrant_point_id,
            ChunkMetadataTypes::Content(content_metadata) => content_metadata.qdrant_point_id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "link": "https://trieve.ai",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "tag_set": "tag1,tag2",
    "metadata": {"key": "value"},
    "tracking_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "time_stamp": "2021-01-01 00:00:00.000",
    "dataset_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "weight": 0.5,
    "score": 0.9,
}))]
pub struct SlimChunkMetadataWithScore {
    pub id: uuid::Uuid,
    pub link: Option<String>,
    #[serde(skip)]
    pub qdrant_point_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub tag_set: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub tracking_id: Option<String>,
    pub time_stamp: Option<NaiveDateTime>,
    pub weight: f64,
    pub score: f32,
}

impl From<ChunkMetadataWithScore> for SlimChunkMetadataWithScore {
    fn from(chunk: ChunkMetadataWithScore) -> Self {
        SlimChunkMetadataWithScore {
            id: chunk.id,
            link: chunk.link,
            qdrant_point_id: chunk.qdrant_point_id,
            created_at: chunk.created_at,
            updated_at: chunk.updated_at,
            tag_set: chunk.tag_set,
            metadata: chunk.metadata,
            tracking_id: chunk.tracking_id,
            time_stamp: chunk.time_stamp,
            weight: chunk.weight,
            score: chunk.score,
        }
    }
}

impl From<ScoreChunk> for SlimChunkMetadataWithScore {
    fn from(chunk: ScoreChunk) -> Self {
        match chunk.chunk {
            NewChunkMetadataTypes::Metadata(metadata) => SlimChunkMetadataWithScore {
                id: metadata.id,
                link: metadata.link,
                qdrant_point_id: metadata.qdrant_point_id,
                created_at: metadata.created_at,
                updated_at: metadata.updated_at,
                tag_set: metadata.tag_set.map(|tags| {
                    tags.into_iter()
                        .map(|tag| tag.unwrap_or_default())
                        .filter(|tag| !tag.is_empty())
                        .join(",")
                }),
                metadata: metadata.metadata,
                tracking_id: metadata.tracking_id,
                time_stamp: metadata.time_stamp,
                weight: metadata.weight,
                score: chunk.score,
            },
            NewChunkMetadataTypes::ID(slim_metadata) => SlimChunkMetadataWithScore {
                id: slim_metadata.id,
                link: slim_metadata.link,
                qdrant_point_id: slim_metadata.qdrant_point_id,
                created_at: slim_metadata.created_at,
                updated_at: slim_metadata.updated_at,
                tag_set: slim_metadata.tag_set.map(|tags| tags.into_iter().join(",")),
                metadata: slim_metadata.metadata,
                tracking_id: slim_metadata.tracking_id,
                time_stamp: slim_metadata.time_stamp,
                weight: slim_metadata.weight,
                score: chunk.score,
            },
            NewChunkMetadataTypes::Content(content_metadata) => SlimChunkMetadataWithScore {
                id: content_metadata.id,
                link: None,
                qdrant_point_id: content_metadata.qdrant_point_id,
                created_at: chrono::Utc::now().naive_local(),
                updated_at: chrono::Utc::now().naive_local(),
                tag_set: None,
                metadata: None,
                tracking_id: content_metadata.tracking_id,
                time_stamp: content_metadata.time_stamp,
                weight: content_metadata.weight,
                score: chunk.score,
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Queryable, ToSchema)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "link": "https://trieve.ai",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "tag_set": "tag1,tag2",
    "metadata": {"key": "value"},
    "tracking_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "time_stamp": "2021-01-01 00:00:00.000",
    "dataset_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "weight": 0.5,
}))]
#[schema(title = "V1")]
pub struct ChunkMetadataStringTagSet {
    pub id: uuid::Uuid,
    pub link: Option<String>,
    #[serde(skip)]
    pub qdrant_point_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub chunk_html: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub tracking_id: Option<String>,
    pub time_stamp: Option<NaiveDateTime>,
    pub dataset_id: uuid::Uuid,
    pub weight: f64,
    pub location: Option<GeoInfo>,
    pub image_urls: Option<Vec<Option<String>>>,
    pub tag_set: Option<String>,
    pub num_value: Option<f64>,
}

impl From<ChunkMetadata> for ChunkMetadataStringTagSet {
    fn from(chunk: ChunkMetadata) -> Self {
        ChunkMetadataStringTagSet {
            id: chunk.id,
            link: chunk.link,
            qdrant_point_id: chunk.qdrant_point_id,
            created_at: chunk.created_at,
            updated_at: chunk.updated_at,
            chunk_html: chunk.chunk_html,
            metadata: chunk.metadata,
            tracking_id: chunk.tracking_id,
            time_stamp: chunk.time_stamp,
            dataset_id: chunk.dataset_id,
            weight: chunk.weight,
            location: chunk.location,
            image_urls: chunk.image_urls,
            tag_set: chunk.tag_set.map(|tags| {
                tags.into_iter()
                    .map(|tag| tag.unwrap_or_default())
                    .join(",")
            }),
            num_value: chunk.num_value,
        }
    }
}

impl From<QdrantChunkMetadata> for ChunkMetadataStringTagSet {
    fn from(chunk: QdrantChunkMetadata) -> Self {
        ChunkMetadataStringTagSet {
            id: uuid::Uuid::default(),
            link: chunk.link,
            qdrant_point_id: chunk.qdrant_point_id,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            chunk_html: chunk.chunk_html,
            metadata: chunk.metadata,
            tracking_id: chunk.tracking_id,
            time_stamp: chunk.time_stamp,
            dataset_id: chunk.dataset_id,
            weight: chunk.weight,
            location: chunk.location,
            image_urls: chunk.image_urls.map(|image_urls| {
                image_urls
                    .into_iter()
                    .map(Some)
                    .collect::<Vec<Option<String>>>()
            }),
            tag_set: chunk.tag_set.map(|tags| tags.into_iter().join(",")),
            num_value: chunk.num_value,
        }
    }
}

impl From<ContentChunkMetadata> for ChunkMetadataStringTagSet {
    fn from(chunk: ContentChunkMetadata) -> Self {
        ChunkMetadataStringTagSet {
            id: chunk.id,
            link: None,
            qdrant_point_id: chunk.qdrant_point_id,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            chunk_html: Some("".to_string()),
            metadata: None,
            tracking_id: chunk.tracking_id,
            time_stamp: None,
            dataset_id: uuid::Uuid::new_v4(),
            weight: chunk.weight,
            location: None,
            image_urls: chunk.image_urls,
            tag_set: None,
            num_value: chunk.num_value,
        }
    }
}

impl From<SlimChunkMetadataWithArrayTagSet> for ChunkMetadataStringTagSet {
    fn from(slim_chunk_metadata: SlimChunkMetadataWithArrayTagSet) -> Self {
        ChunkMetadataStringTagSet {
            id: slim_chunk_metadata.id,
            link: slim_chunk_metadata.link,
            qdrant_point_id: slim_chunk_metadata.qdrant_point_id,
            created_at: slim_chunk_metadata.created_at,
            updated_at: slim_chunk_metadata.updated_at,
            chunk_html: None,
            metadata: slim_chunk_metadata.metadata,
            tracking_id: slim_chunk_metadata.tracking_id,
            time_stamp: slim_chunk_metadata.time_stamp,
            dataset_id: slim_chunk_metadata.dataset_id,
            weight: slim_chunk_metadata.weight,
            location: slim_chunk_metadata.location,
            image_urls: slim_chunk_metadata.image_urls,
            tag_set: slim_chunk_metadata
                .tag_set
                .map(|tags| tags.into_iter().join(",")),
            num_value: slim_chunk_metadata.num_value,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChunkMetadataStringTagSetWithHighlightsScore {
    pub id: uuid::Uuid,
    pub link: Option<String>,
    #[serde(skip)]
    pub qdrant_point_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub chunk_html: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub tracking_id: Option<String>,
    pub time_stamp: Option<NaiveDateTime>,
    pub dataset_id: uuid::Uuid,
    pub weight: f64,
    pub location: Option<GeoInfo>,
    pub image_urls: Option<Vec<Option<String>>>,
    pub tag_set: Option<String>,
    pub num_value: Option<f64>,
    pub highlights: Option<Vec<String>>,
    pub score: f32,
}

impl From<ScoreChunk> for ChunkMetadataStringTagSetWithHighlightsScore {
    fn from(score_chunk: ScoreChunk) -> Self {
        let chunk_metadata: ChunkMetadata = score_chunk.chunk.into();
        ChunkMetadataStringTagSetWithHighlightsScore {
            id: chunk_metadata.id,
            link: chunk_metadata.link,
            qdrant_point_id: chunk_metadata.qdrant_point_id,
            created_at: chunk_metadata.created_at,
            updated_at: chunk_metadata.updated_at,
            chunk_html: chunk_metadata.chunk_html,
            metadata: chunk_metadata.metadata,
            tracking_id: chunk_metadata.tracking_id,
            time_stamp: chunk_metadata.time_stamp,
            dataset_id: chunk_metadata.dataset_id,
            weight: chunk_metadata.weight,
            location: chunk_metadata.location,
            image_urls: chunk_metadata.image_urls,
            tag_set: chunk_metadata.tag_set.map(|tags| {
                tags.into_iter()
                    .map(|tag| tag.unwrap_or_default())
                    .join(",")
            }),
            num_value: chunk_metadata.num_value,
            highlights: score_chunk.highlights,
            score: score_chunk.score,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Queryable)]
pub struct SlimChunkMetadataTable {
    pub id: uuid::Uuid,
    pub link: Option<String>,
    pub qdrant_point_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub metadata: Option<serde_json::Value>,
    pub tracking_id: Option<String>,
    pub time_stamp: Option<NaiveDateTime>,
    pub location: Option<GeoInfo>,
    pub dataset_id: uuid::Uuid,
    pub weight: f64,
    pub image_urls: Option<Vec<Option<String>>>,
    pub num_value: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Queryable, ToSchema)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "link": "https://trieve.ai",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "tag_set": "tag1,tag2",
    "metadata": {"key": "value"},
    "tracking_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "time_stamp": "2021-01-01 00:00:00.000",
    "dataset_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "weight": 0.5,
}))]
pub struct SlimChunkMetadata {
    pub id: uuid::Uuid,
    pub link: Option<String>,
    #[serde(skip)]
    pub qdrant_point_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub tag_set: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub tracking_id: Option<String>,
    pub time_stamp: Option<NaiveDateTime>,
    pub location: Option<GeoInfo>,
    pub dataset_id: uuid::Uuid,
    pub weight: f64,
    pub image_urls: Option<Vec<Option<String>>>,
    pub num_value: Option<f64>,
}

impl SlimChunkMetadata {
    pub fn from_table_and_tag_set(table: SlimChunkMetadataTable, tag_set: Vec<String>) -> Self {
        Self {
            id: table.id,
            link: table.link,
            qdrant_point_id: table.qdrant_point_id,
            created_at: table.created_at,
            updated_at: table.updated_at,
            tag_set: Some(tag_set.into_iter().join(",")),
            metadata: table.metadata,
            tracking_id: table.tracking_id,
            time_stamp: table.time_stamp,
            location: table.location,
            dataset_id: table.dataset_id,
            weight: table.weight,
            image_urls: table.image_urls,
            num_value: table.num_value,
        }
    }
}

impl From<ChunkMetadata> for SlimChunkMetadata {
    fn from(chunk: ChunkMetadata) -> Self {
        SlimChunkMetadata {
            id: chunk.id,
            link: chunk.link,
            qdrant_point_id: chunk.qdrant_point_id,
            created_at: chunk.created_at,
            updated_at: chunk.updated_at,
            tag_set: chunk.tag_set.map(|tags| {
                tags.into_iter()
                    .map(|tag| tag.unwrap_or_default())
                    .join(",")
            }),
            metadata: chunk.metadata,
            tracking_id: chunk.tracking_id,
            time_stamp: chunk.time_stamp,
            location: chunk.location,
            dataset_id: chunk.dataset_id,
            weight: chunk.weight,
            image_urls: chunk.image_urls,
            num_value: chunk.num_value,
        }
    }
}

impl From<ChunkMetadataStringTagSet> for SlimChunkMetadata {
    fn from(chunk: ChunkMetadataStringTagSet) -> Self {
        SlimChunkMetadata {
            id: chunk.id,
            link: chunk.link,
            qdrant_point_id: chunk.qdrant_point_id,
            created_at: chunk.created_at,
            updated_at: chunk.updated_at,
            tag_set: chunk.tag_set,
            metadata: chunk.metadata,
            tracking_id: chunk.tracking_id,
            time_stamp: chunk.time_stamp,
            location: chunk.location,
            dataset_id: chunk.dataset_id,
            weight: chunk.weight,
            image_urls: chunk.image_urls,
            num_value: chunk.num_value,
        }
    }
}

impl From<ContentChunkMetadata> for SlimChunkMetadata {
    fn from(chunk: ContentChunkMetadata) -> Self {
        SlimChunkMetadata {
            id: chunk.id,
            link: None,
            qdrant_point_id: chunk.qdrant_point_id,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            tag_set: None,
            metadata: None,
            tracking_id: chunk.tracking_id,
            time_stamp: None,
            location: None,
            dataset_id: uuid::Uuid::new_v4(),
            weight: chunk.weight,
            image_urls: chunk.image_urls,
            num_value: chunk.num_value,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Queryable, ToSchema)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "link": "https://trieve.ai",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "tag_set": "tag1,tag2",
    "metadata": {"key": "value"},
    "tracking_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "time_stamp": "2021-01-01 00:00:00.000",
    "dataset_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "weight": 0.5,
}))]
#[schema(title = "ContentChunkMetadata")]
pub struct ContentChunkMetadata {
    pub id: uuid::Uuid,
    #[serde(skip)]
    pub qdrant_point_id: uuid::Uuid,
    pub chunk_html: Option<String>,
    pub tracking_id: Option<String>,
    pub time_stamp: Option<NaiveDateTime>,
    pub weight: f64,
    pub image_urls: Option<Vec<Option<String>>>,
    pub num_value: Option<f64>,
}

impl From<ChunkMetadata> for ContentChunkMetadata {
    fn from(chunk: ChunkMetadata) -> Self {
        ContentChunkMetadata {
            id: chunk.id,
            qdrant_point_id: chunk.qdrant_point_id,
            chunk_html: chunk.chunk_html,
            tracking_id: chunk.tracking_id,
            time_stamp: chunk.time_stamp,
            weight: chunk.weight,
            image_urls: chunk.image_urls,
            num_value: chunk.num_value,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[schema(example=json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "name": "Trieve",
    "email": "developers@trieve.ai",
    "user_orgs": [
        {
            "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
            "user_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
            "org_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
            "role": 0,
            "created_at": "2021-01-01 00:00:00.000",
            "updated_at": "2021-01-01 00:00:00.000",
        }
    ],
    "orgs": [
        {
            "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
            "name": "Trieve",
            "created_at": "2021-01-01 00:00:00.000",
            "updated_at": "2021-01-01 00:00:00.000",
            "registerable": true,
        }
    ],
}))]
pub struct SlimUser {
    pub id: uuid::Uuid,
    pub name: Option<String>,
    pub email: String,
    pub created_at: chrono::NaiveDateTime,
    pub user_orgs: Vec<UserOrganization>,
    pub orgs: Vec<Organization>,
}

impl SlimUser {
    pub fn from_details(
        user: User,
        user_orgs: Vec<UserOrganization>,
        orgs: Vec<Organization>,
    ) -> Self {
        SlimUser {
            id: user.id,
            name: user.name,
            email: user.email,
            created_at: user.created_at,
            user_orgs,
            orgs,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserDTO {
    pub id: uuid::Uuid,
    pub email: Option<String>,
    pub created_at: chrono::NaiveDateTime,
}

#[derive(
    Debug, Default, Serialize, Deserialize, Selectable, Queryable, Insertable, Clone, ToSchema,
)]
#[schema(example = json!({
    "name": "Versions of Oversized T-Shirt",
    "description": "All versions and colorways of the oversized t-shirt",
    "tracking_id": "SNOVERSIZEDTSHIRT",
    "tag_set": ["tshirt", "oversized", "clothing"],
    "metadata": {
        "foo": "bar"
    },
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "dataset_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
}))]
#[diesel(table_name = chunk_group)]
pub struct ChunkGroup {
    pub id: uuid::Uuid,
    pub name: String,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub dataset_id: uuid::Uuid,
    pub tracking_id: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub tag_set: Option<Vec<Option<String>>>,
}

impl ChunkGroup {
    pub fn from_details(
        name: Option<String>,
        description: Option<String>,
        dataset_id: uuid::Uuid,
        tracking_id: Option<String>,
        metadata: Option<serde_json::Value>,
        tag_set: Option<Vec<Option<String>>>,
    ) -> Self {
        ChunkGroup {
            id: uuid::Uuid::new_v4(),
            name: name.unwrap_or_default(),
            description: description.unwrap_or_default(),
            dataset_id,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            tracking_id,
            metadata,
            tag_set,
        }
    }

    pub fn from_details_with_id(
        id: uuid::Uuid,
        name: String,
        description: Option<String>,
        dataset_id: uuid::Uuid,
        tracking_id: Option<String>,
        metadata: Option<serde_json::Value>,
        tag_set: Option<Vec<Option<String>>>,
    ) -> Self {
        ChunkGroup {
            id,
            name,
            description: description.unwrap_or_default(),
            dataset_id,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            tracking_id,
            metadata,
            tag_set,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Queryable, ToSchema)]
#[schema(example=json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "dataset_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "name": "Trieve",
    "description": "A group of chunks",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "file_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "tracking_id": "3",
}))]
pub struct ChunkGroupAndFileId {
    pub id: uuid::Uuid,
    pub dataset_id: uuid::Uuid,
    pub name: String,
    pub description: String,
    pub tracking_id: Option<String>,
    pub tag_set: Option<Vec<Option<String>>>,
    pub metadata: Option<serde_json::Value>,
    pub file_id: Option<uuid::Uuid>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl ChunkGroupAndFileId {
    pub fn from_group(group: ChunkGroup, file_id: Option<uuid::Uuid>) -> Self {
        Self {
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
    }

    pub fn to_group(&self) -> ChunkGroup {
        ChunkGroup {
            id: self.id,
            dataset_id: self.dataset_id,
            name: self.name.clone(),
            description: self.description.clone(),
            tracking_id: self.tracking_id.clone(),
            tag_set: self.tag_set.clone(),
            metadata: self.metadata.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

#[derive(
    Debug, Default, Serialize, Deserialize, Selectable, Queryable, Insertable, Clone, ToSchema,
)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "group_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "chunk_metadata_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
}))]
#[diesel(table_name = chunk_group_bookmarks)]
pub struct ChunkGroupBookmark {
    pub id: uuid::Uuid,
    pub group_id: uuid::Uuid,
    pub chunk_metadata_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl ChunkGroupBookmark {
    pub fn from_details(group_id: uuid::Uuid, chunk_metadata_id: uuid::Uuid) -> Self {
        ChunkGroupBookmark {
            id: uuid::Uuid::new_v4(),
            group_id,
            chunk_metadata_id,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
        }
    }
}

#[derive(
    Debug, Default, Serialize, Deserialize, Selectable, Queryable, Insertable, Clone, ToSchema,
)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "file_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "group_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
}))]
#[diesel(table_name = groups_from_files)]
pub struct FileGroup {
    pub id: uuid::Uuid,
    pub file_id: uuid::Uuid,
    pub group_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl FileGroup {
    pub fn from_details(file_id: uuid::Uuid, group_id: uuid::Uuid) -> Self {
        FileGroup {
            id: uuid::Uuid::new_v4(),
            file_id,
            group_id,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserDTOWithChunks {
    pub id: uuid::Uuid,
    pub email: Option<String>,
    pub created_at: chrono::NaiveDateTime,
    pub total_chunks_created: i64,
    pub chunks: Vec<ChunkMetadata>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
/// The key in the ChunkReqPayload which you can map a column or field from the CSV or JSONL file to.
pub enum ChunkReqPayloadFields {
    #[serde(rename = "link")]
    Link,
    #[serde(rename = "tag_set")]
    TagSet,
    #[serde(rename = "num_value")]
    NumValue,
    #[serde(rename = "tracking_id")]
    TrackingId,
    #[serde(rename = "group_tracking_ids")]
    GroupTrackingIds,
    #[serde(rename = "time_stamp")]
    TimeStamp,
    #[serde(rename = "lat")]
    Lat,
    #[serde(rename = "lon")]
    Lon,
    #[serde(rename = "image_urls")]
    ImageUrls,
    #[serde(rename = "weight")]
    Weight,
    #[serde(rename = "boost_phrase")]
    BoostPhrase,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
/// Express a mapping between a column or field in a CSV or JSONL field and a key in the ChunkReqPayload created for each row or object.
pub struct ChunkReqPayloadMapping {
    /// The column or field in the CSV or JSONL file that you want to map to a key in the ChunkReqPayload
    pub csv_jsonl_field: String,
    /// The key in the ChunkReqPayload that you want to map the column or field to.
    pub chunk_req_payload_field: ChunkReqPayloadFields,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
/// Specify all of the mappings between columns or fields in a CSV or JSONL file and keys in the ChunkReqPayload. Array fields like tag_set, image_urls, and group_tracking_ids can have multiple mappings. Boost phrase can also have multiple mappings which get concatenated. Other fields can only have one mapping and only the last mapping will be used.
pub struct ChunkReqPayloadMappings(pub Vec<ChunkReqPayloadMapping>);

#[derive(
    Debug, Default, Serialize, Deserialize, Selectable, Queryable, Insertable, Clone, ToSchema,
)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "file_name": "file.txt",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "size": 1000,
    "tag_set": "tag1,tag2",
    "metadata": {"key": "value"},
    "link": "https://trieve.ai",
    "time_stamp": "2021-01-01 00:00:00.000",
    "dataset_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
}))]
#[diesel(table_name = files)]
pub struct File {
    pub id: uuid::Uuid,
    pub file_name: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub size: i64,
    pub metadata: Option<serde_json::Value>,
    pub link: Option<String>,
    pub time_stamp: Option<chrono::NaiveDateTime>,
    pub dataset_id: uuid::Uuid,
    pub tag_set: Option<Vec<Option<String>>>,
}

impl File {
    #[allow(clippy::too_many_arguments)]
    pub fn from_details(
        file_id: Option<uuid::Uuid>,
        file_name: &str,
        size: i64,
        tag_set: Option<Vec<Option<String>>>,
        metadata: Option<serde_json::Value>,
        link: Option<String>,
        time_stamp: Option<String>,
        dataset_id: uuid::Uuid,
    ) -> Self {
        File {
            id: file_id.unwrap_or(uuid::Uuid::new_v4()),
            file_name: file_name.to_string(),
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            size,
            tag_set,
            metadata,
            link,
            time_stamp: time_stamp.map(|ts| {
                ts.parse::<DateTimeUtc>()
                    .unwrap_or(DateTimeUtc(DateTime::default()))
                    .0
                    .with_timezone(&chrono::Local)
                    .naive_local()
            }),
            dataset_id,
        }
    }
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct FileAndGroupId {
    pub file: File,
    pub group_id: Option<uuid::Uuid>,
}

#[derive(Debug, Default, Serialize, Deserialize, ToSchema)]
#[schema(example=json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "file_name": "file.txt",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "size": 1000,
    "s3_url": "https://trieve.ai",
    "metadata": {"key": "value"},
    "link": "https://trieve.ai",
}))]
pub struct FileDTO {
    pub id: uuid::Uuid,
    pub file_name: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub size: i64,
    pub s3_url: String,
    pub metadata: Option<serde_json::Value>,
    pub link: Option<String>,
}

impl From<File> for FileDTO {
    fn from(file: File) -> Self {
        FileDTO {
            id: file.id,
            file_name: file.file_name,
            created_at: file.created_at,
            updated_at: file.updated_at,
            size: file.size,
            s3_url: "".to_string(),
            metadata: file.metadata,
            link: file.link,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Row, ToSchema)]
pub struct WorkerEventClickhouse {
    #[serde(with = "clickhouse::serde::uuid")]
    pub id: uuid::Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    pub dataset_id: uuid::Uuid,
    pub event_type: String,
    pub event_data: String,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[schema(example=json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "dataset_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "event_type": "file_uploaded",
    "event_data": {"group_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3", "file_name": "file.txt"},
}))]
pub struct WorkerEvent {
    pub id: uuid::Uuid,
    pub created_at: String,
    pub dataset_id: uuid::Uuid,
    pub event_type: String,
    pub event_data: String,
}

impl From<WorkerEventClickhouse> for WorkerEvent {
    fn from(clickhouse_event: WorkerEventClickhouse) -> Self {
        WorkerEvent {
            id: uuid::Uuid::from_bytes(*clickhouse_event.id.as_bytes()),
            created_at: clickhouse_event.created_at.to_string(),
            dataset_id: uuid::Uuid::from_bytes(*clickhouse_event.dataset_id.as_bytes()),
            event_type: clickhouse_event.event_type,
            event_data: clickhouse_event.event_data,
        }
    }
}

impl From<WorkerEvent> for WorkerEventClickhouse {
    fn from(event: WorkerEvent) -> Self {
        WorkerEventClickhouse {
            id: event.id,
            created_at: OffsetDateTime::now_utc(),
            dataset_id: event.dataset_id,
            event_type: event.event_type,
            event_data: event.event_data,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Display, ToSchema)]
#[serde(untagged)]
pub enum EventType {
    #[display(fmt = "file_uploaded")]
    FileUploaded {
        file_id: uuid::Uuid,
        file_name: String,
    },
    #[display(fmt = "file_upload_failed")]
    FileUploadFailed { file_id: uuid::Uuid, error: String },
    #[display(fmt = "chunks_uploaded")]
    ChunksUploaded { chunk_ids: Vec<uuid::Uuid> },
    #[display(fmt = "chunk_updated")]
    ChunkUpdated { chunk_id: uuid::Uuid },
    #[display(fmt = "bulk_chunks_deleted")]
    BulkChunksDeleted { message: String },
    #[display(fmt = "chunk_update_failed")]
    ChunkUpdateFailed {
        chunk_id: uuid::Uuid,
        message: String,
    },
    #[display(fmt = "dataset_delete_failed")]
    DatasetDeleteFailed { error: String },
    #[display(fmt = "bulk_chunk_upload_failed")]
    BulkChunkUploadFailed {
        chunk_ids: Vec<uuid::Uuid>,
        error: String,
    },
    #[display(fmt = "group_chunks_updated")]
    GroupChunksUpdated { group_id: uuid::Uuid },
    #[display(fmt = "group_chunks_action_failed")]
    GroupChunksActionFailed { group_id: uuid::Uuid, error: String },
    #[display(fmt = "crawl_started")]
    CrawlStarted {
        scrape_id: uuid::Uuid,
        crawl_options: CrawlOptions,
    },
    #[display(fmt = "crawl_completed")]
    CrawlCompleted {
        scrape_id: uuid::Uuid,
        pages_crawled: usize,
        chunks_created: usize,
        crawl_options: CrawlOptions,
    },
    #[display(fmt = "crawl_failed")]
    CrawlFailed {
        scrape_id: uuid::Uuid,
        crawl_options: CrawlOptions,
        error: String,
    },
    #[display(fmt = "csv_jsonl_processing_failed")]
    CsvJsonlProcessingFailed { file_id: uuid::Uuid, error: String },
    #[display(fmt = "csv_jsonl_processing_checkpoint")]
    CsvJsonlProcessingCheckpoint {
        file_id: uuid::Uuid,
        chunks_created: usize,
    },
    #[display(fmt = "csv_jsonl_processing_completed")]
    CsvJsonlProcessingCompleted {
        file_id: uuid::Uuid,
        chunks_created: usize,
    },
    #[display(fmt = "video_uploaded")]
    VideoUploaded {
        video_id: String,
        chunks_created: usize,
    },
    #[display(fmt = "pagefind_indexing_started")]
    PagefindIndexingStarted,
    #[display(fmt = "pagefind_indexing_finished")]
    PagefindIndexingFinished { total_files: usize },
    #[display(fmt = "etl_started")]
    EtlStarted {
        prompt: String,
        model: Option<String>,
        tag_enum: Option<Vec<String>>,
        include_images: Option<bool>,
    },
    #[display(fmt = "etl_completed")]
    EtlCompleted,
    #[display(fmt = "etl_failed")]
    EtlFailed { error: String },
}

impl EventType {
    pub fn get_all_event_types() -> Vec<EventTypeRequest> {
        vec![
            EventTypeRequest::FileUploaded,
            EventTypeRequest::FileUploadFailed,
            EventTypeRequest::ChunksUploaded,
            EventTypeRequest::ChunkActionFailed,
            EventTypeRequest::ChunkUpdated,
            EventTypeRequest::BulkChunksDeleted,
            EventTypeRequest::DatasetDeleteFailed,
            EventTypeRequest::BulkChunkUploadFailed,
            EventTypeRequest::GroupChunksUpdated,
            EventTypeRequest::GroupChunksActionFailed,
            EventTypeRequest::CrawlCompleted,
            EventTypeRequest::CrawlStarted,
            EventTypeRequest::CrawlFailed,
            EventTypeRequest::CsvJsonlProcessingFailed,
            EventTypeRequest::CsvJsonlProcessingCheckpoint,
            EventTypeRequest::CsvJsonlProcessingCompleted,
            EventTypeRequest::VideoUploaded,
            EventTypeRequest::PagefindIndexingStarted,
            EventTypeRequest::PagefindIndexingFinished,
            EventTypeRequest::EtlStarted,
            EventTypeRequest::EtlCompleted,
            EventTypeRequest::EtlFailed,
            EventTypeRequest::ChunkUpdateFailed,
        ]
    }
}

impl WorkerEvent {
    pub fn from_details(dataset_id: uuid::Uuid, event_type: EventType) -> Self {
        WorkerEvent {
            id: uuid::Uuid::new_v4(),
            created_at: chrono::Utc::now().naive_local().to_string(),
            dataset_id,
            event_type: event_type.to_string(),
            event_data: serde_json::to_value(event_type).unwrap().to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, ValidGrouping)]
#[diesel(table_name = dataset_group_counts)]
pub struct DatasetGroupCount {
    pub id: uuid::Uuid,
    pub group_count: i32,
    pub dataset_id: uuid::Uuid,
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, ValidGrouping)]
#[diesel(table_name = dataset_event_counts)]
pub struct DatasetEventCount {
    pub id: uuid::Uuid,
    pub dataset_uuid: uuid::Uuid,
    pub notification_count: i32,
}

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Queryable,
    Insertable,
    Selectable,
    Clone,
    ToSchema,
    QueryableByName,
    AsChangeset,
)]
#[schema(example=json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "name": "Trieve",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "organization_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "tracking_id": "foobar-dataset",
    "server_configuration": {
        "LLM_BASE_URL": "https://api.openai.com/v1",
        "EMBEDDING_BASE_URL": "https://api.openai.com/v1",
        "EMBEDDING_MODEL_NAME": "text-embedding-3-small",
        "MESSAGE_TO_QUERY_PROMPT": "Write a 1-2 sentence semantic search query along the lines of a hypothetical response to: \n\n",
        "RAG_PROMPT": "Use the following retrieved documents to respond briefly and accurately:",
        "N_RETRIEVALS_TO_INCLUDE": 8,
        "EMBEDDING_SIZE": 1536,
        "DISTANCE_METRIC": "cosine",
        "LLM_DEFAULT_MODEL": "gpt-4o",
        "BM25_ENABLED": true,
        "BM25_B": 0.75,
        "BM25_K": 0.75,
        "BM25_AVG_LEN": 256.0,
        "FULLTEXT_ENABLED": true,
        "SEMANTIC_ENABLED": true,
        "QDRANT_ONLY": false,
        "EMBEDDING_QUERY_PREFIX": "",
        "USE_MESSAGE_TO_QUERY_PROMPT": false,
        "FREQUENCY_PENALTY": 0.0,
        "TEMPERATURE": 0.5,
        "PRESENCE_PENALTY": 0.0,
        "STOP_TOKENS": ["\n\n", "\n"],
        "INDEXED_ONLY": false,
        "LOCKED": false,
        "SYSTEM_PROMPT": "You are a helpful assistant",
        "MAX_LIMIT": 10000,
        "TASK_DEFINITION": "Your task is to grade the relevance of context document(s) against the specified user query.",
    },
}))]
#[diesel(table_name = datasets)]
pub struct Dataset {
    /// Unique identifier of the dataset, auto-generated uuid created by Trieve
    pub id: uuid::Uuid,
    /// Name of the dataset
    pub name: String,
    /// Timestamp of the creation of the dataset
    pub created_at: chrono::NaiveDateTime,
    /// Timestamp of the last update of the dataset
    pub updated_at: chrono::NaiveDateTime,
    /// Unique identifier of the organization that owns the dataset
    pub organization_id: uuid::Uuid,
    /// Configuration of the dataset for RAG, embeddings, BM25, etc.
    pub server_configuration: serde_json::Value,
    /// Tracking ID of the dataset, can be any string, determined by the user. Tracking ID's are unique identifiers for datasets within an organization. They are designed to match the unique identifier of the dataset in the user's system.
    pub tracking_id: Option<String>,
    /// Flag to indicate if the dataset has been deleted. Deletes are handled async after the flag is set so as to avoid expensive search index compaction.
    pub deleted: i32,
}

impl Dataset {
    pub fn from_details(
        name: String,
        organization_id: uuid::Uuid,
        tracking_id: Option<String>,
        server_configuration: DatasetConfiguration,
    ) -> Self {
        Dataset {
            id: uuid::Uuid::new_v4(),
            name,
            organization_id,
            tracking_id,
            server_configuration: serde_json::to_value(server_configuration).unwrap(),
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            deleted: 0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Clone, ToSchema)]
#[schema(example=json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "name": "Trieve",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "tracking_id": "3",
    "organization_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
}))]
pub struct DatasetDTO {
    pub id: uuid::Uuid,
    pub name: String,
    pub tracking_id: Option<String>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub organization_id: uuid::Uuid,
}

impl From<Dataset> for DatasetDTO {
    fn from(dataset: Dataset) -> Self {
        DatasetDTO {
            id: dataset.id,
            name: dataset.name,
            created_at: dataset.created_at,
            updated_at: dataset.updated_at,
            tracking_id: dataset.tracking_id,
            organization_id: dataset.organization_id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, Selectable, Clone, ToSchema)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "dataset_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "chunk_count": 100,
}))]
#[diesel(table_name = dataset_usage_counts)]
pub struct DatasetUsageCount {
    pub id: uuid::Uuid,
    pub dataset_id: uuid::Uuid,
    pub chunk_count: i32,
}

impl DatasetUsageCount {
    pub fn from_details(dataset_id: uuid::Uuid, chunk_count: i32) -> Self {
        DatasetUsageCount {
            id: uuid::Uuid::new_v4(),
            dataset_id,
            chunk_count,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(example = json!({
    "dataset": {
        "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
        "name": "Trieve",
        "created_at": "2021-01-01 00:00:00.000",
        "updated_at": "2021-01-01 00:00:00.000",
        "organization_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
        "server_configuration": {},
    },
    "dataset_usage": {
        "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
        "dataset_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
        "chunk_count": 100,
    }
}))]
pub struct DatasetAndUsage {
    pub dataset: DatasetDTO,
    pub dataset_usage: DatasetUsageCount,
}

impl DatasetAndUsage {
    pub fn from_components(dataset: DatasetDTO, dataset_usage: DatasetUsageCount) -> Self {
        DatasetAndUsage {
            dataset,
            dataset_usage,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Display, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DistanceMetric {
    #[serde(alias = "euclid")]
    #[display(fmt = "euclidean")]
    Euclidean,
    #[display(fmt = "cosine")]
    Cosine,
    #[display(fmt = "manhattan")]
    Manhattan,
    #[display(fmt = "dot")]
    Dot,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[schema(example=json!({
    "LLM_BASE_URL": "https://api.openai.com/v1",
    "EMBEDDING_BASE_URL": "https://api.openai.com/v1",
    "EMBEDDING_MODEL_NAME": "text-embedding-3-small",
    "MESSAGE_TO_QUERY_PROMPT": "Write a 1-2 sentence semantic search query along the lines of a hypothetical response to: \n\n",
    "RAG_PROMPT": "Use the following retrieved documents to respond briefly and accurately:",
    "N_RETRIEVALS_TO_INCLUDE": 8,
    "EMBEDDING_SIZE": 1536,
    "DISTANCE_METRIC": "cosine",
    "LLM_DEFAULT_MODEL": "gpt-4o",
    "BM25_ENABLED": true,
    "BM25_B": 0.75,
    "BM25_K": 0.75,
    "BM25_AVG_LEN": 256.0,
    "FULLTEXT_ENABLED": true,
    "SEMANTIC_ENABLED": true,
    "QDRANT_ONLY": false,
    "EMBEDDING_QUERY_PREFIX": "",
    "USE_MESSAGE_TO_QUERY_PROMPT": false,
    "FREQUENCY_PENALTY": 0.0,
    "TEMPERATURE": 0.5,
    "PRESENCE_PENALTY": 0.0,
    "STOP_TOKENS": ["\n\n", "\n"],
    "INDEXED_ONLY": false,
    "LOCKED": false,
    "SYSTEM_PROMPT": "You are a helpful assistant",
    "MAX_LIMIT": 10000,
    "TASK_DEFINITION": "Your task is to grade the relevance of context document(s) against the specified user query.",
}))]
#[allow(non_snake_case)]
pub struct DatasetConfiguration {
    pub LLM_BASE_URL: String,
    #[serde(skip_serializing)]
    pub LLM_API_KEY: String,
    #[serde(skip_serializing)]
    pub RERANKER_API_KEY: String,
    pub RERANKER_MODEL_NAME: String,
    pub EMBEDDING_BASE_URL: String,
    pub EMBEDDING_MODEL_NAME: String,
    pub RERANKER_BASE_URL: String,
    pub MESSAGE_TO_QUERY_PROMPT: String,
    pub RAG_PROMPT: String,
    pub N_RETRIEVALS_TO_INCLUDE: usize,
    pub EMBEDDING_SIZE: usize,
    pub DISTANCE_METRIC: DistanceMetric,
    pub LLM_DEFAULT_MODEL: String,
    pub BM25_ENABLED: bool,
    pub BM25_B: f32,
    pub BM25_K: f32,
    pub BM25_AVG_LEN: f32,
    pub FULLTEXT_ENABLED: bool,
    pub SEMANTIC_ENABLED: bool,
    pub QDRANT_ONLY: bool,
    pub EMBEDDING_QUERY_PREFIX: String,
    pub USE_MESSAGE_TO_QUERY_PROMPT: bool,
    pub FREQUENCY_PENALTY: Option<f64>,
    pub TEMPERATURE: Option<f64>,
    pub PRESENCE_PENALTY: Option<f64>,
    pub MAX_TOKENS: Option<u64>,
    pub STOP_TOKENS: Option<Vec<String>>,
    pub INDEXED_ONLY: bool,
    pub LOCKED: bool,
    pub SYSTEM_PROMPT: String,
    pub MAX_LIMIT: u64,
    pub PUBLIC_DATASET: PublicDatasetOptions,
    pub DISABLE_ANALYTICS: bool,
    pub PAGEFIND_ENABLED: bool,
    pub TASK_DEFINITION: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct PublicDatasetOptions {
    pub enabled: bool,
    #[serde(skip_serializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    pub extra_params: Option<PublicPageParameters>,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[schema(example=json!({
    "LLM_BASE_URL": "https://api.openai.com/v1",
    "EMBEDDING_BASE_URL": "https://api.openai.com/v1",
    "EMBEDDING_MODEL_NAME": "text-embedding-3-small",
    "MESSAGE_TO_QUERY_PROMPT": "Write a 1-2 sentence semantic search query along the lines of a hypothetical response to: \n\n",
    "RAG_PROMPT": "Use the following retrieved documents to respond briefly and accurately:",
    "N_RETRIEVALS_TO_INCLUDE": 8,
    "EMBEDDING_SIZE": 1536,
    "DISTANCE_METRIC": "cosine",
    "LLM_DEFAULT_MODEL": "gpt-4o",
    "BM25_ENABLED": true,
    "BM25_B": 0.75,
    "BM25_K": 0.75,
    "BM25_AVG_LEN": 256.0,
    "FULLTEXT_ENABLED": true,
    "SEMANTIC_ENABLED": true,
    "QDRANT_ONLY": false,
    "EMBEDDING_QUERY_PREFIX": "",
    "USE_MESSAGE_TO_QUERY_PROMPT": false,
    "FREQUENCY_PENALTY": 0.0,
    "TEMPERATURE": 0.5,
    "PRESENCE_PENALTY": 0.0,
    "STOP_TOKENS": ["\n\n", "\n"],
    "INDEXED_ONLY": false,
    "LOCKED": false,
    "SYSTEM_PROMPT": "You are a helpful assistant",
    "MAX_LIMIT": 10000,
    "TASK_DEFINITION": "Your task is to grade the relevance of context document(s) against the specified user query.",
}))]
#[allow(non_snake_case)]
/// Lets you specify the configuration for a dataset
pub struct DatasetConfigurationDTO {
    /// The base URL for the LLM API
    pub LLM_BASE_URL: Option<String>,
    #[serde(skip_serializing)]
    /// The API key for the LLM API
    pub LLM_API_KEY: Option<String>,
    #[serde(skip_serializing)]
    /// The API key for the Reranker API
    pub RERANKER_API_KEY: Option<String>,
    /// The model name for the Reranker API
    pub RERANKER_MODEL_NAME: Option<String>,
    /// The base URL for the embedding API
    pub EMBEDDING_BASE_URL: Option<String>,
    /// The name of the embedding model to use
    pub EMBEDDING_MODEL_NAME: Option<String>,
    /// The base URL for the reranker API
    pub RERANKER_BASE_URL: Option<String>,
    /// The prompt to use for converting a message to a query
    pub MESSAGE_TO_QUERY_PROMPT: Option<String>,
    /// The prompt to use for the RAG model
    pub RAG_PROMPT: Option<String>,
    /// The number of retrievals to include with the RAG model
    pub N_RETRIEVALS_TO_INCLUDE: Option<usize>,
    /// The size of the embeddings
    pub EMBEDDING_SIZE: Option<usize>,
    /// Distance metric for scoring embeddings
    pub DISTANCE_METRIC: Option<DistanceMetric>,
    /// The default model to use for the LLM
    pub LLM_DEFAULT_MODEL: Option<String>,
    /// Whether to use BM25
    pub BM25_ENABLED: Option<bool>,
    /// The BM25 B parameter
    pub BM25_B: Option<f32>,
    /// The BM25 K parameter
    pub BM25_K: Option<f32>,
    /// The average length of the chunks in the index for BM25
    pub BM25_AVG_LEN: Option<f32>,
    /// Whether to use fulltext search
    pub FULLTEXT_ENABLED: Option<bool>,
    /// Whether to use semantic search
    pub SEMANTIC_ENABLED: Option<bool>,
    /// Whether or not to insert chunks into Postgres
    pub QDRANT_ONLY: Option<bool>,
    /// The prefix to use for the embedding query
    pub EMBEDDING_QUERY_PREFIX: Option<String>,
    /// Whether to use the message to query prompt
    pub USE_MESSAGE_TO_QUERY_PROMPT: Option<bool>,
    /// The frequency penalty to use
    pub FREQUENCY_PENALTY: Option<f64>,
    /// The temperature to use
    pub TEMPERATURE: Option<f64>,
    /// The presence penalty to use
    pub PRESENCE_PENALTY: Option<f64>,
    /// The stop tokens to use
    pub STOP_TOKENS: Option<Vec<String>>,
    /// The maximum number of tokens to use in LLM Response
    pub MAX_TOKENS: Option<u64>,
    /// Whether to only use indexed chunks
    pub INDEXED_ONLY: Option<bool>,
    /// Whether the dataset is locked to prevent changes or deletion
    pub LOCKED: Option<bool>,
    /// The system prompt to use for the LLM
    pub SYSTEM_PROMPT: Option<String>,
    /// The maximum limit for the number of chunks for counting
    pub MAX_LIMIT: Option<u64>,
    /// Config for making the dataset public
    pub PUBLIC_DATASET: Option<PublicDatasetOptions>,
    /// Whether to disable analytics
    pub DISABLE_ANALYTICS: Option<bool>,
    /// Whether to enable pagefind indexing
    pub PAGEFIND_ENABLED: Option<bool>,

    pub TASK_DEFINITION: Option<String>,
}

impl From<DatasetConfigurationDTO> for DatasetConfiguration {
    fn from(dto: DatasetConfigurationDTO) -> Self {
        DatasetConfiguration {
            LLM_BASE_URL: dto.LLM_BASE_URL.unwrap_or("https://api.openai.com/v1".to_string()),
            LLM_API_KEY: dto.LLM_API_KEY.unwrap_or("".to_string()),
            RERANKER_API_KEY: dto.RERANKER_API_KEY.unwrap_or("".to_string()),
            RERANKER_MODEL_NAME: dto.RERANKER_MODEL_NAME.unwrap_or("bge-reranker-large".to_string()),
            EMBEDDING_BASE_URL: dto.EMBEDDING_BASE_URL.unwrap_or("https://api.openai.com/v1".to_string()),
            EMBEDDING_MODEL_NAME: dto.EMBEDDING_MODEL_NAME.unwrap_or("text-embedding-3-small".to_string()),
            RERANKER_BASE_URL: dto.RERANKER_BASE_URL.unwrap_or("".to_string()),
            MESSAGE_TO_QUERY_PROMPT: dto.MESSAGE_TO_QUERY_PROMPT.unwrap_or("Write a 1-2 sentence semantic search query along the lines of a hypothetical response to: \n\n".to_string()),
            RAG_PROMPT: dto.RAG_PROMPT.unwrap_or("Use the following retrieved documents to respond briefly and accurately:".to_string()),
            N_RETRIEVALS_TO_INCLUDE: dto.N_RETRIEVALS_TO_INCLUDE.unwrap_or(8),
            EMBEDDING_SIZE: dto.EMBEDDING_SIZE.unwrap_or(1536),
            DISTANCE_METRIC: dto.DISTANCE_METRIC.unwrap_or(DistanceMetric::Cosine),
            LLM_DEFAULT_MODEL: dto.LLM_DEFAULT_MODEL.unwrap_or("gpt-4o".to_string()),
            BM25_ENABLED: dto.BM25_ENABLED.unwrap_or(true),
            BM25_B: dto.BM25_B.unwrap_or(0.75),
            BM25_K: dto.BM25_K.unwrap_or(0.75),
            BM25_AVG_LEN: dto.BM25_AVG_LEN.unwrap_or(256.0),
            FULLTEXT_ENABLED: dto.FULLTEXT_ENABLED.unwrap_or(true),
            SEMANTIC_ENABLED: dto.SEMANTIC_ENABLED.unwrap_or(true),
            QDRANT_ONLY: dto.QDRANT_ONLY.unwrap_or(false),
            EMBEDDING_QUERY_PREFIX: dto.EMBEDDING_QUERY_PREFIX.unwrap_or("".to_string()),
            USE_MESSAGE_TO_QUERY_PROMPT: dto.USE_MESSAGE_TO_QUERY_PROMPT.unwrap_or(false),
            FREQUENCY_PENALTY: dto.FREQUENCY_PENALTY,
            TEMPERATURE: dto.TEMPERATURE,
            PRESENCE_PENALTY: dto.PRESENCE_PENALTY,
            STOP_TOKENS: dto.STOP_TOKENS,
            MAX_TOKENS: dto.MAX_TOKENS,
            INDEXED_ONLY: dto.INDEXED_ONLY.unwrap_or(false),
            LOCKED: dto.LOCKED.unwrap_or(false),
            SYSTEM_PROMPT: dto.SYSTEM_PROMPT.unwrap_or("You are a helpful assistant".to_string()),
            MAX_LIMIT: dto.MAX_LIMIT.unwrap_or(10000),
            PUBLIC_DATASET: PublicDatasetOptions {
                enabled: dto.PUBLIC_DATASET.clone().map(|public_dataset| public_dataset.clone().enabled).unwrap_or(false),
                api_key: Some("".to_string()),
                extra_params: dto.PUBLIC_DATASET.map(|public_dataset| public_dataset.extra_params)
                .unwrap_or_default()
            },
            DISABLE_ANALYTICS: dto.DISABLE_ANALYTICS.unwrap_or(false),
            PAGEFIND_ENABLED: dto.PAGEFIND_ENABLED.unwrap_or(false),
            TASK_DEFINITION: dto.TASK_DEFINITION.unwrap_or("Your task is to grade the relevance of context document(s) against the specified user query.".to_string()),
        }
    }
}

impl From<DatasetConfiguration> for DatasetConfigurationDTO {
    fn from(config: DatasetConfiguration) -> Self {
        DatasetConfigurationDTO {
            LLM_BASE_URL: Some(config.LLM_BASE_URL),
            LLM_API_KEY: Some(config.LLM_API_KEY),
            RERANKER_API_KEY: Some(config.RERANKER_API_KEY),
            RERANKER_MODEL_NAME: Some(config.RERANKER_MODEL_NAME),
            EMBEDDING_BASE_URL: Some(config.EMBEDDING_BASE_URL),
            EMBEDDING_MODEL_NAME: Some(config.EMBEDDING_MODEL_NAME),
            RERANKER_BASE_URL: Some(config.RERANKER_BASE_URL),
            MESSAGE_TO_QUERY_PROMPT: Some(config.MESSAGE_TO_QUERY_PROMPT),
            RAG_PROMPT: Some(config.RAG_PROMPT),
            N_RETRIEVALS_TO_INCLUDE: Some(config.N_RETRIEVALS_TO_INCLUDE),
            EMBEDDING_SIZE: Some(config.EMBEDDING_SIZE),
            DISTANCE_METRIC: Some(config.DISTANCE_METRIC),
            LLM_DEFAULT_MODEL: Some(config.LLM_DEFAULT_MODEL),
            BM25_ENABLED: Some(config.BM25_ENABLED),
            BM25_B: Some(config.BM25_B),
            BM25_K: Some(config.BM25_K),
            BM25_AVG_LEN: Some(config.BM25_AVG_LEN),
            FULLTEXT_ENABLED: Some(config.FULLTEXT_ENABLED),
            SEMANTIC_ENABLED: Some(config.SEMANTIC_ENABLED),
            QDRANT_ONLY: Some(config.QDRANT_ONLY),
            EMBEDDING_QUERY_PREFIX: Some(config.EMBEDDING_QUERY_PREFIX),
            USE_MESSAGE_TO_QUERY_PROMPT: Some(config.USE_MESSAGE_TO_QUERY_PROMPT),
            FREQUENCY_PENALTY: config.FREQUENCY_PENALTY,
            TEMPERATURE: config.TEMPERATURE,
            PRESENCE_PENALTY: config.PRESENCE_PENALTY,
            STOP_TOKENS: config.STOP_TOKENS,
            MAX_TOKENS: config.MAX_TOKENS,
            INDEXED_ONLY: Some(config.INDEXED_ONLY),
            LOCKED: Some(config.LOCKED),
            SYSTEM_PROMPT: Some(config.SYSTEM_PROMPT),
            MAX_LIMIT: Some(config.MAX_LIMIT),
            PUBLIC_DATASET: Some(PublicDatasetOptions {
                enabled: config.PUBLIC_DATASET.enabled,
                api_key: None,
                extra_params: config.PUBLIC_DATASET.extra_params.map(|params| {
                    PublicPageParameters {
                        api_key: None,
                        ..params
                    }
                }),
            }),
            DISABLE_ANALYTICS: Some(config.DISABLE_ANALYTICS),
            PAGEFIND_ENABLED: Some(config.PAGEFIND_ENABLED),
            TASK_DEFINITION: Some(config.TASK_DEFINITION),
        }
    }
}

impl Default for DatasetConfiguration {
    fn default() -> Self {
        DatasetConfiguration {
            LLM_BASE_URL: "https://api.openai.com/v1".to_string(),
            LLM_API_KEY: "".to_string(),
            RERANKER_API_KEY: "".to_string(),
            RERANKER_MODEL_NAME: "bge-reranker-large".to_string(),
            EMBEDDING_BASE_URL: "https://api.openai.com/v1".to_string(),
            EMBEDDING_MODEL_NAME: "text-embedding-3-small".to_string(),
            RERANKER_BASE_URL: "".to_string(),
            MESSAGE_TO_QUERY_PROMPT: "Write a 1-2 sentence semantic search query along the lines of a hypothetical response to: \n\n".to_string(),
            RAG_PROMPT: "Use the following retrieved documents to respond briefly and accurately:".to_string(),
            N_RETRIEVALS_TO_INCLUDE: 8,
            EMBEDDING_SIZE: 1536,
            DISTANCE_METRIC: DistanceMetric::Cosine,
            LLM_DEFAULT_MODEL: "gpt-4o".to_string(),
            BM25_ENABLED: true,
            BM25_B: 0.75,
            BM25_K: 0.75,
            BM25_AVG_LEN: 256.0,
            FULLTEXT_ENABLED: true,
            SEMANTIC_ENABLED: true,
            QDRANT_ONLY: false,
            EMBEDDING_QUERY_PREFIX: "".to_string(),
            USE_MESSAGE_TO_QUERY_PROMPT: false,
            FREQUENCY_PENALTY: None,
            TEMPERATURE: None,
            PRESENCE_PENALTY: None,
            STOP_TOKENS: None,
            INDEXED_ONLY: false,
            LOCKED: false,
            MAX_TOKENS: None,
            SYSTEM_PROMPT: "You are a helpful assistant".to_string(),
            MAX_LIMIT: 10000,
            PUBLIC_DATASET: PublicDatasetOptions {
                enabled: false,
                api_key: Some("".to_string()),
                extra_params: None,
            },
            DISABLE_ANALYTICS: false,
            PAGEFIND_ENABLED: false,
            TASK_DEFINITION: "Your task is to grade the relevance of context document(s) against the specified user query.".to_string(),
        }
    }
}

impl DatasetConfiguration {
    pub fn from_json(configuration_json: serde_json::Value) -> Self {
        let default_config = json!({});
        let binding = configuration_json.clone();

        let extra_params: Option<PublicPageParameters> = configuration_json
            .pointer("/PUBLIC_DATASET/extra_params")
            .and_then(|value| serde_json::from_value(value.clone()).ok());

        let configuration = binding
            .as_object()
            .unwrap_or(default_config.as_object().unwrap());

        DatasetConfiguration {
            LLM_BASE_URL: configuration
                .get("LLM_BASE_URL")
                .unwrap_or(&json!("https://api.openai.com/v1".to_string()))
                .as_str()
                .map(|s| {
                    if s.is_empty() {
                        "https://api.openai.com/v1".to_string()
                    } else {
                        s.to_string()
                    }
                })
                .unwrap_or("https://api.openai.com/v1".to_string()),
            LLM_API_KEY: configuration
                .get("LLM_API_KEY")
                .unwrap_or(&json!("".to_string()))
                .as_str()
                .map(|s| {
                    if s.is_empty() {
                        "".to_string()
                    } else {
                        s.to_string()
                    }
                })
                .unwrap_or("".to_string()),
            RERANKER_API_KEY: configuration
                .get("RERANKER_API_KEY")
                .unwrap_or(&json!("".to_string()))
                .as_str()
                .map(|s| {
                    if s.is_empty() {
                        "".to_string()
                    } else {
                        s.to_string()
                    }
                })
                .unwrap_or("".to_string()),
            RERANKER_MODEL_NAME: configuration
                .get("RERANKER_MODEL_NAME")
                .unwrap_or(&json!("bge-reranker-large".to_string()))
                .as_str()
                .map(|s| {
                    if s.is_empty() {
                        "bge-reranker-large".to_string()
                    } else {
                        s.to_string()
                    }
                })
                .unwrap_or("".to_string()),
            EMBEDDING_BASE_URL: configuration
                .get("EMBEDDING_BASE_URL")
                .unwrap_or(&json!(get_env!("OPENAI_BASE_URL", "OPENAI_BASE_URL must be set").to_string()))
                .as_str()
                .map(|s| {
                    if s.is_empty() {
                        get_env!("OPENAI_BASE_URL", "OPENAI_BASE_URL must be set").to_string()
                    } else {
                        s.to_string()
                    }
                }).expect("EMBEDDING_BASE_URL should exist"),
            MESSAGE_TO_QUERY_PROMPT: configuration
                .get("MESSAGE_TO_QUERY_PROMPT")
                .unwrap_or(&json!("Write a 1-2 sentence semantic search query along the lines of a hypothetical response to: \n\n".to_string()))
                .as_str()
                .map(|s| {
                    if s.is_empty() {
                        "Write a 1-2 sentence semantic search query along the lines of a hypothetical response to: \n\n".to_string()
                    } else {
                        s.to_string()
                    }
                }).unwrap_or("Write a 1-2 sentence semantic search query along the lines of a hypothetical response to: \n\n".to_string()),
            RAG_PROMPT: configuration
                .get("RAG_PROMPT")
                .unwrap_or(&json!("Use the following retrieved documents to respond briefly and accurately:".to_string()))
                .as_str()
                .map(|s|
                    if s.is_empty() {
                        "Use the following retrieved documents to respond briefly and accurately:".to_string()
                    } else {
                        s.to_string()
                    }
                )
                .unwrap_or("Use the following retrieved documents to respond briefly and accurately:".to_string()),
            N_RETRIEVALS_TO_INCLUDE: configuration
                .get("N_RETRIEVALS_TO_INCLUDE")
                .unwrap_or(&json!(5))
                .as_u64()
                .map(|u| u as usize)
                .unwrap_or(5),
            EMBEDDING_SIZE: configuration
                .get("EMBEDDING_SIZE")
                .unwrap_or(&json!(1536))
                .as_u64()
                .map(|u| u as usize)
                .unwrap_or(1536),
            DISTANCE_METRIC: configuration
                .get("DISTANCE_METRIC")
                .unwrap_or(&json!("cosine"))
                .as_str()
                .map(|s| {
                    match s {
                        "cosine" => {
                            DistanceMetric::Cosine
                        },
                        "euclid" | "euclidean" => {
                            DistanceMetric::Euclidean
                        },
                        "dot" => {
                            DistanceMetric::Dot
                        },
                        "manhattan" => {
                            DistanceMetric::Manhattan
                        },
                        _ => {
                            DistanceMetric::Cosine
                        }
                    }
                }).unwrap_or(DistanceMetric::Cosine),
            EMBEDDING_MODEL_NAME: configuration
                .get("EMBEDDING_MODEL_NAME")
                .unwrap_or(&json!("text-embedding-3-small"))
                .as_str()
                .map(|s| {
                    if s.is_empty() {
                        "text-embedding-3-small".to_string()
                    } else {
                        s.to_string()
                    }
                })
                .unwrap_or("text-embedding-3-small".to_string()),
            RERANKER_BASE_URL: configuration
                .get("RERANKER_BASE_URL")
                .unwrap_or(&json!(get_env!("RERANKER_SERVER_ORIGIN", "RERANKER_SERVER_ORIGIN must be set").to_string()))
                .as_str()
                .map(|s| {
                    if s.is_empty() {
                        get_env!("RERANKER_SERVER_ORIGIN", "RERANKER_BASE_URL must be set").to_string()
                    } else {
                        s.to_string()
                    }
                }).expect("RERANKER_SERVER_ORIGIN should exist"),
            LLM_DEFAULT_MODEL: configuration
                .get("LLM_DEFAULT_MODEL")
                .unwrap_or(&json!("gpt-4o"))
                .as_str()
                .map(|s| {
                    if s.is_empty() {
                        "gpt-4o".to_string()
                    } else {
                        s.to_string()
                    }
                })
                .unwrap_or("gpt-4o".to_string()),
            FULLTEXT_ENABLED: configuration
                .get("FULLTEXT_ENABLED")
                .unwrap_or(&json!(true))
                .as_bool()
                .unwrap_or(true),
            SEMANTIC_ENABLED: configuration
                .get("SEMANTIC_ENABLED")
                .unwrap_or(&json!(true))
                .as_bool()
                .unwrap_or(true),
            QDRANT_ONLY: configuration
                .get("QDRANT_ONLY")
                .unwrap_or(&json!(false))
                .as_bool()
                .unwrap_or(false),
            BM25_ENABLED: configuration
                .get("BM25_ENABLED")
                .or(std::env::var("BM25_ACTIVE").ok().map(|val| json!(
                    val == "true"
                )).as_ref())
                .unwrap_or(&json!(false))
                .as_bool()
                .unwrap_or(false),
            BM25_B: configuration
                .get("BM25_B")
                .and_then(|v| v.as_f64().map(|f| f as f32))
                .unwrap_or(0.75f32),
            BM25_K: configuration
                .get("BM25_K")
                .and_then(|v| v.as_f64().map(|f| f as f32))
                .unwrap_or(1.2f32),
            BM25_AVG_LEN: configuration
                .get("BM25_AVG_LEN")
                .and_then(|v| v.as_f64().map(|f| f as f32))
                .unwrap_or(256f32),
            EMBEDDING_QUERY_PREFIX: configuration
                .get("EMBEDDING_QUERY_PREFIX")
                .unwrap_or(&{
                    let model_name = configuration
                        .get("EMBEDDING_MODEL_NAME")
                        .unwrap_or(&json!("text-embedding-3-small"))
                        .as_str()
                        .map(|s| {
                            if s.is_empty() {
                                "text-embedding-3-small".to_string()
                            } else {
                                s.to_string()
                            }
                        })
                        .unwrap_or("text-embedding-3-small".to_string());
                    if model_name == "jina-base-en" {
                        json!("Search for")
                    } else {
                        json!("")
                    }
                })
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or("".to_string()),
            USE_MESSAGE_TO_QUERY_PROMPT: configuration
                .get("USE_MESSAGE_TO_QUERY_PROMPT")
                .unwrap_or(&json!(false))
                .as_bool()
                .unwrap_or(false),
            FREQUENCY_PENALTY: configuration
                .get("FREQUENCY_PENALTY")
                .and_then(|v| v.as_f64()),
            TEMPERATURE: configuration
                .get("TEMPERATURE")
                .and_then(|v| v.as_f64()),
            PRESENCE_PENALTY: configuration
                .get("PRESENCE_PENALTY")
                .and_then(|v| v.as_f64()),
            STOP_TOKENS: configuration
                .get("STOP_TOKENS")
                .and_then(|v| v.as_str())
                .map(|v| v.split(',').map(|s| s.to_string()).collect::<Vec<String>>()),
            INDEXED_ONLY: configuration
                .get("INDEXED_ONLY")
                .unwrap_or(&json!(false))
                .as_bool()
                .unwrap_or(false),
            LOCKED: configuration
                .get("LOCKED")
                .unwrap_or(&json!(false))
                .as_bool()
                .unwrap_or(false),
            SYSTEM_PROMPT: configuration
                .get("SYSTEM_PROMPT")
                .and_then(|v| v.as_str())
                .map(|s|
                    if s.is_empty() {
                        "You are a helpful assistant".to_string()
                    } else {
                        s.to_string()
                    }
                )
                .unwrap_or("You are a helpful assistant".to_string()),
            MAX_LIMIT: configuration
                .get("MAX_LIMIT")
                .unwrap_or(&json!(10_000))
                .as_u64()
                .unwrap_or(10_000),
            MAX_TOKENS: configuration
                .get("MAX_TOKENS")
                .and_then(|v| v.as_u64()),
            PUBLIC_DATASET: PublicDatasetOptions {
                enabled: configuration_json.pointer("/PUBLIC_DATASET/enabled").unwrap_or(&json!(false)).as_bool().unwrap_or(false),
                api_key: Some(configuration_json.pointer("/PUBLIC_DATASET/api_key").unwrap_or(&json!("")).as_str().unwrap_or("").to_string()),
                extra_params
            },
            DISABLE_ANALYTICS: configuration
                .get("DISABLE_ANALYTICS")
                .unwrap_or(&json!(false))
                .as_bool()
                .unwrap_or(false),
            PAGEFIND_ENABLED: configuration
                .get("PAGEFIND_ENABLED")
                .unwrap_or(&json!(false))
                .as_bool()
                .unwrap_or(false),
            TASK_DEFINITION: configuration
            .get("TASK_DEFINITION")
            .unwrap_or(&json!("Your task is to grade the relevance of context document(s) against the specified user query.".to_string()))
            .as_str()
            .map(|s| {
                if s.is_empty() {
                    "Your task is to grade the relevance of context document(s) against the specified user query.".to_string()
                } else {
                    s.to_string()
                }
            }).unwrap_or("Your task is to grade the relevance of context document(s) against the specified user query.".to_string()),
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        let extra_params_json = serde_json::to_value(self.PUBLIC_DATASET.clone().extra_params).ok();

        json!({
            "LLM_BASE_URL": self.LLM_BASE_URL,
            "LLM_API_KEY": self.LLM_API_KEY,
            "RERANKER_API_KEY": self.RERANKER_API_KEY,
            "RERANKER_BASE_URL": self.RERANKER_BASE_URL,
            "RERANKER_MODEL_NAME": self.RERANKER_MODEL_NAME,
            "EMBEDDING_BASE_URL": self.EMBEDDING_BASE_URL,
            "EMBEDDING_MODEL_NAME": self.EMBEDDING_MODEL_NAME,
            "MESSAGE_TO_QUERY_PROMPT": self.MESSAGE_TO_QUERY_PROMPT,
            "RAG_PROMPT": self.RAG_PROMPT,
            "N_RETRIEVALS_TO_INCLUDE": self.N_RETRIEVALS_TO_INCLUDE,
            "EMBEDDING_SIZE": self.EMBEDDING_SIZE,
            "LLM_DEFAULT_MODEL": self.LLM_DEFAULT_MODEL,
            "BM25_ENABLED": self.BM25_ENABLED,
            "BM25_B": self.BM25_B,
            "BM25_K": self.BM25_K,
            "BM25_AVG_LEN": self.BM25_AVG_LEN,
            "FULLTEXT_ENABLED": self.FULLTEXT_ENABLED,
            "SEMANTIC_ENABLED": self.SEMANTIC_ENABLED,
            "QDRANT_ONLY": self.QDRANT_ONLY,
            "EMBEDDING_QUERY_PREFIX": self.EMBEDDING_QUERY_PREFIX,
            "USE_MESSAGE_TO_QUERY_PROMPT": self.USE_MESSAGE_TO_QUERY_PROMPT,
            "FREQUENCY_PENALTY": self.FREQUENCY_PENALTY,
            "TEMPERATURE": self.TEMPERATURE,
            "PRESENCE_PENALTY": self.PRESENCE_PENALTY,
            "STOP_TOKENS": self.STOP_TOKENS,
            "INDEXED_ONLY": self.INDEXED_ONLY,
            "LOCKED": self.LOCKED,
            "SYSTEM_PROMPT": self.SYSTEM_PROMPT,
            "MAX_LIMIT": self.MAX_LIMIT,
            "MAX_TOKENS": self.MAX_TOKENS,
            "PUBLIC_DATASET" : {
                "enabled": self.PUBLIC_DATASET.enabled,
                "api_key": self.PUBLIC_DATASET.api_key,
                "extra_params": extra_params_json
            },
            "DISABLE_ANALYTICS": self.DISABLE_ANALYTICS,
            "PAGEFIND_ENABLED": self.PAGEFIND_ENABLED,
            "TASK_DEFINITION": self.TASK_DEFINITION,
        })
    }
}

impl DatasetConfigurationDTO {
    pub fn from_curr_dataset(
        &self,
        curr_dataset_config: DatasetConfiguration,
    ) -> DatasetConfiguration {
        let page_parameters_self = self
            .PUBLIC_DATASET
            .clone()
            .map(|public_dataset| public_dataset.extra_params.unwrap_or_default())
            .unwrap_or_default();
        let page_parameters_curr = curr_dataset_config
            .PUBLIC_DATASET
            .extra_params
            .unwrap_or_default();

        let mut public_dataset_api_key = self
            .PUBLIC_DATASET
            .clone()
            .map(|public_dataset| public_dataset.api_key)
            .clone()
            .unwrap_or_default();

        if public_dataset_api_key.is_none() {
            public_dataset_api_key = curr_dataset_config.PUBLIC_DATASET.api_key;
        }
        println!("public_dataset {:?}", public_dataset_api_key);

        DatasetConfiguration {
            LLM_BASE_URL: self
                .LLM_BASE_URL
                .clone()
                .unwrap_or(curr_dataset_config.LLM_BASE_URL),
            LLM_API_KEY: self
                .LLM_API_KEY
                .clone()
                .unwrap_or(curr_dataset_config.LLM_API_KEY),
            RERANKER_API_KEY: self
                .RERANKER_API_KEY
                .clone()
                .unwrap_or(curr_dataset_config.RERANKER_API_KEY),
            RERANKER_MODEL_NAME: self
                .RERANKER_MODEL_NAME
                .clone()
                .unwrap_or(curr_dataset_config.RERANKER_MODEL_NAME),
            EMBEDDING_BASE_URL: self
                .EMBEDDING_BASE_URL
                .clone()
                .unwrap_or(curr_dataset_config.EMBEDDING_BASE_URL),
            EMBEDDING_MODEL_NAME: self
                .EMBEDDING_MODEL_NAME
                .clone()
                .unwrap_or(curr_dataset_config.EMBEDDING_MODEL_NAME),
            RERANKER_BASE_URL: self
                .RERANKER_BASE_URL
                .clone()
                .unwrap_or(curr_dataset_config.RERANKER_BASE_URL),
            MESSAGE_TO_QUERY_PROMPT: self
                .MESSAGE_TO_QUERY_PROMPT
                .clone()
                .unwrap_or(curr_dataset_config.MESSAGE_TO_QUERY_PROMPT),
            RAG_PROMPT: self
                .RAG_PROMPT
                .clone()
                .unwrap_or(curr_dataset_config.RAG_PROMPT),
            N_RETRIEVALS_TO_INCLUDE: self
                .N_RETRIEVALS_TO_INCLUDE
                .unwrap_or(curr_dataset_config.N_RETRIEVALS_TO_INCLUDE),
            EMBEDDING_SIZE: self
                .EMBEDDING_SIZE
                .unwrap_or(curr_dataset_config.EMBEDDING_SIZE),
            DISTANCE_METRIC: self
                .DISTANCE_METRIC
                .clone()
                .unwrap_or(curr_dataset_config.DISTANCE_METRIC),
            LLM_DEFAULT_MODEL: self
                .LLM_DEFAULT_MODEL
                .clone()
                .unwrap_or(curr_dataset_config.LLM_DEFAULT_MODEL),
            BM25_ENABLED: self
                .BM25_ENABLED
                .unwrap_or(curr_dataset_config.BM25_ENABLED),
            BM25_B: self.BM25_B.unwrap_or(curr_dataset_config.BM25_B),
            BM25_K: self.BM25_K.unwrap_or(curr_dataset_config.BM25_K),
            BM25_AVG_LEN: self
                .BM25_AVG_LEN
                .unwrap_or(curr_dataset_config.BM25_AVG_LEN),
            FULLTEXT_ENABLED: self
                .FULLTEXT_ENABLED
                .unwrap_or(curr_dataset_config.FULLTEXT_ENABLED),
            SEMANTIC_ENABLED: self
                .SEMANTIC_ENABLED
                .unwrap_or(curr_dataset_config.SEMANTIC_ENABLED),
            QDRANT_ONLY: self.QDRANT_ONLY.unwrap_or(curr_dataset_config.QDRANT_ONLY),
            EMBEDDING_QUERY_PREFIX: self
                .EMBEDDING_QUERY_PREFIX
                .clone()
                .unwrap_or(curr_dataset_config.EMBEDDING_QUERY_PREFIX),
            USE_MESSAGE_TO_QUERY_PROMPT: self
                .USE_MESSAGE_TO_QUERY_PROMPT
                .unwrap_or(curr_dataset_config.USE_MESSAGE_TO_QUERY_PROMPT),
            FREQUENCY_PENALTY: self
                .FREQUENCY_PENALTY
                .or(curr_dataset_config.FREQUENCY_PENALTY),
            TEMPERATURE: self.TEMPERATURE.or(curr_dataset_config.TEMPERATURE),
            PRESENCE_PENALTY: self
                .PRESENCE_PENALTY
                .or(curr_dataset_config.PRESENCE_PENALTY),
            STOP_TOKENS: self.STOP_TOKENS.clone().or(curr_dataset_config.STOP_TOKENS),
            MAX_TOKENS: self.MAX_TOKENS.or(curr_dataset_config.MAX_TOKENS),
            INDEXED_ONLY: self
                .INDEXED_ONLY
                .unwrap_or(curr_dataset_config.INDEXED_ONLY),
            LOCKED: self.LOCKED.unwrap_or(curr_dataset_config.LOCKED),
            SYSTEM_PROMPT: self
                .SYSTEM_PROMPT
                .clone()
                .unwrap_or(curr_dataset_config.SYSTEM_PROMPT),
            MAX_LIMIT: self.MAX_LIMIT.unwrap_or(curr_dataset_config.MAX_LIMIT),
            PUBLIC_DATASET: PublicDatasetOptions {
                enabled: self
                    .PUBLIC_DATASET
                    .clone()
                    .map(|dataset| dataset.enabled)
                    .unwrap_or(curr_dataset_config.PUBLIC_DATASET.enabled),
                api_key: public_dataset_api_key,
                extra_params: Some(PublicPageParameters {
                    dataset_id: page_parameters_self
                        .dataset_id
                        .or(page_parameters_curr.dataset_id),
                    base_url: page_parameters_self
                        .base_url
                        .or(page_parameters_curr.base_url),
                    r#type: page_parameters_self.r#type.or(page_parameters_curr.r#type),
                    api_key: page_parameters_self
                        .api_key
                        .or(page_parameters_curr.api_key),
                    analytics: page_parameters_self
                        .analytics
                        .or(page_parameters_curr.analytics),
                    tags: page_parameters_self.tags.or(page_parameters_curr.tags),
                    suggested_queries: page_parameters_self
                        .suggested_queries
                        .or(page_parameters_curr.suggested_queries),
                    followup_questions: page_parameters_self
                        .followup_questions
                        .or(page_parameters_curr.followup_questions),
                    inline: page_parameters_self.inline.or(page_parameters_curr.inline),
                    responsive: page_parameters_self
                        .responsive
                        .or(page_parameters_curr.responsive),
                    chat: page_parameters_self.chat.or(page_parameters_curr.chat),
                    theme: page_parameters_self.theme.or(page_parameters_curr.theme),
                    search_options: page_parameters_self
                        .search_options
                        .or(page_parameters_curr.search_options),
                    heading_prefix: page_parameters_self
                        .heading_prefix
                        .or(page_parameters_curr.heading_prefix),
                    for_brand_name: page_parameters_self
                        .for_brand_name
                        .or(page_parameters_curr.for_brand_name),
                    brand_name: page_parameters_self
                        .brand_name
                        .or(page_parameters_curr.brand_name),
                    brand_logo_img_src_url: page_parameters_self
                        .brand_logo_img_src_url
                        .or(page_parameters_curr.brand_logo_img_src_url),
                    nav_logo_img_src_url: page_parameters_self
                        .nav_logo_img_src_url
                        .or(page_parameters_curr.nav_logo_img_src_url),
                    problem_link: page_parameters_self
                        .problem_link
                        .or(page_parameters_curr.problem_link),
                    brand_color: page_parameters_self
                        .brand_color
                        .or(page_parameters_curr.brand_color),
                    placeholder: page_parameters_self
                        .placeholder
                        .or(page_parameters_curr.placeholder),
                    default_search_queries: page_parameters_self
                        .default_search_queries
                        .or(page_parameters_curr.default_search_queries),
                    default_ai_questions: page_parameters_self
                        .default_ai_questions
                        .or(page_parameters_curr.default_ai_questions),
                    allow_switching_modes: page_parameters_self
                        .allow_switching_modes
                        .or(page_parameters_curr.allow_switching_modes),
                    currency_position: page_parameters_self
                        .currency_position
                        .or(page_parameters_curr.currency_position),
                    floating_button_position: page_parameters_self
                        .floating_button_position
                        .or(page_parameters_curr.floating_button_position),
                    floating_search_icon_position: page_parameters_self
                        .floating_search_icon_position
                        .or(page_parameters_curr.floating_search_icon_position),
                    show_floating_button: page_parameters_self
                        .show_floating_button
                        .or(page_parameters_curr.show_floating_button),
                    show_floating_search_icon: page_parameters_self
                        .show_floating_search_icon
                        .or(page_parameters_curr.show_floating_search_icon),
                    show_floating_input: page_parameters_self
                        .show_floating_input
                        .or(page_parameters_curr.show_floating_input),
                    button_triggers: page_parameters_self
                        .button_triggers
                        .or(page_parameters_curr.button_triggers),
                    debounce_ms: page_parameters_self
                        .debounce_ms
                        .or(page_parameters_curr.debounce_ms),
                    default_currency: page_parameters_self
                        .default_currency
                        .or(page_parameters_curr.default_currency),
                    default_search_mode: page_parameters_self
                        .default_search_mode
                        .or(page_parameters_curr.default_search_mode),
                    use_group_search: page_parameters_self
                        .use_group_search
                        .or(page_parameters_curr.use_group_search),
                    hero_pattern: Some(
                        page_parameters_self
                            .hero_pattern
                            .or(page_parameters_curr.hero_pattern)
                            .unwrap_or_default(),
                    ),
                    tab_messages: page_parameters_self
                        .tab_messages
                        .or(page_parameters_curr.tab_messages),
                    open_graph_metadata: page_parameters_self
                        .open_graph_metadata
                        .or(page_parameters_curr.open_graph_metadata),
                    single_product_options: page_parameters_self
                        .single_product_options
                        .or(page_parameters_curr.single_product_options),
                    open_links_in_new_tab: page_parameters_self
                        .open_links_in_new_tab
                        .or(page_parameters_curr.open_links_in_new_tab),
                    creator_name: page_parameters_self
                        .creator_name
                        .or(page_parameters_curr.creator_name),
                    creator_linked_in_url: page_parameters_self
                        .creator_linked_in_url
                        .or(page_parameters_curr.creator_linked_in_url),
                    brand_font_family: page_parameters_self
                        .brand_font_family
                        .or(page_parameters_curr.brand_font_family),
                    z_index: page_parameters_self
                        .z_index
                        .or(page_parameters_curr.z_index),
                    hide_drawn_text: page_parameters_self
                        .hide_drawn_text
                        .or(page_parameters_curr.hide_drawn_text),
                    use_pagefind: page_parameters_self
                        .use_pagefind
                        .or(page_parameters_curr.use_pagefind),
                    video_link: page_parameters_self
                        .video_link
                        .or(page_parameters_curr.video_link),
                    video_position: page_parameters_self
                        .video_position
                        .or(page_parameters_curr.video_position),
                    is_test_mode: page_parameters_self
                        .is_test_mode
                        .or(page_parameters_curr.is_test_mode),
                    number_of_suggestions: page_parameters_self
                        .number_of_suggestions
                        .or(page_parameters_curr.number_of_suggestions),
                    inline_header: page_parameters_self
                        .inline_header
                        .or(page_parameters_curr.inline_header),
                    default_image_question: page_parameters_self
                        .default_image_question
                        .or(page_parameters_curr.default_image_question),
                    use_local: page_parameters_self
                        .use_local
                        .or(page_parameters_curr.use_local),
                    show_result_highlights: page_parameters_self
                        .show_result_highlights
                        .or(page_parameters_curr.show_result_highlights),
                }),
            },
            DISABLE_ANALYTICS: self
                .DISABLE_ANALYTICS
                .unwrap_or(curr_dataset_config.DISABLE_ANALYTICS),
            PAGEFIND_ENABLED: self
                .PAGEFIND_ENABLED
                .unwrap_or(curr_dataset_config.PAGEFIND_ENABLED),
            TASK_DEFINITION: self
            .TASK_DEFINITION
            .clone()
            .unwrap_or(curr_dataset_config.TASK_DEFINITION),
                
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[schema(example=json!({
    "CREATE_CHUNK_FEATURE": true,
    "SEARCH_QUERIES": "search queries",
    "FRONTMATTER_VALS": "frontmatter vals",
    "LINES_BEFORE_SHOW_MORE": 10,
    "DATE_RANGE_VALUE": "date range value",
    "FILTER_ITEMS": [],
    "SUGGESTED_QUERIES": "suggested queries",
    "IMAGE_RANGE_START_KEY": "image range start key",
    "IMAGE_RANGE_END_KEY": "image range end key",
    "DOCUMENT_UPLOAD_FEATURE": true,
    "FILE_NAME_KEY": "file_name_key",
}))]

pub struct DatasetAndOrgWithSubAndPlan {
    pub dataset: Dataset,
    pub organization: OrganizationWithSubAndPlan,
}

impl DatasetAndOrgWithSubAndPlan {
    pub fn from_components(dataset: Dataset, organization: OrganizationWithSubAndPlan) -> Self {
        DatasetAndOrgWithSubAndPlan {
            dataset,
            organization,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[schema(example=json!({
    "COMPANY_NAME": "Trieve",
    "FAVICON_URL": "https://cdn.trieve.ai/favicon.ico",
    "DEMO_DOMAIN": "demos.trieve.ai",
}))]
#[allow(non_snake_case)]
pub struct PartnerConfiguration {
    pub COMPANY_NAME: String,
    pub COMPANY_URL: String,
    pub FAVICON_URL: String,
    pub DEMO_DOMAIN: String,
    pub CALENDAR_LINK: String,
    pub SLACK_LINK: String,
    pub LINKEDIN_LINK: String,
    pub EMAIL: String,
    pub PHONE: String,
}

impl PartnerConfiguration {
    pub fn from_json(configuration_json: serde_json::Value) -> Self {
        let default_config = json!({});

        let configuration = configuration_json.as_object().unwrap_or(
            default_config
                .as_object()
                .expect("Will always be valid object here"),
        );

        PartnerConfiguration {
            COMPANY_NAME: configuration
                .get("COMPANY_NAME")
                .unwrap_or(&json!("Trieve".to_string()))
                .as_str()
                .map(|str| str.to_string())
                .unwrap_or("Trieve".to_string()),
            COMPANY_URL: configuration
                .get("COMPANY_URL")
                .unwrap_or(&json!("https://trieve.ai".to_string()))
                .as_str()
                .map(|str| str.to_string())
                .unwrap_or("https://trieve.ai".to_string()),
            FAVICON_URL: configuration
                .get("FAVICON_URL")
                .unwrap_or(&json!("https://cdn.trieve.ai/favicon.ico"))
                .as_str()
                .map(|str| str.to_string())
                .unwrap_or("https://cdn.trieve.ai/favicon.ico".to_string()),
            DEMO_DOMAIN: configuration
                .get("DEMO_DOMAIN")
                .unwrap_or(&json!("demos.trieve.ai"))
                .as_str()
                .map(|str| str.to_string())
                .unwrap_or("demos.trieve.ai".to_string()),
            CALENDAR_LINK: configuration
                .get("CALENDAR_LINK")
                .unwrap_or(&json!("https://cal.com/nick.k"))
                .as_str()
                .map(|str| str.to_string())
                .unwrap_or("https://cal.com/nick.k".to_string()),
            EMAIL: configuration
                .get("EMAIL")
                .unwrap_or(&json!("humans@trieve.ai"))
                .as_str()
                .map(|str| str.to_string())
                .unwrap_or("humans@trieve.ai".to_string()),
            PHONE: configuration
                .get("PHONE")
                .unwrap_or(&json!("+16282224090"))
                .as_str()
                .map(|str| str.to_string())
                .unwrap_or("+16282224090".to_string()),
            SLACK_LINK: configuration
                .get("SLACK_LINK")
                .unwrap_or(&json!("https://join.slack.com/t/trievecommunityslack/shared_invite/zt-2v7zyxytf-zWLVgqI8avB5x5Br7INQgw"))
                .as_str()
                .map(|str| str.to_string())
                .unwrap_or("https://join.slack.com/t/trievecommunityslack/shared_invite/zt-2v7zyxytf-zWLVgqI8avB5x5Br7INQgw".to_string()),
            LINKEDIN_LINK: configuration
                .get("LINKEDIN_LINK")
                .unwrap_or(&json!("https://www.linkedin.com/company/trieveai"))
                .as_str()
                .map(|str| str.to_string())
                .unwrap_or("https://www.linkedin.com/company/trieveai".to_string()),
        }
    }
}

impl From<serde_json::Value> for PartnerConfiguration {
    fn from(configuration_json: serde_json::Value) -> Self {
        PartnerConfiguration::from_json(configuration_json)
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, Selectable, Clone, ToSchema)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "name": "Trieve",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "registerable": true,
    "partner_configuration": {
        "COMPANY_NAME": "Trieve",
        "FAVICON_URL": "https://cdn.trieve.ai/favicon.ico",
        "DEMO_DOMAIN": "demos.trieve.ai",
    }
}))]
#[diesel(table_name = organizations)]
pub struct Organization {
    /// Unique identifier of the dataset, auto-generated uuid created by Trieve
    pub id: uuid::Uuid,
    /// Name of the organization
    pub name: String,
    /// Timestamp of the creation of the dataset
    pub created_at: chrono::NaiveDateTime,
    /// Timestamp of the last update of the dataset
    pub updated_at: chrono::NaiveDateTime,
    /// Flag to indicate whether or not new users may join the organization. Default is true.
    pub registerable: Option<bool>,
    /// Flag to indicate if the organization has been deleted. Deletes are handled async after the flag is set so as to avoid expensive search index compaction.
    pub deleted: i32,
    /// Configuration of the organization for the Trieve partner program. Contact partnerships@trieve.ai for more details.
    pub partner_configuration: serde_json::Value,
}

impl Organization {
    pub fn from_details(name: String) -> Self {
        Organization {
            id: uuid::Uuid::new_v4(),
            name,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            registerable: Some(true),
            deleted: 0,
            partner_configuration: json!({}),
        }
    }

    pub fn from_org_with_plan_sub(org_plan_sub: OrganizationWithSubAndPlan) -> Self {
        org_plan_sub.organization.with_complete_partner_config()
    }

    pub fn with_complete_partner_config(&self) -> Self {
        let mut cur = self.clone();
        cur.partner_configuration =
            serde_json::to_value(PartnerConfiguration::from_json(cur.partner_configuration))
                .unwrap_or_default();
        cur
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, ValidGrouping, ToSchema, Clone)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "email": "trieve@trieve.ai",
    "organization_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "used": false,
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "role": 1,
}))]
#[diesel(table_name = invitations)]
pub struct Invitation {
    pub id: uuid::Uuid,
    pub email: String,
    pub organization_id: uuid::Uuid,
    pub used: bool,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub role: i32,
}

// any type that implements Into<String> can be used to create Invitation
impl Invitation {
    pub fn from_details(email: String, organization_id: uuid::Uuid, role: i32) -> Self {
        Invitation {
            id: uuid::Uuid::new_v4(),
            email,
            organization_id,
            used: false,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            role,
        }
    }
}

#[derive(
    Debug, Serialize, Deserialize, Selectable, Clone, Queryable, Insertable, ValidGrouping, ToSchema,
)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "stripe_id": "plan_123",
    "chunk_count": 1000,
    "file_storage": 512,
    "user_count": 5,
    "dataset_count": 1,
    "message_count": 1000,
    "amount": 1000,
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "name": "Free",
}))]
#[diesel(table_name = stripe_plans)]
pub struct StripePlan {
    pub id: uuid::Uuid,
    pub stripe_id: String,
    pub chunk_count: i32,
    pub file_storage: i64,
    pub user_count: i32,
    pub dataset_count: i32,
    pub message_count: i32,
    pub amount: i64,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub name: String,
    pub visible: bool,
}

impl StripePlan {
    #[allow(clippy::too_many_arguments)]
    pub fn from_details(
        stripe_id: String,
        chunk_count: i32,
        file_storage: i64,
        user_count: i32,
        dataset_count: i32,
        message_count: i32,
        amount: i64,
        name: String,
        visible: bool,
    ) -> Self {
        StripePlan {
            id: uuid::Uuid::new_v4(),
            stripe_id,
            chunk_count,
            file_storage,
            user_count,
            dataset_count,
            message_count,
            amount,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            name,
            visible,
        }
    }
}

impl Default for StripePlan {
    fn default() -> Self {
        let unlimited = std::env::var("UNLIMITED").unwrap_or("false".to_string());
        if unlimited == "true" {
            return StripePlan {
                id: uuid::Uuid::default(),
                stripe_id: "".to_string(),
                chunk_count: i32::MAX,
                file_storage: i64::MAX,
                user_count: i32::MAX,
                dataset_count: i32::MAX,
                message_count: i32::MAX,
                amount: 0,
                created_at: chrono::Utc::now().naive_local(),
                updated_at: chrono::Utc::now().naive_local(),
                name: "Unlimited".to_string(),
                visible: true,
            };
        }

        StripePlan {
            id: uuid::Uuid::default(),
            stripe_id: "".to_string(),
            chunk_count: 1000,
            file_storage: 1024,
            user_count: 5,
            dataset_count: 10,
            message_count: 1000,
            amount: 0,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            name: "Free".to_string(),
            visible: true,
        }
    }
}
#[derive(
    Debug, Serialize, Deserialize, Selectable, Clone, Queryable, Insertable, ValidGrouping, ToSchema,
)]
#[schema(example=json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "stripe_id": "sub_123",
    "plan_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "organization_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "current_period_end": "2021-01-01 00:00:00.000",
}))]
#[diesel(table_name = stripe_subscriptions)]
pub struct StripeSubscription {
    pub id: uuid::Uuid,
    pub stripe_id: String,
    pub plan_id: uuid::Uuid,
    pub organization_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub current_period_end: Option<chrono::NaiveDateTime>,
}

impl StripeSubscription {
    pub fn from_details(
        stripe_id: String,
        plan_id: uuid::Uuid,
        organization_id: uuid::Uuid,
        current_period_end: Option<chrono::NaiveDateTime>,
    ) -> Self {
        StripeSubscription {
            id: uuid::Uuid::new_v4(),
            stripe_id,
            plan_id,
            organization_id,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            current_period_end,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(example = json!({
    "organization": {
        "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
        "name": "Trieve",
        "created_at": "2021-01-01 00:00:00.000",
        "updated_at": "2021-01-01 00:00:00.000",
        "registerable": true,
    },
    "plan": {
        "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
        "stripe_id": "plan_123",
        "chunk_count": 1000,
        "file_storage": 512,
        "user_count": 5,
        "dataset_count": 1,
        "message_count": 1000,
        "amount": 1000,
        "created_at": "2021-01-01 00:00:00.000",
        "updated_at": "2021-01-01 00:00:00.000",
        "name": "Free",
    },
    "subscription": {
        "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
        "stripe_id": "sub_123",
        "plan_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
        "organization_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
        "created_at": "2021-01-01 00:00:00.000",
        "updated_at": "2021-01-01 00:00:00.000",
        "current_period_end": "2021-01-01 00:00:00.000",
    }
}))]
pub struct OrganizationWithSubAndPlan {
    pub organization: Organization,
    pub plan: Option<StripePlan>,
    pub subscription: Option<StripeSubscription>,
}

impl OrganizationWithSubAndPlan {
    pub fn from_components(
        organization: Organization,
        plan: Option<StripePlan>,
        subscription: Option<StripeSubscription>,
    ) -> Self {
        OrganizationWithSubAndPlan {
            organization: organization.with_complete_partner_config(),
            plan,
            subscription,
        }
    }

    pub fn with_defaults(&self) -> Self {
        OrganizationWithSubAndPlan {
            organization: self.organization.with_complete_partner_config(),
            plan: Some(self.plan.clone().unwrap_or_default()),
            subscription: self.subscription.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Ord, PartialOrd)]
pub enum UserRole {
    Owner = 2,
    Admin = 1,
    User = 0,
}

impl From<i32> for UserRole {
    fn from(role: i32) -> Self {
        match role {
            2 => UserRole::Owner,
            1 => UserRole::Admin,
            _ => UserRole::User,
        }
    }
}

impl From<UserRole> for i32 {
    fn from(role: UserRole) -> Self {
        match role {
            UserRole::Owner => 2,
            UserRole::Admin => 1,
            UserRole::User => 0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, Selectable, Clone, ToSchema)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "user_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "organization_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "role": 2,
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
}))]
#[diesel(table_name = user_organizations)]
pub struct UserOrganization {
    pub id: uuid::Uuid,
    pub user_id: uuid::Uuid,
    pub organization_id: uuid::Uuid,
    pub role: i32,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl UserOrganization {
    pub fn from_details(user_id: uuid::Uuid, organization_id: uuid::Uuid, role: UserRole) -> Self {
        UserOrganization {
            id: uuid::Uuid::new_v4(),
            user_id,
            organization_id,
            role: role.into(),
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, Selectable, Clone, ToSchema)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "org_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "dataset_count": 1,
    "user_count": 5,
    "file_storage": 512,
    "message_count": 1000,
    "chunk_count": 1000,
}))]
#[diesel(table_name = organization_usage_counts)]
pub struct OrganizationUsageCount {
    pub id: uuid::Uuid,
    pub org_id: uuid::Uuid,
    pub dataset_count: i32,
    pub user_count: i32,
    pub file_storage: i64,
    pub message_count: i32,
    pub chunk_count: i32,
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, Selectable, Clone)]
#[diesel(table_name = dataset_tags)]
pub struct DatasetTags {
    pub id: uuid::Uuid,
    pub dataset_id: uuid::Uuid,
    pub tag: String,
}

impl DatasetTags {
    pub fn from_details(dataset_id: uuid::Uuid, tag: String) -> Self {
        DatasetTags {
            id: uuid::Uuid::new_v4(),
            dataset_id,
            tag,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, Selectable, Clone)]
#[diesel(table_name = chunk_metadata_tags)]
pub struct ChunkMetadataTags {
    pub id: uuid::Uuid,
    pub chunk_metadata_id: uuid::Uuid,
    pub tag_id: uuid::Uuid,
}

impl ChunkMetadataTags {
    pub fn from_details(chunk_metadata_id: uuid::Uuid, tag_id: uuid::Uuid) -> Self {
        ChunkMetadataTags {
            id: uuid::Uuid::new_v4(),
            chunk_metadata_id,
            tag_id,
        }
    }
}

#[derive(Debug)]
pub enum ApiKeyRole {
    Read = 0,
    Admin = 1,
    Owner = 2,
}

impl From<i32> for ApiKeyRole {
    fn from(role: i32) -> Self {
        match role {
            2 => ApiKeyRole::Owner,
            1 => ApiKeyRole::Admin,
            _ => ApiKeyRole::Read,
        }
    }
}

impl From<ApiKeyRole> for i32 {
    fn from(role: ApiKeyRole) -> Self {
        match role {
            ApiKeyRole::Owner => 2,
            ApiKeyRole::Admin => 1,
            ApiKeyRole::Read => 0,
        }
    }
}

/// The default parameters which will be forcibly used when the api key is given on a request. If not provided, the api key will not have default parameters.
#[derive(Serialize, Deserialize, Debug, ToSchema, Clone)]
pub struct ApiKeyRequestParams {
    /// Can be either "semantic", "fulltext", "hybrid", or "bm25". Default behavior varies by endpoint.
    pub search_type: Option<SearchMethod>,
    /// Page size is the number of chunks to fetch. This can be used to fetch more than 10 chunks at a time.
    pub page_size: Option<u64>,
    /// Filters is a JSON object which can be used to filter chunks. This is useful for when you want to filter chunks by arbitrary metadata.
    pub filters: Option<ChunkFilter>,
    /// Highlight Options lets you specify different methods to highlight the chunks in the result set.
    pub highlight_options: Option<HighlightOptions>,
    /// Set score_threshold to a float to filter out chunks with a score below the threshold.
    pub score_threshold: Option<f32>,
    /// Set slim_chunks to true to avoid returning the content and chunk_html of the chunks.
    pub slim_chunks: Option<bool>,
    /// If true, quoted and - prefixed words will be parsed from the queries and used as required and negated words respectively.
    pub use_quote_negated_terms: Option<bool>,
    /// If true, stop words will be removed. Queries that are entirely stop words will be preserved.
    pub remove_stop_words: Option<bool>,
    /// Options for handling typos in the search query
    pub typo_options: Option<TypoOptions>,
    /// Options for handling the response for the llm to return when no results are found
    pub no_result_message: Option<String>,
}

impl ApiKeyRequestParams {
    pub fn combine_with_create_message(
        self,
        payload: CreateMessageReqPayload,
    ) -> CreateMessageReqPayload {
        CreateMessageReqPayload {
            new_message_content: payload.new_message_content,
            topic_id: payload.topic_id,
            user_id: payload.user_id,
            sort_options: payload.sort_options,
            highlight_options: self.highlight_options.or(payload.highlight_options),
            search_type: self.search_type.or(payload.search_type),
            use_group_search: payload.use_group_search,
            concat_user_messages_query: payload.concat_user_messages_query,
            search_query: payload.search_query,
            page_size: self.page_size.or(payload.page_size),
            filters: self.filters.or(payload.filters),
            score_threshold: self.score_threshold.or(payload.score_threshold),
            llm_options: payload.llm_options,
            image_urls: payload.image_urls,
            audio_input: payload.audio_input,
            context_options: payload.context_options,
            no_result_message: self.no_result_message.or(payload.no_result_message),
            only_include_docs_used: payload.only_include_docs_used,
            use_quote_negated_terms: self
                .use_quote_negated_terms
                .or(payload.use_quote_negated_terms),
            remove_stop_words: self.remove_stop_words.or(payload.remove_stop_words),
            typo_options: self.typo_options.or(payload.typo_options),
        }
    }

    pub fn combine_with_search_chunks(
        self,
        payload: SearchChunksReqPayload,
    ) -> SearchChunksReqPayload {
        SearchChunksReqPayload {
            search_type: self.search_type.unwrap_or(payload.search_type),
            query: payload.query,
            page: payload.page,
            page_size: self.page_size.or(payload.page_size),
            get_total_pages: payload.get_total_pages,
            filters: self.filters.or(payload.filters),
            sort_options: payload.sort_options,
            scoring_options: payload.scoring_options,
            highlight_options: self.highlight_options.or(payload.highlight_options),
            score_threshold: self.score_threshold.or(payload.score_threshold),
            slim_chunks: self.slim_chunks.or(payload.slim_chunks),
            content_only: payload.content_only,
            use_quote_negated_terms: self
                .use_quote_negated_terms
                .or(payload.use_quote_negated_terms),
            remove_stop_words: self.remove_stop_words.or(payload.remove_stop_words),
            user_id: payload.user_id,
            typo_options: self.typo_options.or(payload.typo_options),
        }
    }

    pub fn combine_with_scroll_chunks(
        self,
        payload: ScrollChunksReqPayload,
    ) -> ScrollChunksReqPayload {
        ScrollChunksReqPayload {
            page_size: self.page_size.or(payload.page_size),
            filters: self.filters.or(payload.filters),
            offset_chunk_id: payload.offset_chunk_id,
            sort_by: payload.sort_by,
        }
    }

    pub fn combine_with_autocomplete(
        self,
        payload: AutocompleteReqPayload,
    ) -> AutocompleteReqPayload {
        AutocompleteReqPayload {
            search_type: self.search_type.unwrap_or(payload.search_type),
            extend_results: payload.extend_results,
            query: payload.query,
            page_size: self.page_size.or(payload.page_size),
            filters: self.filters.or(payload.filters),
            sort_options: payload.sort_options,
            scoring_options: payload.scoring_options,
            highlight_options: self.highlight_options.or(payload.highlight_options),
            score_threshold: self.score_threshold.or(payload.score_threshold),
            slim_chunks: self.slim_chunks.or(payload.slim_chunks),
            content_only: payload.content_only,
            use_quote_negated_terms: self
                .use_quote_negated_terms
                .or(payload.use_quote_negated_terms),
            remove_stop_words: self.remove_stop_words.or(payload.remove_stop_words),
            user_id: payload.user_id,
            typo_options: self.typo_options.or(payload.typo_options),
        }
    }

    pub fn combine_with_search_over_groups(
        self,
        payload: SearchOverGroupsReqPayload,
    ) -> SearchOverGroupsReqPayload {
        SearchOverGroupsReqPayload {
            search_type: self.search_type.unwrap_or(payload.search_type),
            query: payload.query,
            page: payload.page,
            page_size: self.page_size.or(payload.page_size),
            get_total_pages: payload.get_total_pages,
            filters: self.filters.or(payload.filters),
            highlight_options: self.highlight_options.or(payload.highlight_options),
            score_threshold: self.score_threshold.or(payload.score_threshold),
            group_size: payload.group_size,
            slim_chunks: self.slim_chunks.or(payload.slim_chunks),
            use_quote_negated_terms: self
                .use_quote_negated_terms
                .or(payload.use_quote_negated_terms),
            remove_stop_words: self.remove_stop_words.or(payload.remove_stop_words),
            user_id: payload.user_id,
            typo_options: self.typo_options.or(payload.typo_options),
            sort_options: payload.sort_options,
        }
    }

    pub fn combine_with_search_within_group(
        self,
        payload: SearchWithinGroupReqPayload,
    ) -> SearchWithinGroupReqPayload {
        SearchWithinGroupReqPayload {
            query: payload.query,
            page: payload.page,
            page_size: self.page_size.or(payload.page_size),
            get_total_pages: payload.get_total_pages,
            filters: self.filters.or(payload.filters),
            group_id: payload.group_id,
            group_tracking_id: payload.group_tracking_id,
            search_type: self.search_type.unwrap_or(payload.search_type),
            sort_options: payload.sort_options,
            highlight_options: self.highlight_options.or(payload.highlight_options),
            score_threshold: self.score_threshold.or(payload.score_threshold),
            slim_chunks: self.slim_chunks.or(payload.slim_chunks),
            content_only: payload.content_only,
            use_quote_negated_terms: self
                .use_quote_negated_terms
                .or(payload.use_quote_negated_terms),
            remove_stop_words: self.remove_stop_words.or(payload.remove_stop_words),
            user_id: payload.user_id,
            typo_options: self.typo_options.or(payload.typo_options),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, Selectable, Clone, ToSchema)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "user_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3",
    "api_key_hash": "hash",
    "name": "Trieve",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "role": 1,
    "blake3_hash": "hash",
}))]
#[diesel(table_name = user_api_key)]
pub struct UserApiKey {
    pub id: uuid::Uuid,
    pub user_id: uuid::Uuid,
    pub api_key_hash: Option<String>,
    pub name: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub role: i32,
    pub blake3_hash: Option<String>,
    pub dataset_ids: Option<Vec<Option<String>>>,
    pub organization_ids: Option<Vec<Option<String>>>,
    pub scopes: Option<Vec<Option<String>>>,
    pub params: Option<serde_json::Value>,
    pub expires_at: Option<chrono::NaiveDateTime>,
}

impl UserApiKey {
    #[allow(clippy::too_many_arguments)]
    pub fn from_details(
        user_id: uuid::Uuid,
        blake3_hash: String,
        name: String,
        role: ApiKeyRole,
        dataset_ids: Option<Vec<uuid::Uuid>>,
        organization_ids: Option<Vec<uuid::Uuid>>,
        scopes: Option<Vec<String>>,
        params: Option<ApiKeyRequestParams>,
        expires_at: Option<chrono::NaiveDateTime>,
    ) -> Self {
        UserApiKey {
            id: uuid::Uuid::new_v4(),
            user_id,
            api_key_hash: None,
            name,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            role: role.into(),
            blake3_hash: Some(blake3_hash),
            dataset_ids: dataset_ids
                .map(|ids| ids.into_iter().map(|id| Some(id.to_string())).collect()),
            organization_ids: organization_ids
                .map(|ids| ids.into_iter().map(|id| Some(id.to_string())).collect()),
            scopes: scopes.map(|scopes| scopes.into_iter().map(Some).collect()),
            params: if params.is_some() {
                serde_json::to_value(params).ok()
            } else {
                None
            },
            expires_at,
        }
    }
}
#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, Selectable, Clone, ToSchema)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "user_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3",
    "api_key_hash": "hash",
    "name": "Trieve",
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
    "role": 1,
}))]
#[diesel(table_name = organization_api_key)]
pub struct OrganizationApiKey {
    pub id: uuid::Uuid,
    pub organization_id: uuid::Uuid,
    pub api_key_hash: String,
    pub name: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub role: i32,
    pub dataset_ids: Option<Vec<Option<String>>>,
    pub scopes: Option<Vec<Option<String>>>,
    pub params: Option<serde_json::Value>,
    pub expires_at: Option<chrono::NaiveDateTime>,
}

impl From<OrganizationApiKey> for UserApiKey {
    fn from(api_key: OrganizationApiKey) -> Self {
        UserApiKey {
            id: api_key.id,
            user_id: uuid::Uuid::default(),
            api_key_hash: Some(api_key.api_key_hash),
            name: api_key.name,
            created_at: api_key.created_at,
            updated_at: api_key.updated_at,
            role: api_key.role,
            blake3_hash: None,
            dataset_ids: api_key.dataset_ids,
            organization_ids: Some(vec![Some(api_key.organization_id.to_string())]),
            scopes: api_key.scopes,
            params: api_key.params,
            expires_at: api_key.expires_at,
        }
    }
}

impl OrganizationApiKey {
    #[allow(clippy::too_many_arguments)]
    pub fn from_details(
        organization_id: uuid::Uuid,
        api_key_hash: String,
        name: String,
        role: ApiKeyRole,
        dataset_ids: Option<Vec<uuid::Uuid>>,
        scopes: Option<Vec<String>>,
        params: Option<ApiKeyRequestParams>,
        expires_at: Option<chrono::NaiveDateTime>,
    ) -> Self {
        OrganizationApiKey {
            id: uuid::Uuid::new_v4(),
            organization_id,
            api_key_hash,
            name,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            role: role.into(),
            dataset_ids: dataset_ids
                .map(|ids| ids.into_iter().map(|id| Some(id.to_string())).collect()),
            scopes: scopes.map(|scopes| scopes.into_iter().map(Some).collect()),
            params: if params.is_some() {
                serde_json::to_value(params).ok()
            } else {
                None
            },
            expires_at,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[schema(example = json!({
    "id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "organization_id": "e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3",
    "name": "Trieve",
    "role": 1,
    "dataset_ids": ["d0d0d0d0-d0d0-d0d0-d0d0-d0d0d0d0d0d0"],
    "created_at": "2021-01-01 00:00:00.000",
    "updated_at": "2021-01-01 00:00:00.000",
}))]
pub struct ApiKeyRespBody {
    pub id: uuid::Uuid,
    pub organization_id: uuid::Uuid,
    pub name: String,
    pub role: i32,
    pub organization_ids: Option<Vec<String>>,
    pub dataset_ids: Option<Vec<String>>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl From<OrganizationApiKey> for ApiKeyRespBody {
    fn from(api_key: OrganizationApiKey) -> Self {
        ApiKeyRespBody {
            id: api_key.id,
            organization_id: api_key.organization_id,
            name: api_key.name,
            role: api_key.role,
            organization_ids: None,
            dataset_ids: api_key
                .dataset_ids
                .map(|ids| ids.into_iter().flatten().collect()),
            created_at: api_key.created_at,
            updated_at: api_key.updated_at,
        }
    }
}

impl From<UserApiKey> for ApiKeyRespBody {
    fn from(api_key: UserApiKey) -> Self {
        ApiKeyRespBody {
            id: api_key.id,
            name: api_key.name,
            organization_id: uuid::Uuid::default(),
            role: api_key.role,
            dataset_ids: api_key
                .dataset_ids
                .map(|ids| ids.into_iter().flatten().collect()),
            organization_ids: api_key
                .organization_ids
                .map(|ids| ids.into_iter().flatten().collect()),
            created_at: api_key.created_at,
            updated_at: api_key.updated_at,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub enum UnifiedId {
    TrieveUuid(uuid::Uuid),
    TrackingId(String),
}

impl UnifiedId {
    pub fn as_uuid(&self) -> Option<uuid::Uuid> {
        match self {
            UnifiedId::TrackingId(_) => None,
            UnifiedId::TrieveUuid(uuid) => Some(*uuid),
        }
    }

    pub fn as_tracking_id(&self) -> Option<String> {
        match self {
            UnifiedId::TrackingId(tracking_id) => Some(tracking_id.clone()),
            UnifiedId::TrieveUuid(_) => None,
        }
    }
}

impl From<uuid::Uuid> for UnifiedId {
    fn from(uuid: uuid::Uuid) -> Self {
        UnifiedId::TrieveUuid(uuid)
    }
}

impl From<String> for UnifiedId {
    fn from(tracking_id: String) -> Self {
        UnifiedId::TrackingId(tracking_id)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct QdrantPayload {
    pub link: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub tracking_id: Option<String>,
    pub time_stamp: Option<i64>,
    pub num_value: Option<f64>,
    pub dataset_id: uuid::Uuid,
    pub weight: f64,
    pub location: Option<GeoInfo>,
    pub image_urls: Option<Vec<Option<String>>>,
    pub tag_set: Option<Vec<Option<String>>>,
    // different than QdrantChunkMetadata
    pub content: String,
    pub group_ids: Option<Vec<uuid::Uuid>>,
    pub group_tag_set: Option<Vec<Option<String>>>,
}

impl From<QdrantPayload> for Payload {
    fn from(val: QdrantPayload) -> Self {
        let value = json!(val);
        value
            .try_into()
            .expect("Failed to convert QdrantPayload to Payload")
    }
}

impl QdrantPayload {
    pub fn new(
        chunk_metadata: ChunkMetadata,
        group_ids: Option<Vec<uuid::Uuid>>,
        dataset_id: Option<uuid::Uuid>,
        group_tag_set: Option<Vec<Option<String>>>,
    ) -> Self {
        QdrantPayload {
            link: chunk_metadata.link,
            metadata: chunk_metadata.metadata,
            tracking_id: chunk_metadata.tracking_id,
            time_stamp: chunk_metadata.time_stamp.map(|x| x.timestamp()),
            num_value: chunk_metadata.num_value,
            dataset_id: dataset_id.unwrap_or(chunk_metadata.dataset_id),
            weight: chunk_metadata.weight,
            location: chunk_metadata.location,
            image_urls: chunk_metadata.image_urls,
            tag_set: chunk_metadata.tag_set,
            content: convert_html_to_text(&chunk_metadata.chunk_html.unwrap_or_default()),
            group_ids,
            group_tag_set,
        }
    }

    pub fn new_from_point(point: RetrievedPoint, group_ids: Option<Vec<uuid::Uuid>>) -> Self {
        QdrantPayload {
            link: point.payload.get("link").cloned().map(|x| x.to_string()),
            metadata: point
                .payload
                .get("metadata")
                .cloned()
                .map(|value| value.into()),
            tracking_id: point
                .payload
                .get("tracking_id")
                .cloned()
                .map(|x| x.to_string()),
            time_stamp: point
                .payload
                .get("time_stamp")
                .cloned()
                .and_then(|x| x.as_integer()),
            num_value: point
                .payload
                .get("num_value")
                .cloned()
                .and_then(|x| x.as_double()),
            dataset_id: point
                .payload
                .get("dataset_id")
                .cloned()
                .unwrap_or_default()
                .as_str()
                .map(|s| uuid::Uuid::parse_str(s).unwrap())
                .unwrap_or_default(),
            weight: point
                .payload
                .get("weight")
                .cloned()
                .and_then(|x| x.as_double())
                .unwrap_or_default(),
            location: point
                .payload
                .get("location")
                .cloned()
                .and_then(|value| serde_json::from_value(value.into()).ok()),
            image_urls: point.payload.get("image_urls").cloned().map(|x| {
                x.as_list()
                    .unwrap_or_default()
                    .iter()
                    .map(|value| Some(value.to_string()))
                    .collect()
            }),
            tag_set: point.payload.get("tag_set").cloned().map(|x| {
                x.as_list()
                    .unwrap_or_default()
                    .iter()
                    .map(|value| Some(value.to_string()))
                    .collect()
            }),
            content: point
                .payload
                .get("content")
                .cloned()
                .unwrap_or_default()
                .to_string(),
            group_ids,
            group_tag_set: point.payload.get("group_tag_set").cloned().map(|x| {
                x.as_list()
                    .unwrap_or_default()
                    .iter()
                    .map(|value| Some(value.to_string()))
                    .collect()
            }),
        }
    }
}

impl From<RetrievedPoint> for QdrantPayload {
    fn from(point: RetrievedPoint) -> Self {
        QdrantPayload {
            link: point
                .payload
                .get("link")
                .cloned()
                .map(|x| x.to_string().replace(['"', '\\'], "")),
            metadata: point
                .payload
                .get("metadata")
                .cloned()
                .map(|value| value.into()),
            tracking_id: point
                .payload
                .get("tracking_id")
                .cloned()
                .map(|x| x.to_string()),
            time_stamp: point
                .payload
                .get("time_stamp")
                .cloned()
                .and_then(|x| x.as_integer()),
            num_value: point
                .payload
                .get("num_value")
                .cloned()
                .and_then(|x| x.as_double()),
            dataset_id: point
                .payload
                .get("dataset_id")
                .cloned()
                .unwrap_or_default()
                .as_str()
                .and_then(|s| uuid::Uuid::parse_str(s).ok())
                .unwrap_or_default(),
            weight: point
                .payload
                .get("weight")
                .cloned()
                .and_then(|x| x.as_double())
                .unwrap_or_default(),
            location: point
                .payload
                .get("location")
                .cloned()
                .and_then(|value| serde_json::from_value(value.into()).ok())
                .unwrap_or_default(),
            image_urls: point.payload.get("image_urls").cloned().map(|x| {
                x.as_list()
                    .unwrap_or_default()
                    .iter()
                    .map(|value| Some(value.to_string()))
                    .collect()
            }),
            tag_set: point.payload.get("tag_set").cloned().map(|x| {
                x.as_list()
                    .unwrap_or_default()
                    .iter()
                    .map(|value| Some(value.to_string().replace(['"', '\\'], "")))
                    .collect()
            }),
            content: point
                .payload
                .get("content")
                .cloned()
                .unwrap_or_default()
                .to_string()
                .replace(['"', '\\'], ""),
            group_ids: point.payload.get("group_ids").cloned().map(|x| {
                x.as_list()
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|value| value.to_string().parse().ok())
                    .collect()
            }),
            group_tag_set: point.payload.get("group_tag_set").cloned().map(|x| {
                x.as_list()
                    .unwrap_or_default()
                    .iter()
                    .map(|value| Some(value.to_string().replace(['"', '\\'], "")))
                    .collect()
            }),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FileWorkerMessage {
    pub file_id: uuid::Uuid,
    pub dataset_id: uuid::Uuid,
    pub upload_file_data: UploadFileReqPayload,
    pub attempt_number: u8,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CsvJsonlWorkerMessage {
    pub file_id: uuid::Uuid,
    pub dataset_id: uuid::Uuid,
    pub create_presigned_put_url_data: CreatePresignedUrlForCsvJsonlReqPayload,
    pub created_at: chrono::NaiveDateTime,
    pub attempt_number: u8,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PagefindIndexWorkerMessage {
    pub dataset_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
    pub attempt_number: u8,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(untagged)]
pub enum RangeCondition {
    Float(f64),
    Int(i64),
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(example = json!({
    "gte": 0.0,
    "lte": 1.0,
    "gt": 0.0,
    "lt": 1.0
}))]
pub struct Range {
    // gte is the lower bound of the range. This is inclusive.
    pub gte: Option<RangeCondition>,
    // lte is the upper bound of the range. This is inclusive.
    pub lte: Option<RangeCondition>,
    // gt is the lower bound of the range. This is exclusive.
    pub gt: Option<RangeCondition>,
    // lt is the upper bound of the range. This is exclusive.
    pub lt: Option<RangeCondition>,
}

/// DateRange is a JSON object which can be used to filter chunks by a range of dates. This leverages the time_stamp field on chunks in your dataset. You can specify this if you want values in a certain range. You must provide ISO 8601 combined date and time without timezone.
#[derive(Serialize, Deserialize, Debug, Clone, ToSchema, Default)]
#[schema(example = json!({
    "gte": "2021-01-01 00:00:00.000",
    "lte": "2021-01-01 00:00:00.000",
    "gt": "2021-01-01 00:00:00.000",
    "lt": "2021-01-01 00:00:00.000"
}))]
pub struct DateRange {
    // gte is ISO8601 time for the lower bound of the range. This is inclusive.
    pub gte: Option<String>,
    // lte is ISO8601 time for the upper bound of the range. This is inclusive.
    pub lte: Option<String>,
    // gt is ISO8601 time for the lower bound of the range. This is exclusive.
    pub gt: Option<String>,
    // lt is ISO8601 time for the upper bound of the range. This is exclusive.
    pub lt: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(untagged)]
pub enum MatchCondition {
    Text(String),
    Integer(i64),
    Float(f64),
}

impl MatchCondition {
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        match self {
            MatchCondition::Text(text) => text.clone(),
            MatchCondition::Integer(int) => int.to_string(),
            MatchCondition::Float(float) => float.to_string(),
        }
    }

    pub fn to_i64(&self) -> i64 {
        match self {
            MatchCondition::Text(text) => text.parse().unwrap_or_default(),
            MatchCondition::Integer(int) => *int,
            MatchCondition::Float(float) => *float as i64,
        }
    }

    pub fn to_f64(&self) -> f64 {
        match self {
            MatchCondition::Text(text) => text.parse().unwrap_or_default(),
            MatchCondition::Integer(int) => *int as f64,
            MatchCondition::Float(float) => *float,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct LocationBoundingBox {
    pub top_left: GeoInfo,
    pub bottom_right: GeoInfo,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct LocationRadius {
    pub center: GeoInfo,
    pub radius: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct LocationPolygon {
    pub exterior: Vec<GeoInfo>,
    pub interior: Option<Vec<Vec<GeoInfo>>>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(untagged)]
/// Filters can be constructed using either fields on the chunk objects, ids or tracking ids of chunks, and finally ids or tracking ids of groups.
pub enum ConditionType {
    #[schema(title = "FieldCondition")]
    Field(FieldCondition),
    #[schema(title = "HasChunkIDCondition")]
    HasChunkId(HasChunkIDCondition),
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
/// HasChunkIDCondition is a JSON object which can be used to filter chunks by their ids or tracking ids. This is useful for when you want to filter chunks by their ids or tracking ids.
pub struct HasChunkIDCondition {
    /// Ids of the chunks to apply a match_any condition with. Only chunks with one of these ids will be returned.
    pub ids: Option<Vec<uuid::Uuid>>,
    /// Tracking ids of the chunks to apply a match_any condition with. Only chunks with one of these tracking ids will be returned.
    pub tracking_ids: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(example = json!({
    "field": "metadata.key1",
    "match": ["value1", "value2"],
    "range": {
        "gte": 0.0,
        "lte": 1.0,
        "gt": 0.0,
        "lt": 1.0
    }
}))]
/// FieldCondition is a JSON object which can be used to filter chunks by a field. This is useful for when you want to filter chunks by arbitrary metadata. To access fields inside of the metadata that you provide with the card, prefix the field name with `metadata.`.
pub struct FieldCondition {
    /// Field is the name of the field to filter on. Commonly used fields are `timestamp`, `link`, `tag_set`, `location`, `num_value`, `group_ids`, and `group_tracking_ids`. The field value will be used to check for an exact substring match on the metadata values for each existing chunk. This is useful for when you want to filter chunks by arbitrary metadata. To access fields inside of the metadata that you provide with the card, prefix the field name with `metadata.`.
    pub field: String,
    /// Match any lets you pass in an array of values that will return results if any of the items match. The match value will be used to check for an exact substring match on the metadata values for each existing chunk. If both match_all and match_any are provided, the match_any condition will be used.
    #[serde(alias = "match")]
    pub match_any: Option<Vec<MatchCondition>>,
    /// Match all lets you pass in an array of values that will return results if all of the items match. The match value will be used to check for an exact substring match on the metadata values for each existing chunk. If both match_all and match_any are provided, the match_any condition will be used.
    pub match_all: Option<Vec<MatchCondition>>,
    /// Range is a JSON object which can be used to filter chunks by a range of values. This only works for numerical fields. You can specify this if you want values in a certain range.
    pub range: Option<Range>,
    /// Boolean is a true false value for a field. This only works for boolean fields. You can specify this if you want values to be true or false.
    pub boolean: Option<bool>,
    /// Date range is a JSON object which can be used to filter chunks by a range of dates. This only works for date fields. You can specify this if you want values in a certain range. You must provide ISO 8601 combined date and time without timezone.
    pub date_range: Option<DateRange>,
    /// Geo Bounding Box search is useful for when you want to find points inside a rectangular area. This is useful for when you want to filter chunks by location. The bounding box is defined by two points: the top-left and bottom-right corners of the box.
    pub geo_bounding_box: Option<LocationBoundingBox>,
    /// Geo Radius search is useful for when you want to find points within a certain distance of a point. This is useful for when you want to filter chunks by location. The radius is in meters.
    pub geo_radius: Option<LocationRadius>,
    /// Geo Polygons search is useful for when you want to find points inside an irregularly shaped area, for example a country boundary or a forest boundary. A polygon always has an exterior ring and may optionally include interior rings. When defining a ring, you must pick either a clockwise or counterclockwise ordering for your points. The first and last point of the polygon must be the same.
    pub geo_polygon: Option<LocationPolygon>,
}

pub fn convert_to_date_time(time_stamp: Option<String>) -> Result<Option<f64>, ServiceError> {
    match time_stamp {
        Some(time_stamp) => Ok(Some(
            time_stamp
                .parse::<DateTimeUtc>()
                .map_err(|_| ServiceError::BadRequest("Invalid timestamp format".to_string()))?
                .0
                .with_timezone(&chrono::Local)
                .naive_local()
                .timestamp() as f64,
        )),
        None => Ok(None),
    }
}

fn get_date_range(date_range: DateRange) -> Result<qdrant::Range, ServiceError> {
    // Based on the determined type, process the values
    let gt = convert_to_date_time(date_range.gt)?;
    let gte = convert_to_date_time(date_range.gte)?;
    let lt = convert_to_date_time(date_range.lt)?;
    let lte = convert_to_date_time(date_range.lte)?;

    Ok(qdrant::Range { gt, gte, lt, lte })
}

pub fn get_range(range: Range) -> Result<qdrant::Range, ServiceError> {
    fn convert_range(range: Option<RangeCondition>) -> Result<Option<f64>, ServiceError> {
        match range {
            Some(RangeCondition::Float(val)) => Ok(Some(val)),
            Some(RangeCondition::Int(val)) => Ok(Some(val as f64)),
            None => Ok(None),
        }
    }

    // Based on the determined type, process the values

    let gt = convert_range(range.gt)?;
    let gte = convert_range(range.gte)?;
    let lt = convert_range(range.lt)?;
    let lte = convert_range(range.lte)?;

    Ok(qdrant::Range { gt, gte, lt, lte })
}

impl FieldCondition {
    pub async fn convert_to_qdrant_condition(
        &self,
        dataset_id: uuid::Uuid,
        pool: web::Data<Pool>,
    ) -> Result<Option<qdrant::Condition>, ServiceError> {
        if (self.match_all.is_some() || self.match_any.is_some())
            && (self.range.is_some() || self.boolean.is_some())
        {
            return Err(ServiceError::BadRequest(
                "Cannot have both a match and range or boolean conditions".to_string(),
            ));
        }

        if self.field.starts_with("group_metadata.") {
            return Ok(Some(
                get_group_metadata_filter_condition(self, dataset_id, pool)
                    .await?
                    .into(),
            ));
        }

        if self.field == "group_tag_set" {
            return Ok(Some(
                get_group_tag_set_filter_condition(self, dataset_id, pool)
                    .await?
                    .into(),
            ));
        }

        if let Some(date_range) = self.date_range.clone() {
            let time_range = get_date_range(date_range)?;
            return Ok(Some(qdrant::Condition::range(
                self.field.as_str(),
                time_range,
            )));
        }

        if let Some(range) = self.range.clone() {
            let range = get_range(range)?;
            return Ok(Some(qdrant::Condition::range(self.field.as_str(), range)));
        };

        if let Some(boolean) = self.boolean {
            return Ok(Some(qdrant::Condition::matches(
                self.field.as_str(),
                boolean,
            )));
        }

        if let Some(geo_bounding_box) = self.geo_bounding_box.clone() {
            let top_left = geo_bounding_box.top_left;
            let bottom_right = geo_bounding_box.bottom_right;

            return Ok(Some(qdrant::Condition::geo_bounding_box(
                self.field.as_str(),
                GeoBoundingBox {
                    top_left: Some(GeoPoint {
                        lat: top_left.lat.into(),
                        lon: top_left.lon.into(),
                    }),
                    bottom_right: Some(GeoPoint {
                        lat: bottom_right.lat.into(),
                        lon: bottom_right.lon.into(),
                    }),
                },
            )));
        }

        if let Some(geo_radius) = self.geo_radius.clone() {
            let center = geo_radius.center;
            let radius = geo_radius.radius;
            return Ok(Some(qdrant::Condition::geo_radius(
                self.field.as_str(),
                GeoRadius {
                    center: Some(GeoPoint {
                        lat: center.lat.into(),
                        lon: center.lon.into(),
                    }),
                    radius: radius as f32,
                },
            )));
        }

        if let Some(geo_polygon) = self.geo_polygon.clone() {
            let exterior = geo_polygon.exterior;
            let interior = geo_polygon.interior;
            let exterior = exterior
                .iter()
                .map(|point| GeoPoint {
                    lat: point.lat.into(),
                    lon: point.lon.into(),
                })
                .collect();

            let interior = interior
                .map(|interior| {
                    interior
                        .iter()
                        .map(|points| {
                            let points = points
                                .iter()
                                .map(|point| GeoPoint {
                                    lat: point.lat.into(),
                                    lon: point.lon.into(),
                                })
                                .collect();
                            GeoLineString { points }
                        })
                        .collect()
                })
                .unwrap_or_default();

            return Ok(Some(qdrant::Condition::geo_polygon(
                self.field.as_str(),
                GeoPolygon {
                    exterior: Some(GeoLineString { points: exterior }),
                    interiors: interior,
                },
            )));
        }

        if let Some(match_any) = &self.match_any {
            if let Some(first_match_any) = match_any.first() {
                match first_match_any {
                    MatchCondition::Text(_) => Ok(Some(qdrant::Condition::matches(
                        self.field.as_str(),
                        match_any.iter().map(|x| x.to_string()).collect_vec(),
                    ))),
                    MatchCondition::Integer(_) | MatchCondition::Float(_) => {
                        Ok(Some(qdrant::Condition::matches(
                            self.field.as_str(),
                            match_any
                                .iter()
                                .map(|x: &MatchCondition| x.to_i64())
                                .collect_vec(),
                        )))
                    }
                }
            } else {
                Ok(None)
            }
        } else if let Some(match_all) = &self.match_all {
            if let Some(first_match_all) = match_all.first() {
                match first_match_all {
                    MatchCondition::Text(_) => Ok(Some(
                        qdrant::Filter::must(
                            match_all
                                .iter()
                                .map(|cond| {
                                    qdrant::Condition::matches(
                                        self.field.as_str(),
                                        vec![cond.to_string()],
                                    )
                                })
                                .collect_vec(),
                        )
                        .into(),
                    )),
                    MatchCondition::Integer(_) | MatchCondition::Float(_) => Ok(Some(
                        qdrant::Filter::must(
                            match_all
                                .iter()
                                .map(|cond| {
                                    qdrant::Condition::matches(
                                        self.field.as_str(),
                                        vec![cond.to_i64()],
                                    )
                                })
                                .collect_vec(),
                        )
                        .into(),
                    )),
                }
            } else {
                Ok(None)
            }
        } else {
            Err(ServiceError::BadRequest(
                "No filter condition provided. Field must not be null and date_range, range, boolean, geo_bounding_box, geo_radius, geo_polygon, match_any, or match_all must be populated.".to_string(),
            ))
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, Default)]
pub struct SearchQueryRating {
    pub rating: i32,
    pub note: Option<String>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, Display)]
#[serde(rename_all = "snake_case")]
pub enum ClickhouseSearchTypes {
    #[display(fmt = "search")]
    Search,
    #[display(fmt = "search_over_groups")]
    SearchOverGroups,
    #[display(fmt = "autocomplete")]
    Autocomplete,
    #[display(fmt = "rag")]
    #[serde(rename = "rag")]
    RAG,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(title = "SearchQueryEvent")]
pub struct SearchQueryEvent {
    pub id: uuid::Uuid,
    pub search_type: ClickhouseSearchTypes,
    pub query: String,
    pub request_params: serde_json::Value,
    pub latency: f32,
    pub top_score: f32,
    pub results: Vec<serde_json::Value>,
    pub dataset_id: uuid::Uuid,
    pub created_at: String,
    pub query_rating: Option<SearchQueryRating>,
    pub user_id: String,
}

impl Default for SearchQueryEvent {
    fn default() -> Self {
        SearchQueryEvent {
            id: uuid::Uuid::new_v4(),
            search_type: ClickhouseSearchTypes::Search,
            query: "".to_string(),
            request_params: serde_json::Value::String("".to_string()),
            latency: 0.0,
            top_score: 0.0,
            results: vec![],
            dataset_id: uuid::Uuid::new_v4(),
            created_at: chrono::Utc::now().to_string(),
            query_rating: None,
            user_id: String::from(""),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Row, ToSchema)]
pub struct SearchQueriesWithClicksCTRResponseClickhouse {
    pub query: String,
    pub results: Vec<String>,
    #[serde(with = "clickhouse::serde::uuid")]
    pub dataset_id: uuid::Uuid,
    pub chunk_with_position: String,
    pub request_id: String,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Serialize, Deserialize, Row, ToSchema)]
pub struct SearchQueriesWithClicksCTRResponse {
    pub query: String,
    pub results: Vec<serde_json::Value>,
    pub clicked_chunk: ChunkMetadataWithPosition,
    pub request_id: String,
    pub created_at: String,
}

impl SearchQueriesWithClicksCTRResponseClickhouse {
    pub async fn from_clickhouse(
        self,
        pool: web::Data<Pool>,
    ) -> SearchQueriesWithClicksCTRResponse {
        let chunk_with_position: ChunkWithPosition =
            serde_json::from_str(&self.chunk_with_position).unwrap();

        let chunk =
            get_metadata_from_id_query(chunk_with_position.chunk_id, self.dataset_id, pool.clone())
                .await
                .unwrap_or_default();

        let clicked_chunk = ChunkMetadataWithPosition {
            chunk,
            position: chunk_with_position.position,
        };

        SearchQueriesWithClicksCTRResponse {
            query: self.query,
            results: self
                .results
                .iter()
                .map(|r| {
                    serde_json::from_str(&r.replace("|q", "?").replace('\n', ""))
                        .unwrap_or_default()
                })
                .collect::<Vec<serde_json::Value>>(),
            clicked_chunk,
            request_id: self.request_id,
            created_at: self.created_at.to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Row, ToSchema)]
pub struct SearchQueriesWithoutClicksCTRResponseClickhouse {
    pub query: String,
    pub request_id: String,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Serialize, Deserialize, Row, ToSchema)]
pub struct SearchQueriesWithoutClicksCTRResponse {
    pub query: String,
    pub request_id: String,
    pub created_at: String,
}

impl From<SearchQueriesWithoutClicksCTRResponseClickhouse>
    for SearchQueriesWithoutClicksCTRResponse
{
    fn from(clickhouse: SearchQueriesWithoutClicksCTRResponseClickhouse) -> Self {
        SearchQueriesWithoutClicksCTRResponse {
            query: clickhouse.query,
            request_id: clickhouse.request_id,
            created_at: clickhouse.created_at.to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Row, ToSchema)]
pub struct RecommendationsWithClicksCTRResponseClickhouse {
    pub positive_ids: Vec<String>,
    pub negative_ids: Vec<String>,
    pub positive_tracking_ids: Vec<String>,
    pub negative_tracking_ids: Vec<String>,
    pub results: Vec<String>,
    #[serde(with = "clickhouse::serde::uuid")]
    pub dataset_id: uuid::Uuid,
    pub request_id: String,
    pub chunk_with_position: String,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Serialize, Deserialize, Row, ToSchema)]
pub struct RecommendationsWithClicksCTRResponse {
    pub positive_ids: Option<Vec<String>>,
    pub negative_ids: Option<Vec<String>>,
    pub positive_tracking_ids: Option<Vec<String>>,
    pub negative_tracking_ids: Option<Vec<String>>,
    pub results: Vec<serde_json::Value>,
    pub request_id: String,
    pub clicked_chunk: ChunkMetadataWithPosition,
    pub created_at: String,
}

impl RecommendationsWithClicksCTRResponseClickhouse {
    pub async fn from_clickhouse(
        self,
        pool: web::Data<Pool>,
    ) -> RecommendationsWithClicksCTRResponse {
        let chunk_with_position: ChunkWithPosition =
            serde_json::from_str(&self.chunk_with_position).unwrap();

        let chunk =
            get_metadata_from_id_query(chunk_with_position.chunk_id, self.dataset_id, pool.clone())
                .await
                .unwrap_or_default();

        let clicked_chunk = ChunkMetadataWithPosition {
            chunk,
            position: chunk_with_position.position,
        };

        //only return the vecs that are not empty everything else should be None
        let positive_ids = if !self.positive_ids.is_empty() {
            Some(self.positive_ids)
        } else {
            None
        };

        let negative_ids = if !self.negative_ids.is_empty() {
            Some(self.negative_ids)
        } else {
            None
        };

        let positive_tracking_ids = if !self.positive_tracking_ids.is_empty() {
            Some(self.positive_tracking_ids)
        } else {
            None
        };

        let negative_tracking_ids = if !self.negative_tracking_ids.is_empty() {
            Some(self.negative_tracking_ids)
        } else {
            None
        };

        RecommendationsWithClicksCTRResponse {
            positive_ids,
            negative_ids,
            positive_tracking_ids,
            negative_tracking_ids,
            results: self
                .results
                .iter()
                .map(|r| {
                    serde_json::from_str(&r.replace("|q", "?").replace('\n', ""))
                        .unwrap_or_default()
                })
                .collect::<Vec<serde_json::Value>>(),
            clicked_chunk,
            request_id: self.request_id,
            created_at: self.created_at.to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Row, ToSchema)]
pub struct RecommendationsWithoutClicksCTRResponseClickhouse {
    pub positive_ids: Vec<String>,
    pub negative_ids: Vec<String>,
    pub positive_tracking_ids: Vec<String>,
    pub negative_tracking_ids: Vec<String>,
    pub request_id: String,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Serialize, Deserialize, Row, ToSchema)]
pub struct RecommendationsWithoutClicksCTRResponse {
    pub positive_ids: Option<Vec<String>>,
    pub negative_ids: Option<Vec<String>>,
    pub positive_tracking_ids: Option<Vec<String>>,
    pub negative_tracking_ids: Option<Vec<String>>,
    pub request_id: String,
    pub created_at: String,
}

impl From<RecommendationsWithoutClicksCTRResponseClickhouse>
    for RecommendationsWithoutClicksCTRResponse
{
    fn from(clickhouse: RecommendationsWithoutClicksCTRResponseClickhouse) -> Self {
        //only return the vecs that are not empty everything else should be None
        let positive_ids = if !clickhouse.positive_ids.is_empty() {
            Some(clickhouse.positive_ids)
        } else {
            None
        };

        let negative_ids = if !clickhouse.negative_ids.is_empty() {
            Some(clickhouse.negative_ids)
        } else {
            None
        };

        let positive_tracking_ids = if !clickhouse.positive_tracking_ids.is_empty() {
            Some(clickhouse.positive_tracking_ids)
        } else {
            None
        };

        let negative_tracking_ids = if !clickhouse.negative_tracking_ids.is_empty() {
            Some(clickhouse.negative_tracking_ids)
        } else {
            None
        };

        RecommendationsWithoutClicksCTRResponse {
            positive_ids,
            negative_ids,
            positive_tracking_ids,
            negative_tracking_ids,
            request_id: clickhouse.request_id,
            created_at: clickhouse.created_at.to_string(),
        }
    }
}

#[derive(Debug, Row, Serialize, Deserialize, ToSchema, Clone)]
pub struct SearchQueryEventClickhouse {
    #[serde(with = "clickhouse::serde::uuid")]
    pub id: uuid::Uuid,
    pub search_type: String,
    pub query: String,
    pub request_params: String,
    pub latency: f32,
    pub top_score: f32,
    pub results: Vec<String>,
    #[serde(with = "clickhouse::serde::uuid")]
    pub dataset_id: uuid::Uuid,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub created_at: OffsetDateTime,
    pub query_rating: String,
    pub user_id: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum SearchResultType {
    Search(ScoreChunkDTO),
    GroupSearch(GroupScoreChunk),
}

impl From<String> for ClickhouseSearchTypes {
    fn from(search_type: String) -> Self {
        match search_type.as_str() {
            "search" => ClickhouseSearchTypes::Search,
            "search_over_groups" => ClickhouseSearchTypes::SearchOverGroups,
            "autocomplete" => ClickhouseSearchTypes::Autocomplete,
            "rag" => ClickhouseSearchTypes::RAG,
            _ => ClickhouseSearchTypes::Search,
        }
    }
}

impl From<SearchQueryEventClickhouse> for SearchQueryEvent {
    fn from(clickhouse_response: SearchQueryEventClickhouse) -> SearchQueryEvent {
        let query_rating = if !clickhouse_response.query_rating.is_empty() {
            Some(serde_json::from_str(&clickhouse_response.query_rating).unwrap_or_default())
        } else {
            None
        };

        SearchQueryEvent {
            id: uuid::Uuid::from_bytes(*clickhouse_response.id.as_bytes()),
            search_type: clickhouse_response.search_type.into(),
            query: clickhouse_response
                .query
                .replace("|q", "?")
                .replace('\n', ""),
            request_params: serde_json::from_str(
                &clickhouse_response
                    .request_params
                    .replace("|q", "?")
                    .replace('\n', ""),
            )
            .unwrap_or_default(),
            latency: clickhouse_response.latency,
            top_score: clickhouse_response.top_score,
            results: clickhouse_response
                .results
                .iter()
                .map(|r| {
                    serde_json::from_str(&r.replace("|q", "?").replace('\n', ""))
                        .unwrap_or_default()
                })
                .collect::<Vec<serde_json::Value>>(),
            dataset_id: uuid::Uuid::from_bytes(*clickhouse_response.dataset_id.as_bytes()),
            created_at: clickhouse_response.created_at.to_string(),
            query_rating,
            user_id: clickhouse_response.user_id,
        }
    }
}

pub fn escape_quotes(value: &mut Value) {
    match value {
        Value::String(s) => {
            *s = s.replace('"', "\\\"");
        }
        Value::Array(arr) => {
            for item in arr {
                escape_quotes(item);
            }
        }
        Value::Object(obj) => {
            for (_, v) in obj {
                escape_quotes(v);
            }
        }
        _ => {}
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Display)]
#[serde(rename_all = "snake_case")]
pub enum ClickhouseRagTypes {
    #[display(fmt = "chosen_chunks")]
    ChosenChunks,
    #[display(fmt = "all_chunks")]
    AllChunks,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(title = "RagQueryEvent")]
pub struct RagQueryEvent {
    pub id: uuid::Uuid,
    pub rag_type: ClickhouseRagTypes,
    pub user_message: String,
    pub search_id: uuid::Uuid,
    pub results: Vec<serde_json::Value>,
    pub dataset_id: uuid::Uuid,
    pub llm_response: String,
    pub top_score: f32,
    pub query_rating: Option<SearchQueryRating>,
    pub hallucination_score: f64,
    pub detected_hallucinations: Vec<String>,
    pub created_at: String,
    pub user_id: String,
}

impl From<String> for ClickhouseRagTypes {
    fn from(rag_type: String) -> Self {
        match rag_type.as_str() {
            "chosen_chunks" => ClickhouseRagTypes::ChosenChunks,
            "all_chunks" => ClickhouseRagTypes::AllChunks,
            _ => ClickhouseRagTypes::ChosenChunks,
        }
    }
}

impl RagQueryEventClickhouse {
    pub async fn from_clickhouse(self, pool: web::Data<Pool>) -> RagQueryEvent {
        let chunk_ids = self
            .results
            .iter()
            .filter_map(|x| x.parse::<uuid::Uuid>().ok())
            .collect_vec();

        let results = if !chunk_ids.is_empty() {
            let chunks = get_metadata_from_ids_query(chunk_ids, self.dataset_id, pool)
                .await
                .unwrap_or(vec![]);

            chunks
                .into_iter()
                .map(|chunk| serde_json::to_value(chunk).unwrap_or_default())
                .collect::<Vec<serde_json::Value>>()
        } else {
            self.json_results
                .iter()
                .map(|r| {
                    serde_json::from_str(&r.replace("|q", "?").replace('\n', ""))
                        .unwrap_or_default()
                })
                .collect::<Vec<serde_json::Value>>()
        };

        let query_rating = if !self.query_rating.is_empty() {
            Some(serde_json::from_str(&self.query_rating).unwrap_or_default())
        } else {
            None
        };

        RagQueryEvent {
            id: uuid::Uuid::from_bytes(*self.id.as_bytes()),
            rag_type: self.rag_type.into(),
            user_message: self.user_message,
            search_id: uuid::Uuid::from_bytes(*self.search_id.as_bytes()),
            results,
            top_score: self.top_score,
            query_rating,
            dataset_id: uuid::Uuid::from_bytes(*self.dataset_id.as_bytes()),
            llm_response: self.llm_response,
            hallucination_score: self.hallucination_score,
            detected_hallucinations: self.detected_hallucinations,
            created_at: self.created_at.to_string(),
            user_id: self.user_id,
        }
    }
}

#[derive(Debug, Row, Serialize, Deserialize, ToSchema, Clone)]
pub struct RagQueryEventClickhouse {
    #[serde(with = "clickhouse::serde::uuid")]
    pub id: uuid::Uuid,
    pub rag_type: String,
    pub user_message: String,
    #[serde(with = "clickhouse::serde::uuid")]
    pub search_id: uuid::Uuid,
    pub results: Vec<String>,
    pub json_results: Vec<String>,
    pub top_score: f32,
    pub query_rating: String,
    pub llm_response: String,
    #[serde(with = "clickhouse::serde::uuid")]
    pub dataset_id: uuid::Uuid,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub created_at: OffsetDateTime,
    pub user_id: String,
    pub hallucination_score: f64,
    pub detected_hallucinations: Vec<String>,
}

#[derive(Debug, Row, Serialize, Deserialize, ToSchema)]
pub struct ClusterTopicsClickhouse {
    #[serde(with = "clickhouse::serde::uuid")]
    pub id: uuid::Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    pub dataset_id: uuid::Uuid,
    pub topic: String,
    pub density: i32,
    pub avg_score: f32,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub created_at: OffsetDateTime,
}

impl From<ClusterTopicsClickhouse> for SearchClusterTopics {
    fn from(cluster_topic: ClusterTopicsClickhouse) -> Self {
        SearchClusterTopics {
            id: uuid::Uuid::from_bytes(*cluster_topic.id.as_bytes()),
            dataset_id: uuid::Uuid::from_bytes(*cluster_topic.dataset_id.as_bytes()),
            topic: cluster_topic.topic,
            density: cluster_topic.density,
            avg_score: cluster_topic.avg_score,
            created_at: cluster_topic.created_at.to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Row, Clone)]
pub struct RecommendationEventClickhouse {
    #[serde(with = "clickhouse::serde::uuid")]
    pub id: uuid::Uuid,
    pub recommendation_type: String,
    pub positive_ids: Vec<String>,
    pub negative_ids: Vec<String>,
    pub positive_tracking_ids: Vec<String>,
    pub negative_tracking_ids: Vec<String>,
    pub request_params: String,
    pub results: Vec<String>,
    pub top_score: f32,
    #[serde(with = "clickhouse::serde::uuid")]
    pub dataset_id: uuid::Uuid,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub created_at: OffsetDateTime,
    pub user_id: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Display, Clone, Default)]
#[serde(rename = "snake_case")]
pub enum ClickhouseRecommendationTypes {
    #[display(fmt = "chunk")]
    #[default]
    Chunk,
    #[display(fmt = "group")]
    Group,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Default)]
pub struct RecommendationEvent {
    pub id: uuid::Uuid,
    pub recommendation_type: ClickhouseRecommendationTypes,
    pub positive_ids: Vec<uuid::Uuid>,
    pub negative_ids: Vec<uuid::Uuid>,
    pub positive_tracking_ids: Vec<String>,
    pub negative_tracking_ids: Vec<String>,
    pub request_params: serde_json::Value,
    pub results: Vec<serde_json::Value>,
    pub top_score: f32,
    pub dataset_id: uuid::Uuid,
    pub created_at: String,
    pub user_id: String,
}

impl From<String> for ClickhouseRecommendationTypes {
    fn from(recommendation_type: String) -> Self {
        match recommendation_type.as_str() {
            "chunk" => ClickhouseRecommendationTypes::Chunk,
            "group" => ClickhouseRecommendationTypes::Group,
            _ => ClickhouseRecommendationTypes::Chunk,
        }
    }
}

impl From<RecommendationEventClickhouse> for RecommendationEvent {
    fn from(clickhouse_response: RecommendationEventClickhouse) -> RecommendationEvent {
        RecommendationEvent {
            id: uuid::Uuid::from_bytes(*clickhouse_response.id.as_bytes()),
            recommendation_type: clickhouse_response.recommendation_type.into(),
            positive_ids: clickhouse_response
                .positive_ids
                .iter()
                .map(|id| uuid::Uuid::parse_str(id).unwrap())
                .collect(),
            negative_ids: clickhouse_response
                .negative_ids
                .iter()
                .map(|id| uuid::Uuid::parse_str(id).unwrap())
                .collect(),

            positive_tracking_ids: clickhouse_response.positive_tracking_ids.clone(),
            negative_tracking_ids: clickhouse_response.negative_tracking_ids.clone(),
            request_params: serde_json::from_str(&clickhouse_response.request_params).unwrap(),
            results: clickhouse_response
                .results
                .iter()
                .map(|r| serde_json::from_str(r).unwrap())
                .collect::<Vec<serde_json::Value>>(),
            top_score: clickhouse_response.top_score,
            dataset_id: uuid::Uuid::from_bytes(*clickhouse_response.dataset_id.as_bytes()),
            created_at: clickhouse_response.created_at.to_string(),
            user_id: clickhouse_response.user_id.clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct SearchClusterTopics {
    pub id: uuid::Uuid,
    pub dataset_id: uuid::Uuid,
    pub topic: String,
    pub density: i32,
    pub avg_score: f32,
    pub created_at: String,
}

#[derive(Debug, Row, Serialize, Deserialize, ToSchema)]
pub struct SearchClusterMembership {
    #[serde(with = "clickhouse::serde::uuid")]
    pub id: uuid::Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    pub search_query: uuid::Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    pub cluster_topic: uuid::Uuid,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Display, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SearchType {
    #[display(fmt = "search")]
    Search,
    #[display(fmt = "autocomplete")]
    Autocomplete,
    #[display(fmt = "search_over_groups")]
    SearchOverGroups,
    #[display(fmt = "search_within_groups")]
    SearchWithinGroups,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Display, Clone, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum SearchMethod {
    #[serde(rename = "fulltext", alias = "full_text")]
    #[display(fmt = "fulltext")]
    FullText,
    #[display(fmt = "semantic")]
    Semantic,
    #[display(fmt = "hybrid")]
    #[default]
    Hybrid,
    #[serde(rename = "bm25", alias = "BM25")]
    #[display(fmt = "BM25")]
    BM25,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Display, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SuggestType {
    #[display(fmt = "question")]
    Question,
    #[display(fmt = "keyword")]
    Keyword,
    #[display(fmt = "semantic")]
    Semantic,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Display, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CountSearchMethod {
    #[serde(rename = "fulltext", alias = "full_text")]
    #[display(fmt = "fulltext")]
    FullText,
    #[display(fmt = "semantic")]
    Semantic,
    #[serde(rename = "bm25", alias = "BM25")]
    #[display(fmt = "BM25")]
    BM25,
}

impl From<CountSearchMethod> for SearchMethod {
    fn from(count_search_method: CountSearchMethod) -> Self {
        match count_search_method {
            CountSearchMethod::FullText => SearchMethod::FullText,
            CountSearchMethod::Semantic => SearchMethod::Semantic,
            CountSearchMethod::BM25 => SearchMethod::BM25,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema, Default)]
#[schema(example = json!({
    "gte": 1,
    "lte": 1,
    "gt": 1,
    "lt": 1
}))]
pub struct QueryRatingRange {
    // gte is the lower bound of the range. This is inclusive.
    pub gte: Option<u32>,
    // lte is the upper bound of the range. This is inclusive.
    pub lte: Option<u32>,
    // gt is the lower bound of the range. This is exclusive.
    pub gt: Option<u32>,
    // lt is the upper bound of the range. This is exclusive.
    pub lt: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct SearchAnalyticsFilter {
    pub date_range: Option<DateRange>,
    pub search_method: Option<SearchMethod>,
    pub search_type: Option<SearchType>,
    pub query_rating: Option<QueryRatingRange>,
}

impl SearchAnalyticsFilter {
    pub fn add_to_query(&self, mut query_string: String) -> String {
        if let Some(date_range) = &self.date_range {
            if let Some(gt) = &date_range.gt {
                query_string.push_str(&format!(" AND created_at > '{}'", gt));
            }
            if let Some(lt) = &date_range.lt {
                query_string.push_str(&format!(" AND created_at < '{}'", lt));
            }
            if let Some(gte) = &date_range.gte {
                query_string.push_str(&format!(" AND created_at >= '{}'", gte));
            }
            if let Some(lte) = &date_range.lte {
                query_string.push_str(&format!(" AND created_at <= '{}'", lte));
            }
        }

        if let Some(search_type) = &self.search_type {
            query_string.push_str(&format!(" AND search_type = '{}'", search_type));
        }
        if let Some(search_method) = &self.search_method {
            query_string.push_str(&format!(
                " AND JSONExtractString(request_params, 'search_type') = '{}'",
                search_method
            ));
        }

        if let Some(query_rating) = &self.query_rating {
            if let Some(gt) = &query_rating.gt {
                query_string.push_str(&format!(
                    " AND JSONExtract(query_rating, 'rating', 'Nullable(Float64)') > {}",
                    gt
                ));
            }
            if let Some(lt) = &query_rating.lt {
                query_string.push_str(&format!(
                    " AND JSONExtract(query_rating, 'rating', 'Nullable(Float64)') < {}",
                    lt
                ));
            }
            if let Some(gte) = &query_rating.gte {
                query_string.push_str(&format!(
                    " AND JSONExtract(query_rating, 'rating', 'Nullable(Float64)') >= {}",
                    gte
                ));
            }
            if let Some(lte) = &query_rating.lte {
                query_string.push_str(&format!(
                    " AND JSONExtract(query_rating, 'rating', 'Nullable(Float64)') <= {}",
                    lte
                ));
            }
        }

        query_string
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TopDatasetsResponse {
    pub dataset_id: uuid::Uuid,
    pub dataset_tracking_id: Option<String>,
    pub total_queries: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Row, Clone)]
pub struct TopDatasetsResponseClickhouse {
    #[serde(with = "clickhouse::serde::uuid")]
    pub dataset_id: uuid::Uuid,
    pub total_queries: i64,
}

impl From<TopDatasetsResponseClickhouse> for TopDatasetsResponse {
    fn from(clickhouse_response: TopDatasetsResponseClickhouse) -> TopDatasetsResponse {
        TopDatasetsResponse {
            dataset_id: uuid::Uuid::from_bytes(*clickhouse_response.dataset_id.as_bytes()),
            dataset_tracking_id: None,
            total_queries: clickhouse_response.total_queries,
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, ToSchema, Display)]
#[serde(rename_all = "snake_case")]
pub enum TopDatasetsRequestTypes {
    #[display(fmt = "search_queries")]
    Search,
    #[serde(rename = "rag")]
    #[display(fmt = "rag_queries")]
    RAG,
    #[display(fmt = "recommendations")]
    Recommendation,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Display, Clone)]
#[serde(rename_all = "snake_case")]
pub enum RagTypes {
    #[display(fmt = "chosen_chunks")]
    ChosenChunks,
    #[display(fmt = "all_chunks")]
    AllChunks,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct RAGAnalyticsFilter {
    pub date_range: Option<DateRange>,
    pub rag_type: Option<RagTypes>,
    pub query_rating: Option<QueryRatingRange>,
}

impl RAGAnalyticsFilter {
    pub fn add_to_query(&self, mut query_string: String) -> String {
        if let Some(date_range) = &self.date_range {
            if let Some(gt) = &date_range.gt {
                query_string.push_str(&format!(" AND created_at > '{}'", gt));
            }
            if let Some(lt) = &date_range.lt {
                query_string.push_str(&format!(" AND created_at < '{}'", lt));
            }
            if let Some(gte) = &date_range.gte {
                query_string.push_str(&format!(" AND created_at >= '{}'", gte));
            }
            if let Some(lte) = &date_range.lte {
                query_string.push_str(&format!(" AND created_at <= '{}'", lte));
            }
        }

        if let Some(rag_type) = &self.rag_type {
            query_string.push_str(&format!(" AND rag_type = '{}'", rag_type));
        }

        if let Some(query_rating) = &self.query_rating {
            if let Some(gt) = &query_rating.gt {
                query_string.push_str(&format!(
                    " AND JSONExtract(query_rating, 'rating', 'Nullable(Float64)') > {}",
                    gt
                ));
            }
            if let Some(lt) = &query_rating.lt {
                query_string.push_str(&format!(
                    " AND JSONExtract(query_rating, 'rating', 'Nullable(Float64)') < {}",
                    lt
                ));
            }
            if let Some(gte) = &query_rating.gte {
                query_string.push_str(&format!(
                    " AND JSONExtract(query_rating, 'rating', 'Nullable(Float64)') >= {}",
                    gte
                ));
            }
            if let Some(lte) = &query_rating.lte {
                query_string.push_str(&format!(
                    " AND JSONExtract(query_rating, 'rating', 'Nullable(Float64)') <= {}",
                    lte
                ));
            }
        }

        query_string
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct ClusterAnalyticsFilter {
    pub date_range: Option<DateRange>,
}

impl ClusterAnalyticsFilter {
    pub fn add_to_query(&self, mut query_string: String) -> String {
        if let Some(date_range) = &self.date_range {
            if let Some(gt) = &date_range.gt {
                query_string.push_str(&format!(" AND created_at > '{}'", gt));
            }
            if let Some(lt) = &date_range.lt {
                query_string.push_str(&format!(" AND created_at < '{}'", lt));
            }
            if let Some(gte) = &date_range.gte {
                query_string.push_str(&format!(" AND created_at >= '{}'", gte));
            }
            if let Some(lte) = &date_range.lte {
                query_string.push_str(&format!(" AND created_at <= '{}'", lte));
            }
        }

        query_string
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Display)]
pub enum RecommendationType {
    #[display(fmt = "chunk")]
    Chunk,
    #[display(fmt = "group")]
    Group,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct RecommendationAnalyticsFilter {
    pub date_range: Option<DateRange>,
    pub recommendation_type: Option<RecommendationType>,
}

impl RecommendationAnalyticsFilter {
    pub fn add_to_query(&self, mut query_string: String) -> String {
        if let Some(date_range) = &self.date_range {
            if let Some(gt) = &date_range.gt {
                query_string.push_str(&format!(" AND created_at > '{}'", gt));
            }
            if let Some(lt) = &date_range.lt {
                query_string.push_str(&format!(" AND created_at < '{}'", lt));
            }
            if let Some(gte) = &date_range.gte {
                query_string.push_str(&format!(" AND created_at >= '{}'", gte));
            }
            if let Some(lte) = &date_range.lte {
                query_string.push_str(&format!(" AND created_at <= '{}'", lte));
            }
        }

        if let Some(recommendation_type) = &self.recommendation_type {
            query_string.push_str(&format!(
                " AND recommendation_type = '{}'",
                recommendation_type
            ));
        }

        query_string
    }
}

#[derive(Debug, Row, ToSchema, Serialize, Deserialize)]
#[schema(title = "SearchMetricsResponse")]
pub struct DatasetAnalytics {
    /// Total number of search queries
    pub total_queries: i64,
    /// Average latency of search queries
    pub avg_latency: f64,
    /// 99th percentile latency of search queries
    pub p99: f64,
    /// 95th percentile latency of search queries
    pub p95: f64,
    /// 50th percentile latency of search queries
    pub p50: f64,
    /// Total number of searches with a positive rating
    pub total_positive_ratings: f64,
    /// Total number of searches with a negative rating
    pub total_negative_ratings: f64,
}

#[derive(Debug, ToSchema, Row, Serialize, Deserialize)]
pub struct HeadQueries {
    pub query: String,
    pub count: i64,
}

#[derive(Debug, Row, Serialize, Deserialize, ToSchema)]
pub struct UsageGraphPointClickhouse {
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub time_stamp: OffsetDateTime,
    pub requests: i64,
}

#[derive(Debug, Row, Serialize, Deserialize, ToSchema)]
pub struct UsageGraphPoint {
    pub time_stamp: String,
    pub requests: i64,
}

impl From<UsageGraphPointClickhouse> for UsageGraphPoint {
    fn from(graph: UsageGraphPointClickhouse) -> Self {
        UsageGraphPoint {
            time_stamp: graph.time_stamp.to_string(),
            requests: graph.requests,
        }
    }
}

#[derive(Debug, Row, Serialize, Deserialize, ToSchema)]
pub struct SearchLatencyGraphClickhouse {
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub time_stamp: OffsetDateTime,
    pub average_latency: f64,
}

#[derive(Debug, Row, Serialize, Deserialize, ToSchema)]
pub struct SearchLatencyGraph {
    pub time_stamp: String,
    pub average_latency: f64,
}

impl From<SearchLatencyGraphClickhouse> for SearchLatencyGraph {
    fn from(graph: SearchLatencyGraphClickhouse) -> Self {
        SearchLatencyGraph {
            time_stamp: graph.time_stamp.to_string(),
            average_latency: graph.average_latency,
        }
    }
}

#[derive(Debug, Row, Serialize, Deserialize, ToSchema)]
pub struct SearchCTRMetricsClickhouse {
    pub searches_with_clicks: i64,
    pub percent_searches_with_clicks: f64,
    pub percent_searches_without_clicks: f64,
    pub avg_position_of_click: f64,
}

#[derive(Debug, Row, Serialize, Deserialize, ToSchema)]
#[schema(title = "Search CTR Metrics")]
pub struct SearchCTRMetrics {
    pub searches_with_clicks: i64,
    pub percent_searches_with_clicks: f64,
    pub percent_searches_without_clicks: f64,
    pub avg_position_of_click: f64,
}

impl From<SearchCTRMetricsClickhouse> for SearchCTRMetrics {
    fn from(metrics: SearchCTRMetricsClickhouse) -> Self {
        SearchCTRMetrics {
            searches_with_clicks: metrics.searches_with_clicks,
            percent_searches_with_clicks: f64::from_be_bytes(
                metrics.percent_searches_with_clicks.to_be_bytes(),
            ),
            percent_searches_without_clicks: f64::from_be_bytes(
                metrics.percent_searches_without_clicks.to_be_bytes(),
            ),
            avg_position_of_click: f64::from_be_bytes(metrics.avg_position_of_click.to_be_bytes()),
        }
    }
}

#[derive(Debug, Row, Serialize, Deserialize, ToSchema)]
#[schema(title = "Recommendation CTR Metrics")]
pub struct RecommendationCTRMetrics {
    pub recommendations_with_clicks: i64,
    pub percent_recommendations_with_clicks: f64,
    pub percent_recommendations_without_clicks: f64,
    pub avg_position_of_click: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ChunkData {
    pub chunk_metadata: ChunkMetadata,
    pub content: String,
    pub embedding_content: String,
    pub group_ids: Option<Vec<uuid::Uuid>>,
    pub upsert_by_tracking_id: bool,
    pub fulltext_boost: Option<FullTextBoost>,
    pub semantic_boost: Option<SemanticBoost>,
}

#[derive(Debug, Serialize, Deserialize, Selectable, Queryable, Insertable, Clone)]
#[diesel(table_name = chunk_boosts)]
pub struct ChunkBoost {
    pub chunk_id: uuid::Uuid,
    pub fulltext_boost_phrase: Option<String>,
    pub fulltext_boost_factor: Option<f64>,
    pub semantic_boost_phrase: Option<String>,
    pub semantic_boost_factor: Option<f64>,
}

#[derive(AsChangeset)]
#[diesel(table_name = chunk_boosts)]
pub struct ChunkBoostChangeset {
    fulltext_boost_phrase: Option<String>,
    fulltext_boost_factor: Option<f64>,
    semantic_boost_phrase: Option<String>,
    semantic_boost_factor: Option<f64>,
}

impl From<ChunkBoost> for ChunkBoostChangeset {
    fn from(chunk_boost: ChunkBoost) -> Self {
        ChunkBoostChangeset {
            fulltext_boost_phrase: chunk_boost.fulltext_boost_phrase,
            fulltext_boost_factor: chunk_boost.fulltext_boost_factor,
            semantic_boost_phrase: chunk_boost.semantic_boost_phrase,
            semantic_boost_factor: chunk_boost.semantic_boost_factor,
        }
    }
}

#[derive(Debug, ToSchema, Serialize, Deserialize, Row)]
#[schema(example = json!({
    "search_type": "search",
    "search_count": 8,
}))]
pub struct SearchTypeCount {
    pub search_type: String,
    pub search_method: String,
    pub search_count: i64,
}

#[derive(Debug, ToSchema, Serialize, Deserialize, Row)]
#[schema(example = json!({
    "clause": "must",
    "field": "metadata.ep_num",
    "filter_type": "match_any",
    "count": 8,
    "common_values": "['130']: 2, ['198']: 11"
}))]
pub struct PopularFiltersClickhouse {
    pub clause: String,
    pub field: String,
    pub filter_type: String,
    pub count: i64,
    pub common_values: String,
}

/// EventData represents a single analytics event
#[derive(Debug, ToSchema, Serialize, Deserialize, Clone)]
#[schema(title = "EventData", example = json!({
    "event_type": "view",
    "event_name": "Viewed Home Page",
    "request_id": "00000000-0000-0000-0000-000000000000",
    "items": ["item1", "item2"],
    "user_id": "user1",
    "metadata": "metadata",
    "is_conversion": true,
    "user_id": "user1",
    "dataset_id": "00000000-0000-0000-0000-000000000000",
    "created_at": "2021-08-10T00:00:00Z",
    "updated_at": "2021-08-10T00:00:00Z"
}))]
pub struct EventData {
    /// The unique identifier for the event
    pub id: uuid::Uuid,
    /// The type of event, "add_to_cart", "purchase", "view", "click", "filter_clicked".
    pub event_type: String,
    /// The name of the event, e.g. "Added to Cart", "Purchased", "Viewed Home Page", "Clicked", "Filter Clicked".
    pub event_name: String,
    /// The unique identifier for the request the event is associated with.
    pub request_id: Option<String>,
    /// The type of request the event is associated with.
    pub request_type: Option<String>,
    /// The items associated with the event. This could be a list of stringified json chunks for search events, or a list of items for add_to_cart, purchase, view, and click events.
    pub items: Vec<String>,
    /// Additional metadata associated with the event. This can be custom data that is specific to the event.
    pub metadata: Option<serde_json::Value>,
    /// The user identifier associated with the event.
    pub user_id: Option<String>,
    /// Whether the event is a conversion event.
    pub is_conversion: Option<bool>,
    /// The unique identifier for the dataset the event is associated with.
    pub dataset_id: uuid::Uuid,
    /// The time the event was created.
    pub created_at: String,
    /// The time the event was last updated.
    pub updated_at: String,
}

#[derive(Debug, ToSchema, Serialize, Deserialize, Row, Clone)]
#[schema(example = json!({
    "event_type": "view",
    "event_name": "Viewed Home Page",
    "request_id": "00000000-0000-0000-0000-000000000000",
    "items": ["item1", "item2"],
    "user_id": "user1",
    "metadata": "metadata",
    "is_conversion": true,
    "user_id": "user1",
    "dataset_id": "00000000-0000-0000-0000-000000000000",
    "created_at": "2021-08-10T00:00:00Z",
    "updated_at": "2021-08-10T00:00:00Z"
}))]
pub struct EventDataClickhouse {
    #[serde(with = "clickhouse::serde::uuid")]
    pub id: uuid::Uuid,
    pub event_type: String,
    pub event_name: String,
    pub request_id: String,
    pub request_type: String,
    pub items: Vec<String>,
    pub metadata: String,
    pub user_id: String,
    pub is_conversion: bool,
    #[serde(with = "clickhouse::serde::uuid")]
    pub dataset_id: uuid::Uuid,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub created_at: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub updated_at: OffsetDateTime,
}

pub enum EventDataTypes {
    EventDataClickhouse(EventDataClickhouse),
    SearchQueryEventClickhouse(SearchQueryEventClickhouse),
    RagQueryEventClickhouse(RagQueryEventClickhouse),
    RecommendationEventClickhouse(RecommendationEventClickhouse),
}

impl EventTypes {
    pub fn to_event_data(self, dataset_id: uuid::Uuid) -> EventDataTypes {
        match self {
            EventTypes::AddToCart {
                event_name,
                request,
                items,
                user_id,
                metadata,
                is_conversion,
            } => EventDataTypes::EventDataClickhouse(EventDataClickhouse {
                id: uuid::Uuid::new_v4(),
                event_type: "add_to_cart".to_string(),
                event_name,
                request_id: request.clone().unwrap_or_default().request_id.to_string(),
                request_type: request.unwrap_or_default().request_type.to_string(),
                items,
                metadata: serde_json::to_string(&metadata.unwrap_or_default()).unwrap_or_default(),
                user_id: user_id.unwrap_or_default(),
                is_conversion: is_conversion.unwrap_or(true),
                dataset_id,
                created_at: OffsetDateTime::now_utc(),
                updated_at: OffsetDateTime::now_utc(),
            }),
            EventTypes::Purchase {
                event_name,
                request,
                items,
                user_id,
                is_conversion,
                value,
                currency,
            } => EventDataTypes::EventDataClickhouse(EventDataClickhouse {
                id: uuid::Uuid::new_v4(),
                event_type: "purchase".to_string(),
                event_name,
                request_id: request.clone().unwrap_or_default().request_id.to_string(),
                request_type: request.unwrap_or_default().request_type.to_string(),
                items,
                metadata: json!({
                    "value": value.unwrap_or(0.0f64),
                    "currency": currency.unwrap_or("USD".to_string())
                })
                .to_string(),
                user_id: user_id.unwrap_or_default(),
                is_conversion: is_conversion.unwrap_or(true),
                dataset_id,
                created_at: OffsetDateTime::now_utc(),
                updated_at: OffsetDateTime::now_utc(),
            }),
            EventTypes::View {
                event_name,
                request,
                items,
                user_id,
                metadata,
            } => EventDataTypes::EventDataClickhouse(EventDataClickhouse {
                id: uuid::Uuid::new_v4(),
                event_type: "view".to_string(),
                event_name,
                request_id: request.clone().unwrap_or_default().request_id.to_string(),
                request_type: request.unwrap_or_default().request_type.to_string(),
                items,
                metadata: serde_json::to_string(&metadata.unwrap_or_default()).unwrap_or_default(),
                user_id: user_id.unwrap_or_default(),
                is_conversion: false,
                dataset_id,
                created_at: OffsetDateTime::now_utc(),
                updated_at: OffsetDateTime::now_utc(),
            }),
            EventTypes::Click {
                event_name,
                request,
                clicked_items: clicked_item,
                user_id,
                is_conversion,
            } => EventDataTypes::EventDataClickhouse(EventDataClickhouse {
                id: uuid::Uuid::new_v4(),
                event_type: "click".to_string(),
                event_name,
                request_id: request.clone().unwrap_or_default().request_id.to_string(),
                request_type: request.unwrap_or_default().request_type.to_string(),
                items: vec![],
                metadata: serde_json::to_string(&clicked_item).unwrap_or_default(),
                user_id: user_id.unwrap_or_default(),
                is_conversion: is_conversion.unwrap_or(true),
                dataset_id,
                created_at: OffsetDateTime::now_utc(),
                updated_at: OffsetDateTime::now_utc(),
            }),
            EventTypes::FilterClicked {
                event_name,
                request,
                items,
                user_id,
                is_conversion,
            } => EventDataTypes::EventDataClickhouse(EventDataClickhouse {
                id: uuid::Uuid::new_v4(),
                event_type: "filter_clicked".to_string(),
                event_name,
                request_id: request.clone().unwrap_or_default().request_id.to_string(),
                request_type: request.unwrap_or_default().request_type.to_string(),
                items: vec![],
                metadata: serde_json::to_string(&items).unwrap_or_default(),
                user_id: user_id.unwrap_or_default(),
                is_conversion: is_conversion.unwrap_or(true),
                dataset_id,
                created_at: OffsetDateTime::now_utc(),
                updated_at: OffsetDateTime::now_utc(),
            }),
            EventTypes::Search {
                search_type,
                query,
                request_params,
                latency,
                top_score,
                results,
                query_rating,
                user_id,
            } => EventDataTypes::SearchQueryEventClickhouse(SearchQueryEventClickhouse {
                id: uuid::Uuid::new_v4(),
                search_type: search_type
                    .unwrap_or(ClickhouseSearchTypes::Search)
                    .to_string(),
                query,
                request_params: serde_json::to_string(&request_params).unwrap_or_default(),
                latency: latency.unwrap_or(0.0),
                top_score: top_score.unwrap_or(0.0),
                results: results
                    .unwrap_or_default()
                    .iter()
                    .map(|r| r.to_string())
                    .collect(),
                dataset_id,
                created_at: OffsetDateTime::now_utc(),
                query_rating: serde_json::to_string(&query_rating).unwrap_or("".to_string()),
                user_id: user_id.unwrap_or_default(),
            }),
            EventTypes::RAG {
                rag_type,
                user_message,
                search_id,
                results,
                query_rating,
                llm_response,
                user_id,
                hallucination_score,
                detected_hallucinations,
            } => EventDataTypes::RagQueryEventClickhouse(RagQueryEventClickhouse {
                id: uuid::Uuid::new_v4(),
                rag_type: rag_type
                    .unwrap_or(ClickhouseRagTypes::ChosenChunks)
                    .to_string(),
                user_message,
                search_id: search_id.unwrap_or_default(),
                results: vec![String::new()],
                json_results: results
                    .unwrap_or_default()
                    .iter()
                    .map(|r| r.to_string())
                    .collect(),
                query_rating: serde_json::to_string(&query_rating).unwrap_or("".to_string()),
                llm_response: llm_response.unwrap_or_default(),
                top_score: 0.0,
                dataset_id,
                created_at: OffsetDateTime::now_utc(),
                user_id: user_id.unwrap_or_default(),
                hallucination_score: hallucination_score.unwrap_or(0.0),
                detected_hallucinations: detected_hallucinations.unwrap_or_default(),
            }),
            EventTypes::Recommendation {
                recommendation_type,
                positive_ids,
                negative_ids,
                positive_tracking_ids,
                negative_tracking_ids,
                request_params,
                top_score,
                results,
                user_id,
            } => EventDataTypes::RecommendationEventClickhouse(RecommendationEventClickhouse {
                id: uuid::Uuid::new_v4(),
                recommendation_type: recommendation_type.unwrap_or_default().to_string(),
                positive_ids: positive_ids
                    .unwrap_or_default()
                    .iter()
                    .map(|id| id.to_string())
                    .collect(),
                negative_ids: negative_ids
                    .unwrap_or_default()
                    .iter()
                    .map(|id| id.to_string())
                    .collect(),
                positive_tracking_ids: positive_tracking_ids.unwrap_or_default().clone(),
                negative_tracking_ids: negative_tracking_ids.unwrap_or_default().clone(),
                request_params: serde_json::to_string(&request_params).unwrap_or_default(),
                results: results
                    .unwrap_or_default()
                    .iter()
                    .map(|r| r.to_string())
                    .collect(),
                top_score: top_score.unwrap_or(0.0),
                dataset_id,
                created_at: OffsetDateTime::now_utc(),
                user_id: user_id.unwrap_or_default(),
            }),
        }
    }
}

impl From<EventDataClickhouse> for EventData {
    fn from(clickhouse_response: EventDataClickhouse) -> EventData {
        let (request_type, request_id) = if clickhouse_response.request_id.is_empty() {
            (None, None)
        } else if clickhouse_response.request_type.is_empty() {
            (
                Some(String::from("search")),
                Some(clickhouse_response.request_id),
            )
        } else {
            (
                Some(clickhouse_response.request_type),
                Some(clickhouse_response.request_id),
            )
        };

        let user_id = if clickhouse_response.user_id.is_empty() {
            None
        } else {
            Some(clickhouse_response.user_id)
        };

        EventData {
            id: uuid::Uuid::from_bytes(*clickhouse_response.id.as_bytes()),
            event_type: clickhouse_response.event_type,
            event_name: clickhouse_response.event_name,
            request_id,
            request_type,
            items: clickhouse_response.items,
            metadata: serde_json::from_str(&clickhouse_response.metadata).unwrap_or_default(),
            user_id,
            is_conversion: Some(clickhouse_response.is_conversion),
            dataset_id: uuid::Uuid::from_bytes(*clickhouse_response.dataset_id.as_bytes()),
            created_at: clickhouse_response.created_at.to_string(),
            updated_at: clickhouse_response.updated_at.to_string(),
        }
    }
}

/// Filter to apply to the events when querying for them
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[schema(example = json!({
    "date_range": {
        "gt": "2021-08-10T00:00:00Z",
        "lt": "2021-08-11T00:00:00Z"
    },
    "event_type": "view",
    "is_conversion": true,
    "user_id": "user1",
    "metadata_filter": "path = \"value\""
}))]
pub struct EventAnalyticsFilter {
    /// Filter by date range
    pub date_range: Option<DateRange>,
    /// Filter by event type
    pub event_type: Option<EventTypesFilter>,
    /// Filter by conversions
    pub is_conversion: Option<bool>,
    /// Filter by user ID
    pub user_id: Option<String>,
    /// Filter by metadata path i.e. path.attribute = \"value\"
    pub metadata_filter: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Display)]
#[serde(rename_all = "snake_case")]
pub enum EventTypesFilter {
    #[display(fmt = "add_to_cart")]
    AddToCart,
    #[display(fmt = "purchase")]
    Purchase,
    #[display(fmt = "view")]
    View,
    #[display(fmt = "click")]
    Click,
    #[display(fmt = "filter_clicked")]
    FilterClicked,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct GetEventsRequestBody {
    /// Filter to apply to the events
    pub filter: Option<EventAnalyticsFilter>,
    /// Page of results to return
    pub page: Option<u32>,
}

fn convert_filter(
    json_column: &str,
    json_filter: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    // Parse the JSON filter
    let parts: Vec<&str> = json_filter.split('=').collect();
    if parts.len() != 2 {
        return Err("Invalid filter format. Expected 'path = value'".into());
    }

    let path = parts.first().unwrap_or(&"").trim();
    let value = &parts[1].trim().replace("'", "\"");

    // Parse the value as JSON to handle different types
    let json_value: Value = serde_json::from_str(value)?;

    // Convert dot notation to nested JSON extraction
    let json_path = path.split('.').collect::<Vec<&str>>();
    let json_extract_path = json_path.join("', '");

    // Generate ClickHouse filter based on value type
    let clickhouse_filter = match json_value {
        Value::String(s) => {
            format!(
                "JSONExtractString({}, '{}') = '{}'",
                json_column,
                json_extract_path,
                s.replace("'", "\\'")
            )
        }
        Value::Number(n) => {
            if n.is_i64() {
                format!(
                    "JSONExtractInt({}, '{}') = {}",
                    json_column, json_extract_path, n
                )
            } else {
                format!(
                    "JSONExtractFloat({}, '{}') = {}",
                    json_column, json_extract_path, n
                )
            }
        }
        Value::Bool(b) => {
            format!(
                "JSONExtractBool({}, '{}') = {}",
                json_column,
                json_extract_path,
                if b { "true" } else { "false" }
            )
        }
        Value::Null => format!(
            "JSONExtractString({}, '{}') IS NULL",
            json_column, json_extract_path
        ),
        _ => return Err("Unsupported value type".into()),
    };

    Ok(clickhouse_filter)
}

impl EventAnalyticsFilter {
    pub fn add_to_query(
        &self,
        mut query_string: String,
    ) -> Result<String, Box<dyn std::error::Error>> {
        if let Some(date_range) = &self.date_range {
            if let Some(gt) = &date_range.gt {
                query_string.push_str(&format!(" AND created_at > '{}'", gt));
            }
            if let Some(lt) = &date_range.lt {
                query_string.push_str(&format!(" AND created_at < '{}'", lt));
            }
            if let Some(gte) = &date_range.gte {
                query_string.push_str(&format!(" AND created_at >= '{}'", gte));
            }
            if let Some(lte) = &date_range.lte {
                query_string.push_str(&format!(" AND created_at <= '{}'", lte));
            }
        }

        if let Some(event_type) = &self.event_type {
            query_string.push_str(&format!(" AND event_type = '{}'", event_type));
        }

        if let Some(metadata_filter) = &self.metadata_filter {
            let filter = convert_filter("metadata", metadata_filter)?;
            query_string.push_str(&format!(" AND {}", filter));
        }

        if let Some(is_conversion) = &self.is_conversion {
            query_string.push_str(&format!(" AND is_conversion = {}", is_conversion));
        }

        if let Some(user_id) = &self.user_id {
            query_string.push_str(&format!(" AND user_id = '{}'", user_id));
        }

        Ok(query_string)
    }
}

/// Response body for the GetEvents endpoint
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct GetEventsResponseBody {
    pub events: Vec<EventData>,
}

#[derive(Debug, ToSchema, Serialize, Deserialize, Row)]
#[schema(example = json!({
    "clause": "must",
    "field": "metadata.ep_num",
    "filter_type": "match_any",
    "count": 8,
    "common_values": {
        "130": 2,
        "198": 11
    }
}))]
pub struct PopularFilters {
    pub clause: String,
    pub field: String,
    pub filter_type: String,
    pub count: i64,
    pub common_values: HashMap<String, u32>,
}

fn dedup_string_to_hashmap(input: &str) -> HashMap<String, u32> {
    let mut result: HashMap<String, u32> = HashMap::new();

    // Split the input string and process each part
    for part in input.split(", ") {
        if let Some((key, value)) = part.split_once("]: ") {
            let key = key
                .trim_start_matches('[')
                .trim_end_matches(']')
                .split(',')
                .map(|s| s.trim().trim_matches('"').to_string())
                .collect::<Vec<String>>()
                .join(", ");

            if let Ok(count) = value.parse::<u32>() {
                if !key.is_empty() {
                    result.entry(key).or_insert(count);
                }
            }
        }
    }

    result
}

impl From<PopularFiltersClickhouse> for PopularFilters {
    fn from(clickhouse: PopularFiltersClickhouse) -> Self {
        let common_values: HashMap<String, u32> =
            dedup_string_to_hashmap(&clickhouse.common_values);
        PopularFilters {
            clause: clickhouse.clause,
            field: clickhouse.field,
            filter_type: clickhouse.filter_type,
            count: clickhouse.count,
            common_values,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, Selectable, Clone, ToSchema)]
#[diesel(table_name = stripe_invoices)]
pub struct StripeInvoice {
    pub id: uuid::Uuid,
    pub org_id: uuid::Uuid,
    pub total: i32,
    pub created_at: chrono::NaiveDateTime,
    pub status: String,
    pub hosted_invoice_url: String,
    pub stripe_id: Option<String>,
}

impl StripeInvoice {
    pub fn from_details(
        org_id: uuid::Uuid,
        total: i64,
        created_at: chrono::NaiveDateTime,
        status: String,
        url: String,
        stripe_id: String,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            org_id,
            total: total as i32,
            created_at,
            status,
            hosted_invoice_url: url,
            stripe_id: Some(stripe_id),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Display, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Granularity {
    #[display(fmt = "minute")]
    Minute,
    #[display(fmt = "second")]
    Second,
    #[display(fmt = "hour")]
    Hour,
    #[display(fmt = "day")]
    Day,
    #[display(fmt = "month")]
    Month,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Display, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SearchSortBy {
    #[display(fmt = "created_at")]
    CreatedAt,
    #[display(fmt = "latency")]
    Latency,
    #[display(fmt = "top_score")]
    TopScore,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Display, Clone)]
#[serde(rename_all = "snake_case")]
pub enum RAGSortBy {
    #[display(fmt = "hallucination_score")]
    HallucinationScore,
    #[display(fmt = "top_score")]
    TopScore,
    #[display(fmt = "created_at")]
    CreatedAt,
    #[display(fmt = "latency")]
    Latency,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Display, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    #[display(fmt = "DESC")]
    Desc,
    #[display(fmt = "ASC")]
    Asc,
}

impl From<SortOrder> for i32 {
    fn from(val: SortOrder) -> Self {
        match val {
            SortOrder::Desc => 1,
            SortOrder::Asc => 0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum SearchAnalytics {
    #[schema(title = "LatencyGraph")]
    LatencyGraph {
        filter: Option<SearchAnalyticsFilter>,
        granularity: Option<Granularity>,
    },
    #[serde(rename = "search_usage_graph")]
    #[schema(title = "SearchUsageGraph")]
    SearchUsageGraph {
        filter: Option<SearchAnalyticsFilter>,
        granularity: Option<Granularity>,
    },
    #[schema(title = "SearchMetrics")]
    SearchMetrics {
        filter: Option<SearchAnalyticsFilter>,
    },
    #[schema(title = "HeadQueries")]
    HeadQueries {
        filter: Option<SearchAnalyticsFilter>,
        page: Option<u32>,
    },
    #[schema(title = "LowConfidenceQueries")]
    LowConfidenceQueries {
        filter: Option<SearchAnalyticsFilter>,
        page: Option<u32>,
        threshold: Option<f32>,
    },
    #[schema(title = "NoResultQueries")]
    NoResultQueries {
        filter: Option<SearchAnalyticsFilter>,
        page: Option<u32>,
    },
    #[schema(title = "SearchQueries")]
    SearchQueries {
        filter: Option<SearchAnalyticsFilter>,
        page: Option<u32>,
        sort_by: Option<SearchSortBy>,
        sort_order: Option<SortOrder>,
    },
    #[schema(title = "CountQueries")]
    CountQueries {
        filter: Option<SearchAnalyticsFilter>,
    },
    #[schema(title = "QueryDetails")]
    QueryDetails { request_id: uuid::Uuid },
    #[schema(title = "PopularFilters")]
    PopularFilters {
        filter: Option<SearchAnalyticsFilter>,
    },
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum RAGAnalytics {
    #[schema(title = "RAGQueries")]
    #[serde(rename = "rag_queries")]
    RAGQueries {
        filter: Option<RAGAnalyticsFilter>,
        page: Option<u32>,
        sort_by: Option<RAGSortBy>,
        sort_order: Option<SortOrder>,
    },
    #[schema(title = "RAGUsage")]
    #[serde(rename = "rag_usage")]
    RAGUsage { filter: Option<RAGAnalyticsFilter> },
    #[schema(title = "RAGUsageGraph")]
    #[serde(rename = "rag_usage_graph")]
    RAGUsageGraph {
        filter: Option<RAGAnalyticsFilter>,
        granularity: Option<Granularity>,
    },
    #[schema(title = "QueryDetails")]
    #[serde(rename = "rag_query_details")]
    RAGQueryDetails { request_id: uuid::Uuid },
    #[schema(title = "RAGQueryRatings")]
    #[serde(rename = "rag_query_ratings")]
    RAGQueryRatings { filter: Option<RAGAnalyticsFilter> },
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum RecommendationAnalytics {
    #[schema(title = "LowConfidenceRecommendations")]
    LowConfidenceRecommendations {
        filter: Option<RecommendationAnalyticsFilter>,
        page: Option<u32>,
        threshold: Option<f32>,
    },
    #[schema(title = "RecommendationQueries")]
    RecommendationQueries {
        filter: Option<RecommendationAnalyticsFilter>,
        page: Option<u32>,
        sort_by: Option<SearchSortBy>,
        sort_order: Option<SortOrder>,
    },
    #[schema(title = "QueryDetails")]
    QueryDetails { request_id: uuid::Uuid },
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ClusterAnalytics {
    #[schema(title = "ClusterTopics")]
    ClusterTopics {
        filter: Option<ClusterAnalyticsFilter>,
    },
    #[schema(title = "ClusterQueries")]
    ClusterQueries {
        cluster_id: uuid::Uuid,
        page: Option<u32>,
    },
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum CTRAnalytics {
    #[serde(rename = "search_ctr_metrics")]
    #[schema(title = "SearchCTRMetrics")]
    SearchCTRMetrics {
        filter: Option<SearchAnalyticsFilter>,
    },
    #[schema(title = "SearchesWithClicks")]
    SearchesWithClicks {
        filter: Option<SearchAnalyticsFilter>,
        page: Option<u32>,
    },
    #[schema(title = "SearchesWithoutClicks")]
    SearchesWithoutClicks {
        filter: Option<SearchAnalyticsFilter>,
        page: Option<u32>,
    },
    #[schema(title = "RecommendationCTRMetrics")]
    #[serde(rename = "recommendation_ctr_metrics")]
    RecommendationCTRMetrics {
        filter: Option<RecommendationAnalyticsFilter>,
    },
    #[schema(title = "RecommendationsWithClicks")]
    RecommendationsWithClicks {
        filter: Option<RecommendationAnalyticsFilter>,
        page: Option<u32>,
    },
    #[schema(title = "RecommendationsWithoutClicks")]
    RecommendationsWithoutClicks {
        filter: Option<RecommendationAnalyticsFilter>,
        page: Option<u32>,
    },
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Row)]
#[schema(title = "RAGUsageResponse")]
pub struct RAGUsageResponse {
    pub total_queries: u32,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(title = "RAGUsageGraphResponse")]
pub struct RAGUsageGraphResponse {
    pub usage_points: Vec<UsageGraphPoint>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Row)]
#[schema(title = "RagQueryRatingsResponse")]
pub struct RagQueryRatingsResponse {
    /// Total number of positive RAG ratings
    pub total_positive_ratings: i64,
    /// Total number of negative RAG ratings
    pub total_negative_ratings: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum SearchAnalyticsResponse {
    #[schema(title = "LatencyGraph")]
    LatencyGraph(LatencyGraphResponse),
    #[schema(title = "SearchUsageGraph")]
    SearchUsageGraph(SearchUsageGraphResponse),
    #[schema(title = "SearchMetrics")]
    SearchMetrics(DatasetAnalytics),
    #[schema(title = "HeadQueries")]
    HeadQueries(HeadQueryResponse),
    #[schema(title = "LowConfidenceQueries")]
    LowConfidenceQueries(SearchQueryResponse),
    #[schema(title = "NoResultQueries")]
    NoResultQueries(SearchQueryResponse),
    #[schema(title = "SearchQueries")]
    SearchQueries(SearchQueryResponse),
    #[schema(title = "CountQueries")]
    CountQueries(QueryCountResponse),
    #[schema(title = "QueryDetails")]
    QueryDetails(SearchQueryEvent),
    #[schema(title = "PopularFilters")]
    PopularFilters(PopularFiltersResponse),
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum RAGAnalyticsResponse {
    #[schema(title = "RAGQueries")]
    RAGQueries(RagQueryResponse),
    #[schema(title = "RAGUsage")]
    RAGUsage(RAGUsageResponse),
    #[schema(title = "RAGUsageGraph")]
    RAGUsageGraph(RAGUsageGraphResponse),
    #[schema(title = "RAGQueryDetails")]
    RAGQueryDetails(Box<RagQueryEvent>),
    #[schema(title = "RAGQueryRatings")]
    RAGQueryRatings(RagQueryRatingsResponse),
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum ClusterAnalyticsResponse {
    #[schema(title = "ClusterTopics")]
    ClusterTopics(SearchClusterResponse),
    #[schema(title = "ClusterQueries")]
    ClusterQueries(SearchQueryResponse),
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum RecommendationAnalyticsResponse {
    #[schema(title = "LowConfidenceRecommendations")]
    LowConfidenceRecommendations(RecommendationsEventResponse),
    #[schema(title = "RecommendationQueries")]
    RecommendationQueries(RecommendationsEventResponse),
    #[schema(title = "QueryDetails")]
    QueryDetails(RecommendationEvent),
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum CTRAnalyticsResponse {
    #[schema(title = "SearchCTRMetrics")]
    SearchCTRMetrics(SearchCTRMetrics),
    #[schema(title = "SearchesWithoutClicks")]
    SearchesWithoutClicks(CTRSearchQueryWithoutClicksResponse),
    #[schema(title = "SearchesWithClicks")]
    SearchesWithClicks(CTRSearchQueryWithClicksResponse),
    #[schema(title = "RecommendationCTRMetrics")]
    RecommendationCTRMetrics(RecommendationCTRMetrics),
    #[schema(title = "RecommendationsWithoutClicks")]
    RecommendationsWithoutClicks(CTRRecommendationsWithoutClicksResponse),
    #[schema(title = "RecommendationsWithClicks")]
    RecommendationsWithClicks(CTRRecommendationsWithClicksResponse),
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Display, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
/// Strategy to use for recommendations, either "average_vector" or "best_score". The default is "average_vector". The "average_vector" strategy will construct a single average vector from the positive and negative samples then use it to perform a pseudo-search. The "best_score" strategy is more advanced and navigates the HNSW with a heuristic of picking edges where the point is closer to the positive samples than it is the negatives.
pub enum RecommendationStrategy {
    AverageVector,
    BestScore,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Display, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
/// The type of recommendation to make. This lets you choose whether to recommend based off of `semantic` or `fulltext` similarity. The default is `semantic`.
pub enum RecommendType {
    Semantic,
    #[serde(rename = "fulltext", alias = "full_text")]
    FullText,
    #[serde(rename = "bm25", alias = "BM25")]
    BM25,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Display, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EventTypeRequest {
    #[display(fmt = "file_uploaded")]
    FileUploaded,
    #[display(fmt = "file_upload_failed")]
    FileUploadFailed,
    #[display(fmt = "chunks_uploaded")]
    ChunksUploaded,
    #[display(fmt = "chunk_action_failed")]
    ChunkActionFailed,
    #[display(fmt = "chunk_updated")]
    ChunkUpdated,
    #[display(fmt = "bulk_chunks_deleted")]
    BulkChunksDeleted,
    #[display(fmt = "chunk_update_failed")]
    ChunkUpdateFailed,
    #[display(fmt = "dataset_delete_failed")]
    DatasetDeleteFailed,
    #[display(fmt = "qdrant_index_failed")]
    QdrantUploadFailed,
    #[display(fmt = "bulk_chunk_upload_failed")]
    BulkChunkUploadFailed,
    #[display(fmt = "group_chunks_updated")]
    GroupChunksUpdated,
    #[display(fmt = "group_chunks_action_failed")]
    GroupChunksActionFailed,
    #[display(fmt = "crawl_completed")]
    CrawlCompleted,
    #[display(fmt = "crawl_failed")]
    CrawlFailed,
    #[display(fmt = "crawl_started")]
    CrawlStarted,
    #[display(fmt = "csv_jsonl_processing_failed")]
    CsvJsonlProcessingFailed,
    #[display(fmt = "csv_jsonl_processing_checkpoint")]
    CsvJsonlProcessingCheckpoint,
    #[display(fmt = "csv_jsonl_processing_completed")]
    CsvJsonlProcessingCompleted,
    #[display(fmt = "video_uploaded")]
    VideoUploaded,
    #[display(fmt = "pagefind_indexing_started")]
    PagefindIndexingStarted,
    #[display(fmt = "pagefind_indexing_finished")]
    PagefindIndexingFinished,
    #[display(fmt = "etl_started")]
    EtlStarted,
    #[display(fmt = "etl_completed")]
    EtlCompleted,
    #[display(fmt = "etl_failed")]
    EtlFailed,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum MigrationMode {
    BM25 { average_len: f32, k: f32, b: f32 },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MigratePointMessage {
    pub qdrant_point_ids: Vec<uuid::Uuid>,
    pub to_collection: String,
    pub from_collection: String,
    pub mode: MigrationMode,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct SortByField {
    /// Field to sort by. This has to be a numeric field with a Qdrant `Range` index on it. i.e. num_value and timestamp
    pub field: String,
    /// Direction to sort by
    pub direction: Option<SortOrder>,
    /// How many results to pull in before the sort
    pub prefetch_amount: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct SortBySearchType {
    /// Search Method to get candidates from
    pub rerank_type: ReRankOptions,
    /// How many results to pull in before the rerabj
    pub prefetch_amount: Option<u64>,
    /// Query to use for prefetching defaults to the search query
    pub rerank_query: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(untagged)]
/// Sort by lets you specify a method to sort the results by. If not specified, this defaults to the score of the chunks. If specified, this can be any key in the chunk metadata. This key must be a numeric value within the payload.
pub enum QdrantSortBy {
    Field(SortByField),
    SearchType(SortBySearchType),
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReRankOptions {
    Semantic,
    Fulltext,
    #[serde(rename = "bm25", alias = "BM25")]
    BM25,
    CrossEncoder,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct ChunkWithPosition {
    pub chunk_id: uuid::Uuid,
    pub position: i32,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct ChunkMetadataWithPosition {
    pub chunk: ChunkMetadata,
    pub position: i32,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Default)]
pub struct RequestInfo {
    pub request_type: CTRType,
    pub request_id: uuid::Uuid,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Display)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "event_type")]
pub enum EventTypes {
    #[display(fmt = "view")]
    #[schema(title = "View")]
    View {
        /// The name of the event
        event_name: String,
        /// The request id of the event to associate it with a request
        request: Option<RequestInfo>,
        /// The items that were viewed
        items: Vec<String>,
        /// The user id of the user who viewed the items
        user_id: Option<String>,
        /// Any other metadata associated with the event
        metadata: Option<serde_json::Value>,
    },
    #[display(fmt = "add_to_cart")]
    #[schema(title = "AddToCart")]
    AddToCart {
        /// The name of the event
        event_name: String,
        /// The request id of the event to associate it with a request
        request: Option<RequestInfo>,
        /// The items that were added to the cart
        items: Vec<String>,
        /// The user id of the user who added the items to the cart
        user_id: Option<String>,
        /// Any other metadata associated with the event
        metadata: Option<serde_json::Value>,
        /// Whether the event is a conversion event
        is_conversion: Option<bool>,
    },
    #[display(fmt = "click")]
    #[schema(title = "Click")]
    Click {
        /// The name of the event
        event_name: String,
        /// The request id of the event to associate it with a request
        request: Option<RequestInfo>,
        /// The items that were clicked and their positons in a hashmap ie. {item_id: position}
        clicked_items: Option<ChunkWithPosition>,
        /// The user id of the user who clicked the items
        user_id: Option<String>,
        /// Whether the event is a conversion event
        is_conversion: Option<bool>,
    },
    #[display(fmt = "purchase")]
    #[schema(title = "Purchase")]
    Purchase {
        /// The name of the event
        event_name: String,
        /// The request id of the event to associate it with a request
        request: Option<RequestInfo>,
        /// The items that were purchased
        items: Vec<String>,
        /// The user id of the user who purchased the items
        user_id: Option<String>,
        /// The value of the purchase
        value: Option<f64>,
        /// The currency of the purchase
        currency: Option<String>,
        /// Whether the event is a conversion event
        is_conversion: Option<bool>,
    },
    #[display(fmt = "filter_clicked")]
    #[schema(title = "FilterClicked")]
    FilterClicked {
        /// The name of the event
        event_name: String,
        /// The request id of the event to associate it with a request
        request: Option<RequestInfo>,
        /// The filter items that were clicked in a hashmap ie. {filter_name: filter_value} where filter_name is filter_type::field_name
        items: HashMap<String, String>,
        /// The user id of the user who clicked the items
        user_id: Option<String>,
        /// Whether the event is a conversion event
        is_conversion: Option<bool>,
    },
    #[display(fmt = "search")]
    #[schema(title = "Search")]
    Search {
        /// The search type: search, rag, or search_over_groups
        search_type: Option<ClickhouseSearchTypes>,
        /// The search query
        query: String,
        /// The request params of the search
        request_params: Option<serde_json::Value>,
        /// Latency of the search
        latency: Option<f32>,
        /// The top score of the search
        top_score: Option<f32>,
        /// The results of the search
        results: Option<Vec<serde_json::Value>>,
        /// The rating of the query
        query_rating: Option<SearchQueryRating>,
        /// The user id of the user who made the search
        user_id: Option<String>,
    },
    #[display(fmt = "rag")]
    #[serde(rename = "rag")]
    #[schema(title = "RAG")]
    RAG {
        /// The Type of RAG event: chosen_chunks, all_chunks
        rag_type: Option<ClickhouseRagTypes>,
        /// The user message
        user_message: String,
        /// The search id to associate the RAG event with a search
        search_id: Option<uuid::Uuid>,
        /// The results of the RAG event    
        results: Option<Vec<serde_json::Value>>,
        /// The rating of the query
        query_rating: Option<SearchQueryRating>,
        /// The response from the LLM
        llm_response: Option<String>,
        /// The user id of the user who made the RAG event
        user_id: Option<String>,
        /// The hallucination score of the RAG event
        hallucination_score: Option<f64>,
        /// The detected hallucinations of the RAG event
        detected_hallucinations: Option<Vec<String>>,
    },
    #[display(fmt = "recommendation")]
    #[schema(title = "Recommendation")]
    Recommendation {
        /// The Type of Recommendation event: chunk, group
        recommendation_type: Option<ClickhouseRecommendationTypes>,
        /// Positive ids used for the recommendation
        positive_ids: Option<Vec<uuid::Uuid>>,
        /// Negative ids used for the recommendation
        negative_ids: Option<Vec<uuid::Uuid>>,
        /// Positive tracking ids used for the recommendation
        positive_tracking_ids: Option<Vec<String>>,
        /// Negative tracking ids used for the recommendation
        negative_tracking_ids: Option<Vec<String>>,
        /// The request params of the recommendation
        request_params: Option<serde_json::Value>,
        /// The results of the Recommendation event    
        results: Option<Vec<serde_json::Value>>,
        /// Top score of the recommendation
        top_score: Option<f32>,
        /// The user id of the user who made the recommendation
        user_id: Option<String>,
    },
}

impl From<CTRDataRequestBody> for EventTypes {
    fn from(data: CTRDataRequestBody) -> Self {
        EventTypes::Click {
            event_name: String::from("click"),
            request: Some(RequestInfo {
                request_type: data.ctr_type,
                request_id: data.request_id,
            }),
            clicked_items: Some(ChunkWithPosition {
                chunk_id: data.clicked_chunk_id.unwrap_or_default(),
                position: data.position,
            }),
            user_id: None,
            is_conversion: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Default, Display)]
#[serde(rename_all = "snake_case")]
pub enum CTRType {
    #[default]
    #[display(fmt = "search")]
    Search,
    #[serde(rename = "rag")]
    #[display(fmt = "rag")]
    RAG,
    #[display(fmt = "recommendation")]
    Recommendation,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema, Default)]
/// Sort Options lets you specify different methods to rerank the chunks in the result set. If not specified, this defaults to the score of the chunks.
pub struct SortOptions {
    /// Sort by lets you specify a method to sort the results by. If not specified, this defaults to the score of the chunks. If specified, this can be any key in the chunk metadata. This key must be a numeric value within the payload.
    pub sort_by: Option<QdrantSortBy>,
    /// Location lets you rank your results by distance from a location. If not specified, this has no effect. Bias allows you to determine how much of an effect the location of chunks will have on the search results. If not specified, this defaults to 0.0. We recommend setting this to 1.0 for a gentle reranking of the results, >3.0 for a strong reranking of the results.
    pub location_bias: Option<GeoInfoWithBias>,
    /// Recency Bias lets you determine how much of an effect the recency of chunks will have on the search results. If not specified, this defaults to 0.0. We recommend setting this to 1.0 for a gentle reranking of the results, >3.0 for a strong reranking of the results.
    pub recency_bias: Option<f32>,
    /// Set use_weights to true to use the weights of the chunks in the result set in order to sort them. If not specified, this defaults to true.
    pub use_weights: Option<bool>,
    /// Tag weights is a JSON object which can be used to boost the ranking of chunks with certain tags. This is useful for when you want to be able to bias towards chunks with a certain tag on the fly. The keys are the tag names and the values are the weights.
    pub tag_weights: Option<HashMap<String, f32>>,
    /// Set use_mmr to true to use the Maximal Marginal Relevance algorithm to rerank the results. If not specified, this defaults to false.
    pub mmr: Option<MmrOptions>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema, Default)]
/// MMR Options lets you specify different methods to rerank the chunks in the result set using Maximal Marginal Relevance. If not specified, this defaults to the score of the chunks.
pub struct MmrOptions {
    /// Set use_mmr to true to use the Maximal Marginal Relevance algorithm to rerank the results.
    pub use_mmr: bool,
    /// Set mmr_lambda to a value between 0.0 and 1.0 to control the tradeoff between relevance and diversity. Closer to 1.0 will give more diverse results, closer to 0.0 will give more relevant results. If not specified, this defaults to 0.5.
    pub mmr_lambda: Option<f32>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema, Default)]
/// Highlight Options lets you specify different methods to highlight the chunks in the result set. If not specified, this defaults to the score of the chunks.
pub struct HighlightOptions {
    /// Set highlight_results to false for a slight latency improvement (1-10ms). If not specified, this defaults to true. This will add `<mark><b>` tags to the chunk_html of the chunks to highlight matching splits and return the highlights on each scored chunk in the response.
    pub highlight_results: Option<bool>,
    /// Set highlight_exact_match to true to highlight exact matches from your query.
    pub highlight_strategy: Option<HighlightStrategy>,
    /// Set highlight_threshold to a lower or higher value to adjust the sensitivity of the highlights applied to the chunk html. If not specified, this defaults to 0.8. The range is 0.0 to 1.0.
    pub highlight_threshold: Option<f64>,
    /// Set highlight_delimiters to a list of strings to use as delimiters for highlighting. If not specified, this defaults to ["?", ",", ".", "!"]. These are the characters that will be used to split the chunk_html into splits for highlighting. These are the characters that will be used to split the chunk_html into splits for highlighting.
    pub highlight_delimiters: Option<Vec<char>>,
    /// Set highlight_max_length to control the maximum number of tokens (typically whitespace separated strings, but sometimes also word stems) which can be present within a single highlight. If not specified, this defaults to 8. This is useful to shorten large splits which may have low scores due to length compared to the query. Set to something very large like 100 to highlight entire splits.
    pub highlight_max_length: Option<u32>,
    /// Set highlight_max_num to control the maximum number of highlights per chunk. If not specified, this defaults to 3. It may be less than 3 if no snippets score above the highlight_threshold.
    pub highlight_max_num: Option<u32>,
    /// Set highlight_window to a number to control the amount of words that are returned around the matched phrases. If not specified, this defaults to 0. This is useful for when you want to show more context around the matched words. When specified, window/2 whitespace separated words are added before and after each highlight in the response's highlights array. If an extended highlight overlaps with another highlight, the overlapping words are only included once. This parameter can be overriden to respect the highlight_max_length param.
    pub highlight_window: Option<u32>,
    /// Custom html tag which should appear before highlights. If not specified, this defaults to '<mark><b>'.
    pub pre_tag: Option<String>,
    /// Custom html tag which should appear after highlights. If not specified, this defaults to '</mark></b>'.
    pub post_tag: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema, Default)]
/// Typo Options lets you specify different methods to correct typos in the query. If not specified, typos will not be corrected.
pub struct TypoOptions {
    /// Set correct_typos to true to correct typos in the query. If not specified, this defaults to false.
    pub correct_typos: Option<bool>,
    /// The range of which the query will be corrected if it has one typo. If not specified, this defaults to 5-8.
    pub one_typo_word_range: Option<TypoRange>,
    /// The range of which the query will be corrected if it has two typos. If not specified, this defaults to 8-inf.
    pub two_typo_word_range: Option<TypoRange>,
    /// Words that should not be corrected. If not specified, this defaults to an empty list.
    pub disable_on_word: Option<Vec<String>>,
    /// Auto-require non-english words present in the dataset to exist in each results chunk_html text. If not specified, this defaults to true.
    pub prioritize_domain_specifc_words: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema, Default)]
/// The TypoRange struct is used to specify the range of which the query will be corrected if it has a typo.
pub struct TypoRange {
    /// The minimum number of characters that the query will be corrected if it has a typo. If not specified, this defaults to 5.
    pub min: u32,
    /// The maximum number of characters that the query will be corrected if it has a typo. If not specified, this defaults to 8.
    pub max: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Default)]
#[schema(example = json!({
    "use_images": true,
    "images_per_chunk": 1
}))]
/// Configuration for sending images to the llm
pub struct ImageConfig {
    /// This sends images to the llm if chunk_metadata.image_urls has some value, the call will error if the model is not a vision LLM model. default: false
    pub use_images: Option<bool>,
    /// The number of Images to send to the llm per chunk that is fetched more images may slow down llm inference time. default: 5
    pub images_per_chunk: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug, ToSchema, Clone)]
/// Context options to use for the completion. If not specified, all options will default to false.
pub struct ContextOptions {
    /// Include links in the context. If not specified, this defaults to false.
    pub include_links: Option<bool>,
}

impl Default for ContextOptions {
    fn default() -> Self {
        ContextOptions {
            include_links: Some(false),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Default)]
/// LLM options to use for the completion. If not specified, this defaults to the dataset's LLM options.
pub struct LLMOptions {
    /// Completion first decides whether the stream should contain the stream of the completion response or the chunks first. Default is false. Keep in mind that || is used to separate the chunks from the completion response. If || is in the completion then you may want to split on ||{ instead.
    pub completion_first: Option<bool>,
    /// Whether or not to stream the response. If this is set to true or not included, the response will be a stream. If this is set to false, the response will be a normal JSON response. Default is true.
    pub stream_response: Option<bool>,
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
    /// Optionally, override the system prompt in dataset server settings.
    pub system_prompt: Option<String>,
    /// Configuration for sending images to the llm
    pub image_config: Option<ImageConfig>,
}

// Helper function to extract SortOptions and HighlightOptions
fn extract_sort_highlight_options(
    other: &mut HashMap<String, Value>,
) -> (Option<SortOptions>, Option<HighlightOptions>) {
    let mut sort_options = SortOptions::default();
    let mut highlight_options = HighlightOptions::default();

    // Extract sort options
    if let Some(value) = other.remove("sort_by") {
        sort_options.sort_by = serde_json::from_value(value).ok();
    }
    if let Some(value) = other.remove("location_bias") {
        sort_options.location_bias = serde_json::from_value(value).ok();
    }
    if let Some(value) = other.remove("use_weights") {
        sort_options.use_weights = serde_json::from_value(value).ok();
    }
    if let Some(value) = other.remove("tag_weights") {
        sort_options.tag_weights = serde_json::from_value(value).ok();
    }
    if let Some(value) = other.remove("mmr") {
        sort_options.mmr = serde_json::from_value(value).ok();
    }

    // Extract highlight options
    if let Some(value) = other.remove("highlight_results") {
        highlight_options.highlight_results = serde_json::from_value(value).ok();
    }
    if let Some(value) = other.remove("highlight_strategy") {
        highlight_options.highlight_strategy = serde_json::from_value(value).ok();
    }
    if let Some(value) = other.remove("highlight_threshold") {
        highlight_options.highlight_threshold = serde_json::from_value(value).ok();
    }
    if let Some(value) = other.remove("highlight_delimiters") {
        highlight_options.highlight_delimiters = serde_json::from_value(value).ok();
    }
    if let Some(value) = other.remove("highlight_max_length") {
        highlight_options.highlight_max_length = serde_json::from_value(value).ok();
    }
    if let Some(value) = other.remove("highlight_max_num") {
        highlight_options.highlight_max_num = serde_json::from_value(value).ok();
    }
    if let Some(value) = other.remove("highlight_window") {
        highlight_options.highlight_window = serde_json::from_value(value).ok();
    }

    let sort_options = if sort_options.sort_by.is_none()
        && sort_options.location_bias.is_none()
        && sort_options.use_weights.is_none()
        && sort_options.tag_weights.is_none()
        && sort_options.mmr.is_none()
    {
        None
    } else {
        Some(sort_options)
    };

    let highlight_options = if highlight_options.highlight_results.is_none()
        && highlight_options.highlight_strategy.is_none()
        && highlight_options.highlight_threshold.is_none()
        && highlight_options.highlight_delimiters.is_none()
        && highlight_options.highlight_max_length.is_none()
        && highlight_options.highlight_max_num.is_none()
        && highlight_options.highlight_window.is_none()
    {
        None
    } else {
        Some(highlight_options)
    };

    (sort_options, highlight_options)
}

fn extract_context_options(other: &mut HashMap<String, Value>) -> Option<ContextOptions> {
    let mut context_options = ContextOptions::default();

    if let Some(value) = other.remove("include_links") {
        context_options.include_links = serde_json::from_value(value).ok();
    }

    if context_options.include_links.is_none() {
        None
    } else {
        Some(context_options)
    }
}

fn extract_llm_options(other: &mut HashMap<String, Value>) -> Option<LLMOptions> {
    let mut llm_options = LLMOptions::default();

    if let Some(value) = other.remove("completion_first") {
        llm_options.completion_first = serde_json::from_value(value).ok();
    }
    if let Some(value) = other.remove("stream_response") {
        llm_options.stream_response = serde_json::from_value(value).ok();
    }
    if let Some(value) = other.remove("temperature") {
        llm_options.temperature = serde_json::from_value(value).ok();
    }
    if let Some(value) = other.remove("frequency_penalty") {
        llm_options.frequency_penalty = serde_json::from_value(value).ok();
    }
    if let Some(value) = other.remove("presence_penalty") {
        llm_options.presence_penalty = serde_json::from_value(value).ok();
    }
    if let Some(value) = other.remove("max_tokens") {
        llm_options.max_tokens = serde_json::from_value(value).ok();
    }
    if let Some(value) = other.remove("stop_tokens") {
        llm_options.stop_tokens = serde_json::from_value(value).ok();
    }
    if let Some(value) = other.remove("system_prompt") {
        llm_options.system_prompt = serde_json::from_value(value).ok();
    }

    if llm_options.completion_first.is_none()
        && llm_options.stream_response.is_none()
        && llm_options.temperature.is_none()
        && llm_options.frequency_penalty.is_none()
        && llm_options.presence_penalty.is_none()
        && llm_options.max_tokens.is_none()
        && llm_options.stop_tokens.is_none()
        && llm_options.system_prompt.is_none()
    {
        None
    } else {
        Some(llm_options)
    }
}

impl<'de> Deserialize<'de> for SearchChunksReqPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            search_type: SearchMethod,
            query: QueryTypes,
            page: Option<u64>,
            page_size: Option<u64>,
            get_total_pages: Option<bool>,
            filters: Option<ChunkFilter>,
            sort_options: Option<SortOptions>,
            scoring_options: Option<ScoringOptions>,
            highlight_options: Option<HighlightOptions>,
            score_threshold: Option<f32>,
            slim_chunks: Option<bool>,
            content_only: Option<bool>,
            use_quote_negated_terms: Option<bool>,
            remove_stop_words: Option<bool>,
            user_id: Option<String>,
            typo_options: Option<TypoOptions>,
            #[serde(flatten)]
            other: std::collections::HashMap<String, serde_json::Value>,
        }

        let mut helper = Helper::deserialize(deserializer)?;

        let (extracted_sort_options, extracted_highlight_options) = if !helper.other.is_empty() {
            extract_sort_highlight_options(&mut helper.other)
        } else {
            (None, None)
        };

        let sort_options = helper.sort_options.or(extracted_sort_options);
        let highlight_options = helper.highlight_options.or(extracted_highlight_options);

        Ok(SearchChunksReqPayload {
            search_type: helper.search_type,
            query: helper.query,
            page: helper.page,
            page_size: helper.page_size,
            get_total_pages: helper.get_total_pages,
            filters: helper.filters,
            sort_options,
            scoring_options: helper.scoring_options,
            highlight_options,
            score_threshold: helper.score_threshold,
            slim_chunks: helper.slim_chunks,
            content_only: helper.content_only,
            use_quote_negated_terms: helper.use_quote_negated_terms,
            remove_stop_words: helper.remove_stop_words,
            user_id: helper.user_id,
            typo_options: helper.typo_options,
        })
    }
}

impl<'de> Deserialize<'de> for AutocompleteReqPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            search_type: SearchMethod,
            extend_results: Option<bool>,
            query: SearchModalities,
            page_size: Option<u64>,
            filters: Option<ChunkFilter>,
            sort_options: Option<SortOptions>,
            scoring_options: Option<ScoringOptions>,
            highlight_options: Option<HighlightOptions>,
            score_threshold: Option<f32>,
            slim_chunks: Option<bool>,
            content_only: Option<bool>,
            use_quote_negated_terms: Option<bool>,
            remove_stop_words: Option<bool>,
            user_id: Option<String>,
            typo_options: Option<TypoOptions>,
            #[serde(flatten)]
            other: std::collections::HashMap<String, serde_json::Value>,
        }

        let mut helper = Helper::deserialize(deserializer)?;

        let (extracted_sort_options, extracted_highlight_options) = if !helper.other.is_empty() {
            extract_sort_highlight_options(&mut helper.other)
        } else {
            (None, None)
        };

        let sort_options = helper.sort_options.or(extracted_sort_options);
        let highlight_options = helper.highlight_options.or(extracted_highlight_options);

        Ok(AutocompleteReqPayload {
            search_type: helper.search_type,
            extend_results: helper.extend_results,
            query: helper.query,
            page_size: helper.page_size,
            filters: helper.filters,
            sort_options,
            scoring_options: helper.scoring_options,
            highlight_options,
            score_threshold: helper.score_threshold,
            slim_chunks: helper.slim_chunks,
            content_only: helper.content_only,
            use_quote_negated_terms: helper.use_quote_negated_terms,
            remove_stop_words: helper.remove_stop_words,
            user_id: helper.user_id,
            typo_options: helper.typo_options,
        })
    }
}

impl<'de> Deserialize<'de> for SearchWithinGroupReqPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            search_type: SearchMethod,
            query: QueryTypes,
            page: Option<u64>,
            page_size: Option<u64>,
            group_id: Option<uuid::Uuid>,
            group_tracking_id: Option<String>,
            get_total_pages: Option<bool>,
            filters: Option<ChunkFilter>,
            sort_options: Option<SortOptions>,
            highlight_options: Option<HighlightOptions>,
            score_threshold: Option<f32>,
            slim_chunks: Option<bool>,
            content_only: Option<bool>,
            use_quote_negated_terms: Option<bool>,
            remove_stop_words: Option<bool>,
            user_id: Option<String>,
            typo_options: Option<TypoOptions>,
            #[serde(flatten)]
            other: std::collections::HashMap<String, serde_json::Value>,
        }

        let mut helper = Helper::deserialize(deserializer)?;

        let (extracted_sort_options, extracted_highlight_options) = if !helper.other.is_empty() {
            extract_sort_highlight_options(&mut helper.other)
        } else {
            (None, None)
        };

        let sort_options = helper.sort_options.or(extracted_sort_options);
        let highlight_options = helper.highlight_options.or(extracted_highlight_options);

        Ok(SearchWithinGroupReqPayload {
            search_type: helper.search_type,
            query: helper.query,
            page: helper.page,
            page_size: helper.page_size,
            group_id: helper.group_id,
            group_tracking_id: helper.group_tracking_id,
            get_total_pages: helper.get_total_pages,
            filters: helper.filters,
            sort_options,
            highlight_options,
            score_threshold: helper.score_threshold,
            slim_chunks: helper.slim_chunks,
            content_only: helper.content_only,
            use_quote_negated_terms: helper.use_quote_negated_terms,
            remove_stop_words: helper.remove_stop_words,
            user_id: helper.user_id,
            typo_options: helper.typo_options,
        })
    }
}

impl<'de> Deserialize<'de> for SearchOverGroupsReqPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            search_type: SearchMethod,
            query: QueryTypes,
            page: Option<u64>,
            page_size: Option<u64>,
            get_total_pages: Option<bool>,
            filters: Option<ChunkFilter>,
            group_size: Option<u64>,
            highlight_options: Option<HighlightOptions>,
            score_threshold: Option<f32>,
            slim_chunks: Option<bool>,
            use_quote_negated_terms: Option<bool>,
            remove_stop_words: Option<bool>,
            user_id: Option<String>,
            typo_options: Option<TypoOptions>,
            sort_options: Option<SortOptions>,
            #[serde(flatten)]
            other: std::collections::HashMap<String, serde_json::Value>,
        }

        let mut helper = Helper::deserialize(deserializer)?;

        let (extract_sort_options, extracted_highlight_options) = if !helper.other.is_empty() {
            extract_sort_highlight_options(&mut helper.other)
        } else {
            (None, None)
        };
        let highlight_options = helper.highlight_options.or(extracted_highlight_options);
        let sort_options = helper.sort_options.or(extract_sort_options);

        Ok(SearchOverGroupsReqPayload {
            search_type: helper.search_type,
            query: helper.query,
            page: helper.page,
            page_size: helper.page_size,
            get_total_pages: helper.get_total_pages,
            filters: helper.filters,
            highlight_options,
            group_size: helper.group_size,
            score_threshold: helper.score_threshold,
            slim_chunks: helper.slim_chunks,
            use_quote_negated_terms: helper.use_quote_negated_terms,
            typo_options: helper.typo_options,
            sort_options,
            remove_stop_words: helper.remove_stop_words,
            user_id: helper.user_id,
        })
    }
}

impl<'de> Deserialize<'de> for CreateMessageReqPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            pub new_message_content: Option<String>,
            pub image_urls: Option<Vec<String>>,
            pub audio_input: Option<String>,
            pub topic_id: uuid::Uuid,
            pub highlight_options: Option<HighlightOptions>,
            pub search_type: Option<SearchMethod>,
            pub concat_user_messages_query: Option<bool>,
            pub search_query: Option<String>,
            pub sort_options: Option<SortOptions>,
            pub page_size: Option<u64>,
            pub filters: Option<ChunkFilter>,
            pub score_threshold: Option<f32>,
            pub llm_options: Option<LLMOptions>,
            pub user_id: Option<String>,
            pub use_group_search: Option<bool>,
            pub context_options: Option<ContextOptions>,
            pub no_result_message: Option<String>,
            pub only_include_docs_used: Option<bool>,
            #[serde(flatten)]
            other: std::collections::HashMap<String, serde_json::Value>,
            use_quote_negated_terms: Option<bool>,
            remove_stop_words: Option<bool>,
            typo_options: Option<TypoOptions>,
        }

        let mut helper = Helper::deserialize(deserializer)?;

        let (_, extracted_highlight_options) = if !helper.other.is_empty() {
            extract_sort_highlight_options(&mut helper.other)
        } else {
            (None, None)
        };
        let llm_options = extract_llm_options(&mut helper.other);
        let context_options = extract_context_options(&mut helper.other);
        let highlight_options = helper.highlight_options.or(extracted_highlight_options);
        let llm_options = helper.llm_options.or(llm_options);
        let context_options = helper.context_options.or(context_options);

        Ok(CreateMessageReqPayload {
            new_message_content: helper.new_message_content,
            audio_input: helper.audio_input,
            image_urls: helper.image_urls,
            topic_id: helper.topic_id,
            highlight_options,
            sort_options: helper.sort_options,
            search_type: helper.search_type,
            use_group_search: helper.use_group_search,
            concat_user_messages_query: helper.concat_user_messages_query,
            search_query: helper.search_query,
            page_size: helper.page_size,
            filters: helper.filters,
            score_threshold: helper.score_threshold,
            llm_options,
            user_id: helper.user_id,
            context_options,
            no_result_message: helper.no_result_message,
            only_include_docs_used: helper.only_include_docs_used,
            use_quote_negated_terms: helper.use_quote_negated_terms,
            remove_stop_words: helper.remove_stop_words,
            typo_options: helper.typo_options,
        })
    }
}

impl<'de> Deserialize<'de> for RegenerateMessageReqPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            pub topic_id: uuid::Uuid,
            pub highlight_options: Option<HighlightOptions>,
            pub search_type: Option<SearchMethod>,
            pub concat_user_messages_query: Option<bool>,
            pub sort_options: Option<SortOptions>,
            pub search_query: Option<String>,
            pub page_size: Option<u64>,
            pub filters: Option<ChunkFilter>,
            pub score_threshold: Option<f32>,
            pub llm_options: Option<LLMOptions>,
            pub user_id: Option<String>,
            pub use_group_search: Option<bool>,
            pub context_options: Option<ContextOptions>,
            pub no_result_message: Option<String>,
            pub only_include_docs_used: Option<bool>,
            #[serde(flatten)]
            other: std::collections::HashMap<String, serde_json::Value>,
            pub use_quote_negated_terms: Option<bool>,
            pub remove_stop_words: Option<bool>,
            pub typo_options: Option<TypoOptions>,
        }

        let mut helper = Helper::deserialize(deserializer)?;

        let (_, extracted_highlight_options) = if !helper.other.is_empty() {
            extract_sort_highlight_options(&mut helper.other)
        } else {
            (None, None)
        };
        let llm_options = extract_llm_options(&mut helper.other);
        let context_options = extract_context_options(&mut helper.other);
        let highlight_options = helper.highlight_options.or(extracted_highlight_options);
        let llm_options = helper.llm_options.or(llm_options);
        let context_options = helper.context_options.or(context_options);

        Ok(RegenerateMessageReqPayload {
            topic_id: helper.topic_id,
            highlight_options,
            sort_options: helper.sort_options,
            search_type: helper.search_type,
            concat_user_messages_query: helper.concat_user_messages_query,
            search_query: helper.search_query,
            page_size: helper.page_size,
            use_group_search: helper.use_group_search,
            filters: helper.filters,
            score_threshold: helper.score_threshold,
            llm_options,
            user_id: helper.user_id,
            context_options,
            no_result_message: helper.no_result_message,
            only_include_docs_used: helper.only_include_docs_used,
            use_quote_negated_terms: helper.use_quote_negated_terms,
            remove_stop_words: helper.remove_stop_words,
            typo_options: helper.typo_options,
        })
    }
}

impl<'de> Deserialize<'de> for EditMessageReqPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            pub topic_id: uuid::Uuid,
            pub message_sort_order: i32,
            pub new_message_content: Option<String>,
            pub audio_input: Option<String>,
            pub highlight_options: Option<HighlightOptions>,
            pub search_type: Option<SearchMethod>,
            pub sort_options: Option<SortOptions>,
            pub image_urls: Option<Vec<String>>,
            pub use_group_search: Option<bool>,
            pub concat_user_messages_query: Option<bool>,
            pub search_query: Option<String>,
            pub page_size: Option<u64>,
            pub filters: Option<ChunkFilter>,
            pub score_threshold: Option<f32>,
            pub llm_options: Option<LLMOptions>,
            pub user_id: Option<String>,
            pub context_options: Option<ContextOptions>,
            pub no_result_message: Option<String>,
            pub only_include_docs_used: Option<bool>,
            #[serde(flatten)]
            other: std::collections::HashMap<String, serde_json::Value>,
            pub use_quote_negated_terms: Option<bool>,
            pub remove_stop_words: Option<bool>,
            pub typo_options: Option<TypoOptions>,
        }

        let mut helper = Helper::deserialize(deserializer)?;

        let (_, extracted_highlight_options) = if !helper.other.is_empty() {
            extract_sort_highlight_options(&mut helper.other)
        } else {
            (None, None)
        };
        let llm_options = extract_llm_options(&mut helper.other);
        let context_options = extract_context_options(&mut helper.other);
        let highlight_options = helper.highlight_options.or(extracted_highlight_options);
        let llm_options = helper.llm_options.or(llm_options);
        let context_options = helper.context_options.or(context_options);

        Ok(EditMessageReqPayload {
            topic_id: helper.topic_id,
            message_sort_order: helper.message_sort_order,
            image_urls: helper.image_urls,
            sort_options: helper.sort_options,
            audio_input: helper.audio_input,
            new_message_content: helper.new_message_content,
            highlight_options,
            search_type: helper.search_type,
            use_group_search: helper.use_group_search,
            concat_user_messages_query: helper.concat_user_messages_query,
            search_query: helper.search_query,
            page_size: helper.page_size,
            filters: helper.filters,
            score_threshold: helper.score_threshold,
            user_id: helper.user_id,
            llm_options,
            context_options,
            no_result_message: helper.no_result_message,
            only_include_docs_used: helper.only_include_docs_used,
            use_quote_negated_terms: helper.use_quote_negated_terms,
            remove_stop_words: helper.remove_stop_words,
            typo_options: helper.typo_options,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq)]
/// MultiQuery allows you to construct a dense vector from multiple queries with a weighted sum. This is useful for when you want to emphasize certain features of the query. This only works with Semantic Search and is not compatible with cross encoder re-ranking or highlights.
pub struct MultiQuery {
    /// Query to embed for the final weighted sum vector.
    pub query: SearchModalities,
    /// Float value which is applies as a multiplier to the query vector when summing.
    pub weight: f32,
}

impl From<(ParsedQuery, f32)> for MultiQuery {
    fn from((query, weight): (ParsedQuery, f32)) -> Self {
        Self {
            query: SearchModalities::Text(query.query),
            weight,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq)]
#[serde(untagged)]
pub enum SearchModalities {
    #[schema(title = "Image")]
    Image {
        image_url: String,
        llm_prompt: Option<String>,
    },
    #[schema(title = "Text")]
    Text(String),
    #[schema(title = "Audio")]
    Audio { audio_base64: String },
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq)]
#[serde(untagged)]
/// Query is the search query. This can be any string. The query will be used to create an embedding vector and/or SPLADE vector which will be used to find the result set.  You can either provide one query, or multiple with weights. Multi-query only works with Semantic Search and is not compatible with cross encoder re-ranking or highlights.
pub enum QueryTypes {
    #[schema(title = "SingleQuery")]
    Single(SearchModalities),
    #[schema(title = "MultiQuery")]
    Multi(Vec<MultiQuery>),
}

impl Default for QueryTypes {
    fn default() -> Self {
        QueryTypes::Single(SearchModalities::Text("".to_string()))
    }
}

impl QueryTypes {
    pub fn to_single_query(&self) -> Result<String, ServiceError> {
        match self {
            QueryTypes::Single(query) => match query {
                SearchModalities::Text(text) => Ok(text.clone()),
                SearchModalities::Image { .. } => Err(ServiceError::BadRequest(
                    "Cannot use Image Query with cross encoder or highlights".to_string(),
                )),
                SearchModalities::Audio { .. } => Err(ServiceError::BadRequest(
                    "Cannot use Audio Query with cross encoder or highlights".to_string(),
                )),
            },
            QueryTypes::Multi(queries) => {
                let mut query_strings = Vec::new();
                for query in queries {
                    if let SearchModalities::Text(text) = &query.query {
                        query_strings.push(text);
                    }
                }
                if query_strings.is_empty() {
                    Err(ServiceError::BadRequest(
                        "Cannot use Image or Audio Query alone in a multi-query with cross encoder or highlights"
                            .to_string(),
                    ))
                } else {
                    Ok(query_strings.into_iter().join(", "))
                }
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Row, Clone, ToSchema)]
pub struct WordDataset {
    #[serde(with = "clickhouse::serde::uuid")]
    pub id: uuid::Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    pub dataset_id: uuid::Uuid,
    pub word: String,
    pub count: i32,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub created_at: OffsetDateTime,
}

impl WordDataset {
    pub fn from_details(word: String, dataset_id: uuid::Uuid, count: i32) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            word,
            dataset_id,
            count,
            created_at: OffsetDateTime::now_utc(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Row, Clone, ToSchema)]
pub struct VideoCrawlMessage {
    pub channel_url: String,
    pub dataset_id: uuid::Uuid,
}

#[derive(Debug, Deserialize, Clone, ToSchema, AsExpression, Display)]
#[diesel(sql_type = Text)]
pub enum CrawlStatus {
    #[display(fmt = "Pending")]
    Pending,
    #[display(fmt = "Processed {} pages", _0)]
    Processing(u32),
    #[display(fmt = "Completed")]
    Completed,
    #[display(fmt = "Failed")]
    Failed,
}

impl Serialize for CrawlStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Use the Display implementation to get the formatted string
        serializer.serialize_str(&self.to_string())
    }
}

impl From<String> for CrawlStatus {
    fn from(s: String) -> Self {
        match s.as_str() {
            "Pending" => CrawlStatus::Pending,
            "Completed" => CrawlStatus::Completed,
            "Failed" => CrawlStatus::Failed,
            _ if s.starts_with("Processed") => {
                let pages: u32 = s
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or(0);
                CrawlStatus::Processing(pages)
            }
            _ => CrawlStatus::Failed,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, Display, PartialEq)]
pub enum CrawlType {
    #[serde(rename = "firecrawl")]
    Firecrawl,
    /// OpenAPI crawl type
    #[serde(rename = "openapi")]
    OpenAPI,
    /// Shopify crawl type
    #[serde(rename = "shopify")]
    Shopify,
    /// Youtube crawl type
    #[serde(rename = "youtube")]
    Youtube,
}

impl From<String> for CrawlType {
    fn from(crawl_type: String) -> Self {
        match crawl_type.as_str() {
            "openapi" => CrawlType::OpenAPI,
            "shopify" => CrawlType::Shopify,
            "youtube" => CrawlType::Youtube,
            "firecrawl" => CrawlType::Firecrawl,
            _ => CrawlType::Firecrawl,
        }
    }
}

impl From<ScrapeOptions> for CrawlType {
    fn from(scrape_options: ScrapeOptions) -> Self {
        match scrape_options {
            ScrapeOptions::OpenApi(_) => CrawlType::OpenAPI,
            ScrapeOptions::Shopify(_) => CrawlType::Shopify,
            ScrapeOptions::Youtube(_) => CrawlType::Youtube,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, Selectable, Clone, ToSchema)]
#[diesel(table_name = crawl_requests)]
pub struct CrawlRequestPG {
    pub id: uuid::Uuid,
    pub url: String,
    pub status: String,
    pub crawl_type: String,
    pub next_crawl_at: chrono::NaiveDateTime,
    pub interval: i32,
    pub crawl_options: serde_json::Value,
    pub scrape_id: uuid::Uuid,
    pub dataset_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct CrawlRequest {
    pub id: uuid::Uuid,
    pub url: String,
    pub status: CrawlStatus,
    pub crawl_type: CrawlType,
    pub next_crawl_at: chrono::NaiveDateTime,
    pub interval: std::time::Duration,
    pub crawl_options: CrawlOptions,
    pub scrape_id: uuid::Uuid,
    pub dataset_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
    pub attempt_number: i32,
}

impl From<CrawlRequestPG> for CrawlRequest {
    fn from(crawl_request: CrawlRequestPG) -> Self {
        Self {
            id: crawl_request.id,
            url: crawl_request.url,
            status: crawl_request.status.into(),
            crawl_type: crawl_request.crawl_type.into(),
            next_crawl_at: crawl_request.next_crawl_at,
            interval: std::time::Duration::from_secs(crawl_request.interval as u64),
            crawl_options: serde_json::from_value(crawl_request.crawl_options).unwrap(),
            scrape_id: crawl_request.scrape_id,
            dataset_id: crawl_request.dataset_id,
            created_at: crawl_request.created_at,
            attempt_number: 0,
        }
    }
}

impl From<CrawlRequest> for CrawlRequestPG {
    fn from(crawl_request: CrawlRequest) -> Self {
        Self {
            id: crawl_request.id,
            url: crawl_request.url,
            status: crawl_request.status.to_string(),
            crawl_type: crawl_request.crawl_type.to_string().to_lowercase(),
            next_crawl_at: crawl_request.next_crawl_at,
            interval: crawl_request.interval.as_secs() as i32,
            crawl_options: serde_json::to_value(crawl_request.crawl_options).unwrap(),
            scrape_id: crawl_request.scrape_id,
            dataset_id: crawl_request.dataset_id,
            created_at: crawl_request.created_at,
        }
    }
}

/// Options for setting up the crawl which will populate the dataset.
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[schema(example=json!({
    "site_url": "https://example.com",
    "interval": "daily",
    "limit": 1000,
    "exclude_paths": ["https://example.com/exclude*"],
    "include_paths": ["https://example.com/include*"],
    "include_tags": ["h1", "p", "a", ".main-content"],
    "exclude_tags": ["#ad", "#footer"],
    "heading_remove_strings": ["Advertisement", "Sponsored"],
    "body_remove_strings": ["Edit on github"],
    "boost_titles": true,
}))]
pub struct CrawlOptions {
    /// The URL to crawl
    pub site_url: Option<String>,
    /// The interval to crawl the site, defaults to daily
    pub interval: Option<CrawlInterval>,
    /// How many pages to crawl, defaults to 1000
    pub limit: Option<i32>,
    /// URL Patterns to exclude from the crawl
    pub exclude_paths: Option<Vec<String>>,
    /// URL Patterns to include in the crawl
    pub include_paths: Option<Vec<String>>,
    /// Specify the HTML tags, classes and ids to include in the response.
    pub include_tags: Option<Vec<String>>,
    /// Specify the HTML tags, classes and ids to exclude from the response.
    pub exclude_tags: Option<Vec<String>>,
    /// Boost titles such that keyword matches in titles are prioritized in search results. Strongly recommended to leave this on. Defaults to true.
    pub boost_titles: Option<bool>,
    /// Option for allowing the crawl to follow links to external websites.
    pub allow_external_links: Option<bool>,
    /// Ignore the website sitemap when crawling, defaults to true.
    pub ignore_sitemap: Option<bool>,
    /// Text strings to remove from headings when creating chunks for each page
    pub heading_remove_strings: Option<Vec<String>>,
    /// Text strings to remove from body when creating chunks for each page
    pub body_remove_strings: Option<Vec<String>>,
    /// Options for including an openapi spec in the crawl
    pub scrape_options: Option<ScrapeOptions>,
    /// Host to call back on the webhook for each successful page scrape
    pub webhook_url: Option<String>,
    /// Metadata to send back with the webhook call for each successful page scrape
    pub webhook_metadata: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug, ToSchema, Clone)]
#[serde(tag = "type")]
/// Options for including an openapi spec or shopify settigns
pub enum ScrapeOptions {
    /// OpenAPI Scrape Options
    #[serde(rename = "openapi")]
    OpenApi(CrawlOpenAPIOptions),
    /// Shopify Scrape Options
    #[serde(rename = "shopify")]
    Shopify(CrawlShopifyOptions),
    /// Youtube Scrape Options
    #[serde(rename = "youtube")]
    Youtube(CrawlYoutubeOptions),
}

#[derive(Serialize, Deserialize, Debug, ToSchema, Clone)]
#[schema(title = "CrawlYoutubeOptions")]
/// Options for Crawling Youtube
pub struct CrawlYoutubeOptions {}
#[derive(Serialize, Deserialize, Debug, ToSchema, Clone)]
#[schema(title = "CrawlShopifyOptions")]
/// Options for Crawling Shopify
pub struct CrawlShopifyOptions {
    /// This option will ingest all variants as individual chunks and place them in groups by product id. Turning this off will only scrape 1 variant per product. default: true
    pub group_variants: Option<bool>,
    pub tag_regexes: Option<Vec<String>>,
}

impl CrawlOptions {
    pub fn merge(&self, other: CrawlOptions) -> CrawlOptions {
        CrawlOptions {
            site_url: self.site_url.clone().or(other.site_url.clone()),
            interval: self.interval.clone().or(other.interval.clone()),
            limit: self.limit.or(other.limit),
            include_tags: self.include_tags.clone().or(other.include_tags.clone()),
            exclude_tags: self.exclude_tags.clone().or(other.exclude_tags.clone()),
            include_paths: self.include_paths.clone().or(other.include_paths.clone()),
            exclude_paths: self.exclude_paths.clone().or(other.exclude_paths.clone()),
            ignore_sitemap: self.ignore_sitemap.or(other.ignore_sitemap),
            boost_titles: self.boost_titles.or(other.boost_titles),
            scrape_options: self.scrape_options.clone(),
            allow_external_links: self.allow_external_links.or(other.allow_external_links),
            heading_remove_strings: self
                .heading_remove_strings
                .clone()
                .or(other.heading_remove_strings.clone()),
            body_remove_strings: self
                .body_remove_strings
                .clone()
                .or(other.body_remove_strings.clone()),
            webhook_url: None,
            webhook_metadata: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FirecrawlCrawlRequest {
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude_paths: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_paths: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ignore_sitemap: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_external_links: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scrape_options: Option<FirecrawlScraperOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub webhook_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub webhook_metadata: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct FirecrawlScraperOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_tags: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude_tags: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_for: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub formats: Option<Vec<String>>,
}

impl From<CrawlOptions> for FirecrawlCrawlRequest {
    fn from(crawl_options: CrawlOptions) -> Self {
        Self {
            url: crawl_options.site_url,
            exclude_paths: crawl_options.exclude_paths,
            include_paths: crawl_options.include_paths,
            ignore_sitemap: crawl_options.ignore_sitemap,
            limit: Some(crawl_options.limit.unwrap_or(1000)),
            allow_external_links: crawl_options.allow_external_links,
            scrape_options: Some(FirecrawlScraperOptions {
                include_tags: crawl_options.include_tags,
                exclude_tags: crawl_options.exclude_tags,
                formats: Some(vec!["rawHtml".to_string()]),
                wait_for: Some(1000),
            }),
            webhook_url: crawl_options.webhook_url,
            webhook_metadata: crawl_options.webhook_metadata,
        }
    }
}

#[cfg(not(feature = "hallucination-detection"))]
pub struct DummyHallucinationScore {
    pub total_score: f64,
    pub detected_hallucinations: Vec<String>,
}
