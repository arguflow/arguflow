use crate::operators::chunk_operator::get_random_chunk_metadatas_query;
use crate::operators::message_operator::models::DatasetAndOrgWithSubAndPlan;
use itertools::Itertools;
use simple_server_timing_header::Timer;
use simsearch::SimSearch;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

#[cfg(not(feature = "hallucination-detection"))]
use crate::data::models::DummyHallucinationScore;
use crate::data::models::{
    self, escape_quotes, ChunkMetadata, ChunkMetadataStringTagSet,
    ChunkMetadataStringTagSetWithHighlightsScore, ChunkMetadataTypes, Dataset,
    DatasetConfiguration, LLMOptions, MultiQuery, QdrantChunkMetadata, QueryTypes,
    RagQueryEventClickhouse, RedisPool, ScoreChunk, SearchMethod, SearchModalities, SuggestType,
};
use crate::diesel::prelude::*;
use crate::get_env;
use crate::handlers::chunk_handler::SearchChunksReqPayload;
use crate::handlers::group_handler::SearchOverGroupsReqPayload;
use crate::handlers::message_handler::{CreateMessageReqPayload, SuggestedQueriesReqPayload};
use crate::operators::clickhouse_operator::ClickHouseEvent;
use crate::operators::parse_operator::convert_html_to_text;
use crate::operators::qdrant_operator::scroll_dataset_points;
use crate::{
    data::models::{Message, Pool, SearchQueryEventClickhouse},
    errors::ServiceError,
};
use actix::Arbiter;
use actix_web::web::Bytes;
use actix_web::{web, HttpResponse};
use crossbeam_channel::unbounded;
use diesel_async::RunQueryDsl;
use futures::StreamExt;
use futures_util::stream;
#[cfg(feature = "hallucination-detection")]
use hallucination_detection::{HallucinationDetector, HallucinationScore};
use openai_dive::v1::models::WhisperEngine;
use openai_dive::v1::resources::audio::{AudioOutputFormat, AudioTranscriptionParametersBuilder};
use openai_dive::v1::resources::chat::{
    ChatCompletionChoice, ChatMessageContentPart, ChatMessageImageContentPart,
    ChatMessageTextContentPart, DeltaChatMessage, ImageUrlType,
};
use openai_dive::v1::resources::shared::{FileUpload, FileUploadBytes};
use openai_dive::v1::{
    api::Client,
    resources::{
        chat::{ChatCompletionParameters, ChatMessage, ChatMessageContent},
        shared::StopToken,
    },
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use ureq::json;

use super::chunk_operator::get_chunk_metadatas_from_point_ids;
use super::clickhouse_operator::{get_latency_from_header, EventQueue};
use super::parse_operator::parse_streaming_completetion;
use super::search_operator::{
    assemble_qdrant_filter, hybrid_search_over_groups, search_chunks_query, search_hybrid_chunks,
    search_over_groups_query, ParsedQuery, ParsedQueryTypes,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatCompletionDTO {
    pub completion_message: Message,
    pub completion_tokens: i32,
}

pub async fn get_topic_messages_query(
    messages_topic_id: uuid::Uuid,
    given_dataset_id: uuid::Uuid,
    pool: &web::Data<Pool>,
) -> Result<Vec<Message>, ServiceError> {
    use crate::data::schema::messages::dsl::*;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let topic_messages = messages
        .filter(topic_id.eq(messages_topic_id))
        .filter(dataset_id.eq(given_dataset_id))
        .filter(deleted.eq(false))
        .order(sort_order.asc())
        .load::<Message>(&mut conn)
        .await
        .map_err(|_db_error| {
            ServiceError::BadRequest("Error getting topic messages".to_string())
        })?;

    Ok(topic_messages)
}

pub async fn get_message_by_id_query(
    message_id: uuid::Uuid,
    given_dataset_id: uuid::Uuid,
    pool: &web::Data<Pool>,
) -> Result<Message, ServiceError> {
    use crate::data::schema::messages::dsl as messages_columns;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let message = messages_columns::messages
        .filter(messages_columns::id.eq(message_id))
        .filter(messages_columns::dataset_id.eq(given_dataset_id))
        .filter(messages_columns::deleted.eq(false))
        .first::<Message>(&mut conn)
        .await
        .map_err(|db_error| {
            log::error!("Error getting message by id {:?}", db_error);
            ServiceError::BadRequest("Error getting message by id".to_string())
        })?;

    Ok(message)
}

pub async fn create_messages_query(
    new_messages: Vec<Message>,
    pool: &web::Data<Pool>,
) -> Result<(), ServiceError> {
    use crate::data::schema::messages::dsl::messages;

    let mut conn = pool
        .get()
        .await
        .map_err(|_| ServiceError::BadRequest("Could not get database connection".to_string()))?;

    diesel::insert_into(messages)
        .values(&new_messages)
        .execute(&mut conn)
        .await
        .map_err(|_db_error| {
            ServiceError::BadRequest("Error creating message, try again".to_string())
        })?;

    Ok(())
}

const STRUCTURE_SYSTEM_PROMPT: &str = r#"
Before you start generating respond with the documents that you plan to use to generate your response, YOU MUST INCLUDE AT LEAST 1.
YOU MUST DO THIS BEFORE YOU CONTINUE TO GENERATE A RESPONSE.
After responding with the documents, YOU MUST RESPOND TO THE USERS PROMPT.

```
Example:
User: 
Here's my prompt: what about for spreadsheets \n\n Use the following retrieved documents to respond briefly and accurately: {"doc": 1, "text": "chunk text..", "link": "chunk link.." }\n\n{"doc": 2, "text": "chunk text..", "link": "chunk link.." }... etc

Assistant:
documents: [1,2]
...continue with model response
```

After you have done these things now follow:
"#;

pub async fn create_generic_system_message(
    system_prompt: String,
    messages_topic_id: uuid::Uuid,
    only_include_used_docs: Option<bool>,
    dataset_id: uuid::Uuid,
    pool: &web::Data<Pool>,
) -> Result<Message, ServiceError> {
    let topic =
        crate::operators::topic_operator::get_topic_query(messages_topic_id, dataset_id, pool)
            .await?;

    let system_prompt = if only_include_used_docs.unwrap_or(false) {
        format!("{}\n\n{}", STRUCTURE_SYSTEM_PROMPT, system_prompt)
    } else {
        system_prompt
    };

    let system_message = Message::from_details(
        system_prompt,
        topic.id,
        0,
        "system".into(),
        Some(0),
        Some(0),
        dataset_id,
        uuid::Uuid::new_v4(),
    );

    Ok(system_message)
}

pub async fn create_topic_message_query(
    config: &DatasetConfiguration,
    previous_messages: Vec<Message>,
    new_message: Message,
    only_include_used_docs: Option<bool>,
    dataset_id: uuid::Uuid,
    pool: &web::Data<Pool>,
) -> Result<Vec<Message>, ServiceError> {
    let mut ret_messages = previous_messages.clone();
    let mut new_message_copy = new_message.clone();
    let mut previous_messages_len = previous_messages.len();

    if previous_messages.is_empty() {
        let system_message = create_generic_system_message(
            config.SYSTEM_PROMPT.clone(),
            new_message.topic_id,
            only_include_used_docs,
            dataset_id,
            pool,
        )
        .await?;
        ret_messages.extend(vec![system_message.clone()]);
        create_messages_query(vec![system_message], pool).await?;
        previous_messages_len = 1;
    }

    new_message_copy.sort_order = previous_messages_len as i32;

    create_messages_query(vec![new_message_copy.clone()], pool).await?;
    ret_messages.push(new_message_copy);

    Ok(ret_messages)
}

pub async fn get_message_by_sort_for_topic_query(
    message_topic_id: uuid::Uuid,
    given_dataset_id: uuid::Uuid,
    message_sort_order: i32,
    pool: &web::Data<Pool>,
) -> Result<Message, ServiceError> {
    use crate::data::schema::messages::dsl::*;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    messages
        .filter(deleted.eq(false))
        .filter(topic_id.eq(message_topic_id))
        .filter(sort_order.eq(message_sort_order))
        .filter(dataset_id.eq(given_dataset_id))
        .first::<Message>(&mut conn)
        .await
        .map_err(|_db_error| {
            ServiceError::BadRequest(
                "This message does not exist for the authenticated user".to_string(),
            )
        })
}

pub async fn get_messages_for_topic_query(
    message_topic_id: uuid::Uuid,
    given_dataset_id: uuid::Uuid,
    pool: &web::Data<Pool>,
) -> Result<Vec<Message>, ServiceError> {
    use crate::data::schema::messages::dsl::*;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    messages
        .filter(topic_id.eq(message_topic_id))
        .filter(deleted.eq(false))
        .filter(dataset_id.eq(given_dataset_id))
        .order_by(sort_order.asc())
        .load::<Message>(&mut conn)
        .await
        .map_err(|_db_error| {
            ServiceError::BadRequest(
                "This topic does not exist for the authenticated user".to_string(),
            )
        })
}

pub async fn delete_message_query(
    given_message_id: uuid::Uuid,
    given_topic_id: uuid::Uuid,
    given_dataset_id: uuid::Uuid,
    pool: &web::Data<Pool>,
) -> Result<(), ServiceError> {
    use crate::data::schema::messages::dsl::*;

    let mut conn = pool.get().await.map_err(|_e| {
        ServiceError::InternalServerError("Failed to get postgres connection".to_string())
    })?;

    let target_message: Message = messages
        .find(given_message_id)
        .first::<Message>(&mut conn)
        .await
        .map_err(|_db_error| ServiceError::BadRequest("Error finding message".to_string()))?;

    diesel::update(
        messages
            .filter(topic_id.eq(given_topic_id))
            .filter(dataset_id.eq(given_dataset_id))
            .filter(sort_order.ge(target_message.sort_order)),
    )
    .set(deleted.eq(true))
    .execute(&mut conn)
    .await
    .map_err(|_| ServiceError::BadRequest("Error deleting message".to_string()))?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn get_rag_chunks_query(
    create_message_req_payload: CreateMessageReqPayload,
    dataset_config: DatasetConfiguration,
    dataset: Dataset,
    user_message_query: String,
    chosen_model: String,
    client: &Client,
    pool: web::Data<Pool>,
    redis_pool: web::Data<RedisPool>,
    event_queue: web::Data<EventQueue>,
) -> Result<(SearchQueryEventClickhouse, Vec<ScoreChunk>), actix_web::Error> {
    let mut query =
        if let Some(create_message_query) = create_message_req_payload.search_query.clone() {
            create_message_query
        } else {
            user_message_query
        };

    let use_message_to_query_prompt = dataset_config.USE_MESSAGE_TO_QUERY_PROMPT;
    if create_message_req_payload.search_query.is_none() && use_message_to_query_prompt {
        let message_to_query_prompt = dataset_config.MESSAGE_TO_QUERY_PROMPT.clone();
        let mut gen_inference_msgs = vec![ChatMessage::User {
            content: ChatMessageContent::Text(format!("{}\n{}", message_to_query_prompt, query)),
            name: None,
        }];

        if let Some(ref image_urls) = create_message_req_payload.image_urls {
            if !image_urls.is_empty() {
                gen_inference_msgs.push(ChatMessage::User {
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
                            .collect::<Vec<_>>(),
                    ),
                });
                gen_inference_msgs.push(ChatMessage::User {
                    name: None,
                    content: ChatMessageContent::Text(
                        "These are the images that the user provided with their query. Use them to create your search query".to_string(),
                    ),
                });
            }
        }

        let gen_inference_parameters = ChatCompletionParameters {
            model: chosen_model.clone(),
            messages: gen_inference_msgs,
            stream: Some(false),
            temperature: dataset_config.TEMPERATURE.map(|temp| temp as f32),
            frequency_penalty: Some(dataset_config.FREQUENCY_PENALTY.unwrap_or(0.8) as f32),
            presence_penalty: Some(dataset_config.PRESENCE_PENALTY.unwrap_or(0.8) as f32),
            stop: dataset_config.STOP_TOKENS.clone().map(StopToken::Array),
            top_p: None,
            n: None,
            max_completion_tokens: dataset_config.MAX_TOKENS.map(|max| max as u32),
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

        let search_query_from_message_to_query_prompt =
            match client.chat().create(gen_inference_parameters).await {
                Ok(query) => query,
                Err(err) => {
                    log::error!(
                        "Error getting LLM completion for message to query prompt {:?}",
                        err
                    );
                    return Err(actix_web::error::ErrorInternalServerError(
                        "Error getting LLM completion for message to query prompt",
                    ));
                }
            };

        query = match &search_query_from_message_to_query_prompt
            .choices
            .get(0)
            .expect("No response for LLM completion")
            .message
        {
            ChatMessage::User {
                content: ChatMessageContent::Text(content),
                ..
            }
            | ChatMessage::System {
                content: ChatMessageContent::Text(content),
                ..
            }
            | ChatMessage::Assistant {
                content: Some(ChatMessageContent::Text(content)),
                ..
            } => content.clone(),
            _ => query,
        };
    }

    let n_retrievals_to_include = dataset_config.N_RETRIEVALS_TO_INCLUDE;
    let search_type = create_message_req_payload
        .search_type
        .unwrap_or(SearchMethod::Hybrid);

    let query_type =
        if let Some(ref image_urls) = create_message_req_payload.image_urls.and_then(|x| {
            if x.is_empty() {
                None
            } else {
                Some(x)
            }
        }) {
            let image_queries = image_urls.iter().map(|url| MultiQuery {
                query: SearchModalities::Image {
                    image_url: url.clone(),
                    llm_prompt: None,
                },
                weight: 0.5 / image_urls.len() as f32,
            });

            QueryTypes::Multi(
                vec![MultiQuery {
                    query: SearchModalities::Text(query.clone()),
                    weight: 0.5,
                }]
                .into_iter()
                .chain(image_queries)
                .collect(),
            )
        } else {
            QueryTypes::Single(SearchModalities::Text(query.clone()))
        };

    if create_message_req_payload
        .use_group_search
        .is_some_and(|x| x)
    {
        let search_groups_data = SearchOverGroupsReqPayload {
            search_type: search_type.clone(),
            query: query_type.clone(),
            score_threshold: create_message_req_payload.score_threshold,
            page_size: Some(
                create_message_req_payload
                    .page_size
                    .unwrap_or(n_retrievals_to_include.try_into().unwrap_or(8)),
            ),
            sort_options: create_message_req_payload.sort_options,
            highlight_options: create_message_req_payload.highlight_options,
            filters: create_message_req_payload.filters,
            group_size: Some(1),
            use_quote_negated_terms: create_message_req_payload.use_quote_negated_terms,
            remove_stop_words: create_message_req_payload.remove_stop_words,
            typo_options: create_message_req_payload.typo_options,
            ..Default::default()
        };

        let parsed_query = ParsedQuery {
            query: query.clone(),
            quote_words: None,
            negated_words: None,
        };

        let mut search_timer = Timer::new();

        let result_groups = match search_type {
            SearchMethod::Hybrid => {
                hybrid_search_over_groups(
                    search_groups_data.clone(),
                    parsed_query,
                    pool.clone(),
                    redis_pool,
                    dataset.clone(),
                    &dataset_config,
                    &mut search_timer,
                )
                .await?
            }
            _ => {
                search_over_groups_query(
                    search_groups_data.clone(),
                    ParsedQueryTypes::Single(parsed_query),
                    pool.clone(),
                    redis_pool,
                    dataset.clone(),
                    &dataset_config,
                    &mut search_timer,
                )
                .await?
            }
        };

        let clickhouse_search_event = SearchQueryEventClickhouse {
            request_params: serde_json::to_string(&search_groups_data.clone()).unwrap_or_default(),
            id: uuid::Uuid::new_v4(),
            search_type: "rag_groups".to_string(),
            query: query.clone(),
            dataset_id: dataset.id,
            top_score: result_groups
                .group_chunks
                .get(0)
                .map(|x| x.metadata.get(0).map(|y| y.score as f32).unwrap_or(0.0))
                .unwrap_or(0.0),
            latency: get_latency_from_header(search_timer.header_value()),
            results: result_groups
                .group_chunks
                .clone()
                .into_iter()
                .map(|x| {
                    let mut json = serde_json::to_value(&x).unwrap_or_default();
                    escape_quotes(&mut json);
                    json.to_string()
                })
                .collect(),
            created_at: time::OffsetDateTime::now_utc(),
            query_rating: String::from(""),
            user_id: create_message_req_payload
                .user_id
                .clone()
                .unwrap_or_default(),
        };
        if !dataset_config.DISABLE_ANALYTICS {
            event_queue
                .send(ClickHouseEvent::SearchQueryEvent(
                    clickhouse_search_event.clone(),
                ))
                .await;
        }
        Ok((
            clickhouse_search_event,
            result_groups
                .group_chunks
                .into_iter()
                .flat_map(|group_score_chunk| {
                    group_score_chunk
                        .metadata
                        .into_iter()
                        .map(ScoreChunk::from)
                        .collect::<Vec<ScoreChunk>>()
                })
                .collect::<Vec<ScoreChunk>>(),
        ))
    } else {
        let search_chunk_data = SearchChunksReqPayload {
            search_type: search_type.clone(),
            query: query_type.clone(),
            score_threshold: create_message_req_payload.score_threshold,
            sort_options: create_message_req_payload.sort_options,
            page_size: Some(
                create_message_req_payload
                    .page_size
                    .unwrap_or(n_retrievals_to_include.try_into().unwrap_or(8)),
            ),
            highlight_options: create_message_req_payload.highlight_options,
            filters: create_message_req_payload.filters,
            use_quote_negated_terms: create_message_req_payload.use_quote_negated_terms,
            remove_stop_words: create_message_req_payload.remove_stop_words,
            typo_options: create_message_req_payload.typo_options,
            ..Default::default()
        };
        let parsed_query = ParsedQuery {
            query: query.clone(),
            quote_words: None,
            negated_words: None,
        };
        let mut search_timer = Timer::new();

        let result_chunks = match search_type {
            SearchMethod::Hybrid => {
                search_hybrid_chunks(
                    search_chunk_data.clone(),
                    parsed_query,
                    pool.clone(),
                    redis_pool,
                    dataset.clone(),
                    &dataset_config,
                    &mut search_timer,
                )
                .await?
            }
            _ => {
                search_chunks_query(
                    search_chunk_data.clone(),
                    ParsedQueryTypes::Single(parsed_query),
                    pool.clone(),
                    redis_pool,
                    dataset.clone(),
                    &dataset_config,
                    &mut search_timer,
                )
                .await?
            }
        };

        let clickhouse_search_event = SearchQueryEventClickhouse {
            request_params: serde_json::to_string(&search_chunk_data.clone()).unwrap_or_default(),
            id: uuid::Uuid::new_v4(),
            search_type: "rag_chunks".to_string(),
            query: query.clone(),
            dataset_id: dataset.id,
            top_score: result_chunks
                .score_chunks
                .get(0)
                .map(|x| x.score as f32)
                .unwrap_or(0.0),
            latency: get_latency_from_header(search_timer.header_value()),
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
            created_at: time::OffsetDateTime::now_utc(),
            query_rating: String::from(""),
            user_id: create_message_req_payload
                .user_id
                .clone()
                .unwrap_or_default(),
        };
        if !dataset_config.DISABLE_ANALYTICS {
            event_queue
                .send(ClickHouseEvent::SearchQueryEvent(
                    clickhouse_search_event.clone(),
                ))
                .await;
        }
        Ok((
            clickhouse_search_event,
            result_chunks
                .score_chunks
                .into_iter()
                .map(ScoreChunk::from)
                .collect::<Vec<ScoreChunk>>(),
        ))
    }
}

pub fn clean_markdown(markdown_text: &str) -> String {
    let mut text = markdown_text.to_string();

    let patterns = [
        // Code blocks (both ``` and indented)
        (r"```[\s\S]*?```", ""),
        (r"(?m)^( {4,}|\t+)[^\n]+", ""),
        // Headers
        (r"(?m)^#{1,6}\s+", ""),
        // Emphasis (bold, italic)
        (r"\*\*(.+?)\*\*", "$1"), // Bold
        (r"__(.+?)__", "$1"),     // Bold
        (r"\*(.+?)\*", "$1"),     // Italic
        (r"_(.+?)_", "$1"),       // Italic
        // Inline code
        (r"`([^`]+)`", "$1"),
        // Blockquotes
        (r"(?m)^\s*>\s+", ""),
        // Horizontal rules
        (r"\n[\*\-_]{3,}\n", "\n"),
        // Lists
        (r"(?m)^\s*[\*\-+]\s+", ""), // Unordered lists
        (r"(?m)^\s*\d+\.\s+", ""),   // Ordered lists
        // Links and images
        (r"\[([^\]]+)\]\([^\)]+\)", "$1"), // [text](url)
        (r"\[([^\]]+)\]\[[^\]]*\]", "$1"), // [text][reference]
        (r"!\[([^\]]*)\]\([^\)]+\)", ""),  // Images
        // Reference-style links
        (r"(?m)^\s*\[[^\]]+\]:\s+[^\s]+\s*$", ""),
        // Clean up whitespace
        (r"\n\s*\n", "\n\n"),
    ];

    // Apply all patterns
    for (pattern, replacement) in patterns.iter() {
        if let Ok(regex) = Regex::new(pattern) {
            text = regex.replace_all(&text, *replacement).to_string();
        }
    }

    // Final cleanup
    text.trim().to_string()
}

#[allow(clippy::too_many_arguments)]

pub async fn stream_response(
    messages: Vec<models::Message>,
    topic_id: uuid::Uuid,
    dataset: Dataset,
    pool: web::Data<Pool>,
    event_queue: web::Data<EventQueue>,
    redis_pool: web::Data<RedisPool>,
    dataset_config: DatasetConfiguration,
    create_message_req_payload: CreateMessageReqPayload,
    #[cfg(feature = "hallucination-detection")] hallucination_detector: web::Data<
        HallucinationDetector,
    >,
) -> Result<HttpResponse, actix_web::Error> {
    let user_message_query = match create_message_req_payload.concat_user_messages_query {
        Some(true) => messages
            .iter()
            .filter(|message| message.role == "user")
            .map(|message| message.content.clone())
            .collect::<Vec<String>>()
            .join("\n\n"),
        _ => match messages.last() {
            Some(message) => message.clone().content,
            None => {
                return Err(ServiceError::BadRequest(
                    "No messages found for the topic".to_string(),
                )
                .into());
            }
        },
    };

    let openai_messages: Vec<ChatMessage> = messages
        .iter()
        .map(|message| ChatMessage::from(message.clone()))
        .collect();

    let base_url = dataset_config.LLM_BASE_URL.clone();

    let llm_api_key = get_llm_api_key(&dataset_config);

    let client = Client {
        headers: None,
        project: None,
        api_key: llm_api_key,
        http_client: reqwest::Client::new(),
        base_url,
        organization: None,
    };

    let next_message_order = move || {
        let messages_len = messages.len();
        if messages_len == 0 {
            return 2;
        }
        messages_len
    };

    let rag_prompt = dataset_config.RAG_PROMPT.clone();
    let chosen_model = dataset_config.LLM_DEFAULT_MODEL.clone();

    let (search_event, score_chunks) = get_rag_chunks_query(
        create_message_req_payload.clone(),
        dataset_config.clone(),
        dataset.clone(),
        user_message_query.clone(),
        chosen_model.clone(),
        &client,
        pool.clone(),
        redis_pool.clone(),
        event_queue.clone(),
    )
    .await?;

    if score_chunks.is_empty() {
        let response_stream = stream::iter(vec![Ok::<actix_web::web::Bytes, actix_web::Error>(
            Bytes::from(format!(
                "[]||{}",
                create_message_req_payload.no_result_message.unwrap_or(
                    "I was not able to find any relevant information to answer your query."
                        .to_string(),
                )
            )),
        )]);
        return Ok(HttpResponse::Ok()
            .insert_header(("TR-QueryID", search_event.id.to_string()))
            .streaming(response_stream));
    }

    let rag_content = score_chunks
        .iter()
        .enumerate()
        .map(|(idx, score_chunk)| {
            json!({
                "doc": idx + 1,
                "text": convert_html_to_text(&(ChunkMetadata::from(score_chunk.chunk.clone()).chunk_html.clone().unwrap_or_default())),
                "link": ChunkMetadata::from(score_chunk.chunk.clone()).link.clone().unwrap_or_default()
            })
            .to_string()
        })
        .collect::<Vec<String>>()
        .join("\n\n");

    let user_message = match &openai_messages
        .last()
        .expect("There needs to be at least 1 prior message")
    {
        ChatMessage::User {
            content: ChatMessageContent::Text(text),
            ..
        }
        | ChatMessage::System {
            content: ChatMessageContent::Text(text),
            ..
        }
        | ChatMessage::Assistant {
            content: Some(ChatMessageContent::Text(text)),
            ..
        } => text.clone(),
        _ => "".to_string(),
    };

    let last_message = ChatMessageContent::Text(format!(
        "Here's my prompt: {} \n\n {} {}",
        user_message.clone(),
        rag_prompt,
        rag_content,
    ));

    let images: Vec<String> = score_chunks
        .iter()
        .filter_map(|score_chunk| {
            ChunkMetadata::from(score_chunk.chunk.clone())
                .image_urls
                .clone()
        })
        .flat_map(|image_urls| {
            image_urls
                .iter()
                .filter_map(|image| image.clone())
                .collect::<Vec<_>>()
        })
        .collect();

    // replace the last message with the last message with evidence
    let mut open_ai_messages: Vec<ChatMessage> = openai_messages
        .clone()
        .into_iter()
        .enumerate()
        .map(|(index, message)| {
            if index == openai_messages.len() - 1 {
                match message {
                    ChatMessage::Assistant { name, .. } => ChatMessage::Assistant {
                        content: Some(last_message.clone()),
                        name,
                        tool_calls: None,
                        refusal: None,
                    },
                    ChatMessage::System { name, .. } => ChatMessage::System {
                        content: last_message.clone(),
                        name,
                    },
                    ChatMessage::User { name, .. } => ChatMessage::User {
                        content: last_message.clone(),
                        name,
                    },
                    _ => message,
                }
            } else {
                message
            }
        })
        .collect();

    if let Some(image_urls) = create_message_req_payload.image_urls.clone() {
        if !image_urls.is_empty() {
            open_ai_messages.push(ChatMessage::User {
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

    if !images.is_empty() {
        if let Some(LLMOptions {
            image_config: Some(ref image_config),
            ..
        }) = create_message_req_payload.llm_options
        {
            if image_config.use_images.unwrap_or(false) {
                open_ai_messages.push(ChatMessage::User {
                    name: None,
                    content: ChatMessageContent::ContentPart(
                        images
                            .iter()
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
                })
            }
        }
    }

    let mut parameters = ChatCompletionParameters {
        model: chosen_model,
        messages: open_ai_messages,
        ..Default::default()
    };

    if let Some(llm_options) = create_message_req_payload.llm_options.clone() {
        parameters.stream = llm_options.stream_response;
        parameters.temperature = dataset_config
            .TEMPERATURE
            .map(|x| x as f32)
            .or(llm_options.temperature);
        parameters.frequency_penalty = dataset_config
            .FREQUENCY_PENALTY
            .map(|x| x as f32)
            .or(llm_options.frequency_penalty);
        parameters.presence_penalty = dataset_config
            .PRESENCE_PENALTY
            .map(|x| x as f32)
            .or(llm_options.presence_penalty);
        parameters.max_completion_tokens = dataset_config
            .MAX_TOKENS
            .map(|x| x as u32)
            .or(llm_options.max_tokens);
        parameters.stop = dataset_config
            .STOP_TOKENS
            .clone()
            .map(StopToken::Array)
            .or(llm_options.stop_tokens.map(StopToken::Array));
    }

    let query_id = uuid::Uuid::new_v4();

    if create_message_req_payload
        .llm_options
        .as_ref()
        .is_some_and(|llm_options| !llm_options.stream_response.unwrap_or(true))
    {
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

        let completion_content = match &assistant_completion
            .choices
            .get(0)
            .map(|chat_completion_choice| chat_completion_choice.message.clone())
        {
            Some(ChatMessage::Assistant {
                content: Some(ChatMessageContent::Text(text)),
                ..
            }) => {
                let parsed: serde_json::Value = serde_json::from_str(text)
                    .map_err(|_| ServiceError::BadRequest("Invalid JSON response".to_string()))?;

                let used_docs = parsed["documents"].as_array().ok_or_else(|| {
                    ServiceError::BadRequest("Missing documents array".to_string())
                })?;

                let rag_message = parsed["message"]
                    .as_str()
                    .ok_or_else(|| ServiceError::BadRequest("Missing message".to_string()))?;

                // Filter chunk_metadatas to only include used documents
                let filtered_chunks: Vec<_> = used_docs
                    .iter()
                    .filter_map(|doc_idx| {
                        doc_idx
                            .as_u64()
                            .and_then(|idx| score_chunks.get(idx as usize - 1))
                            .cloned()
                    })
                    .collect();

                (rag_message.to_string(), filtered_chunks)
            }
            _ => {
                return Err(ServiceError::BadRequest("Invalid response format".to_string()).into())
            }
        };

        let (response_text, filtered_chunks) = completion_content;

        let filtered_chunks_stringified = serde_json::to_string(&filtered_chunks)
            .expect("Failed to serialize filtered citation chunks");

        let final_response = if create_message_req_payload
            .llm_options
            .as_ref()
            .map(|x| x.completion_first)
            .unwrap_or(Some(false))
            .unwrap_or(false)
        {
            format!(
                "{}||{}",
                response_text,
                filtered_chunks_stringified.replace("||", "")
            )
        } else {
            format!(
                "{}||{}",
                filtered_chunks_stringified.replace("||", ""),
                response_text
            )
        };

        let new_message = models::Message::from_details(
            final_response.clone(),
            topic_id,
            next_message_order()
                .try_into()
                .expect("usize to i32 conversion should always succeed"),
            "assistant".to_string(),
            assistant_completion
                .usage
                .as_ref()
                .map(|usg| usg.prompt_tokens as i32),
            assistant_completion
                .usage
                .map(|usg| usg.completion_tokens.unwrap_or(0) as i32),
            dataset.id,
            query_id,
        );

        #[cfg(feature = "hallucination-detection")]
        let score = {
            let docs = filtered_chunks
                .iter()
                .map(|score_chunk| {
                    ChunkMetadata::from(score_chunk.chunk.clone())
                        .chunk_html
                        .clone()
                        .unwrap_or_default()
                })
                .collect::<Vec<String>>();
            hallucination_detector
                .detect_hallucinations(&clean_markdown(&response_text), &docs)
                .await
                .map_err(|err| {
                    ServiceError::BadRequest(format!("Failed to detect hallucinations: {}", err))
                })?
        };

        #[cfg(not(feature = "hallucination-detection"))]
        let score = DummyHallucinationScore {
            total_score: 0.0,
            detected_hallucinations: vec![],
        };

        let filtered_chunks_data = filtered_chunks
            .iter()
            .map(|x| {
                let mut json = serde_json::to_value(x).unwrap_or_default();
                escape_quotes(&mut json);
                json.to_string()
            })
            .collect::<Vec<String>>();

        let clickhouse_rag_event = RagQueryEventClickhouse {
            id: query_id,
            created_at: time::OffsetDateTime::now_utc(),
            dataset_id: dataset.id,
            search_id: search_event.id,
            top_score: search_event.top_score,
            results: vec![],
            json_results: filtered_chunks_data,
            user_message: user_message_query.clone(),
            query_rating: String::new(),
            rag_type: "all_chunks".to_string(),
            llm_response: response_text.clone(),
            user_id: create_message_req_payload
                .user_id
                .clone()
                .unwrap_or_default(),
            hallucination_score: score.total_score,
            detected_hallucinations: score.detected_hallucinations,
        };

        if !dataset_config.DISABLE_ANALYTICS {
            event_queue
                .send(ClickHouseEvent::RagQueryEvent(clickhouse_rag_event.clone()))
                .await;
        }
        create_messages_query(vec![new_message], &pool).await?;
        if create_message_req_payload.audio_input.is_some() {
            return Ok(HttpResponse::Ok()
                .insert_header((
                    "X-TR-Query",
                    user_message
                        .to_string()
                        .replace(|c: char| c.is_ascii_control(), ""),
                ))
                .insert_header(("TR-QueryID", query_id.to_string().replace("\n", "")))
                .json(final_response));
        } else {
            return Ok(HttpResponse::Ok()
                .insert_header(("TR-QueryID", query_id.to_string().replace("\n", "")))
                .json(final_response));
        }
    }

    let (s, r) = unbounded::<String>();
    let stream = client.chat().create_stream(parameters).await.unwrap();

    let completion_first = create_message_req_payload
        .llm_options
        .as_ref()
        .map(|x| x.completion_first)
        .unwrap_or(Some(false))
        .unwrap_or(false);

    let query_id_arb = query_id;

    Arbiter::new().spawn(async move {
        let chunk_v: Vec<String> = r.iter().collect();
        let completion = chunk_v.join("");

        let mut split_completion = completion.split("||");

        #[allow(unused_variables)]
        let (response, chunks) = if completion_first {
            let response = split_completion.next().unwrap_or_default().to_string();
            let chunk_data: Vec<ChunkMetadataStringTagSet> =
                serde_json::from_str(split_completion.next().unwrap_or_default())
                    .unwrap_or_default();

            (response, chunk_data)
        } else {
            let chunk_data: Vec<ChunkMetadataStringTagSet> =
                serde_json::from_str(split_completion.next().unwrap_or_default())
                    .unwrap_or_default();

            let response = split_completion.next().unwrap_or_default().to_string();

            (response, chunk_data)
        };

        let chunk_data: Vec<String> = chunks
            .iter()
            .map(|x| {
                let mut json = serde_json::to_value(x).unwrap_or_default();
                escape_quotes(&mut json);
                json.to_string()
            })
            .collect();

        let new_message = models::Message::from_details(
            completion.clone(),
            topic_id,
            next_message_order().try_into().unwrap(),
            "assistant".to_string(),
            None,
            Some(chunk_v.len().try_into().unwrap()),
            dataset.id,
            query_id_arb,
        );

        if !dataset_config.DISABLE_ANALYTICS {
            #[cfg(feature = "hallucination-detection")]
            let score = {
                let docs = chunks
                    .iter()
                    .map(|x| x.chunk_html.clone().unwrap_or_default())
                    .collect::<Vec<String>>();

                hallucination_detector
                    .detect_hallucinations(&clean_markdown(&response), &docs)
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
                id: query_id_arb,
                created_at: time::OffsetDateTime::now_utc(),
                search_id: search_event.id,
                top_score: search_event.top_score,
                dataset_id: dataset.id,
                results: vec![],
                json_results: chunk_data,
                user_message: user_message_query.clone(),
                query_rating: String::new(),
                rag_type: "all_chunks".to_string(),
                llm_response: completion.clone(),
                user_id: create_message_req_payload
                    .user_id
                    .clone()
                    .unwrap_or_default(),
                hallucination_score: score.total_score,
                detected_hallucinations: score.detected_hallucinations,
            };

            event_queue
                .send(ClickHouseEvent::RagQueryEvent(clickhouse_rag_event.clone()))
                .await;
        }
        let _ = create_messages_query(vec![new_message], &pool).await;
    });

    let chat_completion_timeout = std::env::var("CHAT_COMPLETION_TIMEOUT_SECS")
        .unwrap_or("60".to_string())
        .parse::<u64>()
        .unwrap_or(60);

    let state = Arc::new(AtomicU16::new(0));
    let documents = if create_message_req_payload
        .only_include_docs_used
        .unwrap_or(false)
    {
        Arc::new(Mutex::new(vec![]))
    } else {
        Arc::new(Mutex::new((0..score_chunks.len() as u32).collect()))
    };
    let started_parsing_completion = AtomicBool::new(false);
    let mut bail_on_parsing = AtomicBool::new(false);

    let completion_stream = stream
        .take_until(tokio::time::sleep(std::time::Duration::from_secs(chat_completion_timeout)))
        .map(move |response| -> Result<Bytes, actix_web::Error> {
        if let Ok(response) = response {
            let chat_content = response
                .choices
                .get(0)
                .map(|choice| {
                    if choice.finish_reason.is_some() {
                        let docs = documents.lock().unwrap();
                        if !docs.is_empty() && completion_first {
                            let filtered_chunks = score_chunks.iter().enumerate()
                                    .filter_map(|(idx, score_chunk)| {
                                        if docs.contains(&(idx as u32)) {
                                            Some(ChunkMetadataStringTagSetWithHighlightsScore::from(score_chunk.clone()))
                                        } else {
                                            None
                                        }
                                    })
                                    .collect::<Vec<ChunkMetadataStringTagSetWithHighlightsScore>>();
                                Some(format!("||{}", serde_json::to_string(&filtered_chunks).unwrap_or_default().replace("||", "")))
                        } else {
                            Some("".to_string())
                        }
                    } else {
                        match &choice.delta {
                            DeltaChatMessage::Assistant {
                                content: Some(ChatMessageContent::Text(text)),
                                ..
                            }
                            | DeltaChatMessage::Untagged {
                                content: Some(ChatMessageContent::Text(text)),
                                ..
                            } => {
                                if create_message_req_payload
                                    .only_include_docs_used
                                    .unwrap_or(false) {
                                        let bailed_on_iter = bail_on_parsing.get_mut();
                                        let (text, docs) = if !*bailed_on_iter {
                                            let (parsed_text, docs, bail) = parse_streaming_completetion(text, state.clone(), documents.clone());
                                            if bail {
                                                log::info!("Bailing {:?} {:?}", &text, &parsed_text);
                                                *bailed_on_iter = true;
                                                documents.lock().unwrap().extend(0..score_chunks.len() as u32);
                                                (Some(text.clone()), Some((0..score_chunks.len() as u32).collect()))
                                            } else {
                                                (parsed_text, docs)
                                            }
                                        } else {
                                            log::info!("Already bailed");
                                            (Some(text.clone()), None)
                                        };

                                        if let Some(docs) = docs {
                                            if !completion_first {
                                                let filtered_chunks = score_chunks.iter().enumerate().filter_map(|(idx, score_chunk)| {
                                                    if docs.contains(&(idx as u32)) {
                                                        Some(ChunkMetadataStringTagSetWithHighlightsScore::from(score_chunk.clone()))
                                                    } else {
                                                        None
                                                    }
                                                }).collect::<Vec<ChunkMetadataStringTagSetWithHighlightsScore>>();
                                                if *bailed_on_iter {
                                                    return Some(format!("{}||{}", serde_json::to_string(&filtered_chunks).unwrap_or_default().replace("||", ""), text.unwrap_or("".to_string())));
                                                } else {
                                                    return Some(format!("{}||", serde_json::to_string(&filtered_chunks).unwrap_or_default().replace("||", "")));
                                                }
                                            }
                                        }
                                        text.clone()
                                    } else if !completion_first && !started_parsing_completion.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |_| Some(true)).unwrap_or(true) {
                                        let returned_chunks = score_chunks.iter().map(|score_chunk| {
                                            ChunkMetadataStringTagSetWithHighlightsScore::from(score_chunk.clone())
                                        }).collect::<Vec<ChunkMetadataStringTagSetWithHighlightsScore>>();
                                        return Some(format!("{}||", serde_json::to_string(&returned_chunks).unwrap_or_default().replace("||", "")));
                                    } else {
                                        Some(text.clone())
                                    }
                            },
                            _ => {
                                log::error!("Delta of first choice did not have text or was either Tool or Function {:?}", choice);
                                None
                            },
                        }
                    }
                })
                .unwrap_or(None);

            if let Some(message) = chat_content.clone() {
                s.send(message).unwrap();
            }
            return Ok(Bytes::from(chat_content.unwrap_or("".to_string())));
        }
        Err(ServiceError::InternalServerError(format!(
            "Model Response Error. Please try again later. {:?}",
            response
        ))
        .into())
    });

    if create_message_req_payload.audio_input.is_some() {
        return Ok(HttpResponse::Ok()
            .insert_header((
                "X-TR-Query",
                user_message
                    .to_string()
                    .replace(|c: char| c.is_ascii_control(), ""),
            ))
            .insert_header(("TR-QueryID", query_id.to_string().replace("\n", "")))
            .streaming(completion_stream));
    } else {
        return Ok(HttpResponse::Ok()
            .insert_header(("TR-QueryID", query_id.to_string().replace("\n", "")))
            .streaming(completion_stream));
    }
}

pub async fn get_topic_string(
    model: String,
    first_message: String,
    dataset: &Dataset,
) -> Result<String, ServiceError> {
    let prompt_topic_message = ChatMessage::User {
        content: ChatMessageContent::Text(format!(
            "Write a 2-3 word topic name from the following prompt: {}",
            first_message
        )),
        name: None,
    };
    let dataset_config = DatasetConfiguration::from_json(dataset.server_configuration.clone());
    let parameters = ChatCompletionParameters {
        model,
        messages: vec![prompt_topic_message],
        stream: Some(false),
        temperature: None,
        top_p: None,
        n: None,
        stop: None,
        frequency_penalty: Some(dataset_config.FREQUENCY_PENALTY.unwrap_or(0.8) as f32),
        presence_penalty: Some(dataset_config.PRESENCE_PENALTY.unwrap_or(0.8) as f32),
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

    let base_url = dataset_config.LLM_BASE_URL.clone();

    let base_url = if base_url.is_empty() {
        "https://openrouter.ai/api/v1".into()
    } else {
        base_url
    };

    let llm_api_key = get_llm_api_key(&dataset_config);

    let client = Client {
        headers: None,
        api_key: llm_api_key,
        project: None,
        http_client: reqwest::Client::new(),
        base_url,
        organization: None,
    };

    let query = client.chat().create(parameters).await.map_err(|err| {
        ServiceError::BadRequest(format!("No LLM Completion for topic {:?}", err))
    })?;

    let topic = match &query
        .choices
        .get(0)
        .ok_or(ServiceError::BadRequest(
            "No response for LLM completion".to_string(),
        ))?
        .message
    {
        ChatMessage::User {
            content: ChatMessageContent::Text(topic),
            ..
        }
        | ChatMessage::System {
            content: ChatMessageContent::Text(topic),
            ..
        }
        | ChatMessage::Assistant {
            content: Some(ChatMessageContent::Text(topic)),
            ..
        } => topic.clone(),
        _ => "".to_string(),
    };

    Ok(topic)
}

pub async fn get_text_from_image(
    image_url: String,
    prompt: Option<String>,
    dataset: &Dataset,
) -> Result<String, ServiceError> {
    let dataset_config = DatasetConfiguration::from_json(dataset.server_configuration.clone());
    let base_url = dataset_config.LLM_BASE_URL.clone();

    let base_url = if base_url.is_empty() {
        "https://openrouter.ai/api/v1".into()
    } else {
        base_url
    };

    let llm_api_key = get_llm_api_key(&dataset_config);

    let client = Client {
        headers: None,
        api_key: llm_api_key,
        project: None,
        http_client: reqwest::Client::new(),
        base_url,
        organization: None,
    };

    let default_system_prompt = "Please describe the image and turn the description into a search query. DO NOT INCLUDE ANY OTHER CONTEXT OR INFORMATION. JUST OUTPUT THE SEARCH QUERY AND NOTHING ELSE".to_string();

    let messages = vec![
        ChatMessage::System {
            content: ChatMessageContent::Text(prompt.unwrap_or(default_system_prompt)),
            name: None,
        },
        ChatMessage::User {
            content: ChatMessageContent::ContentPart(vec![ChatMessageContentPart::Image(
                ChatMessageImageContentPart {
                    r#type: "image_url".to_string(),
                    image_url: ImageUrlType {
                        url: image_url.clone(),
                        detail: None,
                    },
                },
            )]),
            name: None,
        },
    ];

    let parameters = ChatCompletionParameters {
        model: dataset_config.LLM_DEFAULT_MODEL.clone(),
        messages,
        stream: Some(false),
        temperature: None,
        top_p: None,
        n: None,
        stop: None,
        frequency_penalty: Some(dataset_config.FREQUENCY_PENALTY.unwrap_or(0.8) as f32),
        presence_penalty: Some(dataset_config.PRESENCE_PENALTY.unwrap_or(0.8) as f32),
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

    let query = client
        .chat()
        .create(parameters)
        .await
        .map_err(|err| ServiceError::BadRequest(format!("Error: {:?}", err)))?;

    let text = match &query
        .choices
        .get(0)
        .ok_or(ServiceError::BadRequest(
            "No response for LLM completion".to_string(),
        ))?
        .message
    {
        ChatMessage::User {
            content: ChatMessageContent::Text(text),
            ..
        }
        | ChatMessage::System {
            content: ChatMessageContent::Text(text),
            ..
        }
        | ChatMessage::Assistant {
            content: Some(ChatMessageContent::Text(text)),
            ..
        } => text.clone(),
        _ => "".to_string(),
    };

    Ok(text)
}

pub async fn get_text_from_audio(audio_base64: &str) -> Result<String, ServiceError> {
    let client = Client {
        headers: None,
        api_key: get_env!("OPENAI_API_KEY", "OPENAI_API_KEY for openai should be set").into(),
        project: None,
        http_client: reqwest::Client::new(),
        base_url: "https://api.openai.com/v1/".into(),
        organization: None,
    };

    let audio_bytes = base64::decode(audio_base64).map_err(|err| {
        ServiceError::BadRequest(format!("Error decoding audio base64: {:?}", err))
    })?;

    let parameters = AudioTranscriptionParametersBuilder::default()
        .file(FileUpload::Bytes(FileUploadBytes {
            filename: "audio.mp3".to_string(),
            bytes: audio_bytes.into(),
        }))
        .model(WhisperEngine::Whisper1.to_string())
        .response_format(AudioOutputFormat::Text)
        .language("en".to_string())
        .build()
        .map_err(|err| {
            ServiceError::InternalServerError(format!("Transcription Error: {:?}", err))
        })?;

    let text = client
        .audio()
        .create_transcription(parameters)
        .await
        .map_err(|err| {
            ServiceError::InternalServerError(format!("Transcription Error: {:?}", err))
        })?;

    dbg!(&text);

    Ok(text.replace("\n", ""))
}

fn get_llm_api_key(dataset_config: &DatasetConfiguration) -> String {
    if !dataset_config.LLM_API_KEY.is_empty() {
        dataset_config.LLM_API_KEY.clone()
    } else if dataset_config.LLM_BASE_URL.contains("openai.com") {
        get_env!("OPENAI_API_KEY", "OPENAI_API_KEY for openai should be set").into()
    } else if dataset_config.LLM_BASE_URL.contains("groq.com") {
        get_env!("GROQ_API_KEY", "GROQ_API_KEY for groq should be set").into()
    } else {
        get_env!(
            "LLM_API_KEY",
            "LLM_API_KEY for openrouter or self-hosted should be set"
        )
        .into()
    }
}

pub async fn suggested_followp_questions(
    payload: SuggestedQueriesReqPayload,
    dataset_config: DatasetConfiguration,
) -> Result<Vec<String>, ServiceError> {
    let base_url = dataset_config.LLM_BASE_URL.clone();
    let default_model = dataset_config.LLM_DEFAULT_MODEL.clone();

    let base_url = if base_url.is_empty() {
        "https://api.openai.com/api/v1".into()
    } else {
        base_url
    };

    let llm_api_key = get_llm_api_key(&dataset_config);

    let followup = if payload.is_followup.unwrap_or(false) {
        " followup"
    } else {
        ""
    };

    let number_of_suggestions_to_create = payload.suggestions_to_create.unwrap_or(10);
    let context = payload
        .context
        .clone()
        .unwrap_or("You are an AI assistant that wants to help the user".to_string());

    let content = ChatMessageContent::Text(format!("Generate {number_of_suggestions_to_create}{followup} varied questions followed by a new line. Your only response should be the {number_of_suggestions_to_create}{followup} questions. \n\n Suggest varied questions with the following context in mind: {context}. Keep your query recommendations short, limited to 3-6 words. This should not be a list, so do not number each question."));

    let message = ChatMessage::User {
        content,
        name: None,
    };

    let parameters = ChatCompletionParameters {
        model: default_model,
        messages: vec![message],
        stream: Some(false),
        temperature: None,
        top_p: None,
        n: None,
        stop: None,
        max_completion_tokens: None,
        frequency_penalty: Some(dataset_config.FREQUENCY_PENALTY.unwrap_or(0.8) as f32),
        presence_penalty: Some(dataset_config.PRESENCE_PENALTY.unwrap_or(0.8) as f32),
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

    let client = Client {
        headers: None,
        project: None,
        api_key: llm_api_key,
        http_client: reqwest::Client::new(),
        base_url,
        organization: None,
    };

    let query = client
        .chat()
        .create(parameters.clone())
        .await
        .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

    let queries: Vec<String> = match &query
        .choices
        .get(0)
        .unwrap_or(&ChatCompletionChoice {
            logprobs: None,
            index: 0,
            message: ChatMessage::User {
                content: ChatMessageContent::Text("".to_string()),
                name: None,
            },
            finish_reason: None,
        })
        .message
    {
        ChatMessage::User {
            content: ChatMessageContent::Text(content),
            ..
        }
        | ChatMessage::System {
            content: ChatMessageContent::Text(content),
            ..
        }
        | ChatMessage::Assistant {
            content: Some(ChatMessageContent::Text(content)),
            ..
        } => content.clone(),
        _ => "".to_string(),
    }
    .split('\n')
    .filter_map(|query| {
        let cleaned_query = query.to_string().trim().trim_matches('\n').to_string();
        if cleaned_query.is_empty() {
            None
        } else {
            Some(cleaned_query)
        }
    })
    .map(|query| query.to_string().trim().trim_matches('\n').to_string())
    .collect();

    Ok(queries)
}

pub async fn suggested_new_queries(
    payload: SuggestedQueriesReqPayload,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
    pool: web::Data<Pool>,
    redis_pool: web::Data<RedisPool>,
) -> Result<Vec<String>, ServiceError> {
    let dataset_id = dataset_org_plan_sub.dataset.id;
    let dataset_config =
        DatasetConfiguration::from_json(dataset_org_plan_sub.dataset.clone().server_configuration);

    let base_url = dataset_config.LLM_BASE_URL.clone();
    let default_model = dataset_config.LLM_DEFAULT_MODEL.clone();
    let qdrant_only = dataset_config.QDRANT_ONLY;

    let base_url = if base_url.is_empty() {
        "https://api.openai.com/api/v1".into()
    } else {
        base_url
    };

    let llm_api_key = get_llm_api_key(&dataset_config);
    let search_type = payload.search_type.clone().unwrap_or(SearchMethod::Hybrid);
    let filters = payload.filters.clone();

    let chunk_metadatas = match payload.query.clone() {
        Some(query) => {
            let search_req_payload = SearchChunksReqPayload {
                search_type: search_type.clone(),
                query: models::QueryTypes::Single(models::SearchModalities::Text(query.clone())),
                page_size: Some(10),
                filters,
                ..Default::default()
            };
            let parsed_query = ParsedQuery {
                query,
                quote_words: None,
                negated_words: None,
            };
            match search_type {
                SearchMethod::Hybrid => search_hybrid_chunks(
                    search_req_payload,
                    parsed_query,
                    pool,
                    redis_pool,
                    dataset_org_plan_sub.dataset.clone(),
                    &dataset_config,
                    &mut Timer::new(),
                )
                .await
                .map_err(|err| ServiceError::BadRequest(err.to_string()))?,
                _ => search_chunks_query(
                    search_req_payload,
                    ParsedQueryTypes::Single(parsed_query),
                    pool,
                    redis_pool,
                    dataset_org_plan_sub.dataset.clone(),
                    &dataset_config,
                    &mut Timer::new(),
                )
                .await
                .map_err(|err| ServiceError::BadRequest(err.to_string()))?,
            }
            .score_chunks
            .into_iter()
            .filter_map(|chunk| chunk.metadata.clone().get(0).cloned())
            .map(ChunkMetadata::from)
            .collect::<Vec<ChunkMetadata>>()
        }
        None => {
            let random_chunk = get_random_chunk_metadatas_query(dataset_id, 1, pool.clone())
                .await?
                .clone()
                .get(0)
                .cloned();
            match random_chunk {
                Some(chunk) => {
                    let filter =
                        assemble_qdrant_filter(filters, None, None, dataset_id, pool.clone())
                            .await?;

                    let (search_results, _) = scroll_dataset_points(
                        10,
                        Some(chunk.qdrant_point_id),
                        None,
                        dataset_config.clone(),
                        filter,
                    )
                    .await?;
                    if qdrant_only {
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
                        get_chunk_metadatas_from_point_ids(qdrant_point_ids.clone(), pool)
                            .await?
                            .into_iter()
                            .map(ChunkMetadata::from)
                            .collect()
                    }
                }
                None => vec![],
            }
        }
    };

    let rag_content = chunk_metadatas
        .iter()
        .enumerate()
        .map(|(idx, chunk)| {
            format!(
                "Doc {}: {}",
                idx + 1,
                convert_html_to_text(&(chunk.chunk_html.clone().unwrap_or_default()))
            )
        })
        .collect::<Vec<String>>()
        .join("\n\n");

    let query_style = match payload
        .suggestion_type
        .clone()
        .unwrap_or(SuggestType::Keyword)
    {
        SuggestType::Question => "question",
        SuggestType::Keyword => "keyword",
        SuggestType::Semantic => "semantic while not question",
    };
    let context_sentence = match payload.context.clone() {
        Some(context) => {
            format!(
                "\n\nSuggest varied {query_style} queries with the following context in mind: {}.\n\n",
                context
            )
        }
        None => "".to_string(),
    };

    let number_of_suggestions_to_create = payload.suggestions_to_create.unwrap_or(10);

    let content = ChatMessageContent::Text(format!(
        "Here is some content which the user might be looking for: {rag_content}{context_sentence}. Generate {number_of_suggestions_to_create} varied followup {query_style} style queries based off the domain of this dataset. Your only response should be the {number_of_suggestions_to_create} followup {query_style} style queries which are separated by new lines and are just text and you do not add any other context or information about the followup {query_style} style queries. This should not be a list, so do not number each {query_style} style queries.",
    ));

    let message = ChatMessage::User {
        content,
        name: None,
    };

    let parameters = ChatCompletionParameters {
        model: default_model,
        messages: vec![message],
        stream: Some(false),
        temperature: None,
        top_p: None,
        n: None,
        stop: None,
        max_completion_tokens: None,
        frequency_penalty: Some(dataset_config.clone().FREQUENCY_PENALTY.unwrap_or(0.8) as f32),
        presence_penalty: Some(dataset_config.clone().PRESENCE_PENALTY.unwrap_or(0.8) as f32),
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

    let client = Client {
        headers: None,
        project: None,
        api_key: llm_api_key,
        http_client: reqwest::Client::new(),
        base_url,
        organization: None,
    };

    let mut query = client
        .chat()
        .create(parameters.clone())
        .await
        .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

    let mut queries: Vec<String> = match &query
        .choices
        .get(0)
        .unwrap_or(&ChatCompletionChoice {
            logprobs: None,
            index: 0,
            message: ChatMessage::User {
                content: ChatMessageContent::Text("".to_string()),
                name: None,
            },
            finish_reason: None,
        })
        .message
    {
        ChatMessage::User {
            content: ChatMessageContent::Text(content),
            ..
        }
        | ChatMessage::System {
            content: ChatMessageContent::Text(content),
            ..
        }
        | ChatMessage::Assistant {
            content: Some(ChatMessageContent::Text(content)),
            ..
        } => content.clone(),
        _ => "".to_string(),
    }
    .split('\n')
    .filter_map(|query| {
        let cleaned_query = query.to_string().trim().trim_matches('\n').to_string();
        if cleaned_query.is_empty() {
            None
        } else {
            Some(cleaned_query)
        }
    })
    .map(|query| query.to_string().trim().trim_matches('\n').to_string())
    .collect();

    while queries.len() < number_of_suggestions_to_create {
        query = match client.chat().create(parameters.clone()).await {
            Ok(query) => query,
            Err(err) => {
                log::error!(
                    "Error generating suggested queries when queries are less than 3: {}",
                    err
                );
                return Err(ServiceError::BadRequest(err.to_string()));
            }
        };
        let first_query = match query.choices.get(0) {
            Some(first_query) => first_query,
            None => {
                log::error!("Error generating suggested queries when queries are less than 3: No first query in choices");
                return Err(ServiceError::BadRequest(
                    "No first query in choices on call to LLM".to_string(),
                ));
            }
        };
        queries = match &first_query.message {
            ChatMessage::User {
                content: ChatMessageContent::Text(content),
                ..
            }
            | ChatMessage::System {
                content: ChatMessageContent::Text(content),
                ..
            }
            | ChatMessage::Assistant {
                content: Some(ChatMessageContent::Text(content)),
                ..
            } => content.clone(),
            _ => "".to_string(),
        }
        .split('\n')
        .map(|query| query.to_string().trim().trim_matches('\n').to_string())
        .collect();
    }

    let mut engine: SimSearch<String> = SimSearch::new();

    chunk_metadatas.iter().for_each(|chunk| {
        let content = convert_html_to_text(&chunk.chunk_html.clone().unwrap_or_default());

        engine.insert(content.clone(), &content);
    });

    let sortable_queries = queries
        .iter()
        .map(|query| (query, engine.search(query).len()))
        .collect_vec();

    //search for the query
    queries = sortable_queries
        .iter()
        .sorted_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
        .map(|(content, _length)| content)
        .take(number_of_suggestions_to_create)
        .cloned()
        .cloned()
        .collect_vec();

    Ok(queries)
}
