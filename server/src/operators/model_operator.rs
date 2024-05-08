use crate::{
    data::models::ServerDatasetConfiguration, errors::ServiceError, get_env,
    handlers::chunk_handler::ScoreChunkDTO,
};
use openai_dive::v1::{
    helpers::format_response,
    resources::embedding::{EmbeddingInput, EmbeddingOutput, EmbeddingResponse},
};
use serde::{Deserialize, Serialize};
use std::ops::IndexMut;

use super::parse_operator::convert_html_to_text;

#[derive(Debug, Serialize, Deserialize)]
pub struct EmbeddingParameters {
    /// Input text to embed, encoded as a string or array of tokens.
    /// To embed multiple inputs in a single request, pass an array of strings or array of token arrays.
    pub input: EmbeddingInput,
    /// ID of the model to use.
    pub model: String,
}

#[tracing::instrument]
pub async fn create_embedding(
    message: String,
    embed_type: &str,
    dataset_config: ServerDatasetConfiguration,
) -> Result<Vec<f32>, ServiceError> {
    let parent_span = sentry::configure_scope(|scope| scope.get_span());
    let transaction: sentry::TransactionOrSpan = match &parent_span {
        Some(parent) => parent
            .start_child("create_embedding", "Create semantic dense embedding")
            .into(),
        None => {
            let ctx = sentry::TransactionContext::new(
                "create_embedding",
                "Create semantic dense embedding",
            );
            sentry::start_transaction(ctx).into()
        }
    };
    sentry::configure_scope(|scope| scope.set_span(Some(transaction.clone())));

    let open_ai_api_key = get_env!("OPENAI_API_KEY", "OPENAI_API_KEY should be set");
    let config_embedding_base_url = dataset_config.EMBEDDING_BASE_URL;
    transaction.set_data(
        "EMBEDDING_SERVER",
        config_embedding_base_url.as_str().into(),
    );
    transaction.set_data(
        "EMBEDDING_MODEL",
        dataset_config.EMBEDDING_MODEL_NAME.as_str().into(),
    );

    let embedding_base_url = match config_embedding_base_url.as_str() {
        "" => get_env!("OPENAI_BASE_URL", "OPENAI_BASE_URL must be set").to_string(),
        "https://api.openai.com/v1" => {
            get_env!("OPENAI_BASE_URL", "OPENAI_BASE_URL must be set").to_string()
        }
        "https://embedding.trieve.ai" => std::env::var("EMBEDDING_SERVER_ORIGIN")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or(
                get_env!(
                    "GPU_SERVER_ORIGIN",
                    "GPU_SERVER_ORIGIN should be set if this is called"
                )
                .to_string(),
            ),
        "https://embedding.trieve.ai/bge-m3" => std::env::var("EMBEDDING_SERVER_ORIGIN_BGEM3")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or(
                get_env!(
                    "GPU_SERVER_ORIGIN",
                    "GPU_SERVER_ORIGIN should be set if this is called"
                )
                .to_string(),
            )
            .to_string(),
        _ => config_embedding_base_url,
    };

    let clipped_message = if message.len() > 7000 {
        message.chars().take(20000).collect()
    } else {
        message.clone()
    };

    let input = match embed_type {
        "doc" => EmbeddingInput::StringArray(vec![clipped_message]),
        "query" => EmbeddingInput::String(
            format!(
                "{}{}",
                dataset_config.EMBEDDING_QUERY_PREFIX, &clipped_message
            )
            .to_string(),
        ),
        _ => EmbeddingInput::StringArray(vec![clipped_message]),
    };

    let parameters = EmbeddingParameters {
        model: dataset_config.EMBEDDING_MODEL_NAME.to_string(),
        input,
    };

    let embeddings_resp = ureq::post(&format!(
        "{}/embeddings?api-version=2023-05-15",
        embedding_base_url
    ))
    .set("Authorization", &format!("Bearer {}", open_ai_api_key))
    .set("api-key", open_ai_api_key)
    .set("Content-Type", "application/json")
    .send_json(serde_json::to_value(parameters).unwrap())
    .map_err(|e| {
        ServiceError::InternalServerError(format!(
            "Could not get embeddings from server: {:?}, {:?}",
            e,
            e.to_string()
        ))
    })?;

    let embeddings: EmbeddingResponse = format_response(embeddings_resp.into_string().unwrap())
        .map_err(|e| {
            log::error!("Failed to format response from embeddings server {:?}", e);
            ServiceError::InternalServerError(
                "Failed to format response from embeddings server".to_owned(),
            )
        })?;

    let vectors: Vec<Vec<f32>> = embeddings
    .data
    .into_iter()
    .map(|x| match x.embedding {
        EmbeddingOutput::Float(v) => v.iter().map(|x| *x as f32).collect(),
        EmbeddingOutput::Base64(_) => {
            log::error!("Embedding server responded with Base64 and that is not currently supported for embeddings");
            vec![]
        }
    })
    .collect();

    if vectors.iter().any(|x| x.is_empty()) {
        return Err(ServiceError::InternalServerError(
            "Embedding server responded with Base64 and that is not currently supported for embeddings".to_owned(),
        ));
    }

    transaction.finish();

    match vectors.first() {
        Some(v) => Ok(v.clone()),
        None => Err(ServiceError::InternalServerError(
            "No dense embeddings returned from server".to_owned(),
        )),
    }
}

#[tracing::instrument]
pub async fn get_sparse_vector(
    message: String,
    embed_type: &str,
) -> Result<Vec<(u32, f32)>, ServiceError> {
    let origin_key = match embed_type {
        "doc" => "SPARSE_SERVER_DOC_ORIGIN",
        "query" => "SPARSE_SERVER_QUERY_ORIGIN",
        _ => unreachable!("Invalid embed_type passed"),
    };

    let server_origin = match std::env::var(origin_key).ok().filter(|s| !s.is_empty()) {
        Some(origin) => origin,
        None => get_env!(
            "GPU_SERVER_ORIGIN",
            "GPU_SERVER_ORIGIN should be set if this is called"
        )
        .to_string(),
    };

    let embedding_server_call = format!("{}/embed_sparse", server_origin);

    let sparse_vectors = ureq::post(&embedding_server_call)
        .set("Content-Type", "application/json")
        .set(
            "Authorization",
            &format!(
                "Bearer {}",
                get_env!("OPENAI_API_KEY", "OPENAI_API should be set")
            ),
        )
        .send_json(CustomSparseEmbedData {
            inputs: vec![message],
            encode_type: embed_type.to_string(),
            truncate: true,
        })
        .map_err(|err| {
            log::error!(
                "Failed parsing response from custom embedding server {:?}",
                err
            );
            ServiceError::BadRequest(format!("Failed making call to server {:?}", err))
        })?
        .into_json::<Vec<Vec<SpladeIndicies>>>()
        .map_err(|_e| {
            log::error!(
                "Failed parsing response from custom embedding server {:?}",
                _e
            );
            ServiceError::BadRequest(
                "Failed parsing response from custom embedding server".to_string(),
            )
        })?;

    match sparse_vectors.first() {
        Some(v) => Ok(v
            .iter()
            .map(|splade_idx| (*splade_idx).into_tuple())
            .collect()),
        None => Err(ServiceError::InternalServerError(
            "No sparse embeddings returned from server".to_owned(),
        )),
    }
}

// create_thirty_embeddings

#[tracing::instrument]
pub async fn create_embeddings(
    messages: Vec<String>,
    embed_type: &str,
    dataset_config: ServerDatasetConfiguration,
    reqwest_client: reqwest::Client,
) -> Result<Vec<Vec<f32>>, ServiceError> {
    let parent_span = sentry::configure_scope(|scope| scope.get_span());
    let transaction: sentry::TransactionOrSpan = match &parent_span {
        Some(parent) => parent
            .start_child("create_embedding", "Create semantic dense embedding")
            .into(),
        None => {
            let ctx = sentry::TransactionContext::new(
                "create_embedding",
                "Create semantic dense embedding",
            );
            sentry::start_transaction(ctx).into()
        }
    };
    sentry::configure_scope(|scope| scope.set_span(Some(transaction.clone())));

    let open_ai_api_key = get_env!("OPENAI_API_KEY", "OPENAI_API_KEY should be set");
    let config_embedding_base_url = dataset_config.EMBEDDING_BASE_URL;

    let embedding_base_url = match config_embedding_base_url.as_str() {
        "" => get_env!("OPENAI_BASE_URL", "OPENAI_BASE_URL must be set").to_string(),
        "https://api.openai.com/v1" => {
            get_env!("OPENAI_BASE_URL", "OPENAI_BASE_URL must be set").to_string()
        }
        "https://embedding.trieve.ai" => std::env::var("EMBEDDING_SERVER_ORIGIN")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or(
                get_env!(
                    "GPU_SERVER_ORIGIN",
                    "GPU_SERVER_ORIGIN should be set if this is called"
                )
                .to_string(),
            ),
        "https://embedding.trieve.ai/bge-m3" => std::env::var("EMBEDDING_SERVER_ORIGIN_BGEM3")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or(
                get_env!(
                    "GPU_SERVER_ORIGIN",
                    "GPU_SERVER_ORIGIN should be set if this is called"
                )
                .to_string(),
            )
            .to_string(),
        _ => config_embedding_base_url,
    };

    let thirty_message_groups = messages.chunks(30);

    let vec_futures: Vec<_> = thirty_message_groups
        .enumerate()
        .map(|(i, messages)| {
            let clipped_messages = messages
                .iter()
                .map(|message| {
                    if message.len() > 7000 {
                        message.chars().take(20000).collect()
                    } else {
                        message.clone()
                    }
                })
                .collect::<Vec<String>>();

            let input = match embed_type {
                "doc" => EmbeddingInput::StringArray(clipped_messages),
                "query" => EmbeddingInput::String(
                    format!(
                        "{}{}",
                        dataset_config.EMBEDDING_QUERY_PREFIX, &clipped_messages[0]
                    )
                    .to_string(),
                ),
                _ => EmbeddingInput::StringArray(clipped_messages),
            };

            let parameters = EmbeddingParameters {
                model: dataset_config.EMBEDDING_MODEL_NAME.to_string(),
                input,
            };

            let cur_client = reqwest_client.clone();
            let url = embedding_base_url.clone();

            let vectors_resp = async move {
                let embeddings_resp = cur_client
                .post(&format!("{}/embeddings?api-version=2023-05-15", url))
                .header("Authorization", &format!("Bearer {}", open_ai_api_key))
                .header("api-key", open_ai_api_key)
                .header("Content-Type", "application/json")
                .json(&parameters)
                .send()
                .await
                .map_err(|_| {
                    ServiceError::BadRequest("Failed to send message to embedding server".to_string())
                })?
                .text()
                .await
                .map_err(|_| {
                    ServiceError::BadRequest("Failed to get text from embeddings".to_string())
                })?;

                let embeddings: EmbeddingResponse = format_response(embeddings_resp)
                    .map_err(|e| {
                        log::error!("Failed to format response from embeddings server {:?}", e);
                        ServiceError::InternalServerError(
                            "Failed to format response from embeddings server".to_owned(),
                        )
                    })?;

                let vectors: Vec<Vec<f32>> = embeddings
                .data
                .into_iter()
                .map(|x| match x.embedding {
                    EmbeddingOutput::Float(v) => v.iter().map(|x| *x as f32).collect(),
                    EmbeddingOutput::Base64(_) => {
                        log::error!("Embedding server responded with Base64 and that is not currently supported for embeddings");
                        vec![]
                    }
                })
                .collect();

                if vectors.iter().any(|x| x.is_empty()) {
                    return Err(ServiceError::InternalServerError(
                        "Embedding server responded with Base64 and that is not currently supported for embeddings".to_owned(),
                    ));
                }
                Ok((i, vectors))
            };

            vectors_resp
        })
        .collect();

    let all_chunk_vectors: Vec<(usize, Vec<Vec<f32>>)> = futures::future::join_all(vec_futures)
        .await
        .into_iter()
        .collect::<Result<Vec<(usize, Vec<Vec<f32>>)>, ServiceError>>()?;

    let mut vectors_sorted = vec![];
    for index in 0..all_chunk_vectors.len() {
        let (_, vectors_i) = all_chunk_vectors.iter().find(|(i, _)| *i == index).ok_or(
            ServiceError::InternalServerError(
                "Failed to get index i (this should never happen)".to_string(),
            ),
        )?;

        vectors_sorted.extend(vectors_i.clone());
    }

    transaction.finish();
    Ok(vectors_sorted)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SpladeEmbedding {
    pub embeddings: Vec<(u32, f32)>,
}

#[derive(Copy, Clone, Debug, Deserialize)]
struct SpladeIndicies {
    index: u32,
    value: f32,
}

impl SpladeIndicies {
    pub fn into_tuple(self) -> (u32, f32) {
        (self.index, self.value)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CustomSparseEmbedData {
    pub inputs: Vec<String>,
    pub encode_type: String,
    pub truncate: bool,
}

#[tracing::instrument]
pub async fn get_sparse_vectors(
    messages: Vec<String>,
    embed_type: &str,
    reqwest_client: reqwest::Client,
) -> Result<Vec<Vec<(u32, f32)>>, ServiceError> {
    if messages.is_empty() {
        return Err(ServiceError::BadRequest(
            "No messages to encode".to_string(),
        ));
    }

    let thirty_message_groups = messages.chunks(30);

    let vec_futures: Vec<_> = thirty_message_groups
        .enumerate()
        .map(|(i, thirty_messages)| {
            let cur_client = reqwest_client.clone();

            let origin_key = match embed_type {
                "doc" => "SPARSE_SERVER_DOC_ORIGIN",
                "query" => "SPARSE_SERVER_QUERY_ORIGIN",
                _ => unreachable!("Invalid embed_type passed"),
            };

            let server_origin = match std::env::var(origin_key).ok().filter(|s| !s.is_empty()) {
                Some(origin) => origin,
                None => get_env!(
                    "GPU_SERVER_ORIGIN",
                    "GPU_SERVER_ORIGIN should be set if this is called"
                )
                .to_string(),
            };
            let embedding_server_call = format!("{}/embed_sparse", server_origin);

            async move {
                let sparse_embed_req = CustomSparseEmbedData {
                    inputs: thirty_messages.to_vec(),
                    encode_type: embed_type.to_string(),
                    truncate: true,
                };

                let sparse_vectors = cur_client
                    .post(&embedding_server_call)
                    .header("Content-Type", "application/json")
                    .header(
                        "Authorization",
                        &format!(
                            "Bearer {}",
                            get_env!("OPENAI_API_KEY", "OPENAI_API should be set")
                        ),
                    )
                    .json(&sparse_embed_req)
                    .send()
                    .await
                    .map_err(|err| {
                        log::error!(
                            "Failed parsing response from custom embedding server {:?}",
                            err
                        );
                        ServiceError::BadRequest(format!("Failed making call to server {:?}", err))
                    })?
                    .json::<Vec<Vec<SpladeIndicies>>>()
                    .await
                    .map_err(|_e| {
                        log::error!(
                            "Failed parsing response from custom embedding server {:?}",
                            _e
                        );
                        ServiceError::BadRequest(
                            "Failed parsing response from custom embedding server".to_string(),
                        )
                    })?;

                Ok((i, sparse_vectors))
            }
        })
        .collect();

    let all_chunk_vectors: Vec<(usize, Vec<Vec<SpladeIndicies>>)> =
        futures::future::join_all(vec_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<(usize, Vec<Vec<SpladeIndicies>>)>, ServiceError>>()?;

    let mut vectors_sorted = vec![];
    for index in 0..all_chunk_vectors.len() {
        let (_, vectors_i) = all_chunk_vectors.iter().find(|(i, _)| *i == index).ok_or(
            ServiceError::InternalServerError(
                "Failed to get index i (this should never happen)".to_string(),
            ),
        )?;

        vectors_sorted.extend(vectors_i.clone());
    }

    Ok(vectors_sorted
        .iter()
        .map(|sparse_vector| {
            sparse_vector
                .iter()
                .map(|splade_idx| (*splade_idx).into_tuple())
                .collect()
        })
        .collect())
}

#[derive(Debug, Serialize, Deserialize)]
struct ScorePair {
    index: usize,
    score: f32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CrossEncoderData {
    pub query: String,
    pub texts: Vec<String>,
    pub truncate: bool,
}

#[tracing::instrument]
pub async fn cross_encoder(
    query: String,
    page_size: u64,
    results: Vec<ScoreChunkDTO>,
) -> Result<Vec<ScoreChunkDTO>, actix_web::Error> {
    let parent_span = sentry::configure_scope(|scope| scope.get_span());
    let transaction: sentry::TransactionOrSpan = match &parent_span {
        Some(parent) => parent
            .start_child("Cross Encoder", "Cross Encode semantic and hybrid chunks")
            .into(),
        None => {
            let ctx = sentry::TransactionContext::new(
                "Cross Encoder",
                "Cross Encode semantic and hybrid chunks",
            );
            sentry::start_transaction(ctx).into()
        }
    };
    sentry::configure_scope(|scope| scope.set_span(Some(transaction.clone())));

    let server_origin: String = match std::env::var("RERANKER_SERVER_ORIGIN")
        .ok()
        .filter(|s| !s.is_empty())
    {
        Some(origin) => origin,
        None => get_env!(
            "GPU_SERVER_ORIGIN",
            "GPU_SERVER_ORIGIN should be set if this is called"
        )
        .to_string(),
    };

    let embedding_server_call = format!("{}/rerank", server_origin);

    if results.is_empty() {
        return Ok(vec![]);
    }

    let request_docs = results
        .clone()
        .into_iter()
        .map(|x| convert_html_to_text(&(x.metadata[0].clone().chunk_html.unwrap_or_default())))
        .collect::<Vec<String>>();

    let resp = ureq::post(&embedding_server_call)
        .set("Content-Type", "application/json")
        .set(
            "Authorization",
            &format!(
                "Bearer {}",
                get_env!("OPENAI_API_KEY", "OPENAI_API should be set")
            ),
        )
        .send_json(CrossEncoderData {
            query,
            texts: request_docs,
            truncate: true,
        })
        .map_err(|err| ServiceError::BadRequest(format!("Failed making call to server {:?}", err)))?
        .into_json::<Vec<ScorePair>>()
        .map_err(|_e| {
            log::error!(
                "Failed parsing response from custom embedding server {:?}",
                _e
            );
            ServiceError::BadRequest(
                "Failed parsing response from custom embedding server".to_string(),
            )
        })?;

    let mut results = results.clone();

    resp.into_iter().for_each(|pair| {
        results.index_mut(pair.index).score = pair.score as f64;
    });

    results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());

    results.truncate(page_size.try_into().unwrap());

    transaction.finish();
    Ok(results)
}
