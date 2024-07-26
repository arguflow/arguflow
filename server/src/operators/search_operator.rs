use super::chunk_operator::{
    get_chunk_metadatas_and_collided_chunks_from_point_ids_query,
    get_content_chunk_from_point_ids_query, get_highlights, get_highlights_with_exact_match,
    get_qdrant_ids_from_chunk_ids_query, get_slim_chunks_from_point_ids_query, HighlightStrategy,
};
use super::group_operator::{
    get_group_ids_from_tracking_ids_query, get_groups_from_group_ids_query,
};
use super::model_operator::{
    cross_encoder, get_bm25_embeddings, get_dense_vector, get_sparse_vector,
};
use super::qdrant_operator::{
    count_qdrant_query, search_over_groups_query, GroupSearchResults, QdrantSearchQuery, VectorType,
};
use crate::data::models::{
    convert_to_date_time, ChunkGroup, ChunkGroupAndFileId, ChunkMetadata, ChunkMetadataTypes,
    ConditionType, ContentChunkMetadata, Dataset, DatasetConfiguration, GeoInfoWithBias,
    HasIDCondition, QdrantSortBy, ReRankOptions, ScoreChunk, ScoreChunkDTO, SearchMethod,
    SlimChunkMetadata, SortByField, SortBySearchType, SortOrder, UnifiedId,
};
use crate::handlers::chunk_handler::{
    AutocompleteReqPayload, ChunkFilter, CountChunkQueryResponseBody, CountChunksReqPayload,
    ParsedQuery, SearchChunkQueryResponseBody, SearchChunksReqPayload,
};
use crate::handlers::group_handler::{
    SearchOverGroupsReqPayload, SearchWithinGroupReqPayload, SearchWithinGroupResults,
};
use crate::operators::qdrant_operator::search_qdrant_query;
use crate::{
    data::models::{get_range, FieldCondition, MatchCondition, Pool},
    errors::ServiceError,
};
use actix_web::web;
use diesel::dsl::sql;
use diesel::sql_types::{Bool, Float, Text};
use diesel::{ExpressionMethods, JoinOnDsl, PgArrayExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;

use itertools::Itertools;
use simple_server_timing_header::Timer;
use utoipa::ToSchema;

use qdrant_client::qdrant::condition::ConditionOneOf::HasId;
use qdrant_client::qdrant::Filter;
use qdrant_client::qdrant::{Condition, HasIdCondition, PointId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SearchResult {
    pub score: f32,
    pub point_id: uuid::Uuid,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SearchChunkQueryResult {
    pub search_results: Vec<SearchResult>,
    pub total_chunk_pages: i64,
    pub batch_lengths: Vec<usize>,
}

async fn convert_group_tracking_ids_to_group_ids(
    condition: FieldCondition,
    dataset_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<FieldCondition, ServiceError> {
    if condition.field == "group_tracking_ids" {
        if let Some(match_any) = condition.r#match_any {
            let matches = match_any
                .iter()
                .map(|item| item.to_string())
                .collect::<Vec<String>>();

            let correct_matches: Vec<MatchCondition> =
                get_group_ids_from_tracking_ids_query(matches, dataset_id, pool.clone())
                    .await?
                    .iter()
                    .map(|(id, _)| MatchCondition::Text(id.to_string()))
                    .collect();

            Ok(FieldCondition {
                field: "group_ids".to_string(),
                match_any: Some(correct_matches),
                match_all: None,
                date_range: None,
                range: None,
                geo_bounding_box: None,
                geo_polygon: None,
                geo_radius: None,
            })
        } else if let Some(match_all) = condition.match_all {
            let matches = match_all
                .iter()
                .map(|item| item.to_string())
                .collect::<Vec<String>>();

            let correct_matches: Vec<MatchCondition> =
                get_group_ids_from_tracking_ids_query(matches, dataset_id, pool.clone())
                    .await?
                    .iter()
                    .map(|(id, _)| MatchCondition::Text(id.to_string()))
                    .collect();

            Ok(FieldCondition {
                field: "group_ids".to_string(),
                match_any: None,
                match_all: Some(correct_matches),
                date_range: None,
                range: None,
                geo_bounding_box: None,
                geo_polygon: None,
                geo_radius: None,
            })
        } else {
            Err(ServiceError::BadRequest(
                "match_any key not found for group_tracking_ids".to_string(),
            ))?
        }
    } else {
        Ok(condition)
    }
}

pub async fn get_qdrant_ids_from_condition(
    cond: HasIDCondition,
    pool: web::Data<Pool>,
) -> Result<Vec<String>, ServiceError> {
    if let Some(ids) = cond.ids {
        Ok(get_qdrant_ids_from_chunk_ids_query(
            ids.into_iter().map(UnifiedId::TrieveUuid).collect(),
            pool.clone(),
        )
        .await?
        .into_iter()
        .map(|id| id.to_string())
        .collect::<Vec<String>>())
    } else if let Some(tracking_ids) = cond.tracking_ids {
        Ok(get_qdrant_ids_from_chunk_ids_query(
            tracking_ids
                .into_iter()
                .map(UnifiedId::TrackingId)
                .collect(),
            pool.clone(),
        )
        .await?
        .into_iter()
        .map(|id| id.to_string())
        .collect::<Vec<String>>())
    } else {
        Err(ServiceError::BadRequest(
            "ids or tracking_ids must be provided".to_string(),
        ))?
    }
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip(pool))]
pub async fn assemble_qdrant_filter(
    filters: Option<ChunkFilter>,
    quote_words: Option<Vec<String>>,
    negated_words: Option<Vec<String>>,
    dataset_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Filter, ServiceError> {
    let mut filter = Filter::default();

    filter
        .must
        .push(Condition::matches("dataset_id", dataset_id.to_string()));

    if let Some(filters) = filters {
        if let Some(should_filters) = filters.should {
            for should_condition in should_filters {
                match should_condition {
                    ConditionType::Field(cond) => {
                        let should_condition =
                            convert_group_tracking_ids_to_group_ids(cond, dataset_id, pool.clone())
                                .await?;

                        let qdrant_condition = should_condition
                            .convert_to_qdrant_condition(
                                filters.jsonb_prefilter,
                                dataset_id,
                                pool.clone(),
                            )
                            .await?;

                        if let Some(condition) = qdrant_condition {
                            filter.should.push(condition);
                        }
                    }
                    ConditionType::HasID(cond) => {
                        filter.should.push(Condition::has_id(
                            get_qdrant_ids_from_condition(cond, pool.clone()).await?,
                        ));
                    }
                }
            }
        }

        if let Some(must_filters) = filters.must {
            for must_condition in must_filters {
                match must_condition {
                    ConditionType::Field(cond) => {
                        let must_condition =
                            convert_group_tracking_ids_to_group_ids(cond, dataset_id, pool.clone())
                                .await?;

                        let qdrant_condition = must_condition
                            .convert_to_qdrant_condition(
                                filters.jsonb_prefilter,
                                dataset_id,
                                pool.clone(),
                            )
                            .await?;

                        if let Some(condition) = qdrant_condition {
                            filter.must.push(condition);
                        }
                    }
                    ConditionType::HasID(cond) => {
                        filter.must.push(Condition::has_id(
                            get_qdrant_ids_from_condition(cond, pool.clone()).await?,
                        ));
                    }
                }
            }
        }

        if let Some(must_not_filters) = filters.must_not {
            for must_not_condition in must_not_filters {
                match must_not_condition {
                    ConditionType::Field(cond) => {
                        let must_not_condition =
                            convert_group_tracking_ids_to_group_ids(cond, dataset_id, pool.clone())
                                .await?;

                        let qdrant_condition = must_not_condition
                            .convert_to_qdrant_condition(
                                filters.jsonb_prefilter,
                                dataset_id,
                                pool.clone(),
                            )
                            .await?;

                        if let Some(condition) = qdrant_condition {
                            filter.must_not.push(condition);
                        }
                    }
                    ConditionType::HasID(cond) => {
                        filter.must_not.push(Condition::has_id(
                            get_qdrant_ids_from_condition(cond, pool.clone()).await?,
                        ));
                    }
                }
            }
        }
    };

    if let Some(quote_words) = quote_words {
        for quote_word in quote_words {
            filter
                .must
                .push(Condition::matches_text("content", quote_word));
        }
    }

    if let Some(negated_words) = negated_words {
        for negated_word in negated_words {
            filter
                .must_not
                .push(Condition::matches_text("content", negated_word));
        }
    }

    Ok(filter)
}

#[derive(Debug)]
pub struct RetrievePointQuery {
    vector: VectorType,
    score_threshold: Option<f32>,
    limit: u64,
    sort_by: Option<SortByField>,
    rerank_by: Option<SortBySearchType>,
    filter: Option<ChunkFilter>,
}

impl RetrievePointQuery {
    pub async fn into_qdrant_query(
        self,
        parsed_query: ParsedQuery,
        dataset_id: uuid::Uuid,
        group_id: Option<uuid::Uuid>,
        config: &DatasetConfiguration,
        pool: web::Data<Pool>,
    ) -> Result<QdrantSearchQuery, ServiceError> {
        let mut filter = assemble_qdrant_filter(
            self.filter,
            parsed_query.quote_words.clone(),
            parsed_query.negated_words.clone(),
            dataset_id,
            pool,
        )
        .await?;

        if let Some(group_id) = group_id {
            filter
                .must
                .push(Condition::matches("group_ids", group_id.to_string()));
        }

        let rerank_query = if let Some(rerank_by) = self.rerank_by {
            match rerank_by.rerank_type {
                ReRankOptions::Fulltext => {
                    let data = SearchChunksReqPayload {
                        query: rerank_by
                            .rerank_query
                            .clone()
                            .unwrap_or(parsed_query.query.clone()),
                        search_type: SearchMethod::FullText,
                        ..Default::default()
                    };

                    let vector = get_qdrant_vector(data, parsed_query, config).await?;
                    Some(QdrantSearchQuery {
                        vector,
                        score_threshold: self.score_threshold,
                        limit: rerank_by.prefetch_amount.unwrap_or(1000),
                        rerank_by: Box::new(None),
                        sort_by: None,
                        filter: filter.clone(),
                    })
                }
                ReRankOptions::Semantic => {
                    let data = SearchChunksReqPayload {
                        query: rerank_by
                            .rerank_query
                            .clone()
                            .unwrap_or(parsed_query.query.clone()),
                        search_type: SearchMethod::Semantic,
                        ..Default::default()
                    };

                    let vector = get_qdrant_vector(data, parsed_query, config).await?;
                    Some(QdrantSearchQuery {
                        vector,
                        score_threshold: self.score_threshold,
                        limit: rerank_by.prefetch_amount.unwrap_or(1000),
                        rerank_by: Box::new(None),
                        sort_by: None,
                        filter: filter.clone(),
                    })
                }
                ReRankOptions::CrossEncoder => None,
            }
        } else {
            None
        };

        Ok(QdrantSearchQuery {
            vector: self.vector,
            score_threshold: self.score_threshold,
            limit: self.limit,
            rerank_by: Box::new(rerank_query),
            sort_by: self.sort_by,
            filter: filter.clone(),
        })
    }
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument]
pub async fn retrieve_qdrant_points_query(
    qdrant_searches: Vec<QdrantSearchQuery>,
    page: u64,
    get_total_pages: bool,
    config: &DatasetConfiguration,
) -> Result<SearchChunkQueryResult, ServiceError> {
    let parent_span = sentry::configure_scope(|scope| scope.get_span());
    let transaction: sentry::TransactionOrSpan = match &parent_span {
        Some(parent) => parent
            .start_child("Qdrant Points Query", "retrieve_qdrant_points_query")
            .into(),
        None => {
            let ctx = sentry::TransactionContext::new(
                "Qdrant Points Query",
                "retrieve_qdrant_points_query",
            );
            sentry::start_transaction(ctx).into()
        }
    };
    sentry::configure_scope(|scope| scope.set_span(Some(transaction.clone())));

    let page = if page == 0 { 1 } else { page };

    let (point_ids, count, batch_lengths) = search_qdrant_query(
        page,
        qdrant_searches.clone(),
        config.clone(),
        get_total_pages,
    )
    .await?;

    let limit = qdrant_searches
        .iter()
        .map(|query| query.limit)
        .min()
        .unwrap_or(10);

    let pages = (count as f64 / limit as f64).ceil() as i64;

    Ok(SearchChunkQueryResult {
        search_results: point_ids,
        total_chunk_pages: pages,
        batch_lengths,
    })
}

#[tracing::instrument(skip(pool))]
pub async fn get_metadata_filter_condition(
    filter: &FieldCondition,
    dataset_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Filter, ServiceError> {
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    let key = filter
        .field
        .strip_prefix("metadata.")
        .unwrap_or(&filter.field)
        .to_string();

    let mut conn = pool.get().await.unwrap();

    let mut query = chunk_metadata_columns::chunk_metadata
        .select(chunk_metadata_columns::qdrant_point_id)
        .filter(chunk_metadata_columns::dataset_id.eq(dataset_id))
        .into_boxed();

    if let Some(matches) = &filter.match_any {
        if let Some(first_val) = matches.get(0) {
            match first_val {
                MatchCondition::Text(string_val) => {
                    query = query.filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, string_val
                    )));
                }
                MatchCondition::Integer(id_val) => {
                    query = query.filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, id_val
                    )));
                }
                MatchCondition::Float(id_val) => {
                    query = query.filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, id_val
                    )));
                }
            }
        }

        for match_condition in matches.iter().skip(1) {
            match match_condition {
                MatchCondition::Text(string_val) => {
                    query = query.or_filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, string_val
                    )));
                }
                MatchCondition::Integer(id_val) => {
                    query = query.or_filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, id_val
                    )));
                }
                MatchCondition::Float(id_val) => {
                    query = query.or_filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, id_val
                    )));
                }
            }
        }
    } else if let Some(matches) = &filter.match_all {
        if let Some(first_val) = matches.get(0) {
            match first_val {
                MatchCondition::Text(string_val) => {
                    query = query.filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, string_val
                    )));
                }
                MatchCondition::Integer(id_val) => {
                    query = query.filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, id_val
                    )));
                }
                MatchCondition::Float(id_val) => {
                    query = query.filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, id_val
                    )));
                }
            }
        }

        for match_condition in matches.iter().skip(1) {
            match match_condition {
                MatchCondition::Text(string_val) => {
                    query = query.filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, string_val
                    )));
                }
                MatchCondition::Integer(id_val) => {
                    query = query.filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, id_val
                    )));
                }
                MatchCondition::Float(id_val) => {
                    query = query.filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, id_val
                    )));
                }
            }
        }
    }

    if let Some(range) = &filter.range {
        let range_filter = get_range(range.clone())?;
        if let Some(gt) = range_filter.gt {
            query = query.filter(
                sql::<Bool>(&format!("(chunk_metadata.metadata->>'{}')::float > ", key))
                    .bind::<Float, _>(gt as f32),
            );
        };

        if let Some(gte) = range_filter.gte {
            query = query.filter(
                sql::<Bool>(&format!("(chunk_metadata.metadata->>'{}')::float >= ", key))
                    .bind::<Float, _>(gte as f32),
            );
        };

        if let Some(lt) = range_filter.lt {
            query = query.filter(
                sql::<Bool>(&format!("(chunk_metadata.metadata->>'{}')::float < ", key))
                    .bind::<Float, _>(lt as f32),
            );
        };

        if let Some(lte) = range_filter.lte {
            query = query.filter(
                sql::<Bool>(&format!("(chunk_metadata.metadata->>'{}')::float <= ", key))
                    .bind::<Float, _>(lte as f32),
            );
        };
    }

    if let Some(date_range) = &filter.date_range {
        if let Some(gt) = &date_range.gt {
            query = query.filter(
                sql::<Text>(&format!("chunk_metadata.metadata->>'{}'", key))
                    .gt(convert_to_date_time(Some(gt.clone()))?.unwrap().to_string()),
            );
        };

        if let Some(gte) = &date_range.gte {
            query = query.filter(
                sql::<Text>(&format!("chunk_metadata.metadata->>'{}'", key)).ge(
                    convert_to_date_time(Some(gte.clone()))?
                        .unwrap()
                        .to_string(),
                ),
            );
        };

        if let Some(lt) = &date_range.lt {
            query = query.filter(
                sql::<Text>(&format!("chunk_metadata.metadata->>'{}'", key))
                    .lt(convert_to_date_time(Some(lt.clone()))?.unwrap().to_string()),
            );
        };

        if let Some(lte) = &date_range.lte {
            query = query.filter(
                sql::<Text>(&format!("chunk_metadata.metadata->>'{}'", key)).le(
                    convert_to_date_time(Some(lte.clone()))?
                        .unwrap()
                        .to_string(),
                ),
            );
        };
    }

    let qdrant_point_ids: Vec<uuid::Uuid> = query
        .load::<uuid::Uuid>(&mut conn)
        .await
        .map_err(|_| ServiceError::BadRequest("Failed to load metadata".to_string()))?;

    let matching_point_ids: Vec<PointId> = qdrant_point_ids
        .iter()
        .map(|uuid| uuid.to_string())
        .collect::<HashSet<String>>()
        .iter()
        .map(|uuid| (*uuid).clone().into())
        .collect::<Vec<PointId>>();

    let mut metadata_filter = Filter::default();
    metadata_filter.must.push(Condition {
        condition_one_of: Some(HasId(HasIdCondition {
            has_id: matching_point_ids,
        })),
    });

    Ok(metadata_filter)
}

#[tracing::instrument(skip(pool))]
pub async fn get_group_metadata_filter_condition(
    filter: &FieldCondition,
    dataset_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Filter, ServiceError> {
    let mut metadata_filter = Filter::default();

    let key = filter
        .field
        .strip_prefix("group_metadata.")
        .unwrap_or(&filter.field)
        .to_string();

    use crate::data::schema::chunk_group::dsl as chunk_group_columns;
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    let mut conn = pool.get().await.unwrap();

    let mut query =
        chunk_metadata_columns::chunk_metadata
            .left_outer_join(chunk_group_bookmarks_columns::chunk_group_bookmarks.on(
                chunk_metadata_columns::id.eq(chunk_group_bookmarks_columns::chunk_metadata_id),
            ))
            .left_outer_join(
                chunk_group_columns::chunk_group
                    .on(chunk_group_bookmarks_columns::group_id.eq(chunk_group_columns::id)),
            )
            .select(chunk_metadata_columns::qdrant_point_id)
            .filter(chunk_metadata_columns::dataset_id.eq(dataset_id))
            .into_boxed();

    if let Some(matches) = &filter.match_any {
        if let Some(first_val) = matches.get(0) {
            match first_val {
                MatchCondition::Text(string_val) => {
                    query = query.filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, string_val
                    )));
                }
                MatchCondition::Integer(id_val) => {
                    query = query.filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, id_val
                    )));
                }
                MatchCondition::Float(id_val) => {
                    query = query.filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, id_val
                    )));
                }
            }
        }

        for match_condition in matches.iter().skip(1) {
            match match_condition {
                MatchCondition::Text(string_val) => {
                    query = query.or_filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, string_val
                    )));
                }
                MatchCondition::Integer(id_val) => {
                    query = query.or_filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, id_val
                    )));
                }
                MatchCondition::Float(id_val) => {
                    query = query.or_filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, id_val
                    )));
                }
            }
        }
    } else if let Some(matches) = &filter.match_all {
        if let Some(first_val) = matches.get(0) {
            match first_val {
                MatchCondition::Text(string_val) => {
                    query = query.filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, string_val
                    )));
                }
                MatchCondition::Integer(id_val) => {
                    query = query.filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, id_val
                    )));
                }
                MatchCondition::Float(id_val) => {
                    query = query.filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, id_val
                    )));
                }
            }
        }

        for match_condition in matches.iter().skip(1) {
            match match_condition {
                MatchCondition::Text(string_val) => {
                    query = query.filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, string_val
                    )));
                }
                MatchCondition::Integer(id_val) => {
                    query = query.filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, id_val
                    )));
                }
                MatchCondition::Float(id_val) => {
                    query = query.filter(sql::<Bool>(&format!(
                        "chunk_metadata.metadata @> '{{\"{}\":\"{}\"}}'",
                        key, id_val
                    )));
                }
            }
        }
    };

    if let Some(range) = &filter.range {
        let range_filter = get_range(range.clone())?;
        if let Some(gt) = range_filter.gt {
            query = query.filter(
                sql::<Text>(&format!("chunk_group.metadata->>'{}'", key)).gt(gt.to_string()),
            );
        };

        if let Some(gte) = range_filter.gte {
            query = query.filter(
                sql::<Text>(&format!("chunk_group.metadata->>'{}'", key)).ge(gte.to_string()),
            );
        };

        if let Some(lt) = range_filter.lt {
            query = query.filter(
                sql::<Text>(&format!("chunk_group.metadata->>'{}'", key)).lt(lt.to_string()),
            );
        };

        if let Some(lte) = range_filter.lte {
            query = query.filter(
                sql::<Text>(&format!("chunk_group.metadata->>'{}'", key)).le(lte.to_string()),
            );
        };
    }

    let qdrant_point_ids: Vec<uuid::Uuid> = query
        .load::<uuid::Uuid>(&mut conn)
        .await
        .map_err(|_| ServiceError::BadRequest("Failed to load metadata".to_string()))?;

    let matching_point_ids: Vec<PointId> = qdrant_point_ids
        .iter()
        .map(|uuid| uuid.to_string())
        .collect::<HashSet<String>>()
        .iter()
        .map(|uuid| (*uuid).clone().into())
        .collect::<Vec<PointId>>();

    metadata_filter.must.push(Condition {
        condition_one_of: Some(HasId(HasIdCondition {
            has_id: matching_point_ids,
        })),
    });

    Ok(metadata_filter)
}

#[tracing::instrument(skip(pool))]
pub async fn get_group_tag_set_filter_condition(
    filter: &FieldCondition,
    dataset_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Filter, ServiceError> {
    let mut metadata_filter = Filter::default();

    use crate::data::schema::chunk_group::dsl as chunk_group_columns;
    use crate::data::schema::chunk_group_bookmarks::dsl as chunk_group_bookmarks_columns;
    use crate::data::schema::chunk_metadata::dsl as chunk_metadata_columns;

    let mut conn = pool.get().await.unwrap();

    let mut query =
        chunk_metadata_columns::chunk_metadata
            .left_outer_join(chunk_group_bookmarks_columns::chunk_group_bookmarks.on(
                chunk_metadata_columns::id.eq(chunk_group_bookmarks_columns::chunk_metadata_id),
            ))
            .left_outer_join(
                chunk_group_columns::chunk_group
                    .on(chunk_group_bookmarks_columns::group_id.eq(chunk_group_columns::id)),
            )
            .select(chunk_metadata_columns::qdrant_point_id)
            .filter(chunk_metadata_columns::dataset_id.eq(dataset_id))
            .into_boxed();

    if let Some(matches) = &filter.match_any {
        if let Some(first_val) = matches.get(0) {
            match first_val {
                MatchCondition::Text(string_val) => {
                    query = query
                        .filter(chunk_group_columns::tag_set.contains(vec![string_val.clone()]));
                }
                MatchCondition::Integer(int_val) => {
                    query = query
                        .filter(chunk_group_columns::tag_set.contains(vec![int_val.to_string()]));
                }
                MatchCondition::Float(float_val) => {
                    query = query
                        .filter(chunk_group_columns::tag_set.contains(vec![float_val.to_string()]));
                }
            }
        }

        for match_condition in matches.iter().skip(1) {
            match match_condition {
                MatchCondition::Text(string_val) => {
                    query = query
                        .or_filter(chunk_group_columns::tag_set.contains(vec![string_val.clone()]));
                }
                MatchCondition::Integer(int_val) => {
                    query = query.or_filter(
                        chunk_group_columns::tag_set.contains(vec![int_val.to_string()]),
                    );
                }
                MatchCondition::Float(float_val) => {
                    query = query.or_filter(
                        chunk_group_columns::tag_set.contains(vec![float_val.to_string()]),
                    );
                }
            }
        }
    } else if let Some(matches) = &filter.match_all {
        if let Some(first_val) = matches.get(0) {
            match first_val {
                MatchCondition::Text(string_val) => {
                    query = query
                        .filter(chunk_group_columns::tag_set.contains(vec![string_val.clone()]));
                }
                MatchCondition::Integer(int_val) => {
                    query = query
                        .filter(chunk_group_columns::tag_set.contains(vec![int_val.to_string()]));
                }
                MatchCondition::Float(float_val) => {
                    query = query
                        .filter(chunk_group_columns::tag_set.contains(vec![float_val.to_string()]));
                }
            }
        }

        for match_condition in matches.iter().skip(1) {
            match match_condition {
                MatchCondition::Text(string_val) => {
                    query = query
                        .filter(chunk_group_columns::tag_set.contains(vec![string_val.clone()]));
                }
                MatchCondition::Integer(int_val) => {
                    query = query
                        .filter(chunk_group_columns::tag_set.contains(vec![int_val.to_string()]));
                }
                MatchCondition::Float(float_val) => {
                    query = query
                        .filter(chunk_group_columns::tag_set.contains(vec![float_val.to_string()]));
                }
            }
        }
    }

    if filter.range.is_some() {
        "Range filter not supported for group_tag_set".to_string();
    }

    let qdrant_point_ids: Vec<uuid::Uuid> = query
        .load::<uuid::Uuid>(&mut conn)
        .await
        .map_err(|_| ServiceError::BadRequest("Failed to load metadata".to_string()))?;

    let matching_point_ids: Vec<PointId> = qdrant_point_ids
        .iter()
        .map(|uuid| uuid.to_string())
        .collect::<HashSet<String>>()
        .iter()
        .map(|uuid| (*uuid).clone().into())
        .collect::<Vec<PointId>>();

    metadata_filter.must.push(Condition {
        condition_one_of: Some(HasId(HasIdCondition {
            has_id: matching_point_ids,
        })),
    });

    Ok(metadata_filter)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SearchOverGroupsQueryResult {
    pub search_results: Vec<GroupSearchResults>,
    pub total_chunk_pages: i64,
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip(pool))]
pub async fn retrieve_group_qdrant_points_query(
    vector: VectorType,
    page: u64,
    get_total_pages: bool,
    filters: Option<ChunkFilter>,
    limit: u64,
    score_threshold: Option<f32>,
    group_size: u32,
    parsed_query: ParsedQuery,
    dataset_id: uuid::Uuid,
    pool: web::Data<Pool>,
    config: &DatasetConfiguration,
) -> Result<SearchOverGroupsQueryResult, ServiceError> {
    let page = if page == 0 { 1 } else { page };

    let filter = assemble_qdrant_filter(
        filters.clone(),
        parsed_query.clone().quote_words,
        parsed_query.clone().negated_words.clone(),
        dataset_id,
        pool.clone(),
    )
    .await?;

    let (point_ids, count) = search_over_groups_query(
        page,
        filter.clone(),
        limit,
        score_threshold,
        group_size,
        vector.clone(),
        config.clone(),
        get_total_pages,
    )
    .await?;

    let pages = (count as f64 / limit as f64).ceil() as i64;

    Ok(SearchOverGroupsQueryResult {
        search_results: point_ids,
        total_chunk_pages: pages,
    })
}

#[derive(Debug, Serialize, Deserialize, Queryable)]
pub struct FullTextDocIds {
    pub doc_ids: Option<uuid::Uuid>,
    pub total_count: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema, Default)]
#[schema(title = "V1")]
pub struct GroupScoreChunk {
    pub group_id: uuid::Uuid,
    pub group_name: Option<String>,
    pub group_description: Option<String>,
    pub group_created_at: chrono::NaiveDateTime,
    pub group_updated_at: chrono::NaiveDateTime,
    pub group_tracking_id: Option<String>,
    pub group_metadata: Option<serde_json::Value>,
    pub group_tag_set: Option<Vec<Option<String>>>,
    pub group_dataset_id: uuid::Uuid,
    pub metadata: Vec<ScoreChunkDTO>,
    pub file_id: Option<uuid::Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema, Default)]
#[schema(title = "V2")]
pub struct SearchOverGroupsResults {
    pub group: ChunkGroup,
    pub chunks: Vec<ScoreChunk>,
    pub file_id: Option<uuid::Uuid>,
}

impl From<GroupScoreChunk> for SearchOverGroupsResults {
    fn from(val: GroupScoreChunk) -> Self {
        SearchOverGroupsResults {
            group: ChunkGroup {
                id: val.group_id,
                name: val.group_name.unwrap_or("".to_string()),
                description: val.group_description.unwrap_or("".to_string()),
                created_at: val.group_created_at,
                updated_at: val.group_updated_at,
                dataset_id: val.group_dataset_id,
                tracking_id: val.group_tracking_id,
                metadata: val.group_metadata,
                tag_set: val.group_tag_set,
            },
            chunks: val
                .metadata
                .into_iter()
                .map(|score_chunk| score_chunk.into())
                .collect(),
            file_id: val.file_id,
        }
    }
}

#[derive(Serialize, Deserialize, ToSchema)]
#[schema(title = "V1")]
pub struct DeprecatedSearchOverGroupsResponseBody {
    pub group_chunks: Vec<GroupScoreChunk>,
    pub total_chunk_pages: i64,
}

impl DeprecatedSearchOverGroupsResponseBody {
    pub fn into_v2(self, search_id: uuid::Uuid) -> SearchOverGroupsResponseBody {
        SearchOverGroupsResponseBody {
            id: search_id,
            results: self
                .group_chunks
                .into_iter()
                .map(|chunk| chunk.into())
                .collect(),
            total_pages: self.total_chunk_pages,
        }
    }
}

#[derive(Serialize, Deserialize, ToSchema)]
#[schema(title = "V2")]
pub struct SearchOverGroupsResponseBody {
    pub id: uuid::Uuid,
    pub results: Vec<SearchOverGroupsResults>,
    pub total_pages: i64,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum SearchOverGroupsResponseTypes {
    #[schema(title = "V2")]
    V2(SearchOverGroupsResponseBody),
    #[schema(title = "V1")]
    V1(DeprecatedSearchOverGroupsResponseBody),
}

#[tracing::instrument(skip(pool))]
pub async fn retrieve_chunks_for_groups(
    search_over_groups_query_result: SearchOverGroupsQueryResult,
    data: &SearchOverGroupsReqPayload,
    pool: web::Data<Pool>,
) -> Result<DeprecatedSearchOverGroupsResponseBody, ServiceError> {
    let point_ids = search_over_groups_query_result
        .search_results
        .clone()
        .iter()
        .flat_map(|hit| hit.hits.iter().map(|point| point.point_id).collect_vec())
        .collect_vec();

    let metadata_chunks = match data.slim_chunks.unwrap_or(false)
        && data.search_type != SearchMethod::Hybrid
    {
        true => get_slim_chunks_from_point_ids_query(point_ids, pool.clone()).await?,
        _ => {
            get_chunk_metadatas_and_collided_chunks_from_point_ids_query(point_ids, pool.clone())
                .await?
        }
    };

    let groups = get_groups_from_group_ids_query(
        search_over_groups_query_result
            .search_results
            .iter()
            .map(|group| group.group_id)
            .collect(),
        pool.clone(),
    )
    .await?;

    let group_chunks: Vec<GroupScoreChunk> = search_over_groups_query_result
        .search_results
        .iter()
        .map(|group_search_result| {
            let score_chunks: Vec<ScoreChunkDTO> = group_search_result
                .hits
                .iter()
                .map(|search_result| {
                    let mut chunk: ChunkMetadataTypes =
                        match metadata_chunks.iter().find(|metadata_chunk| {
                            metadata_chunk.metadata().qdrant_point_id == search_result.point_id
                        }) {
                            Some(metadata_chunk) => metadata_chunk.clone(),
                            None => {
                                log::error!(
                                    "Failed to find metadata chunk for point id for chunks with groups: {:?}",
                                    search_result.point_id
                                );
                                sentry::capture_message(
                                    &format!("Failed to find metadata chunk for point id for chunks with groups: {:?}", search_result.point_id),
                                    sentry::Level::Error,
                                );

                                ChunkMetadata {
                                    id: uuid::Uuid::default(),
                                    qdrant_point_id: uuid::Uuid::default(),
                                    created_at: chrono::Utc::now().naive_local(),
                                    updated_at: chrono::Utc::now().naive_local(),
                                    chunk_html: Some("".to_string()),
                                    link: Some("".to_string()),
                                    tag_set: None,
                                    metadata: None,
                                    tracking_id: None,
                                    time_stamp: None,
                                    location: None,
                                    dataset_id: uuid::Uuid::default(),
                                    weight: 1.0,
                                    image_urls: None,
                                    num_value: None
                                }.into()
                            },
                        };

                    let mut highlights: Option<Vec<String>> = None;
                    if data.highlight_results.unwrap_or(true) && !data.slim_chunks.unwrap_or(false) {
                        let (highlighted_chunk, highlighted_snippets) = match data.highlight_strategy {
                            Some(HighlightStrategy::ExactMatch) => {
                                   get_highlights_with_exact_match(
                                        chunk.clone().into(),
                                        data.query.clone(),
                                        data.highlight_threshold,
                                        data.highlight_delimiters.clone().unwrap_or(vec![
                                            ".".to_string(),
                                            "!".to_string(),
                                            "?".to_string(),
                                            "\n".to_string(),
                                            "\t".to_string(),
                                            ",".to_string(),
                                        ]),
                                        data.highlight_max_length,
                                        data.highlight_max_num,
                                        data.highlight_window
                                    )
                                    .unwrap_or((chunk.clone().into(), vec![]))
                            },
                            _ => {
                                   get_highlights(
                                        chunk.clone().into(),
                                        data.query.clone(),
                                        data.highlight_threshold,
                                        data.highlight_delimiters.clone().unwrap_or(vec![
                                            ".".to_string(),
                                            "!".to_string(),
                                            "?".to_string(),
                                            "\n".to_string(),
                                            "\t".to_string(),
                                            ",".to_string(),
                                        ]),
                                        data.highlight_max_length,
                                        data.highlight_max_num,
                                        data.highlight_window,
                                    )
                                    .unwrap_or((chunk.clone().into(), vec![]))
                            },
                        };

                        highlights = Some(highlighted_snippets);

                        match chunk {
                            ChunkMetadataTypes::Metadata(_) => chunk = highlighted_chunk.into(),
                            ChunkMetadataTypes::Content(_) => {
                                chunk =
                                    <ChunkMetadata as Into<ContentChunkMetadata>>::into(highlighted_chunk)
                                        .into()
                            }
                            _ => unreachable!(
                                "If slim_chunks is false, then chunk must be either Metadata or Content"
                            ),
                        }
                    }


                    ScoreChunkDTO {
                        metadata: vec![chunk],
                        highlights,
                        score: search_result.score.into(),
                    }
                })
                .sorted_by(|a, b| b.score.partial_cmp(&a.score).unwrap())
                .collect_vec();

            let group_data = groups.iter().find(|group| group.id == group_search_result.group_id);

            GroupScoreChunk {
                group_id: group_search_result.group_id,
                group_name: group_data.map(|group| group.name.clone()),
                group_description: group_data.map(|group| group.description.clone()),
                group_created_at: group_data.map(|group| group.created_at).unwrap_or_default(),
                group_updated_at: group_data.map(|group| group.updated_at).unwrap_or_default(),
                group_tracking_id: group_data.and_then(|group| group.tracking_id.clone()),
                group_metadata: group_data.and_then(|group| group.metadata.clone()),
                group_tag_set: group_data.and_then(|group| group.tag_set.clone()),
                group_dataset_id: group_data.map(|group| group.dataset_id).unwrap_or_default(),
                metadata: score_chunks,
                file_id: group_data.and_then(|group| group.file_id),
            }
        })
        .collect_vec();

    Ok(DeprecatedSearchOverGroupsResponseBody {
        group_chunks,
        total_chunk_pages: search_over_groups_query_result.total_chunk_pages,
    })
}

#[tracing::instrument(skip(pool))]
pub async fn get_metadata_from_groups(
    search_over_groups_query_result: SearchOverGroupsQueryResult,
    slim_chunks: Option<bool>,
    pool: web::Data<Pool>,
) -> Result<Vec<GroupScoreChunk>, actix_web::Error> {
    let point_ids = search_over_groups_query_result
        .search_results
        .iter()
        .flat_map(|hit| hit.hits.iter().map(|point| point.point_id).collect_vec())
        .collect_vec();

    let chunk_metadatas = match slim_chunks {
        Some(true) => get_slim_chunks_from_point_ids_query(point_ids, pool.clone()).await?,
        _ => {
            get_chunk_metadatas_and_collided_chunks_from_point_ids_query(point_ids, pool.clone())
                .await?
        }
    };

    let groups = get_groups_from_group_ids_query(
        search_over_groups_query_result
            .search_results
            .iter()
            .map(|group| group.group_id)
            .collect(),
        pool.clone(),
    )
    .await?;

    let group_chunks: Vec<GroupScoreChunk> = search_over_groups_query_result
        .search_results
        .iter()
        .map(|group_search_result| {
            let score_chunk: Vec<ScoreChunkDTO> = group_search_result
                .hits
                .iter()
                .map(|search_result| {
                    let chunk: ChunkMetadataTypes =
                        match chunk_metadatas.iter().find(|metadata_chunk| {
                            metadata_chunk.metadata().qdrant_point_id == search_result.point_id
                        }) {
                            Some(metadata_chunk) => metadata_chunk.clone(),
                            None => {
                                log::error!(
                                    "Failed to find metadata chunk for point id for metadata with groups: {:?}",
                                    search_result.point_id
                                );
                                sentry::capture_message(
                                    &format!("Failed to find metadata chunk for point id for metadata with groups: {:?}", search_result.point_id),
                                    sentry::Level::Error,
                                );

                                ChunkMetadata {
                                    id: uuid::Uuid::default(),
                                    qdrant_point_id: uuid::Uuid::default(),
                                    created_at: chrono::Utc::now().naive_local(),
                                    updated_at: chrono::Utc::now().naive_local(),
                                    chunk_html: Some("".to_string()),
                                    link: None,
                                    tag_set: None,
                                    metadata: None,
                                    tracking_id: None,
                                    time_stamp: None,
                                    location: None,
                                    dataset_id: uuid::Uuid::default(),
                                    weight: 1.0,
                                    image_urls: None,
                                    num_value: None
                                }.into()
                            },
                        };

                    ScoreChunkDTO {
                        metadata: vec![chunk],
                        highlights: None,
                        score: search_result.score.into(),
                    }
                })
                .collect_vec();

            let group_data = groups.iter().find(|group| group.id == group_search_result.group_id);

            GroupScoreChunk {
                group_id: group_search_result.group_id,
                group_name: group_data.map(|group| group.name.clone()),
                group_description: group_data.map(|group| group.description.clone()),
                group_created_at: group_data.map(|group| group.created_at).unwrap_or_default(),
                group_updated_at: group_data.map(|group| group.updated_at).unwrap_or_default(),
                group_tracking_id: group_data.and_then(|group| group.tracking_id.clone()),
                group_metadata: group_data.and_then(|group| group.metadata.clone()),
                group_tag_set: group_data.and_then(|group| group.tag_set.clone()),
                group_dataset_id: group_data.map(|group| group.dataset_id).unwrap_or_default(),
                metadata: score_chunk,
                file_id: group_data.and_then(|group| group.file_id),
            }
        })
        .collect_vec();

    Ok(group_chunks)
}

/// Retrieve chunks from point ids, DOES NOT GUARD AGAINST DATASET ACCESS PERMISSIONS
#[tracing::instrument(skip(pool))]
pub async fn retrieve_chunks_from_point_ids(
    search_chunk_query_results: SearchChunkQueryResult,
    data: &SearchChunksReqPayload,
    pool: web::Data<Pool>,
) -> Result<SearchChunkQueryResponseBody, actix_web::Error> {
    let parent_span = sentry::configure_scope(|scope| scope.get_span());
    let transaction: sentry::TransactionOrSpan = match &parent_span {
        Some(parent) => parent
            .start_child(
                "Retrieve Chunks from point IDS",
                "Retrieve Chunks from point IDS",
            )
            .into(),
        None => {
            let ctx = sentry::TransactionContext::new(
                "Retrieve Chunks from point IDS",
                "Retrieve Chunks from point IDS",
            );
            sentry::start_transaction(ctx).into()
        }
    };
    sentry::configure_scope(|scope| scope.set_span(Some(transaction.clone())));

    let point_ids = search_chunk_query_results
        .search_results
        .iter()
        .map(|point| point.point_id)
        .collect::<Vec<_>>();

    let metadata_chunks =
        if data.slim_chunks.unwrap_or(false) && data.search_type != SearchMethod::Hybrid {
            get_slim_chunks_from_point_ids_query(point_ids, pool.clone()).await?
        } else if data.content_only.unwrap_or(false) {
            get_content_chunk_from_point_ids_query(point_ids, pool.clone()).await?
        } else {
            get_chunk_metadatas_and_collided_chunks_from_point_ids_query(point_ids, pool.clone())
                .await?
        };

    let score_chunks: Vec<ScoreChunkDTO> = search_chunk_query_results
        .search_results
        .iter()
        .map(|search_result| {
            let mut chunk: ChunkMetadataTypes =
                match metadata_chunks.iter().find(|metadata_chunk| {
                    metadata_chunk.metadata().qdrant_point_id == search_result.point_id
                }) {
                    Some(metadata_chunk) => metadata_chunk.clone(),
                    None => {
                        log::error!(
                            "Failed to find metadata chunk from point ids: {:?}",
                            search_result.point_id
                        );
                        sentry::capture_message(
                            &format!(
                                "Failed to find metadata chunk from point ids: {:?}",
                                search_result.point_id
                            ),
                            sentry::Level::Error,
                        );

                        ChunkMetadata {
                            id: uuid::Uuid::default(),
                            qdrant_point_id: uuid::Uuid::default(),
                            created_at: chrono::Utc::now().naive_local(),
                            updated_at: chrono::Utc::now().naive_local(),
                            chunk_html: Some("".to_string()),
                            link: None,
                            tag_set: None,
                            metadata: None,
                            tracking_id: None,
                            time_stamp: None,
                            location: None,
                            dataset_id: uuid::Uuid::default(),
                            weight: 1.0,
                            image_urls: None,
                            num_value: None,
                        }
                        .into()
                    }
                };

            let mut highlights: Option<Vec<String>> = None;
            if data.highlight_results.unwrap_or(true) && !data.slim_chunks.unwrap_or(false) {
                let (highlighted_chunk, highlighted_snippets) = match data.highlight_strategy {
                    Some(HighlightStrategy::ExactMatch) => get_highlights_with_exact_match(
                        chunk.clone().into(),
                        data.query.clone(),
                        data.highlight_threshold,
                        data.highlight_delimiters.clone().unwrap_or(vec![
                            ".".to_string(),
                            "!".to_string(),
                            "?".to_string(),
                            "\n".to_string(),
                            "\t".to_string(),
                            ",".to_string(),
                        ]),
                        data.highlight_max_length,
                        data.highlight_max_num,
                        data.highlight_window,
                    )
                    .unwrap_or((chunk.clone().into(), vec![])),
                    _ => get_highlights(
                        chunk.clone().into(),
                        data.query.clone(),
                        data.highlight_threshold,
                        data.highlight_delimiters.clone().unwrap_or(vec![
                            ".".to_string(),
                            "!".to_string(),
                            "?".to_string(),
                            "\n".to_string(),
                            "\t".to_string(),
                            ",".to_string(),
                        ]),
                        data.highlight_max_length,
                        data.highlight_max_num,
                        data.highlight_window,
                    )
                    .unwrap_or((chunk.clone().into(), vec![])),
                };

                highlights = Some(highlighted_snippets);

                match chunk {
                    ChunkMetadataTypes::Metadata(_) => chunk = highlighted_chunk.into(),
                    ChunkMetadataTypes::Content(_) => {
                        chunk =
                            <ChunkMetadata as Into<ContentChunkMetadata>>::into(highlighted_chunk)
                                .into()
                    }
                    _ => unreachable!(
                        "If slim_chunks is false, then chunk must be either Metadata or Content"
                    ),
                }
            }

            ScoreChunkDTO {
                metadata: vec![chunk],
                highlights,
                score: search_result.score.into(),
            }
        })
        .collect();

    transaction.finish();

    Ok(SearchChunkQueryResponseBody {
        score_chunks,
        total_chunk_pages: search_chunk_query_results.total_chunk_pages,
    })
}

#[tracing::instrument]
pub fn rerank_chunks(
    chunks: Vec<ScoreChunkDTO>,
    sort_by: Option<SortByField>,
    tag_weights: Option<HashMap<String, f32>>,
    use_weights: Option<bool>,
    query_location: Option<GeoInfoWithBias>,
) -> Vec<ScoreChunkDTO> {
    let mut reranked_chunks = Vec::new();
    if use_weights.unwrap_or(true) {
        chunks.into_iter().for_each(|mut chunk| {
            if chunk.metadata[0].metadata().weight == 0.0 {
                chunk.score *= 1.0;
            } else {
                chunk.score *= chunk.metadata[0].metadata().weight;
            }
            reranked_chunks.push(chunk);
        });
    } else {
        reranked_chunks = chunks;
    }

    if query_location.is_some() && query_location.unwrap().bias > 0.0 {
        let info_with_bias = query_location.unwrap();
        let query_location = info_with_bias.location;
        let location_bias = info_with_bias.bias;
        let distances = reranked_chunks
            .iter()
            .filter_map(|chunk| chunk.metadata[0].metadata().location)
            .map(|location| query_location.haversine_distance_to(&location));
        let max_distance = distances.clone().max_by(|a, b| a.partial_cmp(b).unwrap());
        let min_distance = distances.clone().min_by(|a, b| a.partial_cmp(b).unwrap());
        let max_score = reranked_chunks
            .iter()
            .map(|chunk| chunk.score)
            .max_by(|a, b| a.partial_cmp(b).unwrap());
        let min_score = reranked_chunks
            .iter()
            .map(|chunk| chunk.score)
            .min_by(|a, b| a.partial_cmp(b).unwrap());

        reranked_chunks = reranked_chunks
            .iter_mut()
            .map(|chunk| {
                let normalized_distance = (chunk.metadata[0]
                    .metadata()
                    .location
                    .map(|location| query_location.haversine_distance_to(&location))
                    .unwrap_or(0.0)
                    - min_distance.unwrap_or(0.0))
                    / (max_distance.unwrap_or(1.0) - min_distance.unwrap_or(0.0));
                let normalized_chunk_score = (chunk.score - min_score.unwrap_or(0.0))
                    / (max_score.unwrap_or(1.0) - min_score.unwrap_or(0.0));
                chunk.score = (normalized_chunk_score * (1.0 - location_bias))
                    + (location_bias * (1.0 - normalized_distance));
                chunk.clone()
            })
            .collect::<Vec<ScoreChunkDTO>>();
    }

    if let Some(tag_weights) = tag_weights {
        reranked_chunks = reranked_chunks
            .iter_mut()
            .map(|chunk| {
                let mut tag_score = 1.0;
                for (tag, weight) in tag_weights.iter() {
                    if let Some(metadata) = chunk.metadata.get(0) {
                        if let Some(metadata_tags) = metadata.metadata().tag_set {
                            if metadata_tags.contains(&Some(tag.clone())) {
                                tag_score *= weight;
                            }
                        }
                    }
                }
                chunk.score *= tag_score as f64;
                chunk.clone()
            })
            .collect::<Vec<ScoreChunkDTO>>();
    }
    reranked_chunks.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    if let Some(sort_by) = sort_by {
        match sort_by.field.as_str() {
            "time_stamp" => {
                if sort_by.direction.is_some_and(|dir| dir == SortOrder::Asc) {
                    reranked_chunks.sort_by(|a, b| {
                        a.metadata[0]
                            .metadata()
                            .time_stamp
                            .partial_cmp(&b.metadata[0].metadata().time_stamp)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                } else {
                    reranked_chunks.sort_by(|a, b| {
                        b.metadata[0]
                            .metadata()
                            .time_stamp
                            .partial_cmp(&a.metadata[0].metadata().time_stamp)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                }
            }
            "num_value" => {
                if sort_by.direction.is_some_and(|dir| dir == SortOrder::Asc) {
                    reranked_chunks.sort_by(|a, b| {
                        a.metadata[0]
                            .metadata()
                            .num_value
                            .partial_cmp(&b.metadata[0].metadata().num_value)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                } else {
                    reranked_chunks.sort_by(|a, b| {
                        b.metadata[0]
                            .metadata()
                            .num_value
                            .partial_cmp(&a.metadata[0].metadata().num_value)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                }
            }
            _ => {
                log::error!("Unsupported sort_by field: {}", sort_by.field);
                sentry::capture_message(
                    &format!("Unsupported sort_by field: {}", sort_by.field),
                    sentry::Level::Error,
                );
            }
        }
    }

    reranked_chunks
}

async fn get_qdrant_vector(
    data: SearchChunksReqPayload,
    parsed_query: ParsedQuery,
    config: &DatasetConfiguration,
) -> Result<VectorType, ServiceError> {
    match data.search_type {
        SearchMethod::Semantic => {
            if !config.SEMANTIC_ENABLED {
                return Err(ServiceError::BadRequest(
                    "Semantic search is not enabled for this dataset".to_string(),
                ));
            }
            let embedding_vector =
                get_dense_vector(data.query.clone(), None, "query", config.clone()).await?;
            Ok(VectorType::Dense(embedding_vector))
        }
        SearchMethod::BM25 => {
            if std::env::var("BM25_ACTIVE").unwrap_or("false".to_string()) != "true" {
                return Err(ServiceError::BadRequest(
                    "BM25 search is not enabled for this dataset".to_string(),
                ));
            }
            let sparse_vectors = get_bm25_embeddings(
                vec![(parsed_query.query.clone(), None)],
                config.BM25_AVG_LEN,
                config.BM25_B,
                config.BM25_K,
            );
            let sparse_vector = sparse_vectors.get(0).expect("Vector will always exist");

            Ok(VectorType::BM25Sparse(sparse_vector.clone()))
        }
        SearchMethod::FullText => {
            if !config.FULLTEXT_ENABLED {
                return Err(ServiceError::BadRequest(
                    "Full text search is not enabled for this dataset".to_string(),
                ));
            }

            let sparse_vector = get_sparse_vector(parsed_query.query.clone(), "query").await?;
            Ok(VectorType::SpladeSparse(sparse_vector))
        }
        SearchMethod::Hybrid => Err(ServiceError::BadRequest(
            "Hybrid search is not supported for this endpoint".to_string(),
        )),
    }
}

#[tracing::instrument(skip(timer, pool))]
pub async fn search_chunks_query(
    data: SearchChunksReqPayload,
    parsed_query: ParsedQuery,
    pool: web::Data<Pool>,
    dataset: Dataset,
    config: &DatasetConfiguration,
    timer: &mut Timer,
) -> Result<SearchChunkQueryResponseBody, actix_web::Error> {
    let parent_span = sentry::configure_scope(|scope| scope.get_span());
    let transaction: sentry::TransactionOrSpan = match &parent_span {
        Some(parent) => parent
            .start_child("semantic search", "Search Semantic Chunks")
            .into(),
        None => {
            let ctx = sentry::TransactionContext::new("semantic search", "Search Semantic Chunks");
            sentry::start_transaction(ctx).into()
        }
    };
    sentry::configure_scope(|scope| scope.set_span(Some(transaction.clone())));

    timer.add("start to create dense embedding vector");

    timer.add("computed dense embedding");

    let vector = get_qdrant_vector(data.clone(), parsed_query.clone(), config).await?;

    let (sort_by, rerank_by) = match data.sort_by.clone() {
        Some(sort_by) => match sort_by {
            QdrantSortBy::Field(field) => (Some(field.clone()), None),
            QdrantSortBy::SearchType(search_type) => (None, Some(search_type)),
        },
        None => (None, None),
    };

    let qdrant_query = RetrievePointQuery {
        vector,
        score_threshold: if rerank_by.clone().map(|r| r.rerank_type)
            == Some(ReRankOptions::CrossEncoder)
        {
            None
        } else {
            data.score_threshold
        },
        limit: data.page_size.unwrap_or(10),
        sort_by: sort_by.clone(),
        rerank_by: rerank_by.clone(),
        filter: data.filters.clone(),
    }
    .into_qdrant_query(parsed_query, dataset.id, None, config, pool.clone())
    .await?;

    let search_chunk_query_results = retrieve_qdrant_points_query(
        vec![qdrant_query],
        data.page.unwrap_or(1),
        data.get_total_pages.unwrap_or(false),
        config,
    )
    .await?;

    timer.add("fetched from qdrant");

    let mut result_chunks =
        retrieve_chunks_from_point_ids(search_chunk_query_results, &data, pool.clone()).await?;

    timer.add("fetched from postgres");

    let rerank_chunks_input = if let Some(rerank_by) = rerank_by {
        match rerank_by.rerank_type {
            ReRankOptions::CrossEncoder => {
                let mut cross_encoder_results = cross_encoder(
                    data.query.clone(),
                    data.page_size.unwrap_or(10),
                    result_chunks.score_chunks,
                    config,
                )
                .await?;

                if let Some(score_threshold) = data.score_threshold {
                    cross_encoder_results.retain(|chunk| chunk.score >= score_threshold.into());
                }

                cross_encoder_results
            }
            _ => result_chunks.score_chunks,
        }
    } else {
        result_chunks.score_chunks
    };

    result_chunks.score_chunks = rerank_chunks(
        rerank_chunks_input,
        sort_by,
        data.tag_weights,
        data.use_weights,
        data.location_bias,
    );

    timer.add("reranking");
    transaction.finish();

    Ok(result_chunks)
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip(timer, pool))]
pub async fn search_hybrid_chunks(
    data: SearchChunksReqPayload,
    parsed_query: ParsedQuery,
    pool: web::Data<Pool>,
    dataset: Dataset,
    config: &DatasetConfiguration,
    timer: &mut Timer,
) -> Result<SearchChunkQueryResponseBody, actix_web::Error> {
    let parent_span = sentry::configure_scope(|scope| scope.get_span());
    let transaction: sentry::TransactionOrSpan = match &parent_span {
        Some(parent) => parent
            .start_child("hybrid search", "Search Hybrid Chunks")
            .into(),
        None => {
            let ctx = sentry::TransactionContext::new("hybrid search", "Search Hybrid Chunks");
            sentry::start_transaction(ctx).into()
        }
    };
    sentry::configure_scope(|scope| scope.set_span(Some(transaction.clone())));

    let dataset_config = DatasetConfiguration::from_json(dataset.server_configuration.clone());

    let dense_vector_future =
        get_dense_vector(data.query.clone(), None, "query", dataset_config.clone());

    let sparse_vector_future = get_sparse_vector(parsed_query.query.clone(), "query");

    let (dense_vector, sparse_vector) =
        futures::try_join!(dense_vector_future, sparse_vector_future)?;

    timer.add("computed sparse and dense embeddings");

    let (sort_by, rerank_by) = match data.sort_by.clone() {
        Some(sort_by) => match sort_by {
            QdrantSortBy::Field(field) => (Some(field.clone()), None),
            QdrantSortBy::SearchType(search_type) => (None, Some(search_type)),
        },
        None => (None, None),
    };

    let qdrant_queries = vec![
        RetrievePointQuery {
            vector: VectorType::Dense(dense_vector),
            score_threshold: None,
            sort_by: sort_by.clone(),
            rerank_by: rerank_by.clone(),
            limit: data.page_size.unwrap_or(10),
            filter: data.filters.clone(),
        }
        .into_qdrant_query(parsed_query.clone(), dataset.id, None, config, pool.clone())
        .await?,
        RetrievePointQuery {
            vector: VectorType::SpladeSparse(sparse_vector),
            score_threshold: None,
            sort_by: sort_by.clone(),
            rerank_by: rerank_by.clone(),
            limit: data.page_size.unwrap_or(10),
            filter: data.filters.clone(),
        }
        .into_qdrant_query(parsed_query.clone(), dataset.id, None, config, pool.clone())
        .await?,
    ];

    let search_chunk_query_results = retrieve_qdrant_points_query(
        qdrant_queries,
        data.page.unwrap_or(1),
        data.get_total_pages.unwrap_or(false),
        config,
    )
    .await?;

    timer.add("fetched point_ids from qdrant");

    let result_chunks =
        retrieve_chunks_from_point_ids(search_chunk_query_results, &data, pool.clone()).await?;

    timer.add("fetched metadata from postgres");

    let mut reranked_chunks = {
        let mut reranked_chunks = {
            let mut cross_encoder_results = cross_encoder(
                data.query.clone(),
                data.page_size.unwrap_or(10),
                result_chunks.score_chunks,
                config,
            )
            .await?;

            if let Some(score_threshold) = data.score_threshold {
                cross_encoder_results.retain(|chunk| chunk.score >= score_threshold.into());
            }

            rerank_chunks(
                cross_encoder_results,
                sort_by,
                data.tag_weights,
                data.use_weights,
                data.location_bias,
            )
        };

        reranked_chunks.truncate(data.page_size.unwrap_or(10) as usize);

        timer.add("reranking");

        SearchChunkQueryResponseBody {
            score_chunks: reranked_chunks,
            total_chunk_pages: result_chunks.total_chunk_pages,
        }
    };

    if data.slim_chunks.unwrap_or(false) {
        reranked_chunks.score_chunks = reranked_chunks
            .score_chunks
            .into_iter()
            .map(|score_chunk| ScoreChunkDTO {
                metadata: score_chunk
                    .metadata
                    .into_iter()
                    .map(|metadata| {
                        let slim_chunk = SlimChunkMetadata::from(metadata.metadata());

                        match metadata {
                            ChunkMetadataTypes::Metadata(_) => slim_chunk.into(),
                            ChunkMetadataTypes::Content(_) => slim_chunk.into(),
                            ChunkMetadataTypes::ID(_) => metadata,
                        }
                    })
                    .collect(),
                highlights: score_chunk.highlights,
                score: score_chunk.score,
            })
            .collect();
    }

    transaction.finish();
    Ok(reranked_chunks)
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip(pool))]
pub async fn search_groups_query(
    data: SearchWithinGroupReqPayload,
    parsed_query: ParsedQuery,
    group: ChunkGroupAndFileId,
    pool: web::Data<Pool>,
    dataset: Dataset,
    config: &DatasetConfiguration,
) -> Result<SearchWithinGroupResults, actix_web::Error> {
    let vector = get_qdrant_vector(data.clone().into(), parsed_query.clone(), config).await?;

    let (sort_by, rerank_by) = match data.sort_by.clone() {
        Some(sort_by) => match sort_by {
            QdrantSortBy::Field(field) => (Some(field.clone()), None),
            QdrantSortBy::SearchType(search_type) => (None, Some(search_type)),
        },
        None => (None, None),
    };

    let qdrant_query = RetrievePointQuery {
        vector,
        score_threshold: if rerank_by.clone().map(|r| r.rerank_type)
            == Some(ReRankOptions::CrossEncoder)
        {
            None
        } else {
            data.score_threshold
        },
        limit: data.page_size.unwrap_or(10),
        sort_by: sort_by.clone(),
        rerank_by: rerank_by.clone(),
        filter: data.filters.clone(),
    }
    .into_qdrant_query(parsed_query, dataset.id, None, config, pool.clone())
    .await?;

    let search_semantic_chunk_query_results = retrieve_qdrant_points_query(
        vec![qdrant_query],
        data.page.unwrap_or(1),
        data.get_total_pages.unwrap_or(false),
        config,
    )
    .await?;

    let mut result_chunks = retrieve_chunks_from_point_ids(
        search_semantic_chunk_query_results,
        &web::Json(data.clone().into()),
        pool.clone(),
    )
    .await?;
    let rerank_chunks_input = match data.rerank_by {
        Some(ReRankOptions::CrossEncoder) => {
            let mut cross_encoder_results = cross_encoder(
                data.query.clone(),
                data.page_size.unwrap_or(10),
                result_chunks.score_chunks,
                config,
            )
            .await?;

            if let Some(score_threshold) = data.score_threshold {
                cross_encoder_results.retain(|chunk| chunk.score >= score_threshold.into());
            }

            cross_encoder_results
        }
        _ => result_chunks.score_chunks,
    };

    result_chunks.score_chunks = rerank_chunks(
        rerank_chunks_input,
        sort_by,
        data.tag_weights,
        data.use_weights,
        data.location_bias,
    );

    Ok(SearchWithinGroupResults {
        bookmarks: result_chunks.score_chunks,
        group,
        total_pages: result_chunks.total_chunk_pages,
    })
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip(pool))]
pub async fn search_hybrid_groups(
    data: SearchWithinGroupReqPayload,
    parsed_query: ParsedQuery,
    group: ChunkGroupAndFileId,
    pool: web::Data<Pool>,
    dataset: Dataset,
    config: &DatasetConfiguration,
) -> Result<SearchWithinGroupResults, actix_web::Error> {
    let dataset_config = DatasetConfiguration::from_json(dataset.server_configuration.clone());

    let dense_vector_future =
        get_dense_vector(data.query.clone(), None, "query", dataset_config.clone());

    let sparse_vector_future = get_sparse_vector(parsed_query.query.clone(), "query");

    let (dense_vector, sparse_vector) =
        futures::try_join!(dense_vector_future, sparse_vector_future)?;

    let (sort_by, rerank_by) = match data.sort_by.clone() {
        Some(sort_by) => match sort_by {
            QdrantSortBy::Field(field) => (Some(field.clone()), None),
            QdrantSortBy::SearchType(search_type) => (None, Some(search_type)),
        },
        None => (None, None),
    };

    let qdrant_queries = vec![
        RetrievePointQuery {
            vector: VectorType::Dense(dense_vector),
            score_threshold: None,
            sort_by: sort_by.clone(),
            rerank_by: rerank_by.clone(),
            limit: data.page_size.unwrap_or(10),
            filter: data.filters.clone(),
        }
        .into_qdrant_query(
            parsed_query.clone(),
            dataset.id,
            Some(group.id),
            config,
            pool.clone(),
        )
        .await?,
        RetrievePointQuery {
            vector: VectorType::SpladeSparse(sparse_vector),
            score_threshold: None,
            sort_by: sort_by.clone(),
            rerank_by: rerank_by.clone(),
            limit: data.page_size.unwrap_or(10),
            filter: data.filters.clone(),
        }
        .into_qdrant_query(
            parsed_query.clone(),
            dataset.id,
            Some(group.id),
            config,
            pool.clone(),
        )
        .await?,
    ];

    let mut qdrant_results = retrieve_qdrant_points_query(
        qdrant_queries,
        data.page.unwrap_or(1),
        data.get_total_pages.unwrap_or(false),
        config,
    )
    .await?;

    qdrant_results.search_results = qdrant_results
        .search_results
        .iter()
        .unique_by(|chunk| chunk.point_id)
        .cloned()
        .collect();

    let result_chunks = retrieve_chunks_from_point_ids(
        qdrant_results,
        &web::Json(data.clone().into()),
        pool.clone(),
    )
    .await?;

    let reranked_chunks = {
        let mut reranked_chunks = if result_chunks.score_chunks.len() > 20 {
            let split_results = result_chunks
                .score_chunks
                .chunks(20)
                .map(|chunk| chunk.to_vec())
                .collect::<Vec<Vec<ScoreChunkDTO>>>();

            let cross_encoder_results = cross_encoder(
                data.query.clone(),
                data.page_size.unwrap_or(10),
                split_results
                    .get(0)
                    .expect("Split results must exist")
                    .to_vec(),
                config,
            )
            .await?;
            let score_chunks = rerank_chunks(
                cross_encoder_results,
                sort_by,
                data.tag_weights,
                data.use_weights,
                data.location_bias,
            );

            score_chunks
                .iter()
                .chain(split_results.get(1).unwrap().iter())
                .cloned()
                .collect::<Vec<ScoreChunkDTO>>()
        } else {
            let cross_encoder_results = cross_encoder(
                data.query.clone(),
                data.page_size.unwrap_or(10),
                result_chunks.score_chunks.clone(),
                config,
            )
            .await?;

            rerank_chunks(
                cross_encoder_results,
                sort_by,
                data.tag_weights,
                data.use_weights,
                data.location_bias,
            )
        };

        if let Some(score_threshold) = data.score_threshold {
            reranked_chunks.retain(|chunk| chunk.score >= score_threshold.into());
        }

        SearchChunkQueryResponseBody {
            score_chunks: reranked_chunks,
            total_chunk_pages: result_chunks.total_chunk_pages,
        }
    };

    Ok(SearchWithinGroupResults {
        bookmarks: reranked_chunks.score_chunks,
        group,
        total_pages: result_chunks.total_chunk_pages,
    })
}

#[tracing::instrument(skip(timer, pool))]
pub async fn semantic_search_over_groups(
    data: SearchOverGroupsReqPayload,
    parsed_query: ParsedQuery,
    pool: web::Data<Pool>,
    dataset: Dataset,
    config: &DatasetConfiguration,
    timer: &mut Timer,
) -> Result<DeprecatedSearchOverGroupsResponseBody, actix_web::Error> {
    let dataset_config = DatasetConfiguration::from_json(dataset.server_configuration.clone());

    timer.add("start to create dense embedding vector");

    let embedding_vector =
        get_dense_vector(data.query.clone(), None, "query", dataset_config.clone()).await?;

    timer.add("computed dense embedding");

    let search_over_groups_qdrant_result = retrieve_group_qdrant_points_query(
        VectorType::Dense(embedding_vector),
        data.page.unwrap_or(1),
        data.get_total_pages.unwrap_or(false),
        data.filters.clone(),
        data.page_size.unwrap_or(10),
        data.score_threshold,
        data.group_size.unwrap_or(3),
        parsed_query,
        dataset.id,
        pool.clone(),
        config,
    )
    .await?;

    timer.add("fetched from qdrant");

    let mut result_chunks = retrieve_chunks_for_groups(
        search_over_groups_qdrant_result.clone(),
        &data,
        pool.clone(),
    )
    .await?;

    result_chunks.group_chunks = search_over_groups_qdrant_result
        .search_results
        .iter()
        .filter_map(|search_result| {
            result_chunks
                .group_chunks
                .iter()
                .find(|group| group.group_id == search_result.group_id)
                .cloned()
        })
        .collect();

    timer.add("fetched from postgres");

    //TODO: rerank for groups

    Ok(result_chunks)
}

#[tracing::instrument(skip(timer, pool))]
pub async fn full_text_search_over_groups(
    data: SearchOverGroupsReqPayload,
    parsed_query: ParsedQuery,
    pool: web::Data<Pool>,
    dataset: Dataset,
    config: &DatasetConfiguration,
    timer: &mut Timer,
) -> Result<DeprecatedSearchOverGroupsResponseBody, actix_web::Error> {
    timer.add("start to get sparse vector");

    let sparse_vector = get_sparse_vector(parsed_query.query.clone(), "query")
        .await
        .map_err(|_| ServiceError::BadRequest("Failed to get splade query embedding".into()))?;

    timer.add("computed sparse vector");

    let search_over_groups_qdrant_result = retrieve_group_qdrant_points_query(
        VectorType::SpladeSparse(sparse_vector),
        data.page.unwrap_or(1),
        data.get_total_pages.unwrap_or(false),
        data.filters.clone(),
        data.page_size.unwrap_or(10),
        data.score_threshold,
        data.group_size.unwrap_or(3),
        parsed_query,
        dataset.id,
        pool.clone(),
        config,
    )
    .await?;

    timer.add("fetched from qdrant");

    let mut result_groups_with_chunk_hits = retrieve_chunks_for_groups(
        search_over_groups_qdrant_result.clone(),
        &data,
        pool.clone(),
    )
    .await?;

    result_groups_with_chunk_hits.group_chunks = search_over_groups_qdrant_result
        .search_results
        .iter()
        .filter_map(|search_result| {
            result_groups_with_chunk_hits
                .group_chunks
                .iter()
                .find(|group| group.group_id == search_result.group_id)
                .cloned()
        })
        .collect();

    timer.add("fetched from postgres");

    //TODO: rerank for groups

    Ok(result_groups_with_chunk_hits)
}

async fn cross_encoder_for_groups(
    query: String,
    page_size: u64,
    groups_chunks: Vec<GroupScoreChunk>,
    config: &DatasetConfiguration,
) -> Result<Vec<GroupScoreChunk>, actix_web::Error> {
    let score_chunks = groups_chunks
        .iter()
        .map(|group| {
            group
                .metadata
                .clone()
                .get(0)
                .expect("Metadata should have one element")
                .clone()
        })
        .collect_vec();

    let cross_encoder_results = cross_encoder(query, page_size, score_chunks, config).await?;
    let mut group_results = cross_encoder_results
        .into_iter()
        .map(|score_chunk| {
            let mut group = groups_chunks
                .iter()
                .find(|group| {
                    group.metadata.iter().any(|chunk| {
                        chunk.metadata[0].metadata().id == score_chunk.metadata[0].metadata().id
                    })
                })
                .expect("Group not found")
                .clone();
            group.metadata[0].score = score_chunk.score;
            group
        })
        .collect_vec();

    group_results.dedup_by(|a, b| a.group_id == b.group_id);

    group_results.sort_by(|a, b| {
        b.metadata[0]
            .score
            .partial_cmp(&a.metadata[0].score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    group_results = group_results
        .into_iter()
        .map(|mut group| {
            group.metadata = group
                .metadata
                .into_iter()
                .sorted_by(|a, b| b.score.partial_cmp(&a.score).unwrap())
                .collect();
            group
        })
        .collect_vec();

    Ok(group_results)
}

#[tracing::instrument(skip(timer, pool))]
pub async fn hybrid_search_over_groups(
    data: SearchOverGroupsReqPayload,
    parsed_query: ParsedQuery,
    pool: web::Data<Pool>,
    dataset: Dataset,
    config: &DatasetConfiguration,
    timer: &mut Timer,
) -> Result<DeprecatedSearchOverGroupsResponseBody, actix_web::Error> {
    let dataset_config = DatasetConfiguration::from_json(dataset.server_configuration.clone());

    timer.add("start to create dense embedding vector and sparse vector");

    let dense_embedding_vectors_future =
        get_dense_vector(data.query.clone(), None, "query", dataset_config.clone());

    let sparse_embedding_vector_future = get_sparse_vector(data.query.clone(), "query");

    let (dense_vector, sparse_vector) = futures::try_join!(
        dense_embedding_vectors_future,
        sparse_embedding_vector_future
    )?;

    timer.add("computed dense embedding");

    let semantic_future = retrieve_group_qdrant_points_query(
        VectorType::Dense(dense_vector),
        data.page.unwrap_or(1),
        data.get_total_pages.unwrap_or(false),
        data.filters.clone(),
        data.page_size.unwrap_or(10),
        None,
        data.group_size.unwrap_or(3),
        parsed_query.clone(),
        dataset.id,
        pool.clone(),
        config,
    );

    let full_text_future = retrieve_group_qdrant_points_query(
        VectorType::SpladeSparse(sparse_vector),
        data.page.unwrap_or(1),
        data.get_total_pages.unwrap_or(false),
        data.filters.clone(),
        data.page_size.unwrap_or(10),
        None,
        data.group_size.unwrap_or(3),
        parsed_query.clone(),
        dataset.id,
        pool.clone(),
        config,
    );

    let (semantic_results, full_text_results) = futures::join!(semantic_future, full_text_future);

    let semantic_results = semantic_results?;

    let full_text_results = full_text_results?;

    let combined_results = semantic_results
        .clone()
        .search_results
        .iter()
        .zip(full_text_results.search_results.iter())
        .flat_map(|(x, y)| vec![x.clone(), y.clone()])
        .unique_by(|chunk| chunk.group_id)
        .collect::<Vec<GroupSearchResults>>();

    let combined_search_chunk_query_results = SearchOverGroupsQueryResult {
        search_results: combined_results,
        total_chunk_pages: semantic_results.total_chunk_pages,
    };

    timer.add("fetched from qdrant");

    let combined_result_chunks = retrieve_chunks_for_groups(
        combined_search_chunk_query_results.clone(),
        &data,
        pool.clone(),
    )
    .await?;

    timer.add("fetched from postgres");

    let mut reranked_chunks = if combined_result_chunks.group_chunks.len() > 20 {
        let split_results = combined_result_chunks
            .group_chunks
            .chunks(20)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<Vec<GroupScoreChunk>>>();

        let cross_encoder_results = cross_encoder_for_groups(
            data.query.clone(),
            data.page_size.unwrap_or(10),
            split_results
                .get(0)
                .expect("Split results must exist")
                .to_vec(),
            config,
        )
        .await?;

        cross_encoder_results
            .iter()
            .chain(split_results.get(1).unwrap().iter())
            .cloned()
            .collect::<Vec<GroupScoreChunk>>()
    } else {
        cross_encoder_for_groups(
            data.query.clone(),
            data.page_size.unwrap_or(10),
            combined_result_chunks.group_chunks.clone(),
            config,
        )
        .await?
    };

    timer.add("reranking");

    if let Some(score_threshold) = data.score_threshold {
        reranked_chunks.retain(|chunk| chunk.metadata[0].score >= score_threshold.into());
        reranked_chunks.iter_mut().for_each(|chunk| {
            chunk
                .metadata
                .retain(|metadata| metadata.score >= score_threshold.into())
        });
    }

    let result_chunks = DeprecatedSearchOverGroupsResponseBody {
        group_chunks: reranked_chunks,
        total_chunk_pages: combined_search_chunk_query_results.total_chunk_pages,
    };

    //TODO: rerank for groups

    Ok(result_chunks)
}

#[tracing::instrument(skip(timer, pool))]
pub async fn autocomplete_chunks_query(
    mut data: AutocompleteReqPayload,
    parsed_query: ParsedQuery,
    pool: web::Data<Pool>,
    dataset: Dataset,
    config: &DatasetConfiguration,
    timer: &mut Timer,
) -> Result<SearchChunkQueryResponseBody, actix_web::Error> {
    let parent_span = sentry::configure_scope(|scope| scope.get_span());
    let transaction: sentry::TransactionOrSpan = match &parent_span {
        Some(parent) => parent
            .start_child("semantic search", "Search Semantic Chunks")
            .into(),
        None => {
            let ctx = sentry::TransactionContext::new("semantic search", "Search Semantic Chunks");
            sentry::start_transaction(ctx).into()
        }
    };
    sentry::configure_scope(|scope| scope.set_span(Some(transaction.clone())));

    timer.add("start to create dense embedding vector");

    timer.add("computed dense embedding");

    let (sort_by, rerank_by) = match data.sort_by.clone() {
        Some(sort_by) => match sort_by {
            QdrantSortBy::Field(field) => (Some(field.clone()), None),
            QdrantSortBy::SearchType(search_type) => (None, Some(search_type)),
        },
        None => (None, None),
    };

    let vector = get_qdrant_vector(data.clone().into(), parsed_query.clone(), config).await?;

    let mut qdrant_query = vec![
        RetrievePointQuery {
            vector: vector.clone(),
            score_threshold: data.score_threshold,
            sort_by: sort_by.clone(),
            rerank_by: rerank_by.clone(),
            limit: data.page_size.unwrap_or(10),
            filter: data.filters.clone(),
        }
        .into_qdrant_query(parsed_query.clone(), dataset.id, None, config, pool.clone())
        .await?,
    ];

    qdrant_query[0]
        .filter
        .must
        .push(Condition::matches_text("content", data.query.clone()));

    if data.extend_results.unwrap_or(false) {
        qdrant_query.push(
            RetrievePointQuery {
                vector,
                score_threshold: data.score_threshold,
                sort_by: sort_by.clone(),
                rerank_by: rerank_by.clone(),
                limit: data.page_size.unwrap_or(10),
                filter: data.filters.clone(),
            }
            .into_qdrant_query(parsed_query.clone(), dataset.id, None, config, pool.clone())
            .await?,
        );
    };

    let search_chunk_query_results =
        retrieve_qdrant_points_query(qdrant_query, 1, false, config).await?;

    timer.add("fetching from qdrant");

    if data.highlight_delimiters.is_none() {
        data.highlight_delimiters = Some(vec![" ".to_string()]);
    }

    let mut result_chunks = retrieve_chunks_from_point_ids(
        search_chunk_query_results.clone(),
        &data.clone().into(),
        pool.clone(),
    )
    .await?;

    timer.add("fetching from postgres");

    let first_increase = *search_chunk_query_results
        .batch_lengths
        .get(0)
        .unwrap_or(&0) as usize;

    let (before_increase, after_increase) = result_chunks.score_chunks.split_at(first_increase);
    let mut reranked_chunks = rerank_chunks(
        before_increase.to_vec(),
        sort_by.clone(),
        data.tag_weights.clone(),
        data.use_weights,
        data.location_bias,
    );
    reranked_chunks.extend(rerank_chunks(
        after_increase.to_vec(),
        sort_by,
        data.tag_weights,
        data.use_weights,
        data.location_bias,
    ));

    result_chunks.score_chunks = reranked_chunks;

    timer.add("reranking");
    transaction.finish();

    Ok(result_chunks)
}

#[tracing::instrument(skip(pool))]
pub async fn count_chunks_query(
    data: CountChunksReqPayload,
    parsed_query: ParsedQuery,
    pool: web::Data<Pool>,
    dataset: Dataset,
    config: &DatasetConfiguration,
) -> Result<CountChunkQueryResponseBody, actix_web::Error> {
    let vector = get_qdrant_vector(data.clone().into(), parsed_query.clone(), config).await?;

    let qdrant_query = RetrievePointQuery {
        vector,
        score_threshold: data.score_threshold,
        sort_by: None,
        rerank_by: None,
        limit: data.limit.unwrap_or(100000_u64),
        filter: data.filters.clone(),
    }
    .into_qdrant_query(parsed_query, dataset.id, None, config, pool.clone())
    .await?;

    let count = count_qdrant_query(
        data.limit.unwrap_or(100000_u64),
        vec![qdrant_query],
        config.clone(),
    )
    .await? as u32;

    Ok(CountChunkQueryResponseBody { count })
}
