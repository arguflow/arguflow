use std::collections::HashSet;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::data::models::{
    CardCollisions, CardFile, CardFileWithName, CardMetadataWithVotesAndFiles, CardVerifications,
    CardVote, FullTextSearchResult, User, UserDTO,
};
use crate::data::schema;
use crate::diesel::TextExpressionMethods;
use crate::diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use crate::{
    data::models::{CardMetadata, Pool},
    errors::DefaultError,
};
use actix_web::web;
use diesel::dsl::sql;
use diesel::result::Error::NotFound;
use diesel::sql_types::Int8;
use diesel::sql_types::Nullable;
use diesel::sql_types::Text;
use diesel::sql_types::{Bool, Double};
use diesel::{
    BoolExpressionMethods, Connection, JoinOnDsl, NullableExpressionMethods, SelectableHelper,
};
use openai_dive::v1::{api::Client, resources::embedding::EmbeddingParameters};
use qdrant_client::qdrant::condition::ConditionOneOf::HasId;
use qdrant_client::{
    prelude::{QdrantClient, QdrantClientConfig},
    qdrant::{point_id::PointIdOptions, Condition, Filter, HasIdCondition, PointId, SearchPoints},
};
use serde::{Deserialize, Serialize};

pub async fn get_qdrant_connection() -> Result<QdrantClient, DefaultError> {
    let qdrant_url = std::env::var("QDRANT_URL").expect("QDRANT_URL must be set");
    QdrantClient::new(Some(QdrantClientConfig::from_url(qdrant_url.as_str()))).map_err(|_err| {
        DefaultError {
            message: "Failed to connect to Qdrant",
        }
    })
}

pub async fn create_openai_embedding(message: &str) -> Result<Vec<f32>, actix_web::Error> {
    let open_ai_api_key = std::env::var("OPENAI_API_KEY").expect("OPENAI_API_KEY must be set");
    let client = Client::new(open_ai_api_key);

    // Vectorize
    let parameters = EmbeddingParameters {
        model: "text-embedding-ada-002".to_string(),
        input: message.to_string(),
        user: None,
    };

    let embeddings = client
        .embeddings()
        .create(parameters)
        .await
        .map_err(actix_web::error::ErrorBadRequest)?;

    let vector = embeddings.data.get(0).unwrap().embedding.clone();
    Ok(vector.iter().map(|&x| x as f32).collect())
}

#[derive(Serialize, Deserialize)]
pub struct SearchResult {
    pub score: f32,
    pub point_id: uuid::Uuid,
}

#[derive(Serialize, Deserialize)]
pub struct SearchCardQueryResult {
    pub search_results: Vec<SearchResult>,
    pub total_card_pages: i64,
}

pub async fn search_card_query(
    embedding_vector: Vec<f32>,
    page: u64,
    pool: Arc<Mutex<web::Data<r2d2::Pool<diesel::r2d2::ConnectionManager<diesel::PgConnection>>>>>,
    filter_oc_file_path: Option<Vec<String>>,
    filter_link_url: Option<Vec<String>>,
    current_user_id: Option<uuid::Uuid>,
) -> Result<SearchCardQueryResult, DefaultError> {
    let page = if page == 0 { 1 } else { page };
    let filter_oc_file_path = filter_oc_file_path.unwrap_or([].to_vec());
    let filter_link_url = filter_link_url.unwrap_or([].to_vec());

    let mut conn = pool.lock().unwrap().get().unwrap();

    // SELECT distinct card_metadata.qdrant_point_id, card_collisions.collision_qdrant_id
    // FROM card_metadata
    // left outer JOIN card_collisions ON card_metadata.id = card_collisions.card_id
    // WHERE card_metadata.private = false OR (card_metadata.private = false and card_metadata.qdrant_point_id is null);

    //SELECT DISTINCT "card\_metadata"."qdrant\_point\_id", "card\_collisions"."collision\_qdrant\_id"
    //FROM ("card\_metadata"
    //LEFT OUTER JOIN "card\_collisions" ON ("card\_metadata"."id" = "card\_collisions"."card\_id"))
    //WHERE (("card\_metadata"."private" = $1) OR (("card\_metadata"."private" = $2) AND ("card\_metadata"."qdrant\_point\_id" IS NULL))) -- binds: \[false, false\]
    use crate::data::schema::card_collisions::dsl as card_collisions_columns;
    use crate::data::schema::card_metadata::dsl as card_metadata_columns;

    // We only want collisions for which the root CardMetadata is public
    // Punting on or the current user is the author

    let mut query = card_metadata_columns::card_metadata
        .left_outer_join(
            card_collisions_columns::card_collisions.on(card_metadata_columns::id
                .eq(card_collisions_columns::card_id)
                .and(card_metadata_columns::private.eq(false))),
        )
        .select((
            card_metadata_columns::qdrant_point_id,
            card_collisions_columns::collision_qdrant_id.nullable(),
        ))
        .filter(card_metadata_columns::private.eq(false))
        .or_filter(
            card_metadata_columns::author_id.eq(current_user_id.unwrap_or(uuid::Uuid::nil())),
        )
        .distinct()
        .into_boxed();

    if !filter_oc_file_path.is_empty() {
        query = query.filter(
            card_metadata_columns::oc_file_path
                .like(format!("%{}%", filter_oc_file_path.get(0).unwrap())),
        );
    }

    for file_path in filter_oc_file_path.iter().skip(1) {
        query =
            query.or_filter(card_metadata_columns::oc_file_path.like(format!("%{}%", file_path)));
    }

    if !filter_link_url.is_empty() {
        query = query.filter(
            card_metadata_columns::link.like(format!("%{}%", filter_link_url.get(0).unwrap())),
        );
    }
    for link_url in filter_link_url.iter().skip(1) {
        query = query.or_filter(card_metadata_columns::link.like(format!("%{}%", link_url)));
    }

    let filtered_option_ids: Vec<(Option<uuid::Uuid>, Option<uuid::Uuid>)> =
        query.load(&mut conn).map_err(|_| DefaultError {
            message: "Failed to load metadata",
        })?;
    let qdrant = get_qdrant_connection().await?;

    let filtered_point_ids: &Vec<PointId> = &filtered_option_ids
        .iter()
        .map(|uuid| {
            uuid.0
                .unwrap_or(uuid.1.unwrap_or(uuid::Uuid::nil()))
                .to_string()
                .into()
        })
        .collect::<Vec<PointId>>();

    let mut filter = Filter::default();
    filter.should.push(Condition {
        condition_one_of: Some(HasId(HasIdCondition {
            has_id: (filtered_point_ids).to_vec(),
        })),
    });

    let data = qdrant
        .search_points(&SearchPoints {
            collection_name: "debate_cards".to_string(),
            vector: embedding_vector,
            limit: 25,
            offset: Some((page - 1) * 25),
            with_payload: None,
            filter: Some(filter),
            ..Default::default()
        })
        .await
        .map_err(|_e| DefaultError {
            message: "Failed to search points on Qdrant",
        })?;

    let point_ids: Vec<SearchResult> = data
        .result
        .iter()
        .filter_map(|point| match point.clone().id?.point_id_options? {
            PointIdOptions::Uuid(id) => Some(SearchResult {
                score: point.score,
                point_id: uuid::Uuid::parse_str(&id).ok()?,
            }),
            PointIdOptions::Num(_) => None,
        })
        .collect();

    Ok(SearchCardQueryResult {
        search_results: point_ids,
        total_card_pages: (filtered_point_ids.len() as f64 / 25.0).ceil() as i64,
    })
}

pub async fn global_unfiltered_top_match_query(
    embedding_vector: Vec<f32>,
) -> Result<SearchResult, DefaultError> {
    let qdrant = get_qdrant_connection().await?;

    let data = qdrant
        .search_points(&SearchPoints {
            collection_name: "debate_cards".to_string(),
            vector: embedding_vector,
            limit: 1,
            with_payload: None,
            ..Default::default()
        })
        .await
        .map_err(|_e| DefaultError {
            message: "Failed to search points on Qdrant",
        })?;

    let top_search_result: SearchResult = match data.result.get(0) {
        Some(point) => match point.clone().id {
            Some(point_id) => match point_id.point_id_options {
                Some(PointIdOptions::Uuid(id)) => SearchResult {
                    score: point.score,
                    point_id: uuid::Uuid::parse_str(&id).map_err(|_| DefaultError {
                        message: "Failed to parse uuid",
                    })?,
                },
                Some(PointIdOptions::Num(_)) => {
                    return Err(DefaultError {
                        message: "Failed to parse uuid",
                    })
                }
                None => {
                    return Err(DefaultError {
                        message: "Failed to parse uuid",
                    })
                }
            },
            None => {
                return Err(DefaultError {
                    message: "Failed to parse uuid",
                })
            }
        },
        // This only happens when there are no cards in the database
        None => SearchResult {
            score: 0.0,
            point_id: uuid::Uuid::nil(),
        },
    };

    Ok(top_search_result)
}

pub async fn search_card_collections_query(
    embedding_vector: Vec<f32>,
    page: u64,
    pool: Arc<Mutex<web::Data<r2d2::Pool<diesel::r2d2::ConnectionManager<diesel::PgConnection>>>>>,
    filter_oc_file_path: Option<Vec<String>>,
    filter_link_url: Option<Vec<String>>,
    collection_id: uuid::Uuid,
) -> Result<SearchCardQueryResult, DefaultError> {
    let page = if page == 0 { 1 } else { page };
    use crate::data::schema::card_collection_bookmarks::dsl as card_collection_bookmarks_columns;
    use crate::data::schema::card_collisions::dsl as card_collisions_columns;
    use crate::data::schema::card_metadata::dsl as card_metadata_columns;

    let mut conn = pool.lock().unwrap().get().unwrap();

    let card_ids: Vec<uuid::Uuid> = card_collection_bookmarks_columns::card_collection_bookmarks
        .select(card_collection_bookmarks_columns::card_metadata_id)
        .filter(card_collection_bookmarks_columns::collection_id.eq(collection_id))
        .load::<uuid::Uuid>(&mut conn)
        .map_err(|_| DefaultError {
            message: "Failed to load metadata",
        })?;

    let mut query = card_metadata_columns::card_metadata
        .left_outer_join(
            card_collisions_columns::card_collisions
                .on(card_metadata_columns::id.eq(card_collisions_columns::card_id)),
        )
        .select((
            card_metadata_columns::qdrant_point_id,
            card_collisions_columns::collision_qdrant_id.nullable(),
        ))
        .filter(card_metadata_columns::private.eq(false))
        .or_filter(
            card_metadata_columns::private
                .eq(false)
                .and(card_metadata_columns::qdrant_point_id.is_null()),
        )
        .distinct()
        .filter(card_metadata_columns::id.eq_any(card_ids))
        .into_boxed();

    let filter_oc_file_path = filter_oc_file_path.unwrap_or([].to_vec());
    let filter_link_url = filter_link_url.unwrap_or([].to_vec());

    if !filter_oc_file_path.is_empty() {
        query = query.filter(
            card_metadata_columns::oc_file_path
                .like(format!("%{}%", filter_oc_file_path.get(0).unwrap())),
        );
    }

    for file_path in filter_oc_file_path.iter().skip(1) {
        query =
            query.or_filter(card_metadata_columns::oc_file_path.like(format!("%{}%", file_path)));
    }

    if !filter_link_url.is_empty() {
        query = query.filter(
            card_metadata_columns::link.like(format!("%{}%", filter_link_url.get(0).unwrap())),
        );
    }
    for link_url in filter_link_url.iter().skip(1) {
        query = query.or_filter(card_metadata_columns::link.like(format!("%{}%", link_url)));
    }
    let filtered_option_ids: Vec<(Option<uuid::Uuid>, Option<uuid::Uuid>)> =
        query.load(&mut conn).map_err(|_| DefaultError {
            message: "Failed to load metadata",
        })?;

    let qdrant = get_qdrant_connection().await?;

    let filtered_point_ids: &Vec<PointId> = &filtered_option_ids
        .iter()
        .map(|uuid| {
            uuid.0
                .unwrap_or(uuid.1.unwrap_or(uuid::Uuid::nil()))
                .to_string()
        })
        // remove duplicates
        .collect::<HashSet<String>>()
        .iter()
        .map(|uuid| (*uuid).clone().into())
        .collect::<Vec<PointId>>();

    let mut filter = Filter::default();
    filter.should.push(Condition {
        condition_one_of: Some(HasId(HasIdCondition {
            has_id: (filtered_point_ids).to_vec(),
        })),
    });

    let data = qdrant
        .search_points(&SearchPoints {
            collection_name: "debate_cards".to_string(),
            vector: embedding_vector,
            limit: 25,
            offset: Some((page - 1) * 25),
            with_payload: None,
            filter: Some(filter),
            ..Default::default()
        })
        .await
        .map_err(|_e| DefaultError {
            message: "Failed to search points on Qdrant",
        })?;

    let point_ids: Vec<SearchResult> = data
        .result
        .iter()
        .filter_map(|point| match point.clone().id?.point_id_options? {
            PointIdOptions::Uuid(id) => Some(SearchResult {
                score: point.score,
                point_id: uuid::Uuid::parse_str(&id).ok()?,
            }),
            PointIdOptions::Num(_) => None,
        })
        .collect();

    Ok(SearchCardQueryResult {
        search_results: point_ids,
        total_card_pages: (data.result.len() as f64 / 25.0).ceil() as i64,
    })
}

pub fn get_metadata(
    card_metadata: Vec<FullTextSearchResult>,
    current_user_id: Option<uuid::Uuid>,
    mut conn: r2d2::PooledConnection<diesel::r2d2::ConnectionManager<diesel::PgConnection>>,
) -> Result<Vec<CardMetadataWithVotesAndFiles>, DefaultError> {
    use crate::data::schema::card_files::dsl as card_files_columns;
    use crate::data::schema::card_verification::dsl as card_verification_columns;
    use crate::data::schema::card_votes::dsl as card_votes_columns;
    use crate::data::schema::files::dsl as files_columns;
    use crate::data::schema::users::dsl as user_columns;

    let card_creators: Vec<User> = user_columns::users
        .filter(
            user_columns::id.eq_any(
                card_metadata
                    .iter()
                    .map(|metadata| metadata.author_id)
                    .collect::<Vec<uuid::Uuid>>(),
            ),
        )
        .load::<User>(&mut conn)
        .map_err(|_| DefaultError {
            message: "Failed to load card creators",
        })?;

    let card_votes: Vec<CardVote> = card_votes_columns::card_votes
        .filter(
            card_votes_columns::card_metadata_id.eq_any(
                card_metadata
                    .iter()
                    .map(|metadata| metadata.id)
                    .collect::<Vec<uuid::Uuid>>(),
            ),
        )
        .load::<CardVote>(&mut conn)
        .map_err(|_| DefaultError {
            message: "Failed to load upvotes",
        })?;

    let file_ids: Vec<CardFileWithName> = card_files_columns::card_files
        .filter(
            card_files_columns::card_id.eq_any(
                card_metadata
                    .iter()
                    .map(|card| card.id)
                    .collect::<Vec<uuid::Uuid>>()
                    .as_slice(),
            ),
        )
        .inner_join(files_columns::files)
        .filter(files_columns::private.eq(false))
        .select((
            card_files_columns::card_id,
            card_files_columns::file_id,
            files_columns::file_name,
        ))
        .load::<CardFileWithName>(&mut conn)
        .map_err(|_| DefaultError {
            message: "Failed to load metadata",
        })?;

    let card_verifications: Vec<CardVerifications> = card_verification_columns::card_verification
        .filter(
            card_verification_columns::card_id.eq_any(
                card_metadata
                    .iter()
                    .map(|card| card.id)
                    .collect::<Vec<uuid::Uuid>>()
                    .as_slice(),
            ),
        )
        .load::<CardVerifications>(&mut conn)
        .map_err(|_| DefaultError {
            message: "Failed to load verification metadata",
        })?;

    let card_metadata_with_upvotes_and_file_id: Vec<CardMetadataWithVotesAndFiles> = card_metadata
        .into_iter()
        .map(|metadata| {
            let votes = card_votes
                .iter()
                .filter(|upvote| upvote.card_metadata_id == metadata.id)
                .collect::<Vec<&CardVote>>();
            let total_upvotes = votes.iter().filter(|upvote| upvote.vote).count() as i64;
            let total_downvotes = votes.iter().filter(|upvote| !upvote.vote).count() as i64;
            let vote_by_current_user = match current_user_id {
                Some(user_id) => votes
                    .iter()
                    .find(|upvote| upvote.voted_user_id == user_id)
                    .map(|upvote| upvote.vote),
                None => None,
            };

            let author = card_creators
                .iter()
                .find(|user| user.id == metadata.author_id)
                .map(|user| UserDTO {
                    id: user.id,
                    username: user.username.clone(),
                    email: if user.visible_email {
                        Some(user.email.clone())
                    } else {
                        None
                    },
                    website: user.website.clone(),
                    visible_email: user.visible_email,
                    created_at: user.created_at,
                });

            let verification_score = card_verifications
                .iter()
                .find(|verification| verification.card_id == metadata.id)
                .map(|verification| verification.similarity_score);

            let card_with_file_name = file_ids.iter().find(|file| file.card_id == metadata.id);

            CardMetadataWithVotesAndFiles {
                id: metadata.id,
                content: metadata.content,
                link: metadata.link,
                oc_file_path: metadata.oc_file_path,
                author,
                qdrant_point_id: metadata.qdrant_point_id.unwrap_or(uuid::Uuid::nil()),
                total_upvotes,
                total_downvotes,
                vote_by_current_user,
                created_at: metadata.created_at,
                updated_at: metadata.updated_at,
                private: metadata.private,
                score: metadata.score,
                card_html: metadata.card_html,
                file_id: card_with_file_name.map(|file| file.file_id),
                file_name: card_with_file_name.map(|file| file.file_name.to_string()),
                verification_score,
            }
        })
        .collect();
    Ok(card_metadata_with_upvotes_and_file_id)
}

#[derive(Serialize, Deserialize)]
pub struct FullTextSearchCardQueryResult {
    pub search_results: Vec<CardMetadataWithVotesAndFiles>,
    pub total_card_pages: i64,
}

pub fn search_full_text_card_query(
    user_query: String,
    page: u64,
    pool: MutexGuard<'_, actix_web::web::Data<Pool>>,
    current_user_id: Option<uuid::Uuid>,
    filter_oc_file_path: Option<Vec<String>>,
    filter_link_url: Option<Vec<String>>,
) -> Result<FullTextSearchCardQueryResult, DefaultError> {
    let page = if page == 0 { 1 } else { page };
    use crate::data::schema::card_collisions::dsl as card_collisions_columns;
    use crate::data::schema::card_metadata::dsl as card_metadata_columns;

    let second_join = diesel::alias!(schema::card_metadata as second_join);

    let mut conn = pool.get().unwrap();
    // SELECT
    //     card_metadata.qdrant_point_id,
    //     second_join.qdrant_point_id
    // FROM
    //     card_metadata
    // LEFT OUTER JOIN card_collisions ON
    //     card_metadata.id = card_collisions.card_id
    //     AND card_metadata.private = false
    // LEFT OUTER JOIN card_metadata AS second_join ON
    //     second_join.qdrant_point_id = card_collisions.collision_qdrant_id
    //     AND second_join.private = true
    // WHERE
    //     card_metadata.private = false
    //     and (second_join.qdrant_point_id notnull or card_metadata.qdrant_point_id notnull);
    let mut query = card_metadata_columns::card_metadata
        .left_outer_join(
            card_collisions_columns::card_collisions.on(card_metadata_columns::id
                .eq(card_collisions_columns::card_id)
                .and(card_metadata_columns::private.eq(false))),
        )
        .left_outer_join(
            second_join.on(second_join
                .field(schema::card_metadata::qdrant_point_id)
                .eq(card_collisions_columns::collision_qdrant_id)
                .and(second_join.field(schema::card_metadata::private).eq(true))),
        )
        .filter(
            card_metadata_columns::private.eq(false).and(
                second_join
                    .field(schema::card_metadata::qdrant_point_id)
                    .is_not_null()
                    .or(card_metadata_columns::qdrant_point_id.is_not_null()),
            ),
        )
        .or_filter(
            card_metadata_columns::author_id.eq(current_user_id.unwrap_or(uuid::Uuid::nil())),
        )
        .select((
            (
                card_metadata_columns::id,
                card_metadata_columns::content,
                card_metadata_columns::link,
                card_metadata_columns::author_id,
                card_metadata_columns::qdrant_point_id,
                card_metadata_columns::created_at,
                card_metadata_columns::updated_at,
                card_metadata_columns::oc_file_path,
                card_metadata_columns::card_html,
                card_metadata_columns::private,
                sql::<Nullable<Double>>(
                    "(ts_rank(card_metadata.card_metadata_tsvector, plainto_tsquery('english', ",
                )
                .bind::<Text, _>(user_query.clone())
                .sql(") , 32) * 10) AS rank"),
                sql::<Int8>("count(*) OVER() AS full_count"),
            ),
            second_join
                .field(schema::card_metadata::qdrant_point_id)
                .nullable(),
        ))
        .distinct_on((
            card_metadata_columns::qdrant_point_id,
            second_join
                .field(schema::card_metadata::qdrant_point_id)
                .nullable(),
        ))
        .into_boxed();

    query = query.filter(
        sql::<Bool>("card_metadata.card_metadata_tsvector @@ plainto_tsquery('english', ")
            .bind::<Text, _>(user_query)
            .sql(")"),
    );

    let filter_oc_file_path = filter_oc_file_path.unwrap_or([].to_vec());
    let filter_link_url = filter_link_url.unwrap_or([].to_vec());

    if !filter_oc_file_path.is_empty() {
        query = query.filter(
            card_metadata_columns::oc_file_path
                .like(format!("%{}%", filter_oc_file_path.get(0).unwrap())),
        );
    }

    for file_path in filter_oc_file_path.iter().skip(1) {
        query =
            query.or_filter(card_metadata_columns::oc_file_path.like(format!("%{}%", file_path)));
    }

    if !filter_link_url.is_empty() {
        query = query.filter(
            card_metadata_columns::link.like(format!("%{}%", filter_link_url.get(0).unwrap())),
        );
    }
    for link_url in filter_link_url.iter().skip(1) {
        query = query.or_filter(card_metadata_columns::link.like(format!("%{}%", link_url)));
    }

    query = query.order((
        card_metadata_columns::qdrant_point_id,
        second_join.field(schema::card_metadata::qdrant_point_id),
        sql::<Text>("rank DESC"),
    ));

    query = query
        .limit(25)
        .offset(((page - 1) * 25).try_into().unwrap());

    let searched_cards: Vec<(FullTextSearchResult, Option<uuid::Uuid>)> =
        query.load(&mut conn).map_err(|_| DefaultError {
            message: "Failed to load trigram searched cards",
        })?;

    let card_metadata_with_upvotes_and_files = get_metadata(
        searched_cards
            .iter()
            .map(|card| card.0.clone())
            .collect::<Vec<FullTextSearchResult>>(),
        current_user_id,
        conn,
    )
    .map_err(|_| DefaultError {
        message: "Failed to load searched cards",
    })?;

    let total_count = if searched_cards.is_empty() {
        0
    } else {
        (searched_cards.get(0).unwrap().0.count as f64 / 25.0).ceil() as i64
    };

    Ok(FullTextSearchCardQueryResult {
        search_results: card_metadata_with_upvotes_and_files,
        total_card_pages: total_count,
    })
}

pub fn global_top_full_text_card_query(
    user_query: String,
    pool: MutexGuard<'_, actix_web::web::Data<Pool>>,
) -> Result<Option<CardMetadataWithVotesAndFiles>, DefaultError> {
    use crate::data::schema::card_collisions::dsl as card_collisions_columns;
    use crate::data::schema::card_metadata::dsl as card_metadata_columns;

    let mut conn = pool.get().unwrap();

    let mut query = card_metadata_columns::card_metadata
        .filter(card_metadata_columns::private.eq(false))
        .inner_join(
            card_collisions_columns::card_collisions
                .on(card_collisions_columns::card_id.eq(card_metadata_columns::id)),
        )
        .select((
            (
                card_metadata_columns::id,
                card_metadata_columns::content,
                card_metadata_columns::link,
                card_metadata_columns::author_id,
                card_metadata_columns::qdrant_point_id,
                card_metadata_columns::created_at,
                card_metadata_columns::updated_at,
                card_metadata_columns::oc_file_path,
                card_metadata_columns::card_html,
                card_metadata_columns::private,
                sql::<Nullable<Double>>(
                    "(ts_rank(card_metadata_tsvector, plainto_tsquery('english', ",
                )
                .bind::<Text, _>(user_query.clone())
                .sql(") , 32) * 10) AS rank"),
                sql::<Int8>("count(*) OVER() AS full_count"),
            ),
            (card_collisions_columns::collision_qdrant_id.assume_not_null()),
        ))
        .distinct()
        .into_boxed();

    query = query.filter(
        sql::<Bool>("card_metadata.card_metadata_tsvector @@ plainto_tsquery('english', ")
            .bind::<Text, _>(user_query)
            .sql(")"),
    );

    query = query.order((sql::<Text>("rank DESC"),));

    let searched_card: (FullTextSearchResult, uuid::Uuid) = match query.first(&mut conn) {
        Ok(card) => Ok(card),
        Err(e) => match e {
            NotFound => {
                return Ok(None);
            }
            _ => Err(DefaultError {
                message: "Failed to load top trigram searched card",
            }),
        },
    }?;

    let card_metadata_with_upvotes_and_files = get_metadata(vec![searched_card.0], None, conn)
        .map_err(|_| DefaultError {
            message: "Failed to load metadata for top trigram searched card",
        })?;

    // This is a hack to replace qdrant_point_id with collision_qdrant_point_id if it is not set
    let mut top_card = match card_metadata_with_upvotes_and_files.get(0) {
        Some(card) => card.clone(),
        None => {
            return Ok(None);
        }
    };

    if top_card.qdrant_point_id == uuid::Uuid::default() {
        top_card.qdrant_point_id = searched_card.1;
    }

    Ok(Some(top_card))
}

#[derive(Serialize, Deserialize)]
pub struct ScoredCardDTO {
    pub metadata: CardMetadata,
    pub score: f32,
}

pub fn get_metadata_from_point_ids(
    point_ids: Vec<uuid::Uuid>,
    current_user_id: Option<uuid::Uuid>,
    pool: MutexGuard<'_, actix_web::web::Data<Pool>>,
) -> Result<Vec<CardMetadataWithVotesAndFiles>, DefaultError> {
    use crate::data::schema::card_metadata::dsl as card_metadata_columns;

    let mut conn = pool.get().unwrap();

    let card_metadata: Vec<CardMetadata> = card_metadata_columns::card_metadata
        .filter(card_metadata_columns::qdrant_point_id.eq_any(&point_ids))
        .select((
            card_metadata_columns::id,
            card_metadata_columns::content,
            card_metadata_columns::link,
            card_metadata_columns::author_id,
            card_metadata_columns::qdrant_point_id,
            card_metadata_columns::created_at,
            card_metadata_columns::updated_at,
            card_metadata_columns::oc_file_path,
            card_metadata_columns::card_html,
            card_metadata_columns::private,
        ))
        .load::<CardMetadata>(&mut conn)
        .map_err(|_| DefaultError {
            message: "Failed to load metadata",
        })?;

    let converted_cards: Vec<FullTextSearchResult> = card_metadata
        .iter()
        .map(|card| <CardMetadata as Into<FullTextSearchResult>>::into(card.clone()))
        .collect::<Vec<FullTextSearchResult>>();

    let card_metadata_with_upvotes_and_file_id =
        get_metadata(converted_cards, current_user_id, conn).map_err(|_| DefaultError {
            message: "Failed to load metadata",
        })?;

    //combine card_metadata_with vote with the file_ids that was loaded

    Ok(card_metadata_with_upvotes_and_file_id)
}

pub fn get_collided_cards_query(
    point_ids: Vec<uuid::Uuid>,
    current_user_id: Option<uuid::Uuid>,
    pool: MutexGuard<'_, actix_web::web::Data<Pool>>,
) -> Result<Vec<(CardMetadataWithVotesAndFiles, uuid::Uuid)>, DefaultError> {
    use crate::data::schema::card_collisions::dsl as card_collisions_columns;
    use crate::data::schema::card_metadata::dsl as card_metadata_columns;

    let mut conn = pool.get().unwrap();

    let card_metadata: Vec<(CardMetadata, uuid::Uuid)> = card_collisions_columns::card_collisions
        .inner_join(
            card_metadata_columns::card_metadata
                .on(card_metadata_columns::id.eq(card_collisions_columns::card_id)),
        )
        .select((
            (
                card_metadata_columns::id,
                card_metadata_columns::content,
                card_metadata_columns::link,
                card_metadata_columns::author_id,
                card_metadata_columns::qdrant_point_id,
                card_metadata_columns::created_at,
                card_metadata_columns::updated_at,
                card_metadata_columns::oc_file_path,
                card_metadata_columns::card_html,
                card_metadata_columns::private,
            ),
            (card_collisions_columns::collision_qdrant_id.assume_not_null()),
        ))
        .filter(card_collisions_columns::collision_qdrant_id.eq_any(point_ids))
        .filter(card_metadata_columns::private.eq(false))
        .or_filter(
            card_metadata_columns::author_id.eq(current_user_id.unwrap_or(uuid::Uuid::nil())),
        )
        .load::<(CardMetadata, uuid::Uuid)>(&mut conn)
        .map_err(|_| DefaultError {
            message: "Failed to load metadata",
        })?;

    let collided_qdrant_ids = card_metadata
        .iter()
        .map(|(_, qdrant_id)| *qdrant_id)
        .collect::<Vec<uuid::Uuid>>();

    let converted_cards: Vec<FullTextSearchResult> = card_metadata
        .iter()
        .map(|card| <CardMetadata as Into<FullTextSearchResult>>::into(card.0.clone()))
        .collect::<Vec<FullTextSearchResult>>();

    let card_metadata_with_upvotes_and_file_id =
        get_metadata(converted_cards, current_user_id, conn).map_err(|_| DefaultError {
            message: "Failed to load metadata",
        })?;

    let card_metadatas_with_collided_qdrant_ids = card_metadata_with_upvotes_and_file_id
        .iter()
        .zip(collided_qdrant_ids.iter())
        .map(|(card, qdrant_id)| (card.clone(), *qdrant_id))
        .collect::<Vec<(CardMetadataWithVotesAndFiles, uuid::Uuid)>>();

    //combine card_metadata_with vote with the file_ids that was loaded

    Ok(card_metadatas_with_collided_qdrant_ids)
}

pub fn get_metadata_from_id_query(
    card_id: uuid::Uuid,
    pool: MutexGuard<'_, actix_web::web::Data<Pool>>,
) -> Result<CardMetadata, DefaultError> {
    use crate::data::schema::card_metadata::dsl as card_metadata_columns;

    let mut conn = pool.get().unwrap();

    card_metadata_columns::card_metadata
        .filter(card_metadata_columns::id.eq(card_id))
        .select((
            card_metadata_columns::id,
            card_metadata_columns::content,
            card_metadata_columns::link,
            card_metadata_columns::author_id,
            card_metadata_columns::qdrant_point_id,
            card_metadata_columns::created_at,
            card_metadata_columns::updated_at,
            card_metadata_columns::oc_file_path,
            card_metadata_columns::card_html,
            card_metadata_columns::private,
        ))
        .first::<CardMetadata>(&mut conn)
        .map_err(|_| DefaultError {
            message: "Failed to load metadata",
        })
}

pub fn get_metadata_and_votes_from_id_query(
    card_id: uuid::Uuid,
    current_user_id: Option<uuid::Uuid>,
    pool: web::Data<Pool>,
) -> Result<CardMetadataWithVotesAndFiles, DefaultError> {
    use crate::data::schema::card_metadata::dsl as card_metadata_columns;

    let mut conn = pool.get().unwrap();

    let card_metadata = card_metadata_columns::card_metadata
        .filter(card_metadata_columns::id.eq(card_id))
        .select((
            card_metadata_columns::id,
            card_metadata_columns::content,
            card_metadata_columns::link,
            card_metadata_columns::author_id,
            card_metadata_columns::qdrant_point_id,
            card_metadata_columns::created_at,
            card_metadata_columns::updated_at,
            card_metadata_columns::oc_file_path,
            card_metadata_columns::card_html,
            card_metadata_columns::private,
        ))
        .first::<CardMetadata>(&mut conn)
        .map_err(|_| DefaultError {
            message: "Failed to load metadata",
        })?;
    let converted_card: FullTextSearchResult =
        <CardMetadata as Into<FullTextSearchResult>>::into(card_metadata);

    let card_metadata_with_upvotes_and_file_id =
        get_metadata(vec![converted_card], current_user_id, conn).map_err(|_| DefaultError {
            message: "Failed to load metadata",
        })?;
    Ok(card_metadata_with_upvotes_and_file_id
        .first()
        .unwrap()
        .clone())
}

pub fn insert_card_metadata_query(
    card_data: CardMetadata,
    file_uuid: Option<uuid::Uuid>,
    pool: MutexGuard<'_, actix_web::web::Data<Pool>>,
) -> Result<CardMetadata, DefaultError> {
    use crate::data::schema::card_files::dsl as card_files_columns;
    use crate::data::schema::card_metadata::dsl::*;

    let mut conn = pool.get().unwrap();

    let transaction_result = conn.transaction::<_, diesel::result::Error, _>(|conn| {
        diesel::insert_into(card_metadata)
            .values(&card_data)
            .execute(conn)?;

        if file_uuid.is_some() {
            diesel::insert_into(card_files_columns::card_files)
                .values(&CardFile::from_details(card_data.id, file_uuid.unwrap()))
                .execute(conn)?;
        }

        Ok(())
    });

    match transaction_result {
        Ok(_) => (),
        Err(_) => {
            return Err(DefaultError {
                message: "Failed to insert card metadata",
            })
        }
    };
    Ok(card_data)
}

pub fn insert_duplicate_card_metadata_query(
    card_data: CardMetadata,
    duplicate_card: uuid::Uuid,
    file_uuid: Option<uuid::Uuid>,
    pool: MutexGuard<'_, actix_web::web::Data<Pool>>,
) -> Result<CardMetadata, DefaultError> {
    use crate::data::schema::card_collisions::dsl::*;
    use crate::data::schema::card_files::dsl as card_files_columns;
    use crate::data::schema::card_metadata::dsl::*;

    let mut conn = pool.get().unwrap();

    let transaction_result = conn.transaction::<_, diesel::result::Error, _>(|conn| {
        diesel::insert_into(card_metadata)
            .values(&card_data)
            .execute(conn)?;

        //insert duplicate into card_collisions
        diesel::insert_into(card_collisions)
            .values(&CardCollisions::from_details(card_data.id, duplicate_card))
            .execute(conn)?;

        if file_uuid.is_some() {
            diesel::insert_into(card_files_columns::card_files)
                .values(&CardFile::from_details(card_data.id, file_uuid.unwrap()))
                .execute(conn)?;
        }

        Ok(())
    });

    match transaction_result {
        Ok(_) => (),
        Err(_) => {
            return Err(DefaultError {
                message: "Failed to insert duplicate card metadata",
            })
        }
    };
    Ok(card_data)
}

pub fn update_card_metadata_query(
    card_data: CardMetadata,
    file_uuid: Option<uuid::Uuid>,
    pool: MutexGuard<'_, actix_web::web::Data<Pool>>,
) -> Result<(), DefaultError> {
    use crate::data::schema::card_files::dsl as card_files_columns;
    use crate::data::schema::card_metadata::dsl as card_metadata_columns;
    use crate::data::schema::card_votes::dsl as card_votes_columns;

    let mut conn = pool.get().unwrap();

    let transaction_result = conn.transaction::<_, diesel::result::Error, _>(|conn| {
        diesel::update(
            card_metadata_columns::card_metadata.filter(card_metadata_columns::id.eq(card_data.id)),
        )
        .set((
            card_metadata_columns::link.eq(card_data.link),
            card_metadata_columns::card_html.eq(card_data.card_html),
            card_metadata_columns::private.eq(card_data.private),
        ))
        .execute(conn)?;

        diesel::update(
            card_votes_columns::card_votes
                .filter(card_votes_columns::card_metadata_id.eq(card_data.id)),
        )
        .set(card_votes_columns::deleted.eq(card_data.private))
        .execute(conn)?;

        if file_uuid.is_some() {
            diesel::insert_into(card_files_columns::card_files)
                .values(&CardFile::from_details(card_data.id, file_uuid.unwrap()))
                .execute(conn)?;
        }
        Ok(())
    });

    match transaction_result {
        Ok(_) => (),
        Err(_) => {
            return Err(DefaultError {
                message: "Failed to update card metadata",
            })
        }
    };

    Ok(())
}

enum TransactionResult {
    CardCollisionDetected,
    CardCollisionNotDetected,
}

pub async fn delete_card_metadata_query(
    card_uuid: uuid::Uuid,
    pool: Arc<Mutex<web::Data<r2d2::Pool<diesel::r2d2::ConnectionManager<diesel::PgConnection>>>>>,
) -> Result<(), DefaultError> {
    use crate::data::schema::card_collection_bookmarks::dsl as card_collection_bookmarks_columns;
    use crate::data::schema::card_collisions::dsl as card_collisions_columns;
    use crate::data::schema::card_files::dsl as card_files_columns;
    use crate::data::schema::card_metadata::dsl as card_metadata_columns;
    let mut conn = pool.lock().unwrap().get().unwrap();

    let transaction_result = conn.transaction::<_, diesel::result::Error, _>(|conn| {
        {
            diesel::delete(
                card_files_columns::card_files.filter(card_files_columns::card_id.eq(card_uuid)),
            )
            .execute(conn)?;

            diesel::delete(
                card_collection_bookmarks_columns::card_collection_bookmarks
                    .filter(card_collection_bookmarks_columns::card_metadata_id.eq(card_uuid)),
            )
            .execute(conn)?;

            let deleted_card_collision_count = diesel::delete(
                card_collisions_columns::card_collisions
                    .filter(card_collisions_columns::card_id.eq(card_uuid)),
            )
            .execute(conn)?;

            if deleted_card_collision_count > 0 {
                // there cannot be collisions for a collision, just delete the card_metadata without issue
                diesel::delete(
                    card_metadata_columns::card_metadata
                        .filter(card_metadata_columns::id.eq(card_uuid)),
                )
                .execute(conn)?;

                return Ok(TransactionResult::CardCollisionNotDetected);
            }

            let card_collisions: Vec<(CardCollisions, bool)> =
                card_collisions_columns::card_collisions
                    .inner_join(
                        card_metadata_columns::card_metadata
                            .on(card_metadata_columns::qdrant_point_id
                                .eq(card_collisions_columns::collision_qdrant_id)),
                    )
                    .filter(card_metadata_columns::id.eq(card_uuid))
                    .select((CardCollisions::as_select(), card_metadata_columns::private))
                    .order_by(card_collisions_columns::created_at.asc())
                    .load::<(CardCollisions, bool)>(conn)?;

            if !card_collisions.is_empty() {
                // get the first collision that is public or the first collision if all are private
                let latest_collision = match card_collisions.iter().find(|x| !x.1) {
                    Some(x) => x.0.clone(),
                    None => card_collisions[0].0.clone(),
                };

                // update all collisions except latest_collision to point to a qdrant_id of None
                diesel::update(
                    card_collisions_columns::card_collisions.filter(
                        card_collisions_columns::id.eq_any(
                            card_collisions
                                .iter()
                                .filter(|x| x.0.id != latest_collision.id)
                                .map(|x| x.0.id)
                                .collect::<Vec<uuid::Uuid>>(),
                        ),
                    ),
                )
                .set(card_collisions_columns::collision_qdrant_id.eq::<Option<uuid::Uuid>>(None))
                .execute(conn)?;

                // delete latest_collision from card_collisions
                diesel::delete(
                    card_collisions_columns::card_collisions
                        .filter(card_collisions_columns::id.eq(latest_collision.id)),
                )
                .execute(conn)?;

                // delete the original card_metadata
                diesel::delete(
                    card_metadata_columns::card_metadata
                        .filter(card_metadata_columns::id.eq(card_uuid)),
                )
                .execute(conn)?;

                // set the card_metadata of latest_collision to have the qdrant_point_id of the original card_metadata
                diesel::update(
                    card_metadata_columns::card_metadata
                        .filter(card_metadata_columns::id.eq(latest_collision.card_id)),
                )
                .set((
                    card_metadata_columns::qdrant_point_id.eq(latest_collision.collision_qdrant_id),
                ))
                .execute(conn)?;

                // set the collision_qdrant_id of all other collisions to be the same as they were to begin with
                diesel::update(
                    card_collisions_columns::card_collisions.filter(
                        card_collisions_columns::id.eq_any(
                            card_collisions
                                .iter()
                                .skip(1)
                                .map(|x| x.0.id)
                                .collect::<Vec<uuid::Uuid>>(),
                        ),
                    ),
                )
                .set((card_collisions_columns::collision_qdrant_id
                    .eq(latest_collision.collision_qdrant_id),))
                .execute(conn)?;

                return Ok(TransactionResult::CardCollisionDetected);
            }

            // if there were no collisions, just delete the card_metadata without issue
            diesel::delete(
                card_metadata_columns::card_metadata
                    .filter(card_metadata_columns::id.eq(card_uuid)),
            )
            .execute(conn)?;

            Ok(TransactionResult::CardCollisionNotDetected)
        }
    });

    match transaction_result {
        Ok(result) => {
            if let TransactionResult::CardCollisionNotDetected = result {
                let qdrant = get_qdrant_connection().await?;
                let _ = qdrant
                    .delete_points(
                        "debate_cards",
                        &vec![<String as Into<PointId>>::into(card_uuid.to_string())].into(),
                        None,
                    )
                    .await
                    .map_err(|_e| {
                        Err::<(), DefaultError>(DefaultError {
                            message: "Failed to delete card from qdrant",
                        })
                    });
            }
        }

        Err(_) => {
            return Err(DefaultError {
                message: "Failed to delete card data",
            })
        }
    };

    Ok(())
}

pub fn get_card_count_query(pool: web::Data<Pool>) -> Result<i64, DefaultError> {
    use crate::data::schema::card_metadata::dsl::*;

    let mut conn = pool.get().unwrap();

    card_metadata
        .count()
        .get_result::<i64>(&mut conn)
        .map_err(|_err| DefaultError {
            message: "Failed to get card count",
        })
}
