use super::auth_handler::AdminOnly;
use crate::{
    data::models::{
        CTRAnalytics, CTRAnalyticsResponse, CTRType, ClusterAnalytics, ClusterAnalyticsResponse,
        DatasetAndOrgWithSubAndPlan, DateRange, EventDataTypes, EventTypes, GetEventsRequestBody,
        OrganizationWithSubAndPlan, Pool, RAGAnalytics, RAGAnalyticsResponse,
        RecommendationAnalytics, RecommendationAnalyticsResponse, SearchAnalytics,
        SearchAnalyticsResponse, TopDatasetsRequestTypes,
    },
    errors::ServiceError,
    operators::{
        analytics_operator::*,
        clickhouse_operator::{ClickHouseEvent, EventQueue},
    },
};
use actix_web::{web, HttpResponse};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Get Cluster Analytics
///
/// This route allows you to view the cluster analytics for a dataset.
#[utoipa::path(
    post,
    path = "/analytics/search/cluster",
    context_path = "/api",
    tag = "Analytics",
    request_body(content = ClusterAnalytics, description = "JSON request payload to filter the graph", content_type = "application/json"),
    responses(
        (status = 200, description = "The cluster analytics for the dataset", body = ClusterAnalyticsResponse),

        (status = 400, description = "Service error relating to getting cluster analytics", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
    ),
    security(
        ("ApiKey" = ["admin"]),
    )
)]
pub async fn get_cluster_analytics(
    data: web::Json<ClusterAnalytics>,
    _user: AdminOnly,
    clickhouse_client: web::Data<clickhouse::Client>,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, ServiceError> {
    let response = match data.into_inner() {
        ClusterAnalytics::ClusterTopics { filter } => {
            let clusters = get_clusters_query(
                dataset_org_plan_sub.dataset.id,
                filter,
                clickhouse_client.get_ref(),
            )
            .await?;
            ClusterAnalyticsResponse::ClusterTopics(clusters)
        }
        ClusterAnalytics::ClusterQueries { cluster_id, page } => {
            let cluster_queries = get_queries_for_cluster_query(
                dataset_org_plan_sub.dataset.id,
                cluster_id,
                page,
                clickhouse_client.get_ref(),
            )
            .await?;
            ClusterAnalyticsResponse::ClusterQueries(cluster_queries)
        }
    };

    Ok(HttpResponse::Ok().json(response))
}

#[derive(Deserialize, Serialize, Clone, Debug, ToSchema)]
pub struct RateQueryRequest {
    pub query_id: uuid::Uuid,
    pub rating: i32,
    pub note: Option<String>,
    pub metadata: Option<serde_json::Value>,
}

/// Rate Search
///
/// This route allows you to Rate a search query.
#[utoipa::path(
    put,
    path = "/analytics/search",
    context_path = "/api",
    tag = "Analytics",
    request_body(content = RateQueryRequest, description = "JSON request payload to rate a search query", content_type = "application/json"),
    responses(
        (status = 204, description = "The search query was successfully rated"),

        (status = 400, description = "Service error relating to rating a search query", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
    ),
    security(
        ("ApiKey" = ["admin"]),
    )
)]
pub async fn set_search_query_rating(
    data: web::Json<RateQueryRequest>,
    _user: AdminOnly,
    event_queue: web::Data<EventQueue>,
    _dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, ServiceError> {
    let data = data.into_inner();

    event_queue
        .send(ClickHouseEvent::SearchQueryRatingEvent(data))
        .await;

    Ok(HttpResponse::NoContent().finish())
}

/// Rate RAG
///
/// This route allows you to Rate a RAG query.
#[utoipa::path(
    put,
    path = "/analytics/rag",
    context_path = "/api",
    tag = "Analytics",
    request_body(content = RateQueryRequest, description = "JSON request payload to rate a RAG query", content_type = "application/json"),
    responses(
        (status = 204, description = "The RAG query was successfully rated"),

        (status = 400, description = "Service error relating to rating a RAG query", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
    ),
    security(
        ("ApiKey" = ["admin"]),
    )
)]
pub async fn set_rag_query_rating(
    data: web::Json<RateQueryRequest>,
    _user: AdminOnly,
    event_queue: web::Data<EventQueue>,
    _dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, ServiceError> {
    let data = data.into_inner();

    event_queue
        .send(ClickHouseEvent::RagQueryRatingEvent(data))
        .await;

    Ok(HttpResponse::NoContent().finish())
}

/// Get Search Analytics
///
/// This route allows you to view the search analytics for a dataset.
#[utoipa::path(
    post,
    path = "/analytics/search",
    context_path = "/api",
    tag = "Analytics",
    request_body(content = SearchAnalytics, description = "JSON request payload to filter the graph", content_type = "application/json"),
    responses(
        (status = 200, description = "The search analytics for the dataset", body = SearchAnalyticsResponse),

        (status = 400, description = "Service error relating to getting search analytics", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
    ),
    security(
        ("ApiKey" = ["admin"]),
    )
)]
pub async fn get_search_analytics(
    data: web::Json<SearchAnalytics>,
    _user: AdminOnly,
    clickhouse_client: web::Data<clickhouse::Client>,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, ServiceError> {
    let response = match data.into_inner() {
        SearchAnalytics::LatencyGraph {
            filter,
            granularity,
        } => {
            let latency_graph = get_latency_graph_query(
                dataset_org_plan_sub.dataset.id,
                filter,
                granularity,
                clickhouse_client.get_ref(),
            )
            .await?;

            SearchAnalyticsResponse::LatencyGraph(latency_graph)
        }
        SearchAnalytics::SearchUsageGraph {
            filter,
            granularity,
        } => {
            let search_frequency_graph = get_search_usage_graph_query(
                dataset_org_plan_sub.dataset.id,
                filter,
                granularity,
                clickhouse_client.get_ref(),
            )
            .await?;

            SearchAnalyticsResponse::SearchUsageGraph(search_frequency_graph)
        }
        SearchAnalytics::SearchMetrics { filter } => {
            let search_metrics = get_search_metrics_query(
                dataset_org_plan_sub.dataset.id,
                filter,
                clickhouse_client.get_ref(),
            )
            .await?;

            SearchAnalyticsResponse::SearchMetrics(search_metrics)
        }
        SearchAnalytics::HeadQueries { filter, page } => {
            let head_queries = get_head_queries_query(
                dataset_org_plan_sub.dataset.id,
                filter,
                page,
                clickhouse_client.get_ref(),
            )
            .await?;

            SearchAnalyticsResponse::HeadQueries(head_queries)
        }
        SearchAnalytics::LowConfidenceQueries {
            filter,
            page,
            threshold,
        } => {
            let low_confidence_queries = get_low_confidence_queries_query(
                dataset_org_plan_sub.dataset.id,
                filter,
                threshold,
                page,
                clickhouse_client.get_ref(),
            )
            .await?;

            SearchAnalyticsResponse::LowConfidenceQueries(low_confidence_queries)
        }
        SearchAnalytics::NoResultQueries { filter, page } => {
            let no_result_queries = get_no_result_queries_query(
                dataset_org_plan_sub.dataset.id,
                filter,
                page,
                clickhouse_client.get_ref(),
            )
            .await?;

            SearchAnalyticsResponse::NoResultQueries(no_result_queries)
        }
        SearchAnalytics::SearchQueries {
            filter,
            page,
            sort_by,
            sort_order,
        } => {
            let search_queries = get_all_queries_query(
                dataset_org_plan_sub.dataset.id,
                filter,
                sort_by,
                sort_order,
                page,
                clickhouse_client.get_ref(),
            )
            .await?;

            SearchAnalyticsResponse::SearchQueries(search_queries)
        }
        SearchAnalytics::QueryDetails { request_id } => {
            let query = get_search_query(
                dataset_org_plan_sub.dataset.id,
                request_id,
                clickhouse_client.get_ref(),
            )
            .await?;

            SearchAnalyticsResponse::QueryDetails(query)
        }
        SearchAnalytics::CountQueries { filter } => {
            let count_queries = get_query_counts_query(
                dataset_org_plan_sub.dataset.id,
                filter,
                clickhouse_client.get_ref(),
            )
            .await?;

            SearchAnalyticsResponse::CountQueries(count_queries)
        }
        SearchAnalytics::PopularFilters { filter } => {
            let popular_filters = get_popular_filter_values_query(
                dataset_org_plan_sub.dataset.id,
                filter,
                clickhouse_client.get_ref(),
            )
            .await?;

            SearchAnalyticsResponse::PopularFilters(popular_filters)
        }
    };

    Ok(HttpResponse::Ok().json(response))
}

/// Get RAG Analytics
///
/// This route allows you to view the RAG analytics for a dataset.
#[utoipa::path(
    post,
    path = "/analytics/rag",
    context_path = "/api",
    tag = "Analytics",
    request_body(content = RAGAnalytics, description = "JSON request payload to filter the graph", content_type = "application/json"),
    responses(
        (status = 200, description = "The RAG analytics for the dataset", body = RAGAnalyticsResponse),

        (status = 400, description = "Service error relating to getting RAG analytics", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
    ),
    security(
        ("ApiKey" = ["admin"]),
    )
)]
pub async fn get_rag_analytics(
    data: web::Json<RAGAnalytics>,
    _user: AdminOnly,
    clickhouse_client: web::Data<clickhouse::Client>,
    pool: web::Data<Pool>,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, ServiceError> {
    let response = match data.into_inner() {
        RAGAnalytics::RAGUsage { filter } => {
            let rag_graph = get_rag_usage_query(
                dataset_org_plan_sub.dataset.id,
                filter,
                clickhouse_client.get_ref(),
            )
            .await?;
            RAGAnalyticsResponse::RAGUsage(rag_graph)
        }
        RAGAnalytics::RAGQueries {
            filter,
            page,
            sort_by,
            sort_order,
        } => {
            let rag_queries = get_rag_queries_query(
                dataset_org_plan_sub.dataset.id,
                filter,
                sort_by,
                sort_order,
                page,
                pool.clone(),
                clickhouse_client.get_ref(),
            )
            .await?;
            RAGAnalyticsResponse::RAGQueries(rag_queries)
        }

        RAGAnalytics::RAGUsageGraph {
            filter,
            granularity,
        } => {
            let rag = get_rag_usage_graph_query(
                dataset_org_plan_sub.dataset.id,
                filter,
                granularity,
                clickhouse_client.get_ref(),
            )
            .await?;
            RAGAnalyticsResponse::RAGUsageGraph(rag)
        }

        RAGAnalytics::RAGQueryDetails { request_id } => {
            let rag_query = get_rag_query(
                dataset_org_plan_sub.dataset.id,
                request_id,
                pool.clone(),
                clickhouse_client.get_ref(),
            )
            .await?;
            RAGAnalyticsResponse::RAGQueryDetails(Box::new(rag_query))
        }
        RAGAnalytics::RAGQueryRatings { filter } => {
            let rag_query_ratings = get_rag_query_ratings_query(
                dataset_org_plan_sub.dataset.id,
                filter,
                clickhouse_client.get_ref(),
            )
            .await?;
            RAGAnalyticsResponse::RAGQueryRatings(rag_query_ratings)
        }
    };

    Ok(HttpResponse::Ok().json(response))
}

/// Get Recommendation Analytics
///
/// This route allows you to view the recommendation analytics for a dataset.
#[utoipa::path(
    post,
    path = "/analytics/recommendations",
    context_path = "/api",
    tag = "Analytics",
    request_body(content = RecommendationAnalytics, description = "JSON request payload to filter the graph", content_type = "application/json"),
    responses(
        (status = 200, description = "The recommendation analytics for the dataset", body = RecommendationAnalyticsResponse),

        (status = 400, description = "Service error relating to getting recommendation analytics", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
    ),
    security(
        ("ApiKey" = ["admin"]),
    )
)]
pub async fn get_recommendation_analytics(
    data: web::Json<RecommendationAnalytics>,
    _user: AdminOnly,
    clickhouse_client: web::Data<clickhouse::Client>,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, ServiceError> {
    let response = match data.into_inner() {
        RecommendationAnalytics::LowConfidenceRecommendations {
            filter,
            page,
            threshold,
        } => {
            let low_confidence_recommendations = get_low_confidence_recommendations_query(
                dataset_org_plan_sub.dataset.id,
                filter,
                threshold,
                page,
                clickhouse_client.get_ref(),
            )
            .await?;
            RecommendationAnalyticsResponse::LowConfidenceRecommendations(
                low_confidence_recommendations,
            )
        }
        RecommendationAnalytics::RecommendationQueries {
            filter,
            page,
            sort_by,
            sort_order,
        } => {
            let recommendation_queries = get_recommendation_queries_query(
                dataset_org_plan_sub.dataset.id,
                filter,
                sort_by,
                sort_order,
                page,
                clickhouse_client.get_ref(),
            )
            .await?;
            RecommendationAnalyticsResponse::RecommendationQueries(recommendation_queries)
        }
        RecommendationAnalytics::QueryDetails { request_id } => {
            let recommendation_query = get_recommendation_query(
                dataset_org_plan_sub.dataset.id,
                request_id,
                clickhouse_client.get_ref(),
            )
            .await?;
            RecommendationAnalyticsResponse::QueryDetails(recommendation_query)
        }
    };

    Ok(HttpResponse::Ok().json(response))
}

#[derive(Deserialize, Serialize, Clone, Debug, ToSchema)]
pub struct CTRDataRequestBody {
    /// The request id for the CTR data
    pub request_id: uuid::Uuid,
    /// The type of CTR data being sent e.g. search or recommendation
    pub ctr_type: CTRType,
    /// The ID of chunk that was clicked
    pub clicked_chunk_id: Option<uuid::Uuid>,
    /// The tracking ID of the chunk that was clicked
    pub clicked_chunk_tracking_id: Option<String>,
    /// The position of the clicked chunk
    pub position: i32,
    /// Any metadata you want to include with the event i.e. action, user_id, etc.
    pub metadata: Option<serde_json::Value>,
}

/// Send CTR Data
///
/// This route allows you to send clickstream data to the system. Clickstream data is used to fine-tune the re-ranking of search results and recommendations.
#[utoipa::path(
    put,
    path = "/analytics/ctr",
    context_path = "/api",
    tag = "Analytics",
    request_body(content = CTRDataRequestBody, description = "JSON request payload to send CTR data", content_type = "application/json"),
    responses(
        (status = 204, description = "The CTR data was successfully sent"),

        (status = 400, description = "Service error relating to sending CTR data", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
    ),
    security(
        ("ApiKey" = ["admin"]),
    )
)]
#[deprecated]
pub async fn send_ctr_data(
    _user: AdminOnly,
    data: web::Json<CTRDataRequestBody>,
    event_queue: web::Data<EventQueue>,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, ServiceError> {
    let event_data =
        EventTypes::from(data.into_inner()).to_event_data(dataset_org_plan_sub.dataset.id);

    if let EventDataTypes::EventDataClickhouse(event_data) = event_data {
        event_queue
            .send(ClickHouseEvent::AnalyticsEvent(event_data))
            .await;
    }

    Ok(HttpResponse::NoContent().finish())
}

/// Send User Event Data
///
/// This route allows you to send user event data to the system.
#[utoipa::path(
    put,
    path = "/analytics/events",
    context_path = "/api",
    tag = "Analytics",
    request_body(content = EventTypes, description = "JSON request payload to send event data", content_type = "application/json"),
    responses(
        (status = 204, description = "The event data was successfully sent"),

        (status = 400, description = "Service error relating to sending event data", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
    ),
    security(
        ("ApiKey" = ["admin"]),
    )
)]
pub async fn send_event_data(
    _user: AdminOnly,
    data: web::Json<EventTypes>,
    event_queue: web::Data<EventQueue>,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, ServiceError> {
    let event_data = data
        .into_inner()
        .to_event_data(dataset_org_plan_sub.dataset.id);

    match event_data {
        EventDataTypes::EventDataClickhouse(event_data) => {
            event_queue
                .send(ClickHouseEvent::AnalyticsEvent(event_data))
                .await;
        }
        EventDataTypes::SearchQueryEventClickhouse(event_data) => {
            event_queue
                .send(ClickHouseEvent::SearchQueryEvent(event_data))
                .await;
        }
        EventDataTypes::RagQueryEventClickhouse(event_data) => {
            event_queue
                .send(ClickHouseEvent::RagQueryEvent(event_data))
                .await;
        }
        EventDataTypes::RecommendationEventClickhouse(event_data) => {
            event_queue
                .send(ClickHouseEvent::RecommendationEvent(event_data))
                .await;
        }
    }

    Ok(HttpResponse::NoContent().finish())
}

/// Get CTR Analytics
///
/// This route allows you to view the CTR analytics for a dataset.
#[utoipa::path(
    post,
    path = "/analytics/events/ctr",
    context_path = "/api",
    tag = "Analytics",
    request_body(content = CTRAnalytics, description = "JSON request payload to filter the graph", content_type = "application/json"),
    responses(
        (status = 200, description = "The CTR analytics for the dataset", body = CTRAnalyticsResponse),

        (status = 400, description = "Service error relating to getting CTR analytics", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
    ),
    security(
        ("ApiKey" = ["admin"]),
    )
)]
pub async fn get_ctr_analytics(
    _user: AdminOnly,
    data: web::Json<CTRAnalytics>,
    clickhouse_client: web::Data<clickhouse::Client>,
    pool: web::Data<Pool>,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, ServiceError> {
    let response = match data.into_inner() {
        CTRAnalytics::SearchCTRMetrics { filter } => {
            let ctr_metrics = get_search_ctr_metrics_query(
                dataset_org_plan_sub.dataset.id,
                filter,
                clickhouse_client.get_ref(),
            )
            .await?;

            CTRAnalyticsResponse::SearchCTRMetrics(ctr_metrics)
        }
        CTRAnalytics::SearchesWithClicks { filter, page } => {
            let searches_with_clicks = get_searches_with_clicks_query(
                dataset_org_plan_sub.dataset.id,
                page,
                filter,
                pool.clone(),
                clickhouse_client.get_ref(),
            )
            .await?;

            CTRAnalyticsResponse::SearchesWithClicks(searches_with_clicks)
        }
        CTRAnalytics::SearchesWithoutClicks { filter, page } => {
            let searches_without_clicks = get_searches_without_clicks_query(
                dataset_org_plan_sub.dataset.id,
                page,
                filter,
                clickhouse_client.get_ref(),
            )
            .await?;

            CTRAnalyticsResponse::SearchesWithoutClicks(searches_without_clicks)
        }
        CTRAnalytics::RecommendationCTRMetrics { filter } => {
            let ctr_metrics = get_recommendation_ctr_metrics_query(
                dataset_org_plan_sub.dataset.id,
                filter,
                clickhouse_client.get_ref(),
            )
            .await?;

            CTRAnalyticsResponse::RecommendationCTRMetrics(ctr_metrics)
        }
        CTRAnalytics::RecommendationsWithClicks { filter, page } => {
            let recommendations_with_clicks = get_recommendations_with_clicks_query(
                dataset_org_plan_sub.dataset.id,
                page,
                filter,
                pool.clone(),
                clickhouse_client.get_ref(),
            )
            .await?;

            CTRAnalyticsResponse::RecommendationsWithClicks(recommendations_with_clicks)
        }
        CTRAnalytics::RecommendationsWithoutClicks { filter, page } => {
            let recommendations_without_clicks = get_recommendations_without_clicks_query(
                dataset_org_plan_sub.dataset.id,
                page,
                filter,
                clickhouse_client.get_ref(),
            )
            .await?;

            CTRAnalyticsResponse::RecommendationsWithoutClicks(recommendations_without_clicks)
        }
    };

    Ok(HttpResponse::Ok().json(response))
}

/// Get All User Events
///
/// This route allows you to view all user events.
#[utoipa::path(
    post,
    path = "/analytics/events/all",
    context_path = "/api",
    tag = "Analytics",
    request_body(content = GetEventsRequestBody, description = "JSON request payload to filter the events", content_type = "application/json"),
    responses(
        (status = 200, description = "The events for the request", body = GetEventsResponseBody),

        (status = 400, description = "Service error relating to getting events", body = ErrorResponseBody),
    ),
    security(
        ("ApiKey" = ["admin"]),
    )
)]
pub async fn get_all_events(
    _user: AdminOnly,
    data: web::Json<GetEventsRequestBody>,
    clickhouse_client: web::Data<clickhouse::Client>,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, ServiceError> {
    let dataset_id = dataset_org_plan_sub.dataset.id;
    let events = get_all_events_query(
        dataset_id,
        data.page,
        data.filter.clone(),
        clickhouse_client.get_ref(),
    )
    .await?;

    Ok(HttpResponse::Ok().json(events))
}

/// Get User Event By ID
///
/// This route allows you to view an user event by its ID. You can pass in any type of event and get the details for that event.
#[utoipa::path(
    get,
    path = "/analytics/events/{event_id}",
    context_path = "/api",
    tag = "Analytics",
    responses(
        (status = 200, description = "The event for the request", body = EventData),

        (status = 400, description = "Service error relating to getting an event", body = ErrorResponseBody),
    ),
    params(
        ("TR-Dataset" = uuid::Uuid, Header, description = "The dataset id or tracking_id to use for the request. We assume you intend to use an id if the value is a valid uuid."),
        ("event_id" = uuid::Uuid, Path, description = "The event id to use for the request"),
    ),
    security(
        ("ApiKey" = ["admin"]),
    )
)]
pub async fn get_event_by_id(
    _user: AdminOnly,
    data: web::Path<uuid::Uuid>,
    clickhouse_client: web::Data<clickhouse::Client>,
    dataset_org_plan_sub: DatasetAndOrgWithSubAndPlan,
) -> Result<HttpResponse, ServiceError> {
    let dataset_id = dataset_org_plan_sub.dataset.id;
    let events =
        get_event_by_id_query(dataset_id, data.into_inner(), clickhouse_client.get_ref()).await?;

    Ok(HttpResponse::Ok().json(events))
}

#[derive(Deserialize, Serialize, Clone, Debug, ToSchema)]
pub struct GetTopDatasetsRequestBody {
    pub r#type: TopDatasetsRequestTypes,
    pub date_range: Option<DateRange>,
}

/// Get Top Datasets
///
/// This route allows you to view the top datasets for a given type.
#[utoipa::path(
    post,
    path = "/analytics/top",
    context_path = "/api",
    tag = "Analytics",
    request_body(content = GetTopDatasetsRequestBody, description = "JSON request payload to filter the top datasets", content_type = "application/json"),
    params(
        ("TR-Organization" = uuid::Uuid, Header, description = "The organization id to use for the request"),
    ),
    responses(
        (status = 200, description = "The top datasets for the request", body = Vec<TopDatasetsResponse>),
        (status = 400, description = "Service error relating to getting top datasets", body = ErrorResponseBody),
    ),
    security(
        ("ApiKey" = ["admin"]),
    )
)]
pub async fn get_top_datasets(
    _user: AdminOnly,
    data: web::Json<GetTopDatasetsRequestBody>,
    clickhouse_client: web::Data<clickhouse::Client>,
    org_with_plan_and_sub: OrganizationWithSubAndPlan,
    pool: web::Data<Pool>,
) -> Result<HttpResponse, ServiceError> {
    let top_datasets = get_top_datasets_query(
        data.into_inner(),
        org_with_plan_and_sub.organization.id,
        clickhouse_client.get_ref(),
        pool,
    )
    .await?;

    Ok(HttpResponse::Ok().json(top_datasets))
}
