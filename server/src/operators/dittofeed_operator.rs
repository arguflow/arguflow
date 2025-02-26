use actix_web::web;
use serde::{Deserialize, Serialize};

use crate::{
    data::models::{
        Dataset, DatasetDTO, DateRange, HeadQueries, Organization, Pool, SearchQueryEvent, SlimUser,
    },
    errors::ServiceError,
    handlers::{
        analytics_handler::GetTopDatasetsRequestBody, dataset_handler::GetDatasetsPagination,
    },
};

use super::{
    analytics_operator::{
        get_head_queries_query, get_low_confidence_queries_query, get_rag_usage_query,
        get_search_metrics_query, get_top_datasets_query,
    },
    dataset_operator::{get_dataset_by_id_query, get_datasets_by_organization_id},
    organization_operator::get_org_usage_by_id_query,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DittofeedIdentifyUser {
    pub r#type: Option<String>,
    pub message_id: uuid::Uuid,
    pub user_id: uuid::Uuid,
    pub traits: DittofeedUserTraits,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DittofeedUserTraits {
    pub email: String,
    pub name: Option<String>,
    pub created_at: chrono::NaiveDateTime,
    pub organization_count: i32,
    pub dataset_count: i32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DittoOrgUsage {
    pub organization: Organization,
    pub dataset_count: i32,
    pub top_search_datasets: Vec<Dataset>,
    pub top_rag_datasets: Vec<Dataset>,
    pub top_recommendation_datasets: Vec<Dataset>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DittoDatasetUsage {
    pub dataset: DatasetDTO,
    pub top_search_queries: Vec<HeadQueries>,
    pub low_confidence_search_queries: Vec<SearchQueryEvent>,
    pub chunk_count: i32,
    pub search_count: i64,
    pub rag_count: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DittoTrackProperties {
    DittoDatasetUsage(DittoDatasetUsage),
    DittoOrgUsage(DittoOrgUsage),
    DittoDatasetCreated(DittoDatasetCreated),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DittoDatasetCreated {
    pub dataset: DatasetDTO,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DittoTrackRequest {
    pub r#type: Option<String>,
    pub message_id: uuid::Uuid,
    pub event: String,
    pub properties: DittoTrackProperties,
    pub user_id: uuid::Uuid,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DittoBatchRequestTypes {
    Identify(DittofeedIdentifyUser),
    Track(DittoTrackRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DittoBatchRequest {
    pub batch: Vec<DittoBatchRequestTypes>,
}

pub async fn get_user_ditto_identity(
    user: SlimUser,
    pool: web::Data<Pool>,
    clickhouse_client: &clickhouse::Client,
) -> Result<DittoBatchRequest, ServiceError> {
    let organization_count = user.orgs.len() as i32;
    let mut org_usages: Vec<DittoOrgUsage> = vec![];
    let mut dataset_usages: Vec<DittoDatasetUsage> = vec![];

    for organization in user.orgs {
        let usage = get_org_usage_by_id_query(organization.id, pool.clone()).await?;

        let top_search_datasets_query = GetTopDatasetsRequestBody {
            r#type: crate::data::models::TopDatasetsRequestTypes::Search,
            date_range: Some(DateRange {
                gte: Some(
                    (chrono::Utc::now().naive_utc() - chrono::Duration::days(14))
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string(),
                ),
                ..Default::default()
            }),
        };

        let top_search_datasets_clickhouse = get_top_datasets_query(
            top_search_datasets_query,
            organization.id,
            clickhouse_client,
            pool.clone(),
        )
        .await?;

        let mut top_search_datasets = vec![];
        for dataset in top_search_datasets_clickhouse.iter() {
            let dataset = get_dataset_by_id_query(dataset.dataset_id, pool.clone()).await?;
            top_search_datasets.push(dataset);
        }

        let top_rag_datasets_query = GetTopDatasetsRequestBody {
            r#type: crate::data::models::TopDatasetsRequestTypes::RAG,
            date_range: Some(DateRange {
                gte: Some(
                    (chrono::Utc::now().naive_utc() - chrono::Duration::days(14))
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string(),
                ),
                ..Default::default()
            }),
        };

        let top_rag_datasets_clickhouse = get_top_datasets_query(
            top_rag_datasets_query,
            organization.id,
            clickhouse_client,
            pool.clone(),
        )
        .await?;

        let mut top_rag_datasets = vec![];
        for dataset in top_rag_datasets_clickhouse.iter() {
            let dataset = get_dataset_by_id_query(dataset.dataset_id, pool.clone()).await?;
            top_rag_datasets.push(dataset);
        }

        let top_recommendation_datasets_query = GetTopDatasetsRequestBody {
            r#type: crate::data::models::TopDatasetsRequestTypes::Recommendation,
            date_range: Some(DateRange {
                gte: Some(
                    (chrono::Utc::now().naive_utc() - chrono::Duration::days(14))
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string(),
                ),
                ..Default::default()
            }),
        };

        let top_recommendation_datasets_clickhouse = get_top_datasets_query(
            top_recommendation_datasets_query,
            organization.id,
            clickhouse_client,
            pool.clone(),
        )
        .await?;

        let mut top_recommendation_datasets = vec![];
        for dataset in top_recommendation_datasets_clickhouse.iter() {
            let dataset = get_dataset_by_id_query(dataset.dataset_id, pool.clone()).await?;
            top_recommendation_datasets.push(dataset);
        }

        let datasets = get_datasets_by_organization_id(
            organization.id,
            GetDatasetsPagination::default(),
            pool.clone(),
        )
        .await?;

        for dataset in datasets {
            let search_metrics =
                get_search_metrics_query(dataset.dataset.id, None, clickhouse_client).await?;
            let rag_metrics =
                get_rag_usage_query(dataset.dataset.id, None, clickhouse_client).await?;
            let top_search_queries =
                get_head_queries_query(dataset.dataset.id, None, None, clickhouse_client).await?;
            let low_confidence_search_queries = get_low_confidence_queries_query(
                dataset.dataset.id,
                None,
                None,
                None,
                clickhouse_client,
            )
            .await?;
            let usage = DittoDatasetUsage {
                dataset: dataset.dataset,
                top_search_queries: top_search_queries.queries,
                low_confidence_search_queries: low_confidence_search_queries.queries,
                chunk_count: dataset.dataset_usage.chunk_count,
                search_count: search_metrics.total_queries,
                rag_count: rag_metrics.total_queries,
            };
            dataset_usages.push(usage);
        }

        let org_usage = DittoOrgUsage {
            organization: organization.clone(),
            dataset_count: usage.dataset_count,
            top_search_datasets,
            top_rag_datasets,
            top_recommendation_datasets,
        };

        org_usages.push(org_usage);
    }

    let org_track_requests = org_usages
        .into_iter()
        .map(|usage| DittoTrackRequest {
            r#type: Some("track".to_string()),
            message_id: usage.organization.id,
            event: "ORGANIZATION_USAGE".to_string(),
            properties: DittoTrackProperties::DittoOrgUsage(usage),
            user_id: user.id,
        })
        .collect::<Vec<_>>();

    let dataset_track_requests = dataset_usages
        .into_iter()
        .map(|usage| DittoTrackRequest {
            r#type: Some("track".to_string()),
            message_id: usage.dataset.id,
            event: "DATASET_USAGE".to_string(),
            properties: DittoTrackProperties::DittoDatasetUsage(usage),
            user_id: user.id,
        })
        .collect::<Vec<_>>();

    let dataset_count = dataset_track_requests.len() as i32;

    let track_requests = org_track_requests
        .into_iter()
        .chain(dataset_track_requests.into_iter())
        .collect::<Vec<_>>();

    let batch_requests = vec![DittoBatchRequestTypes::Identify(DittofeedIdentifyUser {
        r#type: Some("identify".to_string()),
        message_id: uuid::Uuid::new_v4(),
        user_id: user.id,
        traits: DittofeedUserTraits {
            email: user.email,
            name: user.name,
            created_at: user.created_at,
            organization_count,
            dataset_count,
        },
    })];

    let batch_requests = batch_requests
        .into_iter()
        .chain(
            track_requests
                .into_iter()
                .map(DittoBatchRequestTypes::Track),
        )
        .collect::<Vec<_>>();

    let batch_request = DittoBatchRequest {
        batch: batch_requests,
    };

    Ok(batch_request)
}

pub async fn send_user_ditto_identity(
    batch_request: DittoBatchRequest,
) -> Result<(), ServiceError> {
    let dittofeed_url =
        std::env::var("DITTOFEED_URL").unwrap_or("https://app.dittofeed.com".to_string());
    let api_key = match std::env::var("DITTOFEED_API_KEY") {
        Ok(api_key) => api_key,
        Err(_) => {
            return Err(ServiceError::BadRequest(
                "DITTOFEED_API_KEY is not set".to_string(),
            ))
        }
    };

    let client = reqwest::Client::new();

    client
        .post(format!("{}/api/public/apps/batch", dittofeed_url))
        .header("Authorization", format!("Basic {}", api_key))
        .json(&batch_request)
        .send()
        .await
        .map_err(|e| ServiceError::BadRequest(e.to_string()))?
        .error_for_status()
        .map_err(|e| ServiceError::BadRequest(e.to_string()))?;

    Ok(())
}

pub async fn send_ditto_event(event: DittoTrackRequest) -> Result<(), ServiceError> {
    let dittofeed_url =
        std::env::var("DITTOFEED_URL").unwrap_or("https://app.dittofeed.com".to_string());
    let api_key = match std::env::var("DITTOFEED_API_KEY") {
        Ok(api_key) => api_key,
        Err(_) => {
            return Err(ServiceError::BadRequest(
                "DITTOFEED_API_KEY is not set".to_string(),
            ))
        }
    };

    let client = reqwest::Client::new();

    client
        .post(format!("{}/api/public/apps/track", dittofeed_url))
        .header("Authorization", format!("Basic {}", api_key))
        .json(&event)
        .send()
        .await
        .map_err(|e| ServiceError::BadRequest(e.to_string()))?
        .error_for_status()
        .map_err(|e| ServiceError::BadRequest(e.to_string()))?;

    Ok(())
}
