use crate::data::models::CrawlOptions;
use crate::data::models::CrawlStatus;
use crate::data::models::CrawlType;
use crate::data::models::FirecrawlCrawlRequest;
use crate::handlers::chunk_handler::ChunkReqPayload;
use crate::handlers::chunk_handler::CrawlInterval;
use crate::handlers::chunk_handler::FullTextBoost;
use crate::handlers::chunk_handler::SemanticBoost;
use crate::handlers::crawl_handler::GetCrawlRequestsReqPayload;
use crate::{
    data::models::{CrawlRequest, CrawlRequestPG, Pool, ScrapeOptions},
    errors::ServiceError,
};
use actix_web::web;
use broccoli_queue::queue::BroccoliQueue;
use diesel::prelude::*;
use diesel::QueryDsl;
use diesel_async::RunQueryDsl;
use regex::Regex;
use reqwest::Url;
use scraper::Html;
use scraper::Selector;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::chunk_operator::create_chunk_metadata;
use super::organization_operator::hash_function;
use super::parse_operator::convert_html_to_text;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IngestResult {
    pub status: Status,
    pub completed: u32,
    pub total: u32,
    #[serde(rename = "expiresAt")]
    pub expires_at: String,
    pub next: Option<String>,
    pub data: Option<Vec<Option<Document>>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Scraping,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, Hash, Eq, PartialEq)]
pub struct Document {
    pub markdown: Option<String>,
    pub extract: Option<String>,
    pub html: Option<String>,
    #[serde(rename = "rawHtml")]
    pub raw_html: Option<String>,
    pub links: Option<Vec<String>>,
    pub screenshot: Option<String>,
    pub metadata: Metadata,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, Hash, Eq, PartialEq)]
pub struct Metadata {
    pub title: Option<String>,
    pub description: Option<String>,
    pub language: Option<String>,
    pub keywords: Option<String>,
    pub robots: Option<String>,
    #[serde(rename = "ogTitle")]
    pub og_title: Option<String>,
    #[serde(rename = "ogDescription")]
    pub og_description: Option<String>,
    #[serde(rename = "ogUrl")]
    pub og_url: Option<String>,
    #[serde(rename = "ogImage")]
    pub og_image: Option<String>,
    #[serde(rename = "ogAudio")]
    pub og_audio: Option<String>,
    #[serde(rename = "ogDeterminer")]
    pub og_determiner: Option<String>,
    #[serde(rename = "ogLocale")]
    pub og_locale: Option<String>,
    #[serde(rename = "ogLocaleAlternate")]
    pub og_locale_alternate: Option<Vec<String>>,
    #[serde(rename = "ogSiteName")]
    pub og_site_name: Option<String>,
    #[serde(rename = "ogVideo")]
    pub og_video: Option<String>,
    #[serde(rename = "dcTermsCreated")]
    pub dc_terms_created: Option<String>,
    #[serde(rename = "dcDateCreated")]
    pub dc_date_created: Option<String>,
    #[serde(rename = "dcDate")]
    pub dc_date: Option<String>,
    #[serde(rename = "dcTermsType")]
    pub dc_terms_type: Option<String>,
    #[serde(rename = "dcType")]
    pub dc_type: Option<String>,
    #[serde(rename = "dcTermsAudience")]
    pub dc_terms_audience: Option<String>,
    #[serde(rename = "dcTermsSubject")]
    pub dc_terms_subject: Option<String>,
    #[serde(rename = "dcSubject")]
    pub dc_subject: Option<String>,
    #[serde(rename = "dcDescription")]
    pub dc_description: Option<String>,
    #[serde(rename = "dcTermsKeywords")]
    pub dc_terms_keywords: Option<String>,
    #[serde(rename = "modifiedTime")]
    pub modified_time: Option<String>,
    #[serde(rename = "publishedTime")]
    pub published_time: Option<String>,
    #[serde(rename = "articleTag")]
    pub article_tag: Option<String>,
    #[serde(rename = "articleSection")]
    pub article_section: Option<String>,
    #[serde(rename = "sourceURL")]
    pub source_url: Option<String>,
    #[serde(rename = "statusCode")]
    pub status_code: Option<u32>,
    pub error: Option<String>,
    pub site_map: Option<Sitemap>,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, Hash, Eq, PartialEq)]
pub struct Sitemap {
    pub changefreq: String,
}

pub fn validate_crawl_options(crawl_options: &CrawlOptions) -> Result<CrawlOptions, ServiceError> {
    if crawl_options.allow_external_links.is_some_and(|v| v)
        && !crawl_options
            .clone()
            .include_paths
            .unwrap_or_default()
            .into_iter()
            .any(|path| path != "*")
    {
        return Err(ServiceError::BadRequest(
            "If allow_external_links is true, include_paths must contain at least one path that is not '*'".to_string(),
        ));
    }
    Ok(crawl_options.clone())
}

pub async fn create_crawl_query(
    crawl_options: CrawlOptions,
    pool: web::Data<Pool>,
    broccoli_queue: web::Data<BroccoliQueue>,
    dataset_id: uuid::Uuid,
) -> Result<CrawlRequest, ServiceError> {
    validate_crawl_options(&crawl_options)?;

    let webhook_url = format!(
        "{}/api/file/html_page",
        std::env::var("BASE_SERVER_URL").unwrap_or("https://api.trieve.ai".to_string())
    );
    let webhook_metadata = serde_json::json!({
        "dataset_id": dataset_id,
        "webhook_secret": hash_function(std::env::var("STRIPE_WEBHOOK_SECRET").unwrap_or("firecrawl".to_string()).as_str())
    });
    let mut crawl_options = crawl_options.clone();
    crawl_options.webhook_url = Some(webhook_url);
    crawl_options.webhook_metadata = Some(webhook_metadata);

    let scrape_id = if let Some(ScrapeOptions::Shopify(_)) | Some(ScrapeOptions::Youtube(_)) =
        crawl_options.scrape_options
    {
        uuid::Uuid::nil()
    } else {
        crawl_site(crawl_options.clone())
            .await
            .map_err(|err| ServiceError::BadRequest(format!("Could not crawl site: {}", err)))?
    };

    let crawl =
        create_crawl_request(crawl_options, dataset_id, scrape_id, broccoli_queue, pool).await?;
    Ok(crawl)
}

pub async fn get_crawl_requests_by_dataset_id_query(
    options: GetCrawlRequestsReqPayload,
    dataset_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Vec<CrawlRequest>, ServiceError> {
    use crate::data::schema::crawl_requests::dsl as crawl_requests_table;
    let mut conn = pool
        .get()
        .await
        .map_err(|e| ServiceError::InternalServerError(e.to_string()))?;
    let request: Vec<CrawlRequestPG> = crawl_requests_table::crawl_requests
        .filter(crawl_requests_table::dataset_id.eq(dataset_id))
        .select((
            crawl_requests_table::id,
            crawl_requests_table::url,
            crawl_requests_table::status,
            crawl_requests_table::crawl_type,
            crawl_requests_table::next_crawl_at,
            crawl_requests_table::interval,
            crawl_requests_table::crawl_options,
            crawl_requests_table::scrape_id,
            crawl_requests_table::dataset_id,
            crawl_requests_table::created_at,
        ))
        .order_by(crawl_requests_table::created_at.desc())
        .limit(options.limit.unwrap_or(10))
        .offset((options.page.unwrap_or(1) - 1) * options.limit.unwrap_or(10))
        .load(&mut conn)
        .await
        .map_err(|e| ServiceError::InternalServerError(e.to_string()))?;

    Ok(request.into_iter().map(|req| req.into()).collect())
}

pub async fn get_crawl_requests_to_rerun(
    pool: web::Data<Pool>,
) -> Result<Vec<CrawlRequest>, ServiceError> {
    use crate::data::schema::crawl_requests::dsl::*;
    let mut conn = pool
        .get()
        .await
        .map_err(|e| ServiceError::InternalServerError(e.to_string()))?;
    let requests = crawl_requests
        .select((
            id,
            url,
            status,
            crawl_type,
            next_crawl_at,
            interval,
            crawl_options,
            scrape_id,
            dataset_id,
            created_at,
        ))
        .filter(next_crawl_at.le(chrono::Utc::now().naive_utc()))
        .load::<CrawlRequestPG>(&mut conn)
        .await
        .map_err(|e| ServiceError::InternalServerError(e.to_string()))?;
    Ok(requests.into_iter().map(|r| r.into()).collect())
}

pub async fn create_crawl_request(
    crawl_options: CrawlOptions,
    dataset_id: uuid::Uuid,
    scrape_id: uuid::Uuid,
    broccoli_queue: web::Data<BroccoliQueue>,
    pool: web::Data<Pool>,
) -> Result<CrawlRequest, ServiceError> {
    use crate::data::schema::crawl_requests::dsl as crawl_requests_table;

    let interval = match crawl_options.interval {
        Some(CrawlInterval::Daily) => std::time::Duration::from_secs(60 * 60 * 24),
        Some(CrawlInterval::Weekly) => std::time::Duration::from_secs(60 * 60 * 24 * 7),
        Some(CrawlInterval::Monthly) => std::time::Duration::from_secs(60 * 60 * 24 * 30),
        None => std::time::Duration::from_secs(60 * 60 * 24),
    };

    let new_crawl_request = CrawlRequest {
        id: uuid::Uuid::new_v4(),
        url: crawl_options.site_url.clone().unwrap_or_default(),
        status: CrawlStatus::Pending,
        crawl_type: crawl_options
            .scrape_options
            .clone()
            .map(|s| s.into())
            .unwrap_or(CrawlType::Firecrawl),
        interval,
        next_crawl_at: chrono::Utc::now().naive_utc() + interval,
        crawl_options,
        scrape_id,
        dataset_id,
        created_at: chrono::Utc::now().naive_utc(),
        attempt_number: 0,
    };

    let pg_crawl_request: CrawlRequestPG = new_crawl_request.clone().into();

    let mut conn = pool
        .get()
        .await
        .map_err(|e| ServiceError::InternalServerError(e.to_string()))?;

    diesel::insert_into(crawl_requests_table::crawl_requests)
        .values(&pg_crawl_request)
        .execute(&mut conn)
        .await
        .map_err(|e| ServiceError::InternalServerError(e.to_string()))?;

    if new_crawl_request.crawl_type != CrawlType::Firecrawl {
        broccoli_queue
            .publish("crawl_queue", None, &new_crawl_request, None)
            .await
            .map_err(|e| {
                log::error!("Error publishing message to crawl_queue: {:?}", e);
                ServiceError::InternalServerError(
                    "Error publishing message to crawl_queue".to_string(),
                )
            })?;
    }

    Ok(new_crawl_request)
}

pub async fn update_crawl_status(
    crawl_id: uuid::Uuid,
    status: CrawlStatus,
    pool: web::Data<Pool>,
) -> Result<(), ServiceError> {
    use crate::data::schema::crawl_requests::dsl as crawl_requests_table;

    let mut conn = pool
        .get()
        .await
        .map_err(|e| ServiceError::InternalServerError(e.to_string()))?;

    diesel::update(
        crawl_requests_table::crawl_requests.filter(crawl_requests_table::id.eq(crawl_id)),
    )
    .set(crawl_requests_table::status.eq(status.to_string()))
    .execute(&mut conn)
    .await
    .map_err(|e| ServiceError::InternalServerError(e.to_string()))?;

    Ok(())
}

pub async fn update_next_crawl_at(
    crawl_id: uuid::Uuid,
    next_crawl_at: chrono::NaiveDateTime,
    pool: web::Data<Pool>,
) -> Result<(), ServiceError> {
    use crate::data::schema::crawl_requests::dsl as crawl_requests_table;
    let mut conn = pool
        .get()
        .await
        .map_err(|e| ServiceError::InternalServerError(e.to_string()))?;
    diesel::update(
        crawl_requests_table::crawl_requests.filter(crawl_requests_table::scrape_id.eq(crawl_id)),
    )
    .set(crawl_requests_table::next_crawl_at.eq(next_crawl_at))
    .execute(&mut conn)
    .await
    .map_err(|e| ServiceError::InternalServerError(e.to_string()))?;
    Ok(())
}

pub async fn update_crawl_query(
    crawl_options: CrawlOptions,
    crawl_id: uuid::Uuid,
    dataset_id: uuid::Uuid,
    pool: web::Data<Pool>,
    broccoli_queue: web::Data<BroccoliQueue>,
) -> Result<CrawlRequest, ServiceError> {
    use crate::data::schema::crawl_requests::dsl as crawl_requests_table;
    let mut conn = pool
        .get()
        .await
        .map_err(|e| ServiceError::InternalServerError(e.to_string()))?;

    let prev_crawl_req = crawl_requests_table::crawl_requests
        .select((
            crawl_requests_table::id,
            crawl_requests_table::url,
            crawl_requests_table::status,
            crawl_requests_table::crawl_type,
            crawl_requests_table::next_crawl_at,
            crawl_requests_table::interval,
            crawl_requests_table::crawl_options,
            crawl_requests_table::scrape_id,
            crawl_requests_table::dataset_id,
            crawl_requests_table::created_at,
        ))
        .filter(crawl_requests_table::dataset_id.eq(dataset_id))
        .first::<CrawlRequestPG>(&mut conn)
        .await
        .optional()?;

    diesel::delete(
        crawl_requests_table::crawl_requests.filter(crawl_requests_table::scrape_id.eq(crawl_id)),
    )
    .execute(&mut conn)
    .await
    .map_err(|e| {
        log::error!("Error deleting crawl request: {:?}", e);
        ServiceError::InternalServerError("Error deleting crawl request".to_string())
    })?;

    let merged_options = if let Some(prev_crawl_req) = prev_crawl_req {
        let previous_crawl_options: CrawlOptions =
            serde_json::from_value(prev_crawl_req.crawl_options)
                .map_err(|e| ServiceError::InternalServerError(e.to_string()))?;
        crawl_options.merge(previous_crawl_options)
    } else {
        crawl_options
    };

    let crawl = create_crawl_query(
        merged_options.clone(),
        pool.clone(),
        broccoli_queue.clone(),
        dataset_id,
    )
    .await?;

    Ok(crawl)
}

pub async fn delete_crawl_query(
    crawl_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<(), ServiceError> {
    use crate::data::schema::crawl_requests::dsl as crawl_requests_table;
    let mut conn = pool
        .get()
        .await
        .map_err(|e| ServiceError::InternalServerError(e.to_string()))?;
    diesel::delete(
        crawl_requests_table::crawl_requests.filter(crawl_requests_table::id.eq(crawl_id)),
    )
    .execute(&mut conn)
    .await
    .map_err(|e| ServiceError::InternalServerError(e.to_string()))?;
    Ok(())
}

pub async fn update_scrape_id(
    scrape_id: uuid::Uuid,
    new_scrape_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<CrawlRequest, ServiceError> {
    use crate::data::schema::crawl_requests::dsl as crawl_requests_table;
    let mut conn = pool
        .get()
        .await
        .map_err(|e| ServiceError::InternalServerError(e.to_string()))?;
    let updated_request = diesel::update(
        crawl_requests_table::crawl_requests.filter(crawl_requests_table::scrape_id.eq(scrape_id)),
    )
    .set(crawl_requests_table::scrape_id.eq(new_scrape_id))
    .returning(CrawlRequestPG::as_returning())
    .get_result(&mut conn)
    .await
    .map_err(|e| ServiceError::InternalServerError(e.to_string()))?;

    Ok(updated_request.into())
}

pub async fn get_crawl_by_scrape_id_query(
    scrape_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<CrawlRequest, ServiceError> {
    use crate::data::schema::crawl_requests::dsl as crawl_requests_table;
    let mut conn = pool
        .get()
        .await
        .map_err(|e| ServiceError::InternalServerError(e.to_string()))?;
    let request: CrawlRequestPG = crawl_requests_table::crawl_requests
        .select((
            crawl_requests_table::id,
            crawl_requests_table::url,
            crawl_requests_table::status,
            crawl_requests_table::crawl_type,
            crawl_requests_table::next_crawl_at,
            crawl_requests_table::interval,
            crawl_requests_table::crawl_options,
            crawl_requests_table::scrape_id,
            crawl_requests_table::dataset_id,
            crawl_requests_table::created_at,
        ))
        .filter(crawl_requests_table::scrape_id.eq(scrape_id))
        .first(&mut conn)
        .await
        .map_err(|e| ServiceError::InternalServerError(e.to_string()))?;
    Ok(request.into())
}

pub async fn get_crawl_from_firecrawl(scrape_id: uuid::Uuid) -> Result<IngestResult, ServiceError> {
    log::info!("Getting crawl from firecrawl");

    let firecrawl_url =
        std::env::var("FIRECRAWL_URL").unwrap_or_else(|_| "https://api.firecrawl.dev".to_string());
    let firecrawl_api_key = std::env::var("FIRECRAWL_API_KEY").unwrap_or_else(|_| "".to_string());
    let mut firecrawl_url = format!("{}/v1/crawl/{}", firecrawl_url, scrape_id);

    let mut collected_docs: Vec<Option<Document>> = vec![];
    let mut resp = None;

    let client = reqwest::Client::new();

    while resp.is_none() {
        let response = client
            .get(&firecrawl_url)
            .header("Authorization", format!("Bearer {}", firecrawl_api_key))
            .send()
            .await
            .map_err(|e| {
                log::error!("Error sending request to firecrawl: {:?}", e);
                ServiceError::InternalServerError("Error sending request to firecrawl".to_string())
            })?;

        if !response.status().is_success() {
            log::error!(
                "Error getting response from firecrawl: {:?}",
                response.text().await
            );
            return Err(ServiceError::InternalServerError(
                "Error getting response from firecrawl".to_string(),
            ));
        };

        let ingest_result = response.json::<IngestResult>().await.map_err(|e| {
            log::error!("Error parsing response from firecrawl: {:?}", e);
            ServiceError::InternalServerError("Error parsing response from firecrawl".to_string())
        })?;

        if ingest_result.status != Status::Completed && ingest_result.status != Status::Scraping {
            log::info!("Crawl status: {:?}", ingest_result.status);
            return Ok(ingest_result);
        }

        let cur_docs = ingest_result.clone().data.unwrap_or_default();
        collected_docs.extend(cur_docs);

        if let Some(ref next_ingest_result) = ingest_result.next {
            let next_ingest_result = next_ingest_result.replace("https://", "http://");

            log::info!(
                "Next ingest url: {} | prev {}",
                next_ingest_result,
                firecrawl_url
            );
            if next_ingest_result == firecrawl_url {
                log::info!("Breaking loop");
                resp = Some(ingest_result.clone());
                break;
            }

            firecrawl_url = next_ingest_result;
        } else {
            resp = Some(ingest_result.clone());
        }
    }

    match resp {
        Some(resp) => Ok(IngestResult {
            status: resp.status,
            completed: resp.completed,
            total: resp.total,
            expires_at: resp.expires_at,
            next: None,
            data: Some(collected_docs),
        }),
        None => Err(ServiceError::InternalServerError(
            "Error getting response from firecrawl".to_string(),
        )),
    }
}

pub async fn crawl_site(crawl_options: CrawlOptions) -> Result<uuid::Uuid, ServiceError> {
    let firecrawl_url =
        std::env::var("FIRECRAWL_URL").unwrap_or_else(|_| "https://api.firecrawl.dev".to_string());
    let firecrawl_api_key = std::env::var("FIRECRAWL_API_KEY").unwrap_or_else(|_| "".to_string());
    let firecrawl_url = format!("{}/v1/crawl", firecrawl_url);
    let client = reqwest::Client::new();
    let response = client
        .post(&firecrawl_url)
        .json(&FirecrawlCrawlRequest::from(crawl_options))
        .header("Authorization", format!("Bearer {}", firecrawl_api_key))
        .send()
        .await
        .map_err(|e| {
            log::error!("Error sending request to firecrawl: {:?}", e);
            ServiceError::InternalServerError("Error sending request to firecrawl".to_string())
        })?;

    if response.status().is_success() {
        let json = response.json::<serde_json::Value>().await.map_err(|e| {
            log::error!("Error parsing response from firecrawl: {:?}", e);
            ServiceError::InternalServerError("Error parsing response from firecrawl".to_string())
        })?;

        Ok(json.get("id").unwrap().as_str().unwrap().parse().unwrap())
    } else {
        log::error!(
            "Error getting response from firecrawl: {:?}",
            response.text().await
        );
        Err(ServiceError::InternalServerError(
            "Error getting response from firecrawl".to_string(),
        ))
    }
}

pub fn get_tags(url: String) -> Vec<String> {
    if let Ok(parsed_url) = Url::parse(&url) {
        let path_parts: Vec<&str> = parsed_url.path().split('/').collect();
        return path_parts
            .iter()
            .filter_map(|part| {
                if !part.is_empty() {
                    Some(part.to_string())
                } else {
                    None
                }
            })
            .collect();
    }
    Vec::new()
}

pub fn chunk_html(
    html: &str,
    heading_remove_strings: Option<Vec<String>>,
    body_remove_strings: Option<Vec<String>>,
) -> Vec<(String, String)> {
    let re = Regex::new(r"(?i)<h[1-6].*?>").unwrap();
    let mut chunks = Vec::new();
    let mut current_chunk = String::new();
    let mut last_end = 0;
    let mut short_chunk: Option<String> = None;

    for cap in re.find_iter(html) {
        if last_end != cap.start() {
            current_chunk.push_str(&html[last_end..cap.start()]);
        }

        if !current_chunk.is_empty() {
            let trimmed_chunk = current_chunk.trim().to_string();

            if let Some(prev_short_chunk) = short_chunk.take() {
                current_chunk = format!("{} {}", prev_short_chunk, trimmed_chunk);
            } else {
                current_chunk = trimmed_chunk;
            }

            let chunk_text = convert_html_to_text(&current_chunk);

            if chunk_text.split_whitespace().count() > 5 {
                let headings_text = extract_all_headings(&current_chunk);

                if chunk_text
                    .replace(headings_text.as_str(), "")
                    .trim()
                    .is_empty()
                {
                    short_chunk = Some(current_chunk);
                } else {
                    chunks.push((headings_text, current_chunk));
                }
            } else {
                short_chunk = Some(current_chunk);
            }
        }

        current_chunk = cap.as_str().to_string();
        last_end = cap.end();
    }

    if last_end < html.len() {
        current_chunk.push_str(&html[last_end..]);
    }

    if !current_chunk.is_empty() {
        let trimmed_chunk = current_chunk.trim().to_string();

        if let Some(prev_short_chunk) = short_chunk.take() {
            current_chunk = format!("{} {}", prev_short_chunk, trimmed_chunk);
        } else {
            current_chunk = trimmed_chunk;
        }

        let headings_text = extract_all_headings(&current_chunk);
        chunks.push((headings_text, current_chunk));
    } else if let Some(last_short_chunk) = short_chunk {
        let headings_text = extract_all_headings(&last_short_chunk);
        chunks.push((headings_text, last_short_chunk));
    }

    chunks = chunks
        .into_iter()
        .map(|(headings_text, content)| {
            let mut headings_text = headings_text.clone();
            let mut content = content.clone();
            if let Some(heading_remove_strings) = &heading_remove_strings {
                heading_remove_strings.iter().for_each(|remove_string| {
                    headings_text = headings_text.replace(remove_string, "");
                });
            }
            if let Some(body_remove_strings) = &body_remove_strings {
                body_remove_strings.iter().for_each(|remove_string| {
                    content = content.replace(remove_string, "");
                });
            }
            (headings_text, content)
        })
        .collect();
    chunks.retain(|(headings_text, content)| {
        !headings_text.trim().is_empty() && !content.trim().is_empty()
    });

    chunks
}

fn extract_all_headings(html: &str) -> String {
    let fragment = Html::parse_fragment(html);
    let heading_selector = Selector::parse("h1, h2, h3, h4, h5, h6").unwrap();

    fragment
        .select(&heading_selector)
        .map(|element| element.text().collect::<String>())
        .collect::<Vec<String>>()
        .join("\n")
}

pub async fn process_crawl_doc(
    dataset_id: uuid::Uuid,
    scrape_id: uuid::Uuid,
    crawl_doc: Document,
    broccoli_queue: web::Data<BroccoliQueue>,
    pool: web::Data<Pool>,
) -> Result<(), ServiceError> {
    if crawl_doc.metadata.status_code != Some(200) {
        log::error!("Error getting metadata for page: {:?}", crawl_doc.metadata);
        return Err(ServiceError::BadRequest(
            "Error getting metadata for page".to_string(),
        ));
    }

    let prev_crawl = get_crawl_by_scrape_id_query(scrape_id, pool.clone())
        .await
        .map_err(|e| {
            log::error!("Error getting crawl request: {:?}", e);
            ServiceError::InternalServerError("Error getting crawl request".to_string())
        })?;

    let last_page_num = match prev_crawl.status {
        CrawlStatus::Processing(page_num) => page_num,
        _ => 0,
    };

    update_crawl_status(
        prev_crawl.id,
        CrawlStatus::Processing(last_page_num + 1),
        pool.clone(),
    )
    .await
    .map_err(|e| {
        log::error!("Error updating crawl status: {:?}", e);
        ServiceError::InternalServerError("Error updating crawl status".to_string())
    })?;

    let page_link = crawl_doc
        .metadata
        .source_url
        .clone()
        .unwrap_or_default()
        .trim_end_matches("/")
        .to_string();
    if page_link.is_empty() {
        log::error!(
            "Error page source_url is not present for page_metadata: {:?}",
            crawl_doc.metadata
        );
        return Ok(());
    }

    let page_title = crawl_doc.metadata.og_title.clone().unwrap_or_default();
    let page_description = crawl_doc
        .metadata
        .og_description
        .clone()
        .unwrap_or(crawl_doc.metadata.description.unwrap_or_default().clone());
    let page_html = crawl_doc.html.clone().unwrap_or_default();
    let page_tags = get_tags(page_link.clone());

    let chunked_html = chunk_html(&page_html.clone(), None, None);
    let mut chunks = vec![];

    for chunk in chunked_html {
        let heading = chunk.0.clone();
        let chunk_html = chunk.1.clone();

        if chunk_html.is_empty() {
            log::error!("Skipping empty chunk for page: {}", page_link);
            return Ok(());
        }

        let mut metadata = serde_json::json!({
            "url": page_link.clone(),
            "hierarchy": chunk.0.clone(),
        });

        let mut semantic_boost_phrase = heading.clone();
        let mut fulltext_boost_phrase = heading.clone();
        metadata["heading"] = serde_json::json!(heading.clone());

        if !page_title.is_empty() {
            semantic_boost_phrase.push_str(format!("\n\n{}", page_title).as_str());
            fulltext_boost_phrase.push_str(format!("\n\n{}", page_title).as_str());

            metadata["title"] = serde_json::json!(page_title.clone());
            metadata["hierarchy"]
                .as_array_mut()
                .unwrap_or(&mut vec![])
                .insert(0, serde_json::json!(page_title.clone()));
        }
        if !page_description.is_empty() {
            semantic_boost_phrase.push_str(format!("\n\n{}", page_description).as_str());

            metadata["description"] = serde_json::json!(page_description.clone());
        }

        let tracking_hash_val = if heading.is_empty() {
            chunk_html.clone()
        } else {
            heading.clone()
        };

        let chunk = ChunkReqPayload {
            chunk_html: Some(chunk_html.clone()),
            link: Some(page_link.clone()),
            tag_set: Some(page_tags.clone()),
            metadata: Some(serde_json::json!(metadata)),
            tracking_id: Some(hash_function(&format!(
                "{}{}",
                page_link
                    .trim_end_matches("/")
                    .split("/")
                    .collect::<Vec<&str>>()
                    .split_at(3)
                    .1
                    .join("/"),
                tracking_hash_val
            ))),
            upsert_by_tracking_id: Some(true),
            group_tracking_ids: Some(vec![if !page_title.is_empty() {
                page_title.clone()
            } else {
                page_link.clone()
            }]),
            fulltext_boost: if !fulltext_boost_phrase.is_empty() {
                Some(FullTextBoost {
                    phrase: fulltext_boost_phrase,
                    boost_factor: 1.3,
                })
            } else {
                None
            },
            semantic_boost: if !semantic_boost_phrase.is_empty() {
                Some(SemanticBoost {
                    phrase: semantic_boost_phrase,
                    distance_factor: 0.3,
                })
            } else {
                None
            },
            convert_html_to_text: Some(true),
            ..Default::default()
        };
        chunks.push(chunk);
    }

    let chunks_to_upload = chunks.chunks(120);
    for batch in chunks_to_upload {
        let (chunk_ingestion_message, chunk_metadatas) =
            create_chunk_metadata(batch.to_vec(), dataset_id)?;

        if !chunk_metadatas.is_empty() {
            broccoli_queue
                .publish(
                    "ingestion",
                    Some(dataset_id.to_string()),
                    &chunk_ingestion_message,
                    None,
                )
                .await
                .map_err(|e| {
                    log::error!("Could not publish message {:?}", e);
                    ServiceError::BadRequest("Could not publish message".to_string())
                })?;
        }
    }

    Ok(())
}
