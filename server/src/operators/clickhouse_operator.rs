use std::time::Duration;
use time::Instant;
use tokio::sync::mpsc;

use crate::{
    data::models::{
        EventDataClickhouse, RagQueryEventClickhouse, RecommendationEventClickhouse,
        SearchQueryEventClickhouse, SearchQueryRating, WorkerEventClickhouse,
    },
    errors::ServiceError,
    handlers::analytics_handler::RateQueryRequest,
};

#[derive(Debug, Clone)]
pub enum ClickHouseEvent {
    SearchQueryEvent(SearchQueryEventClickhouse),
    RecommendationEvent(RecommendationEventClickhouse),
    RagQueryEvent(RagQueryEventClickhouse),
    AnalyticsEvent(EventDataClickhouse),
    WorkerEvent(WorkerEventClickhouse),
    RagQueryRatingEvent(RateQueryRequest),
    SearchQueryRatingEvent(RateQueryRequest),
}

pub fn get_latency_from_header(header: String) -> f32 {
    header
        .split(", ")
        .filter_map(|s| {
            // Find the "dur=" substring and parse the following value as f32
            s.split(";dur=")
                .nth(1)
                .and_then(|dur| dur.parse::<f32>().ok())
        })
        .sum()
}

pub async fn send_to_clickhouse(
    events: Vec<ClickHouseEvent>,
    clickhouse_client: &clickhouse::Client,
) -> Result<(), ServiceError> {
    if std::env::var("USE_ANALYTICS").unwrap_or("false".to_string()) != "true" {
        return Ok(());
    }

    let mut search_queries_inserter = clickhouse_client.insert("search_queries").map_err(|e| {
        log::error!("Error inserting search queries: {:?}", e);
        ServiceError::InternalServerError(format!("Error inserting search queries: {:?}", e))
    })?;

    let mut rag_queries_inserter = clickhouse_client.insert("rag_queries").map_err(|e| {
        log::error!("Error inserting rag queries: {:?}", e);
        ServiceError::InternalServerError(format!("Error inserting rag queries: {:?}", e))
    })?;

    let mut recommendations_inserter =
        clickhouse_client.insert("recommendations").map_err(|e| {
            log::error!("Error inserting recommendations: {:?}", e);
            ServiceError::InternalServerError(format!("Error inserting recommendations: {:?}", e))
        })?;

    let mut worker_events_inserter = clickhouse_client.insert("dataset_events").map_err(|e| {
        log::error!("Error inserting recommendations: {:?}", e);
        ServiceError::InternalServerError(format!("Error inserting recommendations: {:?}", e))
    })?;

    let mut analytics_events_inserter = clickhouse_client.insert("events").map_err(|e| {
        log::error!("Error inserting analytics: {:?}", e);
        ServiceError::InternalServerError(format!("Error inserting analytics: {:?}", e))
    })?;

    for event in &events {
        match event {
            ClickHouseEvent::SearchQueryEvent(event) => {
                search_queries_inserter.write(event).await.map_err(|e| {
                    log::error!("Error writing search query event: {:?}", e);
                    ServiceError::InternalServerError(format!(
                        "Error writing search query event: {:?}",
                        e
                    ))
                })?;
            }
            ClickHouseEvent::RecommendationEvent(event) => {
                recommendations_inserter.write(event).await.map_err(|e| {
                    log::error!("Error writing recommendation event: {:?}", e);
                    ServiceError::InternalServerError(format!(
                        "Error writing recommendation event: {:?}",
                        e
                    ))
                })?;
            }
            ClickHouseEvent::RagQueryEvent(event) => {
                rag_queries_inserter.write(event).await.map_err(|e| {
                    log::error!("Error writing rag query event: {:?}", e);
                    ServiceError::InternalServerError(format!(
                        "Error writing rag query event: {:?}",
                        e
                    ))
                })?;
            }
            ClickHouseEvent::WorkerEvent(event) => {
                worker_events_inserter.write(event).await.map_err(|e| {
                    log::error!("Error writing worker event: {:?}", e);
                    ServiceError::InternalServerError(format!(
                        "Error writing worker event: {:?}",
                        e
                    ))
                })?;
            }
            ClickHouseEvent::AnalyticsEvent(event) => {
                analytics_events_inserter.write(event).await.map_err(|e| {
                    log::error!("Error writing analytics event: {:?}", e);
                    ServiceError::InternalServerError(format!(
                        "Error writing analytics event: {:?}",
                        e
                    ))
                })?;
            }
            ClickHouseEvent::RagQueryRatingEvent(event) => {
                let mut rag_event = clickhouse_client
                    .query("SELECT ?fields FROM rag_queries WHERE id = ?")
                    .bind(event.query_id)
                    .fetch_optional::<RagQueryEventClickhouse>()
                    .await
                    .map_err(|e| {
                        log::error!("Error fetching query: {:?}", e);
                        ServiceError::InternalServerError("Error fetching query".to_string())
                    })?;

                if rag_event.is_none() {
                    rag_event = events
                        .iter()
                        .filter_map(|e| match e {
                            ClickHouseEvent::RagQueryEvent(event) => Some(event.clone()),
                            _ => None,
                        })
                        .find(|e| e.id == event.query_id);
                }

                let mut rag_event = if let Some(event) = rag_event {
                    event
                } else {
                    continue;
                };

                let rating = SearchQueryRating {
                    rating: event.rating,
                    note: event.note.clone(),
                    metadata: event.metadata.clone(),
                };

                rag_event.query_rating = serde_json::to_string(&rating).unwrap();
                rag_queries_inserter.write(&rag_event).await.map_err(|e| {
                    log::error!("Error writing rag query rating event: {:?}", e);
                    ServiceError::InternalServerError(format!(
                        "Error writing rag query rating event: {:?}",
                        e
                    ))
                })?;
            }
            ClickHouseEvent::SearchQueryRatingEvent(event) => {
                let mut search_event = clickhouse_client
                    .query("SELECT ?fields FROM search_queries WHERE id = ?")
                    .bind(event.query_id)
                    .fetch_optional::<SearchQueryEventClickhouse>()
                    .await
                    .map_err(|e| {
                        log::error!("Error fetching query: {:?}", e);
                        ServiceError::InternalServerError("Error fetching query".to_string())
                    })?;

                let rating = SearchQueryRating {
                    rating: event.rating,
                    note: event.note.clone(),
                    metadata: event.metadata.clone(),
                };

                if search_event.is_none() {
                    search_event = events
                        .iter()
                        .filter_map(|e| match e {
                            ClickHouseEvent::SearchQueryEvent(event) => Some(event.clone()),
                            _ => None,
                        })
                        .find(|e| e.id == event.query_id);
                }

                let mut search_event = if let Some(event) = search_event {
                    event
                } else {
                    continue;
                };

                search_event.query_rating = serde_json::to_string(&rating).unwrap();
                search_queries_inserter
                    .write(&search_event)
                    .await
                    .map_err(|e| {
                        log::error!("Error writing search query rating event: {:?}", e);
                        ServiceError::InternalServerError(format!(
                            "Error writing search query rating event: {:?}",
                            e
                        ))
                    })?;
            }
        }
    }

    search_queries_inserter.end().await.map_err(|e| {
        log::error!("Error ending search queries inserter: {:?}", e);
        ServiceError::InternalServerError(format!("Error ending search queries inserter: {:?}", e))
    })?;

    rag_queries_inserter.end().await.map_err(|e| {
        log::error!("Error ending rag queries inserter: {:?}", e);
        ServiceError::InternalServerError(format!("Error ending rag queries inserter: {:?}", e))
    })?;
    recommendations_inserter.end().await.map_err(|e| {
        log::error!("Error ending recommendations inserter: {:?}", e);
        ServiceError::InternalServerError(format!("Error ending recommendations inserter: {:?}", e))
    })?;
    worker_events_inserter.end().await.map_err(|e| {
        log::error!("Error ending worker events inserter: {:?}", e);
        ServiceError::InternalServerError(format!("Error ending worker events inserter: {:?}", e))
    })?;
    analytics_events_inserter.end().await.map_err(|e| {
        log::error!("Error ending analytics events inserter: {:?}", e);
        ServiceError::InternalServerError(format!(
            "Error ending analytics events inserter: {:?}",
            e
        ))
    })?;

    Ok(())
}

#[derive(Default, Clone)]
pub struct EventQueue {
    sender: Option<mpsc::Sender<ClickHouseEvent>>,
    clickhouse_client: clickhouse::Client,
}

impl EventQueue {
    pub fn new(clickhouse_client: clickhouse::Client) -> Self {
        Self {
            sender: None,
            clickhouse_client,
        }
    }

    pub fn start_service(&mut self) {
        let clickhouse_client = self.clickhouse_client.clone();
        let (sender, mut reciever) = mpsc::channel(1000);
        self.sender = Some(sender);

        tokio::spawn(async move {
            let mut events = Vec::new();
            let mut timer = Instant::now();
            loop {
                tokio::select! {
                    Some(event) = reciever.recv() => {
                        events.push(event);
                        if Instant::now().0.duration_since(timer.0).as_secs() > 10 || events.len() > 500 {
                            if let Err(e) = send_to_clickhouse(events.clone(), &clickhouse_client).await {
                                log::error!("Error sending events to clickhouse: {:?}", e);
                            }
                            events.clear();
                            timer = Instant::now();
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {
                        if !events.is_empty() {
                            if let Err(e) = send_to_clickhouse(events.clone(), &clickhouse_client).await {
                                log::error!("Error sending events to clickhouse: {:?}", e);
                            }
                            events.clear();
                            timer = Instant::now();
                        }
                    }
                }
            }
        });
    }

    pub async fn send(&self, event: ClickHouseEvent) {
        if let Some(sender) = &self.sender {
            let _ = sender.send(event).await.map_err(|e| {
                log::error!("Error sending event to clickhouse: {:?}", e);
            });
        }
    }
}
