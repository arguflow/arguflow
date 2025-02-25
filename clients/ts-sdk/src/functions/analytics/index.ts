/**
 * This includes all the functions you can use to communicate with our analytics API
 *
 * @module Analytics Methods
 */

import {
  ClusterAnalytics,
  CTRAnalytics,
  CTRDataRequestBody,
  EventTypes,
  GetEventsRequestBody,
  GetTopDatasetsRequestBody,
  RAGAnalytics,
  RateQueryRequest,
  RecommendationAnalytics,
  SearchAnalytics,
} from "../../fetch-client";
import { TrieveSDK } from "../../sdk";

/**
 * Function that allows you to view the CTR analytics for a dataset.
 * 
 * Example:
 * ```js
 *const data = await trieve.getCTRAnalytics({
  filter: {
    date_range: {
      gt: "2021-01-01 00:00:00.000",
      gte: "2021-01-01 00:00:00.000",
      lt: "2021-01-01 00:00:00.000",
      lte: "2021-01-01 00:00:00.000"
    },
    search_method: "fulltext",
    search_type: "search"
  },
  type: "search_ctr_metrics"
});
 * ```
 */
export async function getCTRAnalytics(
  /** @hidden */
  this: TrieveSDK,
  data: CTRAnalytics,
  signal?: AbortSignal,
) {
  if (!this.datasetId) {
    throw new Error("datasetId is required");
  }

  return await this.trieve.fetch(
    "/api/analytics/events/ctr",
    "post",
    {
      data,
      datasetId: this.datasetId,
    },
    signal,
  );
}

/**
 * Function that allows you too send CTR data to the system.
 * 
 * Example:
 * ```js
 *const data = await trieve.sendCTRAnalytics({
  clicked_chunk_id: "3c90c3cc-0d44-4b50-8888-8dd25736052a",
  ctr_type: "search",
  position: 123,
  request_id: "3c90c3cc-0d44-4b50-8888-8dd25736052a"
});
 * ```
 */
export async function sendCTRAnalytics(
  /** @hidden */
  this: TrieveSDK,
  data: CTRDataRequestBody,
  signal?: AbortSignal,
) {
  if (!this.datasetId) {
    throw new Error("datasetId is required");
  }

  return await this.trieve.fetch(
    "/api/analytics/ctr",
    "put",
    {
      data,
      datasetId: this.datasetId,
    },
    signal,
  );
}

/**
 * Function that allows you to send analytics events to the system.
 *
 * Example:
 * ```js
 * const data = await trieve.sendAnalyticsEvent({
 *  event_type: "view",
 * metadata: {
 *    "test": "test"
 * },
 * user_id: "user1"
 * });
 * ```
 */
export async function sendAnalyticsEvent(
  /** @hidden */
  this: TrieveSDK,
  data: EventTypes,
  signal?: AbortSignal,
) {
  if (!this.datasetId) {
    throw new Error("datasetId is required");
  }

  return await this.trieve.fetch(
    "/api/analytics/events",
    "put",
    {
      data,
      datasetId: this.datasetId,
    },
    signal,
  );
}

/**
 * Function that allows you to view the RAG analytics for a dataset.
 * 
 * Example:
 * ```js
 *const data = await trieve.getRagAnalytics({
  filter: {
    date_range: {
      gt: "2021-01-01 00:00:00.000",
      gte: "2021-01-01 00:00:00.000",
      lt: "2021-01-01 00:00:00.000",
      lte: "2021-01-01 00:00:00.000",
    },
    rag_type: "chosen_chunks",
  },
  page: 1,
  sort_by: "created_at",
  sort_order: "desc",
  type: "rag_queries",
});
 * ```
 */
export async function getRagAnalytics(
  /** @hidden */
  this: TrieveSDK,
  data: RAGAnalytics,
  signal?: AbortSignal,
) {
  if (!this.datasetId) {
    throw new Error("datasetId is required");
  }

  return this.trieve.fetch(
    "/api/analytics/rag",
    "post",
    {
      data,
      datasetId: this.datasetId,
    },
    signal,
  );
}

/**
 * Function that allows you to view the recommendation analytics for a dataset.
 * 
 * Example:
 * ```js
 *const data = await trieve.getRecommendationAnalytics({
  filter: {
    date_range: {
      gt: "2021-01-01 00:00:00.000",
      gte: "2021-01-01 00:00:00.000",
      lt: "2021-01-01 00:00:00.000",
      lte: "2021-01-01 00:00:00.000",
    },
    recommendation_type: "Chunk",
  },
  page: 1,
  threshold: 123,
  type: "low_confidence_recommendations",
});
 * ```
 */
export async function getRecommendationAnalytics(
  /** @hidden */
  this: TrieveSDK,
  data: RecommendationAnalytics,
  signal?: AbortSignal,
) {
  if (!this.datasetId) {
    throw new Error("datasetId is required");
  }

  return this.trieve.fetch(
    "/api/analytics/recommendations",
    "post",
    {
      data,
      datasetId: this.datasetId,
    },
    signal,
  );
}

/**
 * Function that allows you to view the search analytics for a dataset.
 * 
 * Example:
 * ```js
 *const data = await trieve.getSearchAnalytics({
  filter: {
    date_range: {
      gt: "2021-01-01 00:00:00.000",
      gte: "2021-01-01 00:00:00.000",
      lt: "2021-01-01 00:00:00.000",
      lte: "2021-01-01 00:00:00.000",
    },
    search_method: "fulltext",
    search_type: "search",
  },
  granularity: "minute",
  type: "latency_graph",
});
 * ```
 */
export async function getSearchAnalytics(
  /** @hidden */
  this: TrieveSDK,
  data: SearchAnalytics,
  signal?: AbortSignal,
) {
  if (!this.datasetId) {
    throw new Error("datasetId is required");
  }

  return this.trieve.fetch(
    "/api/analytics/search",
    "post",
    {
      data,
      datasetId: this.datasetId,
    },
    signal,
  );
}

/**
 * Function that allows you to view the cluster analytics for a dataset.
 * 
 * Example:
 * ```js
 *const data = await trieve.getClusterAnalytics({
  filter: {
    date_range: {
      gt: "2021-01-01 00:00:00.000",
      gte: "2021-01-01 00:00:00.000",
      lt: "2021-01-01 00:00:00.000",
      lte: "2021-01-01 00:00:00.000",
    },
  },
  type: "cluster_topics",
});
 * ```
 */
export async function getClusterAnalytics(
  /** @hidden */
  this: TrieveSDK,
  data: ClusterAnalytics,
  signal?: AbortSignal,
) {
  if (!this.datasetId) {
    throw new Error("datasetId is required");
  }

  return this.trieve.fetch(
    "/api/analytics/search/cluster",
    "post",
    {
      data,
      datasetId: this.datasetId,
    },
    signal,
  );
}

/**
 * Function that allows you  to rate a RAG query.
 * 
 * Example:
 * ```js
 *const data = await trieve.rateRagQuery({
  query_id: 123,
  rating: 1,
});
 * ```
 */
export async function rateRagQuery(
  /** @hidden */
  this: TrieveSDK,
  data: RateQueryRequest,
  signal?: AbortSignal,
) {
  if (!this.datasetId) {
    throw new Error("datasetId is required");
  }

  return this.trieve.fetch(
    "/api/analytics/rag",
    "put",
    {
      data,
      datasetId: this.datasetId,
    },
    signal,
  );
}

/**
 * Function that allows you  to rate a search query.
 * 
 * Example:
 * ```js
 *const data = await trieve.rateSearchQuery({
  query_id: 123,
  rating: 1,
});
 * ```
 */
export async function rateSearchQuery(
  /** @hidden */
  this: TrieveSDK,
  data: RateQueryRequest,
  signal?: AbortSignal,
) {
  if (!this.datasetId) {
    throw new Error("datasetId is required");
  }

  return this.trieve.fetch(
    "/api/analytics/search",
    "put",
    {
      data,
      datasetId: this.datasetId,
    },
    signal,
  );
}

/**
 * Function that allows you to fetch the top datasets for an organization
 * 
 * Example:
 * ```js
 *const data = await trieve.getTopDatasets({
  organizationId: 123,
  type: "search"
});
 * ```
 */
export async function getTopDatasets(
  /** @hidden */
  this: TrieveSDK,
  data: GetTopDatasetsRequestBody & { organizationId: string },
  signal?: AbortSignal,
) {
  return this.trieve.fetch(
    "/api/analytics/top",
    "post",
    {
      data,
      organizationId: data.organizationId,
    },
    signal,
  );
}

/**
 * Function that allows you to view the CTR analytics for a dataset.
 * 
 * Example:
 * ```js
 *const data = await trieve.getAllAnalyticsEvents({
  filter: {
    "date_range": {
      "gt": "2021-08-10T00:00:00Z",
      "lt": "2021-08-11T00:00:00Z"
    },
    "event_type": "view",
    "is_conversion": true,
    "metadata_filter": "path = \"value\"",
    "user_id": "user1"
  },
});
 * ```
 */
export async function getAllAnalyticsEvents(
  /** @hidden */
  this: TrieveSDK,
  data: GetEventsRequestBody,
  signal?: AbortSignal,
) {
  return await this.trieve.fetch(
    "/api/analytics/events/all",
    "post",
    {
      data,
    },
    signal,
  );
}
