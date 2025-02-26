import {
  AnalyticsParams,
  HeadQuery,
  LatencyDatapoint,
  RagQueryEvent,
  RAGUsageResponse,
  SearchQueryEvent,
  SearchTypeCount,
  LatencyGraphResponse,
  HeadQueryResponse,
  RagQueryResponse,
  SearchQueryResponse,
  QueryCountResponse,
  AnalyticsFilter,
  RAGAnalyticsFilter,
  RAGSortBy,
  SortOrder,
  UsageDatapoint,
  UsageGraphResponse,
  RecommendationEvent,
  EventData,
  DateRangeFilter,
  SearchMetricsResponse,
  RagQueryRatingResponse,
} from "shared/types";
import { transformAnalyticsFilter } from "../utils/formatDate";

const apiHost = import.meta.env.VITE_API_HOST as string;

export const getLatency = async (
  filters: AnalyticsFilter,
  granularity: AnalyticsParams["granularity"],
  datasetId: string,
): Promise<LatencyDatapoint[]> => {
  const response = await fetch(`${apiHost}/analytics/search`, {
    credentials: "include",
    method: "POST",
    body: JSON.stringify({
      filter: transformAnalyticsFilter(filters),
      granularity: granularity,
      type: "latency_graph",
    }),
    headers: {
      "TR-Dataset": datasetId,
      "Content-Type": "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch trends bubbles: ${response.statusText}`);
  }

  const data: LatencyGraphResponse =
    (await response.json()) as unknown as LatencyGraphResponse;

  return data.latency_points;
};

export const getRpsUsageGraph = async (
  filters: AnalyticsFilter,
  granularity: AnalyticsParams["granularity"],
  datasetId: string,
): Promise<UsageDatapoint[]> => {
  const response = await fetch(`${apiHost}/analytics/search`, {
    credentials: "include",
    method: "POST",
    body: JSON.stringify({
      filter: transformAnalyticsFilter(filters),
      granularity,
      type: "search_usage_graph",
    }),
    headers: {
      "TR-Dataset": datasetId,
      "Content-Type": "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch trends bubbles: ${response.statusText}`);
  }

  const data = (await response.json()) as unknown as UsageGraphResponse;
  return data.usage_points;
};

export const getHeadQueries = async (
  filters: AnalyticsFilter,
  datasetId: string,
  page: number,
): Promise<HeadQuery[]> => {
  const response = await fetch(`${apiHost}/analytics/search`, {
    credentials: "include",
    method: "POST",
    body: JSON.stringify({
      filter: transformAnalyticsFilter(filters),
      page,
      type: "head_queries",
    }),
    headers: {
      "TR-Dataset": datasetId,
      "Content-Type": "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch head queries: ${response.statusText}`);
  }

  const data = (await response.json()) as unknown as HeadQueryResponse;
  return data.queries;
};

export const getRAGQueries = async ({
  datasetId,
  page,
  filter,
  sort_by,
  sort_order,
}: {
  datasetId: string;
  page: number;
  filter?: RAGAnalyticsFilter;
  sort_by?: RAGSortBy;
  sort_order?: SortOrder;
}): Promise<RagQueryEvent[]> => {
  const response = await fetch(`${apiHost}/analytics/rag`, {
    credentials: "include",
    method: "POST",
    body: JSON.stringify({
      page,
      sort_by,
      sort_order,
      filter: filter ? transformAnalyticsFilter(filter) : undefined,
      type: "rag_queries",
    }),
    headers: {
      "TR-Dataset": datasetId,
      "Content-Type": "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch head queries: ${response.statusText}`);
  }

  const data = (await response.json()) as unknown as RagQueryResponse;
  return data.queries;
};

export const getRAGUsage = async (
  datasetId: string,
  filter?: RAGAnalyticsFilter,
): Promise<RAGUsageResponse> => {
  const response = await fetch(`${apiHost}/analytics/rag`, {
    method: "POST",
    credentials: "include",
    headers: {
      "TR-Dataset": datasetId,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      type: "rag_usage",
      filter: filter ? transformAnalyticsFilter(filter) : undefined,
    }),
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch head queries: ${response.statusText}`);
  }

  const data = (await response.json()) as unknown as RAGUsageResponse;
  return data;
};

export const getRAGMetrics = async (
  dateRange: DateRangeFilter,
  datasetId: string,
): Promise<RagQueryRatingResponse> => {
  const response = await fetch(`${apiHost}/analytics/rag`, {
    method: "POST",
    credentials: "include",
    headers: {
      "TR-Dataset": datasetId,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      type: "rag_query_ratings",
      filter: transformAnalyticsFilter({
        date_range: dateRange,
      }),
    }),
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch head queries: ${response.statusText}`);
  }

  const data = (await response.json()) as unknown as RagQueryRatingResponse;
  return data;
};

export const getRagUsageGraph = async (
  filters: RAGAnalyticsFilter,
  granularity: AnalyticsParams["granularity"],
  datasetId: string,
): Promise<UsageDatapoint[]> => {
  const response = await fetch(`${apiHost}/analytics/rag`, {
    credentials: "include",
    method: "POST",
    body: JSON.stringify({
      filter: transformAnalyticsFilter(filters),
      granularity,
      type: "rag_usage_graph",
    }),
    headers: {
      "TR-Dataset": datasetId,
      "Content-Type": "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch trends bubbles: ${response.statusText}`);
  }

  const data = (await response.json()) as unknown as UsageGraphResponse;
  return data.usage_points;
};

export const getLowConfidenceQueries = async (
  filters: AnalyticsFilter,
  datasetId: string,
  page: number,
  threshold?: number,
): Promise<SearchQueryEvent[]> => {
  const response = await fetch(`${apiHost}/analytics/search`, {
    credentials: "include",
    method: "POST",
    body: JSON.stringify({
      filter: transformAnalyticsFilter(filters),
      page,
      threshold,
      type: "low_confidence_queries",
    }),
    headers: {
      "TR-Dataset": datasetId,
      "Content-Type": "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(
      `Failed to fetch low confidence queries: ${response.statusText}`,
    );
  }

  const data = (await response.json()) as unknown as SearchQueryResponse;
  return data.queries;
};

export const getNoResultQueries = async (
  filters: AnalyticsFilter,
  datasetId: string,
  page: number,
): Promise<SearchQueryEvent[]> => {
  const response = await fetch(`${apiHost}/analytics/search`, {
    credentials: "include",
    method: "POST",
    body: JSON.stringify({
      filter: transformAnalyticsFilter(filters),
      page,
      type: "no_result_queries",
    }),
    headers: {
      "TR-Dataset": datasetId,
      "Content-Type": "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(
      `Failed to fetch no result queries: ${response.statusText}`,
    );
  }

  const data = (await response.json()) as unknown as SearchQueryResponse;
  return data.queries;
};

export const getQueryCounts = async (
  dateRange: DateRangeFilter,
  datasetId: string,
): Promise<SearchTypeCount[]> => {
  const response = await fetch(`${apiHost}/analytics/search`, {
    credentials: "include",
    method: "POST",
    body: JSON.stringify({
      filter: transformAnalyticsFilter({
        date_range: dateRange,
      }),
      type: "count_queries",
    }),
    headers: {
      "TR-Dataset": datasetId,
      "Content-Type": "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(
      `Failed to fetch no result queries: ${response.statusText}`,
    );
  }

  const data = (await response.json()) as unknown as QueryCountResponse;
  return data.total_queries;
};

export const getSearchMetrics = async (
  dateRange: DateRangeFilter,
  datasetId: string,
): Promise<SearchMetricsResponse> => {
  const response = await fetch(`${apiHost}/analytics/search`, {
    credentials: "include",
    method: "POST",
    body: JSON.stringify({
      filter: transformAnalyticsFilter({
        date_range: dateRange,
      }),
      type: "search_metrics",
    }),
    headers: {
      "TR-Dataset": datasetId,
      "Content-Type": "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(
      `Failed to fetch no result queries: ${response.statusText}`,
    );
  }

  const data = (await response.json()) as SearchMetricsResponse;
  return data;
};

export const getSearchQuery = async (
  datasetId: string,
  searchId: string,
): Promise<SearchQueryEvent> => {
  const response = await fetch(`${apiHost}/analytics/search`, {
    credentials: "include",
    method: "POST",
    headers: {
      "TR-Dataset": datasetId,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      request_id: searchId,
      type: "query_details",
    }),
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch search event: ${response.statusText}`);
  }

  const data = (await response.json()) as unknown as SearchQueryEvent;
  return data;
};

export const getRagQuery = async (
  datasetId: string,
  searchId: string,
): Promise<RagQueryEvent> => {
  const response = await fetch(`${apiHost}/analytics/rag`, {
    credentials: "include",
    method: "POST",
    headers: {
      "TR-Dataset": datasetId,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      request_id: searchId,
      type: "rag_query_details",
    }),
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch search event: ${response.statusText}`);
  }

  const data = (await response.json()) as unknown as RagQueryEvent;
  return data;
};

export const getRecommendationQuery = async (
  datasetId: string,
  searchId: string,
): Promise<RecommendationEvent> => {
  const response = await fetch(`${apiHost}/analytics/recommendations`, {
    credentials: "include",
    method: "POST",
    headers: {
      "TR-Dataset": datasetId,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      request_id: searchId,
      type: "query_details",
    }),
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch search event: ${response.statusText}`);
  }

  const data = (await response.json()) as unknown as RecommendationEvent;
  return data;
};

export const getEventQuery = async (
  datasetId: string,
  eventId: string,
): Promise<EventData> => {
  const response = await fetch(`${apiHost}/analytics/events/${eventId}`, {
    credentials: "include",
    method: "GET",
    headers: {
      "TR-Dataset": datasetId,
      "Content-Type": "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch search event: ${response.statusText}`);
  }

  const data = (await response.json()) as unknown as EventData;
  return data;
};
