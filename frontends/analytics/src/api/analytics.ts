/* eslint-disable @typescript-eslint/no-unsafe-return */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import {
  AnalyticsParams,
  HeadQuery,
  LatencyDatapoint,
  RagQueryEvent,
  RAGUsageResponse,
  RpsDatapoint,
  SearchQueryEvent,
  SearchTypeCount,
  LatencyGraphResponse,
  HeadQueryResponse,
  RagQueryResponse,
  SearchQueryResponse,
  QueryCountResponse,
  RPSGraphResponse,
} from "shared/types";
import { apiHost } from "../utils/apiHost";
import { transformAnalyticsParams } from "../utils/formatDate";

export const getLatency = async (
  filters: AnalyticsParams,
  datasetId: string,
): Promise<LatencyDatapoint[]> => {
  const response = await fetch(`${apiHost}/analytics/search`, {
    credentials: "include",
    method: "POST",
    body: JSON.stringify({
      latency_graph: transformAnalyticsParams(filters),
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

export const getRps = async (
  filters: AnalyticsParams,
  datasetId: string,
): Promise<RpsDatapoint[]> => {
  const response = await fetch(`${apiHost}/analytics/search`, {
    credentials: "include",
    method: "POST",
    body: JSON.stringify({
      rps_graph: transformAnalyticsParams(filters),
    }),
    headers: {
      "TR-Dataset": datasetId,
      "Content-Type": "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch trends bubbles: ${response.statusText}`);
  }

  const data = (await response.json()) as unknown as RPSGraphResponse;
  return data.rps_points;
};

export const getHeadQueries = async (
  filters: AnalyticsParams,
  datasetId: string,
  page: number,
): Promise<HeadQuery[]> => {
  const response = await fetch(`${apiHost}/analytics/search`, {
    credentials: "include",
    method: "POST",
    body: JSON.stringify({
      head_queries: {
        ...transformAnalyticsParams(filters),
        page,
      },
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

export const getRAGQueries = async (
  datasetId: string,
  page: number,
): Promise<RagQueryEvent[]> => {
  const payload = {
    page,
  };

  const response = await fetch(`${apiHost}/analytics/rag`, {
    credentials: "include",
    method: "POST",
    body: JSON.stringify({
      rag_queries: payload,
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
): Promise<RAGUsageResponse> => {
  const response = await fetch(`${apiHost}/analytics/rag`, {
    credentials: "include",
    method: "POST",
    headers: {
      "TR-Dataset": datasetId,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      rag_usage: {},
    }),
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch head queries: ${response.statusText}`);
  }

  const data = (await response.json()) as unknown as RAGUsageResponse;
  return data;
};

export const getLowConfidenceQueries = async (
  filters: AnalyticsParams,
  datasetId: string,
  page: number,
  threshold?: number,
): Promise<SearchQueryEvent[]> => {
  const response = await fetch(`${apiHost}/analytics/search`, {
    credentials: "include",
    method: "POST",
    body: JSON.stringify({
      low_confidence_queries: {
        ...transformAnalyticsParams(filters),
        page,
        threshold,
      },
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
  filters: AnalyticsParams,
  datasetId: string,
  page: number,
): Promise<SearchQueryEvent[]> => {
  const response = await fetch(`${apiHost}/analytics/search`, {
    credentials: "include",
    method: "POST",
    body: JSON.stringify({
      no_result_queries: {
        ...transformAnalyticsParams(filters),
        page,
      },
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
  filters: AnalyticsParams,
  datasetId: string,
): Promise<SearchTypeCount[]> => {
  const response = await fetch(`${apiHost}/analytics/search`, {
    credentials: "include",
    method: "POST",
    body: JSON.stringify({
      count_queries: transformAnalyticsParams(filters),
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
      query_details: {
        search_id: searchId,
      },
    }),
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch search event: ${response.statusText}`);
  }

  const data = (await response.json()) as unknown as SearchQueryEvent;
  return data;
};
