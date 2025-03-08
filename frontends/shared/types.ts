export interface ChunkMetadataStringTagSet {
  id: string;
  chunk_html?: string;
  link: string | null;
  qdrant_point_id: string;
  created_at: string;
  updated_at: string;
  tag_set: string | null;
  tracking_id: string | null;
  time_stamp: string | null;
  metadata: Record<string, never> | null;
  dataset_id: string;
  weight: number;
  location: {
    lat: number;
    lon: number;
  } | null;
  num_value: number | null;
  image_urls: string[] | null;
}

export interface SlimUser {
  id: string;
  name?: string;
  email: string;
  user_orgs: UserOrganization[];
  orgs: Organization[];
}

export interface UserOrganization {
  id: string;
  user_id: string;
  organization_id: string;
  role: number;
  created_at: string;
  updated_at: string;
}

export interface Organization {
  id: string;
  name: string;
  configuration: [key: string];
  created_at: string;
  updated_at: string;
  registerable?: boolean;
}

export interface OrganizationAndSubAndPlan {
  organization: Organization;
  plan?: StripePlan | null;
  subscription?: StripeSubscription | null;
}

export interface StripeSubscription {
  id: string;
  stripe_id: string;
  plan_id: string;
  organization_id: string;
  created_at: string;
  updated_at: string;
  current_period_end?: string | null;
}

export interface StripePlan {
  id: string;
  stripe_id: string;
  chunk_count: number;
  file_storage: number;
  user_count: number;
  dataset_count: number;
  message_count: number;
  amount: number;
  created_at: string;
  updated_at: string;
  name: string;
  visible: boolean;
}

export interface OrganizationUsageCount {
  id: string;
  org_id: string;
  dataset_count: number;
  user_count: number;
  file_storage: number;
  message_count: number;
  chunk_count: number;
}

export interface Dataset {
  id: string;
  tracking_id: string;
  name: string;
  created_at: string;
  updated_at: string;
  organization_id: string;
  server_configuration: ServerEnvsConfiguration;
}

export interface DatasetUsageCount {
  id: string;
  dataset_id: string;
  chunk_count: number;
}

export interface DatasetAndUsage {
  dataset: Dataset;
  dataset_usage: DatasetUsageCount;
}

export interface ServerEnvsConfiguration {
  LLM_BASE_URL?: string;
  LLM_DEFAULT_MODEL?: string;
  LLM_API_KEY?: string;
  EMBEDDING_BASE_URL?: string;
  EMBEDDING_MODEL_NAME?: string;
  MESSAGE_TO_QUERY_PROMPT?: string;
  RAG_PROMPT?: string;
  N_RETRIEVALS_TO_INCLUDE?: number;
  EMBEDDING_SIZE?: number;
  DISTANCE_METRIC?: string;
  FULLTEXT_ENABLED?: boolean;
  SEMANTIC_ENABLED?: boolean;
  QDRANT_COLLECTION_NAME?: string | null;
  EMBEDDING_QUERY_PREFIX?: string;
  TEMPERATURE?: number | null;
  PRESENCE_PENALTY?: number | null;
  FREQUENCY_PENALTY?: number | null;
  STOP_TOKENS?: string | null;
  USE_MESSAGE_TO_QUERY_PROMPT?: boolean;
  MAX_TOKENS?: number | null;
  INDEXED_ONLY?: boolean;
  LOCKED?: boolean;
  SYSTEM_PROMPT?: string | null;
  MAX_LIMIT?: number;
  BM25_ENABLED?: boolean;
  BM25_B?: number;
  BM25_K?: number;
  BM25_AVG_LEN?: number;
  TASK_DEFINITION?: string;
}

export interface DefaultError {
  message: string;
}

export interface ComboboxItem {
  name: string;
  custom?: boolean;
  selected?: boolean;
}
export interface ComboboxSection {
  name: string;
  comboboxItems: ComboboxItem[];
}

export interface SetUserApiKeyResponse {
  api_key: string;
}

export const isComboboxValues = (
  values: unknown
): values is ComboboxSection[] => {
  if (!Array.isArray(values)) {
    return false;
  }

  for (const value of values) {
    if (typeof value !== "object" || value === null) {
      return false;
    }

    if (!Array.isArray((value as ComboboxSection).comboboxItems)) {
      return false;
    }

    for (const item of (value as ComboboxSection).comboboxItems) {
      if (typeof item !== "object" || item === null || !("name" in item)) {
        return false;
      }
    }
  }

  return true;
};

export enum UserRole {
  User = "User",
  Admin = "Admin",
  Owner = "Owner",
}

export function fromI32ToUserRole(role: number): UserRole {
  switch (role) {
    case 2:
      return UserRole.Owner;
    case 1:
      return UserRole.Admin;
    default:
      return UserRole.User;
  }
}
export function fromUserRoleToI32(role: UserRole): number {
  switch (role) {
    case UserRole.Owner:
      return 2;
    case UserRole.Admin:
      return 1;
    default:
      return 0;
  }
}

export function stringToUserRole(input: string): UserRole | undefined {
  switch (input) {
    case "User":
      return UserRole.User;
    case "Admin":
      return UserRole.Admin;
    case "Owner":
      return UserRole.Owner;
    default:
      return UserRole.User; // or throw an error, depending on your use case
  }
}

export enum ApiKeyRole {
  Read = "Read",
  CurrentPerms = "CurrentPerms",
}

export function fromI32ToApiKeyRole(role: number): ApiKeyRole {
  switch (role) {
    case 1:
      return ApiKeyRole.CurrentPerms;
    default:
      return ApiKeyRole.Read;
  }
}

export function fromApiKeyRoleToI32(role: ApiKeyRole): number {
  switch (role) {
    case ApiKeyRole.CurrentPerms:
      return 1;
    default:
      return 0;
  }
}

export function stringToApiKeyRole(input: string): ApiKeyRole | undefined {
  switch (input) {
    case "Read":
      return ApiKeyRole.Read;
    case "Read And Write":
      return ApiKeyRole.CurrentPerms;
    default:
      return ApiKeyRole.Read; // or throw an error, depending on your use case
  }
}

export interface ApiKeyRespBody {
  id: string;
  user_id: string;
  name: string;
  role: number;
  dataset_ids: string[] | null;
  organization_ids: string[] | null;
  created_at: string;
  updated_at: string;
}

export const availableEmbeddingModels = [
  {
    id: "jina-base-en",
    name: "jina-base-en (hosted by Trieve)",
    url: "https://embedding.trieve.ai",
    dimension: 768,
  },
  {
    id: "bge-m3",
    name: "bge-m3 (hosted by Trieve)",
    url: "https://embedding.trieve.ai/bge-m3",
    dimension: 1024,
  },
  {
    id: "jina-embeddings-v2-base-code",
    name: "jina-embeddings-v2-base-code (hosted by Trieve)",
    url: "https://embedding.trieve.ai/jina-code",
    dimension: 768,
  },
  {
    id: "text-embedding-3-small",
    name: "text-embedding-3-small (hosted by OpenAI)",
    url: "https://api.openai.com/v1",
    dimension: 1536,
  },
  {
    id: "text-embedding-3-large",
    name: "text-embedding-3-large (hosted by OpenAI)",
    url: "https://api.openai.com/v1",
    dimension: 3072,
  },
];

export const availableRerankerModels = [
  {
    id: "bge-reranker-large",
    name: "bge-reranker-large (hosted by Trieve)",
    url: null,
  },
  {
    id: "aimon-rerank",
    name: "AIMon Reranker (with AIMon API key)",
    url: "https://pbe-api.aimon.ai/v2/detect",
  },
  {
    id: "rerank-v3.5",
    name: "Cohere rerank-v3.5 (with Cohere API key)",
    url: "https://api.cohere.com/v2",
  },
];

export const availableDistanceMetrics = [
  {
    id: "cosine",
    name: "Cosine Similarity",
  },
  {
    id: "euclidean",
    name: "Euclidean Distance",
  },
  {
    id: "dot",
    name: "Dot product",
  },
  {
    id: "manhattan",
    name: "Manhattan distance",
  },
];

export interface EventDTO {
  events: Event[];
  event_types: string[];
  page_count: number;
}

export const isEventDTO = (data: unknown): data is EventDTO => {
  if (typeof data !== "object" || data === null) {
    return false;
  }

  if (!Array.isArray((data as EventDTO).events)) {
    return false;
  }

  if (!Array.isArray((data as EventDTO).event_types)) {
    return false;
  }

  if (typeof (data as EventDTO).page_count !== "number") {
    return false;
  }

  return true;
};

export interface Event {
  id: string;
  created_at: string;
  dataset_id: string;
  event_type: string;
  event_data: string;
}

export const isEvent = (data: unknown): data is Event => {
  if (typeof data !== "object" || data === null) {
    return false;
  }

  if (typeof (data as Event).id !== "string") {
    return false;
  }

  if (typeof (data as Event).created_at !== "string") {
    return false;
  }

  if (typeof (data as Event).dataset_id !== "string") {
    return false;
  }

  if (typeof (data as Event).event_type !== "string") {
    return false;
  }

  if (typeof (data as Event).event_data !== "string") {
    return false;
  }

  return true;
};
export interface Invitation {
  id: string;
  email: string;
  organization_id: string;
  used: boolean;
  created_at: string;
  updated_at: string;
  role: number;
}

export const isInvitation = (data: unknown): data is Invitation => {
  if (typeof data !== "object" || data === null) {
    return false;
  }

  if (typeof (data as Invitation).id !== "string") {
    return false;
  }

  if (typeof (data as Invitation).email !== "string") {
    return false;
  }

  if (typeof (data as Invitation).organization_id !== "string") {
    return false;
  }

  if (typeof (data as Invitation).used !== "boolean") {
    return false;
  }

  if (typeof (data as Invitation).created_at !== "string") {
    return false;
  }

  if (typeof (data as Invitation).updated_at !== "string") {
    return false;
  }

  if (typeof (data as Invitation).role !== "number") {
    return false;
  }

  return true;
};

export interface SearchClusterTopics {
  id: string;
  dataset_id: string;
  topic: string;
  density: number;
  avg_score: number;
  created_at: string;
}

export interface DateRangeFilter {
  gt?: Date;
  lt?: Date;
  gte?: Date;
  lte?: Date;
}

export interface RangeFilter {
  gt?: number;
  lt?: number;
  gte?: number;
  lte?: number;
}

export interface AnalyticsFilter {
  date_range: DateRangeFilter;
  search_method?: "fulltext" | "hybrid" | "semantic";
  search_type?:
    | "search"
    | "autocomplete"
    | "rag_chunks"
    | "search_over_groups"
    | "search_within_groups";
  query_rating?: RangeFilter;
}

export interface RequiredAnalyticsFilter {
  date_range: DateRangeFilter;
  search_method: NonNullable<AnalyticsFilter["search_method"]>;
  search_type: NonNullable<AnalyticsFilter["search_type"]>;
}

// The search analytics params bar conforms to this
export interface AnalyticsParams {
  filter: AnalyticsFilter;
  granularity: "minute" | "second" | "hour" | "day" | "month";
}

export interface LatencyDatapoint {
  average_latency: number;
  time_stamp: string;
}

export interface UsageDatapoint {
  requests: number;
  time_stamp: string;
}

export interface SearchQueryEvent {
  id: string;
  search_type: string;
  query: string;
  request_params: Record<string, unknown>;
  latency: number;
  top_score: number;
  results: ScoreChunkDTO[] | object[] | GroupScoreChunkDTO[];
  dataset_id: string;
  created_at: string;
  query_rating?: {
    note?: string;
    rating: number;
  };
}

export interface GroupScoreChunkDTO {
  group_id: string;
  group_name?: string;
  group_description?: string;
  group_created_at: string;
  group_updated_at: string;
  group_tracking_id?: string;
  group_metadata?: Record<string, unknown>;
  group_tag_set?: string[] | null;
  group_dataset_id: string;
  metadata: ScoreChunkDTO[];
  file_id?: string;
}

export interface ScoreChunkDTO {
  highlights?: unknown;
  metadata: ChunkMetadataStringTagSet[];
  score?: number;
}

export function isGroupScoreChunkDTO(
  data: unknown
): data is GroupScoreChunkDTO {
  if (typeof data !== "object" || data === null) {
    return false;
  }

  if (!Array.isArray((data as GroupScoreChunkDTO).metadata)) {
    return false;
  }

  return true;
}

export function isScoreChunkDTO(data: unknown): data is ScoreChunkDTO {
  if (typeof data !== "object" || data === null) {
    return false;
  }

  if (!Array.isArray((data as ScoreChunkDTO).metadata)) {
    return false;
  }

  if (
    (data as ScoreChunkDTO).score !== undefined &&
    typeof (data as ScoreChunkDTO).score !== "number"
  ) {
    return false;
  }

  return true;
}

export interface RecommendationEvent {
  created_at: string;
  dataset_id: string;
  id: string;
  negative_ids: string[];
  negative_tracking_ids: string[];
  positive_ids: string[];
  positive_tracking_ids: string[];
  recommendation_type: string;
  request_params: Record<string, unknown>;
  results: ChunkMetadataStringTagSet[];
  top_score: number;
  user_id: string;
}

export interface CTRSearchQuery {
  query: string;
  results: ChunkMetadataStringTagSet[];
  clicked_chunk: {
    chunk: ChunkMetadataStringTagSet;
    position: number;
  };
  request_id: string;
  created_at: string;
}

export interface HeadQuery {
  query: string;
  count: number;
}

export interface SearchTypeCount {
  search_type: AnalyticsFilter["search_type"];
  search_method: AnalyticsFilter["search_method"];
  search_count: number;
}

export interface RequiredRAGAnalyticsFilter {
  rag_type?: "chosen_chunks" | "all_chunks"; // Optional because that means "BOTH"
  date_range: DateRangeFilter;
  granularity: "minute" | "second" | "hour" | "day" | "month";
}

export interface RAGAnalyticsFilter {
  rag_type?: "chosen_chunks" | "all_chunks";
  date_range?: DateRangeFilter;
  query_rating?: RangeFilter;
}

export interface EventAnalyticsFilter {
  date_range?: DateRangeFilter;
  event_type?: "view" | "click" | "add_to_cart" | "purchase" | "filter_clicked";
  is_conversion?: boolean;
  metadata_filter?: string;
}

export interface RecommendationsAnalyticsFilter {
  date_range?: DateRangeFilter;
  recommendation_type?: "chunk" | "group";
}

export type RAGSortBy =
  | "created_at"
  | "latency"
  | "hallucination_score"
  | "top_score";
export type SearchSortBy = "created_at" | "latency" | "top_score";

export type SortOrder = "desc" | "asc";

export interface RagQueryEvent {
  id: string;
  rag_type: string;
  user_message: string;
  search_id: string;
  results: ChunkMetadataStringTagSet[];
  llm_response: string;
  dataset_id: string;
  created_at: string;
  query_rating?: {
    note?: string;
    rating: number;
  };
  top_score: number;
  hallucination_score?: number;
  detected_hallucinations?: string[];
}

export interface EventData {
  id: string;
  event_type: string;
  event_name: string;
  request_id?: string;
  request_type?: string;
  items: string[];
  metadata?: object;
  user_id?: string;
  is_conversion?: boolean;
  dataset_id: string;
  created_at: string;
  updated_at: string;
}

export interface RAGUsageResponse {
  total_queries: number;
}

export interface QueryCountResponse {
  total_queries: SearchTypeCount[];
}

export interface HeadQueryResponse {
  queries: HeadQuery[];
}

export interface SearchMetricsResponse {
  total_queries: number;
  search_rps: number;
  avg_latency: number;
  p99: number;
  p95: number;
  p50: number;
  percent_thumbs_up: number;
  percent_thumbs_down: number;
}

export interface RagQueryRatingResponse {
  percent_thumbs_up: number;
  percent_thumbs_down: number;
}

export interface SearchQueryResponse {
  queries: SearchQueryEvent[];
}

export interface SearchClusterResponse {
  clusters: SearchClusterTopics[];
}

export interface UsageGraphResponse {
  usage_points: UsageDatapoint[];
}

export interface LatencyGraphResponse {
  latency_points: LatencyDatapoint[];
}

export interface RagQueryResponse {
  queries: RagQueryEvent[];
}

export interface EventResponse {
  events: EventData[];
}
