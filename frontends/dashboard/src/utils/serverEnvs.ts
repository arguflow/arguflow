import { DatasetConfig } from "../components/dataset-settings/LegacySettingsWrapper";

const bm25Active = import.meta.env.VITE_BM25_ACTIVE as unknown as string;

export const defaultServerEnvsConfiguration: DatasetConfig = {
  LLM_BASE_URL: "",
  LLM_DEFAULT_MODEL: "",
  LLM_API_KEY: "",
  EMBEDDING_BASE_URL: "https://embedding.trieve.ai",
  EMBEDDING_MODEL_NAME: "jina-base-en",
  RERANKER_MODEL_NAME: "bge-reranker-large",
  MESSAGE_TO_QUERY_PROMPT: "",
  RAG_PROMPT: "",
  EMBEDDING_SIZE: 768,
  N_RETRIEVALS_TO_INCLUDE: 8,
  FULLTEXT_ENABLED: true,
  SEMANTIC_ENABLED: true,
  EMBEDDING_QUERY_PREFIX: "Search for: ",
  USE_MESSAGE_TO_QUERY_PROMPT: false,
  FREQUENCY_PENALTY: null,
  TEMPERATURE: null,
  PRESENCE_PENALTY: null,
  STOP_TOKENS: null,
  MAX_TOKENS: null,
  INDEXED_ONLY: false,
  LOCKED: false,
  SYSTEM_PROMPT: null,
  MAX_LIMIT: 10000,
  BM25_ENABLED: bm25Active == "true",
  BM25_B: 0.75,
  BM25_K: 1.2,
  BM25_AVG_LEN: 256,
  PUBLIC_DATASET: {
    enabled: false,
    api_key: "",
  },
  TASK_DEFINITION: "",
};
