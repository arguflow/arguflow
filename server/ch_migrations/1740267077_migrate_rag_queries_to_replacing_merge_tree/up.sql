CREATE TABLE rag_queries_new
(
    `id` UUID,
    `rag_type` String,
    `user_message` String,
    `search_id` UUID,
    `results` Array(String),
    `llm_response` String,
    `dataset_id` UUID,
    `created_at` DateTime,
    `user_id` String,
    `query_rating` String DEFAULT 0,
    `json_results` Array(String),
    `hallucination_score` Float64 DEFAULT 0.,
    `detected_hallucinations` Array(String) DEFAULT [],
    `top_score` Int32 DEFAULT 0
)
ENGINE = ReplacingMergeTree()
PARTITION BY (toYYYYMM(created_at), dataset_id)
ORDER BY (id, created_at)
SETTINGS index_granularity = 8192;


INSERT INTO rag_queries_new
SELECT * FROM rag_queries;

RENAME TABLE 
    rag_queries TO rag_queries_old,
    rag_queries_new TO rag_queries;

DROP TABLE rag_queries_old;