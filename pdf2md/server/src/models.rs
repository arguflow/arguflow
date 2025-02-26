use crate::operators::chunkr::{Status, TaskResponse};
use derive_more::derive::Display;
use s3::creds::time::OffsetDateTime;
use utoipa::ToSchema;

pub type RedisPool = bb8_redis::bb8::Pool<bb8_redis::RedisConnectionManager>;

pub trait TaskMessage {
    fn increment_attempt(&mut self);
    fn get_attempts(&self) -> u8;
    fn has_remaining_attempts(&self) -> bool {
        self.get_attempts() < 3
    }
    fn get_task_id(&self) -> uuid::Uuid;
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
pub struct FileTask {
    pub id: uuid::Uuid,
    pub file_name: String,
    pub upload_file_data: UploadFileReqPayload,
    pub attempt_number: u8,
}

impl TaskMessage for FileTask {
    fn increment_attempt(&mut self) {
        self.attempt_number += 1;
    }
    fn get_attempts(&self) -> u8 {
        self.attempt_number
    }
    fn get_task_id(&self) -> uuid::Uuid {
        self.id
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
pub struct ChunkingTask {
    pub id: uuid::Uuid,
    pub file_name: String,
    pub page_num: u32,
    pub params: ChunkingParams,
    pub attempt_number: u8,
}

impl TaskMessage for ChunkingTask {
    fn increment_attempt(&mut self) {
        self.attempt_number += 1;
    }
    fn get_attempts(&self) -> u8 {
        self.attempt_number
    }
    fn get_task_id(&self) -> uuid::Uuid {
        self.id
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, ToSchema)]
pub struct CreateFileTaskResponse {
    pub id: uuid::Uuid,
    pub file_name: String,
    pub status: FileTaskStatus,
    /// Only returned if the provider is LLM.
    pub pos_in_queue: Option<String>,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, ToSchema, Display)]
pub enum Provider {
    #[display("Chunkr")]
    Chunkr,
    #[display("LLM")]
    LLM,
}

impl std::str::FromStr for Provider {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Chunkr" => Ok(Provider::Chunkr),
            "LLM" => Ok(Provider::LLM),
            _ => Err(format!("Unknown provider: {}", s)),
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, ToSchema)]
pub struct UploadFileReqPayload {
    /// Name of the file
    pub file_name: String,
    /// Base64 encoded file. This is the standard base64 encoding.
    pub base64_file: String,
    /// The provider to use for the task. If Chunkr is used then llm_model, llm_api_key, system_prompt, webhook_url, webhook_payload_template are ignored. If not provided, Chunkr will be used.
    pub provider: Option<Provider>,
    /// The name of the llm model to use for the task. If not provided, the default model will be used. We support all models from (OpenRouter)[https://openrouter.ai/models]
    pub llm_model: Option<String>,
    /// The API key to use for the llm being used.
    pub llm_api_key: Option<String>,
    /// The System prompt that will be used for the conversion of the file.
    pub system_prompt: Option<String>,
    /// Optional webhook URL to receive notifications for each page processed. Default is 'Convert the following PDF page to markdown. Return only the markdown with no explanation text. Do not exclude any content from the page.'
    pub webhook_url: Option<String>,
    /// Optional webhook payload template with placeholder values.
    /// Supports the following template variables:
    /// - {{status}} : Current status of the processing
    /// - {{file_name}} : Original file name
    /// - {{result}} : Processing result/output
    /// - {{error}} : Error message if any
    ///   Example: {"status": "{{status}}", "data": {"output": "{{result}}"}}
    ///   If not provided, the default template will be used.
    pub webhook_payload_template: Option<String>,
    /// The API key to use for the Chunkr API.
    pub chunkr_api_key: Option<String>,
}

#[derive(Debug)]
pub struct WebhookPayloadData {
    pub task_id: String,
    pub file_name: String,
    pub pages: u32,
    pub pages_processed: u32,
    pub content: String,
    pub page_num: u32,
    pub usage: String,
    pub status: String,
    pub timestamp: String,
}

impl WebhookPayloadData {
    pub fn from_tasks(task: FileTaskClickhouse, page: ChunkClickhouse) -> Self {
        Self {
            task_id: task.id.clone(),
            file_name: task.id.clone(),
            pages: task.pages,
            pages_processed: task.pages_processed,
            content: page.content.clone(),
            page_num: page.page,
            usage: page.usage.clone(),
            status: task.status,
            timestamp: task.created_at.to_string(),
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
pub struct ChunkingParams {
    pub llm_model: Option<String>,
    pub llm_api_key: Option<String>,
    pub system_prompt: Option<String>,
    pub webhook_url: Option<String>,
    pub webhook_payload_template: Option<String>,
}

impl From<UploadFileReqPayload> for ChunkingParams {
    fn from(payload: UploadFileReqPayload) -> Self {
        Self {
            llm_model: payload.llm_model,
            llm_api_key: payload.llm_api_key,
            system_prompt: payload.system_prompt,
            webhook_url: payload.webhook_url,
            webhook_payload_template: payload.webhook_payload_template,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, clickhouse::Row, Clone)]
pub struct FileTaskClickhouse {
    pub id: String,
    pub file_name: String,
    pub pages: u32,
    pub pages_processed: u32,
    pub status: String,
    pub provider: String,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub created_at: OffsetDateTime,
    pub chunkr_task_id: String,
    pub chunkr_api_key: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, clickhouse::Row, Clone)]
pub struct ChunkClickhouse {
    pub id: String,
    pub task_id: String,
    pub content: String,
    pub page: u32,
    pub usage: String,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub created_at: OffsetDateTime,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, ToSchema)]
pub struct Chunk {
    pub id: String,
    pub task_id: String,
    pub content: String,
    pub page_num: u32,
    pub usage: serde_json::Value,
    pub created_at: String,
}

impl From<ChunkClickhouse> for Chunk {
    fn from(c: ChunkClickhouse) -> Self {
        Self {
            id: c.id,
            task_id: c.task_id,
            content: c.content,
            page_num: c.page,
            usage: serde_json::from_str(&c.usage).unwrap(),
            created_at: c.created_at.to_string(),
        }
    }
}

impl From<TaskResponse> for Vec<Chunk> {
    fn from(response: TaskResponse) -> Self {
        if let Some(output) = response.output {
            let mut page_contents: std::collections::HashMap<u32, String> =
                std::collections::HashMap::new();

            for chunk in output.chunks {
                for segment in chunk.segments {
                    let page_num = segment.page_number;
                    page_contents
                        .entry(page_num)
                        .and_modify(|content| {
                            content.push_str("\n\n");
                            content.push_str(&segment.markdown);
                        })
                        .or_insert(segment.markdown);
                }
            }

            page_contents
                .into_iter()
                .map(|(page_num, content)| Chunk {
                    id: uuid::Uuid::new_v4().to_string(),
                    task_id: response.task_id.clone(),
                    content,
                    page_num,
                    usage: serde_json::json!({}),
                    created_at: response.created_at.to_string(),
                })
                .collect()
        } else {
            vec![]
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct GetTaskRequest {
    pub pagination_token: Option<u32>,
    pub limit: Option<u32>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, ToSchema)]
pub struct GetTaskResponse {
    pub id: String,
    pub file_name: String,
    pub file_url: Option<String>,
    pub total_document_pages: u32,
    pub pages_processed: u32,
    pub status: String,
    pub created_at: String,
    pub pages: Option<Vec<Chunk>>,
    pub pagination_token: Option<u32>,
}

impl GetTaskResponse {
    pub fn new(task: FileTaskClickhouse) -> Self {
        Self {
            id: task.id.clone(),
            file_name: task.file_name.clone(),
            file_url: None,
            total_document_pages: task.pages,
            pages_processed: task.pages_processed,
            status: task.status,
            created_at: task.created_at.to_string(),
            pagination_token: None,
            pages: None,
        }
    }

    pub fn new_with_pages(
        task: FileTaskClickhouse,
        pages: Vec<ChunkClickhouse>,
        file_url: String,
    ) -> Self {
        Self {
            id: task.id.clone(),
            file_name: task.file_name.clone(),
            file_url: Some(file_url),
            total_document_pages: task.pages,
            pages_processed: task.pages_processed,
            status: task.status,
            created_at: task.created_at.to_string(),
            pagination_token: pages.last().map(|c| c.page),
            pages: Some(pages.into_iter().map(Chunk::from).collect()),
        }
    }

    pub fn new_with_chunkr(task: FileTaskClickhouse, chunkr_task: TaskResponse) -> Self {
        let pages = Vec::from(chunkr_task.clone());
        Self {
            id: task.id.clone(),
            file_name: task.file_name.clone(),
            file_url: Some(
                chunkr_task
                    .output
                    .ok_or("No output found")
                    .unwrap()
                    .pdf_url
                    .unwrap_or_default(),
            ),
            total_document_pages: task.pages,
            pages_processed: match chunkr_task.status {
                Status::Succeeded => task.pages,
                _ => 0,
            },
            status: format!("{}", chunkr_task.status),
            created_at: task.created_at.to_string(),
            pagination_token: None,
            pages: Some(pages),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Display, Clone, PartialEq, Eq, ToSchema)]
pub enum FileTaskStatus {
    #[display("Created")]
    Created,
    #[display("Processing {_0} pages")]
    ProcessingFile(u32),
    #[display("Processed {_0} pages")]
    ChunkingFile(u32),
    #[display("Completed")]
    Completed,
    #[display("Failed")]
    Failed,
}

impl FileTaskStatus {
    pub fn get_pages_processed(&self) -> Option<u32> {
        match self {
            FileTaskStatus::ChunkingFile(pages) => Some(*pages),
            _ => None,
        }
    }
}

impl From<String> for FileTaskStatus {
    fn from(s: String) -> Self {
        match s.as_str() {
            "Created" => FileTaskStatus::Created,
            "Completed" => FileTaskStatus::Completed,
            "Failed" => FileTaskStatus::Failed,
            _ => {
                // Try to parse processing or pageing status
                if let Some(pages) = s
                    .strip_prefix("Processed ")
                    .and_then(|s| s.strip_suffix(" pages"))
                {
                    if let Ok(pages) = pages.parse::<u32>() {
                        return FileTaskStatus::ChunkingFile(pages);
                    }
                } else if let Some(pages) = s
                    .strip_prefix("Processing ")
                    .and_then(|s| s.strip_suffix(" pages"))
                {
                    if let Ok(pages) = pages.parse::<u32>() {
                        return FileTaskStatus::ProcessingFile(pages);
                    }
                }
                FileTaskStatus::Failed
            }
        }
    }
}
