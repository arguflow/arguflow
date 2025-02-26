use crate::errors::ServiceError;
use derive_more::Display;
use reqwest;
use serde::{Deserialize, Serialize};

fn get_chunkr_credentials(api_key: Option<&str>) -> Result<(String, String), ServiceError> {
    let api_url = std::env::var("CHUNKR_API_URL").unwrap_or("https://api.chunkr.ai".to_string());
    let api_key = match api_key {
        Some(key) => key.to_string(),
        None => std::env::var("CHUNKR_API_KEY").map_err(|_| {
            ServiceError::InternalServerError("CHUNKR_API_KEY should be set".to_string())
        })?,
    };
    Ok((format!("{}/api/v1/task", api_url), api_key))
}

pub async fn create_chunkr_task(
    file_name: &str,
    file_base64: &str,
    api_key: Option<&str>,
) -> Result<TaskResponse, ServiceError> {
    let client = reqwest::Client::new();
    let (api_url, api_key) = get_chunkr_credentials(api_key)?;

    let response = client
        .post(format!("{}/parse", api_url))
        .header("Authorization", api_key)
        .json(&serde_json::json!({
            "file_name": file_name,
            "file": file_base64,
            "pipeline": "Azure",
            "high_resolution": true,
            "chunk_processing": {
                "ignore_headers_and_footers": false,
            },
        }))
        .send()
        .await
        .map_err(|e| {
            ServiceError::InternalServerError(format!("Failed to send create chunkr task: {}", e))
        })?
        .error_for_status()
        .map_err(|e| {
            ServiceError::InternalServerError(format!("Failed to create chunkr task: {}", e))
        })?
        .json::<TaskResponse>()
        .await
        .map_err(|e| {
            ServiceError::InternalServerError(format!(
                "Failed to parse create chunkr task response: {}",
                e
            ))
        })?;

    Ok(response)
}

pub async fn get_chunkr_task(
    task_id: &str,
    api_key: Option<&str>,
) -> Result<TaskResponse, ServiceError> {
    let client = reqwest::Client::new();
    let (api_url, api_key) = get_chunkr_credentials(api_key)?;
    let response = client
        .get(format!("{}/{}", api_url, task_id))
        .header("Authorization", api_key)
        .send()
        .await
        .map_err(|e| {
            ServiceError::InternalServerError(format!(
                "Failed to send get chunkr task request: {}",
                e
            ))
        })?
        .error_for_status()
        .map_err(|e| {
            ServiceError::InternalServerError(format!("Failed to get chunkr task: {}", e))
        })?
        .json::<TaskResponse>()
        .await
        .map_err(|e| {
            ServiceError::InternalServerError(format!(
                "Failed to parse get chunkr task response: {}",
                e
            ))
        })?;
    Ok(response)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BoundingBox {
    pub height: f32,
    pub left: f32,
    pub top: f32,
    pub width: f32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Chunk {
    pub chunk_id: String,
    pub chunk_length: u32,
    pub segments: Vec<Segment>,
    pub embed: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Configuration {
    pub chunk_processing: ChunkProcessing,
    pub expires_in: Option<i32>,
    pub high_resolution: bool,
    pub input_file_url: Option<String>,
    pub ocr_strategy: OcrStrategy,
    pub segment_processing: SegmentProcessing,
    pub segmentation_strategy: SegmentationStrategy,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExtractedField {
    pub name: String,
    pub field_type: String,
    #[serde(
        serialize_with = "serialize_value",
        deserialize_with = "deserialize_value"
    )]
    pub value: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExtractedJson {
    pub title: String,
    pub schema_type: String,
    pub extracted_fields: Vec<ExtractedField>,
}

#[derive(Debug)]
pub struct Field {
    pub name: String,
    pub description: String,
    pub field_type: String,
    pub default: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JsonSchema {
    pub title: String,
    #[serde(rename = "type")]
    pub schema_type: String,
    pub properties: Vec<Property>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Model {
    Fast,
    HighQuality,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OCRResult {
    pub bbox: BoundingBox,
    pub confidence: Option<f32>,
    pub text: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum OcrStrategy {
    Auto,
    All,
    Off,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OutputResponse {
    pub chunks: Vec<Chunk>,
    pub file_name: Option<String>,
    pub page_count: Option<u32>,
    pub pdf_url: Option<String>,
    pub extracted_json: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Property {
    pub name: String,
    pub title: Option<String>,
    #[serde(rename = "type")]
    pub prop_type: String,
    pub description: Option<String>,
    pub default: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Segment {
    pub bbox: BoundingBox,
    pub confidence: Option<f32>,
    pub content: String,
    pub html: String,
    pub image: Option<String>,
    pub llm: Option<String>,
    pub markdown: String,
    pub ocr: Option<Vec<OCRResult>>,
    pub page_height: f32,
    pub page_number: u32,
    pub page_width: f32,
    pub segment_id: String,
    pub segment_type: SegmentType,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum SegmentationStrategy {
    LayoutAnalysis,
    Page,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum SegmentType {
    Caption,
    Footnote,
    Formula,
    ListItem,
    Page,
    PageFooter,
    PageHeader,
    Picture,
    SectionHeader,
    Table,
    Text,
    Title,
}

#[derive(Debug, Clone, Serialize, Deserialize, Display)]
pub enum Status {
    #[display("Canceled")]
    Canceled,
    #[display("Failed")]
    Failed,
    #[display("Processing")]
    Processing,
    #[display("Starting")]
    Starting,
    #[display("Completed")] // To match pdf2md output
    Succeeded,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskResponse {
    pub configuration: Configuration,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub finished_at: Option<String>,
    pub message: String,
    pub output: Option<OutputResponse>,
    pub started_at: Option<String>,
    pub status: Status,
    pub task_id: String,
    pub task_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChunkProcessing {
    pub ignore_headers_and_footers: bool,
    pub target_length: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SegmentProcessing {
    #[serde(rename = "Title", alias = "title")]
    pub title: Option<AutoGenerationConfig>,
    #[serde(rename = "SectionHeader", alias = "section_header")]
    pub section_header: Option<AutoGenerationConfig>,
    #[serde(rename = "Text", alias = "text")]
    pub text: Option<AutoGenerationConfig>,
    #[serde(rename = "ListItem", alias = "list_item")]
    pub list_item: Option<AutoGenerationConfig>,
    #[serde(rename = "Table", alias = "table")]
    pub table: Option<LlmGenerationConfig>,
    #[serde(rename = "Picture", alias = "picture")]
    pub picture: Option<PictureGenerationConfig>,
    #[serde(rename = "Caption", alias = "caption")]
    pub caption: Option<AutoGenerationConfig>,
    #[serde(rename = "Formula", alias = "formula")]
    pub formula: Option<LlmGenerationConfig>,
    #[serde(rename = "Footnote", alias = "footnote")]
    pub footnote: Option<AutoGenerationConfig>,
    #[serde(rename = "PageHeader", alias = "page_header")]
    pub page_header: Option<AutoGenerationConfig>,
    #[serde(rename = "PageFooter", alias = "page_footer")]
    pub page_footer: Option<AutoGenerationConfig>,
    #[serde(rename = "Page", alias = "page")]
    pub page: Option<AutoGenerationConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AutoGenerationConfig {
    pub crop_image: CroppingStrategy,
    pub html: GenerationStrategy,
    pub llm: Option<String>,
    pub markdown: GenerationStrategy,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LlmGenerationConfig {
    pub crop_image: CroppingStrategy,
    pub html: GenerationStrategy,
    pub llm: Option<String>,
    pub markdown: GenerationStrategy,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PictureGenerationConfig {
    pub crop_image: PictureCroppingStrategy,
    pub html: GenerationStrategy,
    pub llm: Option<String>,
    pub markdown: GenerationStrategy,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum GenerationStrategy {
    LLM,
    Auto,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum CroppingStrategy {
    Auto,
    All,
    Off,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum PictureCroppingStrategy {
    All,
    Auto,
    Off,
}

fn serialize_value<S>(value: &serde_json::Value, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    value.serialize(serializer)
}

fn deserialize_value<'de, D>(deserializer: D) -> Result<serde_json::Value, D::Error>
where
    D: serde::Deserializer<'de>,
{
    serde_json::Value::deserialize(deserializer)
}
