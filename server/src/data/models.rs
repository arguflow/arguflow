#![allow(clippy::extra_unused_lifetimes)]

use super::schema::*;
use chrono::NaiveDateTime;
use diesel::{expression::ValidGrouping, r2d2::ConnectionManager, PgConnection};
use openai_dive::v1::resources::chat_completion::{ChatMessage, Role};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// type alias to use in multiple places
pub type Pool = r2d2::Pool<ConnectionManager<PgConnection>>;

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, Selectable, Clone, ToSchema)]
#[diesel(table_name = users)]
pub struct User {
    pub id: uuid::Uuid,
    pub email: String,
    pub hash: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub username: Option<String>,
    pub website: Option<String>,
    pub visible_email: bool,
    pub api_key_hash: Option<String>,
    pub organization_id: uuid::Uuid,
}

impl User {
    pub fn from_details<S: Into<String>, T: Into<String>>(
        email: S,
        pwd: T,
        organization_id: uuid::Uuid,
    ) -> Self {
        User {
            id: uuid::Uuid::new_v4(),
            email: email.into(),
            hash: pwd.into(),
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            username: None,
            website: None,
            visible_email: true,
            api_key_hash: None,
            organization_id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, ValidGrouping)]
#[diesel(table_name = invitations)]
pub struct Invitation {
    pub id: uuid::Uuid,
    pub email: String,
    pub expires_at: chrono::NaiveDateTime,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub organization_id: uuid::Uuid,
}

// any type that implements Into<String> can be used to create Invitation
impl Invitation {
    pub fn from_details(email: String, organization_id: uuid::Uuid) -> Self {
        Invitation {
            id: uuid::Uuid::new_v4(),
            email: email,
            expires_at: chrono::Utc::now().naive_local() + chrono::Duration::minutes(5),
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            organization_id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, ValidGrouping)]
#[diesel(table_name = password_resets)]
pub struct PasswordReset {
    pub id: uuid::Uuid,
    pub email: String,
    pub expires_at: chrono::NaiveDateTime,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

// any type that implements Into<String> can be used to create PasswordReset
impl<T> From<T> for PasswordReset
where
    T: Into<String>,
{
    fn from(email: T) -> Self {
        PasswordReset {
            id: uuid::Uuid::new_v4(),
            email: email.into(),
            expires_at: chrono::Utc::now().naive_local() + chrono::Duration::minutes(5),
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, ValidGrouping, Clone, ToSchema)]
#[diesel(table_name = topics)]
pub struct Topic {
    pub id: uuid::Uuid,
    pub user_id: uuid::Uuid,
    pub resolution: String,
    pub side: bool,
    pub deleted: bool,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub normal_chat: bool,
}

impl Topic {
    pub fn from_details<S: Into<String>, T: Into<uuid::Uuid>>(
        resolution: S,
        user_id: T,
        normal_chat: Option<bool>,
    ) -> Self {
        Topic {
            id: uuid::Uuid::new_v4(),
            user_id: user_id.into(),
            resolution: resolution.into(),
            side: false,
            deleted: false,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            normal_chat: normal_chat.unwrap_or(false),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, Clone, ToSchema)]
#[diesel(table_name = messages)]
pub struct Message {
    pub id: uuid::Uuid,
    pub topic_id: uuid::Uuid,
    pub sort_order: i32,
    pub content: String,
    pub role: String,
    pub deleted: bool,
    pub prompt_tokens: Option<i32>,
    pub completion_tokens: Option<i32>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl From<Message> for ChatMessage {
    fn from(message: Message) -> Self {
        let role = match message.role.as_str() {
            "system" => Role::System,
            "user" => Role::User,
            _ => Role::Assistant,
        };

        ChatMessage {
            role,
            content: message.content,
            name: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct ChatMessageProxy {
    pub role: String,
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl From<ChatMessageProxy> for ChatMessage {
    fn from(message: ChatMessageProxy) -> Self {
        let role = match message.role.as_str() {
            "system" => Role::System,
            "user" => Role::User,
            _ => Role::Assistant,
        };

        ChatMessage {
            role,
            content: message.content,
            name: message.name,
        }
    }
}

impl Message {
    pub fn from_details<S: Into<String>, T: Into<uuid::Uuid>>(
        content: S,
        topic_id: T,
        sort_order: i32,
        role: String,
        prompt_tokens: Option<i32>,
        completion_tokens: Option<i32>,
    ) -> Self {
        Message {
            id: uuid::Uuid::new_v4(),
            topic_id: topic_id.into(),
            sort_order,
            content: content.into(),
            role,
            deleted: false,
            prompt_tokens,
            completion_tokens,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Queryable)]
pub struct CardMetadataWithCount {
    pub id: uuid::Uuid,
    pub content: String,
    pub link: Option<String>,
    pub author_id: uuid::Uuid,
    pub qdrant_point_id: Option<uuid::Uuid>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub tag_set: Option<String>,
    pub card_html: Option<String>,
    pub private: bool,
    pub metadata: Option<serde_json::Value>,
    pub tracking_id: Option<String>,
    pub time_stamp: Option<NaiveDateTime>,
    pub count: i64,
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, Selectable, Clone, ToSchema)]
#[diesel(table_name = card_metadata)]
pub struct CardMetadata {
    pub id: uuid::Uuid,
    pub content: String,
    pub link: Option<String>,
    pub author_id: uuid::Uuid,
    pub qdrant_point_id: Option<uuid::Uuid>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub tag_set: Option<String>,
    pub card_html: Option<String>,
    pub private: bool,
    pub metadata: Option<serde_json::Value>,
    pub tracking_id: Option<String>,
    pub time_stamp: Option<NaiveDateTime>,
    pub dataset_id: uuid::Uuid,
}

impl CardMetadata {
    #[allow(clippy::too_many_arguments)]
    pub fn from_details<S: Into<String>, T: Into<uuid::Uuid>>(
        content: S,
        card_html: &Option<String>,
        link: &Option<String>,
        tag_set: &Option<String>,
        author_id: T,
        qdrant_point_id: Option<uuid::Uuid>,
        private: bool,
        metadata: Option<serde_json::Value>,
        tracking_id: Option<String>,
        time_stamp: Option<NaiveDateTime>,
        dataset_id: uuid::Uuid,
    ) -> Self {
        CardMetadata {
            id: uuid::Uuid::new_v4(),
            content: content.into(),
            card_html: card_html.clone(),
            link: link.clone(),
            author_id: author_id.into(),
            qdrant_point_id,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            tag_set: tag_set.clone(),
            private,
            metadata,
            tracking_id,
            time_stamp,
            dataset_id,
        }
    }
}

impl CardMetadata {
    #[allow(clippy::too_many_arguments)]
    pub fn from_details_with_id<S: Into<String>, T: Into<uuid::Uuid>>(
        id: T,
        content: S,
        card_html: &Option<String>,
        link: &Option<String>,
        tag_set: &Option<String>,
        author_id: T,
        qdrant_point_id: Option<uuid::Uuid>,
        private: bool,
        metadata: Option<serde_json::Value>,
        tracking_id: Option<String>,
        time_stamp: Option<NaiveDateTime>,
        dataset_id: uuid::Uuid,
    ) -> Self {
        CardMetadata {
            id: id.into(),
            content: content.into(),
            card_html: card_html.clone(),
            link: link.clone(),
            author_id: author_id.into(),
            qdrant_point_id,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            tag_set: tag_set.clone(),
            private,
            metadata,
            tracking_id,
            time_stamp,
            dataset_id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Selectable, Insertable, Clone)]
#[diesel(table_name = card_collisions)]
pub struct CardCollisions {
    pub id: uuid::Uuid,
    pub card_id: uuid::Uuid,
    pub collision_qdrant_id: Option<uuid::Uuid>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl CardCollisions {
    pub fn from_details<T: Into<uuid::Uuid>>(card_id: T, collision_id: T) -> Self {
        CardCollisions {
            id: uuid::Uuid::new_v4(),
            card_id: card_id.into(),
            collision_qdrant_id: Some(collision_id.into()),
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, ValidGrouping, ToSchema)]
#[diesel(table_name = card_votes)]
pub struct CardVote {
    pub id: uuid::Uuid,
    pub voted_user_id: uuid::Uuid,
    pub card_metadata_id: uuid::Uuid,
    pub vote: bool,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub deleted: bool,
}

impl CardVote {
    pub fn from_details(
        voted_user_id: &uuid::Uuid,
        card_metadata_id: &uuid::Uuid,
        vote: &bool,
    ) -> Self {
        CardVote {
            id: uuid::Uuid::new_v4(),
            voted_user_id: *voted_user_id,
            card_metadata_id: *card_metadata_id,
            vote: *vote,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            deleted: false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct CardMetadataWithVotes {
    pub id: uuid::Uuid,
    pub author: Option<UserDTO>,
    pub content: String,
    pub card_html: Option<String>,
    pub link: Option<String>,
    pub qdrant_point_id: uuid::Uuid,
    pub total_upvotes: i64,
    pub total_downvotes: i64,
    pub vote_by_current_user: Option<bool>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub tag_set: Option<String>,
    pub private: bool,
    pub metadata: Option<serde_json::Value>,
    pub tracking_id: Option<String>,
    pub time_stamp: Option<NaiveDateTime>,
    pub score: Option<f64>,
}

impl From<(CardMetadata, i64)> for CardMetadataWithVotes {
    fn from(x: (CardMetadata, i64)) -> Self {
        CardMetadataWithVotes {
            id: x.0.id,
            author: None,
            content: x.0.content,
            card_html: x.0.card_html,
            link: x.0.link,
            qdrant_point_id: x.0.qdrant_point_id.unwrap_or_default(),
            total_upvotes: x.1.max(0),
            total_downvotes: 0,
            vote_by_current_user: None,
            created_at: x.0.created_at,
            updated_at: x.0.updated_at,
            tag_set: x.0.tag_set,
            private: x.0.private,
            metadata: x.0.metadata,
            tracking_id: x.0.tracking_id,
            time_stamp: x.0.time_stamp,
            score: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct CardMetadataWithVotesWithScore {
    pub id: uuid::Uuid,
    pub author: Option<UserDTO>,
    pub content: String,
    pub card_html: Option<String>,
    pub link: Option<String>,
    pub qdrant_point_id: uuid::Uuid,
    pub total_upvotes: i64,
    pub total_downvotes: i64,
    pub vote_by_current_user: Option<bool>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub tag_set: Option<String>,
    pub file_id: Option<uuid::Uuid>,
    pub file_name: Option<String>,
    pub private: bool,
    pub metadata: Option<serde_json::Value>,
    pub tracking_id: Option<String>,
    pub time_stamp: Option<NaiveDateTime>,
    pub score: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct SlimUser {
    pub id: uuid::Uuid,
    pub email: String,
    pub username: Option<String>,
    pub website: Option<String>,
    pub visible_email: bool,
    pub organization_id: uuid::Uuid,
}

impl From<User> for SlimUser {
    fn from(user: User) -> Self {
        SlimUser {
            id: user.id,
            email: user.email,
            username: user.username,
            website: user.website,
            visible_email: user.visible_email,
            organization_id: user.organization_id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct UserDTO {
    pub id: uuid::Uuid,
    pub email: Option<String>,
    pub username: Option<String>,
    pub website: Option<String>,
    pub visible_email: bool,
    pub created_at: chrono::NaiveDateTime,
    pub organization_id: uuid::Uuid,
}

impl From<User> for UserDTO {
    fn from(user: User) -> Self {
        UserDTO {
            id: user.id,
            email: if user.visible_email {
                Some(user.email)
            } else {
                None
            },
            username: user.username,
            website: user.website,
            visible_email: user.visible_email,
            created_at: user.created_at,
            organization_id: user.organization_id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Selectable, Queryable, Insertable, Clone, ToSchema)]
#[diesel(table_name = card_collection)]
pub struct CardCollection {
    pub id: uuid::Uuid,
    pub author_id: uuid::Uuid,
    pub name: String,
    pub is_public: bool,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub dataset_id: uuid::Uuid,
}

impl CardCollection {
    pub fn from_details(
        author_id: uuid::Uuid,
        name: String,
        is_public: bool,
        description: String,
        dataset_id: uuid::Uuid,
    ) -> Self {
        CardCollection {
            id: uuid::Uuid::new_v4(),
            is_public,
            author_id,
            name,
            description,
            dataset_id,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SlimCollection {
    pub id: uuid::Uuid,
    pub name: String,
    pub author_id: uuid::Uuid,
    pub of_current_user: bool,
}

#[derive(Debug, Default, Serialize, Deserialize, Queryable, ToSchema)]
pub struct CardCollectionAndFile {
    pub id: uuid::Uuid,
    pub author_id: uuid::Uuid,
    pub name: String,
    pub is_public: bool,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub file_id: Option<uuid::Uuid>,
}

#[derive(Debug, Default, Serialize, Deserialize, Queryable)]
pub struct CardCollectionAndFileWithCount {
    pub id: uuid::Uuid,
    pub author_id: uuid::Uuid,
    pub name: String,
    pub is_public: bool,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub file_id: Option<uuid::Uuid>,
    pub collection_count: Option<i32>,
}

impl From<CardCollectionAndFileWithCount> for CardCollectionAndFile {
    fn from(collection: CardCollectionAndFileWithCount) -> Self {
        CardCollectionAndFile {
            id: collection.id,
            author_id: collection.author_id,
            name: collection.name,
            is_public: collection.is_public,
            description: collection.description,
            created_at: collection.created_at,
            updated_at: collection.updated_at,
            file_id: collection.file_id,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Queryable, Insertable, Clone)]
#[diesel(table_name = card_collection_bookmarks)]
pub struct CardCollectionBookmark {
    pub id: uuid::Uuid,
    pub collection_id: uuid::Uuid,
    pub card_metadata_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub dataset_id: uuid::Uuid,
}

impl CardCollectionBookmark {
    pub fn from_details(
        collection_id: uuid::Uuid,
        card_metadata_id: uuid::Uuid,
        dataset_id: uuid::Uuid,
    ) -> Self {
        CardCollectionBookmark {
            id: uuid::Uuid::new_v4(),
            collection_id,
            card_metadata_id,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            dataset_id,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Queryable, Insertable, Clone)]
#[diesel(table_name = collections_from_files)]
pub struct FileCollection {
    pub id: uuid::Uuid,
    pub file_id: uuid::Uuid,
    pub collection_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl FileCollection {
    pub fn from_details(file_id: uuid::Uuid, collection_id: uuid::Uuid) -> Self {
        FileCollection {
            id: uuid::Uuid::new_v4(),
            file_id,
            collection_id,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct UserDTOWithVotesAndCards {
    pub id: uuid::Uuid,
    pub email: Option<String>,
    pub username: Option<String>,
    pub website: Option<String>,
    pub visible_email: bool,
    pub created_at: chrono::NaiveDateTime,
    pub total_cards_created: i64,
    pub cards: Vec<CardMetadataWithVotesWithScore>,
    pub total_upvotes_received: i32,
    pub total_downvotes_received: i32,
    pub total_votes_cast: i32,
    pub organization_id: uuid::Uuid,
}

#[derive(Debug, Serialize, Deserialize, Clone, Queryable, ToSchema)]
pub struct UserDTOWithScore {
    pub id: uuid::Uuid,
    pub email: Option<String>,
    pub username: Option<String>,
    pub website: Option<String>,
    pub visible_email: bool,
    pub created_at: chrono::NaiveDateTime,
    pub score: i64,
    pub organization_id: uuid::Uuid,
}

#[derive(Debug, Serialize, Deserialize, Clone, Queryable)]
pub struct UserScore {
    pub author_id: uuid::Uuid,
    pub score: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Queryable, Default)]
pub struct FullTextSearchResult {
    pub id: uuid::Uuid,
    pub content: String,
    pub link: Option<String>,
    pub author_id: uuid::Uuid,
    pub qdrant_point_id: Option<uuid::Uuid>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub tag_set: Option<String>,
    pub card_html: Option<String>,
    pub private: bool,
    pub metadata: Option<serde_json::Value>,
    pub tracking_id: Option<String>,
    pub time_stamp: Option<NaiveDateTime>,
    pub score: Option<f64>,
    pub count: i64,
}

impl From<CardMetadata> for FullTextSearchResult {
    fn from(card: CardMetadata) -> Self {
        FullTextSearchResult {
            id: card.id,
            content: card.content,
            link: card.link,
            author_id: card.author_id,
            qdrant_point_id: card.qdrant_point_id,
            created_at: card.created_at,
            updated_at: card.updated_at,
            tag_set: card.tag_set,
            card_html: card.card_html,
            score: None,
            private: card.private,
            metadata: card.metadata,
            tracking_id: card.tracking_id,
            time_stamp: card.time_stamp,
            count: 0,
        }
    }
}

impl From<&CardMetadata> for FullTextSearchResult {
    fn from(card: &CardMetadata) -> Self {
        FullTextSearchResult {
            id: card.id,
            content: card.content.clone(),
            link: card.link.clone(),
            author_id: card.author_id,
            qdrant_point_id: card.qdrant_point_id,
            created_at: card.created_at,
            updated_at: card.updated_at,
            tag_set: card.tag_set.clone(),
            card_html: card.card_html.clone(),
            score: None,
            private: card.private,
            tracking_id: card.tracking_id.clone(),
            time_stamp: card.time_stamp,
            metadata: card.metadata.clone(),
            count: 0,
        }
    }
}

impl From<CardMetadataWithCount> for FullTextSearchResult {
    fn from(card: CardMetadataWithCount) -> Self {
        FullTextSearchResult {
            id: card.id,
            content: card.content,
            link: card.link,
            author_id: card.author_id,
            qdrant_point_id: card.qdrant_point_id,
            created_at: card.created_at,
            updated_at: card.updated_at,
            tag_set: card.tag_set,
            card_html: card.card_html,
            score: None,
            private: card.private,
            metadata: card.metadata,
            tracking_id: card.tracking_id,
            time_stamp: card.time_stamp,
            count: card.count,
        }
    }
}

#[derive(
    Debug, Default, Serialize, Deserialize, Selectable, Queryable, Insertable, Clone, ToSchema,
)]
#[diesel(table_name = files)]
pub struct File {
    pub id: uuid::Uuid,
    pub user_id: uuid::Uuid,
    pub file_name: String,
    pub private: bool,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub size: i64,
    pub tag_set: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub link: Option<String>,
    pub time_stamp: Option<chrono::NaiveDateTime>,
}

impl File {
    #[allow(clippy::too_many_arguments)]
    pub fn from_details(
        user_id: uuid::Uuid,
        file_name: &str,
        private: bool,
        size: i64,
        tag_set: Option<String>,
        metadata: Option<serde_json::Value>,
        link: Option<String>,
        time_stamp: Option<String>,
    ) -> Self {
        File {
            id: uuid::Uuid::new_v4(),
            user_id,
            file_name: file_name.to_string(),
            private,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
            size,
            tag_set,
            metadata,
            link,
            time_stamp: time_stamp.map(|ts| {
                chrono::NaiveDateTime::parse_from_str(&ts, "%Y-%m-%d %H:%M:%S").unwrap_or_default()
            }),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct FileDTO {
    pub id: uuid::Uuid,
    pub user_id: uuid::Uuid,
    pub file_name: String,
    pub private: bool,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub size: i64,
    pub base64url_content: String,
    pub metadata: Option<serde_json::Value>,
    pub link: Option<String>,
}

impl From<File> for FileDTO {
    fn from(file: File) -> Self {
        FileDTO {
            id: file.id,
            user_id: file.user_id,
            file_name: file.file_name,
            private: file.private,
            created_at: file.created_at,
            updated_at: file.updated_at,
            size: file.size,
            base64url_content: "".to_string(),
            metadata: file.metadata,
            link: file.link,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Selectable, Queryable, Insertable, Clone)]
#[diesel(table_name = card_files)]
pub struct CardFile {
    pub id: uuid::Uuid,
    pub card_id: uuid::Uuid,
    pub file_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl CardFile {
    pub fn from_details(card_id: uuid::Uuid, file_id: uuid::Uuid) -> Self {
        CardFile {
            id: uuid::Uuid::new_v4(),
            card_id,
            file_id,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Queryable)]
pub struct CardFileWithName {
    pub card_id: uuid::Uuid,
    pub file_id: uuid::Uuid,
    pub file_name: String,
}

impl From<CardMetadataWithVotes> for CardMetadataWithVotesWithScore {
    fn from(card: CardMetadataWithVotes) -> Self {
        CardMetadataWithVotesWithScore {
            id: card.id,
            author: card.author,
            content: card.content,
            card_html: card.card_html,
            link: card.link,
            qdrant_point_id: card.qdrant_point_id,
            total_upvotes: card.total_upvotes,
            total_downvotes: card.total_downvotes,
            vote_by_current_user: card.vote_by_current_user,
            created_at: card.created_at,
            updated_at: card.updated_at,
            tag_set: card.tag_set,
            private: card.private,
            score: card.score,
            file_id: None,
            metadata: card.metadata,
            tracking_id: card.tracking_id,
            time_stamp: card.time_stamp,
            file_name: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Queryable, Insertable, Selectable)]
#[diesel(table_name = file_upload_completed_notifications)]
pub struct FileUploadCompletedNotification {
    pub id: uuid::Uuid,
    pub user_uuid: uuid::Uuid,
    pub collection_uuid: uuid::Uuid,
    pub user_read: bool,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl FileUploadCompletedNotification {
    pub fn from_details(user_uuid: uuid::Uuid, collection_uuid: uuid::Uuid) -> Self {
        FileUploadCompletedNotification {
            id: uuid::Uuid::new_v4(),
            user_uuid,
            collection_uuid,
            user_read: false,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct FileUploadCompletedNotificationWithName {
    pub id: uuid::Uuid,
    pub user_uuid: uuid::Uuid,
    pub collection_uuid: uuid::Uuid,
    pub collection_name: Option<String>,
    pub user_read: bool,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl FileUploadCompletedNotificationWithName {
    pub fn from_file_upload_notification(
        notification: FileUploadCompletedNotification,
        collection_name: String,
    ) -> Self {
        FileUploadCompletedNotificationWithName {
            id: notification.id,
            user_uuid: notification.user_uuid,
            collection_uuid: notification.collection_uuid,
            collection_name: Some(collection_name),
            user_read: notification.user_read,
            created_at: notification.created_at,
            updated_at: notification.updated_at,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, ValidGrouping)]
#[diesel(table_name = cut_cards)]
pub struct CutCard {
    pub id: uuid::Uuid,
    pub user_id: uuid::Uuid,
    pub cut_card_content: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl CutCard {
    pub fn from_details(user_id: uuid::Uuid, cut_card_content: String) -> Self {
        CutCard {
            id: uuid::Uuid::new_v4(),
            user_id,
            cut_card_content,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, ValidGrouping)]
#[diesel(table_name = card_metadata_count)]
pub struct CardMetadataCount {
    pub id: uuid::Uuid,
    pub total_rows: i64,
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, ValidGrouping)]
#[diesel(table_name = user_collection_counts)]
pub struct UserCollectionCount {
    pub id: uuid::Uuid,
    pub user_id: uuid::Uuid,
    pub collection_count: i32,
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, ValidGrouping)]
#[diesel(table_name = user_notification_counts)]
pub struct UserNotificationCount {
    pub id: uuid::Uuid,
    pub user_uuid: uuid::Uuid,
    pub notification_count: i32,
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, ValidGrouping)]
#[diesel(table_name = datasets)]
pub struct Dataset {
    pub id: uuid::Uuid,
    pub name: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl Dataset {
    pub fn from_details(name: String) -> Self {
        Dataset {
            id: uuid::Uuid::new_v4(),
            name,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Insertable, Selectable, Clone, ToSchema)]
#[diesel(table_name = organizations)]
pub struct Organization {
    pub id: uuid::Uuid,
    pub name: String,
    pub configuration: serde_json::Value,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
}

impl Organization {
    pub fn from_details(name: String, configuration: serde_json::Value) -> Self {
        Organization {
            id: uuid::Uuid::new_v4(),
            name,
            configuration,
            created_at: chrono::Utc::now().naive_local(),
            updated_at: chrono::Utc::now().naive_local(),
        }
    }
}
