// @generated automatically by Diesel CLI.

diesel::table! {
    chunk_collection (id) {
        id -> Uuid,
        author_id -> Uuid,
        name -> Text,
        description -> Text,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        dataset_id -> Uuid,
    }
}

diesel::table! {
    chunk_collection_bookmarks (id) {
        id -> Uuid,
        collection_id -> Uuid,
        chunk_metadata_id -> Uuid,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    chunk_collisions (id) {
        id -> Uuid,
        chunk_id -> Uuid,
        collision_qdrant_id -> Nullable<Uuid>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    chunk_files (id) {
        id -> Uuid,
        chunk_id -> Uuid,
        file_id -> Uuid,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    chunk_metadata (id) {
        id -> Uuid,
        content -> Text,
        link -> Nullable<Text>,
        author_id -> Uuid,
        qdrant_point_id -> Nullable<Uuid>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        tag_set -> Nullable<Text>,
        chunk_html -> Nullable<Text>,
        metadata -> Nullable<Jsonb>,
        tracking_id -> Nullable<Text>,
        time_stamp -> Nullable<Timestamp>,
        dataset_id -> Uuid,
        weight -> Float8,
    }
}

diesel::table! {
    collections_from_files (id) {
        id -> Uuid,
        collection_id -> Uuid,
        file_id -> Uuid,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    cut_chunks (id) {
        id -> Uuid,
        user_id -> Uuid,
        cut_chunk_content -> Text,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    dataset_notification_counts (id) {
        id -> Uuid,
        notification_count -> Int4,
        dataset_uuid -> Nullable<Uuid>,
    }
}

diesel::table! {
    dataset_usage_counts (id) {
        id -> Uuid,
        dataset_id -> Uuid,
        chunk_count -> Int4,
    }
}

diesel::table! {
    datasets (id) {
        id -> Uuid,
        name -> Text,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        organization_id -> Uuid,
        server_configuration -> Jsonb,
        client_configuration -> Jsonb,
    }
}

diesel::table! {
    file_upload_completed_notifications (id) {
        id -> Uuid,
        collection_uuid -> Uuid,
        user_read -> Bool,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        dataset_id -> Uuid,
    }
}

diesel::table! {
    files (id) {
        id -> Uuid,
        user_id -> Uuid,
        file_name -> Text,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        size -> Int8,
        tag_set -> Nullable<Text>,
        metadata -> Nullable<Jsonb>,
        link -> Nullable<Text>,
        time_stamp -> Nullable<Timestamp>,
        dataset_id -> Uuid,
    }
}

diesel::table! {
    invitations (id) {
        id -> Uuid,
        #[max_length = 100]
        email -> Varchar,
        organization_id -> Uuid,
        used -> Bool,
        expires_at -> Timestamp,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        role -> Int4,
    }
}

diesel::table! {
    messages (id) {
        id -> Uuid,
        topic_id -> Uuid,
        sort_order -> Int4,
        content -> Text,
        #[max_length = 10]
        role -> Varchar,
        deleted -> Bool,
        prompt_tokens -> Nullable<Int4>,
        completion_tokens -> Nullable<Int4>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        dataset_id -> Uuid,
    }
}

diesel::table! {
    organization_usage_counts (id) {
        id -> Uuid,
        org_id -> Uuid,
        dataset_count -> Int4,
        user_count -> Int4,
        file_storage -> Int4,
        message_count -> Int4,
    }
}

diesel::table! {
    organizations (id) {
        id -> Uuid,
        name -> Text,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        registerable -> Nullable<Bool>,
    }
}

diesel::table! {
    stripe_plans (id) {
        id -> Uuid,
        stripe_id -> Text,
        chunk_count -> Int4,
        file_storage -> Int4,
        user_count -> Int4,
        dataset_count -> Int4,
        message_count -> Int4,
        amount -> Int8,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        name -> Text,
    }
}

diesel::table! {
    stripe_subscriptions (id) {
        id -> Uuid,
        stripe_id -> Text,
        plan_id -> Uuid,
        organization_id -> Uuid,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        current_period_end -> Nullable<Timestamp>,
    }
}

diesel::table! {
    topics (id) {
        id -> Uuid,
        user_id -> Uuid,
        name -> Text,
        deleted -> Bool,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        dataset_id -> Uuid,
    }
}

diesel::table! {
    user_api_key (id) {
        id -> Uuid,
        user_id -> Uuid,
        api_key_hash -> Text,
        name -> Text,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        role -> Int4,
    }
}

diesel::table! {
    user_collection_counts (id) {
        id -> Uuid,
        user_id -> Uuid,
        collection_count -> Int4,
    }
}

diesel::table! {
    user_organizations (id) {
        id -> Uuid,
        user_id -> Uuid,
        organization_id -> Uuid,
        role -> Int4,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    users (id) {
        id -> Uuid,
        email -> Text,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        username -> Nullable<Text>,
        website -> Nullable<Text>,
        visible_email -> Bool,
        name -> Nullable<Text>,
    }
}

diesel::joinable!(chunk_collection -> datasets (dataset_id));
diesel::joinable!(chunk_collection -> users (author_id));
diesel::joinable!(chunk_collection_bookmarks -> chunk_collection (collection_id));
diesel::joinable!(chunk_collection_bookmarks -> chunk_metadata (chunk_metadata_id));
diesel::joinable!(chunk_files -> chunk_metadata (chunk_id));
diesel::joinable!(chunk_files -> files (file_id));
diesel::joinable!(chunk_metadata -> datasets (dataset_id));
diesel::joinable!(chunk_metadata -> users (author_id));
diesel::joinable!(collections_from_files -> chunk_collection (collection_id));
diesel::joinable!(collections_from_files -> files (file_id));
diesel::joinable!(cut_chunks -> users (user_id));
diesel::joinable!(dataset_usage_counts -> datasets (dataset_id));
diesel::joinable!(datasets -> organizations (organization_id));
diesel::joinable!(file_upload_completed_notifications -> chunk_collection (collection_uuid));
diesel::joinable!(file_upload_completed_notifications -> datasets (dataset_id));
diesel::joinable!(files -> datasets (dataset_id));
diesel::joinable!(files -> users (user_id));
diesel::joinable!(messages -> datasets (dataset_id));
diesel::joinable!(messages -> topics (topic_id));
diesel::joinable!(organization_usage_counts -> organizations (org_id));
diesel::joinable!(stripe_subscriptions -> organizations (organization_id));
diesel::joinable!(stripe_subscriptions -> stripe_plans (plan_id));
diesel::joinable!(topics -> datasets (dataset_id));
diesel::joinable!(topics -> users (user_id));
diesel::joinable!(user_api_key -> users (user_id));
diesel::joinable!(user_collection_counts -> users (user_id));
diesel::joinable!(user_organizations -> organizations (organization_id));
diesel::joinable!(user_organizations -> users (user_id));

diesel::allow_tables_to_appear_in_same_query!(
    chunk_collection,
    chunk_collection_bookmarks,
    chunk_collisions,
    chunk_files,
    chunk_metadata,
    collections_from_files,
    cut_chunks,
    dataset_notification_counts,
    dataset_usage_counts,
    datasets,
    file_upload_completed_notifications,
    files,
    invitations,
    messages,
    organization_usage_counts,
    organizations,
    stripe_plans,
    stripe_subscriptions,
    topics,
    user_api_key,
    user_collection_counts,
    user_organizations,
    users,
);
