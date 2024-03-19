use super::auth_handler::{AdminOnly, LoggedUser, OwnerOnly};
use crate::{
    data::models::{Pool, UserOrganization, UserRole},
    errors::ServiceError,
    operators::{
        organization_operator::{
            create_organization_query, delete_organization_query, get_org_usage_by_id_query,
            get_org_users_by_id_query, get_organization_by_key_query, update_organization_query,
        },
        user_operator::add_user_to_organization,
    },
};
use actix_web::{web, HttpRequest, HttpResponse};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Get Organization
///
/// Fetch the details of an organization by its id. The auth'ed user must be an admin or owner of the organization to fetch it.
#[utoipa::path(
    get,
    path = "/organization/{organization_id}",
    context_path = "/api",
    tag = "organization",
    responses(
        (status = 200, description = "Organization with the id that was requested", body = Organization),
        (status = 400, description = "Service error relating to finding the organization by id", body = ErrorResponseBody),
    ),
    params(
        ("TR-Organization" = String, Header, description = "The organization id to use for the request"),
        ("organization_id" = Option<uuid::Uuid>, Path, description = "The id of the organization you want to fetch."),
    ),
    security(
        ("ApiKey" = ["admin"]),
        
    )
)]
#[tracing::instrument(skip(pool))]
pub async fn get_organization_by_id(
    organization_id: web::Path<uuid::Uuid>,
    pool: web::Data<Pool>,
    _user: AdminOnly,
) -> Result<HttpResponse, actix_web::Error> {
    let organization_id = organization_id.into_inner();

    let org_plan_sub = get_organization_by_key_query(organization_id.into(), pool)
        .await
        .map_err(|err| ServiceError::BadRequest(err.message.into()))?;

    Ok(HttpResponse::Ok().json(org_plan_sub.with_defaults()))
}

/// Delete Organization
///
/// Delete an organization by its id. The auth'ed user must be an owner of the organization to delete it.
#[utoipa::path(
    delete,
    path = "/organization/{organization_id}",
    context_path = "/api",
    tag = "organization",
    responses(
        (status = 200, description = "Confirmation that the organization was deleted", body = Organization),
        (status = 400, description = "Service error relating to deleting the organization by id", body = ErrorResponseBody),
    ),
    params(
        ("TR-Organization" = String, Header, description = "The organization id to use for the request"),
        ("organization_id" = Option<uuid::Uuid>, Path, description = "The id of the organization you want to fetch."),
    ),
    security(
        ("ApiKey" = ["admin"]),
        
    )
)]
#[tracing::instrument(skip(pool))]
pub async fn delete_organization_by_id(
    req: HttpRequest,
    organization_id: web::Path<uuid::Uuid>,
    pool: web::Data<Pool>,
    user: OwnerOnly,
) -> Result<HttpResponse, actix_web::Error> {
    let organization_id = organization_id.into_inner();

    let org = delete_organization_query(Some(&req), Some(user.0.id), organization_id, pool)
        .await
        .map_err(|err| ServiceError::BadRequest(err.message.into()))?;

    Ok(HttpResponse::Ok().json(org))
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct UpdateOrganizationData {
    /// The id of the organization to update.
    organization_id: uuid::Uuid,
    /// The new name of the organization. If not provided, the name will not be updated.
    name: Option<String>,
}

/// Update Organization
///
/// Update an organization. Only the owner of the organization can update it.
#[utoipa::path(
    put,
    path = "/organization",
    context_path = "/api",
    tag = "organization",
    request_body(content = UpdateOrganizationData, description = "The organization data that you want to update", content_type = "application/json"),
    responses(
        (status = 200, description = "Updated organization object", body = Organization),
        (status = 400, description = "Service error relating to updating the organization", body = ErrorResponseBody),
    ),
    params(
        ("TR-Organization" = String, Header, description = "The organization id to use for the request"),
    ),
    security(
        ("ApiKey" = ["owner"]),
        
    )
)]
#[tracing::instrument(skip(pool))]
pub async fn update_organization(
    organization: web::Json<UpdateOrganizationData>,
    pool: web::Data<Pool>,
    _user: OwnerOnly,
) -> Result<HttpResponse, actix_web::Error> {
    let organization_update_data = organization.into_inner();
    let old_organization = get_organization_by_key_query(
        organization_update_data.organization_id.into(),
        pool.clone(),
    )
    .await
    .map_err(|err| ServiceError::BadRequest(err.message.into()))?;

    let updated_organization = update_organization_query(
        organization_update_data.organization_id,
        organization_update_data
            .name
            .unwrap_or(old_organization.name)
            .as_str(),
        pool,
    )
    .await
    .map_err(|err| ServiceError::BadRequest(err.message.into()))?;

    Ok(HttpResponse::Ok().json(updated_organization))
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct CreateOrganizationData {
    /// The arbitrary name which will be used to identify the organization. This name must be unique.
    name: String,
}

/// Create Organization
///
/// Create a new organization. The auth'ed user who creates the organization will be the default owner of the organization.
#[utoipa::path(
    post,
    path = "/organization",
    context_path = "/api",
    tag = "organization",
    request_body(content = CreateOrganizationData, description = "The organization data that you want to create", content_type = "application/json"),
    responses(
        (status = 200, description = "Created organization object", body = Organization),
        (status = 400, description = "Service error relating to creating the organization", body = ErrorResponseBody),
    ),
    security(
        ("ApiKey" = ["readonly"]),
        
    )
)]
#[tracing::instrument(skip(pool))]
pub async fn create_organization(
    req: HttpRequest,
    organization: web::Json<CreateOrganizationData>,
    pool: web::Data<Pool>,
    user: LoggedUser,
) -> Result<HttpResponse, actix_web::Error> {
    let organization_create_data = organization.into_inner();

    let created_organization =
        create_organization_query(organization_create_data.name.as_str(), pool.clone())
            .await
            .map_err(|err| ServiceError::BadRequest(err.message.into()))?;

    add_user_to_organization(
        Some(&req),
        Some(user.id),
        UserOrganization::from_details(user.id, created_organization.id, UserRole::Owner),
        pool,
    )
    .await?;

    Ok(HttpResponse::Ok().json(created_organization))
}

/// Get Organization Usage
///
/// Fetch the current usage specification of an organization by its id. The auth'ed user must be an admin or owner of the organization to fetch it.
#[utoipa::path(
    get,
    path = "/organization/usage/{organization_id}",
    context_path = "/api",
    tag = "organization",
    responses(
        (status = 200, description = "The current usage of the specified organization", body = OrganizationUsageCount),
        (status = 400, description = "Service error relating to finding the organization's usage by id", body = ErrorResponseBody),
    ),
    params(
        ("TR-Organization" = String, Header, description = "The organization id to use for the request"),
        ("organization_id" = Option<uuid::Uuid>, Path, description = "The id of the organization you want to fetch the usage of."),
    ),
    security(
        ("ApiKey" = ["admin"]),
        
    )
)]
#[tracing::instrument(skip(pool))]
pub async fn get_organization_usage(
    organization: web::Path<uuid::Uuid>,
    pool: web::Data<Pool>,
    _user: AdminOnly,
) -> Result<HttpResponse, actix_web::Error> {
    let org_id = organization.into_inner();

    let usage = get_org_usage_by_id_query(org_id, pool)
        .await
        .map_err(|err| ServiceError::BadRequest(err.message.into()))?;

    Ok(HttpResponse::Ok().json(usage))
}

/// Get Organization Users
///
/// Fetch the users of an organization by its id. The auth'ed user must be an admin or owner of the organization to fetch it.
#[utoipa::path(
    get,
    path = "/organization/users/{organization_id}",
    context_path = "/api",
    tag = "organization",
    responses(
        (status = 200, description = "Array of users who belong to the specified by organization", body = Vec<SlimUser>),
        (status = 400, description = "Service error relating to finding the organization's users by id", body = ErrorResponseBody),
    ),
    params(
        ("TR-Organization" = String, Header, description = "The organization id to use for the request"),
        ("organization_id" = Option<uuid::Uuid>, Path, description = "The id of the organization you want to fetch the users of."),
    ),
    security(
        ("ApiKey" = ["admin"]),
        
    )
)]
#[tracing::instrument(skip(pool))]
pub async fn get_organization_users(
    organization: web::Path<uuid::Uuid>,
    pool: web::Data<Pool>,
    _user: AdminOnly,
) -> Result<HttpResponse, actix_web::Error> {
    let org_id = organization.into_inner();

    let usage = get_org_users_by_id_query(org_id, pool)
        .await
        .map_err(|err| ServiceError::BadRequest(err.message.into()))?;

    Ok(HttpResponse::Ok().json(usage))
}
