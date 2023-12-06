use super::auth_handler::RequireAuth;
use crate::{
    data::{
        models::{Invitation, Pool},
        validators::email_regex,
    },
    errors::{DefaultError, ServiceError},
    operators::user_operator::get_user_by_email_query,
};
use actix_web::{web, HttpRequest, HttpResponse};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, ToSchema)]
pub struct InvitationResponse {
    pub registration_url: String,
}

#[derive(Deserialize, ToSchema)]
pub struct InvitationData {
    pub email: String,
    pub organization_id: uuid::Uuid,
}

#[utoipa::path(
    post,
    path = "/invitation",
    context_path = "/api",
    tag = "invitation",
    request_body(content = InvitationData, description = "JSON request payload to send an invitation", content_type = "application/json"),
    responses(
        (status = 200, description = "Get a registration URL to set password for a given email", body = [InvitationResponse]),
        (status = 400, description = "Invalid email", body = [DefaultError]),
    )
)]
pub async fn post_invitation(
    request: HttpRequest,
    invitation_data: web::Json<InvitationData>,
    pool: web::Data<Pool>,
    _required_user: RequireAuth,
) -> Result<HttpResponse, actix_web::Error> {
    let invitation_data = invitation_data.into_inner();
    let email = invitation_data.email;

    if !email_regex().is_match(&email) {
        return Ok(
            HttpResponse::BadRequest().json(crate::errors::DefaultError {
                message: "Invalid email",
            }),
        );
    }

    // get the host from the request
    let host_name = request
        .headers()
        .get("Origin")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let registration_url = web::block(move || {
        create_invitation(host_name, email, invitation_data.organization_id, pool)
    })
    .await?
    .map_err(|e| ServiceError::BadRequest(e.message.to_string()))?;

    Ok(HttpResponse::Ok().json(InvitationResponse { registration_url }))
}

pub fn create_invitation(
    app_url: String,
    email: String,
    organization_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<String, DefaultError> {
    let invitation = create_invitation_query(email, organization_id, pool)?;
    // send_invitation(app_url, &invitation)

    Ok(format!(
        "{}/auth/register/{}?email={}",
        app_url, invitation.id, invitation.email
    ))
}

/// Diesel query
fn create_invitation_query(
    email: String,
    organization_id: uuid::Uuid,
    pool: web::Data<Pool>,
) -> Result<Invitation, DefaultError> {
    use crate::data::schema::invitations::dsl::invitations;

    let user_exists = get_user_by_email_query(&email, &pool).is_ok();
    if user_exists {
        return Err(DefaultError {
            message: "An account with this email already exists.",
        });
    }

    let mut conn = pool.get().unwrap();

    let new_invitation = Invitation::from_details(email, organization_id);

    let inserted_invitation = diesel::insert_into(invitations)
        .values(&new_invitation)
        .get_result(&mut conn)
        .map_err(|_db_error| DefaultError {
            message: "Error inserting invitation.",
        })?;

    Ok(inserted_invitation)
}
