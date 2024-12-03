use crate::{
    errors::{ErrorResponseBody, ServiceError},
    get_env, Templates,
};
use actix_web::{get, HttpResponse};
use minijinja::{context, path_loader, Environment};

#[utoipa::path(
  get,
  path = "/",
  context_path = "/",
  tag = "UI",
  responses(
      (status = 200, description = "UI meant for public consumption"),
      (status = 400, description = "Service error relating to loading the public page", body = ErrorResponseBody),
  ),
)]
#[get("/")]
pub async fn public_page(templates: Templates<'_>) -> Result<HttpResponse, ServiceError> {
    let templ = templates.get_template("demo-ui.html").unwrap();
    let trieve_api_key = get_env!("API_KEY", "API_KEY should be set");
    let response_body = templ
        .render(context! {
            trieve_api_key
        })
        .unwrap();

    Ok(HttpResponse::Ok().body(response_body))
}
