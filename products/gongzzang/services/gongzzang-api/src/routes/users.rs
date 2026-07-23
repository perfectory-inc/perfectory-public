use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use product_identity_infrastructure::middleware::AuthenticatedUser;
use serde::Serialize;
use shared_kernel::id::{Id, UserMarker};
use tracing::{error, warn};
use user_domain::entity::{User, UserKind};
use user_domain::repository::UserRepository;

/// `Axum` handlers share this user repository state.
#[derive(Clone)]
pub struct UsersState {
    pub user_repo: Arc<dyn UserRepository>,
}

/// Serialized `User` response shape.
#[derive(Serialize)]
pub struct UserResponse {
    id: String,
    zitadel_sub: String,
    email: String,
    display_name: String,
    user_kind: String,
    roles: Vec<String>,
    created_at: String,
    updated_at: String,
    version: i64,
}

impl From<User> for UserResponse {
    fn from(u: User) -> Self {
        Self {
            id: u.id.as_str().to_owned(),
            zitadel_sub: u.zitadel_sub,
            email: u.email.as_str().to_owned(),
            display_name: u.display_name,
            user_kind: match u.user_kind {
                UserKind::Individual => "individual".to_owned(),
                UserKind::Corporation => "corporation".to_owned(),
            },
            roles: u.roles.iter().map(|r| r.as_str().to_owned()).collect(),
            created_at: u.created_at.to_rfc3339(),
            updated_at: u.updated_at.to_rfc3339(),
            version: u.version,
        }
    }
}

/// `GET /users/me` — authenticated user self lookup.
pub async fn me(auth: AuthenticatedUser) -> Json<UserResponse> {
    Json(auth.user.into())
}

/// `GET /users/:id` — only `auth.user.id == path id` is allowed.
pub async fn get_user(
    State(state): State<UsersState>,
    auth: AuthenticatedUser,
    Path(id): Path<String>,
) -> Result<Json<UserResponse>, (StatusCode, String)> {
    let id = Id::<UserMarker>::try_from_str(&id).map_err(|e| {
        warn!(error = %e, "invalid user id path parameter");
        (
            StatusCode::BAD_REQUEST,
            "사용자 식별자가 올바르지 않아요".to_owned(),
        )
    })?;
    if id.as_str() != auth.user.id.as_str() {
        return Err((
            StatusCode::FORBIDDEN,
            "이 사용자 정보는 조회할 권한이 없어요".to_owned(),
        ));
    }
    let user = state
        .user_repo
        .find_by_id(&id)
        .await
        .map_err(|e| {
            error!(error = %e, user_id = %id.as_str(), "user lookup failed");
            internal_user_lookup_error()
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "사용자를 찾을 수 없어요".to_owned()))?;
    Ok(Json(user.into()))
}

fn internal_user_lookup_error() -> (StatusCode, String) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "사용자 정보를 조회할 수 없어요".to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::internal_user_lookup_error;
    use axum::http::StatusCode;

    #[test]
    fn internal_lookup_error_does_not_expose_storage_details() {
        let (status, message) = internal_user_lookup_error();

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(message, "사용자 정보를 조회할 수 없어요");
        assert!(!message.contains("database"));
        assert!(!message.contains("sqlx"));
    }
}
