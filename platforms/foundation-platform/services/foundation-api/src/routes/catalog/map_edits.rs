//! Admin polygon edits that customers see before the next bake (root ADR-0112).

use super::{ApiError, AppState, AuthorizedPrincipal};
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::Json;
use catalog_application::ports::MapEditStoreError;
use catalog_application::{AppendMapEditError, AppendMapEditInput};
use foundation_contracts::catalog::{MapEditRequest, MapEditResponse};
use foundation_shared_kernel::ids::StaffId;
use std::sync::Arc;

#[utoipa::path(
    post,
    path = "/catalog/v1/map-edits/{unit}",
    operation_id = "appendMapEdit",
    params(("unit" = String, Path, description = "Polygon publication unit, e.g. complex")),
    request_body = MapEditRequest,
    responses(
        (status = 201, body = MapEditResponse, description = "Saved; customers see it on their next overlay read"),
        (status = 200, body = MapEditResponse, description = "The same idempotency key and edit were already saved"),
        (status = 409, description = "Idempotency key reused for a different edit, or the unit needs a bake first"),
        (status = 422, description = "The edit is invalid: operation, topology, bounds, id or properties"),
        (status = 503, description = "The edit store is not configured or not reachable; nothing was saved")
    ),
    security(("bearerAuth" = []))
)]
pub async fn append_map_edit(
    State(state): State<Arc<AppState>>,
    Path(unit): Path<String>,
    Extension(principal): Extension<AuthorizedPrincipal>,
    Json(body): Json<MapEditRequest>,
) -> Result<(StatusCode, Json<MapEditResponse>), ApiError> {
    let appended = state
        .append_map_edit
        .execute(AppendMapEditInput {
            unit: unit.clone(),
            feature_id: body.feature_id,
            operation: body.op,
            geometry: body.geometry,
            properties: body.properties,
            editor: StaffId::new(principal.principal_id),
            idempotency_key: body.idempotency_key,
        })
        .await
        .map_err(|error| match error {
            AppendMapEditError::Invalid(invalid) => ApiError::Unprocessable(invalid.to_string()),
            AppendMapEditError::Store(MapEditStoreError::Refused(code)) => {
                ApiError::Unprocessable(code)
            }
            AppendMapEditError::Store(MapEditStoreError::Conflict(code)) => {
                ApiError::Conflict(code)
            }
            AppendMapEditError::Store(MapEditStoreError::Unavailable(detail)) => {
                tracing::error!(error = %detail, "map edit store unavailable");
                ApiError::Unavailable(
                    "the map edit store is unavailable; the edit was not saved".to_owned(),
                )
            }
        })?;
    let status = if appended.replayed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((
        status,
        Json(MapEditResponse {
            unit,
            change_seq: appended.change_seq,
            replayed: appended.replayed,
        }),
    ))
}
