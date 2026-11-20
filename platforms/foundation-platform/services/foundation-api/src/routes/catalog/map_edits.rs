//! Admin polygon edits that customers see before the next bake (root ADR-0112).
//!
//! Every edit is audited inside its request (root ADR-0569 §3): the request is recorded before the
//! store is asked, and the store's answer after, with the editor's staff account. An edit whose
//! request cannot be recorded is not sent. The store's folded rows are deleted later; the audit
//! chain keeps who asked for what.

use super::{ApiError, AppState, AuthorizedPrincipal};
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::Json;
use catalog_application::ports::{AuditAppend, MapEditStoreError};
use catalog_application::{AppendMapEditError, AppendMapEditInput};
use foundation_contracts::catalog::{MapEditRequest, MapEditResponse};
use foundation_shared_kernel::ids::StaffId;
use std::sync::Arc;
use uuid::Uuid;

/// What the chain keeps of an edit: its shape, never its geometry or property values.
struct EditShape {
    unit: String,
    feature_id: String,
    op: String,
    idempotency_key: String,
    has_geometry: bool,
    property_keys: Vec<String>,
}

impl EditShape {
    fn of(unit: &str, body: &MapEditRequest) -> Self {
        let mut property_keys: Vec<String> = body
            .properties
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .map(|map| map.keys().cloned().collect())
            .unwrap_or_default();
        property_keys.sort_unstable();
        Self {
            unit: unit.to_owned(),
            feature_id: body.feature_id.clone(),
            op: body.op.clone(),
            idempotency_key: body.idempotency_key.chars().take(80).collect(),
            has_geometry: body.geometry.is_some(),
            property_keys,
        }
    }

    fn event(
        &self,
        principal: &AuthorizedPrincipal,
        step: &str,
        result: &str,
        error: Option<String>,
        change_seq: Option<u64>,
    ) -> AuditAppend {
        let delete = self.op == "delete";
        let mut after = serde_json::json!({
            "op": self.op,
            "has_geometry": self.has_geometry,
            "property_keys": self.property_keys,
        });
        if let (Some(seq), Some(map)) = (change_seq, after.as_object_mut()) {
            map.insert("change_seq".to_owned(), serde_json::json!(seq));
        }
        AuditAppend {
            event_id: Uuid::new_v4(),
            occurred_at: chrono::Utc::now(),
            source: "dawneer".to_owned(),
            source_event_id: format!("map_edit:{}:{}:{step}", self.unit, self.idempotency_key),
            action: if delete {
                "map_edit.delete"
            } else {
                "map_edit.upsert"
            }
            .to_owned(),
            operation: if delete { "remove" } else { "modify" }.to_owned(),
            actor_kind: "staff".to_owned(),
            actor_staff_id: Some(principal.principal_id),
            object_type: "map_feature".to_owned(),
            object_id: format!("{}/{}", self.unit, self.feature_id)
                .chars()
                .take(300)
                .collect(),
            after: Some(after),
            reason: None,
            applied_via: Some(("api".to_owned(), self.idempotency_key.clone())),
            result: result.to_owned(),
            error: error.map(|why| why.chars().take(1000).collect()),
            // One change per edit, its request and answer and a retry of it alike.
            change_id: Some(Uuid::new_v5(
                &Uuid::NAMESPACE_URL,
                format!("map_edit:{}:{}", self.unit, self.idempotency_key).as_bytes(),
            )),
        }
    }
}

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
    let shape = EditShape::of(&unit, &body);
    // The request first: an edit the chain cannot record is not sent (root ADR-0569 §3).
    state
        .audit_events
        .append(&shape.event(&principal, "requested", "requested", None, None))
        .await
        .map_err(|error| {
            tracing::error!(error = %error, "audit chain unavailable for a map edit");
            ApiError::Unavailable(
                "the audit chain is unavailable; the edit was not saved".to_owned(),
            )
        })?;
    let outcome = state
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
        .await;
    // The store's answer, whatever it was. Losing this row does not change the editor's answer.
    let (result, error, seq) = match &outcome {
        Ok(appended) => ("succeeded", None, Some(appended.change_seq)),
        Err(
            error @ (AppendMapEditError::Invalid(_)
            | AppendMapEditError::Store(MapEditStoreError::Refused(_))),
        ) => ("refused", Some(error.to_string()), None),
        Err(error) => ("failed", Some(error.to_string()), None),
    };
    if let Err(error) = state
        .audit_events
        .append(&shape.event(&principal, "answered", result, error, seq))
        .await
    {
        tracing::error!(error = %error, "the answer of a map edit was not audited");
    }
    let appended = outcome.map_err(|error| match error {
        AppendMapEditError::Invalid(invalid) => ApiError::Unprocessable(invalid.to_string()),
        AppendMapEditError::Store(MapEditStoreError::Refused(code)) => {
            ApiError::Unprocessable(code)
        }
        AppendMapEditError::Store(MapEditStoreError::Conflict(code)) => ApiError::Conflict(code),
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
