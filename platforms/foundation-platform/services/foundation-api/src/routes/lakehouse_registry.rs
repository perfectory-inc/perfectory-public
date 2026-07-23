//! Internal Lakehouse Registry HTTP handlers.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use lakehouse_application::RegisterLakehouseObjectArtifactInput;
use serde::{Deserialize, Serialize};

use crate::routes::ApiError;
use crate::state::AppState;

/// Request body for registering one governed lakehouse object artifact.
#[derive(Debug, Deserialize)]
pub struct RegisterLakehouseObjectArtifactRequest {
    qualified_name: String,
    namespace_id: String,
    object_key: String,
    content_type: String,
    checksum_sha256: String,
    size_bytes: u64,
    logical_record_count: Option<u64>,
}

/// Response body returned after an object artifact registration.
#[derive(Debug, Serialize)]
pub struct RegisterLakehouseObjectArtifactResponse {
    artifact_id: String,
    qualified_name: String,
    object_key: String,
}

/// Registers one object artifact in the Foundation Platform Lakehouse Registry.
pub async fn register_object_artifact(
    State(state): State<Arc<AppState>>,
    Json(body): Json<RegisterLakehouseObjectArtifactRequest>,
) -> Result<(StatusCode, Json<RegisterLakehouseObjectArtifactResponse>), ApiError> {
    let receipt = state
        .register_lakehouse_object_artifact
        .execute(RegisterLakehouseObjectArtifactInput {
            qualified_name: body.qualified_name,
            namespace_id: body.namespace_id,
            object_key: body.object_key,
            content_type: body.content_type,
            checksum_sha256: body.checksum_sha256,
            size_bytes: body.size_bytes,
            logical_record_count: body.logical_record_count,
        })
        .await?;

    Ok((
        StatusCode::CREATED,
        Json(RegisterLakehouseObjectArtifactResponse {
            artifact_id: receipt.artifact_id,
            qualified_name: receipt.qualified_name,
            object_key: receipt.object_key,
        }),
    ))
}
