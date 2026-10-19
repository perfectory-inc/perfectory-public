//! Operator switches of a scheduled job, recorded under a staff identity (root ADR-0282 §1, §2).
//!
//! This records the request in the append-only ledger and answers its id. Writing the switch file
//! the runner reads and starting a `run_now` follow in the next stages of ADR-0282.

use std::sync::Arc;

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::Json;
use catalog_application::{RecordOperatorActionError, RecordOperatorActionInput};
use catalog_domain::operator_action::{OperatorActionError, RunWindow};
use chrono::Utc;
use foundation_contracts::data_catalog::{OperatorActionRecorded, OperatorActionRequest};
use foundation_contracts::error::{ApiErrorResponse, InternalApiErrorResponse};
use foundation_shared_kernel::ids::StaffId;

use super::operations::is_listed_job;
use super::{ApiError, AppState};
use crate::identity_authorization::AuthorizedPrincipal;

#[utoipa::path(
    post,
    path = "/data-catalog/v1/scheduled-jobs/{job_id}/operator-actions",
    operation_id = "recordOperatorAction",
    params(("job_id" = String, Path, description = "The job's id in orchestration/jobs.v1.json")),
    request_body = OperatorActionRequest,
    responses(
        (status = 201, body = OperatorActionRecorded, description = "Recorded in the append-only ledger"),
        (status = 401, description = "Staff identity credential is missing or invalid"),
        (status = 403, description = "Principal may not operate scheduled jobs"),
        (status = 404, description = "No such job in the job list", body = ApiErrorResponse),
        (status = 422, description = "Unknown action, a reason that is not one line of 1 to 300 characters, drop_pending on anything but a pause, or a window that is not two HH:MM times lasting 1 to 168 hours on a set_window", body = ApiErrorResponse),
        (status = 503, description = "The ledger could not be written; nothing was recorded", body = InternalApiErrorResponse),
    )
)]
pub(super) async fn record_operator_action(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
    Extension(principal): Extension<AuthorizedPrincipal>,
    Json(body): Json<OperatorActionRequest>,
) -> Result<(StatusCode, Json<OperatorActionRecorded>), ApiError> {
    if !is_listed_job(&job_id)? {
        return Err(ApiError::NotFound(format!(
            "no job {job_id} in the job list"
        )));
    }
    // A window is three fields; any one of them makes it a window the rules then check whole.
    let window = match (body.window_start, body.window_end, body.window_hours) {
        (None, None, None) => None,
        (Some(start), Some(end), Some(hours)) => Some(RunWindow { start, end, hours }),
        _ => {
            return Err(ApiError::Unprocessable(
                OperatorActionError::InvalidWindow.to_string(),
            ))
        }
    };
    let record = state
        .record_operator_action
        .execute(
            RecordOperatorActionInput {
                job_id,
                action: body.action,
                window,
                drop_pending: body.drop_pending,
                reason: body.reason,
                actor: StaffId::new(principal.principal_id),
            },
            Utc::now(),
        )
        .await
        .map_err(|error| match error {
            RecordOperatorActionError::Invalid(invalid) => {
                ApiError::Unprocessable(invalid.to_string())
            }
            RecordOperatorActionError::SwitchBoard(board) => {
                tracing::error!(error = %board, "job switch board unavailable");
                ApiError::Unavailable(
                    "the job switch board is unavailable; ask again (a request already in the ledger is switched then)"
                        .to_owned(),
                )
            }
            RecordOperatorActionError::Ledger(ledger) => {
                tracing::error!(error = %ledger, "operator action ledger unavailable");
                ApiError::Unavailable(
                    "the operator action ledger is unavailable; nothing was recorded".to_owned(),
                )
            }
        })?;
    Ok((
        StatusCode::CREATED,
        Json(OperatorActionRecorded {
            id: record.id.to_string(),
            job_id: record.job_id,
            action: record.action.as_str().to_owned(),
            drop_pending: record.drop_pending,
            actor: record.actor.to_string(),
            requested_at: record.requested_at,
            expires_at: record.expires_at,
            window_start: record.window_start,
            window_end: record.window_end,
        }),
    ))
}
