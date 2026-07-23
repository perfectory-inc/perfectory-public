//! Internal normalization proposal intake HTTP handlers.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use foundation_contracts::error::{ApiErrorResponse, InternalApiErrorResponse};
use foundation_contracts::normalization::{
    FoundationSubmissionResult, IntakeError, NormalizationApplyRequest, NormalizationApplyResult,
    NormalizationProposal, NormalizationProposalSubmission, NormalizationRequest,
    NormalizationReviewRequest, NormalizationReviewResult, NormalizationRollbackRequest,
    NormalizationRollbackResult, NormalizationTargetKind as NormalizationTargetKindContract,
    ProposalValidation, SubmissionMetadata, TraceContext,
};
use foundation_normalization_application::{
    NormalizationApplicationCommand, NormalizationProposalReviewCommand,
    NormalizationProposalSubmissionCommand, NormalizationRollbackCommand,
};
use foundation_normalization_domain::{
    NormalizationError, NormalizationProposalStatus, NormalizationReviewDecision,
    NormalizationTargetKind,
};
use foundation_shared_kernel::ids::PrincipalId;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};
use uuid::Uuid;

use super::ApiError;
use crate::identity_authorization::AuthorizedPrincipal;
use crate::state::AppState;

const NORMALIZATION_PLATFORM_WIRE_NAME: &str = "foundation-platform";

#[derive(OpenApi)]
#[openapi(
    paths(
        submit_proposal,
        approve_proposal,
        reject_proposal,
        apply_proposal,
        rollback_application
    ),
    components(schemas(
        NormalizationProposalSubmission,
        NormalizationRequest,
        NormalizationTargetKindContract,
        NormalizationProposal,
        ProposalValidation,
        TraceContext,
        SubmissionMetadata,
        NormalizationReviewRequest,
        NormalizationReviewResult,
        NormalizationApplyRequest,
        NormalizationApplyResult,
        NormalizationRollbackRequest,
        NormalizationRollbackResult,
        FoundationSubmissionResult,
        ApiErrorResponse,
        InternalApiErrorResponse,
        IntakeError
    )),
    modifiers(&NormalizationSecurity),
    tags((name = "normalization", description = "Human-gated normalization proposal governance"))
)]
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the generated contract is consumed by offline contract checks, not a runtime docs endpoint"
    )
)]
pub(super) struct NormalizationApiDoc;

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the OpenAPI derive macro consumes this modifier during offline contract generation"
    )
)]
struct NormalizationSecurity;

impl Modify for NormalizationSecurity {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_default();
        components.add_security_scheme(
            "normalization_service_bearer",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .description(Some("Intelligence Platform service identity"))
                    .build(),
            ),
        );
        components.add_security_scheme(
            "normalization_staff_bearer",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .description(Some("Authorized Foundation Platform staff identity"))
                    .build(),
            ),
        );
    }
}

pub(super) fn routes(state: &Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/internal/normalization/proposals",
            super::protected_route(
                post(submit_proposal),
                state,
                super::SERVICE_NORMALIZATION_PROPOSE,
                None,
            ),
        )
        .route(
            "/catalog/v1/normalization/proposals/{id}/approve",
            super::protected_route(
                post(approve_proposal),
                state,
                super::STAFF_CATALOG_WRITE,
                Some("id"),
            ),
        )
        .route(
            "/catalog/v1/normalization/proposals/{id}/reject",
            super::protected_route(
                post(reject_proposal),
                state,
                super::STAFF_CATALOG_WRITE,
                Some("id"),
            ),
        )
        .route(
            "/catalog/v1/normalization/proposals/{id}/apply",
            super::protected_route(
                post(apply_proposal),
                state,
                super::STAFF_CATALOG_WRITE,
                Some("id"),
            ),
        )
        .route(
            "/catalog/v1/normalization/applications/{id}/rollback",
            super::protected_route(
                post(rollback_application),
                state,
                super::STAFF_CATALOG_WRITE,
                Some("id"),
            ),
        )
}

#[utoipa::path(
    post,
    path = "/internal/normalization/proposals",
    request_body = NormalizationProposalSubmission,
    responses(
        (status = 202, description = "Proposal persisted for staff review", body = FoundationSubmissionResult),
        (status = 401, description = "Service identity credential is missing or invalid"),
        (status = 403, description = "Authenticated principal is not an authorized service"),
        (status = 422, description = "Proposal envelope or review gate is invalid", body = IntakeError),
        (status = 500, description = "Proposal persistence failed", body = IntakeError),
        (status = 503, description = "Identity authorization is unavailable")
    ),
    security(("normalization_service_bearer" = [])),
    tag = "normalization"
)]
async fn submit_proposal(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthorizedPrincipal>,
    Json(body): Json<NormalizationProposalSubmission>,
) -> Result<(StatusCode, Json<FoundationSubmissionResult>), (StatusCode, Json<IntakeError>)> {
    validate_submission(&body).map_err(|error| (StatusCode::UNPROCESSABLE_ENTITY, Json(error)))?;
    let command = submission_into_command(body, PrincipalId::new(principal.principal_id));
    let record = state
        .submit_normalization_proposal
        .execute(command)
        .await
        .map_err(normalization_intake_error)?;

    let mut metadata = BTreeMap::new();
    metadata.insert("storage".to_owned(), "proposal_inbox".to_owned());
    metadata.insert("mode".to_owned(), "durable_review_gate".to_owned());
    metadata.insert("proposal_key".to_owned(), record.proposal_key);

    Ok((
        StatusCode::ACCEPTED,
        Json(FoundationSubmissionResult {
            submission_id: record.id.to_string(),
            status: "queued".to_owned(),
            review_required: true,
            platform: NORMALIZATION_PLATFORM_WIRE_NAME.to_owned(),
            metadata,
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/catalog/v1/normalization/proposals/{id}/approve",
    request_body = NormalizationReviewRequest,
    params(("id" = Uuid, Path, description = "Normalization proposal id")),
    responses(
        (status = 200, description = "Proposal approved", body = NormalizationReviewResult),
        (status = 400, description = "Proposal or lifecycle state is invalid", body = ApiErrorResponse),
        (status = 401, description = "Staff identity credential is missing or invalid"),
        (status = 403, description = "Authenticated principal cannot review normalization proposals"),
        (status = 500, description = "Internal persistence failure", body = InternalApiErrorResponse),
        (status = 503, description = "Identity authorization is unavailable")
    ),
    security(("normalization_staff_bearer" = [])),
    tag = "normalization"
)]
async fn approve_proposal(
    State(state): State<Arc<AppState>>,
    Path(proposal_id): Path<Uuid>,
    Extension(principal): Extension<AuthorizedPrincipal>,
    Json(body): Json<NormalizationReviewRequest>,
) -> Result<Json<NormalizationReviewResult>, ApiError> {
    review_proposal(
        state,
        proposal_id,
        principal,
        body,
        NormalizationReviewDecision::Approved,
    )
    .await
}

#[utoipa::path(
    post,
    path = "/catalog/v1/normalization/proposals/{id}/reject",
    request_body = NormalizationReviewRequest,
    params(("id" = Uuid, Path, description = "Normalization proposal id")),
    responses(
        (status = 200, description = "Proposal rejected", body = NormalizationReviewResult),
        (status = 400, description = "Proposal or lifecycle state is invalid", body = ApiErrorResponse),
        (status = 401, description = "Staff identity credential is missing or invalid"),
        (status = 403, description = "Authenticated principal cannot review normalization proposals"),
        (status = 500, description = "Internal persistence failure", body = InternalApiErrorResponse),
        (status = 503, description = "Identity authorization is unavailable")
    ),
    security(("normalization_staff_bearer" = [])),
    tag = "normalization"
)]
async fn reject_proposal(
    State(state): State<Arc<AppState>>,
    Path(proposal_id): Path<Uuid>,
    Extension(principal): Extension<AuthorizedPrincipal>,
    Json(body): Json<NormalizationReviewRequest>,
) -> Result<Json<NormalizationReviewResult>, ApiError> {
    review_proposal(
        state,
        proposal_id,
        principal,
        body,
        NormalizationReviewDecision::Rejected,
    )
    .await
}

async fn review_proposal(
    state: Arc<AppState>,
    proposal_id: Uuid,
    principal: AuthorizedPrincipal,
    body: NormalizationReviewRequest,
    decision: NormalizationReviewDecision,
) -> Result<Json<NormalizationReviewResult>, ApiError> {
    let record = state
        .review_normalization_proposal
        .execute(NormalizationProposalReviewCommand {
            id: Uuid::now_v7(),
            proposal_id,
            reviewer_principal_id: PrincipalId::new(principal.principal_id),
            decision,
            reason: body.reason,
        })
        .await
        .map_err(normalization_api_error)?;

    Ok(Json(NormalizationReviewResult {
        proposal_id: record.id.to_string(),
        proposal_key: record.proposal_key,
        status: record.status.wire_name().to_owned(),
        decision: decision.wire_name().to_owned(),
    }))
}

#[utoipa::path(
    post,
    path = "/catalog/v1/normalization/proposals/{id}/apply",
    request_body = NormalizationApplyRequest,
    params(("id" = Uuid, Path, description = "Approved normalization proposal id")),
    responses(
        (status = 200, description = "Proposal applied atomically", body = NormalizationApplyResult),
        (status = 400, description = "Proposal or lifecycle state is invalid", body = ApiErrorResponse),
        (status = 401, description = "Staff identity credential is missing or invalid"),
        (status = 403, description = "Authenticated principal cannot apply normalization proposals"),
        (status = 404, description = "Canonical target does not exist", body = ApiErrorResponse),
        (status = 409, description = "Canonical target version or state conflicts", body = ApiErrorResponse),
        (status = 500, description = "Internal persistence failure", body = InternalApiErrorResponse),
        (status = 503, description = "Identity authorization is unavailable")
    ),
    security(("normalization_staff_bearer" = [])),
    tag = "normalization"
)]
async fn apply_proposal(
    State(state): State<Arc<AppState>>,
    Path(proposal_id): Path<Uuid>,
    Extension(principal): Extension<AuthorizedPrincipal>,
    Json(body): Json<NormalizationApplyRequest>,
) -> Result<Json<NormalizationApplyResult>, ApiError> {
    let record = state
        .apply_normalization_proposal
        .execute(NormalizationApplicationCommand {
            id: Uuid::now_v7(),
            proposal_id,
            expected_version: body.expected_version,
            applied_by_principal_id: PrincipalId::new(principal.principal_id),
        })
        .await
        .map_err(normalization_api_error)?;

    Ok(Json(NormalizationApplyResult {
        application_id: record.id.to_string(),
        proposal_id: record.proposal_id.to_string(),
        target_kind: record.target_kind.wire_name().to_owned(),
        target_id: record.target_id.map(|id| id.to_string()),
    }))
}

#[utoipa::path(
    post,
    path = "/catalog/v1/normalization/applications/{id}/rollback",
    request_body = NormalizationRollbackRequest,
    params(("id" = Uuid, Path, description = "Normalization application id")),
    responses(
        (status = 200, description = "Application compensated atomically", body = NormalizationRollbackResult),
        (status = 400, description = "Application or lifecycle state is invalid", body = ApiErrorResponse),
        (status = 401, description = "Staff identity credential is missing or invalid"),
        (status = 403, description = "Authenticated principal cannot rollback normalization applications"),
        (status = 404, description = "Canonical target does not exist", body = ApiErrorResponse),
        (status = 409, description = "Canonical target version or state conflicts", body = ApiErrorResponse),
        (status = 500, description = "Internal persistence failure", body = InternalApiErrorResponse),
        (status = 503, description = "Identity authorization is unavailable")
    ),
    security(("normalization_staff_bearer" = [])),
    tag = "normalization"
)]
async fn rollback_application(
    State(state): State<Arc<AppState>>,
    Path(application_id): Path<Uuid>,
    Extension(principal): Extension<AuthorizedPrincipal>,
    Json(body): Json<NormalizationRollbackRequest>,
) -> Result<Json<NormalizationRollbackResult>, ApiError> {
    let record = state
        .rollback_normalization_application
        .execute(NormalizationRollbackCommand {
            id: Uuid::now_v7(),
            application_id,
            expected_current_version: body.expected_current_version,
            reason: body.reason,
            rolled_back_by_principal_id: PrincipalId::new(principal.principal_id),
        })
        .await
        .map_err(normalization_api_error)?;

    Ok(Json(NormalizationRollbackResult {
        application_id: record.id.to_string(),
        rollback_of: record.rollback_of.to_string(),
        target_kind: record.target_kind.wire_name().to_owned(),
        target_id: record.target_id.map(|id| id.to_string()),
    }))
}

fn validate_submission(body: &NormalizationProposalSubmission) -> Result<(), IntakeError> {
    if body.commit_allowed {
        return Err(IntakeError::new(
            "direct_commit_forbidden",
            "normalization proposals must not request direct canonical writes",
        ));
    }
    if !body.requires_human_review {
        return Err(IntakeError::new(
            "human_review_required",
            "normalization proposals must require human review",
        ));
    }
    if !body.validation.accepted {
        return Err(IntakeError::new(
            "validation_not_accepted",
            "normalization proposal validation must be accepted before intake",
        ));
    }
    if body.proposal.raw_record_id != body.request.raw_record_id {
        return Err(IntakeError::new(
            "raw_record_mismatch",
            "proposal raw_record_id must match the originating request",
        ));
    }
    if body.proposal.schema_version != body.request.target_schema_version {
        return Err(IntakeError::new(
            "schema_version_mismatch",
            "proposal schema_version must match the requested target_schema_version",
        ));
    }

    Ok(())
}

fn submission_into_command(
    submission: NormalizationProposalSubmission,
    submitted_by_principal_id: PrincipalId,
) -> NormalizationProposalSubmissionCommand {
    let target_kind = match submission.request.target_kind {
        NormalizationTargetKindContract::IndustrialComplex => {
            NormalizationTargetKind::IndustrialComplex
        }
        NormalizationTargetKindContract::BuildingRegisterFloor => {
            NormalizationTargetKind::BuildingRegisterFloor
        }
        NormalizationTargetKindContract::BuildingRegisterUnit => {
            NormalizationTargetKind::BuildingRegisterUnit
        }
    };
    NormalizationProposalSubmissionCommand {
        id: Uuid::now_v7(),
        proposal_key: String::new(),
        submitted_by_service: "intelligence-platform".to_owned(),
        submitted_by_principal_id,
        source_system: submission.request.source_system,
        raw_record_id: submission.request.raw_record_id,
        raw_object_key: submission.request.raw_object_key,
        raw_checksum_sha256: submission.request.raw_checksum_sha256,
        bronze_object_id: submission.request.bronze_object_id,
        target_kind,
        target_identity: submission.request.target_identity,
        target_schema_version: submission.request.target_schema_version,
        proposal_schema_version: submission.proposal.schema_version,
        proposed_record: submission.proposal.record,
        proposed_record_sha256: String::new(),
        proposed_patch: submission.proposal.patch,
        confidence: submission.proposal.confidence,
        evidence: submission.proposal.evidence,
        validation: serde_json::json!({
            "accepted": submission.validation.accepted,
            "issues": submission.validation.issues,
        }),
        model_profile_id: submission.submission_metadata.model_profile_id,
        model_id: submission.submission_metadata.model_id,
        prompt_id: submission.submission_metadata.prompt_id,
        prompt_version: submission.submission_metadata.prompt_version,
        policy_id: submission.submission_metadata.policy_id,
        policy_version: submission.submission_metadata.policy_version,
        trace_id: submission.trace_context.trace_id,
        status: NormalizationProposalStatus::PendingReview,
    }
}

fn normalization_intake_error(error: NormalizationError) -> (StatusCode, Json<IntakeError>) {
    match error {
        NormalizationError::InvalidInput(message) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(IntakeError::new("invalid_normalization_proposal", message)),
        ),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(IntakeError::new(
                "normalization_proposal_persistence_failed",
                "normalization proposal persistence failed",
            )),
        ),
    }
}

fn normalization_api_error(error: NormalizationError) -> ApiError {
    match error {
        NormalizationError::InvalidInput(message) | NormalizationError::InvalidState(message) => {
            ApiError::BadRequest(message)
        }
        NormalizationError::ProposalNotFound | NormalizationError::ApplicationNotFound => {
            ApiError::BadRequest(error.to_string())
        }
        NormalizationError::TargetNotFound(id) => ApiError::NotFound(id),
        NormalizationError::TargetVersionConflict { .. } => {
            ApiError::Conflict("version mismatch".to_owned())
        }
        NormalizationError::TargetStateConflict(id) | NormalizationError::TargetArchived(id) => {
            ApiError::Conflict(id)
        }
        NormalizationError::Persistence(detail) => ApiError::Internal(detail),
    }
}
