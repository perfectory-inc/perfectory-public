//! Staff routes for deciding review items (root ADR-0115): parcel lineage, and the pairs of
//! abolished 법정동 codes as a second item kind on the same routes (root ADR-0263).
//!
//! Every route is staff-only and authorized by the Identity Platform: reading, claiming and deciding
//! need `foundation.lineage:review`; ruling on a decision that needs a second person needs
//! `foundation.lineage:adjudicate`. The rules themselves live in `stewardship-domain`; this module
//! only translates HTTP to the store and back.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    routing::{get, post},
    Extension, Json, Router,
};
use chrono::Utc;
use foundation_contracts::error::{ApiErrorResponse, InternalApiErrorResponse};
use foundation_contracts::lineage_review::{
    LineageApprovalRequest, LineageApprovalVerdict, LineageDecisionDisposition,
    LineageDecisionOutcome, LineageDecisionRequest, LineageDecisionResponse,
    LineageLegalDongCandidate, LineageLegalDongEvidence, LineageReasonCode, LineageReviewCandidate,
    LineageReviewClaim, LineageReviewItem, LineageReviewItemPage, LineageReviewKind,
    LineageReviewStatus, LineageStewardDecision,
};
use serde::Deserialize;
use stewardship_application::{
    DecideCommand, DecideOutcome, DecisionRecord, ReviewItemFilter, ReviewItemView,
};
use stewardship_domain::{
    Candidate, DecisionDraft, ItemKind, LegalDongEvidence, Outcome, ReasonCode, ReviewStatus,
    StewardshipError,
};
use utoipa::{
    openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
    IntoParams, Modify, OpenApi,
};
use uuid::Uuid;

use super::api_error::ApiError;
use crate::identity_authorization::AuthorizedPrincipal;
use crate::state::AppState;

const PAGE_LIMIT_MAX: u32 = 200;
const PAGE_LIMIT_DEFAULT: u32 = 50;

#[derive(OpenApi)]
#[openapi(
    paths(list_items, get_item, claim_item, release_item, decide_item, rule_on_decision),
    components(schemas(
        LineageReviewItemPage,
        LineageReviewItem,
        LineageReviewKind,
        LineageReviewCandidate,
        LineageLegalDongEvidence,
        LineageLegalDongCandidate,
        LineageReviewClaim,
        LineageReviewStatus,
        LineageStewardDecision,
        LineageDecisionOutcome,
        LineageReasonCode,
        LineageDecisionRequest,
        LineageDecisionResponse,
        LineageDecisionDisposition,
        LineageApprovalRequest,
        LineageApprovalVerdict,
        ApiErrorResponse,
        InternalApiErrorResponse
    )),
    modifiers(&LineageReviewSecurity),
    tags((name = "lineage-review", description = "Staff decisions on parcel lineage and 법정동 code pairs the evidence could not settle"))
)]
pub(super) struct LineageReviewApiDoc;

/// Returns the deterministic `OpenAPI` model of the steward API; the staff console generates its
/// types from the committed copy (`docs/openapi/lineage-review.v1.json`).
#[must_use]
pub fn lineage_review_openapi_document() -> utoipa::openapi::OpenApi {
    LineageReviewApiDoc::openapi()
}

struct LineageReviewSecurity;

impl Modify for LineageReviewSecurity {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        openapi
            .components
            .get_or_insert_default()
            .add_security_scheme(
                "lineage_staff_bearer",
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
    let review = super::STAFF_LINEAGE_REVIEW;
    Router::new()
        .route(
            "/catalog/v1/lineage-review/items",
            super::protected_route(get(list_items), state, review, None),
        )
        .route(
            "/catalog/v1/lineage-review/items/{id}",
            super::protected_route(get(get_item), state, review, Some("id")),
        )
        .route(
            "/catalog/v1/lineage-review/items/{id}/claim",
            super::protected_route(post(claim_item), state, review, Some("id")),
        )
        .route(
            "/catalog/v1/lineage-review/items/{id}/release",
            super::protected_route(post(release_item), state, review, Some("id")),
        )
        .route(
            "/catalog/v1/lineage-review/items/{id}/decisions",
            super::protected_route(post(decide_item), state, review, Some("id")),
        )
        .route(
            "/catalog/v1/lineage-review/decisions/{id}/approval",
            super::protected_route(
                post(rule_on_decision),
                state,
                super::STAFF_LINEAGE_ADJUDICATE,
                Some("id"),
            ),
        )
}

/// Which review items to list.
#[derive(Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct ListQuery {
    /// Only this kind.
    kind: Option<LineageReviewKind>,
    /// Only this status.
    status: Option<LineageReviewStatus>,
    /// Only subjects whose code starts with these digits (a sido or sigungu code).
    prefix: Option<String>,
    /// Only items without a standing decision on their current evidence.
    #[serde(default)]
    undecided_only: bool,
    /// The `next_after` of the previous page.
    after: Option<String>,
    /// Page size, at most 200.
    limit: Option<u32>,
}

/// Decision options.
#[derive(Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct DecideQuery {
    /// Run every check and store nothing.
    #[serde(default)]
    dry_run: bool,
}

#[utoipa::path(
    get,
    path = "/catalog/v1/lineage-review/items",
    params(ListQuery),
    responses(
        (status = 200, description = "Review items in subject-code order", body = LineageReviewItemPage),
        (status = 400, description = "A filter is malformed", body = ApiErrorResponse),
        (status = 401, description = "Staff identity credential is missing or invalid"),
        (status = 403, description = "Principal may not review lineage"),
        (status = 500, description = "Internal persistence failure", body = InternalApiErrorResponse),
        (status = 503, description = "Identity authorization is unavailable")
    ),
    security(("lineage_staff_bearer" = [])),
    tag = "lineage-review"
)]
async fn list_items(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListQuery>,
) -> Result<Json<LineageReviewItemPage>, ApiError> {
    if let Some(prefix) = query.prefix.as_deref() {
        if prefix.is_empty() || prefix.len() > 19 || !prefix.bytes().all(|b| b.is_ascii_digit()) {
            return Err(ApiError::BadRequest("prefix is 1-19 digits".to_owned()));
        }
    }
    if let Some(after) = query.after.as_deref() {
        check_code(after, "after")?;
    }
    let limit = query.limit.unwrap_or(PAGE_LIMIT_DEFAULT);
    if limit == 0 || limit > PAGE_LIMIT_MAX {
        return Err(ApiError::BadRequest(format!("limit is 1-{PAGE_LIMIT_MAX}")));
    }
    let views = state
        .lineage_stewardship
        .list_items(
            ReviewItemFilter {
                kind: query.kind.map(domain_kind),
                status: query.status.map(domain_status),
                code_prefix: query.prefix,
                undecided_only: query.undecided_only,
                after_subject_code: query.after,
                limit,
            },
            Utc::now(),
        )
        .await
        .map_err(stewardship_api_error)?;
    let full_page = views.len() == limit as usize;
    let items = views
        .into_iter()
        .map(item_response)
        .collect::<Result<Vec<_>, _>>()?;
    let next_after = if full_page {
        items.last().map(|item| item.subject_code.clone())
    } else {
        None
    };
    Ok(Json(LineageReviewItemPage { items, next_after }))
}

#[utoipa::path(
    get,
    path = "/catalog/v1/lineage-review/items/{id}",
    params(("id" = Uuid, Path, description = "Review item id")),
    responses(
        (status = 200, description = "The item with its candidates, claim and decisions", body = LineageReviewItem),
        (status = 401, description = "Staff identity credential is missing or invalid"),
        (status = 403, description = "Principal may not review lineage"),
        (status = 404, description = "No such review item", body = ApiErrorResponse),
        (status = 500, description = "Internal persistence failure", body = InternalApiErrorResponse),
        (status = 503, description = "Identity authorization is unavailable")
    ),
    security(("lineage_staff_bearer" = [])),
    tag = "lineage-review"
)]
async fn get_item(
    State(state): State<Arc<AppState>>,
    Path(item_id): Path<Uuid>,
) -> Result<Json<LineageReviewItem>, ApiError> {
    let view = state
        .lineage_stewardship
        .get_item(item_id, Utc::now())
        .await
        .map_err(stewardship_api_error)?;
    Ok(Json(item_response(view)?))
}

#[utoipa::path(
    post,
    path = "/catalog/v1/lineage-review/items/{id}/claim",
    params(("id" = Uuid, Path, description = "Review item id")),
    responses(
        (status = 200, description = "Claimed or renewed for 30 minutes", body = LineageReviewClaim),
        (status = 401, description = "Staff identity credential is missing or invalid"),
        (status = 403, description = "Principal may not review lineage"),
        (status = 404, description = "No such review item", body = ApiErrorResponse),
        (status = 409, description = "Another steward holds the item", body = ApiErrorResponse),
        (status = 500, description = "Internal persistence failure", body = InternalApiErrorResponse),
        (status = 503, description = "Identity authorization is unavailable")
    ),
    security(("lineage_staff_bearer" = [])),
    tag = "lineage-review"
)]
async fn claim_item(
    State(state): State<Arc<AppState>>,
    Path(item_id): Path<Uuid>,
    Extension(principal): Extension<AuthorizedPrincipal>,
) -> Result<Json<LineageReviewClaim>, ApiError> {
    let claim = state
        .lineage_stewardship
        .claim(item_id, principal.principal_id, Utc::now())
        .await
        .map_err(stewardship_api_error)?;
    Ok(Json(LineageReviewClaim {
        claimed_by: claim.claimed_by.to_string(),
        expires_at: claim.expires_at,
    }))
}

#[utoipa::path(
    post,
    path = "/catalog/v1/lineage-review/items/{id}/release",
    params(("id" = Uuid, Path, description = "Review item id")),
    responses(
        (status = 204, description = "The caller's claim, if any, is released"),
        (status = 401, description = "Staff identity credential is missing or invalid"),
        (status = 403, description = "Principal may not review lineage"),
        (status = 500, description = "Internal persistence failure", body = InternalApiErrorResponse),
        (status = 503, description = "Identity authorization is unavailable")
    ),
    security(("lineage_staff_bearer" = [])),
    tag = "lineage-review"
)]
async fn release_item(
    State(state): State<Arc<AppState>>,
    Path(item_id): Path<Uuid>,
    Extension(principal): Extension<AuthorizedPrincipal>,
) -> Result<axum::http::StatusCode, ApiError> {
    state
        .lineage_stewardship
        .release(item_id, principal.principal_id)
        .await
        .map_err(stewardship_api_error)?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/catalog/v1/lineage-review/items/{id}/decisions",
    request_body = LineageDecisionRequest,
    params(
        ("id" = Uuid, Path, description = "Review item id"),
        ("Idempotency-Key" = Option<String>, Header, description = "Required unless dry_run: 8-200 printable ASCII characters"),
        DecideQuery
    ),
    responses(
        (status = 200, description = "Recorded, replayed, or dry run passed", body = LineageDecisionResponse),
        (status = 400, description = "The decision is malformed, names no candidate, or a 법정동 decision says no why", body = ApiErrorResponse),
        (status = 401, description = "Staff identity credential is missing or invalid"),
        (status = 403, description = "Principal may not review lineage"),
        (status = 404, description = "No such review item", body = ApiErrorResponse),
        (status = 409, description = "Evidence changed, another steward holds the item, the key was reused with another body, the predecessor is taken, or the 법정동 item is the data's to decide (split, awaiting data)", body = ApiErrorResponse),
        (status = 500, description = "Internal persistence failure", body = InternalApiErrorResponse),
        (status = 503, description = "Identity authorization is unavailable")
    ),
    security(("lineage_staff_bearer" = [])),
    tag = "lineage-review"
)]
async fn decide_item(
    State(state): State<Arc<AppState>>,
    Path(item_id): Path<Uuid>,
    Query(query): Query<DecideQuery>,
    headers: HeaderMap,
    Extension(principal): Extension<AuthorizedPrincipal>,
    Json(body): Json<LineageDecisionRequest>,
) -> Result<Json<LineageDecisionResponse>, ApiError> {
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    if idempotency_key.is_empty() && !query.dry_run {
        return Err(ApiError::BadRequest(
            "Idempotency-Key header is required".to_owned(),
        ));
    }
    if let Some(code) = body.candidate_code.as_deref() {
        check_code(code, "candidate_code")?;
    }
    let supersedes_decision_id = body
        .supersedes_decision_id
        .as_deref()
        .map(Uuid::parse_str)
        .transpose()
        .map_err(|_| ApiError::BadRequest("supersedes_decision_id is not a UUID".to_owned()))?;
    let outcome = state
        .lineage_stewardship
        .decide(DecideCommand {
            item_id,
            draft: DecisionDraft {
                outcome: domain_outcome(body.outcome),
                candidate_code: body.candidate_code,
                reason_code: body.reason_code.map(domain_reason),
                note: body.note,
                evidence_etag: body.evidence_etag,
                supersedes_decision_id,
            },
            decider: principal.principal_id,
            idempotency_key,
            dry_run: query.dry_run,
            now: Utc::now(),
        })
        .await
        .map_err(stewardship_api_error)?;
    Ok(Json(match outcome {
        DecideOutcome::Recorded(record) => {
            decision_response(LineageDecisionDisposition::Recorded, record)
        }
        DecideOutcome::Replayed(record) => {
            decision_response(LineageDecisionDisposition::Replayed, record)
        }
        DecideOutcome::WouldRecord { requires_approval } => LineageDecisionResponse {
            disposition: LineageDecisionDisposition::DryRunPassed,
            requires_approval,
            decision: None,
        },
    }))
}

#[utoipa::path(
    post,
    path = "/catalog/v1/lineage-review/decisions/{id}/approval",
    request_body = LineageApprovalRequest,
    params(("id" = Uuid, Path, description = "Decision id")),
    responses(
        (status = 200, description = "The ruling is recorded", body = LineageStewardDecision),
        (status = 400, description = "The decision needs no ruling or already has one", body = ApiErrorResponse),
        (status = 401, description = "Staff identity credential is missing or invalid"),
        (status = 403, description = "Principal may not adjudicate, or is the decider", body = ApiErrorResponse),
        (status = 500, description = "Internal persistence failure", body = InternalApiErrorResponse),
        (status = 503, description = "Identity authorization is unavailable")
    ),
    security(("lineage_staff_bearer" = [])),
    tag = "lineage-review"
)]
async fn rule_on_decision(
    State(state): State<Arc<AppState>>,
    Path(decision_id): Path<Uuid>,
    Extension(principal): Extension<AuthorizedPrincipal>,
    Json(body): Json<LineageApprovalRequest>,
) -> Result<Json<LineageStewardDecision>, ApiError> {
    let record = state
        .lineage_stewardship
        .approve(
            decision_id,
            principal.principal_id,
            body.verdict == LineageApprovalVerdict::Approved,
            body.note,
        )
        .await
        .map_err(stewardship_api_error)?;
    Ok(Json(decision_view(record)))
}

/// A subject or candidate code of either kind: a 19-digit PNU or a 10-digit 법정동 code. The store
/// checks the item's own kind.
fn check_code(value: &str, field: &str) -> Result<(), ApiError> {
    if ItemKind::Parcel.is_code(value) || ItemKind::LegalDong.is_code(value) {
        Ok(())
    } else {
        Err(ApiError::BadRequest(format!(
            "{field} is a 19-digit PNU or a 10-digit 법정동 code"
        )))
    }
}

fn item_response(view: ReviewItemView) -> Result<LineageReviewItem, ApiError> {
    let candidates = view.item.candidates().map_err(stewardship_api_error)?;
    let legal_dong = legal_dong_response(&view.item).map_err(stewardship_api_error)?;
    Ok(LineageReviewItem {
        item_id: view.item.item_id.to_string(),
        kind: contract_kind(view.item.kind),
        evidence_etag: view.item.evidence_etag(),
        subject_code: view.item.subject_code,
        status: contract_status(view.item.status),
        candidates: candidates.into_iter().map(candidate_response).collect(),
        legal_dong,
        claim: view.claim.map(|claim| LineageReviewClaim {
            claimed_by: claim.claimed_by.to_string(),
            expires_at: claim.expires_at,
        }),
        decisions: view.decisions.into_iter().map(decision_view).collect(),
    })
}

fn legal_dong_response(
    item: &stewardship_domain::ReviewItem,
) -> Result<Option<LineageLegalDongEvidence>, StewardshipError> {
    let Some(text) = item.evidence_json.as_deref() else {
        return Ok(None);
    };
    let evidence: LegalDongEvidence = serde_json::from_str(text)
        .map_err(|error| StewardshipError::InvalidItem(format!("evidence_json: {error}")))?;
    Ok(Some(LineageLegalDongEvidence {
        name: evidence.name,
        level: evidence.level,
        abolished_date: evidence.abolished_date,
        rule: evidence.rule,
        jibun: evidence.jibun,
        split_into: evidence.split_into,
        waiting_for: evidence.waiting_for,
        official_history: evidence.official_history,
        official_partial_moves: evidence.official_partial_moves,
        candidates: item
            .legal_dong_candidates()?
            .into_iter()
            .map(|candidate| LineageLegalDongCandidate {
                code: candidate.code,
                name: candidate.name,
                created_date: candidate.created_date,
            })
            .collect(),
    }))
}

fn candidate_response(candidate: Candidate) -> LineageReviewCandidate {
    LineageReviewCandidate {
        predecessor_pnu: candidate.predecessor_pnu,
        relation: candidate.relation,
        grade: candidate.grade,
        evidence_kind: candidate.evidence_kind,
        evidence_ref: candidate.evidence_ref,
        in_effect: candidate.in_effect,
    }
}

fn decision_response(
    disposition: LineageDecisionDisposition,
    record: DecisionRecord,
) -> LineageDecisionResponse {
    LineageDecisionResponse {
        disposition,
        requires_approval: record.requires_approval,
        decision: Some(decision_view(record)),
    }
}

fn decision_view(record: DecisionRecord) -> LineageStewardDecision {
    LineageStewardDecision {
        decision_id: record.decision_id.to_string(),
        outcome: contract_outcome(record.outcome),
        candidate_code: record.candidate_code,
        reason_code: record.reason_code.map(contract_reason),
        note: record.note,
        decided_by: record.decided_by.to_string(),
        decided_at: record.decided_at,
        requires_approval: record.requires_approval,
        approval: record.approval,
        supersedes_decision_id: record.supersedes_decision_id.map(|id| id.to_string()),
    }
}

const fn domain_kind(kind: LineageReviewKind) -> ItemKind {
    match kind {
        LineageReviewKind::Parcel => ItemKind::Parcel,
        LineageReviewKind::LegalDong => ItemKind::LegalDong,
    }
}

const fn contract_kind(kind: ItemKind) -> LineageReviewKind {
    match kind {
        ItemKind::Parcel => LineageReviewKind::Parcel,
        ItemKind::LegalDong => LineageReviewKind::LegalDong,
    }
}

const fn domain_status(status: LineageReviewStatus) -> ReviewStatus {
    match status {
        LineageReviewStatus::NeedsReview => ReviewStatus::NeedsReview,
        LineageReviewStatus::Pending => ReviewStatus::Pending,
        LineageReviewStatus::Sample => ReviewStatus::Sample,
        LineageReviewStatus::Steward => ReviewStatus::Steward,
        LineageReviewStatus::Split => ReviewStatus::Split,
        LineageReviewStatus::AwaitingData => ReviewStatus::AwaitingData,
    }
}

const fn contract_status(status: ReviewStatus) -> LineageReviewStatus {
    match status {
        ReviewStatus::NeedsReview => LineageReviewStatus::NeedsReview,
        ReviewStatus::Pending => LineageReviewStatus::Pending,
        ReviewStatus::Sample => LineageReviewStatus::Sample,
        ReviewStatus::Steward => LineageReviewStatus::Steward,
        ReviewStatus::Split => LineageReviewStatus::Split,
        ReviewStatus::AwaitingData => LineageReviewStatus::AwaitingData,
    }
}

const fn domain_outcome(outcome: LineageDecisionOutcome) -> Outcome {
    match outcome {
        LineageDecisionOutcome::Link => Outcome::Link,
        LineageDecisionOutcome::NotALink => Outcome::NotALink,
        LineageDecisionOutcome::Unsure => Outcome::Unsure,
        LineageDecisionOutcome::Escalate => Outcome::Escalate,
    }
}

const fn contract_outcome(outcome: Outcome) -> LineageDecisionOutcome {
    match outcome {
        Outcome::Link => LineageDecisionOutcome::Link,
        Outcome::NotALink => LineageDecisionOutcome::NotALink,
        Outcome::Unsure => LineageDecisionOutcome::Unsure,
        Outcome::Escalate => LineageDecisionOutcome::Escalate,
    }
}

const fn domain_reason(reason: LineageReasonCode) -> ReasonCode {
    match reason {
        LineageReasonCode::BuildingRegister => ReasonCode::BuildingRegister,
        LineageReasonCode::OwnershipRecord => ReasonCode::OwnershipRecord,
        LineageReasonCode::SiteSurvey => ReasonCode::SiteSurvey,
        LineageReasonCode::OfficialDocument => ReasonCode::OfficialDocument,
        LineageReasonCode::CadastralMap => ReasonCode::CadastralMap,
        LineageReasonCode::Other => ReasonCode::Other,
    }
}

const fn contract_reason(reason: ReasonCode) -> LineageReasonCode {
    match reason {
        ReasonCode::BuildingRegister => LineageReasonCode::BuildingRegister,
        ReasonCode::OwnershipRecord => LineageReasonCode::OwnershipRecord,
        ReasonCode::SiteSurvey => LineageReasonCode::SiteSurvey,
        ReasonCode::OfficialDocument => LineageReasonCode::OfficialDocument,
        ReasonCode::CadastralMap => LineageReasonCode::CadastralMap,
        ReasonCode::Other => LineageReasonCode::Other,
    }
}

fn stewardship_api_error(error: StewardshipError) -> ApiError {
    match error {
        StewardshipError::InvalidInput(message) | StewardshipError::InvalidState(message) => {
            ApiError::BadRequest(message)
        }
        StewardshipError::NotACandidate(_) => ApiError::BadRequest(error.to_string()),
        StewardshipError::ItemNotFound => ApiError::NotFound(error.to_string()),
        StewardshipError::StaleEvidence { .. }
        | StewardshipError::ClaimedByAnother { .. }
        | StewardshipError::IdempotencyConflict
        | StewardshipError::PredecessorTaken { .. }
        | StewardshipError::DataDecides { .. } => ApiError::Conflict(error.to_string()),
        StewardshipError::SelfApproval => ApiError::Forbidden(error.to_string()),
        StewardshipError::InvalidItem(detail) | StewardshipError::Persistence(detail) => {
            ApiError::Internal(detail)
        }
    }
}
