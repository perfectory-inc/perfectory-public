//! Normalization proposal, review, application, and rollback HTTP DTOs.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use utoipa::ToSchema;
use uuid::Uuid;

/// Normalization target selected by an Intelligence Platform proposal.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum NormalizationTargetKind {
    /// Canonical industrial-complex facts.
    IndustrialComplex,
    /// Silver building-register floor facts.
    BuildingRegisterFloor,
    /// Silver building-register exclusive-unit facts.
    BuildingRegisterUnit,
}

/// Complete service-to-service normalization proposal submission.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct NormalizationProposalSubmission {
    /// Raw-source identity and canonical target request.
    pub request: NormalizationRequest,
    /// Proposed normalized record and evidence.
    pub proposal: NormalizationProposal,
    /// Intelligence-side validation outcome.
    pub validation: ProposalValidation,
    /// Distributed trace context.
    pub trace_context: TraceContext,
    /// Must remain false; Intelligence Platform cannot write canonical state.
    pub commit_allowed: bool,
    /// Must remain true; a staff review is required before application.
    pub requires_human_review: bool,
    /// Model, prompt, and policy provenance.
    pub submission_metadata: SubmissionMetadata,
}

/// Raw-source identity and desired canonical target for a proposal.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct NormalizationRequest {
    /// Stable source-system identifier.
    pub source_system: String,
    /// Stable source record identifier.
    pub raw_record_id: String,
    /// Optional immutable object-store key for raw evidence.
    #[serde(default)]
    pub raw_object_key: Option<String>,
    /// Optional SHA-256 checksum for raw evidence.
    #[serde(default)]
    pub raw_checksum_sha256: Option<String>,
    /// Optional Foundation Bronze catalog row identifier.
    #[serde(default)]
    pub bronze_object_id: Option<Uuid>,
    /// Canonical fact family targeted by the proposal.
    pub target_kind: NormalizationTargetKind,
    /// Target-specific identity object.
    #[schema(value_type = Object)]
    pub target_identity: JsonValue,
    /// Expected canonical target schema version.
    pub target_schema_version: String,
}

/// Proposed normalized record, confidence, and evidence.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct NormalizationProposal {
    /// Raw record identifier repeated for envelope consistency validation.
    pub raw_record_id: String,
    /// Schema version of the proposed record.
    pub schema_version: String,
    /// Target-specific proposed canonical record.
    #[schema(value_type = Object)]
    pub record: JsonValue,
    /// Model confidence in the closed interval from zero to one.
    pub confidence: f64,
    /// Evidence used to generate the proposal.
    #[schema(value_type = Object)]
    pub evidence: JsonValue,
    /// Optional target-specific patch representation.
    #[serde(default)]
    #[schema(value_type = Option<Object>)]
    pub patch: Option<JsonValue>,
}

/// Intelligence-side validation result attached to a proposal.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ProposalValidation {
    /// Whether the proposal passed Intelligence-side validation.
    pub accepted: bool,
    /// Structured validation issues.
    #[serde(default)]
    #[schema(value_type = Object)]
    pub issues: JsonValue,
}

/// Distributed tracing envelope.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct TraceContext {
    /// Trace identifier propagated across services.
    pub trace_id: String,
}

/// Model, prompt, and policy provenance for a proposal.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct SubmissionMetadata {
    /// Optional configured model profile.
    #[serde(default)]
    pub model_profile_id: Option<String>,
    /// Optional concrete model identifier.
    #[serde(default)]
    pub model_id: Option<String>,
    /// Optional prompt identifier.
    #[serde(default)]
    pub prompt_id: Option<String>,
    /// Optional prompt version.
    #[serde(default)]
    pub prompt_version: Option<String>,
    /// Required normalization policy identifier.
    pub policy_id: String,
    /// Required normalization policy version.
    pub policy_version: String,
}

/// Staff review request for proposal approval or rejection.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct NormalizationReviewRequest {
    /// Human review reason retained in audit history.
    pub reason: String,
}

/// Staff review result.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct NormalizationReviewResult {
    /// Reviewed proposal identifier.
    pub proposal_id: String,
    /// Stable semantic proposal key.
    pub proposal_key: String,
    /// Current proposal lifecycle state.
    pub status: String,
    /// Applied review decision.
    pub decision: String,
}

/// Staff command to apply an approved proposal.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct NormalizationApplyRequest {
    /// Canonical version observed by the reviewer.
    pub expected_version: i64,
}

/// Result of applying a normalization proposal.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct NormalizationApplyResult {
    /// Immutable application-ledger identifier.
    pub application_id: String,
    /// Applied proposal identifier.
    pub proposal_id: String,
    /// Canonical target kind.
    pub target_kind: String,
    /// Canonical aggregate identifier when the target has one.
    pub target_id: Option<String>,
}

/// Staff command to compensate a prior normalization application.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct NormalizationRollbackRequest {
    /// Canonical version observed by the reviewer.
    pub expected_current_version: i64,
    /// Human rollback reason validated at the command boundary.
    ///
    /// Durable persistence requires the separately approved exact-retry and audit schema.
    pub reason: String,
}

/// Result of compensating a prior normalization application.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct NormalizationRollbackResult {
    /// Immutable compensation-ledger identifier.
    pub application_id: String,
    /// Original application identifier compensated by this result.
    pub rollback_of: String,
    /// Canonical target kind.
    pub target_kind: String,
    /// Canonical aggregate identifier when the target has one.
    pub target_id: Option<String>,
}

/// Durable proposal-inbox acknowledgement returned to Intelligence Platform.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct FoundationSubmissionResult {
    /// Persisted proposal identifier.
    pub submission_id: String,
    /// Submission state, currently `queued`.
    pub status: String,
    /// Whether staff review is required before application.
    pub review_required: bool,
    /// Owning platform wire identifier.
    pub platform: String,
    /// Stable submission metadata.
    pub metadata: BTreeMap<String, String>,
}

/// Error envelope returned by internal proposal intake.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct IntakeError {
    /// Stable machine-readable error code.
    pub code: String,
    /// Safe client-facing diagnostic.
    pub message: String,
}

impl IntakeError {
    /// Creates an intake error envelope.
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}
