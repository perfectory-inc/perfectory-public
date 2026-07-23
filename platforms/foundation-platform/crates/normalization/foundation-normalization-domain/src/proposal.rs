//! Domain contracts for AI-assisted normalization proposals.
//!
//! AI systems may propose a normalized record, but these types do not grant write authority.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::NormalizationError;

/// Canonical fact kind targeted by a normalization proposal.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NormalizationTargetKind {
    /// Industrial complex canonical fact.
    IndustrialComplex,
    /// Building-register floor normalized fact proposed from Silver handoff rows.
    BuildingRegisterFloor,
    /// Building-register exclusive-unit normalized fact proposed from Silver handoff rows.
    BuildingRegisterUnit,
}

impl NormalizationTargetKind {
    /// Stable wire representation used in proposal keys and API payloads.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::IndustrialComplex => "industrial_complex",
            Self::BuildingRegisterFloor => "building_register_floor",
            Self::BuildingRegisterUnit => "building_register_unit",
        }
    }
}

/// Lifecycle state of a submitted normalization proposal.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NormalizationProposalStatus {
    /// Proposal is stored and awaiting review by an authorized principal.
    PendingReview,
    /// Proposal was approved by an authorized principal.
    Approved,
    /// Proposal was rejected by an authorized principal.
    Rejected,
    /// Proposal was superseded by a newer proposal for the same target.
    Superseded,
    /// Proposal was applied to canonical state.
    Applied,
    /// Proposal application failed after approval.
    ApplyFailed,
    /// Applied proposal was rolled back through a canonical command.
    RolledBack,
}

impl NormalizationProposalStatus {
    /// Stable wire representation used in storage and API payloads.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::PendingReview => "pending_review",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::Superseded => "superseded",
            Self::Applied => "applied",
            Self::ApplyFailed => "apply_failed",
            Self::RolledBack => "rolled_back",
        }
    }
}

/// Authorized-principal review decision for a pending proposal.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NormalizationReviewDecision {
    /// Proposal may be applied through a canonical command.
    Approved,
    /// Proposal must not be applied.
    Rejected,
    /// Proposal needs a revised AI/human submission before review can finish.
    NeedsChanges,
}

impl NormalizationReviewDecision {
    /// Stable wire representation used in storage and API payloads.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::NeedsChanges => "needs_changes",
        }
    }
}

/// SHA-256 hash of the canonicalized proposed record payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NormalizationProposalContentHash(pub String);

/// Input used to derive an idempotent normalization proposal key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizationProposalKeyInput {
    /// System that produced or owns the raw source record.
    pub source_system: String,
    /// Stable raw record identity inside `source_system`.
    pub raw_record_id: String,
    /// Optional checksum of the raw record bytes.
    pub raw_checksum_sha256: Option<String>,
    /// Canonical fact kind targeted by the proposal.
    pub target_kind: NormalizationTargetKind,
    /// JSON object identifying the target fact.
    pub target_identity: Value,
    /// Schema version expected by the target canonical fact.
    pub target_schema_version: String,
    /// Schema version of `proposed_record`.
    pub proposal_schema_version: String,
    /// Normalization policy identifier.
    pub policy_id: String,
    /// Normalization policy version.
    pub policy_version: String,
    /// Optional model profile identifier used by intelligence-platform.
    pub model_profile_id: Option<String>,
    /// Optional prompt identifier used by intelligence-platform.
    pub prompt_id: Option<String>,
    /// Optional prompt version used by intelligence-platform.
    pub prompt_version: Option<String>,
    /// Proposed normalized record payload.
    pub proposed_record: Value,
}

/// In-memory representation of a normalization proposal before persistence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NormalizationProposal {
    /// Idempotent proposal key derived from source, target, policy, model, prompt, and payload.
    pub proposal_key: String,
    /// Lifecycle state. Newly submitted proposals start as `PendingReview`.
    pub status: NormalizationProposalStatus,
    /// System that produced or owns the raw source record.
    pub source_system: String,
    /// Stable raw record identity inside `source_system`.
    pub raw_record_id: String,
    /// Optional checksum of the raw record bytes.
    pub raw_checksum_sha256: Option<String>,
    /// Canonical fact kind targeted by the proposal.
    pub target_kind: NormalizationTargetKind,
    /// JSON object identifying the target fact.
    pub target_identity: Value,
    /// Schema version expected by the target canonical fact.
    pub target_schema_version: String,
    /// Schema version of `proposed_record`.
    pub proposal_schema_version: String,
    /// Normalization policy identifier.
    pub policy_id: String,
    /// Normalization policy version.
    pub policy_version: String,
    /// Optional model profile identifier used by intelligence-platform.
    pub model_profile_id: Option<String>,
    /// Optional prompt identifier used by intelligence-platform.
    pub prompt_id: Option<String>,
    /// Optional prompt version used by intelligence-platform.
    pub prompt_version: Option<String>,
    /// Proposed normalized record payload.
    pub proposed_record: Value,
    /// SHA-256 hash of the canonicalized proposed record payload.
    pub proposed_record_hash: NormalizationProposalContentHash,
    /// Evidence object supplied with the proposal.
    pub evidence: Value,
    /// Validation details object supplied with the proposal.
    pub validation: Value,
    /// Cross-service trace identifier.
    pub trace_id: String,
}

/// Computes an idempotent proposal key from canonicalized proposal identity fields.
///
/// # Errors
/// Returns [`NormalizationError::InvalidInput`] when flexible JSON fields are not objects, or
/// [`NormalizationError::Persistence`] if canonical JSON encoding fails.
pub fn compute_normalization_proposal_key(
    input: &NormalizationProposalKeyInput,
) -> Result<String, NormalizationError> {
    validate_normalization_json_object("target_identity", &input.target_identity)?;
    validate_normalization_json_object("proposed_record", &input.proposed_record)?;

    let canonical_payload = canonical_value(Value::Object(Map::from_iter([
        (
            "source_system".to_owned(),
            Value::String(input.source_system.clone()),
        ),
        (
            "raw_record_id".to_owned(),
            Value::String(input.raw_record_id.clone()),
        ),
        (
            "raw_checksum_sha256".to_owned(),
            option_string_value(input.raw_checksum_sha256.as_deref()),
        ),
        (
            "target_kind".to_owned(),
            Value::String(input.target_kind.wire_name().to_owned()),
        ),
        ("target_identity".to_owned(), input.target_identity.clone()),
        (
            "target_schema_version".to_owned(),
            Value::String(input.target_schema_version.clone()),
        ),
        (
            "proposal_schema_version".to_owned(),
            Value::String(input.proposal_schema_version.clone()),
        ),
        (
            "policy_id".to_owned(),
            Value::String(input.policy_id.clone()),
        ),
        (
            "policy_version".to_owned(),
            Value::String(input.policy_version.clone()),
        ),
        (
            "model_profile_id".to_owned(),
            option_string_value(input.model_profile_id.as_deref()),
        ),
        (
            "prompt_id".to_owned(),
            option_string_value(input.prompt_id.as_deref()),
        ),
        (
            "prompt_version".to_owned(),
            option_string_value(input.prompt_version.as_deref()),
        ),
        ("proposed_record".to_owned(), input.proposed_record.clone()),
    ])));

    Ok(format!(
        "normprop:v1:{}",
        sha256_hex(&serialize_canonical_value(&canonical_payload)?)
    ))
}

/// Computes the SHA-256 hash of a canonicalized proposed record payload.
///
/// # Errors
/// Returns [`NormalizationError::InvalidInput`] when `proposed_record` is not an object, or
/// [`NormalizationError::Persistence`] if canonical JSON encoding fails.
pub fn compute_normalization_proposal_content_hash(
    proposed_record: &Value,
) -> Result<NormalizationProposalContentHash, NormalizationError> {
    validate_normalization_json_object("proposed_record", proposed_record)?;
    Ok(NormalizationProposalContentHash(sha256_hex(
        &serialize_canonical_value(&canonical_value(proposed_record.clone()))?,
    )))
}

/// Validates that a flexible JSON field is an object, not an array, scalar, or null.
///
/// # Errors
/// Returns [`NormalizationError::InvalidInput`] when `value` is not a JSON object.
pub fn validate_normalization_json_object(
    field_name: &str,
    value: &Value,
) -> Result<(), NormalizationError> {
    if value.is_object() {
        return Ok(());
    }

    Err(NormalizationError::InvalidInput(format!(
        "{field_name} must be a JSON object"
    )))
}

fn option_string_value(value: Option<&str>) -> Value {
    value.map_or(Value::Null, |raw| Value::String(raw.to_owned()))
}

fn serialize_canonical_value(value: &Value) -> Result<Vec<u8>, NormalizationError> {
    serde_json::to_vec(value)
        .map_err(|error| NormalizationError::Persistence(format!("serde encode: {error}")))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("{digest:x}")
}

fn canonical_value(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.into_iter().map(canonical_value).collect()),
        Value::Object(object) => {
            let mut entries = object.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonical_value(value)))
                    .collect(),
            )
        }
        scalar => scalar,
    }
}
