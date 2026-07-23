//! Submit, review, apply, and rollback use cases for normalization proposals.

use std::sync::Arc;

use foundation_normalization_domain::{
    compute_normalization_proposal_content_hash, compute_normalization_proposal_key,
    validate_building_register_unit_target_identity_matches, validate_normalization_json_object,
    NormalizationError, NormalizationProposalKeyInput, NormalizationProposalStatus,
    NormalizationTargetKind,
};
use foundation_shared_kernel::ids::PrincipalId;
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::{
    NormalizationApplicationCommand, NormalizationApplicationRecord, NormalizationProposalRecord,
    NormalizationProposalReviewCommand, NormalizationProposalSubmissionCommand,
    NormalizationRollbackCommand, NormalizationRollbackRecord, NormalizationUnitOfWork,
};

/// Submits an AI-generated normalization proposal to the pending-review inbox.
pub struct SubmitNormalizationProposal {
    uow: Arc<dyn NormalizationUnitOfWork>,
}

impl SubmitNormalizationProposal {
    /// Creates a submit use case backed by the supplied unit of work.
    #[must_use]
    pub fn new(uow: Arc<dyn NormalizationUnitOfWork>) -> Self {
        Self { uow }
    }

    /// Validates, stamps, and stores a pending-review normalization proposal.
    ///
    /// # Errors
    /// Returns [`NormalizationError`] when the payload is invalid or persistence fails.
    pub async fn execute(
        &self,
        mut command: NormalizationProposalSubmissionCommand,
    ) -> Result<NormalizationProposalRecord, NormalizationError> {
        validate_submission_command(&command)?;
        command.status = NormalizationProposalStatus::PendingReview;
        command.proposed_record_sha256 =
            compute_normalization_proposal_content_hash(&command.proposed_record)?.0;
        command.proposal_key =
            compute_normalization_proposal_key(&NormalizationProposalKeyInput {
                source_system: command.source_system.clone(),
                raw_record_id: command.raw_record_id.clone(),
                raw_checksum_sha256: command.raw_checksum_sha256.clone(),
                target_kind: command.target_kind,
                target_identity: command.target_identity.clone(),
                target_schema_version: command.target_schema_version.clone(),
                proposal_schema_version: command.proposal_schema_version.clone(),
                policy_id: command.policy_id.clone(),
                policy_version: command.policy_version.clone(),
                model_profile_id: command.model_profile_id.clone(),
                prompt_id: command.prompt_id.clone(),
                prompt_version: command.prompt_version.clone(),
                proposed_record: command.proposed_record.clone(),
            })?;
        self.uow.submit_normalization_proposal(command).await
    }
}

/// Records a human review decision for an AI-generated normalization proposal.
pub struct ReviewNormalizationProposal {
    uow: Arc<dyn NormalizationUnitOfWork>,
}

impl ReviewNormalizationProposal {
    /// Creates a review use case backed by the supplied unit of work.
    #[must_use]
    pub fn new(uow: Arc<dyn NormalizationUnitOfWork>) -> Self {
        Self { uow }
    }

    /// Validates a review reason before persistence.
    ///
    /// # Errors
    /// Returns [`NormalizationError::InvalidInput`] when the reason is blank.
    pub fn validate_reason(reason: &str) -> Result<(), NormalizationError> {
        validate_required_text("reason", reason)
    }

    /// Validates the reviewer principal identity before persistence.
    ///
    /// # Errors
    /// Returns [`NormalizationError::InvalidInput`] when the principal id is nil.
    pub fn validate_reviewer_principal_id(
        reviewer_principal_id: PrincipalId,
    ) -> Result<(), NormalizationError> {
        validate_non_nil_principal_id("reviewer_principal_id", reviewer_principal_id)
    }

    /// Validates and records an authorized principal's review decision.
    ///
    /// # Errors
    /// Returns [`NormalizationError`] when the command is invalid or persistence fails.
    pub async fn execute(
        &self,
        mut command: NormalizationProposalReviewCommand,
    ) -> Result<NormalizationProposalRecord, NormalizationError> {
        Self::validate_reason(&command.reason)?;
        Self::validate_reviewer_principal_id(command.reviewer_principal_id)?;
        command.reason = command.reason.trim().to_owned();
        self.uow.review_normalization_proposal(command).await
    }
}

/// Applies an approved normalization proposal through its target adapter.
pub struct ApplyNormalizationProposal {
    uow: Arc<dyn NormalizationUnitOfWork>,
}

impl ApplyNormalizationProposal {
    /// Creates an apply use case backed by the supplied unit of work.
    #[must_use]
    pub fn new(uow: Arc<dyn NormalizationUnitOfWork>) -> Self {
        Self { uow }
    }

    /// Validates that a proposal has completed human approval before apply.
    ///
    /// # Errors
    /// Returns [`NormalizationError::InvalidState`] when the proposal is not approved.
    pub fn validate_status(status: NormalizationProposalStatus) -> Result<(), NormalizationError> {
        if status != NormalizationProposalStatus::Approved {
            return Err(NormalizationError::InvalidState(
                "proposal must be approved before apply".to_owned(),
            ));
        }
        Ok(())
    }

    /// Validates and applies an approved proposal through the persistence unit of work.
    ///
    /// # Errors
    /// Returns [`NormalizationError`] when input is invalid or persistence fails.
    pub async fn execute(
        &self,
        command: NormalizationApplicationCommand,
    ) -> Result<NormalizationApplicationRecord, NormalizationError> {
        validate_expected_version(command.expected_version)?;
        validate_non_nil_principal_id("applied_by_principal_id", command.applied_by_principal_id)?;
        self.uow.apply_normalization_proposal(command).await
    }
}

/// Rolls back an applied normalization proposal through a compensating command.
pub struct RollbackNormalizationApplication {
    uow: Arc<dyn NormalizationUnitOfWork>,
}

impl RollbackNormalizationApplication {
    /// Creates a rollback use case backed by the supplied unit of work.
    #[must_use]
    pub fn new(uow: Arc<dyn NormalizationUnitOfWork>) -> Self {
        Self { uow }
    }

    /// Validates that an audit row contains the pre-apply snapshot needed to restore.
    ///
    /// # Errors
    /// Returns [`NormalizationError::InvalidInput`] when the snapshot is not an object.
    pub fn validate_before_snapshot(before_snapshot: &JsonValue) -> Result<(), NormalizationError> {
        if before_snapshot.is_object() {
            Ok(())
        } else {
            Err(NormalizationError::InvalidInput(
                "before_snapshot must be a JSON object".to_owned(),
            ))
        }
    }

    /// Validates and rolls back an application through the persistence unit of work.
    ///
    /// # Errors
    /// Returns [`NormalizationError`] when input is invalid or persistence fails.
    pub async fn execute(
        &self,
        mut command: NormalizationRollbackCommand,
    ) -> Result<NormalizationRollbackRecord, NormalizationError> {
        validate_expected_version(command.expected_current_version)?;
        validate_required_text("reason", &command.reason)?;
        validate_non_nil_principal_id(
            "rolled_back_by_principal_id",
            command.rolled_back_by_principal_id,
        )?;
        command.reason = command.reason.trim().to_owned();
        self.uow.rollback_normalization_application(command).await
    }
}

fn validate_submission_command(
    command: &NormalizationProposalSubmissionCommand,
) -> Result<(), NormalizationError> {
    validate_required_text("submitted_by_service", &command.submitted_by_service)?;
    validate_required_text("source_system", &command.source_system)?;
    validate_required_text("raw_record_id", &command.raw_record_id)?;
    validate_optional_text("raw_object_key", command.raw_object_key.as_deref())?;
    validate_optional_sha256(
        "raw_checksum_sha256",
        command.raw_checksum_sha256.as_deref(),
    )?;
    validate_required_text("target_schema_version", &command.target_schema_version)?;
    validate_required_text("proposal_schema_version", &command.proposal_schema_version)?;
    validate_required_text("policy_id", &command.policy_id)?;
    validate_required_text("policy_version", &command.policy_version)?;
    validate_required_text("trace_id", &command.trace_id)?;
    validate_confidence(command.confidence)?;
    validate_normalization_json_object("target_identity", &command.target_identity)?;
    if command.target_kind == NormalizationTargetKind::BuildingRegisterUnit {
        validate_building_register_unit_target_identity_matches(
            &command.target_identity,
            &command.source_system,
            &command.raw_record_id,
        )?;
    }
    validate_normalization_json_object("proposed_record", &command.proposed_record)?;
    if let Some(proposed_patch) = command.proposed_patch.as_ref() {
        validate_normalization_json_object("proposed_patch", proposed_patch)?;
    }
    validate_normalization_json_object("evidence", &command.evidence)?;
    validate_normalization_json_object("validation", &command.validation)?;
    Ok(())
}

fn validate_required_text(field: &str, value: &str) -> Result<(), NormalizationError> {
    if value.trim().is_empty() {
        return Err(NormalizationError::InvalidInput(format!(
            "{field} must not be empty"
        )));
    }
    Ok(())
}

fn validate_optional_text(field: &str, value: Option<&str>) -> Result<(), NormalizationError> {
    if value.is_some_and(|raw| raw.trim().is_empty()) {
        return Err(NormalizationError::InvalidInput(format!(
            "{field} must not be empty when provided"
        )));
    }
    Ok(())
}

fn validate_optional_sha256(field: &str, value: Option<&str>) -> Result<(), NormalizationError> {
    if value.is_some_and(|raw| !is_lowercase_sha256(raw)) {
        return Err(NormalizationError::InvalidInput(format!(
            "{field} must be 64 lowercase hex characters"
        )));
    }
    Ok(())
}

fn validate_confidence(value: f64) -> Result<(), NormalizationError> {
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(NormalizationError::InvalidInput(
            "confidence must be between 0 and 1".to_owned(),
        ));
    }
    Ok(())
}

fn validate_expected_version(expected_version: i64) -> Result<(), NormalizationError> {
    if expected_version < 1 {
        return Err(NormalizationError::InvalidInput(
            "expected_version must be positive".to_owned(),
        ));
    }
    Ok(())
}

fn validate_non_nil_principal_id(
    field: &str,
    principal_id: PrincipalId,
) -> Result<(), NormalizationError> {
    if principal_id.as_uuid() == Uuid::nil() {
        return Err(NormalizationError::InvalidInput(format!(
            "{field} must not be nil"
        )));
    }
    Ok(())
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
