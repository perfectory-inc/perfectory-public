//! Outbound ports owned by the Normalization application capability.

use async_trait::async_trait;
use foundation_normalization_domain::NormalizationError;

use crate::{
    ActiveBuildingRegisterUnitOverride, NormalizationApplicationCommand,
    NormalizationApplicationRecord, NormalizationProposalRecord,
    NormalizationProposalReviewCommand, NormalizationProposalSubmissionCommand,
    NormalizationRollbackCommand, NormalizationRollbackRecord,
};

/// Atomic mutation boundary for durable normalization proposal governance.
#[async_trait]
pub trait NormalizationUnitOfWork: Send + Sync {
    /// Stores a pending-review proposal idempotently.
    ///
    /// # Errors
    /// Returns [`NormalizationError`] when validation reconciliation or persistence fails.
    async fn submit_normalization_proposal(
        &self,
        command: NormalizationProposalSubmissionCommand,
    ) -> Result<NormalizationProposalRecord, NormalizationError>;

    /// Records an authorized principal's review decision for a pending proposal.
    ///
    /// # Errors
    /// Returns [`NormalizationError`] when the transition or persistence fails.
    async fn review_normalization_proposal(
        &self,
        command: NormalizationProposalReviewCommand,
    ) -> Result<NormalizationProposalRecord, NormalizationError>;

    /// Applies an approved proposal and records audit lineage atomically.
    ///
    /// # Errors
    /// Returns [`NormalizationError`] when the proposal cannot be applied or persistence fails.
    async fn apply_normalization_proposal(
        &self,
        command: NormalizationApplicationCommand,
    ) -> Result<NormalizationApplicationRecord, NormalizationError>;

    /// Rolls back an application and records compensating audit lineage atomically.
    ///
    /// # Errors
    /// Returns [`NormalizationError`] when the application cannot be rolled back.
    async fn rollback_normalization_application(
        &self,
        command: NormalizationRollbackCommand,
    ) -> Result<NormalizationRollbackRecord, NormalizationError>;
}

/// Read-only access to active building-register-unit application snapshots.
#[async_trait]
pub trait ActiveBuildingRegisterUnitOverrideReader: Send + Sync {
    /// Lists the latest non-rolled-back application for each unit identity in stable order.
    ///
    /// # Errors
    /// Returns [`NormalizationError`] when persistence access fails.
    async fn list_active_building_register_unit_overrides(
        &self,
    ) -> Result<Vec<ActiveBuildingRegisterUnitOverride>, NormalizationError>;
}
