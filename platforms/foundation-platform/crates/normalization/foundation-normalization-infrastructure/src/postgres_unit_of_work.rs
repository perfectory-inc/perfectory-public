//! PostgreSQL implementation of the Normalization unit-of-work port.

use async_trait::async_trait;
use foundation_normalization_application::{
    NormalizationApplicationCommand, NormalizationApplicationRecord, NormalizationProposalRecord,
    NormalizationProposalReviewCommand, NormalizationProposalSubmissionCommand,
    NormalizationRollbackCommand, NormalizationRollbackRecord, NormalizationUnitOfWork,
};
use foundation_normalization_domain::NormalizationError;
use sqlx::PgPool;

use crate::{application, proposal, review};

/// `PostgreSQL` implementation of durable proposal governance.
pub struct PgNormalizationUnitOfWork {
    pool: PgPool,
}

impl PgNormalizationUnitOfWork {
    /// Creates a unit of work backed by the given `PostgreSQL` pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl NormalizationUnitOfWork for PgNormalizationUnitOfWork {
    async fn submit_normalization_proposal(
        &self,
        command: NormalizationProposalSubmissionCommand,
    ) -> Result<NormalizationProposalRecord, NormalizationError> {
        proposal::submit(&self.pool, command).await
    }

    async fn review_normalization_proposal(
        &self,
        command: NormalizationProposalReviewCommand,
    ) -> Result<NormalizationProposalRecord, NormalizationError> {
        review::review(&self.pool, command).await
    }

    async fn apply_normalization_proposal(
        &self,
        command: NormalizationApplicationCommand,
    ) -> Result<NormalizationApplicationRecord, NormalizationError> {
        application::apply(&self.pool, command).await
    }

    async fn rollback_normalization_application(
        &self,
        command: NormalizationRollbackCommand,
    ) -> Result<NormalizationRollbackRecord, NormalizationError> {
        application::rollback(&self.pool, command).await
    }
}
