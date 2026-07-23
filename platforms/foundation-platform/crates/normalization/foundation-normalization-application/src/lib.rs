//! Normalization application commands, ports, and proposal governance use cases.

#![deny(missing_docs)]

/// Active building-register-unit override read model.
pub mod active_overrides;

/// Proposal governance commands and receipts.
pub mod commands;

/// Outbound ports implemented by Normalization infrastructure.
pub mod ports;

/// Proposal submit, review, apply, and rollback use cases.
pub mod proposal;

pub use active_overrides::ActiveBuildingRegisterUnitOverride;
pub use commands::{
    NormalizationApplicationCommand, NormalizationApplicationRecord, NormalizationProposalRecord,
    NormalizationProposalReviewCommand, NormalizationProposalSubmissionCommand,
    NormalizationRollbackCommand, NormalizationRollbackRecord,
};
pub use ports::{ActiveBuildingRegisterUnitOverrideReader, NormalizationUnitOfWork};
pub use proposal::{
    ApplyNormalizationProposal, ReviewNormalizationProposal, RollbackNormalizationApplication,
    SubmitNormalizationProposal,
};
