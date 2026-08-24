//! Use cases for the static vector tile build lifecycle.
//!
//! A static `PMTiles` release is only meaningful for the data revision it was built from, so the
//! build's inputs are recorded in the ledger before any artifact exists. Starting and reporting live
//! in one module because they are two halves of one record: the row that `start` writes is what
//! `record` is allowed to update, and the rules that make the pair safe — the frozen-snapshot
//! binding and the terminal-status refusal — read from the same place.
//!
//! Promotion is deliberately not here. It moves the serving pointer and belongs with the other
//! pointer-moving commands.
//!
//! The build ledger is internal, so
//! [`RuntimeManifestPublicationCapability`](crate::ports::RuntimeManifestPublicationCapability) does
//! not gate it. That capability decides whether the transaction writes the v2 *outbox event*;
//! recording builds while it is off is how a deployment accumulates evidence before anyone consumes
//! it.

use std::sync::Arc;

use catalog_domain::{CatalogError, VectorTileBuildOutcome};
use foundation_shared_kernel::ids::VectorTileBuildJobId;

use crate::ports::{
    CatalogUnitOfWork, RecordVectorTileBuildResultCommand, StartVectorTileBuildCommand,
};

/// Records static build attempts and their results through the Catalog unit of work.
pub struct VectorTileBuildLifecycle {
    uow: Arc<dyn CatalogUnitOfWork>,
}

impl VectorTileBuildLifecycle {
    /// Creates a use case instance backed by the given Catalog unit of work.
    #[must_use]
    pub const fn new(uow: Arc<dyn CatalogUnitOfWork>) -> Self {
        Self { uow }
    }

    /// Records a static build attempt against its input release and frozen snapshot.
    ///
    /// # Errors
    ///
    /// Returns `CatalogError` when required text is empty, or when the transaction refuses the
    /// inputs or its write fails.
    pub async fn start(
        &self,
        command: StartVectorTileBuildCommand,
    ) -> Result<VectorTileBuildJobId, CatalogError> {
        self.uow
            .start_vector_tile_build(StartVectorTileBuildCommand {
                unit_key: normalize_required(&command.unit_key, "unit_key")?,
                idempotency_key: normalize_required(&command.idempotency_key, "idempotency_key")?,
                ..command
            })
            .await
    }

    /// Records what a static build attempt produced.
    ///
    /// # Errors
    ///
    /// Returns `CatalogError` when a failure is reported with no reason, or when the transaction
    /// refuses the report or its write fails.
    pub async fn record_result(
        &self,
        command: RecordVectorTileBuildResultCommand,
    ) -> Result<(), CatalogError> {
        self.uow
            .record_vector_tile_build_result(RecordVectorTileBuildResultCommand {
                outcome: normalize_outcome(command.outcome)?,
                ..command
            })
            .await
    }
}

/// Trims a failure reason and refuses an empty one.
///
/// `failed` is the only status an operator cannot diagnose from the row itself, so a blank reason
/// would record that something went wrong while withholding the one field that says what.
fn normalize_outcome(
    outcome: VectorTileBuildOutcome,
) -> Result<VectorTileBuildOutcome, CatalogError> {
    match outcome {
        VectorTileBuildOutcome::Validated { evidence, artifact } => {
            if artifact.size_bytes == 0 {
                return Err(invalid(
                    "validated PMTiles size_bytes must be greater than zero",
                ));
            }
            Ok(VectorTileBuildOutcome::Validated { evidence, artifact })
        }
        VectorTileBuildOutcome::Failed(reason) => Ok(VectorTileBuildOutcome::Failed(
            normalize_required(&reason, "failure reason")?,
        )),
    }
}

fn normalize_required(raw: &str, field: &str) -> Result<String, CatalogError> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(invalid(&format!("{field} must not be empty")));
    }
    Ok(value.to_owned())
}

fn invalid(message: &str) -> CatalogError {
    CatalogError::InvalidVectorTileRuntimeManifest(message.to_owned())
}
