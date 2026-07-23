//! Use case for building canonical industrial-complex Silver handoff JSONL.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use lakehouse_domain::LakehouseError;

use crate::industrial_complex_silver_plan::{
    build_industrial_complex_silver_handoff, normalize_industrial_complex_silver_rows,
    IndustrialComplexSilverHandoff, IndustrialComplexSilverRowsInput,
};
use crate::ports::IndustrialComplexMaterializationReader;

/// Input for building the industrial-complex Silver handoff.
pub struct BuildIndustrialComplexSilverHandoffInput {
    /// Source-snapshot lineage id for this export batch.
    pub source_snapshot_id: String,
    /// UTC timestamp when the exported rows enter the lakehouse flow.
    pub ingested_at_utc: DateTime<Utc>,
}

/// Builds writer-neutral `silver.industrial_complexes` JSONL from Catalog rows.
pub struct BuildIndustrialComplexSilverHandoff {
    repository: Arc<dyn IndustrialComplexMaterializationReader>,
}

impl BuildIndustrialComplexSilverHandoff {
    /// Creates a use case instance backed by a read-only Catalog repository.
    #[must_use]
    pub fn new(repository: Arc<dyn IndustrialComplexMaterializationReader>) -> Self {
        Self { repository }
    }

    /// Loads Catalog industrial complexes and converts them into Silver handoff JSONL.
    ///
    /// # Errors
    /// Returns `LakehouseError` when repository access fails, the Catalog has no exportable
    /// complexes, or a row violates the Silver handoff contract.
    pub async fn execute(
        &self,
        input: BuildIndustrialComplexSilverHandoffInput,
    ) -> Result<IndustrialComplexSilverHandoff, LakehouseError> {
        let complexes = self.repository.list_industrial_complexes().await?;
        if complexes.is_empty() {
            return Err(LakehouseError::InvalidContract(
                "cannot export silver.industrial_complexes handoff from an empty Catalog"
                    .to_owned(),
            ));
        }

        let rows = normalize_industrial_complex_silver_rows(&IndustrialComplexSilverRowsInput {
            complexes: &complexes,
            source_snapshot_id: input.source_snapshot_id.as_str(),
            ingested_at_utc: input.ingested_at_utc,
        })
        .map_err(|error| LakehouseError::InvalidContract(error.to_string()))?;

        build_industrial_complex_silver_handoff(&rows)
            .map_err(|error| LakehouseError::InvalidContract(error.to_string()))
    }
}
