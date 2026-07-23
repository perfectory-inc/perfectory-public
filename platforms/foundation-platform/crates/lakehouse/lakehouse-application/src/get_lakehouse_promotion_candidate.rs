//! Use case for selecting validated lakehouse batch promotion candidates.

use std::sync::Arc;

use lakehouse_domain::{
    industrial_complex_lakehouse_contract_by_table_name, LakehouseError, LakehouseTableContract,
    SparkRunWriteDisposition,
};

use crate::ports::{LakehouseBatchRunRecord, LakehouseBatchRunRepository};

/// Loads the newest batch audit row that is safe to use as a promotion candidate.
pub struct GetLakehousePromotionCandidate {
    repository: Arc<dyn LakehouseBatchRunRepository>,
}

impl GetLakehousePromotionCandidate {
    /// Creates a use case instance backed by the given lakehouse batch run repository.
    #[must_use]
    pub fn new(repository: Arc<dyn LakehouseBatchRunRepository>) -> Self {
        Self { repository }
    }

    /// Finds and revalidates the newest promotion candidate for one lakehouse table contract.
    ///
    /// # Errors
    /// Returns `LakehouseError::InvalidLakehouseBatchRun` when the contract is unknown or a stored
    /// audit row no longer matches the static Foundation Platform contract.
    pub async fn execute(
        &self,
        table_name: &str,
    ) -> Result<Option<LakehouseBatchRunRecord>, LakehouseError> {
        let contract = find_lakehouse_contract(table_name)?;
        let candidate = self.repository.latest_promotion_candidate(contract).await?;

        if let Some(record) = candidate.as_ref() {
            validate_promotion_candidate(record, contract)?;
        }

        Ok(candidate)
    }
}

fn find_lakehouse_contract(
    table_name: &str,
) -> Result<&'static LakehouseTableContract, LakehouseError> {
    industrial_complex_lakehouse_contract_by_table_name(table_name).ok_or_else(|| {
        LakehouseError::InvalidLakehouseBatchRun(format!(
            "unknown lakehouse table contract: {table_name}"
        ))
    })
}

fn validate_promotion_candidate(
    record: &LakehouseBatchRunRecord,
    contract: &'static LakehouseTableContract,
) -> Result<(), LakehouseError> {
    if record.contract != contract.table_name {
        return invalid_candidate(format!(
            "audit row contract {} did not match requested contract {}",
            record.contract, contract.table_name
        ));
    }
    if record.schema_version != record.summary.schema_version {
        return invalid_candidate("audit row schema_version differs from summary_json".to_owned());
    }
    if record.job_name != record.summary.job_name {
        return invalid_candidate("audit row job_name differs from summary_json".to_owned());
    }
    if record.created_at_utc != record.summary.created_at_utc {
        return invalid_candidate("audit row created_at differs from summary_json".to_owned());
    }
    if record.write_disposition != record.summary.write_disposition {
        return invalid_candidate(
            "audit row write_disposition differs from summary_json".to_owned(),
        );
    }
    if record.row_count != record.summary.row_count {
        return invalid_candidate("audit row row_count differs from summary_json".to_owned());
    }
    if record.persisted_row_count != record.summary.persisted_row_count {
        return invalid_candidate(
            "audit row persisted_row_count differs from summary_json".to_owned(),
        );
    }
    if record.source_snapshot_ids != record.summary.source_snapshot_ids {
        return invalid_candidate(
            "audit row source_snapshot_ids differs from summary_json".to_owned(),
        );
    }
    if record.summary.write_disposition == SparkRunWriteDisposition::ValidateOnly {
        return invalid_candidate(
            "validate-only batch run cannot be a promotion candidate".to_owned(),
        );
    }
    if record.summary.persisted_row_count != Some(record.summary.row_count) {
        return invalid_candidate(
            "promotion candidate must have persisted_row_count equal to row_count".to_owned(),
        );
    }

    record
        .summary
        .validate_for_contract(contract)
        .map_err(|error| LakehouseError::InvalidLakehouseBatchRun(error.to_string()))
}

const fn invalid_candidate<T>(reason: String) -> Result<T, LakehouseError> {
    Err(LakehouseError::InvalidLakehouseBatchRun(reason))
}
