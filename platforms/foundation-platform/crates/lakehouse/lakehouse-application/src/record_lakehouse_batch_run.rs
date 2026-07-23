//! Use case for recording validated lakehouse batch run summaries.

use std::sync::Arc;

use foundation_shared_kernel::ids::StaffId;
use lakehouse_domain::{
    industrial_complex_lakehouse_contract_by_table_name, LakehouseError, LakehouseTableContract,
    SparkRunSummary,
};

use crate::ports::{LakehouseBatchRunAudit, LakehouseBatchRunAuditCommand};

/// Input for recording a Spark lakehouse batch run summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordLakehouseBatchRunInput {
    /// Raw `foundation-platform.spark_run_summary.v1` JSON payload emitted by the batch job.
    pub summary_json: String,
    /// Staff operator that caused foundation-platform to accept the summary.
    pub recorded_by_staff_id: StaffId,
    /// Optional caller-supplied request id used for trace correlation.
    pub request_id: Option<String>,
}

/// Records a Spark batch run summary after validating it against foundation-platform contracts.
pub struct RecordLakehouseBatchRun {
    audit: Arc<dyn LakehouseBatchRunAudit>,
}

impl RecordLakehouseBatchRun {
    /// Creates a use case instance backed by the given lakehouse batch audit sink.
    #[must_use]
    pub fn new(audit: Arc<dyn LakehouseBatchRunAudit>) -> Self {
        Self { audit }
    }

    /// Parses, validates, and records a Spark run summary JSON payload.
    ///
    /// # Errors
    /// Returns `LakehouseError::InvalidLakehouseBatchRun` when the JSON shape or contract
    /// validation fails. Returns `LakehouseError` from the audit sink when persistence fails.
    pub async fn execute(
        &self,
        input: RecordLakehouseBatchRunInput,
    ) -> Result<SparkRunSummary, LakehouseError> {
        let summary = SparkRunSummary::from_json_str(&input.summary_json)
            .map_err(|error| LakehouseError::InvalidLakehouseBatchRun(error.to_string()))?;
        let contract = find_lakehouse_contract(&summary.contract)?;
        summary
            .validate_for_contract(contract)
            .map_err(|error| LakehouseError::InvalidLakehouseBatchRun(error.to_string()))?;

        self.audit
            .record_spark_run_summary(LakehouseBatchRunAuditCommand {
                summary: summary.clone(),
                recorded_by_staff_id: input.recorded_by_staff_id,
                request_id: normalize_request_id(input.request_id),
            })
            .await?;
        Ok(summary)
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

fn normalize_request_id(request_id: Option<String>) -> Option<String> {
    request_id
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}
