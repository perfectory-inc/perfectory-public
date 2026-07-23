//! Provider-neutral Spark batch run summary contract.
//!
//! Spark is an execution engine, not the owner of Catalog truth. This module validates the JSON
//! handoff that Spark writes after a batch job so Rust foundation-platform can decide whether a batch is
//! auditable and safe to promote.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::lakehouse::{
    LakehouseTableContract, SILVER_BUILDING_REGISTER_FLOORS, SILVER_BUILDING_REGISTER_UNITS,
    SILVER_INDUSTRIAL_COMPLEXES, SILVER_PARCEL_BOUNDARIES,
};

/// Current Spark run summary JSON schema version.
pub const SPARK_RUN_SUMMARY_SCHEMA_VERSION: &str = "foundation-platform.spark_run_summary.v1";

/// Spark batch input descriptor.
///
/// A `*_jsonl` input kind is an engine-visible transport/staging input. It is not the promoted
/// Silver/Gold table storage format; that is governed by `LakehouseTableContract.physical_format`
/// and the Iceberg target snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SparkRunInput {
    /// Input kind, for example `bronze_jsonl`, `silver_handoff_jsonl`, or
    /// `silver_handoff_parquet`.
    pub kind: String,
    /// Engine-visible input path used by the batch job.
    pub path: String,
}

/// Spark batch write target.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SparkRunTarget {
    /// Local or mounted Parquet path used by contract smoke jobs.
    Parquet {
        /// Engine-visible Parquet output path.
        path: String,
    },
    /// Iceberg REST catalog target table.
    Iceberg {
        /// Spark catalog name.
        catalog: String,
        /// Iceberg namespace.
        namespace: String,
        /// Iceberg table name.
        table: String,
        /// Fully qualified Spark table name, without quoting.
        qualified_table: String,
    },
}

/// Spark batch write mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SparkRunWriteMode {
    /// Writes files to a Parquet path.
    Parquet,
    /// Writes rows into an Iceberg table.
    Iceberg,
}

/// Spark batch write disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SparkRunWriteDisposition {
    /// Validate only; no persisted output is expected.
    ValidateOnly,
    /// Overwrite a local Parquet smoke output.
    ParquetOverwrite,
    /// Append rows into an Iceberg table.
    IcebergAppend,
    /// Overwrite an Iceberg smoke table or explicitly approved table.
    IcebergOverwrite,
}

/// Engine used to validate rows after an Iceberg write.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SparkRunIcebergReadbackValidation {
    /// Spark read the just-written Iceberg table before emitting the summary.
    Spark,
    /// A separate query engine validates the table after Spark emits the summary.
    Deferred,
}

impl SparkRunIcebergReadbackValidation {
    /// Stable wire string for diagnostics and tests.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Spark => "spark",
            Self::Deferred => "deferred",
        }
    }
}

/// Machine-readable summary emitted by a Spark batch job.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SparkRunSummary {
    /// Summary schema version.
    pub schema_version: String,
    /// Stable job name.
    pub job_name: String,
    /// Lakehouse contract table name the job claims to produce.
    pub contract: String,
    /// UTC timestamp when the summary was emitted.
    pub created_at_utc: DateTime<Utc>,
    /// Batch input descriptor.
    pub input: SparkRunInput,
    /// Batch write target descriptor.
    pub target: SparkRunTarget,
    /// High-level write mode.
    pub write_mode: SparkRunWriteMode,
    /// Write disposition used by the job.
    pub write_disposition: SparkRunWriteDisposition,
    /// Optional readback validation mode for Iceberg writes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iceberg_readback_validation: Option<SparkRunIcebergReadbackValidation>,
    /// Number of candidate rows produced by the transformation.
    pub row_count: u64,
    /// Number of rows read back from the persisted output, if the job wrote output.
    pub persisted_row_count: Option<u64>,
    /// Quality metrics emitted by the Spark job.
    pub quality_metrics: BTreeMap<String, u64>,
    /// Declared output column count.
    pub column_count: usize,
    /// Declared output columns in stable contract order.
    pub columns: Vec<String>,
    /// Declared required columns in stable contract order.
    pub required_columns: Vec<String>,
    /// Number of distinct source snapshots represented by the batch.
    pub source_snapshot_count: u64,
    /// Distinct source snapshot ids represented by the batch.
    pub source_snapshot_ids: Vec<String>,
    /// Whether `source_snapshot_ids` was truncated by the execution engine.
    pub source_snapshot_truncated: bool,
}

impl SparkRunSummary {
    /// Parses a run summary from JSON text.
    ///
    /// # Errors
    /// Returns `SparkRunSummaryError::Json` when the JSON shape does not match the schema.
    pub fn from_json_str(raw: &str) -> Result<Self, SparkRunSummaryError> {
        serde_json::from_str(raw).map_err(|error| SparkRunSummaryError::Json(error.to_string()))
    }

    /// Parses a run summary from a JSON value.
    ///
    /// # Errors
    /// Returns `SparkRunSummaryError::Json` when the JSON shape does not match the schema.
    pub fn from_json_value(value: serde_json::Value) -> Result<Self, SparkRunSummaryError> {
        serde_json::from_value(value).map_err(|error| SparkRunSummaryError::Json(error.to_string()))
    }

    /// Validates that the summary is promotion-safe for a static lakehouse table contract.
    ///
    /// # Errors
    /// Returns `SparkRunSummaryError` when schema, target, lineage, rows, quality metrics, or
    /// column declarations do not match the foundation-platform contract.
    pub fn validate_for_contract(
        &self,
        contract: &LakehouseTableContract,
    ) -> Result<(), SparkRunSummaryError> {
        self.validate_schema_version()?;
        self.validate_input()?;
        self.validate_contract_name(contract)?;
        self.validate_columns(contract)?;
        self.validate_write_disposition()?;
        self.validate_target(contract)?;
        self.validate_row_counts()?;
        self.validate_source_lineage()?;
        self.validate_quality_metrics(contract)
    }

    fn validate_schema_version(&self) -> Result<(), SparkRunSummaryError> {
        if self.schema_version == SPARK_RUN_SUMMARY_SCHEMA_VERSION {
            return Ok(());
        }

        Err(SparkRunSummaryError::UnsupportedSchemaVersion {
            expected: SPARK_RUN_SUMMARY_SCHEMA_VERSION,
            actual: self.schema_version.clone(),
        })
    }

    fn validate_input(&self) -> Result<(), SparkRunSummaryError> {
        if !matches!(
            self.input.kind.as_str(),
            "bronze_jsonl" | "silver_handoff_jsonl" | "silver_handoff_parquet"
        ) {
            return Err(SparkRunSummaryError::InvalidInput {
                reason: format!("unsupported input kind {}", self.input.kind),
            });
        }
        if self.input.path.trim().is_empty() {
            return Err(SparkRunSummaryError::InvalidInput {
                reason: "input path must not be empty".to_owned(),
            });
        }
        Ok(())
    }

    fn validate_contract_name(
        &self,
        contract: &LakehouseTableContract,
    ) -> Result<(), SparkRunSummaryError> {
        if self.contract == contract.table_name {
            return Ok(());
        }

        Err(SparkRunSummaryError::ContractMismatch {
            expected: contract.table_name.to_owned(),
            actual: self.contract.clone(),
        })
    }

    fn validate_columns(
        &self,
        contract: &LakehouseTableContract,
    ) -> Result<(), SparkRunSummaryError> {
        let expected_columns = column_names(contract);
        if self.column_count != self.columns.len() {
            return Err(SparkRunSummaryError::ColumnCountMismatch {
                declared: self.column_count,
                actual: self.columns.len(),
            });
        }
        if self.columns != expected_columns {
            return Err(SparkRunSummaryError::ColumnContractMismatch {
                expected: expected_columns,
                actual: self.columns.clone(),
            });
        }

        let expected_required_columns = required_column_names(contract);
        if self.required_columns != expected_required_columns {
            return Err(SparkRunSummaryError::RequiredColumnContractMismatch {
                expected: expected_required_columns,
                actual: self.required_columns.clone(),
            });
        }

        Ok(())
    }

    const fn validate_write_disposition(&self) -> Result<(), SparkRunSummaryError> {
        let valid = matches!(
            (self.write_mode, self.write_disposition),
            (
                SparkRunWriteMode::Parquet,
                SparkRunWriteDisposition::ValidateOnly | SparkRunWriteDisposition::ParquetOverwrite
            ) | (
                SparkRunWriteMode::Iceberg,
                SparkRunWriteDisposition::ValidateOnly
                    | SparkRunWriteDisposition::IcebergAppend
                    | SparkRunWriteDisposition::IcebergOverwrite
            )
        );

        if valid {
            Ok(())
        } else {
            Err(SparkRunSummaryError::WriteDispositionMismatch {
                write_mode: self.write_mode,
                write_disposition: self.write_disposition,
            })
        }
    }

    fn validate_target(
        &self,
        contract: &LakehouseTableContract,
    ) -> Result<(), SparkRunSummaryError> {
        match (&self.target, self.write_mode) {
            (SparkRunTarget::Parquet { path }, SparkRunWriteMode::Parquet) => {
                if path.trim().is_empty() {
                    return Err(SparkRunSummaryError::InvalidTarget {
                        reason: "parquet target path must not be empty".to_owned(),
                    });
                }
                Ok(())
            }
            (
                SparkRunTarget::Iceberg {
                    catalog,
                    namespace,
                    table,
                    qualified_table,
                },
                SparkRunWriteMode::Iceberg,
            ) => {
                validate_non_empty_target_part("catalog", catalog)?;
                validate_non_empty_target_part("namespace", namespace)?;
                validate_non_empty_target_part("table", table)?;

                let expected_qualified_table = format!("{catalog}.{namespace}.{table}");
                if qualified_table != &expected_qualified_table {
                    return Err(SparkRunSummaryError::InvalidTarget {
                        reason: format!(
                            "qualified table {qualified_table} did not match {expected_qualified_table}"
                        ),
                    });
                }

                let target_contract = format!("{namespace}.{table}");
                if !target_matches_contract(&target_contract, contract, self.write_disposition) {
                    return Err(SparkRunSummaryError::ContractMismatch {
                        expected: contract.table_name.to_owned(),
                        actual: target_contract,
                    });
                }

                Ok(())
            }
            _ => Err(SparkRunSummaryError::TargetWriteModeMismatch {
                write_mode: self.write_mode,
            }),
        }
    }

    fn validate_row_counts(&self) -> Result<(), SparkRunSummaryError> {
        let metric_row_count = self.require_quality_metric("row_count")?;
        if metric_row_count != self.row_count {
            return Err(SparkRunSummaryError::RowCountMetricMismatch {
                row_count: self.row_count,
                metric_row_count,
            });
        }

        match self.write_disposition {
            SparkRunWriteDisposition::ValidateOnly => {
                if self.persisted_row_count.is_some() {
                    return Err(SparkRunSummaryError::UnexpectedPersistedRowCount);
                }
            }
            SparkRunWriteDisposition::ParquetOverwrite
            | SparkRunWriteDisposition::IcebergAppend
            | SparkRunWriteDisposition::IcebergOverwrite => {
                let persisted = self
                    .persisted_row_count
                    .ok_or(SparkRunSummaryError::MissingPersistedRowCount)?;
                if persisted != self.row_count {
                    return Err(SparkRunSummaryError::PersistedRowCountMismatch {
                        row_count: self.row_count,
                        persisted_row_count: persisted,
                    });
                }
            }
        }

        Ok(())
    }

    fn validate_source_lineage(&self) -> Result<(), SparkRunSummaryError> {
        if self.source_snapshot_truncated {
            return Err(SparkRunSummaryError::TruncatedSourceLineage);
        }
        if self.source_snapshot_count == 0 {
            return Err(SparkRunSummaryError::EmptySourceLineage);
        }
        if self.source_snapshot_ids.len() as u64 != self.source_snapshot_count {
            return Err(SparkRunSummaryError::SourceSnapshotCountMismatch {
                declared: self.source_snapshot_count,
                actual: self.source_snapshot_ids.len(),
            });
        }
        if self
            .source_snapshot_ids
            .iter()
            .any(|snapshot_id| snapshot_id.trim().is_empty())
        {
            return Err(SparkRunSummaryError::EmptySourceSnapshotId);
        }
        Ok(())
    }

    fn validate_quality_metrics(
        &self,
        contract: &LakehouseTableContract,
    ) -> Result<(), SparkRunSummaryError> {
        for metric in required_quality_metric_names(contract) {
            let value = self.require_quality_metric(&metric)?;
            if !allows_nonzero_required_metric(&metric) && value > 0 {
                return Err(SparkRunSummaryError::BlockingQualityMetric { metric, value });
            }
        }

        for (metric, value) in &self.quality_metrics {
            if is_blocking_quality_metric(metric) && *value > 0 {
                return Err(SparkRunSummaryError::BlockingQualityMetric {
                    metric: metric.clone(),
                    value: *value,
                });
            }
        }

        Ok(())
    }

    fn require_quality_metric(&self, metric: &str) -> Result<u64, SparkRunSummaryError> {
        self.quality_metrics
            .get(metric)
            .copied()
            .ok_or_else(|| SparkRunSummaryError::MissingQualityMetric(metric.to_owned()))
    }
}

/// Errors raised while validating a Spark run summary handoff.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum SparkRunSummaryError {
    /// JSON parsing failed.
    #[error("spark run summary JSON error: {0}")]
    Json(String),

    /// Summary schema version is not supported.
    #[error("unsupported spark run summary schema version {actual}; expected {expected}")]
    UnsupportedSchemaVersion {
        /// Expected schema version.
        expected: &'static str,
        /// Actual schema version.
        actual: String,
    },

    /// Summary contract name does not match the static contract.
    #[error("spark run summary contract mismatch: expected {expected}, actual {actual}")]
    ContractMismatch {
        /// Expected table contract.
        expected: String,
        /// Actual summary contract.
        actual: String,
    },

    /// Declared column count does not match the declared columns list.
    #[error("spark run summary column count mismatch: declared {declared}, actual {actual}")]
    ColumnCountMismatch {
        /// Declared column count.
        declared: usize,
        /// Actual number of column names.
        actual: usize,
    },

    /// Declared columns do not match the static table contract.
    #[error("spark run summary columns do not match table contract")]
    ColumnContractMismatch {
        /// Expected column names.
        expected: Vec<String>,
        /// Actual column names.
        actual: Vec<String>,
    },

    /// Declared required columns do not match the static table contract.
    #[error("spark run summary required columns do not match table contract")]
    RequiredColumnContractMismatch {
        /// Expected required column names.
        expected: Vec<String>,
        /// Actual required column names.
        actual: Vec<String>,
    },

    /// Input descriptor is invalid.
    #[error("invalid spark run summary input: {reason}")]
    InvalidInput {
        /// Human-readable reason.
        reason: String,
    },

    /// Target descriptor is invalid.
    #[error("invalid spark run summary target: {reason}")]
    InvalidTarget {
        /// Human-readable reason.
        reason: String,
    },

    /// Target kind and write mode disagree.
    #[error("spark run summary target kind does not match write mode {write_mode:?}")]
    TargetWriteModeMismatch {
        /// Declared write mode.
        write_mode: SparkRunWriteMode,
    },

    /// Write disposition does not belong to the declared write mode.
    #[error(
        "spark run summary write disposition {write_disposition:?} does not match mode {write_mode:?}"
    )]
    WriteDispositionMismatch {
        /// Declared write mode.
        write_mode: SparkRunWriteMode,
        /// Declared write disposition.
        write_disposition: SparkRunWriteDisposition,
    },

    /// Required quality metric is absent.
    #[error("spark run summary missing quality metric {0}")]
    MissingQualityMetric(String),

    /// Summary row count does not match the quality metric row count.
    #[error(
        "spark run summary row_count {row_count} differs from quality metric {metric_row_count}"
    )]
    RowCountMetricMismatch {
        /// Top-level row count.
        row_count: u64,
        /// Quality metric row count.
        metric_row_count: u64,
    },

    /// Validate-only summaries must not report persisted rows.
    #[error("validate-only spark run summary unexpectedly reported persisted rows")]
    UnexpectedPersistedRowCount,

    /// Write summaries must report persisted row count.
    #[error("spark run summary is missing persisted row count")]
    MissingPersistedRowCount,

    /// Persisted row count does not match candidate row count.
    #[error("persisted row count {persisted_row_count} differs from row_count {row_count}")]
    PersistedRowCountMismatch {
        /// Candidate row count.
        row_count: u64,
        /// Persisted row count.
        persisted_row_count: u64,
    },

    /// Source lineage contains no source snapshots.
    #[error("spark run summary source lineage is empty")]
    EmptySourceLineage,

    /// Source lineage includes an empty snapshot id.
    #[error("spark run summary source lineage includes an empty snapshot id")]
    EmptySourceSnapshotId,

    /// Source lineage was truncated by the execution engine.
    #[error("spark run summary source lineage was truncated")]
    TruncatedSourceLineage,

    /// Source snapshot count does not match the ids list.
    #[error("source snapshot count mismatch: declared {declared}, actual {actual}")]
    SourceSnapshotCountMismatch {
        /// Declared source snapshot count.
        declared: u64,
        /// Actual number of source snapshot ids.
        actual: usize,
    },

    /// A blocking quality metric is non-zero.
    #[error("blocking quality metric {metric} is non-zero: {value}")]
    BlockingQualityMetric {
        /// Metric name.
        metric: String,
        /// Metric value.
        value: u64,
    },
}

fn validate_non_empty_target_part(label: &str, value: &str) -> Result<(), SparkRunSummaryError> {
    if value.trim().is_empty() {
        return Err(SparkRunSummaryError::InvalidTarget {
            reason: format!("{label} must not be empty"),
        });
    }
    Ok(())
}

fn target_matches_contract(
    target_contract: &str,
    contract: &LakehouseTableContract,
    write_disposition: SparkRunWriteDisposition,
) -> bool {
    if target_contract == contract.table_name {
        return true;
    }
    if write_disposition != SparkRunWriteDisposition::IcebergOverwrite {
        return false;
    }
    smoke_contract_name(contract).is_some_and(|smoke_contract| target_contract == smoke_contract)
}

fn smoke_contract_name(contract: &LakehouseTableContract) -> Option<String> {
    let (namespace, table) = contract.table_name.split_once('.')?;
    Some(format!("{namespace}.{table}_smoke"))
}

fn column_names(contract: &LakehouseTableContract) -> Vec<String> {
    contract
        .columns
        .iter()
        .map(|column| column.name.to_owned())
        .collect()
}

fn required_column_names(contract: &LakehouseTableContract) -> Vec<String> {
    contract
        .columns
        .iter()
        .filter(|column| column.required)
        .map(|column| column.name.to_owned())
        .collect()
}

fn required_quality_metric_names(contract: &LakehouseTableContract) -> Vec<String> {
    let mut names = Vec::from(["row_count".to_owned()]);

    for column in contract.columns.iter().filter(|column| column.required) {
        names.push(format!("{}__null_count", column.name));
        if column.logical_type == "string" {
            names.push(format!("{}__empty_count", column.name));
        }
    }

    if contract.table_name == SILVER_INDUSTRIAL_COMPLEXES.table_name {
        names.extend(
            [
                "invalid_complex_kind_count",
                "invalid_status_count",
                "invalid_official_area_count",
                "invalid_complex_id_count",
                "invalid_checksum_count",
            ]
            .into_iter()
            .map(str::to_owned),
        );
    }
    if contract.table_name == SILVER_PARCEL_BOUNDARIES.table_name {
        names.extend(
            [
                "invalid_pnu_count",
                "invalid_code_derivation_count",
                "invalid_geometry_srid_count",
                "invalid_geometry_encoding_count",
                "invalid_geometry_wkb_hex_count",
                "invalid_geometry_wkb_count",
                "invalid_bbox_count",
                "invalid_checksum_count",
                "duplicate_active_pnu_count",
            ]
            .into_iter()
            .map(str::to_owned),
        );
    }
    if contract.table_name == SILVER_BUILDING_REGISTER_FLOORS.table_name
        || contract.table_name == SILVER_BUILDING_REGISTER_UNITS.table_name
    {
        names.extend(
            ["proposal_required_count", "invalid_checksum_count"]
                .into_iter()
                .map(str::to_owned),
        );
    }

    names
}

fn is_blocking_quality_metric(metric: &str) -> bool {
    metric.ends_with("__null_count")
        || metric.ends_with("__empty_count")
        || metric.starts_with("invalid_")
}

fn allows_nonzero_required_metric(metric: &str) -> bool {
    matches!(metric, "row_count" | "proposal_required_count")
}
