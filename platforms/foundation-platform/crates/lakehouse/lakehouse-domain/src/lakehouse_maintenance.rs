//! Pure lakehouse maintenance planning contracts.
//!
//! This module decides whether an Iceberg table snapshot needs maintenance work. It does not know
//! about Spark, Cloudflare, R2, SQL, or worker queues; infrastructure adapters can execute the
//! returned actions later.

use thiserror::Error;

use crate::{LakehousePhysicalFormat, LakehouseTableContract};

/// Fixed-point percentage with two decimal places.
///
/// `10_000` means 100.00%. Ratios are represented this way to avoid float thresholds in domain
/// decisions.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct BasisPoints(u16);

impl BasisPoints {
    /// 0.00%.
    pub const ZERO: Self = Self(0);
    /// 100.00%.
    pub const FULL: Self = Self(10_000);

    /// Creates a bounded basis-points value.
    ///
    /// # Errors
    /// Returns `LakehouseTableHealthError::InvalidBasisPoints` when `value` exceeds 100.00%.
    pub const fn new(value: u16) -> Result<Self, LakehouseTableHealthError> {
        if value <= Self::FULL.0 {
            Ok(Self(value))
        } else {
            Err(LakehouseTableHealthError::InvalidBasisPoints(value))
        }
    }

    /// Computes `numerator / denominator` as basis points.
    #[must_use]
    pub fn ratio(numerator: u64, denominator: u64) -> Self {
        if denominator == 0 {
            return Self::ZERO;
        }

        let capped = scaled_basis_points(numerator, denominator);
        u16::try_from(capped).map_or(Self::FULL, Self)
    }

    /// Raw basis-points value.
    #[must_use]
    pub const fn value(self) -> u16 {
        self.0
    }
}

fn scaled_basis_points(numerator: u64, denominator: u64) -> u64 {
    numerator
        .saturating_mul(u64::from(BasisPoints::FULL.0))
        .checked_div(denominator)
        .unwrap_or(0)
        .min(u64::from(BasisPoints::FULL.0))
}

/// File and metadata metrics observed for one Iceberg table snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LakehouseTableHealthMetrics {
    /// Number of data files in the snapshot.
    pub data_file_count: u64,
    /// Number of data files smaller than the policy threshold.
    pub small_file_count: u64,
    /// Average data file size in bytes.
    pub average_file_size_bytes: u64,
    /// Number of manifests referenced by the snapshot metadata.
    pub manifest_count: u32,
    /// Number of snapshots eligible for expiration.
    pub expired_snapshot_count: u32,
    /// Skew estimate for partition/file distribution.
    pub partition_skew: BasisPoints,
}

/// Health metrics observed for one Iceberg table snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LakehouseTableHealth {
    /// Fully qualified table name.
    pub table_name: String,
    /// Iceberg snapshot id these metrics describe.
    pub snapshot_id: String,
    /// File and metadata metrics.
    pub metrics: LakehouseTableHealthMetrics,
}

impl LakehouseTableHealth {
    /// Creates validated table health metrics.
    ///
    /// # Errors
    /// Returns `LakehouseTableHealthError` when names are empty or counts are contradictory.
    pub fn new(
        table_name: impl Into<String>,
        snapshot_id: impl Into<String>,
        metrics: LakehouseTableHealthMetrics,
    ) -> Result<Self, LakehouseTableHealthError> {
        let table_name = table_name.into();
        let snapshot_id = snapshot_id.into();

        if table_name.trim().is_empty() {
            return Err(LakehouseTableHealthError::EmptyTableName);
        }
        if snapshot_id.trim().is_empty() {
            return Err(LakehouseTableHealthError::EmptySnapshotId);
        }
        if metrics.small_file_count > metrics.data_file_count {
            return Err(LakehouseTableHealthError::SmallFileCountExceedsDataFiles {
                small_file_count: metrics.small_file_count,
                data_file_count: metrics.data_file_count,
            });
        }

        Ok(Self {
            table_name,
            snapshot_id,
            metrics,
        })
    }

    /// Ratio of small files to total data files.
    #[must_use]
    pub fn small_file_ratio(&self) -> BasisPoints {
        BasisPoints::ratio(self.metrics.small_file_count, self.metrics.data_file_count)
    }
}

/// Maintenance thresholds applied by the planner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LakehouseMaintenancePolicy {
    /// Files below this size count as small files.
    pub small_file_threshold_bytes: u64,
    /// Maximum acceptable small-file ratio before compaction.
    pub max_small_file_ratio: BasisPoints,
    /// Maximum manifest count before manifest rewrite.
    pub max_manifest_count: u32,
    /// Maximum expired snapshots tolerated before cleanup.
    pub max_expired_snapshot_count: u32,
    /// Maximum partition/file skew before sort rewrite.
    pub max_partition_skew: BasisPoints,
}

impl Default for LakehouseMaintenancePolicy {
    fn default() -> Self {
        Self {
            small_file_threshold_bytes: 32 * 1024 * 1024,
            max_small_file_ratio: BasisPoints(2_000),
            max_manifest_count: 100,
            max_expired_snapshot_count: 24,
            max_partition_skew: BasisPoints(3_000),
        }
    }
}

/// Kind of maintenance work the infrastructure layer may execute.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LakehouseMaintenanceActionKind {
    /// Rewrite small files into fewer larger files.
    SmallFileCompaction,
    /// Rewrite files using the table sort order.
    SortRewrite,
    /// Rewrite manifests to reduce metadata planning cost.
    ManifestRewrite,
    /// Expire old snapshots after retention guards pass.
    SnapshotExpiration,
}

/// One planned maintenance action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LakehouseMaintenanceAction {
    /// Action kind.
    pub kind: LakehouseMaintenanceActionKind,
    /// Human-readable, deterministic reason.
    pub reason: String,
}

impl LakehouseMaintenanceAction {
    fn new(kind: LakehouseMaintenanceActionKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            reason: reason.into(),
        }
    }
}

/// Provider-neutral maintenance plan for one table snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LakehouseMaintenancePlan {
    /// Fully qualified table name.
    pub table_name: String,
    /// Iceberg snapshot id the plan was computed from.
    pub snapshot_id: String,
    /// Ordered maintenance actions.
    pub actions: Vec<LakehouseMaintenanceAction>,
}

impl LakehouseMaintenancePlan {
    /// Returns whether the snapshot has no blocking maintenance work.
    #[must_use]
    pub const fn is_promotion_safe(&self) -> bool {
        self.actions.is_empty()
    }
}

/// Errors raised while validating table health input.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum LakehouseTableHealthError {
    /// Table name is empty.
    #[error("lakehouse table health table name must not be empty")]
    EmptyTableName,

    /// Snapshot id is empty.
    #[error("lakehouse table health snapshot id must not be empty")]
    EmptySnapshotId,

    /// Small-file count cannot exceed total data file count.
    #[error("small file count {small_file_count} exceeds data file count {data_file_count}")]
    SmallFileCountExceedsDataFiles {
        /// Observed small-file count.
        small_file_count: u64,
        /// Observed data-file count.
        data_file_count: u64,
    },

    /// Basis-points values are bounded to 100.00%.
    #[error("basis points must be <= 10000, got {0}")]
    InvalidBasisPoints(u16),

    /// Health metrics belong to a different table than the contract.
    #[error("health table {health_table} does not match contract table {contract_table}")]
    TableNameMismatch {
        /// Table from the static contract.
        contract_table: String,
        /// Table from observed metrics.
        health_table: String,
    },
}

/// Builds a deterministic maintenance plan for one table snapshot.
///
/// # Errors
/// Returns `LakehouseTableHealthError::TableNameMismatch` when observed metrics do not belong to
/// the provided table contract.
pub fn plan_lakehouse_maintenance(
    contract: &LakehouseTableContract,
    health: &LakehouseTableHealth,
    policy: &LakehouseMaintenancePolicy,
) -> Result<LakehouseMaintenancePlan, LakehouseTableHealthError> {
    if contract.table_name != health.table_name {
        return Err(LakehouseTableHealthError::TableNameMismatch {
            contract_table: contract.table_name.to_owned(),
            health_table: health.table_name.clone(),
        });
    }

    let mut actions = Vec::new();
    let small_file_ratio = health.small_file_ratio();

    if health.metrics.small_file_count > 0
        && health.metrics.average_file_size_bytes < policy.small_file_threshold_bytes
        && small_file_ratio > policy.max_small_file_ratio
    {
        actions.push(LakehouseMaintenanceAction::new(
            LakehouseMaintenanceActionKind::SmallFileCompaction,
            format!(
                "small file ratio {}bp exceeds {}bp",
                small_file_ratio.value(),
                policy.max_small_file_ratio.value()
            ),
        ));
    }

    if contract.physical_format == LakehousePhysicalFormat::GeoParquet
        && !contract.sort_order.is_empty()
        && health.metrics.partition_skew > policy.max_partition_skew
    {
        actions.push(LakehouseMaintenanceAction::new(
            LakehouseMaintenanceActionKind::SortRewrite,
            format!(
                "partition skew {}bp exceeds {}bp",
                health.metrics.partition_skew.value(),
                policy.max_partition_skew.value()
            ),
        ));
    }

    if health.metrics.manifest_count > policy.max_manifest_count {
        actions.push(LakehouseMaintenanceAction::new(
            LakehouseMaintenanceActionKind::ManifestRewrite,
            format!(
                "manifest count {} exceeds {}",
                health.metrics.manifest_count, policy.max_manifest_count
            ),
        ));
    }

    if health.metrics.expired_snapshot_count > policy.max_expired_snapshot_count {
        actions.push(LakehouseMaintenanceAction::new(
            LakehouseMaintenanceActionKind::SnapshotExpiration,
            format!(
                "expired snapshot count {} exceeds {}",
                health.metrics.expired_snapshot_count, policy.max_expired_snapshot_count
            ),
        ));
    }

    Ok(LakehouseMaintenancePlan {
        table_name: contract.table_name.to_owned(),
        snapshot_id: health.snapshot_id.clone(),
        actions,
    })
}
