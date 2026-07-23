//! Contract tests for lakehouse maintenance planning.

use lakehouse_domain::{
    plan_lakehouse_maintenance, BasisPoints, LakehouseMaintenanceActionKind,
    LakehouseMaintenancePolicy, LakehouseTableHealth, LakehouseTableHealthError,
    LakehouseTableHealthMetrics, GOLD_COMPLEX_CATALOG, SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES,
};

fn healthy_gold_table() -> Result<LakehouseTableHealth, LakehouseTableHealthError> {
    LakehouseTableHealth::new(
        GOLD_COMPLEX_CATALOG.table_name,
        "snapshot-0001",
        LakehouseTableHealthMetrics {
            data_file_count: 512,
            small_file_count: 8,
            average_file_size_bytes: 134_217_728,
            manifest_count: 12,
            expired_snapshot_count: 2,
            partition_skew: BasisPoints::new(250)?,
        },
    )
}

#[test]
fn healthy_table_does_not_schedule_rewrite_work() -> Result<(), LakehouseTableHealthError> {
    let policy = LakehouseMaintenancePolicy::default();
    let health = healthy_gold_table()?;
    let plan = plan_lakehouse_maintenance(&GOLD_COMPLEX_CATALOG, &health, &policy)?;

    assert_eq!(plan.table_name, GOLD_COMPLEX_CATALOG.table_name);
    assert_eq!(plan.snapshot_id, "snapshot-0001");
    assert!(plan.actions.is_empty());
    assert!(plan.is_promotion_safe());
    Ok(())
}

#[test]
fn small_file_pressure_schedules_compaction_before_promotion(
) -> Result<(), LakehouseTableHealthError> {
    let health = LakehouseTableHealth::new(
        GOLD_COMPLEX_CATALOG.table_name,
        "snapshot-0002",
        LakehouseTableHealthMetrics {
            data_file_count: 1_000,
            small_file_count: 420,
            average_file_size_bytes: 16_777_216,
            manifest_count: 18,
            expired_snapshot_count: 2,
            partition_skew: BasisPoints::new(300)?,
        },
    )?;

    let plan = plan_lakehouse_maintenance(
        &GOLD_COMPLEX_CATALOG,
        &health,
        &LakehouseMaintenancePolicy::default(),
    )?;

    assert_eq!(plan.actions.len(), 1);
    assert_eq!(
        plan.actions[0].kind,
        LakehouseMaintenanceActionKind::SmallFileCompaction
    );
    assert!(!plan.is_promotion_safe());
    assert!(plan.actions[0].reason.contains("small file ratio"));
    Ok(())
}

#[test]
fn metadata_pressure_schedules_manifest_and_snapshot_cleanup(
) -> Result<(), LakehouseTableHealthError> {
    let health = LakehouseTableHealth::new(
        GOLD_COMPLEX_CATALOG.table_name,
        "snapshot-0003",
        LakehouseTableHealthMetrics {
            data_file_count: 1_000,
            small_file_count: 12,
            average_file_size_bytes: 134_217_728,
            manifest_count: 240,
            expired_snapshot_count: 80,
            partition_skew: BasisPoints::new(200)?,
        },
    )?;

    let plan = plan_lakehouse_maintenance(
        &GOLD_COMPLEX_CATALOG,
        &health,
        &LakehouseMaintenancePolicy::default(),
    )?;

    let action_kinds = plan
        .actions
        .iter()
        .map(|action| action.kind)
        .collect::<Vec<_>>();

    assert_eq!(
        action_kinds,
        vec![
            LakehouseMaintenanceActionKind::ManifestRewrite,
            LakehouseMaintenanceActionKind::SnapshotExpiration,
        ]
    );
    assert!(!plan.is_promotion_safe());
    Ok(())
}

#[test]
fn spatial_skew_schedules_sort_rewrite_for_geoparquet_tables(
) -> Result<(), LakehouseTableHealthError> {
    let health = LakehouseTableHealth::new(
        SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES.table_name,
        "snapshot-0004",
        LakehouseTableHealthMetrics {
            data_file_count: 700,
            small_file_count: 16,
            average_file_size_bytes: 134_217_728,
            manifest_count: 20,
            expired_snapshot_count: 2,
            partition_skew: BasisPoints::new(4_500)?,
        },
    )?;

    let plan = plan_lakehouse_maintenance(
        &SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES,
        &health,
        &LakehouseMaintenancePolicy::default(),
    )?;

    assert!(plan
        .actions
        .iter()
        .any(|action| action.kind == LakehouseMaintenanceActionKind::SortRewrite));
    Ok(())
}

#[test]
fn health_metrics_must_match_the_contract_table_name() -> Result<(), LakehouseTableHealthError> {
    let health = LakehouseTableHealth::new(
        "gold.other_table",
        "snapshot-0005",
        LakehouseTableHealthMetrics {
            data_file_count: 10,
            small_file_count: 0,
            average_file_size_bytes: 134_217_728,
            manifest_count: 1,
            expired_snapshot_count: 0,
            partition_skew: BasisPoints::ZERO,
        },
    )?;

    let result = plan_lakehouse_maintenance(
        &GOLD_COMPLEX_CATALOG,
        &health,
        &LakehouseMaintenancePolicy::default(),
    );

    assert_eq!(
        result,
        Err(LakehouseTableHealthError::TableNameMismatch {
            contract_table: GOLD_COMPLEX_CATALOG.table_name.to_owned(),
            health_table: "gold.other_table".to_owned(),
        })
    );
    Ok(())
}

#[test]
fn basis_points_reject_values_above_one_hundred_percent() {
    assert_eq!(
        BasisPoints::new(10_001),
        Err(LakehouseTableHealthError::InvalidBasisPoints(10_001))
    );
}
