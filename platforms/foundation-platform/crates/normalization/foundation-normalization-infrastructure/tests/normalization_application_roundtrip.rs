//! `PostgreSQL` round-trip tests for normalization apply and rollback persistence.

#[path = "support/building_register_unit_roundtrip.rs"]
mod building_register_unit_roundtrip;
#[path = "support/industrial_complex_fixture.rs"]
mod industrial_complex_fixture;
#[allow(dead_code)]
mod support;

use catalog_application::ports::CatalogUnitOfWork;
use catalog_domain::ComplexMutation;
use catalog_infrastructure::PgCatalogUnitOfWork;
use foundation_normalization_application::{
    NormalizationApplicationCommand, NormalizationRollbackCommand, NormalizationUnitOfWork,
};
use foundation_normalization_domain::{NormalizationError, NormalizationTargetKind};
use foundation_normalization_infrastructure::PgNormalizationUnitOfWork;
use foundation_shared_kernel::ids::PrincipalId;
use serde_json::{json, Value as JsonValue};
use sqlx::Row;
use uuid::Uuid;

use building_register_unit_roundtrip::apply_building_register_unit_proposal;
use industrial_complex_fixture::{
    approve_fixture, create_pending_fixture, load_complex_scope_state, load_transaction_state,
    submit_and_approve,
};
use support::{run_in_disposable_database, TestResult};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../migrations");

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn approved_building_register_unit_proposal_applies_to_ledger() -> TestResult {
    run_in_disposable_database("normalization_unit_apply", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let normalization_uow = PgNormalizationUnitOfWork::new(pool.clone());
        let reviewer = PrincipalId::new(Uuid::now_v7());
        let (_, application_id) =
            apply_building_register_unit_proposal(&pool, &normalization_uow, reviewer).await?;

        let row = sqlx::query(
            "SELECT command_type, target_kind, target_id, before_snapshot, after_snapshot
             FROM catalog.normalization_application
             WHERE id = $1",
        )
        .bind(application_id)
        .fetch_one(&pool)
        .await?;
        let command_type: String = row.try_get("command_type")?;
        let target_kind: String = row.try_get("target_kind")?;
        let target_id: Option<Uuid> = row.try_get("target_id")?;
        let before_snapshot: JsonValue = row.try_get("before_snapshot")?;
        let after_snapshot: JsonValue = row.try_get("after_snapshot")?;

        assert_eq!(
            command_type,
            "building_register_unit.normalization.apply.v1"
        );
        assert_eq!(target_kind, "building_register_unit");
        assert_eq!(target_id, None);
        assert_eq!(before_snapshot["active_override"], JsonValue::Null);
        assert_eq!(
            after_snapshot["proposed_record"]["normalization_status"],
            "proposal_required"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn approved_building_register_unit_application_rolls_back_with_compensating_row() -> TestResult
{
    run_in_disposable_database("normalization_unit_rollback", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let normalization_uow = PgNormalizationUnitOfWork::new(pool.clone());
        let reviewer = PrincipalId::new(Uuid::now_v7());
        let (proposal_id, application_id) =
            apply_building_register_unit_proposal(&pool, &normalization_uow, reviewer).await?;

        let rollback_id = Uuid::now_v7();
        let rolled_back = normalization_uow
            .rollback_normalization_application(NormalizationRollbackCommand {
                id: rollback_id,
                application_id,
                expected_current_version: 1,
                reason: "operator rejected unit override after review".to_owned(),
                rolled_back_by_principal_id: reviewer,
            })
            .await?;
        assert_eq!(rolled_back.rollback_of, application_id);
        assert_eq!(
            rolled_back.target_kind,
            NormalizationTargetKind::BuildingRegisterUnit
        );
        assert_eq!(rolled_back.target_id, None);

        let row = sqlx::query(
            "SELECT command_type, target_kind, target_id, rollback_of, after_snapshot
             FROM catalog.normalization_application
             WHERE id = $1",
        )
        .bind(rollback_id)
        .fetch_one(&pool)
        .await?;
        let command_type: String = row.try_get("command_type")?;
        let target_kind: String = row.try_get("target_kind")?;
        let target_id: Option<Uuid> = row.try_get("target_id")?;
        let rollback_of: Uuid = row.try_get("rollback_of")?;
        let after_snapshot: JsonValue = row.try_get("after_snapshot")?;

        assert_eq!(
            command_type,
            "building_register_unit.normalization.rollback.v1"
        );
        assert_eq!(target_kind, "building_register_unit");
        assert_eq!(target_id, None);
        assert_eq!(rollback_of, application_id);
        assert_eq!(after_snapshot["active_override"], JsonValue::Null);

        let rollback_count: i64 = sqlx::query_scalar(
            "SELECT count(*)
             FROM catalog.normalization_application
             WHERE rollback_of = $1",
        )
        .bind(application_id)
        .fetch_one(&pool)
        .await?;
        let proposal_status: String =
            sqlx::query_scalar("SELECT status FROM catalog.normalization_proposal WHERE id = $1")
                .bind(proposal_id)
                .fetch_one(&pool)
                .await?;
        assert_eq!(rollback_count, 1);
        assert_eq!(proposal_status, "rolled_back");
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn approved_proposal_apply_and_rollback_are_atomic_db_roundtrip() -> TestResult {
    run_in_disposable_database("normalization_complex_roundtrip", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let fixture = create_pending_fixture(&pool).await?;
        approve_fixture(&fixture).await?;
        let application_id = Uuid::now_v7();

        let applied = fixture
            .normalization_uow
            .apply_normalization_proposal(NormalizationApplicationCommand {
                id: application_id,
                proposal_id: fixture.proposal_id,
                expected_version: 1,
                applied_by_principal_id: fixture.principal_id,
            })
            .await?;
        assert_eq!(applied.target_id, Some(fixture.complex.id.as_uuid()));

        let after_apply = load_transaction_state(&pool, &fixture).await?;
        assert_eq!(
            after_apply.canonical_row["name"],
            json!("normalized industrial complex")
        );
        assert_eq!(after_apply.canonical_row["area_m2"], json!(987_654));
        assert_eq!(after_apply.canonical_row["version"], json!(2));
        assert_eq!(after_apply.proposal_row["status"], json!("applied"));

        let rollback = fixture
            .normalization_uow
            .rollback_normalization_application(NormalizationRollbackCommand {
                id: Uuid::now_v7(),
                application_id,
                expected_current_version: 2,
                reason: "roundtrip smoke rollback".to_owned(),
                rolled_back_by_principal_id: fixture.principal_id,
            })
            .await?;
        assert_eq!(rollback.rollback_of, application_id);
        assert_eq!(rollback.target_id, Some(fixture.complex.id.as_uuid()));

        let after_rollback = load_transaction_state(&pool, &fixture).await?;
        assert_eq!(
            after_rollback.canonical_row["name"],
            json!(fixture.complex.name)
        );
        assert_eq!(
            after_rollback.canonical_row["area_m2"],
            json!(fixture.complex.area_m2)
        );
        assert_eq!(after_rollback.canonical_row["version"], json!(3));
        assert_eq!(after_rollback.proposal_row["status"], json!("rolled_back"));
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn stale_target_version_rejects_apply_without_changing_transaction_state() -> TestResult {
    run_in_disposable_database("normalization_target_conflict", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let fixture = create_pending_fixture(&pool).await?;
        approve_fixture(&fixture).await?;
        let before = load_transaction_state(&pool, &fixture).await?;

        let result = fixture
            .normalization_uow
            .apply_normalization_proposal(NormalizationApplicationCommand {
                id: Uuid::now_v7(),
                proposal_id: fixture.proposal_id,
                expected_version: 9,
                applied_by_principal_id: fixture.principal_id,
            })
            .await;

        assert!(matches!(
            result,
            Err(NormalizationError::TargetVersionConflict {
                expected: 9,
                current: 1
            })
        ));
        let after = load_transaction_state(&pool, &fixture).await?;
        assert_eq!(after, before);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn duplicate_rollback_is_rejected_without_replaying_canonical_or_audit_writes() -> TestResult
{
    run_in_disposable_database("normalization_duplicate_rollback", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let fixture = create_pending_fixture(&pool).await?;
        approve_fixture(&fixture).await?;
        let application_id = Uuid::now_v7();
        fixture
            .normalization_uow
            .apply_normalization_proposal(NormalizationApplicationCommand {
                id: application_id,
                proposal_id: fixture.proposal_id,
                expected_version: 1,
                applied_by_principal_id: fixture.principal_id,
            })
            .await?;
        fixture
            .normalization_uow
            .rollback_normalization_application(NormalizationRollbackCommand {
                id: Uuid::now_v7(),
                application_id,
                expected_current_version: 2,
                reason: "first rollback".to_owned(),
                rolled_back_by_principal_id: fixture.principal_id,
            })
            .await?;
        let before_retry = load_transaction_state(&pool, &fixture).await?;

        let result = fixture
            .normalization_uow
            .rollback_normalization_application(NormalizationRollbackCommand {
                id: Uuid::now_v7(),
                application_id,
                expected_current_version: 3,
                reason: "duplicate rollback".to_owned(),
                rolled_back_by_principal_id: fixture.principal_id,
            })
            .await;

        assert!(
            matches!(
                result,
                Err(NormalizationError::InvalidState(ref message))
                    if message == "normalization application is already rolled back"
            ),
            "unexpected duplicate rollback result: {result:?}"
        );
        let after_retry = load_transaction_state(&pool, &fixture).await?;
        assert_eq!(after_retry, before_retry);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn non_lifo_industrial_complex_rollback_rejects_later_disjoint_changes() -> TestResult {
    run_in_disposable_database("normalization_complex_non_lifo", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let fixture = create_pending_fixture(&pool).await?;
        let proposal_a =
            submit_and_approve(&pool, &fixture, json!({"name":"first approved name"})).await?;
        let proposal_b = submit_and_approve(&pool, &fixture, json!({"area_m2":654_321})).await?;
        let application_a = Uuid::now_v7();
        let application_b = Uuid::now_v7();
        fixture
            .normalization_uow
            .apply_normalization_proposal(NormalizationApplicationCommand {
                id: application_a,
                proposal_id: proposal_a,
                expected_version: 1,
                applied_by_principal_id: fixture.principal_id,
            })
            .await?;
        fixture
            .normalization_uow
            .apply_normalization_proposal(NormalizationApplicationCommand {
                id: application_b,
                proposal_id: proposal_b,
                expected_version: 2,
                applied_by_principal_id: fixture.principal_id,
            })
            .await?;
        let before = load_complex_scope_state(&pool, fixture.complex.id.to_string()).await?;
        let result = fixture
            .normalization_uow
            .rollback_normalization_application(NormalizationRollbackCommand {
                id: Uuid::now_v7(),
                application_id: application_a,
                expected_current_version: 3,
                reason: "must not erase later approved area".to_owned(),
                rolled_back_by_principal_id: fixture.principal_id,
            })
            .await;

        assert!(matches!(
            result,
            Err(NormalizationError::TargetStateConflict(ref id))
                if id == &fixture.complex.id.to_string()
        ));
        assert_eq!(
            load_complex_scope_state(&pool, fixture.complex.id.to_string()).await?,
            before
        );
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn non_lifo_industrial_complex_rollback_rejects_overlapping_later_changes() -> TestResult {
    run_in_disposable_database("normalization_complex_overlap", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let fixture = create_pending_fixture(&pool).await?;
        let proposal_a =
            submit_and_approve(&pool, &fixture, json!({"name":"first approved name"})).await?;
        let proposal_b =
            submit_and_approve(&pool, &fixture, json!({"name":"later approved name"})).await?;
        let application_a = Uuid::now_v7();
        fixture
            .normalization_uow
            .apply_normalization_proposal(NormalizationApplicationCommand {
                id: application_a,
                proposal_id: proposal_a,
                expected_version: 1,
                applied_by_principal_id: fixture.principal_id,
            })
            .await?;
        fixture
            .normalization_uow
            .apply_normalization_proposal(NormalizationApplicationCommand {
                id: Uuid::now_v7(),
                proposal_id: proposal_b,
                expected_version: 2,
                applied_by_principal_id: fixture.principal_id,
            })
            .await?;
        let before = load_complex_scope_state(&pool, fixture.complex.id.to_string()).await?;

        let result = fixture
            .normalization_uow
            .rollback_normalization_application(NormalizationRollbackCommand {
                id: Uuid::now_v7(),
                application_id: application_a,
                expected_current_version: 3,
                reason: "must not erase later approved name".to_owned(),
                rolled_back_by_principal_id: fixture.principal_id,
            })
            .await;

        assert!(matches!(
            result,
            Err(NormalizationError::TargetStateConflict(ref id))
                if id == &fixture.complex.id.to_string()
        ));
        assert_eq!(
            load_complex_scope_state(&pool, fixture.complex.id.to_string()).await?,
            before
        );
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn industrial_complex_rollback_rejects_aba_overlapping_changes() -> TestResult {
    run_in_disposable_database("normalization_complex_aba", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let fixture = create_pending_fixture(&pool).await?;
        let proposal_a =
            submit_and_approve(&pool, &fixture, json!({"name":"first approved name"})).await?;
        let proposal_b =
            submit_and_approve(&pool, &fixture, json!({"name":"intermediate name"})).await?;
        let proposal_c =
            submit_and_approve(&pool, &fixture, json!({"name":"first approved name"})).await?;
        let application_a = Uuid::now_v7();
        fixture
            .normalization_uow
            .apply_normalization_proposal(NormalizationApplicationCommand {
                id: application_a,
                proposal_id: proposal_a,
                expected_version: 1,
                applied_by_principal_id: fixture.principal_id,
            })
            .await?;
        fixture
            .normalization_uow
            .apply_normalization_proposal(NormalizationApplicationCommand {
                id: Uuid::now_v7(),
                proposal_id: proposal_b,
                expected_version: 2,
                applied_by_principal_id: fixture.principal_id,
            })
            .await?;
        fixture
            .normalization_uow
            .apply_normalization_proposal(NormalizationApplicationCommand {
                id: Uuid::now_v7(),
                proposal_id: proposal_c,
                expected_version: 3,
                applied_by_principal_id: fixture.principal_id,
            })
            .await?;
        let before = load_complex_scope_state(&pool, fixture.complex.id.to_string()).await?;

        let result = fixture
            .normalization_uow
            .rollback_normalization_application(NormalizationRollbackCommand {
                id: Uuid::now_v7(),
                application_id: application_a,
                expected_current_version: 4,
                reason: "must reject an ABA overlap".to_owned(),
                rolled_back_by_principal_id: fixture.principal_id,
            })
            .await;

        assert!(matches!(
            result,
            Err(NormalizationError::TargetStateConflict(ref id))
                if id == &fixture.complex.id.to_string()
        ));
        assert_eq!(
            load_complex_scope_state(&pool, fixture.complex.id.to_string()).await?,
            before
        );
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn industrial_complex_rollbacks_unwind_the_active_stack_in_lifo_order() -> TestResult {
    run_in_disposable_database("normalization_complex_lifo", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let fixture = create_pending_fixture(&pool).await?;
        let original_name = fixture.complex.name.clone();
        let original_area = fixture.complex.area_m2;
        let proposal_a =
            submit_and_approve(&pool, &fixture, json!({"name":"first approved name"})).await?;
        let proposal_b = submit_and_approve(&pool, &fixture, json!({"area_m2":654_321})).await?;
        let application_a = Uuid::now_v7();
        let application_b = Uuid::now_v7();

        fixture
            .normalization_uow
            .apply_normalization_proposal(NormalizationApplicationCommand {
                id: application_a,
                proposal_id: proposal_a,
                expected_version: 1,
                applied_by_principal_id: fixture.principal_id,
            })
            .await?;
        fixture
            .normalization_uow
            .apply_normalization_proposal(NormalizationApplicationCommand {
                id: application_b,
                proposal_id: proposal_b,
                expected_version: 2,
                applied_by_principal_id: fixture.principal_id,
            })
            .await?;
        fixture
            .normalization_uow
            .rollback_normalization_application(NormalizationRollbackCommand {
                id: Uuid::now_v7(),
                application_id: application_b,
                expected_current_version: 3,
                reason: "undo latest area normalization".to_owned(),
                rolled_back_by_principal_id: fixture.principal_id,
            })
            .await?;
        fixture
            .normalization_uow
            .rollback_normalization_application(NormalizationRollbackCommand {
                id: Uuid::now_v7(),
                application_id: application_a,
                expected_current_version: 4,
                reason: "undo preceding name normalization".to_owned(),
                rolled_back_by_principal_id: fixture.principal_id,
            })
            .await?;

        let state = load_complex_scope_state(&pool, fixture.complex.id.to_string()).await?;
        assert_eq!(state["canonical"]["name"], json!(original_name));
        assert_eq!(state["canonical"]["area_m2"], json!(original_area));
        assert_eq!(state["canonical"]["version"], json!(5));
        assert_eq!(state["applications"].as_array().map(Vec::len), Some(4));
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn industrial_complex_rollback_rejects_external_change_after_partial_unwind() -> TestResult {
    run_in_disposable_database(
        "normalization_complex_external_after_unwind",
        |pool| async move {
            MIGRATOR.run(&pool).await?;
            let fixture = create_pending_fixture(&pool).await?;
            let proposal_a =
                submit_and_approve(&pool, &fixture, json!({"name":"first approved name"})).await?;
            let proposal_b =
                submit_and_approve(&pool, &fixture, json!({"area_m2":654_321})).await?;
            let application_a = Uuid::now_v7();
            let application_b = Uuid::now_v7();

            fixture
                .normalization_uow
                .apply_normalization_proposal(NormalizationApplicationCommand {
                    id: application_a,
                    proposal_id: proposal_a,
                    expected_version: 1,
                    applied_by_principal_id: fixture.principal_id,
                })
                .await?;
            fixture
                .normalization_uow
                .apply_normalization_proposal(NormalizationApplicationCommand {
                    id: application_b,
                    proposal_id: proposal_b,
                    expected_version: 2,
                    applied_by_principal_id: fixture.principal_id,
                })
                .await?;
            fixture
                .normalization_uow
                .rollback_normalization_application(NormalizationRollbackCommand {
                    id: Uuid::now_v7(),
                    application_id: application_b,
                    expected_current_version: 3,
                    reason: "undo latest area normalization".to_owned(),
                    rolled_back_by_principal_id: fixture.principal_id,
                })
                .await?;

            PgCatalogUnitOfWork::new(pool.clone())
                .update_complex(
                    fixture.complex.id,
                    4,
                    ComplexMutation {
                        name: None,
                        area_m2: Some(777_777),
                    },
                )
                .await?;
            let before = load_complex_scope_state(&pool, fixture.complex.id.to_string()).await?;

            let result = fixture
                .normalization_uow
                .rollback_normalization_application(NormalizationRollbackCommand {
                    id: Uuid::now_v7(),
                    application_id: application_a,
                    expected_current_version: 5,
                    reason: "must not erase a later catalog mutation".to_owned(),
                    rolled_back_by_principal_id: fixture.principal_id,
                })
                .await;

            assert!(matches!(
                result,
                Err(NormalizationError::TargetStateConflict(ref id))
                    if id == &fixture.complex.id.to_string()
            ));
            assert_eq!(
                load_complex_scope_state(&pool, fixture.complex.id.to_string()).await?,
                before
            );
            Ok(())
        },
    )
    .await
}
