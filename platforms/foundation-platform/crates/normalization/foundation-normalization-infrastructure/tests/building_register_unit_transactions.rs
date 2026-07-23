//! `PostgreSQL` concurrency and rollback-state contracts for unit normalization.

#[path = "support/building_register_unit_fixture.rs"]
#[allow(dead_code)]
mod building_register_unit_fixture;
mod support;

use std::sync::Arc;

use foundation_normalization_application::ActiveBuildingRegisterUnitOverrideReader;
use foundation_normalization_infrastructure::PgActiveBuildingRegisterUnitOverrideReader;
use foundation_shared_kernel::ids::PrincipalId;
use serde_json::{json, Value as JsonValue};
use tokio::sync::Barrier;
use uuid::Uuid;

use building_register_unit_fixture::{
    acquire_proposal_lock, acquire_target_lock, apply, assert_active_application,
    force_application_order, load_ledger, rollback, spawn_apply, submit_approved, target_identity,
    wait_for_advisory_waiters, wait_for_transaction_lock_waiters,
};
use support::{database_count_with_prefix, run_in_disposable_database, TestResult};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../migrations");

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn concurrent_first_apply_serializes_the_predecessor_chain() -> TestResult {
    let label = "normalization_unit_first_apply";
    run_in_disposable_database(label, |pool| async move {
        MIGRATOR.run(&pool).await?;
        let identity = target_identity("concurrent-first-apply");
        let principal = PrincipalId::new(Uuid::now_v7());
        let proposal_a = submit_approved(&pool, principal, &identity, 101).await?;
        let proposal_b = submit_approved(&pool, principal, &identity, 102).await?;
        let application_a = Uuid::now_v7();
        let application_b = Uuid::now_v7();

        let mut blocker = pool.begin().await?;
        acquire_target_lock(&mut blocker, &identity).await?;
        let barrier = Arc::new(Barrier::new(3));
        let apply_a = spawn_apply(
            pool.clone(),
            Arc::clone(&barrier),
            proposal_a,
            application_a,
            principal,
        );
        let apply_b = spawn_apply(
            pool.clone(),
            Arc::clone(&barrier),
            proposal_b,
            application_b,
            principal,
        );
        barrier.wait().await;
        wait_for_advisory_waiters(&pool, 2).await?;
        assert!(!apply_a.is_finished());
        assert!(!apply_b.is_finished());
        blocker.rollback().await?;

        apply_a.await??;
        apply_b.await??;
        let ledger_a = load_ledger(&pool, application_a).await?;
        let ledger_b = load_ledger(&pool, application_b).await?;
        let ledgers = [&ledger_a, &ledger_b];
        let roots = ledgers
            .iter()
            .filter(|ledger| ledger.before_snapshot["active_override"].is_null())
            .copied()
            .collect::<Vec<_>>();
        let successors = ledgers
            .iter()
            .filter(|ledger| !ledger.before_snapshot["active_override"].is_null())
            .copied()
            .collect::<Vec<_>>();

        assert_eq!(roots.len(), 1);
        assert_eq!(successors.len(), 1);
        assert_eq!(
            successors[0].before_snapshot["active_override"],
            roots[0].after_snapshot
        );
        Ok(())
    })
    .await?;
    assert_eq!(database_count_with_prefix(label).await?, 0);
    Ok(())
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn active_override_follows_serialized_chain_not_transaction_start_time() -> TestResult {
    run_in_disposable_database("normalization_unit_chain_order", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let identity = target_identity("serialized-chain-order");
        let principal = PrincipalId::new(Uuid::now_v7());
        let delayed_proposal = submit_approved(&pool, principal, &identity, 201).await?;
        let first_proposal = submit_approved(&pool, principal, &identity, 202).await?;
        let delayed_application = Uuid::now_v7();
        let first_application = Uuid::now_v7();

        let mut proposal_blocker = pool.begin().await?;
        acquire_proposal_lock(&mut proposal_blocker, delayed_proposal).await?;
        let delayed_apply = tokio::spawn({
            let pool = pool.clone();
            async move { apply(&pool, delayed_proposal, delayed_application, principal).await }
        });
        wait_for_transaction_lock_waiters(&pool, 1).await?;

        apply(&pool, first_proposal, first_application, principal).await?;
        proposal_blocker.rollback().await?;
        delayed_apply.await??;

        let first = load_ledger(&pool, first_application).await?;
        let delayed = load_ledger(&pool, delayed_application).await?;
        assert_eq!(
            delayed.before_snapshot["active_override"],
            first.after_snapshot
        );
        let reader = PgActiveBuildingRegisterUnitOverrideReader::new(pool.clone());
        assert_active_application(&reader, delayed_application).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn writer_predecessor_selection_does_not_depend_on_timestamp_or_uuid_order() -> TestResult {
    run_in_disposable_database("normalization_unit_writer_chain", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let identity = target_identity("writer-chain-order");
        let principal = PrincipalId::new(Uuid::now_v7());
        let proposal_a = submit_approved(&pool, principal, &identity, 211).await?;
        let proposal_b = submit_approved(&pool, principal, &identity, 212).await?;
        let proposal_c = submit_approved(&pool, principal, &identity, 213).await?;
        let application_a = Uuid::from_u128(u128::MAX);
        let application_b = Uuid::from_u128(u128::MAX - 1);
        let application_c = Uuid::now_v7();
        apply(&pool, proposal_a, application_a, principal).await?;
        apply(&pool, proposal_b, application_b, principal).await?;
        force_application_order(&pool, application_b, application_a).await?;

        apply(&pool, proposal_c, application_c, principal).await?;
        let ledger_b = load_ledger(&pool, application_b).await?;
        let ledger_c = load_ledger(&pool, application_c).await?;
        assert_eq!(
            ledger_c.before_snapshot["active_override"],
            ledger_b.after_snapshot
        );
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn non_lifo_rollbacks_persist_actual_state_without_resurrection() -> TestResult {
    run_in_disposable_database("normalization_unit_non_lifo", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let identity = target_identity("non-lifo-rollback");
        let principal = PrincipalId::new(Uuid::now_v7());
        let proposal_a = submit_approved(&pool, principal, &identity, 301).await?;
        let proposal_b = submit_approved(&pool, principal, &identity, 302).await?;
        let application_a = Uuid::now_v7();
        let application_b = Uuid::now_v7();
        apply(&pool, proposal_a, application_a, principal).await?;
        apply(&pool, proposal_b, application_b, principal).await?;
        force_application_order(&pool, application_a, application_b).await?;
        let ledger_b = load_ledger(&pool, application_b).await?;

        let reader = PgActiveBuildingRegisterUnitOverrideReader::new(pool.clone());
        assert_active_application(&reader, application_b).await?;

        let rollback_a = Uuid::now_v7();
        rollback(&pool, application_a, rollback_a, principal).await?;
        let older_rollback_ledger = load_ledger(&pool, rollback_a).await?;
        let active_b = json!({"active_override": ledger_b.after_snapshot});
        assert_eq!(older_rollback_ledger.before_snapshot, active_b);
        assert_eq!(older_rollback_ledger.after_snapshot, active_b);
        assert_active_application(&reader, application_b).await?;

        let rollback_b = Uuid::now_v7();
        rollback(&pool, application_b, rollback_b, principal).await?;
        let current_rollback_ledger = load_ledger(&pool, rollback_b).await?;
        assert_eq!(current_rollback_ledger.before_snapshot, active_b);
        assert_eq!(
            current_rollback_ledger.after_snapshot,
            json!({"active_override": JsonValue::Null})
        );
        assert!(reader
            .list_active_building_register_unit_overrides()
            .await?
            .is_empty());
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn middle_rollback_preserves_the_deepest_active_descendant() -> TestResult {
    run_in_disposable_database("normalization_unit_middle_rollback", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let identity = target_identity("middle-rollback");
        let principal = PrincipalId::new(Uuid::now_v7());
        let proposal_a = submit_approved(&pool, principal, &identity, 311).await?;
        let proposal_b = submit_approved(&pool, principal, &identity, 312).await?;
        let proposal_c = submit_approved(&pool, principal, &identity, 313).await?;
        let application_a = Uuid::now_v7();
        let application_b = Uuid::now_v7();
        let application_c = Uuid::now_v7();
        apply(&pool, proposal_a, application_a, principal).await?;
        apply(&pool, proposal_b, application_b, principal).await?;
        apply(&pool, proposal_c, application_c, principal).await?;

        let rollback_b = Uuid::now_v7();
        rollback(&pool, application_b, rollback_b, principal).await?;

        let ledger_c = load_ledger(&pool, application_c).await?;
        let rollback_ledger = load_ledger(&pool, rollback_b).await?;
        let active_c = json!({"active_override": ledger_c.after_snapshot});
        assert_eq!(rollback_ledger.before_snapshot, active_c);
        assert_eq!(rollback_ledger.after_snapshot, active_c);
        let reader = PgActiveBuildingRegisterUnitOverrideReader::new(pool.clone());
        assert_active_application(&reader, application_c).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn rollback_then_reapply_keeps_state_and_history_in_one_chain() -> TestResult {
    run_in_disposable_database("normalization_unit_reapply", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let identity = target_identity("rollback-reapply");
        let principal = PrincipalId::new(Uuid::now_v7());
        let proposal_a = submit_approved(&pool, principal, &identity, 321).await?;
        let proposal_b = submit_approved(&pool, principal, &identity, 322).await?;
        let proposal_c = submit_approved(&pool, principal, &identity, 323).await?;
        let proposal_d = submit_approved(&pool, principal, &identity, 324).await?;
        let application_a = Uuid::now_v7();
        let application_b = Uuid::now_v7();
        let application_c = Uuid::now_v7();
        let application_d = Uuid::now_v7();

        apply(&pool, proposal_a, application_a, principal).await?;
        apply(&pool, proposal_b, application_b, principal).await?;
        rollback(&pool, application_b, Uuid::now_v7(), principal).await?;
        apply(&pool, proposal_c, application_c, principal).await?;

        let ledger_c = load_ledger(&pool, application_c).await?;
        assert_eq!(
            ledger_c.before_snapshot["active_override"]["proposal_id"],
            json!(proposal_a)
        );
        assert_eq!(
            ledger_c.before_snapshot["lineage_predecessor_proposal_id"],
            json!(proposal_b)
        );
        let reader = PgActiveBuildingRegisterUnitOverrideReader::new(pool.clone());
        assert_active_application(&reader, application_c).await?;

        rollback(&pool, application_c, Uuid::now_v7(), principal).await?;
        rollback(&pool, application_a, Uuid::now_v7(), principal).await?;
        assert!(reader
            .list_active_building_register_unit_overrides()
            .await?
            .is_empty());

        apply(&pool, proposal_d, application_d, principal).await?;
        let ledger_d = load_ledger(&pool, application_d).await?;
        assert_eq!(ledger_d.before_snapshot["active_override"], JsonValue::Null);
        assert_eq!(
            ledger_d.before_snapshot["lineage_predecessor_proposal_id"],
            json!(proposal_c)
        );
        assert_active_application(&reader, application_d).await?;
        Ok(())
    })
    .await
}
