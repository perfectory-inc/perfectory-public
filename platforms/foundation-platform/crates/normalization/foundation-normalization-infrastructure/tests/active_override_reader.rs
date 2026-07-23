//! `PostgreSQL` contracts for active building-register-unit override reads.

#[path = "support/building_register_unit_fixture.rs"]
#[allow(dead_code)]
mod building_register_unit_fixture;
#[allow(dead_code)]
mod support;

use foundation_normalization_application::ActiveBuildingRegisterUnitOverrideReader;
use foundation_normalization_domain::NormalizationError;
use foundation_normalization_infrastructure::PgActiveBuildingRegisterUnitOverrideReader;
use foundation_shared_kernel::ids::PrincipalId;
use serde_json::json;
use uuid::Uuid;

use building_register_unit_fixture::{
    apply, force_application_order, load_ledger, rollback, set_application_after_proposal_id,
    set_application_before_snapshot, set_application_predecessor, set_same_application_time,
    submit_approved, target_identity,
};
use support::{run_in_disposable_database, TestResult};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../migrations");

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn latest_active_row_returns_exact_application_id_and_snapshot() -> TestResult {
    run_in_disposable_database("normalization_override_latest", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let identity = target_identity("reader-latest");
        let principal = PrincipalId::new(Uuid::now_v7());
        let older_proposal = submit_approved(&pool, principal, &identity, 401).await?;
        let newer_proposal = submit_approved(&pool, principal, &identity, 402).await?;
        let older_application = Uuid::now_v7();
        let newer_application = Uuid::now_v7();
        apply(&pool, older_proposal, older_application, principal).await?;
        apply(&pool, newer_proposal, newer_application, principal).await?;
        force_application_order(&pool, older_application, newer_application).await?;
        let expected = load_ledger(&pool, newer_application).await?;

        let active = PgActiveBuildingRegisterUnitOverrideReader::new(pool.clone())
            .list_active_building_register_unit_overrides()
            .await?;

        assert_eq!(active.len(), 1);
        assert_eq!(active[0].application_id, newer_application);
        assert_eq!(active[0].snapshot, expected.after_snapshot);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn rolled_back_latest_row_is_excluded_and_previous_row_becomes_active() -> TestResult {
    run_in_disposable_database("normalization_override_rollback", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let identity = target_identity("reader-rollback");
        let principal = PrincipalId::new(Uuid::now_v7());
        let older_proposal = submit_approved(&pool, principal, &identity, 501).await?;
        let newer_proposal = submit_approved(&pool, principal, &identity, 502).await?;
        let older_application = Uuid::now_v7();
        let newer_application = Uuid::now_v7();
        apply(&pool, older_proposal, older_application, principal).await?;
        apply(&pool, newer_proposal, newer_application, principal).await?;
        force_application_order(&pool, older_application, newer_application).await?;
        let expected = load_ledger(&pool, older_application).await?;
        rollback(&pool, newer_application, Uuid::now_v7(), principal).await?;

        let active = PgActiveBuildingRegisterUnitOverrideReader::new(pool.clone())
            .list_active_building_register_unit_overrides()
            .await?;

        assert_eq!(active.len(), 1);
        assert_eq!(active[0].application_id, older_application);
        assert_eq!(active[0].snapshot, expected.after_snapshot);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn legacy_rollback_snapshot_shape_remains_readable() -> TestResult {
    run_in_disposable_database(
        "normalization_override_legacy_rollback",
        |pool| async move {
            MIGRATOR.run(&pool).await?;
            let identity = target_identity("reader-legacy-rollback");
            let principal = PrincipalId::new(Uuid::now_v7());
            let proposal = submit_approved(&pool, principal, &identity, 511).await?;
            let application = Uuid::now_v7();
            apply(&pool, proposal, application, principal).await?;
            let ledger = load_ledger(&pool, application).await?;

            sqlx::query(
                "INSERT INTO catalog.normalization_application
             (id, proposal_id, command_type, target_kind, target_id, expected_version,
              before_snapshot, after_snapshot, applied_by_principal_id, rollback_of,
              outbox_event_id)
             VALUES ($1, $2, 'building_register_unit.normalization.rollback.v1',
                     'building_register_unit', NULL, 1, $3, $4, $5, $6, NULL)",
            )
            .bind(Uuid::now_v7())
            .bind(proposal)
            .bind(ledger.after_snapshot)
            .bind(ledger.before_snapshot)
            .bind(principal.as_uuid())
            .bind(application)
            .execute(&pool)
            .await?;

            let active = PgActiveBuildingRegisterUnitOverrideReader::new(pool)
                .list_active_building_register_unit_overrides()
                .await?;
            assert!(active.is_empty());
            Ok(())
        },
    )
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn distinct_identities_return_exact_rows_in_identity_order() -> TestResult {
    run_in_disposable_database("normalization_override_identity_order", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let identity_a = target_identity("reader-a");
        let identity_b = target_identity("reader-b");
        let principal = PrincipalId::new(Uuid::now_v7());
        let proposal_b = submit_approved(&pool, principal, &identity_b, 602).await?;
        let proposal_a = submit_approved(&pool, principal, &identity_a, 601).await?;
        let application_b = Uuid::now_v7();
        let application_a = Uuid::now_v7();
        apply(&pool, proposal_b, application_b, principal).await?;
        apply(&pool, proposal_a, application_a, principal).await?;
        let expected_a = load_ledger(&pool, application_a).await?;
        let expected_b = load_ledger(&pool, application_b).await?;

        let active = PgActiveBuildingRegisterUnitOverrideReader::new(pool.clone())
            .list_active_building_register_unit_overrides()
            .await?;

        assert_eq!(active.len(), 2);
        assert_eq!(active[0].application_id, application_a);
        assert_eq!(active[0].snapshot, expected_a.after_snapshot);
        assert_eq!(active[1].application_id, application_b);
        assert_eq!(active[1].snapshot, expected_b.after_snapshot);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn equal_applied_at_still_follows_the_predecessor_chain() -> TestResult {
    run_in_disposable_database("normalization_override_tie_break", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let identity = target_identity("reader-tie-break");
        let principal = PrincipalId::new(Uuid::now_v7());
        let high_proposal = submit_approved(&pool, principal, &identity, 701).await?;
        let low_proposal = submit_approved(&pool, principal, &identity, 702).await?;
        let high_id = Uuid::from_u128(u128::MAX);
        let low_id = Uuid::from_u128(u128::MAX - 1);
        apply(&pool, high_proposal, high_id, principal).await?;
        apply(&pool, low_proposal, low_id, principal).await?;
        set_same_application_time(&pool, high_id, low_id).await?;
        let expected = load_ledger(&pool, low_id).await?;

        let active = PgActiveBuildingRegisterUnitOverrideReader::new(pool.clone())
            .list_active_building_register_unit_overrides()
            .await?;

        assert_eq!(active.len(), 1);
        assert_eq!(active[0].application_id, low_id);
        assert_eq!(active[0].snapshot, expected.after_snapshot);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn cyclic_predecessor_chain_fails_loudly() -> TestResult {
    run_in_disposable_database("normalization_override_cycle", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let identity = target_identity("reader-cycle");
        let principal = PrincipalId::new(Uuid::now_v7());
        let proposal_a = submit_approved(&pool, principal, &identity, 711).await?;
        let proposal_b = submit_approved(&pool, principal, &identity, 712).await?;
        let proposal_c = submit_approved(&pool, principal, &identity, 713).await?;
        let application_a = Uuid::now_v7();
        let application_b = Uuid::now_v7();
        let application_c = Uuid::now_v7();
        apply(&pool, proposal_a, application_a, principal).await?;
        apply(&pool, proposal_b, application_b, principal).await?;
        apply(&pool, proposal_c, application_c, principal).await?;
        let ledger_c = load_ledger(&pool, application_c).await?;
        set_application_predecessor(&pool, application_a, &ledger_c.after_snapshot).await?;

        let result = PgActiveBuildingRegisterUnitOverrideReader::new(pool)
            .list_active_building_register_unit_overrides()
            .await;

        assert!(matches!(
            result,
            Err(NormalizationError::Persistence(ref message))
                if message == "building-register-unit override chain is invalid"
        ));
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn branched_predecessor_chain_fails_loudly() -> TestResult {
    run_in_disposable_database("normalization_override_branch", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let identity = target_identity("reader-branch");
        let principal = PrincipalId::new(Uuid::now_v7());
        let proposal_a = submit_approved(&pool, principal, &identity, 721).await?;
        let proposal_b = submit_approved(&pool, principal, &identity, 722).await?;
        let proposal_c = submit_approved(&pool, principal, &identity, 723).await?;
        let application_a = Uuid::now_v7();
        let application_b = Uuid::now_v7();
        let application_c = Uuid::now_v7();
        apply(&pool, proposal_a, application_a, principal).await?;
        apply(&pool, proposal_b, application_b, principal).await?;
        apply(&pool, proposal_c, application_c, principal).await?;
        let ledger_a = load_ledger(&pool, application_a).await?;
        set_application_predecessor(&pool, application_c, &ledger_a.after_snapshot).await?;

        let result = PgActiveBuildingRegisterUnitOverrideReader::new(pool)
            .list_active_building_register_unit_overrides()
            .await;

        assert!(matches!(
            result,
            Err(NormalizationError::Persistence(ref message))
                if message == "building-register-unit override chain is invalid"
        ));
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn missing_active_override_envelope_fails_loudly() -> TestResult {
    run_in_disposable_database(
        "normalization_override_missing_envelope",
        |pool| async move {
            MIGRATOR.run(&pool).await?;
            let identity = target_identity("reader-missing-envelope");
            let principal = PrincipalId::new(Uuid::now_v7());
            let proposal = submit_approved(&pool, principal, &identity, 731).await?;
            let application = Uuid::now_v7();
            apply(&pool, proposal, application, principal).await?;
            set_application_before_snapshot(&pool, application, &json!({})).await?;

            assert_invalid_chain(&pool).await?;
            Ok(())
        },
    )
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn malformed_explicit_lineage_predecessor_fails_loudly() -> TestResult {
    run_in_disposable_database("normalization_override_bad_lineage", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let identity = target_identity("reader-bad-lineage");
        let principal = PrincipalId::new(Uuid::now_v7());
        let proposal = submit_approved(&pool, principal, &identity, 732).await?;
        let application = Uuid::now_v7();
        apply(&pool, proposal, application, principal).await?;
        set_application_before_snapshot(
            &pool,
            application,
            &json!({
                "active_override": null,
                "lineage_predecessor_proposal_id": "not-a-uuid"
            }),
        )
        .await?;

        assert_invalid_chain(&pool).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn after_snapshot_proposal_mismatch_fails_loudly() -> TestResult {
    run_in_disposable_database(
        "normalization_override_proposal_mismatch",
        |pool| async move {
            MIGRATOR.run(&pool).await?;
            let identity = target_identity("reader-proposal-mismatch");
            let principal = PrincipalId::new(Uuid::now_v7());
            let proposal = submit_approved(&pool, principal, &identity, 733).await?;
            let application = Uuid::now_v7();
            apply(&pool, proposal, application, principal).await?;
            set_application_after_proposal_id(&pool, application, Uuid::now_v7()).await?;

            assert_invalid_chain(&pool).await?;
            Ok(())
        },
    )
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn targeted_writer_rejects_application_missing_snapshot_identity() -> TestResult {
    run_in_disposable_database(
        "normalization_override_missing_snapshot_identity",
        |pool| async move {
            MIGRATOR.run(&pool).await?;
            let identity = target_identity("writer-missing-snapshot-identity");
            let principal = PrincipalId::new(Uuid::now_v7());
            let first_proposal = submit_approved(&pool, principal, &identity, 741).await?;
            let first_application = Uuid::now_v7();
            apply(&pool, first_proposal, first_application, principal).await?;
            sqlx::query(
                "UPDATE catalog.normalization_application
                 SET after_snapshot = after_snapshot - 'target_identity'
                 WHERE id = $1",
            )
            .bind(first_application)
            .execute(&pool)
            .await?;

            let second_proposal = submit_approved(&pool, principal, &identity, 742).await?;
            let result = apply(&pool, second_proposal, Uuid::now_v7(), principal).await;
            assert!(matches!(
                result,
                Err(NormalizationError::Persistence(ref message))
                    if message == "building-register-unit override chain is invalid"
            ));
            let application_count: i64 = sqlx::query_scalar(
                "SELECT count(*)
                 FROM catalog.normalization_application
                 WHERE target_kind = 'building_register_unit'
                   AND command_type = 'building_register_unit.normalization.apply.v1'",
            )
            .fetch_one(&pool)
            .await?;
            assert_eq!(application_count, 1);
            Ok(())
        },
    )
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn malformed_rollback_relationship_fails_loudly() -> TestResult {
    run_in_disposable_database("normalization_override_bad_rollback", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let identity = target_identity("reader-bad-rollback");
        let principal = PrincipalId::new(Uuid::now_v7());
        let proposal = submit_approved(&pool, principal, &identity, 734).await?;
        let application = Uuid::now_v7();
        apply(&pool, proposal, application, principal).await?;

        sqlx::query(
            "INSERT INTO catalog.normalization_application
             (id, proposal_id, command_type, target_kind, target_id, expected_version,
              before_snapshot, after_snapshot, applied_by_principal_id, rollback_of,
              outbox_event_id)
             VALUES ($1, $2, 'not-a-rollback-command', 'building_register_unit', NULL, 1,
                     $3, $4, $5, $6, NULL)",
        )
        .bind(Uuid::now_v7())
        .bind(proposal)
        .bind(json!({"active_override": null}))
        .bind(json!({"active_override": null}))
        .bind(principal.as_uuid())
        .bind(application)
        .execute(&pool)
        .await?;

        assert_invalid_chain(&pool).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn rollback_without_expected_version_fails_loudly() -> TestResult {
    run_in_disposable_database("normalization_override_null_version", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let identity = target_identity("reader-null-rollback-version");
        let principal = PrincipalId::new(Uuid::now_v7());
        let proposal = submit_approved(&pool, principal, &identity, 735).await?;
        let application = Uuid::now_v7();
        apply(&pool, proposal, application, principal).await?;

        sqlx::query(
            "INSERT INTO catalog.normalization_application
             (id, proposal_id, command_type, target_kind, target_id, expected_version,
              before_snapshot, after_snapshot, applied_by_principal_id, rollback_of,
              outbox_event_id)
             VALUES ($1, $2, 'building_register_unit.normalization.rollback.v1',
                     'building_register_unit', NULL, NULL, $3, $4, $5, $6, NULL)",
        )
        .bind(Uuid::now_v7())
        .bind(proposal)
        .bind(json!({"active_override": null}))
        .bind(json!({"active_override": null}))
        .bind(principal.as_uuid())
        .bind(application)
        .execute(&pool)
        .await?;

        assert_invalid_chain(&pool).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn rollback_with_empty_active_override_fails_loudly() -> TestResult {
    run_in_disposable_database("normalization_override_empty_rollback", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let identity = target_identity("reader-empty-rollback");
        let principal = PrincipalId::new(Uuid::now_v7());
        let proposal = submit_approved(&pool, principal, &identity, 736).await?;
        let application = Uuid::now_v7();
        apply(&pool, proposal, application, principal).await?;

        sqlx::query(
            "INSERT INTO catalog.normalization_application
             (id, proposal_id, command_type, target_kind, target_id, expected_version,
              before_snapshot, after_snapshot, applied_by_principal_id, rollback_of,
              outbox_event_id)
             VALUES ($1, $2, 'building_register_unit.normalization.rollback.v1',
                     'building_register_unit', NULL, 1, $3, $4, $5, $6, NULL)",
        )
        .bind(Uuid::now_v7())
        .bind(proposal)
        .bind(json!({"active_override": {}}))
        .bind(json!({"active_override": {}}))
        .bind(principal.as_uuid())
        .bind(application)
        .execute(&pool)
        .await?;

        assert_invalid_chain(&pool).await?;
        Ok(())
    })
    .await
}

async fn assert_invalid_chain(pool: &sqlx::PgPool) -> TestResult {
    let result = PgActiveBuildingRegisterUnitOverrideReader::new(pool.clone())
        .list_active_building_register_unit_overrides()
        .await;
    assert!(matches!(
        result,
        Err(NormalizationError::Persistence(ref message))
            if message == "building-register-unit override chain is invalid"
    ));
    Ok(())
}
