//! Adversarial `PostgreSQL` tests for industrial-complex normalization ledger integrity.

#[path = "support/industrial_complex_fixture.rs"]
#[allow(dead_code)]
mod industrial_complex_fixture;
#[allow(dead_code)]
mod support;

use foundation_normalization_application::{
    NormalizationApplicationCommand, NormalizationRollbackCommand, NormalizationUnitOfWork,
};
use foundation_normalization_domain::NormalizationError;
use serde_json::json;
use uuid::Uuid;

use industrial_complex_fixture::{
    create_pending_fixture, load_complex_scope_state, submit_and_approve,
};
use support::{run_in_disposable_database, TestResult};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../migrations");

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn rollback_rejects_a_corrupted_snapshot_handoff() -> TestResult {
    run_in_disposable_database("normalization_complex_corrupt_handoff", |pool| async move {
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

        sqlx::query(
            "UPDATE catalog.normalization_application
             SET before_snapshot = jsonb_set(before_snapshot, '{area_m2}', '111'::jsonb)
             WHERE id = $1",
        )
        .bind(application_b)
        .execute(&pool)
        .await?;
        let before = load_complex_scope_state(&pool, fixture.complex.id.to_string()).await?;

        let result = fixture
            .normalization_uow
            .rollback_normalization_application(NormalizationRollbackCommand {
                id: Uuid::now_v7(),
                application_id: application_b,
                expected_current_version: 3,
                reason: "must reject a corrupted snapshot handoff".to_owned(),
                rolled_back_by_principal_id: fixture.principal_id,
            })
            .await;

        assert!(matches!(
            result,
            Err(NormalizationError::Persistence(ref message))
                if message == "industrial-complex ledger snapshots do not form a continuous handoff"
        ));
        assert_eq!(
            load_complex_scope_state(&pool, fixture.complex.id.to_string()).await?,
            before
        );
        Ok(())
    })
    .await
}
