//! `PostgreSQL` contract tests for the Catalog canonical transaction participant.

#[allow(dead_code)]
mod support;

use catalog_application::{
    industrial_complex_patch::{
        parse_industrial_complex_proposed_record, parse_industrial_complex_restore_input,
    },
    ports::{CatalogUnitOfWork, UpsertIndustrialComplexCommand},
};
use catalog_domain::{CatalogError, IndustrialComplex, IndustrialComplexKind};
use catalog_infrastructure::{PgCatalogUnitOfWork, PgIndustrialComplexTransactionParticipant};
use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::ComplexId;
use serde_json::{json, Value as JsonValue};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use support::{run_in_disposable_database, TestResult};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../migrations");

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn caller_rollback_removes_apply_restore_and_exact_outbox_events() -> TestResult {
    run_in_disposable_database("catalog_participant_rollback", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let complex = create_complex(&pool).await?;
        let baseline = load_state(&pool, complex.id).await?;
        let participant = PgIndustrialComplexTransactionParticipant::new();
        let mut tx = pool.begin().await?;

        let applied = participant
            .apply(
                &mut tx,
                complex.id,
                1,
                parse_industrial_complex_proposed_record(&json!({
                    "name": "Normalized Industrial Complex",
                    "area_m2": 125_000,
                }))?,
            )
            .await?;

        assert_eq!(applied.target_id, complex.id);
        assert_eq!(
            applied.before_snapshot.as_json(),
            &snapshot_json(&complex, complex.name.as_str(), complex.area_m2, 1)
        );
        assert_eq!(
            applied.after_snapshot.as_json(),
            &snapshot_json(&complex, "Normalized Industrial Complex", 125_000, 2)
        );
        assert_exact_update_event(
            &mut tx,
            applied.outbox_event_id,
            complex.id,
            &["name", "area_m2"],
        )
        .await?;

        let restore = parse_industrial_complex_restore_input(
            applied.before_snapshot.as_json(),
            applied.after_snapshot.as_json(),
            applied.after_snapshot.as_json(),
            complex.id,
        )?;
        let restored = participant.restore(&mut tx, 2, restore).await?;

        assert_eq!(restored.target_id, complex.id);
        assert_eq!(
            restored.before_snapshot.as_json(),
            &snapshot_json(&complex, "Normalized Industrial Complex", 125_000, 2)
        );
        assert_eq!(
            restored.after_snapshot.as_json(),
            &snapshot_json(&complex, complex.name.as_str(), complex.area_m2, 3)
        );
        assert_exact_update_event(
            &mut tx,
            restored.outbox_event_id,
            complex.id,
            &["name", "area_m2"],
        )
        .await?;

        let in_transaction = load_state_in_transaction(&mut tx, complex.id).await?;
        assert_eq!(in_transaction.name, complex.name);
        assert_eq!(in_transaction.area_m2, i64::try_from(complex.area_m2)?);
        assert_eq!(in_transaction.version, 3);
        assert_eq!(in_transaction.outbox_count, baseline.outbox_count + 2);

        tx.rollback().await?;

        let after_rollback = load_state(&pool, complex.id).await?;
        assert_eq!(after_rollback, baseline);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn existing_zero_area_survives_apply_and_restore() -> TestResult {
    run_in_disposable_database("catalog_participant_zero_area", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let mut complex = create_complex(&pool).await?;
        sqlx::query("UPDATE catalog.industrial_complex SET area_m2 = 0 WHERE id = $1")
            .bind(complex.id.as_uuid())
            .execute(&pool)
            .await?;
        complex.area_m2 = 0;
        let baseline = load_state(&pool, complex.id).await?;

        let participant = PgIndustrialComplexTransactionParticipant::new();
        let mut tx = pool.begin().await?;
        let applied = participant
            .apply(
                &mut tx,
                complex.id,
                1,
                parse_industrial_complex_proposed_record(&json!({
                    "name": "Normalized Zero Area Complex",
                }))?,
            )
            .await?;
        let restore = parse_industrial_complex_restore_input(
            applied.before_snapshot.as_json(),
            applied.after_snapshot.as_json(),
            applied.after_snapshot.as_json(),
            complex.id,
        )?;
        let restored = participant.restore(&mut tx, 2, restore).await?;

        assert_eq!(restored.after_snapshot.as_json()["area_m2"], json!(0));
        assert_eq!(
            load_state_in_transaction(&mut tx, complex.id).await?,
            StoredState {
                name: complex.name.clone(),
                area_m2: 0,
                version: 3,
                outbox_count: baseline.outbox_count + 2,
            }
        );
        tx.rollback().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn version_conflict_keeps_row_locked_until_caller_rolls_back() -> TestResult {
    run_in_disposable_database("catalog_participant_lock", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let complex = create_complex(&pool).await?;
        let baseline = load_state(&pool, complex.id).await?;
        let participant = PgIndustrialComplexTransactionParticipant::new();
        let mut tx = pool.begin().await?;

        let result = participant
            .apply(
                &mut tx,
                complex.id,
                9,
                parse_industrial_complex_proposed_record(&json!({
                    "name": "Stale Change",
                }))?,
            )
            .await;
        let Err(error) = result else {
            return Err("stale expected version was accepted".into());
        };
        assert!(matches!(
            error,
            CatalogError::ComplexVersionConflict {
                expected: 9,
                current: 1
            }
        ));

        let mut contender = pool.acquire().await?;
        sqlx::query("SET lock_timeout = '150ms'")
            .execute(&mut *contender)
            .await?;
        let lock_result =
            sqlx::query("UPDATE catalog.industrial_complex SET name = name WHERE id = $1")
                .bind(complex.id.as_uuid())
                .execute(&mut *contender)
                .await;
        let Err(lock_error) = lock_result else {
            return Err("FOR UPDATE lock was not retained by the caller transaction".into());
        };
        assert!(matches!(
            lock_error,
            sqlx::Error::Database(ref database_error)
                if database_error.code().as_deref() == Some("55P03")
        ));

        tx.rollback().await?;
        assert_eq!(load_state(&pool, complex.id).await?, baseline);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn no_op_patch_is_rejected_without_version_or_outbox_change() -> TestResult {
    run_in_disposable_database("catalog_participant_no_op", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let complex = create_complex(&pool).await?;
        let baseline = load_state(&pool, complex.id).await?;
        let participant = PgIndustrialComplexTransactionParticipant::new();
        let mut tx = pool.begin().await?;

        let result = participant
            .apply(
                &mut tx,
                complex.id,
                1,
                parse_industrial_complex_proposed_record(&json!({
                    "name": complex.name,
                }))?,
            )
            .await;

        assert!(matches!(
            result,
            Err(CatalogError::InvalidIndustrialComplexInput(ref message))
                if message == "industrial complex mutation must change canonical state"
        ));
        assert_eq!(
            load_state_in_transaction(&mut tx, complex.id).await?,
            baseline
        );
        tx.rollback().await?;
        assert_eq!(load_state(&pool, complex.id).await?, baseline);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn unchanged_patch_fields_are_omitted_from_the_canonical_event() -> TestResult {
    run_in_disposable_database("catalog_participant_effective_patch", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let complex = create_complex(&pool).await?;
        let participant = PgIndustrialComplexTransactionParticipant::new();
        let mut tx = pool.begin().await?;

        let applied = participant
            .apply(
                &mut tx,
                complex.id,
                1,
                parse_industrial_complex_proposed_record(&json!({
                    "name": complex.name,
                    "area_m2": 125_000,
                }))?,
            )
            .await?;

        assert_exact_update_event(&mut tx, applied.outbox_event_id, complex.id, &["area_m2"])
            .await?;
        assert_eq!(
            applied.after_snapshot.as_json()["name"],
            json!(complex.name)
        );
        assert_eq!(applied.after_snapshot.as_json()["area_m2"], json!(125_000));
        tx.rollback().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn archived_target_returns_exact_catalog_error() -> TestResult {
    run_in_disposable_database("catalog_participant_archived", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let complex = create_complex(&pool).await?;
        sqlx::query(
            "UPDATE catalog.industrial_complex
             SET archived_at = now(), archived_by_staff_id = $2
             WHERE id = $1",
        )
        .bind(complex.id.as_uuid())
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await?;
        let participant = PgIndustrialComplexTransactionParticipant::new();
        let mut tx = pool.begin().await?;

        let result = participant
            .apply(
                &mut tx,
                complex.id,
                1,
                parse_industrial_complex_proposed_record(&json!({
                    "name": "Rejected Change",
                }))?,
            )
            .await;
        let Err(error) = result else {
            return Err("archived target mutation was accepted".into());
        };

        assert!(matches!(
            error,
            CatalogError::ComplexAlreadyArchived(ref id) if id == &complex.id.to_string()
        ));
        tx.rollback().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn archived_official_code_can_be_reused_by_a_new_active_row() -> TestResult {
    run_in_disposable_database("catalog_archive_reentry", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let complex = create_complex(&pool).await?;
        sqlx::query(
            "UPDATE catalog.industrial_complex
             SET archived_at = now(), archived_by_staff_id = $2, archive_reason = 'source replacement'
             WHERE id = $1",
        )
        .bind(complex.id.as_uuid())
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await?;

        let replacement = PgCatalogUnitOfWork::new(pool.clone())
            .upsert_complexes_by_official_code(&[UpsertIndustrialComplexCommand {
                official_complex_code: complex.official_complex_code.clone(),
                name: "Replacement Industrial Complex".to_owned(),
                kind: IndustrialComplexKind::General,
                primary_bjdong_code: "1111010100".to_owned(),
                area_m2: 100_000,
            }])
            .await?;

        assert_eq!(replacement.len(), 1);
        assert_ne!(replacement[0].id, complex.id);
        assert!(replacement[0].archived_at.is_none());
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn missing_target_returns_exact_catalog_error() -> TestResult {
    run_in_disposable_database("catalog_participant_missing", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let missing_id = ComplexId::new(Uuid::now_v7());
        let participant = PgIndustrialComplexTransactionParticipant::new();
        let mut tx = pool.begin().await?;

        let result = participant
            .apply(
                &mut tx,
                missing_id,
                1,
                parse_industrial_complex_proposed_record(&json!({
                    "name": "Missing Change",
                }))?,
            )
            .await;
        let Err(error) = result else {
            return Err("missing target mutation was accepted".into());
        };

        assert!(matches!(
            error,
            CatalogError::ComplexNotFound(ref id) if id == &missing_id.to_string()
        ));
        tx.rollback().await?;
        Ok(())
    })
    .await
}

async fn create_complex(pool: &PgPool) -> TestResult<IndustrialComplex> {
    let complex = IndustrialComplex {
        id: ComplexId::new(Uuid::now_v7()),
        official_complex_code: format!("participant-{}", Uuid::new_v4()),
        name: "Original Industrial Complex".to_owned(),
        kind: IndustrialComplexKind::General,
        primary_bjdong_code: "1111010100".to_owned(),
        area_m2: 95_000,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        archived_at: None,
        version: 1,
    };
    PgCatalogUnitOfWork::new(pool.clone())
        .create_complex(&complex)
        .await?;
    Ok(complex)
}

#[derive(Debug, Eq, PartialEq)]
struct StoredState {
    name: String,
    area_m2: i64,
    version: i64,
    outbox_count: i64,
}

async fn load_state(pool: &PgPool, complex_id: ComplexId) -> TestResult<StoredState> {
    let (name, area_m2, version) = sqlx::query_as(
        "SELECT name, area_m2, version FROM catalog.industrial_complex WHERE id = $1",
    )
    .bind(complex_id.as_uuid())
    .fetch_one(pool)
    .await?;
    let outbox_count = sqlx::query_scalar(
        "SELECT count(*) FROM catalog.outbox_event WHERE payload->>'complex_id' = $1",
    )
    .bind(complex_id.to_string())
    .fetch_one(pool)
    .await?;
    Ok(StoredState {
        name,
        area_m2,
        version,
        outbox_count,
    })
}

async fn load_state_in_transaction(
    tx: &mut Transaction<'_, Postgres>,
    complex_id: ComplexId,
) -> TestResult<StoredState> {
    let (name, area_m2, version) = sqlx::query_as(
        "SELECT name, area_m2, version FROM catalog.industrial_complex WHERE id = $1",
    )
    .bind(complex_id.as_uuid())
    .fetch_one(&mut **tx)
    .await?;
    let outbox_count = sqlx::query_scalar(
        "SELECT count(*) FROM catalog.outbox_event WHERE payload->>'complex_id' = $1",
    )
    .bind(complex_id.to_string())
    .fetch_one(&mut **tx)
    .await?;
    Ok(StoredState {
        name,
        area_m2,
        version,
        outbox_count,
    })
}

async fn assert_exact_update_event(
    tx: &mut Transaction<'_, Postgres>,
    event_id: Uuid,
    complex_id: ComplexId,
    changed_fields: &[&str],
) -> TestResult {
    let (event_type, payload): (String, JsonValue) =
        sqlx::query_as("SELECT type, payload FROM catalog.outbox_event WHERE event_id = $1")
            .bind(event_id)
            .fetch_one(&mut **tx)
            .await?;
    let updated_at: DateTime<Utc> =
        sqlx::query_scalar("SELECT updated_at FROM catalog.industrial_complex WHERE id = $1")
            .bind(complex_id.as_uuid())
            .fetch_one(&mut **tx)
            .await?;

    assert_eq!(event_type, "catalog.industrial_complex.updated.v1");
    assert_eq!(
        payload,
        json!({
            "type": "catalog.industrial_complex.updated.v1",
            "schema_version": 1,
            "complex_id": complex_id,
            "changed_fields": changed_fields,
            "updated_at": updated_at,
        })
    );
    Ok(())
}

fn snapshot_json(complex: &IndustrialComplex, name: &str, area_m2: u64, version: i64) -> JsonValue {
    json!({
        "id": complex.id.as_uuid(),
        "official_complex_code": complex.official_complex_code,
        "name": name,
        "kind": complex.kind.wire_name(),
        "primary_bjdong_code": complex.primary_bjdong_code,
        "area_m2": area_m2,
        "version": version,
    })
}
