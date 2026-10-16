//! `PostgreSQL` contract tests for the Catalog canonical transaction participant, and for the
//! staff edit path that records what it replaced in `catalog.catalog_edit` (ADR-0264).

use catalog_application::{
    industrial_complex_patch::{
        parse_industrial_complex_proposed_record, parse_industrial_complex_restore_input,
    },
    ports::{CatalogUnitOfWork, UpsertIndustrialComplexCommand},
};
use catalog_domain::{CatalogError, ComplexMutation, IndustrialComplex, IndustrialComplexKind};
use catalog_infrastructure::{PgCatalogUnitOfWork, PgIndustrialComplexTransactionParticipant};
use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::{ComplexId, StaffId};
use serde_json::{json, Value as JsonValue};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use foundation_disposable_database::{run_in_disposable_database, TestResult};

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
/// The id is derived from the code (root ADR-0250), so a source row for an archived complex's code
/// is that same complex: the load refreshes the archived row in place and leaves it archived,
/// instead of minting a second identity beside it or failing the whole batch.
async fn a_reloaded_archived_code_refreshes_the_archived_row() -> TestResult {
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
                primary_bjdong_code: Some("1111010100".to_owned()),
                area_m2: 100_000,
                status: None,
                sido_code: None,
                sigungu_code: None,
                address_text: None,
                management_agency_name: None,
                developer_name: None,
                designated_date: None,
                construction_start_date: None,
                completion_date: None,
                development_progress_percent: None,
                lot_sales_status: None,
                business_period_raw: None,
                business_period_start_month: None,
                business_period_end_month: None,
                designation_basis_law_raw: None,
                development_method_raw: None,
                development_purpose_raw: None,
                invited_industries_raw: None,
                development_stage: None,
                kind_raw: None,
                status_raw: None,
                lot_sales_status_raw: None,
            }])
            .await;

        let replacement = replacement?;
        assert_eq!(replacement.len(), 1);
        assert_eq!(replacement[0].complex.id, complex.id);
        assert!(replacement[0].complex.archived_at.is_some());
        let rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM catalog.industrial_complex WHERE official_complex_code = $1",
        )
        .bind(&complex.official_complex_code)
        .fetch_one(&pool)
        .await?;
        assert_eq!(rows, 1, "one code, one complex");
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

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn an_update_keeps_the_replaced_values_and_the_editor() -> TestResult {
    run_in_disposable_database("catalog_complex_edit_ledger", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let complex = create_complex(&pool).await?;
        let editor = StaffId::new(Uuid::now_v7());
        let uow = PgCatalogUnitOfWork::new(pool.clone());

        uow.update_complex(complex.id, 1, rename("Renamed Complex", 120_000), editor)
            .await?;
        uow.update_complex(complex.id, 2, rename("Renamed Again", 130_000), editor)
            .await?;

        let rows: Vec<(String, String, i64, JsonValue, JsonValue, Uuid)> = sqlx::query_as(
            "SELECT command_type, target_kind, expected_version,
                    before_snapshot, after_snapshot, applied_by_principal_id
             FROM catalog.catalog_edit
             WHERE target_id = $1
             ORDER BY expected_version",
        )
        .bind(complex.id.as_uuid())
        .fetch_all(&pool)
        .await?;

        assert_eq!(rows.len(), 2);
        let (command_type, target_kind, expected_version, before, after, applied_by) = &rows[0];
        assert_eq!(command_type, "catalog.industrial_complex.update");
        assert_eq!(target_kind, "industrial_complex");
        assert_eq!(*expected_version, 1);
        assert_eq!(before["name"], complex.name.as_str());
        assert_eq!(before["area_m2"], 95_000);
        assert_eq!(before["version"], 1);
        assert_eq!(after["name"], "Renamed Complex");
        assert_eq!(after["version"], 2);
        assert_eq!(*applied_by, editor.as_uuid());
        // The first value survives the second overwrite.
        assert_eq!(rows[1].3["name"], "Renamed Complex");
        assert_eq!(rows[1].4["name"], "Renamed Again");
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_rejected_update_writes_no_edit() -> TestResult {
    run_in_disposable_database("catalog_complex_edit_stale", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let complex = create_complex(&pool).await?;
        let uow = PgCatalogUnitOfWork::new(pool.clone());

        let stale = uow
            .update_complex(
                complex.id,
                7,
                rename("Stale Edit", 1),
                StaffId::new(Uuid::now_v7()),
            )
            .await;
        assert!(matches!(
            stale,
            Err(CatalogError::ComplexVersionConflict {
                expected: 7,
                current: 1
            })
        ));
        let missing = uow
            .update_complex(
                ComplexId::new(Uuid::now_v7()),
                1,
                rename("Missing", 1),
                StaffId::new(Uuid::now_v7()),
            )
            .await;
        assert!(matches!(missing, Err(CatalogError::ComplexNotFound(_))));

        let edits: i64 = sqlx::query_scalar("SELECT count(*) FROM catalog.catalog_edit")
            .fetch_one(&pool)
            .await?;
        assert_eq!(edits, 0);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn the_edit_ledger_refuses_rewrite_and_unknown_kinds() -> TestResult {
    run_in_disposable_database("catalog_complex_edit_immutable", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let complex = create_complex(&pool).await?;
        PgCatalogUnitOfWork::new(pool.clone())
            .update_complex(
                complex.id,
                1,
                rename("Renamed Complex", 120_000),
                StaffId::new(Uuid::now_v7()),
            )
            .await?;

        let update = sqlx::query(
            "UPDATE catalog.catalog_edit SET before_snapshot = '{}'::jsonb WHERE target_id = $1",
        )
        .bind(complex.id.as_uuid())
        .execute(&pool)
        .await;
        assert!(update.is_err(), "the edit ledger accepted an UPDATE");
        let delete = sqlx::query("DELETE FROM catalog.catalog_edit WHERE target_id = $1")
            .bind(complex.id.as_uuid())
            .execute(&pool)
            .await;
        assert!(delete.is_err(), "the edit ledger accepted a DELETE");

        let unknown_kind = sqlx::query(
            "INSERT INTO catalog.catalog_edit (
                 id, command_type, target_kind, target_id, expected_version,
                 before_snapshot, after_snapshot, applied_by_principal_id
             ) VALUES ($1, 'catalog.building.update', 'building', $2, 1, '{}', '{}', $3)",
        )
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await;
        assert!(
            unknown_kind.is_err(),
            "the edit ledger accepted a kind with no writer"
        );
        Ok(())
    })
    .await
}

fn rename(name: &str, area_m2: u64) -> ComplexMutation {
    ComplexMutation {
        name: Some(name.to_owned()),
        area_m2: Some(area_m2),
    }
}

async fn create_complex(pool: &PgPool) -> TestResult<IndustrialComplex> {
    let official_complex_code = format!("participant-{}", Uuid::new_v4());
    let id = ComplexId::for_official_complex_code(&official_complex_code);
    let complex = IndustrialComplex {
        id,
        official_complex_code,
        name: "Original Industrial Complex".to_owned(),
        kind: IndustrialComplexKind::General,
        primary_bjdong_code: Some("1111010100".to_owned()),
        area_m2: 95_000,
        status: None,
        sido_code: None,
        sigungu_code: None,
        address_text: None,
        management_agency_name: None,
        developer_name: None,
        designated_date: None,
        construction_start_date: None,
        completion_date: None,
        development_progress_percent: None,
        lot_sales_status: None,
        business_period_raw: None,
        business_period_start_month: None,
        business_period_end_month: None,
        designation_basis_law_raw: None,
        development_method_raw: None,
        development_purpose_raw: None,
        invited_industries_raw: None,
        development_stage: None,
        kind_raw: None,
        status_raw: None,
        lot_sales_status_raw: None,
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
