//! A parcel edit lands in the edit ledger, not only on the parcel row (ADR-0023).
//!
//! ADR-0006 rebuilds the `PostGIS` serving projection from a snapshot plus an audited edit ledger.
//! Until this increment `update_parcel_kind` wrote the row and an outbox event and nothing else, so
//! a rebuild would have dropped the edit — and the outbox is a delivery channel built to be
//! drained, not a place history lives.
//!
//! The assertions here are about the ledger row, because the parcel row updating is not in doubt.
//! What was in doubt is whether anything records that it happened.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use catalog_application::ports::CatalogUnitOfWork;
use catalog_domain::ParcelKind;
use catalog_infrastructure::PgCatalogUnitOfWork;
use foundation_disposable_database::TestResult;
use foundation_shared_kernel::ids::{ParcelId, StaffId};
use sqlx::{PgPool, Row};
use uuid::Uuid;

async fn pool() -> Result<PgPool, sqlx::Error> {
    let url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set; run `cargo xtask integration foundation postgres`");
    PgPool::connect(&url).await
}

/// Inserts one parcel and returns its id and PNU.
async fn insert_parcel(pool: &PgPool) -> Result<(Uuid, String), sqlx::Error> {
    let parcel_id = Uuid::now_v7();
    let suffix = Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .filter(char::is_ascii_digit)
        .take(8)
        .collect::<String>();
    let pnu = format!("9999900101 1{suffix:0<8}").replace(' ', "");
    sqlx::query(
        "INSERT INTO catalog.parcel (id, pnu, kind, area_m2, version)
         VALUES ($1, $2, 'factory', 100, 1)",
    )
    .bind(parcel_id)
    .bind(&pnu)
    .execute(pool)
    .await?;
    Ok((parcel_id, pnu))
}

/// Removes the fixture, taking the publisher capability the ledger's append-only trigger requires.
///
/// One transaction, and `set_config(.., true)` so the setting is transaction-scoped: session scope
/// does not survive a pool, because each `execute(pool)` borrows whatever connection is free and the
/// setting would land on a different one than the DELETE. Needing the capability at all is the
/// trigger working — a fixture that could erase the ledger without it would mean nothing else
/// needed it either.
async fn delete_parcel(pool: &PgPool, parcel_id: Uuid) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT set_config('foundation.temporal_publisher', 'on', true)")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM catalog.catalog_edit WHERE target_id = $1")
        .bind(parcel_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM catalog.outbox_event WHERE payload->>'parcel_id' = $1::text")
        .bind(parcel_id.to_string())
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM catalog.parcel WHERE id = $1")
        .bind(parcel_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Changing a parcel kind writes one ledger row carrying both sides of the change.
///
/// Both snapshots are asserted, not merely the row's existence: a ledger entry that records the
/// same value on both sides, or `now()` in place of the version it replaced, is a row a rebuild
/// cannot use and an existence check accepts.
#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_parcel_kind_edit_is_recorded_in_the_edit_ledger() -> TestResult {
    let pool = pool().await?;
    let (parcel_id, _pnu) = insert_parcel(&pool).await?;
    let staff_id = Uuid::now_v7();

    let uow = PgCatalogUnitOfWork::new(pool.clone());
    let updated = uow
        .update_parcel_kind(
            ParcelId::new(parcel_id),
            1,
            ParcelKind::Support,
            StaffId::new(staff_id),
        )
        .await?;
    assert_eq!(updated.version, 2, "the parcel row still advances");

    let row = sqlx::query(
        "SELECT command_type,
                target_kind,
                expected_version,
                applied_by_principal_id,
                before_snapshot->>'kind' AS before_kind,
                after_snapshot->>'kind' AS after_kind,
                (before_snapshot->>'version')::bigint AS before_version,
                (after_snapshot->>'version')::bigint AS after_version
           FROM catalog.catalog_edit
          WHERE target_id = $1",
    )
    .bind(parcel_id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(
        row.try_get::<String, _>("command_type")?,
        "parcel.kind.update.v1"
    );
    assert_eq!(row.try_get::<String, _>("target_kind")?, "parcel");
    assert_eq!(row.try_get::<i64, _>("expected_version")?, 1);
    assert_eq!(
        row.try_get::<Uuid, _>("applied_by_principal_id")?,
        staff_id,
        "the route receives the acting principal and must not discard it"
    );
    assert_eq!(row.try_get::<String, _>("before_kind")?, "factory");
    assert_eq!(row.try_get::<String, _>("after_kind")?, "support");
    assert_eq!(row.try_get::<i64, _>("before_version")?, 1);
    assert_eq!(row.try_get::<i64, _>("after_version")?, 2);

    delete_parcel(&pool, parcel_id).await?;
    Ok(())
}

/// A stale version writes nothing at all.
///
/// The ledger row and the parcel row share one transaction, so a refused edit must leave neither.
/// Without this a rejected write could still have deposited a ledger entry claiming a change that
/// never happened.
#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_refused_edit_leaves_no_ledger_row() -> TestResult {
    let pool = pool().await?;
    let (parcel_id, _pnu) = insert_parcel(&pool).await?;

    let uow = PgCatalogUnitOfWork::new(pool.clone());
    let refused = uow
        .update_parcel_kind(
            ParcelId::new(parcel_id),
            99,
            ParcelKind::Support,
            StaffId::new(Uuid::now_v7()),
        )
        .await;
    assert!(refused.is_err(), "a stale expected version must be refused");

    let ledger_rows = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM catalog.catalog_edit WHERE target_id = $1",
    )
    .bind(parcel_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(ledger_rows, 0, "a refused edit must not be recorded as one");

    let kind = sqlx::query_scalar::<_, String>("SELECT kind FROM catalog.parcel WHERE id = $1")
        .bind(parcel_id)
        .fetch_one(&pool)
        .await?;
    assert_eq!(kind, "factory", "and the parcel must be untouched");

    delete_parcel(&pool, parcel_id).await?;
    Ok(())
}

/// A Normalization command cannot enter the Catalog ledger.
///
/// Normalization is a separate bounded context; `package_boundary.rs` keeps its tables out of
/// Catalog and Catalog does not depend on its crates. That boundary was going to be crossed by this
/// very change — the first draft of ADR-0023 widened `normalization_application` instead of adding
/// this table, and the boundary test refused it. The CHECK writes the same rule into the schema so
/// crossing it by spelling is refused too.
#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_normalization_command_cannot_enter_the_catalog_ledger() -> TestResult {
    let pool = pool().await?;
    let (parcel_id, _pnu) = insert_parcel(&pool).await?;

    let refused = sqlx::query(
        "INSERT INTO catalog.catalog_edit
         (id, command_type, target_kind, target_id, expected_version,
          before_snapshot, after_snapshot, applied_by_principal_id)
         VALUES ($1, 'parcel.normalization.apply.v1', 'parcel', $2, 1,
                 '{}'::jsonb, '{}'::jsonb, $3)",
    )
    .bind(Uuid::new_v4())
    .bind(parcel_id)
    .bind(Uuid::now_v7())
    .execute(&pool)
    .await;
    assert!(
        refused.is_err(),
        "a *.normalization.* command belongs to the Normalization ledger, not this one"
    );

    // The nearest row the database must still accept, or the constraint could be refusing every
    // command rather than only the normalization ones.
    sqlx::query(
        "INSERT INTO catalog.catalog_edit
         (id, command_type, target_kind, target_id, expected_version,
          before_snapshot, after_snapshot, applied_by_principal_id)
         VALUES ($1, 'parcel.kind.update.v1', 'parcel', $2, 1,
                 '{}'::jsonb, '{}'::jsonb, $3)",
    )
    .bind(Uuid::new_v4())
    .bind(parcel_id)
    .bind(Uuid::now_v7())
    .execute(&pool)
    .await?;

    delete_parcel(&pool, parcel_id).await?;
    Ok(())
}
