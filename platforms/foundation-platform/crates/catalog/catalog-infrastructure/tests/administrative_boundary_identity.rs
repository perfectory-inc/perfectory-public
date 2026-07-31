//! Disposable-Postgres proof for the administrative identity bridge.
//!
//! The migration is applied by `scripts/verify/integration.sh foundation`; this test is ignored
//! during offline verification and runs when `DATABASE_URL` is supplied.

#![allow(clippy::expect_used, clippy::too_many_lines, clippy::unwrap_used)]

use sqlx::{Acquire, PgPool, Row};
use uuid::Uuid;

/// Connection for this `#[ignore]`d live suite. A missing `DATABASE_URL` aborts
/// the test instead of yielding `None`: these tests run only when a harness asked
/// for them, so an absent database is a provisioning failure — not a reason to
/// report success.
async fn pool() -> Result<PgPool, sqlx::Error> {
    let url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set; run `cargo xtask integration foundation`");
    PgPool::connect(&url).await
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn temporal_aliases_are_stable_and_history_is_guarded(
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    let complex_id = Uuid::new_v4();
    let parcel_id = Uuid::new_v4();
    let revision_one = Uuid::new_v4();
    let revision_two = Uuid::new_v4();
    let source_one = Uuid::new_v4();
    let source_two = Uuid::new_v4();
    let jeonnam_id = Uuid::new_v4();
    let gwangju_id = Uuid::new_v4();
    let merged_special_city_id = Uuid::new_v4();
    let old_pnu = "9999900101100010001";
    let new_pnu = "9999900101100010002";

    sqlx::query(
        "INSERT INTO catalog.industrial_complex
         (id, official_complex_code, name, kind, primary_bjdong_code, area_m2, version)
         VALUES ($1, $2, 'Boundary identity fixture', 'general', '9999900101', 1, 1)",
    )
    .bind(complex_id)
    .bind(format!("IC-BOUNDARY-{complex_id}"))
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO catalog.parcel (id, complex_id, pnu, kind, area_m2, version)
         VALUES ($1, $2, $3, 'factory', 1, 1)",
    )
    .bind(parcel_id)
    .bind(complex_id)
    .bind(old_pnu)
    .execute(&mut *tx)
    .await?;

    for (source_id, external_id) in [
        (source_one, "boundary-source-one"),
        (source_two, "boundary-source-two"),
    ] {
        sqlx::query(
            "INSERT INTO catalog.source_record (id, source, external_id, checksum_sha256)
             VALUES ($1, 'test', $2, repeat('a', 64))",
        )
        .bind(source_id)
        .bind(external_id)
        .execute(&mut *tx)
        .await?;
    }

    for (unit_id, stable_key, kind) in [
        (jeonnam_id, "admin:sido:jeonnam", "sido"),
        (gwangju_id, "admin:sido:gwangju", "sido"),
        (
            merged_special_city_id,
            "admin:sido:jeonnam-gwangju-special-city",
            "sido",
        ),
    ] {
        sqlx::query(
            "INSERT INTO catalog.administrative_unit (id, unit_kind, stable_key)
             VALUES ($1, $2, $3)",
        )
        .bind(unit_id)
        .bind(kind)
        .bind(stable_key)
        .execute(&mut *tx)
        .await?;
    }
    for (revision_id, source_id, canonical_snapshot, source_snapshot) in [
        (
            revision_one,
            source_one,
            "1001",
            "iceberg:test-boundary-one",
        ),
        (
            revision_two,
            source_two,
            "1002",
            "iceberg:test-boundary-two",
        ),
    ] {
        sqlx::query(
            "INSERT INTO catalog.administrative_boundary_revision
             (id, canonical_iceberg_snapshot_id, source_snapshot_id, source_record_id, status, validated_at)
             VALUES ($1, $3, $4, $2, 'published', now())",
        )
        .bind(revision_id)
        .bind(source_id)
        .bind(canonical_snapshot)
        .bind(source_snapshot)
        .execute(&mut *tx)
        .await?;
    }

    for (id, unit_id, code, display_name, period, revision, source_snapshot, source_id) in [
        (
            Uuid::new_v4(),
            jeonnam_id,
            "46",
            "전라남도",
            "[2020-01-01,2026-07-01)",
            revision_one,
            "iceberg:test-boundary-one",
            source_one,
        ),
        (
            Uuid::new_v4(),
            gwangju_id,
            "29",
            "광주광역시",
            "[2020-01-01,2026-07-01)",
            revision_one,
            "iceberg:test-boundary-one",
            source_one,
        ),
        (
            Uuid::new_v4(),
            merged_special_city_id,
            "99",
            "전남광주통합특별시",
            "[2026-07-01,)",
            revision_two,
            "iceberg:test-boundary-two",
            source_two,
        ),
    ] {
        sqlx::query(
            "INSERT INTO catalog.administrative_unit_identifier
             (id, administrative_unit_id, authority, code, display_name, effective_period,
              data_revision, source_snapshot_id, source_record_id)
             VALUES ($1, $2, 'mois', $3, $4, $5::daterange, $6, $7, $8)",
        )
        .bind(id)
        .bind(unit_id)
        .bind(code)
        .bind(display_name)
        .bind(period)
        .bind(revision)
        .bind(source_snapshot)
        .bind(source_id)
        .execute(&mut *tx)
        .await?;
    }
    for (from_unit_id, source_id) in [(jeonnam_id, source_one), (gwangju_id, source_one)] {
        sqlx::query(
            "INSERT INTO catalog.administrative_unit_transition
             (id, from_unit_id, to_unit_id, transition_kind, effective_period,
              data_revision, source_snapshot_id, source_record_id)
             VALUES ($1, $2, $3, 'merged_into', '[2026-07-01,)'::daterange, $4, $5, $6)",
        )
        .bind(Uuid::new_v4())
        .bind(from_unit_id)
        .bind(merged_special_city_id)
        .bind(revision_two)
        .bind("iceberg:test-boundary-two")
        .bind(source_id)
        .execute(&mut *tx)
        .await?;
    }

    let old_admin_name = sqlx::query_scalar::<_, String>(
        "SELECT display_name
           FROM catalog.administrative_unit_identifier
          WHERE administrative_unit_id = $1
            AND effective_period @> DATE '2026-06-30'",
    )
    .bind(jeonnam_id)
    .fetch_one(&mut *tx)
    .await?;
    let new_admin_name = sqlx::query_scalar::<_, String>(
        "SELECT display_name
           FROM catalog.administrative_unit_identifier
          WHERE administrative_unit_id = $1
            AND effective_period @> DATE '2026-07-01'",
    )
    .bind(merged_special_city_id)
    .fetch_one(&mut *tx)
    .await?;
    assert_eq!(old_admin_name, "전라남도");
    assert_eq!(new_admin_name, "전남광주통합특별시");
    let merged_predecessor_count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)
           FROM catalog.administrative_unit_transition
          WHERE to_unit_id = $1 AND transition_kind = 'merged_into'",
    )
    .bind(merged_special_city_id)
    .fetch_one(&mut *tx)
    .await?;
    assert_eq!(merged_predecessor_count, 2);

    sqlx::query("SELECT catalog.publish_parcel_identifier($1, $2, $3, $4, NULL, $5, $6)")
        .bind(parcel_id)
        .bind(old_pnu)
        .bind(chrono::NaiveDate::from_ymd_opt(2020, 1, 1).expect("date"))
        .bind(revision_one)
        .bind("iceberg:test-boundary-one")
        .bind(source_one)
        .execute(&mut *tx)
        .await?;
    sqlx::query("SELECT catalog.publish_parcel_identifier($1, $2, $3, $4, $5, $6, $7)")
        .bind(parcel_id)
        .bind(new_pnu)
        .bind(chrono::NaiveDate::from_ymd_opt(2026, 7, 1).expect("date"))
        .bind(revision_two)
        .bind(revision_one)
        .bind("iceberg:test-boundary-two")
        .bind(source_two)
        .execute(&mut *tx)
        .await?;

    let alias = sqlx::query(
        "SELECT parcel_id, identifier_value
           FROM catalog.parcel_identifier_lookup
          WHERE identifier_value = $1",
    )
    .bind(old_pnu)
    .fetch_one(&mut *tx)
    .await?;
    assert_eq!(alias.try_get::<Uuid, _>("parcel_id")?, parcel_id);
    assert_eq!(alias.try_get::<String, _>("identifier_value")?, old_pnu);

    let current = sqlx::query(
        "SELECT identifier_value, data_revision
           FROM catalog.parcel_current_identifier
          WHERE parcel_id = $1",
    )
    .bind(parcel_id)
    .fetch_one(&mut *tx)
    .await?;
    assert_eq!(current.try_get::<String, _>("identifier_value")?, new_pnu);
    assert_eq!(current.try_get::<Uuid, _>("data_revision")?, revision_two);

    let before_effective = sqlx::query_scalar::<_, String>(
        "SELECT identifier_value FROM catalog.parcel_identifier
          WHERE parcel_id = $1 AND effective_period @> DATE '2026-06-30'",
    )
    .bind(parcel_id)
    .fetch_one(&mut *tx)
    .await?;
    let at_effective = sqlx::query_scalar::<_, String>(
        "SELECT identifier_value FROM catalog.parcel_identifier
          WHERE parcel_id = $1 AND effective_period @> DATE '2026-07-01'",
    )
    .bind(parcel_id)
    .fetch_one(&mut *tx)
    .await?;
    assert_eq!(before_effective, old_pnu);
    assert_eq!(at_effective, new_pnu);

    let mut stale_tx = tx.begin().await?;
    let stale = sqlx::query("SELECT catalog.publish_parcel_identifier($1, $2, $3, $4, $5, $6, $7)")
        .bind(parcel_id)
        .bind("9999900101100010003")
        .bind(chrono::NaiveDate::from_ymd_opt(2027, 1, 1).expect("date"))
        .bind(revision_one)
        .bind(revision_one)
        .bind("iceberg:test-boundary-one")
        .bind(source_one)
        .execute(&mut *stale_tx)
        .await;
    assert!(
        stale.is_err(),
        "stale publisher must fail rather than overwrite current identity"
    );
    stale_tx.rollback().await?;

    let mut history_tx = tx.begin().await?;
    let direct_history_update = sqlx::query(
        "UPDATE catalog.parcel_identifier SET source_snapshot_id = 'tampered' WHERE parcel_id = $1",
    )
    .bind(parcel_id)
    .execute(&mut *history_tx)
    .await;
    assert!(
        direct_history_update.is_err(),
        "history must be append-only for API sessions"
    );
    history_tx.rollback().await?;

    let mut projection_tx = tx.begin().await?;
    let direct_projection_update =
        sqlx::query("UPDATE catalog.parcel SET pnu = '9999900101100010003' WHERE id = $1")
            .bind(parcel_id)
            .execute(&mut *projection_tx)
            .await;
    assert!(
        direct_projection_update.is_err(),
        "legacy PNU projection must not drift"
    );
    projection_tx.rollback().await?;

    tx.rollback().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn administrative_geometry_projection_is_valid_and_append_only(
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    let unit_id = Uuid::new_v4();
    let revision_id = Uuid::new_v4();
    let source_id = Uuid::new_v4();
    let projection_load_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO catalog.source_record (id, source, external_id, checksum_sha256)
         VALUES ($1, 'test', $2, repeat('a', 64))",
    )
    .bind(source_id)
    .bind(format!("geometry-{source_id}"))
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO catalog.administrative_boundary_revision
         (id, canonical_iceberg_snapshot_id, source_snapshot_id, source_record_id, status, validated_at)
         VALUES ($1, '9001', 'iceberg:geometry-test', $2, 'validated', now())",
    )
    .bind(revision_id)
    .bind(source_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO catalog.administrative_unit (id, unit_kind, stable_key)
         VALUES ($1, 'legal_dong', $2)",
    )
    .bind(unit_id)
    .bind(format!("scope:legal-dong:{unit_id}"))
    .execute(&mut *tx)
    .await?;
    // A publication row belongs to the load that materialised it, so the fixture opens one. The load
    // is `running`: this test asserts the projection's own geometry and append-only rules, and
    // nothing here promotes it — an unclosed load is exactly what a promotion must refuse.
    sqlx::query(
        "INSERT INTO serving_postgis.spatial_projection_load
         (id, publication_unit_key, data_revision, canonical_iceberg_snapshot_id, status)
         VALUES ($1, 'admin', $2, '9001', 'running')",
    )
    .bind(projection_load_id)
    .bind(revision_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO serving_postgis.administrative_unit_boundary_publication
         (administrative_unit_id, data_revision, canonical_iceberg_snapshot_id,
          source_snapshot_id, source_record_id, source_object_key, scope_kind,
          canonical_code, display_name, geometry_checksum_sha256, geom, properties,
          projection_load_id)
         VALUES ($1, $2, '9001', 'iceberg:geometry-test', $3,
                 'gold/admin-boundaries/geometry-test.geojson', 'legal_dong', '9999900101',
                 'Geometry Fixture', repeat('a', 64),
                 ST_Multi(ST_GeomFromText(
                   'POLYGON((127.1231 36.1231,127.1232 36.1231,127.1232 36.1232,127.1231 36.1232,127.1231 36.1231))', 4326)),
                 '{\"canonical_code\":\"9999900101\"}'::jsonb, $4)",
    )
    .bind(unit_id)
    .bind(revision_id)
    .bind(source_id)
    .bind(projection_load_id)
    .execute(&mut *tx)
    .await?;
    let row = sqlx::query(
        "SELECT public.st_srid(geom) AS srid, public.st_isvalid(geom) AS valid
           FROM serving_postgis.administrative_unit_boundary_publication
          WHERE data_revision = $1 AND administrative_unit_id = $2",
    )
    .bind(revision_id)
    .bind(unit_id)
    .fetch_one(&mut *tx)
    .await?;
    assert_eq!(row.try_get::<i32, _>("srid")?, 4326);
    assert!(row.try_get::<bool, _>("valid")?);

    let direct_update = sqlx::query(
        "UPDATE serving_postgis.administrative_unit_boundary_publication
            SET display_name = 'tampered'
          WHERE data_revision = $1 AND administrative_unit_id = $2",
    )
    .bind(revision_id)
    .bind(unit_id)
    .execute(&mut *tx)
    .await;
    assert!(
        direct_update.is_err(),
        "boundary projection must be append-only"
    );
    tx.rollback().await?;
    Ok(())
}
