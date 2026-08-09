//! Local PostGIS-backed complex anchor summary read tests.
//!
//! Live-database tests: ignored by default, run by
//! `cargo xtask integration foundation`, which refuses to start without
//! `DATABASE_URL`. A missing or unreachable database fails the test rather than
//! passing it — an unrun contract test must never read as a verified one.

use catalog_application::ports::CatalogRepository;
use catalog_infrastructure::PgCatalogRepository;
use foundation_shared_kernel::ids::{ComplexId, ParcelId};
use sqlx::PgPool;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn pool() -> TestResult<PgPool> {
    let url = std::env::var("DATABASE_URL")?;
    Ok(PgPool::connect(&url).await?)
}

#[tokio::test]
#[ignore = "requires a migrated Foundation PostGIS database in DATABASE_URL"]
async fn reads_complex_anchor_summary_from_active_pnu_anchors() -> TestResult {
    let pool = pool().await?;
    let fixture = ComplexAnchorSummaryFixture::new();
    fixture.cleanup(&pool).await?;
    fixture.insert(&pool).await?;

    let repo = PgCatalogRepository::new(pool.clone());
    let summary = repo
        .find_complex_anchor_summary(fixture.complex_id)
        .await?
        .ok_or_else(|| std::io::Error::other("missing complex anchor summary"))?;

    assert_eq!(summary.complex_id, fixture.complex_id);
    assert_eq!(summary.position_source, "pnu_anchor");
    assert_close(summary.center_lng, 127.123_470_234_80);
    assert_close(summary.center_lat, 36.123_430);
    assert_close(summary.min_lng, 127.123_470);
    assert_close(summary.min_lat, 36.123_420);
    assert_close(summary.max_lng, 127.123_470_234_90);
    assert_close(summary.max_lat, 36.123_440);
    assert_eq!(summary.anchor_count, 2);

    fixture.cleanup(&pool).await?;
    Ok(())
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 0.000_001,
        "expected {actual} to be close to {expected}"
    );
}

struct ComplexAnchorSummaryFixture {
    complex_id: ComplexId,
    official_complex_code: String,
    primary_bjdong_code: String,
    first_parcel_id: ParcelId,
    second_parcel_id: ParcelId,
    first_anchor_id: Uuid,
    second_anchor_id: Uuid,
    run_id: Uuid,
    first_pnu: String,
    second_pnu: String,
    source_snapshot_id: String,
    // The summary reads membership, not `catalog.parcel.complex_id` (ADR-0019 step 2). The step-1
    // backfill only reached parcels that existed when that migration ran, so a fixture creating
    // parcels afterwards has to state their membership itself — an inherited one does not exist.
    membership_source_record_id: Uuid,
    membership_revision_id: Uuid,
    membership_snapshot_id: String,
}

impl ComplexAnchorSummaryFixture {
    fn new() -> Self {
        let complex_id = ComplexId::new(Uuid::now_v7());
        let suffix = Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .filter(char::is_ascii_digit)
            .take(10)
            .collect::<String>();
        let primary_bjdong_code = format!("{suffix:0<10}")[..10].to_owned();
        let first_pnu = format!("{primary_bjdong_code}000000001");
        let second_pnu = format!("{primary_bjdong_code}000000002");

        Self {
            complex_id,
            official_complex_code: format!("IC-ANCHOR-{}", Uuid::new_v4().simple()),
            primary_bjdong_code,
            first_parcel_id: ParcelId::new(Uuid::now_v7()),
            second_parcel_id: ParcelId::new(Uuid::now_v7()),
            first_anchor_id: Uuid::now_v7(),
            second_anchor_id: Uuid::now_v7(),
            run_id: Uuid::now_v7(),
            first_pnu,
            second_pnu,
            source_snapshot_id: format!(
                "iceberg:complex-anchor-summary-{}",
                Uuid::new_v4().simple()
            ),
            membership_source_record_id: Uuid::now_v7(),
            membership_revision_id: Uuid::now_v7(),
            membership_snapshot_id: format!(
                "iceberg:anchor-summary-membership-{}",
                Uuid::new_v4().simple()
            ),
        }
    }

    async fn insert(&self, pool: &PgPool) -> TestResult {
        sqlx::query(
            "INSERT INTO catalog.industrial_complex
             (id, official_complex_code, name, kind, primary_bjdong_code, area_m2, version)
             VALUES ($1, $2, 'Anchor summary fixture', 'general', $3, 1000, 1)",
        )
        .bind(self.complex_id.as_uuid())
        .bind(&self.official_complex_code)
        .bind(&self.primary_bjdong_code)
        .execute(pool)
        .await?;

        sqlx::query(
            "INSERT INTO catalog.parcel
             (id, complex_id, pnu, kind, area_m2, version)
             VALUES ($1, $2, $3, 'factory', 500, 1),
                    ($4, $2, $5, 'factory', 500, 1)",
        )
        .bind(self.first_parcel_id.as_uuid())
        .bind(self.complex_id.as_uuid())
        .bind(&self.first_pnu)
        .bind(self.second_parcel_id.as_uuid())
        .bind(&self.second_pnu)
        .execute(pool)
        .await?;

        self.insert_membership(pool).await?;

        sqlx::query(
            "INSERT INTO catalog.parcel_marker_anchor_generation_run
             (id, source_snapshot_id, source_table, algorithm, algorithm_version,
              status, loaded_row_count, rejected_row_count, started_at, finished_at)
             VALUES ($1, $2, 'silver.parcel_boundaries', 'polylabel', 'polylabel:1',
                     'succeeded', 2, 0, now(), now())",
        )
        .bind(self.run_id)
        .bind(&self.source_snapshot_id)
        .execute(pool)
        .await?;

        sqlx::query(
            "INSERT INTO catalog.parcel_marker_anchor
             (id, pnu, parcel_id, generation_run_id, source_geometry_version, source_table,
              source_object_key, anchor_point, algorithm, algorithm_version,
              source_geometry_checksum_sha256, computed_at_utc, activated_at_utc, is_active)
             VALUES
             ($1, $2, $3, $4, $5, 'silver.parcel_boundaries',
              'gold/parcel-boundaries/complex-anchor-summary-first.parquet',
              ST_SetSRID(ST_MakePoint(127.123470, 36.123420), 4326),
              'polylabel', 'polylabel:1', repeat('a', 64), now(), now(), true),
             ($6, $7, $8, $4, $5, 'silver.parcel_boundaries',
              'gold/parcel-boundaries/complex-anchor-summary-second.parquet',
              ST_SetSRID(ST_MakePoint(127.12347023490, 36.123440), 4326),
              'polylabel', 'polylabel:1', repeat('b', 64), now(), now(), true)",
        )
        .bind(self.first_anchor_id)
        .bind(&self.first_pnu)
        .bind(self.first_parcel_id.as_uuid())
        .bind(self.run_id)
        .bind(&self.source_snapshot_id)
        .bind(self.second_anchor_id)
        .bind(&self.second_pnu)
        .bind(self.second_parcel_id.as_uuid())
        .execute(pool)
        .await?;

        Ok(())
    }

    /// States that both fixture parcels belong to the fixture complex today.
    ///
    /// Separate from `insert` because it is a different job: the anchors above are the thing under
    /// test, and this is the membership the summary now filters by (ADR-0019 step 2). The interval
    /// opens in the past and stays open, so `parcel_current_complex` selects both rows on any day
    /// this test runs.
    async fn insert_membership(&self, pool: &PgPool) -> TestResult {
        sqlx::query(
            "INSERT INTO catalog.source_record (id, source, external_id, checksum_sha256)
             VALUES ($1, 'test', $2, repeat('c', 64))",
        )
        .bind(self.membership_source_record_id)
        .bind(format!(
            "anchor-summary-membership-{}",
            self.membership_source_record_id
        ))
        .execute(pool)
        .await?;

        sqlx::query(
            "INSERT INTO catalog.administrative_boundary_revision
             (id, canonical_iceberg_snapshot_id, source_snapshot_id, source_record_id, status,
              validated_at)
             VALUES ($1, '8101', $2, $3, 'validated', now())",
        )
        .bind(self.membership_revision_id)
        .bind(&self.membership_snapshot_id)
        .bind(self.membership_source_record_id)
        .execute(pool)
        .await?;

        sqlx::query(
            "INSERT INTO catalog.parcel_complex_membership
             (id, parcel_id, complex_id, asserted_by, effective_period, data_revision,
              source_snapshot_id, source_record_id)
             VALUES (gen_random_uuid(), $1, $2, 'official_list', '[2020-01-01,)'::daterange,
                     $3, $4, $5),
                    (gen_random_uuid(), $6, $2, 'official_list', '[2020-01-01,)'::daterange,
                     $3, $4, $5)",
        )
        .bind(self.first_parcel_id.as_uuid())
        .bind(self.complex_id.as_uuid())
        .bind(self.membership_revision_id)
        .bind(&self.membership_snapshot_id)
        .bind(self.membership_source_record_id)
        .bind(self.second_parcel_id.as_uuid())
        .execute(pool)
        .await?;

        Ok(())
    }

    async fn cleanup(&self, pool: &PgPool) -> TestResult {
        sqlx::query(
            "DELETE FROM catalog.parcel_marker_anchor
             WHERE generation_run_id = $1
                OR pnu = $2
                OR pnu = $3",
        )
        .bind(self.run_id)
        .bind(&self.first_pnu)
        .bind(&self.second_pnu)
        .execute(pool)
        .await?;
        sqlx::query(
            "DELETE FROM catalog.parcel_marker_anchor_generation_run
             WHERE id = $1 OR source_snapshot_id = $2",
        )
        .bind(self.run_id)
        .bind(&self.source_snapshot_id)
        .execute(pool)
        .await?;
        // Membership rows and revisions are append-only, and the parcel foreign key is RESTRICT, so
        // they have to go first and only the publisher capability may remove them. Taking that
        // capability here is the point of the trigger rather than a way around it: a fixture that
        // could erase history without it would mean nothing else needed it either.
        //
        // One transaction, and `set_config(.., true)` so the setting is transaction-scoped. Session
        // scope (`false`) does not work through a pool: each `execute(pool)` borrows whatever
        // connection is free, so the setting and the DELETE land on different sessions and the
        // trigger refuses the delete. That is the 42501 this cleanup first failed with.
        let mut tx = pool.begin().await?;
        sqlx::query("SELECT set_config('foundation.temporal_publisher', 'on', true)")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM catalog.parcel_complex_membership WHERE complex_id = $1")
            .bind(self.complex_id.as_uuid())
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM catalog.parcel WHERE complex_id = $1")
            .bind(self.complex_id.as_uuid())
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM catalog.industrial_complex WHERE id = $1")
            .bind(self.complex_id.as_uuid())
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM catalog.administrative_boundary_revision WHERE id = $1")
            .bind(self.membership_revision_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM catalog.source_record WHERE id = $1")
            .bind(self.membership_source_record_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}
