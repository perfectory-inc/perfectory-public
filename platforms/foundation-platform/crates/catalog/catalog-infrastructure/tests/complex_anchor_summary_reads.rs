//! Local PostGIS-backed complex anchor summary read tests.
//!
//! Skips when `DATABASE_URL` is not set or unreachable.

use catalog_application::ports::CatalogRepository;
use catalog_infrastructure::PgCatalogRepository;
use foundation_shared_kernel::ids::{ComplexId, ParcelId};
use sqlx::PgPool;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPool::connect(&url).await.ok()
}

#[tokio::test]
async fn reads_complex_anchor_summary_from_active_pnu_anchors() -> TestResult {
    let Some(pool) = pool().await else {
        return Ok(());
    };
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
        sqlx::query("DELETE FROM catalog.parcel WHERE complex_id = $1")
            .bind(self.complex_id.as_uuid())
            .execute(pool)
            .await?;
        sqlx::query("DELETE FROM catalog.industrial_complex WHERE id = $1")
            .bind(self.complex_id.as_uuid())
            .execute(pool)
            .await?;
        Ok(())
    }
}
