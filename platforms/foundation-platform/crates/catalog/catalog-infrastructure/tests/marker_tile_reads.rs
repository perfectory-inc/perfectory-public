//! Local PostGIS-backed marker tile read tests.
//!
//! Live-database tests: ignored by default, run by
//! `cargo xtask integration foundation`, which refuses to start without
//! `DATABASE_URL`. A missing or unreachable database fails the test rather than
//! passing it — an unrun contract test must never read as a verified one.

use catalog_application::ports::CatalogRepository;
use catalog_domain::MarkerTileRequest;
use catalog_infrastructure::PgCatalogRepository;
use sqlx::PgPool;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn pool() -> TestResult<PgPool> {
    let url = std::env::var("DATABASE_URL")?;
    Ok(PgPool::connect(&url).await?)
}

#[tokio::test]
#[ignore = "requires a migrated Foundation PostGIS database in DATABASE_URL"]
async fn reads_active_parcel_anchor_mvt_tile_without_data_cap() -> TestResult {
    let pool = pool().await?;
    let fixture = MarkerTileFixture::new();
    fixture.cleanup(&pool).await?;
    fixture.insert(&pool).await?;

    let (x, y) = fixture.tile_address(&pool, 12).await?;

    let repo = PgCatalogRepository::new(pool.clone());
    let tile = repo
        .get_marker_tile(MarkerTileRequest::new(
            "parcel_anchor",
            12,
            x,
            y,
            "all-active-v1",
        )?)
        .await?;

    assert!(!tile.is_empty());

    fixture.cleanup(&pool).await?;
    Ok(())
}

struct MarkerTileFixture {
    run_id: &'static str,
    anchor_id: &'static str,
    pnu: &'static str,
    source_snapshot_id: &'static str,
    longitude: f64,
    latitude: f64,
}

impl MarkerTileFixture {
    const fn new() -> Self {
        Self {
            run_id: "018f0000-0000-7000-8000-00000000c001",
            anchor_id: "018f0000-0000-7000-8000-00000000d001",
            pnu: "9999999999900000001",
            source_snapshot_id: "iceberg:marker-tile-read-test-0001",
            longitude: 127.123470,
            latitude: 36.123420,
        }
    }

    async fn insert(&self, pool: &PgPool) -> TestResult {
        sqlx::query(
            "INSERT INTO catalog.parcel_marker_anchor_generation_run
             (id, source_snapshot_id, source_table, algorithm, algorithm_version,
              status, loaded_row_count, rejected_row_count, started_at, finished_at)
             VALUES ($1, $2, 'silver.parcel_boundaries', 'polylabel', 'polylabel:1',
                     'succeeded', 1, 0, now(), now())",
        )
        .bind(self.run_id.parse::<uuid::Uuid>()?)
        .bind(self.source_snapshot_id)
        .execute(pool)
        .await?;

        sqlx::query(
            "INSERT INTO catalog.parcel_marker_anchor
             (id, pnu, generation_run_id, source_geometry_version, source_table,
              source_object_key, anchor_point, algorithm, algorithm_version,
              source_geometry_checksum_sha256, computed_at_utc, activated_at_utc, is_active)
             VALUES ($1, $2, $3, $4, 'silver.parcel_boundaries',
                     'gold/parcel-boundaries/marker-tile-read-test.parquet',
                     ST_SetSRID(ST_MakePoint($5, $6), 4326),
                     'polylabel', 'polylabel:1', repeat('c', 64), now(), now(), true)",
        )
        .bind(self.anchor_id.parse::<uuid::Uuid>()?)
        .bind(self.pnu)
        .bind(self.run_id.parse::<uuid::Uuid>()?)
        .bind(self.source_snapshot_id)
        .bind(self.longitude)
        .bind(self.latitude)
        .execute(pool)
        .await?;

        Ok(())
    }

    async fn tile_address(&self, pool: &PgPool, zoom: i32) -> TestResult<(i32, i32)> {
        Ok(sqlx::query_as(
            "SELECT
                 floor((($1::double precision + 180) / 360) * power(2, $3))::int,
                 floor((1 - asinh(tan(radians($2::double precision))) / pi()) / 2
                       * power(2, $3))::int",
        )
        .bind(self.longitude)
        .bind(self.latitude)
        .bind(zoom)
        .fetch_one(pool)
        .await?)
    }

    async fn cleanup(&self, pool: &PgPool) -> TestResult {
        sqlx::query(
            "DELETE FROM catalog.parcel_marker_anchor
             WHERE id = $1
                OR pnu = $2
                OR generation_run_id = $3
                OR source_geometry_version = $4",
        )
        .bind(self.anchor_id.parse::<uuid::Uuid>()?)
        .bind(self.pnu)
        .bind(self.run_id.parse::<uuid::Uuid>()?)
        .bind(self.source_snapshot_id)
        .execute(pool)
        .await?;
        sqlx::query(
            "DELETE FROM catalog.parcel_marker_anchor_generation_run
             WHERE id = $1
                OR source_snapshot_id = $2",
        )
        .bind(self.run_id.parse::<uuid::Uuid>()?)
        .bind(self.source_snapshot_id)
        .execute(pool)
        .await?;
        Ok(())
    }
}
