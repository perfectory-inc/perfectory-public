//! Local PostGIS-backed parcel marker anchor rebuild tests.
//!
//! Skips when `DATABASE_URL` is not set or unreachable.

use catalog_application::{RebuildParcelMarkerAnchors, RebuildParcelMarkerAnchorsInput};
use catalog_infrastructure::PgParcelMarkerAnchorRebuilder;
use sqlx::{PgPool, Row};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPool::connect(&url).await.ok()
}

#[tokio::test]
async fn rebuilds_active_parcel_marker_anchors_from_postgis_mirror() -> TestResult {
    let Some(pool) = pool().await else {
        return Ok(());
    };
    let fixture = ParcelMarkerAnchorRebuildFixture::new();
    fixture.cleanup(&pool).await?;
    fixture.insert_mirror(&pool).await?;

    let rebuilder = std::sync::Arc::new(PgParcelMarkerAnchorRebuilder::new(pool.clone()));
    let use_case = RebuildParcelMarkerAnchors::new(rebuilder);
    let report = use_case
        .execute(RebuildParcelMarkerAnchorsInput {
            source_snapshot_id: fixture.source_snapshot_id.to_owned(),
            algorithm_version: "postgis-st_maximuminscribedcircle-v1".to_owned(),
            requested_by_staff_id: None,
            request_id: Some("anchor-rebuild-infra-test".to_owned()),
        })
        .await?;

    assert_eq!(report.source_snapshot_id, fixture.source_snapshot_id);
    assert_eq!(report.scanned_row_count, 1);
    assert_eq!(report.loaded_row_count, 1);
    assert_eq!(report.rejected_row_count, 0);
    assert_eq!(report.superseded_row_count, 0);

    let generation_run_quality_report = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT quality_report
         FROM catalog.parcel_marker_anchor_generation_run
         WHERE id = $1",
    )
    .bind(report.generation_run_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        generation_run_quality_report["request_id"].as_str(),
        Some("anchor-rebuild-infra-test")
    );
    assert_eq!(
        generation_run_quality_report["source_mirror_table"].as_str(),
        Some("serving_postgis.parcel_boundary_mirror")
    );

    let row = sqlx::query(
        "SELECT pma.pnu::text AS pnu,
                pma.source_geometry_version,
                pma.source_object_key,
                pma.source_row_id,
                pma.algorithm,
                pma.algorithm_version,
                pma.source_geometry_checksum_sha256,
                pma.is_active,
                ST_Contains(pb.geom, ST_Transform(pma.anchor_point, 5179)) AS anchor_inside
         FROM catalog.parcel_marker_anchor pma
         JOIN serving_postgis.parcel_boundary_mirror pb ON pb.pnu = pma.pnu
         WHERE pma.pnu = $1
           AND pma.source_geometry_version = $2
           AND pma.is_active = true",
    )
    .bind(fixture.pnu)
    .bind(fixture.source_snapshot_id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(row.try_get::<String, _>("pnu")?, fixture.pnu);
    assert_eq!(
        row.try_get::<String, _>("source_geometry_version")?,
        fixture.source_snapshot_id
    );
    assert_eq!(
        row.try_get::<String, _>("source_object_key")?,
        fixture.source_object_key
    );
    assert_eq!(
        row.try_get::<Option<String>, _>("source_row_id")?
            .as_deref(),
        Some(fixture.source_row_id)
    );
    assert_eq!(row.try_get::<String, _>("algorithm")?, "polylabel");
    assert_eq!(
        row.try_get::<String, _>("algorithm_version")?,
        "postgis-st_maximuminscribedcircle-v1"
    );
    assert_eq!(
        row.try_get::<String, _>("source_geometry_checksum_sha256")?,
        fixture.geometry_checksum_sha256
    );
    assert!(row.try_get::<bool, _>("is_active")?);
    assert!(row.try_get::<bool, _>("anchor_inside")?);

    fixture.cleanup(&pool).await?;
    Ok(())
}

struct ParcelMarkerAnchorRebuildFixture {
    mirror_run_id: &'static str,
    pnu: &'static str,
    source_snapshot_id: &'static str,
    source_object_key: &'static str,
    source_row_id: &'static str,
    geometry_checksum_sha256: &'static str,
}

impl ParcelMarkerAnchorRebuildFixture {
    const fn new() -> Self {
        Self {
            mirror_run_id: "018f0000-0000-7000-8000-00000000e001",
            pnu: "9999900501100090001",
            source_snapshot_id: "iceberg:parcel-marker-anchor-rebuild-test-0001",
            source_object_key: "gold/parcel-boundaries/anchor-rebuild-test.parquet",
            source_row_id: "vworld-cadastral:parcel-boundary:pnu:9999900501100090001",
            geometry_checksum_sha256:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        }
    }

    async fn insert_mirror(&self, pool: &PgPool) -> TestResult {
        sqlx::query(
            "INSERT INTO serving_postgis.parcel_boundary_mirror_rebuild_run
             (id, source_snapshot_id, source_table, srid, status, loaded_row_count,
              rejected_row_count, quality_report, started_at, finished_at)
             VALUES ($1, $2, 'silver.parcel_boundaries', 5179, 'succeeded', 1,
                     0, '{}'::jsonb, now(), now())",
        )
        .bind(self.mirror_run_id.parse::<uuid::Uuid>()?)
        .bind(self.source_snapshot_id)
        .execute(pool)
        .await?;

        sqlx::query(
            "INSERT INTO serving_postgis.parcel_boundary_mirror
             (pnu, rebuild_run_id, source_snapshot_id, source_table, source_object_key,
              source_row_id, geometry_checksum_sha256, properties, geom)
             VALUES ($1, $2, $3, 'silver.parcel_boundaries', $4, $5, $6,
                     '{}'::jsonb,
                     ST_Multi(ST_Transform(ST_SetSRID(ST_GeomFromText(
                         'POLYGON((127.12347023470 36.1234200,127.123470234710 36.1234200,127.123470234710 36.1234210,127.12347023470 36.1234210,127.12347023470 36.1234200))'
                     ), 4326), 5179)))",
        )
        .bind(self.pnu)
        .bind(self.mirror_run_id.parse::<uuid::Uuid>()?)
        .bind(self.source_snapshot_id)
        .bind(self.source_object_key)
        .bind(self.source_row_id)
        .bind(self.geometry_checksum_sha256)
        .execute(pool)
        .await?;

        Ok(())
    }

    async fn cleanup(&self, pool: &PgPool) -> TestResult {
        sqlx::query("DELETE FROM catalog.parcel_marker_anchor WHERE pnu = $1")
            .bind(self.pnu)
            .execute(pool)
            .await?;
        sqlx::query(
            "DELETE FROM catalog.parcel_marker_anchor_generation_run
             WHERE source_snapshot_id = $1",
        )
        .bind(self.source_snapshot_id)
        .execute(pool)
        .await?;
        sqlx::query("DELETE FROM serving_postgis.parcel_boundary_mirror WHERE pnu = $1")
            .bind(self.pnu)
            .execute(pool)
            .await?;
        sqlx::query("DELETE FROM serving_postgis.parcel_boundary_mirror_rebuild_run WHERE id = $1")
            .bind(self.mirror_run_id.parse::<uuid::Uuid>()?)
            .execute(pool)
            .await?;
        Ok(())
    }
}
