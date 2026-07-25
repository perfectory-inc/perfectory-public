//! `PostgreSQL` tests for the v2 runtime-manifest compare-and-swap gate.
//!
//! These tests are ignored unless a migrated Foundation database is available. They exercise the
//! database function rather than duplicating its invariants in Rust, so a migration change cannot
//! silently remove the atomic pointer switch.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use catalog_application::ports::CatalogUnitOfWork;
use catalog_domain::CatalogError;
use catalog_infrastructure::PgCatalogUnitOfWork;
use sqlx::{PgPool, Row};
use uuid::Uuid;

async fn pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPool::connect(&url).await.ok()
}

#[tokio::test]
#[ignore = "requires a migrated Foundation PostgreSQL database"]
async fn runtime_manifest_cas_switches_one_complete_unit() {
    let Some(pool) = pool().await else {
        return;
    };
    let fixture = Fixture::new();
    fixture.insert(&pool).await;

    let uow = PgCatalogUnitOfWork::new(pool.clone());
    let generation = uow
        .promote_vector_tile_runtime_manifest(None, fixture.manifest_id)
        .await
        .expect("first runtime manifest promotion");
    assert_eq!(generation, 1);

    let row = sqlx::query(
        "SELECT p.manifest_id, u.active_release_id, u.active_data_revision, u.serving_generation
         FROM catalog.vector_tile_runtime_manifest_pointer p
         JOIN catalog.vector_tile_publication_unit u ON u.id = $1
         WHERE p.singleton",
    )
    .bind(fixture.unit_id)
    .fetch_one(&pool)
    .await
    .expect("active pointer and unit");
    assert_eq!(
        row.try_get::<Uuid, _>("manifest_id").unwrap(),
        fixture.manifest_id
    );
    assert_eq!(
        row.try_get::<Option<Uuid>, _>("active_release_id").unwrap(),
        Some(fixture.release_id)
    );
    assert_eq!(
        row.try_get::<Option<Uuid>, _>("active_data_revision")
            .unwrap(),
        Some(fixture.data_revision)
    );
    assert_eq!(row.try_get::<i64, _>("serving_generation").unwrap(), 1);

    fixture.cleanup(&pool).await;
}

#[tokio::test]
#[ignore = "requires a migrated Foundation PostgreSQL database"]
async fn runtime_manifest_cas_rejects_stale_writer_without_switching_pointer() {
    let Some(pool) = pool().await else {
        return;
    };
    let fixture = Fixture::new();
    fixture.insert(&pool).await;

    let uow = PgCatalogUnitOfWork::new(pool.clone());
    uow.promote_vector_tile_runtime_manifest(None, fixture.manifest_id)
        .await
        .expect("first runtime manifest promotion");
    let stale = uow
        .promote_vector_tile_runtime_manifest(Some(Uuid::now_v7()), fixture.manifest_id)
        .await
        .expect_err("stale writer must fail compare-and-swap");
    assert!(matches!(
        stale,
        CatalogError::InvalidVectorTileRuntimeManifest(message)
            if message.contains("compare-and-swap")
    ));

    let active: Uuid = sqlx::query_scalar(
        "SELECT manifest_id FROM catalog.vector_tile_runtime_manifest_pointer WHERE singleton",
    )
    .fetch_one(&pool)
    .await
    .expect("active pointer");
    assert_eq!(active, fixture.manifest_id);
    fixture.cleanup(&pool).await;
}

struct Fixture {
    unit_id: Uuid,
    release_id: Uuid,
    manifest_id: Uuid,
    data_revision: Uuid,
    snapshot_id: String,
}

impl Fixture {
    fn new() -> Self {
        let suffix = Uuid::new_v4().simple().to_string();
        Self {
            unit_id: Uuid::now_v7(),
            release_id: Uuid::now_v7(),
            manifest_id: Uuid::now_v7(),
            data_revision: Uuid::now_v7(),
            snapshot_id: format!("9{}", &suffix[..10]),
        }
    }

    async fn insert(&self, pool: &PgPool) {
        sqlx::query(
            "INSERT INTO catalog.vector_tile_publication_unit (id, unit_key)
             VALUES ($1, $2)",
        )
        .bind(self.unit_id)
        .bind(format!("cas-{}", &self.unit_id.simple().to_string()[..12]))
        .execute(pool)
        .await
        .expect("publication unit");
        sqlx::query(
            "INSERT INTO catalog.vector_tile_release
             (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
              source_record_id, source_kind, martin_source_id, tiles_url_template,
              postgis_projection_revision)
             VALUES ($1, $2, $3, $4, $5, 'dynamic_postgis', 'cas_parcels',
                     'https://tiles.example.test/parcels/{z}/{x}/{y}', $6)",
        )
        .bind(self.release_id)
        .bind(self.unit_id)
        .bind(self.data_revision)
        .bind(&self.snapshot_id)
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .execute(pool)
        .await
        .expect("release");
        sqlx::query(
            "INSERT INTO catalog.vector_tile_runtime_manifest (id, manifest_generation)
             VALUES ($1, 1)",
        )
        .bind(self.manifest_id)
        .execute(pool)
        .await
        .expect("runtime manifest");
        sqlx::query(
            "INSERT INTO catalog.vector_tile_runtime_manifest_unit
             (manifest_id, publication_unit_id, release_id, serving_generation,
              data_revision, canonical_iceberg_snapshot_id)
             VALUES ($1, $2, $3, 1, $4, $5)",
        )
        .bind(self.manifest_id)
        .bind(self.unit_id)
        .bind(self.release_id)
        .bind(self.data_revision)
        .bind(&self.snapshot_id)
        .execute(pool)
        .await
        .expect("runtime manifest unit");
    }

    async fn cleanup(&self, pool: &PgPool) {
        sqlx::query(
            "DELETE FROM catalog.vector_tile_runtime_manifest_pointer WHERE manifest_id = $1",
        )
        .bind(self.manifest_id)
        .execute(pool)
        .await
        .expect("pointer cleanup");
        sqlx::query("DELETE FROM catalog.vector_tile_runtime_manifest WHERE id = $1")
            .bind(self.manifest_id)
            .execute(pool)
            .await
            .expect("manifest cleanup");
        sqlx::query("DELETE FROM catalog.vector_tile_release WHERE id = $1")
            .bind(self.release_id)
            .execute(pool)
            .await
            .expect("release cleanup");
        sqlx::query("DELETE FROM catalog.vector_tile_publication_unit WHERE id = $1")
            .bind(self.unit_id)
            .execute(pool)
            .await
            .expect("unit cleanup");
    }
}
