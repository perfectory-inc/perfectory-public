//! `PostgreSQL` tests for the v2 runtime-manifest compare-and-swap gate.
//!
//! These tests are ignored unless a migrated Foundation database is available. They exercise the
//! database function rather than duplicating its invariants in Rust, so a migration change cannot
//! silently remove the atomic pointer switch.

#![allow(clippy::expect_used, clippy::too_many_lines, clippy::unwrap_used)]

use catalog_application::ports::CatalogUnitOfWork;
use catalog_domain::CatalogError;
use catalog_infrastructure::PgCatalogUnitOfWork;
use sqlx::{PgPool, Row};
use std::sync::OnceLock;
use uuid::Uuid;

static RUNTIME_MANIFEST_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

async fn runtime_manifest_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    RUNTIME_MANIFEST_TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

/// Connection for this `#[ignore]`d live suite. Both failure modes abort the
/// test: a configured-but-unreachable database must not silently downgrade a
/// contract test into a no-op that still reports success — this helper used to
/// swallow the connection error without printing anything at all.
async fn pool() -> PgPool {
    let url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set; run `cargo xtask integration foundation`");
    PgPool::connect(&url)
        .await
        .expect("connect to the database in DATABASE_URL")
}

#[tokio::test]
#[ignore = "requires a migrated Foundation PostgreSQL database"]
async fn runtime_manifest_cas_switches_one_complete_unit() {
    let _guard = runtime_manifest_test_guard().await;
    let pool = pool().await;
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
    let _guard = runtime_manifest_test_guard().await;
    let pool = pool().await;
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
    source_record_id: Uuid,
    snapshot_id: String,
}

impl Fixture {
    fn new() -> Self {
        Self {
            // These are disposable database keys. Use random v4 IDs rather than timestamp-based
            // v7 IDs because Cargo runs ignored test binaries concurrently against one database;
            // test identity must not depend on clock ordering or a shared v7 generator state.
            unit_id: Uuid::new_v4(),
            release_id: Uuid::new_v4(),
            manifest_id: Uuid::new_v4(),
            data_revision: Uuid::new_v4(),
            source_record_id: Uuid::new_v4(),
            snapshot_id: format!("9{}", Uuid::new_v4().as_u128()),
        }
    }

    async fn insert(&self, pool: &PgPool) {
        sqlx::query(
            "INSERT INTO catalog.source_record (id, source, external_id, checksum_sha256)
             VALUES ($1, 'test', $2, repeat('a', 64))
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(self.source_record_id)
        .bind(format!("runtime-manifest-{}", self.source_record_id))
        .execute(pool)
        .await
        .expect("source record");
        sqlx::query(
            "INSERT INTO catalog.administrative_boundary_revision
             (id, canonical_iceberg_snapshot_id, source_snapshot_id, source_record_id,
              status, validated_at)
             VALUES ($1, $2, $3, $4, 'published', now())",
        )
        .bind(self.data_revision)
        .bind(&self.snapshot_id)
        .bind(format!("iceberg:runtime-manifest-{}", self.data_revision))
        .bind(self.source_record_id)
        .execute(pool)
        .await
        .expect("administrative boundary revision");
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
                     'https://tiles.example.test/cas_parcels/{z}/{x}/{y}', $6)",
        )
        .bind(self.release_id)
        .bind(self.unit_id)
        .bind(self.data_revision)
        .bind(&self.snapshot_id)
        .bind(self.source_record_id)
        .bind(Uuid::new_v4())
        .execute(pool)
        .await
        .expect("release");
        sqlx::query(
            "INSERT INTO catalog.vector_tile_release_layer
             (release_id, layer_id, source_layer, feature_id_property,
              tile_min_zoom, tile_max_zoom, render_min_zoom, render_max_zoom,
              feature_filter_properties)
             VALUES ($1, 'parcels', 'parcels', 'pnu', 14, 16, 14, 22, '{}'::jsonb)",
        )
        .bind(self.release_id)
        .execute(pool)
        .await
        .expect("release layer");
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
        sqlx::query(
            "UPDATE catalog.vector_tile_publication_unit
                SET active_release_id = NULL,
                    active_data_revision = NULL,
                    fallback_release_id = NULL,
                    fallback_data_revision = NULL
              WHERE id = $1",
        )
        .bind(self.unit_id)
        .execute(pool)
        .await
        .expect("publication unit pointer cleanup");
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
        // The revision ledger is append-only for ordinary API sessions. Test cleanup uses the
        // same transaction-local publisher capability as the Foundation publisher, so the guard
        // remains enabled in production and the pool connection cannot retain the override.
        let mut tx = pool.begin().await.expect("cleanup transaction");
        sqlx::query("SELECT set_config('foundation.temporal_publisher', 'on', true)")
            .execute(&mut *tx)
            .await
            .expect("enable temporal fixture cleanup");
        sqlx::query("DELETE FROM catalog.administrative_boundary_revision WHERE id = $1")
            .bind(self.data_revision)
            .execute(&mut *tx)
            .await
            .expect("administrative boundary revision cleanup");
        // Source records are immutable lineage. Reused fixture rows are deliberately retained so
        // a concurrent or rerun test cannot delete provenance that another fixture is using.
        tx.commit().await.expect("cleanup transaction commit");
    }
}

/// The migration writes the derivative root as a SQL literal because a `plpgsql` function cannot
/// call into the Rust crate. This is what keeps the two statements from drifting: it reads the
/// function body actually installed in the database, not a file, so it also proves the migration ran.
#[tokio::test]
#[ignore = "requires a migrated Foundation PostgreSQL database"]
async fn the_promotion_gate_and_the_domain_agree_on_the_release_object_root() {
    let pool = pool().await;
    let source: String = sqlx::query_scalar(
        "SELECT prosrc
         FROM pg_proc
         JOIN pg_namespace ON pg_namespace.oid = pg_proc.pronamespace
         WHERE pg_namespace.nspname = 'catalog'
           AND pg_proc.proname = 'promote_vector_tile_runtime_manifest'",
    )
    .fetch_one(&pool)
    .await
    .expect("the installed promotion function");

    let expected = format!("{}/%s.pmtiles", catalog_domain::STATIC_RELEASE_OBJECT_ROOT);
    assert!(
        source.contains(&expected),
        "the gate must compare the whole object key against {expected}; installed body:\n{source}"
    );
}

/// The bypass the gate's own comment claimed to close. The Martin source name below is
/// release-addressed and the filename matches it, so the object key prefix is the only reason the
/// promotion can fail — the gate checks the static identity before it checks the data revision.
///
/// The static release deliberately carries its *own* revision and snapshot rather than the active
/// ones. `vector_tile_release` is `UNIQUE (publication_unit_id, data_revision,
/// canonical_iceberg_snapshot_id)`, so a second release for the unit's active revision cannot be
/// inserted at all — a real blocker for the static promotion path, tracked separately. It is not this
/// test's subject: reusing the active revision fails at the insert and never reaches the gate.
#[tokio::test]
#[ignore = "requires a migrated Foundation PostgreSQL database"]
async fn a_static_release_at_a_foreign_object_prefix_cannot_be_promoted() {
    let _guard = runtime_manifest_test_guard().await;
    let pool = pool().await;
    let fixture = Fixture::new();
    fixture.insert(&pool).await;
    let uow = PgCatalogUnitOfWork::new(pool.clone());
    uow.promote_vector_tile_runtime_manifest(None, fixture.manifest_id)
        .await
        .expect("first dynamic promotion");

    let unit_key: String = sqlx::query_scalar(
        "SELECT unit_key FROM catalog.vector_tile_publication_unit WHERE id = $1",
    )
    .bind(fixture.unit_id)
    .fetch_one(&pool)
    .await
    .expect("unit key");
    let static_revision = Uuid::new_v4();
    let static_snapshot = format!("9{}", Uuid::new_v4().as_u128());
    sqlx::query(
        "INSERT INTO catalog.administrative_boundary_revision
         (id, canonical_iceberg_snapshot_id, source_snapshot_id, source_record_id,
          status, validated_at)
         VALUES ($1, $2, $3, $4, 'published', now())",
    )
    .bind(static_revision)
    .bind(&static_snapshot)
    .bind(format!("iceberg:static-prefix-{static_revision}"))
    .bind(fixture.source_record_id)
    .execute(&pool)
    .await
    .expect("static release revision");
    let static_release_id = Uuid::new_v4();
    let martin_source_id = format!("{unit_key}-{static_release_id}");
    let file_asset_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO catalog.file_asset (id, object_key, mime_type, size_bytes, visibility)
         VALUES ($1, $2, 'application/vnd.pmtiles', 1, 'private')",
    )
    .bind(file_asset_id)
    .bind(format!("staging/{martin_source_id}.pmtiles"))
    .execute(&pool)
    .await
    .expect("pmtiles file asset");
    sqlx::query(
        "INSERT INTO catalog.vector_tile_release
         (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
          source_record_id, source_kind, martin_source_id, tiles_url_template,
          pmtiles_object_key, pmtiles_file_asset_id, pmtiles_sha256, pmtiles_bytes,
          validated_at, validation_evidence_sha256)
         VALUES ($1, $2, $3, $4, $5, 'static_pmtiles', $6, $7, $8, $9, repeat('b', 64), 1,
                 now(), repeat('c', 64))",
    )
    .bind(static_release_id)
    .bind(fixture.unit_id)
    .bind(static_revision)
    .bind(&static_snapshot)
    .bind(fixture.source_record_id)
    .bind(&martin_source_id)
    .bind(format!(
        "https://tiles.example.test/{martin_source_id}/{{z}}/{{x}}/{{y}}"
    ))
    // Right filename, foreign prefix. The old gate compared only the tail and accepted this.
    .bind(format!("staging/{martin_source_id}.pmtiles"))
    .bind(file_asset_id)
    .execute(&pool)
    .await
    .expect("static release at a foreign prefix");

    let next_manifest_id = Uuid::new_v4();
    // `manifest_generation` is globally unique, so a literal would collide with any manifest another
    // test in this single-threaded suite left behind.
    let next_generation: i64 = sqlx::query_scalar(
        "SELECT coalesce(max(manifest_generation), 0) + 1 FROM catalog.vector_tile_runtime_manifest",
    )
    .fetch_one(&pool)
    .await
    .expect("next manifest generation");
    sqlx::query(
        "INSERT INTO catalog.vector_tile_runtime_manifest (id, manifest_generation) VALUES ($1, $2)",
    )
    .bind(next_manifest_id)
    .bind(next_generation)
    .execute(&pool)
    .await
    .expect("second runtime manifest");
    sqlx::query(
        "INSERT INTO catalog.vector_tile_runtime_manifest_unit
         (manifest_id, publication_unit_id, release_id, serving_generation,
          data_revision, canonical_iceberg_snapshot_id)
         VALUES ($1, $2, $3, 2, $4, $5)",
    )
    .bind(next_manifest_id)
    .bind(fixture.unit_id)
    .bind(static_release_id)
    .bind(static_revision)
    .bind(&static_snapshot)
    .execute(&pool)
    .await
    .expect("second runtime manifest unit");

    let error = uow
        .promote_vector_tile_runtime_manifest(Some(fixture.manifest_id), next_manifest_id)
        .await
        .expect_err("a foreign object prefix must not promote");
    let message = error.to_string();
    assert!(
        message.contains("non-release-addressed static PMTiles source"),
        "got: {message}"
    );

    let pointed_at: Uuid = sqlx::query_scalar(
        "SELECT manifest_id FROM catalog.vector_tile_runtime_manifest_pointer WHERE singleton",
    )
    .fetch_one(&pool)
    .await
    .expect("pointer after the refused promotion");
    assert_eq!(
        pointed_at, fixture.manifest_id,
        "the refused promotion must not move the pointer"
    );

    sqlx::query("DELETE FROM catalog.vector_tile_runtime_manifest_unit WHERE manifest_id = $1")
        .bind(next_manifest_id)
        .execute(&pool)
        .await
        .expect("second manifest unit cleanup");
    sqlx::query("DELETE FROM catalog.vector_tile_runtime_manifest WHERE id = $1")
        .bind(next_manifest_id)
        .execute(&pool)
        .await
        .expect("second manifest cleanup");
    sqlx::query("DELETE FROM catalog.vector_tile_release WHERE id = $1")
        .bind(static_release_id)
        .execute(&pool)
        .await
        .expect("static release cleanup");
    sqlx::query("DELETE FROM catalog.file_asset WHERE id = $1")
        .bind(file_asset_id)
        .execute(&pool)
        .await
        .expect("file asset cleanup");
    // The revision row is left behind on purpose: `reject_temporal_history_mutation` makes temporal
    // identity facts append-only, so deleting one raises 42501. `Fixture::cleanup` leaves its own
    // revision for the same reason, and the harness database is disposable.
    fixture.cleanup(&pool).await;
}
