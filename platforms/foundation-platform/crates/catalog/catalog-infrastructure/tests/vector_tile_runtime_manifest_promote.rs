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

/// The four ways a release can name a projection load that does not back what the manifest claims.
///
/// Each case mutates exactly one column of an otherwise-promotable load, so a passing case cannot be
/// explained by anything else in the fixture. The `expect_err` is deliberately the *only* assertion
/// that names the gate: the pointer assertion after it is what proves the refusal happened before any
/// state moved, which is the property that matters — a gate that rejects after switching the pointer
/// would still return an error and still be broken.
///
/// Before this gate existed all four of these promoted cleanly, and Martin then served zero features
/// out of a unit the runtime manifest advertised as live.
#[tokio::test]
#[ignore = "requires a migrated Foundation PostgreSQL database"]
async fn a_dynamic_release_without_a_matching_succeeded_projection_load_cannot_be_promoted() {
    // (label, the single-column edit that breaks the load's claim)
    let cases: [(&str, &str); 4] = [
        (
            "the load never succeeded",
            "UPDATE serving_postgis.spatial_projection_load
                SET status = 'running', finished_at = NULL, loaded_row_count = 0
              WHERE id = $1",
        ),
        (
            "the load materialised a different unit",
            "UPDATE serving_postgis.spatial_projection_load
                SET publication_unit_key = 'someone-elses-unit'
              WHERE id = $1",
        ),
        (
            "the load carries a different data revision",
            "UPDATE serving_postgis.spatial_projection_load
                SET data_revision = gen_random_uuid()
              WHERE id = $1",
        ),
        (
            "the load was built from a different Iceberg snapshot",
            "UPDATE serving_postgis.spatial_projection_load
                SET canonical_iceberg_snapshot_id = '811111111111111111'
              WHERE id = $1",
        ),
    ];

    for (label, break_the_load) in cases {
        let _guard = runtime_manifest_test_guard().await;
        let pool = pool().await;
        let fixture = Fixture::new();
        fixture.insert(&pool).await;
        sqlx::query(break_the_load)
            .bind(fixture.projection_load_id)
            .execute(&pool)
            .await
            .expect("breaking the projection load");
        let pointer_before = current_pointer(&pool).await;

        let uow = PgCatalogUnitOfWork::new(pool.clone());
        let refused = uow
            .promote_vector_tile_runtime_manifest(None, fixture.manifest_id)
            .await
            .expect_err(label);
        assert!(
            matches!(
                &refused,
                CatalogError::InvalidVectorTileRuntimeManifest(message)
                    if message.contains("no succeeded PostGIS projection load")
            ),
            "{label}: got {refused:?}"
        );

        assert_eq!(
            current_pointer(&pool).await,
            pointer_before,
            "{label}: the pointer moved even though the promotion was refused"
        );

        // The load row is the fixture's, so the ordinary cleanup removes it along with the release.
        fixture.cleanup(&pool).await;
    }
}

/// The manifest the runtime pointer currently selects, or `None` before any publication.
async fn current_pointer(pool: &PgPool) -> Option<Uuid> {
    sqlx::query_scalar::<_, Uuid>(
        "SELECT manifest_id FROM catalog.vector_tile_runtime_manifest_pointer WHERE singleton",
    )
    .fetch_optional(pool)
    .await
    .expect("runtime manifest pointer")
}

struct Fixture {
    unit_id: Uuid,
    release_id: Uuid,
    manifest_id: Uuid,
    data_revision: Uuid,
    source_record_id: Uuid,
    snapshot_id: String,
    /// The `serving_postgis.spatial_projection_load` the dynamic release serves rows out of.
    ///
    /// This used to be a bare `Uuid::new_v4()` bound straight into
    /// `postgis_projection_revision`. The column now carries a foreign key and the gate refuses a
    /// dynamic unit whose load did not succeed, so an unbacked id no longer reaches the gate at all.
    projection_load_id: Uuid,
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
            projection_load_id: Uuid::new_v4(),
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
        let unit_key = format!("cas-{}", &self.unit_id.simple().to_string()[..12]);
        sqlx::query(
            "INSERT INTO catalog.vector_tile_publication_unit (id, unit_key)
             VALUES ($1, $2)",
        )
        .bind(self.unit_id)
        .bind(&unit_key)
        .execute(pool)
        .await
        .expect("publication unit");
        // The gate compares the load's unit key against the unit the manifest selects and its
        // revision against the one the manifest names, so both are bound from the same fixture
        // values rather than restated. `loaded_row_count` is 1 because the `succeeded` CHECK refuses
        // a load that materialised nothing; these CAS tests exercise the pointer switch, and the
        // publication rows a real load writes are the subject of `spatial_projection_load_ledger`.
        sqlx::query(
            "INSERT INTO serving_postgis.spatial_projection_load
                (id, publication_unit_key, data_revision, canonical_iceberg_snapshot_id,
                 status, loaded_row_count, finished_at)
             VALUES ($1, $2, $3, $4, 'succeeded', 1, now())",
        )
        .bind(self.projection_load_id)
        .bind(&unit_key)
        .bind(self.data_revision)
        .bind(&self.snapshot_id)
        .execute(pool)
        .await
        .expect("spatial projection load");
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
        .bind(self.projection_load_id)
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
        // `manifest_generation` is globally unique and these tests share one database
        // sequentially. A literal `1` made every fixture depend on every earlier test having
        // cleaned up, so one panic mid-test cascaded into unrelated failures. Deriving the next
        // generation removes that coupling.
        let manifest_generation: i64 = sqlx::query_scalar(
            "SELECT coalesce(max(manifest_generation), 0) + 1
             FROM catalog.vector_tile_runtime_manifest",
        )
        .fetch_one(pool)
        .await
        .expect("next manifest generation");
        sqlx::query(
            "INSERT INTO catalog.vector_tile_runtime_manifest (id, manifest_generation)
             VALUES ($1, $2)",
        )
        .bind(self.manifest_id)
        .bind(manifest_generation)
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
        // After the release, which carries the foreign key to it.
        sqlx::query("DELETE FROM serving_postgis.spatial_projection_load WHERE id = $1")
            .bind(self.projection_load_id)
            .execute(pool)
            .await
            .expect("spatial projection load cleanup");
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

/// The static promotion path, end to end at the database boundary: a static release for the unit's
/// *active* revision and snapshot now inserts, promotes, and leaves the dynamic release it replaced
/// available as the same-revision fallback.
///
/// Before `20260730000002` the first insert here failed with 23505 and the path was unreachable. The
/// assertion order matters — the insert is the part that used to be impossible, so a failure there is
/// a regression of the uniqueness key rather than of the gate.
#[tokio::test]
#[ignore = "requires a migrated Foundation PostgreSQL database"]
async fn a_static_release_can_replace_the_dynamic_release_of_the_same_revision() {
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
    let static_release_id = Uuid::new_v4();
    let martin_source_id = format!("{unit_key}-{static_release_id}");
    let object_key = format!(
        "{}/{martin_source_id}.pmtiles",
        catalog_domain::STATIC_RELEASE_OBJECT_ROOT
    );
    let file_asset_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO catalog.file_asset (id, object_key, mime_type, size_bytes, visibility)
         VALUES ($1, $2, 'application/vnd.pmtiles', 1, 'private')",
    )
    .bind(file_asset_id)
    .bind(&object_key)
    .execute(&pool)
    .await
    .expect("pmtiles file asset");

    // Same unit, same revision, same snapshot as the active dynamic release. This is the insert the
    // old uniqueness key refused.
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
    .bind(fixture.data_revision)
    .bind(&fixture.snapshot_id)
    .bind(fixture.source_record_id)
    .bind(&martin_source_id)
    .bind(format!(
        "https://tiles.example.test/{martin_source_id}/{{z}}/{{x}}/{{y}}"
    ))
    .bind(&object_key)
    .bind(file_asset_id)
    .execute(&pool)
    .await
    .expect("a static release must coexist with the dynamic release of the same revision");
    sqlx::query(
        "INSERT INTO catalog.vector_tile_release_layer
         (release_id, layer_id, source_layer, feature_id_property,
          tile_min_zoom, tile_max_zoom, render_min_zoom, render_max_zoom,
          feature_filter_properties)
         VALUES ($1, 'parcels', 'parcels', 'pnu', 14, 16, 14, 22, '{}'::jsonb)",
    )
    .bind(static_release_id)
    .execute(&pool)
    .await
    .expect("static release layer");

    let next_manifest_id = Uuid::new_v4();
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
    .bind(fixture.data_revision)
    .bind(&fixture.snapshot_id)
    .execute(&pool)
    .await
    .expect("second runtime manifest unit");

    uow.promote_vector_tile_runtime_manifest(Some(fixture.manifest_id), next_manifest_id)
        .await
        .expect("the static promotion must pass the gate");

    // The fallback is written *after* the promotion, not before. `fallback_distinct_check` forbids it
    // equalling `active_release_id`, and before the promotion the active release still *is* the
    // dynamic one — so a promoter that wrote the fallback first would be refused. This is the
    // ordering constraint the transaction in Task 6 Step 6 has to follow: capture the previous
    // release id, call the gate, then record it as the fallback.
    sqlx::query(
        "UPDATE catalog.vector_tile_publication_unit
            SET fallback_release_id = $2,
                fallback_data_revision = $3
          WHERE id = $1",
    )
    .bind(fixture.unit_id)
    .bind(fixture.release_id)
    .bind(fixture.data_revision)
    .execute(&pool)
    .await
    .expect("the replaced dynamic release must be recordable as the same-revision fallback");

    let row = sqlx::query(
        "SELECT active_release_id, fallback_release_id, active_data_revision,
                fallback_data_revision, serving_generation
         FROM catalog.vector_tile_publication_unit
         WHERE id = $1",
    )
    .bind(fixture.unit_id)
    .fetch_one(&pool)
    .await
    .expect("unit after the static promotion");
    assert_eq!(
        row.try_get::<Option<Uuid>, _>("active_release_id").unwrap(),
        Some(static_release_id)
    );
    // Both point at the same revision, which is what `fallback_revision_check` demands and what
    // makes the same-revision rollback in Task 6 Step 4 possible at all.
    assert_eq!(
        row.try_get::<Option<Uuid>, _>("fallback_release_id")
            .unwrap(),
        Some(fixture.release_id)
    );
    assert_eq!(
        row.try_get::<Option<Uuid>, _>("fallback_data_revision")
            .unwrap(),
        row.try_get::<Option<Uuid>, _>("active_data_revision")
            .unwrap()
    );
    assert_eq!(row.try_get::<i64, _>("serving_generation").unwrap(), 2);

    sqlx::query(
        "UPDATE catalog.vector_tile_publication_unit
            SET active_release_id = NULL, active_data_revision = NULL,
                fallback_release_id = NULL, fallback_data_revision = NULL
          WHERE id = $1",
    )
    .bind(fixture.unit_id)
    .execute(&pool)
    .await
    .expect("unit pointer cleanup");
    sqlx::query("DELETE FROM catalog.vector_tile_runtime_manifest_unit WHERE manifest_id = $1")
        .bind(next_manifest_id)
        .execute(&pool)
        .await
        .expect("second manifest unit cleanup");
    sqlx::query("DELETE FROM catalog.vector_tile_runtime_manifest_pointer WHERE manifest_id = $1")
        .bind(next_manifest_id)
        .execute(&pool)
        .await
        .expect("pointer cleanup");
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
    fixture.cleanup(&pool).await;
}
