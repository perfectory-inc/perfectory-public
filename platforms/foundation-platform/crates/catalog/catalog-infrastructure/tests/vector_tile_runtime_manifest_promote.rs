//! `PostgreSQL` tests for the v2 runtime-manifest compare-and-swap gate.
//!
//! These tests are ignored unless a migrated Foundation database is available. They exercise the
//! database function rather than duplicating its invariants in Rust, so a migration change cannot
//! silently remove the atomic pointer switch.

#![allow(clippy::expect_used, clippy::too_many_lines, clippy::unwrap_used)]

use catalog_application::ports::CatalogUnitOfWork;
use catalog_domain::CatalogError;
use catalog_infrastructure::PgCatalogUnitOfWork;
use foundation_disposable_database::{disposable_database_url, DisposableDatabaseUrl};
use sqlx::{PgPool, Row};
use uuid::Uuid;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../migrations");

struct TestDatabase {
    guard: DisposableDatabaseUrl,
    pool: PgPool,
}

impl TestDatabase {
    async fn create(label: &str) -> Self {
        let guard = disposable_database_url(label)
            .await
            .expect("create disposable runtime-manifest database");
        let pool = guard.pool().await.expect("connect disposable database");
        MIGRATOR
            .run(&pool)
            .await
            .expect("migrate disposable database");
        Self { guard, pool }
    }

    async fn finish(self) {
        self.pool.close().await;
        self.guard
            .drop_database()
            .await
            .expect("drop disposable runtime-manifest database");
    }
}

/// Connection for this `#[ignore]`d live suite. Both failure modes abort the
/// test: a configured-but-unreachable database must not silently downgrade a
/// contract test into a no-op that still reports success — this helper used to
/// swallow the connection error without printing anything at all.
#[tokio::test]
#[ignore = "requires a migrated Foundation PostgreSQL database"]
async fn runtime_manifest_cas_switches_one_complete_unit() {
    let database = TestDatabase::create("runtime_manifest_switch").await;
    let pool = database.pool.clone();
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

    database.finish().await;
}

#[tokio::test]
#[ignore = "requires a migrated Foundation PostgreSQL database"]
async fn runtime_manifest_cas_rejects_stale_writer_without_switching_pointer() {
    let database = TestDatabase::create("runtime_manifest_stale").await;
    let pool = database.pool.clone();
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
    database.finish().await;
}

/// The ways a release can still name a projection load that does not back what the manifest claims.
///
/// The `expect_err` is deliberately the *only* assertion that names the gate: the pointer assertion
/// after it is what proves the refusal happened before any state moved, which is the property that
/// matters — a gate that rejects after switching the pointer would still return an error and still
/// be broken.
///
/// Before this gate existed both of these promoted cleanly, and Martin then served zero features out
/// of a unit the runtime manifest advertised as live.
#[tokio::test]
#[ignore = "requires a migrated Foundation PostgreSQL database"]
async fn a_dynamic_release_without_a_matching_succeeded_projection_load_cannot_be_promoted() {
    // Only one of the gate's conditions is still reachable by editing the load in place, and that is
    // the point of `20260731000002`. A load now names its unit by foreign key and its
    // `(data_revision, publication_unit_id, canonical_iceberg_snapshot_id)` triple is a foreign key
    // into `catalog.publication_revision`, so "different unit", "different revision" and "different
    // snapshot" are no longer states a row can hold — they are 23503 at write time. Those three
    // moved from a gate check to a schema constraint;
    // `a_load_cannot_be_written_for_another_units_revision` below is what asserts they are refused.
    //
    // What remains reachable, and therefore still worth a gate, is a release pointing at a load that
    // is internally consistent but describes a *different* revision than the manifest selects. The
    // second case builds exactly that.
    let cases: [(&str, &str); 1] = [(
        "the release points at a load for another revision of the same unit",
        // A second revision of this unit, a second succeeded load under it, and the release
        // re-pointed at that load. Everything is internally consistent; only the agreement
        // between the load and the manifest's selection is broken.
        "WITH other_revision AS (
                 INSERT INTO catalog.publication_revision
                     (id, publication_unit_id, canonical_iceberg_snapshot_id, source_record_id)
                 SELECT gen_random_uuid(), load.publication_unit_id, '811111111111111111',
                        release.source_record_id
                   FROM serving_postgis.spatial_projection_load AS load
                   JOIN catalog.vector_tile_release AS release
                     ON release.postgis_projection_revision = load.id
                  WHERE load.id = $1
                 RETURNING id, publication_unit_id, canonical_iceberg_snapshot_id
             ), other_load AS (
                 INSERT INTO serving_postgis.spatial_projection_load
                     (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
                      status, loaded_row_count, finished_at)
                 SELECT gen_random_uuid(), other_revision.publication_unit_id, other_revision.id,
                        other_revision.canonical_iceberg_snapshot_id, 'succeeded', 1, now()
                   FROM other_revision
                 RETURNING id
             )
             UPDATE catalog.vector_tile_release
                SET postgis_projection_revision = (SELECT id FROM other_load)
              WHERE postgis_projection_revision = $1",
    )];

    for (case_index, (label, break_the_load)) in cases.into_iter().enumerate() {
        let database = TestDatabase::create(&format!("runtime_manifest_gate_{case_index}")).await;
        let pool = database.pool.clone();
        let fixture = Fixture::new();
        fixture.insert(&pool).await;
        let mut break_tx = pool.begin().await.expect("break transaction");
        sqlx::query("SELECT set_config('foundation.temporal_publisher', 'on', true)")
            .execute(&mut *break_tx)
            .await
            .expect("publisher capability");
        sqlx::query(break_the_load)
            .bind(fixture.projection_load_id)
            .execute(&mut *break_tx)
            .await
            .expect("breaking the projection load");
        break_tx.commit().await.expect("break transaction commit");
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

        database.finish().await;
    }
}

/// A revision belongs to the unit it revises, and the database is what says so.
///
/// Before `20260731000002` this was three of the promotion gate's disjuncts, checked once at
/// promotion time against a `publication_unit_key` the load carried as free text — kept in step with
/// `vector_tile_publication_unit.unit_key` by a comment saying "the two spellings must not drift".
/// A comment is not a constraint. Now the load's `(data_revision, publication_unit_id,
/// canonical_iceberg_snapshot_id)` is a foreign key into `catalog.publication_revision`, so a load
/// naming another unit's revision cannot be written at all — the gate never has to notice it.
#[tokio::test]
#[ignore = "requires a migrated Foundation PostgreSQL database"]
async fn a_load_cannot_be_written_for_another_units_revision() {
    let database = TestDatabase::create("runtime_manifest_foreign_revision").await;
    let pool = database.pool.clone();
    let fixture = Fixture::new();
    fixture.insert(&pool).await;

    // A second unit, with no revision of its own.
    let foreign_unit_id = Uuid::new_v4();
    sqlx::query("INSERT INTO catalog.vector_tile_publication_unit (id, unit_key) VALUES ($1, $2)")
        .bind(foreign_unit_id)
        .bind(format!(
            "foreign-{}",
            &foreign_unit_id.simple().to_string()[..12]
        ))
        .execute(&pool)
        .await
        .expect("foreign publication unit");

    let refused = sqlx::query(
        "INSERT INTO serving_postgis.spatial_projection_load
            (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
             status, loaded_row_count, finished_at)
         VALUES (gen_random_uuid(), $1, $2, $3, 'succeeded', 1, now())",
    )
    .bind(foreign_unit_id)
    .bind(fixture.data_revision)
    .bind(&fixture.snapshot_id)
    .execute(&pool)
    .await
    .expect_err("a load may not claim another unit's revision");
    assert_eq!(
        refused
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some("23503"),
        "expected a foreign-key violation, got {refused:?}"
    );

    sqlx::query("DELETE FROM catalog.vector_tile_publication_unit WHERE id = $1")
        .bind(foreign_unit_id)
        .execute(&pool)
        .await
        .expect("foreign unit cleanup");
    database.finish().await;
}

/// Registers a revision of a publication unit, through the publisher capability the ledger requires.
///
/// Every release and every projection load carries a foreign key into this table, so a fixture that
/// mints a release now mints its revision first — there is no longer a way to name a revision that
/// does not belong to the unit the release is for.
async fn seed_publication_revision(
    pool: &PgPool,
    revision_id: Uuid,
    unit_id: Uuid,
    snapshot_id: &str,
    source_record_id: Uuid,
) {
    let mut tx = pool.begin().await.expect("revision transaction");
    sqlx::query("SELECT set_config('foundation.temporal_publisher', 'on', true)")
        .execute(&mut *tx)
        .await
        .expect("publisher capability");
    sqlx::query(
        "INSERT INTO catalog.publication_revision
            (id, publication_unit_id, canonical_iceberg_snapshot_id, source_record_id,
             derived_from_administrative_revision)
         VALUES ($1, $2, $3, $4, $1)",
    )
    .bind(revision_id)
    .bind(unit_id)
    .bind(snapshot_id)
    .bind(source_record_id)
    .execute(&mut *tx)
    .await
    .expect("publication revision");
    tx.commit().await.expect("revision transaction commit");
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
             VALUES ($1, $2, $3, $4, 'validated', now())",
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
        // The revision belongs to this unit, in the unit's own ledger. It used to be registered in
        // `catalog.administrative_boundary_revision` — for a fixture unit named `cas-…` that had
        // nothing to do with administrative boundaries — because that was the only ledger a release
        // could reference. The administrative row above remains as the lineage this derives from.
        seed_publication_revision(
            pool,
            self.data_revision,
            self.unit_id,
            &self.snapshot_id,
            self.source_record_id,
        )
        .await;
        // `loaded_row_count` is 1 because the `succeeded` CHECK refuses a load that materialised
        // nothing; these CAS tests exercise the pointer switch, and the publication rows a real load
        // writes are the subject of the projection-load ledger tests.
        sqlx::query(
            "INSERT INTO serving_postgis.spatial_projection_load
                (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
                 status, loaded_row_count, finished_at)
             VALUES ($1, $2, $3, $4, 'succeeded', 1, now())",
        )
        .bind(self.projection_load_id)
        .bind(self.unit_id)
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
}

/// The migration writes the derivative root as a SQL literal because a `plpgsql` function cannot
/// call into the Rust crate. This is what keeps the two statements from drifting: it reads the
/// function body actually installed in the database, not a file, so it also proves the migration ran.
#[tokio::test]
#[ignore = "requires a migrated Foundation PostgreSQL database"]
async fn the_promotion_gate_and_the_domain_agree_on_the_release_object_root() {
    let database = TestDatabase::create("runtime_manifest_contract_root").await;
    let pool = database.pool.clone();
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
    database.finish().await;
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
    let database = TestDatabase::create("runtime_manifest_foreign_prefix").await;
    let pool = database.pool.clone();
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
         VALUES ($1, $2, $3, $4, 'validated', now())",
    )
    .bind(static_revision)
    .bind(&static_snapshot)
    .bind(format!("iceberg:static-prefix-{static_revision}"))
    .bind(fixture.source_record_id)
    .execute(&pool)
    .await
    .expect("static release revision");
    seed_publication_revision(
        &pool,
        static_revision,
        fixture.unit_id,
        &static_snapshot,
        fixture.source_record_id,
    )
    .await;
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
    database.finish().await;
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
    let database = TestDatabase::create("runtime_manifest_static_replace").await;
    let pool = database.pool.clone();
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
    database.finish().await;
}
