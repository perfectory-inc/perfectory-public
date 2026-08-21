//! The industrial-complex boundary projection against a real `PostGIS`, under CI.
//!
//! `publish-industrial-complex-boundary-postgis` is the only thing that writes
//! `serving_postgis.industrial_complex_boundary_publication`, and three of its properties cannot be
//! asserted anywhere but against a database that has `PostGIS` in it:
//!
//!   * the reprojection is `PostGIS`'s (root ADR-0042), so whether EPSG:5186 metres actually became
//!     EPSG:4326 degrees is a question only `PostGIS` can answer;
//!   * the `complex_id` shape CHECK refuses a locally minted `now_v7()` id, which is a rule written
//!     in SQL and therefore has to be attacked in SQL;
//!   * an unpromoted load is invisible through `serving_postgis.industrial_complex_boundary_current`,
//!     and a view that is *always* empty would pass that assertion without meaning anything — so the
//!     same test promotes the load through `catalog.promote_vector_tile_runtime_manifest` and
//!     watches the rows appear.
//!
//! The command is a process, not a function: it reads its whole configuration from the environment.
//! These tests drive the built binary, which keeps the environment-variable contract under test too.
//!
//! Each test takes its own database. The command creates the `complex` publication unit, and
//! `catalog.promote_vector_tile_runtime_manifest` compares a manifest's unit count against
//! `count(*)` over every unit — a leaked one would fail a neighbouring suite while reading like a
//! promotion bug rather than a leak here.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use foundation_disposable_database::{disposable_database_url, DisposableDatabaseUrl, TestResult};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use uuid::Uuid;

const BINARY: &str = env!("CARGO_BIN_EXE_foundation-outbox-publisher");
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

/// The `unit_key` the command materialises. Declared in the command as well; asserted here so a
/// rename has to change the test that proves the promote gate can still find the load's unit.
const COMPLEX_UNIT_KEY: &str = "complex";
const CANONICAL_SNAPSHOT_ID: &str = "841361364657368624";
const SOURCE_SNAPSHOT_ID: &str = "vworldkr__sandan_boundary-publication-test";
const OBJECT_KEY: &str = "bronze/vworldkr__sandan_boundary/publication-test.zip";
/// What the collector recorded for [`OBJECT_KEY`], in the lowercase hex
/// `bronze_object_checksum_sha256_check` requires.
const OBJECT_SHA256: &str = "1111111111111111111111111111111111111111111111111111111111111111";
/// Well formed, and not [`OBJECT_SHA256`]. Naming this is naming a different object.
const OTHER_SHA256: &str = "2222222222222222222222222222222222222222222222222222222222222222";
/// A key `catalog.bronze_object` holds nothing at.
const UNCOLLECTED_OBJECT_KEY: &str = "bronze/vworldkr__sandan_boundary/never-collected.zip";
const TILES_URL_TEMPLATE: &str = "http://127.0.0.1:3112/complex/{z}/{x}/{y}";

/// Synthetic complex codes, six characters like the source's, in a range the source does not use.
const CODE_A: &str = "999ZZ0";
const CODE_B: &str = "999ZZ1";
/// `UUIDv5` values in the shape `industrial_complex_bronze_to_silver.py` derives.
const COMPLEX_A: &str = "7df3859c-0000-51fa-8000-000000000001";
const COMPLEX_B: &str = "7df3859c-0000-51fa-8000-000000000002";
/// A third, published by no fixture row, so the control insert below cannot collide with the load.
const COMPLEX_C: &str = "7df3859c-0000-51fa-8000-000000000004";
/// The shape `Uuid::now_v7()` mints for a locally created canonical row: version nibble 7.
const LOCALLY_MINTED_COMPLEX_ID: &str = "01a0136d-0000-7e61-8000-000000000003";

/// Easting/northing in EPSG:5186, in metres, well inside the projection's usable band.
///
/// Written as plain metres rather than as degrees on purpose: the source is metric, and a fixture
/// in degrees would pass a publish that never reprojected anything.
const EASTING: f64 = 200_000.0;
const NORTHING: f64 = 400_000.0;
const SIDE_METRES: f64 = 100.0;

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
#[allow(clippy::too_many_lines)]
async fn a_publish_reprojects_into_one_load_and_stays_invisible_until_promotion() -> TestResult {
    let fixture = Fixture::create("complex_publish_once").await?;
    let pool = fixture.pool().await?;

    let output = fixture.publish()?;
    assert!(
        output.status.success(),
        "publish failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout)
        .contains("industrial-complex-boundary-postgis-publish-ok"));

    let loads = fixture.loads(&pool).await?;
    assert_eq!(loads.len(), 1, "one run must open exactly one load");
    let load = &loads[0];
    assert_eq!(load.status, "succeeded");
    assert_eq!(load.loaded_row_count, Some(Fixture::BOUNDARY_ROWS));
    assert_eq!(load.rejected_row_count, Some(0));

    // The revision belongs to the unit it revises and carries no administrative lineage: an
    // industrial-complex boundary asserts nothing about an administrative boundary. Its provenance
    // is the collected object, and the `catalog.source_record` this fixture also seeded — the one
    // the release will name — is deliberately not it (root ADR-0046).
    let revision = sqlx::query(
        "SELECT unit.unit_key, revision.derived_from_administrative_revision,
                revision.bronze_object_id, revision.source_record_id
           FROM catalog.publication_revision AS revision
           JOIN catalog.vector_tile_publication_unit AS unit
             ON unit.id = revision.publication_unit_id
          WHERE revision.id = $1",
    )
    .bind(load.data_revision)
    .fetch_one(&pool)
    .await?;
    assert_eq!(revision.try_get::<String, _>("unit_key")?, COMPLEX_UNIT_KEY);
    assert_eq!(
        revision.try_get::<Option<Uuid>, _>("derived_from_administrative_revision")?,
        None
    );
    assert_eq!(
        revision.try_get::<Option<Uuid>, _>("bronze_object_id")?,
        Some(fixture.bronze_object_id)
    );
    assert_eq!(
        revision.try_get::<Option<Uuid>, _>("source_record_id")?,
        None
    );

    // Every geometry row reaches the same object.
    let anchored: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM serving_postgis.industrial_complex_boundary_publication
          WHERE projection_load_id = $1 AND bronze_object_id = $2",
    )
    .bind(load.id)
    .bind(fixture.bronze_object_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(anchored, Fixture::BOUNDARY_ROWS);

    // The reprojection. Metres in, degrees out: an identity load would leave the centroid at the
    // easting, which is five orders of magnitude outside the bound below. The bounds are integers so
    // no Korea-shaped decimal coordinate has to appear in this file.
    let geometry = sqlx::query(
        "SELECT count(*) AS rows,
                bool_and(public.st_srid(geom) = 4326) AS srid_ok,
                bool_and(public.st_isvalid(geom)) AS valid_ok,
                bool_and(public.geometrytype(geom) = 'MULTIPOLYGON') AS type_ok,
                bool_and(public.st_x(public.st_centroid(geom)) BETWEEN 125 AND 130) AS longitude_ok,
                bool_and(public.st_y(public.st_centroid(geom)) BETWEEN 33 AND 39) AS latitude_ok
           FROM serving_postgis.industrial_complex_boundary_publication
          WHERE projection_load_id = $1",
    )
    .bind(load.id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(geometry.try_get::<i64, _>("rows")?, Fixture::BOUNDARY_ROWS);
    assert!(geometry.try_get::<bool, _>("srid_ok")?);
    assert!(geometry.try_get::<bool, _>("valid_ok")?);
    assert!(geometry.try_get::<bool, _>("type_ok")?);
    assert!(
        geometry.try_get::<bool, _>("longitude_ok")?,
        "the source easting was not reprojected out of metres"
    );
    assert!(geometry.try_get::<bool, _>("latitude_ok")?);

    // Silver's area travelled without being recomputed or rounded away.
    let area: String = sqlx::query_scalar(
        "SELECT area_sqm_calculated::text
           FROM serving_postgis.industrial_complex_boundary_publication
          WHERE projection_load_id = $1 AND official_complex_code = $2",
    )
    .bind(load.id)
    .bind(CODE_A)
    .fetch_one(&pool)
    .await?;
    assert_eq!(area, Fixture::AREA_SQM);

    // Written, and invisible. Nothing has promoted this load, so the view Martin reads is empty
    // while the table behind it holds every row.
    assert_eq!(fixture.current_rows(&pool).await?, 0);

    // The same view, once the load is promoted through the real gate. Without this half, the
    // assertion above would also hold for a view that can never return anything.
    fixture.promote(&pool, load).await?;
    assert_eq!(fixture.current_rows(&pool).await?, Fixture::BOUNDARY_ROWS);
    let codes: Vec<String> = sqlx::query_scalar(
        "SELECT official_complex_code FROM serving_postgis.industrial_complex_boundary_current
          ORDER BY official_complex_code",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(codes, vec![CODE_A.to_owned(), CODE_B.to_owned()]);

    fixture.finish(pool).await
}

/// The `complex_id` shape CHECK, attacked in the language it is written in.
///
/// `read_rows` refuses a `now_v7()` id before the command opens a connection, so the SQL rule would
/// never be reached through the command and an inert CHECK would look exactly like a working one.
/// This inserts one directly, under the publisher capability, and requires the database to refuse.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn a_locally_minted_complex_id_is_refused_by_the_database() -> TestResult {
    let fixture = Fixture::create("complex_publish_v7").await?;
    let pool = fixture.pool().await?;
    let output = fixture.publish()?;
    assert!(output.status.success());
    let load = fixture.loads(&pool).await?.remove(0);

    // The control: the same statement with the lakehouse's UUIDv5 lands. Without it a typo anywhere
    // else in the insert would produce the rejection this test is looking for.
    let accepted = fixture
        .insert_boundary_directly(&pool, load.id, COMPLEX_C, "999ZZ2")
        .await;
    assert!(
        accepted.is_ok(),
        "a UUIDv5 complex_id must be accepted: {accepted:?}"
    );

    let refused = fixture
        .insert_boundary_directly(&pool, load.id, LOCALLY_MINTED_COMPLEX_ID, "999ZZ3")
        .await
        .expect_err("a UUIDv7 complex_id must be refused by the CHECK");
    assert!(
        refused.to_string().contains("complex_id_is_uuid_v5"),
        "unexpected error: {refused}"
    );

    fixture.finish(pool).await
}

/// The provenance check, attacked three ways.
///
/// The publish that succeeds elsewhere in this file is the control: the same command, the same
/// export, the same database, and only the two values naming the object changed. Without these the
/// check would be indistinguishable from no check at all — the anchor is *resolved* from a key the
/// export already carries, so a resolution that always found something would still look green.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn a_publish_naming_another_object_is_refused() -> TestResult {
    let fixture = Fixture::create("complex_publish_wrong_object").await?;
    let pool = fixture.pool().await?;

    // 1. The right key, a checksum the collector did not record. Same address, different bytes.
    let wrong_checksum = fixture.publish_naming(OBJECT_KEY, OTHER_SHA256)?;
    assert!(!wrong_checksum.status.success());
    let message = String::from_utf8_lossy(&wrong_checksum.stderr).into_owned();
    assert!(
        message.contains("these are not the same object"),
        "unexpected error: {message}"
    );

    // 2a. A key nothing was collected at, while the export still cites the real one. The file check
    //     answers first, and says which of the two disagrees.
    let mismatched = fixture.publish_naming(UNCOLLECTED_OBJECT_KEY, OBJECT_SHA256)?;
    assert!(!mismatched.status.success());
    let message = String::from_utf8_lossy(&mismatched.stderr).into_owned();
    assert!(
        message.contains("source_record_id mismatch"),
        "unexpected error: {message}"
    );

    // 2b. Now an export that agrees with the argument, and both name an object that was never
    //     collected. This is the case the old command could not refuse: a hand-made row would have
    //     satisfied it, and here there is nothing to hand-make.
    let uncollected_export = fixture.root.join("never-collected.jsonl");
    Fixture::write_source_at(&uncollected_export, UNCOLLECTED_OBJECT_KEY)?;
    let uncollected =
        fixture.publish_from(&uncollected_export, UNCOLLECTED_OBJECT_KEY, OBJECT_SHA256)?;
    assert!(!uncollected.status.success());
    let message = String::from_utf8_lossy(&uncollected.stderr).into_owned();
    assert!(
        message.contains("holds 0 objects"),
        "unexpected error: {message}"
    );

    // 3. Two source catalogs recording the same key. `bronze_object` is unique on
    //    `(source_catalog_id, object_key)`, so this is representable, and resolving by key alone
    //    would have to pick one. It refuses instead.
    let second_catalog = fixture
        .seed_source_catalog(&pool, "vworldkr-sandan-boundary-mirror")
        .await?;
    fixture
        .seed_bronze_object(
            &pool,
            second_catalog,
            Uuid::new_v4(),
            OBJECT_KEY,
            OTHER_SHA256,
        )
        .await?;
    let ambiguous = fixture.publish_naming(OBJECT_KEY, OBJECT_SHA256)?;
    assert!(!ambiguous.status.success());
    let message = String::from_utf8_lossy(&ambiguous.stderr).into_owned();
    assert!(
        message.contains("holds 2 objects"),
        "unexpected error: {message}"
    );

    // Nothing survived any of the three: a refused publish leaves no load, and therefore no rows.
    assert!(fixture.loads(&pool).await?.is_empty());
    let published: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM serving_postgis.industrial_complex_boundary_publication",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(published, 0);

    fixture.finish(pool).await
}

/// A re-publish is a second load, not an edit of the first.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn republishing_opens_a_second_load_and_leaves_the_first_untouched() -> TestResult {
    let fixture = Fixture::create("complex_publish_twice").await?;
    let pool = fixture.pool().await?;
    for attempt in 0..2 {
        let output = fixture.publish()?;
        assert!(
            output.status.success(),
            "publish attempt {attempt} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let loads = fixture.loads(&pool).await?;
    assert_eq!(loads.len(), 2, "a re-publish is a second load, not an edit");
    assert_ne!(loads[0].id, loads[1].id);
    // One snapshot is one revision: the second load reuses it rather than minting a second name for
    // one version of the data.
    assert_eq!(loads[0].data_revision, loads[1].data_revision);
    for load in &loads {
        assert_eq!(load.status, "succeeded");
        assert_eq!(load.loaded_row_count, Some(Fixture::BOUNDARY_ROWS));
        assert_eq!(
            fixture.publication_rows(&pool, load.id).await?,
            Fixture::BOUNDARY_ROWS
        );
    }

    let total: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM serving_postgis.industrial_complex_boundary_publication",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(total, Fixture::BOUNDARY_ROWS * 2, "both loads are retained");

    fixture.finish(pool).await
}

/// The projection is append-only to everyone who is not holding the publisher capability.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn a_caller_without_the_publisher_capability_cannot_edit_a_published_row() -> TestResult {
    let fixture = Fixture::create("complex_publish_append_only").await?;
    let pool = fixture.pool().await?;
    assert!(fixture.publish()?.status.success());

    for statement in [
        "UPDATE serving_postgis.industrial_complex_boundary_publication SET official_complex_code = '999ZZ9'",
        "DELETE FROM serving_postgis.industrial_complex_boundary_publication",
    ] {
        let error = sqlx::query(statement)
            .execute(&pool)
            .await
            .expect_err("the append-only trigger must refuse this");
        assert!(
            error.to_string().contains("append-only"),
            "unexpected error for `{statement}`: {error}"
        );
    }

    fixture.finish(pool).await
}

struct ProjectionLoad {
    id: Uuid,
    data_revision: Uuid,
    status: String,
    loaded_row_count: Option<i64>,
    rejected_row_count: Option<i64>,
}

struct Fixture {
    database: DisposableDatabaseUrl,
    root: PathBuf,
    /// The collected object the publish anchors to (root ADR-0046).
    bronze_object_id: Uuid,
    /// The release's own lineage record, which `promote` still writes into
    /// `catalog.vector_tile_release`. Deliberately not what the publish names.
    source_record_id: Uuid,
}

impl Fixture {
    const BOUNDARY_ROWS: i64 = 2;
    /// `decimal(18,2)`, as `numeric(18,2)` renders it back.
    const AREA_SQM: &'static str = "10000.00";

    async fn create(label: &str) -> TestResult<Self> {
        let database = disposable_database_url(label).await?;
        let root = env::temp_dir().join(format!("{label}-{}", Uuid::new_v4()));
        fs::create_dir_all(&root)?;
        let fixture = Self {
            database,
            root,
            bronze_object_id: Uuid::new_v4(),
            source_record_id: Uuid::new_v4(),
        };
        fixture.write_source()?;

        let pool = fixture.pool().await?;
        MIGRATOR.run(&pool).await?;
        // The collection side, in full. A `catalog.bronze_object` cannot exist without the source
        // catalog and ingestion run that produced it, and seeding it through those two is what makes
        // this fixture the shape a real collection leaves behind rather than a row shaped like one.
        let source_catalog_id = fixture
            .seed_source_catalog(&pool, "vworldkr-sandan-boundary")
            .await?;
        fixture
            .seed_bronze_object(
                &pool,
                source_catalog_id,
                fixture.bronze_object_id,
                OBJECT_KEY,
                OBJECT_SHA256,
            )
            .await?;
        sqlx::query(
            "INSERT INTO catalog.source_record
                (id, source, external_id, checksum_sha256, raw_object_key)
             VALUES ($1, 'industrial-complex-boundary-publish-test', $2, repeat('e', 64), $3)",
        )
        .bind(fixture.source_record_id)
        .bind(format!("publish-test-{}", fixture.source_record_id))
        .bind(OBJECT_KEY)
        .execute(&pool)
        .await?;
        pool.close().await;
        Ok(fixture)
    }

    /// Inserts one source catalog and its ingestion run, returning the catalog id.
    async fn seed_source_catalog(&self, pool: &PgPool, slug: &str) -> TestResult<Uuid> {
        let source_catalog_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO catalog.source_catalog
                (id, slug, name, provider, dataset_name, auth_kind, payload_format)
             VALUES ($1, $2, $2, 'vworld.kr', 'sandan-boundary', 'none', 'zip')",
        )
        .bind(source_catalog_id)
        .bind(slug)
        .execute(pool)
        .await?;
        Ok(source_catalog_id)
    }

    /// Appends one collected object, the way a Bronze commit leaves it.
    async fn seed_bronze_object(
        &self,
        pool: &PgPool,
        source_catalog_id: Uuid,
        bronze_object_id: Uuid,
        object_key: &str,
        checksum_sha256: &str,
    ) -> TestResult<()> {
        let ingestion_run_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO catalog.ingestion_run (id, source_catalog_id, trigger, status)
             VALUES ($1, $2, 'test', 'succeeded')",
        )
        .bind(ingestion_run_id)
        .bind(source_catalog_id)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.bronze_object
                (id, source_catalog_id, ingestion_run_id, dedupe_key, object_key, checksum_sha256,
                 content_type, size_bytes, source_identity_key, snapshot_date, snapshot_granularity,
                 snapshot_basis)
             VALUES ($1, $2, $3, $4, $4, $5, 'application/zip', 1, $4, DATE '2026-07-21', 'day',
                     'collected_at_fallback')",
        )
        .bind(bronze_object_id)
        .bind(source_catalog_id)
        .bind(ingestion_run_id)
        .bind(object_key)
        .bind(checksum_sha256)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn pool(&self) -> TestResult<PgPool> {
        self.database.pool().await
    }

    async fn finish(self, pool: PgPool) -> TestResult {
        pool.close().await;
        let _ = fs::remove_dir_all(&self.root);
        self.database.drop_database().await
    }

    fn source_path(&self) -> PathBuf {
        self.root.join("industrial-complex-boundaries.jsonl")
    }

    /// Writes the export in the shape
    /// `industrial_complex_boundaries_silver_to_postgis_handoff.py` emits.
    fn write_source(&self) -> TestResult {
        Self::write_source_at(&self.source_path(), OBJECT_KEY)
    }

    /// The same export, citing whichever Bronze object key the caller names.
    fn write_source_at(path: &Path, object_key: &str) -> TestResult {
        let mut body = String::new();
        for (complex_id, code, offset) in [(COMPLEX_A, CODE_A, 0.0), (COMPLEX_B, CODE_B, 1_000.0)] {
            let wkb = square_wkb(EASTING + offset, NORTHING + offset);
            let row = json!({
                "complex_id": complex_id,
                "official_complex_code": code,
                "boundary_kind": "official",
                "geometry_wkb_hex": hex_lower(&wkb),
                "geometry_srid": 5186,
                "area_sqm_calculated": Self::AREA_SQM,
                "geometry_checksum_sha256": format!("{:x}", Sha256::digest(&wkb)),
                "source_record_id": object_key,
                "source_snapshot_id": SOURCE_SNAPSHOT_ID,
            });
            body.push_str(&serde_json::to_string(&row)?);
            body.push('\n');
        }
        fs::write(path, body)?;
        Ok(())
    }

    fn publish(&self) -> TestResult<std::process::Output> {
        self.publish_naming(OBJECT_KEY, OBJECT_SHA256)
    }

    /// The same command, with the two values that say *which object* under the caller's control.
    fn publish_naming(
        &self,
        object_key: &str,
        checksum_sha256: &str,
    ) -> TestResult<std::process::Output> {
        self.publish_from(&self.source_path(), object_key, checksum_sha256)
    }

    /// The same command again, reading whichever export the caller wrote.
    fn publish_from(
        &self,
        source_path: &Path,
        object_key: &str,
        checksum_sha256: &str,
    ) -> TestResult<std::process::Output> {
        Command::new(BINARY)
            .arg("publish-industrial-complex-boundary-postgis")
            .env("DATABASE_URL", self.database.url())
            .env(
                "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_CONFIRM",
                "1",
            )
            .env(
                "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_SOURCE_PATH",
                source_path,
            )
            .env(
                "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_CANONICAL_ICEBERG_SNAPSHOT_ID",
                CANONICAL_SNAPSHOT_ID,
            )
            .env(
                "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_SOURCE_SNAPSHOT_ID",
                SOURCE_SNAPSHOT_ID,
            )
            .env(
                "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_SOURCE_OBJECT_KEY",
                object_key,
            )
            .env(
                "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_SOURCE_OBJECT_CHECKSUM_SHA256",
                checksum_sha256,
            )
            .output()
            .map_err(Into::into)
    }

    /// Builds the release and complete manifest for one load and moves the pointer through the gate.
    ///
    /// The gate is called rather than the pointer being written directly. It is the thing that
    /// decides what the public map serves, and a test that stepped around it would prove the view's
    /// joins without proving that this unit can pass the promotion rules at all.
    async fn promote(&self, pool: &PgPool, load: &ProjectionLoad) -> TestResult {
        let release_id = Uuid::new_v4();
        let manifest_id = Uuid::new_v4();
        let publication_unit_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM catalog.vector_tile_publication_unit WHERE unit_key = $1",
        )
        .bind(COMPLEX_UNIT_KEY)
        .fetch_one(pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.vector_tile_release
                (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
                 source_record_id, source_kind, martin_source_id, tiles_url_template,
                 postgis_projection_revision)
             VALUES ($1, $2, $3, $4, $5, 'dynamic_postgis', $6, $7, $8)",
        )
        .bind(release_id)
        .bind(publication_unit_id)
        .bind(load.data_revision)
        .bind(CANONICAL_SNAPSHOT_ID)
        .bind(self.source_record_id)
        .bind(COMPLEX_UNIT_KEY)
        .bind(TILES_URL_TEMPLATE)
        .bind(load.id)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.vector_tile_runtime_manifest (id, manifest_generation) VALUES ($1, 1)",
        )
        .bind(manifest_id)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.vector_tile_runtime_manifest_unit
                (manifest_id, publication_unit_id, release_id, serving_generation, data_revision,
                 canonical_iceberg_snapshot_id)
             VALUES ($1, $2, $3, 1, $4, $5)",
        )
        .bind(manifest_id)
        .bind(publication_unit_id)
        .bind(release_id)
        .bind(load.data_revision)
        .bind(CANONICAL_SNAPSHOT_ID)
        .execute(pool)
        .await?;
        sqlx::query("SELECT catalog.promote_vector_tile_runtime_manifest(NULL, $1)")
            .bind(manifest_id)
            .execute(pool)
            .await?;
        Ok(())
    }

    /// Appends one row the way the command does, holding the publisher capability.
    async fn insert_boundary_directly(
        &self,
        pool: &PgPool,
        projection_load_id: Uuid,
        complex_id: &str,
        official_complex_code: &str,
    ) -> Result<(), sqlx::Error> {
        let mut transaction = pool.begin().await?;
        sqlx::query("SELECT set_config('foundation.temporal_publisher', 'on', true)")
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "INSERT INTO serving_postgis.industrial_complex_boundary_publication
                (complex_id, official_complex_code, projection_load_id, boundary_kind,
                 source_snapshot_id, bronze_object_id, source_object_key, area_sqm_calculated,
                 source_geometry_checksum_sha256, geom)
             SELECT $1::uuid, $2, $3, 'official', $4, $5, $6, 1.00, repeat('a', 64),
                    public.st_multi(public.st_transform(
                        public.st_setsrid(public.st_geomfromwkb(decode($7, 'hex')), 5186), 4326))",
        )
        .bind(complex_id)
        .bind(official_complex_code)
        .bind(projection_load_id)
        .bind(SOURCE_SNAPSHOT_ID)
        .bind(self.bronze_object_id)
        .bind(OBJECT_KEY)
        .bind(hex_lower(&square_wkb(EASTING, NORTHING)))
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await
    }

    /// Every `complex` load, oldest first.
    async fn loads(&self, pool: &PgPool) -> TestResult<Vec<ProjectionLoad>> {
        let rows = sqlx::query(
            "SELECT load.id, load.data_revision, load.status, load.loaded_row_count,
                    load.rejected_row_count
               FROM serving_postgis.spatial_projection_load AS load
               JOIN catalog.vector_tile_publication_unit AS unit
                 ON unit.id = load.publication_unit_id
              WHERE unit.unit_key = $1
              ORDER BY load.started_at, load.id",
        )
        .bind(COMPLEX_UNIT_KEY)
        .fetch_all(pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ProjectionLoad {
                    id: row.try_get("id")?,
                    data_revision: row.try_get("data_revision")?,
                    status: row.try_get("status")?,
                    loaded_row_count: row.try_get("loaded_row_count")?,
                    rejected_row_count: row.try_get("rejected_row_count")?,
                })
            })
            .collect()
    }

    async fn publication_rows(&self, pool: &PgPool, load_id: Uuid) -> TestResult<i64> {
        Ok(sqlx::query_scalar(
            "SELECT count(*) FROM serving_postgis.industrial_complex_boundary_publication
              WHERE projection_load_id = $1",
        )
        .bind(load_id)
        .fetch_one(pool)
        .await?)
    }

    async fn current_rows(&self, pool: &PgPool) -> TestResult<i64> {
        Ok(sqlx::query_scalar(
            "SELECT count(*) FROM serving_postgis.industrial_complex_boundary_current",
        )
        .fetch_one(pool)
        .await?)
    }
}

/// A closed, counter-clockwise square as standard little-endian WKB, in the source's own metres.
///
/// Built here rather than fetched from `PostGIS`: the point of the fixture is to be the bytes Silver
/// stores, and asking the database to produce them would make the test agree with itself.
fn square_wkb(easting: f64, northing: f64) -> Vec<u8> {
    let corners = [
        (easting, northing),
        (easting + SIDE_METRES, northing),
        (easting + SIDE_METRES, northing + SIDE_METRES),
        (easting, northing + SIDE_METRES),
        (easting, northing),
    ];
    let mut wkb = Vec::with_capacity(9 + 8 + corners.len() * 16);
    wkb.push(1); // little endian
    wkb.extend_from_slice(&3_u32.to_le_bytes()); // Polygon
    wkb.extend_from_slice(&1_u32.to_le_bytes()); // one ring
    wkb.extend_from_slice(
        &u32::try_from(corners.len())
            .expect("ring length")
            .to_le_bytes(),
    );
    for (x, y) in corners {
        wkb.extend_from_slice(&x.to_le_bytes());
        wkb.extend_from_slice(&y.to_le_bytes());
    }
    wkb
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    bytes.iter().fold(String::new(), |mut hex, byte| {
        let _ = write!(&mut hex, "{byte:02x}");
        hex
    })
}
