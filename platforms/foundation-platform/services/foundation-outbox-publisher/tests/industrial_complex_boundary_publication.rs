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
//!     watches the rows appear;
//!   * the geometry repair (root ADR-0047) is `PostGIS`'s `ST_MakeValid` and its gates are the
//!     table's own constraints, so both what it produces and what refuses it are questions only
//!     `PostGIS` can answer. Two of those tests take the gate away and publish again, because a
//!     refusal proves nothing until the same run without the gate is shown to succeed.
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
/// Where [`bowtie_wkb`] pinches in, in the same metres: short of the far side, so the two lobes are
/// different sizes and the crossed ring encloses a positive area to compare the repair's against.
const BOWTIE_WAIST_METRES: f64 = 40.0;
const BOWTIE_HEIGHT_METRES: f64 = 60.0;
/// How far [`spiked_wkb`]'s zero-width spur reaches past the corner it leaves from.
const SPUR_METRES: f64 = 100.0;

/// The area [`self_touching_wkb`] encloses, in the source's own square metres: two triangles of
/// `SIDE_METRES²/4` meeting at the ring's own midpoint. Stated here rather than derived from the
/// repair, so the assertion that the repair preserved it has something independent to compare to.
const SELF_TOUCHING_AREA_SQM: f64 = SIDE_METRES * SIDE_METRES / 2.0;
/// What repairing [`bowtie_wkb`] does to its area: `ST_Area` of the crossed ring is the difference
/// of the two lobes and `ST_Area` of the repair is their sum. Measured on `PostGIS` 3.5 / GEOS 3.14.
const BOWTIE_AREA_CHANGE_RATIO: f64 = 1.125;

/// The two bounds `complex_boundary_publication_repair_tolerance_check` states, restated here so
/// that moving either one has to move this file as well. `20260821190721` is where they are decided
/// and where the reasoning for the numbers lives; nothing in the publisher holds a copy.
const MAX_REPAIR_HAUSDORFF_DISTANCE_M: f64 = 0.000_001;
const MAX_REPAIR_AREA_CHANGE_RATIO: f64 = 0.000_000_001;

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
        .insert_boundary_directly(
            &pool,
            load.id,
            COMPLEX_C,
            "999ZZ2",
            RepairEvidence::untouched(),
        )
        .await;
    assert!(
        accepted.is_ok(),
        "a UUIDv5 complex_id must be accepted: {accepted:?}"
    );

    let refused = fixture
        .insert_boundary_directly(
            &pool,
            load.id,
            LOCALLY_MINTED_COMPLEX_ID,
            "999ZZ3",
            RepairEvidence::untouched(),
        )
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

/// A boundary that is invalid in the source coordinates is repaired, published, and said so.
///
/// The valid square beside it is the control: `geometry_repaired` has to be able to be false in the
/// same load, or the column would be reporting the run rather than the row.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn a_self_touching_boundary_is_repaired_rather_than_dropped() -> TestResult {
    let fixture = Fixture::create("complex_publish_repair").await?;
    let pool = fixture.pool().await?;
    let export = fixture.root.join("self-touching.jsonl");
    Fixture::write_source_rows(
        &export,
        OBJECT_KEY,
        &[
            (COMPLEX_A, CODE_A, square_wkb(EASTING, NORTHING)),
            (
                COMPLEX_B,
                CODE_B,
                self_touching_wkb(EASTING + 1_000.0, NORTHING + 1_000.0),
            ),
        ],
    )?;

    let output = fixture.publish_from(&export, OBJECT_KEY, OBJECT_SHA256)?;
    assert!(
        output.status.success(),
        "the repairable boundary must publish: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Repairing quietly is what the summary exists to prevent, so the run has to name the complex.
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    assert!(
        stdout.contains(&format!(
            "industrial-complex-boundary-postgis-publish-repaired official_complex_code={CODE_B}"
        )),
        "the run did not report the repair: {stdout}"
    );
    assert!(
        !stdout.contains(&format!("publish-repaired official_complex_code={CODE_A}")),
        "the untouched boundary must not be reported as repaired: {stdout}"
    );
    assert!(
        stdout.contains("repaired_rows=1"),
        "the summary must count the repair: {stdout}"
    );

    let load = fixture.loads(&pool).await?.remove(0);
    assert_eq!(load.loaded_row_count, Some(Fixture::BOUNDARY_ROWS));
    assert_eq!(load.rejected_row_count, Some(0));

    // The square: published, and not marked as something it is not.
    let untouched = fixture.published_row(&pool, load.id, CODE_A).await?;
    assert!(!untouched.geometry_repaired);
    assert_eq!(untouched.repair_hausdorff_distance_m, None);
    assert_eq!(untouched.repair_area_change_ratio, None);

    // The repaired one: two parts, valid, and — the point of the whole exercise — the same outline.
    // The area is read back through the source CRS so it can be compared with the metres the ring
    // was written in; `AREA_SQM` is Silver's own number and says nothing about this ring.
    let repaired = fixture.published_row(&pool, load.id, CODE_B).await?;
    assert!(repaired.geometry_repaired);
    assert_eq!(repaired.repair_hausdorff_distance_m, Some(0.0));
    assert_eq!(repaired.repair_area_change_ratio, Some(0.0));
    assert_eq!(repaired.geometry_type, "MULTIPOLYGON");
    assert!(repaired.is_valid);
    assert_eq!(repaired.parts, 2);
    assert!(
        (repaired.source_crs_area_sqm - SELF_TOUCHING_AREA_SQM).abs() < 0.01,
        "the repair enclosed {} m² where the source ring enclosed {SELF_TOUCHING_AREA_SQM} m²",
        repaired.source_crs_area_sqm
    );

    fixture.finish(pool).await
}

/// A repair that changes the area is refused, and removing the tolerance is what stops refusing it.
///
/// Both halves are needed. The first alone would also pass if the geometry were being rejected for
/// some unrelated reason; the second is the same command, the same file and the same database with
/// one constraint dropped, so what changed is the only thing that can explain the difference.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn a_repair_that_changes_the_area_is_refused_by_the_tolerance() -> TestResult {
    let fixture = Fixture::create("complex_publish_bowtie").await?;
    let pool = fixture.pool().await?;
    let export = fixture.root.join("bowtie.jsonl");
    Fixture::write_source_rows(
        &export,
        OBJECT_KEY,
        &[(COMPLEX_A, CODE_A, bowtie_wkb(EASTING, NORTHING))],
    )?;

    let refused = fixture.publish_from(&export, OBJECT_KEY, OBJECT_SHA256)?;
    assert!(
        !refused.status.success(),
        "a repair that changes the area must not publish"
    );
    let message = String::from_utf8_lossy(&refused.stderr).into_owned();
    assert!(
        message.contains("complex_boundary_publication_repair_tolerance_check"),
        "unexpected error: {message}"
    );
    assert!(
        fixture.loads(&pool).await?.is_empty(),
        "a refused publish leaves no load"
    );

    sqlx::query(
        "ALTER TABLE serving_postgis.industrial_complex_boundary_publication
           DROP CONSTRAINT complex_boundary_publication_repair_tolerance_check",
    )
    .execute(&pool)
    .await?;

    let accepted = fixture.publish_from(&export, OBJECT_KEY, OBJECT_SHA256)?;
    assert!(
        accepted.status.success(),
        "without the tolerance the same publish must succeed, or the tolerance is not what refused \
         it: {}",
        String::from_utf8_lossy(&accepted.stderr)
    );
    let load = fixture.loads(&pool).await?.remove(0);
    let admitted = fixture.published_row(&pool, load.id, CODE_A).await?;
    assert!(admitted.geometry_repaired);
    assert_eq!(
        admitted.repair_area_change_ratio,
        Some(BOWTIE_AREA_CHANGE_RATIO),
        "the repair the tolerance refused enlarges the boundary by more than its own area"
    );
    // And it was refused for its area alone: nothing moved, and the result is a valid MultiPolygon.
    assert_eq!(admitted.repair_hausdorff_distance_m, Some(0.0));
    assert_eq!(admitted.geometry_type, "MULTIPOLYGON");
    assert!(admitted.is_valid);

    fixture.finish(pool).await
}

/// A repair that stops being polygonal is refused, and the column's own type is what refuses it.
///
/// This is the failure neither tolerance can see: the spur `ST_MakeValid` hands back beside the
/// square moves no vertex and changes no area, and `ST_IsValid` is perfectly happy with a collection
/// of a polygon and a line. Relaxing the column type and publishing again shows the collection
/// landing with both measurements reading zero.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn a_repair_that_is_not_polygonal_is_refused_by_the_column_type() -> TestResult {
    let fixture = Fixture::create("complex_publish_collection").await?;
    let pool = fixture.pool().await?;
    let export = fixture.root.join("spiked.jsonl");
    Fixture::write_source_rows(
        &export,
        OBJECT_KEY,
        &[(COMPLEX_A, CODE_A, spiked_wkb(EASTING, NORTHING))],
    )?;

    let refused = fixture.publish_from(&export, OBJECT_KEY, OBJECT_SHA256)?;
    assert!(
        !refused.status.success(),
        "a repair that is not polygonal must not publish"
    );
    let message = String::from_utf8_lossy(&refused.stderr).into_owned();
    assert!(
        message.contains("GeometryCollection") && message.contains("MultiPolygon"),
        "unexpected error: {message}"
    );
    assert!(fixture.loads(&pool).await?.is_empty());

    // The view has to go first — a column type cannot be relaxed underneath one — and it is not
    // this test's subject. Dropped rather than recreated afterwards: re-stating the view here would
    // put a second copy of `20260819030646`'s joins in a test file, free to drift from the real one.
    // The database is thrown away at the end of this function either way.
    sqlx::query("DROP VIEW serving_postgis.industrial_complex_boundary_current")
        .execute(&pool)
        .await?;
    sqlx::query(
        "ALTER TABLE serving_postgis.industrial_complex_boundary_publication
           ALTER COLUMN geom TYPE public.geometry(Geometry, 4326)",
    )
    .execute(&pool)
    .await?;

    let accepted = fixture.publish_from(&export, OBJECT_KEY, OBJECT_SHA256)?;
    assert!(
        accepted.status.success(),
        "without the declared type the same publish must succeed, or the type is not what refused \
         it: {}",
        String::from_utf8_lossy(&accepted.stderr)
    );
    let load = fixture.loads(&pool).await?.remove(0);
    let admitted = fixture.published_row(&pool, load.id, CODE_A).await?;
    assert_eq!(admitted.geometry_type, "GEOMETRYCOLLECTION");
    assert!(
        admitted.is_valid,
        "ST_IsValid does not refuse a collection either, which is why the type gate is the one \
         that has to"
    );
    assert_eq!(admitted.repair_hausdorff_distance_m, Some(0.0));
    assert_eq!(admitted.repair_area_change_ratio, Some(0.0));

    fixture.finish(pool).await
}

/// Where the two tolerances sit, and that the flag and its evidence have to agree — attacked in SQL.
///
/// The publish reaches the tolerances only through whatever `ST_MakeValid` happens to produce, so
/// nothing driving the command can say where the bound is or show that a value just outside it is
/// refused. These rows say it directly, under the publisher capability, with the same valid square
/// as their geometry so that the three repair columns are the only thing under test.
///
/// The accepted row sits **on** both bounds. Without it the refusals below would also hold for a
/// constraint that refuses everything.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn the_repair_tolerances_are_where_the_migration_says_they_are() -> TestResult {
    let fixture = Fixture::create("complex_publish_tolerance").await?;
    let pool = fixture.pool().await?;
    assert!(fixture.publish()?.status.success());
    let load = fixture.loads(&pool).await?.remove(0);

    let accepted = fixture
        .insert_boundary_directly(
            &pool,
            load.id,
            &synthetic_complex_id(10),
            "999Y10",
            RepairEvidence::measured(
                MAX_REPAIR_HAUSDORFF_DISTANCE_M,
                MAX_REPAIR_AREA_CHANGE_RATIO,
            ),
        )
        .await;
    assert!(
        accepted.is_ok(),
        "a repair exactly on both bounds must be accepted: {accepted:?}"
    );

    for (index, code, evidence, constraint, what) in [
        (
            11,
            "999Y11",
            RepairEvidence::measured(MAX_REPAIR_HAUSDORFF_DISTANCE_M * 10.0, 0.0),
            "repair_tolerance_check",
            "a repair that moved a vertex ten times further than the bound",
        ),
        (
            12,
            "999Y12",
            RepairEvidence::measured(0.0, MAX_REPAIR_AREA_CHANGE_RATIO * 10.0),
            "repair_tolerance_check",
            "a repair that changed the area ten times more than the bound",
        ),
        (
            13,
            "999Y13",
            RepairEvidence::measured(f64::NAN, 0.0),
            "repair_tolerance_check",
            "a distance that is not a number",
        ),
        (
            14,
            "999Y14",
            RepairEvidence::measured(-1.0, 0.0),
            "repair_tolerance_check",
            "a distance below zero",
        ),
        (
            15,
            "999Y15",
            RepairEvidence {
                repaired: true,
                hausdorff_distance_m: None,
                area_change_ratio: None,
            },
            "repair_evidence_check",
            "a repair claimed with nothing measured",
        ),
        (
            16,
            "999Y16",
            RepairEvidence {
                repaired: false,
                hausdorff_distance_m: Some(0.0),
                area_change_ratio: Some(0.0),
            },
            "repair_evidence_check",
            "measurements under a row claiming no repair",
        ),
    ] {
        let refused = fixture
            .insert_boundary_directly(&pool, load.id, &synthetic_complex_id(index), code, evidence)
            .await
            .expect_err(what);
        assert!(
            refused.to_string().contains(constraint),
            "{what} was refused by something other than {constraint}: {refused}"
        );
    }

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

/// What the three repair columns of a directly-inserted row say.
///
/// One value rather than three arguments, because the constraint under test is about the three
/// agreeing: a caller has to state a combination, including the combinations that must be refused.
struct RepairEvidence {
    repaired: bool,
    hausdorff_distance_m: Option<f64>,
    area_change_ratio: Option<f64>,
}

impl RepairEvidence {
    /// A geometry that was published as it arrived.
    const fn untouched() -> Self {
        Self {
            repaired: false,
            hausdorff_distance_m: None,
            area_change_ratio: None,
        }
    }

    /// A repair that measured these two numbers.
    const fn measured(hausdorff_distance_m: f64, area_change_ratio: f64) -> Self {
        Self {
            repaired: true,
            hausdorff_distance_m: Some(hausdorff_distance_m),
            area_change_ratio: Some(area_change_ratio),
        }
    }
}

/// One published boundary, with what `PostGIS` says about the geometry that was stored.
struct PublishedRow {
    geometry_repaired: bool,
    repair_hausdorff_distance_m: Option<f64>,
    repair_area_change_ratio: Option<f64>,
    geometry_type: String,
    is_valid: bool,
    parts: i32,
    /// `ST_Area` of the stored geometry taken back to the source CRS, so it can be compared with the
    /// metres the fixture ring was written in.
    source_crs_area_sqm: f64,
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
        Self::write_source_rows(
            path,
            object_key,
            &[
                (COMPLEX_A, CODE_A, square_wkb(EASTING, NORTHING)),
                (
                    COMPLEX_B,
                    CODE_B,
                    square_wkb(EASTING + 1_000.0, NORTHING + 1_000.0),
                ),
            ],
        )
    }

    /// An export of exactly the geometries the caller hands over.
    ///
    /// `area_sqm_calculated` stays [`Self::AREA_SQM`] for every row, because it is Silver's own
    /// number and the publish neither recomputes it nor compares it to the geometry. A fixture that
    /// derived it from the ring would be asserting a relationship the command does not have.
    fn write_source_rows(
        path: &Path,
        object_key: &str,
        rows: &[(&str, &str, Vec<u8>)],
    ) -> TestResult {
        let mut body = String::new();
        for (complex_id, code, wkb) in rows {
            let row = json!({
                "complex_id": complex_id,
                "official_complex_code": code,
                "boundary_kind": "official",
                "geometry_wkb_hex": hex_lower(wkb),
                "geometry_srid": 5186,
                "area_sqm_calculated": Self::AREA_SQM,
                "geometry_checksum_sha256": format!("{:x}", Sha256::digest(wkb)),
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
        repair: RepairEvidence,
    ) -> Result<(), sqlx::Error> {
        let mut transaction = pool.begin().await?;
        sqlx::query("SELECT set_config('foundation.temporal_publisher', 'on', true)")
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            // `geometry_repaired` is stated rather than defaulted: `20260821190721` takes the
            // default away so that a repaired geometry cannot be recorded as untouched by omission,
            // and this insert is the shape the command's own is. The geometry is always the valid
            // square, so a refusal here is about the three repair columns and nothing else.
            "INSERT INTO serving_postgis.industrial_complex_boundary_publication
                (complex_id, official_complex_code, projection_load_id, boundary_kind,
                 source_snapshot_id, bronze_object_id, source_object_key, area_sqm_calculated,
                 source_geometry_checksum_sha256, geom, geometry_repaired,
                 repair_hausdorff_distance_m, repair_area_change_ratio)
             SELECT $1::uuid, $2, $3, 'official', $4, $5, $6, 1.00, repeat('a', 64),
                    public.st_multi(public.st_transform(
                        public.st_setsrid(public.st_geomfromwkb(decode($7, 'hex')), 5186), 4326)),
                    $8, $9, $10",
        )
        .bind(complex_id)
        .bind(official_complex_code)
        .bind(projection_load_id)
        .bind(SOURCE_SNAPSHOT_ID)
        .bind(self.bronze_object_id)
        .bind(OBJECT_KEY)
        .bind(hex_lower(&square_wkb(EASTING, NORTHING)))
        .bind(repair.repaired)
        .bind(repair.hausdorff_distance_m)
        .bind(repair.area_change_ratio)
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

    /// One row of one load, by the code the layer contract publishes it under.
    async fn published_row(
        &self,
        pool: &PgPool,
        load_id: Uuid,
        official_complex_code: &str,
    ) -> TestResult<PublishedRow> {
        let row = sqlx::query(
            "SELECT geometry_repaired, repair_hausdorff_distance_m, repair_area_change_ratio,
                    public.geometrytype(geom) AS geometry_type,
                    public.st_isvalid(geom) AS is_valid,
                    public.st_numgeometries(geom) AS parts,
                    public.st_area(public.st_transform(geom, 5186)) AS source_crs_area_sqm
               FROM serving_postgis.industrial_complex_boundary_publication
              WHERE projection_load_id = $1 AND official_complex_code = $2",
        )
        .bind(load_id)
        .bind(official_complex_code)
        .fetch_one(pool)
        .await?;
        Ok(PublishedRow {
            geometry_repaired: row.try_get("geometry_repaired")?,
            repair_hausdorff_distance_m: row.try_get("repair_hausdorff_distance_m")?,
            repair_area_change_ratio: row.try_get("repair_area_change_ratio")?,
            geometry_type: row.try_get("geometry_type")?,
            is_valid: row.try_get("is_valid")?,
            parts: row.try_get("parts")?,
            source_crs_area_sqm: row.try_get("source_crs_area_sqm")?,
        })
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
fn square_wkb(easting: f64, northing: f64) -> Vec<u8> {
    ring_wkb(&[
        (easting, northing),
        (easting + SIDE_METRES, northing),
        (easting + SIDE_METRES, northing + SIDE_METRES),
        (easting, northing + SIDE_METRES),
        (easting, northing),
    ])
}

/// A ring that revisits a place it has already been: the defect the two real boundaries carry.
///
/// The midpoint appears twice, so the loop closes against itself there and encloses two triangles.
/// No pair of segments crosses — they meet at a shared endpoint — which is why `ST_MakeValid` can
/// cut the ring at that vertex without inventing a coordinate. On `247920` the two occurrences are
/// 0.000000 m apart, as they are here; on `141060` they are 0.0008 m apart.
fn self_touching_wkb(easting: f64, northing: f64) -> Vec<u8> {
    let middle = (easting + SIDE_METRES / 2.0, northing + SIDE_METRES / 2.0);
    ring_wkb(&[
        (easting, northing),
        (easting + SIDE_METRES, northing),
        middle,
        (easting + SIDE_METRES, northing + SIDE_METRES),
        (easting, northing + SIDE_METRES),
        middle,
        (easting, northing),
    ])
}

/// A ring whose two lobes genuinely cross, so that repairing it changes the area it encloses.
///
/// This is the case the tolerance exists for and the one the real boundaries are not: the crossing
/// point is on no vertex, `ST_Area` of the crossed ring counts the lobes against each other, and the
/// repair returns their sum. Deliberately lopsided — with equal lobes the crossed ring encloses zero
/// and the ratio is infinity, which is a second, easier thing to refuse.
fn bowtie_wkb(easting: f64, northing: f64) -> Vec<u8> {
    ring_wkb(&[
        (easting, northing),
        (easting + SIDE_METRES, northing),
        (
            easting + BOWTIE_WAIST_METRES,
            northing + BOWTIE_HEIGHT_METRES,
        ),
        (easting + SIDE_METRES, northing + BOWTIE_HEIGHT_METRES),
        (easting, northing),
    ])
}

/// A square with a zero-width spur, which `ST_MakeValid` repairs into a `GEOMETRYCOLLECTION`.
///
/// The spur is a line, so it cannot be part of a polygon and `ST_MakeValid` hands it back beside
/// one. Neither tolerance sees anything wrong: the square's area is untouched and no vertex moves.
fn spiked_wkb(easting: f64, northing: f64) -> Vec<u8> {
    let corner = (easting + SIDE_METRES, northing + SIDE_METRES);
    ring_wkb(&[
        (easting, northing),
        (easting + SIDE_METRES, northing),
        corner,
        (corner.0 + SPUR_METRES, corner.1),
        corner,
        (easting, northing + SIDE_METRES),
        (easting, northing),
    ])
}

/// A `UUIDv5` in the lakehouse's shape, distinct per `index`, for rows inserted straight into the
/// table. Each attempt needs its own id: a repeat would be refused by the primary key instead of by
/// the constraint under test, which reads identically from the outside.
fn synthetic_complex_id(index: u8) -> String {
    format!("7df3859c-0000-51fa-8000-{index:012}")
}

/// One closed ring as a standard little-endian WKB polygon, in the source's own metres.
///
/// Built here rather than fetched from `PostGIS`: the point of the fixture is to be the bytes Silver
/// stores, and asking the database to produce them would make the test agree with itself — which
/// matters most for the invalid rings above, where `PostGIS` would have refused to hand them back.
fn ring_wkb(corners: &[(f64, f64)]) -> Vec<u8> {
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
