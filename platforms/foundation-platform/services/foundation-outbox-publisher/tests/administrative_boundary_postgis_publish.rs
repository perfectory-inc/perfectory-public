//! The only production writer of a spatial projection load, under a check CI actually runs.
//!
//! `publish-administrative-boundary-postgis` opens the load, writes the geometry rows, and closes
//! the load with the counts those rows produced. Until now the only thing that exercised it end to
//! end was `scripts/tiles/administrative-boundary-slice-proof.sh`, which needs Docker Compose and a
//! Martin deployment and therefore is not in CI. ADR-0016 records that absence as the reason
//! earlier regressions in exactly this path reached the branch uncaught.
//!
//! The command is a process, not a function: it reads its whole configuration from the environment
//! and opens its own pool. So these tests drive the built binary and assert against the rows it
//! committed, which also keeps the environment-variable contract itself under test.
//!
//! Each test takes its own database. `catalog.promote_vector_tile_runtime_manifest` compares a
//! manifest's unit count against `count(*)` over every publication unit, and this command creates
//! the `admin` unit — a leaked one would fail the promotion suite while reading like a promotion
//! bug rather than a leak here.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::{env, fs, path::PathBuf, process::Command};

use foundation_disposable_database::{disposable_database_url, DisposableDatabaseUrl, TestResult};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use uuid::Uuid;

const BINARY: &str = env!("CARGO_BIN_EXE_foundation-outbox-publisher");
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

/// The `unit_key` the command materialises. Declared in the command as well; asserted here so a
/// rename has to change the test that proves the promote gate can still find the load's unit.
const ADMINISTRATIVE_UNIT_KEY: &str = "admin";
const SOURCE_SNAPSHOT_ID: &str = "iceberg:administrative-boundary-publish-test";
const CANONICAL_SNAPSHOT_ID: &str = "841361364657368624";
const OBJECT_KEY: &str = "publish-test/administrative-boundary/fixture.geojson";
const VALID_FROM: &str = "2026-07-01T00:00:00Z";

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
#[allow(clippy::too_many_lines)]
async fn a_publish_run_opens_one_load_and_closes_it_with_the_rows_it_wrote() -> TestResult {
    let fixture = Fixture::create("admin_publish_once").await?;
    let pool = fixture.pool().await?;

    let output = fixture.publish(fixture.data_revision)?;
    assert!(
        output.status.success(),
        "publish failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout)
        .contains("administrative-boundary-postgis-publish-ok"));

    // The revision the load names belongs to the unit the load names. Before ADR-0017 a revision
    // could only be registered in the administrative boundary ledger, so this pairing had nowhere
    // to be asserted.
    let revision = sqlx::query(
        "SELECT unit.unit_key, revision.derived_from_administrative_revision
           FROM catalog.publication_revision AS revision
           JOIN catalog.vector_tile_publication_unit AS unit
             ON unit.id = revision.publication_unit_id
          WHERE revision.id = $1",
    )
    .bind(fixture.data_revision)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        revision.try_get::<String, _>("unit_key")?,
        ADMINISTRATIVE_UNIT_KEY
    );
    assert_eq!(
        revision.try_get::<Option<Uuid>, _>("derived_from_administrative_revision")?,
        Some(fixture.data_revision),
        "the publication revision must keep lineage to the boundary fact it was built from"
    );

    let loads = fixture.loads(&pool).await?;
    assert_eq!(loads.len(), 1, "one run must open exactly one load");
    let load = &loads[0];
    assert_eq!(load.status, "succeeded");
    assert_eq!(load.loaded_row_count, Some(Fixture::GEOMETRY_ROWS));
    assert_eq!(load.rejected_row_count, Some(0));

    // Every geometry row carries the load that wrote it, and none carries anything else. The
    // publication table is keyed on the load precisely so a re-publish cannot overwrite in place.
    assert_eq!(
        fixture.publication_rows(&pool, load.id).await?,
        Fixture::GEOMETRY_ROWS
    );
    let foreign: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM serving_postgis.administrative_unit_boundary_publication
          WHERE data_revision = $1 AND projection_load_id <> $2",
    )
    .bind(fixture.data_revision)
    .bind(load.id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(foreign, 0);

    let status: String = sqlx::query_scalar(
        "SELECT status FROM catalog.administrative_boundary_revision WHERE id = $1",
    )
    .bind(fixture.data_revision)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        status, "validated",
        "a published candidate becomes validated"
    );

    fixture.finish(pool).await
}

/// The regression ADR-0016 names: a count read by revision rather than by load.
///
/// Under the revision-scoped count the second run reported the first run's rows as its own, so a
/// run that wrote nothing still closed `succeeded` with a plausible number. Asserting that *both*
/// loads carry the same row count is what distinguishes the two shapes — a cumulative count would
/// make the second one twice the first.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn republishing_a_revision_opens_a_second_load_that_counts_only_its_own_rows() -> TestResult {
    let fixture = Fixture::create("admin_publish_twice").await?;
    let pool = fixture.pool().await?;

    for attempt in 0..2 {
        let output = fixture.publish(fixture.data_revision)?;
        assert!(
            output.status.success(),
            "publish attempt {attempt} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let loads = fixture.loads(&pool).await?;
    assert_eq!(loads.len(), 2, "a re-publish is a second load, not an edit");
    assert_ne!(loads[0].id, loads[1].id);
    for load in &loads {
        assert_eq!(load.status, "succeeded");
        assert_eq!(
            load.loaded_row_count,
            Some(Fixture::GEOMETRY_ROWS),
            "each load counts the rows it wrote, not every row under the revision"
        );
        assert_eq!(
            fixture.publication_rows(&pool, load.id).await?,
            Fixture::GEOMETRY_ROWS
        );
    }

    let total: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM serving_postgis.administrative_unit_boundary_publication
          WHERE data_revision = $1",
    )
    .bind(fixture.data_revision)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        total,
        Fixture::GEOMETRY_ROWS * 2,
        "both loads are retained; neither overwrites"
    );

    fixture.finish(pool).await
}

/// A publish that cannot prove its revision must leave no ledger row at all.
///
/// The load is opened inside the same transaction as the geometry, so a rejection has to take the
/// opened load with it. A `running` row surviving here would be an orphan no later run can close.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn a_publish_naming_an_unknown_revision_leaves_no_load_behind() -> TestResult {
    let fixture = Fixture::create("admin_publish_unknown").await?;
    let pool = fixture.pool().await?;
    let unknown = Uuid::new_v4();

    let output = fixture.publish(unknown)?;

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("data revision does not exist"),
        "unexpected failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let loads: i64 =
        sqlx::query_scalar("SELECT count(*) FROM serving_postgis.spatial_projection_load")
            .fetch_one(&pool)
            .await?;
    assert_eq!(loads, 0);
    let revisions: i64 = sqlx::query_scalar("SELECT count(*) FROM catalog.publication_revision")
        .fetch_one(&pool)
        .await?;
    assert_eq!(revisions, 0);

    fixture.finish(pool).await
}

/// The encoding assumption the checksum contract rests on, asserted without a database.
///
/// The command hashes the geometry it parsed back out of the file, while a producer hashes the
/// value it is about to write. Those agree only if printing JSON is a fixed point — `print(parse(
/// print(v))) == print(v)`. If it ever is not, the failure surfaces at the far end as
/// `geometry_sha256 mismatch at line N`, which reads like corrupted input rather than an encoding
/// difference. This states it as what it is, and covers the coordinates the fixture actually uses.
#[test]
fn printing_the_fixture_geometry_is_a_fixed_point() -> TestResult {
    for geometry in [SIDO_SQUARE, SIGUNGU_SQUARE, LEGAL_DONG_SQUARE].map(square) {
        let printed = serde_json::to_vec(&geometry)?;
        let parsed: Value = serde_json::from_slice(&printed)?;
        assert_eq!(
            serde_json::to_vec(&parsed)?,
            printed,
            "re-printing a parsed geometry changed its bytes: {geometry}"
        );
    }
    Ok(())
}

struct ProjectionLoad {
    id: Uuid,
    status: String,
    loaded_row_count: Option<i64>,
    rejected_row_count: Option<i64>,
}

struct Fixture {
    database: DisposableDatabaseUrl,
    root: PathBuf,
    data_revision: Uuid,
    source_record_id: Uuid,
}

impl Fixture {
    /// Every source row carries geometry, so the load's row count and the source row count agree.
    const GEOMETRY_ROWS: i64 = 3;

    async fn create(label: &str) -> TestResult<Self> {
        let database = disposable_database_url(label).await?;
        // The system temp directory rather than the workspace `target/`. The command takes an
        // absolute path, so nothing here needs to live inside the repository, and resolving the
        // crate's manifest directory at compile time would add a data dependency that
        // `build-coupling-baseline` prices and a sandboxed build system would have to be told about.
        let root = env::temp_dir().join(format!("{label}-{}", Uuid::new_v4()));
        fs::create_dir_all(&root)?;

        let fixture = Self {
            database,
            root,
            data_revision: Uuid::new_v4(),
            source_record_id: Uuid::new_v4(),
        };
        fixture.write_source()?;
        fixture.write_registry_evidence()?;

        let pool = fixture.pool().await?;
        MIGRATOR.run(&pool).await?;
        fixture.seed_lineage(&pool).await?;
        pool.close().await;
        Ok(fixture)
    }

    async fn pool(&self) -> TestResult<PgPool> {
        self.database.pool().await
    }

    /// Drops the disposable database, reporting a cleanup failure rather than swallowing it.
    async fn finish(self, pool: PgPool) -> TestResult {
        pool.close().await;
        let _ = fs::remove_dir_all(&self.root);
        self.database.drop_database().await
    }

    async fn seed_lineage(&self, pool: &PgPool) -> TestResult {
        sqlx::query(
            "INSERT INTO catalog.source_record
                (id, source, external_id, checksum_sha256, raw_object_key)
             VALUES ($1, 'official-administrative-boundary-publish-test', $2, repeat('e', 64), $3)",
        )
        .bind(self.source_record_id)
        .bind(format!("publish-test-{}", self.source_record_id))
        .bind(OBJECT_KEY)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.administrative_boundary_revision
                (id, canonical_iceberg_snapshot_id, source_snapshot_id, source_record_id, status)
             VALUES ($1, $2, $3, $4, 'candidate')",
        )
        .bind(self.data_revision)
        .bind(CANONICAL_SNAPSHOT_ID)
        .bind(SOURCE_SNAPSHOT_ID)
        .bind(self.source_record_id)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// One row per level, because `catalog.validate_administrative_unit_parent` requires a
    /// `legal_dong` to hang off a `sigungu` and a `sigungu` off a `sido`. A two-level fixture is
    /// rejected before any geometry lands, so the shallower shape could not exercise the load at all.
    fn write_source(&self) -> TestResult {
        let rows = [
            source_row("sido", "99", "검증용 광역", "", "", SIDO_SQUARE),
            source_row(
                "sigungu",
                "99999",
                "검증용 시군구",
                "sido",
                "99",
                SIGUNGU_SQUARE,
            ),
            source_row(
                "legal_dong",
                "9999900101",
                "검증용 법정동",
                "sigungu",
                "99999",
                LEGAL_DONG_SQUARE,
            ),
        ];
        let mut body = String::new();
        for row in rows {
            body.push_str(&serde_json::to_string(&row)?);
            body.push('\n');
        }
        fs::write(self.source_path(), body)?;
        Ok(())
    }

    fn write_registry_evidence(&self) -> TestResult {
        fs::write(
            self.registry_evidence_path(),
            serde_json::to_vec(&json!({"status": "ready"}))?,
        )?;
        Ok(())
    }

    fn source_path(&self) -> PathBuf {
        self.root.join("source.jsonl")
    }

    fn registry_evidence_path(&self) -> PathBuf {
        self.root.join("registry-evidence.json")
    }

    fn publish(&self, data_revision: Uuid) -> TestResult<std::process::Output> {
        Command::new(BINARY)
            .arg("publish-administrative-boundary-postgis")
            .env("DATABASE_URL", self.database.url())
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_CONFIRM",
                "1",
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_PATH",
                self.source_path(),
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_REGISTRY_EVIDENCE_PATH",
                self.registry_evidence_path(),
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_DATA_REVISION",
                data_revision.to_string(),
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_CANONICAL_ICEBERG_SNAPSHOT_ID",
                CANONICAL_SNAPSHOT_ID,
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_SNAPSHOT_ID",
                SOURCE_SNAPSHOT_ID,
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_RECORD_ID",
                self.source_record_id.to_string(),
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_OBJECT_KEY",
                OBJECT_KEY,
            )
            .output()
            .map_err(Into::into)
    }

    /// Every load for this fixture's revision, oldest first.
    async fn loads(&self, pool: &PgPool) -> TestResult<Vec<ProjectionLoad>> {
        let rows = sqlx::query(
            "SELECT load.id, load.status, load.loaded_row_count, load.rejected_row_count
               FROM serving_postgis.spatial_projection_load AS load
               JOIN catalog.vector_tile_publication_unit AS unit
                 ON unit.id = load.publication_unit_id
              WHERE load.data_revision = $1 AND unit.unit_key = $2
              ORDER BY load.started_at",
        )
        .bind(self.data_revision)
        .bind(ADMINISTRATIVE_UNIT_KEY)
        .fetch_all(pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ProjectionLoad {
                    id: row.try_get("id")?,
                    status: row.try_get("status")?,
                    loaded_row_count: row.try_get("loaded_row_count")?,
                    rejected_row_count: row.try_get("rejected_row_count")?,
                })
            })
            .collect()
    }

    async fn publication_rows(&self, pool: &PgPool, load_id: Uuid) -> TestResult<i64> {
        Ok(sqlx::query_scalar(
            "SELECT count(*) FROM serving_postgis.administrative_unit_boundary_publication
              WHERE projection_load_id = $1",
        )
        .bind(load_id)
        .fetch_one(pool)
        .await?)
    }
}

/// A source row in the shape `write-official-administrative-boundary-source-snapshot` emits.
///
/// The checksum is computed over the parsed geometry rather than over the text, because the command
/// verifies it that way — hashing the string this test happens to write would let a serialisation
/// difference pass here and fail in the slice proof.
fn source_row(
    scope_kind: &str,
    canonical_code: &str,
    source_name: &str,
    parent_scope_kind: &str,
    parent_canonical_code: &str,
    corners: (f64, f64, f64, f64),
) -> Value {
    let geometry = square(corners);
    let geometry_sha256 = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&geometry).expect("geometry must serialise"))
    );
    json!({
        "scope_kind": scope_kind,
        "canonical_code": canonical_code,
        "valid_from_utc": VALID_FROM,
        "status": "active",
        "geometry_srid": 4326,
        "source_provider": "official-administrative-boundary-publish-test",
        "source_snapshot_id": SOURCE_SNAPSHOT_ID,
        "source_name": source_name,
        "parent_scope_kind": parent_scope_kind,
        "parent_canonical_code": parent_canonical_code,
        "geometry": geometry,
        "geometry_sha256": geometry_sha256,
    })
}

/// Corners as `(west, south, east, north)`, inside the reserved synthetic coordinate band.
///
/// `scripts/guard/public-fixture-safety.py` reserves a narrow synthetic longitude band for fixtures
/// and rejects every other Korea-area value; `RESERVED_LONGITUDE` there is the one definition of it.
/// These sit inside that band, one disjoint square per unit.
///
/// They are written as literals rather than derived from an origin and a side, and that is not
/// style. A computed corner — `37.2 + 0.1` — lands on an f64 whose shortest decimal form needs
/// seventeen digits, which `serde_json` parses back one unit in the last place away. The command
/// hashes the geometry it parsed out of the file, so the fixture's own checksum stopped matching and
/// the failure surfaced as `geometry_sha256 mismatch`, reading like corrupted input rather than an
/// encoding difference. Literals parsed by the compiler round-trip; the arithmetic that produced the
/// long form is simply gone. [`printing_the_fixture_geometry_is_a_fixed_point`] holds that line.
const SIDO_SQUARE: (f64, f64, f64, f64) = (127.1231, 36.1231, 127.1232, 36.1232);
const SIGUNGU_SQUARE: (f64, f64, f64, f64) = (127.1233, 36.1233, 127.1234, 36.1234);
const LEGAL_DONG_SQUARE: (f64, f64, f64, f64) = (127.1235, 36.1235, 127.1236, 36.1236);

/// A closed, counter-clockwise square: the smallest polygon `st_isvalid` accepts.
fn square(corners: (f64, f64, f64, f64)) -> Value {
    let (longitude, latitude, east, north) = corners;
    json!({
        "type": "Polygon",
        "coordinates": [[
            [longitude, latitude],
            [east, latitude],
            [east, north],
            [longitude, north],
            [longitude, latitude],
        ]],
    })
}
