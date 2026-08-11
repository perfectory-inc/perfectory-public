//! The parcel publication path — copy the mirror, then record what was copied — under CI.
//!
//! `serving_postgis.parcel_boundary_publication` is the table Martin's `parcel_boundary_current`
//! view reads and, until `publish-parcel-boundary-postgis`, the only things that ever wrote it were
//! the local seed and a migration backfill. ADR-0024 남은 부채 1 names that gap: two serving tables
//! coexist, a production command fills one, and the other is the one served. These tests hold the
//! command that closes it.
//!
//! The command is a process, not a function: it reads its whole configuration from the environment
//! and opens its own pool. So the tests drive the built binary and assert against the rows it
//! committed, which keeps the environment-variable contract itself under test.
//!
//! Each test takes its own database. The command creates the `parcels` publication unit, and
//! `catalog.promote_vector_tile_runtime_manifest` compares a manifest's unit count against
//! `count(*)` over every publication unit — a leaked one would fail the promotion suite while
//! reading like a promotion bug rather than a leak here.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::process::Command;

use foundation_disposable_database::{disposable_database_url, DisposableDatabaseUrl, TestResult};
use sqlx::{PgPool, Row};
use uuid::Uuid;

const BINARY: &str = env!("CARGO_BIN_EXE_foundation-outbox-publisher");
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

/// The `unit_key` the command materialises. Declared in the command as well; asserted here so a
/// rename has to change the test that proves the serving view can still find the load's unit.
const PARCEL_UNIT_KEY: &str = "parcels";
/// The mirror's lineage namespace. `parcel_boundary_mirror_source_snapshot_id_check` requires the
/// `iceberg:` prefix, which is a different vocabulary from the publication's decimal snapshot below.
const MIRROR_SOURCE_SNAPSHOT_ID: &str = "iceberg:parcel-boundary-publish-test";
const CANONICAL_SNAPSHOT_ID: &str = "841361364657368626";
/// The snapshot a second collection would carry, used where a revision has to describe another one.
const NEXT_CANONICAL_SNAPSHOT_ID: &str = "841361364657368627";
const OBJECT_KEY: &str = "publish-test/parcel-boundary/part-0001.jsonl";

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn a_publish_run_opens_one_load_and_closes_it_with_the_rows_it_wrote() -> TestResult {
    let fixture = Fixture::create("parcel_publish_once").await?;
    let pool = fixture.pool().await?;

    let output = fixture.publish()?;
    assert!(
        output.status.success(),
        "publish failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("parcel-boundary-postgis-publish-ok"));

    // The revision belongs to the parcels unit and claims no administrative lineage. Before ADR-0017
    // a parcels revision could only be registered as an administrative boundary fact, which is the
    // defect that blocked this loader.
    let revision = sqlx::query(
        "SELECT unit.unit_key, revision.derived_from_administrative_revision
           FROM catalog.publication_revision AS revision
           JOIN catalog.vector_tile_publication_unit AS unit
             ON unit.id = revision.publication_unit_id
          WHERE revision.id = $1",
    )
    .bind(fixture.revision)
    .fetch_one(&pool)
    .await?;
    assert_eq!(revision.try_get::<String, _>("unit_key")?, PARCEL_UNIT_KEY);
    assert_eq!(
        revision.try_get::<Option<Uuid>, _>("derived_from_administrative_revision")?,
        None,
        "a parcels revision asserts nothing about administrative boundaries"
    );

    let loads = fixture.loads(&pool).await?;
    assert_eq!(loads.len(), 1, "one run must open exactly one load");
    let load = &loads[0];
    assert_eq!(load.status, "succeeded");
    assert_eq!(load.loaded_row_count, Fixture::MIRROR_ROWS);
    assert_eq!(load.rejected_row_count, 0);
    assert_eq!(load.error_message, None);
    assert_eq!(
        fixture.publication_rows(&pool, load.id).await?,
        Fixture::MIRROR_ROWS
    );

    // The projection carries no membership claim. ADR-0024 leaves whether a serving row should carry
    // one open, and the mirror's own column has no production producer — so a value here could only
    // have been forwarded from nothing.
    let claimed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM serving_postgis.parcel_boundary_publication
          WHERE projection_load_id = $1 AND (complex_id IS NOT NULL OR parcel_id IS NOT NULL)",
    )
    .bind(load.id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(claimed, 0);

    fixture.finish(pool).await
}

/// A re-publish is a second load, and each load counts only the rows it wrote.
///
/// This is the regression ADR-0016 names, asserted on the parcel side: under a revision-scoped count
/// the second run would report the first run's rows as its own, so a cumulative count would make the
/// second load twice the first.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn republishing_one_mirror_rebuild_opens_a_second_load_that_counts_only_its_own_rows(
) -> TestResult {
    let fixture = Fixture::create("parcel_publish_twice").await?;
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
    for load in &loads {
        assert_eq!(load.status, "succeeded");
        assert_eq!(load.loaded_row_count, Fixture::MIRROR_ROWS);
        assert_eq!(
            fixture.publication_rows(&pool, load.id).await?,
            Fixture::MIRROR_ROWS
        );
    }

    let total: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM serving_postgis.parcel_boundary_publication WHERE data_revision = $1",
    )
    .bind(fixture.revision)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        total,
        Fixture::MIRROR_ROWS * 2,
        "both loads are retained; neither overwrites"
    );

    fixture.finish(pool).await
}

/// A publish whose named input is not a completed rebuild must leave no ledger row at all.
///
/// This is the half of the split the command's first transaction owns: refusal is still free here,
/// so it costs nothing and records nothing. A `running` row surviving would be an orphan no later
/// run can close.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn a_publish_naming_an_unfinished_mirror_rebuild_leaves_no_load_behind() -> TestResult {
    let fixture = Fixture::create_with("parcel_publish_unfinished", "running", 0, 0).await?;
    let pool = fixture.pool().await?;

    let output = fixture.publish()?;

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("is 'running', not 'succeeded'"),
        "unexpected failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fixture.loads(&pool).await?.len(), 0);
    let revisions: i64 = sqlx::query_scalar("SELECT count(*) FROM catalog.publication_revision")
        .fetch_one(&pool)
        .await?;
    assert_eq!(revisions, 0);

    fixture.finish(pool).await
}

/// The other half of the split: a failure with the load already open is closed as `failed`.
///
/// `parcel_boundary_mirror` is `UNLOGGED` and a national rebuild replaces it wholesale, while the
/// rebuild-run ledger is logged — so a run row outliving its rows is the expected disagreement, and
/// this is the state ADR-0016 남은 부채 2 records as having no production writer. The load must carry
/// the reason, and it must carry no geometry: the materialising transaction rolled back.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn a_mirror_short_of_what_its_rebuild_recorded_closes_the_load_failed() -> TestResult {
    let fixture = Fixture::create_with("parcel_publish_short", "succeeded", 3, 2).await?;
    let pool = fixture.pool().await?;

    let output = fixture.publish()?;

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("was closed as failed"),
        "unexpected failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let loads = fixture.loads(&pool).await?;
    assert_eq!(
        loads.len(),
        1,
        "the load stays; it is the record of failure"
    );
    let load = &loads[0];
    assert_eq!(load.status, "failed");
    assert_eq!(load.loaded_row_count, 0);
    assert!(
        load.error_message
            .as_deref()
            .unwrap_or_default()
            .contains("recorded 3 row(s) and the mirror now holds 2"),
        "the load must carry the reason: {:?}",
        load.error_message
    );
    assert_eq!(
        fixture.publication_rows(&pool, load.id).await?,
        0,
        "a failed load publishes nothing"
    );

    fixture.finish(pool).await
}

/// Reusing a revision id for another canonical snapshot is refused, and only the read-back can.
///
/// The revision insert is `ON CONFLICT (id) DO NOTHING`, so an existing row keeps whatever it said
/// while every input was checked against the environment instead. The load's composite foreign key
/// would refuse the difference one statement later as a bare constraint violation naming a key,
/// which reads like a schema problem rather than "this revision already means something else".
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn a_revision_id_already_describing_another_snapshot_is_refused() -> TestResult {
    let fixture = Fixture::create("parcel_publish_revision_reuse").await?;
    let pool = fixture.pool().await?;
    fixture
        .seed_revision(&pool, NEXT_CANONICAL_SNAPSHOT_ID)
        .await?;

    let output = fixture.publish()?;

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("needs a new revision id"),
        "unexpected failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fixture.loads(&pool).await?.len(), 0);
    let stored: String = sqlx::query_scalar(
        "SELECT canonical_iceberg_snapshot_id FROM catalog.publication_revision WHERE id = $1",
    )
    .bind(fixture.revision)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        stored, NEXT_CANONICAL_SNAPSHOT_ID,
        "a refused publish leaves the registered revision as it was"
    );

    fixture.finish(pool).await
}

struct ProjectionLoad {
    id: Uuid,
    status: String,
    loaded_row_count: i64,
    rejected_row_count: i64,
    error_message: Option<String>,
}

struct Fixture {
    database: DisposableDatabaseUrl,
    revision: Uuid,
    source_record_id: Uuid,
    mirror_rebuild_run_id: Uuid,
}

impl Fixture {
    /// Every mirror row becomes one publication row, so the load's count and this agree.
    const MIRROR_ROWS: i64 = 3;

    async fn create(label: &str) -> TestResult<Self> {
        Self::create_with(label, "succeeded", Self::MIRROR_ROWS, Self::MIRROR_ROWS).await
    }

    /// `recorded_row_count` and `mirror_rows` are set apart so a rebuild run can be made to disagree
    /// with the rows it claims — the state the mirror's `UNLOGGED` storage makes reachable.
    async fn create_with(
        label: &str,
        status: &str,
        recorded_row_count: i64,
        mirror_rows: i64,
    ) -> TestResult<Self> {
        let fixture = Self {
            database: disposable_database_url(label).await?,
            revision: Uuid::new_v4(),
            source_record_id: Uuid::new_v4(),
            mirror_rebuild_run_id: Uuid::new_v4(),
        };
        let pool = fixture.pool().await?;
        MIGRATOR.run(&pool).await?;
        fixture.seed_source_record(&pool).await?;
        fixture
            .seed_mirror_rebuild_run(&pool, status, recorded_row_count)
            .await?;
        fixture.seed_mirror_rows(&pool, mirror_rows).await?;
        pool.close().await;
        Ok(fixture)
    }

    async fn pool(&self) -> TestResult<PgPool> {
        self.database.pool().await
    }

    /// Drops the disposable database, reporting a cleanup failure rather than swallowing it.
    async fn finish(self, pool: PgPool) -> TestResult {
        pool.close().await;
        self.database.drop_database().await
    }

    async fn seed_source_record(&self, pool: &PgPool) -> TestResult {
        sqlx::query(
            "INSERT INTO catalog.source_record
                (id, source, external_id, checksum_sha256, raw_object_key)
             VALUES ($1, 'parcel-boundary-publish-test', $2, repeat('e', 64), $3)",
        )
        .bind(self.source_record_id)
        .bind(format!("publish-test-{}", self.source_record_id))
        .bind(OBJECT_KEY)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// One rebuild-run row in the shape `rebuild-postgis-parcel-boundary-mirror-national` commits.
    ///
    /// `finished_at` follows the status because the table refuses a `succeeded` run without one, and
    /// `started_at` has no default — the column is supplied by the command that opens the run.
    async fn seed_mirror_rebuild_run(
        &self,
        pool: &PgPool,
        status: &str,
        recorded_row_count: i64,
    ) -> TestResult {
        sqlx::query(
            "INSERT INTO serving_postgis.parcel_boundary_mirror_rebuild_run
                (id, source_snapshot_id, source_table, srid, status, loaded_row_count,
                 rejected_row_count, quality_report, started_at, finished_at)
             VALUES ($1, $2, 'silver.parcel_boundaries', 5179, $3, $4, 0, '{}'::jsonb, now(),
                     CASE WHEN $3::text = 'running' THEN NULL ELSE now() END)",
        )
        .bind(self.mirror_rebuild_run_id)
        .bind(MIRROR_SOURCE_SNAPSHOT_ID)
        .bind(status)
        .bind(recorded_row_count)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Mirror rows in the SRID the projection stores, built the way the production loader builds
    /// them: a WGS84 polygon reprojected to EPSG:5179 and collected into a multipolygon.
    async fn seed_mirror_rows(&self, pool: &PgPool, rows: i64) -> TestResult {
        for (index, corners) in SQUARES.iter().enumerate().take(usize::try_from(rows)?) {
            let (west, south, east, north) = *corners;
            sqlx::query(
                "INSERT INTO serving_postgis.parcel_boundary_mirror
                    (pnu, rebuild_run_id, source_snapshot_id, source_table, source_object_key,
                     source_row_id, geometry_checksum_sha256, properties, geom)
                 VALUES ($1, $2, $3, 'silver.parcel_boundaries', $4, $1, $5,
                         jsonb_build_object('boundary_id', $1),
                         public.st_multi(public.st_transform(
                             public.st_setsrid(public.st_geomfromtext($6), 4326), 5179)))",
            )
            .bind(format!("9999900101100010{:03}", index + 1))
            .bind(self.mirror_rebuild_run_id)
            .bind(MIRROR_SOURCE_SNAPSHOT_ID)
            .bind(OBJECT_KEY)
            .bind(format!("{index:064x}"))
            .bind(format!(
                "POLYGON(({west} {south},{east} {south},{east} {north},{west} {north},{west} {south}))"
            ))
            .execute(pool)
            .await?;
        }
        Ok(())
    }

    /// Registers the fixture's revision id under the parcels unit at `canonical_snapshot`.
    ///
    /// The capability and the insert share one transaction: `set_config(..., true)` is
    /// transaction-local, and `publication_revision_publisher_only` covers INSERT. Writing a revision
    /// is publishing one, so a fixture has to hold the capability exactly as the command does.
    async fn seed_revision(&self, pool: &PgPool, canonical_snapshot: &str) -> TestResult {
        let mut transaction = pool.begin().await?;
        sqlx::query("SELECT set_config('foundation.temporal_publisher', 'on', true)")
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "INSERT INTO catalog.vector_tile_publication_unit (id, unit_key)
             VALUES (gen_random_uuid(), $1) ON CONFLICT (unit_key) DO NOTHING",
        )
        .bind(PARCEL_UNIT_KEY)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.publication_revision
                (id, publication_unit_id, canonical_iceberg_snapshot_id, source_record_id)
             SELECT $1, unit.id, $2, $3
               FROM catalog.vector_tile_publication_unit AS unit
              WHERE unit.unit_key = $4",
        )
        .bind(self.revision)
        .bind(canonical_snapshot)
        .bind(self.source_record_id)
        .bind(PARCEL_UNIT_KEY)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    fn publish(&self) -> TestResult<std::process::Output> {
        Command::new(BINARY)
            .arg("publish-parcel-boundary-postgis")
            .env("DATABASE_URL", self.database.url())
            .env(
                "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_POSTGIS_PUBLISH_CONFIRM",
                "1",
            )
            .env(
                "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_POSTGIS_PUBLISH_DATA_REVISION",
                self.revision.to_string(),
            )
            .env(
                "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_POSTGIS_PUBLISH_CANONICAL_ICEBERG_SNAPSHOT_ID",
                CANONICAL_SNAPSHOT_ID,
            )
            .env(
                "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_POSTGIS_PUBLISH_SOURCE_RECORD_ID",
                self.source_record_id.to_string(),
            )
            .env(
                "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_POSTGIS_PUBLISH_MIRROR_REBUILD_RUN_ID",
                self.mirror_rebuild_run_id.to_string(),
            )
            .output()
            .map_err(Into::into)
    }

    /// Every parcels load in this database, oldest first.
    async fn loads(&self, pool: &PgPool) -> TestResult<Vec<ProjectionLoad>> {
        let rows = sqlx::query(
            "SELECT load.id, load.status, load.loaded_row_count, load.rejected_row_count,
                    load.error_message
               FROM serving_postgis.spatial_projection_load AS load
               JOIN catalog.vector_tile_publication_unit AS unit
                 ON unit.id = load.publication_unit_id
              WHERE unit.unit_key = $1
              ORDER BY load.started_at",
        )
        .bind(PARCEL_UNIT_KEY)
        .fetch_all(pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ProjectionLoad {
                    id: row.try_get("id")?,
                    status: row.try_get("status")?,
                    loaded_row_count: row.try_get("loaded_row_count")?,
                    rejected_row_count: row.try_get("rejected_row_count")?,
                    error_message: row.try_get("error_message")?,
                })
            })
            .collect()
    }

    async fn publication_rows(&self, pool: &PgPool, load_id: Uuid) -> TestResult<i64> {
        Ok(sqlx::query_scalar(
            "SELECT count(*) FROM serving_postgis.parcel_boundary_publication
              WHERE projection_load_id = $1",
        )
        .bind(load_id)
        .fetch_one(pool)
        .await?)
    }
}

/// Corners as `(west, south, east, north)`, inside the reserved synthetic coordinate band.
///
/// `scripts/guard/public-fixture-safety.py` reserves a narrow synthetic longitude band for fixtures
/// and rejects every other Korea-area value; `RESERVED_LONGITUDE` there is the one definition of it.
/// These sit inside that band, one disjoint square per parcel.
const SQUARES: [(f64, f64, f64, f64); 3] = [
    (127.1231, 36.1231, 127.1232, 36.1232),
    (127.1233, 36.1233, 127.1234, 36.1234),
    (127.1235, 36.1235, 127.1236, 36.1236),
];
