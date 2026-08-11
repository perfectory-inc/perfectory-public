//! The administrative boundary publication path — write the rows, then serve them — under CI.
//!
//! Two operator commands, one after the other. `publish-administrative-boundary-postgis` opens a
//! projection load, writes the geometry rows, and closes the load with the counts those rows
//! produced. `promote-administrative-boundary-runtime` then builds the immutable release and
//! complete manifest and moves the runtime pointer, which is what decides the rows the public map
//! reads.
//!
//! Until now the only thing that exercised either end to end was
//! `scripts/tiles/administrative-boundary-slice-proof.sh`, which needs Docker Compose and a Martin
//! deployment and therefore is not in CI. ADR-0016 records that absence as the reason earlier
//! regressions in exactly this path reached the branch uncaught. Neither command needs any of that:
//! both are `DATABASE_URL` and their own inputs. Martin enters only where the slice proof goes on to
//! decode a tile, which is still the slice proof's job.
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
/// The snapshot a corrected collection would carry. Strictly greater than the first: the promotion
/// gate refuses a manifest that moves a dynamic unit to an older canonical snapshot.
const NEXT_CANONICAL_SNAPSHOT_ID: &str = "841361364657368625";
const OBJECT_KEY: &str = "publish-test/administrative-boundary/fixture.geojson";
const VALID_FROM: &str = "2026-07-01T00:00:00Z";
/// A later legal effective date for the corrected revision. `publish_parent` closes the open period
/// only when the new date is strictly later, so a second revision dated the same day would leave two
/// open-ended parent rows and `administrative_unit_parent_one_parent_excl` would refuse it.
const NEXT_VALID_FROM: &str = "2026-08-01T00:00:00Z";
/// The promote command requires this shape: `http…` and ending in the unit's tile path.
const TILES_URL_TEMPLATE: &str = "http://127.0.0.1:3112/admin/{z}/{x}/{y}";

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
#[allow(clippy::too_many_lines)]
async fn a_publish_run_opens_one_load_and_closes_it_with_the_rows_it_wrote() -> TestResult {
    let fixture = Fixture::create("admin_publish_once").await?;
    let pool = fixture.pool().await?;

    let output = fixture.publish(fixture.revision)?;
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
    .bind(fixture.revision.id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        revision.try_get::<String, _>("unit_key")?,
        ADMINISTRATIVE_UNIT_KEY
    );
    assert_eq!(
        revision.try_get::<Option<Uuid>, _>("derived_from_administrative_revision")?,
        Some(fixture.revision.id),
        "the publication revision must keep lineage to the boundary fact it was built from"
    );

    let loads = fixture.loads(&pool, fixture.revision).await?;
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
    .bind(fixture.revision.id)
    .bind(load.id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(foreign, 0);

    let status: String = sqlx::query_scalar(
        "SELECT status FROM catalog.administrative_boundary_revision WHERE id = $1",
    )
    .bind(fixture.revision.id)
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
        let output = fixture.publish(fixture.revision)?;
        assert!(
            output.status.success(),
            "publish attempt {attempt} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let loads = fixture.loads(&pool, fixture.revision).await?;
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
    .bind(fixture.revision.id)
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
    // Registered nowhere. The snapshot is the fixture's, so the only thing wrong is the identity.
    let unknown = Revision {
        id: Uuid::new_v4(),
        canonical_snapshot: CANONICAL_SNAPSHOT_ID,
        valid_from: VALID_FROM,
    };

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

/// The whole operator flow: publish, then move the runtime pointer to the load that publish wrote.
///
/// The pointer is what decides which rows the public map reads, and until now the only thing that
/// exercised the command that moves it needed Docker Compose and a Martin deployment. It does not:
/// the promotion is `DATABASE_URL` and its own inputs, and Martin only enters when the slice proof
/// goes on to decode a tile.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn a_promotion_moves_the_pointer_to_the_load_its_release_names() -> TestResult {
    let fixture = Fixture::create("admin_promote_once").await?;
    let pool = fixture.pool().await?;
    let load_id = fixture.publish_one_load(&pool, fixture.revision).await?;

    assert_eq!(fixture.pointer(&pool).await?, None, "nothing is served yet");
    let promotion = Promotion::bootstrap(fixture.revision, load_id);
    let output = fixture.promote(&promotion)?;
    assert!(
        output.status.success(),
        "promote failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let (manifest_id, generation) = fixture
        .pointer(&pool)
        .await?
        .ok_or_else(|| std::io::Error::other("promotion left no runtime pointer"))?;
    assert_eq!(manifest_id, promotion.manifest);
    assert_eq!(generation, 1, "the first manifest is generation 1");

    // The release names the load, not the revision. That column used to be bound to the data
    // revision twice over, and the serving views join through it — a release pointing at anything
    // else serves rows some other run wrote.
    let served_load: Option<Uuid> = sqlx::query_scalar(
        "SELECT postgis_projection_revision FROM catalog.vector_tile_release WHERE id = $1",
    )
    .bind(promotion.release)
    .fetch_one(&pool)
    .await?;
    assert_eq!(served_load, Some(load_id));

    let unit = sqlx::query(
        "SELECT unit.serving_generation, unit.active_release_id, selection.release_id
           FROM catalog.vector_tile_publication_unit AS unit
           JOIN catalog.vector_tile_runtime_manifest_unit AS selection
             ON selection.publication_unit_id = unit.id AND selection.manifest_id = $1
          WHERE unit.unit_key = $2",
    )
    .bind(promotion.manifest)
    .bind(ADMINISTRATIVE_UNIT_KEY)
    .fetch_one(&pool)
    .await?;
    assert_eq!(unit.try_get::<i64, _>("serving_generation")?, 1);
    assert_eq!(
        unit.try_get::<Option<Uuid>, _>("active_release_id")?,
        Some(promotion.release)
    );
    assert_eq!(
        unit.try_get::<Uuid, _>("release_id")?,
        promotion.release,
        "the manifest must select the release the unit just became active on"
    );

    fixture.finish(pool).await
}

/// A second promotion advances both counters, and they are different facts.
///
/// `serving_generation` belongs to one unit's source selection; `manifest_generation` is global.
/// The command once incremented the serving generation of *every* unit in the manifest, which
/// `20260730000003` later made wrong — a unit re-selecting the release it already serves holds its
/// generation. Asserting the promoted unit's counter here is what keeps that narrowing in place.
///
/// The second promotion needs a second *revision*, not merely a second load.
/// `vector_tile_release_unit_revision_snapshot_kind_key` admits one dynamic release per
/// `(unit, revision, snapshot)`, so re-publishing the same revision and promoting it again is a
/// state the schema refuses ([ADR-0013](../../../../../docs/adr/0013-release-uniqueness-admits-both-source-kinds.md)).
/// That is also the only way the `+ 1` branch is reachable at all.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn a_second_promotion_advances_the_serving_and_manifest_generations() -> TestResult {
    let fixture = Fixture::create("admin_promote_twice").await?;
    let pool = fixture.pool().await?;

    let first_load = fixture.publish_one_load(&pool, fixture.revision).await?;
    let first = Promotion::bootstrap(fixture.revision, first_load);
    assert!(fixture.promote(&first)?.status.success());

    let next = fixture.seed_next_revision(&pool).await?;
    let second_load = fixture.publish_one_load(&pool, next).await?;
    assert_ne!(second_load, first_load, "a new revision is a new load");
    let second = first.after(next, second_load);
    let output = fixture.promote(&second)?;
    assert!(
        output.status.success(),
        "second promote failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert_eq!(
        fixture.pointer(&pool).await?,
        Some((second.manifest, 2)),
        "the pointer moves to the second manifest at generation 2"
    );
    let serving_generation: i64 = sqlx::query_scalar(
        "SELECT serving_generation FROM catalog.vector_tile_publication_unit WHERE unit_key = $1",
    )
    .bind(ADMINISTRATIVE_UNIT_KEY)
    .fetch_one(&pool)
    .await?;
    assert_eq!(serving_generation, 2);

    fixture.finish(pool).await
}

/// A promotion that expects a pointer other than the one in place must not move it.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn a_promotion_expecting_a_stale_pointer_is_refused() -> TestResult {
    let fixture = Fixture::create("admin_promote_stale").await?;
    let pool = fixture.pool().await?;

    let load_id = fixture.publish_one_load(&pool, fixture.revision).await?;
    let first = Promotion::bootstrap(fixture.revision, load_id);
    assert!(fixture.promote(&first)?.status.success());

    // Bootstrap again: it expects no pointer, but one is in place now.
    let stale = Promotion::bootstrap(
        fixture.revision,
        fixture.publish_one_load(&pool, fixture.revision).await?,
    );
    let output = fixture.promote(&stale)?;

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("compare-and-swap precondition failed"),
        "unexpected failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fixture.pointer(&pool).await?,
        Some((first.manifest, 1)),
        "a refused promotion leaves the pointer where it was"
    );

    fixture.finish(pool).await
}

/// Reusing a release id for a different load is refused, and only the read-back can refuse it.
///
/// The release insert is `ON CONFLICT (id) DO NOTHING`, so a second promotion under an existing
/// release id would keep whatever that row already said while every input was checked against the
/// environment instead. The promotion gate cannot see it either: an earlier succeeded load for the
/// same unit, revision and snapshot satisfies every condition it has. The pointer would move while
/// the serving view kept reading the first load's rows — the exact class of silent staleness the
/// load ledger exists to prevent.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn reusing_a_release_id_for_a_different_load_is_refused() -> TestResult {
    let fixture = Fixture::create("admin_promote_reuse").await?;
    let pool = fixture.pool().await?;

    let first_load = fixture.publish_one_load(&pool, fixture.revision).await?;
    let first = Promotion::bootstrap(fixture.revision, first_load);
    assert!(fixture.promote(&first)?.status.success());

    let second_load = fixture.publish_one_load(&pool, fixture.revision).await?;
    let reused = Promotion {
        revision: fixture.revision,
        projection_load: second_load,
        release: first.release,
        manifest: Uuid::new_v4(),
        expected_manifest: Some(first.manifest),
    };
    let output = fixture.promote(&reused)?;

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("needs a new release id"),
        "unexpected failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fixture.pointer(&pool).await?,
        Some((first.manifest, 1)),
        "the pointer must not move onto a release describing another load"
    );
    let served_load: Option<Uuid> = sqlx::query_scalar(
        "SELECT postgis_projection_revision FROM catalog.vector_tile_release WHERE id = $1",
    )
    .bind(first.release)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        served_load,
        Some(first_load),
        "the existing release still names the load it was created for"
    );

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

/// One operator promotion: which load to serve, under which release and manifest identity.
struct Promotion {
    revision: Revision,
    projection_load: Uuid,
    release: Uuid,
    manifest: Uuid,
    /// The pointer this promotion expects to replace. `None` is the bootstrap case, not "any".
    expected_manifest: Option<Uuid>,
}

impl Promotion {
    fn bootstrap(revision: Revision, projection_load: Uuid) -> Self {
        Self {
            revision,
            projection_load,
            release: Uuid::new_v4(),
            manifest: Uuid::new_v4(),
            expected_manifest: None,
        }
    }

    fn after(&self, revision: Revision, projection_load: Uuid) -> Self {
        Self {
            revision,
            projection_load,
            release: Uuid::new_v4(),
            manifest: Uuid::new_v4(),
            expected_manifest: Some(self.manifest),
        }
    }
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
    revision: Revision,
    source_record_id: Uuid,
    source_file_asset_id: Uuid,
}

/// One administrative boundary fact revision: its id and the canonical snapshot it was built from.
///
/// Carried as a pair because everything downstream reads them together. The publish verifies the
/// ledger row against both, the load records both, and the promotion gate refuses a manifest that
/// would move a dynamic unit to an older snapshot — so a fixture that varied one without the other
/// would be describing a state the system does not admit.
#[derive(Clone, Copy)]
struct Revision {
    id: Uuid,
    canonical_snapshot: &'static str,
    /// The legal effective date its facts take. A later revision must carry a later one.
    valid_from: &'static str,
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
            revision: Revision {
                id: Uuid::new_v4(),
                canonical_snapshot: CANONICAL_SNAPSHOT_ID,
                valid_from: VALID_FROM,
            },
            source_record_id: Uuid::new_v4(),
            source_file_asset_id: Uuid::new_v4(),
        };
        fixture.write_source(fixture.revision)?;
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
        self.seed_revision(pool, self.revision).await?;
        // Promotion lineage. `verify_inputs` requires the release's file asset to exist before the
        // pointer may move, so the fixture carries the same evidence an operator would.
        sqlx::query(
            "INSERT INTO catalog.file_asset
                (id, object_key, mime_type, size_bytes, checksum_sha256, title,
                 source_record_id, visibility, version)
             VALUES ($1, $2, 'application/geo+json', 1, repeat('e', 64),
                     'Administrative boundary publication fixture', $3, 'internal', 1)",
        )
        .bind(self.source_file_asset_id)
        .bind(OBJECT_KEY)
        .bind(self.source_record_id)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Registers one administrative boundary fact revision the publish can verify itself against.
    async fn seed_revision(&self, pool: &PgPool, revision: Revision) -> TestResult {
        sqlx::query(
            "INSERT INTO catalog.administrative_boundary_revision
                (id, canonical_iceberg_snapshot_id, source_snapshot_id, source_record_id, status)
             VALUES ($1, $2, $3, $4, 'candidate')",
        )
        .bind(revision.id)
        .bind(revision.canonical_snapshot)
        .bind(SOURCE_SNAPSHOT_ID)
        .bind(self.source_record_id)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// The revision a corrected collection would produce: a new id at a newer canonical snapshot.
    async fn seed_next_revision(&self, pool: &PgPool) -> TestResult<Revision> {
        let revision = Revision {
            id: Uuid::new_v4(),
            canonical_snapshot: NEXT_CANONICAL_SNAPSHOT_ID,
            valid_from: NEXT_VALID_FROM,
        };
        self.seed_revision(pool, revision).await?;
        self.write_source(revision)?;
        Ok(revision)
    }

    /// One row per level, because `catalog.validate_administrative_unit_parent` requires a
    /// `legal_dong` to hang off a `sigungu` and a `sigungu` off a `sido`. A two-level fixture is
    /// rejected before any geometry lands, so the shallower shape could not exercise the load at all.
    fn write_source(&self, revision: Revision) -> TestResult {
        let valid_from = revision.valid_from;
        let rows = [
            source_row("sido", "99", "검증용 광역", "", "", SIDO_SQUARE, valid_from),
            source_row(
                "sigungu",
                "99999",
                "검증용 시군구",
                "sido",
                "99",
                SIGUNGU_SQUARE,
                valid_from,
            ),
            source_row(
                "legal_dong",
                "9999900101",
                "검증용 법정동",
                "sigungu",
                "99999",
                LEGAL_DONG_SQUARE,
                valid_from,
            ),
        ];
        let mut body = String::new();
        for row in rows {
            body.push_str(&serde_json::to_string(&row)?);
            body.push('\n');
        }
        fs::write(self.source_path(revision), body)?;
        Ok(())
    }

    fn write_registry_evidence(&self) -> TestResult {
        fs::write(
            self.registry_evidence_path(),
            serde_json::to_vec(&json!({"status": "ready"}))?,
        )?;
        Ok(())
    }

    /// One file per revision: each carries its own effective date, so they cannot share a snapshot.
    fn source_path(&self, revision: Revision) -> PathBuf {
        self.root
            .join(format!("source-{}.jsonl", revision.canonical_snapshot))
    }

    fn registry_evidence_path(&self) -> PathBuf {
        self.root.join("registry-evidence.json")
    }

    fn publish(&self, revision: Revision) -> TestResult<std::process::Output> {
        Command::new(BINARY)
            .arg("publish-administrative-boundary-postgis")
            .env("DATABASE_URL", self.database.url())
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_CONFIRM",
                "1",
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_PATH",
                self.source_path(revision),
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_REGISTRY_EVIDENCE_PATH",
                self.registry_evidence_path(),
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_DATA_REVISION",
                revision.id.to_string(),
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_CANONICAL_ICEBERG_SNAPSHOT_ID",
                revision.canonical_snapshot,
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

    /// Runs `promote-administrative-boundary-runtime` for one load, under one release and manifest.
    fn promote(&self, promotion: &Promotion) -> TestResult<std::process::Output> {
        let mut command = Command::new(BINARY);
        command
            .arg("promote-administrative-boundary-runtime")
            .env("DATABASE_URL", self.database.url())
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_CONFIRM",
                "1",
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_PROJECTION_LOAD_ID",
                promotion.projection_load.to_string(),
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_DATA_REVISION",
                promotion.revision.id.to_string(),
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_CANONICAL_ICEBERG_SNAPSHOT_ID",
                promotion.revision.canonical_snapshot,
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_SOURCE_RECORD_ID",
                self.source_record_id.to_string(),
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_SOURCE_FILE_ASSET_ID",
                self.source_file_asset_id.to_string(),
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_RELEASE_ID",
                promotion.release.to_string(),
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_MANIFEST_ID",
                promotion.manifest.to_string(),
            )
            .env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_TILES_URL_TEMPLATE",
                TILES_URL_TEMPLATE,
            );
        // Absent rather than empty when there is no pointer yet. The command reads this one as
        // optional and compares `Option` to `Option`, so a bootstrap promotion is expressed by the
        // variable not being there — an empty string would be a parse failure, not a `None`.
        if let Some(expected) = promotion.expected_manifest {
            command.env(
                "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_EXPECTED_MANIFEST_ID",
                expected.to_string(),
            );
        }
        command.output().map_err(Into::into)
    }

    /// Publishes once and returns the load that run committed, which is what a promotion names.
    async fn publish_one_load(&self, pool: &PgPool, revision: Revision) -> TestResult<Uuid> {
        let output = self.publish(revision)?;
        assert!(
            output.status.success(),
            "publish failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let loads = self.loads(pool, revision).await?;
        Ok(loads
            .last()
            .ok_or_else(|| std::io::Error::other("publish committed no projection load"))?
            .id)
    }

    /// The runtime pointer and the generation of the manifest it selects, if one has been promoted.
    async fn pointer(&self, pool: &PgPool) -> TestResult<Option<(Uuid, i64)>> {
        let row = sqlx::query(
            "SELECT pointer.manifest_id, manifest.manifest_generation
               FROM catalog.vector_tile_runtime_manifest_pointer AS pointer
               JOIN catalog.vector_tile_runtime_manifest AS manifest
                 ON manifest.id = pointer.manifest_id
              WHERE pointer.singleton = true",
        )
        .fetch_optional(pool)
        .await?;
        match row {
            Some(row) => Ok(Some((
                row.try_get("manifest_id")?,
                row.try_get("manifest_generation")?,
            ))),
            None => Ok(None),
        }
    }

    /// Every load for this fixture's revision, oldest first.
    async fn loads(&self, pool: &PgPool, revision: Revision) -> TestResult<Vec<ProjectionLoad>> {
        let rows = sqlx::query(
            "SELECT load.id, load.status, load.loaded_row_count, load.rejected_row_count
               FROM serving_postgis.spatial_projection_load AS load
               JOIN catalog.vector_tile_publication_unit AS unit
                 ON unit.id = load.publication_unit_id
              WHERE load.data_revision = $1 AND unit.unit_key = $2
              ORDER BY load.started_at",
        )
        .bind(revision.id)
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
    valid_from: &str,
) -> Value {
    let geometry = square(corners);
    let geometry_sha256 = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&geometry).expect("geometry must serialise"))
    );
    json!({
        "scope_kind": scope_kind,
        "canonical_code": canonical_code,
        "valid_from_utc": valid_from,
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
