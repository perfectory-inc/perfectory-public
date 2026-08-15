//! Process-level contract for publishing one sealed parcel source into `PostGIS`.
//!
//! Every test runs the built `foundation-outbox-publisher` binary. Fixtures deliberately inject
//! forbidden states into a disposable database when `PostgreSQL` normally prevents them, so each
//! publisher-side rejection proves an independent defence rather than re-testing only the schema.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::process::{Command, Output};

use foundation_disposable_database::{disposable_database_url, DisposableDatabaseUrl, TestResult};
use serde_json::{json, Value as JsonValue};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use uuid::Uuid;

const BINARY: &str = env!("CARGO_BIN_EXE_foundation-outbox-publisher");
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

const PARCEL_UNIT_KEY: &str = "parcels";
const CANONICAL_SNAPSHOT_ID: i64 = 841_361_364_657_368_626;
const MIRROR_SNAPSHOT_ID: &str = "iceberg:841361364657368626";
const EXECUTION_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_publication_execution_evidence.v1";
const QUALITY_SCHEMA_VERSION: &str = "foundation-platform.parcel_publication_quality.v1";
const CONTENT_DIGEST_PREFIX: &[u8] = b"perfectory.parcel-projection-content.v1\0";
const ICEBERG_TABLE_UUID: &str = "2f7bf2d1-3e08-4d1a-936e-556d8ebfd055";

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn one_sealed_evidence_opens_one_complete_load() -> TestResult {
    let fixture = Fixture::create("parcel_publish_once").await?;
    let pool = fixture.pool().await?;

    let output = fixture.publish()?;
    assert_success(&output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("parcel-boundary-postgis-publish-ok"));
    assert!(stdout.contains(&format!("source_evidence_id={}", fixture.evidence_id)));
    assert!(stdout.contains(&format!(
        "canonical_iceberg_snapshot_id={CANONICAL_SNAPSHOT_ID}"
    )));
    assert!(stdout.contains("source_rows=3 loaded_rows=3 rejected_rows=0"));

    let loads = fixture.loads(&pool).await?;
    assert_eq!(loads.len(), 1);
    let load = &loads[0];
    assert_eq!(load.status, "succeeded");
    assert_eq!(load.loaded_row_count, Fixture::MIRROR_ROWS);
    assert_eq!(load.rejected_row_count, 0);
    assert!(load.finished);
    assert_eq!(load.error_message, None);
    assert_eq!(load.source_evidence_id, Some(fixture.evidence_id));
    assert_eq!(
        load.canonical_snapshot_id,
        CANONICAL_SNAPSHOT_ID.to_string()
    );

    let revision = sqlx::query(
        "SELECT revision.publication_unit_id, revision.canonical_iceberg_snapshot_id,
                revision.source_record_id, revision.derived_from_administrative_revision
           FROM catalog.publication_revision AS revision
          WHERE revision.id = $1",
    )
    .bind(load.data_revision)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        revision.try_get::<String, _>("canonical_iceberg_snapshot_id")?,
        CANONICAL_SNAPSHOT_ID.to_string()
    );
    assert_eq!(
        revision.try_get::<Uuid, _>("source_record_id")?,
        fixture.source.source_record_id
    );
    assert_eq!(
        revision.try_get::<Option<Uuid>, _>("derived_from_administrative_revision")?,
        None
    );

    assert_eq!(
        fixture.publication_rows(&pool, load.id).await?,
        Fixture::MIRROR_ROWS
    );
    assert_eq!(
        fixture.target_contract_violations(&pool, load).await?,
        0,
        "every target row must retain the sealed lineage and copied row payload"
    );
    assert_eq!(
        target_content_digest(&pool, load.id).await?,
        fixture.evidence_digest(&pool).await?
    );

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn republishing_one_evidence_reuses_revision_and_appends_a_fresh_load() -> TestResult {
    let fixture = Fixture::create("parcel_publish_twice").await?;
    let pool = fixture.pool().await?;

    assert_success(&fixture.publish()?);
    assert_success(&fixture.publish()?);

    let loads = fixture.loads(&pool).await?;
    assert_eq!(loads.len(), 2);
    assert_ne!(loads[0].id, loads[1].id);
    assert_eq!(
        loads[0].data_revision, loads[1].data_revision,
        "one Iceberg snapshot has one revision and each materialisation has a fresh load"
    );
    for load in &loads {
        assert_eq!(load.status, "succeeded");
        assert_eq!(load.loaded_row_count, Fixture::MIRROR_ROWS);
        assert_eq!(load.source_evidence_id, Some(fixture.evidence_id));
        assert_eq!(
            target_content_digest(&pool, load.id).await?,
            fixture.evidence_digest(&pool).await?
        );
    }

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn nonzero_rejected_rows_are_refused_before_a_load_is_opened() -> TestResult {
    let fixture = Fixture::create_with("parcel_publish_rejected", SeedMode::RejectedRows).await?;
    let pool = fixture.pool().await?;

    let output = fixture.publish()?;
    assert_rejected(&output, "rejected_row_count=1", "must be zero");
    assert_eq!(fixture.loads(&pool).await?.len(), 0);

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn incomplete_quality_report_is_not_treated_as_zero_defects() -> TestResult {
    let fixture =
        Fixture::create_with("parcel_publish_quality", SeedMode::IncompleteQuality).await?;
    let pool = fixture.pool().await?;

    let output = fixture.publish()?;
    assert_rejected(
        &output,
        "complete parcel publication quality report",
        "schema_version",
    );
    assert_eq!(fixture.loads(&pool).await?.len(), 0);

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn same_count_with_changed_target_geometry_is_rolled_back() -> TestResult {
    let fixture = Fixture::create("parcel_publish_content_tamper").await?;
    let pool = fixture.pool().await?;
    fixture.install_target_content_tamper(&pool).await?;

    let output = fixture.publish()?;
    assert_rejected(&output, "target content digest", "was closed as failed");
    let loads = fixture.loads(&pool).await?;
    assert_eq!(loads.len(), 1);
    assert_eq!(loads[0].status, "failed");
    assert_eq!(loads[0].loaded_row_count, 0);
    assert_eq!(fixture.publication_rows(&pool, loads[0].id).await?, 0);

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn an_insert_failure_rolls_back_every_target_row_and_closes_the_load_failed() -> TestResult {
    let fixture = Fixture::create("parcel_publish_insert_failure").await?;
    let pool = fixture.pool().await?;
    fixture.install_second_row_failure(&pool).await?;

    let output = fixture.publish()?;
    assert_rejected(
        &output,
        "injected second-row materialisation failure",
        "was closed as failed",
    );
    let loads = fixture.loads(&pool).await?;
    assert_eq!(loads.len(), 1);
    assert_eq!(loads[0].status, "failed");
    assert_eq!(loads[0].loaded_row_count, 0);
    assert_eq!(fixture.publication_rows(&pool, loads[0].id).await?, 0);

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn an_existing_revision_with_administrative_lineage_is_refused() -> TestResult {
    let fixture = Fixture::create("parcel_publish_lineage").await?;
    let pool = fixture.pool().await?;
    let conflicting_revision = fixture.seed_revision_with_lineage(&pool).await?;

    let output = fixture.publish()?;
    assert_rejected(
        &output,
        "has administrative lineage",
        &conflicting_revision.to_string(),
    );
    assert_eq!(fixture.loads(&pool).await?.len(), 0);

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn two_evidence_rows_cannot_be_mixed_through_one_snapshot_revision() -> TestResult {
    let fixture = Fixture::create("parcel_publish_two_evidence").await?;
    let pool = fixture.pool().await?;
    assert_success(&fixture.publish()?);

    let second = SourceSet::new();
    fixture
        .seed_source_set(&pool, &second, complete_quality_report(), 0)
        .await?;
    let second_evidence_id = fixture
        .seed_evidence(&pool, &second, EXECUTION_SCHEMA_VERSION, 0)
        .await?;

    let output = fixture.publish_with_evidence(second_evidence_id)?;
    assert_rejected(
        &output,
        "already describes a different sealed publication",
        &second.source_record_id.to_string(),
    );
    assert_eq!(
        fixture.loads(&pool).await?.len(),
        1,
        "the conflicting evidence must not open another load"
    );

    fixture.finish(pool).await
}

#[derive(Clone, Copy)]
enum SeedMode {
    Publishable,
    RejectedRows,
    IncompleteQuality,
}

// The shared `_id` postfix is the column name each field is bound to; renaming it here would
// make the INSERT statements read against their own schema.
#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy)]
struct SourceSet {
    run_id: Uuid,
    source_record_id: Uuid,
    source_file_asset_id: Uuid,
}

impl SourceSet {
    fn new() -> Self {
        Self {
            run_id: Uuid::new_v4(),
            source_record_id: Uuid::new_v4(),
            source_file_asset_id: Uuid::new_v4(),
        }
    }
}

struct ProjectionLoad {
    id: Uuid,
    data_revision: Uuid,
    canonical_snapshot_id: String,
    source_evidence_id: Option<Uuid>,
    status: String,
    loaded_row_count: i64,
    rejected_row_count: i64,
    finished: bool,
    error_message: Option<String>,
}

struct Fixture {
    database: DisposableDatabaseUrl,
    source: SourceSet,
    evidence_id: Uuid,
}

impl Fixture {
    const MIRROR_ROWS: i64 = 3;

    async fn create(label: &str) -> TestResult<Self> {
        Self::create_with(label, SeedMode::Publishable).await
    }

    async fn create_with(label: &str, mode: SeedMode) -> TestResult<Self> {
        let fixture = Self {
            database: disposable_database_url(label).await?,
            source: SourceSet::new(),
            evidence_id: Uuid::new_v4(),
        };
        let pool = fixture.pool().await?;
        MIGRATOR.run(&pool).await?;

        let rejected_row_count = i64::from(matches!(mode, SeedMode::RejectedRows));
        let quality_report = if matches!(mode, SeedMode::IncompleteQuality) {
            json!({})
        } else {
            complete_quality_report()
        };
        fixture
            .seed_source_set(&pool, &fixture.source, quality_report, rejected_row_count)
            .await?;

        if matches!(mode, SeedMode::RejectedRows) {
            sqlx::query(
                "ALTER TABLE catalog.parcel_publication_source_evidence
                 DROP CONSTRAINT parcel_publication_source_evidence_rejected_count_check",
            )
            .execute(&pool)
            .await?;
        }
        if matches!(mode, SeedMode::IncompleteQuality) {
            sqlx::query(
                "ALTER TABLE catalog.parcel_publication_source_evidence
                 DISABLE TRIGGER parcel_publication_source_evidence_validate",
            )
            .execute(&pool)
            .await?;
        }

        let evidence_id = fixture
            .seed_evidence_with_id(
                &pool,
                fixture.evidence_id,
                &fixture.source,
                EXECUTION_SCHEMA_VERSION,
                rejected_row_count,
            )
            .await?;
        assert_eq!(evidence_id, fixture.evidence_id);

        if matches!(mode, SeedMode::IncompleteQuality) {
            sqlx::query(
                "ALTER TABLE catalog.parcel_publication_source_evidence
                 ENABLE TRIGGER parcel_publication_source_evidence_validate",
            )
            .execute(&pool)
            .await?;
        }
        pool.close().await;
        Ok(fixture)
    }

    async fn pool(&self) -> TestResult<PgPool> {
        self.database.pool().await
    }

    async fn finish(self, pool: PgPool) -> TestResult {
        pool.close().await;
        self.database.drop_database().await
    }

    async fn seed_source_set(
        &self,
        pool: &PgPool,
        source: &SourceSet,
        quality_report: JsonValue,
        rejected_row_count: i64,
    ) -> TestResult {
        sqlx::query(
            "INSERT INTO catalog.source_record
                (id, source, external_id, checksum_sha256, raw_object_key)
             VALUES ($1, 'parcel-boundary-publish-test', $2, repeat('e', 64), $3)",
        )
        .bind(source.source_record_id)
        .bind(format!("publish-test-{}", source.source_record_id))
        .bind(format!(
            "silver/parcel-boundaries/{}/metadata.json",
            source.source_record_id
        ))
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.file_asset
                (id, object_key, mime_type, size_bytes, checksum_sha256,
                 source_record_id, visibility)
             VALUES ($1, $2, 'application/json', 1, repeat('f', 64), $3, 'internal')",
        )
        .bind(source.source_file_asset_id)
        .bind(format!(
            "silver/parcel-boundaries/{}/manifest.json",
            source.source_file_asset_id
        ))
        .bind(source.source_record_id)
        .execute(pool)
        .await?;

        sqlx::query(
            r#"INSERT INTO serving_postgis.parcel_boundary_mirror_rebuild_run
                (id, source_snapshot_id, source_table, source_record_id, source_file_asset_id,
                 srid, status, loaded_row_count, rejected_row_count, quality_report,
                 publication_scope, publication_limits, started_at)
             VALUES ($1, $2, 'silver.parcel_boundaries', $3, $4, 5179, 'planned', 0, 0,
                     $5, '{"kind":"national","complete":true}'::jsonb,
                     '{"object_limit":null,"row_limit":null,"shard_limit":null}'::jsonb,
                     now())"#,
        )
        .bind(source.run_id)
        .bind(MIRROR_SNAPSHOT_ID)
        .bind(source.source_record_id)
        .bind(source.source_file_asset_id)
        .bind(quality_report)
        .execute(pool)
        .await?;

        sqlx::query(
            "UPDATE serving_postgis.parcel_boundary_mirror_rebuild_run
                SET status = 'running', rejected_row_count = $2,
                    updated_at = now(), version = version + 1
              WHERE id = $1 AND status = 'planned'",
        )
        .bind(source.run_id)
        .bind(rejected_row_count)
        .execute(pool)
        .await?;

        for (index, corners) in SQUARES.iter().enumerate() {
            let (west, south, east, north) = *corners;
            let pnu = format!("9999900101100010{:03}", index + 1);
            sqlx::query(
                "INSERT INTO serving_postgis.parcel_boundary_mirror
                    (pnu, rebuild_run_id, source_snapshot_id, source_table,
                     source_record_id, source_file_asset_id, source_object_key,
                     source_row_id, geometry_checksum_sha256, properties, geom)
                 VALUES ($1, $2, $3, 'silver.parcel_boundaries', $4, $5, $6, $1, $7,
                         jsonb_build_object('boundary_id', $1),
                         public.st_multi(public.st_transform(
                             public.st_setsrid(public.st_geomfromtext($8), 4326), 5179)))",
            )
            .bind(&pnu)
            .bind(source.run_id)
            .bind(MIRROR_SNAPSHOT_ID)
            .bind(source.source_record_id)
            .bind(source.source_file_asset_id)
            .bind(format!(
                "silver/parcel-boundaries/{}/part-0001.parquet",
                source.run_id
            ))
            .bind(format!("{index:064x}"))
            .bind(format!(
                "POLYGON(({west} {south},{east} {south},{east} {north},{west} {north},{west} {south}))"
            ))
            .execute(pool)
            .await?;
        }

        sqlx::query(
            "UPDATE serving_postgis.parcel_boundary_mirror_rebuild_run
                SET status = 'succeeded', loaded_row_count = $2, finished_at = now(),
                    updated_at = now(), version = version + 1
              WHERE id = $1 AND status = 'running'",
        )
        .bind(source.run_id)
        .bind(Self::MIRROR_ROWS)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn seed_evidence(
        &self,
        pool: &PgPool,
        source: &SourceSet,
        execution_schema: &str,
        rejected_row_count: i64,
    ) -> TestResult<Uuid> {
        self.seed_evidence_with_id(
            pool,
            Uuid::new_v4(),
            source,
            execution_schema,
            rejected_row_count,
        )
        .await
    }

    async fn seed_evidence_with_id(
        &self,
        pool: &PgPool,
        evidence_id: Uuid,
        source: &SourceSet,
        execution_schema: &str,
        rejected_row_count: i64,
    ) -> TestResult<Uuid> {
        let digest = source_content_digest(pool, source.run_id).await?;
        let execution_sha256 = execution_evidence_sha256(source);
        let mut transaction = pool.begin().await?;
        sqlx::query(
            "SELECT set_config('foundation.parcel_publication_evidence_sealer', 'on', true)",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.parcel_publication_source_evidence
                (id, mirror_rebuild_run_id, mirror_rebuild_run_status,
                 mirror_rebuild_rejected_row_count, iceberg_table_uuid,
                 iceberg_logical_table, iceberg_snapshot_id, source_record_id,
                 source_file_asset_id, execution_evidence_schema_version,
                 execution_evidence_object_key, execution_evidence_sha256,
                 source_row_count, projection_content_sha256, quality_schema_version)
             VALUES ($1, $2, 'succeeded', $3, $4, 'silver.parcel_boundaries', $5, $6, $7,
                     $8, $9, $10, $11, $12, $13)",
        )
        .bind(evidence_id)
        .bind(source.run_id)
        .bind(rejected_row_count)
        .bind(Uuid::parse_str(ICEBERG_TABLE_UUID)?)
        .bind(CANONICAL_SNAPSHOT_ID)
        .bind(source.source_record_id)
        .bind(source.source_file_asset_id)
        .bind(execution_schema)
        .bind(format!("evidence/parcel-publication/{evidence_id}.json"))
        .bind(execution_sha256)
        .bind(Self::MIRROR_ROWS)
        .bind(digest)
        .bind(QUALITY_SCHEMA_VERSION)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(evidence_id)
    }

    async fn seed_revision_with_lineage(&self, pool: &PgPool) -> TestResult<Uuid> {
        let administrative_revision_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO catalog.administrative_boundary_revision
                (id, canonical_iceberg_snapshot_id, source_snapshot_id, source_record_id, status)
             VALUES ($1, $2, 'iceberg:administrative-lineage-test', $3, 'candidate')",
        )
        .bind(administrative_revision_id)
        .bind(CANONICAL_SNAPSHOT_ID.to_string())
        .bind(self.source.source_record_id)
        .execute(pool)
        .await?;

        let revision_id = Uuid::new_v4();
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
                (id, publication_unit_id, canonical_iceberg_snapshot_id, source_record_id,
                 derived_from_administrative_revision)
             SELECT $1, unit.id, $2, $3, $4
               FROM catalog.vector_tile_publication_unit AS unit
              WHERE unit.unit_key = $5",
        )
        .bind(revision_id)
        .bind(CANONICAL_SNAPSHOT_ID.to_string())
        .bind(self.source.source_record_id)
        .bind(administrative_revision_id)
        .bind(PARCEL_UNIT_KEY)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(revision_id)
    }

    async fn install_target_content_tamper(&self, pool: &PgPool) -> TestResult {
        sqlx::query(
            "CREATE FUNCTION serving_postgis.test_shift_parcel_publication_geometry()
             RETURNS trigger LANGUAGE plpgsql AS $function$
             BEGIN
                 NEW.geom := public.st_translate(NEW.geom, 1.0, 0.0);
                 RETURN NEW;
             END
             $function$",
        )
        .execute(pool)
        .await?;
        sqlx::query(
            "CREATE TRIGGER test_shift_parcel_publication_geometry
             BEFORE INSERT ON serving_postgis.parcel_boundary_publication
             FOR EACH ROW EXECUTE FUNCTION serving_postgis.test_shift_parcel_publication_geometry()",
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn install_second_row_failure(&self, pool: &PgPool) -> TestResult {
        sqlx::query(
            "CREATE FUNCTION serving_postgis.test_reject_second_parcel_publication_row()
             RETURNS trigger LANGUAGE plpgsql AS $function$
             BEGIN
                 IF NEW.pnu = '9999900101100010002' THEN
                     RAISE EXCEPTION 'injected second-row materialisation failure';
                 END IF;
                 RETURN NEW;
             END
             $function$",
        )
        .execute(pool)
        .await?;
        sqlx::query(
            "CREATE TRIGGER test_reject_second_parcel_publication_row
             BEFORE INSERT ON serving_postgis.parcel_boundary_publication
             FOR EACH ROW EXECUTE FUNCTION serving_postgis.test_reject_second_parcel_publication_row()",
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    fn publish(&self) -> TestResult<Output> {
        self.publish_with_evidence(self.evidence_id)
    }

    fn publish_with_evidence(&self, evidence_id: Uuid) -> TestResult<Output> {
        Command::new(BINARY)
            .arg("publish-parcel-boundary-postgis")
            .env("DATABASE_URL", self.database.url())
            .env(
                "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_POSTGIS_PUBLISH_CONFIRM",
                "1",
            )
            .env(
                "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_POSTGIS_PUBLISH_SOURCE_EVIDENCE_ID",
                evidence_id.to_string(),
            )
            .output()
            .map_err(Into::into)
    }

    async fn loads(&self, pool: &PgPool) -> TestResult<Vec<ProjectionLoad>> {
        let rows = sqlx::query(
            "SELECT load.id, load.data_revision, load.canonical_iceberg_snapshot_id,
                    load.source_evidence_id, load.status, load.loaded_row_count,
                    load.rejected_row_count, load.finished_at IS NOT NULL AS finished,
                    load.error_message
               FROM serving_postgis.spatial_projection_load AS load
               JOIN catalog.vector_tile_publication_unit AS unit
                 ON unit.id = load.publication_unit_id
              WHERE unit.unit_key = $1
              ORDER BY load.started_at, load.id",
        )
        .bind(PARCEL_UNIT_KEY)
        .fetch_all(pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ProjectionLoad {
                    id: row.try_get("id")?,
                    data_revision: row.try_get("data_revision")?,
                    canonical_snapshot_id: row.try_get("canonical_iceberg_snapshot_id")?,
                    source_evidence_id: row.try_get("source_evidence_id")?,
                    status: row.try_get("status")?,
                    loaded_row_count: row.try_get("loaded_row_count")?,
                    rejected_row_count: row.try_get("rejected_row_count")?,
                    finished: row.try_get("finished")?,
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

    async fn target_contract_violations(
        &self,
        pool: &PgPool,
        load: &ProjectionLoad,
    ) -> TestResult<i64> {
        Ok(sqlx::query_scalar(
            "SELECT count(*)
               FROM serving_postgis.parcel_boundary_publication AS target
               LEFT JOIN serving_postgis.parcel_boundary_mirror AS source
                 ON source.rebuild_run_id = $5 AND source.pnu = target.pnu
              WHERE target.projection_load_id = $1
                AND (target.data_revision IS DISTINCT FROM $2
                  OR target.canonical_iceberg_snapshot_id IS DISTINCT FROM $3
                  OR target.source_record_id IS DISTINCT FROM $4
                  OR target.parcel_id IS NOT NULL
                  OR source.pnu IS NULL
                  OR target.source_object_key IS DISTINCT FROM source.source_object_key
                  OR target.geometry_checksum_sha256 IS DISTINCT FROM source.geometry_checksum_sha256
                  OR target.properties IS DISTINCT FROM source.properties
                  OR public.st_srid(target.geom) <> 5179
                  OR NOT public.st_isvalid(target.geom)
                  OR public.st_isempty(target.geom)
                  OR public.st_area(target.geom) <= 0)",
        )
        .bind(load.id)
        .bind(load.data_revision)
        .bind(&load.canonical_snapshot_id)
        .bind(self.source.source_record_id)
        .bind(self.source.run_id)
        .fetch_one(pool)
        .await?)
    }

    async fn evidence_digest(&self, pool: &PgPool) -> TestResult<String> {
        Ok(sqlx::query_scalar(
            "SELECT projection_content_sha256::text
               FROM catalog.parcel_publication_source_evidence WHERE id = $1",
        )
        .bind(self.evidence_id)
        .fetch_one(pool)
        .await?)
    }
}

fn execution_evidence_sha256(source: &SourceSet) -> String {
    let bytes = serde_json::to_vec(&json!({
        "schema_version": EXECUTION_SCHEMA_VERSION,
        "mirror_rebuild_run_id": source.run_id,
        "source_record_id": source.source_record_id,
        "source_file_asset_id": source.source_file_asset_id,
        "iceberg_table_uuid": ICEBERG_TABLE_UUID,
        "iceberg_snapshot_id": CANONICAL_SNAPSHOT_ID,
    }))
    .expect("fixture evidence serializes");
    format!("{:x}", Sha256::digest(bytes))
}

fn complete_quality_report() -> JsonValue {
    json!({
        "schema_version": QUALITY_SCHEMA_VERSION,
        "object_count": 1,
        "expected_row_count": Fixture::MIRROR_ROWS,
        "loaded_row_count": Fixture::MIRROR_ROWS,
        "invalid_srid_count": 0,
        "invalid_geometry_count": 0,
        "empty_geometry_count": 0,
        "nonpositive_area_count": 0,
        "source_srid": "EPSG:4326",
        "target_srid": "EPSG:5179",
        "geometry_repair_strategy": "postgis-make-valid-v1"
    })
}

async fn source_content_digest(pool: &PgPool, run_id: Uuid) -> TestResult<String> {
    let rows = sqlx::query(
        "SELECT pnu::text AS pnu, public.st_asewkb(geom, 'NDR') AS ewkb
           FROM serving_postgis.parcel_boundary_mirror
          WHERE rebuild_run_id = $1
          ORDER BY pnu COLLATE \"C\"",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;
    projection_content_digest(rows)
}

async fn target_content_digest(pool: &PgPool, load_id: Uuid) -> TestResult<String> {
    let rows = sqlx::query(
        "SELECT pnu::text AS pnu, public.st_asewkb(geom, 'NDR') AS ewkb
           FROM serving_postgis.parcel_boundary_publication
          WHERE projection_load_id = $1
          ORDER BY pnu COLLATE \"C\"",
    )
    .bind(load_id)
    .fetch_all(pool)
    .await?;
    projection_content_digest(rows)
}

fn projection_content_digest(rows: Vec<sqlx::postgres::PgRow>) -> TestResult<String> {
    let mut digest = Sha256::new();
    digest.update(CONTENT_DIGEST_PREFIX);
    for row in rows {
        let pnu: String = row.try_get("pnu")?;
        assert_eq!(pnu.len(), 19);
        assert!(pnu.bytes().all(|byte| byte.is_ascii_digit()));
        let ewkb: Vec<u8> = row.try_get("ewkb")?;
        digest.update(pnu.as_bytes());
        digest.update([0]);
        digest.update(Sha256::digest(ewkb));
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "publish failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_rejected(output: &Output, first: &str, second: &str) {
    assert!(
        !output.status.success(),
        "violating publish unexpectedly succeeded"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(first) && stderr.contains(second),
        "rejection did not name the violated invariant: {stderr}"
    );
}

const SQUARES: [(f64, f64, f64, f64); 3] = [
    (127.1231, 36.1231, 127.1232, 36.1232),
    (127.1233, 36.1233, 127.1234, 36.1234),
    (127.1235, 36.1235, 127.1236, 36.1236),
];
