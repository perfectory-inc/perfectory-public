//! Database invariants for the sealed parcel-publication source path.
//!
//! These tests inject the forbidden writes themselves. A green assertion therefore means
//! PostgreSQL returned the named rejection while rows were present; it never means that an empty
//! query happened to find no violation.
//! The schema under test is appended by `20260811000001_parcel_publication_source_evidence.sql`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use foundation_disposable_database::{disposable_database_url, DisposableDatabaseUrl, TestResult};
use sqlx::PgPool;
use uuid::Uuid;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

const CANONICAL_SNAPSHOT_ID: i64 = 841_361_364_657_368_626;
const NEXT_CANONICAL_SNAPSHOT_ID: i64 = 841_361_364_657_368_627;
const MIRROR_SNAPSHOT_ID: &str = "iceberg:841361364657368626";
const EXECUTION_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_publication_execution_evidence.v1";
const QUALITY_SCHEMA_VERSION: &str = "foundation-platform.parcel_publication_quality.v1";
const PNU: &str = "9999900101100010001";

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn evidence_update_is_rejected_after_insert() -> TestResult {
    let fixture = Fixture::create("parcel_evidence_update").await?;
    let pool = fixture.pool().await?;
    fixture.seed_run(&pool, fixture.first_run_id).await?;
    let evidence_id = fixture
        .seed_evidence(&pool, fixture.first_run_id, 'a')
        .await?;

    let error = sqlx::query(
        "UPDATE catalog.parcel_publication_source_evidence
            SET projection_content_sha256 = repeat('f', 64)
          WHERE id = $1",
    )
    .bind(evidence_id)
    .execute(&pool)
    .await
    .expect_err("sealed evidence UPDATE must be rejected");
    assert_rejection(
        &error,
        "55000",
        "parcel publication source evidence is append-only",
    );

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn evidence_delete_is_rejected_after_insert() -> TestResult {
    let fixture = Fixture::create("parcel_evidence_delete").await?;
    let pool = fixture.pool().await?;
    fixture.seed_run(&pool, fixture.first_run_id).await?;
    let evidence_id = fixture
        .seed_evidence(&pool, fixture.first_run_id, 'b')
        .await?;

    let error = sqlx::query("DELETE FROM catalog.parcel_publication_source_evidence WHERE id = $1")
        .bind(evidence_id)
        .execute(&pool)
        .await
        .expect_err("sealed evidence DELETE must be rejected");
    assert_rejection(
        &error,
        "55000",
        "parcel publication source evidence is append-only",
    );

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn one_mirror_run_cannot_receive_second_evidence() -> TestResult {
    let fixture = Fixture::create("parcel_evidence_one_per_run").await?;
    let pool = fixture.pool().await?;
    fixture.seed_run(&pool, fixture.first_run_id).await?;
    fixture
        .seed_evidence(&pool, fixture.first_run_id, 'c')
        .await?;

    let error = fixture
        .seed_evidence(&pool, fixture.first_run_id, 'd')
        .await
        .expect_err("one mirror run must not receive two evidence rows");
    assert_constraint_rejection(
        &error,
        "23505",
        "parcel_publication_source_evidence_one_per_run_key",
    );

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn one_iceberg_snapshot_can_be_sealed_by_two_distinct_runs() -> TestResult {
    let fixture = Fixture::create("parcel_evidence_snapshot_reuse").await?;
    let pool = fixture.pool().await?;
    fixture.seed_run(&pool, fixture.first_run_id).await?;
    fixture.seed_run(&pool, fixture.second_run_id).await?;

    fixture
        .seed_evidence(&pool, fixture.first_run_id, 'e')
        .await?;
    fixture
        .seed_evidence(&pool, fixture.second_run_id, 'f')
        .await?;

    let evidence_count: i64 = sqlx::query_scalar(
        "SELECT count(*)
           FROM catalog.parcel_publication_source_evidence
          WHERE iceberg_snapshot_id = $1",
    )
    .bind(CANONICAL_SNAPSHOT_ID)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        evidence_count, 2,
        "snapshot reuse is a new run, not an evidence overwrite"
    );

    let source_row_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM serving_postgis.parcel_boundary_mirror WHERE pnu = $1",
    )
    .bind(PNU)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        source_row_count, 2,
        "the source row key must include the run so both materialisations survive"
    );

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn terminal_mirror_run_tuple_is_immutable() -> TestResult {
    let fixture = Fixture::create("parcel_evidence_terminal_run").await?;
    let pool = fixture.pool().await?;
    fixture.seed_run(&pool, fixture.first_run_id).await?;

    let error = sqlx::query(
        "UPDATE serving_postgis.parcel_boundary_mirror_rebuild_run
            SET quality_report = jsonb_set(quality_report, '{object_count}', '2'::jsonb),
                updated_at = now()
          WHERE id = $1",
    )
    .bind(fixture.first_run_id)
    .execute(&pool)
    .await
    .expect_err("a terminal run tuple must not change after validation");
    assert_rejection(
        &error,
        "55000",
        "terminal parcel mirror rebuild run is immutable",
    );

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn mirror_run_cannot_skip_from_planned_to_succeeded() -> TestResult {
    let fixture = Fixture::create("parcel_evidence_run_transition").await?;
    let pool = fixture.pool().await?;
    sqlx::query(
        "INSERT INTO serving_postgis.parcel_boundary_mirror_rebuild_run
            (id, source_snapshot_id, source_table, srid, status, started_at)
         VALUES ($1, $2, 'silver.parcel_boundaries', 5179, 'planned', now())",
    )
    .bind(fixture.first_run_id)
    .bind(MIRROR_SNAPSHOT_ID)
    .execute(&pool)
    .await?;

    let error = sqlx::query(
        "UPDATE serving_postgis.parcel_boundary_mirror_rebuild_run
            SET status = 'succeeded', loaded_row_count = 1, finished_at = now()
          WHERE id = $1",
    )
    .bind(fixture.first_run_id)
    .execute(&pool)
    .await
    .expect_err("a run must pass through running before success");
    assert_rejection(
        &error,
        "23514",
        "invalid parcel mirror rebuild run state transition",
    );

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn source_rows_of_a_sealed_run_are_immutable() -> TestResult {
    let fixture = Fixture::create("parcel_evidence_source_rows").await?;
    let pool = fixture.pool().await?;
    fixture.seed_run(&pool, fixture.first_run_id).await?;
    fixture
        .seed_evidence(&pool, fixture.first_run_id, '1')
        .await?;

    let error = sqlx::query(
        "DELETE FROM serving_postgis.parcel_boundary_mirror
          WHERE rebuild_run_id = $1 AND pnu = $2",
    )
    .bind(fixture.first_run_id)
    .bind(PNU)
    .execute(&pool)
    .await
    .expect_err("a sealed run's source row must not be deleted");
    assert_rejection(
        &error,
        "55000",
        "sealed parcel mirror source rows are append-only",
    );

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn sealed_source_rows_cannot_be_removed_by_truncate() -> TestResult {
    let fixture = Fixture::create("parcel_evidence_source_truncate").await?;
    let pool = fixture.pool().await?;
    fixture.seed_run(&pool, fixture.first_run_id).await?;
    fixture
        .seed_evidence(&pool, fixture.first_run_id, '5')
        .await?;

    let error = sqlx::query("TRUNCATE TABLE serving_postgis.parcel_boundary_mirror")
        .execute(&pool)
        .await
        .expect_err("TRUNCATE must not bypass sealed-row immutability");
    assert_rejection(
        &error,
        "55000",
        "sealed parcel mirror source rows are append-only",
    );

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn evidence_rejects_a_run_whose_source_row_tuple_changed_before_sealing() -> TestResult {
    let fixture = Fixture::create("parcel_evidence_source_tuple").await?;
    let pool = fixture.pool().await?;
    fixture.seed_run(&pool, fixture.first_run_id).await?;
    sqlx::query(
        "UPDATE serving_postgis.parcel_boundary_mirror
            SET source_record_id = NULL
          WHERE rebuild_run_id = $1",
    )
    .bind(fixture.first_run_id)
    .execute(&pool)
    .await?;

    let error = fixture
        .seed_evidence(&pool, fixture.first_run_id, '6')
        .await
        .expect_err("the evidence row must re-read every source-row provenance tuple");
    assert_rejection(
        &error,
        "23514",
        "source evidence does not match the run-keyed parcel mirror row set",
    );

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn legacy_run_without_source_record_cannot_be_sealed() -> TestResult {
    let fixture = Fixture::create("parcel_evidence_legacy_null_source").await?;
    let pool = fixture.pool().await?;
    fixture.seed_legacy_run_without_source(&pool).await?;

    let error = fixture
        .seed_evidence(&pool, fixture.first_run_id, '2')
        .await
        .expect_err("legacy nullable provenance must not become publishable evidence");
    assert_rejection(
        &error,
        "23514",
        "source evidence does not match one succeeded mirror rebuild tuple",
    );

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn empty_quality_report_cannot_be_sealed() -> TestResult {
    let fixture = Fixture::create("parcel_evidence_empty_quality").await?;
    let pool = fixture.pool().await?;
    fixture.seed_run(&pool, fixture.first_run_id).await?;

    // This is an intentionally violating fixture. Temporarily disable only the terminal-run guard
    // in this disposable database so the row can model a historical succeeded run whose old `{}`
    // report predates the migration; the evidence validator itself remains enabled and must refuse.
    sqlx::query(
        "ALTER TABLE serving_postgis.parcel_boundary_mirror_rebuild_run
         DISABLE TRIGGER parcel_boundary_mirror_rebuild_run_state_guard",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "UPDATE serving_postgis.parcel_boundary_mirror_rebuild_run
            SET quality_report = '{}'::jsonb
          WHERE id = $1",
    )
    .bind(fixture.first_run_id)
    .execute(&pool)
    .await?;
    sqlx::query(
        "ALTER TABLE serving_postgis.parcel_boundary_mirror_rebuild_run
         ENABLE TRIGGER parcel_boundary_mirror_rebuild_run_state_guard",
    )
    .execute(&pool)
    .await?;

    let error = fixture
        .seed_evidence(&pool, fixture.first_run_id, '7')
        .await
        .expect_err("an empty quality report is unmeasured, not zero defects");
    assert_rejection(
        &error,
        "23514",
        "source evidence requires a complete parcel publication quality report",
    );

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn runtime_promote_rejects_parcel_load_without_sealed_evidence() -> TestResult {
    let fixture = Fixture::create("parcel_evidence_runtime_gate").await?;
    let pool = fixture.pool().await?;
    let manifest_id = fixture.seed_runtime_publication(&pool, None).await?;

    let error = sqlx::query_scalar::<_, i64>(
        "SELECT catalog.promote_vector_tile_runtime_manifest(NULL, $1)",
    )
    .bind(manifest_id)
    .fetch_one(&pool)
    .await
    .expect_err("an unbound parcels load must not reach the runtime pointer");
    assert_rejection(&error, "23514", "sealed parcel publication evidence");

    let pointer_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM catalog.vector_tile_runtime_manifest_pointer WHERE singleton",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        pointer_count, 0,
        "a refused promotion must not move the pointer"
    );

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn parcel_load_cannot_mix_revision_and_source_evidence() -> TestResult {
    let fixture = Fixture::create("parcel_evidence_load_mix").await?;
    let pool = fixture.pool().await?;
    fixture.seed_run(&pool, fixture.first_run_id).await?;
    let evidence_id = fixture
        .seed_evidence(&pool, fixture.first_run_id, '3')
        .await?;
    let revision_id = fixture
        .seed_publication_revision(&pool, NEXT_CANONICAL_SNAPSHOT_ID)
        .await?;

    let error = sqlx::query(
        "INSERT INTO serving_postgis.spatial_projection_load
            (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
             source_evidence_id, status)
         VALUES (gen_random_uuid(), $1, $2, $3, $4, 'running')",
    )
    .bind(fixture.publication_unit_id)
    .bind(revision_id)
    .bind(NEXT_CANONICAL_SNAPSHOT_ID.to_string())
    .bind(evidence_id)
    .execute(&pool)
    .await
    .expect_err("a load may not combine a revision with another snapshot's evidence");
    assert_rejection(
        &error,
        "23514",
        "parcel projection load does not match its sealed source evidence",
    );

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn terminal_load_cannot_receive_evidence_after_materialisation() -> TestResult {
    let fixture = Fixture::create("parcel_evidence_late_load_binding").await?;
    let pool = fixture.pool().await?;
    fixture.seed_run(&pool, fixture.first_run_id).await?;
    let evidence_id = fixture
        .seed_evidence(&pool, fixture.first_run_id, '8')
        .await?;
    let revision_id = fixture
        .seed_publication_revision(&pool, CANONICAL_SNAPSHOT_ID)
        .await?;
    let load_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO serving_postgis.spatial_projection_load
            (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id, status)
         VALUES ($1, $2, $3, $4, 'running')",
    )
    .bind(load_id)
    .bind(fixture.publication_unit_id)
    .bind(revision_id)
    .bind(CANONICAL_SNAPSHOT_ID.to_string())
    .execute(&pool)
    .await?;
    sqlx::query(
        "UPDATE serving_postgis.spatial_projection_load
            SET status = 'succeeded', loaded_row_count = 1, finished_at = now()
          WHERE id = $1",
    )
    .bind(load_id)
    .execute(&pool)
    .await?;

    let error = sqlx::query(
        "UPDATE serving_postgis.spatial_projection_load
            SET source_evidence_id = $2
          WHERE id = $1",
    )
    .bind(load_id)
    .bind(evidence_id)
    .execute(&pool)
    .await
    .expect_err("evidence must be bound before a load becomes terminal");
    assert_rejection(
        &error,
        "55000",
        "terminal spatial projection load evidence binding is immutable",
    );

    fixture.finish(pool).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
async fn runtime_promote_accepts_parcel_load_bound_to_matching_evidence() -> TestResult {
    let fixture = Fixture::create("parcel_evidence_runtime_accept").await?;
    let pool = fixture.pool().await?;
    fixture.seed_run(&pool, fixture.first_run_id).await?;
    let evidence_id = fixture
        .seed_evidence(&pool, fixture.first_run_id, '4')
        .await?;
    let manifest_id = fixture
        .seed_runtime_publication(&pool, Some(evidence_id))
        .await?;

    let generation: i64 =
        sqlx::query_scalar("SELECT catalog.promote_vector_tile_runtime_manifest(NULL, $1)")
            .bind(manifest_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(generation, 1);
    let selected: Uuid = sqlx::query_scalar(
        "SELECT manifest_id
           FROM catalog.vector_tile_runtime_manifest_pointer
          WHERE singleton",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(selected, manifest_id);

    fixture.finish(pool).await
}

struct Fixture {
    database: DisposableDatabaseUrl,
    source_record_id: Uuid,
    source_file_asset_id: Uuid,
    first_run_id: Uuid,
    second_run_id: Uuid,
    publication_unit_id: Uuid,
    iceberg_table_uuid: Uuid,
}

impl Fixture {
    async fn create(label: &str) -> TestResult<Self> {
        let fixture = Self {
            database: disposable_database_url(label).await?,
            source_record_id: Uuid::new_v4(),
            source_file_asset_id: Uuid::new_v4(),
            first_run_id: Uuid::new_v4(),
            second_run_id: Uuid::new_v4(),
            publication_unit_id: Uuid::new_v4(),
            iceberg_table_uuid: Uuid::new_v4(),
        };
        let pool = fixture.pool().await?;
        MIGRATOR.run(&pool).await?;
        fixture.seed_catalog_provenance(&pool).await?;
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

    async fn seed_catalog_provenance(&self, pool: &PgPool) -> TestResult {
        sqlx::query(
            "INSERT INTO catalog.source_record
                (id, source, external_id, checksum_sha256, raw_object_key)
             VALUES ($1, 'parcel-publication-evidence-test', $2, repeat('a', 64), $3)",
        )
        .bind(self.source_record_id)
        .bind(format!(
            "parcel-publication-evidence-{}",
            self.source_record_id
        ))
        .bind(format!(
            "silver/parcel-boundaries/{}/metadata.json",
            self.source_record_id
        ))
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.file_asset
                (id, object_key, mime_type, size_bytes, checksum_sha256,
                 source_record_id, visibility)
             VALUES ($1, $2, 'application/json', 1, repeat('b', 64), $3, 'internal')",
        )
        .bind(self.source_file_asset_id)
        .bind(format!(
            "silver/parcel-boundaries/{}/manifest.json",
            self.source_file_asset_id
        ))
        .bind(self.source_record_id)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.vector_tile_publication_unit (id, unit_key)
             VALUES ($1, 'parcels')",
        )
        .bind(self.publication_unit_id)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn seed_run(&self, pool: &PgPool, run_id: Uuid) -> TestResult {
        sqlx::query(
            "INSERT INTO serving_postgis.parcel_boundary_mirror_rebuild_run
                (id, source_snapshot_id, source_table, source_record_id, source_file_asset_id,
                 srid, status, loaded_row_count, rejected_row_count, quality_report, started_at)
             VALUES ($1, $2, 'silver.parcel_boundaries', $3, $4, 5179, 'running', 0, 0,
                     jsonb_build_object(
                         'schema_version', $5::text,
                         'object_count', 1,
                         'expected_row_count', 1,
                         'loaded_row_count', 1,
                         'invalid_srid_count', 0,
                         'invalid_geometry_count', 0,
                         'empty_geometry_count', 0,
                         'nonpositive_area_count', 0,
                         'source_srid', 'EPSG:4326',
                         'target_srid', 'EPSG:5179',
                         'geometry_repair_strategy', 'postgis-make-valid-v1'
                     ), now())",
        )
        .bind(run_id)
        .bind(MIRROR_SNAPSHOT_ID)
        .bind(self.source_record_id)
        .bind(self.source_file_asset_id)
        .bind(QUALITY_SCHEMA_VERSION)
        .execute(pool)
        .await?;

        sqlx::query(
            "INSERT INTO serving_postgis.parcel_boundary_mirror
                (pnu, rebuild_run_id, source_snapshot_id, source_table,
                 source_record_id, source_file_asset_id, source_object_key, source_row_id,
                 geometry_checksum_sha256, properties, geom)
             VALUES ($1, $2, $3, 'silver.parcel_boundaries', $4, $5, $6, $1,
                     repeat('c', 64), '{}'::jsonb,
                     public.st_multi(public.st_transform(
                         public.st_setsrid(public.st_geomfromtext(
                             'POLYGON((127.1231 36.1231,127.1232 36.1231,127.1232 36.1232,127.1231 36.1232,127.1231 36.1231))'
                         ), 4326), 5179)))",
        )
        .bind(PNU)
        .bind(run_id)
        .bind(MIRROR_SNAPSHOT_ID)
        .bind(self.source_record_id)
        .bind(self.source_file_asset_id)
        .bind(format!("silver/parcel-boundaries/{run_id}/part-0001.parquet"))
        .execute(pool)
        .await?;

        sqlx::query(
            "UPDATE serving_postgis.parcel_boundary_mirror_rebuild_run
                SET status = 'succeeded', loaded_row_count = 1, finished_at = now(),
                    updated_at = now(), version = version + 1
              WHERE id = $1 AND status = 'running'",
        )
        .bind(run_id)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn seed_legacy_run_without_source(&self, pool: &PgPool) -> TestResult {
        sqlx::query(
            "INSERT INTO serving_postgis.parcel_boundary_mirror_rebuild_run
                (id, source_snapshot_id, source_table, srid, status, loaded_row_count,
                 rejected_row_count, quality_report, started_at, finished_at)
             VALUES ($1, $2, 'silver.parcel_boundaries', 5179, 'succeeded', 1, 0,
                     '{}'::jsonb, now(), now())",
        )
        .bind(self.first_run_id)
        .bind(MIRROR_SNAPSHOT_ID)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn seed_evidence(
        &self,
        pool: &PgPool,
        run_id: Uuid,
        hash_digit: char,
    ) -> Result<Uuid, sqlx::Error> {
        let evidence_id = Uuid::new_v4();
        let hash = hash_digit.to_string().repeat(64);
        sqlx::query(
            "INSERT INTO catalog.parcel_publication_source_evidence
                (id, mirror_rebuild_run_id, mirror_rebuild_run_status,
                 iceberg_table_uuid, iceberg_logical_table, iceberg_snapshot_id,
                 source_record_id, source_file_asset_id,
                 execution_evidence_schema_version, execution_evidence_object_key,
                 execution_evidence_sha256, source_row_count, projection_content_sha256,
                 quality_schema_version)
             VALUES ($1, $2, 'succeeded', $3, 'silver.parcel_boundaries', $4, $5, $6,
                     $7, $8, $9, 1, $9, $10)",
        )
        .bind(evidence_id)
        .bind(run_id)
        .bind(self.iceberg_table_uuid)
        .bind(CANONICAL_SNAPSHOT_ID)
        .bind(self.source_record_id)
        .bind(self.source_file_asset_id)
        .bind(EXECUTION_SCHEMA_VERSION)
        .bind(format!("evidence/parcel-publication/{evidence_id}.json"))
        .bind(hash)
        .bind(QUALITY_SCHEMA_VERSION)
        .execute(pool)
        .await?;
        Ok(evidence_id)
    }

    async fn seed_runtime_publication(
        &self,
        pool: &PgPool,
        source_evidence_id: Option<Uuid>,
    ) -> TestResult<Uuid> {
        let revision_id = self
            .seed_publication_revision(pool, CANONICAL_SNAPSHOT_ID)
            .await?;
        let projection_load_id = Uuid::new_v4();
        let release_id = Uuid::new_v4();
        let manifest_id = Uuid::new_v4();

        sqlx::query(
            "INSERT INTO serving_postgis.spatial_projection_load
                (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
                 source_evidence_id, status, loaded_row_count, finished_at)
             VALUES ($1, $2, $3, $4, $5, 'succeeded', 1, now())",
        )
        .bind(projection_load_id)
        .bind(self.publication_unit_id)
        .bind(revision_id)
        .bind(CANONICAL_SNAPSHOT_ID.to_string())
        .bind(source_evidence_id)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.vector_tile_release
                (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
                 source_record_id, source_kind, martin_source_id, tiles_url_template,
                 postgis_projection_revision)
             VALUES ($1, $2, $3, $4, $5, 'dynamic_postgis', 'sealed_parcels',
                     'https://tiles.example.test/sealed_parcels/{z}/{x}/{y}', $6)",
        )
        .bind(release_id)
        .bind(self.publication_unit_id)
        .bind(revision_id)
        .bind(CANONICAL_SNAPSHOT_ID.to_string())
        .bind(self.source_record_id)
        .bind(projection_load_id)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.vector_tile_release_layer
                (release_id, layer_id, source_layer, feature_id_property,
                 tile_min_zoom, tile_max_zoom, render_min_zoom, render_max_zoom,
                 feature_filter_properties)
             VALUES ($1, 'parcels', 'parcels', 'pnu', 14, 16, 14, 22, '{}'::jsonb)",
        )
        .bind(release_id)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.vector_tile_runtime_manifest (id, manifest_generation)
             VALUES ($1, 1)",
        )
        .bind(manifest_id)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.vector_tile_runtime_manifest_unit
                (manifest_id, publication_unit_id, release_id, serving_generation,
                 data_revision, canonical_iceberg_snapshot_id)
             VALUES ($1, $2, $3, 1, $4, $5)",
        )
        .bind(manifest_id)
        .bind(self.publication_unit_id)
        .bind(release_id)
        .bind(revision_id)
        .bind(CANONICAL_SNAPSHOT_ID.to_string())
        .execute(pool)
        .await?;
        Ok(manifest_id)
    }

    async fn seed_publication_revision(&self, pool: &PgPool, snapshot_id: i64) -> TestResult<Uuid> {
        let revision_id = Uuid::new_v4();
        let mut transaction = pool.begin().await?;
        sqlx::query("SELECT set_config('foundation.temporal_publisher', 'on', true)")
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "INSERT INTO catalog.publication_revision
                (id, publication_unit_id, canonical_iceberg_snapshot_id, source_record_id)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(revision_id)
        .bind(self.publication_unit_id)
        .bind(snapshot_id.to_string())
        .bind(self.source_record_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(revision_id)
    }
}

fn assert_rejection(error: &sqlx::Error, expected_code: &str, message_fragment: &str) {
    let database_error = error
        .as_database_error()
        .unwrap_or_else(|| panic!("expected a PostgreSQL rejection, got {error:?}"));
    assert_eq!(
        database_error.code().as_deref(),
        Some(expected_code),
        "unexpected SQLSTATE: {error:?}"
    );
    assert!(
        database_error.message().contains(message_fragment),
        "rejection did not name the invariant: {error:?}"
    );
    eprintln!(
        "observed rejection SQLSTATE={} message={}",
        expected_code,
        database_error.message()
    );
}

fn assert_constraint_rejection(error: &sqlx::Error, expected_code: &str, constraint: &str) {
    let database_error = error
        .as_database_error()
        .unwrap_or_else(|| panic!("expected a PostgreSQL rejection, got {error:?}"));
    assert_eq!(database_error.code().as_deref(), Some(expected_code));
    assert_eq!(
        database_error.constraint(),
        Some(constraint),
        "wrong constraint rejected the violating row: {error:?}"
    );
    eprintln!(
        "observed rejection SQLSTATE={} constraint={}",
        expected_code, constraint
    );
}
