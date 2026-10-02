//! A lakehouse bake replaces the active static release with an archive baked from a new lakehouse
//! snapshot, under a new revision tied to the same collected source, and leaves no fallback
//! (root ADR-0112).

use super::readdress::{
    assert_database_error, ledger_counts, promotion_command, release_json, seed_static_release,
    validated_result,
};
use super::*;
use catalog_application::ports::{LakehouseBakeSilverSource, StartLakehouseBakeCommand};
use foundation_shared_kernel::ids::VectorTileBuildJobId;

/// A lakehouse snapshot other than the one the seeded static release serves.
const GOLD_SNAPSHOT: &str = "841361364657368625";

fn bake_command(
    input_release_id: VectorTileReleaseId,
    snapshot: &str,
    key: &str,
) -> TestResult<StartLakehouseBakeCommand> {
    Ok(StartLakehouseBakeCommand {
        unit_key: "complex".to_owned(),
        input_release_id,
        canonical_iceberg_snapshot_id: CanonicalIcebergSnapshotId::new(snapshot.to_owned())?,
        idempotency_key: key.to_owned(),
        operator_staff_id: StaffId::new(OPERATOR_STAFF_ID),
        silver_source: None,
    })
}

async fn fallback(pool: &PgPool) -> TestResult<(Option<Uuid>, Option<Uuid>)> {
    Ok(sqlx::query_as(
        "SELECT fallback_release_id, fallback_data_revision
         FROM catalog.vector_tile_publication_unit WHERE unit_key = 'complex'",
    )
    .fetch_one(pool)
    .await?)
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_lakehouse_bake_serves_a_new_revision_and_leaves_no_fallback() -> TestResult {
    run_in_disposable_database("tile_lakehouse_bake", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let (_dynamic, original) = seed_static_release(&pool).await?;
        let input = published_unit(&original, "complex")?;
        assert!(
            fallback(&pool).await?.0.is_some(),
            "the seeded bake keeps a dynamic fallback"
        );
        let input_row = release_json(&pool, input.active_release_id).await?;

        let uow = Arc::new(PgCatalogUnitOfWork::new(pool.clone()));
        let lifecycle = VectorTileBuildLifecycle::new(uow.clone());
        let command = bake_command(input.active_release_id, GOLD_SNAPSHOT, "lakehouse-bake-1")?;
        let build_job_id = lifecycle.start_lakehouse_bake(command.clone()).await?;
        assert_eq!(
            lifecycle.start_lakehouse_bake(command.clone()).await?,
            build_job_id
        );

        let (kind, output_revision, frozen): (String, Uuid, String) = sqlx::query_as(
            "SELECT kind, output_data_revision, frozen_source_snapshot_id
             FROM catalog.vector_tile_build_job WHERE id = $1",
        )
        .bind(build_job_id.as_uuid())
        .fetch_one(&pool)
        .await?;
        assert_eq!(kind, "lakehouse_bake");
        assert_eq!(frozen, GOLD_SNAPSHOT);
        assert_ne!(output_revision, input.data_revision.as_uuid());
        let (input_source, output_source): (Uuid, Uuid) = sqlx::query_as(
            "SELECT (SELECT source_record_id FROM catalog.publication_revision WHERE id = $1),
                    (SELECT source_record_id FROM catalog.publication_revision WHERE id = $2)",
        )
        .bind(input.data_revision.as_uuid())
        .bind(output_revision)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            output_source, input_source,
            "the new revision keeps the collected source"
        );

        let result = validated_result(build_job_id, "https://tiles.example.test")?;
        lifecycle.record_result(result).await?;
        let manifest = PromoteTileLayerStatic::new(uow)
            .execute(promotion_command(
                build_job_id,
                input,
                "promote-lakehouse-bake-1",
            ))
            .await?;
        let selected = published_unit(&manifest, "complex")?;
        assert_eq!(
            selected.active_release_id,
            static_release_id_for_build(build_job_id)
        );
        assert_eq!(selected.data_revision.as_uuid(), output_revision);
        assert_eq!(
            selected.canonical_iceberg_snapshot_id.as_str(),
            GOLD_SNAPSHOT
        );
        assert_eq!(
            selected.serving_generation.value(),
            input.serving_generation.value() + 1
        );
        assert_eq!(
            serde_json::to_value(&selected.layers)?,
            serde_json::to_value(&input.layers)?
        );
        assert_eq!(
            fallback(&pool).await?,
            (None, None),
            "ADR-0112: no fallback after a lakehouse bake"
        );

        let row = release_json(&pool, selected.active_release_id).await?;
        for field in ["source_record_id", "source_file_asset_ids"] {
            assert_eq!(
                row[field], input_row[field],
                "a lakehouse bake changed {field}"
            );
        }
        assert!(row["readdressed_from_release_id"].is_null());

        // A retried start answers with the one build and writes nothing.
        let counts = ledger_counts(&pool).await?;
        assert_eq!(lifecycle.start_lakehouse_bake(command).await?, build_job_id);
        assert_eq!(ledger_counts(&pool).await?, counts);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_lakehouse_bake_refuses_the_same_snapshot_a_stale_input_and_a_dynamic_input() -> TestResult
{
    run_in_disposable_database("tile_lakehouse_bake_refusals", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let (dynamic, original) = seed_static_release(&pool).await?;
        let input = published_unit(&original, "complex")?;
        let lifecycle =
            VectorTileBuildLifecycle::new(Arc::new(PgCatalogUnitOfWork::new(pool.clone())));
        let counts = ledger_counts(&pool).await?;

        let same = bake_command(
            input.active_release_id,
            input.canonical_iceberg_snapshot_id.as_str(),
            "same",
        )?;
        assert!(
            lifecycle.start_lakehouse_bake(same).await.is_err(),
            "the same snapshot is not a new bake"
        );
        let dynamic_input = published_unit(&dynamic, "complex")?.active_release_id;
        let stale = bake_command(dynamic_input, GOLD_SNAPSHOT, "stale")?;
        assert!(
            lifecycle.start_lakehouse_bake(stale).await.is_err(),
            "only the active static release"
        );
        assert_eq!(
            ledger_counts(&pool).await?,
            counts,
            "a refused start writes nothing"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn direct_sql_cannot_rebind_a_lakehouse_bake_to_another_source_or_change_its_inputs(
) -> TestResult {
    run_in_disposable_database("tile_lakehouse_bake_schema", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let (_dynamic, original) = seed_static_release(&pool).await?;
        let input = published_unit(&original, "complex")?;
        let lifecycle = VectorTileBuildLifecycle::new(Arc::new(PgCatalogUnitOfWork::new(pool.clone())));
        let build_job_id: VectorTileBuildJobId = lifecycle
            .start_lakehouse_bake(bake_command(input.active_release_id, GOLD_SNAPSHOT, "schema")?)
            .await?;

        // A revision over another snapshot anchored to a different collected source.
        let other_source = Uuid::new_v4();
        let foreign_revision = Uuid::new_v4();
        let mut tx = pool.begin().await?;
        sqlx::query(
            "INSERT INTO catalog.source_record (id, source, external_id, checksum_sha256)
             VALUES ($1, 'test', $2, repeat('b', 64))",
        )
        .bind(other_source)
        .bind(format!("foreign-{other_source}"))
        .execute(&mut *tx)
        .await?;
        sqlx::query("SELECT set_config('foundation.temporal_publisher', 'on', true)")
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO catalog.publication_revision
             (id, publication_unit_id, canonical_iceberg_snapshot_id, source_record_id)
             SELECT $1, id, '841361364657368626', $2 FROM catalog.vector_tile_publication_unit
             WHERE unit_key = 'complex'",
        )
        .bind(foreign_revision)
        .bind(other_source)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        let error = sqlx::query(
            "INSERT INTO catalog.vector_tile_build_job
             (id, publication_unit_id, input_release_id, input_data_revision, frozen_source_snapshot_id,
              status, idempotency_key, kind, input_serving_generation, output_data_revision)
             SELECT $1, publication_unit_id, input_release_id, input_data_revision,
                    '841361364657368626', 'running', 'foreign', 'lakehouse_bake',
                    input_serving_generation, $2
             FROM catalog.vector_tile_build_job WHERE id = $3",
        )
        .bind(Uuid::now_v7())
        .bind(foreign_revision)
        .bind(build_job_id.as_uuid())
        .execute(&pool)
        .await
        .expect_err("a lakehouse bake cannot claim a revision of another collected source");
        assert_database_error(&error, "23514", None);

        let error = sqlx::query(
            "UPDATE catalog.vector_tile_build_job SET frozen_source_snapshot_id = '841361364657368626'
             WHERE id = $1",
        )
        .bind(build_job_id.as_uuid())
        .execute(&pool)
        .await
        .expect_err("a lakehouse bake's inputs are frozen");
        assert_database_error(&error, "23514", None);
        Ok(())
    })
    .await
}

/// The Silver snapshot a v2 served summary names (synthetic; root ADR-0133 §5).
const SILVER_SNAPSHOT: &str = "synthetic-silver-2";

fn verdict(snapshot: &str, passed: bool, checked: u64) -> serde_json::Value {
    serde_json::json!({
        "schema_version": "foundation-platform.parcel_matching_verdict.v1",
        "job": "parcel_matching_gate",
        "snapshot_id": snapshot,
        "passed": passed,
        "snapshot_parcel_count": 3,
        "parcels": {"passed": passed, "checked": checked, "violations": {}, "allowed": {}},
        "attributes": {},
    })
}

fn silver_bake_command(
    input_release_id: VectorTileReleaseId,
    key: &str,
    matching_verdict: serde_json::Value,
) -> TestResult<StartLakehouseBakeCommand> {
    Ok(StartLakehouseBakeCommand {
        silver_source: Some(LakehouseBakeSilverSource {
            source_snapshot_id: SILVER_SNAPSHOT.to_owned(),
            matching_verdict,
            matching_verdict_sha256: "c".repeat(64),
        }),
        ..bake_command(input_release_id, GOLD_SNAPSHOT, key)?
    })
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_bake_of_a_silver_snapshot_anchors_its_revision_to_that_snapshot() -> TestResult {
    run_in_disposable_database("tile_lakehouse_bake_silver", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let (_dynamic, original) = seed_static_release(&pool).await?;
        let input = published_unit(&original, "complex")?;
        let input_row = release_json(&pool, input.active_release_id).await?;
        let uow = Arc::new(PgCatalogUnitOfWork::new(pool.clone()));
        let lifecycle = VectorTileBuildLifecycle::new(uow.clone());
        let command = silver_bake_command(
            input.active_release_id,
            "silver-1",
            verdict(SILVER_SNAPSHOT, true, 3),
        )?;
        let build_job_id = lifecycle.start_lakehouse_bake(command.clone()).await?;
        assert_eq!(lifecycle.start_lakehouse_bake(command).await?, build_job_id);

        let (source, external_id, bronze, input_source, recorded): (
            String,
            String,
            Option<Uuid>,
            Option<Uuid>,
            serde_json::Value,
        ) = sqlx::query_as(
            "SELECT record.source, record.external_id, output.bronze_object_id,
                    input.source_record_id, build.matching_verdict
             FROM catalog.vector_tile_build_job AS build
             JOIN catalog.publication_revision AS output ON output.id = build.output_data_revision
             JOIN catalog.publication_revision AS input ON input.id = build.input_data_revision
             JOIN catalog.source_record AS record ON record.id = output.source_record_id
             WHERE build.id = $1 AND build.bound_source_record_id = output.source_record_id",
        )
        .bind(build_job_id.as_uuid())
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            (source.as_str(), external_id.as_str(), bronze),
            ("lakehouse:silver.parcel_boundaries", SILVER_SNAPSHOT, None),
            "the new revision names the Silver snapshot it was baked from"
        );
        assert!(
            input_source.is_some(),
            "the seeded input names its own record"
        );
        assert_eq!(
            recorded["passed"], true,
            "the verdict is kept as build evidence"
        );

        lifecycle
            .record_result(validated_result(
                build_job_id,
                "https://tiles.example.test",
            )?)
            .await?;
        let manifest = PromoteTileLayerStatic::new(uow)
            .execute(promotion_command(build_job_id, input, "promote-silver-1"))
            .await?;
        let selected = published_unit(&manifest, "complex")?;
        let bound: Uuid = sqlx::query_scalar(
            "SELECT revision.source_record_id FROM catalog.publication_revision AS revision
             JOIN catalog.vector_tile_build_job AS build
               ON build.output_data_revision = revision.id
              AND build.bound_source_record_id = revision.source_record_id
             WHERE build.id = $1 AND revision.id = $2",
        )
        .bind(build_job_id.as_uuid())
        .bind(selected.data_revision.as_uuid())
        .fetch_one(&pool)
        .await?;
        assert_ne!(
            serde_json::json!(bound),
            input_row["source_record_id"],
            "the served revision names the Silver snapshot, not the input's source"
        );
        // The release keeps the file lineage of its PMTiles; the provenance anchor is the revision.
        let row = release_json(&pool, selected.active_release_id).await?;
        for field in ["source_record_id", "source_file_asset_ids"] {
            assert_eq!(row[field], input_row[field], "{field}");
        }
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_bake_of_a_silver_snapshot_without_a_passing_verdict_for_it_does_not_start() -> TestResult
{
    run_in_disposable_database("tile_lakehouse_bake_silver_refusals", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let (_dynamic, original) = seed_static_release(&pool).await?;
        let input = published_unit(&original, "complex")?;
        let lifecycle =
            VectorTileBuildLifecycle::new(Arc::new(PgCatalogUnitOfWork::new(pool.clone())));
        let counts = ledger_counts(&pool).await?;
        for (key, planted, refusal) in [
            (
                "refused",
                verdict(SILVER_SNAPSHOT, false, 3),
                "needs a passing matching verdict",
            ),
            (
                "other",
                verdict("synthetic-silver-1", true, 3),
                "needs a passing matching verdict",
            ),
            (
                "partial",
                verdict(SILVER_SNAPSHOT, true, 2),
                "did not check every parcel",
            ),
            (
                "no-verdict",
                serde_json::json!({}),
                "needs a passing matching verdict",
            ),
        ] {
            let error = lifecycle
                .start_lakehouse_bake(silver_bake_command(
                    input.active_release_id,
                    key,
                    planted,
                )?)
                .await
                .expect_err("the build must not start");
            assert!(error.to_string().contains(refusal), "{key}: {error}");
        }
        assert_eq!(
            ledger_counts(&pool).await?,
            counts,
            "a refused start writes nothing"
        );
        let records: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM catalog.source_record
             WHERE source = 'lakehouse:silver.parcel_boundaries'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(records, 0, "a refused start leaves no source record behind");

        // Direct SQL: a v2 row without its evidence, and one whose revision keeps the old source.
        let v1 = lifecycle
            .start_lakehouse_bake(bake_command(input.active_release_id, GOLD_SNAPSHOT, "v1")?)
            .await?;
        let error = sqlx::query(
            "INSERT INTO catalog.vector_tile_build_job
             (id, publication_unit_id, input_release_id, input_data_revision, frozen_source_snapshot_id,
              status, idempotency_key, kind, input_serving_generation, output_data_revision,
              source_snapshot_id)
             SELECT $1, publication_unit_id, input_release_id, input_data_revision,
                    frozen_source_snapshot_id, 'running', 'no-evidence', 'lakehouse_bake',
                    input_serving_generation, output_data_revision, $2
             FROM catalog.vector_tile_build_job WHERE id = $3",
        )
        .bind(Uuid::now_v7())
        .bind(SILVER_SNAPSHOT)
        .bind(v1.as_uuid())
        .execute(&pool)
        .await
        .expect_err("a Silver snapshot without its verdict is refused");
        assert_database_error(&error, "23514", None);
        assert!(
            error.to_string().contains("needs a passing matching verdict"),
            "{error}"
        );
        let error = sqlx::query(
            "UPDATE catalog.vector_tile_build_job SET source_snapshot_id = $2 WHERE id = $1",
        )
        .bind(v1.as_uuid())
        .bind(SILVER_SNAPSHOT)
        .execute(&pool)
        .await
        .expect_err("a started v1 bake cannot be relabelled as a Silver bake");
        assert_database_error(&error, "23514", None);
        let record = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO catalog.source_record (id, source, external_id)
             VALUES ($1, 'lakehouse:silver.parcel_boundaries', $2)",
        )
        .bind(record)
        .bind(SILVER_SNAPSHOT)
        .execute(&pool)
        .await?;
        let error = sqlx::query(
            "INSERT INTO catalog.vector_tile_build_job
             (id, publication_unit_id, input_release_id, input_data_revision, frozen_source_snapshot_id,
              status, idempotency_key, kind, input_serving_generation, output_data_revision,
              source_snapshot_id, matching_verdict, matching_verdict_sha256, bound_source_record_id)
             SELECT $1, publication_unit_id, input_release_id, input_data_revision,
                    frozen_source_snapshot_id, 'running', 'kept-old-source', 'lakehouse_bake',
                    input_serving_generation, output_data_revision, $2, $3, repeat('c', 64), $4
             FROM catalog.vector_tile_build_job WHERE id = $5",
        )
        .bind(Uuid::now_v7())
        .bind(SILVER_SNAPSHOT)
        .bind(verdict(SILVER_SNAPSHOT, true, 3))
        .bind(record)
        .bind(v1.as_uuid())
        .execute(&pool)
        .await
        .expect_err("a revision anchored to the input's source does not match the snapshot read");
        assert_database_error(&error, "23514", None);
        assert!(
            error
                .to_string()
                .contains("anchored to the Silver snapshot it read"),
            "{error}"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_parcels_bake_without_a_silver_snapshot_does_not_start() -> TestResult {
    run_in_disposable_database(
        "tile_lakehouse_bake_parcels_needs_silver",
        |pool| async move {
            MIGRATOR.run(&pool).await?;
            let (_dynamic, original) = seed_static_release(&pool).await?;
            let input = published_unit(&original, "complex")?;
            // Nothing references unit_key, so the seeded unit can stand in for parcels.
            sqlx::query(
                "UPDATE catalog.vector_tile_publication_unit SET unit_key = 'parcels'
             WHERE unit_key = 'complex'",
            )
            .execute(&pool)
            .await?;
            let lifecycle =
                VectorTileBuildLifecycle::new(Arc::new(PgCatalogUnitOfWork::new(pool.clone())));
            let counts = ledger_counts(&pool).await?;
            let mut command = bake_command(input.active_release_id, GOLD_SNAPSHOT, "parcels-v1")?;
            command.unit_key = "parcels".to_owned();
            let error = lifecycle
                .start_lakehouse_bake(command)
                .await
                .expect_err("a parcels bake that keeps the inherited source must not start");
            assert!(
                error
                    .to_string()
                    .contains("must name the Silver snapshot it read"),
                "{error}"
            );
            assert_eq!(
                ledger_counts(&pool).await?,
                counts,
                "a refused start writes nothing"
            );
            Ok(())
        },
    )
    .await
}
