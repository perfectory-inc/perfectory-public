//! Readdressing preserves a validated static release's data and bytes while publishing a new URL.

use super::*;
use catalog_application::ports::StartStaticReleaseReaddressCommand;
use foundation_shared_kernel::ids::VectorTileBuildJobId;
use serde_json::json;

const DESTINATION: &str = "https://relocated.example.test/tiles";
const ARTIFACT_SIZE: u64 = 321_987;

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn readdress_preserves_data_bytes_and_fallback_and_retries_write_nothing() -> TestResult {
    run_in_disposable_database("tile_readdress_publication", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let (dynamic, original) = seed_static_release(&pool).await?;
        let input = published_unit(&original, "complex")?;
        let original_row = release_json(&pool, input.active_release_id).await?;
        let uow = Arc::new(PgCatalogUnitOfWork::new(pool.clone()));
        let lifecycle = VectorTileBuildLifecycle::new(uow.clone());
        let command = readdress_command(input.active_release_id, "readdress-complex-1");
        let build_job_id = lifecycle.start_readdress(command.clone()).await?;
        assert_eq!(
            lifecycle.start_readdress(command.clone()).await?,
            build_job_id
        );

        let build = sqlx::query(
            "SELECT kind, input_release_id, input_data_revision, frozen_source_snapshot_id,
                    input_serving_generation, readdress_tiles_base_url
             FROM catalog.vector_tile_build_job WHERE id = $1",
        )
        .bind(build_job_id.as_uuid())
        .fetch_one(&pool)
        .await?;
        assert_eq!(build.try_get::<String, _>("kind")?, "readdress");
        assert_eq!(
            build.try_get::<Uuid, _>("input_release_id")?,
            input.active_release_id.as_uuid()
        );
        assert_eq!(
            build.try_get::<Uuid, _>("input_data_revision")?,
            input.data_revision.as_uuid()
        );
        assert_eq!(
            build.try_get::<String, _>("frozen_source_snapshot_id")?,
            input.canonical_iceberg_snapshot_id.as_str()
        );
        assert_eq!(
            u64::try_from(build.try_get::<i64, _>("input_serving_generation")?)?,
            input.serving_generation.value()
        );
        assert_eq!(
            build.try_get::<String, _>("readdress_tiles_base_url")?,
            DESTINATION
        );

        let result = validated_result(build_job_id, DESTINATION)?;
        lifecycle.record_result(result.clone()).await?;
        lifecycle.record_result(result.clone()).await?;
        let promote = promotion_command(build_job_id, input, "promote-readdress-complex-1");
        let publisher = PromoteTileLayerStatic::new(uow);
        let published = publisher.execute(promote.clone()).await?;
        let selected = published_unit(&published, "complex")?;
        assert_eq!(
            selected.active_release_id,
            static_release_id_for_build(build_job_id)
        );
        assert_ne!(selected.active_release_id, input.active_release_id);
        assert_eq!(selected.data_revision, input.data_revision);
        assert_eq!(
            selected.canonical_iceberg_snapshot_id.as_str(),
            input.canonical_iceberg_snapshot_id.as_str()
        );
        assert_eq!(
            serde_json::to_value(&selected.layers)?,
            serde_json::to_value(&input.layers)?
        );
        assert_eq!(
            serde_json::to_value(&selected.lineage)?,
            serde_json::to_value(&input.lineage)?
        );
        assert_eq!(
            selected.serving_generation.value(),
            input.serving_generation.value() + 1
        );
        assert_eq!(
            published.manifest_generation.value(),
            original.manifest_generation.value() + 1
        );
        let (ActiveTileSource::StaticPmtiles(before), ActiveTileSource::StaticPmtiles(after)) =
            (&input.source, &selected.source)
        else {
            return Err("readdress must keep both releases static".into());
        };
        assert_eq!(after.pmtiles_sha256, before.pmtiles_sha256);
        assert_eq!(after.pmtiles_bytes, before.pmtiles_bytes);
        assert_ne!(after.pmtiles_object_key, before.pmtiles_object_key);
        assert_ne!(after.pmtiles_file_asset_id, before.pmtiles_file_asset_id);
        assert_eq!(
            after.tiles_url_template.as_str(),
            format!("{DESTINATION}/{}/{{z}}/{{x}}/{{y}}", after.martin_source_id)
        );

        let row = release_json(&pool, selected.active_release_id).await?;
        assert_eq!(
            row["readdressed_from_release_id"],
            json!(input.active_release_id.as_uuid())
        );
        for field in [
            "publication_unit_id", "data_revision", "canonical_iceberg_snapshot_id",
            "source_record_id", "source_file_asset_ids", "pmtiles_sha256", "pmtiles_bytes",
        ] {
            assert_eq!(row[field], original_row[field], "readdress changed {field}");
        }
        let fallback: Uuid = sqlx::query_scalar(
            "SELECT fallback_release_id FROM catalog.vector_tile_publication_unit WHERE unit_key = 'complex'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            fallback,
            published_unit(&dynamic, "complex")?.active_release_id.as_uuid()
        );
        assert_eq!(
            release_json(&pool, input.active_release_id).await?,
            original_row
        );

        let counts = ledger_counts(&pool).await?;
        assert_eq!(lifecycle.start_readdress(command).await?, build_job_id);
        lifecycle.record_result(result.clone()).await?;
        for report in [
            RecordVectorTileBuildResultCommand {
                operator_staff_id: StaffId::new(Uuid::new_v4()),
                ..result.clone()
            },
            RecordVectorTileBuildResultCommand {
                outcome: VectorTileBuildOutcome::Failed("late failure".to_owned()),
                ..result
            },
        ] {
            assert!(
                lifecycle.record_result(report).await.is_err(),
                "a promoted result may only be replayed exactly"
            );
        }
        let replay = publisher.execute(promote).await?;
        assert_eq!(
            serde_json::to_value(&replay)?,
            serde_json::to_value(&published)?
        );
        assert_eq!(ledger_counts(&pool).await?, counts);
        assert_eq!(
            active_pointer(&pool).await?,
            published.current_version.as_uuid()
        );
        let status: String = sqlx::query_scalar(
            "SELECT status FROM catalog.vector_tile_build_job WHERE id = $1",
        )
        .bind(build_job_id.as_uuid())
        .fetch_one(&pool)
        .await?;
        assert_eq!(status, "promoted");
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn readdress_refuses_wrong_input_unit_destination_and_changed_bytes() -> TestResult {
    run_in_disposable_database("tile_readdress_refusals", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let (dynamic, original) = seed_static_release(&pool).await?;
        let input = published_unit(&original, "complex")?;
        let lifecycle =
            VectorTileBuildLifecycle::new(Arc::new(PgCatalogUnitOfWork::new(pool.clone())));
        let counts = ledger_counts(&pool).await?;
        for command in [
            readdress_command(
                published_unit(&dynamic, "complex")?.active_release_id,
                "readdress-dynamic",
            ),
            readdress_command(
                VectorTileReleaseId::new(Uuid::new_v4()),
                "readdress-missing",
            ),
            StartStaticReleaseReaddressCommand {
                unit_key: "other".to_owned(),
                ..readdress_command(input.active_release_id, "readdress-wrong-unit")
            },
        ] {
            let error = lifecycle
                .start_readdress(command)
                .await
                .expect_err("only the active static release of this unit is admissible");
            assert!(
                matches!(error, CatalogError::InvalidVectorTileRuntimeManifest(_)),
                "got {error:?}"
            );
        }
        assert_eq!(ledger_counts(&pool).await?, counts);

        let command = readdress_command(input.active_release_id, "readdress-valid");
        let build_job_id = lifecycle.start_readdress(command.clone()).await?;
        let reused_key = StartStaticReleaseReaddressCommand {
            public_tiles_base_url: "https://another.example.test/tiles".to_owned(),
            ..command
        };
        assert!(matches!(
            lifecycle.start_readdress(reused_key).await,
            Err(CatalogError::MutationIdempotencyKeyReused { .. })
        ));

        let valid = validated_result(build_job_id, DESTINATION)?;
        let counts = ledger_counts(&pool).await?;
        for wrong_field in ["checksum", "size", "destination"] {
            let mut wrong = valid.clone();
            let VectorTileBuildOutcome::Validated { artifact, .. } = &mut wrong.outcome else {
                unreachable!("validated_result always constructs a validated outcome")
            };
            match wrong_field {
                "checksum" => artifact.checksum = PmtilesChecksum::new("e".repeat(64))?,
                "size" => artifact.size_bytes += 1,
                _ => {
                    artifact.tiles_url_template = RuntimeTilesUrlTemplate::new(format!(
                        "https://unclaimed.example.test/{}/{{z}}/{{x}}/{{y}}",
                        static_release_martin_source_id("complex", artifact.release_id)
                    ))?
                }
            }
            assert!(
                lifecycle.record_result(wrong).await.is_err(),
                "accepted changed {wrong_field}"
            );
            let status: String = sqlx::query_scalar(
                "SELECT status FROM catalog.vector_tile_build_job WHERE id = $1",
            )
            .bind(build_job_id.as_uuid())
            .fetch_one(&pool)
            .await?;
            assert_eq!(status, "running");
            assert_eq!(ledger_counts(&pool).await?, counts);
        }
        lifecycle.record_result(valid).await?;
        assert_eq!(
            active_pointer(&pool).await?,
            original.current_version.as_uuid()
        );
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_readdress_build_cannot_promote_after_the_active_revision_changes() -> TestResult {
    run_in_disposable_database("tile_readdress_stale_revision", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let (_, original) = seed_static_release(&pool).await?;
        let input = published_unit(&original, "complex")?;
        let uow = Arc::new(PgCatalogUnitOfWork::new(pool.clone()));
        let lifecycle = VectorTileBuildLifecycle::new(uow.clone());
        let build_job_id = lifecycle
            .start_readdress(readdress_command(
                input.active_release_id,
                "readdress-stale",
            ))
            .await?;
        lifecycle
            .record_result(validated_result(build_job_id, DESTINATION)?)
            .await?;
        let source_record_id = input.lineage.source_record_id.as_uuid();
        let next_revision =
            seed_data_revision(&pool, "complex", NEXT_SNAPSHOT, source_record_id).await?;
        let current = use_case(&pool, RuntimeManifestPublicationCapability::disabled())
            .execute(activation_command(
                "complex",
                next_revision,
                NEXT_SNAPSHOT,
                source_record_id,
                Some((input.active_release_id, input.serving_generation)),
            )?)
            .await?;
        let counts = ledger_counts(&pool).await?;
        let error = PromoteTileLayerStatic::new(uow)
            .execute(promotion_command(
                build_job_id,
                input,
                "promote-stale-readdress",
            ))
            .await
            .expect_err("the observed current release cannot replace a build's frozen input");
        assert!(
            matches!(
                error,
                CatalogError::InvalidVectorTileRuntimeManifest(_)
                    | CatalogError::VectorTileServingStateConflict { .. }
            ),
            "got {error:?}"
        );
        assert_eq!(ledger_counts(&pool).await?, counts);
        assert_eq!(
            active_pointer(&pool).await?,
            current.current_version.as_uuid()
        );
        let status: String =
            sqlx::query_scalar("SELECT status FROM catalog.vector_tile_build_job WHERE id = $1")
                .bind(build_job_id.as_uuid())
                .fetch_one(&pool)
                .await?;
        assert_eq!(status, "superseded");
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn reactivating_the_same_release_cannot_replace_a_builds_captured_generation() -> TestResult {
    run_in_disposable_database("tile_readdress_reactivation", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let (_, original) = seed_static_release(&pool).await?;
        let input = published_unit(&original, "complex")?;
        let uow = Arc::new(PgCatalogUnitOfWork::new(pool.clone()));
        let lifecycle = VectorTileBuildLifecycle::new(uow.clone());
        let pending = lifecycle
            .start_readdress(readdress_command(input.active_release_id, "readdress-before-return"))
            .await?;
        lifecycle.record_result(validated_result(pending, DESTINATION)?).await?;
        let sibling = lifecycle
            .start_readdress(readdress_command(input.active_release_id, "readdress-before-sibling"))
            .await?;
        lifecycle.record_result(validated_result(sibling, DESTINATION)?).await?;
        let publisher = PromoteTileLayerStatic::new(uow);
        let moved = publisher
            .execute(promotion_command(sibling, input, "promote-sibling-readdress"))
            .await?;

        // Re-select preserved S1 through the real gate. The pointer returns to the same release,
        // but the serving generation now proves that two source switches occurred after start.
        let returned_manifest = seed_bare_manifest(&pool).await?;
        let returned_generation: i64 = sqlx::query_scalar(
            "INSERT INTO catalog.vector_tile_runtime_manifest_unit
                 (manifest_id, publication_unit_id, release_id, serving_generation,
                  data_revision, canonical_iceberg_snapshot_id)
             SELECT $1, release.publication_unit_id, release.id, unit.serving_generation + 1,
                    release.data_revision, release.canonical_iceberg_snapshot_id
             FROM catalog.vector_tile_release AS release
             JOIN catalog.vector_tile_publication_unit AS unit ON unit.id = release.publication_unit_id
             WHERE release.id = $2
             RETURNING serving_generation",
        ).bind(returned_manifest).bind(input.active_release_id.as_uuid()).fetch_one(&pool).await?;
        sqlx::query("SELECT catalog.promote_vector_tile_runtime_manifest($1, $2)")
            .bind(moved.current_version.as_uuid()).bind(returned_manifest).execute(&pool).await?;
        assert_eq!(u64::try_from(returned_generation)?, input.serving_generation.value() + 2);

        let counts = ledger_counts(&pool).await?;
        let mut command = promotion_command(pending, input, "promote-after-return");
        command.expected_serving_generation = ServingGeneration::new(u64::try_from(returned_generation)?)?;
        let error = publisher.execute(command).await
            .expect_err("a current generation cannot replace the build's frozen observation");
        assert!(matches!(error, CatalogError::InvalidVectorTileRuntimeManifest(_)), "got {error:?}");
        assert_eq!(ledger_counts(&pool).await?, counts);
        assert_eq!(active_pointer(&pool).await?, returned_manifest);
        let status: String = sqlx::query_scalar("SELECT status FROM catalog.vector_tile_build_job WHERE id = $1")
            .bind(pending.as_uuid()).fetch_one(&pool).await?;
        assert_eq!(status, "validated");
        Ok(())
    }).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn the_schema_preserves_bake_uniqueness_and_allows_multiple_readdress_destinations(
) -> TestResult {
    run_in_disposable_database("tile_readdress_uniqueness", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let (_, original) = seed_static_release(&pool).await?;
        let input = published_unit(&original, "complex")?.active_release_id;
        let error = clone_release(&pool, input, None, "https://duplicate.example.test", json!({})).await
            .expect_err("a second ordinary static release of this revision remains forbidden");
        assert_database_error(&error, "23505", Some("vector_tile_release_unit_revision_snapshot_kind_key"));

        let first = clone_release(&pool, input, Some(input), DESTINATION, json!({})).await?;
        clone_release(&pool, input, Some(input), "https://second.example.test/tiles", json!({})).await?;
        let descendants: i64 = sqlx::query_scalar("SELECT count(*) FROM catalog.vector_tile_release WHERE readdressed_from_release_id = $1")
            .bind(input.as_uuid()).fetch_one(&pool).await?;
        assert_eq!(descendants, 2, "a source-only unique key would forbid the second destination");

        let first_row = release_json(&pool, first).await?;
        let error = clone_release(&pool, input, Some(input), DESTINATION, json!({
            "martin_source_id": first_row["martin_source_id"],
            "tiles_url_template": first_row["tiles_url_template"],
        })).await.expect_err("the exact source/destination pair must remain unique");
        assert_database_error(&error, "23505", Some("vector_tile_release_readdress_destination_key"));
        assert_eq!(active_pointer(&pool).await?, original.current_version.as_uuid());
        Ok(())
    }).await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn direct_sql_cannot_readdress_a_wrong_source_or_change_its_bytes_lineage_or_selection(
) -> TestResult {
    run_in_disposable_database("tile_readdress_schema_binding", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let (dynamic, original) = seed_static_release(&pool).await?;
        let input = published_unit(&original, "complex")?;
        let dynamic_id = published_unit(&dynamic, "complex")?.active_release_id;
        for patch in [
            json!({"readdressed_from_release_id": dynamic_id.as_uuid()}),
            json!({"readdressed_from_release_id": Uuid::new_v4()}),
            json!({"pmtiles_sha256": "e".repeat(64)}),
            json!({"pmtiles_bytes": ARTIFACT_SIZE + 1}),
            json!({"source_record_id": Uuid::new_v4()}),
            json!({"source_file_asset_ids": []}),
        ] {
            let error = clone_release(
                &pool,
                input.active_release_id,
                Some(input.active_release_id),
                DESTINATION,
                patch,
            )
            .await
            .expect_err("direct SQL must preserve a validated static source's bytes and lineage");
            assert_database_error(&error, "23514", None);
        }

        let source_record_id = input.lineage.source_record_id.as_uuid();
        let revision =
            seed_data_revision(&pool, "complex", NEXT_SNAPSHOT, source_record_id).await?;
        let other_unit = seed_publication_unit(&pool, "other").await?;
        let other_revision =
            seed_data_revision(&pool, "other", FOURTH_SNAPSHOT, source_record_id).await?;
        for patch in [
            json!({"data_revision": revision, "canonical_iceberg_snapshot_id": NEXT_SNAPSHOT}),
            json!({"publication_unit_id": other_unit, "data_revision": other_revision,
                "canonical_iceberg_snapshot_id": FOURTH_SNAPSHOT}),
        ] {
            let error = clone_release(
                &pool,
                input.active_release_id,
                Some(input.active_release_id),
                DESTINATION,
                patch,
            )
            .await
            .expect_err("a valid revision of a different selection cannot borrow this source");
            assert_database_error(
                &error,
                "23503",
                Some("vector_tile_release_readdress_binding_fkey"),
            );
        }

        // Legacy release CHECKs admit SQL UNKNOWN for nullable metadata. Neither new trigger may
        // mistake such a row for a validated source merely because validated_at is present.
        let original_row = release_json(&pool, input.active_release_id).await?;
        let lifecycle =
            VectorTileBuildLifecycle::new(Arc::new(PgCatalogUnitOfWork::new(pool.clone())));
        let counts = ledger_counts(&pool).await?;
        for patch in [
            json!({"pmtiles_sha256": null}),
            json!({"pmtiles_bytes": null}),
            json!({"validation_evidence_sha256": null}),
        ] {
            replace_source_metadata(&pool, input.active_release_id, &patch).await?;
            let error = clone_release(
                &pool,
                input.active_release_id,
                Some(input.active_release_id),
                DESTINATION,
                json!({}),
            )
            .await
            .expect_err(
                "a release must not inherit NULL metadata from a supposedly validated source",
            );
            assert_database_error(&error, "23514", None);
            assert!(
                lifecycle
                    .start_readdress(readdress_command(
                        input.active_release_id,
                        "readdress-null-source"
                    ))
                    .await
                    .is_err(),
                "a build must not start from NULL validation metadata"
            );
            replace_source_metadata(&pool, input.active_release_id, &original_row).await?;
            assert_eq!(ledger_counts(&pool).await?, counts);
        }
        assert_eq!(
            release_json(&pool, input.active_release_id).await?,
            original_row
        );
        assert_eq!(release_count(&pool).await?, 2);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_readdress_build_freezes_its_source_selection_and_destination_in_the_database(
) -> TestResult {
    run_in_disposable_database("tile_readdress_build_binding", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let (_, original) = seed_static_release(&pool).await?;
        let input = published_unit(&original, "complex")?;
        let lifecycle =
            VectorTileBuildLifecycle::new(Arc::new(PgCatalogUnitOfWork::new(pool.clone())));
        let build_job_id = lifecycle
            .start_readdress(readdress_command(
                input.active_release_id,
                "readdress-frozen-input",
            ))
            .await?;
        let before: serde_json::Value = sqlx::query_scalar(
            "SELECT to_jsonb(build) FROM catalog.vector_tile_build_job AS build WHERE id = $1",
        )
        .bind(build_job_id.as_uuid())
        .fetch_one(&pool)
        .await?;
        for patch in [
            json!({"input_release_id": Uuid::new_v4()}),
            json!({"input_data_revision": Uuid::new_v4()}),
            json!({"frozen_source_snapshot_id": NEXT_SNAPSHOT}),
            json!({"input_serving_generation": input.serving_generation.value() + 1}),
            json!({"readdress_tiles_base_url": "https://unclaimed.example.test"}),
            json!({"kind": "bake"}),
        ] {
            let error = sqlx::query(
                "UPDATE catalog.vector_tile_build_job AS build
                 SET (kind, input_release_id, input_data_revision, frozen_source_snapshot_id,
                      input_serving_generation, readdress_tiles_base_url) = (
                     SELECT changed.kind, changed.input_release_id, changed.input_data_revision,
                            changed.frozen_source_snapshot_id, changed.input_serving_generation,
                            changed.readdress_tiles_base_url
                     FROM jsonb_populate_record(build, $2::jsonb) AS changed
                 ) WHERE build.id = $1",
            )
            .bind(build_job_id.as_uuid())
            .bind(patch)
            .execute(&pool)
            .await
            .expect_err("readdress inputs must stay frozen even for direct SQL updates");
            assert_database_error(&error, "23514", None);
        }
        let after: serde_json::Value = sqlx::query_scalar(
            "SELECT to_jsonb(build) FROM catalog.vector_tile_build_job AS build WHERE id = $1",
        )
        .bind(build_job_id.as_uuid())
        .fetch_one(&pool)
        .await?;
        assert_eq!(after, before);
        Ok(())
    })
    .await
}

/// Creates S1 through the production bake path; the existing fixture owns revision/projection facts.
async fn seed_static_release(
    pool: &PgPool,
) -> TestResult<(VectorTileRuntimeManifest, VectorTileRuntimeManifest)> {
    let source_record_id = seed_source_record(pool).await?;
    seed_publication_unit(pool, "complex").await?;
    let revision = seed_data_revision(pool, "complex", COMPLEX_SNAPSHOT, source_record_id).await?;
    let dynamic = use_case(pool, RuntimeManifestPublicationCapability::disabled())
        .execute(activation_command(
            "complex",
            revision,
            COMPLEX_SNAPSHOT,
            source_record_id,
            None,
        )?)
        .await?;
    let input = published_unit(&dynamic, "complex")?;
    let uow = Arc::new(PgCatalogUnitOfWork::new(pool.clone()));
    let lifecycle = VectorTileBuildLifecycle::new(uow.clone());
    let build_job_id = lifecycle
        .start(StartVectorTileBuildCommand {
            unit_key: "complex".to_owned(),
            input_release_id: input.active_release_id,
            input_data_revision: input.data_revision,
            frozen_source_snapshot_id: input.canonical_iceberg_snapshot_id.clone(),
            idempotency_key: "bake-original-static".to_owned(),
            operator_staff_id: StaffId::new(OPERATOR_STAFF_ID),
        })
        .await?;
    lifecycle
        .record_result(validated_result(
            build_job_id,
            "https://tiles.example.test",
        )?)
        .await?;
    let original = PromoteTileLayerStatic::new(uow)
        .execute(promotion_command(
            build_job_id,
            input,
            "promote-original-static",
        ))
        .await?;
    let bake: (String, Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT kind, input_serving_generation, readdress_tiles_base_url
         FROM catalog.vector_tile_build_job WHERE id = $1",
    )
    .bind(build_job_id.as_uuid())
    .fetch_one(pool)
    .await?;
    assert_eq!(bake, ("bake".to_owned(), None, None));
    Ok((dynamic, original))
}

fn readdress_command(
    input_release_id: VectorTileReleaseId,
    key: &str,
) -> StartStaticReleaseReaddressCommand {
    StartStaticReleaseReaddressCommand {
        unit_key: "complex".to_owned(),
        input_release_id,
        public_tiles_base_url: DESTINATION.to_owned(),
        idempotency_key: key.to_owned(),
        operator_staff_id: StaffId::new(OPERATOR_STAFF_ID),
    }
}

fn validated_result(
    build_job_id: VectorTileBuildJobId,
    base_url: &str,
) -> TestResult<RecordVectorTileBuildResultCommand> {
    let release_id = static_release_id_for_build(build_job_id);
    let source_id = static_release_martin_source_id("complex", release_id);
    Ok(RecordVectorTileBuildResultCommand {
        build_job_id,
        outcome: VectorTileBuildOutcome::Validated {
            evidence: BuildEvidenceDigest::new("d".repeat(64))?,
            artifact: ValidatedPmtilesArtifact {
                release_id,
                file_asset_id: static_file_asset_id_for_build(build_job_id),
                object_key: static_release_pmtiles_object_key("complex", release_id),
                tiles_url_template: RuntimeTilesUrlTemplate::new(format!(
                    "{base_url}/{source_id}/{{z}}/{{x}}/{{y}}"
                ))?,
                checksum: PmtilesChecksum::new("c".repeat(64))?,
                size_bytes: ARTIFACT_SIZE,
            },
        },
        operator_staff_id: StaffId::new(OPERATOR_STAFF_ID),
    })
}

fn promotion_command(
    build_job_id: VectorTileBuildJobId,
    input: &catalog_domain::PublicationUnit,
    key: &str,
) -> PromoteTileLayerStaticCommand {
    PromoteTileLayerStaticCommand {
        unit_key: "complex".to_owned(),
        build_job_id,
        expected_active_release_id: input.active_release_id,
        expected_serving_generation: input.serving_generation,
        idempotency_key: key.to_owned(),
        operator_staff_id: StaffId::new(OPERATOR_STAFF_ID),
    }
}

async fn release_json(pool: &PgPool, id: VectorTileReleaseId) -> TestResult<serde_json::Value> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(release) FROM catalog.vector_tile_release AS release WHERE id = $1",
    )
    .bind(id.as_uuid())
    .fetch_one(pool)
    .await?)
}

async fn ledger_counts(pool: &PgPool) -> TestResult<(i64, i64, i64, i64, i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT (SELECT count(*) FROM catalog.vector_tile_release),
                (SELECT count(*) FROM catalog.vector_tile_build_job),
                (SELECT count(*) FROM catalog.vector_tile_runtime_manifest),
                (SELECT count(*) FROM catalog.catalog_mutation_idempotency),
                (SELECT count(*) FROM catalog.file_asset),
                (SELECT count(*) FROM catalog.outbox_event)",
    )
    .fetch_one(pool)
    .await?)
}

async fn replace_source_metadata(
    pool: &PgPool,
    id: VectorTileReleaseId,
    patch: &serde_json::Value,
) -> TestResult {
    sqlx::query(
        "UPDATE catalog.vector_tile_release AS release
         SET (pmtiles_sha256, pmtiles_bytes, validation_evidence_sha256) = (
             SELECT changed.pmtiles_sha256, changed.pmtiles_bytes, changed.validation_evidence_sha256
             FROM jsonb_populate_record(release, $2::jsonb) AS changed
         ) WHERE release.id = $1",
    )
    .bind(id.as_uuid())
    .bind(patch)
    .execute(pool)
    .await?;
    Ok(())
}

/// Copies seeded fixture rows so each rejected patch differs only along the invariant under test.
async fn clone_release(
    pool: &PgPool,
    input: VectorTileReleaseId,
    parent: Option<VectorTileReleaseId>,
    base_url: &str,
    patch: serde_json::Value,
) -> Result<VectorTileReleaseId, sqlx::Error> {
    let id = VectorTileReleaseId::new(Uuid::new_v4());
    let source_id = static_release_martin_source_id("complex", id);
    let mut changes = json!({
        "id": id.as_uuid(),
        "martin_source_id": source_id,
        "tiles_url_template": format!("{base_url}/{source_id}/{{z}}/{{x}}/{{y}}"),
        "pmtiles_object_key": static_release_pmtiles_object_key("complex", id),
        "readdressed_from_release_id": parent.map(|release| release.as_uuid()),
    });
    changes
        .as_object_mut()
        .expect("changes is an object")
        .extend(patch.as_object().expect("patch is an object").clone());
    sqlx::query(
        "INSERT INTO catalog.vector_tile_release
         SELECT changed.* FROM catalog.vector_tile_release AS source
         CROSS JOIN LATERAL jsonb_populate_record(
             NULL::catalog.vector_tile_release, to_jsonb(source) || $2::jsonb
         ) AS changed WHERE source.id = $1",
    )
    .bind(input.as_uuid())
    .bind(changes)
    .execute(pool)
    .await?;
    Ok(id)
}

fn assert_database_error(error: &sqlx::Error, code: &str, constraint: Option<&str>) {
    let database = error
        .as_database_error()
        .expect("expected a PostgreSQL constraint error");
    assert_eq!(database.code().as_deref(), Some(code), "got {error:?}");
    if let Some(constraint) = constraint {
        assert_eq!(database.constraint(), Some(constraint), "got {error:?}");
    }
}
