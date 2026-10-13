//! Every id in a release's `source_file_asset_ids` names a `catalog.file_asset` row, and that row
//! cannot be deleted while a release names it (root ADR-0231).

use super::readdress::assert_database_error;
use super::*;

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_release_cannot_name_a_file_asset_that_does_not_exist() -> TestResult {
    run_in_disposable_database("tile_release_missing_asset", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;
        seed_publication_unit(&pool, "parcels").await?;
        let revision =
            seed_data_revision(&pool, "parcels", PARCELS_SNAPSHOT, source_record_id).await?;
        let mut command = activation_command(
            "parcels",
            revision,
            PARCELS_SNAPSHOT,
            source_record_id,
            None,
        )?;
        command
            .lineage
            .source_file_asset_ids
            .push(FileAssetId::new(Uuid::new_v4()));

        let error = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(command)
            .await
            .expect_err("a release naming a missing file asset must be refused");

        let message = error.to_string();
        assert!(
            message.contains("vector_tile_release_source_file_asset_file_asset_fkey"),
            "{message}"
        );
        assert_eq!(release_count(&pool).await?, 0);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_file_asset_a_release_names_cannot_be_deleted() -> TestResult {
    run_in_disposable_database("tile_release_asset_delete", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;
        seed_publication_unit(&pool, "parcels").await?;
        let revision =
            seed_data_revision(&pool, "parcels", PARCELS_SNAPSHOT, source_record_id).await?;
        use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(activation_command(
                "parcels",
                revision,
                PARCELS_SNAPSHOT,
                source_record_id,
                None,
            )?)
            .await?;

        let error = sqlx::query("DELETE FROM catalog.file_asset WHERE id = $1")
            .bind(derived_id(revision, 2))
            .execute(&pool)
            .await
            .expect_err("a named file asset must not be deletable");
        assert_database_error(
            &error,
            "23503",
            Some("vector_tile_release_source_file_asset_file_asset_fkey"),
        );
        Ok(())
    })
    .await
}
