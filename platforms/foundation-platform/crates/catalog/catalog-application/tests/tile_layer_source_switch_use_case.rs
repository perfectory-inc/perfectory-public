//! Static promotion and same-revision rollback use-case tests.
//!
//! Both commands move the serving pointer, and both leave the decisions that need locked state to
//! `catalog-infrastructure`: whether the build is still promotable, and what the recorded fallback
//! is. What this layer owns is that the command's own fields agree with each other.

mod common;

use std::sync::Arc;

use catalog_application::ports::{PromoteTileLayerStaticCommand, RollbackTileLayerSourceCommand};
use catalog_application::{PromoteTileLayerStatic, RollbackTileLayerSource};
use catalog_domain::{
    static_release_martin_source_id, BuildEvidenceDigest, CanonicalIcebergSnapshotId, CatalogError,
    PmtilesChecksum, RuntimeTilesUrlTemplate, ServingGeneration,
};
use foundation_shared_kernel::ids::{
    FileAssetId, StaffId, VectorTileBuildJobId, VectorTileReleaseId,
};
use uuid::Uuid;

use common::{unused, RecordingUnitOfWork, SilentUnitOfWork};

const fn invalid(message: String) -> CatalogError {
    CatalogError::InvalidVectorTileRuntimeManifest(message)
}

/// A promotion whose URL template addresses the release-derived Martin source, with padded text so
/// trimming is observable at the port.
fn promotion() -> Result<PromoteTileLayerStaticCommand, CatalogError> {
    let release_id = VectorTileReleaseId::new(Uuid::now_v7());
    let martin_source_id = static_release_martin_source_id("parcels", release_id);
    Ok(PromoteTileLayerStaticCommand {
        unit_key: " parcels ".to_owned(),
        build_job_id: VectorTileBuildJobId::new(Uuid::now_v7()),
        release_id,
        expected_active_release_id: VectorTileReleaseId::new(Uuid::now_v7()),
        expected_serving_generation: ServingGeneration::new(4).map_err(invalid)?,
        input_release_id: VectorTileReleaseId::new(Uuid::now_v7()),
        frozen_source_snapshot_id: CanonicalIcebergSnapshotId::new("70000000000000001".to_owned())
            .map_err(invalid)?,
        validation_evidence: BuildEvidenceDigest::new("d".repeat(64)).map_err(invalid)?,
        tiles_url_template: RuntimeTilesUrlTemplate::new(format!(
            "https://tiles.example.com/{martin_source_id}/{{z}}/{{x}}/{{y}}"
        ))
        .map_err(invalid)?,
        pmtiles_file_asset_id: FileAssetId::new(Uuid::now_v7()),
        pmtiles_sha256: PmtilesChecksum::new("e".repeat(64)).map_err(invalid)?,
        pmtiles_bytes: 987_654_321,
        idempotency_key: " promotion-1 ".to_owned(),
        operator_staff_id: StaffId::new(Uuid::now_v7()),
    })
}

fn rollback() -> Result<RollbackTileLayerSourceCommand, CatalogError> {
    Ok(RollbackTileLayerSourceCommand {
        unit_key: " parcels ".to_owned(),
        expected_active_release_id: VectorTileReleaseId::new(Uuid::now_v7()),
        expected_serving_generation: ServingGeneration::new(5).map_err(invalid)?,
        reason: "  first-tile latency regressed past the SLO  ".to_owned(),
        idempotency_key: " rollback-1 ".to_owned(),
        operator_staff_id: StaffId::new(Uuid::now_v7()),
    })
}

async fn promotion_refusal(command: PromoteTileLayerStaticCommand) -> Result<String, CatalogError> {
    let uow = Arc::new(RecordingUnitOfWork::default());
    let error = PromoteTileLayerStatic::new(uow.clone())
        .execute(command)
        .await
        .err();
    assert!(
        uow.static_promotions()?.is_empty(),
        "a refused promotion must not reach the unit of work"
    );
    Ok(error.map(|error| error.to_string()).unwrap_or_default())
}

#[tokio::test]
async fn a_promotion_is_normalized_and_publishes_the_derived_static_source(
) -> Result<(), CatalogError> {
    let uow = Arc::new(RecordingUnitOfWork::default());
    let command = promotion()?;
    let expected_source_id = static_release_martin_source_id("parcels", command.release_id);

    let manifest = PromoteTileLayerStatic::new(uow.clone())
        .execute(command)
        .await?;

    let recorded = uow.static_promotions()?;
    assert_eq!(recorded.len(), 1);
    let sent = recorded
        .first()
        .ok_or_else(|| unused("recorded promotion"))?;
    assert_eq!(sent.unit_key, "parcels");
    assert_eq!(sent.idempotency_key, "promotion-1");

    // The manifest the double builds derives the Martin id and the object key rather than echoing
    // the command, and it goes through the domain validator — so this asserts the derived identity
    // and the published URL agree.
    let unit = manifest
        .publication_units
        .get("parcels")
        .ok_or_else(|| unused("promoted unit"))?;
    assert_eq!(unit.source.martin_source_id(), expected_source_id);
    assert_eq!(unit.serving_generation.value(), 5);
    Ok(())
}

#[tokio::test]
async fn a_promotion_that_republishes_the_observed_release_is_refused() -> Result<(), CatalogError>
{
    let mut command = promotion()?;
    command.expected_active_release_id = command.release_id;

    let message = promotion_refusal(command).await?;
    assert!(
        message.contains("must differ from expected_active_release_id"),
        "got: {message}"
    );
    Ok(())
}

#[tokio::test]
async fn an_empty_pmtiles_artifact_is_refused_by_name() -> Result<(), CatalogError> {
    let mut command = promotion()?;
    command.pmtiles_bytes = 0;

    let message = promotion_refusal(command).await?;
    assert!(
        message.contains("pmtiles_bytes must be greater than zero"),
        "got: {message}"
    );
    Ok(())
}

#[tokio::test]
async fn a_promotion_url_addressing_the_unit_instead_of_the_release_is_refused(
) -> Result<(), CatalogError> {
    let mut command = promotion()?;
    // The unit-addressed form a dynamic source uses. For a static release the Martin source is
    // release-addressed, so this URL would serve a different source than the pointer claims.
    command.tiles_url_template =
        RuntimeTilesUrlTemplate::new("https://tiles.example.com/parcels/{z}/{x}/{y}".to_owned())
            .map_err(invalid)?;

    let message = promotion_refusal(command).await?;
    assert!(
        message.contains("must address the release-derived Martin source"),
        "got: {message}"
    );
    Ok(())
}

#[tokio::test]
async fn a_rollback_is_normalized_before_it_reaches_the_unit_of_work() -> Result<(), CatalogError> {
    let uow = Arc::new(RecordingUnitOfWork::default());
    // The double has no fallback state to project a manifest from, so it records and then errors.
    let _ = RollbackTileLayerSource::new(uow.clone())
        .execute(rollback()?)
        .await;

    let recorded = uow.rollbacks()?;
    assert_eq!(recorded.len(), 1);
    let sent = recorded
        .first()
        .ok_or_else(|| unused("recorded rollback"))?;
    assert_eq!(sent.unit_key, "parcels");
    assert_eq!(sent.reason, "first-tile latency regressed past the SLO");
    assert_eq!(sent.idempotency_key, "rollback-1");
    Ok(())
}

#[tokio::test]
async fn a_rollback_without_a_reason_is_refused() -> Result<(), CatalogError> {
    let uow = Arc::new(RecordingUnitOfWork::default());
    let mut command = rollback()?;
    command.reason = "   ".to_owned();

    let message = RollbackTileLayerSource::new(uow.clone())
        .execute(command)
        .await
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();

    assert!(
        message.contains("reason must not be empty"),
        "got: {message}"
    );
    assert!(
        uow.rollbacks()?.is_empty(),
        "a refused rollback must not reach the unit of work"
    );
    Ok(())
}

#[tokio::test]
async fn a_unit_of_work_that_ignores_the_switch_cannot_report_a_publication(
) -> Result<(), CatalogError> {
    let promotion_error = PromoteTileLayerStatic::new(Arc::new(SilentUnitOfWork))
        .execute(promotion()?)
        .await
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(
        promotion_error.contains("not implemented by this Catalog unit of work"),
        "got: {promotion_error}"
    );

    let rollback_error = RollbackTileLayerSource::new(Arc::new(SilentUnitOfWork))
        .execute(rollback()?)
        .await
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(
        rollback_error.contains("not implemented by this Catalog unit of work"),
        "got: {rollback_error}"
    );
    Ok(())
}
