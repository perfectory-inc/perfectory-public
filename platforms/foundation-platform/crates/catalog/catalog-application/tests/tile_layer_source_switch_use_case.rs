//! Static promotion and same-revision rollback use-case tests.
//!
//! Both commands move the serving pointer, and both leave the decisions that need locked state to
//! `catalog-infrastructure`: whether the build is still promotable, and what the recorded fallback
//! is. What this layer owns is that the command's own fields agree with each other.

mod common;

use std::sync::Arc;

use catalog_application::ports::{PromoteTileLayerStaticCommand, RollbackTileLayerSourceCommand};
use catalog_application::{PromoteTileLayerStatic, RollbackTileLayerSource};
use catalog_domain::{CatalogError, ServingGeneration};
use foundation_shared_kernel::ids::{StaffId, VectorTileBuildJobId, VectorTileReleaseId};
use uuid::Uuid;

use common::{unused, RecordingUnitOfWork, SilentUnitOfWork};

const fn invalid(message: String) -> CatalogError {
    CatalogError::InvalidVectorTileRuntimeManifest(message)
}

/// A promotion whose URL template addresses the release-derived Martin source, with padded text so
/// trimming is observable at the port.
fn promotion() -> Result<PromoteTileLayerStaticCommand, CatalogError> {
    Ok(PromoteTileLayerStaticCommand {
        unit_key: " parcels ".to_owned(),
        build_job_id: VectorTileBuildJobId::new(Uuid::now_v7()),
        expected_active_release_id: VectorTileReleaseId::new(Uuid::now_v7()),
        expected_serving_generation: ServingGeneration::new(4).map_err(invalid)?,
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

#[tokio::test]
async fn a_promotion_is_normalized_and_publishes_the_derived_static_source(
) -> Result<(), CatalogError> {
    let uow = Arc::new(RecordingUnitOfWork::default());
    let command = promotion()?;
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
    assert!(matches!(
        unit.source,
        catalog_domain::ActiveTileSource::StaticPmtiles(_)
    ));
    assert_eq!(unit.serving_generation.value(), 5);
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
