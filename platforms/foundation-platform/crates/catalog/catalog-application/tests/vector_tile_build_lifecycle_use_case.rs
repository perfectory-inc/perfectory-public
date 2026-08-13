//! Static vector tile build lifecycle use-case tests.
//!
//! The frozen-snapshot binding and the terminal-status refusal need state only the transaction
//! holds, so they are `catalog-infrastructure`'s to prove. What this layer owns is narrower: the
//! publication capability, text normalisation, and the fact that a build cannot report a status the
//! promotion decision owns.

mod common;

use std::sync::Arc;

use catalog_application::ports::{RecordVectorTileBuildResultCommand, StartVectorTileBuildCommand};
use catalog_application::VectorTileBuildLifecycle;
use catalog_domain::{
    BuildEvidenceDigest, CanonicalIcebergSnapshotId, CatalogError, VectorTileBuildOutcome,
    VectorTileBuildStatus,
};
use foundation_shared_kernel::ids::{
    StaffId, VectorTileBuildJobId, VectorTileDataRevisionId, VectorTileReleaseId,
};
use uuid::Uuid;

use common::{unused, RecordingUnitOfWork, SilentUnitOfWork};

/// A build start with padded text, so trimming is observable at the port.
fn start_command() -> Result<StartVectorTileBuildCommand, CatalogError> {
    Ok(StartVectorTileBuildCommand {
        unit_key: " parcels ".to_owned(),
        input_release_id: VectorTileReleaseId::new(Uuid::now_v7()),
        input_data_revision: VectorTileDataRevisionId::new(Uuid::now_v7()),
        frozen_source_snapshot_id: CanonicalIcebergSnapshotId::new("70000000000000001".to_owned())
            .map_err(CatalogError::InvalidVectorTileRuntimeManifest)?,
        idempotency_key: " build-1 ".to_owned(),
        operator_staff_id: StaffId::new(Uuid::now_v7()),
    })
}

fn result_command(outcome: VectorTileBuildOutcome) -> RecordVectorTileBuildResultCommand {
    RecordVectorTileBuildResultCommand {
        build_job_id: VectorTileBuildJobId::new(Uuid::now_v7()),
        outcome,
        operator_staff_id: StaffId::new(Uuid::now_v7()),
    }
}

fn evidence() -> Result<BuildEvidenceDigest, CatalogError> {
    BuildEvidenceDigest::new("c".repeat(64)).map_err(CatalogError::InvalidVectorTileRuntimeManifest)
}

fn lifecycle(uow: Arc<RecordingUnitOfWork>) -> VectorTileBuildLifecycle {
    VectorTileBuildLifecycle::new(uow)
}

#[tokio::test]
async fn a_started_build_is_normalized_before_it_reaches_the_unit_of_work(
) -> Result<(), CatalogError> {
    let uow = Arc::new(RecordingUnitOfWork::default());
    lifecycle(uow.clone()).start(start_command()?).await?;

    let recorded = uow.started_builds()?;
    assert_eq!(recorded.len(), 1);
    let command = recorded.first().ok_or_else(|| unused("recorded start"))?;
    assert_eq!(command.unit_key, "parcels");
    assert_eq!(command.idempotency_key, "build-1");
    Ok(())
}

#[tokio::test]
async fn a_build_start_without_an_idempotency_key_is_refused() -> Result<(), CatalogError> {
    let uow = Arc::new(RecordingUnitOfWork::default());
    let mut command = start_command()?;
    command.idempotency_key = "   ".to_owned();

    let message = lifecycle(uow.clone())
        .start(command)
        .await
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();

    assert!(
        message.contains("idempotency_key must not be empty"),
        "got: {message}"
    );
    assert!(
        uow.started_builds()?.is_empty(),
        "a refused start must not reach the unit of work"
    );
    Ok(())
}

#[tokio::test]
async fn a_reported_failure_must_say_why() -> Result<(), CatalogError> {
    let uow = Arc::new(RecordingUnitOfWork::default());

    let message = lifecycle(uow.clone())
        .record_result(result_command(VectorTileBuildOutcome::Failed(
            "  ".to_owned(),
        )))
        .await
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();

    assert!(
        message.contains("failure reason must not be empty"),
        "got: {message}"
    );
    assert!(uow.recorded_results()?.is_empty());
    Ok(())
}

#[tokio::test]
async fn a_reported_outcome_reaches_the_unit_of_work_trimmed() -> Result<(), CatalogError> {
    let uow = Arc::new(RecordingUnitOfWork::default());
    let use_case = lifecycle(uow.clone());

    use_case
        .record_result(result_command(VectorTileBuildOutcome::Failed(
            "  tippecanoe exited 1  ".to_owned(),
        )))
        .await?;
    use_case
        .record_result(result_command(VectorTileBuildOutcome::Validated(
            evidence()?
        )))
        .await?;

    let recorded = uow.recorded_results()?;
    assert_eq!(recorded.len(), 2);
    let statuses: Vec<VectorTileBuildStatus> = recorded
        .iter()
        .map(|command| command.outcome.status())
        .collect();
    assert_eq!(
        statuses,
        vec![
            VectorTileBuildStatus::Failed,
            VectorTileBuildStatus::Validated
        ]
    );
    let first = recorded.first().ok_or_else(|| unused("recorded result"))?;
    assert_eq!(
        first.outcome,
        VectorTileBuildOutcome::Failed("tippecanoe exited 1".to_owned())
    );
    Ok(())
}

#[tokio::test]
async fn a_unit_of_work_that_ignores_the_build_ledger_cannot_report_success(
) -> Result<(), CatalogError> {
    let use_case = VectorTileBuildLifecycle::new(Arc::new(SilentUnitOfWork));

    let start = use_case
        .start(start_command()?)
        .await
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(
        start.contains("not implemented by this Catalog unit of work"),
        "got: {start}"
    );

    let record = use_case
        .record_result(result_command(VectorTileBuildOutcome::Validated(
            evidence()?
        )))
        .await
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(
        record.contains("not implemented by this Catalog unit of work"),
        "got: {record}"
    );
    Ok(())
}
