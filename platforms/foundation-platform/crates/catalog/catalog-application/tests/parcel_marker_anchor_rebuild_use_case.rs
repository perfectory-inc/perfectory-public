//! Parcel marker anchor rebuild use-case tests.

use std::sync::Mutex;

use async_trait::async_trait;
use catalog_application::ports::{
    ParcelMarkerAnchorRebuildCommand, ParcelMarkerAnchorRebuildPort,
    ParcelMarkerAnchorRebuildReport,
};
use catalog_application::{RebuildParcelMarkerAnchors, RebuildParcelMarkerAnchorsInput};
use catalog_domain::{CatalogError, MarkerAnchorAlgorithm};
use foundation_shared_kernel::ids::StaffId;
use uuid::Uuid;

#[derive(Default)]
struct RecordingParcelMarkerAnchorRebuilder {
    commands: Mutex<Vec<ParcelMarkerAnchorRebuildCommand>>,
}

#[async_trait]
impl ParcelMarkerAnchorRebuildPort for RecordingParcelMarkerAnchorRebuilder {
    async fn rebuild_parcel_marker_anchors(
        &self,
        command: ParcelMarkerAnchorRebuildCommand,
    ) -> Result<ParcelMarkerAnchorRebuildReport, CatalogError> {
        self.commands
            .lock()
            .map_err(|_| CatalogError::Infrastructure("commands mutex poisoned".to_owned()))?
            .push(command.clone());

        Ok(ParcelMarkerAnchorRebuildReport {
            generation_run_id: Uuid::nil(),
            source_snapshot_id: command.source_snapshot_id,
            source_table: command.source_table,
            algorithm: command.algorithm,
            algorithm_version: command.algorithm_version,
            scanned_row_count: 2,
            loaded_row_count: 2,
            rejected_row_count: 0,
            superseded_row_count: 1,
        })
    }
}

#[tokio::test]
async fn delegates_normalized_polylabel_rebuild_command() -> Result<(), CatalogError> {
    let rebuilder = std::sync::Arc::new(RecordingParcelMarkerAnchorRebuilder::default());
    let use_case = RebuildParcelMarkerAnchors::new(rebuilder.clone());
    let staff_id = StaffId::new(Uuid::now_v7());

    let report = use_case
        .execute(RebuildParcelMarkerAnchorsInput {
            source_snapshot_id: " iceberg:parcel-boundaries-snapshot-20260522 ".to_owned(),
            algorithm_version: " postgis-st_maximuminscribedcircle-v1 ".to_owned(),
            requested_by_staff_id: Some(staff_id),
            request_id: Some(" anchor-rebuild-req-1 ".to_owned()),
        })
        .await?;

    assert_eq!(
        report.source_snapshot_id,
        "iceberg:parcel-boundaries-snapshot-20260522"
    );
    assert_eq!(report.source_table, "silver.parcel_boundaries");
    assert_eq!(report.algorithm, MarkerAnchorAlgorithm::Polylabel);
    assert_eq!(
        report.algorithm_version,
        "postgis-st_maximuminscribedcircle-v1"
    );
    assert_eq!(report.loaded_row_count, 2);
    assert_eq!(report.superseded_row_count, 1);

    let command = {
        let commands = rebuilder
            .commands
            .lock()
            .map_err(|_| CatalogError::Infrastructure("commands mutex poisoned".to_owned()))?;
        assert_eq!(commands.len(), 1);
        commands[0].clone()
    };
    assert_eq!(
        command.source_snapshot_id,
        "iceberg:parcel-boundaries-snapshot-20260522"
    );
    assert_eq!(command.source_table, "silver.parcel_boundaries");
    assert_eq!(command.algorithm, MarkerAnchorAlgorithm::Polylabel);
    assert_eq!(
        command.algorithm_version,
        "postgis-st_maximuminscribedcircle-v1"
    );
    assert_eq!(command.requested_by_staff_id, Some(staff_id));
    assert_eq!(command.request_id.as_deref(), Some("anchor-rebuild-req-1"));
    Ok(())
}

#[tokio::test]
async fn rejects_non_iceberg_snapshot_before_writing() -> Result<(), CatalogError> {
    let rebuilder = std::sync::Arc::new(RecordingParcelMarkerAnchorRebuilder::default());
    let use_case = RebuildParcelMarkerAnchors::new(rebuilder.clone());

    let result = use_case
        .execute(RebuildParcelMarkerAnchorsInput {
            source_snapshot_id: "parcel-boundaries-snapshot-20260522".to_owned(),
            algorithm_version: "postgis-st_maximuminscribedcircle-v1".to_owned(),
            requested_by_staff_id: None,
            request_id: None,
        })
        .await;

    assert!(matches!(
        result,
        Err(CatalogError::InvalidParcelMarkerAnchorRebuild(_))
    ));
    let commands_empty = rebuilder
        .commands
        .lock()
        .map_err(|_| CatalogError::Infrastructure("commands mutex poisoned".to_owned()))?
        .is_empty();
    assert!(commands_empty);
    Ok(())
}
