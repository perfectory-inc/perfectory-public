//! Industrial-complex Gold pointer publication use-case tests.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use foundation_shared_kernel::ids::{ComplexId, FileAssetId, SourceRecordId};
use lakehouse_application::ports::LakehousePublicationUnitOfWork;
use lakehouse_application::{
    PublishIndustrialComplexGoldPointer, PublishIndustrialComplexGoldPointerCommand,
    PublishIndustrialComplexGoldPointerInput,
};
use lakehouse_domain::{IndustrialComplexGoldPointer, LakehouseError};
use uuid::Uuid;

#[derive(Default)]
struct RecordingLakehousePublicationUnitOfWork {
    commands: Mutex<Vec<PublishIndustrialComplexGoldPointerCommand>>,
}

#[async_trait]
impl LakehousePublicationUnitOfWork for RecordingLakehousePublicationUnitOfWork {
    async fn publish_industrial_complex_gold_pointer(
        &self,
        command: PublishIndustrialComplexGoldPointerCommand,
    ) -> Result<IndustrialComplexGoldPointer, LakehouseError> {
        self.commands
            .lock()
            .map_err(|_| LakehouseError::Persistence("commands mutex poisoned".to_owned()))?
            .push(command.clone());

        Ok(IndustrialComplexGoldPointer {
            complex_id: command.complex_id,
            current_version: command.current_version,
            previous_version: command.expected_current_version,
            profile_file_asset_id: FileAssetId::new(Uuid::nil()),
            profile_object_key: foundation_shared_kernel::ObjectKey::parse(
                &command.profile_object_key,
            )
            .map_err(|error| LakehouseError::InvalidContract(error.to_string()))?,
            spatial_locator_file_asset_id: None,
            spatial_locator_object_key: None,
            source_record_id: SourceRecordId::new(Uuid::nil()),
            source_snapshot_id: command.source_snapshot_id,
            iceberg_snapshot_id: command.iceberg_snapshot_id,
            profile_row_count: command.profile_row_count,
            profile_checksum_sha256: command.profile_checksum_sha256,
            published_at: command.published_at,
            updated_at: command.published_at,
            version: 1,
        })
    }
}

fn valid_input() -> PublishIndustrialComplexGoldPointerInput {
    PublishIndustrialComplexGoldPointerInput {
        complex_id: ComplexId::new(Uuid::nil()),
        current_version: "0196e7e0-3c20-7000-8000-100000000001".to_owned(),
        expected_current_version: None,
        profile_object_key:
            "gold/industrial-complex/profiles/0196e7e0-3c20-7000-8000-100000000002.json".to_owned(),
        spatial_locator_object_key: None,
        source: "foundation-platform.spark.industrial_complex_gold".to_owned(),
        source_url: None,
        source_external_id: Some("spark-run-20260518".to_owned()),
        source_snapshot_id: "bronze-industrial-complex-20260518".to_owned(),
        iceberg_snapshot_id: "iceberg-snapshot-42".to_owned(),
        profile_row_count: 1,
        profile_size_bytes: 512,
        spatial_locator_size_bytes: None,
        profile_checksum_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            .to_owned(),
        published_at: chrono::Utc::now(),
    }
}

#[tokio::test]
async fn publishes_valid_gold_pointer_through_the_lakehouse_boundary() -> Result<(), LakehouseError>
{
    let unit_of_work = Arc::new(RecordingLakehousePublicationUnitOfWork::default());
    let use_case = PublishIndustrialComplexGoldPointer::new(unit_of_work.clone());

    let pointer = use_case.execute(valid_input()).await?;

    assert_eq!(pointer.complex_id, ComplexId::new(Uuid::nil()));
    assert_eq!(
        pointer.profile_object_key.as_str(),
        "gold/industrial-complex/profiles/0196e7e0-3c20-7000-8000-100000000002.json"
    );
    let commands = unit_of_work
        .commands
        .lock()
        .map_err(|_| LakehouseError::Persistence("commands mutex poisoned".to_owned()))?;
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].profile_row_count, 1);
    assert_eq!(commands[0].profile_size_bytes, 512);
    drop(commands);
    Ok(())
}

#[tokio::test]
async fn rejects_invalid_object_key_before_writing() {
    let unit_of_work = Arc::new(RecordingLakehousePublicationUnitOfWork::default());
    let use_case = PublishIndustrialComplexGoldPointer::new(unit_of_work.clone());
    let mut input = valid_input();
    input.profile_object_key = "/bad/key.json".to_owned();

    let result = use_case.execute(input).await;

    assert!(matches!(result, Err(LakehouseError::InvalidContract(_))));
    assert!(unit_of_work
        .commands
        .lock()
        .is_ok_and(|commands| commands.is_empty()));
}

#[tokio::test]
async fn rejects_spatial_locator_key_without_size_before_writing() {
    let unit_of_work = Arc::new(RecordingLakehousePublicationUnitOfWork::default());
    let use_case = PublishIndustrialComplexGoldPointer::new(unit_of_work.clone());
    let mut input = valid_input();
    input.spatial_locator_object_key = Some(
        "gold/industrial-complex/spatial-locators/0196e7e0-3c20-7000-8000-100000000002.parquet"
            .to_owned(),
    );

    let result = use_case.execute(input).await;

    assert!(matches!(result, Err(LakehouseError::InvalidContract(_))));
    assert!(unit_of_work
        .commands
        .lock()
        .is_ok_and(|commands| commands.is_empty()));
}
