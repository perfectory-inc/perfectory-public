//! Application contract for atomic Lakehouse Registry artifact registration.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lakehouse_application::ports::LakehouseRegistryUnitOfWork;
use lakehouse_application::{
    RegisterLakehouseObjectArtifact, RegisterLakehouseObjectArtifactCommand,
    RegisterLakehouseObjectArtifactInput, RegisterLakehouseObjectArtifactReceipt,
};
use lakehouse_domain::{LakehouseError, LakehouseOwnerService, LakehouseRegistryLayer};

#[tokio::test]
async fn governed_registration_delegates_one_atomic_command() -> Result<(), LakehouseError> {
    let unit_of_work = Arc::new(RecordingUnitOfWork::default());
    let use_case = RegisterLakehouseObjectArtifact::new(unit_of_work.clone());

    let receipt = use_case
        .execute(RegisterLakehouseObjectArtifactInput {
            qualified_name: "gongzzang.gold.listing_photo_media".to_owned(),
            namespace_id: "gongzzang_r2_production".to_owned(),
            object_key: "media/listing-photo/listings/lst_1/photos/lph_1.webp".to_owned(),
            content_type: "image/webp".to_owned(),
            checksum_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_owned(),
            size_bytes: 2048,
            logical_record_count: None,
        })
        .await?;

    assert_eq!(receipt.artifact_id, "artifact-1");
    let commands = unit_of_work
        .commands
        .lock()
        .map_err(|_| LakehouseError::Persistence("recording mutex poisoned".to_owned()))?;
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].owner_service, LakehouseOwnerService::Gongzzang);
    assert_eq!(commands[0].layer, LakehouseRegistryLayer::Gold);
    assert_eq!(commands[0].dataset_version, "append_only_v1");
    assert_eq!(commands[0].object_key, receipt.object_key);
    drop(commands);
    Ok(())
}

#[tokio::test]
async fn invalid_governed_prefix_is_rejected_before_the_unit_of_work() -> Result<(), LakehouseError>
{
    let unit_of_work = Arc::new(RecordingUnitOfWork::default());
    let use_case = RegisterLakehouseObjectArtifact::new(unit_of_work.clone());

    let result = use_case
        .execute(RegisterLakehouseObjectArtifactInput {
            qualified_name: "gongzzang.gold.listing_photo_media".to_owned(),
            namespace_id: "gongzzang_r2_production".to_owned(),
            object_key: "gold/listing-marker-tiles/0/0/0.pbf".to_owned(),
            content_type: "application/x-protobuf".to_owned(),
            checksum_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_owned(),
            size_bytes: 2048,
            logical_record_count: None,
        })
        .await;
    let Err(error) = result else {
        return Err(LakehouseError::Persistence(
            "an object outside the governed prefix was accepted".to_owned(),
        ));
    };

    assert!(matches!(
        error,
        LakehouseError::InvalidLakehouseRegistryInput(_)
    ));
    assert!(unit_of_work
        .commands
        .lock()
        .map_err(|_| LakehouseError::Persistence("recording mutex poisoned".to_owned()))?
        .is_empty());
    Ok(())
}

#[derive(Default)]
struct RecordingUnitOfWork {
    commands: Mutex<Vec<RegisterLakehouseObjectArtifactCommand>>,
}

#[async_trait]
impl LakehouseRegistryUnitOfWork for RecordingUnitOfWork {
    async fn register_object_artifact(
        &self,
        command: RegisterLakehouseObjectArtifactCommand,
    ) -> Result<RegisterLakehouseObjectArtifactReceipt, LakehouseError> {
        self.commands
            .lock()
            .map_err(|_| LakehouseError::Persistence("recording mutex poisoned".to_owned()))?
            .push(command.clone());
        Ok(RegisterLakehouseObjectArtifactReceipt {
            artifact_id: "artifact-1".to_owned(),
            qualified_name: command.qualified_name,
            object_key: command.object_key,
        })
    }
}
