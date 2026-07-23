//! Industrial-complex Gold pointer domain contract tests.

use chrono::Utc;
use foundation_shared_kernel::ids::{ComplexId, FileAssetId, SourceRecordId};
use foundation_shared_kernel::ObjectKey;
use lakehouse_domain::IndustrialComplexGoldPointer;
use uuid::Uuid;

#[test]
fn builds_publish_event_from_gold_pointer() -> Result<(), Box<dyn std::error::Error>> {
    let pointer = IndustrialComplexGoldPointer {
        complex_id: ComplexId::new(Uuid::nil()),
        current_version: "0196e7e0-3c20-7000-8000-100000000001".to_owned(),
        previous_version: Some("gold-industrial-complex-profile-v0".to_owned()),
        profile_file_asset_id: FileAssetId::new(Uuid::nil()),
        profile_object_key: ObjectKey::parse(
            "gold/industrial-complex/profiles/0196e7e0-3c20-7000-8000-100000000002.json",
        )?,
        spatial_locator_file_asset_id: Some(FileAssetId::new(Uuid::nil())),
        spatial_locator_object_key: Some(ObjectKey::parse(
            "gold/industrial-complex/spatial-locators/0196e7e0-3c20-7000-8000-100000000002.parquet",
        )?),
        source_record_id: SourceRecordId::new(Uuid::nil()),
        source_snapshot_id: "bronze-industrial-complex-20260518".to_owned(),
        iceberg_snapshot_id: "iceberg-snapshot-42".to_owned(),
        profile_row_count: 1,
        profile_checksum_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            .to_owned(),
        published_at: Utc::now(),
        updated_at: Utc::now(),
        version: 1,
    };

    let event = pointer.published_event();

    assert_eq!(event.complex_id, pointer.complex_id);
    assert_eq!(event.current_version, pointer.current_version);
    assert_eq!(
        event.profile_object_key,
        "gold/industrial-complex/profiles/0196e7e0-3c20-7000-8000-100000000002.json"
    );
    assert_eq!(event.profile_row_count, 1);
    Ok(())
}
