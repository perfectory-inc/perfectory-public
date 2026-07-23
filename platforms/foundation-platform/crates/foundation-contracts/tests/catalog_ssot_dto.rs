//! Contract tests for Catalog SSOT DTOs.

use chrono::Utc;
use foundation_contracts::catalog::{
    ArchiveComplexRequest, BuildingResponse, FileAssetResponse,
    IndustrialComplexGoldPointerResponse, IndustrialComplexResponse, ManufacturerResponse,
    UpdateComplexRequest,
};
use uuid::Uuid;

#[test]
fn file_asset_response_uses_provider_neutral_object_key() -> Result<(), serde_json::Error> {
    let dto = FileAssetResponse {
        id: Uuid::nil(),
        object_key: "complexes/2820000000/blueprints/master.pdf".to_owned(),
        mime_type: "application/pdf".to_owned(),
        size_bytes: 128,
        checksum_sha256: None,
        title: Some("master plan".to_owned()),
        visibility: "internal".to_owned(),
        version: 1,
        updated_at: Utc::now(),
    };

    let json = serde_json::to_value(dto)?;

    assert_eq!(
        json["object_key"].as_str(),
        Some("complexes/2820000000/blueprints/master.pdf")
    );
    let legacy_key = ["s3", "_key"].concat();
    assert!(json.get(legacy_key.as_str()).is_none());
    Ok(())
}

#[test]
fn building_response_exposes_catalog_building_read_model() -> Result<(), serde_json::Error> {
    let dto = BuildingResponse {
        id: Uuid::nil(),
        parcel_id: Uuid::nil(),
        purpose_code: "02000".to_owned(),
        structure_code: "11".to_owned(),
        floor_area_m2: 1234.5,
        stories: 5,
        below_ground_floors: 2,
        has_rooftop: true,
        rooftop_area_m2: Some(13.87),
        rooftop_usage: "기타제2종근린생활시설 · 주차장".to_owned(),
        built_year: 2020,
        updated_at: Utc::now(),
    };

    let json = serde_json::to_value(dto)?;

    assert_eq!(json["purpose_code"].as_str(), Some("02000"));
    assert_eq!(json["structure_code"].as_str(), Some("11"));
    assert_eq!(json["floor_area_m2"].as_f64(), Some(1234.5));
    assert_eq!(json["stories"].as_i64(), Some(5));
    assert_eq!(json["below_ground_floors"].as_i64(), Some(2));
    assert_eq!(json["has_rooftop"].as_bool(), Some(true));
    assert_eq!(json["rooftop_area_m2"].as_f64(), Some(13.87));
    assert_eq!(
        json["rooftop_usage"].as_str(),
        Some("기타제2종근린생활시설 · 주차장")
    );
    assert_eq!(json["built_year"].as_i64(), Some(2020));
    Ok(())
}

#[test]
fn manufacturer_response_never_exposes_business_registration_number(
) -> Result<(), serde_json::Error> {
    let dto = ManufacturerResponse {
        id: Uuid::nil(),
        primary_parcel_id: Uuid::nil(),
        name: "fixture manufacturer".to_owned(),
        ksic_code: "26299".to_owned(),
        updated_at: Utc::now(),
    };

    let json = serde_json::to_value(dto)?;

    assert_eq!(json["name"].as_str(), Some("fixture manufacturer"));
    assert_eq!(json["ksic_code"].as_str(), Some("26299"));
    assert!(json.get("business_registration_number").is_none());
    assert!(json.get("business_number").is_none());
    Ok(())
}

#[test]
fn industrial_complex_response_embeds_thin_gold_pointer_contract() -> Result<(), serde_json::Error>
{
    let published_at = Utc::now();
    let dto = IndustrialComplexResponse {
        id: Uuid::nil(),
        official_complex_code: "IC-0001".to_owned(),
        name: "fixture complex".to_owned(),
        kind: "general".to_owned(),
        primary_bjdong_code: "1234567890".to_owned(),
        area_m2: 1024,
        version: 3,
        updated_at: published_at,
        archived_at: None,
        gold_pointer: Some(IndustrialComplexGoldPointerResponse {
            current_version: "gold-2026-05-18T00-00-00Z".to_owned(),
            previous_version: Some("gold-2026-05-17T00-00-00Z".to_owned()),
            profile_object_key: "gold/industrial-complex/profiles/0196e7e0-3c20-7000-8000-100000000002.json".to_owned(),
            spatial_locator_object_key: Some(
                "gold/industrial-complex/spatial-locators/0196e7e0-3c20-7000-8000-100000000002.parquet".to_owned(),
            ),
            source_record_id: Uuid::nil(),
            source_snapshot_id: "source-snapshot-1".to_owned(),
            iceberg_snapshot_id: "iceberg-snapshot-1".to_owned(),
            profile_row_count: 10,
            profile_checksum_sha256:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            published_at,
        }),
    };

    let json = serde_json::to_value(dto)?;
    let gold_pointer = &json["gold_pointer"];

    assert_eq!(
        gold_pointer["profile_object_key"].as_str(),
        Some("gold/industrial-complex/profiles/0196e7e0-3c20-7000-8000-100000000002.json")
    );
    assert_eq!(
        gold_pointer["spatial_locator_object_key"].as_str(),
        Some(
            "gold/industrial-complex/spatial-locators/0196e7e0-3c20-7000-8000-100000000002.parquet"
        )
    );
    assert_eq!(gold_pointer["profile_row_count"].as_u64(), Some(10));
    assert!(gold_pointer.get("raw_response").is_none());
    assert!(gold_pointer.get("payload").is_none());
    assert!(gold_pointer.get("url").is_none());
    Ok(())
}

#[test]
fn update_complex_request_uses_partial_patch_with_version_guard() -> Result<(), serde_json::Error> {
    let dto = UpdateComplexRequest {
        name: Some("Synthetic Industrial Complex Gamma".to_owned()),
        area_m2: Some(9_600_000),
        if_match_version: 3,
    };

    let json = serde_json::to_value(dto)?;

    assert_eq!(
        json["name"].as_str(),
        Some("Synthetic Industrial Complex Gamma")
    );
    assert_eq!(json["area_m2"].as_u64(), Some(9_600_000));
    assert_eq!(json["if_match_version"].as_i64(), Some(3));
    assert!(json.get("price").is_none());
    assert!(json.get("status").is_none());
    Ok(())
}

#[test]
fn archive_complex_request_uses_lifecycle_guard_not_hard_delete() -> Result<(), serde_json::Error> {
    let dto = ArchiveComplexRequest {
        if_match_version: 4,
        reason: Some("duplicate source record".to_owned()),
    };

    let json = serde_json::to_value(dto)?;

    assert_eq!(json["if_match_version"].as_i64(), Some(4));
    assert_eq!(json["reason"].as_str(), Some("duplicate source record"));
    assert!(json.get("hard_delete").is_none());
    assert!(json.get("cascade").is_none());
    Ok(())
}
