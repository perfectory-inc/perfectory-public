//! Catalog-owned parsing contracts for canonical industrial-complex patches.

use catalog_application::industrial_complex_patch::{
    parse_industrial_complex_proposed_record, parse_industrial_complex_restore_input,
    parse_industrial_complex_target_identity, RestoreIndustrialComplexInput,
};
use catalog_domain::{CatalogError, IndustrialComplex, IndustrialComplexKind};
use chrono::Utc;
use foundation_shared_kernel::ids::ComplexId;
use serde_json::{json, Value as JsonValue};
use uuid::Uuid;

const COMPLEX_ID: &str = "018f47a6-7d2f-7a31-8e4d-6c77b28a9210";

#[test]
fn target_identity_parses_complex_uuid() -> Result<(), CatalogError> {
    let parsed = parse_industrial_complex_target_identity(&json!({
        "complex_id": COMPLEX_ID,
    }))?;

    assert_eq!(parsed, target_id()?);
    Ok(())
}

#[test]
fn target_identity_rejects_missing_complex_id() -> Result<(), CatalogError> {
    assert_invalid_input(
        parse_industrial_complex_target_identity(&json!({})),
        "target_identity.complex_id must be a UUID string",
    )
}

#[test]
fn target_identity_rejects_uuid_with_surrounding_whitespace() -> Result<(), CatalogError> {
    assert_invalid_input(
        parse_industrial_complex_target_identity(&json!({
            "complex_id": format!(" {COMPLEX_ID} "),
        })),
        "target_identity.complex_id must be a UUID string",
    )
}

#[test]
fn proposed_record_parses_validated_catalog_patch() -> Result<(), CatalogError> {
    let mutation = parse_industrial_complex_proposed_record(&json!({
        "name": "West Harbor Industrial Complex",
        "area_m2": 125_000,
    }))?
    .into_mutation();

    assert_eq!(
        mutation.name.as_deref(),
        Some("West Harbor Industrial Complex")
    );
    assert_eq!(mutation.area_m2, Some(125_000));
    Ok(())
}

#[test]
fn proposed_record_rejects_name_with_surrounding_whitespace() -> Result<(), CatalogError> {
    assert_invalid_input(
        parse_industrial_complex_proposed_record(&json!({
            "name": " West Harbor Industrial Complex ",
        })),
        "name must be non-empty text without surrounding whitespace",
    )
}

#[test]
fn proposed_record_rejects_blank_name() -> Result<(), CatalogError> {
    assert_invalid_input(
        parse_industrial_complex_proposed_record(&json!({ "name": "   " })),
        "name must be non-empty text without surrounding whitespace",
    )
}

#[test]
fn proposed_record_requires_positive_area() -> Result<(), CatalogError> {
    assert_invalid_input(
        parse_industrial_complex_proposed_record(&json!({ "area_m2": 0 })),
        "area_m2 must be positive",
    )
}

#[test]
fn proposed_record_rejects_unsupported_fields() -> Result<(), CatalogError> {
    assert_invalid_input(
        parse_industrial_complex_proposed_record(&json!({
            "name": "West Harbor Industrial Complex",
            "kind": "national",
        })),
        "unsupported industrial_complex normalization field: kind",
    )
}

#[test]
fn proposed_record_requires_at_least_one_supported_field() -> Result<(), CatalogError> {
    assert_invalid_input(
        parse_industrial_complex_proposed_record(&json!({})),
        "at least one industrial complex field must be changed",
    )
}

#[test]
fn proposed_patch_rejects_values_equal_to_canonical_state() -> Result<(), CatalogError> {
    let canonical = canonical_complex()?;
    assert_invalid_input(
        parse_industrial_complex_proposed_record(&json!({
            "name": canonical.name,
            "area_m2": canonical.area_m2,
        }))?
        .validate_changes(&canonical),
        "industrial complex mutation must change canonical state",
    )
}

#[test]
fn proposed_patch_accepts_when_any_supplied_field_changes() -> Result<(), CatalogError> {
    let canonical = canonical_complex()?;
    parse_industrial_complex_proposed_record(&json!({
        "name": canonical.name,
        "area_m2": canonical.area_m2 + 1,
    }))?
    .validate_changes(&canonical)
}

#[test]
fn rollback_snapshot_parses_catalog_restore_input() -> Result<(), CatalogError> {
    let target_id = target_id()?;
    let restore = parse_restore(
        &snapshot("Original Industrial Complex", 95_000, 4),
        &snapshot("Normalized Industrial Complex", 95_000, 5),
        target_id,
    )?;

    let restore_target_id = restore.target_id();
    let mutation = restore.into_patch().into_mutation();

    assert_eq!(restore_target_id, target_id);
    assert_eq!(
        mutation.name.as_deref(),
        Some("Original Industrial Complex")
    );
    assert_eq!(mutation.area_m2, None);
    Ok(())
}

#[test]
fn rollback_snapshot_rejects_invalid_uuid() -> Result<(), CatalogError> {
    assert_invalid_input(
        parse_restore(
            &json!({
                "id": "not-a-uuid",
                "name": "Original Industrial Complex",
                "area_m2": 95_000,
                "version": 4,
            }),
            &valid_after_snapshot(),
            target_id()?,
        ),
        "before_snapshot.id must be a UUID string",
    )
}

#[test]
fn rollback_snapshot_rejects_target_id_mismatch() -> Result<(), CatalogError> {
    assert_invalid_input(
        parse_restore(
            &json!({
                "id": "018f47a6-7d2f-7a31-8e4d-6c77b28a9211",
                "name": "Original Industrial Complex",
                "area_m2": 95_000,
                "version": 4,
            }),
            &valid_after_snapshot(),
            target_id()?,
        ),
        "before_snapshot.id must match application target_id",
    )
}

#[test]
fn rollback_snapshot_rejects_missing_name() -> Result<(), CatalogError> {
    assert_invalid_input(
        parse_restore(
            &json!({
                "id": COMPLEX_ID,
                "area_m2": 95_000,
                "version": 4,
            }),
            &valid_after_snapshot(),
            target_id()?,
        ),
        "before_snapshot.name must be a non-empty string",
    )
}

#[test]
fn rollback_snapshot_rejects_blank_name() -> Result<(), CatalogError> {
    assert_invalid_input(
        parse_restore(
            &json!({
                "id": COMPLEX_ID,
                "name": "   ",
                "area_m2": 95_000,
                "version": 4,
            }),
            &valid_after_snapshot(),
            target_id()?,
        ),
        "before_snapshot.name must be a non-empty string",
    )
}

#[test]
fn rollback_snapshot_accepts_existing_zero_area() -> Result<(), CatalogError> {
    let restore = parse_restore(
        &json!({
            "id": COMPLEX_ID,
            "name": "Original Industrial Complex",
            "area_m2": 0,
            "version": 4,
        }),
        &json!({
            "id": COMPLEX_ID,
            "name": "Normalized Industrial Complex",
            "area_m2": 0,
            "version": 5,
        }),
        target_id()?,
    )?;

    let mutation = restore.into_patch().into_mutation();
    assert_eq!(
        mutation.name.as_deref(),
        Some("Original Industrial Complex")
    );
    assert_eq!(mutation.area_m2, None);
    Ok(())
}

#[test]
fn rollback_snapshot_rejects_negative_area() -> Result<(), CatalogError> {
    assert_invalid_input(
        parse_restore(
            &json!({
                "id": COMPLEX_ID,
                "name": "Original Industrial Complex",
                "area_m2": -1,
                "version": 4,
            }),
            &valid_after_snapshot(),
            target_id()?,
        ),
        "before_snapshot.area_m2 must be a non-negative integer",
    )
}

#[test]
fn rollback_snapshot_rejects_missing_version() -> Result<(), CatalogError> {
    assert_invalid_input(
        parse_restore(
            &json!({
                "id": COMPLEX_ID,
                "name": "Original Industrial Complex",
                "area_m2": 95_000,
            }),
            &valid_after_snapshot(),
            target_id()?,
        ),
        "before_snapshot.version must be a positive integer",
    )
}

fn snapshot(name: &str, area_m2: i64, version: i64) -> JsonValue {
    json!({
        "id": COMPLEX_ID,
        "official_complex_code": "IC-001",
        "name": name,
        "kind": "general",
        "primary_bjdong_code": "1111010100",
        "area_m2": area_m2,
        "version": version,
    })
}

fn parse_restore(
    before_snapshot: &JsonValue,
    after_snapshot: &JsonValue,
    target_id: ComplexId,
) -> Result<RestoreIndustrialComplexInput, CatalogError> {
    parse_industrial_complex_restore_input(
        before_snapshot,
        after_snapshot,
        after_snapshot,
        target_id,
    )
}

fn valid_after_snapshot() -> JsonValue {
    snapshot("Normalized Industrial Complex", 125_000, 5)
}

fn target_id() -> Result<ComplexId, CatalogError> {
    Uuid::parse_str(COMPLEX_ID)
        .map(ComplexId::new)
        .map_err(|error| CatalogError::Infrastructure(format!("invalid fixture UUID: {error}")))
}

fn canonical_complex() -> Result<IndustrialComplex, CatalogError> {
    let now = Utc::now();
    Ok(IndustrialComplex {
        id: target_id()?,
        official_complex_code: "IC-001".to_owned(),
        name: "Original Industrial Complex".to_owned(),
        kind: IndustrialComplexKind::General,
        primary_bjdong_code: "1111010100".to_owned(),
        area_m2: 95_000,
        created_at: now,
        updated_at: now,
        archived_at: None,
        version: 1,
    })
}

fn assert_invalid_input<T>(
    result: Result<T, CatalogError>,
    expected: &str,
) -> Result<(), CatalogError> {
    match result {
        Err(CatalogError::InvalidIndustrialComplexInput(message)) => {
            assert_eq!(message, expected);
            Ok(())
        }
        Err(other) => Err(other),
        Ok(_) => Err(CatalogError::Infrastructure(format!(
            "expected invalid industrial complex input: {expected}"
        ))),
    }
}
