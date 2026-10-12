//! Published Catalog PNU wire-contract parity tests.

use foundation_contracts::catalog::ParcelResponse;
use utoipa::PartialSchema;

const STANDARD_PNU_PATTERN: &str = "^[0-9]{10}[1289][0-9]{8}$";

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn parcel_response_schema_carries_the_standard_pnu_pattern() -> TestResult {
    let schema = serde_json::to_value(ParcelResponse::schema())?;

    assert_eq!(schema["properties"]["pnu"]["pattern"], STANDARD_PNU_PATTERN);
    assert_eq!(schema["properties"]["pnu"]["minLength"], 19);
    assert_eq!(schema["properties"]["pnu"]["maxLength"], 19);
    Ok(())
}

#[test]
fn catalog_openapi_uses_the_standard_pnu_pattern_everywhere() -> TestResult {
    let catalog_openapi: serde_json::Value =
        serde_json::from_str(include_str!("../../../docs/openapi/catalog.v1.json"))?;

    // No route takes a PNU path parameter since the by-PNU reads left (root ADR-0194 §3).
    assert_eq!(
        catalog_openapi["components"]["schemas"]["ParcelResponse"]["properties"]["pnu"]["pattern"],
        STANDARD_PNU_PATTERN
    );
    Ok(())
}
