//! The committed staff data catalog `OpenAPI` document is exactly what the code generates.
//!
//! The staff console generates its TypeScript types from this file, so a route or schema change
//! that is not re-exported fails here, before the console can drift. Read at run time (cargo runs a
//! test from its package directory), so it costs no compile-time read
//! (scripts/guard/build-coupling-baseline.sh). Regenerate with
//! `cargo run -p foundation-api --bin export-data-catalog-openapi -- docs/openapi/data-catalog.v1.json`
//! from the platform root.

use std::error::Error;

#[test]
fn committed_data_catalog_openapi_is_the_exact_generated_document() -> Result<(), Box<dyn Error>> {
    let committed: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        "../../docs/openapi/data-catalog.v1.json",
    )?)?;
    let generated = serde_json::to_value(foundation_api::data_catalog_openapi_document())?;
    assert_eq!(committed, generated);
    Ok(())
}
