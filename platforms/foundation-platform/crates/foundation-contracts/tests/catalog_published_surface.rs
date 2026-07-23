//! Regression coverage for the versioned Catalog surface consumed across repositories.

const CATALOG_OPENAPI: &str = include_str!("../../../docs/openapi/catalog.v1.json");

#[test]
fn published_catalog_contract_contains_the_live_cross_repo_surface() {
    for path in [
        "/catalog/v1/complexes/{id}/anchor-summary",
        "/catalog/v1/parcels/by-pnu/{pnu}/buildings",
        "/catalog/v1/parcels/by-pnu/{pnu}/units",
        "/catalog/v1/lakehouse/batch-runs",
        "/map/v1/marker-tiles/contract",
    ] {
        assert!(
            CATALOG_OPENAPI.contains(path),
            "published Catalog contract is missing live route {path}"
        );
    }
}
