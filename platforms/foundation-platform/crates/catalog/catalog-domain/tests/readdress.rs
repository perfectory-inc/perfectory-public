//! Readdress mutation ledger contract.

use catalog_domain::{CatalogMutationKind, ServingSourceKind, VectorTileBuildKind};

#[test]
fn readdress_has_a_distinct_idempotent_build_start_without_a_manifest_outcome() -> Result<(), String>
{
    let kind = CatalogMutationKind::parse("start_static_release_readdress")
        .map_err(|error| format!("readdress must have its own mutation ledger kind: {error}"))?;
    assert!(!kind.answers_with_manifest());
    assert_ne!(kind, CatalogMutationKind::StartVectorTileBuild);
    assert!(CatalogMutationKind::ALL.contains(&kind));
    Ok(())
}

#[test]
fn baking_keeps_dynamic_inputs_and_only_readdress_accepts_static_inputs() {
    assert_eq!(
        VectorTileBuildKind::Bake.input_source_kind(),
        ServingSourceKind::DynamicPostgis
    );
    assert_eq!(
        VectorTileBuildKind::Readdress.input_source_kind(),
        ServingSourceKind::StaticPmtiles
    );
    for kind in VectorTileBuildKind::ALL {
        assert_eq!(VectorTileBuildKind::parse(kind.as_str()), Ok(kind));
    }
    assert!(VectorTileBuildKind::parse("copy").is_err());
}
