//! Contract tests for lakehouse catalog configuration.

use std::collections::BTreeMap;

use lakehouse_infrastructure::{
    live_lakehouse_smoke_enabled, validate_lakehouse_smoke_table_name, LakehouseCatalogConfig,
    LakehouseCatalogConfigError, LakehouseCatalogProvider, DEFAULT_LAKEHOUSE_SMOKE_TABLE,
};

fn vars(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

#[test]
fn parses_r2_data_catalog_configuration() -> Result<(), LakehouseCatalogConfigError> {
    let config = LakehouseCatalogConfig::from_vars(&vars(&[
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER",
            "r2_data_catalog",
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI",
            "https://catalog.cloudflarestorage.com/account/catalog",
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE",
            "foundation-platform",
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN",
            "secret-token",
        ),
    ]))?;

    assert_eq!(config.provider, LakehouseCatalogProvider::R2DataCatalog);
    assert_eq!(
        config.catalog_uri,
        "https://catalog.cloudflarestorage.com/account/catalog"
    );
    assert_eq!(config.warehouse, "foundation-platform");
    assert_eq!(config.catalog_token.as_deref(), Some("secret-token"));
    Ok(())
}

#[test]
fn rejects_missing_required_catalog_uri() {
    let result = LakehouseCatalogConfig::from_vars(&vars(&[
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER",
            "r2_data_catalog",
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE",
            "foundation-platform",
        ),
    ]));

    assert_eq!(
        result,
        Err(LakehouseCatalogConfigError::MissingEnv(
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI"
        ))
    );
}

#[test]
fn rejects_unknown_provider() {
    let result = LakehouseCatalogConfig::from_vars(&vars(&[
        ("FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER", "custom"),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI",
            "https://example.com/catalog",
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE",
            "foundation-platform",
        ),
    ]));

    assert_eq!(
        result,
        Err(LakehouseCatalogConfigError::UnknownProvider(
            "custom".to_owned()
        ))
    );
}

#[test]
fn debug_output_redacts_catalog_token() -> Result<(), LakehouseCatalogConfigError> {
    let config = LakehouseCatalogConfig::from_vars(&vars(&[
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER",
            "r2_data_catalog",
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI",
            "https://example.com/catalog",
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE",
            "foundation-platform",
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN",
            "secret-token",
        ),
    ]))?;

    let debug = format!("{config:?}");

    assert!(debug.contains("catalog_token: \"<redacted>\""));
    assert!(!debug.contains("secret-token"));
    Ok(())
}

#[test]
fn live_lakehouse_smoke_requires_exact_opt_in() {
    assert!(!live_lakehouse_smoke_enabled(None));
    assert!(!live_lakehouse_smoke_enabled(Some("")));
    assert!(!live_lakehouse_smoke_enabled(Some(" 1 ")));
    assert!(!live_lakehouse_smoke_enabled(Some("true")));
    assert!(live_lakehouse_smoke_enabled(Some("1")));
}

#[test]
fn default_lakehouse_smoke_table_targets_silver_industrial_complexes(
) -> Result<(), LakehouseCatalogConfigError> {
    assert_eq!(DEFAULT_LAKEHOUSE_SMOKE_TABLE, "silver.industrial_complexes");
    validate_lakehouse_smoke_table_name(DEFAULT_LAKEHOUSE_SMOKE_TABLE)?;
    Ok(())
}

#[test]
fn smoke_table_name_rejects_path_like_or_ambiguous_values() {
    for table_name in [
        "",
        " silver.industrial_complexes",
        "silver.industrial_complexes ",
        "/silver.industrial_complexes",
        "silver/industrial_complexes",
        "silver\\industrial_complexes",
        "silver..industrial_complexes",
        "silver.",
        ".industrial_complexes",
        "silver.industrial complexes",
        "silver.industrial-complexes",
        "Silver.industrial_complexes",
        "silver",
    ] {
        let result = validate_lakehouse_smoke_table_name(table_name);

        assert!(matches!(
            result,
            Err(LakehouseCatalogConfigError::InvalidSmokeTableName(_))
        ));
    }
}
