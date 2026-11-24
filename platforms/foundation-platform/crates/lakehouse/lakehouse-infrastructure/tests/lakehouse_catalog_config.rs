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
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN",
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

/// The operations map reads with the read-only token and never falls back to the writer's
/// (root ADR-0247): a process that holds both still gets the reader, and one that holds only the
/// writer gets no token at all.
#[test]
fn the_reader_configuration_takes_only_the_read_only_token(
) -> Result<(), LakehouseCatalogConfigError> {
    let base = [
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER",
            "r2_data_catalog",
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI",
            "https://catalog.example.test/account/catalog",
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE",
            "foundation-platform",
        ),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN",
            "writer-token",
        ),
    ];
    let mut both = base.to_vec();
    both.push((
        "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN",
        "reader-token",
    ));

    let reader = LakehouseCatalogConfig::reader_from_vars(&vars(&both))?;
    assert_eq!(reader.catalog_token.as_deref(), Some("reader-token"));
    let writer = LakehouseCatalogConfig::from_vars(&vars(&both))?;
    assert_eq!(writer.catalog_token.as_deref(), Some("writer-token"));
    let writer_only = LakehouseCatalogConfig::reader_from_vars(&vars(&base))?;
    assert_eq!(writer_only.catalog_token, None);
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
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN",
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

#[test]
fn the_declared_requirements_are_what_each_loader_needs() {
    use foundation_shared_kernel::env_source::Requirement;

    type Load = fn(
        &BTreeMap<String, String>,
    ) -> Result<LakehouseCatalogConfig, LakehouseCatalogConfigError>;
    let loaders: [(&[Requirement], Load); 2] = [
        (
            LakehouseCatalogConfig::WRITER_REQUIREMENTS,
            LakehouseCatalogConfig::from_vars,
        ),
        (
            LakehouseCatalogConfig::READER_REQUIREMENTS,
            LakehouseCatalogConfig::reader_from_vars,
        ),
    ];
    for (requirements, load) in loaders {
        // Every required name set and nothing else: enough. Any one of them gone: refused.
        let enough: BTreeMap<String, String> = requirements
            .iter()
            .filter_map(|requirement| match requirement {
                Requirement::Required(name) => Some(*name),
                Requirement::OneOf(names) => names.first().copied(),
                Requirement::Optional(_) => None,
            })
            .map(|name| {
                let value = if name.ends_with("_PROVIDER") {
                    "iceberg_rest"
                } else {
                    "synthetic"
                };
                (name.to_owned(), value.to_owned())
            })
            .collect();
        assert!(load(&enough).is_ok(), "the declared names alone load");
        for name in enough.keys() {
            let mut without = enough.clone();
            without.remove(name);
            assert!(
                load(&without).is_err(),
                "{name} is declared required and is"
            );
        }
        // The optional token is read when it is there.
        let token = requirements
            .iter()
            .find_map(|requirement| match requirement {
                Requirement::Optional(name) => Some(*name),
                _ => None,
            });
        let mut with_token = enough.clone();
        with_token.extend(token.map(|name| (name.to_owned(), "synthetic-token".to_owned())));
        assert_eq!(
            load(&with_token)
                .ok()
                .and_then(|config| config.catalog_token),
            Some("synthetic-token".to_owned())
        );
    }
}
