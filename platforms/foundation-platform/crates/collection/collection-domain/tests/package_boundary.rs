//! Executable ownership boundary for the Foundation Collection capability.

use std::{error::Error, fs, io, path::Path};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const FORBIDDEN_DOMAIN_MODULES: &[&str] = &[
    "pub mod bronze;",
    "pub mod operation_dataset_slug;",
    "pub mod provider_acquisition;",
    "pub mod source_slug;",
];

const FORBIDDEN_DOMAIN_ERROR_VARIANTS: &[&str] = &[
    "IngestionRunNotFound",
    "InvalidIngestionRunCompletion",
    "ProviderAcquisitionBlocked",
];

const FORBIDDEN_APPLICATION_MODULES: &[&str] = &[
    "pub mod bronze_catalog_recovery;",
    "pub mod bronze_committer;",
    "pub mod building_hub_bulk_collection_plan;",
    "pub mod building_register_bronze_plan;",
    "pub mod provider_acquisition_landing;",
    "pub mod provider_acquisition_plan;",
    "pub mod public_data_bronze_plan;",
    "pub mod public_data_bulk_plan;",
    "pub mod real_transaction_bronze_plan;",
    "pub mod rt_molit_real_transaction_export_plan;",
    "pub mod vworld_cadastral_bronze_plan;",
    "pub mod vworld_dataset_collection_plan;",
    "pub mod vworld_land_register_bronze_plan;",
    "pub mod vworld_ned_bronze_plan;",
];

const FORBIDDEN_INFRASTRUCTURE_MODULES: &[&str] = &[
    "pub mod bronze_repository;",
    "pub mod building_hub_bulk;",
    "pub mod data_go_kr_building_register;",
    "pub mod data_go_kr_odcloud_api;",
    "pub mod data_go_kr_service_api;",
    "pub mod vworld_data_api;",
    "pub mod vworld_dataset_file;",
    "pub mod vworld_ned_attribute;",
];

#[test]
fn catalog_packages_do_not_own_collection_modules() -> TestResult {
    let workspace = workspace_root()?;
    assert_absent(
        &workspace.join("crates/catalog/catalog-domain/src/lib.rs"),
        FORBIDDEN_DOMAIN_MODULES,
    )?;
    assert_absent(
        &workspace.join("crates/catalog/catalog-domain/src/errors.rs"),
        FORBIDDEN_DOMAIN_ERROR_VARIANTS,
    )?;
    assert_absent(
        &workspace.join("crates/catalog/catalog-application/src/lib.rs"),
        FORBIDDEN_APPLICATION_MODULES,
    )?;
    assert_absent(
        &workspace.join("crates/catalog/catalog-infrastructure/src/lib.rs"),
        FORBIDDEN_INFRASTRUCTURE_MODULES,
    )?;
    Ok(())
}

#[test]
fn collection_packages_do_not_depend_on_catalog_packages() -> TestResult {
    let workspace = workspace_root()?;
    for relative in [
        "crates/collection/collection-domain",
        "crates/collection/collection-application",
        "crates/collection/collection-infrastructure",
    ] {
        assert_tree_has_no_catalog_dependency(&workspace.join(relative))?;
    }
    Ok(())
}

#[test]
fn boundary_detector_rejects_a_forbidden_fixture() {
    let fixture = "pub mod bronze;\n";
    assert!(find_forbidden(fixture, FORBIDDEN_DOMAIN_MODULES).is_some());
}

fn workspace_root() -> TestResult<std::path::PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .ok_or_else(|| test_error("collection-domain must live under crates/collection"))
        .map(Path::to_path_buf)?;
    Ok(root)
}

fn assert_absent(path: &Path, forbidden: &[&str]) -> TestResult {
    let contents = fs::read_to_string(path)?;
    if let Some(token) = find_forbidden(&contents, forbidden) {
        return Err(test_error(format!(
            "{} still owns forbidden Collection token {token:?}",
            path.display()
        )));
    }
    Ok(())
}

fn find_forbidden<'a>(contents: &str, forbidden: &'a [&str]) -> Option<&'a str> {
    forbidden
        .iter()
        .copied()
        .find(|token| contents.contains(token))
}

fn assert_tree_has_no_catalog_dependency(root: &Path) -> TestResult {
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let entry_path = entry.path();
            if entry_path.is_dir() {
                if entry_path.file_name().is_some_and(|name| name == "tests") {
                    continue;
                }
                pending.push(entry_path);
                continue;
            }
            let is_rust = entry_path
                .extension()
                .is_some_and(|extension| extension == "rs");
            let is_manifest = entry_path
                .file_name()
                .is_some_and(|name| name == "Cargo.toml");
            if !is_rust && !is_manifest {
                continue;
            }
            let contents = fs::read_to_string(&entry_path)?;
            for token in [
                "catalog_domain",
                "catalog_application",
                "catalog_infrastructure",
                "catalog-domain",
                "catalog-application",
                "catalog-infrastructure",
            ] {
                if contents.contains(token) {
                    return Err(test_error(format!(
                        "{} contains forbidden Catalog dependency {token:?}",
                        entry_path.display()
                    )));
                }
            }
        }
    }
    Ok(())
}

fn test_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::other(message.into()))
}
