//! Executable ownership boundary between Catalog, Collection, and Lakehouse packages.

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fs, io,
    path::{Path, PathBuf},
    process::Command,
};

use serde::Deserialize;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const FORBIDDEN_CATALOG_DOMAIN_EXPORTS: &[&str] = &[
    "pub mod industrial_complex_gold_pointer;",
    "pub mod lakehouse;",
    "pub mod lakehouse_maintenance;",
    "pub mod lakehouse_registry;",
    "pub mod lakehouse_run_summary;",
    "pub use industrial_complex_gold_pointer::",
    "pub use lakehouse::",
    "pub use lakehouse_maintenance::",
    "pub use lakehouse_registry::",
    "pub use lakehouse_run_summary::",
];

const FORBIDDEN_CATALOG_APPLICATION_MODULES: &[&str] = &[
    "pub mod building_register_floor_silver_plan;",
    "pub mod building_register_title;",
    "pub mod building_register_unit_area_silver_plan;",
    "pub mod building_register_unit_silver_plan;",
    "pub mod build_industrial_complex_silver_handoff;",
    "pub mod get_lakehouse_promotion_candidate;",
    "pub mod industrial_complex_silver_plan;",
    "pub mod publish_industrial_complex_gold_pointer;",
    "pub mod record_lakehouse_batch_run;",
    "pub mod register_lakehouse_object_artifact;",
    "pub mod vworld_cadastral_silver_plan;",
];

const FORBIDDEN_CATALOG_APPLICATION_PORTS: &[&str] = &[
    "trait LakehouseCatalog",
    "trait LakehouseBatchRunRepository",
    "trait LakehouseBatchRunAudit",
    "trait LakehouseRegistryUnitOfWork",
    "trait IndustrialComplexMaterializationReader",
    "trait IndustrialComplexGoldPointerReader",
    "trait LakehousePublicationUnitOfWork",
];

const FORBIDDEN_CATALOG_INFRASTRUCTURE_EXPORTS: &[&str] = &[
    "pub mod iceberg_rest_catalog;",
    "pub mod lakehouse_batch_audit;",
    "pub mod lakehouse_config;",
    "pub mod lakehouse_registry;",
    "pub use iceberg_rest_catalog::",
    "pub use lakehouse_batch_audit::",
    "pub use lakehouse_config::",
    "pub use lakehouse_registry::",
];

const FORBIDDEN_CATALOG_ROW_MAPPERS: &[&str] = &["row_to_industrial_complex_gold_pointer"];

const PACKAGES_WITHOUT_LAKEHOUSE_DEPENDENCIES: &[&str] = &[
    "catalog-domain",
    "catalog-application",
    "catalog-infrastructure",
    "collection-domain",
    "collection-application",
    "collection-infrastructure",
];

const FORBIDDEN_CATALOG_DEPENDENCY_TOKENS: &[&str] = &[
    "lakehouse_domain",
    "lakehouse_application",
    "lakehouse_infrastructure",
    "lakehouse-domain",
    "lakehouse-application",
    "lakehouse-infrastructure",
];

const MOVED_CATALOG_PATHS: &[&str] = &[
    "crates/catalog/catalog-domain/src/industrial_complex_gold_pointer.rs",
    "crates/catalog/catalog-domain/src/lakehouse.rs",
    "crates/catalog/catalog-domain/src/lakehouse_maintenance.rs",
    "crates/catalog/catalog-domain/src/lakehouse_registry.rs",
    "crates/catalog/catalog-domain/src/lakehouse_run_summary.rs",
    "crates/catalog/catalog-application/src/building_register_floor_silver_plan.rs",
    "crates/catalog/catalog-application/src/building_register_floor_silver_plan",
    "crates/catalog/catalog-application/src/building_register_title.rs",
    "crates/catalog/catalog-application/src/building_register_unit_area_silver_plan.rs",
    "crates/catalog/catalog-application/src/building_register_unit_silver_plan.rs",
    "crates/catalog/catalog-application/src/build_industrial_complex_silver_handoff.rs",
    "crates/catalog/catalog-application/src/get_lakehouse_promotion_candidate.rs",
    "crates/catalog/catalog-application/src/industrial_complex_silver_plan.rs",
    "crates/catalog/catalog-application/src/publish_industrial_complex_gold_pointer.rs",
    "crates/catalog/catalog-application/src/record_lakehouse_batch_run.rs",
    "crates/catalog/catalog-application/src/register_lakehouse_object_artifact.rs",
    "crates/catalog/catalog-application/src/vworld_cadastral_silver_plan.rs",
    "crates/catalog/catalog-infrastructure/src/iceberg_rest_catalog.rs",
    "crates/catalog/catalog-infrastructure/src/lakehouse_batch_audit.rs",
    "crates/catalog/catalog-infrastructure/src/lakehouse_config.rs",
    "crates/catalog/catalog-infrastructure/src/lakehouse_registry.rs",
];

#[test]
fn catalog_packages_do_not_own_lakehouse_modules() -> TestResult {
    let workspace = workspace_root()?;
    assert_absent(
        &workspace.join("crates/catalog/catalog-domain/src/lib.rs"),
        FORBIDDEN_CATALOG_DOMAIN_EXPORTS,
    )?;
    assert_absent(
        &workspace.join("crates/catalog/catalog-application/src/lib.rs"),
        FORBIDDEN_CATALOG_APPLICATION_MODULES,
    )?;
    assert_absent(
        &workspace.join("crates/catalog/catalog-application/src/ports.rs"),
        FORBIDDEN_CATALOG_APPLICATION_PORTS,
    )?;
    assert_absent(
        &workspace.join("crates/catalog/catalog-infrastructure/src/lib.rs"),
        FORBIDDEN_CATALOG_INFRASTRUCTURE_EXPORTS,
    )?;
    assert_absent(
        &workspace.join("crates/catalog/catalog-infrastructure/src/row_map.rs"),
        FORBIDDEN_CATALOG_ROW_MAPPERS,
    )?;
    Ok(())
}

#[test]
fn catalog_and_collection_packages_do_not_depend_on_lakehouse_packages() -> TestResult {
    let metadata = load_cargo_metadata(&workspace_root()?)?;
    for package in PACKAGES_WITHOUT_LAKEHOUSE_DEPENDENCIES {
        if let Some(path) = forbidden_lakehouse_dependency_path(&metadata, package)? {
            return Err(test_error(format!(
                "forbidden dependency path: {}",
                path.join(" -> ")
            )));
        }
    }
    Ok(())
}

#[test]
fn catalog_source_tree_does_not_reference_lakehouse_packages() -> TestResult {
    let workspace = workspace_root()?;
    for package in [
        "crates/catalog/catalog-domain",
        "crates/catalog/catalog-application",
        "crates/catalog/catalog-infrastructure",
    ] {
        assert_tree_absent(
            &workspace.join(package),
            FORBIDDEN_CATALOG_DEPENDENCY_TOKENS,
        )?;
    }
    Ok(())
}

#[test]
fn moved_lakehouse_files_do_not_return_to_catalog() -> TestResult {
    let workspace = workspace_root()?;
    for relative_path in MOVED_CATALOG_PATHS {
        let path = workspace.join(relative_path);
        if path.exists() {
            return Err(test_error(format!(
                "moved Lakehouse path returned to Catalog: {}",
                path.display()
            )));
        }
    }
    Ok(())
}

#[test]
fn boundary_scanner_rejects_forbidden_source_fixtures() {
    assert_eq!(
        find_forbidden(
            "pub mod lakehouse;\npub use lakehouse::LakehouseTableContract;\n",
            FORBIDDEN_CATALOG_DOMAIN_EXPORTS,
        ),
        Some("pub mod lakehouse;")
    );
    assert_eq!(
        find_forbidden(
            "pub mod record_lakehouse_batch_run;\n",
            FORBIDDEN_CATALOG_APPLICATION_MODULES,
        ),
        Some("pub mod record_lakehouse_batch_run;")
    );
    assert_eq!(
        find_forbidden(
            "pub mod iceberg_rest_catalog;\n",
            FORBIDDEN_CATALOG_INFRASTRUCTURE_EXPORTS,
        ),
        Some("pub mod iceberg_rest_catalog;")
    );
}

#[test]
fn dependency_scanner_rejects_a_transitive_lakehouse_fixture() -> TestResult {
    let metadata = CargoMetadata {
        packages: vec![
            CargoPackage::fixture("catalog-domain", "catalog"),
            CargoPackage::fixture("bridge", "bridge"),
            CargoPackage::fixture("lakehouse-domain", "lakehouse"),
        ],
        resolve: Some(CargoResolve {
            nodes: vec![
                CargoNode::fixture("catalog", &["bridge"]),
                CargoNode::fixture("bridge", &["lakehouse"]),
                CargoNode::fixture("lakehouse", &[]),
            ],
        }),
    };

    assert_eq!(
        forbidden_lakehouse_dependency_path(&metadata, "catalog-domain")?,
        Some(vec![
            "catalog-domain".to_owned(),
            "bridge".to_owned(),
            "lakehouse-domain".to_owned(),
        ])
    );
    Ok(())
}

#[test]
fn dependency_scanner_rejects_future_lakehouse_packages() -> TestResult {
    let metadata = CargoMetadata {
        packages: vec![
            CargoPackage::fixture("catalog-domain", "catalog"),
            CargoPackage::fixture("lakehouse-maintenance", "maintenance"),
        ],
        resolve: Some(CargoResolve {
            nodes: vec![
                CargoNode::fixture("catalog", &["maintenance"]),
                CargoNode::fixture("maintenance", &[]),
            ],
        }),
    };

    assert_eq!(
        forbidden_lakehouse_dependency_path(&metadata, "catalog-domain")?,
        Some(vec![
            "catalog-domain".to_owned(),
            "lakehouse-maintenance".to_owned(),
        ])
    );
    Ok(())
}

#[test]
fn dependency_scanner_fails_closed_for_incomplete_metadata() {
    let missing_resolve = CargoMetadata {
        packages: vec![CargoPackage::fixture("catalog-domain", "catalog")],
        resolve: None,
    };
    let missing_package = CargoMetadata {
        packages: Vec::new(),
        resolve: Some(CargoResolve { nodes: Vec::new() }),
    };
    let missing_start_node = CargoMetadata {
        packages: vec![CargoPackage::fixture("catalog-domain", "catalog")],
        resolve: Some(CargoResolve { nodes: Vec::new() }),
    };
    let missing_dependency_package = CargoMetadata {
        packages: vec![CargoPackage::fixture("catalog-domain", "catalog")],
        resolve: Some(CargoResolve {
            nodes: vec![CargoNode::fixture("catalog", &["missing"])],
        }),
    };

    assert!(forbidden_lakehouse_dependency_path(&missing_resolve, "catalog-domain").is_err());
    assert!(forbidden_lakehouse_dependency_path(&missing_package, "catalog-domain").is_err());
    assert!(forbidden_lakehouse_dependency_path(&missing_start_node, "catalog-domain").is_err());
    assert!(
        forbidden_lakehouse_dependency_path(&missing_dependency_package, "catalog-domain").is_err()
    );
}

fn workspace_root() -> TestResult<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .map(Path::to_path_buf)
        .ok_or_else(|| test_error("lakehouse-domain must live under crates/lakehouse"))
}

fn assert_absent(path: &Path, forbidden: &[&str]) -> TestResult {
    let contents = fs::read_to_string(path)?;
    if let Some(token) = find_forbidden(&contents, forbidden) {
        return Err(test_error(format!(
            "{} still owns forbidden Lakehouse token {token:?}",
            path.display()
        )));
    }
    Ok(())
}

fn assert_tree_absent(root: &Path, forbidden: &[&str]) -> TestResult {
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            assert_tree_absent(&path, forbidden)?;
            continue;
        }
        let is_rust = path.extension().is_some_and(|extension| extension == "rs");
        let is_manifest = path.file_name().is_some_and(|name| name == "Cargo.toml");
        if is_rust || is_manifest {
            assert_absent(&path, forbidden)?;
        }
    }
    Ok(())
}

fn find_forbidden<'a>(contents: &str, forbidden: &'a [&str]) -> Option<&'a str> {
    forbidden
        .iter()
        .copied()
        .find(|token| contents.contains(token))
}

fn load_cargo_metadata(workspace: &Path) -> TestResult<CargoMetadata> {
    let cargo = option_env!("CARGO").unwrap_or("cargo");
    let output = Command::new(cargo)
        .args(["metadata", "--format-version", "1", "--locked"])
        .current_dir(workspace)
        .output()?;
    if !output.status.success() {
        return Err(test_error(format!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    serde_json::from_slice(&output.stdout).map_err(Into::into)
}

fn forbidden_lakehouse_dependency_path(
    metadata: &CargoMetadata,
    start_name: &str,
) -> TestResult<Option<Vec<String>>> {
    let names_by_id: HashMap<&str, &str> = metadata
        .packages
        .iter()
        .map(|package| (package.id.as_str(), package.name.as_str()))
        .collect();
    let resolve = metadata
        .resolve
        .as_ref()
        .ok_or_else(|| test_error("cargo metadata is missing its resolve graph"))?;
    let dependencies_by_id: HashMap<&str, Vec<&str>> = resolve
        .nodes
        .iter()
        .map(|node| {
            (
                node.id.as_str(),
                node.deps
                    .iter()
                    .map(|dependency| dependency.pkg.as_str())
                    .collect(),
            )
        })
        .collect();
    let start_id = metadata
        .packages
        .iter()
        .find(|package| package.name == start_name)
        .ok_or_else(|| test_error(format!("cargo metadata is missing package {start_name}")))?
        .id
        .as_str();
    if !dependencies_by_id.contains_key(start_id) {
        return Err(test_error(format!(
            "cargo metadata resolve graph is missing node for package {start_name}"
        )));
    }
    let mut pending = vec![(start_id, vec![start_name.to_owned()])];
    let mut visited = HashSet::new();

    while let Some((id, path)) = pending.pop() {
        if !visited.insert(id) {
            continue;
        }
        for dependency_id in dependencies_by_id.get(id).into_iter().flatten() {
            let dependency_name = names_by_id.get(dependency_id).copied().ok_or_else(|| {
                test_error(format!(
                    "cargo metadata resolve graph references unknown package id {dependency_id}"
                ))
            })?;
            let mut dependency_path = path.clone();
            dependency_path.push(dependency_name.to_owned());
            if dependency_name.starts_with("lakehouse-") {
                return Ok(Some(dependency_path));
            }
            pending.push((dependency_id, dependency_path));
        }
    }
    Ok(None)
}

#[derive(Debug, Deserialize)]
struct CargoMetadata {
    packages: Vec<CargoPackage>,
    resolve: Option<CargoResolve>,
}

#[derive(Debug, Deserialize)]
struct CargoPackage {
    name: String,
    id: String,
}

impl CargoPackage {
    fn fixture(name: &str, id: &str) -> Self {
        Self {
            name: name.to_owned(),
            id: id.to_owned(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct CargoResolve {
    nodes: Vec<CargoNode>,
}

#[derive(Debug, Deserialize)]
struct CargoNode {
    id: String,
    deps: Vec<CargoDependency>,
}

impl CargoNode {
    fn fixture(id: &str, dependencies: &[&str]) -> Self {
        Self {
            id: id.to_owned(),
            deps: dependencies
                .iter()
                .map(|dependency| CargoDependency {
                    pkg: (*dependency).to_owned(),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct CargoDependency {
    pkg: String,
}

fn test_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::other(message.into()))
}
