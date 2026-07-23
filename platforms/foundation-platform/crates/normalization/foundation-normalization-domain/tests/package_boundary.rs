//! Executable ownership boundary for the Foundation Normalization capability.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    error::Error,
    fs, io,
    path::{Path, PathBuf},
    process::Command,
};

use serde::Deserialize;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const NORMALIZATION_DOMAIN_ONLY: &[&str] = &["foundation-normalization-domain"];

const FORBIDDEN_PACKAGE_TOKENS: &[&str] = &[
    "foundation_normalization_domain",
    "foundation_normalization_application",
    "foundation_normalization_infrastructure",
    "foundation-normalization-domain",
    "foundation-normalization-application",
    "foundation-normalization-infrastructure",
];

const FORBIDDEN_NORMALIZATION_TABLES: &[&str] = &[
    "catalog.normalization_proposal",
    "catalog.normalization_proposal_review",
    "catalog.normalization_application",
];

const FORBIDDEN_CATALOG_EXPORTS: &[&str] = &[
    "pub mod normalization;",
    "pub use normalization::",
    "PgNormalizationProposalUnitOfWork",
    "NormalizationProposalUnitOfWork",
    "ActiveBuildingRegisterUnitOverrideReader",
];

const MOVED_CATALOG_PATHS: &[&str] = &[
    "crates/catalog/catalog-domain/src/building_register_floor.rs",
    "crates/catalog/catalog-domain/src/building_register_floor",
    "crates/catalog/catalog-domain/src/building_register_unit.rs",
    "crates/catalog/catalog-domain/src/semantic_entity_impact.rs",
    "crates/catalog/catalog-domain/src/semantic_metadata.rs",
    "crates/catalog/catalog-domain/src/normalization.rs",
    "crates/catalog/catalog-application/src/normalization.rs",
    "crates/catalog/catalog-infrastructure/src/normalization.rs",
    "crates/catalog/catalog-infrastructure/src/normalization",
];

#[test]
fn cargo_graph_enforces_normalization_dependency_direction() -> TestResult {
    let metadata = load_cargo_metadata(&workspace_root()?)?;

    for package in workspace_package_names(&metadata)? {
        if package.starts_with("catalog-") || package.starts_with("collection-") {
            assert_no_forbidden_normalization_path(&metadata, package, &[])?;
        } else if package.starts_with("lakehouse-") {
            assert_no_forbidden_normalization_path(&metadata, package, NORMALIZATION_DOMAIN_ONLY)?;
        }
    }
    Ok(())
}

#[test]
fn catalog_and_collection_source_trees_do_not_import_normalization_packages() -> TestResult {
    let workspace = workspace_root()?;
    for relative in [
        "crates/catalog/catalog-domain",
        "crates/catalog/catalog-application",
        "crates/catalog/catalog-infrastructure",
        "crates/collection/collection-domain",
        "crates/collection/collection-application",
        "crates/collection/collection-infrastructure",
    ] {
        assert_tree_absent(&workspace.join(relative), FORBIDDEN_PACKAGE_TOKENS)?;
    }
    Ok(())
}

#[test]
fn normalization_tables_are_owned_only_by_normalization_infrastructure() -> TestResult {
    let workspace = workspace_root()?;
    for relative in ["crates", "services"] {
        assert_normalization_table_ownership(&workspace.join(relative))?;
    }
    Ok(())
}

#[test]
fn moved_normalization_ownership_cannot_return_to_catalog() -> TestResult {
    let workspace = workspace_root()?;
    for relative in MOVED_CATALOG_PATHS {
        let path = workspace.join(relative);
        if path.exists() {
            return Err(test_error(format!(
                "moved Normalization path returned to Catalog: {}",
                path.display()
            )));
        }
    }

    for relative in [
        "crates/catalog/catalog-domain/src/lib.rs",
        "crates/catalog/catalog-application/src/lib.rs",
        "crates/catalog/catalog-application/src/ports.rs",
        "crates/catalog/catalog-infrastructure/src/lib.rs",
    ] {
        assert_absent(&workspace.join(relative), FORBIDDEN_CATALOG_EXPORTS)?;
    }
    Ok(())
}

#[test]
fn dependency_scanner_rejects_direct_transitive_and_future_packages() -> TestResult {
    let metadata = CargoMetadata::fixture(
        &[
            ("catalog-domain", "catalog"),
            ("bridge", "bridge"),
            (
                "foundation-normalization-worker",
                "foundation-normalization-worker",
            ),
        ],
        &[
            ("catalog", &["bridge"]),
            ("bridge", &["foundation-normalization-worker"]),
            ("foundation-normalization-worker", &[]),
        ],
    );

    assert_eq!(
        forbidden_normalization_dependency_path(&metadata, "catalog-domain", &[])?,
        Some(vec![
            "catalog-domain".to_owned(),
            "bridge".to_owned(),
            "foundation-normalization-worker".to_owned(),
        ])
    );
    Ok(())
}

#[test]
fn lakehouse_scanner_allows_domain_but_rejects_higher_normalization_layers() -> TestResult {
    let allowed = CargoMetadata::fixture(
        &[
            ("lakehouse-application", "lakehouse"),
            ("foundation-normalization-domain", "domain"),
        ],
        &[("lakehouse", &["domain"]), ("domain", &[])],
    );
    assert_eq!(
        forbidden_normalization_dependency_path(
            &allowed,
            "lakehouse-application",
            NORMALIZATION_DOMAIN_ONLY,
        )?,
        None
    );

    let forbidden = CargoMetadata::fixture(
        &[
            ("lakehouse-application", "lakehouse"),
            ("foundation-normalization-domain", "domain"),
            ("foundation-normalization-infrastructure", "infrastructure"),
        ],
        &[
            ("lakehouse", &["domain"]),
            ("domain", &["infrastructure"]),
            ("infrastructure", &[]),
        ],
    );
    assert_eq!(
        forbidden_normalization_dependency_path(
            &forbidden,
            "lakehouse-application",
            NORMALIZATION_DOMAIN_ONLY,
        )?,
        Some(vec![
            "lakehouse-application".to_owned(),
            "foundation-normalization-domain".to_owned(),
            "foundation-normalization-infrastructure".to_owned(),
        ])
    );
    Ok(())
}

#[test]
fn dependency_scanner_fails_closed_for_incomplete_metadata() {
    let missing_resolve = CargoMetadata {
        packages: vec![CargoPackage::fixture("catalog-domain", "catalog")],
        resolve: None,
        workspace_members: vec!["catalog".to_owned()],
    };
    let missing_package = CargoMetadata {
        packages: Vec::new(),
        resolve: Some(CargoResolve { nodes: Vec::new() }),
        workspace_members: vec!["catalog".to_owned()],
    };
    let missing_start_node = CargoMetadata {
        packages: vec![CargoPackage::fixture("catalog-domain", "catalog")],
        resolve: Some(CargoResolve { nodes: Vec::new() }),
        workspace_members: vec!["catalog".to_owned()],
    };
    let missing_dependency_package = CargoMetadata::fixture(
        &[("catalog-domain", "catalog")],
        &[("catalog", &["missing"])],
    );

    assert!(
        forbidden_normalization_dependency_path(&missing_resolve, "catalog-domain", &[]).is_err()
    );
    assert!(
        forbidden_normalization_dependency_path(&missing_package, "catalog-domain", &[]).is_err()
    );
    assert!(
        forbidden_normalization_dependency_path(&missing_start_node, "catalog-domain", &[])
            .is_err()
    );
    assert!(forbidden_normalization_dependency_path(
        &missing_dependency_package,
        "catalog-domain",
        &[],
    )
    .is_err());
    assert!(workspace_package_names(&missing_package).is_err());
}

#[test]
fn source_scanner_rejects_forbidden_fixtures() {
    assert_eq!(
        find_forbidden(
            "use foundation_normalization_infrastructure::PgNormalizationUnitOfWork;",
            FORBIDDEN_PACKAGE_TOKENS,
        ),
        Some("foundation_normalization_infrastructure")
    );
    assert_eq!(
        find_forbidden(
            "SELECT * FROM catalog.normalization_application",
            FORBIDDEN_NORMALIZATION_TABLES,
        ),
        Some("catalog.normalization_application")
    );
}

#[test]
fn normalization_table_owner_path_is_exclusive() {
    assert!(is_normalization_table_owner(Path::new(
        "crates/normalization/foundation-normalization-infrastructure/src/proposal.rs"
    )));
    assert!(!is_normalization_table_owner(Path::new(
        "crates/catalog/catalog-infrastructure/src/repository.rs"
    )));
    assert!(!is_normalization_table_owner(Path::new(
        "services/foundation-outbox-publisher/src/main.rs"
    )));
}

#[test]
fn normalization_http_does_not_borrow_catalog_route_errors() -> TestResult {
    let route = workspace_root()?.join("services/foundation-api/src/routes/normalization.rs");
    assert_absent(&route, &["catalog::ApiError", "use super::catalog;"])
}

fn workspace_root() -> TestResult<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .map(Path::to_path_buf)
        .ok_or_else(|| {
            test_error("foundation-normalization-domain must live under crates/normalization")
        })
}

fn assert_no_forbidden_normalization_path(
    metadata: &CargoMetadata,
    package: &str,
    allowed: &[&str],
) -> TestResult {
    if let Some(path) = forbidden_normalization_dependency_path(metadata, package, allowed)? {
        return Err(test_error(format!(
            "forbidden dependency path: {}",
            path.join(" -> ")
        )));
    }
    Ok(())
}

fn assert_tree_absent(root: &Path, forbidden: &[&str]) -> TestResult {
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "tests") {
                continue;
            }
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

fn assert_normalization_table_ownership(root: &Path) -> TestResult {
    if is_normalization_table_owner(root) {
        return Ok(());
    }
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "tests") {
                continue;
            }
            assert_normalization_table_ownership(&path)?;
            continue;
        }
        if matches!(
            path.extension().and_then(|value| value.to_str()),
            Some("rs" | "sql")
        ) {
            assert_absent(&path, FORBIDDEN_NORMALIZATION_TABLES)?;
        }
    }
    Ok(())
}

fn is_normalization_table_owner(path: &Path) -> bool {
    let normalized = path.to_string_lossy().replace('\\', "/");
    normalized.starts_with("crates/normalization/foundation-normalization-infrastructure/")
        || normalized.contains("/crates/normalization/foundation-normalization-infrastructure/")
}

fn assert_absent(path: &Path, forbidden: &[&str]) -> TestResult {
    let contents = fs::read_to_string(path)?;
    if let Some(token) = find_forbidden(&contents, forbidden) {
        return Err(test_error(format!(
            "{} contains forbidden Normalization token {token:?}",
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

fn workspace_package_names(metadata: &CargoMetadata) -> TestResult<Vec<&str>> {
    let names_by_id: HashMap<&str, &str> = metadata
        .packages
        .iter()
        .map(|package| (package.id.as_str(), package.name.as_str()))
        .collect();
    metadata
        .workspace_members
        .iter()
        .map(|id| {
            names_by_id.get(id.as_str()).copied().ok_or_else(|| {
                test_error(format!(
                    "cargo metadata workspace references unknown package id {id}"
                ))
            })
        })
        .collect()
}

fn forbidden_normalization_dependency_path(
    metadata: &CargoMetadata,
    start_name: &str,
    allowed: &[&str],
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

    let mut pending = VecDeque::from([(start_id, vec![start_name.to_owned()])]);
    let mut visited = HashSet::new();
    while let Some((id, path)) = pending.pop_front() {
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
            if dependency_name.starts_with("foundation-normalization-")
                && !allowed.contains(&dependency_name)
            {
                return Ok(Some(dependency_path));
            }
            pending.push_back((dependency_id, dependency_path));
        }
    }
    Ok(None)
}

#[derive(Debug, Deserialize)]
struct CargoMetadata {
    packages: Vec<CargoPackage>,
    resolve: Option<CargoResolve>,
    workspace_members: Vec<String>,
}

impl CargoMetadata {
    fn fixture(packages: &[(&str, &str)], nodes: &[(&str, &[&str])]) -> Self {
        Self {
            workspace_members: packages.iter().map(|(_, id)| (*id).to_owned()).collect(),
            packages: packages
                .iter()
                .map(|(name, id)| CargoPackage::fixture(name, id))
                .collect(),
            resolve: Some(CargoResolve {
                nodes: nodes
                    .iter()
                    .map(|(id, dependencies)| CargoNode::fixture(id, dependencies))
                    .collect(),
            }),
        }
    }
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
