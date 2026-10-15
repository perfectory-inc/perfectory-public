//! The publisher's library crates depend in one direction only (root ADR-0200 §2).
//!
//! A lane crate may depend on `foundation-publisher-core` and on no other lane; the core depends on
//! no lane. Cargo already refuses a cycle, but not a lane reaching into another lane: that compiles,
//! and quietly turns two crates that build side by side back into a chain, so a change in one lane
//! rebuilds the other. The manifests under `crates/publisher/` are the one statement of the graph;
//! this test reads them.

use std::path::{Path, PathBuf};

const CORE: &str = "foundation-publisher-core";
const PREFIX: &str = "foundation-publisher-";

/// The publisher crates a manifest names in any dependency table, plain or target-specific.
fn publisher_dependencies(manifest: &str) -> Vec<String> {
    let mut in_dependency_table = false;
    let mut found = Vec::new();
    for line in manifest.lines().map(str::trim) {
        if let Some(header) = line
            .strip_prefix('[')
            .and_then(|rest| rest.strip_suffix(']'))
        {
            // `[dependencies.foundation-publisher-x]` names the dependency in the header itself.
            let table = header.rsplit_once("dependencies.").map(|(_, name)| name);
            if let Some(name) = table.filter(|name| name.starts_with(PREFIX)) {
                found.push(name.to_owned());
            }
            in_dependency_table = header.ends_with("dependencies");
            continue;
        }
        if !in_dependency_table {
            continue;
        }
        if let Some((key, _)) = line.split_once('=') {
            let key = key.trim().trim_matches('"');
            if key.starts_with(PREFIX) {
                found.push(key.to_owned());
            }
        }
    }
    found
}

/// Every edge between publisher crates the direction forbids, as `from -> to`.
fn violations(manifests: &[(String, String)]) -> Vec<String> {
    let mut found = Vec::new();
    for (crate_name, manifest) in manifests {
        for dependency in publisher_dependencies(manifest) {
            let allowed = crate_name != CORE && dependency == CORE;
            if !allowed {
                found.push(format!("{crate_name} -> {dependency}"));
            }
        }
    }
    found
}

fn publisher_crates_dir() -> anyhow::Result<PathBuf> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    Ok(Path::new(&manifest_dir).join("../../crates/publisher"))
}

fn manifest(name: &str, dependencies: &str) -> (String, String) {
    (
        name.to_owned(),
        format!("[package]\nname = \"{name}\"\n\n[dependencies]\n{dependencies}\n"),
    )
}

#[test]
fn the_real_publisher_crates_depend_on_the_core_only() -> anyhow::Result<()> {
    let mut manifests = Vec::new();
    for entry in std::fs::read_dir(publisher_crates_dir()?)? {
        let directory = entry?.path();
        let path = directory.join("Cargo.toml");
        if path.is_file() {
            let name = directory
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| anyhow::anyhow!("crate directory without a UTF-8 name"))?
                .to_owned();
            manifests.push((name, std::fs::read_to_string(path)?));
        }
    }
    // An empty directory would make the check pass by reading nothing.
    anyhow::ensure!(
        manifests.iter().any(|(name, _)| name == CORE),
        "{CORE} not found under {}",
        publisher_crates_dir()?.display()
    );
    anyhow::ensure!(manifests.len() >= 2, "no lane crate found beside the core");
    let found = violations(&manifests);
    anyhow::ensure!(
        found.is_empty(),
        "forbidden publisher crate edges: {found:?}"
    );
    Ok(())
}

#[test]
fn a_lane_on_the_core_is_allowed() {
    let manifests = [
        manifest(CORE, "anyhow = { workspace = true }"),
        manifest(
            "foundation-publisher-by-pnu",
            "foundation-publisher-core = { workspace = true }",
        ),
    ];
    assert!(violations(&manifests).is_empty());
}

#[test]
fn a_lane_on_another_lane_is_refused() {
    let manifests = [manifest(
        "foundation-publisher-by-pnu",
        "foundation-publisher-core = { workspace = true }\nfoundation-publisher-hub = { workspace = true }",
    )];
    assert_eq!(
        violations(&manifests),
        ["foundation-publisher-by-pnu -> foundation-publisher-hub"]
    );
}

#[test]
fn the_core_on_a_lane_is_refused() {
    let manifests = [manifest(
        CORE,
        "foundation-publisher-by-pnu = { workspace = true }",
    )];
    assert_eq!(
        violations(&manifests),
        ["foundation-publisher-core -> foundation-publisher-by-pnu"]
    );
}

#[test]
fn a_lane_edge_in_a_dev_target_or_dotted_table_is_still_refused() {
    let manifests = [(
        "foundation-publisher-by-pnu".to_owned(),
        "[dev-dependencies]\nfoundation-publisher-hub = { workspace = true }\n\n\
         [target.'cfg(unix)'.dependencies]\n\"foundation-publisher-tiles\" = { workspace = true }\n\n\
         [dependencies.foundation-publisher-spatial]\nworkspace = true\n"
            .to_owned(),
    )];
    assert_eq!(
        violations(&manifests),
        [
            "foundation-publisher-by-pnu -> foundation-publisher-hub",
            "foundation-publisher-by-pnu -> foundation-publisher-tiles",
            "foundation-publisher-by-pnu -> foundation-publisher-spatial",
        ]
    );
}
