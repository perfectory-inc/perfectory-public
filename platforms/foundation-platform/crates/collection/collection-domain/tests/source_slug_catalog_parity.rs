//! Parity test: the public source-endpoint catalog's `bronze.source_slug` is a DERIVED projection
//! of the single generator `collection_domain::source_slug(provider, dataset_slug)` (ADR 0014 D3).
//!
//! For every entry with an approved provider domain this asserts
//! `entry.bronze.source_slug == source_slug(entry.provider, entry.dataset_slug)`, which makes the
//! catalog file unable to drift away from the generator. The 10 `mixed_public_source` / POI entries
//! are the only named non-provider sentinel: they keep their legacy slug and carry no `dataset_slug`
//! (Phase 2 scope boundary). Every other unregistered label fails.

use std::{error::Error, path::Path, path::PathBuf};

use collection_domain::{
    building_register_dataset_slug, provider_id, real_transaction_dataset_slug, source_slug,
    vworld_ned_dataset_slug,
};
use serde_json::Value;

type TestResult = Result<(), Box<dyn Error>>;

const LEGACY_NON_PROVIDER_SENTINELS: [&str; 1] = ["mixed_public_source"];
/// Every `docs/catalog/*.json` that carries a provider label, discovered rather than listed.
///
/// A hand-written list is the defect this crate exists to remove: a fifth catalog document that
/// grows a `provider` key would be silently unscanned. The directory is the collector, so the set
/// widens on its own. Documents with no provider label at all are skipped here and asserted
/// non-empty only for those that do carry one.
fn provider_documents() -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let dir = workspace_root()?.join("docs/catalog");
    let mut found = Vec::new();
    for entry in std::fs::read_dir(&dir)? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        found.push(path);
    }
    found.sort();
    if found.is_empty() {
        return Err(format!("no catalog JSON documents under {}", dir.display()).into());
    }
    Ok(found)
}

#[test]
fn catalog_source_slug_is_derived_from_generator() -> TestResult {
    let raw = std::fs::read_to_string(catalog_path()?)?;
    let catalog: Value = serde_json::from_str(&raw)?;
    let endpoints = catalog["endpoints"]
        .as_array()
        .ok_or("endpoints must be a JSON array")?;

    let mut checked = 0_usize;
    let mut skipped = 0_usize;
    for entry in endpoints {
        let provider = entry["provider"]
            .as_str()
            .ok_or("entry.provider must be a string")?;

        // The named non-provider sentinel keeps its legacy slug. Any other unregistered label is a
        // hard failure rather than a silent skip.
        if provider_id(provider).is_none() {
            assert_registered_provider_or_sentinel(provider)?;
            assert!(
                entry.get("dataset_slug").is_none(),
                "legacy non-provider sentinel {provider:?} must not carry a dataset_slug"
            );
            skipped += 1;
            continue;
        }

        let dataset_slug = entry["dataset_slug"].as_str().ok_or_else(|| {
            format!("in-scope entry for provider {provider:?} must carry a string dataset_slug")
        })?;
        let actual = entry["bronze"]["source_slug"]
            .as_str()
            .ok_or("entry.bronze.source_slug must be a string")?;

        let expected = source_slug(provider, dataset_slug)?;
        assert_eq!(
            actual, expected,
            "catalog bronze.source_slug drifted from generator for provider={provider:?} \
             dataset_slug={dataset_slug:?}"
        );
        checked += 1;
    }

    // Guardrail: the in-scope / out-of-scope split must stay as authored (123 in-scope, 10 mixed).
    // The three ILIS entries joined when the industrial-complex address source got a collector.
    assert_eq!(checked, 123, "expected 123 in-scope catalog entries");
    assert_eq!(
        skipped, 10,
        "expected 10 skipped mixed_public_source entries"
    );
    Ok(())
}

#[test]
fn every_provider_document_uses_registered_labels_or_named_sentinel() -> TestResult {
    let mut documents_with_labels = 0usize;
    for path in provider_documents()? {
        let raw = std::fs::read_to_string(&path)?;
        let document: Value = serde_json::from_str(&raw)?;
        let file_name = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("<unnamed>");
        let mut providers = Vec::new();
        collect_provider_labels(&document, &mut providers)?;
        if providers.is_empty() {
            continue;
        }
        documents_with_labels += 1;
        for provider in providers {
            assert_registered_provider_or_sentinel(provider).map_err(|error| {
                format!("{file_name} contains an invalid provider label: {error}")
            })?;
        }
    }
    // The discovery must not silently match nothing: four documents carry provider labels today,
    // and a drop to zero would make this test vacuous rather than failing.
    assert!(
        documents_with_labels >= 4,
        "expected at least four catalog documents carrying provider labels, found \
         {documents_with_labels}"
    );
    Ok(())
}

#[test]
fn unknown_non_sentinel_provider_is_rejected() -> TestResult {
    let error = assert_registered_provider_or_sentinel("unknown-provider.invalid")
        .err()
        .ok_or("an unregistered non-sentinel provider must fail")?;
    assert_eq!(
        error,
        "unregistered provider label \"unknown-provider.invalid\" is not an approved legacy sentinel"
    );
    Ok(())
}

/// Parity: the in-code data.go.kr `operation -> dataset_slug` maps must agree with the catalog's
/// `(operation, dataset_slug)` pairs, so the catalog JSON stays the human-facing SSOT and neither the
/// building-register nor real-transaction producer can drift its slug away from it.
#[test]
fn operation_dataset_slug_maps_match_catalog() -> TestResult {
    let raw = std::fs::read_to_string(catalog_path()?)?;
    let catalog: Value = serde_json::from_str(&raw)?;
    let endpoints = catalog["endpoints"]
        .as_array()
        .ok_or("endpoints must be a JSON array")?;

    let mut building_checked = 0_usize;
    let mut real_transaction_checked = 0_usize;
    for entry in endpoints {
        let provider = entry["provider"]
            .as_str()
            .ok_or("entry.provider must be a string")?;
        if provider != "data.go.kr" {
            continue;
        }
        let operation = entry["operation"]
            .as_str()
            .ok_or("data.go.kr entry must carry a string operation")?;
        let dataset_slug = entry["dataset_slug"]
            .as_str()
            .ok_or("data.go.kr entry must carry a string dataset_slug")?;

        if let Some(code_slug) = building_register_dataset_slug(operation) {
            assert_eq!(
                code_slug, dataset_slug,
                "building-register operation->dataset_slug map drifted from catalog for {operation:?}"
            );
            building_checked += 1;
        } else if let Some(code_slug) = real_transaction_dataset_slug(operation) {
            assert_eq!(
                code_slug, dataset_slug,
                "real-transaction operation->dataset_slug map drifted from catalog for {operation:?}"
            );
            real_transaction_checked += 1;
        } else {
            return Err(format!(
                "catalog data.go.kr operation {operation:?} is not covered by either in-code map"
            )
            .into());
        }
    }

    assert_eq!(
        building_checked, 10,
        "expected all 10 building-register operations to match the catalog"
    );
    assert_eq!(
        real_transaction_checked, 12,
        "expected all 12 real-transaction operations to match the catalog"
    );
    Ok(())
}

/// Parity: every `dataset_slug` the in-code V-World NED `operation -> dataset_slug` map produces must
/// exist as a `VWorld` `dataset_slug` in the catalog, so the map (whose provider-native operations
/// drive the Bronze object-key operation collapse, ADR 0016 T1.2 / D-C) cannot point at a dataset the
/// catalog does not define. The catalog's V-World `operation` field is the canonical `snake_case`
/// dataset name (== `dataset_slug`), distinct from the provider-native API operation in the code map,
/// so this parity is keyed on the resolved `dataset_slug`, not the operation string.
#[test]
fn vworld_ned_dataset_slug_map_targets_exist_in_catalog() -> TestResult {
    let raw = std::fs::read_to_string(catalog_path()?)?;
    let catalog: Value = serde_json::from_str(&raw)?;
    let endpoints = catalog["endpoints"]
        .as_array()
        .ok_or("endpoints must be a JSON array")?;

    let vworld_dataset_slugs: Vec<&str> = endpoints
        .iter()
        .filter(|entry| entry["provider"].as_str() == Some("vworld.kr"))
        .filter_map(|entry| entry["dataset_slug"].as_str())
        .collect();

    // The seven provider-native V-World NED operations the code map covers.
    let provider_operations = [
        "ladfrlList",
        "getLandCharacteristic",
        "getIndvdLandPriceAttr",
        "getPossessionAttr",
        "getLandUseAttr",
        "getLandMoveAttr",
        "ldaregList",
    ];
    for operation in provider_operations {
        let dataset_slug =
            vworld_ned_dataset_slug(operation).ok_or("V-World NED operation must resolve")?;
        assert!(
            vworld_dataset_slugs.contains(&dataset_slug),
            "V-World NED map dataset_slug {dataset_slug:?} (operation {operation:?}) is absent from the catalog"
        );
    }
    Ok(())
}

fn catalog_path() -> Result<PathBuf, &'static str> {
    Ok(workspace_root()?.join("docs/catalog/public-source-endpoint-catalog.v1.json"))
}

fn workspace_root() -> Result<PathBuf, &'static str> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .map(Path::to_path_buf)
        .ok_or("collection-domain manifest must live under crates/collection/collection-domain")
}

fn collect_provider_labels<'a>(value: &'a Value, providers: &mut Vec<&'a str>) -> TestResult {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                if key == "provider" {
                    providers.push(child.as_str().ok_or("provider field must be a string")?);
                } else {
                    collect_provider_labels(child, providers)?;
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_provider_labels(item, providers)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn assert_registered_provider_or_sentinel(provider: &str) -> Result<(), String> {
    if provider_id(provider).is_some() || LEGACY_NON_PROVIDER_SENTINELS.contains(&provider) {
        return Ok(());
    }
    Err(format!(
        "unregistered provider label {provider:?} is not an approved legacy sentinel"
    ))
}
