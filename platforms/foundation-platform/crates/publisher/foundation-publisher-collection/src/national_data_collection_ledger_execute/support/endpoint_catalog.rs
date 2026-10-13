use std::{collections::BTreeMap, path::PathBuf};

use anyhow::bail;
use serde_json::Value as JsonValue;

use crate::public_data_control_support::{read_json, resolve_repo_path};

use super::super::ENDPOINT_CATALOG_SCHEMA_VERSION;
use super::{bool_prop, file_sha256, is_sha256, string_at, string_prop, Config};

pub(in crate::national_data_collection_ledger_execute) struct EndpointPolicy {
    pub(in crate::national_data_collection_ledger_execute) source_acquisition_lane: String,
    pub(in crate::national_data_collection_ledger_execute) national_collection_allowed: bool,
}

pub(in crate::national_data_collection_ledger_execute) fn load_endpoint_catalog(
    config: &Config,
    plan: &JsonValue,
) -> anyhow::Result<BTreeMap<String, EndpointPolicy>> {
    let catalog_path = string_at(plan, &["endpoint_catalog", "path"]);
    let catalog_sha256 = string_at(plan, &["endpoint_catalog", "sha256"]);
    if catalog_path.is_empty() {
        bail!("national collection plan endpoint_catalog.path is required");
    }
    if !is_sha256(&catalog_sha256) {
        bail!("national collection plan endpoint_catalog.sha256 must be sha256");
    }
    let resolved = resolve_repo_path(
        &config.root,
        &PathBuf::from(catalog_path),
        "endpoint_catalog.path",
    )?;
    if !resolved.is_file() {
        bail!("national collection plan endpoint catalog missing");
    }
    if file_sha256(&resolved)? != catalog_sha256 {
        bail!("national collection plan endpoint_catalog.sha256 must match file");
    }
    let catalog = read_json(&resolved, "endpoint catalog")?;
    if string_prop(&catalog, "schema_version") != ENDPOINT_CATALOG_SCHEMA_VERSION {
        bail!("national collection endpoint catalog schema mismatch");
    }
    let mut policies = BTreeMap::new();
    for endpoint in catalog
        .get("endpoints")
        .and_then(JsonValue::as_array)
        .into_iter()
        .flatten()
    {
        let endpoint_slug = string_prop(endpoint, "endpoint_slug");
        if endpoint_slug.is_empty() {
            continue;
        }
        let source_acquisition_lane = string_prop(endpoint, "source_acquisition_lane");
        if source_acquisition_lane.is_empty() {
            bail!("endpoint catalog endpoint missing source_acquisition_lane: {endpoint_slug}");
        }
        policies.insert(
            endpoint_slug,
            EndpointPolicy {
                source_acquisition_lane,
                national_collection_allowed: bool_prop(
                    endpoint,
                    "national_collection_allowed",
                    false,
                ),
            },
        );
    }
    Ok(policies)
}
