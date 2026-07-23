//! Input-readiness and endpoint-policy validation.
//!
//! Asserts the approval/pilot-evidence inputs are in a `ready` state and that the building-register
//! and real-transaction operations are permitted by the public source endpoint catalog
//! (source-acquisition lane policy, ADR 0014).

use std::collections::BTreeSet;

use anyhow::bail;
use serde_json::Value as JsonValue;

use super::config::{ProviderSet, WriterConfig};
use super::source_inputs::{EndpointCatalog, PageCountPlan};
use super::support::*;
use super::{APPROVAL_SCHEMA_VERSION, DEFAULT_BUILDING_REGISTER_OPERATION, PILOT_SCHEMA_VERSION};

pub(super) fn assert_ready_inputs(
    approval: &JsonValue,
    pilot_evidence: &JsonValue,
) -> anyhow::Result<()> {
    if string_property(approval, "schema_version") != APPROVAL_SCHEMA_VERSION {
        bail!("national rollout approval schema mismatch");
    }
    if string_property(approval, "status") != "ready" {
        bail!("national rollout approval status must be ready");
    }
    if !bool_property(approval, "national_rollout_allowed").unwrap_or(false) {
        bail!("national rollout approval must allow planning the national shard manifest");
    }
    if string_property(pilot_evidence, "schema_version") != PILOT_SCHEMA_VERSION {
        bail!("national pilot evidence schema mismatch");
    }
    if string_property(pilot_evidence, "status") != "ready" {
        bail!("national pilot evidence status must be ready");
    }
    if string_property(pilot_evidence, "run_mode") != "pilot" {
        bail!("national pilot evidence must be a pilot run");
    }
    if !bool_property(pilot_evidence, "raw_response_preserved").unwrap_or(false) {
        bail!("national pilot evidence must preserve raw responses");
    }
    Ok(())
}

pub(super) fn building_register_operations(
    page_count_plan: Option<&PageCountPlan>,
) -> anyhow::Result<Vec<String>> {
    let Some(page_count_plan) = page_count_plan else {
        return Ok(vec![DEFAULT_BUILDING_REGISTER_OPERATION.to_owned()]);
    };
    let mut operations = BTreeSet::new();
    for job in page_count_plan.jobs_by_id.values() {
        if string_property(job, "provider") != "data.go.kr" {
            continue;
        }
        let operation = string_property(job, "operation");
        if !is_building_operation(&operation) {
            bail!("national page count plan building-register operation invalid: {operation}");
        }
        operations.insert(operation);
    }
    if operations.is_empty() {
        bail!("national page count plan must contain at least one building-register operation");
    }
    let mut ordered = Vec::new();
    if operations.remove(DEFAULT_BUILDING_REGISTER_OPERATION) {
        ordered.push(DEFAULT_BUILDING_REGISTER_OPERATION.to_owned());
    }
    ordered.extend(operations);
    Ok(ordered)
}

pub(super) fn real_transaction_operations(
    config: &WriterConfig,
    endpoint_catalog: &EndpointCatalog,
) -> anyhow::Result<Vec<String>> {
    if !config.real_transaction_operations.is_empty() {
        return Ok(config.real_transaction_operations.clone());
    }
    if endpoint_catalog.real_transaction_operations.is_empty() {
        bail!("public source endpoint catalog must contain real_transaction_open_api operations");
    }
    Ok(endpoint_catalog.real_transaction_operations.clone())
}

pub(super) fn assert_real_transaction_endpoints(
    endpoint_catalog: &EndpointCatalog,
    operations: &[String],
) -> anyhow::Result<()> {
    for operation in operations {
        let endpoint_slug = real_transaction_endpoint_slug(operation);
        if !endpoint_catalog.endpoint_slugs.contains(&endpoint_slug) {
            bail!(
                "public source endpoint catalog missing real-transaction endpoint: {endpoint_slug}"
            );
        }
        let Some(metadata) = endpoint_catalog
            .endpoint_metadata_by_slug
            .get(&endpoint_slug)
        else {
            bail!("public source endpoint catalog missing endpoint metadata: {endpoint_slug}");
        };
        if !metadata.national_collection_allowed
            || metadata.source_acquisition_lane != "open_api_only"
        {
            bail!(
                "real-transaction endpoint disabled for national collection by source acquisition lane: endpoint={} lane={}",
                endpoint_slug,
                metadata.source_acquisition_lane
            );
        }
    }
    Ok(())
}

pub(super) fn validate_building_endpoint_policy(
    provider_set: ProviderSet,
    endpoint_catalog: &EndpointCatalog,
    operations: &[String],
) -> anyhow::Result<()> {
    for operation in operations {
        let endpoint_slug = building_endpoint_slug(operation);
        if !endpoint_catalog.endpoint_slugs.contains(&endpoint_slug) {
            bail!("public source endpoint catalog missing building-register endpoint: {endpoint_slug}");
        }
        let Some(metadata) = endpoint_catalog
            .endpoint_metadata_by_slug
            .get(&endpoint_slug)
        else {
            bail!("public source endpoint catalog missing endpoint metadata: {endpoint_slug}");
        };
        if !metadata.national_collection_allowed
            || metadata.source_acquisition_lane == "disabled_api_duplicate"
        {
            bail!(
                "building-register OpenAPI endpoint disabled for national collection by source acquisition lane: endpoint={} lane={}",
                endpoint_slug,
                metadata.source_acquisition_lane
            );
        }
    }
    if provider_set.includes_building_register()
        && endpoint_catalog.building_hub_bulk_endpoint_count > 0
        && !operations.is_empty()
    {
        bail!("building register national Bronze must use building_hub_bulk when bulk_file endpoints exist; data.go.kr building-register OpenAPI national shard manifests are disabled");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::national_data_collection_shard_manifest_writer::source_inputs::EndpointMetadata;

    #[test]
    fn real_transaction_duplicate_api_lane_is_not_national_collection_source() {
        let endpoint_slug = "data-go-kr-real-transaction-getRTMSDataSvcAptTradeDev".to_owned();
        let endpoint_catalog = EndpointCatalog {
            schema_version: "foundation-platform.public_source_endpoint_catalog.v1".to_owned(),
            endpoint_count: 1,
            sha256: "fixture".to_owned(),
            endpoint_slugs: BTreeSet::from([endpoint_slug.clone()]),
            endpoint_metadata_by_slug: BTreeMap::from([(
                endpoint_slug,
                EndpointMetadata {
                    source_acquisition_lane: "disabled_api_duplicate".to_owned(),
                    national_collection_allowed: false,
                    source_slug: "datagokr__real_transaction_apartment_trade".to_owned(),
                },
            )]),
            building_hub_bulk_endpoint_count: 0,
            real_transaction_operations: vec!["getRTMSDataSvcAptTradeDev".to_owned()],
        };

        let error = assert_real_transaction_endpoints(
            &endpoint_catalog,
            &["getRTMSDataSvcAptTradeDev".to_owned()],
        )
        .err()
        .expect("duplicate API lane must be rejected for national collection");

        assert!(
            error
                .to_string()
                .contains("real-transaction endpoint disabled for national collection"),
            "unexpected error: {error}"
        );
    }
}
