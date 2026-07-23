use std::fs;

use anyhow::{bail, Context};
use collection_domain::canonical_page_size;
use serde_json::{json, Value as JsonValue};

use crate::public_data_control_support::{git_head, read_json, repo_relative_path, utc_now};

mod config;
mod jobs;
mod scope;
mod source_inputs;
mod support;
mod validation;

use config::{ProviderSet, WriterConfig};
use jobs::{build_jobs, build_shards, provider_request_counts};
use scope::{read_scope_jsonl, validate_scope_evidence, validate_scope_rows};
use source_inputs::{read_endpoint_catalog, read_national_page_count_plan};
use support::*;
use validation::{
    assert_ready_inputs, assert_real_transaction_endpoints, building_register_operations,
    real_transaction_operations, validate_building_endpoint_policy,
};

// Re-exported so the sibling `jobs` submodule (and its tests) can name `super::ScopeRow`.
pub(super) use scope::ScopeRow;

const MANIFEST_SCHEMA_VERSION: &str =
    "foundation-platform.national_data_collection_shard_manifest.v1";
const APPROVAL_SCHEMA_VERSION: &str =
    "foundation-platform.national_data_collection_rollout_approval.v1";
const PILOT_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_run_evidence.v1";
const SCOPE_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_scope.v1";
const SCOPE_ROW_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_scope_row.v1";
const DEFAULT_BUILDING_REGISTER_OPERATION: &str = "getBrTitleInfo";
const FIXED_PAGE_COUNT_FALLBACK_REQUEST_CAP_CEILING: u64 = 100;

pub fn run() -> anyhow::Result<()> {
    let config = WriterConfig::from_env()?;
    let manifest = build_manifest(&config)?;
    if let Some(parent) = config.output_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(&manifest).context("failed to serialize manifest")?;
    fs::write(&config.output_path, bytes)
        .with_context(|| format!("failed to write {}", config.output_path.display()))?;

    let scopes = manifest
        .pointer("/scope_source/row_count")
        .and_then(JsonValue::as_u64)
        .unwrap_or(0);
    let shards = manifest
        .pointer("/sharding/shard_count")
        .and_then(JsonValue::as_u64)
        .unwrap_or(0);
    let jobs = manifest
        .pointer("/sharding/total_job_count")
        .and_then(JsonValue::as_u64)
        .unwrap_or(0);
    let requests = manifest
        .pointer("/request_plan/estimated_request_count_total")
        .and_then(JsonValue::as_u64)
        .unwrap_or(0);
    println!(
        "national-data-collection-shard-manifest-written status=ready scopes={scopes} shards={shards} jobs={jobs} requests={requests} path={}",
        repo_relative_path(&config.root, &config.output_path)
    );
    Ok(())
}

fn build_manifest(config: &WriterConfig) -> anyhow::Result<JsonValue> {
    let approval = read_json(&config.approval_path, "national rollout approval")?;
    let pilot_evidence = read_json(&config.pilot_evidence_path, "national pilot evidence")?;
    let scope_evidence = read_json(&config.scope_evidence_path, "national scope evidence")?;
    let endpoint_catalog = read_endpoint_catalog(&config.endpoint_catalog_path)?;
    let page_count_plan = config
        .page_count_plan_path
        .as_ref()
        .map(|path| read_national_page_count_plan(&config.root, path))
        .transpose()?;

    assert_ready_inputs(&approval, &pilot_evidence)?;
    validate_scope_evidence(config, &scope_evidence)?;

    let building_operations = if config.provider_set.includes_building_register() {
        building_register_operations(page_count_plan.as_ref())?
    } else {
        Vec::new()
    };
    validate_building_endpoint_policy(
        config.provider_set,
        &endpoint_catalog,
        &building_operations,
    )?;

    let real_transaction_selected = config.provider_set == ProviderSet::RealTransaction
        || (config.provider_set == ProviderSet::All
            && !config.real_transaction_start_deal_ymd.is_empty());
    let real_transaction_deal_ymds = if real_transaction_selected {
        if config.real_transaction_max_pages < 1 {
            bail!(
                "RealTransactionMaxPages is required when real-transaction collection is selected"
            );
        }
        if config.real_transaction_start_deal_ymd.is_empty()
            || config.real_transaction_end_deal_ymd.is_empty()
        {
            bail!("RealTransactionStartDealYmd and RealTransactionEndDealYmd are required when real-transaction collection is selected");
        }
        deal_ymd_range(
            &config.real_transaction_start_deal_ymd,
            &config.real_transaction_end_deal_ymd,
        )?
    } else {
        Vec::new()
    };
    let real_transaction_operations = if real_transaction_selected {
        real_transaction_operations(config, &endpoint_catalog)?
    } else {
        Vec::new()
    };
    if real_transaction_selected {
        assert_real_transaction_endpoints(&endpoint_catalog, &real_transaction_operations)?;
    }

    let scope_rows = read_scope_jsonl(&config.scope_jsonl_path)?;
    validate_scope_rows(&scope_rows, &scope_evidence)?;
    let jobs = build_jobs(
        config,
        &scope_rows,
        &building_operations,
        &real_transaction_operations,
        &real_transaction_deal_ymds,
        &endpoint_catalog,
        page_count_plan.as_ref(),
    )?;
    let estimated_request_count = jobs.iter().map(job_request_count).sum::<u64>();
    if estimated_request_count > config.request_cap {
        bail!(
            "estimated request count exceeds RequestCap: estimated={} cap={}",
            estimated_request_count,
            config.request_cap
        );
    }
    let shards = build_shards(&jobs, config.shard_size);
    let provider_counts = provider_request_counts(&jobs);

    // The real-transaction provider summary reports the canonical page size derived from the
    // SSOT (ADR 0016 D-A), not a config knob. All selected operations pin to the same value, so
    // the first selected operation is representative; `null` when real-transaction is not selected.
    let real_transaction_num_of_rows = match real_transaction_operations.first() {
        Some(operation) => json!(canonical_page_size(operation).with_context(|| {
            format!("no canonical page size for real-transaction operation {operation}")
        })?),
        None => JsonValue::Null,
    };

    Ok(json!({
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": utc_now(),
        "git_head": git_head(&config.root),
        "status": "ready",
        "run_mode": "national",
        "completion_claim_allowed": false,
        "production_cutover_allowed": false,
        "national_rollout_allowed": false,
        "national_rollout_blocked_reason": "manifest_only_no_public_api_execution",
        "endpoint_catalog": {
            "path": repo_relative_path(&config.root, &config.endpoint_catalog_path),
            "sha256": endpoint_catalog.sha256,
            "schema_version": endpoint_catalog.schema_version,
            "endpoint_count": endpoint_catalog.endpoint_count,
        },
        "approval": {
            "path": repo_relative_path(&config.root, &config.approval_path),
            "status": string_property(&approval, "status"),
            "approved_scope": string_property(&approval, "approved_scope"),
            "national_rollout_allowed": bool_property(&approval, "national_rollout_allowed").unwrap_or(false),
        },
        "pilot_evidence": {
            "path": repo_relative_path(&config.root, &config.pilot_evidence_path),
            "status": string_property(&pilot_evidence, "status"),
            "request_count_total": u64_pointer(&pilot_evidence, "/public_api_quota/request_count_total").unwrap_or(0),
        },
        "scope_source": {
            "format": "jsonl",
            "path": repo_relative_path(&config.root, &config.scope_jsonl_path),
            "projection_path": string_property(&scope_evidence, "csv_projection_path"),
            "evidence_path": repo_relative_path(&config.root, &config.scope_evidence_path),
            "row_count": scope_rows.len(),
            "source_rows": u64_property(&scope_evidence, "source_row_count").unwrap_or(0),
            "registry_path": string_property(&scope_evidence, "registry_path"),
            "registry_sha256": string_property(&scope_evidence, "registry_sha256"),
            "registry_rows": u64_property(&scope_evidence, "registry_row_count").unwrap_or(0),
            "granularity": "legal_dong",
        },
        "request_plan": {
            "request_cap": config.request_cap,
            "estimated_request_count_total": estimated_request_count,
            "page_count_source": if page_count_plan.is_some() { "national_page_count_plan" } else { "fixed_parameter" },
            "page_count_plan": page_count_plan.as_ref().map(|plan| json!({
                "path": plan.path,
                "sha256": plan.sha256,
                "job_count": plan.job_count,
            })).unwrap_or(JsonValue::Null),
            "fixed_page_count_fallback": if page_count_plan.is_none() && config.confirm_fixed_page_count_fallback {
                json!({
                    "status": "bounded_proof",
                    "confirmed": true,
                    "request_cap_ceiling": FIXED_PAGE_COUNT_FALLBACK_REQUEST_CAP_CEILING,
                })
            } else {
                JsonValue::Null
            },
            "providers": {
                "data_go_kr_building_register": {
                    "request_count_estimate": provider_counts.building_register,
                    "source_acquisition_policy": "endpoint_catalog_lane",
                    "bulk_alternative_endpoint_count": endpoint_catalog.building_hub_bulk_endpoint_count,
                    "endpoint_slug": "data-go-kr-building-register-getBrTitleInfo",
                    "operation": DEFAULT_BUILDING_REGISTER_OPERATION,
                    "endpoint_slugs": building_operations.iter().map(|operation| building_endpoint_slug(operation)).collect::<Vec<_>>(),
                    "operations": building_operations,
                    "page_count_source": if page_count_plan.is_some() { "national_page_count_plan" } else { "fixed_parameter" },
                    "page_window_size": config.building_page_window_size,
                },
                "data_go_kr_real_transaction": {
                    "request_count_estimate": provider_counts.real_transaction,
                    "source_acquisition_policy": "endpoint_catalog_lane",
                    "endpoint_slugs": real_transaction_operations.iter().map(|operation| real_transaction_endpoint_slug(operation)).collect::<Vec<_>>(),
                    "operations": real_transaction_operations,
                    "deal_ymd_start": real_transaction_deal_ymds.first().cloned().unwrap_or_default(),
                    "deal_ymd_end": real_transaction_deal_ymds.last().cloned().unwrap_or_default(),
                    "month_count": real_transaction_deal_ymds.len(),
                    "scope_granularity": "sigungu_month",
                    "page_count_source": "fixed_parameter",
                    "max_pages": config.real_transaction_max_pages,
                    "num_of_rows": real_transaction_num_of_rows,
                },
                "vworld_cadastral": {
                    "request_count_estimate": provider_counts.vworld_cadastral,
                    "endpoint_slug": "vworld-dataset-parcel",
                    "endpoint": "ingest-vworld-cadastral",
                    "dataset": "LP_PA_CBND_BUBUN",
                    "filter_strategy": "legal_dong_to_vworld_emd_attr_filter",
                    "filter_field": "emdCd",
                    "page_count_source": if page_count_plan.is_some() { "national_page_count_plan" } else { "fixed_parameter" },
                },
                "vworld_land_register": {
                    "request_count_estimate": provider_counts.vworld_land_register,
                    "endpoint_slug": "vworld-dataset-land_register",
                    "endpoint": "ingest-vworld-land-register",
                    "operation": "ladfrlList",
                    "filter_strategy": "legal_dong_to_vworld_pnu_prefix",
                    "filter_field": "pnu",
                    "page_count_source": if page_count_plan.is_some() { "national_page_count_plan" } else { "fixed_parameter" },
                },
            },
        },
        "sharding": {
            "shard_count": shards.len(),
            "total_job_count": jobs.len(),
            "jobs_per_shard_max": config.shard_size,
            "retry_policy": "resume_by_job_idempotency_key",
        },
        "shards": shards,
        "evidence_limitations": [
            "manifest_only",
            "does_not_execute_public_api_requests",
            "does_not_promote_silver_gold_national_tables",
            "does_not_approve_production_cutover",
        ],
        "next_gates": [
            "national-data-collection-shard-execution",
            "silver-gold-national-promotion",
        ],
    }))
}
