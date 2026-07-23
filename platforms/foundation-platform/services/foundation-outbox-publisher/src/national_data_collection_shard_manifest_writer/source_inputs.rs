use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use anyhow::bail;
use serde_json::Value as JsonValue;

use crate::public_data_control_support::{read_json, repo_relative_path};

use super::support::*;

const ENDPOINT_CATALOG_SCHEMA_VERSION: &str =
    "foundation-platform.public_source_endpoint_catalog.v1";
const PAGE_COUNT_PLAN_SCHEMA_VERSION: &str = "foundation-platform.national_page_count_plan.v1";

#[derive(Clone, Debug)]
pub(super) struct EndpointCatalog {
    pub(super) schema_version: String,
    pub(super) endpoint_count: usize,
    pub(super) sha256: String,
    pub(super) endpoint_slugs: BTreeSet<String>,
    pub(super) endpoint_metadata_by_slug: BTreeMap<String, EndpointMetadata>,
    pub(super) building_hub_bulk_endpoint_count: usize,
    pub(super) real_transaction_operations: Vec<String>,
}

#[derive(Clone, Debug)]
pub(super) struct EndpointMetadata {
    pub(super) source_acquisition_lane: String,
    pub(super) national_collection_allowed: bool,
    pub(super) source_slug: String,
}

#[derive(Clone, Debug)]
pub(super) struct PageCountPlan {
    pub(super) path: String,
    pub(super) sha256: String,
    pub(super) job_count: usize,
    pub(super) jobs_by_id: BTreeMap<String, JsonValue>,
}

pub(super) fn read_endpoint_catalog(path: &Path) -> anyhow::Result<EndpointCatalog> {
    let catalog = read_json(path, "public source endpoint catalog")?;
    if string_property(&catalog, "schema_version") != ENDPOINT_CATALOG_SCHEMA_VERSION {
        bail!("public source endpoint catalog schema mismatch");
    }
    if string_property(&catalog, "status") != "ready" {
        bail!("public source endpoint catalog status must be ready");
    }
    if string_property(&catalog, "owner") != "foundation-platform" {
        bail!("public source endpoint catalog owner must be foundation-platform");
    }

    let mut endpoint_slugs = BTreeSet::new();
    let mut endpoint_metadata_by_slug = BTreeMap::new();
    let mut building_hub_bulk_endpoint_count = 0_usize;
    let mut real_transaction_operations = Vec::new();
    for endpoint in array_property(&catalog, "endpoints") {
        let endpoint_slug = string_property(&endpoint, "endpoint_slug");
        if endpoint_slug.is_empty() {
            bail!("public source endpoint catalog contains an endpoint without endpoint_slug");
        }
        if !endpoint_slugs.insert(endpoint_slug.clone()) {
            bail!("public source endpoint catalog duplicate endpoint_slug: {endpoint_slug}");
        }
        let group = string_property(&endpoint, "group");
        let access_kind = string_property(&endpoint, "access_kind");
        let operation = string_property(&endpoint, "operation");
        let source_acquisition_lane = string_property(&endpoint, "source_acquisition_lane");
        if source_acquisition_lane.is_empty() {
            bail!("public source endpoint catalog endpoint missing source_acquisition_lane: {endpoint_slug}");
        }
        let source_slug = endpoint
            .get("bronze")
            .map(|bronze| string_property(bronze, "source_slug"))
            .unwrap_or_default();
        let metadata = EndpointMetadata {
            source_acquisition_lane,
            national_collection_allowed: bool_property(&endpoint, "national_collection_allowed")
                .unwrap_or(false),
            source_slug,
        };
        if group == "building_hub_bulk" && access_kind == "bulk_file" {
            building_hub_bulk_endpoint_count += 1;
        }
        if group == "real_transaction_open_api" && is_real_transaction_operation(&operation) {
            real_transaction_operations.push(operation);
        }
        endpoint_metadata_by_slug.insert(endpoint_slug, metadata);
    }
    for required in [
        "data-go-kr-building-register-getBrTitleInfo",
        "vworld-dataset-parcel",
        "vworld-dataset-land_register",
    ] {
        if !endpoint_slugs.contains(required) {
            bail!("public source endpoint catalog missing required national collection endpoint: {required}");
        }
    }
    real_transaction_operations.sort();

    Ok(EndpointCatalog {
        schema_version: ENDPOINT_CATALOG_SCHEMA_VERSION.to_owned(),
        endpoint_count: endpoint_slugs.len(),
        sha256: sha256_file_hex(path)?,
        endpoint_slugs,
        endpoint_metadata_by_slug,
        building_hub_bulk_endpoint_count,
        real_transaction_operations,
    })
}

pub(super) fn read_national_page_count_plan(
    root: &Path,
    path: &Path,
) -> anyhow::Result<PageCountPlan> {
    let plan = read_json(path, "national page count plan")?;
    if string_property(&plan, "schema_version") != PAGE_COUNT_PLAN_SCHEMA_VERSION {
        bail!("national page count plan schema mismatch");
    }
    if string_property(&plan, "status") != "ready" {
        bail!("national page count plan status must be ready");
    }
    let mut jobs_by_id = BTreeMap::new();
    for job in array_property(&plan, "jobs") {
        let job_id = string_property(&job, "job_id");
        if !valid_page_count_job_id(&job_id) {
            bail!("national page count plan job_id invalid: {job_id}");
        }
        if jobs_by_id.contains_key(&job_id) {
            bail!("national page count plan duplicate job_id: {job_id}");
        }
        validate_page_count_job(&job, &job_id)?;
        jobs_by_id.insert(job_id, job);
    }
    if jobs_by_id.is_empty() {
        bail!("national page count plan jobs must not be empty");
    }
    Ok(PageCountPlan {
        path: repo_relative_path(root, path),
        sha256: sha256_file_hex(path)?,
        job_count: jobs_by_id.len(),
        jobs_by_id,
    })
}

fn validate_page_count_job(job: &JsonValue, job_id: &str) -> anyhow::Result<()> {
    let provider = string_property(job, "provider");
    let endpoint_slug = string_property(job, "endpoint_slug");
    let sigungu = string_property(job, "sigungu_cd");
    let bjdong = string_property(job, "bjdong_cd");
    let scope_unit_id = string_property(job, "scope_unit_id");
    let requested_page_size = u64_property(job, "requested_page_size").unwrap_or(0);
    let effective_page_size = u64_property(job, "effective_page_size").unwrap_or(0);
    let provider_total_count = i64_property(job, "provider_total_count").unwrap_or(-1);
    let required_pages = u64_property(job, "required_pages").unwrap_or(0);

    if !is_digits(&sigungu, 5) || !is_digits(&bjdong, 5) {
        bail!("national page count plan sigungu_cd and bjdong_cd must be five digits: {job_id}");
    }
    if scope_unit_id != format!("scope:legal-dong:{sigungu}{bjdong}") {
        bail!("national page count plan scope_unit_id must match legal-dong code: {job_id}");
    }
    if requested_page_size < 1 || effective_page_size < 1 {
        bail!("national page count plan page sizes must be positive: {job_id}");
    }
    if provider_total_count < 0 {
        bail!("national page count plan provider_total_count must be non-negative: {job_id}");
    }
    if required_pages
        != required_pages_for_total_count(provider_total_count as u64, effective_page_size)
    {
        bail!("national page count plan required_pages must match provider_total_count and effective_page_size: {job_id}");
    }

    if job_id.starts_with("building-register-") {
        let operation = string_property(job, "operation");
        if provider != "data.go.kr"
            || endpoint_slug != building_endpoint_slug(&operation)
            || job_id != building_job_id(&operation, &sigungu, &bjdong)
        {
            bail!("national page count plan building-register contract mismatch: {job_id}");
        }
    } else if job_id.starts_with("vworld-cadastral-") {
        if provider != "VWorld"
            || endpoint_slug != "vworld-dataset-parcel"
            || string_property(job, "dataset") != "LP_PA_CBND_BUBUN"
        {
            bail!("national page count plan VWorld cadastral contract mismatch: {job_id}");
        }
    } else if job_id.starts_with("vworld-land-register-")
        && (provider != "VWorld"
            || endpoint_slug != "vworld-dataset-land_register"
            || string_property(job, "operation") != "ladfrlList")
    {
        bail!("national page count plan VWorld land-register contract mismatch: {job_id}");
    }
    Ok(())
}
