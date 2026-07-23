//! Shard-manifest job builders.
//!
//! Builds the per-scope provider jobs (building-register, VWorld cadastral/land-register,
//! real-transaction), groups them into shards, and derives provider request counts. The
//! `JsonValue` job objects and their `source_slug` derivation (ADR 0014) are shared across all
//! providers here.

use std::collections::BTreeSet;

use anyhow::{bail, Context};
use collection_domain::{
    building_register_dataset_slug, canonical_page_size, real_transaction_dataset_slug, source_slug,
};
use serde_json::{json, Map, Value as JsonValue};

use super::config::WriterConfig;
use super::source_inputs::{EndpointCatalog, PageCountPlan};
use super::support::*;
use super::ScopeRow;

pub(super) fn build_jobs(
    config: &WriterConfig,
    scope_rows: &[ScopeRow],
    building_operations: &[String],
    real_transaction_operations: &[String],
    real_transaction_deal_ymds: &[String],
    endpoint_catalog: &EndpointCatalog,
    page_count_plan: Option<&PageCountPlan>,
) -> anyhow::Result<Vec<JsonValue>> {
    let mut jobs = Vec::new();
    let mut sigungu_seen = BTreeSet::new();
    let mut sigungu_rows = Vec::new();
    for row in scope_rows {
        if sigungu_seen.insert(row.sigungu_cd.clone()) {
            sigungu_rows.push(row.clone());
        }
        if config.provider_set.includes_building_register() {
            for operation in building_operations {
                jobs.extend(new_building_register_jobs(
                    config,
                    row,
                    operation,
                    page_count_plan,
                )?);
            }
        }
        if config.provider_set.includes_vworld_cadastral() {
            jobs.push(new_vworld_cadastral_job(config, row, page_count_plan)?);
        }
        if config.provider_set.includes_land_register() {
            jobs.push(new_vworld_land_register_job(config, row, page_count_plan)?);
        }
    }
    if !real_transaction_operations.is_empty() {
        for sigungu_row in sigungu_rows {
            for operation in real_transaction_operations {
                for deal_ymd in real_transaction_deal_ymds {
                    jobs.push(new_real_transaction_job(
                        config,
                        &sigungu_row,
                        operation,
                        deal_ymd,
                        endpoint_catalog,
                    )?);
                }
            }
        }
    }
    Ok(jobs)
}

pub(super) fn new_building_register_jobs(
    config: &WriterConfig,
    row: &ScopeRow,
    operation: &str,
    page_count_plan: Option<&PageCountPlan>,
) -> anyhow::Result<Vec<JsonValue>> {
    let job_id = building_job_id(operation, &row.sigungu_cd, &row.bjdong_cd);
    let endpoint_slug = building_endpoint_slug(operation);
    // Page size is derived from the canonical SSOT keyed by `operation`, never a config knob.
    let num_of_rows = national_canonical_page_size(operation)?;
    let mut max_pages = config.building_max_pages;
    let mut page_count_source = "fixed_parameter";
    let mut provider_total_count = JsonValue::Null;
    let mut effective_page_size = JsonValue::Null;
    if let Some(page_count_job) = national_page_count_job(
        page_count_plan,
        &job_id,
        &row.scope_unit_id,
        &endpoint_slug,
        &row.sigungu_cd,
        &row.bjdong_cd,
        num_of_rows,
    )? {
        max_pages = u64_property(page_count_job, "required_pages").unwrap_or(0);
        provider_total_count =
            json!(u64_property(page_count_job, "provider_total_count").unwrap_or(0));
        effective_page_size =
            json!(u64_property(page_count_job, "effective_page_size").unwrap_or(0));
        page_count_source = "national_page_count_plan";
    }

    let mut base = Map::new();
    base.insert("id".to_owned(), json!(job_id));
    base.insert("status".to_owned(), json!("planned"));
    base.insert("provider".to_owned(), json!("data.go.kr"));
    base.insert("endpoint_slug".to_owned(), json!(endpoint_slug));
    base.insert("endpoint".to_owned(), json!(operation));
    base.insert("operation".to_owned(), json!(operation));
    base.insert("scope_unit_id".to_owned(), json!(row.scope_unit_id));
    base.insert("sigungu_cd".to_owned(), json!(row.sigungu_cd));
    base.insert("bjdong_cd".to_owned(), json!(row.bjdong_cd));
    base.insert("num_of_rows".to_owned(), json!(num_of_rows));
    base.insert("page_count_source".to_owned(), json!(page_count_source));
    base.insert("provider_total_count".to_owned(), provider_total_count);
    base.insert("effective_page_size".to_owned(), effective_page_size);
    // ADR 0014 §1.1 / §6: the source slug is canonical + scope-free; the sigungu/bjdong scope
    // lives in the sigungu_cd/bjdong_cd/scope_unit_id job fields and the idempotency_key.
    let dataset_slug = building_register_dataset_slug(operation).with_context(|| {
        format!("no canonical dataset_slug for building-register operation {operation}")
    })?;
    base.insert(
        "source_slug".to_owned(),
        json!(source_slug("data.go.kr", dataset_slug)?),
    );

    if config.building_page_window_size < 1 {
        base.insert("max_pages".to_owned(), json!(max_pages));
        base.insert("request_count_estimate".to_owned(), json!(max_pages));
        base.insert(
            "idempotency_key".to_owned(),
            json!(format!(
                "national/data-go-kr/building-register/{}/{}/{}",
                operation, row.sigungu_cd, row.bjdong_cd
            )),
        );
        return Ok(vec![JsonValue::Object(base)]);
    }

    let mut jobs = Vec::new();
    let mut page_start = 1_u64;
    while page_start <= max_pages {
        let page_end = (page_start + config.building_page_window_size - 1).min(max_pages);
        let window_pages = page_end - page_start + 1;
        let mut window = base.clone();
        window.insert(
            "id".to_owned(),
            json!(format!("{job_id}-p{page_start:06}-{page_end:06}")),
        );
        window.insert("page_start".to_owned(), json!(page_start));
        window.insert("page_end".to_owned(), json!(page_end));
        window.insert("page_count_total".to_owned(), json!(max_pages));
        window.insert("max_pages".to_owned(), json!(window_pages));
        window.insert("request_count_estimate".to_owned(), json!(window_pages));
        window.insert(
            "idempotency_key".to_owned(),
            json!(format!(
                "national/data-go-kr/building-register/{}/{}/{}/pages/{}-{}",
                operation, row.sigungu_cd, row.bjdong_cd, page_start, page_end
            )),
        );
        jobs.push(JsonValue::Object(window));
        page_start += config.building_page_window_size;
    }
    Ok(jobs)
}

fn new_vworld_cadastral_job(
    config: &WriterConfig,
    row: &ScopeRow,
    page_count_plan: Option<&PageCountPlan>,
) -> anyhow::Result<JsonValue> {
    let job_id = format!("vworld-cadastral-{}-{}", row.sigungu_cd, row.bjdong_cd);
    let provider_emd_cd = row.bjdong_code[..8].to_owned();
    // V-World cadastral runs the `GetFeature` operation; its page size comes from the canonical SSOT.
    let size = national_canonical_page_size("GetFeature")?;
    let mut max_pages = config.vworld_max_pages;
    let mut page_count_source = "fixed_parameter";
    let mut provider_total_count = JsonValue::Null;
    let mut effective_page_size = JsonValue::Null;
    let mut provider_empty_reason = String::new();
    if let Some(page_count_job) = national_page_count_job(
        page_count_plan,
        &job_id,
        &row.scope_unit_id,
        "vworld-dataset-parcel",
        &row.sigungu_cd,
        &row.bjdong_cd,
        size,
    )? {
        max_pages = u64_property(page_count_job, "required_pages").unwrap_or(0);
        provider_total_count =
            json!(u64_property(page_count_job, "provider_total_count").unwrap_or(0));
        effective_page_size =
            json!(u64_property(page_count_job, "effective_page_size").unwrap_or(0));
        provider_empty_reason = string_property(page_count_job, "provider_empty_reason");
        page_count_source = "national_page_count_plan";
    }
    let mut job = json_object([
        ("id", json!(job_id)),
        ("status", json!("planned")),
        ("provider", json!("VWorld")),
        ("endpoint_slug", json!("vworld-dataset-parcel")),
        ("endpoint", json!("ingest-vworld-cadastral")),
        ("dataset", json!("LP_PA_CBND_BUBUN")),
        ("scope_unit_id", json!(row.scope_unit_id)),
        ("sigungu_cd", json!(row.sigungu_cd)),
        ("bjdong_cd", json!(row.bjdong_cd)),
        ("bjdong_code", json!(row.bjdong_code)),
        ("provider_emd_cd", json!(provider_emd_cd)),
        ("filter_kind", json!("attr_filter")),
        (
            "attr_filter",
            json!(format!("emdCd:=:{}", &row.bjdong_code[..8])),
        ),
        ("max_pages", json!(max_pages)),
        ("size", json!(size)),
        ("request_count_estimate", json!(max_pages)),
        ("page_count_source", json!(page_count_source)),
        ("provider_total_count", provider_total_count),
        ("effective_page_size", effective_page_size),
        // ADR 0014 §6 (owner-confirmed): the source slug is canonical + scope-free; the
        // sigungu/bjdong scope lives in the idempotency_key and the sigungu_cd/bjdong_cd fields.
        ("source_slug", json!(source_slug("VWorld", "cadastral")?)),
        (
            "idempotency_key",
            json!(format!(
                "national/vworld/cadastral/{}/{}",
                row.sigungu_cd, row.bjdong_cd
            )),
        ),
    ]);
    if !provider_empty_reason.is_empty() {
        job.as_object_mut()
            .context("VWorld cadastral job must be a JSON object")?
            .insert(
                "provider_empty_reason".to_owned(),
                json!(provider_empty_reason),
            );
    }
    Ok(job)
}

fn new_vworld_land_register_job(
    config: &WriterConfig,
    row: &ScopeRow,
    page_count_plan: Option<&PageCountPlan>,
) -> anyhow::Result<JsonValue> {
    let job_id = format!("vworld-land-register-{}-{}", row.sigungu_cd, row.bjdong_cd);
    // V-World land-register runs the `ladfrlList` operation; page size comes from the canonical SSOT.
    let num_of_rows = national_canonical_page_size("ladfrlList")?;
    let mut max_pages = config.land_register_max_pages;
    let mut page_count_source = "fixed_parameter";
    let mut provider_total_count = JsonValue::Null;
    let mut effective_page_size = JsonValue::Null;
    if let Some(page_count_job) = national_page_count_job(
        page_count_plan,
        &job_id,
        &row.scope_unit_id,
        "vworld-dataset-land_register",
        &row.sigungu_cd,
        &row.bjdong_cd,
        num_of_rows,
    )? {
        max_pages = u64_property(page_count_job, "required_pages").unwrap_or(0);
        provider_total_count =
            json!(u64_property(page_count_job, "provider_total_count").unwrap_or(0));
        effective_page_size =
            json!(u64_property(page_count_job, "effective_page_size").unwrap_or(0));
        page_count_source = "national_page_count_plan";
    }
    Ok(json_object([
        ("id", json!(job_id)),
        ("status", json!("planned")),
        ("provider", json!("VWorld")),
        ("endpoint_slug", json!("vworld-dataset-land_register")),
        ("endpoint", json!("ingest-vworld-land-register")),
        ("operation", json!("ladfrlList")),
        ("scope_unit_id", json!(row.scope_unit_id)),
        ("sigungu_cd", json!(row.sigungu_cd)),
        ("bjdong_cd", json!(row.bjdong_cd)),
        ("pnu_prefix", json!(row.bjdong_code)),
        ("max_pages", json!(max_pages)),
        ("num_of_rows", json!(num_of_rows)),
        ("request_count_estimate", json!(max_pages)),
        ("page_count_source", json!(page_count_source)),
        ("provider_total_count", provider_total_count),
        ("effective_page_size", effective_page_size),
        // ADR 0014 §6 (owner-confirmed): canonical + scope-free; scope lives in idempotency_key.
        (
            "source_slug",
            json!(source_slug("VWorld", "land_register")?),
        ),
        (
            "idempotency_key",
            json!(format!(
                "national/vworld/land-register/{}/{}",
                row.sigungu_cd, row.bjdong_cd
            )),
        ),
    ]))
}

fn new_real_transaction_job(
    config: &WriterConfig,
    sigungu_row: &ScopeRow,
    operation: &str,
    deal_ymd: &str,
    endpoint_catalog: &EndpointCatalog,
) -> anyhow::Result<JsonValue> {
    let lawd_cd = &sigungu_row.sigungu_cd;
    // Page size is derived from the canonical SSOT keyed by `operation`, never a config knob.
    let num_of_rows = national_canonical_page_size(operation)?;
    let base_job_id = format!("real-transaction-{operation}-{lawd_cd}-{deal_ymd}");
    let job_id = format!(
        "{base_job_id}-p{start:06}-{end:06}",
        start = 1,
        end = config.real_transaction_max_pages
    );
    let endpoint_slug = real_transaction_endpoint_slug(operation);
    // Prefer the catalog's generator-derived slug; when the catalog has no entry, fold to the
    // canonical generator output (ADR 0014) instead of the old opaque hyphen format.
    let source_slug = match endpoint_catalog
        .endpoint_metadata_by_slug
        .get(&endpoint_slug)
        .map(|metadata| metadata.source_slug.clone())
        .filter(|slug| !slug.is_empty())
    {
        Some(slug) => slug,
        None => {
            let dataset_slug = real_transaction_dataset_slug(operation).with_context(|| {
                format!("real-transaction operation has no registered dataset_slug: {operation}")
            })?;
            source_slug("data.go.kr", dataset_slug)?
        }
    };
    Ok(json_object([
        ("id", json!(job_id)),
        ("status", json!("planned")),
        ("provider", json!("data.go.kr")),
        ("endpoint_slug", json!(endpoint_slug)),
        ("endpoint", json!(operation)),
        ("operation", json!(operation)),
        (
            "scope_unit_id",
            json!(format!("scope:sigungu-month:{lawd_cd}:{deal_ymd}")),
        ),
        ("sigungu_cd", json!(lawd_cd)),
        ("bjdong_cd", json!("")),
        ("lawd_cd", json!(lawd_cd)),
        ("deal_ymd", json!(deal_ymd)),
        ("page_start", json!(1)),
        ("page_end", json!(config.real_transaction_max_pages)),
        ("page_count_total", json!(config.real_transaction_max_pages)),
        ("max_pages", json!(config.real_transaction_max_pages)),
        ("num_of_rows", json!(num_of_rows)),
        (
            "request_count_estimate",
            json!(config.real_transaction_max_pages),
        ),
        ("page_count_source", json!("fixed_parameter")),
        ("source_slug", json!(source_slug)),
        (
            "idempotency_key",
            json!(format!(
                "national/data-go-kr/real-transaction/{operation}/{lawd_cd}/{deal_ymd}/pages/1-{}",
                config.real_transaction_max_pages
            )),
        ),
    ]))
}

pub(super) fn build_shards(jobs: &[JsonValue], shard_size: usize) -> Vec<JsonValue> {
    jobs.chunks(shard_size)
        .enumerate()
        .map(|(index, shard_jobs)| {
            let shard_scopes = shard_jobs
                .iter()
                .map(|job| string_property(job, "scope_unit_id"))
                .collect::<BTreeSet<_>>();
            let request_count = shard_jobs.iter().map(job_request_count).sum::<u64>();
            json!({
                "shard_id": format!("national-shard-{:04}", index + 1),
                "sequence": index + 1,
                "status": "planned",
                "scope_count": shard_scopes.len(),
                "job_count": shard_jobs.len(),
                "request_count_estimate": request_count,
                "jobs": shard_jobs,
            })
        })
        .collect()
}

#[derive(Default)]
pub(super) struct ProviderRequestCounts {
    pub(super) building_register: u64,
    pub(super) real_transaction: u64,
    pub(super) vworld_cadastral: u64,
    pub(super) vworld_land_register: u64,
}

pub(super) fn provider_request_counts(jobs: &[JsonValue]) -> ProviderRequestCounts {
    let mut counts = ProviderRequestCounts::default();
    for job in jobs {
        let endpoint_slug = string_property(job, "endpoint_slug");
        let endpoint = string_property(job, "endpoint");
        let request_count = job_request_count(job);
        if endpoint_slug.starts_with("data-go-kr-building-register-") {
            counts.building_register += request_count;
        }
        if endpoint_slug.starts_with("data-go-kr-real-transaction-") {
            counts.real_transaction += request_count;
        }
        if endpoint == "ingest-vworld-cadastral" {
            counts.vworld_cadastral += request_count;
        }
        if endpoint == "ingest-vworld-land-register" {
            counts.vworld_land_register += request_count;
        }
    }
    counts
}

/// Resolves the canonical (pinned) provider page size for a national job's `operation`, surfacing a
/// hard error when the operation is not pinned. Page size is sourced from ONE SSOT
/// ([`collection_domain::canonical_page_size`], ADR 0016 D-A) instead of drifting per-source config
/// knobs; every national page-lane operation is pinned, so `None` means a bug.
fn national_canonical_page_size(operation: &str) -> anyhow::Result<u64> {
    canonical_page_size(operation)
        .map(u64::from)
        .with_context(|| format!("no canonical page size for operation {operation}"))
}

fn national_page_count_job<'a>(
    plan: Option<&'a PageCountPlan>,
    job_id: &str,
    scope_unit_id: &str,
    endpoint_slug: &str,
    sigungu: &str,
    bjdong: &str,
    requested_page_size: u64,
) -> anyhow::Result<Option<&'a JsonValue>> {
    let Some(plan) = plan else {
        return Ok(None);
    };
    let Some(job) = plan.jobs_by_id.get(job_id) else {
        bail!("national page count plan missing job: {job_id}");
    };
    if string_property(job, "scope_unit_id") != scope_unit_id {
        bail!("national page count plan scope_unit_id mismatch: {job_id}");
    }
    if string_property(job, "endpoint_slug") != endpoint_slug {
        bail!("national page count plan endpoint_slug mismatch: {job_id}");
    }
    if string_property(job, "sigungu_cd") != sigungu || string_property(job, "bjdong_cd") != bjdong
    {
        bail!("national page count plan legal-dong mismatch: {job_id}");
    }
    if u64_property(job, "requested_page_size").unwrap_or(0) != requested_page_size {
        bail!("national page count plan requested_page_size mismatch: {job_id}");
    }
    Ok(Some(job))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::PathBuf;

    use anyhow::Context;
    use collection_domain::{building_register_dataset_slug, canonical_page_size, source_slug};

    use super::super::config::{ProviderSet, WriterConfig};
    use super::super::source_inputs::EndpointCatalog;
    use super::super::support::{string_property, u64_property};
    use super::super::ScopeRow;
    use super::{
        new_building_register_jobs, new_real_transaction_job, new_vworld_cadastral_job,
        new_vworld_land_register_job,
    };

    /// The 10 owner-approved building-register operations (ADR 0014 §1.1), the SSOT for which
    /// `getBr*` methods the national data.go.kr collection runs.
    const BUILDING_REGISTER_OPERATIONS: [&str; 10] = [
        "getBrTitleInfo",
        "getBrRecapTitleInfo",
        "getBrExposInfo",
        "getBrExposPubuseAreaInfo",
        "getBrFlrOulnInfo",
        "getBrHsprcInfo",
        "getBrJijiguInfo",
        "getBrWclfInfo",
        "getBrAtchJibunInfo",
        "getBrBasisOulnInfo",
    ];

    fn test_config() -> WriterConfig {
        // A single unwindowed job per operation (building_page_window_size == 0), no page-count
        // plan, so `new_building_register_jobs` returns exactly one fixed-parameter job.
        WriterConfig {
            root: PathBuf::from("."),
            approval_path: PathBuf::new(),
            pilot_evidence_path: PathBuf::new(),
            scope_jsonl_path: PathBuf::new(),
            scope_evidence_path: PathBuf::new(),
            endpoint_catalog_path: PathBuf::new(),
            page_count_plan_path: None,
            output_path: PathBuf::new(),
            request_cap: 1,
            shard_size: 100,
            building_max_pages: 1,
            building_page_window_size: 0,
            vworld_max_pages: 1,
            land_register_max_pages: 1,
            real_transaction_start_deal_ymd: String::new(),
            real_transaction_end_deal_ymd: String::new(),
            real_transaction_operations: Vec::new(),
            real_transaction_max_pages: 0,
            provider_set: ProviderSet::BuildingRegister,
            confirm_fixed_page_count_fallback: false,
            confirm_national_shard_manifest: true,
        }
    }

    fn test_scope_row() -> ScopeRow {
        ScopeRow {
            scope_unit_id: "scope:legal-dong:11110:10100".to_owned(),
            sigungu_cd: "11110".to_owned(),
            bjdong_cd: "10100".to_owned(),
            bjdong_code: "1111010100".to_owned(),
            source_row_count: 1,
        }
    }

    #[test]
    fn building_register_jobs_emit_canonical_datagokr_source_slug_for_every_operation(
    ) -> anyhow::Result<()> {
        let config = test_config();
        let row = test_scope_row();

        for operation in BUILDING_REGISTER_OPERATIONS {
            let jobs = new_building_register_jobs(&config, &row, operation, None)?;
            let [job] = jobs.as_slice() else {
                anyhow::bail!(
                    "expected exactly one unwindowed building-register job for {operation}, got {}",
                    jobs.len()
                );
            };

            let dataset_slug = building_register_dataset_slug(operation).with_context(|| {
                format!("no canonical dataset_slug for building-register operation {operation}")
            })?;
            let expected = source_slug("data.go.kr", dataset_slug)?;

            let actual = string_property(job, "source_slug");
            assert_eq!(
                actual, expected,
                "{operation} must emit the canonical generator-based source_slug"
            );
            assert!(
                !actual.starts_with("molit-"),
                "{operation} source_slug must not use the old molit- format: {actual}"
            );
            assert!(
                !actual.contains(&row.sigungu_cd) && !actual.contains(&row.bjdong_cd),
                "{operation} source_slug must be scope-free (no sigungu/bjdong): {actual}"
            );

            // The emitted page size must come from the canonical SSOT, not a config knob.
            assert_eq!(
                u64_property(job, "num_of_rows"),
                Some(100),
                "{operation} must emit the canonical building-register page size (100)"
            );
        }
        Ok(())
    }

    /// An `EndpointCatalog` with no metadata, so `new_real_transaction_job` folds to the canonical
    /// generator-derived source_slug instead of a catalog lookup.
    fn empty_endpoint_catalog() -> EndpointCatalog {
        EndpointCatalog {
            schema_version: String::new(),
            endpoint_count: 0,
            sha256: String::new(),
            endpoint_slugs: BTreeSet::new(),
            endpoint_metadata_by_slug: BTreeMap::new(),
            building_hub_bulk_endpoint_count: 0,
            real_transaction_operations: Vec::new(),
        }
    }

    /// Every national job kind must emit the page size pinned by `canonical_page_size`, not a
    /// drifting per-source config knob (ADR 0016 D-A). This guards against a regression that would
    /// reintroduce a config page-size knob that disagrees with the plan-time guard.
    #[test]
    fn each_job_kind_emits_its_canonical_page_size() -> anyhow::Result<()> {
        let mut config = test_config();
        config.real_transaction_max_pages = 1;
        let row = test_scope_row();
        let catalog = empty_endpoint_catalog();

        // Building-register: canonical 100 under the `num_of_rows` key.
        let building = new_building_register_jobs(&config, &row, "getBrTitleInfo", None)?;
        let [building_job] = building.as_slice() else {
            anyhow::bail!("expected one building-register job");
        };
        assert_eq!(
            u64_property(building_job, "num_of_rows"),
            canonical_page_size("getBrTitleInfo").map(u64::from),
        );
        assert_eq!(u64_property(building_job, "num_of_rows"), Some(100));

        // V-World cadastral (`GetFeature`): canonical 1000 under the `size` key.
        let cadastral = new_vworld_cadastral_job(&config, &row, None)?;
        assert_eq!(
            u64_property(&cadastral, "size"),
            canonical_page_size("GetFeature").map(u64::from),
        );
        assert_eq!(u64_property(&cadastral, "size"), Some(1000));

        // V-World land-register (`ladfrlList`): canonical 1000 under the `num_of_rows` key.
        let land_register = new_vworld_land_register_job(&config, &row, None)?;
        assert_eq!(
            u64_property(&land_register, "num_of_rows"),
            canonical_page_size("ladfrlList").map(u64::from),
        );
        assert_eq!(u64_property(&land_register, "num_of_rows"), Some(1000));

        // Real-transaction (`getRTMSDataSvc*`): canonical 1000 under the `num_of_rows` key.
        let real_transaction = new_real_transaction_job(
            &config,
            &row,
            "getRTMSDataSvcAptTradeDev",
            "202401",
            &catalog,
        )?;
        assert_eq!(
            u64_property(&real_transaction, "num_of_rows"),
            canonical_page_size("getRTMSDataSvcAptTradeDev").map(u64::from),
        );
        assert_eq!(u64_property(&real_transaction, "num_of_rows"), Some(1000));

        Ok(())
    }
}
