use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
};

use anyhow::{bail, Context};
use serde::Serialize;
use serde_json::{Map, Value as JsonValue};

use crate::public_data_control_support::{git_head, read_json, repo_relative_path, utc_now};

mod config;
mod support;

use config::CompileConfig;
use support::*;

const PLAN_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_plan.v1";
const LEDGER_ENTRY_SCHEMA_VERSION: &str =
    "foundation-platform.national_data_collection_ledger_entry.v1";
const MANIFEST_SCHEMA_VERSION: &str =
    "foundation-platform.national_data_collection_shard_manifest.v1";
const REQUEST_FINGERPRINT_SCHEMA_VERSION: &str =
    "foundation-platform.bronze_request_fingerprint.v1";
const ENDPOINT_CATALOG_SCHEMA_VERSION: &str =
    "foundation-platform.public_source_endpoint_catalog.v1";

pub fn run() -> anyhow::Result<()> {
    let config = CompileConfig::from_env()?;
    let compiled = compile(&config)?;

    if let Some(parent) = config.ledger_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create ledger directory {}", parent.display()))?;
    }
    let ledger_text = compiled
        .ledger_rows
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .context("failed to serialize execution ledger")?
        .join("\n");
    fs::write(&config.ledger_path, ledger_text)
        .with_context(|| format!("failed to write {}", config.ledger_path.display()))?;

    if let Some(parent) = config.output_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create plan directory {}", parent.display()))?;
    }
    let plan_bytes =
        serde_json::to_vec_pretty(&compiled.plan).context("failed to serialize plan")?;
    fs::write(&config.output_path, plan_bytes)
        .with_context(|| format!("failed to write {}", config.output_path.display()))?;

    println!(
        "national-data-collection-plan-compiled status=ready jobs={} ledger_entries={} path={}",
        compiled.ledger_rows.len(),
        compiled.ledger_rows.len(),
        repo_relative_path(&config.root, &config.output_path)
    );
    Ok(())
}

struct CompiledPlan {
    plan: JsonValue,
    ledger_rows: Vec<LedgerRow>,
}

fn compile(config: &CompileConfig) -> anyhow::Result<CompiledPlan> {
    let manifest = read_json(&config.manifest_path, "national shard manifest")?;
    assert_manifest(&manifest)?;

    let scope_source = object_property(&manifest, "scope_source")?;
    if string_property(scope_source, "format") != "jsonl" {
        bail!("manifest scope_source format must be jsonl");
    }
    let scope_path = env_resolved_manifest_path(
        &config.root,
        &string_property(scope_source, "path"),
        "scope_source.path",
    )?;
    let scope_evidence_path = env_resolved_manifest_path(
        &config.root,
        &string_property(scope_source, "evidence_path"),
        "scope_source.evidence_path",
    )?;
    let endpoint_catalog = object_property(&manifest, "endpoint_catalog")?;
    let endpoint_catalog_path = env_resolved_manifest_path(
        &config.root,
        &string_property(endpoint_catalog, "path"),
        "endpoint_catalog.path",
    )?;

    require_file(&scope_path, "manifest scope JSONL missing")?;
    require_file(&scope_evidence_path, "manifest scope evidence missing")?;
    require_file(&endpoint_catalog_path, "manifest endpoint catalog missing")?;

    let endpoint_catalog_hash = sha256_file_hex(&endpoint_catalog_path)?;
    if endpoint_catalog_hash != string_property(endpoint_catalog, "sha256") {
        bail!("manifest endpoint catalog sha256 must match file");
    }
    let endpoint_catalog_file = read_json(&endpoint_catalog_path, "endpoint catalog")?;
    if string_property(&endpoint_catalog_file, "schema_version") != ENDPOINT_CATALOG_SCHEMA_VERSION
    {
        bail!("endpoint catalog file schema mismatch");
    }
    let endpoint_policy_by_slug = endpoint_policy_by_slug(&endpoint_catalog_file)?;

    let request_plan = object_property(&manifest, "request_plan")?;
    let sharding = object_property(&manifest, "sharding")?;
    let shards = array_property(&manifest, "shards");
    let declared_job_count = u64_property(sharding, "total_job_count").unwrap_or(0);
    let declared_shard_count = u64_property(sharding, "shard_count").unwrap_or(0);
    let estimated_request_count =
        u64_property(request_plan, "estimated_request_count_total").unwrap_or(0);
    let request_cap = u64_property(request_plan, "request_cap").unwrap_or(0);
    if declared_shard_count != u64::try_from(shards.len()).unwrap_or(u64::MAX)
        || declared_job_count < 1
        || estimated_request_count < 1
        || estimated_request_count > request_cap
    {
        bail!("manifest sharding and request plan must be internally consistent");
    }

    let manifest_hash = sha256_file_hex(&config.manifest_path)?;
    let scope_hash = sha256_file_hex(&scope_path)?;
    let scope_evidence_hash = sha256_file_hex(&scope_evidence_path)?;
    let compiler_input_hash = sha256_text(&format!(
        "{manifest_hash}\n{scope_hash}\n{scope_evidence_hash}\n{endpoint_catalog_hash}"
    ));
    let collection_snapshot_id =
        resolve_collection_snapshot_id(scope_source, config.collection_snapshot_id.as_deref())?;

    let mut ledger_rows = Vec::new();
    let mut seen_job_ids = BTreeSet::new();
    let mut seen_idempotency_keys = BTreeSet::new();
    let mut seen_request_fingerprints = BTreeSet::new();
    let mut actual_request_count = 0_u64;

    for shard in shards {
        let shard_id = string_property(&shard, "shard_id");
        let shard_sequence = u64_property(&shard, "sequence").unwrap_or(0);
        for job in array_property(&shard, "jobs") {
            assert_job(&job, &shard_id)?;
            let job_id = string_property(&job, "id");
            let endpoint_slug = string_property(&job, "endpoint_slug");
            if let Some(policy) = endpoint_policy_by_slug.get(&endpoint_slug) {
                if !policy.national_collection_allowed
                    || policy.source_acquisition_lane == "disabled_api_duplicate"
                {
                    bail!(
                        "job endpoint disabled for national collection: {} lane={}",
                        endpoint_slug,
                        policy.source_acquisition_lane
                    );
                }
            }
            let idempotency_key = string_property(&job, "idempotency_key");
            if !seen_job_ids.insert(job_id.clone()) {
                bail!("duplicate manifest job id: {job_id}");
            }
            if !seen_idempotency_keys.insert(idempotency_key.clone()) {
                bail!("duplicate manifest idempotency_key: {idempotency_key}");
            }
            let request_count = u64_property(&job, "request_count_estimate").unwrap_or(0);
            actual_request_count = actual_request_count.saturating_add(request_count);
            let request_fingerprint = request_fingerprint_sha256(&job, &collection_snapshot_id)?;
            if !seen_request_fingerprints.insert(request_fingerprint.clone()) {
                bail!("duplicate manifest request fingerprint: {request_fingerprint}");
            }
            ledger_rows.push(ledger_row(
                config,
                &job,
                &compiler_input_hash,
                &request_fingerprint,
                &collection_snapshot_id,
                &shard_id,
                shard_sequence,
                request_count,
            ));
        }
    }

    if ledger_rows.len() != usize::try_from(declared_job_count).unwrap_or(usize::MAX)
        || actual_request_count != estimated_request_count
    {
        bail!("compiled ledger must match manifest job and request counts");
    }

    Ok(CompiledPlan {
        plan: plan_json(
            config,
            &compiler_input_hash,
            &collection_snapshot_id,
            &manifest_hash,
            &scope_hash,
            &scope_evidence_hash,
            &endpoint_catalog_hash,
            scope_source,
            endpoint_catalog,
            sharding,
            declared_shard_count,
            declared_job_count,
            estimated_request_count,
            request_cap,
            ledger_rows.len(),
        ),
        ledger_rows,
    })
}

fn plan_json(
    config: &CompileConfig,
    compiler_input_hash: &str,
    collection_snapshot_id: &str,
    manifest_hash: &str,
    scope_hash: &str,
    scope_evidence_hash: &str,
    endpoint_catalog_hash: &str,
    scope_source: &JsonValue,
    endpoint_catalog: &JsonValue,
    sharding: &JsonValue,
    declared_shard_count: u64,
    declared_job_count: u64,
    estimated_request_count: u64,
    request_cap: u64,
    ledger_entry_count: usize,
) -> JsonValue {
    object([
        ("schema_version", str_value(PLAN_SCHEMA_VERSION)),
        ("generated_at_utc", str_value(utc_now())),
        ("git_head", str_value(git_head(&config.root))),
        ("status", str_value("ready")),
        ("run_mode", str_value("national")),
        ("completion_claim_allowed", JsonValue::Bool(false)),
        ("production_cutover_allowed", JsonValue::Bool(false)),
        ("national_rollout_allowed", JsonValue::Bool(false)),
        (
            "national_rollout_blocked_reason",
            str_value("plan_only_no_public_api_execution"),
        ),
        ("compiler_input_hash_sha256", str_value(compiler_input_hash)),
        (
            "bronze_reuse_policy",
            object([
                (
                    "request_fingerprint_schema_version",
                    str_value(REQUEST_FINGERPRINT_SCHEMA_VERSION),
                ),
                ("collection_snapshot_id", str_value(collection_snapshot_id)),
                (
                    "duplicate_request_policy",
                    str_value("reuse_existing_validated_bronze_objects"),
                ),
                (
                    "refresh_policy",
                    str_value("change_collection_snapshot_id_or_force_refresh"),
                ),
            ]),
        ),
        (
            "content_hashes",
            object([
                ("manifest_sha256", str_value(manifest_hash)),
                ("scope_jsonl_sha256", str_value(scope_hash)),
                ("scope_evidence_sha256", str_value(scope_evidence_hash)),
                ("endpoint_catalog_sha256", str_value(endpoint_catalog_hash)),
            ]),
        ),
        (
            "manifest",
            object([
                (
                    "path",
                    str_value(repo_relative_path(&config.root, &config.manifest_path)),
                ),
                ("shard_count", u64_value(declared_shard_count)),
                ("job_count", u64_value(declared_job_count)),
                ("request_count", u64_value(estimated_request_count)),
                ("request_cap", u64_value(request_cap)),
                (
                    "retry_policy",
                    str_value(string_property(sharding, "retry_policy")),
                ),
            ]),
        ),
        (
            "scope_source",
            object([
                ("path", str_value(string_property(scope_source, "path"))),
                (
                    "evidence_path",
                    str_value(string_property(scope_source, "evidence_path")),
                ),
                (
                    "row_count",
                    u64_value(u64_property(scope_source, "row_count").unwrap_or(0)),
                ),
                (
                    "source_rows",
                    u64_value(u64_property(scope_source, "source_rows").unwrap_or(0)),
                ),
                (
                    "registry_path",
                    str_value(string_property(scope_source, "registry_path")),
                ),
                (
                    "registry_sha256",
                    str_value(string_property(scope_source, "registry_sha256")),
                ),
                (
                    "registry_rows",
                    u64_value(u64_property(scope_source, "registry_rows").unwrap_or(0)),
                ),
            ]),
        ),
        (
            "endpoint_catalog",
            object([
                ("path", str_value(string_property(endpoint_catalog, "path"))),
                ("sha256", str_value(endpoint_catalog_hash)),
                (
                    "schema_version",
                    str_value(string_property(endpoint_catalog, "schema_version")),
                ),
                (
                    "endpoint_count",
                    u64_value(u64_property(endpoint_catalog, "endpoint_count").unwrap_or(0)),
                ),
            ]),
        ),
        (
            "execution_ledger",
            object([
                (
                    "path",
                    str_value(repo_relative_path(&config.root, &config.ledger_path)),
                ),
                (
                    "entry_schema_version",
                    str_value(LEDGER_ENTRY_SCHEMA_VERSION),
                ),
                (
                    "entry_count",
                    u64_value(u64::try_from(ledger_entry_count).unwrap_or(u64::MAX)),
                ),
                (
                    "planned_count",
                    u64_value(u64::try_from(ledger_entry_count).unwrap_or(u64::MAX)),
                ),
                ("append_only", JsonValue::Bool(true)),
                (
                    "allowed_statuses",
                    JsonValue::Array(
                        ["planned", "running", "succeeded", "failed", "retryable"]
                            .into_iter()
                            .map(str_value)
                            .collect(),
                    ),
                ),
            ]),
        ),
        (
            "evidence_limitations",
            JsonValue::Array(
                [
                    "plan_only",
                    "does_not_execute_public_api_requests",
                    "does_not_promote_silver_gold_national_tables",
                    "does_not_approve_production_cutover",
                ]
                .into_iter()
                .map(str_value)
                .collect(),
            ),
        ),
        (
            "next_gates",
            JsonValue::Array(
                [
                    "national-data-collection-ledger-executor",
                    "silver-gold-national-promotion",
                ]
                .into_iter()
                .map(str_value)
                .collect(),
            ),
        ),
    ])
}

#[allow(clippy::too_many_arguments)]
fn ledger_row(
    config: &CompileConfig,
    job: &JsonValue,
    compiler_input_hash: &str,
    request_fingerprint: &str,
    collection_snapshot_id: &str,
    shard_id: &str,
    shard_sequence: u64,
    request_count: u64,
) -> LedgerRow {
    LedgerRow {
        schema_version: LEDGER_ENTRY_SCHEMA_VERSION,
        compiler_input_hash_sha256: compiler_input_hash.to_owned(),
        request_fingerprint_schema_version: REQUEST_FINGERPRINT_SCHEMA_VERSION,
        request_fingerprint_sha256: request_fingerprint.to_owned(),
        collection_snapshot_id: collection_snapshot_id.to_owned(),
        manifest_path: repo_relative_path(&config.root, &config.manifest_path),
        shard_id: shard_id.to_owned(),
        shard_sequence,
        job_id: string_property(job, "id"),
        idempotency_key: string_property(job, "idempotency_key"),
        scope_unit_id: string_property(job, "scope_unit_id"),
        provider: string_property(job, "provider"),
        endpoint_slug: string_property(job, "endpoint_slug"),
        endpoint: string_property(job, "endpoint"),
        operation: string_property(job, "operation"),
        dataset: string_property(job, "dataset"),
        sigungu_cd: string_property(job, "sigungu_cd"),
        bjdong_cd: string_property(job, "bjdong_cd"),
        lawd_cd: string_property(job, "lawd_cd"),
        deal_ymd: string_property(job, "deal_ymd"),
        bjdong_code: string_property(job, "bjdong_code"),
        provider_emd_cd: string_property(job, "provider_emd_cd"),
        filter_kind: string_property(job, "filter_kind"),
        attr_filter: string_property(job, "attr_filter"),
        pnu_prefix: string_property(job, "pnu_prefix"),
        provider_empty_reason: string_property(job, "provider_empty_reason"),
        page_start: u64_property(job, "page_start").unwrap_or(1),
        page_end: page_end(job),
        page_count_total: page_count_total(job),
        max_pages: u64_property(job, "max_pages").unwrap_or(1),
        num_of_rows: u64_property(job, "num_of_rows").unwrap_or(0),
        size: u64_property(job, "size").unwrap_or(0),
        source_slug: string_property(job, "source_slug"),
        request_count_estimate: request_count,
        status: "planned",
        attempt_count: 0,
        last_error: None,
        bronze_object_path: None,
        started_at_utc: None,
        finished_at_utc: None,
    }
}

#[derive(Clone, Debug, Serialize)]
struct LedgerRow {
    schema_version: &'static str,
    compiler_input_hash_sha256: String,
    request_fingerprint_schema_version: &'static str,
    request_fingerprint_sha256: String,
    collection_snapshot_id: String,
    manifest_path: String,
    shard_id: String,
    shard_sequence: u64,
    job_id: String,
    idempotency_key: String,
    scope_unit_id: String,
    provider: String,
    endpoint_slug: String,
    endpoint: String,
    operation: String,
    dataset: String,
    sigungu_cd: String,
    bjdong_cd: String,
    lawd_cd: String,
    deal_ymd: String,
    bjdong_code: String,
    provider_emd_cd: String,
    filter_kind: String,
    attr_filter: String,
    pnu_prefix: String,
    provider_empty_reason: String,
    page_start: u64,
    page_end: u64,
    page_count_total: u64,
    max_pages: u64,
    num_of_rows: u64,
    size: u64,
    source_slug: String,
    request_count_estimate: u64,
    status: &'static str,
    attempt_count: u64,
    last_error: Option<String>,
    bronze_object_path: Option<String>,
    started_at_utc: Option<String>,
    finished_at_utc: Option<String>,
}

fn assert_manifest(manifest: &JsonValue) -> anyhow::Result<()> {
    if string_property(manifest, "schema_version") != MANIFEST_SCHEMA_VERSION {
        bail!("national shard manifest schema mismatch");
    }
    if string_property(manifest, "status") != "ready" {
        bail!("national shard manifest status must be ready");
    }
    if string_property(manifest, "run_mode") != "national" {
        bail!("national shard manifest run_mode must be national");
    }
    if bool_property(manifest, "completion_claim_allowed").unwrap_or(true) {
        bail!("manifest must not allow completion claims");
    }
    if bool_property(manifest, "production_cutover_allowed").unwrap_or(true) {
        bail!("manifest must not allow production cutover");
    }
    if bool_property(manifest, "national_rollout_allowed").unwrap_or(true) {
        bail!("manifest must not claim national rollout completion");
    }
    let endpoint_catalog = object_property(manifest, "endpoint_catalog")?;
    if string_property(endpoint_catalog, "schema_version") != ENDPOINT_CATALOG_SCHEMA_VERSION {
        bail!("manifest endpoint_catalog schema mismatch");
    }
    if string_property(endpoint_catalog, "path").is_empty() {
        bail!("manifest endpoint_catalog path is required");
    }
    if !is_sha256(&string_property(endpoint_catalog, "sha256")) {
        bail!("manifest endpoint_catalog sha256 must be sha256");
    }
    Ok(())
}

fn assert_job(job: &JsonValue, shard_id: &str) -> anyhow::Result<()> {
    let job_id = string_property(job, "id");
    if !valid_job_id(&job_id) {
        bail!("job id must include provider and canonical scope: {job_id}");
    }
    if string_property(job, "status") != "planned" {
        bail!("job status must be planned: {job_id}");
    }
    if string_property(job, "idempotency_key").is_empty() {
        bail!("job idempotency_key is required: {job_id}");
    }
    if string_property(job, "scope_unit_id").is_empty() {
        bail!("job scope_unit_id is required: {job_id}");
    }
    if u64_property(job, "request_count_estimate").unwrap_or(0) < 1 {
        bail!("job request_count_estimate must be positive: {job_id}");
    }
    if shard_id.is_empty() {
        bail!("job shard_id is required: {job_id}");
    }
    let endpoint_slug = string_property(job, "endpoint_slug");
    if endpoint_slug.is_empty() {
        bail!("job endpoint_slug is required: {job_id}");
    }
    if endpoint_slug.starts_with("data-go-kr-real-transaction-") {
        assert_real_transaction_job(job, &job_id, &endpoint_slug)?;
    } else {
        let sigungu = string_property(job, "sigungu_cd");
        let bjdong = string_property(job, "bjdong_cd");
        if !is_digits(&sigungu, 5) || !is_digits(&bjdong, 5) {
            bail!("job scope codes must be five digits: {job_id}");
        }
        if string_property(job, "scope_unit_id") != format!("scope:legal-dong:{sigungu}{bjdong}") {
            bail!("job scope_unit_id must match legal-dong code: {job_id}");
        }
        let provider = string_property(job, "provider");
        if provider == "data.go.kr" {
            assert_building_register_job(job, &job_id, &endpoint_slug, &sigungu, &bjdong)?;
        }
        if provider == "VWorld" {
            assert_vworld_job(job, &job_id, &endpoint_slug, &sigungu, &bjdong)?;
        }
    }
    Ok(())
}

fn assert_building_register_job(
    job: &JsonValue,
    job_id: &str,
    endpoint_slug: &str,
    sigungu: &str,
    bjdong: &str,
) -> anyhow::Result<()> {
    let operation = string_property(job, "operation");
    if !is_building_operation(&operation) {
        bail!("building-register operation is not supported: {operation}");
    }
    let mut expected_job_id = if operation == "getBrTitleInfo" {
        format!("building-register-{sigungu}-{bjdong}")
    } else {
        format!("building-register-{operation}-{sigungu}-{bjdong}")
    };
    if has_any_page_window_field(job) {
        expected_job_id.push_str(&page_window_suffix(job, "data.go.kr")?);
    }
    if *endpoint_slug != format!("data-go-kr-building-register-{operation}") {
        bail!("data.go.kr job endpoint_slug mismatch: {job_id}");
    }
    if job_id != expected_job_id {
        bail!("data.go.kr job id must match operation and legal-dong scope: {job_id}");
    }
    if string_property(job, "endpoint") != operation {
        bail!("data.go.kr job endpoint must match operation: {job_id}");
    }
    if u64_property(job, "num_of_rows").unwrap_or(0) < 1 {
        bail!("data.go.kr job num_of_rows must be positive: {job_id}");
    }
    Ok(())
}

fn assert_real_transaction_job(
    job: &JsonValue,
    job_id: &str,
    endpoint_slug: &str,
) -> anyhow::Result<()> {
    let operation = string_property(job, "operation");
    if !is_real_transaction_operation(&operation) {
        bail!("real-transaction operation is not supported: {operation}");
    }
    let lawd_cd = string_property(job, "lawd_cd");
    let deal_ymd = string_property(job, "deal_ymd");
    let sigungu = string_property(job, "sigungu_cd");
    let bjdong = string_property(job, "bjdong_cd");
    if !is_digits(&lawd_cd, 5) || sigungu != lawd_cd {
        bail!("real-transaction job sigungu_cd must equal five-digit lawd_cd: {job_id}");
    }
    if !bjdong.is_empty() {
        bail!("real-transaction job bjdong_cd must be empty: {job_id}");
    }
    if !is_digits(&deal_ymd, 6) {
        bail!("real-transaction job deal_ymd must use YYYYMM: {job_id}");
    }
    if string_property(job, "scope_unit_id") != format!("scope:sigungu-month:{lawd_cd}:{deal_ymd}")
    {
        bail!("real-transaction job scope_unit_id must match sigungu-month: {job_id}");
    }
    let expected_job_id = format!(
        "real-transaction-{operation}-{lawd_cd}-{deal_ymd}{}",
        page_window_suffix(job, "real-transaction")?
    );
    if *endpoint_slug != format!("data-go-kr-real-transaction-{operation}") {
        bail!("real-transaction job endpoint_slug mismatch: {job_id}");
    }
    if job_id != expected_job_id {
        bail!("real-transaction job id must match operation and sigungu-month scope: {job_id}");
    }
    if string_property(job, "endpoint") != operation {
        bail!("real-transaction job endpoint must match operation: {job_id}");
    }
    if u64_property(job, "num_of_rows").unwrap_or(0) < 1 {
        bail!("data.go.kr job num_of_rows must be positive: {job_id}");
    }
    Ok(())
}

fn assert_vworld_job(
    job: &JsonValue,
    job_id: &str,
    endpoint_slug: &str,
    sigungu: &str,
    bjdong: &str,
) -> anyhow::Result<()> {
    if job.get("geom_filter").is_some() {
        bail!("VWorld job must not include geom_filter: {job_id}");
    }
    let endpoint = string_property(job, "endpoint");
    let expected_bjdong_code = format!("{sigungu}{bjdong}");
    let expected_provider_emd_cd = if is_digits(&expected_bjdong_code, 10) {
        expected_bjdong_code[..8].to_owned()
    } else {
        String::new()
    };
    if endpoint == "ingest-vworld-cadastral" {
        if endpoint_slug != "vworld-dataset-parcel" {
            bail!("VWorld cadastral job endpoint_slug mismatch: {job_id}");
        }
        if string_property(job, "filter_kind") != "attr_filter" {
            bail!("VWorld cadastral job filter_kind must be attr_filter: {job_id}");
        }
        if string_property(job, "provider_emd_cd") != expected_provider_emd_cd {
            bail!(
                "VWorld cadastral job provider_emd_cd must equal first eight digits of bjdong_code: {job_id}"
            );
        }
        if string_property(job, "attr_filter") != format!("emdCd:=:{expected_provider_emd_cd}") {
            bail!("VWorld cadastral job attr_filter must equal emdCd:=:provider_emd_cd: {job_id}");
        }
    } else if endpoint == "ingest-vworld-land-register" {
        if endpoint_slug != "vworld-dataset-land_register" {
            bail!("VWorld land-register job endpoint_slug mismatch: {job_id}");
        }
        if string_property(job, "operation") != "ladfrlList" {
            bail!("VWorld land-register job operation must be ladfrlList: {job_id}");
        }
        if string_property(job, "pnu_prefix") != expected_bjdong_code {
            bail!("VWorld land-register job pnu_prefix must equal legal-dong code: {job_id}");
        }
        if u64_property(job, "num_of_rows").unwrap_or(0) < 1 {
            bail!("VWorld land-register job num_of_rows must be positive: {job_id}");
        }
    } else {
        bail!("VWorld job endpoint is not supported: {job_id}");
    }
    Ok(())
}

fn request_fingerprint_sha256(
    job: &JsonValue,
    collection_snapshot_id: &str,
) -> anyhow::Result<String> {
    let provider = string_property(job, "provider");
    let endpoint = string_property(job, "endpoint");
    let endpoint_slug = string_property(job, "endpoint_slug");
    let mut request = Map::new();
    request.insert(
        "schema_version".to_owned(),
        str_value(REQUEST_FINGERPRINT_SCHEMA_VERSION),
    );
    request.insert(
        "collection_snapshot_id".to_owned(),
        str_value(collection_snapshot_id),
    );
    request.insert("provider".to_owned(), str_value(&provider));
    request.insert("endpoint_slug".to_owned(), str_value(&endpoint_slug));
    request.insert("endpoint".to_owned(), str_value(&endpoint));
    request.insert(
        "scope_unit_id".to_owned(),
        str_value(string_property(job, "scope_unit_id")),
    );
    request.insert(
        "sigungu_cd".to_owned(),
        str_value(string_property(job, "sigungu_cd")),
    );
    request.insert(
        "bjdong_cd".to_owned(),
        str_value(string_property(job, "bjdong_cd")),
    );
    request.insert(
        "lawd_cd".to_owned(),
        str_value(string_property(job, "lawd_cd")),
    );
    request.insert(
        "deal_ymd".to_owned(),
        str_value(string_property(job, "deal_ymd")),
    );
    request.insert(
        "page_start".to_owned(),
        u64_value(u64_property(job, "page_start").unwrap_or(1)),
    );
    request.insert("page_end".to_owned(), u64_value(page_end(job)));
    request.insert(
        "page_count_total".to_owned(),
        u64_value(page_count_total(job)),
    );
    request.insert(
        "max_pages".to_owned(),
        u64_value(u64_property(job, "max_pages").unwrap_or(1)),
    );
    request.insert("response_format".to_owned(), str_value("json"));
    request.insert(
        "provider_request".to_owned(),
        provider_request(job, &provider, &endpoint, &endpoint_slug),
    );
    let canonical_json = serde_json::to_string(&JsonValue::Object(request))
        .context("failed to serialize request fingerprint input")?;
    Ok(sha256_text(&canonical_json))
}

fn provider_request(
    job: &JsonValue,
    provider: &str,
    endpoint: &str,
    endpoint_slug: &str,
) -> JsonValue {
    if provider == "VWorld" && endpoint == "ingest-vworld-cadastral" {
        return object([
            ("request", str_value("GetFeature")),
            ("service", str_value("data")),
            ("dataset", str_value(string_property(job, "dataset"))),
            (
                "bjdong_code",
                str_value(string_property(job, "bjdong_code")),
            ),
            (
                "provider_emd_cd",
                str_value(string_property(job, "provider_emd_cd")),
            ),
            (
                "filter_kind",
                str_value(string_property(job, "filter_kind")),
            ),
            (
                "attr_filter",
                str_value(string_property(job, "attr_filter")),
            ),
            ("geometry", JsonValue::Bool(true)),
            ("attribute", JsonValue::Bool(true)),
            ("crs", str_value("EPSG:4326")),
            ("page_param_name", str_value("page")),
            ("size_param_name", str_value("size")),
            (
                "page_size",
                u64_value(u64_property(job, "size").unwrap_or(0)),
            ),
        ]);
    }
    if provider == "VWorld" && endpoint == "ingest-vworld-land-register" {
        return object([
            ("operation", str_value(string_property(job, "operation"))),
            ("pnu_prefix", str_value(string_property(job, "pnu_prefix"))),
            ("type", str_value("json")),
            ("page_param_name", str_value("pageNo")),
            ("size_param_name", str_value("numOfRows")),
            (
                "page_size",
                u64_value(u64_property(job, "num_of_rows").unwrap_or(0)),
            ),
        ]);
    }
    if provider == "data.go.kr" && endpoint_slug.starts_with("data-go-kr-real-transaction-") {
        return object([
            ("operation", str_value(string_property(job, "operation"))),
            ("lawd_cd", str_value(string_property(job, "lawd_cd"))),
            ("deal_ymd", str_value(string_property(job, "deal_ymd"))),
            ("type", str_value("json")),
            ("page_param_name", str_value("pageNo")),
            ("size_param_name", str_value("numOfRows")),
            (
                "page_size",
                u64_value(u64_property(job, "num_of_rows").unwrap_or(0)),
            ),
        ]);
    }
    object([
        ("operation", str_value(string_property(job, "operation"))),
        ("sigungu_cd", str_value(string_property(job, "sigungu_cd"))),
        ("bjdong_cd", str_value(string_property(job, "bjdong_cd"))),
        ("type", str_value("json")),
        ("page_param_name", str_value("pageNo")),
        ("size_param_name", str_value("numOfRows")),
        (
            "page_size",
            u64_value(u64_property(job, "num_of_rows").unwrap_or(0)),
        ),
    ])
}

#[derive(Clone)]
struct EndpointPolicy {
    source_acquisition_lane: String,
    national_collection_allowed: bool,
}

fn endpoint_policy_by_slug(
    endpoint_catalog_file: &JsonValue,
) -> anyhow::Result<BTreeMap<String, EndpointPolicy>> {
    let mut policies = BTreeMap::new();
    for endpoint in array_property(endpoint_catalog_file, "endpoints") {
        let endpoint_slug = string_property(&endpoint, "endpoint_slug");
        if endpoint_slug.is_empty() {
            continue;
        }
        let source_acquisition_lane = string_property(&endpoint, "source_acquisition_lane");
        if source_acquisition_lane.is_empty() {
            bail!("endpoint catalog endpoint missing source_acquisition_lane: {endpoint_slug}");
        }
        policies.insert(
            endpoint_slug,
            EndpointPolicy {
                source_acquisition_lane,
                national_collection_allowed: bool_property(
                    &endpoint,
                    "national_collection_allowed",
                )
                .unwrap_or(false),
            },
        );
    }
    Ok(policies)
}

fn resolve_collection_snapshot_id(
    scope_source: &JsonValue,
    explicit: Option<&str>,
) -> anyhow::Result<String> {
    let snapshot_id = explicit.map(str::to_owned).unwrap_or_else(|| {
        format!(
            "registry:{}",
            string_property(scope_source, "registry_sha256")
        )
    });
    if snapshot_id.is_empty() || snapshot_id.chars().any(char::is_whitespace) {
        bail!("CollectionSnapshotId must be non-empty and must not contain whitespace");
    }
    Ok(snapshot_id)
}

fn page_window_suffix(job: &JsonValue, label: &str) -> anyhow::Result<String> {
    if !has_all_page_window_fields(job) {
        bail!(
            "{label} page-window fields must be provided together: {}",
            string_property(job, "id")
        );
    }
    let page_start = u64_property(job, "page_start").unwrap_or(0);
    let page_end = u64_property(job, "page_end").unwrap_or(0);
    let page_count_total = u64_property(job, "page_count_total").unwrap_or(0);
    let request_count = u64_property(job, "request_count_estimate").unwrap_or(0);
    let max_pages = u64_property(job, "max_pages").unwrap_or(0);
    if page_start < 1 || page_end < page_start || page_count_total < page_end {
        bail!(
            "{label} page-window range is invalid: {}",
            string_property(job, "id")
        );
    }
    let window_len = page_end - page_start + 1;
    if max_pages != window_len || request_count != window_len {
        bail!(
            "{label} page-window request count must equal range length: {}",
            string_property(job, "id")
        );
    }
    Ok(format!("-p{page_start:06}-{page_end:06}"))
}

fn has_any_page_window_field(job: &JsonValue) -> bool {
    job.get("page_start").is_some()
        || job.get("page_end").is_some()
        || job.get("page_count_total").is_some()
}

fn has_all_page_window_fields(job: &JsonValue) -> bool {
    job.get("page_start").is_some()
        && job.get("page_end").is_some()
        && job.get("page_count_total").is_some()
}

fn page_end(job: &JsonValue) -> u64 {
    u64_property(job, "page_end").unwrap_or_else(|| u64_property(job, "max_pages").unwrap_or(1))
}

fn page_count_total(job: &JsonValue) -> u64 {
    u64_property(job, "page_count_total")
        .unwrap_or_else(|| u64_property(job, "max_pages").unwrap_or(1))
}
