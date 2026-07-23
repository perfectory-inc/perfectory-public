use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::public_data_control_support::{
    env_path, git_head, read_json, repo_relative_path, resolve_repo_path, utc_now, write_json_file,
};

const REPORT_SCHEMA_VERSION: &str = "foundation-platform.national_bronze_object_manifest.v1";
const ENTRY_SCHEMA_VERSION: &str = "foundation-platform.national_bronze_object_manifest_entry.v1";
const COVERAGE_SCHEMA_VERSION: &str =
    "foundation-platform.national_data_collection_coverage_ledger.v1";
const EXECUTION_EVIDENCE_SCHEMA_VERSION: &str =
    "foundation-platform.national_data_collection_ledger_execution.v1";
const EVENT_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_ledger_event.v1";
pub(crate) const SNAPSHOT_NATIONAL_COVERAGE_LEDGER: &str =
    "national_data_collection_coverage_ledger";
pub(crate) type ReportSnapshot = HashMap<&'static str, JsonValue>;

pub fn run_check() -> anyhow::Result<()> {
    let config = CheckConfig::from_env()?;
    let snapshot = load_report_snapshot(&config)?;
    Checker::new(config).run(&snapshot)
}

pub fn run_write() -> anyhow::Result<()> {
    let config = WriteConfig::from_env()?;
    Writer::new(config)?.run()
}

#[derive(Clone)]
struct CheckConfig {
    root: PathBuf,
    coverage_ledger_path: PathBuf,
    manifest_path: PathBuf,
    report_path: PathBuf,
}

impl CheckConfig {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );
        Ok(Self {
            coverage_ledger_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_BRONZE_OBJECT_MANIFEST_CHECK_COVERAGE_LEDGER_PATH",
                    "target/audit/national-data-collection-coverage-ledger.json",
                )?,
                "CoverageLedgerPath",
            )?,
            manifest_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_BRONZE_OBJECT_MANIFEST_CHECK_MANIFEST_PATH",
                    "target/audit/national-bronze-object-manifest.jsonl",
                )?,
                "ManifestPath",
            )?,
            report_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_BRONZE_OBJECT_MANIFEST_CHECK_REPORT_PATH",
                    "target/audit/national-bronze-object-manifest.json",
                )?,
                "ReportPath",
            )?,
            root,
        })
    }
}

struct WriteConfig {
    check: CheckConfig,
    overwrite: bool,
}

impl WriteConfig {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );
        Ok(Self {
            check: CheckConfig {
                coverage_ledger_path: resolve_repo_path(
                    &root,
                    &env_path(
                        "FOUNDATION_PLATFORM_NATIONAL_BRONZE_OBJECT_MANIFEST_COVERAGE_LEDGER_PATH",
                        "target/audit/national-data-collection-coverage-ledger.json",
                    )?,
                    "CoverageLedgerPath",
                )?,
                manifest_path: resolve_repo_path(
                    &root,
                    &env_path(
                        "FOUNDATION_PLATFORM_NATIONAL_BRONZE_OBJECT_MANIFEST_MANIFEST_PATH",
                        "target/audit/national-bronze-object-manifest.jsonl",
                    )?,
                    "ManifestPath",
                )?,
                report_path: resolve_repo_path(
                    &root,
                    &env_path(
                        "FOUNDATION_PLATFORM_NATIONAL_BRONZE_OBJECT_MANIFEST_REPORT_PATH",
                        "target/audit/national-bronze-object-manifest.json",
                    )?,
                    "ReportPath",
                )?,
                root,
            },
            overwrite: env_bool(
                "FOUNDATION_PLATFORM_NATIONAL_BRONZE_OBJECT_MANIFEST_OVERWRITE",
                false,
            )?,
        })
    }
}

struct Writer {
    config: WriteConfig,
}

impl Writer {
    fn new(config: WriteConfig) -> anyhow::Result<Self> {
        if !config.check.coverage_ledger_path.is_file() {
            bail!(
                "coverage ledger missing: {}",
                config.check.coverage_ledger_path.display()
            );
        }
        if config.check.manifest_path.is_file() && !config.overwrite {
            bail!("manifest already exists; pass -Overwrite to replace it");
        }
        Ok(Self { config })
    }

    fn run(&self) -> anyhow::Result<()> {
        let snapshot = load_report_snapshot(&self.config.check)?;
        let coverage = coverage_from_snapshot(&snapshot, &self.config.check.coverage_ledger_path)?;
        assert_ready_coverage(coverage)?;
        let coverage_summary = coverage.get("coverage").unwrap_or(&JsonValue::Null);
        let expected_object_count = json_u64(coverage_summary, "request_count_total", 0);
        let expected_job_count = json_u64(coverage_summary, "succeeded_job_count", 0);
        let compiler_hash = json_string(
            coverage.get("plan").unwrap_or(&JsonValue::Null),
            "compiler_input_hash_sha256",
        );
        let evidence_paths = json_string_array(
            coverage.get("evidence").unwrap_or(&JsonValue::Null),
            "paths",
            Vec::new(),
        );
        if evidence_paths.is_empty() {
            bail!("coverage ledger evidence paths are required");
        }

        let mut rows = Vec::new();
        let mut seen_jobs = HashSet::new();
        for evidence_path in evidence_paths {
            let resolved_evidence_path = resolve_repo_path(
                &self.config.check.root,
                &PathBuf::from(&evidence_path),
                "coverage.evidence.paths",
            )?;
            if !resolved_evidence_path.is_file() {
                bail!("execution evidence missing: {evidence_path}");
            }
            let evidence = read_json(&resolved_evidence_path, "execution evidence")?;
            if json_string(&evidence, "schema_version") != EXECUTION_EVIDENCE_SCHEMA_VERSION {
                bail!("execution evidence schema mismatch: {evidence_path}");
            }
            if json_string(&evidence, "status") != "ready" {
                bail!("execution evidence status must be ready: {evidence_path}");
            }
            let event_log_path = json_string(
                evidence.get("event_log").unwrap_or(&JsonValue::Null),
                "path",
            );
            let resolved_event_log_path = resolve_repo_path(
                &self.config.check.root,
                &PathBuf::from(&event_log_path),
                "event_log.path",
            )?;
            let events = read_jsonl(&resolved_event_log_path, None)?;
            for event in events {
                if json_string(&event, "status") != "succeeded" {
                    continue;
                }
                if json_string(&event, "schema_version") != EVENT_SCHEMA_VERSION {
                    bail!("event schema mismatch");
                }
                let job_id = json_string(&event, "job_id");
                if !seen_jobs.insert(job_id.clone()) {
                    bail!("duplicate succeeded event: {job_id}");
                }
                let request_count = json_u64(&event, "request_count", 0);
                let last_object_key = json_string(&event, "bronze_object_key");
                let object_keys =
                    expand_bronze_object_keys(&job_id, &last_object_key, request_count)?;
                for (index, object_key) in object_keys.into_iter().enumerate() {
                    rows.push(ManifestEntry {
                        schema_version: ENTRY_SCHEMA_VERSION,
                        compiler_input_hash_sha256: compiler_hash.clone(),
                        request_fingerprint_schema_version: json_string(
                            &event,
                            "request_fingerprint_schema_version",
                        ),
                        request_fingerprint_sha256: json_string(
                            &event,
                            "request_fingerprint_sha256",
                        ),
                        collection_snapshot_id: json_string(&event, "collection_snapshot_id"),
                        coverage_ledger_path: repo_relative_path(
                            &self.config.check.root,
                            &self.config.check.coverage_ledger_path,
                        ),
                        execution_evidence_path: repo_relative_path(
                            &self.config.check.root,
                            &resolved_evidence_path,
                        ),
                        event_log_path: repo_relative_path(
                            &self.config.check.root,
                            &resolved_event_log_path,
                        ),
                        job_id: job_id.clone(),
                        scope_unit_id: json_string(&event, "scope_unit_id"),
                        provider: json_string(&event, "provider"),
                        endpoint: json_string(&event, "endpoint"),
                        storage_driver: json_string(&event, "storage_driver"),
                        object_key,
                        page_number: u64::try_from(index + 1).context("page_number overflow")?,
                        page_count: request_count,
                        job_source_record_count: json_u64(&event, "source_record_count", 0),
                        job_last_bronze_object_key: last_object_key.clone(),
                    });
                }
            }
        }

        if u64::try_from(rows.len()).unwrap_or(u64::MAX) != expected_object_count {
            bail!("expanded Bronze object count must match coverage request_count_total");
        }
        if u64::try_from(seen_jobs.len()).unwrap_or(u64::MAX) != expected_job_count {
            bail!("expanded job count must match coverage succeeded_job_count");
        }
        write_jsonl(&self.config.check.manifest_path, &rows)?;
        Checker::new(self.config.check.clone()).run(&snapshot)
    }
}

struct Checker {
    config: CheckConfig,
}

impl Checker {
    fn new(config: CheckConfig) -> Self {
        Self { config }
    }

    fn run(&self, snapshot: &ReportSnapshot) -> anyhow::Result<()> {
        let Some(coverage) = snapshot.get(SNAPSHOT_NATIONAL_COVERAGE_LEDGER) else {
            let report = self.skip_report("national coverage ledger has not been produced");
            write_json_file(&self.config.report_path, &report)?;
            println!(
                "national-bronze-object-manifest-ok status=skipped report={}",
                self.config.report_path.display()
            );
            return Ok(());
        };

        let coverage_schema = json_string(coverage, "schema_version");
        let coverage_status = json_string(coverage, "status");
        if coverage_schema == COVERAGE_SCHEMA_VERSION && coverage_status == "skipped" {
            let report = self.skip_report("national coverage ledger is skipped");
            write_json_file(&self.config.report_path, &report)?;
            println!(
                "national-bronze-object-manifest-ok status=skipped report={}",
                self.config.report_path.display()
            );
            return Ok(());
        }

        let mut blockers = snapshot_coverage_blockers(snapshot);
        let coverage_summary = coverage.get("coverage").unwrap_or(&JsonValue::Null);
        let expected_job_count = json_u64(coverage_summary, "succeeded_job_count", 0);
        let expected_object_count = json_u64(coverage_summary, "request_count_total", 0);

        let rows = if !self.config.manifest_path.is_file() {
            blockers.push("national Bronze object manifest has not been produced".to_owned());
            Vec::new()
        } else {
            add_forbidden_token_blockers(
                &self.config.manifest_path,
                "Bronze object manifest",
                &mut blockers,
            );
            read_jsonl(&self.config.manifest_path, Some(&mut blockers))?
        };

        let parsed_entries = validate_manifest_rows(&rows, &mut blockers);
        let summary = summarize_entries(
            &parsed_entries,
            expected_job_count,
            expected_object_count,
            &mut blockers,
        );
        let provider_rows = provider_rows(&parsed_entries);
        let status = if blockers.is_empty() {
            "ready"
        } else {
            "blocked"
        };
        let report = ManifestReport {
            schema_version: REPORT_SCHEMA_VERSION,
            generated_at_utc: utc_now(),
            git_head: git_head(&self.config.root),
            status,
            coverage_ledger_path: repo_relative_path(
                &self.config.root,
                &self.config.coverage_ledger_path,
            ),
            manifest_path: repo_relative_path(&self.config.root, &self.config.manifest_path),
            summary,
            providers: provider_rows,
            blockers: blockers.clone(),
            completion_claim_allowed: false,
            production_cutover_allowed: false,
            national_rollout_allowed: false,
            evidence_limitations: vec![
                "bronze_object_manifest_only",
                "does_not_download_or_transform_r2_objects",
                "does_not_promote_silver_gold_national_tables",
                "does_not_approve_production_cutover",
            ],
            next_gates: vec!["silver-gold-national-promotion"],
        };
        write_json_file(&self.config.report_path, &report)?;
        if status != "ready" {
            println!(
                "national-bronze-object-manifest-blocked status={status} blockers={} report={}",
                blockers.len(),
                self.config.report_path.display()
            );
            for blocker in blockers {
                println!("blocker={blocker}");
            }
            bail!("national Bronze object manifest blocked");
        }
        println!(
            "national-bronze-object-manifest-ok status=ready jobs={} objects={} report={}",
            report.summary.job_count,
            report.summary.object_count,
            self.config.report_path.display()
        );
        Ok(())
    }

    fn skip_report(&self, reason: &'static str) -> SkipReport {
        SkipReport {
            schema_version: REPORT_SCHEMA_VERSION,
            generated_at_utc: utc_now(),
            git_head: git_head(&self.config.root),
            status: "skipped",
            coverage_ledger_path: repo_relative_path(
                &self.config.root,
                &self.config.coverage_ledger_path,
            ),
            manifest_path: repo_relative_path(&self.config.root, &self.config.manifest_path),
            summary: SkipSummary {
                job_count: 0,
                object_count: 0,
                provider_count: 0,
            },
            providers: Vec::new(),
            blockers: vec![reason],
            completion_claim_allowed: false,
            production_cutover_allowed: false,
            national_rollout_allowed: false,
            evidence_limitations: vec!["bronze_object_manifest_not_evaluated"],
            next_gates: vec!["national-bronze-object-manifest"],
        }
    }
}

fn load_report_snapshot(config: &CheckConfig) -> anyhow::Result<ReportSnapshot> {
    let mut snapshot = ReportSnapshot::new();
    load_json_snapshot(
        &mut snapshot,
        SNAPSHOT_NATIONAL_COVERAGE_LEDGER,
        &config.coverage_ledger_path,
        "coverage ledger",
    )?;
    Ok(snapshot)
}

fn load_json_snapshot(
    snapshot: &mut ReportSnapshot,
    key: &'static str,
    path: &Path,
    label: &str,
) -> anyhow::Result<()> {
    if path.is_file() {
        snapshot.insert(key, read_json(path, label)?);
    }
    Ok(())
}

fn coverage_from_snapshot<'a>(
    snapshot: &'a ReportSnapshot,
    coverage_ledger_path: &Path,
) -> anyhow::Result<&'a JsonValue> {
    let Some(coverage) = snapshot.get(SNAPSHOT_NATIONAL_COVERAGE_LEDGER) else {
        bail!(
            "coverage ledger missing: {}",
            coverage_ledger_path.display()
        );
    };
    Ok(coverage)
}

pub(crate) fn snapshot_coverage_blockers(snapshot: &ReportSnapshot) -> Vec<String> {
    let mut blockers = Vec::new();
    let Some(coverage) = snapshot.get(SNAPSHOT_NATIONAL_COVERAGE_LEDGER) else {
        blockers.push("national coverage ledger has not been produced".to_owned());
        return blockers;
    };
    add_forbidden_token_blockers_from_value(coverage, "coverage ledger", &mut blockers);
    validate_coverage_for_manifest(coverage, &mut blockers);
    blockers
}

#[derive(Serialize)]
struct ManifestEntry {
    schema_version: &'static str,
    compiler_input_hash_sha256: String,
    request_fingerprint_schema_version: String,
    request_fingerprint_sha256: String,
    collection_snapshot_id: String,
    coverage_ledger_path: String,
    execution_evidence_path: String,
    event_log_path: String,
    job_id: String,
    scope_unit_id: String,
    provider: String,
    endpoint: String,
    storage_driver: String,
    object_key: String,
    page_number: u64,
    page_count: u64,
    job_source_record_count: u64,
    job_last_bronze_object_key: String,
}

#[derive(Clone)]
struct ParsedEntry {
    job_id: String,
    provider: String,
    endpoint: String,
    object_key: String,
    request_fingerprint_schema_version: String,
    request_fingerprint_sha256: String,
    collection_snapshot_id: String,
    page_number: u64,
    page_count: u64,
}

#[derive(Serialize)]
struct ManifestReport {
    schema_version: &'static str,
    generated_at_utc: String,
    git_head: String,
    status: &'static str,
    coverage_ledger_path: String,
    manifest_path: String,
    summary: ManifestSummary,
    providers: Vec<ProviderRow>,
    blockers: Vec<String>,
    completion_claim_allowed: bool,
    production_cutover_allowed: bool,
    national_rollout_allowed: bool,
    evidence_limitations: Vec<&'static str>,
    next_gates: Vec<&'static str>,
}

#[derive(Serialize)]
struct ManifestSummary {
    job_count: u64,
    object_count: u64,
    expected_job_count: u64,
    expected_object_count: u64,
    request_fingerprint_count: u64,
    provider_count: u64,
}

#[derive(Serialize)]
struct ProviderRow {
    provider: String,
    endpoint: String,
    job_count: u64,
    object_count: u64,
}

#[derive(Serialize)]
struct SkipReport {
    schema_version: &'static str,
    generated_at_utc: String,
    git_head: String,
    status: &'static str,
    coverage_ledger_path: String,
    manifest_path: String,
    summary: SkipSummary,
    providers: Vec<ProviderRow>,
    blockers: Vec<&'static str>,
    completion_claim_allowed: bool,
    production_cutover_allowed: bool,
    national_rollout_allowed: bool,
    evidence_limitations: Vec<&'static str>,
    next_gates: Vec<&'static str>,
}

#[derive(Serialize)]
struct SkipSummary {
    job_count: u64,
    object_count: u64,
    provider_count: u64,
}

fn assert_ready_coverage(coverage: &JsonValue) -> anyhow::Result<()> {
    if json_string(coverage, "schema_version") != COVERAGE_SCHEMA_VERSION {
        bail!("coverage ledger schema mismatch");
    }
    if json_string(coverage, "status") != "ready" {
        bail!("coverage ledger status must be ready");
    }
    let summary = coverage.get("coverage").unwrap_or(&JsonValue::Null);
    for field in [
        "failed_job_count",
        "missing_job_count",
        "extra_job_count",
        "duplicate_succeeded_job_count",
    ] {
        if json_u64(summary, field, 0) != 0 {
            bail!("coverage {field} must be zero");
        }
    }
    if !json_bool(summary, "raw_response_preserved", false) {
        bail!("coverage raw_response_preserved must be true");
    }
    Ok(())
}

fn validate_coverage_for_manifest(coverage: &JsonValue, blockers: &mut Vec<String>) {
    add_if(
        blockers,
        json_string(coverage, "schema_version") != COVERAGE_SCHEMA_VERSION,
        "coverage ledger schema mismatch",
    );
    add_if(
        blockers,
        json_string(coverage, "status") != "ready",
        "coverage ledger status must be ready",
    );
    add_if(
        blockers,
        json_bool(coverage, "completion_claim_allowed", true),
        "coverage ledger completion_claim_allowed must be false",
    );
    add_if(
        blockers,
        json_bool(coverage, "production_cutover_allowed", true),
        "coverage ledger production_cutover_allowed must be false",
    );
    add_if(
        blockers,
        json_bool(coverage, "national_rollout_allowed", true),
        "coverage ledger national_rollout_allowed must be false",
    );
    let summary = coverage.get("coverage").unwrap_or(&JsonValue::Null);
    for field in [
        "failed_job_count",
        "missing_job_count",
        "extra_job_count",
        "duplicate_succeeded_job_count",
    ] {
        add_if(
            blockers,
            json_u64(summary, field, 0) != 0,
            format!("coverage {field} must be zero"),
        );
    }
    add_if(
        blockers,
        !json_bool(summary, "raw_response_preserved", false),
        "coverage raw_response_preserved must be true",
    );
}

fn expand_bronze_object_keys(
    job_id: &str,
    last_object_key: &str,
    request_count: u64,
) -> anyhow::Result<Vec<String>> {
    if request_count < 1 {
        bail!("succeeded event request_count must be positive: {job_id}");
    }
    // ADR 0019: the API-page lanes write a readable `.../page-NNNNNN.json` leaf (no `run_id`,
    // no `partition=` wrapper, and no `/page=NNN/part-NNN.json` directory+file). The
    // page identity is the zero-padded number in the leaf filename, and the last page must equal
    // request_count. Every prior page shares the same prefix and zero-pad width.
    let Some(stripped_suffix) = last_object_key.strip_suffix(".json") else {
        bail!("succeeded event bronze_object_key must end with a page-NNNNNN.json leaf: {job_id}");
    };
    let Some(page_index) = stripped_suffix.rfind("/page-") else {
        bail!("succeeded event bronze_object_key must end with a page-NNNNNN.json leaf: {job_id}");
    };
    let prefix = &last_object_key[..page_index + "/page-".len()];
    let page_text = &stripped_suffix[page_index + "/page-".len()..];
    let last_page = page_text
        .parse::<u64>()
        .context("failed to parse last page number")?;
    if last_page != request_count {
        bail!("succeeded event request_count must match last bronze object page: {job_id}");
    }
    let mut keys = Vec::new();
    for page in 1..=request_count {
        keys.push(format!(
            "{prefix}{page:0page_width$}.json",
            page_width = page_text.len(),
        ));
    }
    Ok(keys)
}

fn validate_manifest_rows(rows: &[JsonValue], blockers: &mut Vec<String>) -> Vec<ParsedEntry> {
    let mut parsed = Vec::new();
    let mut object_keys = HashSet::new();
    let mut fingerprint_jobs = HashMap::<String, String>::new();
    for row in rows {
        let entry = ParsedEntry {
            job_id: json_string(row, "job_id"),
            provider: json_string(row, "provider"),
            endpoint: json_string(row, "endpoint"),
            object_key: json_string(row, "object_key"),
            request_fingerprint_schema_version: json_string(
                row,
                "request_fingerprint_schema_version",
            ),
            request_fingerprint_sha256: json_string(row, "request_fingerprint_sha256"),
            collection_snapshot_id: json_string(row, "collection_snapshot_id"),
            page_number: json_u64(row, "page_number", 0),
            page_count: json_u64(row, "page_count", 0),
        };
        add_if(
            blockers,
            json_string(row, "schema_version") != ENTRY_SCHEMA_VERSION,
            "manifest entry schema mismatch",
        );
        add_if(
            blockers,
            entry.job_id.trim().is_empty(),
            "manifest entry job_id is required",
        );
        add_if(
            blockers,
            entry.request_fingerprint_schema_version
                != "foundation-platform.bronze_request_fingerprint.v1",
            format!(
                "manifest entry request_fingerprint_schema_version is required: {}",
                entry.job_id
            ),
        );
        add_if(
            blockers,
            !is_lower_sha256(&entry.request_fingerprint_sha256),
            format!(
                "manifest entry request_fingerprint_sha256 is required: {}",
                entry.job_id
            ),
        );
        add_if(
            blockers,
            entry.collection_snapshot_id.trim().is_empty(),
            format!(
                "manifest entry collection_snapshot_id is required: {}",
                entry.job_id
            ),
        );
        add_if(
            blockers,
            entry.provider.trim().is_empty(),
            format!("manifest entry provider is required: {}", entry.job_id),
        );
        add_if(
            blockers,
            entry.endpoint.trim().is_empty(),
            format!("manifest entry endpoint is required: {}", entry.job_id),
        );
        add_if(
            blockers,
            entry.object_key.trim().is_empty(),
            format!("manifest entry object_key is required: {}", entry.job_id),
        );
        add_if(
            blockers,
            !entry.object_key.starts_with("bronze/source="),
            format!(
                "manifest entry object_key must be provider-relative Bronze key: {}",
                entry.job_id
            ),
        );
        add_if(
            blockers,
            json_string(row, "storage_driver") != "r2",
            format!("manifest entry storage_driver must be r2: {}", entry.job_id),
        );
        add_if(
            blockers,
            entry.page_number < 1,
            format!(
                "manifest entry page_number must be positive: {}",
                entry.job_id
            ),
        );
        add_if(
            blockers,
            entry.page_count < 1,
            format!(
                "manifest entry page_count must be positive: {}",
                entry.job_id
            ),
        );
        add_if(
            blockers,
            entry.page_number > entry.page_count,
            format!(
                "manifest entry page_number must not exceed page_count: {}",
                entry.job_id
            ),
        );
        if !object_keys.insert(entry.object_key.clone()) {
            blockers.push(format!("duplicate Bronze object key: {}", entry.object_key));
        }
        if is_lower_sha256(&entry.request_fingerprint_sha256) {
            match fingerprint_jobs.get(&entry.request_fingerprint_sha256) {
                Some(job_id) if job_id != &entry.job_id => blockers.push(format!(
                    "duplicate request fingerprint across jobs: {}",
                    entry.request_fingerprint_sha256
                )),
                Some(_) => {}
                None => {
                    fingerprint_jobs.insert(
                        entry.request_fingerprint_sha256.clone(),
                        entry.job_id.clone(),
                    );
                }
            }
        }
        parsed.push(entry);
    }
    parsed
}

fn summarize_entries(
    entries: &[ParsedEntry],
    expected_job_count: u64,
    expected_object_count: u64,
    blockers: &mut Vec<String>,
) -> ManifestSummary {
    let mut jobs = BTreeMap::<String, Vec<ParsedEntry>>::new();
    let mut fingerprints = HashSet::new();
    for entry in entries {
        jobs.entry(entry.job_id.clone())
            .or_default()
            .push(entry.clone());
        if is_lower_sha256(&entry.request_fingerprint_sha256) {
            fingerprints.insert(entry.request_fingerprint_sha256.clone());
        }
    }
    for (job_id, group_rows) in &jobs {
        let page_count = group_rows[0].page_count;
        let request_fingerprint_schema = &group_rows[0].request_fingerprint_schema_version;
        let request_fingerprint = &group_rows[0].request_fingerprint_sha256;
        let collection_snapshot_id = &group_rows[0].collection_snapshot_id;
        if u64::try_from(group_rows.len()).unwrap_or(u64::MAX) != page_count {
            blockers.push(format!("manifest page count mismatch: {job_id}"));
        }
        let mut seen_pages = HashSet::new();
        for row in group_rows {
            if row.page_count != page_count {
                blockers.push(format!(
                    "manifest page_count must be stable within job: {job_id}"
                ));
            }
            if &row.request_fingerprint_schema_version != request_fingerprint_schema
                || &row.request_fingerprint_sha256 != request_fingerprint
                || &row.collection_snapshot_id != collection_snapshot_id
            {
                blockers.push(format!(
                    "manifest request fingerprint metadata must be stable within job: {job_id}"
                ));
            }
            if !seen_pages.insert(row.page_number) {
                blockers.push(format!(
                    "duplicate manifest page: {job_id} page={}",
                    row.page_number
                ));
            }
        }
        for page in 1..=page_count {
            if !seen_pages.contains(&page) {
                blockers.push(format!("missing manifest page: {job_id} page={page}"));
            }
        }
    }
    add_if(
        blockers,
        u64::try_from(entries.len()).unwrap_or(u64::MAX) != expected_object_count,
        "manifest object count must match coverage request_count_total",
    );
    add_if(
        blockers,
        u64::try_from(jobs.len()).unwrap_or(u64::MAX) != expected_job_count,
        "manifest job count must match coverage succeeded_job_count",
    );
    let provider_count = provider_rows(entries).len();
    ManifestSummary {
        job_count: u64::try_from(jobs.len()).unwrap_or(u64::MAX),
        object_count: u64::try_from(entries.len()).unwrap_or(u64::MAX),
        expected_job_count,
        expected_object_count,
        request_fingerprint_count: u64::try_from(fingerprints.len()).unwrap_or(u64::MAX),
        provider_count: u64::try_from(provider_count).unwrap_or(u64::MAX),
    }
}

fn provider_rows(entries: &[ParsedEntry]) -> Vec<ProviderRow> {
    let mut rows = BTreeMap::<(String, String), ProviderRow>::new();
    let mut job_buckets = HashSet::<(String, String, String)>::new();
    for entry in entries {
        let key = (entry.provider.clone(), entry.endpoint.clone());
        let row = rows.entry(key.clone()).or_insert_with(|| ProviderRow {
            provider: entry.provider.clone(),
            endpoint: entry.endpoint.clone(),
            job_count: 0,
            object_count: 0,
        });
        row.object_count += 1;
        if job_buckets.insert((
            entry.provider.clone(),
            entry.endpoint.clone(),
            entry.job_id.clone(),
        )) {
            row.job_count += 1;
        }
    }
    rows.into_values().collect()
}

fn read_jsonl(
    path: &Path,
    mut blockers: Option<&mut Vec<String>>,
) -> anyhow::Result<Vec<JsonValue>> {
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut rows = Vec::new();
    for (index, raw_line) in content.lines().enumerate() {
        let line_number = index + 1;
        let line = if line_number == 1 {
            raw_line.trim_start_matches('\u{feff}')
        } else {
            raw_line
        };
        if line.trim().is_empty() {
            if let Some(blockers) = blockers.as_deref_mut() {
                blockers.push(format!("manifest line {line_number} must not be blank"));
            }
            continue;
        }
        match serde_json::from_str::<JsonValue>(line) {
            Ok(row) => rows.push(row),
            Err(error) => {
                if let Some(blockers) = blockers.as_deref_mut() {
                    blockers.push(format!("manifest line {line_number} is not valid JSON"));
                } else {
                    return Err(error)
                        .with_context(|| format!("manifest line {line_number} is not valid JSON"));
                }
            }
        }
    }
    Ok(rows)
}

fn write_jsonl(path: &Path, rows: &[ManifestEntry]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create manifest directory {}", parent.display()))?;
    }
    let mut output = String::new();
    for row in rows {
        output.push_str(&serde_json::to_string(row).context("failed to serialize manifest row")?);
        output.push('\n');
    }
    fs::write(path, output).with_context(|| format!("failed to write {}", path.display()))
}

fn add_forbidden_token_blockers(path: &Path, label: &str, blockers: &mut Vec<String>) {
    if !path.is_file() {
        return;
    }
    let Ok(content) = fs::read_to_string(path) else {
        return;
    };
    add_forbidden_token_blockers_from_content(&content, label, blockers);
}

fn add_forbidden_token_blockers_from_value(
    value: &JsonValue,
    label: &str,
    blockers: &mut Vec<String>,
) {
    let Ok(content) = serde_json::to_string(value) else {
        return;
    };
    add_forbidden_token_blockers_from_content(&content, label, blockers);
}

fn add_forbidden_token_blockers_from_content(
    content: &str,
    label: &str,
    blockers: &mut Vec<String>,
) {
    for forbidden in [
        "DATA_GO_KR_SERVICE_KEY",
        "VWORLD_API_KEY",
        "serviceKey",
        "raw_payload",
        "unit-test-key",
        "fake-vworld-key",
    ] {
        if content.contains(forbidden) {
            blockers.push(format!(
                "{label} must not contain forbidden token: {forbidden}"
            ));
        }
    }
}

fn add_if<S: Into<String>>(blockers: &mut Vec<String>, condition: bool, message: S) {
    if condition {
        blockers.push(message.into());
    }
}

fn json_string(value: &JsonValue, field: &str) -> String {
    value
        .get(field)
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn json_string_array(value: &JsonValue, field: &str, default: Vec<String>) -> Vec<String> {
    value
        .get(field)
        .and_then(JsonValue::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(JsonValue::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or(default)
}

fn json_bool(value: &JsonValue, field: &str, default: bool) -> bool {
    value
        .get(field)
        .and_then(JsonValue::as_bool)
        .unwrap_or(default)
}

fn json_u64(value: &JsonValue, field: &str, default: u64) -> u64 {
    value
        .get(field)
        .and_then(|raw| {
            raw.as_u64()
                .or_else(|| raw.as_i64().and_then(|value| value.try_into().ok()))
        })
        .unwrap_or(default)
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn env_bool(name: &str, default: bool) -> anyhow::Result<bool> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(default),
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Ok(true),
            "0" | "false" | "no" => Ok(false),
            _ => bail!("{name} must be a boolean"),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn normalize_windows_verbatim_path(path: PathBuf) -> PathBuf {
    const VERBATIM_PREFIX: &str = r"\\?\";
    let raw = path.to_string_lossy();
    if let Some(stripped) = raw.strip_prefix(VERBATIM_PREFIX) {
        PathBuf::from(stripped)
    } else {
        path
    }
}

#[cfg(test)]
mod tests {
    use super::{
        coverage_from_snapshot, expand_bronze_object_keys, ReportSnapshot,
        SNAPSHOT_NATIONAL_COVERAGE_LEDGER,
    };
    use serde_json::json;
    use std::path::Path;

    #[test]
    fn expand_bronze_object_keys_expands_readable_page_leaf() {
        // ADR 0019: the API-page lanes now write a readable `page-NNNNNN.json` leaf
        // (no `run_id`, no `partition=` wrapper, no `/page=NNN/part-NNN.json` directory+file).
        // The expander must reconstruct every page key from the last page key.
        let last_object_key = "bronze/source=datagokr__building_register_main/sigungu=11680/bjdong=10300/page-000003.json";
        let keys = expand_bronze_object_keys("job-1", last_object_key, 3)
            .expect("readable page leaf must expand");
        assert_eq!(
            keys,
            vec![
                "bronze/source=datagokr__building_register_main/sigungu=11680/bjdong=10300/page-000001.json".to_owned(),
                "bronze/source=datagokr__building_register_main/sigungu=11680/bjdong=10300/page-000002.json".to_owned(),
                "bronze/source=datagokr__building_register_main/sigungu=11680/bjdong=10300/page-000003.json".to_owned(),
            ]
        );
    }

    #[test]
    fn expand_bronze_object_keys_rejects_zero_request_count() {
        let last_object_key =
            "bronze/source=datagokr__building_register_main/sigungu=11680/page-000001.json";
        assert!(expand_bronze_object_keys("job-1", last_object_key, 0).is_err());
    }

    #[test]
    fn expand_bronze_object_keys_rejects_last_page_request_count_mismatch() {
        // The leaf page number must equal request_count.
        let last_object_key =
            "bronze/source=datagokr__building_register_main/sigungu=11680/page-000002.json";
        assert!(expand_bronze_object_keys("job-1", last_object_key, 3).is_err());
    }

    #[test]
    fn expand_bronze_object_keys_rejects_legacy_part_leaf() {
        // The old `/page=NNN/part-NNN.json` directory+file shape is no longer produced.
        let legacy_key =
            "bronze/source=datagokr__building_register_main/page=000001/part-000001.json";
        assert!(expand_bronze_object_keys("job-1", legacy_key, 1).is_err());
    }

    #[test]
    fn coverage_from_snapshot_uses_frozen_snapshot_value() {
        let mut snapshot = ReportSnapshot::new();
        snapshot.insert(
            SNAPSHOT_NATIONAL_COVERAGE_LEDGER,
            json!({
                "status": "ready",
                "coverage": {
                    "request_count_total": 1,
                    "succeeded_job_count": 1
                }
            }),
        );

        let coverage =
            coverage_from_snapshot(&snapshot, Path::new("target/audit/coverage-ledger.json"))
                .expect("coverage should come from snapshot");

        assert_eq!(coverage["status"], json!("ready"));
    }

    #[test]
    fn coverage_from_snapshot_fails_when_snapshot_omits_coverage() {
        let snapshot = ReportSnapshot::new();

        let error =
            coverage_from_snapshot(&snapshot, Path::new("target/audit/coverage-ledger.json"))
                .expect_err("missing snapshot coverage should fail closed");

        assert!(error
            .to_string()
            .contains("coverage ledger missing: target/audit/coverage-ledger.json"));
    }
}
