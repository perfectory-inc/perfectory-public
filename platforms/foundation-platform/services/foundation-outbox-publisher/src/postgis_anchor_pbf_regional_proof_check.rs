use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{bail, Context};
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::public_data_control_support::{
    env_path, git_head, read_json, repo_relative_path, resolve_cargo_exe, utc_now, write_json_file,
};

const SCHEMA_VERSION: &str = "foundation-platform.postgis_anchor_pbf_regional_proof.v1";
const QUALITY_SCHEMA_VERSION: &str =
    "foundation-platform.silver_gold_data_collection_quality_evidence.v1";
const POSTGIS_SCHEMA_VERSION: &str =
    "foundation-platform.postgis_parcel_boundary_mirror_rebuild_summary.v1";
const ANCHOR_SCHEMA_VERSION: &str = "foundation-platform.parcel_marker_anchor_rebuild_summary.v1";
const DEFAULT_QUALITY_PATH: &str = "target/audit/silver-gold-data-collection-quality-evidence.json";
const DEFAULT_POSTGIS_PATH: &str =
    "target/audit/postgis-parcel-boundary-mirror-rebuild-summary.json";
const DEFAULT_ANCHOR_PATH: &str = "target/audit/parcel-marker-anchor-rebuild-summary.json";
const DEFAULT_OUTPUT_PATH: &str = "target/audit/postgis-anchor-pbf-regional-proof.json";
pub(crate) const SNAPSHOT_SILVER_GOLD_QUALITY: &str = "silver_gold_quality";
pub(crate) const SNAPSHOT_POSTGIS_REBUILD: &str = "postgis_rebuild";
pub(crate) const SNAPSHOT_ANCHOR_REBUILD: &str = "anchor_rebuild";
type ReportSnapshot = HashMap<&'static str, JsonValue>;

pub fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let snapshot = load_report_snapshot(&config)?;
    run_with_snapshot(&config, &snapshot)
}

fn run_with_snapshot(config: &Config, snapshot: &ReportSnapshot) -> anyhow::Result<()> {
    let evidence = snapshot_evidence(snapshot);
    let quality_exists = evidence.quality.is_some();
    let postgis_exists = evidence.postgis.is_some();
    let anchor_exists = evidence.anchor.is_some();

    if !quality_exists && !postgis_exists && !anchor_exists {
        let report = Report::skipped(config);
        write_json_file(&config.output_path, &report)?;
        println!(
            "postgis-anchor-pbf-regional-proof-ok status=skipped report={}",
            config.output_path.display()
        );
        return Ok(());
    }

    let mut blockers = Vec::new();
    let verdict = evaluate_snapshot_evidence(&evidence, &mut blockers);

    let mut pbf_contract_tests = Vec::new();
    if blockers.is_empty() {
        match run_pbf_contract_tests(config) {
            Ok(results) => {
                for result in &results {
                    if result.exit_code != 0 {
                        blockers.push(format!("PBF contract test failed: {}", result.command));
                    }
                }
                pbf_contract_tests = results;
            }
            Err(error) => blockers.push(format!("PBF contract test failed: {error}")),
        }
    }

    let status = if blockers.is_empty() {
        "ready"
    } else {
        "blocked"
    };
    let report = Report::checked(
        config,
        status,
        blockers.clone(),
        pbf_contract_tests.clone(),
        PostgisSummary {
            source_snapshot_id: verdict.postgis_source_snapshot_id,
            source_table: json_string_opt(evidence.postgis, "source_table"),
            target_srid: json_string_opt(evidence.postgis, "target_srid"),
            row_count: verdict.postgis_row_count,
            loaded_row_count: verdict.postgis_loaded_row_count,
        },
        AnchorSummary {
            source_snapshot_id: verdict.anchor_source_snapshot_id,
            source_table: json_string_opt(evidence.anchor, "source_table"),
            generation_run_id: json_string_opt(evidence.anchor, "generation_run_id"),
            algorithm: json_string_opt(evidence.anchor, "algorithm"),
            algorithm_version: json_string_opt(evidence.anchor, "algorithm_version"),
            scanned_row_count: verdict.anchor_scanned_row_count,
            loaded_row_count: verdict.anchor_loaded_row_count,
            rejected_row_count: verdict.anchor_rejected_row_count,
            superseded_row_count: verdict.anchor_superseded_row_count,
        },
    );
    write_json_file(&config.output_path, &report)?;

    if status != "ready" {
        println!(
            "postgis-anchor-pbf-regional-proof-blocked status={status} blockers={} report={}",
            blockers.len(),
            config.output_path.display()
        );
        for blocker in blockers {
            println!("blocker={blocker}");
        }
        bail!("postgis anchor PBF regional proof blocked");
    }

    println!(
        "postgis-anchor-pbf-regional-proof-ok status=ready postgis_rows={postgis_loaded_row_count} anchor_rows={anchor_loaded_row_count} pbf_tests={pbf_test_count} report={report_path}",
        postgis_loaded_row_count = verdict.postgis_loaded_row_count,
        anchor_loaded_row_count = verdict.anchor_loaded_row_count,
        pbf_test_count = pbf_contract_tests.len(),
        report_path = config.output_path.display()
    );
    Ok(())
}

fn load_report_snapshot(config: &Config) -> anyhow::Result<ReportSnapshot> {
    let mut snapshot = ReportSnapshot::new();
    load_json_snapshot(
        &mut snapshot,
        SNAPSHOT_SILVER_GOLD_QUALITY,
        &config.quality_path,
        "silver/gold quality evidence",
    )?;
    load_json_snapshot(
        &mut snapshot,
        SNAPSHOT_POSTGIS_REBUILD,
        &config.postgis_path,
        "PostGIS mirror rebuild summary",
    )?;
    load_json_snapshot(
        &mut snapshot,
        SNAPSHOT_ANCHOR_REBUILD,
        &config.anchor_path,
        "parcel marker anchor rebuild summary",
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

struct SnapshotEvidence<'a> {
    quality: Option<&'a JsonValue>,
    postgis: Option<&'a JsonValue>,
    anchor: Option<&'a JsonValue>,
}

struct SnapshotVerdict {
    postgis_source_snapshot_id: String,
    anchor_source_snapshot_id: String,
    postgis_row_count: i64,
    postgis_loaded_row_count: i64,
    anchor_scanned_row_count: i64,
    anchor_loaded_row_count: i64,
    anchor_rejected_row_count: i64,
    anchor_superseded_row_count: i64,
}

fn snapshot_evidence(snapshot: &ReportSnapshot) -> SnapshotEvidence<'_> {
    SnapshotEvidence {
        quality: snapshot.get(SNAPSHOT_SILVER_GOLD_QUALITY),
        postgis: snapshot.get(SNAPSHOT_POSTGIS_REBUILD),
        anchor: snapshot.get(SNAPSHOT_ANCHOR_REBUILD),
    }
}

#[cfg(test)]
pub(crate) fn snapshot_evidence_blockers(snapshot: &ReportSnapshot) -> Vec<String> {
    let evidence = snapshot_evidence(snapshot);
    let mut blockers = Vec::new();
    evaluate_snapshot_evidence(&evidence, &mut blockers);
    blockers
}

fn evaluate_snapshot_evidence(
    evidence: &SnapshotEvidence<'_>,
    blockers: &mut Vec<String>,
) -> SnapshotVerdict {
    validate_quality(evidence.quality, blockers);
    validate_postgis(evidence.postgis, blockers);
    validate_anchor(evidence.anchor, blockers);

    let postgis_source_snapshot_id = json_string_opt(evidence.postgis, "source_snapshot_id");
    let anchor_source_snapshot_id = json_string_opt(evidence.anchor, "source_snapshot_id");
    let postgis_row_count =
        json_i64_opt(evidence.postgis, "row_count", "postgis.row_count", blockers);
    let postgis_loaded_row_count = json_i64_opt(
        evidence.postgis,
        "loaded_row_count",
        "postgis.loaded_row_count",
        blockers,
    );
    let anchor_scanned_row_count = json_i64_opt(
        evidence.anchor,
        "scanned_row_count",
        "anchor.scanned_row_count",
        blockers,
    );
    let anchor_loaded_row_count = json_i64_opt(
        evidence.anchor,
        "loaded_row_count",
        "anchor.loaded_row_count",
        blockers,
    );
    let anchor_rejected_row_count = json_i64_opt(
        evidence.anchor,
        "rejected_row_count",
        "anchor.rejected_row_count",
        blockers,
    );
    let anchor_superseded_row_count = json_i64_opt(
        evidence.anchor,
        "superseded_row_count",
        "anchor.superseded_row_count",
        blockers,
    );

    add_if(
        blockers,
        !is_iceberg_snapshot_id(&postgis_source_snapshot_id),
        "PostGIS source snapshot must use iceberg:<snapshot-id> format",
    );
    add_if(
        blockers,
        anchor_source_snapshot_id != postgis_source_snapshot_id,
        "anchor source snapshot must match PostGIS source snapshot",
    );
    add_if(
        blockers,
        postgis_row_count < 1,
        "PostGIS row_count must be positive",
    );
    add_if(
        blockers,
        postgis_loaded_row_count < 1,
        "PostGIS loaded_row_count must be positive",
    );
    add_if(
        blockers,
        postgis_row_count != postgis_loaded_row_count,
        "PostGIS row_count must match loaded_row_count",
    );
    add_if(
        blockers,
        anchor_scanned_row_count != postgis_loaded_row_count,
        "anchor scanned row count must match PostGIS loaded row count",
    );
    add_if(
        blockers,
        anchor_loaded_row_count != postgis_loaded_row_count,
        "anchor loaded row count must match PostGIS loaded row count",
    );
    add_if(
        blockers,
        anchor_rejected_row_count != 0,
        "anchor rejected row count must be zero",
    );

    SnapshotVerdict {
        postgis_source_snapshot_id,
        anchor_source_snapshot_id,
        postgis_row_count,
        postgis_loaded_row_count,
        anchor_scanned_row_count,
        anchor_loaded_row_count,
        anchor_rejected_row_count,
        anchor_superseded_row_count,
    }
}

fn validate_quality(value: Option<&JsonValue>, blockers: &mut Vec<String>) {
    let Some(value) = value else {
        blockers.push("silver/gold quality evidence missing".to_owned());
        return;
    };
    add_if(
        blockers,
        json_string(value, "schema_version") != QUALITY_SCHEMA_VERSION,
        "silver/gold quality evidence schema mismatch",
    );
    add_if(
        blockers,
        json_string(value, "status") != "ready",
        "silver/gold quality evidence status must be ready",
    );
    add_if(
        blockers,
        json_bool(value, "completion_claim_allowed", false),
        "silver/gold quality completion_claim_allowed must be false",
    );
    add_if(
        blockers,
        json_bool(value, "national_rollout_allowed", false),
        "silver/gold quality national_rollout_allowed must be false",
    );

    let summary = value.get("summary");
    let table_count = json_i64_opt(
        summary,
        "table_count",
        "quality.summary.table_count",
        blockers,
    );
    add_if(
        blockers,
        table_count < 3,
        "silver/gold quality table_count must include catalog, gold, and spatial summaries",
    );
    let invalid_geometry_count = json_i64_opt(
        summary,
        "spatial_invalid_geometry_count",
        "quality.summary.spatial_invalid_geometry_count",
        blockers,
    );
    add_if(
        blockers,
        invalid_geometry_count != 0,
        "silver/gold quality spatial_invalid_geometry_count must be zero",
    );
}

fn validate_postgis(value: Option<&JsonValue>, blockers: &mut Vec<String>) {
    let Some(value) = value else {
        blockers.push("PostGIS mirror rebuild summary missing".to_owned());
        return;
    };
    add_if(
        blockers,
        json_string(value, "schema_version") != POSTGIS_SCHEMA_VERSION,
        "PostGIS rebuild summary schema mismatch",
    );
    add_if(
        blockers,
        json_bool(value, "validate_only", true),
        "PostGIS rebuild must be executed, not validate-only",
    );
    add_if(
        blockers,
        json_string(value, "source_table") != "silver.parcel_boundaries",
        "PostGIS source_table must be silver.parcel_boundaries",
    );
    add_if(
        blockers,
        json_string(value, "target_srid") != "EPSG:5179",
        "PostGIS target_srid must be EPSG:5179",
    );
}

fn validate_anchor(value: Option<&JsonValue>, blockers: &mut Vec<String>) {
    let Some(value) = value else {
        blockers.push("parcel marker anchor rebuild summary missing".to_owned());
        return;
    };
    add_if(
        blockers,
        json_string(value, "schema_version") != ANCHOR_SCHEMA_VERSION,
        "anchor rebuild summary schema mismatch",
    );
    add_if(
        blockers,
        json_string(value, "source_table") != "silver.parcel_boundaries",
        "anchor source_table must be silver.parcel_boundaries",
    );
    add_if(
        blockers,
        json_string(value, "algorithm") != "polylabel",
        "anchor algorithm must be polylabel",
    );
    add_if(
        blockers,
        json_string(value, "algorithm_version") != "postgis-st_maximuminscribedcircle-v1",
        "anchor algorithm_version mismatch",
    );
}

fn run_pbf_contract_tests(config: &Config) -> anyhow::Result<Vec<PbfContractTestResult>> {
    let cargo_path = resolve_cargo_exe(config.cargo_path.clone())?;
    [
        ("foundation-contracts", "marker_tile_contract_dto"),
        ("catalog-domain", "marker_tile_contract"),
    ]
    .into_iter()
    .map(|(package, test)| run_pbf_contract_test(&cargo_path, package, test))
    .collect()
}

fn run_pbf_contract_test(
    cargo_path: &Path,
    package: &str,
    test: &str,
) -> anyhow::Result<PbfContractTestResult> {
    let args = ["test", "-p", package, "--test", test];
    let output = Command::new(cargo_path)
        .args(args)
        .output()
        .with_context(|| format!("failed to run {}", cargo_path.display()))?;
    let mut combined = String::new();
    combined.push_str(&String::from_utf8_lossy(&output.stdout));
    combined.push_str(&String::from_utf8_lossy(&output.stderr));
    Ok(PbfContractTestResult {
        package: package.to_owned(),
        test: test.to_owned(),
        command: format!("cargo test -p {package} --test {test}"),
        exit_code: output.status.code().unwrap_or(1),
        status: if output.status.success() {
            "passed"
        } else {
            "failed"
        }
        .to_owned(),
        output: last_lines(&combined, 20),
    })
}

fn last_lines(value: &str, count: usize) -> String {
    let lines = value.lines().map(str::to_owned).collect::<Vec<_>>();
    let start = lines.len().saturating_sub(count);
    lines[start..].join("\n")
}

fn json_string_opt(value: Option<&JsonValue>, field: &str) -> String {
    value
        .map(|value| json_string(value, field))
        .unwrap_or_default()
}

fn json_string(value: &JsonValue, field: &str) -> String {
    value
        .get(field)
        .and_then(|value| match value {
            JsonValue::String(text) => Some(text.to_owned()),
            JsonValue::Number(number) => Some(number.to_string()),
            JsonValue::Bool(flag) => Some(flag.to_string()),
            _ => None,
        })
        .unwrap_or_default()
}

fn json_i64_opt(
    value: Option<&JsonValue>,
    field: &str,
    label: &str,
    blockers: &mut Vec<String>,
) -> i64 {
    let parsed = value.and_then(|value| value.get(field)).and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|value| value.try_into().ok()))
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
    });
    if let Some(parsed) = parsed {
        parsed
    } else {
        blockers.push(format!("{label} must be an integer"));
        0
    }
}

fn json_bool(value: &JsonValue, field: &str, default: bool) -> bool {
    value
        .get(field)
        .and_then(|value| match value {
            JsonValue::Bool(flag) => Some(*flag),
            JsonValue::String(text) => text.parse().ok(),
            _ => None,
        })
        .unwrap_or(default)
}

fn is_iceberg_snapshot_id(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("iceberg:") else {
        return false;
    };
    if rest.len() < 3 || rest.len() > 128 {
        return false;
    }
    let mut bytes = rest.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    first.is_ascii_alphanumeric()
        && bytes
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

fn add_if(blockers: &mut Vec<String>, condition: bool, message: &str) {
    if condition {
        blockers.push(message.to_owned());
    }
}

struct Config {
    root: PathBuf,
    quality_path: PathBuf,
    postgis_path: PathBuf,
    anchor_path: PathBuf,
    output_path: PathBuf,
    cargo_path: Option<PathBuf>,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root = repo_root()?;
        Ok(Self {
            quality_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_POSTGIS_ANCHOR_PBF_REGIONAL_PROOF_SILVER_GOLD_QUALITY_EVIDENCE_PATH",
                    DEFAULT_QUALITY_PATH,
                )?,
                "SilverGoldQualityEvidencePath",
            )?,
            postgis_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_POSTGIS_ANCHOR_PBF_REGIONAL_PROOF_POSTGIS_REBUILD_SUMMARY_PATH",
                    DEFAULT_POSTGIS_PATH,
                )?,
                "PostgisRebuildSummaryPath",
            )?,
            anchor_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_POSTGIS_ANCHOR_PBF_REGIONAL_PROOF_ANCHOR_REBUILD_SUMMARY_PATH",
                    DEFAULT_ANCHOR_PATH,
                )?,
                "AnchorRebuildSummaryPath",
            )?,
            output_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_POSTGIS_ANCHOR_PBF_REGIONAL_PROOF_OUTPUT_PATH",
                    DEFAULT_OUTPUT_PATH,
                )?,
                "OutputPath",
            )?,
            cargo_path: env_optional_path(
                "FOUNDATION_PLATFORM_POSTGIS_ANCHOR_PBF_REGIONAL_PROOF_CARGO_PATH",
            )?,
            root,
        })
    }
}

fn env_optional_path(name: &str) -> anyhow::Result<Option<PathBuf>> {
    Ok(match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Some(PathBuf::from(value)),
        Ok(_) | Err(env::VarError::NotPresent) => None,
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    })
}

fn resolve_repo_path(root: &Path, path: &Path, label: &str) -> anyhow::Result<PathBuf> {
    if path.as_os_str().is_empty() {
        bail!("{label} is required");
    }
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        bail!("{label} must stay within Root");
    }
    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    if !resolved.starts_with(root) {
        bail!("{label} must stay within Root");
    }
    Ok(resolved)
}

fn repo_root() -> anyhow::Result<PathBuf> {
    let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
    let root = fs::canonicalize(&root)
        .with_context(|| format!("failed to resolve repo root {}", root.display()))?;
    Ok(normalize_windows_verbatim_path(root))
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

#[derive(Serialize)]
struct Report {
    schema_version: &'static str,
    generated_at_utc: String,
    git_head: String,
    status: String,
    completion_claim_allowed: bool,
    national_rollout_allowed: bool,
    national_rollout_blocked_reason: &'static str,
    evidence_paths: EvidencePaths,
    #[serde(skip_serializing_if = "Option::is_none")]
    postgis: Option<PostgisSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    anchor: Option<AnchorSummary>,
    pbf_contract_tests: Vec<PbfContractTestResult>,
    blockers: Vec<String>,
    next_gates: Vec<&'static str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    evidence_limitations: Vec<&'static str>,
}

impl Report {
    fn skipped(config: &Config) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            generated_at_utc: utc_now(),
            git_head: git_head(&config.root),
            status: "skipped".to_owned(),
            completion_claim_allowed: false,
            national_rollout_allowed: false,
            national_rollout_blocked_reason: "postgis_anchor_pbf_regional_proof_not_produced",
            evidence_paths: EvidencePaths::new(config),
            postgis: None,
            anchor: None,
            pbf_contract_tests: Vec::new(),
            blockers: vec![
                "postgis/anchor/PBF regional proof evidence has not been produced".to_owned(),
            ],
            next_gates: vec![
                "regional-data-serving-load",
                "explicit-national-rollout-approval",
            ],
            evidence_limitations: Vec::new(),
        }
    }

    fn checked(
        config: &Config,
        status: &str,
        blockers: Vec<String>,
        pbf_contract_tests: Vec<PbfContractTestResult>,
        postgis: PostgisSummary,
        anchor: AnchorSummary,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            generated_at_utc: utc_now(),
            git_head: git_head(&config.root),
            status: status.to_owned(),
            completion_claim_allowed: false,
            national_rollout_allowed: false,
            national_rollout_blocked_reason: "regional_proof_only",
            evidence_paths: EvidencePaths::new(config),
            postgis: Some(postgis),
            anchor: Some(anchor),
            pbf_contract_tests,
            blockers,
            next_gates: vec![
                "regional-data-serving-load",
                "explicit-national-rollout-approval",
            ],
            evidence_limitations: vec![
                "bounded_regional_proof_only",
                "does_not_run_national_collection",
                "does_not_approve_production_cutover",
            ],
        }
    }
}

#[derive(Serialize)]
struct EvidencePaths {
    silver_gold_quality: String,
    postgis_rebuild: String,
    anchor_rebuild: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    output: String,
}

impl EvidencePaths {
    fn new(config: &Config) -> Self {
        Self {
            silver_gold_quality: repo_relative_path(&config.root, &config.quality_path),
            postgis_rebuild: repo_relative_path(&config.root, &config.postgis_path),
            anchor_rebuild: repo_relative_path(&config.root, &config.anchor_path),
            output: repo_relative_path(&config.root, &config.output_path),
        }
    }
}

#[derive(Serialize)]
struct PostgisSummary {
    source_snapshot_id: String,
    source_table: String,
    target_srid: String,
    row_count: i64,
    loaded_row_count: i64,
}

#[derive(Serialize)]
struct AnchorSummary {
    source_snapshot_id: String,
    source_table: String,
    generation_run_id: String,
    algorithm: String,
    algorithm_version: String,
    scanned_row_count: i64,
    loaded_row_count: i64,
    rejected_row_count: i64,
    superseded_row_count: i64,
}

#[derive(Clone, Serialize)]
struct PbfContractTestResult {
    package: String,
    test: String,
    command: String,
    exit_code: i32,
    status: String,
    output: String,
}

#[cfg(test)]
mod tests {
    use super::{
        snapshot_evidence_blockers, SNAPSHOT_ANCHOR_REBUILD, SNAPSHOT_POSTGIS_REBUILD,
        SNAPSHOT_SILVER_GOLD_QUALITY,
    };
    use crate::national_bronze_object_manifest::{
        snapshot_coverage_blockers, SNAPSHOT_NATIONAL_COVERAGE_LEDGER,
    };
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn snapshot_dependent_checkers_are_order_independent() {
        let mut snapshot = HashMap::new();
        snapshot.insert(
            SNAPSHOT_NATIONAL_COVERAGE_LEDGER,
            json!({
                "schema_version": "foundation-platform.national_data_collection_coverage_ledger.v1",
                "status": "ready",
                "completion_claim_allowed": false,
                "production_cutover_allowed": false,
                "national_rollout_allowed": false,
                "coverage": {
                    "failed_job_count": 0,
                    "missing_job_count": 0,
                    "extra_job_count": 0,
                    "duplicate_succeeded_job_count": 0,
                    "raw_response_preserved": true
                }
            }),
        );
        snapshot.insert(
            SNAPSHOT_SILVER_GOLD_QUALITY,
            json!({
                "schema_version": "foundation-platform.silver_gold_data_collection_quality_evidence.v1",
                "status": "ready",
                "completion_claim_allowed": false,
                "national_rollout_allowed": false,
                "summary": {
                    "table_count": 3,
                    "spatial_invalid_geometry_count": 0
                }
            }),
        );
        snapshot.insert(
            SNAPSHOT_POSTGIS_REBUILD,
            json!({
                "schema_version": "foundation-platform.postgis_parcel_boundary_mirror_rebuild_summary.v1",
                "validate_only": false,
                "source_table": "silver.parcel_boundaries",
                "target_srid": "EPSG:5179",
                "source_snapshot_id": "iceberg:test-snapshot",
                "row_count": 1,
                "loaded_row_count": 1
            }),
        );
        snapshot.insert(
            SNAPSHOT_ANCHOR_REBUILD,
            json!({
                "schema_version": "foundation-platform.parcel_marker_anchor_rebuild_summary.v1",
                "source_table": "silver.parcel_boundaries",
                "algorithm": "polylabel",
                "algorithm_version": "postgis-st_maximuminscribedcircle-v1",
                "source_snapshot_id": "iceberg:test-snapshot",
                "scanned_row_count": 1,
                "loaded_row_count": 1,
                "rejected_row_count": 0,
                "superseded_row_count": 0
            }),
        );

        let national_first = snapshot_coverage_blockers(&snapshot);
        let postgis_second = snapshot_evidence_blockers(&snapshot);
        let postgis_first = snapshot_evidence_blockers(&snapshot);
        let national_second = snapshot_coverage_blockers(&snapshot);

        assert_eq!(national_first, national_second);
        assert_eq!(postgis_first, postgis_second);
        assert!(national_first.is_empty(), "{national_first:?}");
        assert!(postgis_first.is_empty(), "{postgis_first:?}");
    }
}
