use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::public_data_control_support::{
    env_path, git_head, read_json, repo_relative_path, resolve_repo_path, utc_now, write_json_file,
};

const SCHEMA_VERSION: &str = "foundation-platform.bounded_live_ingestion_evidence.v1";
const SCOPE_SCHEMA_VERSION: &str = "foundation-platform.bounded_live_ingestion_scope.v1";
const PROOF_SCHEMA_VERSION: &str = "foundation-platform.data_collection_proof_evidence.v1";
const DEFAULT_SCOPE_PATH: &str = "target/audit/bounded-live-ingestion-scope.json";
const DEFAULT_PROOF_PATH: &str = "target/audit/data-collection-proof-evidence.json";
const DEFAULT_OUTPUT_PATH: &str = "target/audit/bounded-live-ingestion-evidence.json";
const HARD_QUOTA_CAP: i64 = 20;

pub fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let scope = read_optional_json(&config.scope_path, "bounded live ingestion scope")?;
    // The data-collection-proof checker writes its own skipped report to the same default
    // path this gate reads as input; a skipped proof means no collection evidence exists
    // yet, so the gate treats it as absent — the parallel checker execution order must not
    // change the verdict.
    let proof = read_optional_json(&config.proof_path, "data collection proof evidence")?
        .filter(|proof| proof.get("status").and_then(JsonValue::as_str) != Some("skipped"));

    if scope.is_none() && proof.is_none() {
        let output = report(
            &config,
            "skipped",
            vec!["bounded live ingestion scope has not been produced".to_owned()],
            None,
            None,
            0,
            HARD_QUOTA_CAP,
        );
        write_json_file(&config.output_path, &output)?;
        println!(
            "bounded-live-ingestion-gate-ok status=skipped report={}",
            repo_relative_path(&config.root, &config.output_path)
        );
        return Ok(());
    }

    let mut blockers = Vec::new();
    if scope.is_none() {
        blockers.push("bounded live ingestion scope missing".to_owned());
    }
    if proof.is_none() {
        blockers.push("data collection proof evidence missing".to_owned());
    }

    let mut planned_request_count = 0;
    let mut quota_cap = HARD_QUOTA_CAP;
    if let Some(proof) = &proof {
        validate_proof(proof, &mut blockers);
    }
    if let Some(scope) = &scope {
        let quota = validate_scope(scope, &mut blockers);
        planned_request_count = quota.planned_request_count;
        quota_cap = quota.quota_cap;
    }

    let status = if blockers.is_empty() {
        "ready"
    } else {
        "blocked"
    };
    let output = report(
        &config,
        status,
        blockers.clone(),
        scope.as_ref(),
        proof.as_ref(),
        planned_request_count,
        quota_cap,
    );
    write_json_file(&config.output_path, &output)?;

    if !blockers.is_empty() {
        println!(
            "bounded-live-ingestion-gate-blocked status={status} blockers={} report={}",
            blockers.len(),
            repo_relative_path(&config.root, &config.output_path)
        );
        for blocker in blockers {
            println!("blocker={blocker}");
        }
        bail!("bounded live ingestion gate blocked");
    }

    println!(
        "bounded-live-ingestion-gate-ok status=ready requests={planned_request_count} cap={quota_cap} report={}",
        repo_relative_path(&config.root, &config.output_path)
    );
    Ok(())
}

struct Config {
    root: PathBuf,
    scope_path: PathBuf,
    proof_path: PathBuf,
    output_path: PathBuf,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root = repo_root()?;
        Ok(Self {
            scope_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_BOUNDED_LIVE_INGESTION_SCOPE_PATH",
                    DEFAULT_SCOPE_PATH,
                )?,
                "ScopePath",
            )?,
            proof_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_BOUNDED_LIVE_INGESTION_PROOF_PATH",
                    DEFAULT_PROOF_PATH,
                )?,
                "ProofPath",
            )?,
            output_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_BOUNDED_LIVE_INGESTION_OUTPUT_PATH",
                    DEFAULT_OUTPUT_PATH,
                )?,
                "OutputPath",
            )?,
            root,
        })
    }
}

fn validate_proof(proof: &JsonValue, blockers: &mut Vec<String>) {
    add_if(
        blockers,
        json_string(proof, "schema_version") != PROOF_SCHEMA_VERSION,
        "data collection proof schema mismatch",
    );
    add_if(
        blockers,
        json_string(proof, "status") != "ready",
        "data collection proof status is not ready",
    );
    add_if(
        blockers,
        json_bool(proof, "completion_claim_allowed", false),
        "data collection proof must not allow completion claims",
    );
    add_if(
        blockers,
        json_bool(proof, "national_rollout_allowed", false),
        "data collection proof national_rollout_allowed must be false",
    );
}

fn validate_scope(scope: &JsonValue, blockers: &mut Vec<String>) -> ScopeQuota {
    add_if(
        blockers,
        json_string(scope, "schema_version") != SCOPE_SCHEMA_VERSION,
        "bounded live ingestion scope schema mismatch",
    );
    add_if(
        blockers,
        json_string(scope, "scope_name").trim().is_empty(),
        "scope_name is required",
    );
    add_if(
        blockers,
        json_bool(scope, "national_rollout_allowed", false),
        "scope national_rollout_allowed must be false",
    );

    let mut planned_request_count = 0;
    if let Some(data_go_kr) = scope.get("data_go_kr") {
        planned_request_count += validate_data_go_kr_scope(data_go_kr, blockers);
    } else {
        blockers.push("data_go_kr scope is required".to_owned());
    }
    if let Some(vworld) = scope.get("vworld") {
        planned_request_count += validate_vworld_scope(vworld, blockers);
    } else {
        blockers.push("vworld scope is required".to_owned());
    }

    let quota_cap = if let Some(quota) = scope.get("quota_cap") {
        let quota_cap = json_i64(quota, "total_public_api_requests", 0);
        add_if(
            blockers,
            quota_cap < 1,
            "quota_cap.total_public_api_requests must be positive",
        );
        add_if(
            blockers,
            quota_cap > HARD_QUOTA_CAP,
            "quota cap exceeds bounded hard cap",
        );
        quota_cap
    } else {
        blockers.push("quota_cap is required".to_owned());
        HARD_QUOTA_CAP
    };

    add_if(
        blockers,
        planned_request_count > quota_cap,
        "planned request count exceeds quota cap",
    );
    add_if(
        blockers,
        planned_request_count > HARD_QUOTA_CAP,
        "planned request count exceeds bounded hard cap",
    );
    ScopeQuota {
        planned_request_count,
        quota_cap,
    }
}

fn validate_data_go_kr_scope(value: &JsonValue, blockers: &mut Vec<String>) -> i64 {
    let operation = json_string(value, "operation");
    let sigungu_cd = json_string(value, "sigungu_cd");
    let bjdong_cd = json_string(value, "bjdong_cd");
    let max_pages = json_i64(value, "max_pages", 0);
    let num_of_rows = json_i64(value, "num_of_rows", 0);

    add_if(
        blockers,
        !is_simple_operation_identifier(&operation),
        "data_go_kr.operation must be a simple API operation identifier",
    );
    add_if(
        blockers,
        !is_fixed_digits(&sigungu_cd, 5),
        "data_go_kr.sigungu_cd must be exactly 5 digits",
    );
    add_if(
        blockers,
        !is_fixed_digits(&bjdong_cd, 5),
        "data_go_kr.bjdong_cd must be exactly 5 digits",
    );
    add_if(
        blockers,
        max_pages < 1,
        "data_go_kr.max_pages must be positive",
    );
    add_if(
        blockers,
        max_pages > HARD_QUOTA_CAP,
        "data_go_kr.max_pages exceeds bounded hard cap",
    );
    add_if(
        blockers,
        num_of_rows < 1,
        "data_go_kr.num_of_rows must be positive",
    );
    add_if(
        blockers,
        num_of_rows > 100,
        "data_go_kr.num_of_rows must stay <= 100 for bounded proof",
    );
    max_pages.max(0)
}

fn validate_vworld_scope(value: &JsonValue, blockers: &mut Vec<String>) -> i64 {
    let endpoint = json_string(value, "smoke_endpoint");
    let max_requests = json_i64(value, "max_requests", 0);
    add_if(
        blockers,
        endpoint != "smoke-vworld-cadastral",
        "vworld.smoke_endpoint must be smoke-vworld-cadastral",
    );
    add_if(
        blockers,
        max_requests < 1,
        "vworld.max_requests must be positive",
    );
    add_if(
        blockers,
        max_requests > 10,
        "vworld.max_requests must stay <= 10 for bounded proof",
    );
    max_requests.max(0)
}

fn report(
    config: &Config,
    status: &str,
    blockers: Vec<String>,
    scope: Option<&JsonValue>,
    proof: Option<&JsonValue>,
    planned_request_count: i64,
    quota_cap: i64,
) -> Evidence {
    Evidence {
        schema_version: SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        git_head: git_head(&config.root),
        status: status.to_owned(),
        completion_claim_allowed: false,
        national_rollout_allowed: false,
        national_rollout_blocked_reason: "bounded_scope_only",
        evidence_paths: EvidencePaths {
            scope: repo_relative_path(&config.root, &config.scope_path),
            prior_proof: repo_relative_path(&config.root, &config.proof_path),
        },
        scope: ScopeSummary {
            scope_name: scope
                .map(|scope| json_string(scope, "scope_name"))
                .unwrap_or_default(),
        },
        prior_proof: PriorProofSummary {
            status: proof
                .map(|proof| json_string(proof, "status"))
                .unwrap_or_else(|| "missing".to_owned()),
        },
        quota: QuotaSummary {
            planned_request_count,
            cap: quota_cap,
            hard_cap: HARD_QUOTA_CAP,
        },
        blockers,
        next_gates: vec![
            "bronze-public-data-ingestion",
            "silver-gold-data-collection-quality",
            "postgis-anchor-pbf-regional-proof",
            "regional-data-serving-load",
            "explicit-national-rollout-approval",
        ],
        evidence_limitations: vec![
            "does_not_run_national_collection",
            "does_not_write_bronze_silver_gold",
            "does_not_fetch_more_than_bounded_scope",
            "does_not_allow_runtime_user_request_public_api_dependency",
        ],
    }
}

fn read_optional_json(path: &Path, label: &str) -> anyhow::Result<Option<JsonValue>> {
    if path.is_file() {
        Ok(Some(read_json(path, label)?))
    } else {
        Ok(None)
    }
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

fn json_i64(value: &JsonValue, field: &str, default: i64) -> i64 {
    value
        .get(field)
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|value| value.try_into().ok()))
                .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        })
        .unwrap_or(default)
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

fn is_fixed_digits(value: &str, expected_len: usize) -> bool {
    value.len() == expected_len && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_simple_operation_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    first.is_ascii_alphabetic() && bytes.all(|byte| byte.is_ascii_alphanumeric())
}

fn add_if(blockers: &mut Vec<String>, condition: bool, message: &str) {
    if condition {
        blockers.push(message.to_owned());
    }
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

#[derive(Clone, Copy)]
struct ScopeQuota {
    planned_request_count: i64,
    quota_cap: i64,
}

#[derive(Serialize)]
struct Evidence {
    schema_version: &'static str,
    generated_at_utc: String,
    git_head: String,
    status: String,
    completion_claim_allowed: bool,
    national_rollout_allowed: bool,
    national_rollout_blocked_reason: &'static str,
    evidence_paths: EvidencePaths,
    scope: ScopeSummary,
    prior_proof: PriorProofSummary,
    quota: QuotaSummary,
    blockers: Vec<String>,
    next_gates: Vec<&'static str>,
    evidence_limitations: Vec<&'static str>,
}

#[derive(Serialize)]
struct EvidencePaths {
    scope: String,
    prior_proof: String,
}

#[derive(Serialize)]
struct ScopeSummary {
    scope_name: String,
}

#[derive(Serialize)]
struct PriorProofSummary {
    status: String,
}

#[derive(Serialize)]
struct QuotaSummary {
    planned_request_count: i64,
    cap: i64,
    hard_cap: i64,
}
