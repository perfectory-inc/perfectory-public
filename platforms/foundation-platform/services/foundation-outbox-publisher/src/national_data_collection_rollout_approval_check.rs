use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::public_data_control_support::{
    env_path, git_head, optional_env_value, read_json, repo_relative_path, resolve_repo_path,
    utc_now, write_json_file,
};

const SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_rollout_approval.v1";
const DEFAULT_APPROVAL_PATH: &str = "target/audit/national-data-collection-rollout-approval.json";
// The checker's report MUST NOT default to the approval artifact path: writing the report there
// would overwrite (fabricate/destroy) the operator's recorded approval. Keep them distinct.
const DEFAULT_CHECK_OUTPUT_PATH: &str =
    "target/audit/national-data-collection-rollout-approval-check.json";
const PRIOR_EVIDENCE: &[PriorEvidenceSpec] = &[
    PriorEvidenceSpec {
        id: "tiny-live-readonly-proof",
        path: "target/audit/data-collection-proof-evidence.json",
        schema_version: "foundation-platform.data_collection_proof_evidence.v1",
    },
    PriorEvidenceSpec {
        id: "bounded-live-ingestion",
        path: "target/audit/bounded-live-ingestion-evidence.json",
        schema_version: "foundation-platform.bounded_live_ingestion_evidence.v1",
    },
    PriorEvidenceSpec {
        id: "bronze-raw-preservation",
        path: "target/audit/bronze-public-data-ingestion-evidence.json",
        schema_version: "foundation-platform.bronze_public_data_ingestion_evidence.v1",
    },
    PriorEvidenceSpec {
        id: "silver-gold-quality",
        path: "target/audit/silver-gold-data-collection-quality-evidence.json",
        schema_version: "foundation-platform.silver_gold_data_collection_quality_evidence.v1",
    },
    PriorEvidenceSpec {
        id: "postgis-anchor-pbf-regional-proof",
        path: "target/audit/postgis-anchor-pbf-regional-proof.json",
        schema_version: "foundation-platform.postgis_anchor_pbf_regional_proof.v1",
    },
    PriorEvidenceSpec {
        id: "regional-data-serving-load",
        path: "target/audit/regional-data-serving-load-evidence.json",
        schema_version: "foundation-platform.regional_data_serving_load_evidence.v1",
    },
];

pub fn run() -> anyhow::Result<()> {
    let config = CheckConfig::from_env()?;
    let approval_exists = config.approval_path.is_file();
    let existing_prior_count = PRIOR_EVIDENCE
        .iter()
        .map(|spec| resolve_repo_path(&config.root, &PathBuf::from(spec.path), spec.id))
        .collect::<anyhow::Result<Vec<_>>>()?
        .into_iter()
        .filter(|path| path.is_file())
        .count();

    if !approval_exists && existing_prior_count == 0 {
        let report = ApprovalReport::skipped(&config.root);
        write_json_file(&config.output_path, &report)?;
        println!(
            "national-data-collection-rollout-approval-ok status=skipped report={}",
            repo_relative_path(&config.root, &config.output_path)
        );
        return Ok(());
    }

    let mut blockers = Vec::new();
    let prior_reports = validate_prior_evidence(&config.root, &mut blockers)?;
    let approval = if approval_exists {
        Some(read_json(
            &config.approval_path,
            "national data collection rollout approval",
        )?)
    } else {
        blockers.push("national rollout approval artifact missing".to_owned());
        None
    };
    if let Some(approval) = &approval {
        validate_approval(approval, &mut blockers);
    }

    let national_allowed = blockers.is_empty();
    let report = ApprovalReport::from_parts(
        &config.root,
        approval.as_ref(),
        prior_reports,
        blockers,
        national_allowed,
    );
    write_json_file(&config.output_path, &report)?;

    if report.status != "ready" {
        println!(
            "national-data-collection-rollout-approval-blocked status={} blockers={} report={}",
            report.status,
            report.blockers.len(),
            repo_relative_path(&config.root, &config.output_path)
        );
        for blocker in &report.blockers {
            println!("blocker={blocker}");
        }
        if config.fail_on_blocked {
            bail!("national data collection rollout approval blocked");
        }
        return Ok(());
    }

    println!(
        "national-data-collection-rollout-approval-ok status=ready approved_scope=national prior_evidence={} report={}",
        report.prior_evidence.len(),
        repo_relative_path(&config.root, &config.output_path)
    );
    Ok(())
}

pub fn write() -> anyhow::Result<()> {
    let config = WriteConfig::from_env()?;
    if !config.confirm {
        bail!(
            "ConfirmNationalRolloutApproval is required before writing national rollout approval"
        );
    }
    if config.approved_by.trim().is_empty() {
        bail!("ApprovedBy is required");
    }
    if config.operator_instruction.trim().is_empty() {
        bail!("OperatorInstruction is required");
    }
    if !config.quota_plan_reviewed {
        bail!("QuotaPlanReviewed is required");
    }
    if !config.rollback_plan_reviewed {
        bail!("RollbackPlanReviewed is required");
    }
    if config.output_path.is_file() {
        bail!(
            "national rollout approval artifact already exists: {}",
            repo_relative_path(&config.root, &config.output_path)
        );
    }

    let now = utc_now();
    let report = serde_json::json!({
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": now.clone(),
        "git_head": git_head(&config.root),
        "approved": true,
        "approved_at_utc": now,
        "approved_by": config.approved_by,
        "approved_scope": "national",
        "operator_instruction": config.operator_instruction,
        "quota_plan_reviewed": config.quota_plan_reviewed,
        "rollback_plan_reviewed": config.rollback_plan_reviewed,
        "completion_claim_allowed": false,
        "national_rollout_allowed": true,
        "national_rollout_allowed_reason": "all_local_pre_national_gates_ready_and_operator_approval_recorded",
        "evidence_limitations": [
            "approval_only",
            "does_not_execute_national_collection",
            "does_not_approve_production_cutover",
            "does_not_prove_aws_runtime",
        ],
        "next_gates": [
            "national-data-collection-run-evidence",
            "production-cutover-artifacts",
        ],
    });
    write_json_file(&config.output_path, &report)?;
    println!(
        "national-data-collection-rollout-approval-written path={}",
        repo_relative_path(&config.root, &config.output_path)
    );
    Ok(())
}

struct CheckConfig {
    root: PathBuf,
    approval_path: PathBuf,
    output_path: PathBuf,
    fail_on_blocked: bool,
}

struct WriteConfig {
    root: PathBuf,
    output_path: PathBuf,
    approved_by: String,
    operator_instruction: String,
    quota_plan_reviewed: bool,
    rollback_plan_reviewed: bool,
    confirm: bool,
}

impl WriteConfig {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = fs::canonicalize(&root)
            .with_context(|| format!("failed to resolve repo root {}", root.display()))?;
        Ok(Self {
            output_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_ROLLOUT_APPROVAL_WRITE_OUTPUT_PATH",
                    DEFAULT_APPROVAL_PATH,
                )?,
                "OutputPath",
            )?,
            approved_by: optional_env_value(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_ROLLOUT_APPROVAL_APPROVED_BY",
            )?
            .unwrap_or_default(),
            operator_instruction: optional_env_value(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_ROLLOUT_APPROVAL_OPERATOR_INSTRUCTION",
            )?
            .unwrap_or_default(),
            quota_plan_reviewed: env_bool(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_ROLLOUT_APPROVAL_QUOTA_PLAN_REVIEWED",
                false,
            )?,
            rollback_plan_reviewed: env_bool(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_ROLLOUT_APPROVAL_ROLLBACK_PLAN_REVIEWED",
                false,
            )?,
            confirm: env_bool(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_ROLLOUT_APPROVAL_CONFIRM",
                false,
            )?,
            root,
        })
    }
}

impl CheckConfig {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = fs::canonicalize(&root)
            .with_context(|| format!("failed to resolve repo root {}", root.display()))?;
        let config = Self {
            approval_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_ROLLOUT_APPROVAL_PATH",
                    DEFAULT_APPROVAL_PATH,
                )?,
                "ApprovalPath",
            )?,
            output_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_ROLLOUT_APPROVAL_OUTPUT_PATH",
                    DEFAULT_CHECK_OUTPUT_PATH,
                )?,
                "OutputPath",
            )?,
            fail_on_blocked: env_bool(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_ROLLOUT_APPROVAL_FAIL_ON_BLOCKED",
                false,
            )?,
            root,
        };
        // Guard against an env override that re-collides the two: the checker reads the
        // approval artifact and must never write its report over it.
        if config.approval_path == config.output_path {
            bail!(
                "rollout approval check output path must differ from the approval artifact path \
                 ({}); writing the report there would overwrite the operator approval",
                repo_relative_path(&config.root, &config.approval_path)
            );
        }
        Ok(config)
    }
}

fn validate_prior_evidence(
    root: &Path,
    blockers: &mut Vec<String>,
) -> anyhow::Result<Vec<PriorEvidenceReport>> {
    let mut reports = Vec::new();
    for spec in PRIOR_EVIDENCE {
        let path = resolve_repo_path(root, &PathBuf::from(spec.path), spec.id)?;
        if !path.is_file() {
            blockers.push(format!("prior evidence missing: {}", spec.path));
            reports.push(PriorEvidenceReport {
                id: spec.id.to_owned(),
                path: repo_relative_path(root, &path),
                status: "missing".to_owned(),
                national_rollout_allowed: None,
            });
            continue;
        }

        let json = read_json(&path, "prior national rollout evidence")?;
        add_if(
            blockers,
            string_property(&json, "schema_version") != spec.schema_version,
            &format!("prior evidence schema mismatch: {}", spec.id),
        );
        add_if(
            blockers,
            string_property(&json, "status") != "ready",
            &format!("prior evidence status must be ready: {}", spec.id),
        );
        add_if(
            blockers,
            bool_property(&json, "completion_claim_allowed", false),
            &format!(
                "prior evidence must not allow completion claims: {}",
                spec.id
            ),
        );
        add_if(
            blockers,
            bool_property(&json, "national_rollout_allowed", false),
            &format!(
                "prior evidence must not already allow national rollout: {}",
                spec.id
            ),
        );
        reports.push(PriorEvidenceReport {
            id: spec.id.to_owned(),
            path: repo_relative_path(root, &path),
            status: string_property(&json, "status"),
            national_rollout_allowed: Some(bool_property(&json, "national_rollout_allowed", false)),
        });
    }
    Ok(reports)
}

fn validate_approval(approval: &JsonValue, blockers: &mut Vec<String>) {
    add_if(
        blockers,
        string_property(approval, "schema_version") != SCHEMA_VERSION,
        "approval schema mismatch",
    );
    add_if(
        blockers,
        !bool_property(approval, "approved", false),
        "approved must be true",
    );
    add_if(
        blockers,
        string_property(approval, "approved_scope") != "national",
        "approved_scope must be national",
    );
    add_if(
        blockers,
        !bool_property(approval, "quota_plan_reviewed", false),
        "quota_plan_reviewed must be true",
    );
    add_if(
        blockers,
        !bool_property(approval, "rollback_plan_reviewed", false),
        "rollback_plan_reviewed must be true",
    );
    add_if(
        blockers,
        string_property(approval, "approved_by").trim().is_empty(),
        "approved_by must be nonblank",
    );
    add_if(
        blockers,
        string_property(approval, "operator_instruction")
            .trim()
            .is_empty(),
        "operator_instruction must be nonblank",
    );
    add_if(
        blockers,
        !utc_timestamp_not_future(&string_property(approval, "approved_at_utc")),
        "approved_at_utc must be a non-future UTC timestamp",
    );
}

fn utc_timestamp_not_future(value: &str) -> bool {
    let Ok(parsed) = DateTime::parse_from_rfc3339(value.trim()) else {
        return false;
    };
    parsed.offset().local_minus_utc() == 0
        && parsed.with_timezone(&Utc) <= Utc::now() + Duration::minutes(1)
}

fn normalize_utc_timestamp(value: &str) -> String {
    DateTime::parse_from_rfc3339(value.trim())
        .ok()
        .map(|parsed| {
            parsed
                .with_timezone(&Utc)
                .to_rfc3339_opts(SecondsFormat::Nanos, true)
        })
        .unwrap_or_else(|| value.to_owned())
}

fn env_bool(name: &str, default: bool) -> anyhow::Result<bool> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(default),
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => bail!("invalid {name} environment variable"),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn add_if(blockers: &mut Vec<String>, condition: bool, message: &str) {
    if condition {
        blockers.push(message.to_owned());
    }
}

fn string_property(value: &JsonValue, name: &str) -> String {
    value
        .get(name)
        .map(|property| match property {
            JsonValue::String(text) => text.clone(),
            JsonValue::Null => String::new(),
            JsonValue::Bool(flag) => flag.to_string(),
            JsonValue::Number(number) => number.to_string(),
            other => other.to_string(),
        })
        .unwrap_or_default()
}

fn bool_property(value: &JsonValue, name: &str, default: bool) -> bool {
    value
        .get(name)
        .and_then(|property| match property {
            JsonValue::Bool(flag) => Some(*flag),
            JsonValue::String(text) => text.trim().parse::<bool>().ok(),
            _ => None,
        })
        .unwrap_or(default)
}

struct PriorEvidenceSpec {
    id: &'static str,
    path: &'static str,
    schema_version: &'static str,
}

#[derive(Serialize)]
struct ApprovalReport {
    schema_version: &'static str,
    generated_at_utc: String,
    git_head: String,
    status: &'static str,
    approved: bool,
    approved_at_utc: String,
    approved_by: String,
    approved_scope: String,
    operator_instruction: String,
    quota_plan_reviewed: bool,
    rollback_plan_reviewed: bool,
    completion_claim_allowed: bool,
    national_rollout_allowed: bool,
    national_rollout_blocked_reason: String,
    prior_evidence: Vec<PriorEvidenceReport>,
    blockers: Vec<String>,
    evidence_limitations: Vec<String>,
    next_gates: Vec<String>,
}

impl ApprovalReport {
    fn skipped(root: &Path) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            generated_at_utc: utc_now(),
            git_head: git_head(root),
            status: "skipped",
            approved: false,
            approved_at_utc: String::new(),
            approved_by: String::new(),
            approved_scope: String::new(),
            operator_instruction: String::new(),
            quota_plan_reviewed: false,
            rollback_plan_reviewed: false,
            completion_claim_allowed: false,
            national_rollout_allowed: false,
            national_rollout_blocked_reason: "prior_evidence_and_approval_not_produced".to_owned(),
            prior_evidence: Vec::new(),
            blockers: vec![
                "prior evidence and national rollout approval have not been produced".to_owned(),
            ],
            evidence_limitations: Vec::new(),
            next_gates: vec!["explicit-national-rollout-approval".to_owned()],
        }
    }

    fn from_parts(
        root: &Path,
        approval: Option<&JsonValue>,
        prior_evidence: Vec<PriorEvidenceReport>,
        blockers: Vec<String>,
        national_allowed: bool,
    ) -> Self {
        let approved_at_utc = approval
            .map(|approval| normalize_utc_timestamp(&string_property(approval, "approved_at_utc")))
            .unwrap_or_default();
        let status = if national_allowed { "ready" } else { "blocked" };
        Self {
            schema_version: SCHEMA_VERSION,
            generated_at_utc: utc_now(),
            git_head: git_head(root),
            status,
            approved: national_allowed,
            approved_at_utc,
            approved_by: approval
                .map(|approval| string_property(approval, "approved_by"))
                .unwrap_or_default(),
            approved_scope: approval
                .map(|approval| string_property(approval, "approved_scope"))
                .unwrap_or_default(),
            operator_instruction: approval
                .map(|approval| string_property(approval, "operator_instruction"))
                .unwrap_or_default(),
            quota_plan_reviewed: approval
                .map(|approval| bool_property(approval, "quota_plan_reviewed", false))
                .unwrap_or_default(),
            rollback_plan_reviewed: approval
                .map(|approval| bool_property(approval, "rollback_plan_reviewed", false))
                .unwrap_or_default(),
            completion_claim_allowed: false,
            national_rollout_allowed: national_allowed,
            national_rollout_blocked_reason: if national_allowed {
                String::new()
            } else {
                "approval_or_prior_evidence_blocked".to_owned()
            },
            prior_evidence,
            blockers,
            evidence_limitations: vec![
                "approval_only".to_owned(),
                "does_not_execute_national_collection".to_owned(),
                "does_not_approve_production_cutover".to_owned(),
                "does_not_prove_aws_runtime".to_owned(),
            ],
            next_gates: if national_allowed {
                vec!["national-data-collection-run-evidence".to_owned()]
            } else {
                vec!["explicit-national-rollout-approval".to_owned()]
            },
        }
    }
}

#[derive(Serialize)]
struct PriorEvidenceReport {
    id: String,
    path: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    national_rollout_allowed: Option<bool>,
}
