use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
};

use anyhow::{bail, Context};
use serde_json::{json, Value as JsonValue};

use crate::public_data_control_support::{
    env_path, git_head, optional_env_value, read_json, repo_relative_path, resolve_cargo_exe,
    resolve_repo_path, utc_now, write_json_file,
};

const SCHEMA_VERSION: &str = "foundation-platform.silver_gold_national_promotion_execution.v1";
const PLAN_SCHEMA_VERSION: &str = "foundation-platform.silver_gold_national_promotion_plan.v1";
const SHARD_SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.vworld_cadastral_silver_handoff_shard_export.v1";
const DEFAULT_PLAN_PATH: &str = "target/audit/silver-gold-national-promotion-plan.json";
const DEFAULT_MANIFEST_PATH: &str = "target/audit/national-bronze-object-manifest.jsonl";
const DEFAULT_EVIDENCE_PATH: &str = "target/audit/silver-gold-national-promotion-execution.json";
const DEFAULT_CHECK_REPORT_PATH: &str =
    "target/audit/silver-gold-national-promotion-execution-check.json";
const DEFAULT_OUTPUT_ROOT: &str = "target/lakehouse/national-promotion/silver-handoff";
const DEFAULT_OUTPUT_R2_PREFIX: &str = "silver-handoff/national-promotion/vworld-cadastral";
const DEFAULT_SHARD_SUMMARY_ROOT: &str = "target/audit/silver-gold-national-promotion-execution";
const DEFAULT_BRONZE_LOCAL_ROOT: &str = "target/bronze-national-data-collection-juso-202604";
const FORBIDDEN_TOKENS: &[&str] = &[
    concat!("DATA_GO_KR", "_SERVICE_KEY"),
    concat!("VWORLD", "_API_KEY"),
    concat!("service", "Key"),
    concat!("raw", "_payload"),
    concat!("unit", "-test-key"),
    concat!("fake", "-vworld-key"),
];

pub fn run_check() -> anyhow::Result<()> {
    let config = CheckConfig::from_env()?;
    run_check_with_config(&config)
}

pub fn run_execute() -> anyhow::Result<()> {
    let mut config = ExecuteConfig::from_env()?;
    if config.max_shards < 1 {
        bail!("MaxShards must be positive");
    }
    if config.skip_shards < 0 {
        bail!("SkipShards must be non-negative");
    }
    if config.execute && !config.confirm {
        bail!("Pass -ConfirmNationalPromotionExecution with -Execute");
    }
    config.env_overrides = import_dot_env_file(&config.env_file)?;
    if config.bronze_storage_driver == "r2" {
        assert_r2_env_present(&config)?;
    }
    if config.output_storage_driver == "r2" {
        assert_r2_env_present(&config)?;
        config.output_r2_prefix =
            safe_object_key_prefix(&config.output_r2_prefix, "OutputR2Prefix")?;
    }
    if !matches!(config.bronze_storage_driver.as_str(), "local" | "r2") {
        bail!("BronzeStorageDriver must be local or r2");
    }
    if !matches!(config.output_storage_driver.as_str(), "local" | "r2") {
        bail!("OutputStorageDriver must be local or r2");
    }
    if !config.plan_path.is_file() {
        bail!("promotion plan missing: {}", config.plan_path.display());
    }
    if !config.manifest_path.is_file() {
        bail!(
            "Bronze object manifest missing: {}",
            config.manifest_path.display()
        );
    }

    let plan = read_json(&config.plan_path, "silver/gold national promotion plan")?;
    if string_property(&plan, "schema_version") != PLAN_SCHEMA_VERSION {
        bail!("promotion plan schema mismatch");
    }
    if string_property(&plan, "status") != "ready" {
        bail!("promotion plan status must be ready");
    }
    if string_property(&plan, "execution_model") != "manifest_filtered_streaming" {
        bail!("promotion plan execution_model must be manifest_filtered_streaming");
    }
    let shards = select_shards(
        &json_array(plan.get("shards").unwrap_or(&JsonValue::Null)),
        &config,
    );
    if shards.is_empty() {
        bail!("no promotion shards selected");
    }

    let runner = Runner::from_config(&config)?;
    let mut shard_results = Vec::new();
    let mut succeeded = 0_i64;
    let mut failed = 0_i64;
    let mut selected_object_count = 0_i64;
    let mut output_row_count = 0_i64;
    let mut input_bytes = 0_i64;

    for shard in &shards {
        let shard_id = string_property(shard, "shard_id");
        let output_path = if config.output_storage_driver == "local" {
            config.output_root.join(format!("{shard_id}.jsonl"))
        } else {
            PathBuf::new()
        };
        let output_object_key = if config.output_storage_driver == "r2" {
            format!("{}/{}.jsonl", config.output_r2_prefix, shard_id)
        } else {
            String::new()
        };
        let summary_path = config.shard_summary_root.join(format!("{shard_id}.json"));
        selected_object_count += long_property(shard, "object_count");

        if !config.execute {
            shard_results.push(planned_shard_result(
                &config,
                shard,
                &output_path,
                &output_object_key,
                &summary_path,
            ));
            continue;
        }

        let run = runner.run(
            &config,
            shard,
            &output_path,
            &output_object_key,
            &summary_path,
        )?;
        if !run.status_success {
            failed += 1;
            shard_results.push(failed_shard_result(
                &config,
                shard,
                &output_path,
                &output_object_key,
                &summary_path,
                run.exit_code,
                &run.output,
            ));
            continue;
        }
        if !summary_path.is_file() {
            bail!(
                "runner did not produce shard summary: {}",
                summary_path.display()
            );
        }
        let summary = read_json(&summary_path, "silver handoff shard summary")?;
        let summary_source = summary.get("source").unwrap_or(&JsonValue::Null);
        let summary_output = summary.get("output").unwrap_or(&JsonValue::Null);
        let row_count = long_property(summary_output, "row_count");
        let shard_input_bytes = long_property(summary_source, "input_bytes");
        input_bytes += shard_input_bytes;
        output_row_count += row_count;
        succeeded += 1;
        shard_results.push(succeeded_shard_result(
            &config,
            shard,
            &output_path,
            &output_object_key,
            &summary_path,
            long_property(summary_source, "selected_object_count"),
            shard_input_bytes,
            row_count,
        ));
    }

    let status = if !config.execute {
        "planned"
    } else if failed == 0 {
        "ready"
    } else {
        "blocked"
    };
    let evidence = json!({
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now(),
        "git_head": git_head(&config.root),
        "status": status,
        "executed": config.execute,
        "execution_model": "manifest_filtered_streaming",
        "storage_driver": config.bronze_storage_driver,
        "output_storage_driver": config.output_storage_driver,
        "promotion_plan_path": repo_relative_path(&config.root, &config.plan_path),
        "bronze_object_manifest": repo_relative_path(&config.root, &config.manifest_path),
        "summary": {
            "selected_shard_count": shards.len(),
            "succeeded_shard_count": succeeded,
            "failed_shard_count": failed,
            "selected_object_count": selected_object_count,
            "input_bytes": input_bytes,
            "output_row_count": output_row_count,
        },
        "shard_results": shard_results,
        "full_promotion_allowed": false,
        "completion_claim_allowed": false,
        "production_cutover_allowed": false,
        "national_rollout_allowed": false,
        "evidence_limitations": [
            "vworld_cadastral_shard_handoff_only",
            "does_not_write_iceberg_table",
            "does_not_promote_gold_tables",
            "does_not_rebuild_postgis_anchor_or_pbf",
            "data_go_kr_building_register_deferred",
        ],
        "next_gates": [
            "silver-parcel-boundaries-iceberg-write",
            "postgis-anchor-pbf-national-rebuild",
        ],
    });
    write_json_file(&config.evidence_path, &evidence)?;
    let check_config = CheckConfig {
        root: config.root,
        evidence_path: config.evidence_path,
        report_path: config.check_report_path,
    };
    run_check_with_config(&check_config)
}

struct CheckConfig {
    root: PathBuf,
    evidence_path: PathBuf,
    report_path: PathBuf,
}

impl CheckConfig {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = fs::canonicalize(&root)
            .with_context(|| format!("failed to resolve repo root {}", root.display()))?;
        Ok(Self {
            evidence_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_EXECUTION_EVIDENCE_PATH",
                    DEFAULT_EVIDENCE_PATH,
                )?,
                "EvidencePath",
            )?,
            report_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_EXECUTION_REPORT_PATH",
                    DEFAULT_CHECK_REPORT_PATH,
                )?,
                "ReportPath",
            )?,
            root,
        })
    }
}

struct ExecuteConfig {
    root: PathBuf,
    env_file: PathBuf,
    plan_path: PathBuf,
    manifest_path: PathBuf,
    evidence_path: PathBuf,
    check_report_path: PathBuf,
    output_root: PathBuf,
    output_storage_driver: String,
    output_r2_prefix: String,
    shard_summary_root: PathBuf,
    shard_id: String,
    max_shards: i64,
    skip_shards: i64,
    bronze_storage_driver: String,
    bronze_local_root: PathBuf,
    cargo_exe: Option<PathBuf>,
    runner_exe: Option<PathBuf>,
    execute: bool,
    confirm: bool,
    env_overrides: HashMap<String, String>,
}

impl ExecuteConfig {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = fs::canonicalize(&root)
            .with_context(|| format!("failed to resolve repo root {}", root.display()))?;
        let env_file = env_path(
            "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_ENV_FILE",
            ".env.local",
        )?;
        Ok(Self {
            env_file: resolve_optional_input_path(&root, &env_file),
            plan_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_EXECUTION_PLAN_PATH",
                    DEFAULT_PLAN_PATH,
                )?,
                "PlanPath",
            )?,
            manifest_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_EXECUTION_MANIFEST_PATH",
                    DEFAULT_MANIFEST_PATH,
                )?,
                "BronzeObjectManifestPath",
            )?,
            evidence_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_EXECUTION_EVIDENCE_PATH",
                    DEFAULT_EVIDENCE_PATH,
                )?,
                "EvidencePath",
            )?,
            check_report_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_EXECUTION_REPORT_PATH",
                    DEFAULT_CHECK_REPORT_PATH,
                )?,
                "CheckReportPath",
            )?,
            output_root: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_OUTPUT_ROOT",
                    DEFAULT_OUTPUT_ROOT,
                )?,
                "OutputRoot",
            )?,
            output_storage_driver: env_string(
                "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_OUTPUT_STORAGE_DRIVER",
                "local",
            )?,
            output_r2_prefix: env_string(
                "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_OUTPUT_R2_PREFIX",
                DEFAULT_OUTPUT_R2_PREFIX,
            )?,
            shard_summary_root: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_SHARD_SUMMARY_ROOT",
                    DEFAULT_SHARD_SUMMARY_ROOT,
                )?,
                "ShardSummaryRoot",
            )?,
            shard_id: env_string(
                "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_SHARD_ID",
                "",
            )?,
            max_shards: env_i64(
                "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_MAX_SHARDS",
                1,
            )?,
            skip_shards: env_i64(
                "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_SKIP_SHARDS",
                0,
            )?,
            bronze_storage_driver: env_string(
                "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_BRONZE_STORAGE_DRIVER",
                "r2",
            )?,
            bronze_local_root: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_BRONZE_LOCAL_ROOT",
                    DEFAULT_BRONZE_LOCAL_ROOT,
                )?,
                "BronzeLocalRoot",
            )?,
            cargo_exe: optional_env_value(
                "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_CARGO_EXE",
            )?
            .map(PathBuf::from),
            runner_exe: optional_env_value(
                "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_RUNNER_EXE",
            )?
            .map(PathBuf::from),
            execute: env_bool(
                "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_EXECUTE",
                false,
            )?,
            confirm: env_bool(
                "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_CONFIRM",
                false,
            )?,
            env_overrides: HashMap::new(),
            root,
        })
    }

    fn env_value(&self, name: &str) -> Option<String> {
        self.env_overrides
            .get(name)
            .cloned()
            .or_else(|| std::env::var(name).ok())
            .filter(|value| !value.trim().is_empty())
    }
}

fn run_check_with_config(config: &CheckConfig) -> anyhow::Result<()> {
    if !config.evidence_path.is_file() {
        let report = json!({
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": utc_now(),
            "git_head": git_head(&config.root),
            "status": "skipped",
            "evidence_path": repo_relative_path(&config.root, &config.evidence_path),
            "completion_claim_allowed": false,
            "production_cutover_allowed": false,
            "national_rollout_allowed": false,
            "blockers": ["silver/gold national promotion execution evidence has not been produced"],
            "next_gates": ["silver-gold-national-promotion-executor"],
        });
        write_json_file(&config.report_path, &report)?;
        println!(
            "silver-gold-national-promotion-execution-ok status=skipped report={}",
            config.report_path.display()
        );
        return Ok(());
    }

    let evidence = read_json(
        &config.evidence_path,
        "silver/gold national promotion execution evidence",
    )?;
    let mut blockers = Vec::new();
    add_forbidden_token_blockers(
        &config.evidence_path,
        "promotion execution evidence",
        &mut blockers,
    )?;
    validate_evidence_top_level(&evidence, &mut blockers);

    let summary = evidence.get("summary").unwrap_or(&JsonValue::Null);
    let selected_shard_count = long_property(summary, "selected_shard_count");
    let succeeded_shard_count = long_property(summary, "succeeded_shard_count");
    let failed_shard_count = long_property(summary, "failed_shard_count");
    let selected_object_count = long_property(summary, "selected_object_count");
    let output_row_count = long_property(summary, "output_row_count");
    let status = string_property(&evidence, "status");
    validate_evidence_summary(
        &status,
        selected_shard_count,
        succeeded_shard_count,
        failed_shard_count,
        selected_object_count,
        output_row_count,
        &mut blockers,
    );
    let results = json_array(evidence.get("shard_results").unwrap_or(&JsonValue::Null));
    add_if(
        &mut blockers,
        results.len() as i64 != selected_shard_count,
        "shard_results count must match selected_shard_count",
    );
    for result in &results {
        validate_shard_result(config, result, &mut blockers)?;
    }

    let report_status = if blockers.is_empty() {
        status.as_str()
    } else {
        "blocked"
    };
    let report = json!({
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now(),
        "git_head": git_head(&config.root),
        "status": report_status,
        "evidence_path": repo_relative_path(&config.root, &config.evidence_path),
        "summary": {
            "selected_shard_count": selected_shard_count,
            "succeeded_shard_count": succeeded_shard_count,
            "failed_shard_count": failed_shard_count,
            "selected_object_count": selected_object_count,
            "output_row_count": output_row_count,
        },
        "completion_claim_allowed": false,
        "production_cutover_allowed": false,
        "national_rollout_allowed": false,
        "blockers": blockers,
        "next_gates": [
            "silver-parcel-boundaries-iceberg-write",
            "postgis-anchor-pbf-national-rebuild",
        ],
    });
    write_json_file(&config.report_path, &report)?;
    let blockers = report
        .get("blockers")
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();
    if !blockers.is_empty() {
        println!(
            "silver-gold-national-promotion-execution-blocked status=blocked blockers={} report={}",
            blockers.len(),
            config.report_path.display()
        );
        for blocker in blockers {
            println!("blocker={}", json_to_string(&blocker));
        }
        bail!("silver/gold national promotion execution blocked");
    }

    println!(
        "silver-gold-national-promotion-execution-ok status={report_status} shards={selected_shard_count} objects={selected_object_count} rows={output_row_count} report={}",
        config.report_path.display()
    );
    Ok(())
}

fn validate_evidence_top_level(evidence: &JsonValue, blockers: &mut Vec<String>) {
    let status = string_property(evidence, "status");
    add_if(
        blockers,
        string_property(evidence, "schema_version") != SCHEMA_VERSION,
        "execution evidence schema mismatch",
    );
    add_if(
        blockers,
        !matches!(status.as_str(), "planned" | "ready" | "blocked"),
        "execution evidence status invalid",
    );
    add_if(
        blockers,
        bool_property(evidence, "completion_claim_allowed", true),
        "completion_claim_allowed must be false",
    );
    add_if(
        blockers,
        bool_property(evidence, "production_cutover_allowed", true),
        "production_cutover_allowed must be false",
    );
    add_if(
        blockers,
        bool_property(evidence, "national_rollout_allowed", true),
        "national_rollout_allowed must be false",
    );
}

fn validate_evidence_summary(
    status: &str,
    selected_shard_count: i64,
    succeeded_shard_count: i64,
    failed_shard_count: i64,
    selected_object_count: i64,
    output_row_count: i64,
    blockers: &mut Vec<String>,
) {
    add_if(
        blockers,
        selected_shard_count < 1,
        "selected_shard_count must be positive",
    );
    add_if(
        blockers,
        succeeded_shard_count + failed_shard_count != selected_shard_count,
        "succeeded plus failed shards must match selected shards",
    );
    add_if(
        blockers,
        status == "ready" && failed_shard_count != 0,
        "ready execution must have zero failed shards",
    );
    add_if(
        blockers,
        status == "ready" && selected_object_count < 1,
        "ready execution must select at least one Bronze object",
    );
    add_if(
        blockers,
        status == "ready" && output_row_count < 1,
        "ready execution must produce Silver handoff rows",
    );
}

fn validate_shard_result(
    config: &CheckConfig,
    result: &JsonValue,
    blockers: &mut Vec<String>,
) -> anyhow::Result<()> {
    let shard_id = string_property(result, "shard_id");
    let status = string_property(result, "status");
    let summary_path = string_property(result, "summary_path");
    let output_path = string_property(result, "output_path");
    let output_storage_driver = string_property_default(result, "output_storage_driver", "local");
    let output_object_key = string_property(result, "output_object_key");

    add_if(
        blockers,
        shard_id.trim().is_empty(),
        "shard_result shard_id is required",
    );
    add_if(
        blockers,
        !matches!(status.as_str(), "succeeded" | "failed" | "planned"),
        &format!("shard_result status invalid: {shard_id}"),
    );
    if status == "succeeded" {
        add_if(
            blockers,
            summary_path.trim().is_empty(),
            &format!("succeeded shard must include summary_path: {shard_id}"),
        );
        add_if(
            blockers,
            !matches!(output_storage_driver.as_str(), "local" | "r2"),
            &format!("succeeded shard output_storage_driver invalid: {shard_id}"),
        );
        if output_storage_driver == "local" {
            add_if(
                blockers,
                output_path.trim().is_empty(),
                &format!("succeeded local shard must include output_path: {shard_id}"),
            );
        } else if output_storage_driver == "r2" {
            add_if(
                blockers,
                !is_safe_object_key(&output_object_key),
                &format!("succeeded r2 shard must include safe output_object_key: {shard_id}"),
            );
        }
        if !summary_path.trim().is_empty() {
            validate_shard_summary(config, &summary_path, &shard_id, blockers)?;
        }
        if output_storage_driver == "local" && !output_path.trim().is_empty() {
            let resolved =
                resolve_repo_path(&config.root, &PathBuf::from(&output_path), "output_path")?;
            if !resolved.is_file() {
                blockers.push(format!("succeeded shard output file missing: {shard_id}"));
            }
        }
    }
    if status == "failed" {
        add_if(
            blockers,
            string_property(result, "error").trim().is_empty(),
            &format!("failed shard must include error: {shard_id}"),
        );
    }
    Ok(())
}

fn validate_shard_summary(
    config: &CheckConfig,
    summary_path: &str,
    shard_id: &str,
    blockers: &mut Vec<String>,
) -> anyhow::Result<()> {
    let resolved = resolve_repo_path(&config.root, &PathBuf::from(summary_path), "summary_path")?;
    add_forbidden_token_blockers(&resolved, &format!("shard summary {shard_id}"), blockers)?;
    if !resolved.is_file() {
        blockers.push(format!("succeeded shard summary file missing: {shard_id}"));
        return Ok(());
    }
    let summary = read_json(&resolved, "silver handoff shard summary")?;
    let source = summary.get("source").unwrap_or(&JsonValue::Null);
    let output = summary.get("output").unwrap_or(&JsonValue::Null);
    add_if(
        blockers,
        string_property(&summary, "schema_version") != SHARD_SUMMARY_SCHEMA_VERSION,
        &format!("shard summary schema mismatch: {shard_id}"),
    );
    add_if(
        blockers,
        string_property(&summary, "status") != "ready",
        &format!("shard summary status must be ready: {shard_id}"),
    );
    add_if(
        blockers,
        bool_property(&summary, "completion_claim_allowed", true),
        &format!("shard summary completion_claim_allowed must be false: {shard_id}"),
    );
    add_if(
        blockers,
        !matches!(
            string_property(source, "storage_driver").as_str(),
            "local" | "r2"
        ),
        &format!("shard storage_driver invalid: {shard_id}"),
    );
    add_if(
        blockers,
        long_property(source, "selected_object_count") < 1,
        &format!("shard selected_object_count must be positive: {shard_id}"),
    );
    add_if(
        blockers,
        !matches!(
            string_property_default(output, "storage_driver", "local").as_str(),
            "local" | "r2"
        ),
        &format!("shard output storage_driver invalid: {shard_id}"),
    );
    add_if(
        blockers,
        string_property(output, "contract") != "silver.parcel_boundaries",
        &format!("shard output contract must be silver.parcel_boundaries: {shard_id}"),
    );
    add_if(
        blockers,
        long_property(output, "row_count") < 1,
        &format!("shard row_count must be positive: {shard_id}"),
    );
    Ok(())
}

fn select_shards(shards: &[JsonValue], config: &ExecuteConfig) -> Vec<JsonValue> {
    if !config.shard_id.trim().is_empty() {
        return shards
            .iter()
            .filter(|shard| string_property(shard, "shard_id") == config.shard_id)
            .cloned()
            .collect();
    }
    shards
        .iter()
        .skip(config.skip_shards as usize)
        .take(config.max_shards as usize)
        .cloned()
        .collect()
}

fn planned_shard_result(
    config: &ExecuteConfig,
    shard: &JsonValue,
    output_path: &Path,
    output_object_key: &str,
    summary_path: &Path,
) -> JsonValue {
    base_shard_result(
        config,
        shard,
        "planned",
        output_path,
        output_object_key,
        summary_path,
        json!({}),
    )
}

fn failed_shard_result(
    config: &ExecuteConfig,
    shard: &JsonValue,
    output_path: &Path,
    output_object_key: &str,
    summary_path: &Path,
    exit_code: i32,
    output: &[String],
) -> JsonValue {
    base_shard_result(
        config,
        shard,
        "failed",
        output_path,
        output_object_key,
        summary_path,
        json!({
            "exit_code": exit_code,
            "error": output.iter().rev().take(20).cloned().collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>().join("\n"),
        }),
    )
}

fn succeeded_shard_result(
    config: &ExecuteConfig,
    shard: &JsonValue,
    output_path: &Path,
    output_object_key: &str,
    summary_path: &Path,
    selected_object_count: i64,
    input_bytes: i64,
    output_row_count: i64,
) -> JsonValue {
    base_shard_result(
        config,
        shard,
        "succeeded",
        output_path,
        output_object_key,
        summary_path,
        json!({
            "selected_object_count": selected_object_count,
            "input_bytes": input_bytes,
            "output_row_count": output_row_count,
            "exit_code": 0,
        }),
    )
}

fn base_shard_result(
    config: &ExecuteConfig,
    shard: &JsonValue,
    status: &str,
    output_path: &Path,
    output_object_key: &str,
    summary_path: &Path,
    extra: JsonValue,
) -> JsonValue {
    let mut value = json!({
        "shard_id": string_property(shard, "shard_id"),
        "status": status,
        "target_contract": string_property(shard, "target_contract"),
        "transformer": string_property(shard, "transformer"),
        "filtered_manifest_start_index": long_property(shard, "filtered_manifest_start_index"),
        "filtered_manifest_end_index": long_property(shard, "filtered_manifest_end_index"),
        "object_count": long_property(shard, "object_count"),
        "output_path": if config.output_storage_driver == "local" {
            repo_relative_path(&config.root, output_path)
        } else {
            String::new()
        },
        "output_storage_driver": config.output_storage_driver,
        "output_object_key": output_object_key,
        "summary_path": repo_relative_path(&config.root, summary_path),
    });
    if let (Some(target), Some(extra)) = (value.as_object_mut(), extra.as_object()) {
        for (key, value) in extra {
            target.insert(key.clone(), value.clone());
        }
    }
    value
}

struct Runner {
    executable: PathBuf,
    prefix_args: Vec<String>,
}

impl Runner {
    fn from_config(config: &ExecuteConfig) -> anyhow::Result<Self> {
        if let Some(path) = &config.runner_exe {
            let resolved = fs::canonicalize(path)
                .with_context(|| format!("RunnerExe does not exist: {}", path.display()))?;
            return Ok(Self {
                executable: resolved,
                prefix_args: Vec::new(),
            });
        }
        Ok(Self {
            executable: resolve_cargo_exe(config.cargo_exe.clone())?,
            prefix_args: vec![
                "run".to_owned(),
                "-p".to_owned(),
                "foundation-outbox-publisher".to_owned(),
                "--".to_owned(),
            ],
        })
    }

    fn run(
        &self,
        config: &ExecuteConfig,
        shard: &JsonValue,
        output_path: &Path,
        output_object_key: &str,
        summary_path: &Path,
    ) -> anyhow::Result<RunnerOutput> {
        let shard_id = string_property(shard, "shard_id");
        let mut command = ProcessCommand::new(&self.executable);
        command.args(&self.prefix_args);
        command.arg("export-vworld-cadastral-silver-handoff-shard");
        for (name, value) in &config.env_overrides {
            command.env(name, value);
        }
        command
            .env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_BRONZE_MANIFEST_PATH",
                &config.manifest_path,
            )
            .env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_STORAGE_DRIVER",
                &config.bronze_storage_driver,
            )
            .env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_BRONZE_ROOT",
                &config.bronze_local_root,
            )
            .env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_OUTPUT_STORAGE_DRIVER",
                &config.output_storage_driver,
            )
            .env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_OUTPUT_PATH",
                output_path,
            )
            .env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_OUTPUT_OBJECT_KEY",
                output_object_key,
            )
            .env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_SUMMARY_PATH",
                summary_path,
            )
            .env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_SOURCE_RECORD_ID",
                format!("national-promotion:{shard_id}"),
            )
            .env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_SOURCE_SNAPSHOT_ID",
                format!("national-promotion:{shard_id}"),
            )
            .env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_VALID_FROM_UTC",
                "2026-05-24T00:00:00Z",
            )
            .env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_MANIFEST_START_INDEX",
                long_property(shard, "filtered_manifest_start_index").to_string(),
            )
            .env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_MANIFEST_END_INDEX",
                long_property(shard, "filtered_manifest_end_index").to_string(),
            );
        let output = command
            .output()
            .with_context(|| format!("failed to run shard runner {}", self.executable.display()))?;
        let mut lines = String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        lines.extend(
            String::from_utf8_lossy(&output.stderr)
                .lines()
                .map(str::to_owned),
        );
        Ok(RunnerOutput {
            status_success: output.status.success(),
            exit_code: output.status.code().unwrap_or(1),
            output: lines,
        })
    }
}

struct RunnerOutput {
    status_success: bool,
    exit_code: i32,
    output: Vec<String>,
}

fn import_dot_env_file(path: &Path) -> anyhow::Result<HashMap<String, String>> {
    let mut values = HashMap::new();
    if !path.is_file() {
        return Ok(values);
    }
    for line in read_text(path, "EnvFile")?.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((name, value)) = trimmed.split_once('=') else {
            bail!("Invalid .env line in {}: {line}", path.display());
        };
        values.insert(
            name.trim().to_owned(),
            value.trim().trim_matches('"').trim_matches('\'').to_owned(),
        );
    }
    Ok(values)
}

fn resolve_optional_input_path(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}

fn assert_r2_env_present(config: &ExecuteConfig) -> anyhow::Result<()> {
    for name in ["R2_BUCKET_NAME", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"] {
        if config.env_value(name).is_none() {
            bail!("Missing required environment variable: {name}");
        }
    }
    if config.env_value("R2_ENDPOINT").is_none() && config.env_value("R2_ACCOUNT_ID").is_none() {
        bail!("Missing required R2 addressing environment variable: R2_ENDPOINT or R2_ACCOUNT_ID");
    }
    Ok(())
}

fn safe_object_key_prefix(prefix: &str, field: &str) -> anyhow::Result<String> {
    if prefix.trim().is_empty() {
        bail!("{field} is required");
    }
    let trimmed = prefix.trim().trim_end_matches('/').to_owned();
    if trimmed != prefix.trim_end_matches('/') {
        bail!("{field} must not contain surrounding whitespace");
    }
    if !is_safe_object_key(&trimmed) {
        bail!("{field} must be a safe provider-relative key prefix");
    }
    Ok(trimmed)
}

fn is_safe_object_key(key: &str) -> bool {
    let trimmed = key.trim();
    if trimmed.is_empty() || trimmed != key || trimmed.starts_with('/') || trimmed.contains('\\') {
        return false;
    }
    if trimmed.contains("..") {
        return false;
    }
    trimmed
        .split('/')
        .all(|segment| !segment.trim().is_empty() && segment != "." && segment != "..")
}

fn add_forbidden_token_blockers(
    path: &Path,
    label: &str,
    blockers: &mut Vec<String>,
) -> anyhow::Result<()> {
    if !path.is_file() {
        return Ok(());
    }
    let content = read_text(path, label)?;
    for token in FORBIDDEN_TOKENS {
        if content.contains(token) {
            blockers.push(format!("{label} must not contain forbidden token: {token}"));
        }
    }
    Ok(())
}

fn read_text(path: &Path, label: &str) -> anyhow::Result<String> {
    let bytes =
        fs::read(path).with_context(|| format!("failed to read {label} {}", path.display()))?;
    Ok(String::from_utf8_lossy(&bytes)
        .trim_start_matches('\u{feff}')
        .to_owned())
}

fn add_if(blockers: &mut Vec<String>, condition: bool, message: &str) {
    if condition {
        blockers.push(message.to_owned());
    }
}

fn string_property(value: &JsonValue, name: &str) -> String {
    value.get(name).map(json_to_string).unwrap_or_default()
}

fn string_property_default(value: &JsonValue, name: &str, default: &str) -> String {
    value
        .get(name)
        .map(json_to_string)
        .unwrap_or_else(|| default.to_owned())
}

fn json_to_string(value: &JsonValue) -> String {
    match value {
        JsonValue::String(text) => text.clone(),
        JsonValue::Null => String::new(),
        JsonValue::Bool(flag) => flag.to_string(),
        JsonValue::Number(number) => number.to_string(),
        other => other.to_string(),
    }
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

fn long_property(value: &JsonValue, name: &str) -> i64 {
    value
        .get(name)
        .and_then(|property| match property {
            JsonValue::Number(number) => number.as_i64(),
            JsonValue::String(text) => text.trim().parse::<i64>().ok(),
            _ => None,
        })
        .unwrap_or_default()
}

fn json_array(value: &JsonValue) -> Vec<JsonValue> {
    if let Some(values) = value.as_array() {
        return values.to_vec();
    }
    if value.is_object() {
        return vec![value.clone()];
    }
    Vec::new()
}

fn env_string(name: &str, default: &str) -> anyhow::Result<String> {
    Ok(optional_env_value(name)?.unwrap_or_else(|| default.to_owned()))
}

fn env_bool(name: &str, default: bool) -> anyhow::Result<bool> {
    match std::env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(default),
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => bail!("invalid {name} environment variable"),
        },
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn env_i64(name: &str, default: i64) -> anyhow::Result<i64> {
    match std::env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(default),
        Ok(value) => value
            .trim()
            .parse::<i64>()
            .with_context(|| format!("invalid {name} environment variable")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}
