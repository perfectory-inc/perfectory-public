use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{bail, Context};
use chrono::{SecondsFormat, Utc};
use collection_domain::{
    building_register_dataset_slug, is_canonical_source_slug, source_slug as canonical_source_slug,
};
use serde_json::{json, Value as JsonValue};

use crate::public_api_metric_writer;
use crate::public_data_control_support::{
    env_path, git_head, read_json, repo_relative_path, resolve_repo_path, utc_now, write_json_file,
};

mod bronze_report;
mod child_env;
mod command_runner;
mod env_helpers;

use bronze_report::{bronze_run_report, BronzeRunReport};
use child_env::{building_child_env, vworld_child_env};
use command_runner::{invoke_outbox_command, read_last_json_from_command_log};
use env_helpers::{
    env_bool, env_i64, env_string, import_dotenv, normalize_windows_verbatim_path, require_env,
    resolve_cargo,
};

const SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_run_evidence.v1";
const APPROVAL_SCHEMA_VERSION: &str =
    "foundation-platform.national_data_collection_rollout_approval.v1";
const SHARD_MANIFEST_SCHEMA_VERSION: &str =
    "foundation-platform.national_data_collection_shard_manifest.v1";
const PLAN_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_plan.v1";
const DEFAULT_APPROVAL_PATH: &str = "target/audit/national-data-collection-rollout-approval.json";
const DEFAULT_OUTPUT_PATH: &str = "target/audit/national-data-collection-run-evidence.json";
const DEFAULT_LOG_PATH: &str = "target/audit/national-data-collection-run.log";
const DEFAULT_LAKEHOUSE_VERIFY_PATH: &str = "target/audit/lakehouse-registry-verify.json";
const DEFAULT_LAKEHOUSE_RECORD_PATH: &str =
    "target/audit/lakehouse-bronze-evidence-registry-record.json";
const DEFAULT_QUOTA_METRICS_PATH: &str =
    "target/public-api-quota/national-data-collection-run.prom";
const DEFAULT_LOCAL_OBJECT_ROOT: &str = "target/bronze-national-data-collection";
const DEFAULT_SHARD_MANIFEST_PATH: &str =
    "target/audit/national-data-collection-shard-manifest.json";
const DEFAULT_PLAN_PATH: &str = "target/audit/national-data-collection-plan.json";
const MODE: &str = "national_data_collection_run";

pub fn run() -> anyhow::Result<()> {
    let config = RunConfig::from_env()?;
    let approval = read_json(&config.approval_path, "national rollout approval")
        .with_context(|| "National rollout approval artifact missing")?;
    validate_approval(&approval)?;
    config.validate()?;

    if config.run_mode == RunMode::National && config.execute {
        validate_national_mode_inputs(&config)?;
        bail!("national mode collection plan requires a dedicated ledger executor");
    }

    let planned_provider = provider_plan(&config);
    let planned_vworld = if config.include_vworld_cadastral {
        Some(vworld_provider_plan(&config))
    } else {
        None
    };

    if !config.execute {
        let report = base_report(
            &config,
            &approval,
            "planned",
            false,
            planned_provider,
            planned_vworld,
            0,
            None,
            None,
        );
        write_json_file(&config.output_path, &report)?;
        println!(
            "national-data-collection-run-planned status=planned run_mode={} requests=0 cap={} report={}",
            config.run_mode.as_str(),
            config.request_cap,
            config.output_path.display()
        );
        return Ok(());
    }

    validate_execution_confirmations(&config)?;
    let dotenv = import_dotenv(&config.env_file)?;
    require_env(&dotenv, "DATABASE_URL")?;
    require_env(&dotenv, "DATA_GO_KR_SERVICE_KEY")?;
    if config.include_vworld_cadastral {
        require_env(&dotenv, "VWORLD_API_KEY")?;
    }

    fs::create_dir_all(&config.local_object_root)?;
    if let Some(parent) = config.log_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        &config.log_path,
        format!(
            "national_data_collection_run started_at_utc={}\n",
            utc_now()
        ),
    )?;

    let cargo = resolve_cargo(&config.cargo_exe)?;
    let preflight = run_lakehouse_registry_preflight(&config, &cargo, &dotenv)?;
    write_quota_metric(
        &config.quota_metrics_path,
        "data.go.kr",
        &config.operation,
        config.max_pages,
    )?;

    let building_env = building_child_env(&config, &dotenv);
    let building_run = invoke_outbox_command(
        &config.root,
        &cargo,
        "ingest-building-register",
        &building_env,
        &[],
        &config.log_path,
    )?;
    if building_run.exit_code != 0 {
        write_dependency_metric(
            &config.quota_metrics_path,
            "data.go.kr",
            &config.operation,
            building_run.duration,
            "failed",
        )?;
        bail!(
            "national data collection run failed with cargo exit code {}",
            building_run.exit_code
        );
    }
    write_dependency_metric(
        &config.quota_metrics_path,
        "data.go.kr",
        &config.operation,
        building_run.duration,
        "succeeded",
    )?;

    let building_bronze = bronze_run_report(
        &config.local_object_root,
        building_run.started_at,
        &config.source_slug,
        "data.go.kr",
    )?;
    let ready_provider = ready_provider(&config, &building_bronze);

    let mut total_object_count = building_bronze.object_count;
    let mut total_record_count = building_bronze.logical_record_count;
    let mut executed_request_count = config.max_pages;
    let mut ready_vworld = None;

    if config.include_vworld_cadastral {
        write_quota_metric(
            &config.quota_metrics_path,
            "VWorld",
            "ingest-vworld-cadastral",
            config.vworld_max_pages,
        )?;
        let vworld_env = vworld_child_env(&config, &dotenv);
        let stale_env = [
            "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_ATTR_FILTER",
            "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_PNU",
            "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_PNU",
            "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GEOM_FILTER",
            "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GRID_BBOX",
            "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GRID_ROWS",
            "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GRID_COLUMNS",
            "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_ADAPTIVE_SUBDIVISION",
            "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_MAX_SUBDIVISION_DEPTH",
        ];
        let vworld_run = invoke_outbox_command(
            &config.root,
            &cargo,
            "ingest-vworld-cadastral",
            &vworld_env,
            &stale_env,
            &config.log_path,
        )?;
        if vworld_run.exit_code != 0 {
            write_dependency_metric(
                &config.quota_metrics_path,
                "VWorld",
                "ingest-vworld-cadastral",
                vworld_run.duration,
                "failed",
            )?;
            bail!(
                "VWorld cadastral data collection run failed with cargo exit code {}",
                vworld_run.exit_code
            );
        }
        write_dependency_metric(
            &config.quota_metrics_path,
            "VWorld",
            "ingest-vworld-cadastral",
            vworld_run.duration,
            "succeeded",
        )?;

        let vworld_bronze = bronze_run_report(
            &config.local_object_root,
            vworld_run.started_at,
            &config.vworld_source_slug,
            "VWorld",
        )?;
        total_object_count += vworld_bronze.object_count;
        total_record_count += vworld_bronze.logical_record_count;
        executed_request_count += config.vworld_max_pages;
        ready_vworld = Some(ready_vworld_provider(&config, &vworld_bronze));
    }

    let mut report = base_report(
        &config,
        &approval,
        "ready",
        true,
        ready_provider,
        ready_vworld,
        executed_request_count,
        Some(preflight),
        None,
    );
    report["raw_response_preserved"] = JsonValue::Bool(true);
    write_json_file(&config.output_path, &report)?;

    let registry_record = run_lakehouse_bronze_registry_record(&config, &cargo, &dotenv)?;
    report["lakehouse_registry_record"] = registry_record;
    write_json_file(&config.output_path, &report)?;

    println!(
        "national-data-collection-run-ok status=ready run_mode={} requests={} objects={} records={} report={}",
        config.run_mode.as_str(),
        executed_request_count,
        total_object_count,
        total_record_count,
        config.output_path.display()
    );
    Ok(())
}

struct RunConfig {
    root: PathBuf,
    env_file: PathBuf,
    approval_path: PathBuf,
    output_path: PathBuf,
    log_path: PathBuf,
    lakehouse_registry_verify_path: PathBuf,
    lakehouse_registry_record_path: PathBuf,
    quota_metrics_path: PathBuf,
    local_object_root: PathBuf,
    shard_manifest_path: PathBuf,
    plan_path: PathBuf,
    run_mode: RunMode,
    operation: String,
    sigungu_cd: String,
    bjdong_cd: String,
    max_pages: i64,
    num_of_rows: i64,
    request_cap: i64,
    source_slug: String,
    include_vworld_cadastral: bool,
    vworld_source_slug: String,
    vworld_dataset: String,
    vworld_attr_filter: String,
    vworld_max_pages: i64,
    vworld_size: i64,
    cargo_exe: String,
    execute: bool,
    confirm_public_api_quota_impact: bool,
    confirm_national_data_collection_run: bool,
    confirm_local_bronze_storage: bool,
}

impl RunConfig {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );
        let env_file = env_path(
            "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_ENV_FILE",
            "",
        )?;
        let env_file = if env_file.as_os_str().is_empty() {
            root.join(".env.local")
        } else {
            resolve_repo_path(&root, &env_file, "EnvFile")?
        };
        let sigungu_cd = env_string(
            "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_SIGUNGU_CD",
            "11680",
        )?;
        let bjdong_cd = env_string(
            "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_BJDONG_CD",
            "10300",
        )?;
        let vworld_attr_filter = env_string(
            "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_VWORLD_ATTR_FILTER",
            &format!("emdCd:=:{}{}", sigungu_cd, &bjdong_cd[..3]),
        )?;
        let operation = env_string(
            "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_OPERATION",
            "getBrTitleInfo",
        )?;
        // ADR 0014 §6 (owner-confirmed): the `*-national-pilot` suffix is a RUN-scope distinction
        // (carried by run_id / manifest / local-FS prefix), NOT dataset identity, so the source slug
        // folds to the plain canonical generator output. Building-register resolves the run's
        // operation to its specific sub-type slug; cadastral is the `cadastral` dataset.
        let default_source_slug = default_building_register_source_slug(&operation)?;
        let default_vworld_source_slug = canonical_source_slug("VWorld", "cadastral")?;

        Ok(Self {
            approval_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_APPROVAL_PATH",
                    DEFAULT_APPROVAL_PATH,
                )?,
                "ApprovalPath",
            )?,
            output_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_OUTPUT_PATH",
                    DEFAULT_OUTPUT_PATH,
                )?,
                "OutputPath",
            )?,
            log_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_LOG_PATH",
                    DEFAULT_LOG_PATH,
                )?,
                "LogPath",
            )?,
            lakehouse_registry_verify_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_LAKEHOUSE_REGISTRY_VERIFY_PATH",
                    DEFAULT_LAKEHOUSE_VERIFY_PATH,
                )?,
                "LakehouseRegistryVerifyPath",
            )?,
            lakehouse_registry_record_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_LAKEHOUSE_REGISTRY_RECORD_PATH",
                    DEFAULT_LAKEHOUSE_RECORD_PATH,
                )?,
                "LakehouseRegistryRecordPath",
            )?,
            quota_metrics_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_QUOTA_METRICS_PATH",
                    DEFAULT_QUOTA_METRICS_PATH,
                )?,
                "QuotaMetricsPath",
            )?,
            local_object_root: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_LOCAL_OBJECT_ROOT",
                    DEFAULT_LOCAL_OBJECT_ROOT,
                )?,
                "LocalObjectRoot",
            )?,
            shard_manifest_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_SHARD_MANIFEST_PATH",
                    DEFAULT_SHARD_MANIFEST_PATH,
                )?,
                "ShardManifestPath",
            )?,
            plan_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_PLAN_PATH",
                    DEFAULT_PLAN_PATH,
                )?,
                "PlanPath",
            )?,
            root,
            env_file,
            run_mode: RunMode::parse(&env_string(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_MODE",
                "pilot",
            )?)?,
            operation,
            sigungu_cd,
            bjdong_cd,
            max_pages: env_i64("FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_MAX_PAGES", 1)?,
            num_of_rows: env_i64("FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_NUM_OF_ROWS", 10)?,
            request_cap: env_i64("FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_REQUEST_CAP", 1)?,
            source_slug: env_string(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_SOURCE_SLUG",
                &default_source_slug,
            )?,
            include_vworld_cadastral: env_bool(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_INCLUDE_VWORLD_CADASTRAL",
                false,
            )?,
            vworld_source_slug: env_string(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_VWORLD_SOURCE_SLUG",
                &default_vworld_source_slug,
            )?,
            vworld_dataset: env_string(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_VWORLD_DATASET",
                "LP_PA_CBND_BUBUN",
            )?,
            vworld_attr_filter,
            vworld_max_pages: env_i64(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_VWORLD_MAX_PAGES",
                1,
            )?,
            vworld_size: env_i64("FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_VWORLD_SIZE", 10)?,
            cargo_exe: env_string("FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_CARGO_EXE", "")?,
            execute: env_bool("FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_EXECUTE", false)?,
            confirm_public_api_quota_impact: env_bool(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_CONFIRM_PUBLIC_API_QUOTA_IMPACT",
                false,
            )?,
            confirm_national_data_collection_run: env_bool(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_CONFIRM_NATIONAL_DATA_COLLECTION_RUN",
                false,
            )?,
            confirm_local_bronze_storage: env_bool(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_RUN_CONFIRM_LOCAL_BRONZE_STORAGE",
                false,
            )?,
        })
    }

    fn validate(&self) -> anyhow::Result<()> {
        if !simple_identifier(&self.operation) {
            bail!("Operation must be a simple API operation identifier");
        }
        if !five_digits(&self.sigungu_cd) || !five_digits(&self.bjdong_cd) {
            bail!("SigunguCd and BjdongCd must be exactly five digits");
        }
        if !is_canonical_source_slug(&self.source_slug) {
            bail!("SourceSlug must be canonical {{providerid}}__{{dataset_slug}}");
        }
        if !is_canonical_source_slug(&self.vworld_source_slug) {
            bail!("VWorldSourceSlug must be canonical {{providerid}}__{{dataset_slug}}");
        }
        if !self
            .vworld_dataset
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            bail!("VWorldDataset must be an ASCII dataset identifier");
        }
        if !is_vworld_emd_filter(&self.vworld_attr_filter) {
            bail!("VWorldAttrFilter must use emdCd:=:<8-digit-provider-emd-code>");
        }
        if self.max_pages < 1
            || self.num_of_rows < 1
            || self.num_of_rows > 100
            || self.request_cap < 1
        {
            bail!("MaxPages, NumOfRows, and RequestCap must be positive bounded values");
        }
        if self.include_vworld_cadastral
            && (self.vworld_max_pages < 1 || self.vworld_size < 1 || self.vworld_size > 1000)
        {
            bail!("VWorldMaxPages and VWorldSize must be positive bounded values");
        }
        let planned_request_count = self.max_pages
            + if self.include_vworld_cadastral {
                self.vworld_max_pages
            } else {
                0
            };
        if planned_request_count > self.request_cap {
            bail!("planned request count must not exceed RequestCap");
        }
        if self.run_mode == RunMode::Pilot && (planned_request_count > 20 || self.request_cap > 20)
        {
            bail!("pilot run is capped at 20 public API requests");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum RunMode {
    Pilot,
    National,
}

impl RunMode {
    fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "pilot" => Ok(Self::Pilot),
            "national" => Ok(Self::National),
            other => bail!("RunMode is invalid: {other}"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Pilot => "pilot",
            Self::National => "national",
        }
    }
}

fn validate_execution_confirmations(config: &RunConfig) -> anyhow::Result<()> {
    if !config.confirm_public_api_quota_impact {
        bail!("Public API quota impact must be confirmed with -ConfirmPublicApiQuotaImpact when -Execute is used");
    }
    if !config.confirm_national_data_collection_run {
        bail!("ConfirmNationalDataCollectionRun is required when -Execute is used");
    }
    if !config.confirm_local_bronze_storage {
        bail!("Local Bronze storage is proof-only and requires -ConfirmLocalBronzeStorage when -Execute is used");
    }
    Ok(())
}

fn validate_approval(approval: &JsonValue) -> anyhow::Result<()> {
    if string_property(approval, "schema_version") != APPROVAL_SCHEMA_VERSION {
        bail!("national rollout approval schema mismatch");
    }
    if string_property(approval, "status") != "ready"
        || !bool_property(approval, "approved", false)
        || !bool_property(approval, "national_rollout_allowed", false)
    {
        bail!("national rollout approval artifact is not ready");
    }
    Ok(())
}

fn validate_national_mode_inputs(config: &RunConfig) -> anyhow::Result<()> {
    if !config.shard_manifest_path.is_file() {
        bail!("national mode requires a sharded run manifest before execution");
    }
    let manifest = read_json(&config.shard_manifest_path, "national shard manifest")?;
    if string_property(&manifest, "schema_version") != SHARD_MANIFEST_SCHEMA_VERSION
        || string_property(&manifest, "status") != "ready"
        || string_property(&manifest, "run_mode") != "national"
        || bool_property(&manifest, "completion_claim_allowed", true)
        || bool_property(&manifest, "production_cutover_allowed", true)
        || bool_property(&manifest, "national_rollout_allowed", true)
    {
        bail!("national shard manifest is not ready");
    }
    if !config.plan_path.is_file() {
        bail!("national mode requires a compiled collection plan and execution ledger");
    }
    let plan = read_json(&config.plan_path, "national collection plan")?;
    if string_property(&plan, "schema_version") != PLAN_SCHEMA_VERSION
        || string_property(&plan, "status") != "ready"
        || string_property(&plan, "run_mode") != "national"
        || bool_property(&plan, "completion_claim_allowed", true)
        || bool_property(&plan, "production_cutover_allowed", true)
        || bool_property(&plan, "national_rollout_allowed", true)
        || !sha256_hex(&string_property(&plan, "compiler_input_hash_sha256"))
    {
        bail!("national collection plan is not ready");
    }
    let ledger = plan
        .get("execution_ledger")
        .ok_or_else(|| anyhow::anyhow!("national collection plan ledger missing"))?;
    if i64_property(ledger, "entry_count", 0) < 1 || !bool_property(ledger, "append_only", false) {
        bail!("national collection plan ledger is not ready");
    }
    let ledger_path = resolve_repo_path(
        &config.root,
        &PathBuf::from(string_property(ledger, "path")),
        "execution_ledger.path",
    )?;
    if !ledger_path.is_file() {
        bail!("national collection execution ledger missing");
    }
    Ok(())
}

fn base_report(
    config: &RunConfig,
    approval: &JsonValue,
    status: &str,
    executed: bool,
    provider: JsonValue,
    vworld_provider: Option<JsonValue>,
    request_count_total: i64,
    preflight: Option<JsonValue>,
    registry_record: Option<JsonValue>,
) -> JsonValue {
    let mut providers = serde_json::Map::new();
    providers.insert("data_go_kr_building_register".to_owned(), provider);
    if let Some(provider) = vworld_provider {
        providers.insert("vworld_cadastral".to_owned(), provider);
    }
    let next_gates = if config.include_vworld_cadastral {
        vec![
            "sharded-national-run-manifest",
            "silver-gold-national-promotion",
        ]
    } else {
        vec![
            "sharded-national-run-manifest",
            "vworld-cadastral-local-bronze-storage-support",
            "silver-gold-national-promotion",
        ]
    };

    json!({
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true),
        "git_head": git_head(&config.root),
        "status": status,
        "executed": executed,
        "run_mode": config.run_mode.as_str(),
        "raw_response_preserved": false,
        "national_rollout_allowed": true,
        "completion_claim_allowed": false,
        "production_cutover_allowed": false,
        "approval": {
            "path": repo_relative_path(&config.root, &config.approval_path),
            "status": string_property(approval, "status"),
            "approved_by": string_property(approval, "approved_by"),
            "approved_scope": string_property(approval, "approved_scope"),
            "national_rollout_allowed": bool_property(approval, "national_rollout_allowed", false)
        },
        "public_api_quota": {
            "request_count_total": request_count_total,
            "request_cap": config.request_cap,
            "mode": MODE
        },
        "lakehouse_registry_preflight": preflight.unwrap_or_else(|| json!({
            "status": "not_run",
            "report_path": repo_relative_path(&config.root, &config.lakehouse_registry_verify_path),
            "namespace_count": 0,
            "blocker_count": 0
        })),
        "lakehouse_registry_record": registry_record.unwrap_or_else(|| json!({
            "status": "not_run",
            "report_path": repo_relative_path(&config.root, &config.lakehouse_registry_record_path),
            "provider_count": 0,
            "artifact_count": 0,
            "assets": []
        })),
        "providers": JsonValue::Object(providers),
        "evidence_paths": {
            "run_log": repo_relative_path(&config.root, &config.log_path),
            "quota_metrics": repo_relative_path(&config.root, &config.quota_metrics_path),
            "local_bronze_root": repo_relative_path(&config.root, &config.local_object_root),
            "lakehouse_registry_verify": repo_relative_path(&config.root, &config.lakehouse_registry_verify_path),
            "lakehouse_registry_record": repo_relative_path(&config.root, &config.lakehouse_registry_record_path)
        },
        "blockers": [],
        "evidence_limitations": [
            "pilot_scope_only",
            "local_bronze_object_storage_only",
            "does_not_execute_full_national_shards",
            "does_not_approve_production_cutover"
        ],
        "next_gates": next_gates
    })
}

fn provider_plan(config: &RunConfig) -> JsonValue {
    json!({
        "status": "planned",
        "provider": "data.go.kr",
        "operation": config.operation,
        "sigungu_cd": config.sigungu_cd,
        "bjdong_cd": config.bjdong_cd,
        "max_pages": config.max_pages,
        "num_of_rows": config.num_of_rows,
        "request_count": 0,
        "source_slug": config.source_slug,
        "source_record_count": 0,
        "bronze": {
            "storage_driver": "local",
            "object_count": 0,
            "total_size_bytes": 0,
            "objects": []
        }
    })
}

fn vworld_provider_plan(config: &RunConfig) -> JsonValue {
    json!({
        "status": "planned",
        "provider": "VWorld",
        "endpoint": "ingest-vworld-cadastral",
        "dataset": config.vworld_dataset,
        "filter_kind": "attr_filter",
        "attr_filter": config.vworld_attr_filter,
        "max_pages": config.vworld_max_pages,
        "size": config.vworld_size,
        "request_count": 0,
        "source_slug": config.vworld_source_slug,
        "source_record_count": 0,
        "bronze": {
            "storage_driver": "local",
            "object_count": 0,
            "total_size_bytes": 0,
            "objects": []
        }
    })
}

fn ready_provider(config: &RunConfig, bronze: &BronzeRunReport) -> JsonValue {
    json!({
        "status": "ready",
        "provider": "data.go.kr",
        "operation": config.operation,
        "sigungu_cd": config.sigungu_cd,
        "bjdong_cd": config.bjdong_cd,
        "max_pages": config.max_pages,
        "num_of_rows": config.num_of_rows,
        "request_count": config.max_pages,
        "source_slug": config.source_slug,
        "source_record_count": bronze.logical_record_count,
        "ingestion_run_id": bronze.run_id,
        "bronze": {
            "storage_driver": "local",
            "object_count": bronze.object_count,
            "total_size_bytes": bronze.total_size_bytes,
            "objects": bronze.objects
        }
    })
}

fn ready_vworld_provider(config: &RunConfig, bronze: &BronzeRunReport) -> JsonValue {
    json!({
        "status": "ready",
        "provider": "VWorld",
        "endpoint": "ingest-vworld-cadastral",
        "dataset": config.vworld_dataset,
        "filter_kind": "attr_filter",
        "attr_filter": config.vworld_attr_filter,
        "max_pages": config.vworld_max_pages,
        "size": config.vworld_size,
        "request_count": config.vworld_max_pages,
        "source_slug": config.vworld_source_slug,
        "source_record_count": bronze.logical_record_count,
        "ingestion_run_id": bronze.run_id,
        "bronze": {
            "storage_driver": "local",
            "object_count": bronze.object_count,
            "total_size_bytes": bronze.total_size_bytes,
            "objects": bronze.objects
        }
    })
}

fn run_lakehouse_registry_preflight(
    config: &RunConfig,
    cargo: &Path,
    dotenv: &BTreeMap<String, String>,
) -> anyhow::Result<JsonValue> {
    let run = invoke_outbox_command(
        &config.root,
        cargo,
        "verify-lakehouse-registry",
        dotenv,
        &[],
        &config.log_path,
    )?;
    if run.exit_code != 0 {
        bail!("Lakehouse Registry preflight failed before public API collection: command failed");
    }
    let output = read_last_json_from_command_log(&config.log_path, "verify-lakehouse-registry")?;
    if let Some(parent) = config.lakehouse_registry_verify_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&config.lakehouse_registry_verify_path, &output)?;
    let report: JsonValue = serde_json::from_str(&output)?;
    let status = string_property(&report, "status");
    let blockers = report
        .get("blockers")
        .and_then(JsonValue::as_array)
        .map(|items| items.len())
        .unwrap_or(0);
    if status != "ready" || blockers != 0 {
        bail!("Lakehouse Registry preflight failed before public API collection: status={status} blockers={blockers}");
    }
    Ok(json!({
        "status": "ready",
        "report_path": repo_relative_path(&config.root, &config.lakehouse_registry_verify_path),
        "namespace_count": report.get("namespaces").and_then(JsonValue::as_array).map(|items| items.len()).unwrap_or(0),
        "blocker_count": blockers
    }))
}

fn run_lakehouse_bronze_registry_record(
    config: &RunConfig,
    cargo: &Path,
    dotenv: &BTreeMap<String, String>,
) -> anyhow::Result<JsonValue> {
    let mut envs = dotenv.clone();
    envs.insert(
        "FOUNDATION_PLATFORM_LAKEHOUSE_BRONZE_RUN_EVIDENCE_PATH".to_owned(),
        config.output_path.to_string_lossy().to_string(),
    );
    let run = invoke_outbox_command(
        &config.root,
        cargo,
        "record-lakehouse-bronze-run-evidence",
        &envs,
        &[],
        &config.log_path,
    )?;
    if run.exit_code != 0 {
        bail!("Lakehouse Bronze registry record failed");
    }
    let output =
        read_last_json_from_command_log(&config.log_path, "record-lakehouse-bronze-run-evidence")?;
    if let Some(parent) = config.lakehouse_registry_record_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&config.lakehouse_registry_record_path, &output)?;
    let mut report: JsonValue = serde_json::from_str(&output)?;
    if let Some(object) = report.as_object_mut() {
        object.insert(
            "report_path".to_owned(),
            JsonValue::String(repo_relative_path(
                &config.root,
                &config.lakehouse_registry_record_path,
            )),
        );
    }
    Ok(report)
}

fn write_quota_metric(
    path: &Path,
    provider: &str,
    endpoint: &str,
    count: i64,
) -> anyhow::Result<()> {
    public_api_metric_writer::write_quota_metric(path, provider, endpoint, count, "attempted", MODE)
}

fn write_dependency_metric(
    path: &Path,
    provider: &str,
    endpoint: &str,
    duration: Duration,
    outcome: &str,
) -> anyhow::Result<()> {
    public_api_metric_writer::write_dependency_metric_duration(
        path, provider, endpoint, duration, outcome, MODE, None,
    )
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

fn i64_property(value: &JsonValue, name: &str, default: i64) -> i64 {
    value
        .get(name)
        .and_then(|property| match property {
            JsonValue::Number(number) => number.as_i64(),
            JsonValue::String(text) => text.trim().parse::<i64>().ok(),
            _ => None,
        })
        .unwrap_or(default)
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

fn simple_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    first.is_ascii_alphabetic() && bytes.all(|byte| byte.is_ascii_alphanumeric())
}

fn five_digits(value: &str) -> bool {
    value.len() == 5 && value.bytes().all(|byte| byte.is_ascii_digit())
}

/// Canonical default Bronze `source_slug` for the building-register child of a national run.
///
/// Resolves the run's `operation` to its specific dataset_slug and runs the generator, so the
/// default is e.g. `datagokr__building_register_main` for `getBrTitleInfo` (ADR 0014 §6 fold).
fn default_building_register_source_slug(operation: &str) -> anyhow::Result<String> {
    let dataset_slug = building_register_dataset_slug(operation).with_context(|| {
        format!("building-register operation has no registered dataset_slug: {operation}")
    })?;
    Ok(canonical_source_slug("data.go.kr", dataset_slug)?)
}

fn is_vworld_emd_filter(value: &str) -> bool {
    let Some(code) = value.strip_prefix("emdCd:=:") else {
        return false;
    };
    code.len() == 8 && code.bytes().all(|byte| byte.is_ascii_digit())
}

fn sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::{RunConfig, RunMode};
    use std::path::PathBuf;

    /// Builds a minimal `RunConfig` whose non-slug fields already satisfy `validate`, so a test can
    /// flip only the slug fields to exercise the canonical-slug guard. Path fields are unused by
    /// `validate` and are left empty.
    fn valid_config() -> RunConfig {
        RunConfig {
            root: PathBuf::new(),
            env_file: PathBuf::new(),
            approval_path: PathBuf::new(),
            output_path: PathBuf::new(),
            log_path: PathBuf::new(),
            lakehouse_registry_verify_path: PathBuf::new(),
            lakehouse_registry_record_path: PathBuf::new(),
            quota_metrics_path: PathBuf::new(),
            local_object_root: PathBuf::new(),
            shard_manifest_path: PathBuf::new(),
            plan_path: PathBuf::new(),
            run_mode: RunMode::Pilot,
            operation: "getBrTitleInfo".to_owned(),
            sigungu_cd: "11680".to_owned(),
            bjdong_cd: "10300".to_owned(),
            max_pages: 1,
            num_of_rows: 10,
            request_cap: 1,
            source_slug: "datagokr__building_register_main".to_owned(),
            include_vworld_cadastral: false,
            vworld_source_slug: "vworldkr__cadastral".to_owned(),
            vworld_dataset: "LP_PA_CBND_BUBUN".to_owned(),
            vworld_attr_filter: "emdCd:=:11680103".to_owned(),
            vworld_max_pages: 1,
            vworld_size: 10,
            cargo_exe: String::new(),
            execute: false,
            confirm_public_api_quota_impact: false,
            confirm_national_data_collection_run: false,
            confirm_local_bronze_storage: false,
        }
    }

    #[test]
    fn validate_accepts_canonical_slugs() {
        let config = valid_config();
        assert!(
            config.validate().is_ok(),
            "canonical baseline config should validate"
        );
    }

    #[test]
    fn validate_rejects_old_format_source_slug() {
        let mut config = valid_config();
        config.source_slug = "vworld-cadastral".to_owned();
        assert!(
            config.validate().is_err(),
            "old-format source_slug vworld-cadastral must be rejected"
        );
    }

    #[test]
    fn validate_rejects_old_format_vworld_source_slug() {
        let mut config = valid_config();
        config.vworld_source_slug = "vworld-cadastral".to_owned();
        assert!(
            config.validate().is_err(),
            "old-format vworld_source_slug vworld-cadastral must be rejected"
        );
    }

    #[test]
    fn validate_accepts_canonical_vworld_source_slug() {
        let mut config = valid_config();
        config.vworld_source_slug = "vworldkr__cadastral".to_owned();
        assert!(
            config.validate().is_ok(),
            "canonical vworldkr__cadastral must be accepted"
        );
    }
}
