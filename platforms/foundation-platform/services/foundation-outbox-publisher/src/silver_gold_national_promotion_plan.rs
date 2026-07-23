use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use serde_json::{json, Value as JsonValue};

use crate::public_data_control_support::{
    env_path, git_head, read_json, repo_relative_path, resolve_repo_path, utc_now, write_json_file,
};

const SCHEMA_VERSION: &str = "foundation-platform.silver_gold_national_promotion_plan.v1";
const MANIFEST_REPORT_SCHEMA_VERSION: &str =
    "foundation-platform.national_bronze_object_manifest.v1";
const MANIFEST_ENTRY_SCHEMA_VERSION: &str =
    "foundation-platform.national_bronze_object_manifest_entry.v1";
const DEFAULT_PLAN_PATH: &str = "target/audit/silver-gold-national-promotion-plan.json";
const DEFAULT_MANIFEST_REPORT_PATH: &str = "target/audit/national-bronze-object-manifest.json";
const DEFAULT_MANIFEST_PATH: &str = "target/audit/national-bronze-object-manifest.jsonl";
const PROMOTABLE_PROVIDER: &str = "VWorld";
const PROMOTABLE_ENDPOINT: &str = "ingest-vworld-cadastral";
const PROMOTABLE_TARGET_CONTRACT: &str = "silver.parcel_boundaries";
const PROMOTABLE_TRANSFORMER: &str = "vworld_cadastral_r2_bronze_to_silver_handoff";
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

pub fn run_write() -> anyhow::Result<()> {
    let config = WriteConfig::from_env()?;
    if config.max_objects_per_shard < 1 {
        bail!("MaxObjectsPerShard must be positive");
    }
    if config.output_path.is_file() && !config.overwrite {
        bail!("promotion plan already exists; pass -Overwrite to replace it");
    }
    if !config.manifest_report_path.is_file() {
        bail!(
            "Bronze object manifest report missing: {}",
            config.manifest_report_path.display()
        );
    }
    if !config.manifest_path.is_file() {
        bail!(
            "Bronze object manifest missing: {}",
            config.manifest_path.display()
        );
    }

    let manifest_report = read_json(
        &config.manifest_report_path,
        "national bronze object manifest report",
    )?;
    if string_property(&manifest_report, "schema_version") != MANIFEST_REPORT_SCHEMA_VERSION {
        bail!("Bronze object manifest report schema mismatch");
    }
    if string_property(&manifest_report, "status") != "ready" {
        bail!("Bronze object manifest report status must be ready");
    }
    let expected_object_count = long_property(
        manifest_report.get("summary").unwrap_or(&JsonValue::Null),
        "object_count",
    );

    let rows = read_manifest_rows_for_writer(&config.manifest_path)?;
    if rows.len() as i64 != expected_object_count {
        bail!("Bronze object manifest row count must match report object_count");
    }

    let plan = build_plan(&config, expected_object_count, rows);
    write_json_file(&config.output_path, &plan)?;

    let check_config = CheckConfig {
        root: config.root,
        plan_path: config.output_path,
        manifest_report_path: config.manifest_report_path,
        manifest_path: config.manifest_path,
    };
    run_check_with_config(&check_config)
}

struct CheckConfig {
    root: PathBuf,
    plan_path: PathBuf,
    manifest_report_path: PathBuf,
    manifest_path: PathBuf,
}

impl CheckConfig {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = fs::canonicalize(&root)
            .with_context(|| format!("failed to resolve repo root {}", root.display()))?;
        Ok(Self {
            plan_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_PLAN_PATH",
                    DEFAULT_PLAN_PATH,
                )?,
                "PlanPath",
            )?,
            manifest_report_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_MANIFEST_REPORT_PATH",
                    DEFAULT_MANIFEST_REPORT_PATH,
                )?,
                "BronzeObjectManifestReportPath",
            )?,
            manifest_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_MANIFEST_PATH",
                    DEFAULT_MANIFEST_PATH,
                )?,
                "BronzeObjectManifestPath",
            )?,
            root,
        })
    }
}

struct WriteConfig {
    root: PathBuf,
    manifest_report_path: PathBuf,
    manifest_path: PathBuf,
    output_path: PathBuf,
    max_objects_per_shard: i64,
    overwrite: bool,
}

impl WriteConfig {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = fs::canonicalize(&root)
            .with_context(|| format!("failed to resolve repo root {}", root.display()))?;
        Ok(Self {
            manifest_report_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_MANIFEST_REPORT_PATH",
                    DEFAULT_MANIFEST_REPORT_PATH,
                )?,
                "BronzeObjectManifestReportPath",
            )?,
            manifest_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_MANIFEST_PATH",
                    DEFAULT_MANIFEST_PATH,
                )?,
                "BronzeObjectManifestPath",
            )?,
            output_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_PLAN_OUTPUT_PATH",
                    DEFAULT_PLAN_PATH,
                )?,
                "OutputPath",
            )?,
            max_objects_per_shard: env_i64(
                "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_MAX_OBJECTS_PER_SHARD",
                5_000,
            )?,
            overwrite: env_bool(
                "FOUNDATION_PLATFORM_SILVER_GOLD_NATIONAL_PROMOTION_OVERWRITE",
                false,
            )?,
            root,
        })
    }
}

fn run_check_with_config(config: &CheckConfig) -> anyhow::Result<()> {
    if !config.manifest_report_path.is_file()
        || !config.manifest_path.is_file()
        || !config.plan_path.is_file()
    {
        write_skip_report(
            config,
            "silver/gold national promotion plan has not been produced",
        )?;
        println!(
            "silver-gold-national-promotion-plan-ok status=skipped report={}",
            config.plan_path.display()
        );
        return Ok(());
    }

    let mut blockers = Vec::new();
    let manifest_report = read_json(
        &config.manifest_report_path,
        "national bronze object manifest report",
    )?;
    let plan = read_json(&config.plan_path, "silver/gold national promotion plan")?;
    add_forbidden_token_blockers(
        &config.manifest_report_path,
        "Bronze object manifest report",
        &mut blockers,
    )?;
    add_forbidden_token_blockers(
        &config.manifest_path,
        "Bronze object manifest",
        &mut blockers,
    )?;
    add_forbidden_token_blockers(&config.plan_path, "promotion plan", &mut blockers)?;

    validate_manifest_report(&manifest_report, &mut blockers);
    let manifest_rows = read_manifest_rows_for_check(&config.manifest_path, &mut blockers)?;
    let manifest_object_count = long_property(
        manifest_report.get("summary").unwrap_or(&JsonValue::Null),
        "object_count",
    );
    add_if(
        &mut blockers,
        manifest_rows.len() as i64 != manifest_object_count,
        "Bronze object manifest row count must match report object_count",
    );

    let summary = plan.get("summary").unwrap_or(&JsonValue::Null);
    let total_object_count = long_property(summary, "total_object_count");
    let promotable_object_count = long_property(summary, "promotable_object_count");
    let deferred_object_count = long_property(summary, "deferred_object_count");
    let shard_count = long_property(summary, "shard_count");
    let max_objects_per_shard = long_property(summary, "max_objects_per_shard");

    validate_plan_top_level(
        &plan,
        manifest_object_count,
        total_object_count,
        promotable_object_count,
        deferred_object_count,
        max_objects_per_shard,
        &mut blockers,
    );
    let promotion_groups = json_array(plan.get("promotion_groups").unwrap_or(&JsonValue::Null));
    let deferred_groups = json_array(
        plan.get("deferred_provider_groups")
            .unwrap_or(&JsonValue::Null),
    );
    let shards = json_array(plan.get("shards").unwrap_or(&JsonValue::Null));
    add_if(
        &mut blockers,
        shards.len() as i64 != shard_count,
        "shard_count must match shards",
    );
    add_if(
        &mut blockers,
        !deferred_groups.is_empty() && bool_property(&plan, "full_promotion_allowed", true),
        "full_promotion_allowed must be false while provider groups are deferred",
    );

    let promotable_from_groups = validate_promotion_groups(&promotion_groups, &mut blockers);
    add_if(
        &mut blockers,
        promotable_from_groups != promotable_object_count,
        "promotion group object_count must match promotable_object_count",
    );
    let deferred_from_groups = validate_deferred_groups(&deferred_groups, &mut blockers);
    add_if(
        &mut blockers,
        deferred_from_groups != deferred_object_count,
        "deferred group object_count must match deferred_object_count",
    );
    let shard_object_sum = validate_shards(
        &shards,
        max_objects_per_shard,
        promotable_object_count,
        &mut blockers,
    );
    add_if(
        &mut blockers,
        shard_object_sum != promotable_object_count,
        "shard object count must match promotable_object_count",
    );

    if !blockers.is_empty() {
        println!(
            "silver-gold-national-promotion-plan-blocked status=blocked blockers={} report={}",
            blockers.len(),
            config.plan_path.display()
        );
        for blocker in blockers {
            println!("blocker={blocker}");
        }
        bail!("silver/gold national promotion plan blocked");
    }

    println!(
        "silver-gold-national-promotion-plan-ok status=ready promotable_objects={promotable_object_count} deferred_objects={deferred_object_count} shards={shard_count} report={}",
        config.plan_path.display()
    );
    Ok(())
}

fn build_plan(config: &WriteConfig, expected_object_count: i64, rows: Vec<JsonValue>) -> JsonValue {
    let mut groups: HashMap<String, GroupAccumulator> = HashMap::new();
    for row in rows {
        let provider = string_property(&row, "provider");
        let endpoint = string_property(&row, "endpoint");
        let job_id = string_property(&row, "job_id");
        let key = format!("{provider}\t{endpoint}");
        let group = groups
            .entry(key)
            .or_insert_with(|| GroupAccumulator::new(provider, endpoint));
        group.object_count += 1;
        if !job_id.trim().is_empty() {
            group.job_ids.insert(job_id);
        }
    }

    let mut entries = groups.into_values().collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        format!("{}\t{}", left.provider, left.endpoint)
            .cmp(&format!("{}\t{}", right.provider, right.endpoint))
    });

    let mut promotion_groups = Vec::new();
    let mut deferred_groups = Vec::new();
    for group in entries {
        if group.provider == PROMOTABLE_PROVIDER && group.endpoint == PROMOTABLE_ENDPOINT {
            promotion_groups.push(json!({
                "provider": group.provider,
                "endpoint": group.endpoint,
                "target_contract": PROMOTABLE_TARGET_CONTRACT,
                "transformer": PROMOTABLE_TRANSFORMER,
                "source_format": "r2_bronze_json",
                "object_count": group.object_count,
                "job_count": group.job_ids.len(),
                "status": "planned",
            }));
        } else {
            let reason = if group.provider == "data.go.kr" && group.endpoint == "getBrTitleInfo" {
                "missing_silver_contract"
            } else {
                "unsupported_provider_endpoint"
            };
            deferred_groups.push(json!({
                "provider": group.provider,
                "endpoint": group.endpoint,
                "reason": reason,
                "required_next_action": "define_silver_contract_and_transformer",
                "object_count": group.object_count,
                "job_count": group.job_ids.len(),
                "status": "deferred",
            }));
        }
    }

    let promotable_object_count = promotion_groups
        .iter()
        .map(|group| long_property(group, "object_count"))
        .sum::<i64>();
    let deferred_object_count = deferred_groups
        .iter()
        .map(|group| long_property(group, "object_count"))
        .sum::<i64>();
    let shards = new_shards(promotable_object_count, config.max_objects_per_shard);

    json!({
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now(),
        "git_head": git_head(&config.root),
        "status": "ready",
        "execution_model": "manifest_filtered_streaming",
        "local_full_download_allowed": false,
        "bronze_object_manifest": {
            "report_path": repo_relative_path(&config.root, &config.manifest_report_path),
            "manifest_path": repo_relative_path(&config.root, &config.manifest_path),
            "object_count": expected_object_count,
        },
        "summary": {
            "total_object_count": expected_object_count,
            "promotable_object_count": promotable_object_count,
            "deferred_object_count": deferred_object_count,
            "provider_group_count": promotion_groups.len() + deferred_groups.len(),
            "promotion_group_count": promotion_groups.len(),
            "deferred_group_count": deferred_groups.len(),
            "shard_count": shards.len(),
            "max_objects_per_shard": config.max_objects_per_shard,
        },
        "promotion_groups": promotion_groups,
        "deferred_provider_groups": deferred_groups,
        "shards": shards,
        "full_promotion_allowed": false,
        "completion_claim_allowed": false,
        "production_cutover_allowed": false,
        "national_rollout_allowed": false,
        "evidence_limitations": [
            "promotion_plan_only",
            "does_not_download_or_transform_r2_objects",
            "does_not_promote_silver_gold_national_tables",
            "does_not_rebuild_postgis_anchor_or_pbf",
            "does_not_approve_production_cutover",
        ],
        "next_gates": ["silver-gold-national-promotion-executor"],
    })
}

fn new_shards(object_count: i64, max_objects: i64) -> Vec<JsonValue> {
    let mut shards = Vec::new();
    let mut sequence = 1_i64;
    let mut start = 1_i64;
    while start <= object_count {
        let count = max_objects.min(object_count - start + 1);
        let end = start + count - 1;
        shards.push(json!({
            "shard_id": format!("silver-parcel-boundaries-vworld-{sequence:04}"),
            "sequence": sequence,
            "status": "planned",
            "provider": PROMOTABLE_PROVIDER,
            "endpoint": PROMOTABLE_ENDPOINT,
            "source_manifest_filter": {
                "provider": PROMOTABLE_PROVIDER,
                "endpoint": PROMOTABLE_ENDPOINT,
            },
            "target_contract": PROMOTABLE_TARGET_CONTRACT,
            "transformer": PROMOTABLE_TRANSFORMER,
            "execution_model": "manifest_filtered_streaming",
            "filtered_manifest_start_index": start,
            "filtered_manifest_end_index": end,
            "object_count": count,
            "retry_policy": "resume_by_shard_id",
        }));
        start = end + 1;
        sequence += 1;
    }
    shards
}

fn write_skip_report(config: &CheckConfig, reason: &str) -> anyhow::Result<()> {
    let report = json!({
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now(),
        "status": "skipped",
        "bronze_object_manifest": {
            "report_path": repo_relative_path(&config.root, &config.manifest_report_path),
            "manifest_path": repo_relative_path(&config.root, &config.manifest_path),
        },
        "execution_model": "manifest_filtered_streaming",
        "local_full_download_allowed": false,
        "summary": {
            "total_object_count": 0,
            "promotable_object_count": 0,
            "deferred_object_count": 0,
            "shard_count": 0,
        },
        "promotion_groups": [],
        "deferred_provider_groups": [],
        "shards": [],
        "blockers": [reason],
        "completion_claim_allowed": false,
        "production_cutover_allowed": false,
        "national_rollout_allowed": false,
        "full_promotion_allowed": false,
        "evidence_limitations": ["promotion_plan_not_evaluated"],
        "next_gates": ["silver-gold-national-promotion-executor"],
    });
    write_json_file(&config.plan_path, &report)
}

fn validate_manifest_report(manifest_report: &JsonValue, blockers: &mut Vec<String>) {
    add_if(
        blockers,
        string_property(manifest_report, "schema_version") != MANIFEST_REPORT_SCHEMA_VERSION,
        "Bronze object manifest report schema mismatch",
    );
    add_if(
        blockers,
        string_property(manifest_report, "status") != "ready",
        "Bronze object manifest report status must be ready",
    );
    add_if(
        blockers,
        bool_property(manifest_report, "completion_claim_allowed", true),
        "Bronze object manifest completion_claim_allowed must be false",
    );
    add_if(
        blockers,
        bool_property(manifest_report, "production_cutover_allowed", true),
        "Bronze object manifest production_cutover_allowed must be false",
    );
    add_if(
        blockers,
        bool_property(manifest_report, "national_rollout_allowed", true),
        "Bronze object manifest national_rollout_allowed must be false",
    );
}

fn validate_plan_top_level(
    plan: &JsonValue,
    manifest_object_count: i64,
    total_object_count: i64,
    promotable_object_count: i64,
    deferred_object_count: i64,
    max_objects_per_shard: i64,
    blockers: &mut Vec<String>,
) {
    add_if(
        blockers,
        string_property(plan, "schema_version") != SCHEMA_VERSION,
        "promotion plan schema mismatch",
    );
    add_if(
        blockers,
        string_property(plan, "status") != "ready",
        "promotion plan status must be ready",
    );
    add_if(
        blockers,
        string_property(plan, "execution_model") != "manifest_filtered_streaming",
        "execution_model must be manifest_filtered_streaming",
    );
    add_if(
        blockers,
        bool_property(plan, "local_full_download_allowed", true),
        "local_full_download_allowed must be false",
    );
    add_if(
        blockers,
        bool_property(plan, "completion_claim_allowed", true),
        "completion_claim_allowed must be false",
    );
    add_if(
        blockers,
        bool_property(plan, "production_cutover_allowed", true),
        "production_cutover_allowed must be false",
    );
    add_if(
        blockers,
        bool_property(plan, "national_rollout_allowed", true),
        "national_rollout_allowed must be false",
    );
    add_if(
        blockers,
        total_object_count != manifest_object_count,
        "plan total_object_count must match manifest object_count",
    );
    add_if(
        blockers,
        promotable_object_count + deferred_object_count != total_object_count,
        "promotable plus deferred object count must match total",
    );
    add_if(
        blockers,
        max_objects_per_shard < 1,
        "max_objects_per_shard must be positive",
    );
}

fn validate_promotion_groups(groups: &[JsonValue], blockers: &mut Vec<String>) -> i64 {
    let mut object_sum = 0_i64;
    for group in groups {
        let provider = string_property(group, "provider");
        let endpoint = string_property(group, "endpoint");
        let target_contract = string_property(group, "target_contract");
        let transformer = string_property(group, "transformer");
        let object_count = long_property(group, "object_count");
        add_if(
            blockers,
            provider != PROMOTABLE_PROVIDER || endpoint != PROMOTABLE_ENDPOINT,
            "only VWorld cadastral group is promotable in this plan",
        );
        add_if(
            blockers,
            target_contract != PROMOTABLE_TARGET_CONTRACT,
            "VWorld cadastral target_contract must be silver.parcel_boundaries",
        );
        add_if(
            blockers,
            transformer != PROMOTABLE_TRANSFORMER,
            "VWorld cadastral transformer mismatch",
        );
        add_if(
            blockers,
            object_count < 1,
            "promotion group object_count must be positive",
        );
        object_sum += object_count;
    }
    object_sum
}

fn validate_deferred_groups(groups: &[JsonValue], blockers: &mut Vec<String>) -> i64 {
    let mut object_sum = 0_i64;
    for group in groups {
        let provider = string_property(group, "provider");
        let endpoint = string_property(group, "endpoint");
        let reason = string_property(group, "reason");
        let object_count = long_property(group, "object_count");
        add_if(
            blockers,
            provider.trim().is_empty(),
            "deferred group provider is required",
        );
        add_if(
            blockers,
            endpoint.trim().is_empty(),
            "deferred group endpoint is required",
        );
        add_if(
            blockers,
            provider == "data.go.kr"
                && endpoint == "getBrTitleInfo"
                && reason != "missing_silver_contract",
            "data.go.kr building register must be deferred by missing_silver_contract",
        );
        add_if(
            blockers,
            object_count < 1,
            "deferred group object_count must be positive",
        );
        object_sum += object_count;
    }
    object_sum
}

fn validate_shards(
    shards: &[JsonValue],
    max_objects_per_shard: i64,
    promotable_object_count: i64,
    blockers: &mut Vec<String>,
) -> i64 {
    let mut next_expected_start = 1_i64;
    let mut object_sum = 0_i64;
    let mut shard_ids = HashSet::new();
    for shard in shards {
        let shard_id = string_property(shard, "shard_id");
        let target_contract = string_property(shard, "target_contract");
        let start_index = long_property(shard, "filtered_manifest_start_index");
        let end_index = long_property(shard, "filtered_manifest_end_index");
        let object_count = long_property(shard, "object_count");
        add_if(
            blockers,
            !shard_ids.insert(shard_id.clone()),
            &format!("duplicate shard_id: {shard_id}"),
        );
        add_if(
            blockers,
            target_contract != PROMOTABLE_TARGET_CONTRACT,
            "shard target_contract must be silver.parcel_boundaries",
        );
        add_if(
            blockers,
            object_count < 1,
            "shard object_count must be positive",
        );
        add_if(
            blockers,
            object_count > max_objects_per_shard,
            "shard object_count must not exceed max_objects_per_shard",
        );
        add_if(
            blockers,
            start_index != next_expected_start,
            "shard filtered indexes must be contiguous",
        );
        add_if(
            blockers,
            end_index < start_index,
            "shard end index must be greater than or equal to start index",
        );
        add_if(
            blockers,
            end_index - start_index + 1 != object_count,
            "shard object_count must match filtered index span",
        );
        next_expected_start = end_index + 1;
        object_sum += object_count;
    }
    if promotable_object_count == 0 {
        add_if(
            blockers,
            next_expected_start != 1,
            "shard filtered indexes must be contiguous",
        );
    }
    object_sum
}

fn read_manifest_rows_for_writer(path: &Path) -> anyhow::Result<Vec<JsonValue>> {
    let mut rows = Vec::new();
    for (index, line) in read_text(path, "Bronze object manifest")?
        .lines()
        .enumerate()
    {
        if line.trim().is_empty() {
            continue;
        }
        let row = serde_json::from_str::<JsonValue>(line).with_context(|| {
            format!(
                "Bronze object manifest line {} is not valid JSON",
                index + 1
            )
        })?;
        if string_property(&row, "schema_version") != MANIFEST_ENTRY_SCHEMA_VERSION {
            bail!("Bronze object manifest entry schema mismatch");
        }
        rows.push(row);
    }
    Ok(rows)
}

fn read_manifest_rows_for_check(
    path: &Path,
    blockers: &mut Vec<String>,
) -> anyhow::Result<Vec<JsonValue>> {
    let mut rows = Vec::new();
    for (index, line) in read_text(path, "Bronze object manifest")?
        .lines()
        .enumerate()
    {
        let line_number = index + 1;
        if line.trim().is_empty() {
            blockers.push(format!(
                "Bronze object manifest line {line_number} must not be blank"
            ));
            continue;
        }
        match serde_json::from_str::<JsonValue>(line) {
            Ok(row) => {
                add_if(
                    blockers,
                    string_property(&row, "schema_version") != MANIFEST_ENTRY_SCHEMA_VERSION,
                    "Bronze object manifest entry schema mismatch",
                );
                rows.push(row);
            }
            Err(_) => blockers.push(format!(
                "Bronze object manifest line {line_number} is not valid JSON"
            )),
        }
    }
    Ok(rows)
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
    value
        .as_array()
        .map(|values| values.to_vec())
        .unwrap_or_default()
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

struct GroupAccumulator {
    provider: String,
    endpoint: String,
    job_ids: HashSet<String>,
    object_count: i64,
}

impl GroupAccumulator {
    fn new(provider: String, endpoint: String) -> Self {
        Self {
            provider,
            endpoint,
            job_ids: HashSet::new(),
            object_count: 0,
        }
    }
}
