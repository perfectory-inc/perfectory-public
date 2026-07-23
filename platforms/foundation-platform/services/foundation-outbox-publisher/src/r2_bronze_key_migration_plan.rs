use std::{
    collections::HashMap,
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

use crate::r2_command_support::{
    canonical_path, env_bool, env_path, normalize_windows_verbatim_path, read_json, resolve_path,
    utc_now, write_json_file,
};

const AUDIT_SCHEMA_VERSION: &str = "foundation-platform.r2_inventory_audit.v1";
const PLAN_SCHEMA_VERSION: &str = "foundation-platform.r2_bronze_key_migration_plan.v1";
const DEFAULT_AUDIT_PATH: &str = "target/r2-inventory-audit/r2-inventory-audit.json";
const DEFAULT_OUTPUT_DIR: &str = "target/r2-bronze-key-migration-plan";
const PLAN_FILE_NAME: &str = "r2-bronze-key-migration-plan.json";
const MIGRATION_STRATEGY: &str =
    "copy_legacy_date_partitioned_bronze_to_source_run_id_partition_key";
const DELETION_STRATEGY: &str = "delete_old_keys_only_after_canonical_copy_inventory_verification";
const PREFIX: &str = "FOUNDATION_PLATFORM_R2_BRONZE_KEY_MIGRATION_PLAN";

pub fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let audit = read_audit(&config.audit_path)?;
    let plan = build_plan(&config.audit_path, &audit)?;
    let plan_path = config.output_dir.join(PLAN_FILE_NAME);
    write_json_file(&plan_path, &plan)?;

    write_summary(config.quiet, &plan_path, &plan)?;
    Ok(())
}

struct Config {
    audit_path: PathBuf,
    output_dir: PathBuf,
    quiet: bool,
}

#[derive(Debug, Deserialize)]
struct InventoryAudit {
    schema_version: String,
    objects: Vec<InventoryObject>,
}

#[derive(Clone, Debug, Deserialize)]
struct InventoryObject {
    key: String,
    size_bytes: i64,
    last_modified: String,
}

#[derive(Debug, Serialize)]
struct MigrationPlan {
    schema_version: &'static str,
    generated_at_utc: String,
    status: &'static str,
    source_audit: String,
    migration_strategy: &'static str,
    deletion_strategy: &'static str,
    legacy_object_count: usize,
    legacy_total_size_bytes: i64,
    copy_required_count: usize,
    copy_required_total_size_bytes: i64,
    already_canonicalized_count: usize,
    target_size_conflict_count: usize,
    duplicate_target_review_count: usize,
    objects: Vec<MigrationObject>,
}

#[derive(Clone, Debug, Serialize)]
struct MigrationObject {
    old_key: String,
    new_key: String,
    source: String,
    ingest_date: String,
    size_bytes: i64,
    last_modified: String,
    status: &'static str,
    reason: &'static str,
}

struct MigrationTarget {
    source: String,
    ingest_date: String,
    new_key: String,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );
        let audit_path = env_path(&format!("{PREFIX}_AUDIT_PATH"), DEFAULT_AUDIT_PATH)?;
        let output_dir = env_path(&format!("{PREFIX}_OUTPUT_DIR"), DEFAULT_OUTPUT_DIR)?;

        Ok(Self {
            audit_path: resolve_path(&root, &audit_path),
            output_dir: resolve_path(&root, &output_dir),
            quiet: env_bool(&format!("{PREFIX}_QUIET"), false)?,
        })
    }
}

fn read_audit(path: &Path) -> anyhow::Result<InventoryAudit> {
    if !path.is_file() {
        bail!("AuditPath not found: {}", path.display());
    }
    let audit: InventoryAudit = serde_json::from_value(read_json(path, "R2 inventory audit")?)
        .context("failed to parse R2 inventory audit")?;
    if audit.schema_version != AUDIT_SCHEMA_VERSION {
        bail!("Unsupported audit schema_version: {}", audit.schema_version);
    }
    Ok(audit)
}

fn build_plan(audit_path: &Path, audit: &InventoryAudit) -> anyhow::Result<MigrationPlan> {
    let mut object_by_key = HashMap::new();
    for object in &audit.objects {
        if object.key.trim().is_empty() {
            bail!("Audit object omitted key.");
        }
        object_by_key.insert(object.key.clone(), object.clone());
    }

    let mut legacy_rows = Vec::new();
    for object in &audit.objects {
        let Some(target) = migration_target(&object.key) else {
            continue;
        };
        legacy_rows.push(MigrationObject {
            old_key: object.key.clone(),
            new_key: target.new_key,
            source: target.source,
            ingest_date: target.ingest_date,
            size_bytes: object.size_bytes,
            last_modified: object.last_modified.clone(),
            status: "copy_required",
            reason: "Canonical target key is absent.",
        });
    }

    let mut target_counts = HashMap::<String, usize>::new();
    for row in &legacy_rows {
        *target_counts.entry(row.new_key.clone()).or_default() += 1;
    }

    let mut objects = legacy_rows;
    objects.sort_by(|left, right| left.old_key.cmp(&right.old_key));
    for object in &mut objects {
        let target_count = target_counts.get(&object.new_key).copied().unwrap_or(0);
        if target_count > 1 {
            object.status = "duplicate_target_review";
            object.reason = "Multiple legacy objects map to the same canonical target key.";
            continue;
        }
        if let Some(target_object) = object_by_key.get(&object.new_key) {
            if target_object.size_bytes == object.size_bytes {
                object.status = "already_canonicalized";
                object.reason = "Canonical target key already exists with the same size.";
            } else {
                object.status = "target_size_conflict";
                object.reason = "Canonical target key already exists with a different size.";
            }
        }
    }

    let copy_required_count = count_status(&objects, "copy_required");
    let already_canonicalized_count = count_status(&objects, "already_canonicalized");
    let target_size_conflict_count = count_status(&objects, "target_size_conflict");
    let duplicate_target_review_count = count_status(&objects, "duplicate_target_review");
    let blocked_count = target_size_conflict_count + duplicate_target_review_count;

    Ok(MigrationPlan {
        schema_version: PLAN_SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        status: if blocked_count == 0 {
            "ready"
        } else {
            "blocked"
        },
        source_audit: canonical_path(audit_path),
        migration_strategy: MIGRATION_STRATEGY,
        deletion_strategy: DELETION_STRATEGY,
        legacy_object_count: objects.len(),
        legacy_total_size_bytes: size_sum(&objects),
        copy_required_count,
        copy_required_total_size_bytes: objects
            .iter()
            .filter(|object| object.status == "copy_required")
            .map(|object| object.size_bytes)
            .sum(),
        already_canonicalized_count,
        target_size_conflict_count,
        duplicate_target_review_count,
        objects,
    })
}

fn migration_target(key: &str) -> Option<MigrationTarget> {
    let rest = key.strip_prefix("bronze/source=")?;
    let (source, rest) = rest.split_once("/ingest_date=")?;
    let ingest_date = rest.get(..10)?;
    if !valid_date(ingest_date) {
        return None;
    }
    let rest = rest.get(10..)?;
    let tail = rest.strip_prefix('/')?;
    if !tail.starts_with("run_id=") || !tail.contains("/partition=") {
        return None;
    }
    Some(MigrationTarget {
        source: source.to_owned(),
        ingest_date: ingest_date.to_owned(),
        new_key: format!("bronze/source={source}/{tail}"),
    })
}

fn valid_date(value: &str) -> bool {
    value.len() == 10
        && value.as_bytes()[4] == b'-'
        && value.as_bytes()[7] == b'-'
        && value
            .bytes()
            .enumerate()
            .all(|(index, byte)| index == 4 || index == 7 || byte.is_ascii_digit())
}

fn count_status(objects: &[MigrationObject], status: &str) -> usize {
    objects
        .iter()
        .filter(|object| object.status == status)
        .count()
}

fn size_sum(objects: &[MigrationObject]) -> i64 {
    objects.iter().map(|object| object.size_bytes).sum()
}

fn write_summary(quiet: bool, plan_path: &Path, plan: &MigrationPlan) -> anyhow::Result<()> {
    if quiet {
        return Ok(());
    }

    let mut stdout = io::stdout().lock();
    writeln!(stdout, "R2 Bronze key migration plan wrote:")?;
    writeln!(stdout, "  plan: {}", plan_path.display())?;
    writeln!(stdout, "Summary:")?;
    writeln!(stdout, "  status: {}", plan.status)?;
    writeln!(stdout, "  legacy objects: {}", plan.legacy_object_count)?;
    writeln!(stdout, "  copy_required: {}", plan.copy_required_count)?;
    writeln!(
        stdout,
        "  already_canonicalized: {}",
        plan.already_canonicalized_count
    )?;
    writeln!(
        stdout,
        "  target_size_conflict: {}",
        plan.target_size_conflict_count
    )?;
    writeln!(
        stdout,
        "  duplicate_target_review: {}",
        plan.duplicate_target_review_count
    )?;
    Ok(())
}
