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
const DELETE_PLAN_SCHEMA_VERSION: &str = "foundation-platform.r2_delete_plan.v1";
const VERIFICATION_SCHEMA_VERSION: &str = "foundation-platform.r2_cleanup_verification.v1";
const DEFAULT_BEFORE_AUDIT_PATH: &str = "target/r2-inventory-audit/r2-inventory-audit.json";
const DEFAULT_AFTER_AUDIT_PATH: &str =
    "target/r2-inventory-audit-after-cleanup/r2-inventory-audit.json";
const DEFAULT_DELETE_PLAN_PATH: &str = "target/r2-delete-candidates/r2-delete-plan.json";
const DEFAULT_OUTPUT_DIR: &str = "target/r2-cleanup-verify";
const REPORT_FILE_NAME: &str = "r2-cleanup-verification.json";
const PREFIX: &str = "FOUNDATION_PLATFORM_R2_CLEANUP_VERIFY";

pub fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let before_audit = read_audit(&config.before_audit_path, "BeforeAuditPath")?;
    let after_audit = read_audit(&config.after_audit_path, "AfterAuditPath")?;
    let delete_plan = read_delete_plan(&config.delete_plan_path)?;
    let verification = verify_cleanup(&config, &before_audit, &after_audit, &delete_plan)?;
    let verification_path = config.output_dir.join(REPORT_FILE_NAME);
    write_json_file(&verification_path, &verification)?;

    write_summary(config.quiet, &verification_path, &verification)?;
    Ok(())
}

struct Config {
    before_audit_path: PathBuf,
    after_audit_path: PathBuf,
    delete_plan_path: PathBuf,
    output_dir: PathBuf,
    quiet: bool,
}

#[derive(Debug, Deserialize)]
struct InventoryAudit {
    schema_version: String,
    review_count: i64,
    objects: Vec<InventoryObject>,
}

#[derive(Clone, Debug, Deserialize)]
struct InventoryObject {
    key: String,
    size_bytes: i64,
    action: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DeletePlan {
    schema_version: String,
    mode: String,
    object_count: i64,
    executed_count: i64,
    objects: Vec<DeletePlanObject>,
}

#[derive(Debug, Deserialize)]
struct DeletePlanObject {
    key: String,
}

#[derive(Debug, Serialize)]
struct CleanupVerification {
    schema_version: &'static str,
    generated_at_utc: String,
    status: &'static str,
    before_audit: String,
    after_audit: String,
    delete_plan: String,
    deleted_candidate_count: usize,
    preserved_keep_count: usize,
    missing_keep_count: usize,
    changed_keep_count: usize,
    remaining_deleted_candidate_count: usize,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );
        let before_audit_path = env_path(
            &format!("{PREFIX}_BEFORE_AUDIT_PATH"),
            DEFAULT_BEFORE_AUDIT_PATH,
        )?;
        let after_audit_path = env_path(
            &format!("{PREFIX}_AFTER_AUDIT_PATH"),
            DEFAULT_AFTER_AUDIT_PATH,
        )?;
        let delete_plan_path = env_path(
            &format!("{PREFIX}_DELETE_PLAN_PATH"),
            DEFAULT_DELETE_PLAN_PATH,
        )?;
        let output_dir = env_path(&format!("{PREFIX}_OUTPUT_DIR"), DEFAULT_OUTPUT_DIR)?;

        Ok(Self {
            before_audit_path: resolve_path(&root, &before_audit_path),
            after_audit_path: resolve_path(&root, &after_audit_path),
            delete_plan_path: resolve_path(&root, &delete_plan_path),
            output_dir: resolve_path(&root, &output_dir),
            quiet: env_bool(&format!("{PREFIX}_QUIET"), false)?,
        })
    }
}

fn read_audit(path: &Path, label: &str) -> anyhow::Result<InventoryAudit> {
    if !path.is_file() {
        bail!("{label} not found: {}", path.display());
    }
    let audit: InventoryAudit = serde_json::from_value(read_json(path, label)?)
        .with_context(|| format!("failed to parse {label} {}", path.display()))?;
    if audit.schema_version != AUDIT_SCHEMA_VERSION {
        bail!(
            "{} has unsupported schema_version: {}",
            label.replace("Path", " audit"),
            audit.schema_version
        );
    }
    Ok(audit)
}

fn read_delete_plan(path: &Path) -> anyhow::Result<DeletePlan> {
    if !path.is_file() {
        bail!("DeletePlanPath not found: {}", path.display());
    }
    let plan: DeletePlan = serde_json::from_value(read_json(path, "DeletePlanPath")?)
        .with_context(|| format!("failed to parse DeletePlanPath {}", path.display()))?;
    if plan.schema_version != DELETE_PLAN_SCHEMA_VERSION {
        bail!(
            "Delete plan has unsupported schema_version: {}",
            plan.schema_version
        );
    }
    if plan.mode != "execute" {
        bail!("Delete plan must be execute mode before cleanup verification.");
    }
    if plan.executed_count != plan.object_count {
        bail!("Delete plan executed_count must equal object_count before cleanup verification.");
    }
    Ok(plan)
}

fn verify_cleanup(
    config: &Config,
    before_audit: &InventoryAudit,
    after_audit: &InventoryAudit,
    delete_plan: &DeletePlan,
) -> anyhow::Result<CleanupVerification> {
    let after_map = build_object_map(&after_audit.objects)?;
    let keep_objects = keep_objects(before_audit);

    let mut missing_keep = Vec::new();
    let mut changed_keep = Vec::new();
    for object in &keep_objects {
        let Some(after_object) = after_map.get(&object.key) else {
            missing_keep.push(object.key.clone());
            continue;
        };
        if after_object.size_bytes != object.size_bytes {
            changed_keep.push(ChangedKeepObject {
                key: object.key.clone(),
            });
        }
    }

    let mut remaining_deleted_candidates = Vec::new();
    for object in &delete_plan.objects {
        if after_map.contains_key(&object.key) {
            remaining_deleted_candidates.push(object.key.clone());
        }
    }

    if !missing_keep.is_empty() {
        bail!(
            "cleanup verification failed: keep object disappeared: {}",
            missing_keep.join(", ")
        );
    }
    if let Some(object) = changed_keep.first() {
        bail!(
            "cleanup verification failed: keep object size changed: {}",
            object.key
        );
    }
    if !remaining_deleted_candidates.is_empty() {
        bail!(
            "cleanup verification failed: delete candidate still exists: {}",
            remaining_deleted_candidates.join(", ")
        );
    }
    if after_audit.review_count != 0 {
        bail!("cleanup verification failed: after audit still has review objects.");
    }

    Ok(CleanupVerification {
        schema_version: VERIFICATION_SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        status: "passed",
        before_audit: canonical_path(&config.before_audit_path),
        after_audit: canonical_path(&config.after_audit_path),
        delete_plan: canonical_path(&config.delete_plan_path),
        deleted_candidate_count: delete_plan.objects.len(),
        preserved_keep_count: keep_objects.len(),
        missing_keep_count: 0,
        changed_keep_count: 0,
        remaining_deleted_candidate_count: 0,
    })
}

struct ChangedKeepObject {
    key: String,
}

fn build_object_map(
    objects: &[InventoryObject],
) -> anyhow::Result<HashMap<String, InventoryObject>> {
    let mut map = HashMap::new();
    for object in objects {
        if object.key.trim().is_empty() {
            bail!("Object entry omitted key.");
        }
        if map.insert(object.key.clone(), object.clone()).is_some() {
            bail!("Object entry duplicated key: {}", object.key);
        }
    }
    Ok(map)
}

fn keep_objects(audit: &InventoryAudit) -> Vec<InventoryObject> {
    audit
        .objects
        .iter()
        .filter(|object| object.action.as_deref() == Some("keep"))
        .cloned()
        .collect()
}

fn write_summary(
    quiet: bool,
    verification_path: &Path,
    verification: &CleanupVerification,
) -> anyhow::Result<()> {
    if quiet {
        return Ok(());
    }

    let mut stdout = io::stdout().lock();
    writeln!(stdout, "R2 cleanup verification wrote:")?;
    writeln!(stdout, "  report: {}", verification_path.display())?;
    writeln!(stdout, "Summary:")?;
    writeln!(stdout, "  status: {}", verification.status)?;
    writeln!(
        stdout,
        "  deleted candidates verified absent: {}",
        verification.deleted_candidate_count
    )?;
    writeln!(
        stdout,
        "  keep objects preserved: {}",
        verification.preserved_keep_count
    )?;
    Ok(())
}
