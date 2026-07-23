use std::{
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

const PLAN_SCHEMA_VERSION: &str = "foundation-platform.r2_bronze_key_migration_plan.v1";
const MANIFEST_SCHEMA_VERSION: &str = "foundation-platform.r2_delete_candidates.v1";
const DEFAULT_PLAN_PATH: &str =
    "target/r2-bronze-key-migration-plan/r2-bronze-key-migration-plan.json";
const DEFAULT_OUTPUT_DIR: &str = "target/r2-bronze-key-cleanup-candidates";
const MANIFEST_FILE_NAME: &str = "r2-delete-candidates.json";
const PREFIX: &str = "FOUNDATION_PLATFORM_R2_BRONZE_KEY_CLEANUP_CANDIDATES";

pub fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let plan = read_plan(&config.plan_path)?;
    let manifest = build_manifest(&config.plan_path, &plan)?;
    let manifest_path = config.output_dir.join(MANIFEST_FILE_NAME);
    write_json_file(&manifest_path, &manifest)?;

    write_summary(config.quiet, &manifest_path, &manifest)?;
    Ok(())
}

struct Config {
    plan_path: PathBuf,
    output_dir: PathBuf,
    quiet: bool,
}

#[derive(Debug, Deserialize)]
struct MigrationPlan {
    schema_version: String,
    status: String,
    copy_required_count: i64,
    target_size_conflict_count: i64,
    duplicate_target_review_count: i64,
    objects: Vec<MigrationRow>,
}

#[derive(Debug, Deserialize)]
struct MigrationRow {
    old_key: String,
    new_key: String,
    size_bytes: i64,
    status: String,
}

#[derive(Debug, Serialize)]
struct DeleteCandidateManifest {
    schema_version: &'static str,
    generated_at_utc: String,
    source_plan: String,
    object_count: usize,
    total_size_bytes: i64,
    objects: Vec<DeleteCandidate>,
}

#[derive(Debug, Serialize)]
struct DeleteCandidate {
    key: String,
    canonical_key: String,
    size_bytes: i64,
    classification: &'static str,
    action: &'static str,
    reason: &'static str,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );
        let plan_path = env_path(&format!("{PREFIX}_PLAN_PATH"), DEFAULT_PLAN_PATH)?;
        let output_dir = env_path(&format!("{PREFIX}_OUTPUT_DIR"), DEFAULT_OUTPUT_DIR)?;

        Ok(Self {
            plan_path: resolve_path(&root, &plan_path),
            output_dir: resolve_path(&root, &output_dir),
            quiet: env_bool(&format!("{PREFIX}_QUIET"), false)?,
        })
    }
}

fn read_plan(path: &Path) -> anyhow::Result<MigrationPlan> {
    if !path.is_file() {
        bail!("PlanPath not found: {}", path.display());
    }
    let plan: MigrationPlan =
        serde_json::from_value(read_json(path, "R2 Bronze key migration plan")?)
            .context("failed to parse R2 Bronze key migration plan")?;
    if plan.schema_version != PLAN_SCHEMA_VERSION {
        bail!(
            "Unsupported migration plan schema_version: {}",
            plan.schema_version
        );
    }
    if plan.status != "ready" {
        bail!("Migration plan must be ready before cleanup candidates are generated.");
    }
    Ok(plan)
}

fn build_manifest(
    plan_path: &Path,
    plan: &MigrationPlan,
) -> anyhow::Result<DeleteCandidateManifest> {
    assert_cleanup_ready(plan)?;

    let mut objects = Vec::with_capacity(plan.objects.len());
    for row in &plan.objects {
        assert_migration_row(row)?;
        objects.push(DeleteCandidate {
            key: row.old_key.clone(),
            canonical_key: row.new_key.clone(),
            size_bytes: row.size_bytes,
            classification: "legacy_date_partitioned_bronze",
            action: "delete_candidate",
            reason: "Canonical copy was verified by R2 inventory.",
        });
    }

    Ok(DeleteCandidateManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        source_plan: canonical_path(plan_path),
        object_count: objects.len(),
        total_size_bytes: objects.iter().map(|object| object.size_bytes).sum(),
        objects,
    })
}

fn assert_cleanup_ready(plan: &MigrationPlan) -> anyhow::Result<()> {
    if plan.copy_required_count != 0 {
        bail!("Migration cleanup requires copy_required_count=0.");
    }
    if plan.target_size_conflict_count != 0 {
        bail!("Migration cleanup requires target_size_conflict_count=0.");
    }
    if plan.duplicate_target_review_count != 0 {
        bail!("Migration cleanup requires duplicate_target_review_count=0.");
    }
    Ok(())
}

fn assert_migration_row(row: &MigrationRow) -> anyhow::Result<()> {
    if row.status != "already_canonicalized" {
        bail!(
            "Cleanup candidate row must be already_canonicalized: {}",
            row.old_key
        );
    }
    if row.old_key.trim().is_empty() || row.new_key.trim().is_empty() {
        bail!("Cleanup candidate row must include old_key and new_key.");
    }
    assert_safe_key("old_key", &row.old_key)?;
    assert_safe_key("new_key", &row.new_key)?;
    if !is_legacy_date_partitioned_bronze_key(&row.old_key) {
        bail!(
            "Cleanup candidate old_key must be legacy date-partitioned Bronze: {}",
            row.old_key
        );
    }
    if row.new_key.contains("/ingest_date=") {
        bail!(
            "Cleanup candidate new_key must not contain ingest_date: {}",
            row.new_key
        );
    }
    Ok(())
}

fn assert_safe_key(label: &str, key: &str) -> anyhow::Result<()> {
    if key.starts_with('/') || key.contains("..") || key.contains('\\') {
        bail!("Cleanup candidate {label} is unsafe: {key}");
    }
    Ok(())
}

fn is_legacy_date_partitioned_bronze_key(key: &str) -> bool {
    let Some(rest) = key.strip_prefix("bronze/source=") else {
        return false;
    };
    let Some((source, rest)) = rest.split_once("/ingest_date=") else {
        return false;
    };
    if source.is_empty() || source.contains('/') {
        return false;
    }
    let Some(date) = rest.get(..10) else {
        return false;
    };
    if !valid_date(date) {
        return false;
    }
    let Some(tail) = rest.get(10..).and_then(|value| value.strip_prefix('/')) else {
        return false;
    };
    tail.starts_with("run_id=") && tail.contains("/partition=")
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

fn write_summary(
    quiet: bool,
    manifest_path: &Path,
    manifest: &DeleteCandidateManifest,
) -> anyhow::Result<()> {
    if quiet {
        return Ok(());
    }

    let mut stdout = io::stdout().lock();
    writeln!(stdout, "R2 Bronze cleanup candidates wrote:")?;
    writeln!(stdout, "  manifest: {}", manifest_path.display())?;
    writeln!(stdout, "Summary:")?;
    writeln!(stdout, "  objects: {}", manifest.object_count)?;
    writeln!(stdout, "  bytes: {}", manifest.total_size_bytes)?;
    Ok(())
}
