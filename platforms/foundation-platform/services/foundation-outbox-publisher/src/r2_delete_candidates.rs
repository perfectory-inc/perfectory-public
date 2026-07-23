use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use foundation_outbox::{PublishError, R2ObjectStorage};
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::r2_command_support::{
    canonical_path, env_bool, env_path, normalize_windows_verbatim_path, optional_env,
    r2_config_from_env_file, read_json, resolve_path, utc_now, write_json_file,
};
use crate::r2_layout::{
    PARCEL_MARKER_ANCHOR_ARTIFACT_ROOT, VECTOR_TILE_ARTIFACT_ROOT, VECTOR_TILE_MANIFEST_ROOT,
};

const MANIFEST_SCHEMA_VERSION: &str = "foundation-platform.r2_delete_candidates.v1";
const PLAN_SCHEMA_VERSION: &str = "foundation-platform.r2_delete_plan.v1";
const REQUIRED_CONFIRM_PHRASE: &str = "DELETE FOUNDATION PLATFORM R2 CANDIDATES";
const DEFAULT_MANIFEST_PATH: &str = "target/r2-inventory-audit/r2-delete-candidates.json";
const DEFAULT_OUTPUT_DIR: &str = "target/r2-delete-candidates";
const DEFAULT_ENV_FILE: &str = ".env.local";
const PLAN_FILE_NAME: &str = "r2-delete-plan.json";
const PREFIX: &str = "FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES";
const DEFAULT_DELETE_CONCURRENCY: usize = 64;
const MAX_DELETE_CONCURRENCY: usize = 256;

pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let manifest = read_manifest(&config.manifest_path)?;
    let mut plan = build_delete_plan(&config, &manifest)?;

    if config.execute {
        plan.executed_count = execute_deletes(&config, &plan.objects).await?;
    }

    let plan_path = config.output_dir.join(PLAN_FILE_NAME);
    write_json_file(&plan_path, &plan)?;

    write_summary(config.quiet, &plan_path, &plan)?;
    Ok(())
}

struct Config {
    manifest_path: PathBuf,
    output_dir: PathBuf,
    env_file: PathBuf,
    allowed_prefixes: Vec<String>,
    execute: bool,
    concurrency: usize,
    quiet: bool,
}

#[derive(Debug, Deserialize)]
struct DeleteCandidateManifest {
    schema_version: String,
    objects: Vec<DeleteCandidate>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DeleteCandidate {
    key: String,
    size_bytes: i64,
    classification: String,
    action: String,
    reason: String,
}

#[derive(Debug, Serialize)]
struct DeletePlan {
    schema_version: &'static str,
    generated_at_utc: String,
    mode: &'static str,
    source_manifest: String,
    allowed_prefixes: Vec<String>,
    object_count: usize,
    total_size_bytes: i64,
    executed_count: usize,
    objects: Vec<DeletePlanObject>,
}

#[derive(Clone, Debug, Serialize)]
struct DeletePlanObject {
    key: String,
    size_bytes: i64,
    classification: String,
    action: String,
    reason: String,
    would_delete: bool,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );
        let manifest_path = env_path(&format!("{PREFIX}_MANIFEST_PATH"), DEFAULT_MANIFEST_PATH)?;
        let output_dir = env_path(&format!("{PREFIX}_OUTPUT_DIR"), DEFAULT_OUTPUT_DIR)?;
        let env_file = env_path(&format!("{PREFIX}_ENV_FILE"), DEFAULT_ENV_FILE)?;
        let allowed_prefixes = normalize_allowed_prefixes(
            optional_env(&format!("{PREFIX}_ALLOWED_PREFIXES"))?
                .as_deref()
                .unwrap_or_default(),
        )?;
        let execute = env_bool(&format!("{PREFIX}_EXECUTE"), false)?;
        let concurrency = optional_env(&format!("{PREFIX}_CONCURRENCY"))?
            .map(|value| parse_concurrency(&value))
            .transpose()?
            .unwrap_or(DEFAULT_DELETE_CONCURRENCY);

        if execute {
            let phrase = optional_env(&format!("{PREFIX}_CONFIRM_PHRASE"))?.unwrap_or_default();
            if phrase != REQUIRED_CONFIRM_PHRASE {
                bail!("ConfirmPhrase must exactly equal '{REQUIRED_CONFIRM_PHRASE}' when -Execute is used.");
            }
            if allowed_prefixes.is_empty() {
                bail!("At least one AllowedPrefix is required when -Execute is used.");
            }
        }

        Ok(Self {
            manifest_path: resolve_path(&root, &manifest_path),
            output_dir: resolve_path(&root, &output_dir),
            env_file: resolve_path(&root, &env_file),
            allowed_prefixes,
            execute,
            concurrency,
            quiet: env_bool(&format!("{PREFIX}_QUIET"), false)?,
        })
    }
}

fn read_manifest(path: &Path) -> anyhow::Result<DeleteCandidateManifest> {
    if !path.is_file() {
        bail!("ManifestPath not found: {}", path.display());
    }
    let manifest: DeleteCandidateManifest =
        serde_json::from_value(read_json(path, "R2 delete candidate manifest")?)
            .context("failed to parse R2 delete candidate manifest")?;
    if manifest.schema_version != MANIFEST_SCHEMA_VERSION {
        bail!(
            "Unsupported delete candidate manifest schema_version: {}",
            manifest.schema_version
        );
    }
    Ok(manifest)
}

fn build_delete_plan(
    config: &Config,
    manifest: &DeleteCandidateManifest,
) -> anyhow::Result<DeletePlan> {
    let mut objects = Vec::with_capacity(manifest.objects.len());
    for object in &manifest.objects {
        assert_delete_candidate_object(object, &config.allowed_prefixes)?;
        objects.push(DeletePlanObject {
            key: object.key.clone(),
            size_bytes: object.size_bytes,
            classification: object.classification.clone(),
            action: object.action.clone(),
            reason: object.reason.clone(),
            would_delete: true,
        });
    }

    Ok(DeletePlan {
        schema_version: PLAN_SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        mode: if config.execute { "execute" } else { "dry_run" },
        source_manifest: canonical_path(&config.manifest_path),
        allowed_prefixes: config.allowed_prefixes.clone(),
        object_count: objects.len(),
        total_size_bytes: objects.iter().map(|object| object.size_bytes).sum(),
        executed_count: 0,
        objects,
    })
}

async fn execute_deletes(config: &Config, objects: &[DeletePlanObject]) -> anyhow::Result<usize> {
    if objects.is_empty() {
        return Ok(0);
    }

    let storage = R2ObjectStorage::from_config(r2_config_from_env_file(&config.env_file)?);
    let results: Vec<Result<(), PublishError>> =
        stream::iter(objects.iter().cloned().map(|object| {
            let storage = storage.clone();
            async move { storage.delete_object(&object.key).await }
        }))
        .buffer_unordered(config.concurrency)
        .collect()
        .await;

    let mut deleted = 0usize;
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok(()) => deleted += 1,
            Err(error) => errors.push(error.to_string()),
        }
    }
    if let Some(error) = errors.first() {
        bail!("R2 delete candidates failed: {error}");
    }
    Ok(deleted)
}

fn write_summary(quiet: bool, plan_path: &Path, plan: &DeletePlan) -> anyhow::Result<()> {
    if quiet {
        return Ok(());
    }

    let mut stdout = io::stdout().lock();
    writeln!(stdout, "R2 delete candidate plan wrote:")?;
    writeln!(stdout, "  plan: {}", plan_path.display())?;
    writeln!(stdout, "Summary:")?;
    writeln!(stdout, "  mode: {}", plan.mode)?;
    writeln!(stdout, "  objects: {}", plan.object_count)?;
    writeln!(stdout, "  bytes: {}", plan.total_size_bytes)?;
    writeln!(stdout, "  executed: {}", plan.executed_count)?;
    Ok(())
}

fn assert_delete_candidate_object(
    object: &DeleteCandidate,
    prefixes: &[String],
) -> anyhow::Result<()> {
    if object.action != "delete_candidate" {
        bail!(
            "Delete manifest contains non delete_candidate action for key: {}",
            object.key
        );
    }

    if object.key.trim().is_empty() {
        bail!("Delete manifest contains an empty key.");
    }
    if object.key.starts_with('/') || object.key.contains("..") || object.key.contains('\\') {
        bail!("Delete manifest contains an unsafe key: {}", object.key);
    }

    let is_legacy_date_partitioned_bronze = is_legacy_date_partitioned_bronze_key(&object.key);
    if is_legacy_date_partitioned_bronze
        && object.classification != "legacy_date_partitioned_bronze"
    {
        bail!(
            "Delete manifest contains legacy date-partitioned Bronze with invalid classification: {}",
            object.key
        );
    }
    if is_protected_key(&object.key, is_legacy_date_partitioned_bronze) {
        bail!(
            "Delete manifest contains a protected runtime/catalog key: {}",
            object.key
        );
    }

    if !prefixes.is_empty() && !prefixes.iter().any(|prefix| object.key.starts_with(prefix)) {
        bail!(
            "Delete candidate key is outside allowed prefixes: {}",
            object.key
        );
    }
    Ok(())
}

fn normalize_allowed_prefixes(raw: &str) -> anyhow::Result<Vec<String>> {
    let mut prefixes = Vec::new();
    for value in raw.lines().map(str::trim) {
        if value.is_empty() {
            continue;
        }
        if value.starts_with('/') || value.contains("..") || value.contains('\\') {
            bail!("AllowedPrefix must be a clean provider-relative prefix: {value}");
        }
        if !value.ends_with('/') {
            bail!("AllowedPrefix must end with '/': {value}");
        }
        if !prefixes.iter().any(|prefix| prefix == value) {
            prefixes.push(value.to_owned());
        }
    }
    Ok(prefixes)
}

fn is_protected_key(key: &str, is_legacy_date_partitioned_bronze: bool) -> bool {
    key == "gold/manifest.json"
        || key.starts_with("__r2_data_catalog/")
        || key.starts_with("warehouse/")
        || is_current_spatial_artifact(key)
        || key.starts_with("silver-handoff/")
        || (key.starts_with("bronze/source=") && !is_legacy_date_partitioned_bronze)
}

fn is_current_spatial_artifact(key: &str) -> bool {
    [
        VECTOR_TILE_ARTIFACT_ROOT,
        VECTOR_TILE_MANIFEST_ROOT,
        PARCEL_MARKER_ANCHOR_ARTIFACT_ROOT,
    ]
    .iter()
    .any(|root| {
        key.strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
    })
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

fn parse_concurrency(raw: &str) -> anyhow::Result<usize> {
    let value = raw
        .parse::<usize>()
        .context("R2 delete candidates concurrency must be an integer")?;
    if value == 0 || value > MAX_DELETE_CONCURRENCY {
        bail!("R2 delete candidates concurrency must be between 1 and {MAX_DELETE_CONCURRENCY}");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{assert_delete_candidate_object, DeleteCandidate};

    fn candidate(key: &str, classification: &str) -> DeleteCandidate {
        DeleteCandidate {
            key: key.to_owned(),
            size_bytes: 1,
            classification: classification.to_owned(),
            action: "delete_candidate".to_owned(),
            reason: "reference audit found no live pointer".to_owned(),
        }
    }

    #[test]
    fn delete_plan_protects_canonical_spatial_layouts() {
        for key in [
            "gold/vector-tiles/artifacts/018f0000-0000-7000-8000-000000000001/manifest.json",
            "gold/vector-tiles/manifests/018f0000-0000-7000-8000-000000000002.json",
            "gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000003/manifest.json",
        ] {
            assert!(assert_delete_candidate_object(&candidate(key, "stale"), &[]).is_err());
        }
    }

    #[test]
    fn delete_plan_rejects_raw_deletion_of_iceberg_table_files() {
        let key = "warehouse/silver/buildings_smoke/metadata/00001.metadata.json";
        assert!(assert_delete_candidate_object(
            &candidate(key, "iceberg_catalog_cleanup_required"),
            &[]
        )
        .is_err());
    }

    #[test]
    fn delete_plan_allows_explicitly_audited_legacy_spatial_layouts() -> anyhow::Result<()> {
        for key in [
            "gold/v1/parcels/0/0/0.pbf",
            "gold/parcel-marker-anchor-pbf/legacy/manifest.json",
            "gold/parcel-marker-anchor-aggregate-pbf/legacy/manifest.json",
            "gold/parcel-marker-anchor-runtime/legacy/manifest.json",
            "gold/parcel-marker-anchors/legacy/manifest.json",
        ] {
            assert_delete_candidate_object(&candidate(key, "legacy_gold_artifact"), &[])?;
        }
        Ok(())
    }
}
