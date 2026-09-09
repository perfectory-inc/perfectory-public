//! Points the parcel by-PNU gateway at a baked generation (root ADR-0096).
//!
//! Reads the summary `export-parcel-by-pnu-serving` wrote, verifies a sample of the objects it
//! names actually holds the recorded bytes, and only then overwrites the one mutable object of
//! the lane — `serving/parcels/by-pnu/manifest.json`. Producing the objects and pointing at them
//! are separate commands so a half-run cannot leave the pointer aimed at nothing, the same
//! doctrine as the industrial-complex Gold pointer publish.
//!
//! The generation may only move forward. Repointing the current generation (a delta re-bake) and
//! the very first publication are both explicit operator statements, not inferences from a
//! missing or unreadable manifest.

use std::{env, path::PathBuf};

use anyhow::{bail, ensure, Context};
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::parcel_by_pnu_serving_store::{local_root, ParcelServingObjectStore};
use crate::r2_layout::{parcel_by_pnu_serving_manifest_key, parcel_by_pnu_serving_object_key};

const EXPORT_SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_by_pnu_serving_export_summary.v1";
/// Wire schema of the serving manifest; the gateway Worker validates the same shape.
const MANIFEST_SCHEMA_VERSION: u32 = 1;
const MANIFEST_UNIT: &str = "parcel-by-pnu";
const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_CONFIRM_PUBLISH";
const OUTPUT_STORAGE_DRIVER_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_OUTPUT_STORAGE_DRIVER";
const OUTPUT_ROOT_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_OUTPUT_ROOT";
const EXPORT_SUMMARY_PATH_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_EXPORT_SUMMARY_PATH";
const ALLOW_REPOINT_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_ALLOW_REPOINT";
const FIRST_PUBLICATION_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_FIRST_PUBLICATION";
/// Read-back sample bound: enough to catch a wrong bucket or a truncated bake, cheap enough to
/// run before every repoint.
const MAX_VERIFICATION_SAMPLES: usize = 16;

/// The serving manifest — the pointer that pins the currently served generation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ParcelServingManifest {
    pub(crate) schema_version: u32,
    pub(crate) unit: String,
    pub(crate) current_generation: u64,
    pub(crate) gold_table: String,
    pub(crate) gold_iceberg_snapshot_id: String,
    pub(crate) object_count: u64,
    pub(crate) published_at_utc: String,
}

/// Runs the parcel by-PNU serving manifest publication.
pub async fn run() -> anyhow::Result<()> {
    let config = ManifestPublishConfig::from_env()?;
    let summary_raw = std::fs::read_to_string(&config.export_summary_path).with_context(|| {
        format!(
            "failed to read the export summary {}",
            config.export_summary_path.display()
        )
    })?;
    let summary: ExportSummaryInput = serde_json::from_str(&summary_raw).with_context(|| {
        format!(
            "the export summary {} does not parse",
            config.export_summary_path.display()
        )
    })?;

    let store = ParcelServingObjectStore::open(&config.output)?;
    let manifest = publish(&config, &store, &summary).await?;

    tracing::info!(
        output_bucket = store.bucket().unwrap_or("(local)"),
        current_generation = manifest.current_generation,
        gold_iceberg_snapshot_id = %manifest.gold_iceberg_snapshot_id,
        object_count = manifest.object_count,
        "parcel by-PNU serving manifest published"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ManifestPublishConfig {
    output: ProfileStoreConfig,
    export_summary_path: PathBuf,
    allow_repoint: bool,
    first_publication: bool,
}

/// The slice of the export summary this command consumes; unknown fields are the export's own.
#[derive(Debug, Deserialize)]
struct ExportSummaryInput {
    schema_version: String,
    gold_table: String,
    gold_iceberg_snapshot_id: String,
    target_generation: u64,
    artifacts: Vec<ExportArtifactInput>,
}

#[derive(Clone, Debug, Deserialize)]
struct ExportArtifactInput {
    pnu: String,
    object_key: String,
    object_checksum_sha256: String,
}

impl ManifestPublishConfig {
    fn from_env() -> anyhow::Result<Self> {
        let confirm = optional_env(CONFIRM_ENV)?.unwrap_or_default();
        ensure!(
            confirm.eq_ignore_ascii_case("true"),
            "{CONFIRM_ENV} must be true"
        );
        Ok(Self {
            output: ProfileStoreConfig::parse(
                optional_env(OUTPUT_STORAGE_DRIVER_ENV)?
                    .unwrap_or_else(|| "local".to_owned())
                    .as_str(),
                local_root(optional_env(OUTPUT_ROOT_ENV)?),
            )
            .with_context(|| format!("{OUTPUT_STORAGE_DRIVER_ENV}/{OUTPUT_ROOT_ENV}"))?,
            export_summary_path: optional_env(EXPORT_SUMMARY_PATH_ENV)?
                .map(PathBuf::from)
                .with_context(|| format!("{EXPORT_SUMMARY_PATH_ENV} is required"))?,
            allow_repoint: optional_env(ALLOW_REPOINT_ENV)?
                .is_some_and(|value| value.eq_ignore_ascii_case("true")),
            first_publication: optional_env(FIRST_PUBLICATION_ENV)?
                .is_some_and(|value| value.eq_ignore_ascii_case("true")),
        })
    }
}

async fn publish(
    config: &ManifestPublishConfig,
    store: &ParcelServingObjectStore,
    summary: &ExportSummaryInput,
) -> anyhow::Result<ParcelServingManifest> {
    ensure!(
        summary.schema_version == EXPORT_SUMMARY_SCHEMA_VERSION,
        "export summary schema must be {EXPORT_SUMMARY_SCHEMA_VERSION}, got {}",
        summary.schema_version
    );
    ensure!(
        !summary.artifacts.is_empty(),
        "the export summary names no objects; refusing to point the gateway at an empty bake"
    );
    for artifact in &summary.artifacts {
        let canonical = parcel_by_pnu_serving_object_key(summary.target_generation, &artifact.pnu)?;
        ensure!(
            canonical == artifact.object_key,
            "export summary object key {} does not belong to generation {} of parcel {}",
            artifact.object_key,
            summary.target_generation,
            artifact.pnu
        );
    }

    verify_sampled_objects(store, &summary.artifacts).await?;
    check_generation_transition(config, store, summary.target_generation).await?;

    let manifest = ParcelServingManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        unit: MANIFEST_UNIT.to_owned(),
        current_generation: summary.target_generation,
        gold_table: summary.gold_table.clone(),
        gold_iceberg_snapshot_id: summary.gold_iceberg_snapshot_id.clone(),
        object_count: u64::try_from(summary.artifacts.len()).context("object count overflow")?,
        published_at_utc: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
    };
    let mut body =
        serde_json::to_vec_pretty(&manifest).context("failed to serialize the serving manifest")?;
    body.push(b'\n');
    let checksum = format!("{:x}", Sha256::digest(&body));
    store
        .write_manifest(parcel_by_pnu_serving_manifest_key()?, &body, &checksum)
        .await?;
    Ok(manifest)
}

/// Reads back an evenly spaced sample and refuses to publish when any object is absent or holds
/// bytes other than the summary recorded — a wrong bucket, a truncated bake, or a stale summary.
async fn verify_sampled_objects(
    store: &ParcelServingObjectStore,
    artifacts: &[ExportArtifactInput],
) -> anyhow::Result<()> {
    let step = artifacts.len().div_ceil(MAX_VERIFICATION_SAMPLES).max(1);
    let mut verified = 0_usize;
    for artifact in artifacts.iter().step_by(step) {
        let stored = store
            .read_bytes(&artifact.object_key)
            .await
            .with_context(|| {
                format!(
                    "the export summary names {} but it cannot be read back",
                    artifact.object_key
                )
            })?;
        let stored_checksum = format!("{:x}", Sha256::digest(&stored));
        ensure!(
            stored_checksum == artifact.object_checksum_sha256,
            "{} holds bytes other than the export summary recorded",
            artifact.object_key
        );
        verified += 1;
    }
    ensure!(verified >= 1, "no objects were verified before publishing");
    Ok(())
}

/// The generation may only move forward; standing still and starting from nothing are explicit.
async fn check_generation_transition(
    config: &ManifestPublishConfig,
    store: &ParcelServingObjectStore,
    target_generation: u64,
) -> anyhow::Result<()> {
    let manifest_key = parcel_by_pnu_serving_manifest_key()?;
    let existing = store.read_bytes(manifest_key).await;
    match existing {
        Ok(bytes) => {
            ensure!(
                !config.first_publication,
                "{FIRST_PUBLICATION_ENV} is set but a serving manifest already exists"
            );
            let manifest: ParcelServingManifest = serde_json::from_slice(&bytes).context(
                "the existing serving manifest does not parse; refusing to overwrite it",
            )?;
            ensure!(
                manifest.schema_version == MANIFEST_SCHEMA_VERSION
                    && manifest.unit == MANIFEST_UNIT,
                "the existing serving manifest is not a {MANIFEST_UNIT} v{MANIFEST_SCHEMA_VERSION} manifest; refusing to overwrite it"
            );
            if target_generation == manifest.current_generation {
                ensure!(
                    config.allow_repoint,
                    "generation {target_generation} is already published; a delta re-bake repoint \
                     must state {ALLOW_REPOINT_ENV}=true"
                );
            } else {
                ensure!(
                    target_generation > manifest.current_generation,
                    "generation may only move forward: manifest is at {}, got {target_generation}",
                    manifest.current_generation
                );
            }
            Ok(())
        }
        Err(error) => {
            if config.first_publication {
                return Ok(());
            }
            bail!(
                "the serving manifest could not be read ({error:#}); if this is genuinely the \
                 first publication, state {FIRST_PUBLICATION_ENV}=true"
            )
        }
    }
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value.trim().to_owned())),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

#[cfg(test)]
mod tests;
