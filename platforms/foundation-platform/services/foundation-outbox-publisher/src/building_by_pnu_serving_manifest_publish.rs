//! Points the building by-PNU gateway at a baked generation (root ADR-0100).
//!
//! Reads the summary `export-building-by-pnu-serving` wrote, verifies a sample of the objects it
//! names actually holds the recorded bytes, and only then overwrites the one mutable object of
//! the lane — `serving/buildings/by-pnu/manifest.json`. Producing the objects and pointing at them
//! are separate commands so a half-run cannot leave the pointer aimed at nothing, the same
//! doctrine as the industrial-complex Gold pointer publish.
//!
//! The generation may only move forward. Repointing the current generation (a delta re-bake) and
//! the very first publication are both explicit operator statements, not inferences from a
//! missing or unreadable manifest.

use std::{env, path::PathBuf};

use anyhow::{bail, ensure, Context};
use chrono::{SecondsFormat, Utc};
use lakehouse_domain::GOLD_BUILDING_PANEL;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::building_by_pnu_serving_export::building_document::BUILDING_DOCUMENT_SCHEMA_VERSION;
use crate::building_by_pnu_serving_store::{local_root, BuildingServingObjectStore};
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::r2_layout::{building_by_pnu_serving_manifest_key, building_by_pnu_serving_object_key};

const EXPORT_SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.building_by_pnu_serving_export_summary.v1";
/// Wire schema of the serving manifest; the gateway Worker validates the same shape.
const MANIFEST_SCHEMA_VERSION: u32 = 1;
const MANIFEST_UNIT: &str = "building-by-pnu";
const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_CONFIRM_PUBLISH";
const OUTPUT_STORAGE_DRIVER_ENV: &str =
    "FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_OUTPUT_STORAGE_DRIVER";
const OUTPUT_ROOT_ENV: &str = "FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_OUTPUT_ROOT";
const EXPORT_SUMMARY_PATH_ENV: &str =
    "FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_EXPORT_SUMMARY_PATH";
const ALLOW_REPOINT_ENV: &str = "FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_ALLOW_REPOINT";
const FIRST_PUBLICATION_ENV: &str = "FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_FIRST_PUBLICATION";
const PUBLISH_FROM_LISTING_ENV: &str =
    "FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_PUBLISH_FROM_LISTING";
const TARGET_GENERATION_ENV: &str = "FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_TARGET_GENERATION";
const EXPECTED_GOLD_SNAPSHOT_ENV: &str =
    "FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID";
const EXPECTED_OBJECT_COUNT_ENV: &str =
    "FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_EXPECTED_OBJECT_COUNT";
/// Read-back sample bound: enough to catch a wrong bucket or a truncated bake, cheap enough to
/// run before every repoint.
const MAX_VERIFICATION_SAMPLES: usize = 16;

/// The serving manifest — the pointer that pins the currently served generation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct BuildingServingManifest {
    pub(crate) schema_version: u32,
    pub(crate) unit: String,
    pub(crate) current_generation: u64,
    pub(crate) gold_table: String,
    pub(crate) gold_iceberg_snapshot_id: String,
    pub(crate) object_count: u64,
    pub(crate) published_at_utc: String,
}

/// Runs the building by-PNU serving manifest publication.
pub async fn run() -> anyhow::Result<()> {
    let config = ManifestPublishConfig::from_env()?;
    let store = BuildingServingObjectStore::open(&config.output)?;

    let manifest = match &config.input {
        ManifestPublishInput::ExportSummary(export_summary_path) => {
            let summary_raw = std::fs::read_to_string(export_summary_path).with_context(|| {
                format!(
                    "failed to read the export summary {}",
                    export_summary_path.display()
                )
            })?;
            let summary: ExportSummaryInput =
                serde_json::from_str(&summary_raw).with_context(|| {
                    format!(
                        "the export summary {} does not parse",
                        export_summary_path.display()
                    )
                })?;
            publish(&config, &store, &summary).await?
        }
        ManifestPublishInput::Listing(expectation) => {
            publish_from_listing(&config, &store, expectation).await?
        }
    };

    tracing::info!(
        output_bucket = store.bucket().unwrap_or("(local)"),
        current_generation = manifest.current_generation,
        gold_iceberg_snapshot_id = %manifest.gold_iceberg_snapshot_id,
        object_count = manifest.object_count,
        "building by-PNU serving manifest published"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ManifestPublishConfig {
    output: ProfileStoreConfig,
    input: ManifestPublishInput,
    allow_repoint: bool,
    first_publication: bool,
}

/// Where the publish learns what one generation holds.
#[derive(Clone, Debug, Eq, PartialEq)]
enum ManifestPublishInput {
    /// One export run's summary — carries every object key and checksum (the single-run lane).
    ExportSummary(PathBuf),
    /// The bucket's own listing, checked against stated expectations — the sharded lane, where
    /// no single export run holds the whole generation and a merged summary would be gigabytes.
    /// The bucket is the record (root ADR-0062); the operator states what it must contain.
    Listing(ListingExpectation),
}

/// What the operator asserts the listed generation contains.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ListingExpectation {
    target_generation: u64,
    expected_gold_iceberg_snapshot_id: String,
    expected_object_count: u64,
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

        let from_listing = optional_env(PUBLISH_FROM_LISTING_ENV)?
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
        let export_summary_path = optional_env(EXPORT_SUMMARY_PATH_ENV)?.map(PathBuf::from);
        let input = if from_listing {
            ensure!(
                export_summary_path.is_none(),
                "{PUBLISH_FROM_LISTING_ENV} and {EXPORT_SUMMARY_PATH_ENV} name two different \
                 sources of truth; state exactly one"
            );
            ManifestPublishInput::Listing(ListingExpectation {
                target_generation: optional_env(TARGET_GENERATION_ENV)?
                    .with_context(|| format!("{TARGET_GENERATION_ENV} is required"))?
                    .parse::<u64>()
                    .with_context(|| {
                        format!("{TARGET_GENERATION_ENV} must be a positive integer")
                    })?,
                expected_gold_iceberg_snapshot_id: optional_env(EXPECTED_GOLD_SNAPSHOT_ENV)?
                    .with_context(|| format!("{EXPECTED_GOLD_SNAPSHOT_ENV} is required"))?,
                expected_object_count: optional_env(EXPECTED_OBJECT_COUNT_ENV)?
                    .with_context(|| format!("{EXPECTED_OBJECT_COUNT_ENV} is required"))?
                    .parse::<u64>()
                    .with_context(|| {
                        format!("{EXPECTED_OBJECT_COUNT_ENV} must be a positive integer")
                    })?,
            })
        } else {
            ManifestPublishInput::ExportSummary(
                export_summary_path
                    .with_context(|| format!("{EXPORT_SUMMARY_PATH_ENV} is required"))?,
            )
        };

        Ok(Self {
            output: ProfileStoreConfig::parse(
                optional_env(OUTPUT_STORAGE_DRIVER_ENV)?
                    .unwrap_or_else(|| "local".to_owned())
                    .as_str(),
                local_root(optional_env(OUTPUT_ROOT_ENV)?),
            )
            .with_context(|| format!("{OUTPUT_STORAGE_DRIVER_ENV}/{OUTPUT_ROOT_ENV}"))?,
            input,
            allow_repoint: optional_env(ALLOW_REPOINT_ENV)?
                .is_some_and(|value| value.eq_ignore_ascii_case("true")),
            first_publication: optional_env(FIRST_PUBLICATION_ENV)?
                .is_some_and(|value| value.eq_ignore_ascii_case("true")),
        })
    }
}

async fn publish(
    config: &ManifestPublishConfig,
    store: &BuildingServingObjectStore,
    summary: &ExportSummaryInput,
) -> anyhow::Result<BuildingServingManifest> {
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
        let canonical =
            building_by_pnu_serving_object_key(summary.target_generation, &artifact.pnu)?;
        ensure!(
            canonical == artifact.object_key,
            "export summary object key {} does not belong to generation {} of building {}",
            artifact.object_key,
            summary.target_generation,
            artifact.pnu
        );
    }

    verify_sampled_objects(store, &summary.artifacts).await?;
    check_generation_transition(config, store, summary.target_generation).await?;

    let manifest = BuildingServingManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        unit: MANIFEST_UNIT.to_owned(),
        current_generation: summary.target_generation,
        gold_table: summary.gold_table.clone(),
        gold_iceberg_snapshot_id: summary.gold_iceberg_snapshot_id.clone(),
        object_count: u64::try_from(summary.artifacts.len()).context("object count overflow")?,
        published_at_utc: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
    };
    write_manifest_object(store, &manifest).await?;
    Ok(manifest)
}

/// Publishes from the bucket's own listing, checked against the operator's stated expectations.
///
/// The sharded national bake leaves no single export summary that names the whole generation,
/// and a merged one would carry tens of millions of artifact lines. The bucket is the record
/// (root ADR-0062): the listing supplies what exists, the operator states what must exist, and
/// an evenly spaced sample proves the objects are this generation's documents of the stated
/// Gold snapshot.
async fn publish_from_listing(
    config: &ManifestPublishConfig,
    store: &BuildingServingObjectStore,
    expectation: &ListingExpectation,
) -> anyhow::Result<BuildingServingManifest> {
    let mut keys = store
        .list_existing_generation_keys(expectation.target_generation, None)
        .await?
        .into_iter()
        .collect::<Vec<_>>();
    keys.sort_unstable();
    let listed = u64::try_from(keys.len()).context("listed object count overflow")?;
    ensure!(
        listed == expectation.expected_object_count,
        "generation {} lists {listed} serving objects but the operator stated {}; a shard is \
         missing or foreign keys crept in — refusing to point the gateway at it",
        expectation.target_generation,
        expectation.expected_object_count
    );

    verify_sampled_listing(store, &keys, expectation).await?;
    check_generation_transition(config, store, expectation.target_generation).await?;

    let manifest = BuildingServingManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        unit: MANIFEST_UNIT.to_owned(),
        current_generation: expectation.target_generation,
        gold_table: GOLD_BUILDING_PANEL.table_name.to_owned(),
        gold_iceberg_snapshot_id: expectation.expected_gold_iceberg_snapshot_id.clone(),
        object_count: listed,
        published_at_utc: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
    };
    write_manifest_object(store, &manifest).await?;
    Ok(manifest)
}

/// Reads an evenly spaced sample of listed keys and refuses to publish unless each one is this
/// lane's document, of its own PNU, baked from the stated Gold snapshot.
async fn verify_sampled_listing(
    store: &BuildingServingObjectStore,
    keys: &[String],
    expectation: &ListingExpectation,
) -> anyhow::Result<()> {
    let step = keys.len().div_ceil(MAX_VERIFICATION_SAMPLES).max(1);
    let mut verified = 0_usize;
    for key in keys.iter().step_by(step) {
        let stored = store
            .read_bytes(key)
            .await
            .with_context(|| format!("the listing names {key} but it cannot be read back"))?;
        let document: serde_json::Value = serde_json::from_slice(&stored)
            .with_context(|| format!("{key} does not hold a JSON document"))?;
        ensure!(
            document
                .get("schema_version")
                .and_then(serde_json::Value::as_str)
                == Some(BUILDING_DOCUMENT_SCHEMA_VERSION),
            "{key} does not hold a {BUILDING_DOCUMENT_SCHEMA_VERSION} document"
        );
        let pnu_in_key = key
            .rsplit('/')
            .next()
            .and_then(|file_name| file_name.strip_suffix(".json"))
            .with_context(|| format!("{key} does not end in a PNU object file name"))?;
        ensure!(
            document.get("pnu").and_then(serde_json::Value::as_str) == Some(pnu_in_key),
            "{key} holds a document for another building"
        );
        let baked_from = document
            .pointer("/source/iceberg_snapshot_id")
            .and_then(serde_json::Value::as_str);
        ensure!(
            baked_from == Some(expectation.expected_gold_iceberg_snapshot_id.as_str()),
            "{key} was baked from gold snapshot {baked_from:?}, not the stated {}",
            expectation.expected_gold_iceberg_snapshot_id
        );
        verified += 1;
    }
    ensure!(verified >= 1, "no objects were verified before publishing");
    Ok(())
}

/// Writes the manifest — the lane's one mutable object — with its own checksum.
async fn write_manifest_object(
    store: &BuildingServingObjectStore,
    manifest: &BuildingServingManifest,
) -> anyhow::Result<()> {
    let mut body =
        serde_json::to_vec_pretty(manifest).context("failed to serialize the serving manifest")?;
    body.push(b'\n');
    let checksum = format!("{:x}", Sha256::digest(&body));
    store
        .write_manifest(building_by_pnu_serving_manifest_key()?, &body, &checksum)
        .await
}

/// Reads back an evenly spaced sample and refuses to publish when any object is absent or holds
/// bytes other than the summary recorded — a wrong bucket, a truncated bake, or a stale summary.
async fn verify_sampled_objects(
    store: &BuildingServingObjectStore,
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
    store: &BuildingServingObjectStore,
    target_generation: u64,
) -> anyhow::Result<()> {
    let manifest_key = building_by_pnu_serving_manifest_key()?;
    let existing = store.read_bytes(manifest_key).await;
    match existing {
        Ok(bytes) => {
            ensure!(
                !config.first_publication,
                "{FIRST_PUBLICATION_ENV} is set but a serving manifest already exists"
            );
            let manifest: BuildingServingManifest = serde_json::from_slice(&bytes).context(
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
