//! Points a by-PNU gateway at what was baked (root ADR-0096, ADR-0100, ADR-0141).
//!
//! `publish-{parcel,building}-by-pnu-serving-manifest` moves the lane's one mutable object, the
//! manifest. Four inputs, exactly one per run:
//!
//! - **full, from an export summary** — one export run that baked a whole new base generation;
//! - **full, from the listing** — a sharded bake: the bucket lists the base, the bake states the
//!   Gold snapshot and the row count it must hold;
//! - **patch** — one patch generation over the served base, named by the change set the delta
//!   job wrote (`by_pnu_panel_delta.py`). A change set with nothing in it advances only
//!   `reflected_gold_iceberg_snapshot_id`;
//! - **rollback** — a manifest the history holds, accepted only when it is the live manifest
//!   minus its newest patches.
//!
//! Every publish first stores the manifest it replaces, create-only, under the lane's manifest
//! history, so the bucket keeps every state it served. The first v2 manifest a lane gets is
//! written only after the lane's public gateway says it reads v2: an older Worker would answer
//! every request 503.
//!
//! The manifest write is a compare-and-swap over the version read at the start: two publishes
//! that read the same manifest cannot both move it, and the one that loses is refused, naming
//! the conflict, with nothing written.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, SecondsFormat, Utc};
use lakehouse_domain::{GOLD_BUILDING_PANEL, GOLD_PARCEL_PANEL};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::building_by_pnu_serving_export::building_document::BUILDING_DOCUMENT_SCHEMA_VERSION;
use crate::by_pnu_gateway_contract::{by_pnu_serving_patch_policy, ByPnuLane};
use crate::by_pnu_serving_manifest::{
    pnu_prefixes, read_tombstone, PatchEntry, ServedManifest, ServingManifest, StoredManifest,
    VerifiedRebase,
};
use crate::by_pnu_serving_patch_export::read_pnu_list;
use crate::by_pnu_serving_pins::{self as pins, SnapshotPins};
use crate::by_pnu_serving_rebase as rebase;
use crate::by_pnu_serving_store::{
    local_root, refuse_removed_switches, ByPnuServingStore, ManifestMoved,
};
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::parcel_by_pnu_serving_export::parcel_document::PARCEL_DOCUMENT_SCHEMA_VERSION;
use crate::r2_layout::by_pnu;

/// Read-back sample bound: enough to catch a wrong bucket or a truncated bake, cheap enough to
/// run before every publish.
const MAX_VERIFICATION_SAMPLES: usize = 16;
const DELTA_SUMMARY_JOB: &str = "by_pnu_panel_delta";
const GATEWAY_PROBE_TIMEOUT_SECONDS: u64 = 15;

/// The document schema the lane's export bakes now.
pub(crate) const fn document_schema_version(lane: ByPnuLane) -> &'static str {
    match lane {
        ByPnuLane::Parcel => PARCEL_DOCUMENT_SCHEMA_VERSION,
        ByPnuLane::Building => BUILDING_DOCUMENT_SCHEMA_VERSION,
    }
}

/// The lane's Gold table.
pub(crate) const fn gold_table(lane: ByPnuLane) -> &'static str {
    match lane {
        ByPnuLane::Parcel => GOLD_PARCEL_PANEL.table_name,
        ByPnuLane::Building => GOLD_BUILDING_PANEL.table_name,
    }
}

/// Runs the lane's manifest publication.
///
/// # Errors
/// Refuses on any failed gate; the manifest is then unchanged.
pub(crate) async fn run(lane: ByPnuLane) -> anyhow::Result<()> {
    let config = PublishConfig::from_env(lane)?;
    let store = ByPnuServingStore::open(lane, &config.output)?;
    let gateway = format!("https://{}", lane.policy()?.public_hostname);
    let manifest = publish(&config, &store, &gateway).await?;
    tracing::info!(
        unit = lane.unit(),
        output_bucket = store.bucket().unwrap_or("(local)"),
        base_generation = manifest.base_generation,
        patches = manifest.patches.len(),
        newest_patch = manifest.patches.first().map_or(0, |patch| patch.generation),
        reflected_gold_iceberg_snapshot_id = %manifest.reflected_gold_iceberg_snapshot_id,
        object_count = manifest.object_count,
        "by-PNU serving manifest published"
    );
    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct PublishConfig {
    pub(crate) output: ProfileStoreConfig,
    pub(crate) input: PublishInput,
    pub(crate) first_publication: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum PublishInput {
    /// One export run's summary — every object key with its checksum.
    ExportSummary(PathBuf),
    /// The bucket's own listing of a sharded base, checked against stated expectations.
    Listing(ListingExpectation),
    /// One patch generation, or a change set with nothing in it.
    Patch(PatchInput),
    /// A manifest the lane's history holds.
    Rollback(String),
}

#[derive(Clone, Debug)]
pub(crate) struct ListingExpectation {
    pub(crate) target_generation: u64,
    pub(crate) expected_gold_iceberg_snapshot_id: String,
    pub(crate) expected_object_count: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct PatchInput {
    pub(crate) base_generation: u64,
    /// Absent exactly when the change set is empty.
    pub(crate) patch: Option<u64>,
    pub(crate) expected_gold_iceberg_snapshot_id: String,
    pub(crate) change_set_summary: PathBuf,
    pub(crate) upserts: PathBuf,
    pub(crate) deletes: PathBuf,
}

impl PublishConfig {
    fn from_env(lane: ByPnuLane) -> anyhow::Result<Self> {
        refuse_removed_switches(lane, &["ALLOW_REPOINT", "ALLOW_OVERWRITE"])?;
        let env = |name: &str| optional_env(&lane.env(name));
        let flag = |name: &str| -> anyhow::Result<bool> {
            Ok(env(name)?.is_some_and(|value| value.eq_ignore_ascii_case("true")))
        };
        let required = |name: &str| -> anyhow::Result<String> {
            env(name)?.with_context(|| format!("{} is required", lane.env(name)))
        };
        let number = |name: &str| -> anyhow::Result<u64> {
            required(name)?
                .parse::<u64>()
                .with_context(|| format!("{} must be a positive integer", lane.env(name)))
        };
        ensure!(
            flag("CONFIRM_PUBLISH")?,
            "{} must be true",
            lane.env("CONFIRM_PUBLISH")
        );

        let summary = env("EXPORT_SUMMARY_PATH")?.map(PathBuf::from);
        let listing = flag("PUBLISH_FROM_LISTING")?;
        let patch = flag("PUBLISH_PATCH")?;
        let rollback = env("ROLLBACK_TO_MANIFEST_KEY")?;
        let stated = [summary.is_some(), listing, patch, rollback.is_some()]
            .into_iter()
            .filter(|stated| *stated)
            .count();
        ensure!(
            stated == 1,
            "state exactly one of {}, {}=true, {}=true or {}",
            lane.env("EXPORT_SUMMARY_PATH"),
            lane.env("PUBLISH_FROM_LISTING"),
            lane.env("PUBLISH_PATCH"),
            lane.env("ROLLBACK_TO_MANIFEST_KEY")
        );
        let input = if let Some(path) = summary {
            PublishInput::ExportSummary(path)
        } else if listing {
            PublishInput::Listing(ListingExpectation {
                target_generation: number("TARGET_GENERATION")?,
                expected_gold_iceberg_snapshot_id: required("EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID")?,
                expected_object_count: number("EXPECTED_OBJECT_COUNT")?,
            })
        } else if patch {
            PublishInput::Patch(PatchInput {
                base_generation: number("TARGET_GENERATION")?,
                patch: env("TARGET_PATCH")?
                    .map(|raw| {
                        raw.parse::<u64>().with_context(|| {
                            format!("{} must be a positive integer", lane.env("TARGET_PATCH"))
                        })
                    })
                    .transpose()?,
                expected_gold_iceberg_snapshot_id: required("EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID")?,
                change_set_summary: PathBuf::from(required("CHANGE_SET_SUMMARY_PATH")?),
                upserts: PathBuf::from(required("UPSERT_LIST_PATH")?),
                deletes: PathBuf::from(required("DELETE_LIST_PATH")?),
            })
        } else {
            PublishInput::Rollback(rollback.context("unreachable: one input was stated")?)
        };

        Ok(Self {
            output: ProfileStoreConfig::parse(
                env("OUTPUT_STORAGE_DRIVER")?
                    .unwrap_or_else(|| "local".to_owned())
                    .as_str(),
                local_root(env("OUTPUT_ROOT")?),
            )
            .with_context(|| {
                format!(
                    "{}/{}",
                    lane.env("OUTPUT_STORAGE_DRIVER"),
                    lane.env("OUTPUT_ROOT")
                )
            })?,
            input,
            first_publication: flag("FIRST_PUBLICATION")?,
        })
    }
}

/// Publishes what `config` names, after every gate of its input.
///
/// # Errors
/// Refuses on any failed gate; the manifest is then unchanged.
pub(crate) async fn publish(
    config: &PublishConfig,
    store: &ByPnuServingStore,
    gateway_base_url: &str,
) -> anyhow::Result<ServingManifest> {
    let lane = store.lane();
    let existing = read_existing(config, store).await?;
    let manifest = match &config.input {
        PublishInput::ExportSummary(path) => {
            let summary = read_export_summary(lane, path)?;
            full_from_summary(store, &summary, existing.as_ref()).await?
        }
        PublishInput::Listing(expectation) => {
            full_from_listing(store, expectation, existing.as_ref()).await?
        }
        PublishInput::Patch(input) => {
            let existing = existing
                .as_ref()
                .context("a patch needs a published base; there is no serving manifest")?;
            patch(store, input, &existing.manifest).await?
        }
        PublishInput::Rollback(history_key) => {
            let existing = existing
                .as_ref()
                .context("a rollback needs a live serving manifest to roll back")?;
            rollback(store, history_key, &existing.manifest).await?
        }
    };
    if existing
        .as_ref()
        .is_none_or(|stored| stored.manifest.wire_schema_version < manifest.schema_version)
    {
        require_gateway_reads(lane, gateway_base_url, manifest.schema_version).await?;
    }
    // Pin the snapshot the next change set is computed against before anyone relies on it, and
    // release the older pins only once this manifest is live (root ADR-0146 §2).
    let snapshot_pins = SnapshotPins::for_output(&config.output)?;
    let table = gold_table(lane);
    let pinned = pins::pin(
        &snapshot_pins,
        lane,
        table,
        &manifest.reflected_gold_iceberg_snapshot_id,
        &manifest.published_at_utc,
    )
    .await;
    // A rollback is the way back from a bad publish; an unpinnable snapshot (already expired)
    // must not block it. The older pins then stay, and the next bake re-bases if it must.
    let mut manifest = manifest;
    manifest.reflected_gold_snapshot_tag = match pinned {
        Ok(tag) => Some(tag),
        Err(error) if matches!(config.input, PublishInput::Rollback(_)) => {
            tracing::warn!(error = %format!("{error:#}"), "rolling back without pinning its reflected snapshot");
            None
        }
        Err(error) => return Err(error),
    };
    let body = manifest.to_bytes()?;
    if let Err(error) = commit(store, existing.as_ref(), &body).await {
        if !settle_failed_write(store, &snapshot_pins, &manifest, &body, &error).await {
            return Err(error);
        }
        tracing::warn!(error = %format!("{error:#}"), "the manifest write reported a failure but the manifest it wrote is live");
    }
    if manifest.reflected_gold_snapshot_tag.is_some() {
        release_behind_live(store, &snapshot_pins).await;
    }
    Ok(manifest)
}

/// After a failed manifest write: whether the manifest it wrote is live after all (its answer was
/// lost after the store accepted it). The new pin is released only when the write lost its
/// compare-and-swap and the live manifest does not reflect the pinned snapshot; anything less
/// certain keeps it, which only keeps a snapshot alive until the next publish.
async fn settle_failed_write(
    store: &ByPnuServingStore,
    snapshot_pins: &SnapshotPins,
    manifest: &ServingManifest,
    body: &[u8],
    error: &anyhow::Error,
) -> bool {
    let live = match store.read_manifest().await {
        Ok((bytes, _)) => bytes,
        Err(read) => {
            tracing::warn!(error = %format!("{read:#}"), "the manifest cannot be read after a failed write; the new pin stays until the next publish");
            return false;
        }
    };
    if live == body {
        return true;
    }
    let Some(tag) = &manifest.reflected_gold_snapshot_tag else {
        return false;
    };
    let reflects_this_snapshot = ServedManifest::parse(store.lane(), &live).is_ok_and(|live| {
        live.reflected_gold_iceberg_snapshot_id == manifest.reflected_gold_iceberg_snapshot_id
    });
    if error.downcast_ref::<ManifestMoved>().is_none() || reflects_this_snapshot {
        tracing::warn!(tag = %tag, "the manifest write failed without a confirmed lost compare-and-swap, or the live manifest reflects the same snapshot; the new pin stays until the next publish");
        return false;
    }
    let released = match pins::parse_snapshot(&manifest.reflected_gold_iceberg_snapshot_id) {
        Ok(snapshot) => {
            snapshot_pins
                .remove(gold_table(store.lane()), tag, snapshot)
                .await
        }
        Err(parse) => Err(parse),
    };
    if let Err(release) = released {
        tracing::warn!(tag = %tag, error = %format!("{release:#}"), "the unused pin stays until the next publish");
    }
    false
}

/// Releases the lane's pins older than the one the live manifest names, read now: when another
/// publish moved the manifest since this one wrote it, its pin is the one kept.
pub(crate) async fn release_behind_live(store: &ByPnuServingStore, snapshot_pins: &SnapshotPins) {
    let lane = store.lane();
    let live = match store.read_manifest().await {
        Ok((bytes, _)) => ServedManifest::parse(lane, &bytes),
        Err(error) => Err(error),
    };
    let live_tag = match live {
        Ok(live) => oldest_live_tag(lane, &live),
        Err(error) => {
            tracing::warn!(error = %format!("{error:#}"), "the live manifest cannot be read; older pins stay until the next publish");
            return;
        }
    };
    let Some(live_tag) = live_tag else {
        tracing::warn!("the live manifest names no pin; older pins stay until the next publish");
        return;
    };
    let released = pins::release_older(snapshot_pins, lane, gold_table(lane), &live_tag).await;
    tracing::info!(tag = %live_tag, released = ?released, "the live manifest's Gold snapshot is pinned");
}

/// The oldest pin the live manifest names: the object lane's and the pack lane's reflected
/// snapshots are both pinned while both are named, so release stops below the older of them.
pub(crate) fn oldest_live_tag(lane: ByPnuLane, live: &ServedManifest) -> Option<String> {
    [
        live.reflected_gold_snapshot_tag.as_ref(),
        live.section_packs
            .as_ref()
            .and_then(|packs| packs.reflected_gold_snapshot_tag.as_ref()),
    ]
    .into_iter()
    .flatten()
    .min_by_key(|tag| pins::tag_order(lane, tag))
    .cloned()
}

async fn read_existing(
    config: &PublishConfig,
    store: &ByPnuServingStore,
) -> anyhow::Result<Option<StoredManifest>> {
    let lane = store.lane();
    match store.read_manifest().await {
        Ok((bytes, version)) => {
            ensure!(
                !config.first_publication,
                "{} is set but a serving manifest already exists",
                lane.env("FIRST_PUBLICATION")
            );
            let manifest = ServedManifest::parse(lane, &bytes)
                .context("the existing serving manifest cannot be read; refusing to replace it")?;
            Ok(Some(StoredManifest {
                manifest,
                bytes,
                version,
            }))
        }
        Err(error) if config.first_publication => {
            tracing::info!(error = %format!("{error:#}"), "first publication of the lane");
            Ok(None)
        }
        Err(error) => bail!(
            "the serving manifest could not be read ({error:#}); if this is genuinely the first \
             publication, state {}=true",
            lane.env("FIRST_PUBLICATION")
        ),
    }
}

/// Stores the replaced manifest in the history, then writes the new one over exactly the version
/// read at the start (or, on a first publication, only where none exists).
pub(crate) async fn commit(
    store: &ByPnuServingStore,
    existing: Option<&StoredManifest>,
    body: &[u8],
) -> anyhow::Result<()> {
    let lane = store.lane();
    if let Some(existing) = existing {
        let published = DateTime::parse_from_rfc3339(&existing.manifest.published_at_utc)
            .with_context(|| {
                format!(
                    "the live manifest's published_at_utc {:?} is not RFC 3339; its history key \
                     cannot be named",
                    existing.manifest.published_at_utc
                )
            })?
            .with_timezone(&Utc)
            .format("%Y%m%dT%H%M%SZ")
            .to_string();
        let checksum = format!("{:x}", Sha256::digest(&existing.bytes));
        let key = by_pnu::manifest_history_key(lane, &published, &checksum)?;
        store
            .write_manifest_history(&key, &existing.bytes, &checksum)
            .await?;
        tracing::info!(history_key = %key, "replaced manifest stored in the history");
    }
    let checksum = format!("{:x}", Sha256::digest(body));
    store
        .write_manifest(
            by_pnu::manifest_key(lane)?,
            body,
            &checksum,
            existing.map(|stored| stored.version.as_str()),
        )
        .await
}

/// A new base: the generation only moves forward, and it starts with no patches.
fn new_base(
    lane: ByPnuLane,
    existing: Option<&StoredManifest>,
    generation: u64,
    snapshot: &str,
    object_count: u64,
) -> anyhow::Result<ServingManifest> {
    if let Some(existing) = existing {
        ensure!(
            generation > existing.manifest.base_generation,
            "the base generation may only move forward: the manifest serves {}, got {generation}; \
             a changed document goes into a patch, not into a served generation",
            existing.manifest.base_generation
        );
    }
    let policy = by_pnu_serving_patch_policy()?;
    Ok(ServingManifest {
        schema_version: policy.manifest_schema_version,
        unit: lane.unit().to_owned(),
        base_generation: generation,
        base_object_count: object_count,
        document_schema_version: document_schema_version(lane).to_owned(),
        gold_table: gold_table(lane).to_owned(),
        gold_iceberg_snapshot_id: snapshot.to_owned(),
        reflected_gold_iceberg_snapshot_id: snapshot.to_owned(),
        pnu_prefix_length: policy.pnu_prefix_length,
        patches: Vec::new(),
        object_count,
        published_at_utc: now(),
        verified_rebase: None,
        reflected_gold_snapshot_tag: None,
        // An object bake never drops the packs the lane serves from (root ADR-0147).
        section_packs: existing.and_then(|stored| stored.manifest.section_packs.clone()),
    })
}

/// The slice of the export summary this command consumes; unknown fields are the export's own.
#[derive(Debug, Deserialize)]
pub(crate) struct ExportSummaryInput {
    pub(crate) schema_version: String,
    pub(crate) gold_table: String,
    pub(crate) gold_iceberg_snapshot_id: String,
    pub(crate) target_generation: u64,
    #[serde(default)]
    pub(crate) target_patch: Option<u64>,
    pub(crate) artifacts: Vec<ExportArtifactInput>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ExportArtifactInput {
    pub(crate) pnu: String,
    pub(crate) object_key: String,
    pub(crate) object_checksum_sha256: String,
}

fn read_export_summary(lane: ByPnuLane, path: &Path) -> anyhow::Result<ExportSummaryInput> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read the export summary {}", path.display()))?;
    let summary: ExportSummaryInput = serde_json::from_str(&raw)
        .with_context(|| format!("the export summary {} does not parse", path.display()))?;
    let expected = format!(
        "foundation-platform.{}_by_pnu_serving_export_summary.v1",
        lane.noun()
    );
    ensure!(
        summary.schema_version == expected,
        "export summary schema must be {expected}, got {}",
        summary.schema_version
    );
    Ok(summary)
}

pub(crate) async fn full_from_summary(
    store: &ByPnuServingStore,
    summary: &ExportSummaryInput,
    existing: Option<&StoredManifest>,
) -> anyhow::Result<ServingManifest> {
    let lane = store.lane();
    ensure!(
        summary.target_patch.is_none(),
        "the export summary is of a patch; publish it as a patch from its change set"
    );
    ensure!(
        summary.gold_table == gold_table(lane),
        "the export summary is of {}, not {}",
        summary.gold_table,
        gold_table(lane)
    );
    ensure!(
        !summary.artifacts.is_empty(),
        "the export summary names no objects; refusing to point the gateway at an empty bake"
    );
    for artifact in &summary.artifacts {
        let canonical = by_pnu::object_key(lane, summary.target_generation, &artifact.pnu)?;
        ensure!(
            canonical == artifact.object_key,
            "export summary object key {} does not belong to generation {} of {} {}",
            artifact.object_key,
            summary.target_generation,
            lane.noun(),
            artifact.pnu
        );
    }
    let step = summary
        .artifacts
        .len()
        .div_ceil(MAX_VERIFICATION_SAMPLES)
        .max(1);
    for artifact in summary.artifacts.iter().step_by(step) {
        let stored = store
            .read_bytes(&artifact.object_key)
            .await
            .with_context(|| {
                format!(
                    "the export summary names {} but it cannot be read back",
                    artifact.object_key
                )
            })?;
        ensure!(
            format!("{:x}", Sha256::digest(&stored)) == artifact.object_checksum_sha256,
            "{} holds bytes other than the export summary recorded",
            artifact.object_key
        );
    }
    new_base(
        lane,
        existing,
        summary.target_generation,
        &summary.gold_iceberg_snapshot_id,
        u64::try_from(summary.artifacts.len()).context("object count overflow")?,
    )
}

/// The sharded national bake leaves no single export summary that names the whole generation.
/// The bucket is the record (root ADR-0062): the listing supplies what exists, the bake states
/// what must exist, and an evenly spaced sample proves the objects are this generation's
/// documents of the stated Gold snapshot.
pub(crate) async fn full_from_listing(
    store: &ByPnuServingStore,
    expectation: &ListingExpectation,
    existing: Option<&StoredManifest>,
) -> anyhow::Result<ServingManifest> {
    let lane = store.lane();
    let mut keys = store
        .list_existing_generation_keys(expectation.target_generation, None)
        .await?
        .into_iter()
        .collect::<Vec<_>>();
    keys.sort_unstable();
    let listed = u64::try_from(keys.len()).context("listed object count overflow")?;
    ensure!(
        listed == expectation.expected_object_count,
        "generation {} lists {listed} serving objects but the bake stated {}; a shard is missing \
         or foreign keys crept in — refusing to point the gateway at it",
        expectation.target_generation,
        expectation.expected_object_count
    );
    let step = keys.len().div_ceil(MAX_VERIFICATION_SAMPLES).max(1);
    let mut verified = 0_usize;
    for key in keys.iter().step_by(step) {
        let (_, pnu) = key_pnu(key)?;
        verify_document(
            store,
            key,
            &pnu,
            &expectation.expected_gold_iceberg_snapshot_id,
        )
        .await?;
        verified += 1;
    }
    ensure!(verified >= 1, "no objects were verified before publishing");
    new_base(
        lane,
        existing,
        expectation.target_generation,
        &expectation.expected_gold_iceberg_snapshot_id,
        listed,
    )
}

/// The PNU an object key's file name carries.
fn key_pnu(key: &str) -> anyhow::Result<(String, String)> {
    let file_name = key
        .rsplit('/')
        .next()
        .with_context(|| format!("{key} has no file name"))?;
    let pnu = file_name
        .strip_suffix(".json")
        .with_context(|| format!("{key} does not end in a PNU object file name"))?;
    Ok((key.to_owned(), pnu.to_owned()))
}

/// Gate (나): one sampled object is this lane's document of its own PNU, baked from `snapshot`.
async fn verify_document(
    store: &ByPnuServingStore,
    key: &str,
    pnu: &str,
    snapshot: &str,
) -> anyhow::Result<()> {
    let lane = store.lane();
    let stored = store
        .read_bytes(key)
        .await
        .with_context(|| format!("the listing names {key} but it cannot be read back"))?;
    let document: serde_json::Value = serde_json::from_slice(&stored)
        .with_context(|| format!("{key} does not hold a JSON document"))?;
    let schema = document_schema_version(lane);
    ensure!(
        document
            .get("schema_version")
            .and_then(serde_json::Value::as_str)
            == Some(schema),
        "{key} does not hold a {schema} document"
    );
    ensure!(
        document.get("pnu").and_then(serde_json::Value::as_str) == Some(pnu),
        "{key} holds a document for another {}",
        lane.noun()
    );
    let baked_from = document
        .pointer("/source/iceberg_snapshot_id")
        .and_then(serde_json::Value::as_str);
    ensure!(
        baked_from == Some(snapshot),
        "{key} was baked from gold snapshot {baked_from:?}, not the stated {snapshot}"
    );
    Ok(())
}

/// The change set the delta job summarised (`by_pnu_panel_delta.py`), or the verified re-base
/// (`verify-parcel-by-pnu-serving-rebase`, root ADR-0146 §1) found.
#[derive(Debug, Deserialize)]
struct ChangeSetSummary {
    job_name: String,
    contract: String,
    quality_metrics: ChangeSetCounts,
    input: ChangeSetInput,
    /// Present exactly in a verified re-base's change set.
    #[serde(default)]
    verification: Option<RebaseVerification>,
}

#[derive(Debug, Deserialize)]
struct RebaseVerification {
    run_id: String,
    reason: String,
    method: String,
    served_objects_read: u64,
    equal: u64,
    changed: u64,
    only_served: u64,
    only_gold: u64,
}

/// The record a verified re-base leaves in the manifest it publishes; `None` for a delta.
fn verified_rebase(
    change: &ChangeSetSummary,
    served: &ServedManifest,
) -> anyhow::Result<Option<VerifiedRebase>> {
    match (change.job_name.as_str(), &change.verification) {
        (DELTA_SUMMARY_JOB, None) => Ok(None),
        (rebase::JOB_NAME, Some(found)) => {
            ensure!(
                found.served_objects_read >= found.equal + found.changed + found.only_served
                    && found.equal + found.changed + found.only_served == served.object_count,
                "the re-base read {} objects with {} equal, {} changed and {} only served, which \
                 does not account for the {} PNUs the lane serves; it is incomplete",
                found.served_objects_read,
                found.equal,
                found.changed,
                found.only_served,
                served.object_count
            );
            ensure!(
                found.changed + found.only_gold == change.quality_metrics.upsert_count
                    && found.only_served == change.quality_metrics.delete_count,
                "the re-base's verdicts disagree with its own change set"
            );
            ensure!(
                !found.reason.trim().is_empty() && !found.run_id.trim().is_empty(),
                "a verified re-base names its run and its reason"
            );
            Ok(Some(VerifiedRebase {
                run_id: found.run_id.clone(),
                reason: found.reason.clone(),
                method: found.method.clone(),
                baseline_gold_iceberg_snapshot_id: change.input.baseline_snapshot_id.clone(),
                served_objects_read: found.served_objects_read,
                equal: found.equal,
                changed: found.changed,
                only_served: found.only_served,
                only_gold: found.only_gold,
            }))
        }
        (job, _) => bail!(
            "the change set is a {job} summary {} a verification block; neither the delta nor \
             the verified re-base",
            if change.verification.is_some() {
                "with"
            } else {
                "without"
            }
        ),
    }
}

#[derive(Debug, Deserialize)]
struct ChangeSetCounts {
    new_count: u64,
    upsert_count: u64,
    delete_count: u64,
}

#[derive(Debug, Deserialize)]
struct ChangeSetInput {
    baseline_snapshot_id: String,
    current_snapshot_id: String,
    /// The served base the change set was compared against; the verified re-base names it.
    #[serde(default)]
    base_generation: Option<u64>,
    /// The served patches, newest first, the change set was compared against; the verified
    /// re-base names them.
    #[serde(default)]
    patches: Option<Vec<u64>>,
}

/// One patch generation over the served base, or an empty change set.
pub(crate) async fn patch(
    store: &ByPnuServingStore,
    input: &PatchInput,
    served: &ServedManifest,
) -> anyhow::Result<ServingManifest> {
    let lane = store.lane();
    let policy = by_pnu_serving_patch_policy()?;
    ensure!(
        input.base_generation == served.base_generation,
        "the patch is over generation {} but the manifest serves generation {}",
        input.base_generation,
        served.base_generation
    );
    let served_schema = served_document_schema(store, served).await?;
    ensure!(
        served_schema == document_schema_version(lane),
        "the base holds {served_schema} documents but the export bakes {}; a schema change needs \
         a new base (full bake), not a patch",
        document_schema_version(lane)
    );
    // The served patches stay in the next manifest, which is held to the contract as it is now.
    ensure!(
        served.patches.len() <= policy.max_patches,
        "the base carries {} patches, over the contract's max_patches {}; a full bake compacts \
         them",
        served.patches.len(),
        policy.max_patches
    );
    ensure!(
        served.patches.is_empty() || served.pnu_prefix_length == Some(policy.pnu_prefix_length),
        "the served patches are listed by {:?}-digit prefixes but the contract's \
         pnu_prefix_length is {}; a full bake compacts them",
        served.pnu_prefix_length,
        policy.pnu_prefix_length
    );

    let raw = std::fs::read_to_string(&input.change_set_summary).with_context(|| {
        format!(
            "failed to read the change set {}",
            input.change_set_summary.display()
        )
    })?;
    let change: ChangeSetSummary =
        serde_json::from_str(&raw).context("the change set summary does not parse")?;
    ensure!(
        [DELTA_SUMMARY_JOB, rebase::JOB_NAME].contains(&change.job_name.as_str())
            && change.contract == gold_table(lane),
        "the change set is a {} summary of {}, not {DELTA_SUMMARY_JOB} or {} of {}",
        change.job_name,
        change.contract,
        rebase::JOB_NAME,
        gold_table(lane)
    );
    ensure!(
        change.input.baseline_snapshot_id == served.reflected_gold_iceberg_snapshot_id,
        "the change set was computed against Gold snapshot {} but the manifest reflects {}; it \
         would miss or repeat changes",
        change.input.baseline_snapshot_id,
        served.reflected_gold_iceberg_snapshot_id
    );
    ensure!(
        change.input.current_snapshot_id == input.expected_gold_iceberg_snapshot_id,
        "the change set reaches Gold snapshot {}, not the stated {}",
        change.input.current_snapshot_id,
        input.expected_gold_iceberg_snapshot_id
    );
    // A re-base compared what was served: the base and the patches it read must still be served.
    let served_patches = served
        .patches
        .iter()
        .map(|patch| patch.generation)
        .collect::<Vec<_>>();
    ensure!(
        change.job_name != rebase::JOB_NAME
            || (change.input.base_generation.is_some() && change.input.patches.is_some()),
        "the verified re-base's change set names no served base or patch list"
    );
    ensure!(
        change
            .input
            .base_generation
            .is_none_or(|base| base == served.base_generation)
            && change
                .input
                .patches
                .as_ref()
                .is_none_or(|patches| *patches == served_patches),
        "the change set was compared against generation {:?} with patches {:?}, but the manifest \
         serves generation {} with patches {served_patches:?}; it would miss or repeat changes",
        change.input.base_generation,
        change.input.patches,
        served.base_generation
    );
    let upserts = read_pnu_list(lane, &input.upserts)?;
    let deletes = read_pnu_list(lane, &input.deletes)?;
    ensure!(
        upserts.is_disjoint(&deletes),
        "a PNU is both upserted and deleted in one change set"
    );
    ensure!(
        u64::try_from(upserts.len())? == change.quality_metrics.upsert_count
            && u64::try_from(deletes.len())? == change.quality_metrics.delete_count
            && change.quality_metrics.new_count <= change.quality_metrics.upsert_count,
        "the PNU lists ({} upserts, {} deletes) disagree with the change set summary ({}, {})",
        upserts.len(),
        deletes.len(),
        change.quality_metrics.upsert_count,
        change.quality_metrics.delete_count
    );

    let object_count = (served.object_count + change.quality_metrics.new_count)
        .checked_sub(change.quality_metrics.delete_count)
        .context("the change set deletes more PNUs than the lane serves")?;
    let mut next = ServingManifest {
        schema_version: policy.manifest_schema_version,
        unit: lane.unit().to_owned(),
        base_generation: served.base_generation,
        base_object_count: served.base_object_count,
        document_schema_version: served_schema,
        gold_table: served.gold_table.clone(),
        gold_iceberg_snapshot_id: served.gold_iceberg_snapshot_id.clone(),
        reflected_gold_iceberg_snapshot_id: input.expected_gold_iceberg_snapshot_id.clone(),
        pnu_prefix_length: policy.pnu_prefix_length,
        patches: served.patches.clone(),
        object_count,
        published_at_utc: now(),
        verified_rebase: verified_rebase(&change, served)?,
        reflected_gold_snapshot_tag: None,
        section_packs: served.section_packs.clone(),
    };
    if upserts.is_empty() && deletes.is_empty() {
        ensure!(
            input.patch.is_none(),
            "the change set is empty; it advances the reflected snapshot and writes no patch"
        );
        return Ok(next);
    }

    let patch = input
        .patch
        .context("a change set with objects needs a target patch generation")?;
    ensure!(
        patch > served.newest_patch(),
        "patch generation {patch} is not above the newest served patch {}",
        served.newest_patch()
    );
    // A rollback leaves the dropped patches' objects in place, unserved. Their numbers are
    // spent: reusing one would mix its old objects with the new change set's.
    let spent = store
        .list_patches_with_objects(served.base_generation)
        .await?
        .into_iter()
        .filter(|listed| *listed != patch)
        .max()
        .unwrap_or(0);
    ensure!(
        patch > spent,
        "patch generation {patch} is not above patch {spent}, which already holds objects of \
         generation {} (a rolled-back or half-written patch); patch numbers are never reused",
        served.base_generation
    );
    ensure!(
        served.patches.len() < policy.max_patches,
        "the base already carries {} patches, the contract's max_patches; a full bake compacts \
         them",
        served.patches.len()
    );
    let changes = served.cumulative_changes()
        + u64::try_from(upserts.len() + deletes.len()).context("change count overflow")?;
    #[allow(clippy::cast_precision_loss)] // counts far below 2^52
    let ratio = changes as f64 / served.base_object_count.max(1) as f64;
    ensure!(
        ratio <= policy.max_cumulative_change_ratio,
        "the patches would carry {changes} changes, {ratio:.4} of the base's {} objects, over the \
         contract's max_cumulative_change_ratio {}; a full bake compacts them",
        served.base_object_count,
        policy.max_cumulative_change_ratio
    );

    verify_patch_contents(store, input, patch, &upserts, &deletes).await?;
    next.patches.insert(
        0,
        PatchEntry {
            generation: patch,
            gold_iceberg_snapshot_id: input.expected_gold_iceberg_snapshot_id.clone(),
            upserted: u64::try_from(upserts.len())?,
            deleted: u64::try_from(deletes.len())?,
            prefixes: pnu_prefixes(upserts.iter().chain(&deletes).map(String::as_str))?,
        },
    );
    Ok(next)
}

/// Gates (가), (나) and (다) of root ADR-0141 §7 over one patch generation.
async fn verify_patch_contents(
    store: &ByPnuServingStore,
    input: &PatchInput,
    patch: u64,
    upserts: &BTreeSet<String>,
    deletes: &BTreeSet<String>,
) -> anyhow::Result<()> {
    let lane = store.lane();
    let generation = input.base_generation;
    let listed = store.list_patch_keys(generation, patch, None).await?;
    let mut expected = BTreeSet::new();
    for pnu in upserts.iter().chain(deletes) {
        expected.insert(by_pnu::patch_object_key(lane, generation, patch, pnu)?);
    }
    // (가) every PNU of the change set is in the patch, as a document or a tombstone.
    let missing = expected
        .iter()
        .filter(|key| !listed.contains(*key))
        .collect::<Vec<_>>();
    ensure!(
        missing.is_empty(),
        "patch {patch} lacks {} objects of its change set (first: {}); the export did not finish",
        missing.len(),
        missing.first().map_or("", |key| key.as_str())
    );
    // (다) nothing outside the change set is in the patch.
    let mut foreign = listed
        .iter()
        .filter(|key| !expected.contains(*key))
        .collect::<Vec<_>>();
    foreign.sort_unstable();
    ensure!(
        foreign.is_empty(),
        "patch {patch} holds {} objects outside its change set (first: {}); another writer used \
         this patch generation",
        foreign.len(),
        foreign.first().map_or("", |key| key.as_str())
    );
    // (나) a spread of the patch is of the stated snapshot: documents for upserts, tombstones for
    // deletes.
    let snapshot = &input.expected_gold_iceberg_snapshot_id;
    let keys = expected.into_iter().collect::<Vec<_>>();
    let step = keys.len().div_ceil(MAX_VERIFICATION_SAMPLES).max(1);
    for key in keys.iter().step_by(step) {
        let (_, pnu) = key_pnu(key)?;
        if deletes.contains(&pnu) {
            let stored = store.read_bytes(key).await?;
            let tombstone = read_tombstone(&stored)
                .with_context(|| format!("{key} should be a tombstone and is not"))?;
            ensure!(
                tombstone.pnu == pnu && &tombstone.source.iceberg_snapshot_id == snapshot,
                "{key} is a tombstone of {} from snapshot {}, not of {pnu} from {snapshot}",
                tombstone.pnu,
                tombstone.source.iceberg_snapshot_id
            );
        } else {
            verify_document(store, key, &pnu, snapshot).await?;
        }
    }
    Ok(())
}

/// The document schema the served base holds: the manifest says it, or — for a v1 manifest —
/// its first object does.
async fn served_document_schema(
    store: &ByPnuServingStore,
    served: &ServedManifest,
) -> anyhow::Result<String> {
    if let Some(schema) = &served.document_schema_version {
        return Ok(schema.clone());
    }
    let key = store
        .first_object_key(served.base_generation)
        .await?
        .with_context(|| {
            format!(
                "generation {} holds no objects to read its document schema from",
                served.base_generation
            )
        })?;
    let document: serde_json::Value = serde_json::from_slice(&store.read_bytes(&key).await?)
        .with_context(|| format!("{key} does not hold a JSON document"))?;
    document
        .get("schema_version")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .with_context(|| format!("{key} names no schema_version"))
}

/// The live manifest minus its newest patches, as the history stored it.
pub(crate) async fn rollback(
    store: &ByPnuServingStore,
    history_key: &str,
    served: &ServedManifest,
) -> anyhow::Result<ServingManifest> {
    let lane = store.lane();
    ensure!(
        by_pnu::is_manifest_history_key(lane, history_key),
        "{history_key} is not a {} manifest history key",
        lane.noun()
    );
    let target = ServedManifest::parse(lane, &store.read_bytes(history_key).await?)
        .with_context(|| format!("{history_key} does not hold a readable manifest"))?;
    ensure!(
        target.base_generation == served.base_generation,
        "{history_key} serves generation {} but the live manifest serves {}; a rollback drops \
         patches of the live base, it does not change the base",
        target.base_generation,
        served.base_generation
    );
    // A rollback undoes the newest object patches or the newest pack state, one at a time; the
    // other part must be what the live manifest serves.
    let objects_rolled_back = target.patches.len() < served.patches.len()
        && served.patches.ends_with(&target.patches)
        && target.section_packs == served.section_packs;
    let packs_rolled_back = target.patches == served.patches
        && match (&target.section_packs, &served.section_packs) {
            (None, Some(_)) => true,
            (Some(back), Some(live)) => {
                back.sections == live.sections
                    && back.patches.len() < live.patches.len()
                    && live.patches.ends_with(&back.patches)
            }
            _ => false,
        };
    ensure!(
        objects_rolled_back || packs_rolled_back,
        "{history_key}'s patches are not the live patch list minus its newest entries, and its \
         section packs are not the live packs minus their newest patches or none"
    );
    let schema = match target.document_schema_version {
        Some(schema) => schema,
        None => served_document_schema(store, served).await?,
    };
    let policy = by_pnu_serving_patch_policy()?;
    // A target with patches keeps the prefix length they are listed by; the write then holds it
    // to the contract as it is now.
    let pnu_prefix_length = if target.patches.is_empty() {
        policy.pnu_prefix_length
    } else {
        target
            .pnu_prefix_length
            .with_context(|| format!("{history_key} lists patches but no prefix length"))?
    };
    Ok(ServingManifest {
        schema_version: policy.manifest_schema_version,
        unit: target.unit,
        base_generation: target.base_generation,
        base_object_count: target.base_object_count,
        document_schema_version: schema,
        gold_table: target.gold_table,
        gold_iceberg_snapshot_id: target.gold_iceberg_snapshot_id,
        reflected_gold_iceberg_snapshot_id: target.reflected_gold_iceberg_snapshot_id,
        pnu_prefix_length,
        patches: target.patches,
        object_count: target.object_count,
        published_at_utc: now(),
        verified_rebase: None,
        reflected_gold_snapshot_tag: None,
        section_packs: target.section_packs,
    })
}

#[derive(Deserialize)]
struct GatewayCapabilities {
    unit: String,
    manifest_schema_versions: Vec<u32>,
}

/// Refuses unless the lane's public gateway says it reads manifest `schema_version`.
pub(crate) async fn require_gateway_reads(
    lane: ByPnuLane,
    gateway_base_url: &str,
    schema_version: u32,
) -> anyhow::Result<()> {
    let url = format!(
        "{}{}",
        gateway_base_url.trim_end_matches('/'),
        lane.policy()?.request_path.capabilities
    );
    let refuse = |why: String| {
        anyhow::anyhow!(
            "the {} gateway at {url} {why}; a v{schema_version} manifest would turn every request \
             into a 503 — deploy the gateway first (root ADR-0141)",
            lane.unit()
        )
    };
    let response = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(
            GATEWAY_PROBE_TIMEOUT_SECONDS,
        ))
        .build()?
        .get(&url)
        .send()
        .await
        .map_err(|error| refuse(format!("could not be asked ({error})")))?;
    if !response.status().is_success() {
        return Err(refuse(format!("answered {}", response.status())));
    }
    let capabilities: GatewayCapabilities = response.json().await.map_err(|error| {
        refuse(format!(
            "answered something other than its capabilities ({error})"
        ))
    })?;
    if capabilities.unit != lane.unit()
        || !capabilities
            .manifest_schema_versions
            .contains(&schema_version)
    {
        return Err(refuse(format!(
            "reads {} manifests {:?}",
            capabilities.unit, capabilities.manifest_schema_versions
        )));
    }
    Ok(())
}

pub(crate) fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value.trim().to_owned())),
        Ok(_) | Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

#[cfg(test)]
mod tests;
