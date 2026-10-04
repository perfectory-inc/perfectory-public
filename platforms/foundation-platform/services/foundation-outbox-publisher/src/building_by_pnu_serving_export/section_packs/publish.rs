//! `publish-building-by-pnu-section-packs`: points the building gateway at section packs (root
//! ADR-0147 §4–§6).
//!
//! The manifest stays one v2 envelope; this command writes its `section_packs` block and leaves
//! every v2 field as it is. Inputs: a directory of pack export summaries of one Gold snapshot.
//!
//! - **base** — the summaries hold base packs of one generation per section. Every section listed
//!   must hold exactly the summaries' packs, every row of Gold (`PACK_EXPECTED_DOCUMENT_COUNT`),
//!   and a sample of them must read back as the summaries recorded. The **first** base publish
//!   (no `section_packs` yet) is the cut-over: it also needs the gateway to say it reads
//!   `section_packs`, and the equality and latency evidence of that generation (gate 가, 나), both
//!   passing, the equality evidence of the object state the manifest still serves.
//! - **patch** — the summaries hold patch `m` of every section: exactly the change set's upserts as
//!   documents and its deletes as tombstones (root ADR-0141 §7 on packs).
//!
//! Every write goes over the version read at the start (compare-and-swap), after the replaced
//! manifest is stored in the history, and pins the Gold snapshot the packs reflect.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::super::{building_document, optional_env, LANE};
use super::bake::{PackEntry, PackExportSummary, SUMMARY_SCHEMA_VERSION};
use super::gate;
use crate::by_pnu_gateway_contract::section_pack_policy;
use crate::by_pnu_pack::Pack;
use crate::by_pnu_section_pack_manifest::{
    CutoverRecord, PackPatch, SectionPacksState, SectionState,
};
use crate::by_pnu_serving_manifest::{ServedManifest, ServingManifest, StoredManifest};
use crate::by_pnu_serving_manifest_publish::{self as manifest_publish, gold_table};
use crate::by_pnu_serving_patch_export::read_pnu_list;
use crate::by_pnu_serving_pins::{self as pins, SnapshotPins};
use crate::by_pnu_serving_store::{local_root, ByPnuServingStore};
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::r2_layout::by_pnu_packs;

/// Packs re-read per section before a publish.
const SAMPLES_PER_SECTION: usize = 16;

#[derive(Clone, Debug)]
pub(crate) struct PublishConfig {
    pub(crate) output: ProfileStoreConfig,
    pub(crate) summary_dir: PathBuf,
    pub(crate) expected_gold_snapshot: String,
    pub(crate) expected_document_count: Option<u64>,
    pub(crate) equality_evidence: Option<PathBuf>,
    pub(crate) latency_evidence: Option<PathBuf>,
    pub(crate) change_set: Option<ChangeSetPaths>,
}

#[derive(Clone, Debug)]
pub(crate) struct ChangeSetPaths {
    pub(crate) summary: PathBuf,
    pub(crate) upserts: PathBuf,
    pub(crate) deletes: PathBuf,
}

impl PublishConfig {
    fn from_env() -> anyhow::Result<Self> {
        let env = |name: &str| optional_env(&LANE.env(name));
        let required = |name: &str| -> anyhow::Result<String> {
            env(name)?.with_context(|| format!("{} is required", LANE.env(name)))
        };
        ensure!(
            env("CONFIRM_PACK_PUBLISH")?.is_some_and(|value| value.eq_ignore_ascii_case("true")),
            "{} must be true",
            LANE.env("CONFIRM_PACK_PUBLISH")
        );
        let change_set = match env("CHANGE_SET_SUMMARY_PATH")? {
            Some(summary) => Some(ChangeSetPaths {
                summary: PathBuf::from(summary),
                upserts: PathBuf::from(required("UPSERT_LIST_PATH")?),
                deletes: PathBuf::from(required("DELETE_LIST_PATH")?),
            }),
            None => None,
        };
        Ok(Self {
            output: ProfileStoreConfig::parse(
                env("OUTPUT_STORAGE_DRIVER")?
                    .unwrap_or_else(|| "local".to_owned())
                    .as_str(),
                local_root(env("OUTPUT_ROOT")?),
            )?,
            summary_dir: PathBuf::from(required("PACK_SUMMARY_DIR")?),
            expected_gold_snapshot: required("EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID")?,
            expected_document_count: env("PACK_EXPECTED_DOCUMENT_COUNT")?
                .map(|raw| raw.parse::<u64>())
                .transpose()
                .context("the expected document count must be a number")?,
            equality_evidence: env("PACK_EQUALITY_EVIDENCE_PATH")?.map(PathBuf::from),
            latency_evidence: env("PACK_LATENCY_EVIDENCE_PATH")?.map(PathBuf::from),
            change_set,
        })
    }
}

/// Runs the publish against the lane's public gateway.
///
/// # Errors
/// Refuses on any failed gate; the manifest is then unchanged.
pub(crate) async fn run() -> anyhow::Result<()> {
    super::sections::check_contract_sections()?;
    let config = PublishConfig::from_env()?;
    let store = ByPnuServingStore::open(LANE, &config.output)?;
    let gateway = format!("https://{}", LANE.policy()?.public_hostname);
    let state = publish(&config, &store, &gateway).await?;
    tracing::info!(
        reflected_gold_iceberg_snapshot_id = %state.reflected_gold_iceberg_snapshot_id,
        document_count = state.document_count,
        patches = state.patches.len(),
        "building section packs published"
    );
    Ok(())
}

/// Publishes what the summaries name, after every gate.
///
/// # Errors
/// Refuses on any failed gate; the manifest is then unchanged.
pub(crate) async fn publish(
    config: &PublishConfig,
    store: &ByPnuServingStore,
    gateway_base_url: &str,
) -> anyhow::Result<SectionPacksState> {
    let (bytes, version) = store.read_manifest().await.context(
        "the building lane has no readable manifest; packs are published over a served lane",
    )?;
    let live = ServedManifest::parse(LANE, &bytes)
        .context("the live manifest cannot be read; refusing to replace it")?;
    ensure!(
        live.wire_schema_version == 2 && live.document_schema_version.is_some(),
        "the live manifest is v{}; publish a v2 manifest of the objects before packs",
        live.wire_schema_version
    );
    let summaries = read_summaries(&config.summary_dir, &config.expected_gold_snapshot)?;
    let patch = summaries[0].patch;
    let mut state = match patch {
        None => base(config, store, &live, &summaries, gateway_base_url).await?,
        Some(patch) => patched(config, store, &live, &summaries, patch).await?,
    };
    let published_at_utc = manifest_publish::now();
    let snapshot_pins = SnapshotPins::for_output(&config.output)?;
    state.reflected_gold_snapshot_tag = Some(
        pins::pin(
            &snapshot_pins,
            LANE,
            gold_table(LANE),
            &state.reflected_gold_iceberg_snapshot_id,
            &published_at_utc,
        )
        .await?,
    );
    let manifest = with_packs(&live, state.clone(), published_at_utc)?;
    let body = manifest.to_bytes()?;
    let stored = StoredManifest {
        manifest: live,
        bytes,
        version,
    };
    if let Err(error) = manifest_publish::commit(store, Some(&stored), &body).await {
        // A write whose answer was lost after it landed is a success; anything else is not, and
        // the new pin stays until the next publish releases it (root ADR-0146 §2).
        match store.read_manifest().await {
            Ok((now_live, _)) if now_live == body => {
                tracing::warn!(error = %format!("{error:#}"), "the manifest write reported a failure but it is live");
            }
            _ => return Err(error),
        }
    }
    manifest_publish::release_behind_live(store, &snapshot_pins).await;
    Ok(state)
}

/// The live manifest with a new `section_packs` block; every v2 field unchanged.
fn with_packs(
    live: &ServedManifest,
    state: SectionPacksState,
    published_at_utc: String,
) -> anyhow::Result<ServingManifest> {
    Ok(ServingManifest {
        schema_version: live.wire_schema_version,
        unit: live.unit.clone(),
        base_generation: live.base_generation,
        base_object_count: live.base_object_count,
        document_schema_version: live
            .document_schema_version
            .clone()
            .context("a v2 manifest names its document schema")?,
        gold_table: live.gold_table.clone(),
        gold_iceberg_snapshot_id: live.gold_iceberg_snapshot_id.clone(),
        reflected_gold_iceberg_snapshot_id: live.reflected_gold_iceberg_snapshot_id.clone(),
        pnu_prefix_length: live
            .pnu_prefix_length
            .context("a v2 manifest names its prefix length")?,
        patches: live.patches.clone(),
        object_count: live.object_count,
        published_at_utc,
        verified_rebase: None,
        reflected_gold_snapshot_tag: live.reflected_gold_snapshot_tag.clone(),
        section_packs: Some(state),
    })
}

/// Every `*.json` export summary of the directory, all of one snapshot and one patch.
fn read_summaries(dir: &Path, expected_snapshot: &str) -> anyhow::Result<Vec<PackExportSummary>> {
    let mut paths = std::fs::read_dir(dir)
        .with_context(|| format!("failed to read the summary directory {}", dir.display()))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    paths.retain(|path| path.extension().is_some_and(|ext| ext == "json"));
    paths.sort();
    let mut summaries = Vec::with_capacity(paths.len());
    for path in &paths {
        let summary: PackExportSummary = serde_json::from_slice(&std::fs::read(path)?)
            .with_context(|| format!("{} is not a pack export summary", path.display()))?;
        ensure!(
            summary.schema_version == SUMMARY_SCHEMA_VERSION,
            "{} is a {} summary",
            path.display(),
            summary.schema_version
        );
        ensure!(
            summary.gold_table == gold_table(LANE)
                && summary.gold_iceberg_snapshot_id == expected_snapshot
                && summary.document_schema_version
                    == building_document::BUILDING_DOCUMENT_SCHEMA_VERSION,
            "{} is of {} snapshot {} ({}), not of {} snapshot {expected_snapshot} as baked now",
            path.display(),
            summary.gold_table,
            summary.gold_iceberg_snapshot_id,
            summary.document_schema_version,
            gold_table(LANE)
        );
        summaries.push(summary);
    }
    ensure!(
        !summaries.is_empty(),
        "{} holds no pack export summary",
        dir.display()
    );
    ensure!(
        summaries
            .iter()
            .all(|summary| summary.patch == summaries[0].patch),
        "the summaries mix base packs and patches, or patches of different numbers"
    );
    Ok(summaries)
}

/// Per section: its generation and its packs, from every summary.
fn packs_by_section(
    summaries: &[PackExportSummary],
) -> anyhow::Result<BTreeMap<String, (u64, Vec<PackEntry>)>> {
    let mut by_section: BTreeMap<String, (u64, Vec<PackEntry>)> = BTreeMap::new();
    let mut keys = BTreeSet::new();
    for summary in summaries {
        for pack in &summary.packs {
            let parsed = by_pnu_packs::parse_pack_key(LANE, &pack.key)
                .with_context(|| format!("{} is not a building pack key", pack.key))?;
            ensure!(
                parsed.section == pack.section
                    && parsed.unit == pack.unit
                    && parsed.patch == summary.patch
                    && parsed.generation == summary.generation,
                "summary entry {} disagrees with its own key",
                pack.key
            );
            ensure!(
                keys.insert(pack.key.clone()),
                "two summaries name {}",
                pack.key
            );
            let entry = by_section
                .entry(pack.section.clone())
                .or_insert((summary.generation, Vec::new()));
            ensure!(
                entry.0 == summary.generation,
                "section {} is in two generations across the summaries",
                pack.section
            );
            entry.1.push(pack.clone());
        }
    }
    Ok(by_section)
}

/// The listing of each section's directory holds exactly the summaries' packs, and a sample of
/// them reads back as recorded.
async fn verify_packs(
    store: &ByPnuServingStore,
    section: &str,
    generation: u64,
    patch: Option<u64>,
    packs: &[PackEntry],
    snapshot: &str,
) -> anyhow::Result<()> {
    let listed = store.list_pack_keys(section, generation, patch).await?;
    let named = packs
        .iter()
        .map(|pack| pack.key.clone())
        .collect::<BTreeSet<_>>();
    ensure!(
        listed == named,
        "section {section} generation {generation} patch {patch:?} lists {} packs but the \
         summaries name {}; a shard is missing or foreign packs crept in",
        listed.len(),
        named.len()
    );
    let step = packs.len().div_ceil(SAMPLES_PER_SECTION).max(1);
    for pack in packs.iter().step_by(step) {
        let bytes = store.read_bytes(&pack.key).await?;
        ensure!(
            format!("{:x}", Sha256::digest(&bytes)) == pack.sha256,
            "{} holds bytes other than its summary recorded",
            pack.key
        );
        let read = Pack::read(&bytes).with_context(|| format!("{} does not read", pack.key))?;
        ensure!(
            read.header.section == section
                && read.header.unit == pack.unit
                && read.header.generation == generation
                && read.header.patch == patch
                && read.header.gold_iceberg_snapshot_id == snapshot
                && read.header.document_count == pack.documents
                && read.header.tombstone_count == pack.tombstones,
            "{}'s header disagrees with its summary",
            pack.key
        );
    }
    Ok(())
}

async fn base(
    config: &PublishConfig,
    store: &ByPnuServingStore,
    live: &ServedManifest,
    summaries: &[PackExportSummary],
    gateway_base_url: &str,
) -> anyhow::Result<SectionPacksState> {
    let expected = config.expected_document_count.with_context(|| {
        format!(
            "a base publish states the Gold row count as {}",
            LANE.env("PACK_EXPECTED_DOCUMENT_COUNT")
        )
    })?;
    let snapshot = config.expected_gold_snapshot.as_str();
    let by_section = packs_by_section(summaries)?;
    let contract = &LANE.section_packs()?.sections;
    let mut units: Option<BTreeSet<String>> = None;
    for (section, (generation, packs)) in &by_section {
        let documents: u64 = packs.iter().map(|pack| pack.documents).sum();
        ensure!(
            documents == expected && packs.iter().all(|pack| pack.tombstones == 0),
            "section {section} holds {documents} documents but Gold holds {expected} rows"
        );
        let these = packs
            .iter()
            .map(|pack| pack.unit.clone())
            .collect::<BTreeSet<_>>();
        ensure!(
            units.as_ref().is_none_or(|units| *units == these),
            "section {section} covers other legal dongs than the other sections"
        );
        units = Some(these);
        let (generations, _) = store.list_pack_numbers(section).await?;
        ensure!(
            generations.last() == Some(generation),
            "section {section} generation {generation} is not above every generation holding \
             packs ({generations:?})"
        );
        verify_packs(store, section, *generation, None, packs, snapshot).await?;
    }
    let pack_count = |section: &str| -> anyhow::Result<u64> {
        Ok(u64::try_from(
            by_section.get(section).map_or(0, |(_, packs)| packs.len()),
        )?)
    };
    let section_state = |name: &str, patch_floor: u64| -> anyhow::Result<SectionState> {
        let (generation, _) = by_section
            .get(name)
            .with_context(|| format!("no summary holds section {name}"))?;
        Ok(SectionState {
            name: name.to_owned(),
            generation: *generation,
            gold_iceberg_snapshot_id: snapshot.to_owned(),
            document_count: expected,
            pack_count: pack_count(name)?,
            patch_floor,
        })
    };
    let policy = section_pack_policy()?;
    match &live.section_packs {
        None => {
            ensure!(
                by_section
                    .keys()
                    .eq(contract.iter().collect::<BTreeSet<_>>()),
                "the first pack publish carries every contract section {contract:?}"
            );
            let generation = by_section
                .values()
                .next()
                .map_or(0, |(generation, _)| *generation);
            ensure!(
                by_section.values().all(|(g, _)| *g == generation),
                "the first pack publish is one generation of every section"
            );
            manifest_publish::require_gateway_reads(
                LANE,
                gateway_base_url,
                policy.manifest_section_packs_schema_version,
            )
            .await?;
            let cutover = cutover_gate(config, generation, snapshot, expected)?;
            Ok(SectionPacksState {
                schema_version: policy.manifest_section_packs_schema_version,
                format_version: policy.format_version,
                unit_prefix_length: policy.unit_prefix_length,
                document_schema_version: building_document::BUILDING_DOCUMENT_SCHEMA_VERSION
                    .to_owned(),
                gold_table: gold_table(LANE).to_owned(),
                reflected_gold_iceberg_snapshot_id: snapshot.to_owned(),
                reflected_gold_snapshot_tag: None,
                document_count: expected,
                sections: contract
                    .iter()
                    .map(|name| section_state(name, 0))
                    .collect::<anyhow::Result<_>>()?,
                patches: Vec::new(),
                cutover: Some(cutover),
            })
        }
        Some(current) => {
            let whole = by_section
                .keys()
                .eq(contract.iter().collect::<BTreeSet<_>>());
            let at_reflected = snapshot == current.reflected_gold_iceberg_snapshot_id;
            ensure!(
                whole || at_reflected,
                "a section re-baked alone must be of the reflected snapshot {}; a new snapshot \
                 re-bakes every section",
                current.reflected_gold_iceberg_snapshot_id
            );
            // A re-baked section holds every patch so far: of the reflected snapshot when alone,
            // of a newer one when every section is re-baked (then no patch stays).
            let floor = current.newest_patch();
            let mut next = current.clone();
            for section in &mut next.sections {
                if by_section.contains_key(&section.name) {
                    ensure!(
                        by_section[&section.name].0 > section.generation,
                        "section {} generation may only move forward from {}",
                        section.name,
                        section.generation
                    );
                    *section = section_state(&section.name, floor)?;
                }
            }
            let lowest = next
                .sections
                .iter()
                .map(|s| s.patch_floor)
                .min()
                .unwrap_or(0);
            next.patches.retain(|patch| patch.patch > lowest);
            if !at_reflected {
                next.reflected_gold_iceberg_snapshot_id = snapshot.to_owned();
                next.document_count = expected;
            }
            ensure!(
                next.document_count == expected,
                "the packs answer for {} PNUs but the re-baked sections hold {expected}",
                next.document_count
            );
            next.reflected_gold_snapshot_tag = None;
            Ok(next)
        }
    }
}

/// Gate (가)+(나) of the first publish, or the refusal naming what is missing.
fn cutover_gate(
    config: &PublishConfig,
    generation: u64,
    snapshot: &str,
    expected_documents: u64,
) -> anyhow::Result<CutoverRecord> {
    let (Some(equality), Some(latency)) = (&config.equality_evidence, &config.latency_evidence)
    else {
        bail!(
            "the first pack publish is the cut-over (root ADR-0147 §6): it needs {} and {} of \
             generation {generation}, both passing",
            LANE.env("PACK_EQUALITY_EVIDENCE_PATH"),
            LANE.env("PACK_LATENCY_EVIDENCE_PATH")
        );
    };
    let (equality, equality_sha256) = gate::read::<gate::EqualityEvidence>(equality)?;
    gate::require_equality(&equality, generation, snapshot, expected_documents)?;
    let (latency, latency_sha256) = gate::read::<gate::LatencyEvidence>(latency)?;
    gate::require_latency(&latency, generation, &equality)?;
    Ok(CutoverRecord {
        equality_evidence_sha256: equality_sha256,
        latency_evidence_sha256: latency_sha256,
    })
}

#[derive(Deserialize)]
struct ChangeSetSummary {
    quality_metrics: ChangeSetCounts,
    input: ChangeSetInput,
}

#[derive(Deserialize)]
struct ChangeSetCounts {
    new_count: u64,
    upsert_count: u64,
    delete_count: u64,
}

#[derive(Deserialize)]
struct ChangeSetInput {
    baseline_snapshot_id: String,
    current_snapshot_id: String,
}

async fn patched(
    config: &PublishConfig,
    store: &ByPnuServingStore,
    live: &ServedManifest,
    summaries: &[PackExportSummary],
    patch: u64,
) -> anyhow::Result<SectionPacksState> {
    let current = live
        .section_packs
        .as_ref()
        .context("a pack patch needs published section packs")?;
    let paths = config.change_set.as_ref().with_context(|| {
        format!(
            "a pack patch names its change set ({})",
            LANE.env("CHANGE_SET_SUMMARY_PATH")
        )
    })?;
    let change: ChangeSetSummary = serde_json::from_slice(&std::fs::read(&paths.summary)?)
        .context("the change set summary does not parse")?;
    let upserts = read_pnu_list(LANE, &paths.upserts)?;
    let deletes = read_pnu_list(LANE, &paths.deletes)?;
    let snapshot = config.expected_gold_snapshot.as_str();
    ensure!(
        change.input.baseline_snapshot_id == current.reflected_gold_iceberg_snapshot_id
            && change.input.current_snapshot_id == snapshot,
        "the change set goes from {} to {}, but the packs reflect {} and the patch is of {snapshot}",
        change.input.baseline_snapshot_id,
        change.input.current_snapshot_id,
        current.reflected_gold_iceberg_snapshot_id
    );
    ensure!(
        change.quality_metrics.upsert_count == u64::try_from(upserts.len())?
            && change.quality_metrics.delete_count == u64::try_from(deletes.len())?
            && upserts.is_disjoint(&deletes),
        "the change set summary disagrees with its lists"
    );
    ensure!(
        !(upserts.is_empty() && deletes.is_empty()),
        "an empty change set writes no patch"
    );
    let mut used = BTreeSet::new();
    for section in &current.sections {
        used.extend(store.list_pack_numbers(&section.name).await?.1);
    }
    ensure!(
        patch > current.newest_patch() && used.last().is_none_or(|last| *last <= patch),
        "patch {patch} is not above every patch number in use ({used:?}, newest published {})",
        current.newest_patch()
    );
    let by_section = packs_by_section(summaries)?;
    let mut units: Option<BTreeSet<String>> = None;
    for section in &current.sections {
        let (generation, packs) = by_section.get(&section.name).with_context(|| {
            format!("no summary holds patch {patch} of section {}", section.name)
        })?;
        ensure!(
            *generation == section.generation,
            "patch {patch} of {} was baked under generation {generation}, the section serves {}",
            section.name,
            section.generation
        );
        let mut documents = BTreeSet::new();
        let mut tombstones = 0_u64;
        for pack in packs {
            documents.extend(pack.pnus.iter().cloned());
            tombstones += pack.tombstones;
        }
        let changed = upserts.union(&deletes).cloned().collect::<BTreeSet<_>>();
        ensure!(
            documents == changed
                && tombstones == u64::try_from(deletes.len())?
                && packs.iter().map(|p| p.documents).sum::<u64>() == u64::try_from(upserts.len())?,
            "patch {patch} of {} does not hold exactly the change set: a document for every \
             upsert, a tombstone for every delete, nothing else",
            section.name
        );
        let these = packs
            .iter()
            .map(|pack| pack.unit.clone())
            .collect::<BTreeSet<_>>();
        ensure!(
            units.as_ref().is_none_or(|units| *units == these),
            "patch {patch} covers other legal dongs in section {}",
            section.name
        );
        units = Some(these);
        verify_packs(
            store,
            &section.name,
            *generation,
            Some(patch),
            packs,
            snapshot,
        )
        .await?;
    }
    let mut next = current.clone();
    next.patches.insert(
        0,
        PackPatch {
            patch,
            gold_iceberg_snapshot_id: snapshot.to_owned(),
            upserted: u64::try_from(upserts.len())?,
            deleted: u64::try_from(deletes.len())?,
            units: units.unwrap_or_default().into_iter().collect(),
        },
    );
    next.reflected_gold_iceberg_snapshot_id = snapshot.to_owned();
    next.document_count = (current.document_count + change.quality_metrics.new_count)
        .checked_sub(u64::try_from(deletes.len())?)
        .context("the change set deletes more PNUs than the packs answer for")?;
    next.reflected_gold_snapshot_tag = None;
    Ok(next)
}
