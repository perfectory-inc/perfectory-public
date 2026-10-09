//! `publish-<lane>-by-pnu-section-packs`: points a lane's gateway at section packs (root
//! ADR-0147 §4–§6).
//!
//! The manifest stays one v2 envelope; this command writes its `section_packs` block and leaves
//! every v2 field as it is. Inputs: a directory of pack export summaries of one Gold snapshot.
//!
//! - **base** — the summaries hold base packs of one generation per section. Every section listed
//!   must hold exactly the summaries' packs, every row of the Gold snapshot (the count the
//!   catalog's table metadata records; `PACK_EXPECTED_DOCUMENT_COUNT`, when stated, must agree),
//!   and a sample of them must read back as the summaries recorded. The **first** base publish
//!   (no `section_packs` yet) is the cut-over: it also needs the gateway to say it reads
//!   `section_packs`, the installed scheduled bake to declare that it bakes pack patches (else the
//!   packs would stop taking daily changes; runbook 7절), and the equality and latency evidence of
//!   that generation (gate 가, 나), both passing.
//! - **patch** — the summaries hold patch `m` of every section, each under the generation the lane
//!   serves that section from: exactly the change set's upserts as documents and its deletes as
//!   tombstones (root ADR-0141 §7 on packs).
//! - **reflect** — no summaries and an empty change set: the packs already hold the new snapshot,
//!   only the reflected snapshot moves.
//!
//! Every write goes over the version read at the start (compare-and-swap), after the replaced
//! manifest is stored in the history, and pins the Gold snapshot the packs reflect.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::super::optional_env;
use super::bake::{summary_schema_version, PackEntry, PackExportSummary};
use super::{equality, gate};
use crate::by_pnu_gateway_contract::{section_pack_policy, ByPnuLane};
use crate::by_pnu_pack::Pack;
use crate::by_pnu_section_pack_manifest::{
    CutoverRecord, PackPatch, PartsRef, SectionPacksState, SectionState,
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
/// Where the host keeps the installed release's job list (`orchestration/dags/job_specs.py`
/// `RELEASE_PREFIX`).
const INSTALLED_JOBS: &str = "/opt/foundation-platform/current/orchestration/jobs.v1.json";

#[derive(Clone, Debug)]
pub(crate) struct PublishConfig {
    pub(crate) lane: ByPnuLane,
    pub(crate) output: ProfileStoreConfig,
    /// The export summaries; none for a reflect, which writes no pack.
    pub(crate) summary_dir: Option<PathBuf>,
    pub(crate) expected_gold_snapshot: String,
    /// The Gold snapshot's row count as the catalog records it (cross-checked against a stated one).
    pub(crate) gold_record_count: Option<u64>,
    /// The installed release's `orchestration/jobs.v1.json`, read by the first pack publish.
    pub(crate) installed_jobs: PathBuf,
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
    fn from_env(lane: ByPnuLane) -> anyhow::Result<Self> {
        let env = |name: &str| optional_env(&lane.env(name));
        let required = |name: &str| -> anyhow::Result<String> {
            env(name)?.with_context(|| format!("{} is required", lane.env(name)))
        };
        ensure!(
            env("CONFIRM_PACK_PUBLISH")?.is_some_and(|value| value.eq_ignore_ascii_case("true")),
            "{} must be true",
            lane.env("CONFIRM_PACK_PUBLISH")
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
            lane,
            output: ProfileStoreConfig::parse(
                env("OUTPUT_STORAGE_DRIVER")?
                    .unwrap_or_else(|| "local".to_owned())
                    .as_str(),
                local_root(env("OUTPUT_ROOT")?),
            )?,
            summary_dir: env("PACK_SUMMARY_DIR")?.map(PathBuf::from),
            expected_gold_snapshot: required("EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID")?,
            gold_record_count: None,
            installed_jobs: PathBuf::from(
                env("INSTALLED_JOBS_PATH")?.unwrap_or_else(|| INSTALLED_JOBS.to_owned()),
            ),
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
pub(crate) async fn run(lane: ByPnuLane) -> anyhow::Result<()> {
    super::sections::check_contract_sections(lane)?;
    let mut config = PublishConfig::from_env(lane)?;
    let stated = optional_env(&lane.env("PACK_EXPECTED_DOCUMENT_COUNT"))?
        .map(|raw| raw.parse::<u64>())
        .transpose()
        .context("the expected document count must be a number")?;
    config.gold_record_count =
        Some(gate::gold_record_count(lane, &config.expected_gold_snapshot, stated).await?);
    let store = ByPnuServingStore::open(lane, &config.output)?;
    let gateway = format!("https://{}", lane.policy()?.public_hostname);
    let state = publish(&config, &store, &gateway).await?;
    tracing::info!(
        reflected_gold_iceberg_snapshot_id = %state.reflected_gold_iceberg_snapshot_id,
        document_count = state.document_count,
        patches = state.patches.len(),
        lane = lane.unit(),
        "section packs published"
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
    let (bytes, version) = store.read_manifest().await.with_context(|| {
        format!(
            "the {} lane has no readable manifest; packs are published over a served lane",
            config.lane.unit()
        )
    })?;
    let live = ServedManifest::parse(config.lane, &bytes)
        .context("the live manifest cannot be read; refusing to replace it")?;
    let live = if live.wire_schema_version == 1 {
        restate_v1(config, store, live).await?
    } else {
        live
    };
    ensure!(
        live.wire_schema_version == 2 && live.document_schema_version.is_some(),
        "the live manifest is v{}; publish a v2 manifest of the objects before packs",
        live.wire_schema_version
    );
    let mut state = match &config.summary_dir {
        None => reflected(config, &live)?,
        Some(dir) => {
            let summaries = read_summaries(config.lane, dir, &config.expected_gold_snapshot)?;
            match summaries[0].patch {
                None => base(config, store, &live, &summaries, gateway_base_url).await?,
                Some(patch) => patched(config, store, &live, &summaries, patch).await?,
            }
        }
    };
    let published_at_utc = manifest_publish::now();
    let snapshot_pins = SnapshotPins::for_output(&config.output)?;
    state.reflected_gold_snapshot_tag = Some(
        pins::pin(
            &snapshot_pins,
            config.lane,
            gold_table(config.lane),
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
/// How many sample documents of the served base a v1 manifest must hold before it is restated.
const V1_RESTATE_SAMPLES: usize = 64;

/// A v1 manifest restated as v2 by the first pack publish over it: the same base generation,
/// count and snapshot, now naming the lane's document schema and prefix length (the gateway reads
/// a v1 manifest as exactly that). A lane whose reflected snapshot predates row_digest cannot reach
/// v2 by a patch, and restating it from a listing held every key of the base in memory (2026-10-08,
/// parcel g2: 39.86M keys, killed after five hours). So the base is checked where the cut-over
/// already looks: [`V1_RESTATE_SAMPLES`] PNUs of gate (가)'s sample must each be read from the base
/// generation and hold this lane's document of that PNU, baked from the manifest's snapshot. A
/// sample PNU the base does not hold (new in Gold since) is skipped; as many skips as samples
/// refuses.
async fn restate_v1(
    config: &PublishConfig,
    store: &ByPnuServingStore,
    live: ServedManifest,
) -> anyhow::Result<ServedManifest> {
    let evidence = config.equality_evidence.as_ref().with_context(|| {
        format!(
            "the live manifest is v1; the first pack publish restates it from the base's sample \
             documents and needs {}",
            config.lane.env("PACK_EQUALITY_EVIDENCE_PATH")
        )
    })?;
    let (equality, _) = gate::read::<gate::EqualityEvidence>(evidence)?;
    // A gate sample is 10,000 PNUs; a smaller one (a test lane) is read whole.
    let wanted = V1_RESTATE_SAMPLES.min(equality.sample.len());
    ensure!(wanted > 0, "gate (가)'s sample is empty");
    let (mut verified, mut unread) = (0_usize, 0_usize);
    for pnu in &equality.sample {
        if verified == wanted {
            break;
        }
        let key = crate::r2_layout::by_pnu::object_key(config.lane, live.base_generation, pnu)?;
        let Ok(stored) = store.read_bytes(&key).await else {
            unread += 1;
            ensure!(
                unread < V1_RESTATE_SAMPLES,
                "{unread} sample PNUs cannot be read from base generation {}; refusing to restate \
                 the v1 manifest",
                live.base_generation
            );
            continue;
        };
        manifest_publish::check_document(
            config.lane,
            &key,
            pnu,
            &live.gold_iceberg_snapshot_id,
            &stored,
        )?;
        verified += 1;
    }
    ensure!(
        verified == wanted,
        "only {verified} sample documents of base generation {} were verified, not {wanted}",
        live.base_generation
    );
    tracing::info!(
        lane = config.lane.unit(),
        base_generation = live.base_generation,
        verified,
        unread,
        "restating the v1 manifest as v2 under the first pack publish"
    );
    Ok(ServedManifest {
        wire_schema_version: 2,
        document_schema_version: Some(
            manifest_publish::document_schema_version(config.lane).to_owned(),
        ),
        pnu_prefix_length: Some(
            crate::by_pnu_gateway_contract::by_pnu_serving_patch_policy()?.pnu_prefix_length,
        ),
        ..live
    })
}

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
fn read_summaries(
    lane: ByPnuLane,
    dir: &Path,
    expected_snapshot: &str,
) -> anyhow::Result<Vec<PackExportSummary>> {
    let paths = equality::summary_paths(dir)?;
    let mut summaries = Vec::with_capacity(paths.len());
    for path in &paths {
        let summary: PackExportSummary = serde_json::from_slice(&std::fs::read(path)?)
            .with_context(|| format!("{} is not a pack export summary", path.display()))?;
        ensure!(
            summary.schema_version == summary_schema_version(lane),
            "{} is a {} summary",
            path.display(),
            summary.schema_version
        );
        ensure!(
            summary.gold_table == gold_table(lane)
                && summary.gold_iceberg_snapshot_id == expected_snapshot
                && summary.document_schema_version
                    == manifest_publish::document_schema_version(lane),
            "{} is of {} snapshot {} ({}), not of {} snapshot {expected_snapshot} as baked now",
            path.display(),
            summary.gold_table,
            summary.gold_iceberg_snapshot_id,
            summary.document_schema_version,
            gold_table(lane)
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

/// Per section: its generation and its packs, from every summary. A patch's sections each carry
/// their own generation.
pub(super) fn packs_by_section(
    lane: ByPnuLane,
    summaries: &[PackExportSummary],
) -> anyhow::Result<BTreeMap<String, (u64, Vec<PackEntry>)>> {
    let mut by_section: BTreeMap<String, (u64, Vec<PackEntry>)> = BTreeMap::new();
    let mut keys = BTreeSet::new();
    for summary in summaries {
        for pack in &summary.packs {
            let parsed = by_pnu_packs::parse_pack_key(lane, &pack.key)
                .with_context(|| format!("{} is not a {} pack key", pack.key, lane.unit()))?;
            ensure!(
                parsed.section == pack.section
                    && parsed.unit == pack.unit
                    && parsed.patch == summary.patch
                    && parsed.generation == summary.generation_of(&pack.section),
                "summary entry {} disagrees with its own key",
                pack.key
            );
            ensure!(
                keys.insert(pack.key.clone()),
                "two summaries name {}",
                pack.key
            );
            let generation = summary.generation_of(&pack.section);
            let entry = by_section
                .entry(pack.section.clone())
                .or_insert((generation, Vec::new()));
            ensure!(
                entry.0 == generation,
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
    let expected = config
        .gold_record_count
        .context("a base publish holds every section to the Gold row count the catalog records")?;
    let snapshot = config.expected_gold_snapshot.as_str();
    for summary in summaries {
        ensure!(
            summary.gold_record_count.is_none_or(|count| count == expected),
            "a bake scanned a Gold snapshot of {:?} rows by its manifests, but the catalog records \
             {expected}",
            summary.gold_record_count
        );
    }
    let by_section = packs_by_section(config.lane, summaries)?;
    let contract = &config.lane.section_packs()?.sections;
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
    let parts = write_parts_indexes(config.lane, store, summaries, &by_section).await?;
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
            parts: parts.get(name).cloned(),
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
            require_scheduled_pack_bake(config.lane, &config.installed_jobs)?;
            let cutover = cutover_gate(config, generation, snapshot, expected)?;
            let mut state = SectionPacksState {
                schema_version: policy.manifest_section_packs_schema_version,
                format_version: policy.format_version,
                unit_prefix_length: policy.unit_prefix_length,
                document_schema_version: manifest_publish::document_schema_version(config.lane)
                    .to_owned(),
                gold_table: gold_table(config.lane).to_owned(),
                reflected_gold_iceberg_snapshot_id: snapshot.to_owned(),
                reflected_gold_snapshot_tag: None,
                document_count: expected,
                sections: contract
                    .iter()
                    .map(|name| section_state(name, 0))
                    .collect::<anyhow::Result<_>>()?,
                patches: Vec::new(),
                cutover: Some(cutover),
            };
            state.schema_version = state.schema_version_for_sections()?;
            manifest_publish::require_gateway_reads(
                config.lane,
                gateway_base_url,
                state.schema_version,
            )
            .await?;
            Ok(state)
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
            // A generation that cuts dongs needs a gateway that reads the parted block; one the
            // lane already serves it from has been asked before.
            let version = next.schema_version_for_sections()?;
            if version != current.schema_version {
                manifest_publish::require_gateway_reads(config.lane, gateway_base_url, version)
                    .await?;
            }
            next.schema_version = version;
            Ok(next)
        }
    }
}

/// Writes each baked section's parts index (root ADR-0163): the dongs the bake's summaries cut
/// into more than one part, create-only under the section generation, and returns what the
/// manifest names of each. A section none of whose dongs is cut has no index.
///
/// # Errors
/// Refuses summaries that disagree on a dong's part count, and a write that fails or would
/// replace other bytes.
pub(super) async fn write_parts_indexes(
    lane: ByPnuLane,
    store: &ByPnuServingStore,
    summaries: &[PackExportSummary],
    by_section: &BTreeMap<String, (u64, Vec<PackEntry>)>,
) -> anyhow::Result<BTreeMap<String, PartsRef>> {
    let mut counts: BTreeMap<String, BTreeMap<String, u32>> = BTreeMap::new();
    for summary in summaries {
        for (section, cut) in &summary.parts {
            let section_counts = counts.entry(section.clone()).or_default();
            for (dong, count) in cut {
                if let Some(earlier) = section_counts.insert(dong.clone(), *count) {
                    ensure!(
                        earlier == *count,
                        "two summaries cut {section} dong {dong} into {earlier} and {count} parts"
                    );
                }
            }
        }
    }
    ensure!(
        counts
            .keys()
            .all(|section| by_section.contains_key(section)),
        "a summary cuts a section no summary bakes"
    );
    let mut named = BTreeMap::new();
    for (section, (generation, packs)) in by_section {
        let section = section.clone();
        let parts = by_pnu_packs::Parts::new(counts.remove(&section).unwrap_or_default())?;
        // Every part a summary wrote is one the index names, and the other way round no part
        // beyond the count exists.
        for pack in packs {
            let dong = by_pnu_packs::dong_of(&pack.unit);
            ensure!(
                parts.units_of_dong(dong)?.contains(&pack.unit),
                "pack {} is not a unit of dong {dong} as the parts index cuts it",
                pack.key
            );
        }
        if parts.counts().is_empty() {
            continue;
        }
        let file = by_pnu_packs::PartsIndexFile::new(lane, &section, *generation, &parts)?;
        let mut body = serde_json::to_vec(&file)?;
        body.push(b'\n');
        let sha256 = format!("{:x}", Sha256::digest(&body));
        let (key, _) = store
            .write_parts_index_create_only(&section, *generation, &body, &sha256)
            .await?;
        named.insert(
            section,
            PartsRef {
                key,
                sha256,
                parted_units: u64::try_from(parts.counts().len())?,
            },
        );
    }
    Ok(named)
}

/// The first pack publish only over an installed scheduled bake that patches packs (runbook 7절):
/// the job list of the installed release must declare the contract's capability on the contract's
/// job. Otherwise the object bake would go on publishing objects and the packs, which the lane
/// then serves, would stop taking daily changes.
///
/// # Errors
/// Refuses an unreadable job list, a missing job and a job that does not declare the capability.
pub(crate) fn require_scheduled_pack_bake(lane: ByPnuLane, jobs_path: &Path) -> anyhow::Result<()> {
    let wanted = &lane.section_packs()?.scheduled_bake;
    let jobs: serde_json::Value =
        serde_json::from_slice(&std::fs::read(jobs_path).with_context(|| {
            format!(
                "failed to read the installed job list {}",
                jobs_path.display()
            )
        })?)
        .with_context(|| format!("{} is not JSON", jobs_path.display()))?;
    let job = jobs
        .get("jobs")
        .and_then(serde_json::Value::as_array)
        .and_then(|jobs| {
            jobs.iter().find(|job| {
                job.get("id").and_then(serde_json::Value::as_str) == Some(wanted.job.as_str())
            })
        })
        .with_context(|| format!("{} lists no job {}", jobs_path.display(), wanted.job))?;
    let declares = job
        .get("capabilities")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|capabilities| {
            capabilities
                .iter()
                .any(|capability| capability.as_str() == Some(wanted.capability.as_str()))
        });
    ensure!(
        declares,
        "the installed scheduled bake {} ({}) does not declare {}: it still bakes objects, and \
         packs published now would stop taking daily changes. Install a release whose bake \
         patches packs before the first pack publish (runbook 7절)",
        wanted.job,
        jobs_path.display(),
        wanted.capability
    );
    Ok(())
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
            config.lane.env("PACK_EQUALITY_EVIDENCE_PATH"),
            config.lane.env("PACK_LATENCY_EVIDENCE_PATH")
        );
    };
    let (equality, equality_sha256) = gate::read::<gate::EqualityEvidence>(equality)?;
    gate::require_equality(
        config.lane,
        &equality,
        generation,
        snapshot,
        expected_documents,
    )?;
    let (latency, latency_sha256) = gate::read::<gate::LatencyEvidence>(latency)?;
    let latency_waiver = gate::require_latency_or_waiver(
        config.lane,
        &latency,
        &latency_sha256,
        generation,
        &equality,
    )?;
    if let Some(waiver) = &latency_waiver {
        tracing::warn!(lane = config.lane.unit(), waiver = %waiver, "cut-over latency gate waived");
    }
    Ok(CutoverRecord {
        equality_evidence_sha256: equality_sha256,
        latency_evidence_sha256: latency_sha256,
        latency_waiver,
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

/// The change set as the publish checks it: its counts and lists agree, and it goes from the
/// snapshot the packs reflect to the one being published.
fn change_set(
    config: &PublishConfig,
    current: &SectionPacksState,
) -> anyhow::Result<(ChangeSetSummary, BTreeSet<String>, BTreeSet<String>)> {
    let paths = config.change_set.as_ref().with_context(|| {
        format!(
            "a pack patch or reflect names its change set ({})",
            config.lane.env("CHANGE_SET_SUMMARY_PATH")
        )
    })?;
    let change: ChangeSetSummary = serde_json::from_slice(&std::fs::read(&paths.summary)?)
        .context("the change set summary does not parse")?;
    let upserts = read_pnu_list(config.lane, &paths.upserts)?;
    let deletes = read_pnu_list(config.lane, &paths.deletes)?;
    let snapshot = config.expected_gold_snapshot.as_str();
    ensure!(
        change.input.baseline_snapshot_id == current.reflected_gold_iceberg_snapshot_id
            && change.input.current_snapshot_id == snapshot,
        "the change set goes from {} to {}, but the packs reflect {} and the publish is of \
         {snapshot}",
        change.input.baseline_snapshot_id,
        change.input.current_snapshot_id,
        current.reflected_gold_iceberg_snapshot_id
    );
    ensure!(
        change.quality_metrics.upsert_count == u64::try_from(upserts.len())?
            && change.quality_metrics.delete_count == u64::try_from(deletes.len())?
            && change.quality_metrics.new_count <= change.quality_metrics.upsert_count
            && upserts.is_disjoint(&deletes),
        "the change set summary disagrees with its lists"
    );
    Ok((change, upserts, deletes))
}

/// An empty change set: nothing to bake, the packs now reflect the new snapshot.
fn reflected(config: &PublishConfig, live: &ServedManifest) -> anyhow::Result<SectionPacksState> {
    let current = live
        .section_packs
        .as_ref()
        .context("a pack reflect needs published section packs")?;
    let (_, upserts, deletes) = change_set(config, current)?;
    ensure!(
        upserts.is_empty() && deletes.is_empty(),
        "the change set holds {} upserts and {} deletes; without {} a publish only reflects an \
         empty change set",
        upserts.len(),
        deletes.len(),
        config.lane.env("PACK_SUMMARY_DIR")
    );
    let mut next = current.clone();
    next.reflected_gold_iceberg_snapshot_id = config.expected_gold_snapshot.clone();
    next.reflected_gold_snapshot_tag = None;
    answers_for_every_gold_row(config, &next)?;
    Ok(next)
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
    let (change, upserts, deletes) = change_set(config, current)?;
    let snapshot = config.expected_gold_snapshot.as_str();
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
    let by_section = packs_by_section(config.lane, summaries)?;
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
    answers_for_every_gold_row(config, &next)?;
    Ok(next)
}

/// The packs answer for exactly the rows the catalog records for the snapshot they reflect: a
/// patch or a reflect that drifted from Gold is refused now, not at the next re-bake.
fn answers_for_every_gold_row(
    config: &PublishConfig,
    next: &SectionPacksState,
) -> anyhow::Result<()> {
    let expected = config
        .gold_record_count
        .context("a pack publish holds the packs to the Gold row count the catalog records")?;
    ensure!(
        next.document_count == expected,
        "the packs would answer for {} PNUs, but Gold snapshot {} holds {expected} rows",
        next.document_count,
        config.expected_gold_snapshot
    );
    Ok(())
}
