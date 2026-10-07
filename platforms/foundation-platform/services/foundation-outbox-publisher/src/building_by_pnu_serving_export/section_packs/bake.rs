//! `export-<lane>-by-pnu-section-packs`: Gold into section packs (root ADR-0147 §1, §4, §5).
//!
//! One run reads one Gold snapshot (sharded by PNU prefix, as the object export is) and writes,
//! per legal dong of the shard and per section asked for, one pack, create-only:
//!
//! - **base** — every row of the shard into generation `PACK_GENERATION` of every section, or of
//!   the sections `PACK_SECTIONS` names (a section re-baked alone, of the reflected snapshot only);
//! - **patch** — with `TARGET_PATCH`, the change set's rows (`PNU_ALLOWLIST_PATH`) as documents and
//!   its deletes (`DELETE_LIST_PATH`) as tombstones, into `g{n}/p{patch}/` of each section, where
//!   `n` is the generation the lane serves that section from (the manifest's `section_packs`), only
//!   for the dongs the change set touches.
//!
//! The documents go through the lane's object export builder (`sections::Renderer`), so a pack
//! holds exactly what an object would have held. Before a dong's packs are written, the gateway's
//! answer for every PNU of the dong is joined from them and compared with that builder's object
//! document (gate 가). A section re-baked alone is joined with the other sections exactly as the
//! lane serves them (their base and patches, read from the bucket), so a re-bake whose ids no
//! longer line up with the served sections is refused before anything of the dong is written.
//! The summary names every pack with its counts; `publish-<lane>-by-pnu-section-packs` checks
//! the listing against it.
//!
//! A shard prefix is at most a legal dong long, so no dong is split across runs. A generation that
//! already holds packs of another Gold snapshot is refused before the first write: create-only
//! makes a re-run of the same snapshot idempotent, and nothing else may mix into a generation.
//! Dongs are written as they pass, so a run that fails on a later dong leaves the passing dongs'
//! packs behind: unpublished, never served, and reused byte for byte by a re-run.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{ensure, Context};
use futures_util::{stream, StreamExt as _, TryStreamExt as _};
use lakehouse_infrastructure::{
    IcebergRestCatalog, IcebergSnapshotManifestList, LakehouseCatalogConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};

use super::super::{
    optional_env, parse_max_concurrency, read_pnu_allowlist, refuse_a_moved_table, select_rows,
    write_summary, MAX_ROWS_PER_RUN,
};
use super::sections::{self, PackDocument, PackProvenance, Renderer};
use super::{gate, read};
use crate::by_pnu_gateway_contract::ByPnuLane;
use crate::by_pnu_pack::{self, Pack, PackIdentity, PackWriter};
use crate::by_pnu_section_pack_manifest::SectionPacksState;
use crate::by_pnu_serving_manifest::ServedManifest;
use crate::by_pnu_serving_patch_export::{self as patch_export, PatchTarget};
use crate::by_pnu_serving_store::{local_root, ByPnuServingStore};
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::lakehouse_snapshot_scan::{scan_snapshot_rows_kept, LakehouseObjectReader};
use crate::r2_layout::by_pnu_packs;

/// The schema of a lane's export summary; the publish reads only its own lane's.
pub(crate) const fn summary_schema_version(lane: ByPnuLane) -> &'static str {
    match lane {
        ByPnuLane::Building => "foundation-platform.building_by_pnu_section_pack_export_summary.v1",
        ByPnuLane::Parcel => "foundation-platform.parcel_by_pnu_section_pack_export_summary.v1",
    }
}
const DEFAULT_MAX_CONCURRENCY: usize = 16;
/// How many differing PNUs a refusal names.
const MAX_DIFFERING: usize = 20;

#[derive(Clone, Debug)]
pub(crate) struct BakeConfig {
    pub(crate) lane: ByPnuLane,
    pub(crate) output: ProfileStoreConfig,
    /// The generation a base bake writes. A patch names none: each section's patch goes under the
    /// generation the lane serves that section from.
    pub(crate) generation: Option<u64>,
    pub(crate) sections: Vec<String>,
    pub(crate) patch: Option<PatchTarget>,
    pub(crate) upserts: Option<BTreeSet<String>>,
    pub(crate) pnu_prefix: Option<String>,
    pub(crate) expected_gold_snapshot: Option<String>,
    pub(crate) max_concurrency: usize,
    pub(crate) summary_path: PathBuf,
}

impl BakeConfig {
    fn from_env(lane: ByPnuLane) -> anyhow::Result<Self> {
        let env = |name: &str| optional_env(&lane.env(name));
        ensure!(
            env("CONFIRM_PACK_EXPORT")?.is_some_and(|value| value.eq_ignore_ascii_case("true")),
            "{} must be true",
            lane.env("CONFIRM_PACK_EXPORT")
        );
        let generation = env("PACK_GENERATION")?
            .map(|raw| {
                raw.parse::<u64>()
                    .ok()
                    .filter(|generation| *generation >= 1)
                    .with_context(|| {
                        format!("{} must be a positive integer", lane.env("PACK_GENERATION"))
                    })
            })
            .transpose()?;
        let contract = &lane.section_packs()?.sections;
        let sections = match env("PACK_SECTIONS")? {
            None => contract.clone(),
            Some(raw) => raw
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(ToOwned::to_owned)
                .collect(),
        };
        ensure!(
            !sections.is_empty() && sections.iter().all(|name| contract.contains(name)),
            "{} names sections outside the contract's {contract:?}",
            lane.env("PACK_SECTIONS")
        );
        let patch = patch_export::from_env(lane, optional_env)?;
        ensure!(
            patch.is_some() != generation.is_some(),
            "a base names its generation as {}; a patch names none, each section's patch goes \
             under the generation the lane serves it from",
            lane.env("PACK_GENERATION")
        );
        let upserts = env("PNU_ALLOWLIST_PATH")?
            .map(|raw| {
                let path = std::path::Path::new(raw.as_str());
                if patch.is_some() {
                    patch_export::read_pnu_list(lane, path)
                } else {
                    read_pnu_allowlist(path)
                }
            })
            .transpose()?;
        ensure!(
            patch.is_none() || (upserts.is_some() && sections == *contract),
            "a patch names its upserts as {} and writes every section",
            lane.env("PNU_ALLOWLIST_PATH")
        );
        let unit_length = crate::by_pnu_gateway_contract::section_pack_policy()?.unit_prefix_length;
        let pnu_prefix = env("PNU_PREFIX")?;
        if let Some(prefix) = &pnu_prefix {
            ensure!(
                (1..=unit_length).contains(&prefix.len())
                    && prefix.bytes().all(|byte| byte.is_ascii_digit()),
                "{} must be 1 to {unit_length} digits: a pack is one whole legal dong",
                lane.env("PNU_PREFIX")
            );
        }
        Ok(Self {
            lane,
            output: ProfileStoreConfig::parse(
                env("OUTPUT_STORAGE_DRIVER")?
                    .unwrap_or_else(|| "local".to_owned())
                    .as_str(),
                local_root(env("OUTPUT_ROOT")?),
            )?,
            generation,
            sections,
            patch,
            upserts,
            pnu_prefix,
            expected_gold_snapshot: env("EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID")?,
            max_concurrency: env("MAX_CONCURRENCY")?
                .map(|raw| parse_max_concurrency(&raw))
                .transpose()?
                .unwrap_or(DEFAULT_MAX_CONCURRENCY),
            summary_path: PathBuf::from(
                env("PACK_SUMMARY_PATH")?
                    .with_context(|| format!("{} is required", lane.env("PACK_SUMMARY_PATH")))?,
            ),
        })
    }
}

/// What one export run wrote; the publish reads a directory of these.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct PackExportSummary {
    pub(crate) schema_version: String,
    pub(crate) document_schema_version: String,
    pub(crate) gold_table: String,
    pub(crate) gold_iceberg_snapshot_id: String,
    /// The generation of every section, unless `section_generations` names another for it.
    pub(crate) generation: u64,
    /// A patch's sections, each under the generation the lane served it from when it was baked.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) section_generations: BTreeMap<String, u64>,
    pub(crate) patch: Option<u64>,
    pub(crate) sections: Vec<String>,
    pub(crate) pnu_prefix: Option<String>,
    /// Every live row of the Gold snapshot, by its manifests' record counts; not only the shard's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) gold_record_count: Option<u64>,
    pub(crate) exported_row_count: u64,
    pub(crate) tombstone_count: u64,
    pub(crate) totals: BTreeMap<String, SectionTotals>,
    pub(crate) packs: Vec<PackEntry>,
    /// Gate (가) of this run: documents read back from their packs and compared, before writing.
    pub(crate) equality: Equality,
    pub(crate) elapsed_seconds: f64,
}

impl PackExportSummary {
    /// The generation `section` was written under.
    pub(crate) fn generation_of(&self, section: &str) -> u64 {
        self.section_generations
            .get(section)
            .copied()
            .unwrap_or(self.generation)
    }
}

/// What a run compared before writing, and the sample candidates it drew (base runs only).
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Equality {
    /// PNUs whose answer was joined and compared with what it must be: every kept row against its
    /// object document, and, when a section was re-baked alone, every other PNU the lane serves
    /// in the run's dongs against "no document".
    pub(crate) compared: u64,
    /// Of those, the ones that matched; counted on its own, so `equal == compared` is a finding.
    pub(crate) equal: u64,
    /// Whether the run's sections were joined with the sections the lane serves (a section
    /// re-baked alone) rather than only with each other.
    #[serde(default)]
    pub(crate) joined_with_served: bool,
    pub(crate) sample_candidates: Vec<String>,
}

/// Per section: what the run's packs hold together.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct SectionTotals {
    pub(crate) packs: u64,
    pub(crate) documents: u64,
    pub(crate) tombstones: u64,
    pub(crate) bytes: u64,
    pub(crate) head_bytes: u64,
    pub(crate) largest_pack_bytes: u64,
    pub(crate) largest_head_bytes: u64,
}

/// One written pack.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PackEntry {
    pub(crate) section: String,
    pub(crate) unit: String,
    pub(crate) key: String,
    pub(crate) sha256: String,
    pub(crate) bytes: u64,
    pub(crate) head_bytes: u64,
    pub(crate) documents: u64,
    pub(crate) tombstones: u64,
    /// A patch pack's PNUs (documents and tombstones); empty for a base pack, whose PNUs are
    /// counted, not listed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) pnus: Vec<String>,
    pub(crate) outcome: String,
}

/// Runs the export.
///
/// # Errors
/// Refuses on any failed check; packs already written stay (create-only, same bytes on re-run).
pub(crate) async fn run(lane: ByPnuLane) -> anyhow::Result<()> {
    sections::check_contract_sections(lane)?;
    let config = BakeConfig::from_env(lane)?;
    let table = sections::gold_table(lane)?.table_name;
    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::from_env().context("failed to configure the Iceberg catalog")?,
    )?;
    let snapshot = catalog
        .load_current_snapshot_manifest_list(table)
        .await?
        .with_context(|| format!("{table} has no current snapshot to export"))?;
    let lakehouse = LakehouseObjectReader::from_env()?;
    let store = ByPnuServingStore::open(lane, &config.output)?;
    let renderer = Renderer::load(lane).await?;
    let (rows, gold_record_count) = scan(&config, &lakehouse, &snapshot).await?;
    let mut summary = bake(
        &config,
        &store,
        &snapshot_provenance(&snapshot),
        &rows,
        &renderer,
    )
    .await?;
    summary.gold_record_count = Some(gold_record_count);
    write_summary(&config.summary_path, &summary)?;
    tracing::info!(
        generation = summary.generation,
        patch = summary.patch,
        exported_row_count = summary.exported_row_count,
        tombstone_count = summary.tombstone_count,
        packs = summary.packs.len(),
        elapsed_seconds = summary.elapsed_seconds,
        lane = lane.unit(),
        "section pack export succeeded"
    );
    Ok(())
}

fn snapshot_provenance(snapshot: &IcebergSnapshotManifestList) -> PackProvenance {
    PackProvenance {
        table: snapshot.table_name.clone(),
        iceberg_snapshot_id: snapshot.snapshot_id.to_string(),
        metadata_location: snapshot.metadata_location.clone(),
        manifest_list_location: snapshot.manifest_list_location.clone(),
    }
}

/// The kept rows, and the snapshot's whole row count by its manifests.
async fn scan(
    config: &BakeConfig,
    lakehouse: &LakehouseObjectReader,
    snapshot: &IcebergSnapshotManifestList,
) -> anyhow::Result<(Vec<JsonMap<String, JsonValue>>, u64)> {
    refuse_a_moved_table(
        config.expected_gold_snapshot.as_deref(),
        &snapshot.table_name,
        &snapshot.snapshot_id.to_string(),
    )?;
    let rows = scan_snapshot_rows_kept(
        sections::gold_table(config.lane)?,
        lakehouse,
        snapshot,
        |row| match row.get("pnu").and_then(JsonValue::as_str) {
            Some(pnu) => patch_export::keeps(
                pnu,
                config.pnu_prefix.as_deref(),
                config.upserts.as_ref(),
                config.patch.as_ref(),
            ),
            None => true,
        },
        Some(MAX_ROWS_PER_RUN),
    )
    .await?;
    // The scheduled bake splits a shard on these words, as it does for the object export.
    ensure!(
        !rows.keep_limit_exceeded,
        "this shard keeps more than {MAX_ROWS_PER_RUN} rows; shard the run with {}, not a bigger \
         heap",
        config.lane.env("PNU_PREFIX")
    );
    ensure!(
        rows.decoded_row_count == rows.manifest_record_count,
        "scanned {} rows but the manifests declared {}",
        rows.decoded_row_count,
        rows.manifest_record_count
    );
    patch_export::refuse_live_deletes(
        config.patch.as_ref(),
        rows.rows
            .iter()
            .filter_map(|row| row.get("pnu").and_then(JsonValue::as_str)),
    )?;
    Ok((rows.rows, rows.manifest_record_count))
}

/// Where each baked section goes, and what a section re-baked alone is joined with.
struct Plan {
    generations: BTreeMap<String, u64>,
    served: Option<ServedSections>,
}

/// The sections a run does not bake, as the lane serves them: the view of them and, per section,
/// the dongs its base generation holds a pack for.
struct ServedSections {
    view: read::PackView,
    bases: BTreeMap<String, BTreeSet<String>>,
}

impl Plan {
    fn generation(&self, section: &str) -> anyhow::Result<u64> {
        self.generations
            .get(section)
            .copied()
            .with_context(|| format!("the run does not bake section {section}"))
    }
}

/// The `section_packs` block of the live manifest.
async fn served_state(
    lane: ByPnuLane,
    store: &ByPnuServingStore,
) -> anyhow::Result<Option<SectionPacksState>> {
    let (bytes, _) = store
        .read_manifest()
        .await
        .with_context(|| format!("the {} lane has no readable manifest", lane.unit()))?;
    Ok(ServedManifest::parse(lane, &bytes)
        .context("the live manifest cannot be read")?
        .section_packs)
}

/// Each section's generation: a whole base bake writes `PACK_GENERATION`; a patch writes each
/// section under the generation the lane serves it from; a section re-baked alone writes a new
/// generation of the reflected snapshot and is joined with the served sections beside it.
async fn plan(
    config: &BakeConfig,
    store: &ByPnuServingStore,
    provenance: &PackProvenance,
) -> anyhow::Result<Plan> {
    let contract = &config.lane.section_packs()?.sections;
    if let (None, Some(generation)) = (&config.patch, config.generation) {
        if config.sections == *contract {
            return Ok(Plan {
                generations: config
                    .sections
                    .iter()
                    .map(|name| (name.clone(), generation))
                    .collect(),
                served: None,
            });
        }
    }
    let state = served_state(config.lane, store).await?.with_context(|| {
        if config.patch.is_some() {
            "a pack patch goes over the packs the lane serves, and the manifest names none"
        } else {
            "a section re-baked alone is joined with the sections the lane serves, and the \
             manifest names none; a first bake bakes every section"
        }
    })?;
    let served_generation = |name: &str| -> anyhow::Result<u64> {
        state
            .sections
            .iter()
            .find(|section| section.name == name)
            .map(|section| section.generation)
            .with_context(|| format!("the served packs have no section {name}"))
    };
    if let Some(patch) = &config.patch {
        ensure!(
            patch.patch > state.newest_patch(),
            "patch {} is not above the newest patch {} the packs serve",
            patch.patch,
            state.newest_patch()
        );
        return Ok(Plan {
            generations: config
                .sections
                .iter()
                .map(|name| Ok((name.clone(), served_generation(name)?)))
                .collect::<anyhow::Result<_>>()?,
            served: None,
        });
    }
    let generation = config
        .generation
        .context("a base bake names its generation")?;
    ensure!(
        provenance.iceberg_snapshot_id == state.reflected_gold_iceberg_snapshot_id,
        "a section re-baked alone must be of the reflected snapshot {}, not {}; a new snapshot \
         re-bakes every section",
        state.reflected_gold_iceberg_snapshot_id,
        provenance.iceberg_snapshot_id
    );
    for name in &config.sections {
        let served = served_generation(name)?;
        ensure!(
            generation > served,
            "section {name} generation may only move forward from {served}, not to {generation}"
        );
    }
    let mut view = read::PackView::served(config.lane, &state);
    view.sections
        .retain(|section| !config.sections.contains(&section.name));
    let mut bases = BTreeMap::new();
    for section in &view.sections {
        let units = store
            .list_pack_keys(&section.name, section.generation, None)
            .await?
            .iter()
            .filter_map(|key| {
                by_pnu_packs::parse_pack_key(config.lane, key).map(|parsed| parsed.unit)
            })
            .collect::<BTreeSet<_>>();
        bases.insert(section.name.clone(), units);
    }
    Ok(Plan {
        generations: config
            .sections
            .iter()
            .map(|name| (name.clone(), generation))
            .collect(),
        served: Some(ServedSections { view, bases }),
    })
}

/// One bake's shared inputs.
struct Run<'a> {
    config: &'a BakeConfig,
    store: &'a ByPnuServingStore,
    provenance: &'a PackProvenance,
    renderer: &'a Renderer,
    plan: &'a Plan,
}

/// Builds and writes the packs of the kept rows.
///
/// # Errors
/// Refuses a row the object export would refuse, a generation holding another snapshot's packs,
/// a dong whose joined answers differ from the object documents, and counts that do not add up.
pub(crate) async fn bake(
    config: &BakeConfig,
    store: &ByPnuServingStore,
    provenance: &PackProvenance,
    rows: &[JsonMap<String, JsonValue>],
    renderer: &Renderer,
) -> anyhow::Result<PackExportSummary> {
    let started = Instant::now();
    let selected = select_rows(rows, config.upserts.as_ref(), config.pnu_prefix.as_deref())?;
    // Rows are grouped by dong and turned into documents one dong at a time, inside the write
    // job: a shard's documents are never all in memory beside its rows.
    let mut units: BTreeMap<String, Vec<&JsonMap<String, JsonValue>>> = BTreeMap::new();
    for row in &selected {
        let pnu = row
            .get("pnu")
            .and_then(JsonValue::as_str)
            .context("a Gold row is missing pnu")?;
        units
            .entry(by_pnu_packs::unit_of(pnu)?.to_owned())
            .or_default()
            .push(row);
    }
    let in_shard = |pnu_or_unit: &str| {
        config
            .pnu_prefix
            .as_deref()
            .is_none_or(|prefix| pnu_or_unit.starts_with(prefix))
    };
    let mut tombstones: BTreeMap<String, Vec<String>> = BTreeMap::new();
    if let Some(patch) = &config.patch {
        for pnu in patch.deleted.iter().filter(|pnu| in_shard(pnu)) {
            tombstones
                .entry(by_pnu_packs::unit_of(pnu)?.to_owned())
                .or_default()
                .push(pnu.clone());
        }
    }
    let plan = plan(config, store, provenance).await?;
    refuse_a_foreign_generation(config, store, provenance, &plan).await?;

    let mut all_units = units
        .keys()
        .chain(tombstones.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    // A section re-baked alone is checked in every dong of the shard the lane serves, also those
    // where Gold has no row left.
    if let Some(served) = &plan.served {
        for section in &served.view.sections {
            all_units.extend(
                served
                    .bases
                    .get(&section.name)
                    .into_iter()
                    .flatten()
                    .chain(section.patches.iter().flat_map(|(_, units)| units))
                    .filter(|unit| in_shard(unit))
                    .cloned(),
            );
        }
    }
    let (no_rows, no_deletes) = (Vec::new(), Vec::new());
    let run = Run {
        config,
        store,
        provenance,
        renderer,
        plan: &plan,
    };
    // Futures are built in a loop, not in a `map` closure: a closure over borrowed rows makes
    // the command's future fail the higher-ranked `Send` check of the command table.
    let mut jobs = Vec::with_capacity(all_units.len());
    for unit in &all_units {
        let rows = units.get(unit).unwrap_or(&no_rows);
        let deleted = tombstones.get(unit).unwrap_or(&no_deletes);
        jobs.push(write_unit(&run, unit, rows, deleted));
    }
    let written = stream::iter(jobs)
        .buffer_unordered(config.max_concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    let mut packs = Vec::new();
    let mut equality = Equality {
        joined_with_served: plan.served.is_some(),
        ..Equality::default()
    };
    for (unit_packs, check) in written {
        packs.extend(unit_packs);
        equality.compared += check.compared;
        equality.equal += check.equal;
        equality.sample_candidates.extend(check.candidates);
    }
    packs.sort_by(|a, b| (&a.section, &a.unit).cmp(&(&b.section, &b.unit)));
    equality.sample_candidates.sort_unstable();

    let exported = u64::try_from(selected.len())?;
    let deleted = u64::try_from(tombstones.values().map(Vec::len).sum::<usize>())?;
    let mut totals: BTreeMap<String, SectionTotals> = BTreeMap::new();
    for pack in &packs {
        let total = totals.entry(pack.section.clone()).or_default();
        total.packs += 1;
        total.documents += pack.documents;
        total.tombstones += pack.tombstones;
        total.bytes += pack.bytes;
        total.head_bytes += pack.head_bytes;
        total.largest_pack_bytes = total.largest_pack_bytes.max(pack.bytes);
        total.largest_head_bytes = total.largest_head_bytes.max(pack.head_bytes);
    }
    // The completeness gate of the run: every section holds every kept row and every delete,
    // and the indexes add up to them.
    for section in &config.sections {
        let total = totals.get(section).cloned().unwrap_or_default();
        ensure!(
            total.documents == exported && total.tombstones == deleted,
            "section {section} holds {} documents and {} tombstones but the run kept {exported} \
             rows and {deleted} deletes",
            total.documents,
            total.tombstones
        );
    }
    let anchor = &config.lane.section_packs()?.anchor_section;
    let generation = plan
        .generations
        .get(anchor)
        .or_else(|| plan.generations.values().next())
        .copied()
        .context("the run bakes no section")?;
    Ok(PackExportSummary {
        schema_version: summary_schema_version(config.lane).to_owned(),
        document_schema_version: crate::by_pnu_serving_manifest_publish::document_schema_version(
            config.lane,
        )
        .to_owned(),
        gold_table: provenance.table.clone(),
        gold_iceberg_snapshot_id: provenance.iceberg_snapshot_id.clone(),
        generation,
        section_generations: if config.patch.is_some() {
            plan.generations.clone()
        } else {
            BTreeMap::new()
        },
        patch: config.patch.as_ref().map(|patch| patch.patch),
        sections: config.sections.clone(),
        pnu_prefix: config.pnu_prefix.clone(),
        gold_record_count: None,
        exported_row_count: exported,
        tombstone_count: deleted,
        totals,
        packs,
        equality,
        elapsed_seconds: started.elapsed().as_secs_f64(),
    })
}

/// Refuses to add packs to a directory that holds packs of another Gold snapshot.
async fn refuse_a_foreign_generation(
    config: &BakeConfig,
    store: &ByPnuServingStore,
    provenance: &PackProvenance,
    plan: &Plan,
) -> anyhow::Result<()> {
    let patch = config.patch.as_ref().map(|patch| patch.patch);
    for section in &config.sections {
        let keys = store
            .list_pack_keys(section, plan.generation(section)?, patch)
            .await?;
        if let Some(key) = keys.iter().next() {
            let bytes = store.read_bytes(key).await?;
            let (header, _) = by_pnu_pack::read_head(&bytes)?;
            ensure!(
                header.gold_iceberg_snapshot_id == provenance.iceberg_snapshot_id,
                "{key} holds packs of Gold snapshot {} but this run is of {}; a new snapshot \
                 goes into a new generation or patch",
                header.gold_iceberg_snapshot_id,
                provenance.iceberg_snapshot_id
            );
        }
    }
    Ok(())
}

/// What one dong's packs were checked against before they were written.
#[derive(Debug, Default)]
pub(super) struct UnitCheck {
    pub(super) compared: u64,
    pub(super) equal: u64,
    /// The first PNUs that did not answer as they must.
    pub(super) differing: Vec<String>,
    pub(super) candidates: Vec<String>,
}

impl UnitCheck {
    fn tally(&mut self, pnu: &str, equal: bool) {
        self.compared += 1;
        if equal {
            self.equal += 1;
        } else if self.differing.len() < MAX_DIFFERING {
            self.differing.push(pnu.to_owned());
        }
    }

    /// Refuses a dong any of whose PNUs did not answer as it must.
    ///
    /// # Errors
    /// Names the dong and the first differing PNUs.
    pub(super) fn require_equal(&self, unit: &str) -> anyhow::Result<()> {
        ensure!(
            self.equal == self.compared,
            "legal dong {unit}: {} of {} PNUs answer from the packs as they must; differing: {:?}. \
             Nothing of this dong is written",
            self.equal,
            self.compared,
            self.differing
        );
        Ok(())
    }
}

/// One dong: its documents, every baked section's pack laid out and read back in memory, the
/// gateway's answer joined from them (and from the served sections beside a section re-baked
/// alone) and compared with the object documents (gate 가), then the packs written.
async fn write_unit(
    run: &Run<'_>,
    unit: &str,
    rows: &[&JsonMap<String, JsonValue>],
    deleted: &[String],
) -> anyhow::Result<(Vec<PackEntry>, UnitCheck)> {
    let documents = rows
        .iter()
        .map(|row| run.renderer.render(run.provenance, row))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let patch = run.config.patch.as_ref().map(|patch| patch.patch);
    let mut laid_out = Vec::with_capacity(run.config.sections.len());
    if !documents.is_empty() || !deleted.is_empty() {
        for section in &run.config.sections {
            laid_out.push((
                section.clone(),
                lay_out_pack(
                    run.config,
                    run.plan.generation(section)?,
                    run.provenance,
                    section,
                    unit,
                    &documents,
                    deleted,
                )?,
            ));
        }
    }
    let served = match &run.plan.served {
        Some(served) => {
            read::load_unit(run.store, &served.view, unit, &|section, unit| {
                served
                    .bases
                    .get(section)
                    .is_some_and(|units| units.contains(unit))
            })
            .await?
            .sections
        }
        None => Vec::new(),
    };
    let check = check_round_trip(
        run.config.lane,
        &laid_out,
        &served,
        &documents,
        deleted,
        patch,
    )?;
    check.require_equal(unit)?;
    let mut packs = Vec::with_capacity(laid_out.len());
    for (section, bytes) in laid_out {
        packs.push(write_pack(run, section, unit, bytes, &documents, deleted).await?);
    }
    Ok((packs, check))
}

pub(super) fn lay_out_pack(
    config: &BakeConfig,
    generation: u64,
    provenance: &PackProvenance,
    section: &str,
    unit: &str,
    documents: &[PackDocument],
    deleted: &[String],
) -> anyhow::Result<Vec<u8>> {
    let mut writer = PackWriter::new(PackIdentity {
        lane: config.lane.unit().to_owned(),
        section: section.to_owned(),
        generation,
        patch: config.patch.as_ref().map(|patch| patch.patch),
        unit: unit.to_owned(),
        gold_table: provenance.table.clone(),
        gold_iceberg_snapshot_id: provenance.iceberg_snapshot_id.clone(),
    })?;
    for (pnu, document) in entries(documents, deleted) {
        match document {
            Some(document) => writer.push_document(pnu, &sections::fragment(document, section)?)?,
            None => writer.push_tombstone(pnu)?,
        }
    }
    writer.finish()
}

fn entries<'a>(
    documents: &'a [PackDocument],
    deleted: &'a [String],
) -> Vec<(&'a str, Option<&'a PackDocument>)> {
    let mut entries: Vec<(&str, Option<&PackDocument>)> = documents
        .iter()
        .map(|document| (document.pnu.as_str(), Some(document)))
        .chain(deleted.iter().map(|pnu| (pnu.as_str(), None)))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    entries
}

/// Gate (가) on one dong, before anything is written.
///
/// - Every baked section's pack, read back from its bytes, holds exactly the fragment the
///   document cuts; anything else is a format defect and stops the run.
/// - The gateway's answer for every document, joined from the baked packs and the `served`
///   sections beside them (a section re-baked alone), is compared byte for byte with the object
///   document the same row renders; every other PNU the served sections answer must not answer
///   as a document. Both are tallied in [`UnitCheck`], so `equal` is counted, not assumed.
/// - Every delete answers as a tombstone.
pub(super) fn check_round_trip(
    lane: ByPnuLane,
    laid_out: &[(String, Vec<u8>)],
    served: &[read::SectionPacksOfUnit],
    documents: &[PackDocument],
    deleted: &[String],
    patch: Option<u64>,
) -> anyhow::Result<UnitCheck> {
    let mut baked = Vec::with_capacity(laid_out.len());
    for (section, bytes) in laid_out {
        let pack = Pack::read(bytes)?;
        for document in documents {
            let entry = pack
                .find(&document.pnu)
                .with_context(|| format!("the {section} pack lost {}", document.pnu))?;
            ensure!(
                pack.document(entry)?.as_deref()
                    == Some(sections::fragment(document, section)?.as_slice()),
                "the {section} pack does not read back the fragment of {}",
                document.pnu
            );
        }
        baked.push(read::SectionPacksOfUnit {
            name: section.clone(),
            patches: patch
                .map(|number| (number, pack.clone()))
                .into_iter()
                .collect(),
            base: patch.is_none().then_some(pack),
        });
    }
    // Contract order; a section neither baked here nor served beside them has no pack in this
    // dong, and a document then cannot answer.
    let mut joined = Vec::new();
    for name in &lane.section_packs()?.sections {
        joined.push(
            baked
                .iter()
                .chain(served)
                .find(|section| &section.name == name)
                .cloned()
                .unwrap_or_else(|| read::SectionPacksOfUnit {
                    name: name.clone(),
                    patches: Vec::new(),
                    base: None,
                }),
        );
    }
    let packs = read::UnitPacks {
        lane,
        sections: joined,
    };
    let mut check = UnitCheck::default();
    for document in documents {
        let answer = match read::resolve(&packs, &document.pnu) {
            Ok(read::Resolved::Document(fragments)) => read::joined_bytes(lane, &fragments).ok(),
            _ => None,
        };
        check.tally(
            &document.pnu,
            answer.as_deref() == Some(document.bytes.as_slice()),
        );
        if patch.is_none() && gate::is_sample_candidate(&document.pnu)? {
            check.candidates.push(document.pnu.clone());
        }
    }
    let rendered = documents
        .iter()
        .map(|document| document.pnu.as_str())
        .chain(deleted.iter().map(String::as_str))
        .collect::<BTreeSet<_>>();
    let mut beside = BTreeSet::new();
    for section in served {
        for pack in section
            .patches
            .iter()
            .map(|(_, pack)| pack)
            .chain(section.base.iter())
        {
            beside.extend(
                pack.entries
                    .iter()
                    .map(|entry| entry.pnu.as_str())
                    .filter(|pnu| !rendered.contains(pnu)),
            );
        }
    }
    for pnu in beside {
        let answers = !matches!(
            read::resolve(&packs, pnu),
            Ok(read::Resolved::Tombstone | read::Resolved::Absent)
        );
        if answers {
            check.tally(pnu, false);
        }
    }
    for pnu in deleted {
        ensure!(
            matches!(read::resolve(&packs, pnu)?, read::Resolved::Tombstone),
            "the packs do not answer {pnu} as deleted"
        );
    }
    Ok(check)
}

async fn write_pack(
    run: &Run<'_>,
    section: String,
    unit: &str,
    bytes: Vec<u8>,
    documents: &[PackDocument],
    deleted: &[String],
) -> anyhow::Result<PackEntry> {
    let patch = run.config.patch.as_ref().map(|patch| patch.patch);
    let generation = run.plan.generation(&section)?;
    let pnus = if patch.is_some() {
        entries(documents, deleted)
            .iter()
            .map(|(pnu, _)| (*pnu).to_owned())
            .collect()
    } else {
        Vec::new()
    };
    let head = by_pnu_pack::read_prefix(&bytes)?.head_length();
    let key = by_pnu_packs::pack_key(run.config.lane, &section, generation, patch, unit)?;
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    let created = run
        .store
        .write_pack_create_only(&key, &bytes, &sha256)
        .await?;
    Ok(PackEntry {
        section,
        unit: unit.to_owned(),
        key,
        sha256,
        bytes: u64::try_from(bytes.len())?,
        head_bytes: u64::try_from(head)?,
        documents: u64::try_from(documents.len())?,
        tombstones: u64::try_from(deleted.len())?,
        pnus,
        outcome: if created { "created" } else { "reused" }.to_owned(),
    })
}
