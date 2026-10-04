//! `export-building-by-pnu-section-packs`: Gold into section packs (root ADR-0147 §1, §4, §5).
//!
//! One run reads one Gold snapshot (sharded by PNU prefix, as the object export is) and writes,
//! per legal dong of the shard and per section asked for, one pack, create-only:
//!
//! - **base** — every row of the shard into generation `PACK_GENERATION`;
//! - **patch** — with `TARGET_PATCH`, the change set's rows (`PNU_ALLOWLIST_PATH`) as documents and
//!   its deletes (`DELETE_LIST_PATH`) as tombstones, into `g{generation}/p{patch}/` of each
//!   section, only for the dongs the change set touches.
//!
//! The documents go through the object export's own builder (`building_document`), so a pack
//! holds exactly what an object would have held. The summary names every pack with its counts;
//! `publish-building-by-pnu-section-packs` checks the listing against it.
//!
//! A shard prefix is at most a legal dong long, so no dong is split across runs. A generation that
//! already holds packs of another Gold snapshot is refused before the first write: create-only
//! makes a re-run of the same snapshot idempotent, and nothing else may mix into a generation.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{bail, ensure, Context};
use futures_util::{stream, StreamExt as _, TryStreamExt as _};
use lakehouse_domain::GOLD_BUILDING_PANEL;
use lakehouse_infrastructure::{
    IcebergRestCatalog, IcebergSnapshotManifestList, LakehouseCatalogConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};

use super::super::building_document::{self, BuildingByPnuDocument, GoldSnapshotProvenance};
use super::super::{
    optional_env, parse_max_concurrency, read_pnu_allowlist, refuse_a_moved_table, select_rows,
    write_summary, LANE, MAX_ROWS_PER_RUN,
};
use super::{gate, read, sections};
use crate::building_link_evidence::ApprovedBuildingLinks;
use crate::by_pnu_pack::{self, Pack, PackIdentity, PackWriter};
use crate::by_pnu_serving_patch_export::{self as patch_export, PatchTarget};
use crate::by_pnu_serving_store::{local_root, ByPnuServingStore};
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::lakehouse_snapshot_scan::{scan_snapshot_rows_kept, LakehouseObjectReader};
use crate::r2_layout::by_pnu_packs;

pub(crate) const SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.building_by_pnu_section_pack_export_summary.v1";
const DEFAULT_MAX_CONCURRENCY: usize = 16;

#[derive(Clone, Debug)]
pub(crate) struct BakeConfig {
    pub(crate) output: ProfileStoreConfig,
    pub(crate) generation: u64,
    pub(crate) sections: Vec<String>,
    pub(crate) patch: Option<PatchTarget>,
    pub(crate) upserts: Option<BTreeSet<String>>,
    pub(crate) pnu_prefix: Option<String>,
    pub(crate) expected_gold_snapshot: Option<String>,
    pub(crate) max_concurrency: usize,
    pub(crate) summary_path: PathBuf,
}

impl BakeConfig {
    fn from_env() -> anyhow::Result<Self> {
        let env = |name: &str| optional_env(&LANE.env(name));
        ensure!(
            env("CONFIRM_PACK_EXPORT")?.is_some_and(|value| value.eq_ignore_ascii_case("true")),
            "{} must be true",
            LANE.env("CONFIRM_PACK_EXPORT")
        );
        let generation = env("PACK_GENERATION")?
            .with_context(|| format!("{} is required", LANE.env("PACK_GENERATION")))?
            .parse::<u64>()
            .with_context(|| {
                format!("{} must be a positive integer", LANE.env("PACK_GENERATION"))
            })?;
        let contract = &LANE.section_packs()?.sections;
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
            LANE.env("PACK_SECTIONS")
        );
        let patch = patch_export::from_env(LANE, optional_env)?;
        let upserts = env("PNU_ALLOWLIST_PATH")?
            .map(|raw| {
                let path = std::path::Path::new(raw.as_str());
                if patch.is_some() {
                    patch_export::read_pnu_list(LANE, path)
                } else {
                    read_pnu_allowlist(path)
                }
            })
            .transpose()?;
        ensure!(
            patch.is_none() || (upserts.is_some() && sections.len() == contract.len()),
            "a patch names its upserts as {} and writes every section",
            LANE.env("PNU_ALLOWLIST_PATH")
        );
        let unit_length = crate::by_pnu_gateway_contract::section_pack_policy()?.unit_prefix_length;
        let pnu_prefix = env("PNU_PREFIX")?;
        if let Some(prefix) = &pnu_prefix {
            ensure!(
                (1..=unit_length).contains(&prefix.len())
                    && prefix.bytes().all(|byte| byte.is_ascii_digit()),
                "{} must be 1 to {unit_length} digits: a pack is one whole legal dong",
                LANE.env("PNU_PREFIX")
            );
        }
        Ok(Self {
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
                    .with_context(|| format!("{} is required", LANE.env("PACK_SUMMARY_PATH")))?,
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
    pub(crate) generation: u64,
    pub(crate) patch: Option<u64>,
    pub(crate) sections: Vec<String>,
    pub(crate) pnu_prefix: Option<String>,
    pub(crate) exported_row_count: u64,
    pub(crate) tombstone_count: u64,
    pub(crate) totals: BTreeMap<String, SectionTotals>,
    pub(crate) packs: Vec<PackEntry>,
    /// Gate (가) of this run: documents read back from their packs and compared, before writing.
    pub(crate) equality: Equality,
    pub(crate) elapsed_seconds: f64,
}

/// What a run compared before writing, and the sample candidates it drew (base runs only).
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Equality {
    /// Documents whose joined pack answer was compared with their object document; zero when the
    /// run bakes only some sections (each fragment is still checked).
    pub(crate) compared: u64,
    pub(crate) equal: u64,
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
pub(crate) async fn run() -> anyhow::Result<()> {
    sections::check_contract_sections()?;
    let config = BakeConfig::from_env()?;
    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::from_env().context("failed to configure the Iceberg catalog")?,
    )?;
    let snapshot = catalog
        .load_current_snapshot_manifest_list(GOLD_BUILDING_PANEL.table_name)
        .await?
        .context("gold.building_panel has no current snapshot to export")?;
    let lakehouse = LakehouseObjectReader::from_env()?;
    let store = ByPnuServingStore::open(LANE, &config.output)?;
    let approvals = ApprovedBuildingLinks::load_current().await?;
    let rows = scan(&config, &lakehouse, &snapshot).await?;
    let summary = bake(
        &config,
        &store,
        &snapshot_provenance(&snapshot),
        &rows,
        &approvals,
    )
    .await?;
    write_summary(&config.summary_path, &summary)?;
    tracing::info!(
        generation = summary.generation,
        patch = summary.patch,
        exported_row_count = summary.exported_row_count,
        tombstone_count = summary.tombstone_count,
        packs = summary.packs.len(),
        elapsed_seconds = summary.elapsed_seconds,
        "building section pack export succeeded"
    );
    Ok(())
}

fn snapshot_provenance(snapshot: &IcebergSnapshotManifestList) -> GoldSnapshotProvenance {
    GoldSnapshotProvenance {
        table: snapshot.table_name.clone(),
        iceberg_snapshot_id: snapshot.snapshot_id.to_string(),
        metadata_location: snapshot.metadata_location.clone(),
        manifest_list_location: snapshot.manifest_list_location.clone(),
    }
}

async fn scan(
    config: &BakeConfig,
    lakehouse: &LakehouseObjectReader,
    snapshot: &IcebergSnapshotManifestList,
) -> anyhow::Result<Vec<JsonMap<String, JsonValue>>> {
    refuse_a_moved_table(
        config.expected_gold_snapshot.as_deref(),
        &snapshot.table_name,
        &snapshot.snapshot_id.to_string(),
    )?;
    let rows = scan_snapshot_rows_kept(
        &GOLD_BUILDING_PANEL,
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
    ensure!(
        !rows.keep_limit_exceeded,
        "this shard keeps more than {MAX_ROWS_PER_RUN} rows; shard by {}",
        LANE.env("PNU_PREFIX")
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
    Ok(rows.rows)
}

/// Builds and writes the packs of the kept rows.
///
/// # Errors
/// Refuses a row the object export would refuse, a generation holding another snapshot's packs,
/// and counts that do not add up.
pub(crate) async fn bake(
    config: &BakeConfig,
    store: &ByPnuServingStore,
    provenance: &GoldSnapshotProvenance,
    rows: &[JsonMap<String, JsonValue>],
    approvals: &ApprovedBuildingLinks,
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
            .context("gold.building_panel row is missing pnu")?;
        units
            .entry(by_pnu_packs::unit_of(pnu)?.to_owned())
            .or_default()
            .push(row);
    }
    let mut tombstones: BTreeMap<String, Vec<String>> = BTreeMap::new();
    if let Some(patch) = &config.patch {
        for pnu in &patch.deleted {
            if config
                .pnu_prefix
                .as_deref()
                .is_none_or(|prefix| pnu.starts_with(prefix))
            {
                tombstones
                    .entry(by_pnu_packs::unit_of(pnu)?.to_owned())
                    .or_default()
                    .push(pnu.clone());
            }
        }
    }
    refuse_a_foreign_generation(config, store, provenance).await?;

    let all_units = units
        .keys()
        .chain(tombstones.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let (no_rows, no_deletes) = (Vec::new(), Vec::new());
    // Futures are built in a loop, not in a `map` closure: a closure over borrowed rows makes
    // the command's future fail the higher-ranked `Send` check of the command table.
    let mut jobs = Vec::with_capacity(all_units.len());
    for unit in &all_units {
        let rows = units.get(unit).unwrap_or(&no_rows);
        let deleted = tombstones.get(unit).unwrap_or(&no_deletes);
        jobs.push(write_unit(
            config, store, provenance, approvals, unit, rows, deleted,
        ));
    }
    let written = stream::iter(jobs)
        .buffer_unordered(config.max_concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    let mut packs = Vec::new();
    let mut equality = Equality::default();
    for (unit_packs, check) in written {
        packs.extend(unit_packs);
        equality.compared += check.compared;
        equality.equal += check.compared;
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
    Ok(PackExportSummary {
        schema_version: SUMMARY_SCHEMA_VERSION.to_owned(),
        document_schema_version: building_document::BUILDING_DOCUMENT_SCHEMA_VERSION.to_owned(),
        gold_table: provenance.table.clone(),
        gold_iceberg_snapshot_id: provenance.iceberg_snapshot_id.clone(),
        generation: config.generation,
        patch: config.patch.as_ref().map(|patch| patch.patch),
        sections: config.sections.clone(),
        pnu_prefix: config.pnu_prefix.clone(),
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
    provenance: &GoldSnapshotProvenance,
) -> anyhow::Result<()> {
    let patch = config.patch.as_ref().map(|patch| patch.patch);
    for section in &config.sections {
        let keys = store
            .list_pack_keys(section, config.generation, patch)
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
#[derive(Default)]
pub(super) struct UnitCheck {
    pub(super) compared: u64,
    pub(super) candidates: Vec<String>,
}

/// One dong: its documents, every section's pack laid out and read back in memory, the read-back
/// compared with the object documents (gate 가, no R2 read), then the packs written.
async fn write_unit(
    config: &BakeConfig,
    store: &ByPnuServingStore,
    provenance: &GoldSnapshotProvenance,
    approvals: &ApprovedBuildingLinks,
    unit: &str,
    rows: &[&JsonMap<String, JsonValue>],
    deleted: &[String],
) -> anyhow::Result<(Vec<PackEntry>, UnitCheck)> {
    let documents = rows
        .iter()
        .map(|row| building_document::document_with_approvals(provenance, row, approvals))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let patch = config.patch.as_ref().map(|patch| patch.patch);
    let mut laid_out = Vec::with_capacity(config.sections.len());
    for section in &config.sections {
        laid_out.push((
            section.clone(),
            lay_out_pack(config, provenance, section, unit, &documents, deleted)?,
        ));
    }
    let check = check_round_trip(config, &laid_out, &documents, deleted, patch)?;
    let mut packs = Vec::with_capacity(laid_out.len());
    for (section, bytes) in laid_out {
        packs.push(write_pack(config, store, section, unit, bytes, &documents, deleted).await?);
    }
    Ok((packs, check))
}

pub(super) fn lay_out_pack(
    config: &BakeConfig,
    provenance: &GoldSnapshotProvenance,
    section: &str,
    unit: &str,
    documents: &[BuildingByPnuDocument],
    deleted: &[String],
) -> anyhow::Result<Vec<u8>> {
    let mut writer = PackWriter::new(PackIdentity {
        lane: LANE.unit().to_owned(),
        section: section.to_owned(),
        generation: config.generation,
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
    documents: &'a [BuildingByPnuDocument],
    deleted: &'a [String],
) -> Vec<(&'a str, Option<&'a BuildingByPnuDocument>)> {
    let mut entries: Vec<(&str, Option<&BuildingByPnuDocument>)> = documents
        .iter()
        .map(|document| (document.pnu.as_str(), Some(document)))
        .chain(deleted.iter().map(|pnu| (pnu.as_str(), None)))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    entries
}

/// Gate (가) on one dong, before anything is written: every section's pack, read back from its
/// bytes, holds exactly the fragment the document cuts; when the run bakes every section, the
/// gateway's answer joined from them is byte for byte the object document the same row renders,
/// and every delete answers as a tombstone. A difference is a defect, so the run stops.
pub(super) fn check_round_trip(
    config: &BakeConfig,
    laid_out: &[(String, Vec<u8>)],
    documents: &[BuildingByPnuDocument],
    deleted: &[String],
    patch: Option<u64>,
) -> anyhow::Result<UnitCheck> {
    let mut of_unit = Vec::with_capacity(laid_out.len());
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
        of_unit.push(read::SectionPacksOfUnit {
            name: section.clone(),
            patches: patch
                .map(|number| (number, pack.clone()))
                .into_iter()
                .collect(),
            base: patch.is_none().then_some(pack),
        });
    }
    let mut check = UnitCheck::default();
    if config.sections == LANE.section_packs()?.sections {
        let packs = read::UnitPacks { sections: of_unit };
        for document in documents {
            let read::Resolved::Document(fragments) = read::resolve(&packs, &document.pnu)? else {
                bail!("{} does not answer from its packs", document.pnu);
            };
            ensure!(
                read::joined_bytes(&fragments)? == document.to_bytes()?,
                "the packs of {} do not join into its object document",
                document.pnu
            );
            check.compared += 1;
            if patch.is_none() && gate::is_sample_candidate(&document.pnu)? {
                check.candidates.push(document.pnu.clone());
            }
        }
        for pnu in deleted {
            ensure!(
                matches!(read::resolve(&packs, pnu)?, read::Resolved::Tombstone),
                "the packs do not answer {pnu} as deleted"
            );
        }
    }
    Ok(check)
}

async fn write_pack(
    config: &BakeConfig,
    store: &ByPnuServingStore,
    section: String,
    unit: &str,
    bytes: Vec<u8>,
    documents: &[BuildingByPnuDocument],
    deleted: &[String],
) -> anyhow::Result<PackEntry> {
    let patch = config.patch.as_ref().map(|patch| patch.patch);
    let pnus = if patch.is_some() {
        entries(documents, deleted)
            .iter()
            .map(|(pnu, _)| (*pnu).to_owned())
            .collect()
    } else {
        Vec::new()
    };
    let head = by_pnu_pack::read_prefix(&bytes)?.head_length();
    let key = by_pnu_packs::pack_key(LANE, &section, config.generation, patch, unit)?;
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    let created = store.write_pack_create_only(&key, &bytes, &sha256).await?;
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
