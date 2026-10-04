//! Verified re-base of the parcel by-PNU serving lane (root ADR-0146 §1).
//!
//! `verify-parcel-by-pnu-serving-rebase` answers the question the change-set job cannot answer
//! when the snapshot the manifest reflects can no longer be compared (expired, or written before
//! `row_digest` existed): what does the lane serve, against what the current Gold snapshot holds?
//! It reads every object the manifest serves — the newest patch's object of a PNU, else the
//! base's — and compares each with the document the export would bake from the current Gold row,
//! **everything but the `source` block** (the block names the snapshot a document was baked
//! from, so the bytes of an unchanged document differ between snapshots by design).
//!
//! The comparison is by content digest: SHA-256 of the document with `source` removed, keys
//! sorted, compact. The Gold side goes through the export's own document builder, so a column
//! the document does not show cannot count as a change and a column it shows cannot be missed.
//! Every Gold content column `row_digest` covers is shown by the document (the tests prove it
//! column by column), so a PNU this command calls equal is one whose `row_digest`-relevant
//! content the served document already carries.
//!
//! It writes the same three files the change-set job writes (upserts, deletes, a summary), so
//! the bake reflects (nothing differs) or patches (some do) with the publish paths it already
//! has. It never writes to the store, and it refuses unless the comparison is complete: a read
//! failure, a count that does not add up, or a work directory of another run.
//!
//! Resumable: the Gold scan runs per PNU first digit (the scan keeps one digest per row of the
//! shard), the reads per chunk of the shard's served objects in PNU order, and each finished
//! chunk and shard is a file in the work directory a rerun skips.

#[cfg(test)]
mod tests;

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{bail, ensure, Context};
use futures_util::{stream, StreamExt as _, TryStreamExt as _};
use lakehouse_domain::GOLD_PARCEL_PANEL;
use lakehouse_infrastructure::{
    IcebergRestCatalog, IcebergSnapshotManifestList, LakehouseCatalogConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};

use crate::by_pnu_gateway_contract::{by_pnu_serving_patch_policy, ByPnuLane};
use crate::by_pnu_serving_manifest::{read_tombstone, ServedManifest};
use crate::by_pnu_serving_manifest_publish::{document_schema_version, gold_table};
use crate::by_pnu_serving_store::{local_root, ByPnuServingStore};
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::lakehouse_snapshot_scan::{scan_snapshot_rows_kept, LakehouseObjectReader};
use crate::parcel_by_pnu_serving_export::parcel_document::{self, GoldSnapshotProvenance};
use crate::r2_layout::by_pnu;

/// The `job_name` of the change set this command writes; the publish records a re-base by it.
pub(crate) const JOB_NAME: &str = "by_pnu_serving_rebase_verify";
const SUMMARY_SCHEMA_VERSION: &str = "foundation-platform.by_pnu_serving_rebase_verify.v1";
const STATE_SCHEMA_VERSION: &str = "foundation-platform.by_pnu_serving_rebase_state.v1";
/// How the served content was compared; the manifest and the run summary record it.
pub(crate) const METHOD: &str = "content digest of every served object without source";
const LANE: ByPnuLane = ByPnuLane::Parcel;
const DEFAULT_MAX_CONCURRENCY: usize = 64;
const MAX_CONCURRENCY: usize = 256;
const DEFAULT_CHUNK_OBJECTS: usize = 100_000;
const READ_ATTEMPTS: usize = 3;
/// Listings of one shard's sub-prefixes in flight at once (each holds its keys' metadata).
const LIST_CONCURRENCY: usize = 4;
/// Every PNU starts with a digit; the full comparison scans one shard per first digit.
const FULL_SHARDS: [&str; 10] = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"];

/// A SHA-256 of one document's content (`source` removed, keys sorted, compact).
pub(crate) type ContentDigest = [u8; 32];

/// What one run compares and where it keeps its work.
#[derive(Clone, Debug)]
pub(crate) struct VerifyConfig {
    pub(crate) reason: String,
    pub(crate) run_id: String,
    pub(crate) expected_gold_iceberg_snapshot_id: String,
    pub(crate) work_dir: PathBuf,
    /// `None`: the whole lane, ready to publish. `Some`: a sample of these PNU prefixes, which
    /// measures and never writes a change set.
    pub(crate) sample_prefixes: Option<Vec<String>>,
    pub(crate) max_concurrency: usize,
    pub(crate) chunk_objects: usize,
}

impl VerifyConfig {
    fn from_env() -> anyhow::Result<(Self, ProfileStoreConfig)> {
        let env = |name: &str| optional_env(&LANE.env(name));
        let required = |name: &str| -> anyhow::Result<String> {
            env(name)?.with_context(|| format!("{} is required", LANE.env(name)))
        };
        let reason = required("REBASE_REASON").with_context(|| {
            "a verified re-base is an operator decision; say why (the manifest records it)"
        })?;
        let work_dir = PathBuf::from(required("REBASE_WORK_DIR")?);
        ensure!(
            work_dir.is_absolute(),
            "{} must be an absolute path",
            LANE.env("REBASE_WORK_DIR")
        );
        let sample_prefixes = env("REBASE_SAMPLE_PREFIXES")?
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|prefix| !prefix.is_empty())
                    .map(|prefix| {
                        ensure!(
                            (1..=19).contains(&prefix.len())
                                && prefix.bytes().all(|byte| byte.is_ascii_digit()),
                            "sample prefix {prefix:?} is not 1 to 19 digits"
                        );
                        Ok(prefix.to_owned())
                    })
                    .collect::<anyhow::Result<Vec<_>>>()
            })
            .transpose()?;
        let bounded = |name: &str, default: usize, max: usize| -> anyhow::Result<usize> {
            let Some(raw) = env(name)? else {
                return Ok(default);
            };
            let value = raw
                .parse::<usize>()
                .with_context(|| format!("{} must be a positive integer", LANE.env(name)))?;
            ensure!(
                (1..=max).contains(&value),
                "{} must be between 1 and {max}",
                LANE.env(name)
            );
            Ok(value)
        };
        let config = Self {
            reason,
            run_id: required("REBASE_RUN_ID")?,
            expected_gold_iceberg_snapshot_id: required("EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID")?,
            work_dir,
            sample_prefixes,
            max_concurrency: bounded("MAX_CONCURRENCY", DEFAULT_MAX_CONCURRENCY, MAX_CONCURRENCY)?,
            chunk_objects: bounded("REBASE_CHUNK_OBJECTS", DEFAULT_CHUNK_OBJECTS, usize::MAX)?,
        };
        let output = ProfileStoreConfig::parse(
            env("OUTPUT_STORAGE_DRIVER")?
                .unwrap_or_else(|| "local".to_owned())
                .as_str(),
            local_root(env("OUTPUT_ROOT")?),
        )?;
        Ok((config, output))
    }
}

/// `verify-parcel-by-pnu-serving-rebase`.
///
/// # Errors
/// Refuses on any incomplete comparison; nothing is written but the work directory.
pub async fn run_parcel() -> anyhow::Result<()> {
    let (config, output) = VerifyConfig::from_env()?;
    let store = ByPnuServingStore::open(LANE, &output)?;
    let (manifest_bytes, _) = store.read_manifest().await?;
    let served = ServedManifest::parse(LANE, &manifest_bytes)?;
    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::from_env().context("failed to configure the Iceberg catalog")?,
    )
    .context("failed to build the Iceberg catalog client")?;
    let snapshot = catalog
        .load_current_snapshot_manifest_list(GOLD_PARCEL_PANEL.table_name)
        .await?
        .context("gold.parcel_panel has no current snapshot")?;
    ensure!(
        snapshot.snapshot_id.to_string() == config.expected_gold_iceberg_snapshot_id,
        "gold.parcel_panel is at snapshot {}, not the stated {}; the re-base compares against \
         the snapshot the bake will publish",
        snapshot.snapshot_id,
        config.expected_gold_iceberg_snapshot_id
    );
    let lakehouse = LakehouseObjectReader::from_env()?;
    let provenance = GoldSnapshotProvenance {
        table: snapshot.table_name.clone(),
        iceberg_snapshot_id: snapshot.snapshot_id.to_string(),
        metadata_location: snapshot.metadata_location.clone(),
        manifest_list_location: snapshot.manifest_list_location.clone(),
    };
    let gold = |prefix: String| scan_gold_shard(&lakehouse, &snapshot, &provenance, prefix);
    let read = |key: String| {
        let store = &store;
        async move { store.read_bytes(&key).await }
    };
    let outcome = verify(&config, &store, &served, gold, read).await?;
    tracing::info!(
        unit = LANE.unit(),
        run_id = %config.run_id,
        served_objects_read = outcome.totals.served_objects_read,
        equal = outcome.totals.equal,
        changed = outcome.totals.changed,
        only_served = outcome.totals.only_served,
        only_gold = outcome.totals.only_gold,
        sample = config.sample_prefixes.is_some(),
        "parcel by-PNU serving re-base verified"
    );
    Ok(())
}

/// One shard of the Gold side: a content digest per PNU, and the table's row count.
#[derive(Debug, Default)]
pub(crate) struct GoldShard {
    pub(crate) digests: HashMap<u64, ContentDigest>,
    /// Rows of the whole snapshot (every shard scans all of them and keeps its own).
    pub(crate) table_rows: u64,
}

/// What a run found, summed over its shards.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Totals {
    pub(crate) gold_rows: u64,
    pub(crate) served_base_listed: u64,
    pub(crate) served_patch_listed: u64,
    pub(crate) served_objects_read: u64,
    pub(crate) equal: u64,
    pub(crate) changed: u64,
    pub(crate) only_served: u64,
    pub(crate) only_gold: u64,
    /// Patch tombstones read; a tombstoned PNU Gold holds again counts in `only_gold` too.
    pub(crate) tombstones_read: u64,
}

impl Totals {
    fn add(&mut self, other: &Self) {
        self.gold_rows += other.gold_rows;
        self.served_base_listed += other.served_base_listed;
        self.served_patch_listed += other.served_patch_listed;
        self.served_objects_read += other.served_objects_read;
        self.equal += other.equal;
        self.changed += other.changed;
        self.only_served += other.only_served;
        self.only_gold += other.only_gold;
        self.tombstones_read += other.tombstones_read;
    }
}

/// The verdicts of one shard (or one chunk of it).
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Verdicts {
    pub(crate) totals: Totals,
    /// Served documents whose content differs from the current Gold render.
    pub(crate) changed: Vec<String>,
    /// Served PNUs Gold no longer holds: tombstones.
    pub(crate) only_served: Vec<String>,
    /// Gold PNUs the lane does not answer for (absent, or tombstoned): new documents.
    pub(crate) only_gold: Vec<String>,
}

impl Verdicts {
    fn add(&mut self, other: Self) {
        self.totals.add(&other.totals);
        self.changed.extend(other.changed);
        self.only_served.extend(other.only_served);
        self.only_gold.extend(other.only_gold);
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ChunkRecord {
    first: String,
    last: String,
    count: usize,
    verdicts: Verdicts,
}

#[derive(Debug, Serialize, Deserialize)]
struct ShardRecord {
    prefix: String,
    gold_table_rows: u64,
    verdicts: Verdicts,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
struct RunState {
    schema_version: String,
    run_id: String,
    gold_iceberg_snapshot_id: String,
    reflected_gold_iceberg_snapshot_id: String,
    base_generation: u64,
    patches: Vec<u64>,
    sample_prefixes: Option<Vec<String>>,
    chunk_objects: usize,
}

/// The run's outcome; the change-set files are written only for a complete, full comparison.
#[derive(Debug)]
pub(crate) struct Outcome {
    pub(crate) totals: Totals,
}

/// Compares every served object with the current Gold render and writes the change set.
///
/// `gold(prefix)` scans the current Gold snapshot's rows whose PNU starts with `prefix` and
/// returns their content digests; `read(key)` returns a served object's exact bytes.
///
/// # Errors
/// Refuses a work directory of another run, a base of another document schema, any read or
/// parse failure, counts that do not add up, and a change set over the contract's
/// `max_delta_fraction`.
pub(crate) async fn verify<G, GF, R, RF>(
    config: &VerifyConfig,
    store: &ByPnuServingStore,
    served: &ServedManifest,
    gold: G,
    read: R,
) -> anyhow::Result<Outcome>
where
    G: Fn(String) -> GF,
    GF: Future<Output = anyhow::Result<GoldShard>>,
    R: Fn(String) -> RF,
    RF: Future<Output = anyhow::Result<Vec<u8>>>,
{
    let lane = store.lane();
    ensure!(
        lane == LANE,
        "the verified re-base renders parcel documents, not {} documents",
        lane.noun()
    );
    if let Some(schema) = &served.document_schema_version {
        ensure!(
            schema == document_schema_version(lane),
            "the base holds {schema} documents but the export bakes {}; a schema change needs a \
             full bake, not a re-base",
            document_schema_version(lane)
        );
    }
    std::fs::create_dir_all(&config.work_dir)
        .with_context(|| format!("failed to create {}", config.work_dir.display()))?;
    adopt_work_dir(config, served)?;

    let shards: Vec<String> = config.sample_prefixes.clone().unwrap_or_else(|| {
        FULL_SHARDS
            .iter()
            .map(|digit| (*digit).to_owned())
            .collect()
    });
    let mut all = Verdicts::default();
    let mut table_rows = BTreeSet::new();
    for prefix in &shards {
        let record = shard(config, store, served, &gold, &read, prefix).await?;
        table_rows.insert(record.gold_table_rows);
        all.add(record.verdicts);
    }
    ensure!(
        table_rows.len() == 1,
        "the shards scanned Gold snapshots of different sizes {table_rows:?}; the table is not \
         the one this run started on"
    );
    let totals = all.totals.clone();
    if config.sample_prefixes.is_some() {
        write_json(
            &config.work_dir.join("sample-summary.json"),
            &summary_json(config, served, &all, None),
        )?;
        tracing::info!("a sample measures; it writes no change set and cannot be published");
        return Ok(Outcome { totals });
    }

    // The whole lane: every count must add up before a change set exists.
    let answering = totals.equal + totals.changed + totals.only_served;
    let gold_rows = table_rows.into_iter().next().unwrap_or_default();
    ensure!(
        totals.gold_rows == gold_rows,
        "the shards kept {} Gold rows but the snapshot holds {gold_rows}; a row outside every \
         digit shard was not compared",
        totals.gold_rows
    );
    ensure!(
        totals.served_base_listed == served.base_object_count,
        "the base lists {} objects but the manifest says {}; the comparison is incomplete",
        totals.served_base_listed,
        served.base_object_count
    );
    let patch_objects: u64 = served
        .patches
        .iter()
        .map(|patch| patch.upserted + patch.deleted)
        .sum();
    ensure!(
        totals.served_patch_listed == patch_objects,
        "the patches list {} objects but the manifest says {patch_objects}",
        totals.served_patch_listed
    );
    ensure!(
        answering == served.object_count,
        "{answering} served PNUs answer but the manifest says {}; the comparison is incomplete",
        served.object_count
    );
    let current_total = totals.equal + totals.changed + totals.only_gold;
    ensure!(
        current_total == gold_rows,
        "the verdicts cover {current_total} Gold PNUs of {gold_rows}"
    );
    let changes = totals.changed + totals.only_gold + totals.only_served;
    let fraction = by_pnu_serving_patch_policy()?.max_delta_fraction;
    #[allow(clippy::cast_precision_loss)] // counts far below 2^52
    let over = changes as f64 > current_total as f64 * fraction;
    ensure!(
        !over,
        "{changes} of {current_total} PNUs differ from what the lane serves, more than the \
         contract's max_delta_fraction {fraction} — not a delta; nothing is published and the \
         lane needs a full bake (root ADR-0141 §3)"
    );

    let mut upserts = all.changed.clone();
    upserts.extend(all.only_gold.iter().cloned());
    upserts.sort_unstable();
    let mut deletes = all.only_served.clone();
    deletes.sort_unstable();
    write_lines(&config.work_dir.join("upserts.txt"), &upserts)?;
    write_lines(&config.work_dir.join("deletes.txt"), &deletes)?;
    write_json(
        &config.work_dir.join("change-set.json"),
        &summary_json(config, served, &all, Some((&upserts, &deletes))),
    )?;
    Ok(Outcome { totals })
}

/// The change-set summary the publish reads (`by_pnu_serving_manifest_publish`), or the sample's.
fn summary_json(
    config: &VerifyConfig,
    served: &ServedManifest,
    all: &Verdicts,
    lists: Option<(&[String], &[String])>,
) -> JsonValue {
    let totals = &all.totals;
    let new_count = totals.only_gold;
    serde_json::json!({
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "job_name": JOB_NAME,
        "contract": gold_table(LANE),
        "row_count": lists.map_or(0, |(upserts, _)| upserts.len()),
        "quality_metrics": {
            "changed_count": totals.changed,
            "new_count": new_count,
            "deleted_count": totals.only_served,
            "unchanged_count": totals.equal,
            "current_total": totals.equal + totals.changed + totals.only_gold,
            "upsert_count": lists.map_or(0, |(upserts, _)| upserts.len()),
            "delete_count": lists.map_or(0, |(_, deletes)| deletes.len()),
        },
        "input": {
            "unit": LANE.noun(),
            "baseline_snapshot_id": served.reflected_gold_iceberg_snapshot_id,
            "current_snapshot_id": config.expected_gold_iceberg_snapshot_id,
            "base_generation": served.base_generation,
            "patches": served.patches.iter().map(|patch| patch.generation).collect::<Vec<_>>(),
            "sample_prefixes": config.sample_prefixes,
        },
        "verification": {
            "run_id": config.run_id,
            "reason": config.reason,
            "method": METHOD,
            "served_objects_read": totals.served_objects_read,
            "served_base_listed": totals.served_base_listed,
            "served_patch_listed": totals.served_patch_listed,
            "tombstones_read": totals.tombstones_read,
            "gold_rows": totals.gold_rows,
            "equal": totals.equal,
            "changed": totals.changed,
            "only_served": totals.only_served,
            "only_gold": totals.only_gold,
        },
    })
}

/// A work directory belongs to one run: same run id, same served state, same Gold snapshot.
fn adopt_work_dir(config: &VerifyConfig, served: &ServedManifest) -> anyhow::Result<()> {
    let state = RunState {
        schema_version: STATE_SCHEMA_VERSION.to_owned(),
        run_id: config.run_id.clone(),
        gold_iceberg_snapshot_id: config.expected_gold_iceberg_snapshot_id.clone(),
        reflected_gold_iceberg_snapshot_id: served.reflected_gold_iceberg_snapshot_id.clone(),
        base_generation: served.base_generation,
        patches: served
            .patches
            .iter()
            .map(|patch| patch.generation)
            .collect(),
        sample_prefixes: config.sample_prefixes.clone(),
        chunk_objects: config.chunk_objects,
    };
    let path = config.work_dir.join("state.json");
    match std::fs::read(&path) {
        Ok(bytes) => {
            let earlier: RunState = serde_json::from_slice(&bytes)
                .with_context(|| format!("{} is not a re-base state", path.display()))?;
            ensure!(
                earlier == state,
                "{} holds another run's work ({} over generation {} at Gold {}); a re-base \
                 resumes only its own run — use a new work directory",
                config.work_dir.display(),
                earlier.run_id,
                earlier.base_generation,
                earlier.gold_iceberg_snapshot_id
            );
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            ensure!(
                std::fs::read_dir(&config.work_dir)?.next().is_none(),
                "{} is not empty and holds no re-base state; refusing to mix its files in",
                config.work_dir.display()
            );
            write_json(&path, &state)
        }
        Err(error) => Err(error).with_context(|| format!("failed to read {}", path.display())),
    }
}

/// Where one served PNU's answer comes from. Kept small: a digit shard holds tens of millions.
#[derive(Clone, Copy, Debug)]
struct ServedObject {
    pnu: u64,
    generation: u64,
    /// The newest patch holding the PNU, or `None` for the base object.
    patch: Option<u64>,
}

impl ServedObject {
    fn key(&self) -> String {
        let pnu = format_pnu(self.pnu);
        match self.patch {
            Some(patch) => by_pnu::patch_object_key(LANE, self.generation, patch, &pnu),
            None => by_pnu::object_key(LANE, self.generation, &pnu),
        }
        .unwrap_or_else(|_| format!("<no canonical key for {pnu}>"))
    }
}

async fn shard<G, GF, R, RF>(
    config: &VerifyConfig,
    store: &ByPnuServingStore,
    served: &ServedManifest,
    gold: &G,
    read: &R,
    prefix: &str,
) -> anyhow::Result<ShardRecord>
where
    G: Fn(String) -> GF,
    GF: Future<Output = anyhow::Result<GoldShard>>,
    R: Fn(String) -> RF,
    RF: Future<Output = anyhow::Result<Vec<u8>>>,
{
    let record_path = config.work_dir.join(format!("shard-{prefix}.json"));
    if let Ok(bytes) = std::fs::read(&record_path) {
        return serde_json::from_slice(&bytes)
            .with_context(|| format!("{} is not a shard record", record_path.display()));
    }
    let GoldShard {
        mut digests,
        table_rows,
    } = gold(prefix.to_owned()).await?;
    tracing::info!(
        prefix,
        gold_rows = digests.len(),
        table_rows,
        "re-base shard: Gold scanned"
    );
    let mut verdicts = Verdicts::default();
    verdicts.totals.gold_rows = u64::try_from(digests.len())?;

    let (objects, base_listed, patch_listed) = served_view(store, served, prefix).await?;
    tracing::info!(
        prefix,
        served = objects.len(),
        base_listed,
        patch_listed,
        "re-base shard: served objects listed"
    );
    verdicts.totals.served_base_listed = base_listed;
    verdicts.totals.served_patch_listed = patch_listed;
    let chunk_dir = config.work_dir.join(format!("shard-{prefix}"));
    std::fs::create_dir_all(&chunk_dir)?;
    for (index, chunk) in objects.chunks(config.chunk_objects).enumerate() {
        let path = chunk_dir.join(format!("chunk-{index:06}.json"));
        let (first, last) = match (chunk.first(), chunk.last()) {
            (Some(first), Some(last)) => (format_pnu(first.pnu), format_pnu(last.pnu)),
            _ => continue,
        };
        let done = match std::fs::read(&path) {
            Ok(bytes) => {
                let record: ChunkRecord = serde_json::from_slice(&bytes)
                    .with_context(|| format!("{} is not a chunk record", path.display()))?;
                ensure!(
                    record.first == first && record.last == last && record.count == chunk.len(),
                    "{} covered {}..{} ({} objects) but the listing now gives {first}..{last} \
                     ({}); the served objects changed during the re-base",
                    path.display(),
                    record.first,
                    record.last,
                    record.count,
                    chunk.len()
                );
                for object in chunk {
                    digests.remove(&object.pnu);
                }
                record.verdicts
            }
            Err(_) => {
                let chunk_verdicts = compare_chunk(config, read, chunk, &mut digests).await?;
                write_json(
                    &path,
                    &ChunkRecord {
                        first,
                        last,
                        count: chunk.len(),
                        verdicts: chunk_verdicts.clone(),
                    },
                )?;
                chunk_verdicts
            }
        };
        verdicts.add(done);
    }
    ensure!(
        verdicts.totals.served_objects_read == u64::try_from(objects.len())?,
        "shard {prefix} read {} served objects but its served view holds {}; the comparison is          incomplete",
        verdicts.totals.served_objects_read,
        objects.len()
    );
    let mut only_gold = digests.into_keys().map(format_pnu).collect::<Vec<_>>();
    only_gold.sort_unstable();
    verdicts.totals.only_gold += u64::try_from(only_gold.len())?;
    verdicts.only_gold.extend(only_gold);
    let record = ShardRecord {
        prefix: prefix.to_owned(),
        gold_table_rows: table_rows,
        verdicts,
    };
    write_json(&record_path, &record)?;
    tracing::info!(
        prefix,
        equal = record.verdicts.totals.equal,
        changed = record.verdicts.totals.changed,
        only_served = record.verdicts.totals.only_served,
        only_gold = record.verdicts.totals.only_gold,
        "re-base shard verified"
    );
    Ok(record)
}

/// The served view under `prefix`: per PNU the newest patch's object, else the base's, in PNU
/// order; with the base and patch listing counts.
async fn served_view(
    store: &ByPnuServingStore,
    served: &ServedManifest,
    prefix: &str,
) -> anyhow::Result<(Vec<ServedObject>, u64, u64)> {
    // Listed one digit further down, a few at a time: a national digit shard is tens of
    // millions of keys, and one listing of it would hold every key's metadata at once.
    let sub_prefixes: Vec<String> = if prefix.len() < 19 {
        (0..10).map(|digit| format!("{prefix}{digit}")).collect()
    } else {
        vec![prefix.to_owned()]
    };
    let parts = stream::iter(
        sub_prefixes
            .into_iter()
            .map(|sub: String| async move { served_view_under(store, served, &sub).await }),
    )
    .buffer_unordered(LIST_CONCURRENCY)
    .try_collect::<Vec<_>>()
    .await?;
    let mut objects = Vec::new();
    let (mut base_listed, mut patch_listed) = (0_u64, 0_u64);
    for (part, base, patches) in parts {
        objects.extend(part);
        base_listed += base;
        patch_listed += patches;
    }
    objects.sort_unstable_by_key(|object| object.pnu);
    Ok((objects, base_listed, patch_listed))
}

async fn served_view_under(
    store: &ByPnuServingStore,
    served: &ServedManifest,
    prefix: &str,
) -> anyhow::Result<(Vec<ServedObject>, u64, u64)> {
    let lane = store.lane();
    let generation = served.base_generation;
    let mut view: HashMap<u64, ServedObject> = HashMap::new();
    let mut base_listed = 0_u64;
    let mut patch_listed = 0_u64;
    // Oldest patch first, so a newer patch replaces it; the base goes in under every patch.
    for patch in served.patches.iter().rev() {
        for key in store
            .list_patch_keys(generation, patch.generation, Some(prefix))
            .await?
        {
            let (_, _, pnu) = by_pnu::parse_patch_object_key(lane, &key)
                .with_context(|| format!("{key} is not a patch object key"))?;
            patch_listed += 1;
            let pnu = parse_pnu(&pnu)?;
            view.insert(
                pnu,
                ServedObject {
                    pnu,
                    generation,
                    patch: Some(patch.generation),
                },
            );
        }
    }
    for key in store
        .list_existing_generation_keys(generation, Some(prefix))
        .await?
    {
        base_listed += 1;
        let pnu = parse_pnu(pnu_of_key(&key)?)?;
        view.entry(pnu).or_insert(ServedObject {
            pnu,
            generation,
            patch: None,
        });
    }
    Ok((view.into_values().collect(), base_listed, patch_listed))
}

async fn compare_chunk<R, RF>(
    config: &VerifyConfig,
    read: &R,
    chunk: &[ServedObject],
    digests: &mut HashMap<u64, ContentDigest>,
) -> anyhow::Result<Verdicts>
where
    R: Fn(String) -> RF,
    RF: Future<Output = anyhow::Result<Vec<u8>>>,
{
    let reads = chunk.iter().copied().map(|object| read_one(read, object));
    let read_back = stream::iter(reads)
        .buffer_unordered(config.max_concurrency)
        .try_collect::<Vec<_>>()
        .await?;

    let mut verdicts = Verdicts::default();
    for (object, digest) in read_back {
        verdicts.totals.served_objects_read += 1;
        let gold = digests.remove(&object.pnu);
        match (digest, gold) {
            (None, Some(_)) => {
                // Tombstoned, and Gold holds the PNU again: it needs a document.
                verdicts.totals.tombstones_read += 1;
                verdicts.totals.only_gold += 1;
                verdicts.only_gold.push(format_pnu(object.pnu));
            }
            (None, None) => verdicts.totals.tombstones_read += 1,
            (Some(_), None) => {
                verdicts.totals.only_served += 1;
                verdicts.only_served.push(format_pnu(object.pnu));
            }
            (Some(served), Some(gold)) if served == gold => verdicts.totals.equal += 1,
            (Some(_), Some(_)) => {
                verdicts.totals.changed += 1;
                verdicts.changed.push(format_pnu(object.pnu));
            }
        }
    }
    verdicts.changed.sort_unstable();
    verdicts.only_served.sort_unstable();
    verdicts.only_gold.sort_unstable();
    Ok(verdicts)
}

/// Reads one served object (with retries) and digests it.
async fn read_one<R, RF>(
    read: &R,
    object: ServedObject,
) -> anyhow::Result<(ServedObject, Option<ContentDigest>)>
where
    R: Fn(String) -> RF,
    RF: Future<Output = anyhow::Result<Vec<u8>>>,
{
    let mut last_error = None;
    for _ in 0..READ_ATTEMPTS {
        match read(object.key()).await {
            Ok(bytes) => {
                let digest = served_digest(&object, &bytes)?;
                return Ok((object, digest));
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no read was attempted"))).with_context(|| {
        format!(
            "served object {} could not be read in {READ_ATTEMPTS} attempts; the comparison is              incomplete and nothing is published",
            object.key()
        )
    })
}

/// The content digest of a served object, `None` for a patch tombstone of its own PNU.
fn served_digest(object: &ServedObject, bytes: &[u8]) -> anyhow::Result<Option<ContentDigest>> {
    let pnu = format_pnu(object.pnu);
    let key = object.key();
    if object.patch.is_some() {
        if let Some(tombstone) = read_tombstone(bytes) {
            ensure!(
                tombstone.pnu == pnu,
                "{key} is a tombstone of {}, not of {pnu}",
                tombstone.pnu
            );
            return Ok(None);
        }
    }
    let document: JsonValue = serde_json::from_slice(bytes)
        .with_context(|| format!("{key} does not hold a JSON document"))?;
    ensure!(
        document.get("pnu").and_then(JsonValue::as_str) == Some(pnu.as_str()),
        "{key} holds a document of another parcel"
    );
    let schema = document_schema_version(LANE);
    ensure!(
        document.get("schema_version").and_then(JsonValue::as_str) == Some(schema),
        "{key} does not hold a {schema} document"
    );
    content_digest_of(document).map(Some)
}

/// SHA-256 of a document's content: `source` removed, object keys sorted, compact.
///
/// # Errors
/// Refuses bytes that are not a JSON object.
pub(crate) fn content_digest(bytes: &[u8]) -> anyhow::Result<ContentDigest> {
    content_digest_of(serde_json::from_slice(bytes).context("the document is not JSON")?)
}

fn content_digest_of(mut document: JsonValue) -> anyhow::Result<ContentDigest> {
    let object = document
        .as_object_mut()
        .context("the document is not a JSON object")?;
    object.remove("source");
    let mut canonical = String::new();
    write_canonical(&document, &mut canonical)?;
    Ok(Sha256::digest(canonical.as_bytes()).into())
}

fn write_canonical(value: &JsonValue, out: &mut String) -> anyhow::Result<()> {
    match value {
        JsonValue::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            out.push('{');
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key)?);
                out.push(':');
                write_canonical(&map[key], out)?;
            }
            out.push('}');
        }
        JsonValue::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_canonical(item, out)?;
            }
            out.push(']');
        }
        scalar => out.push_str(&serde_json::to_string(scalar)?),
    }
    Ok(())
}

/// The content digest of the document the export bakes from one Gold row.
///
/// # Errors
/// Refuses a row the export would refuse.
pub(crate) fn gold_digest(
    provenance: &GoldSnapshotProvenance,
    row: &JsonMap<String, JsonValue>,
) -> anyhow::Result<(u64, ContentDigest)> {
    let artifact = parcel_document::build(provenance, row)?;
    Ok((parse_pnu(&artifact.pnu)?, content_digest(&artifact.body)?))
}

/// Scans the current Gold snapshot once for the rows under `prefix`, keeping one digest per row.
async fn scan_gold_shard(
    lakehouse: &LakehouseObjectReader,
    snapshot: &IcebergSnapshotManifestList,
    provenance: &GoldSnapshotProvenance,
    prefix: String,
) -> anyhow::Result<GoldShard> {
    let digests = Mutex::new(HashMap::new());
    let failure = Mutex::new(None::<anyhow::Error>);
    let rows = scan_snapshot_rows_kept(
        &GOLD_PARCEL_PANEL,
        lakehouse,
        snapshot,
        |row| {
            let inside = row
                .get("pnu")
                .and_then(JsonValue::as_str)
                .is_none_or(|pnu| pnu.starts_with(prefix.as_str()));
            if inside {
                let outcome = gold_digest(provenance, row).and_then(|(pnu, digest)| {
                    let mut digests = digests
                        .lock()
                        .map_err(|_| anyhow::anyhow!("the digest map lock was poisoned"))?;
                    ensure!(
                        digests.insert(pnu, digest).is_none(),
                        "Gold snapshot carries more than one row for parcel {}",
                        format_pnu(pnu)
                    );
                    Ok(())
                });
                if let Err(error) = outcome {
                    if let Ok(mut slot) = failure.lock() {
                        slot.get_or_insert(error);
                    }
                }
            }
            false // nothing is kept: the digest is all the comparison needs
        },
        None,
    )
    .await?;
    if let Some(error) = failure
        .into_inner()
        .map_err(|_| anyhow::anyhow!("the failure lock was poisoned"))?
    {
        return Err(error);
    }
    ensure!(
        rows.decoded_row_count == rows.manifest_record_count,
        "scanned {} rows but the manifests declared {}",
        rows.decoded_row_count,
        rows.manifest_record_count
    );
    Ok(GoldShard {
        digests: digests
            .into_inner()
            .map_err(|_| anyhow::anyhow!("the digest map lock was poisoned"))?,
        table_rows: rows.decoded_row_count,
    })
}

fn pnu_of_key(key: &str) -> anyhow::Result<&str> {
    key.rsplit('/')
        .next()
        .and_then(|name| name.strip_suffix(".json"))
        .with_context(|| format!("{key} does not end in a PNU object file name"))
}

fn parse_pnu(pnu: &str) -> anyhow::Result<u64> {
    by_pnu::check_pnu(LANE, pnu)?;
    ensure!(pnu.len() == 19, "PNU {pnu} is not 19 digits");
    pnu.parse::<u64>()
        .with_context(|| format!("PNU {pnu} is not a number"))
}

fn format_pnu(pnu: u64) -> String {
    format!("{pnu:019}")
}

fn write_lines(path: &Path, lines: &[String]) -> anyhow::Result<()> {
    let body = lines
        .iter()
        .map(|line| format!("{line}\n"))
        .collect::<String>();
    write_atomic(path, body.as_bytes())
}

fn write_json(path: &Path, value: &impl Serialize) -> anyhow::Result<()> {
    let mut body = serde_json::to_vec_pretty(value).context("failed to serialize")?;
    body.push(b'\n');
    write_atomic(path, &body)
}

/// A record is either whole or absent: a crash mid-write leaves no half record to resume from.
fn write_atomic(path: &Path, body: &[u8]) -> anyhow::Result<()> {
    let partial = path.with_extension("partial");
    std::fs::write(&partial, body)
        .with_context(|| format!("failed to write {}", partial.display()))?;
    std::fs::rename(&partial, path)
        .with_context(|| format!("failed to move {} into place", path.display()))
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value.trim().to_owned())),
        Ok(_) | Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}
