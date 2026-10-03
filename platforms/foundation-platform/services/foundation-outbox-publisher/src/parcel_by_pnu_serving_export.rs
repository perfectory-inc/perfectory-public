//! Parcel by-PNU serving object export (root ADR-0096).
//!
//! Reads `gold.parcel_panel` from the Iceberg catalog and writes one serving JSON object per
//! parcel under a caller-named generation directory. Its summary is the input
//! `publish-parcel-by-pnu-serving-manifest` needs: the generation, the snapshot it represents,
//! and every object key with its checksum.
//!
//! The command does not write the manifest. Baking the objects and pointing the gateway at them
//! are separate failures, and were separated so a half-run cannot leave the pointer aimed at a
//! generation that is not fully there.
//!
//! Objects are written create-only. A re-export of the same snapshot into the same generation is
//! an idempotent re-run; different bytes at an existing key are refused unless the caller
//! explicitly states the delta re-bake intent with the overwrite flag.

pub(crate) mod parcel_document;

use std::{
    collections::{hash_map::DefaultHasher, BTreeSet, HashSet},
    env,
    hash::{Hash as _, Hasher as _},
    path::{Path, PathBuf},
};

use anyhow::{bail, ensure, Context};
use futures_util::{stream, StreamExt as _, TryStreamExt as _};
use lakehouse_domain::GOLD_PARCEL_PANEL;
use lakehouse_infrastructure::{
    IcebergRestCatalog, IcebergSnapshotManifestList, LakehouseCatalogConfig,
};
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::lakehouse_snapshot_scan::{scan_snapshot_rows_kept, LakehouseObjectReader};
use crate::parcel_by_pnu_serving_store::{local_root, ParcelServingObjectStore};
use crate::r2_layout::parcel_by_pnu_serving_object_key;
use parcel_document::{
    GoldSnapshotProvenance, ParcelServingArtifact, PARCEL_DOCUMENT_SCHEMA_VERSION,
};

const SUMMARY_SCHEMA_VERSION: &str = "foundation-platform.parcel_by_pnu_serving_export_summary.v1";
const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_CONFIRM_EXPORT";
const OUTPUT_STORAGE_DRIVER_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_OUTPUT_STORAGE_DRIVER";
const OUTPUT_ROOT_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_OUTPUT_ROOT";
const TARGET_GENERATION_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_TARGET_GENERATION";
const EXPECTED_ROW_COUNT_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_EXPECTED_ROW_COUNT";
const MAX_CONCURRENCY_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_MAX_CONCURRENCY";
const SUMMARY_PATH_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_SUMMARY_PATH";
const ALLOW_OVERWRITE_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_ALLOW_OVERWRITE";
const PNU_ALLOWLIST_PATH_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_PNU_ALLOWLIST_PATH";
const RESUME_FROM_LISTING_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_RESUME_FROM_LISTING";
const PNU_PREFIX_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_PNU_PREFIX";
const EXPECTED_GOLD_SNAPSHOT_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID";
const FRESH_GENERATION_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_FRESH_GENERATION";
const DEFAULT_MAX_CONCURRENCY: usize = 8;
/// Measured 2026-09-09 on the Seoul bake: one R2 put costs ~0.29s from the batch host, so the
/// old cap of 32 topped out near 110 objects/s and a national bake would take days. The client
/// now retries adaptively when R2 pushes back with 429, which is what makes a higher ceiling
/// safe to offer; the default stays low and the operator raises it deliberately.
const MAX_CONCURRENCY: usize = 256;
/// The export holds every KEPT row in memory, plus the one data file the scan is decoding. This cap
/// makes that boundary an explicit refusal instead of an OOM; national runs shard by the
/// PNU-prefix filter, which drops out-of-shard rows during the scan itself, and the scan stops at
/// the first kept row past the cap.
///
/// Sized from a measurement (root ADR-0138; ai-server, 2026-10-03, local output): a real parcel
/// shard of 898,741 rows peaked at 11,108,622,336 bytes of anonymous memory, 1,970,539 rows at
/// 15,025,704,960 — about 3.66KB per kept row over a 7.8GB floor, the floor being one whole data
/// file (~1.66M rows) decoded before the filter runs. The bake's systemd unit allows 14G, the
/// 900,000-row peak x1.3; the old 2,000,000 would need 18.2GiB, which the host budget does not
/// have beside a fold. The building lane shares the cap and the unit.
const MAX_ROWS_PER_RUN: usize = 900_000;

/// Runs the parcel by-PNU serving export.
pub async fn run() -> anyhow::Result<()> {
    let config = ServingExportConfig::from_env()?;
    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::from_env().context("failed to configure the Iceberg catalog")?,
    )
    .context("failed to build the Iceberg catalog client")?;
    let snapshot = catalog
        .load_current_snapshot_manifest_list(GOLD_PARCEL_PANEL.table_name)
        .await
        .context("failed to resolve the Gold parcel panel snapshot")?
        .with_context(|| {
            format!(
                "{} has no current Iceberg snapshot to export",
                GOLD_PARCEL_PANEL.table_name
            )
        })?;

    let lakehouse = LakehouseObjectReader::from_env()?;
    let output = ParcelServingObjectStore::open(&config.output)?;
    let summary = export(&config, &lakehouse, &output, &snapshot).await?;

    if let Some(summary_path) = &config.summary_path {
        write_summary(summary_path, &summary)?;
    }

    tracing::info!(
        output_bucket = summary.output_bucket.as_deref().unwrap_or("(local)"),
        gold_table = %summary.gold_table,
        gold_iceberg_snapshot_id = %summary.gold_iceberg_snapshot_id,
        target_generation = summary.target_generation,
        scanned_row_count = summary.scanned_row_count,
        exported_row_count = summary.exported_row_count,
        created_object_count = summary.created_object_count,
        reused_object_count = summary.reused_object_count,
        listed_object_count = summary.listed_object_count,
        overwritten_object_count = summary.overwritten_object_count,
        output_storage_driver = summary.output_storage_driver,
        "parcel by-PNU serving export succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ServingExportConfig {
    output: ProfileStoreConfig,
    target_generation: u64,
    expected_row_count: Option<u64>,
    max_concurrency: usize,
    summary_path: Option<PathBuf>,
    allow_overwrite: bool,
    pnu_allowlist: Option<BTreeSet<String>>,
    resume_from_listing: bool,
    pnu_prefix: Option<String>,
    /// The Gold snapshot the whole bake is of. A shard of a multi-shard bake that finds the table
    /// moved on refuses before it scans, instead of the bake learning it after the last shard.
    expected_gold_snapshot: Option<String>,
    /// The caller started the target generation in this run, so the shard's key range must be
    /// empty before the first write. Objects there were left by a bake that did not record this
    /// generation as its own, and a resume would count them without reading them back.
    fresh_generation: bool,
}

#[derive(Debug, Serialize)]
struct ServingExportSummary {
    schema_version: &'static str,
    document_schema_version: &'static str,
    gold_table: String,
    gold_iceberg_snapshot_id: String,
    gold_metadata_location: String,
    gold_manifest_list_location: String,
    target_generation: u64,
    /// Shard filter this run kept, when one was set — the summary says what it covers.
    pnu_prefix: Option<String>,
    data_file_count: u64,
    scanned_row_count: u64,
    exported_row_count: u64,
    output_storage_driver: &'static str,
    output_bucket: Option<String>,
    created_object_count: u64,
    reused_object_count: u64,
    /// Skipped because one generation listing already named the key (resume path). Unlike
    /// `reused`, these were not byte-verified this run — publish-time sampling covers them.
    listed_object_count: u64,
    overwritten_object_count: u64,
    artifacts: Vec<ServingExportEntry>,
}

/// One baked object, named the way `publish-parcel-by-pnu-serving-manifest` reads it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ServingExportEntry {
    pnu: String,
    object_key: String,
    object_size_bytes: u64,
    object_checksum_sha256: String,
    write_outcome: &'static str,
}

impl ServingExportConfig {
    fn from_env() -> anyhow::Result<Self> {
        let confirm = optional_env(CONFIRM_ENV)?.unwrap_or_default();
        ensure!(
            confirm.eq_ignore_ascii_case("true"),
            "{CONFIRM_ENV} must be true"
        );

        let target_generation = optional_env(TARGET_GENERATION_ENV)?
            .with_context(|| format!("{TARGET_GENERATION_ENV} is required"))?
            .parse::<u64>()
            .with_context(|| format!("{TARGET_GENERATION_ENV} must be a positive integer"))?;
        ensure!(
            target_generation >= 1,
            "{TARGET_GENERATION_ENV} must be at least 1"
        );

        let pnu_allowlist = optional_env(PNU_ALLOWLIST_PATH_ENV)?
            .map(|raw| read_pnu_allowlist(Path::new(raw.as_str())))
            .transpose()?;

        Ok(Self {
            output: ProfileStoreConfig::parse(
                optional_env(OUTPUT_STORAGE_DRIVER_ENV)?
                    .unwrap_or_else(|| "local".to_owned())
                    .as_str(),
                local_root(optional_env(OUTPUT_ROOT_ENV)?),
            )
            .with_context(|| format!("{OUTPUT_STORAGE_DRIVER_ENV}/{OUTPUT_ROOT_ENV}"))?,
            target_generation,
            expected_row_count: optional_env(EXPECTED_ROW_COUNT_ENV)?
                .map(|value| parse_positive_u64(value.as_str(), EXPECTED_ROW_COUNT_ENV))
                .transpose()?,
            max_concurrency: optional_env(MAX_CONCURRENCY_ENV)?
                .map(|value| parse_max_concurrency(value.as_str()))
                .transpose()?
                .unwrap_or(DEFAULT_MAX_CONCURRENCY),
            summary_path: optional_env(SUMMARY_PATH_ENV)?.map(PathBuf::from),
            allow_overwrite: optional_env(ALLOW_OVERWRITE_ENV)?
                .is_some_and(|value| value.eq_ignore_ascii_case("true")),
            pnu_allowlist,
            // On by default: the bucket is the record (root ADR-0062), so a re-run skips
            // every object one paged listing says is already there instead of paying a
            // conditional put and a read-back per object. Off means every object is
            // byte-verified against the store again. The delta re-bake (allow_overwrite)
            // ignores this — it exists to rewrite listed objects.
            resume_from_listing: optional_env(RESUME_FROM_LISTING_ENV)?
                .is_none_or(|value| value.eq_ignore_ascii_case("true")),
            pnu_prefix: optional_env(PNU_PREFIX_ENV)?
                .map(|raw| {
                    ensure!(
                        (1..=10).contains(&raw.len())
                            && raw.bytes().all(|byte| byte.is_ascii_digit()),
                        "{PNU_PREFIX_ENV} must be 1 to 10 digits"
                    );
                    Ok(raw)
                })
                .transpose()?,
            expected_gold_snapshot: optional_env(EXPECTED_GOLD_SNAPSHOT_ENV)?,
            fresh_generation: optional_env(FRESH_GENERATION_ENV)?
                .is_some_and(|value| value.eq_ignore_ascii_case("true")),
        })
    }
}

/// Reads the one-PNU-per-line allowlist that scopes a rehearsal or a single-parcel proof run.
fn read_pnu_allowlist(path: &Path) -> anyhow::Result<BTreeSet<String>> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read the PNU allowlist {}", path.display()))?;
    let allowlist = raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    ensure!(
        !allowlist.is_empty(),
        "the PNU allowlist {} names no parcels",
        path.display()
    );
    Ok(allowlist)
}

async fn export(
    config: &ServingExportConfig,
    lakehouse: &LakehouseObjectReader,
    output: &ParcelServingObjectStore,
    snapshot: &IcebergSnapshotManifestList,
) -> anyhow::Result<ServingExportSummary> {
    refuse_a_moved_table(
        config.expected_gold_snapshot.as_deref(),
        &snapshot.table_name,
        &snapshot.snapshot_id.to_string(),
    )?;
    let provenance = GoldSnapshotProvenance {
        table: snapshot.table_name.clone(),
        iceberg_snapshot_id: snapshot.snapshot_id.to_string(),
        metadata_location: snapshot.metadata_location.clone(),
        manifest_list_location: snapshot.manifest_list_location.clone(),
    };

    let rows = scan_snapshot_rows_kept(
        &GOLD_PARCEL_PANEL,
        lakehouse,
        snapshot,
        |row| {
            match (
                &config.pnu_prefix,
                row.get("pnu").and_then(JsonValue::as_str),
            ) {
                (Some(prefix), Some(pnu)) => pnu.starts_with(prefix.as_str()),
                (Some(_), None) => true, // 식별자 없는 행은 남겨서 문서 조립이 사유를 말하며 거부하게 한다
                (None, _) => true,
            }
        },
        Some(MAX_ROWS_PER_RUN),
    )
    .await?;
    let data_file_count = rows.data_file_count;
    let scanned_row_count = rows.decoded_row_count;
    // The scan stopped at the first kept row past the cap, holding the cap and no more.
    ensure!(
        !rows.keep_limit_exceeded,
        "{} snapshot {} keeps more than {MAX_ROWS_PER_RUN} rows for this run; this export refuses \
         more than {MAX_ROWS_PER_RUN} — shard the run with {PNU_PREFIX_ENV}, not a bigger heap",
        snapshot.table_name,
        snapshot.snapshot_id,
    );
    if let Some(expected_row_count) = config.expected_row_count {
        ensure!(
            expected_row_count == scanned_row_count,
            "{} snapshot {} holds {scanned_row_count} rows but {expected_row_count} were expected",
            snapshot.table_name,
            snapshot.snapshot_id
        );
    }
    ensure!(
        scanned_row_count == rows.manifest_record_count,
        "scanned {scanned_row_count} rows but the manifests declared {} rows",
        rows.manifest_record_count
    );

    let mut selected = select_rows(&rows.rows, config.pnu_allowlist.as_ref())?;
    spread_write_order(&mut selected);
    let listed = if config.fresh_generation
        || (config.resume_from_listing && !config.allow_overwrite)
    {
        output
            .list_existing_generation_keys(config.target_generation, config.pnu_prefix.as_deref())
            .await?
    } else {
        HashSet::new()
    };
    refuse_a_claimed_shard(config, listed.len())?;
    let existing_keys = if config.resume_from_listing && !config.allow_overwrite {
        listed
    } else {
        HashSet::new()
    };
    let entries = write_artifacts(config, output, &provenance, &selected, &existing_keys).await?;

    let created_object_count = count_outcome(&entries, "created")?;
    let reused_object_count = count_outcome(&entries, "reused")?;
    let listed_object_count = count_outcome(&entries, "listed")?;
    let overwritten_object_count = count_outcome(&entries, "overwritten")?;
    Ok(ServingExportSummary {
        schema_version: SUMMARY_SCHEMA_VERSION,
        document_schema_version: PARCEL_DOCUMENT_SCHEMA_VERSION,
        gold_table: provenance.table.clone(),
        gold_iceberg_snapshot_id: provenance.iceberg_snapshot_id.clone(),
        gold_metadata_location: provenance.metadata_location.clone(),
        gold_manifest_list_location: provenance.manifest_list_location.clone(),
        target_generation: config.target_generation,
        pnu_prefix: config.pnu_prefix.clone(),
        data_file_count,
        scanned_row_count,
        exported_row_count: u64::try_from(entries.len()).context("exported row count overflow")?,
        output_storage_driver: output.storage_driver(),
        output_bucket: output.bucket().map(ToOwned::to_owned),
        created_object_count,
        reused_object_count,
        listed_object_count,
        overwritten_object_count,
        artifacts: entries,
    })
}

/// Refuses a shard whose Gold table is no longer at the snapshot the bake is of.
fn refuse_a_moved_table(expected: Option<&str>, table: &str, current: &str) -> anyhow::Result<()> {
    if let Some(expected) = expected {
        ensure!(
            expected == current,
            "{table} moved during the bake: this bake is of snapshot {expected} but the table is \
             now at {current}; nothing more is baked into this generation, the next run starts a \
             new one"
        );
    }
    Ok(())
}

/// Refuses to write into a generation this run started when the shard's range already holds keys.
fn refuse_a_claimed_shard(config: &ServingExportConfig, listed: usize) -> anyhow::Result<()> {
    ensure!(
        !config.fresh_generation || listed == 0,
        "generation {} already holds {listed} objects under shard {}, and this run did not start \
         them: a bake that never recorded this generation wrote there; choose a generation above \
         every one in the bucket",
        config.target_generation,
        config.pnu_prefix.as_deref().unwrap_or("(all)")
    );
    Ok(())
}

/// Reorders writes so concurrent puts land across the keyspace instead of on one shelf.
///
/// The Gold scan yields rows in PNU order, and neighbouring PNUs are neighbouring object keys.
/// R2 partitions writes by key internally, so a sorted write stream concentrates the whole
/// concurrency budget on one partition at a time — measured 2026-09-09: 429
/// ("Reduce your concurrent request rate") killed sorted-order bakes at 32 and again at 16
/// concurrent puts, adaptive retry included. Ordering by a deterministic hash of the PNU
/// spreads simultaneous writes across partitions; deterministic (`DefaultHasher::new()` is
/// keyed with zeros) so a resumed run replays the same order and the summary stays stable.
fn spread_write_order(rows: &mut [&JsonMap<String, JsonValue>]) {
    rows.sort_by_cached_key(|row| {
        let mut hasher = DefaultHasher::new();
        row.get("pnu").and_then(JsonValue::as_str).hash(&mut hasher);
        hasher.finish()
    });
}

/// Applies the allowlist and refuses duplicate PNUs — one parcel must resolve to one object.
fn select_rows<'a>(
    rows: &'a [JsonMap<String, JsonValue>],
    allowlist: Option<&BTreeSet<String>>,
) -> anyhow::Result<Vec<&'a JsonMap<String, JsonValue>>> {
    let mut seen = BTreeSet::new();
    let mut selected = Vec::new();
    for row in rows {
        let pnu = row
            .get("pnu")
            .and_then(JsonValue::as_str)
            .context("gold.parcel_panel row is missing pnu")?;
        ensure!(
            seen.insert(pnu.to_owned()),
            "Gold snapshot carries more than one row for parcel {pnu}"
        );
        if allowlist.is_none_or(|list| list.contains(pnu)) {
            selected.push(row);
        }
    }
    if let Some(list) = allowlist {
        let missing = list
            .iter()
            .filter(|pnu| !seen.contains(*pnu))
            .collect::<Vec<_>>();
        ensure!(
            missing.is_empty(),
            "the PNU allowlist names parcels absent from the Gold snapshot: {missing:?}"
        );
    }
    Ok(selected)
}

async fn write_artifacts(
    config: &ServingExportConfig,
    output: &ParcelServingObjectStore,
    provenance: &GoldSnapshotProvenance,
    rows: &[&JsonMap<String, JsonValue>],
    existing_keys: &HashSet<String>,
) -> anyhow::Result<Vec<ServingExportEntry>> {
    let mut writes = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        writes.push(write_artifact(
            config,
            output,
            provenance,
            row,
            index,
            existing_keys,
        ));
    }
    let mut indexed = stream::iter(writes)
        .buffer_unordered(config.max_concurrency)
        .try_collect::<Vec<_>>()
        .await?;

    indexed.sort_by_key(|(index, _)| *index);
    Ok(indexed.into_iter().map(|(_, entry)| entry).collect())
}

async fn write_artifact(
    config: &ServingExportConfig,
    output: &ParcelServingObjectStore,
    provenance: &GoldSnapshotProvenance,
    row: &JsonMap<String, JsonValue>,
    index: usize,
    existing_keys: &HashSet<String>,
) -> anyhow::Result<(usize, ServingExportEntry)> {
    let artifact = parcel_document::build(provenance, row)?;
    let object_key = parcel_by_pnu_serving_object_key(config.target_generation, &artifact.pnu)?;
    let write_outcome = if existing_keys.contains(&object_key) {
        // The listing already names this key: record the locally rebuilt artifact without a
        // network round trip. The document is a pure function of the Gold row, the original
        // write was create-only, and publish-time sampling reads a spread of these back.
        "listed"
    } else {
        write_with_policy(config, output, &object_key, &artifact).await?
    };
    Ok((
        index,
        ServingExportEntry {
            pnu: artifact.pnu.clone(),
            object_key,
            object_size_bytes: u64::try_from(artifact.body.len())
                .context("serving artifact size overflow")?,
            object_checksum_sha256: artifact.checksum_sha256.clone(),
            write_outcome,
        },
    ))
}

/// Create-only by default; the overwrite flag turns a byte-collision into a stated delta re-bake.
async fn write_with_policy(
    config: &ServingExportConfig,
    output: &ParcelServingObjectStore,
    object_key: &str,
    artifact: &ParcelServingArtifact,
) -> anyhow::Result<&'static str> {
    if !config.allow_overwrite {
        return Ok(
            if output
                .write_object_create_only(object_key, &artifact.body, &artifact.checksum_sha256)
                .await?
            {
                "created"
            } else {
                "reused"
            },
        );
    }
    match output
        .write_object_create_only(object_key, &artifact.body, &artifact.checksum_sha256)
        .await
    {
        Ok(true) => Ok("created"),
        Ok(false) => Ok("reused"),
        Err(_) => {
            output
                .write_object_overwrite(object_key, &artifact.body, &artifact.checksum_sha256)
                .await?;
            Ok("overwritten")
        }
    }
}

fn count_outcome(entries: &[ServingExportEntry], outcome: &str) -> anyhow::Result<u64> {
    u64::try_from(
        entries
            .iter()
            .filter(|entry| entry.write_outcome == outcome)
            .count(),
    )
    .context("write outcome tally overflow")
}

fn write_summary(path: &Path, summary: &ServingExportSummary) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create summary directory {}", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(summary)
        .context("failed to serialize the serving export summary")?;
    std::fs::write(path, payload)
        .with_context(|| format!("failed to write the summary {}", path.display()))
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value.trim().to_owned())),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn parse_positive_u64(value: &str, name: &str) -> anyhow::Result<u64> {
    let parsed = value
        .parse::<u64>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    ensure!(parsed > 0, "{name} must be greater than zero");
    Ok(parsed)
}

fn parse_max_concurrency(value: &str) -> anyhow::Result<usize> {
    let parsed = value
        .parse::<usize>()
        .with_context(|| format!("{MAX_CONCURRENCY_ENV} must be a positive integer"))?;
    ensure!(
        (1..=MAX_CONCURRENCY).contains(&parsed),
        "{MAX_CONCURRENCY_ENV} must be between 1 and {MAX_CONCURRENCY}"
    );
    Ok(parsed)
}

#[cfg(test)]
mod tests;
