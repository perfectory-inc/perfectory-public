//! A Silver lane that refreshes itself from the newest complete release in the Bronze ledger
//! (root ADR-0169 §2–4), in the shape of the FLOOR cycle (root ADR-0128).
//!
//! One run of `foundation-silver-refresh@<lane>.service`:
//!
//! 1. reads the ledger (`catalog.bronze_object`) and picks the newest provider month in which every
//!    source of the lane has an object (`release::select`);
//! 2. asks the Silver table's own snapshot summaries whether that release is already in it — the
//!    `foundation.ingest-batch-objects` record `append_batch_once` commits with the rows. If it is,
//!    or a newer release of the lane is, the run ends `unchanged` in seconds and writes nothing;
//! 3. otherwise stages the release's Bronze (verified against the ledger's bytes), runs the lane's
//!    export, loads the handoff with `silver_scalar_handoff_to_lakehouse.py`, and reads the table's
//!    record again to see the release in it: `changed`.
//!
//! The last line of every successful run is `silver-refresh-outcome lane=… outcome=changed|unchanged
//! …` (ADR-0169 §4). The lane's runner values are its contract's (`lane.rs`); this file holds none.
use std::collections::BTreeSet;

use anyhow::{ensure, Context};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde::Deserialize;
use sqlx::PgPool;

mod execute;
mod lane;
mod release;
#[cfg(test)]
mod tests;

use super::invocation_cleanup::{self, Job};
use execute::{Runtime, SparkLoad};
use lane::{Lane, LaneContract};
use release::Release;

const LANE_ENV: &str = "FOUNDATION_PLATFORM_SILVER_REFRESH_LANE";
const STATE_ROOT_ENV: &str = "FOUNDATION_PLATFORM_SILVER_REFRESH_STATE_ROOT";
/// `silver-refresh.sh <lane> --plan`: decide, report, and do nothing else.
const PLAN_ENV: &str = "FOUNDATION_PLATFORM_SILVER_REFRESH_PLAN";

/// What the run found, and what it reports on its last line.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Decision {
    /// The table already reflects this release (or a newer one): nothing to do.
    Unchanged(&'static str),
    /// The release has no handoff yet: export it, then load it.
    ExportAndLoad,
    /// A part lane's complete handoff exists in R2 and some of its parts are not in the table.
    LoadOnly,
}

/// `ExecStopPost`: remove the Spark container a killed run left (the Compose project of this
/// invocation, nothing else).
pub(super) fn stop() -> anyhow::Result<()> {
    invocation_cleanup::run(Job::SilverRefresh)
}

pub(super) async fn run() -> anyhow::Result<()> {
    let mut lookup = |name: &str| std::env::var(name).ok();
    let lane = Lane::parse(&super::required_lookup(&mut lookup, LANE_ENV)?)?;
    let contract = lane.contract()?;
    let runtime = runtime(lane, &mut lookup)?;
    let pool = PgPool::connect(&super::required_lookup(&mut lookup, "DATABASE_URL")?)
        .await
        .context("cannot connect to the Bronze ledger")?;
    let ledger = release::read_ledger(&pool, &contract.roles).await;
    pool.close().await;
    let release = release::select(&contract.roles, &ledger?)?;
    for month in &release.skipped_incomplete {
        println!(
            "silver-refresh lane={} skipped_incomplete_release={}",
            lane.id(),
            month.format("%Y%m")
        );
    }
    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::from_env()
            .context("the Silver refresh reads the lakehouse catalog")?,
    )
    .context("cannot build the Iceberg catalog client")?;
    let ingested = catalog
        .load_ingested_batch_objects(&contract.table)
        .await?
        .unwrap_or_default();
    let plan = super::optional_bool_lookup(&mut lookup, PLAN_ENV)?.unwrap_or(false);
    let (decision, identity, rows) = if contract.kind.loads_as_one_run() {
        refresh_run(&contract, &release, &runtime, &catalog, &ingested, plan).await?
    } else {
        refresh_parts(&contract, &release, &runtime, &catalog, &ingested, plan).await?
    };
    let line = outcome_line(lane, &release, &decision, &identity, rows);
    println!("{}", if plan { as_plan(&line) } else { line });
    Ok(())
}

/// A plan stages, exports and writes nothing, so its line must not read as a run's outcome.
pub(super) fn as_plan(outcome: &str) -> String {
    outcome
        .replacen("silver-refresh-outcome ", "silver-refresh-plan ", 1)
        .replacen(" outcome=changed ", " outcome=would_change ", 1)
}

fn runtime(lane: Lane, lookup: &mut impl FnMut(&str) -> Option<String>) -> anyhow::Result<Runtime> {
    let absolute = |raw: String, name: &str| {
        let path = std::path::PathBuf::from(raw);
        ensure!(path.is_absolute(), "{name} must be an absolute path");
        Ok(path)
    };
    let state_root = absolute(
        super::required_lookup(lookup, STATE_ROOT_ENV)?,
        STATE_ROOT_ENV,
    )?;
    let release_root = absolute(
        super::required_lookup(lookup, "FOUNDATION_PLATFORM_SILVER_REFRESH_RELEASE_ROOT")?,
        "the release root",
    )?;
    ensure!(
        !state_root.starts_with(&release_root),
        "the Silver refresh state must live outside the immutable release"
    );
    let gid = super::required_lookup(lookup, "FOUNDATION_PLATFORM_LAKEHOUSE_GID")?;
    ensure!(
        !gid.is_empty() && gid.bytes().all(|b| b.is_ascii_digit()),
        "FOUNDATION_PLATFORM_LAKEHOUSE_GID must be a numeric group id"
    );
    Ok(Runtime {
        release_root,
        lane_root: state_root.join(lane.id()),
        spark_jars: super::required_lookup(
            lookup,
            "FOUNDATION_PLATFORM_SILVER_REFRESH_SPARK_JARS",
        )?,
        jars_dir: absolute(
            super::required_lookup(lookup, "FOUNDATION_PLATFORM_SILVER_REFRESH_JARS_DIR")?,
            "the release jars directory",
        )?,
        project: invocation_cleanup::project_id(Job::SilverRefresh, lookup)?,
        gid,
        publisher: std::env::current_exe().context("cannot locate the running publisher")?,
    })
}

/// The one line a run ends with, for the job log and whatever reads it next (ADR-0169 §4).
pub(super) fn outcome_line(
    lane: Lane,
    release: &Release,
    decision: &Decision,
    identity: &str,
    rows: Option<u64>,
) -> String {
    let (outcome, reason) = match decision {
        Decision::Unchanged(reason) => ("unchanged", *reason),
        Decision::ExportAndLoad => ("changed", "exported_and_loaded"),
        Decision::LoadOnly => ("changed", "loaded_existing_handoff"),
    };
    format!(
        "silver-refresh-outcome lane={} outcome={outcome} reason={reason} release={} identity={identity} rows={}",
        lane.id(),
        release.vintage(),
        rows.map_or_else(|| "0".to_owned(), |rows| rows.to_string())
    )
}

/// A one-run lane: its release is in the table when the table records its run identity.
pub(super) fn decide_run(lane: Lane, release: &Release, ingested: &BTreeSet<String>) -> Decision {
    if ingested.contains(&release.run_identity(lane.id())) {
        return Decision::Unchanged("already_loaded");
    }
    let vintage = release.vintage();
    if ingested
        .iter()
        .filter_map(|identity| release::run_identity_month(lane.id(), identity))
        .any(|month| month > vintage)
    {
        return Decision::Unchanged("newer_release_loaded");
    }
    Decision::ExportAndLoad
}

async fn refresh_run(
    contract: &LaneContract,
    release: &Release,
    runtime: &Runtime,
    catalog: &IcebergRestCatalog,
    ingested: &BTreeSet<String>,
    plan: bool,
) -> anyhow::Result<(Decision, String, Option<u64>)> {
    let identity = release.run_identity(contract.lane.id());
    let decision = decide_run(contract.lane, release, ingested);
    if decision != Decision::ExportAndLoad || plan {
        return Ok((decision, identity, None));
    }
    println!(
        "silver-refresh lane={} release={} identity={identity}: exporting",
        contract.lane.id(),
        release.vintage()
    );
    runtime.reset_work()?;
    let work = runtime.work();
    execute::stage(release, &work).await?;
    let environment = execute::export_environment(contract, release, &identity, &work)?;
    execute::export(runtime, contract, &environment)?;
    let summary = execute::spark(runtime, contract, &SparkLoad::local(contract))?;
    let evidence = execute::keep_evidence(runtime, &identity, 0)?;
    println!("silver-refresh evidence={}", evidence.display());
    confirm_recorded(catalog, &contract.table, std::slice::from_ref(&identity)).await?;
    runtime.reset_work()?;
    Ok((decision, identity, summary.persisted_row_count))
}

/// The R2 manifest the hub export publishes after its last part (root ADR-0092).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(super) struct HubManifest {
    pub schema_version: u32,
    pub status: String,
    pub input_object_key: String,
    pub output_object_prefix: String,
    pub vintage: String,
    pub rows_per_part: u64,
    pub rows_read: u64,
    pub rows_emitted: u64,
    pub rejected_rows: u64,
    pub pnu_ok: u64,
    pub pnu_bad: u64,
    pub parts: Vec<HubPart>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(super) struct HubPart {
    pub object_key: String,
    pub rows: u64,
    pub bytes: u64,
}

/// Where the hub export puts a release's manifest.
pub(super) fn manifest_key(prefix: &str, release: &Release) -> anyhow::Result<String> {
    let file = release.object("source")?.file_name();
    let stem = file.strip_suffix(".zip").context("a hub source is a ZIP")?;
    Ok(format!("{prefix}/{stem}/manifest.json"))
}

/// What a manifest must be before its parts are loaded: this release, this prefix, complete, and
/// parts that add up, every part full but the last (root ADR-0092; moved here from
/// `source_handoff_inputs.py`, which no longer plans hub parts).
pub(super) fn validate_manifest(
    manifest: &HubManifest,
    prefix: &str,
    release: &Release,
) -> anyhow::Result<()> {
    let object = release.object("source")?;
    ensure!(
        manifest.schema_version == 1 && manifest.status == "complete",
        "the hub handoff manifest is not complete"
    );
    ensure!(
        manifest.input_object_key == object.object_key
            && manifest.vintage == release.vintage()
            && manifest.output_object_prefix == prefix,
        "the hub handoff manifest belongs to another release or prefix"
    );
    ensure!(
        manifest.rows_read == manifest.rows_emitted + manifest.rejected_rows
            && manifest.pnu_ok + manifest.pnu_bad == manifest.rows_emitted
            && manifest.rows_emitted > 0
            && manifest.rows_per_part > 0
            && manifest.parts.iter().map(|part| part.rows).sum::<u64>() == manifest.rows_emitted,
        "the hub handoff manifest's row accounting disagrees"
    );
    let stem = object
        .file_name()
        .strip_suffix(".zip")
        .context("a hub source is a ZIP")?;
    let base = format!("{prefix}/{stem}/attempt=");
    let mut attempt = None;
    for (index, part) in manifest.parts.iter().enumerate() {
        let rest = part
            .object_key
            .strip_prefix(&base)
            .context("a manifest part lies outside its release's handoff")?;
        let (this_attempt, name) = rest
            .split_once('/')
            .context("a manifest part has no attempt")?;
        ensure!(
            name == format!("part-{:04}.jsonl.gz", index + 1)
                && !this_attempt.is_empty()
                && *attempt.get_or_insert(this_attempt) == this_attempt
                && part.rows > 0
                && part.bytes > 0
                && part.rows <= manifest.rows_per_part
                && (part.rows == manifest.rows_per_part || index + 1 == manifest.parts.len()),
            "the manifest's parts are out of sequence, mixed or empty"
        );
    }
    Ok(())
}

/// The provider month of a recorded part key under `prefix`, from its ZIP's `OPN` date.
pub(super) fn part_month(prefix: &str, key: &str) -> Option<String> {
    let stem = key
        .strip_prefix(prefix)?
        .strip_prefix('/')?
        .split('/')
        .next()?;
    let day = foundation_outbox_publisher::building_register_snapshot::object_date(&format!(
        "{stem}.zip"
    ))
    .ok()?;
    Some(day.format("%Y%m").to_string())
}

/// A part lane: its release is in the table when every part of its manifest is recorded.
pub(super) fn decide_parts(
    prefix: &str,
    release: &Release,
    manifest: Option<&HubManifest>,
    ingested: &BTreeSet<String>,
) -> Decision {
    if let Some(manifest) = manifest {
        return if manifest
            .parts
            .iter()
            .all(|part| ingested.contains(&part.object_key))
        {
            Decision::Unchanged("already_loaded")
        } else {
            Decision::LoadOnly
        };
    }
    let vintage = release.vintage();
    if ingested
        .iter()
        .filter_map(|key| part_month(prefix, key))
        .any(|month| month > vintage)
    {
        return Decision::Unchanged("newer_release_loaded");
    }
    Decision::ExportAndLoad
}

async fn read_manifest(
    storage: &foundation_outbox::R2ObjectStorage,
    key: &str,
) -> anyhow::Result<Option<HubManifest>> {
    if !storage.object_exists(key).await? {
        return Ok(None);
    }
    let bytes = storage.get_object_bytes(key).await?;
    Ok(Some(
        serde_json::from_slice(&bytes).with_context(|| format!("{key} is not a hub manifest"))?,
    ))
}

async fn refresh_parts(
    contract: &LaneContract,
    release: &Release,
    runtime: &Runtime,
    catalog: &IcebergRestCatalog,
    ingested: &BTreeSet<String>,
    plan: bool,
) -> anyhow::Result<(Decision, String, Option<u64>)> {
    let prefix = contract
        .handoff_prefix
        .as_deref()
        .context("a part lane has a handoff prefix")?;
    let key = manifest_key(prefix, release)?;
    let storage = foundation_outbox::R2ObjectStorage::from_env().context("a part lane reads R2")?;
    let mut manifest = read_manifest(&storage, &key).await?;
    if let Some(manifest) = &manifest {
        validate_manifest(manifest, prefix, release)?;
    }
    let decision = decide_parts(prefix, release, manifest.as_ref(), ingested);
    if matches!(decision, Decision::Unchanged(_)) || plan {
        return Ok((decision, key, None));
    }
    runtime.reset_work()?;
    if decision == Decision::ExportAndLoad {
        println!(
            "silver-refresh lane={} release={}: exporting to {key}",
            contract.lane.id(),
            release.vintage()
        );
        let identity = release.run_identity(contract.lane.id());
        let environment =
            execute::export_environment(contract, release, &identity, &runtime.work())?;
        execute::export(runtime, contract, &environment)?;
        let published = read_manifest(&storage, &key)
            .await?
            .context("the hub export finished without publishing its manifest")?;
        validate_manifest(&published, prefix, release)?;
        manifest = Some(published);
    }
    let manifest = manifest.context("a part load has a manifest")?;
    let bucket = super::required_lookup(
        &mut |name: &str| std::env::var(name).ok(),
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET",
    )?;
    let evidence_name = release.run_identity(contract.lane.id());
    let mut rows = 0;
    for (index, batch) in part_batches(&manifest, contract.spark.input_file_batch_size)
        .into_iter()
        .enumerate()
    {
        let load = SparkLoad {
            input: batch
                .iter()
                .map(|part| format!("s3a://{bucket}/{}", part.object_key))
                .collect::<Vec<_>>()
                .join(","),
            input_format: "jsonl",
            expected_count: Some(batch.iter().map(|part| part.rows).sum()),
            reads_r2: true,
        };
        // A batch the table already holds is reported and skipped by append_batch_once.
        let summary = execute::spark(runtime, contract, &load)?;
        execute::keep_evidence(runtime, &evidence_name, index)?;
        rows += summary.row_count;
    }
    let parts: Vec<String> = manifest
        .parts
        .iter()
        .map(|part| part.object_key.clone())
        .collect();
    confirm_recorded(catalog, &contract.table, &parts).await?;
    runtime.reset_work()?;
    Ok((decision, key, Some(rows)))
}

/// The manifest's parts in load batches of `size`, in manifest order (a resumed load regroups
/// them the same way, which `append_batch_once` requires).
pub(super) fn part_batches(manifest: &HubManifest, size: u32) -> Vec<&[HubPart]> {
    manifest
        .parts
        .chunks(usize::try_from(size.max(1)).unwrap_or(1))
        .collect()
}

async fn confirm_recorded(
    catalog: &IcebergRestCatalog,
    table: &str,
    identities: &[String],
) -> anyhow::Result<()> {
    let recorded = catalog
        .load_ingested_batch_objects(table)
        .await?
        .context("the Silver table is missing after its load")?;
    let missing: Vec<&String> = identities
        .iter()
        .filter(|identity| !recorded.contains(*identity))
        .collect();
    ensure!(
        missing.is_empty(),
        "the load finished but {table} records no {missing:?}"
    );
    Ok(())
}
