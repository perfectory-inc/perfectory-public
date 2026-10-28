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
//!
//! The VWorld land lanes (step 3) differ only in step 1 and in what one release is: the newest
//! vintage whose objects, read by their ZIP member names, cover the contract's regions
//! (`land.rs`). Steps 2 and 3 are the same: the table's own record, the lane's existing export,
//! the same Spark load.
use std::collections::BTreeSet;

use anyhow::{ensure, Context};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde::Deserialize;
use sqlx::PgPool;

mod execute;
mod land;
#[cfg(test)]
mod land_tests;
mod lane;
mod release;
#[cfg(test)]
mod tests;

use super::invocation_cleanup::{self, Job};
use execute::{Runtime, SparkLoad};
use lane::{Lane, LaneContract};
use release::Release;

/// The publisher command every lane's contract names as its export. The executable owns the
/// command parser, so it checks that each one is a command the release can start (root ADR-0200).
pub fn export_commands() -> anyhow::Result<Vec<String>> {
    Lane::ALL
        .into_iter()
        .map(|lane| Ok(lane.contract()?.export))
        .collect()
}

const LANE_ENV: &str = "FOUNDATION_PLATFORM_SILVER_REFRESH_LANE";
const STATE_ROOT_ENV: &str = "FOUNDATION_PLATFORM_SILVER_REFRESH_STATE_ROOT";
/// `silver-refresh.sh <lane> --plan`: decide, report, and do nothing else.
const PLAN_ENV: &str = "FOUNDATION_PLATFORM_SILVER_REFRESH_PLAN";
/// `TimeoutStartSec` of `infra/systemd/foundation-silver-refresh@.service` (a test reads the unit).
pub(super) const UNIT_TIME_LIMIT_MINUTES: i64 = 150;
/// No compare bucket starts later than this into a run (root ADR-0318). The largest bucket of the
/// largest compared table, 시도 41 of `silver.land_use_plan` (46,842,275 of 247,584,668 rows,
/// measured 2026-10-28), is about 31 minutes at the 2026-10-21 rate (131,664,668 rows in 87 minutes),
/// so the 40 minutes left hold it and the publish.
pub(super) const LAST_BUCKET_START_MINUTES: i64 = UNIT_TIME_LIMIT_MINUTES - LARGEST_BUCKET_MINUTES;
/// What the deadline leaves for the bucket that starts last and the publish (see above).
const LARGEST_BUCKET_MINUTES: i64 = 40;

/// What the run found, and what it reports on its last line.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Decision {
    /// The table already reflects this release (or a newer one): nothing to do.
    Unchanged(&'static str),
    /// The release has no handoff yet: export it, then load it.
    ExportAndLoad,
    /// A part lane's complete handoff exists in R2 and some of its parts are not in the table.
    LoadOnly,
    /// The release is staged and part of its compare is done; the run's time ran out before the
    /// rest, and `main` is unchanged until the next run finishes it (root ADR-0318).
    CompareInProgress,
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
    let plan = super::optional_bool_lookup(&mut lookup, PLAN_ENV)?.unwrap_or(false);
    let deadline = chrono::Utc::now() + chrono::Duration::minutes(LAST_BUCKET_START_MINUTES);
    if let Some(rule) = &contract.land {
        let measured = land::read_measured(&pool, &rule.source).await;
        pool.close().await;
        let line = refresh_land(&contract, rule, &measured?, &runtime, plan, deadline).await?;
        println!("{}", if plan { as_plan(&line) } else { line });
        return Ok(());
    }
    let ledger = release::read_ledger(&pool, &contract.roles).await;
    pool.close().await;
    let release = release::select(&contract.roles, &ledger?)?;
    for month in &release.skipped_incomplete {
        println!(
            "silver-refresh lane={} skipped_incomplete_release={}",
            lane.id(),
            crate::hub_provider_release::ProviderMonth::containing(*month).vintage()
        );
    }
    let catalog = lakehouse_catalog()?;
    let ingested = catalog
        .load_ingested_batch_objects(&contract.table)
        .await?
        .unwrap_or_default();
    let (decision, identity, rows) = if contract.kind.loads_as_one_run() {
        refresh_run(
            &contract, &release, &runtime, &catalog, &ingested, plan, deadline,
        )
        .await?
    } else {
        refresh_parts(
            &contract, &release, &runtime, &catalog, &ingested, plan, deadline,
        )
        .await?
    };
    let line = outcome_line(lane, &release.vintage(), &decision, &identity, rows);
    println!("{}", if plan { as_plan(&line) } else { line });
    Ok(())
}

fn lakehouse_catalog() -> anyhow::Result<IcebergRestCatalog> {
    IcebergRestCatalog::new(
        LakehouseCatalogConfig::from_env()
            .context("the Silver refresh reads the lakehouse catalog")?,
    )
    .context("cannot build the Iceberg catalog client")
}

/// A land lane's run after its ledger read: the release, the table's record, and the load.
async fn refresh_land(
    contract: &LaneContract,
    rule: &land::LandRule,
    measured: &[land::MeasuredObject],
    runtime: &Runtime,
    plan: bool,
    deadline: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<String> {
    let lane = contract.lane.id();
    let export = crate::land_use_silver_export::export_command(&contract.export)
        .context("a land lane runs a land export")?;
    let candidates = land::candidates(rule, measured, |name| export.converts_member(name))?;
    for refused in &candidates.refused {
        println!("silver-refresh lane={lane} refused_object={refused}");
    }
    let catalog = lakehouse_catalog()?;
    let sido = match rule.completeness {
        land::Completeness::Sido => Some(land::read_sido_calendar(&catalog).await?),
        land::Completeness::National => None,
    };
    let release = land::select(rule, &candidates.found, sido.as_ref())?;
    for skipped in &release.skipped_incomplete {
        println!("silver-refresh lane={lane} skipped_incomplete_release={skipped}");
    }
    for (region, object) in &release.objects {
        println!(
            "silver-refresh lane={lane} release={} region={region} object={}",
            release.vintage, object.object_key
        );
    }
    let identity = release.source_snapshot_id(rule)?;
    let ingested = catalog
        .load_ingested_batch_objects(&contract.table)
        .await?
        .unwrap_or_default();
    let mut decision = land::decide(&release, &candidates.found, &ingested);
    let rows = if decision == Decision::ExportAndLoad && !plan {
        let (rows, published) =
            land::load(contract, rule, &release, runtime, &catalog, deadline).await?;
        if !published {
            decision = Decision::CompareInProgress;
        }
        Some(rows)
    } else {
        None
    };
    Ok(outcome_line(
        contract.lane,
        &release.vintage,
        &decision,
        &identity,
        rows,
    ))
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
    release: &str,
    decision: &Decision,
    identity: &str,
    rows: Option<u64>,
) -> String {
    let (outcome, reason) = match decision {
        Decision::Unchanged(reason) => ("unchanged", *reason),
        Decision::ExportAndLoad => ("changed", "exported_and_loaded"),
        Decision::LoadOnly => ("changed", "loaded_existing_handoff"),
        Decision::CompareInProgress => ("unchanged", "compare_resumes_next_run"),
    };
    format!(
        "silver-refresh-outcome lane={} outcome={outcome} reason={reason} release={release} identity={identity} rows={}",
        lane.id(),
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
    deadline: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<(Decision, String, Option<u64>)> {
    let identity = release.run_identity(contract.lane.id());
    let decision = decide_run(contract.lane, release, ingested);
    if decision != Decision::ExportAndLoad || plan {
        return Ok((decision, identity, None));
    }
    let mut load = SparkLoad::local(contract);
    // A comparing lane stages the release on its load branch and appends only its changes to
    // main (root ADR-0275, ADR-0318); a run that stopped after staging it does not export again.
    let mut staged = false;
    if contract.compare_content.is_some() {
        let branch = stage_load(catalog, &contract.table, &identity).await?;
        staged = catalog
            .load_staged_batch_objects(&contract.table, &branch)
            .await?
            .contains(&identity);
        load.branch = Some(branch);
    }
    let mut rows = None;
    if staged {
        println!(
            "silver-refresh lane={} release={} identity={identity}: already staged; comparing",
            contract.lane.id(),
            release.vintage()
        );
    } else {
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
        let summary = execute::spark(runtime, contract, &load)?;
        let evidence = execute::keep_evidence(runtime, &identity, 0)?;
        println!("silver-refresh evidence={}", evidence.display());
        rows = summary.persisted_row_count;
    }
    let published = match (&contract.compare_content, load.branch) {
        (Some(content), Some(branch)) => {
            let compare = execute::ReleaseCompare {
                branch,
                changes_branch: execute::changes_branch(&identity),
                source_snapshot_id: identity.clone(),
                objects: vec![identity.clone()],
                content_columns: content.clone(),
                bucket_prefix_length: contract.compare_bucket_prefix_length,
                deadline_utc: Some(deadline.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
            };
            append_changes(contract, runtime, catalog, &compare).await?
        }
        _ => {
            confirm_recorded(catalog, &contract.table, std::slice::from_ref(&identity)).await?;
            true
        }
    };
    if !published {
        return Ok((Decision::CompareInProgress, identity, rows));
    }
    runtime.reset_work()?;
    Ok((decision, identity, rows))
}

/// Appends a staged release's changes to `main` instead of publishing its load branch (root
/// ADR-0275): the compare stages them on the changes branch bucket by bucket, and that branch is
/// published as `main` in one fast-forward once every bucket is in (root ADR-0318), so `main` keeps
/// every row it held and a reader never sees half a release. Returns whether `main` holds the
/// release; false when the run's time ran out with buckets left, which the next run compares. A
/// table that had no snapshot when the load began was written on `main` directly, whole; there is
/// nothing to compare.
async fn append_changes(
    contract: &LaneContract,
    runtime: &Runtime,
    catalog: &IcebergRestCatalog,
    compare: &execute::ReleaseCompare,
) -> anyhow::Result<bool> {
    let table = &contract.table;
    let staged = catalog
        .load_snapshot_refs(table)
        .await?
        .is_some_and(|refs| refs.refs.contains_key(&compare.branch));
    if staged {
        // Buckets compared against an older main would be published over it: start them again.
        if catalog
            .remove_stale_branch(table, &compare.changes_branch)
            .await?
        {
            println!(
                "silver-refresh {table}: branch {} no longer follows main; comparing again",
                compare.changes_branch
            );
        }
        let (outcome, summary) = execute::compare(runtime, contract, compare)?;
        println!(
            "silver-refresh {table}: compared the release staged on {}: {summary}",
            compare.branch
        );
        match outcome {
            execute::CompareOutcome::Deadline => return Ok(false),
            execute::CompareOutcome::Staged => {
                let head = catalog
                    .publish_branch(table, &compare.changes_branch)
                    .await?;
                println!(
                    "silver-refresh {table}: published branch {} as main (snapshot {head})",
                    compare.changes_branch
                );
            }
            execute::CompareOutcome::AlreadyAppended => {}
        }
        // main moved past both branches' base, so whatever is left of them is stale now.
        for branch in [&compare.branch, &compare.changes_branch] {
            if catalog.remove_stale_branch(table, branch).await? {
                println!("silver-refresh {table}: dropped branch {branch}");
            }
        }
    }
    confirm_recorded(catalog, table, &compare.objects).await?;
    Ok(true)
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
/// `source_handoff_inputs.py`, the hand planner root ADR-0169 step 3 retired).
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
    crate::hub_provider_release::ProviderMonth::of_file(&format!("{stem}.zip"))
        .ok()
        .map(|month| month.vintage())
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
    deadline: chrono::DateTime<chrono::Utc>,
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
    let branch = stage_load(catalog, &contract.table, &evidence_name).await?;
    let staged = catalog
        .load_staged_batch_objects(&contract.table, &branch)
        .await?;
    let mut rows = 0;
    for (index, batch) in part_batches(&manifest, contract.spark.input_file_batch_size)
        .into_iter()
        .enumerate()
    {
        // A batch the branch already records is not read again: a resumed 40M-row batch spent ten
        // minutes in Spark only to be skipped (2026-10-11). append_batch_once still refuses a
        // second copy of anything this check lets through.
        if batch.iter().all(|part| staged.contains(&part.object_key)) {
            println!(
                "silver-refresh lane={} batch={index}: already staged on {branch}; not read again",
                contract.lane.id()
            );
            rows += batch.iter().map(|part| part.rows).sum::<u64>();
            continue;
        }
        let load = SparkLoad {
            input: batch
                .iter()
                .map(|part| format!("s3a://{bucket}/{}", part.object_key))
                .collect::<Vec<_>>()
                .join(","),
            input_format: "jsonl",
            expected_count: Some(batch.iter().map(|part| part.rows).sum()),
            reads_r2: true,
            write_mode: contract.spark.iceberg_write_mode,
            branch: Some(branch.clone()),
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
    if let Some(content) = &contract.compare_content {
        // The parts are the release; main gets only its changed keys (root ADR-0318).
        let compare = execute::ReleaseCompare {
            branch: branch.clone(),
            changes_branch: execute::changes_branch(&evidence_name),
            source_snapshot_id: evidence_name.clone(),
            objects: parts,
            content_columns: content.clone(),
            bucket_prefix_length: contract.compare_bucket_prefix_length,
            deadline_utc: Some(deadline.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
        };
        if !append_changes(contract, runtime, catalog, &compare).await? {
            return Ok((Decision::CompareInProgress, key, Some(rows)));
        }
    } else {
        publish_load(catalog, &contract.table, &branch, &parts).await?;
    }
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

/// Prepares the branch the batches of load `identity` stage on (root ADR-0183): a branch left by
/// an earlier attempt continues while `main` is still in its lineage, and is removed when `main`
/// moved since, so the batches are staged again on the current `main`.
async fn stage_load(
    catalog: &IcebergRestCatalog,
    table: &str,
    identity: &str,
) -> anyhow::Result<String> {
    let branch = execute::load_branch(identity);
    if catalog.remove_stale_branch(table, &branch).await? {
        println!("silver-refresh {table}: branch {branch} no longer follows main; staging again");
    }
    Ok(branch)
}

/// Publishes the staged batches as `main` in one commit, then confirms `main` records every one
/// (root ADR-0183). A table that had no snapshot when the load began was written on `main`
/// directly (there was no branch to make and no reader); then there is nothing to publish.
async fn publish_load(
    catalog: &IcebergRestCatalog,
    table: &str,
    branch: &str,
    identities: &[String],
) -> anyhow::Result<()> {
    let staged = catalog
        .load_snapshot_refs(table)
        .await?
        .is_some_and(|refs| refs.refs.contains_key(branch));
    if staged {
        let head = catalog.publish_branch(table, branch).await?;
        println!("silver-refresh {table}: published branch {branch} as main (snapshot {head})");
    }
    confirm_recorded(catalog, table, identities).await
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
