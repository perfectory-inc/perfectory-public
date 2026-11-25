//! A whole-table lane of the shared versions build (root ADR-0615, revision 2026-11-26): one
//! partition, keyed by the versions table's current-row key.
//!
//! The run is the shared one without the 시도: the same plan ([`plan`]: the inputs' pins, a shadow's
//! replaced build, the base commit), the stage's one-pass build into `built`, the shared diff over the
//! whole table, the shared result checks and bounds, one file and one commit, the build's tags. A
//! whole-table lane is not rehearsed: it serves no tiles.

use std::{path::Path, time::Instant};

use anyhow::{ensure, Context};
use foundation_publisher_silver::{
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead},
    process_memory,
    whole_table_stage::WholeTableStage,
};
use serde_json::json;

use super::{
    checks::{declared, run_result_checks},
    content_columns, plan, whole_table_bounds, KindBuild, Plan,
};
use crate::{
    gold_versions_commit::{self as versions, Commit, RECORDED_EXPLANATION_PROPERTY},
    gold_versions_support::{contract, max_row_loss_fraction_of, write_summary},
    public_data_control_support::required_env_value,
};

/// The label a whole-table lane's checks report as their part: the table is one.
const WHOLE: &str = "national";

/// Builds and appends `kind`'s changed keys, the table built whole by `stage`.
#[allow(clippy::too_many_lines)] // the run in its order: plan, build, diff, check, commit, report
pub(super) async fn run(
    kind: &KindBuild,
    stage: &'static (dyn WholeTableStage + Sync),
) -> anyhow::Result<()> {
    let table = kind.table;
    let started = Instant::now();
    let summary_path = required_env_value(super::SUMMARY_ENV)?;
    let published_at_utc = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let versions_contract = contract(table)?;
    let content_columns = content_columns(versions_contract);
    let Plan {
        rehearsal,
        catalog,
        committer,
        created,
        history: _,
        base,
        head,
        pins,
        shadow,
        ..
    } = plan(table, kind.lane, &stage.inputs(), String::new()).await?;
    ensure!(
        rehearsal.is_none(),
        "{table} is built whole and serves no tiles: there is no rehearsal of it"
    );
    let committer = committer.context("a run that is no rehearsal commits")?;
    let shadow_build = shadow
        .as_ref()
        .map(|shadow| json!({"table": shadow.replaced, "build": shadow.build}));
    let code = stage.code_version();
    if let Some(base) = &base {
        versions::pin(&catalog, table, base.snapshot_id, &base.pins).await?;
    }
    let moved = base
        .as_ref()
        .map_or_else(std::collections::BTreeSet::new, |base| {
            versions::moved(base, &pins)
        });
    if base
        .as_ref()
        .is_some_and(|base| moved.is_empty() && base.code_version == code)
    {
        write_summary(
            &summary_path,
            &json!({
                "job": kind.job, "engine": "duckdb", "table": table, "outcome": "unchanged",
                "pins": pins, "shadow_of": shadow_build,
                "seconds": started.elapsed().as_secs_f64(),
            }),
        )?;
        println!("gold-versions {table} nothing to do: its inputs and code are the base commit's");
        return Ok(());
    }
    let declared = declared(kind)?;
    let max_loss = max_row_loss_fraction_of(kind.replaced)?;

    // Compute: the whole table in one pass.
    // Asked before the DuckDB session opens: the session is not `Send` across an await.
    let shape = committer.table_shape(table).await?;
    let work = Path::new(&summary_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let duck = DuckDbParquetStage::open_reading_lakehouse(work, &LakehouseRead::from_env()?)?;
    // The open checked the disk the job declares (root ADR-0622).
    let memory_limit = duck.memory_limit_bytes()?;
    let connection = duck.connection();
    let built = stage.build(connection, &pins, &published_at_utc)?;
    let version_seq = base.as_ref().map_or(1, |base| base.version_seq + 1);
    let commit = Commit {
        table,
        versions: versions_contract,
        content_columns,
        head,
        base: base.clone(),
        version_seq,
        source_snapshot_id: built.source_snapshot_id.clone(),
        published_at_utc: published_at_utc.clone(),
    };
    let run = versions::run_id(table, &pins, &code, "");
    let key = commit.key()?.join(", ");
    let diff = versions::diff(connection, &commit, "TRUE")?;
    let mut checks = Vec::new();
    let refused = run_result_checks(
        &stage.result_checks(),
        connection,
        (table, WHOLE),
        &diff,
        &declared,
        &mut checks,
    )?;
    ensure!(
        refused.is_empty(),
        "{table} breaks the blocking checks {refused:?}: nothing was appended"
    );
    let digests = versions::digests(connection, &commit)?;
    // One file: the table has no partition, and its key names no 시도.
    let files = if diff.keys() > 0 {
        vec![duck.write(
            &shape,
            &format!("SELECT * EXCLUDE (change) FROM appended ORDER BY {key}"),
            &run,
        )?]
    } else {
        Vec::new()
    };
    let (fingerprint, current_records) = versions::after_digests(&commit, &diff, digests)?;
    let recorded_explanation = whole_table_bounds(
        table,
        base.as_ref(),
        (current_records, diff.keys()),
        max_loss,
        || {
            Ok(base
                .as_ref()
                .filter(|base| base.code_version != code)
                .map(|_| format!("the code version moved to {code}")))
        },
    )?;

    // Commit.
    let built_from = versions::BuiltFrom {
        pins: pins.clone(),
        code_version: code.clone(),
        served_edition: String::new(),
    };
    let mut properties = super::commit_properties(
        &commit,
        (fingerprint, current_records),
        &built_from,
        diff.keys(),
        None,
    )?;
    if let Some(why) = &recorded_explanation {
        properties.insert(RECORDED_EXPLANATION_PROPERTY.to_owned(), why.clone());
    }
    let snapshot =
        versions::append(&committer, &catalog, &commit, &files, &run, properties).await?;
    drop(duck);
    let tags = versions::pin(&catalog, table, snapshot, &pins).await?;
    write_summary(
        &summary_path,
        &json!({
            "job": kind.job, "engine": "duckdb", "table": table, "outcome": "committed",
            "created_table": created, "snapshot_id": snapshot, "parent_snapshot_id": head,
            "version_seq": version_seq, "run": run, "pins": pins, "moved": moved,
            "shadow_of": shadow_build, "code_version": code, "rows": built.rows,
            "recorded_explanation": recorded_explanation, "diff": diff, "checks": checks,
            "current_records": current_records,
            "content_fingerprint": versions::fingerprint_text(fingerprint),
            "files": files.len(), "tags": tags, "duckdb_memory_limit_bytes": memory_limit,
            "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
            "seconds": started.elapsed().as_secs_f64(),
        }),
    )?;
    println!(
        "committed {table}: snapshot {snapshot} version_seq={version_seq} keys_changed={} \
         (added {}, changed {}, removed {}) current_records={current_records} in {:.0}s",
        diff.keys(),
        diff.added,
        diff.changed,
        diff.removed,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}
