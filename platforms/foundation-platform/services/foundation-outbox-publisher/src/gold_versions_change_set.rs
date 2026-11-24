//! `export-gold-versions-change-set`: the change set a by-PNU bake reflects between two snapshots of
//! a table of appended row versions (root ADR-0564 §5).
//!
//! The keys that changed between the reflected snapshot and the new one are the keys of the rows
//! whose `version_seq` is above the reflected snapshot's and at most the new one's; each key's
//! newest such row says what it is now: a removal marker is a delete (of a key the reflected
//! snapshot served), anything else an upsert. No two snapshots are compared row by row and no lineage
//! of appends is walked, so a compaction between them changes nothing. It stands in for
//! `by_pnu_panel_delta.py` (Spark) on a lane whose Gold is such a table and writes the same three
//! files: `upserts.txt`, `deletes.txt` and a summary the bake and the publish read
//! (`quality_metrics`, `input`), with its verdict in `verdict`: `delta`, `comparison_gone`
//! (the reflected snapshot is not in `main`'s lineage any more), `not_a_delta` (more than the
//! contract's `max_delta_fraction` changed and nothing recorded explains it) or `explained_full`
//! (the code version moved between the two, the bake states a document schema revision, or the
//! commits between them record an explanation the producer checked while the rest of their keys
//! fit the bound: root ADR-0346, ADR-0596). The bake maps the verdict to the delta job's exit codes.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use anyhow::{ensure, Context};
use foundation_outbox::object_storage::R2ObjectStorageConfig;
use foundation_publisher_silver::{
    duckdb_extensions,
    duckdb_parquet_stage::{with_contract_columns, DuckDbParquetStage, LakehouseRead},
};
use lakehouse_domain::industrial_complex_lakehouse_contracts;
use lakehouse_infrastructure::{
    IcebergRestCatalog, IcebergSnapshotSummary, LakehouseCatalogConfig,
};
use serde_json::json;

use crate::{
    gold_versions_commit::{
        CODE_VERSION_PROPERTY, CURRENT_RECORDS_PROPERTY, KEYS_CHANGED_PROPERTY,
        RECORDED_EXPLANATION_PROPERTY, VERSION_SEQ_PROPERTY,
    },
    public_data_control_support::required_env_value,
};

const PREFIX: &str = "FOUNDATION_PLATFORM_GOLD_VERSIONS_CHANGE_SET_";
/// The R2 contract: the explanation names it accepts.
const CONNECTIONS: &str = include_str!("../../../config/r2-connections.contract.json");
/// The summary's job name.
pub(crate) const JOB_NAME: &str = "gold_versions_change_set";
/// The summary schema the delta job writes (`by_pnu_panel_delta.RUN_SUMMARY_SCHEMA_VERSION`).
const SUMMARY_SCHEMA_VERSION: &str = "foundation-platform.spark_run_summary.v1";

fn setting(name: &str) -> anyhow::Result<String> {
    required_env_value(&format!("{PREFIX}{name}"))
}

/// The newest commit at or before `snapshot` in `main`'s lineage (`history`, newest first) that
/// records the version sequence: its sequence, its code version and its current rows. `None` when
/// the snapshot is not in the lineage.
pub(crate) fn version_at(
    history: &[IcebergSnapshotSummary],
    snapshot: i64,
) -> anyhow::Result<Option<(i64, Option<String>, Option<u64>)>> {
    let Some(position) = history
        .iter()
        .position(|found| found.snapshot_id == snapshot)
    else {
        return Ok(None);
    };
    for commit in &history[position..] {
        if let Some(sequence) = commit.summary.get(VERSION_SEQ_PROPERTY) {
            let current = commit
                .summary
                .get(CURRENT_RECORDS_PROPERTY)
                .map(|text| text.parse::<u64>())
                .transpose()?;
            return Ok(Some((
                sequence.parse()?,
                commit.summary.get(CODE_VERSION_PROPERTY).cloned(),
                current,
            )));
        }
    }
    Ok(Some((0, None, Some(0))))
}

/// The explanations recorded by the commits from snapshot `current` back to, not including, version
/// `before` (in `main`'s lineage, `history` newest first; root ADR-0596), and the keys the commits
/// that recorded none changed: a commit without its keys-changed count counts as every key
/// ([`u64::MAX`]).
pub(crate) fn recorded_explanations(
    history: &[IcebergSnapshotSummary],
    current: i64,
    before: i64,
) -> anyhow::Result<(Vec<String>, u64)> {
    let Some(position) = history
        .iter()
        .position(|found| found.snapshot_id == current)
    else {
        return Ok((Vec::new(), u64::MAX));
    };
    let (mut explained, mut unexplained) = (Vec::new(), 0_u64);
    for commit in &history[position..] {
        let Some(sequence) = commit.summary.get(VERSION_SEQ_PROPERTY) else {
            continue;
        };
        if sequence.parse::<i64>()? <= before {
            break;
        }
        if let Some(why) = commit.summary.get(RECORDED_EXPLANATION_PROPERTY) {
            explained.push(format!("version {sequence}: {why}"));
        } else {
            let keys = commit
                .summary
                .get(KEYS_CHANGED_PROPERTY)
                .map_or(Ok(u64::MAX), |text| text.parse::<u64>())?;
            unexplained = unexplained.saturating_add(keys);
        }
    }
    Ok((explained, unexplained))
}

/// The contract's names of the records that may explain a change set over its bound
/// (`by_pnu_serving_patches.over_max_delta_fraction.explained_by`, root ADR-0346): the one list the
/// Spark delta job reads too.
pub(crate) fn accepted_explanations() -> anyhow::Result<Vec<String>> {
    let contract: serde_json::Value = serde_json::from_str(CONNECTIONS)?;
    contract["by_pnu_serving_patches"]["over_max_delta_fraction"]["explained_by"]
        .as_array()
        .context("the R2 contract names no over_max_delta_fraction.explained_by")?
        .iter()
        .map(|name| {
            name.as_str()
                .map(str::to_owned)
                .context("an explained_by name is not a string")
        })
        .collect()
}

/// What the verdict is, from the counts and whether a record explains them.
pub(crate) fn verdict(
    changes: u64,
    current_total: u64,
    max_delta_fraction: f64,
    explained: bool,
) -> &'static str {
    #[allow(clippy::cast_precision_loss)]
    let over = changes as f64 > current_total as f64 * max_delta_fraction;
    if current_total == 0 || over {
        if !explained {
            "not_a_delta"
        } else {
            "explained_full"
        }
    } else {
        "delta"
    }
}

fn write_list(path: &Path, keys: &BTreeSet<String>) -> anyhow::Result<()> {
    let mut text = String::new();
    for key in keys {
        text.push_str(key);
        text.push('\n');
    }
    std::fs::write(path, text).with_context(|| format!("cannot write {}", path.display()))
}

fn lakehouse_read() -> anyhow::Result<LakehouseRead> {
    let catalog = LakehouseCatalogConfig::from_env()
        .context("failed to configure the lakehouse catalog for the change set")?;
    let storage = R2ObjectStorageConfig::from_env()
        .context("failed to configure lakehouse R2 reads for the change set")?;
    Ok(LakehouseRead {
        extensions: duckdb_extensions::release_directory()?,
        catalog_uri: catalog.catalog_uri,
        warehouse: catalog.warehouse,
        catalog_token: catalog
            .catalog_token
            .context("the lakehouse catalog token is required")?,
        s3_endpoint: storage.endpoint,
        s3_access_key_id: storage.access_key_id,
        s3_secret_access_key: storage.secret_access_key,
    })
}

/// Writes the change set between the two snapshots the environment names.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run() -> anyhow::Result<()> {
    let table = setting("TABLE")?;
    let baseline: i64 = setting("BASELINE_SNAPSHOT_ID")?
        .parse()
        .context("the baseline is an Iceberg snapshot id")?;
    let current: i64 = setting("CURRENT_SNAPSHOT_ID")?
        .parse()
        .context("the current snapshot is an Iceberg snapshot id")?;
    let max_delta_fraction: f64 = setting("MAX_DELTA_FRACTION")?.parse()?;
    ensure!(
        max_delta_fraction.is_finite() && max_delta_fraction > 0.0,
        "max_delta_fraction must be positive"
    );
    let revision = std::env::var(format!("{PREFIX}DOCUMENT_SCHEMA_REVISION")).unwrap_or_default();
    let (upserts_path, deletes_path, summary_path) = (
        setting("UPSERTS")?,
        setting("DELETES")?,
        setting("SUMMARY")?,
    );
    let contract = industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == table)
        .with_context(|| format!("no lakehouse contract for {table}"))?;
    let latest = contract
        .current_row_latest()
        .with_context(|| format!("{table} is no table of appended row versions"))?;
    ensure!(
        latest.key == ["pnu"],
        "{table}'s key is {:?}; the change set names PNUs",
        latest.key
    );
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let history = catalog
        .load_main_snapshot_history(&table)
        .await?
        .with_context(|| format!("{table} does not exist"))?;
    let input = json!({
        "unit": table, "baseline_snapshot_id": baseline.to_string(),
        "current_snapshot_id": current.to_string(), "max_delta_fraction": max_delta_fraction,
        "document_schema_revision": revision,
    });
    let write = |summary: &serde_json::Value| -> anyhow::Result<()> {
        std::fs::write(&summary_path, serde_json::to_vec_pretty(summary)?)
            .with_context(|| format!("cannot write {summary_path}"))
    };
    let Some((before, before_code, _)) = version_at(&history, baseline)? else {
        write(&json!({
            "schema_version": SUMMARY_SCHEMA_VERSION, "job_name": JOB_NAME, "contract": table,
            "verdict": "comparison_gone", "input": input,
        }))?;
        println!("{JOB_NAME} comparison_gone: {table} main no longer holds snapshot {baseline}");
        return Ok(());
    };
    let (after, after_code, after_rows) = version_at(&history, current)?
        .with_context(|| format!("{table} main does not hold snapshot {current}"))?;
    ensure!(
        after >= before,
        "{table} snapshot {current} is at version {after}, before the reflected {before}"
    );
    let current_total = after_rows.with_context(|| {
        format!("{table}'s commit of version {after} records no {CURRENT_RECORDS_PROPERTY}")
    })?;

    let (mut upserts, mut deletes) = (BTreeSet::new(), BTreeSet::new());
    if after > before {
        let work = Path::new(&summary_path)
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let stage = DuckDbParquetStage::open_reading_lakehouse(work, &lakehouse_read()?)?;
        let connection = stage.connection();
        let removal = latest
            .removal_column
            .context("a table of appended row versions names its removal column")?;
        let order = latest.newest_first.join(" DESC, ");
        let source =
            |at: i64| foundation_publisher_silver::duckdb_parquet_stage::table_at(&table, at);
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE moved AS SELECT pnu, {removal} = 'removed' AS removed FROM ( \
               SELECT pnu, {removal}, row_number() OVER (PARTITION BY pnu ORDER BY {order} DESC) AS newest \
               FROM {} WHERE version_seq > {before} AND version_seq <= {after}) WHERE newest = 1;",
            source(current)
        ))?;
        // A key that came and went between the two is no delete: the reflected snapshot never
        // served it.
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE served_before AS {};",
            latest.current_rows_sql(
                &with_contract_columns(connection, contract, &source(baseline))?,
                &["pnu"],
                "pnu IN (SELECT pnu FROM moved)"
            )
        ))?;
        let keys = |sql: &str| -> anyhow::Result<BTreeSet<String>> {
            connection
                .prepare(sql)?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<_, _>>()
                .context("DuckDB failed to read the changed keys")
        };
        upserts = keys("SELECT pnu FROM moved WHERE NOT coalesce(removed, FALSE)")?;
        deletes = keys(
            "SELECT pnu FROM moved WHERE coalesce(removed, FALSE) AND pnu IN (SELECT pnu FROM served_before)",
        )?;
        let new_count: i64 = connection.query_row(
            "SELECT count(*) FROM moved WHERE NOT coalesce(removed, FALSE) \
             AND pnu NOT IN (SELECT pnu FROM served_before)",
            [],
            |row| row.get(0),
        )?;
        drop(stage);
        return finish(
            &table,
            &input,
            (&upserts, &deletes, u64::try_from(new_count)?),
            current_total,
            max_delta_fraction,
            (
                before_code != after_code,
                &revision,
                recorded_explanations(&history, current, before)?,
            ),
            (&upserts_path, &deletes_path, &summary_path),
        );
    }
    finish(
        &table,
        &input,
        (&upserts, &deletes, 0),
        current_total,
        max_delta_fraction,
        (before_code != after_code, &revision, (Vec::new(), 0)),
        (&upserts_path, &deletes_path, &summary_path),
    )
}

fn finish(
    table: &str,
    input: &serde_json::Value,
    (upserts, deletes, new_count): (&BTreeSet<String>, &BTreeSet<String>, u64),
    current_total: u64,
    max_delta_fraction: f64,
    (code_moved, revision, (recorded, unexplained_keys)): (bool, &str, (Vec<String>, u64)),
    (upserts_path, deletes_path, summary_path): (&str, &str, &str),
) -> anyhow::Result<()> {
    let (upsert_count, delete_count) =
        (u64::try_from(upserts.len())?, u64::try_from(deletes.len())?);
    // The Spark delta job's shape (`by_pnu_panel_delta.explanations`): name -> why, only the names
    // the contract accepts, and the names checked.
    let accepted = accepted_explanations()?;
    let mut found = BTreeMap::new();
    if code_moved {
        found.insert(
            "code_version",
            "the code version moved between the two snapshots".to_owned(),
        );
    }
    if !revision.is_empty() {
        found.insert(
            "document_schema",
            format!("the document schema revision {revision}"),
        );
    }
    // A recorded explanation covers its own commit's keys; the commits that record none must fit
    // the bound by themselves, or nothing explains the whole (root ADR-0596).
    #[allow(clippy::cast_precision_loss)]
    let rest_fits = unexplained_keys as f64 <= current_total as f64 * max_delta_fraction;
    if rest_fits && !recorded.is_empty() {
        found.insert("recorded_explanation", recorded.join("; "));
    }
    let explained: BTreeMap<&str, String> = found
        .into_iter()
        .filter(|(name, _)| accepted.iter().any(|known| known == name))
        .collect();
    let verdict = verdict(
        upsert_count + delete_count,
        current_total,
        max_delta_fraction,
        !explained.is_empty(),
    );
    write_list(Path::new(upserts_path), upserts)?;
    write_list(Path::new(deletes_path), deletes)?;
    let changed = upsert_count - new_count;
    let summary = json!({
        "schema_version": SUMMARY_SCHEMA_VERSION, "job_name": JOB_NAME, "contract": table,
        "verdict": verdict, "row_count": upsert_count,
        "counts": {"changed": changed, "new": new_count, "deleted": delete_count,
                   "same": current_total.saturating_sub(changed + new_count)},
        "explained_by": explained, "explanations_checked": accepted,
        "quality_metrics": {
            "changed_count": changed, "new_count": new_count, "deleted_count": delete_count,
            "unchanged_count": current_total.saturating_sub(changed + new_count),
            "current_total": current_total, "upsert_count": upsert_count, "delete_count": delete_count,
            "change_source": "version_seq",
        },
        "input": input,
    });
    std::fs::write(summary_path, serde_json::to_vec_pretty(&summary)?)
        .with_context(|| format!("cannot write {summary_path}"))?;
    println!(
        "{JOB_NAME} {verdict}: {table} upserts={upsert_count} (new {new_count}) deletes={delete_count} \
         of {current_total} current rows"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn snapshot(id: i64, summary: &[(&str, &str)]) -> IcebergSnapshotSummary {
        IcebergSnapshotSummary {
            snapshot_id: id,
            timestamp_ms: None,
            summary: summary
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect::<BTreeMap<_, _>>(),
        }
    }

    #[test]
    fn a_snapshot_s_version_is_the_newest_commit_at_or_before_it_past_compactions(
    ) -> anyhow::Result<()> {
        let history = [
            snapshot(4, &[("operation", "replace")]),
            snapshot(
                3,
                &[(VERSION_SEQ_PROPERTY, "2"), (CURRENT_RECORDS_PROPERTY, "9")],
            ),
            snapshot(2, &[("operation", "replace")]),
            snapshot(
                1,
                &[(VERSION_SEQ_PROPERTY, "1"), (CODE_VERSION_PROPERTY, "c")],
            ),
        ];
        assert_eq!(version_at(&history, 4)?, Some((2, None, Some(9))));
        assert_eq!(
            version_at(&history, 2)?,
            Some((1, Some("c".to_owned()), None))
        );
        assert_eq!(
            version_at(&history, 7)?,
            None,
            "a snapshot main no longer holds"
        );
        Ok(())
    }

    #[test]
    fn a_recorded_explanation_covers_its_commit_and_the_rest_are_counted() -> anyhow::Result<()> {
        // Root ADR-0596: newest first; the reflected snapshot is version 1.
        let history = [
            snapshot(
                5,
                &[(VERSION_SEQ_PROPERTY, "3"), (KEYS_CHANGED_PROPERTY, "4")],
            ),
            snapshot(4, &[("operation", "replace")]),
            snapshot(
                3,
                &[
                    (VERSION_SEQ_PROPERTY, "2"),
                    (KEYS_CHANGED_PROPERTY, "60"),
                    (RECORDED_EXPLANATION_PROPERTY, "store-only: 60 changed keys"),
                ],
            ),
            snapshot(
                1,
                &[(VERSION_SEQ_PROPERTY, "1"), (KEYS_CHANGED_PROPERTY, "100")],
            ),
        ];
        let (explained, rest) = recorded_explanations(&history, 5, 1)?;
        assert_eq!(explained, ["version 2: store-only: 60 changed keys"]);
        assert_eq!(rest, 4);
        assert_eq!(recorded_explanations(&history, 3, 1)?.1, 0);
        assert!(recorded_explanations(&history, 5, 2)?.0.is_empty());
        // A commit that records no count of its keys is not bounded.
        let unknown = [snapshot(2, &[(VERSION_SEQ_PROPERTY, "2")])];
        assert_eq!(recorded_explanations(&unknown, 2, 1)?.1, u64::MAX);
        Ok(())
    }

    #[test]
    fn a_store_only_commit_over_the_bound_bakes_full_and_an_unrecorded_one_is_refused(
    ) -> anyhow::Result<()> {
        // Root ADR-0596: 60 of 100 keys changed, over max_delta_fraction 0.5.
        let dir = tempfile::tempdir()?;
        let path = |name: &str| dir.path().join(name).to_string_lossy().into_owned();
        let (upserts_path, deletes_path, summary_path) = (path("u"), path("d"), path("s"));
        let upserts: BTreeSet<String> = (0..60).map(|key| format!("{key:019}")).collect();
        let verdict_of = |recorded: Vec<String>, rest: u64| -> anyhow::Result<String> {
            finish(
                "gold.parcel_nearby_versions",
                &json!({}),
                (&upserts, &BTreeSet::new(), 0),
                100,
                0.5,
                (false, "", (recorded, rest)),
                (&upserts_path, &deletes_path, &summary_path),
            )?;
            let summary: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&summary_path)?)?;
            // The bake reads the Spark delta job's shape: a map of name -> why, and the names checked.
            assert!(summary["explained_by"].is_object(), "{summary}");
            assert!(summary["explanations_checked"].is_array(), "{summary}");
            if summary["verdict"] == "explained_full" {
                assert!(summary["explained_by"]["recorded_explanation"]
                    .as_str()
                    .is_some_and(|why| why.contains("store-only")));
            }
            Ok(summary["verdict"].as_str().unwrap_or_default().to_owned())
        };
        let store_only = || vec!["version 2: store-only: 60 changed keys".to_owned()];
        assert_eq!(verdict_of(store_only(), 4)?, "explained_full");
        assert_eq!(
            verdict_of(store_only(), 51)?,
            "not_a_delta",
            "the unrecorded rest is over the bound"
        );
        assert_eq!(
            verdict_of(Vec::new(), 0)?,
            "not_a_delta",
            "nothing recorded"
        );
        Ok(())
    }

    #[test]
    fn the_explanations_are_the_contract_s() -> anyhow::Result<()> {
        // Root ADR-0346 §4: one list of names, the Spark delta job's and this one's.
        let accepted = accepted_explanations()?;
        for name in ["code_version", "document_schema", "recorded_explanation"] {
            assert!(
                accepted.iter().any(|found| found == name),
                "{name} not in {accepted:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn a_change_over_the_bound_is_full_only_when_a_record_explains_it() {
        assert_eq!(verdict(5, 100, 0.1, false), "delta");
        assert_eq!(verdict(50, 100, 0.1, false), "not_a_delta");
        assert_eq!(verdict(50, 100, 0.1, true), "explained_full");
        assert_eq!(
            verdict(0, 0, 0.1, false),
            "not_a_delta",
            "an empty table is no delta"
        );
    }
}
