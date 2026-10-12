//! A dataset's Iceberg snapshots with the run that committed each and the snapshots built from it
//! (root ADR-0195): the run-to-dataset-version view Marquez gives a dataset, read from what the
//! lakehouse already records. Each table's `main` snapshots carry their rows (`added-records`),
//! the source objects a Silver load recorded (`foundation.ingest-batch-objects`) and the input
//! snapshots a Gold build recorded (`foundation.source-iceberg-snapshots`); the run is the
//! writer's run that was going when the snapshot was committed.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::{
    extract::{Query, State},
    Json,
};
use chrono::{DateTime, TimeZone, Utc};
use foundation_contracts::data_catalog::{
    DatasetSnapshot, DatasetSnapshotLink, DatasetSnapshots, ScheduledJobStatus,
};
use foundation_contracts::error::InternalApiErrorResponse;
use lakehouse_infrastructure::IcebergSnapshotSummary;
use serde::Deserialize;
use utoipa::IntoParams;

use super::facts;
use super::health::iceberg_table;
use super::operations::{job_statuses, read_registry};
use crate::routes::api_error::ApiError;
use crate::state::AppState;

/// Which dataset to read.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(super) struct SnapshotsQuery {
    /// The dataset's pipeline-graph node, as a scheduled job's `reads` or `writes` names it.
    node_id: String,
}

/// A dataset's Iceberg snapshots, newest first, with the run that made each and what read it.
#[utoipa::path(
    get,
    path = "/data-catalog/v1/dataset-snapshots",
    operation_id = "listDatasetSnapshots",
    params(SnapshotsQuery),
    responses(
        (status = 200, body = DatasetSnapshots),
        (status = 401, description = "No staff identity"),
        (status = 403, description = "Principal may not read the data catalog"),
        (status = 500, description = "The job list could not be read", body = InternalApiErrorResponse),
    )
)]
pub(super) async fn dataset_snapshots(
    State(_state): State<Arc<AppState>>,
    Query(query): Query<SnapshotsQuery>,
) -> Result<Json<DatasetSnapshots>, ApiError> {
    let (list, dependencies) = read_registry()?;
    let facts = facts::current(Arc::clone(&list), Arc::clone(&dependencies)).await;
    let statuses = job_statuses(
        &list,
        &dependencies,
        facts.flows.as_deref().unwrap_or_default(),
        Utc::now(),
    );
    Ok(Json(snapshots_of(
        &query.node_id,
        &statuses,
        &facts.tables,
        facts.lakehouse_error.as_deref(),
    )))
}

/// The job and run, among `writers`' recent runs, that was going at `at`.
fn run_going_at(writers: &[&ScheduledJobStatus], at: DateTime<Utc>) -> Option<(String, String)> {
    writers.iter().find_map(|status| {
        let run = status.recent_runs.iter().find(|run| {
            run.started_at.is_some_and(|start| start <= at)
                && run.finished_at.is_none_or(|finish| at <= finish)
        })?;
        Some((status.job_id.clone(), run.run_urn.clone()))
    })
}

fn committed_at(snapshot: &IcebergSnapshotSummary) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(snapshot.timestamp_ms?).single()
}

/// `node_id`'s snapshots from `tables`, each with the run of a job writing it that was going when
/// it was committed and the snapshots of other tables that name it as their input.
fn snapshots_of(
    node_id: &str,
    statuses: &[ScheduledJobStatus],
    tables: &BTreeMap<String, Vec<IcebergSnapshotSummary>>,
    lakehouse_error: Option<&str>,
) -> DatasetSnapshots {
    let mut nodes: BTreeMap<&str, &str> = BTreeMap::new();
    for dataset in statuses
        .iter()
        .flat_map(|status| status.reads.iter().chain(&status.writes))
    {
        if let Some(table) = iceberg_table(&dataset.dataset) {
            nodes.insert(table, dataset.node_id.as_str());
        }
    }
    let Some(table) = nodes
        .iter()
        .find(|(_, node)| **node == node_id)
        .map(|(table, _)| *table)
    else {
        return DatasetSnapshots {
            node_id: node_id.to_owned(),
            table: None,
            snapshots: Vec::new(),
            error: None,
        };
    };
    let Some(history) = tables.get(table) else {
        return DatasetSnapshots {
            node_id: node_id.to_owned(),
            table: Some(table.to_owned()),
            snapshots: Vec::new(),
            error: Some(
                lakehouse_error
                    .unwrap_or("the table was not read")
                    .to_owned(),
            ),
        };
    };
    let link = |table: &str, snapshot_id: i64| DatasetSnapshotLink {
        table: table.to_owned(),
        node_id: nodes.get(table).map(|node| (*node).to_owned()),
        snapshot_id: snapshot_id.to_string(),
        committed_at: tables
            .get(table)
            .and_then(|history| {
                history
                    .iter()
                    .find(|snapshot| snapshot.snapshot_id == snapshot_id)
            })
            .and_then(committed_at),
    };
    let writers: Vec<&ScheduledJobStatus> = statuses
        .iter()
        .filter(|status| {
            status
                .writes
                .iter()
                .any(|dataset| iceberg_table(&dataset.dataset) == Some(table))
        })
        .collect();
    let snapshots = history
        .iter()
        .map(|snapshot| {
            let at = committed_at(snapshot);
            let run = at.and_then(|at| run_going_at(&writers, at));
            let read_by = tables
                .iter()
                .filter(|(other, _)| other.as_str() != table)
                .flat_map(|(other, history)| {
                    history
                        .iter()
                        .filter(|later| {
                            later.source_snapshots().get(table) == Some(&snapshot.snapshot_id)
                        })
                        .map(|later| link(other, later.snapshot_id))
                })
                .collect();
            DatasetSnapshot {
                snapshot_id: snapshot.snapshot_id.to_string(),
                committed_at: at,
                operation: snapshot.summary.get("operation").cloned(),
                added_records: snapshot.added_records(),
                total_records: snapshot
                    .summary
                    .get("total-records")
                    .and_then(|value| value.parse().ok()),
                job_id: run.as_ref().map(|(job, _)| job.clone()),
                run_urn: run.map(|(_, urn)| urn),
                ingested_objects: snapshot.ingested_objects(),
                read_from: snapshot
                    .source_snapshots()
                    .into_iter()
                    .map(|(source, id)| link(&source, id))
                    .collect(),
                read_by,
            }
        })
        .collect();
    DatasetSnapshots {
        node_id: node_id.to_owned(),
        table: Some(table.to_owned()),
        snapshots,
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use foundation_contracts::data_catalog::{
        ScheduledJobDataset, ScheduledJobRun, ScheduledJobRunOutcome, ScheduledJobStart,
    };

    fn dataset(table: &str) -> ScheduledJobDataset {
        ScheduledJobDataset {
            dataset: format!("iceberg://{table}"),
            catalog_urn: String::new(),
            node_id: format!("node-{table}"),
            title: String::new(),
            kind: String::new(),
            surface_kind: None,
        }
    }

    fn job(
        id: &str,
        reads: &[&str],
        writes: &[&str],
        runs: Vec<ScheduledJobRun>,
    ) -> ScheduledJobStatus {
        ScheduledJobStatus {
            job_id: id.to_owned(),
            dag_id: id.to_owned(),
            description: String::new(),
            schedule: "0 6 * * *".to_owned(),
            enabled: true,
            disabled_reason: None,
            next_run_at: None,
            catalog_job_urn: None,
            last_run: None,
            typical_duration_seconds: None,
            typical_duration_runs: 0,
            recent_runs: runs,
            started_by: ScheduledJobStart::Schedule,
            upstream_job_ids: Vec::new(),
            pipeline_graph_edges: Vec::new(),
            reads: reads.iter().map(|table| dataset(table)).collect(),
            writes: writes.iter().map(|table| dataset(table)).collect(),
            problems: Vec::new(),
        }
    }

    fn snapshot(id: i64, at_ms: i64, summary: &[(&str, &str)]) -> IcebergSnapshotSummary {
        IcebergSnapshotSummary {
            snapshot_id: id,
            timestamp_ms: Some(at_ms),
            summary: summary
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        }
    }

    #[test]
    fn a_silver_snapshot_names_its_run_and_the_gold_snapshot_built_from_it() {
        let run = ScheduledJobRun {
            run_urn: "urn:run-1".to_owned(),
            started_at: Utc.timestamp_millis_opt(1_000).single(),
            finished_at: Utc.timestamp_millis_opt(5_000).single(),
            outcome: ScheduledJobRunOutcome::Succeeded,
            native_result: None,
            rows_written: None,
        };
        let statuses = vec![
            job("load", &[], &["silver.a"], vec![run]),
            job("build", &["silver.a"], &["gold.b"], Vec::new()),
        ];
        let tables = BTreeMap::from([
            (
                "silver.a".to_owned(),
                vec![
                    snapshot(
                        7,
                        4_000,
                        &[
                            ("added-records", "12"),
                            ("operation", "append"),
                            ("foundation.ingest-batch-objects", "bronze/x.zip"),
                        ],
                    ),
                    snapshot(6, 9_000_000, &[("added-records", "3")]),
                ],
            ),
            (
                "gold.b".to_owned(),
                vec![snapshot(
                    70,
                    6_000,
                    &[(
                        "foundation.source-iceberg-snapshots",
                        "{\"silver.a\":\"7\"}",
                    )],
                )],
            ),
        ]);
        let answer = snapshots_of("node-silver.a", &statuses, &tables, None);
        assert_eq!(answer.table.as_deref(), Some("silver.a"));
        let first = &answer.snapshots[0];
        assert_eq!(first.snapshot_id, "7");
        assert_eq!(
            (first.job_id.as_deref(), first.run_urn.as_deref()),
            (Some("load"), Some("urn:run-1"))
        );
        assert_eq!(first.added_records, Some(12));
        assert_eq!(first.ingested_objects, vec!["bronze/x.zip"]);
        assert_eq!(first.read_by.len(), 1);
        assert_eq!(
            (
                first.read_by[0].table.as_str(),
                first.read_by[0].node_id.as_deref(),
                first.read_by[0].snapshot_id.as_str()
            ),
            ("gold.b", Some("node-gold.b"), "70")
        );
        assert_eq!(
            answer.snapshots[1].job_id, None,
            "committed outside every run"
        );
        assert!(answer.snapshots[1].read_by.is_empty());

        let gold = snapshots_of("node-gold.b", &statuses, &tables, None);
        assert_eq!(gold.snapshots[0].read_from[0].snapshot_id, "7");
        assert_eq!(
            gold.snapshots[0].read_from[0].committed_at,
            Utc.timestamp_millis_opt(4_000).single()
        );

        let unread = snapshots_of("node-gold.b", &statuses, &BTreeMap::new(), Some("down"));
        assert_eq!(unread.error.as_deref(), Some("down"));
        assert_eq!(snapshots_of("tiles", &statuses, &tables, None).table, None);
    }
}
