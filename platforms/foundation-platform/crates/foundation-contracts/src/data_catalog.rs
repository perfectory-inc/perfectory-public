//! Wire contract of the staff data catalog (root ADR-0117, ADR-0119).
//!
//! Staff read the data catalog through Foundation; the catalog's own store (`DataHub`) is behind
//! this API and never reached by a browser. The staff console (Dawneer) uses these types by path.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// What kind of thing a catalog entity is.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DataCatalogEntityKind {
    /// A table, file set or other body of data.
    Dataset,
    /// A job that reads datasets and writes others.
    Job,
}

/// One entity as a list, a search result or a graph node shows it.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DataCatalogEntity {
    /// The catalog's identifier; pass it back unchanged to read the entity or its neighbours.
    pub urn: String,
    /// Dataset or job.
    pub kind: DataCatalogEntityKind,
    /// The name people know it by.
    pub name: String,
    /// The system it lives in, when the catalog records one.
    pub platform: Option<String>,
    /// What it is, in the words its owner wrote.
    pub description: Option<String>,
}

/// One page of search results.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DataCatalogSearchPage {
    /// Entities the query matches in total, not just on this page.
    pub total: u32,
    /// Zero-based offset of the first result on this page.
    pub start: u32,
    /// Results on this page, best match first.
    pub results: Vec<DataCatalogEntity>,
}

/// One column of a dataset.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DataCatalogField {
    /// Column path.
    pub path: String,
    /// Type as the catalog records it.
    pub native_type: Option<String>,
    /// What the column holds.
    pub description: Option<String>,
}

/// An entity and the entities one step before and after it.
///
/// A dataset's neighbours are the jobs that write and read it; a job's neighbours are the datasets
/// it reads and writes. A screen walks the graph by asking for a neighbour's neighbours.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DataCatalogNeighbourhood {
    /// The entity asked about.
    pub entity: DataCatalogEntity,
    /// Its columns, for a dataset whose schema the catalog holds.
    pub fields: Vec<DataCatalogField>,
    /// What it comes from.
    pub upstream: Vec<DataCatalogEntity>,
    /// What comes from it.
    pub downstream: Vec<DataCatalogEntity>,
}

/// How a scheduled job's run stands, in the run record the scheduler sent to the data catalog
/// (`OpenLineage`, root ADR-0118 §2): started and not yet complete, or complete with a result.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ScheduledJobRunOutcome {
    /// Started; no completion recorded yet.
    Running,
    /// Completed, and the scheduler called it a success.
    Succeeded,
    /// Completed, and the scheduler called it a failure.
    Failed,
    /// The scheduler skipped it.
    Skipped,
    /// Failed and waiting for the scheduler's next try.
    UpForRetry,
    /// Completed with a result the catalog does not name.
    Unknown,
}

/// One run of a scheduled job that the data catalog holds.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct ScheduledJobRun {
    /// The run's identifier in the data catalog.
    pub run_urn: String,
    /// When the scheduler began the run. The job waited from here for a slot of its pool until
    /// `started_at`; absent when the catalog holds only the job's own task.
    #[serde(default)]
    pub queued_at: Option<DateTime<Utc>>,
    /// When the job's own work started: its run task's first start, after any wait for a pool slot.
    /// Absent while the run still waits. How long a run took, and whether it is slow, count from here.
    pub started_at: Option<DateTime<Utc>>,
    /// When the run completed; absent while it runs.
    pub finished_at: Option<DateTime<Utc>>,
    /// How it stands.
    pub outcome: ScheduledJobRunOutcome,
    /// The scheduler's own word for the result, as it sent it, when it sent one.
    pub native_result: Option<String>,
    /// Rows the run added to the Iceberg tables it writes: the `added-records` of the `main`
    /// snapshots committed to them while it ran. Absent while it runs, for a job that writes no
    /// Iceberg table, and when the lakehouse catalog could not be read.
    #[serde(default)]
    pub rows_written: Option<u64>,
}

/// What a failed run's log says broke it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FailureCause {
    /// The kernel or the JVM ran out of memory.
    OutOfMemory,
    /// It ran past its time limit.
    TimedOut,
    /// A file, object or table it reads was not there.
    SourceMissing,
    /// Anything else; the cause line says what.
    Other,
}

/// Something about a scheduled job a person should look at (root ADR-0195).
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScheduledJobProblem {
    /// Its latest run failed.
    Failed {
        /// The cause, classified from the run's log; absent when the log could not be read.
        cause: Option<FailureCause>,
        /// The line of the log that says it, with credentials masked.
        cause_line: Option<String>,
        /// The scheduler's id of the failed run, for its log.
        scheduler_run_id: Option<String>,
    },
    /// A job it waits on failed or is itself blocked, so its inputs will not come.
    Blocked {
        /// The jobs it waits on that failed or are blocked, in job-list order.
        upstream_job_ids: Vec<String>,
    },
    /// Its schedule started it at `due_at` and gave it its time limit, and no run has succeeded
    /// since.
    Late {
        /// The scheduled start a success is missing for.
        due_at: DateTime<Utc>,
        /// When it last succeeded, among the runs the catalog holds.
        last_success_at: Option<DateTime<Utc>>,
    },
    /// Its latest run took, or is taking, much longer than it usually does.
    Slow {
        /// How long it took, or has run so far.
        seconds: u64,
        /// How long it usually takes (`typical_duration_seconds`).
        typical_seconds: u64,
        /// Whether it is still running.
        running: bool,
    },
    /// Its latest run wrote far fewer or far more rows to a table than its recent runs did.
    Volume {
        /// The table's pipeline-graph node.
        node_id: String,
        /// Rows the latest run added to it.
        rows: u64,
        /// The median of what the recent runs before it added.
        typical_rows: u64,
    },
    /// Its latest run ended waiting for something it named, not failed (root ADR-0260).
    Waiting {
        /// What it waits for, as its run said; absent when the log could not be read.
        reason: Option<String>,
        /// The scheduler's id of that run, for its log.
        scheduler_run_id: Option<String>,
    },
}

/// Where staff open the scheduler's and the data catalog's own screens, when configured.
#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct OperationsLinks {
    /// The scheduler's (Airflow's) web address.
    pub scheduler_url: Option<String>,
    /// The data catalog's (`DataHub`'s) web address.
    pub catalog_url: Option<String>,
}

/// One table snapshot another snapshot read or was read by.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DatasetSnapshotLink {
    /// The table, `namespace.table`.
    pub table: String,
    /// The table's pipeline-graph node, when a scheduled job reads or writes it.
    pub node_id: Option<String>,
    /// The snapshot's id, as text (it does not fit a JSON number).
    pub snapshot_id: String,
    /// When it was committed, when the table's metadata still holds it.
    pub committed_at: Option<DateTime<Utc>>,
}

/// One `main` snapshot of an Iceberg table.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DatasetSnapshot {
    /// The snapshot's id, as text (it does not fit a JSON number).
    pub snapshot_id: String,
    /// When it was committed.
    pub committed_at: Option<DateTime<Utc>>,
    /// Iceberg's word for the commit (`append`, `overwrite`, ...).
    pub operation: Option<String>,
    /// Rows it added.
    pub added_records: Option<u64>,
    /// Rows the table held after it.
    pub total_records: Option<u64>,
    /// The scheduled job whose run was going when it was committed.
    pub job_id: Option<String>,
    /// That run, in the data catalog.
    pub run_urn: Option<String>,
    /// The source objects the commit recorded as loaded (`foundation.ingest-batch-objects`).
    pub ingested_objects: Vec<String>,
    /// The input snapshots it was built from (`foundation.source-iceberg-snapshots`).
    pub read_from: Vec<DatasetSnapshotLink>,
    /// The snapshots of other tables built from it.
    pub read_by: Vec<DatasetSnapshotLink>,
}

/// A dataset's Iceberg snapshots, newest first.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DatasetSnapshots {
    /// The pipeline-graph node asked about.
    pub node_id: String,
    /// Its Iceberg table, when it is one.
    pub table: Option<String>,
    /// Its `main` snapshots the metadata still holds, newest first.
    pub snapshots: Vec<DatasetSnapshot>,
    /// Why the snapshots could not be read, when they could not.
    pub error: Option<String>,
}

/// One live hostname as the host in Korea reaches it (root ADR-0209).
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct EdgeHostCheck {
    /// The hostname, from the gateways' contract.
    pub hostname: String,
    /// The Cloudflare data centre most answers came from (`ICN` is Seoul).
    pub colo: Option<String>,
    /// The median answer time of `/cdn-cgi/trace`, each on a new connection.
    pub p50_ms: Option<u64>,
    /// The 95th percentile of the same.
    pub p95_ms: Option<u64>,
    /// Answers measured.
    pub samples: u32,
    /// Asks that failed.
    pub failed_samples: u32,
    /// Why nothing could be measured, when nothing could.
    pub error: Option<String>,
}

/// How the live hostnames are reached from Korea.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct EdgeCheck {
    /// When it was measured.
    pub checked_at: DateTime<Utc>,
    /// The data centre `cloudflare.com` itself answered from at the same time: where Korea should
    /// enter.
    pub reference_colo: Option<String>,
    /// Every live catalog, buildings and tiles hostname.
    pub hosts: Vec<EdgeHostCheck>,
}

/// One full UTC week of pull request CI wall time (root ADR-0203).
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct CiWeek {
    /// Its Monday, `YYYY-MM-DD`.
    pub week_start: String,
    /// Pull request attempts measured.
    pub count: u32,
    /// Median attempt, minutes.
    pub p50: Option<f64>,
    /// 90th percentile attempt, minutes.
    pub p90: Option<f64>,
    /// Longest attempt, minutes.
    pub max: Option<f64>,
    /// The workflow with the longest median.
    pub worst_workflow: Option<String>,
    /// That workflow's median, minutes.
    pub worst_workflow_p50: Option<f64>,
}

/// The weekly pull request CI wall-time trend against its one budget.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct CiTimeTrend {
    /// When it was last measured or tried.
    pub attempted_at: DateTime<Utc>,
    /// `tools/ci/ci-time-budget.contract.json` `pull_request_wall_minutes`.
    pub budget_minutes: Option<f64>,
    /// Oldest first.
    pub weeks: Vec<CiWeek>,
    /// Days of the trend's weeks whose runs are not fetched yet: those weeks are left out.
    pub pending_days: Option<u32>,
    /// Why the last fetch failed; the weeks are the ones the kept days give.
    pub error: Option<String>,
}

/// The host's service checks (root ADR-0209): the edge path from Korea and the CI time trend.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct ServiceChecks {
    /// When the host last ran them.
    pub checked_at: Option<DateTime<Utc>>,
    /// The edge part.
    pub edge: Option<EdgeCheck>,
    /// The CI part.
    pub ci: Option<CiTimeTrend>,
    /// Why the checks' file could not be read, when it could not.
    pub error: Option<String>,
}

/// What starts a scheduled job (the job list's `started_by`, root ADR-0171).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ScheduledJobStart {
    /// Its schedule.
    Schedule,
    /// A run of a job in `upstream_job_ids` that changed one of its inputs, and its schedule as
    /// the fallback.
    Inputs,
}

/// One dataset a scheduled job reads or writes: an endpoint of a pipeline-graph edge the job
/// carries out.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct ScheduledJobDataset {
    /// The dataset as the scheduler and the data catalog name it (`<platform>://<name>`).
    pub dataset: String,
    /// Its entity in the data catalog (`GET /data-catalog/v1/entity?urn=`).
    pub catalog_urn: String,
    /// Its node in the pipeline graph (`GET /catalog/v1/pipeline-graph`).
    pub node_id: String,
    /// The node's title there, in Korean.
    pub title: String,
    /// The node's kind there (for example `source_group`, `silver_table`, `gold_table`,
    /// `serving_surface`), as the pipeline graph names it.
    pub kind: String,
    /// For a serving surface, what kind (for example `tiles` or `r2_baked_documents`).
    pub surface_kind: Option<String>,
}

/// One scheduled job: what the job list says about it and its latest run.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct ScheduledJobStatus {
    /// The job's id in the job list (`orchestration/jobs.v1.json`).
    pub job_id: String,
    /// The scheduler's name for the job (its Airflow DAG).
    pub dag_id: String,
    /// What the job does, as the job list says.
    pub description: String,
    /// When it runs: the job list's cron schedule, in UTC.
    pub schedule: String,
    /// Whether the scheduler runs it.
    pub enabled: bool,
    /// Why it is off and what turns it on, for a job that is off.
    pub disabled_reason: Option<String>,
    /// The next time its schedule starts it; absent for a job that is off.
    pub next_run_at: Option<DateTime<Utc>>,
    /// The job's entity in the data catalog that holds the latest run, once a run reported.
    pub catalog_job_urn: Option<String>,
    /// Its latest run the data catalog holds; absent before the first.
    pub last_run: Option<ScheduledJobRun>,
    /// How long the job usually takes, in whole seconds: the median of `finished_at - started_at`
    /// over the recent succeeded runs the data catalog holds for the catalog job of `last_run`.
    /// Absent when none of those runs succeeded with both times recorded.
    pub typical_duration_seconds: Option<u64>,
    /// How many succeeded runs `typical_duration_seconds` is the median of; 0 when it is absent.
    pub typical_duration_runs: u32,
    /// The recent runs of the catalog job of `last_run`, newest first, at most ten; `last_run` is
    /// the first.
    pub recent_runs: Vec<ScheduledJobRun>,
    /// What starts it.
    pub started_by: ScheduledJobStart,
    /// The other jobs that write one of the datasets it reads, in job-list order: the jobs it waits
    /// on (root ADR-0171, ADR-0188).
    pub upstream_job_ids: Vec<String>,
    /// The pipeline-graph edges it carries out, as the job list names them; empty for a job that
    /// only checks the data-contract tables.
    pub pipeline_graph_edges: Vec<String>,
    /// The datasets it reads.
    pub reads: Vec<ScheduledJobDataset>,
    /// The datasets it writes.
    pub writes: Vec<ScheduledJobDataset>,
    /// What about it a person should look at, worst first; empty when it is fine (root ADR-0195).
    #[serde(default)]
    pub problems: Vec<ScheduledJobProblem>,
}

/// Every scheduled job and its latest run.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct ScheduledJobsStatus {
    /// When Foundation read this.
    pub read_at: DateTime<Utc>,
    /// Every job in the job list, in its order.
    pub jobs: Vec<ScheduledJobStatus>,
    /// Why the runs could not be read, when the data catalog did not answer. The jobs are still
    /// listed, and then no job's missing run means it never ran.
    pub run_history_error: Option<String>,
    /// Why the tables' snapshots could not be read, when the lakehouse catalog did not answer or
    /// is not configured: no run then has `rows_written` and no job a volume problem.
    #[serde(default)]
    pub lakehouse_error: Option<String>,
    /// Why failed runs' logs could not be read, when the scheduler did not answer or is not
    /// configured: a failed problem then has no cause.
    #[serde(default)]
    pub scheduler_error: Option<String>,
    /// Where the scheduler's and the catalog's own screens are.
    #[serde(default)]
    pub links: OperationsLinks,
}

/// The latest result of a data-quality check.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DataQualityResult {
    /// The rule held.
    Success,
    /// The rule did not hold.
    Failure,
    /// The check could not be evaluated.
    Error,
    /// No result has been reported for this check.
    NotRun,
}

/// One key and value the quality job reported with a result.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DataQualityDetail {
    /// The key, as the job wrote it.
    pub key: String,
    /// The value, as the job wrote it.
    pub value: String,
}

/// One check a data contract defines and its latest result.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DataQualityCheck {
    /// The check's identifier in the data catalog.
    pub assertion_urn: String,
    /// The catalog's kind for it (for example `FIELD` or `DATA_SCHEMA`), as the catalog names it.
    pub kind: String,
    /// The column it checks, for a column check.
    pub field: Option<String>,
    /// Its latest result.
    pub result: DataQualityResult,
    /// When that result was reported.
    pub checked_at: Option<DateTime<Utc>>,
    /// What the job reported with it (counts, missing columns), as it wrote them.
    pub details: Vec<DataQualityDetail>,
}

/// One data contract and the latest result of each of its checks.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DataContractQuality {
    /// The contract's id (the dataset it promises).
    pub contract_id: String,
    /// The contract's entity in the data catalog.
    pub dataset_urn: String,
    /// Its checks.
    pub checks: Vec<DataQualityCheck>,
}

/// Every data contract the data catalog holds, with its checks' latest results.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DataQualityStatus {
    /// When Foundation read this.
    pub read_at: DateTime<Utc>,
    /// The contracts, by id.
    pub contracts: Vec<DataContractQuality>,
}

/// An operator switch of one scheduled job (root ADR-0282): `pause`, `resume`, `run_now` or `stop`.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct OperatorActionRequest {
    /// `pause` (no new run until resumed), `resume`, `run_now` (once, through the same queue as a
    /// scheduled start; expires in 10 minutes if not started) or `stop` (end the run in progress).
    pub action: String,
    /// Why, in one line of 1 to 300 characters; kept in the append-only ledger.
    pub reason: String,
    /// A pause only: also drop the pending runs.
    #[serde(default)]
    pub drop_pending: bool,
    /// A `set_window` only: the window's start, `HH:MM` KST.
    #[serde(default)]
    pub window_start: Option<String>,
    /// A `set_window` only: the window's end, `HH:MM` KST (across midnight when it comes first; the
    /// whole day when it equals the start).
    #[serde(default)]
    pub window_end: Option<String>,
    /// A `set_window` only: how many hours the override lasts, 1 to 168; then the contract's window
    /// is back.
    #[serde(default)]
    pub window_hours: Option<i64>,
}

/// The ledger's record of an operator request.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct OperatorActionRecorded {
    /// The request's id; a `run_now` is consumed by it.
    pub id: String,
    /// The job's id.
    pub job_id: String,
    /// What was asked.
    pub action: String,
    /// Whether a pause also drops the pending runs.
    pub drop_pending: bool,
    /// The staff member who asked.
    pub actor: String,
    /// When it was asked.
    pub requested_at: DateTime<Utc>,
    /// When a `run_now` stops being consumable or a `set_window` ends; absent for the other actions.
    pub expires_at: Option<DateTime<Utc>>,
    /// A `set_window`'s start, `HH:MM` KST.
    pub window_start: Option<String>,
    /// A `set_window`'s end, `HH:MM` KST.
    pub window_end: Option<String>,
}
