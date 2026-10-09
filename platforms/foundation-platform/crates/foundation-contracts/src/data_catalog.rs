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

/// The latest run of a scheduled job that the data catalog holds.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct ScheduledJobRun {
    /// The run's identifier in the data catalog.
    pub run_urn: String,
    /// When the run started, from its first recorded start.
    pub started_at: Option<DateTime<Utc>>,
    /// When the run completed; absent while it runs.
    pub finished_at: Option<DateTime<Utc>>,
    /// How it stands.
    pub outcome: ScheduledJobRunOutcome,
    /// The scheduler's own word for the result, as it sent it, when it sent one.
    pub native_result: Option<String>,
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
