//! The one writer of the gzip JSON Lines Bronze objects a collector builds (root ADR-0616 §3-1).
//!
//! A collector hands [`JsonLinesWriter::push_group`] its lines a group at a time: one property's
//! 물건상세, one notice, one whole search. The writer gzips each line as it comes, never splits a
//! group, and closes the object it is filling at the first group boundary once the contract's
//! `close_after_seconds` have passed since that object's first group, as Kinesis Data Firehose's
//! buffering interval and the Kafka Connect S3 sink's `rotate.interval.ms` do. Each object closes
//! through the one commit seam ([`BronzeCommitter::commit`]) in an ingestion run of its own, so a
//! run killed part way keeps every object it closed; a lane's stop and error paths still call
//! [`JsonLinesWriter::finish`], so what was read is committed. A dry run writes each object under
//! `objects/` of its output directory instead.
//!
//! The lane says only what is its own ([`LineObjects`]): its source, the name of its object number
//! `n` (the lane's key form), whether the key carries the checksum (root ADR-0152), and what it
//! adds to the request parameters as an object closes. Everything else — the gzip, the plan, the
//! run row, the commit, the checksum check, the ledger row, the local copy — is here, once.

use std::io::Write as _;
use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context};
use chrono::{DateTime, Utc};
use collection_application::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};
use collection_application::{
    plan_public_data_bulk_file, BronzeCommitter, BronzePayload, PlannedBronzeObject,
    PublicDataBulkFileIdentity, PublicDataBulkFilePlan, PublicDataBulkFilePlanInput,
};
use collection_domain::{
    build_bronze_content_object_key, BronzeObject, IngestionRun, IngestionRunStatus,
    IngestionTrigger, SourceCatalogEntry,
};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use uuid::Uuid;

use crate::bronze_object_storage::BronzeObjectStorageWriter;

/// The writer's contract: when it closes an object.
pub const CONTRACT_JSON: &str =
    include_str!("../../../../infra/lakehouse/contracts/bronze-line-objects.contract.json");

const BRONZE_COMMITTER: BronzeCommitter = BronzeCommitter::new();
const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";
const CONTENT_TYPE: &str = "application/gzip";

#[derive(Debug, Deserialize)]
struct Contract {
    close_after_seconds: u64,
}

/// The contract's `close_after_seconds`: how long an object stays open after its first group.
///
/// # Errors
/// Refuses a contract that does not parse or names no positive interval.
pub fn close_after_seconds() -> anyhow::Result<u64> {
    static SECONDS: OnceLock<Result<u64, String>> = OnceLock::new();
    SECONDS
        .get_or_init(|| {
            let contract: Contract = serde_json::from_str(CONTRACT_JSON)
                .map_err(|error| format!("bronze-line-objects.contract.json: {error}"))?;
            if contract.close_after_seconds == 0 {
                return Err(
                    "bronze-line-objects.contract.json names no close_after_seconds".into(),
                );
            }
            Ok(contract.close_after_seconds)
        })
        .clone()
        .map_err(anyhow::Error::msg)
}

/// What one lane says about the line objects of one run.
pub struct LineObjects<'a> {
    /// The source the objects belong to; upserted with each object.
    pub source: SourceCatalogEntry,
    /// The provider operation the lines answer.
    pub operation: &'a str,
    /// When the run started: every object's `collected_at`, ingest date and snapshot date.
    pub collected_at: DateTime<Utc>,
    /// The provider file id of the run's object number `n` (0 first): the lane's key form. The
    /// object's file name is this id with `.gz`.
    pub file_id: Box<dyn Fn(u32) -> String + Send + Sync + 'a>,
    /// Whether the key carries the object's checksum, so the same lines again are the same object
    /// (root ADR-0152).
    pub content_qualified: bool,
    /// What the lane adds to the run's and the object's request parameters as an object closes
    /// (an object, or `null` for nothing).
    pub params: Box<dyn Fn() -> JsonValue + Send + Sync + 'a>,
}

/// Where the objects go.
pub enum Target<'a, Uow: ?Sized, Storage: ?Sized> {
    /// Committed to Bronze and recorded in the ledger.
    Live { uow: &'a Uow, storage: &'a Storage },
    /// Planned and written under `objects/` of `output_dir` only.
    DryRun { output_dir: &'a Path },
}

impl<'a, Uow: ?Sized, Storage: ?Sized> Target<'a, Uow, Storage> {
    /// A live run's ledger and store, else a dry run into `output_dir`.
    pub fn of(live: Option<(&'a Uow, &'a Storage)>, output_dir: &'a Path) -> Self {
        match live {
            Some((uow, storage)) => Self::Live { uow, storage },
            None => Self::DryRun { output_dir },
        }
    }
}

/// One object the writer closed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineObject {
    pub object_key: String,
    pub checksum_sha256: String,
    pub size_bytes: u64,
    pub lines: usize,
    /// Where a dry run wrote it, relative to its output directory.
    pub local_path: Option<String>,
    /// Committed to Bronze (a live run).
    pub written: bool,
    /// Recorded by this run: false when Bronze already held it (a retry of the run, or a
    /// content-qualified key whose same bytes landed before).
    pub new: bool,
}

/// The object being filled.
struct Open {
    gzip: flate2::write::GzEncoder<Vec<u8>>,
    lines: usize,
    since: Instant,
}

/// Writes one lane's JSON Lines objects of one run.
pub struct JsonLinesWriter<'a, Uow: ?Sized, Storage: ?Sized> {
    lane: LineObjects<'a>,
    target: Target<'a, Uow, Storage>,
    close_after: Duration,
    open: Option<Open>,
    closed: Vec<LineObject>,
}

impl<'a, Uow, Storage> JsonLinesWriter<'a, Uow, Storage>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    /// A writer for `lane`'s objects of one run, closing them as the contract says.
    ///
    /// # Errors
    /// Refuses a contract that does not parse.
    pub fn new(lane: LineObjects<'a>, target: Target<'a, Uow, Storage>) -> anyhow::Result<Self> {
        Ok(Self::closing_after(
            lane,
            target,
            Duration::from_secs(close_after_seconds()?),
        ))
    }

    fn closing_after(
        lane: LineObjects<'a>,
        target: Target<'a, Uow, Storage>,
        close_after: Duration,
    ) -> Self {
        Self {
            lane,
            target,
            close_after,
            open: None,
            closed: Vec::new(),
        }
    }

    /// Adds one group of lines to the object being filled, whole, and closes that object when
    /// `close_after_seconds` have passed since its first group.
    ///
    /// # Errors
    /// Refuses a line that holds a newline (it would land as two lines), and fails when the object
    /// that closes cannot be committed.
    pub async fn push_group<L: AsRef<str>>(&mut self, lines: &[L]) -> anyhow::Result<()> {
        if lines.is_empty() {
            return Ok(());
        }
        let open = self.open.get_or_insert_with(|| Open {
            gzip: flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default()),
            lines: 0,
            since: Instant::now(),
        });
        for line in lines {
            let line = line.as_ref();
            ensure!(
                !line.contains('\n'),
                "a JSON Lines line holds a newline (line {} of {})",
                open.lines + 1,
                self.lane.operation
            );
            open.gzip.write_all(line.as_bytes())?;
            open.gzip.write_all(b"\n")?;
            open.lines += 1;
        }
        if open.since.elapsed() >= self.close_after {
            self.close().await?;
        }
        Ok(())
    }

    /// Closes the object being filled, if any, and returns every object of the run in order.
    ///
    /// # Errors
    /// Fails when the last object cannot be committed.
    pub async fn finish(mut self) -> anyhow::Result<Vec<LineObject>> {
        self.close().await?;
        Ok(self.closed)
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        let Some(open) = self.open.take() else {
            return Ok(());
        };
        let number = u32::try_from(self.closed.len()).context("a run of 2^32 objects")?;
        let bytes = open.gzip.finish()?;
        let lane = &self.lane;
        let file_id = (lane.file_id)(number);
        let at = lane.collected_at;
        let params = (lane.params)();
        let plan = move |run| -> anyhow::Result<PublicDataBulkFilePlan> {
            let mut plan = plan_public_data_bulk_file(PublicDataBulkFilePlanInput {
                source_slug: &lane.source.slug,
                ingest_date: at.date_naive(),
                ingestion_run_id: run,
                identity: PublicDataBulkFileIdentity {
                    operation: lane.operation.to_owned(),
                    provider_file_period: None,
                    provider_snapshot_date: Some(at.date_naive()),
                    provider_file_name: format!("{file_id}.gz"),
                    provider_file_id: file_id,
                    provider_updated_at: None,
                },
                raw_payload: bytes,
                content_type: CONTENT_TYPE.to_owned(),
            })
            .context("failed to plan a Bronze line object")?;
            if lane.content_qualified {
                plan.object_key =
                    build_bronze_content_object_key(&plan.object_key, &plan.checksum_sha256)
                        .with_context(|| {
                            format!("failed to content-qualify {}", plan.object_key.as_str())
                        })?;
            }
            Ok(plan)
        };
        let object = match &self.target {
            Target::Live { uow, storage } => {
                let run_params = merged(
                    json!({"operation": lane.operation, "object": number, "lines": open.lines}),
                    &params,
                );
                commit_object(
                    *uow,
                    *storage,
                    &lane.source,
                    plan,
                    run_params,
                    &params,
                    open.lines,
                    at,
                )
                .await?
            }
            Target::DryRun { output_dir } => {
                let plan = plan(IngestionRunId::new(Uuid::new_v4()))?;
                let local = write_local_copy(output_dir, &plan)?;
                LineObject {
                    object_key: plan.object_key.as_str().to_owned(),
                    checksum_sha256: plan.checksum_sha256,
                    size_bytes: plan.size_bytes,
                    lines: open.lines,
                    local_path: Some(local),
                    written: false,
                    new: false,
                }
            }
        };
        self.closed.push(object);
        Ok(())
    }
}

/// Commits one planned object of `source` in an ingestion run of its own: the source row, the run
/// row, the write-once object, its ledger row, the checksum check, the completed run. The plan is
/// made for the run's id. `run_params` are the run's request parameters, `params` what the lane
/// adds to the object's, `records` how many records it holds.
///
/// The writer's own commit, and the one a lane uses for an object that is not lines (a list
/// answer kept as served).
///
/// # Errors
/// Fails when the ledger or the store refuses, or Bronze holds different bytes at the key.
#[allow(clippy::too_many_arguments)] // the run's and the object's parts, each once
pub async fn commit_object<Uow, Storage>(
    uow: &Uow,
    storage: &Storage,
    source: &SourceCatalogEntry,
    plan: impl FnOnce(IngestionRunId) -> anyhow::Result<PublicDataBulkFilePlan>,
    run_params: JsonValue,
    params: &JsonValue,
    records: usize,
    at: DateTime<Utc>,
) -> anyhow::Result<LineObject>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    let writer = BronzeObjectStorageWriter::new(storage);
    let source = uow
        .upsert_source_catalog_entry(source)
        .await
        .with_context(|| format!("failed to upsert the source {}", source.slug))?;
    let run = uow
        .create_ingestion_run(&IngestionRun {
            id: IngestionRunId::new(Uuid::new_v4()),
            source_catalog_id: source.id,
            trigger: IngestionTrigger::Scheduled,
            status: IngestionRunStatus::Running,
            request_params: run_params,
            started_at: at,
            finished_at: None,
            logical_records_seen: 0,
            objects_written: 0,
            error_message: None,
            created_at: at,
            updated_at: at,
            version: 1,
        })
        .await
        .with_context(|| format!("failed to create the {} run", source.slug))?;
    let plan = plan(run.id)?;
    let key = plan.object_key.as_str().to_owned();
    let record = BronzeObject {
        id: BronzeObjectId::new(Uuid::new_v4()),
        source_catalog_id: source.id,
        ingestion_run_id: run.id,
        source_record_id: None,
        source_partition_key: Some(plan.source_partition_key.clone()),
        source_identity_key: plan.source_identity_key.clone(),
        dedupe_key: plan.dedupe_key.clone(),
        request_params: merged(plan.request_params.clone(), params),
        object_key: plan.object_key.clone(),
        checksum_sha256: plan.checksum_sha256.clone(),
        content_type: plan.content_type.clone(),
        size_bytes: plan.size_bytes,
        logical_record_count: u64::try_from(records).ok(),
        collected_at: at,
        snapshot_period: plan.snapshot_period.clone(),
        snapshot_date: plan.snapshot_date,
        snapshot_granularity: plan.snapshot_granularity,
        snapshot_basis: plan.snapshot_basis,
        provider_file_id: None,
        provider_file_name: None,
        provider_updated_at: None,
        effective_date: None,
        created_at: at,
    };
    let checksum_sha256 = plan.checksum_sha256.clone();
    let size_bytes = plan.size_bytes;
    let outcome = BRONZE_COMMITTER
        .commit(
            &writer,
            uow,
            PlannedBronzeObject {
                object_key: key.clone(),
                payload: BronzePayload::InMemory(plan.raw_payload),
                content_type: plan.content_type,
                cache_control: BRONZE_CACHE_CONTROL.to_owned(),
                checksum_sha256: checksum_sha256.clone(),
                record,
            },
        )
        .await
        .with_context(|| format!("failed to commit {key}"))?;
    ensure!(
        outcome.checksum_sha256 == checksum_sha256,
        "Bronze holds different bytes at {key}"
    );
    uow.complete_ingestion_run(CompleteIngestionRunCommand {
        id: run.id,
        status: IngestionRunStatus::Succeeded,
        finished_at: Utc::now(),
        logical_records_seen: u64::try_from(records).unwrap_or(u64::MAX),
        objects_written: 1,
        error_message: None,
    })
    .await
    .with_context(|| format!("failed to complete the {} run", source.slug))?;
    let row = uow
        .find_bronze_object_by_object_key(source.id, &key)
        .await
        .with_context(|| format!("failed to read the ledger row of {key}"))?
        .with_context(|| format!("the ledger holds no row for the committed {key}"))?;
    Ok(LineObject {
        object_key: key,
        checksum_sha256,
        size_bytes,
        lines: records,
        local_path: None,
        written: true,
        new: row.ingestion_run_id == run.id,
    })
}

/// `base` with the keys of `extra` added (an `extra` that is not an object adds nothing).
fn merged(mut base: JsonValue, extra: &JsonValue) -> JsonValue {
    if let (Some(base), Some(extra)) = (base.as_object_mut(), extra.as_object()) {
        for (key, value) in extra {
            base.insert(key.clone(), value.clone());
        }
    }
    base
}

/// Writes `plan`'s bytes under `objects/` of `output_dir`; returns the path relative to it.
fn write_local_copy(output_dir: &Path, plan: &PublicDataBulkFilePlan) -> anyhow::Result<String> {
    let name = plan
        .object_key
        .as_str()
        .rsplit('/')
        .next()
        .context("a Bronze object key has a leaf")?;
    let relative = format!("objects/{name}");
    let path = output_dir.join(&relative);
    std::fs::create_dir_all(output_dir.join("objects"))
        .with_context(|| format!("failed to create {}", output_dir.join("objects").display()))?;
    std::fs::write(&path, &plan.raw_payload)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(relative)
}

#[cfg(test)]
mod tests;
