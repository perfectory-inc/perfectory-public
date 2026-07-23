//! Shared page-collection loop (ADR 0017 — the `PageCollector` seam).
//!
//! ADR 0016's [`BronzeCommitter`](collection_application::BronzeCommitter) unified the Bronze *write*; this is
//! the *collection* side of the same pipeline. Every data.go.kr / V-World **page lane** ran a
//! byte-identical persist loop:
//!
//! ```text
//! upsert source -> create run
//!   -> for each planned page: build the lane CommitInput -> committer.commit_*_page -> account
//!        (on commit error: map to the lane's terminal anyhow error + mark the run Failed)
//!   -> for each schema profile (lane CandidateKeyOverride): upsert_schema_profile
//!   -> complete_ingestion_run(Succeeded) with the run's logical-record / objects-written accounting
//! ```
//!
//! That loop, its ordering, the `objects_written` accounting, the commit-error -> terminal-failure
//! mapping, the run lifecycle, and the schema-profile gathering are IDENTICAL across lanes — only the
//! lane request type, the `commit_*_page` wrapper (all of which delegate to the one generic
//! [`commit_public_data_page`](collection_application::BronzeCommitter::commit_public_data_page)), the
//! [`CandidateKeyOverride`], and the lane name used in error context differ. This module owns the
//! loop ONCE; a lane plugs in by implementing [`PageCollectorLane`] (the per-source declaration) and
//! handing its already-planned pages to [`collect_planned_pages`].
//!
//! The *fetch* loop (`plan_pages`) stays per-lane: it genuinely diverges (per-lane stop conditions,
//! provider total-count JSON pointers, single vs multi request-window iteration, and different client
//! method signatures), so forcing it into this seam would need an ugly signature. The honest shared
//! seam is the persist/commit loop above — the fetch-side parallel of the write-side scatter ADR 0016
//! fixed.

use anyhow::{anyhow, bail, Context};
use chrono::Utc;
use collection_application::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};
use collection_application::{
    BronzeCommitError, BronzeCommitter, PublicDataBronzePagePlan, PublicDataPageCommitInput,
    PublicDataPageRequest,
};
use collection_domain::{IngestionRun, IngestionRunStatus, SourceCatalogEntry};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId};

use crate::bronze_object_storage::BronzeObjectStorageWriter;
use crate::bronze_schema_profile::{schema_profiles_for_plans, CandidateKeyOverride};

const BRONZE_JSON_CONTENT_TYPE: &str = "application/json";
const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";
const MAX_FAILURE_MESSAGE_BYTES: usize = 1_000;

/// One already-fetched-and-planned page handed to the collector.
///
/// Carries the lane's compiled `plan` (drives the run-level accounting + schema profiles, unchanged)
/// plus the RAW page identity (`request` + `raw_payload` + `payload`) the
/// [`BronzeCommitter`](collection_application::BronzeCommitter) needs to OWN the key-compile (ADR 0016). This is
/// the exact set of fields every lane's `*PlannedPage` struct already holds.
#[derive(Clone, Debug)]
pub(crate) struct CollectablePage<Req> {
    /// The lane's compiled Bronze page plan (an alias of [`PublicDataBronzePagePlan`]).
    pub plan: PublicDataBronzePagePlan,
    /// Provider request parameters for this page (handed to the committer's owned key-compile).
    pub request: Req,
    /// Raw provider response bytes, stored unchanged in Bronze.
    pub raw_payload: Vec<u8>,
    /// Parsed provider response used only for metadata and schema profiling.
    pub payload: serde_json::Value,
}

/// The per-source declaration a page lane supplies so the shared loop knows the lane-specific bits:
/// the human-readable lane label used in error context, and the lane's candidate-key override.
///
/// This is intentionally tiny — the loop, ordering, accounting, commit handoff, run lifecycle, and
/// schema-profile gathering live in [`collect_planned_pages`], never in the lane. Adding a page lane
/// is one impl of this trait + one call to the collector (the same shape the committer's generic gave
/// the write side).
pub(crate) trait PageCollectorLane {
    /// The lane's provider request type (e.g. `RealTransactionPageRequest`). It compiles itself into
    /// the shared Bronze page plan through [`PublicDataPageRequest`], which is what lets the committer
    /// own the key-compile generically.
    type Request: PublicDataPageRequest;

    /// Human-readable lane label used in commit-error context messages, e.g. `"real-transaction"`.
    fn lane_label(&self) -> &str;

    /// The candidate-key re-scoring this lane applies while merging schema observations.
    fn candidate_key_override(&self) -> CandidateKeyOverride;
}

/// Evidence + accounting returned by a successful page collection — the run's identity and the
/// last-object pointers + record/object counts every lane echoed in its live-write summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PageCollectionReport {
    pub run_id: IngestionRunId,
    pub last_object_key: Option<String>,
    /// Lowercase hex sha256 of the last committed Bronze object (pairs with `last_object_key`).
    pub last_object_checksum_sha256: Option<String>,
    pub last_bronze_object_id: Option<BronzeObjectId>,
    pub logical_records_seen: u64,
    pub objects_written: u64,
}

/// Runs the shared page-collection loop for one run: upsert the source, create the run, commit each
/// planned page through the [`BronzeCommitter`](collection_application::BronzeCommitter), upsert the merged
/// schema profiles, and complete the run — with identical ordering, accounting, and failure handling
/// for every page lane.
///
/// `source_catalog_entry` / `ingestion_run` are the lane's already-built catalog entities (the only
/// per-lane catalog identity), and `pages` are the lane's already-planned pages. On any commit /
/// schema-profile / completion error the run is marked Failed with the lane's terminal message,
/// preserving the per-lane `objects_written` accounting (a record failure after a successful write
/// still counts the object).
pub(crate) async fn collect_planned_pages<Lane, Uow, Storage>(
    lane: &Lane,
    source_catalog_entry: SourceCatalogEntry,
    ingestion_run: IngestionRun,
    source_slug: &str,
    started_at: chrono::DateTime<Utc>,
    pages: &[CollectablePage<Lane::Request>],
    uow: &Uow,
    storage: &Storage,
) -> anyhow::Result<PageCollectionReport>
where
    Lane: PageCollectorLane,
    Lane::Request: Clone,
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    let label = lane.lane_label();
    if pages.is_empty() {
        bail!("{label} ingest produced no Bronze page plans");
    }

    // The compiled plans the planning stage already produced drive the run-level request snapshot,
    // schema profiles, and logical-record accounting (unchanged). The per-page key compile + record
    // lives INSIDE the committer (ADR 0016), so the loop hands it the RAW page identity instead.
    let plans: Vec<PublicDataBronzePagePlan> = pages.iter().map(|page| page.plan.clone()).collect();

    let source = uow
        .upsert_source_catalog_entry(&source_catalog_entry)
        .await
        .with_context(|| format!("failed to upsert {label} source catalog entry"))?;
    let run = uow
        .create_ingestion_run(&IngestionRun {
            source_catalog_id: source.id,
            ..ingestion_run
        })
        .await
        .with_context(|| format!("failed to create {label} ingestion run"))?;

    // ADR 0016: every Bronze raw write flows through the single BronzeCommitter seam, which OWNS the
    // key compile (object key + checksum + dedupe + the `bronze_object` record) AND the storage write
    // + record in one place, with the CreateOnly write-once + recoverable commit protocol. The writer
    // adapter bridges the low-level storage port to the committer's narrow write seam.
    let committer = BronzeCommitter::new();
    let writer = BronzeObjectStorageWriter::new(storage);

    let mut last_object_key = None;
    let mut last_object_checksum_sha256 = None;
    let mut last_bronze_object_id = None;
    let mut objects_written = 0;
    for page in pages {
        let object_key = page.plan.object_key.as_str().to_owned();
        let input = PublicDataPageCommitInput {
            source_slug,
            ingest_date: started_at.date_naive(),
            ingestion_run_id: run.id,
            request: page.request.clone(),
            raw_payload: page.raw_payload.clone(),
            payload: page.payload.clone(),
            source_catalog_id: source.id,
            collected_at: started_at,
            content_type: BRONZE_JSON_CONTENT_TYPE.to_owned(),
            cache_control: BRONZE_CACHE_CONTROL.to_owned(),
        };

        match committer.commit_public_data_page(&writer, uow, input).await {
            Ok(outcome) => {
                objects_written += 1;
                last_object_key = Some(outcome.object_key);
                last_object_checksum_sha256 = Some(outcome.checksum_sha256);
                last_bronze_object_id = Some(outcome.bronze_object_id);
            }
            Err(commit_error) => {
                // The storage write happening before the record means a record failure still leaves
                // an object on R2: keep the same `objects_written` accounting the inline path had
                // (incremented after a successful write, before the record).
                let (error, write_succeeded) =
                    commit_error_to_anyhow(label, &object_key, commit_error);
                if write_succeeded {
                    objects_written += 1;
                }
                return Err(mark_run_failed_after_error(
                    uow,
                    label,
                    run.id,
                    &plans,
                    objects_written,
                    error,
                )
                .await);
            }
        }
    }

    for profile in schema_profiles_for_plans(
        source.id,
        run.id,
        started_at,
        &plans,
        lane.candidate_key_override(),
    ) {
        if let Err(error) = uow
            .upsert_schema_profile(&profile)
            .await
            .with_context(|| format!("failed to upsert {label} schema profile"))
        {
            return Err(mark_run_failed_after_error(
                uow,
                label,
                run.id,
                &plans,
                objects_written,
                error,
            )
            .await);
        }
    }

    let completed = uow
        .complete_ingestion_run(CompleteIngestionRunCommand {
            id: run.id,
            status: IngestionRunStatus::Succeeded,
            finished_at: Utc::now(),
            logical_records_seen: total_logical_record_count(&plans),
            objects_written,
            error_message: None,
        })
        .await
        .with_context(|| format!("failed to complete {label} ingestion run"))?;

    Ok(PageCollectionReport {
        run_id: completed.id,
        last_object_key,
        last_object_checksum_sha256,
        last_bronze_object_id,
        logical_records_seen: completed.logical_records_seen,
        objects_written: completed.objects_written,
    })
}

/// Maps a [`BronzeCommitError`] back to the lane's terminal-failure `anyhow` error, preserving the
/// exact context messages the per-lane inline put+record path produced, plus whether the storage
/// write had already succeeded (so the run's `objects_written` count stays identical).
///
/// The lane label is the only per-lane part; the message shapes are otherwise identical across lanes.
fn commit_error_to_anyhow(
    lane_label: &str,
    object_key: &str,
    error: BronzeCommitError,
) -> (anyhow::Error, bool) {
    match error {
        // The committer compiles the key BEFORE writing, so a plan failure means nothing was written.
        // Unreachable on the live path (the planning stage already compiled these inputs), but mapped
        // for completeness so a compile failure fails the run loudly rather than silently.
        BronzeCommitError::Plan { source } => (
            anyhow!("{source}")
                .context(format!("failed to plan {lane_label} Bronze object: {object_key}")),
            false,
        ),
        BronzeCommitError::Storage { source, .. } => (
            anyhow!("{source}")
                .context(format!("failed to write {lane_label} Bronze object: {object_key}")),
            false,
        ),
        BronzeCommitError::Record { source, .. } => (
            anyhow!("{source}").context(format!(
                "failed to record {lane_label} Bronze object metadata: {object_key}"
            )),
            true,
        ),
        // Quarantine terminal of the recoverable commit protocol (ADR 0016): the key is occupied by a
        // DIFFERENT object than this run produced. The committer only reaches here on a CreateOnly
        // collision it could not reconcile by checksum, so nothing new was written
        // (`write_succeeded = false`) — fail the run loudly for operator investigation.
        BronzeCommitError::ChecksumConflict { .. } => (
            anyhow!(
                "existing Bronze object has a different checksum than this run's payload: {object_key}"
            )
            .context(format!(
                "Bronze checksum conflict for {lane_label} object: {object_key}"
            )),
            false,
        ),
    }
}

async fn mark_run_failed_after_error<Uow>(
    uow: &Uow,
    lane_label: &str,
    run_id: IngestionRunId,
    plans: &[PublicDataBronzePagePlan],
    objects_written: u64,
    error: anyhow::Error,
) -> anyhow::Error
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
{
    let failure_message = terminal_failure_message(&error);
    let failure_result = uow
        .complete_ingestion_run(CompleteIngestionRunCommand {
            id: run_id,
            status: IngestionRunStatus::Failed,
            finished_at: Utc::now(),
            logical_records_seen: total_logical_record_count(plans),
            objects_written,
            error_message: Some(failure_message),
        })
        .await;

    match failure_result {
        Ok(_) => error,
        Err(failure_error) => error.context(format!(
            "also failed to mark {lane_label} ingestion run {run_id} as failed: {failure_error}"
        )),
    }
}

fn terminal_failure_message(error: &anyhow::Error) -> String {
    truncate_failure_message(&format!("{error:#}"))
}

fn truncate_failure_message(message: &str) -> String {
    if message.len() <= MAX_FAILURE_MESSAGE_BYTES {
        return message.to_owned();
    }

    let mut end = MAX_FAILURE_MESSAGE_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &message[..end])
}

fn total_logical_record_count(plans: &[PublicDataBronzePagePlan]) -> u64 {
    plans.iter().map(|plan| plan.logical_record_count).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use async_trait::async_trait;
    use collection_application::ports::CompleteIngestionRunCommand;
    use collection_application::{
        plan_real_transaction_bronze_page, RealTransactionBronzePagePlanInput,
        RealTransactionPageRequest,
    };
    use collection_domain::CollectionError;
    use collection_domain::{
        BronzeObject, IngestionTrigger, SchemaProfile, SourceAuthKind, SourcePayloadFormat,
    };
    use foundation_outbox::object_storage::PutObjectRequest;
    use foundation_outbox::PublishError;
    use foundation_shared_kernel::ids::SourceCatalogId;
    use uuid::Uuid;

    /// The real-transaction lane stands in for "any page lane" here — the collector loop is generic
    /// over the lane, so proving it commits N pages in order for one lane proves the shared loop.
    struct ProbeLane;

    impl PageCollectorLane for ProbeLane {
        type Request = RealTransactionPageRequest;

        fn lane_label(&self) -> &str {
            "probe-lane"
        }

        fn candidate_key_override(&self) -> CandidateKeyOverride {
            CandidateKeyOverride::None
        }
    }

    fn started_at() -> chrono::DateTime<Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-05-14T00:00:00Z")
            .expect("valid rfc3339")
            .to_utc()
    }

    fn collectable_page(
        run_id: IngestionRunId,
        page_no: u32,
    ) -> CollectablePage<RealTransactionPageRequest> {
        let payload = serde_json::json!({
            "response": { "body": { "items": { "item": [
                { "거래금액": format!("12,{page_no:03}"), "건물면적": "84.5" }
            ] } } }
        });
        let raw_payload = serde_json::to_vec(&payload).expect("serialize payload");
        let request = RealTransactionPageRequest {
            operation: "getRTMSDataSvcInduTrade".to_owned(),
            lawd_cd: "11680".to_owned(),
            deal_ymd: "202605".to_owned(),
            page_no,
            num_of_rows: 1000,
        };
        let plan = plan_real_transaction_bronze_page(RealTransactionBronzePagePlanInput {
            source_slug: "datagokr__real_transaction_industrial_trade",
            ingest_date: started_at().date_naive(),
            ingestion_run_id: run_id,
            request: request.clone(),
            raw_payload: raw_payload.clone(),
            payload: payload.clone(),
        })
        .expect("plan page");
        CollectablePage {
            plan,
            request,
            raw_payload,
            payload,
        }
    }

    fn source_entry() -> SourceCatalogEntry {
        SourceCatalogEntry {
            id: SourceCatalogId::new(Uuid::nil()),
            slug: "datagokr__real_transaction_industrial_trade".to_owned(),
            name: "probe".to_owned(),
            provider: "data.go.kr".to_owned(),
            dataset_name: "real-transaction-industrial-trade".to_owned(),
            base_url: None,
            auth_kind: SourceAuthKind::ServiceKey,
            payload_format: SourcePayloadFormat::Json,
            license_name: None,
            license_url: None,
            terms_url: None,
            collection_frequency: None,
            is_active: true,
            created_at: started_at(),
            updated_at: started_at(),
            version: 1,
        }
    }

    fn running_run(run_id: IngestionRunId) -> IngestionRun {
        IngestionRun {
            id: run_id,
            source_catalog_id: SourceCatalogId::new(Uuid::nil()),
            trigger: IngestionTrigger::Manual,
            status: IngestionRunStatus::Running,
            request_params: serde_json::json!({}),
            started_at: started_at(),
            finished_at: None,
            logical_records_seen: 0,
            objects_written: 0,
            error_message: None,
            created_at: started_at(),
            updated_at: started_at(),
            version: 1,
        }
    }

    /// The collector iterates the planned pages, hands each to the committer, and completes the run —
    /// so for N planned pages it writes N objects, in page order, and records N rows with the
    /// run-level logical-record / objects-written accounting. This is the shared-loop proof.
    #[tokio::test]
    async fn collects_each_planned_page_in_order() -> anyhow::Result<()> {
        let run_id = IngestionRunId::new(Uuid::new_v4());
        let source_id = SourceCatalogId::new(Uuid::new_v4());
        let pages: Vec<_> = (1..=3)
            .map(|page_no| collectable_page(run_id, page_no))
            .collect();
        let expected_keys: Vec<String> = pages
            .iter()
            .map(|page| page.plan.object_key.as_str().to_owned())
            .collect();

        let uow = FakeUow::new(source_id);
        let storage = FakeStorage::default();

        let report = collect_planned_pages(
            &ProbeLane,
            source_entry(),
            running_run(run_id),
            "datagokr__real_transaction_industrial_trade",
            started_at(),
            &pages,
            &uow,
            &storage,
        )
        .await?;

        // Three pages -> three objects written, in page order; the run completes Succeeded with the
        // summed logical-record count (one item per page) and the matching objects-written count.
        assert_eq!(storage.written_keys()?, expected_keys);
        assert_eq!(report.objects_written, 3);
        assert_eq!(report.logical_records_seen, 3);
        assert_eq!(
            report.last_object_key.as_deref(),
            expected_keys.last().map(String::as_str)
        );

        let recorded_keys: Vec<String> = uow
            .recorded()?
            .iter()
            .map(|object| object.object_key.as_str().to_owned())
            .collect();
        assert_eq!(recorded_keys, expected_keys);

        let completions = uow.completions()?;
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].status, IngestionRunStatus::Succeeded);
        assert_eq!(completions[0].objects_written, 3);
        assert_eq!(completions[0].logical_records_seen, 3);
        Ok(())
    }

    struct FakeUow {
        source_id: SourceCatalogId,
        runs: Mutex<Vec<IngestionRun>>,
        completions: Mutex<Vec<CompleteIngestionRunCommand>>,
        bronze_objects: Mutex<Vec<BronzeObject>>,
        schema_profiles: Mutex<Vec<SchemaProfile>>,
    }

    impl FakeUow {
        const fn new(source_id: SourceCatalogId) -> Self {
            Self {
                source_id,
                runs: Mutex::new(Vec::new()),
                completions: Mutex::new(Vec::new()),
                bronze_objects: Mutex::new(Vec::new()),
                schema_profiles: Mutex::new(Vec::new()),
            }
        }

        fn recorded(&self) -> anyhow::Result<Vec<BronzeObject>> {
            Ok(self
                .bronze_objects
                .lock()
                .map_err(|_| anyhow::anyhow!("bronze object lock poisoned"))?
                .clone())
        }

        fn completions(&self) -> anyhow::Result<Vec<CompleteIngestionRunCommand>> {
            Ok(self
                .completions
                .lock()
                .map_err(|_| anyhow::anyhow!("completion lock poisoned"))?
                .clone())
        }
    }

    #[async_trait]
    impl BronzeIngestUnitOfWork for FakeUow {
        async fn upsert_source_catalog_entry(
            &self,
            entry: &SourceCatalogEntry,
        ) -> Result<SourceCatalogEntry, CollectionError> {
            let mut source = entry.clone();
            source.id = self.source_id;
            Ok(source)
        }

        async fn create_ingestion_run(
            &self,
            run: &IngestionRun,
        ) -> Result<IngestionRun, CollectionError> {
            self.runs
                .lock()
                .map_err(|_| CollectionError::Infrastructure("run lock poisoned".to_owned()))?
                .push(run.clone());
            Ok(run.clone())
        }

        async fn complete_ingestion_run(
            &self,
            command: CompleteIngestionRunCommand,
        ) -> Result<IngestionRun, CollectionError> {
            self.completions
                .lock()
                .map_err(|_| {
                    CollectionError::Infrastructure("completion lock poisoned".to_owned())
                })?
                .push(command.clone());
            let run = self
                .runs
                .lock()
                .map_err(|_| CollectionError::Infrastructure("run lock poisoned".to_owned()))?
                .iter()
                .find(|run| run.id == command.id)
                .cloned()
                .ok_or_else(|| CollectionError::IngestionRunNotFound(command.id.to_string()))?;
            Ok(IngestionRun {
                status: command.status,
                finished_at: Some(command.finished_at),
                logical_records_seen: command.logical_records_seen,
                objects_written: command.objects_written,
                error_message: command.error_message,
                ..run
            })
        }

        async fn find_bronze_object_by_object_key(
            &self,
            source_catalog_id: SourceCatalogId,
            object_key: &str,
        ) -> Result<Option<BronzeObject>, CollectionError> {
            Ok(self
                .bronze_objects
                .lock()
                .map_err(|_| {
                    CollectionError::Infrastructure("bronze object lock poisoned".to_owned())
                })?
                .iter()
                .rev()
                .find(|object| {
                    object.source_catalog_id == source_catalog_id
                        && object.object_key.as_str() == object_key
                })
                .cloned())
        }

        async fn record_bronze_object(
            &self,
            object: &BronzeObject,
        ) -> Result<BronzeObject, CollectionError> {
            self.bronze_objects
                .lock()
                .map_err(|_| {
                    CollectionError::Infrastructure("bronze object lock poisoned".to_owned())
                })?
                .push(object.clone());
            Ok(object.clone())
        }

        async fn upsert_schema_profile(
            &self,
            profile: &SchemaProfile,
        ) -> Result<SchemaProfile, CollectionError> {
            self.schema_profiles
                .lock()
                .map_err(|_| {
                    CollectionError::Infrastructure("schema profile lock poisoned".to_owned())
                })?
                .push(profile.clone());
            Ok(profile.clone())
        }
    }

    #[derive(Default)]
    struct FakeStorage {
        written_keys: Mutex<Vec<String>>,
    }

    impl FakeStorage {
        fn written_keys(&self) -> anyhow::Result<Vec<String>> {
            Ok(self
                .written_keys
                .lock()
                .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?
                .clone())
        }
    }

    #[async_trait]
    impl ObjectStorageService for FakeStorage {
        async fn put_object(&self, request: PutObjectRequest) -> Result<(), PublishError> {
            self.written_keys
                .lock()
                .map_err(|_| PublishError::Infrastructure("storage lock poisoned".to_owned()))?
                .push(request.key);
            Ok(())
        }

        async fn read_object_sha256(&self, _key: &str) -> Result<Option<String>, PublishError> {
            Ok(None)
        }
    }
}
