//! Shared Bronze-commit context for the async national data.go.kr executor (ADR 0013 / 0016).
//!
//! The async lane is concurrent per-page over a JSONL ledger (the ledger stays SSOT for collection
//! STATE — ADR 0013 §5). This module is the seam that lets each concurrent page task route its raw
//! Bronze write through the SAME [`BronzeCommitter::commit_public_data_page`] the sync page lanes use
//! — so the async lane WRITES a `bronze_object` row (ADR 0016 option-a, reversing the Slice-3-B "no
//! DB row" deferral) and gets `CreateOnly` + the recoverable commit protocol, unifying write meaning
//! and verification across sync and async.
//!
//! Why a setup pre-pass: `catalog.bronze_object.source_catalog_id` and `.ingestion_run_id` are both
//! `NOT NULL REFERENCES` contract is defined by the Foundation SQLx baseline, so the
//! committer cannot record a row against invented UUIDs. Before the concurrent fan-out, the executor
//! upserts one `source_catalog` row + creates one `ingestion_run` per distinct `source_slug`
//! ([`BronzeIngestContext::prepare`]); the resulting `(source_catalog_id, ingestion_run_id)` per slug
//! is shared read-only across the `buffer_unordered` tasks. The object key is unaffected: the
//! readable key (ADR 0019) carries neither the run id nor the ingest date, so routing through the
//! committer is byte-identical to the prior direct `put_object`.
//!
//! Concurrency sharing: the whole context lives behind one `Arc` cloned into every page task. The
//! `BronzeCommitter` is a stateless zero-sized value; the UoW is `Arc<dyn BronzeIngestUnitOfWork>`
//! backed by a sqlx `PgPool` (itself `Clone`/`Send`/`Sync`, an internal connection pool), so all
//! tasks share one pool and one committer with no per-task DB connection setup.

use std::{collections::BTreeMap, sync::Arc};

use anyhow::{bail, Context};
use chrono::{DateTime, NaiveDate, Utc};
use collection_application::ports::BronzeIngestUnitOfWork;
use collection_application::{
    BronzeCommitError, BronzeCommitter, PublicDataPageCommitInput, PublicDataPageCommitOutcome,
    PublicDataPageRequest,
};
use collection_domain::{
    IngestionRun, IngestionRunStatus, IngestionTrigger, SourceAuthKind, SourceCatalogEntry,
    SourcePayloadFormat,
};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::ids::{IngestionRunId, SourceCatalogId};
use serde_json::json;
use uuid::Uuid;

use crate::bronze_object_storage::BronzeObjectStorageWriter;

use super::{ledger::LedgerEntry, BRONZE_CACHE_CONTROL, BRONZE_JSON_CONTENT_TYPE};

/// Resolved Catalog identity for one `source_slug` in this executor run: the upserted
/// `source_catalog` row and the `ingestion_run` created to own this run's Bronze objects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SourceRunContext {
    pub(super) source_catalog_id: SourceCatalogId,
    pub(super) ingestion_run_id: IngestionRunId,
}

/// Shared, read-only-after-build Bronze-commit context handed to every concurrent page task.
///
/// Holds the single object-storage port, the single `bronze_object` unit-of-work, and the
/// per-`source_slug` Catalog identity resolved by [`prepare`](Self::prepare). One instance is built
/// before the fan-out and cloned (via `Arc`) into each `buffer_unordered` task.
pub(super) struct BronzeIngestContext {
    storage: Arc<dyn ObjectStorageService>,
    uow: Arc<dyn BronzeIngestUnitOfWork>,
    committer: BronzeCommitter,
    runs: BTreeMap<String, SourceRunContext>,
}

impl BronzeIngestContext {
    /// Builds the context by upserting one `source_catalog` row + creating one `ingestion_run` per
    /// distinct `source_slug` among `selected`, SEQUENTIALLY (before any concurrent page runs).
    ///
    /// Each `ingestion_run` is created `Running` and is intentionally left open: the async lane's
    /// terminal STATE is the JSONL ledger (ADR 0013 §5), not the `ingestion_run` row, so this lane
    /// does not complete the run here (it would need cross-task per-source accounting the ledger
    /// already owns). The run row exists only to satisfy the `bronze_object` foreign key and to give
    /// the recorded objects a real run lineage.
    ///
    /// # Errors
    /// Returns an error if a source upsert or run creation fails, or if a selected entry carries a
    /// non-canonical `source_slug` that cannot be split into `(provider_id, dataset_slug)`.
    pub(super) async fn prepare(
        storage: Arc<dyn ObjectStorageService>,
        uow: Arc<dyn BronzeIngestUnitOfWork>,
        selected: &[LedgerEntry],
        now: DateTime<Utc>,
    ) -> anyhow::Result<Self> {
        let mut slugs: Vec<&str> = selected
            .iter()
            .map(|entry| entry.source_slug.as_str())
            .collect();
        slugs.sort_unstable();
        slugs.dedup();

        let mut runs = BTreeMap::new();
        for slug in slugs {
            let provider = selected
                .iter()
                .find(|entry| entry.source_slug == slug)
                .map_or("data.go.kr", |entry| entry.provider.as_str());
            let entry = source_catalog_entry_for_slug(slug, provider, now)?;
            let source = uow
                .upsert_source_catalog_entry(&entry)
                .await
                .with_context(|| format!("failed to upsert source catalog entry for {slug}"))?;
            let run_id = IngestionRunId::new(Uuid::new_v4());
            uow.create_ingestion_run(&ingestion_run_for_slug(source.id, run_id, slug, now))
                .await
                .with_context(|| format!("failed to create ingestion run for {slug}"))?;
            runs.insert(
                slug.to_owned(),
                SourceRunContext {
                    source_catalog_id: source.id,
                    ingestion_run_id: run_id,
                },
            );
        }

        Ok(Self {
            storage,
            uow,
            committer: BronzeCommitter::new(),
            runs,
        })
    }

    /// Returns the resolved Catalog identity for a `source_slug`, or an error if the slug was not
    /// registered during [`prepare`](Self::prepare) (an executor bug — every selected entry's slug
    /// is registered there).
    pub(super) fn source_run(&self, source_slug: &str) -> anyhow::Result<SourceRunContext> {
        self.runs
            .get(source_slug)
            .copied()
            .with_context(|| format!("no prepared ingestion run for source_slug {source_slug}"))
    }

    /// Commits one data.go.kr page through the single [`BronzeCommitter`] seam: write the raw payload
    /// `CreateOnly` (write-once) then record the `bronze_object` row, with the recoverable commit
    /// protocol (412 → reconcile by checksum). The object key + checksum come from the lane's own
    /// `compile_bronze_page_plan`, identical to the prior direct `put_object` path.
    ///
    /// # Errors
    /// Returns an error (with the same lane context the inline path produced) when the page identity
    /// cannot be compiled, the storage write fails, the metadata record fails, or an existing object
    /// at the key holds different content (`ChecksumConflict`, the quarantine terminal).
    pub(super) async fn commit_page<Req>(
        &self,
        source_slug: &str,
        ingest_date: NaiveDate,
        collected_at: DateTime<Utc>,
        request: Req,
        raw_payload: Vec<u8>,
        payload: serde_json::Value,
    ) -> anyhow::Result<PublicDataPageCommitOutcome>
    where
        Req: PublicDataPageRequest,
    {
        let run = self.source_run(source_slug)?;
        let writer = BronzeObjectStorageWriter::new(self.storage.as_ref());
        let input = PublicDataPageCommitInput {
            source_slug,
            ingest_date,
            ingestion_run_id: run.ingestion_run_id,
            request,
            raw_payload,
            payload,
            source_catalog_id: run.source_catalog_id,
            collected_at,
            content_type: BRONZE_JSON_CONTENT_TYPE.to_owned(),
            cache_control: BRONZE_CACHE_CONTROL.to_owned(),
        };
        self.committer
            .commit_public_data_page(&writer, self.uow.as_ref(), input)
            .await
            .map_err(|error| commit_error_to_anyhow(source_slug, error))
    }
}

/// Maps a [`BronzeCommitError`] to an `anyhow` error that names the failing source slug and the
/// failure class. Mirrors the sync `page_collector` mapping: a `ChecksumConflict` is the
/// recoverable-commit-protocol quarantine terminal (the key holds different content) and fails loud.
fn commit_error_to_anyhow(source_slug: &str, error: BronzeCommitError) -> anyhow::Error {
    match error {
        BronzeCommitError::Plan { source } => anyhow::anyhow!("{source}")
            .context(format!("failed to plan Bronze object for {source_slug}")),
        BronzeCommitError::Storage { source, key } => anyhow::anyhow!("{source}")
            .context(format!("failed to write Bronze object to R2: {key}")),
        BronzeCommitError::Record { source, key } => anyhow::anyhow!("{source}").context(format!(
            "failed to record Bronze object metadata for {source_slug}: {key}"
        )),
        BronzeCommitError::ChecksumConflict { key } => anyhow::anyhow!(
            "existing Bronze object has a different checksum than this run's payload: {key}"
        )
        .context(format!("Bronze checksum conflict for {source_slug}: {key}")),
    }
}

/// Builds the `source_catalog` row for one canonical `source_slug` (`{provider_id}__{dataset_slug}`).
///
/// The upsert is keyed by slug, so `name`/`dataset_name` are display lineage only (not load-bearing
/// for the object key or the `bronze_object` identity); `dataset_name` is derived from the slug's
/// `dataset_slug` segment. Returns an error if the slug is not the canonical `__`-joined shape.
fn source_catalog_entry_for_slug(
    slug: &str,
    provider: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<SourceCatalogEntry> {
    let Some((_provider_id, dataset_slug)) = slug.split_once("__") else {
        bail!("non-canonical Bronze source_slug for async ingest: {slug:?} (expected '<providerid>__<dataset_slug>')");
    };
    Ok(SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: slug.to_owned(),
        name: format!("national async {slug}"),
        provider: provider.to_owned(),
        dataset_name: dataset_slug.replace('_', "-"),
        base_url: None,
        auth_kind: SourceAuthKind::ServiceKey,
        payload_format: SourcePayloadFormat::Json,
        license_name: None,
        license_url: None,
        terms_url: Some("https://www.data.go.kr/".to_owned()),
        collection_frequency: None,
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    })
}

/// Builds the open (`Running`) `ingestion_run` row that owns this run's Bronze objects for one slug.
fn ingestion_run_for_slug(
    source_catalog_id: SourceCatalogId,
    run_id: IngestionRunId,
    slug: &str,
    now: DateTime<Utc>,
) -> IngestionRun {
    IngestionRun {
        id: run_id,
        source_catalog_id,
        trigger: IngestionTrigger::Manual,
        status: IngestionRunStatus::Running,
        request_params: json!({ "lane": "national-data-collection-async", "source_slug": slug }),
        started_at: now,
        finished_at: None,
        logical_records_seen: 0,
        objects_written: 0,
        error_message: None,
        created_at: now,
        updated_at: now,
        version: 1,
    }
}

#[cfg(test)]
#[path = "bronze_ingest/tests.rs"]
mod tests;
