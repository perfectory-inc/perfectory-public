//! One API's responses of one run as one Bronze object: the source row, the run row, the
//! write-once object and its ledger row (root ADR-0280).

use std::path::Path;

use anyhow::{ensure, Context};
use chrono::{DateTime, Utc};
use collection_application::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};
use collection_application::{BronzePayload, PlannedBronzeObject, PublicDataBulkFilePlan};
use collection_domain::{
    source_slug, BronzeObject, IngestionRun, IngestionRunStatus, IngestionTrigger, SourceAuthKind,
    SourceCatalogEntry, SourcePayloadFormat,
};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

use super::{
    gzip_lines, plan, run_id, Api, Endpoint, SourceContract, BRONZE_CACHE_CONTROL,
    BRONZE_COMMITTER, PROVIDER,
};
use crate::bronze_object_storage::BronzeObjectStorageWriter;

/// The provider file id of one API's object of one run.
pub(crate) fn file_id(api: Api, at: DateTime<Utc>) -> String {
    format!(
        "{}-{}",
        api.name().replace('_', "-"),
        run_id(at).to_ascii_lowercase()
    )
}

/// One landed (or, in a dry run, planned) object, as the manifest names it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ManifestObject {
    pub(crate) role: String,
    pub(crate) dataset_slug: String,
    pub(crate) object_key: String,
    pub(crate) checksum_sha256: String,
    pub(crate) size_bytes: u64,
    pub(crate) lines: usize,
    pub(crate) local_path: String,
    pub(crate) written: bool,
}

fn write_local_copy(output_dir: &Path, plan: &PublicDataBulkFilePlan) -> anyhow::Result<String> {
    let name = plan
        .object_key
        .as_str()
        .rsplit('/')
        .next()
        .context("a Bronze object key has a leaf")?;
    let relative = format!("objects/{name}");
    let path = output_dir.join(&relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(&path, &plan.raw_payload)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(relative)
}

/// Commits one API's object: source row, run row, write-once object, ledger row.
pub(crate) async fn persist<Uow, Storage>(
    contract: &SourceContract,
    output_dir: &Path,
    api: Api,
    lines: &[String],
    at: DateTime<Utc>,
    live: Option<(&Uow, &Storage)>,
) -> anyhow::Result<ManifestObject>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    let endpoint = contract.endpoint(api);
    let bytes = gzip_lines(lines)?;
    let Some((uow, storage)) = live else {
        let plan = plan(
            contract,
            api,
            bytes,
            IngestionRunId::new(Uuid::new_v4()),
            at,
        )?;
        let local = write_local_copy(output_dir, &plan)?;
        return Ok(manifest_object(
            api,
            endpoint,
            &plan,
            lines.len(),
            local,
            false,
        ));
    };
    let writer = BronzeObjectStorageWriter::new(storage);
    let source = uow
        .upsert_source_catalog_entry(&source_catalog_entry(contract, endpoint, at)?)
        .await
        .with_context(|| format!("failed to upsert the {} source", endpoint.dataset_slug))?;
    let run = uow
        .create_ingestion_run(&ingestion_run(source.id, at, endpoint, lines.len()))
        .await
        .with_context(|| format!("failed to create the {} run", endpoint.dataset_slug))?;
    let plan = plan(contract, api, bytes, run.id, at)?;
    let outcome = BRONZE_COMMITTER
        .commit(
            &writer,
            uow,
            PlannedBronzeObject {
                object_key: plan.object_key.as_str().to_owned(),
                payload: BronzePayload::InMemory(plan.raw_payload.clone()),
                content_type: plan.content_type.clone(),
                cache_control: BRONZE_CACHE_CONTROL.to_owned(),
                checksum_sha256: plan.checksum_sha256.clone(),
                record: bronze_object_record(&source, &run, at, &plan, &file_id(api, at)),
            },
        )
        .await
        .with_context(|| format!("failed to commit {}", plan.object_key.as_str()))?;
    ensure!(
        outcome.checksum_sha256 == plan.checksum_sha256,
        "Bronze holds different bytes at {}",
        plan.object_key.as_str()
    );
    let local = write_local_copy(output_dir, &plan)?;
    uow.complete_ingestion_run(CompleteIngestionRunCommand {
        id: run.id,
        status: IngestionRunStatus::Succeeded,
        finished_at: Utc::now(),
        logical_records_seen: u64::try_from(lines.len()).unwrap_or(u64::MAX),
        objects_written: 1,
        error_message: None,
    })
    .await
    .with_context(|| format!("failed to complete the {} run", endpoint.dataset_slug))?;
    Ok(manifest_object(
        api,
        endpoint,
        &plan,
        lines.len(),
        local,
        true,
    ))
}

fn manifest_object(
    api: Api,
    endpoint: &Endpoint,
    plan: &PublicDataBulkFilePlan,
    lines: usize,
    local_path: String,
    written: bool,
) -> ManifestObject {
    ManifestObject {
        role: api.name().to_owned(),
        dataset_slug: endpoint.dataset_slug.clone(),
        object_key: plan.object_key.as_str().to_owned(),
        checksum_sha256: plan.checksum_sha256.clone(),
        size_bytes: plan.size_bytes,
        lines,
        local_path,
        written,
    }
}

fn bronze_object_record(
    source: &SourceCatalogEntry,
    run: &IngestionRun,
    at: DateTime<Utc>,
    plan: &PublicDataBulkFilePlan,
    provider_file_id: &str,
) -> BronzeObject {
    BronzeObject {
        id: BronzeObjectId::new(Uuid::new_v4()),
        source_catalog_id: source.id,
        ingestion_run_id: run.id,
        source_record_id: None,
        source_partition_key: Some(plan.source_partition_key.clone()),
        source_identity_key: plan.source_identity_key.clone(),
        dedupe_key: plan.dedupe_key.clone(),
        request_params: plan.request_params.clone(),
        object_key: plan.object_key.clone(),
        checksum_sha256: plan.checksum_sha256.clone(),
        content_type: plan.content_type.clone(),
        size_bytes: plan.size_bytes,
        logical_record_count: None,
        collected_at: at,
        snapshot_period: plan.snapshot_period.clone(),
        snapshot_date: plan.snapshot_date,
        snapshot_granularity: plan.snapshot_granularity,
        snapshot_basis: plan.snapshot_basis,
        provider_file_id: Some(provider_file_id.to_owned()),
        provider_file_name: Some(format!("{provider_file_id}.gz")),
        provider_updated_at: None,
        effective_date: None,
        created_at: at,
    }
}

fn source_catalog_entry(
    contract: &SourceContract,
    endpoint: &Endpoint,
    now: DateTime<Utc>,
) -> anyhow::Result<SourceCatalogEntry> {
    Ok(SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: source_slug(PROVIDER, &endpoint.dataset_slug)?,
        name: endpoint.dataset_slug.clone(),
        provider: PROVIDER.to_owned(),
        dataset_name: endpoint.dataset_slug.clone(),
        base_url: Some(contract.base_url.clone()),
        auth_kind: SourceAuthKind::ServiceKey,
        payload_format: SourcePayloadFormat::Json,
        license_name: None,
        license_url: None,
        terms_url: None,
        collection_frequency: Some(format!("every {} hours", endpoint.every_hours)),
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    })
}

fn ingestion_run(
    source_catalog_id: SourceCatalogId,
    now: DateTime<Utc>,
    endpoint: &Endpoint,
    lines: usize,
) -> IngestionRun {
    IngestionRun {
        id: IngestionRunId::new(Uuid::new_v4()),
        source_catalog_id,
        trigger: IngestionTrigger::Scheduled,
        status: IngestionRunStatus::Running,
        request_params: json!({"operation": endpoint.operation, "requests": lines}),
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
