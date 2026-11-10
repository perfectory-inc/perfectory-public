//! 산업입지정보시스템(ILIS) 산업단지 고시와 그 첨부, Bronze 와 공용 미디어 저장소로 (root ADR-0484).
//!
//! One run reads the notice list (one bulk request), computes which notices are still owed from the
//! lakehouse (`notice_detail.read_from`: the last read of each notice; a notice whose last read
//! settled every attachment is never asked again — a notice is published once, a change is a new
//! notice), and asks the owed ones newest first, at most `per_run_limit` and until
//! `run_time_budget_seconds`. Each detail lists its attachments (`saveFiles`); every one still in
//! use is downloaded and stored once, whatever its format (`MediaFormat::sniff_document`), in the
//! shared content-addressed media store (`media_store::store_bytes_as`, root ADR-0298). The detail
//! lands in Bronze with each attachment annotated by its hash, format, size, source URL and
//! outcome, `commit_every` notices to a gzip JSON Lines object, so a stopped run loses at most one
//! batch. The Silver load (`infra/lakehouse/spark/jobs/industryland_or_kr.py`) turns the lines into
//! the read record and the attachment index.
//!
//! It reads nothing inside the files: text, OCR and extraction belong to the intelligence platform,
//! on ai-server, later (root ADR-0484 §1).

use std::{collections::HashSet, io::Write as _, path::PathBuf, time::Duration};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Utc};
use collection_application::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};
use collection_application::{
    plan_public_data_bulk_file, BronzeCommitter, BronzePayload, PlannedBronzeObject,
    PublicDataBulkFileIdentity, PublicDataBulkFilePlan, PublicDataBulkFilePlanInput,
};
use collection_domain::{
    source_slug, BronzeObject, IngestionRun, IngestionRunStatus, IngestionTrigger, SourceAuthKind,
    SourceCatalogEntry, SourcePayloadFormat,
};
use collection_infrastructure::{
    IlisFileDownload, IlisIndustrialComplexApiClient, IlisIndustrialComplexApiConfig,
    PgBronzeIngestUnitOfWork,
};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use foundation_shared_kernel::{MediaFormat, ObjectKeyPrefix, Sha256Hex};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use tokio::time::Instant;
use uuid::Uuid;

use crate::bronze_object_storage::{
    live_write_bronze_object_storage_from_env, live_write_target_preflight,
    BronzeObjectStorageWriter,
};
use crate::media_store::{store_bytes_as, Stored};
use crate::provider_request_spacing::ProviderRequestSpacing;
use crate::public_data_control_support::{optional_env_value, required_env_value};

/// `infra/lakehouse/contracts/industryland-or-kr.contract.json`, as compiled in.
pub(crate) const CONTRACT_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/industryland-or-kr.contract.json");
const PROVIDER: &str = "industryland.or.kr";
const PREFIX: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_NOTICE";
const USER_AGENT: &str =
    "foundation-platform-industrial-complex-notice/1.0 (+https://github.com/perfectory)";
const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";
const BRONZE_COMMITTER: BronzeCommitter = BronzeCommitter::new();
/// The provider's politeness floor (the address-source collector's): the contract may only slow it.
const MIN_REQUEST_SPACING: Duration = Duration::from_secs(2);

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SourceContract {
    pub(crate) base_url: String,
    pub(crate) request_spacing_ms: u64,
    pub(crate) notice_list: NoticeList,
    pub(crate) notice_detail: NoticeDetail,
    pub(crate) attachments: Attachments,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct NoticeList {
    pub(crate) path: String,
    pub(crate) page_size: u32,
    pub(crate) records_pointer: String,
    pub(crate) key_field: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct NoticeDetail {
    pub(crate) dataset_slug: String,
    pub(crate) operation: String,
    pub(crate) path: String,
    pub(crate) result_pointer: String,
    pub(crate) files_field: String,
    pub(crate) per_run_limit: usize,
    pub(crate) commit_every: usize,
    pub(crate) run_time_budget_seconds: u64,
    pub(crate) stop_after_consecutive_failures: u32,
    pub(crate) read_from: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Attachments {
    pub(crate) download_path: String,
    pub(crate) path_field: String,
    pub(crate) name_field: String,
    pub(crate) in_use_field: String,
    pub(crate) in_use_value: String,
    pub(crate) object_prefix_from: String,
    pub(crate) max_file_bytes: u64,
}

pub(crate) fn source_contract() -> anyhow::Result<SourceContract> {
    serde_json::from_str(CONTRACT_JSON).context("industryland-or-kr.contract.json is not valid")
}

/// The prefix the attachments are stored under: the shared store's, as its writer's contract names
/// it (one store, root ADR-0296 §2; the court-auction photos point at the same value).
pub(crate) fn attachment_prefix(attachments: &Attachments) -> anyhow::Result<ObjectKeyPrefix> {
    let (file, pointer) = attachments
        .object_prefix_from
        .split_once('#')
        .context("attachments.object_prefix_from is <file>#<pointer>")?;
    ensure!(
        file == "data-go-kr-onbid.contract.json",
        "attachments.object_prefix_from names {file}; the store's writer contract is data-go-kr-onbid.contract.json"
    );
    let onbid: JsonValue = serde_json::from_str(crate::data_go_kr_onbid_collect::CONTRACT_JSON)
        .context("data-go-kr-onbid.contract.json is not valid")?;
    let prefix = onbid
        .pointer(pointer)
        .and_then(JsonValue::as_str)
        .with_context(|| format!("data-go-kr-onbid.contract.json has no string at {pointer}"))?;
    ObjectKeyPrefix::parse(prefix).context("the media prefix is not a valid object key prefix")
}

/// How one notice read ended. Only `Read` settles it: the next run asks the others again.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReadOutcome {
    /// The detail was read and every attachment settled (stored, already stored, withdrawn, gone,
    /// over the cap).
    Read,
    /// The detail was read but an attachment failed for a reason a later run can get past.
    Incomplete,
    /// The detail itself was not read.
    Failed,
}

/// What one attachment came to; all but `Failed` settle it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StoreOutcome {
    Stored,
    Deduplicated,
    /// The provider marks it withdrawn (`in_use_field` is not `in_use_value`); not asked for.
    Withdrawn,
    /// The listing names no file to ask for.
    NoReference,
    /// The provider answered 4xx.
    Gone,
    /// Over `max_file_bytes`: hashed and counted, not stored.
    Oversize,
    /// A dry run: hashed, not stored.
    DryRun,
    Failed,
}

impl StoreOutcome {
    const fn settled(self) -> bool {
        !matches!(self, Self::Failed)
    }
}

/// What one run's attachments came to.
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct AttachmentReport {
    pub(crate) listed: u64,
    pub(crate) stored: u64,
    pub(crate) deduplicated: u64,
    pub(crate) bytes_stored: u64,
    pub(crate) withdrawn: u64,
    pub(crate) gone: u64,
    pub(crate) oversize: u64,
    pub(crate) failed: u64,
    /// Stored or already stored, by `MediaFormat::extension`: (files, bytes).
    pub(crate) by_format: std::collections::BTreeMap<String, (u64, u64)>,
}

/// The notices whose last read settled them, from rows of `notice_detail.read_from`.
pub(crate) fn settled_notices<'a>(
    rows: impl IntoIterator<Item = &'a serde_json::Map<String, JsonValue>>,
) -> HashSet<i64> {
    let mut last: std::collections::HashMap<i64, ((String, i64), bool)> =
        std::collections::HashMap::new();
    for row in rows {
        let (Some(notice), Some(snapshot)) = (
            whole(&row["notice_sequence"]),
            row["source_snapshot_id"].as_str(),
        ) else {
            continue;
        };
        let order = (
            snapshot.to_owned(),
            whole(&row["source_line_number"]).unwrap_or(0),
        );
        let settled = row["read_outcome"].as_str() == Some("read");
        match last.get(&notice) {
            Some((seen, _)) if *seen >= order => {}
            _ => {
                last.insert(notice, (order, settled));
            }
        }
    }
    last.into_iter()
        .filter_map(|(notice, (_, settled))| settled.then_some(notice))
        .collect()
}

/// The notices this run asks: the listed ones not settled, newest (largest sequence) first, at
/// most `limit`.
pub(crate) fn frontier(listed: &[i64], settled: &HashSet<i64>, limit: usize) -> Vec<i64> {
    let mut owed: Vec<i64> = listed
        .iter()
        .copied()
        .filter(|notice| !settled.contains(notice))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    owed.reverse();
    owed.truncate(limit);
    owed
}

fn whole(value: &JsonValue) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.trim().parse().ok()))
}

/// The notice sequences the list response carries.
pub(crate) fn listed_notices(list: &NoticeList, payload: &JsonValue) -> anyhow::Result<Vec<i64>> {
    let rows = payload
        .pointer(&list.records_pointer)
        .and_then(JsonValue::as_array)
        .with_context(|| {
            format!(
                "the ILIS notice list carries no records at {}",
                list.records_pointer
            )
        })?;
    Ok(rows
        .iter()
        .filter_map(|row| whole(&row[list.key_field.as_str()]))
        .collect())
}

/// Reads the settled notices from the lakehouse (`read_from`'s current snapshot); none when the
/// table holds no snapshot yet. A failure fails the run: it never asks blind.
async fn settled_from_lakehouse(detail: &NoticeDetail) -> anyhow::Result<HashSet<i64>> {
    use crate::lakehouse_snapshot_scan::{scan_snapshot_rows, LakehouseObjectReader};
    use lakehouse_domain::SILVER_INDUSTRIAL_COMPLEX_NOTICE_DETAIL;
    use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};

    ensure!(
        detail.read_from == SILVER_INDUSTRIAL_COMPLEX_NOTICE_DETAIL.table_name,
        "notice_detail.read_from names {}, the collector reads {}",
        detail.read_from,
        SILVER_INDUSTRIAL_COMPLEX_NOTICE_DETAIL.table_name
    );
    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::from_env().context("failed to configure the Iceberg catalog")?,
    )
    .context("failed to build the Iceberg catalog client")?;
    let Some(snapshot) = catalog
        .load_current_snapshot_manifest_list(&detail.read_from)
        .await
        .with_context(|| format!("failed to resolve {}", detail.read_from))?
    else {
        return Ok(HashSet::new());
    };
    let lakehouse = LakehouseObjectReader::from_env()?;
    let scanned = scan_snapshot_rows(
        &SILVER_INDUSTRIAL_COMPLEX_NOTICE_DETAIL,
        &lakehouse,
        &snapshot,
    )
    .await?;
    Ok(settled_notices(scanned.rows.iter()))
}

/// Where one attachment is asked for, as a query and as the URL recorded beside it.
pub(crate) fn download_query(
    attachments: &Attachments,
    file: &JsonValue,
) -> Option<Vec<(String, String)>> {
    let value = |field: &str| {
        file.get(field)
            .and_then(JsonValue::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_owned)
    };
    Some(vec![
        (
            attachments.name_field.clone(),
            value(&attachments.name_field)?,
        ),
        (
            attachments.path_field.clone(),
            value(&attachments.path_field)?,
        ),
    ])
}

/// The URL the bytes were asked from, recorded beside them (the client builds the same query).
pub(crate) fn source_url(
    base: &str,
    attachments: &Attachments,
    query: &[(String, String)],
) -> anyhow::Result<String> {
    let url = reqwest::Url::parse_with_params(
        &format!(
            "{}/{}",
            base.trim_end_matches('/'),
            attachments.download_path.trim_start_matches('/')
        ),
        query,
    )
    .context("the attachment URL is invalid")?;
    Ok(url.into())
}

/// What a run needs to ask: the client, the pace and the count of requests sent.
pub(crate) struct Asker<'a> {
    pub(crate) client: &'a IlisIndustrialComplexApiClient,
    pub(crate) spacing: ProviderRequestSpacing,
    pub(crate) requests: usize,
}

impl Asker<'_> {
    async fn pace(&mut self) {
        self.spacing.wait_before_request(self.requests).await;
        self.requests += 1;
    }
}

/// Reads one notice: its detail, then each attachment, annotated in place in the detail's
/// `files_field` (`file_sequence`, `store_outcome`, and for fetched bytes `sha256`,
/// `media_format`, `size_bytes`, `source_url`, `fetched_at_utc`). Returns its Bronze line.
pub(crate) async fn read_notice(
    contract: &SourceContract,
    asker: &mut Asker<'_>,
    notice: i64,
    prefix: &ObjectKeyPrefix,
    storage: Option<&dyn ObjectStorageService>,
    report: &mut AttachmentReport,
) -> anyhow::Result<(ReadOutcome, JsonValue)> {
    let requested_at = Utc::now();
    asker.pace().await;
    let detail = match asker
        .client
        .fetch_detail(&contract.notice_detail.path, &notice.to_string())
        .await
    {
        Ok(page) => page.payload,
        Err(error) => {
            return Ok((
                ReadOutcome::Failed,
                line(
                    notice,
                    requested_at,
                    None,
                    ReadOutcome::Failed,
                    Some(&error.to_string()),
                    JsonValue::Null,
                ),
            ));
        }
    };
    let mut body = detail;
    let pointer = format!(
        "{}/{}",
        contract.notice_detail.result_pointer, contract.notice_detail.files_field
    );
    if !body
        .pointer(&contract.notice_detail.result_pointer)
        .is_some_and(JsonValue::is_object)
    {
        let reason = format!("no object at {}", contract.notice_detail.result_pointer);
        return Ok((
            ReadOutcome::Failed,
            line(
                notice,
                requested_at,
                Some(200),
                ReadOutcome::Failed,
                Some(&reason),
                body,
            ),
        ));
    }
    let mut outcome = ReadOutcome::Read;
    if let Some(files) = body.pointer_mut(&pointer).and_then(JsonValue::as_array_mut) {
        for (index, file) in files.iter_mut().enumerate() {
            let stored = read_attachment(contract, asker, file, prefix, storage, report).await?;
            if !stored.settled() {
                outcome = ReadOutcome::Incomplete;
            }
            file["file_sequence"] = json!(index + 1);
            file["store_outcome"] = json!(stored);
        }
    }
    Ok((
        outcome,
        line(notice, requested_at, Some(200), outcome, None, body),
    ))
}

async fn read_attachment(
    contract: &SourceContract,
    asker: &mut Asker<'_>,
    file: &mut JsonValue,
    prefix: &ObjectKeyPrefix,
    storage: Option<&dyn ObjectStorageService>,
    report: &mut AttachmentReport,
) -> anyhow::Result<StoreOutcome> {
    let attachments = &contract.attachments;
    report.listed += 1;
    if file
        .get(&attachments.in_use_field)
        .and_then(JsonValue::as_str)
        != Some(attachments.in_use_value.as_str())
    {
        report.withdrawn += 1;
        return Ok(StoreOutcome::Withdrawn);
    }
    let Some(query) = download_query(attachments, file) else {
        return Ok(StoreOutcome::NoReference);
    };
    file["source_url"] = json!(source_url(&contract.base_url, attachments, &query)?);
    asker.pace().await;
    let pairs: Vec<(&str, &str)> = query
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    let fetched_at = Utc::now();
    let bytes = match asker
        .client
        .download_file(&attachments.download_path, &pairs)
        .await
    {
        Ok(IlisFileDownload::Bytes(bytes)) => bytes,
        Ok(IlisFileDownload::Gone(status)) => {
            report.gone += 1;
            file["http_status"] = json!(status);
            return Ok(StoreOutcome::Gone);
        }
        Err(error) => {
            report.failed += 1;
            file["error"] = json!(error.to_string());
            return Ok(StoreOutcome::Failed);
        }
    };
    let format = MediaFormat::sniff_document(&bytes);
    file["sha256"] = json!(Sha256Hex::of(&bytes).as_str());
    file["media_format"] = json!(format.extension());
    file["size_bytes"] = json!(bytes.len() as u64);
    file["fetched_at_utc"] = json!(fetched_at.to_rfc3339());
    let Some(storage) = storage else {
        return Ok(StoreOutcome::DryRun);
    };
    let outcome = match store_bytes_as(&bytes, format, prefix, storage, attachments.max_file_bytes)
        .await
    {
        Ok(Stored::New(media)) => {
            report.stored += 1;
            report.bytes_stored += media.size_bytes;
            StoreOutcome::Stored
        }
        Ok(Stored::Deduplicated(_)) => {
            report.deduplicated += 1;
            StoreOutcome::Deduplicated
        }
        Ok(Stored::Oversize) => {
            report.oversize += 1;
            return Ok(StoreOutcome::Oversize);
        }
        // `store_bytes_as` names the format itself, so neither comes back; counted if one ever does.
        Ok(Stored::UnknownFormat | Stored::Gone) => {
            report.failed += 1;
            return Ok(StoreOutcome::Failed);
        }
        Err(error) => {
            report.failed += 1;
            file["error"] = json!(format!("{error:#}"));
            return Ok(StoreOutcome::Failed);
        }
    };
    let entry = report
        .by_format
        .entry(format.extension().to_owned())
        .or_default();
    entry.0 += 1;
    entry.1 += bytes.len() as u64;
    Ok(outcome)
}

fn line(
    notice: i64,
    requested_at: DateTime<Utc>,
    http_status: Option<u16>,
    outcome: ReadOutcome,
    error: Option<&str>,
    body: JsonValue,
) -> JsonValue {
    json!({
        "notice_sequence": notice,
        "requested_at_utc": requested_at.to_rfc3339(),
        "http_status": http_status,
        "read_outcome": outcome,
        "error": error,
        "body": body,
    })
}

/// Why a run ended before its frontier did, if it did.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Stop {
    /// `run_time_budget_seconds` passed; the next run continues.
    TimeBudget,
    /// `stop_after_consecutive_failures` details failed in a row: the site is not answering.
    SiteFailing,
}

pub(crate) fn gzip_lines(lines: &[JsonValue]) -> anyhow::Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    for line in lines {
        serde_json::to_writer(&mut encoder, line)?;
        encoder.write_all(b"\n")?;
    }
    Ok(encoder.finish()?)
}

pub(crate) fn plan(
    detail: &NoticeDetail,
    bytes: Vec<u8>,
    run: IngestionRunId,
    at: DateTime<Utc>,
    batch: usize,
) -> anyhow::Result<PublicDataBulkFilePlan> {
    let file_id = format!(
        "{}-{}-{batch:03}",
        detail.dataset_slug.replace('_', "-"),
        at.format("%Y%m%dt%H%M%Sz")
    );
    plan_public_data_bulk_file(PublicDataBulkFilePlanInput {
        source_slug: &source_slug(PROVIDER, &detail.dataset_slug)?,
        ingest_date: at.date_naive(),
        ingestion_run_id: run,
        identity: PublicDataBulkFileIdentity {
            operation: detail.operation.clone(),
            provider_file_period: None,
            provider_snapshot_date: Some(at.date_naive()),
            provider_file_id: file_id.clone(),
            provider_file_name: format!("{file_id}.gz"),
            provider_updated_at: None,
        },
        raw_payload: bytes,
        content_type: "application/gzip".to_owned(),
    })
    .with_context(|| {
        format!(
            "failed to plan the Bronze object for {}",
            detail.dataset_slug
        )
    })
}

/// Commits one batch of lines as one Bronze object and returns its key.
async fn persist(
    contract: &SourceContract,
    lines: &[JsonValue],
    at: DateTime<Utc>,
    batch: usize,
    uow: &dyn BronzeIngestUnitOfWork,
    storage: &dyn ObjectStorageService,
) -> anyhow::Result<String> {
    let detail = &contract.notice_detail;
    let bytes = gzip_lines(lines)?;
    let writer = BronzeObjectStorageWriter::new(storage);
    let source = uow
        .upsert_source_catalog_entry(&source_catalog_entry(contract, at)?)
        .await
        .context("failed to upsert the ILIS notice detail source")?;
    let run = uow
        .create_ingestion_run(&IngestionRun {
            id: IngestionRunId::new(Uuid::new_v4()),
            source_catalog_id: source.id,
            trigger: IngestionTrigger::Scheduled,
            status: IngestionRunStatus::Running,
            request_params: json!({"operation": detail.operation, "batch": batch}),
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
        .context("failed to create the ILIS notice detail run")?;
    let plan = plan(detail, bytes, run.id, at, batch)?;
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
                record: BronzeObject {
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
                    logical_record_count: u64::try_from(lines.len()).ok(),
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
                },
            },
        )
        .await
        .with_context(|| format!("failed to commit {}", plan.object_key.as_str()))?;
    ensure!(
        outcome.checksum_sha256 == plan.checksum_sha256,
        "Bronze holds different bytes at {}",
        plan.object_key.as_str()
    );
    uow.complete_ingestion_run(CompleteIngestionRunCommand {
        id: run.id,
        status: IngestionRunStatus::Succeeded,
        finished_at: Utc::now(),
        logical_records_seen: u64::try_from(lines.len()).unwrap_or(u64::MAX),
        objects_written: 1,
        error_message: None,
    })
    .await
    .context("failed to complete the ILIS notice detail run")?;
    Ok(plan.object_key.as_str().to_owned())
}

fn source_catalog_entry(
    contract: &SourceContract,
    now: DateTime<Utc>,
) -> anyhow::Result<SourceCatalogEntry> {
    let detail = &contract.notice_detail;
    Ok(SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: source_slug(PROVIDER, &detail.dataset_slug)?,
        name: "ILIS 산업단지 고시 상세·첨부".to_owned(),
        provider: PROVIDER.to_owned(),
        dataset_name: detail.dataset_slug.clone(),
        base_url: Some(contract.base_url.clone()),
        auth_kind: SourceAuthKind::NoAuth,
        payload_format: SourcePayloadFormat::Json,
        license_name: None,
        license_url: None,
        terms_url: None,
        collection_frequency: Some(
            "on the industrial_complex_notice_collect schedule (orchestration/jobs.v1.json)"
                .to_owned(),
        ),
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    })
}

struct Config {
    base_uri: String,
    live_write: bool,
    output_dir: PathBuf,
}

impl Config {
    fn from_env(contract: &SourceContract) -> anyhow::Result<Self> {
        Ok(Self {
            base_uri: optional_env_value(&format!("{PREFIX}_BASE_URI"))?
                .unwrap_or_else(|| contract.base_url.clone()),
            live_write: matches!(
                optional_env_value(&format!("{PREFIX}_LIVE_WRITE"))?
                    .map(|value| value.trim().to_ascii_lowercase())
                    .as_deref(),
                Some("1" | "true" | "yes")
            ),
            output_dir: PathBuf::from(required_env_value(&format!("{PREFIX}_OUTPUT_DIR"))?),
        })
    }
}

/// Collects the owed notices and their attachments (root ADR-0484). The last stdout lines are
/// `industrial-complex-notice-summary-json {json}` and `foundation-job-outcome changed|unchanged`.
///
/// # Errors
/// The list could not be read, the lakehouse's last reads could not be read, a Bronze commit
/// failed, or every detail asked failed.
pub async fn run() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let config = Config::from_env(&contract)?;
    let client = IlisIndustrialComplexApiClient::new(&IlisIndustrialComplexApiConfig {
        base_uri: config.base_uri.clone(),
        user_agent: USER_AGENT.to_owned(),
    })?;
    let spacing = ProviderRequestSpacing::try_new(
        Duration::from_millis(contract.request_spacing_ms).max(MIN_REQUEST_SPACING),
    )?;
    let prefix = attachment_prefix(&contract.attachments)?;
    let started_at = Utc::now();
    let deadline =
        Instant::now() + Duration::from_secs(contract.notice_detail.run_time_budget_seconds);

    let mut asker = Asker {
        client: &client,
        spacing,
        requests: 0,
    };
    asker.pace().await;
    let list = client
        .fetch_page(
            &contract.notice_list.path,
            1,
            contract.notice_list.page_size,
        )
        .await
        .context("failed to fetch the ILIS notice list")?;
    let listed = listed_notices(&contract.notice_list, &list.payload)?;
    let settled = settled_from_lakehouse(&contract.notice_detail).await?;
    let owed = frontier(&listed, &settled, contract.notice_detail.per_run_limit);

    let live = if config.live_write {
        live_write_target_preflight()
            .context("ILIS notice collection live-write target preflight failed")?;
        let pool = PgPool::connect(&required_env_value("DATABASE_URL")?)
            .await
            .context("failed to connect to the database for the ILIS notice collection")?;
        let storage = live_write_bronze_object_storage_from_env()
            .await
            .context("failed to configure object storage for the ILIS notice collection")?;
        Some((PgBronzeIngestUnitOfWork::new(pool), storage))
    } else {
        None
    };

    let mut report = AttachmentReport::default();
    let mut counts = std::collections::BTreeMap::<&str, u64>::new();
    let mut objects = Vec::new();
    let mut batch = Vec::new();
    let mut consecutive_failures = 0_u32;
    let mut stop = None;
    for notice in &owed {
        if Instant::now() >= deadline {
            stop = Some(Stop::TimeBudget);
            break;
        }
        if consecutive_failures >= contract.notice_detail.stop_after_consecutive_failures {
            stop = Some(Stop::SiteFailing);
            break;
        }
        let storage = live
            .as_ref()
            .map(|(_, storage)| storage.as_ref() as &dyn ObjectStorageService);
        let (outcome, line) = read_notice(
            &contract,
            &mut asker,
            *notice,
            &prefix,
            storage,
            &mut report,
        )
        .await?;
        consecutive_failures = if outcome == ReadOutcome::Failed {
            consecutive_failures + 1
        } else {
            0
        };
        *counts
            .entry(match outcome {
                ReadOutcome::Read => "read",
                ReadOutcome::Incomplete => "incomplete",
                ReadOutcome::Failed => "failed",
            })
            .or_default() += 1;
        batch.push(line);
        if batch.len() >= contract.notice_detail.commit_every {
            objects
                .push(flush(&contract, &config, &live, &batch, started_at, objects.len()).await?);
            batch.clear();
        }
    }
    if !batch.is_empty() {
        objects.push(flush(&contract, &config, &live, &batch, started_at, objects.len()).await?);
    }

    let asked: u64 = counts.values().sum();
    let summary = json!({
        "schema_version": "foundation-platform.industrial_complex_notice_collect.v1",
        "started_at_utc": started_at.to_rfc3339(),
        "live_write": config.live_write,
        "listed": listed.len(),
        "settled_before": settled.len(),
        "owed_this_run": owed.len(),
        "notices": counts,
        "attachments": report,
        "requests_sent": asker.requests,
        "stopped": stop,
        "objects": objects,
    });
    println!("industrial-complex-notice-summary-json {summary}");
    if asked > 0 && counts.get("failed").copied().unwrap_or(0) == asked {
        bail!("every one of the {asked} notice details asked failed: the site is not answering");
    }
    println!(
        "foundation-job-outcome {}",
        if objects.is_empty() || !config.live_write {
            "unchanged"
        } else {
            "changed"
        }
    );
    Ok(())
}

async fn flush(
    contract: &SourceContract,
    config: &Config,
    live: &Option<(PgBronzeIngestUnitOfWork, Box<dyn ObjectStorageService>)>,
    lines: &[JsonValue],
    at: DateTime<Utc>,
    batch: usize,
) -> anyhow::Result<String> {
    if let Some((uow, storage)) = live {
        return persist(contract, lines, at, batch, uow, storage.as_ref()).await;
    }
    // A dry run keeps the batch on disk for an operator to read.
    let plan = plan(
        &contract.notice_detail,
        gzip_lines(lines)?,
        IngestionRunId::new(Uuid::new_v4()),
        at,
        batch,
    )?;
    let name = plan
        .object_key
        .as_str()
        .rsplit('/')
        .next()
        .context("a Bronze object key has a leaf")?
        .to_owned();
    std::fs::create_dir_all(config.output_dir.join("objects"))?;
    std::fs::write(
        config.output_dir.join("objects").join(&name),
        &plan.raw_payload,
    )?;
    Ok(format!("objects/{name}"))
}

#[cfg(test)]
mod tests;
