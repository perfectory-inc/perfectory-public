//! data.go.kr file data (파일 데이터), Bronze collection (root ADR-0280 §4, ADR-0401 §1).
//!
//! `data-go-kr-file-data.contract.json` lists the files; a run collects the ones its job names
//! (`..._DATASETS`). For each, the portal names the current file in a metadata answer (`atchFileId`);
//! the file itself is fetched by that id. A run asks each file's metadata once and fetches the file
//! only when its id differs from the last one landed (`<dataset_slug>/accepted.json` in the state
//! directory), so one file is fetched once, and a new edition is a new object. The file lands in
//! Bronze as received, under its own source (`datagokr__<dataset_slug>`): the Silver loads open it
//! and check its rows. Nothing here reads the CSV.
//!
//! One file's failure does not stop the others (each commits on its own); the run fails at the end
//! naming every file that failed. A run writes `..._OUTPUT_DIR/manifest.json` and a local copy of
//! each file it fetched; its last stdout line says whether it landed a new file.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

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
use collection_infrastructure::PgBronzeIngestUnitOfWork;
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use uuid::Uuid;

use crate::bronze_object_storage::{
    live_write_bronze_object_storage_from_env, live_write_target_preflight,
    BronzeObjectStorageWriter,
};
use crate::provider_request_spacing::ProviderRequestSpacing;
use crate::public_data_control_support::{optional_env_value, required_env_value, write_json_file};

/// `infra/lakehouse/contracts/data-go-kr-file-data.contract.json`, as compiled in.
pub(crate) const CONTRACT_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/data-go-kr-file-data.contract.json");
const PROVIDER: &str = "data.go.kr";
const PREFIX: &str = "FOUNDATION_PLATFORM_DATA_GO_KR_FILE_DATA";
const MANIFEST_SCHEMA: &str = "foundation-platform.data_go_kr_file_data_collect.v1";
const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";
const BRONZE_COMMITTER: BronzeCommitter = BronzeCommitter::new();
const ZIP_MAGIC: &[u8] = b"PK\x03\x04";

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Metadata {
    pub(crate) path: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Download {
    pub(crate) path: String,
    pub(crate) query: BTreeMap<String, String>,
}

/// How a file arrives: a zip archive, or one CSV as it is.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum FileFormat {
    Zip,
    Csv,
}

/// One file of the contract. The Silver-side fields (header, bounds, keys) are not read here.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ContractFile {
    /// The key it is listed under: the catalog's `dataset_slug`.
    #[serde(skip)]
    pub(crate) dataset_slug: String,
    pub(crate) operation: String,
    pub(crate) name_ko: String,
    pub(crate) published: String,
    pub(crate) metadata_query: BTreeMap<String, String>,
    pub(crate) format: FileFormat,
    /// A file too large for the contract-wide timeout states its own.
    pub(crate) request_timeout_seconds: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SourceContract {
    pub(crate) base_url: String,
    pub(crate) user_agent: String,
    pub(crate) request_spacing_seconds: u64,
    pub(crate) request_timeout_seconds: u64,
    pub(crate) metadata: Metadata,
    pub(crate) download: Download,
    pub(crate) files: BTreeMap<String, ContractFile>,
}

pub(crate) fn source_contract() -> anyhow::Result<SourceContract> {
    let mut contract: SourceContract = serde_json::from_str(CONTRACT_JSON)
        .context("data-go-kr-file-data.contract.json is not valid")?;
    for (slug, file) in &mut contract.files {
        file.dataset_slug.clone_from(slug);
    }
    Ok(contract)
}

/// The contract's files a job names, in the job's order; an unknown or repeated name refuses.
pub(crate) fn selected<'c>(
    contract: &'c SourceContract,
    names: &str,
) -> anyhow::Result<Vec<&'c ContractFile>> {
    let mut picked: Vec<&ContractFile> = Vec::new();
    for name in names
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        let file = contract
            .files
            .get(name)
            .with_context(|| format!("the file-data contract has no file {name:?}"))?;
        ensure!(
            !picked.iter().any(|seen| seen.dataset_slug == name),
            "{PREFIX}_DATASETS names {name:?} twice"
        );
        picked.push(file);
    }
    ensure!(!picked.is_empty(), "{PREFIX}_DATASETS names no file");
    Ok(picked)
}

/// The file the metadata answer names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Announced {
    pub(crate) attach_file_id: String,
    pub(crate) updated_at: Option<String>,
}

/// Reads the metadata answer: `atchFileId` names the current file.
pub(crate) fn read_metadata(body: &str) -> anyhow::Result<Announced> {
    let value: JsonValue =
        serde_json::from_str(body).context("the file metadata answer is not JSON")?;
    let attach_file_id = value["atchFileId"]
        .as_str()
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .context("the file metadata answer names no atchFileId")?;
    ensure!(
        attach_file_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_'),
        "the file metadata answer names an unexpected atchFileId {attach_file_id:?}"
    );
    Ok(Announced {
        attach_file_id: attach_file_id.to_owned(),
        updated_at: value["dataSetFileDetailInfo"]["updtDt"]
            .as_str()
            .map(str::to_owned),
    })
}

/// The Bronze leaf of a file: its dataset and portal id, lowercased into the key's alphabet.
pub(crate) fn provider_file_id(file: &ContractFile, announced: &Announced) -> String {
    format!(
        "{}-{}",
        file.dataset_slug.replace('_', "-"),
        announced
            .attach_file_id
            .to_ascii_lowercase()
            .replace('_', "-")
    )
}

fn extension(file: &ContractFile) -> &'static str {
    match file.format {
        FileFormat::Zip => "zip",
        FileFormat::Csv => "csv",
    }
}

fn state_path(state_dir: &Path, file: &ContractFile) -> PathBuf {
    state_dir.join(&file.dataset_slug).join("accepted.json")
}

fn last_accepted(state_dir: &Path, file: &ContractFile) -> anyhow::Result<Option<String>> {
    let path = state_path(state_dir, file);
    if !path.exists() {
        return Ok(None);
    }
    let value: JsonValue = serde_json::from_slice(
        &std::fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?,
    )
    .with_context(|| format!("{} is not JSON", path.display()))?;
    Ok(value["attach_file_id"].as_str().map(str::to_owned))
}

struct Config {
    base_uri: String,
    request_spacing: ProviderRequestSpacing,
    live_write: bool,
    output_dir: PathBuf,
    state_dir: PathBuf,
    datasets: String,
}

impl Config {
    fn from_env(contract: &SourceContract) -> anyhow::Result<Self> {
        Ok(Self {
            base_uri: optional_env_value(&format!("{PREFIX}_BASE_URI"))?
                .unwrap_or_else(|| contract.base_url.clone())
                .trim_end_matches('/')
                .to_owned(),
            request_spacing: ProviderRequestSpacing::try_new(Duration::from_secs(
                contract.request_spacing_seconds,
            ))?,
            live_write: matches!(
                optional_env_value(&format!("{PREFIX}_LIVE_WRITE"))?
                    .map(|value| value.trim().to_ascii_lowercase())
                    .as_deref(),
                Some("1" | "true" | "yes")
            ),
            output_dir: PathBuf::from(required_env_value(&format!("{PREFIX}_OUTPUT_DIR"))?),
            state_dir: PathBuf::from(required_env_value(&format!("{PREFIX}_STATE_DIR"))?),
            datasets: required_env_value(&format!("{PREFIX}_DATASETS"))?,
        })
    }
}

/// Refuses a download that is not the file: a zip must open as one, and a CSV is never markup
/// (the portal answers a login or notice page with HTTP 200).
fn check_download(file: &ContractFile, bytes: &[u8]) -> anyhow::Result<()> {
    match file.format {
        FileFormat::Zip => ensure!(
            bytes.starts_with(ZIP_MAGIC),
            "the {} download is not a zip ({} bytes)",
            file.dataset_slug,
            bytes.len()
        ),
        FileFormat::Csv => {
            let head = bytes.trim_ascii_start();
            ensure!(
                !head.is_empty() && !head.starts_with(b"<") && !head.starts_with(ZIP_MAGIC),
                "the {} download is not a CSV ({} bytes)",
                file.dataset_slug,
                bytes.len()
            );
        }
    }
    Ok(())
}

/// Asks one file's metadata, and the file when it is new. `None` when the last landed file is
/// current. `requests` counts this run's requests, so the spacing holds across files.
pub(crate) async fn fetch(
    contract: &SourceContract,
    file: &ContractFile,
    base_uri: &str,
    spacing: ProviderRequestSpacing,
    requests: &mut usize,
    last: Option<&str>,
) -> anyhow::Result<Option<(Announced, Vec<u8>)>> {
    let http = reqwest::Client::builder()
        .user_agent(contract.user_agent.clone())
        .timeout(Duration::from_secs(
            file.request_timeout_seconds
                .unwrap_or(contract.request_timeout_seconds),
        ))
        .build()
        .context("failed to build the data.go.kr HTTP client")?;
    spacing.wait_before_request(*requests).await;
    *requests += 1;
    let response = http
        .get(format!("{base_uri}{}", contract.metadata.path))
        .query(&file.metadata_query)
        .send()
        .await
        .context("the file metadata request failed")?;
    ensure!(
        response.status().is_success(),
        "the file metadata answered HTTP {}",
        response.status()
    );
    let announced = read_metadata(&response.text().await?)?;
    if last == Some(announced.attach_file_id.as_str()) {
        return Ok(None);
    }
    spacing.wait_before_request(*requests).await;
    *requests += 1;
    let mut query = contract.download.query.clone();
    query.insert("atchFileId".to_owned(), announced.attach_file_id.clone());
    let response = http
        .get(format!("{base_uri}{}", contract.download.path))
        .query(&query)
        .send()
        .await
        .context("the file download failed")?;
    ensure!(
        response.status().is_success(),
        "the file download answered HTTP {}",
        response.status()
    );
    let bytes = Vec::from(response.bytes().await?);
    check_download(file, &bytes)?;
    Ok(Some((announced, bytes)))
}

pub(crate) fn plan(
    file: &ContractFile,
    announced: &Announced,
    bytes: Vec<u8>,
    run_id: IngestionRunId,
    at: DateTime<Utc>,
) -> anyhow::Result<PublicDataBulkFilePlan> {
    let file_id = provider_file_id(file, announced);
    plan_public_data_bulk_file(PublicDataBulkFilePlanInput {
        source_slug: &source_slug(PROVIDER, &file.dataset_slug)?,
        ingest_date: at.date_naive(),
        ingestion_run_id: run_id,
        identity: PublicDataBulkFileIdentity {
            operation: file.operation.clone(),
            provider_file_period: None,
            provider_snapshot_date: Some(at.date_naive()),
            provider_file_name: format!("{file_id}.{}", extension(file)),
            provider_file_id: file_id,
            provider_updated_at: None,
        },
        raw_payload: bytes,
        content_type: content_type(file).to_owned(),
    })
    .with_context(|| format!("failed to plan the Bronze object for {}", file.dataset_slug))
}

fn content_type(file: &ContractFile) -> &'static str {
    match file.format {
        FileFormat::Zip => "application/zip",
        FileFormat::Csv => "text/csv",
    }
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
    std::fs::create_dir_all(output_dir.join("objects"))?;
    std::fs::write(&path, &plan.raw_payload)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(relative)
}

/// Commits one file: source row, run row, write-once object, ledger row. Returns the object key,
/// checksum and the local copy's path relative to `output_dir`.
#[allow(clippy::too_many_arguments)] // the registered-factory commit's seven, plus which file
pub(crate) async fn persist<Uow, Storage>(
    contract: &SourceContract,
    file: &ContractFile,
    output_dir: &Path,
    announced: &Announced,
    bytes: Vec<u8>,
    at: DateTime<Utc>,
    uow: &Uow,
    storage: &Storage,
) -> anyhow::Result<(String, String, String)>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    let writer = BronzeObjectStorageWriter::new(storage);
    let source = uow
        .upsert_source_catalog_entry(&source_catalog_entry(contract, file, at)?)
        .await
        .with_context(|| format!("failed to upsert the {} source", file.dataset_slug))?;
    let run = uow
        .create_ingestion_run(&ingestion_run(source.id, file, at))
        .await
        .with_context(|| format!("failed to create the {} run", file.dataset_slug))?;
    let mut plan = plan(file, announced, bytes, run.id, at)?;
    // The local copy first, so the payload moves into the commit instead of being copied (the
    // 상가(상권) zip is about 350 MB).
    let local = write_local_copy(output_dir, &plan)?;
    let record = bronze_object_record(&source, &run, at, &plan, file, announced);
    let outcome = BRONZE_COMMITTER
        .commit(
            &writer,
            uow,
            PlannedBronzeObject {
                object_key: plan.object_key.as_str().to_owned(),
                payload: BronzePayload::InMemory(std::mem::take(&mut plan.raw_payload)),
                content_type: plan.content_type.clone(),
                cache_control: BRONZE_CACHE_CONTROL.to_owned(),
                checksum_sha256: plan.checksum_sha256.clone(),
                record,
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
        logical_records_seen: 1,
        objects_written: 1,
        error_message: None,
    })
    .await
    .with_context(|| format!("failed to complete the {} run", file.dataset_slug))?;
    Ok((
        plan.object_key.as_str().to_owned(),
        plan.checksum_sha256.clone(),
        local,
    ))
}

fn bronze_object_record(
    source: &SourceCatalogEntry,
    run: &IngestionRun,
    at: DateTime<Utc>,
    plan: &PublicDataBulkFilePlan,
    file: &ContractFile,
    announced: &Announced,
) -> BronzeObject {
    let file_id = provider_file_id(file, announced);
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
        provider_file_name: Some(format!("{file_id}.{}", extension(file))),
        provider_file_id: Some(file_id),
        provider_updated_at: None,
        effective_date: None,
        created_at: at,
    }
}

fn source_catalog_entry(
    contract: &SourceContract,
    file: &ContractFile,
    now: DateTime<Utc>,
) -> anyhow::Result<SourceCatalogEntry> {
    Ok(SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: source_slug(PROVIDER, &file.dataset_slug)?,
        name: file.name_ko.clone(),
        provider: PROVIDER.to_owned(),
        dataset_name: file.dataset_slug.clone(),
        base_url: Some(contract.base_url.clone()),
        auth_kind: SourceAuthKind::NoAuth,
        payload_format: match file.format {
            FileFormat::Zip => SourcePayloadFormat::Zip,
            FileFormat::Csv => SourcePayloadFormat::Csv,
        },
        license_name: None,
        license_url: None,
        terms_url: None,
        collection_frequency: Some(format!("checked daily, published {}", file.published)),
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    })
}

fn ingestion_run(
    source_catalog_id: SourceCatalogId,
    file: &ContractFile,
    now: DateTime<Utc>,
) -> IngestionRun {
    IngestionRun {
        id: IngestionRunId::new(Uuid::new_v4()),
        source_catalog_id,
        trigger: IngestionTrigger::Scheduled,
        status: IngestionRunStatus::Running,
        request_params: json!({"operation": file.operation, "objects": 1}),
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

struct Live {
    uow: PgBronzeIngestUnitOfWork,
    storage: Box<dyn ObjectStorageService>,
}

/// Lands one file when it is new; `None` when the landed one is current.
async fn collect_one(
    contract: &SourceContract,
    file: &ContractFile,
    config: &Config,
    live: Option<&Live>,
    requests: &mut usize,
    now: DateTime<Utc>,
) -> anyhow::Result<Option<JsonValue>> {
    let last = last_accepted(&config.state_dir, file)?;
    let Some((announced, bytes)) = fetch(
        contract,
        file,
        &config.base_uri,
        config.request_spacing,
        requests,
        last.as_deref(),
    )
    .await?
    else {
        return Ok(None);
    };
    let size = bytes.len();
    let object = if let Some(live) = live {
        let (key, checksum, local) = persist(
            contract,
            file,
            &config.output_dir,
            &announced,
            bytes,
            now,
            &live.uow,
            live.storage.as_ref(),
        )
        .await?;
        let state = state_path(&config.state_dir, file);
        std::fs::create_dir_all(state.parent().context("a state file has a directory")?)?;
        write_json_file(
            &state,
            &json!({"attach_file_id": announced.attach_file_id, "accepted_at_utc": now.to_rfc3339()}),
        )?;
        json!({"object_key": key, "checksum_sha256": checksum, "local_path": local, "written": true})
    } else {
        let plan = plan(
            file,
            &announced,
            bytes,
            IngestionRunId::new(Uuid::new_v4()),
            now,
        )?;
        let local = write_local_copy(&config.output_dir, &plan)?;
        json!({"object_key": plan.object_key.as_str(), "checksum_sha256": plan.checksum_sha256, "local_path": local, "written": false})
    };
    tracing::info!(
        live_write = config.live_write,
        dataset = %file.dataset_slug,
        attach_file_id = %announced.attach_file_id,
        "data.go.kr file collection landed a new edition"
    );
    Ok(Some(json!({
        "dataset_slug": file.dataset_slug,
        "attach_file_id": announced.attach_file_id,
        "provider_updated_at": announced.updated_at,
        "size_bytes": size,
        "object": object,
    })))
}

/// Checks the named files for a new edition and lands each in Bronze.
///
/// # Errors
/// Returns an error when the configuration is invalid, when any file's portal answer is something
/// other than the metadata or the file, a Bronze commit fails, or the manifest or state cannot be
/// written. The other files are still collected first.
pub async fn run() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let config = Config::from_env(&contract)?;
    let files = selected(&contract, &config.datasets)?;
    let now = Utc::now();
    std::fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("failed to create {}", config.output_dir.display()))?;
    std::fs::create_dir_all(&config.state_dir)
        .with_context(|| format!("failed to create {}", config.state_dir.display()))?;
    let live = if config.live_write {
        live_write_target_preflight()
            .context("data.go.kr file collection live-write preflight failed")?;
        let pool = PgPool::connect(&required_env_value("DATABASE_URL")?)
            .await
            .context("failed to connect to the database for data.go.kr file collection")?;
        Some(Live {
            uow: PgBronzeIngestUnitOfWork::new(pool),
            storage: live_write_bronze_object_storage_from_env()
                .await
                .context("failed to configure object storage for data.go.kr file collection")?,
        })
    } else {
        None
    };
    let mut requests = 0;
    let mut landed = Vec::new();
    let mut failed = Vec::new();
    for file in files {
        match collect_one(&contract, file, &config, live.as_ref(), &mut requests, now).await {
            Ok(Some(entry)) => landed.push(entry),
            Ok(None) => {}
            Err(error) => {
                tracing::error!(dataset = %file.dataset_slug, error = %format!("{error:#}"), "data.go.kr file collection failed");
                failed.push(format!("{}: {error:#}", file.dataset_slug));
            }
        }
    }
    write_json_file(
        &config.output_dir.join("manifest.json"),
        &json!({"schema_version": MANIFEST_SCHEMA, "collected_at_utc": now.to_rfc3339(), "files": landed, "failed": failed}),
    )?;
    if !failed.is_empty() {
        bail!(
            "data.go.kr file collection failed for {}",
            failed.join("; ")
        );
    }
    println!(
        "foundation-job-outcome {}",
        if landed.is_empty() {
            "unchanged"
        } else {
            "changed"
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests;
