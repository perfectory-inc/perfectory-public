//! 한국산업단지공단 전국등록공장현황 (data.go.kr file data 15105482), Bronze collection (root ADR-0280).
//!
//! The portal names the current file in a metadata answer (`atchFileId`); the file itself is fetched
//! by that id. A run asks the metadata once and fetches the file only when its id differs from the
//! last one landed (`accepted.json` in the state directory), so one file is fetched once, and a new
//! edition is a new object. The zip lands in Bronze as received: the Silver load
//! (`infra/lakehouse/spark/jobs/data_go_kr_registered_factory.py`) opens it and checks its member.
//! Nothing here reads the CSV.
//!
//! The paths, query fields and spacing come from `data-go-kr-registered-factory.contract.json`, the
//! file the Silver load reads too. A run writes `..._OUTPUT_DIR/manifest.json` and a local copy of
//! the zip; its last stdout line says whether it landed a new file.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{ensure, Context};
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

/// `infra/lakehouse/contracts/data-go-kr-registered-factory.contract.json`, as compiled in.
pub(crate) const CONTRACT_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/data-go-kr-registered-factory.contract.json");
const PROVIDER: &str = "data.go.kr";
const PREFIX: &str = "FOUNDATION_PLATFORM_DATA_GO_KR_REGISTERED_FACTORY";
const MANIFEST_SCHEMA: &str = "foundation-platform.data_go_kr_registered_factory_collect.v1";
const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";
const BRONZE_COMMITTER: BronzeCommitter = BronzeCommitter::new();
const ZIP_MAGIC: &[u8] = b"PK\x03\x04";

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct FileEndpoints {
    pub(crate) metadata_path: String,
    pub(crate) metadata_query: BTreeMap<String, String>,
    pub(crate) download_path: String,
    pub(crate) download_query: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SourceContract {
    pub(crate) base_url: String,
    pub(crate) user_agent: String,
    pub(crate) request_spacing_seconds: u64,
    pub(crate) request_timeout_seconds: u64,
    pub(crate) dataset_slug: String,
    pub(crate) operation: String,
    pub(crate) file: FileEndpoints,
}

pub(crate) fn source_contract() -> anyhow::Result<SourceContract> {
    serde_json::from_str(CONTRACT_JSON)
        .context("data-go-kr-registered-factory.contract.json is not valid")
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

/// The Bronze leaf of a file: its portal id, lowercased into the key's alphabet.
pub(crate) fn provider_file_id(announced: &Announced) -> String {
    format!(
        "registered-factory-{}",
        announced
            .attach_file_id
            .to_ascii_lowercase()
            .replace('_', "-")
    )
}

fn last_accepted(state_dir: &Path) -> anyhow::Result<Option<String>> {
    let path = state_dir.join("accepted.json");
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
        })
    }
}

/// Asks the metadata, and the file when it is new. `None` when the last landed file is current.
pub(crate) async fn fetch(
    contract: &SourceContract,
    base_uri: &str,
    spacing: ProviderRequestSpacing,
    last: Option<&str>,
) -> anyhow::Result<Option<(Announced, Vec<u8>)>> {
    let http = reqwest::Client::builder()
        .user_agent(contract.user_agent.clone())
        .timeout(Duration::from_secs(contract.request_timeout_seconds))
        .build()
        .context("failed to build the data.go.kr HTTP client")?;
    let file = &contract.file;
    spacing.wait_before_request(0).await;
    let response = http
        .get(format!("{base_uri}{}", file.metadata_path))
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
    spacing.wait_before_request(1).await;
    let mut query = file.download_query.clone();
    query.insert("atchFileId".to_owned(), announced.attach_file_id.clone());
    let response = http
        .get(format!("{base_uri}{}", file.download_path))
        .query(&query)
        .send()
        .await
        .context("the file download failed")?;
    ensure!(
        response.status().is_success(),
        "the file download answered HTTP {}",
        response.status()
    );
    let bytes = response.bytes().await?.to_vec();
    ensure!(
        bytes.starts_with(ZIP_MAGIC),
        "the file download is not a zip ({} bytes)",
        bytes.len()
    );
    Ok(Some((announced, bytes)))
}

pub(crate) fn plan(
    contract: &SourceContract,
    announced: &Announced,
    bytes: Vec<u8>,
    run_id: IngestionRunId,
    at: DateTime<Utc>,
) -> anyhow::Result<PublicDataBulkFilePlan> {
    let file_id = provider_file_id(announced);
    plan_public_data_bulk_file(PublicDataBulkFilePlanInput {
        source_slug: &source_slug(PROVIDER, &contract.dataset_slug)?,
        ingest_date: at.date_naive(),
        ingestion_run_id: run_id,
        identity: PublicDataBulkFileIdentity {
            operation: contract.operation.clone(),
            provider_file_period: None,
            provider_snapshot_date: Some(at.date_naive()),
            provider_file_id: file_id.clone(),
            provider_file_name: format!("{file_id}.zip"),
            provider_updated_at: None,
        },
        raw_payload: bytes,
        content_type: "application/zip".to_owned(),
    })
    .context("failed to plan the Bronze object for the registered-factory file")
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

/// Commits the file: source row, run row, write-once object, ledger row. Returns the object key,
/// checksum and the local copy's path relative to `output_dir`.
pub(crate) async fn persist<Uow, Storage>(
    contract: &SourceContract,
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
        .upsert_source_catalog_entry(&source_catalog_entry(contract, at)?)
        .await
        .context("failed to upsert the registered-factory source")?;
    let run = uow
        .create_ingestion_run(&ingestion_run(source.id, contract, at))
        .await
        .context("failed to create the registered-factory run")?;
    let plan = plan(contract, announced, bytes, run.id, at)?;
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
                record: bronze_object_record(&source, &run, at, &plan, announced),
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
        logical_records_seen: 1,
        objects_written: 1,
        error_message: None,
    })
    .await
    .context("failed to complete the registered-factory run")?;
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
    announced: &Announced,
) -> BronzeObject {
    let file_id = provider_file_id(announced);
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
        provider_file_name: Some(format!("{file_id}.zip")),
        provider_file_id: Some(file_id),
        provider_updated_at: None,
        effective_date: None,
        created_at: at,
    }
}

fn source_catalog_entry(
    contract: &SourceContract,
    now: DateTime<Utc>,
) -> anyhow::Result<SourceCatalogEntry> {
    Ok(SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: source_slug(PROVIDER, &contract.dataset_slug)?,
        name: "한국산업단지공단 전국등록공장현황".to_owned(),
        provider: PROVIDER.to_owned(),
        dataset_name: contract.dataset_slug.clone(),
        base_url: Some(contract.base_url.clone()),
        auth_kind: SourceAuthKind::NoAuth,
        payload_format: SourcePayloadFormat::Zip,
        license_name: None,
        license_url: None,
        terms_url: None,
        collection_frequency: Some("checked daily, published yearly".to_owned()),
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    })
}

fn ingestion_run(
    source_catalog_id: SourceCatalogId,
    contract: &SourceContract,
    now: DateTime<Utc>,
) -> IngestionRun {
    IngestionRun {
        id: IngestionRunId::new(Uuid::new_v4()),
        source_catalog_id,
        trigger: IngestionTrigger::Scheduled,
        status: IngestionRunStatus::Running,
        request_params: json!({"operation": contract.operation, "objects": 1}),
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

/// Checks for a new edition and lands it in Bronze.
///
/// # Errors
/// Returns an error when the configuration is invalid, the portal answers something other than
/// the metadata or a zip, the Bronze commit fails, or the manifest or state cannot be written.
pub async fn run() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let config = Config::from_env(&contract)?;
    let now = Utc::now();
    std::fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("failed to create {}", config.output_dir.display()))?;
    std::fs::create_dir_all(&config.state_dir)
        .with_context(|| format!("failed to create {}", config.state_dir.display()))?;
    let last = last_accepted(&config.state_dir)?;
    let fetched = fetch(
        &contract,
        &config.base_uri,
        config.request_spacing,
        last.as_deref(),
    )
    .await?;
    let Some((announced, bytes)) = fetched else {
        write_json_file(
            &config.output_dir.join("manifest.json"),
            &json!({"schema_version": MANIFEST_SCHEMA, "collected_at_utc": now.to_rfc3339(), "objects": []}),
        )?;
        println!("foundation-job-outcome unchanged");
        return Ok(());
    };
    let size = bytes.len();
    let object = if config.live_write {
        live_write_target_preflight()
            .context("registered-factory collection live-write preflight failed")?;
        let pool = PgPool::connect(&required_env_value("DATABASE_URL")?)
            .await
            .context("failed to connect to the database for registered-factory collection")?;
        let uow = PgBronzeIngestUnitOfWork::new(pool);
        let storage = live_write_bronze_object_storage_from_env()
            .await
            .context("failed to configure object storage for registered-factory collection")?;
        let (key, checksum, local) = persist(
            &contract,
            &config.output_dir,
            &announced,
            bytes,
            now,
            &uow,
            storage.as_ref(),
        )
        .await?;
        json!({"object_key": key, "checksum_sha256": checksum, "local_path": local, "written": true})
    } else {
        let plan = plan(
            &contract,
            &announced,
            bytes,
            IngestionRunId::new(Uuid::new_v4()),
            now,
        )?;
        let local = write_local_copy(&config.output_dir, &plan)?;
        json!({"object_key": plan.object_key.as_str(), "checksum_sha256": plan.checksum_sha256, "local_path": local, "written": false})
    };
    write_json_file(
        &config.output_dir.join("manifest.json"),
        &json!({
            "schema_version": MANIFEST_SCHEMA,
            "collected_at_utc": now.to_rfc3339(),
            "attach_file_id": announced.attach_file_id,
            "provider_updated_at": announced.updated_at,
            "size_bytes": size,
            "objects": [object],
        }),
    )?;
    if config.live_write {
        write_json_file(
            &config.state_dir.join("accepted.json"),
            &json!({"attach_file_id": announced.attach_file_id, "accepted_at_utc": now.to_rfc3339()}),
        )?;
    }
    tracing::info!(
        live_write = config.live_write,
        attach_file_id = %announced.attach_file_id,
        "registered-factory collection landed a new edition"
    );
    println!("foundation-job-outcome changed");
    Ok(())
}

#[cfg(test)]
mod tests;
