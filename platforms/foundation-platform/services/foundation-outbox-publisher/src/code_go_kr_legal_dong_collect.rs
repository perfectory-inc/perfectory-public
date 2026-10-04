//! code.go.kr 법정동 전체 표, Bronze collection (root ADR-0143; the notice board is dropped by
//! ADR-0144).
//!
//! `code.go.kr` (행정표준코드관리시스템) is an approved provider (`APPROVED_PROVIDER_DOMAINS`, root
//! ADR-0032), so the response lands as a Bronze object with a canonical `source_slug`, a `sha256`,
//! a `catalog.bronze_object` ledger row and the write-once `CreateOnly` policy of the
//! [`BronzeCommitter`]. Nothing here reads the response beyond its HTTP status: the parser lives in
//! `infra/lakehouse/spark/jobs/code_go_kr_legal_dong.py`, and a response it cannot read stops the
//! load there (ADR-0143 §6).
//!
//! One run sends two requests: the table's form page (for the session cookie) and the table itself
//! (`legal_dong_code_table`): every code, current and abolished, with its parent, 생성일 and 폐지일.
//! Only downloaded data is a source. The site's 코드변경안내 notice board is not read at all: change
//! pairs come from the table's dates and the parcel snapshots (ADR-0144). The endpoint,
//! the form fields and the request spacing come from `code-go-kr-legal-dong.contract.json`, the same
//! file the parser reads. The spacing is a floor: a configured value may only make a run slower.
//!
//! A run writes `..._OUTPUT_DIR/manifest.json` naming the object (role, Bronze key, sha256, size)
//! and a local copy of its bytes under `objects/` (the manifest names it relative to itself), which
//! the handoff and the Spark load read and check against the sha256. A dry run (no
//! `..._LIVE_WRITE=1`) writes the same files and commits nothing.

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
use foundation_outbox_publisher::code_go_kr_legal_dong_contract::CONTRACT_JSON;
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use reqwest::header::{CONTENT_TYPE, COOKIE, REFERER, SET_COOKIE};
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

/// Catalog-native provider label. `provider_id` derives `codegokr` from it (root ADR-0032).
const PROVIDER: &str = "code.go.kr";
const PREFIX: &str = "FOUNDATION_PLATFORM_CODE_GO_KR_LEGAL_DONG";
const MANIFEST_SCHEMA: &str = "foundation-platform.code_go_kr_legal_dong_collect.v2";
const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";
const BRONZE_COMMITTER: BronzeCommitter = BronzeCommitter::new();
/// The second half of the Bronze `source_slug`; the endpoint catalog lists it.
pub(crate) const DATASET_SLUG: &str = "legal_dong_code_table";
/// The provider-native operation, recorded in lineage and in the Bronze object key.
pub(crate) const OPERATION: &str = "regCodeL";
const DATASET_NAME: &str = "행정표준코드 법정동 전체 표";
/// The manifest's name for the object, which the handoff reads.
const MANIFEST_ROLE: &str = "full_table";

#[derive(Deserialize)]
struct SourceContract {
    base_url: String,
    request_spacing_seconds: u64,
    request_timeout_seconds: u64,
    user_agent: String,
    full_table: FormEndpoint,
}

#[derive(Deserialize)]
struct FormEndpoint {
    #[serde(default)]
    warmup_path: Option<String>,
    path: String,
    form: BTreeMap<String, String>,
}

fn source_contract() -> anyhow::Result<SourceContract> {
    serde_json::from_str(CONTRACT_JSON).context("code-go-kr-legal-dong.contract.json is not valid")
}

struct CollectConfig {
    base_uri: String,
    request_spacing: ProviderRequestSpacing,
    live_write: bool,
    output_dir: PathBuf,
}

/// The table response, before it is committed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Fetched {
    pub(crate) provider_file_id: String,
    pub(crate) provider_file_name: String,
    pub(crate) content_type: String,
    pub(crate) bytes: Vec<u8>,
}

/// Collects the full table into Bronze.
///
/// # Errors
/// Returns an error when the configuration is invalid, a provider request fails or answers with an
/// empty body, the Bronze commit fails, or the manifest cannot be written.
pub async fn run() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let config = CollectConfig::from_env(&contract)?;
    let started_at = Utc::now();
    std::fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("failed to create {}", config.output_dir.display()))?;

    let fetched = fetch_table(&contract, &config, started_at).await?;
    let object = if config.live_write {
        live_write_target_preflight()
            .context("code.go.kr legal-dong collection live-write target preflight failed")?;
        let pool = PgPool::connect(&required_env_value("DATABASE_URL")?)
            .await
            .context("failed to connect to the database for code.go.kr collection")?;
        let uow = PgBronzeIngestUnitOfWork::new(pool);
        let storage = live_write_bronze_object_storage_from_env()
            .await
            .context("failed to configure object storage for code.go.kr collection")?;
        persist(&config, &fetched, started_at, &uow, storage.as_ref()).await?
    } else {
        let plan = plan(&fetched, IngestionRunId::new(Uuid::new_v4()), started_at)?;
        let local = write_local_copy(&config.output_dir, &plan)?;
        manifest_object(&fetched, &plan, local, false)
    };
    let manifest = manifest_json(&config, &contract, started_at, &object);
    let path = config.output_dir.join("manifest.json");
    write_json_file(&path, &manifest)?;
    tracing::info!(
        live_write = config.live_write,
        object_key = %object.object_key,
        manifest = %path.display(),
        "code.go.kr legal-dong full table collection succeeded"
    );
    Ok(())
}

impl CollectConfig {
    fn from_env(contract: &SourceContract) -> anyhow::Result<Self> {
        let spacing_millis = optional_env_value(&format!("{PREFIX}_REQUEST_SPACING_MS"))?
            .map(|value| value.trim().parse::<u64>())
            .transpose()
            .with_context(|| format!("{PREFIX}_REQUEST_SPACING_MS must be a whole number"))?;
        Ok(Self {
            base_uri: optional_env_value(&format!("{PREFIX}_BASE_URI"))?
                .unwrap_or_else(|| contract.base_url.clone())
                .trim_end_matches('/')
                .to_owned(),
            request_spacing: request_spacing(contract, spacing_millis)?,
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

/// The contract's spacing, or a slower configured one; never faster.
fn request_spacing(
    contract: &SourceContract,
    configured_millis: Option<u64>,
) -> anyhow::Result<ProviderRequestSpacing> {
    let floor = Duration::from_secs(contract.request_spacing_seconds);
    let configured = configured_millis.map_or(floor, Duration::from_millis);
    ProviderRequestSpacing::try_new(configured.max(floor))
}

/// The provider file id of the table: the second it was taken.
///
/// A dated id keeps every day's table as its own object, and a same-day retry (the scheduler
/// retries once) as another, instead of colliding with a different body under one key.
pub(crate) fn dated_file_id(at: DateTime<Utc>) -> String {
    format!("regcode-{}", at.format("%Y%m%dT%H%M%SZ"))
}

/// GET the form page for its session cookie, then POST the table form with it.
async fn fetch_table(
    contract: &SourceContract,
    config: &CollectConfig,
    started_at: DateTime<Utc>,
) -> anyhow::Result<Fetched> {
    let table = &contract.full_table;
    let warmup = table.warmup_path.as_deref().unwrap_or(&table.path);
    let http = reqwest::Client::builder()
        .user_agent(contract.user_agent.clone())
        .timeout(Duration::from_secs(contract.request_timeout_seconds))
        .build()
        .context("failed to build the code.go.kr HTTP client")?;
    let url = |path: &str| format!("{}{path}", config.base_uri);

    config.request_spacing.wait_before_request(0).await;
    let response = http
        .get(url(warmup))
        .send()
        .await
        .with_context(|| format!("failed to open {warmup}"))?;
    ensure!(
        response.status().is_success(),
        "code.go.kr {warmup} returned HTTP {}",
        response.status()
    );
    let cookie = response
        .headers()
        .get_all(SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .filter_map(|value| value.split(';').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>()
        .join("; ");

    config.request_spacing.wait_before_request(1).await;
    let form = table
        .form
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<Vec<_>>();
    let mut request = http
        .post(url(&table.path))
        .header(REFERER, url(warmup))
        .form(&form);
    if !cookie.is_empty() {
        request = request.header(COOKIE, cookie);
    }
    let response = request
        .send()
        .await
        .with_context(|| format!("failed to POST {}", table.path))?;
    ensure!(
        response.status().is_success(),
        "code.go.kr {} returned HTTP {}",
        table.path,
        response.status()
    );
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_owned();
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("failed to read the body of {}", table.path))?
        .to_vec();
    ensure!(
        !bytes.is_empty(),
        "code.go.kr {} answered with an empty body",
        table.path
    );
    let id = dated_file_id(started_at);
    Ok(Fetched {
        provider_file_name: format!("{id}.html"),
        provider_file_id: id,
        content_type,
        bytes,
    })
}

/// The landed (or, in a dry run, planned) object, as the manifest names it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestObject {
    object_key: String,
    checksum_sha256: String,
    size_bytes: u64,
    local_path: PathBuf,
    provider_file_name: String,
    content_type: String,
    written: bool,
}

fn plan(
    item: &Fetched,
    run_id: IngestionRunId,
    at: DateTime<Utc>,
) -> anyhow::Result<PublicDataBulkFilePlan> {
    plan_public_data_bulk_file(PublicDataBulkFilePlanInput {
        source_slug: &source_slug(PROVIDER, DATASET_SLUG)?,
        ingest_date: at.date_naive(),
        ingestion_run_id: run_id,
        identity: PublicDataBulkFileIdentity {
            operation: OPERATION.to_owned(),
            provider_file_period: None,
            provider_snapshot_date: Some(at.date_naive()),
            provider_file_id: item.provider_file_id.clone(),
            provider_file_name: item.provider_file_name.clone(),
            provider_updated_at: None,
        },
        raw_payload: item.bytes.clone(),
        content_type: item.content_type.clone(),
    })
    .with_context(|| {
        format!(
            "failed to plan the Bronze object for {}",
            item.provider_file_id
        )
    })
}

/// Keeps a local copy of the bytes for the handoff, under the object's own file name.
fn write_local_copy(output_dir: &Path, plan: &PublicDataBulkFilePlan) -> anyhow::Result<PathBuf> {
    let name = plan
        .object_key
        .as_str()
        .rsplit('/')
        .next()
        .context("a Bronze object key has a leaf")?;
    let path = output_dir.join("objects").join(name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(&path, &plan.raw_payload)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
}

fn manifest_object(
    item: &Fetched,
    plan: &PublicDataBulkFilePlan,
    local_path: PathBuf,
    written: bool,
) -> ManifestObject {
    ManifestObject {
        object_key: plan.object_key.as_str().to_owned(),
        checksum_sha256: plan.checksum_sha256.clone(),
        size_bytes: plan.size_bytes,
        local_path,
        provider_file_name: item.provider_file_name.clone(),
        content_type: item.content_type.clone(),
        written,
    }
}

async fn persist<Uow, Storage>(
    config: &CollectConfig,
    item: &Fetched,
    at: DateTime<Utc>,
    uow: &Uow,
    storage: &Storage,
) -> anyhow::Result<ManifestObject>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    let writer = BronzeObjectStorageWriter::new(storage);
    let source = uow
        .upsert_source_catalog_entry(&source_catalog_entry(at)?)
        .await
        .with_context(|| format!("failed to upsert the {DATASET_SLUG} source"))?;
    let run = uow
        .create_ingestion_run(&ingestion_run(source.id, at, config))
        .await
        .with_context(|| format!("failed to create the {DATASET_SLUG} run"))?;
    let plan = plan(item, run.id, at)?;
    let record = bronze_object_record(&source, &run, at, item, &plan);
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
    let local = write_local_copy(&config.output_dir, &plan)?;
    uow.complete_ingestion_run(CompleteIngestionRunCommand {
        id: run.id,
        status: IngestionRunStatus::Succeeded,
        finished_at: Utc::now(),
        logical_records_seen: 1,
        objects_written: 1,
        error_message: None,
    })
    .await
    .with_context(|| format!("failed to complete the {DATASET_SLUG} run"))?;
    Ok(manifest_object(item, &plan, local, true))
}

fn bronze_object_record(
    source: &SourceCatalogEntry,
    run: &IngestionRun,
    at: DateTime<Utc>,
    item: &Fetched,
    plan: &PublicDataBulkFilePlan,
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
        provider_file_id: Some(item.provider_file_id.clone()),
        provider_file_name: Some(item.provider_file_name.clone()),
        provider_updated_at: None,
        effective_date: None,
        created_at: at,
    }
}

fn source_catalog_entry(now: DateTime<Utc>) -> anyhow::Result<SourceCatalogEntry> {
    Ok(SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: source_slug(PROVIDER, DATASET_SLUG)?,
        name: DATASET_NAME.to_owned(),
        provider: PROVIDER.to_owned(),
        dataset_name: DATASET_SLUG.to_owned(),
        base_url: Some(source_contract()?.base_url),
        auth_kind: SourceAuthKind::NoAuth,
        payload_format: SourcePayloadFormat::Html,
        license_name: None,
        license_url: None,
        terms_url: None,
        collection_frequency: Some("daily".to_owned()),
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    })
}

fn ingestion_run(
    source_catalog_id: SourceCatalogId,
    now: DateTime<Utc>,
    config: &CollectConfig,
) -> IngestionRun {
    IngestionRun {
        id: IngestionRunId::new(Uuid::new_v4()),
        source_catalog_id,
        trigger: IngestionTrigger::Scheduled,
        status: IngestionRunStatus::Running,
        request_params: json!({
            "operation": OPERATION,
            "objects": 1,
            "request_spacing_ms": config.request_spacing.interval_millis(),
        }),
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

fn manifest_json(
    config: &CollectConfig,
    contract: &SourceContract,
    started_at: DateTime<Utc>,
    object: &ManifestObject,
) -> JsonValue {
    json!({
        "schema_version": MANIFEST_SCHEMA,
        "collected_at_utc": started_at.to_rfc3339(),
        "collection_date": started_at.date_naive().to_string(),
        "live_write": config.live_write,
        "base_uri": config.base_uri,
        "user_agent": contract.user_agent,
        "request_spacing_ms": config.request_spacing.interval_millis(),
        "objects": [json!({
            "role": MANIFEST_ROLE,
            "object_key": object.object_key,
            "checksum_sha256": object.checksum_sha256,
            "size_bytes": object.size_bytes,
            // Relative to the manifest, so a Spark container that mounts the directory elsewhere
            // reads the same file.
            "local_path": object
                .local_path
                .strip_prefix(&config.output_dir)
                .unwrap_or(&object.local_path)
                .to_string_lossy()
                .replace('\\', "/"),
            "provider_file_name": object.provider_file_name,
            "content_type": object.content_type,
            "written": object.written,
        })],
    })
}

#[cfg(test)]
mod tests;
