//! Reconcile path: rebuilds a terminal run's catalog metadata from immutable Bronze objects.
//!
//! Re-reads the Bronze objects an earlier run wrote to object storage, re-plans them, and repairs
//! any missing Bronze metadata or schema profiles before completing the run as Succeeded. Used to
//! recover runs whose payloads landed but whose metadata write failed.

use std::collections::BTreeMap;

use anyhow::{bail, Context};
use async_trait::async_trait;
use chrono::Utc;
use collection_application::ports::{
    BronzeIngestRepository, BronzeIngestUnitOfWork, CompleteIngestionRunCommand,
};
use collection_application::{
    build_building_register_bronze_object_key, plan_building_register_bronze_page,
    BuildingRegisterBronzePagePlan, BuildingRegisterBronzePagePlanInput,
    BuildingRegisterPageRequest,
};
use collection_domain::{BronzeObject, IngestionRun, IngestionRunStatus, SourceCatalogEntry};
use foundation_outbox::R2ObjectStorage;
use foundation_shared_kernel::ids::{IngestionRunId, SourceCatalogId};
use serde_json::Value as JsonValue;

use super::config::BuildingRegisterSourceIdentity;
use super::model::{bronze_object, schema_profiles_for_plans, total_logical_record_count};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BuildingRegisterReconcileReport {
    pub(super) run_id: IngestionRunId,
    pub(super) objects_expected: u64,
    pub(super) objects_repaired: u64,
    pub(super) schema_profiles_upserted: u64,
    pub(super) logical_records_seen: u64,
}

#[async_trait]
pub(crate) trait BuildingRegisterBronzeObjectReader: Send + Sync {
    async fn read_object_bytes(&self, key: &str) -> anyhow::Result<Vec<u8>>;
}

#[async_trait]
impl BuildingRegisterBronzeObjectReader for R2ObjectStorage {
    async fn read_object_bytes(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        self.get_object_bytes(key)
            .await
            .map_err(anyhow::Error::from)
    }
}

pub(super) async fn reconcile_building_register_run_with_adapters<Repo, Uow, Reader>(
    source_identity: &BuildingRegisterSourceIdentity,
    run_id: IngestionRunId,
    repo: &Repo,
    uow: &Uow,
    reader: &Reader,
) -> anyhow::Result<BuildingRegisterReconcileReport>
where
    Repo: BronzeIngestRepository + ?Sized,
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Reader: BuildingRegisterBronzeObjectReader + ?Sized,
{
    let (source, run, existing_object_keys) =
        load_reconcile_context(source_identity, run_id, repo).await?;
    let request_window = BuildingRegisterReconcileRequestWindow::from_run(&run)?;
    let plans = plan_existing_bronze_pages(source_identity, &run, &request_window, reader).await?;
    let now = Utc::now();
    let objects_repaired =
        repair_missing_bronze_objects(source.id, &run, now, &plans, &existing_object_keys, uow)
            .await?;
    let schema_profiles_upserted =
        repair_schema_profiles(source.id, &run, now, &plans, uow).await?;

    let completed = uow
        .complete_ingestion_run(CompleteIngestionRunCommand {
            id: run.id,
            status: IngestionRunStatus::Succeeded,
            finished_at: Utc::now(),
            logical_records_seen: total_logical_record_count(&plans),
            objects_written: plans.len() as u64,
            error_message: None,
        })
        .await
        .with_context(|| format!("failed to complete reconciled ingestion run {run_id}"))?;

    Ok(BuildingRegisterReconcileReport {
        run_id: completed.id,
        objects_expected: plans.len() as u64,
        objects_repaired,
        schema_profiles_upserted,
        logical_records_seen: completed.logical_records_seen,
    })
}

async fn load_reconcile_context<Repo>(
    source_identity: &BuildingRegisterSourceIdentity,
    run_id: IngestionRunId,
    repo: &Repo,
) -> anyhow::Result<(
    SourceCatalogEntry,
    IngestionRun,
    BTreeMap<String, BronzeObject>,
)>
where
    Repo: BronzeIngestRepository + ?Sized,
{
    let source = repo
        .find_source_catalog_by_slug(&source_identity.source_slug)
        .await
        .context("failed to load building-register source catalog entry")?
        .with_context(|| {
            format!(
                "building-register source catalog entry not found: {}",
                source_identity.source_slug
            )
        })?;
    let run = repo
        .find_ingestion_run(run_id)
        .await
        .with_context(|| format!("failed to load building-register ingestion run {run_id}"))?
        .with_context(|| format!("building-register ingestion run not found: {run_id}"))?;
    validate_reconcile_run(&source, &run)?;
    let existing_objects = repo
        .list_bronze_objects_by_run(run.id)
        .await
        .with_context(|| format!("failed to list Bronze objects for run {run_id}"))?;
    let existing_objects_by_key = bronze_objects_by_key(existing_objects)?;

    Ok((source, run, existing_objects_by_key))
}

fn validate_reconcile_run(source: &SourceCatalogEntry, run: &IngestionRun) -> anyhow::Result<()> {
    if run.source_catalog_id != source.id {
        bail!(
            "ingestion run {} belongs to source {}, not {}",
            run.id,
            run.source_catalog_id,
            source.id
        );
    }
    if matches!(
        run.status,
        IngestionRunStatus::Planned | IngestionRunStatus::Running
    ) {
        bail!(
            "building-register ingestion run {} is {}, only terminal runs can be reconciled",
            run.id,
            run.status.wire_name()
        );
    }
    Ok(())
}

async fn plan_existing_bronze_pages<Reader>(
    source_identity: &BuildingRegisterSourceIdentity,
    run: &IngestionRun,
    request_window: &BuildingRegisterReconcileRequestWindow,
    reader: &Reader,
) -> anyhow::Result<Vec<BuildingRegisterBronzePagePlan>>
where
    Reader: BuildingRegisterBronzeObjectReader + ?Sized,
{
    let expected_requests = request_window.page_requests()?;
    let mut plans = Vec::with_capacity(expected_requests.len());
    for request in expected_requests {
        let object_key =
            build_building_register_bronze_object_key(&source_identity.source_slug, &request)
                .with_context(|| {
                    format!(
                        "failed to build expected building-register Bronze object key for page {}",
                        request.page_no
                    )
                })?;
        let raw_payload = reader
            .read_object_bytes(object_key.as_str())
            .await
            .with_context(|| {
                format!(
                    "failed to read existing building-register Bronze object {}",
                    object_key.as_str()
                )
            })?;
        let payload = serde_json::from_slice::<JsonValue>(&raw_payload).with_context(|| {
            format!(
                "building-register Bronze object {} is not valid JSON",
                object_key.as_str()
            )
        })?;
        plans.push(plan_reconciled_bronze_page(
            source_identity,
            run,
            request,
            raw_payload,
            payload,
            object_key.as_str(),
        )?);
    }
    Ok(plans)
}

fn plan_reconciled_bronze_page(
    source_identity: &BuildingRegisterSourceIdentity,
    run: &IngestionRun,
    request: BuildingRegisterPageRequest,
    raw_payload: Vec<u8>,
    payload: JsonValue,
    object_key: &str,
) -> anyhow::Result<BuildingRegisterBronzePagePlan> {
    plan_building_register_bronze_page(BuildingRegisterBronzePagePlanInput {
        source_slug: &source_identity.source_slug,
        ingest_date: run.started_at.date_naive(),
        ingestion_run_id: run.id,
        request,
        raw_payload,
        payload,
    })
    .with_context(|| format!("failed to re-plan building-register Bronze object {object_key}"))
}

async fn repair_missing_bronze_objects<Uow>(
    source_id: SourceCatalogId,
    run: &IngestionRun,
    now: chrono::DateTime<Utc>,
    plans: &[BuildingRegisterBronzePagePlan],
    existing_objects_by_key: &BTreeMap<String, BronzeObject>,
    uow: &Uow,
) -> anyhow::Result<u64>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
{
    let mut objects_repaired = 0;
    for plan in plans {
        if let Some(existing) = existing_objects_by_key.get(plan.object_key.as_str()) {
            validate_existing_bronze_object(plan, existing)?;
            continue;
        }
        uow.record_bronze_object(&bronze_object(source_id, run.id, now, plan))
            .await
            .with_context(|| {
                format!(
                    "failed to repair building-register Bronze object metadata: {}",
                    plan.object_key.as_str()
                )
            })?;
        objects_repaired += 1;
    }
    Ok(objects_repaired)
}

fn bronze_objects_by_key(
    objects: Vec<BronzeObject>,
) -> anyhow::Result<BTreeMap<String, BronzeObject>> {
    let mut objects_by_key = BTreeMap::new();
    for object in objects {
        let key = object.object_key.as_str().to_owned();
        if objects_by_key.insert(key.clone(), object).is_some() {
            bail!("duplicate Bronze object metadata for object key {key}");
        }
    }
    Ok(objects_by_key)
}

fn validate_existing_bronze_object(
    plan: &BuildingRegisterBronzePagePlan,
    existing: &BronzeObject,
) -> anyhow::Result<()> {
    let mut mismatches = Vec::new();
    if existing.checksum_sha256 != plan.checksum_sha256 {
        mismatches.push("checksum_sha256");
    }
    if existing.size_bytes != plan.size_bytes {
        mismatches.push("size_bytes");
    }
    if existing.logical_record_count != Some(plan.logical_record_count) {
        mismatches.push("logical_record_count");
    }
    if existing.source_partition_key.as_deref() != Some(plan.source_partition_key.as_str()) {
        mismatches.push("source_partition_key");
    }
    if mismatches.is_empty() {
        return Ok(());
    }

    bail!(
        "existing building-register Bronze object metadata mismatch for {}: {}",
        plan.object_key.as_str(),
        mismatches.join(", ")
    )
}

async fn repair_schema_profiles<Uow>(
    source_id: SourceCatalogId,
    run: &IngestionRun,
    now: chrono::DateTime<Utc>,
    plans: &[BuildingRegisterBronzePagePlan],
    uow: &Uow,
) -> anyhow::Result<u64>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
{
    let schema_profiles = schema_profiles_for_plans(source_id, run.id, now, plans);
    let schema_profiles_upserted = schema_profiles.len() as u64;
    for profile in schema_profiles {
        uow.upsert_schema_profile(&profile)
            .await
            .context("failed to repair building-register schema profile")?;
    }
    Ok(schema_profiles_upserted)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BuildingRegisterReconcileRequestWindow {
    operation: String,
    sigungu_cd: String,
    bjdong_cd: String,
    start_page_no: u32,
    num_of_rows: u32,
    pages_planned: u32,
}

impl BuildingRegisterReconcileRequestWindow {
    fn from_run(run: &IngestionRun) -> anyhow::Result<Self> {
        let params = &run.request_params;
        let window = Self {
            operation: json_string_field(params, "operation")?,
            sigungu_cd: json_string_field(params, "sigunguCd")?,
            bjdong_cd: json_string_field(params, "bjdongCd")?,
            start_page_no: json_u32_field(params, "startPageNo")?,
            num_of_rows: json_u32_field(params, "numOfRows")?,
            pages_planned: json_u32_field(params, "pagesPlanned")?,
        };
        if window.start_page_no == 0 || window.num_of_rows == 0 || window.pages_planned == 0 {
            bail!(
                "building-register ingestion run {} has non-positive request window fields",
                run.id
            );
        }
        Ok(window)
    }

    fn page_requests(&self) -> anyhow::Result<Vec<BuildingRegisterPageRequest>> {
        super::plan::page_requests_for_batch(
            &BuildingRegisterPageRequest {
                operation: self.operation.clone(),
                sigungu_cd: self.sigungu_cd.clone(),
                bjdong_cd: self.bjdong_cd.clone(),
                page_no: self.start_page_no,
                num_of_rows: self.num_of_rows,
            },
            self.pages_planned,
        )
    }
}

fn json_string_field(params: &JsonValue, field: &'static str) -> anyhow::Result<String> {
    params
        .get(field)
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
        .with_context(|| format!("building-register request_params.{field} must be a string"))
}

fn json_u32_field(params: &JsonValue, field: &'static str) -> anyhow::Result<u32> {
    let value = params
        .get(field)
        .with_context(|| format!("building-register request_params.{field} is required"))?;
    match value {
        JsonValue::Number(number) => number
            .as_u64()
            .and_then(|raw| u32::try_from(raw).ok())
            .with_context(|| format!("building-register request_params.{field} must fit in u32")),
        JsonValue::String(raw) => raw
            .parse::<u32>()
            .with_context(|| format!("building-register request_params.{field} must be u32")),
        _ => bail!("building-register request_params.{field} must be a number or numeric string"),
    }
}
