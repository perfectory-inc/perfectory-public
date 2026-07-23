//! `VWorld` land-register Bronze ingestion command.

use std::collections::BTreeMap;

use anyhow::{bail, Context};
use chrono::Utc;
use collection_application::ports::BronzeIngestUnitOfWork;
use collection_application::{
    plan_vworld_land_register_bronze_page, VWorldLandRegisterBronzePagePlan,
    VWorldLandRegisterBronzePagePlanInput, VWorldLandRegisterPageRequest,
};
use collection_domain::{
    IngestionRun, IngestionRunStatus, IngestionTrigger, SourceAuthKind, SourceCatalogEntry,
    SourcePayloadFormat,
};
use collection_infrastructure::{
    PgBronzeIngestUnitOfWork, VWorldNedAttributeClient, VWorldNedAttributeConfig,
    VWorldRequestPolicy,
};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use uuid::Uuid;

use crate::bronze_object_storage::bronze_object_storage_from_env;
use crate::bronze_schema_profile::CandidateKeyOverride;
use crate::page_collector::{collect_planned_pages, CollectablePage, PageCollectorLane};
use crate::pagination_guard::assert_page_window_complete;
use crate::provider_request_spacing::ProviderRequestSpacing;
use crate::public_data_control_support::{
    optional_duration_millis_env, optional_duration_seconds_env, optional_env_value,
    optional_positive_u32_env, optional_u32_env, optional_u64_env, required_env_value,
};

const SOURCE_NAME: &str = "VWorld Land Register";
const PROVIDER: &str = "vworld";
/// Catalog-native provider label used by the canonical `source_slug` generator (ADR 0014 D2).
const GENERATOR_PROVIDER: &str = "VWorld";
const DATASET_NAME: &str = "vworld-ned-ladfrl-list";
/// Canonical semantic dataset identity for the VWorld land register (ADR 0014 §6).
const DEFAULT_DATASET_SLUG: &str = "land_register";
const DEFAULT_BASE_URI: &str = "https://api.vworld.kr/ned/data";
const DEFAULT_OPERATION: &str = "ladfrlList";
const DEFAULT_MAX_PAGES: u32 = 1;
const DEFAULT_USER_AGENT: &str = "foundation-outbox-publisher/0.1";
// The Bronze content-type / cache-control constants now live with the shared PageCollector loop
// (ADR 0017), which is the single place that builds the per-page commit input.

/// One fetched `VWorld` land-register page: the compiled Bronze plan plus the RAW page identity +
/// parsed payload the [`BronzeCommitter`] needs to OWN the key-compile (ADR 0016).
///
/// The compiled `plan` drives the page-window completeness assertion, the dry-run summary, and the
/// run-level logical-record / size accounting; the persist stage hands the raw `request` +
/// `raw_payload` + `payload` to the committer, which re-runs the land-register Bronze plan as its
/// owned compile step — the exact mirror of the building-register / real-transaction lanes.
#[derive(Clone, Debug)]
struct VWorldLandRegisterPlannedPage {
    plan: VWorldLandRegisterBronzePagePlan,
    request: VWorldLandRegisterPageRequest,
    raw_payload: Vec<u8>,
    payload: JsonValue,
}

/// Runs one `VWorld` land-register Bronze ingestion batch.
pub async fn run() -> anyhow::Result<()> {
    let config = VWorldLandRegisterIngestConfig::from_env()?;
    let client = VWorldNedAttributeClient::new_with_policy(
        &VWorldNedAttributeConfig {
            base_uri: config.base_uri.clone(),
            api_key: config.api_key.clone(),
            domain: config.domain.clone(),
            user_agent: config.user_agent.clone(),
        },
        config.request_policy,
    )?;
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let now = Utc::now();
    let pages = plan_pages(&config, &client, run_id, now.date_naive()).await?;

    if !live_write_enabled(config.live_write.as_deref()) {
        tracing::info!(
            objects_planned = pages.len(),
            logical_records_seen = total_logical_record_count_of_pages(&pages),
            total_size_bytes = total_size_bytes_of_pages(&pages),
            first_object_key = pages.first().map(|page| page.plan.object_key.as_str()),
            last_object_key = pages.last().map(|page| page.plan.object_key.as_str()),
            "VWorld land-register Bronze ingest dry run succeeded"
        );
        return Ok(());
    }

    persist_plans(&config, run_id, now, &pages).await
}

async fn plan_pages(
    config: &VWorldLandRegisterIngestConfig,
    client: &VWorldNedAttributeClient,
    run_id: IngestionRunId,
    ingest_date: chrono::NaiveDate,
) -> anyhow::Result<Vec<VWorldLandRegisterPlannedPage>> {
    let mut pages = Vec::new();
    let mut last_page_observation = None;
    for (request_index, request) in page_requests_for_batch(&config.request, config.max_pages)?
        .into_iter()
        .enumerate()
    {
        if let Some(spacing) = config.request_spacing {
            spacing.wait_before_request(request_index).await;
        }
        let fetched_page = client
            .fetch_json_page(
                &request.operation,
                &BTreeMap::from([("pnu".to_owned(), request.pnu.clone())]),
                request.page_no,
                request.num_of_rows,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to fetch VWorld land-register page {} for PNU {}",
                    request.page_no, request.pnu
                )
            })?;
        let provider_total_count = land_register_total_count(&fetched_page.payload)?;
        let plan = plan_vworld_land_register_bronze_page(VWorldLandRegisterBronzePagePlanInput {
            source_slug: &config.source_slug,
            ingest_date,
            ingestion_run_id: run_id,
            request: request.clone(),
            raw_payload: fetched_page.raw_payload.clone(),
            payload: fetched_page.payload.clone(),
        })
        .with_context(|| {
            format!(
                "failed to plan VWorld land-register Bronze page {} for PNU {}",
                request.page_no, request.pnu
            )
        })?;
        let logical_record_count = plan.logical_record_count;
        pages.push(VWorldLandRegisterPlannedPage {
            plan,
            request: request.clone(),
            raw_payload: fetched_page.raw_payload,
            payload: fetched_page.payload,
        });
        last_page_observation = Some((
            request.page_no,
            request.num_of_rows,
            logical_record_count,
            provider_total_count,
        ));
        if should_stop_after_page(
            request.page_no,
            request.num_of_rows,
            logical_record_count,
            provider_total_count,
        ) {
            break;
        }
    }

    if let Some((last_page, page_size, logical_record_count, provider_total_count)) =
        last_page_observation
    {
        assert_page_window_complete(
            "VWorld land-register",
            last_page,
            page_size,
            logical_record_count,
            total_logical_record_count_of_pages(&pages),
            provider_total_count,
            config.max_pages,
        )?;
    }

    Ok(pages)
}

fn page_requests_for_batch(
    base_request: &VWorldLandRegisterPageRequest,
    max_pages: u32,
) -> anyhow::Result<Vec<VWorldLandRegisterPageRequest>> {
    (0..max_pages)
        .map(|offset| {
            let page_no = base_request
                .page_no
                .checked_add(offset)
                .context("VWorld land-register pageNo window exceeds u32")?;
            Ok(VWorldLandRegisterPageRequest {
                operation: base_request.operation.clone(),
                pnu: base_request.pnu.clone(),
                page_no,
                num_of_rows: base_request.num_of_rows,
            })
        })
        .collect()
}

fn should_stop_after_page(
    page: u32,
    page_size: u32,
    logical_record_count: u64,
    provider_total_count: Option<u64>,
) -> bool {
    if let Some(total_count) = provider_total_count {
        return u64::from(page).saturating_mul(u64::from(page_size)) >= total_count;
    }
    logical_record_count < u64::from(page_size)
}

fn land_register_total_count(payload: &JsonValue) -> anyhow::Result<Option<u64>> {
    if let Some(total_count) = json_u64_pointer(payload, "/ladfrlVOList/totalCount")? {
        return Ok(Some(total_count));
    }
    json_u64_pointer(payload, "/response/totalCount")
}

fn json_u64_pointer(payload: &JsonValue, pointer: &str) -> anyhow::Result<Option<u64>> {
    let Some(value) = payload.pointer(pointer) else {
        return Ok(None);
    };
    match value {
        JsonValue::Null => Ok(None),
        JsonValue::Number(number) => number
            .as_u64()
            .with_context(|| {
                format!("VWorld land-register JSON field {pointer} must be an unsigned integer")
            })
            .map(Some),
        JsonValue::String(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            trimmed
                .parse::<u64>()
                .with_context(|| {
                    format!("VWorld land-register JSON field {pointer} must be an unsigned integer")
                })
                .map(Some)
        }
        _ => bail!("VWorld land-register JSON field {pointer} must be an unsigned integer"),
    }
}

async fn persist_plans(
    config: &VWorldLandRegisterIngestConfig,
    run_id: IngestionRunId,
    started_at: chrono::DateTime<Utc>,
    pages: &[VWorldLandRegisterPlannedPage],
) -> anyhow::Result<()> {
    if pages.is_empty() {
        bail!("VWorld land-register ingest produced no Bronze page plans");
    }

    // Live-write path (`run` already gated on live_write before calling this): validate + log the
    // resolved R2 target before the first put, instead of failing mid-run on a misconfigured target.
    crate::bronze_object_storage::live_write_target_preflight()
        .context("VWorld land-register live-write target preflight failed")?;
    let database_url = required_env_value("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to database for VWorld land-register ingest")?;
    let uow = PgBronzeIngestUnitOfWork::new(pool);
    let storage = land_register_bronze_object_storage_from_env()
        .await
        .context("failed to configure object storage for VWorld land-register Bronze ingest")?;

    let report =
        persist_plans_with_adapters(config, run_id, started_at, pages, &uow, storage.as_ref())
            .await?;

    tracing::info!(
        run_id = %report.run_id,
        last_object_key = ?report.last_object_key,
        last_bronze_object_id = ?report.last_bronze_object_id,
        logical_record_count = report.logical_records_seen,
        objects_written = report.objects_written,
        "VWorld land-register Bronze ingest live write succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VWorldLandRegisterPersistReport {
    run_id: IngestionRunId,
    last_object_key: Option<String>,
    last_bronze_object_id: Option<BronzeObjectId>,
    logical_records_seen: u64,
    objects_written: u64,
}

async fn land_register_bronze_object_storage_from_env(
) -> anyhow::Result<Box<dyn ObjectStorageService>> {
    bronze_object_storage_from_env().await
}

async fn persist_plans_with_adapters<Uow, Storage>(
    config: &VWorldLandRegisterIngestConfig,
    run_id: IngestionRunId,
    started_at: chrono::DateTime<Utc>,
    pages: &[VWorldLandRegisterPlannedPage],
    uow: &Uow,
    storage: &Storage,
) -> anyhow::Result<VWorldLandRegisterPersistReport>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    if pages.is_empty() {
        bail!("VWorld land-register ingest produced no Bronze page plans");
    }

    // The compiled plans the planning stage already produced drive the run-level request snapshot;
    // the per-page commit + schema-profile + accounting now lives in the shared PageCollector loop
    // (ADR 0017), which hands each page's RAW identity to the BronzeCommitter (ADR 0016). The loop
    // body, ordering, `objects_written` accounting, commit-error -> terminal-failure mapping, run
    // lifecycle, and schema-profile gathering are identical across page lanes, so they live ONCE in
    // `collect_planned_pages`; this lane supplies only its catalog identity + per-source declaration.
    let plans: Vec<VWorldLandRegisterBronzePagePlan> =
        pages.iter().map(|page| page.plan.clone()).collect();

    let collectable_pages: Vec<CollectablePage<VWorldLandRegisterPageRequest>> = pages
        .iter()
        .map(|page| CollectablePage {
            plan: page.plan.clone(),
            request: page.request.clone(),
            raw_payload: page.raw_payload.clone(),
            payload: page.payload.clone(),
        })
        .collect();

    let report = collect_planned_pages(
        &VWorldLandRegisterLane,
        source_catalog_entry(config, started_at),
        ingestion_run(
            // The collector overrides `source_catalog_id` from the upserted source, so any placeholder
            // is fine here; pass the land-register run identity + request snapshot as before.
            SourceCatalogId::new(Uuid::nil()),
            run_id,
            started_at,
            batch_request_params(config, &plans),
        ),
        &config.source_slug,
        started_at,
        &collectable_pages,
        uow,
        storage,
    )
    .await?;

    Ok(VWorldLandRegisterPersistReport {
        run_id: report.run_id,
        last_object_key: report.last_object_key,
        last_bronze_object_id: report.last_bronze_object_id,
        logical_records_seen: report.logical_records_seen,
        objects_written: report.objects_written,
    })
}

/// VWorld land-register page lane declaration (ADR 0017): the only per-source bits the shared
/// [`collect_planned_pages`] loop needs — the lane label used in commit-error context, and the
/// candidate-key override (land-register re-scores the trailing `pnu` segment). The loop, accounting,
/// commit handoff, run lifecycle, and schema-profile gathering all live in the collector.
struct VWorldLandRegisterLane;

impl PageCollectorLane for VWorldLandRegisterLane {
    type Request = VWorldLandRegisterPageRequest;

    fn lane_label(&self) -> &str {
        "VWorld land-register"
    }

    fn candidate_key_override(&self) -> CandidateKeyOverride {
        CandidateKeyOverride::LastDotSegmentEquals("pnu")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VWorldLandRegisterIngestConfig {
    source_slug: String,
    base_uri: String,
    api_key: String,
    domain: Option<String>,
    user_agent: String,
    request: VWorldLandRegisterPageRequest,
    max_pages: u32,
    request_spacing: Option<ProviderRequestSpacing>,
    request_policy: VWorldRequestPolicy,
    live_write: Option<String>,
}

impl VWorldLandRegisterIngestConfig {
    fn from_env() -> anyhow::Result<Self> {
        let default_policy = VWorldRequestPolicy::default();
        let max_attempts = optional_positive_u32_env("FOUNDATION_PLATFORM_VWORLD_MAX_ATTEMPTS")?
            .unwrap_or_else(|| default_policy.max_attempts());
        let request_timeout =
            optional_duration_seconds_env("FOUNDATION_PLATFORM_VWORLD_REQUEST_TIMEOUT_SECONDS")?
                .unwrap_or_else(|| default_policy.request_timeout());
        let initial_backoff =
            optional_duration_millis_env("FOUNDATION_PLATFORM_VWORLD_RETRY_INITIAL_BACKOFF_MS")?
                .unwrap_or_else(|| default_policy.initial_backoff());
        let max_backoff =
            optional_duration_millis_env("FOUNDATION_PLATFORM_VWORLD_RETRY_MAX_BACKOFF_MS")?
                .unwrap_or_else(|| default_policy.max_backoff());

        Ok(Self {
            source_slug: crate::public_data_control_support::resolve_canonical_source_slug(
                "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_SOURCE_SLUG",
                optional_env_value("FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_SOURCE_SLUG")?,
                GENERATOR_PROVIDER,
                DEFAULT_DATASET_SLUG,
            )?,
            base_uri: optional_env_value("FOUNDATION_PLATFORM_VWORLD_NED_BASE_URI")?
                .unwrap_or_else(|| DEFAULT_BASE_URI.to_owned()),
            api_key: required_env_value("VWORLD_API_KEY")?,
            domain: optional_env_value("VWORLD_DOMAIN")?,
            user_agent: optional_env_value("FOUNDATION_PLATFORM_VWORLD_USER_AGENT")?
                .unwrap_or_else(|| DEFAULT_USER_AGENT.to_owned()),
            request: VWorldLandRegisterPageRequest {
                operation: optional_env_value(
                    "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_OPERATION",
                )?
                .unwrap_or_else(|| DEFAULT_OPERATION.to_owned()),
                pnu: required_env_value("FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_PNU")?,
                page_no: optional_u32_env("FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_PAGE_NO")?
                    .unwrap_or(1),
                num_of_rows: optional_u32_env(
                    "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_NUM_OF_ROWS",
                )?
                .unwrap_or(1000),
            },
            max_pages: optional_positive_u32_env(
                "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_MAX_PAGES",
            )?
            .unwrap_or(DEFAULT_MAX_PAGES),
            request_spacing: ProviderRequestSpacing::optional_from_millis(
                optional_u64_env("FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_MIN_PAGE_INTERVAL_MS")?
                    .or(optional_u64_env(
                        "FOUNDATION_PLATFORM_PROVIDER_MIN_PAGE_INTERVAL_MS",
                    )?),
            )?,
            request_policy: VWorldRequestPolicy::new(
                max_attempts,
                request_timeout,
                initial_backoff,
                max_backoff,
            )?,
            live_write: optional_env_value("FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_LIVE_WRITE")?,
        })
    }
}

fn source_catalog_entry(
    config: &VWorldLandRegisterIngestConfig,
    now: chrono::DateTime<Utc>,
) -> SourceCatalogEntry {
    SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: config.source_slug.clone(),
        name: SOURCE_NAME.to_owned(),
        provider: PROVIDER.to_owned(),
        dataset_name: DATASET_NAME.to_owned(),
        base_url: Some(config.base_uri.clone()),
        auth_kind: SourceAuthKind::ServiceKey,
        payload_format: SourcePayloadFormat::Json,
        license_name: None,
        license_url: None,
        terms_url: Some("https://www.vworld.kr/dev/v4apiRefer.do".to_owned()),
        collection_frequency: None,
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    }
}

const fn ingestion_run(
    source_catalog_id: SourceCatalogId,
    run_id: IngestionRunId,
    now: chrono::DateTime<Utc>,
    request_params: JsonValue,
) -> IngestionRun {
    IngestionRun {
        id: run_id,
        source_catalog_id,
        trigger: IngestionTrigger::Manual,
        status: IngestionRunStatus::Running,
        request_params,
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

fn batch_request_params(
    config: &VWorldLandRegisterIngestConfig,
    plans: &[VWorldLandRegisterBronzePagePlan],
) -> JsonValue {
    json!({
        "operation": config.request.operation,
        "pnu": config.request.pnu,
        "startPageNo": config.request.page_no,
        "numOfRows": config.request.num_of_rows,
        "maxPages": config.max_pages,
        "pagesPlanned": plans.len(),
        "format": "json"
    })
}

// `total_logical_record_count` over plans (the failure-path / completion accounting helper) now lives
// in the shared PageCollector loop, which owns the run lifecycle (ADR 0017). The dry-run summary still
// needs the planned-page variant below.
fn total_logical_record_count_of_pages(pages: &[VWorldLandRegisterPlannedPage]) -> u64 {
    pages
        .iter()
        .map(|page| page.plan.logical_record_count)
        .sum()
}

fn total_size_bytes_of_pages(pages: &[VWorldLandRegisterPlannedPage]) -> u64 {
    pages.iter().map(|page| page.plan.size_bytes).sum()
}

fn live_write_enabled(value: Option<&str>) -> bool {
    value == Some("1")
}

#[cfg(test)]
mod tests {
    use crate::pagination_guard::assert_page_window_complete;

    use foundation_outbox::object_storage::{ObjectWriteMode, PutObjectRequest};

    use serde_json::json;

    use super::{
        land_register_total_count, page_requests_for_batch, should_stop_after_page,
        VWorldLandRegisterPageRequest,
    };

    #[test]
    fn page_requests_cover_configured_batch_window() -> anyhow::Result<()> {
        let base_request = VWorldLandRegisterPageRequest {
            operation: "ladfrlList".to_owned(),
            pnu: "9999900601100010000".to_owned(),
            page_no: 7,
            num_of_rows: 10,
        };

        let pages = page_requests_for_batch(&base_request, 3)?;

        assert_eq!(
            pages
                .iter()
                .map(|request| request.page_no)
                .collect::<Vec<_>>(),
            vec![7, 8, 9]
        );
        assert!(pages.iter().all(|request| request.operation == "ladfrlList"
            && request.pnu == "9999900601100010000"
            && request.num_of_rows == 10));
        Ok(())
    }

    #[test]
    fn page_window_rejects_full_short_page_fallback_at_exhausted_cap() -> anyhow::Result<()> {
        let error =
            match assert_page_window_complete("VWorld land-register", 1, 1000, 1000, 1000, None, 1)
            {
                Ok(()) => anyhow::bail!("expected exhausted page cap to fail"),
                Err(error) => error,
            };

        assert!(
            error.to_string().contains("page cap exhausted"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn parses_land_register_provider_total_count() -> anyhow::Result<()> {
        let payload = json!({
            "ladfrlVOList": {
                "totalCount": "1000",
                "ladfrlVOList": []
            }
        });

        assert_eq!(land_register_total_count(&payload)?, Some(1000));
        Ok(())
    }

    #[test]
    fn stop_condition_uses_land_register_provider_total_count() {
        assert!(should_stop_after_page(1, 1000, 1000, Some(1000)));
        assert!(!should_stop_after_page(1, 1000, 1000, Some(1001)));
        assert!(!should_stop_after_page(1, 1000, 1000, None));
    }

    #[tokio::test]
    async fn storage_configuration_accepts_local_bronze_driver() -> anyhow::Result<()> {
        let root = std::env::temp_dir().join(format!(
            "foundation-platform-vworld-land-register-bronze-{}",
            uuid::Uuid::new_v4()
        ));
        std::env::set_var("FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER", "local");
        std::env::set_var("FOUNDATION_PLATFORM_BRONZE_LOCAL_OBJECT_ROOT", &root);

        let storage = super::land_register_bronze_object_storage_from_env().await?;
        storage
            .put_object(PutObjectRequest {
                key: "bronze/source=vworld-land-register-test/operation=ladfrlList/pnu=9999900101100010000/page-000001.json"
                    .to_owned(),
                body: br#"{"ok":true}"#.to_vec(),
                content_type: "application/json".to_owned(),
                cache_control: "no-store, max-age=0".to_owned(),
                write_mode: ObjectWriteMode::OverwriteAllowed,
                sha256: None,
            })
            .await?;

        assert!(root
            .join("bronze")
            .join("source=vworld-land-register-test")
            .join("operation=ladfrlList")
            .join("pnu=9999900101100010000")
            .join("page-000001.json")
            .exists());

        std::env::remove_var("FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER");
        std::env::remove_var("FOUNDATION_PLATFORM_BRONZE_LOCAL_OBJECT_ROOT");
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    // ---- Live persist path through the BronzeCommitter (Task 3) ----
    //
    // These mirror the real-transaction / NED lanes' persist tests. The put/record path now flows
    // through `committer.commit_vworld_land_register_page` (CreateOnly + recoverable commit protocol),
    // so the R2-already-exists case is no longer an unconditional hard failure: a matching-checksum
    // object with a missing DB row RECOVERS, while a conflicting-checksum object fails loud.

    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use collection_application::plan_vworld_land_register_bronze_page;
    use collection_application::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};
    use collection_domain::CollectionError;
    use collection_domain::{
        BronzeObject, IngestionRun, IngestionRunStatus, SchemaProfile, SourceCatalogEntry,
    };
    use collection_infrastructure::VWorldRequestPolicy;
    use foundation_outbox::{ObjectStorageService, PublishError};
    use foundation_shared_kernel::ids::{IngestionRunId, SourceCatalogId};
    use uuid::Uuid;

    use super::{
        persist_plans_with_adapters, VWorldLandRegisterBronzePagePlanInput,
        VWorldLandRegisterIngestConfig, VWorldLandRegisterPlannedPage, DEFAULT_BASE_URI,
        DEFAULT_USER_AGENT,
    };

    fn test_config() -> VWorldLandRegisterIngestConfig {
        VWorldLandRegisterIngestConfig {
            source_slug: "vworldkr__land_register".to_owned(),
            base_uri: DEFAULT_BASE_URI.to_owned(),
            api_key: "redacted-test-key".to_owned(),
            domain: None,
            user_agent: DEFAULT_USER_AGENT.to_owned(),
            request: VWorldLandRegisterPageRequest {
                operation: "ladfrlList".to_owned(),
                pnu: "9999900601100010000".to_owned(),
                page_no: 1,
                num_of_rows: 1000,
            },
            max_pages: 1,
            request_spacing: None,
            request_policy: VWorldRequestPolicy::default(),
            live_write: Some("1".to_owned()),
        }
    }

    fn test_planned_page(
        config: &VWorldLandRegisterIngestConfig,
        run_id: IngestionRunId,
        page_no: u32,
    ) -> anyhow::Result<VWorldLandRegisterPlannedPage> {
        let payload = json!({
            "ladfrlVOList": { "ladfrlVOList": [
                { "pnu": "9999900601100010000", "page": format!("{page_no:03}") }
            ] }
        });
        let raw_payload = serde_json::to_vec(&payload)?;
        let request = VWorldLandRegisterPageRequest {
            page_no,
            ..config.request.clone()
        };
        let plan = plan_vworld_land_register_bronze_page(VWorldLandRegisterBronzePagePlanInput {
            source_slug: &config.source_slug,
            ingest_date: chrono::NaiveDate::from_ymd_opt(2026, 6, 2)
                .ok_or_else(|| anyhow::anyhow!("valid date"))?,
            ingestion_run_id: run_id,
            request: request.clone(),
            raw_payload: raw_payload.clone(),
            payload: payload.clone(),
        })?;
        Ok(VWorldLandRegisterPlannedPage {
            plan,
            request,
            raw_payload,
            payload,
        })
    }

    /// Recoverable commit protocol end-to-end through the land-register live persist path: the object
    /// is already in R2 with a matching checksum (a prior run's write) but no `bronze_object` row
    /// exists (that prior run's DB record failed). The CreateOnly write hits already-exists; the
    /// committer recovers by recording the missing row, so the run completes Succeeded — an
    /// R2-already-exists is no longer a hard failure.
    #[tokio::test]
    async fn persist_recovers_when_r2_object_exists_but_db_row_is_missing() -> anyhow::Result<()> {
        let run_id = IngestionRunId::new(Uuid::new_v4());
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-06-02T00:00:00Z")?.to_utc();
        let config = test_config();
        let source_id = SourceCatalogId::new(Uuid::new_v4());
        let page = test_planned_page(&config, run_id, 1)?;
        let uow = FakeBronzeUow::new(source_id, FakeFailureMode::None);
        // Object already present with this page's exact checksum, but no DB row recorded yet.
        let storage = FakeObjectStorage::with_existing_object(
            page.plan.object_key.as_str(),
            &page.plan.checksum_sha256,
        );
        let pages = vec![page.clone()];

        let report =
            persist_plans_with_adapters(&config, run_id, started_at, &pages, &uow, &storage)
                .await?;

        assert_eq!(report.objects_written, 1);
        assert_eq!(report.logical_records_seen, 1);
        // No fresh write (the object already existed), but the missing row was recovered.
        assert!(storage.written_keys()?.is_empty());
        let recorded = uow.recorded_bronze_objects()?;
        assert_eq!(recorded.len(), 1);
        assert_eq!(
            recorded[0].object_key.as_str(),
            page.plan.object_key.as_str()
        );
        assert_eq!(recorded[0].checksum_sha256, page.plan.checksum_sha256);

        let completions = uow.completions()?;
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].status, IngestionRunStatus::Succeeded);
        assert!(completions[0].error_message.is_none());
        Ok(())
    }

    /// Quarantine terminal through the land-register live persist path: the object is already in R2
    /// but with a DIFFERENT checksum and no DB row. The committer cannot prove the object is ours, so
    /// it fails loud and the run is marked Failed (never silently overwritten).
    #[tokio::test]
    async fn persist_fails_loud_when_r2_object_checksum_conflicts() -> anyhow::Result<()> {
        let run_id = IngestionRunId::new(Uuid::new_v4());
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-06-02T00:00:00Z")?.to_utc();
        let config = test_config();
        let source_id = SourceCatalogId::new(Uuid::new_v4());
        let page = test_planned_page(&config, run_id, 1)?;
        let uow = FakeBronzeUow::new(source_id, FakeFailureMode::None);
        let conflicting_sha = "0".repeat(64);
        let storage = FakeObjectStorage::with_existing_object(
            page.plan.object_key.as_str(),
            &conflicting_sha,
        );
        let pages = vec![page];

        let error =
            persist_plans_with_adapters(&config, run_id, started_at, &pages, &uow, &storage)
                .await
                .err()
                .ok_or_else(|| anyhow::anyhow!("expected checksum conflict failure"))?;

        assert!(
            error.to_string().contains("Bronze checksum conflict"),
            "unexpected error: {error}"
        );
        assert!(uow.recorded_bronze_objects()?.is_empty());
        let completions = uow.completions()?;
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].status, IngestionRunStatus::Failed);
        // Nothing newly written: the CreateOnly collision did not add an object.
        assert_eq!(completions[0].objects_written, 0);
        Ok(())
    }

    /// A Bronze metadata record failure still fails the run, with the same context message the inline
    /// put+record path produced, and the object is counted as written (the CreateOnly write happened
    /// before the record).
    #[tokio::test]
    async fn persist_marks_run_failed_when_bronze_metadata_recording_fails() -> anyhow::Result<()> {
        let run_id = IngestionRunId::new(Uuid::new_v4());
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-06-02T00:00:00Z")?.to_utc();
        let config = test_config();
        let source_id = SourceCatalogId::new(Uuid::new_v4());
        let pages = vec![test_planned_page(&config, run_id, 1)?];
        let uow = FakeBronzeUow::new(source_id, FakeFailureMode::RecordBronzeObject);
        let storage = FakeObjectStorage::default();

        let error =
            persist_plans_with_adapters(&config, run_id, started_at, &pages, &uow, &storage)
                .await
                .err()
                .ok_or_else(|| anyhow::anyhow!("expected persistence failure"))?;

        assert!(
            error
                .to_string()
                .contains("failed to record VWorld land-register Bronze object metadata"),
            "unexpected error: {error}"
        );
        // The object was written (CreateOnly) before the record attempt failed.
        assert_eq!(storage.written_keys()?.len(), 1);

        let completions = uow.completions()?;
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].status, IngestionRunStatus::Failed);
        assert_eq!(completions[0].objects_written, 1);
        assert_eq!(completions[0].logical_records_seen, 1);
        Ok(())
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum FakeFailureMode {
        None,
        RecordBronzeObject,
    }

    struct FakeBronzeUow {
        source_id: SourceCatalogId,
        failure_mode: FakeFailureMode,
        runs: Mutex<Vec<IngestionRun>>,
        completions: Mutex<Vec<CompleteIngestionRunCommand>>,
        bronze_objects: Mutex<Vec<BronzeObject>>,
        schema_profiles: Mutex<Vec<SchemaProfile>>,
    }

    impl FakeBronzeUow {
        const fn new(source_id: SourceCatalogId, failure_mode: FakeFailureMode) -> Self {
            Self {
                source_id,
                failure_mode,
                runs: Mutex::new(Vec::new()),
                completions: Mutex::new(Vec::new()),
                bronze_objects: Mutex::new(Vec::new()),
                schema_profiles: Mutex::new(Vec::new()),
            }
        }

        fn completions(&self) -> anyhow::Result<Vec<CompleteIngestionRunCommand>> {
            Ok(self
                .completions
                .lock()
                .map_err(|_| anyhow::anyhow!("completion lock poisoned"))?
                .clone())
        }

        fn recorded_bronze_objects(&self) -> anyhow::Result<Vec<BronzeObject>> {
            Ok(self
                .bronze_objects
                .lock()
                .map_err(|_| anyhow::anyhow!("bronze object lock poisoned"))?
                .clone())
        }
    }

    #[async_trait]
    impl BronzeIngestUnitOfWork for FakeBronzeUow {
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
            if self.failure_mode == FakeFailureMode::RecordBronzeObject {
                return Err(CollectionError::Infrastructure(
                    "simulated bronze metadata failure".to_owned(),
                ));
            }
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
    struct FakeObjectStorage {
        written_keys: Mutex<Vec<String>>,
        /// Pre-seeded objects already in storage, keyed by object key -> stored `x-amz-meta-sha256`.
        /// A `CreateOnly` write to a seeded key returns `ObjectAlreadyExists` (the R2 412), driving
        /// the committer's recoverable commit protocol; `read_object_sha256` reports the seeded sum.
        existing: Mutex<BTreeMap<String, String>>,
    }

    impl FakeObjectStorage {
        fn with_existing_object(key: &str, sha256: &str) -> Self {
            let storage = Self::default();
            storage
                .existing
                .lock()
                .expect("existing lock")
                .insert(key.to_owned(), sha256.to_owned());
            storage
        }

        fn written_keys(&self) -> anyhow::Result<Vec<String>> {
            Ok(self
                .written_keys
                .lock()
                .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?
                .clone())
        }
    }

    #[async_trait]
    impl ObjectStorageService for FakeObjectStorage {
        async fn put_object(&self, request: PutObjectRequest) -> Result<(), PublishError> {
            // CreateOnly collision with a pre-seeded key surfaces as ObjectAlreadyExists (R2 412).
            if matches!(request.write_mode, ObjectWriteMode::CreateOnly)
                && self
                    .existing
                    .lock()
                    .map_err(|_| PublishError::Infrastructure("existing lock poisoned".to_owned()))?
                    .contains_key(&request.key)
            {
                return Err(PublishError::ObjectAlreadyExists { key: request.key });
            }
            self.written_keys
                .lock()
                .map_err(|_| PublishError::Infrastructure("storage lock poisoned".to_owned()))?
                .push(request.key);
            Ok(())
        }

        async fn read_object_sha256(&self, key: &str) -> Result<Option<String>, PublishError> {
            Ok(self
                .existing
                .lock()
                .map_err(|_| PublishError::Infrastructure("existing lock poisoned".to_owned()))?
                .get(key)
                .cloned())
        }
    }
}
