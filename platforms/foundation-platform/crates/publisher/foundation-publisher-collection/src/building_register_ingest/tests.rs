use std::{collections::BTreeMap, sync::Mutex, time::Duration};

use super::{
    building_register_region_from_options, effective_page_size_from_response_metadata,
    json_u64_pointer, live_write_enabled, page_count_probe_from_response_metadata,
    page_requests_for_batch, partial_page_window_enabled, persist_plans_with_adapters,
    reconcile_building_register_run_with_adapters, schema_profiles_for_plans,
    should_stop_after_page, BuildingRegisterBronzeObjectReader,
    BuildingRegisterBronzePagePlanInput, BuildingRegisterIngestConfig, BuildingRegisterPageRequest,
    BuildingRegisterPlannedPage, BuildingRegisterSourceIdentity,
};
use crate::bronze_object_storage::{
    bronze_object_storage_driver_from_options, BronzeObjectStorageDriver,
};
use crate::provider_request_spacing::ProviderRequestSpacing;
use async_trait::async_trait;
use chrono::NaiveDate;
use collection_application::ports::{
    BronzeIngestRepository, BronzeIngestUnitOfWork, CompleteIngestionRunCommand,
};
use collection_domain::CollectionError;
use collection_domain::{
    BronzeObject, IngestionRun, IngestionRunStatus, SchemaProfile, SourceCatalogEntry,
};
use collection_infrastructure::DataGoKrRequestPolicy;
use foundation_outbox::{object_storage::PutObjectRequest, ObjectStorageService, PublishError};
use foundation_shared_kernel::ids::{IngestionRunId, SourceCatalogId};
use serde_json::json;
use uuid::Uuid;

use crate::pagination_guard::assert_page_window_complete;

#[test]
fn live_write_requires_exact_opt_in() {
    assert!(!live_write_enabled(None));
    assert!(!live_write_enabled(Some("")));
    assert!(!live_write_enabled(Some("true")));
    assert!(!live_write_enabled(Some(" 1 ")));
    assert!(live_write_enabled(Some("1")));
}

#[test]
fn partial_page_window_requires_exact_opt_in() {
    assert!(!partial_page_window_enabled(None));
    assert!(!partial_page_window_enabled(Some("")));
    assert!(!partial_page_window_enabled(Some("true")));
    assert!(!partial_page_window_enabled(Some(" 1 ")));
    assert!(partial_page_window_enabled(Some("1")));
}

#[test]
fn live_write_requires_explicit_building_register_region() -> anyhow::Result<()> {
    let Err(error) = building_register_region_from_options(None, None, Some("1")) else {
        return Err(anyhow::anyhow!("live write without region must fail"));
    };

    assert!(
        error
            .to_string()
            .contains("FOUNDATION_PLATFORM_BUILDING_REGISTER_SIGUNGU_CD is required"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn bronze_local_storage_driver_requires_explicit_root() -> anyhow::Result<()> {
    let driver = bronze_object_storage_driver_from_options(Some("local"), Some("target/bronze"))?;
    assert_eq!(
        driver,
        BronzeObjectStorageDriver::Local(std::path::PathBuf::from("target/bronze"))
    );

    let error = bronze_object_storage_driver_from_options(Some("local"), None)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected missing local root failure"))?;
    assert!(
        error
            .to_string()
            .contains("FOUNDATION_PLATFORM_BRONZE_LOCAL_OBJECT_ROOT is required"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn bronze_storage_driver_defaults_to_r2_for_live_ingestion() -> anyhow::Result<()> {
    assert_eq!(
        bronze_object_storage_driver_from_options(None, None)?,
        BronzeObjectStorageDriver::R2
    );
    Ok(())
}

#[test]
fn page_requests_cover_configured_batch_window() -> anyhow::Result<()> {
    let base_request = BuildingRegisterPageRequest {
        operation: "getBrTitleInfo".to_owned(),
        sigungu_cd: "11680".to_owned(),
        bjdong_cd: "10300".to_owned(),
        page_no: 7,
        num_of_rows: 100,
    };

    let pages = page_requests_for_batch(&base_request, 3)?;

    assert_eq!(
        pages
            .iter()
            .map(|request| request.page_no)
            .collect::<Vec<_>>(),
        vec![7, 8, 9]
    );
    assert!(pages
        .iter()
        .all(|request| request.operation == "getBrTitleInfo"
            && request.sigungu_cd == "11680"
            && request.bjdong_cd == "10300"
            && request.num_of_rows == 100));
    Ok(())
}

#[test]
fn page_requests_reject_page_number_overflow() -> anyhow::Result<()> {
    let base_request = BuildingRegisterPageRequest {
        operation: "getBrTitleInfo".to_owned(),
        sigungu_cd: "11680".to_owned(),
        bjdong_cd: "10300".to_owned(),
        page_no: u32::MAX,
        num_of_rows: 100,
    };

    let error = match page_requests_for_batch(&base_request, 2) {
        Ok(pages) => anyhow::bail!("expected overflow failure, got pages: {pages:?}"),
        Err(error) => error,
    };

    assert!(
        error.to_string().contains("pageNo window exceeds u32"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn provider_request_spacing_skips_first_page_and_delays_later_pages() {
    let spacing =
        ProviderRequestSpacing::try_new(Duration::from_millis(500)).expect("valid interval");

    assert_eq!(spacing.delay_before_request(0), None);
    assert_eq!(
        spacing.delay_before_request(1),
        Some(Duration::from_millis(500))
    );
    assert_eq!(
        spacing.delay_before_request(12),
        Some(Duration::from_millis(500))
    );
}

#[test]
fn provider_request_spacing_rejects_zero_interval() {
    let error = ProviderRequestSpacing::try_new(Duration::from_millis(0))
        .err()
        .expect("zero interval should fail");

    assert!(
        error
            .to_string()
            .contains("provider request spacing interval must be greater than zero"),
        "unexpected error: {error}"
    );
}

#[test]
fn stop_condition_uses_provider_total_count_before_short_page_fallback() {
    assert!(should_stop_after_page(10, 100, 100, Some(1_000)));
    assert!(!should_stop_after_page(9, 100, 100, Some(1_000)));
    assert!(should_stop_after_page(1, 100, 0, Some(0)));
    assert!(should_stop_after_page(11, 100, 25, None));
    assert!(!should_stop_after_page(10, 100, 100, None));
}

#[test]
fn effective_page_size_prefers_provider_response_metadata() -> anyhow::Result<()> {
    let payload = json!({
        "response": {
            "body": {
                "numOfRows": "100"
            }
        }
    });

    assert_eq!(
        effective_page_size_from_response_metadata(&payload, 1_000)?,
        100
    );

    let missing = json!({ "response": { "body": {} } });
    assert_eq!(
        effective_page_size_from_response_metadata(&missing, 1_000)?,
        1_000
    );
    Ok(())
}

#[test]
fn page_window_rejects_provider_capped_page_size() -> anyhow::Result<()> {
    let error =
        match assert_page_window_complete("building-register", 3, 100, 100, 300, Some(2_864), 3) {
            Ok(()) => anyhow::bail!("provider-capped page size must fail when total is incomplete"),
            Err(error) => error,
        };

    assert!(
        error.to_string().contains("provider_total_count=2864"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn page_count_probe_uses_effective_provider_page_size() -> anyhow::Result<()> {
    let request = BuildingRegisterPageRequest {
        operation: "getBrTitleInfo".to_owned(),
        sigungu_cd: "11110".to_owned(),
        bjdong_cd: "17400".to_owned(),
        page_no: 1,
        num_of_rows: 1_000,
    };
    let payload = json!({
        "response": {
            "body": {
                "numOfRows": "100",
                "totalCount": "2864"
            }
        }
    });

    let probe = page_count_probe_from_response_metadata(&request, &payload)?;

    assert_eq!(probe.operation, "getBrTitleInfo");
    assert_eq!(probe.sigungu_cd, "11110");
    assert_eq!(probe.bjdong_cd, "17400");
    assert_eq!(probe.requested_page_size, 1_000);
    assert_eq!(probe.effective_page_size, 100);
    assert_eq!(probe.provider_total_count, 2_864);
    assert_eq!(probe.required_pages, 29);
    Ok(())
}

#[test]
fn page_count_probe_rejects_missing_total_count() -> anyhow::Result<()> {
    let request = BuildingRegisterPageRequest {
        operation: "getBrTitleInfo".to_owned(),
        sigungu_cd: "11110".to_owned(),
        bjdong_cd: "17400".to_owned(),
        page_no: 1,
        num_of_rows: 100,
    };
    let payload = json!({
        "response": {
            "body": {
                "numOfRows": "100"
            }
        }
    });

    let error = match page_count_probe_from_response_metadata(&request, &payload) {
        Ok(probe) => anyhow::bail!("expected missing totalCount failure, got {probe:?}"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("building-register response body totalCount is required"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn page_window_rejects_exhausted_cap_before_provider_total_is_complete() -> anyhow::Result<()> {
    let error =
        match assert_page_window_complete("building-register", 1, 100, 100, 100, Some(250), 1) {
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
fn parses_building_register_total_count_from_response_metadata() -> anyhow::Result<()> {
    let payload = json!({
        "response": {
            "body": {
                "totalCount": "1000"
            }
        }
    });

    assert_eq!(
        json_u64_pointer(&payload, "/response/body/totalCount")?,
        Some(1_000)
    );
    assert_eq!(json_u64_pointer(&payload, "/response/body/missing")?, None);
    Ok(())
}

#[test]
fn schema_profiles_for_plans_aggregate_page_observations() -> anyhow::Result<()> {
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000004")?);
    let source_catalog_id = foundation_shared_kernel::ids::SourceCatalogId::new(Uuid::parse_str(
        "018f0000-0000-7000-8000-000000000005",
    )?);
    let now = chrono::DateTime::parse_from_rfc3339("2026-05-14T00:00:00Z")?.to_utc();
    let plans = [1, 2]
        .into_iter()
        .map(|page_no| {
            let payload = json!({
                "response": {
                    "body": {
                        "items": {
                            "item": [
                                {
                                    "mgmBldrgstPk": format!("11680-10300-{page_no}"),
                                    "totArea": if page_no == 1 {
                                        json!("100.25")
                                    } else {
                                        json!(null)
                                    }
                                }
                            ]
                        }
                    }
                }
            });
            let raw_payload = serde_json::to_vec(&payload)?;
            collection_application::plan_building_register_bronze_page(
                BuildingRegisterBronzePagePlanInput {
                    source_slug: "datagokr__building_register_main",
                    ingest_date: NaiveDate::from_ymd_opt(2026, 5, 14)
                        .ok_or_else(|| anyhow::anyhow!("valid date"))?,
                    ingestion_run_id: run_id,
                    request: BuildingRegisterPageRequest {
                        operation: "getBrTitleInfo".to_owned(),
                        sigungu_cd: "11680".to_owned(),
                        bjdong_cd: "10300".to_owned(),
                        page_no,
                        num_of_rows: 100,
                    },
                    raw_payload,
                    payload,
                },
            )
            .map_err(anyhow::Error::from)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let profiles = schema_profiles_for_plans(source_catalog_id, run_id, now, &plans);

    let total_area = profiles
        .iter()
        .find(|profile| profile.field_path == "response.body.items.item[].totArea")
        .ok_or_else(|| anyhow::anyhow!("expected totArea profile"))?;
    assert_eq!(total_area.nonnull_count, 1);
    assert_eq!(total_area.null_count, 1);

    let primary_key = profiles
        .iter()
        .find(|profile| profile.field_path == "response.body.items.item[].mgmBldrgstPk")
        .ok_or_else(|| anyhow::anyhow!("expected mgmBldrgstPk profile"))?;
    assert_eq!(primary_key.nonnull_count, 2);
    assert_eq!(primary_key.null_count, 0);
    assert!(primary_key.candidate_key_score > 0.9);
    Ok(())
}

#[tokio::test]
async fn persist_marks_run_failed_when_bronze_metadata_recording_fails() -> anyhow::Result<()> {
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000006")?);
    let started_at = chrono::DateTime::parse_from_rfc3339("2026-05-14T00:00:00Z")?.to_utc();
    let config = test_config()?;
    let pages = vec![test_planned_page(run_id, 1)?];
    let source_id = SourceCatalogId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000007")?);
    let uow = FakeBronzeUow::new(source_id, FakeFailureMode::RecordBronzeObject);
    let storage = FakeObjectStorage::default();

    let error = persist_plans_with_adapters(&config, run_id, started_at, &pages, &uow, &storage)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected persistence failure"))?;

    assert!(
        error
            .to_string()
            .contains("failed to record building-register Bronze object metadata"),
        "unexpected error: {error}"
    );
    assert_eq!(storage.written_keys()?.len(), 1);

    let completions = uow.completions()?;
    assert_eq!(completions.len(), 1);
    let failure = &completions[0];
    assert_eq!(failure.status, IngestionRunStatus::Failed);
    assert_eq!(failure.objects_written, 1);
    assert_eq!(failure.logical_records_seen, 1);
    assert!(failure
        .error_message
        .as_deref()
        .unwrap_or_default()
        .contains("failed to record building-register Bronze object metadata"));
    Ok(())
}

#[tokio::test]
async fn persist_marks_run_failed_when_r2_write_fails() -> anyhow::Result<()> {
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000008")?);
    let started_at = chrono::DateTime::parse_from_rfc3339("2026-05-14T00:00:00Z")?.to_utc();
    let config = test_config()?;
    let pages = vec![test_planned_page(run_id, 1)?];
    let source_id = SourceCatalogId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000009")?);
    let uow = FakeBronzeUow::new(source_id, FakeFailureMode::None);
    let storage = FakeObjectStorage::failing("simulated R2 outage");

    let error = persist_plans_with_adapters(&config, run_id, started_at, &pages, &uow, &storage)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected R2 write failure"))?;

    // The shared PageCollector loop (ADR 0017) is the single source of the storage-failure context
    // message; it uses the lane-agnostic "failed to write <lane> Bronze object" shape (no per-lane
    // " to R2" suffix), matching the real-transaction / V-World lanes. The inner R2 outage still
    // survives in the chain (asserted below).
    assert!(
        error
            .to_string()
            .contains("failed to write building-register Bronze object"),
        "unexpected error: {error}"
    );
    assert_eq!(storage.written_keys()?.len(), 0);

    let completions = uow.completions()?;
    assert_eq!(completions.len(), 1);
    let failure = &completions[0];
    assert_eq!(failure.status, IngestionRunStatus::Failed);
    assert_eq!(failure.objects_written, 0);
    assert_eq!(failure.logical_records_seen, 1);
    assert!(failure
        .error_message
        .as_deref()
        .unwrap_or_default()
        .contains("simulated R2 outage"));
    Ok(())
}

/// Recoverable commit protocol end-to-end through the live persist path: the page's object is
/// already in R2 (a prior run's write) with a matching checksum, but no `bronze_object` row exists
/// (that prior run's DB record failed). The CreateOnly write hits already-exists; the committer
/// recovers by recording the missing row, so the run completes Succeeded instead of failing.
#[tokio::test]
async fn persist_recovers_when_r2_object_exists_but_db_row_is_missing() -> anyhow::Result<()> {
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-00000000000c")?);
    let started_at = chrono::DateTime::parse_from_rfc3339("2026-05-14T00:00:00Z")?.to_utc();
    let config = test_config()?;
    let page = test_planned_page(run_id, 1)?;
    let source_id = SourceCatalogId::new(Uuid::parse_str("018f0000-0000-7000-8000-00000000000d")?);
    let uow = FakeBronzeUow::new(source_id, FakeFailureMode::None);
    // Object already present in R2 with this page's exact checksum, but no DB row recorded yet.
    let storage = FakeObjectStorage::with_existing_object(
        page.plan.object_key.as_str(),
        &page.plan.checksum_sha256,
    );
    let pages = vec![page.clone()];

    let report =
        persist_plans_with_adapters(&config, run_id, started_at, &pages, &uow, &storage).await?;

    // The run succeeded by recovery: the row was recorded (not a fresh write — the object existed).
    assert_eq!(report.objects_written, 1);
    assert_eq!(report.logical_records_seen, 1);
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

/// Quarantine terminal through the live persist path: the page's object is already in R2 but with a
/// DIFFERENT checksum, and no DB row exists. The committer cannot prove the object is ours, so it
/// fails loud and the run is marked Failed (never silently overwritten).
#[tokio::test]
async fn persist_fails_loud_when_r2_object_checksum_conflicts() -> anyhow::Result<()> {
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-00000000000e")?);
    let started_at = chrono::DateTime::parse_from_rfc3339("2026-05-14T00:00:00Z")?.to_utc();
    let config = test_config()?;
    let page = test_planned_page(run_id, 1)?;
    let source_id = SourceCatalogId::new(Uuid::parse_str("018f0000-0000-7000-8000-00000000000f")?);
    let uow = FakeBronzeUow::new(source_id, FakeFailureMode::None);
    // Object present at the key but with a DIFFERENT stored checksum than this run computed.
    let conflicting_sha = "0".repeat(64);
    let storage =
        FakeObjectStorage::with_existing_object(page.plan.object_key.as_str(), &conflicting_sha);
    let pages = vec![page];

    let error = persist_plans_with_adapters(&config, run_id, started_at, &pages, &uow, &storage)
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

#[tokio::test]
async fn reconcile_repairs_missing_bronze_metadata_from_existing_r2_object() -> anyhow::Result<()> {
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000010")?);
    let started_at = chrono::DateTime::parse_from_rfc3339("2026-05-14T00:00:00Z")?.to_utc();
    let source_id = SourceCatalogId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000011")?);
    let config = test_config()?;
    let plan = test_plan(run_id, 1)?;
    let mut source = super::source_catalog_entry(&config, started_at);
    source.id = source_id;
    let run = super::ingestion_run(
        source_id,
        run_id,
        started_at,
        super::batch_request_params(&config, std::slice::from_ref(&plan)),
    );
    let failed_run = IngestionRun {
        status: IngestionRunStatus::Failed,
        finished_at: Some(started_at),
        logical_records_seen: 0,
        objects_written: 1,
        error_message: Some("simulated metadata failure".to_owned()),
        ..run
    };
    let repo = FakeBronzeRepo::new(source, failed_run.clone(), Vec::new(), Vec::new());
    let uow =
        FakeBronzeUow::new_with_runs(source_id, FakeFailureMode::None, vec![failed_run.clone()]);
    let reader = FakeBronzeObjectReader::new([(
        plan.object_key.as_str().to_owned(),
        plan.raw_payload.clone(),
    )]);
    let source_identity = BuildingRegisterSourceIdentity {
        source_slug: config.source_slug.clone(),
    };

    let report = reconcile_building_register_run_with_adapters(
        &source_identity,
        run_id,
        &repo,
        &uow,
        &reader,
    )
    .await?;

    assert_eq!(report.run_id, run_id);
    assert_eq!(report.objects_expected, 1);
    assert_eq!(report.objects_repaired, 1);
    assert_eq!(report.logical_records_seen, 1);

    let recorded_objects = uow.recorded_bronze_objects()?;
    assert_eq!(recorded_objects.len(), 1);
    assert_eq!(
        recorded_objects[0].object_key.as_str(),
        plan.object_key.as_str()
    );

    let recorded_profiles = uow.recorded_schema_profiles()?;
    assert!(recorded_profiles
        .iter()
        .any(|profile| profile.field_path == "response.body.items.item[].mgmBldrgstPk"));

    let completions = uow.completions()?;
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].status, IngestionRunStatus::Succeeded);
    assert_eq!(completions[0].logical_records_seen, 1);
    assert_eq!(completions[0].objects_written, 1);
    assert!(completions[0].error_message.is_none());
    Ok(())
}

#[tokio::test]
async fn reconcile_rejects_existing_bronze_metadata_mismatch() -> anyhow::Result<()> {
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000012")?);
    let started_at = chrono::DateTime::parse_from_rfc3339("2026-05-14T00:00:00Z")?.to_utc();
    let source_id = SourceCatalogId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000013")?);
    let config = test_config()?;
    let plan = test_plan(run_id, 1)?;
    let mut source = super::source_catalog_entry(&config, started_at);
    source.id = source_id;
    let run = IngestionRun {
        status: IngestionRunStatus::Failed,
        finished_at: Some(started_at),
        objects_written: 1,
        error_message: Some("simulated follow-up failure".to_owned()),
        ..super::ingestion_run(
            source_id,
            run_id,
            started_at,
            super::batch_request_params(&config, std::slice::from_ref(&plan)),
        )
    };
    let mut stale_object = super::bronze_object(source_id, run_id, started_at, &plan);
    stale_object.checksum_sha256 =
        "0000000000000000000000000000000000000000000000000000000000000000".to_owned();
    let repo = FakeBronzeRepo::new(source, run.clone(), vec![stale_object], Vec::new());
    let uow = FakeBronzeUow::new_with_runs(source_id, FakeFailureMode::None, vec![run]);
    let reader = FakeBronzeObjectReader::new([(
        plan.object_key.as_str().to_owned(),
        plan.raw_payload.clone(),
    )]);
    let source_identity = BuildingRegisterSourceIdentity {
        source_slug: config.source_slug.clone(),
    };

    let error = reconcile_building_register_run_with_adapters(
        &source_identity,
        run_id,
        &repo,
        &uow,
        &reader,
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("expected metadata mismatch failure"))?;

    assert!(
        error.to_string().contains("metadata mismatch"),
        "unexpected error: {error}"
    );
    assert!(uow.completions()?.is_empty());
    Ok(())
}

// Behavior coverage for the crash-stuck recovery (audit finding 7): a Running run is abandoned
// to Cancelled, and an already-terminal run is rejected. Reuses the Fake repo/uow so the async
// path — not just the pure eligibility check — is exercised.
#[tokio::test]
async fn abandon_ingestion_run_cancels_a_running_run() -> anyhow::Result<()> {
    let source_id = SourceCatalogId::new(Uuid::parse_str("018f0000-0000-7000-8000-0000000000a1")?);
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-0000000000a2")?);
    let started_at = chrono::DateTime::parse_from_rfc3339("2026-05-14T00:00:00Z")?.to_utc();
    let config = test_config()?;
    let mut source = super::source_catalog_entry(&config, started_at);
    source.id = source_id;
    // ingestion_run() builds a Running run (the state a crash leaves behind).
    let running = super::ingestion_run(source_id, run_id, started_at, json!({}));
    let repo = FakeBronzeRepo::new(source, running.clone(), Vec::new(), Vec::new());
    let uow = FakeBronzeUow::new_with_runs(source_id, FakeFailureMode::None, vec![running]);

    let cancelled =
        crate::ingestion_run_recovery::abandon_ingestion_run(&repo, &uow, run_id, "operator: dead")
            .await?;

    assert_eq!(cancelled.status, IngestionRunStatus::Cancelled);
    let completions = uow.completions()?;
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].status, IngestionRunStatus::Cancelled);
    assert_eq!(
        completions[0].error_message.as_deref(),
        Some("operator: dead")
    );
    Ok(())
}

#[tokio::test]
async fn abandon_ingestion_run_rejects_a_terminal_run() -> anyhow::Result<()> {
    let source_id = SourceCatalogId::new(Uuid::parse_str("018f0000-0000-7000-8000-0000000000b1")?);
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-0000000000b2")?);
    let started_at = chrono::DateTime::parse_from_rfc3339("2026-05-14T00:00:00Z")?.to_utc();
    let config = test_config()?;
    let mut source = super::source_catalog_entry(&config, started_at);
    source.id = source_id;
    let succeeded = IngestionRun {
        status: IngestionRunStatus::Succeeded,
        finished_at: Some(started_at),
        ..super::ingestion_run(source_id, run_id, started_at, json!({}))
    };
    let repo = FakeBronzeRepo::new(source, succeeded.clone(), Vec::new(), Vec::new());
    let uow = FakeBronzeUow::new_with_runs(source_id, FakeFailureMode::None, vec![succeeded]);

    let error = match crate::ingestion_run_recovery::abandon_ingestion_run(&repo, &uow, run_id, "x")
        .await
    {
        Ok(_) => anyhow::bail!("a terminal run must not be abandonable"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("already terminal"),
        "unexpected error: {error}"
    );
    // No state change: the terminal run is left untouched.
    assert!(uow.completions()?.is_empty());
    Ok(())
}

fn test_config() -> anyhow::Result<BuildingRegisterIngestConfig> {
    Ok(BuildingRegisterIngestConfig {
        source_slug: "datagokr__building_register_main".to_owned(),
        base_uri: "https://apis.data.go.kr/1613000/BldRgstHubService".to_owned(),
        service_key: "redacted-test-key".to_owned(),
        request: BuildingRegisterPageRequest {
            operation: "getBrTitleInfo".to_owned(),
            sigungu_cd: "11680".to_owned(),
            bjdong_cd: "10300".to_owned(),
            page_no: 1,
            num_of_rows: 100,
        },
        max_pages: 1,
        allow_partial_page_window: false,
        request_spacing: None,
        request_policy: DataGoKrRequestPolicy::new(
            3,
            Duration::from_secs(5),
            Duration::ZERO,
            Duration::ZERO,
        )?,
        live_write: Some("1".to_owned()),
    })
}

fn test_plan(
    run_id: IngestionRunId,
    page_no: u32,
) -> anyhow::Result<super::BuildingRegisterBronzePagePlan> {
    Ok(test_planned_page(run_id, page_no)?.plan)
}

/// Builds one fetched-and-planned building-register page (the carrier `plan_pages` now produces and
/// `persist_plans_with_adapters` consumes): the compiled plan plus the RAW request + payload the
/// committer recompiles from.
fn test_planned_page(
    run_id: IngestionRunId,
    page_no: u32,
) -> anyhow::Result<BuildingRegisterPlannedPage> {
    let payload = json!({
        "response": {
            "body": {
                "items": {
                    "item": [
                        {
                            "mgmBldrgstPk": format!("11680-10300-{page_no}"),
                            "totArea": "100.25"
                        }
                    ]
                }
            }
        }
    });
    let raw_payload = serde_json::to_vec(&payload)?;
    let request = BuildingRegisterPageRequest {
        operation: "getBrTitleInfo".to_owned(),
        sigungu_cd: "11680".to_owned(),
        bjdong_cd: "10300".to_owned(),
        page_no,
        num_of_rows: 100,
    };
    let plan = collection_application::plan_building_register_bronze_page(
        BuildingRegisterBronzePagePlanInput {
            source_slug: "datagokr__building_register_main",
            ingest_date: NaiveDate::from_ymd_opt(2026, 5, 14)
                .ok_or_else(|| anyhow::anyhow!("valid date"))?,
            ingestion_run_id: run_id,
            request: request.clone(),
            raw_payload: raw_payload.clone(),
            payload: payload.clone(),
        },
    )
    .map_err(anyhow::Error::from)?;
    Ok(BuildingRegisterPlannedPage {
        plan,
        request,
        raw_payload,
        payload,
    })
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

    fn new_with_runs(
        source_id: SourceCatalogId,
        failure_mode: FakeFailureMode,
        runs: Vec<IngestionRun>,
    ) -> Self {
        Self {
            source_id,
            failure_mode,
            runs: Mutex::new(runs),
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

    fn recorded_schema_profiles(&self) -> anyhow::Result<Vec<SchemaProfile>> {
        Ok(self
            .schema_profiles
            .lock()
            .map_err(|_| anyhow::anyhow!("schema profile lock poisoned"))?
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
            .map_err(|_| CollectionError::Infrastructure("completion lock poisoned".to_owned()))?
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
            .map_err(|_| CollectionError::Infrastructure("bronze object lock poisoned".to_owned()))?
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
            .map_err(|_| CollectionError::Infrastructure("bronze object lock poisoned".to_owned()))?
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

struct FakeBronzeRepo {
    source: SourceCatalogEntry,
    run: IngestionRun,
    bronze_objects: Vec<BronzeObject>,
    schema_profiles: Vec<SchemaProfile>,
}

impl FakeBronzeRepo {
    const fn new(
        source: SourceCatalogEntry,
        run: IngestionRun,
        bronze_objects: Vec<BronzeObject>,
        schema_profiles: Vec<SchemaProfile>,
    ) -> Self {
        Self {
            source,
            run,
            bronze_objects,
            schema_profiles,
        }
    }
}

#[async_trait]
impl BronzeIngestRepository for FakeBronzeRepo {
    async fn find_source_catalog_by_slug(
        &self,
        slug: &str,
    ) -> Result<Option<SourceCatalogEntry>, CollectionError> {
        Ok((self.source.slug == slug).then(|| self.source.clone()))
    }

    async fn find_ingestion_run(
        &self,
        id: IngestionRunId,
    ) -> Result<Option<IngestionRun>, CollectionError> {
        Ok((self.run.id == id).then(|| self.run.clone()))
    }

    async fn list_bronze_objects_by_run(
        &self,
        run_id: IngestionRunId,
    ) -> Result<Vec<BronzeObject>, CollectionError> {
        Ok(if self.run.id == run_id {
            self.bronze_objects.clone()
        } else {
            Vec::new()
        })
    }

    async fn find_bronze_object_by_source_partition_key(
        &self,
        source_catalog_id: SourceCatalogId,
        source_partition_key: &str,
    ) -> Result<Option<BronzeObject>, CollectionError> {
        Ok(self
            .bronze_objects
            .iter()
            .find(|object| {
                object.source_catalog_id == source_catalog_id
                    && object.source_partition_key.as_deref() == Some(source_partition_key)
            })
            .cloned())
    }

    async fn list_schema_profiles_by_run(
        &self,
        run_id: IngestionRunId,
    ) -> Result<Vec<SchemaProfile>, CollectionError> {
        Ok(if self.run.id == run_id {
            self.schema_profiles.clone()
        } else {
            Vec::new()
        })
    }
}

struct FakeBronzeObjectReader {
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl FakeBronzeObjectReader {
    fn new<const N: usize>(objects: [(String, Vec<u8>); N]) -> Self {
        Self {
            objects: Mutex::new(BTreeMap::from(objects)),
        }
    }
}

#[async_trait]
impl BuildingRegisterBronzeObjectReader for FakeBronzeObjectReader {
    async fn read_object_bytes(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        self.objects
            .lock()
            .map_err(|_| anyhow::anyhow!("reader lock poisoned"))?
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("missing fake object {key}"))
    }
}

#[derive(Default)]
struct FakeObjectStorage {
    written_keys: Mutex<Vec<String>>,
    /// Pre-seeded objects already in storage, keyed by object key -> stored `x-amz-meta-sha256`.
    /// A `CreateOnly` write to a seeded key returns `ObjectAlreadyExists` (the R2 412), driving the
    /// committer's recoverable commit protocol; `read_object_sha256` reports the seeded checksum.
    existing: Mutex<BTreeMap<String, String>>,
    fail_message: Option<String>,
}

impl FakeObjectStorage {
    fn failing(message: &str) -> Self {
        Self {
            written_keys: Mutex::new(Vec::new()),
            existing: Mutex::new(BTreeMap::new()),
            fail_message: Some(message.to_owned()),
        }
    }

    /// Pre-seeds an object already present at `key` with stored checksum `sha256` (simulating a
    /// prior R2 write whose DB record then failed).
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
        if let Some(message) = &self.fail_message {
            return Err(PublishError::Infrastructure(message.clone()));
        }
        // CreateOnly collision with a pre-seeded key surfaces as ObjectAlreadyExists (R2 412).
        if matches!(
            request.write_mode,
            foundation_outbox::object_storage::ObjectWriteMode::CreateOnly
        ) && self
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
