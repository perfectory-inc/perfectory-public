use anyhow::{bail, Context};
use chrono::Utc;
pub(super) use collection_application::BuildingRegisterPageRequest;
use foundation_shared_kernel::ids::IngestionRunId;
use uuid::Uuid;

use crate::pagination_guard::{
    assert_page_window_slice_complete, ASYNC_SHARD_WINDOW_DEFERS_TO_COVERAGE_MANIFEST,
};

use super::{
    effective_page_size_from_response_metadata, json_u64_pointer, page_numbers_for_entry,
    JobExecutionState, JobSuccessReport, LedgerEntry, PageSuccessReport,
};

pub(super) async fn execute_job_pages(
    entry: &LedgerEntry,
    state: &JobExecutionState,
) -> anyhow::Result<JobSuccessReport> {
    if entry.provider != "data.go.kr" {
        bail!("national async collector only supports data.go.kr jobs in this phase");
    }
    let page_numbers = page_numbers_for_entry(entry)?;

    let run_id = IngestionRunId::new(Uuid::new_v4());
    let ingest_date = Utc::now().date_naive();
    let mut provider_request_count = 0_u64;
    let mut source_record_count = 0_u64;
    let mut bronze_size_bytes = 0_u64;
    let mut last_object_key = String::new();
    let mut last_checksum_sha256 = String::new();
    let mut last_page_observation = None;

    for page_no in page_numbers {
        let page_report = execute_single_page(entry, page_no, run_id, ingest_date, state).await?;
        provider_request_count += 1;
        source_record_count += page_report.source_record_count;
        bronze_size_bytes += page_report.bronze_size_bytes;
        last_object_key = page_report.object_key.clone();
        last_checksum_sha256 = page_report.checksum_sha256.clone();
        last_page_observation = Some((
            page_report.page_no,
            page_report.effective_page_size,
            page_report.logical_record_count,
            page_report.provider_total_count,
        ));
    }

    if let Some((last_page, page_size, logical_record_count, provider_total_count)) =
        last_page_observation
    {
        // Async lane: this collects a page-window SHARD FRAGMENT, not a full provider scope. The
        // guard intentionally does NOT assert full provider coverage here. Full "전국 누락 없음"
        // completeness is asserted only by the national coverage manifest
        // (check-national-bronze-object-manifest); this run's evidence is not a completeness claim.
        assert_page_window_slice_complete(
            "building-register",
            last_page,
            page_size,
            logical_record_count,
            // shard-fragment mode short-circuits the completeness check below.
            logical_record_count,
            provider_total_count,
            entry.max_pages,
            ASYNC_SHARD_WINDOW_DEFERS_TO_COVERAGE_MANIFEST,
        )?;
    }
    if last_object_key.is_empty() {
        bail!("building-register async job wrote no Bronze objects");
    }

    Ok(JobSuccessReport {
        provider_request_count,
        source_record_count,
        bronze_size_bytes,
        last_object_key,
        last_checksum_sha256,
    })
}

pub(super) async fn execute_single_page(
    entry: &LedgerEntry,
    page_no: u32,
    // The per-page plan run id is no longer used: the committer records the `bronze_object` row
    // against the per-source-slug `ingestion_run` the context prepared (a real FK target). Kept in
    // the signature so the dispatcher + page-queue task carrier stay uniform across lanes.
    _run_id: IngestionRunId,
    ingest_date: chrono::NaiveDate,
    state: &JobExecutionState,
) -> anyhow::Result<PageSuccessReport> {
    let request = BuildingRegisterPageRequest {
        operation: entry.operation.clone(),
        sigungu_cd: entry.sigungu_cd.clone(),
        bjdong_cd: entry.bjdong_cd.clone(),
        page_no,
        num_of_rows: entry.num_of_rows,
    };
    let handle = super::acquire_lane(state, entry).await?;
    let result = state.client.fetch_page(&request).await;
    super::record_lane(handle, &result)?;
    let fetched = result.with_context(|| format!("failed to fetch data.go.kr page {page_no}"))?;
    let provider_total_count = json_u64_pointer(&fetched.payload, "/response/body/totalCount")?;
    let effective_page_size =
        effective_page_size_from_response_metadata(&fetched.payload, request.num_of_rows)?;

    // ADR 0016 option-a: route the raw write through the single BronzeCommitter (via the shared
    // BronzeIngestContext) instead of a direct put. The committer OWNS the key compile (running the
    // SAME `plan_building_register_bronze_page` this lane used before — so the object key is
    // byte-identical), writes the bytes write-once (`CreateOnly`), and records the `bronze_object`
    // row with the recoverable 412 commit protocol. The recorded row's `ingestion_run_id` comes
    // from the per-source-slug run the context prepared.
    let outcome = state
        .bronze
        .commit_page(
            &entry.source_slug,
            ingest_date,
            Utc::now(),
            request,
            fetched.raw_payload,
            fetched.payload,
        )
        .await
        .with_context(|| format!("failed to commit Bronze object for job {}", entry.job_id))?;
    let plan = outcome.plan;

    Ok(PageSuccessReport {
        job_id: entry.job_id.clone(),
        page_no,
        effective_page_size,
        logical_record_count: plan.logical_record_count,
        provider_total_count,
        source_record_count: plan.logical_record_count,
        bronze_size_bytes: plan.size_bytes,
        object_key: outcome.object_key,
        checksum_sha256: outcome.checksum_sha256,
    })
}
