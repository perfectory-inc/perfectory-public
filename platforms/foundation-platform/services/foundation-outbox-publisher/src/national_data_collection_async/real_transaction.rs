use std::{collections::BTreeMap, sync::Arc};

use anyhow::{bail, Context};
use chrono::Utc;
use collection_application::RealTransactionPageRequest;
use collection_infrastructure::{
    DataGoKrRequestPolicy, DataGoKrServiceApiClient, DataGoKrServiceApiConfig,
};
use foundation_shared_kernel::ids::IngestionRunId;
use uuid::Uuid;

use crate::pagination_guard::{
    assert_page_window_slice_complete, ASYNC_SHARD_WINDOW_DEFERS_TO_COVERAGE_MANIFEST,
};

use super::{
    effective_page_size_from_response_metadata, json_u64_pointer, page_numbers_for_entry,
    JobExecutionState, JobSuccessReport, LedgerEntry, PageSuccessReport,
};

#[derive(Clone, Debug, Default)]
pub(super) struct RealTransactionServiceApiClients {
    by_operation: BTreeMap<String, Arc<DataGoKrServiceApiClient>>,
}

impl RealTransactionServiceApiClients {
    pub(super) fn build(
        entries: &[LedgerEntry],
        service_key: &str,
        request_policy: DataGoKrRequestPolicy,
    ) -> anyhow::Result<Self> {
        let mut by_operation = BTreeMap::new();
        for entry in entries.iter().filter(|entry| is_entry(entry)) {
            if by_operation.contains_key(&entry.operation) {
                continue;
            }
            let base_uri =
                crate::real_transaction_ingest::default_base_uri_for_operation(&entry.operation)?;
            let client = DataGoKrServiceApiClient::new_with_policy(
                &DataGoKrServiceApiConfig {
                    base_uri,
                    service_key: service_key.to_owned(),
                    user_agent: "foundation-platform-national-real-transaction-async/1.0"
                        .to_owned(),
                },
                request_policy,
            )
            .with_context(|| {
                format!(
                    "failed to build shared data.go.kr real-transaction client for {}",
                    entry.operation
                )
            })?;
            by_operation.insert(entry.operation.clone(), Arc::new(client));
        }
        Ok(Self { by_operation })
    }

    fn client_for_operation(
        &self,
        operation: &str,
    ) -> anyhow::Result<Arc<DataGoKrServiceApiClient>> {
        self.by_operation
            .get(operation)
            .cloned()
            .with_context(|| format!("missing shared real-transaction client for {operation}"))
    }
}

pub(super) fn is_entry(entry: &LedgerEntry) -> bool {
    entry
        .endpoint_slug
        .starts_with("data-go-kr-real-transaction-")
        || entry.operation.starts_with("getRTMSDataSvc")
}

pub(super) fn request_for_entry(
    entry: &LedgerEntry,
    page_no: u32,
) -> anyhow::Result<RealTransactionPageRequest> {
    if entry.lawd_cd.trim().is_empty() || entry.deal_ymd.trim().is_empty() {
        bail!(
            "real-transaction ledger entry requires lawd_cd and deal_ymd: {}",
            entry.job_id
        );
    }
    Ok(RealTransactionPageRequest {
        operation: entry.operation.clone(),
        lawd_cd: entry.lawd_cd.clone(),
        deal_ymd: entry.deal_ymd.clone(),
        page_no,
        num_of_rows: entry.num_of_rows,
    })
}

pub(super) async fn execute_job_pages(
    entry: &LedgerEntry,
    state: &JobExecutionState,
) -> anyhow::Result<JobSuccessReport> {
    if entry.provider != "data.go.kr" {
        bail!("real-transaction async collector only supports data.go.kr jobs");
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
            "real-transaction",
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
        bail!("real-transaction async job wrote no Bronze objects");
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
    // See building_register::execute_single_page: the per-page plan run id is unused now (the
    // committer records against the per-source-slug prepared run); kept for a uniform signature.
    _run_id: IngestionRunId,
    ingest_date: chrono::NaiveDate,
    state: &JobExecutionState,
) -> anyhow::Result<PageSuccessReport> {
    let client = state
        .real_transaction_clients
        .client_for_operation(&entry.operation)?;
    let request = request_for_entry(entry, page_no)?;
    let public_request = request.to_public_data_request().with_context(|| {
        format!(
            "failed to build real-transaction request for {} page {}",
            request.operation, request.page_no
        )
    })?;
    let handle = super::acquire_lane(state, entry).await?;
    let result = client.fetch_page(&public_request).await;
    super::record_lane(handle, &result)?;
    let fetched = result.with_context(|| {
        format!(
            "failed to fetch data.go.kr real-transaction operation {} page {}",
            request.operation, request.page_no
        )
    })?;
    let provider_total_count = json_u64_pointer(&fetched.payload, "/response/body/totalCount")?;
    let effective_page_size =
        effective_page_size_from_response_metadata(&fetched.payload, request.num_of_rows)?;

    // ADR 0016 option-a: route the raw write through the single BronzeCommitter (via the shared
    // BronzeIngestContext). The committer OWNS the key compile (running the SAME
    // `plan_real_transaction_bronze_page` this lane used before — so the object key is
    // byte-identical), writes write-once (`CreateOnly`), and records the `bronze_object` row with the
    // recoverable 412 commit protocol.
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
        .with_context(|| {
            format!(
                "failed to commit real-transaction Bronze for {}",
                entry.job_id
            )
        })?;
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
