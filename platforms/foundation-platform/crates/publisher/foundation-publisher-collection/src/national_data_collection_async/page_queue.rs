use foundation_shared_kernel::ids::IngestionRunId;

use crate::pagination_guard::{
    assert_page_window_slice_complete, ASYNC_SHARD_WINDOW_DEFERS_TO_COVERAGE_MANIFEST,
};

use super::{
    events::{failed_event, succeeded_event, EventWriter},
    page_numbers_for_entry, real_transaction, JobSuccessReport, LedgerEntry, PageSuccessReport,
};

#[derive(Clone)]
pub(super) struct PageTask {
    pub(super) entry: LedgerEntry,
    pub(super) run_id: IngestionRunId,
    pub(super) ingest_date: chrono::NaiveDate,
    pub(super) page_no: u32,
}

pub(super) enum PageTaskOutcome {
    Succeeded(PageSuccessReport),
    Failed {
        job_id: String,
        error_message: String,
    },
}

pub(super) fn aggregate_page_queue_results(
    selected: &[LedgerEntry],
    page_outcomes: Vec<PageTaskOutcome>,
    compiler_input_hash: &str,
    event_writer: &EventWriter,
) -> anyhow::Result<Vec<anyhow::Result<JobSuccessReport>>> {
    let mut successes = std::collections::BTreeMap::<String, Vec<PageSuccessReport>>::new();
    let mut failures = std::collections::BTreeMap::<String, Vec<String>>::new();
    for outcome in page_outcomes {
        match outcome {
            PageTaskOutcome::Succeeded(report) => {
                successes
                    .entry(report.job_id.clone())
                    .or_default()
                    .push(report);
            }
            PageTaskOutcome::Failed {
                job_id,
                error_message,
            } => {
                failures.entry(job_id).or_default().push(error_message);
            }
        }
    }

    let mut results = Vec::new();
    for entry in selected {
        if let Some(errors) = failures.get(&entry.job_id) {
            let error_message = errors
                .first()
                .cloned()
                .unwrap_or_else(|| "page queue job failed".to_owned());
            event_writer.write_event(&failed_event(
                entry,
                compiler_input_hash,
                error_message.clone(),
            ))?;
            results.push(Err(anyhow::anyhow!(error_message)));
            continue;
        }

        let expected_page_count = page_numbers_for_entry(entry)?.len();
        let mut reports = successes.remove(&entry.job_id).unwrap_or_default();
        reports.sort_by_key(|report| report.page_no);
        if reports.len() != expected_page_count {
            let error_message = format!(
                "page queue job missing pages: job_id={} expected={} actual={}",
                entry.job_id,
                expected_page_count,
                reports.len()
            );
            event_writer.write_event(&failed_event(
                entry,
                compiler_input_hash,
                error_message.clone(),
            ))?;
            results.push(Err(anyhow::anyhow!(error_message)));
            continue;
        }

        if let Some(last_page) = reports.last() {
            let source_label = if real_transaction::is_entry(entry) {
                "real-transaction"
            } else {
                "building-register"
            };
            // Async lane: this collects a page-window SHARD FRAGMENT, not a full provider scope.
            // The guard intentionally does NOT assert full provider coverage here. Full
            // "전국 누락 없음" completeness is asserted only by the national coverage manifest
            // (check-national-bronze-object-manifest); this run's evidence is not a completeness
            // claim.
            assert_page_window_slice_complete(
                source_label,
                last_page.page_no,
                last_page.effective_page_size,
                last_page.logical_record_count,
                // shard-fragment mode short-circuits the completeness check below.
                last_page.logical_record_count,
                last_page.provider_total_count,
                entry.max_pages,
                ASYNC_SHARD_WINDOW_DEFERS_TO_COVERAGE_MANIFEST,
            )?;
        }

        let provider_request_count = reports.len() as u64;
        let source_record_count = reports
            .iter()
            .map(|report| report.source_record_count)
            .sum::<u64>();
        let bronze_size_bytes = reports
            .iter()
            .map(|report| report.bronze_size_bytes)
            .sum::<u64>();
        let last_object_key = reports
            .last()
            .map(|report| report.object_key.clone())
            .unwrap_or_default();
        let last_checksum_sha256 = reports
            .last()
            .map(|report| report.checksum_sha256.clone())
            .unwrap_or_default();
        let report = JobSuccessReport {
            provider_request_count,
            source_record_count,
            bronze_size_bytes,
            last_object_key,
            last_checksum_sha256,
        };
        event_writer.write_event(&succeeded_event(entry, compiler_input_hash, &report))?;
        results.push(Ok(report));
    }

    Ok(results)
}
