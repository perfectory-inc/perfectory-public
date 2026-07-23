use std::path::Path;

use serde_json::{json, Value as JsonValue};

use super::{
    append_event_log_entry, base_event, string_prop, validate_reuse_identity, ExecutionStats,
    ReuseEntry,
};

pub(in crate::national_data_collection_ledger_execute) fn record_reused_job(
    event_log_path: &Path,
    job: &JsonValue,
    reuse: &ReuseEntry,
    stats: &mut ExecutionStats,
) -> anyhow::Result<()> {
    validate_reuse_identity(job, reuse)?;
    stats.reused_job_count += 1;
    stats.succeeded_job_count += 1;
    stats.source_record_count += reuse.source_record_count;
    stats.request_count_total += reuse.page_count;
    if reuse.source_record_count == 0 {
        stats.empty_job_count += 1;
    }
    append_event_log_entry(
        event_log_path,
        &base_event(
            job,
            "job_reused",
            "succeeded",
            json!({
                "request_count": reuse.page_count,
                "provider_request_count": 0,
                "storage_driver": reuse.storage_driver,
                "source_record_count": reuse.source_record_count,
                "bronze_object_key": reuse.last_object_key,
                "bronze_checksum_sha256": "",
                "bronze_size_bytes": 0,
                "reused_bronze_object": true
            }),
        ),
    )?;
    stats.event_count += 1;
    Ok(())
}

pub(in crate::national_data_collection_ledger_execute) fn record_provider_empty_job(
    event_log_path: &Path,
    job: &JsonValue,
    stats: &mut ExecutionStats,
) -> anyhow::Result<()> {
    stats.empty_job_count += 1;
    stats.succeeded_job_count += 1;
    append_event_log_entry(
        event_log_path,
        &base_event(
            job,
            "job_provider_empty",
            "succeeded",
            json!({
                "request_count": 0,
                "provider_request_count": 0,
                "storage_driver": "not_applicable",
                "source_record_count": 0,
                "bronze_object_key": "",
                "bronze_checksum_sha256": "",
                "bronze_size_bytes": 0,
                "reused_bronze_object": false,
                "provider_empty_reason": string_prop(job, "provider_empty_reason"),
                "page_count_source": string_prop(job, "page_count_source")
            }),
        ),
    )?;
    stats.event_count += 1;
    Ok(())
}
