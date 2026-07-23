use std::collections::BTreeMap;

use serde_json::{json, Value as JsonValue};

use super::{
    append_event_log_entry, base_event, parse_r2_run_summary, read_local_bronze_result,
    request_count, safe_runner_error_message, set_job_environment, string_prop,
    write_dependency_metric, write_quota_metric, Config, ExecutionStats, Runner, StorageDriver,
};

pub(in crate::national_data_collection_ledger_execute) fn run_provider_job(
    config: &Config,
    job: &JsonValue,
    runner: &Runner,
    dotenv: &BTreeMap<String, String>,
    stats: &mut ExecutionStats,
) -> anyhow::Result<()> {
    append_event_log_entry(
        &config.event_log_path,
        &base_event(
            job,
            "job_started",
            "running",
            json!({
                "request_count": 0
            }),
        ),
    )?;
    stats.event_count += 1;
    let mut child_env = dotenv.clone();
    let command = set_job_environment(job, config, &mut child_env);
    let run = runner.invoke(&config.root, &command, &child_env)?;
    let provider = string_prop(job, "provider");
    let endpoint = string_prop(job, "endpoint");
    let request_count = request_count(job);
    if run.exit_code != 0 {
        write_quota_metric(
            &config.quota_metrics_path,
            &provider,
            &endpoint,
            request_count,
            "failed",
        )?;
        write_dependency_metric(
            &config.quota_metrics_path,
            &provider,
            &endpoint,
            run.duration,
            "failed",
        )?;
        stats.failed_job_count += 1;
        stats.provider_request_count_total += request_count;
        append_event_log_entry(
            &config.event_log_path,
            &base_event(
                job,
                "job_failed",
                "failed",
                json!({
                    "request_count": request_count,
                    "provider_request_count": request_count,
                    "error_kind": format!("cargo_exit_code_{}", run.exit_code),
                    "error_message": safe_runner_error_message(&run.output)
                }),
            ),
        )?;
        stats.event_count += 1;
        return Ok(());
    }
    write_dependency_metric(
        &config.quota_metrics_path,
        &provider,
        &endpoint,
        run.duration,
        "succeeded",
    )?;
    let bronze = match config.bronze_storage_driver {
        StorageDriver::Local => read_local_bronze_result(
            &config.local_object_root,
            run.started_at,
            &string_prop(job, "source_slug"),
            request_count,
        )?,
        StorageDriver::R2 => parse_r2_run_summary(&run.output)?,
    };
    write_quota_metric(
        &config.quota_metrics_path,
        &provider,
        &endpoint,
        bronze.request_count,
        "attempted",
    )?;
    if bronze.record_count == 0 {
        stats.empty_job_count += 1;
    }
    stats.succeeded_job_count += 1;
    stats.source_record_count += bronze.record_count;
    stats.bronze_total_size_bytes += bronze.size_bytes;
    stats.request_count_total += bronze.request_count;
    stats.provider_request_count_total += bronze.request_count;
    append_event_log_entry(
        &config.event_log_path,
        &base_event(
            job,
            "job_succeeded",
            "succeeded",
            json!({
                "request_count": bronze.request_count,
                "provider_request_count": bronze.request_count,
                "storage_driver": config.bronze_storage_driver.as_str(),
                "source_record_count": bronze.record_count,
                "bronze_object_key": bronze.object_key,
                "bronze_checksum_sha256": bronze.checksum_sha256,
                "bronze_size_bytes": bronze.size_bytes,
                "reused_bronze_object": false
            }),
        ),
    )?;
    stats.event_count += 1;
    Ok(())
}
