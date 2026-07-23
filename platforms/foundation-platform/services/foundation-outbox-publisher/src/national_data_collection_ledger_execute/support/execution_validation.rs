use std::fs;

use anyhow::bail;
use serde_json::Value as JsonValue;

use super::{
    import_dotenv, is_provider_empty_job, require_env, require_r2_env, string_prop, Config,
    ReuseIndex, StorageDriver,
};

pub(in crate::national_data_collection_ledger_execute) fn validate_execution_inputs(
    config: &Config,
    jobs: &[JsonValue],
    reuse: &ReuseIndex,
) -> anyhow::Result<()> {
    if !config.confirm_public_api_quota_impact {
        bail!("Public API quota impact must be confirmed with -ConfirmPublicApiQuotaImpact when -Execute is used");
    }
    if !config.confirm_national_ledger_execution {
        bail!("ConfirmNationalLedgerExecution is required when -Execute is used");
    }
    let dotenv = import_dotenv(&config.env_file)?;
    let jobs_needing_provider = jobs
        .iter()
        .filter(|job| !reuse.contains(job) && !is_provider_empty_job(job))
        .collect::<Vec<_>>();
    if config.bronze_storage_driver == StorageDriver::Local
        && !jobs_needing_provider.is_empty()
        && !config.confirm_local_bronze_storage
    {
        bail!("Local Bronze storage is proof-only and requires -ConfirmLocalBronzeStorage when -Execute is used");
    }
    if !jobs_needing_provider.is_empty() {
        require_env(&dotenv, "DATABASE_URL")?;
    }
    if jobs_needing_provider
        .iter()
        .any(|job| string_prop(job, "provider") == "data.go.kr")
    {
        require_env(&dotenv, "DATA_GO_KR_SERVICE_KEY")?;
    }
    if jobs_needing_provider
        .iter()
        .any(|job| string_prop(job, "provider") == "VWorld")
    {
        require_env(&dotenv, "VWORLD_API_KEY")?;
    }
    if config.bronze_storage_driver == StorageDriver::R2 && !jobs_needing_provider.is_empty() {
        require_r2_env(&dotenv)?;
    }
    if config.bronze_storage_driver == StorageDriver::Local && !jobs_needing_provider.is_empty() {
        fs::create_dir_all(&config.local_object_root)?;
    }
    Ok(())
}
