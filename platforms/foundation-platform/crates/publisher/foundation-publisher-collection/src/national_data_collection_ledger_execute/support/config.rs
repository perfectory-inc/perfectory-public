use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};

use crate::public_data_control_support::{env_path, repo_relative_path, resolve_repo_path};

use super::{
    env_bool, env_i64, env_string, env_u64, env_usize, normalize_windows_verbatim_path,
    optional_path_env,
};

const DEFAULT_PLAN_PATH: &str = "target/audit/national-data-collection-plan.json";
const DEFAULT_EVENT_LOG_PATH: &str = "target/audit/national-data-collection-ledger-events.jsonl";
const DEFAULT_EVIDENCE_PATH: &str =
    "target/audit/national-data-collection-ledger-execution-evidence.json";
const DEFAULT_QUOTA_METRICS_PATH: &str =
    "target/public-api-quota/national-data-collection-ledger-execution.prom";
const DEFAULT_LOCAL_OBJECT_ROOT: &str = "target/bronze-national-data-collection";

pub(in crate::national_data_collection_ledger_execute) struct Config {
    pub(in crate::national_data_collection_ledger_execute) root: PathBuf,
    pub(in crate::national_data_collection_ledger_execute) env_file: PathBuf,
    pub(in crate::national_data_collection_ledger_execute) plan_path: PathBuf,
    pub(in crate::national_data_collection_ledger_execute) event_log_path: PathBuf,
    pub(in crate::national_data_collection_ledger_execute) evidence_path: PathBuf,
    pub(in crate::national_data_collection_ledger_execute) quota_metrics_path: PathBuf,
    pub(in crate::national_data_collection_ledger_execute) local_object_root: PathBuf,
    pub(in crate::national_data_collection_ledger_execute) reuse_manifest_path: Option<PathBuf>,
    pub(in crate::national_data_collection_ledger_execute) bronze_storage_driver: StorageDriver,
    pub(in crate::national_data_collection_ledger_execute) cargo_exe: Option<PathBuf>,
    pub(in crate::national_data_collection_ledger_execute) runner_exe: Option<PathBuf>,
    pub(in crate::national_data_collection_ledger_execute) job_ids_path: Option<PathBuf>,
    pub(in crate::national_data_collection_ledger_execute) provider_min_page_interval_ms: u64,
    pub(in crate::national_data_collection_ledger_execute) max_jobs: usize,
    pub(in crate::national_data_collection_ledger_execute) skip_jobs: usize,
    pub(in crate::national_data_collection_ledger_execute) request_cap: u64,
    pub(in crate::national_data_collection_ledger_execute) execute: bool,
    pub(in crate::national_data_collection_ledger_execute) reuse_validated_bronze_objects: bool,
    pub(in crate::national_data_collection_ledger_execute) confirm_public_api_quota_impact: bool,
    pub(in crate::national_data_collection_ledger_execute) confirm_national_ledger_execution: bool,
    pub(in crate::national_data_collection_ledger_execute) confirm_local_bronze_storage: bool,
}

impl Config {
    pub(in crate::national_data_collection_ledger_execute) fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );
        let env_file = optional_path_env("FOUNDATION_PLATFORM_NATIONAL_LEDGER_ENV_FILE")?
            .map(|path| resolve_repo_path(&root, &path, "EnvFile"))
            .transpose()?
            .unwrap_or_else(|| root.join(".env.local"));
        let reuse_manifest_path = optional_path_env(
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_REUSE_BRONZE_OBJECT_MANIFEST_PATH",
        )?
        .map(|path| resolve_repo_path(&root, &path, "ReuseBronzeObjectManifestPath"))
        .transpose()?;
        let runner_exe = optional_path_env("FOUNDATION_PLATFORM_NATIONAL_LEDGER_RUNNER_EXE")?
            .map(|path| resolve_external_path(&root, path));
        let cargo_exe = optional_path_env("FOUNDATION_PLATFORM_NATIONAL_LEDGER_CARGO_EXE")?;
        let job_ids_path = optional_path_env("FOUNDATION_PLATFORM_NATIONAL_LEDGER_JOB_IDS_PATH")?
            .map(|path| resolve_repo_path(&root, &path, "JobIdsPath"))
            .transpose()?;
        let bronze_storage_driver = StorageDriver::parse(&env_string(
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_BRONZE_STORAGE_DRIVER",
            "r2",
        )?)?;
        let max_jobs = env_usize("FOUNDATION_PLATFORM_NATIONAL_LEDGER_MAX_JOBS", 100)?;
        let skip_jobs = env_usize("FOUNDATION_PLATFORM_NATIONAL_LEDGER_SKIP_JOBS", 0)?;
        let request_cap = env_u64("FOUNDATION_PLATFORM_NATIONAL_LEDGER_REQUEST_CAP", 100)?;
        let provider_min_page_interval_ms = env_u64(
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_PROVIDER_MIN_PAGE_INTERVAL_MS",
            0,
        )?;
        if max_jobs < 1 || request_cap < 1 {
            bail!("MaxJobs and RequestCap must be positive");
        }
        if env_i64(
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_PROVIDER_MIN_PAGE_INTERVAL_MS",
            0,
        )? < 0
        {
            bail!("ProviderMinPageIntervalMs must be non-negative");
        }
        Ok(Self {
            plan_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_LEDGER_PLAN_PATH",
                    DEFAULT_PLAN_PATH,
                )?,
                "PlanPath",
            )?,
            event_log_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_LEDGER_EVENT_LOG_PATH",
                    DEFAULT_EVENT_LOG_PATH,
                )?,
                "EventLogPath",
            )?,
            evidence_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_LEDGER_EVIDENCE_PATH",
                    DEFAULT_EVIDENCE_PATH,
                )?,
                "EvidencePath",
            )?,
            quota_metrics_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_LEDGER_QUOTA_METRICS_PATH",
                    DEFAULT_QUOTA_METRICS_PATH,
                )?,
                "QuotaMetricsPath",
            )?,
            local_object_root: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_LEDGER_LOCAL_OBJECT_ROOT",
                    DEFAULT_LOCAL_OBJECT_ROOT,
                )?,
                "LocalObjectRoot",
            )?,
            root,
            env_file,
            reuse_manifest_path,
            bronze_storage_driver,
            cargo_exe,
            runner_exe,
            job_ids_path,
            provider_min_page_interval_ms,
            max_jobs,
            skip_jobs,
            request_cap,
            execute: env_bool("FOUNDATION_PLATFORM_NATIONAL_LEDGER_EXECUTE", false)?,
            reuse_validated_bronze_objects: env_bool(
                "FOUNDATION_PLATFORM_NATIONAL_LEDGER_REUSE_VALIDATED_BRONZE_OBJECTS",
                false,
            )?,
            confirm_public_api_quota_impact: env_bool(
                "FOUNDATION_PLATFORM_NATIONAL_LEDGER_CONFIRM_PUBLIC_API_QUOTA_IMPACT",
                false,
            )?,
            confirm_national_ledger_execution: env_bool(
                "FOUNDATION_PLATFORM_NATIONAL_LEDGER_CONFIRM_NATIONAL_LEDGER_EXECUTION",
                false,
            )?,
            confirm_local_bronze_storage: env_bool(
                "FOUNDATION_PLATFORM_NATIONAL_LEDGER_CONFIRM_LOCAL_BRONZE_STORAGE",
                false,
            )?,
        })
    }

    pub(in crate::national_data_collection_ledger_execute) fn validate_output_paths(
        &self,
    ) -> anyhow::Result<()> {
        if self.evidence_path.is_file() || self.event_log_path.is_file() {
            bail!("national ledger execution output already exists");
        }
        if let Some(path) = &self.job_ids_path {
            if !path.is_file() {
                bail!(
                    "JobIdsPath file missing: {}",
                    repo_relative_path(&self.root, path)
                );
            }
        }
        if self.reuse_validated_bronze_objects && self.reuse_manifest_path.is_none() {
            bail!("ReuseBronzeObjectManifestPath is required when -ReuseValidatedBronzeObjects is used");
        }
        if !self.reuse_validated_bronze_objects && self.reuse_manifest_path.is_some() {
            bail!("ReuseValidatedBronzeObjects is required when ReuseBronzeObjectManifestPath is provided");
        }
        Ok(())
    }
}

fn resolve_external_path(root: &Path, path: PathBuf) -> PathBuf {
    let candidate = if path.is_absolute() {
        path
    } else {
        root.join(path)
    };
    fs::canonicalize(&candidate).unwrap_or(candidate)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::national_data_collection_ledger_execute) enum StorageDriver {
    Local,
    R2,
}

impl StorageDriver {
    fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "local" => Ok(Self::Local),
            "r2" => Ok(Self::R2),
            other => bail!("BronzeStorageDriver is invalid: {other}"),
        }
    }

    pub(in crate::national_data_collection_ledger_execute) fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::R2 => "r2",
        }
    }
}
