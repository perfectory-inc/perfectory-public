use std::{fs, path::PathBuf};

use anyhow::{bail, Context};

use crate::public_data_control_support::{env_path, repo_relative_path, resolve_repo_path};

use super::support::*;
use super::types::ProviderLaneMode;

const DEFAULT_PLAN_PATH: &str = "target/audit/national-data-collection-plan.json";
const DEFAULT_EVIDENCE_GLOB: &str =
    "target/audit/national-data-collection-ledger-execution-*-evidence.json";
const DEFAULT_OUTPUT_DIR: &str = "target/audit";
const DEFAULT_REPORT_PATH: &str = "target/audit/national-data-collection-ledger-resume-report.json";
const DEFAULT_POLICY_PATH: &str = "docs/catalog/provider-rate-policy.v1.json";

pub(super) struct ResumeConfig {
    pub(super) root: PathBuf,
    pub(super) env_file: String,
    pub(super) plan_path: PathBuf,
    pub(super) evidence_glob: PathBuf,
    pub(super) output_dir: PathBuf,
    pub(super) report_path: PathBuf,
    pub(super) run_id: String,
    pub(super) bronze_storage_driver: String,
    pub(super) cargo_exe: String,
    pub(super) runner_exe: String,
    pub(super) provider_lane_mode: ProviderLaneMode,
    pub(super) provider_rate_policy_path: PathBuf,
    pub(super) provider_lane_seed_report_path: Option<PathBuf>,
    pub(super) chunk_size: usize,
    pub(super) max_chunks: usize,
    pub(super) max_parallel_chunks: usize,
    pub(super) request_cap: u64,
    pub(super) execute: bool,
    pub(super) confirm_local_bronze_storage: bool,
    pub(super) allow_compatible_prior_plan_evidence: bool,
}

impl ResumeConfig {
    pub(super) fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );
        let execute = env_bool("FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_EXECUTE", false)?;
        let bronze_storage_driver = env_string(
            "FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_BRONZE_STORAGE_DRIVER",
            "r2",
        )?;
        let provider_lane_mode = match env_string(
            "FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_PROVIDER_LANE_MODE",
            "provider_policy",
        )?
        .as_str()
        {
            "off" => ProviderLaneMode::Off,
            "provider_policy" => ProviderLaneMode::ProviderPolicy,
            other => bail!("ProviderLaneMode is invalid: {other}"),
        };
        let chunk_size = env_usize("FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_CHUNK_SIZE", 100)?;
        let max_chunks = env_usize("FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_MAX_CHUNKS", 1)?;
        let max_parallel_chunks = env_usize(
            "FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_MAX_PARALLEL_CHUNKS",
            1,
        )?;
        let request_cap = env_u64(
            "FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_REQUEST_CAP",
            100_000,
        )?;
        if chunk_size < 1 || max_chunks < 1 || max_parallel_chunks < 1 || request_cap < 1 {
            bail!("ChunkSize, MaxChunks, MaxParallelChunks, and RequestCap must be positive");
        }
        if execute
            && !env_bool(
                "FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_CONFIRM_PUBLIC_API_QUOTA_IMPACT",
                false,
            )?
        {
            bail!("Public API quota impact must be confirmed with -ConfirmPublicApiQuotaImpact when -Execute is used");
        }
        if execute
            && !env_bool(
                "FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_CONFIRM_NATIONAL_LEDGER_EXECUTION",
                false,
            )?
        {
            bail!("ConfirmNationalLedgerExecution is required when -Execute is used");
        }
        let confirm_local = env_bool(
            "FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_CONFIRM_LOCAL_BRONZE_STORAGE",
            false,
        )?;
        if execute && bronze_storage_driver == "local" && !confirm_local {
            bail!("Local Bronze storage is proof-only and requires -ConfirmLocalBronzeStorage when -Execute is used");
        }

        let seed_path = env_path(
            "FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_PROVIDER_LANE_SEED_REPORT_PATH",
            "",
        )?;
        let provider_lane_seed_report_path = if seed_path.as_os_str().is_empty() {
            None
        } else {
            let resolved = resolve_repo_path(&root, &seed_path, "ProviderLaneSeedReportPath")?;
            if provider_lane_mode == ProviderLaneMode::ProviderPolicy && !resolved.is_file() {
                bail!(
                    "ProviderLaneSeedReportPath file missing: {}",
                    repo_relative_path(&root, &resolved)
                );
            }
            Some(resolved)
        };

        Ok(Self {
            plan_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_PLAN_PATH",
                    DEFAULT_PLAN_PATH,
                )?,
                "PlanPath",
            )?,
            evidence_glob: env_path(
                "FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_EVIDENCE_GLOB",
                DEFAULT_EVIDENCE_GLOB,
            )?,
            output_dir: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_OUTPUT_DIR",
                    DEFAULT_OUTPUT_DIR,
                )?,
                "OutputDir",
            )?,
            report_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_REPORT_PATH",
                    DEFAULT_REPORT_PATH,
                )?,
                "ReportPath",
            )?,
            provider_rate_policy_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_PROVIDER_RATE_POLICY_PATH",
                    DEFAULT_POLICY_PATH,
                )?,
                "ProviderRatePolicyPath",
            )?,
            root,
            env_file: env_string("FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_ENV_FILE", "")?,
            run_id: env_string(
                "FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_RUN_ID",
                "resume",
            )?,
            bronze_storage_driver,
            cargo_exe: env_string("FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_CARGO_EXE", "")?,
            runner_exe: env_string("FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_RUNNER_EXE", "")?,
            provider_lane_mode,
            provider_lane_seed_report_path,
            chunk_size,
            max_chunks,
            max_parallel_chunks,
            request_cap,
            execute,
            confirm_local_bronze_storage: confirm_local,
            allow_compatible_prior_plan_evidence: env_bool(
                "FOUNDATION_PLATFORM_RESUME_NATIONAL_LEDGER_ALLOW_COMPATIBLE_PRIOR_PLAN_EVIDENCE",
                false,
            )?,
        })
    }
}
