use std::{path::PathBuf, time::Duration};

use anyhow::bail;
use collection_infrastructure::DataGoKrRequestPolicy;

use super::{env, JobExecutionSummary, DEFAULT_BASE_URI};

use env::{
    env_path, optional_env_value, optional_u32_env, optional_u64_env, optional_u8_env,
    optional_usize_env, require_flag,
};

#[derive(Clone, Debug)]
pub(super) struct AsyncExecutorConfig {
    pub(super) plan_path: PathBuf,
    pub(super) event_log_path: PathBuf,
    pub(super) evidence_path: PathBuf,
    pub(super) evidence_scan_dir: PathBuf,
    pub(super) max_jobs: usize,
    pub(super) max_in_flight: usize,
    pub(super) circuit_breaker_failure_threshold: u32,
    pub(super) circuit_breaker_open_seconds: u64,
    pub(super) adaptive_in_flight: AdaptiveInFlightConfig,
    pub(super) page_queue_enabled: bool,
    pub(super) request_cap: u64,
    pub(super) base_uri: String,
}

impl AsyncExecutorConfig {
    pub(super) fn from_env() -> anyhow::Result<Self> {
        let max_in_flight =
            optional_usize_env("FOUNDATION_PLATFORM_NATIONAL_ASYNC_MAX_IN_FLIGHT")?.unwrap_or(64);
        Ok(Self {
            plan_path: env_path(
                "FOUNDATION_PLATFORM_NATIONAL_ASYNC_PLAN_PATH",
                "target/audit/national-data-collection-plan-datago-building-register-pagewindow50-20260603-001.json",
            ),
            event_log_path: env_path(
                "FOUNDATION_PLATFORM_NATIONAL_ASYNC_EVENT_LOG_PATH",
                "target/audit/national-data-collection-async-ledger-events.jsonl",
            ),
            evidence_path: env_path(
                "FOUNDATION_PLATFORM_NATIONAL_ASYNC_EVIDENCE_PATH",
                "target/audit/national-data-collection-async-ledger-execution-evidence.json",
            ),
            evidence_scan_dir: env_path(
                "FOUNDATION_PLATFORM_NATIONAL_ASYNC_EVIDENCE_SCAN_DIR",
                "target/audit",
            ),
            max_jobs: optional_usize_env("FOUNDATION_PLATFORM_NATIONAL_ASYNC_MAX_JOBS")?.unwrap_or(100),
            max_in_flight,
            circuit_breaker_failure_threshold: optional_u32_env(
                "FOUNDATION_PLATFORM_NATIONAL_ASYNC_CIRCUIT_BREAKER_FAILURE_THRESHOLD",
            )?
            .unwrap_or(1),
            circuit_breaker_open_seconds: optional_u64_env(
                "FOUNDATION_PLATFORM_NATIONAL_ASYNC_CIRCUIT_BREAKER_OPEN_SECONDS",
            )?
            .unwrap_or(30),
            adaptive_in_flight: AdaptiveInFlightConfig::from_env(max_in_flight)?,
            page_queue_enabled: optional_env_value("FOUNDATION_PLATFORM_NATIONAL_ASYNC_PAGE_QUEUE")?
                .as_deref()
                == Some("1"),
            request_cap: optional_u64_env("FOUNDATION_PLATFORM_NATIONAL_ASYNC_REQUEST_CAP")?
                .unwrap_or(10_000),
            base_uri: optional_env_value("FOUNDATION_PLATFORM_BUILDING_REGISTER_BASE_URI")?
                .unwrap_or_else(|| DEFAULT_BASE_URI.to_owned()),
        })
    }

    pub(super) fn validate_execution_confirmation(&self) -> anyhow::Result<()> {
        require_flag("FOUNDATION_PLATFORM_NATIONAL_ASYNC_EXECUTE")?;
        require_flag("FOUNDATION_PLATFORM_NATIONAL_ASYNC_CONFIRM_PUBLIC_API_QUOTA_IMPACT")?;
        if self.max_jobs == 0 || self.max_in_flight == 0 || self.request_cap == 0 {
            bail!("national async max_jobs, max_in_flight, and request_cap must be positive");
        }
        if self.circuit_breaker_failure_threshold == 0 || self.circuit_breaker_open_seconds == 0 {
            bail!("national async circuit breaker settings must be positive");
        }
        self.adaptive_in_flight.validate()?;
        if self.page_queue_enabled && self.adaptive_in_flight.enabled {
            bail!(
                "national async page queue and adaptive job-window mode cannot be enabled together"
            );
        }
        Ok(())
    }

    pub(super) fn data_go_kr_request_policy(&self) -> anyhow::Result<DataGoKrRequestPolicy> {
        DataGoKrRequestPolicy::default()
            .with_circuit_breaker(
                self.circuit_breaker_failure_threshold,
                Duration::from_secs(self.circuit_breaker_open_seconds),
            )
            .map_err(anyhow::Error::from)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AdaptiveInFlightConfig {
    pub(super) enabled: bool,
    pub(super) start_in_flight: usize,
    pub(super) min_in_flight: usize,
    pub(super) max_in_flight: usize,
    pub(super) increase_step: usize,
    pub(super) decrease_percent: u8,
}

impl AdaptiveInFlightConfig {
    fn from_env(max_in_flight: usize) -> anyhow::Result<Self> {
        let enabled = optional_env_value("FOUNDATION_PLATFORM_NATIONAL_ASYNC_ADAPTIVE_IN_FLIGHT")?
            .as_deref()
            == Some("1");
        Ok(Self {
            enabled,
            start_in_flight: optional_usize_env(
                "FOUNDATION_PLATFORM_NATIONAL_ASYNC_ADAPTIVE_START_IN_FLIGHT",
            )?
            .unwrap_or(max_in_flight.min(64)),
            min_in_flight: optional_usize_env(
                "FOUNDATION_PLATFORM_NATIONAL_ASYNC_ADAPTIVE_MIN_IN_FLIGHT",
            )?
            .unwrap_or(1),
            max_in_flight,
            increase_step: optional_usize_env(
                "FOUNDATION_PLATFORM_NATIONAL_ASYNC_ADAPTIVE_INCREASE_STEP",
            )?
            .unwrap_or(16),
            decrease_percent: optional_u8_env(
                "FOUNDATION_PLATFORM_NATIONAL_ASYNC_ADAPTIVE_DECREASE_PERCENT",
            )?
            .unwrap_or(50),
        })
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.min_in_flight == 0
            || self.start_in_flight == 0
            || self.max_in_flight == 0
            || self.increase_step == 0
            || self.decrease_percent == 0
            || self.decrease_percent >= 100
        {
            bail!("national async adaptive in-flight settings must be positive and decrease_percent must be 1..99");
        }
        if self.min_in_flight > self.start_in_flight || self.start_in_flight > self.max_in_flight {
            bail!("national async adaptive in-flight must satisfy min <= start <= max");
        }
        Ok(())
    }

    pub(super) fn next_in_flight(&self, current: usize, summary: &JobExecutionSummary) -> usize {
        if !self.enabled {
            return self.max_in_flight;
        }
        if summary.failed_job_count > 0 {
            let decreased = current.saturating_mul(usize::from(self.decrease_percent)) / 100;
            return decreased.max(self.min_in_flight);
        }
        current
            .saturating_add(self.increase_step)
            .min(self.max_in_flight)
    }
}
