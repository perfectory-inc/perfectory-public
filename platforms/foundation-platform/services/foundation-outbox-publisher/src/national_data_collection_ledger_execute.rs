use std::{collections::BTreeMap, path::PathBuf};

use anyhow::{bail, Context};
use serde_json::Value as JsonValue;

use crate::public_data_control_support::{
    read_json, repo_relative_path, resolve_repo_path, write_json_file,
};

mod support;

use support::*;

const SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_ledger_execution.v1";
const EVENT_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_ledger_event.v1";
const PLAN_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_plan.v1";
const ENDPOINT_CATALOG_SCHEMA_VERSION: &str =
    "foundation-platform.public_source_endpoint_catalog.v1";
const LEDGER_ENTRY_SCHEMA_VERSION: &str =
    "foundation-platform.national_data_collection_ledger_entry.v1";
const MODE: &str = "national_data_collection_ledger_execution";
pub fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    Executor::new(config)?.run()
}

struct Executor {
    config: Config,
    plan: JsonValue,
    ledger_path: PathBuf,
    endpoint_policy_by_slug: BTreeMap<String, EndpointPolicy>,
}

impl Executor {
    fn new(config: Config) -> anyhow::Result<Self> {
        config.validate_output_paths()?;
        if !config.plan_path.is_file() {
            bail!(
                "National collection plan missing: {}",
                repo_relative_path(&config.root, &config.plan_path)
            );
        }
        let plan = read_json(&config.plan_path, "national collection plan")?;
        validate_plan(&plan)?;
        let endpoint_policy_by_slug = load_endpoint_catalog(&config, &plan)?;
        let ledger_path = resolve_repo_path(
            &config.root,
            &PathBuf::from(string_at(&plan, &["execution_ledger", "path"])),
            "execution_ledger.path",
        )?;
        if !ledger_path.is_file() {
            bail!(
                "execution ledger missing: {}",
                repo_relative_path(&config.root, &ledger_path)
            );
        }
        Ok(Self {
            config,
            plan,
            ledger_path,
            endpoint_policy_by_slug,
        })
    }

    fn run(&self) -> anyhow::Result<()> {
        let selected = self.select_jobs()?;
        self.validate_endpoint_policy(&selected.jobs)?;
        let reuse = if self.config.reuse_validated_bronze_objects {
            let manifest_path = self
                .config
                .reuse_manifest_path
                .as_ref()
                .context("ReuseBronzeObjectManifestPath is required when reuse is enabled")?;
            ReuseIndex::read(manifest_path)?
        } else {
            ReuseIndex::default()
        };
        let provider_request_estimate = selected
            .jobs
            .iter()
            .filter(|job| !reuse.contains(job) && !is_provider_empty_job(job))
            .map(request_count)
            .sum::<u64>();
        if provider_request_estimate > self.config.request_cap {
            bail!("selected request count must not exceed RequestCap");
        }
        if !self.config.execute {
            return self.write_planned_evidence(&selected);
        }
        self.execute_jobs(selected, reuse)
    }

    fn select_jobs(&self) -> anyhow::Result<SelectedJobs> {
        if let Some(path) = &self.config.job_ids_path {
            let requested = read_requested_job_ids(path)?;
            let selected = read_planned_ledger_rows_for_job_ids(
                &self.ledger_path,
                &requested.ordered,
                &requested.set,
            )?;
            return Ok(selected);
        }
        let rows = read_ledger_jsonl(&self.ledger_path)?;
        let scanned = rows.len();
        let jobs = rows
            .into_iter()
            .filter(|row| string_prop(row, "status") == "planned")
            .skip(self.config.skip_jobs)
            .take(self.config.max_jobs)
            .collect::<Vec<_>>();
        if jobs.is_empty() {
            bail!("ledger has no planned jobs to execute");
        }
        Ok(SelectedJobs {
            jobs,
            read_mode: "full_materialize",
            scanned_row_count: scanned,
            loaded_row_count: scanned,
            skipped_job_count: self.config.skip_jobs,
        })
    }

    fn validate_endpoint_policy(&self, jobs: &[JsonValue]) -> anyhow::Result<()> {
        for job in jobs {
            let endpoint_slug = string_prop(job, "endpoint_slug");
            if endpoint_slug.is_empty() {
                continue;
            }
            if let Some(policy) = self.endpoint_policy_by_slug.get(&endpoint_slug) {
                if !policy.national_collection_allowed
                    || policy.source_acquisition_lane == "disabled_api_duplicate"
                {
                    bail!(
                        "selected job endpoint disabled for national collection: {} lane={}",
                        endpoint_slug,
                        policy.source_acquisition_lane
                    );
                }
            }
        }
        Ok(())
    }

    fn write_planned_evidence(&self, selected: &SelectedJobs) -> anyhow::Result<()> {
        let report = build_execution_evidence(
            &self.config,
            &self.plan,
            "planned",
            false,
            selected,
            &ExecutionStats::default(),
        );
        write_json_file(&self.config.evidence_path, &report)?;
        println!(
            "national-data-collection-ledger-execution-planned status=planned jobs={} report={}",
            selected.jobs.len(),
            self.config.evidence_path.display()
        );
        Ok(())
    }

    fn execute_jobs(&self, selected: SelectedJobs, reuse: ReuseIndex) -> anyhow::Result<()> {
        validate_execution_inputs(&self.config, &selected.jobs, &reuse)?;
        let dotenv = import_dotenv(&self.config.env_file)?;
        let jobs_needing_provider = selected
            .jobs
            .iter()
            .filter(|job| !reuse.contains(job) && !is_provider_empty_job(job))
            .collect::<Vec<_>>();
        let runner = if jobs_needing_provider.is_empty() {
            None
        } else {
            Some(Runner::resolve(&self.config)?)
        };
        let mut stats = ExecutionStats::default();
        for job in &selected.jobs {
            let fingerprint = string_prop(job, "request_fingerprint_sha256");
            if let Some(reuse_entry) = reuse.by_fingerprint.get(&fingerprint) {
                record_reused_job(&self.config.event_log_path, job, reuse_entry, &mut stats)?;
                continue;
            }
            if is_provider_empty_job(job) {
                record_provider_empty_job(&self.config.event_log_path, job, &mut stats)?;
                continue;
            }
            let runner = runner
                .as_ref()
                .context("runner is required for provider-backed jobs")?;
            run_provider_job(&self.config, job, runner, &dotenv, &mut stats)?;
        }
        let status = if stats.failed_job_count == 0 {
            "ready"
        } else {
            "blocked"
        };
        let report =
            build_execution_evidence(&self.config, &self.plan, status, true, &selected, &stats);
        write_json_file(&self.config.evidence_path, &report)?;
        if stats.failed_job_count > 0 {
            println!(
                "national-data-collection-ledger-execution-blocked status=blocked jobs={} succeeded={} failed={} report={}",
                selected.jobs.len(),
                stats.succeeded_job_count,
                stats.failed_job_count,
                self.config.evidence_path.display()
            );
            bail!("national data collection ledger execution blocked");
        }
        println!(
            "national-data-collection-ledger-execution-ok status=ready jobs={} succeeded={} failed={} report={}",
            selected.jobs.len(),
            stats.succeeded_job_count,
            stats.failed_job_count,
            self.config.evidence_path.display()
        );
        Ok(())
    }
}
