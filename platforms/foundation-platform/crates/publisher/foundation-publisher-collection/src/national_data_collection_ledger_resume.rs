use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::Duration,
};

use anyhow::{bail, Context};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::Value as JsonValue;

mod config;
mod provider_lanes;
mod support;
mod types;

use crate::public_data_control_support::{
    git_head, read_json, repo_relative_path, resolve_repo_path, utc_now, write_json_file,
};
use crate::public_provider_rate_policy::{
    update_lane_state, LanePolicy, LaneState, ProviderOutcome, ProviderRatePolicyDocument,
};
use config::ResumeConfig;
use provider_lanes::*;
use support::*;
use types::*;

const SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_ledger_resume.v1";
const PLAN_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_plan.v1";
const LEDGER_ENTRY_SCHEMA_VERSION: &str =
    "foundation-platform.national_data_collection_ledger_entry.v1";
const EXECUTION_EVIDENCE_SCHEMA_VERSION: &str =
    "foundation-platform.national_data_collection_ledger_execution.v1";
const EVENT_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_ledger_event.v1";

pub fn run() -> anyhow::Result<()> {
    let config = ResumeConfig::from_env()?;
    let mut engine = ResumeEngine::new(config)?;
    let report = engine.run()?;
    let report_path = engine.config.report_path.clone();
    write_json_file(&report_path, &report)?;

    if !engine.config.execute {
        println!(
            "national-data-collection-ledger-resume-planned status=planned pending={} chunks={} report={}",
            report.coverage.pending_job_count,
            report.chunks.len(),
            report_path.display()
        );
        return Ok(());
    }

    if report.chunking.failed_chunks > 0 {
        println!(
            "national-data-collection-ledger-resume-blocked status=blocked pending={} chunks={} succeeded_chunks={} failed_chunks={} report={}",
            report.coverage.pending_job_count,
            report.chunks.len(),
            report.chunking.succeeded_chunks,
            report.chunking.failed_chunks,
            report_path.display()
        );
        bail!("national data collection ledger resume blocked");
    }

    println!(
        "national-data-collection-ledger-resume-ok status=executed pending={} chunks={} succeeded_chunks={} failed_chunks={} report={}",
        report.coverage.pending_job_count,
        report.chunks.len(),
        report.chunking.succeeded_chunks,
        report.chunking.failed_chunks,
        report_path.display()
    );
    Ok(())
}

struct ResumeEngine {
    config: ResumeConfig,
    policy: Option<ProviderRatePolicyDocument>,
    lane_policies: BTreeMap<String, LanePolicy>,
    lane_states: BTreeMap<String, LaneState>,
    lane_seed: Option<ProviderLaneSeedReport>,
    lane_decisions: Vec<ProviderLaneDecision>,
}

impl ResumeEngine {
    fn new(config: ResumeConfig) -> anyhow::Result<Self> {
        let (policy, lane_policies, lane_states, lane_seed) = load_provider_policy_state(&config)?;
        Ok(Self {
            config,
            policy,
            lane_policies,
            lane_states,
            lane_seed,
            lane_decisions: Vec::new(),
        })
    }

    fn run(&mut self) -> anyhow::Result<ResumeReport> {
        if !self.config.plan_path.is_file() {
            bail!(
                "PlanPath file missing: {}",
                repo_relative_path(&self.config.root, &self.config.plan_path)
            );
        }
        let plan = read_json(&self.config.plan_path, "national data collection plan")?;
        if string_property(&plan, "schema_version") != PLAN_SCHEMA_VERSION
            || string_property(&plan, "status") != "ready"
        {
            bail!("national collection plan must be ready");
        }
        let plan_hash = string_property(&plan, "compiler_input_hash_sha256");
        let ledger_path = string_property(
            plan.get("execution_ledger").unwrap_or(&JsonValue::Null),
            "path",
        );
        let resolved_ledger_path = resolve_repo_path(
            &self.config.root,
            &PathBuf::from(&ledger_path),
            "execution_ledger.path",
        )?;
        if !resolved_ledger_path.is_file() {
            bail!(
                "execution ledger file missing: {}",
                repo_relative_path(&self.config.root, &resolved_ledger_path)
            );
        }

        let planned_rows = read_planned_ledger_rows(&resolved_ledger_path)?;
        let planned_ids = planned_rows.keys().cloned().collect::<Vec<_>>();
        let evidence_files = resolve_evidence_files(&self.config)?;
        let resume_state =
            collect_succeeded_jobs(&self.config, &plan_hash, &planned_rows, &evidence_files)?;
        let pending_ids = planned_ids
            .iter()
            .filter(|job_id| !resume_state.succeeded_ids.contains(*job_id))
            .cloned()
            .collect::<Vec<_>>();

        fs::create_dir_all(&self.config.output_dir).with_context(|| {
            format!(
                "failed to create output dir {}",
                self.config.output_dir.display()
            )
        })?;
        let mut chunks = self.plan_chunks(&pending_ids, &planned_rows)?;
        if self.config.execute {
            self.execute_chunks(&mut chunks)?;
        }
        let succeeded_chunks = chunks
            .iter()
            .filter(|chunk| chunk.exit_code == Some(0))
            .count();
        let failed_chunks = chunks
            .iter()
            .filter(|chunk| chunk.exit_code.is_some_and(|code| code != 0))
            .count();
        let executed_chunks = chunks
            .iter()
            .filter(|chunk| chunk.exit_code.is_some())
            .count();
        let status = if self.config.execute {
            if failed_chunks == 0 {
                "executed"
            } else {
                "blocked"
            }
        } else {
            "planned"
        };

        Ok(ResumeReport {
            schema_version: SCHEMA_VERSION,
            generated_at_utc: utc_now(),
            git_head: git_head(&self.config.root),
            status,
            executed: self.config.execute,
            plan: ResumePlanReport {
                path: repo_relative_path(&self.config.root, &self.config.plan_path),
                compiler_input_hash_sha256: plan_hash,
                ledger_path: repo_relative_path(&self.config.root, &resolved_ledger_path),
            },
            evidence: ResumeEvidenceReport {
                glob: self
                    .config
                    .evidence_glob
                    .to_string_lossy()
                    .replace('\\', "/"),
                file_count: evidence_files.len(),
            },
            coverage: ResumeCoverageReport {
                planned_job_count: planned_ids.len(),
                succeeded_job_count: resume_state.succeeded_ids.len(),
                compatible_prior_plan_succeeded_job_count: resume_state
                    .compatible_prior_plan_succeeded_ids
                    .len(),
                pending_job_count: pending_ids.len(),
            },
            execution_strategy: self.execution_strategy_report(),
            chunking: ResumeChunkingReport {
                chunk_size: self.config.chunk_size,
                max_chunks: self.config.max_chunks,
                max_parallel_chunks: self.config.max_parallel_chunks,
                planned_chunks: chunks.len(),
                executed_chunks,
                succeeded_chunks,
                failed_chunks,
            },
            chunks,
            provider_lanes: if self.config.provider_lane_mode == ProviderLaneMode::ProviderPolicy {
                self.lane_states.values().cloned().collect()
            } else {
                Vec::new()
            },
            provider_lane_decisions: self.lane_decisions.clone(),
            completion_claim_allowed: false,
            production_cutover_allowed: false,
            national_rollout_allowed: false,
            evidence_limitations: vec![
                "resume_orchestration_only".to_owned(),
                "does_not_promote_silver_gold_national_tables".to_owned(),
                "does_not_approve_production_cutover".to_owned(),
                "does_not_mark_national_rollout_complete".to_owned(),
            ],
            next_gates: vec!["national-data-collection-coverage-ledger".to_owned()],
        })
    }

    fn execution_strategy_report(&self) -> ExecutionStrategyReport {
        let mode = match self.config.provider_lane_mode {
            ProviderLaneMode::ProviderPolicy => "provider_lane_bounded_parallel_chunks",
            ProviderLaneMode::Off if self.config.max_parallel_chunks == 1 => "sequential_chunks",
            ProviderLaneMode::Off => "bounded_parallel_chunks",
        };
        ExecutionStrategyReport {
            mode,
            max_parallel_chunks: self.config.max_parallel_chunks,
            executor_isolation: if self.config.max_parallel_chunks == 1 {
                "inline_executor_process"
            } else {
                "child_executor_process_per_chunk"
            },
            provider_lane_mode: match self.config.provider_lane_mode {
                ProviderLaneMode::ProviderPolicy => "provider_policy",
                ProviderLaneMode::Off => "off",
            },
            provider_rate_policy: self.policy.as_ref().map(|policy| ProviderRatePolicyReport {
                path: repo_relative_path(&self.config.root, &self.config.provider_rate_policy_path),
                schema_version: policy.schema_version.clone(),
                status: policy.status.clone(),
                throughput_profile: policy.throughput_profile.clone(),
            }),
            provider_lane_seed: if self.config.provider_lane_mode
                == ProviderLaneMode::ProviderPolicy
            {
                self.lane_seed.clone()
            } else {
                None
            },
        }
    }

    fn plan_chunks(
        &self,
        pending_ids: &[String],
        planned_rows: &BTreeMap<String, JsonValue>,
    ) -> anyhow::Result<Vec<ResumeChunkReport>> {
        let mut chunks = Vec::new();
        let mut next_chunk_number = next_run_chunk_number(&self.config)?;
        if self.config.provider_lane_mode == ProviderLaneMode::Off {
            let chunk_count = self
                .config
                .max_chunks
                .min(pending_ids.len().div_ceil(self.config.chunk_size));
            for index in 0..chunk_count {
                let start = index * self.config.chunk_size;
                let ids = pending_ids
                    .iter()
                    .skip(start)
                    .take(self.config.chunk_size)
                    .cloned()
                    .collect::<Vec<_>>();
                chunks.push(self.create_chunk(next_chunk_number, ids, None)?);
                next_chunk_number += 1;
            }
            return Ok(chunks);
        }

        let policy = self
            .policy
            .as_ref()
            .context("provider rate policy is required")?;
        let mut lane_queues: BTreeMap<String, VecDeque<String>> = BTreeMap::new();
        let mut lane_order = Vec::new();
        for job_id in pending_ids {
            let job = planned_rows
                .get(job_id)
                .with_context(|| format!("planned job missing: {job_id}"))?;
            let lane = provider_lane_for_job(job, policy)?;
            if !lane_queues.contains_key(&lane.lane_id) {
                lane_order.push(lane.lane_id.clone());
                lane_queues.insert(lane.lane_id.clone(), VecDeque::new());
            }
            lane_queues
                .entry(lane.lane_id.clone())
                .or_default()
                .push_back(job_id.clone());
        }

        let mut lane_cursor = 0usize;
        while chunks.len() < self.config.max_chunks && !lane_order.is_empty() {
            let mut selected = None;
            for offset in 0..lane_order.len() {
                let index = (lane_cursor + offset) % lane_order.len();
                let lane_id = &lane_order[index];
                if lane_queues
                    .get(lane_id)
                    .is_some_and(|queue| !queue.is_empty())
                {
                    selected = Some((index, lane_id.clone()));
                    break;
                }
            }
            let Some((selected_index, lane_id)) = selected else {
                break;
            };
            let queue = lane_queues
                .get_mut(&lane_id)
                .with_context(|| format!("lane queue missing: {lane_id}"))?;
            let mut ids = Vec::new();
            while ids.len() < self.config.chunk_size {
                let Some(job_id) = queue.pop_front() else {
                    break;
                };
                ids.push(job_id);
            }
            let lane = self
                .lane_policies
                .get(&lane_id)
                .with_context(|| format!("lane policy missing: {lane_id}"))?;
            chunks.push(self.create_chunk(next_chunk_number, ids, Some(lane))?);
            next_chunk_number += 1;
            lane_cursor = (selected_index + 1) % lane_order.len();
        }
        Ok(chunks)
    }

    fn create_chunk(
        &self,
        chunk_number: usize,
        job_ids: Vec<String>,
        lane: Option<&LanePolicy>,
    ) -> anyhow::Result<ResumeChunkReport> {
        let chunk_id = format!("{chunk_number:04}");
        let job_ids_path = self.config.output_dir.join(format!(
            "national-data-collection-{}-chunk-{chunk_id}-job-ids.txt",
            self.config.run_id
        ));
        fs::write(&job_ids_path, job_ids.join("\n"))
            .with_context(|| format!("failed to write {}", job_ids_path.display()))?;
        let event_log_path = self.config.output_dir.join(format!(
            "national-data-collection-ledger-events-{}-{chunk_id}.jsonl",
            self.config.run_id
        ));
        let evidence_path = self.config.output_dir.join(format!(
            "national-data-collection-ledger-execution-{}-{chunk_id}-evidence.json",
            self.config.run_id
        ));

        let mut chunk = ResumeChunkReport {
            chunk_id,
            job_count: job_ids.len(),
            job_ids_path: repo_relative_path(&self.config.root, &job_ids_path),
            event_log_path: repo_relative_path(&self.config.root, &event_log_path),
            evidence_path: repo_relative_path(&self.config.root, &evidence_path),
            status: "planned".to_owned(),
            exit_code: None,
            lane_id: None,
            provider: None,
            endpoint_groups: Vec::new(),
            lane_max_parallel_chunks: None,
            lane_start_rps: None,
            lane_max_rps: None,
            lane_min_page_interval_ms: None,
            lane_start_page_interval_ms: None,
            lane_current_rps_at_start: None,
            lane_current_in_flight_at_start: None,
            lane_effective_parallel_chunks_at_start: None,
            lane_effective_rps_per_chunk_at_start: None,
            provider_min_page_interval_ms: None,
            started_at_utc: None,
            finished_at_utc: None,
            duration_ms: None,
            output_tail: None,
        };
        if let Some(lane) = lane {
            let rate = lane
                .rate_window
                .as_ref()
                .with_context(|| format!("lane rate_window missing: {}", lane.lane_id))?;
            chunk.lane_id = Some(lane.lane_id.clone());
            chunk.provider = Some(lane.provider.clone());
            chunk.endpoint_groups = lane.endpoint_groups.clone();
            chunk.lane_max_parallel_chunks = Some(rate.start_in_flight as usize);
            chunk.lane_start_rps = Some(rate.start_rps);
            chunk.lane_max_rps = Some(rate.max_rps);
            chunk.lane_min_page_interval_ms = Some(min_page_interval_ms(rate.max_rps)?);
            chunk.lane_start_page_interval_ms = Some(min_page_interval_ms(rate.start_rps)?);
        }
        Ok(chunk)
    }

    fn execute_chunks(&mut self, chunks: &mut [ResumeChunkReport]) -> anyhow::Result<()> {
        let planned_by_lane = planned_chunk_count_by_lane(chunks);
        if self.config.max_parallel_chunks == 1 {
            for chunk in chunks.iter_mut() {
                self.set_chunk_lane_start_state(&planned_by_lane, chunk)?;
                let result = self.invoke_executor_chunk(chunk)?;
                apply_chunk_result(chunk, result);
                self.update_chunk_lane_state(chunk)?;
            }
            return Ok(());
        }

        let mut pending = (0..chunks.len()).collect::<VecDeque<_>>();
        let mut running: Vec<RunningChunk> = Vec::new();
        while !pending.is_empty() || !running.is_empty() {
            while running.len() < self.config.max_parallel_chunks {
                let Some(position) = pending
                    .iter()
                    .position(|index| self.chunk_can_start(&chunks[*index], chunks, &running))
                else {
                    break;
                };
                let index = pending
                    .remove(position)
                    .context("pending chunk position vanished")?;
                self.set_chunk_lane_start_state(&planned_by_lane, &mut chunks[index])?;
                let child = self.spawn_executor_chunk(&chunks[index])?;
                running.push(RunningChunk {
                    chunk_index: index,
                    child,
                    started_at: Utc::now(),
                });
            }
            if running.is_empty() {
                if !pending.is_empty() {
                    bail!("provider-lane scheduler could not start any pending chunk");
                }
                break;
            }

            let mut completed_index = None;
            loop {
                for (index, running_chunk) in running.iter_mut().enumerate() {
                    if running_chunk
                        .child
                        .try_wait()
                        .context("failed to poll executor process")?
                        .is_some()
                    {
                        completed_index = Some(index);
                        break;
                    }
                }
                if completed_index.is_some() {
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
            let completed_index = completed_index.context("completed chunk index missing")?;
            let running_chunk = running.remove(completed_index);
            let result = wait_child_result(running_chunk)?;
            apply_chunk_result(&mut chunks[result.chunk_index], result.result);
            self.update_chunk_lane_state(&chunks[result.chunk_index])?;
        }
        Ok(())
    }

    fn chunk_can_start(
        &self,
        chunk: &ResumeChunkReport,
        chunks: &[ResumeChunkReport],
        running: &[RunningChunk],
    ) -> bool {
        if running.len() >= self.config.max_parallel_chunks {
            return false;
        }
        if self.config.provider_lane_mode == ProviderLaneMode::Off {
            return true;
        }
        let Some(lane_id) = chunk.lane_id.as_deref() else {
            return true;
        };
        let lane_limit = self
            .lane_states
            .get(lane_id)
            .map(|state| state.current_in_flight as usize)
            .unwrap_or(1);
        let running_lane_count = running
            .iter()
            .filter(|item| chunks[item.chunk_index].lane_id.as_deref() == Some(lane_id))
            .count();
        running_lane_count < lane_limit
    }

    fn set_chunk_lane_start_state(
        &self,
        planned_by_lane: &BTreeMap<String, usize>,
        chunk: &mut ResumeChunkReport,
    ) -> anyhow::Result<()> {
        if self.config.provider_lane_mode == ProviderLaneMode::Off {
            return Ok(());
        }
        let Some(lane_id) = chunk.lane_id.clone() else {
            return Ok(());
        };
        let state = self
            .lane_states
            .get(&lane_id)
            .with_context(|| format!("provider lane state missing for lane: {lane_id}"))?;
        let planned_lane_chunks = *planned_by_lane.get(&lane_id).unwrap_or(&1);
        let effective_parallel = 1usize.max(
            (state.current_in_flight as usize)
                .min(self.config.max_parallel_chunks.min(planned_lane_chunks)),
        );
        let effective_rps = state.current_rps / effective_parallel as f64;
        chunk.lane_current_rps_at_start = Some(state.current_rps);
        chunk.lane_current_in_flight_at_start = Some(state.current_in_flight as usize);
        chunk.lane_effective_parallel_chunks_at_start = Some(effective_parallel);
        chunk.lane_effective_rps_per_chunk_at_start = Some(effective_rps);
        chunk.provider_min_page_interval_ms = Some(min_page_interval_ms(effective_rps)?);
        Ok(())
    }

    fn invoke_executor_chunk(&self, chunk: &ResumeChunkReport) -> anyhow::Result<ChunkExecResult> {
        let started = Utc::now();
        let output = executor_command(&self.config, chunk)?
            .output()
            .context("failed to execute ledger executor")?;
        let finished = Utc::now();
        Ok(ChunkExecResult {
            exit_code: output.status.code().unwrap_or(1),
            output: process_output_lines(output.stdout, output.stderr),
            started_at_utc: started.to_rfc3339_opts(SecondsFormat::Nanos, true),
            finished_at_utc: finished.to_rfc3339_opts(SecondsFormat::Nanos, true),
            duration_ms: (finished - started).num_milliseconds().max(0) as u64,
        })
    }

    fn spawn_executor_chunk(
        &self,
        chunk: &ResumeChunkReport,
    ) -> anyhow::Result<std::process::Child> {
        let mut command = executor_command(&self.config, chunk)?;
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        command.spawn().context("failed to spawn ledger executor")
    }

    fn update_chunk_lane_state(&mut self, chunk: &ResumeChunkReport) -> anyhow::Result<()> {
        if self.config.provider_lane_mode == ProviderLaneMode::Off {
            return Ok(());
        }
        let Some(lane_id) = chunk.lane_id.as_deref() else {
            return Ok(());
        };
        let lane_policy = self
            .lane_policies
            .get(lane_id)
            .with_context(|| format!("provider lane policy missing for lane: {lane_id}"))?;
        let before = self
            .lane_states
            .get(lane_id)
            .with_context(|| format!("provider lane state missing for lane: {lane_id}"))?
            .clone();
        let outcome = provider_lane_outcome(lane_policy, chunk);
        let latency_ms = chunk_provider_latency_ms(&self.config, chunk)?;
        let provider_request_count = if outcome == ProviderOutcome::Success {
            chunk_provider_request_count(&self.config, chunk)?
        } else {
            1
        };
        let observed = chunk
            .finished_at_utc
            .as_deref()
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);
        let mut after = before.clone();
        for _ in 0..provider_request_count {
            after = update_lane_state(lane_policy, &after, outcome, latency_ms, observed)?;
        }
        self.lane_states.insert(lane_id.to_owned(), after.clone());
        self.lane_decisions.push(ProviderLaneDecision {
            chunk_id: chunk.chunk_id.clone(),
            lane_id: lane_id.to_owned(),
            provider: chunk.provider.clone().unwrap_or_default(),
            outcome: match outcome {
                ProviderOutcome::Success => "success",
                ProviderOutcome::Throttle => "throttle",
                ProviderOutcome::Timeout => "timeout",
                ProviderOutcome::Error => "error",
            },
            latency_ms,
            provider_request_count,
            before_current_rps: before.current_rps,
            after_current_rps: after.current_rps,
            before_current_in_flight: before.current_in_flight,
            after_current_in_flight: after.current_in_flight,
            decision: after.decision,
            job_disposition: after.job_disposition,
            observed_at_utc: observed.to_rfc3339_opts(SecondsFormat::Nanos, true),
        });
        Ok(())
    }
}

fn read_planned_ledger_rows(path: &Path) -> anyhow::Result<BTreeMap<String, JsonValue>> {
    let mut rows = BTreeMap::new();
    for row in read_jsonl(path)? {
        if string_property(&row, "schema_version") != LEDGER_ENTRY_SCHEMA_VERSION
            || string_property(&row, "status") != "planned"
        {
            continue;
        }
        let job_id = string_property(&row, "job_id");
        if job_id.trim().is_empty() {
            bail!("planned ledger row job_id is required");
        }
        if rows.insert(job_id.clone(), row).is_some() {
            bail!("planned ledger contains duplicate job_id: {job_id}");
        }
    }
    Ok(rows)
}

fn collect_succeeded_jobs(
    config: &ResumeConfig,
    plan_hash: &str,
    planned_rows: &BTreeMap<String, JsonValue>,
    evidence_files: &[PathBuf],
) -> anyhow::Result<ResumeState> {
    let mut succeeded_ids = BTreeSet::new();
    let mut compatible_prior_plan_succeeded_ids = BTreeSet::new();
    for evidence_path in evidence_files {
        let evidence = read_json(evidence_path, "ledger execution evidence")?;
        if string_property(&evidence, "schema_version") != EXECUTION_EVIDENCE_SCHEMA_VERSION {
            continue;
        }
        let evidence_hash = string_property(
            evidence.get("plan").unwrap_or(&JsonValue::Null),
            "compiler_input_hash_sha256",
        );
        let compatible_prior = if evidence_hash != plan_hash {
            if !config.allow_compatible_prior_plan_evidence {
                continue;
            }
            true
        } else {
            false
        };
        let event_path = string_property(
            evidence.get("event_log").unwrap_or(&JsonValue::Null),
            "path",
        );
        if event_path.trim().is_empty() {
            continue;
        }
        let resolved_event_path =
            resolve_repo_path(&config.root, &PathBuf::from(&event_path), "event_log.path")?;
        if !resolved_event_path.is_file() {
            continue;
        }
        for event in read_jsonl(&resolved_event_path)? {
            if string_property(&event, "schema_version") != EVENT_SCHEMA_VERSION
                || string_property(&event, "status") != "succeeded"
            {
                continue;
            }
            let job_id = string_property(&event, "job_id");
            if job_id.trim().is_empty() {
                continue;
            }
            if compatible_prior {
                if planned_rows
                    .get(&job_id)
                    .is_some_and(|row| compatible_prior_plan_event(&event, row))
                {
                    succeeded_ids.insert(job_id.clone());
                    compatible_prior_plan_succeeded_ids.insert(job_id);
                }
            } else {
                succeeded_ids.insert(job_id);
            }
        }
    }
    Ok(ResumeState {
        succeeded_ids,
        compatible_prior_plan_succeeded_ids,
    })
}

fn compatible_prior_plan_event(event: &JsonValue, row: &JsonValue) -> bool {
    for field in [
        "job_id",
        "idempotency_key",
        "scope_unit_id",
        "provider",
        "endpoint",
    ] {
        let event_value = string_property(event, field);
        if event_value.trim().is_empty() || event_value != string_property(row, field) {
            return false;
        }
    }
    if i64_property(event, "request_count", 0) < i64_property(row, "request_count_estimate", 0) {
        return false;
    }
    let bronze_key = string_property(event, "bronze_object_key");
    for token in [
        format!("source={}", string_property(row, "source_slug")),
        optional_token("operation", &string_property(row, "operation")),
        optional_token("dataset", &string_property(row, "dataset")),
        optional_token("pnu", &string_property(row, "pnu_prefix")),
    ] {
        if !token.is_empty() && !bronze_key.contains(&token) {
            return false;
        }
    }
    true
}

fn optional_token(name: &str, value: &str) -> String {
    if value.trim().is_empty() {
        String::new()
    } else {
        format!("{name}={value}")
    }
}

fn resolve_evidence_files(config: &ResumeConfig) -> anyhow::Result<Vec<PathBuf>> {
    let glob_text = config.evidence_glob.to_string_lossy().replace('\\', "/");
    let (parent_text, file_pattern) = glob_text
        .rsplit_once('/')
        .map_or((".", glob_text.as_str()), |(parent, file)| (parent, file));
    let parent = resolve_repo_path(&config.root, &PathBuf::from(parent_text), "EvidenceGlob")?;
    if !parent.is_dir() {
        return Ok(Vec::new());
    }
    let mut paths = Vec::new();
    for entry in
        fs::read_dir(&parent).with_context(|| format!("failed to read {}", parent.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.contains("coverage-ledger") || name.contains("resume-report") {
            continue;
        }
        if wildcard_matches(file_pattern, &name) {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn next_run_chunk_number(config: &ResumeConfig) -> anyhow::Result<usize> {
    let mut numbers = BTreeSet::new();
    if config.output_dir.is_dir() {
        for entry in fs::read_dir(&config.output_dir)? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            add_existing_chunk_number(&mut numbers, &name, &config.run_id);
        }
    }
    let quota_dir = config.root.join("target/public-api-quota");
    if quota_dir.is_dir() {
        for entry in fs::read_dir(quota_dir)? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            if let Some(number) = extract_number(
                &name,
                &format!("national-data-collection-ledger-{}-", config.run_id),
                ".prom",
            ) {
                numbers.insert(number);
            }
        }
    }
    Ok(numbers.last().copied().unwrap_or(0) + 1)
}

fn add_existing_chunk_number(numbers: &mut BTreeSet<usize>, name: &str, run_id: &str) {
    for (prefix, suffix) in [
        (
            format!("national-data-collection-{run_id}-chunk-"),
            "-job-ids.txt".to_owned(),
        ),
        (
            format!("national-data-collection-ledger-events-{run_id}-"),
            ".jsonl".to_owned(),
        ),
        (
            format!("national-data-collection-ledger-execution-{run_id}-"),
            "-evidence.json".to_owned(),
        ),
    ] {
        if let Some(number) = extract_number(name, &prefix, &suffix) {
            numbers.insert(number);
        }
    }
}

fn extract_number(name: &str, prefix: &str, suffix: &str) -> Option<usize> {
    name.strip_prefix(prefix)?
        .strip_suffix(suffix)?
        .parse::<usize>()
        .ok()
}

fn executor_command(config: &ResumeConfig, chunk: &ResumeChunkReport) -> anyhow::Result<Command> {
    let mut command = ledger_executor_command()?;
    command
        .current_dir(&config.root)
        .arg("execute-national-data-collection-ledger");
    for (name, value) in executor_env(config, chunk) {
        command.env(name, value);
    }
    Ok(command)
}

/// Resolves the command that runs the `execute-national-data-collection-ledger` subcommand.
///
/// Prefers re-invoking the current binary so a resumed chunk uses exactly the build that is
/// running; falls back to `cargo run -p foundation-outbox-publisher --` when the current
/// executable cannot be resolved (for example under some test harnesses).
fn ledger_executor_command() -> anyhow::Result<Command> {
    match std::env::current_exe() {
        Ok(exe) => Ok(Command::new(exe)),
        Err(_) => {
            let mut command = Command::new("cargo");
            command.args(["run", "-p", "foundation-outbox-publisher", "--"]);
            Ok(command)
        }
    }
}

fn executor_env(config: &ResumeConfig, chunk: &ResumeChunkReport) -> Vec<(String, String)> {
    let quota_path = config.root.join(format!(
        "target/public-api-quota/national-data-collection-ledger-{}-{}.prom",
        config.run_id, chunk.chunk_id
    ));
    let mut env = vec![
        (
            "FOUNDATION_PLATFORM_REPO_ROOT".to_owned(),
            config.root.to_string_lossy().into_owned(),
        ),
        (
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_PLAN_PATH".to_owned(),
            repo_relative_path(&config.root, &config.plan_path),
        ),
        (
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_JOB_IDS_PATH".to_owned(),
            chunk.job_ids_path.clone(),
        ),
        (
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_EVENT_LOG_PATH".to_owned(),
            chunk.event_log_path.clone(),
        ),
        (
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_EVIDENCE_PATH".to_owned(),
            chunk.evidence_path.clone(),
        ),
        (
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_QUOTA_METRICS_PATH".to_owned(),
            repo_relative_path(&config.root, &quota_path),
        ),
        (
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_BRONZE_STORAGE_DRIVER".to_owned(),
            config.bronze_storage_driver.clone(),
        ),
        (
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_MAX_JOBS".to_owned(),
            config.chunk_size.to_string(),
        ),
        (
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_REQUEST_CAP".to_owned(),
            config.request_cap.to_string(),
        ),
        (
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_EXECUTE".to_owned(),
            "true".to_owned(),
        ),
        (
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_CONFIRM_PUBLIC_API_QUOTA_IMPACT".to_owned(),
            "true".to_owned(),
        ),
        (
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_CONFIRM_NATIONAL_LEDGER_EXECUTION".to_owned(),
            "true".to_owned(),
        ),
    ];
    if !config.env_file.trim().is_empty() {
        env.push((
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_ENV_FILE".to_owned(),
            config.env_file.clone(),
        ));
    }
    if config.confirm_local_bronze_storage {
        env.push((
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_CONFIRM_LOCAL_BRONZE_STORAGE".to_owned(),
            "true".to_owned(),
        ));
    }
    if !config.cargo_exe.trim().is_empty() {
        env.push((
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_CARGO_EXE".to_owned(),
            config.cargo_exe.clone(),
        ));
    }
    if !config.runner_exe.trim().is_empty() {
        env.push((
            "FOUNDATION_PLATFORM_NATIONAL_LEDGER_RUNNER_EXE".to_owned(),
            config.runner_exe.clone(),
        ));
    }
    if let Some(interval) = chunk.provider_min_page_interval_ms {
        if config.provider_lane_mode == ProviderLaneMode::ProviderPolicy {
            env.push((
                "FOUNDATION_PLATFORM_NATIONAL_LEDGER_PROVIDER_MIN_PAGE_INTERVAL_MS".to_owned(),
                interval.to_string(),
            ));
        }
    }
    env
}

fn wait_child_result(running: RunningChunk) -> anyhow::Result<CompletedChunk> {
    let output = running
        .child
        .wait_with_output()
        .context("failed to wait for ledger executor")?;
    let finished = Utc::now();
    Ok(CompletedChunk {
        chunk_index: running.chunk_index,
        result: ChunkExecResult {
            exit_code: output.status.code().unwrap_or(1),
            output: process_output_lines(output.stdout, output.stderr),
            started_at_utc: running
                .started_at
                .to_rfc3339_opts(SecondsFormat::Nanos, true),
            finished_at_utc: finished.to_rfc3339_opts(SecondsFormat::Nanos, true),
            duration_ms: (finished - running.started_at).num_milliseconds().max(0) as u64,
        },
    })
}

fn apply_chunk_result(chunk: &mut ResumeChunkReport, result: ChunkExecResult) {
    chunk.status = if result.exit_code == 0 {
        "succeeded".to_owned()
    } else {
        "failed".to_owned()
    };
    chunk.exit_code = Some(result.exit_code);
    chunk.started_at_utc = Some(result.started_at_utc);
    chunk.finished_at_utc = Some(result.finished_at_utc);
    chunk.duration_ms = Some(result.duration_ms);
    chunk.output_tail = Some(output_tail(&result.output));
}

fn read_jsonl(path: &Path) -> anyhow::Result<Vec<JsonValue>> {
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut rows = Vec::new();
    for raw_line in content.lines() {
        let line = raw_line.trim_start_matches('\u{feff}');
        if !line.trim().is_empty() {
            rows.push(
                serde_json::from_str(line)
                    .with_context(|| format!("failed to parse JSONL row in {}", path.display()))?,
            );
        }
    }
    Ok(rows)
}

fn process_output_lines(stdout: Vec<u8>, stderr: Vec<u8>) -> Vec<String> {
    let mut output = Vec::new();
    for bytes in [stdout, stderr] {
        let text = String::from_utf8_lossy(&bytes);
        output.extend(text.lines().map(|line| line.to_owned()));
    }
    output
}

fn output_tail(output: &[String]) -> String {
    output
        .iter()
        .rev()
        .take(20)
        .cloned()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n")
}

fn wildcard_matches(pattern: &str, value: &str) -> bool {
    if !pattern.contains('*') {
        return pattern == value;
    }
    let parts = pattern.split('*').collect::<Vec<_>>();
    let mut remaining = value;
    if let Some(first) = parts.first().copied() {
        if !first.is_empty() {
            let Some(stripped) = remaining.strip_prefix(first) else {
                return false;
            };
            remaining = stripped;
        }
    }
    for part in parts.iter().skip(1).take(parts.len().saturating_sub(2)) {
        if part.is_empty() {
            continue;
        }
        let Some(position) = remaining.find(part) else {
            return false;
        };
        remaining = &remaining[position + part.len()..];
    }
    if let Some(last) = parts.last().copied() {
        if !last.is_empty() {
            return remaining.ends_with(last);
        }
    }
    true
}
