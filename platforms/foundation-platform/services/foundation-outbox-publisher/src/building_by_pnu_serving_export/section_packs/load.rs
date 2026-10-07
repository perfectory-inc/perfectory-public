//! The load phase of gate (나): the preview alone, paced at the contract's `load_test` rate for its
//! duration, at most `max_in_flight` requests outstanding.
//!
//! The PNUs are the gate sample drawn with replacement (a seeded hash of the request number), so
//! legal dongs repeat and the mix holds cold and warm reads the way a panel's users would. A tick
//! that finds `max_in_flight` requests outstanding is shed and counted, never queued: a queue would
//! hide that the target rate was not met. Every answer must be 200; the share that is, and the
//! Worker's CPU and cut-offs over the window from Workers analytics, are what the gate holds.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::analytics::{self, AnalyticsConfig, Invocations};
use super::gate::{LoadEvidence, ServerTimingSummary};
use super::latency::{parse_server_timing, require_gzip, timed_get, timings, Accept};
use crate::by_pnu_gateway_contract::{section_pack_policy, ByPnuLane, LoadTestPolicy};

/// One load phase.
#[derive(Clone, Debug)]
pub(crate) struct LoadPlan {
    pub(crate) requests_per_second: u32,
    pub(crate) duration: Duration,
    pub(crate) max_in_flight: usize,
    /// What salts the draw besides the contract's seed ([`drawn`]): [`LOAD_DRAW`] for gate (나),
    /// the new version for a canary step ([`canary_draw`]).
    pub(crate) draw: String,
}

/// Gate (나)'s draw: the same PNUs every run, so two runs of the gate compare like with like.
pub(crate) const LOAD_DRAW: &str = "load";

/// A canary step's draw. Each rollout reads PNUs of its own: with the gate's fixed draw every
/// rollout read the same PNUs, which earlier runs had left in the old version's edge copies,
/// while the new version's pack path read them from R2, so a step compared warm answers with
/// cold ones and refused a healthy version on CPU (2026-10-07, building, 1% step). Salted with the
/// new version, both versions meet the step's first reads cold.
pub(crate) fn canary_draw(new_version: &str) -> String {
    format!("canary:{new_version}")
}

impl LoadPlan {
    pub(crate) fn from_contract(policy: &LoadTestPolicy, draw: String) -> Self {
        Self {
            requests_per_second: policy.requests_per_second,
            duration: Duration::from_secs(policy.duration_seconds),
            max_in_flight: policy.max_in_flight,
            draw,
        }
    }

    fn requests(&self) -> u64 {
        u64::from(self.requests_per_second) * self.duration.as_secs().max(1)
    }
}

/// The sample index request `number` reads: a seeded hash, the same every run of the same draw.
///
/// # Errors
/// Returns an error when the contract cannot be read.
pub(crate) fn drawn(draw: &str, number: u64, sample_len: usize) -> anyhow::Result<usize> {
    let seed = &section_pack_policy()?.cutover_gate.sample_seed;
    let digest = Sha256::digest(format!("{seed}:{draw}:{number}").as_bytes());
    let mut first = [0_u8; 8];
    first.copy_from_slice(&digest[..8]);
    let len = u64::try_from(sample_len.max(1))?;
    Ok(usize::try_from(u64::from_be_bytes(first) % len)?)
}

/// Runs the phase and, when `analytics` names it, reads the Worker's invocations for its window.
///
/// # Errors
/// Returns an error when the sample is empty or analytics refuses.
pub(crate) async fn run(
    lane: ByPnuLane,
    client: &reqwest::Client,
    plan: &LoadPlan,
    pnus: &[String],
    url_of: impl Fn(&str) -> String,
    analytics: Option<(&AnalyticsConfig, &str)>,
) -> anyhow::Result<LoadEvidence> {
    anyhow::ensure!(!pnus.is_empty(), "the load phase needs a sample");
    let total = plan.requests();
    let interval = Duration::from_secs_f64(1.0 / f64::from(plan.requests_per_second.max(1)));
    let in_flight = Arc::new(Semaphore::new(plan.max_in_flight.max(1)));
    let mut tasks = JoinSet::new();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let (started_at, started) = (Utc::now(), Instant::now());
    let mut shed = 0_u64;
    for number in 0..total {
        ticker.tick().await;
        let Ok(permit) = Arc::clone(&in_flight).try_acquire_owned() else {
            shed += 1;
            continue;
        };
        let url = url_of(&pnus[drawn(&plan.draw, number, pnus.len())?]);
        let client = client.clone();
        tasks.spawn(async move {
            // The preview answers browsers with the member as stored; anything else is a failure.
            let answer = require_gzip(timed_get(lane, &client, &url, Accept::Gzip).await, &url);
            drop(permit);
            answer
        });
    }
    let mut latency = Vec::new();
    let (mut server_total, mut server_r2) = (Vec::new(), Vec::new());
    let mut sources = BTreeMap::<String, u64>::new();
    let mut outcomes = BTreeMap::<String, u64>::new();
    let mut failures = BTreeMap::<String, u64>::new();
    let mut versions = BTreeMap::<String, u64>::new();
    let mut answered = 0_u64;
    while let Some(joined) = tasks.join_next().await {
        match joined? {
            Ok(answer) => {
                answered += 1;
                latency.push(answer.ms);
                if let Some(version) = answer.version {
                    *versions.entry(version).or_default() += 1;
                }
                if let Some(timing) = &answer.server_timing {
                    let parsed = parse_server_timing(timing);
                    server_total.extend(parsed.total_ms);
                    server_r2.extend(parsed.r2_ms);
                    for source in parsed.sources {
                        *sources.entry(source).or_default() += 1;
                    }
                    if let Some(outcome) = parsed.outcome {
                        *outcomes.entry(outcome).or_default() += 1;
                    }
                }
            }
            Err(failure) => *failures.entry(failure.class).or_default() += 1,
        }
    }
    let elapsed = started.elapsed();
    let ended_at = Utc::now();
    let sent = total - shed;
    let worker_cpu = match analytics {
        Some((config, script)) => Some(
            analytics::worker_cpu(
                client,
                config,
                &Invocations {
                    script,
                    version: None,
                    from: started_at,
                    to: ended_at + chrono::Duration::seconds(5),
                },
                sent,
            )
            .await?,
        ),
        None => None,
    };
    #[allow(clippy::cast_precision_loss)]
    let (availability, achieved) = (
        if total == 0 {
            0.0
        } else {
            answered as f64 / total as f64
        },
        sent as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
    );
    Ok(LoadEvidence {
        requests_per_second: plan.requests_per_second,
        duration_seconds: plan.duration.as_secs(),
        max_in_flight: u64::try_from(plan.max_in_flight)?,
        sent,
        answered,
        shed,
        achieved_requests_per_second: achieved,
        // A shed request counts against availability: the target rate was the load asked for.
        availability,
        failures,
        latency_ms: timings(&mut latency),
        server_timing: ServerTimingSummary {
            answers: u64::try_from(server_total.len())?,
            total_ms: timings(&mut server_total),
            r2_ms: timings(&mut server_r2),
            sources,
            outcomes,
        },
        worker_cpu,
        versions,
    })
}
