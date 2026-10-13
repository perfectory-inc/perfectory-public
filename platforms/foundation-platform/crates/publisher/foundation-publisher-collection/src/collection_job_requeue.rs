//! Operator requeue for one dead-lettered Collection Event Fabric job.
//!
//! Dead-lettering is terminal for every consumer (ADR 0013), so a job that died on a
//! transient failure blocks its source lane until an operator intervenes — the daily sweep
//! (ADR 0077) reports the whole run as blocked for as long as the corpse sits in the queue.
//! This command names exactly one job, returns it to `pending` with a fresh attempt budget,
//! and prints the outcome; the next sweep run claims it like any other pending job.

use anyhow::Context;
use chrono::Duration;
use foundation_outbox::PostgresJobBus;
use sqlx::PgPool;
use tracing::info;

const JOB_ID_ENV: &str = "FOUNDATION_PLATFORM_COLLECTION_JOB_REQUEUE_JOB_ID";

pub async fn run() -> anyhow::Result<()> {
    let job_id = std::env::var(JOB_ID_ENV).with_context(|| {
        format!("{JOB_ID_ENV} is required: the dead-lettered collection job id to requeue")
    })?;
    let job_id = job_id.trim().to_owned();
    anyhow::ensure!(!job_id.is_empty(), "{JOB_ID_ENV} must not be blank");

    let pool = PgPool::connect(&std::env::var("DATABASE_URL").context("DATABASE_URL is required")?)
        .await
        .context("failed to connect to database for collection job requeue")?;
    // max_attempts and lease_timeout do not participate in a requeue; the bus only needs a
    // pool for this operation, so the remaining constructor arguments are minimal.
    let bus = PostgresJobBus::new(pool, 1, Duration::seconds(1));

    if bus.requeue_dead_lettered(&job_id).await? {
        info!(job_id, "dead-lettered collection job returned to pending");
        return Ok(());
    }
    let state = bus.job_state(&job_id).await?;
    match state {
        Some(state) => anyhow::bail!(
            "collection job {job_id} is '{state}', not 'dead_lettered'; refusing to requeue"
        ),
        None => anyhow::bail!("collection job {job_id} does not exist"),
    }
}
