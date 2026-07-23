//! Operator recovery for ingestion runs left non-terminal by a crashed collector.
//!
//! A hard crash (OOM, kill, power loss) between `create_ingestion_run` and
//! `complete_ingestion_run` leaves a run stuck at `Planned`/`Running` with `finished_at` NULL
//! forever; the per-source reconcile tooling refuses non-terminal runs, so recovery previously
//! required manual SQL. This command lets an operator explicitly abandon a named stuck run,
//! transitioning it to `Cancelled` so reconcile (and audit) can proceed.
//!
//! The transition is operator-driven by run id plus an explicit confirmation rather than a
//! time-based staleness heuristic: a national collection can legitimately run for hours, so
//! "old" must not be inferred as "dead" — only the operator, who knows the process is gone,
//! names the run.

use anyhow::{bail, Context};
use chrono::Utc;
use collection_application::ports::{
    BronzeIngestRepository, BronzeIngestUnitOfWork, CompleteIngestionRunCommand,
};
use collection_domain::{IngestionRun, IngestionRunStatus};
use collection_infrastructure::{PgBronzeIngestRepository, PgBronzeIngestUnitOfWork};
use foundation_shared_kernel::ids::IngestionRunId;
use sqlx::PgPool;
use uuid::Uuid;

use crate::public_data_control_support::optional_env_value;

const RUN_ID_ENV: &str = "FOUNDATION_PLATFORM_ABANDON_INGESTION_RUN_ID";
const REASON_ENV: &str = "FOUNDATION_PLATFORM_ABANDON_INGESTION_RUN_REASON";
const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_CONFIRM_ABANDON_INGESTION_RUN";
const DEFAULT_ABANDON_REASON: &str = "abandoned by operator after non-terminal crash";

/// Decides whether a run in `status` may be abandoned (cancelled) by the operator.
///
/// Only non-terminal runs (`Planned`/`Running`) are eligible — a crash leaves these stuck. A run
/// that already reached a terminal state (`Succeeded`/`Failed`/`Cancelled`) has nothing to
/// recover and is rejected so the command cannot silently rewrite finished history.
///
/// # Errors
/// Returns an error when the run is already terminal.
fn assert_abandonable(status: IngestionRunStatus) -> anyhow::Result<()> {
    match status {
        IngestionRunStatus::Planned | IngestionRunStatus::Running => Ok(()),
        IngestionRunStatus::Succeeded
        | IngestionRunStatus::Failed
        | IngestionRunStatus::Cancelled => {
            bail!(
                "ingestion run is already terminal ({}); only Planned or Running runs can be abandoned",
                status.wire_name()
            )
        }
    }
}

/// Abandons a stuck ingestion run by transitioning it to `Cancelled`, preserving the metrics
/// observed before the crash and recording the operator-supplied reason.
///
/// # Errors
/// Returns an error when the run is missing, already terminal, or persistence fails.
pub(crate) async fn abandon_ingestion_run<Repo, Uow>(
    repo: &Repo,
    uow: &Uow,
    run_id: IngestionRunId,
    reason: &str,
) -> anyhow::Result<IngestionRun>
where
    Repo: BronzeIngestRepository + ?Sized,
    Uow: BronzeIngestUnitOfWork + ?Sized,
{
    let run = repo
        .find_ingestion_run(run_id)
        .await
        .with_context(|| format!("failed to load ingestion run {run_id}"))?
        .with_context(|| format!("ingestion run not found: {run_id}"))?;
    assert_abandonable(run.status)?;
    let cancelled = uow
        .complete_ingestion_run(CompleteIngestionRunCommand {
            id: run.id,
            status: IngestionRunStatus::Cancelled,
            finished_at: Utc::now(),
            logical_records_seen: run.logical_records_seen,
            objects_written: run.objects_written,
            error_message: Some(reason.to_owned()),
        })
        .await
        .with_context(|| format!("failed to cancel ingestion run {run_id}"))?;
    Ok(cancelled)
}

/// Runs the `abandon-ingestion-run` operator command.
///
/// # Errors
/// Returns an error when the run id is missing/invalid, confirmation is absent, the database is
/// unreachable, or the run cannot be abandoned.
pub async fn run() -> anyhow::Result<()> {
    let run_id = run_id_from_env()?;
    let reason =
        optional_env_value(REASON_ENV)?.unwrap_or_else(|| DEFAULT_ABANDON_REASON.to_owned());
    require_confirmation()?;

    let database_url = optional_env_value("DATABASE_URL")?
        .context("DATABASE_URL is required to abandon an ingestion run")?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to database for ingestion run abandon")?;
    let repo = PgBronzeIngestRepository::new(pool.clone());
    let uow = PgBronzeIngestUnitOfWork::new(pool);

    let cancelled = abandon_ingestion_run(&repo, &uow, run_id, &reason).await?;
    tracing::warn!(
        run_id = %cancelled.id,
        status = cancelled.status.wire_name(),
        "ingestion run abandoned (cancelled) by operator"
    );
    println!(
        "ingestion-run-abandoned run_id={} status={}",
        cancelled.id,
        cancelled.status.wire_name()
    );
    Ok(())
}

fn run_id_from_env() -> anyhow::Result<IngestionRunId> {
    let raw =
        optional_env_value(RUN_ID_ENV)?.with_context(|| format!("{RUN_ID_ENV} is required"))?;
    let uuid = Uuid::parse_str(&raw).with_context(|| format!("{RUN_ID_ENV} must be a UUID"))?;
    Ok(IngestionRunId::new(uuid))
}

fn require_confirmation() -> anyhow::Result<()> {
    let confirmed = optional_env_value(CONFIRM_ENV)?.is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes"
        )
    });
    if !confirmed {
        bail!(
            "abandoning an ingestion run is irreversible; set {CONFIRM_ENV}=1 to confirm the \
             collector process for this run is no longer alive"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::assert_abandonable;
    use collection_domain::IngestionRunStatus;

    #[test]
    fn non_terminal_runs_are_abandonable() -> anyhow::Result<()> {
        assert_abandonable(IngestionRunStatus::Planned)?;
        assert_abandonable(IngestionRunStatus::Running)?;
        Ok(())
    }

    #[test]
    fn terminal_runs_are_rejected() -> anyhow::Result<()> {
        for status in [
            IngestionRunStatus::Succeeded,
            IngestionRunStatus::Failed,
            IngestionRunStatus::Cancelled,
        ] {
            let error = match assert_abandonable(status) {
                Ok(()) => anyhow::bail!(
                    "terminal run {} must not be abandonable",
                    status.wire_name()
                ),
                Err(error) => error,
            };
            assert!(
                error.to_string().contains("already terminal"),
                "unexpected error for {}: {error}",
                status.wire_name()
            );
        }
        Ok(())
    }
}
