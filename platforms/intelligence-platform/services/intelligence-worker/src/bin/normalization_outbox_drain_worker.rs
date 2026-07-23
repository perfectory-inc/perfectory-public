//! Normalization outbox drain worker binary.
//!
//! Claims `FailedRetryable` / expired-`InFlight` / `Pending` outbox records
//! and delivers them to Foundation Platform via the [`FoundationNormalizationSubmitter`]
//! port.  Runs until SIGTERM or Ctrl-C.
//!
//! # Required environment variables
//!
//! - `DATABASE_URL` - Postgres connection string. The drain worker requires a
//!   durable store; refusing to start with an in-memory outbox prevents silent
//!   data loss across process restarts.
//! - `FOUNDATION_PLATFORM_BASE_URL` - Base URL for Foundation Platform. Used
//!   with `FOUNDATION_PLATFORM_INTELLIGENCE_WORKLOAD_IDENTITY_TOKEN_FILE` to
//!   authenticate submissions with a Zitadel workload bearer.
//!
//! See [`intelligence_worker::outbox_worker::DrainConfig`] for optional tuning
//! variables (`NORMALIZATION_OUTBOX_DRAIN_BATCH_SIZE`, etc.).

use std::env;
use std::io;

use intelligence_worker::outbox_worker::{
    drain_config_from_lookup, durable_outbox_from_env,
    foundation_submit_timeout_seconds_from_lookup, foundation_submitter_from_env, run_drain_loop,
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Guard: the drain worker requires a durable outbox store.  An in-memory
    // outbox cannot be drained across processes — records would be invisible to
    // any other process and lost on restart.
    if env::var("DATABASE_URL")
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the drain worker requires DATABASE_URL; \
             an in-memory outbox cannot be drained across processes",
        )
        .into());
    }

    let outbox = durable_outbox_from_env().await?;

    // Require a configured foundation submitter.
    let submitter = foundation_submitter_from_env()?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "FOUNDATION_PLATFORM_BASE_URL is required for the normalization outbox drain worker",
        )
    })?;

    // Parse drain worker configuration from environment.
    let config = drain_config_from_lookup(|key| env::var(key).ok())
        .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?;

    // Lease-vs-batch invariant check.
    // Foundation submit timeout shares the same default (10) as the
    // foundation submitter's HTTP client timeout.
    let foundation_submit_timeout_secs =
        foundation_submit_timeout_seconds_from_lookup(|key| env::var(key).ok());
    let lease_secs = config.lease.as_secs();
    if config.batch_size as u64 * foundation_submit_timeout_secs >= lease_secs {
        tracing::warn!(
            batch_size = config.batch_size,
            foundation_submit_timeout_secs,
            lease_secs,
            "batch_size * foundation submit timeout >= \
             NORMALIZATION_OUTBOX_DRAIN_LEASE_SECONDS; tail of batch may outlive \
             its lease and be reclaimed by another worker — reduce \
             NORMALIZATION_OUTBOX_DRAIN_BATCH_SIZE or increase \
             NORMALIZATION_OUTBOX_DRAIN_LEASE_SECONDS"
        );
    }

    // Wire Ctrl-C and SIGTERM to cancel the drain loop.
    let cancel = CancellationToken::new();
    let cancel_for_signal = cancel.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        cancel_for_signal.cancel();
    });

    tracing::info!("normalization outbox drain worker starting");
    run_drain_loop(outbox, submitter, config, cancel).await;
    tracing::info!("drain worker stopped");

    Ok(())
}

// Signal handler installation failure is unrecoverable at startup — the
// process cannot run safely without OS signal delivery.
#[allow(clippy::expect_used)]
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
