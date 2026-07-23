use std::io;

use intelligence_api::{
    admission, app_with_admission,
    observability::install_metrics_recorder,
    routes::ROUTE_LEASE,
    state::{api_runtime_config_from_env, AppState},
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Fail-closed guard runs before any socket is opened.
    let runtime = api_runtime_config_from_env()?;
    let admission = admission::admission_config_from_lookup(|key| std::env::var(key).ok())
        .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?;

    // Warn when INTELLIGENCE_REQUEST_TIMEOUT_SECONDS >= ROUTE_LEASE: a request
    // that times out after the inline submit call may already have delivered to
    // Foundation Platform, so the next retry can trigger a double-submit;
    // Foundation Platform Idempotency-Key dedup is the only backstop in that window.
    if admission.request_timeout_seconds >= ROUTE_LEASE.as_secs() {
        tracing::warn!(
            INTELLIGENCE_REQUEST_TIMEOUT_SECONDS = admission.request_timeout_seconds,
            ROUTE_LEASE_SECONDS = ROUTE_LEASE.as_secs(),
            "INTELLIGENCE_REQUEST_TIMEOUT_SECONDS ({}) is >= ROUTE_LEASE ({} s); \
             a timed-out inline submit may already have delivered to Foundation Platform, \
             leading to a double-submit on retry — set INTELLIGENCE_REQUEST_TIMEOUT_SECONDS \
             below {} to avoid this",
            admission.request_timeout_seconds,
            ROUTE_LEASE.as_secs(),
            ROUTE_LEASE.as_secs(),
        );
    }

    let listener = tokio::net::TcpListener::bind(runtime.bind_address).await?;

    let metrics = install_metrics_recorder()
        .map_err(|error| {
            tracing::warn!(%error, "metrics recorder installation failed; /metrics will return unavailable");
        })
        .ok();

    // Spawn the Prometheus upkeep task so histogram samples are periodically
    // flushed.  Without this, unscraped histograms accumulate samples
    // unboundedly (metrics-exporter-prometheus 0.18 does not start this task
    // automatically from install_recorder()).
    if let Some(handle) = metrics.clone() {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(20));
            loop {
                interval.tick().await;
                handle.run_upkeep();
            }
        });
    }

    let state = AppState::from_env()
        .await?
        .with_inbound_auth(runtime.inbound_auth)
        .with_metrics(metrics);

    tracing::info!(address = %runtime.bind_address, "starting intelligence-platform rust api");
    axum::serve(listener, app_with_admission(state, admission))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

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
