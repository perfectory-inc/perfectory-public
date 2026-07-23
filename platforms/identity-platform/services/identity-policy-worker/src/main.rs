//! Identity policy event publisher process.

use std::ffi::OsStr;
use std::sync::Arc;

use identity_policy_worker::{
    run_until_shutdown, DeliveryWorker, HttpEventPublisher, PgOutboxRepository, WorkerConfig,
};
use sqlx::postgres::PgPoolOptions;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if healthcheck_requested(std::env::args_os().nth(1).as_deref()) {
        let config = WorkerConfig::from_env()?;
        return check_worker_readiness(&config).await;
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .json()
        .init();

    let config = WorkerConfig::from_env()?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_lazy(&config.database_url)?;
    let publisher = HttpEventPublisher::new(config.endpoint, config.publish_timeout)?;
    let worker = DeliveryWorker::new(
        Arc::new(PgOutboxRepository::new(pool, config.repository_timeout)),
        Arc::new(publisher),
        config.worker_options,
    );
    run_until_shutdown(
        &worker,
        config.poll_interval,
        shutdown_signal(),
        |tick| match tick {
            Ok(stats) => {
                info!(
                    claimed = stats.claimed,
                    published = stats.published,
                    failed = stats.failed,
                    "Identity outbox poll completed"
                );
            }
            Err(worker_error) => {
                error!(
                    error_code = worker_error.error_code(),
                    "Identity outbox poll failed"
                );
            }
        },
    )
    .await?;
    info!("Identity policy worker shutdown requested");
    Ok(())
}

async fn check_worker_readiness(config: &WorkerConfig) -> anyhow::Result<()> {
    tokio::time::timeout(config.repository_timeout, async {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(config.repository_timeout)
            .connect(&config.database_url)
            .await?;
        sqlx::query("SELECT 1 FROM identity.outbox_event LIMIT 0")
            .execute(&pool)
            .await?;
        pool.close().await;
        anyhow::Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("Identity worker readiness database probe timed out"))?
}

fn healthcheck_requested(argument: Option<&OsStr>) -> bool {
    argument == Some(OsStr::new("--healthcheck"))
}

#[cfg(not(unix))]
async fn shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}

#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        signal = terminate.recv() => signal.map_or_else(
            || Err(std::io::Error::other("SIGTERM listener closed")),
            |()| Ok(()),
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::time::Duration;

    use identity_policy_worker::{PublisherEndpoint, WorkerConfig, WorkerOptions};

    use super::{check_worker_readiness, healthcheck_requested};

    #[test]
    fn healthcheck_mode_is_explicit() {
        assert!(healthcheck_requested(Some(OsStr::new("--healthcheck"))));
        assert!(!healthcheck_requested(Some(OsStr::new("run"))));
        assert!(!healthcheck_requested(None));
    }

    #[tokio::test]
    async fn healthcheck_rejects_an_unreachable_database() -> anyhow::Result<()> {
        let config =
            readiness_config("postgres://identity_policy_worker:unused@127.0.0.1:1/identity")?;

        assert!(check_worker_readiness(&config).await.is_err());
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a migrated Identity PostgreSQL database"]
    async fn healthcheck_accepts_a_reachable_migrated_database() -> anyhow::Result<()> {
        let database_url = std::env::var("IDENTITY_LIVE_TEST_DATABASE_URL")?;
        let config = readiness_config(&database_url)?;

        check_worker_readiness(&config).await
    }

    fn readiness_config(database_url: &str) -> anyhow::Result<WorkerConfig> {
        Ok(WorkerConfig {
            database_url: database_url.to_owned(),
            endpoint: PublisherEndpoint::parse("http://127.0.0.1:19090/identity-events")?,
            poll_interval: Duration::from_secs(1),
            publish_timeout: Duration::from_secs(1),
            repository_timeout: Duration::from_millis(200),
            worker_options: WorkerOptions {
                worker_id: "healthcheck-test".to_owned(),
                batch_size: 1,
                lease_duration: Duration::from_secs(10),
                base_backoff: Duration::from_secs(1),
                max_backoff: Duration::from_secs(2),
            },
        })
    }
}
