//! 공짱 outbox publisher daemon — `outbox_event` row 를 폴링해 `Sink` 로 발행.
//!
//! 환경변수:
//! - `DATABASE_URL` (필수) — `Postgres` 접속 문자열
//! - `OUTBOX_POLL_INTERVAL_MS` (기본 1000) — tick 주기
//! - `OUTBOX_BATCH_SIZE` (기본 100) — tick 당 fetch limit
//! - `RUST_LOG` (기본 `info`) — `tracing-subscriber` env filter
//!
//! 종료 신호 (`SIGTERM` / `Ctrl+C`) 받으면 진행 중 tick 완료 후 graceful shutdown.

#![forbid(unsafe_code)]
// `main.rs`: init failure panic은 정답이라 expect/unwrap 허용해요.
// pedantic: `tokio::select!` 매크로 안 redundant_pub_crate / 비공개 main 의
// missing_panics_doc / cfg-gated future 의 redundant_async_block 등 false-positive 차단.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::redundant_pub_crate,
    clippy::redundant_async_block
)]

use std::env;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use foundation_platform_client::{FoundationServiceAuth, FoundationServiceAuthError};
use gongzzang_outbox::{tick, LoggingSink, Sink, SinkError};
use gongzzang_persistence::outbox::PgOutboxRepository;
use outbox_event_domain::repository::OutboxRepository;
use shared_kernel::id::{Id, OutboxEventMarker};
use sqlx::postgres::PgPoolOptions;
use thiserror::Error;
use tokio::signal;
use tokio::time;
use tracing::{error, info, warn};

use crate::foundation_lakehouse_registry::FoundationPlatformLakehouseRegistryClient;
use crate::listing_photo_lakehouse::{
    ListingPhotoLakehouseSink, ListingPhotoR2ReadConfig, R2ListingPhotoObjectReader,
};

mod foundation_lakehouse_registry;
mod listing_photo_lakehouse;

const OUTBOX_LAKEHOUSE_REGISTRY_ENABLED_ENV: &str = "OUTBOX_LAKEHOUSE_REGISTRY_ENABLED";
const FOUNDATION_PLATFORM_API_BASE_URL_ENV: &str = "FOUNDATION_PLATFORM_API_BASE_URL";
const FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE_ENV: &str =
    "FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE";

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .json()
        .init();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let interval_ms: u64 = env::var("OUTBOX_POLL_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let batch_size: u32 = env::var("OUTBOX_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let lease_seconds: u64 = env::var("OUTBOX_LEASE_SECONDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let worker_id = Id::<OutboxEventMarker>::new().as_str().to_owned();

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .expect("connect to Postgres");

    let postgres_repo = Arc::new(PgOutboxRepository::new(pool));
    postgres_repo
        .validate_delivery_lease_schema()
        .await
        .expect("outbox delivery lease migration 30018 must be applied before startup");
    let repo: Arc<dyn OutboxRepository> = postgres_repo;
    let is_production = is_production_env();
    let sink = build_sink(is_production).expect("build outbox sink");

    info!(interval_ms, batch_size, lease_seconds, worker_id = %worker_id, "outbox publisher starting");

    let mut interval = time::interval(Duration::from_millis(interval_ms));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                match tick(
                    repo.as_ref(),
                    sink.as_ref(),
                    batch_size,
                    &worker_id,
                    Duration::from_secs(lease_seconds),
                ).await {
                    Ok(report) if report.fetched > 0 => {
                        info!(
                            fetched = report.fetched,
                            published = report.published,
                            failed = report.failed,
                            "tick"
                        );
                    }
                    Ok(_) => {} // empty tick — silent (운영 spam 방지)
                    Err(e) => error!(error = %e, "tick failed"),
                }
            }
            () = shutdown_signal() => {
                info!("shutdown signal received — stopping");
                break;
            }
        }
    }
}

/// `SIGTERM` (Unix) / `Ctrl+C` 대기.
///
/// Windows 빌드는 `SIGTERM` 미지원 — `pending::<()>()` 로 대체해 `Ctrl+C` 만 동작.
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("install ctrl-c handler");
    };
    #[cfg(unix)]
    let term = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = term => {}
    }
}

#[derive(Debug, Error)]
enum OutboxSinkConfigError {
    #[error("{name} must be set")]
    MissingEnv { name: &'static str },
    #[error("{name} must be true, false, 1, or 0")]
    InvalidBoolEnv { name: &'static str },
    #[error("listing photo R2 read config: {0}")]
    ListingPhotoR2(#[from] listing_photo_lakehouse::ListingPhotoR2ReadConfigError),
    #[error("Foundation Platform workload auth: {0}")]
    FoundationServiceAuth(#[from] FoundationServiceAuthError),
    #[error("Foundation Platform Lakehouse Registry config: {0}")]
    LakehouseRegistryConfig(
        #[from] foundation_lakehouse_registry::FoundationPlatformLakehouseRegistryConfigError,
    ),
}

fn build_sink(is_production: bool) -> Result<Box<dyn Sink>, OutboxSinkConfigError> {
    if !lakehouse_registry_enabled(is_production)? {
        warn!(
            "outbox lakehouse registry sink disabled - listing photo media lineage is not registered"
        );
        return Ok(Box::new(LoggingSink::new()));
    }

    let reader = Arc::new(R2ListingPhotoObjectReader::new(
        ListingPhotoR2ReadConfig::from_env()?,
    ));
    let service_auth = build_worker_foundation_service_auth()?;
    let api_base_url = optional_env(FOUNDATION_PLATFORM_API_BASE_URL_ENV).ok_or(
        OutboxSinkConfigError::MissingEnv {
            name: FOUNDATION_PLATFORM_API_BASE_URL_ENV,
        },
    )?;
    let registry = Arc::new(FoundationPlatformLakehouseRegistryClient::new(
        &api_base_url,
        service_auth,
    )?);
    let listing_photo_sink = ListingPhotoLakehouseSink::new(reader, registry);
    Ok(Box::new(FanoutSink::new(
        LoggingSink::new(),
        listing_photo_sink,
    )))
}

fn lakehouse_registry_enabled(is_production: bool) -> Result<bool, OutboxSinkConfigError> {
    lakehouse_registry_enabled_value(
        is_production,
        optional_env(OUTBOX_LAKEHOUSE_REGISTRY_ENABLED_ENV).as_deref(),
    )
}

fn lakehouse_registry_enabled_value(
    is_production: bool,
    value: Option<&str>,
) -> Result<bool, OutboxSinkConfigError> {
    match value {
        None => Ok(is_production),
        Some("true" | "1") => Ok(true),
        Some("false" | "0") => Ok(false),
        Some(_) => Err(OutboxSinkConfigError::InvalidBoolEnv {
            name: OUTBOX_LAKEHOUSE_REGISTRY_ENABLED_ENV,
        }),
    }
}

fn build_worker_foundation_service_auth() -> Result<FoundationServiceAuth, OutboxSinkConfigError> {
    let token_file = optional_env(FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE_ENV).ok_or(
        OutboxSinkConfigError::MissingEnv {
            name: FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE_ENV,
        },
    )?;
    FoundationServiceAuth::from_workload_identity_token_file(token_file)
        .map_err(OutboxSinkConfigError::from)
}

fn is_production_env() -> bool {
    env::var("APP_ENV").as_deref() == Ok("production")
        || env::var("NODE_ENV").as_deref() == Ok("production")
}

fn optional_env(name: &'static str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

#[derive(Debug)]
struct FanoutSink<A, B> {
    first: A,
    second: B,
}

impl<A, B> FanoutSink<A, B> {
    const fn new(first: A, second: B) -> Self {
        Self { first, second }
    }
}

#[async_trait]
impl<A, B> Sink for FanoutSink<A, B>
where
    A: Sink + Send + Sync,
    B: Sink + Send + Sync,
{
    async fn publish(
        &self,
        event: &outbox_event_domain::entity::OutboxEvent,
    ) -> Result<(), SinkError> {
        self.first.publish(event).await?;
        self.second.publish(event).await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::sync::{Mutex, OnceLock};

    use super::*;

    #[test]
    fn lakehouse_registry_defaults_to_enabled_in_production() {
        assert!(lakehouse_registry_enabled_value(true, None).expect("enabled"));
    }

    #[test]
    fn lakehouse_registry_defaults_to_disabled_outside_production() {
        assert!(!lakehouse_registry_enabled_value(false, None).expect("disabled"));
    }

    static ENV_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    fn lock_env_tests() -> std::sync::MutexGuard<'static, ()> {
        ENV_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env test lock")
    }

    #[test]
    fn worker_auth_rejects_static_service_token_fallback() {
        let _guard = lock_env_tests();
        std::env::remove_var(FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE_ENV);
        std::env::set_var(
            "FOUNDATION_PLATFORM_SERVICE_TOKEN",
            "foundation-static-service-token-32-valid",
        );

        let result = build_worker_foundation_service_auth();

        std::env::remove_var("FOUNDATION_PLATFORM_SERVICE_TOKEN");
        assert!(matches!(
            result,
            Err(OutboxSinkConfigError::MissingEnv {
                name: FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE_ENV
            })
        ));
    }
}
