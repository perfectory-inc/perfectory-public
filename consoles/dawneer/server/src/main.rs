//! Starts the Dawneer server.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use dawneer_server::app::{router, AppState};
use dawneer_server::config::Config;
use dawneer_server::oidc;
use dawneer_server::session::SessionStore;
use tower_sessions_core::ExpiredDeletion;
use tower_sessions_sqlx_store::{sqlx::postgres::PgPoolOptions, PostgresStore};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let config = Config::from_env()?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("cannot build the HTTP client")?;
    let provider = oidc::Provider::discover(&http, &config)
        .await
        .context("cannot read the issuer's discovery document; is the tunnel open?")?;
    let sessions = sessions(&config).await?;
    let bind = config.bind;
    let app = router(Arc::new(AppState::new(config, provider, http, sessions)));
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("cannot listen on {bind}"))?;
    tracing::info!(%bind, "dawneer server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .context("server stopped")
}

/// Postgres when `DAWNEER_DATABASE_URL` is set (required over https), memory otherwise. The table
/// is the compose bootstrap's; expired records are deleted hourly.
async fn sessions(config: &Config) -> anyhow::Result<SessionStore> {
    let Some(url) = config.database_url.as_deref() else {
        tracing::warn!("sessions are in memory: a restart signs everyone out");
        return Ok(SessionStore::default());
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(url)
        .await
        .context("cannot reach the session database")?;
    let store = PostgresStore::new(pool);
    tokio::spawn(
        store
            .clone()
            .continuously_delete_expired(Duration::from_secs(3600)),
    );
    Ok(SessionStore::new(store))
}
