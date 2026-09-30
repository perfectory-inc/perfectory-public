//! Starts the Dawneer server.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use dawneer_server::app::{router, AppState};
use dawneer_server::config::Config;
use dawneer_server::oidc;
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
    let discovery = oidc::discover(&http, &config)
        .await
        .context("cannot read the issuer's discovery document; is the tunnel open?")?;
    let bind = config.bind;
    let app = router(Arc::new(AppState::new(config, discovery, http)));
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
