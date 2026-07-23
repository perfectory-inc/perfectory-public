use intelligence_worker::knowledge_consumer::{
    run_foundation_knowledge_consumer, KnowledgeConsumerRunStatus,
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cancel = CancellationToken::new();
    let cancel_for_signal = cancel.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        cancel_for_signal.cancel();
    });

    tracing::info!("foundation knowledge consumer starting");
    match run_foundation_knowledge_consumer(cancel).await? {
        KnowledgeConsumerRunStatus::Disabled => {
            tracing::info!("foundation knowledge consumer disabled");
        }
        KnowledgeConsumerRunStatus::Stopped => {
            tracing::info!("foundation knowledge consumer stopped");
        }
    }
    Ok(())
}

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
