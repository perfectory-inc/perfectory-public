#![allow(clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use intelligence_worker::knowledge_consumer::{
    run_foundation_knowledge_consumer_with, KnowledgeConsumerRunStatus,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn deployable_composition_reaches_consumer_loop_and_cancels_without_live_services() {
    let values = BTreeMap::from([
        (
            "FOUNDATION_KNOWLEDGE_CONSUMER_BOOTSTRAP_SERVERS",
            "127.0.0.1:19092",
        ),
        (
            "FOUNDATION_KNOWLEDGE_CONSUMER_ENABLE_FIXTURE_CONTRACT",
            "true",
        ),
        (
            "FOUNDATION_KNOWLEDGE_CONSUMER_KARAPACE_URL",
            "http://127.0.0.1:18081",
        ),
        ("FOUNDATION_KNOWLEDGE_CONSUMER_GROUP_ID", "ip-knowledge"),
        ("FOUNDATION_KNOWLEDGE_CONSUMER_CLIENT_ID", "ip-knowledge-1"),
        ("DATABASE_URL", "postgres://localhost/intelligence"),
    ]);
    let entered = Arc::new(Notify::new());
    let entered_by_runner = entered.clone();
    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();

    let task = tokio::spawn(async move {
        run_foundation_knowledge_consumer_with(
            |key| values.get(key).map(|value| value.to_string()),
            cancel_for_task,
            move |config, loop_cancel| async move {
                assert_eq!(config.consumer.group_id, "ip-knowledge");
                assert_eq!(config.consumer.client_id, "ip-knowledge-1");
                assert_eq!(
                    config.consumer.source_topic,
                    "intelligence-platform.fixture.foundation-knowledge-source.upserted.v1"
                );
                assert_eq!(config.database_url, "postgres://localhost/intelligence");
                assert_eq!(config.karapace_url, "http://127.0.0.1:18081");
                entered_by_runner.notify_one();
                loop_cancel.cancelled().await;
                Ok(())
            },
        )
        .await
    });

    entered.notified().await;
    cancel.cancel();

    assert_eq!(
        task.await
            .expect("composition task must join")
            .expect("runner must stop cleanly"),
        KnowledgeConsumerRunStatus::Stopped
    );
}
