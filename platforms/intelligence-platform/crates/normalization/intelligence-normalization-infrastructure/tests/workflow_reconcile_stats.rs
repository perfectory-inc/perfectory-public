// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use intelligence_normalization_application::{
    NormalizationOutboxPort, NormalizationOutboxRecord, NormalizationOutboxStatus,
    NormalizationReconcileQueuePort,
};
use intelligence_normalization_infrastructure::InMemoryWorkflowState;

mod support;

#[tokio::test]
async fn terminal_failure_transitions_from_in_flight() {
    let workflow = InMemoryWorkflowState::default();
    let record = NormalizationOutboxRecord::new("key-terminal".to_string(), support::submission());

    workflow
        .enqueue(record, Duration::from_secs(60))
        .await
        .unwrap();
    let updated = workflow
        .mark_terminal_failure("key-terminal", "foundation rejected payload".to_string())
        .await
        .unwrap();

    assert_eq!(updated.status, NormalizationOutboxStatus::FailedTerminal);
    assert_eq!(updated.attempts, 1);
    assert_eq!(
        updated.last_error.as_deref(),
        Some("foundation rejected payload")
    );
    assert!(updated.claimed_until.is_none());
}

#[tokio::test]
async fn reconcile_stats_report_depth_and_oldest_age() {
    let workflow = Arc::new(InMemoryWorkflowState::default());
    let first = NormalizationOutboxRecord::new("key-r1".to_string(), support::submission());
    let second = NormalizationOutboxRecord::new("key-r2".to_string(), support::submission());

    workflow
        .enqueue(first, Duration::from_secs(60))
        .await
        .unwrap();
    workflow
        .mark_reconcile_required("key-r1", "ambiguous".to_string())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    workflow
        .enqueue(second, Duration::from_secs(60))
        .await
        .unwrap();
    workflow
        .mark_reconcile_required("key-r2", "invalid response".to_string())
        .await
        .unwrap();

    let stats = workflow.stats().await.unwrap();

    assert_eq!(stats.depth, 2);
    assert!(stats.oldest_age_seconds >= 0.0);
}
