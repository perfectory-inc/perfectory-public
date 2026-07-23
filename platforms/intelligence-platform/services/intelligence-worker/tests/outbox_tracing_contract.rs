#![allow(clippy::expect_used)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use intelligence_normalization_application::{
    DrainOutcomeEvent, DrainOutcomeKind, DrainSummary, DrainTransitionCause, DrainTransitionClass,
    DrainTransitionFailure, DrainTransitionStage,
};
use intelligence_worker::outbox_worker::emit_drain_events;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::{Layer, Registry};

#[test]
fn emitted_events_preserve_exact_outbox_messages_and_error_fields() {
    let captured = CapturedEvents::default();
    let subscriber = Registry::default().with(captured.clone());
    let summary = DrainSummary {
        transition_failures: vec![
            transition_failure(
                "record-failure",
                DrainTransitionStage::RecordSubmissionFailure,
                DrainTransitionClass::Retryable,
            ),
            transition_failure(
                "dead-letter-failure",
                DrainTransitionStage::MarkDeadLetter,
                DrainTransitionClass::RetryBudgetExhausted,
            ),
            transition_failure(
                "mark-sent-failure",
                DrainTransitionStage::MarkSent,
                DrainTransitionClass::SuccessfulSubmission,
            ),
        ],
        outcome_events: vec![
            submission_failure(
                "retryable",
                DrainTransitionClass::Retryable,
                "foundation-platform submission failed before send",
            ),
            submission_failure(
                "terminal",
                DrainTransitionClass::Terminal,
                "foundation-platform rejected normalization submission with status 422",
            ),
            submission_failure(
                "reconcile",
                DrainTransitionClass::ReconcileRequired,
                "foundation-platform submission outcome is ambiguous",
            ),
            DrainOutcomeEvent {
                idempotency_key: "dead-lettered".to_string(),
                kind: DrainOutcomeKind::DeadLettered,
                class: DrainTransitionClass::RetryBudgetExhausted,
                attempts: Some(8),
                safe_diagnostic: None,
            },
        ],
        ..DrainSummary::default()
    };

    tracing::subscriber::with_default(subscriber, || emit_drain_events(&summary));

    let events = captured.events();
    assert_event(
        &events,
        "normalization submission failed; marked retryable",
        Some("foundation-platform submission failed before send"),
    );
    assert_event(
        &events,
        "normalization submission failed; marked terminal",
        Some("foundation-platform rejected normalization submission with status 422"),
    );
    assert_event(
        &events,
        "normalization submission failed; marked reconcile_required",
        Some("foundation-platform submission outcome is ambiguous"),
    );
    assert_event(
        &events,
        "failed to record submission failure",
        Some("outbox store failed"),
    );
    assert_event(
        &events,
        "mark_dead_letter failed; skipping record",
        Some("outbox store failed"),
    );
    assert_event(
        &events,
        "submission delivered but mark_sent failed (R3); record may require manual reconciliation",
        Some("outbox store failed"),
    );
    assert_event(
        &events,
        "record dead-lettered; operator action required",
        None,
    );
}

fn transition_failure(
    idempotency_key: &str,
    stage: DrainTransitionStage,
    class: DrainTransitionClass,
) -> DrainTransitionFailure {
    DrainTransitionFailure {
        idempotency_key: idempotency_key.to_string(),
        stage,
        class,
        cause: DrainTransitionCause::StoreFailed,
        safe_diagnostic: "outbox store failed".to_string(),
    }
}

fn submission_failure(
    idempotency_key: &str,
    class: DrainTransitionClass,
    safe_diagnostic: &str,
) -> DrainOutcomeEvent {
    DrainOutcomeEvent {
        idempotency_key: idempotency_key.to_string(),
        kind: DrainOutcomeKind::SubmissionFailureRecorded,
        class,
        attempts: Some(1),
        safe_diagnostic: Some(safe_diagnostic.to_string()),
    }
}

fn assert_event(events: &[CapturedEvent], message: &str, error: Option<&str>) {
    let event = events
        .iter()
        .find(|event| event.fields.get("message").map(String::as_str) == Some(message))
        .unwrap_or_else(|| panic!("missing emitted tracing event: {message}"));
    assert_eq!(event.fields.get("error").map(String::as_str), error);
}

#[derive(Clone, Default)]
struct CapturedEvents {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl CapturedEvents {
    fn events(&self) -> Vec<CapturedEvent> {
        self.events.lock().expect("captured events mutex").clone()
    }
}

impl<S> Layer<S> for CapturedEvents
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        self.events
            .lock()
            .expect("captured events mutex")
            .push(CapturedEvent {
                fields: visitor.fields,
            });
    }
}

#[derive(Clone, Debug)]
struct CapturedEvent {
    fields: BTreeMap<String, String>,
}

#[derive(Default)]
struct FieldVisitor {
    fields: BTreeMap<String, String>,
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        let value = format!("{value:?}");
        self.fields.insert(
            field.name().to_string(),
            value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .unwrap_or(&value)
                .to_string(),
        );
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }
}
