// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use intelligence_normalization_application::{
    RateLimitDecision, RateLimitError, RateLimitQuota, RateLimitRequest, RateLimitRouteClass,
    RateLimitSubject,
};

#[test]
fn route_class_serializes_to_stable_key_segment() {
    assert_eq!(RateLimitRouteClass::Chat.as_key_segment(), "chat");
    assert_eq!(RateLimitRouteClass::Retrieval.as_key_segment(), "retrieval");
    assert_eq!(
        RateLimitRouteClass::GraphContext.as_key_segment(),
        "graph_context"
    );
    assert_eq!(
        RateLimitRouteClass::NormalizationSubmit.as_key_segment(),
        "normalization_submit"
    );
    assert_eq!(
        RateLimitRouteClass::BatchControl.as_key_segment(),
        "batch_control"
    );
}

#[test]
fn request_builds_enterprise_key_from_verified_identity() {
    let request = RateLimitRequest {
        subject: RateLimitSubject {
            tenant_id: " tenant-1 ".to_string(),
            subject_id: " service:worker-1 ".to_string(),
        },
        route_class: RateLimitRouteClass::NormalizationSubmit,
        quota: RateLimitQuota {
            capacity: 10,
            refill_per_second: 1.0,
        },
        cost: 2,
    };

    assert_eq!(
        request.key("ip").unwrap(),
        "ip:rate:74656e616e742d31:736572766963653a776f726b65722d31:normalization_submit"
    );
}

#[test]
fn request_key_encoding_prevents_delimiter_collisions() {
    let first = RateLimitRequest {
        subject: RateLimitSubject {
            tenant_id: "a:b".to_string(),
            subject_id: "c".to_string(),
        },
        route_class: RateLimitRouteClass::Chat,
        quota: RateLimitQuota {
            capacity: 10,
            refill_per_second: 1.0,
        },
        cost: 1,
    };
    let second = RateLimitRequest {
        subject: RateLimitSubject {
            tenant_id: "a".to_string(),
            subject_id: "b:c".to_string(),
        },
        route_class: RateLimitRouteClass::Chat,
        quota: RateLimitQuota {
            capacity: 10,
            refill_per_second: 1.0,
        },
        cost: 1,
    };

    assert_ne!(first.key("ip").unwrap(), second.key("ip").unwrap());
}

#[test]
fn request_rejects_empty_identity_parts() {
    let request = RateLimitRequest {
        subject: RateLimitSubject {
            tenant_id: "tenant-1".to_string(),
            subject_id: " ".to_string(),
        },
        route_class: RateLimitRouteClass::Chat,
        quota: RateLimitQuota {
            capacity: 10,
            refill_per_second: 1.0,
        },
        cost: 1,
    };

    let error = request.key("ip").unwrap_err();

    assert!(matches!(error, RateLimitError::InvalidSubject { .. }));
    assert_eq!(error.safe_message(), "rate limit subject is invalid");
}

#[test]
fn denied_decision_requires_positive_retry_after() {
    let decision = RateLimitDecision::denied(0);

    assert_eq!(decision.retry_after_seconds(), Some(1));
}
