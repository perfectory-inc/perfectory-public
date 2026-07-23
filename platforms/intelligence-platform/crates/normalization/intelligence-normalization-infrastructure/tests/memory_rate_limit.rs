// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use intelligence_normalization_application::{
    RateLimitDecision, RateLimitQuota, RateLimitRequest, RateLimitRouteClass, RateLimitSubject,
    RateLimiterPort,
};
use intelligence_normalization_infrastructure::{MemoryRateLimitConfig, MemoryRateLimiter};

#[tokio::test]
async fn memory_token_bucket_denies_after_capacity_is_exhausted() {
    let limiter = MemoryRateLimiter::new(MemoryRateLimitConfig {
        key_prefix: "ip-test".to_string(),
    })
    .unwrap();
    let request = request_for("tenant-1", "service:test", 2, 0.1, 1);

    assert!(matches!(
        limiter.check(request.clone()).await.unwrap(),
        RateLimitDecision::Allowed { remaining: 1 }
    ));
    assert!(matches!(
        limiter.check(request.clone()).await.unwrap(),
        RateLimitDecision::Allowed { remaining: 0 }
    ));
    let denied = limiter.check(request).await.unwrap();

    assert!(matches!(denied, RateLimitDecision::Denied { .. }));
    assert!(denied.retry_after_seconds().unwrap() >= 1);
}

#[tokio::test]
async fn memory_token_bucket_isolates_subjects_with_delimiters() {
    let limiter = MemoryRateLimiter::new(MemoryRateLimitConfig {
        key_prefix: "ip-test".to_string(),
    })
    .unwrap();

    assert!(matches!(
        limiter
            .check(request_for("a:b", "c", 1, 0.1, 1))
            .await
            .unwrap(),
        RateLimitDecision::Allowed { remaining: 0 }
    ));
    assert!(matches!(
        limiter
            .check(request_for("a", "b:c", 1, 0.1, 1))
            .await
            .unwrap(),
        RateLimitDecision::Allowed { remaining: 0 }
    ));
}

#[tokio::test]
async fn memory_token_bucket_concurrent_requests_do_not_overspend_capacity() {
    let limiter = MemoryRateLimiter::new(MemoryRateLimitConfig {
        key_prefix: "ip-test".to_string(),
    })
    .unwrap();
    let request = request_for("tenant-1", "service:test", 3, 0.1, 1);

    let mut handles = Vec::new();
    for _ in 0..12 {
        let limiter = limiter.clone();
        let request = request.clone();
        handles.push(tokio::spawn(async move {
            limiter.check(request).await.unwrap()
        }));
    }
    let mut decisions = Vec::new();
    for handle in handles {
        decisions.push(handle.await.unwrap());
    }
    let allowed = decisions
        .into_iter()
        .filter(|decision| matches!(decision, RateLimitDecision::Allowed { .. }))
        .count();

    assert_eq!(allowed, 3);
}

fn request_for(
    tenant_id: &str,
    subject_id: &str,
    capacity: u32,
    refill_per_second: f64,
    cost: u32,
) -> RateLimitRequest {
    RateLimitRequest {
        subject: RateLimitSubject {
            tenant_id: tenant_id.to_string(),
            subject_id: subject_id.to_string(),
        },
        route_class: RateLimitRouteClass::Chat,
        quota: RateLimitQuota {
            capacity,
            refill_per_second,
        },
        cost,
    }
}
