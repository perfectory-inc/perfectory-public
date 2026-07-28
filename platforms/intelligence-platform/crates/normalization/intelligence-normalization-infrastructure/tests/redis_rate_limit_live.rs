// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use intelligence_normalization_application::{
    RateLimitDecision, RateLimitRequest, RateLimitRouteClass, RateLimitSubject, RateLimiterPort,
};
use intelligence_normalization_infrastructure::redis_rate_limit::{
    RedisRateLimitConfig, RedisRateLimiter,
};

/// Live-Redis endpoint for this suite.
///
/// These tests are `#[ignore]`d, so they run only when a harness explicitly asks
/// for them. Having been asked, a missing endpoint is a provisioning failure, not
/// a reason to report success: an unrun contract test must never be
/// indistinguishable from a verified one. (`RedisRateLimiter`'s token-bucket
/// atomicity has no other executed coverage in this repository.)
fn live_redis_url() -> String {
    match std::env::var("INTELLIGENCE_REDIS_LIVE_TEST_URL") {
        Ok(url) => url,
        Err(_) => panic!(
            "INTELLIGENCE_REDIS_LIVE_TEST_URL must be set to run the live Redis lane; \
             provision Redis/Valkey and export it, or do not request ignored tests"
        ),
    }
}

#[tokio::test]
#[ignore = "requires a live Redis/Valkey endpoint in INTELLIGENCE_REDIS_LIVE_TEST_URL"]
async fn redis_token_bucket_denies_after_capacity_is_exhausted() {
    let url = live_redis_url();
    let limiter = RedisRateLimiter::connect(RedisRateLimitConfig {
        redis_url: url,
        key_prefix: format!("ip-test-{}", uuid::Uuid::new_v4()),
        ttl_seconds: 60,
        timeout_ms: 500,
    })
    .await
    .unwrap();

    let request = RateLimitRequest {
        subject: RateLimitSubject {
            tenant_id: "tenant-1".to_string(),
            subject_id: "service:test".to_string(),
        },
        route_class: RateLimitRouteClass::Chat,
        quota: intelligence_normalization_application::RateLimitQuota {
            capacity: 2,
            refill_per_second: 0.1,
        },
        cost: 1,
    };

    assert!(matches!(
        limiter.check(request.clone()).await.unwrap(),
        RateLimitDecision::Allowed { .. }
    ));
    assert!(matches!(
        limiter.check(request.clone()).await.unwrap(),
        RateLimitDecision::Allowed { .. }
    ));
    let denied = limiter.check(request).await.unwrap();

    assert!(matches!(denied, RateLimitDecision::Denied { .. }));
    assert!(denied.retry_after_seconds().unwrap() >= 1);
}

#[tokio::test]
#[ignore = "requires a live Redis/Valkey endpoint in INTELLIGENCE_REDIS_LIVE_TEST_URL"]
async fn redis_token_bucket_concurrent_requests_do_not_overspend_capacity() {
    let url = live_redis_url();
    let limiter = RedisRateLimiter::connect(RedisRateLimitConfig {
        redis_url: url,
        key_prefix: format!("ip-test-{}", uuid::Uuid::new_v4()),
        ttl_seconds: 60,
        timeout_ms: 500,
    })
    .await
    .unwrap();

    let request = RateLimitRequest {
        subject: RateLimitSubject {
            tenant_id: "tenant-1".to_string(),
            subject_id: "service:test".to_string(),
        },
        route_class: RateLimitRouteClass::Chat,
        quota: intelligence_normalization_application::RateLimitQuota {
            capacity: 3,
            refill_per_second: 0.1,
        },
        cost: 1,
    };

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
