//! Public contract checks for provider-neutral outbound HTTP resilience.

use std::time::Duration;

use outbound_http_infrastructure::{
    OutboundHttpError, RequestCircuitBreakerPolicy, ResiliencePolicy,
};

#[test]
fn invalid_policy_returns_provider_neutral_error() -> Result<(), Box<dyn std::error::Error>> {
    let policy = ResiliencePolicy {
        connect_timeout: Duration::from_secs(1),
        read_timeout: Duration::from_secs(1),
        total_timeout: Some(Duration::from_secs(1)),
        max_attempts: 0,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
        jitter: false,
        circuit_breaker: RequestCircuitBreakerPolicy::default(),
    };

    let error: OutboundHttpError = policy
        .validate()
        .err()
        .ok_or("zero attempts must be rejected")?;

    assert_eq!(
        error.to_string(),
        "resilience policy max_attempts must be greater than zero"
    );
    Ok(())
}
