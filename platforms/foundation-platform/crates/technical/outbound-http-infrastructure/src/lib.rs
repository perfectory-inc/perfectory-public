//! Provider-neutral outbound HTTP resilience infrastructure.

mod error;
mod request_circuit_breaker;
mod request_resilience;

pub use error::OutboundHttpError;
pub use request_circuit_breaker::{RequestCircuitBreaker, RequestCircuitBreakerPolicy};
pub use request_resilience::{
    classify_response, classify_status, execute_retryable, execute_single,
    execute_streaming_handshake, redact_transport_error, redact_url_query_secrets,
    shared_http_client, AttemptError, ResilienceAudit, ResilienceCtx, ResilienceEvent,
    ResiliencePolicy, RetryDecision, DATA_GO_KR, HUB, ICEBERG, VWORLD_FILE, VWORLD_JSON,
};
