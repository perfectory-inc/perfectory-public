//! Small in-process circuit breaker for public provider HTTP clients.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::OutboundHttpError;

const DEFAULT_FAILURE_THRESHOLD: u32 = 1;
const DEFAULT_OPEN_DURATION: Duration = Duration::from_secs(30);

/// Admission policy for one in-process outbound HTTP circuit breaker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestCircuitBreakerPolicy {
    failure_threshold: u32,
    open_duration: Duration,
}

impl RequestCircuitBreakerPolicy {
    /// Default policy as a `const`, usable inside provider `const` resilience policies.
    pub const DEFAULT: Self = Self::new(DEFAULT_FAILURE_THRESHOLD, DEFAULT_OPEN_DURATION);

    /// Creates a policy with the failure threshold that opens the circuit and its open duration.
    #[must_use]
    pub const fn new(failure_threshold: u32, open_duration: Duration) -> Self {
        Self {
            failure_threshold,
            open_duration,
        }
    }
}

impl Default for RequestCircuitBreakerPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Shared in-process circuit state for one named outbound provider.
#[derive(Clone, Debug)]
pub struct RequestCircuitBreaker {
    provider: &'static str,
    policy: RequestCircuitBreakerPolicy,
    state: Arc<Mutex<RequestCircuitBreakerState>>,
}

impl RequestCircuitBreaker {
    /// Creates a closed circuit breaker for `provider`.
    #[must_use]
    pub fn new(provider: &'static str, policy: RequestCircuitBreakerPolicy) -> Self {
        Self {
            provider,
            policy,
            state: Arc::new(Mutex::new(RequestCircuitBreakerState::default())),
        }
    }

    /// Rejects a request while the circuit is open, or admits it after the open period elapses.
    ///
    /// # Errors
    /// Returns an error when the circuit remains open or its state mutex is poisoned.
    pub fn before_request(&self) -> Result<(), OutboundHttpError> {
        let mut state = self.lock_state()?;
        let Some(opened_at) = state.opened_at else {
            return Ok(());
        };

        if opened_at.elapsed() < self.policy.open_duration {
            return Err(OutboundHttpError::new(format!(
                "{} circuit breaker is open",
                self.provider
            )));
        }

        state.consecutive_failures = 0;
        state.opened_at = None;
        drop(state);
        Ok(())
    }

    /// Closes the circuit and clears its consecutive failure count.
    ///
    /// # Errors
    /// Returns an error when the state mutex is poisoned.
    pub fn record_success(&self) -> Result<(), OutboundHttpError> {
        let mut state = self.lock_state()?;
        state.consecutive_failures = 0;
        state.opened_at = None;
        drop(state);
        Ok(())
    }

    /// Records one exhausted retryable call and opens the circuit at the configured threshold.
    ///
    /// # Errors
    /// Returns an error when the state mutex is poisoned.
    pub fn record_retryable_failure(&self) -> Result<(), OutboundHttpError> {
        let mut state = self.lock_state()?;
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        if state.consecutive_failures >= self.policy.failure_threshold {
            state.opened_at = Some(Instant::now());
        }
        drop(state);
        Ok(())
    }

    fn lock_state(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, RequestCircuitBreakerState>, OutboundHttpError> {
        self.state.lock().map_err(|_| {
            OutboundHttpError::new(format!(
                "{} circuit breaker state mutex poisoned",
                self.provider
            ))
        })
    }
}

#[derive(Debug, Default)]
struct RequestCircuitBreakerState {
    consecutive_failures: u32,
    opened_at: Option<Instant>,
}
