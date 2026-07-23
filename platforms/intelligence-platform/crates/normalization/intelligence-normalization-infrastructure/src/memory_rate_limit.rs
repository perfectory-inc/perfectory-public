use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use intelligence_normalization_application::{
    RateLimitDecision, RateLimitError, RateLimitRequest, RateLimiterPort,
};

#[derive(Clone, Debug, PartialEq)]
pub struct MemoryRateLimitConfig {
    pub key_prefix: String,
}

#[derive(Clone, Debug)]
pub struct MemoryRateLimiter {
    key_prefix: String,
    buckets: Arc<Mutex<HashMap<String, BucketState>>>,
}

#[derive(Clone, Copy, Debug)]
struct BucketState {
    tokens: f64,
    updated_at: Instant,
}

impl MemoryRateLimiter {
    pub fn new(config: MemoryRateLimitConfig) -> Result<Self, RateLimitError> {
        if config.key_prefix.trim().is_empty() {
            return Err(RateLimitError::Unavailable {
                message: "memory rate limit config is invalid".to_string(),
            });
        }
        Ok(Self {
            key_prefix: config.key_prefix.trim().to_string(),
            buckets: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

#[async_trait]
impl RateLimiterPort for MemoryRateLimiter {
    async fn check(&self, request: RateLimitRequest) -> Result<RateLimitDecision, RateLimitError> {
        let key = request.key(&self.key_prefix)?;
        let now = Instant::now();
        let mut buckets = self
            .buckets
            .lock()
            .map_err(|_| RateLimitError::Unavailable {
                message: "memory rate limiter lock poisoned".to_string(),
            })?;

        let bucket = buckets.entry(key).or_insert(BucketState {
            tokens: request.quota.capacity as f64,
            updated_at: now,
        });

        let elapsed = now.saturating_duration_since(bucket.updated_at);
        let refill = elapsed.as_secs_f64() * request.quota.refill_per_second;
        if refill > 0.0 {
            bucket.tokens = (bucket.tokens + refill).min(request.quota.capacity as f64);
            bucket.updated_at = now;
        }

        let cost = request.cost as f64;
        if bucket.tokens >= cost {
            bucket.tokens -= cost;
            return Ok(RateLimitDecision::allowed(bucket.tokens.floor() as u32));
        }

        let missing = cost - bucket.tokens;
        let retry_after_seconds = (missing / request.quota.refill_per_second).ceil() as u64;
        Ok(RateLimitDecision::denied(retry_after_seconds))
    }
}
