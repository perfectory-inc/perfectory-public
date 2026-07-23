use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use intelligence_normalization_application::{
    RateLimitDecision, RateLimitError, RateLimitRequest, RateLimiterPort,
};
use redis::Script;
use tokio::sync::Mutex;
use tokio::time::timeout;

#[derive(Clone, Debug, PartialEq)]
pub struct RedisRateLimitConfig {
    pub redis_url: String,
    pub key_prefix: String,
    pub ttl_seconds: u64,
    pub timeout_ms: u64,
}

#[derive(Clone)]
pub struct RedisRateLimiter {
    client: redis::Client,
    connection: Arc<Mutex<Option<redis::aio::MultiplexedConnection>>>,
    config: RedisRateLimitConfig,
    script: Arc<Script>,
}

impl RedisRateLimiter {
    pub async fn connect(config: RedisRateLimitConfig) -> Result<Self, RateLimitError> {
        if config.redis_url.trim().is_empty()
            || config.key_prefix.trim().is_empty()
            || config.ttl_seconds == 0
            || config.timeout_ms == 0
        {
            return Err(RateLimitError::Unavailable {
                message: "redis rate limit config is invalid".to_string(),
            });
        }

        let client = redis::Client::open(config.redis_url.as_str()).map_err(|error| {
            RateLimitError::Unavailable {
                message: error.to_string(),
            }
        })?;
        Ok(Self {
            client,
            connection: Arc::new(Mutex::new(None)),
            config,
            script: Arc::new(Script::new(TOKEN_BUCKET_LUA)),
        })
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection, RateLimitError> {
        if let Some(connection) = self.stored_connection().await? {
            return Ok(connection);
        }

        let new_connection = timeout(
            self.timeout_duration(),
            self.client.get_multiplexed_async_connection(),
        )
        .await
        .map_err(|_| self.timeout_error("redis connection timed out"))?
        .map_err(|error| RateLimitError::Unavailable {
            message: error.to_string(),
        })?;

        let mut guard = timeout(self.timeout_duration(), self.connection.lock())
            .await
            .map_err(|_| self.timeout_error("redis connection lock timed out"))?;
        *guard = Some(new_connection.clone());
        Ok(new_connection)
    }

    async fn stored_connection(
        &self,
    ) -> Result<Option<redis::aio::MultiplexedConnection>, RateLimitError> {
        let guard = timeout(self.timeout_duration(), self.connection.lock())
            .await
            .map_err(|_| self.timeout_error("redis connection lock timed out"))?;
        Ok(guard.as_ref().cloned())
    }

    async fn clear_connection(&self) {
        if let Ok(mut guard) = timeout(self.timeout_duration(), self.connection.lock()).await {
            *guard = None;
        }
    }

    fn timeout_duration(&self) -> Duration {
        Duration::from_millis(self.config.timeout_ms)
    }

    fn timeout_error(&self, message: &'static str) -> RateLimitError {
        RateLimitError::Unavailable {
            message: message.to_string(),
        }
    }
}

#[async_trait]
impl RateLimiterPort for RedisRateLimiter {
    async fn check(&self, request: RateLimitRequest) -> Result<RateLimitDecision, RateLimitError> {
        let key = request.key(&self.config.key_prefix)?;
        let mut connection = self.connection().await?;
        let invoke_result: Result<Vec<i64>, redis::RedisError> =
            timeout(self.timeout_duration(), async {
                self.script
                    .key(key)
                    .arg(request.quota.capacity)
                    .arg(request.quota.refill_per_second)
                    .arg(request.cost)
                    .arg(self.config.ttl_seconds)
                    .invoke_async(&mut connection)
                    .await
            })
            .await
            .map_err(|_| self.timeout_error("redis token bucket timed out"))?;

        let result: Vec<i64> = match invoke_result {
            Ok(result) => result,
            Err(error) => {
                self.clear_connection().await;
                return Err(RateLimitError::Unavailable {
                    message: error.to_string(),
                });
            }
        };

        if result.len() != 3 {
            return Err(RateLimitError::Unavailable {
                message: "redis token bucket returned invalid shape".to_string(),
            });
        }

        match result.as_slice() {
            [1, remaining, _] => Ok(RateLimitDecision::allowed((*remaining).max(0) as u32)),
            [0, _, retry_after] => Ok(RateLimitDecision::denied((*retry_after).max(1) as u64)),
            _ => Err(RateLimitError::Unavailable {
                message: "redis token bucket returned invalid shape".to_string(),
            }),
        }
    }
}

const TOKEN_BUCKET_LUA: &str = r#"
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_per_second = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local ttl_seconds = tonumber(ARGV[4])

local server_time = redis.call('TIME')
local now_ms = (tonumber(server_time[1]) * 1000) + math.floor(tonumber(server_time[2]) / 1000)

local state = redis.call('HMGET', key, 'tokens', 'updated_ms')
local tokens = tonumber(state[1])
local updated_ms = tonumber(state[2])

if tokens == nil then
  tokens = capacity
end
if updated_ms == nil then
  updated_ms = now_ms
end

local elapsed_ms = math.max(0, now_ms - updated_ms)
local refill = math.floor((elapsed_ms * refill_per_second) / 1000)
tokens = math.min(capacity, tokens + refill)
if refill > 0 then
  updated_ms = now_ms
end

if tokens >= cost then
  tokens = tokens - cost
  redis.call('HMSET', key, 'tokens', tokens, 'updated_ms', updated_ms)
  redis.call('EXPIRE', key, ttl_seconds)
  return {1, tokens, 0}
end

local missing = cost - tokens
local retry_after = math.ceil(missing / refill_per_second)
redis.call('HMSET', key, 'tokens', tokens, 'updated_ms', updated_ms)
redis.call('EXPIRE', key, ttl_seconds)
return {0, tokens, math.max(1, retry_after)}
"#;
