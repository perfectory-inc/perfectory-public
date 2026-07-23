use async_trait::async_trait;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RateLimitRouteClass {
    Chat,
    Retrieval,
    GraphContext,
    NormalizationSubmit,
    BatchControl,
}

impl RateLimitRouteClass {
    pub fn as_key_segment(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::Retrieval => "retrieval",
            Self::GraphContext => "graph_context",
            Self::NormalizationSubmit => "normalization_submit",
            Self::BatchControl => "batch_control",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RateLimitSubject {
    pub tenant_id: String,
    pub subject_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RateLimitQuota {
    pub capacity: u32,
    pub refill_per_second: f64,
}

impl RateLimitQuota {
    pub fn validate(self) -> Result<(), RateLimitError> {
        if self.capacity == 0
            || !self.refill_per_second.is_finite()
            || self.refill_per_second <= 0.0
        {
            return Err(RateLimitError::InvalidSubject {
                message: "positive capacity and refill_per_second are required".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RateLimitRequest {
    pub subject: RateLimitSubject,
    pub route_class: RateLimitRouteClass,
    pub quota: RateLimitQuota,
    pub cost: u32,
}

impl RateLimitRequest {
    pub fn key(&self, prefix: &str) -> Result<String, RateLimitError> {
        let prefix = normalize_part(prefix);
        let tenant_id = normalize_part(&self.subject.tenant_id);
        let subject_id = normalize_part(&self.subject.subject_id);
        if prefix.is_empty() || tenant_id.is_empty() || subject_id.is_empty() || self.cost == 0 {
            return Err(RateLimitError::InvalidSubject {
                message: "prefix, tenant_id, subject_id, and positive cost are required"
                    .to_string(),
            });
        }
        self.quota.validate()?;
        if self.cost > self.quota.capacity {
            return Err(RateLimitError::InvalidSubject {
                message: "cost must not exceed capacity".to_string(),
            });
        }
        Ok(format!(
            "{prefix}:rate:{}:{}:{}",
            hex_encode(tenant_id.as_bytes()),
            hex_encode(subject_id.as_bytes()),
            self.route_class.as_key_segment()
        ))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RateLimitDecision {
    Allowed { remaining: u32 },
    Denied { retry_after_seconds: u64 },
}

impl RateLimitDecision {
    pub fn allowed(remaining: u32) -> Self {
        Self::Allowed { remaining }
    }

    pub fn denied(retry_after_seconds: u64) -> Self {
        Self::Denied {
            retry_after_seconds: retry_after_seconds.max(1),
        }
    }

    pub fn retry_after_seconds(self) -> Option<u64> {
        match self {
            Self::Allowed { .. } => None,
            Self::Denied {
                retry_after_seconds,
            } => Some(retry_after_seconds),
        }
    }
}

#[derive(Debug, Error)]
pub enum RateLimitError {
    #[error("rate limit subject is invalid: {message}")]
    InvalidSubject { message: String },
    #[error("rate limiter unavailable: {message}")]
    Unavailable { message: String },
}

impl RateLimitError {
    pub fn safe_message(&self) -> &'static str {
        match self {
            Self::InvalidSubject { .. } => "rate limit subject is invalid",
            Self::Unavailable { .. } => "rate limiter unavailable",
        }
    }
}

#[async_trait]
pub trait RateLimiterPort: Send + Sync {
    async fn check(&self, request: RateLimitRequest) -> Result<RateLimitDecision, RateLimitError>;
}

fn normalize_part(value: &str) -> String {
    value.trim().to_string()
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
