//! HTTP traffic budget and overload protection.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// HTTP traffic budget for a small-box foundation-platform deployment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TrafficConfig {
    /// Maximum request processing time before the HTTP layer returns timeout.
    pub request_timeout_ms: u64,
    /// Maximum number of in-flight requests accepted by the API process.
    pub max_concurrency: usize,
    /// Maximum request body accepted by Axum.
    pub body_limit_bytes: usize,
}

impl Default for TrafficConfig {
    fn default() -> Self {
        Self {
            request_timeout_ms: 3000,
            max_concurrency: 128,
            body_limit_bytes: 1_048_576,
        }
    }
}

impl TrafficConfig {
    /// Reads the traffic budget from process environment variables.
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_vars(|key| std::env::var(key).ok())
    }

    fn from_vars(lookup: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        Ok(Self {
            request_timeout_ms: optional_positive_u64_var(
                &lookup,
                "FOUNDATION_PLATFORM_HTTP_REQUEST_TIMEOUT_MS",
                Self::default().request_timeout_ms,
            )?,
            max_concurrency: optional_positive_usize_var(
                &lookup,
                "FOUNDATION_PLATFORM_HTTP_MAX_CONCURRENCY",
                Self::default().max_concurrency,
            )?,
            body_limit_bytes: optional_positive_usize_var(
                &lookup,
                "FOUNDATION_PLATFORM_HTTP_BODY_LIMIT_BYTES",
                Self::default().body_limit_bytes,
            )?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct TrafficRuntime {
    concurrency: Arc<Semaphore>,
}

impl TrafficRuntime {
    pub fn new(config: TrafficConfig) -> Self {
        Self {
            concurrency: Arc::new(Semaphore::new(config.max_concurrency)),
        }
    }

    pub fn try_acquire_concurrency(&self) -> Option<OwnedSemaphorePermit> {
        self.concurrency.clone().try_acquire_owned().ok()
    }
}

fn optional_positive_u64_var(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &'static str,
    default: u64,
) -> anyhow::Result<u64> {
    let Some(raw) = lookup(key) else {
        return Ok(default);
    };
    let value = raw.trim();
    if value.is_empty() {
        return Err(anyhow::anyhow!("{key} must not be empty"));
    }
    let parsed = value
        .parse::<u64>()
        .map_err(|_| anyhow::anyhow!("{key} must be a positive integer"))?;
    if parsed == 0 {
        return Err(anyhow::anyhow!("{key} must be positive"));
    }
    Ok(parsed)
}

fn optional_positive_usize_var(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &'static str,
    default: usize,
) -> anyhow::Result<usize> {
    let Some(raw) = lookup(key) else {
        return Ok(default);
    };
    let value = raw.trim();
    if value.is_empty() {
        return Err(anyhow::anyhow!("{key} must not be empty"));
    }
    let parsed = value
        .parse::<usize>()
        .map_err(|_| anyhow::anyhow!("{key} must be a positive integer"))?;
    if parsed == 0 {
        return Err(anyhow::anyhow!("{key} must be positive"));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::TrafficConfig;

    fn vars(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn traffic_config_defaults_are_small_box_safe() {
        let config = TrafficConfig::default();

        assert_eq!(config.request_timeout_ms, 3000);
        assert_eq!(config.max_concurrency, 128);
        assert_eq!(config.body_limit_bytes, 1_048_576);
    }

    #[test]
    fn traffic_config_parses_environment_overrides() -> anyhow::Result<()> {
        let vars = vars(&[
            ("FOUNDATION_PLATFORM_HTTP_REQUEST_TIMEOUT_MS", "4500"),
            ("FOUNDATION_PLATFORM_HTTP_MAX_CONCURRENCY", "64"),
            ("FOUNDATION_PLATFORM_HTTP_BODY_LIMIT_BYTES", "65536"),
        ]);

        let config = TrafficConfig::from_vars(|key| vars.get(key).cloned())?;

        assert_eq!(config.request_timeout_ms, 4500);
        assert_eq!(config.max_concurrency, 64);
        assert_eq!(config.body_limit_bytes, 65_536);
        Ok(())
    }

    #[test]
    fn traffic_config_rejects_zero_budgets() -> anyhow::Result<()> {
        let vars = vars(&[("FOUNDATION_PLATFORM_HTTP_MAX_CONCURRENCY", "0")]);

        let Err(error) = TrafficConfig::from_vars(|key| vars.get(key).cloned()) else {
            return Err(anyhow::anyhow!("zero concurrency should be rejected"));
        };

        assert!(error
            .to_string()
            .contains("FOUNDATION_PLATFORM_HTTP_MAX_CONCURRENCY"));
        Ok(())
    }
}
