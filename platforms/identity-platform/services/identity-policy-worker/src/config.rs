//! Environment configuration for the bounded worker process.

use std::env;
use std::time::Duration;

use thiserror::Error;

use crate::http_publisher::{PublisherEndpoint, PublisherEndpointError};
use crate::worker::WorkerOptions;

const DEFAULT_POLL_INTERVAL_MS: u64 = 1_000;
const MIN_POLL_INTERVAL_MS: u64 = 10;
const DEFAULT_BATCH_SIZE: usize = 50;
const MIN_BATCH_SIZE: usize = 1;
const MAX_BATCH_SIZE: usize = 1_000;
const DEFAULT_LEASE_SECONDS: u64 = 300;
const DEFAULT_TIMEOUT_MS: u64 = 5_000;
const MIN_PUBLISH_TIMEOUT_MS: u64 = 100;
const DEFAULT_REPOSITORY_TIMEOUT_MS: u64 = 5_000;
const MIN_REPOSITORY_TIMEOUT_MS: u64 = 100;
const MAX_REPOSITORY_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_BASE_BACKOFF_SECONDS: u64 = 2;
const MIN_RETRY_BASE_SECONDS: u64 = 1;
const DEFAULT_MAX_BACKOFF_SECONDS: u64 = 300;
const MAX_POLL_INTERVAL_MS: u64 = 60_000;
const MAX_PUBLISH_TIMEOUT_MS: u64 = 120_000;
const MAX_LEASE_SECONDS: u64 = 900;
const MIN_LEASE_SECONDS: u64 = 1;
const MAX_RETRY_BASE_SECONDS: u64 = 3_600;
const MAX_RETRY_SECONDS: u64 = MAX_RETRY_BASE_SECONDS;
/// Time reserved after the worst-case single-row HTTP window for persistence.
const PERSISTENCE_MARGIN: Duration = Duration::from_secs(1);
/// Transaction begin, claim query, claim commit, and terminal update each consume lease time.
const POST_LEASE_REPOSITORY_WINDOWS: u32 = 4;

/// Validated production configuration loaded by the worker binary.
pub struct WorkerConfig {
    /// Identity database connection URL. This value must never be logged.
    pub database_url: String,
    /// Exact receiver endpoint.
    pub endpoint: PublisherEndpoint,
    /// Delay between bounded claim ticks.
    pub poll_interval: Duration,
    /// Per-request network timeout.
    pub publish_timeout: Duration,
    /// Per-operation `PostgreSQL` timeout.
    pub repository_timeout: Duration,
    /// Delivery and lease options.
    pub worker_options: WorkerOptions,
}

impl WorkerConfig {
    /// Loads and validates worker environment variables.
    ///
    /// # Errors
    /// Returns a bounded error naming only the invalid setting.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    fn from_lookup<F>(lookup: F) -> Result<Self, ConfigError>
    where
        F: Fn(&'static str) -> Option<String>,
    {
        let database_url = required(&lookup, "IDENTITY_DATABASE_URL")?;
        let endpoint =
            PublisherEndpoint::parse(&required(&lookup, "IDENTITY_POLICY_EVENT_ENDPOINT")?)?;
        let worker_id = required(&lookup, "IDENTITY_POLICY_WORKER_ID")?;
        if worker_id.len() > 128
            || !worker_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(ConfigError::Invalid("IDENTITY_POLICY_WORKER_ID"));
        }

        let numeric = NumericSettings {
            batch_size: optional_number(&lookup, "IDENTITY_POLICY_BATCH_SIZE", DEFAULT_BATCH_SIZE)?,
            poll_interval_ms: optional_number(
                &lookup,
                "IDENTITY_POLICY_POLL_INTERVAL_MS",
                DEFAULT_POLL_INTERVAL_MS,
            )?,
            publish_timeout_ms: optional_number(
                &lookup,
                "IDENTITY_POLICY_PUBLISH_TIMEOUT_MS",
                DEFAULT_TIMEOUT_MS,
            )?,
            repository_timeout_ms: optional_number(
                &lookup,
                "IDENTITY_POLICY_REPOSITORY_TIMEOUT_MS",
                DEFAULT_REPOSITORY_TIMEOUT_MS,
            )?,
            lease_seconds: optional_number(
                &lookup,
                "IDENTITY_POLICY_LEASE_SECONDS",
                DEFAULT_LEASE_SECONDS,
            )?,
            base_backoff_seconds: optional_number(
                &lookup,
                "IDENTITY_POLICY_RETRY_BASE_SECONDS",
                DEFAULT_BASE_BACKOFF_SECONDS,
            )?,
            max_backoff_seconds: optional_number(
                &lookup,
                "IDENTITY_POLICY_RETRY_MAX_SECONDS",
                DEFAULT_MAX_BACKOFF_SECONDS,
            )?,
        }
        .validate()?;

        Ok(Self {
            database_url,
            endpoint,
            poll_interval: Duration::from_millis(numeric.poll_interval_ms),
            publish_timeout: Duration::from_millis(numeric.publish_timeout_ms),
            repository_timeout: Duration::from_millis(numeric.repository_timeout_ms),
            worker_options: WorkerOptions {
                worker_id,
                batch_size: numeric.batch_size,
                lease_duration: Duration::from_secs(numeric.lease_seconds),
                base_backoff: Duration::from_secs(numeric.base_backoff_seconds),
                max_backoff: Duration::from_secs(numeric.max_backoff_seconds),
            },
        })
    }
}

/// Bounded worker configuration failures.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// A required setting is absent or blank.
    #[error("required setting is missing: {0}")]
    Missing(&'static str),
    /// A setting is outside its accepted shape or range.
    #[error("setting is invalid: {0}")]
    Invalid(&'static str),
    /// The receiver endpoint is invalid.
    #[error(transparent)]
    Endpoint(#[from] PublisherEndpointError),
}

fn required<F>(lookup: &F, name: &'static str) -> Result<String, ConfigError>
where
    F: Fn(&'static str) -> Option<String>,
{
    let value = lookup(name).ok_or(ConfigError::Missing(name))?;
    if value.trim().is_empty() {
        return Err(ConfigError::Missing(name));
    }
    Ok(value)
}

fn optional_number<F, T>(lookup: &F, name: &'static str, default: T) -> Result<T, ConfigError>
where
    F: Fn(&'static str) -> Option<String>,
    T: std::str::FromStr,
{
    lookup(name).map_or(Ok(default), |value| {
        value.parse().map_err(|_| ConfigError::Invalid(name))
    })
}

#[derive(Clone, Copy)]
struct NumericSettings {
    batch_size: usize,
    poll_interval_ms: u64,
    publish_timeout_ms: u64,
    repository_timeout_ms: u64,
    lease_seconds: u64,
    base_backoff_seconds: u64,
    max_backoff_seconds: u64,
}

impl NumericSettings {
    fn validate(self) -> Result<Self, ConfigError> {
        if !(MIN_BATCH_SIZE..=MAX_BATCH_SIZE).contains(&self.batch_size) {
            return Err(ConfigError::Invalid("IDENTITY_POLICY_BATCH_SIZE"));
        }
        if !(MIN_POLL_INTERVAL_MS..=MAX_POLL_INTERVAL_MS).contains(&self.poll_interval_ms) {
            return Err(ConfigError::Invalid("IDENTITY_POLICY_POLL_INTERVAL_MS"));
        }
        if !(MIN_PUBLISH_TIMEOUT_MS..=MAX_PUBLISH_TIMEOUT_MS).contains(&self.publish_timeout_ms) {
            return Err(ConfigError::Invalid("IDENTITY_POLICY_PUBLISH_TIMEOUT_MS"));
        }
        if !(MIN_REPOSITORY_TIMEOUT_MS..=MAX_REPOSITORY_TIMEOUT_MS)
            .contains(&self.repository_timeout_ms)
        {
            return Err(ConfigError::Invalid(
                "IDENTITY_POLICY_REPOSITORY_TIMEOUT_MS",
            ));
        }
        if !(MIN_LEASE_SECONDS..=MAX_LEASE_SECONDS).contains(&self.lease_seconds) {
            return Err(ConfigError::Invalid("IDENTITY_POLICY_LEASE_SECONDS"));
        }
        let required_lease = Duration::from_millis(self.repository_timeout_ms)
            .checked_mul(POST_LEASE_REPOSITORY_WINDOWS)
            .and_then(|database_window| {
                Duration::from_millis(self.publish_timeout_ms).checked_add(database_window)
            })
            .and_then(|window| window.checked_add(PERSISTENCE_MARGIN))
            .ok_or(ConfigError::Invalid("IDENTITY_POLICY_LEASE_SECONDS"))?;
        if Duration::from_secs(self.lease_seconds) < required_lease {
            return Err(ConfigError::Invalid("IDENTITY_POLICY_LEASE_SECONDS"));
        }
        if !(MIN_RETRY_BASE_SECONDS..=MAX_RETRY_SECONDS).contains(&self.base_backoff_seconds) {
            return Err(ConfigError::Invalid("IDENTITY_POLICY_RETRY_BASE_SECONDS"));
        }
        if !(self.base_backoff_seconds..=MAX_RETRY_SECONDS).contains(&self.max_backoff_seconds) {
            return Err(ConfigError::Invalid("IDENTITY_POLICY_RETRY_MAX_SECONDS"));
        }
        Ok(self)
    }
}

#[cfg(test)]
mod numeric_tests {
    use std::time::Duration;

    use super::{
        ConfigError, NumericSettings, DEFAULT_BASE_BACKOFF_SECONDS, DEFAULT_BATCH_SIZE,
        DEFAULT_LEASE_SECONDS, DEFAULT_MAX_BACKOFF_SECONDS, DEFAULT_POLL_INTERVAL_MS,
        DEFAULT_REPOSITORY_TIMEOUT_MS, DEFAULT_TIMEOUT_MS, MAX_REPOSITORY_TIMEOUT_MS,
        MIN_REPOSITORY_TIMEOUT_MS, PERSISTENCE_MARGIN, POST_LEASE_REPOSITORY_WINDOWS,
    };

    const VALID: NumericSettings = NumericSettings {
        batch_size: 1,
        poll_interval_ms: 1_000,
        publish_timeout_ms: 100,
        repository_timeout_ms: 100,
        lease_seconds: 2,
        base_backoff_seconds: 2,
        max_backoff_seconds: 300,
    };

    #[test]
    fn batch_size_accepts_only_one_through_one_thousand() {
        assert!(NumericSettings {
            batch_size: 1,
            ..VALID
        }
        .validate()
        .is_ok());
        assert!(NumericSettings {
            batch_size: 1_000,
            lease_seconds: 101,
            ..VALID
        }
        .validate()
        .is_ok());
        assert_invalid(
            &NumericSettings {
                batch_size: 0,
                ..VALID
            }
            .validate(),
            "IDENTITY_POLICY_BATCH_SIZE",
        );
        assert_invalid(
            &NumericSettings {
                batch_size: 1_001,
                ..VALID
            }
            .validate(),
            "IDENTITY_POLICY_BATCH_SIZE",
        );
    }

    #[test]
    fn poll_interval_accepts_only_ten_milliseconds_through_sixty_seconds() {
        assert!(NumericSettings {
            poll_interval_ms: 10,
            ..VALID
        }
        .validate()
        .is_ok());
        assert!(NumericSettings {
            poll_interval_ms: 60_000,
            ..VALID
        }
        .validate()
        .is_ok());
        assert_invalid(
            &NumericSettings {
                poll_interval_ms: 9,
                ..VALID
            }
            .validate(),
            "IDENTITY_POLICY_POLL_INTERVAL_MS",
        );
        assert_invalid(
            &NumericSettings {
                poll_interval_ms: 60_001,
                ..VALID
            }
            .validate(),
            "IDENTITY_POLICY_POLL_INTERVAL_MS",
        );
    }

    #[test]
    fn publish_timeout_accepts_only_one_hundred_milliseconds_through_two_minutes() {
        assert!(NumericSettings {
            publish_timeout_ms: 100,
            ..VALID
        }
        .validate()
        .is_ok());
        assert!(NumericSettings {
            publish_timeout_ms: 120_000,
            lease_seconds: 122,
            ..VALID
        }
        .validate()
        .is_ok());
        assert_invalid(
            &NumericSettings {
                publish_timeout_ms: 99,
                ..VALID
            }
            .validate(),
            "IDENTITY_POLICY_PUBLISH_TIMEOUT_MS",
        );
        assert_invalid(
            &NumericSettings {
                publish_timeout_ms: 120_001,
                ..VALID
            }
            .validate(),
            "IDENTITY_POLICY_PUBLISH_TIMEOUT_MS",
        );
    }

    #[test]
    fn repository_timeout_accepts_only_one_hundred_milliseconds_through_thirty_seconds() {
        assert!(NumericSettings {
            repository_timeout_ms: MIN_REPOSITORY_TIMEOUT_MS,
            ..VALID
        }
        .validate()
        .is_ok());
        assert!(NumericSettings {
            repository_timeout_ms: MAX_REPOSITORY_TIMEOUT_MS,
            lease_seconds: 122,
            ..VALID
        }
        .validate()
        .is_ok());
        assert_invalid(
            &NumericSettings {
                repository_timeout_ms: MIN_REPOSITORY_TIMEOUT_MS - 1,
                ..VALID
            }
            .validate(),
            "IDENTITY_POLICY_REPOSITORY_TIMEOUT_MS",
        );
        assert_invalid(
            &NumericSettings {
                repository_timeout_ms: MAX_REPOSITORY_TIMEOUT_MS + 1,
                lease_seconds: 122,
                ..VALID
            }
            .validate(),
            "IDENTITY_POLICY_REPOSITORY_TIMEOUT_MS",
        );
    }

    #[test]
    fn lease_duration_rejects_values_outside_absolute_bounds() {
        assert!(NumericSettings {
            lease_seconds: 2,
            ..VALID
        }
        .validate()
        .is_ok());
        assert!(NumericSettings {
            lease_seconds: 900,
            ..VALID
        }
        .validate()
        .is_ok());
        assert_invalid(
            &NumericSettings {
                lease_seconds: 0,
                ..VALID
            }
            .validate(),
            "IDENTITY_POLICY_LEASE_SECONDS",
        );
        assert_invalid(
            &NumericSettings {
                lease_seconds: 901,
                ..VALID
            }
            .validate(),
            "IDENTITY_POLICY_LEASE_SECONDS",
        );
    }

    #[test]
    fn retry_base_accepts_only_one_second_through_one_hour() {
        assert!(NumericSettings {
            base_backoff_seconds: 1,
            ..VALID
        }
        .validate()
        .is_ok());
        assert!(NumericSettings {
            base_backoff_seconds: 3_600,
            max_backoff_seconds: 3_600,
            ..VALID
        }
        .validate()
        .is_ok());
        assert_invalid(
            &NumericSettings {
                base_backoff_seconds: 0,
                ..VALID
            }
            .validate(),
            "IDENTITY_POLICY_RETRY_BASE_SECONDS",
        );
        assert_invalid(
            &NumericSettings {
                base_backoff_seconds: 3_601,
                max_backoff_seconds: 3_601,
                ..VALID
            }
            .validate(),
            "IDENTITY_POLICY_RETRY_BASE_SECONDS",
        );
    }

    #[test]
    fn retry_max_is_bounded_and_not_less_than_retry_base() {
        assert!(NumericSettings {
            max_backoff_seconds: 1,
            base_backoff_seconds: 1,
            ..VALID
        }
        .validate()
        .is_ok());
        assert!(NumericSettings {
            max_backoff_seconds: 3_600,
            ..VALID
        }
        .validate()
        .is_ok());
        assert_invalid(
            &NumericSettings {
                max_backoff_seconds: 0,
                ..VALID
            }
            .validate(),
            "IDENTITY_POLICY_RETRY_MAX_SECONDS",
        );
        assert_invalid(
            &NumericSettings {
                max_backoff_seconds: 3_601,
                ..VALID
            }
            .validate(),
            "IDENTITY_POLICY_RETRY_MAX_SECONDS",
        );
        assert_invalid(
            &NumericSettings {
                max_backoff_seconds: 1,
                ..VALID
            }
            .validate(),
            "IDENTITY_POLICY_RETRY_MAX_SECONDS",
        );
    }

    #[test]
    fn batch_size_does_not_increase_the_single_row_lease_requirement() {
        assert!(NumericSettings {
            batch_size: 1,
            ..VALID
        }
        .validate()
        .is_ok());
        assert!(NumericSettings {
            batch_size: 1_000,
            ..VALID
        }
        .validate()
        .is_ok());
    }

    #[test]
    fn defaults_are_accepted_and_cover_the_single_row_delivery_window() {
        let result = NumericSettings {
            batch_size: DEFAULT_BATCH_SIZE,
            poll_interval_ms: DEFAULT_POLL_INTERVAL_MS,
            publish_timeout_ms: DEFAULT_TIMEOUT_MS,
            repository_timeout_ms: DEFAULT_REPOSITORY_TIMEOUT_MS,
            lease_seconds: DEFAULT_LEASE_SECONDS,
            base_backoff_seconds: DEFAULT_BASE_BACKOFF_SECONDS,
            max_backoff_seconds: DEFAULT_MAX_BACKOFF_SECONDS,
        }
        .validate();
        assert!(result.is_ok(), "default settings must be valid");
        let Ok(settings) = result else {
            return;
        };

        assert!(lease_covers_single_row(settings));
    }

    #[test]
    fn lease_window_relation_enforces_lower_and_upper_boundaries() {
        assert!(NumericSettings {
            batch_size: 1,
            publish_timeout_ms: 100,
            repository_timeout_ms: 100,
            lease_seconds: 2,
            ..VALID
        }
        .validate()
        .is_ok());
        assert_invalid(
            &NumericSettings {
                batch_size: 1,
                publish_timeout_ms: 100,
                repository_timeout_ms: 100,
                lease_seconds: 1,
                ..VALID
            }
            .validate(),
            "IDENTITY_POLICY_LEASE_SECONDS",
        );

        let result = NumericSettings {
            publish_timeout_ms: 120_000,
            repository_timeout_ms: 30_000,
            lease_seconds: 241,
            ..VALID
        }
        .validate();
        assert!(
            result.is_ok(),
            "slow configuration must fit within the maximum lease"
        );
        let Ok(slow) = result else {
            return;
        };
        assert!(lease_covers_single_row(slow));
        assert_invalid(
            &NumericSettings {
                publish_timeout_ms: 120_000,
                repository_timeout_ms: 30_000,
                lease_seconds: 240,
                ..VALID
            }
            .validate(),
            "IDENTITY_POLICY_LEASE_SECONDS",
        );
    }

    fn lease_covers_single_row(settings: NumericSettings) -> bool {
        let publish_timeout = Duration::from_millis(settings.publish_timeout_ms);
        let repository_timeout = Duration::from_millis(settings.repository_timeout_ms);
        let lease = Duration::from_secs(settings.lease_seconds);
        repository_timeout
            .checked_mul(POST_LEASE_REPOSITORY_WINDOWS)
            .and_then(|database_window| publish_timeout.checked_add(database_window))
            .and_then(|window| window.checked_add(PERSISTENCE_MARGIN))
            .is_some_and(|required| required <= lease)
    }

    fn assert_invalid(result: &Result<NumericSettings, ConfigError>, expected_name: &'static str) {
        assert!(matches!(result, Err(ConfigError::Invalid(name)) if *name == expected_name));
    }
}
