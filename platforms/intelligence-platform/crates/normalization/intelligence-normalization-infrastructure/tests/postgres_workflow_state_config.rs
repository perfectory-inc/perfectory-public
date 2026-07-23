//! Unit tests for [`PostgresWorkflowStateConfig`] validation.
//!
//! These tests require no live database.

// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use intelligence_normalization_infrastructure::{
    PostgresWorkflowStateConfig, PostgresWorkflowStateError,
};

#[test]
fn empty_url_returns_invalid_config() {
    let err = PostgresWorkflowStateConfig::new("", 10).unwrap_err();
    assert!(
        matches!(err, PostgresWorkflowStateError::InvalidConfig),
        "empty URL must produce InvalidConfig; got {err:?}"
    );
    assert_eq!(
        err.safe_message(),
        "postgres workflow state config is invalid"
    );
}

#[test]
fn whitespace_url_returns_invalid_config() {
    let err = PostgresWorkflowStateConfig::new("   ", 10).unwrap_err();
    assert!(
        matches!(err, PostgresWorkflowStateError::InvalidConfig),
        "whitespace-only URL must produce InvalidConfig; got {err:?}"
    );
    assert_eq!(
        err.safe_message(),
        "postgres workflow state config is invalid"
    );
}

#[test]
fn zero_timeout_returns_invalid_config() {
    let err = PostgresWorkflowStateConfig::new("postgres://localhost/test", 0).unwrap_err();
    assert!(
        matches!(err, PostgresWorkflowStateError::InvalidConfig),
        "zero timeout must produce InvalidConfig; got {err:?}"
    );
    assert_eq!(
        err.safe_message(),
        "postgres workflow state config is invalid"
    );
}

#[test]
fn valid_config_succeeds() {
    let config = PostgresWorkflowStateConfig::new("postgres://localhost/test", 10)
        .expect("valid config must succeed");
    assert_eq!(config.database_url(), "postgres://localhost/test");
    assert_eq!(config.timeout_seconds(), 10);
}

#[test]
fn store_failed_has_expected_safe_message() {
    let err = PostgresWorkflowStateError::StoreFailed {
        message: "internal detail".to_string(),
    };
    assert_eq!(err.safe_message(), "postgres workflow state failed");
    // Display must include the internal detail (for operator logs, not API responses).
    assert!(err.to_string().contains("internal detail"));
}

#[test]
fn with_max_connections_zero_returns_invalid_config() {
    let err = PostgresWorkflowStateConfig::new("postgres://localhost/test", 10)
        .expect("valid config")
        .with_max_connections(0)
        .unwrap_err();
    assert!(
        matches!(err, PostgresWorkflowStateError::InvalidConfig),
        "zero max_connections must produce InvalidConfig; got {err:?}"
    );
    assert_eq!(
        err.safe_message(),
        "postgres workflow state config is invalid"
    );
}

#[test]
fn with_max_connections_nonzero_succeeds() {
    let config = PostgresWorkflowStateConfig::new("postgres://localhost/test", 10)
        .expect("valid config")
        .with_max_connections(25)
        .expect("nonzero max_connections must succeed");
    assert_eq!(config.max_connections(), 25);
}

#[test]
fn default_max_connections_is_ten() {
    let config =
        PostgresWorkflowStateConfig::new("postgres://localhost/test", 10).expect("valid config");
    assert_eq!(config.max_connections(), 10);
}
