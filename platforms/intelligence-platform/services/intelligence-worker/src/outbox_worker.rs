use std::sync::Arc;
use std::time::Duration;

pub use intelligence_normalization_application::DrainSummary;
use intelligence_normalization_application::{
    FoundationNormalizationSubmitter, FoundationSubmissionError, NormalizationOutboxPort,
    NormalizationOutboxStatus, OutboxTransitionError,
};
use intelligence_normalization_infrastructure::{
    FoundationPlatformNormalizationClient, FoundationPlatformNormalizationConfig,
    PostgresWorkflowState, PostgresWorkflowStateConfig, WorkloadTokenProvider,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct DrainConfig {
    pub batch_size: usize,
    pub lease: Duration,
    pub max_attempts: u32,
    pub idle_sleep: Duration,
}

impl Default for DrainConfig {
    fn default() -> Self {
        Self {
            batch_size: 4,
            lease: Duration::from_secs(60),
            max_attempts: 8,
            idle_sleep: Duration::from_secs(2),
        }
    }
}

pub fn foundation_submitter_from_env(
) -> Result<Option<Arc<dyn FoundationNormalizationSubmitter>>, FoundationSubmissionError> {
    let lookup = |key: &str| std::env::var(key).ok();
    let Some(base_url) = lookup("FOUNDATION_PLATFORM_BASE_URL") else {
        return Ok(None);
    };
    let submission_path = lookup("FOUNDATION_PLATFORM_NORMALIZATION_PATH")
        .unwrap_or_else(|| "/internal/normalization/proposals".to_string());
    let workload_token_provider = foundation_platform_workload_token_provider_from_lookup(&lookup)?;
    if workload_token_provider.is_none() {
        return Err(FoundationSubmissionError::InvalidResponse {
            message: "FOUNDATION_PLATFORM_INTELLIGENCE_WORKLOAD_IDENTITY_TOKEN_FILE is required when the Foundation Platform base URL is set".to_string(),
        });
    }
    let timeout_seconds = lookup("FOUNDATION_PLATFORM_TIMEOUT_SECONDS")
        .and_then(|value| value.parse().ok())
        .unwrap_or(10);
    let client =
        FoundationPlatformNormalizationClient::new(FoundationPlatformNormalizationConfig {
            base_url,
            submission_path,
            workload_token_provider,
            timeout_seconds,
        })?;
    Ok(Some(Arc::new(client)))
}

pub async fn durable_outbox_from_env(
) -> Result<Arc<dyn NormalizationOutboxPort>, FoundationSubmissionError> {
    let lookup = |key: &str| std::env::var(key).ok();
    let database_url = lookup("DATABASE_URL").unwrap_or_default();
    let timeout_seconds = lookup("DATABASE_TIMEOUT_SECONDS")
        .map(|value| {
            value
                .parse()
                .map_err(|error| FoundationSubmissionError::InvalidResponse {
                    message: format!("DATABASE_TIMEOUT_SECONDS is invalid: {error}"),
                })
        })
        .transpose()?
        .unwrap_or(10);
    let mut config = PostgresWorkflowStateConfig::new(database_url, timeout_seconds).map_err(
        |error| FoundationSubmissionError::InvalidResponse {
            message: format!(
                "DATABASE_URL/DATABASE_TIMEOUT_SECONDS produced an invalid postgres workflow state config: {error}"
            ),
        },
    )?;
    if let Some(value) = lookup("DATABASE_MAX_CONNECTIONS") {
        let max_connections =
            value
                .parse()
                .map_err(|error| FoundationSubmissionError::InvalidResponse {
                    message: format!("DATABASE_MAX_CONNECTIONS is invalid: {error}"),
                })?;
        config = config.with_max_connections(max_connections).map_err(|error| {
            FoundationSubmissionError::InvalidResponse {
                message: format!(
                    "DATABASE_MAX_CONNECTIONS produced an invalid postgres workflow state config: {error}"
                ),
            }
        })?;
    }
    let outbox = PostgresWorkflowState::connect(config)
        .await
        .map_err(|error| FoundationSubmissionError::InvalidResponse {
            message: format!(
                "DATABASE_URL is set but postgres workflow state connect failed: {error}"
            ),
        })?;
    Ok(Arc::new(outbox))
}

fn foundation_platform_workload_token_provider_from_lookup(
    lookup: &impl Fn(&str) -> Option<String>,
) -> Result<Option<WorkloadTokenProvider>, FoundationSubmissionError> {
    if let Some(token_file) =
        lookup("FOUNDATION_PLATFORM_INTELLIGENCE_WORKLOAD_IDENTITY_TOKEN_FILE")
            .filter(|value| !value.trim().is_empty())
    {
        return WorkloadTokenProvider::from_file(token_file.trim()).map(Some);
    }

    Ok(None)
}

pub async fn drain_once(
    outbox: Arc<dyn NormalizationOutboxPort>,
    submitter: Arc<dyn FoundationNormalizationSubmitter>,
    config: &DrainConfig,
) -> Result<DrainSummary, OutboxTransitionError> {
    intelligence_normalization_application::drain_once(
        outbox,
        submitter,
        &intelligence_normalization_application::DrainOnceConfig {
            batch_size: config.batch_size,
            lease: config.lease,
            max_attempts: config.max_attempts,
        },
    )
    .await
}

pub async fn record_submission_failure(
    outbox: Arc<dyn NormalizationOutboxPort>,
    idempotency_key: &str,
    error: &FoundationSubmissionError,
) -> Result<NormalizationOutboxStatus, OutboxTransitionError> {
    let status = intelligence_normalization_application::record_submission_failure(
        outbox,
        idempotency_key,
        error,
    )
    .await?;
    if status == NormalizationOutboxStatus::ReconcileRequired {
        metrics::counter!("outbox_reconcile_required_total").increment(1);
    }
    Ok(status)
}

pub fn drain_config_from_lookup(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<DrainConfig, String> {
    let batch_size = parse_positive_usize(&lookup, "NORMALIZATION_OUTBOX_DRAIN_BATCH_SIZE", 4)?;
    let lease_secs = parse_positive_u64(&lookup, "NORMALIZATION_OUTBOX_DRAIN_LEASE_SECONDS", 60)?;
    let max_attempts = parse_positive_u32(&lookup, "NORMALIZATION_OUTBOX_MAX_ATTEMPTS", 8)?;
    let idle_secs = parse_positive_u64(&lookup, "NORMALIZATION_OUTBOX_DRAIN_IDLE_SECONDS", 2)?;

    Ok(DrainConfig {
        batch_size,
        lease: Duration::from_secs(lease_secs),
        max_attempts,
        idle_sleep: Duration::from_secs(idle_secs),
    })
}

pub fn foundation_submit_timeout_seconds_from_lookup(
    lookup: impl Fn(&str) -> Option<String>,
) -> u64 {
    lookup("FOUNDATION_PLATFORM_TIMEOUT_SECONDS")
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(10)
}

pub async fn run_drain_loop(
    outbox: Arc<dyn NormalizationOutboxPort>,
    submitter: Arc<dyn FoundationNormalizationSubmitter>,
    config: DrainConfig,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            result = drain_once(outbox.clone(), submitter.clone(), &config) => {
                match result {
                    Ok(summary) => {
                        emit_drain_events(&summary);
                        if summary.reconcile_required > 0 {
                            metrics::counter!("outbox_reconcile_required_total")
                                .increment(summary.reconcile_required as u64);
                        }
                        tracing::info!(
                            claimed = summary.claimed,
                            submitted = summary.submitted,
                            failed_retryable = summary.failed_retryable,
                            failed_terminal = summary.failed_terminal,
                            dead_lettered = summary.dead_lettered,
                            reconcile_required = summary.reconcile_required,
                            lease_races = summary.lease_races,
                            "drain_once completed"
                        );
                        if summary.claimed == 0 || summary.submitted == 0 {
                            tokio::select! {
                                biased;
                                _ = cancel.cancelled() => break,
                                _ = tokio::time::sleep(config.idle_sleep) => {}
                            }
                        }
                    }
                    Err(error) => {
                        tracing::error!(error = %error, "drain_once claim_next_pending failed; sleeping before retry");
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => break,
                            _ = tokio::time::sleep(config.idle_sleep) => {}
                        }
                    }
                }
            }
        }
    }
}

pub fn emit_drain_events(summary: &DrainSummary) {
    for failure in &summary.transition_failures {
        match failure.stage {
            intelligence_normalization_application::DrainTransitionStage::MarkSent => {
                tracing::error!(
                    idempotency_key = %failure.idempotency_key,
                    error = %failure.safe_diagnostic,
                    "submission delivered but mark_sent failed (R3); record may require manual reconciliation"
                )
            }
            intelligence_normalization_application::DrainTransitionStage::RecordSubmissionFailure => {
                tracing::warn!(
                    idempotency_key = %failure.idempotency_key,
                    error = %failure.safe_diagnostic,
                    "failed to record submission failure"
                )
            }
            intelligence_normalization_application::DrainTransitionStage::MarkDeadLetter => tracing::warn!(
                idempotency_key = %failure.idempotency_key,
                error = %failure.safe_diagnostic,
                "mark_dead_letter failed; skipping record"
            ),
        }
    }

    for outcome in &summary.outcome_events {
        match (&outcome.kind, &outcome.class) {
            (
                intelligence_normalization_application::DrainOutcomeKind::SubmissionFailureRecorded,
                intelligence_normalization_application::DrainTransitionClass::Retryable,
            ) => tracing::warn!(
                idempotency_key = %outcome.idempotency_key,
                error = %outcome.safe_diagnostic.as_deref().unwrap_or("foundation-platform submission failed"),
                "normalization submission failed; marked retryable"
            ),
            (
                intelligence_normalization_application::DrainOutcomeKind::SubmissionFailureRecorded,
                intelligence_normalization_application::DrainTransitionClass::Terminal,
            ) => tracing::warn!(
                idempotency_key = %outcome.idempotency_key,
                error = %outcome.safe_diagnostic.as_deref().unwrap_or("foundation-platform submission failed"),
                "normalization submission failed; marked terminal"
            ),
            (
                intelligence_normalization_application::DrainOutcomeKind::SubmissionFailureRecorded,
                intelligence_normalization_application::DrainTransitionClass::ReconcileRequired,
            ) => tracing::warn!(
                idempotency_key = %outcome.idempotency_key,
                error = %outcome.safe_diagnostic.as_deref().unwrap_or("foundation-platform submission failed"),
                "normalization submission failed; marked reconcile_required"
            ),
            (intelligence_normalization_application::DrainOutcomeKind::DeadLettered, _) => {
                tracing::error!(
                    idempotency_key = %outcome.idempotency_key,
                    attempts = ?outcome.attempts,
                    "record dead-lettered; operator action required"
                )
            }
            (
                intelligence_normalization_application::DrainOutcomeKind::SubmissionFailureRecorded,
                _,
            ) => {}
        }
    }
}

fn parse_positive_usize(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &str,
    default: usize,
) -> Result<usize, String> {
    let Some(raw) = lookup(key) else {
        return Ok(default);
    };
    let value = raw
        .trim()
        .parse()
        .map_err(|error| format!("{key} is invalid: {error}"))?;
    if value == 0 {
        return Err(format!("{key} must be greater than zero"));
    }
    Ok(value)
}

fn parse_positive_u64(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &str,
    default: u64,
) -> Result<u64, String> {
    let Some(raw) = lookup(key) else {
        return Ok(default);
    };
    let value = raw
        .trim()
        .parse()
        .map_err(|error| format!("{key} is invalid: {error}"))?;
    if value == 0 {
        return Err(format!("{key} must be greater than zero"));
    }
    Ok(value)
}

fn parse_positive_u32(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &str,
    default: u32,
) -> Result<u32, String> {
    let Some(raw) = lookup(key) else {
        return Ok(default);
    };
    let value = raw
        .trim()
        .parse()
        .map_err(|error| format!("{key} is invalid: {error}"))?;
    if value == 0 {
        return Err(format!("{key} must be greater than zero"));
    }
    Ok(value)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        drain_config_from_lookup, foundation_platform_workload_token_provider_from_lookup,
        foundation_submit_timeout_seconds_from_lookup, DrainConfig,
    };

    #[test]
    fn drain_config_defaults_when_no_env_vars() {
        let config = drain_config_from_lookup(|_| None).unwrap();
        assert_eq!(config.batch_size, DrainConfig::default().batch_size);
        assert_eq!(config.lease, DrainConfig::default().lease);
        assert_eq!(config.max_attempts, DrainConfig::default().max_attempts);
        assert_eq!(config.idle_sleep, DrainConfig::default().idle_sleep);
    }

    #[test]
    fn drain_config_zero_batch_size_errors_with_var_name() {
        let values = BTreeMap::from([("NORMALIZATION_OUTBOX_DRAIN_BATCH_SIZE", "0")]);
        let err = drain_config_from_lookup(|key| values.get(key).map(|value| value.to_string()))
            .unwrap_err();
        assert!(
            err.contains("NORMALIZATION_OUTBOX_DRAIN_BATCH_SIZE must be greater than zero"),
            "error must name the env var; got: {err}"
        );
    }

    #[test]
    fn drain_config_invalid_batch_size_errors_with_var_name() {
        let values = BTreeMap::from([("NORMALIZATION_OUTBOX_DRAIN_BATCH_SIZE", "not-a-number")]);
        let err = drain_config_from_lookup(|key| values.get(key).map(|value| value.to_string()))
            .unwrap_err();
        assert!(
            err.contains("NORMALIZATION_OUTBOX_DRAIN_BATCH_SIZE is invalid"),
            "error must name the env var; got: {err}"
        );
    }

    #[test]
    fn drain_config_explicit_values_are_stored() {
        let values = BTreeMap::from([
            ("NORMALIZATION_OUTBOX_DRAIN_BATCH_SIZE", "16"),
            ("NORMALIZATION_OUTBOX_DRAIN_LEASE_SECONDS", "120"),
            ("NORMALIZATION_OUTBOX_MAX_ATTEMPTS", "5"),
            ("NORMALIZATION_OUTBOX_DRAIN_IDLE_SECONDS", "10"),
        ]);
        let config =
            drain_config_from_lookup(|key| values.get(key).map(|value| value.to_string())).unwrap();
        assert_eq!(config.batch_size, 16);
        assert_eq!(config.lease.as_secs(), 120);
        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.idle_sleep.as_secs(), 10);
    }

    #[test]
    fn foundation_submit_timeout_uses_final_foundation_platform_env_name() {
        let values = BTreeMap::from([("FOUNDATION_PLATFORM_TIMEOUT_SECONDS", "7")]);
        assert_eq!(
            foundation_submit_timeout_seconds_from_lookup(|key| values
                .get(key)
                .map(|value| value.to_string())),
            7
        );
    }

    #[test]
    fn foundation_workload_provider_validates_deployed_token_file() {
        let token_path = std::env::temp_dir().join(format!(
            "intelligence-worker-token-{}.txt",
            std::process::id()
        ));
        std::fs::write(&token_path, "zitadel-worker-token\n").unwrap();
        let values = BTreeMap::from([(
            "FOUNDATION_PLATFORM_INTELLIGENCE_WORKLOAD_IDENTITY_TOKEN_FILE",
            token_path.to_str().unwrap(),
        )]);

        let provider = foundation_platform_workload_token_provider_from_lookup(&|key| {
            values.get(key).map(|value| value.to_string())
        })
        .unwrap();

        std::fs::remove_file(token_path).unwrap();
        assert!(provider.is_some());
    }

    #[test]
    fn foundation_workload_provider_rejects_static_service_tokens() {
        let values = BTreeMap::from([(
            "FOUNDATION_PLATFORM_INTELLIGENCE_SERVICE_TOKEN",
            "foundation-static-token",
        )]);

        let provider = foundation_platform_workload_token_provider_from_lookup(&|key| {
            values.get(key).map(|value| value.to_string())
        })
        .unwrap();

        assert_eq!(provider, None);
    }
}
