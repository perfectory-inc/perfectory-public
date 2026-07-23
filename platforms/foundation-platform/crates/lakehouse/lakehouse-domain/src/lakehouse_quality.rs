//! Provider-neutral Lakehouse quality-rule contract and evaluator.

use serde::Deserialize;
use serde_json::Value as JsonValue;
use thiserror::Error;

use crate::{SparkRunSummary, SPARK_RUN_SUMMARY_SCHEMA_VERSION};

/// Current Lakehouse quality-rules schema version.
pub const LAKEHOUSE_QUALITY_RULES_SCHEMA_VERSION: &str =
    "foundation-platform.lakehouse_quality_rules.v1";

/// A versioned document containing table-scoped Lakehouse quality rules.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct LakehouseQualityRules {
    schema_version: String,
    measurement_source: String,
    rule_sets: Vec<QualityRuleSet>,
}

impl LakehouseQualityRules {
    /// Parses a quality-rules document from JSON text.
    ///
    /// # Errors
    /// Returns `LakehouseQualityError` when the JSON shape is invalid.
    pub fn from_json_str(raw: &str) -> Result<Self, LakehouseQualityError> {
        serde_json::from_str(raw).map_err(|error| {
            LakehouseQualityError::new(format!("invalid Lakehouse quality rules JSON: {error}"))
        })
    }

    /// Parses a quality-rules document from a JSON value.
    ///
    /// # Errors
    /// Returns `LakehouseQualityError` when the JSON shape is invalid.
    pub fn from_json_value(value: JsonValue) -> Result<Self, LakehouseQualityError> {
        serde_json::from_value(value).map_err(|error| {
            LakehouseQualityError::new(format!("invalid Lakehouse quality rules JSON: {error}"))
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
struct QualityRuleSet {
    table: String,
    rules: Vec<QualityRule>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
struct QualityRule {
    id: String,
    metric: String,
    severity: String,
    threshold: JsonValue,
}

/// Provider-neutral result of evaluating one table's quality rules.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LakehouseQualityEvaluation {
    /// Table contract evaluated by the rule set.
    pub table: String,
    /// Number of configured rules, including non-blocking rules.
    pub evaluated_rule_count: usize,
    /// Stable human-readable blocking violation messages.
    pub violations: Vec<String>,
}

impl LakehouseQualityEvaluation {
    /// Returns true when at least one blocking quality rule failed.
    #[must_use]
    pub const fn is_blocked(&self) -> bool {
        !self.violations.is_empty()
    }
}

/// Error raised when a quality document or rule cannot be evaluated.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{message}")]
pub struct LakehouseQualityError {
    message: String,
}

impl LakehouseQualityError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Evaluates the blocking quality rules for a typed Spark run summary.
///
/// # Errors
/// Returns `LakehouseQualityError` when document versions, table selection,
/// metrics, or threshold definitions are invalid.
pub fn evaluate_lakehouse_quality_rules(
    summary: &SparkRunSummary,
    rules: &LakehouseQualityRules,
) -> Result<LakehouseQualityEvaluation, LakehouseQualityError> {
    validate_document_headers(summary, rules)?;
    let contract = summary.contract.as_str();
    let rule_set = rules
        .rule_sets
        .iter()
        .find(|rule_set| rule_set.table == contract)
        .ok_or_else(|| {
            LakehouseQualityError::new(format!(
                "No lakehouse quality rule set for table: {contract}"
            ))
        })?;

    let mut violations = Vec::new();
    for rule in &rule_set.rules {
        if rule.severity != "blocking" {
            continue;
        }
        let metric = rule.metric.as_str();
        let Some(actual) = metric_value(summary, metric) else {
            violations.push(format!(
                "lakehouse quality metric missing: rule={} metric={metric}",
                rule.id
            ));
            continue;
        };
        if threshold_passes(summary, rule, actual)? {
            continue;
        }
        let threshold = serde_json::to_string(&rule.threshold).map_err(|error| {
            LakehouseQualityError::new(format!("failed to serialize threshold: {error}"))
        })?;
        violations.push(format!(
            "lakehouse quality rule failed: rule={} metric={metric} actual={} threshold={threshold}",
            rule.id,
            display_number(actual)
        ));
    }

    Ok(LakehouseQualityEvaluation {
        table: contract.to_owned(),
        evaluated_rule_count: rule_set.rules.len(),
        violations,
    })
}

fn validate_document_headers(
    summary: &SparkRunSummary,
    rules: &LakehouseQualityRules,
) -> Result<(), LakehouseQualityError> {
    if summary.schema_version != SPARK_RUN_SUMMARY_SCHEMA_VERSION {
        return Err(LakehouseQualityError::new(format!(
            "Unexpected Spark run summary schema_version: {}",
            summary.schema_version
        )));
    }
    if rules.schema_version != LAKEHOUSE_QUALITY_RULES_SCHEMA_VERSION {
        return Err(LakehouseQualityError::new(format!(
            "Unexpected quality rules schema_version: {}",
            rules.schema_version
        )));
    }
    if rules.measurement_source != SPARK_RUN_SUMMARY_SCHEMA_VERSION {
        return Err(LakehouseQualityError::new(format!(
            "Unexpected quality rules measurement_source: {}",
            rules.measurement_source
        )));
    }
    Ok(())
}

fn metric_value(summary: &SparkRunSummary, metric: &str) -> Option<f64> {
    match metric {
        "row_count" => count_as_f64(summary.row_count),
        "persisted_row_count" => summary.persisted_row_count.and_then(count_as_f64),
        _ => summary
            .quality_metrics
            .get(metric)
            .copied()
            .and_then(count_as_f64),
    }
}

fn count_as_f64(value: u64) -> Option<f64> {
    serde_json::Number::from(value).as_f64()
}

fn threshold_passes(
    summary: &SparkRunSummary,
    rule: &QualityRule,
    actual: f64,
) -> Result<bool, LakehouseQualityError> {
    let kind = rule
        .threshold
        .get("kind")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    match kind {
        "equals" => Ok(float_eq(actual, threshold_value(rule)?)),
        "min" => Ok(actual >= threshold_value(rule)?),
        "matches_row_count" => Ok(
            count_as_f64(summary.row_count).is_some_and(|row_count| float_eq(actual, row_count))
        ),
        _ => Err(LakehouseQualityError::new(format!(
            "Unsupported lakehouse quality threshold kind: {kind}"
        ))),
    }
}

fn threshold_value(rule: &QualityRule) -> Result<f64, LakehouseQualityError> {
    json_number(rule.threshold.get("value"))?.ok_or_else(|| {
        LakehouseQualityError::new(format!(
            "lakehouse quality threshold value missing: rule={}",
            rule.id
        ))
    })
}

fn json_number(value: Option<&JsonValue>) -> Result<Option<f64>, LakehouseQualityError> {
    match value {
        Some(JsonValue::Number(number)) => Ok(number.as_f64()),
        Some(JsonValue::String(text)) if !text.trim().is_empty() => {
            text.parse::<f64>().map(Some).map_err(|_| {
                LakehouseQualityError::new(format!("invalid numeric metric value: {text}"))
            })
        }
        Some(JsonValue::Null) | None => Ok(None),
        Some(other) => Err(LakehouseQualityError::new(format!(
            "invalid numeric metric value: {other}"
        ))),
    }
}

fn float_eq(left: f64, right: f64) -> bool {
    (left - right).abs() < f64::EPSILON
}

fn display_number(value: f64) -> String {
    value.to_string()
}
