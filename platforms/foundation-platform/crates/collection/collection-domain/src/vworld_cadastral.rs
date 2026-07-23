//! Domain rules for deterministic `VWorld` cadastral feature reconciliation.

use std::collections::{btree_map::Entry, BTreeMap};

use serde_json::Value as JsonValue;
use thiserror::Error;

const LOGICAL_ITEMS_POINTER: &str = "/response/result/featureCollection/features";

/// Failure returned when a cadastral provider payload cannot be reconciled safely.
#[derive(Debug, Error)]
#[error("invalid VWorld cadastral feature payload: {0}")]
pub struct VWorldCadastralFeatureError(String);

/// One `VWorld` cadastral feature selected as the canonical record for a PNU.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldCadastralDedupedFeature {
    /// Nineteen-digit parcel number used as the source key.
    pub pnu: String,
    /// Representative raw `GeoJSON` feature retained for downstream Silver normalization.
    pub feature: JsonValue,
    /// Raw provider attribute object from the representative feature.
    pub properties: JsonValue,
    /// Raw provider geometry object from the representative feature.
    pub geometry: JsonValue,
    /// Number of times this PNU appeared across scanned Bronze payloads.
    pub occurrence_count: u64,
}

/// Deterministic PNU-level deduplication report for `VWorld` cadastral Bronze payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldCadastralFeatureDedupeReport {
    /// Unique features ordered by ascending PNU.
    pub records: Vec<VWorldCadastralDedupedFeature>,
    /// Number of non-canonical duplicate feature occurrences skipped.
    pub duplicate_count: u64,
    /// Number of provider features excluded because `properties.pnu` was not 19 ASCII digits.
    pub invalid_pnu_feature_count: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum InvalidPnuFeaturePolicy {
    #[default]
    Reject,
    Quarantine,
}

/// Incremental PNU-level deduplicator for chunked `VWorld` cadastral Bronze payload processing.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VWorldCadastralFeatureDedupeAccumulator {
    by_pnu: BTreeMap<String, VWorldCadastralDedupedFeature>,
    duplicate_count: u64,
    invalid_pnu_feature_count: u64,
    payload_count: usize,
    invalid_pnu_feature_policy: InvalidPnuFeaturePolicy,
}

impl VWorldCadastralFeatureDedupeAccumulator {
    /// Creates an empty accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an accumulator that excludes invalid-PNU provider features while counting them.
    ///
    /// This is intended for national public-data promotion jobs where one malformed provider
    /// feature must not block unrelated valid parcels in the same page.
    #[must_use]
    pub fn new_with_invalid_pnu_quarantine() -> Self {
        Self {
            invalid_pnu_feature_policy: InvalidPnuFeaturePolicy::Quarantine,
            ..Self::default()
        }
    }

    /// Ingests one parsed Bronze payload into this accumulator.
    ///
    /// # Errors
    /// Returns [`VWorldCadastralFeatureError`] when the payload shape is invalid, a feature has an
    /// invalid PNU, or a repeated PNU carries conflicting geometry.
    pub fn ingest_payload(
        &mut self,
        payload: &JsonValue,
    ) -> Result<(), VWorldCadastralFeatureError> {
        let payload_index = self.payload_count;
        self.payload_count += 1;
        self.ingest_payload_at_index(payload, payload_index)
    }

    /// Consumes the accumulator and returns a deterministic deduplication report.
    #[must_use]
    pub fn finish(self) -> VWorldCadastralFeatureDedupeReport {
        VWorldCadastralFeatureDedupeReport {
            records: self.by_pnu.into_values().collect(),
            duplicate_count: self.duplicate_count,
            invalid_pnu_feature_count: self.invalid_pnu_feature_count,
        }
    }

    fn ingest_payload_at_index(
        &mut self,
        payload: &JsonValue,
        payload_index: usize,
    ) -> Result<(), VWorldCadastralFeatureError> {
        if is_vworld_zero_record_payload(payload) {
            return Ok(());
        }
        let features = payload
            .pointer(LOGICAL_ITEMS_POINTER)
            .and_then(JsonValue::as_array)
            .ok_or_else(|| {
                invalid_payload(format!(
                    "VWorld cadastral payload {payload_index} must contain a features array at {LOGICAL_ITEMS_POINTER}"
                ))
            })?;

        for (feature_index, feature) in features.iter().enumerate() {
            self.ingest_feature(feature, payload_index, feature_index)?;
        }
        Ok(())
    }

    fn ingest_feature(
        &mut self,
        feature: &JsonValue,
        payload_index: usize,
        feature_index: usize,
    ) -> Result<(), VWorldCadastralFeatureError> {
        let pnu = match extract_feature_pnu(feature, payload_index, feature_index) {
            Ok(pnu) => pnu,
            Err(_error)
                if self.invalid_pnu_feature_policy == InvalidPnuFeaturePolicy::Quarantine =>
            {
                self.invalid_pnu_feature_count += 1;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let properties =
            extract_feature_object(feature, "properties", payload_index, feature_index)?;
        let geometry = extract_feature_object(feature, "geometry", payload_index, feature_index)?;

        match self.by_pnu.entry(pnu.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(VWorldCadastralDedupedFeature {
                    pnu,
                    feature: feature.clone(),
                    properties,
                    geometry,
                    occurrence_count: 1,
                });
            }
            Entry::Occupied(mut entry) => {
                if entry.get().geometry != geometry {
                    return Err(invalid_payload(format!(
                        "VWorld cadastral geometry conflict for pnu {pnu} at payload {payload_index} feature {feature_index}"
                    )));
                }
                self.duplicate_count += 1;
                entry.get_mut().occurrence_count += 1;
            }
        }
        Ok(())
    }
}

/// Extracts `VWorld` cadastral `GeoJSON` features and keeps one canonical feature per PNU.
///
/// Duplicate features are accepted only when the repeated PNU carries the same geometry.
///
/// # Errors
/// Returns [`VWorldCadastralFeatureError`] when payload shape, PNU, or duplicate geometry is
/// invalid.
pub fn dedupe_vworld_cadastral_features_by_pnu(
    payloads: &[JsonValue],
) -> Result<VWorldCadastralFeatureDedupeReport, VWorldCadastralFeatureError> {
    let mut accumulator = VWorldCadastralFeatureDedupeAccumulator::new();
    for payload in payloads {
        accumulator.ingest_payload(payload)?;
    }
    Ok(accumulator.finish())
}

fn extract_feature_pnu(
    feature: &JsonValue,
    payload_index: usize,
    feature_index: usize,
) -> Result<String, VWorldCadastralFeatureError> {
    let raw = feature
        .pointer("/properties/pnu")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            invalid_payload(format!(
                "VWorld cadastral feature at payload {payload_index} index {feature_index} must contain string properties.pnu"
            ))
        })?;
    if raw.len() == 19 && raw.bytes().all(|byte| byte.is_ascii_digit()) {
        Ok(raw.to_owned())
    } else {
        Err(invalid_payload("pnu must be exactly 19 digits".to_owned()))
    }
}

fn extract_feature_object(
    feature: &JsonValue,
    field: &'static str,
    payload_index: usize,
    feature_index: usize,
) -> Result<JsonValue, VWorldCadastralFeatureError> {
    match feature.get(field) {
        Some(JsonValue::Object(_)) => Ok(feature[field].clone()),
        _ => Err(invalid_payload(format!(
            "VWorld cadastral feature at payload {payload_index} index {feature_index} must contain object {field}"
        ))),
    }
}

fn is_vworld_zero_record_payload(payload: &JsonValue) -> bool {
    payload
        .pointer("/response/status")
        .and_then(JsonValue::as_str)
        .is_some_and(|status| status == "NOT_FOUND")
        && payload
            .pointer("/response/record/total")
            .and_then(JsonValue::as_str)
            .is_some_and(|total| total == "0")
        && payload
            .pointer("/response/record/current")
            .and_then(JsonValue::as_str)
            .is_some_and(|current| current == "0")
}

const fn invalid_payload(message: String) -> VWorldCadastralFeatureError {
    VWorldCadastralFeatureError(message)
}
