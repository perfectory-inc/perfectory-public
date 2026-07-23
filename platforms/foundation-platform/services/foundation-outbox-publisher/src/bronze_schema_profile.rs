//! Shared Bronze schema-observation profiling for per-lane ingest producers.
//!
//! Each public-data ingest lane (building register, real transaction, V-World
//! cadastral / land register / NED attribute) plans pages whose
//! [`PublicDataSchemaObservation`]s are produced per page by the planner in
//! `catalog-application`. This module merges those per-page observations across all
//! pages of a run into one profile per field path, then maps them onto
//! [`SchemaProfile`] rows for persistence.
//!
//! Lanes previously each carried a byte-identical private copy of this merge
//! logic. Some lanes additionally re-derived a candidate-key score during the
//! merge, scoped to a lane-specific field path. That lane-specific override is
//! preserved here through [`CandidateKeyOverride`]; lanes without an override
//! pass [`CandidateKeyOverride::None`] and keep the merged score unchanged.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use collection_application::{PublicDataBronzePagePlan, PublicDataSchemaObservation};
use collection_domain::{SchemaObservedType, SchemaProfile};
use foundation_shared_kernel::ids::{IngestionRunId, SchemaProfileId, SourceCatalogId};
use serde_json::Value as JsonValue;
use uuid::Uuid;

/// Lane-specific candidate-key re-scoring applied while merging per-page
/// observations into a single profile.
///
/// The score is only re-derived for the matched field path; all other fields
/// keep the score carried over from per-page observations. This mirrors the
/// per-lane behavior that previously lived in each lane's `into_observation`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CandidateKeyOverride {
    /// No re-scoring; the merged candidate-key score is kept as-is.
    None,
    /// Re-score field paths whose full string ends with the given suffix.
    EndsWith(&'static str),
    /// Re-score field paths whose last `.`-delimited segment equals the value.
    LastDotSegmentEquals(&'static str),
}

impl CandidateKeyOverride {
    fn matches(self, field_path: &str) -> bool {
        match self {
            CandidateKeyOverride::None => false,
            CandidateKeyOverride::EndsWith(suffix) => field_path.ends_with(suffix),
            CandidateKeyOverride::LastDotSegmentEquals(segment) => {
                field_path.split('.').next_back() == Some(segment)
            }
        }
    }
}

/// Maps merged cross-page observations onto `SchemaProfile` rows for one run.
pub(crate) fn schema_profiles_for_plans(
    source_catalog_id: SourceCatalogId,
    ingestion_run_id: IngestionRunId,
    now: DateTime<Utc>,
    plans: &[PublicDataBronzePagePlan],
    candidate_key_override: CandidateKeyOverride,
) -> Vec<SchemaProfile> {
    aggregate_schema_observations(plans, candidate_key_override)
        .into_iter()
        .map(|observation| SchemaProfile {
            id: SchemaProfileId::new(Uuid::new_v4()),
            source_catalog_id,
            ingestion_run_id,
            field_path: observation.field_path,
            observed_type: observation.observed_type,
            nonnull_count: observation.nonnull_count,
            null_count: observation.null_count,
            sample_values: observation.sample_values,
            candidate_key_score: observation.candidate_key_score,
            profiled_at: now,
            created_at: now,
            updated_at: now,
            version: 1,
        })
        .collect()
}

/// Merges per-page observations across all pages into one observation per field
/// path, applying any lane-specific candidate-key override.
fn aggregate_schema_observations(
    plans: &[PublicDataBronzePagePlan],
    candidate_key_override: CandidateKeyOverride,
) -> Vec<PublicDataSchemaObservation> {
    let mut fields = BTreeMap::<String, SchemaObservationAccumulator>::new();
    for observation in plans
        .iter()
        .flat_map(|plan| plan.schema_observations.iter())
    {
        fields
            .entry(observation.field_path.clone())
            .or_default()
            .record(observation);
    }
    fields
        .into_iter()
        .map(|(field_path, accumulator)| {
            accumulator.into_observation(field_path, candidate_key_override)
        })
        .collect()
}

#[derive(Clone, Debug, Default)]
struct SchemaObservationAccumulator {
    observed_type: Option<SchemaObservedType>,
    nonnull_count: u64,
    null_count: u64,
    sample_values: Vec<JsonValue>,
    candidate_key_score: f64,
}

impl SchemaObservationAccumulator {
    fn record(&mut self, observation: &PublicDataSchemaObservation) {
        self.observed_type = Some(
            self.observed_type
                .map_or(observation.observed_type, |existing| {
                    merge_observed_type(existing, observation.observed_type)
                }),
        );
        self.nonnull_count += observation.nonnull_count;
        self.null_count += observation.null_count;
        self.candidate_key_score = self
            .candidate_key_score
            .max(observation.candidate_key_score);

        if let JsonValue::Array(values) = &observation.sample_values {
            for value in values {
                if self.sample_values.len() >= 3 {
                    break;
                }
                if !self.sample_values.contains(value) {
                    self.sample_values.push(value.clone());
                }
            }
        }
    }

    fn into_observation(
        self,
        field_path: String,
        candidate_key_override: CandidateKeyOverride,
    ) -> PublicDataSchemaObservation {
        let candidate_key_score = if candidate_key_override.matches(&field_path) {
            if self.null_count == 0 {
                self.candidate_key_score.max(1.0)
            } else {
                0.0
            }
        } else {
            self.candidate_key_score
        };
        PublicDataSchemaObservation {
            field_path,
            observed_type: self.observed_type.unwrap_or(SchemaObservedType::Null),
            nonnull_count: self.nonnull_count,
            null_count: self.null_count,
            sample_values: JsonValue::Array(self.sample_values),
            candidate_key_score,
        }
    }
}

fn merge_observed_type(
    existing: SchemaObservedType,
    next: SchemaObservedType,
) -> SchemaObservedType {
    if matches!(existing, SchemaObservedType::Mixed) || matches!(next, SchemaObservedType::Mixed) {
        SchemaObservedType::Mixed
    } else if matches!(existing, SchemaObservedType::Null) {
        next
    } else if matches!(next, SchemaObservedType::Null) || existing == next {
        existing
    } else {
        SchemaObservedType::Mixed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation(
        field_path: &str,
        observed_type: SchemaObservedType,
        nonnull_count: u64,
        null_count: u64,
        sample_values: JsonValue,
        candidate_key_score: f64,
    ) -> PublicDataSchemaObservation {
        PublicDataSchemaObservation {
            field_path: field_path.to_owned(),
            observed_type,
            nonnull_count,
            null_count,
            sample_values,
            candidate_key_score,
        }
    }

    #[test]
    fn merge_observed_type_promotes_conflicts_to_mixed() {
        assert_eq!(
            merge_observed_type(SchemaObservedType::String, SchemaObservedType::Number),
            SchemaObservedType::Mixed
        );
        assert_eq!(
            merge_observed_type(SchemaObservedType::Null, SchemaObservedType::String),
            SchemaObservedType::String
        );
        assert_eq!(
            merge_observed_type(SchemaObservedType::String, SchemaObservedType::Null),
            SchemaObservedType::String
        );
        assert_eq!(
            merge_observed_type(SchemaObservedType::String, SchemaObservedType::String),
            SchemaObservedType::String
        );
        assert_eq!(
            merge_observed_type(SchemaObservedType::Mixed, SchemaObservedType::String),
            SchemaObservedType::Mixed
        );
    }

    #[test]
    fn accumulator_merges_counts_types_and_caps_samples() {
        let mut accumulator = SchemaObservationAccumulator::default();
        accumulator.record(&observation(
            "a.b",
            SchemaObservedType::String,
            1,
            0,
            JsonValue::Array(vec![JsonValue::from("x"), JsonValue::from("y")]),
            0.5,
        ));
        accumulator.record(&observation(
            "a.b",
            SchemaObservedType::Number,
            2,
            3,
            JsonValue::Array(vec![
                JsonValue::from("y"),
                JsonValue::from("z"),
                JsonValue::from("w"),
            ]),
            0.25,
        ));

        let merged = accumulator.into_observation("a.b".to_owned(), CandidateKeyOverride::None);
        assert_eq!(merged.observed_type, SchemaObservedType::Mixed);
        assert_eq!(merged.nonnull_count, 3);
        assert_eq!(merged.null_count, 3);
        // de-duplicated, capped at 3 samples.
        assert_eq!(
            merged.sample_values,
            JsonValue::Array(vec![
                JsonValue::from("x"),
                JsonValue::from("y"),
                JsonValue::from("z")
            ])
        );
        // candidate score is the max of recorded scores when no override applies.
        assert_eq!(merged.candidate_key_score, 0.5);
    }

    #[test]
    fn ends_with_override_applies_only_to_matching_field() {
        let key = into_one(
            CandidateKeyOverride::EndsWith("mgmBldrgstPk"),
            "items[].mgmBldrgstPk",
            0,
            0.0,
        );
        assert_eq!(key.candidate_key_score, 1.0);

        let key_with_nulls = into_one(
            CandidateKeyOverride::EndsWith("mgmBldrgstPk"),
            "items[].mgmBldrgstPk",
            1,
            0.9,
        );
        assert_eq!(key_with_nulls.candidate_key_score, 0.0);

        let other = into_one(
            CandidateKeyOverride::EndsWith("mgmBldrgstPk"),
            "items[].totArea",
            0,
            0.4,
        );
        assert_eq!(other.candidate_key_score, 0.4);
    }

    #[test]
    fn last_segment_override_matches_only_exact_trailing_segment() {
        let matched = into_one(
            CandidateKeyOverride::LastDotSegmentEquals("pnu"),
            "properties.pnu",
            0,
            0.0,
        );
        assert_eq!(matched.candidate_key_score, 1.0);

        // ends-with would match "spnu", last-segment equality must not.
        let not_matched = into_one(
            CandidateKeyOverride::LastDotSegmentEquals("pnu"),
            "properties.spnu",
            0,
            0.3,
        );
        assert_eq!(not_matched.candidate_key_score, 0.3);
    }

    fn into_one(
        candidate_key_override: CandidateKeyOverride,
        field_path: &str,
        null_count: u64,
        existing_score: f64,
    ) -> PublicDataSchemaObservation {
        let mut accumulator = SchemaObservationAccumulator::default();
        accumulator.record(&observation(
            field_path,
            SchemaObservedType::String,
            1,
            null_count,
            JsonValue::Array(Vec::new()),
            existing_score,
        ));
        accumulator.into_observation(field_path.to_owned(), candidate_key_override)
    }
}
