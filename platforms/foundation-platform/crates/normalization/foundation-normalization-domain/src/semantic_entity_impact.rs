//! Entity impact detection from semantic metadata mappings.

use std::collections::BTreeMap;

use crate::semantic_metadata::entity_impact_mappings_for_source;

/// One entity whose consistency domains are impacted by a source record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DetectedEntityImpact {
    /// Impacted entity type.
    pub entity_type: String,
    /// Stable entity key computed from the source fields named by the mapping.
    pub entity_key: String,
    /// Consistency domains to recalculate for this entity.
    pub consistency_domains: Vec<String>,
}

/// Detects entity consistency impacts for one source record.
#[must_use]
pub fn detect_entity_impacts(
    source_slug: &str,
    fields: &BTreeMap<String, String>,
) -> Vec<DetectedEntityImpact> {
    entity_impact_mappings_for_source(source_slug)
        .iter()
        .filter_map(|mapping| {
            let entity_key = entity_key_from_fields(mapping.entity_key_fields, fields)?;
            Some(DetectedEntityImpact {
                entity_type: mapping.entity_type.as_str().to_owned(),
                entity_key,
                consistency_domains: mapping
                    .consistency_domains
                    .iter()
                    .map(|domain| domain.as_str().to_owned())
                    .collect(),
            })
        })
        .collect()
}

fn entity_key_from_fields(
    entity_key_fields: &[&str],
    fields: &BTreeMap<String, String>,
) -> Option<String> {
    let parts = entity_key_fields
        .iter()
        .map(|field| fields.get(*field).map(|value| value.trim()))
        .collect::<Option<Vec<_>>>()?;

    if parts.iter().any(|part| part.is_empty()) {
        return None;
    }

    Some(parts.join("|"))
}

#[cfg(test)]
mod tests;
