use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::{bail, Context as _, Result};
use chrono::{DateTime, Datelike as _, NaiveDate};
use collection_application::{
    plan_public_data_bulk_file_storage_location, PublicDataBulkFileIdentity,
    PublicDataBulkFileStorageLocationInput,
};
use foundation_shared_kernel::ids::IngestionRunId;
use serde::Deserialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use uuid::Uuid;

use crate::bronze_catalog_recovery_manifest::{
    BronzeCatalogRecoveryManifestCandidate, BronzeCatalogRecoverySourceManifest,
    BronzeCatalogRecoveryUnresolvedObject, RecoverySourceSnapshot,
};

const MISSING_CATALOG_CLASSIFICATION: &str = "bronze_catalog_metadata_missing";
const COMMON_REQUEST_PARAM_KEYS: [&str; 9] = [
    "operation",
    "provider_file_period",
    "provider_snapshot_date",
    "provider_file_id",
    "provider_file_name_label",
    "provider_updated_at",
    "raw_preserved",
    "physicalObjectFileName",
    "physicalObjectFileNameBasis",
];

#[derive(Clone, Debug)]
pub(crate) struct ProviderFileEvidence {
    pub(crate) source: RecoverySourceSnapshot,
    pub(crate) operation: String,
    pub(crate) provider_file_period: Option<String>,
    pub(crate) provider_snapshot_date: Option<NaiveDate>,
    pub(crate) provider_file_id: String,
    pub(crate) provider_file_name_label: String,
    pub(crate) provider_updated_at: Option<NaiveDate>,
    pub(crate) request_params_extra: JsonMap<String, JsonValue>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ProviderFileR2Object {
    pub(crate) key: String,
    pub(crate) size_bytes: i64,
    pub(crate) last_modified: Option<String>,
    #[serde(default)]
    pub(crate) e_tag: Option<String>,
    pub(crate) classification: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ProviderFileR2AuditDocument {
    pub(crate) schema_version: String,
    pub(crate) objects: Vec<ProviderFileR2Object>,
}

#[derive(Debug)]
pub(crate) struct ProviderFileRecoveryCompilation {
    pub(crate) sources: Vec<BronzeCatalogRecoverySourceManifest>,
    pub(crate) unresolved: Vec<BronzeCatalogRecoveryUnresolvedObject>,
}

pub(crate) fn compile_provider_file_recovery(
    selected_source_slugs: &[String],
    evidence: Vec<ProviderFileEvidence>,
    r2_objects: Vec<ProviderFileR2Object>,
    fallback_date: NaiveDate,
) -> Result<ProviderFileRecoveryCompilation> {
    let selected_sources = selected_source_slugs
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if selected_sources.len() != selected_source_slugs.len() {
        bail!("provider-file recovery source selection contains duplicates");
    }
    if selected_sources.is_empty() {
        bail!("provider-file recovery requires at least one selected source");
    }

    let mut source_contracts = BTreeMap::<&str, &RecoverySourceSnapshot>::new();
    let mut evidence_by_identity = HashMap::<String, Vec<&ProviderFileEvidence>>::new();
    for item in &evidence {
        if !selected_sources.contains(item.source.slug.as_str()) {
            bail!(
                "provider evidence source {} is outside the selected recovery scope",
                item.source.slug
            );
        }
        if let Some(previous) = source_contracts.insert(item.source.slug.as_str(), &item.source) {
            if previous != &item.source {
                bail!(
                    "provider evidence contains conflicting source contracts for {}",
                    item.source.slug
                );
            }
        }
        evidence_by_identity
            .entry(provider_identity_key(
                &item.source.slug,
                &item.provider_file_id,
            ))
            .or_default()
            .push(item);
    }

    let source_prefixes = selected_sources
        .iter()
        .map(|source| format!("bronze/source={source}/"))
        .collect::<Vec<_>>();
    let scoped_objects = r2_objects
        .into_iter()
        .filter(|object| {
            object.classification == MISSING_CATALOG_CLASSIFICATION
                && source_prefixes
                    .iter()
                    .any(|prefix| object.key.starts_with(prefix))
        })
        .collect::<Vec<_>>();
    let mut seen_r2_keys = HashSet::with_capacity(scoped_objects.len());
    for object in &scoped_objects {
        if !seen_r2_keys.insert(object.key.as_str()) {
            bail!(
                "R2 audit contains duplicate R2 inventory key {}",
                object.key
            );
        }
    }

    let mut sources = BTreeMap::<String, BronzeCatalogRecoverySourceManifest>::new();
    let mut unresolved = Vec::new();
    for object in scoped_objects {
        let physical = parse_physical_object_identity(&object.key)?;
        let matching = evidence_by_identity
            .get(&provider_identity_key(
                &physical.source_slug,
                &physical.provider_file_id,
            ))
            .cloned()
            .unwrap_or_default();
        if matching.len() != 1 {
            unresolved.push(BronzeCatalogRecoveryUnresolvedObject {
                source_slug: physical.source_slug,
                object_key: object.key,
                reason: if matching.is_empty() {
                    "missing_provider_inventory_match".to_owned()
                } else {
                    "ambiguous_provider_inventory_match".to_owned()
                },
                matching_evidence_count: matching.len(),
            });
            continue;
        }
        let evidence = matching
            .into_iter()
            .next()
            .context("provider evidence match disappeared after cardinality validation")?;
        let candidate = compile_candidate(evidence, &physical, &object, fallback_date)?;
        let source_slug = evidence.source.slug.clone();
        sources
            .entry(source_slug)
            .or_insert_with(|| BronzeCatalogRecoverySourceManifest {
                source: evidence.source.clone(),
                candidates: Vec::new(),
            })
            .candidates
            .push(candidate);
    }

    for source in sources.values_mut() {
        source
            .candidates
            .sort_by(|left, right| left.object_key.cmp(&right.object_key));
    }
    unresolved.sort_by(|left, right| left.object_key.cmp(&right.object_key));

    Ok(ProviderFileRecoveryCompilation {
        sources: sources.into_values().collect(),
        unresolved,
    })
}

fn compile_candidate(
    evidence: &ProviderFileEvidence,
    physical: &ProviderFilePhysicalObjectIdentity,
    object: &ProviderFileR2Object,
    fallback_date: NaiveDate,
) -> Result<BronzeCatalogRecoveryManifestCandidate> {
    validate_request_params_extra(&evidence.request_params_extra)?;
    let identity = PublicDataBulkFileIdentity {
        operation: evidence.operation.clone(),
        provider_file_period: evidence.provider_file_period.clone(),
        provider_snapshot_date: evidence.provider_snapshot_date,
        provider_file_id: evidence.provider_file_id.clone(),
        provider_file_name: physical.object_file_name.clone(),
        provider_updated_at: evidence.provider_updated_at,
    };
    let location =
        plan_public_data_bulk_file_storage_location(&PublicDataBulkFileStorageLocationInput {
            source_slug: &evidence.source.slug,
            ingest_date: fallback_date,
            ingestion_run_id: IngestionRunId::new(Uuid::nil()),
            identity,
        })?;
    if location.object_key.as_str() != object.key {
        bail!(
            "provider identity did not reproduce observed physical key {}",
            object.key
        );
    }

    let observed = object
        .last_modified
        .as_deref()
        .context("R2 recovery candidate is missing last_modified")?;
    let observed_at =
        DateTime::parse_from_rfc3339(observed).context("invalid R2 last_modified timestamp")?;
    let observed_r2_etag = object
        .e_tag
        .as_deref()
        .filter(|value| !value.trim().is_empty() && value.trim() == *value)
        .context("R2 recovery candidate is missing a canonical ETag")?;
    let expected_size_bytes =
        u64::try_from(object.size_bytes).context("R2 audit object size must be non-negative")?;
    let mut request_params = common_request_params(evidence, &physical.object_file_name);
    request_params.extend(evidence.request_params_extra.clone());
    let mut snapshot_period = location.snapshot_period;
    let mut snapshot_date = location.snapshot_date;
    if location.snapshot_basis.as_str() == "collected_at_fallback" {
        snapshot_date = observed_at.date_naive();
        snapshot_period = Some(format!(
            "{}-{:02}",
            snapshot_date.year(),
            snapshot_date.month()
        ));
    }

    Ok(BronzeCatalogRecoveryManifestCandidate {
        object_key: object.key.clone(),
        expected_size_bytes,
        expected_checksum_sha256: None,
        source_partition_key: Some(location.source_partition_key),
        source_identity_key: location.source_identity_key,
        request_params: JsonValue::Object(request_params),
        content_type: content_type_from_file_name(&physical.object_file_name),
        logical_record_count: None,
        observed_r2_etag: Some(observed_r2_etag.to_owned()),
        observed_r2_last_modified: observed.to_owned(),
        snapshot_period,
        snapshot_date: snapshot_date.to_string(),
        snapshot_granularity: location.snapshot_granularity.as_str().to_owned(),
        snapshot_basis: location.snapshot_basis.as_str().to_owned(),
        provider_file_id: Some(evidence.provider_file_id.clone()),
        provider_file_name: Some(evidence.provider_file_name_label.clone()),
        provider_updated_at: evidence.provider_updated_at.map(|date| date.to_string()),
        effective_date: None,
        evidence_kind: "provider_inventory".to_owned(),
    })
}

fn common_request_params(
    evidence: &ProviderFileEvidence,
    physical_object_file_name: &str,
) -> JsonMap<String, JsonValue> {
    let mut params = JsonMap::new();
    params.insert(
        "operation".to_owned(),
        JsonValue::String(evidence.operation.clone()),
    );
    params.insert(
        "provider_file_period".to_owned(),
        evidence
            .provider_file_period
            .clone()
            .map_or(JsonValue::Null, JsonValue::String),
    );
    params.insert(
        "provider_snapshot_date".to_owned(),
        evidence
            .provider_snapshot_date
            .map(|date| JsonValue::String(date.to_string()))
            .unwrap_or(JsonValue::Null),
    );
    params.insert(
        "provider_file_id".to_owned(),
        JsonValue::String(evidence.provider_file_id.clone()),
    );
    params.insert(
        "provider_file_name_label".to_owned(),
        JsonValue::String(evidence.provider_file_name_label.clone()),
    );
    params.insert(
        "provider_updated_at".to_owned(),
        evidence
            .provider_updated_at
            .map(|date| JsonValue::String(date.to_string()))
            .unwrap_or(JsonValue::Null),
    );
    params.insert("raw_preserved".to_owned(), JsonValue::Bool(true));
    params.insert(
        "physicalObjectFileName".to_owned(),
        JsonValue::String(physical_object_file_name.to_owned()),
    );
    params.insert(
        "physicalObjectFileNameBasis".to_owned(),
        JsonValue::String("r2_inventory_key_leaf".to_owned()),
    );
    params
}

fn validate_request_params_extra(extra: &JsonMap<String, JsonValue>) -> Result<()> {
    if let Some(key) = COMMON_REQUEST_PARAM_KEYS
        .iter()
        .find(|key| extra.contains_key(**key))
    {
        bail!("provider adapter attempted to overwrite reserved request_params field {key}");
    }
    Ok(())
}

fn provider_identity_key(source_slug: &str, provider_file_id: &str) -> String {
    format!("{source_slug}\0{provider_file_id}")
}

struct ProviderFilePhysicalObjectIdentity {
    source_slug: String,
    provider_file_id: String,
    object_file_name: String,
}

fn parse_physical_object_identity(key: &str) -> Result<ProviderFilePhysicalObjectIdentity> {
    collection_domain::validate_bronze_object_key_contract(key)
        .context("invalid provider-file Bronze physical object key")?;
    let rest = key
        .strip_prefix("bronze/source=")
        .context("provider-file recovery object key must use the Bronze source prefix")?;
    let (source_slug, relative_path) = rest
        .split_once('/')
        .context("provider-file recovery object key must contain a source-relative path")?;
    let object_file_name = relative_path
        .rsplit_once('/')
        .map_or(relative_path, |(_, leaf)| leaf);
    let (provider_file_id, extension) = object_file_name
        .rsplit_once('.')
        .context("provider-file recovery object key leaf must include an extension")?;
    if provider_file_id.is_empty() || extension.is_empty() {
        bail!("provider-file recovery object key leaf has an invalid provider file identity");
    }
    Ok(ProviderFilePhysicalObjectIdentity {
        source_slug: source_slug.to_owned(),
        provider_file_id: provider_file_id.to_owned(),
        object_file_name: object_file_name.to_owned(),
    })
}

fn content_type_from_file_name(file_name: &str) -> String {
    match file_name
        .rsplit_once('.')
        .map(|(_, extension)| extension.to_ascii_lowercase())
        .as_deref()
    {
        Some("zip") => "application/zip",
        Some("csv") => "text/csv",
        Some("json") => "application/json",
        Some("hwp") => "application/x-hwp",
        Some("xlsx") => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        _ => "application/octet-stream",
    }
    .to_owned()
}

#[cfg(test)]
mod tests;
