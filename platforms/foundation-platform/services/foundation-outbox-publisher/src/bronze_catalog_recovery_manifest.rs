use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context as _, Result};
use chrono::{DateTime, NaiveDate, Utc};
use collection_application::bronze_catalog_recovery::{
    BronzeCatalogRecoveryCandidate, BronzeCatalogRecoveryInput, BronzeCatalogRecoveryMode,
    RecoveryEvidenceKind,
};
use collection_domain::{
    SnapshotBasis, SnapshotGranularity, SourceAuthKind, SourceCatalogEntry, SourcePayloadFormat,
};
use foundation_shared_kernel::ids::SourceCatalogId;
use foundation_shared_kernel::ObjectKey;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

pub(crate) const BRONZE_CATALOG_RECOVERY_MANIFEST_SCHEMA_VERSION: &str =
    "foundation-platform.bronze_catalog_recovery_manifest.v1";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BronzeCatalogRecoveryManifestStatus {
    Ready,
    ReadyWithQuarantine,
    Blocked,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct RecoveryEvidenceArtifact {
    pub(crate) uri: String,
    pub(crate) sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct RecoverySourceSnapshot {
    pub(crate) endpoint_slug: String,
    pub(crate) slug: String,
    pub(crate) name: String,
    pub(crate) provider: String,
    pub(crate) dataset_name: String,
    pub(crate) base_url: Option<String>,
    pub(crate) auth_kind: String,
    pub(crate) payload_format: String,
    pub(crate) terms_url: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct BronzeCatalogRecoveryManifestCandidate {
    pub(crate) object_key: String,
    pub(crate) expected_size_bytes: u64,
    pub(crate) expected_checksum_sha256: Option<String>,
    pub(crate) source_partition_key: Option<String>,
    pub(crate) source_identity_key: String,
    pub(crate) request_params: JsonValue,
    pub(crate) content_type: String,
    pub(crate) logical_record_count: Option<u64>,
    #[serde(default)]
    pub(crate) observed_r2_etag: Option<String>,
    pub(crate) observed_r2_last_modified: String,
    pub(crate) snapshot_period: Option<String>,
    pub(crate) snapshot_date: String,
    pub(crate) snapshot_granularity: String,
    pub(crate) snapshot_basis: String,
    pub(crate) provider_file_id: Option<String>,
    pub(crate) provider_file_name: Option<String>,
    pub(crate) provider_updated_at: Option<String>,
    pub(crate) effective_date: Option<String>,
    pub(crate) evidence_kind: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct BronzeCatalogRecoverySourceManifest {
    pub(crate) source: RecoverySourceSnapshot,
    pub(crate) candidates: Vec<BronzeCatalogRecoveryManifestCandidate>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct BronzeCatalogRecoveryUnresolvedObject {
    pub(crate) source_slug: String,
    pub(crate) object_key: String,
    pub(crate) reason: String,
    pub(crate) matching_evidence_count: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct BronzeCatalogRecoveryManifest {
    pub(crate) schema_version: String,
    pub(crate) generated_at_utc: String,
    pub(crate) status: BronzeCatalogRecoveryManifestStatus,
    pub(crate) endpoint_catalog: RecoveryEvidenceArtifact,
    pub(crate) provider_inventory: RecoveryEvidenceArtifact,
    pub(crate) r2_inventory: RecoveryEvidenceArtifact,
    pub(crate) sources: Vec<BronzeCatalogRecoverySourceManifest>,
    pub(crate) unresolved: Vec<BronzeCatalogRecoveryUnresolvedObject>,
}

impl BronzeCatalogRecoveryManifest {
    pub(crate) fn executable_source_projections(&self) -> Vec<Self> {
        self.sources
            .iter()
            .filter(|source| !source.candidates.is_empty())
            .map(|source| {
                let unresolved = self
                    .unresolved
                    .iter()
                    .filter(|object| object.source_slug == source.source.slug)
                    .cloned()
                    .collect::<Vec<_>>();
                let status = if unresolved.is_empty() {
                    BronzeCatalogRecoveryManifestStatus::Ready
                } else {
                    BronzeCatalogRecoveryManifestStatus::ReadyWithQuarantine
                };
                Self {
                    schema_version: self.schema_version.clone(),
                    generated_at_utc: self.generated_at_utc.clone(),
                    status,
                    endpoint_catalog: self.endpoint_catalog.clone(),
                    provider_inventory: self.provider_inventory.clone(),
                    r2_inventory: self.r2_inventory.clone(),
                    sources: vec![source.clone()],
                    unresolved,
                }
            })
            .collect()
    }

    pub(crate) fn write_executable_source_projections(
        &self,
        output_directory: &Path,
    ) -> Result<Vec<PathBuf>> {
        fs::create_dir_all(output_directory).with_context(|| {
            format!(
                "failed to create recovery projection directory {}",
                output_directory.display()
            )
        })?;
        for entry in fs::read_dir(output_directory).with_context(|| {
            format!(
                "failed to inspect recovery projection directory {}",
                output_directory.display()
            )
        })? {
            let entry = entry.context("failed to inspect recovery projection entry")?;
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            if entry.file_type()?.is_file()
                && ((file_name.starts_with("source=") && file_name.ends_with(".json"))
                    || matches!(
                        file_name.as_ref(),
                        "ready-sources.json" | "executable-sources.json"
                    ))
            {
                fs::remove_file(entry.path()).with_context(|| {
                    format!(
                        "failed to remove stale recovery projection {}",
                        entry.path().display()
                    )
                })?;
            }
        }
        let projections = self.executable_source_projections();
        let mut paths = Vec::new();
        for projection in &projections {
            let source_slug = &projection.sources[0].source.slug;
            if source_slug.is_empty()
                || !source_slug.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'_' | b'-')
                })
            {
                bail!("recovery source slug is unsafe for projection filename");
            }
            let path = output_directory.join(format!("source={source_slug}.json"));
            crate::r2_command_support::write_json_file(&path, &projection)?;
            paths.push(path);
        }
        if !projections.is_empty() {
            let unresolved = projections
                .iter()
                .flat_map(|projection| projection.unresolved.iter().cloned())
                .collect::<Vec<_>>();
            let status = if unresolved.is_empty() {
                BronzeCatalogRecoveryManifestStatus::Ready
            } else {
                BronzeCatalogRecoveryManifestStatus::ReadyWithQuarantine
            };
            let aggregate = Self {
                schema_version: self.schema_version.clone(),
                generated_at_utc: self.generated_at_utc.clone(),
                status,
                endpoint_catalog: self.endpoint_catalog.clone(),
                provider_inventory: self.provider_inventory.clone(),
                r2_inventory: self.r2_inventory.clone(),
                sources: projections
                    .into_iter()
                    .map(|projection| projection.sources[0].clone())
                    .collect(),
                unresolved,
            };
            crate::r2_command_support::write_json_file(
                &output_directory.join("executable-sources.json"),
                &aggregate,
            )?;
        }
        Ok(paths)
    }

    pub(crate) fn to_recovery_inputs(
        &self,
        mode: BronzeCatalogRecoveryMode,
        manifest_uri: &str,
        manifest_sha256: &str,
        started_at: DateTime<Utc>,
    ) -> Result<Vec<BronzeCatalogRecoveryInput>> {
        if self.schema_version != BRONZE_CATALOG_RECOVERY_MANIFEST_SCHEMA_VERSION {
            bail!("unsupported Bronze Catalog recovery manifest schema version");
        }
        match self.status {
            BronzeCatalogRecoveryManifestStatus::Blocked => {
                bail!("blocked Bronze Catalog recovery manifest cannot be executed");
            }
            BronzeCatalogRecoveryManifestStatus::Ready if !self.unresolved.is_empty() => {
                bail!("ready Bronze Catalog recovery manifest cannot hide unresolved objects");
            }
            BronzeCatalogRecoveryManifestStatus::ReadyWithQuarantine
                if self.unresolved.is_empty() =>
            {
                bail!("ready_with_quarantine manifest requires unresolved objects");
            }
            BronzeCatalogRecoveryManifestStatus::Ready
            | BronzeCatalogRecoveryManifestStatus::ReadyWithQuarantine => {}
        }
        if manifest_uri.trim().is_empty() || !is_sha256(manifest_sha256) {
            bail!("recovery manifest URI and lowercase SHA-256 are required");
        }
        parse_timestamp("manifest generated_at_utc", &self.generated_at_utc)?;
        validate_evidence_artifact("endpoint_catalog", &self.endpoint_catalog)?;
        validate_evidence_artifact("provider_inventory", &self.provider_inventory)?;
        validate_evidence_artifact("r2_inventory", &self.r2_inventory)?;
        if self.sources.is_empty() {
            bail!("recovery manifest contains no source scopes");
        }

        let declared_source_slugs = self
            .sources
            .iter()
            .map(|source| source.source.slug.as_str())
            .collect::<BTreeSet<_>>();
        let mut unresolved_counts = BTreeMap::<&str, u64>::new();
        let mut unresolved_keys = BTreeSet::new();
        for unresolved in &self.unresolved {
            if !declared_source_slugs.contains(unresolved.source_slug.as_str()) {
                bail!(
                    "recovery unresolved source {} is outside the manifest source scope",
                    unresolved.source_slug
                );
            }
            if unresolved.object_key.trim().is_empty()
                || unresolved.reason.trim().is_empty()
                || unresolved.matching_evidence_count == 1
            {
                bail!("recovery unresolved evidence is incomplete or contradictory");
            }
            let expected_prefix = format!("bronze/source={}/", unresolved.source_slug);
            if !unresolved.object_key.starts_with(&expected_prefix) {
                bail!("recovery unresolved object key disagrees with its source");
            }
            if !unresolved_keys.insert(unresolved.object_key.as_str()) {
                bail!("duplicate recovery unresolved object key");
            }
            let count = unresolved_counts
                .entry(unresolved.source_slug.as_str())
                .or_default();
            *count = count
                .checked_add(1)
                .context("recovery unresolved object count overflowed")?;
        }

        let mut source_slugs = BTreeSet::new();
        let mut object_keys = BTreeSet::new();
        let mut inputs = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            if source.candidates.is_empty() {
                bail!(
                    "recovery source {} contains no candidates",
                    source.source.slug
                );
            }
            if !source_slugs.insert(source.source.slug.as_str()) {
                bail!("duplicate recovery source slug {}", source.source.slug);
            }
            for candidate in &source.candidates {
                if unresolved_keys.contains(candidate.object_key.as_str()) {
                    bail!("recovery object key appears in both candidate and unresolved scopes");
                }
                if !object_keys.insert(candidate.object_key.as_str()) {
                    bail!("duplicate recovery object key {}", candidate.object_key);
                }
            }
            let source_entry = source.source.to_catalog_entry(started_at)?;
            let candidates = source
                .candidates
                .iter()
                .map(BronzeCatalogRecoveryManifestCandidate::to_candidate)
                .collect::<Result<Vec<_>>>()?;
            inputs.push(BronzeCatalogRecoveryInput {
                mode,
                source: source_entry,
                evidence_manifest_uri: manifest_uri.to_owned(),
                evidence_manifest_sha256: manifest_sha256.to_owned(),
                excluded_unresolved_object_count: unresolved_counts
                    .get(source.source.slug.as_str())
                    .copied()
                    .unwrap_or_default(),
                started_at,
                candidates,
            });
        }
        Ok(inputs)
    }
}

impl RecoverySourceSnapshot {
    fn to_catalog_entry(&self, now: DateTime<Utc>) -> Result<SourceCatalogEntry> {
        if self.endpoint_slug.trim().is_empty() {
            bail!("recovery source endpoint_slug must not be empty");
        }
        Ok(SourceCatalogEntry {
            id: SourceCatalogId::new(Uuid::new_v4()),
            slug: self.slug.clone(),
            name: self.name.clone(),
            provider: self.provider.clone(),
            dataset_name: self.dataset_name.clone(),
            base_url: self.base_url.clone(),
            auth_kind: SourceAuthKind::from_wire(&self.auth_kind)
                .context("invalid recovery source auth_kind")?,
            payload_format: SourcePayloadFormat::from_wire(&self.payload_format)
                .context("invalid recovery source payload_format")?,
            license_name: None,
            license_url: None,
            terms_url: self.terms_url.clone(),
            collection_frequency: None,
            is_active: true,
            created_at: now,
            updated_at: now,
            version: 1,
        })
    }
}

impl BronzeCatalogRecoveryManifestCandidate {
    fn to_candidate(&self) -> Result<BronzeCatalogRecoveryCandidate> {
        Ok(BronzeCatalogRecoveryCandidate {
            object_key: ObjectKey::parse(&self.object_key)
                .context("invalid recovery candidate object_key")?,
            expected_size_bytes: self.expected_size_bytes,
            expected_checksum_sha256: self.expected_checksum_sha256.clone(),
            source_partition_key: self.source_partition_key.clone(),
            source_identity_key: self.source_identity_key.clone(),
            request_params: self.request_params.clone(),
            content_type: self.content_type.clone(),
            logical_record_count: self.logical_record_count,
            observed_r2_etag: self
                .observed_r2_etag
                .as_deref()
                .filter(|value| !value.trim().is_empty() && value.trim() == *value)
                .context("recovery candidate is missing a canonical observed_r2_etag")?
                .to_owned(),
            observed_r2_last_modified: parse_timestamp(
                "observed_r2_last_modified",
                &self.observed_r2_last_modified,
            )?,
            snapshot_period: self.snapshot_period.clone(),
            snapshot_date: parse_date("snapshot_date", &self.snapshot_date)?,
            snapshot_granularity: SnapshotGranularity::from_wire(&self.snapshot_granularity)
                .context("invalid recovery candidate snapshot_granularity")?,
            snapshot_basis: SnapshotBasis::from_wire(&self.snapshot_basis)
                .context("invalid recovery candidate snapshot_basis")?,
            provider_file_id: self.provider_file_id.clone(),
            provider_file_name: self.provider_file_name.clone(),
            provider_updated_at: self
                .provider_updated_at
                .as_deref()
                .map(|value| parse_date("provider_updated_at", value))
                .transpose()?,
            effective_date: self
                .effective_date
                .as_deref()
                .map(|value| parse_date("effective_date", value))
                .transpose()?,
            evidence_kind: parse_evidence_kind(&self.evidence_kind)?,
        })
    }
}

fn parse_evidence_kind(value: &str) -> Result<RecoveryEvidenceKind> {
    match value {
        "provider_inventory" => Ok(RecoveryEvidenceKind::ProviderInventory),
        "collection_ledger" => Ok(RecoveryEvidenceKind::CollectionLedger),
        "provider_response_manifest" => Ok(RecoveryEvidenceKind::ProviderResponseManifest),
        "object_path_inference" => Ok(RecoveryEvidenceKind::ObjectPathInference),
        _ => bail!("invalid recovery evidence_kind {value:?}"),
    }
}

fn validate_evidence_artifact(label: &str, artifact: &RecoveryEvidenceArtifact) -> Result<()> {
    if artifact.uri.trim().is_empty() || !is_sha256(&artifact.sha256) {
        bail!("recovery {label} evidence URI and lowercase SHA-256 are required");
    }
    Ok(())
}

fn parse_timestamp(field: &str, value: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| format!("invalid recovery candidate {field}"))
}

fn parse_date(field: &str, value: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .with_context(|| format!("invalid recovery candidate {field}"))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests;
