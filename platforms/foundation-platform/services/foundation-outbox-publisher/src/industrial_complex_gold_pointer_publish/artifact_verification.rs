//! The check that a pointer's claims are true of the object it points at.
//!
//! `catalog.industrial_complex_gold_pointer` records a checksum, a size and a version for an object
//! it does not itself hold. Every one of those values used to arrive as an environment variable
//! typed by an operator reading an export summary, and nothing compared them to the bytes. A pointer
//! could therefore name an object that does not exist, or name a real object while describing a
//! different one, and the publish would succeed either way.
//!
//! `VerifiedGoldProfileArtifact` closes that by construction: its fields are private, its only
//! constructor reads the object, and `PublishIndustrialComplexGoldPointerInput` is only ever built
//! from one. Inside this service there is no expression that publishes an unverified pointer.
//!
//! The check is not only "these bytes hash to that value". The stored document states which complex
//! it describes and which artifact it is, so a summary row copied against the wrong complex — the
//! exact accident hand-transcription produces — is refused rather than stored.

use anyhow::{ensure, Context};
use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::{ComplexId, LakehouseComplexId};
use lakehouse_application::PublishIndustrialComplexGoldPointerInput;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::industrial_complex_gold_profile_store::ProfileObjectStore;
use crate::r2_layout::is_industrial_complex_gold_profile_key;

/// The subset of the stored profile document this check reads back.
///
/// Deliberately not the whole document: this module asserts identity, not content. The attributes
/// are the export's business and re-stating their shape here would be a second copy of that schema.
#[derive(Debug, Deserialize)]
struct StoredProfileIdentity {
    artifact_id: String,
    complex_id: String,
}

/// One Gold profile object whose stored bytes were read and found to match the pointer's claims.
///
/// Construct only through [`VerifiedGoldProfileArtifact::verify`]. The private fields are the whole
/// point: a value of this type is evidence that the read happened.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VerifiedGoldProfileArtifact {
    lakehouse_complex_id: LakehouseComplexId,
    current_version: String,
    object_key: String,
    checksum_sha256: String,
    size_bytes: u64,
}

/// What a caller claims about an artifact, before anything has been read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClaimedGoldProfileArtifact {
    /// Lakehouse identity stated by the Gold artifact.
    pub(crate) lakehouse_complex_id: LakehouseComplexId,
    /// Artifact version the pointer will publish as current.
    pub(crate) current_version: String,
    /// Object key the pointer will name.
    pub(crate) object_key: String,
    /// SHA-256 the pointer will record.
    pub(crate) checksum_sha256: String,
    /// Size in bytes the pointer will record.
    pub(crate) size_bytes: u64,
}

impl VerifiedGoldProfileArtifact {
    /// Reads the stored object and confirms every claim the pointer would make about it.
    ///
    /// # Errors
    /// Returns an error when the key is not canonical, the object is absent, or its bytes, size,
    /// artifact identity or complex identity differ from the claim.
    pub(crate) async fn verify(
        store: &ProfileObjectStore,
        claim: ClaimedGoldProfileArtifact,
    ) -> anyhow::Result<Self> {
        ensure!(
            is_industrial_complex_gold_profile_key(claim.object_key.as_str()),
            "{} is not a canonical industrial-complex Gold profile key",
            claim.object_key
        );

        let stored = store
            .read_bytes(claim.object_key.as_str())
            .await
            .with_context(|| {
                format!(
                    "the pointer for complex {} names {}, which could not be read",
                    claim.lakehouse_complex_id.as_uuid(),
                    claim.object_key
                )
            })?;

        let checksum = format!("{:x}", Sha256::digest(&stored));
        ensure!(
            checksum == claim.checksum_sha256,
            "profile object {} hashes to {checksum} but the pointer claims {}",
            claim.object_key,
            claim.checksum_sha256
        );

        let size_bytes = u64::try_from(stored.len()).context("profile object size overflow")?;
        ensure!(
            size_bytes == claim.size_bytes,
            "profile object {} is {size_bytes} bytes but the pointer claims {}",
            claim.object_key,
            claim.size_bytes
        );

        let identity: StoredProfileIdentity =
            serde_json::from_slice(&stored).with_context(|| {
                format!(
                    "profile object {} is not a readable profile document",
                    claim.object_key
                )
            })?;
        ensure!(
            identity.artifact_id == claim.current_version,
            "profile object {} states artifact {} but the pointer publishes version {}",
            claim.object_key,
            identity.artifact_id,
            claim.current_version
        );
        let stored_complex_id = claim.lakehouse_complex_id.as_uuid().to_string();
        ensure!(
            identity.complex_id == stored_complex_id,
            "profile object {} describes complex {} but the pointer is for {stored_complex_id}",
            claim.object_key,
            identity.complex_id
        );

        Ok(Self {
            lakehouse_complex_id: claim.lakehouse_complex_id,
            current_version: claim.current_version,
            object_key: claim.object_key,
            checksum_sha256: claim.checksum_sha256,
            size_bytes: claim.size_bytes,
        })
    }

    /// Industrial complex this artifact describes.
    pub(crate) const fn lakehouse_complex_id(&self) -> LakehouseComplexId {
        self.lakehouse_complex_id
    }

    /// Object key that was read.
    pub(crate) fn object_key(&self) -> &str {
        self.object_key.as_str()
    }

    /// Builds the publish input for this verified artifact.
    ///
    /// The only constructor of `PublishIndustrialComplexGoldPointerInput` in this service, so the
    /// artifact-describing fields cannot be supplied by anything that did not read the object.
    pub(crate) fn into_publish_input(
        self,
        complex_id: ComplexId,
        publication: PointerPublication,
    ) -> PublishIndustrialComplexGoldPointerInput {
        PublishIndustrialComplexGoldPointerInput {
            complex_id,
            current_version: self.current_version,
            expected_current_version: publication.expected_current_version,
            profile_object_key: self.object_key,
            profile_url_template: publication.profile_url_template,
            spatial_locator_object_key: publication.spatial_locator_object_key,
            source: publication.source,
            source_url: publication.source_url,
            source_external_id: publication.source_external_id,
            source_snapshot_id: publication.source_snapshot_id,
            iceberg_snapshot_id: publication.iceberg_snapshot_id,
            profile_row_count: publication.profile_row_count,
            profile_size_bytes: self.size_bytes,
            spatial_locator_size_bytes: publication.spatial_locator_size_bytes,
            profile_checksum_sha256: self.checksum_sha256,
            published_at: publication.published_at,
        }
    }
}

/// The publication-side facts, which describe the act of pointing rather than the object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PointerPublication {
    /// Active version observed before this publication, for stale-write detection.
    pub(crate) expected_current_version: Option<String>,
    /// Address template consumers substitute this pointer's object keys into (root ADR-0037).
    pub(crate) profile_url_template: String,
    /// Optional spatial locator object key.
    pub(crate) spatial_locator_object_key: Option<String>,
    /// Source system or pipeline name.
    pub(crate) source: String,
    /// Optional source URL.
    pub(crate) source_url: Option<String>,
    /// Optional source-side publication id.
    pub(crate) source_external_id: Option<String>,
    /// Source snapshot the artifact represents.
    pub(crate) source_snapshot_id: String,
    /// Gold Iceberg snapshot the artifact was scanned from.
    pub(crate) iceberg_snapshot_id: String,
    /// Complexes the profile document describes; always one (root ADR-0036 decision 4).
    pub(crate) profile_row_count: u64,
    /// Optional spatial locator size in bytes.
    pub(crate) spatial_locator_size_bytes: Option<u64>,
    /// UTC publication time.
    pub(crate) published_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests;
