//! Evidence-driven recovery of missing Bronze Catalog metadata.
//!
//! Existing R2 bytes are not enough to reconstruct Catalog truth. This use case accepts an
//! authoritative provider/collection evidence manifest, verifies every referenced object by
//! bounded-memory rehash through a storage port, and only then records explicit recovery lineage.
//! Object paths are validated but never treated as evidence for snapshot or provider metadata.

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use collection_domain::{
    BronzeObject, CollectionError, IngestionRun, IngestionRunStatus, IngestionTrigger,
    SnapshotBasis, SnapshotGranularity, SourceCatalogEntry,
};
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId};
use foundation_shared_kernel::ObjectKey;
use serde_json::{json, Value as JsonValue};
use uuid::Uuid;

use crate::ports::CompleteIngestionRunCommand;

mod validation;

use validation::{validate_input, validate_observed_object};

/// Whether a recovery invocation only verifies evidence or also records Catalog metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BronzeCatalogRecoveryMode {
    /// Read and verify every object without mutating Catalog.
    DryRun,
    /// Verify every object first, then record source/run/object metadata.
    Apply,
}

/// Authoritative evidence class used to reconstruct source metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryEvidenceKind {
    /// Provider inventory captured as an immutable evidence artifact.
    ProviderInventory,
    /// Foundation collection ledger captured during the original run.
    CollectionLedger,
    /// Provider response manifest captured during the original run.
    ProviderResponseManifest,
    /// Unsupported inference from an object path alone.
    ObjectPathInference,
}

impl RecoveryEvidenceKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderInventory => "provider_inventory",
            Self::CollectionLedger => "collection_ledger",
            Self::ProviderResponseManifest => "provider_response_manifest",
            Self::ObjectPathInference => "object_path_inference",
        }
    }

    const fn is_authoritative(self) -> bool {
        !matches!(self, Self::ObjectPathInference)
    }
}

/// One existing Bronze object plus metadata reconstructed from authoritative evidence.
#[derive(Clone, Debug)]
pub struct BronzeCatalogRecoveryCandidate {
    /// Existing canonical R2 object key.
    pub object_key: ObjectKey,
    /// Size reported by the read-only R2 inventory used to build the recovery manifest.
    pub expected_size_bytes: u64,
    /// Previously captured checksum, when one exists independently of this recovery read.
    pub expected_checksum_sha256: Option<String>,
    /// Provider partition represented by this object.
    pub source_partition_key: Option<String>,
    /// Canonical source coverage identity used for skip, coverage, and dedupe.
    pub source_identity_key: String,
    /// Original provider request/inventory parameters.
    pub request_params: JsonValue,
    /// MIME content type established by source evidence.
    pub content_type: String,
    /// Logical source row count, when the original evidence recorded one.
    pub logical_record_count: Option<u64>,
    /// Opaque R2 `ETag` captured by the read-only inventory that selected this object.
    pub observed_r2_etag: String,
    /// Immutable R2 object's last-modified timestamp, used explicitly as the recovered collection
    /// timestamp rather than pretending the original runtime timestamp survived.
    pub observed_r2_last_modified: DateTime<Utc>,
    /// Human-readable source period bucket, when supplied by the provider.
    pub snapshot_period: Option<String>,
    /// Canonical source as-of date established by evidence.
    pub snapshot_date: NaiveDate,
    /// Granularity of `snapshot_date`.
    pub snapshot_granularity: SnapshotGranularity,
    /// Provenance of `snapshot_date`.
    pub snapshot_basis: SnapshotBasis,
    /// Provider file identifier for bulk sources.
    pub provider_file_id: Option<String>,
    /// Provider file name for bulk sources.
    pub provider_file_name: Option<String>,
    /// Provider update date, when supplied by inventory.
    pub provider_updated_at: Option<NaiveDate>,
    /// Effective date represented by the object, when distinct from snapshot date.
    pub effective_date: Option<NaiveDate>,
    /// Evidence class proving the non-physical metadata above.
    pub evidence_kind: RecoveryEvidenceKind,
}

/// Complete request for one source-scoped Catalog recovery run.
#[derive(Clone, Debug)]
pub struct BronzeCatalogRecoveryInput {
    /// Verification-only or apply mode.
    pub mode: BronzeCatalogRecoveryMode,
    /// Source catalog metadata projected from the endpoint/source SSOT.
    pub source: SourceCatalogEntry,
    /// Stable URI/path of the immutable evidence manifest.
    pub evidence_manifest_uri: String,
    /// Lowercase SHA-256 of the evidence manifest.
    pub evidence_manifest_sha256: String,
    /// Objects explicitly excluded because their semantic evidence is unresolved.
    pub excluded_unresolved_object_count: u64,
    /// UTC time this verification/recovery invocation began.
    pub started_at: DateTime<Utc>,
    /// Existing R2 objects whose metadata can be proven by the evidence manifest.
    pub candidates: Vec<BronzeCatalogRecoveryCandidate>,
}

/// Fingerprint obtained by reading an existing object without writing it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExistingBronzeObject {
    /// Lowercase SHA-256 calculated from the existing bytes.
    pub checksum_sha256: String,
    /// Exact number of bytes read.
    pub size_bytes: u64,
    /// Opaque R2 `ETag` observed by the stable storage read.
    pub observed_r2_etag: String,
    /// Storage timestamp observed for the stable byte read.
    pub observed_r2_last_modified: DateTime<Utc>,
}

/// Read-only storage failure during recovery verification.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct BronzeCatalogRecoveryStorageError(pub String);

/// Read-only port used to fingerprint an existing Bronze object.
#[async_trait]
pub trait BronzeCatalogRecoveryObjectReader: Send + Sync {
    /// Rehashes one existing object with bounded memory.
    ///
    /// Returns `Ok(None)` only when the object does not exist.
    ///
    /// # Errors
    /// Returns [`BronzeCatalogRecoveryStorageError`] when storage cannot be read reliably.
    async fn read_existing_object(
        &self,
        key: &str,
    ) -> Result<Option<ExistingBronzeObject>, BronzeCatalogRecoveryStorageError>;

    /// Reads one complete source-scoped candidate set. Runtime adapters may override this method
    /// with bounded concurrency; the default preserves sequential behavior for simple adapters.
    async fn read_existing_objects(
        &self,
        keys: &[String],
    ) -> Vec<Result<Option<ExistingBronzeObject>, BronzeCatalogRecoveryStorageError>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.read_existing_object(key).await);
        }
        results
    }
}

/// One atomic Catalog mutation containing the source, replay run, recovered objects, and terminal
/// run state.
#[derive(Clone, Debug)]
pub struct ApplyBronzeCatalogRecoveryCommand {
    /// Source metadata proven by the endpoint/source SSOT and evidence manifest.
    pub source: SourceCatalogEntry,
    /// Replay ingestion run describing this recovery operation.
    pub run: IngestionRun,
    /// Existing R2 objects whose Catalog metadata will be recovered.
    pub objects: Vec<BronzeObject>,
    /// Successful terminal state recorded in the same transaction.
    pub completion: CompleteIngestionRunCommand,
}

/// Atomic Catalog write boundary for Bronze metadata recovery.
#[async_trait]
pub trait BronzeCatalogRecoveryCatalogWriter: Send + Sync {
    /// Applies the complete recovery batch in one database transaction.
    ///
    /// The implementation must leave no source, run, or object mutation visible when any part of
    /// the batch fails.
    ///
    /// # Errors
    /// Returns [`CollectionError`] when the transaction cannot be committed.
    async fn apply_recovery(
        &self,
        command: ApplyBronzeCatalogRecoveryCommand,
    ) -> Result<IngestionRun, CollectionError>;
}

/// Summary of a successful verification or recovery invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BronzeCatalogRecoveryReport {
    /// Number of objects whose bytes and metadata evidence were validated.
    pub validated_object_count: u64,
    /// Number of Catalog object rows recorded; always zero in dry-run mode.
    pub applied_object_count: u64,
    /// Total verified bytes.
    pub total_size_bytes: u64,
    /// Objects that remain outside Catalog under the same source-scoped evidence manifest.
    pub excluded_unresolved_object_count: u64,
    /// Replay ingestion run created by apply mode.
    pub ingestion_run_id: Option<IngestionRunId>,
}

/// Failure returned before unproven metadata can enter Catalog.
#[derive(Debug, thiserror::Error)]
pub enum BronzeCatalogRecoveryError {
    /// Manifest- or candidate-level evidence is incomplete or contradictory.
    #[error("invalid Bronze Catalog recovery evidence: {0}")]
    InvalidEvidence(String),
    /// Referenced object is absent from storage.
    #[error("Bronze Catalog recovery object is missing: {key}")]
    ObjectMissing {
        /// Missing object key.
        key: String,
    },
    /// Existing object size differs from the inventory evidence.
    #[error(
        "Bronze Catalog recovery size mismatch at {key}: expected {expected}, observed {observed}"
    )]
    SizeMismatch {
        /// Conflicting object key.
        key: String,
        /// Size in the evidence manifest.
        expected: u64,
        /// Size calculated while reading the object.
        observed: u64,
    },
    /// Existing object checksum differs from an independently captured checksum.
    #[error("Bronze Catalog recovery checksum mismatch at {key}")]
    ChecksumMismatch {
        /// Conflicting object key.
        key: String,
    },
    /// The object changed after the recovery inventory captured its identity.
    #[error("Bronze Catalog recovery object version changed after inventory at {key}: expected ETag {expected_etag} at {expected_last_modified}, observed ETag {observed_etag} at {observed_last_modified}")]
    ObjectVersionMismatch {
        /// Object key whose storage version changed.
        key: String,
        /// `ETag` captured by the recovery inventory.
        expected_etag: String,
        /// `ETag` observed during the stable recovery read.
        observed_etag: String,
        /// Timestamp captured by the recovery inventory.
        expected_last_modified: DateTime<Utc>,
        /// Timestamp observed during the stable recovery read.
        observed_last_modified: DateTime<Utc>,
    },
    /// Storage could not be read reliably.
    #[error("failed to read Bronze object for Catalog recovery: {key}: {source}")]
    Storage {
        /// Object key being read.
        key: String,
        /// Underlying storage error.
        #[source]
        source: BronzeCatalogRecoveryStorageError,
    },
    /// Collection persistence failed after all storage/evidence verification completed.
    #[error("failed to record Bronze recovery metadata: {0}")]
    Persistence(#[from] CollectionError),
}

#[derive(Clone, Debug)]
struct VerifiedRecoveryCandidate {
    candidate: BronzeCatalogRecoveryCandidate,
    object: ExistingBronzeObject,
}

/// Evidence-first Bronze Catalog metadata recovery use case.
#[derive(Clone, Copy, Debug, Default)]
pub struct BronzeCatalogRecoveryService;

impl BronzeCatalogRecoveryService {
    /// Creates a recovery service.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Verifies all candidates before performing any Catalog mutation, then optionally records a
    /// replay run and its recovered object rows.
    ///
    /// # Errors
    /// Returns [`BronzeCatalogRecoveryError`] when evidence is invalid, an object is missing or has
    /// drifted, storage cannot be read, or Catalog persistence fails.
    pub async fn execute<Reader>(
        &self,
        reader: &Reader,
        writer: Option<&dyn BronzeCatalogRecoveryCatalogWriter>,
        input: BronzeCatalogRecoveryInput,
    ) -> Result<BronzeCatalogRecoveryReport, BronzeCatalogRecoveryError>
    where
        Reader: BronzeCatalogRecoveryObjectReader + ?Sized,
    {
        validate_input(&input)?;
        if input.mode == BronzeCatalogRecoveryMode::Apply && writer.is_none() {
            return Err(invalid(
                "apply mode requires an atomic Bronze Catalog recovery writer",
            ));
        }

        let keys = input
            .candidates
            .iter()
            .map(|candidate| candidate.object_key.as_str().to_owned())
            .collect::<Vec<_>>();
        let observed_objects = reader.read_existing_objects(&keys).await;
        if observed_objects.len() != input.candidates.len() {
            return Err(invalid(format!(
                "batch object reader returned {} results for {} candidates",
                observed_objects.len(),
                input.candidates.len()
            )));
        }

        let mut verified = Vec::with_capacity(input.candidates.len());
        let mut total_size_bytes = 0_u64;
        for (candidate, observed) in input.candidates.iter().zip(observed_objects) {
            let key = candidate.object_key.as_str();
            let object = observed
                .map_err(|source| BronzeCatalogRecoveryError::Storage {
                    key: key.to_owned(),
                    source,
                })?
                .ok_or_else(|| BronzeCatalogRecoveryError::ObjectMissing {
                    key: key.to_owned(),
                })?;
            validate_observed_object(candidate, &object)?;
            total_size_bytes =
                total_size_bytes
                    .checked_add(object.size_bytes)
                    .ok_or_else(|| {
                        BronzeCatalogRecoveryError::InvalidEvidence(
                            "verified byte total overflowed u64".to_owned(),
                        )
                    })?;
            verified.push(VerifiedRecoveryCandidate {
                candidate: candidate.clone(),
                object,
            });
        }

        if input.mode == BronzeCatalogRecoveryMode::DryRun {
            return Ok(BronzeCatalogRecoveryReport {
                validated_object_count: verified.len() as u64,
                applied_object_count: 0,
                total_size_bytes,
                excluded_unresolved_object_count: input.excluded_unresolved_object_count,
                ingestion_run_id: None,
            });
        }

        let run = recovery_run(&input, input.source.id, verified.len() as u64);
        let objects = verified
            .iter()
            .map(|item| {
                recovered_bronze_object(
                    &input,
                    &input.source,
                    run.id,
                    &item.candidate,
                    &item.object,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let completion = CompleteIngestionRunCommand {
            id: run.id,
            status: IngestionRunStatus::Succeeded,
            finished_at: Utc::now(),
            logical_records_seen: verified.len() as u64,
            // Recovery adopts existing bytes; it does not claim that this run wrote R2 objects.
            objects_written: 0,
            error_message: None,
        };
        let writer = writer.ok_or_else(|| {
            invalid("apply mode requires an atomic Bronze Catalog recovery writer")
        })?;
        let run = writer
            .apply_recovery(ApplyBronzeCatalogRecoveryCommand {
                source: input.source.clone(),
                run,
                objects,
                completion,
            })
            .await?;

        Ok(BronzeCatalogRecoveryReport {
            validated_object_count: verified.len() as u64,
            applied_object_count: verified.len() as u64,
            total_size_bytes,
            excluded_unresolved_object_count: input.excluded_unresolved_object_count,
            ingestion_run_id: Some(run.id),
        })
    }
}

fn recovery_run(
    input: &BronzeCatalogRecoveryInput,
    source_catalog_id: foundation_shared_kernel::ids::SourceCatalogId,
    object_count: u64,
) -> IngestionRun {
    IngestionRun {
        id: IngestionRunId::new(Uuid::now_v7()),
        source_catalog_id,
        trigger: IngestionTrigger::Replay,
        status: IngestionRunStatus::Running,
        request_params: json!({
            "catalog_recovery": {
                "kind": "evidence_rehydration",
                "evidence_manifest_uri": input.evidence_manifest_uri,
                "evidence_manifest_sha256": input.evidence_manifest_sha256,
                "candidate_object_count": object_count,
                "excluded_unresolved_object_count": input.excluded_unresolved_object_count,
                "r2_write_performed": false
            }
        }),
        started_at: input.started_at,
        finished_at: None,
        logical_records_seen: 0,
        objects_written: 0,
        error_message: None,
        created_at: input.started_at,
        updated_at: input.started_at,
        version: 1,
    }
}

fn recovered_bronze_object(
    input: &BronzeCatalogRecoveryInput,
    source: &SourceCatalogEntry,
    ingestion_run_id: IngestionRunId,
    candidate: &BronzeCatalogRecoveryCandidate,
    existing: &ExistingBronzeObject,
) -> Result<BronzeObject, BronzeCatalogRecoveryError> {
    let mut request_params = candidate
        .request_params
        .as_object()
        .cloned()
        .ok_or_else(|| invalid("request_params must be a JSON object"))?;
    request_params.insert(
        "catalog_recovery".to_owned(),
        json!({
            "kind": "evidence_rehydration",
            "evidence_kind": candidate.evidence_kind.as_str(),
            "evidence_manifest_uri": input.evidence_manifest_uri,
            "evidence_manifest_sha256": input.evidence_manifest_sha256,
            "collected_at_basis": "r2_last_modified",
            "observed_r2_etag": existing.observed_r2_etag,
            "observed_r2_last_modified": existing.observed_r2_last_modified,
            "r2_write_performed": false
        }),
    );

    Ok(BronzeObject {
        id: BronzeObjectId::new(Uuid::now_v7()),
        source_catalog_id: source.id,
        ingestion_run_id,
        source_record_id: None,
        source_partition_key: candidate.source_partition_key.clone(),
        source_identity_key: candidate.source_identity_key.clone(),
        dedupe_key: format!(
            "{}:{}:sha256={}",
            source.slug, candidate.source_identity_key, existing.checksum_sha256
        ),
        request_params: JsonValue::Object(request_params),
        object_key: candidate.object_key.clone(),
        checksum_sha256: existing.checksum_sha256.clone(),
        content_type: candidate.content_type.clone(),
        size_bytes: existing.size_bytes,
        logical_record_count: candidate.logical_record_count,
        collected_at: candidate.observed_r2_last_modified,
        snapshot_period: candidate.snapshot_period.clone(),
        snapshot_date: candidate.snapshot_date,
        snapshot_granularity: candidate.snapshot_granularity,
        snapshot_basis: candidate.snapshot_basis,
        provider_file_id: candidate.provider_file_id.clone(),
        provider_file_name: candidate.provider_file_name.clone(),
        provider_updated_at: candidate.provider_updated_at,
        effective_date: candidate.effective_date,
        created_at: input.started_at,
    })
}

fn invalid(reason: impl Into<String>) -> BronzeCatalogRecoveryError {
    BronzeCatalogRecoveryError::InvalidEvidence(reason.into())
}

#[cfg(test)]
mod tests;
