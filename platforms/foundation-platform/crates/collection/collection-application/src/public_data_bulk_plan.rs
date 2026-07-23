//! Generic planning helpers for immutable public-data bulk files.

use std::fmt::Write as _;

use chrono::{Datelike, NaiveDate};
use collection_domain::{
    build_bronze_object_key, operation_collapses_into_slug, BronzeObjectKeyError,
    BronzeObjectKeyParts, SnapshotBasis, SnapshotGranularity,
};
use foundation_shared_kernel::ids::IngestionRunId;
use foundation_shared_kernel::ObjectKey;
use serde_json::{json, Value as JsonValue};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Provider file identity for one immutable public-data bulk file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicDataBulkFileIdentity {
    /// Provider-neutral operation or dataset name, for example `building_register_main`.
    pub operation: String,
    /// Provider-declared file period such as `2026-05`, when the source publishes one.
    pub provider_file_period: Option<String>,
    /// Provider-declared snapshot date such as `2026-05-20`, when the source publishes one.
    pub provider_snapshot_date: Option<NaiveDate>,
    /// Stable provider file id such as a hub.go.kr `OPN...` file id.
    pub provider_file_id: String,
    /// Provider file name as distributed by the source.
    pub provider_file_name: String,
    /// Provider-declared update date, when the source publishes one.
    pub provider_updated_at: Option<NaiveDate>,
}

/// Input required to plan one immutable public-data bulk file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicDataBulkFilePlanInput<'a> {
    /// Stable lowercase source slug.
    pub source_slug: &'a str,
    /// Ingestion date recorded as metadata; object keys are not partitioned by date.
    pub ingest_date: NaiveDate,
    /// Ingestion run id used in the object key.
    pub ingestion_run_id: IngestionRunId,
    /// Provider file identity.
    pub identity: PublicDataBulkFileIdentity,
    /// Raw provider file bytes, stored unchanged in Bronze.
    pub raw_payload: Vec<u8>,
    /// MIME content type attached to the Bronze object.
    pub content_type: String,
}

/// Input required to plan one immutable public-data bulk file after streaming its bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicDataBulkFileMetadataInput<'a> {
    /// Stable lowercase source slug.
    pub source_slug: &'a str,
    /// Ingestion date recorded as metadata; object keys are not partitioned by date.
    pub ingest_date: NaiveDate,
    /// Ingestion run id used in the object key.
    pub ingestion_run_id: IngestionRunId,
    /// Provider file identity.
    pub identity: PublicDataBulkFileIdentity,
    /// Lowercase SHA-256 checksum of the raw payload.
    pub checksum_sha256: String,
    /// Raw payload size in bytes.
    pub size_bytes: u64,
    /// MIME content type attached to the Bronze object.
    pub content_type: String,
}

/// Input required to plan where one immutable public-data bulk file will be stored.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicDataBulkFileStorageLocationInput<'a> {
    /// Stable lowercase source slug.
    pub source_slug: &'a str,
    /// Ingestion date recorded as metadata; object keys are not partitioned by date.
    pub ingest_date: NaiveDate,
    /// Ingestion run id used in the object key.
    pub ingestion_run_id: IngestionRunId,
    /// Provider file identity.
    pub identity: PublicDataBulkFileIdentity,
}

/// Input required to build a stable provider partition key without a provider file name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublicDataBulkFileSourcePartitionKeyInput<'a> {
    /// Provider-neutral operation or dataset name, for example `building_register_main`.
    pub operation: &'a str,
    /// Stable provider file id such as a hub.go.kr `OPN...` file id.
    pub provider_file_id: &'a str,
}

/// Planned object location for one immutable public-data bulk file before bytes are streamed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicDataBulkFileStorageLocationPlan {
    /// Provider-neutral object key for the raw Bronze payload.
    pub object_key: ObjectKey,
    /// Canonical source coverage identity for skip / coverage / dedupe.
    pub source_identity_key: String,
    /// Provider partition represented by the file.
    pub source_partition_key: String,
    /// Human-readable source period bucket.
    pub snapshot_period: Option<String>,
    /// Canonical source as-of date.
    pub snapshot_date: NaiveDate,
    /// Granularity of [`snapshot_date`](Self::snapshot_date).
    pub snapshot_granularity: SnapshotGranularity,
    /// Provenance of [`snapshot_date`](Self::snapshot_date).
    pub snapshot_basis: SnapshotBasis,
}

/// Planned metadata for one immutable public-data bulk file after streaming its bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicDataBulkFileMetadataPlan {
    /// Provider-neutral object key for the raw Bronze payload.
    pub object_key: ObjectKey,
    /// Canonical source coverage identity for skip / coverage / dedupe.
    pub source_identity_key: String,
    /// Provider partition represented by the file.
    pub source_partition_key: String,
    /// Idempotency key scoped to the source catalog entry.
    pub dedupe_key: String,
    /// Lowercase SHA-256 checksum of the raw payload.
    pub checksum_sha256: String,
    /// Raw payload size in bytes.
    pub size_bytes: u64,
    /// Content type attached to the Bronze object.
    pub content_type: String,
    /// Provider identity parameters stored with the Bronze object metadata.
    pub request_params: JsonValue,
    /// Human-readable source period bucket.
    pub snapshot_period: Option<String>,
    /// Canonical source as-of date.
    pub snapshot_date: NaiveDate,
    /// Granularity of [`snapshot_date`](Self::snapshot_date).
    pub snapshot_granularity: SnapshotGranularity,
    /// Provenance of [`snapshot_date`](Self::snapshot_date).
    pub snapshot_basis: SnapshotBasis,
}

/// Planned metadata and bytes for one immutable public-data bulk file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicDataBulkFilePlan {
    /// Provider-neutral object key for the raw Bronze payload.
    pub object_key: ObjectKey,
    /// Canonical source coverage identity for skip / coverage / dedupe.
    pub source_identity_key: String,
    /// Provider partition represented by the file.
    pub source_partition_key: String,
    /// Idempotency key scoped to the source catalog entry.
    pub dedupe_key: String,
    /// Lowercase SHA-256 checksum of the raw payload.
    pub checksum_sha256: String,
    /// Raw payload size in bytes.
    pub size_bytes: u64,
    /// Content type attached to the Bronze object.
    pub content_type: String,
    /// Provider identity parameters stored with the Bronze object metadata.
    pub request_params: JsonValue,
    /// Human-readable source period bucket.
    pub snapshot_period: Option<String>,
    /// Canonical source as-of date.
    pub snapshot_date: NaiveDate,
    /// Granularity of [`snapshot_date`](Self::snapshot_date).
    pub snapshot_granularity: SnapshotGranularity,
    /// Provenance of [`snapshot_date`](Self::snapshot_date).
    pub snapshot_basis: SnapshotBasis,
    /// Raw payload bytes to write to object storage.
    pub raw_payload: Vec<u8>,
}

/// Error returned while planning a public-data bulk file.
#[derive(Debug, Error)]
pub enum PublicDataBulkFilePlanError {
    /// The canonical Bronze object key could not be built.
    #[error(transparent)]
    ObjectKey(#[from] BronzeObjectKeyError),
    /// A provider file identity field is invalid.
    #[error("invalid public-data bulk file identity {field}: {reason}")]
    InvalidIdentity {
        /// Field that failed validation.
        field: &'static str,
        /// Human-readable validation failure.
        reason: String,
    },
}

/// Plans object metadata for one immutable public-data bulk file.
///
/// # Errors
///
/// Returns `PublicDataBulkFilePlanError` when provider identity or key parts are invalid.
pub fn plan_public_data_bulk_file(
    input: PublicDataBulkFilePlanInput<'_>,
) -> Result<PublicDataBulkFilePlan, PublicDataBulkFilePlanError> {
    let raw_payload = input.raw_payload;
    let checksum_sha256 = sha256_hex(&raw_payload);
    let size_bytes = raw_payload.len() as u64;

    let metadata_plan = plan_public_data_bulk_file_metadata(PublicDataBulkFileMetadataInput {
        source_slug: input.source_slug,
        ingest_date: input.ingest_date,
        ingestion_run_id: input.ingestion_run_id,
        identity: input.identity,
        checksum_sha256,
        size_bytes,
        content_type: input.content_type,
    })?;

    Ok(PublicDataBulkFilePlan {
        object_key: metadata_plan.object_key,
        source_identity_key: metadata_plan.source_identity_key,
        source_partition_key: metadata_plan.source_partition_key,
        dedupe_key: metadata_plan.dedupe_key,
        checksum_sha256: metadata_plan.checksum_sha256,
        size_bytes: metadata_plan.size_bytes,
        content_type: metadata_plan.content_type,
        request_params: metadata_plan.request_params,
        snapshot_period: metadata_plan.snapshot_period,
        snapshot_date: metadata_plan.snapshot_date,
        snapshot_granularity: metadata_plan.snapshot_granularity,
        snapshot_basis: metadata_plan.snapshot_basis,
        raw_payload,
    })
}

/// Plans object metadata for one immutable public-data bulk file after streaming its bytes.
///
/// # Errors
///
/// Returns `PublicDataBulkFilePlanError` when provider identity, checksum, or key parts are invalid.
pub fn plan_public_data_bulk_file_metadata(
    input: PublicDataBulkFileMetadataInput<'_>,
) -> Result<PublicDataBulkFileMetadataPlan, PublicDataBulkFilePlanError> {
    validate_content_type(&input.content_type)?;
    validate_checksum_sha256(&input.checksum_sha256)?;
    let location =
        plan_public_data_bulk_file_storage_location(&PublicDataBulkFileStorageLocationInput {
            source_slug: input.source_slug,
            ingest_date: input.ingest_date,
            ingestion_run_id: input.ingestion_run_id,
            identity: input.identity.clone(),
        })?;
    let dedupe_key = public_data_bulk_file_dedupe_key(
        input.source_slug,
        &location.source_identity_key,
        &input.checksum_sha256,
    );
    let request_params = public_data_bulk_file_request_params(&input.identity);

    Ok(PublicDataBulkFileMetadataPlan {
        object_key: location.object_key,
        source_identity_key: location.source_identity_key,
        source_partition_key: location.source_partition_key,
        dedupe_key,
        checksum_sha256: input.checksum_sha256,
        size_bytes: input.size_bytes,
        content_type: input.content_type,
        request_params,
        snapshot_period: location.snapshot_period,
        snapshot_date: location.snapshot_date,
        snapshot_granularity: location.snapshot_granularity,
        snapshot_basis: location.snapshot_basis,
    })
}

/// Builds the bulk dedupe key `<slug>:<source_identity_key>:sha256=<checksum>`.
///
/// The single source for the bulk dedupe-key shape: the in-memory metadata plan and the streaming
/// commit path (which knows the checksum only after the stream) both derive it from here so they
/// cannot drift.
#[must_use]
pub fn public_data_bulk_file_dedupe_key(
    source_slug: &str,
    source_identity_key: &str,
    checksum_sha256: &str,
) -> String {
    format!("{source_slug}:{source_identity_key}:sha256={checksum_sha256}")
}

/// Builds the `request_params` JSON stored on a bulk Bronze object's metadata row from its identity.
///
/// Depends only on the provider file identity (not the streamed bytes), so the streaming commit
/// path can build the same `request_params` before the checksum is known. The single source for the
/// bulk `request_params` shape.
#[must_use]
pub fn public_data_bulk_file_request_params(identity: &PublicDataBulkFileIdentity) -> JsonValue {
    json!({
        "operation": identity.operation,
        "provider_file_period": identity.provider_file_period,
        "provider_snapshot_date": identity.provider_snapshot_date.map(|date| date.to_string()),
        "provider_file_id": identity.provider_file_id,
        "provider_file_name": identity.provider_file_name,
        "provider_updated_at": identity.provider_updated_at.map(|date| date.to_string()),
        "raw_preserved": true
    })
}

/// Plans the object location for one immutable public-data bulk file before streaming bytes.
///
/// # Errors
///
/// Returns `PublicDataBulkFilePlanError` when provider identity or key parts are invalid.
pub fn plan_public_data_bulk_file_storage_location(
    input: &PublicDataBulkFileStorageLocationInput<'_>,
) -> Result<PublicDataBulkFileStorageLocationPlan, PublicDataBulkFilePlanError> {
    validate_identity(&input.identity)?;
    let snapshot = snapshot_metadata(&input.identity, input.ingest_date)?;
    let extension = extension_from_provider_file_name(&input.identity.provider_file_name)?;
    let source_partition_key =
        public_data_bulk_file_source_partition_key(PublicDataBulkFileSourcePartitionKeyInput {
            operation: &input.identity.operation,
            provider_file_id: &input.identity.provider_file_id,
        })?;
    let source_identity_key = public_data_bulk_file_source_identity_key(&input.identity)?;

    // ADR 0019: snapshot dates are catalog metadata, not physical identity. A leading
    // `operation=` segment is kept only when the operation is not already represented by the
    // source slug.
    let mut partition_path = String::new();
    if !operation_collapses_into_slug(&input.identity.operation, input.source_slug) {
        partition_path.push_str("operation=");
        partition_path.push_str(&input.identity.operation);
    }

    let object_key = build_bronze_object_key(BronzeObjectKeyParts {
        source_slug: input.source_slug,
        partition_path: &partition_path,
        leaf_name: &input.identity.provider_file_id,
        extension: &extension,
    })?;

    Ok(PublicDataBulkFileStorageLocationPlan {
        object_key,
        source_identity_key,
        source_partition_key,
        snapshot_period: Some(snapshot.period),
        snapshot_date: snapshot.date,
        snapshot_granularity: snapshot.granularity,
        snapshot_basis: snapshot.basis,
    })
}

/// Builds the canonical source coverage identity for one immutable bulk source file.
///
/// # Errors
/// Returns `PublicDataBulkFilePlanError` when provider identity fields are invalid.
pub fn public_data_bulk_file_source_identity_key(
    identity: &PublicDataBulkFileIdentity,
) -> Result<String, PublicDataBulkFilePlanError> {
    validate_object_key_value("provider_file_id", &identity.provider_file_id)?;
    Ok(format!("provider_file_id={}", identity.provider_file_id))
}

/// Builds the stable provider partition key used to identify one immutable bulk source file.
///
/// # Errors
///
/// Returns `PublicDataBulkFilePlanError` when provider identity fields are invalid.
pub fn public_data_bulk_file_source_partition_key(
    input: PublicDataBulkFileSourcePartitionKeyInput<'_>,
) -> Result<String, PublicDataBulkFilePlanError> {
    validate_identifier("operation", input.operation)?;
    validate_object_key_value("provider_file_id", input.provider_file_id)?;
    Ok(source_partition_key(input))
}

fn source_partition_key(input: PublicDataBulkFileSourcePartitionKeyInput<'_>) -> String {
    format!(
        "operation={}/provider_file_id={}",
        input.operation, input.provider_file_id
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BulkSnapshotMetadata {
    period: String,
    date: NaiveDate,
    granularity: SnapshotGranularity,
    basis: SnapshotBasis,
}

fn snapshot_metadata(
    identity: &PublicDataBulkFileIdentity,
    ingest_date: NaiveDate,
) -> Result<BulkSnapshotMetadata, PublicDataBulkFilePlanError> {
    if let Some(provider_snapshot_date) = identity.provider_snapshot_date {
        return Ok(BulkSnapshotMetadata {
            period: month_bucket(provider_snapshot_date),
            date: provider_snapshot_date,
            granularity: SnapshotGranularity::Day,
            basis: SnapshotBasis::ProviderSnapshotDate,
        });
    }

    if let Some(provider_file_period) = &identity.provider_file_period {
        return Ok(BulkSnapshotMetadata {
            period: provider_file_period.clone(),
            date: first_day_of_period(provider_file_period)?,
            granularity: SnapshotGranularity::Month,
            basis: SnapshotBasis::ProviderFilePeriod,
        });
    }

    if let Some(provider_updated_at) = identity.provider_updated_at {
        return Ok(BulkSnapshotMetadata {
            period: month_bucket(provider_updated_at),
            date: provider_updated_at,
            granularity: SnapshotGranularity::Day,
            basis: SnapshotBasis::ProviderUpdatedAt,
        });
    }

    Ok(BulkSnapshotMetadata {
        period: month_bucket(ingest_date),
        date: ingest_date,
        granularity: SnapshotGranularity::Day,
        basis: SnapshotBasis::CollectedAtFallback,
    })
}

fn month_bucket(date: NaiveDate) -> String {
    format!("{}-{:02}", date.year(), date.month())
}

fn first_day_of_period(period: &str) -> Result<NaiveDate, PublicDataBulkFilePlanError> {
    if period.len() != 7 || period.as_bytes().get(4) != Some(&b'-') {
        return Err(invalid_identity(
            "provider_file_period",
            "must be in YYYY-MM form",
        ));
    }
    let year = period[0..4]
        .parse::<i32>()
        .map_err(|_| invalid_identity("provider_file_period", "year must be numeric"))?;
    let month = period[5..7]
        .parse::<u32>()
        .map_err(|_| invalid_identity("provider_file_period", "month must be numeric"))?;
    NaiveDate::from_ymd_opt(year, month, 1).ok_or_else(|| {
        invalid_identity(
            "provider_file_period",
            "must contain a valid calendar month",
        )
    })
}

fn validate_identity(
    identity: &PublicDataBulkFileIdentity,
) -> Result<(), PublicDataBulkFilePlanError> {
    validate_identifier("operation", &identity.operation)?;
    if let Some(provider_file_period) = &identity.provider_file_period {
        first_day_of_period(provider_file_period)?;
        validate_object_key_value("provider_file_period", provider_file_period)?;
    }
    validate_object_key_value("provider_file_id", &identity.provider_file_id)?;
    validate_provider_file_name(&identity.provider_file_name)?;
    Ok(())
}

fn validate_identifier(
    field: &'static str,
    value: &str,
) -> Result<(), PublicDataBulkFilePlanError> {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Ok(());
    }
    Err(invalid_identity(
        field,
        "must contain only ASCII letters, digits, '_' and '-'",
    ))
}

fn validate_object_key_value(
    field: &'static str,
    value: &str,
) -> Result<(), PublicDataBulkFilePlanError> {
    if value.is_empty() {
        return Err(invalid_identity(field, "must not be empty"));
    }
    if value.trim() != value {
        return Err(invalid_identity(
            field,
            "must not contain leading or trailing whitespace",
        ));
    }
    if value.contains('/') || value.contains('\\') || value.contains("..") {
        return Err(invalid_identity(
            field,
            "must not contain path separators or traversal markers",
        ));
    }
    Ok(())
}

fn validate_provider_file_name(value: &str) -> Result<(), PublicDataBulkFilePlanError> {
    validate_object_key_value("provider_file_name", value)?;
    if value.rsplit_once('.').is_none() {
        return Err(invalid_identity(
            "provider_file_name",
            "must include a file extension",
        ));
    }
    Ok(())
}

fn validate_content_type(value: &str) -> Result<(), PublicDataBulkFilePlanError> {
    if value.is_empty() {
        return Err(invalid_identity("content_type", "must not be empty"));
    }
    if value.trim() != value || value.contains('\r') || value.contains('\n') {
        return Err(invalid_identity(
            "content_type",
            "must not contain leading/trailing whitespace or line breaks",
        ));
    }
    Ok(())
}

fn validate_checksum_sha256(value: &str) -> Result<(), PublicDataBulkFilePlanError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Ok(());
    }
    Err(invalid_identity(
        "checksum_sha256",
        "must be a lowercase 64-character SHA-256 hex digest",
    ))
}

fn extension_from_provider_file_name(
    file_name: &str,
) -> Result<String, PublicDataBulkFilePlanError> {
    let (_, extension) = file_name
        .rsplit_once('.')
        .ok_or_else(|| invalid_identity("provider_file_name", "must include a file extension"))?;
    let normalized = extension.to_ascii_lowercase();
    validate_identifier("provider_file_extension", &normalized)?;
    Ok(normalized)
}

fn invalid_identity(field: &'static str, reason: &str) -> PublicDataBulkFilePlanError {
    PublicDataBulkFilePlanError::InvalidIdentity {
        field,
        reason: reason.to_owned(),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut checksum, byte| {
            let _ = write!(&mut checksum, "{byte:02x}");
            checksum
        })
}
