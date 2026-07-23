//! Bronze ingestion metadata for raw public-data objects.

use std::fmt::Write as _;

use chrono::{DateTime, NaiveDate, Utc};
use foundation_shared_kernel::ids::{
    BronzeObjectId, IngestionRunId, SchemaProfileId, SourceCatalogId, SourceRecordId,
};
use foundation_shared_kernel::{ObjectKey, ObjectKeyError};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

/// Authentication model required by a source dataset.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SourceAuthKind {
    /// No authentication is required.
    NoAuth,
    /// Public-data service key authentication is required.
    ServiceKey,
    /// `OAuth2` authentication is required.
    OAuth2,
    /// Credentials or files are handled manually outside automated ingestion.
    Manual,
}

impl SourceAuthKind {
    /// Returns the stable wire value stored in `PostgreSQL`.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::NoAuth => "none",
            Self::ServiceKey => "service_key",
            Self::OAuth2 => "oauth2",
            Self::Manual => "manual",
        }
    }

    /// Parses a stable wire value into a source authentication kind.
    ///
    /// # Errors
    /// Returns `ParseSourceAuthKindError::Unknown` for unsupported wire values.
    pub fn from_wire(raw: &str) -> Result<Self, ParseSourceAuthKindError> {
        match raw {
            "none" => Ok(Self::NoAuth),
            "service_key" => Ok(Self::ServiceKey),
            "oauth2" => Ok(Self::OAuth2),
            "manual" => Ok(Self::Manual),
            other => Err(ParseSourceAuthKindError::Unknown(other.to_owned())),
        }
    }
}

/// Error returned while parsing source authentication kinds.
#[derive(Debug, Error)]
pub enum ParseSourceAuthKindError {
    /// Unsupported wire value.
    #[error("unknown SourceAuthKind wire value: {0:?}")]
    Unknown(String),
}

/// Payload format returned by a source dataset.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SourcePayloadFormat {
    /// JSON payload.
    Json,
    /// XML payload.
    Xml,
    /// CSV payload.
    Csv,
    /// ZIP archive payload.
    Zip,
    /// HTML document payload.
    Html,
    /// Binary payload without a more specific format.
    Binary,
    /// Unknown or mixed payload format.
    Unknown,
}

impl SourcePayloadFormat {
    /// Returns the stable wire value stored in `PostgreSQL`.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Xml => "xml",
            Self::Csv => "csv",
            Self::Zip => "zip",
            Self::Html => "html",
            Self::Binary => "binary",
            Self::Unknown => "unknown",
        }
    }

    /// Parses a stable wire value into a payload format.
    ///
    /// # Errors
    /// Returns `ParseSourcePayloadFormatError::Unknown` for unsupported wire values.
    pub fn from_wire(raw: &str) -> Result<Self, ParseSourcePayloadFormatError> {
        match raw {
            "json" => Ok(Self::Json),
            "xml" => Ok(Self::Xml),
            "csv" => Ok(Self::Csv),
            "zip" => Ok(Self::Zip),
            "html" => Ok(Self::Html),
            "binary" => Ok(Self::Binary),
            "unknown" => Ok(Self::Unknown),
            other => Err(ParseSourcePayloadFormatError::Unknown(other.to_owned())),
        }
    }
}

/// Error returned while parsing source payload formats.
#[derive(Debug, Error)]
pub enum ParseSourcePayloadFormatError {
    /// Unsupported wire value.
    #[error("unknown SourcePayloadFormat wire value: {0:?}")]
    Unknown(String),
}

/// Trigger that started an ingestion run.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IngestionTrigger {
    /// Human-triggered collection.
    Manual,
    /// Scheduler-triggered collection.
    Scheduled,
    /// Historical backfill collection.
    Backfill,
    /// Replay of an existing source snapshot or known input set.
    Replay,
    /// Test collection that should not be treated as production source coverage.
    Test,
}

impl IngestionTrigger {
    /// Returns the stable wire value stored in `PostgreSQL`.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Scheduled => "scheduled",
            Self::Backfill => "backfill",
            Self::Replay => "replay",
            Self::Test => "test",
        }
    }

    /// Parses a stable wire value into an ingestion trigger.
    ///
    /// # Errors
    /// Returns `ParseIngestionTriggerError::Unknown` for unsupported wire values.
    pub fn from_wire(raw: &str) -> Result<Self, ParseIngestionTriggerError> {
        match raw {
            "manual" => Ok(Self::Manual),
            "scheduled" => Ok(Self::Scheduled),
            "backfill" => Ok(Self::Backfill),
            "replay" => Ok(Self::Replay),
            "test" => Ok(Self::Test),
            other => Err(ParseIngestionTriggerError::Unknown(other.to_owned())),
        }
    }
}

/// Error returned while parsing ingestion triggers.
#[derive(Debug, Error)]
pub enum ParseIngestionTriggerError {
    /// Unsupported wire value.
    #[error("unknown IngestionTrigger wire value: {0:?}")]
    Unknown(String),
}

/// Lifecycle status of an ingestion run.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IngestionRunStatus {
    /// Run has been planned but has not started work.
    Planned,
    /// Run is currently collecting source data.
    Running,
    /// Run completed successfully.
    Succeeded,
    /// Run ended with an error.
    Failed,
    /// Run was cancelled before normal completion.
    Cancelled,
}

impl IngestionRunStatus {
    /// Returns the stable wire value stored in `PostgreSQL`.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Parses a stable wire value into an ingestion status.
    ///
    /// # Errors
    /// Returns `ParseIngestionRunStatusError::Unknown` for unsupported wire values.
    pub fn from_wire(raw: &str) -> Result<Self, ParseIngestionRunStatusError> {
        match raw {
            "planned" => Ok(Self::Planned),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(ParseIngestionRunStatusError::Unknown(other.to_owned())),
        }
    }
}

/// Error returned while parsing ingestion run statuses.
#[derive(Debug, Error)]
pub enum ParseIngestionRunStatusError {
    /// Unsupported wire value.
    #[error("unknown IngestionRunStatus wire value: {0:?}")]
    Unknown(String),
}

/// Observed JSON-like field type in a sampled source payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SchemaObservedType {
    /// Null value.
    Null,
    /// Boolean value.
    Boolean,
    /// Numeric value.
    Number,
    /// String value.
    String,
    /// Object value.
    Object,
    /// Array value.
    Array,
    /// Multiple incompatible values were observed for the same field path.
    Mixed,
}

impl SchemaObservedType {
    /// Returns the stable wire value stored in `PostgreSQL`.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Boolean => "boolean",
            Self::Number => "number",
            Self::String => "string",
            Self::Object => "object",
            Self::Array => "array",
            Self::Mixed => "mixed",
        }
    }

    /// Parses a stable wire value into an observed schema type.
    ///
    /// # Errors
    /// Returns `ParseSchemaObservedTypeError::Unknown` for unsupported wire values.
    pub fn from_wire(raw: &str) -> Result<Self, ParseSchemaObservedTypeError> {
        match raw {
            "null" => Ok(Self::Null),
            "boolean" => Ok(Self::Boolean),
            "number" => Ok(Self::Number),
            "string" => Ok(Self::String),
            "object" => Ok(Self::Object),
            "array" => Ok(Self::Array),
            "mixed" => Ok(Self::Mixed),
            other => Err(ParseSchemaObservedTypeError::Unknown(other.to_owned())),
        }
    }
}

/// Error returned while parsing observed schema types.
#[derive(Debug, Error)]
pub enum ParseSchemaObservedTypeError {
    /// Unsupported wire value.
    #[error("unknown SchemaObservedType wire value: {0:?}")]
    Unknown(String),
}

/// Registered source dataset that foundation-platform can collect into Bronze storage.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceCatalogEntry {
    /// Stable foundation-platform source catalog identifier.
    pub id: SourceCatalogId,
    /// Stable lowercase source slug used by operators and ingestion jobs.
    pub slug: String,
    /// Human-readable source name.
    pub name: String,
    /// Source provider or agency.
    pub provider: String,
    /// Provider-side dataset name.
    pub dataset_name: String,
    /// Optional base URL for automated collection.
    pub base_url: Option<String>,
    /// Authentication model required by the source.
    pub auth_kind: SourceAuthKind,
    /// Payload format returned by the source.
    pub payload_format: SourcePayloadFormat,
    /// Optional license name.
    pub license_name: Option<String>,
    /// Optional license URL.
    pub license_url: Option<String>,
    /// Optional terms-of-use URL.
    pub terms_url: Option<String>,
    /// Optional expected collection frequency.
    pub collection_frequency: Option<String>,
    /// Whether this source is active for new ingestion runs.
    pub is_active: bool,
    /// UTC timestamp when the source catalog entry was created.
    pub created_at: DateTime<Utc>,
    /// UTC timestamp when the source catalog entry was last updated.
    pub updated_at: DateTime<Utc>,
    /// Monotonic version used for optimistic concurrency and audit.
    pub version: i64,
}

/// One collection attempt for a source catalog entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IngestionRun {
    /// Stable foundation-platform ingestion run identifier.
    pub id: IngestionRunId,
    /// Source catalog entry collected by this run.
    pub source_catalog_id: SourceCatalogId,
    /// Trigger that started the run.
    pub trigger: IngestionTrigger,
    /// Current lifecycle status.
    pub status: IngestionRunStatus,
    /// Request parameters used by the collector.
    pub request_params: JsonValue,
    /// UTC timestamp when collection started.
    pub started_at: DateTime<Utc>,
    /// Optional UTC timestamp when collection ended.
    pub finished_at: Option<DateTime<Utc>>,
    /// Number of logical source records observed by the collector, when countable.
    pub logical_records_seen: u64,
    /// Number of R2 objects written to Bronze storage.
    pub objects_written: u64,
    /// Optional failure message for failed or cancelled runs.
    pub error_message: Option<String>,
    /// UTC timestamp when the run row was created.
    pub created_at: DateTime<Utc>,
    /// UTC timestamp when the run row was last updated.
    pub updated_at: DateTime<Utc>,
    /// Monotonic version used for optimistic concurrency and audit.
    pub version: i64,
}

/// Immutable metadata for one Bronze R2 object or object batch.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BronzeObject {
    /// Stable foundation-platform Bronze object identifier.
    pub id: BronzeObjectId,
    /// Source catalog entry that produced the object.
    pub source_catalog_id: SourceCatalogId,
    /// Ingestion run that first recorded this object.
    pub ingestion_run_id: IngestionRunId,
    /// Optional normalized Catalog source record created from this object.
    pub source_record_id: Option<SourceRecordId>,
    /// Optional provider-side partition key represented by the object.
    pub source_partition_key: Option<String>,
    /// Canonical source coverage identity used for skip, coverage, and dedupe.
    pub source_identity_key: String,
    /// Caller-provided idempotency key scoped to the source catalog entry.
    pub dedupe_key: String,
    /// Request parameters that produced this object.
    pub request_params: JsonValue,
    /// Provider-neutral object key for the Bronze payload.
    pub object_key: ObjectKey,
    /// Lowercase SHA-256 checksum of the Bronze payload.
    pub checksum_sha256: String,
    /// MIME type observed for the Bronze payload.
    pub content_type: String,
    /// Bronze payload size in bytes.
    pub size_bytes: u64,
    /// Optional number of logical source records packed into this object.
    pub logical_record_count: Option<u64>,
    /// UTC timestamp when the object was collected.
    pub collected_at: DateTime<Utc>,
    /// Human-readable source period bucket, when applicable.
    pub snapshot_period: Option<String>,
    /// Canonical source as-of date.
    pub snapshot_date: NaiveDate,
    /// Granularity of [`snapshot_date`](Self::snapshot_date).
    pub snapshot_granularity: SnapshotGranularity,
    /// Provenance of [`snapshot_date`](Self::snapshot_date).
    pub snapshot_basis: SnapshotBasis,
    /// Provider file id for bulk sources, when applicable.
    pub provider_file_id: Option<String>,
    /// Provider file name for bulk sources, when applicable.
    pub provider_file_name: Option<String>,
    /// Provider update date, when supplied by the source inventory.
    pub provider_updated_at: Option<NaiveDate>,
    /// Optional effective date represented by the source object.
    pub effective_date: Option<NaiveDate>,
    /// UTC timestamp when the Bronze object row was created.
    pub created_at: DateTime<Utc>,
}

/// Granularity of the source snapshot date recorded for a Bronze object.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SnapshotGranularity {
    /// The source object represents one calendar day.
    Day,
    /// The source object represents one calendar month.
    Month,
}

impl SnapshotGranularity {
    /// Returns the stable database/API wire value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Day => "day",
            Self::Month => "month",
        }
    }

    /// Parses a stable database/API wire value.
    ///
    /// # Errors
    /// Returns [`BronzeSnapshotMetadataError`] when the value is unknown.
    pub fn from_wire(value: &str) -> Result<Self, BronzeSnapshotMetadataError> {
        match value {
            "day" => Ok(Self::Day),
            "month" => Ok(Self::Month),
            _ => Err(BronzeSnapshotMetadataError::InvalidGranularity(
                value.to_owned(),
            )),
        }
    }
}

/// Provenance of the snapshot date recorded for a Bronze object.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SnapshotBasis {
    /// The provider supplied an explicit data 기준일.
    ProviderSnapshotDate,
    /// The provider supplied a file period such as `2026-05`.
    ProviderFilePeriod,
    /// The request month selected the data, such as data.go.kr `DEAL_YMD=202605`.
    RequestMonth,
    /// The provider supplied an update date and no better 기준일.
    ProviderUpdatedAt,
    /// No provider date was available; the collection date was used explicitly as a fallback.
    CollectedAtFallback,
}

impl SnapshotBasis {
    /// Returns the stable database/API wire value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderSnapshotDate => "provider_snapshot_date",
            Self::ProviderFilePeriod => "provider_file_period",
            Self::RequestMonth => "request_month",
            Self::ProviderUpdatedAt => "provider_updated_at",
            Self::CollectedAtFallback => "collected_at_fallback",
        }
    }

    /// Parses a stable database/API wire value.
    ///
    /// # Errors
    /// Returns [`BronzeSnapshotMetadataError`] when the value is unknown.
    pub fn from_wire(value: &str) -> Result<Self, BronzeSnapshotMetadataError> {
        match value {
            "provider_snapshot_date" => Ok(Self::ProviderSnapshotDate),
            "provider_file_period" => Ok(Self::ProviderFilePeriod),
            "request_month" => Ok(Self::RequestMonth),
            "provider_updated_at" => Ok(Self::ProviderUpdatedAt),
            "collected_at_fallback" => Ok(Self::CollectedAtFallback),
            _ => Err(BronzeSnapshotMetadataError::InvalidBasis(value.to_owned())),
        }
    }
}

/// Error returned when parsing Bronze snapshot metadata wire values.
#[derive(Debug, Error)]
pub enum BronzeSnapshotMetadataError {
    /// Unknown snapshot granularity.
    #[error("invalid Bronze snapshot granularity: {0}")]
    InvalidGranularity(String),
    /// Unknown snapshot basis.
    #[error("invalid Bronze snapshot basis: {0}")]
    InvalidBasis(String),
}

/// Observed schema information for one source field path in an ingestion run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaProfile {
    /// Stable foundation-platform schema profile identifier.
    pub id: SchemaProfileId,
    /// Source catalog entry whose payloads were profiled.
    pub source_catalog_id: SourceCatalogId,
    /// Ingestion run that produced the profile sample.
    pub ingestion_run_id: IngestionRunId,
    /// Field path observed in the source payload.
    pub field_path: String,
    /// Observed value type for this field path.
    pub observed_type: SchemaObservedType,
    /// Number of sampled records with non-null values.
    pub nonnull_count: u64,
    /// Number of sampled records with null or absent values.
    pub null_count: u64,
    /// Small JSON array of representative sample values.
    pub sample_values: JsonValue,
    /// Heuristic score from 0.0 to 1.0 indicating likely key usefulness.
    pub candidate_key_score: f64,
    /// UTC timestamp when this profile was produced.
    pub profiled_at: DateTime<Utc>,
    /// UTC timestamp when the profile row was created.
    pub created_at: DateTime<Utc>,
    /// UTC timestamp when the profile row was last updated.
    pub updated_at: DateTime<Utc>,
    /// Monotonic version used for optimistic concurrency and audit.
    pub version: i64,
}

/// Inputs required to build a canonical Bronze object key.
///
/// The key is a readable physical location label and is request/file-deterministic (ADR 0019): it
/// carries no `run_id` (run/lineage lives only in the `bronze_object` row + run manifest), no
/// `partition=` wrapper, and no unique-id-as-directory. Partitions are clean Hive `key=value`
/// segments and the provider file/page identity is the leaf filename, not a directory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BronzeObjectKeyParts<'a> {
    /// Stable canonical source slug, for example `datagokr__building_register_main`.
    pub source_slug: &'a str,
    /// Meaningful low-cardinality Hive partition path, for example
    /// `period=2026-05` or `sigungu=11680/bjdong=10300`. Each `/`-separated segment must be a
    /// `key=value` pair. May be empty when the dataset has no meaningful partition.
    pub partition_path: &'a str,
    /// Deterministic leaf filename stem, without extension: the provider file id for bulk lanes
    /// (`OPN209912310000000008`) or a sequence/page id for API-page lanes (`page-000001`). This is
    /// a filename, never a directory.
    pub leaf_name: &'a str,
    /// File extension without a leading dot.
    pub extension: &'a str,
}

/// Error returned while building a canonical Bronze object key.
#[derive(Debug, Error)]
pub enum BronzeObjectKeyError {
    /// A caller supplied an invalid key part.
    #[error("invalid Bronze object key {field}: {reason}")]
    InvalidPart {
        /// Field that failed validation.
        field: &'static str,
        /// Human-readable validation failure.
        reason: String,
    },
    /// The composed key failed provider-neutral object-key validation.
    #[error("invalid generated Bronze object key: {0}")]
    ObjectKey(#[from] ObjectKeyError),
}

/// Builds the canonical R2 object key for an immutable Bronze payload.
///
/// The key is a readable physical location label and is request/file-deterministic (ADR 0019): the
/// same provider file or same page request resolves to the same key, with no `run_id` segment and no
/// `partition=` wrapper. The provider file/page identity is the leaf filename; content identity,
/// snapshot semantics, and lineage live in `bronze_object`.
///
/// # Errors
///
/// Returns `BronzeObjectKeyError` when any key part is empty, path-like, the partition path is not
/// a sequence of Hive `key=value` segments, or the result is outside the canonical Bronze object
/// layout.
pub fn build_bronze_object_key(
    parts: BronzeObjectKeyParts<'_>,
) -> Result<ObjectKey, BronzeObjectKeyError> {
    validate_source_slug(parts.source_slug)?;
    validate_partition_path("partition_path", parts.partition_path)?;
    validate_operation_partition(parts.source_slug, parts.partition_path)?;
    validate_leaf_name(parts.leaf_name)?;
    validate_extension(parts.extension)?;

    let mut key = format!("bronze/source={}", parts.source_slug);
    if !parts.partition_path.is_empty() {
        key.push('/');
        key.push_str(parts.partition_path);
    }
    let _ = write!(&mut key, "/{}.{}", parts.leaf_name, parts.extension);

    Ok(ObjectKey::parse(&key)?)
}

/// Validates an existing key with the same canonical contract used by Bronze writers.
///
/// # Errors
///
/// Returns [`BronzeObjectKeyError`] when the key is outside the canonical Bronze layout.
pub fn validate_bronze_object_key_contract(key: &str) -> Result<(), BronzeObjectKeyError> {
    let Some(rest) = key.strip_prefix("bronze/source=") else {
        return Err(invalid_bronze_key_part(
            "object_key",
            "must start with 'bronze/source='",
        ));
    };
    let Some((source_slug, tail)) = rest.split_once('/') else {
        return Err(invalid_bronze_key_part(
            "object_key",
            "must include a source slug and filename",
        ));
    };
    let (partition_path, filename) = tail.rsplit_once('/').unwrap_or(("", tail));
    let Some((leaf_name, extension)) = filename.rsplit_once('.') else {
        return Err(invalid_bronze_key_part(
            "object_key",
            "filename must include an extension",
        ));
    };

    let rebuilt = build_bronze_object_key(BronzeObjectKeyParts {
        source_slug,
        partition_path,
        leaf_name,
        extension,
    })?;
    if rebuilt.as_str() != key {
        return Err(invalid_bronze_key_part(
            "object_key",
            "must equal the canonical key produced from its parts",
        ));
    }
    Ok(())
}

fn validate_source_slug(source_slug: &str) -> Result<(), BronzeObjectKeyError> {
    if source_slug.is_empty() {
        return Err(invalid_bronze_key_part("source_slug", "must not be empty"));
    }
    // The source slug must be the canonical `{providerid}__{dataset_slug}` shape (ADR 0014). This
    // is the single Bronze write-boundary backstop: it makes a non-canonical slug (old hyphenated
    // names, single-underscore variants, unknown providers, uppercase) impossible to write, no
    // matter which producer assembled it. A permissive charset check is intentionally NOT enough.
    //
    // SCOPE (ADR 0014/0019): this boundary checks the slug SHAPE only — it does NOT verify that the
    // dataset_slug is actually REGISTERED in the catalog (e.g. `datagokr__foo` passes the shape).
    // This Collection domain package is a pure low-level crate with no registry I/O, so membership
    // cannot be checked here without embedding a generated table or file I/O (meta-machine/ceremony,
    // against the product-first rule). Source membership is enforced upstream, closer to where the
    // slug is born: (1) the catalog parity test (source_slug_catalog_parity.rs), (2) the curated
    // operation->dataset_slug maps (operation_dataset_slug.rs) which return None for any unregistered
    // operation, and (3) resolve_canonical_source_slug override validation. This boundary is the
    // last-resort *shape* gate, not a membership gate — by design.
    if !crate::source_slug::is_canonical_source_slug(source_slug) {
        return Err(invalid_bronze_key_part(
            "source_slug",
            "must be canonical \"{providerid}__{dataset_slug}\" \
             (known provider id + '__' + lowercase snake_case dataset, no '-')",
        ));
    }
    Ok(())
}

/// Validates a Hive partition path: zero or more `/`-separated `key=value` segments.
///
/// An empty path is allowed (a dataset without a meaningful partition). When non-empty, every
/// segment must be a clean Hive `key=value` pair (no `partition=` wrapper, no bare directory).
fn validate_partition_path(field: &'static str, value: &str) -> Result<(), BronzeObjectKeyError> {
    if value.is_empty() {
        return Ok(());
    }
    if value.starts_with('/') || value.ends_with('/') {
        return Err(invalid_bronze_key_part(
            field,
            "must not start or end with '/'",
        ));
    }
    if value.contains('\\') {
        return Err(invalid_bronze_key_part(
            field,
            "must not contain backslash separators",
        ));
    }
    if value.contains("..") {
        return Err(invalid_bronze_key_part(
            field,
            "must not contain traversal markers",
        ));
    }
    for segment in value.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(invalid_bronze_key_part(
                field,
                "must not contain empty, '.', or '..' path segments",
            ));
        }
        // Each partition segment must be a single Hive key=value pair. This rejects the legacy
        // `partition=operation=...` double wrapper and any bare directory segment.
        let Some((key, partition_value)) = segment.split_once('=') else {
            return Err(invalid_bronze_key_part(
                field,
                "each partition segment must be a Hive 'key=value' pair",
            ));
        };
        if key.is_empty() || partition_value.is_empty() || partition_value.contains('=') {
            return Err(invalid_bronze_key_part(
                field,
                "each partition segment must be exactly one non-empty 'key=value' pair",
            ));
        }
        // SEMANTIC GUARD 1 (ADR 0016 T1.3): reserved partition-KEY blocklist. A partition key names
        // *what a slice of the dataset is* (region, period, dataset, pnu). These reserved names are
        // request knobs (page-size, format, paging) or constants/secrets (service key, filter kind)
        // — they are NOT a meaningful partition axis and were the seed of prior cadastral jank. A
        // careless new lane that folds a request knob into the path is rejected structurally here.
        if is_reserved_partition_key(key) {
            return Err(invalid_bronze_key_part(
                field,
                "partition key is a reserved request-knob/constant, not a meaningful \
                 partition axis (e.g. size, pageno, format, servicekey, filter_kind)",
            ));
        }
        // SEMANTIC GUARD 2a (ADR 0016 T1.3): an opaque hash/uuid as a partition VALUE means the path
        // encodes an unreadable digest instead of meaning (the old `filter_sha256=<region hash>`).
        if is_opaque_hash_or_uuid(partition_value) {
            return Err(invalid_bronze_key_part(
                field,
                "partition value must be a readable identifier, not an opaque hash/uuid digest",
            ));
        }
    }
    Ok(())
}

/// Reserved partition KEYs that must never appear as a Hive `key=` partition axis.
///
/// These are request knobs (paging / output format) or constants/secrets, not slices of the
/// dataset. Compared case-insensitively so no casing variant slips through.
fn is_reserved_partition_key(key: &str) -> bool {
    const RESERVED: &[&str] = &[
        "provider_file_period",
        "provider_snapshot_date",
        "snapshot_period",
        "snapshot_date",
        "snapshot_granularity",
        "snapshot_basis",
        "provider_updated_at",
        "ingest_date",
        "collected_at",
        "run_id",
        "checksum",
        "checksum_sha256",
        "sha256",
        "version",
        "filter_kind",
        "size",
        "numofrows",
        "num_of_rows",
        "pageno",
        "page_no",
        "format",
        "_type",
        "columns",
        "servicekey",
    ];
    let lowered = key.to_ascii_lowercase();
    RESERVED.contains(&lowered.as_str())
}

fn validate_operation_partition(
    source_slug: &str,
    partition_path: &str,
) -> Result<(), BronzeObjectKeyError> {
    for segment in partition_path.split('/') {
        let Some((key, operation)) = segment.split_once('=') else {
            continue;
        };
        if key.eq_ignore_ascii_case("operation")
            && crate::operation_dataset_slug::operation_collapses_into_slug(operation, source_slug)
        {
            return Err(invalid_bronze_key_part(
                "partition_path",
                "operation is already represented by source_slug",
            ));
        }
    }
    Ok(())
}

/// Returns true when `value` is a single bare token that is an opaque content digest:
/// either an all-lowercase-hex run of **>= 32** chars, or a `8-4-4-4-12` hex UUID shape.
///
/// This is intentionally narrow so meaningful identifiers still pass: a short named
/// `filter_fingerprint=<12 hex>` (12 < 32), uppercase provider file ids like
/// `OPN209912310000000002`, numeric region/PNU/date codes, and dataset names are all NOT opaque.
fn is_opaque_hash_or_uuid(value: &str) -> bool {
    is_lowercase_hex_run(value, 32) || is_uuid_shape(value)
}

/// True when `value` is entirely lowercase hex (`0-9`, `a-f`) and at least `min_len` chars long.
fn is_lowercase_hex_run(value: &str, min_len: usize) -> bool {
    value.len() >= min_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// True when `value` is a canonical `8-4-4-4-12` hex UUID shape (lowercase hex groups, dashes).
fn is_uuid_shape(value: &str) -> bool {
    let mut groups = value.split('-');
    let expected_lens = [8_usize, 4, 4, 4, 12];
    for &len in &expected_lens {
        match groups.next() {
            Some(group) if is_lowercase_hex_run(group, len) && group.len() == len => {}
            _ => return false,
        }
    }
    groups.next().is_none()
}

/// Validates a leaf filename stem: non-empty, no path separators or traversal markers.
fn validate_leaf_name(value: &str) -> Result<(), BronzeObjectKeyError> {
    if value.is_empty() {
        return Err(invalid_bronze_key_part("leaf_name", "must not be empty"));
    }
    if value.contains('/') || value.contains('\\') {
        return Err(invalid_bronze_key_part(
            "leaf_name",
            "must not contain path separators (it is a filename, not a directory)",
        ));
    }
    if value.contains("..") || value == "." {
        return Err(invalid_bronze_key_part(
            "leaf_name",
            "must not contain traversal markers",
        ));
    }
    // SEMANTIC GUARD 2b (ADR 0016 T1.3): the leaf names a provider file/page (`page-000001`,
    // `OPN209912310000000002`), not a content hash. An opaque hash/uuid stem means the path is
    // digest-only blob paths instead of file/page identity → reject them structurally.
    if is_opaque_hash_or_uuid(value) {
        return Err(invalid_bronze_key_part(
            "leaf_name",
            "must be a readable provider file/page identity, not an opaque hash/uuid digest",
        ));
    }
    Ok(())
}

fn validate_extension(extension: &str) -> Result<(), BronzeObjectKeyError> {
    if extension.is_empty() {
        return Err(invalid_bronze_key_part("extension", "must not be empty"));
    }
    if !extension
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    {
        return Err(invalid_bronze_key_part(
            "extension",
            "must contain only lowercase ASCII letters and digits",
        ));
    }
    Ok(())
}

fn invalid_bronze_key_part(field: &'static str, reason: &str) -> BronzeObjectKeyError {
    BronzeObjectKeyError::InvalidPart {
        field,
        reason: reason.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_bronze_object_key, is_opaque_hash_or_uuid, BronzeObjectKeyParts, SnapshotBasis,
        SnapshotGranularity,
    };

    #[test]
    fn snapshot_granularity_round_trips_wire_values() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(SnapshotGranularity::Day.as_str(), "day");
        assert_eq!(SnapshotGranularity::Month.as_str(), "month");
        assert_eq!(
            SnapshotGranularity::from_wire("day")?,
            SnapshotGranularity::Day
        );
        assert_eq!(
            SnapshotGranularity::from_wire("month")?,
            SnapshotGranularity::Month
        );
        assert!(SnapshotGranularity::from_wire("hour").is_err());
        Ok(())
    }

    #[test]
    fn snapshot_basis_round_trips_wire_values() -> Result<(), Box<dyn std::error::Error>> {
        for (basis, wire) in [
            (
                SnapshotBasis::ProviderSnapshotDate,
                "provider_snapshot_date",
            ),
            (SnapshotBasis::ProviderFilePeriod, "provider_file_period"),
            (SnapshotBasis::RequestMonth, "request_month"),
            (SnapshotBasis::ProviderUpdatedAt, "provider_updated_at"),
            (SnapshotBasis::CollectedAtFallback, "collected_at_fallback"),
        ] {
            assert_eq!(basis.as_str(), wire);
            assert_eq!(SnapshotBasis::from_wire(wire)?, basis);
        }
        assert!(SnapshotBasis::from_wire("effective_date").is_err());
        Ok(())
    }

    #[test]
    fn bronze_object_key_uses_readable_identity_layout() -> Result<(), Box<dyn std::error::Error>> {
        // data.go.kr API-page lane: meaningful sigungu/bjdong partitions, page leaf filename.
        // No redundant operation, run_id, `partition=` wrapper, or provider-id directory.
        let key = build_bronze_object_key(BronzeObjectKeyParts {
            source_slug: "datagokr__building_register_main",
            partition_path: "sigungu=11680/bjdong=10300",
            leaf_name: "page-000001",
            extension: "json",
        })?;

        assert_eq!(
            key.as_str(),
            "bronze/source=datagokr__building_register_main/sigungu=11680/bjdong=10300/page-000001.json"
        );
        Ok(())
    }

    #[test]
    fn bronze_object_key_hub_bulk_leaf_is_provider_file_id(
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Hub bulk lane: provider_file_id is the physical identity. Provider period/date remains
        // typed Catalog metadata and is not duplicated in the object key.
        let key = build_bronze_object_key(BronzeObjectKeyParts {
            source_slug: "hubgokr__building_register_main",
            partition_path: "",
            leaf_name: "OPN209912310000000008",
            extension: "zip",
        })?;

        assert_eq!(
            key.as_str(),
            "bronze/source=hubgokr__building_register_main/OPN209912310000000008.zip"
        );
        Ok(())
    }

    #[test]
    fn bronze_object_key_allows_empty_partition_path() -> Result<(), Box<dyn std::error::Error>> {
        let key = build_bronze_object_key(BronzeObjectKeyParts {
            source_slug: "hubgokr__building_register_main",
            partition_path: "",
            leaf_name: "OPN209912310000000008",
            extension: "zip",
        })?;

        assert_eq!(
            key.as_str(),
            "bronze/source=hubgokr__building_register_main/OPN209912310000000008.zip"
        );
        Ok(())
    }

    #[test]
    fn bronze_object_key_rejects_non_canonical_source_slug(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let partition = "sigungu=11680";

        // The old (pre-rename) hyphenated slug must be rejected at the write boundary: it would
        // otherwise leak a non-canonical Bronze key. This is the bug the guard closes.
        assert!(
            build_bronze_object_key(BronzeObjectKeyParts {
                source_slug: "molit-building-register",
                partition_path: partition,
                leaf_name: "page-000001",
                extension: "json",
            })
            .is_err(),
            "old-format slug molit-building-register must be rejected at the Bronze write boundary"
        );

        // The canonical slug must still produce the expected key.
        let key = build_bronze_object_key(BronzeObjectKeyParts {
            source_slug: "datagokr__building_register_main",
            partition_path: partition,
            leaf_name: "page-000001",
            extension: "json",
        })?;
        assert!(
            key.as_str()
                .starts_with("bronze/source=datagokr__building_register_main/"),
            "unexpected key: {}",
            key.as_str()
        );
        Ok(())
    }

    #[test]
    fn bronze_object_key_rejects_legacy_partition_wrapper() {
        // The legacy `partition=operation=...` double wrapper is no longer a valid partition path.
        assert!(
            build_bronze_object_key(BronzeObjectKeyParts {
                source_slug: "datagokr__building_register_main",
                partition_path: "partition=operation=getBrTitleInfo",
                leaf_name: "page-000001",
                extension: "json",
            })
            .is_err(),
            "legacy partition=operation= wrapper must be rejected"
        );
    }

    #[test]
    fn bronze_object_key_rejects_ambiguous_path_parts() -> Result<(), Box<dyn std::error::Error>> {
        let valid_partition = "sigungu=11680";

        // These must be rejected.
        for (source_slug, partition_path, leaf_name, extension) in [
            // Uppercase slug rejected.
            ("MOLIT", valid_partition, "page-000001", "json"),
            // Leading underscore rejected.
            (
                "_datagokr__building_register",
                valid_partition,
                "page-000001",
                "json",
            ),
            // Trailing underscore rejected.
            (
                "datagokr__building_register_",
                valid_partition,
                "page-000001",
                "json",
            ),
            // Leading hyphen rejected.
            ("-datagokr", valid_partition, "page-000001", "json"),
            // Trailing hyphen rejected.
            ("datagokr-", valid_partition, "page-000001", "json"),
            // Leading slash in partition rejected.
            (
                "datagokr__building_register_main",
                "/operation=getBrTitleInfo",
                "page-000001",
                "json",
            ),
            // Path traversal in partition rejected.
            (
                "datagokr__building_register_main",
                "operation=../gold",
                "page-000001",
                "json",
            ),
            // Bare directory partition segment (no '=') rejected.
            (
                "datagokr__building_register_main",
                "operation",
                "page-000001",
                "json",
            ),
            // Leaf with a path separator rejected (must be a filename).
            (
                "datagokr__building_register_main",
                valid_partition,
                "sub/page-000001",
                "json",
            ),
            // Leaf traversal rejected.
            (
                "datagokr__building_register_main",
                valid_partition,
                "..",
                "json",
            ),
            // Leading dot in extension rejected.
            (
                "datagokr__building_register_main",
                valid_partition,
                "page-000001",
                ".json",
            ),
        ] {
            assert!(
                build_bronze_object_key(BronzeObjectKeyParts {
                    source_slug,
                    partition_path,
                    leaf_name,
                    extension,
                })
                .is_err(),
                "expected invalid key parts: {source_slug:?} {partition_path:?} {leaf_name:?} {extension:?}"
            );
        }

        // Single underscore in the middle must be ACCEPTED.
        let Ok(_) = build_bronze_object_key(BronzeObjectKeyParts {
            source_slug: "vworldkr__cadastral",
            partition_path: valid_partition,
            leaf_name: "page-000001",
            extension: "json",
        }) else {
            return Err("single underscore slug vworldkr__cadastral must be accepted".into());
        };

        // Double underscore separator (provider__dataset) must be ACCEPTED.
        let Ok(_) = build_bronze_object_key(BronzeObjectKeyParts {
            source_slug: "datagokr__building_register_main",
            partition_path: valid_partition,
            leaf_name: "page-000001",
            extension: "json",
        }) else {
            return Err(
                "double underscore slug datagokr__building_register_main must be accepted".into(),
            );
        };

        Ok(())
    }

    /// Every current (post-collapse, post-cadastral-redesign) lane key shape must still build OK.
    /// These are the real shapes the semantic guard must NOT reject (purely additive validation).
    #[test]
    fn semantic_guard_accepts_all_current_lane_shapes() -> Result<(), Box<dyn std::error::Error>> {
        for (label, source_slug, partition_path, leaf_name, extension) in [
            // building_register API-page lane: sigungu/bjdong partitions, page leaf.
            (
                "building_register sigungu/bjdong/page",
                "datagokr__building_register_main",
                "sigungu=11680/bjdong=10300",
                "page-000001",
                "json",
            ),
            // real_transaction API-page lane: lawd + deal_ymd partitions, page leaf.
            (
                "real_transaction lawd/deal_ymd/page",
                "datagokr__apt_trade",
                "lawd=11680/deal_ymd=202605",
                "page-000001",
                "json",
            ),
            // V-World cadastral single-field scope key: dataset + pnu.
            (
                "vworld cadastral dataset/pnu",
                "vworldkr__cadastral",
                "dataset=LP_PA_CBND_BUBUN/pnu=9999900601100010000",
                "page-000001",
                "json",
            ),
            // V-World cadastral single-field scope key: dataset + emd.
            (
                "vworld cadastral dataset/emd",
                "vworldkr__cadastral",
                "dataset=LP_PA_CBND_BUBUN/emd=9999900601",
                "page-000001",
                "json",
            ),
            // V-World cadastral compound scope key: short named filter_fingerprint (12 hex < 32).
            (
                "vworld cadastral dataset/filter_fingerprint",
                "vworldkr__cadastral",
                "dataset=LP_PA_CBND_BUBUN/filter_fingerprint=a1b2c3d4e5f6",
                "page-000001",
                "json",
            ),
            // V-World plain pnu lane.
            (
                "vworld pnu",
                "vworldkr__cadastral",
                "pnu=9999900601100010000",
                "page-000001",
                "json",
            ),
            // Hub bulk lane: only the uppercase OPN provider file id is physical identity.
            (
                "hub OPN leaf",
                "hubgokr__building_register_main",
                "",
                "OPN209912310000000002",
                "zip",
            ),
        ] {
            build_bronze_object_key(BronzeObjectKeyParts {
                source_slug,
                partition_path,
                leaf_name,
                extension,
            })
            .map_err(|e| format!("current lane key was rejected ({label}): {e}"))?;
        }
        Ok(())
    }

    /// The reserved partition-KEY blocklist: request knobs / constants must never be partition keys.
    #[test]
    fn semantic_guard_rejects_reserved_partition_keys() {
        let base = ("datagokr__building_register_main", "page-000001", "json");
        for reserved_segment in [
            "filter_kind=attr",
            "size=000010",
            "numofrows=1000",
            "num_of_rows=1000",
            "pageno=1",
            "page_no=1",
            "format=json",
            "_type=json",
            "columns=a,b,c",
            "servicekey=abc",
            // case-insensitive: keys must be rejected regardless of case.
            "ServiceKey=abc",
            "PageNo=1",
            "FORMAT=json",
        ] {
            let partition_path = format!("sigungu=11680/{reserved_segment}");
            assert!(
                build_bronze_object_key(BronzeObjectKeyParts {
                    source_slug: base.0,
                    partition_path: &partition_path,
                    leaf_name: base.1,
                    extension: base.2,
                })
                .is_err(),
                "reserved partition key segment must be rejected: {reserved_segment:?}"
            );
        }
    }

    #[test]
    fn semantic_guard_rejects_catalog_metadata_partition_keys() {
        for metadata_segment in [
            "provider_file_period=2026-04",
            "provider_snapshot_date=2026-04-01",
            "snapshot_period=2026-04",
            "snapshot_date=2026-04-01",
            "snapshot_granularity=month",
            "snapshot_basis=provider_file_period",
            "provider_updated_at=2026-04-09",
            "ingest_date=2026-07-02",
            "collected_at=2026-07-02T12:00:00Z",
            "run_id=0196e7e0-3c20-7000-8000-000000000001",
            "checksum_sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "version=1",
        ] {
            assert!(
                build_bronze_object_key(BronzeObjectKeyParts {
                    source_slug: "hubgokr__building_register_main",
                    partition_path: metadata_segment,
                    leaf_name: "OPN209912310000000002",
                    extension: "zip",
                })
                .is_err(),
                "catalog metadata must not become a physical path segment: {metadata_segment:?}"
            );
        }
    }

    #[test]
    fn semantic_guard_rejects_operation_repeated_by_source_slug() {
        assert!(
            build_bronze_object_key(BronzeObjectKeyParts {
                source_slug: "datagokr__building_register_main",
                partition_path: "operation=getBrTitleInfo/sigungu=11680/bjdong=10300",
                leaf_name: "page-000001",
                extension: "json",
            })
            .is_err(),
            "operation already represented by source_slug must not be repeated in the path"
        );
    }

    /// Opaque hash / uuid values are forbidden as partition values and as leaf stems.
    #[test]
    fn semantic_guard_rejects_opaque_hash_or_uuid_values() {
        let hex64 = "a".repeat(64);
        let uuid = "550e8400-e29b-41d4-a716-446655440000";

        // Bare 64-hex as a partition value → rejected.
        assert!(
            build_bronze_object_key(BronzeObjectKeyParts {
                source_slug: "vworldkr__cadastral",
                partition_path: &format!("filter_sha256={hex64}"),
                leaf_name: "page-000001",
                extension: "json",
            })
            .is_err(),
            "bare 64-hex partition value must be rejected"
        );

        // uuid as a partition value → rejected.
        assert!(
            build_bronze_object_key(BronzeObjectKeyParts {
                source_slug: "vworldkr__cadastral",
                partition_path: &format!("scope={uuid}"),
                leaf_name: "page-000001",
                extension: "json",
            })
            .is_err(),
            "uuid partition value must be rejected"
        );

        // Bare 64-hex as a leaf stem → rejected (content-hash-in-path).
        assert!(
            build_bronze_object_key(BronzeObjectKeyParts {
                source_slug: "vworldkr__cadastral",
                partition_path: "dataset=LP_PA_CBND_BUBUN/pnu=9999900601100010000",
                leaf_name: &hex64,
                extension: "json",
            })
            .is_err(),
            "bare 64-hex leaf stem must be rejected"
        );

        // uuid as a leaf stem → rejected.
        assert!(
            build_bronze_object_key(BronzeObjectKeyParts {
                source_slug: "vworldkr__cadastral",
                partition_path: "dataset=LP_PA_CBND_BUBUN/pnu=9999900601100010000",
                leaf_name: uuid,
                extension: "json",
            })
            .is_err(),
            "uuid leaf stem must be rejected"
        );
    }

    /// Unit coverage for the shared opaque-hash/uuid predicate, including the must-NOT-match cases.
    #[test]
    fn is_opaque_hash_or_uuid_classifies_correctly() {
        // Opaque: 32+ char lowercase hex, and uuid shape.
        assert!(is_opaque_hash_or_uuid(&"a".repeat(32)));
        assert!(is_opaque_hash_or_uuid(&"a".repeat(64)));
        assert!(is_opaque_hash_or_uuid(&"0123456789abcdef".repeat(2))); // 32 hex
        assert!(is_opaque_hash_or_uuid(
            "550e8400-e29b-41d4-a716-446655440000"
        ));

        // NOT opaque — these are real, meaningful values that must still pass.
        assert!(!is_opaque_hash_or_uuid("a1b2c3d4e5f6")); // 12 hex < 32
        assert!(!is_opaque_hash_or_uuid("page-000001"));
        assert!(!is_opaque_hash_or_uuid("OPN209912310000000002")); // uppercase, not lowercase-hex
        assert!(!is_opaque_hash_or_uuid("11680")); // sigungu / lawd
        assert!(!is_opaque_hash_or_uuid("10300")); // bjdong
        assert!(!is_opaque_hash_or_uuid("9999900601100010000")); // pnu (decimal, but 19 chars)
        assert!(!is_opaque_hash_or_uuid("2026-04")); // provider_file_period
        assert!(!is_opaque_hash_or_uuid("202605")); // deal_ymd
        assert!(!is_opaque_hash_or_uuid("LP_PA_CBND_BUBUN")); // dataset
                                                              // 32+ chars but contains non-hex (g) → not opaque hex.
        assert!(!is_opaque_hash_or_uuid(&format!("g{}", "a".repeat(40))));
        // Dash-grouped but wrong uuid group sizes (4-4-4-4-12, not 8-4-4-4-12) → not a uuid shape,
        // and short enough per group that it is not a >=32 hex run either.
        assert!(!is_opaque_hash_or_uuid(
            "550e-8400-e29b-41d4-a716446655440000"
        ));
        // Uuid group sizes but uppercase hex → not a (lowercase) uuid shape.
        assert!(!is_opaque_hash_or_uuid(
            "550E8400-E29B-41D4-A716-446655440000"
        ));
    }
}
