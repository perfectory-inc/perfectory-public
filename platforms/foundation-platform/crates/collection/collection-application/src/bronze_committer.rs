//! Single-seam Bronze commit boundary (ADR 0016).
//!
//! Every Bronze raw write must flow through [`BronzeCommitter::commit`], which performs, in ONE
//! place: (a) write the raw payload to the object storage port with the write-once `CreateOnly`
//! policy (stamping `x-amz-meta-sha256`), then (b) record the `bronze_object` metadata row through
//! the existing [`BronzeIngestUnitOfWork`]. Consolidating these two steps behind one seam is what
//! lets the shared invariants attach to a single code path instead of the ~8 scattered `put_object`
//! call sites.
//!
//! Because R2 and Postgres are not one transaction (write -> record), a `CreateOnly` collision is
//! reconciled by checksum rather than failed: same content -> idempotent success / recover the
//! missing row; different content -> fail loud (`ChecksumConflict`, the quarantine terminal). This
//! is the recoverable commit protocol; see [`BronzeCommitter::commit`].
//!
//! This module routes the building-register API-page lane through the seam. The remaining compile
//! invariants (page-size validation, key-compile collapse, semantic guard) are left as explicit
//! `// Task:` seams below.

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use collection_domain::{BronzeObject, CollectionError, SnapshotBasis, SnapshotGranularity};
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use foundation_shared_kernel::ObjectKey;
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::building_register_bronze_plan::{
    BuildingRegisterBronzePlanError, BuildingRegisterPageRequest,
};
use crate::ports::BronzeIngestUnitOfWork;
use crate::public_data_bronze_plan::{PublicDataBronzePagePlan, PublicDataPageRequest};
use crate::real_transaction_bronze_plan::RealTransactionPageRequest;
use crate::vworld_cadastral_bronze_plan::VWorldCadastralPageRequest;
use crate::vworld_land_register_bronze_plan::VWorldLandRegisterPageRequest;
use crate::vworld_ned_bronze_plan::VWorldNedPageRequest;

/// Raw Bronze payload to persist at the planned object key.
///
/// Today the only variant is [`BronzePayload::InMemory`] (the full page bytes already buffered in
/// memory). The enum exists so a `Streaming` variant can be added LATER for large bulk objects whose
/// checksum is only known post-stream — see the documented seam below — WITHOUT changing the
/// committer's call sites.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BronzePayload {
    /// The complete raw payload bytes, already buffered in memory (API-page lanes).
    InMemory(Vec<u8>),
    // Task (streaming): add `Streaming(BronzeByteStream)` for large immutable bulk objects whose
    // sha256 is known only after the stream completes. The committer's write step will branch on the
    // variant (non-streaming sets `x-amz-meta-sha256`; streaming reconciles the checksum
    // post-upload). Do NOT add it in Task 1.
}

impl BronzePayload {
    /// Returns the in-memory bytes when this payload is buffered, or `None` for a future streaming
    /// variant.
    #[must_use]
    pub fn as_in_memory_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::InMemory(bytes) => Some(bytes),
        }
    }
}

/// A fully-planned, in-memory Bronze object ready to be committed.
///
/// All identity (key, checksum, dedupe key, partition) is already resolved by the per-lane Bronze
/// plan; the committer does not re-derive it in Task 1. The `record` row is the exact
/// [`BronzeObject`] the lane would have recorded inline, carried here so the committer is the single
/// place that calls [`BronzeIngestUnitOfWork::record_bronze_object`].
#[derive(Clone, Debug)]
pub struct PlannedBronzeObject {
    /// Provider-neutral object key for the raw Bronze payload.
    pub object_key: String,
    /// Raw payload to write to object storage.
    pub payload: BronzePayload,
    /// MIME content type stored with the object.
    pub content_type: String,
    /// Cache-Control header stored with the object.
    pub cache_control: String,
    /// Lowercase hex SHA-256 of the raw payload (echoed back in the commit outcome).
    pub checksum_sha256: String,
    /// The `bronze_object` metadata row to record after the storage write succeeds.
    pub record: BronzeObject,
}

/// Small outcome returned by a successful Bronze commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BronzeCommitOutcome {
    /// Object key written and recorded.
    pub object_key: String,
    /// Lowercase hex SHA-256 of the committed payload.
    pub checksum_sha256: String,
}

/// Outcome returned by a successful streaming bulk commit.
///
/// Richer than [`BronzeCommitOutcome`] because the streaming bulk callers (hub.go.kr / V-World bulk
/// lanes) carry the streamed size + the recorded `bronze_object` id forward in their run evidence —
/// the same way [`PublicDataPageCommitOutcome`] carries the page id + plan. `size_bytes`/
/// `bronze_object_id` are the streamed write's values (normal path), the existing row's values
/// (idempotent skip), or the GET-rehash + newly recorded row's values (recovery).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamingBronzeCommitOutcome {
    /// Object key streamed and recorded.
    pub object_key: String,
    /// Lowercase hex SHA-256 of the streamed (or recovered/existing) payload.
    pub checksum_sha256: String,
    /// Size in bytes of the streamed (or recovered/existing) object.
    pub size_bytes: u64,
    /// Identifier of the `bronze_object` row this commit recorded or found.
    pub bronze_object_id: BronzeObjectId,
}

/// Raw data.go.kr page identity + payload handed to
/// [`BronzeCommitter::commit_public_data_page`], generic over the lane request `Req`.
///
/// The committer OWNS the key-compile step (ADR 0016): it takes these raw inputs and internally runs
/// the lane's Bronze plan (via [`PublicDataPageRequest::compile_bronze_page_plan`]) to derive the
/// object key, checksum, dedupe key, and the `bronze_object` record — instead of the caller
/// pre-building a [`PlannedBronzeObject`]. This is the single place the upcoming compile-time rules
/// (canonical page-size validation, operation-collapse, the cadastral scope-key, and the
/// reserved-partition-key semantic guard) will attach, for EVERY data.go.kr page lane.
///
/// Each lane keeps a stable named alias: [`BuildingRegisterCommitInput`] and
/// [`RealTransactionCommitInput`] (and the upcoming V-World cadastral / NED / land lanes) are just
/// `Req`-bound aliases of this one struct, so adding a lane adds no new input type.
#[derive(Clone, Debug)]
pub struct PublicDataPageCommitInput<'a, Req> {
    /// Stable lowercase source slug, for example `datagokr__building_register_main`.
    pub source_slug: &'a str,
    /// Ingestion date recorded as run context (the readable object key is not date-partitioned).
    pub ingest_date: NaiveDate,
    /// Ingestion run that owns the page (used by the per-lane plan).
    pub ingestion_run_id: IngestionRunId,
    /// Provider request parameters for this page.
    pub request: Req,
    /// Raw provider response bytes, stored unchanged in Bronze.
    pub raw_payload: Vec<u8>,
    /// Parsed provider response used only for metadata and schema profiling.
    pub payload: JsonValue,
    /// Source catalog entry that produced the object (recorded on the `bronze_object` row).
    pub source_catalog_id: SourceCatalogId,
    /// UTC timestamp recorded as both `collected_at` and `created_at` on the row.
    pub collected_at: DateTime<Utc>,
    /// MIME content type stored with the object and on the row.
    pub content_type: String,
    /// Cache-Control header stored with the object.
    pub cache_control: String,
}

/// Outcome of committing one data.go.kr page, carrying back the compiled shared plan.
///
/// The caller still needs the compiled [`PublicDataBronzePagePlan`] downstream (schema-profile
/// upserts + the run's logical-record / size accounting), so the committer returns it here instead
/// of forcing the caller to recompile it. The plan type is the SAME for every lane (the per-lane
/// `*BronzePagePlan` types are aliases of [`PublicDataBronzePagePlan`]), so one outcome type serves
/// all lanes; [`BuildingRegisterCommitOutcome`] / [`RealTransactionCommitOutcome`] are aliases.
#[derive(Clone, Debug)]
pub struct PublicDataPageCommitOutcome {
    /// Object key written and recorded.
    pub object_key: String,
    /// Lowercase hex SHA-256 of the committed payload.
    pub checksum_sha256: String,
    /// Identifier of the recorded `bronze_object` row.
    pub bronze_object_id: BronzeObjectId,
    /// The compiled plan the committer derived, for downstream schema-profile + accounting reuse.
    pub plan: PublicDataBronzePagePlan,
}

/// Building-register page commit input: a [`PublicDataPageCommitInput`] bound to the
/// building-register lane request. Kept as a named alias so the building-register call site is stable.
pub type BuildingRegisterCommitInput<'a> =
    PublicDataPageCommitInput<'a, BuildingRegisterPageRequest>;

/// Real-transaction page commit input: a [`PublicDataPageCommitInput`] bound to the real-transaction
/// lane request. Kept as a named alias so the real-transaction call site is stable.
pub type RealTransactionCommitInput<'a> = PublicDataPageCommitInput<'a, RealTransactionPageRequest>;

/// V-World NED page commit input: a [`PublicDataPageCommitInput`] bound to the generic V-World NED
/// lane request. Kept as a named alias so the NED call site is stable.
pub type VWorldNedCommitInput<'a> = PublicDataPageCommitInput<'a, VWorldNedPageRequest>;

/// V-World cadastral page commit input: a [`PublicDataPageCommitInput`] bound to the V-World
/// cadastral lane request. Kept as a named alias so the cadastral call site is stable.
pub type VWorldCadastralCommitInput<'a> = PublicDataPageCommitInput<'a, VWorldCadastralPageRequest>;

/// V-World land-register page commit input: a [`PublicDataPageCommitInput`] bound to the V-World
/// land-register lane request. Kept as a named alias so the land-register call site is stable.
pub type VWorldLandRegisterCommitInput<'a> =
    PublicDataPageCommitInput<'a, VWorldLandRegisterPageRequest>;

/// Building-register page commit outcome.
///
/// The compiled plan is a
/// [`BuildingRegisterBronzePagePlan`](crate::building_register_bronze_plan::BuildingRegisterBronzePagePlan)
/// (an alias of the shared [`PublicDataBronzePagePlan`]); kept as a named alias so callers read
/// `BuildingRegisterCommitOutcome` while the underlying type is shared.
pub type BuildingRegisterCommitOutcome = PublicDataPageCommitOutcome;

/// Real-transaction page commit outcome.
///
/// The compiled plan is a
/// [`RealTransactionBronzePagePlan`](crate::real_transaction_bronze_plan::RealTransactionBronzePagePlan)
/// (an alias of the shared [`PublicDataBronzePagePlan`]); kept as a named alias so callers read
/// `RealTransactionCommitOutcome` while the underlying type is shared.
pub type RealTransactionCommitOutcome = PublicDataPageCommitOutcome;

/// V-World NED page commit outcome.
///
/// The compiled plan is a
/// [`VWorldNedBronzePagePlan`](crate::vworld_ned_bronze_plan::VWorldNedBronzePagePlan) (an alias of
/// the shared [`PublicDataBronzePagePlan`]); kept as a named alias so callers read
/// `VWorldNedCommitOutcome` while the underlying type is shared.
pub type VWorldNedCommitOutcome = PublicDataPageCommitOutcome;

/// V-World cadastral page commit outcome.
///
/// The compiled plan is a
/// [`VWorldCadastralBronzePagePlan`](crate::vworld_cadastral_bronze_plan::VWorldCadastralBronzePagePlan)
/// (an alias of the shared [`PublicDataBronzePagePlan`]); kept as a named alias so callers read
/// `VWorldCadastralCommitOutcome` while the underlying type is shared.
pub type VWorldCadastralCommitOutcome = PublicDataPageCommitOutcome;

/// V-World land-register page commit outcome.
///
/// The compiled plan is a
/// [`VWorldLandRegisterBronzePagePlan`](crate::vworld_land_register_bronze_plan::VWorldLandRegisterBronzePagePlan)
/// (an alias of the shared [`PublicDataBronzePagePlan`]); kept as a named alias so callers read
/// `VWorldLandRegisterCommitOutcome` while the underlying type is shared.
pub type VWorldLandRegisterCommitOutcome = PublicDataPageCommitOutcome;

/// Error returned by a Bronze commit.
#[derive(Debug, thiserror::Error)]
pub enum BronzeCommitError {
    /// The raw page identity could not be compiled into a Bronze object key/plan.
    ///
    /// Raised before any storage write or metadata record: the committer owns the key-compile step,
    /// so an invalid request (e.g. malformed region code) surfaces here. The later page-size,
    /// operation-collapse, cadastral scope-key, and semantic-guard rules attach to this same compile
    /// step and will surface through this variant too.
    ///
    /// The source type is the shared [`PublicDataBronzePlanError`] every data.go.kr page lane's plan
    /// returns (`BuildingRegisterBronzePlanError` and `RealTransactionBronzePlanError` are both
    /// aliases of it), so this one variant carries the compile failure for every lane the committer
    /// owns the key-compile for.
    #[error("failed to compile Bronze object plan: {source}")]
    Plan {
        /// Underlying per-lane Bronze plan error.
        #[source]
        source: BuildingRegisterBronzePlanError,
    },
    /// The object storage write failed before the metadata row could be recorded.
    #[error("failed to write Bronze object to storage: {key}: {source}")]
    Storage {
        /// Object key whose write failed.
        key: String,
        /// Underlying storage error.
        #[source]
        source: BronzeStorageError,
    },
    /// The object was written to storage but recording its `bronze_object` row failed.
    ///
    /// On retry, the `CreateOnly` write hits `412`/already-exists; the recoverable commit protocol
    /// then reconciles by checksum and recovers this missing row instead of failing (see
    /// [`BronzeCommitter::commit`]).
    #[error("failed to record Bronze object metadata: {key}: {source}")]
    Record {
        /// Object key whose metadata recording failed.
        key: String,
        /// Underlying Collection persistence error.
        #[source]
        source: CollectionError,
    },
    /// A `CreateOnly` write collided with an existing object whose checksum differs from the
    /// payload this run computed — the key is occupied by *different* content.
    ///
    /// This is the "quarantine" terminal of the recoverable commit protocol (ADR 0016): rather than
    /// build a quarantine subsystem, the committer fails loud with this distinct error so the
    /// conflict is never silently overwritten and an operator can investigate. It is raised both when
    /// a recorded `bronze_object` row has a different checksum and when no row exists but the stored
    /// object's checksum (or missing metadata) does not match.
    #[error("Bronze checksum conflict at existing object key: {key}")]
    ChecksumConflict {
        /// Object key whose existing content checksum does not match this run's payload.
        key: String,
    },
}

/// Error returned by the Bronze raw-object storage writer port.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct BronzeStorageError(pub String);

/// Write-once policy the committer selects for a single Bronze raw write.
///
/// This is `collection-application`'s own storage-port write mode (the services adapter maps it to
/// the low-level `ObjectWriteMode`), so the application layer can request `If-None-Match: *` without
/// depending on the concrete object-storage crate. Bronze raw page writes use
/// [`BronzeWriteMode::CreateOnly`]; mutable writes are out of scope for this seam.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BronzeWriteMode {
    /// Fail if the object key already exists (write-once). A collision surfaces as
    /// [`BronzeWriteOutcome::AlreadyExists`] so the committer can run the recovery protocol.
    CreateOnly,
    /// Unconditionally write, overwriting any existing object at the key.
    OverwriteAllowed,
}

/// Outcome of a single Bronze raw-object write.
///
/// A [`BronzeWriteMode::CreateOnly`] write that collides with an existing key is NOT an error: the
/// adapter reports [`BronzeWriteOutcome::AlreadyExists`] so the committer can reconcile by checksum
/// (recoverable commit protocol, ADR 0016) instead of failing the run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BronzeWriteOutcome {
    /// The object was written at the requested key.
    Written,
    /// A `CreateOnly` write found the key already present; the body was not written.
    AlreadyExists,
}

/// Request handed to the [`BronzeRawObjectWriter`] storage port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BronzeWriteRequest {
    /// Provider-neutral object key.
    pub key: String,
    /// Exact bytes to store at `key`.
    pub body: Vec<u8>,
    /// MIME content type attached to the object.
    pub content_type: String,
    /// Cache-Control header stored with the object.
    pub cache_control: String,
    /// Write-once policy for this object. Bronze raw page writes use
    /// [`BronzeWriteMode::CreateOnly`]; the services adapter maps this to the underlying
    /// `ObjectWriteMode` (`If-None-Match: *` on R2, `create_new` locally).
    pub write_mode: BronzeWriteMode,
    /// Lowercase hex SHA-256 to attach as the object's `x-amz-meta-sha256` metadata on a
    /// non-streaming put, enabling later checksum reconcile of a `CreateOnly` collision via
    /// `head_object`. `None` for writes that do not stamp a checksum.
    pub sha256: Option<String>,
}

/// Storage port the committer writes raw Bronze bytes through.
///
/// `collection-application` must not depend on the concrete object-storage adapter, so the
/// committer owns this narrow write seam. The services layer provides a thin adapter that bridges
/// this port to the existing `ObjectStorageService::put_object`.
#[async_trait]
pub trait BronzeRawObjectWriter: Send + Sync {
    /// Writes one raw Bronze object to the configured storage provider.
    ///
    /// A [`BronzeWriteMode::CreateOnly`] write that collides with an existing key returns
    /// [`BronzeWriteOutcome::AlreadyExists`] (not an error) so the committer reconciles the
    /// collision by checksum.
    ///
    /// # Errors
    ///
    /// Returns [`BronzeStorageError`] when the provider rejects the write or cannot be reached.
    async fn write_object(
        &self,
        request: BronzeWriteRequest,
    ) -> Result<BronzeWriteOutcome, BronzeStorageError>;

    /// Reads back the stored SHA-256 checksum of an existing object, when present.
    ///
    /// Used by the recoverable commit protocol when a `CreateOnly` write collides but no
    /// `bronze_object` row exists yet (R2 written previously, DB record failed): the committer
    /// compares this against the just-computed checksum to decide RECOVER vs quarantine. R2 reads it
    /// from `x-amz-meta-sha256` via `head_object`; the local adapter rehashes the existing file
    /// bytes. Returns `Ok(None)` when the object has no stored checksum.
    ///
    /// # Errors
    ///
    /// Returns [`BronzeStorageError`] when the provider rejects the read or cannot be reached.
    async fn read_object_sha256(&self, key: &str) -> Result<Option<String>, BronzeStorageError>;
}

/// Outcome of one streaming Bronze raw-object write.
///
/// Unlike the in-memory [`BronzeWriteOutcome`], the streaming write computes the payload checksum
/// and size *while the bytes flow to storage*, so a successful write reports them back here — the
/// committer cannot know them before the stream completes. A `CreateOnly` collision is again NOT an
/// error: it reports [`BronzeStreamingWriteOutcome::AlreadyExists`] (the source stream was rejected
/// by `If-None-Match: *` before being consumed, so no checksum is available) so the committer runs
/// the streaming recovery protocol instead of failing the run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BronzeStreamingWriteOutcome {
    /// The object was streamed to the key; `checksum_sha256`/`size_bytes` were computed in-flight.
    Written {
        /// Lowercase hex SHA-256 of the streamed payload, computed incrementally during the upload.
        checksum_sha256: String,
        /// Exact number of bytes streamed to storage.
        size_bytes: u64,
    },
    /// A `CreateOnly` stream found the key already present; the body was not written and no checksum
    /// was computed (the conditional `If-None-Match: *` write was rejected up front).
    AlreadyExists,
}

/// Checksum + size read back from an existing streamed Bronze object by GET-rehashing its bytes.
///
/// The streaming recovery path cannot use stored `x-amz-meta-sha256` metadata (a streaming write
/// never stamps it — the sha is known only post-stream), so it reads the existing object back and
/// rehashes the bytes to recover both the checksum and the size for the missing `bronze_object` row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamedObjectRehash {
    /// Lowercase hex SHA-256 of the existing object's bytes, computed by reading them back.
    pub checksum_sha256: String,
    /// Exact size in bytes of the existing object.
    pub size_bytes: u64,
}

/// Streaming storage port the committer streams large immutable raw Bronze objects through.
///
/// The committer must not buffer a multi-gigabyte bulk file in memory, so this port carries the
/// provider byte source itself (captured by the services-layer implementor around the storage
/// adapter); the committer never touches the bytes. `collection-application` therefore depends on no streaming
/// wire types — only this narrow seam — mirroring [`BronzeRawObjectWriter`] for the in-memory lanes.
#[async_trait]
pub trait BronzeStreamingRawObjectWriter: Send + Sync {
    /// Streams the captured provider byte source to `key` with the write-once `CreateOnly` policy,
    /// computing the payload checksum + size in-flight.
    ///
    /// A `CreateOnly` collision returns [`BronzeStreamingWriteOutcome::AlreadyExists`] (not an
    /// error) so the committer reconciles by GET-rehash. The stream is consumed at most once: a
    /// collision rejects the conditional write before the body is read, so the committer never
    /// re-streams (the provider is not re-downloaded).
    ///
    /// # Errors
    ///
    /// Returns [`BronzeStorageError`] when the provider rejects the write, the stream fails, or the
    /// streamed length disagrees with the declared `Content-Length`.
    async fn write_streaming_object(
        &self,
        request: BronzeStreamingWriteRequest,
    ) -> Result<BronzeStreamingWriteOutcome, BronzeStorageError>;

    /// Reads the existing object at `key` back and rehashes its bytes to obtain its checksum + size.
    ///
    /// Used by the streaming recovery protocol when a `CreateOnly` stream collides but no
    /// `bronze_object` row exists yet (R2 written previously, DB record failed). A streaming write
    /// never stamped `x-amz-meta-sha256`, so — unlike the in-memory page recovery's
    /// [`BronzeRawObjectWriter::read_object_sha256`] HEAD read — recovery must GET the bytes and
    /// rehash them. Returns `Ok(None)` only when the object is absent at read time (a TOCTOU race).
    ///
    /// # Errors
    ///
    /// Returns [`BronzeStorageError`] when the provider rejects the read or the body cannot be read.
    async fn read_object_sha256_by_rehash(
        &self,
        key: &str,
    ) -> Result<Option<StreamedObjectRehash>, BronzeStorageError>;
}

/// Request handed to the [`BronzeStreamingRawObjectWriter`] streaming write port.
///
/// Carries only the object identity + the declared length; the provider byte source itself is held
/// by the services-layer port implementor, so the committer hands no bytes across the seam. The
/// write mode is implicitly [`BronzeWriteMode::CreateOnly`] (write-once); streaming bulk Bronze has
/// no mutable variant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BronzeStreamingWriteRequest {
    /// Provider-neutral object key.
    pub key: String,
    /// MIME content type attached to the object.
    pub content_type: String,
    /// Cache-Control header stored with the object.
    pub cache_control: String,
    /// Declared `Content-Length` of the provider stream, validated against the bytes actually
    /// streamed by the port implementor.
    pub expected_size_bytes: u64,
}

/// A fully-planned streaming Bronze object ready to be committed, minus the checksum + size (which
/// are known only after the stream completes).
///
/// Mirrors [`PlannedBronzeObject`] for the streaming bulk lanes: identity (key, partition, dedupe
/// prefix, request params) is already resolved by the per-lane bulk plan; the committer assembles
/// the final [`BronzeObject`] row once the streaming write reports the in-flight checksum + size.
/// The streamed-to object key + content type live on the carried [`StreamingBronzeRecord`] (single
/// source for both the write request and the recorded row), so they cannot drift apart.
#[derive(Clone, Debug)]
pub struct PlannedStreamingBronzeObject {
    /// Cache-Control header stored with the object.
    pub cache_control: String,
    /// Declared provider `Content-Length`, validated against the streamed bytes.
    pub expected_size_bytes: u64,
    /// Identity fields for the `bronze_object` row that do NOT depend on the streamed bytes; the
    /// committer fills `checksum_sha256`/`size_bytes` from the streaming write outcome (or the
    /// GET-rehash on recovery). Also carries the validated object key + content type used for the
    /// streaming write request.
    pub record: StreamingBronzeRecord,
}

/// The bytes-independent identity of a streaming Bronze object's `bronze_object` row.
///
/// Everything here is resolved by the per-lane bulk plan before any byte streams; the committer
/// completes the row with the checksum + size obtained from the streaming write (or the recovery
/// GET-rehash) and a fresh `bronze_object` id. The `dedupe_key` is parameterized over the checksum
/// via [`StreamingBronzeRecord::dedupe_key_for_checksum`] because the bulk dedupe key embeds the
/// payload sha (`<slug>:<partition>:sha256=<checksum>`), which is unknown until the stream finishes.
#[derive(Clone, Debug)]
pub struct StreamingBronzeRecord {
    /// Validated provider-neutral object key for the raw Bronze payload (the streamed-to key); also
    /// the key the streaming write request targets.
    pub object_key: ObjectKey,
    /// MIME content type stored with the object and on the row.
    pub content_type: String,
    /// Source catalog entry that produced the object.
    pub source_catalog_id: SourceCatalogId,
    /// Ingestion run that owns the streamed object.
    pub ingestion_run_id: IngestionRunId,
    /// Provider partition represented by the file (lineage).
    pub source_partition_key: String,
    /// Canonical source coverage identity for skip / coverage / dedupe.
    pub source_identity_key: String,
    /// Dedupe key prefix WITHOUT the trailing `sha256=<checksum>`; the committer appends the
    /// streamed checksum. Example: `hubgokr__building_register_main:operation=.../provider_file_id=...`.
    pub dedupe_key_prefix: String,
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
    /// Provider file id for bulk sources.
    pub provider_file_id: Option<String>,
    /// Provider file name for bulk sources.
    pub provider_file_name: Option<String>,
    /// Provider update date, when supplied by inventory.
    pub provider_updated_at: Option<NaiveDate>,
    /// UTC timestamp recorded as both `collected_at` and `created_at` on the row.
    pub collected_at: DateTime<Utc>,
}

impl StreamingBronzeRecord {
    /// Builds the bulk dedupe key for a known payload checksum: `<prefix>:sha256=<checksum>`,
    /// matching `plan_public_data_bulk_file_metadata`'s `<slug>:<partition>:sha256=<checksum>` form.
    #[must_use]
    pub fn dedupe_key_for_checksum(&self, checksum_sha256: &str) -> String {
        format!("{}:sha256={checksum_sha256}", self.dedupe_key_prefix)
    }
}

/// The single Bronze commit boundary (ADR 0016).
///
/// Holds no state; it sequences the storage write and the metadata record through the ports it is
/// handed. This keeps it trivially testable and lets every lane share one commit path.
#[derive(Clone, Copy, Debug, Default)]
pub struct BronzeCommitter;

impl BronzeCommitter {
    /// Creates a Bronze committer.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Commits one fully-planned Bronze object: write the raw payload to storage with the write-once
    /// `CreateOnly` policy, then record the `bronze_object` metadata row — recovering self-heal-ably
    /// from a prior R2-success / DB-fail crash.
    ///
    /// R2 and Postgres are NOT one transaction (order: R2 write -> DB record). The recoverable commit
    /// protocol (ADR 0016) makes a `CreateOnly` collision (the storage reports
    /// [`BronzeWriteOutcome::AlreadyExists`], i.e. the R2 `412`) self-healing instead of a hard
    /// failure:
    ///
    /// - **Write `Written`** (key was absent): record the row and succeed — the unchanged 200 path.
    /// - **Write `AlreadyExists` + a `bronze_object` row exists for this key:**
    ///   - same checksum -> **idempotent success** (already collected + recorded; write nothing, record
    ///     nothing, return the existing outcome);
    ///   - different checksum -> [`BronzeCommitError::ChecksumConflict`] (quarantine / fail loud).
    /// - **Write `AlreadyExists` + NO row** (R2 written previously, DB record failed): read the stored
    ///   object's `x-amz-meta-sha256`. If it equals our checksum the object is OURS -> **RECOVER** by
    ///   recording the missing row now; otherwise (different or missing metadata) ->
    ///   [`BronzeCommitError::ChecksumConflict`].
    ///
    /// # Errors
    ///
    /// Returns [`BronzeCommitError::Storage`] when the storage write fails (no metadata is recorded),
    /// [`BronzeCommitError::Record`] when the object was written but its metadata row could not be
    /// recorded, or [`BronzeCommitError::ChecksumConflict`] when an existing object at the key holds
    /// different content than this run's payload.
    pub async fn commit<Writer, Uow>(
        &self,
        writer: &Writer,
        uow: &Uow,
        planned: PlannedBronzeObject,
    ) -> Result<BronzeCommitOutcome, BronzeCommitError>
    where
        Writer: BronzeRawObjectWriter + ?Sized,
        Uow: BronzeIngestUnitOfWork + ?Sized,
    {
        // Task (page-size + key-compile + semantic guard): the canonical page-size validation,
        // operation-collapse key compile, and reserved-partition-key guard plug in HERE, before the
        // write, operating on `planned`. Not in this task — the per-lane plan already produced the key.

        // Destructure once so the body can be moved into the write request while the identity fields
        // the recovery path needs (`object_key`, `checksum_sha256`, `record`) stay owned and intact.
        let PlannedBronzeObject {
            object_key,
            payload,
            content_type,
            cache_control,
            checksum_sha256,
            record,
        } = planned;

        // Intentionally an exhaustive `match`, NOT a single-pattern `let`: when the future
        // `Streaming` variant lands (see `BronzePayload`), this becomes the one compile error that
        // forces the streaming write path to be handled here rather than silently defaulting.
        #[allow(clippy::infallible_destructuring_match)]
        let body = match payload {
            BronzePayload::InMemory(bytes) => bytes,
        };

        // Bronze raw page write: CreateOnly (`If-None-Match: *`) + stamp `x-amz-meta-sha256` so a
        // later collision can be reconciled by checksum without re-downloading the object body.
        let outcome = writer
            .write_object(BronzeWriteRequest {
                key: object_key.clone(),
                body,
                content_type,
                cache_control,
                write_mode: BronzeWriteMode::CreateOnly,
                sha256: Some(checksum_sha256.clone()),
            })
            .await
            .map_err(|source| BronzeCommitError::Storage {
                key: object_key.clone(),
                source,
            })?;

        match outcome {
            // 200 path (key was absent): write succeeded, record the row.
            BronzeWriteOutcome::Written => {
                uow.record_bronze_object(&record).await.map_err(|source| {
                    BronzeCommitError::Record {
                        key: object_key.clone(),
                        source,
                    }
                })?;
            }
            // 412 path (key already existed): NOT a failure — reconcile by checksum.
            BronzeWriteOutcome::AlreadyExists => {
                return self
                    .reconcile_already_exists(writer, uow, &object_key, &checksum_sha256, &record)
                    .await;
            }
        }

        // Task (ledger + event + manifest material): record the DB ledger row, emit the future
        // `raw_written` event (the committer is Kafka's single emit point per ADR 0013), and return
        // manifest material here. Not in this task.

        Ok(BronzeCommitOutcome {
            object_key,
            checksum_sha256,
        })
    }

    /// Recoverable commit protocol — runs when the `CreateOnly` write reported the key already
    /// exists (R2 `412`). Decides idempotent success vs RECOVER vs quarantine by checksum.
    ///
    /// See [`commit`](Self::commit) for the decision tree; this is the `AlreadyExists` branch.
    /// `object_key` / `checksum_sha256` / `record` are the just-written page's identity (the
    /// `record` is the row to recover if the object turns out to be ours).
    async fn reconcile_already_exists<Writer, Uow>(
        &self,
        writer: &Writer,
        uow: &Uow,
        object_key: &str,
        checksum_sha256: &str,
        record: &BronzeObject,
    ) -> Result<BronzeCommitOutcome, BronzeCommitError>
    where
        Writer: BronzeRawObjectWriter + ?Sized,
        Uow: BronzeIngestUnitOfWork + ?Sized,
    {
        let source_catalog_id = record.source_catalog_id;

        // (a) Look up the bronze_object row for this deterministic (source_catalog_id, object_key).
        let existing_row = uow
            .find_bronze_object_by_object_key(source_catalog_id, object_key)
            .await
            .map_err(|source| BronzeCommitError::Record {
                key: object_key.to_owned(),
                source,
            })?;

        if let Some(row) = existing_row {
            // (b) Row EXISTS: same checksum = idempotent success; different = quarantine.
            if row.checksum_sha256 == checksum_sha256 {
                return Ok(BronzeCommitOutcome {
                    object_key: object_key.to_owned(),
                    checksum_sha256: checksum_sha256.to_owned(),
                });
            }
            return Err(BronzeCommitError::ChecksumConflict {
                key: object_key.to_owned(),
            });
        }

        // (c) NO row (R2 written previously, DB record failed): read the stored object's checksum.
        // Matches ours => the object is OURS => RECOVER by recording the missing row now. Different
        // (or missing metadata) => quarantine / fail loud.
        let stored_sha256 = writer
            .read_object_sha256(object_key)
            .await
            .map_err(|source| BronzeCommitError::Storage {
                key: object_key.to_owned(),
                source,
            })?;

        if stored_sha256.as_deref() == Some(checksum_sha256) {
            uow.record_bronze_object(record)
                .await
                .map_err(|source| BronzeCommitError::Record {
                    key: object_key.to_owned(),
                    source,
                })?;
            return Ok(BronzeCommitOutcome {
                object_key: object_key.to_owned(),
                checksum_sha256: checksum_sha256.to_owned(),
            });
        }

        Err(BronzeCommitError::ChecksumConflict {
            key: object_key.to_owned(),
        })
    }

    /// Commits one large immutable Bronze object by STREAMING it: stream the provider byte source to
    /// storage with the write-once `CreateOnly` policy (computing the checksum + size in-flight),
    /// then record the `bronze_object` row from that streamed checksum + size — recovering
    /// self-heal-ably from a prior R2-success / DB-fail crash.
    ///
    /// This is the streaming sibling of [`commit`](Self::commit) for objects too large to buffer in
    /// memory (hub.go.kr / V-World bulk files). The recoverable commit protocol is the same shape,
    /// but the checksum is known ONLY AFTER the stream completes, so the write port cannot stamp
    /// `x-amz-meta-sha256` and the `412` recovery differs:
    ///
    /// - **Stream `Written`** (key was absent): the port computed the checksum + size in-flight;
    ///   build the row from them and record it — the unchanged normal path.
    /// - **Stream `AlreadyExists`** (`CreateOnly` rejected by `If-None-Match: *`; the provider was
    ///   NOT re-downloaded): reconcile via [`reconcile_streaming_already_exists`].
    ///
    /// # Errors
    ///
    /// Returns [`BronzeCommitError::Storage`] when the streaming write fails (no metadata is
    /// recorded), [`BronzeCommitError::Record`] when the object was streamed but its metadata row
    /// could not be recorded, or [`BronzeCommitError::ChecksumConflict`] when an existing object at
    /// the key holds different content than this run streamed (or cannot be read back to prove it is
    /// ours).
    pub async fn commit_streaming_bulk<Writer, Uow>(
        &self,
        writer: &Writer,
        uow: &Uow,
        planned: PlannedStreamingBronzeObject,
    ) -> Result<StreamingBronzeCommitOutcome, BronzeCommitError>
    where
        Writer: BronzeStreamingRawObjectWriter + ?Sized,
        Uow: BronzeIngestUnitOfWork + ?Sized,
    {
        let PlannedStreamingBronzeObject {
            cache_control,
            expected_size_bytes,
            record,
        } = planned;
        let object_key = record.object_key.as_str().to_owned();

        // Stream the raw payload write-once. A 412 collision surfaces as `AlreadyExists` (the
        // conditional `If-None-Match: *` write was rejected before the stream was consumed), NOT a
        // failure — so the provider is never re-downloaded on the recovery path.
        let outcome = writer
            .write_streaming_object(BronzeStreamingWriteRequest {
                key: object_key.clone(),
                content_type: record.content_type.clone(),
                cache_control,
                expected_size_bytes,
            })
            .await
            .map_err(|source| BronzeCommitError::Storage {
                key: object_key.clone(),
                source,
            })?;

        match outcome {
            // Normal path (key was absent): the stream computed the checksum + size in-flight.
            BronzeStreamingWriteOutcome::Written {
                checksum_sha256,
                size_bytes,
            } => {
                let row = build_streaming_bronze_object(&record, &checksum_sha256, size_bytes);
                let bronze_object_id = row.id;
                uow.record_bronze_object(&row).await.map_err(|source| {
                    BronzeCommitError::Record {
                        key: object_key.clone(),
                        source,
                    }
                })?;
                Ok(StreamingBronzeCommitOutcome {
                    object_key,
                    checksum_sha256,
                    size_bytes,
                    bronze_object_id,
                })
            }
            // 412 path (key already existed): NOT a failure — reconcile by GET-rehash.
            BronzeStreamingWriteOutcome::AlreadyExists => {
                self.reconcile_streaming_already_exists(writer, uow, &object_key, &record)
                    .await
            }
        }
    }

    /// Streaming recoverable commit protocol — runs when the `CreateOnly` STREAM reported the key
    /// already exists (R2 `412`). Decides idempotent skip vs RECOVER vs quarantine.
    ///
    /// The checksum was NOT computed (the stream was rejected up front) and was never stamped as
    /// `x-amz-meta-sha256`, so unlike the in-memory [`reconcile_already_exists`] this path cannot
    /// HEAD-read a stored checksum. Instead:
    ///
    /// - **A `bronze_object` row exists for `(source_catalog_id, object_key)`** → **idempotent
    ///   skip**: the `object_key` is content-stable for these bulk lanes (each published file gets a
    ///   fresh provider id → same key ⇒ same content by construction), so the existing object is
    ///   ours; write nothing, record nothing, return the existing row's checksum.
    /// - **NO row** (R2 streamed previously, DB record failed) → **GET-rehash** the existing object
    ///   to obtain its checksum + size, then RECOVER by recording the missing row from them. If the
    ///   object cannot be read/rehashed (absent at read time / read failure) → fail loud
    ///   [`BronzeCommitError::ChecksumConflict`].
    async fn reconcile_streaming_already_exists<Writer, Uow>(
        &self,
        writer: &Writer,
        uow: &Uow,
        object_key: &str,
        record: &StreamingBronzeRecord,
    ) -> Result<StreamingBronzeCommitOutcome, BronzeCommitError>
    where
        Writer: BronzeStreamingRawObjectWriter + ?Sized,
        Uow: BronzeIngestUnitOfWork + ?Sized,
    {
        let source_catalog_id = record.source_catalog_id;

        // (a) Look up the bronze_object row for this deterministic (source_catalog_id, object_key).
        let existing_row = uow
            .find_bronze_object_by_object_key(source_catalog_id, object_key)
            .await
            .map_err(|source| BronzeCommitError::Record {
                key: object_key.to_owned(),
                source,
            })?;

        // (b) Row EXISTS: idempotent skip. The bulk object_key is content-stable (a fresh provider
        // id is minted per published file), so the existing object IS the one this run would have
        // streamed — already collected + recorded on a prior run. Return its recorded identity.
        if let Some(row) = existing_row {
            return Ok(StreamingBronzeCommitOutcome {
                object_key: object_key.to_owned(),
                checksum_sha256: row.checksum_sha256,
                size_bytes: row.size_bytes,
                bronze_object_id: row.id,
            });
        }

        // (c) NO row (R2 streamed previously, DB record failed): GET-rehash the existing object to
        // obtain its checksum + size — a streaming write never stamped x-amz-meta-sha256, so HEAD
        // metadata is unavailable. Record the missing row from the rehash (RECOVER). If the object
        // cannot be read back / rehashed, fail loud rather than silently treat 412 as a no-op.
        let rehash = writer
            .read_object_sha256_by_rehash(object_key)
            .await
            .map_err(|source| BronzeCommitError::Storage {
                key: object_key.to_owned(),
                source,
            })?
            .ok_or_else(|| BronzeCommitError::ChecksumConflict {
                key: object_key.to_owned(),
            })?;

        let row = build_streaming_bronze_object(record, &rehash.checksum_sha256, rehash.size_bytes);
        let bronze_object_id = row.id;
        uow.record_bronze_object(&row)
            .await
            .map_err(|source| BronzeCommitError::Record {
                key: object_key.to_owned(),
                source,
            })?;
        Ok(StreamingBronzeCommitOutcome {
            object_key: object_key.to_owned(),
            checksum_sha256: rehash.checksum_sha256,
            size_bytes: rehash.size_bytes,
            bronze_object_id,
        })
    }

    /// Commits one data.go.kr page from its RAW identity + payload — the SINGLE generic page-commit
    /// path every data.go.kr page lane uses.
    ///
    /// The committer owns the key-compile step (ADR 0016): it runs the lane's Bronze plan through
    /// [`PublicDataPageRequest::compile_bronze_page_plan`] to derive the object key, checksum, dedupe
    /// key, and the `bronze_object` record, then routes the result through the SAME shared
    /// [`commit`](Self::commit) write+record+recovery core. Behaviour is identical to the previous
    /// per-lane `commit_<lane>_page` methods: same object key, same bytes, same recorded row, same
    /// recovery semantics, same error messages — only the duplicated body is now collapsed to one.
    ///
    /// This is the single place the upcoming per-lane compile-time rules (canonical page-size
    /// validation, operation-collapse, the cadastral scope-key, and the reserved-partition-key
    /// semantic guard) attach — they will run inside the lane's `compile_bronze_page_plan` (or the
    /// shared `plan_public_data_bronze_page` it delegates to), derived here before the write, so this
    /// one generic method does not block them. Adding the remaining V-World cadastral / NED / land
    /// page lanes is therefore a new [`PublicDataPageRequest`] impl, not a new commit method.
    ///
    /// # Errors
    ///
    /// Returns [`BronzeCommitError::Plan`] when the raw identity cannot be compiled into a valid
    /// Bronze object key/plan, [`BronzeCommitError::Storage`] when the storage write fails (no
    /// metadata is recorded), [`BronzeCommitError::Record`] when the object was written but its
    /// metadata row could not be recorded, or [`BronzeCommitError::ChecksumConflict`] when an existing
    /// object at the key holds different content than this run's payload.
    pub async fn commit_public_data_page<Writer, Uow, Req>(
        &self,
        writer: &Writer,
        uow: &Uow,
        input: PublicDataPageCommitInput<'_, Req>,
    ) -> Result<PublicDataPageCommitOutcome, BronzeCommitError>
    where
        Writer: BronzeRawObjectWriter + ?Sized,
        Uow: BronzeIngestUnitOfWork + ?Sized,
        Req: PublicDataPageRequest,
    {
        // The committer's owned key-compile: each lane supplies its provider partition shape +
        // validation + logical-items pointer + candidate-key suffixes through the trait, producing the
        // SHARED page plan. The page-size / operation-collapse / cadastral-scope / semantic-guard rules
        // plug in INSIDE this compile (the lane impl or its shared `plan_public_data_bronze_page`).
        let plan = input
            .request
            .compile_bronze_page_plan(
                input.source_slug,
                input.ingest_date,
                input.ingestion_run_id,
                input.raw_payload,
                input.payload,
            )
            .map_err(|source| BronzeCommitError::Plan { source })?;

        let record = BronzeObject {
            id: BronzeObjectId::new(Uuid::new_v4()),
            source_catalog_id: input.source_catalog_id,
            ingestion_run_id: input.ingestion_run_id,
            source_record_id: None,
            source_partition_key: Some(plan.source_partition_key.clone()),
            source_identity_key: plan.source_identity_key.clone(),
            dedupe_key: plan.dedupe_key.clone(),
            request_params: plan.request_params.clone(),
            object_key: plan.object_key.clone(),
            checksum_sha256: plan.checksum_sha256.clone(),
            content_type: input.content_type.clone(),
            size_bytes: plan.size_bytes,
            logical_record_count: Some(plan.logical_record_count),
            collected_at: input.collected_at,
            snapshot_period: plan.snapshot_period.clone(),
            snapshot_date: plan.snapshot_date,
            snapshot_granularity: plan.snapshot_granularity,
            snapshot_basis: plan.snapshot_basis,
            provider_file_id: None,
            provider_file_name: None,
            provider_updated_at: None,
            effective_date: None,
            created_at: input.collected_at,
        };
        let bronze_object_id = record.id;

        let planned = PlannedBronzeObject {
            object_key: plan.object_key.as_str().to_owned(),
            payload: BronzePayload::InMemory(plan.raw_payload.clone()),
            content_type: input.content_type,
            cache_control: input.cache_control,
            checksum_sha256: plan.checksum_sha256.clone(),
            record,
        };

        let outcome = self.commit(writer, uow, planned).await?;

        Ok(PublicDataPageCommitOutcome {
            object_key: outcome.object_key,
            checksum_sha256: outcome.checksum_sha256,
            bronze_object_id,
            plan,
        })
    }

    /// Commits one building-register page — a thin wrapper over the generic
    /// [`commit_public_data_page`](Self::commit_public_data_page) that keeps the building-register
    /// call site stable. The duplicated commit body lives ONCE, in the generic.
    ///
    /// # Errors
    ///
    /// See [`commit_public_data_page`](Self::commit_public_data_page).
    pub async fn commit_building_register_page<Writer, Uow>(
        &self,
        writer: &Writer,
        uow: &Uow,
        input: BuildingRegisterCommitInput<'_>,
    ) -> Result<BuildingRegisterCommitOutcome, BronzeCommitError>
    where
        Writer: BronzeRawObjectWriter + ?Sized,
        Uow: BronzeIngestUnitOfWork + ?Sized,
    {
        self.commit_public_data_page(writer, uow, input).await
    }

    /// Commits one real-transaction page — a thin wrapper over the generic
    /// [`commit_public_data_page`](Self::commit_public_data_page) that keeps the real-transaction
    /// call site stable. The duplicated commit body lives ONCE, in the generic.
    ///
    /// # Errors
    ///
    /// See [`commit_public_data_page`](Self::commit_public_data_page).
    pub async fn commit_real_transaction_page<Writer, Uow>(
        &self,
        writer: &Writer,
        uow: &Uow,
        input: RealTransactionCommitInput<'_>,
    ) -> Result<RealTransactionCommitOutcome, BronzeCommitError>
    where
        Writer: BronzeRawObjectWriter + ?Sized,
        Uow: BronzeIngestUnitOfWork + ?Sized,
    {
        self.commit_public_data_page(writer, uow, input).await
    }

    /// Commits one generic V-World NED page — a thin wrapper over the generic
    /// [`commit_public_data_page`](Self::commit_public_data_page) that keeps the NED call site
    /// stable. The duplicated commit body lives ONCE, in the generic.
    ///
    /// # Errors
    ///
    /// See [`commit_public_data_page`](Self::commit_public_data_page).
    pub async fn commit_vworld_ned_page<Writer, Uow>(
        &self,
        writer: &Writer,
        uow: &Uow,
        input: VWorldNedCommitInput<'_>,
    ) -> Result<VWorldNedCommitOutcome, BronzeCommitError>
    where
        Writer: BronzeRawObjectWriter + ?Sized,
        Uow: BronzeIngestUnitOfWork + ?Sized,
    {
        self.commit_public_data_page(writer, uow, input).await
    }

    /// Commits one V-World cadastral page — a thin wrapper over the generic
    /// [`commit_public_data_page`](Self::commit_public_data_page) that keeps the cadastral call site
    /// stable. The duplicated commit body lives ONCE, in the generic; the cadastral lane's redesigned
    /// object key (human-readable `attr_filter` scope, no `operation`/`filter_kind`/`size` segments —
    /// Task 3 / T1.1) is produced inside its `compile_bronze_page_plan`, so routing it here gives it
    /// `CreateOnly` + the recoverable commit protocol for free.
    ///
    /// # Errors
    ///
    /// See [`commit_public_data_page`](Self::commit_public_data_page).
    pub async fn commit_vworld_cadastral_page<Writer, Uow>(
        &self,
        writer: &Writer,
        uow: &Uow,
        input: VWorldCadastralCommitInput<'_>,
    ) -> Result<VWorldCadastralCommitOutcome, BronzeCommitError>
    where
        Writer: BronzeRawObjectWriter + ?Sized,
        Uow: BronzeIngestUnitOfWork + ?Sized,
    {
        self.commit_public_data_page(writer, uow, input).await
    }

    /// Commits one V-World land-register page — a thin wrapper over the generic
    /// [`commit_public_data_page`](Self::commit_public_data_page) that keeps the land-register call
    /// site stable. The duplicated commit body lives ONCE, in the generic.
    ///
    /// # Errors
    ///
    /// See [`commit_public_data_page`](Self::commit_public_data_page).
    pub async fn commit_vworld_land_register_page<Writer, Uow>(
        &self,
        writer: &Writer,
        uow: &Uow,
        input: VWorldLandRegisterCommitInput<'_>,
    ) -> Result<VWorldLandRegisterCommitOutcome, BronzeCommitError>
    where
        Writer: BronzeRawObjectWriter + ?Sized,
        Uow: BronzeIngestUnitOfWork + ?Sized,
    {
        self.commit_public_data_page(writer, uow, input).await
    }
}

/// Assembles the `bronze_object` row for a streamed object once its checksum + size are known.
///
/// The non-checksum/size identity comes from the planned [`StreamingBronzeRecord`]; the checksum +
/// size are supplied by the streaming write outcome (normal path) or the recovery GET-rehash. The
/// dedupe key is completed by appending `:sha256=<checksum>` to the planned prefix, matching the
/// bulk plan's `<slug>:<partition>:sha256=<checksum>` form.
fn build_streaming_bronze_object(
    record: &StreamingBronzeRecord,
    checksum_sha256: &str,
    size_bytes: u64,
) -> BronzeObject {
    BronzeObject {
        id: BronzeObjectId::new(Uuid::new_v4()),
        source_catalog_id: record.source_catalog_id,
        ingestion_run_id: record.ingestion_run_id,
        source_record_id: None,
        source_partition_key: Some(record.source_partition_key.clone()),
        source_identity_key: record.source_identity_key.clone(),
        dedupe_key: record.dedupe_key_for_checksum(checksum_sha256),
        request_params: record.request_params.clone(),
        object_key: record.object_key.clone(),
        checksum_sha256: checksum_sha256.to_owned(),
        content_type: record.content_type.clone(),
        size_bytes,
        logical_record_count: None,
        collected_at: record.collected_at,
        snapshot_period: record.snapshot_period.clone(),
        snapshot_date: record.snapshot_date,
        snapshot_granularity: record.snapshot_granularity,
        snapshot_basis: record.snapshot_basis,
        provider_file_id: record.provider_file_id.clone(),
        provider_file_name: record.provider_file_name.clone(),
        provider_updated_at: record.provider_updated_at,
        effective_date: None,
        created_at: record.collected_at,
    }
}

#[cfg(test)]
#[path = "bronze_committer/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "bronze_committer/streaming_tests.rs"]
mod streaming_tests;
