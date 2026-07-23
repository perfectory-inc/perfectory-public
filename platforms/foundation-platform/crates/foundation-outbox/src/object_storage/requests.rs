//! Request/response value types and the smoke report for the object storage port.

pub use aws_sdk_s3::primitives::ByteStream;

/// Write-once policy a caller selects for a single object write.
///
/// This is the storage-port half of the object commit protocol:
/// Bronze raw objects and immutable derived artifacts use
/// [`ObjectWriteMode::CreateOnly`] (R2 `If-None-Match: *`, local `create_new`).
/// Stable pointers and disposable smoke/scratch objects use
/// [`ObjectWriteMode::OverwriteAllowed`]. Iceberg owns Silver/Gold table-file
/// commit semantics rather than relying on this enum.
///
/// No `Default` is provided on purpose: every construction site must choose a mode
/// explicitly so the choice is compile-forced rather than inherited silently.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectWriteMode {
    /// Fail if the object key already exists (write-once). The adapter maps the
    /// provider's "already exists" signal to [`PublishError::ObjectAlreadyExists`].
    CreateOnly,
    /// Unconditionally write, overwriting any existing object at the key.
    OverwriteAllowed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Request to write an object into the configured object storage provider.
pub struct PutObjectRequest {
    /// Provider-relative object key.
    pub key: String,
    /// Exact bytes to store at `key`.
    pub body: Vec<u8>,
    /// MIME content type attached to the object.
    pub content_type: String,
    /// Cache-Control header stored with the object.
    pub cache_control: String,
    /// Write-once policy for this object (no default — caller must choose).
    pub write_mode: ObjectWriteMode,
    /// Optional lowercase hex SHA-256 stored as the object's `x-amz-meta-sha256` user metadata.
    ///
    /// The non-streaming Bronze commit path stamps this so a later `CreateOnly` collision can be
    /// reconciled by checksum via [`ObjectStorageService::read_object_sha256`] (R2 `head_object`)
    /// without re-downloading the body. `None` leaves no checksum metadata.
    pub sha256: Option<String>,
}

/// Request to stream an object into the configured object storage provider.
pub struct StreamingPutObjectRequest {
    /// Provider-relative object key.
    pub key: String,
    /// MIME content type attached to the object.
    pub content_type: String,
    /// Cache-Control header stored with the object.
    pub cache_control: String,
    /// Expected stream length in bytes.
    pub size_bytes: u64,
    /// Provider byte stream to store at `key`.
    pub body: ByteStream,
    /// Write-once policy for this object (no default — caller must choose).
    pub write_mode: ObjectWriteMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Result of a successful object storage smoke round trip.
pub struct ObjectStorageSmokeReport {
    /// Object key used for the smoke object.
    pub key: String,
    /// Number of bytes written and read back exactly.
    pub bytes_verified: usize,
    /// Number of successful write requests in this smoke run.
    pub put_request_count: u64,
    /// Number of successful read requests in this smoke run.
    pub get_request_count: u64,
    /// Number of successful delete requests in this smoke run.
    pub delete_request_count: u64,
}

impl ObjectStorageSmokeReport {
    /// Renders this smoke report as Prometheus text exposition.
    ///
    /// The object key is intentionally not included as a label to avoid high-cardinality
    /// scrape output and accidental disclosure of provider-relative paths.
    #[must_use]
    pub fn to_prometheus_metrics(&self, source: &str) -> String {
        let source = prometheus_label_value(source);
        [
            "# HELP foundation_platform_r2_smoke_request_total R2 smoke operation requests completed by the latest smoke run.".to_owned(),
            "# TYPE foundation_platform_r2_smoke_request_total counter".to_owned(),
            "# HELP foundation_platform_r2_smoke_bytes_verified Bytes written and read back exactly by the latest R2 smoke run.".to_owned(),
            "# TYPE foundation_platform_r2_smoke_bytes_verified gauge".to_owned(),
            format!(
                "foundation_platform_r2_smoke_request_total{{source=\"{source}\",operation=\"put\"}} {}",
                self.put_request_count
            ),
            format!(
                "foundation_platform_r2_smoke_request_total{{source=\"{source}\",operation=\"get\"}} {}",
                self.get_request_count
            ),
            format!(
                "foundation_platform_r2_smoke_request_total{{source=\"{source}\",operation=\"delete\"}} {}",
                self.delete_request_count
            ),
            format!(
                "foundation_platform_r2_smoke_bytes_verified{{source=\"{source}\"}} {}",
                self.bytes_verified
            ),
        ]
        .join("\n")
    }
}

/// Checksum + size of an existing object obtained by reading its bytes back and rehashing them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamingObjectRehash {
    /// Lowercase hex SHA-256 of the read-back bytes.
    pub checksum_sha256: String,
    /// Exact size in bytes of the read-back object.
    pub size_bytes: u64,
    /// Opaque storage entity tag observed for the stable read, when exposed by the adapter.
    pub observed_e_tag: Option<String>,
    /// Provider timestamp observed for the stable read, when the adapter exposes one.
    pub observed_last_modified: Option<String>,
}

fn prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
