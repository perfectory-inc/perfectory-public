//! Read-only R2 inventory request/report value types and prefix normalization.

use crate::errors::PublishError;

/// Default maximum number of R2 keys returned by a read-only inventory check.
pub const DEFAULT_R2_INVENTORY_MAX_KEYS: i32 = 100;
/// Upper bound for one read-only R2 inventory check.
pub const MAX_R2_INVENTORY_MAX_KEYS: i32 = 1_000;

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::module_name_repetitions)]
/// Read-only request for a bounded R2 object inventory check.
pub struct R2InventoryRequest {
    prefix: Option<String>,
    max_keys: i32,
}

impl R2InventoryRequest {
    /// Creates a read-only R2 inventory request.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when the prefix is ambiguous or `max_keys` is outside
    /// the bounded range accepted by the S3-compatible `ListObjectsV2` API.
    pub fn new(prefix: Option<&str>, max_keys: Option<i32>) -> Result<Self, PublishError> {
        let max_keys = max_keys.unwrap_or(DEFAULT_R2_INVENTORY_MAX_KEYS);
        if !(1..=MAX_R2_INVENTORY_MAX_KEYS).contains(&max_keys) {
            return Err(PublishError::Infrastructure(format!(
                "R2 inventory max_keys must be between 1 and {MAX_R2_INVENTORY_MAX_KEYS}"
            )));
        }

        Ok(Self {
            prefix: normalize_r2_inventory_prefix(prefix)?,
            max_keys,
        })
    }

    /// Provider-relative prefix to inspect, or root when absent.
    #[must_use]
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// Maximum number of keys requested from R2.
    #[must_use]
    pub const fn max_keys(&self) -> i32 {
        self.max_keys
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::module_name_repetitions)]
/// Summary returned by a read-only R2 inventory check.
pub struct R2InventoryReport {
    pub(super) prefix: Option<String>,
    pub(super) max_keys: i32,
    pub(super) key_count: i32,
    pub(super) is_truncated: bool,
    pub(super) common_prefixes: Vec<String>,
    pub(super) objects: Vec<R2InventoryObject>,
}

/// Recursive R2 inventory result for audit workflows that must inspect every object.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::module_name_repetitions)]
pub struct R2InventoryAuditReport {
    pub(super) prefix: Option<String>,
    pub(super) max_keys: i32,
    pub(super) list_request_count: usize,
    pub(super) objects: Vec<R2InventoryObject>,
}

impl R2InventoryAuditReport {
    /// Prefix inspected by the recursive inventory audit, or root when absent.
    #[must_use]
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// Maximum number of keys requested per R2 page.
    #[must_use]
    pub const fn max_keys(&self) -> i32 {
        self.max_keys
    }

    /// Number of `ListObjectsV2` requests required to exhaust the requested scope.
    #[must_use]
    pub const fn list_request_count(&self) -> usize {
        self.list_request_count
    }

    /// Objects returned across every page in provider order.
    #[must_use]
    pub fn objects(&self) -> &[R2InventoryObject] {
        &self.objects
    }
}

impl R2InventoryReport {
    /// Prefix inspected by the inventory check, or root when absent.
    #[must_use]
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// Maximum number of keys requested from R2.
    #[must_use]
    pub const fn max_keys(&self) -> i32 {
        self.max_keys
    }

    /// Number of keys R2 says were returned in this bounded page.
    #[must_use]
    pub const fn key_count(&self) -> i32 {
        self.key_count
    }

    /// Whether more objects exist beyond this bounded page.
    #[must_use]
    pub const fn is_truncated(&self) -> bool {
        self.is_truncated
    }

    /// One-level child prefixes returned by R2 when using `/` as delimiter.
    #[must_use]
    pub fn common_prefixes(&self) -> &[String] {
        &self.common_prefixes
    }

    /// Objects returned directly under the requested prefix page.
    #[must_use]
    pub fn objects(&self) -> &[R2InventoryObject] {
        &self.objects
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::module_name_repetitions)]
/// Object entry returned by a read-only R2 inventory check.
pub struct R2InventoryObject {
    /// Provider-relative object key.
    pub key: String,
    /// Object size in bytes reported by R2.
    pub size_bytes: i64,
    /// Opaque entity tag reported by R2, when present.
    pub e_tag: Option<String>,
    /// Provider timestamp rendered in RFC3339-like text when present.
    pub last_modified: Option<String>,
}

/// Normalizes a read-only R2 inventory prefix.
///
/// # Errors
///
/// Returns `PublishError` when the prefix is absolute, path-like, or ambiguous.
pub fn normalize_r2_inventory_prefix(prefix: Option<&str>) -> Result<Option<String>, PublishError> {
    let Some(raw) = prefix else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed != raw {
        return Err(r2_inventory_prefix_error(
            "must not contain leading or trailing whitespace",
        ));
    }
    if trimmed.starts_with('/') {
        return Err(r2_inventory_prefix_error(
            "must be a relative object prefix",
        ));
    }
    if trimmed.contains('\\') {
        return Err(r2_inventory_prefix_error(
            "must not contain backslash separators",
        ));
    }
    if trimmed.contains("..") {
        return Err(r2_inventory_prefix_error(
            "must not contain traversal markers",
        ));
    }

    let normalized = trimmed.strip_suffix('/').unwrap_or(trimmed);
    if normalized
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(r2_inventory_prefix_error(
            "must not contain empty, '.', or '..' path segments",
        ));
    }

    Ok(Some(trimmed.to_owned()))
}

fn r2_inventory_prefix_error(reason: &str) -> PublishError {
    PublishError::Infrastructure(format!("R2 inventory prefix {reason}"))
}
