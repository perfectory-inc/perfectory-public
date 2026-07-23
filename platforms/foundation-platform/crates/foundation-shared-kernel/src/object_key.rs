//! Provider-neutral object storage key value objects.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Provider-neutral object storage key.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObjectKey(String);

impl ObjectKey {
    /// Builds a provider-neutral object key.
    ///
    /// # Errors
    /// Returns [`ObjectKeyError`] when the value is empty or path-like in a way that
    /// would make object ownership ambiguous.
    pub fn parse(raw: &str) -> Result<Self, ObjectKeyError> {
        if raw.is_empty() {
            return Err(ObjectKeyError::Empty);
        }
        if raw.starts_with('/') {
            return Err(ObjectKeyError::LeadingSlash);
        }
        if raw.contains('\\') {
            return Err(ObjectKeyError::Backslash);
        }
        if raw
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
        {
            return Err(ObjectKeyError::AmbiguousSegment);
        }
        if gold_path_contains_semantic_version(raw) {
            return Err(ObjectKeyError::SemanticVersionInGoldPath);
        }
        Ok(Self(raw.to_owned()))
    }

    /// Returns the validated object key string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Provider-neutral object storage key prefix.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObjectKeyPrefix(String);

impl ObjectKeyPrefix {
    /// Builds a provider-neutral object key prefix for a set of immutable objects.
    ///
    /// # Errors
    /// Returns [`ObjectKeyError`] when the value is empty or path-like in a way that
    /// would make object ownership ambiguous.
    pub fn parse(raw: &str) -> Result<Self, ObjectKeyError> {
        if raw.is_empty() {
            return Err(ObjectKeyError::Empty);
        }
        if raw.starts_with('/') {
            return Err(ObjectKeyError::LeadingSlash);
        }
        if raw.contains('\\') {
            return Err(ObjectKeyError::Backslash);
        }
        let normalized = raw.strip_suffix('/').unwrap_or(raw);
        if normalized.is_empty()
            || normalized
                .split('/')
                .any(|segment| segment.is_empty() || segment == "." || segment == "..")
        {
            return Err(ObjectKeyError::AmbiguousSegment);
        }
        if gold_path_contains_semantic_version(normalized) {
            return Err(ObjectKeyError::SemanticVersionInGoldPath);
        }
        Ok(Self(raw.to_owned()))
    }

    /// Returns the validated object key prefix string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Validation errors returned while parsing object keys.
#[derive(Debug, Error)]
pub enum ObjectKeyError {
    /// Object key was empty.
    #[error("object key must not be empty")]
    Empty,
    /// Object key started with a slash.
    #[error("object key must not start with '/'")]
    LeadingSlash,
    /// Object key used a backslash separator.
    #[error("object key must not contain backslash separators")]
    Backslash,
    /// Object key contained empty, current-directory, or parent-directory segments.
    #[error("object key must not contain empty, '.', or '..' path segments")]
    AmbiguousSegment,
    /// Gold paths used a semantic data version instead of Catalog metadata.
    #[error("Gold object key must keep semantic data versions in Catalog metadata")]
    SemanticVersionInGoldPath,
}

fn gold_path_contains_semantic_version(raw: &str) -> bool {
    if !raw.starts_with("gold/") {
        return false;
    }

    raw.split('/').any(|segment| {
        let lowered = segment.to_ascii_lowercase();
        lowered.starts_with("version=")
            || lowered.split(['.', '-', '_']).any(is_simple_version_token)
    })
}

fn is_simple_version_token(token: &str) -> bool {
    token.strip_prefix('v').is_some_and(|digits| {
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
    })
}
