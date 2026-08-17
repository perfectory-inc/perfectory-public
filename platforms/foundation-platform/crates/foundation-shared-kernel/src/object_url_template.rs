//! Address template that turns a provider-neutral object key into a fetchable URL.
//!
//! An [`ObjectKey`] says which object a pointer refers to. It does not say where a client can
//! read it. Publishing the template next to the key keeps the artifact itself address-free
//! (it can be mirrored or moved) while still handing consumers something they can fetch,
//! which is the same split `tiles_url_template` already makes for vector tiles.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::object_key::ObjectKey;

/// The single placeholder an object URL template must carry.
pub const OBJECT_KEY_PLACEHOLDER: &str = "{object_key}";

const HTTPS_SCHEME: &str = "https://";
const HTTP_SCHEME: &str = "http://";

/// Validated address template for one object key.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObjectUrlTemplate(String);

impl ObjectUrlTemplate {
    /// Builds a validated object URL template.
    ///
    /// # Errors
    /// Returns [`ObjectUrlTemplateError`] when the template is empty or padded, carries a query
    /// or fragment, does not contain `{object_key}` exactly once inside its path, carries an
    /// unknown placeholder, or is not an absolute HTTPS URL (plain HTTP is admitted only for
    /// loopback hosts, so local runs stay possible without weakening the published contract).
    pub fn parse(raw: &str) -> Result<Self, ObjectUrlTemplateError> {
        if raw.is_empty() {
            return Err(ObjectUrlTemplateError::Empty);
        }
        if raw.trim() != raw {
            return Err(ObjectUrlTemplateError::Padded);
        }
        if raw.contains('?') || raw.contains('#') {
            return Err(ObjectUrlTemplateError::QueryOrFragment);
        }

        let placeholder_count = raw.matches(OBJECT_KEY_PLACEHOLDER).count();
        if placeholder_count != 1 {
            return Err(ObjectUrlTemplateError::PlaceholderOccurrences(
                placeholder_count,
            ));
        }
        if raw.replace(OBJECT_KEY_PLACEHOLDER, "").contains(['{', '}']) {
            return Err(ObjectUrlTemplateError::UnknownPlaceholder);
        }

        let authority_and_path = parse_scheme(raw)?;
        let (authority, path) = split_authority(authority_and_path)?;
        if authority.is_empty() {
            return Err(ObjectUrlTemplateError::MissingHost);
        }
        if !path.contains(OBJECT_KEY_PLACEHOLDER) {
            return Err(ObjectUrlTemplateError::PlaceholderOutsidePath);
        }

        Ok(Self(raw.to_owned()))
    }

    /// Returns the validated template string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Materializes the fetchable URL for one object key.
    #[must_use]
    pub fn materialize(&self, object_key: &ObjectKey) -> String {
        self.0.replace(OBJECT_KEY_PLACEHOLDER, object_key.as_str())
    }
}

/// Validation errors returned while parsing object URL templates.
#[derive(Debug, Error)]
pub enum ObjectUrlTemplateError {
    /// Template was empty.
    #[error("object URL template must not be empty")]
    Empty,
    /// Template carried surrounding whitespace.
    #[error("object URL template must not have surrounding whitespace")]
    Padded,
    /// Template carried a query string or fragment.
    #[error("object URL template must not contain a query string or fragment")]
    QueryOrFragment,
    /// Template did not carry `{object_key}` exactly once.
    #[error("object URL template must contain {OBJECT_KEY_PLACEHOLDER} exactly once, found {0}")]
    PlaceholderOccurrences(usize),
    /// Template carried a placeholder other than `{object_key}`.
    #[error(
        "object URL template must not contain a placeholder other than {OBJECT_KEY_PLACEHOLDER}"
    )]
    UnknownPlaceholder,
    /// Template was not an absolute HTTP(S) URL.
    #[error("object URL template must be an absolute https:// URL")]
    NotAbsoluteHttps,
    /// Template used plain HTTP against a non-loopback host.
    #[error("object URL template may only use http:// for loopback hosts")]
    InsecureNonLoopbackHost,
    /// Template carried no host.
    #[error("object URL template must contain a host")]
    MissingHost,
    /// Template placed `{object_key}` in the scheme or authority instead of the path.
    #[error("object URL template must place {OBJECT_KEY_PLACEHOLDER} in the URL path")]
    PlaceholderOutsidePath,
}

fn parse_scheme(raw: &str) -> Result<&str, ObjectUrlTemplateError> {
    if let Some(rest) = raw.strip_prefix(HTTPS_SCHEME) {
        return Ok(rest);
    }
    let Some(rest) = raw.strip_prefix(HTTP_SCHEME) else {
        return Err(ObjectUrlTemplateError::NotAbsoluteHttps);
    };
    let (authority, _) = split_authority(rest)?;
    if is_loopback_authority(authority) {
        Ok(rest)
    } else {
        Err(ObjectUrlTemplateError::InsecureNonLoopbackHost)
    }
}

fn split_authority(authority_and_path: &str) -> Result<(&str, &str), ObjectUrlTemplateError> {
    authority_and_path.find('/').map_or_else(
        || Err(ObjectUrlTemplateError::PlaceholderOutsidePath),
        |index| Ok(authority_and_path.split_at(index)),
    )
}

fn is_loopback_authority(authority: &str) -> bool {
    let host = authority
        .rsplit_once(':')
        .map_or(authority, |(host, port)| {
            if port.bytes().all(|byte| byte.is_ascii_digit()) {
                host
            } else {
                authority
            }
        });
    host == "localhost" || host == "127.0.0.1" || host == "[::1]" || host.ends_with(".localhost")
}
