//! The staging key namespace of the lakehouse bucket (root ADR-0177).
//!
//! Staging uses the production bucket and the production credentials, isolated by name: every
//! object a staging process touches lives under [`STAGING_KEY_PREFIX`]. The switch is the one
//! runtime environment variable every operational command already carries
//! ([`RUNTIME_ENVIRONMENT_ENV`] = `staging`), read once where the client is built, so no call site
//! can forget it.
//!
//! Keys stay logical in the process (`bronze/...`, exactly what production writes and what the
//! staging database records); the namespace maps them to physical keys at one place, the client's
//! request pipeline:
//!
//! - before the retry loop, an object request's path gains the prefix, a list request's `prefix`
//!   and `start-after` parameters gain it, and a copy's source header gains it;
//! - before every attempt is sent, the request actually on the wire is checked again: in staging a
//!   request that names a key outside the prefix is refused, and in production a request that names
//!   a key inside it is refused. The two namespaces are disjoint, and a new call site that bypassed
//!   the rewrite still cannot write outside its own.
//!
//! A logical key that already starts with the prefix is refused in both namespaces: it can only mean
//! a caller computed a physical key itself.

use aws_sdk_s3::config::{
    interceptors::{BeforeTransmitInterceptorContextMut, BeforeTransmitInterceptorContextRef},
    ConfigBag, Intercept, RuntimeComponents,
};
use aws_sdk_s3::error::BoxError;

use crate::errors::PublishError;

/// The runtime environment variable whose value `staging` selects the staging namespace.
pub const RUNTIME_ENVIRONMENT_ENV: &str = "FOUNDATION_PLATFORM_RUNTIME_ENV";
/// Every staging object's key starts with this; production never writes under it.
pub const STAGING_KEY_PREFIX: &str = "staging/";
/// Optional, staging only: the size from which a streaming put goes multipart, so the staging
/// smoke exercises the multipart path on a file of tens of megabytes instead of four gibibytes.
pub const STAGING_MULTIPART_THRESHOLD_ENV: &str =
    "FOUNDATION_PLATFORM_R2_STAGING_MULTIPART_THRESHOLD_BYTES";

const STAGING_ENVIRONMENT: &str = "staging";
/// R2 refuses a multipart part below 5 MiB (other than the last), so a lower threshold could not
/// take the path it is meant to exercise.
const MIN_STAGING_MULTIPART_THRESHOLD_BYTES: u64 = 5 * 1024 * 1024;
const COPY_SOURCE_HEADER: &str = "x-amz-copy-source";

/// Which part of the lakehouse bucket a client may name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum R2KeyNamespace {
    /// Every key except those under [`STAGING_KEY_PREFIX`].
    Production,
    /// Only keys under [`STAGING_KEY_PREFIX`].
    Staging {
        /// Streaming puts at or above this size go multipart.
        single_put_bytes_max: u64,
    },
}

impl R2KeyNamespace {
    /// Reads the namespace from [`RUNTIME_ENVIRONMENT_ENV`] and [`STAGING_MULTIPART_THRESHOLD_ENV`].
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when the threshold is set outside staging or is not a byte count in
    /// range.
    pub fn from_env() -> Result<Self, PublishError> {
        let environment = super::optional_env(RUNTIME_ENVIRONMENT_ENV)?;
        let threshold = super::optional_env(STAGING_MULTIPART_THRESHOLD_ENV)?;
        Self::from_settings(environment.as_deref(), threshold.as_deref())
    }

    /// The namespace for a runtime environment name and an optional staging multipart threshold.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when the threshold is set outside staging or is not a byte count in
    /// range.
    pub fn from_settings(
        environment: Option<&str>,
        multipart_threshold: Option<&str>,
    ) -> Result<Self, PublishError> {
        let staging = environment.map(str::trim) == Some(STAGING_ENVIRONMENT);
        let Some(raw) = multipart_threshold else {
            return Ok(if staging {
                Self::Staging {
                    single_put_bytes_max: super::R2_STREAMING_SINGLE_PUT_BYTES_MAX,
                }
            } else {
                Self::Production
            });
        };
        if !staging {
            return Err(PublishError::Infrastructure(format!(
                "{STAGING_MULTIPART_THRESHOLD_ENV} is honoured only when {RUNTIME_ENVIRONMENT_ENV}=staging"
            )));
        }
        let bytes = raw.trim().parse::<u64>().map_err(|error| {
            PublishError::Infrastructure(format!(
                "{STAGING_MULTIPART_THRESHOLD_ENV} must be a byte count: {error}"
            ))
        })?;
        if !(MIN_STAGING_MULTIPART_THRESHOLD_BYTES..=super::R2_STREAMING_SINGLE_PUT_BYTES_MAX)
            .contains(&bytes)
        {
            return Err(PublishError::Infrastructure(format!(
                "{STAGING_MULTIPART_THRESHOLD_ENV} must be between {MIN_STAGING_MULTIPART_THRESHOLD_BYTES} and {} bytes, got {bytes}",
                super::R2_STREAMING_SINGLE_PUT_BYTES_MAX
            )));
        }
        Ok(Self::Staging {
            single_put_bytes_max: bytes,
        })
    }

    /// Whether a streaming put of `size_bytes` must use the multipart path.
    #[must_use]
    pub const fn requires_multipart(self, size_bytes: u64) -> bool {
        match self {
            Self::Production => super::streaming_put_requires_multipart(size_bytes),
            Self::Staging {
                single_put_bytes_max,
            } => size_bytes >= single_put_bytes_max,
        }
    }

    /// The physical key a logical key names in this namespace.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` for an empty or absolute key, and for a key that already starts with
    /// [`STAGING_KEY_PREFIX`] (in staging it would be prefixed twice, in production it would write
    /// into staging).
    pub fn physical_key(self, key: &str) -> Result<String, PublishError> {
        if key.is_empty() || key.starts_with('/') {
            return Err(PublishError::Infrastructure(format!(
                "R2 key {key:?} must be a non-empty relative key"
            )));
        }
        if key.starts_with(STAGING_KEY_PREFIX) {
            return Err(PublishError::Infrastructure(format!(
                "R2 key {key:?} names the staging prefix itself; keys are logical and the namespace \
                 alone adds {STAGING_KEY_PREFIX} (root ADR-0177)"
            )));
        }
        Ok(match self {
            Self::Production => key.to_owned(),
            Self::Staging { .. } => format!("{STAGING_KEY_PREFIX}{key}"),
        })
    }

    /// The logical key of a physical key a listing returned, or `None` when it is not this
    /// namespace's.
    #[must_use]
    pub fn logical_key(self, physical: &str) -> Option<&str> {
        match self {
            Self::Production => (!physical.starts_with(STAGING_KEY_PREFIX)).then_some(physical),
            Self::Staging { .. } => physical.strip_prefix(STAGING_KEY_PREFIX),
        }
    }

    /// Rewrites a request the SDK serialized (a relative URI, before the endpoint names the bucket)
    /// and its copy-source header into this namespace.
    fn rewrite(
        self,
        uri: &str,
        copy_source: Option<&str>,
    ) -> Result<(String, Option<String>), String> {
        let (path, query) = uri
            .split_once('?')
            .map_or((uri, None), |(path, query)| (path, Some(query)));
        let key = path.strip_prefix('/').unwrap_or(path);
        let rewritten = match (key.is_empty(), query) {
            (true, None) => "/".to_owned(),
            (true, Some(query)) => format!("/?{}", self.rewrite_list_query(query)),
            (false, query) => {
                let physical = self.physical_key(key).map_err(|error| error.to_string())?;
                format!(
                    "/{physical}{}",
                    query.map_or_else(String::new, |q| format!("?{q}"))
                )
            }
        };
        let copy_source = copy_source
            .map(|value| {
                let (bucket, source) = value
                    .trim_start_matches('/')
                    .split_once('/')
                    .ok_or_else(|| format!("copy source {value:?} names no key"))?;
                let physical = self
                    .physical_key(source)
                    .map_err(|error| error.to_string())?;
                Ok::<_, String>(format!("{bucket}/{physical}"))
            })
            .transpose()?;
        Ok((rewritten, copy_source))
    }

    /// A listing's `prefix` and `start-after` name keys; in staging both gain the prefix, and a
    /// listing without a `prefix` gets one, so it cannot see outside the namespace.
    fn rewrite_list_query(self, query: &str) -> String {
        let Self::Staging { .. } = self else {
            return query.to_owned();
        };
        let encoded_prefix = STAGING_KEY_PREFIX.replace('/', "%2F");
        let mut has_prefix = false;
        let mut parts: Vec<String> = query
            .split('&')
            .filter(|part| !part.is_empty())
            .map(|part| match part.split_once('=') {
                Some((name @ ("prefix" | "start-after"), value)) => {
                    has_prefix |= name == "prefix";
                    format!("{name}={encoded_prefix}{value}")
                }
                _ => part.to_owned(),
            })
            .collect();
        if !has_prefix {
            parts.push(format!("prefix={encoded_prefix}"));
        }
        parts.join("&")
    }

    /// Whether the request about to be sent stays in this namespace. `uri` is absolute here: the
    /// endpoint has put the bucket first in the path (path-style addressing).
    fn admits_on_wire(
        self,
        bucket: &str,
        method: &str,
        uri: &str,
        copy_source: Option<&str>,
    ) -> Result<(), String> {
        let after_scheme = uri.split_once("://").map_or(uri, |(_, rest)| rest);
        let path_and_query = after_scheme
            .find('/')
            .map_or("/", |index| &after_scheme[index..]);
        let (path, query) = path_and_query
            .split_once('?')
            .map_or((path_and_query, ""), |(p, q)| (p, q));
        let bucket_root = format!("/{bucket}");
        let key = if path == bucket_root || path == format!("{bucket_root}/") {
            None
        } else {
            Some(
                path.strip_prefix(&format!("{bucket_root}/"))
                    .ok_or_else(|| {
                        format!("R2 request {method} {path} does not address bucket {bucket}")
                    })?,
            )
        };
        let staged = |key: &str| key.starts_with(STAGING_KEY_PREFIX);
        let source = copy_source.map(|value| {
            value
                .trim_start_matches('/')
                .split_once('/')
                .map_or("", |(_, key)| key)
                .to_owned()
        });
        match self {
            Self::Staging { .. } => {
                // A bucket-level request: only a read, and a listing only inside the prefix.
                let encoded_prefix = STAGING_KEY_PREFIX.replace('/', "%2F");
                let bucket_read_stays_inside = || {
                    matches!(method, "GET" | "HEAD")
                        && (!query.contains("list-type")
                            || query.split('&').any(|part| {
                                part.strip_prefix("prefix=")
                                    .is_some_and(|value| value.starts_with(&encoded_prefix))
                            }))
                };
                let ok = key.map_or_else(bucket_read_stays_inside, staged)
                    && source.as_deref().is_none_or(staged);
                if ok {
                    Ok(())
                } else {
                    Err(format!(
                        "staging refuses R2 request {method} {path}: outside {STAGING_KEY_PREFIX} (root ADR-0177)"
                    ))
                }
            }
            Self::Production => {
                if key.is_some_and(staged) || source.as_deref().is_some_and(staged) {
                    Err(format!(
                        "production refuses R2 request {method} {path}: inside {STAGING_KEY_PREFIX} (root ADR-0177)"
                    ))
                } else {
                    Ok(())
                }
            }
        }
    }
}

/// Puts every request of one client into its namespace and refuses one that leaves it.
#[derive(Debug)]
pub(super) struct R2NamespaceInterceptor {
    namespace: R2KeyNamespace,
    bucket: String,
}

impl R2NamespaceInterceptor {
    pub(super) const fn new(namespace: R2KeyNamespace, bucket: String) -> Self {
        Self { namespace, bucket }
    }
}

impl Intercept for R2NamespaceInterceptor {
    fn name(&self) -> &'static str {
        "R2NamespaceInterceptor"
    }

    // Once per operation, before the request is checkpointed for retries: every attempt is sent
    // from the rewritten request.
    fn modify_before_retry_loop(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let request = context.request_mut();
        let copy_source = request
            .headers()
            .get(COPY_SOURCE_HEADER)
            .map(ToOwned::to_owned);
        let (uri, copy_source) = self
            .namespace
            .rewrite(request.uri(), copy_source.as_deref())?;
        request.set_uri(uri.as_str())?;
        if let Some(copy_source) = copy_source {
            request
                .headers_mut()
                .insert(COPY_SOURCE_HEADER, copy_source);
        }
        Ok(())
    }

    // Every attempt, on the request as it will be sent.
    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let request = context.request();
        self.namespace
            .admits_on_wire(
                &self.bucket,
                request.method(),
                request.uri(),
                request.headers().get(COPY_SOURCE_HEADER),
            )
            .map_err(Into::into)
    }
}

#[cfg(test)]
#[path = "namespace_tests.rs"]
mod tests;
