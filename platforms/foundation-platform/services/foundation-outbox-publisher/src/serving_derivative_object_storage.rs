//! The one place that says what may live in the serving-derivative bucket.
//!
//! Canonical bytes (Bronze collection sources, the Silver/Gold Iceberg tables) live in the
//! lakehouse bucket. Serving derivatives — the immutable artifacts a client is meant to fetch —
//! live in a separate bucket so the two can be given different credentials, different retention,
//! and, if the owner ever attaches one, different public exposure. Putting a fetchable artifact in
//! the lakehouse bucket would mean any public domain on that bucket also exposes 257 GB of
//! collection sources.
//!
//! `TileDerivativeR2Config` was the first user of that boundary and states the tile-specific part
//! of it. This module holds the part every serving derivative shares, so the protected-bucket list
//! exists once rather than once per artifact kind.

use anyhow::{bail, ensure, Context};
use foundation_outbox::object_storage::R2ObjectStorageConfig;

use crate::r2_layout::INDUSTRIAL_COMPLEX_GOLD_PROFILE_ROOT;

const ACCOUNT_ID: &str = "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ACCOUNT_ID";
const ENDPOINT: &str = "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ENDPOINT";
const BUCKET: &str = "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_BUCKET";
const REGION: &str = "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_REGION";
const WRITE_ACCESS_KEY: &str = "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_WRITER_ACCESS_KEY_ID";
const WRITE_SECRET_KEY: &str = "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_WRITER_SECRET_ACCESS_KEY";

/// Buckets a serving derivative may never be written to, whatever the environment says.
///
/// The lakehouse entry is the load-bearing one: it holds the canonical bytes, so a fetchable
/// artifact must never share a bucket with it.
const PROTECTED_BUCKETS: &[&str] = &[
    "foundation-platform-lakehouse-prod",
    "gongzzang-lakehouse-prod",
    "dawneer-lakehouse-prod",
    "foundation-platform-postgres-recovery-prod",
    "agency-public-assets",
];

/// Object-key roots that may exist in the serving-derivative bucket.
///
/// Each entry is one artifact kind whose bytes a client is meant to fetch. Both roots are read
/// from the module that owns the key layout, so this list names artifact kinds rather than
/// restating paths.
const SERVING_DERIVATIVE_OBJECT_ROOTS: &[&str] = &[
    catalog_domain::STATIC_RELEASE_OBJECT_ROOT,
    INDUSTRIAL_COMPLEX_GOLD_PROFILE_ROOT,
];

/// Writer configuration for the serving-derivative bucket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServingDerivativeR2Config {
    /// SDK configuration used only for create-only writes.
    pub writer: R2ObjectStorageConfig,
}

impl ServingDerivativeR2Config {
    /// Loads and validates the serving-derivative environment.
    ///
    /// # Errors
    /// Returns an error when a required variable is missing or the configured bucket is one the
    /// serving derivatives may not be written to.
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> anyhow::Result<Self> {
        let account_id = required(&mut lookup, ACCOUNT_ID)?;
        let endpoint = lookup(ENDPOINT)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("https://{account_id}.r2.cloudflarestorage.com"));
        let bucket = required(&mut lookup, BUCKET)?;
        validate_serving_derivative_bucket(bucket.as_str())?;

        Ok(Self {
            writer: R2ObjectStorageConfig {
                bucket_name: bucket,
                endpoint: endpoint.trim().trim_end_matches('/').to_owned(),
                region: lookup(REGION)
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| "auto".to_owned()),
                access_key_id: required(&mut lookup, WRITE_ACCESS_KEY)?,
                secret_access_key: required(&mut lookup, WRITE_SECRET_KEY)?,
            },
        })
    }
}

/// Rejects buckets that must never hold a fetchable serving derivative.
///
/// # Errors
/// Returns an error when the name is not a legal bucket name or names a protected bucket.
pub fn validate_serving_derivative_bucket(bucket: &str) -> anyhow::Result<()> {
    ensure!(
        bucket.len() >= 3
            && bucket.len() <= 63
            && bucket == bucket.to_ascii_lowercase()
            && bucket
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            && !bucket.starts_with('-')
            && !bucket.ends_with('-')
            && !bucket.contains("--"),
        "{BUCKET} is not a valid R2 bucket name"
    );
    for protected in PROTECTED_BUCKETS {
        if bucket == *protected {
            bail!("a serving derivative may not be written to protected bucket {protected}");
        }
    }
    Ok(())
}

/// Rejects object keys outside the artifact kinds this bucket is declared to hold.
///
/// # Errors
/// Returns an error when the key does not sit under one of the declared roots.
pub fn assert_serving_derivative_key(key: &str) -> anyhow::Result<()> {
    ensure!(
        SERVING_DERIVATIVE_OBJECT_ROOTS
            .iter()
            .any(|root| key.starts_with(&format!("{root}/"))),
        "object key {key} is not a declared serving derivative"
    );
    Ok(())
}

fn required(lookup: &mut impl FnMut(&str) -> Option<String>, name: &str) -> anyhow::Result<String> {
    lookup(name)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .with_context(|| format!("missing required serving derivative environment variable {name}"))
}

#[cfg(test)]
mod tests {
    use super::{
        assert_serving_derivative_key, validate_serving_derivative_bucket,
        ServingDerivativeR2Config,
    };
    use std::collections::BTreeMap;

    fn valid() -> BTreeMap<&'static str, String> {
        BTreeMap::from([
            (
                "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ACCOUNT_ID",
                "a".repeat(32),
            ),
            (
                "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_BUCKET",
                "foundation-platform-tile-derivatives-prod".to_owned(),
            ),
            (
                "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_WRITER_ACCESS_KEY_ID",
                "writer".to_owned(),
            ),
            (
                "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_WRITER_SECRET_ACCESS_KEY",
                "writer-secret".to_owned(),
            ),
        ])
    }

    #[test]
    fn the_canonical_bytes_bucket_can_never_hold_a_serving_derivative() {
        let mut values = valid();
        values.insert(
            "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_BUCKET",
            "foundation-platform-lakehouse-prod".to_owned(),
        );

        assert!(ServingDerivativeR2Config::from_lookup(|name| values.get(name).cloned()).is_err());
        assert!(validate_serving_derivative_bucket("foundation-platform-lakehouse-prod").is_err());
        assert!(
            validate_serving_derivative_bucket("foundation-platform-postgres-recovery-prod")
                .is_err()
        );
        assert!(
            validate_serving_derivative_bucket("foundation-platform-tile-derivatives-prod").is_ok()
        );
    }

    #[test]
    fn only_the_declared_artifact_kinds_may_be_written_there() -> anyhow::Result<()> {
        assert_serving_derivative_key(
            "gold/industrial-complex/profiles/018f0000-0000-7000-8000-000000000001.json",
        )?;
        assert_serving_derivative_key(
            "gold/vector-tiles/releases/parcels-018f0000-0000-7000-8000-000000000001.pmtiles",
        )?;
        for key in [
            "bronze/vworld/2026/raw.jsonl",
            "gold/manifest.json",
            "gold/industrial-complex/profiles",
            "silver/industrial-complexes/part-0.parquet",
        ] {
            assert!(
                assert_serving_derivative_key(key).is_err(),
                "key outside the declared serving derivatives was accepted: {key}"
            );
        }
        Ok(())
    }

    #[test]
    fn the_writer_configuration_is_read_from_the_serving_derivative_connection(
    ) -> anyhow::Result<()> {
        let values = valid();
        let config = ServingDerivativeR2Config::from_lookup(|name| values.get(name).cloned())?;

        assert_eq!(
            config.writer.bucket_name,
            "foundation-platform-tile-derivatives-prod"
        );
        assert_eq!(
            config.writer.endpoint,
            format!("https://{}.r2.cloudflarestorage.com", "a".repeat(32))
        );
        assert_eq!(config.writer.region, "auto");
        Ok(())
    }
}
