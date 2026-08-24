//! Configuration and key guards for immutable PMTiles derivatives.
//!
//! This module deliberately does not reuse the generic `R2_*` environment. A tile derivative is
//! a separate storage security boundary: the publisher may create objects, while Martin receives
//! a different read-only credential. No delete or overwrite operation is exposed here.

use anyhow::{bail, ensure, Context};
use foundation_outbox::object_storage::R2ObjectStorageConfig;

use crate::r2_layout::vector_tile_release_key;

const ACCOUNT_ID: &str = "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ACCOUNT_ID";
const ENDPOINT: &str = "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ENDPOINT";
const BUCKET: &str = "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_BUCKET";
const REGION: &str = "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_REGION";
const WRITE_ACCESS_KEY: &str = "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_WRITER_ACCESS_KEY_ID";
const WRITE_SECRET_KEY: &str = "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_WRITER_SECRET_ACCESS_KEY";
const READ_ACCESS_KEY: &str = "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_ACCESS_KEY_ID";
const READ_SECRET_KEY: &str = "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_SECRET_ACCESS_KEY";
const PREFIX: &str = "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_PREFIX";
const DEFAULT_PREFIX: &str = "gold/vector-tiles/releases";

/// Dedicated R2 configuration for PMTiles derivative publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TileDerivativeR2Config {
    /// Generic SDK configuration used only for create-only writes.
    pub writer: R2ObjectStorageConfig,
    /// Read-only access key configured on the Martin deployment.
    pub martin_read_access_key_id: String,
    /// Read-only secret configured on the Martin deployment.
    pub martin_read_secret_access_key: String,
    /// Immutable object-key prefix.
    pub object_prefix: String,
}

impl TileDerivativeR2Config {
    /// Loads and validates the dedicated derivative environment.
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> anyhow::Result<Self> {
        let account_id = required(&mut lookup, ACCOUNT_ID)?;
        let endpoint = lookup(ENDPOINT)
            .unwrap_or_else(|| format!("https://{account_id}.r2.cloudflarestorage.com"));
        validate_endpoint(&account_id, &endpoint)?;
        let bucket = required(&mut lookup, BUCKET)?;
        validate_bucket(&bucket)?;
        let writer_access_key = required(&mut lookup, WRITE_ACCESS_KEY)?;
        let writer_secret_key = required(&mut lookup, WRITE_SECRET_KEY)?;
        let martin_read_access_key_id = required(&mut lookup, READ_ACCESS_KEY)?;
        let martin_read_secret_access_key = required(&mut lookup, READ_SECRET_KEY)?;
        ensure!(
            writer_access_key != martin_read_access_key_id,
            "writer and Martin read credentials must be distinct"
        );
        let object_prefix = lookup(PREFIX).unwrap_or_else(|| DEFAULT_PREFIX.to_owned());
        ensure!(
            object_prefix == DEFAULT_PREFIX,
            "{PREFIX} must be the canonical immutable tile derivative prefix"
        );

        Ok(Self {
            writer: R2ObjectStorageConfig {
                bucket_name: bucket,
                endpoint,
                region: lookup(REGION).unwrap_or_else(|| "auto".to_owned()),
                access_key_id: writer_access_key,
                secret_access_key: writer_secret_key,
            },
            martin_read_access_key_id,
            martin_read_secret_access_key,
            object_prefix,
        })
    }

    /// Derives the immutable object key and prevents callers from choosing an arbitrary path.
    pub fn release_key(&self, publication_unit: &str, release_id: &str) -> anyhow::Result<String> {
        vector_tile_release_key(publication_unit, release_id)
    }

    /// Builds the read-only configuration used by Martin and by the publisher's exact GET gate.
    #[must_use]
    pub fn reader_config(&self) -> R2ObjectStorageConfig {
        R2ObjectStorageConfig {
            bucket_name: self.writer.bucket_name.clone(),
            endpoint: self.writer.endpoint.clone(),
            region: self.writer.region.clone(),
            access_key_id: self.martin_read_access_key_id.clone(),
            secret_access_key: self.martin_read_secret_access_key.clone(),
        }
    }
}

/// Validates the dedicated R2 boundary without performing any network operation or write.
pub fn validate_environment() -> anyhow::Result<()> {
    let config = TileDerivativeR2Config::from_env()?;
    let key_shape = config.release_key("parcels", "00000000-0000-7000-8000-000000000001")?;
    println!(
        "tile derivative R2 configuration OK bucket={} prefix={} key_shape={}",
        config.writer.bucket_name, config.object_prefix, key_shape
    );
    Ok(())
}

fn required(lookup: &mut impl FnMut(&str) -> Option<String>, name: &str) -> anyhow::Result<String> {
    lookup(name)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .with_context(|| format!("missing required tile derivative environment variable {name}"))
}

fn validate_endpoint(account_id: &str, endpoint: &str) -> anyhow::Result<()> {
    let account_id = account_id.trim().to_ascii_lowercase();
    let endpoint = endpoint.trim().trim_end_matches('/');
    ensure!(
        account_id.len() == 32 && account_id.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "{ACCOUNT_ID} must be a 32-character hexadecimal account id"
    );
    ensure!(
        endpoint == format!("https://{account_id}.r2.cloudflarestorage.com"),
        "{ENDPOINT} must be the standard Cloudflare R2 S3 endpoint for {ACCOUNT_ID}"
    );
    Ok(())
}

fn validate_bucket(bucket: &str) -> anyhow::Result<()> {
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
    ensure!(
        bucket.contains("tile") || bucket.contains("derivative"),
        "{BUCKET} must be a dedicated tile-derivative bucket"
    );
    for protected in [
        "foundation-platform-lakehouse-prod",
        "gongzzang-lakehouse-prod",
        "foundation-platform-postgres-recovery-prod",
        "agency-public-assets",
        "dawneer-lakehouse-prod",
    ] {
        if bucket == protected {
            bail!("{BUCKET} may not target protected bucket {protected}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{TileDerivativeR2Config, DEFAULT_PREFIX};
    use std::collections::BTreeMap;

    fn valid() -> BTreeMap<&'static str, String> {
        BTreeMap::from([
            (
                "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ACCOUNT_ID",
                "a".repeat(32),
            ),
            (
                "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_BUCKET",
                "perfectory-tiles-prod".to_owned(),
            ),
            (
                "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_WRITER_ACCESS_KEY_ID",
                "writer".to_owned(),
            ),
            (
                "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_WRITER_SECRET_ACCESS_KEY",
                "writer-secret".to_owned(),
            ),
            (
                "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_ACCESS_KEY_ID",
                "reader".to_owned(),
            ),
            (
                "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_SECRET_ACCESS_KEY",
                "reader-secret".to_owned(),
            ),
        ])
    }

    #[test]
    fn requires_separate_writer_and_martin_reader_credentials() {
        let values = valid();
        let config = TileDerivativeR2Config::from_lookup(|name| values.get(name).cloned())
            .expect("valid derivative config");
        assert_eq!(config.object_prefix, DEFAULT_PREFIX);
        assert_ne!(
            config.writer.access_key_id,
            config.martin_read_access_key_id
        );
        let reader = config.reader_config();
        assert_eq!(reader.access_key_id, config.martin_read_access_key_id);
        assert_eq!(reader.bucket_name, config.writer.bucket_name);
        assert_eq!(
            config
                .release_key("parcels", "018f0000-0000-7000-8000-000000000001")
                .unwrap(),
            "gold/vector-tiles/releases/parcels-018f0000-0000-7000-8000-000000000001.pmtiles"
        );
    }

    #[test]
    fn rejects_protected_bucket_and_credential_reuse() {
        let mut values = valid();
        values.insert(
            "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_BUCKET",
            "foundation-platform-lakehouse-prod".to_owned(),
        );
        assert!(TileDerivativeR2Config::from_lookup(|name| values.get(name).cloned()).is_err());

        let mut values = valid();
        values.insert(
            "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_ACCESS_KEY_ID",
            "writer".to_owned(),
        );
        assert!(TileDerivativeR2Config::from_lookup(|name| values.get(name).cloned()).is_err());
    }
}
