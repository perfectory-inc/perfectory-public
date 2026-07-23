//! Industrial-complex Gold pointer publish command.

use std::{env, sync::Arc};

use anyhow::{bail, Context};
use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::ComplexId;
use lakehouse_application::{
    PublishIndustrialComplexGoldPointer, PublishIndustrialComplexGoldPointerInput,
};
use lakehouse_infrastructure::PgLakehousePublicationUnitOfWork;
use sqlx::PgPool;
use uuid::Uuid;

const DATABASE_URL_ENV: &str = "DATABASE_URL";
const COMPLEX_ID_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_COMPLEX_ID";
const CURRENT_VERSION_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_CURRENT_VERSION";
const EXPECTED_CURRENT_VERSION_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_EXPECTED_CURRENT_VERSION";
const PROFILE_OBJECT_KEY_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_PROFILE_OBJECT_KEY";
const SPATIAL_LOCATOR_OBJECT_KEY_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_SPATIAL_LOCATOR_OBJECT_KEY";
const SOURCE_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_SOURCE";
const SOURCE_URL_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_SOURCE_URL";
const SOURCE_EXTERNAL_ID_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_SOURCE_EXTERNAL_ID";
const SOURCE_SNAPSHOT_ID_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_SOURCE_SNAPSHOT_ID";
const ICEBERG_SNAPSHOT_ID_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_ICEBERG_SNAPSHOT_ID";
const PROFILE_ROW_COUNT_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_PROFILE_ROW_COUNT";
const PROFILE_SIZE_BYTES_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_PROFILE_SIZE_BYTES";
const SPATIAL_LOCATOR_SIZE_BYTES_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_SPATIAL_LOCATOR_SIZE_BYTES";
const PROFILE_CHECKSUM_SHA256_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_PROFILE_CHECKSUM_SHA256";
const PUBLISHED_AT_UTC_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_PUBLISHED_AT_UTC";

/// Publishes the current Gold pointer for one industrial complex.
pub async fn run() -> anyhow::Result<()> {
    let config = PublishIndustrialComplexGoldPointerConfig::from_env(Utc::now())?;
    let pool = PgPool::connect(config.database_url.as_str())
        .await
        .context("failed to connect to database for industrial-complex Gold pointer publish")?;
    let use_case = PublishIndustrialComplexGoldPointer::new(Arc::new(
        PgLakehousePublicationUnitOfWork::new(pool),
    ));
    let pointer = use_case
        .execute(config.input)
        .await
        .context("failed to publish industrial-complex Gold pointer")?;

    tracing::info!(
        complex_id = %pointer.complex_id,
        current_version = %pointer.current_version,
        profile_object_key = %pointer.profile_object_key.as_str(),
        source_snapshot_id = %pointer.source_snapshot_id,
        iceberg_snapshot_id = %pointer.iceberg_snapshot_id,
        "industrial-complex Gold pointer publish succeeded"
    );

    Ok(())
}

#[derive(Debug)]
struct PublishIndustrialComplexGoldPointerConfig {
    database_url: String,
    input: PublishIndustrialComplexGoldPointerInput,
}

impl PublishIndustrialComplexGoldPointerConfig {
    fn from_env(now: DateTime<Utc>) -> anyhow::Result<Self> {
        Self::from_lookup(now, |name| match env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(env::VarError::NotPresent) => Ok(None),
            Err(error) => bail!("invalid {name} environment variable: {error}"),
        })
    }

    fn from_lookup<F>(now: DateTime<Utc>, mut lookup: F) -> anyhow::Result<Self>
    where
        F: FnMut(&str) -> anyhow::Result<Option<String>>,
    {
        let database_url = required_lookup_value(&mut lookup, DATABASE_URL_ENV)?;
        let complex_id = parse_uuid_env(
            COMPLEX_ID_ENV,
            required_lookup_value(&mut lookup, COMPLEX_ID_ENV)?.as_str(),
        )?;
        let published_at = optional_lookup_value(&mut lookup, PUBLISHED_AT_UTC_ENV)?
            .map(|raw| parse_utc_env(PUBLISHED_AT_UTC_ENV, raw.as_str()))
            .transpose()?
            .unwrap_or(now);

        Ok(Self {
            database_url,
            input: PublishIndustrialComplexGoldPointerInput {
                complex_id: ComplexId::new(complex_id),
                current_version: required_lookup_value(&mut lookup, CURRENT_VERSION_ENV)?,
                expected_current_version: optional_lookup_value(
                    &mut lookup,
                    EXPECTED_CURRENT_VERSION_ENV,
                )?,
                profile_object_key: required_lookup_value(&mut lookup, PROFILE_OBJECT_KEY_ENV)?,
                spatial_locator_object_key: optional_lookup_value(
                    &mut lookup,
                    SPATIAL_LOCATOR_OBJECT_KEY_ENV,
                )?,
                source: required_lookup_value(&mut lookup, SOURCE_ENV)?,
                source_url: optional_lookup_value(&mut lookup, SOURCE_URL_ENV)?,
                source_external_id: optional_lookup_value(&mut lookup, SOURCE_EXTERNAL_ID_ENV)?,
                source_snapshot_id: required_lookup_value(&mut lookup, SOURCE_SNAPSHOT_ID_ENV)?,
                iceberg_snapshot_id: required_lookup_value(&mut lookup, ICEBERG_SNAPSHOT_ID_ENV)?,
                profile_row_count: parse_required_u64_env(&mut lookup, PROFILE_ROW_COUNT_ENV)?,
                profile_size_bytes: parse_required_u64_env(&mut lookup, PROFILE_SIZE_BYTES_ENV)?,
                spatial_locator_size_bytes: parse_optional_u64_env(
                    &mut lookup,
                    SPATIAL_LOCATOR_SIZE_BYTES_ENV,
                )?,
                profile_checksum_sha256: required_lookup_value(
                    &mut lookup,
                    PROFILE_CHECKSUM_SHA256_ENV,
                )?,
                published_at,
            },
        })
    }
}

fn required_lookup_value<F>(lookup: &mut F, name: &str) -> anyhow::Result<String>
where
    F: FnMut(&str) -> anyhow::Result<Option<String>>,
{
    optional_lookup_value(lookup, name)?.map_or_else(|| bail!("{name} is required"), Ok)
}

fn optional_lookup_value<F>(lookup: &mut F, name: &str) -> anyhow::Result<Option<String>>
where
    F: FnMut(&str) -> anyhow::Result<Option<String>>,
{
    lookup(name).map(|value| {
        value.and_then(|raw| {
            let trimmed = raw.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_owned())
        })
    })
}

fn parse_required_u64_env<F>(lookup: &mut F, name: &str) -> anyhow::Result<u64>
where
    F: FnMut(&str) -> anyhow::Result<Option<String>>,
{
    parse_u64_env(name, required_lookup_value(lookup, name)?.as_str())
}

fn parse_optional_u64_env<F>(lookup: &mut F, name: &str) -> anyhow::Result<Option<u64>>
where
    F: FnMut(&str) -> anyhow::Result<Option<String>>,
{
    optional_lookup_value(lookup, name)?
        .map(|raw| parse_u64_env(name, raw.as_str()))
        .transpose()
}

fn parse_u64_env(name: &str, raw: &str) -> anyhow::Result<u64> {
    raw.parse::<u64>()
        .with_context(|| format!("{name} must be an unsigned integer"))
}

fn parse_uuid_env(name: &str, raw: &str) -> anyhow::Result<Uuid> {
    Uuid::parse_str(raw).with_context(|| format!("{name} must be a UUID"))
}

fn parse_utc_env(name: &str, raw: &str) -> anyhow::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| format!("{name} must be an RFC3339 UTC timestamp"))
}

#[cfg(test)]
mod tests {
    use super::{
        PublishIndustrialComplexGoldPointerConfig, COMPLEX_ID_ENV, CURRENT_VERSION_ENV,
        DATABASE_URL_ENV, EXPECTED_CURRENT_VERSION_ENV, ICEBERG_SNAPSHOT_ID_ENV,
        PROFILE_CHECKSUM_SHA256_ENV, PROFILE_OBJECT_KEY_ENV, PROFILE_ROW_COUNT_ENV,
        PROFILE_SIZE_BYTES_ENV, PUBLISHED_AT_UTC_ENV, SOURCE_ENV, SOURCE_EXTERNAL_ID_ENV,
        SOURCE_SNAPSHOT_ID_ENV, SOURCE_URL_ENV, SPATIAL_LOCATOR_OBJECT_KEY_ENV,
        SPATIAL_LOCATOR_SIZE_BYTES_ENV,
    };
    use chrono::{DateTime, SecondsFormat, Utc};
    use std::collections::BTreeMap;

    #[test]
    fn parses_gold_pointer_publish_config() -> anyhow::Result<()> {
        let now = DateTime::parse_from_rfc3339("2026-05-18T00:00:00Z")?.with_timezone(&Utc);
        let complex_id = "00000000-0000-7000-8000-000000000001";
        let values = BTreeMap::from([
            (DATABASE_URL_ENV, "postgres://example"),
            (COMPLEX_ID_ENV, complex_id),
            (CURRENT_VERSION_ENV, "0196e7e0-3c20-7000-8000-100000000002"),
            (EXPECTED_CURRENT_VERSION_ENV, "0196e7e0-3c20-7000-8000-100000000001"),
            (
                PROFILE_OBJECT_KEY_ENV,
                "gold/industrial-complex/profiles/0196e7e0-3c20-7000-8000-100000000002.json",
            ),
            (
                SPATIAL_LOCATOR_OBJECT_KEY_ENV,
                "gold/industrial-complex/spatial-locators/0196e7e0-3c20-7000-8000-100000000002.parquet",
            ),
            (
                SOURCE_ENV,
                "foundation-platform.spark.industrial_complex_gold",
            ),
            (SOURCE_URL_ENV, "s3://warehouse/gold"),
            (SOURCE_EXTERNAL_ID_ENV, "spark-run-20260518"),
            (SOURCE_SNAPSHOT_ID_ENV, "bronze-snapshot-1"),
            (ICEBERG_SNAPSHOT_ID_ENV, "iceberg-snapshot-1"),
            (PROFILE_ROW_COUNT_ENV, "10"),
            (PROFILE_SIZE_BYTES_ENV, "2048"),
            (SPATIAL_LOCATOR_SIZE_BYTES_ENV, "4096"),
            (
                PROFILE_CHECKSUM_SHA256_ENV,
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            (PUBLISHED_AT_UTC_ENV, "2026-05-18T01:02:03+09:00"),
        ]);
        let config = PublishIndustrialComplexGoldPointerConfig::from_lookup(now, |name| {
            Ok(values.get(name).map(ToString::to_string))
        })?;

        assert_eq!(config.database_url, "postgres://example");
        assert_eq!(config.input.complex_id.as_uuid().to_string(), complex_id);
        assert_eq!(
            config.input.current_version,
            "0196e7e0-3c20-7000-8000-100000000002"
        );
        assert_eq!(
            config.input.spatial_locator_object_key.as_deref(),
            Some("gold/industrial-complex/spatial-locators/0196e7e0-3c20-7000-8000-100000000002.parquet")
        );
        assert_eq!(config.input.profile_row_count, 10);
        assert_eq!(config.input.profile_size_bytes, 2048);
        assert_eq!(config.input.spatial_locator_size_bytes, Some(4096));
        assert_eq!(
            config
                .input
                .published_at
                .to_rfc3339_opts(SecondsFormat::Secs, true),
            "2026-05-17T16:02:03Z"
        );
        Ok(())
    }

    #[test]
    fn defaults_published_at_to_now() -> anyhow::Result<()> {
        let now = DateTime::parse_from_rfc3339("2026-05-18T00:00:00Z")?.with_timezone(&Utc);
        let values = BTreeMap::from([
            (DATABASE_URL_ENV, "postgres://example"),
            (COMPLEX_ID_ENV, "00000000-0000-7000-8000-000000000001"),
            (CURRENT_VERSION_ENV, "0196e7e0-3c20-7000-8000-100000000001"),
            (
                PROFILE_OBJECT_KEY_ENV,
                "gold/industrial-complex/profiles/0196e7e0-3c20-7000-8000-100000000001.json",
            ),
            (
                SOURCE_ENV,
                "foundation-platform.spark.industrial_complex_gold",
            ),
            (SOURCE_SNAPSHOT_ID_ENV, "bronze-snapshot-1"),
            (ICEBERG_SNAPSHOT_ID_ENV, "iceberg-snapshot-1"),
            (PROFILE_ROW_COUNT_ENV, "10"),
            (PROFILE_SIZE_BYTES_ENV, "2048"),
            (
                PROFILE_CHECKSUM_SHA256_ENV,
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
        ]);
        let config = PublishIndustrialComplexGoldPointerConfig::from_lookup(now, |name| {
            Ok(values.get(name).map(ToString::to_string))
        })?;

        assert_eq!(config.input.published_at, now);
        assert_eq!(config.input.expected_current_version, None);
        assert_eq!(config.input.spatial_locator_object_key, None);
        assert_eq!(config.input.spatial_locator_size_bytes, None);
        Ok(())
    }

    #[test]
    fn rejects_invalid_numeric_values() -> anyhow::Result<()> {
        let now = DateTime::parse_from_rfc3339("2026-05-18T00:00:00Z")?.with_timezone(&Utc);
        let values = BTreeMap::from([
            (DATABASE_URL_ENV, "postgres://example"),
            (COMPLEX_ID_ENV, "00000000-0000-7000-8000-000000000001"),
            (CURRENT_VERSION_ENV, "0196e7e0-3c20-7000-8000-100000000001"),
            (
                PROFILE_OBJECT_KEY_ENV,
                "gold/industrial-complex/profiles/0196e7e0-3c20-7000-8000-100000000001.json",
            ),
            (
                SOURCE_ENV,
                "foundation-platform.spark.industrial_complex_gold",
            ),
            (SOURCE_SNAPSHOT_ID_ENV, "bronze-snapshot-1"),
            (ICEBERG_SNAPSHOT_ID_ENV, "iceberg-snapshot-1"),
            (PROFILE_ROW_COUNT_ENV, "ten"),
            (PROFILE_SIZE_BYTES_ENV, "2048"),
            (
                PROFILE_CHECKSUM_SHA256_ENV,
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
        ]);

        let result = PublishIndustrialComplexGoldPointerConfig::from_lookup(now, |name| {
            Ok(values.get(name).map(ToString::to_string))
        });
        let Err(error) = result else {
            anyhow::bail!("numeric parse should fail");
        };

        assert!(error
            .to_string()
            .contains("PROFILE_ROW_COUNT must be an unsigned integer"));
        Ok(())
    }
}
