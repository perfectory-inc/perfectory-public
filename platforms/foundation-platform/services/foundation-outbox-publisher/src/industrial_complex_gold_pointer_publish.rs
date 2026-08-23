//! Industrial-complex Gold pointer publish command, for one complex.
//!
//! Every artifact-describing value this command is given is checked against the stored object
//! before the pointer is written (`artifact_verification`). The command therefore needs to be told
//! where the object lives, which is why the profile store is a required input rather than a
//! defaulted one: a pointer publish is a production act and there is no store worth guessing.
//!
//! For a whole export — 1,442 complexes — use `publish-industrial-complex-gold-pointers`, which
//! reads the same values out of the export summary instead of out of an operator's hands.

mod artifact_verification;
mod from_export_summary;

use std::{env, sync::Arc};

use anyhow::{bail, Context};
use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::ComplexId;
use lakehouse_application::PublishIndustrialComplexGoldPointer;
use lakehouse_infrastructure::PgLakehousePublicationUnitOfWork;
use sqlx::PgPool;
use uuid::Uuid;

use crate::industrial_complex_gold_profile_store::{
    local_root, ProfileObjectStore, ProfileStoreConfig,
};
use artifact_verification::{
    ClaimedGoldProfileArtifact, PointerPublication, VerifiedGoldProfileArtifact,
};

pub(crate) use from_export_summary::run as run_from_export_summary;

const DATABASE_URL_ENV: &str = "DATABASE_URL";
const COMPLEX_ID_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_COMPLEX_ID";
const CURRENT_VERSION_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_CURRENT_VERSION";
const EXPECTED_CURRENT_VERSION_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_EXPECTED_CURRENT_VERSION";
const PROFILE_OBJECT_KEY_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_PROFILE_OBJECT_KEY";
const PROFILE_URL_TEMPLATE_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_PROFILE_URL_TEMPLATE";
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
const PROFILE_STORAGE_DRIVER_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_PROFILE_STORAGE_DRIVER";
const PROFILE_LOCAL_ROOT_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_PROFILE_LOCAL_ROOT";

/// Publishes the current Gold pointer for one industrial complex.
pub async fn run() -> anyhow::Result<()> {
    let config = PublishIndustrialComplexGoldPointerConfig::from_env(Utc::now())?;
    let store = ProfileObjectStore::open(&config.store)?;
    let verified = VerifiedGoldProfileArtifact::verify(&store, config.claim).await?;

    tracing::info!(
        complex_id = %verified.complex_id().as_uuid(),
        profile_object_key = verified.object_key(),
        profile_storage_driver = store.storage_driver(),
        profile_bucket = store.bucket().unwrap_or("(local)"),
        "industrial-complex Gold profile object matched the pointer's claims"
    );

    let pool = PgPool::connect(config.database_url.as_str())
        .await
        .context("failed to connect to database for industrial-complex Gold pointer publish")?;
    let use_case = PublishIndustrialComplexGoldPointer::new(Arc::new(
        PgLakehousePublicationUnitOfWork::new(pool),
    ));
    let pointer = use_case
        .execute(verified.into_publish_input(config.publication))
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
    store: ProfileStoreConfig,
    claim: ClaimedGoldProfileArtifact,
    publication: PointerPublication,
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
        let store = ProfileStoreConfig::parse(
            required_lookup_value(&mut lookup, PROFILE_STORAGE_DRIVER_ENV)?.as_str(),
            local_root(optional_lookup_value(&mut lookup, PROFILE_LOCAL_ROOT_ENV)?),
        )
        .with_context(|| format!("{PROFILE_STORAGE_DRIVER_ENV}/{PROFILE_LOCAL_ROOT_ENV}"))?;

        Ok(Self {
            database_url,
            store,
            claim: ClaimedGoldProfileArtifact {
                complex_id: ComplexId::new(complex_id),
                current_version: required_lookup_value(&mut lookup, CURRENT_VERSION_ENV)?,
                object_key: required_lookup_value(&mut lookup, PROFILE_OBJECT_KEY_ENV)?,
                checksum_sha256: required_lookup_value(&mut lookup, PROFILE_CHECKSUM_SHA256_ENV)?,
                size_bytes: parse_required_u64_env(&mut lookup, PROFILE_SIZE_BYTES_ENV)?,
            },
            publication: PointerPublication {
                expected_current_version: optional_lookup_value(
                    &mut lookup,
                    EXPECTED_CURRENT_VERSION_ENV,
                )?,
                profile_url_template: required_lookup_value(&mut lookup, PROFILE_URL_TEMPLATE_ENV)?,
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
                spatial_locator_size_bytes: parse_optional_u64_env(
                    &mut lookup,
                    SPATIAL_LOCATOR_SIZE_BYTES_ENV,
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
        PROFILE_CHECKSUM_SHA256_ENV, PROFILE_LOCAL_ROOT_ENV, PROFILE_OBJECT_KEY_ENV,
        PROFILE_ROW_COUNT_ENV, PROFILE_SIZE_BYTES_ENV, PROFILE_STORAGE_DRIVER_ENV,
        PROFILE_URL_TEMPLATE_ENV, PUBLISHED_AT_UTC_ENV, SOURCE_ENV, SOURCE_EXTERNAL_ID_ENV,
        SOURCE_SNAPSHOT_ID_ENV, SOURCE_URL_ENV, SPATIAL_LOCATOR_OBJECT_KEY_ENV,
        SPATIAL_LOCATOR_SIZE_BYTES_ENV,
    };
    use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
    use chrono::{DateTime, SecondsFormat, Utc};
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn minimal() -> BTreeMap<&'static str, &'static str> {
        BTreeMap::from([
            (DATABASE_URL_ENV, "postgres://example"),
            (COMPLEX_ID_ENV, "00000000-0000-7000-8000-000000000001"),
            (CURRENT_VERSION_ENV, "0196e7e0-3c20-7000-8000-100000000001"),
            (
                PROFILE_OBJECT_KEY_ENV,
                "gold/industrial-complex/profiles/0196e7e0-3c20-7000-8000-100000000001.json",
            ),
            (
                PROFILE_URL_TEMPLATE_ENV,
                "https://lakehouse.example.com/{object_key}",
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
            (PROFILE_STORAGE_DRIVER_ENV, "local"),
            (PROFILE_LOCAL_ROOT_ENV, "target/lakehouse/gold-profiles"),
        ])
    }

    fn parse(
        values: &BTreeMap<&'static str, &'static str>,
        now: DateTime<Utc>,
    ) -> anyhow::Result<PublishIndustrialComplexGoldPointerConfig> {
        PublishIndustrialComplexGoldPointerConfig::from_lookup(now, |name| {
            Ok(values.get(name).map(ToString::to_string))
        })
    }

    fn now() -> anyhow::Result<DateTime<Utc>> {
        Ok(DateTime::parse_from_rfc3339("2026-05-18T00:00:00Z")?.with_timezone(&Utc))
    }

    #[test]
    fn parses_gold_pointer_publish_config() -> anyhow::Result<()> {
        let complex_id = "00000000-0000-7000-8000-000000000001";
        let mut values = minimal();
        values.insert(CURRENT_VERSION_ENV, "0196e7e0-3c20-7000-8000-100000000002");
        values.insert(
            PROFILE_OBJECT_KEY_ENV,
            "gold/industrial-complex/profiles/0196e7e0-3c20-7000-8000-100000000002.json",
        );
        values.insert(
            EXPECTED_CURRENT_VERSION_ENV,
            "0196e7e0-3c20-7000-8000-100000000001",
        );
        values.insert(
            SPATIAL_LOCATOR_OBJECT_KEY_ENV,
            "gold/industrial-complex/spatial-locators/0196e7e0-3c20-7000-8000-100000000002.parquet",
        );
        values.insert(SOURCE_URL_ENV, "s3://warehouse/gold");
        values.insert(SOURCE_EXTERNAL_ID_ENV, "spark-run-20260518");
        values.insert(SPATIAL_LOCATOR_SIZE_BYTES_ENV, "4096");
        values.insert(PUBLISHED_AT_UTC_ENV, "2026-05-18T01:02:03+09:00");

        let config = parse(&values, now()?)?;

        assert_eq!(config.database_url, "postgres://example");
        assert_eq!(config.claim.complex_id.as_uuid().to_string(), complex_id);
        assert_eq!(
            config.claim.current_version,
            "0196e7e0-3c20-7000-8000-100000000002"
        );
        assert_eq!(config.claim.size_bytes, 2048);
        assert_eq!(
            config.publication.spatial_locator_object_key.as_deref(),
            Some("gold/industrial-complex/spatial-locators/0196e7e0-3c20-7000-8000-100000000002.parquet")
        );
        assert_eq!(config.publication.profile_row_count, 10);
        assert_eq!(config.publication.spatial_locator_size_bytes, Some(4096));
        assert_eq!(
            config
                .publication
                .published_at
                .to_rfc3339_opts(SecondsFormat::Secs, true),
            "2026-05-17T16:02:03Z"
        );
        Ok(())
    }

    #[test]
    fn defaults_published_at_to_now() -> anyhow::Result<()> {
        let config = parse(&minimal(), now()?)?;

        assert_eq!(config.publication.published_at, now()?);
        assert_eq!(config.publication.expected_current_version, None);
        assert_eq!(config.publication.spatial_locator_object_key, None);
        assert_eq!(config.publication.spatial_locator_size_bytes, None);
        Ok(())
    }

    #[test]
    fn rejects_invalid_numeric_values() -> anyhow::Result<()> {
        let mut values = minimal();
        values.insert(PROFILE_ROW_COUNT_ENV, "ten");

        let Err(error) = parse(&values, now()?) else {
            anyhow::bail!("numeric parse should fail");
        };

        assert!(error
            .to_string()
            .contains("PROFILE_ROW_COUNT must be an unsigned integer"));
        Ok(())
    }

    /// The store is required, not defaulted: the object has to be read before the pointer is
    /// written, and there is no store worth guessing for a production publish.
    #[test]
    fn requires_a_profile_store_to_check_the_object_against() -> anyhow::Result<()> {
        let mut values = minimal();
        values.remove(PROFILE_STORAGE_DRIVER_ENV);

        let Err(error) = parse(&values, now()?) else {
            anyhow::bail!("publishing without a profile store should fail");
        };

        assert!(error.to_string().contains(PROFILE_STORAGE_DRIVER_ENV));
        Ok(())
    }

    #[test]
    fn reads_the_local_store_root() -> anyhow::Result<()> {
        let config = parse(&minimal(), now()?)?;

        assert_eq!(
            config.store,
            ProfileStoreConfig::Local {
                root: PathBuf::from("target/lakehouse/gold-profiles")
            }
        );
        Ok(())
    }
}
