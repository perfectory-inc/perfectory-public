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
use async_trait::async_trait;
use catalog_application::ports::CatalogRepository as _;
use catalog_infrastructure::PgCatalogRepository;
use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::{ComplexId, LakehouseComplexId};
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
    let pool = PgPool::connect(config.database_url.as_str())
        .await
        .context("failed to connect to database for industrial-complex Gold pointer publish")?;
    let catalog_repository = PgCatalogRepository::new(pool.clone());
    let mut resolved_complex_ids =
        resolve_catalog_complex_ids(&catalog_repository, &[config.claim.lakehouse_complex_id])
            .await?;
    let complex_id = resolved_complex_ids
        .pop()
        .context("one Gold artifact identity did not produce one Catalog identity")?;

    let store = ProfileObjectStore::open(&config.store)?;
    let verified = VerifiedGoldProfileArtifact::verify(&store, config.claim).await?;

    tracing::info!(
        complex_id = %complex_id,
        lakehouse_complex_id = %verified.lakehouse_complex_id(),
        profile_object_key = verified.object_key(),
        profile_storage_driver = store.storage_driver(),
        profile_bucket = store.bucket().unwrap_or("(local)"),
        "industrial-complex Gold profile object matched the pointer's claims"
    );

    let use_case = PublishIndustrialComplexGoldPointer::new(Arc::new(
        PgLakehousePublicationUnitOfWork::new(pool),
    ));
    let pointer = use_case
        .execute(verified.into_publish_input(complex_id, config.publication))
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
                lakehouse_complex_id: LakehouseComplexId::new(complex_id),
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

/// The one capability the pointer publisher needs from the Catalog projection.
///
/// This stays narrow so both publish commands share one translation boundary while the Catalog
/// repository remains the source of the actual lookup definition.
#[async_trait]
trait ArtifactComplexIdentityResolver: Send + Sync {
    async fn find_catalog_complex_id(
        &self,
        lakehouse_complex_id: LakehouseComplexId,
    ) -> anyhow::Result<Option<ComplexId>>;
}

#[async_trait]
impl ArtifactComplexIdentityResolver for PgCatalogRepository {
    async fn find_catalog_complex_id(
        &self,
        lakehouse_complex_id: LakehouseComplexId,
    ) -> anyhow::Result<Option<ComplexId>> {
        self.find_complex_by_lakehouse_id(lakehouse_complex_id)
            .await
            .map(|complex| complex.map(|complex| complex.id))
            .context("failed to find a Catalog complex by its lakehouse id")
    }
}

/// Resolves every Gold identity before any pointer write starts.
///
/// All misses are collected instead of silently skipped or hidden behind the first miss. The
/// caller receives no partial result when even one identity is absent.
async fn resolve_catalog_complex_ids<R>(
    resolver: &R,
    lakehouse_complex_ids: &[LakehouseComplexId],
) -> anyhow::Result<Vec<ComplexId>>
where
    R: ArtifactComplexIdentityResolver + ?Sized,
{
    let mut resolved = Vec::with_capacity(lakehouse_complex_ids.len());
    let mut missing = Vec::new();
    for &lakehouse_complex_id in lakehouse_complex_ids {
        match resolver
            .find_catalog_complex_id(lakehouse_complex_id)
            .await
            .with_context(|| {
                format!(
                    "failed to resolve lakehouse complex id {lakehouse_complex_id} to its Catalog id"
                )
            })? {
            Some(complex_id) => resolved.push(complex_id),
            None => missing.push(lakehouse_complex_id),
        }
    }

    if !missing.is_empty() {
        let missing_ids = missing
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "{} Gold artifact complex ids do not map to catalog.industrial_complex.id: {missing_ids}",
            missing.len()
        );
    }

    Ok(resolved)
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
        resolve_catalog_complex_ids, ArtifactComplexIdentityResolver,
        PublishIndustrialComplexGoldPointerConfig, COMPLEX_ID_ENV, CURRENT_VERSION_ENV,
        DATABASE_URL_ENV, EXPECTED_CURRENT_VERSION_ENV, ICEBERG_SNAPSHOT_ID_ENV,
        PROFILE_CHECKSUM_SHA256_ENV, PROFILE_LOCAL_ROOT_ENV, PROFILE_OBJECT_KEY_ENV,
        PROFILE_ROW_COUNT_ENV, PROFILE_SIZE_BYTES_ENV, PROFILE_STORAGE_DRIVER_ENV,
        PROFILE_URL_TEMPLATE_ENV, PUBLISHED_AT_UTC_ENV, SOURCE_ENV, SOURCE_EXTERNAL_ID_ENV,
        SOURCE_SNAPSHOT_ID_ENV, SOURCE_URL_ENV, SPATIAL_LOCATOR_OBJECT_KEY_ENV,
        SPATIAL_LOCATOR_SIZE_BYTES_ENV,
    };
    use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
    use async_trait::async_trait;
    use chrono::{DateTime, SecondsFormat, Utc};
    use foundation_shared_kernel::ids::{ComplexId, LakehouseComplexId};
    use std::collections::{BTreeMap, HashMap};
    use std::path::PathBuf;
    use uuid::Uuid;

    struct FakeArtifactComplexIdentityResolver {
        matches: HashMap<LakehouseComplexId, ComplexId>,
    }

    #[async_trait]
    impl ArtifactComplexIdentityResolver for FakeArtifactComplexIdentityResolver {
        async fn find_catalog_complex_id(
            &self,
            lakehouse_complex_id: LakehouseComplexId,
        ) -> anyhow::Result<Option<ComplexId>> {
            Ok(self.matches.get(&lakehouse_complex_id).copied())
        }
    }

    fn lakehouse_id(raw: &str) -> anyhow::Result<LakehouseComplexId> {
        Ok(LakehouseComplexId::new(Uuid::parse_str(raw)?))
    }

    fn catalog_id(raw: &str) -> anyhow::Result<ComplexId> {
        Ok(ComplexId::new(Uuid::parse_str(raw)?))
    }

    fn minimal() -> BTreeMap<&'static str, &'static str> {
        BTreeMap::from([
            (DATABASE_URL_ENV, "postgres://example"),
            (COMPLEX_ID_ENV, "001533c1-8504-5651-bd49-d9df4e87bc37"),
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
        let lakehouse_complex_id = "001533c1-8504-5651-bd49-d9df4e87bc37";
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
        assert_eq!(
            config.claim.lakehouse_complex_id.as_uuid().to_string(),
            lakehouse_complex_id
        );
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

    #[tokio::test]
    async fn resolves_gold_ids_to_the_catalog_ids_the_pointer_table_uses() -> anyhow::Result<()> {
        let lakehouse_ids = [
            lakehouse_id("001533c1-8504-5651-bd49-d9df4e87bc37")?,
            lakehouse_id("7df3859c-768f-51fa-a78d-6398acd5f052")?,
        ];
        let catalog_ids = [
            catalog_id("019c0000-0000-7000-8000-000000000001")?,
            catalog_id("019c0000-0000-7000-8000-000000000002")?,
        ];
        let resolver = FakeArtifactComplexIdentityResolver {
            matches: HashMap::from([
                (lakehouse_ids[0], catalog_ids[0]),
                (lakehouse_ids[1], catalog_ids[1]),
            ]),
        };

        let resolved = resolve_catalog_complex_ids(&resolver, &lakehouse_ids).await?;

        assert_eq!(resolved, catalog_ids);
        Ok(())
    }

    #[tokio::test]
    async fn every_unmapped_gold_id_is_reported_before_publication_starts() -> anyhow::Result<()> {
        let missing = [
            lakehouse_id("001533c1-8504-5651-bd49-d9df4e87bc37")?,
            lakehouse_id("7df3859c-768f-51fa-a78d-6398acd5f052")?,
        ];
        let resolver = FakeArtifactComplexIdentityResolver {
            matches: HashMap::new(),
        };

        let error = resolve_catalog_complex_ids(&resolver, &missing)
            .await
            .expect_err("unmapped Gold ids must stop the run");
        let message = error.to_string();

        assert!(message.contains("2 Gold artifact complex ids"));
        assert!(message.contains(&missing[0].to_string()));
        assert!(message.contains(&missing[1].to_string()));
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
