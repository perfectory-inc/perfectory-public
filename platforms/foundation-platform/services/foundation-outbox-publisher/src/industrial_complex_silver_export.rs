//! Industrial-complex Silver handoff export command.

use std::{fs, path::PathBuf, sync::Arc};

use anyhow::Context;
use catalog_infrastructure::PgCatalogRepository;
use chrono::{DateTime, Utc};
use lakehouse_application::{
    BuildIndustrialComplexSilverHandoff, BuildIndustrialComplexSilverHandoffInput,
};
use lakehouse_infrastructure::CatalogIndustrialComplexMaterializationReader;
use sqlx::PgPool;

use crate::public_data_control_support::{optional_env_value, required_env_value};

const DEFAULT_OUTPUT_PATH: &str = "target/lakehouse/canonical-input/industrial_complexes.jsonl";
const DATABASE_URL_ENV: &str = "DATABASE_URL";
const OUTPUT_PATH_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_SILVER_HANDOFF_PATH";
const SOURCE_SNAPSHOT_ID_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_SOURCE_SNAPSHOT_ID";
const INGESTED_AT_UTC_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_INGESTED_AT_UTC";

/// Runs the Catalog-to-Silver industrial-complex handoff export.
pub async fn run() -> anyhow::Result<()> {
    let now = Utc::now();
    let config = ExportIndustrialComplexSilverHandoffConfig::from_env(now)?;
    let pool = PgPool::connect(config.database_url.as_str())
        .await
        .context("failed to connect to database for industrial-complex Silver export")?;
    let canonical_reader = Arc::new(PgCatalogRepository::new(pool));
    let materialization_reader = Arc::new(CatalogIndustrialComplexMaterializationReader::new(
        canonical_reader,
    ));
    let use_case = BuildIndustrialComplexSilverHandoff::new(materialization_reader);
    let handoff = use_case
        .execute(BuildIndustrialComplexSilverHandoffInput {
            source_snapshot_id: config.source_snapshot_id.clone(),
            ingested_at_utc: config.ingested_at_utc,
        })
        .await
        .context("failed to build industrial-complex Silver handoff")?;

    write_handoff(&config.output_path, handoff.jsonl.as_str())?;

    tracing::info!(
        output_path = %config.output_path.display(),
        contract = handoff.contract_table_name,
        row_count = handoff.quality_metrics.get("row_count").copied().unwrap_or_default(),
        source_snapshot_id = %config.source_snapshot_id,
        "industrial-complex Silver handoff export succeeded"
    );

    Ok(())
}

struct ExportIndustrialComplexSilverHandoffConfig {
    database_url: String,
    output_path: PathBuf,
    source_snapshot_id: String,
    ingested_at_utc: DateTime<Utc>,
}

impl ExportIndustrialComplexSilverHandoffConfig {
    fn from_env(now: DateTime<Utc>) -> anyhow::Result<Self> {
        let database_url = required_env_value(DATABASE_URL_ENV)?;
        let output_path = optional_env_value(OUTPUT_PATH_ENV)?
            .map_or_else(|| PathBuf::from(DEFAULT_OUTPUT_PATH), PathBuf::from);
        let source_snapshot_id = optional_env_value(SOURCE_SNAPSHOT_ID_ENV)?
            .unwrap_or_else(|| default_source_snapshot_id(now));
        let ingested_at_utc = optional_env_value(INGESTED_AT_UTC_ENV)?
            .map(|raw| parse_utc_env(INGESTED_AT_UTC_ENV, raw.as_str()))
            .transpose()?
            .unwrap_or(now);

        Ok(Self {
            database_url,
            output_path,
            source_snapshot_id,
            ingested_at_utc,
        })
    }
}

fn write_handoff(output_path: &PathBuf, jsonl: &str) -> anyhow::Result<()> {
    let parent = output_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .context("handoff output path must have a parent directory")?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create handoff output directory {}",
            parent.display()
        )
    })?;
    fs::write(output_path, jsonl).with_context(|| {
        format!(
            "failed to write industrial-complex Silver handoff {}",
            output_path.display()
        )
    })
}

fn default_source_snapshot_id(now: DateTime<Utc>) -> String {
    format!(
        "catalog-industrial-complexes-{}",
        now.format("%Y%m%dT%H%M%SZ")
    )
}

fn parse_utc_env(name: &str, raw: &str) -> anyhow::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| format!("{name} must be an RFC3339 UTC timestamp"))
}

#[cfg(test)]
mod tests {
    use super::{default_source_snapshot_id, parse_utc_env};
    use chrono::{DateTime, SecondsFormat, Utc};

    #[test]
    fn default_snapshot_id_is_utc_timestamped() -> Result<(), chrono::ParseError> {
        let now = DateTime::parse_from_rfc3339("2026-05-18T01:02:03Z")?.with_timezone(&Utc);

        assert_eq!(
            default_source_snapshot_id(now),
            "catalog-industrial-complexes-20260518T010203Z"
        );
        Ok(())
    }

    #[test]
    fn parses_ingested_at_as_utc() -> anyhow::Result<()> {
        let parsed = parse_utc_env(
            "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_INGESTED_AT_UTC",
            "2026-05-18T01:02:03+09:00",
        )?;

        assert_eq!(
            parsed.to_rfc3339_opts(SecondsFormat::Secs, true),
            "2026-05-17T16:02:03Z"
        );
        Ok(())
    }
}
