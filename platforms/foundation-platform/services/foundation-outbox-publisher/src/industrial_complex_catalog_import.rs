//! Industrial-complex Catalog seed import command.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context;
use catalog_application::{
    ImportIndustrialComplexCatalogSeed, ImportIndustrialComplexCatalogSeedInput,
    IndustrialComplexCatalogSeedRow,
};
use catalog_domain::IndustrialComplexKind;
use catalog_infrastructure::PgCatalogUnitOfWork;
use serde::Deserialize;

use crate::public_data_control_support::required_env_value;
use sqlx::PgPool;

const DATABASE_URL_ENV: &str = "DATABASE_URL";
const SEED_PATH_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_CATALOG_SEED_PATH";

/// Runs the source-side industrial-complex Catalog seed import.
pub async fn run() -> anyhow::Result<()> {
    let config = ImportIndustrialComplexCatalogSeedConfig::from_env()?;
    let rows = read_seed_rows(&config.seed_path)?;
    let pool = PgPool::connect(config.database_url.as_str())
        .await
        .context("failed to connect to database for industrial-complex Catalog seed import")?;
    let use_case =
        ImportIndustrialComplexCatalogSeed::new(Arc::new(PgCatalogUnitOfWork::new(pool)));
    let report = use_case
        .execute(ImportIndustrialComplexCatalogSeedInput { rows })
        .await
        .context("failed to import industrial-complex Catalog seed rows")?;

    tracing::info!(
        seed_path = %config.seed_path.display(),
        imported_count = report.imported_count,
        complex_ids = ?report.complex_ids,
        "industrial-complex Catalog seed import succeeded"
    );

    Ok(())
}

struct ImportIndustrialComplexCatalogSeedConfig {
    database_url: String,
    seed_path: PathBuf,
}

impl ImportIndustrialComplexCatalogSeedConfig {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            database_url: required_env_value(DATABASE_URL_ENV)?,
            seed_path: PathBuf::from(required_env_value(SEED_PATH_ENV)?),
        })
    }
}

fn read_seed_rows(path: &Path) -> anyhow::Result<Vec<IndustrialComplexCatalogSeedRow>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read seed file {}", path.display()))?;
    parse_seed_jsonl(raw.as_str())
}

fn parse_seed_jsonl(raw: &str) -> anyhow::Result<Vec<IndustrialComplexCatalogSeedRow>> {
    raw.lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let trimmed = line.trim();
            (!trimmed.is_empty()).then_some((index + 1, trimmed))
        })
        .map(|(line_number, line)| parse_seed_line(line_number, line))
        .collect()
}

fn parse_seed_line(
    line_number: usize,
    line: &str,
) -> anyhow::Result<IndustrialComplexCatalogSeedRow> {
    let raw = serde_json::from_str::<RawIndustrialComplexCatalogSeedRow>(line)
        .with_context(|| format!("invalid JSONL at seed line {line_number}"))?;
    raw.into_seed_row()
        .with_context(|| format!("invalid industrial-complex seed row at line {line_number}"))
}

#[derive(Debug, Deserialize)]
struct RawIndustrialComplexCatalogSeedRow {
    official_complex_code: String,
    name: String,
    kind: String,
    primary_bjdong_code: String,
    area_m2: u64,
}

impl RawIndustrialComplexCatalogSeedRow {
    fn into_seed_row(self) -> anyhow::Result<IndustrialComplexCatalogSeedRow> {
        let kind = IndustrialComplexKind::from_wire(self.kind.as_str())
            .map_err(|error| anyhow::anyhow!(error))?;
        Ok(IndustrialComplexCatalogSeedRow {
            official_complex_code: self.official_complex_code,
            name: self.name,
            kind,
            primary_bjdong_code: self.primary_bjdong_code,
            area_m2: self.area_m2,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::parse_seed_jsonl;
    use catalog_domain::IndustrialComplexKind;

    #[test]
    fn parses_seed_jsonl_rows() -> anyhow::Result<()> {
        let rows = parse_seed_jsonl(
            r#"{"official_complex_code":"SYNTHETIC-COMPLEX-001","name":"Synthetic Industrial Complex Alpha","kind":"general","primary_bjdong_code":"9999900101","area_m2":9574000}"#,
        )?;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].official_complex_code, "SYNTHETIC-COMPLEX-001");
        assert_eq!(rows[0].kind, IndustrialComplexKind::General);
        assert_eq!(rows[0].primary_bjdong_code, "9999900101");
        Ok(())
    }

    #[test]
    fn rejects_unknown_kind() {
        let result = parse_seed_jsonl(
            r#"{"official_complex_code":"SYNTHETIC-COMPLEX-001","name":"Synthetic Industrial Complex Alpha","kind":"unknown","primary_bjdong_code":"9999900101","area_m2":9574000}"#,
        );

        assert!(result.is_err());
    }
}
