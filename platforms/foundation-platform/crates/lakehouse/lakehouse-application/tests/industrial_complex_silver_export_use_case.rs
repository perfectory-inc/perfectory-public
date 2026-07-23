//! Use-case tests for exporting Catalog industrial complexes to Silver handoff JSONL.

use std::sync::Mutex;

use async_trait::async_trait;
use catalog_domain::{IndustrialComplex, IndustrialComplexKind};
use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::ComplexId;
use lakehouse_application::{
    ports::IndustrialComplexMaterializationReader, BuildIndustrialComplexSilverHandoff,
    BuildIndustrialComplexSilverHandoffInput,
};
use lakehouse_domain::LakehouseError;
use serde_json::Value as JsonValue;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const FIXTURE_COMPLEX_ID: &str = "00000000-0000-7000-8000-000000000001";

#[derive(Default)]
struct InMemoryIndustrialComplexRepository {
    complexes: Mutex<Vec<IndustrialComplex>>,
}

#[async_trait]
impl IndustrialComplexMaterializationReader for InMemoryIndustrialComplexRepository {
    async fn list_industrial_complexes(&self) -> Result<Vec<IndustrialComplex>, LakehouseError> {
        self.complexes
            .lock()
            .map_err(|_| LakehouseError::Persistence("complex repo mutex poisoned".to_owned()))
            .map(|complexes| complexes.clone())
    }
}

#[tokio::test]
async fn exports_catalog_complexes_to_silver_handoff_jsonl() -> TestResult {
    let repository = std::sync::Arc::new(InMemoryIndustrialComplexRepository {
        complexes: Mutex::new(vec![sample_complex()?]),
    });
    let use_case = BuildIndustrialComplexSilverHandoff::new(repository);

    let handoff = use_case
        .execute(BuildIndustrialComplexSilverHandoffInput {
            source_snapshot_id: "synthetic-source-snapshot-industrial-complexes-20990101"
                .to_owned(),
            ingested_at_utc: parse_utc("2099-01-01T00:00:01Z")?,
        })
        .await?;

    assert_eq!(handoff.contract_table_name, "silver.industrial_complexes");
    assert_eq!(handoff.quality_metrics["row_count"], 1);
    let rows = handoff.jsonl.lines().collect::<Vec<_>>();
    assert_eq!(rows.len(), 1);
    let row: JsonValue = serde_json::from_str(rows[0])?;
    assert_eq!(row["complex_id"], FIXTURE_COMPLEX_ID);
    assert_eq!(row["official_complex_code"], "SYNTHETIC-COMPLEX-001");
    assert_eq!(
        row["source_snapshot_id"],
        "synthetic-source-snapshot-industrial-complexes-20990101"
    );
    Ok(())
}

#[tokio::test]
async fn rejects_empty_catalog_export() {
    let repository = std::sync::Arc::new(InMemoryIndustrialComplexRepository::default());
    let use_case = BuildIndustrialComplexSilverHandoff::new(repository);

    let result = use_case
        .execute(BuildIndustrialComplexSilverHandoffInput {
            source_snapshot_id: "synthetic-source-snapshot-industrial-complexes-20990101"
                .to_owned(),
            ingested_at_utc: Utc::now(),
        })
        .await;

    assert!(matches!(result, Err(LakehouseError::InvalidContract(_))));
}

#[tokio::test]
async fn placeholder_official_code_error_identifies_catalog_row() -> TestResult {
    let mut complex = sample_complex()?;
    complex.official_complex_code = format!("foundation-platform:{}", complex.id);
    let repository = std::sync::Arc::new(InMemoryIndustrialComplexRepository {
        complexes: Mutex::new(vec![complex]),
    });
    let use_case = BuildIndustrialComplexSilverHandoff::new(repository);

    let result = use_case
        .execute(BuildIndustrialComplexSilverHandoffInput {
            source_snapshot_id: "synthetic-source-snapshot-industrial-complexes-20990101"
                .to_owned(),
            ingested_at_utc: parse_utc("2099-01-01T00:00:01Z")?,
        })
        .await;

    let Err(error) = result else {
        return Err(std::io::Error::other("placeholder official code must be rejected").into());
    };
    let message = error.to_string();
    assert!(message.contains(FIXTURE_COMPLEX_ID));
    assert!(message.contains("foundation-platform:00000000-0000-7000-8000-000000000001"));
    Ok(())
}

fn sample_complex() -> TestResult<IndustrialComplex> {
    Ok(IndustrialComplex {
        id: ComplexId::new(Uuid::parse_str(FIXTURE_COMPLEX_ID)?),
        official_complex_code: "SYNTHETIC-COMPLEX-001".to_owned(),
        name: "Synthetic Industrial Complex Alpha".to_owned(),
        kind: IndustrialComplexKind::General,
        primary_bjdong_code: "9999900101".to_owned(),
        area_m2: 123_456,
        created_at: parse_utc("2099-01-01T00:00:00Z")?,
        updated_at: parse_utc("2099-01-01T00:00:00Z")?,
        archived_at: None,
        version: 1,
    })
}

fn parse_utc(raw: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}
