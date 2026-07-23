//! Silver handoff helpers for Catalog-owned industrial-complex rows.

use std::collections::BTreeMap;

use catalog_domain::IndustrialComplex;
use chrono::{DateTime, Utc};
use lakehouse_domain::{LakehouseTableContract, SILVER_INDUSTRIAL_COMPLEXES};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};
use thiserror::Error;

const DEFAULT_COMPLEX_STATUS: &str = "unknown";

/// Input required to normalize Catalog industrial-complex aggregates into Silver rows.
pub struct IndustrialComplexSilverRowsInput<'a> {
    /// Catalog aggregates ordered by the caller.
    pub complexes: &'a [IndustrialComplex],
    /// Source-snapshot lineage id for this Catalog-to-lakehouse handoff.
    pub source_snapshot_id: &'a str,
    /// UTC timestamp when the rows entered the lakehouse flow.
    pub ingested_at_utc: DateTime<Utc>,
}

/// Silver `silver.industrial_complexes` row prepared from one Catalog aggregate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndustrialComplexSilverRow {
    /// Stable foundation-platform complex identifier.
    pub complex_id: String,
    /// Source-side official industrial-complex code.
    pub official_complex_code: String,
    /// Human-readable official industrial-complex name.
    pub complex_name: String,
    /// Lowercase whitespace-normalized name for search/sort projections.
    pub complex_name_normalized: String,
    /// Domain wire value for complex kind.
    pub complex_kind: String,
    /// Operational status wire value. `unknown` is used until a source provides status.
    pub status: String,
    /// Two-digit province/city code derived from `primary_bjdong_code`.
    pub sido_code: String,
    /// Five-digit city/county/district code derived from `primary_bjdong_code`.
    pub sigungu_code: String,
    /// Ten-digit legal-dong code derived from `primary_bjdong_code`.
    pub primary_bjdong_code: Option<String>,
    /// Optional official address text.
    pub address_text: Option<String>,
    /// Optional management-agency name.
    pub management_agency_name: Option<String>,
    /// Optional developer name.
    pub developer_name: Option<String>,
    /// Optional designation date.
    pub designated_date: Option<String>,
    /// Optional completion date.
    pub completion_date: Option<String>,
    /// Official complex area in square meters.
    pub official_area_sqm: Option<u64>,
    /// Stable lineage id for the Catalog source row.
    pub source_record_id: String,
    /// Source-snapshot lineage id.
    pub source_snapshot_id: String,
    /// UTC timestamp from which this fact is valid.
    pub valid_from_utc: DateTime<Utc>,
    /// UTC timestamp until which this fact is valid.
    pub valid_to_utc: Option<DateTime<Utc>>,
    /// UTC timestamp when this fact entered the lakehouse flow.
    pub ingested_at_utc: DateTime<Utc>,
    /// Lowercase SHA-256 checksum of the row payload excluding this checksum field.
    pub row_checksum_sha256: String,
}

/// Writer-neutral JSONL handoff for `silver.industrial_complexes`.
///
/// This is transient transport for writers and tests. The canonical lakehouse table storage remains
/// the `LakehouseTableContract` physical format, currently `Parquet`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndustrialComplexSilverHandoff {
    /// Static lakehouse contract table name.
    pub contract_table_name: &'static str,
    /// Target table columns in static contract order.
    pub table_columns: Vec<String>,
    /// JSONL transport columns in stable writer input order.
    pub transport_columns: Vec<String>,
    /// Newline-delimited JSON records for a downstream Spark/Iceberg writer, not final lakehouse
    /// storage.
    pub jsonl: String,
    /// Quality metrics keyed using the same convention as `SparkRunSummary`.
    pub quality_metrics: BTreeMap<String, u64>,
    /// Number of distinct source snapshots represented by the handoff.
    pub source_snapshot_count: u64,
    /// Distinct source snapshot ids represented by the handoff.
    pub source_snapshot_ids: Vec<String>,
    /// Whether `source_snapshot_ids` was truncated by this builder.
    pub source_snapshot_truncated: bool,
}

/// Error returned while normalizing industrial complexes into Silver rows.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum IndustrialComplexSilverPlanError {
    /// Input data cannot be represented as a Silver industrial-complex row.
    #[error("invalid industrial-complex Silver input: {0}")]
    InvalidInput(String),
}

/// Normalizes Catalog industrial-complex aggregates into Silver rows.
///
/// # Errors
/// Returns `IndustrialComplexSilverPlanError` when lineage is empty, required Catalog identity
/// fields are empty, or the `primary_bjdong_code` cannot provide administrative codes.
pub fn normalize_industrial_complex_silver_rows(
    input: &IndustrialComplexSilverRowsInput<'_>,
) -> Result<Vec<IndustrialComplexSilverRow>, IndustrialComplexSilverPlanError> {
    validate_lineage_part("source_snapshot_id", input.source_snapshot_id)?;

    input
        .complexes
        .iter()
        .map(|complex| {
            normalize_complex(complex, input).map_err(|error| {
                IndustrialComplexSilverPlanError::InvalidInput(format!(
                    "complex_id={} official_complex_code={}: {}",
                    complex.id, complex.official_complex_code, error
                ))
            })
        })
        .collect()
}

/// Builds a writer-neutral JSONL handoff from Silver industrial-complex rows.
///
/// # Errors
/// Returns `IndustrialComplexSilverPlanError` when a row has invalid required fields or JSON
/// serialization fails.
pub fn build_industrial_complex_silver_handoff(
    rows: &[IndustrialComplexSilverRow],
) -> Result<IndustrialComplexSilverHandoff, IndustrialComplexSilverPlanError> {
    let mut quality_metrics = required_quality_metrics(&SILVER_INDUSTRIAL_COMPLEXES);
    quality_metrics.insert("row_count".to_owned(), rows.len() as u64);
    quality_metrics.insert("invalid_official_area_count".to_owned(), 0);
    quality_metrics.insert("invalid_checksum_count".to_owned(), 0);

    let mut records = Vec::with_capacity(rows.len());
    let mut source_snapshot_ids = Vec::<String>::new();

    for row in rows {
        validate_handoff_row(row, &mut quality_metrics);
        if !source_snapshot_ids.contains(&row.source_snapshot_id) {
            source_snapshot_ids.push(row.source_snapshot_id.clone());
        }
        records.push(row_to_json_value(row));
    }

    source_snapshot_ids.sort();
    let jsonl = records
        .iter()
        .map(compact_json_line)
        .collect::<Result<Vec<_>, _>>()?
        .join("\n");
    let jsonl = if jsonl.is_empty() {
        String::new()
    } else {
        format!("{jsonl}\n")
    };

    Ok(IndustrialComplexSilverHandoff {
        contract_table_name: SILVER_INDUSTRIAL_COMPLEXES.table_name,
        table_columns: column_names(&SILVER_INDUSTRIAL_COMPLEXES),
        transport_columns: industrial_complex_transport_columns(),
        jsonl,
        quality_metrics,
        source_snapshot_count: source_snapshot_ids.len() as u64,
        source_snapshot_ids,
        source_snapshot_truncated: false,
    })
}

fn normalize_complex(
    complex: &IndustrialComplex,
    input: &IndustrialComplexSilverRowsInput<'_>,
) -> Result<IndustrialComplexSilverRow, IndustrialComplexSilverPlanError> {
    let official_complex_code = require_source_official_complex_code(
        "official_complex_code",
        complex.official_complex_code.as_str(),
    )?;
    let complex_name = require_clean_text("complex_name", complex.name.as_str())?;
    validate_primary_bjdong_code(complex.primary_bjdong_code.as_str())?;

    let mut row = IndustrialComplexSilverRow {
        complex_id: complex.id.to_string(),
        official_complex_code,
        complex_name_normalized: normalize_name(complex_name.as_str()),
        complex_name,
        complex_kind: complex.kind.wire_name().to_owned(),
        status: DEFAULT_COMPLEX_STATUS.to_owned(),
        sido_code: complex.primary_bjdong_code[0..2].to_owned(),
        sigungu_code: complex.primary_bjdong_code[0..5].to_owned(),
        primary_bjdong_code: Some(complex.primary_bjdong_code.clone()),
        address_text: None,
        management_agency_name: None,
        developer_name: None,
        designated_date: None,
        completion_date: None,
        official_area_sqm: (complex.area_m2 > 0).then_some(complex.area_m2),
        source_record_id: format!(
            "foundation-platform:catalog.industrial_complex:{}",
            complex.id
        ),
        source_snapshot_id: input.source_snapshot_id.to_owned(),
        valid_from_utc: complex.updated_at,
        valid_to_utc: None,
        ingested_at_utc: input.ingested_at_utc,
        row_checksum_sha256: String::new(),
    };
    row.row_checksum_sha256 = row_checksum(&row)?;
    Ok(row)
}

fn validate_handoff_row(
    row: &IndustrialComplexSilverRow,
    quality_metrics: &mut BTreeMap<String, u64>,
) {
    record_required_string_quality("complex_id", &row.complex_id, quality_metrics);
    record_required_string_quality(
        "official_complex_code",
        &row.official_complex_code,
        quality_metrics,
    );
    record_required_string_quality("complex_name", &row.complex_name, quality_metrics);
    record_required_string_quality(
        "complex_name_normalized",
        &row.complex_name_normalized,
        quality_metrics,
    );
    record_required_string_quality("complex_kind", &row.complex_kind, quality_metrics);
    record_required_string_quality("status", &row.status, quality_metrics);
    record_required_string_quality("sido_code", &row.sido_code, quality_metrics);
    record_required_string_quality("sigungu_code", &row.sigungu_code, quality_metrics);
    record_required_string_quality("source_record_id", &row.source_record_id, quality_metrics);
    record_required_string_quality(
        "source_snapshot_id",
        &row.source_snapshot_id,
        quality_metrics,
    );
    record_required_string_quality(
        "row_checksum_sha256",
        &row.row_checksum_sha256,
        quality_metrics,
    );
    if row.official_area_sqm == Some(0) {
        increment_metric(quality_metrics, "invalid_official_area_count");
    }
    if !is_lowercase_sha256(&row.row_checksum_sha256) {
        increment_metric(quality_metrics, "invalid_checksum_count");
    }
}

fn required_quality_metrics(contract: &LakehouseTableContract) -> BTreeMap<String, u64> {
    let mut metrics = BTreeMap::from([("row_count".to_owned(), 0)]);
    for column in contract.columns.iter().filter(|column| column.required) {
        metrics.insert(format!("{}__null_count", column.name), 0);
        if column.logical_type == "string" {
            metrics.insert(format!("{}__empty_count", column.name), 0);
        }
    }
    metrics
}

fn record_required_string_quality(
    name: &'static str,
    value: &str,
    quality_metrics: &mut BTreeMap<String, u64>,
) {
    if value.is_empty() {
        increment_metric(quality_metrics, &format!("{name}__empty_count"));
    }
}

fn increment_metric(metrics: &mut BTreeMap<String, u64>, name: &str) {
    *metrics.entry(name.to_owned()).or_insert(0) += 1;
}

fn row_to_json_value(row: &IndustrialComplexSilverRow) -> JsonValue {
    let mut record = JsonMap::new();
    record.insert(
        "complex_id".to_owned(),
        JsonValue::String(row.complex_id.clone()),
    );
    record.insert(
        "official_complex_code".to_owned(),
        JsonValue::String(row.official_complex_code.clone()),
    );
    record.insert(
        "complex_name".to_owned(),
        JsonValue::String(row.complex_name.clone()),
    );
    record.insert(
        "complex_name_normalized".to_owned(),
        JsonValue::String(row.complex_name_normalized.clone()),
    );
    record.insert(
        "complex_kind".to_owned(),
        JsonValue::String(row.complex_kind.clone()),
    );
    record.insert("status".to_owned(), JsonValue::String(row.status.clone()));
    record.insert(
        "sido_code".to_owned(),
        JsonValue::String(row.sido_code.clone()),
    );
    record.insert(
        "sigungu_code".to_owned(),
        JsonValue::String(row.sigungu_code.clone()),
    );
    record.insert(
        "primary_bjdong_code".to_owned(),
        optional_string_json(row.primary_bjdong_code.as_ref()),
    );
    record.insert(
        "address_text".to_owned(),
        optional_string_json(row.address_text.as_ref()),
    );
    record.insert(
        "management_agency_name".to_owned(),
        optional_string_json(row.management_agency_name.as_ref()),
    );
    record.insert(
        "developer_name".to_owned(),
        optional_string_json(row.developer_name.as_ref()),
    );
    record.insert(
        "designated_date".to_owned(),
        optional_string_json(row.designated_date.as_ref()),
    );
    record.insert(
        "completion_date".to_owned(),
        optional_string_json(row.completion_date.as_ref()),
    );
    record.insert(
        "official_area_sqm".to_owned(),
        row.official_area_sqm
            .map_or(JsonValue::Null, JsonValue::from),
    );
    record.insert(
        "source_record_id".to_owned(),
        JsonValue::String(row.source_record_id.clone()),
    );
    record.insert(
        "source_snapshot_id".to_owned(),
        JsonValue::String(row.source_snapshot_id.clone()),
    );
    record.insert(
        "valid_from_utc".to_owned(),
        JsonValue::String(timestamp_json(row.valid_from_utc)),
    );
    record.insert("valid_to_utc".to_owned(), JsonValue::Null);
    record.insert(
        "ingested_at_utc".to_owned(),
        JsonValue::String(timestamp_json(row.ingested_at_utc)),
    );
    record.insert(
        "row_checksum_sha256".to_owned(),
        JsonValue::String(row.row_checksum_sha256.clone()),
    );
    JsonValue::Object(record)
}

fn row_checksum(
    row: &IndustrialComplexSilverRow,
) -> Result<String, IndustrialComplexSilverPlanError> {
    let mut payload = row_to_json_value(row);
    if let JsonValue::Object(record) = &mut payload {
        record.remove("row_checksum_sha256");
    }
    Ok(sha256_hex(compact_json_line(&payload)?.as_bytes()))
}

fn optional_string_json(value: Option<&String>) -> JsonValue {
    value.map_or(JsonValue::Null, |value| JsonValue::String(value.clone()))
}

fn timestamp_json(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn compact_json_line(value: &JsonValue) -> Result<String, IndustrialComplexSilverPlanError> {
    serde_json::to_string(value)
        .map_err(|error| IndustrialComplexSilverPlanError::InvalidInput(error.to_string()))
}

fn column_names(contract: &LakehouseTableContract) -> Vec<String> {
    contract
        .columns
        .iter()
        .map(|column| column.name.to_owned())
        .collect()
}

fn industrial_complex_transport_columns() -> Vec<String> {
    [
        "complex_id",
        "official_complex_code",
        "complex_name",
        "complex_kind",
        "status",
        "sido_code",
        "sigungu_code",
        "primary_bjdong_code",
        "address_text",
        "management_agency_name",
        "developer_name",
        "designated_date",
        "completion_date",
        "official_area_sqm",
        "source_record_id",
        "source_snapshot_id",
        "valid_from_utc",
        "ingested_at_utc",
        "row_checksum_sha256",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn validate_lineage_part(
    label: &'static str,
    value: &str,
) -> Result<(), IndustrialComplexSilverPlanError> {
    if value.trim() == value && !value.is_empty() {
        return Ok(());
    }
    Err(IndustrialComplexSilverPlanError::InvalidInput(format!(
        "{label} must be non-empty text without surrounding whitespace"
    )))
}

fn require_clean_text(
    label: &'static str,
    value: &str,
) -> Result<String, IndustrialComplexSilverPlanError> {
    if value.trim() == value && !value.is_empty() {
        return Ok(value.to_owned());
    }
    Err(IndustrialComplexSilverPlanError::InvalidInput(format!(
        "{label} must be non-empty text without surrounding whitespace"
    )))
}

fn require_source_official_complex_code(
    label: &'static str,
    value: &str,
) -> Result<String, IndustrialComplexSilverPlanError> {
    let value = require_clean_text(label, value)?;
    if !value.starts_with("foundation-platform:") {
        return Ok(value);
    }
    Err(IndustrialComplexSilverPlanError::InvalidInput(
        "official_complex_code must be source-side, not a foundation-platform migration placeholder"
            .to_owned(),
    ))
}

fn validate_primary_bjdong_code(value: &str) -> Result<(), IndustrialComplexSilverPlanError> {
    if value.len() == 10 && value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(());
    }
    Err(IndustrialComplexSilverPlanError::InvalidInput(format!(
        "primary_bjdong_code must be exactly 10 ASCII digits: {value}"
    )))
}

fn normalize_name(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}
